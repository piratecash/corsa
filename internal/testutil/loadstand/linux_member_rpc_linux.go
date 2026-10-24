//go:build linux

package loadstand

import (
	"context"
	"errors"
	"net/netip"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

func (m *linuxMember) PeerHealth(ctx context.Context) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	var frame protocol.Frame
	if err := m.fixture.RPC().Call(ctx, "fetchPeerHealth", nil, &frame); err != nil {
		return err
	}
	if frame.Type != "peer_health" {
		return ErrMemberState
	}
	count := 0
	for _, peer := range frame.PeerHealth {
		if !peer.Connected {
			continue
		}
		count++
		address, err := netip.ParseAddrPort(peer.Address)
		if err != nil {
			return ErrInvalidSample
		}
		found := false
		for _, original := range m.group.owners {
			if original.current == nil || original.current.Public.Identity.String() != peer.PeerID || original.physical.address != address.Addr() {
				continue
			}
			switch domain.PeerDirection(peer.Direction) {
			case domain.PeerDirectionOutbound:
				found = domain.PeerAddress(peer.Address) == original.physical.endpoint
			case domain.PeerDirectionInbound:
				found = true
			}
		}
		// Inbound sourceport is ephemeral. It is not a new dial endpoint;
		// original proofidentity+plannedsourceIP binds it to this real plan.
		if !found {
			return ErrMemberState
		}
	}
	if count == 0 {
		return ErrMemberState
	}
	return nil
}
func (m *linuxMember) ImportContacts(ctx context.Context, contacts []PublicIdentity) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	rows := make([]protocol.ContactFrame, len(contacts))
	for i, c := range contacts {
		rows[i] = protocol.ContactFrame{Address: c.Identity.String(), PubKey: c.PublicKey, BoxKey: c.BoxPublicKey, BoxSig: c.BoxSignature}
	}
	var reply protocol.Frame
	if err := m.fixture.RPC().Call(ctx, "importContacts", map[string][]protocol.ContactFrame{"contacts": rows}, &reply); err != nil {
		return err
	}
	if reply.Type != "contacts_imported" {
		return ErrMemberState
	}
	return nil
}
func (m *linuxMember) RuntimeProcess() RuntimeProcess {
	if m.fixture == nil {
		return RuntimeProcess{}
	}
	return RuntimeProcess{owner: m.fixture}
}
func (m *linuxMember) Sample(ctx context.Context) (sample MemberSample, result error) {
	if err := m.CheckRunning(ctx); err != nil {
		return sample, err
	}
	receipt := *m.current
	sample = MemberSample{Incarnation: receipt.Incarnation, Identity: receipt.Public.Identity}
	clock := runjournal.SystemClock{}
	source, err := NewBoundProcessSource(m.fixture.RPC(), clock, RuntimeBinding{Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt})
	if err != nil {
		return sample, err
	}
	before, osErr := m.fixture.Sample(ctx)
	// One fresh response belongs only to this Sample and its original OS bracket.
	// No failed fetch is retried by a later family; cancellation never admits
	// an already fetched response at a subsequent observation point.
	summary, summaryErr := source.RouteSummary(ctx)
	observationError := func() error {
		if summaryErr != nil {
			return summaryErr
		}
		return ctx.Err()
	}
	traffic, trafficErr := transportTrafficFromSummary(summary, observationError())
	sample.Traffic = Observed(traffic, trafficErr)
	m.highWater.Observe(traffic, trafficErr)
	sessions, err := sessionOutcomesFromSummary(summary, observationError())
	sample.Sessions = Observed(sessions, err)
	modes, err := source.modeSelectionFromSummary(summary, observationError())
	sample.Modes = Observed(modes, err)
	neighbours, err := source.neighboursFromSummary(summary, observationError())
	sample.Neighbours = Observed(neighbours, err)
	routes, err := routeEntriesFromSummary(summary, observationError())
	sample.RouteEntries = Observed(routes, err)
	resources, err := source.Resources(ctx)
	sample.Resources = Observed(resources, err)
	reading, err := source.DatagramSummary(ctx)
	if err == nil {
		value, readErr := reading.Get()
		if errors.Is(readErr, ErrDatagramDisabled) {
			sample.Datagram.State = ObservationDisabled
		} else {
			sample.Datagram = Observed(value, readErr)
		}
	} else {
		sample.Datagram = Observed(DatagramSample{}, err)
	}
	bans, banErr := source.Bans(ctx, clock.Now(), BanPolicy{})
	if len(bans) != 0 {
		banErr = ErrMemberState
	}
	sample.Bans = Observed(bans, banErr)
	// Go heap belongs to this exact target and is read inside the original OS
	// provenance bracket, after its resource/family reads. RSS is not a proxy.
	metrics, runtimeErr := source.Runtime(ctx)
	if runtimeErr == nil {
		runtimeErr = ValidateMemberRuntime(metrics, receipt.Incarnation.StartedAt)
	}
	sample.Process = m.RuntimeProcess()
	sample.Runtime = Observed(metrics, runtimeErr)
	after, afterErr := m.fixture.Sample(ctx)
	if osErr == nil && afterErr == nil && (before.Provenance != after.Provenance || before.Provenance != m.fixture.readyProvenance || after.ReadStartedAt.Before(before.ReadStartedAt)) {
		osErr = ErrProcessObservation
	}
	osErr = errors.Join(osErr, afterErr)
	observation := Observed(after, osErr)
	sample.LinuxProcess = &observation
	sample.ReadAt = time.Now().UTC()
	if err = m.CheckRunning(ctx); err != nil {
		return sample, err
	}
	if !sample.FullSampleAvailable() {
		return sample, errors.Join(ErrMemberState, trafficErr, banErr, osErr, runtimeErr)
	}
	return sample, nil
}
func (m *linuxSDKMember) PlannedRoute(ctx context.Context, recipient domain.PeerIdentity, gateways []domain.PeerIdentity) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	return NewProcessSource(m.fixture.RPC()).PlannedRoute(ctx, recipient, gateways)
}
func (m *linuxSDKMember) SendDM(ctx context.Context, recipient domain.PeerIdentity, body MessageBody) (PendingAck, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return PendingAck{}, err
	}
	var ack struct {
		Status string `json:"status"`
		To     string `json:"to"`
	}
	if err := m.fixture.RPC().Call(ctx, "sendDm", map[string]string{"to": recipient.String(), "body": string(body)}, &ack); err != nil {
		return PendingAck{}, err
	}
	if ack.Status != "pending" || ack.To != recipient.String() {
		return PendingAck{}, ErrMemberState
	}
	return PendingAck{Recipient: recipient, Queued: true}, nil
}
func (m *linuxSDKMember) PublishGlobal(ctx context.Context, body MessageBody) (domain.GlobalPublication, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return domain.GlobalPublication{}, err
	}
	value, err := NewProcessSource(m.fixture.RPC()).PublishGlobal(ctx, body)
	if err == nil && value.Message.ClaimedSender != m.current.Public.Identity {
		return domain.GlobalPublication{}, ErrInvalidSample
	}
	return value, err
}
func (m *linuxFullMember) GlobalReplica(ctx context.Context, id domain.MessageID) (domain.GlobalMessage, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return domain.GlobalMessage{}, err
	}
	return NewProcessSource(m.fixture.RPC()).GlobalReplica(ctx, id)
}

var _ UserTraffic = (*linuxSDKMember)(nil)
var _ GlobalPublisher = (*linuxSDKMember)(nil)
var _ GlobalObserver = (*linuxFullMember)(nil)
