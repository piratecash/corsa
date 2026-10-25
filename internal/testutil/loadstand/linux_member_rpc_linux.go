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

// These labels identify existing observed outcomes and the original return site,
// not the initiating read or its cost. Earlier errors are never retained here.
type linuxSampleFailureStage string

const (
	linuxSampleInitialRunning linuxSampleFailureStage = "initial_running"
	linuxSampleBoundSource    linuxSampleFailureStage = "bound_source"
	linuxSampleBeforeOS       linuxSampleFailureStage = "before_os"
	linuxSampleRouteSummary   linuxSampleFailureStage = "route_summary"
	linuxSampleTraffic        linuxSampleFailureStage = "traffic"
	linuxSampleSessions       linuxSampleFailureStage = "sessions"
	linuxSampleModes          linuxSampleFailureStage = "modes"
	linuxSampleNeighbours     linuxSampleFailureStage = "neighbours"
	linuxSampleRouteEntries   linuxSampleFailureStage = "route_entries"
	linuxSampleResources      linuxSampleFailureStage = "resources"
	linuxSampleDatagram       linuxSampleFailureStage = "datagram"
	linuxSampleBans           linuxSampleFailureStage = "bans"
	linuxSampleRuntimeFetch   linuxSampleFailureStage = "runtime_fetch"
	linuxSampleRuntimeBinding linuxSampleFailureStage = "runtime_binding"
	linuxSampleAfterOS        linuxSampleFailureStage = "after_os"
	linuxSampleOSBracket      linuxSampleFailureStage = "os_bracket"
	linuxSampleFinalRunning   linuxSampleFailureStage = "final_running"
	linuxSampleFullSample     linuxSampleFailureStage = "full_sample"
)

type linuxSampleFailureClass string

const (
	linuxSampleContextCancelled   linuxSampleFailureClass = "ctx_cancelled"
	linuxSampleContextDeadline    linuxSampleFailureClass = "ctx_deadline"
	linuxSampleFamilyUnavailable  linuxSampleFailureClass = "family_unavailable"
	linuxSampleInvalidSample      linuxSampleFailureClass = "invalid_sample"
	linuxSampleMemberState        linuxSampleFailureClass = "member_state"
	linuxSampleProcessObservation linuxSampleFailureClass = "process_observation"
	linuxSampleRPCAccess          linuxSampleFailureClass = "rpc_access"
	linuxSampleRPCClosed          linuxSampleFailureClass = "rpc_closed"
	linuxSampleOwnerProtocol      linuxSampleFailureClass = "owner_protocol"
	linuxSampleOwnerPoisoned      linuxSampleFailureClass = "owner_poisoned"
	linuxSampleOtherFailure       linuxSampleFailureClass = "other"
)

// Ordered precedence for original joined causes; no hidden error is stored.
func classifyLinuxSampleFailure(err error) linuxSampleFailureClass {
	switch {
	case errors.Is(err, context.Canceled):
		return linuxSampleContextCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return linuxSampleContextDeadline
	case errors.Is(err, ErrFamilyUnavailable):
		return linuxSampleFamilyUnavailable
	case errors.Is(err, ErrInvalidSample):
		return linuxSampleInvalidSample
	case errors.Is(err, ErrMemberState):
		return linuxSampleMemberState
	case errors.Is(err, ErrProcessObservation):
		return linuxSampleProcessObservation
	case errors.Is(err, ErrRPCAccess):
		return linuxSampleRPCAccess
	case errors.Is(err, ErrRPCClosed):
		return linuxSampleRPCClosed
	case errors.Is(err, ErrLinuxOwnerProtocol):
		return linuxSampleOwnerProtocol
	case errors.Is(err, ErrLinuxOwnerPoisoned):
		return linuxSampleOwnerPoisoned
	default:
		return linuxSampleOtherFailure
	}
}

type linuxSampleFirstFailure struct {
	stage linuxSampleFailureStage
	class linuxSampleFailureClass
}

func (first *linuxSampleFirstFailure) observe(stage linuxSampleFailureStage, err error) {
	if first.stage == "" && err != nil {
		first.stage = stage
		first.class = classifyLinuxSampleFailure(err)
	}
}

type linuxSampleFailure struct {
	firstObservedFailedStage  linuxSampleFailureStage
	firstObservedFailureClass linuxSampleFailureClass
	returnSite                linuxSampleFailureStage
	original                  error
}

func (failure *linuxSampleFailure) Error() string {
	return "linux-member-sample first_observed_failed_stage=" + string(failure.firstObservedFailedStage) + " first_observed_failure_class=" + string(failure.firstObservedFailureClass) + " return_site=" + string(failure.returnSite) + ": " + failure.original.Error()
}
func (failure *linuxSampleFailure) Unwrap() error { return failure.original }

func (first linuxSampleFirstFailure) at(site linuxSampleFailureStage, original error) error {
	first.observe(site, original)
	return &linuxSampleFailure{firstObservedFailedStage: first.stage, firstObservedFailureClass: first.class, returnSite: site, original: original}
}

func (m *linuxMember) Sample(ctx context.Context) (sample MemberSample, result error) {
	var firstFailure linuxSampleFirstFailure
	if err := m.CheckRunning(ctx); err != nil {
		return sample, firstFailure.at(linuxSampleInitialRunning, err)
	}
	receipt := *m.current
	sample = MemberSample{Incarnation: receipt.Incarnation, Identity: receipt.Public.Identity}
	clock := runjournal.SystemClock{}
	source, err := NewBoundProcessSource(m.fixture.RPC(), clock, RuntimeBinding{Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt})
	if err != nil {
		return sample, firstFailure.at(linuxSampleBoundSource, err)
	}
	before, osErr := m.fixture.Sample(ctx)
	if osErr == nil {
		sample.LinuxBeforeExecutableReadBrackets = before.ExecutableReadBrackets
	}
	if osErr == nil && before.ReadElapsedKnown && before.ReadElapsedNS >= 0 && before.ReadElapsedNS < 1<<63-1 {
		sample.LinuxBeforeElapsedNS, sample.LinuxBeforeElapsedKnown = before.ReadElapsedNS, true
	}
	firstFailure.observe(linuxSampleBeforeOS, osErr)
	// One fresh response belongs only to this Sample and its original OS bracket.
	// No failed fetch is retried by a later family; cancellation never admits
	// an already fetched response at a subsequent observation point.
	summary, summaryErr := source.RouteSummary(ctx)
	firstFailure.observe(linuxSampleRouteSummary, summaryErr)
	observationError := func() error {
		if summaryErr != nil {
			return summaryErr
		}
		return ctx.Err()
	}
	traffic, trafficErr := transportTrafficFromSummary(summary, observationError())
	firstFailure.observe(linuxSampleTraffic, trafficErr)
	sample.Traffic = Observed(traffic, trafficErr)
	m.highWater.Observe(traffic, trafficErr)
	sessions, err := sessionOutcomesFromSummary(summary, observationError())
	firstFailure.observe(linuxSampleSessions, err)
	sample.Sessions = Observed(sessions, err)
	modes, err := source.modeSelectionFromSummary(summary, observationError())
	firstFailure.observe(linuxSampleModes, err)
	sample.Modes = Observed(modes, err)
	neighbours, err := source.neighboursFromSummary(summary, observationError())
	firstFailure.observe(linuxSampleNeighbours, err)
	sample.Neighbours = Observed(neighbours, err)
	routes, err := routeEntriesFromSummary(summary, observationError())
	firstFailure.observe(linuxSampleRouteEntries, err)
	sample.RouteEntries = Observed(routes, err)
	resources, err := source.Resources(ctx)
	firstFailure.observe(linuxSampleResources, err)
	sample.Resources = Observed(resources, err)
	reading, err := source.DatagramSummary(ctx)
	if err == nil {
		value, readErr := reading.Get()
		if errors.Is(readErr, ErrDatagramDisabled) {
			sample.Datagram.State = ObservationDisabled
		} else {
			firstFailure.observe(linuxSampleDatagram, readErr)
			sample.Datagram = Observed(value, readErr)
		}
	} else {
		firstFailure.observe(linuxSampleDatagram, err)
		sample.Datagram = Observed(DatagramSample{}, err)
	}
	bans, banErr := source.Bans(ctx, clock.Now(), BanPolicy{})
	if len(bans) != 0 {
		banErr = ErrMemberState
	}
	firstFailure.observe(linuxSampleBans, banErr)
	sample.Bans = Observed(bans, banErr)
	// Go heap belongs to this exact target and is read inside the original OS
	// provenance bracket, after its resource/family reads. RSS is not a proxy.
	metrics, runtimeErr := source.Runtime(ctx)
	firstFailure.observe(linuxSampleRuntimeFetch, runtimeErr)
	if runtimeErr == nil {
		runtimeErr = ValidateMemberRuntime(metrics, receipt.Incarnation.StartedAt)
		firstFailure.observe(linuxSampleRuntimeBinding, runtimeErr)
	}
	sample.Process = m.RuntimeProcess()
	sample.Runtime = Observed(metrics, runtimeErr)
	after, afterErr := m.fixture.Sample(ctx)
	firstFailure.observe(linuxSampleAfterOS, afterErr)
	if osErr == nil && afterErr == nil && (before.Provenance != after.Provenance || before.Provenance != m.fixture.readyProvenance || after.ReadStartedAt.Before(before.ReadStartedAt)) {
		osErr = ErrProcessObservation
		firstFailure.observe(linuxSampleOSBracket, osErr)
	}
	osErr = errors.Join(osErr, afterErr)
	observation := Observed(after, osErr)
	sample.LinuxProcess = &observation
	sample.ReadAt = time.Now().UTC()
	if err = m.CheckRunning(ctx); err != nil {
		return sample, firstFailure.at(linuxSampleFinalRunning, err)
	}
	if !sample.FullSampleAvailable() {
		return sample, firstFailure.at(linuxSampleFullSample, errors.Join(ErrMemberState, trafficErr, banErr, osErr, runtimeErr))
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
