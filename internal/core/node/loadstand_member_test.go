package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// Both adapters use real lifecycle owners. The Service-only edge fixture is
// intentionally not an SDK member and cannot implement UserTraffic.
type loadStandFullMember struct {
	node    *loadStandNode
	clock   runjournal.Clock
	receipt loadstand.StartReceipt
}

func (m *loadStandFullMember) ID() loadstand.MemberID   { return loadstand.MemberID(m.node.id) }
func (*loadStandFullMember) Role() loadstand.MemberRole { return loadstand.MemberFull }
func (m *loadStandFullMember) Owned() bool              { return m.node.current != nil }
func (m *loadStandFullMember) Start(lifetime, readiness context.Context) (loadstand.StartReceipt, error) {
	if err := m.node.startWithReadiness(lifetime, readiness); err != nil {
		return loadstand.StartReceipt{}, err
	}
	m.receipt = loadStandStartReceipt(m.node.current.running.svc.NodeStatus(), loadstand.IncarnationOrdinal(m.node.current.life))
	return m.receipt, nil
}
func (m *loadStandFullMember) CheckRunning(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.Owned() {
		return loadstand.ErrMemberState
	}
	return m.node.current.running.refuseIfExited()
}
func (m *loadStandFullMember) PeerHealth(ctx context.Context) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	for _, p := range m.node.current.running.svc.peerHealthFrames() {
		if p.Connected {
			return nil
		}
	}
	return loadstand.ErrMemberState
}
func (m *loadStandFullMember) ImportContacts(ctx context.Context, contacts []loadstand.PublicIdentity) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	reply := m.node.current.running.svc.HandleLocalFrame(protocol.Frame{Type: "import_contacts", Contacts: loadStandContactFrames(contacts)})
	if reply.Type != "contacts_imported" {
		return loadstand.ErrMemberState
	}
	return nil
}
func (*loadStandFullMember) RuntimeProcess() loadstand.RuntimeProcess {
	return loadstand.CurrentRuntimeProcess()
}
func (m *loadStandFullMember) Sample(ctx context.Context) (loadstand.MemberSample, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return loadstand.MemberSample{}, err
	}
	s, sampleErr := loadStandMemberSample(ctx, m.clock, m.receipt, loadStandServiceSource{svc: m.node.current.running.svc, binding: loadstand.RuntimeBinding{Identity: m.receipt.Public.Identity, StartedAt: m.receipt.Incarnation.StartedAt}})
	// This genuine heap read follows this member's resource/family reads.
	metrics := m.node.current.running.svc.RuntimeMetrics()
	runtimeErr := loadstand.ValidateMemberRuntime(metrics, m.receipt.Incarnation.StartedAt)
	s.Process = m.RuntimeProcess()
	s.Runtime = loadstand.Observed(metrics, runtimeErr)
	s.ReadAt = m.clock.Now()
	return s, errors.Join(sampleErr, runtimeErr, m.CheckRunning(ctx))
}
func (m *loadStandFullMember) Stop(ctx context.Context) (loadstand.StopReceipt, error) {
	before := len(m.node.ended)
	err := m.node.Stop(ctx)
	if m.Owned() {
		return loadstand.StopReceipt{Terminated: false}, err
	}
	ended := m.EndedIncarnations()
	if len(ended) <= before {
		return loadstand.StopReceipt{Terminated: true}, err
	}
	last := ended[len(ended)-1].Clone()
	return loadstand.StopReceipt{Terminated: true, Ended: &last}, err
}
func (m *loadStandFullMember) EndedIncarnations() []loadstand.EndedIncarnation {
	out := make([]loadstand.EndedIncarnation, 0, len(m.node.ended))
	for _, e := range m.node.EndedIncarnations() {
		out = append(out, loadstand.EndedIncarnation{Incarnation: loadstand.Incarnation{Ordinal: loadstand.IncarnationOrdinal(e.Life), StartedAt: e.Final.StartedAt}, Finality: loadstand.FinalExact, Final: loadstand.Observed(e.Final, nil)})
	}
	return out
}

type loadStandSDKMember struct {
	id        loadstand.MemberID
	options   loadstand.ProcessOptions
	process   *loadstand.Process
	clock     runjournal.Clock
	receipt   loadstand.StartReceipt
	ended     []loadstand.EndedIncarnation
	completed *loadstand.CompletedStartBinding
	highWater loadstand.TransportHighWater
}

func (m *loadStandSDKMember) ID() loadstand.MemberID   { return m.id }
func (*loadStandSDKMember) Role() loadstand.MemberRole { return loadstand.MemberSDK }
func (m *loadStandSDKMember) Owned() bool              { return m.process != nil }
func (m *loadStandSDKMember) Start(lifetime, readiness context.Context) (loadstand.StartReceipt, error) {
	if m.Owned() {
		return loadstand.StartReceipt{}, loadstand.ErrMemberState
	}
	m.completed = nil
	m.highWater = loadstand.TransportHighWater{}
	p, err := loadstand.LaunchLoopbackProcess(lifetime, m.options)
	m.process = p
	if err != nil {
		return loadstand.StartReceipt{}, err
	}
	if err = p.AwaitReady(readiness); err != nil {
		return loadstand.StartReceipt{}, err
	}
	source := loadstand.NewProcessSource(p.RPC())
	status, err := source.Status(readiness)
	if err != nil {
		return loadstand.StartReceipt{}, err
	}
	receipt := loadStandStartReceipt(status, loadstand.IncarnationOrdinal(len(m.ended)+1))
	var frame protocol.Frame
	if err := p.RPC().Call(readiness, "fetchContacts", nil, &frame); err != nil {
		return loadstand.StartReceipt{}, err
	}
	for _, contact := range frame.Contacts {
		if contact.Address == status.Address && contact.PubKey == status.PublicKey && contact.BoxKey == status.BoxPublicKey {
			receipt.Public.BoxSignature = contact.BoxSig
		}
	}
	if receipt.Public.BoxSignature == "" {
		return loadstand.StartReceipt{}, loadstand.ErrMemberState
	}
	if m.receipt.Public.Identity.IsZero() {
		expected := receipt.Public.Identity
		m.options.ExpectedIdentity = &expected
	}
	if !m.receipt.Public.Identity.IsZero() && m.receipt.Public != receipt.Public {
		return loadstand.StartReceipt{}, loadstand.ErrMemberState
	}
	binding, err := p.CompletedStart(receipt)
	if err != nil {
		return loadstand.StartReceipt{}, err
	}
	m.completed = &binding
	m.highWater = loadstand.NewTransportHighWater(binding.RuntimeBinding())
	m.receipt = receipt
	return receipt, nil
}
func (m *loadStandSDKMember) CheckRunning(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.Owned() {
		return loadstand.ErrMemberState
	}
	select {
	case <-m.process.Done():
		return errors.Join(loadstand.ErrMemberState, m.process.Wait(ctx))
	default:
		return nil
	}
}
func (m *loadStandSDKMember) PeerHealth(ctx context.Context) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	peers, err := loadstand.NewProcessSource(m.process.RPC()).ConnectedPeers(ctx)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return loadstand.ErrMemberState
	}
	return nil
}
func (m *loadStandSDKMember) ImportContacts(ctx context.Context, contacts []loadstand.PublicIdentity) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	var reply protocol.Frame
	err := m.process.RPC().Call(ctx, "importContacts", map[string][]protocol.ContactFrame{"contacts": loadStandContactFrames(contacts)}, &reply)
	if err != nil {
		return err
	}
	if reply.Type != "contacts_imported" {
		return loadstand.ErrMemberState
	}
	return nil
}
func (m *loadStandSDKMember) RuntimeProcess() loadstand.RuntimeProcess {
	return m.process.RuntimeProcess()
}
func (m *loadStandSDKMember) Sample(ctx context.Context) (sample loadstand.MemberSample, sampleErr error) {
	defer func() {
		if sample.Traffic.Available() {
			m.highWater.Observe(*sample.Traffic.Value, nil)
		} else {
			m.highWater.Observe(domain.TransportTrafficStats{}, loadstand.ErrInvalidSample)
		}
	}()
	if err := m.CheckRunning(ctx); err != nil {
		return loadstand.MemberSample{}, err
	}
	status, err := loadstand.NewProcessSource(m.process.RPC()).Status(ctx)
	if err != nil {
		return loadstand.MemberSample{}, err
	}
	if status.Address != m.receipt.Public.Identity.String() || !status.StartedAt.Equal(m.receipt.Incarnation.StartedAt) {
		return loadstand.MemberSample{}, loadstand.ErrMemberState
	}
	source, err := loadstand.NewBoundProcessSource(m.process.RPC(), m.clock, loadstand.RuntimeBinding{Identity: m.receipt.Public.Identity, StartedAt: m.receipt.Incarnation.StartedAt})
	if err != nil {
		return loadstand.MemberSample{}, err
	}
	sample, sampleErr = loadStandMemberSample(ctx, m.clock, m.receipt, loadStandProcessSource{source: source})
	metrics, runtimeErr := source.Runtime(ctx)
	if runtimeErr == nil {
		runtimeErr = loadstand.ValidateMemberRuntime(metrics, m.receipt.Incarnation.StartedAt)
	}
	sample.Process = m.RuntimeProcess()
	sample.Runtime = loadstand.Observed(metrics, runtimeErr)
	sample.ReadAt = m.clock.Now()
	return sample, errors.Join(sampleErr, runtimeErr, m.CheckRunning(ctx))
}
func (m *loadStandSDKMember) Stop(ctx context.Context) (loadstand.StopReceipt, error) {
	if !m.Owned() {
		return loadstand.StopReceipt{}, loadstand.ErrMemberState
	}
	err := m.process.Stop(ctx)
	select {
	case <-m.process.Done():
		if m.completed == nil {
			if cleanupErr := m.process.ReleaseArtifacts(ctx); cleanupErr != nil {
				return loadstand.StopReceipt{Terminated: true}, errors.Join(err, cleanupErr)
			}
			m.process = nil
			return loadstand.StopReceipt{Terminated: true}, err
		}
		final, finalErr := m.process.FinalAccounting(ctx, *m.completed, m.highWater)
		if errors.Is(finalErr, context.Canceled) || errors.Is(finalErr, context.DeadlineExceeded) {
			return loadstand.StopReceipt{Terminated: true}, errors.Join(err, finalErr)
		}
		e := loadstand.EndedIncarnation{Incarnation: m.completed.Receipt().Incarnation, Finality: final.Finality, Final: loadstand.Observation[domain.TransportTrafficStats]{State: loadstand.ObservationUnavailable}}
		if final.Finality == loadstand.FinalExact {
			e.Final = loadstand.Observed(final.Traffic, nil)
		}
		m.ended = append(m.ended, e.Clone())
		m.process = nil
		m.completed = nil
		owned := e.Clone()
		return loadstand.StopReceipt{Terminated: true, Ended: &owned}, errors.Join(err, finalErr)
	default:
		return loadstand.StopReceipt{Terminated: false}, errors.Join(loadstand.ErrStopUnconfirmed, err)
	}
}
func (m *loadStandSDKMember) EndedIncarnations() []loadstand.EndedIncarnation {
	out := make([]loadstand.EndedIncarnation, len(m.ended))
	for i, e := range m.ended {
		out[i] = e.Clone()
	}
	return out
}
func (m *loadStandSDKMember) SendDM(ctx context.Context, recipient domain.PeerIdentity, body loadstand.MessageBody) (loadstand.PendingAck, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return loadstand.PendingAck{}, err
	}
	var ack struct {
		Status string `json:"status"`
		To     string `json:"to"`
	}
	err := m.process.RPC().Call(ctx, "sendDm", map[string]string{"to": recipient.String(), "body": string(body)}, &ack)
	if err != nil {
		return loadstand.PendingAck{}, err
	}
	if ack.Status != "pending" || ack.To != recipient.String() {
		return loadstand.PendingAck{}, loadstand.ErrMemberState
	}
	return loadstand.PendingAck{Recipient: recipient, Queued: true}, nil
}

// The child writes only decrypted receipt metadata. Reads are capped, no raw
// body/key material or remote error response is copied into the run journal.
func (m *loadStandSDKMember) Receipts(ctx context.Context, limit int) ([]loadstand.Receipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > int(^uint(0)>>1)/1024-1 {
		return nil, loadstand.ErrInvalidConfiguration
	}
	raw, err := loadstand.ReadPublicArtifact(ctx, filepath.Join(m.options.Config.DataDir, "receipts.jsonl"), loadstand.ArtifactReadLimit(limit)*1024)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	out := make([]loadstand.Receipt, 0)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(out) >= limit {
			return nil, loadstand.ErrInvalidSample
		}
		var r loadstand.Receipt
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return nil, loadstand.ErrInvalidSample
		}
		if !r.ID.IsValid() || r.Recipient != m.receipt.Public.Identity {
			return nil, loadstand.ErrInvalidSample
		}
		out = append(out, r)
	}
	return out, errors.Join(scanner.Err(), ctx.Err())
}
func loadStandStartReceipt(s domain.NodeStatus, ordinal loadstand.IncarnationOrdinal) loadstand.StartReceipt {
	return loadstand.StartReceipt{Public: loadstand.PublicIdentity{Identity: domain.PeerIdentityFromWire(s.Address), PublicKey: s.PublicKey, BoxPublicKey: s.BoxPublicKey}, Incarnation: loadstand.Incarnation{Ordinal: ordinal, StartedAt: s.StartedAt}}
}
func loadStandContactFrames(contacts []loadstand.PublicIdentity) []protocol.ContactFrame {
	out := make([]protocol.ContactFrame, 0, len(contacts))
	for _, c := range contacts {
		out = append(out, protocol.ContactFrame{Address: c.Identity.String(), PubKey: c.PublicKey, BoxKey: c.BoxPublicKey, BoxSig: c.BoxSignature})
	}
	return out
}
func loadStandMemberSample(ctx context.Context, clock runjournal.Clock, r loadstand.StartReceipt, source loadStandNodeSource) (loadstand.MemberSample, error) {
	s := loadstand.MemberSample{Incarnation: r.Incarnation, Identity: r.Public.Identity}
	traffic, trafficErr := source.TransportTraffic(ctx)
	s.Traffic = loadstand.Observed(traffic, trafficErr)
	sessions, err := source.SessionOutcomes(ctx)
	s.Sessions = loadstand.Observed(sessions, err)
	modes, err := source.ModeSelection(ctx)
	s.Modes = loadstand.Observed(modes, err)
	neighbours, err := source.Neighbours(ctx)
	s.Neighbours = loadstand.Observed(neighbours, err)
	routes, err := source.RouteEntries(ctx)
	s.RouteEntries = loadstand.Observed(routes, err)
	resources, err := source.Resources(ctx)
	s.Resources = loadstand.Observed(resources, err)
	datagram, err := source.DatagramSummary(ctx)
	if err == nil {
		value, readErr := datagram.Get()
		if errors.Is(readErr, errDatagramNotEnabled) {
			s.Datagram.State = loadstand.ObservationDisabled
		} else {
			s.Datagram = loadstand.Observed(loadstand.DatagramSample{Metrics: value.Metrics, Reverse: value.Reverse}, readErr)
		}
	} else {
		s.Datagram = loadstand.Observed(loadstand.DatagramSample{}, err)
	}
	bans, banErr := source.Bans(ctx, clock.Now(), loadStandBanPolicy{})
	if len(bans) > 0 {
		banErr = loadstand.ErrMemberState
	}
	s.Bans = loadstand.Observed([]loadstand.BanFinding{}, banErr)
	s.ReadAt = clock.Now()
	if trafficErr != nil || banErr != nil {
		return s, errors.Join(loadstand.ErrMemberState, trafficErr, banErr)
	}
	if !traffic.StartedAt.Equal(r.Incarnation.StartedAt) || traffic.ReadAt.Before(traffic.StartedAt) {
		return s, loadstand.ErrMemberState
	}
	return s, nil
}

var _ loadstand.Member = (*loadStandFullMember)(nil)
var _ loadstand.Member = (*loadStandSDKMember)(nil)
var _ loadstand.UserTraffic = (*loadStandSDKMember)(nil)

func (m *loadStandFullMember) Endpoint() (domain.PeerAddress, bool) { return m.node.DialAddress() }
func (m *loadStandFullMember) Bootstrap() []domain.PeerAddress {
	out := make([]domain.PeerAddress, 0, len(m.node.cfg.BootstrapPeers))
	for _, p := range m.node.cfg.BootstrapPeers {
		out = append(out, domain.PeerAddress(p))
	}
	return out
}
func (*loadStandSDKMember) Endpoint() (domain.PeerAddress, bool) { return "", false }
func (m *loadStandSDKMember) Bootstrap() []domain.PeerAddress {
	return slices.Clone(m.options.Config.Bootstrap)
}

func (m *loadStandFullMember) Done() <-chan struct{} {
	if !m.Owned() {
		return nil
	}
	return m.node.current.running.exited
}
func (m *loadStandSDKMember) Done() <-chan struct{} {
	if !m.Owned() {
		return nil
	}
	return m.process.Done()
}

func (m *loadStandSDKMember) PlannedRoute(ctx context.Context, recipient domain.PeerIdentity, gateways []domain.PeerIdentity) error {
	if err := m.CheckRunning(ctx); err != nil {
		return err
	}
	return loadstand.NewProcessSource(m.process.RPC()).PlannedRoute(ctx, recipient, gateways)
}

func (m *loadStandSDKMember) PublishGlobal(ctx context.Context, body loadstand.MessageBody) (domain.GlobalPublication, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return domain.GlobalPublication{}, err
	}
	result, err := loadstand.NewProcessSource(m.process.RPC()).PublishGlobal(ctx, body)
	if err != nil {
		return result, err
	}
	if result.Message.ClaimedSender != m.receipt.Public.Identity {
		return domain.GlobalPublication{}, loadstand.ErrInvalidSample
	}
	return result, nil
}
func (m *loadStandFullMember) GlobalReplica(ctx context.Context, id domain.MessageID) (domain.GlobalMessage, error) {
	if err := m.CheckRunning(ctx); err != nil {
		return domain.GlobalMessage{}, err
	}
	reply := m.node.current.running.svc.HandleLocalFrame(protocol.Frame{Type: "fetch_message", Topic: "global", ID: string(id)})
	raw, err := json.Marshal(reply)
	if err != nil {
		return domain.GlobalMessage{}, err
	}
	return loadstand.DecodeGlobalReplica(raw, id)
}
