package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

// Explicit fixture proposal, never a deployment/default profile. The clock is
// injected privately BEFORE actual Run; no public test bridge or late enabling.
type ownDMAdmissionClock struct {
	wall     time.Time
	elapsed  atomic.Int64
	wallStep atomic.Int64
}

func (c *ownDMAdmissionClock) sample() relationObservedTime {
	e := time.Duration(c.elapsed.Load())
	return relationObservedTime{wall: c.wall.Add(e + time.Duration(c.wallStep.Load())), elapsed: e}
}

type ownDMAdmissionFixture struct {
	svc         *Service
	port        OwnDMProvenancePort
	peer        *Service
	publication r2aHopPublication
	wire        *r2aWireGate
	clock       *ownDMAdmissionClock
}

// Hold only a successful original Write's return, before the real NetCore ACK.
// No network deadline is paused: the exact socket write has already completed.
func ownDMAdmissionGatedConn(conn net.Conn, gate *r2aWireGate) net.Conn {
	return &readyRelationWriteReturnConn{r2aGatedConn: &r2aGatedConn{Conn: conn, gate: gate, stopped: make(chan struct{})}}
}

// Observations are buffered/nonblocking and never change the original deadline
// or Write result. The real writer and real socket remain the only ACK source.
type ownDMGateWriteResult struct {
	n   int
	err error
}
type ownDMGateObservedConn struct {
	net.Conn
	deadline chan time.Time
	result   chan ownDMGateWriteResult
}

func (c *ownDMGateObservedConn) SetWriteDeadline(deadline time.Time) error {
	err := c.Conn.SetWriteDeadline(deadline)
	if !deadline.IsZero() {
		select {
		case c.deadline <- deadline:
		default:
		}
	}
	return err
}
func (c *ownDMGateObservedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	select {
	case c.result <- ownDMGateWriteResult{n: n, err: err}:
	default:
	}
	return n, err
}

func TestOwnDMAdmissionGateKeepsActualWriterResultPastDeadline(t *testing.T) {
	for _, afterWrite := range []bool{false, true} {
		name := "original-pre-write-gate"
		if afterWrite {
			name = "post-success-write-gate"
		}
		t.Run(name, func(t *testing.T) {
			server, client := tcpPair(t)
			observed := &ownDMGateObservedConn{Conn: client, deadline: make(chan time.Time, 1), result: make(chan ownDMGateWriteResult, 1)}
			gate := &r2aWireGate{entered: make(chan struct{}), release: make(chan struct{})}
			gate.armed.Store(true)
			var wire net.Conn = &r2aGatedConn{Conn: observed, gate: gate, stopped: make(chan struct{})}
			if afterWrite {
				wire = ownDMAdmissionGatedConn(observed, gate)
			}
			// The ordinary Outbound constructor supplies its unchanged 3s
			// deadline. No bootstrap override, ticket, or manual expiry is used.
			core := newTestNetCore(t, netcore.ConnID(0), wire, netcore.Outbound, netcore.Options{})
			t.Cleanup(gate.unblock) // before the existing exact writer Close/join
			ack := make(chan struct{})
			frame := protocol.Frame{Type: "ping"}
			line, err := protocol.MarshalFrameLine(frame)
			if err != nil {
				t.Fatal(err)
			}
			if status := core.SendTrackedObserved(frame, nil, ack); status != netcore.SendOK {
				t.Fatal("original writer refused its one frame", status)
			}
			select {
			case <-gate.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("original socket wrapper did not enter its gate")
			}
			var deadline time.Time
			select {
			case deadline = <-observed.deadline:
			default:
				t.Fatal("original NetCore did not set its real socket deadline")
			}
			var actual ownDMGateWriteResult
			if afterWrite {
				select {
				case actual = <-observed.result:
				default:
					t.Fatal("post-write gate entered before the actual Write returned")
				}
				if actual.n != len(line) || actual.err != nil {
					t.Fatalf("original socket Write = (%d,%v), want (%d,nil)", actual.n, actual.err, len(line))
				}
				if err := server.SetReadDeadline(deadline); err != nil {
					t.Fatal(err)
				}
				if got, err := bufio.NewReader(server).ReadString('\n'); err != nil || got != line {
					t.Fatalf("actual peer frame = %q, %v; want %q", got, err, line)
				}
			} else {
				select {
				case <-observed.result:
					t.Fatal("pre-write gate already called the original Write")
				default:
				}
			}
			select {
			case <-ack:
				t.Fatal("original NetCore ACK preceded release of the Write return")
			default:
			}
			// Cross the deadline actually chosen by the original writer;
			// scheduler delay only moves farther past it. No fixed sleep,
			// retry, altered grace or extra production tolerance is involved.
			timer := time.NewTimer(time.Until(deadline))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-t.Context().Done():
				t.Fatal("test ended before the original deadline crossed")
			}
			if time.Now().Before(deadline) {
				t.Fatal("deadline control returned early")
			}
			select {
			case <-ack:
				t.Fatal("original NetCore ACK escaped the held Write return")
			default:
			}
			gate.unblock()
			if afterWrite {
				select {
				case <-ack:
				case <-time.After(2 * time.Second):
					t.Fatal("successful original Write was not acknowledged after release")
				}
				select {
				case <-core.WriterDone():
					t.Fatal("successful post-write hold falsely killed the original writer")
				default:
				}
			} else {
				select {
				case <-core.WriterDone():
				case <-time.After(2 * time.Second):
					t.Fatal("expired original Write did not end the writer")
				}
				select {
				case actual = <-observed.result:
				default:
					t.Fatal("original pre-write control has no actual Write result")
				}
				timeout, ok := actual.err.(net.Error)
				if actual.n != 0 || !ok || !timeout.Timeout() {
					t.Fatalf("expired original Write = (%d,%v), want zero bytes and timeout", actual.n, actual.err)
				}
				select {
				case <-ack:
					t.Fatal("failed original Write fabricated an ACK")
				default:
				}
			}
		})
	}
}

func ownDMAdmissionManaged(t *testing.T, setup func(*Service)) ownDMAdmissionFixture {
	t.Helper()
	peer, address, stopPeer := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopPeer)
	cfg := deriveTestAdvertisePort(configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{normalizeAddress(address)}, AllowPrivatePeers: true, Type: domain.NodeTypeFull}))
	s, port, err := NewInactiveOwnDMAdmissionCandidate(cfg, ownDMRecipient(t), nil, OwnDMResidenceProfile{MaxPrestage: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	clock := &ownDMAdmissionClock{wall: time.Now()}
	s.identityResolver.recordAdmission.clock = clock.sample
	gate := &r2aWireGate{entered: make(chan struct{}), release: make(chan struct{})}
	actualDial := s.peerTCPDial
	s.peerTCPDial = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
		conn, err := actualDial(ctx, address, timeout)
		if err != nil {
			return nil, err
		}
		return ownDMAdmissionGatedConn(conn, gate), nil
	}
	if setup != nil {
		setup(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, stop := startTestService(t, ctx, cancel, s)
	t.Cleanup(stop)
	t.Cleanup(gate.unblock)
	var publication r2aHopPublication
	waitForConditionMsg(t, 10*time.Second, "admission genuine CM Ready missing", func() bool {
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		for _, session := range s.sessions {
			if hop := session.r2OwnDMHop; hop != nil && hop.r2PublishedReady && hop.session == session && session.selectedGeneration != 0 {
				publication = r2aHopPublication{sealed: hop}
				return true
			}
		}
		return false
	})
	clock.elapsed.Store(int64(time.Minute))
	return ownDMAdmissionFixture{svc: s, port: port, peer: peer, publication: publication, wire: gate, clock: clock}
}
func ownDMAdmissionRecord(t *testing.T, s *Service, id *identity.Identity, minimum domain.IdentityRecordMinSession) protocol.SignedIdentityRecord {
	t.Helper()
	record, err := protocol.BuildSignedIdentityRecord(id, protocol.IdentityRecordSpec{Network: s.identityResolver.network, Seq: 1000, IssuedAt: 1, DM: true, MinSession: minimum})
	if err != nil {
		t.Fatal(err)
	}
	body, err := protocol.VerifyIdentityRecord(record, s.identityResolver.network, domain.PeerIdentityFromWire(id.Address))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.importVerifiedIdentityRecord(s.identityResolver.network, record, body); err != nil {
		t.Fatal(err)
	}
	return record
}
func ownDMAdmissionSend(t *testing.T, f ownDMAdmissionFixture, id *identity.Identity) (protocol.Envelope, OwnDMSubmission) {
	t.Helper()
	e := r2aEnvelope(t, f.svc, id)
	target := domain.PeerIdentityFromWire(e.Recipient)
	if target == f.publication.sealed.binding.identity {
		// Ready is published before onPeerSessionEstablished installs direct routing.
		// Observe that genuine existing row; never forge a two-hop self announcement.
		waitForConditionMsg(t, 3*time.Second, "genuine same-X/Y direct route missing", func() bool {
			for _, route := range f.svc.routingTable.Lookup(target) {
				if route.Identity == target && route.NextHop == target && route.Source == routing.RouteSourceDirect && route.Hops == 1 && route.Origin == domain.PeerIdentityFromWire(f.svc.Address()) {
					return true
				}
			}
			return false
		})
	} else {
		r2aRouteTo(t, f.svc, target, f.publication)
	}
	f.wire.armed.Store(true)
	reply, receipt, err := f.port.Submit(t.Context(), e)
	if err != nil || reply.Type != "message_stored" || receipt.r2Submission == nil {
		t.Fatalf("actual own-DM admission/storage missing: %+v %v", reply, err)
	}
	select {
	case <-f.wire.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual first writer did not enter")
	}
	return e, receipt
}
func ownDMAdmissionRows(s *Service) (int, int) {
	r := s.identityResolver
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for i := range r.recordAdmission.slots {
		if r.recordAdmission.slots[i].phase != relationAdmissionFree {
			n++
		}
	}
	return n, r.recordAdmission.rate.count
}
func ownDMAdmissionSettled(t *testing.T, f ownDMAdmissionFixture, records int) {
	t.Helper()
	// Two roles need at most four existing whole-pass operations, not a new timer.
	waitForConditionMsg(t, 12*time.Second, "actual own-DM record strict completion missing", func() bool {
		rows, debits := ownDMAdmissionRows(f.svc)
		stats := f.svc.SecureSessionStoreStats()
		return rows == 0 && debits == records && stats.RelationAllocations == records && stats.RelationConfirmed == records
	})
}

func TestOwnDMAdmissionActualRunACKDistinctRolesAndSiblingBinding(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	_, receipt := ownDMAdmissionSend(t, f, x)
	if rows, debits := ownDMAdmissionRows(f.svc); rows != 2 || debits != 0 {
		t.Fatal("shared pending-ACK rows/debits", rows, debits)
	}
	if _, err := receipt.TrySettle(t.Context()); !errors.Is(err, ErrOwnDMProvenanceUnavailable) {
		t.Fatal("delegated original escaped into a second claim", err)
	}
	if f.svc.SecureSessionStoreStats().RelationAllocations != 0 {
		t.Fatal("queue acceptance became writer witness")
	}
	f.wire.unblock()
	ownDMAdmissionSettled(t, f, 2)
	store := f.svc.localContactPinStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.identities[domain.PeerIdentityFromWire(x.Address)].classMetadata().Relation.Origin != domain.IdentityPinOriginOutgoingSend || store.identities[f.publication.sealed.binding.identity].classMetadata().Relation.Origin != domain.IdentityPinOriginProvenNextHop {
		t.Fatal("original X/Y origins conflated")
	}
	c := receipt.r2Submission.r2Result.stamp.r2Claim
	if c.earned[0].binding.owner != store || c.earned[1].binding.owner != store || !c.earned[0].binding.matchesLocked(store) {
		t.Fatal("own absent-X creation was not retained as same-claim earned binding")
	}
}

func TestOwnDMAdmissionACKResidenceCancellationAndOriginalOwner(t *testing.T) {
	for _, kind := range []string{"exact_deadline", "before_deadline", "original_cancel", "X_removed", "Y_removed", "freeze", "freeze_thaw", "record_min1", "failed_write"} {
		t.Run(kind, func(t *testing.T) {
			f := ownDMAdmissionManaged(t, nil)
			x := ownDMRecipient(t)
			ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
			ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
			e, receipt := ownDMAdmissionSend(t, f, x)
			origin := receipt.r2Submission.r2Result.stamp.prepared.r2Residence
			switch kind {
			case "exact_deadline":
				f.clock.elapsed.Store(int64(origin.deadline))
			case "before_deadline":
				f.clock.elapsed.Store(int64(origin.deadline - time.Nanosecond))
			case "original_cancel":
				if _, err := f.svc.CancelOutgoingDelivery(e.ID, domain.PeerIdentityFromWire(e.Recipient)); err != nil {
					t.Fatal(err)
				}
			case "X_removed": // A true destructive epoch invalidates the original absent X.
				store := f.svc.localContactPinStore()
				store.mu.Lock()
				err := store.recordPinEvidenceLocked(domain.PeerIdentityFromWire(x.Address), store.now())
				store.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				r2aRemoveActualPin(t, f.svc, domain.PeerIdentityFromWire(x.Address))
			case "Y_removed":
				r2aRemoveActualPin(t, f.svc, f.publication.sealed.binding.identity)
			case "freeze":
				if _, err := f.svc.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(e.Recipient), []protocol.MessageID{e.ID}); err != nil {
					t.Fatal(err)
				}
			case "freeze_thaw":
				if _, err := f.svc.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(e.Recipient), []protocol.MessageID{e.ID}); err != nil {
					t.Fatal(err)
				}
				if err := f.svc.ThawOutgoingDeliveriesTo(domain.PeerIdentityFromWire(e.Recipient), []protocol.MessageID{e.ID}); err != nil {
					t.Fatal(err)
				}
			case "failed_write":
				if err := f.publication.sealed.session.Close(); err != nil {
					t.Fatal(err)
				}
				f.clock.elapsed.Store(int64(origin.deadline))
			case "record_min1":
				record, err := protocol.BuildSignedIdentityRecord(x, protocol.IdentityRecordSpec{Network: f.svc.identityResolver.network, Seq: 1001, IssuedAt: 1, DM: true, MinSession: domain.DeclaredMinSession(domain.SessionVersionLegacy)})
				if err != nil {
					t.Fatal(err)
				}
				body, err := protocol.VerifyIdentityRecord(record, f.svc.identityResolver.network, domain.PeerIdentityFromWire(x.Address))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.svc.importVerifiedIdentityRecord(f.svc.identityResolver.network, record, body); err != nil {
					t.Fatal(err)
				}
			}
			f.wire.unblock()
			if kind == "before_deadline" || kind == "freeze_thaw" {
				ownDMAdmissionSettled(t, f, 2)
				return
			}
			// A min1 X does not prevent independently valid Y; other failures revoke pair.
			expected := 0
			if kind == "record_min1" {
				expected = 1
			}
			waitForConditionMsg(t, 12*time.Second, "refused original retained or staged", func() bool { rows, debits := ownDMAdmissionRows(f.svc); return rows == 0 && debits == expected })
			if f.svc.SecureSessionStoreStats().RelationAllocations != expected {
				t.Fatal("refused original staged protection")
			}
		})
	}
}

func TestOwnDMAdmissionContextDoneAndRetryNeverRefresh(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	e := r2aEnvelope(t, f.svc, x)
	r2aRouteTo(t, f.svc, domain.PeerIdentityFromWire(e.Recipient), f.publication)
	f.wire.armed.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reply, receipt, err := f.port.Submit(ctx, e)
	if err != nil || reply.Type != "message_stored" {
		t.Fatal("ordinary echo lost", reply, err)
	}
	old := receipt.r2Submission.r2Result.stamp
	deadline := old.prepared.r2Residence.deadline
	cancel()
	f.clock.elapsed.Store(int64(deadline))
	reply, again, err := f.port.Submit(t.Context(), e)
	if err != nil || reply.Type != "message_known" || again.r2Submission == nil || again.r2Submission.r2Result.stamp != old {
		t.Fatal("typed retry replaced original owner", reply, err)
	}
	if again.r2Submission.r2Result.stamp.prepared.r2Residence.deadline != deadline {
		t.Fatal("retry refreshed original monotonic deadline")
	}
	f.wire.unblock()
	waitForConditionMsg(t, 4*time.Second, "Done/expiry did not release unearned rows", func() bool { rows, debits := ownDMAdmissionRows(f.svc); return rows == 0 && debits == 0 })
}

func TestOwnDMAdmissionRequiredProfileAndClockValidation(t *testing.T) {
	id := ownDMRecipient(t)
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t)})
	for _, duration := range []time.Duration{0, -1, time.Duration(math.MaxInt64)} {
		if s, _, err := NewInactiveOwnDMAdmissionCandidate(cfg, id, nil, OwnDMResidenceProfile{MaxPrestage: duration}); err == nil || s != nil {
			t.Fatal("invalid profile constructed Service")
		}
	}
	s, port, err := NewInactiveOwnDMAdmissionCandidate(configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t)}), ownDMRecipient(t), nil, OwnDMResidenceProfile{MaxPrestage: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a := s.identityResolver.recordAdmission
	c := &ownDMAdmissionClock{wall: time.Now()}
	a.clock = c.sample
	if port.captureOwnDMResidence(t.Context()) != nil {
		t.Fatal("pre-Run anchor minted")
	}
	if !s.identityResolver.beginRecordAdmissionRun() {
		t.Fatal("private unit Run boundary refused")
	}
	c.elapsed.Store(math.MaxInt64 - int64(time.Second)/2)
	if port.captureOwnDMResidence(t.Context()) != nil {
		t.Fatal("deadline overflow minted authority")
	}
	c.elapsed.Store(1)
	if port.captureOwnDMResidence(t.Context()) != nil || !a.clockFailed {
		t.Fatal("elapsed regression reopened admission")
	}
}

type ownDMStoreHold struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *ownDMStoreHold) StoreMessage(protocol.Envelope, bool) StoreResult {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return StoreInserted
}
func (g *ownDMStoreHold) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }
func TestOwnDMAdmissionPreRunStorageCrossingDoesNotMintAnchor(t *testing.T) {
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s, port, err := NewInactiveOwnDMAdmissionCandidate(cfg, ownDMRecipient(t), nil, OwnDMResidenceProfile{MaxPrestage: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	withoutOwnDials(nil)(s)
	hold := &ownDMStoreHold{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(hold.release) }) }
	t.Cleanup(unblock)
	s.RegisterMessageStore(hold)
	type result struct {
		reply   protocol.Frame
		receipt OwnDMSubmission
		err     error
	}
	done := make(chan result, 1)
	e := r2aEnvelope(t, s, ownDMRecipient(t))
	go func() { reply, receipt, err := port.Submit(t.Context(), e); done <- result{reply, receipt, err} }()
	select {
	case <-hold.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("initial pre-Run Submit did not enter actual store")
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, stop := startTestService(t, ctx, cancel, s)
	t.Cleanup(stop)
	t.Cleanup(unblock)
	unblock()
	var got result
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stored Submit did not return")
	}
	if got.err != nil || got.reply.Type != "message_stored" || got.receipt.r2Submission == nil {
		t.Fatal("performed pre-Run storage hidden", got.reply, got.err)
	}
	if got.receipt.r2Submission.r2Result.stamp.prepared.r2Residence != nil {
		t.Fatal("tail registration restamped pre-Run original after Run")
	}
	if rows, debits := ownDMAdmissionRows(s); rows != 0 || debits != 0 {
		t.Fatal("pre-Run original gained R capacity", rows, debits)
	}
}

func TestOwnDMAdmissionOriginalStoreDelayAndTTLClamp(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	hold := &ownDMStoreHold{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(hold.release) }) }
	t.Cleanup(unblock)
	f.svc.RegisterMessageStore(hold)
	e := r2aEnvelope(t, f.svc, ownDMRecipient(t))
	type outcome struct {
		reply   protocol.Frame
		receipt OwnDMSubmission
		err     error
	}
	done := make(chan outcome, 1)
	go func() { reply, receipt, err := f.port.Submit(t.Context(), e); done <- outcome{reply, receipt, err} }()
	select {
	case <-hold.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual store wait not reached")
	}
	f.clock.elapsed.Store(int64(3 * time.Minute))
	unblock()
	got := <-done
	if got.err != nil || got.reply.Type != "message_stored" || got.receipt.r2Submission == nil {
		t.Fatal("post-store optional expiry hid actual storage")
	}
	original := got.receipt.r2Submission.r2Result.stamp.prepared.r2Residence
	if original == nil || original.at.elapsed != time.Minute || original.deadline != 3*time.Minute {
		t.Fatal("store wait refreshed original anchor")
	}
	if rows, debits := ownDMAdmissionRows(f.svc); rows != 0 || debits != 0 {
		t.Fatal("expired store wait reserved capacity")
	}
	// A pure prepared-value clamp uses the original fields; no network/ACK fake.
	input := r2aEnvelope(t, f.svc, ownDMRecipient(t))
	input.CreatedAt = f.clock.sample().wall
	input.TTLSeconds = 3
	origin := f.port.captureOwnDMResidence(t.Context())
	prepared, err := f.svc.prepareR2aOwnDM(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	bounded := f.svc.bindOwnDMResidence(prepared, origin).sealed.r2Residence
	if bounded == nil || bounded.deadline != origin.at.elapsed+3*time.Second || !bounded.ttlExpiry.Equal(input.CreatedAt.Add(3*time.Second)) {
		t.Fatal("original TTL failed to clamp finite profile")
	}
}

func TestOwnDMAdmissionUnknownWriteAndActualCloseRetainEarned(t *testing.T) {
	entered, release, closing, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var writeOnce, closeOnce sync.Once
	unblockWrite := func() { writeOnce.Do(func() { close(release) }) }
	unblockClose := func() { closeOnce.Do(func() { close(closed) }) }
	t.Cleanup(unblockClose)
	t.Cleanup(unblockWrite)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, []byte) (secretfile.DurableCommit, error) {
		close(entered)
		<-release
		return secretfile.DurableIndeterminate, errors.New("actual unknown outcome")
	}).Once()
	files.EXPECT().Close().RunAndReturn(func() error { close(closing); <-closed; return nil }).Once()
	f := ownDMAdmissionManaged(t, func(s *Service) {
		s.identityResolver.recordAdmission.openFiles = func() (identityPinDurableFiles, error) { return files, nil }
	})
	// Register unblock after service cleanup so failed test never wedges teardown.
	t.Cleanup(unblockClose)
	t.Cleanup(unblockWrite)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	_, receipt := ownDMAdmissionSend(t, f, x)
	f.wire.unblock()
	select {
	case <-entered:
	case <-time.After(6 * time.Second):
		t.Fatal("actual owned strict writer never entered")
	}
	f.clock.elapsed.Store(int64(4 * time.Minute))
	f.svc.runCancel()
	check := func() {
		r := f.svc.identityResolver
		r.mu.Lock()
		defer r.mu.Unlock()
		slot := r.relationSlotLocked(&receipt.r2Submission.r2Result.stamp.r2Claim.refs[0])
		if slot == nil || !slot.processing || slot.phase != relationAdmissionEarnedOwed || slot.completion.attemptCovered || r.recordAdmission.rate.count != 1 {
			t.Fatal("Unknown/deadline detached or confirmed/refunded earned protection")
		}
	}
	check()
	unblockWrite()
	select {
	case <-closing:
	case <-time.After(2 * time.Second):
		t.Fatal("actual strict Close not reached")
	}
	check()
	unblockClose()
	waitForConditionMsg(t, 3*time.Second, "actual return did not retain stopped owed", func() bool {
		r := f.svc.identityResolver
		r.mu.Lock()
		defer r.mu.Unlock()
		for i := range r.recordAdmission.slots {
			slot := &r.recordAdmission.slots[i]
			if slot.phase == relationAdmissionStoppedOwed {
				return !slot.processing && slot.r2Claim == nil && r.recordAdmission.rate.count == 1
			}
		}
		return false
	})
	if f.svc.SecureSessionStoreStats().RelationConfirmed != 0 {
		t.Fatal("indeterminate mirror became strict confirmation")
	}
}

func TestOwnDMAdmissionFixedPhysicalInventory(t *testing.T) {
	var a relationRecordAdmission
	var c ownDMRelationClaim
	if len(a.slots) != 256 || len(a.rate.events) != 30 || len(c.refs) != 2 || len(c.earned) != 2 {
		t.Fatal("R2 grew a separate/unbounded queue")
	}
	t.Logf("same table=%d, fixed claim=%d, origin=%d; excludes ciphertext/context-channel allocation/allocator and is not RSS or D1 resource acceptance", unsafe.Sizeof(a), unsafe.Sizeof(c), unsafe.Sizeof(ownDMResidenceOrigin{}))
}

func TestOwnDMAdmissionActualWorkerCannotUseQueuedUnwrittenItem(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	_, receipt := ownDMAdmissionSend(t, f, x)
	waitForConditionMsg(t, 4*time.Second, "existing worker did not inspect pending real item", func() bool {
		r := f.svc.identityResolver
		r.mu.Lock()
		defer r.mu.Unlock()
		slot := r.relationSlotLocked(&receipt.r2Submission.r2Result.stamp.r2Claim.refs[0])
		return r.recordAdmission.r2PendingCursor != 0 && slot != nil && !slot.processing
	})
	f.svc.deliveryMu.RLock()
	delegated := receipt.r2Submission.r2Result.stamp.r2Claim.delegated
	f.svc.deliveryMu.RUnlock()
	if rows, debits := ownDMAdmissionRows(f.svc); delegated || rows != 2 || debits != 0 || f.svc.SecureSessionStoreStats().RelationAllocations != 0 {
		t.Fatal("actual worker promoted queue acceptance without socket Write")
	}
	f.wire.unblock()
	ownDMAdmissionSettled(t, f, 2)
}

func TestOwnDMAdmissionPartialCapacityRetryAndSlotABA(t *testing.T) {
	for _, free := range []int{0, 1, 2} {
		t.Run(string(rune('0'+free)), func(t *testing.T) {
			f := ownDMAdmissionManaged(t, nil)
			r := f.svc.identityResolver
			x := ownDMRecipient(t)
			// Synthetic occupancy carries NO pin authority; genuine typed Submit below
			// must fit in the same physical table without evicting those reservations.
			r.mu.Lock()
			sample, ok := r.recordAdmission.sampleLocked()
			if !ok {
				r.mu.Unlock()
				t.Fatal("clock unavailable")
			}
			for i := 0; i < len(r.recordAdmission.slots)-free; i++ {
				r.recordAdmission.slots[i] = relationAdmissionSlot{generation: 1, phase: relationAdmissionProspective, registered: sample.elapsed, lifetime: relationAdmissionLifetime{origin: sample}}
			}
			r.mu.Unlock()
			// Share the existing genuine-send3s guard; polling never extends it.
			recycleBy := time.Now().Add(3 * time.Second)
			e, receipt := ownDMAdmissionSend(t, f, x)
			c := receipt.r2Submission.r2Result.stamp.r2Claim
			f.svc.deliveryMu.RLock()
			reserved := 0
			for _, ref := range c.refs {
				if ref.owner != nil {
					reserved++
				}
			}
			f.svc.deliveryMu.RUnlock()
			if reserved != free {
				t.Fatal("partial X/Y physical capacity wrong", reserved, free)
			}
			if free == 1 && c.refs[0].owner == nil {
				t.Fatal("single slot did not preserve deterministic X-first policy")
			}
			if rows, debits := ownDMAdmissionRows(f.svc); rows != 256 || debits != 0 {
				t.Fatal("shared256 exceeded or reservation debited", rows, debits)
			}
			// Reclaim unrelated test occupancy, then retry cannot obtain denied roles.
			r.mu.Lock()
			for i := 0; i < 256-free; i++ {
				r.clearRelationSlotLocked(&r.recordAdmission.slots[i])
			}
			r.mu.Unlock()
			if free > 0 {
				old := c.refs[0]
				for {
					if !time.Now().Before(recycleBy) {
						t.Fatal("actual pending owner did not return within original send guard")
					}
					r.mu.Lock()
					slot := r.relationSlotLocked(&old)
					if slot == nil {
						r.mu.Unlock()
						t.Fatal("original ABA fixture slot unexpectedly disappeared")
					}
					// Ownership test and synthetic recycle are ONE mutex section. A polling
					// observation alone cannot authorize overwriting a later Processing row.
					if !slot.processing {
						r.clearRelationSlotLocked(slot)
						if slot.phase != relationAdmissionFree {
							r.mu.Unlock()
							t.Fatal("unprocessed slot did not clear before ABA fixture")
						}
						*slot = relationAdmissionSlot{generation: old.generation + 1, phase: relationAdmissionProspective, registered: sample.elapsed, lifetime: relationAdmissionLifetime{origin: sample}}
						if r.relationSlotLocked(&old) != nil {
							r.mu.Unlock()
							t.Fatal("old fixed ref rebound recycled slot")
						}
						r.mu.Unlock()
						break
					}
					r.mu.Unlock()
					time.Sleep(time.Millisecond)
				}
			}
			_, again, err := f.port.Submit(t.Context(), e)
			if err != nil || again.r2Submission.r2Result.stamp != c.stamp {
				t.Fatal("retry replaced original claim", err)
			}
			f.svc.deliveryMu.RLock()
			nowReserved := 0
			for _, ref := range c.refs {
				if ref.owner != nil {
					nowReserved++
				}
			}
			f.svc.deliveryMu.RUnlock()
			if nowReserved != reserved {
				t.Fatal("retry reacquired refused capacity")
			}
			f.clock.elapsed.Store(int64(3 * time.Minute))
			f.wire.unblock()
		})
	}
}

func TestOwnDMAdmissionVerifiedRecordNoLookupTargetProof(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	x := ownDMRecipient(t)
	record := ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	_, receipt := ownDMAdmissionSend(t, f, x)
	c := receipt.r2Submission.r2Result.stamp.r2Claim
	for _, kind := range []string{"valid", "wrong_network", "wrong_identity", "signature", "min1", "unsupported_version"} {
		t.Run(kind, func(t *testing.T) {
			network := f.svc.identityResolver.network
			candidate := protocol.SignedIdentityRecord{Version: record.Version, Body: append([]byte(nil), record.Body...), Sig: append([]byte(nil), record.Sig...)}
			if kind == "min1" {
				var err error
				candidate, err = protocol.BuildSignedIdentityRecord(x, protocol.IdentityRecordSpec{Network: network, Seq: 1001, IssuedAt: 1, MinSession: domain.DeclaredMinSession(domain.SessionVersionLegacy)})
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "wrong_network" {
				network = domain.NetworkID("foreign")
			}
			if kind == "wrong_identity" {
				var buildErr error
				candidate, buildErr = protocol.BuildSignedIdentityRecord(ownDMRecipient(t), protocol.IdentityRecordSpec{Network: network, Seq: 1000, IssuedAt: 1, MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure)})
				if buildErr != nil {
					t.Fatal("wrong-identity signed fixture failed", buildErr)
				}
			}
			if kind == "signature" {
				candidate.Sig[0] ^= 1
			}
			if kind == "unsupported_version" {
				candidate.Version = 0
			}
			if kind == "min1" {
				body, protocolErr := protocol.VerifyIdentityRecord(candidate, network, domain.PeerIdentityFromWire(x.Address))
				if protocolErr != nil || body.MinSession != domain.DeclaredMinSession(domain.SessionVersionLegacy) {
					t.Fatal("min1 fixture did not independently verify", protocolErr)
				}
			}
			verified, err := verifyOwnDMRelationRecord(c, 0, network, candidate)
			if kind == "valid" {
				if err != nil || verified.target != domain.PeerIdentityFromWire(x.Address) {
					t.Fatal("actual signed min2 rejected without TargetProof", err)
				}
			} else {
				var expected error
				switch kind {
				case "wrong_network", "signature":
					expected = protocol.ErrIdentityRecordSignature
				case "wrong_identity":
					expected = protocol.ErrIdentityRecordAddressMismatch
				case "unsupported_version":
					expected = protocol.ErrIdentityRecordVersionUnsupported
				case "min1":
					expected = errRelationPinRecord
				}
				if err == nil || !errors.Is(err, expected) || (kind != "unsupported_version" && errors.Is(err, protocol.ErrIdentityRecordVersionUnsupported)) {
					t.Fatal("foreign/mutated record did not reach its precise refusal", kind, err)
				}
			}
		})
	}
	f.clock.elapsed.Store(int64(3 * time.Minute))
	f.wire.unblock()
}

func TestOwnDMAdmissionSameIdentityOneRecordAndDebit(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	hold := &ownDMStoreHold{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(hold.release) }) }
	t.Cleanup(unblock)
	f.peer.RegisterMessageStore(hold)
	_, receipt := ownDMAdmissionSend(t, f, f.peer.identity)
	if rows, debits := ownDMAdmissionRows(f.svc); rows != 1 || debits != 0 {
		t.Fatal("same X/Y reserved twice", rows, debits)
	}
	f.wire.unblock()
	select {
	case <-hold.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("genuine same-X/Y receiver store not reached")
	}
	ownDMAdmissionSettled(t, f, 1)
	c := receipt.r2Submission.r2Result.stamp.r2Claim
	if c.refs[1].owner != nil {
		t.Fatal("same identity retained second admission ref")
	}
	store := f.svc.localContactPinStore()
	store.mu.Lock()
	origin := store.identities[f.publication.sealed.binding.identity].classMetadata().Relation.Origin
	store.mu.Unlock()
	if origin != domain.IdentityPinOriginOutgoingSend {
		t.Fatal("same identity new origin precedence wrong")
	}
	unblock()
}

type ownDMAdmissionCloseBarrier struct {
	identityPinDurableFiles
	entered chan struct{}
	release <-chan struct{}
}

func (f ownDMAdmissionCloseBarrier) Close() error {
	err := f.identityPinDurableFiles.Close()
	close(f.entered)
	<-f.release
	return err
}
func TestOwnDMAdmissionC3BetweenEarnedSiblingStagesNeverRebinds(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f := ownDMAdmissionManaged(t, func(s *Service) {
		original := s.identityResolver.recordAdmission.openFiles
		first := true
		s.identityResolver.recordAdmission.openFiles = func() (identityPinDurableFiles, error) {
			files, err := original()
			if err != nil {
				return nil, err
			}
			if first {
				first = false
				return ownDMAdmissionCloseBarrier{identityPinDurableFiles: files, entered: entered, release: release}, nil
			}
			return files, nil
		}
	})
	t.Cleanup(unblock)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	_, receipt := ownDMAdmissionSend(t, f, x)
	f.wire.unblock()
	select {
	case <-entered:
	case <-time.After(6 * time.Second):
		t.Fatal("first earned X did not reach actual Close")
	}
	c := receipt.r2Submission.r2Result.stamp.r2Claim
	store := f.svc.localContactPinStore()
	f.svc.deliveryMu.RLock()
	binding := c.earned[0].binding
	f.svc.deliveryMu.RUnlock()
	if binding.owner != store {
		t.Fatal("exact same-claim earned X binding not published")
	}
	r2aRemoveActualPin(t, f.svc, domain.PeerIdentityFromWire(x.Address))
	store.mu.Lock()
	err := store.recordPinEvidenceLocked(domain.PeerIdentityFromWire(x.Address), store.now())
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	unblock()
	waitForConditionMsg(t, 6*time.Second, "C3 ABA sibling remained queued or staged", func() bool { rows, debits := ownDMAdmissionRows(f.svc); return rows == 0 && debits == 1 })
	store.mu.Lock()
	y := store.identities[f.publication.sealed.binding.identity]
	rebounds := binding.matchesLocked(store)
	store.mu.Unlock()
	if rebounds || y.classMetadata().Relation.State != domain.IdentityPinAllocationAbsent {
		t.Fatal("earned sibling binding rebound after C3 delete/readd")
	}
}

func TestOwnDMAdmissionGlobalPinLimitDoesNotRefundOrEvict(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	_, _ = ownDMAdmissionSend(t, f, x)
	store := f.svc.localContactPinStore()
	fillContactRecordPins(t, store, maxPinnedIdentities)
	f.wire.unblock()
	waitForConditionMsg(t, 12*time.Second, "full-store candidate not resolved independently", func() bool { rows, debits := ownDMAdmissionRows(f.svc); return rows == 0 && debits == 1 })
	if store.identityPinned(domain.PeerIdentityFromWire(x.Address)) || f.svc.SecureSessionStoreStats().RelationAllocations != 1 {
		t.Fatal("new X evicted existing pin/bypassed20k; already pinned Y must remain usable")
	}
	store.mu.Lock()
	count := len(store.identities)
	store.mu.Unlock()
	if count != maxPinnedIdentities {
		t.Fatal("hard pin bound changed", count)
	}
}

func TestOwnDMAdmissionOriginalDoneRevokesWithoutClockAdvance(t *testing.T) {
	f := ownDMAdmissionManaged(t, nil)
	x := ownDMRecipient(t)
	ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	e := r2aEnvelope(t, f.svc, x)
	r2aRouteTo(t, f.svc, domain.PeerIdentityFromWire(e.Recipient), f.publication)
	f.wire.armed.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reply, _, err := f.port.Submit(ctx, e)
	if err != nil || reply.Type != "message_stored" {
		t.Fatal("stored original reply lost before Done", reply, err)
	}
	cancel()
	f.wire.unblock()
	waitForConditionMsg(t, 4*time.Second, "original Done ignored before stage", func() bool { rows, debits := ownDMAdmissionRows(f.svc); return rows == 0 && debits == 0 })
	if f.clock.elapsed.Load() != int64(time.Minute) || f.svc.SecureSessionStoreStats().RelationAllocations != 0 {
		t.Fatal("Done case used expiry or staged protection")
	}
}

// This deterministic value/crypto oracle tests the ACTUAL bounded copier, not a
// Run wait or a synthetic target proof. Its only production caller is prepare.
func TestOwnDMAdmissionBoundedCopyPreservesSignedEnvelope(t *testing.T) {
	references := map[string]map[r2aReferenceSite]int{"copyOwnDMRelationRecord": {{"node/identity_record_admission_own_dm.go", "prepareOwnDMRelation"}: 1}}
	if failures := ownDMGuardErrors(ownDMProductionSources(t), references); len(failures) != 0 {
		t.Fatal(failures)
	}
	id := ownDMRecipient(t)
	network := domain.NetworkID(networkName)
	record, err := protocol.BuildSignedIdentityRecord(id, protocol.IdentityRecordSpec{Network: network, Seq: 1001, IssuedAt: 1, DM: true, MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure)})
	if err != nil {
		t.Fatal(err)
	}
	slot := new(relationAdmissionSlot)
	copied, err := copyOwnDMRelationRecord(slot, record)
	if err != nil || copied.Version != record.Version || !bytes.Equal(copied.Body, record.Body) || !bytes.Equal(copied.Sig, record.Sig) {
		t.Fatal("bounded copy changed signed envelope", err)
	}
	body, err := protocol.VerifyIdentityRecord(copied, network, domain.PeerIdentityFromWire(id.Address))
	if err != nil || !recordRequiresSessionV2(body.MinSession) {
		t.Fatal("actual bounded copy lost valid signed min2", err)
	}
	bodyBefore, sigBefore := append([]byte(nil), copied.Body...), append([]byte(nil), copied.Sig...)
	record.Body[0] ^= 1
	record.Sig[0] ^= 1
	slot.body[0] ^= 1
	slot.signature[0] ^= 1
	if !bytes.Equal(copied.Body, bodyBefore) || !bytes.Equal(copied.Sig, sigBefore) {
		t.Fatal("bounded copy retained original or slot backing")
	}
	if _, err := protocol.VerifyIdentityRecord(copied, network, domain.PeerIdentityFromWire(id.Address)); err != nil {
		t.Fatal("owned bounded copy lost signature after source mutation", err)
	}
	unsupported := copied
	unsupported.Version = 0
	unsupportedCopy, err := copyOwnDMRelationRecord(slot, unsupported)
	if err != nil || unsupportedCopy.Version != unsupported.Version {
		t.Fatal("bounded copy normalized unsupported version", err)
	}
	if _, err := protocol.VerifyIdentityRecord(unsupportedCopy, network, domain.PeerIdentityFromWire(id.Address)); !errors.Is(err, protocol.ErrIdentityRecordVersionUnsupported) {
		t.Fatal("unsupported copied envelope reached signature verification", err)
	}
	if _, err := copyOwnDMRelationRecord(nil, copied); !errors.Is(err, errRelationPinRecord) {
		t.Fatal("bounded copy accepted missing slot", err)
	}
	beforeLen, beforeBody, beforeSig := slot.bodyLen, slot.body, slot.signature
	for _, bad := range []protocol.SignedIdentityRecord{
		{Version: copied.Version, Body: make([]byte, len(slot.body)+1), Sig: copied.Sig},
		{Version: copied.Version, Body: copied.Body, Sig: copied.Sig[:len(copied.Sig)-1]},
	} {
		if _, err := copyOwnDMRelationRecord(slot, bad); !errors.Is(err, errRelationPinRecord) {
			t.Fatal("bounded copy accepted invalid capacity", err)
		}
		if slot.bodyLen != beforeLen || slot.body != beforeBody || slot.signature != beforeSig {
			t.Fatal("bounded copy rejected after partial buffer mutation")
		}
	}
}

// The SDK releases its operation context on successful return. That cleanup
// must not cancel the separate original caller residence before real ACK/R.
func TestOwnDMAdmissionOperationReleaseKeepsOriginalCallerResidence(t *testing.T) {
	for _, kind := range []string{"operation_release", "caller_cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := ownDMAdmissionManaged(t, nil)
			x := ownDMRecipient(t)
			ownDMAdmissionRecord(t, f.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
			ownDMAdmissionRecord(t, f.svc, f.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
			e := r2aEnvelope(t, f.svc, x)
			r2aRouteTo(t, f.svc, domain.PeerIdentityFromWire(e.Recipient), f.publication)
			operation, release := context.WithCancel(t.Context())
			defer release()
			caller, cancelCaller := context.WithCancel(t.Context())
			defer cancelCaller()
			f.wire.armed.Store(true)
			reply, receipt, err := f.port.SubmitWithResidence(operation, caller, e)
			if err != nil || reply.Type != "message_stored" || receipt.r2Submission == nil {
				t.Fatal("two-context genuine storage/admission missing", reply, err)
			}
			select {
			case <-f.wire.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("two-context original writer did not enter")
			}
			origin := receipt.r2Submission.r2Result.stamp.prepared.r2Residence
			if origin == nil || origin.done != caller.Done() || origin.done == operation.Done() {
				t.Fatal("accepted residence is not the original caller lifetime")
			}
			if rows, debits := ownDMAdmissionRows(f.svc); rows != 2 || debits != 0 {
				t.Fatal("two-context pending rows or debit changed before ACK", rows, debits)
			}
			release()
			if caller.Err() != nil || operation.Err() == nil {
				t.Fatal("operation cleanup cancelled original caller")
			}
			if kind == "caller_cancel" {
				cancelCaller()
			}
			f.wire.unblock()
			if kind == "operation_release" {
				ownDMAdmissionSettled(t, f, 2)
			} else {
				waitForConditionMsg(t, 4*time.Second, "original caller cancellation did not release unearned rows", func() bool { rows, debits := ownDMAdmissionRows(f.svc); return rows == 0 && debits == 0 })
				if stats := f.svc.SecureSessionStoreStats(); stats.RelationAllocations != 0 || stats.RelationConfirmed != 0 {
					t.Fatal("cancelled original caller earned R", stats)
				}
			}
		})
	}
}
