package node

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/stretchr/testify/mock"
)

// A barrier around the REAL socket; no simulated TLS/proof/ready/ACK.
type r2aWireGate struct {
	armed       atomic.Bool
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (g *r2aWireGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

type r2aGatedConn struct {
	net.Conn
	gate    *r2aWireGate
	stopped chan struct{}
	once    sync.Once
}

func (c *r2aGatedConn) Write(p []byte) (int, error) {
	if c.gate.armed.Load() {
		c.gate.once.Do(func() { close(c.gate.entered) })
		select {
		case <-c.gate.release:
		case <-c.stopped:
			return 0, net.ErrClosed
		}
	}
	return c.Conn.Write(p)
}
func (c *r2aGatedConn) Close() error { c.once.Do(func() { close(c.stopped) }); return c.Conn.Close() }

func r2aManagedFixture(t *testing.T, setup ...func(*Service)) (*Service, *Service, r2aHopPublication, *r2aWireGate, <-chan r2aHopPublication) {
	t.Helper()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopListener)
	publications := make(chan r2aHopPublication, 16)
	gate := &r2aWireGate{entered: make(chan struct{}), release: make(chan struct{})}
	dialer, stopDialer := dialerNode(t, address, func(s *Service) {
		actualDial := s.peerTCPDial
		s.peerTCPDial = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
			c, err := actualDial(ctx, address, timeout)
			if err != nil {
				return nil, err
			}
			return &r2aGatedConn{Conn: c, gate: gate, stopped: make(chan struct{})}, nil
		}
		// Construct, do not mutate a running CM. Keep every actual lifecycle callback.
		cfg := s.connManager.config
		cfg.DialFn = func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
			if len(addresses) == 0 {
				return DialResult{}, errR2aOrigin
			}
			session, publication, err := s.openR2aFixtureHop(ctx, addresses[0])
			if err != nil {
				return DialResult{}, err
			}
			select {
			case publications <- publication:
			case <-ctx.Done():
				_ = session.Close()
				return DialResult{}, ctx.Err()
			}
			return DialResult{Session: session, ConnectedAddress: addresses[0]}, nil
		}
		s.connManager = NewConnectionManager(cfg)
		for _, apply := range setup {
			apply(s)
		}
	})
	t.Cleanup(stopDialer)
	t.Cleanup(gate.unblock)
	var publication r2aHopPublication
	select {
	case publication = <-publications:
	case <-time.After(10 * time.Second):
		t.Fatal("no actual mandatory v2 publication")
	}
	waitForConditionMsg(t, 10*time.Second, "real CM session never became active", func() bool {
		dialer.peerMu.RLock()
		session := publication.sealed.session
		published := dialer.sessions[session.address] == session && session.authOK && session.selectedGeneration != 0
		dialer.peerMu.RUnlock()
		return published && dialer.connManager.ActiveCount() > 0
	})
	return dialer, listener, publication, gate, publications
}
func r2aRouteTo(t *testing.T, s *Service, target domain.PeerIdentity, publication r2aHopPublication) {
	t.Helper()
	hop := publication.sealed.binding.identity
	status, err := s.routingTable.UpdateRoute(routing.RouteEntry{Identity: target, Origin: hop, NextHop: hop, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("actual table route: %v %v", status, err)
	}
}
func r2aCountDirected(t *testing.T, s *Service, publication r2aHopPublication, path string, id protocol.MessageID) int {
	t.Helper()
	stop := s.captureManager.StopByConnIDs([]domain.ConnID{publication.sealed.connID})
	if len(stop.Errors) > 0 {
		t.Fatal(stop.Errors)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range strings.Split(string(raw), "\n") {
		index := strings.Index(line, " out: ")
		if index < 0 {
			continue
		}
		frame, err := protocol.ParseFrameLine(line[index+6:])
		if err == nil && frame.Type == "relay_message" && frame.ID == string(id) {
			count++
		}
	}
	return count
}
func TestR2aFirstDirectedWriteOwnsTheOnlyACK(t *testing.T) {
	s, _, publication, gate, _ := r2aManagedFixture(t)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	envelope := r2aEnvelope(t, s, recipient)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(recipient.Address), publication)
	prepared, err := s.prepareR2aFixtureOwnDM(t.Context(), envelope)
	if err != nil {
		t.Fatal(err)
	}
	capture := s.captureManager.StartByConnIDs([]domain.ConnID{publication.sealed.connID}, domain.CaptureFormatCompact)
	if len(capture.Started) != 1 {
		t.Fatalf("actual capture unavailable: %+v", capture)
	}
	gate.armed.Store(true)
	result, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, publication)
	if err != nil || !result.stored {
		t.Fatalf("private real admission: %+v %v", result, err)
	}
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual first writer never reached barrier")
	}
	for i := 0; i < 3; i++ {
		if _, _, err := s.settleR2aOwnHop(t.Context(), result); !errors.Is(err, errR2aNotWritten) {
			t.Fatalf("blocked first writer fabricated evidence: %v", err)
		}
	}
	gate.unblock()
	var x r2aRecipientEvidence
	var y r2aNearestHopEvidence
	waitForConditionMsg(t, 3*time.Second, "first actual write did not settle", func() bool {
		var settleErr error
		x, y, settleErr = s.settleR2aOwnHop(t.Context(), result)
		return settleErr == nil
	})
	if x.original.identity.String() != recipient.Address || y.original.identity != publication.sealed.binding.identity || y.connID != publication.sealed.connID || x.original.identity == y.original.identity {
		t.Fatal("recipient X or actual hop Y conflated")
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), result); !errors.Is(err, errR2aOrigin) {
		t.Fatal("duplicate settlement was not consumed")
	}
	if count := r2aCountDirected(t, s, publication, capture.Started[0].FilePath, envelope.ID); count != 1 {
		t.Fatalf("first directed writes=%d, want exactly1", count)
	}
	if s.secureSessions.store.stats().RelationAllocations != 0 {
		t.Fatal("inactive foundation created R")
	}
}

func TestR2aActualRouteY1DoesNotBecomeExpectedY2(t *testing.T) {
	s, _, first, _, publications := r2aManagedFixture(t)
	probe, listenErr := net.Listen("tcp", "[::1]:0")
	if listenErr != nil {
		t.Skipf("IPv6 loopback unavailable: %v", listenErr)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	_, stop := startTestNodeWithSetup(t, config.Node{ListenAddress: address, Type: domain.NodeTypeFull}, withoutOwnDials(nil))
	t.Cleanup(stop)
	if !s.connManager.EmitSlot(ManualPeerRequested{Address: domain.PeerAddress(address)}) {
		t.Fatal("manual fixture slot refused")
	}
	var second r2aHopPublication
	select {
	case second = <-publications:
	case <-time.After(10 * time.Second):
		t.Fatal("no second real CM proof")
	}
	waitForConditionMsg(t, 10*time.Second, "second CM not actually published", func() bool {
		return s.snapshotR2aDirectedSession(&r2aDirectedDispatch{origin: &r2aDispatch{expected: second}, nextHop: second.sealed.binding.identity, address: second.sealed.session.address, hops: 2}) != nil
	})
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), first)
	prepared, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	capture := s.captureManager.StartByConnIDs([]domain.ConnID{first.sealed.connID, second.sealed.connID}, domain.CaptureFormatCompact)
	if len(capture.Started) != 2 {
		t.Fatalf("capture %+v", capture)
	}
	paths := map[domain.ConnID]string{}
	for _, entry := range capture.Started {
		paths[entry.ConnID] = entry.FilePath
	}
	result, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, second)
	if err != nil || !result.stored {
		t.Fatalf("ordinary apply lost: %+v %v", result, err)
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), result); !errors.Is(err, errR2aNotWritten) {
		t.Fatalf("wrong expected hop got authority: %v", err)
	}
	waitForConditionMsg(t, 3*time.Second, "actual Y1 was not written", func() bool {
		raw, readErr := os.ReadFile(paths[first.sealed.connID])
		return readErr == nil && strings.Contains(string(raw), string(e.ID))
	})
	if n := r2aCountDirected(t, s, first, paths[first.sealed.connID], e.ID); n != 1 {
		t.Fatalf("Y1 directed count%d", n)
	}
	if n := r2aCountDirected(t, s, second, paths[second.sealed.connID], e.ID); n != 0 {
		t.Fatalf("expectation forced%d directed sends toY2", n)
	}
}

func r2aRemoveActualPin(t *testing.T, s *Service, id domain.PeerIdentity) {
	t.Helper()
	store := s.secureSessions.store
	preview := pinRemovalPreview(t, store, id)
	store.mu.Lock()
	entry := store.identities[id]
	metadata := entry.classMetadata()
	store.mu.Unlock()
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, metadata)
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentityFromWire(s.Address()), Store: "r2a-fixture-store"}, RuntimeEpoch: "r2a-runtime", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt, Backup: backup}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	fence := NewMockIdentityPinRemovalFence(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "r2a-one-removal", Binding: binding, Generation: 2}
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	result, err := store.removeIdentityPin(t.Context(), binding, preview.Token, fence, files)
	if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
		t.Fatalf("actual C3 remove %+v %v", result, err)
	}
}
func TestR2aOriginalYBindingSurvivesNoC3Rebinding(t *testing.T) {
	s, _, publication, gate, _ := r2aManagedFixture(t)
	recipient, address, stop := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stop)
	if err := dialWithIntent(t, s, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient.identity)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
	prepared, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	gate.armed.Store(true)
	result, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, publication)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer not blocked")
	}
	r2aRemoveActualPin(t, s, publication.sealed.binding.identity)
	gate.unblock()
	s.deliveryMu.RLock()
	candidate := result.stamp.candidate
	s.deliveryMu.RUnlock()
	select {
	case <-candidate.ack:
	case <-time.After(3 * time.Second):
		t.Fatal("actual write never completed")
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), result); !errors.Is(err, errR2aOrigin) {
		t.Fatal("old mandatory binding repaired after C3", err)
	}
}
func TestR2aOriginalXObservationPrecedesStorageIO(t *testing.T) {
	recipient, address, stop := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stop)
	store := NewMockMessageStore(t)
	var owner *Service
	store.EXPECT().StoreMessage(mock.Anything, true).RunAndReturn(func(e protocol.Envelope, _ bool) StoreResult {
		if err := dialWithIntent(t, owner, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
			t.Fatal(err)
		}
		r2aRemoveActualPin(t, owner, domain.PeerIdentityFromWire(e.Recipient))
		return StoreInserted
	}).Once()
	s, _, publication, _, _ := r2aManagedFixture(t, func(s *Service) { owner = s; s.messageStore = store })
	e := r2aEnvelope(t, s, recipient.identity)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
	prepared, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.submitR2aFixtureOwnDM(t.Context(), prepared, publication)
	if !result.stored || !errors.Is(err, errR2aOrigin) || result.stamp != nil {
		t.Fatalf("old absent X reattached after C3: %+v %v", result, err)
	}
}

func TestR2aAReplacedBeforeFirstOfferCannotBorrowBProof(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	s, _, first, _, publications := r2aManagedFixture(t, func(s *Service) {
		s.sendAdmissionBarrier = func() {
			if r2aAtFirstDirectedOffer() && armed.Swap(false) {
				close(arrived)
				<-release
			}
		}
	})
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), first)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	type answer struct {
		result r2aSubmissionResult
		err    error
	}
	done := make(chan answer, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		unblock()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("first submit not joined")
		}
	})
	go func() {
		defer close(finished)
		result, err := s.submitR2aFixtureOwnDM(t.Context(), p, first)
		done <- answer{result, err}
	}()
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("exact first item did not reach actual offer gate")
	}
	if err := first.sealed.session.Close(); err != nil {
		t.Fatal(err)
	}
	var replacement r2aHopPublication
	select {
	case replacement = <-publications:
	case <-time.After(10 * time.Second):
		t.Fatal("real CM did not reconnect")
	}
	waitForConditionMsg(t, 10*time.Second, "replacement not published", func() bool {
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		return s.sessions[replacement.sealed.session.address] == replacement.sealed.session
	})
	if replacement.sealed.connID == first.sealed.connID {
		t.Fatal("fixture failed to replace actual connection")
	}
	unblock()
	var out answer
	select {
	case out = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("old offer remained blocked")
	}
	if out.err != nil {
		t.Fatal(out.err)
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), out.result); err == nil {
		t.Fatal("old first item borrowed replacement ACK/proof")
	}
	s.deliveryMu.RLock()
	candidate := out.result.stamp.candidate
	offered := candidate.offered
	s.deliveryMu.RUnlock()
	if offered {
		t.Fatal("closed original queue accepted first item")
	}
	select {
	case <-candidate.ack:
		t.Fatal("closed original socket produced positive ACK")
	default:
	}
}
func TestR2aMemoryOnlyMandatoryPublicationDoesNotClaimStrictDurability(t *testing.T) {
	s, _, publication, _, _ := r2aManagedFixture(t, func(s *Service) { s.secureSessions.store = loadSecureSessionStore("", newStoreClock().trusted()) })
	s.secureSessions.store.mu.Lock()
	memoryOnly := s.secureSessions.store.memoryOnlyLocked()
	s.secureSessions.store.mu.Unlock()
	if !memoryOnly || publication.sealed.binding.incarnation == 0 {
		t.Fatal("no actual memory-only mandatory binding")
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.submitR2aFixtureOwnDM(t.Context(), p, publication)
	if err != nil {
		t.Fatal(err)
	}
	waitForConditionMsg(t, 3*time.Second, "memory-only actual first write not observed", func() bool { _, _, err := s.settleR2aOwnHop(t.Context(), result); return err == nil })
	if s.secureSessions.store.stats().RelationAllocations != 0 {
		t.Fatal("provenance was presented as R/strict ACK")
	}
}

func TestR2aPositiveWriteTerminalAndFreezeLifecycle(t *testing.T) {
	for _, terminal := range []string{"freeze_thaw", "withdraw", "delivered", "ttl", "shutdown", "disconnect"} {
		t.Run(terminal, func(t *testing.T) {
			s, _, publication, _, _ := r2aManagedFixture(t)
			recipient, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			e := r2aEnvelope(t, s, recipient)
			if terminal == "ttl" {
				e.Flag = protocol.MessageFlagAutoDeleteTTL
				e.TTLSeconds = 1
			}
			r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
			p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			result, err := s.submitR2aFixtureOwnDM(t.Context(), p, publication)
			if err != nil {
				t.Fatal(err)
			}
			s.deliveryMu.RLock()
			candidate := result.stamp.candidate
			s.deliveryMu.RUnlock()
			if candidate == nil {
				t.Fatal("real first item not captured")
			}
			select {
			case <-candidate.ack:
			case <-time.After(3 * time.Second):
				t.Fatal("actual write not completed")
			}
			switch terminal {
			case "freeze_thaw":
				if _, err := s.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(e.Recipient), []protocol.MessageID{e.ID}); err != nil {
					t.Fatal(err)
				}
				if _, _, err := s.settleR2aOwnHop(t.Context(), result); !errors.Is(err, errR2aOrigin) {
					t.Fatal("freeze ignored", err)
				}
				if err := s.ThawOutgoingDeliveriesTo(domain.PeerIdentityFromWire(e.Recipient), []protocol.MessageID{e.ID}); err != nil {
					t.Fatal(err)
				}
			case "withdraw":
				if _, err := s.CancelOutgoingDelivery(e.ID, domain.PeerIdentityFromWire(e.Recipient)); err != nil {
					t.Fatal(err)
				}
			case "delivered":
				s.storeDeliveryReceipt(protocol.DeliveryReceipt{MessageID: e.ID, Sender: e.Recipient, Recipient: e.Sender, Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC()})
			case "ttl":
				s.retryDueDeliveries(e.CreatedAt.Add(2 * time.Second))
			case "shutdown":
				s.runCancel()
			case "disconnect":
				if err := publication.sealed.session.Close(); err != nil {
					t.Fatal(err)
				}
			}
			_, _, err = s.settleR2aOwnHop(t.Context(), result)
			historical := terminal == "freeze_thaw" || terminal == "disconnect"
			if historical && err != nil {
				t.Fatalf("actual historical A write erased: %v", err)
			}
			if !historical && !errors.Is(err, errR2aOrigin) {
				t.Fatalf("terminal %s retained evidence: %v", terminal, err)
			}
		})
	}
}
func TestR2aLegalUnsignedRestampUsesTheFirstActualFrame(t *testing.T) {
	s, _, publication, _, _ := r2aManagedFixture(t)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	e.CreatedAt = time.Now().Add(-2 * time.Hour).UTC()
	e.Flag = protocol.MessageFlagAutoDeleteTTL
	e.TTLSeconds = 3 * 60 * 60
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), publication)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	capture := s.captureManager.StartByConnIDs([]domain.ConnID{publication.sealed.connID}, domain.CaptureFormatCompact)
	if len(capture.Started) != 1 {
		t.Fatal(capture)
	}
	result, err := s.submitR2aFixtureOwnDM(t.Context(), p, publication)
	if err != nil {
		t.Fatal(err)
	}
	waitForConditionMsg(t, 3*time.Second, "restamped actual write missing", func() bool { _, _, err := s.settleR2aOwnHop(t.Context(), result); return err == nil })
	s.captureManager.StopByConnIDs([]domain.ConnID{publication.sealed.connID})
	raw, err := os.ReadFile(capture.Started[0].FilePath)
	if err != nil {
		t.Fatal(err)
	}
	var frame protocol.Frame
	for _, line := range strings.Split(string(raw), "\n") {
		i := strings.Index(line, " out: ")
		if i < 0 {
			continue
		}
		candidate, err := protocol.ParseFrameLine(line[i+6:])
		if err == nil && candidate.Type == "relay_message" && candidate.ID == string(e.ID) {
			frame = candidate
		}
	}
	at, err := time.Parse(time.RFC3339, frame.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Body != string(e.Payload) || frame.TTLSeconds <= 0 || at.Add(time.Duration(frame.TTLSeconds)*time.Second).After(e.CreatedAt.Add(time.Duration(e.TTLSeconds)*time.Second)) {
		t.Fatal("wire projection changed body or extended unsigned deadline")
	}
	if legacyTransitRestampNeeded() && !at.After(e.CreatedAt) {
		t.Fatal("legal legacy restamp was disabled")
	}
	if !p.sealed.message.CreatedAt.Equal(e.CreatedAt) {
		t.Fatal("projection mutated original recipient provenance")
	}
}

// The existing admission barrier has no item argument. Target its real first
// directed builder stack, so unrelated announce/heartbeat enqueues cannot win
// this interleaving. This observes execution; it supplies no route/proof/ready.
func r2aAtFirstDirectedOffer() bool {
	stack := make([]uintptr, 20)
	n := runtime.Callers(2, stack)
	frames := runtime.CallersFrames(stack[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".(*Service).sendTableRoutedRelayMessageWithR2a") {
			return true
		}
		if !more {
			return false
		}
	}
}

type r2aActualRouteGate struct {
	actual           Router
	armed            atomic.Bool
	entered, release chan struct{}
	once             sync.Once
}

func (g *r2aActualRouteGate) Route(e protocol.Envelope) RoutingDecision {
	decision := g.actual.Route(e)
	if g.armed.Swap(false) {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
	return decision
}
func TestR2aAReplacedAfterActualRouteBeforeCapture(t *testing.T) {
	gate := &r2aActualRouteGate{entered: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	unblock := func() { released.Do(func() { close(gate.release) }) }
	defer unblock()
	s, _, first, _, publications := r2aManagedFixture(t, func(s *Service) { gate.actual = s.router; s.router = gate })
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	e := r2aEnvelope(t, s, recipient)
	r2aRouteTo(t, s, domain.PeerIdentityFromWire(e.Recipient), first)
	p, err := s.prepareR2aFixtureOwnDM(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	gate.armed.Store(true)
	type answer struct {
		result r2aSubmissionResult
		err    error
	}
	done := make(chan answer, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r, err := s.submitR2aFixtureOwnDM(t.Context(), p, first)
		done <- answer{r, err}
	}()
	t.Cleanup(func() {
		unblock()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("original submit not joined")
		}
	})
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual route never captured")
	}
	if err := first.sealed.session.Close(); err != nil {
		t.Fatal(err)
	}
	var replacement r2aHopPublication
	select {
	case replacement = <-publications:
	case <-time.After(10 * time.Second):
		t.Fatal("CM replacement missing")
	}
	waitForConditionMsg(t, 10*time.Second, "real replacement not current", func() bool {
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		return s.sessions[replacement.sealed.session.address] == replacement.sealed.session
	})
	unblock()
	var out answer
	select {
	case out = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("route did not finish")
	}
	if out.err != nil {
		t.Fatal(out.err)
	}
	s.deliveryMu.RLock()
	candidate := out.result.stamp.candidate
	s.deliveryMu.RUnlock()
	if candidate != nil {
		t.Fatal("old route/expectedA was repaired by current B")
	}
	if _, _, err := s.settleR2aOwnHop(t.Context(), out.result); !errors.Is(err, errR2aNotWritten) {
		t.Fatal("replacement manufactured first-hop witness", err)
	}
}
