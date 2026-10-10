package node

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// local_eviction_session_test.go pins what a LOCAL eviction of an outbound
// slot that is still Initializing may do to the peer behind it. The
// ConnectionManager evicts such a slot on its own decision — RetainOnly (the
// egress half of connect_only), shrinkToLimit, the handleManualPeer eviction,
// shutdown — and closes its session. The session goroutine started by
// onCMSessionEstablished is still inside initPeerSession at that moment.
//
// Two things must hold whichever path evicted the slot:
//
//   - the failure initPeerSession sees on the closed socket is the node's own
//     doing, so it is not a setup failure of the peer: the per-address counter
//     that feeds the dial cooldown (setup_failure.go) does not move, and route
//     quarantine (armed when that counter reaches the threshold) is not armed;
//   - a session the CM has already closed is never published as live — not
//     entered in s.sessions / upstream, not counted as an identity session —
//     even when initPeerSession happens to succeed on it.
//
// The rig wires a test ConnectionManager to the Service's real CM callbacks
// and dials one hand-built session over net.Pipe with a real NetCore. The far
// end is played by the test, so initPeerSession ends exactly when the test
// lets it.

var (
	evictionSlotAddress  = domain.PeerAddress("10.0.0.1:64646")
	evictionOtherAddress = domain.PeerAddress("10.0.0.2:64646")
	evictionPinAddress   = domain.PeerAddress("203.0.113.9:64646")
)

// evictionSeededSetupFailures is the setup-failure count the rig starts from:
// one below the threshold, so a single wrongly charged eviction both moves the
// counter and arms route quarantine.
const evictionSeededSetupFailures = setupFailureBanThreshold - 1

// farEndFunc plays the peer on the far side of the session's pipe. inbox and
// errs stand in for readPeerSession, the session's real reader: a reply is
// delivered on inbox, a dead transport on errs.
type farEndFunc func(remote net.Conn, inbox chan<- protocol.Frame, errs chan<- error)

// evictionRigOptions configures newEvictionRig. Both fields are optional.
type evictionRigOptions struct {
	// beforeEstablished runs on the event loop ahead of the Service's
	// onCMSessionEstablished, so a test can hold the loop there.
	beforeEstablished func(SessionInfo)
	// farEnd replaces the default far end, which reads every request,
	// never answers, and closes requestSeen on the first one.
	farEnd farEndFunc
}

// evictionRig is a Service whose ConnectionManager has dialled one outbound
// session at evictionSlotAddress.
type evictionRig struct {
	svc     *Service
	cm      *ConnectionManager
	session *peerSession

	// maxSlots backs the manager's MaxSlotsFn; lowering it and triggering a
	// fill makes shrinkToLimit evict.
	maxSlots atomic.Int32
	// requestSeen is closed by the default far end when the first setup
	// request arrives, i.e. initPeerSession is in flight.
	requestSeen chan struct{}
	// establishedReturned is closed once onCMSessionEstablished has returned
	// on the event loop, so the session goroutine is on runLoopsWg.
	establishedReturned chan struct{}
	established         atomic.Pointer[SessionInfo]
	cancelCM            context.CancelFunc
	// retainEnqueued is closed when the first RetainOnly request reaches
	// the manager's queue (ConnectionManagerConfig.RetainOnlyEnqueued).
	retainEnqueued <-chan struct{}
}

func newEvictionRig(t *testing.T, opts evictionRigOptions) *evictionRig {
	t.Helper()

	svc := newTestService(t, config.NodeTypeFull)
	rig := &evictionRig{
		svc:                 svc,
		requestSeen:         make(chan struct{}),
		establishedReturned: make(chan struct{}),
	}
	rig.maxSlots.Store(1)
	rig.session = newEvictionSession()
	rig.attachFarEnd(t, opts.farEnd)
	rig.seedSetupFailures()

	b := testCMConfig(string(evictionSlotAddress))
	b.Cfg.MaxSlotsFn = func() int { return int(rig.maxSlots.Load()) }
	// No periodic refill: every fill in these tests is one the test asked for.
	b.Cfg.FillInterval = time.Hour
	b.Cfg.DialFn = rig.dialOnce()
	beforeEstablished := opts.beforeEstablished
	var returnedOnce sync.Once
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		rig.established.Store(&info)
		if beforeEstablished != nil {
			beforeEstablished(info)
		}
		svc.onCMSessionEstablished(info)
		returnedOnce.Do(func() { close(rig.establishedReturned) })
	}
	b.Cfg.OnSessionTeardown = svc.onCMSessionTeardown
	b.Cfg.RetainOnlyEnqueued, rig.retainEnqueued = retainEnqueuedSignal()
	b.Cfg.OnStaleSession = svc.onCMStaleSession
	b.Cfg.IsSetupFailureBannedFn = svc.IsSetupFailureBanned

	rig.cm = b.Build()
	// The session goroutine reports ActiveSessionLost / SessionInitReady to
	// svc.connManager; it has to reach the manager that owns the slot.
	svc.connManager = rig.cm
	rig.cancelCM = runCM(rig.cm)
	t.Cleanup(rig.cancelCM)
	rig.cm.NotifyBootstrapReady()

	return rig
}

// newEvictionSession builds the outbound session the rig dials, still without
// a transport (attachFarEnd gives it one). inboxCh is unbuffered so a reply
// handed to it is known to have been taken by initPeerSession.
func newEvictionSession() *peerSession {
	return &peerSession{
		address:      evictionSlotAddress,
		peerIdentity: domaintest.ID("local-eviction-peer"),
		connID:       9001,
		authOK:       true,
		inboxCh:      make(chan protocol.Frame),
		errCh:        make(chan error, 1),
	}
}

// attachFarEnd connects the session to a pipe whose other end farEnd plays,
// and attaches the session's NetCore the way openPeerSessionForCM does.
func (rig *evictionRig) attachFarEnd(t *testing.T, farEnd farEndFunc) {
	t.Helper()
	local, remote := net.Pipe()
	rig.session.conn = local
	rig.svc.attachOutboundNetCore(rig.session)
	// The session goroutine must not outlive the test if an assertion fails
	// before the CM closes the session.
	t.Cleanup(func() { _ = rig.session.Close() })
	t.Cleanup(func() { _ = remote.Close() })

	if farEnd == nil {
		farEnd = func(remote net.Conn, _ chan<- protocol.Frame, errs chan<- error) {
			readRequestsWithoutAnswering(remote, errs, rig.requestSeen)
		}
	}
	go farEnd(remote, rig.session.inboxCh, rig.session.errCh)
}

// dialOnce returns the rig's session to the first dial. A later dial — the
// handleManualPeer slot, a refill — parks until the manager stops, so it adds
// no slot events of its own.
func (rig *evictionRig) dialOnce() func(context.Context, []domain.PeerAddress) (DialResult, error) {
	var dials atomic.Int32
	return func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
		if dials.Add(1) > 1 {
			<-ctx.Done()
			return DialResult{}, ctx.Err()
		}
		return DialResult{Session: rig.session, ConnectedAddress: addresses[0]}, nil
	}
}

func (rig *evictionRig) seedSetupFailures() {
	now := time.Now()
	rig.svc.peerMu.Lock()
	defer rig.svc.peerMu.Unlock()
	for i := 0; i < evictionSeededSetupFailures; i++ {
		rig.svc.recordSetupFailureLocked(evictionSlotAddress, now)
	}
}

// readRequestsWithoutAnswering plays a peer that accepts the session's setup
// requests and never replies, so initPeerSession can end only when the
// session is closed. On the closed socket the read error reaches errs, which
// is how a real session learns its transport is gone.
func readRequestsWithoutAnswering(remote net.Conn, errs chan<- error, requestSeen chan<- struct{}) {
	reader := bufio.NewReader(remote)
	var seenOnce sync.Once
	for {
		if _, err := reader.ReadString('\n'); err != nil {
			select {
			case errs <- err:
			default:
			}
			return
		}
		seenOnce.Do(func() { close(requestSeen) })
	}
}

func (rig *evictionRig) awaitRequestInFlight(t *testing.T) {
	t.Helper()
	select {
	case <-rig.requestSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("initPeerSession never sent its first setup request: the premise of this test never armed")
	}
}

// awaitSessionGoroutine waits until the session goroutine onCMSessionEstablished
// started has exited.
func (rig *evictionRig) awaitSessionGoroutine(t *testing.T) {
	t.Helper()
	select {
	case <-rig.establishedReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("onCMSessionEstablished never returned")
	}
	done := make(chan struct{})
	go func() {
		rig.svc.runLoopsWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Nothing closed the session, so initPeerSession is still waiting for
		// a reply that will not come: the eviction did not happen.
		_ = rig.session.Close()
		t.Fatal("the session goroutine did not exit: the eviction never closed the session")
	}
}

// assertSlotEvicted checks that the slot generation the session was
// established under is gone. Matching on the generation, not the address,
// keeps a later slot for the same address from hiding a missed eviction.
func (rig *evictionRig) assertSlotEvicted(t *testing.T) {
	t.Helper()
	info := rig.established.Load()
	if info == nil {
		t.Fatal("precondition: the session was never established")
	}
	for _, s := range rig.cm.Slots() {
		if s.Generation == info.SlotGeneration {
			t.Fatalf("precondition: slot %s at generation %d was not evicted (state %s)", s.Address, s.Generation, s.State)
		}
	}
}

// assertEvictionNotChargedToPeer is the shared verdict of the setup-failure
// tests: the eviction moved neither the counter nor route quarantine.
func (rig *evictionRig) assertEvictionNotChargedToPeer(t *testing.T, eviction string) {
	t.Helper()
	rig.svc.peerMu.RLock()
	consecutive := 0
	if entry := rig.svc.setupFailures[evictionSlotAddress]; entry != nil {
		consecutive = entry.Consecutive
	}
	rig.svc.peerMu.RUnlock()

	if consecutive != evictionSeededSetupFailures {
		t.Errorf("setup failures recorded against %s = %d, want the seeded %d: the session was closed by a local %s, "+
			"not by the peer failing setup", evictionSlotAddress, consecutive, evictionSeededSetupFailures, eviction)
	}
	if rig.svc.isSubjectInRouteQuarantine(rig.session.penaltySubject()) {
		t.Errorf("route quarantine was armed against %s by a local %s", evictionSlotAddress, eviction)
	}
}

// retainEnqueuedSignal returns a ConnectionManagerConfig.RetainOnlyEnqueued
// hook and the channel it closes the first time a RetainOnly request is
// accepted into the manager's queue. It is wired into the configuration
// before the manager is built, so the manager never sees it change.
func retainEnqueuedSignal() (func(), <-chan struct{}) {
	enqueued := make(chan struct{})
	var once sync.Once
	return func() { once.Do(func() { close(enqueued) }) }, enqueued
}

// evictWithRetainOnlyAcrossHeldCallback runs RetainOnly while the event loop
// may be held by the test and returns once the request is in the loop's queue
// — observed through the manager's RetainOnlyEnqueued hook, whose channel is
// enqueued — so the test releases the loop only after the eviction is actually
// pending behind it. The premise is required: a RetainOnly that returns
// without enqueuing, or never gets its request in, fails the test. Returns the
// channel closed when RetainOnly has returned.
func evictWithRetainOnlyAcrossHeldCallback(t *testing.T, cm *ConnectionManager, enqueued <-chan struct{}, keep domain.PeerAddress) <-chan struct{} {
	t.Helper()
	retainDone := make(chan struct{})
	go func() {
		defer close(retainDone)
		cm.RetainOnly(context.Background(), keep)
	}()
	select {
	case <-enqueued:
	case <-retainDone:
		t.Fatal("RetainOnly returned without enqueuing its request: the loop was not running")
	case <-time.After(2 * time.Second):
		t.Fatal("RetainOnly never enqueued its request behind the held event loop")
	}
	return retainDone
}

func awaitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// ---------------------------------------------------------------------------
// Setup failure is not charged for a local eviction
// ---------------------------------------------------------------------------

// RetainOnly evicts the slot while initPeerSession waits for the peer's reply,
// well after onCMSessionEstablished has returned — the race-free half.
func TestRetainOnlyDuringInitPeerSession_DoesNotRecordSetupFailure(t *testing.T) {
	rig := newEvictionRig(t, evictionRigOptions{})
	rig.awaitRequestInFlight(t)

	rig.cm.RetainOnly(context.Background(), evictionPinAddress)
	rig.awaitSessionGoroutine(t)
	rig.assertSlotEvicted(t)

	rig.assertEvictionNotChargedToPeer(t, "RetainOnly eviction")
}

// RetainOnly evicts the slot while onCMSessionEstablished is still held on the
// event loop — the window between handleDialSucceeded releasing cm.mu and the
// callback.
func TestRetainOnlyDuringSessionEstablished_DoesNotRecordSetupFailure(t *testing.T) {
	gate := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	entered := make(chan SessionInfo, 1)

	rig := newEvictionRig(t, evictionRigOptions{
		beforeEstablished: func(info SessionInfo) {
			reportSessionEstablished(t, entered, info)
			<-gate
		},
	})
	// Registered after the rig's cleanups, so it runs first and the event loop
	// is not left parked on the gate when the manager is cancelled.
	t.Cleanup(releaseGate)

	awaitSessionEstablished(t, entered)
	retainDone := evictWithRetainOnlyAcrossHeldCallback(t, rig.cm, rig.retainEnqueued, evictionPinAddress)
	releaseGate()

	rig.awaitSessionGoroutine(t)
	awaitClosed(t, retainDone, "RetainOnly to return after the event loop was released")
	rig.assertSlotEvicted(t)

	rig.assertEvictionNotChargedToPeer(t, "RetainOnly eviction")
}

// shrinkToLimit evicts the Initializing slot when the slot limit drops to zero
// while initPeerSession is in flight.
func TestShrinkToLimitDuringInitPeerSession_DoesNotRecordSetupFailure(t *testing.T) {
	rig := newEvictionRig(t, evictionRigOptions{})
	rig.awaitRequestInFlight(t)

	rig.maxSlots.Store(0)
	rig.cm.EmitHint(NewPeersDiscovered{Count: 1})
	rig.awaitSessionGoroutine(t)
	rig.assertSlotEvicted(t)

	rig.assertEvictionNotChargedToPeer(t, "shrinkToLimit eviction")
}

// handleManualPeer evicts the Initializing slot — a non-active slot, its
// preferred victim — to make room for an operator add_peer while
// initPeerSession is in flight.
func TestManualPeerEvictionDuringInitPeerSession_DoesNotRecordSetupFailure(t *testing.T) {
	rig := newEvictionRig(t, evictionRigOptions{})
	rig.awaitRequestInFlight(t)

	if !rig.cm.EmitSlot(ManualPeerRequested{Address: evictionOtherAddress}) {
		t.Fatal("the manager refused the manual peer request")
	}
	rig.awaitSessionGoroutine(t)
	rig.assertSlotEvicted(t)

	rig.assertEvictionNotChargedToPeer(t, "handleManualPeer eviction")
}

// Manager shutdown closes the Initializing slot's session while
// initPeerSession is in flight.
func TestCMShutdownDuringInitPeerSession_DoesNotRecordSetupFailure(t *testing.T) {
	rig := newEvictionRig(t, evictionRigOptions{})
	rig.awaitRequestInFlight(t)

	rig.cancelCM()
	rig.awaitSessionGoroutine(t)
	if got := rig.cm.SlotCount(); got != 0 {
		t.Fatalf("precondition: %d slots left after the manager shut down", got)
	}

	rig.assertEvictionNotChargedToPeer(t, "ConnectionManager shutdown")
}

// ---------------------------------------------------------------------------
// A session closed by a local eviction is never published as live
// ---------------------------------------------------------------------------

// replyAfterEvictionFarEnd answers the session's setup requests, but holds the
// reply to fetch_contacts — the last request initPeerSession makes on a node
// whose datagram plane is not running — until the test has evicted the slot.
// The reply then reaches initPeerSession after the CM closed the session: the
// real reader can have queued it on inboxCh just before the close.
type replyAfterEvictionFarEnd struct {
	lastRequestSeen chan struct{}
	evicted         chan struct{}
	// lastReplyTaken is closed once initPeerSession has taken the
	// fetch_contacts reply off the unbuffered inbox, i.e. setup got its last
	// answer and completed.
	lastReplyTaken chan struct{}
	done           <-chan struct{}
}

func (f *replyAfterEvictionFarEnd) play(remote net.Conn, inbox chan<- protocol.Frame, errs chan<- error) {
	replies := map[string]string{"get_peers": "peers", "fetch_contacts": "contacts"}
	reader := bufio.NewReader(remote)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			select {
			case errs <- err:
			default:
			}
			return
		}
		request, err := protocol.ParseFrameLine(line)
		if err != nil {
			continue
		}
		replyType, ok := replies[request.Type]
		if !ok {
			continue
		}
		if request.Type == "fetch_contacts" {
			close(f.lastRequestSeen)
			select {
			case <-f.evicted:
			case <-f.done:
				return
			}
		}
		select {
		case inbox <- protocol.Frame{Type: replyType}:
		case <-f.done:
			return
		}
		if request.Type == "fetch_contacts" {
			close(f.lastReplyTaken)
		}
	}
}

// liveSessionObservation is what the Service publishes about the rig's session
// at the moment its serve loop tears it down.
type liveSessionObservation struct {
	registered       bool
	identitySessions int
}

// initPeerSession succeeds — its last reply was already on the way — but the
// slot was evicted, and its session closed, before the session goroutine
// published it. The closed session must not then be entered in s.sessions or
// counted as a live identity session, and its address must not be marked
// connected.
//
// peerTeardownBarrier is where it is observed: retirePeerSession calls it
// while the serve loop's entry is still in s.sessions, so a session that was
// published is seen there; one that was not never reaches it. That a session
// never reaches it proves something only if its setup did complete, which the
// far end records when initPeerSession takes its last reply.
func TestLocallyEvictedSessionIsNotPublishedAsLiveWhenSetupSucceeds(t *testing.T) {
	farEndDone := make(chan struct{})
	farEnd := &replyAfterEvictionFarEnd{
		lastRequestSeen: make(chan struct{}),
		evicted:         make(chan struct{}),
		lastReplyTaken:  make(chan struct{}),
		done:            farEndDone,
	}
	rig := newEvictionRig(t, evictionRigOptions{farEnd: farEnd.play})
	// Registered after the rig's cleanups, so it runs first and unblocks a far
	// end still holding its reply before the session is closed under it.
	t.Cleanup(func() { close(farEndDone) })

	var observed atomic.Pointer[liveSessionObservation]
	rig.svc.peerTeardownBarrier = func() {
		rig.svc.peerMu.RLock()
		defer rig.svc.peerMu.RUnlock()
		observed.CompareAndSwap(nil, &liveSessionObservation{
			registered:       rig.svc.sessions[evictionSlotAddress] == rig.session,
			identitySessions: rig.svc.identitySessions[rig.session.peerIdentity],
		})
	}

	awaitClosed(t, farEnd.lastRequestSeen, "initPeerSession to send fetch_contacts")
	rig.cm.RetainOnly(context.Background(), evictionPinAddress)
	rig.assertSlotEvicted(t)
	close(farEnd.evicted)

	rig.awaitSessionGoroutine(t)

	// The far end closes lastReplyTaken right after its send returns, which
	// can be after the session goroutine has already finished with the
	// reply, so it is waited for rather than polled.
	select {
	case <-farEnd.lastReplyTaken:
	case <-time.After(2 * time.Second):
		t.Fatal("precondition: initPeerSession never took its last reply, so setup did not complete and the session " +
			"was never at risk of being published")
	}
	if o := observed.Load(); o != nil {
		t.Errorf("a session the CM had already closed was published as live and served: registered in s.sessions = %v, "+
			"identity sessions = %d", o.registered, o.identitySessions)
	}
	rig.svc.peerMu.RLock()
	health := rig.svc.health[rig.svc.resolveHealthAddress(evictionSlotAddress)]
	var lastConnected time.Time
	if health != nil {
		lastConnected = health.LastConnectedAt
	}
	rig.svc.peerMu.RUnlock()
	if !lastConnected.IsZero() {
		t.Errorf("%s was marked connected at %v by a session the CM had already closed", evictionSlotAddress, lastConnected)
	}
}
