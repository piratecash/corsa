package node

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// datagram_lifecycle_test.go pins the LIFECYCLE contract of Run's background
// work, with the datagram plane's four schedules as the case that drives it:
// nothing Run started may still be running when Run returns.
//
// The damage is not an idle goroutine. Every plane worker calls INTO the layer
// with the lifecycle context, and the desktop runtime treats Run returning as
// permission to tear down what those calls reach. A pass still in flight at
// that moment is a call against an owner that has already been stopped.
//
// The plane has NO shutdown subsystem of its own, and that is the point of
// these tests standing here: the schedules are ordinary lifecycle loops on
// runLoopsWg, joined by the one ordered teardown (stopRunLifecycle), so what is
// asserted below is that ONE mechanism covers them. A second wait group existed
// while the plane owned durable stores whose owner had to be stopped after its
// workers; the stores are gone and the guarantees it carried — join before Run
// returns, join before every subsystem defer, cancel before join — are all
// properties of that one teardown.
//
// Three paths produce the damage, and all three are covered here: an ordinary
// cancellation while a pass is inside a call it cannot leave, the early failure
// path where net.Listen fails AFTER the loops were started (which used to leave
// them running under a context nobody had cancelled), and a panic unwinding
// through Run's own startup.

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

// hangingReplaySweep is a maintenance PASS that parks until its context is
// cancelled, and reports that the cancellation arrived while it was still
// executing.
//
// The seam it is installed through is the plane's own pass
// (datagramPlaneParts.replayCacheSweep) and NOT the anti-replay memory: the
// memory is *datagram.BaseReplayCache everywhere, it is arithmetic under one
// mutex and it cannot block, which is precisely why a pass that hangs has to be
// staged one level up. What is under test here is the LIFECYCLE — the loop
// carries the lifecycle context into the pass, and Run may not return while that
// call is still executing.
type hangingReplaySweep struct {
	entered     chan struct{}
	ctxObserved chan struct{}
	// escape releases the pass when the test itself has failed, so a broken
	// assertion reports the failure instead of wedging the fixture's join.
	escape      chan struct{}
	enterOnce   sync.Once
	observeOnce sync.Once
	escapeOnce  sync.Once
	// stubborn keeps the call INSIDE the pass after its context was cancelled,
	// which is the state a join has to be observed against: a cooperative pass
	// returns on cancellation and leaves nothing to wait for.
	stubborn bool
}

func newHangingReplaySweep(stubborn bool) *hangingReplaySweep {
	return &hangingReplaySweep{
		entered:     make(chan struct{}),
		ctxObserved: make(chan struct{}),
		escape:      make(chan struct{}),
		stubborn:    stubborn,
	}
}

// pass is what the plane's maintenance loop calls in place of the cache's own
// full sweep.
func (s *hangingReplaySweep) pass(ctx context.Context) int {
	s.enterOnce.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		s.observeOnce.Do(func() { close(s.ctxObserved) })
		if !s.stubborn {
			return 0
		}
	case <-s.escape:
		return 0
	}
	<-s.escape
	return 0
}

func (s *hangingReplaySweep) unpark() { s.escapeOnce.Do(func() { close(s.escape) }) }

func (s *hangingReplaySweep) unparkOnCleanup(t *testing.T) {
	t.Helper()
	t.Cleanup(s.unpark)
}

// ---------------------------------------------------------------------------
// Ordinary shutdown
// ---------------------------------------------------------------------------

// TestTheJoinPointWaitsForAMaintenancePassInFlight is the finding at
// ordinary-shutdown scope.
//
// The schedules were started with a bare `go`, so nothing anywhere waited for
// them. Cancelling the context told them to stop; it did not tell anyone when
// they HAD stopped, and a maintenance pass still executing kept running while
// the process moved on to tear down the components that pass reaches.
//
// It also carries the second half of that guarantee — that the pass really is
// cancellable, so the join cannot hang for ever on one that stopped answering.
//
// The mutation this kills: starting any of the schedules with a bare `go`
// again, joining them to something Run does not wait on, or handing the
// maintenance pass a background context.
func TestTheJoinPointWaitsForAMaintenancePassInFlight(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)

	sweep := newHangingReplaySweep(true)
	sweep.unparkOnCleanup(t)
	layer := svc.datagramLayer()
	layer.replayCacheSweep = sweep.pass
	// The pass has to be reachable inside a test's patience; the cadence is a
	// field precisely so this needs no ten-second tick.
	layer.maintenancePace = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc.startDatagramSchedules(ctx, layer)

	select {
	case <-sweep.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the maintenance loop never reached the base replay cache's sweep")
	}

	joined := make(chan struct{})
	cancel()
	go func() {
		defer close(joined)
		svc.WaitBackground()
	}()

	select {
	case <-sweep.ctxObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation never reached the pass that was in flight: the loop is not passing the lifecycle context down")
	}

	// A negative has to be observed over SOME window; this one is a liveness
	// barrier and not a schedule, so it pins nothing about timing — the pass is
	// still parked by construction, and the join point must still be open for as
	// long as that is true.
	select {
	case <-joined:
		t.Fatal("the join point returned while a datagram maintenance pass was still executing: the runtime tears the plane's components down at this point, so that call lands on a torn-down owner")
	case <-time.After(200 * time.Millisecond):
	}

	sweep.unpark()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("the join point never returned after the pass finished")
	}
}

// ---------------------------------------------------------------------------
// The early failure path
// ---------------------------------------------------------------------------

// TestRunCancelsTheDatagramPlaneWhenTheListenerFails is the finding at the
// other end: Run returning an ERROR is still Run returning.
//
// The plane is started before the listener is opened, so a bind that fails —
// the address already in use, which is the ordinary way a second instance
// starts — used to leave every schedule running under the CALLER's context.
// That context is typically the process-lifetime one and is not cancelled by
// Run failing, so the loops kept sweeping the caches and writing to the sockets
// that the caller, having been handed an error, was entitled to close.
//
// Chasing it turned up a second defect in the same shape: Run did not merely
// return with a live context, it BLOCKED FOREVER in its own teardown. The CM
// event loop stops on that context and nothing else, and `defer` runs LIFO, so
// a cancel registered at the top of Run runs after the wait for that loop.
//
// The assertions are staged on Run's own teardown signal (`s.done`, closed as
// the teardown begins) rather than on elapsed time, so nothing here is a bet on
// the scheduler. The loop parked below stands for any lifecycle goroutine that
// has not finished; that the REAL datagram schedules are on the same group,
// with a real maintenance pass in flight, is what
// TestTheJoinPointWaitsForAMaintenancePassInFlight pins.
//
// The mutation this kills: assigning the caller's context to s.runCtx, placing
// the cancel where an early `return` or a preceding wait can skip it, or
// dropping the join from Run's teardown.
func TestRunCancelsTheDatagramPlaneWhenTheListenerFails(t *testing.T) {
	t.Parallel()

	// The address is taken from the listener that is HOLDING it, and that
	// listener stays open for the whole test — nothing here guesses a port or
	// races the OS for one. The premise is asserted below rather than assumed:
	// a test that can pass because its premise never armed is worse than one
	// that flakes.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy an address: %v", err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	if probe, err := net.Listen("tcp", blocker.Addr().String()); err == nil {
		_ = probe.Close()
		t.Fatalf("this platform allows a second listener on %s, so Run's listen will not fail and this test's premise cannot arm", blocker.Addr())
	}

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		// The address is already bound, so net.Listen fails AFTER the datagram
		// layer has been started — which is the whole shape of this finding.
		ListenAddress:     blocker.Addr().String(),
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
		EnableDatagramV1:  true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	// A lifecycle loop that is still running when Run exits, holding the join
	// open until the test lets go.
	//
	// It parks on a channel of the test's own and NOT on any of Run's teardown
	// signals: the join runs BEFORE the subsystem teardown, so waiting for
	// `s.done` would be waiting for a step that is itself waiting for this loop.
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	svc.goRunLoop(func() { <-release })

	// The parent context is never cancelled while the assertions run — that is
	// the whole shape of this finding. The cleanup cancel exists only so a
	// FAILING assertion cannot leave Run alive and hang the fixture's join;
	// cleanups run LIFO, so it fires before WaitBackground above.
	parent, stopParent := context.WithCancel(context.Background())
	t.Cleanup(stopParent)
	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(parent) }()

	// The listener fails within milliseconds of the loops starting, so a Run
	// that does not join them returns far inside this window. The window is a
	// liveness barrier and not a schedule: the loop is parked by construction, so
	// Run must not return for as long as the test holds it.
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while a lifecycle loop it owns was still running", err)
	case <-time.After(300 * time.Millisecond):
	}

	unpark()
	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run returned nil for an address that was already bound: the premise of this test never armed")
		}
		if !strings.Contains(err.Error(), "listen on "+blocker.Addr().String()) {
			t.Fatalf("Run failed for some other reason than the bound address (%v): the premise of this test never armed", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run never returned after its last lifecycle loop finished")
	}

	if svc.runCtx.Err() == nil {
		t.Fatal("the datagram lifecycle context is still live after Run returned: the caller was handed an error while work it started keeps running against stores the caller may now close")
	}
}

// TestTheLifecycleLoopsAreJoinedBeforeTheSubsystemsTheyUseAreStopped is the
// ORDERING half of the join.
//
// Joining before Run returns was only half the guarantee. `defer` runs LIFO, so
// a join registered FIRST runs LAST — after the file-transfer manager, the
// relay states and the capture manager have already been stopped, and after
// `s.done` has been closed. Cancellation does not save it: a loop already
// INSIDE a call keeps running until that call returns, which is the very reason
// the join exists. The datagram plane's outbound pump is the standing example —
// it hands frames to the network writer whose connections the teardown closes —
// and its four schedules are on this same group, with no teardown of their own.
//
// The observable is `s.done`, whose close is the LAST of Run's subsystem
// defers: if the loops are joined before the subsystem teardown, `s.done`
// cannot have closed while one is still parked. It discriminates sharply — with
// the join registered first it closes immediately, because the whole teardown
// runs before the join is even reached.
//
// The mutation this kills: moving `defer s.stopRunLifecycle(runCancel)` below
// the subsystem defers (i.e. registering it earlier), or dropping the
// `runLoopsWg.Wait` out of it.
func TestTheLifecycleLoopsAreJoinedBeforeTheSubsystemsTheyUseAreStopped(t *testing.T) {
	t.Parallel()

	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy an address: %v", err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	if probe, err := net.Listen("tcp", blocker.Addr().String()); err == nil {
		_ = probe.Close()
		t.Fatalf("this platform allows a second listener on %s, so Run's listen will not fail and this test's premise cannot arm", blocker.Addr())
	}

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     blocker.Addr().String(),
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
		EnableDatagramV1:  true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	// A lifecycle loop that is still inside its work when Run tears down, which
	// is exactly the state a call in flight leaves behind.
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	svc.goRunLoop(func() { <-release })

	parent, stopParent := context.WithCancel(context.Background())
	t.Cleanup(stopParent)
	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(parent) }()

	// The listener fails, so the teardown starts on its own. While the worker is
	// parked, nothing Run tears down may have been torn down yet.
	select {
	case <-svc.done:
		t.Fatal("Run tore its subsystems down while a lifecycle loop was still running: a subsystem can be stopped under a loop that is inside a call to it")
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while a lifecycle loop it owns was still running", err)
	case <-time.After(300 * time.Millisecond):
	}

	unpark()
	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run returned nil for an address that was already bound: the premise of this test never armed")
		}
		if !strings.Contains(err.Error(), "listen on "+blocker.Addr().String()) {
			t.Fatalf("Run failed for some other reason than the bound address (%v): the premise of this test never armed", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run never returned after its last lifecycle loop finished")
	}
	select {
	case <-svc.done:
	default:
		t.Fatal("Run returned without closing its done channel")
	}
}

// isClosed reports whether a signalling channel has already been closed,
// without blocking on one that has not.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestAPanicDuringRunStartupStillJoinsTheLifecycleLoopsFirst is the finding:
// the correct shutdown order only applied from the point the ordered sequence
// was registered, and the loops start well before that.
//
// `defer` runs LIFO, so a panic raised in that window unwound through the
// subsystem teardowns FIRST — `stopFileTransfer`, `relayStates.stop`,
// `captureManager.Close`, `close(s.done)` — and reached the join only at the
// very end. A lifecycle loop inside a call to one of those subsystems would
// then have that subsystem stopped underneath it, which is the exact hazard the
// join was introduced to prevent, reappearing on the one path nobody had
// ordered.
//
// The panic really does unwind through those defers: crashlog.DeferRecover
// guards goroutines and RE-PANICS by design, and Run itself recovers nothing,
// so the unwind is the ordinary one.
//
// The observable is `relayStates.stopCh`, closed by the subsystem defer
// registered immediately after `stopFileTransfer` and therefore run
// immediately after it. With `stopRunLifecycle` armed above them it runs before
// both; without it, both have already run by the time the join is reached.
//
// The mutation this kills: moving the `defer s.stopRunLifecycle` registration
// back below the subsystem defers, or dropping the `runLoopsWg.Wait` from it.
func TestAPanicDuringRunStartupStillJoinsTheLifecycleLoopsFirst(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:0",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
		EnableDatagramV1:  true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	// A lifecycle loop that is still inside its work when the unwind reaches the
	// join, and that reports what had already been torn down at that moment.
	release := make(chan struct{})
	observed := make(chan bool, 1)
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	svc.goRunLoop(func() {
		<-release
		observed <- isClosed(svc.relayStates.stopCh)
	})

	svc.faultDuringRunStartup = func() {
		panic("startup fault raised inside the unordered window")
	}

	parent, stopParent := context.WithCancel(context.Background())
	t.Cleanup(stopParent)
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_ = svc.Run(parent)
	}()

	// The unwind must be blocked on the join while the loop is parked.
	select {
	case value := <-panicked:
		t.Fatalf("Run unwound to its caller (%v) while a lifecycle loop it owns was still running", value)
	case <-time.After(300 * time.Millisecond):
	}

	unpark()
	select {
	case value := <-panicked:
		if value == nil {
			t.Fatal("the injected fault did not reach the caller: the premise of this test never armed")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run never unwound after its last lifecycle loop finished")
	}

	select {
	case tornDown := <-observed:
		if tornDown {
			t.Fatal("the subsystems the loops use were torn down before the panic unwind joined them: a subsystem can be stopped under a loop that is inside a call to it")
		}
	default:
		t.Fatal("the parked loop never reported")
	}
}

// TestTheLifecycleTeardownReleasesAHungMaintenancePass is the same guarantee
// stated through the teardown Run actually arms, driven against the REAL
// schedules rather than a parked stand-in.
//
// stopRunLifecycle is one closure that cancels AND joins, and it is the only
// thing that has to be right on every exit path — including a panic unwind.
// A maintenance pass parked mid-call is the state that tells the two steps
// apart: cancellation must reach the pass and the closure must then return.
//
// The mutation this kills: dropping `cancel()` from stopRunLifecycle so it
// joins without asking first, which deadlocks the moment any pass is in flight.
func TestTheLifecycleTeardownReleasesAHungMaintenancePass(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)

	sweep := newHangingReplaySweep(false)
	sweep.unparkOnCleanup(t)
	layer := svc.datagramLayer()
	layer.replayCacheSweep = sweep.pass
	layer.maintenancePace = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc.startDatagramSchedules(ctx, layer)

	select {
	case <-sweep.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the maintenance pass never reached the base replay cache's sweep")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		svc.stopRunLifecycle(cancel)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the lifecycle teardown never returned: it joins the loops without cancelling them first, so a pass parked mid-call holds Service.Run open for ever")
	}
}

// TestRunWaitsForALifecycleLoopInsideItsBlockingCall is the mechanism half of
// the join, at the scope of the loops Run itself owns.
//
// Cancellation only ASKS a loop to stop. The announce loop, the routing TTL
// ticker and the probe sender were started with plain goroutines, so Run could
// return while one of them was still inside a network send or a TTL drain —
// with the sockets and stores it uses being closed underneath it. They now go
// through goRunLoop, and this pins what that buys: while a lifecycle loop is
// inside its blocking call, Run does not return; cancellation releases it and
// Run then does.
//
// It parks a loop through the SAME wrapper the four real loops now use, which
// is the mechanism under test; that each of them actually goes through that
// wrapper is what TestRunStartsNoUnjoinedGoroutine enforces. Blocking each real
// loop inside its own call would mean adding seams to internal/core/routing and
// to files this change does not own, and would test those packages' internals
// rather than Run's contract.
//
// The mutation this kills: starting a lifecycle loop outside runLoopsWg, or
// dropping the runLoopsWg wait from stopRunLifecycle.
func TestRunWaitsForALifecycleLoopInsideItsBlockingCall(t *testing.T) {
	t.Parallel()

	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy an address: %v", err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	if probe, err := net.Listen("tcp", blocker.Addr().String()); err == nil {
		_ = probe.Close()
		t.Fatalf("this platform allows a second listener on %s, so Run's listen will not fail and this test's premise cannot arm", blocker.Addr())
	}

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     blocker.Addr().String(),
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	// A lifecycle loop that is inside a blocking call and observes its own
	// cancellation, exactly as a network send under a write deadline does.
	inCall := make(chan struct{})
	released := make(chan struct{})
	svc.goRunLoop(func() {
		close(inCall)
		<-released
	})

	parent, stopParent := context.WithCancel(context.Background())
	t.Cleanup(stopParent)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	t.Cleanup(release)

	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(parent) }()

	select {
	case <-inCall:
	case <-time.After(5 * time.Second):
		t.Fatal("the lifecycle loop never started")
	}
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while a lifecycle loop it owns was inside its blocking call", err)
	case <-time.After(300 * time.Millisecond):
	}

	release()
	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run returned nil for an address that was already bound: the premise of this test never armed")
		}
		if !strings.Contains(err.Error(), "listen on "+blocker.Addr().String()) {
			t.Fatalf("Run failed for some other reason than the bound address (%v)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run never returned after its last lifecycle loop finished")
	}
}

// TestRunWaitsForAGossipJobInFlight is the P1: the gossip dispatch pool — a
// supervisor and thirty-six workers — lived for the whole of Run but was
// tracked by backgroundWg, which Run does not wait on.
//
// So Run could return while a gossip job was inside a dial, a handshake or a
// socket write, with the runtime free to close those underneath it. The pool is
// on the lifecycle group now, and joining it is bounded: a worker refuses to
// START a job once the context is done, so the wait covers only the jobs
// already running, each of which is bounded by the dial timeout, the handshake
// timeout and netcore's write deadline.
//
// The job here is enqueued and observed BEFORE the teardown begins, so nothing
// depends on which of the two the scheduler runs first.
//
// The mutation this kills: starting the gossip workers or their shutdown
// supervisor with goBackground instead of goRunLoop.
func TestRunWaitsForAGossipJobInFlight(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:0",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	parent, stopParent := context.WithCancel(context.Background())
	t.Cleanup(stopParent)
	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(parent) }()

	// Wait for the pool to be up before enqueueing: gossipPoolUp is published
	// after the channels exist and the workers are launched.
	deadline := time.Now().Add(10 * time.Second)
	for !svc.gossipPoolUp.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !svc.gossipPoolUp.Load() {
		t.Fatal("the gossip dispatch pool never came up")
	}

	inJob := make(chan struct{})
	released := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	t.Cleanup(release)
	svc.gossipJobs <- func() {
		close(inJob)
		<-released
	}

	select {
	case <-inJob:
	case <-time.After(10 * time.Second):
		t.Fatal("no gossip worker picked the job up")
	}

	// The job is running. Only now does the shutdown begin, so the worker
	// cannot have abandoned it at the door.
	stopParent()
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while a gossip job it owns was in flight: the pool outlives the Service, and a job inside a dial or a socket write is left running against a runtime entitled to close both", err)
	case <-time.After(300 * time.Millisecond):
	}

	release()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v after a clean cancellation", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run never returned after its last gossip job finished")
	}
}

// TestTheInboundDrainRunsBeforeTheLifecycleJoin pins Run's two joins AGAINST
// EACH OTHER, which nothing did — and which is why the code, its comment and
// docs/locking.md were free to disagree about the order for as long as they did.
//
// `defer` runs LIFO, so the drain of inbound connections (`closeAllInboundConns`
// + `connWg.Wait`), registered BELOW `defer s.stopRunLifecycle`, runs BEFORE the
// lifecycle join. That is the correct order — the drain stops no subsystem a
// loop calls into, while the connection handlers are producers into what the
// loops consume — and the argument is written out on stopRunLifecycle.
//
// The observable is the inbound SOCKET. A lifecycle loop is parked, so the join
// cannot complete; if the drain runs first the peer sees its connection closed
// while that loop is still parked, and if the join runs first nothing closes the
// connection until the test lets the loop go. Nothing else closes it in that
// window: the handler's read loop watches no context, and no heartbeat is armed
// for a socket that never sent a `hello`.
//
// The mutation this kills: registering `defer s.stopRunLifecycle(runCancel)`
// below the drain (or the drain above it), so the loops are joined first.
func TestTheInboundDrainRunsBeforeTheLifecycleJoin(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     address,
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	svc.disableRateLimiting = true
	t.Cleanup(svc.WaitBackground)

	// A lifecycle loop that is still inside its work when the teardown starts,
	// which is the state that tells the two joins apart.
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	svc.goRunLoop(func() { <-release })

	parent, stopParent := context.WithCancel(context.Background())
	t.Cleanup(stopParent)
	runDone := make(chan error, 1)
	go func() { runDone <- svc.Run(parent) }()

	client := dialWhenListening(t, address)
	t.Cleanup(func() { _ = client.Close() })
	waitForConditionMsg(t, 5*time.Second, "the node never registered the inbound connection", func() bool {
		return inboundConnCount(svc) > 0
	})

	// The peer's half: report the instant the node closes the socket.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		buf := make([]byte, 1)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()

	// The premise: nothing has closed the connection yet, so a close observed
	// after the cancellation below really is the drain's.
	select {
	case <-closed:
		t.Fatal("the inbound connection was closed before the shutdown began: the premise of this test never armed")
	case <-time.After(200 * time.Millisecond):
	}

	stopParent()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the inbound connection was still open while a lifecycle loop was parked: " +
			"the lifecycle join is running BEFORE the inbound drain, so the connection handlers " +
			"are left producing session-closed work against consumers that were already joined")
	}
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while a lifecycle loop it owns was still parked", err)
	default:
	}

	unpark()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v after a clean cancellation", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run never returned after its last lifecycle loop finished")
	}
}

// dialWhenListening opens one TCP connection to the node, retrying until its
// listener is up. The connection is the test's own peer and stays raw: it sends
// nothing, so no heartbeat is armed for it and only the shutdown drain closes it.
func dialWhenListening(t *testing.T, address string) net.Conn {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", address, 200*time.Millisecond)
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("the node never started listening on %s: %v", address, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// inboundConnCount reports how many inbound connections the node's network
// registry currently holds — the population `closeAllInboundConns` walks.
func inboundConnCount(svc *Service) int {
	count := 0
	svc.Network().Enumerate(context.Background(), netcore.Inbound, func(domain.ConnID) bool {
		count++
		return true
	})
	return count
}

// blockingSendNetwork parks every frame send until the test releases it, which
// is how a real socket behaves against a peer that has stopped reading: the
// write blocks until its deadline.
type blockingSendNetwork struct {
	netcore.Network
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
}

func (n *blockingSendNetwork) SendFrame(context.Context, domain.ConnID, []byte) error {
	n.enterOnce.Do(func() { close(n.entered) })
	<-n.release
	return nil
}

func (n *blockingSendNetwork) RemoteAddr(domain.ConnID) string { return "" }

// TestTheInboundHeartbeatIsJoinedNotMerelyStopped is the P1.
//
// The connection handler closed the heartbeat's stop channel on its way out and
// returned. Closing only ASKS: the loop can be inside its ping send at that
// moment, and it goes on to touch the Network and peerMu afterwards. So connWg
// — and therefore Run — could complete while a heartbeat was still writing to a
// socket the runtime is entitled to have closed.
//
// This drives the real loop in the state that matters: parked inside the send,
// with its stop channel already closed. If closing the channel were the whole
// story the goroutine would be gone; the join is what proves it is not.
//
// The mutation this kills: dropping heartbeatDone.Wait() from the handler's
// teardown, or starting the heartbeat without adding it to that group.
func TestTheInboundHeartbeatIsJoinedNotMerelyStopped(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:0",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	network := &blockingSendNetwork{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(network.release) }) }
	t.Cleanup(release)

	svc.networkOverride = network
	svc.runCtx = context.Background()
	svc.heartbeatIntervalOverride = time.Millisecond

	// Exactly the shape handleConn uses: a stop channel plus the wait group its
	// teardown blocks on.
	stop := make(chan struct{})
	var heartbeatDone sync.WaitGroup
	heartbeatDone.Add(1)
	go func() {
		defer heartbeatDone.Done()
		svc.inboundHeartbeat(domain.ConnID(1), domain.PeerAddress("10.9.20.1:64646"), stop)
	}()

	select {
	case <-network.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat never reached its ping send")
	}

	// The handler's half: ask it to stop, then wait for it. The ask alone must
	// not be mistaken for the answer.
	close(stop)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		heartbeatDone.Wait()
	}()
	select {
	case <-joined:
		t.Fatal("the heartbeat was reported finished while it was still inside its ping send: closing the stop channel is an ask, not a join, and connWg would complete with a write still in flight")
	case <-time.After(300 * time.Millisecond):
	}

	release()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat never exited after its send returned")
	}
}
