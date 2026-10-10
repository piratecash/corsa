package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// connection_manager_retain_only_lifecycle_test.go pins what RetainOnly does
// around the life of the event loop it hands its work to, and the
// level-triggered pin rule: while a connect_only pin is live, the slot table
// holds only the live pin.

// retainOnlyReturnBound is how long a RetainOnly that must not block is given
// to return. It only bounds a hang; the assertions are on the result.
const retainOnlyReturnBound = 2 * time.Second

// retainOnlyAsync runs RetainOnly on its own goroutine and returns its result
// channel, so a test can fail on a hang instead of hanging.
func retainOnlyAsync(ctx context.Context, cm *ConnectionManager, keep domain.PeerAddress) <-chan bool {
	result := make(chan bool, 1)
	go func() { result <- cm.RetainOnly(ctx, keep) }()
	return result
}

func awaitRetainOnlyResult(t *testing.T, result <-chan bool, what string) bool {
	t.Helper()
	select {
	case applied := <-result:
		return applied
	case <-time.After(retainOnlyReturnBound):
		t.Fatalf("RetainOnly blocked %s", what)
		return false
	}
}

// runCMUntilStopped starts the event loop and returns a stop function that
// cancels it and waits until Run has returned, i.e. shutdown has finished.
func runCMUntilStopped(t *testing.T, cm *ConnectionManager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cm.Run(ctx)
	}()
	<-cm.Ready()
	return func() {
		cancel()
		awaitClosed(t, stopped, "the event loop to return")
	}
}

// livePin is a test pin source for ConnectionManagerConfig.ConnectOnlyFn that
// the test moves while the manager runs.
type livePin struct {
	mu      sync.Mutex
	address domain.PeerAddress
	pinned  bool
}

func (p *livePin) set(address domain.PeerAddress) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.address, p.pinned = address, true
}

func (p *livePin) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.address, p.pinned = "", false
}

func (p *livePin) read() (domain.PeerAddress, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.address, p.pinned
}

// Before Run there is no event loop to take the request and no slot to evict,
// so RetainOnly reports that it did nothing instead of waiting for a loop that
// does not exist.
func TestCM_RetainOnlyBeforeRun_ReturnsFalseWithoutBlocking(t *testing.T) {
	cm := testCMConfig("10.0.0.1:64646").Build()

	if awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, mustAddr("10.0.0.1:64646")), "before Run") {
		t.Error("RetainOnly before Run = true, want false: no event loop applied it")
	}
}

// After shutdown the event loop is gone and the slot table is empty; the same
// holds as before Run.
func TestCM_RetainOnlyAfterShutdown_ReturnsFalseWithoutBlocking(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }
	b.Cfg.FillInterval = time.Hour
	cm := b.Build()
	stop := runCMUntilStopped(t, cm)
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "2 active slots", func() bool { return cm.ActiveCount() == 2 })
	stop()

	if awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, mustAddr("10.0.0.1:64646")), "after shutdown") {
		t.Error("RetainOnly after shutdown = true, want false: no event loop applied it")
	}
	if got := cm.SlotCount(); got != 0 {
		t.Errorf("slots after shutdown = %d, want 0", got)
	}
}

// A request that reached the channel while shutdown was already under way —
// its producer passed EmitSlot's gate just before shutdown closed it — is never
// handled by the loop. The shutdown drain must still settle it, or its waiter
// would wait for good.
func TestCM_RetainOnlyRequestLeftAtShutdown_IsSettledByTheDrain(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.FillInterval = time.Hour
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	// The dial ignores its context, so shutdown parks in dialWg.Wait with the
	// emit gate already closed — the window in which a late request sits in
	// the channel with no loop to handle it.
	b.Cfg.DialFn = func(context.Context, []domain.PeerAddress) (DialResult, error) {
		close(dialStarted)
		<-releaseDial
		return DialResult{}, errors.New("dial released by the test")
	}
	cm := b.Build()
	stop := runCMUntilStopped(t, cm)
	cm.NotifyBootstrapReady()
	awaitClosed(t, dialStarted, "the fill to start its dial")

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		stop()
	}()
	waitFor(t, 2*time.Second, "shutdown to close the emit gate", func() bool { return cm.accepting.Load() == 0 })

	late := newRetainOnlyRequest(mustAddr("203.0.113.9:64646"))
	cm.slotEvents <- late
	close(releaseDial)

	awaitClosed(t, late.applied, "the shutdown drain to settle the undelivered RetainOnly request")
	awaitClosed(t, stopDone, "shutdown to finish")
}

// A request enqueued after the drain has run is settled by nobody. Its waiter
// must return on the loop's context instead.
func TestCM_RetainOnlyRequestEnqueuedAfterTheDrain_WaiterReturns(t *testing.T) {
	cm := testCMConfig("10.0.0.1:64646").Build()
	stop := runCMUntilStopped(t, cm)
	stop()

	late := newRetainOnlyRequest(mustAddr("203.0.113.9:64646"))
	cm.slotEvents <- late
	result := make(chan bool, 1)
	go func() { result <- cm.awaitRetainOnly(context.Background(), late) }()

	if awaitRetainOnlyResult(t, result, "on a request nobody will settle") {
		t.Error("awaitRetainOnly = true for a request no event loop applied")
	}
}

// A request the loop settled is reported as applied even when the loop's
// context has also ended by the time the waiter looks: the eviction did run,
// and "false" would tell the caller it did not.
func TestCM_RetainOnlyAwait_PrefersAppliedOverAnEndedContext(t *testing.T) {
	cm := testCMConfig("10.0.0.1:64646").Build()
	stop := runCMUntilStopped(t, cm)
	stop()

	for i := 0; i < 200; i++ {
		settled := newRetainOnlyRequest(mustAddr("10.0.0.1:64646"))
		settled.settle()
		if !cm.awaitRetainOnly(context.Background(), settled) {
			t.Fatalf("iteration %d: awaitRetainOnly = false for a settled request whose loop has stopped", i)
		}
	}
}

// The caller's context bounds the wait: an RPC whose client went away must not
// stay parked on a busy event loop.
func TestCM_RetainOnly_CallerContextEndsTheWait(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.FillInterval = time.Hour
	gate := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(releaseGate)
	entered := make(chan SessionInfo, 1)
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		reportSessionEstablished(t, entered, info)
		<-gate
	}
	var enqueued <-chan struct{}
	b.Cfg.RetainOnlyEnqueued, enqueued = retainEnqueuedSignal()
	cm := b.Build()
	cancelCM := runCM(cm)
	t.Cleanup(cancelCM)
	cm.NotifyBootstrapReady()
	awaitSessionEstablished(t, entered)

	ctx, cancel := context.WithCancel(context.Background())
	result := retainOnlyAsync(ctx, cm, mustAddr("203.0.113.9:64646"))
	awaitClosed(t, enqueued, "the request to be enqueued behind the held loop")
	cancel()

	if awaitRetainOnlyResult(t, result, "after its caller's context ended") {
		t.Error("RetainOnly = true while the loop was still held: nothing had been applied")
	}
}

// RetainOnly(A) is applied against the LIVE pin, not against the address it
// was asked for. A request queued for a pin the operator has since replaced
// retains the newer pin; one for a pin since cleared evicts nothing.
func TestCM_RetainOnly_RetainsTheLivePinNotTheRequestedOne(t *testing.T) {
	requested := mustAddr("10.0.0.1:64646")
	newer := mustAddr("10.0.0.2:64646")
	addresses := []string{string(requested), string(newer), "10.0.0.3:64646"}

	cases := []struct {
		name string
		move func(*livePin)
		want int
		keep domain.PeerAddress
	}{
		{name: "re-pinned to another peer", move: func(p *livePin) { p.set(newer) }, want: 1, keep: newer},
		{name: "pin cleared", move: func(p *livePin) { p.clear() }, want: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pin := &livePin{}
			b := testCMConfig(addresses...)
			b.Cfg.MaxSlotsFn = func() int { return 3 }
			b.Cfg.FillInterval = time.Hour
			b.Cfg.ConnectOnlyFn = pin.read
			b.Cfg.Provider.config.ConnectOnlyFn = pin.read
			cm := b.Build()
			cancel := runCM(cm)
			defer cancel()
			cm.NotifyBootstrapReady()
			waitFor(t, 2*time.Second, "3 active slots", func() bool { return cm.ActiveCount() == 3 })

			tc.move(pin)
			if !awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, requested), "on a running loop") {
				t.Fatal("RetainOnly = false on a running event loop")
			}
			got := slotAddresses(cm)
			if len(got) != tc.want {
				t.Fatalf("slots after RetainOnly(%s) = %v, want %d", requested, got, tc.want)
			}
			if tc.keep != "" && got[0] != tc.keep {
				t.Errorf("slot kept = %s, want the live pin %s", got[0], tc.keep)
			}

			// Control: the very same request does evict once a pin is
			// live, so "nothing evicted" above was the loop applying the
			// live pin, not a request that went nowhere.
			pin.set(requested)
			if !awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, requested), "on a running loop") {
				t.Fatal("RetainOnly = false on a running event loop")
			}
			if got := slotAddresses(cm); len(got) != 1 || got[0] != requested {
				t.Errorf("slots after RetainOnly(%s) with it pinned = %v, want only it", requested, got)
			}
		})
	}
}

func TestCM_RetainOnlyForTheLivePin_IsApplied(t *testing.T) {
	pinned := mustAddr("10.0.0.2:64646")
	// Pinned only after the slots exist, as when connect_only is issued on a
	// node that already dials freely.
	pin := &livePin{}
	b := testCMConfig("10.0.0.1:64646", string(pinned), "10.0.0.3:64646")
	b.Cfg.MaxSlotsFn = func() int { return 3 }
	b.Cfg.FillInterval = time.Hour
	b.Cfg.ConnectOnlyFn = pin.read
	b.Cfg.Provider.config.ConnectOnlyFn = pin.read
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "3 active slots", func() bool { return cm.ActiveCount() == 3 })
	pin.set(pinned)

	if !awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, pinned), "on the live pin") {
		t.Fatal("RetainOnly = false on a running event loop")
	}
	if got := slotAddresses(cm); len(got) != 1 || got[0] != pinned {
		t.Errorf("slots after RetainOnly(%s) = %v, want only the pinned one", pinned, got)
	}
}

// The pin rule is level-triggered: every fill re-applies it, so slots that a
// lost request (an abandoned caller, a pin written without one) left behind
// are evicted at the next fill instead of living forever.
func TestCM_FillEnforcesTheLivePin(t *testing.T) {
	pinned := mustAddr("10.0.0.2:64646")
	pin := &livePin{}
	b := testCMConfig("10.0.0.1:64646", string(pinned), "10.0.0.3:64646")
	b.Cfg.MaxSlotsFn = func() int { return 3 }
	b.Cfg.FillInterval = time.Hour
	b.Cfg.ConnectOnlyFn = pin.read
	b.Cfg.Provider.config.ConnectOnlyFn = pin.read
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "3 active slots", func() bool { return cm.ActiveCount() == 3 })

	pin.set(pinned)
	cm.EmitHint(NewPeersDiscovered{Count: 1})

	waitFor(t, 2*time.Second, "the fill to evict every slot but the pin", func() bool {
		got := slotAddresses(cm)
		return len(got) == 1 && got[0] == pinned
	})
}

// add_peer of another peer while egress is pinned must not open an outbound
// slot: the pin says this node dials only the pinned address.
func TestCM_ManualPeerUnderALivePin_IsNotDialled(t *testing.T) {
	pinned := mustAddr("10.0.0.2:64646")
	other := mustAddr("10.0.0.7:64646")
	pin := &livePin{}
	pin.set(pinned)
	b := testCMConfig(string(pinned))
	b.Cfg.MaxSlotsFn = func() int { return 3 }
	b.Cfg.FillInterval = time.Hour
	b.Cfg.ConnectOnlyFn = pin.read
	b.Cfg.Provider.config.ConnectOnlyFn = pin.read
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	b.Cfg.EventBus = bus
	view := newSlotStateView()
	bus.Subscribe(ebus.TopicSlotStateChanged, view.record, ebus.WithSync())
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "the pinned slot", func() bool { return cm.ActiveCount() == 1 })

	if !cm.EmitSlot(ManualPeerRequested{Address: other}) {
		t.Fatal("the manager refused the manual peer request")
	}
	// The request is handled after the manual peer, so once it returns the
	// manual peer has been handled too.
	if !awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, pinned), "as a barrier") {
		t.Fatal("RetainOnly = false on a running event loop")
	}
	if n := view.eventsFor(other); n != 0 {
		t.Errorf("%s got %d slot-state publications under a pin to %s, want none: it was dialled", other, n, pinned)
	}
}

// A request the loop could receive must never take it down, even if some
// in-package code built one without its constructor.
func TestRetainOnlyRequest_ZeroValueSettleIsHarmless(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("settling a zero-value retainOnlyRequest panicked: %v", r)
		}
	}()
	var zero retainOnlyRequest
	zero.settle()
}

// When the slot limit drops while a pin is live, the pin rule runs before the
// shrink: shrinkToLimit prefers non-active victims, and the pinned slot is
// exactly the one still dialling or initializing right after connect_only.
func TestCM_PinRuleRunsBeforeShrink_PinnedSlotSurvivesALimitDrop(t *testing.T) {
	active := mustAddr("10.0.0.1:64646")
	pinned := mustAddr("10.0.0.2:64646")
	pin := &livePin{}
	var maxSlots atomic.Int32
	maxSlots.Store(2)
	b := testCMConfig(string(active), string(pinned))
	b.Cfg.MaxSlotsFn = func() int { return int(maxSlots.Load()) }
	b.Cfg.FillInterval = time.Hour
	b.Cfg.ConnectOnlyFn = pin.read
	b.Cfg.Provider.config.ConnectOnlyFn = pin.read
	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
		if addresses[0] == pinned {
			// The pinned peer is still dialling when the limit drops.
			<-ctx.Done()
			return DialResult{}, ctx.Err()
		}
		return dialFn(ctx, addresses)
	}
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	b.Cfg.EventBus = bus
	view := newSlotStateView()
	bus.Subscribe(ebus.TopicSlotStateChanged, view.record, ebus.WithSync())
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "one active and one dialling slot", func() bool {
		return cm.ActiveCount() == 1 && cm.SlotCount() == 2
	})

	pin.set(pinned)
	maxSlots.Store(1)
	cm.EmitHint(NewPeersDiscovered{Count: 1})

	waitFor(t, 2*time.Second, "the unpinned slot to be evicted", func() bool { return view.stateOf(active) == "" && view.eventsFor(active) > 0 })
	if n, state := view.eventsFor(pinned), view.stateOf(pinned); n != 1 || state != domain.SlotStateDialing.String() {
		t.Errorf("pinned slot saw %d publications and is %q, want only its first dialing: the limit drop evicted the pin", n, state)
	}
}

// A retain that leaves the live pin without a slot — its own manual dial was
// refused by the same-IP dedup while an unpinned port of that host held a slot
// — refills at once instead of leaving egress at zero until the periodic fill.
func TestCM_RetainOnlyRefillsAPinWithoutASlot(t *testing.T) {
	sameHost := mustAddr("10.0.0.1:64646")
	pinned := mustAddr("10.0.0.1:7777")
	pin := &livePin{}
	b := testCMConfig(string(sameHost))
	b.Cfg.MaxSlotsFn = func() int { return 2 }
	b.Cfg.FillInterval = time.Hour
	b.Cfg.ConnectOnlyFn = pin.read
	b.Cfg.Provider.config.ConnectOnlyFn = pin.read
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "the same-host slot", func() bool { return cm.ActiveCount() == 1 })

	b.Cfg.Provider.Add(pinned, domain.PeerSourceManual)
	pin.set(pinned)
	if !cm.EmitSlot(ManualPeerRequested{Address: pinned}) {
		t.Fatal("the manager refused the manual peer request")
	}
	if !awaitRetainOnlyResult(t, retainOnlyAsync(context.Background(), cm, pinned), "on a running loop") {
		t.Fatal("RetainOnly = false on a running event loop")
	}

	if got := slotAddresses(cm); len(got) != 1 || got[0] != pinned {
		t.Errorf("slots once RetainOnly(%s) returned = %v, want the pin dialled at once", pinned, got)
	}
}
