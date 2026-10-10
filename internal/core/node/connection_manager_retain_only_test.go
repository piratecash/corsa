package node

import (
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// TestCM_RetainOnly_EvictsAllButPinned fills several slots, then pins one with
// RetainOnly and verifies every other outbound slot is evicted while the pinned
// peer survives.
func TestCM_RetainOnly_EvictsAllButPinned(t *testing.T) {
	keep := "10.0.0.2:64646"
	b := testCMConfig("10.0.0.1:64646", keep, "10.0.0.3:64646")
	b.Cfg.MaxSlotsFn = func() int { return 3 }
	// Disable the periodic fill ticker so RetainOnly is observed in
	// isolation — in production the connectOnly Candidates() gate is what
	// keeps a refill from re-dialing the evicted peers.
	b.Cfg.FillInterval = time.Hour

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "3 active slots", func() bool {
		return cm.ActiveCount() == 3
	})

	cm.RetainOnly(context.Background(), mustAddr(keep))

	waitFor(t, 2*time.Second, "only pinned slot remains", func() bool {
		return cm.SlotCount() == 1
	})

	slots := cm.Slots()
	if len(slots) != 1 {
		t.Fatalf("expected 1 slot, got %d", len(slots))
	}
	if slots[0].Address != mustAddr(keep) {
		t.Errorf("expected pinned slot %s, got %s", keep, slots[0].Address)
	}
}

// TestCM_RetainOnly_NoMatchEvictsAll verifies that pinning an address with no
// current slot evicts everything (the caller enqueues the pinned dial
// separately via ManualPeerRequested).
func TestCM_RetainOnly_NoMatchEvictsAll(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }
	b.Cfg.FillInterval = time.Hour

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "2 active slots", func() bool {
		return cm.ActiveCount() == 2
	})

	cm.RetainOnly(context.Background(), mustAddr("203.0.113.9:64646"))

	waitFor(t, 2*time.Second, "all slots evicted", func() bool {
		return cm.SlotCount() == 0
	})
}

// cmCallbackLog records the order in which ConnectionManager callbacks start
// and finish, from whichever goroutine they run on.
type cmCallbackLog struct {
	mu     sync.Mutex
	events []string
}

func (l *cmCallbackLog) record(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *cmCallbackLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// TestCM_RetainOnlyDuringSessionEstablished_SlotIsNotTornDownUnderTheCallback
// pins the single-writer contract of the slot table against RetainOnly, the
// eviction an operator command drives from outside the event loop
// (Service.enableConnectOnly calls it on the RPC / console goroutine).
//
// OnSessionEstablished is held open on the event loop while RetainOnly evicts
// the very slot it was handed. With the event loop as the sole writer, nothing
// can change that slot until the callback returns, so:
//
//   - the slot the callback was handed is still in the table, Initializing,
//     at the generation in its SessionInfo, for as long as the callback runs;
//   - OnSessionTeardown for that session runs only AFTER OnSessionEstablished
//     for it has returned — never concurrently with it, never before it.
//
// The eviction itself must still happen once the callback returns.
func TestCM_RetainOnlyDuringSessionEstablished_SlotIsNotTornDownUnderTheCallback(t *testing.T) {
	slotAddress := mustAddr("10.0.0.1:64646")
	b := testCMConfig(string(slotAddress))
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	// No periodic refill: the test observes RetainOnly alone, and a refill of
	// the evicted address would blur which generation is gone.
	b.Cfg.FillInterval = time.Hour

	gate := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	// A failed assertion must not leave the event loop parked on the gate.
	t.Cleanup(releaseGate)

	var callbacks cmCallbackLog
	entered := make(chan SessionInfo, 1)
	returned := make(chan SessionInfo, 1)
	var slotsUnderCallback atomic.Pointer[[]SlotInfo]
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		callbacks.record("established:enter")
		reportSessionEstablished(t, entered, info)
		<-gate
		// Read while the callback is still on the event loop: whatever the
		// table says now is what the callback is entitled to rely on.
		slots := b.cmPtr.Load().Slots()
		slotsUnderCallback.Store(&slots)
		callbacks.record("established:return")
		reportSessionEstablished(t, returned, info)
	}
	tornDown := make(chan SessionInfo, 1)
	b.Cfg.OnSessionTeardown = func(info SessionInfo) {
		callbacks.record("teardown")
		select {
		case tornDown <- info:
		default:
			t.Errorf("unexpected second OnSessionTeardown for %s", info.Address)
		}
	}

	var retainEnqueued <-chan struct{}
	b.Cfg.RetainOnlyEnqueued, retainEnqueued = retainEnqueuedSignal()
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()

	info := awaitSessionEstablished(t, entered)

	retainDone := evictWithRetainOnlyAcrossHeldCallback(t, cm, retainEnqueued, mustAddr("203.0.113.9:64646"))
	releaseGate()
	awaitSessionEstablished(t, returned)

	var teardown SessionInfo
	select {
	case teardown = <-tornDown:
	case <-time.After(2 * time.Second):
		t.Fatalf("RetainOnly never tore down the evicted session; callbacks = %v", callbacks.snapshot())
	}
	awaitClosed(t, retainDone, "RetainOnly to return after the event loop was released")

	if teardown.Session != info.Session {
		t.Fatalf("teardown was for session %p, want the established one %p", teardown.Session, info.Session)
	}

	want := []string{"established:enter", "established:return", "teardown"}
	if got := callbacks.snapshot(); !slices.Equal(got, want) {
		t.Errorf("callback order = %v, want %v: OnSessionTeardown ran while OnSessionEstablished "+
			"for the same session was still running on the event loop", got, want)
	}

	assertSlotHeldUnderCallback(t, slotsUnderCallback.Load(), info)

	for _, s := range cm.Slots() {
		if s.Generation == info.SlotGeneration {
			t.Errorf("slot %s at generation %d survived RetainOnly", s.Address, s.Generation)
		}
	}
}

// assertSlotHeldUnderCallback checks that the slot an OnSessionEstablished
// callback was handed was still in the table, Initializing, at the
// callback's generation, while the callback ran.
func assertSlotHeldUnderCallback(t *testing.T, slots *[]SlotInfo, info SessionInfo) {
	t.Helper()
	if slots == nil {
		t.Fatal("OnSessionEstablished never read the slot table")
	}
	for _, s := range *slots {
		if s.Address != info.Address {
			continue
		}
		if s.Generation != info.SlotGeneration || s.State != domain.SlotStateInitializing {
			t.Errorf("slot under OnSessionEstablished = %s at generation %d, want %s at generation %d",
				s.State, s.Generation, domain.SlotStateInitializing, info.SlotGeneration)
		}
		return
	}
	t.Errorf("slot %s was removed from the table while OnSessionEstablished for it was still running; table = %+v",
		info.Address, *slots)
}

// TestCM_RetainOnlyDuringFillCandidates_NoUnpinnedSlotSurvives pins the
// connect_only egress guarantee against a fill that is already past its
// Candidates() pin check when the pin is set.
//
// fill() asks the provider for candidates without cm.mu, and Candidates()
// reads the pin once, before it walks the known peers. The test parks that
// walk in ForbiddenFn, an existing provider hook it calls per peer AFTER the
// pin was read, then does what Service.enableConnectOnly does — store the pin,
// RetainOnly(pin) — and lets the walk finish. The candidate list it returns
// was computed without the pin. Once RetainOnly has returned, no outbound slot
// other than the pinned one may exist: a slot fill appends from that stale
// list is otherwise left for the next fill's pin enforcement — and this rig
// wires no pin source into the manager, so here nothing else would evict it.
func TestCM_RetainOnlyDuringFillCandidates_NoUnpinnedSlotSurvives(t *testing.T) {
	pin := mustAddr("10.0.0.1:64646")
	others := []domain.PeerAddress{mustAddr("10.0.0.2:64646"), mustAddr("10.0.0.3:64646")}

	gate := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(releaseGate)
	walkParked := make(chan struct{})
	var parkArmed atomic.Bool

	var cmPtr atomic.Pointer[ConnectionManager]
	var pinned atomic.Pointer[domain.PeerAddress]
	ppCfg := testProviderConfig()
	ppCfg.QueuedFn = func() map[string]struct{} {
		if cm := cmPtr.Load(); cm != nil {
			return cm.QueuedIPs()
		}
		return nil
	}
	ppCfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) {
		if p := pinned.Load(); p != nil {
			return *p, true
		}
		return "", false
	}
	ppCfg.ForbiddenFn = func(net.IP) bool {
		if parkArmed.CompareAndSwap(true, false) {
			close(walkParked)
			<-gate
		}
		return false
	}
	pp := NewPeerProvider(ppCfg)
	for _, addr := range append([]domain.PeerAddress{pin}, others...) {
		pp.Add(addr, domain.PeerSourceBootstrap)
	}

	dialFn, dialled := fakeDialFn()
	retainEnqueuedHook, retainEnqueued := retainEnqueuedSignal()
	cm := NewConnectionManager(ConnectionManagerConfig{
		RetainOnlyEnqueued: retainEnqueuedHook,
		MaxSlotsFn:         func() int { return 3 },
		Provider:           pp,
		DialFn:             dialFn,
		OnSessionEstablished: func(info SessionInfo) {
			cmPtr.Load().EmitSlot(SessionInitReady{Address: info.Address, SlotGeneration: info.SlotGeneration})
		},
		OnSessionTeardown: func(SessionInfo) {},
		BackoffFn:         func(int) time.Duration { return 0 },
		NowFn:             func() time.Time { return time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC) },
		FillInterval:      time.Hour,
	})
	cmPtr.Store(cm)
	cancel := runCM(cm)
	defer cancel()

	parkArmed.Store(true)
	cm.NotifyBootstrapReady()
	select {
	case <-walkParked:
	case <-time.After(2 * time.Second):
		t.Fatal("the bootstrap fill never reached Candidates(): the premise of this test never armed")
	}

	pinned.Store(&pin)
	retainDone := evictWithRetainOnlyAcrossHeldCallback(t, cm, retainEnqueued, pin)
	releaseGate()

	// Every slot a fill appends is appended under cm.mu before its first dial
	// worker starts, so the first dial means this fill's appends are done —
	// whichever slots it appended.
	select {
	case <-dialled:
	case <-time.After(2 * time.Second):
		t.Fatal("the fill released from Candidates() dialled nothing")
	}
	awaitClosed(t, retainDone, "RetainOnly to return after the fill was released")

	for _, s := range cm.Slots() {
		if s.Address != pin {
			t.Errorf("slot %s (%s) survived RetainOnly(%s): fill appended it from a candidate list computed before the pin",
				s.Address, s.State, pin)
		}
	}
}

// slotStateView is what a TopicSlotStateChanged subscriber believes the slot
// table holds: the last state published per address, "" meaning removed.
type slotStateView struct {
	mu     sync.Mutex
	states map[domain.PeerAddress]string
	events map[domain.PeerAddress]int
	notify chan struct{}
}

func newSlotStateView() *slotStateView {
	return &slotStateView{
		states: make(map[domain.PeerAddress]string),
		events: make(map[domain.PeerAddress]int),
		notify: make(chan struct{}, 64),
	}
}

func (v *slotStateView) record(address domain.PeerAddress, state string) {
	v.mu.Lock()
	v.states[address] = state
	v.events[address]++
	v.mu.Unlock()
	select {
	case v.notify <- struct{}{}:
	default:
	}
}

func (v *slotStateView) eventsFor(address domain.PeerAddress) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.events[address]
}

func (v *slotStateView) stateOf(address domain.PeerAddress) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.states[address]
}

// TestCM_RetainOnlyBetweenFillAndItsPublication_LeavesNoGhostSlot pins the
// order of slot-state publications against RetainOnly.
//
// fill() appends a slot under cm.mu and publishes its "dialing" state after
// releasing the lock. A synchronous subscriber placed ahead of the observing
// one holds that publication — on the event loop, past the unlock — while
// RetainOnly evicts the slot and publishes its removal. A subscriber's view
// must end where the slot table ends: once the slot is gone and every
// publication about it has been delivered, the view must not still show it.
func TestCM_RetainOnlyBetweenFillAndItsPublication_LeavesNoGhostSlot(t *testing.T) {
	slotAddress := mustAddr("10.0.0.1:64646")
	b := testCMConfig(string(slotAddress))
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.FillInterval = time.Hour
	// The dial never completes, so the slot stays Dialing and publishes
	// nothing further of its own.
	b.Cfg.DialFn = func(ctx context.Context, _ []domain.PeerAddress) (DialResult, error) {
		<-ctx.Done()
		return DialResult{}, ctx.Err()
	}
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	b.Cfg.EventBus = bus

	gate := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(releaseGate)
	publicationHeld := make(chan struct{})
	var holdOnce sync.Once
	// Subscribed first, so the bus calls it before the view below.
	bus.Subscribe(ebus.TopicSlotStateChanged, func(address domain.PeerAddress, state string) {
		if address != slotAddress || state != domain.SlotStateDialing.String() {
			return
		}
		holdOnce.Do(func() {
			close(publicationHeld)
			<-gate
		})
	}, ebus.WithSync())
	view := newSlotStateView()
	bus.Subscribe(ebus.TopicSlotStateChanged, view.record, ebus.WithSync())

	var retainEnqueued <-chan struct{}
	b.Cfg.RetainOnlyEnqueued, retainEnqueued = retainEnqueuedSignal()
	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()

	awaitClosed(t, publicationHeld, "fill to publish the dialing state")
	retainDone := evictWithRetainOnlyAcrossHeldCallback(t, cm, retainEnqueued, mustAddr("203.0.113.9:64646"))
	releaseGate()
	awaitClosed(t, retainDone, "RetainOnly to return after the event loop was released")

	// The view hears about this slot exactly twice — dialing and removal —
	// in whatever order the manager publishes them.
	deadline := time.After(2 * time.Second)
	for view.eventsFor(slotAddress) < 2 {
		select {
		case <-view.notify:
		case <-deadline:
			t.Fatalf("the view received %d publications for %s, want dialing and removal", view.eventsFor(slotAddress), slotAddress)
		}
	}

	if got := cm.SlotCount(); got != 0 {
		t.Fatalf("precondition: RetainOnly left %d slots", got)
	}
	if state := view.stateOf(slotAddress); state != "" {
		t.Errorf("subscriber view shows %s as %q after RetainOnly removed it: the removal was published before "+
			"the state it removes", slotAddress, state)
	}
}
