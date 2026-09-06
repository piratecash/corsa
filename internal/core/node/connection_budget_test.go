package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// connection_budget_test.go covers the property that makes the shared ceiling
// worth having: capacity is accounted for as long as the thing it paid for can
// still exist.
//
// The dangerous cases are all the same shape — an attempt outlives the slot
// that started it. Removing a slot does NOT cancel a dial already in flight
// (there is no per-slot cancel, and adding one would not help: a cancel is a
// request, not a guarantee that the socket is closed). So a design that
// released on slot removal would free capacity while the socket it paid for is
// being opened, and the ceiling would be exceeded by exactly the traffic it
// exists to bound.

func mustBudget(t *testing.T, cfg connbudget.Config) *connbudget.Budget {
	t.Helper()
	b, err := connbudget.New(cfg)
	if err != nil {
		t.Fatalf("connbudget.New(%+v): %v", cfg, err)
	}
	return b
}

// blockingDialFn returns a DialFn that parks until release is closed, so a
// test can remove or replace the slot while the dial is provably in flight.
func blockingDialFn(started chan<- struct{}, release <-chan struct{}) func(context.Context, []domain.PeerAddress) (DialResult, error) {
	var once sync.Once
	return func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		once.Do(func() { close(started) })
		<-release
		session := fakePeerSession(addrs[0], domaintest.ID("id-"+string(addrs[0])))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}
}

// TestBudgetRefusalStopsFillWithoutCreatingSlots pins the admission side: when
// the ceiling has no room, no slot is created at all. A slot without a
// reservation would be a connection the budget does not know about.
func TestBudgetRefusalStopsFillWithoutCreatingSlots(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 2})
	// Take the whole ceiling before the manager gets a chance.
	for i := 0; i < 2; i++ {
		if _, err := budget.Reserve(connbudget.DirectionInbound); err != nil {
			t.Fatalf("pre-reserve %d: %v", i, err)
		}
	}

	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646")
	b.Cfg.Budget = budget
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	time.Sleep(150 * time.Millisecond)

	if got := cm.SlotCount(); got != 0 {
		t.Fatalf("SlotCount = %d with an exhausted budget, want 0", got)
	}
	if snap := budget.Snapshot(); snap.Outbound != 0 {
		t.Fatalf("outbound usage = %d, want 0 — a refused dial must not be accounted", snap.Outbound)
	}
	if snap := budget.Snapshot(); snap.RefusedTotal == 0 {
		t.Fatal("refusals were not counted; the diagnostic cannot say which ceiling bound")
	}
}

// TestSlotRemovedDuringDialKeepsCapacityAccounted is the core case. The slot
// disappears while the dial runs; the capacity must stay taken, because the
// socket is still going to be opened.
func TestSlotRemovedDuringDialKeepsCapacityAccounted(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})

	started := make(chan struct{})
	release := make(chan struct{})

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = blockingDialFn(started, release)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	if snap := budget.Snapshot(); snap.Outbound != 1 {
		t.Fatalf("outbound usage during dial = %d, want 1", snap.Outbound)
	}

	// Remove the slot out from under the running dial.
	cm.mu.Lock()
	if len(cm.slots) != 1 {
		cm.mu.Unlock()
		t.Fatal("expected exactly one slot before removal")
	}
	cm.removeSlotLocked(cm.slots[0])
	orphans := len(cm.orphanReservations)
	cm.mu.Unlock()

	if orphans != 1 {
		t.Fatalf("orphaned reservations = %d, want 1 — removal must not release a live attempt", orphans)
	}
	if snap := budget.Snapshot(); snap.Outbound != 1 {
		t.Fatalf("outbound usage after slot removal = %d, want 1 — the socket is still being opened", snap.Outbound)
	}

	// Let the dial finish. Its success is stale: the session is closed and
	// the capacity is released, exactly once, by the handler that owns the
	// close.
	close(release)

	deadline := time.After(3 * time.Second)
	for {
		if snap := budget.Snapshot(); snap.Outbound == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("capacity never released after the stale success: %+v", budget.Snapshot())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cm.mu.Lock()
	remaining := len(cm.orphanReservations)
	cm.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("orphan table still holds %d entries", remaining)
	}
}

// TestLateSuccessAfterSlotReplacementReleasesOnce covers the double-release
// risk: the attempt reports back under a generation that no longer matches,
// and both the stale-close path and any later cleanup could try to free the
// same unit. Freeing it twice would invent capacity.
func TestLateSuccessAfterSlotReplacementReleasesOnce(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})

	started := make(chan struct{})
	release := make(chan struct{})

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = blockingDialFn(started, release)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	// Replace the slot: the generation moves, so the in-flight success will
	// arrive stale.
	cm.mu.Lock()
	target := cm.slots[0]
	cm.replaceSlotLocked(target)
	cm.mu.Unlock()

	close(release)

	deadline := time.After(3 * time.Second)
	for {
		snap := budget.Snapshot()
		if snap.Outbound == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("stale success did not release: %+v", snap)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// The ceiling must still be four, not five: a double release would show
	// up here as an extra reservation being granted.
	var granted int
	for i := 0; i < 8; i++ {
		if _, err := budget.Reserve(connbudget.DirectionOutbound); err == nil {
			granted++
		}
	}
	if granted != 4 {
		t.Fatalf("granted %d reservations after the stale path, want 4 — capacity was invented", granted)
	}
}

// TestDialFailureForRemovedSlotReleasesCapacity is the other end of the same
// attempt: no socket was produced, and the parked unit must not be leaked.
func TestDialFailureForRemovedSlotReleasesCapacity(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})

	started := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = func(_ context.Context, _ []domain.PeerAddress) (DialResult, error) {
		once.Do(func() { close(started) })
		<-release
		return DialResult{}, errors.New("connection refused")
	}
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	cm.mu.Lock()
	cm.removeSlotLocked(cm.slots[0])
	cm.mu.Unlock()

	close(release)

	deadline := time.After(3 * time.Second)
	for {
		if budget.Snapshot().Outbound == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("failed attempt leaked its reservation: %+v", budget.Snapshot())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestShutdownDuringDialReleasesEverything covers cancellation racing a
// success: the manager stops while a dial is in flight, and whichever way that
// attempt ends, nothing may stay accounted against a budget nobody will
// consult again.
func TestShutdownDuringDialReleasesEverything(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})

	started := make(chan struct{})
	release := make(chan struct{})

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = blockingDialFn(started, release)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	// Cancel and let the dial complete at the same time.
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(release)
	}()
	cancel()

	deadline := time.After(5 * time.Second)
	for {
		if snap := budget.Snapshot(); snap.Used == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("shutdown left capacity accounted: %+v", budget.Snapshot())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestOutboundReserveIsNotTakenByInbound checks the wiring rather than the
// arithmetic (that is covered in the connbudget package): a Service-side
// inbound reservation and a manager-side outbound reservation draw from the
// same object, which is the entire point of injecting it.
func TestOutboundReserveIsNotTakenByInbound(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 3, OutboundReserve: 2})

	// Inbound may take one; the rest is the outbound reserve.
	if _, err := budget.Reserve(connbudget.DirectionInbound); err != nil {
		t.Fatalf("first inbound: %v", err)
	}
	if _, err := budget.Reserve(connbudget.DirectionInbound); !errors.Is(err, connbudget.ErrOutboundReserved) {
		t.Fatalf("second inbound error = %v, want ErrOutboundReserved", err)
	}

	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 8 }
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})

	deadline := time.After(3 * time.Second)
	for cm.SlotCount() != 2 {
		select {
		case <-deadline:
			t.Fatalf("slots = %d, want 2 — the reserve must be available to outbound", cm.SlotCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	time.Sleep(150 * time.Millisecond)
	if got := cm.SlotCount(); got != 2 {
		t.Fatalf("slots = %d, want exactly 2 — the ceiling was exceeded", got)
	}
}

// TestTerminalDialFailureReleasesImmediately is the P1 review found: a dial
// that ends for good — incompatible peer, or retries exhausted — removes its
// slot while the slot still reads "dialing", so the naive path parked the unit
// for an attempt that had just reported back. Nothing would ever release it,
// and a handful of such failures exhausted the outbound capacity even with the
// shared ceiling off, because the per-direction limit applies regardless.
func TestTerminalDialFailureReleasesImmediately(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 8, MaxOutbound: 2})

	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 2 }
	// Incompatible is terminal on the first failure: no retry, slot replaced.
	b.Cfg.DialFn = func(_ context.Context, _ []domain.PeerAddress) (DialResult, error) {
		return DialResult{}, errIncompatibleProtocol
	}
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	for i := 0; i < 4; i++ {
		cm.EmitHint(NewPeersDiscovered{Count: 1})
		time.Sleep(60 * time.Millisecond)
	}

	deadline := time.After(3 * time.Second)
	for {
		snap := budget.Snapshot()
		cm.mu.Lock()
		slots := len(cm.slots)
		orphans := len(cm.orphanReservations)
		cm.mu.Unlock()

		if snap.Outbound == slots && orphans == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("terminal failures leaked capacity: outbound=%d slots=%d orphans=%d",
				snap.Outbound, slots, orphans)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// The decisive check: capacity must still be dialable. A leak shows up
	// here as a refusal, because MaxOutbound binds even with B disabled.
	cm.mu.Lock()
	slots := len(cm.slots)
	cm.mu.Unlock()
	for i := slots; i < 2; i++ {
		r, err := budget.Reserve(connbudget.DirectionOutbound)
		if err != nil {
			t.Fatalf("outbound capacity exhausted by terminal failures: %v", err)
		}
		r.Release()
	}
}

// TestStaleSuccessReleasesAfterTheSocketIsClosed pins the ORDER, not the final
// count. Releasing before the close would let another attempt occupy the
// capacity while the old socket is still open — the overshoot the ceiling
// exists to prevent, and invisible to a test that only compares totals at the
// end.
func TestStaleSuccessReleasesAfterTheSocketIsClosed(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})

	started := make(chan struct{})
	release := make(chan struct{})

	var (
		mu             sync.Mutex
		usedAtClose    int
		observedClose  bool
		staleSessionCh = make(chan *peerSession, 1)
	)

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = blockingDialFn(started, release)
	// OnStaleSession runs while the stale socket is still open and before it
	// is closed, so the budget must still count it here.
	b.Cfg.OnStaleSession = func(session *peerSession) {
		mu.Lock()
		usedAtClose = budget.Snapshot().Outbound
		observedClose = true
		mu.Unlock()
		staleSessionCh <- session
	}
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	cm.mu.Lock()
	cm.replaceSlotLocked(cm.slots[0])
	cm.mu.Unlock()

	close(release)

	select {
	case <-staleSessionCh:
	case <-time.After(3 * time.Second):
		t.Fatal("stale session was never handed to the close path")
	}

	mu.Lock()
	saw, used := observedClose, usedAtClose
	mu.Unlock()
	if !saw {
		t.Fatal("close path did not run")
	}
	if used != 1 {
		t.Fatalf("outbound usage while the stale socket was still open = %d, want 1 — "+
			"capacity was freed before the socket it paid for was closed", used)
	}

	deadline := time.After(3 * time.Second)
	for budget.Snapshot().Outbound != 0 {
		select {
		case <-deadline:
			t.Fatalf("stale success never released: %+v", budget.Snapshot())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestManualPeerRefusedByCeilingEvictsNobody is the fourth review finding: the
// previous order evicted first and reserved second, so a refusal by the shared
// ceiling cost a live session and bought nothing. Eviction is the answer to a
// full slot table, never to an exhausted ceiling — freeing an outbound slot
// does not create capacity somebody else's inbound connections are holding.
func TestManualPeerRefusedByCeilingEvictsNobody(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 3, OutboundReserve: 1})

	// Fill the ceiling with inbound usage: outbound has room by its own
	// limit, but the shared ceiling has none.
	for i := 0; i < 2; i++ {
		if _, err := budget.Reserve(connbudget.DirectionInbound); err != nil {
			t.Fatalf("inbound pre-reserve %d: %v", i, err)
		}
	}

	var teardowns int32
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	// The slot table must be FULL, otherwise the eviction branch is never
	// reached and the test would pass for the wrong reason.
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.OnSessionTeardown = func(SessionInfo) { atomic.AddInt32(&teardowns, 1) }
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	// One outbound slot exists, is dialable within the reserve, and becomes
	// an ACTIVE session — the thing the old order threw away.
	cm.EmitHint(NewPeersDiscovered{Count: 1})
	deadline := time.After(3 * time.Second)
	for cm.ActiveCount() != 1 {
		select {
		case <-deadline:
			t.Fatalf("active slots = %d, want 1 (total %d)", cm.ActiveCount(), cm.SlotCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Now the ceiling is full. A manual peer must be refused WITHOUT taking
	// the existing slot down.
	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})
	time.Sleep(200 * time.Millisecond)

	if got := cm.SlotCount(); got != 1 {
		t.Fatalf("slots = %d after a refused manual peer, want 1 — a session was evicted for nothing", got)
	}
	if got := atomic.LoadInt32(&teardowns); got != 0 {
		t.Fatalf("teardowns = %d, want 0 — the ceiling refusal must not cost a live session", got)
	}
}

// TestManualPeerDoesNotEvictAnAttemptThatFreesNothing is the production-shaped
// case review asked for, with the scenario narrowed to the one that is
// actually a defect.
//
// Review's concern was that ErrDirectionLimit — the most specific reason the
// budget returns — can mask an equally exhausted shared ceiling. Checking it
// showed the refusal reason alone is not the discriminator: evicting an ACTIVE
// victim frees its unit, so the manual peer does get in and nothing was lost.
// The case where the mask bites is a victim whose DIAL IS STILL IN FLIGHT: its
// reservation is parked rather than released (removal does not cancel a dial),
// so the count does not go down, the retry is refused again, and the attempt
// was destroyed for nothing.
func TestManualPeerDoesNotEvictAnAttemptThatFreesNothing(t *testing.T) {
	// Production shape: the direction limit equals the slot limit.
	budget := mustBudget(t, connbudget.Config{Total: 3, OutboundReserve: 1, MaxOutbound: 1})
	if _, err := budget.Reserve(connbudget.DirectionInbound); err != nil {
		t.Fatalf("inbound pre-reserve: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = blockingDialFn(started, release)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}

	// One slot, dial in flight, direction limit reached.
	if snap := budget.Snapshot(); snap.Outbound != snap.MaxOutbound {
		t.Fatalf("precondition not met: %+v", snap)
	}

	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})
	time.Sleep(200 * time.Millisecond)

	cm.mu.Lock()
	slots := len(cm.slots)
	var dialing bool
	if slots == 1 {
		dialing = cm.slots[0].Address == mustAddr("10.0.0.1:64646")
	}
	cm.mu.Unlock()

	if slots != 1 || !dialing {
		t.Fatalf("slots = %d (original still present: %v) — an in-flight attempt was evicted "+
			"even though its unit stays parked and frees nothing", slots, dialing)
	}
}

// TestManualPeerHonoursSlotLimitWithoutABudget pins the manager's own
// invariant. With no budget wired — a supported mode — the reservation always
// succeeds, so a slot-limit check that only runs on a budget error would let
// the slot table grow past its maximum.
func TestManualPeerHonoursSlotLimitWithoutABudget(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = nil
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	deadline := time.After(3 * time.Second)
	for cm.ActiveCount() != 1 {
		select {
		case <-deadline:
			t.Fatalf("active slots = %d, want 1", cm.ActiveCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})
	time.Sleep(200 * time.Millisecond)

	if got := cm.SlotCount(); got != 1 {
		t.Fatalf("slots = %d, want 1 — MaxSlotsFn must bind with no budget wired", got)
	}
}

// TestManualPeerHonoursSlotLimitWhenTheCeilingIsWider is the same invariant
// with a budget that says yes: B larger than MaxSlotsFn must not turn the slot
// limit off. The operator's peer still gets in — by eviction, which is the
// pre-existing behaviour — but the table does not grow.
func TestManualPeerHonoursSlotLimitWhenTheCeilingIsWider(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 32})

	var teardowns int32
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.OnSessionTeardown = func(SessionInfo) { atomic.AddInt32(&teardowns, 1) }
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	cm.EmitHint(NewPeersDiscovered{Count: 1})
	deadline := time.After(3 * time.Second)
	for cm.ActiveCount() != 1 {
		select {
		case <-deadline:
			t.Fatalf("active slots = %d, want 1", cm.ActiveCount())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})

	deadline = time.After(3 * time.Second)
	for atomic.LoadInt32(&teardowns) == 0 {
		select {
		case <-deadline:
			t.Fatal("manual peer never made room; the operator's request was dropped")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if got := cm.SlotCount(); got != 1 {
		t.Fatalf("slots = %d, want 1 — a wider ceiling must not disable MaxSlotsFn", got)
	}
	// The evicted slot's unit must be back: exactly one outbound is held.
	deadline = time.After(3 * time.Second)
	for budget.Snapshot().Outbound != 1 {
		select {
		case <-deadline:
			t.Fatalf("outbound usage = %d after eviction + admit, want 1", budget.Snapshot().Outbound)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
