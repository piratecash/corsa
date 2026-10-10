package node

import (
	"context"
	"errors"
	"slices"
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

// budgetSettleTimeout bounds every wait for an attempt to report back. The
// attempts are in-process fakes, so anything near it means the event never
// came, not that it was slow.
const budgetSettleTimeout = 3 * time.Second

// waitForBudget waits until the budget reaches the wanted state. On timeout it
// also logs the snapshot it gave up on: which counter is still held is the
// whole diagnosis, and waitFor alone only reports the description.
func waitForBudget(t *testing.T, budget *connbudget.Budget, desc string, reached func(connbudget.Stats) bool) {
	t.Helper()
	settled := false
	// waitFor ends the test with t.Fatalf, which unwinds through deferred
	// calls — the only place left to report what the budget looked like.
	defer func() {
		if !settled {
			t.Logf("budget when the wait gave up: %+v", budget.Snapshot())
		}
	}()
	waitFor(t, budgetSettleTimeout, desc, func() bool { return reached(budget.Snapshot()) })
	settled = true
}

// parkedDialOutcome is how the parked dial ends once the test releases it.
type parkedDialOutcome func(addrs []domain.PeerAddress) (DialResult, error)

func parkedDialSucceeds(addrs []domain.PeerAddress) (DialResult, error) {
	session := fakePeerSession(addrs[0], domaintest.ID("id-"+string(addrs[0])))
	return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
}

func parkedDialIsRefused(_ []domain.PeerAddress) (DialResult, error) {
	return DialResult{}, errors.New("connection refused")
}

// parkedDial is a manager with one slot whose dial is provably in flight:
// DialFn has been entered and stays there until release.
//
// Every test built on it removes, replaces or abandons that slot mid-dial and
// then asserts on what THAT attempt leaves behind. The assertions are only
// about that attempt if it is the only one: a second fill would find the slot
// table free again, dial the same candidate under a new generation, and leave
// a live slot legitimately holding a unit — indistinguishable, in the budget
// counts, from the leak the test hunts for. So the fixture runs exactly one
// fill (the bootstrap one), keeps the periodic refill out of the test's
// lifetime, and checks at cleanup that exactly one dial happened.
type parkedDial struct {
	cm      *ConnectionManager
	cancel  context.CancelFunc
	started chan struct{}
	release chan struct{}

	releaseOnce sync.Once
	attempts    atomic.Int32
}

// startParkedDial starts the manager over a single candidate with room for a
// single slot and returns once that slot's dial is parked in DialFn. configure
// adjusts the manager config before it is built.
func startParkedDial(t *testing.T, budget *connbudget.Budget, outcome parkedDialOutcome, configure ...func(*ConnectionManagerConfig)) *parkedDial {
	t.Helper()
	p := &parkedDial{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	// Closing bootstrapCh is the single fill. The ticker is the only other
	// source that refills without being asked; an hour puts it outside any
	// test run.
	b.Cfg.FillInterval = time.Hour
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		if p.attempts.Add(1) == 1 {
			close(p.started)
		}
		<-p.release
		return outcome(addrs)
	}
	for _, apply := range configure {
		apply(&b.Cfg)
	}
	p.cm = b.Build()

	// runCM is not used: the cleanup needs Run's return as a barrier, because
	// shutdown joins every dial worker, so only after it can the dial count
	// no longer grow.
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	runReturned := make(chan struct{})
	go func() {
		p.cm.Run(ctx)
		close(runReturned)
	}()
	<-p.cm.Ready()
	t.Cleanup(func() { p.stopAndCheckSingleDial(t, runReturned) })

	p.cm.NotifyBootstrapReady()
	select {
	case <-p.started:
	case <-time.After(budgetSettleTimeout):
		t.Fatal("dial never started")
	}
	return p
}

// releaseDial lets every dial parked in DialFn finish with the fixture's
// outcome.
func (p *parkedDial) releaseDial() {
	p.releaseOnce.Do(func() { close(p.release) })
}

// stopAndCheckSingleDial stops the manager, waits until no dial worker can
// still run, and verifies the precondition every assertion of the test
// relied on.
func (p *parkedDial) stopAndCheckSingleDial(t *testing.T, runReturned <-chan struct{}) {
	t.Helper()
	p.releaseDial()
	p.cancel()
	select {
	case <-runReturned:
	case <-time.After(budgetSettleTimeout):
		t.Errorf("manager did not stop; dial count cannot be checked")
		return
	}
	// startParkedDial returning already proved the first dial; only a second
	// one breaks the precondition. A fixture that never got that far has
	// already failed with its own reason.
	if got := p.attempts.Load(); got > 1 {
		t.Errorf("DialFn ran %d times, want exactly 1 — a second dial ran, but the test assumes the "+
			"parked attempt is the only one, so the assertions above may describe a different attempt", got)
	}
}

// slotAddresses lists every slot, whatever its state. Slots reads the table
// under cm.mu, so called after a signal raised inside one of the manager's
// locked sections it observes the table exactly as that section left it.
func slotAddresses(cm *ConnectionManager) []domain.PeerAddress {
	slots := cm.Slots()
	addrs := make([]domain.PeerAddress, 0, len(slots))
	for _, s := range slots {
		addrs = append(addrs, s.Address)
	}
	return addrs
}

// countTeardowns wires OnSessionTeardown to a counter. In the manual-peer
// tests a teardown is the observable trace of an eviction, and it is called
// only after the locked section that changed the slot table.
func countTeardowns(cfg *ConnectionManagerConfig) *atomic.Int32 {
	var teardowns atomic.Int32
	cfg.OnSessionTeardown = func(SessionInfo) { teardowns.Add(1) }
	return &teardowns
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

	// The refusal is what ends the bootstrap fill. fill reserves under
	// cm.mu.Lock and SlotCount reads under cm.mu.RLock, so once a refusal is
	// counted, SlotCount observes the slot table that fill left behind.
	waitForBudget(t, budget, "a counted refusal of the bootstrap fill (uncounted, the diagnostic "+
		"cannot say which ceiling bound)", func(snap connbudget.Stats) bool {
		return snap.RefusedTotal > 0
	})

	if got := cm.SlotCount(); got != 0 {
		t.Fatalf("SlotCount = %d with an exhausted budget, want 0", got)
	}
	if snap := budget.Snapshot(); snap.Outbound != 0 {
		t.Fatalf("outbound usage = %d, want 0 — a refused dial must not be accounted", snap.Outbound)
	}
}

// TestSlotRemovedDuringDialKeepsCapacityAccounted is the core case. The slot
// disappears while the dial runs; the capacity must stay taken, because the
// socket is still going to be opened.
func TestSlotRemovedDuringDialKeepsCapacityAccounted(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})
	dial := startParkedDial(t, budget, parkedDialSucceeds)
	cm := dial.cm

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
	dial.releaseDial()

	waitForBudget(t, budget, "capacity released after the stale success", func(snap connbudget.Stats) bool {
		return snap.Outbound == 0
	})

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
	dial := startParkedDial(t, budget, parkedDialSucceeds)
	cm := dial.cm

	// Replace the slot: the generation moves, so the in-flight success will
	// arrive stale.
	cm.mu.Lock()
	cm.replaceSlotLocked(cm.slots[0])
	cm.mu.Unlock()

	dial.releaseDial()

	waitForBudget(t, budget, "stale success released its unit", func(snap connbudget.Stats) bool {
		return snap.Outbound == 0
	})

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
	dial := startParkedDial(t, budget, parkedDialIsRefused)
	cm := dial.cm

	cm.mu.Lock()
	cm.removeSlotLocked(cm.slots[0])
	cm.mu.Unlock()

	dial.releaseDial()

	waitForBudget(t, budget, "failed attempt released its parked unit", func(snap connbudget.Stats) bool {
		return snap.Outbound == 0
	})
}

// TestShutdownDuringDialReleasesEverything covers cancellation racing a
// success: the manager stops while a dial is in flight, and whichever way that
// attempt ends, nothing may stay accounted against a budget nobody will
// consult again.
func TestShutdownDuringDialReleasesEverything(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 4})
	dial := startParkedDial(t, budget, parkedDialSucceeds)

	// Cancel and let the dial complete at the same time.
	go func() {
		time.Sleep(10 * time.Millisecond)
		dial.releaseDial()
	}()
	dial.cancel()

	waitForBudget(t, budget, "shutdown released all capacity", func(snap connbudget.Stats) bool {
		return snap.Used == 0
	})
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

	// The bootstrap fill offers all three candidates; the third reservation
	// hits the shared ceiling and ends the fill. It is refused in the same
	// cm.mu section that created the first two slots, so once it is counted
	// the slot table is final. (The inbound refusal above is counted as
	// RefusedReserved, not RefusedTotal.)
	waitForBudget(t, budget, "the third outbound reservation refused by the shared ceiling",
		func(snap connbudget.Stats) bool {
			return snap.RefusedTotal > 0
		})

	if got := cm.SlotCount(); got != 2 {
		t.Fatalf("slots = %d, want exactly 2 — the reserve must be available to outbound, "+
			"and the ceiling must not be exceeded", got)
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

	waitForBudget(t, budget, "outbound usage matches the live slots with nothing parked — "+
		"otherwise terminal failures leaked capacity", func(snap connbudget.Stats) bool {
		cm.mu.Lock()
		defer cm.mu.Unlock()
		return snap.Outbound == len(cm.slots) && len(cm.orphanReservations) == 0
	})

	// The decisive check: capacity must still be dialable. A leak shows up
	// here as a refusal, because MaxOutbound binds even with B disabled.
	for i := cm.SlotCount(); i < 2; i++ {
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

	// OnStaleSession runs while the stale socket is still open and before it
	// is closed, so the budget must still count it here. The channel hands the
	// first observation to the test goroutine; the send never blocks, because
	// the callback runs on the event loop and a blocked loop would surface
	// only as a manager that does not stop.
	outboundAtClose := make(chan int, 1)
	var staleSessions atomic.Int32
	// Registered before the fixture, so it runs after the fixture's cleanup
	// has waited for Run to return: every OnStaleSession call has happened.
	// The first call is proved by the test body; only a second one is news.
	t.Cleanup(func() {
		if got := staleSessions.Load(); got > 1 {
			t.Errorf("OnStaleSession ran %d times, want exactly 1 — one parked attempt has one stale session", got)
		}
	})
	dial := startParkedDial(t, budget, parkedDialSucceeds, func(cfg *ConnectionManagerConfig) {
		cfg.OnStaleSession = func(*peerSession) {
			staleSessions.Add(1)
			select {
			case outboundAtClose <- budget.Snapshot().Outbound:
			default:
			}
		}
	})
	cm := dial.cm

	cm.mu.Lock()
	cm.replaceSlotLocked(cm.slots[0])
	cm.mu.Unlock()

	dial.releaseDial()

	var used int
	select {
	case used = <-outboundAtClose:
	case <-time.After(budgetSettleTimeout):
		t.Fatal("stale session was never handed to the close path")
	}
	if used != 1 {
		t.Fatalf("outbound usage while the stale socket was still open = %d, want 1 — "+
			"capacity was freed before the socket it paid for was closed", used)
	}

	waitForBudget(t, budget, "stale success released its unit", func(snap connbudget.Stats) bool {
		return snap.Outbound == 0
	})
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

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	// The slot table must be FULL, otherwise the eviction branch is never
	// reached and the test would pass for the wrong reason.
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	teardowns := countTeardowns(&b.Cfg)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	// One outbound slot exists, is dialable within the reserve, and becomes
	// an ACTIVE session — the thing the old order threw away.
	waitFor(t, budgetSettleTimeout, "1 active slot", func() bool {
		return cm.ActiveCount() == 1
	})

	// Now the ceiling is full. A manual peer must be refused WITHOUT taking
	// the existing slot down.
	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})

	// handleManualPeer reserves and decides on eviction in one cm.mu section,
	// so once the refusal is counted the slot table reflects the decision.
	// The old evict-first order freed the victim's unit before reserving and
	// was never refused at all — this wait is red for it.
	waitForBudget(t, budget, "the manual peer refused by the shared ceiling", func(snap connbudget.Stats) bool {
		return snap.RefusedTotal > 0
	})

	// Comparing the addresses, not just the count: evicting the victim and
	// admitting the manual peer in its place also leaves one slot.
	if got, want := slotAddresses(cm), []domain.PeerAddress{mustAddr("10.0.0.1:64646")}; !slices.Equal(got, want) {
		t.Fatalf("slots = %v after a refused manual peer, want %v — a session was evicted for nothing", got, want)
	}
	// A teardown is only ever the consequence of an eviction, which the slot
	// check above already sees in the locked section; this is the backstop
	// for the callback side.
	if got := teardowns.Load(); got != 0 {
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

	dial := startParkedDial(t, budget, parkedDialSucceeds)
	cm := dial.cm

	// One slot, dial in flight, direction limit reached.
	if snap := budget.Snapshot(); snap.Outbound != snap.MaxOutbound {
		t.Fatalf("precondition not met: %+v", snap)
	}

	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})

	// The direction limit refuses the manual peer inside the same cm.mu
	// section that decides whether to evict the in-flight victim.
	waitForBudget(t, budget, "the manual peer refused by the outbound direction limit",
		func(snap connbudget.Stats) bool {
			return snap.RefusedDirection > 0
		})

	if got, want := slotAddresses(cm), []domain.PeerAddress{mustAddr("10.0.0.1:64646")}; !slices.Equal(got, want) {
		t.Fatalf("slots = %v, want %v — an in-flight attempt was evicted even though its unit "+
			"stays parked and frees nothing", got, want)
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
	teardowns := countTeardowns(&b.Cfg)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	waitFor(t, budgetSettleTimeout, "1 active slot", func() bool {
		return cm.ActiveCount() == 1
	})

	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})

	// With no budget the reservation always succeeds, so the slot limit is
	// honoured only by evicting the active slot. Its teardown is called after
	// the locked section that removed it and appended the manual slot, so the
	// table is final once the teardown is seen. A table allowed to grow
	// evicts nobody — this wait is red for it.
	waitFor(t, budgetSettleTimeout, "the active slot torn down to make room for the manual peer", func() bool {
		return teardowns.Load() > 0
	})

	if got, want := slotAddresses(cm), []domain.PeerAddress{mustAddr("10.0.0.9:64646")}; !slices.Equal(got, want) {
		t.Fatalf("slots = %v, want %v — MaxSlotsFn must bind with no budget wired, "+
			"and the operator's peer takes the evicted place", got, want)
	}
}

// TestManualPeerHonoursSlotLimitWhenTheCeilingIsWider is the same invariant
// with a budget that says yes: B larger than MaxSlotsFn must not turn the slot
// limit off. The operator's peer still gets in — by eviction, which is the
// pre-existing behaviour — but the table does not grow.
func TestManualPeerHonoursSlotLimitWhenTheCeilingIsWider(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 32})

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.Budget = budget
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	teardowns := countTeardowns(&b.Cfg)
	cm := b.Build()
	cancel := runCM(cm)
	cm.NotifyBootstrapReady()
	defer cancel()

	waitFor(t, budgetSettleTimeout, "1 active slot", func() bool {
		return cm.ActiveCount() == 1
	})

	cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.9:64646")})

	// The budget says yes, so room comes only from evicting the active slot.
	// Its teardown is called after the locked section that removed it and
	// appended the manual slot, so the table is final once it is seen.
	waitFor(t, budgetSettleTimeout, "a teardown making room for the manual peer — "+
		"otherwise the operator's request was dropped", func() bool {
		return teardowns.Load() > 0
	})

	if got, want := slotAddresses(cm), []domain.PeerAddress{mustAddr("10.0.0.9:64646")}; !slices.Equal(got, want) {
		t.Fatalf("slots = %v, want %v — a wider ceiling must not disable MaxSlotsFn, "+
			"and the operator's peer takes the evicted place", got, want)
	}
	// The evicted slot's unit must be back: exactly one outbound is held.
	waitForBudget(t, budget, "exactly 1 outbound unit after eviction + admit", func(snap connbudget.Stats) bool {
		return snap.Outbound == 1
	})
}
