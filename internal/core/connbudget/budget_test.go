package connbudget

import (
	"errors"
	"sync"
	"testing"
)

func mustNew(t *testing.T, cfg Config) *Budget {
	t.Helper()
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	return b
}

// TestInvalidConfigurationIsRefusedNotRepaired pins the rule that a
// contradictory ceiling is an error the operator resolves, not something this
// package quietly trims into consistency. A clamped limit is a limit nobody
// configured and nobody can find in the logs later.
func TestInvalidConfigurationIsRefusedNotRepaired(t *testing.T) {
	cases := map[string]Config{
		"reserve above total": {Total: 4, OutboundReserve: 5},
		"negative total":      {Total: -1},
		"negative reserve":    {OutboundReserve: -1},
		"negative outbound":   {MaxOutbound: -1},
		"negative inbound":    {MaxInbound: -1},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New(%+v) error = %v, want ErrInvalidConfig", cfg, err)
			}
		})
	}
}

// TestReserveEqualToTotalIsAValidNoInboundMode covers the deliberate corner:
// R_out == B means this node never accepts an inbound connection. It is
// allowed, and Snapshot states it as InboundCapacity 0 rather than leaving it
// to be discovered by an unexplained refusal.
func TestReserveEqualToTotalIsAValidNoInboundMode(t *testing.T) {
	b := mustNew(t, Config{Total: 3, OutboundReserve: 3})

	if got := b.Snapshot().NonSlotCapacity; got != 0 {
		t.Fatalf("NonSlotCapacity = %d, want 0", got)
	}
	if _, err := b.Reserve(DirectionAuxiliary); !errors.Is(err, ErrOutboundReserved) {
		t.Fatalf("auxiliary reserve error = %v, want ErrOutboundReserved", err)
	}
	if _, err := b.Reserve(DirectionInbound); !errors.Is(err, ErrOutboundReserved) {
		t.Fatalf("inbound reserve error = %v, want ErrOutboundReserved", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Reserve(DirectionOutbound); err != nil {
			t.Fatalf("outbound reserve %d: %v", i, err)
		}
	}
}

// TestDisabledBudgetKeepsDirectionLimits is the default-off contract: B == 0
// removes the SHARED ceiling only. The per-direction limits a node had before
// this package existed keep binding, so switching the budget off is not a way
// to switch admission off.
func TestDisabledBudgetKeepsDirectionLimits(t *testing.T) {
	b := mustNew(t, Config{Total: 0, MaxOutbound: 2, MaxInbound: 1})

	if b.Enabled() {
		t.Fatal("Total 0 must leave the shared ceiling disabled")
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Reserve(DirectionOutbound); err != nil {
			t.Fatalf("outbound %d: %v", i, err)
		}
	}
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
		t.Fatalf("third outbound error = %v, want ErrDirectionLimit", err)
	}
	if _, err := b.Reserve(DirectionInbound); err != nil {
		t.Fatalf("first inbound: %v", err)
	}
	if _, err := b.Reserve(DirectionInbound); !errors.Is(err, ErrDirectionLimit) {
		t.Fatalf("second inbound error = %v, want ErrDirectionLimit", err)
	}
}

// TestInboundCannotTakeTheOutboundReserve is the property the reserve exists
// for: a node saturated by inbound connections keeps the capacity to dial out.
// Note what it does NOT claim — the reserve guarantees CAPACITY, not a
// successful dial, and it creates no room when the outbound positions are
// already occupied.
func TestInboundCannotTakeTheOutboundReserve(t *testing.T) {
	b := mustNew(t, Config{Total: 5, OutboundReserve: 2})

	for i := 0; i < 3; i++ {
		if _, err := b.Reserve(DirectionInbound); err != nil {
			t.Fatalf("inbound %d: %v", i, err)
		}
	}
	if _, err := b.Reserve(DirectionInbound); !errors.Is(err, ErrOutboundReserved) {
		t.Fatalf("fourth inbound error = %v, want ErrOutboundReserved", err)
	}

	// The reserve is still there for its owner.
	for i := 0; i < 2; i++ {
		if _, err := b.Reserve(DirectionOutbound); err != nil {
			t.Fatalf("outbound %d must fit in the reserve: %v", i, err)
		}
	}
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
		t.Fatalf("outbound past the ceiling error = %v, want ErrTotalExhausted", err)
	}
}

// TestReleaseIsIdempotent pins the property that makes the handle safe to hand
// around: an attempt has several ends — failure, cancellation, a late success
// closing an orphaned socket — and more than one may run. A second release
// must not invent capacity.
func TestReleaseIsIdempotent(t *testing.T) {
	b := mustNew(t, Config{Total: 1})

	r, err := b.Reserve(DirectionOutbound)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	r.Release()
	r.Release()
	r.Release()

	if used := b.Snapshot().Used; used != 0 {
		t.Fatalf("used = %d after repeated release, want 0", used)
	}
	// One unit of capacity exists, not three.
	if _, err := b.Reserve(DirectionOutbound); err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
		t.Fatalf("second reserve error = %v, want ErrTotalExhausted — repeated release inflated capacity", err)
	}
}

// TestNilBudgetAndNilReservationAreNoOps keeps the call sites free of nil
// checks on paths where a budget may not be wired at all.
func TestNilBudgetAndNilReservationAreNoOps(t *testing.T) {
	var b *Budget
	r, err := b.Reserve(DirectionInbound)
	if err != nil {
		t.Fatalf("nil budget must reserve freely, got %v", err)
	}
	r.Release()
	r.Release()

	var missing *Reservation
	missing.Release()

	if snap := b.Snapshot(); snap.Enabled {
		t.Fatal("nil budget must report disabled")
	}
}

// TestConcurrentReserveNeverExceedsTheCeiling is the reason reservation and
// admission are one operation. With a read-then-take pair, N goroutines pass
// the same check and the ceiling is exceeded by construction.
func TestConcurrentReserveNeverExceedsTheCeiling(t *testing.T) {
	const total = 8
	b := mustNew(t, Config{Total: total})

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted []*Reservation
	)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := b.Reserve(DirectionOutbound)
			if err != nil {
				return
			}
			mu.Lock()
			granted = append(granted, r)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(granted) != total {
		t.Fatalf("granted %d reservations, want exactly %d", len(granted), total)
	}
	if used := b.Snapshot().Used; used != total {
		t.Fatalf("used = %d, want %d", used, total)
	}

	for _, r := range granted {
		r.Release()
	}
	if used := b.Snapshot().Used; used != 0 {
		t.Fatalf("used = %d after releasing everything, want 0", used)
	}
}

// TestConcurrentReleaseAndReserveKeepsTheInvariant runs the two operations
// against each other: at no point may the accounted usage exceed the ceiling,
// and at the end the books must balance.
func TestConcurrentReleaseAndReserveKeepsTheInvariant(t *testing.T) {
	const total = 4
	b := mustNew(t, Config{Total: total})

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, err := b.Reserve(DirectionInbound)
				if err != nil {
					continue
				}
				if used := b.Snapshot().Used; used > total {
					t.Errorf("used = %d exceeds ceiling %d", used, total)
				}
				r.Release()
			}
		}()
	}
	wg.Wait()

	if used := b.Snapshot().Used; used != 0 {
		t.Fatalf("used = %d after the storm, want 0", used)
	}
}

// TestRefusalsAreCountedByReason keeps the diagnostic honest: a node sitting
// against the shared ceiling and a node sitting against the outbound reserve
// need different answers, and "connections refused" alone gives neither.
func TestRefusalsAreCountedByReason(t *testing.T) {
	b := mustNew(t, Config{Total: 2, OutboundReserve: 1, MaxInbound: 4})

	if _, err := b.Reserve(DirectionInbound); err != nil {
		t.Fatalf("first inbound: %v", err)
	}
	// Second inbound hits the reserve, not the ceiling: one slot is free.
	if _, err := b.Reserve(DirectionInbound); !errors.Is(err, ErrOutboundReserved) {
		t.Fatalf("second inbound error = %v, want ErrOutboundReserved", err)
	}
	if _, err := b.Reserve(DirectionOutbound); err != nil {
		t.Fatalf("outbound into the reserve: %v", err)
	}
	// Now the ceiling itself is full.
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
		t.Fatalf("outbound past the ceiling error = %v, want ErrTotalExhausted", err)
	}

	snap := b.Snapshot()
	if snap.RefusedReserved != 1 {
		t.Fatalf("RefusedReserved = %d, want 1", snap.RefusedReserved)
	}
	if snap.RefusedTotal != 1 {
		t.Fatalf("RefusedTotal = %d, want 1", snap.RefusedTotal)
	}
	if snap.Used != 2 || snap.Inbound != 1 || snap.Outbound != 1 {
		t.Fatalf("snapshot = %+v, want used 2 = inbound 1 + outbound 1", snap)
	}
}

// TestUnknownDirectionIsRefused closes the direction set: a value outside it
// must not silently draw from either half.
func TestUnknownDirectionIsRefused(t *testing.T) {
	b := mustNew(t, Config{Total: 4})

	if _, err := b.Reserve(Direction(9)); !errors.Is(err, ErrUnknownDirection) {
		t.Fatalf("error = %v, want ErrUnknownDirection", err)
	}
	if used := b.Snapshot().Used; used != 0 {
		t.Fatalf("used = %d after a refused direction, want 0", used)
	}
}

// TestAuxiliaryIsNotBoundedByTheOutboundLimit is the P1 review found, at the
// level where it originates. The outbound limit sizes the node's PERSISTENT
// neighbourhood, and the connection manager keeps every one of those positions
// occupied by design — so an auxiliary dial charged to that limit is refused
// permanently, and waiting cannot help.
func TestAuxiliaryIsNotBoundedByTheOutboundLimit(t *testing.T) {
	b := mustNew(t, Config{MaxOutbound: 2, MaxAuxiliary: 0})

	for i := 0; i < 2; i++ {
		if _, err := b.Reserve(DirectionOutbound); err != nil {
			t.Fatalf("outbound %d: %v", i, err)
		}
	}
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
		t.Fatalf("third outbound error = %v, want ErrDirectionLimit", err)
	}

	// The peer slots are full; auxiliary dials must still be possible.
	for i := 0; i < 5; i++ {
		if _, err := b.Reserve(DirectionAuxiliary); err != nil {
			t.Fatalf("auxiliary %d refused with the outbound limit full: %v", i, err)
		}
	}
	if snap := b.Snapshot(); snap.Auxiliary != 5 || snap.Outbound != 2 || snap.Used != 7 {
		t.Fatalf("snapshot = %+v, want auxiliary 5 + outbound 2 = used 7", snap)
	}
}

// TestAuxiliaryIsBoundedWhenTheCeilingIsOn is the other half of the same
// decision: unbounded auxiliary dials would be able to consume the whole of B
// and starve the peer slots, so an enabled ceiling comes with a bound.
func TestAuxiliaryIsBoundedWhenTheCeilingIsOn(t *testing.T) {
	b := mustNew(t, Config{Total: 10, MaxAuxiliary: 2})

	for i := 0; i < 2; i++ {
		if _, err := b.Reserve(DirectionAuxiliary); err != nil {
			t.Fatalf("auxiliary %d: %v", i, err)
		}
	}
	if _, err := b.Reserve(DirectionAuxiliary); !errors.Is(err, ErrDirectionLimit) {
		t.Fatalf("third auxiliary error = %v, want ErrDirectionLimit", err)
	}
	// The peer slots still have the rest of the ceiling.
	if _, err := b.Reserve(DirectionOutbound); err != nil {
		t.Fatalf("outbound must still fit: %v", err)
	}
}

// TestAuxiliaryCountsAgainstTheSharedCeiling keeps the accounting requirement:
// auxiliary dials are exempt from the OUTBOUND limit, never from B.
func TestAuxiliaryCountsAgainstTheSharedCeiling(t *testing.T) {
	b := mustNew(t, Config{Total: 2, MaxAuxiliary: 8})

	for i := 0; i < 2; i++ {
		if _, err := b.Reserve(DirectionAuxiliary); err != nil {
			t.Fatalf("auxiliary %d: %v", i, err)
		}
	}
	if _, err := b.Reserve(DirectionAuxiliary); !errors.Is(err, ErrTotalExhausted) {
		t.Fatalf("third auxiliary error = %v, want ErrTotalExhausted", err)
	}
	if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
		t.Fatalf("outbound error = %v, want ErrTotalExhausted — auxiliary must occupy the ceiling", err)
	}
	if snap := b.Snapshot(); snap.Used != 2 {
		t.Fatalf("used = %d, want 2", snap.Used)
	}
}

// TestReserveProtectsSlotsFromInboundAndAuxiliaryTogether is the review
// finding in its own words: two limits that are each satisfied can still add
// up past the thing they were meant to protect.
//
// B = 12, R_out = 8: four inbound plus four auxiliary would leave the
// connection manager four positions instead of eight, and MaxAuxiliary alone
// does not prevent it. Both orders are checked, because a rule that depends on
// the order connections arrive in is not a rule.
func TestReserveProtectsSlotsFromInboundAndAuxiliaryTogether(t *testing.T) {
	orders := map[string][]Direction{
		"inbound first":   {DirectionInbound, DirectionInbound, DirectionAuxiliary, DirectionAuxiliary},
		"auxiliary first": {DirectionAuxiliary, DirectionAuxiliary, DirectionInbound, DirectionInbound},
		"interleaved":     {DirectionInbound, DirectionAuxiliary, DirectionInbound, DirectionAuxiliary},
	}

	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			b := mustNew(t, Config{Total: 12, OutboundReserve: 8, MaxAuxiliary: 4})

			// Four non-slot units fit: B − R_out.
			for i, direction := range order {
				if _, err := b.Reserve(direction); err != nil {
					t.Fatalf("%s %d: %v", direction, i, err)
				}
			}
			// The fifth must be refused whichever direction asks.
			if _, err := b.Reserve(DirectionAuxiliary); !errors.Is(err, ErrOutboundReserved) {
				t.Fatalf("fifth auxiliary error = %v, want ErrOutboundReserved", err)
			}
			if _, err := b.Reserve(DirectionInbound); !errors.Is(err, ErrOutboundReserved) {
				t.Fatalf("fifth inbound error = %v, want ErrOutboundReserved", err)
			}

			// The whole persistent neighbourhood is still available.
			for i := 0; i < 8; i++ {
				if _, err := b.Reserve(DirectionOutbound); err != nil {
					t.Fatalf("peer slot %d must fit in the reserve: %v — "+
						"inbound and auxiliary together ate the neighbourhood", i, err)
				}
			}
			if snap := b.Snapshot(); snap.Used != 12 {
				t.Fatalf("used = %d, want 12", snap.Used)
			}
		})
	}
}
