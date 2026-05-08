package routing

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestNewTableNotDirty verifies that a freshly constructed Table reports
// IsDirty=false and ConsumeDirty=false. The publisher (cold-start path)
// relies on this to distinguish "first publish ever, build unconditionally"
// from "table mutated, must rebuild".
func TestNewTableNotDirty(t *testing.T) {
	tbl := NewTable()

	if tbl.IsDirty() {
		t.Fatal("freshly constructed table reports dirty=true; expected false")
	}
	if tbl.ConsumeDirty() {
		t.Fatal("freshly constructed table ConsumeDirty=true; expected false")
	}
}

// TestConsumeDirtyClearsFlag verifies that ConsumeDirty performs an atomic
// CAS true→false: once it returns true, the next read must see clean.
// This is the core publisher contract — without it the publisher would
// rebuild on every tick regardless of writer activity.
func TestConsumeDirtyClearsFlag(t *testing.T) {
	tbl := NewTable()
	tbl.dirty.Store(true)

	if !tbl.ConsumeDirty() {
		t.Fatal("ConsumeDirty returned false on dirty=true")
	}
	if tbl.IsDirty() {
		t.Fatal("dirty flag not cleared after ConsumeDirty")
	}
	if tbl.ConsumeDirty() {
		t.Fatal("second ConsumeDirty returned true; expected false")
	}
}

// TestUpdateRouteAcceptedMarksDirty verifies that an accepted UpdateRoute
// (new entry, higher SeqNo, higher trust, fewer hops on tie, expired-same-seq
// refresh) marks the table dirty. Each branch is exercised so a future
// refactor that drops a dirty mark on one of them is caught.
func TestUpdateRouteAcceptedMarksDirty(t *testing.T) {
	cases := []struct {
		name string
		seed []RouteEntry
		add  RouteEntry
	}{
		{
			name: "new_entry",
			add: RouteEntry{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
			},
		},
		{
			name: "higher_seq",
			seed: []RouteEntry{{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
			}},
			add: RouteEntry{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 2, SeqNo: 2, Source: RouteSourceAnnouncement,
			},
		},
		{
			name: "higher_trust_same_seq",
			seed: []RouteEntry{{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
			}},
			add: RouteEntry{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 3, SeqNo: 1, Source: RouteSourceHopAck,
			},
		},
		{
			name: "fewer_hops_same_seq_same_trust",
			seed: []RouteEntry{{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
			}},
			add: RouteEntry{
				Identity: "alice", Origin: "bob", NextHop: "charlie",
				Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := NewTable()
			for _, seed := range tc.seed {
				mustUpdate(t, tbl, seed)
			}
			// Drain any dirty marks the seed step produced — we want to
			// observe ONLY the mark from the case under test.
			tbl.ConsumeDirty()

			status, err := tbl.UpdateRoute(tc.add)
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if status != RouteAccepted {
				t.Fatalf("expected RouteAccepted for case %q, got %v", tc.name, status)
			}
			if !tbl.ConsumeDirty() {
				t.Fatalf("UpdateRoute accepted path did not mark dirty for case %q", tc.name)
			}
		})
	}
}

// TestUpdateRouteRejectedDoesNotMarkDirty verifies that paths that do not
// mutate the table — stale SeqNo, tombstone protection, RouteSourceLocal
// rejection — leave dirty untouched. Without this guarantee the publisher
// would rebuild on every announce tick that delivered already-known
// routes, which is the common case once the network stabilises.
func TestUpdateRouteRejectedDoesNotMarkDirty(t *testing.T) {
	t.Run("stale_seq", func(t *testing.T) {
		tbl := NewTable()
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		tbl.ConsumeDirty()

		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 2, SeqNo: 3, Source: RouteSourceAnnouncement,
		})
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if status != RouteRejected {
			t.Fatalf("expected RouteRejected for stale SeqNo, got %v", status)
		}
		if tbl.IsDirty() {
			t.Fatal("rejected stale-seq path marked dirty")
		}
	})

	t.Run("tombstone_protection", func(t *testing.T) {
		tbl := NewTable()
		// Seed a withdrawal tombstone via WithdrawRoute.
		if !tbl.WithdrawRoute("alice", "bob", "charlie", 5) {
			t.Fatal("WithdrawRoute returned false on first withdraw")
		}
		tbl.ConsumeDirty()

		// Same-seq update on the tombstone must be rejected.
		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 2, SeqNo: 5, Source: RouteSourceHopAck,
		})
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if status != RouteRejected {
			t.Fatalf("expected RouteRejected for same-seq tombstone, got %v", status)
		}
		if tbl.IsDirty() {
			t.Fatal("rejected tombstone path marked dirty")
		}
	})

	t.Run("local_source_reserved", func(t *testing.T) {
		tbl := NewTable()
		_, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "alice", NextHop: "alice",
			Hops: 0, SeqNo: 1, Source: RouteSourceLocal,
		})
		if err == nil {
			t.Fatal("expected ErrLocalSourceReserved")
		}
		if tbl.IsDirty() {
			t.Fatal("RouteSourceLocal rejection path marked dirty")
		}
	})
}

// TestUpdateRouteUnchangedDirtyContract pins the dirty contract for the
// three RouteUnchanged sub-paths inside UpdateRoute. Only the exact-
// reconfirmation branch rewrites ExpiresAt — the others observe the entry
// without mutating it and must leave dirty untouched, otherwise every
// stable-network announce tick would force a snapshot rebuild and defeat
// the dirty-flag economy.
func TestUpdateRouteUnchangedDirtyContract(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	t.Run("exact_reconfirmation_rewrites_ttl_marks_dirty", func(t *testing.T) {
		tbl := NewTable(WithClock(fixedClock(now)))
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		tbl.ConsumeDirty()

		// Identical entry — same source, same hops, same SeqNo — is the
		// exact reconfirmation path. ExpiresAt is rewritten, snapshot
		// must be republished.
		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if status != RouteUnchanged {
			t.Fatalf("expected RouteUnchanged, got %v", status)
		}
		if !tbl.ConsumeDirty() {
			t.Fatal("exact reconfirmation did not mark dirty; ExpiresAt rewrite must trigger republish")
		}
	})

	t.Run("worse_hops_same_source_keeps_clean", func(t *testing.T) {
		tbl := NewTable(WithClock(fixedClock(now)))
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		tbl.ConsumeDirty()

		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 5, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if status != RouteUnchanged {
			t.Fatalf("expected RouteUnchanged, got %v", status)
		}
		if tbl.IsDirty() {
			t.Fatal("worse-hops tie marked dirty; nothing was rewritten")
		}
	})

	t.Run("weaker_source_keeps_clean", func(t *testing.T) {
		// Seed with hop_ack (trust=1), then deliver an announcement
		// (trust=0) at the same SeqNo: weaker trust path, no rewrite,
		// no dirty.
		tbl := NewTable(WithClock(fixedClock(now)))
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 3, SeqNo: 5, Source: RouteSourceHopAck,
		})
		tbl.ConsumeDirty()

		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if status != RouteUnchanged {
			t.Fatalf("expected RouteUnchanged, got %v", status)
		}
		if tbl.IsDirty() {
			t.Fatal("weaker-source tie marked dirty; nothing was rewritten")
		}
	})
}

// TestAddDirectPeerIdempotentDoesNotMarkDirty verifies that the idempotent
// fast-path of AddDirectPeer (peer already has an alive direct route) leaves
// dirty untouched. Connect-storm scenarios call AddDirectPeer for every
// session reattach; without this fast path the publisher would rebuild
// on every reconnect even when the route is unchanged.
func TestAddDirectPeerIdempotentDoesNotMarkDirty(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("alice"))

	// First add — real mutation, dirty.
	mustAddDirect(t, tbl, "bob")
	if !tbl.ConsumeDirty() {
		t.Fatal("first AddDirectPeer did not mark dirty")
	}

	// Second add for the same peer — idempotent no-op.
	mustAddDirect(t, tbl, "bob")
	if tbl.IsDirty() {
		t.Fatal("idempotent AddDirectPeer marked dirty; reconnect storms would force rebuilds")
	}
}

// TestAddDirectPeerReactivationMarksDirty verifies that a re-add after
// withdrawal (the actual disconnect-reconnect path) marks dirty.
func TestAddDirectPeerReactivationMarksDirty(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("alice"))
	mustAddDirect(t, tbl, "bob")
	mustRemoveDirect(t, tbl, "bob")
	tbl.ConsumeDirty()

	mustAddDirect(t, tbl, "bob")
	if !tbl.ConsumeDirty() {
		t.Fatal("reactivation after withdrawal did not mark dirty")
	}
}

// TestRemoveDirectPeerMarksDirty verifies that RemoveDirectPeer always
// marks dirty: at minimum recordWithdrawalLocked appends to withdrawTimes,
// which is part of the published FlapState.
func TestRemoveDirectPeerMarksDirty(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("alice"))
	mustAddDirect(t, tbl, "bob")
	tbl.ConsumeDirty()

	mustRemoveDirect(t, tbl, "bob")
	if !tbl.ConsumeDirty() {
		t.Fatal("RemoveDirectPeer did not mark dirty")
	}
}

// TestWithdrawRouteSuccessMarksDirty verifies the withdrawal mutation paths.
func TestWithdrawRouteSuccessMarksDirty(t *testing.T) {
	t.Run("tombstone_creation", func(t *testing.T) {
		tbl := NewTable()
		if !tbl.WithdrawRoute("alice", "bob", "charlie", 1) {
			t.Fatal("WithdrawRoute returned false on tombstone creation")
		}
		if !tbl.ConsumeDirty() {
			t.Fatal("tombstone creation did not mark dirty")
		}
	})

	t.Run("active_to_withdrawn", func(t *testing.T) {
		tbl := NewTable()
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		tbl.ConsumeDirty()

		if !tbl.WithdrawRoute("alice", "bob", "charlie", 2) {
			t.Fatal("WithdrawRoute returned false on active→withdrawn")
		}
		if !tbl.ConsumeDirty() {
			t.Fatal("active→withdrawn did not mark dirty")
		}
	})

	t.Run("stale_seq_keeps_clean", func(t *testing.T) {
		tbl := NewTable()
		if !tbl.WithdrawRoute("alice", "bob", "charlie", 5) {
			t.Fatal("WithdrawRoute returned false on tombstone creation")
		}
		tbl.ConsumeDirty()

		if tbl.WithdrawRoute("alice", "bob", "charlie", 3) {
			t.Fatal("WithdrawRoute should reject lower SeqNo")
		}
		if tbl.IsDirty() {
			t.Fatal("rejected lower-SeqNo withdraw marked dirty")
		}
	})
}

// TestInvalidateTransitRoutesMarksDirtyOnlyOnRealInvalidation verifies that
// the no-op path (no transit routes via the disconnected peer) leaves dirty
// untouched, while a real invalidation marks it.
func TestInvalidateTransitRoutesMarksDirtyOnlyOnRealInvalidation(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("self"))

	t.Run("no_routes_keeps_clean", func(t *testing.T) {
		invalidated, _ := tbl.InvalidateTransitRoutes("ghost")
		if invalidated != 0 {
			t.Fatalf("expected 0 invalidated for unknown peer, got %d", invalidated)
		}
		if tbl.IsDirty() {
			t.Fatal("no-op InvalidateTransitRoutes marked dirty")
		}
	})

	t.Run("real_invalidation_marks_dirty", func(t *testing.T) {
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "charlie",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		tbl.ConsumeDirty()

		invalidated, _ := tbl.InvalidateTransitRoutes("charlie")
		if invalidated != 1 {
			t.Fatalf("expected 1 invalidated, got %d", invalidated)
		}
		if !tbl.ConsumeDirty() {
			t.Fatal("real InvalidateTransitRoutes did not mark dirty")
		}
	})
}

// TestTickTTLClearsExpiredHoldDownAndMarksDirty pins the contract that
// TickTTL is responsible for publishing FlapEntry.InHoldDown=false once
// fs.holdDownUntil has elapsed. Hold-down expiry is a TIME-DERIVED
// flap-state transition: the deadline elapses on the wall clock without
// any writer event, so the dirty-flag publisher cannot observe it on
// its own. TickTTL is the schedule that converts the wall-clock
// transition into a writer event (clear holdDownUntil + dirty mark);
// the refresher then republishes within one refresh tick.
//
// End-to-end visibility for the InHoldDown=true → false transition is
// therefore TickTTL_interval (≈10 s in production) + one refresh
// (≈500 ms), NOT a single refresh tick. This test fixes only the
// "after TickTTL observes expiry" half of that chain — without the
// fix, even running TickTTL while withdrawTimes were still inside
// flapWindow left InHoldDown=true and a stale HoldDownUntil cached
// for up to ~90 s on default settings (30 s hold-down + 120 s flap
// window). See docs/routing.md "Snapshot freshness" for the full
// time-derived bound.
//
// Fix: TickTTL now zeros fs.holdDownUntil the first time it observes
// expired hold-down, marks dirty, and the next refresh publishes
// InHoldDown=false. Withdraw-time trimming still runs on its own
// schedule (the peer remains in FlapState with RecentWithdrawals > 0
// until the window slides forward), but the InHoldDown flag flips
// as soon as TickTTL runs.
func TestTickTTLClearsExpiredHoldDownAndMarksDirty(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}

	// Tight thresholds so the test triggers hold-down with two flaps
	// while keeping the flap window long enough that withdrawTimes
	// remain inside it after hold-down expires.
	tbl := NewTable(
		WithClock(clock.nowFunc),
		WithLocalOrigin("self"),
		WithFlapWindow(120*time.Second),
		WithFlapThreshold(2),
		WithHoldDownDuration(30*time.Second),
	)

	// Force a flap burst: two RemoveDirectPeer events within the
	// flap window arm hold-down. AddDirectPeer between the two
	// removes is what produces a real "second disconnect" rather
	// than a no-op.
	mustAddDirect(t, tbl, "peerA")
	mustRemoveDirect(t, tbl, "peerA")
	mustAddDirect(t, tbl, "peerA")
	mustRemoveDirect(t, tbl, "peerA")

	// Sanity: the peer is in hold-down and the cached snapshot
	// would report InHoldDown=true.
	snap := tbl.Snapshot()
	if len(snap.FlapState) == 0 {
		t.Fatal("expected FlapState to contain peerA after burst")
	}
	if !snap.FlapState[0].InHoldDown {
		t.Fatalf("expected InHoldDown=true immediately after burst, got %+v", snap.FlapState[0])
	}

	// Drain dirty so we observe ONLY the mark from the next TickTTL.
	tbl.ConsumeDirty()

	// Advance clock past holdDownUntil (30 s) but stay well inside
	// flapWindow (120 s) so withdrawTimes are NOT yet drained.
	clock.advance(40 * time.Second)

	// TickTTL must observe hold-down expiry, clear the timestamp,
	// and mark the table dirty.
	tbl.TickTTL()
	if !tbl.ConsumeDirty() {
		t.Fatal("TickTTL did not mark dirty after hold-down expired " +
			"while withdrawTimes were still inside flapWindow; " +
			"cached snapshot will keep stale InHoldDown=true for up to flapWindow")
	}

	// The published snapshot now reflects the transition.
	snap = tbl.Snapshot()
	if len(snap.FlapState) == 0 {
		t.Fatal("expected FlapState to still contain peerA (withdrawTimes within window)")
	}
	if snap.FlapState[0].InHoldDown {
		t.Fatalf("expected InHoldDown=false after hold-down expiry, got %+v", snap.FlapState[0])
	}
	if !snap.FlapState[0].HoldDownUntil.IsZero() {
		t.Fatalf("expected HoldDownUntil to be zero after expiry, got %s", snap.FlapState[0].HoldDownUntil)
	}
	if snap.FlapState[0].RecentWithdrawals == 0 {
		t.Fatal("withdrawTimes were cleared too eagerly; the peer should still be tracked")
	}
}

// TestUpdateRouteCapRejectionMarksDirty verifies the side channel
// that surfaces cap admission counters into the published snapshot:
// even when a cap rejection leaves the routes map untouched, the
// admission counters in Table.CapStats() advanced — so the snapshot
// MUST republish, otherwise fetchRouteSummary keeps showing stale
// rejected_full / rejected_all_protected values until some unrelated
// mutation lands. This is the only path in UpdateRoute that flips
// dirty without a routes-map change; everywhere else the
// "no-mutation paths leave dirty clean" rule holds.
func TestUpdateRouteCapRejectionMarksDirty(t *testing.T) {
	tbl := NewTable(WithMaxNextHopsPerOrigin(1))

	// Saturate the (Identity, Origin) bucket with one hop_ack — it
	// will protect against further hop_ack admissions of the same or
	// worse rank.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-a",
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})
	tbl.ConsumeDirty()

	// Worse-than-existing hop_ack — admission rejects with
	// AdmissionRejectedFull. Routes map is unchanged but the
	// rejected_full counter advanced.
	statusFull, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-b",
		Hops: 5, SeqNo: 1, Source: RouteSourceHopAck,
	})
	if err != nil {
		t.Fatalf("rejected admission produced an error: %v", err)
	}
	if statusFull != RouteRejected {
		t.Fatalf("worse hop_ack on a saturated bucket: expected RouteRejected, got %v", statusFull)
	}
	if !tbl.ConsumeDirty() {
		t.Fatal("AdmissionRejectedFull did not mark dirty; cap counters would stay stale in the cached snapshot")
	}
	if tbl.CapStats().RejectedFull != 1 {
		t.Fatalf("expected RejectedFull counter to advance to 1, got %+v", tbl.CapStats())
	}

	// Now make the bucket all-protected (replace the only entry with
	// a direct route) and try a hop_ack against it. This produces
	// AdmissionRejectedAllProtected — also a non-routes-map change
	// that must publish.
	tbl2 := NewTable(
		WithLocalOrigin("self"),
		WithMaxNextHopsPerOrigin(1),
	)
	mustAddDirect(t, tbl2, "neighbor")
	tbl2.ConsumeDirty()

	statusProtected, err := tbl2.UpdateRoute(RouteEntry{
		Identity: "neighbor", Origin: "self", NextHop: "transit",
		Hops: 1, SeqNo: 99, Source: RouteSourceHopAck,
	})
	if err != nil {
		t.Fatalf("all-protected admission produced an error: %v", err)
	}
	if statusProtected != RouteRejected {
		t.Fatalf("hop_ack against direct-only saturated bucket: expected RouteRejected, got %v", statusProtected)
	}
	if !tbl2.ConsumeDirty() {
		t.Fatal("AdmissionRejectedAllProtected did not mark dirty; cap counters would stay stale")
	}
	if tbl2.CapStats().RejectedAllProtected != 1 {
		t.Fatalf("expected RejectedAllProtected counter to advance to 1, got %+v", tbl2.CapStats())
	}
}

// TestSnapshotNormalizesHoldDownUntilWhenNotInHoldDown pins the
// FlapEntry contract documented at types.go: "HoldDownUntil is zero
// when not in hold-down". Both Snapshot and FlapSnapshot must enforce
// this invariant on the wire, regardless of whether TickTTL has had
// a chance to clear fs.holdDownUntil yet.
//
// Scenario the test reproduces: hold-down was armed, clock advanced
// past fs.holdDownUntil, BUT TickTTL has not run (it ticks every 10 s
// in production; an unrelated mutation can trigger rebuildRoutingSnapshot
// in between). Without normalization, callers see InHoldDown=false
// alongside a non-zero HoldDownUntil pointing at the past — which
// directly contradicts the FlapEntry doc-comment and confuses any
// dashboard that displays "hold-down expires at X" while the boolean
// flag says "not in hold-down".
func TestSnapshotNormalizesHoldDownUntilWhenNotInHoldDown(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}

	tbl := NewTable(
		WithClock(clock.nowFunc),
		WithLocalOrigin("self"),
		WithFlapWindow(120*time.Second),
		WithFlapThreshold(2),
		WithHoldDownDuration(30*time.Second),
	)

	// Arm hold-down via a flap burst.
	mustAddDirect(t, tbl, "peerA")
	mustRemoveDirect(t, tbl, "peerA")
	mustAddDirect(t, tbl, "peerA")
	mustRemoveDirect(t, tbl, "peerA")

	// Sanity: hold-down is currently armed and the snapshot reports
	// a non-zero HoldDownUntil with InHoldDown=true.
	snap := tbl.Snapshot()
	if len(snap.FlapState) == 0 || !snap.FlapState[0].InHoldDown {
		t.Fatalf("test setup: expected InHoldDown=true after burst, got %+v", snap.FlapState)
	}
	if snap.FlapState[0].HoldDownUntil.IsZero() {
		t.Fatal("test setup: expected non-zero HoldDownUntil while in hold-down")
	}

	// Advance the clock past fs.holdDownUntil but do NOT call
	// TickTTL — this is the gap where rebuildRoutingSnapshot can
	// fire on an unrelated event (e.g. another peer added a route)
	// and snapshot the table while fs.holdDownUntil is still a
	// stale non-zero past timestamp.
	clock.advance(40 * time.Second)

	// Snapshot must normalize: InHoldDown=false ⇒ HoldDownUntil.IsZero().
	snap = tbl.Snapshot()
	if len(snap.FlapState) == 0 {
		t.Fatal("FlapState dropped peerA too eagerly; withdrawTimes were still inside flapWindow")
	}
	entry := snap.FlapState[0]
	if entry.InHoldDown {
		t.Fatalf("Snapshot: expected InHoldDown=false after clock advanced past hold-down, got %+v", entry)
	}
	if !entry.HoldDownUntil.IsZero() {
		t.Fatalf("Snapshot: HoldDownUntil must be zero when InHoldDown=false; "+
			"FlapEntry contract violated. Got HoldDownUntil=%s "+
			"(stale past timestamp leaked because Snapshot copied fs.holdDownUntil unconditionally)",
			entry.HoldDownUntil)
	}

	// FlapSnapshot must apply the same normalization.
	flap := tbl.FlapSnapshot()
	if len(flap) == 0 {
		t.Fatal("FlapSnapshot dropped peerA too eagerly")
	}
	if flap[0].InHoldDown {
		t.Fatalf("FlapSnapshot: expected InHoldDown=false, got %+v", flap[0])
	}
	if !flap[0].HoldDownUntil.IsZero() {
		t.Fatalf("FlapSnapshot: HoldDownUntil must be zero when InHoldDown=false; got %s",
			flap[0].HoldDownUntil)
	}
}

// TestTickTTLMarksDirtyOnlyWhenSomethingExpired verifies that an empty
// or no-op tick (nothing expired, no flap-state cleanup needed) does not
// force a republish. Without this gate every 10 s TickTTL on a stable
// network would rebuild the snapshot — defeating the dirty-flag economy.
func TestTickTTLMarksDirtyOnlyWhenSomethingExpired(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}

	tbl := NewTable(WithClock(clock.nowFunc), WithDefaultTTL(10*time.Second))
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	tbl.ConsumeDirty()

	// Tick before TTL — nothing expires, no flap state, must stay clean.
	if r := tbl.TickTTL(); r.Removed != 0 {
		t.Fatalf("unexpected expiry: %+v", r)
	}
	if tbl.IsDirty() {
		t.Fatal("no-op TickTTL marked dirty")
	}

	// Advance past TTL — the entry expires.
	clock.advance(20 * time.Second)
	if r := tbl.TickTTL(); r.Removed != 1 {
		t.Fatalf("expected 1 removed, got %+v", r)
	}
	if !tbl.ConsumeDirty() {
		t.Fatal("TickTTL with expiry did not mark dirty")
	}
}

// TestRecordSuccessfulRouteAddDirtyOnlyOnRealReset verifies that the
// hot-path no-op (no flap state, or already-clean flap state) does not
// mark dirty. A successful announce-plane round-trip on a peer that has
// never flapped fires this code on every successful exchange — without
// the gate the publisher would rebuild on every announce tick.
func TestRecordSuccessfulRouteAddDirtyOnlyOnRealReset(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("self"))

	t.Run("no_flap_state_keeps_clean", func(t *testing.T) {
		tbl.RecordSuccessfulRouteAdd("ghost")
		if tbl.IsDirty() {
			t.Fatal("RecordSuccessfulRouteAdd on unknown peer marked dirty")
		}
	})

	t.Run("real_reset_marks_dirty", func(t *testing.T) {
		// Force a flap: configure a tiny window/threshold and burst.
		tbl := NewTable(
			WithLocalOrigin("self"),
			WithFlapWindow(60*time.Second),
			WithFlapThreshold(2),
			WithHoldDownDuration(5*time.Second),
		)
		mustAddDirect(t, tbl, "peerA")
		mustRemoveDirect(t, tbl, "peerA")
		mustAddDirect(t, tbl, "peerA")
		mustRemoveDirect(t, tbl, "peerA")
		tbl.ConsumeDirty()

		// Now consecutiveFlaps > 0; a successful announce after stabilisation
		// resets it and must mark dirty (HoldDownUntil semantics change
		// for the next burst).
		tbl.RecordSuccessfulRouteAdd("peerA")
		if !tbl.ConsumeDirty() {
			t.Fatal("RecordSuccessfulRouteAdd with real reset did not mark dirty")
		}

		// Idempotent second call — no fields to reset, must stay clean.
		tbl.RecordSuccessfulRouteAdd("peerA")
		if tbl.IsDirty() {
			t.Fatal("idempotent RecordSuccessfulRouteAdd marked dirty")
		}
	})
}

// TestSnapshotDoesNotMarkDirty verifies that read paths (Snapshot, Lookup,
// Announceable, AnnounceTo, Size, ActiveSize) never mark the table dirty.
// Every reader on the hot path lives outside the publisher loop; if any
// of them flipped dirty the publisher would be forced into an infinite
// rebuild loop driven by its own consumers.
func TestSnapshotDoesNotMarkDirty(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("self"))
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	tbl.ConsumeDirty()

	_ = tbl.Snapshot()
	_ = tbl.Lookup("alice")
	_ = tbl.Announceable("nobody")
	_ = tbl.AnnounceTo("nobody")
	_ = tbl.Size()
	_ = tbl.ActiveSize()
	_ = tbl.FlapSnapshot()
	_ = tbl.InspectTriple(RouteTriple{Identity: "alice", Origin: "bob", NextHop: "charlie"})

	if tbl.IsDirty() {
		t.Fatal("read-only path marked dirty")
	}
}

// mutableClock is a tiny test helper so TickTTL tests can advance wall time
// without resorting to time.Sleep.
type mutableClock struct {
	now time.Time
}

func (c *mutableClock) nowFunc() time.Time { return c.now }

func (c *mutableClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// TestPublishedSnapshotPointerLoadIsLockFreeUnderWriteLock pins the
// foundational mechanism behind Service.RoutingSnapshot: an
// atomic.Pointer load of a previously-published Snapshot completes
// without touching Table.t.mu, so a held writer Lock cannot starve
// readers that go through the published pointer.
//
// This test lives in package routing because the deterministic writer
// hold is implemented by acquiring t.mu.Lock directly — the field is
// unexported and there is no production API to acquire it. Service-
// level cache semantics are covered separately by
// TestServiceRoutingSnapshotReturnsCachedNotFresh in package node,
// which proves Service.RoutingSnapshot reads the cached pointer
// rather than calling Table.Snapshot.
//
// Together the two tests close the contract: the routing test proves
// the mechanism is lock-free; the node test proves the production
// path uses that mechanism.
func TestPublishedSnapshotPointerLoadIsLockFreeUnderWriteLock(t *testing.T) {
	tbl := NewTable()
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}

	// Publish a snapshot through an atomic.Pointer that mirrors the
	// shape Service uses (see internal/core/node/routing_snapshot.go).
	var ptr atomic.Pointer[Snapshot]
	primed := tbl.Snapshot()
	ptr.Store(&primed)

	// Hold the write lock for `hold` deterministically. This blocks
	// every reader that takes t.mu.RLock (Table.Snapshot, Lookup,
	// Announceable, AnnounceTo, every writer) but must NOT block the
	// atomic.Pointer.Load below.
	const hold = 500 * time.Millisecond
	holdStart := time.Now()
	holderEntered := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		tbl.mu.Lock()
		close(holderEntered)
		time.Sleep(hold)
		tbl.mu.Unlock()
	}()
	<-holderEntered

	const reads = 1000
	start := time.Now()
	for i := 0; i < reads; i++ {
		snap := ptr.Load()
		if snap == nil || snap.TotalEntries != 1 {
			t.Fatalf("read #%d: unexpected snapshot %+v", i, snap)
		}
	}
	elapsed := time.Since(start)

	// Loads through the atomic pointer must finish far below the hold
	// window. A regression that re-routes the load through t.mu.RLock
	// would push elapsed close to `hold`.
	if elapsed >= hold/2 {
		t.Fatalf("%d atomic.Pointer.Load calls took %s while writer held t.mu.Lock for %s; "+
			"expected reads to finish far below the hold window",
			reads, elapsed, hold)
	}
	// Sanity: writer was still holding when reads finished.
	if time.Since(holdStart) >= hold {
		t.Fatalf("writer hold ended (%s elapsed) before reads finished; "+
			"increase hold or reduce read count to keep the assertion meaningful",
			time.Since(holdStart))
	}

	<-holderDone
}
