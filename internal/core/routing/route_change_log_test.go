package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// route_change_log_test.go pins the Phase 3 change-log ring semantics:
// distinct-identity enumeration since a cursor, the "nothing changed"
// steady state, bulk-reset force-full, and ring-overflow force-full.

func idSet(ids []PeerIdentity) map[PeerIdentity]struct{} {
	m := make(map[PeerIdentity]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}

func TestRouteChangeLog_SinceReturnsDistinctChangedIdentities(t *testing.T) {
	l := newRouteChangeLog(64)

	start := l.headLocked()
	if start != 0 {
		t.Fatalf("fresh log head = %d, want 0", start)
	}

	a, b, c := domaintest.ID("a"), domaintest.ID("b"), domaintest.ID("c")
	l.recordLocked(a)
	l.recordLocked(b)
	l.recordLocked(a) // duplicate of a — must collapse in the set
	l.recordLocked(c)

	changed, needFull := l.sinceUpToLocked(start, l.headLocked())
	if needFull {
		t.Fatal("in-ring cursor must not force full")
	}
	if l.headLocked() != 4 {
		t.Fatalf("head = %d, want 4", l.headLocked())
	}
	got := idSet(changed)
	if len(got) != 3 {
		t.Fatalf("changed set size = %d, want 3 (a,b,c distinct): %+v", len(got), changed)
	}
	for _, id := range []PeerIdentity{a, b, c} {
		if _, ok := got[id]; !ok {
			t.Fatalf("changed set missing %s", id)
		}
	}
}

func TestRouteChangeLog_NothingChangedSteadyState(t *testing.T) {
	l := newRouteChangeLog(64)
	l.recordLocked(domaintest.ID("a"))

	cursor := l.headLocked()
	// No further changes — a cursor at head sees an empty set, no force-full.
	changed, needFull := l.sinceUpToLocked(cursor, l.headLocked())
	if needFull {
		t.Fatal("cursor at head must not force full")
	}
	if len(changed) != 0 {
		t.Fatalf("expected empty changed set, got %+v", changed)
	}
	if l.headLocked() != cursor {
		t.Fatalf("head = %d, want unchanged %d", l.headLocked(), cursor)
	}
}

func TestRouteChangeLog_BulkResetForcesFull(t *testing.T) {
	l := newRouteChangeLog(64)
	l.recordLocked(domaintest.ID("a"))
	cursor := l.headLocked() // peer is caught up here

	// A bulk mutation touched an unbounded identity set.
	l.recordFullLocked()
	l.recordLocked(domaintest.ID("b")) // a normal change after the reset

	_, needFull := l.sinceUpToLocked(cursor, l.headLocked())
	if !needFull {
		t.Fatal("cursor below a bulk reset must force full")
	}

	// A peer that force-synced after the reset (cursor == head) is fine again.
	if _, nf := l.sinceUpToLocked(l.headLocked(), l.headLocked()); nf {
		t.Fatal("cursor at head after reset must not force full")
	}
}

func TestRouteChangeLog_RingOverflowForcesFull(t *testing.T) {
	const cap = 8
	l := newRouteChangeLog(cap)

	cursor := l.headLocked() // 0
	// Append more than the ring can remember — the cursor falls out.
	for i := 0; i < cap+5; i++ {
		l.recordLocked(domaintest.ID("dest-" + string(rune('a'+i))))
	}

	_, needFull := l.sinceUpToLocked(cursor, l.headLocked())
	if !needFull {
		t.Fatal("a cursor further behind than the ring capacity must force full")
	}

	// A cursor still inside the ring window is served incrementally.
	recent := l.headLocked() - 3
	_, nf := l.sinceUpToLocked(recent, l.headLocked())
	if nf {
		t.Fatal("a cursor within the ring window must not force full")
	}
}

func TestRouteChangeLog_ZeroIdentityIgnored(t *testing.T) {
	l := newRouteChangeLog(64)
	l.recordLocked(PeerIdentity{})
	if l.headLocked() != 0 {
		t.Fatalf("zero identity must not advance head, got %d", l.headLocked())
	}
}
