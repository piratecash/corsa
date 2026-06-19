package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_shadow_cursor_test.go pins the Phase 3 deploy-1 shadow validator
// (internal): shadowValidateDelta flags (and counts) a delta identity the
// change journal did not record in [cursor, bound), and skips a needFull
// window. The loop-level "cursor commits only on send success" contract is
// covered by the external integration test in announce_shadow_commit_test.go.

func recordChange(t *testing.T, tbl *Table, id PeerIdentity) {
	t.Helper()
	tbl.mu.Lock()
	tbl.markRouteChangedLocked(id)
	tbl.mu.Unlock()
}

func journalHead(t *testing.T, tbl *Table) uint64 {
	t.Helper()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	return tbl.changeLog.headLocked()
}

func TestShadowValidateDelta_FlagsUnjournalledIdentity(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	a := &AnnounceLoop{table: tbl}
	x, y := domaintest.ID("x"), domaintest.ID("y")

	recordChange(t, tbl, x) // head = 1
	a.shadowValidateDelta(domaintest.ID("peer"), nil, 0, journalHead(t, tbl), []AnnounceEntry{{Identity: x}})
	if got := a.ShadowDivergenceTotal(); got != 0 {
		t.Fatalf("journalled identity must not diverge, got %d", got)
	}

	recordChange(t, tbl, x) // head = 2
	a.shadowValidateDelta(domaintest.ID("peer"), nil, 1, journalHead(t, tbl), []AnnounceEntry{{Identity: y}})
	if got := a.ShadowDivergenceTotal(); got != 1 {
		t.Fatalf("unjournalled identity must diverge once, got %d", got)
	}
}

// TestShadowValidateDelta_SeqNoOnlyMissIsNotDivergence pins the deploy-1
// classification: an unjournalled delta entry that differs from the last-sent
// baseline ONLY in its wire SeqNo is emit-side bookkeeping, not an unwired
// mutation — it must NOT count as structural divergence, only as a SeqNo-only
// miss.
func TestShadowValidateDelta_SeqNoOnlyMissIsNotDivergence(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	a := &AnnounceLoop{table: tbl}
	x := domaintest.ID("x")
	origin := domaintest.ID("o")

	// Baseline: x at SeqNo 5; nothing journalled since (cursor == head).
	old := &AnnounceSnapshot{Entries: []AnnounceEntry{{Identity: x, Origin: origin, Hops: 2, SeqNo: 5}}}
	head := journalHead(t, tbl)

	// Delta carries x with the SAME routing fields but a bumped SeqNo.
	a.shadowValidateDelta(domaintest.ID("peer"), old, head, head,
		[]AnnounceEntry{{Identity: x, Origin: origin, Hops: 2, SeqNo: 6}})

	if got := a.ShadowDivergenceTotal(); got != 0 {
		t.Fatalf("SeqNo-only change must not be structural divergence, got %d", got)
	}
	if got := a.ShadowSeqOnlyMissesTotal(); got != 1 {
		t.Fatalf("SeqNo-only change must be counted as a seq-only miss, got %d", got)
	}

	// A Hops change at the same (unjournalled) identity IS structural.
	a.shadowValidateDelta(domaintest.ID("peer"), old, head, head,
		[]AnnounceEntry{{Identity: x, Origin: origin, Hops: 3, SeqNo: 7}})
	if got := a.ShadowDivergenceTotal(); got != 1 {
		t.Fatalf("Hops change must count as structural divergence, got %d", got)
	}

	// An Origin (lineage) change is wire content the cursor model must re-emit,
	// so Origin+SeqNo is structural even though Origin is not in trackKeyFor.
	a.shadowValidateDelta(domaintest.ID("peer"), old, head, head,
		[]AnnounceEntry{{Identity: x, Origin: domaintest.ID("o2"), Hops: 2, SeqNo: 8}})
	if got := a.ShadowDivergenceTotal(); got != 2 {
		t.Fatalf("Origin change must count as structural divergence, got %d", got)
	}
}

func TestShadowValidateDelta_SkipsNeedFullWindow(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	a := &AnnounceLoop{table: tbl}

	tbl.mu.Lock()
	tbl.markRouteChangedFullLocked() // bulk reset; cursor 0 is below it → needFull
	tbl.mu.Unlock()

	a.shadowValidateDelta(domaintest.ID("peer"), nil, 0, journalHead(t, tbl), []AnnounceEntry{{Identity: domaintest.ID("z")}})
	if got := a.ShadowDivergenceTotal(); got != 0 {
		t.Fatalf("needFull window must skip the check, got %d", got)
	}
}
