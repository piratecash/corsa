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
	a.shadowValidateDelta(domaintest.ID("peer"), 0, journalHead(t, tbl), []AnnounceEntry{{Identity: x}})
	if got := a.ShadowDivergenceTotal(); got != 0 {
		t.Fatalf("journalled identity must not diverge, got %d", got)
	}

	recordChange(t, tbl, x) // head = 2
	a.shadowValidateDelta(domaintest.ID("peer"), 1, journalHead(t, tbl), []AnnounceEntry{{Identity: y}})
	if got := a.ShadowDivergenceTotal(); got != 1 {
		t.Fatalf("unjournalled identity must diverge once, got %d", got)
	}
}

func TestShadowValidateDelta_SkipsNeedFullWindow(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	a := &AnnounceLoop{table: tbl}

	tbl.mu.Lock()
	tbl.markRouteChangedFullLocked() // bulk reset; cursor 0 is below it → needFull
	tbl.mu.Unlock()

	a.shadowValidateDelta(domaintest.ID("peer"), 0, journalHead(t, tbl), []AnnounceEntry{{Identity: domaintest.ID("z")}})
	if got := a.ShadowDivergenceTotal(); got != 0 {
		t.Fatalf("needFull window must skip the check, got %d", got)
	}
}
