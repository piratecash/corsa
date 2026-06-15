package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestWithdrawRouteRejectsZeroIdentity pins the guard that a withdrawal
// for the zero (absent) identity is rejected before any mutation. Without
// it, an ingested wire withdrawal carrying an all-zero Identity (which
// passes the syntactic 40-hex check but decodes to the zero sentinel)
// would tombstone a claim under the zero key and inflate the snapshot's
// TotalEntries with a row that never represents a real route.
func TestWithdrawRouteRejectsZeroIdentity(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))

	var zero PeerIdentity
	if tbl.WithdrawRoute(zero, domaintest.ID("origin"), domaintest.ID("uplink"), 5) {
		t.Fatal("WithdrawRoute(zeroIdentity) must return false (no mutation)")
	}

	snap := tbl.Snapshot()
	if snap.TotalEntries != 0 {
		t.Fatalf("zero-identity withdrawal must not create any entry, got TotalEntries=%d", snap.TotalEntries)
	}
	if _, ok := snap.Routes[zero]; ok {
		t.Fatal("zero-identity withdrawal must not create a routes bucket under the zero key")
	}
}
