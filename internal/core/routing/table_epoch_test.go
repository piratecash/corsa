package routing

import "testing"

// table_epoch_test.go pins the Phase 4 Table.Epoch contract: default 1
// for production Table construction, deterministic override via
// WithEpoch for tests / fixed scenarios. The receiver's epoch logic is
// exercised in announce_state_v3_epoch_test.go; this file covers the
// sender-side accessor.

func TestTableEpoch_DefaultIsOne(t *testing.T) {
	tbl := NewTable()
	if got := tbl.Epoch(); got != 1 {
		t.Fatalf("default epoch: got %d want 1", got)
	}
}

func TestTableEpoch_WithEpochOverride(t *testing.T) {
	tbl := NewTable(WithEpoch(424242))
	if got := tbl.Epoch(); got != 424242 {
		t.Fatalf("WithEpoch override: got %d want 424242", got)
	}
}
