package node

import (
	"testing"
	"time"
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

	cm.RetainOnly(mustAddr(keep))

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

	cm.RetainOnly(mustAddr("203.0.113.9:64646"))

	waitFor(t, 2*time.Second, "all slots evicted", func() bool {
		return cm.SlotCount() == 0
	})
}
