package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// ---------------------------------------------------------------------------
// CM setup-failure cooldown gate — short-circuit retry on banned address.
//
// Why this test exists: setupFailureBanThreshold gates PeerProvider.Candidates()
// (gate 6c.3) so a banned address is not picked by fill(). But CM's
// retryAfterBackoff bypasses PeerProvider entirely — it dials slot.DialAddresses
// directly. Without the gate inside handleActiveSessionLost the cooldown only
// activates AFTER the slot is replaced (after reconnectMaxRetries failed
// retries = ~14s of agitation at the default 2-4-8s backoff).
//
// The gate added in handleActiveSessionLost (WasHealthy=false path) checks
// IsSetupFailureBannedFn(slot.Address) and, if true, skips the retry cycle
// entirely: replace immediately + fill() to give the slot to a different
// candidate.
// ---------------------------------------------------------------------------

// TestCM_SetupFailureCooldown_ShortCircuitsRetry pins the contract: a
// WasHealthy=false event for an address that the cooldown gate reports
// as banned must produce a slot replacement on the FIRST event, not after
// reconnectMaxRetries retries.
func TestCM_SetupFailureCooldown_ShortCircuitsRetry(t *testing.T) {
	t.Parallel()

	const bannedAddr = "10.0.0.1:64646"
	const cleanAddr = "10.0.0.2:64646"

	b := testCMConfig(bannedAddr, cleanAddr)
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	// Setup-failure gate: address bannedAddr is reported as banned.
	// cleanAddr is allowed.
	var bannedLookups int32
	b.Cfg.IsSetupFailureBannedFn = func(addr domain.PeerAddress) bool {
		atomic.AddInt32(&bannedLookups, 1)
		return string(addr) == bannedAddr
	}

	// Track dial attempts per address so we can assert the banned one
	// did NOT enter retry-after-backoff loop.
	var mu sync.Mutex
	dialAttempts := map[string]int{}
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		mu.Lock()
		dialAttempts[string(addrs[0])]++
		mu.Unlock()
		session := fakePeerSession(addrs[0], domaintest.ID("id-"+string(addrs[0])))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Wait until the first slot becomes Active (must be the bannedAddr — it
	// was first in the candidate list).
	waitFor(t, 2*time.Second, "first slot active", func() bool {
		return cm.ActiveCount() == 1
	})

	slots := cm.Slots()
	if len(slots) != 1 {
		t.Fatalf("expected 1 slot, got %d", len(slots))
	}
	firstAddr := string(slots[0].Address)
	if firstAddr != bannedAddr {
		t.Fatalf("expected banned address %q to be filled first, got %q", bannedAddr, firstAddr)
	}
	gen := slots[0].Generation

	// Simulate setup failure (WasHealthy=false). With the cooldown gate
	// the slot MUST be replaced on this first event — no retry-after-
	// backoff cycle against the banned address.
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr(bannedAddr),
		Identity:       domaintest.ID("id-banned"),
		Error:          errors.New("sync: EOF"),
		WasHealthy:     false,
		SlotGeneration: gen,
	})

	// fill() must pick the clean address as the replacement.
	waitFor(t, 2*time.Second, "clean address replaces banned", func() bool {
		ss := cm.Slots()
		return len(ss) == 1 && string(ss[0].Address) == cleanAddr
	})

	// Assert exactly one dial attempt against the banned address. If the
	// retry-within-slot path was taken instead of the cooldown short-
	// circuit, we would see 2-4 attempts here (initial + retries up to
	// reconnectMaxRetries) before replacement.
	mu.Lock()
	bannedDials := dialAttempts[bannedAddr]
	cleanDials := dialAttempts[cleanAddr]
	mu.Unlock()

	if bannedDials != 1 {
		t.Fatalf("banned address dialled %d times; expected 1 (retry must be short-circuited)", bannedDials)
	}
	if cleanDials < 1 {
		t.Fatalf("clean address should have been dialled at least once after replace; got %d", cleanDials)
	}
	if atomic.LoadInt32(&bannedLookups) == 0 {
		t.Fatal("IsSetupFailureBannedFn was never consulted")
	}
}

// TestCM_SetupFailureCooldown_NilCallbackKeepsLegacyRetry guards the
// opt-in contract: when IsSetupFailureBannedFn is nil (legacy
// configurations, tests that do not exercise the gate), the
// reconnectMaxRetries retry budget continues to apply unchanged.
func TestCM_SetupFailureCooldown_NilCallbackKeepsLegacyRetry(t *testing.T) {
	t.Parallel()

	const peerAddr = "10.0.0.1:64646"

	b := testCMConfig(peerAddr)
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.IsSetupFailureBannedFn = nil // explicit: opt out

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		session := fakePeerSession(addrs[0], domaintest.ID("id-"+string(addrs[0])))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "initial active", func() bool {
		return cm.ActiveCount() == 1
	})

	gen := cm.Slots()[0].Generation

	// Three WasHealthy=false events (RetryCount goes 1, 2, 3 — all
	// within reconnectMaxRetries=3). The slot must NOT be replaced
	// after the first event, because no cooldown gate is wired.
	for i := 0; i < 3; i++ {
		cm.EmitSlot(ActiveSessionLost{
			Address:        mustAddr(peerAddr),
			Identity:       domaintest.ID("id-peer"),
			Error:          errors.New("sync: EOF"),
			WasHealthy:     false,
			SlotGeneration: gen,
		})
		// Wait for the retry to fire (dial count grows) and produce
		// a new generation we can use for the next event.
		waitFor(t, 2*time.Second, "retry dialled", func() bool {
			return atomic.LoadInt32(&dialCount) >= int32(2+i)
		})
		waitFor(t, 2*time.Second, "slot re-active", func() bool {
			return cm.ActiveCount() == 1
		})
		gen = cm.Slots()[0].Generation
	}

	// Slot must still be on peerAddr (no replacement was triggered).
	ss := cm.Slots()
	if len(ss) != 1 || string(ss[0].Address) != peerAddr {
		t.Fatalf("legacy retry budget violated: slot replaced unexpectedly; slots=%+v", ss)
	}
}
