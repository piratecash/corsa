package node

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// setupFailure tracking — service-side counter & cooldown.
//
// Mirrors the recordRemoteBan family but is driven by our own observation
// (cm_session_setup_failed) rather than a peer-banned notice. The behavioural
// contract:
//
//   - first (setupFailureBanThreshold - 1) failures: noted, no cooldown
//   - reaching the threshold: cooldownUntil = now + setupFailureCooldown
//   - additional failures inside the cooldown: cooldown is re-anchored to
//     now + setupFailureCooldown (sliding window — peer must go quiet to
//     leave the penalty box)
//   - clearSetupFailuresLocked: wipes both counter and cooldown
//   - isSetupFailureBanned: read-only, true only inside an unexpired window
//
// These tests pin the exact thresholds + side effects so a refactor that
// silently shifts them surfaces as a failed test, not a regression in the
// field.
// ---------------------------------------------------------------------------

func newSetupFailureService() *Service {
	return &Service{
		setupFailures: make(map[domain.PeerAddress]*setupFailureEntry),
	}
}

func TestRecordSetupFailureLocked_BelowThresholdNoBan(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")
	now := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)

	for i := 0; i < setupFailureBanThreshold-1; i++ {
		svc.recordSetupFailureLocked(addr, now.Add(time.Duration(i)*time.Second))
	}

	if svc.isSetupFailureBannedAt(addr, now.Add(setupFailureBanThreshold*time.Second)) {
		t.Fatal("must not be banned before threshold reached")
	}

	entry := svc.setupFailures[addr]
	if entry == nil {
		t.Fatal("expected entry recorded for below-threshold failures")
	}
	if entry.Consecutive != setupFailureBanThreshold-1 {
		t.Fatalf("Consecutive = %d, want %d", entry.Consecutive, setupFailureBanThreshold-1)
	}
	if !entry.CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil must be zero below threshold, got %v", entry.CooldownUntil)
	}
}

func TestRecordSetupFailureLocked_ThresholdSetsCooldown(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")
	now := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)

	for i := 0; i < setupFailureBanThreshold; i++ {
		svc.recordSetupFailureLocked(addr, now)
	}

	if !svc.isSetupFailureBannedAt(addr, now) {
		t.Fatal("must be banned immediately after hitting threshold")
	}

	entry := svc.setupFailures[addr]
	if entry.Consecutive != setupFailureBanThreshold {
		t.Fatalf("Consecutive = %d, want %d", entry.Consecutive, setupFailureBanThreshold)
	}
	want := now.Add(setupFailureCooldown)
	if !entry.CooldownUntil.Equal(want) {
		t.Fatalf("CooldownUntil = %v, want %v", entry.CooldownUntil, want)
	}
}

func TestRecordSetupFailureLocked_AdditionalFailureSlidesCooldown(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")
	t0 := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)

	for i := 0; i < setupFailureBanThreshold; i++ {
		svc.recordSetupFailureLocked(addr, t0)
	}
	firstUntil := svc.setupFailures[addr].CooldownUntil

	// Another failure 5 seconds later must extend cooldownUntil.
	later := t0.Add(5 * time.Second)
	svc.recordSetupFailureLocked(addr, later)

	got := svc.setupFailures[addr].CooldownUntil
	if !got.After(firstUntil) {
		t.Fatalf("additional failure must slide cooldown; got %v, want > %v", got, firstUntil)
	}
	want := later.Add(setupFailureCooldown)
	if !got.Equal(want) {
		t.Fatalf("CooldownUntil = %v, want %v (now + cooldown)", got, want)
	}
}

func TestIsSetupFailureBanned_ExpiredCooldownAllows(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")
	t0 := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)

	for i := 0; i < setupFailureBanThreshold; i++ {
		svc.recordSetupFailureLocked(addr, t0)
	}

	after := t0.Add(setupFailureCooldown + time.Second)
	if svc.isSetupFailureBannedAt(addr, after) {
		t.Fatal("must not be banned after cooldown elapsed")
	}
}

func TestIsSetupFailureBanned_NilEntryAllows(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")

	if svc.isSetupFailureBannedAt(addr, time.Now()) {
		t.Fatal("missing entry must mean not banned")
	}
}

func TestClearSetupFailuresLocked_RemovesEntry(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")
	now := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)

	for i := 0; i < setupFailureBanThreshold; i++ {
		svc.recordSetupFailureLocked(addr, now)
	}
	if !svc.isSetupFailureBannedAt(addr, now) {
		t.Fatal("precondition: peer must be banned before clear")
	}

	svc.clearSetupFailuresLocked(addr)
	if _, exists := svc.setupFailures[addr]; exists {
		t.Fatal("entry must be removed by clearSetupFailuresLocked")
	}
	if svc.isSetupFailureBannedAt(addr, now) {
		t.Fatal("cleared peer must not be reported as banned")
	}
}

func TestRecordSetupFailureLocked_NilMapLazilyAllocated(t *testing.T) {
	t.Parallel()

	svc := &Service{} // setupFailures is nil
	now := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)
	svc.recordSetupFailureLocked(mustAddr("1.2.3.4:64646"), now)

	if svc.setupFailures == nil {
		t.Fatal("setupFailures must be lazily allocated")
	}
	if _, ok := svc.setupFailures[mustAddr("1.2.3.4:64646")]; !ok {
		t.Fatal("entry must be inserted")
	}
}

// TestIsSetupFailureBanned_ConcurrentReaders verifies the exported
// (non-Locked) accessor takes the read lock and is safe under contention.
// Without proper locking this would race with recordSetupFailureLocked
// and trip -race.
func TestIsSetupFailureBanned_ConcurrentReaders(t *testing.T) {
	t.Parallel()

	svc := newSetupFailureService()
	addr := mustAddr("1.2.3.4:64646")
	now := time.Date(2026, 6, 2, 18, 46, 0, 0, time.UTC)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.IsSetupFailureBanned(addr)
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.peerMu.Lock()
			svc.recordSetupFailureLocked(addr, now)
			svc.peerMu.Unlock()
		}()
	}
	wg.Wait()
}
