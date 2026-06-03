package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Per-peer route quarantine tests (Part 2 of probation + quarantine design).
//
// Unit-level coverage of the detection / arming / expiry / recidivism
// state machine. Integration with applyAnnounceEntries and the
// relay-forward path is covered by the existing routing tests once
// the gate is in place — these focus on the pure state-machine
// contract.
// ---------------------------------------------------------------------------

// newQuarantineFixture returns a minimal Service for state-machine
// tests. All maps initialised so tests can exercise both arming
// and the "already-allocated" branches.
func newQuarantineFixture() *Service {
	return &Service{
		peerQuarantine:        make(map[domain.PeerIdentity]routeQuarantineEntry),
		peerDisconnectHistory: make(map[domain.PeerIdentity][]time.Time),
	}
}

// TestQuarantine_NoTriggerBelowThreshold pins that a peer below
// the disconnect threshold is NOT quarantined.
func TestQuarantine_NoTriggerBelowThreshold(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	peer := domain.PeerIdentity("test-peer-quiet")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	svc.peerMu.Lock()
	for i := 0; i < quarantineDisconnectThreshold-1; i++ {
		svc.maybeArmRouteQuarantineOnCloseLocked(peer, now.Add(time.Duration(i)*time.Second))
	}
	// Use the *Locked variant with the SAME controlled `now` we
	// armed with so the test stays deterministic: the public
	// IsPeerInRouteQuarantine consults time.Now(), which would
	// silently start failing in any test that armed with a fixed
	// past date once wall-clock time crossed
	// armTime + quarantineBaseDuration.
	banned := svc.isPeerInRouteQuarantineLocked(peer, now.Add(time.Second))
	svc.peerMu.Unlock()

	if banned {
		t.Fatalf("peer in quarantine after %d disconnects; threshold is %d",
			quarantineDisconnectThreshold-1, quarantineDisconnectThreshold)
	}
}

// TestQuarantine_TriggersOnDisconnectRate pins that hitting the
// threshold inside the window arms quarantine.
func TestQuarantine_TriggersOnDisconnectRate(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	peer := domain.PeerIdentity("test-peer-flapping")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	svc.peerMu.Lock()
	for i := 0; i < quarantineDisconnectThreshold; i++ {
		svc.maybeArmRouteQuarantineOnCloseLocked(peer, now.Add(time.Duration(i)*time.Second))
	}
	// Controlled now (see TestQuarantine_NoTriggerBelowThreshold for
	// the rationale on using *Locked instead of the public helper).
	banned := svc.isPeerInRouteQuarantineLocked(peer, now.Add(time.Second))
	svc.peerMu.Unlock()

	if !banned {
		t.Fatalf("peer NOT in quarantine after %d disconnects", quarantineDisconnectThreshold)
	}

	svc.peerMu.RLock()
	entry := svc.peerQuarantine[peer]
	svc.peerMu.RUnlock()
	if entry.Reason != "disconnect_storm" {
		t.Fatalf("entry.Reason = %q, want disconnect_storm", entry.Reason)
	}
	if entry.Strikes != 1 {
		t.Fatalf("entry.Strikes = %d, want 1 on first arm", entry.Strikes)
	}
}

// TestQuarantine_EventsOutsideWindowDoNotCount checks the sliding-
// window behaviour: events older than the window are pruned and
// do not count toward the threshold.
func TestQuarantine_EventsOutsideWindowDoNotCount(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	peer := domain.PeerIdentity("test-peer-history")
	t0 := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// Three old events (outside the window).
	svc.peerMu.Lock()
	for i := 0; i < 3; i++ {
		svc.recordPeerDisconnectLocked(peer, t0.Add(time.Duration(i)*time.Second))
	}
	svc.peerMu.Unlock()

	// Advance past the window.
	later := t0.Add(quarantineDisconnectWindow + time.Minute)

	svc.peerMu.Lock()
	if svc.disconnectRateExceedsLocked(peer, later) {
		svc.peerMu.Unlock()
		t.Fatal("expected old events to be excluded by sliding window")
	}
	svc.peerMu.Unlock()
}

// TestQuarantine_ExpiryRestoresNormalProcessing covers the cooldown
// elapsing: after Until passes, the peer is no longer in quarantine.
// Strike counter survives until the recidivism window cools off.
func TestQuarantine_ExpiryRestoresNormalProcessing(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	peer := domain.PeerIdentity("test-peer-expiring")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	svc.peerMu.Lock()
	for i := 0; i < quarantineDisconnectThreshold; i++ {
		svc.maybeArmRouteQuarantineOnCloseLocked(peer, now.Add(time.Duration(i)*time.Second))
	}
	armedEntry := svc.peerQuarantine[peer]
	svc.peerMu.Unlock()

	afterCooldown := armedEntry.Until.Add(time.Second)

	svc.peerMu.RLock()
	stillBanned := svc.isPeerInRouteQuarantineLocked(peer, afterCooldown)
	svc.peerMu.RUnlock()
	if stillBanned {
		t.Fatal("peer still in quarantine after Until elapsed")
	}
}

// TestQuarantine_RecidivismDoubles pins that a re-trigger inside
// the recidivism window grows the cooldown exponentially.
func TestQuarantine_RecidivismDoubles(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	peer := domain.PeerIdentity("test-peer-recidivist")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	armCycle := func(at time.Time) routeQuarantineEntry {
		svc.peerMu.Lock()
		defer svc.peerMu.Unlock()
		for i := 0; i < quarantineDisconnectThreshold; i++ {
			svc.maybeArmRouteQuarantineOnCloseLocked(peer, at.Add(time.Duration(i)*time.Second))
		}
		return svc.peerQuarantine[peer]
	}

	first := armCycle(now)
	if first.Strikes != 1 {
		t.Fatalf("first arm strikes = %d, want 1", first.Strikes)
	}
	firstDur := first.Until.Sub(now.Add(time.Duration(quarantineDisconnectThreshold-1) * time.Second))

	// Re-arm shortly after first.Until (still inside recidivism window).
	reArmTime := first.Until.Add(time.Second)
	second := armCycle(reArmTime)
	if second.Strikes != 2 {
		t.Fatalf("second arm strikes = %d, want 2 (recidivism)", second.Strikes)
	}
	secondDur := second.Until.Sub(reArmTime.Add(time.Duration(quarantineDisconnectThreshold-1) * time.Second))
	if secondDur < firstDur*2-time.Second {
		t.Fatalf("second cooldown %v not roughly 2x first cooldown %v", secondDur, firstDur)
	}
}

// TestQuarantine_RecidivismResetsAfterQuietPeriod pins that if
// the peer stays calm beyond the recidivism window, the next
// quarantine restarts at strike=1 (no exponential growth).
func TestQuarantine_RecidivismResetsAfterQuietPeriod(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	peer := domain.PeerIdentity("test-peer-reformed")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	svc.peerMu.Lock()
	for i := 0; i < quarantineDisconnectThreshold; i++ {
		svc.maybeArmRouteQuarantineOnCloseLocked(peer, now.Add(time.Duration(i)*time.Second))
	}
	svc.peerMu.Unlock()

	// Wait past the recidivism window before re-triggering.
	later := now.Add(quarantineRecidivismWindow + 2*time.Minute)

	svc.peerMu.Lock()
	for i := 0; i < quarantineDisconnectThreshold; i++ {
		svc.maybeArmRouteQuarantineOnCloseLocked(peer, later.Add(time.Duration(i)*time.Second))
	}
	entry := svc.peerQuarantine[peer]
	svc.peerMu.Unlock()

	if entry.Strikes != 1 {
		t.Fatalf("strikes after quiet-window reset = %d, want 1", entry.Strikes)
	}
}

// TestQuarantine_DurationCappedAtMax pins the upper bound on the
// exponential growth.
func TestQuarantine_DurationCappedAtMax(t *testing.T) {
	t.Parallel()

	d := computeQuarantineDuration(20)
	if d != quarantineMaxDuration {
		t.Fatalf("computeQuarantineDuration(20) = %v, want %v (cap)", d, quarantineMaxDuration)
	}
}

// TestQuarantine_IsPeerInRouteQuarantineEmptyIdentity guards
// the no-op path: empty identity should never report banned.
func TestQuarantine_IsPeerInRouteQuarantineEmptyIdentity(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	if svc.IsPeerInRouteQuarantine("") {
		t.Fatal("empty identity should not be reported as quarantined")
	}
}

// TestQuarantine_PurgeRemovesStaleEntries pins the cleanup contract.
// Entries with elapsed Until AND old LastArmed disappear; entries
// that still have a recent LastArmed survive (recidivism tracking).
func TestQuarantine_PurgeRemovesStaleEntries(t *testing.T) {
	t.Parallel()

	svc := newQuarantineFixture()
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	old := domain.PeerIdentity("very-old-peer")
	recent := domain.PeerIdentity("recent-peer")

	svc.peerMu.Lock()
	// "Old": LastArmed long ago, Until long elapsed.
	svc.peerQuarantine[old] = routeQuarantineEntry{
		Until:     now.Add(-time.Hour),
		LastArmed: now.Add(-2 * time.Hour),
		Strikes:   3,
		Reason:    "disconnect_storm",
	}
	// "Recent": Until just elapsed, LastArmed still recent.
	svc.peerQuarantine[recent] = routeQuarantineEntry{
		Until:     now.Add(-time.Second),
		LastArmed: now.Add(-time.Minute),
		Strikes:   2,
		Reason:    "disconnect_storm",
	}
	svc.purgeExpiredQuarantineLocked(now)
	svc.peerMu.Unlock()

	svc.peerMu.RLock()
	_, oldPresent := svc.peerQuarantine[old]
	_, recentPresent := svc.peerQuarantine[recent]
	svc.peerMu.RUnlock()

	if oldPresent {
		t.Fatal("old stale entry should have been purged")
	}
	if !recentPresent {
		t.Fatal("recent entry should survive (recidivism tracking)")
	}
}

// TestNextStrikeCount covers the small helper directly.
func TestNextStrikeCount(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		entry routeQuarantineEntry
		now   time.Time
		want  int
	}{
		{"first arm", routeQuarantineEntry{}, now, 1},
		{"recidivism within window", routeQuarantineEntry{LastArmed: now.Add(-time.Minute), Strikes: 2}, now, 3},
		{"reset after quiet window", routeQuarantineEntry{LastArmed: now.Add(-quarantineRecidivismWindow - time.Minute), Strikes: 5}, now, 1},
		{"zero strikes reset to 1", routeQuarantineEntry{LastArmed: now.Add(-time.Minute), Strikes: 0}, now, 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := nextStrikeCount(tc.entry, tc.now)
			if got != tc.want {
				t.Fatalf("nextStrikeCount = %d, want %d", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// chatty_routes trigger
// ---------------------------------------------------------------------------

// newChattyFixture returns a Service initialised for chatty_routes
// tests. peerAnnounceHistory is allocated explicitly so the test
// can exercise both arming and "already-allocated" branches.
func newChattyFixture() *Service {
	return &Service{
		peerQuarantine:        make(map[domain.PeerIdentity]routeQuarantineEntry),
		peerDisconnectHistory: make(map[domain.PeerIdentity][]time.Time),
		peerAnnounceHistory:   make(map[domain.PeerIdentity][]time.Time),
	}
}

// TestChattyRoutes_NoTriggerBelowThreshold pins that a quiet peer
// (fewer announces than chattyAnnounceThreshold inside the window)
// is NOT route-quarantined.
func TestChattyRoutes_NoTriggerBelowThreshold(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("quiet-peer")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	svc.peerMu.Lock()
	for i := 0; i < chattyAnnounceThreshold-1; i++ {
		svc.recordPeerAnnounceLocked(peer, now.Add(time.Duration(i)*time.Millisecond))
	}
	exceeds := svc.announceRateExceedsLocked(peer, now)
	banned := svc.isPeerInRouteQuarantineLocked(peer, now.Add(time.Second))
	svc.peerMu.Unlock()

	if exceeds {
		t.Fatalf("announceRateExceedsLocked true at %d events; threshold is %d",
			chattyAnnounceThreshold-1, chattyAnnounceThreshold)
	}
	if banned {
		t.Fatal("peer reported quarantined despite no arm call")
	}
}

// TestChattyRoutes_TriggersAtThreshold pins that exactly threshold
// frames inside the window arms chatty_routes quarantine.
func TestChattyRoutes_TriggersAtThreshold(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("chatty-peer")
	base := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// Drive recordInboundAnnounceAndMaybeArm through the public
	// wrapper so we exercise the same code path as the receive
	// handlers. Use a tight inter-arrival to fit inside the
	// chattyAnnounceWindow trivially.
	for i := 0; i < chattyAnnounceThreshold; i++ {
		svc.recordInboundAnnounceAndMaybeArm(peer, base.Add(time.Duration(i)*time.Millisecond))
	}

	svc.peerMu.RLock()
	entry, ok := svc.peerQuarantine[peer]
	svc.peerMu.RUnlock()

	if !ok {
		t.Fatal("peer not quarantined after threshold inbound announces")
	}
	if entry.Reason != "chatty_routes" {
		t.Fatalf("reason: got %q, want %q", entry.Reason, "chatty_routes")
	}
	if entry.Strikes != 1 {
		t.Fatalf("first arm should have strike=1, got %d", entry.Strikes)
	}
}

// TestChattyRoutes_EventsOutsideWindowDoNotCount pins the sliding-
// window pruning: frames older than chattyAnnounceWindow must NOT
// count toward the threshold.
func TestChattyRoutes_EventsOutsideWindowDoNotCount(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("paced-peer")
	base := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// (threshold-1) ancient frames + 1 fresh frame would superficially
	// look like a threshold-crossing if the window were unbounded.
	// But the ancient ones are well past chattyAnnounceWindow before
	// the fresh one — the sliding window must drop them.
	farPast := base.Add(-chattyAnnounceWindow - time.Hour)
	svc.peerMu.Lock()
	for i := 0; i < chattyAnnounceThreshold-1; i++ {
		svc.recordPeerAnnounceLocked(peer, farPast.Add(time.Duration(i)*time.Millisecond))
	}
	svc.recordPeerAnnounceLocked(peer, base)
	exceeds := svc.announceRateExceedsLocked(peer, base)
	svc.peerMu.Unlock()

	if exceeds {
		t.Fatal("announceRateExceedsLocked true with only one in-window event")
	}
}

// TestChattyRoutes_ReArmsOngoingQuarantineAfterDebounce pins the
// throttled re-arm: a peer that stays chatty across multiple
// chattyReArmDebounce windows must continue to escalate Strikes
// (one bump per debounce, not per frame). Without this, a
// sustained chatty peer would escape quarantine when the original
// cooldown elapses despite still being chatty.
func TestChattyRoutes_ReArmsOngoingQuarantineAfterDebounce(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("persistent-chatty-peer")
	base := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// First flood — arms quarantine. Spread events tightly so the
	// whole burst fits well inside chattyAnnounceWindow.
	for i := 0; i < chattyAnnounceThreshold; i++ {
		svc.recordInboundAnnounceAndMaybeArm(peer, base.Add(time.Duration(i)*time.Millisecond))
	}
	svc.peerMu.RLock()
	first := svc.peerQuarantine[peer]
	svc.peerMu.RUnlock()
	if first.Strikes != 1 {
		t.Fatalf("first arm strikes = %d, want 1 (per-frame re-arm leaked)", first.Strikes)
	}

	// Second flood AFTER the debounce window — must bump strikes
	// once and push Until forward. We move the wall-clock anchor
	// just past chattyReArmDebounce so shouldArmChattyLocked
	// allows exactly one re-arm.
	later := base.Add(chattyReArmDebounce + time.Second)
	for i := 0; i < chattyAnnounceThreshold; i++ {
		svc.recordInboundAnnounceAndMaybeArm(peer, later.Add(time.Duration(i)*time.Millisecond))
	}
	svc.peerMu.RLock()
	second := svc.peerQuarantine[peer]
	svc.peerMu.RUnlock()

	if second.Strikes != first.Strikes+1 {
		t.Fatalf("re-arm after debounce should bump Strikes by exactly 1 (got %d → %d)", first.Strikes, second.Strikes)
	}
	if !second.Until.After(first.Until) {
		t.Fatalf("re-arm did not push Until forward (first=%v, second=%v)", first.Until, second.Until)
	}
}

// TestChattyRoutes_SustainedFloodDoesNotBumpStrikesPerFrame pins
// the throttle contract: once chatty_routes quarantine is armed,
// every subsequent inbound frame inside the same debounce window
// MUST NOT call armRouteQuarantineLocked. Otherwise the protection
// itself becomes a lock+log storm (peerMu Write + zerolog.Warn on
// every frame) and Strikes saturates the duration cap within a
// second of the flood starting.
//
// We drive a flood of (2 * chattyAnnounceThreshold) frames spread
// across one tenth of the debounce window. With the fix: arm fires
// once on the threshold-crossing frame, Strikes stays at 1, Until
// reflects the first base duration. Without the fix: Strikes would
// equal (frames - threshold + 1) and Until would reflect the
// duration cap.
func TestChattyRoutes_SustainedFloodDoesNotBumpStrikesPerFrame(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("flood-without-pause-peer")
	base := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	const floodFrames = 2 * chattyAnnounceThreshold
	// Spread across (debounce / 10) so EVERY frame after the
	// threshold-crossing one is well inside the debounce window
	// from LastArmed.
	step := chattyReArmDebounce / 10 / time.Duration(floodFrames)
	if step <= 0 {
		step = time.Microsecond
	}

	for i := 0; i < floodFrames; i++ {
		svc.recordInboundAnnounceAndMaybeArm(peer, base.Add(time.Duration(i)*step))
	}

	svc.peerMu.RLock()
	entry, ok := svc.peerQuarantine[peer]
	svc.peerMu.RUnlock()

	if !ok {
		t.Fatal("threshold crossing did not arm quarantine")
	}
	if entry.Strikes != 1 {
		t.Fatalf("sustained flood bumped Strikes per frame: got %d, want 1 (throttle missing)", entry.Strikes)
	}
	if entry.Reason != "chatty_routes" {
		t.Fatalf("reason: got %q, want chatty_routes", entry.Reason)
	}
	// Until must reflect base duration (one arm @ strikes=1), not
	// the cap. Sanity-bound: within the half-second precision the
	// loop produces, Until should be base + quarantineBaseDuration
	// ± a few hundred ms.
	expectedUntil := base.Add(quarantineBaseDuration)
	delta := entry.Until.Sub(expectedUntil)
	if delta < -2*time.Second || delta > 2*time.Second {
		t.Fatalf("Until reflects more than one arm: got %v, want approximately %v (delta %v)",
			entry.Until, expectedUntil, delta)
	}
}

// TestChattyRoutes_HistoryBoundedAtThreshold pins the bounded-cap
// contract on peerAnnounceHistory: a sustained flood of N >>
// chattyAnnounceThreshold frames must NOT grow the per-peer slice
// beyond chattyAnnounceThreshold. Without the cap, the slice
// grows to rate * window timestamps and every subsequent record
// call pays O(n) prune + O(n) scan in announceRateExceedsLocked —
// debounce stopped the arm/log storm but the CPU/lock storm on
// the history bookkeeping would remain.
func TestChattyRoutes_HistoryBoundedAtThreshold(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("bounded-history-peer")
	base := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// Drive 10x the threshold tightly inside the window. Without
	// the cap, len(peerAnnounceHistory[peer]) would be 5000+.
	const floodFrames = 10 * chattyAnnounceThreshold
	step := chattyAnnounceWindow / 2 / time.Duration(floodFrames)
	if step <= 0 {
		step = time.Microsecond
	}
	for i := 0; i < floodFrames; i++ {
		svc.recordInboundAnnounceAndMaybeArm(peer, base.Add(time.Duration(i)*step))
	}

	svc.peerMu.RLock()
	histLen := len(svc.peerAnnounceHistory[peer])
	entry, ok := svc.peerQuarantine[peer]
	svc.peerMu.RUnlock()

	if histLen > chattyAnnounceThreshold {
		t.Fatalf("history grew past cap: len=%d, cap=%d", histLen, chattyAnnounceThreshold)
	}
	// Cap is "at most threshold", not "exactly threshold" — flow
	// during arming converges on threshold but never exceeds it.
	if histLen != chattyAnnounceThreshold {
		t.Fatalf("history did not converge to cap: len=%d, want %d", histLen, chattyAnnounceThreshold)
	}
	if !ok {
		t.Fatal("flood did not arm quarantine")
	}
	if entry.Strikes != 1 {
		t.Fatalf("flood bumped strikes despite debounce: got %d, want 1", entry.Strikes)
	}
}

// TestChattyRoutes_AgeoutFrontTrimsSlice pins the prune side of
// the bound: when older events fall out of the window, they are
// trimmed from the FRONT (slice header advances). This keeps the
// per-frame cost O(prune_count) instead of O(n) and ensures a
// peer that goes quiet eventually drops back below threshold.
func TestChattyRoutes_AgeoutFrontTrimsSlice(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("ageout-peer")
	base := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// Phase 1: fill the window with threshold events.
	svc.peerMu.Lock()
	for i := 0; i < chattyAnnounceThreshold; i++ {
		svc.recordPeerAnnounceLocked(peer, base.Add(time.Duration(i)*time.Millisecond))
	}
	full := len(svc.peerAnnounceHistory[peer])
	svc.peerMu.Unlock()

	if full != chattyAnnounceThreshold {
		t.Fatalf("phase 1 fill: len=%d, want %d", full, chattyAnnounceThreshold)
	}

	// Phase 2: advance now well past the window. The next record
	// call should age out all phase-1 entries and leave a single
	// fresh one.
	later := base.Add(chattyAnnounceWindow + time.Hour)
	svc.peerMu.Lock()
	svc.recordPeerAnnounceLocked(peer, later)
	afterAgeOut := len(svc.peerAnnounceHistory[peer])
	exceeds := svc.announceRateExceedsLocked(peer, later)
	svc.peerMu.Unlock()

	if afterAgeOut != 1 {
		t.Fatalf("ageout did not front-trim: len=%d, want 1", afterAgeOut)
	}
	if exceeds {
		t.Fatal("post-ageout peer should not exceed threshold")
	}
}

// TestChattyRoutes_ShouldArmDebouncesUnderActiveQuarantine isolates
// the gate function and proves the debounce decision directly,
// independent of the full record/arm path.
func TestChattyRoutes_ShouldArmDebouncesUnderActiveQuarantine(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	peer := domain.PeerIdentity("debounce-gate-peer")
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	// Pre-seed: peer is over threshold AND already quarantined
	// recently. The gate must return false.
	svc.peerMu.Lock()
	for i := 0; i < chattyAnnounceThreshold; i++ {
		svc.recordPeerAnnounceLocked(peer, now.Add(time.Duration(-i)*time.Millisecond))
	}
	svc.peerQuarantine[peer] = routeQuarantineEntry{
		Until:     now.Add(quarantineBaseDuration),
		LastArmed: now.Add(-time.Second), // very fresh
		Strikes:   1,
		Reason:    "chatty_routes",
	}
	skip := svc.shouldArmChattyLocked(peer, now)
	svc.peerMu.Unlock()

	if skip {
		t.Fatal("shouldArmChattyLocked returned true within debounce window — throttle broken")
	}

	// Move LastArmed past the debounce and re-test — must allow.
	svc.peerMu.Lock()
	entry := svc.peerQuarantine[peer]
	entry.LastArmed = now.Add(-chattyReArmDebounce - time.Second)
	svc.peerQuarantine[peer] = entry
	allow := svc.shouldArmChattyLocked(peer, now)
	svc.peerMu.Unlock()

	if !allow {
		t.Fatal("shouldArmChattyLocked returned false beyond debounce — peer can never re-escalate")
	}
}

// TestChattyRoutes_PurgeRemovesStaleHistory verifies that
// purgePeerAnnounceHistoryLocked drops peer slices whose newest
// entry is older than chattyAnnounceWindow. recordPeerAnnounceLocked
// already prunes on every SAME-peer event, but a peer that goes
// silent forever leaves a stale slice in the outer map — the
// periodic sweep is the only thing that removes it.
func TestChattyRoutes_PurgeRemovesStaleHistory(t *testing.T) {
	t.Parallel()

	svc := newChattyFixture()
	now := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)

	silent := domain.PeerIdentity("went-silent")
	live := domain.PeerIdentity("still-chatty")

	svc.peerMu.Lock()
	svc.peerAnnounceHistory[silent] = []time.Time{
		now.Add(-chattyAnnounceWindow - time.Minute),
	}
	svc.peerAnnounceHistory[live] = []time.Time{
		now.Add(-time.Second),
	}
	svc.purgePeerAnnounceHistoryLocked(now)
	_, silentPresent := svc.peerAnnounceHistory[silent]
	_, livePresent := svc.peerAnnounceHistory[live]
	svc.peerMu.Unlock()

	if silentPresent {
		t.Fatal("silent peer's stale history was not purged")
	}
	if !livePresent {
		t.Fatal("chatty peer's recent history was wrongly purged")
	}
}
