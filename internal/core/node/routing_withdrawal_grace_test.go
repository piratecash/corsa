package node

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// ---------------------------------------------------------------------------
// Route withdrawal grace period tests — Part 1 of the probation +
// quarantine design (see docs/refactoring/route-withdrawal-grace-period.md).
//
// These tests exercise the unit-level state machine directly via the
// helper API (maybeScheduleDeferredWithdrawal /
// tryCancelPendingWithdrawal / firePendingWithdrawal). Integration with
// a real routing.Table is covered by the existing routing tests and
// the smoke-test path — these tests focus on the timer / map / lock
// contract.
//
// effectiveWithdrawalGracePeriod resolution: setting
// routeWithdrawalGracePeriodTest in the fixture to a short positive
// duration both (a) enables the grace path even on a literal &Service{}
// and (b) keeps test runtime small.
// ---------------------------------------------------------------------------

// newGraceFixture returns a minimal Service set up for probation
// tests. routingTable is nil — executeDeferredWithdrawal early-exits
// on that, so the tests can observe the timer lifecycle without
// pulling in a real routing fixture.
func newGraceFixture(t *testing.T, grace time.Duration) *Service {
	t.Helper()
	return &Service{
		identitySessions:               map[domain.PeerIdentity]int{},
		identityRelaySessions:          map[domain.PeerIdentity]int{},
		pendingWithdrawals:             make(map[domain.PeerIdentity]*pendingWithdrawal),
		routeWithdrawalGracePeriodTest: grace,
	}
}

// peerInPendingMap is a tiny helper for the assertion pattern below.
func peerInPendingMap(s *Service, peer domain.PeerIdentity) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	_, ok := s.pendingWithdrawals[peer]
	return ok
}

// TestWithdrawalGrace_DefaultLegacyForBareService pins the
// backward-compat contract: a Service literal with no
// pendingWithdrawals map initialised and no test override must
// behave like the pre-grace path (synchronous inline). This is the
// invariant that keeps every existing routing test passing.
func TestWithdrawalGrace_DefaultLegacyForBareService(t *testing.T) {
	t.Parallel()

	svc := &Service{
		identitySessions:      map[domain.PeerIdentity]int{},
		identityRelaySessions: map[domain.PeerIdentity]int{},
		// pendingWithdrawals: nil
		// routeWithdrawalGracePeriodTest: 0
	}
	if got := svc.effectiveWithdrawalGracePeriod(); got != 0 {
		t.Fatalf("bare &Service{} must report grace=0 (sync legacy); got %v", got)
	}
}

// TestWithdrawalGrace_ProductionDefault verifies that a Service
// with the production-style initialisation (map allocated, no test
// override) gets the production grace constant.
func TestWithdrawalGrace_ProductionDefault(t *testing.T) {
	t.Parallel()

	svc := &Service{
		pendingWithdrawals: make(map[domain.PeerIdentity]*pendingWithdrawal),
	}
	if got := svc.effectiveWithdrawalGracePeriod(); got != routeWithdrawalGracePeriod {
		t.Fatalf("production grace = %v, want %v", got, routeWithdrawalGracePeriod)
	}
}

// TestWithdrawalGrace_PeriodCoversCMRetryBudget pins the contract
// that motivated deriving the grace period from the CM constants:
// the window MUST be strictly longer than the total backoff the
// connection manager spends across its full retry cycle
// (reconnectMaxRetries × backoffDuration), with non-trivial slack
// for the pacer gate and dial latency that each retry incurs AFTER
// its backoff elapses. A previous revision hard-coded 10s against a
// 14s retry cycle — the deferred withdrawal fired before the third
// retry could reconnect, producing the exact seqno-bump/fanout/
// poison/re-add storm the grace period exists to absorb.
func TestWithdrawalGrace_PeriodCoversCMRetryBudget(t *testing.T) {
	t.Parallel()

	budget := cmReconnectRetryBudget()
	if routeWithdrawalGracePeriod <= budget {
		t.Fatalf("grace period %v must exceed CM retry budget %v", routeWithdrawalGracePeriod, budget)
	}
	if slack := routeWithdrawalGracePeriod - budget; slack < 2*time.Second {
		t.Fatalf("grace slack %v too small — pacer gate + dial latency after the last backoff need headroom", slack)
	}
}

// TestWithdrawalGrace_ReconnectAfterThirdBackoffCancels simulates,
// on a compressed timescale, the storm pattern from the field: the
// peer's reconnect succeeds only on the LAST retry of the CM cycle
// (after the full 2+4+8s backoff budget). The grace window — derived
// as budget + slack — must still be open at that point, so the
// reconnect cancels the pending withdrawal and no seqno bump /
// fanout happens. With the old hard-coded 10s grace this scenario
// fired the withdrawal at 10s while the reconnect landed at ~14s.
//
// Timescale: 1s of production time = 10ms of test time (compress
// 100). The relative geometry (reconnect at budget, window at
// budget+slack) is what is under test, not the absolute durations;
// the resulting ~60ms margin between sleep target and window edge
// absorbs scheduler overshoot on loaded CI.
func TestWithdrawalGrace_ReconnectAfterThirdBackoffCancels(t *testing.T) {
	t.Parallel()

	const compress = 100 // 1s -> 10ms
	scaledBudget := cmReconnectRetryBudget() / compress
	scaledGrace := (cmReconnectRetryBudget() + routeWithdrawalGraceSlack) / compress

	svc := newGraceFixture(t, scaledGrace)
	peer := domaintest.ID("test-peer-third-backoff")

	svc.maybeScheduleDeferredWithdrawal(peer, nil)
	if !peerInPendingMap(svc, peer) {
		t.Fatal("schedule should have armed a timer")
	}

	// Reconnect lands after the FULL retry budget has elapsed —
	// the moment the third retry's dial completes.
	time.Sleep(scaledBudget)

	if !svc.tryCancelPendingWithdrawal(peer) {
		t.Fatalf("reconnect at retry-budget mark (%v) fell outside the grace window (%v) — grace no longer covers the CM retry cycle", scaledBudget, scaledGrace)
	}
	if peerInPendingMap(svc, peer) {
		t.Fatal("pending entry should have been removed by tryCancel")
	}
}

// TestWithdrawalGrace_NegativeOverrideDisablesGrace pins the
// explicit-opt-out path: setting the test override to a negative
// duration forces sync legacy behaviour even when the map is
// initialised.
func TestWithdrawalGrace_NegativeOverrideDisablesGrace(t *testing.T) {
	t.Parallel()

	svc := &Service{
		pendingWithdrawals:             make(map[domain.PeerIdentity]*pendingWithdrawal),
		routeWithdrawalGracePeriodTest: -1,
	}
	if got := svc.effectiveWithdrawalGracePeriod(); got != 0 {
		t.Fatalf("negative override must disable grace; got %v", got)
	}
}

// TestWithdrawalGrace_ScheduleArmsTimer verifies that
// maybeScheduleDeferredWithdrawal stores a pending entry when the
// grace period is positive.
func TestWithdrawalGrace_ScheduleArmsTimer(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, time.Hour) // long enough never to fire during the test
	peer := domaintest.ID("test-peer-schedule")

	svc.maybeScheduleDeferredWithdrawal(peer, nil)

	if !peerInPendingMap(svc, peer) {
		t.Fatal("maybeScheduleDeferredWithdrawal should have stored a pending entry")
	}

	// Cleanup — stop the timer to avoid goroutine leak.
	svc.cancelAllPendingWithdrawalsForShutdown()
}

// TestWithdrawalGrace_ReconnectInsideWindowCancels covers the
// happy path: a reconnect inside the grace window cancels the
// timer and the body never runs.
func TestWithdrawalGrace_ReconnectInsideWindowCancels(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, 200*time.Millisecond)
	peer := domaintest.ID("test-peer-reconnect")

	svc.maybeScheduleDeferredWithdrawal(peer, nil)
	if !peerInPendingMap(svc, peer) {
		t.Fatal("schedule should have armed a timer")
	}

	// Reconnect after 30ms — well inside the 200ms grace window.
	time.Sleep(30 * time.Millisecond)

	if !svc.tryCancelPendingWithdrawal(peer) {
		t.Fatal("tryCancelPendingWithdrawal returned false despite an armed timer")
	}
	if peerInPendingMap(svc, peer) {
		t.Fatal("pending entry should have been removed by tryCancel")
	}

	// Wait past the original grace window to ensure no body ran.
	time.Sleep(300 * time.Millisecond)
	if peerInPendingMap(svc, peer) {
		t.Fatal("entry must stay absent — timer was cancelled")
	}
}

// TestWithdrawalGrace_TimerFiresIfNoReconnect covers the timeout
// path: the timer fires after the grace period and the entry is
// removed from the map. executeDeferredWithdrawal early-exits on
// nil routingTable so we can observe the map state without
// dragging in a real routing fixture.
func TestWithdrawalGrace_TimerFiresIfNoReconnect(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, 50*time.Millisecond)
	peer := domaintest.ID("test-peer-timeout")

	svc.maybeScheduleDeferredWithdrawal(peer, nil)
	if !peerInPendingMap(svc, peer) {
		t.Fatal("schedule should have armed a timer")
	}

	// Wait past the grace window.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !peerInPendingMap(svc, peer) {
			return // expected: timer fired, entry removed
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timer did not fire within the deadline; pending entry never cleared")
}

// TestWithdrawalGrace_DoubleScheduleKeepsFirstTimer pins the
// "first close wins" rule. If onPeerSessionClosed is called twice
// for the same identity (race / repeated signal), the original
// timer is kept — restarting it would let a chatty close pattern
// indefinitely defer the withdrawal.
func TestWithdrawalGrace_DoubleScheduleKeepsFirstTimer(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, time.Hour)
	peer := domaintest.ID("test-peer-double-close")

	svc.maybeScheduleDeferredWithdrawal(peer, nil)
	svc.peerMu.RLock()
	first := svc.pendingWithdrawals[peer]
	svc.peerMu.RUnlock()
	if first == nil {
		t.Fatal("first schedule did not arm a timer")
	}

	svc.maybeScheduleDeferredWithdrawal(peer, nil)
	svc.peerMu.RLock()
	second := svc.pendingWithdrawals[peer]
	svc.peerMu.RUnlock()
	if second != first {
		t.Fatal("second schedule replaced the timer; first-wins contract broken")
	}

	svc.cancelAllPendingWithdrawalsForShutdown()
}

// TestWithdrawalGrace_ShutdownCancelsAll verifies the shutdown
// helper stops every armed timer. Without it, late-firing timers
// could fire against a stopped Service and racing with state
// teardown.
func TestWithdrawalGrace_ShutdownCancelsAll(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, time.Hour)
	peers := []domain.PeerIdentity{
		domaintest.ID("p1"), domaintest.ID("p2"), domaintest.ID("p3"), domaintest.ID("p4"),
	}
	for _, p := range peers {
		svc.maybeScheduleDeferredWithdrawal(p, nil)
	}
	for _, p := range peers {
		if !peerInPendingMap(svc, p) {
			t.Fatalf("peer %s missing from pending map after schedule", p)
		}
	}

	svc.cancelAllPendingWithdrawalsForShutdown()

	for _, p := range peers {
		if peerInPendingMap(svc, p) {
			t.Fatalf("peer %s still in pending map after shutdown cancel", p)
		}
	}
}

// TestWithdrawalGrace_CancelOnUnarmedReturnsFalse verifies that
// tryCancelPendingWithdrawal on an identity that has no pending
// timer returns false — telling the caller to take the normal
// AddDirectPeer path.
func TestWithdrawalGrace_CancelOnUnarmedReturnsFalse(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, time.Hour)
	peer := domaintest.ID("test-peer-no-timer")

	if svc.tryCancelPendingWithdrawal(peer) {
		t.Fatal("tryCancelPendingWithdrawal returned true on identity with no armed timer")
	}
}

// TestWithdrawalGrace_CancelReturnsTrueEvenWhenTimerStopRaceLost pins
// the fix for the previous race: tryCancelPendingWithdrawal used to
// inspect timer.Stop()'s return value and, on false, give back false
// to the caller — sending onPeerSessionEstablished through a full
// AddDirectPeer / MarkReconnected / TriggerUpdate cycle for a route
// the timer callback was about to discover it could not remove.
//
// Atomic-claim contract: once tryCancel has DELETED the pending
// entry under peerMu, firePendingWithdrawal's absent-entry guard
// causes the callback to abort. So tryCancel may safely return true
// regardless of Stop's verdict — the cancellation is already
// committed by the delete, not by Stop.
//
// We simulate the race deterministically: arm a timer, manually
// Stop() it once so a subsequent Stop() returns false (Go timer
// semantics), then tryCancel. With the old code the result was
// false; with the fix it must be true. We also assert that
// firePendingWithdrawal, called explicitly to simulate the
// callback running after Stop() returned false, does NOT execute
// the deferred body (no panic against the nil routingTable).
func TestWithdrawalGrace_CancelReturnsTrueEvenWhenTimerStopRaceLost(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, time.Hour)
	peer := domaintest.ID("test-peer-stop-race")

	// Real timer, but with a no-op AfterFunc body: we never let it
	// actually invoke firePendingWithdrawal — the test drives the
	// callback path manually below to keep timing deterministic.
	timer := time.AfterFunc(time.Hour, func() { /* no-op */ })
	// First Stop disarms; the SECOND Stop call inside tryCancel
	// will return false, which is the race window we are pinning.
	if !timer.Stop() {
		t.Fatal("setup: first Stop must succeed on a freshly armed timer")
	}

	svc.peerMu.Lock()
	svc.pendingWithdrawals[peer] = &pendingWithdrawal{timer: timer, caps: nil}
	svc.peerMu.Unlock()

	if !svc.tryCancelPendingWithdrawal(peer) {
		t.Fatal("tryCancelPendingWithdrawal must return true after claiming the entry, even when timer.Stop returns false (race lost)")
	}
	if peerInPendingMap(svc, peer) {
		t.Fatal("tryCancelPendingWithdrawal must delete the entry under peerMu before returning")
	}

	// Simulate the AfterFunc callback running after Stop returned
	// false. The absent-entry guard must abort it before
	// executeDeferredWithdrawal touches state. nil routingTable is
	// fine — guard fires earlier; this also gives a clear failure
	// signal if the guard were broken (executeDeferredWithdrawal
	// would still nil-check routingTable, but the log path differs).
	svc.firePendingWithdrawal(peer, nil)
}

// TestWithdrawalGrace_LegacySyncFiresInline verifies the legacy
// path: with grace disabled (negative override) the schedule call
// runs the body inline — i.e. control returns AFTER the deferred
// body has executed. We assert this by checking that no pending
// entry is left.
func TestWithdrawalGrace_LegacySyncFiresInline(t *testing.T) {
	t.Parallel()

	svc := &Service{
		identitySessions:               map[domain.PeerIdentity]int{},
		identityRelaySessions:          map[domain.PeerIdentity]int{},
		pendingWithdrawals:             make(map[domain.PeerIdentity]*pendingWithdrawal),
		routeWithdrawalGracePeriodTest: -1, // explicit disable
	}
	peer := domaintest.ID("test-peer-legacy-sync")

	svc.maybeScheduleDeferredWithdrawal(peer, nil)

	if peerInPendingMap(svc, peer) {
		t.Fatal("legacy sync path should NOT leave a pending entry — body must have run inline")
	}
}

// TestWithdrawalGrace_TimerCallbackAbortedAfterShutdownClaim pins the
// shutdown-race contract: if cancelAllPendingWithdrawalsForShutdown
// deletes the pending entry between the AfterFunc firing and the
// callback acquiring peerMu, the callback MUST abort without running
// executeDeferredWithdrawal. Otherwise the callback would perform
// fanout/trigger/poison against a Service whose subsystems are
// already tearing down.
//
// The test uses a REAL routing.Table so the failure mode is
// directly observable: without the atomic claim the callback
// proceeds into executeDeferredWithdrawal, the relay-session
// re-check (0 sessions) does NOT abort it, RemoveDirectPeer fires,
// and the direct route disappears from the table. With the atomic
// claim the callback exits before touching the table and the route
// stays. A previous version of this test only checked a Service
// with a nil routingTable, which would early-return inside
// executeDeferredWithdrawal regardless of the atomic claim — that
// "test" passed even when the claim was deliberately removed.
func TestWithdrawalGrace_TimerCallbackAbortedAfterShutdownClaim(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	peer := domaintest.ID("peer-shutdown-race")
	if _, err := table.AddDirectPeer(peer); err != nil {
		t.Fatalf("setup: AddDirectPeer: %v", err)
	}
	if got := table.Lookup(peer); len(got) == 0 {
		t.Fatal("setup: direct route not present after AddDirectPeer")
	}

	svc := &Service{
		routingTable:                   table,
		pendingWithdrawals:             make(map[domain.PeerIdentity]*pendingWithdrawal),
		identitySessions:               map[domain.PeerIdentity]int{},
		identityRelaySessions:          map[domain.PeerIdentity]int{},
		routeWithdrawalGracePeriodTest: time.Hour,
	}
	peerID := peer

	// Schedule pending entry (long grace so the timer never fires
	// during the test on its own).
	svc.maybeScheduleDeferredWithdrawal(peerID, nil)
	if !peerInPendingMap(svc, peerID) {
		t.Fatal("schedule did not arm timer")
	}

	// Simulate shutdown stealing the entry first (this is what
	// happens when cancelAllPendingWithdrawalsForShutdown wins the
	// race against the AlreadyFiring timer).
	svc.cancelAllPendingWithdrawalsForShutdown()
	if peerInPendingMap(svc, peerID) {
		t.Fatal("cancelAll did not remove pending entry")
	}

	// Now run the callback as if its AfterFunc invocation was
	// already scheduled when shutdown grabbed peerMu. With the
	// atomic claim, firePendingWithdrawal sees the entry absent
	// and returns immediately. Without the claim, it would proceed
	// into executeDeferredWithdrawal, find identityRelaySessions[peer]
	// == 0 (no abort), and call RemoveDirectPeer.
	svc.firePendingWithdrawal(peerID, nil)

	if got := table.Lookup(peer); len(got) == 0 {
		t.Fatal("route was removed despite shutdown cancellation — atomic claim missing or broken")
	}
}

// TestWithdrawalGrace_ConcurrentScheduleCancelSafe is a -race
// probe for the lock-contention pattern. Multiple goroutines
// schedule and cancel pending withdrawals for the same identity.
// No invariant is asserted other than "no panic, no race
// detector trip" — the lock discipline inside the helpers is the
// real product.
func TestWithdrawalGrace_ConcurrentScheduleCancelSafe(t *testing.T) {
	t.Parallel()

	svc := newGraceFixture(t, time.Hour)
	peer := domaintest.ID("test-peer-concurrent")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			svc.maybeScheduleDeferredWithdrawal(peer, nil)
		}()
		go func() {
			defer wg.Done()
			_ = svc.tryCancelPendingWithdrawal(peer)
		}()
	}
	wg.Wait()

	svc.cancelAllPendingWithdrawalsForShutdown()
}

// TestWithdrawalGrace_ReconnectClearsBlackHoleCooldown pins the
// node-level integration of Table.ClearDirectPairCooldown: a peer
// that goes down, accumulates BlackHoleThreshold hop-ack failures
// during the grace window (the route is still in the table and
// still selectable, so relay attempts keep timing out against the
// dead session), and reconnects INSIDE the window must be
// immediately selectable again. Before the fix the grace branch of
// onPeerSessionEstablished returned without touching reputation
// state, leaving the still-present direct route hidden from
// Lookup / fetchRouteLookup for up to BlackHoleCooldown — the
// "peer online, route count=0" field bug.
func TestWithdrawalGrace_ReconnectClearsBlackHoleCooldown(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRouting(t, idNodeA)
	svc.pendingWithdrawals = make(map[domain.PeerIdentity]*pendingWithdrawal)
	// Long grace so the deferred withdrawal cannot fire mid-test.
	svc.routeWithdrawalGracePeriodTest = time.Hour
	caps := []domain.Capability{domain.CapMeshRelayV1}

	// Session up → direct route present and selectable.
	svc.onPeerSessionEstablished(idPeerB, caps)
	if got := svc.routingTable.Lookup(idPeerB); len(got) != 1 {
		t.Fatalf("precondition: direct route missing after establish; Lookup=%d", len(got))
	}

	// Session down → deferred withdrawal armed, route stays in table.
	svc.onPeerSessionClosed(idPeerB, caps)
	if !peerInPendingMap(svc, idPeerB) {
		t.Fatal("withdrawal grace timer should be armed after close")
	}

	// Relay traffic toward the dead peer keeps timing out and arms
	// the black-hole cooldown for the (peer, peer) direct pair.
	for i := 0; i < routing.BlackHoleThreshold; i++ {
		svc.routingTable.MarkHopFailure(idPeerB, idPeerB)
	}
	if got := svc.routingTable.Lookup(idPeerB); len(got) != 0 {
		t.Fatalf("precondition: cooldown must hide the direct route; Lookup=%d", len(got))
	}

	// Reconnect inside the window: withdrawal cancelled AND cooldown
	// lifted — the route must be selectable immediately, not after
	// BlackHoleCooldown.
	svc.onPeerSessionEstablished(idPeerB, caps)
	if peerInPendingMap(svc, idPeerB) {
		t.Fatal("pending withdrawal should have been cancelled by the reconnect")
	}
	if got := svc.routingTable.Lookup(idPeerB); len(got) != 1 {
		t.Fatalf("direct route must be selectable right after grace reconnect; Lookup=%d", len(got))
	}

	svc.cancelAllPendingWithdrawalsForShutdown()
}
