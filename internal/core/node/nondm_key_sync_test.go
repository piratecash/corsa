package node

import (
	"fmt"
	"testing"
	"time"
)

// nondm_key_sync_test.go pins the budget of the key sync a non-DM message of
// an unknown author triggers. That trigger is fed by attacker-chosen author
// names, so it must not become a work generator (dials to honest neighbours),
// and it must not take the slots the keyless-DM recovery depends on.

// fakeKeySyncClock is a settable clock for the limiter.
type fakeKeySyncClock struct{ now time.Time }

func (c *fakeKeySyncClock) read() time.Time { return c.now }

func newTestNonDMLimiter() (*nonDMKeySyncLimiter, *fakeKeySyncClock) {
	clock := &fakeKeySyncClock{now: time.Unix(1780000000, 0)}
	return newNonDMKeySyncLimiter(clock.read), clock
}

// TestNonDMKeySyncLimiterBudgets walks every gate of the limiter on a fake
// clock: one pass at a time, one start per global interval, one start per hop
// per hop cooldown.
func TestNonDMKeySyncLimiterBudgets(t *testing.T) {
	t.Parallel()
	limiter, clock := newTestNonDMLimiter()
	// Production records every arrival (nonDMAuthorAdmitted →
	// noteNonDMAttribution) before it asks for a pass; a hop never noted is
	// untracked and buys none.
	limiter.noteLocked("hop-a", false)
	limiter.noteLocked("hop-b", false)

	if got := limiter.admitLocked("hop-a", fabricatedAuthor(1)); got != nonDMKeySyncAdmitted {
		t.Fatalf("first pass = %v, want admitted", got)
	}
	if got := limiter.admitLocked("hop-b", fabricatedAuthor(2)); got != nonDMKeySyncBusy {
		t.Fatalf("second concurrent pass = %v, want busy", got)
	}
	limiter.releaseLocked(fabricatedAuthor(1))

	if got := limiter.admitLocked("hop-b", fabricatedAuthor(2)); got != nonDMKeySyncGlobalPacing {
		t.Fatalf("pass inside the global interval = %v, want global pacing", got)
	}
	clock.now = clock.now.Add(nonDMKeySyncMinInterval)
	if got := limiter.admitLocked("hop-a", fabricatedAuthor(3)); got != nonDMKeySyncHopCoolingDown {
		t.Fatalf("same hop inside its cooldown = %v, want hop cooldown", got)
	}
	if got := limiter.admitLocked("hop-b", fabricatedAuthor(2)); got != nonDMKeySyncAdmitted {
		t.Fatalf("another hop after the global interval = %v, want admitted", got)
	}
}

// TestNonDMKeySyncLimiterSuppressesAHopOfMostlyUnknownAuthors: a hop whose
// non-DM traffic is mostly unattributable stops buying sync passes for the
// suppression period, in silence; a hop whose traffic is mostly known authors
// keeps them.
func TestNonDMKeySyncLimiterSuppressesAHopOfMostlyUnknownAuthors(t *testing.T) {
	t.Parallel()
	limiter, clock := newTestNonDMLimiter()

	for i := 0; i < nonDMAttributionMinSample; i++ {
		limiter.noteLocked("noisy-hop", false)
	}
	for i := 0; i < 3*nonDMAttributionMinSample; i++ {
		limiter.noteLocked("honest-hop", true)
	}
	limiter.noteLocked("honest-hop", false)

	clock.now = clock.now.Add(nonDMKeySyncHopCooldown)
	if got := limiter.admitLocked("noisy-hop", fabricatedAuthor(1)); got != nonDMKeySyncHopSuppressed {
		t.Fatalf("noisy hop = %v, want suppressed", got)
	}
	if got := limiter.admitLocked("honest-hop", fabricatedAuthor(2)); got != nonDMKeySyncAdmitted {
		t.Fatalf("honest hop = %v, want admitted", got)
	}
	limiter.releaseLocked(fabricatedAuthor(2))

	clock.now = clock.now.Add(nonDMHopSuppression)
	if got := limiter.admitLocked("noisy-hop", fabricatedAuthor(3)); got != nonDMKeySyncAdmitted {
		t.Fatalf("noisy hop after the suppression period = %v, want admitted", got)
	}
}

// TestNonDMFloodFromFabricatedAuthorsStartsBoundedSyncPasses: one session
// pushes 1 000 non-DM messages from 1 000 distinct fabricated authors inside
// one global interval. Before the budget every author bought its own pass —
// up to 1+senderKeySyncFanout fresh dials each. Now at most ONE pass starts
// (so at most 1+senderKeySyncFanout fetch_contacts to third nodes), and the
// session is not banned.
func TestNonDMFloodFromFabricatedAuthorsStartsBoundedSyncPasses(t *testing.T) {
	t.Parallel()
	svc, _, connID := newRoutableDatagramInboundFixture(t)
	clock := &fakeKeySyncClock{now: time.Unix(1780000000, 0)}
	svc.senderKeySyncMu.Lock()
	svc.nonDMKeySync = newNonDMKeySyncLimiter(clock.read)
	svc.senderKeySyncMu.Unlock()

	const messages = 1000
	for i := 0; i < messages; i++ {
		svc.handleInboundPushMessage(connID, nonDMPush(fabricatedAuthor(i), fmt.Sprintf("flood-%d", i)))
	}

	if got := svc.nonDMKeySyncPasses.Load(); got > 1 {
		t.Fatalf("non-DM key sync passes = %d inside one global interval, want ≤ 1", got)
	}
	if got := svc.nonDMKeySyncSkipped.Load(); got < messages-1 {
		t.Fatalf("skipped non-DM key sync triggers = %d, want ≥ %d", got, messages-1)
	}
	if got := banScoreForIP(svc, datagramTestPeerIP); got != 0 {
		t.Fatalf("ban score = %d for relaying non-DM traffic", got)
	}
}

// TestNonDMKeySyncLimiterTrackedHopsStayBoundedWithoutDroppingSuppression:
// 1 025 distinct hops arrive within one attribution window on a frozen clock,
// so no entry has lapsed and pruning frees nothing. The map must stay at its
// cap rather than grow past it; an already suppressed hop must stay
// suppressed; the hop that found no room buys no pass, because its
// attribution — the only thing that could suppress it — is not tracked; and
// once pruning has found nothing to drop, the next arrivals must not rescan
// the full map under senderKeySyncMu before an entry can actually lapse.
func TestNonDMKeySyncLimiterTrackedHopsStayBoundedWithoutDroppingSuppression(t *testing.T) {
	t.Parallel()
	limiter, clock := newTestNonDMLimiter()

	for i := 0; i < nonDMAttributionMinSample; i++ {
		limiter.noteLocked("noisy-hop", false)
	}
	for i := 1; i < maxNonDMKeySyncTrackedHops; i++ {
		limiter.noteLocked(fmt.Sprintf("hop-%d", i), true)
	}
	if got := len(limiter.hops); got != maxNonDMKeySyncTrackedHops {
		t.Fatalf("tracked hops = %d, want the cap %d", got, maxNonDMKeySyncTrackedHops)
	}

	limiter.noteLocked("overflow-hop", false)

	if got := len(limiter.hops); got > maxNonDMKeySyncTrackedHops {
		t.Fatalf("tracked hops = %d after a 1 025th distinct hop, want ≤ %d", got, maxNonDMKeySyncTrackedHops)
	}
	if !limiter.hopSuppressedLocked("noisy-hop", clock.now) {
		t.Fatal("an active suppression was dropped to make room for a new hop")
	}
	if got := limiter.admitLocked("overflow-hop", fabricatedAuthor(1)); got != nonDMKeySyncHopUntracked {
		t.Fatalf("pass for a hop whose attribution is not tracked = %v, want %v", got, nonDMKeySyncHopUntracked)
	}
	wantNextPrune := clock.now.Add(nonDMAttributionWindow)
	if !limiter.nextPruneAt.Equal(wantNextPrune) {
		t.Fatalf("next prune at %v, want the earliest lapse %v", limiter.nextPruneAt, wantNextPrune)
	}

	clock.now = wantNextPrune
	limiter.noteLocked("late-hop", true)
	if _, tracked := limiter.hops["late-hop"]; !tracked {
		t.Fatal("a new hop is still refused after the window lapsed and pruning could free room")
	}
	if !limiter.hopSuppressedLocked("noisy-hop", clock.now) {
		t.Fatal("pruning after the window dropped a hop whose suppression is still running")
	}
}
