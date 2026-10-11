package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// announce_ratelimit_test.go pins the Phase 4 13.7 announce-plane
// per-peer rate limit contract: burst allows up to N route-cost
// tokens (Round-10: route-count budgeting), refill replenishes at
// the configured rate, an empty bucket drops the next request, and
// cleanup removes long-idle buckets. Frame cost is route-entry count
// (min 1) — see announceCostForEntries for the helper used by the
// production receive handlers.
//
// Every limiter here runs on a hand-driven clock that only the test moves.
// The bucket refills from the injected clock alone, so a drain of 10,000
// calls sees no refill however long it takes — on the wall clock the race
// detector slows that drain enough to refill dozens of tokens, and the
// "exhausted" preconditions below stopped holding.

// newTestAnnounceLimiter returns a limiter on a manual clock together with
// that clock. A test that never advances it sees no refill at all.
func newTestAnnounceLimiter() (*announceRateLimiter, *manualTestClock) {
	clock := newManualTestClock()
	return newAnnounceRateLimiter(clock.now), clock
}

func TestAnnounceRateLimiter_AllowsUpToBurstThenThrottles(t *testing.T) {
	rl, _ := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	// Drain the bucket at unit cost — same shape a stream of
	// request_resync / poison / empty announce frames would produce.
	for i := 0; i < announceBurstRoutesPerPeer; i++ {
		if !rl.allow(peer, 1) {
			t.Fatalf("burst slot %d/%d rejected — limiter must allow up to burst", i+1, announceBurstRoutesPerPeer)
		}
	}
	if rl.allow(peer, 1) {
		t.Fatal("burst exhausted — next allow must be false until refill")
	}
}

func TestAnnounceRateLimiter_EmptyIdentityAccepts(t *testing.T) {
	// Defence-in-depth: receive handlers reject empty senders
	// upstream; the limiter does not block on empty identity so the
	// validation gate's malformed-input signal stays distinct.
	rl, _ := newTestAnnounceLimiter()
	for i := 0; i < announceBurstRoutesPerPeer+5; i++ {
		if !rl.allow(penaltySubject{}, 1) {
			t.Fatalf("empty identity must always pass the limiter; failed at %d", i)
		}
	}
}

func TestAnnounceRateLimiter_PerPeerIsolation(t *testing.T) {
	// Two peers consume independent buckets; exhausting one must
	// NOT affect the other.
	rl, _ := newTestAnnounceLimiter()
	a := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	b := provenIdentitySubject(domain.PeerIdentityFromWire("bb00000000000000000000000000000000000002"))
	for i := 0; i < announceBurstRoutesPerPeer; i++ {
		rl.allow(a, 1)
	}
	if rl.allow(a, 1) {
		t.Fatal("peer a must be exhausted")
	}
	if !rl.allow(b, 1) {
		t.Fatal("peer b must NOT be affected by peer a's exhaustion")
	}
}

func TestAnnounceRateLimiter_CleanupRemovesStaleBuckets(t *testing.T) {
	rl, clock := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	rl.allow(peer, 1)

	clock.advance(time.Hour - time.Second)
	rl.cleanup(time.Hour)
	if !announceLimiterHasBucket(rl, peer) {
		t.Fatal("cleanup must keep a bucket used inside maxAge")
	}

	clock.advance(2 * time.Second)
	rl.cleanup(time.Hour)
	if announceLimiterHasBucket(rl, peer) {
		t.Fatal("cleanup must remove a bucket idle for longer than maxAge")
	}
}

func announceLimiterHasBucket(rl *announceRateLimiter, subject penaltySubject) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	_, ok := rl.buckets[subject]
	return ok
}

func TestAnnounceRateLimiter_RefillRestoresCapacityOverTime(t *testing.T) {
	rl, clock := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	if !rl.allow(peer, announceBurstRoutesPerPeer) {
		t.Fatal("precondition: a fresh bucket holds the whole burst")
	}
	if rl.allow(peer, 1) {
		t.Fatal("precondition: bucket exhausted")
	}

	// One refill period yields exactly one token: the first charge passes,
	// the second finds the bucket empty again.
	clock.advance(time.Second / announceRefillRoutesPerSec)
	if !rl.allow(peer, 1) {
		t.Fatal("one refill period must restore one token")
	}
	if rl.allow(peer, 1) {
		t.Fatal("one refill period must restore exactly one token, not more")
	}

	// A long idle period refills to the burst ceiling and no further.
	clock.advance(time.Hour)
	if !rl.allow(peer, announceBurstRoutesPerPeer) {
		t.Fatal("a long idle period must refill the bucket to the full burst")
	}
	if rl.allow(peer, 1) {
		t.Fatal("refill must be capped at the burst")
	}
}

// TestAnnounceRateLimiter_LargeFrameDrainsByEntryCount pins the
// Round-10 fix: a single announce frame carrying N routes consumes
// N tokens (not 1), so the per-peer bound matches the per-entry
// trust-classification work the receive path does. Before the fix
// the limiter counted by frame and a legitimate chunked full-sync
// of >3000 routes was silently truncated past frame 30.
func TestAnnounceRateLimiter_LargeFrameDrainsByEntryCount(t *testing.T) {
	rl, _ := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	// A single 100-route frame must consume exactly 100 tokens.
	if !rl.allow(peer, 100) {
		t.Fatal("100-route frame against full burst must pass")
	}
	rl.mu.Lock()
	got := rl.buckets[peer].tokens
	rl.mu.Unlock()
	if want := float64(announceBurstRoutesPerPeer - 100); got != want {
		t.Fatalf("after 100-cost allow, tokens = %v, want %v", got, want)
	}
}

// TestAnnounceRateLimiter_FullSyncOfFullBurstFitsExactly pins the
// upper-edge: a full-sync that consumes exactly the configured burst
// budget passes in one shot. This is the case the Round-10 fix
// preserves — the previous per-frame budget would have dropped any
// sync past ~3000 routes silently.
func TestAnnounceRateLimiter_FullSyncOfFullBurstFitsExactly(t *testing.T) {
	rl, _ := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	// Spend the whole burst in one allow call.
	if !rl.allow(peer, announceBurstRoutesPerPeer) {
		t.Fatalf("burst-sized single-frame full-sync must pass; budget %d", announceBurstRoutesPerPeer)
	}
	// One more unit-cost charge must fail — bucket is exactly empty.
	if rl.allow(peer, 1) {
		t.Fatal("post-burst single-token allow must be throttled")
	}
}

// TestAnnounceRateLimiter_OverBurstFrameRejectedWholesale pins the
// all-or-nothing reservation semantic: a frame that demands more
// tokens than the bucket holds is rejected outright; the bucket is
// NOT partially drained. Without this guarantee a slow attacker
// could send sequentially-larger frames to drip-drain the bucket
// without ever delivering a full frame.
func TestAnnounceRateLimiter_OverBurstFrameRejectedWholesale(t *testing.T) {
	rl, _ := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	// Demand more than the burst — must reject without touching
	// tokens. (Counting from a fresh bucket so tokens == burst.)
	if rl.allow(peer, announceBurstRoutesPerPeer+1) {
		t.Fatal("frame demanding more than burst must be rejected")
	}
	rl.mu.Lock()
	got := rl.buckets[peer].tokens
	rl.mu.Unlock()
	if got != float64(announceBurstRoutesPerPeer) {
		t.Fatalf("rejected frame must not partially drain bucket; tokens = %v, want %d", got, announceBurstRoutesPerPeer)
	}
}

// TestAnnounceRateLimiter_NegativeCostClampedToOne pins the
// defensive clamp: a caller that passes 0 or negative cost still
// charges 1 token, so a buggy helper can never bypass the limiter.
func TestAnnounceRateLimiter_NegativeCostClampedToOne(t *testing.T) {
	rl, _ := newTestAnnounceLimiter()
	peer := provenIdentitySubject(domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001"))
	if !rl.allow(peer, 0) {
		t.Fatal("cost=0 must be accepted (clamped to 1)")
	}
	rl.mu.Lock()
	gotZero := rl.buckets[peer].tokens
	rl.mu.Unlock()
	if !rl.allow(peer, -42) {
		t.Fatal("cost<0 must be accepted (clamped to 1)")
	}
	rl.mu.Lock()
	gotNeg := rl.buckets[peer].tokens
	rl.mu.Unlock()
	if want := float64(announceBurstRoutesPerPeer - 1); gotZero != want {
		t.Fatalf("cost=0 must drain exactly 1 token; tokens = %v, want %v", gotZero, want)
	}
	if want := float64(announceBurstRoutesPerPeer - 2); gotNeg != want {
		t.Fatalf("cost<0 must drain exactly 1 token; tokens = %v, want %v", gotNeg, want)
	}
}

// TestAnnounceCostForEntries_HelperContract pins the helper used by
// the production call sites: 0 → 1 (charge for the frame itself),
// n → n for n >= 1 (per-entry work).
func TestAnnounceCostForEntries_HelperContract(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{-1, 1},
		{0, 1},
		{1, 1},
		{42, 42},
		{maxRoutesPerAnnounceFrame, maxRoutesPerAnnounceFrame},
	}
	for _, c := range cases {
		if got := announceCostForEntries(c.in); got != c.want {
			t.Errorf("announceCostForEntries(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
