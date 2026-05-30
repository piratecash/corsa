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

func TestAnnounceRateLimiter_AllowsUpToBurstThenThrottles(t *testing.T) {
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
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
	rl := newAnnounceRateLimiter()
	for i := 0; i < announceBurstRoutesPerPeer+5; i++ {
		if !rl.allow("", 1) {
			t.Fatalf("empty identity must always pass the limiter; failed at %d", i)
		}
	}
}

func TestAnnounceRateLimiter_PerPeerIsolation(t *testing.T) {
	// Two peers consume independent buckets; exhausting one must
	// NOT affect the other.
	rl := newAnnounceRateLimiter()
	a := domain.PeerIdentity("aa00000000000000000000000000000000000001")
	b := domain.PeerIdentity("bb00000000000000000000000000000000000002")
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
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
	rl.allow(peer, 1)
	// Force the bucket's lastRefill into the past so cleanup considers
	// it stale.
	rl.mu.Lock()
	rl.buckets[peer].lastRefill = time.Now().Add(-2 * time.Hour)
	rl.mu.Unlock()
	rl.cleanup(time.Hour)
	rl.mu.Lock()
	_, ok := rl.buckets[peer]
	rl.mu.Unlock()
	if ok {
		t.Fatal("cleanup must remove stale bucket")
	}
}

func TestAnnounceRateLimiter_RefillRestoresCapacityOverTime(t *testing.T) {
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
	for i := 0; i < announceBurstRoutesPerPeer; i++ {
		rl.allow(peer, 1)
	}
	if rl.allow(peer, 1) {
		t.Fatal("precondition: bucket exhausted")
	}
	// Advance the bucket's lastRefill backward by an amount that
	// should produce at least 2 fresh tokens at the configured
	// refill rate. (2 / announceRefillRoutesPerSec seconds.)
	advance := time.Duration(2.5/announceRefillRoutesPerSec*float64(time.Second.Nanoseconds())) * time.Nanosecond
	rl.mu.Lock()
	rl.buckets[peer].lastRefill = rl.buckets[peer].lastRefill.Add(-advance)
	rl.mu.Unlock()

	if !rl.allow(peer, 1) {
		t.Fatal("refill should produce at least one token after the advance")
	}
}

// TestAnnounceRateLimiter_LargeFrameDrainsByEntryCount pins the
// Round-10 fix: a single announce frame carrying N routes consumes
// N tokens (not 1), so the per-peer bound matches the per-entry
// trust-classification work the receive path does. Before the fix
// the limiter counted by frame and a legitimate chunked full-sync
// of >3000 routes was silently truncated past frame 30.
func TestAnnounceRateLimiter_LargeFrameDrainsByEntryCount(t *testing.T) {
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
	// A single 100-route frame must consume exactly 100 tokens.
	if !rl.allow(peer, 100) {
		t.Fatal("100-route frame against full burst must pass")
	}
	rl.mu.Lock()
	got := rl.buckets[peer].tokens
	rl.mu.Unlock()
	want := float64(announceBurstRoutesPerPeer - 100)
	// Allow tiny float drift from the elapsed-since-creation refill.
	if got < want-1 || got > want+1 {
		t.Fatalf("after 100-cost allow, tokens = %v, want ~%v", got, want)
	}
}

// TestAnnounceRateLimiter_FullSyncOfFullBurstFitsExactly pins the
// upper-edge: a full-sync that consumes exactly the configured burst
// budget passes in one shot. This is the case the Round-10 fix
// preserves — the previous per-frame budget would have dropped any
// sync past ~3000 routes silently.
func TestAnnounceRateLimiter_FullSyncOfFullBurstFitsExactly(t *testing.T) {
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
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
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
	// Demand more than the burst — must reject without touching
	// tokens. (Counting from a fresh bucket so tokens == burst.)
	if rl.allow(peer, announceBurstRoutesPerPeer+1) {
		t.Fatal("frame demanding more than burst must be rejected")
	}
	rl.mu.Lock()
	got := rl.buckets[peer].tokens
	rl.mu.Unlock()
	if got < float64(announceBurstRoutesPerPeer)-1 {
		t.Fatalf("rejected frame must not partially drain bucket; tokens = %v, want ~%d", got, announceBurstRoutesPerPeer)
	}
}

// TestAnnounceRateLimiter_NegativeCostClampedToOne pins the
// defensive clamp: a caller that passes 0 or negative cost still
// charges 1 token, so a buggy helper can never bypass the limiter.
func TestAnnounceRateLimiter_NegativeCostClampedToOne(t *testing.T) {
	rl := newAnnounceRateLimiter()
	peer := domain.PeerIdentity("aa00000000000000000000000000000000000001")
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
	// Each "clamped" allow drained exactly 1 token; expect the two
	// calls produced two distinct decrements (modulo float drift).
	if gotZero <= gotNeg {
		t.Fatalf("clamp must drain 1 token per call; tokens after 0-cost = %v, after -42-cost = %v", gotZero, gotNeg)
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
