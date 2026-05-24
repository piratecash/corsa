package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// seedHealthyPeerSession installs a session under addr-<identity>
// with the given capability set AND a connected, non-stalled
// peerHealth entry. Used by query-sender tests where the
// candidate-discovery path (peersWithRouteQueryCap /
// hasAnyPeerWithRouteQueryCap) now requires both capability and
// sendable-health gates to align with the actual send path
// through peerSendableConnectionsLocked (PR 11.21 P2#2). A bare
// session without health would otherwise be filtered out during
// candidate discovery and the test would not see the peer at all.
func seedHealthyPeerSession(t *testing.T, svc *Service, addr domain.PeerAddress, identity domain.PeerIdentity, caps []domain.Capability) {
	t.Helper()
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: identity,
		capabilities: caps,
	}
	if svc.health == nil {
		svc.health = make(map[domain.PeerAddress]*peerHealth)
	}
	svc.health[addr] = &peerHealth{
		Address:         addr,
		Connected:       true,
		Direction:       peerDirectionOutbound,
		LastConnectedAt: time.Now().UTC(),
		LastPongAt:      time.Now().UTC(),
	}
}

// routing_query_sender_test.go tests the Phase 2 PR 11.4c sender-side
// machinery — queryRateLimit and Service.SendRouteQuery. The
// receive-side handlers are covered in routing_query_test.go.

// --- queryRateLimit ---

// TestQueryRateLimit_AllowsUpToLimitInWindow — basic capacity
// check: limit emissions fit in the budget, the (limit+1)-th is
// rejected within the same window.
func TestQueryRateLimit_AllowsUpToLimitInWindow(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := now
	r := newQueryRateLimit(func() time.Time { return clk }, 30*time.Second, 3)

	for i := 0; i < 3; i++ {
		if !r.HasCapacity("id-target") {
			t.Fatalf("HasCapacity returned false at i=%d, want true (budget 3)", i)
		}
		if !r.Record("id-target") {
			t.Fatalf("Record returned false at i=%d, want true (budget 3)", i)
		}
	}
	// Fourth attempt within the same window must be rejected.
	if r.HasCapacity("id-target") {
		t.Fatal("HasCapacity returned true after 3 records, want false")
	}
	if r.Record("id-target") {
		t.Fatal("Record returned true after 3 records, want false (budget exhausted)")
	}
}

// TestQueryRateLimit_SlidingWindowReclaimsBudget — once the
// window has fully elapsed, the budget resets. This is what makes
// the limit a "rate" rather than a hard count.
func TestQueryRateLimit_SlidingWindowReclaimsBudget(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := now
	r := newQueryRateLimit(func() time.Time { return clk }, 30*time.Second, 3)

	// Fill the budget.
	for i := 0; i < 3; i++ {
		r.Record("id-target")
	}
	if r.HasCapacity("id-target") {
		t.Fatal("setup: budget should be full")
	}

	// Step past the window.
	clk = now.Add(31 * time.Second)
	if !r.HasCapacity("id-target") {
		t.Fatal("HasCapacity returned false after window expired, want true (budget reclaimed)")
	}
	if r.PendingCount("id-target") != 0 {
		t.Fatalf("PendingCount = %d after window expired, want 0", r.PendingCount("id-target"))
	}
}

// TestQueryRateLimit_SlidingWindowReclaimsPartialBudget — when
// only some of the stamps have aged out, only that many slots are
// reclaimed.
func TestQueryRateLimit_SlidingWindowReclaimsPartialBudget(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := now
	r := newQueryRateLimit(func() time.Time { return clk }, 30*time.Second, 3)

	r.Record("id-target") // t=0
	clk = now.Add(10 * time.Second)
	r.Record("id-target") // t=10
	clk = now.Add(20 * time.Second)
	r.Record("id-target") // t=20

	if r.PendingCount("id-target") != 3 {
		t.Fatalf("setup: PendingCount = %d, want 3", r.PendingCount("id-target"))
	}

	// Advance past the first stamp (t=0 + 30s = t=30s).
	// The first stamp at t=0 falls outside the window because
	// `before(cutoff = now - window = t=31s - 30s = t=1s)` and
	// t=0 < t=1s. Two stamps remain (t=10, t=20).
	clk = now.Add(31 * time.Second)
	if got := r.PendingCount("id-target"); got != 2 {
		t.Fatalf("PendingCount = %d after partial window pass, want 2", got)
	}
	if !r.HasCapacity("id-target") {
		t.Fatal("HasCapacity false after partial reclaim, want true")
	}
}

// TestQueryRateLimit_PerTargetIsolation — budget is per-target,
// not global. Filling the budget for target A must not affect
// target B.
func TestQueryRateLimit_PerTargetIsolation(t *testing.T) {
	r := newQueryRateLimit(nil, 30*time.Second, 3)

	for i := 0; i < 3; i++ {
		r.Record("id-target-A")
	}
	if !r.HasCapacity("id-target-B") {
		t.Fatal("HasCapacity for target B returned false; budget must be per-target, not global")
	}
}

// TestQueryRateLimit_DefaultsApplied — passing zero/nil values to
// newQueryRateLimit applies production defaults.
func TestQueryRateLimit_DefaultsApplied(t *testing.T) {
	r := newQueryRateLimit(nil, 0, 0)
	if r.window != queryRateWindow {
		t.Fatalf("window = %v, want %v (production default)", r.window, queryRateWindow)
	}
	if r.limit != queryFanOutLimit {
		t.Fatalf("limit = %d, want %d (production default)", r.limit, queryFanOutLimit)
	}
}

// --- Service.SendRouteQuery ---

// TestSendRouteQuery_NoCapablePeersReturnsZero — defensive contract:
// when no directly connected peer advertises
// mesh_route_query_v1, the fan-out returns 0 and consumes no rate
// budget.
func TestSendRouteQuery_NoCapablePeersReturnsZero(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, 3)

	sent := svc.SendRouteQuery(idTargetX)
	if sent != 0 {
		t.Fatalf("SendRouteQuery returned %d, want 0 (no capable peers)", sent)
	}
	if got := svc.queryRateLimit.PendingCount(idTargetX); got != 0 {
		t.Fatalf("PendingCount = %d, want 0 (no-peers fail must not consume budget)", got)
	}
}

// TestSendRouteQuery_RateLimitedReturnsZero — when the per-target
// budget is exhausted, SendRouteQuery returns 0 and does not even
// look for capable peers.
func TestSendRouteQuery_RateLimitedReturnsZero(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, 1)

	// Fill the (artificially small) budget.
	svc.queryRateLimit.Record(idTargetX)

	if got := svc.SendRouteQuery(idTargetX); got != 0 {
		t.Fatalf("SendRouteQuery returned %d under rate limit, want 0", got)
	}
}

// TestSendRouteQuery_EmptyTargetReturnsZero — defensive guard:
// empty identity is a no-op.
func TestSendRouteQuery_EmptyTargetReturnsZero(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, 3)

	if got := svc.SendRouteQuery(""); got != 0 {
		t.Fatalf("SendRouteQuery(\"\") returned %d, want 0", got)
	}
}

// TestSendRouteQuery_NilRateLimitReturnsZero — defensive guard:
// Service constructed without queryRateLimit (legacy fixtures)
// returns 0 without panicking.
func TestSendRouteQuery_NilRateLimitReturnsZero(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA) // no queryRateLimit wired

	if got := svc.SendRouteQuery(idTargetX); got != 0 {
		t.Fatalf("SendRouteQuery returned %d with nil rate limit, want 0", got)
	}
}

// TestNextRouteQueryID_NonZeroAndMonotonic — the ID generator
// must skip zero (so wire frames with query_id=0 are
// unambiguously distinct from a generated query) and produce
// strictly increasing IDs across calls.
func TestNextRouteQueryID_NonZeroAndMonotonic(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	prev := uint64(0)
	for i := 0; i < 100; i++ {
		id := svc.nextRouteQueryID()
		if id == 0 {
			t.Fatalf("nextRouteQueryID returned 0 at iter %d", i)
		}
		if id <= prev {
			t.Fatalf("nextRouteQueryID non-monotonic: prev=%d, got=%d at iter %d", prev, id, i)
		}
		prev = id
	}
}

// TestSendRouteQuery_FansOutToCapableDirectPeers — happy path
// with real capable sessions in the peer state. PR 11.9 P2#3
// requires the full triplet (query + relay + routing) on
// candidate peers, so two of the three sessions advertise the
// triplet and one is missing relay/routing.
//
// Without a real netcore writer the actual wire send returns
// false and SendRouteQuery's return value reflects that, but the
// budget is still consumed (one stamp per attempted send).
func TestSendRouteQuery_FansOutToCapableDirectPeers(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	tripletCaps := []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	}
	// Wire three sessions: two with the full triplet, one missing
	// the route_query cap (so it's not a candidate at all).
	// PR 11.21 P2#2: candidate discovery now also requires a live
	// peerHealth entry, so the helper seeds both halves together.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, tripletCaps)
	seedHealthyPeerSession(t, svc, "addr-C", idPeerC, tripletCaps)
	seedHealthyPeerSession(t, svc, "addr-D", idPeerD, []domain.Capability{domain.CapMeshRelayV1})

	candidates := svc.peersWithRouteQueryCap()
	if len(candidates) != 2 {
		t.Fatalf("peersWithRouteQueryCap returned %d candidates, want 2 (peer-B and peer-C carry the triplet; peer-D lacks query cap)", len(candidates))
	}

	// We can't assert how many actually got enqueued without a
	// netcore writer, but the rate-limit budget consumption is
	// observable. SendRouteQuery attempts up to len(candidates),
	// not queryFanOutLimit, when fewer capable peers exist.
	svc.SendRouteQuery(idTargetX)
	pending := svc.queryRateLimit.PendingCount(idTargetX)
	if pending == 0 {
		t.Fatal("rate limit consumed 0 stamps; SendRouteQuery must record one per attempted send")
	}
	if pending > 2 {
		t.Fatalf("rate limit consumed %d stamps, want ≤ 2 (capable peer count)", pending)
	}
}
