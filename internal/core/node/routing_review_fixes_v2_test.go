package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_review_fixes_v2_test.go pins behaviour fixed in PR 11.8
// in response to a second Phase 2 review pass. Each test
// corresponds to one numbered concern from that pass.
//
// idTargetX / idTargetY / idPeerB / idPeerC / idPeerD come from
// routing_integration_test.go's package-level const block.

// --- P1 #1: ensureLocked on Accepted ---

// TestReview_v2_P1_1_UpdateRoute_EnsuresHealthOnAccepted —
// concern P1#1: a freshly accepted live claim must create a
// RouteHealthState entry so the pair becomes visible to
// fetchRouteHealth, gets aged by TickHealth, and eventually
// reaches the probe-sender ticker through the Questionable
// scan. Without this step health remains nil and the active
// probe path never starts for the pair.
func TestReview_v2_P1_1_UpdateRoute_EnsuresHealthOnAccepted(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d after first UpdateRoute, want 1", len(snap))
	}
	if snap[0].Identity != idTargetX || snap[0].Uplink != idPeerB {
		t.Fatalf("entry = (%q, %q), want (idTargetX, idPeerB)", snap[0].Identity, snap[0].Uplink)
	}
	// PR 11.9 P1#1: freshly accepted claims seed Questionable
	// (not Good) so the probe loop performs one-shot verify-
	// before-trust before the route earns the full composite
	// score bonus. Good is reserved for confirmed reachability
	// (hop_ack or probe_ack(reachable=true)).
	if snap[0].Health != routing.HealthQuestionable {
		t.Fatalf("Health = %s, want questionable (initial state for freshly accepted live claim — verify-before-trust)", snap[0].Health)
	}
}

// TestReview_v2_P1_1_UpdateRoute_PreservesExistingGoodAndQuestionable —
// the ensureLocked path must NOT clobber existing
// Good/Questionable health when an accepted update re-asserts the
// claim. Only Bad/Dead get reset; everything else is preserved so
// the EWMA RTT and TransitionAt history survive the update.
func TestReview_v2_P1_1_UpdateRoute_PreservesExistingGoodAndQuestionable(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}
	// Force into Questionable AFTER seed so we observe whether
	// a fresh UpdateRoute clobbers it.
	tbl.ForceHealthForTest(idTargetX, idPeerB, routing.HealthQuestionable)

	// Reconfirmation with higher SeqNo (status=RouteAccepted).
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 2, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("reconfirmation UpdateRoute: %v", err)
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthQuestionable {
		t.Fatalf("Health = %s, want questionable (existing non-Bad/Dead must be preserved)", snap[0].Health)
	}
}

// --- P1 #2: RouteUnchanged also evicts Bad/Dead ---

// TestReview_v2_P1_2_UpdateRoute_UnchangedEvictsDeadHealth —
// concern P1#2: same-SeqNo reconfirmation returns RouteUnchanged
// AND refreshes TTL (a fresh peer attestation arrived even if no
// fields changed). The stale Bad/Dead health must be evicted on
// that path too, otherwise a route_query_response confirming the
// same uplink with the same SeqNo would leave Lookup filtering
// the freshly-confirmed route.
func TestReview_v2_P1_2_UpdateRoute_UnchangedEvictsDeadHealth(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}
	tbl.ForceHealthForTest(idTargetX, idPeerB, routing.HealthDead)

	// Lookup must filter the Dead claim — confirms the
	// precondition we are about to fix.
	if routes := tbl.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("setup: Lookup returned %d entries with Dead health, want 0", len(routes))
	}

	// Same-SeqNo reconfirmation; ApplyUpdate returns
	// RouteUnchanged and refreshes the TTL on the existing claim.
	status, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("reconfirmation UpdateRoute: %v", err)
	}
	if status != routing.RouteUnchanged {
		t.Fatalf("status = %v, want RouteUnchanged for same-SeqNo reconfirmation", status)
	}

	// Post-condition: Dead evicted, Good ensured, Lookup surfaces
	// the claim.
	routes := tbl.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("after Unchanged reconfirmation: Lookup len = %d, want 1", len(routes))
	}
	if routes[0].NextHop != idPeerB {
		t.Fatalf("Lookup[0].NextHop = %q, want idPeerB", routes[0].NextHop)
	}
}

// --- P2 #3: ResolveMatching is atomic and uplink-aware ---

// TestReview_v2_P2_3_ResolveMatching_MismatchKeepsPending —
// concern P2#3: a probe_ack arriving from peer V for a probe sent
// to peer U must NOT consume the registered probe. The real
// uplink's ack (or timeout) must still apply.
func TestReview_v2_P2_3_ResolveMatching_MismatchKeepsPending(t *testing.T) {
	reg := newProbeRegistry(time.Second, nil, nil)
	id := reg.Register(idTargetX, idPeerB)
	if reg.Len() != 1 {
		t.Fatalf("setup: registry Len = %d, want 1", reg.Len())
	}

	// Spoofed ack from a different uplink.
	_, _, ok := reg.ResolveMatching(id, idPeerC)
	if ok {
		t.Fatal("ResolveMatching returned ok=true for mismatched uplink; want false")
	}
	if reg.Len() != 1 {
		t.Fatalf("registry Len = %d after mismatched ResolveMatching, want 1 (entry must be retained)", reg.Len())
	}

	// Real ack from the actual uplink resolves the entry.
	target, _, ok := reg.ResolveMatching(id, idPeerB)
	if !ok {
		t.Fatal("ResolveMatching returned ok=false for the real uplink; want true")
	}
	if target != idTargetX {
		t.Fatalf("target = %q, want idTargetX", target)
	}
	if reg.Len() != 0 {
		t.Fatalf("registry Len = %d after successful Resolve, want 0", reg.Len())
	}
}

// TestReview_v2_P2_3_HandleRouteProbeAck_MismatchDoesNotConsume —
// integration-level pin of the same invariant via the actual
// receive-path handler. A mismatched ack must not leave a real
// outstanding probe vulnerable to subsequent absent-state
// behaviour.
func TestReview_v2_P2_3_HandleRouteProbeAck_MismatchDoesNotConsume(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	id := svc.probeRegistry.Register(idTargetX, idPeerB)
	if svc.probeRegistry.Len() != 1 {
		t.Fatalf("setup: registry Len = %d, want 1", svc.probeRegistry.Len())
	}

	// Mismatched ack — sender identity != registered uplink.
	svc.handleRouteProbeAck(idPeerC, protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   id,
		Reachable: true,
		RTTMs:     5,
	})

	if svc.probeRegistry.Len() != 1 {
		t.Fatalf("registry Len = %d after mismatched ack, want 1", svc.probeRegistry.Len())
	}
	if snap := svc.routingTable.HealthSnapshot(); snap != nil {
		t.Fatalf("mismatched ack mutated health state: %v", snap)
	}

	// Now the legitimate ack arrives and is processed normally.
	svc.handleRouteProbeAck(idPeerB, protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   id,
		Reachable: true,
		RTTMs:     0,
	})
	if svc.probeRegistry.Len() != 0 {
		t.Fatalf("registry Len = %d after legitimate ack, want 0", svc.probeRegistry.Len())
	}
}

// --- P2 #4: identity validation on ingest and trigger ---

// TestReview_v2_P2_4_HandleRouteQueryResponse_DropsInvalidTarget —
// concern P2#4: an authenticated query-capable peer must not be
// able to inject routes to malformed identities. The handler
// validates resp.TargetIdentity through identity.IsValidAddress
// (the same guard the announce path uses).
func TestReview_v2_P2_4_HandleRouteQueryResponse_DropsInvalidTarget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	junkTargets := []string{
		"*",
		"   ",
		"not-a-fingerprint",
		"abc",              // too short
		"abc123abc123abc1", // wrong length / non-hex tail
	}
	for _, junk := range junkTargets {
		svc.handleRouteQueryResponse(idPeerB, protocol.RouteQueryResponseFrame{
			Type:           protocol.RouteQueryResponseFrameType,
			QueryID:        100,
			TargetIdentity: domain.PeerIdentity(junk),
			Found:          true,
			BestUplink:     idPeerD,
			BestHops:       2,
			BestSeqNo:      42,
		})
		// Lookup on the junk target must return nothing — the
		// handler must have rejected the ingest before
		// UpdateRoute.
		if routes := svc.routingTable.Lookup(routing.PeerIdentity(junk)); len(routes) != 0 {
			t.Fatalf("junk target %q produced %d ingested routes; want 0", junk, len(routes))
		}
	}
}

// TestReview_v2_P2_4_TriggerRouteQueryAsync_RejectsInvalidTarget —
// the same guard on the sender side: triggerRouteQueryAsync (and
// the underlying SendRouteQuery) refuse to consume rate-limit
// budget on a junk target.
func TestReview_v2_P2_4_TriggerRouteQueryAsync_RejectsInvalidTarget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	// Add a capable peer so the only thing keeping us from
	// consuming budget would be the validation gate.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRouteQueryV1},
	}

	junkTargets := []string{
		"*",
		"   ",
		"not-a-fingerprint",
		"abc",
	}
	for _, junk := range junkTargets {
		svc.triggerRouteQueryAsync(domain.PeerIdentity(junk))
	}
	// Even after some scheduler slack, the budget must be untouched.
	time.Sleep(50 * time.Millisecond)
	for _, junk := range junkTargets {
		if got := svc.queryRateLimit.PendingCount(domain.PeerIdentity(junk)); got != 0 {
			t.Fatalf("junk target %q consumed %d rate-limit slot(s); want 0", junk, got)
		}
	}
}

// TestReview_v2_P2_4_SendRouteQuery_RejectsInvalidTarget — direct
// pin on the synchronous SendRouteQuery entry point. Returns 0
// without touching any state.
func TestReview_v2_P2_4_SendRouteQuery_RejectsInvalidTarget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	if got := svc.SendRouteQuery("*"); got != 0 {
		t.Fatalf("SendRouteQuery(\"*\") = %d, want 0", got)
	}
	if got := svc.queryRateLimit.PendingCount(domain.PeerIdentity("*")); got != 0 {
		t.Fatalf("rate-limit consumed for invalid target: %d", got)
	}
}

// --- Cross-cutting regression guard ---

// TestReview_v2_P1_1_FreshUplinkAppearsInHealthSnapshot — pin of
// the user-visible consequence of P1#1: fetchRouteHealth surfaces
// the new pair immediately after the first announce ingest. This
// is what was broken before — operators would see an empty
// health snapshot for routes that obviously existed in the
// routing table.
func TestReview_v2_P1_1_FreshUplinkAppearsInHealthSnapshot(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	// Two distinct (Identity, Uplink) pairs.
	for _, ent := range []routing.RouteEntry{
		{Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
		{Identity: idTargetY, Origin: idTargetY, NextHop: idPeerC, Hops: 3, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
	} {
		if _, err := tbl.UpdateRoute(ent); err != nil {
			t.Fatalf("UpdateRoute(%v): %v", ent.Identity, err)
		}
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 2 {
		t.Fatalf("HealthSnapshot len = %d after two distinct UpdateRoutes, want 2", len(snap))
	}
}
