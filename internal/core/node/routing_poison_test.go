package node

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_poison_test.go covers the Phase 4 13.3-A explicit poison-
// reverse foundation: SendRoutePoison emit shape, handleRoutePoison
// receive contract (invalidates the (identity, sender) slot only,
// other uplinks for the same identity untouched), sender-sig
// verification (present-invalid drops, absent or unknown-pubkey
// accepts), and the no-claim short-circuit.

// addDirectViaIdentity is a small helper that registers `peer` as a
// direct neighbour in svc.routingTable so subsequent tests have a
// concrete (identity, uplink) slot to poison.
func addDirectViaIdentity(t *testing.T, svc *Service, peer domain.PeerIdentity) {
	t.Helper()
	if _, err := svc.routingTable.AddDirectPeer(domain.PeerIdentity(peer)); err != nil {
		t.Fatalf("AddDirectPeer(%q): %v", peer, err)
	}
}

func TestSendRoutePoison_EmitsRawLineFrameWithSenderSig(t *testing.T) {
	svc, id := newTestServiceWithIdentity(t)

	senderAddr := domain.PeerAddress("addr-peerB-poison")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	if !svc.SendRoutePoison(context.Background(), senderAddr, domain.PeerIdentity(idTargetX), protocol.RoutePoisonReasonUplinkLost) {
		t.Fatalf("SendRoutePoison returned false against a capable session")
	}

	select {
	case got := <-sendCh:
		if got.Type != protocol.RoutePoisonFrameType {
			t.Fatalf("frame Type: got %q want %q", got.Type, protocol.RoutePoisonFrameType)
		}
		parsed, err := protocol.UnmarshalRoutePoisonFrame([]byte(strings.TrimRight(got.RawLine, "\n")))
		if err != nil {
			t.Fatalf("RawLine did not parse as poison: %v (raw=%q)", err, got.RawLine)
		}
		if parsed.Identity != idTargetX || parsed.Reason != protocol.RoutePoisonReasonUplinkLost {
			t.Fatalf("payload wrong: %+v", parsed)
		}
		if parsed.SenderSig == "" {
			t.Fatalf("expected SenderSig populated by local private key")
		}
		// Verify the sig the emitter produced.
		sigBytes, err := base64.StdEncoding.DecodeString(parsed.SenderSig)
		if err != nil {
			t.Fatalf("emitted sig is not valid base64: %v", err)
		}
		if !ed25519.Verify(id.PublicKey, parsed.CanonicalSenderSigBytes(), sigBytes) {
			t.Fatal("emitted sig does not verify against the local public key")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("no frame was emitted within 100ms")
	}
}

func TestSendRoutePoison_RefusesPeerWithoutCapability(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)

	senderAddr := domain.PeerAddress("addr-peerB-nopoison")
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentity(idPeerB),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()

	if svc.SendRoutePoison(context.Background(), senderAddr, domain.PeerIdentity(idTargetX), protocol.RoutePoisonReasonUplinkLost) {
		t.Fatalf("SendRoutePoison must return false when peer lacks poison-reverse cap")
	}
	select {
	case got := <-sendCh:
		t.Fatalf("no frame must be enqueued without cap; got %q", got.Type)
	default:
	}
}

func TestHandleRoutePoison_InvalidatesOnlySenderUplinkClaim(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)

	// Two direct peers: idPeerB (the poison sender) and idOriginC (an
	// alternate uplink for the same target). Both have a direct claim
	// for themselves; we also seed a transit announcement so the same
	// target identity (idTargetX) is reachable via BOTH peers.
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idPeerB))
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idOriginC))

	// Insert a transit claim for idTargetX via idPeerB.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed via idPeerB: %v", err)
	}
	// And a transit claim for idTargetX via idOriginC.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idOriginC),
		NextHop:  domain.PeerIdentity(idOriginC),
		Hops:     3,
		SeqNo:    7,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed via idOriginC: %v", err)
	}

	// Sanity precondition: both uplinks reachable.
	pre := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	if len(pre) < 2 {
		t.Fatalf("precondition: expected 2 uplinks for target, got %d", len(pre))
	}

	// Receive an unsigned poison from idPeerB targeting idTargetX.
	frame := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-28T12:00:00Z",
	}
	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), frame)

	// Lookup filters withdrawn / expired entries (see table_lookup.go),
	// so the invalidated claim disappears from the result: post must
	// contain NO live entry with NextHop == idPeerB, and must contain
	// at least one entry with NextHop == idOriginC — that uplink slot
	// is untouched (overview §4.2 trust budget: poison invalidates ONLY
	// the sender's own slot).
	post := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	for _, r := range post {
		if r.NextHop == domain.PeerIdentity(idPeerB) {
			t.Fatalf("idPeerB uplink claim must be invalidated and filtered from Lookup; got %+v", r)
		}
	}
	found := false
	for _, r := range post {
		if r.NextHop == domain.PeerIdentity(idOriginC) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("idOriginC uplink claim must survive — poison must NOT touch other uplinks")
	}
}

func TestHandleRoutePoison_InvalidSenderSigDropsFrame(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerKnownPubKey(t, svc, peer.Address, peer.PublicKey)
	addDirectViaIdentity(t, svc, domain.PeerIdentity(peer.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(peer.Address),
		NextHop:  domain.PeerIdentity(peer.Address),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim: %v", err)
	}

	// Build a poison frame with a corrupt SenderSig.
	frame := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonHealthDead,
		IssuedAt: "2026-05-28T12:00:00Z",
	}
	goodSig := ed25519.Sign(peer.PrivateKey, frame.CanonicalSenderSigBytes())
	goodSig[0] ^= 0xff // corrupt
	frame.SenderSig = base64.StdEncoding.EncodeToString(goodSig)
	svc.handleRoutePoison(domain.PeerIdentity(peer.Address), frame)

	// The claim must NOT have been invalidated: Lookup still returns
	// a live entry for (idTargetX, peer.Address).
	stillLive := false
	for _, r := range svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)) {
		if r.NextHop == domain.PeerIdentity(peer.Address) {
			stillLive = true
			break
		}
	}
	if !stillLive {
		t.Fatal("invalid sig must drop the poison frame; claim must remain live in Lookup")
	}
}

// TestHandleRoutePoison_QuarantinedSender_DropsSilently is the regression
// for the quarantine-gate gap in handleRoutePoison. Without the top-level
// gate, a quarantined peer can still send a properly-signed
// route_poison_v1 frame and:
//
//  1. Mutate routingTable via InvalidateUplinkClaim (the quarantined
//     peer's opinion that uplink X is dead overrides our state).
//  2. When that uplink was the last one to the target, trigger
//     poisonReverseToOtherPeers — outbound routing-control traffic
//     that propagates the quarantined peer's untrusted view across
//     the mesh.
//
// Both effects are exactly what quarantine is supposed to suppress.
// This test arms quarantine for the sender, then sends a poison
// frame the handler would normally honour (correctly signed,
// session-known pubkey, valid identity), and asserts that the
// uplink claim survives.
func TestHandleRoutePoison_QuarantinedSender_DropsSilently(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerKnownPubKey(t, svc, peer.Address, peer.PublicKey)
	addDirectViaIdentity(t, svc, domain.PeerIdentity(peer.Address))

	// Arm route quarantine for the sender FIRST. Arming now locally
	// invalidates existing transit claims via the peer (see
	// invalidateTransitOnQuarantineLocked), so the claim this test
	// needs alive is seeded AFTER the arm, via the direct UpdateRoute
	// API (which bypasses the receive-path quarantine gate by design
	// — this test is about handleRoutePoison's gate, not the
	// announce-ingest one).
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(domain.PeerIdentity(peer.Address), "test", time.Now())
	svc.peerMu.Unlock()

	// Seed a transit claim through the sender so the poison frame
	// has something to invalidate. Without the seed, InvalidateUplinkClaim
	// is a no-op and the test would pass even without the gate.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(peer.Address),
		NextHop:  domain.PeerIdentity(peer.Address),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim: %v", err)
	}

	// Sanity: the seeded claim is live before the handler call.
	pre := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	preLive := false
	for _, r := range pre {
		if r.NextHop == domain.PeerIdentity(peer.Address) {
			preLive = true
			break
		}
	}
	if !preLive {
		t.Fatalf("precondition: seeded claim must be live before poison")
	}

	// Build a properly signed poison frame the handler would
	// otherwise accept. The point of the test is that the
	// quarantine gate fires BEFORE signature verification + storage
	// mutation, so the frame's payload is correct on purpose.
	frame := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonHealthDead,
		IssuedAt: "2026-05-28T12:00:00Z",
	}
	sig := ed25519.Sign(peer.PrivateKey, frame.CanonicalSenderSigBytes())
	frame.SenderSig = base64.StdEncoding.EncodeToString(sig)

	svc.handleRoutePoison(domain.PeerIdentity(peer.Address), frame)

	// The claim must still be live — quarantine gate fired before
	// InvalidateUplinkClaim.
	post := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	stillLive := false
	for _, r := range post {
		if r.NextHop == domain.PeerIdentity(peer.Address) {
			stillLive = true
			break
		}
	}
	if !stillLive {
		t.Fatal("quarantined peer's poison frame was honoured: uplink claim invalidated — quarantine gate missing in handleRoutePoison")
	}
}

func TestHandleRoutePoison_AbsentSigAccepted(t *testing.T) {
	// Absent SenderSig is accepted (session-level identity binding is
	// the primary trust anchor; sig is defence-in-depth). This mirrors
	// the Tier-2 lenient policy for unsigned attested-links entries.
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idPeerB))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Sanity precondition: the seeded claim is reachable in Lookup
	// before poison runs.
	pre := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	if len(pre) == 0 {
		t.Fatalf("precondition: target must be reachable via idPeerB before poison")
	}

	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonLoopDetected,
		IssuedAt: "2026-05-28T12:00:00Z",
		// SenderSig deliberately empty.
	})
	// Lookup filters withdrawn entries, so the invalidated claim
	// disappears from the result. With idPeerB the only uplink, the
	// post-Lookup result must be empty — proving the invalidation
	// landed.
	post := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX))
	for _, r := range post {
		if r.NextHop == domain.PeerIdentity(idPeerB) {
			t.Fatalf("unsigned poison from a session-known sender must invalidate the claim; got live %+v", r)
		}
	}
}

// TestHandleRoutePoison_RepeatedPoisonIsIdempotent pins the Round-5
// idempotency contract on Table.InvalidateUplinkClaim: a duplicate
// route_poison_v1 from the same peer for the same identity must NOT
// bump the tombstone SeqNo a second time, must NOT publish a fresh
// route-change event, and must NOT touch storage. The earlier
// implementation called peekUplinkSeqLocked which returned the SeqNo
// of ANY slot (including already-withdrawn tombstones), so each
// duplicate poison wrote a NEW tombstone at SeqNo+1; the inflated
// tombstone SeqNo could then rank above a legitimate recovery
// announce and break recovery. The fix gates peek on live/non-
// expired claims, making the second-and-later poison a clean no-op.
func TestHandleRoutePoison_RepeatedPoisonIsIdempotent(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idPeerB))
	const seededSeqNo = uint64(5)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
		Hops:     2,
		SeqNo:    seededSeqNo,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim: %v", err)
	}

	frame := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-28T12:00:00Z",
	}

	// First poison: live claim → withdraw → tombstone at SeqNo=6
	// (seededSeqNo + 1, the strictly-newer SeqNo InvalidateUplinkClaim
	// synthesises against the live claim).
	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), frame)
	first := svc.routingTable.InspectTriple(routing.RouteTriple{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
	})
	if first == nil {
		t.Fatalf("first poison must leave a tombstone for inspection")
	}
	if !first.IsWithdrawn() {
		t.Fatalf("first poison must mark the claim withdrawn, got Hops=%d", first.Hops)
	}
	if first.SeqNo != seededSeqNo+1 {
		t.Fatalf("tombstone SeqNo after first poison: got %d want %d (seeded+1)", first.SeqNo, seededSeqNo+1)
	}

	// Second poison for the same (identity, sender): the live-claim
	// gate makes this a clean no-op — tombstone SeqNo must NOT move.
	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), frame)
	second := svc.routingTable.InspectTriple(routing.RouteTriple{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
	})
	if second == nil {
		t.Fatalf("second poison must not delete the tombstone")
	}
	if second.SeqNo != first.SeqNo {
		t.Fatalf("repeated poison must NOT bump tombstone SeqNo: got %d want %d (idempotent)", second.SeqNo, first.SeqNo)
	}

	// Third poison drives the point home: still the same SeqNo.
	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), frame)
	third := svc.routingTable.InspectTriple(routing.RouteTriple{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
	})
	if third == nil || third.SeqNo != first.SeqNo {
		t.Fatalf("third poison must remain a no-op; SeqNo: got %d want %d", third.SeqNo, first.SeqNo)
	}
}

// TestHandleRoutePoison_RateLimited pins the Round-5 fix that the
// announce-plane rate limiter also covers route_poison_v1. The
// original implementation only protected announce_routes /
// routes_update / route_announce_v3 / request_resync, which left
// the poison receive path as an un-throttled flood channel into the
// base64+ed25519 verify + Table.InvalidateUplinkClaim mutation. The
// fix charges a token from the SAME shared bucket BEFORE the verify
// path runs, so a flood is rejected with the standard
// announce_rate_limit_drop log line.
//
// Strategy: drain the limiter for the test peer to zero by spending
// announceBurstRoutesPerPeer route-tokens in one allow call (Round-10:
// per-route budgeting, equivalent to the peer having exhausted the
// bucket via a burst of announce frames), then fire one poison and
// assert (a) no table mutation, (b) limiter still empty.
func TestHandleRoutePoison_RateLimited(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)
	// The test-harness Service constructor leaves announceLimiter nil
	// for backward compatibility with fixtures that predate Phase 4
	// 13.7; the poison handler short-circuits the rate-limit branch on
	// nil. This test exercises the actual throttle, so wire a fresh
	// limiter explicitly — same shape NewService gives the production
	// service.
	svc.announceLimiter = newAnnounceRateLimiter()
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idPeerB))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idPeerB),
		NextHop:  domain.PeerIdentity(idPeerB),
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim: %v", err)
	}

	// Exhaust the shared per-peer announce-plane bucket so the next
	// allow() call (whatever the wire type) returns false. The bucket
	// is per-PeerIdentity, so any peer-correlated allow drains it.
	// Drain by spending the whole burst in one allow (Round-10:
	// per-route budgeting — one call with cost=burst drops the
	// bucket to exactly zero, which is what we want for the
	// "next allow must fail" precondition).
	if !svc.announceLimiter.allow(domain.PeerIdentity(idPeerB), announceBurstRoutesPerPeer) {
		t.Fatalf("precondition: full-burst allow against fresh bucket must pass")
	}
	if svc.announceLimiter.allow(domain.PeerIdentity(idPeerB), 1) {
		t.Fatalf("precondition: bucket should be exhausted after burst drain")
	}
	// Re-seed exactly zero tokens to remove any micro-refill that
	// elapsed between the drain and this point. After this the next
	// poison allow (cost=1) is precisely the one that should fail.
	svc.announceLimiter.mu.Lock()
	svc.announceLimiter.buckets[domain.PeerIdentity(idPeerB)].tokens = 0
	svc.announceLimiter.mu.Unlock()

	frame := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-28T12:00:00Z",
		// No SenderSig — the test would still pass with a sig (verify
		// is gated behind the rate-limit), but absence keeps the test
		// focused on the limiter and avoids per-test key plumbing.
	}
	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), frame)

	// Storage must not have been touched: the live transit claim
	// stays in Lookup.
	stillLive := false
	for _, r := range svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)) {
		if r.NextHop == domain.PeerIdentity(idPeerB) {
			stillLive = true
			break
		}
	}
	if !stillLive {
		t.Fatal("rate-limited poison must NOT mutate storage; claim must remain live")
	}
}

// TestHandleRoutePoison_FansOutWhenNoBackupUplink pins the Round-16
// fix: a received route_poison_v1 that leaves our local table with NO
// surviving uplink to the target must re-emit our own poison to all
// OTHER direct peers. Without this fan-out the wave stops at the
// first receiver because AnnounceProjectionFor only emits own-direct
// tombstones — transit tombstones from InvalidateUplinkClaim are
// filtered out, so downstream peers would keep the stale route until
// TTL and count-to-infinity would collapse only one hop per cycle.
//
// Topology for the test: senderPeer is the poison source (has the
// route via its uplink that just died); otherPeer is a different
// direct neighbour that learned target through us. After we receive
// the poison from senderPeer, otherPeer must receive a poison from us
// about the same target.
func TestHandleRoutePoison_FansOutWhenNoBackupUplink(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)

	// Direct sessions for both the poison sender and the downstream
	// peer that needs the fan-out. peerSessionFixture wires the
	// session, capabilities, and health so SendRoutePoison can
	// enqueue frames.
	senderAddr := domain.PeerAddress("addr-sender")
	otherAddr := domain.PeerAddress("addr-other")
	senderID := domain.PeerIdentity(idPeerB)
	otherID := domain.PeerIdentity(idOriginC)
	_ = peerSessionFixture(t, svc, senderAddr, senderID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	otherCh := peerSessionFixture(t, svc, otherAddr, otherID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	// Direct claims so routingCapablePeers picks them up.
	addDirectViaIdentity(t, svc, senderID)
	addDirectViaIdentity(t, svc, otherID)

	// Single transit claim: target reachable ONLY via senderID. After
	// the poison invalidates this slot, len(Lookup) == 0 and the
	// fan-out branch must fire.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   senderID,
		NextHop:  senderID,
		Hops:     2,
		SeqNo:    5,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed transit claim: %v", err)
	}

	// Drain otherCh of any pre-existing fixture noise.
	for {
		select {
		case <-otherCh:
		default:
			goto done
		}
	}
done:

	svc.handleRoutePoison(senderID, protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-30T12:00:00Z",
	})

	// otherPeer must receive a route_poison_v1 frame about idTargetX
	// — that is the count-to-infinity collapse the wave is supposed
	// to deliver.
	select {
	case got := <-otherCh:
		if got.Type != protocol.RoutePoisonFrameType {
			t.Fatalf("otherPeer frame type: got %q want %q", got.Type, protocol.RoutePoisonFrameType)
		}
		parsed, err := protocol.UnmarshalRoutePoisonFrame([]byte(strings.TrimRight(got.RawLine, "\n")))
		if err != nil {
			t.Fatalf("otherPeer RawLine did not parse as poison: %v", err)
		}
		if parsed.Identity != idTargetX {
			t.Fatalf("fan-out poison about wrong identity: got %q want %q", parsed.Identity, idTargetX)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Round-16 fan-out missing: otherPeer received no poison frame within 200ms; count-to-infinity wave stops at first receiver")
	}
}

// TestHandleRoutePoison_NoFanOutWhenBackupUplinkSurvives pins the
// complementary contract: when a backup uplink to target survives
// the invalidation, our claim to target is still live, so downstream
// peers should keep routing through us — no fan-out poison goes out.
// This guards against turning the new fan-out into an unconditional
// echo that would spam the mesh on every poison receive.
func TestHandleRoutePoison_NoFanOutWhenBackupUplinkSurvives(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)

	senderAddr := domain.PeerAddress("addr-sender")
	otherAddr := domain.PeerAddress("addr-other")
	backupAddr := domain.PeerAddress("addr-backup")
	senderID := domain.PeerIdentity(idPeerB)
	otherID := domain.PeerIdentity(idOriginC)
	backupID := domain.PeerIdentity("dd00000000000000000000000000000000000020")

	_ = peerSessionFixture(t, svc, senderAddr, senderID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	otherCh := peerSessionFixture(t, svc, otherAddr, otherID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	_ = peerSessionFixture(t, svc, backupAddr, backupID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	addDirectViaIdentity(t, svc, senderID)
	addDirectViaIdentity(t, svc, otherID)
	addDirectViaIdentity(t, svc, backupID)

	// Target reachable via BOTH senderID AND backupID. After the
	// poison invalidates the senderID slot, the backupID claim
	// survives — fan-out must be suppressed.
	for _, uplink := range []domain.PeerIdentity{senderID, backupID} {
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: domain.PeerIdentity(idTargetX),
			Origin:   uplink,
			NextHop:  uplink,
			Hops:     2,
			SeqNo:    5,
			Source:   routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("seed transit via %s: %v", uplink, err)
		}
	}

	// Drain otherCh.
	for {
		select {
		case <-otherCh:
		default:
			goto done
		}
	}
done:

	svc.handleRoutePoison(senderID, protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-30T12:00:00Z",
	})

	// otherPeer MUST receive nothing — backup uplink survives, so
	// our claim is still live and there's no reason to invalidate
	// downstream.
	select {
	case got := <-otherCh:
		t.Fatalf("fan-out must be suppressed when a backup uplink survives; got unexpected frame %q", got.Type)
	case <-time.After(150 * time.Millisecond):
		// Expected: silence.
	}
}

func TestHandleRoutePoison_NoClaimShortCircuits(t *testing.T) {
	// No prior claim for (identity, sender) → handler returns without
	// touching the table. Verified indirectly by the table staying
	// empty across the call.
	svc, _ := newTestServiceWithIdentity(t)
	svc.eventBus = newStormBus(t)

	svc.handleRoutePoison(domain.PeerIdentity(idPeerB), protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX,
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: "2026-05-28T12:00:00Z",
	})
	if got := svc.routingTable.Lookup(domain.PeerIdentity(idTargetX)); len(got) > 0 {
		t.Fatalf("table must stay empty when no claim to poison; got %d entries", len(got))
	}
}
