package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Transit retention + hop budget — the contract pinned here:
//
//   - hop budget: absent/0 or above-default wire values collapse to
//     defaultMessageHopBudget ("treat as if we created it" — legacy
//     peers and hostile inflation alike); in-range values pass through.
//   - admission scan: exact-ID dedup against the backlog (the bloom
//     forgets after 5–10 min; the backlog does not), transit-only
//     accounting per recipient and per byte total.
//   - eviction: per-recipient FIFO cap, global byte budget, local
//     (non-transit) envelopes are never evicted.
//   - expiry: transitInFlightWindow (forwarding-only relays — the
//     window is the lifetime of the retry cycle, not a mailbox TTL)
//     anchored at StoredAt, CreatedAt fallback.
//   - propagation gates: hops < 1 → no gossip at all; ingress link
//     excluded from targets.
// ---------------------------------------------------------------------------

func transitTestService() *Service {
	return &Service{identity: &identity.Identity{Address: "self"}}
}

func transitEnvelope(id, recipient string, payload int, storedAt time.Time) protocol.Envelope {
	return protocol.Envelope{
		ID:        protocol.MessageID(id),
		Topic:     "dm",
		Sender:    "alice",
		Recipient: recipient,
		Payload:   make([]byte, payload),
		StoredAt:  storedAt,
	}
}

func TestEffectiveHopBudget(t *testing.T) {
	t.Parallel()

	cases := []struct{ wire, want int }{
		{0, defaultMessageHopBudget},  // legacy peer / local send
		{-3, defaultMessageHopBudget}, // malformed
		{1, 1},
		{defaultMessageHopBudget, defaultMessageHopBudget},
		{defaultMessageHopBudget + 50, defaultMessageHopBudget}, // hostile inflation clamped
	}
	for _, c := range cases {
		if got := effectiveHopBudget(c.wire); got != c.want {
			t.Errorf("effectiveHopBudget(%d) = %d, want %d", c.wire, got, c.want)
		}
	}
}

func TestScanTopicForAdmission(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	now := time.Now().UTC()
	backlog := []protocol.Envelope{
		transitEnvelope("t1", "bob", 100, now),
		transitEnvelope("t2", "bob", 200, now),
		transitEnvelope("t3", "carol", 400, now),
		{ID: "local1", Topic: "dm", Sender: "self", Recipient: "bob", Payload: make([]byte, 800)},
	}

	scan := scanTopicForAdmission(backlog, "t2", "bob", svc.isTransitEnvelope)
	if !scan.duplicate {
		t.Error("expected duplicate detection for t2")
	}
	if scan.recipientCount != 2 {
		t.Errorf("recipientCount = %d, want 2 (transit only, local1 excluded)", scan.recipientCount)
	}
	if scan.totalBytes != 700 {
		t.Errorf("totalBytes = %d, want 700 (transit only)", scan.totalBytes)
	}

	if scanTopicForAdmission(backlog, "fresh", "bob", svc.isTransitEnvelope).duplicate {
		t.Error("unexpected duplicate for fresh ID")
	}
}

func TestEvictTransitOverflowLocked_PerRecipientFIFO(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	now := time.Now().UTC()
	var backlog []protocol.Envelope
	// Local envelope first — must survive any eviction.
	backlog = append(backlog, protocol.Envelope{ID: "local1", Topic: "dm", Sender: "self", Recipient: "bob", Payload: make([]byte, 10)})
	for i := 0; i < maxTransitPerRecipient; i++ {
		backlog = append(backlog, transitEnvelope(fmt.Sprintf("bob-%d", i), "bob", 10, now))
	}
	backlog = append(backlog, transitEnvelope("carol-0", "carol", 10, now))

	scan := scanTopicForAdmission(backlog, "incoming", "bob", svc.isTransitEnvelope)
	filtered, evicted := evictTransitOverflowLocked(backlog, scan, "bob", 10, svc.isTransitEnvelope)

	if len(evicted) != 1 || evicted[0] != "bob-0" {
		t.Fatalf("evicted = %v, want exactly [bob-0] (oldest transit for bob)", evicted)
	}
	for _, e := range filtered {
		if e.ID == "bob-0" {
			t.Error("bob-0 still present after eviction")
		}
	}
	// Survivors: local + carol untouched.
	if filtered[0].ID != "local1" {
		t.Errorf("local envelope displaced: first is %s", filtered[0].ID)
	}
	if filtered[len(filtered)-1].ID != "carol-0" {
		t.Errorf("carol-0 displaced: last is %s", filtered[len(filtered)-1].ID)
	}
}

func TestEvictTransitOverflowLocked_GlobalByteBudget(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	now := time.Now().UTC()
	const chunk = maxTransitBacklogBytes / 4
	backlog := []protocol.Envelope{
		transitEnvelope("t1", "bob", chunk, now),
		transitEnvelope("t2", "carol", chunk, now),
		transitEnvelope("t3", "dave", chunk, now),
		transitEnvelope("t4", "erin", chunk, now),
	}

	// Incoming transit of one more chunk pushes the total over budget:
	// the globally oldest transit entries must go, regardless of
	// recipient.
	scan := scanTopicForAdmission(backlog, "incoming", "frank", svc.isTransitEnvelope)
	filtered, evicted := evictTransitOverflowLocked(backlog, scan, "frank", chunk, svc.isTransitEnvelope)

	if len(evicted) == 0 {
		t.Fatal("expected global byte-budget eviction, got none")
	}
	if evicted[0] != "t1" {
		t.Errorf("first evicted = %s, want t1 (globally oldest)", evicted[0])
	}
	remaining := 0
	for _, e := range filtered {
		remaining += len(e.Payload)
	}
	if remaining+chunk > maxTransitBacklogBytes {
		t.Errorf("post-eviction total %d + incoming %d still over budget %d", remaining, chunk, maxTransitBacklogBytes)
	}
}

func TestEvictTransitOverflowLocked_NoOpUnderLimits(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	now := time.Now().UTC()
	backlog := []protocol.Envelope{
		transitEnvelope("t1", "bob", 10, now),
	}
	scan := scanTopicForAdmission(backlog, "incoming", "bob", svc.isTransitEnvelope)
	filtered, evicted := evictTransitOverflowLocked(backlog, scan, "bob", 10, svc.isTransitEnvelope)
	if len(evicted) != 0 || len(filtered) != 1 {
		t.Fatalf("under-limit admission must be a no-op: evicted=%v len=%d", evicted, len(filtered))
	}
}

func TestTransitExpired(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// Inside the active retry cycle (relayRetryTTL = 3 min): must stay.
	fresh := transitEnvelope("a", "bob", 1, now.Add(-relayRetryTTL))
	if transitExpired(fresh, now) {
		t.Error("transit inside its in-flight window reported expired")
	}
	old := transitEnvelope("b", "bob", 1, now.Add(-transitInFlightWindow-time.Minute))
	if !transitExpired(old, now) {
		t.Error("transit past its in-flight window not reported expired")
	}
	// Legacy fixture: zero StoredAt falls back to CreatedAt.
	legacy := protocol.Envelope{ID: "c", CreatedAt: now.Add(-transitInFlightWindow - time.Minute)}
	if !transitExpired(legacy, now) {
		t.Error("zero-StoredAt envelope with aged CreatedAt not expired")
	}
	noAnchor := protocol.Envelope{ID: "d"}
	if !transitExpired(noAnchor, now) {
		t.Error("envelope without any time anchor must be expirable")
	}
}

func TestFilterGossipTargetsForEnvelope(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	targets := []domain.PeerAddress{"peer-a:7777", "peer-b:7777", "peer-c:7777"}
	admitted := time.Now().UTC()

	// Budget exhausted (admitted envelope: StoredAt set) → no gossip.
	if got := svc.filterGossipTargetsForEnvelope(protocol.Envelope{Hops: 0, StoredAt: admitted, Via: "peer-a:7777"}, targets); got != nil {
		t.Errorf("exhausted budget must suppress gossip entirely, got %v", got)
	}

	// Legacy envelope (no Hops, no StoredAt — queue-state restore or
	// old fixture): absent => default contract, gossip proceeds.
	if got := svc.filterGossipTargetsForEnvelope(protocol.Envelope{}, targets); len(got) != 3 {
		t.Errorf("legacy envelope must re-arm the default budget, got %v", got)
	}

	// Ingress suppression: the link the message arrived on is excluded.
	got := svc.filterGossipTargetsForEnvelope(protocol.Envelope{Hops: 3, Via: "peer-b:7777"}, targets)
	if len(got) != 2 {
		t.Fatalf("expected 2 targets after ingress suppression, got %v", got)
	}
	for _, tgt := range got {
		if string(tgt) == "peer-b:7777" {
			t.Error("ingress link survived the filter")
		}
	}

	// Local origin (no Via): all targets pass.
	if got := svc.filterGossipTargetsForEnvelope(protocol.Envelope{Hops: defaultMessageHopBudget}, targets); len(got) != 3 {
		t.Errorf("local origin must keep all targets, got %v", got)
	}
}

// Ingress suppression must compare CANONICAL addresses: a session
// dialed on a fallback port (host:64646) and the canonical peer
// address (host:7777) are the same ingress — dialOrigin maps
// fallback → primary, and the filter must collapse either direction.
func TestFilterGossipTargets_FallbackPortAlias(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	svc.dialOrigin = map[domain.PeerAddress]domain.PeerAddress{
		"peer-b:64646": "peer-b:7777", // fallback dial → canonical
	}
	targets := []domain.PeerAddress{"peer-a:7777", "peer-b:64646", "peer-c:7777"}

	// Message arrived via the CANONICAL address; the target list holds
	// the FALLBACK alias of the same peer — it must be suppressed.
	got := svc.filterGossipTargetsForEnvelope(protocol.Envelope{Hops: 3, Via: "peer-b:7777"}, targets)
	if len(got) != 2 {
		t.Fatalf("expected fallback alias suppressed, got %v", got)
	}
	for _, tgt := range got {
		if tgt == "peer-b:64646" {
			t.Error("fallback-port alias of the ingress link survived the filter")
		}
	}

	// And the inverse: arrived via the fallback alias, target list
	// holds... the same alias resolution applies through Via too.
	got = svc.filterGossipTargetsForEnvelope(protocol.Envelope{Hops: 3, Via: "peer-b:64646"}, []domain.PeerAddress{"peer-b:64646", "peer-c:7777"})
	if len(got) != 1 || got[0] != "peer-c:7777" {
		t.Fatalf("expected only peer-c after alias suppression, got %v", got)
	}

	// sameCanonicalAddress agrees in both directions.
	if !svc.sameCanonicalAddress("peer-b:64646", "peer-b:7777") {
		t.Error("sameCanonicalAddress missed the fallback alias")
	}
	if svc.sameCanonicalAddress("peer-a:7777", "peer-b:7777") {
		t.Error("sameCanonicalAddress conflated different peers")
	}
}

// Directed-path ingress suppression for INBOUND-ONLY next-hops: the
// routing table keys those sessions "inbound:<raw remote addr>" while
// Via carries the sanitized overlay address — no address
// canonicalization can bridge the forms, so the check must match on
// the authenticated identity.
func TestIsIngressNextHop_InboundOnlyRouteKey(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	msg := protocol.Envelope{
		Topic: "dm",
		Via:   "10.0.0.5:7777", // sanitized overlay address
		// ViaIdentity is the canonical 40-char hex fingerprint on the wire;
		// it must equal nextHopIdentity.String() for the identity match.
		ViaIdentity: domaintest.ID("identity-B").String(),
	}

	// Table next-hop resolved to B's inbound-keyed session: addresses
	// differ in form AND port, identity matches → suppress.
	if !svc.isIngressNextHop(msg, domaintest.ID("identity-B"), "inbound:10.0.0.5:54321") {
		t.Error("inbound-only ingress next-hop not suppressed by identity")
	}
	// Different peer's identity → no suppression.
	if svc.isIngressNextHop(msg, domaintest.ID("identity-C"), "inbound:10.0.0.9:1234") {
		t.Error("suppressed a next-hop that is not the ingress peer")
	}

	// Identity unknown at admission (ViaIdentity empty): falls back to
	// canonical-address comparison.
	noID := protocol.Envelope{Topic: "dm", Via: "peer-b:7777"}
	if !svc.isIngressNextHop(noID, domaintest.ID("identity-B"), "peer-b:7777") {
		t.Error("address fallback missed an exact ingress match")
	}
	if svc.isIngressNextHop(noID, domaintest.ID("identity-B"), "inbound:10.0.0.5:54321") {
		t.Error("address fallback false-positived on an unrelated inbound key")
	}
}

// TestHopBudgetAdmissionChain pins the end-to-end decrement arithmetic
// implemented in storeIncomingMessage: a network arrival spends one
// hop; a legacy frame (hops absent) is re-armed with the full default
// AS IF originated here; the emitted wire value never goes below 1
// (gossipPushFrame floor — but the gates stop emission before that).
func TestHopBudgetAdmissionChain(t *testing.T) {
	t.Parallel()

	// Origin: local send, no Via → full budget, no decrement.
	if got := effectiveHopBudget(0); got != defaultMessageHopBudget {
		t.Fatalf("origin budget = %d, want %d", got, defaultMessageHopBudget)
	}

	// Relay chain: each hop receives the previous emission and spends 1.
	budget := defaultMessageHopBudget // what origin emits
	hops := 0
	for budget-1 >= 1 {
		budget = effectiveHopBudget(budget) - 1 // admission at next relay
		hops++
	}
	// Origin emits 8 → R1 stores 7 … R7 stores 1 → R8 would store 0.
	if hops != defaultMessageHopBudget-1 {
		t.Errorf("relay chain length = %d, want %d", hops, defaultMessageHopBudget-1)
	}

	// The exhausted-budget envelope still admits (store/deliver) but
	// must never re-gossip. Admission always stamps StoredAt, which is
	// what distinguishes "exhausted" from "legacy pre-field".
	exhausted := protocol.Envelope{Hops: 0, StoredAt: time.Now().UTC(), Via: "x"}
	if got := transitTestService().filterGossipTargetsForEnvelope(exhausted, []domain.PeerAddress{"y"}); got != nil {
		t.Error("exhausted budget re-gossiped")
	}
}

func TestGossipPushFrameCarriesHops(t *testing.T) {
	t.Parallel()

	frame := gossipPushFrame(protocol.Envelope{ID: "m", Topic: "dm", Hops: 5, CreatedAt: time.Now()})
	if frame.Item == nil || frame.Item.Hops != 5 {
		t.Fatalf("gossipPushFrame did not carry hops: %+v", frame.Item)
	}
	// Defensive floor for an ADMITTED exhausted envelope (StoredAt
	// set): 0 is never emitted — absent on the wire would re-arm the
	// FULL default budget at the receiver.
	frame = gossipPushFrame(protocol.Envelope{ID: "m", Topic: "dm", Hops: 0, StoredAt: time.Now().UTC(), CreatedAt: time.Now()})
	if frame.Item.Hops != 1 {
		t.Fatalf("exhausted hops must clamp to 1 on the wire, got %d", frame.Item.Hops)
	}
	// Legacy envelope (neither field): absent => default contract.
	frame = gossipPushFrame(protocol.Envelope{ID: "m", Topic: "dm", CreatedAt: time.Now()})
	if frame.Item.Hops != defaultMessageHopBudget {
		t.Fatalf("legacy envelope must emit the default budget, got %d", frame.Item.Hops)
	}

	// API-contract pin: messageFrame serves FINAL-delivery surfaces
	// (fetch_messages / fetch_inbox / subscriber push) and must NOT
	// stamp a hop budget — clients of the local/query API expect the
	// field absent (TestDuplicateSendMessageIsDeduplicated et al.).
	// Propagation budgets belong exclusively to gossipPushFrame.
	item := messageFrame(protocol.Envelope{ID: "m", Topic: "dm", Hops: 5, CreatedAt: time.Now()})
	if item.Hops != 0 {
		t.Errorf("messageFrame must not stamp hops on final-delivery surfaces, got %d", item.Hops)
	}
}

// relay_message hardening: the gossip budget of a relayed message is a
// function of the chain's HopCount/MaxHops ONLY. A hostile top-level
// hops:8 has no path into the computation (the function does not take
// it), so a near-exhausted chain must come out with a stored budget of
// 0 — no blind-gossip re-arm through the "no capable next hop"
// stored+gossip fallback.
func TestRelayChainGossipBudget(t *testing.T) {
	t.Parallel()

	cases := []struct{ hopCount, maxHops, want int }{
		{0, 0, defaultMaxHops + 1}, // legacy: absent MaxHops normalizes like handleRelayMessage
		{1, 10, 10},                // fresh chain
		{9, 10, 2},                 // one hop left on the chain
		{10, 10, 1},                // chain exhausted
		{12, 10, 1},                // malformed: HopCount past MaxHops
		{-5, 10, 11},               // malformed: negative HopCount
	}
	for _, c := range cases {
		if got := relayChainGossipBudget(c.hopCount, c.maxHops); got != c.want {
			t.Errorf("relayChainGossipBudget(%d, %d) = %d, want %d", c.hopCount, c.maxHops, got, c.want)
		}
	}

	// Composition with admission (storeIncomingMessage arithmetic:
	// effectiveHopBudget then the Via-decrement): an exhausted chain
	// stores 0 regardless of any hops value the sender stamped on the
	// frame — and 0 with StoredAt set means filterGossipTargets-
	// ForEnvelope returns nil.
	stored := effectiveHopBudget(relayChainGossipBudget(10, 10)) - 1
	if stored != 0 {
		t.Fatalf("exhausted chain stored budget = %d, want 0", stored)
	}
	e := protocol.Envelope{Hops: stored, StoredAt: time.Now().UTC(), Via: "b:7777"}
	if got := transitTestService().filterGossipTargetsForEnvelope(e, []domain.PeerAddress{"c:7777"}); got != nil {
		t.Fatalf("exhausted relayed envelope re-gossiped: %v", got)
	}

	// And a fresh chain composes to a usable budget, clamped by the
	// gossip default.
	if stored := effectiveHopBudget(relayChainGossipBudget(1, 10)) - 1; stored != defaultMessageHopBudget-1 {
		t.Fatalf("fresh chain stored budget = %d, want %d", stored, defaultMessageHopBudget-1)
	}
}

// isTransitEnvelope must mirror isLocalMessage's classification:
// global/broadcast traffic is LOCAL (its history must survive), and a
// Service without an identity must classify everything as local
// rather than panic (minimal test fixtures call the sweep).
func TestIsTransitEnvelopeScope(t *testing.T) {
	t.Parallel()

	svc := transitTestService()
	cases := []struct {
		name string
		e    protocol.Envelope
		want bool
	}{
		{"transit dm", protocol.Envelope{Topic: "dm", Sender: "alice", Recipient: "bob"}, true},
		{"own outgoing", protocol.Envelope{Topic: "dm", Sender: "self", Recipient: "bob"}, false},
		{"own incoming", protocol.Envelope{Topic: "dm", Sender: "alice", Recipient: "self"}, false},
		{"global topic", protocol.Envelope{Topic: "global", Sender: "alice", Recipient: "bob"}, false},
		{"broadcast recipient", protocol.Envelope{Topic: "dm", Sender: "alice", Recipient: "*"}, false},
		{"empty recipient", protocol.Envelope{Topic: "dm", Sender: "alice"}, false},
	}
	for _, c := range cases {
		if got := svc.isTransitEnvelope(c.e); got != c.want {
			t.Errorf("%s: isTransitEnvelope = %v, want %v", c.name, got, c.want)
		}
	}

	var bare Service // no identity — must not panic, must say "local"
	if bare.isTransitEnvelope(protocol.Envelope{Topic: "dm", Sender: "a", Recipient: "b"}) {
		t.Error("Service without identity classified an envelope as transit")
	}
}
