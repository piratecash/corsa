package node

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func identHex(c string) domain.PeerIdentity {
	return domain.PeerIdentityFromWire(strings.Repeat(c, 40))
}

func TestRelayDeliveredRecordSnapshotForget(t *testing.T) {
	t.Parallel()
	svc := &Service{}
	a := identHex("a")
	b := identHex("b")

	// No-ops: empty id, zero identity.
	svc.recordRelayDeliveredTo("", a)
	svc.recordRelayDeliveredTo("m1", domain.PeerIdentity{})
	if svc.relayDeliveredSnapshot("m1") != nil {
		t.Fatal("no-op records must leave the set empty")
	}

	svc.recordRelayDeliveredTo("m1", a)
	svc.recordRelayDeliveredTo("m1", b)
	snap := svc.relayDeliveredSnapshot("m1")
	if len(snap) != 2 {
		t.Fatalf("snapshot size = %d, want 2", len(snap))
	}
	if _, ok := snap[a]; !ok {
		t.Error("identity a missing from snapshot")
	}

	// Snapshot is a copy — mutating it must not affect stored state.
	delete(snap, a)
	if len(svc.relayDeliveredSnapshot("m1")) != 2 {
		t.Error("snapshot must be a copy, not the live set")
	}

	svc.forgetRelayDelivered([]protocol.MessageID{"m1"})
	if svc.relayDeliveredSnapshot("m1") != nil {
		t.Error("forgetRelayDelivered must drop the set")
	}
}

func TestExcludeNextHopFromGossip(t *testing.T) {
	t.Parallel()
	a := domain.PeerAddress("a")
	b := domain.PeerAddress("b")
	c := domain.PeerAddress("c")
	idA, idB, idC := identHex("a"), identHex("b"), identHex("c")
	svc := &Service{peerIDs: map[domain.PeerAddress]domain.PeerIdentity{a: idA, b: idB, c: idC}}

	// By identity: next-hop identity idB → drop b, keep a and c.
	got := svc.excludeNextHopFromGossip([]domain.PeerAddress{a, b, c}, idB, b)
	if len(got) != 2 || got[0] != a || got[1] != c {
		t.Fatalf("identity match dropped wrong element: %v", got)
	}

	// No-op: zero identity AND empty address.
	if got := svc.excludeNextHopFromGossip([]domain.PeerAddress{a, b}, domain.PeerIdentity{}, ""); len(got) != 2 {
		t.Errorf("zero id + empty addr must be a no-op, got %v", got)
	}

	// inbound: alias — next-hop ADDRESS is inbound:<raw> but the gossip target
	// is the plain peer; identity match drops it where a raw/canonical address
	// compare would miss (P3 regression).
	rid := identHex("d")
	inboundSvc := &Service{peerIDs: map[domain.PeerAddress]domain.PeerIdentity{
		"remote:7777": rid, "other:7777": identHex("e"),
	}}
	gotIn := inboundSvc.excludeNextHopFromGossip([]domain.PeerAddress{"remote:7777", "other:7777"}, rid, "inbound:remote")
	if len(gotIn) != 1 || gotIn[0] != "other:7777" {
		t.Errorf("identity match must drop inbound-alias next-hop: %v", gotIn)
	}

	// Canonical-address fallback when identity is unknown (zero id): a
	// dialOrigin fallback-port alias of the next-hop is still dropped.
	aliasSvc := &Service{dialOrigin: map[domain.PeerAddress]domain.PeerAddress{"peer:64646": "peer:7777"}}
	gotAlias := aliasSvc.excludeNextHopFromGossip([]domain.PeerAddress{"peer:64646", "x:7777"}, domain.PeerIdentity{}, "peer:7777")
	if len(gotAlias) != 1 || gotAlias[0] != "x:7777" {
		t.Errorf("canonical alias of next-hop must be excluded, got %v", gotAlias)
	}
}

func TestSelectFanoutSubset(t *testing.T) {
	t.Parallel()
	targets := []domain.PeerAddress{"a", "b", "c", "d", "e"}
	set := map[domain.PeerAddress]bool{"a": true, "b": true, "c": true, "d": true, "e": true}

	// limit 0 (disabled) and limit >= len → unchanged.
	if got := selectFanoutSubset(targets, 0, "m"); len(got) != len(targets) {
		t.Fatalf("limit 0 must be unlimited, got %d", len(got))
	}
	if got := selectFanoutSubset(targets, 10, "m"); len(got) != len(targets) {
		t.Fatalf("limit >= len must return all, got %d", len(got))
	}

	// limit 2 → exactly 2, all valid members, deterministic across calls.
	s1 := selectFanoutSubset(targets, 2, "m")
	s2 := selectFanoutSubset(targets, 2, "m")
	if len(s1) != 2 {
		t.Fatalf("limit 2 must return 2, got %d", len(s1))
	}
	for i := range s1 {
		if !set[s1[i]] {
			t.Errorf("subset member %q not in targets", s1[i])
		}
		if s1[i] != s2[i] {
			t.Errorf("subset not deterministic: %v vs %v", s1, s2)
		}
	}

	// Spread: different message ids should not all map to the same subset.
	distinct := map[domain.PeerAddress]bool{}
	for i := 0; i < 50; i++ {
		got := selectFanoutSubset(targets, 1, protocol.MessageID(string(rune('A'+i))))
		distinct[got[0]] = true
	}
	if len(distinct) < 2 {
		t.Errorf("fan-out subset does not spread across seeds: %v", distinct)
	}
}

func TestFilterGossipTargetsExcludesAckedPeer(t *testing.T) {
	t.Parallel()
	pA := domain.PeerAddress("peerA")
	pB := domain.PeerAddress("peerB")
	identA := identHex("a")
	identB := identHex("b")
	svc := &Service{
		peerIDs: map[domain.PeerAddress]domain.PeerIdentity{pA: identA, pB: identB},
	}

	env := protocol.Envelope{ID: "m1", Hops: 5}

	// Baseline: nothing acked → both targets pass through unchanged.
	got := svc.filterGossipTargetsForEnvelope(env, []domain.PeerAddress{pA, pB})
	if len(got) != 2 {
		t.Fatalf("baseline targets = %v, want both", got)
	}

	// Peer A acked m1 → excluded from the fan-out, B kept.
	svc.recordRelayDeliveredTo("m1", identA)
	got = svc.filterGossipTargetsForEnvelope(env, []domain.PeerAddress{pA, pB})
	if len(got) != 1 || got[0] != pB {
		t.Fatalf("after ack from A: targets = %v, want [peerB]", got)
	}
}
