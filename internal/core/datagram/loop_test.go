package datagram

import (
	"context"
	"fmt"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// loop_test.go is the "Loops" row of §9 line 1094, which the package had no
// test for at all:
//
//   - a ring of honest nodes goes out by ttl, with no more forwards than the
//     ttl allows, and the frame never comes back to its originator;
//   - a relay that resets ttl = max_ttl does NOT thereby escape the
//     per-neighbour budgets or the anti-replay — the counters keep ticking and
//     the ban for header violations still applies.

// ringTopology builds a closed ring of `size` transit nodes. Every node routes
// `dst` — a destination NOBODY holds — through its successor, which is what
// turns an honest routing table into a loop.
func ringTopology(t *testing.T, net *fakeNetwork, size int, dst domain.PeerIdentity) []*pipelineNode {
	t.Helper()
	nodes := make([]*pipelineNode, 0, size)
	for i := 0; i < size; i++ {
		nodes = append(nodes, newPipelineNode(t, net, nodeOpts{
			name:    fmt.Sprintf("ring-%02d", i),
			transit: true,
		}))
	}
	for i := 0; i < size; i++ {
		next := nodes[(i+1)%size]
		link(nodes[i], next, true, true)
	}
	for i := 0; i < size; i++ {
		route(nodes[i], dst, nodes[(i+1)%size].id, 1)
	}
	return nodes
}

// TestRingOfHonestNodesDiesByTTL pins the first half of §9 line 1094.
//
// The delivery in this fixture is SYNCHRONOUS and re-entrant, so the whole
// loop plays out inside the originating SendLocal call — which is the harshest
// version of the scenario: nothing but the ttl and the anti-replay can stop it.
//
// Two ring sizes, because the two clauses of the rule are stopped by different
// mechanisms and a single size would let one of them hide behind the other.
func TestRingOfHonestNodesDiesByTTL(t *testing.T) {
	t.Run("a ring longer than the hop budget dies by ttl", func(t *testing.T) {
		// Longer than defaultMaxHops, so the frame provably runs out of budget
		// before it could ever come back round.
		const size = 12
		net := newFakeNetwork()
		dst := domaintest.ID("nobody-holds-this")
		ring := ringTopology(t, net, size, dst)

		private, signer := newSigner(t)
		outcome := ring[0].pipeline.SendLocal(context.Background(), LocalSendOpts{
			Frame: signedRouted(t, routedOpts{
				private: private, src: signer, dst: dst, now: ring[0].clock(),
			}),
		})
		if outcome.Kind() != SendQueued {
			t.Fatalf("the first hop refused the frame: %s", outcome)
		}

		journal := net.journal()
		// The origin does not decrement, every relay does exactly once, and a
		// frame whose decrement would reach zero is not handed on at all — so
		// the hand-overs can never exceed the budget it started with.
		if len(journal) > int(OriginTTL()) {
			t.Fatalf("the ring produced %d forwards, ttl allows at most %d", len(journal), OriginTTL())
		}
		if len(journal) < int(OriginTTL()) {
			t.Fatalf("the ring stopped after %d forwards, the budget was %d", len(journal), OriginTTL())
		}

		// The ttl strictly decreases along the loop and no frame is ever
		// handed over with nothing left to spend.
		previous := int(OriginTTL()) + 1
		for i, event := range journal {
			ttl := int(event.frame.TTL)
			if ttl <= 0 {
				t.Fatalf("hand-over %d left with ttl = 0", i)
			}
			if ttl >= previous {
				t.Fatalf("hand-over %d left with ttl %d, previous was %d", i, ttl, previous)
			}
			previous = ttl
		}
	})

	t.Run("a short ring cannot hand the frame back to its originator", func(t *testing.T) {
		net := newFakeNetwork()
		dst := domaintest.ID("nobody-holds-this")
		ring := ringTopology(t, net, 4, dst)
		origin := ring[0]

		private, signer := newSigner(t)
		outcome := origin.pipeline.SendLocal(context.Background(), LocalSendOpts{
			Frame: signedRouted(t, routedOpts{
				private: private, src: signer, dst: dst, now: origin.clock(),
			}),
		})
		if outcome.Kind() != SendQueued {
			t.Fatalf("the first hop refused the frame: %s", outcome)
		}

		// The frame does come back round — split horizon only excludes the
		// neighbour it arrived from — and the ANTI-REPLAY is what refuses it:
		// the origin reserved and committed the key when it created the frame.
		origin.mu.Lock()
		inbound := append([]InboundResult(nil), origin.inbound...)
		origin.mu.Unlock()
		if len(inbound) == 0 {
			t.Fatal("the ring never closed, so the rule under test was not exercised")
		}
		for _, result := range inbound {
			if !result.Dropped() {
				t.Fatalf("the originator accepted its own frame back: %s", result.Outcome())
			}
			if result.Reason() != DropReplayDuplicate {
				t.Fatalf("the originator dropped its own frame for %s, want the anti-replay",
					result.Reason())
			}
		}
		if len(net.journal()) > int(OriginTTL()) {
			t.Fatalf("the ring produced %d forwards, ttl allows at most %d",
				len(net.journal()), OriginTTL())
		}
	})
}

func TestTTLResettingRelayEscapesNeitherBudgetsNorAntiReplay(t *testing.T) {
	net := newFakeNetwork()
	dst := domaintest.ID("nobody-holds-this")
	hostile := newPipelineNode(t, net, nodeOpts{name: "hostile", transit: true})
	victim := newPipelineNode(t, net, nodeOpts{name: "victim", transit: true})
	onward := newPipelineNode(t, net, nodeOpts{name: "onward", transit: true})
	link(hostile, victim, true, true)
	link(victim, onward, true, true)
	route(victim, dst, onward.id, 2)

	private, signer := newSigner(t)
	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: dst, now: victim.clock(),
	})

	// Lap one: the honest frame passes through and is forwarded.
	requireOutcome(t, victim.deliver(t, hostile.id, frame), InboundForwarded)

	// Lap two: the hostile relay hands the SAME signed frame back with a
	// restored hop budget. `ttl` is not part of the transcript, so the
	// signature still verifies and only the layer's own rules can stop it.
	restored := frame.Clone()
	restored.TTL = frame.Auth.MaxTTL
	requireDrop(t, victim.deliver(t, hostile.id, restored), DropReplayDuplicate)

	// Stage one of §5 does not live in the conveyor — it is charged by the owner
	// of the receive path, and that a replayed lap is charged like any other
	// frame is pinned where the charge is (node: datagram_budget_key_test.go).
	// What this test owns is the stage the conveyor does own: the anti-replay
	// stopped the second lap BEFORE a second verification was paid for.
	if victim.crypto.charged() != 1 {
		t.Fatalf("verifications = %d, want 1 — the replay must die before cryptography",
			victim.crypto.charged())
	}
	if events := framesTo(net, onward.id); events != 1 {
		t.Fatalf("the victim forwarded %d frames onward, want 1 — the second lap must not travel", events)
	}

	// A header violation on a looping frame is still punishable: the ban rule
	// of §4.4 is not weakened by the loop.
	inflated := signedRouted(t, routedOpts{
		private: private, src: signer, dst: dst, now: victim.clock(),
	})
	inflated.TTL = inflated.Auth.MaxTTL + 1
	result := victim.deliver(t, hostile.id, inflated)
	requireDrop(t, result, DropTTLBudget)
	if !result.BanWorthy() {
		t.Fatal("`ttl > max_ttl` on a looping frame is still a punishable header violation")
	}
}

// framesTo counts the frames the fake transport carried to one peer.
func framesTo(net *fakeNetwork, peer domain.PeerIdentity) int {
	seen := 0
	for _, event := range net.journal() {
		if event.to == peer {
			seen++
		}
	}
	return seen
}
