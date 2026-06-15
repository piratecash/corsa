package node

import (
	"hash/fnv"
	"sort"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// gossip_fanout.go — deterministic K-of-N gossip fan-out (Phase 2.3).
//
// Background: gossip previously fanned every message out to ALL viable
// targets on every emission (and re-emission). In a settled mesh that is
// the dominant redundant traffic — each node re-broadcasts the same id to
// the same neighbours repeatedly. bitchat bounds this by relaying a
// broadcast to a DETERMINISTIC subset of K neighbours (seeded by the
// message id) rather than all N, relying on transitive coverage across
// hops; ANNOUNCE/SYNC still go to everyone. Corsa applies the same idea via
// filterGossipTargetsForEnvelope, which feeds BOTH the gossip push fan-out
// and the blind relay fallback (tryRelayToCapableFullNodes) — so the cap is a
// general blind-propagation limit, applied before the per-path capability
// filter (a small limit can thus exclude a relay-capable peer from a pass;
// see config.GossipFanoutLimit). The directed table relay to a KNOWN route
// does NOT go through this cap and remains the reliable primary path, so
// trimming the blind baseline removes redundant flood, not known-route delivery.
//
// The subset is seeded by the message id, so: (a) it is STABLE across
// retries of the same id (a peer either is or isn't a fan-out target for
// that id — no churn), and (b) different ids hash to different subsets, so
// load spreads across the neighbour set instead of always hitting the same
// K. Selection is pure and deterministic — fully unit-testable.
//
// Disabled by default (limit 0 = NO extra K-of-N cap; the legacy target
// selection is used unchanged — note it already bounds session fan-out via
// routingTargetsFiltered's maxTargets=3, so "0" is not literally "all peers").
// Topology-affecting, so it is opt-in: an operator turns it on
// (CORSA_GOSSIP_FANOUT_LIMIT) after confirming their mesh stays connected.

// gossipTargetsForRelay computes the gossip fan-out targets for a message,
// EXCLUDING the table-directed relay next-hop BEFORE the ingress/ack filter
// and the K-of-N cap are applied. Centralises the Phase 2.2 double-send
// suppression so every emission path (initial store, transit relay-retry,
// sender-owned retry) drops the redundant push_message to the peer that will
// receive the directed relay_message. Excluding the next hop FIRST also means
// the K-of-N subset is chosen from the peers that will ACTUALLY receive
// gossip, instead of being shrunk below the limit afterwards.
func (s *Service) gossipTargetsForRelay(env protocol.Envelope, decision RoutingDecision) []domain.PeerAddress {
	targets := decision.GossipTargets
	if decision.RelayNextHop != nil && protocol.IsDMTopic(env.Topic) && env.Recipient != "" && env.Recipient != "*" {
		// Drop the directed next-hop FIRST (by identity + canonical address),
		// so the K-of-N cap inside filterGossipTargetsForEnvelope is computed
		// over the peers that will actually receive gossip — not shrunk below
		// the limit afterwards.
		targets = s.excludeNextHopFromGossip(targets, *decision.RelayNextHop, decision.RelayNextHopAddress)
	}
	return s.filterGossipTargetsForEnvelope(env, targets)
}

// gossipFanoutLimit resolves the per-message K-of-N cap. Zero (the default)
// means no extra cap is applied on top of the legacy target selection (which
// is itself already bounded — see the file header), i.e. legacy behaviour.
func (s *Service) gossipFanoutLimit() int {
	if s.cfg.GossipFanoutLimit > 0 {
		return s.cfg.GossipFanoutLimit
	}
	return 0
}

// selectFanoutSubset returns up to `limit` targets chosen deterministically
// from `targets`, seeded by `seed` (the message id). limit<=0 or a target
// count already within the limit returns the input unchanged. Selection is
// by ascending FNV-1a hash of (seed, addr) with the address as a stable
// tie-break, so the same (seed, target-set) always yields the same subset
// and distinct seeds spread across the set.
func selectFanoutSubset(targets []domain.PeerAddress, limit int, seed protocol.MessageID) []domain.PeerAddress {
	if limit <= 0 || len(targets) <= limit {
		return targets
	}
	type scored struct {
		addr domain.PeerAddress
		h    uint64
	}
	arr := make([]scored, len(targets))
	for i, t := range targets {
		hsh := fnv.New64a()
		_, _ = hsh.Write([]byte(seed))
		_, _ = hsh.Write([]byte{0}) // separator so seed/addr boundary is unambiguous
		_, _ = hsh.Write([]byte(t))
		arr[i] = scored{addr: t, h: hsh.Sum64()}
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].h != arr[j].h {
			return arr[i].h < arr[j].h
		}
		return arr[i].addr < arr[j].addr
	})
	out := make([]domain.PeerAddress, limit)
	for i := 0; i < limit; i++ {
		out[i] = arr[i].addr
	}
	return out
}
