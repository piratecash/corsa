package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// relay_delivered.go — per-(message, peer) "already has it" suppression
// (Phase 2 amplitude fix).
//
// Background: ack_delete is a BACKLOG-CLEANUP signal — a peer telling us
// "I already hold message id X, release the per-hop resource". For a DM
// addressed to that peer, handleAckDeleteFrame removes the envelope via
// deleteBacklogMessageForRecipient. But for a TRANSIT DM (recipient is a
// third party) that match never fires — the acking peer is just a relay
// hop that now has a copy — so the gossip fan-out kept re-sending the same
// id to a peer that had explicitly confirmed it (the dump showed an
// ack_delete after every push, ignored, then re-pushed). This index
// records those confirmations so filterGossipTargetsForEnvelope can skip
// a peer that already has the message, without deleting the envelope
// (other peers / the eventual recipient may still need it).
//
// Bounded: an entry is forgotten when its envelope leaves s.topics
// (forgetRelayDelivered is wired next to dropRelayRetryEntries on every
// expiry/eviction/delete path), and a hard cap guards against a leak if a
// forget path is ever missed.
//
// Locking: guarded by its OWN leaf mutex (s.relayDeliveredMu). It is only
// ever taken alone or as the innermost lock (snapshotted BEFORE peerMu in
// filterGossipTargetsForEnvelope), never while acquiring a domain mutex —
// so it adds no edge to the canonical domain lock order. See docs/locking.md.

// maxRelayDeliveredMessages caps how many distinct message ids carry a
// suppression set. Sized well above the live transit backlog (per-recipient
// 128 × typical recipients); hitting it means a forget path leaked, so we
// shed the signal rather than grow unbounded.
const maxRelayDeliveredMessages = 8192

// recordRelayDeliveredTo notes that peer `who` confirmed (via ack_delete)
// it already holds message `id`. Subsequent gossip fan-outs of `id` skip
// that peer. No-op for an empty id or zero identity.
func (s *Service) recordRelayDeliveredTo(id protocol.MessageID, who domain.PeerIdentity) {
	if id == "" || who.IsZero() {
		return
	}
	s.relayDeliveredMu.Lock()
	defer s.relayDeliveredMu.Unlock()
	if s.relayDeliveredTo == nil {
		s.relayDeliveredTo = make(map[protocol.MessageID]map[domain.PeerIdentity]struct{})
	}
	peers := s.relayDeliveredTo[id]
	if peers == nil {
		if len(s.relayDeliveredTo) >= maxRelayDeliveredMessages {
			return
		}
		peers = make(map[domain.PeerIdentity]struct{})
		s.relayDeliveredTo[id] = peers
	}
	peers[who] = struct{}{}
}

// relayDeliveredSnapshot returns a COPY of the identities that have
// confirmed `id`, or nil when none. Copying lets the caller consult it
// without holding relayDeliveredMu across the peerMu section in
// filterGossipTargetsForEnvelope (so the leaf lock never nests under
// peerMu). A concurrent record after the snapshot is benign: the
// freshly-acked peer is simply not excluded until the next fan-out.
func (s *Service) relayDeliveredSnapshot(id protocol.MessageID) map[domain.PeerIdentity]struct{} {
	s.relayDeliveredMu.Lock()
	defer s.relayDeliveredMu.Unlock()
	src := s.relayDeliveredTo[id]
	if len(src) == 0 {
		return nil
	}
	out := make(map[domain.PeerIdentity]struct{}, len(src))
	for k := range src {
		out[k] = struct{}{}
	}
	return out
}

// hasRelayRetryEntry reports whether we are actively relaying message id
// (a relayRetry bookkeeping entry exists). Used to gate ack_delete
// suppression recording: an authenticated peer must not be able to fill
// relayDeliveredTo with ack_deletes for ids we never relay (phantom ids),
// which would otherwise reach the hard cap and starve real entries. Takes
// deliveryMu.RLock alone — caller must hold no domain mutex.
func (s *Service) hasRelayRetryEntry(id protocol.MessageID) bool {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	_, ok := s.relayRetry[relayMessageKey(id)]
	return ok
}

// forgetRelayDelivered drops the suppression sets for envelopes that left
// s.topics (expired / evicted / acked-and-removed). Wired next to
// dropRelayRetryEntries so the two bounded side-indexes are pruned together.
func (s *Service) forgetRelayDelivered(ids []protocol.MessageID) {
	if len(ids) == 0 {
		return
	}
	s.relayDeliveredMu.Lock()
	defer s.relayDeliveredMu.Unlock()
	for _, id := range ids {
		delete(s.relayDeliveredTo, id)
	}
}
