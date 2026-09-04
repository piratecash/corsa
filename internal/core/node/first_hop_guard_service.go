package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// first_hop_guard_service.go connects the guard set to the node: it reads the
// live neighbourhood, and it turns the set's answer into the preference a local
// send carries.
//
// The split is deliberate. first_hop_guards.go is the policy and knows nothing
// about sessions, locks or the datagram layer; this file is the only place that
// does, so the policy stays testable without a Service and the lock discipline
// stays in one readable function.

// firstHopGuardCandidates lists the neighbours a locally created datagram could
// be handed to right now.
//
// Eligibility is not re-derived here: it asks peerSendableConnectionsLocked,
// the same helper the live send path uses, so the guard set cannot sample a
// neighbour the send would never reach. A guard chosen from a different notion
// of "usable" would silently degrade into no preference at all — the send would
// skip it every time and take the ranking's first candidate instead, which
// looks exactly like the policy working.
//
// Lock: takes s.peerMu (R) and returns values only. No I/O and no publication
// happen under it — the set's own mutex and its disk write are reached after
// this returns, per docs/locking.md.
func (s *Service) firstHopGuardCandidates() []guardCandidate {
	// Wall(): the guard set is persisted and diagnosed in calendar time; it
	// measures no presence interval.
	now := s.presenceNow().Wall()

	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	neighbours := make(map[domain.PeerIdentity]struct{})
	s.forEachConnLocked(func(info connInfo) bool {
		if !info.identity.IsZero() {
			neighbours[info.identity] = struct{}{}
		}
		return true
	})

	out := make([]guardCandidate, 0, len(neighbours))
	for peer := range neighbours {
		conns := s.peerSendableConnectionsLocked(peer, domain.CapMeshDatagramV1, now)
		if len(conns) == 0 {
			continue
		}
		candidate := guardCandidate{Identity: peer}
		for _, conn := range conns {
			if candidate.ConnectedAt.IsZero() || conn.connectedAt.Before(candidate.ConnectedAt) {
				candidate.ConnectedAt = conn.connectedAt
			}
			// outbound == nil is the discriminator the send path itself uses:
			// an entry with no session behind it is an INBOUND connection,
			// one the neighbour dialled to us. That is the direction where our
			// identity was never proven to them (§5.1), so a peer holding one
			// counts as inbound for the admission preference even if it also
			// holds an outbound session.
			if conn.outbound == nil {
				candidate.Inbound = true
			}
		}
		out = append(out, candidate)
	}
	return out
}

// firstHopGuardEntries is the set as it stands, for the peer-state writer and
// the diagnostic. Nil-safe: a node built without a guard set persists nothing
// rather than making its single file writer conditional.
func (s *Service) firstHopGuardEntries() []guardEntry {
	if s.firstHopGuards == nil {
		return nil
	}
	return s.firstHopGuards.Entries()
}

// firstHopGuardOwner is the identity the persisted guard set belongs to. Empty
// when this node has no identity yet, which makes the rows unreadable on the
// next start — the safe direction, since a set nobody can be shown to own is
// one that must not be reused.
func (s *Service) firstHopGuardOwner() string {
	if s.identity == nil {
		return ""
	}
	return domain.PeerIdentityFromWire(s.identity.Address).String()
}

// preferredFirstHops is what a local send should carry: the guard set's answer,
// in its order, as the layer's preference type.
//
// An empty preference is a correct answer and not a failure. It means this node
// has no usable guard right now — a fresh start, or every guard in back-off —
// and the ranking decides that one send. Refusing to send instead would turn a
// privacy policy into an outage, which is the trade named in
// PreferredFirstHops.
func (s *Service) preferredFirstHops() datagram.PreferredFirstHops {
	if s.firstHopGuards == nil {
		return datagram.NoFirstHopPreference()
	}
	return datagram.PreferFirstHops(s.firstHopGuards.Pick(s.firstHopGuardCandidates())...)
}

// noteFirstHopCarried records that a neighbour demonstrably carried a frame of
// ours all the way to its destination.
//
// The evidence is an ANSWER, not a send. Three things sit between "the layer
// took the frame" and "the neighbour saw it" — the class queue, its send
// deadline, and the writer, none of which reports back (queue.go: "the layer
// keeps no per-send record") — so a `queued` outcome cannot confirm anything.
// A verified reply that came back through this hop can: the frame reached the
// target and the answer found its way home along the reverse crumbs.
//
// Confirming earlier is the mistake Tor names explicitly: it commits to a
// guard before any sensitive traffic has actually crossed it.
func (s *Service) noteFirstHopCarried(nextHop domain.PeerIdentity) {
	if s.firstHopGuards == nil {
		return
	}
	// The live neighbour count travels with the call because admitting a hop
	// that is not yet in the set is subject to the FRACTIONAL cap as well as
	// the absolute one, and only the caller can see the current neighbourhood.
	s.firstHopGuards.NoteUsed(nextHop, len(s.firstHopGuardCandidates()))
}

// noteFirstHopsPassedOver records the guards a send OFFERED the frame to and
// that did not take it.
//
// `attempted` comes from the walk itself, and nothing else will do. The
// preference list is not it: the walk stops at the first acceptance, so guards
// listed after the winner were never asked, and a guard with no route to this
// destination never entered the candidate list at all. Blaming those was a
// false back-off on a working neighbour — and, because a guard in back-off is
// skipped, the set then topped itself up with somebody new. The policy would
// have widened its own exposure out of an accounting error.
//
// Our own connectivity gates the whole thing, because with our network down
// every neighbour looks dead and blaming them one by one is how a client
// rebuilds its entire set out of a single local outage.
func (s *Service) noteFirstHopsPassedOver(attempted []domain.PeerIdentity, actual domain.PeerIdentity) {
	if s.firstHopGuards == nil || len(attempted) == 0 {
		return
	}
	connectivity := s.presenceLocalConnectivity()
	for _, hop := range attempted {
		if hop == actual {
			// Everything after the winner was never offered the frame.
			return
		}
		s.firstHopGuards.NoteUnusable(hop, connectivity)
	}
}

// firstHopGuardFrame answers the local RPC "fetch_first_hop_guards".
//
// The entries alone would not answer the question this exists for. "Is the
// policy holding" is a question about RATES — how often the leading hop
// changed, how far the set grew, how much traffic left through a neighbour
// outside it — and every one of those failures leaves a perfectly ordinary
// looking set behind. So the counters travel with it.
func (s *Service) firstHopGuardFrame() protocol.Frame {
	stats := protocol.FirstHopGuardStatsFrame{
		Cap:           maxSampledGuards,
		PrimaryTarget: guardPrimaryCount,
	}
	if s.firstHopGuards == nil {
		return protocol.Frame{Type: "first_hop_guards", FirstHopGuardStats: &stats}
	}

	// ONE read-only pass. Building this from Pick would make the diagnostic a
	// mutator: it would sample new guards, move the very counters printed
	// above it, and persist — so the first request could report rows it had
	// just created, next to an `admitted` count read before it created them.
	entries, primary, counters := s.firstHopGuards.Inspect(s.firstHopGuardCandidates())
	stats.Admitted = counters.Admitted
	stats.Confirmed = counters.Confirmed
	stats.PrimaryChanges = counters.PrimaryChanges
	stats.BackOffs = counters.BackOffs
	stats.OutsideSetUses = counters.OutsideSetUses
	stats.Retired = counters.Retired

	rows := make([]protocol.FirstHopGuardFrame, 0, len(entries))
	for _, entry := range entries {
		row := protocol.FirstHopGuardFrame{
			Identity:     entry.Identity.String(),
			SampledAt:    guardTimeString(entry.SampledAt),
			ConfirmedAt:  guardTimeString(entry.ConfirmedAt),
			ConfirmedSeq: entry.ConfirmedSeq,
			RetryAt:      guardTimeString(entry.RetryAt),
			Failures:     entry.Failures,
			Inbound:      entry.Inbound,
		}
		_, row.Primary = primary[entry.Identity]
		rows = append(rows, row)
	}
	return protocol.Frame{
		Type:               "first_hop_guards",
		FirstHopGuards:     rows,
		FirstHopGuardStats: &stats,
	}
}

// guardTimeString renders a stored date, or nothing for the zero value —
// which in this set means "never", not "the epoch".
func guardTimeString(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339)
}
