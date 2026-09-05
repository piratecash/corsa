package datagram

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// candidates.go builds and ranks the next-hop list of §4.3.
//
// The order of operations is normative and carried over from the file
// router without a change of behaviour, because "one deterministic
// comparator" is too vague to preserve anything:
//
//  1. capability filter (§6, §2.2 rule 2) BEFORE the sort, never as a
//     penalty;
//  2. exclusions: the neighbour the frame came from (split-horizon),
//     routes to self, the destination already tried by the direct branch,
//     expired / withdrawn / stalled entries;
//  3. deduplication by next hop, choosing the better entry with the SAME
//     comparator as the final sort;
//  4. sort: protocolVersion DESC → hops ASC → connectedAt ASC (zero last,
//     meaning unknown) → next hop lexicographic.
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §6, §2.2.

// versionInflationWarnGap is how far above the local protocol version a
// peer may claim before the cap is logged as a probable misconfiguration or
// a traffic-capture attempt rather than a staged rollout.
//
// The value matches the file router's: alpha releases bump the protocol
// version every few weeks, so four versions cover roughly half a year of
// rollout drift. Below the gap the cap still fires — only the log level
// differs, because normalization runs once per candidate per frame and a
// single neighbour one version ahead would otherwise flood the journal.
const versionInflationWarnGap = 4

// versionNormalizer implements the mandatory version normalization of §4.3:
// the handshake value is reported by the peer itself and may claim v999, so
// ranking uses min(reported, local) while the raw value is kept apart for
// diagnostics.
//
// It caps rather than zeroes. Zeroing neutralised the same attack but broke
// staged rollout: the single upgraded node was pushed behind every legacy
// peer on the primary key and starved of traffic. The cap collapses the
// inflated peer into the local-version tier, where hops and uptime decide.
type versionNormalizer struct {
	local domain.ProtocolVersion
}

func (n versionNormalizer) normalize(peer domain.PeerIdentity, reported domain.ProtocolVersion) domain.ProtocolVersion {
	if reported <= n.local {
		return reported
	}
	event := log.Debug()
	if int(reported)-int(n.local) > versionInflationWarnGap {
		event = log.Warn()
	}
	event.
		Str("peer", peer.String()).
		Int("reported_version", int(reported)).
		Int("local_version", int(n.local)).
		Msg("datagram: peer reports newer protocol version, capping at local for ranking")
	return n.local
}

// ---------------------------------------------------------------------------
// Candidate
// ---------------------------------------------------------------------------

// RouteCandidate is one ranked next hop together with the keys that ranked
// it. The fields are unexported and read through accessors so a caller
// cannot rewrite a ranking key after the fact and make the plan disagree
// with the send.
type RouteCandidate struct {
	connectedAt        time.Time
	attribution        domain.RouteAttribution
	nextHop            domain.PeerIdentity
	channel            ChannelID
	hops               int
	protocolVersion    domain.ProtocolVersion
	rawProtocolVersion domain.ProtocolVersion
	direct             bool
}

// NextHop is the peer the frame would be handed to.
func (c RouteCandidate) NextHop() domain.PeerIdentity { return c.nextHop }

// Channel is the connection the ranking keys describe — the one PeerConnection
// named. The bool is false for a resolver that does not name its connections.
//
// It is read by the plane that must ADDRESS the connection rather than merely
// rank it: a forwarded request is pinned to this channel and the channel goes
// into the reverse record, so an answer can be checked against the socket the
// question left over instead of against the name of whoever the question named
// (§4.2). The routed plane ignores it — there the transport chooses the socket
// among all the peer's gated connections (§4.3).
func (c RouteCandidate) Channel() (ChannelID, bool) {
	if c.channel.IsZero() {
		return ChannelID{}, false
	}
	return c.channel, true
}

// Hops is the distance to the destination through this candidate. The
// synthetic direct candidate reports 1: a direct send is one network hop
// away, which puts it on the same scale as relay entries.
func (c RouteCandidate) Hops() int { return c.hops }

// ProtocolVersion is the NORMALIZED ranking key — min(reported, local).
func (c RouteCandidate) ProtocolVersion() domain.ProtocolVersion { return c.protocolVersion }

// RawProtocolVersion is what the peer actually claimed, kept for
// diagnostics. It differs from ProtocolVersion only when the peer reported
// a version above the local build's.
func (c RouteCandidate) RawProtocolVersion() domain.ProtocolVersion { return c.rawProtocolVersion }

// ConnectedAt is when the chosen connection was established; the zero value
// means unknown.
func (c RouteCandidate) ConnectedAt() time.Time { return c.connectedAt }

// IsDirect reports whether this is the direct session to the destination
// promoted by step 1 of §4.3 rather than a routing-table entry.
func (c RouteCandidate) IsDirect() bool { return c.direct }

// Attribution is the two-axis record of who says this route exists and which
// plane found it. It is carried, logged and rendered — and it is deliberately
// absent from routeCandidateLess: which plane answered decides the order in
// which sources are ASKED (CompositeRouteResolver), never how the answers are
// ranked against each other.
func (c RouteCandidate) Attribution() domain.RouteAttribution { return c.attribution }

// routeCandidateLess is the single total order behind BOTH the dedup branch
// and the final sort. Keeping the keys in one function is what makes the
// dedup safe: picking "the better of two entries for the same next hop"
// with a different order than the sort would let the two disagree about
// which route is best.
func routeCandidateLess(a, b RouteCandidate) bool {
	// 1. protocolVersion DESC — a newer protocol unlocks features an older
	// path may silently drop, so it wins even at the cost of a hop.
	if a.protocolVersion != b.protocolVersion {
		return a.protocolVersion > b.protocolVersion
	}
	// 2. hops ASC — among equal versions, fewer relays touch the bytes.
	if a.hops != b.hops {
		return a.hops < b.hops
	}
	// 3. connectedAt ASC — longer uptime is empirically more stable. A
	// zero timestamp is "unknown" and sorts after every known one, so a
	// peer with real uptime always beats one we have no health data for.
	if a.connectedAt.IsZero() != b.connectedAt.IsZero() {
		return !a.connectedAt.IsZero()
	}
	if !a.connectedAt.Equal(b.connectedAt) {
		return a.connectedAt.Before(b.connectedAt)
	}
	// 4. next hop lexicographic — deterministic final tie-break.
	return a.nextHop.Compare(b.nextHop) < 0
}

// sortCandidates orders the slice in place with routeCandidateLess.
// Insertion sort is deliberate: candidate sets are tiny, the sort is
// stable, and it allocates nothing on the hot path.
func sortCandidates(candidates []RouteCandidate) {
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && routeCandidateLess(candidates[j], candidates[j-1]); j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}
}

// ---------------------------------------------------------------------------
// The gates a peer must pass to receive a frame
// ---------------------------------------------------------------------------

// RejectionReason names why policy refused to send. It exists because
// "rejected" alone tells an operator nothing about what the network expects
// this node — or its destination — to support.
type RejectionReason uint8

const (
	// RejectionUnset is the zero value: no policy refusal happened.
	RejectionUnset RejectionReason = iota
	// RejectionMissingCapability means the peer does not advertise
	// mesh_datagram_v1, or mesh_datagram_transit_v1 in the transit role.
	RejectionMissingCapability
	// RejectionUnsupportedDType means the destination is the peer we would
	// hand the frame to and it never declared this dtype (§6.1) — the
	// last-hop gate of §4.3.
	RejectionUnsupportedDType
)

var rejectionReasonNames = map[RejectionReason]string{
	RejectionUnset:             "unset",
	RejectionMissingCapability: "missing_capability",
	RejectionUnsupportedDType:  "unsupported_dtype",
}

// String returns the metric label of the reason.
func (r RejectionReason) String() string { return enumName(rejectionReasonNames, r) }

// peerAdmission is the combined verdict of both gates a peer must pass.
type peerAdmission struct {
	missing  domain.CapabilityName
	reason   RejectionReason
	role     CandidateRole
	admitted bool
}

// admitPeer applies BOTH gates, in the order §4.3 fixes:
//
//  1. the role gate — AdmitCandidate from gates.go: mesh_datagram_v1
//     always, mesh_datagram_transit_v1 unless the peer IS the destination;
//  2. the last-hop dtype gate — ALWAYS, not "for mandatory migrations
//     only": if the peer we would send to is the destination, the dtype must
//     appear in the set it declared.
//
// One implementation for the direct branch, the routing-table candidates
// and the reachability probe. That is the point: the probe's guarantee
// ("unreachable means the send would not have been queued") holds only
// while the two run the same code.
func admitPeer(frame protocol.DatagramFrame, peer domain.PeerIdentity, conn PeerConnection) peerAdmission {
	decision := AdmitCandidate(frame, peer, conn.Advertised)
	if !decision.Admitted() {
		missing, _ := decision.Missing()
		return peerAdmission{
			reason:  RejectionMissingCapability,
			missing: missing,
			role:    decision.Role(),
		}
	}
	if decision.Role() == CandidateRoleLastHop && !conn.DTypes.Supports(frame.DType) {
		return peerAdmission{
			reason: RejectionUnsupportedDType,
			role:   decision.Role(),
		}
	}
	return peerAdmission{admitted: true, role: decision.Role()}
}

// AdmitPeer reports whether this frame may be handed to this peer over this
// connection, applying BOTH gates of §4.3 in their fixed order.
//
// It exists because the decision has a second caller outside the layer, and
// that caller now has two duties rather than one. PeerMetadata must hand back
// the FIRST connection of a peer that may carry this frame (§4.3 line 574 —
// "the metadata describes the connection the send will try"), and the emitter
// must refuse every socket that may not, so no fall-back connection carries
// bytes nobody gated. Both duties ask this one question, and a second spelling
// of the rule is how one copy silently loses a check the day a third gate is
// added — so the layer exports its own decision instead of letting the node
// re-derive it. The layer still re-applies the gates over whatever connection
// comes back: the node picks WHICH connection is described, the layer decides
// whether it is admitted, and neither answer is derived from the other.
//
// The reason is not returned: a caller choosing between sockets of one peer
// acts identically on every refusal, and the diagnostic reason belongs to the
// scheduler, which reports it as the send outcome.
func AdmitPeer(frame protocol.DatagramFrame, peer domain.PeerIdentity, conn PeerConnection) bool {
	return admitPeer(frame, peer, conn).admitted
}

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------

// refusal records that policy — not the absence of routes — is what stopped
// a peer from receiving the frame. The scheduler needs the distinction to
// answer `rejected` instead of `no_route`: the first is pointless to repeat
// without changing conditions, the second is worth waiting on.
type refusal struct {
	missing domain.CapabilityName
	reason  RejectionReason
	set     bool
}

func (r *refusal) record(a peerAdmission) {
	if r.set {
		return
	}
	r.set = true
	r.reason = a.reason
	r.missing = a.missing
}

// merge folds another refusal in, keeping the first one seen: the direct
// branch runs before the routing table, so its verdict is the one an
// operator needs to read first.
func (r *refusal) merge(other refusal) {
	if r.set || !other.set {
		return
	}
	*r = other
}

// selectionOpts are the per-call knobs of one candidate selection.
type selectionOpts struct {
	// incomingPeer is where the frame came from. A local ingress means a
	// locally created frame: fresh lookup, no split-horizon. A remote one
	// is the split-horizon exclusion (exclude_via).
	incomingPeer IngressPeer
	// avoid is the local avoid_next_hop parameter. It is applied BEFORE
	// the direct branch, so a retry cannot land on the same first hop
	// through the direct session.
	avoid AvoidedNextHop
	// firstHop is the local guard preference: listed neighbours are hoisted
	// to the front of the ranked list, in the caller's order. Never a filter
	// — see PreferredFirstHops.
	firstHop PreferredFirstHops
	// rotate enables the explore rotation and its counter increment. The
	// read-only plan leaves it false: it shows the comparator order, and
	// it must not move a counter.
	rotate bool
	// directTried marks that the direct branch already offered the frame to
	// the destination, which is what turns "next_hop == dst" into a duplicate
	// candidate rather than the only remaining way to reach it (§4.3 item 2).
	directTried bool
}

// candidateSelection is the ordered list a send would walk, plus the policy
// verdict that shaped it.
type candidateSelection struct {
	candidates []RouteCandidate
	refusal    refusal
	// aborted marks a HARD policy stop: the destination has a live direct
	// session and failed the last-hop dtype gate. Relaying around it would
	// only move the silent drop to the destination, and it would make the
	// probe (which must call such a destination unreachable) disagree with
	// the send.
	aborted bool
}

// publishable reports whether the walk has anything to try. It is the ONE
// definition of "there is somebody to hand the frame to": the reservation of
// the replay key is taken only when it answers true, because §4.1 forbids an
// empty candidate list from occupying a reservation slot even briefly.
func (s candidateSelection) publishable() bool {
	return !s.aborted && len(s.candidates) > 0
}

// outcomeWithoutCandidates names WHY the walk had nothing to try, and §4.3
// makes that two different answers: a gate verdict is `rejected` — repeating
// it without changed conditions is pointless — while the plain absence of a
// route is `no_route`, worth waiting on. Collapsing them would make the
// reachability probe, which must call a dtype-refusing destination
// unreachable, disagree with the send it exists to predict.
func (s candidateSelection) outcomeWithoutCandidates(local bool) SendOutcome {
	if s.refusal.set {
		return rejectedOutcome(local, s.refusal)
	}
	return noRouteOutcome(local)
}

// candidateSelector turns raw route hints into the ranked list.
type candidateSelector struct {
	clock    func() time.Time
	peers    PeerMetadata
	localID  domain.PeerIdentity
	versions versionNormalizer
}

// sendableConnection is the ONE call into the node's per-connection metadata,
// boundary included.
//
// A panic becomes "no usable connection", which is the answer this seam already
// defines for a peer the node cannot send to at all: the hop is skipped, the
// walk continues to the next candidate, and the frame ends as `no_route` only
// if every candidate answers the same. Letting the panic unwind instead would
// take the session reader out mid-selection, with the frame's reservation and
// its reverse record still held by the caller.
func (s candidateSelector) sendableConnection(
	ctx context.Context,
	hop domain.PeerIdentity,
	frame protocol.DatagramFrame,
) (PeerConnection, bool) {
	site := hookSite{hook: "SendableConnection", peer: hop, dtype: frame.DType}
	return guardHookPair(site, PeerConnection{}, func() (PeerConnection, bool) {
		return s.peers.SendableConnection(ctx, hop, frame)
	})
}

// peerConnectionResult memoizes one PeerMetadata lookup for the duration of
// a selection pass: several route entries usually collapse onto the same
// next hop, and the node-side lookup takes a domain lock.
type peerConnectionResult struct {
	conn     PeerConnection
	ok       bool
	resolved bool
}

// rank applies the four steps of §4.3 to the raw hints.
//
// A route whose next hop IS the destination is dropped only when the direct
// branch ALREADY TRIED it (opts.directTried) — which is exactly how §4.3
// item 2 words the exclusion. While DirectSession and PeerMetadata answer
// from one node-side helper the two readings coincide; the moment they
// disagree, an unconditional drop makes the destination unreachable through
// BOTH paths at once — no session for the direct branch, no route for the
// ranked one. The gates are unchanged either way: such a hop is the last hop,
// so admitPeer applies the same role and dtype checks the direct branch would
// have applied, and the two exclusions of avoid_next_hop and split-horizon are
// enforced below for every hop including this one.
func (s candidateSelector) rank(
	ctx context.Context,
	frame protocol.DatagramFrame,
	hints []RouteHint,
	opts selectionOpts,
) ([]RouteCandidate, refusal) {
	var refused refusal
	if len(hints) == 0 {
		return nil, refused
	}

	// Expiry is judged against the CURRENT clock, not against the moment
	// the snapshot was published: the cached snapshot republishes on a
	// dirty flag, so a finite-TTL route that aged out between publishes
	// still looks alive in it. At worst this drops a candidate that is in
	// fact still alive — the next republish brings it back — but a frame
	// never leaves through a route already dead by the wall clock.
	now := s.clock()
	excludeVia, hasExcludeVia := splitHorizonExclusion(opts.incomingPeer)

	candidates := make([]RouteCandidate, 0, len(hints))
	byNextHop := make(map[domain.PeerIdentity]int, len(hints))
	connCache := make(map[domain.PeerIdentity]peerConnectionResult, len(hints))

	for i := range hints {
		hint := hints[i]
		if hint.Withdrawn || hint.IsExpired(now) {
			continue
		}
		if hint.NextHop == s.localID {
			continue
		}
		if hint.NextHop == frame.Dst && opts.directTried {
			continue
		}
		if hasExcludeVia && hint.NextHop == excludeVia {
			continue
		}
		if opts.avoid.Excludes(hint.NextHop) {
			continue
		}
		result := connCache[hint.NextHop]
		if !result.resolved {
			result.conn, result.ok = s.sendableConnection(ctx, hint.NextHop, frame)
			result.resolved = true
			connCache[hint.NextHop] = result
		}
		if !result.ok {
			continue
		}
		admission := admitPeer(frame, hint.NextHop, result.conn)
		if !admission.admitted {
			refused.record(admission)
			continue
		}
		candidate := s.newRoutedCandidate(hint, result.conn)
		if idx, exists := byNextHop[hint.NextHop]; exists {
			if routeCandidateLess(candidate, candidates[idx]) {
				candidates[idx] = candidate
			}
			continue
		}
		byNextHop[hint.NextHop] = len(candidates)
		candidates = append(candidates, candidate)
	}

	sortCandidates(candidates)
	return candidates, refused
}

// splitHorizonExclusion is the next hop a frame must not be handed back to: the
// neighbour it arrived from.
//
// It reads the PRESENTED name and not the proven one, and that is a deliberate
// weakening with a bounded cost. The exclusion can only ever REMOVE a candidate,
// never admit one, so a neighbour that presents somebody else's fingerprint on a
// dialled session can at most take one hop out of the candidate set of the
// frames it is itself relaying — a self-restriction, not an escalation. Reading
// only proven names would cost the opposite and worse: on a dialled session
// there would be no exclusion at all, and the honest split horizon this rule
// exists for would stop working exactly where a two-node loop is cheapest.
//
// The COMPLETE rule is channel-level — "never hand the frame back over the
// channel it came in on" — and it is still not expressed here, though for a
// narrower reason than before. A candidate now NAMES a channel when the resolver
// supplies one (RouteCandidate.Channel), so the comparison is expressible for a
// pinned plane; what is missing is the other half, since on the routed plane the
// transport still picks among all of the peer's gated sockets after the layer
// has handed the frame over, and OutboundFrame.Channel pins a frame without
// having an `avoid` twin. Excluding by name here is therefore the exclusion that
// holds for both planes.
func splitHorizonExclusion(incoming IngressPeer) (domain.PeerIdentity, bool) {
	via, _ := incoming.PresentedIdentity()
	if via.IsZero() {
		return domain.PeerIdentity{}, false
	}
	return via, true
}

// newRoutedCandidate builds the candidate of one routing hint. The
// attribution is the resolver's — the layer neither invents nor overrides it.
func (s candidateSelector) newRoutedCandidate(hint RouteHint, conn PeerConnection) RouteCandidate {
	return s.newCandidate(hint.NextHop, hint.Hops, conn, false, hint.Attribution)
}

// newDirectCandidate builds the synthetic candidate of step 1 — the
// destination itself, reached over a live session.
//
// It reports 1 hop: a direct send is one network hop away, which puts it on
// the same scale as relay entries. Its attribution comes from
// directRouteAttribution rather than from a hint, because this branch never
// consults the routing table.
func (s candidateSelector) newDirectCandidate(dst domain.PeerIdentity, conn PeerConnection) RouteCandidate {
	return s.newCandidate(dst, 1, conn, true, directRouteAttribution(conn))
}

// directRouteAttribution states both axes of a live direct session.
//
// The trust axis is fixed by construction — a session we hold IS
// RouteSourceDirect — while the discovery axis is whatever the node said about
// how this connection came to exist. The two are set together here and nowhere
// else, so "the overlay found it and it is nevertheless a direct session"
// survives as ONE value instead of collapsing into either half.
//
// A connection that names no plane produces no attribution at all. Defaulting
// it to mesh would be a claim nobody made, and the unattributed value is the
// one the diagnostics can render as absence.
func directRouteAttribution(conn PeerConnection) domain.RouteAttribution {
	switch conn.Discovery {
	case domain.DiscoveryPlaneMesh:
		return domain.MeshRouteAttribution(domain.RouteSourceDirect)
	case domain.DiscoveryPlaneOverlay:
		return domain.OverlayRouteAttribution(domain.RouteSourceDirect)
	default:
		return domain.UnattributedRoute()
	}
}

// newCandidate builds a candidate from a connection, applying the version
// normalization on the way in so no path can forget it.
func (s candidateSelector) newCandidate(
	nextHop domain.PeerIdentity,
	hops int,
	conn PeerConnection,
	direct bool,
	attribution domain.RouteAttribution,
) RouteCandidate {
	return RouteCandidate{
		nextHop:            nextHop,
		channel:            conn.Channel,
		hops:               hops,
		protocolVersion:    s.versions.normalize(nextHop, conn.ReportedProtocolVersion),
		rawProtocolVersion: conn.ReportedProtocolVersion,
		connectedAt:        conn.ConnectedAt,
		direct:             direct,
		attribution:        attribution,
	}
}
