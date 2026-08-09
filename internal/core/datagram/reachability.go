package datagram

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// reachability.go holds the two READ-ONLY surfaces of the layer: the
// reachability probe and the route plan (§4.3).
//
// They are a contract of the layer, not a convenience for the migration:
// artifact owners build retries and diagnostics on them — the file manager
// has both its send entry point and its periodic top-up ticker on the
// probe, and the console and RPC on the plan.
//
// Both take the same inputs the send gates depend on — dst and dtype —
// because without them a probe would answer about "some" datagram rather
// than the one about to be sent: the last-hop gate depends on dtype. The
// plan takes route_policy in addition.
//
// Neither reserves anything, dials anything or spends a cryptographic
// budget; both read the FRESH lookup, exactly as a locally originated send
// does, so an action taken right after a route appears is not answered with
// "unreachable" while the send would already work.
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §6.1, §9.

// ErrInvalidQuery marks a read-only query describing a datagram that no real
// send could build.
//
// It is a refusal and not an empty answer, because the two mean opposite
// things to a caller: "there is no route to this destination" is a fact about
// the network, and "your question is malformed" is a fact about the caller. A
// probe that answered `unreachable` to a malformed query would send an adapter
// looking for a routing problem it does not have — and would quietly promise
// that a send built the same way is possible, which RoutedFrameBuilder refuses.
var ErrInvalidQuery = errors.New("datagram: invalid read-only query")

// ReachabilityQueryOpts is what a caller fills in to ask the probe.
//
// It is a separate, open struct from the query itself: this is the caller's
// raw material, and the query is the validated result. There is no way to
// reach the surfaces below with the raw material.
type ReachabilityQueryOpts struct {
	// Dst is the destination.
	Dst domain.PeerIdentity
	// DType is the type to be carried. It decides the last-hop gate: a
	// destination that never declared this dtype is unreachable FOR THAT
	// TYPE while being perfectly reachable for others.
	DType domain.DType
}

// ReachabilityQuery is a VALIDATED question about one datagram.
//
// Its fields are unexported and it has exactly one constructor, so a query
// that reached these surfaces is one a real send could have been built from:
// the same dtype rule protocol.DatagramFrame.Validate enforces on the wire.
//
// Go still allows the zero value to be written down, so the surfaces refuse
// anything this constructor did not stamp rather than probing on a zero
// destination. "Not constructible" is the goal; "not usable" is what the
// language can actually guarantee.
type ReachabilityQuery struct {
	dst       domain.PeerIdentity
	dtype     domain.DType
	validated bool
}

// NewReachabilityQuery validates the shape and returns the query.
func NewReachabilityQuery(opts ReachabilityQueryOpts) (ReachabilityQuery, error) {
	if opts.Dst.IsZero() {
		return ReachabilityQuery{}, fmt.Errorf("%w: a destination is required", ErrInvalidQuery)
	}
	if _, err := domain.ParseDType(opts.DType.String()); err != nil {
		return ReachabilityQuery{}, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
	}
	return ReachabilityQuery{
		dst:       opts.Dst,
		dtype:     opts.DType,
		validated: true,
	}, nil
}

// Dst returns the destination.
func (q ReachabilityQuery) Dst() domain.PeerIdentity { return q.dst }

// DType returns the type to be carried.
func (q ReachabilityQuery) DType() domain.DType { return q.dtype }

// gateFrame projects the query onto the shape the GATES read, and it is not a
// sendable frame: no version, no mode, no class, no src, no signature. The
// name says so, because the previous one — probeFrame — read like something a
// send could use, and the fields it leaves zero are exactly the ones
// Validate would refuse.
//
// Only the two fields the gates use are set: a probe must not be able to
// influence a decision through a field a real send would have filled
// differently.
func (q ReachabilityQuery) gateFrame() protocol.DatagramFrame {
	return protocol.DatagramFrame{
		Dst:   q.dst,
		DType: q.dtype,
	}
}

// RoutePlanQueryOpts is the probe's input plus the policy.
type RoutePlanQueryOpts struct {
	ReachabilityQueryOpts
	// RoutePolicy is `best` or `explore` (§4.3). The zero value is refused
	// rather than defaulted, exactly as RoutedFrameOpts refuses it: "which
	// policy did this answer describe" must not be a guess.
	RoutePolicy domain.RoutePolicy
}

// RoutePlanQuery is the probe's validated input plus the policy, because the
// plan renders an order and the order depends on the policy.
//
// The policy changes neither the candidate SET nor the comparator: under
// `explore` the plan deliberately shows the comparator order (§4.3), because
// the rotation counter mutates on a send, a read-only plan must neither move
// nor reserve it, and under concurrent sends "the next candidate" is not
// defined in advance at all. What it changes is what the plan may PROMISE,
// and the plan reports that itself rather than leaving a reader to guess —
// see RoutePlan.FirstCandidateGuaranteed.
type RoutePlanQuery struct {
	reach  ReachabilityQuery
	policy domain.RoutePolicy
}

// NewRoutePlanQuery validates the whole shape and returns the query.
func NewRoutePlanQuery(opts RoutePlanQueryOpts) (RoutePlanQuery, error) {
	reach, err := NewReachabilityQuery(opts.ReachabilityQueryOpts)
	if err != nil {
		return RoutePlanQuery{}, err
	}
	if !opts.RoutePolicy.Valid() {
		return RoutePlanQuery{}, fmt.Errorf("%w: route policy %q", ErrInvalidQuery, opts.RoutePolicy.String())
	}
	return RoutePlanQuery{reach: reach, policy: opts.RoutePolicy}, nil
}

// Reachability returns the probe half of the plan query.
func (q RoutePlanQuery) Reachability() ReachabilityQuery { return q.reach }

// RoutePolicy returns the policy the plan is asked for.
func (q RoutePlanQuery) RoutePolicy() domain.RoutePolicy { return q.policy }

// RoutePlanEntry is the public projection of one candidate. The fields
// mirror the comparator keys exactly, so a reader can rebuild the ranking
// decision from the output. ConnectedAt is left zero when the underlying
// metadata had no known timestamp — render that as "unknown" rather than
// inventing an uptime of now-minus-zero.
type RoutePlanEntry struct {
	ConnectedAt     time.Time
	NextHop         domain.PeerIdentity
	Hops            int
	ProtocolVersion domain.ProtocolVersion
}

// RoutePlan is the ranked next-hop plan for a destination.
type RoutePlan struct {
	entries []RoutePlanEntry
	// policy is the route_policy the plan was built for. It is kept because
	// it decides what element 0 means (§4.3).
	policy domain.RoutePolicy
	// refusal is the gate verdict that emptied the plan, when a gate is what
	// emptied it. An empty plan otherwise means the topology has nothing to
	// offer, and the two call for opposite reactions: a gate refusal is
	// pointless to wait out, a missing route is not.
	refusal refusal
}

// GateRefusal names the gate that cut the candidates. The bool is false when
// the plan is non-empty and when it is empty because there is simply no route:
// an operator reading "no path" has to be able to tell "the destination
// declared no handler for this type" from "the routing table has nothing".
func (p RoutePlan) GateRefusal() (RejectionReason, bool) {
	if len(p.entries) > 0 || !p.refusal.set {
		return RejectionUnset, false
	}
	return p.refusal.reason, true
}

// MissingCapability returns the capability name behind a
// RejectionMissingCapability gate refusal.
func (p RoutePlan) MissingCapability() (domain.CapabilityName, bool) {
	reason, refused := p.GateRefusal()
	if !refused || reason != RejectionMissingCapability {
		return "", false
	}
	return p.refusal.missing, true
}

// Policy returns the route policy this plan was built for.
func (p RoutePlan) Policy() domain.RoutePolicy { return p.policy }

// FirstCandidateGuaranteed reports whether element 0 is the hop a send would
// really try first. §4.3 promises that only for `best`: under `explore` the
// send rotates by a counter this read-only plan neither moves nor reads, so
// the plan shows the comparator order and says out loud that the first
// element is not a prediction.
func (p RoutePlan) FirstCandidateGuaranteed() bool {
	return p.policy != domain.RoutePolicyExplore
}

// Entries returns the plan in selection order: element 0 is what the send
// would try first, the rest are the fall-back order. The slice is a copy.
func (p RoutePlan) Entries() []RoutePlanEntry {
	return append([]RoutePlanEntry(nil), p.entries...)
}

// ReachabilityResult is the probe's answer, and it is a STRUCT rather than a
// bool because §6.1 makes the two negatives mean opposite things.
//
// "A negative live answer cancels a positive cached confirmation and clears it
// immediately" is a rule about SUPPORT: the peer told us in its handshake that
// it has no handler for this dtype. A destination that is merely off the
// routing table this second says nothing about support, and invalidating a
// confirmation on that would wipe a perfectly good one on every transient
// route loss. A single bool cannot carry the difference, and a caller forced
// to guess would either implement the rule wrong or not at all.
//
// The reason is the SAME vocabulary a send returns, produced from the same
// verdict, so the one-way guarantee of §4.3 is unchanged and strengthened:
// "unreachable" still means a send at that moment would not have been queued,
// and now it also names which of `no_route` and `rejected(reason)` it would
// have been.
type ReachabilityResult struct {
	missing   domain.CapabilityName
	reason    RejectionReason
	reachable bool
}

// Reachable reports whether there is somebody to give the first hop to.
func (r ReachabilityResult) Reachable() bool { return r.reachable }

// Rejection names the GATE that refused. The bool is false for a reachable
// destination and for the plain absence of a route — the negative that must
// NOT invalidate a cached confirmation.
func (r ReachabilityResult) Rejection() (RejectionReason, bool) {
	if r.reachable || r.reason == RejectionUnset {
		return RejectionUnset, false
	}
	return r.reason, true
}

// UnsupportedDType reports the one negative §6.1 calls a negative live answer
// about SUPPORT: the destination is a live neighbour and its declared dtype
// set does not contain this type. This — and only this — cancels a cached
// (dtype, caps) confirmation immediately.
func (r ReachabilityResult) UnsupportedDType() bool {
	return !r.reachable && r.reason == RejectionUnsupportedDType
}

// MissingCapability returns the capability name behind a
// RejectionMissingCapability, so an operator learns which name the path
// expects instead of only that something was refused.
func (r ReachabilityResult) MissingCapability() (domain.CapabilityName, bool) {
	if r.reachable || r.reason != RejectionMissingCapability {
		return "", false
	}
	return r.missing, true
}

// String renders the result for a log line.
func (r ReachabilityResult) String() string {
	if r.reachable {
		return "reachable"
	}
	if r.reason == RejectionUnset {
		return "unreachable(no_route)"
	}
	return "unreachable(" + r.reason.String() + ")"
}

// Reachable answers "is there anybody to give the first hop to" for this
// exact datagram: a direct session that passes the role gate of step 1, or
// a route whose next hop passes the candidate filters.
//
// The guarantee is ONE-WAY and covers BOTH negative outcomes of a send: an
// "unreachable" answer means a send performed at the same moment over the
// same data would NOT have been queued — it would have returned `no_route`
// OR a gate's `rejected`, the last-hop dtype gate included. Phrasing it
// through `no_route` alone would be a lie: a destination that never
// declared the dtype must be called unreachable, and a send to it returns
// `rejected`. The result carries WHICH of the two it would have been, because
// §6.1 acts on one of them and must not act on the other.
//
// A positive answer guarantees nothing. The probe is TOCTOU by
// construction: the route may disappear between the two calls and no
// read-only interface can fix that. It also proves nothing about the
// remote endpoint's support for the type — that is a separate confirmation
// (§6.1), which for a direct peer is exactly what the last-hop gate gives.
//
// The probe reserves nothing and spends no cryptographic budget: it walks
// the same selection code the send does and stops before the enqueue.
//
// It deliberately does NOT accept avoid_next_hop. Its agreement with the
// send is scoped to sends WITHOUT an exclusion, so "reachable" from the
// probe together with `no_route` from a send-with-avoid is a lawful pair,
// not a contradiction.
func (s *Scheduler) Reachable(ctx context.Context, query ReachabilityQuery) (ReachabilityResult, error) {
	if !query.validated {
		return ReachabilityResult{}, fmt.Errorf(
			"%w: build it with NewReachabilityQuery", ErrInvalidQuery)
	}
	selection := s.ordinaryCandidates(ctx, query.gateFrame(), selectionOpts{
		incomingPeer: LocalIngress(),
		avoid:        NoAvoidedNextHop(),
		rotate:       false,
	})
	if selection.publishable() {
		return ReachabilityResult{reachable: true}, nil
	}
	// The refusal is read through the SAME function a send uses to name why
	// its walk had nothing to try, so the probe cannot come to a different
	// conclusion than the send it exists to predict.
	outcome := selection.outcomeWithoutCandidates(true)
	reason, rejected := outcome.Rejection()
	if !rejected {
		return ReachabilityResult{}, nil
	}
	missing, _ := outcome.MissingCapability()
	return ReachabilityResult{reason: reason, missing: missing}, nil
}

// ExplainRoute returns the ranked plan a real send would build.
//
// For `best` this is the same list from the same fresh source, with the
// direct session as element 0 whenever it passes the gates: a diagnostic
// that disagrees with the live send inside the snapshot's republish window
// misinforms the operator.
//
// For `explore` the plan shows the COMPARATOR order, not the future
// rotation. The rotation counter advances on a send; a read-only plan
// neither moves nor reserves it, and under concurrent sends of the same key
// "the next candidate" is not defined in advance at all. Only `best`
// promises that the plan's first element is the first candidate of the
// send.
func (s *Scheduler) ExplainRoute(ctx context.Context, query RoutePlanQuery) (RoutePlan, error) {
	if !query.reach.validated {
		return RoutePlan{}, fmt.Errorf("%w: build it with NewRoutePlanQuery", ErrInvalidQuery)
	}
	selection := s.ordinaryCandidates(ctx, query.reach.gateFrame(), selectionOpts{
		incomingPeer: LocalIngress(),
		avoid:        NoAvoidedNextHop(),
		rotate:       false,
	})
	plan := RoutePlan{
		policy:  query.policy,
		refusal: selection.refusal,
	}
	if selection.aborted {
		return plan, nil
	}
	plan.entries = make([]RoutePlanEntry, 0, len(selection.candidates))
	for _, candidate := range selection.candidates {
		plan.entries = append(plan.entries, RoutePlanEntry{
			NextHop:         candidate.nextHop,
			Hops:            candidate.hops,
			ProtocolVersion: candidate.protocolVersion,
			ConnectedAt:     candidate.connectedAt,
		})
	}
	return plan, nil
}
