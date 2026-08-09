package datagram

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// planner.go is the scheduler itself: it turns a frame into an ordered list
// of next hops, walks it until one accepts the frame, and reports an
// outcome whose shape depends on WHOSE frame it is (§4.3).
//
// Reference: docs/refactoring/datagram-transport.md §4.3, §6, §6.1.

// ---------------------------------------------------------------------------
// avoid_next_hop
// ---------------------------------------------------------------------------

// AvoidedNextHop is the optional avoid_next_hop parameter of a local send.
// It is a LOCAL call parameter and never appears on the wire.
//
// The exclusion is applied BEFORE the direct-first branch and covers it: a
// retry to a destination that has a live direct session would otherwise go
// straight back into that same session, and the promise of a changed hop
// would be empty for the most common case.
//
// What it guarantees is exactly one thing — a different FIRST HOP, not a
// different path: two distinct first hops may converge downstream, and a
// local parameter cannot promise more. The explore rotation does not even
// promise this much, because parallel traffic on the same key decorrelates
// it.
//
// The zero value is "no exclusion", and that is not a zero-value business
// signal: the absence is a named constructor, so a caller cannot mean
// "avoid the zero identity".
type AvoidedNextHop struct {
	peer domain.PeerIdentity
	set  bool
}

// NoAvoidedNextHop is the explicit "no exclusion" value.
func NoAvoidedNextHop() AvoidedNextHop { return AvoidedNextHop{} }

// AvoidNextHop excludes one peer from this send entirely.
func AvoidNextHop(peer domain.PeerIdentity) AvoidedNextHop {
	return AvoidedNextHop{peer: peer, set: true}
}

// Excludes reports whether the peer is the excluded one.
func (a AvoidedNextHop) Excludes(peer domain.PeerIdentity) bool {
	return a.set && a.peer == peer
}

// Peer returns the excluded peer. The bool is false when nothing is
// excluded.
func (a AvoidedNextHop) Peer() (domain.PeerIdentity, bool) {
	if !a.set {
		return domain.PeerIdentity{}, false
	}
	return a.peer, true
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

// sendJob is one scheduling call. It is UNEXPORTED on purpose: the layer has
// exactly two entry points into §4.3 — Pipeline.SendLocal for a frame this
// node created and Pipeline.HandleInbound for one passing through — and a
// second, publicly reachable scheduler entry is how the vocabulary of §4.3
// grew two implementations that disagreed.
type sendJob struct {
	// frame is the datagram to place. Its dst, dtype and route_policy drive
	// every gate and the rotation key.
	frame protocol.DatagramFrame
	// incoming is where the frame came from: LocalIngress() for a frame this
	// node created, the arrival's channel and presented name for one being
	// forwarded. It decides three things at once — the freshness of the route
	// source, the split-horizon exclusion (splitHorizonExclusion states what
	// that exclusion can and cannot be built on) and whether the caller gets a
	// synchronous outcome or the frame is dropped silently.
	incoming IngressPeer
	// avoid is the optional avoid_next_hop exclusion. It reaches the
	// SELECTION, not a post-filter: §4.3 applies it before direct-first and
	// therefore before the explore rotation counts its modulus.
	avoid AvoidedNextHop
	// readOnly marks a selection that will NOT publish: the early
	// deliverability sieve of §4.1 step 7. §4.3 is explicit that the explore
	// counter mutates on a SEND, so a read-only look must leave it alone —
	// otherwise a transited explore frame would rotate twice on every hop,
	// once for the sieve and once for the selection that follows it.
	readOnly bool
}

// isLocal reports whether this node created the frame — the case that gets a
// synchronous outcome.
func (j sendJob) isLocal() bool { return j.incoming.IsLocal() }

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

// SendOutcomeKind is the four-way result of a local send (§4.3 item 4).
type SendOutcomeKind uint8

const (
	// SendOutcomeUnset is the zero value.
	SendOutcomeUnset SendOutcomeKind = iota
	// SendQueued means the frame reached a next hop's queue. It carries
	// the next hop ACTUALLY chosen, which is not necessarily the first
	// candidate.
	SendQueued
	// SendNoRoute means there were no candidates. Waiting for a route or
	// falling back is reasonable.
	SendNoRoute
	// SendRejected means the frame was refused rather than lost: a gate,
	// including the last-hop dtype gate, or the anti-replay cache refusing the
	// reservation — the key is already taken, or there was no room for it
	// (reserveRefusalOutcome). Repeating the SAME frame without changed
	// conditions is pointless in either case.
	SendRejected
	// SendFailed means a step of THIS NODE's own work failed rather than the
	// network's. Three things produce it:
	//
	//   - a locally created frame that does not form a valid header, delivery
	//     header, transcript or request label (sendLocalRouted,
	//     sendLocalRequest);
	//   - a send window ComputeDeadlines refuses;
	//   - a candidate walk in which no admitted next hop's queue CONFIRMED the
	//     frame (dispatch).
	//
	// The first two happen before anything is offered to a queue. The third does
	// NOT prove the frame stayed home: a queue answers about admission, and a
	// refusal read once the frame is already in it can follow a completed write
	// on a link that died afterwards (docs/protocol/network_core.md, "Tracked
	// sends"). It is also the only one of the three that can clear on its own,
	// and it is the one the retry is for: the SAME attempt is repeated with a
	// backoff — not because nothing went out, but because the receiver's
	// anti-replay cache drops the duplicate if something did. The first two are
	// faults in the frame the caller built, and an identical repeat meets an
	// identical refusal — the error says which.
	//
	// It does NOT license a transport fallback, and the difference is not a
	// nuance: `no_route` and `rejected` say the layer cannot carry this frame
	// at all — no route, or a gate refusing the route there is — so another
	// transport answers the same question differently. Here the layer's own way
	// is intact and one local step failed; falling back would put the same
	// ciphertext on the wire twice, once after the retry and once in a legacy
	// envelope, which is the duplicate §8 says does not exist.
	SendFailed
)

var sendOutcomeKindNames = map[SendOutcomeKind]string{
	SendOutcomeUnset: "unset",
	SendQueued:       "queued",
	SendNoRoute:      "no_route",
	SendRejected:     "rejected",
	SendFailed:       "failed",
}

// String returns the metric label of the kind.
func (k SendOutcomeKind) String() string { return enumName(sendOutcomeKindNames, k) }

// SendOutcome is the result of one scheduling call.
//
// Every field is unexported and there is no setter: the outcome is
// FINALIZED AT ENQUEUE and no later refusal — a Commit of the layer, or
// anything else that runs after the frame is in the queue — may rewrite a
// queued outcome into a rejection.
// This is enforced by the type rather than asked for in a comment, because
// the migration's transport fallback (§8) fires on exactly `no_route |
// rejected` under the assumption that nothing went out; a late `rejected`
// after an enqueue would send the same ciphertext a second time in a legacy
// envelope — a duplicate attempt that the contract says does not exist.
type SendOutcome struct {
	err     error
	nextHop domain.PeerIdentity
	missing domain.CapabilityName
	kind    SendOutcomeKind
	reason  RejectionReason
	local   bool
}

func queuedOutcome(local bool, nextHop domain.PeerIdentity) SendOutcome {
	return SendOutcome{kind: SendQueued, nextHop: nextHop, local: local}
}

func noRouteOutcome(local bool) SendOutcome {
	return SendOutcome{kind: SendNoRoute, local: local}
}

func rejectedOutcome(local bool, refused refusal) SendOutcome {
	return SendOutcome{
		kind:    SendRejected,
		reason:  refused.reason,
		missing: refused.missing,
		local:   local,
	}
}

func failedOutcome(local bool, err error) SendOutcome {
	return SendOutcome{kind: SendFailed, err: err, local: local}
}

// Kind reports which of the four results happened.
func (o SendOutcome) Kind() SendOutcomeKind { return o.kind }

// NextHop returns the next hop the frame was actually queued to. The bool
// is false for every non-queued outcome.
//
// The value matters beyond diagnostics: candidates are walked until one
// accepts, so the first candidate may have refused and the second taken the
// frame. Without this the caller could not aim the next retry's
// avoid_next_hop at the hop the frame really went to.
func (o SendOutcome) NextHop() (domain.PeerIdentity, bool) {
	if o.kind != SendQueued {
		return domain.PeerIdentity{}, false
	}
	return o.nextHop, true
}

// Rejection returns why policy refused. The bool is false unless the
// outcome is a rejection.
func (o SendOutcome) Rejection() (RejectionReason, bool) {
	if o.kind != SendRejected {
		return RejectionUnset, false
	}
	return o.reason, true
}

// MissingCapability returns the capability name behind a
// RejectionMissingCapability. The bool is false for every other outcome: a
// metric that only says "rejected" cannot tell an operator which name the
// network expects.
func (o SendOutcome) MissingCapability() (domain.CapabilityName, bool) {
	if o.kind != SendRejected || o.reason != RejectionMissingCapability {
		return "", false
	}
	return o.missing, true
}

// Err returns the failure behind a `failed` outcome, nil otherwise. It names
// which of the three local steps refused (see SendFailed).
func (o SendOutcome) Err() error { return o.err }

// SilentDrop reports whether the caller must drop this result silently
// instead of surfacing it. A transit frame with nowhere to go is a silent
// drop — the layer is unguaranteed and recovery belongs to the originator —
// while the same situation on a locally created frame is a synchronous
// refusal to its caller.
func (o SendOutcome) SilentDrop() bool { return !o.local && o.kind != SendQueued }

// String renders the outcome for a log line.
func (o SendOutcome) String() string {
	if o.kind == SendQueued {
		return fmt.Sprintf("queued(%s)", o.nextHop.String())
	}
	if o.kind == SendRejected {
		return fmt.Sprintf("rejected(%s)", o.reason.String())
	}
	return o.kind.String()
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

// SchedulerConfig wires the scheduler. Everything it needs from the node
// arrives here as an interface: the layer must not import
// internal/core/node, and a hidden global would make the freshness contract
// untestable.
type SchedulerConfig struct {
	// Routes is the candidate source (fresh lookup and cached snapshot).
	Routes RouteResolver
	// Peers resolves a next hop to the ONE connection the send would use.
	Peers PeerMetadata
	// Direct answers step 1: is the destination itself a live neighbour.
	Direct DirectSession
	// Secret is node_local_secret, behind the explore starting offset.
	Secret NodeSecret
	// Clock is the injectable time source, following the project
	// convention. Defaults to time.Now.
	Clock func() time.Time
	// LocalID is this node's identity, used to drop routes to self.
	LocalID domain.PeerIdentity
	// LocalProtocolVersion is the ceiling of the ranking key. A peer
	// claiming more is capped at it — see versionNormalizer.
	LocalProtocolVersion domain.ProtocolVersion
	// ExploreCounters is the size of the bounded rotation-counter LRU.
	// Defaults to DefaultExploreCounters.
	ExploreCounters int
}

// Scheduler places datagrams on next hops (§4.3) and answers the two
// read-only questions built on the same code: is a destination reachable,
// and what would the plan be.
type Scheduler struct {
	clock    func() time.Time
	routes   RouteResolver
	direct   DirectSession
	rotator  *exploreRotator
	selector candidateSelector
}

// NewScheduler validates the wiring. A missing dependency is refused at
// construction rather than nil-checked at every call site: a scheduler that
// silently skips the direct branch because Direct was nil would look like a
// routing bug months later.
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	if cfg.LocalID.IsZero() {
		return nil, errors.New("datagram: scheduler requires a local identity")
	}
	if cfg.LocalProtocolVersion <= 0 {
		return nil, errors.New("datagram: scheduler requires the local protocol version")
	}
	required := []struct {
		name   string
		absent bool
	}{
		{"a route resolver", isNilValue(cfg.Routes)},
		{"peer metadata", isNilValue(cfg.Peers)},
		{"a direct session source", isNilValue(cfg.Direct)},
		{"a node local secret", isNilValue(cfg.Secret)},
	}
	for _, dependency := range required {
		if dependency.absent {
			return nil, fmt.Errorf("datagram: scheduler requires %s", dependency.name)
		}
	}
	// THE PANIC BOUNDARY REACHES CONSTRUCTORS.
	//
	// This is a call into foreign code, made while the node is being built, and
	// it was the one such call with no boundary around it: an ordinary
	// implementation panic took node construction down instead of degrading to
	// the documented failure value, which the runtime path already does
	// correctly (see PathExplorer's use of the same seam).
	//
	// It was also called after isNilValue had already flagged the field, because
	// the checks above are built as a slice and every entry is evaluated: a
	// TYPED nil satisfies `!= nil`, so the method ran on it anyway.
	//
	// A crash converts to the empty secret, which the emptiness check below
	// then refuses — so the build fails with a named reason instead of a stack
	// trace, and the failure is the same one an implementation that honestly
	// returned nothing would produce.
	secret := guardHook(
		hookSite{hook: "NodeLocalSecret"},
		[]byte(nil),
		func() []byte { return cfg.Secret.NodeLocalSecret() },
	)
	if len(secret) == 0 {
		return nil, errors.New("datagram: scheduler requires a non-empty node local secret")
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Scheduler{
		clock:   clock,
		routes:  cfg.Routes,
		direct:  cfg.Direct,
		rotator: newExploreRotator(cfg.Secret, cfg.ExploreCounters),
		selector: candidateSelector{
			clock:    clock,
			peers:    cfg.Peers,
			localID:  cfg.LocalID,
			versions: versionNormalizer{local: cfg.LocalProtocolVersion},
		},
	}, nil
}

// selectFor is step 1 of a send: it turns a frame into the ordered candidate
// list §4.3 prescribes, WITHOUT touching a single piece of state.
//
// It is deliberately separate from dispatch, because the reservation of the
// replay key stands between the two: §4.1 puts Reserve next to the first
// mutating operation and after every decision that must not occupy state, and
// "there are no candidates" is one of those decisions.
//
// The selection is the WHOLE verdict, refusal included, and that is why it is
// the only return value: this step never ends a send by itself. The variant
// that could — a profile naming its own next hops instead of the routing table
// — is gone with the profiles, and every caller reads the outcome off the
// selection (outcomeWithoutCandidates).
func (s *Scheduler) selectFor(ctx context.Context, job sendJob) candidateSelection {
	opts := selectionOpts{
		incomingPeer: job.incoming,
		avoid:        job.avoid,
		rotate:       !job.readOnly && job.frame.RoutePolicy == domain.RoutePolicyExplore,
	}
	return s.ordinaryCandidates(ctx, job.frame, opts)
}

// ordinaryCandidates builds the §4.3 list: the direct session first when it
// passes its gates, then the ranked routing-table candidates, rotated when
// the policy is explore.
func (s *Scheduler) ordinaryCandidates(
	ctx context.Context,
	frame protocol.DatagramFrame,
	opts selectionOpts,
) candidateSelection {
	var selection candidateSelection

	direct := s.directSessionCandidate(ctx, frame, opts)
	if direct.present && !direct.admission.admitted {
		selection.refusal.record(direct.admission)
		if direct.admission.reason == RejectionUnsupportedDType {
			// HARD stop. Relaying around a destination that told us in its
			// handshake that it has no handler for this dtype would only
			// move the silent drop one hop further, and it would make the
			// reachability probe — which must call such a destination
			// unreachable — disagree with the send.
			selection.aborted = true
			return selection
		}
	}

	// The ranked list learns whether the destination was already offered the
	// frame here, so the two branches cannot both produce it and cannot both
	// drop it (§4.3 item 2).
	opts.directTried = direct.present
	ranked, refused := s.selector.rank(ctx, frame, s.routeHints(ctx, frame.Dst, opts), opts)
	selection.refusal.merge(refused)
	if opts.rotate {
		ranked = s.rotator.rotate(ranked, newExploreKey(frame.Dst, frame.DType))
	}

	if direct.present && direct.admission.admitted {
		selection.candidates = append(make([]RouteCandidate, 0, len(ranked)+1), direct.candidate)
	}
	selection.candidates = append(selection.candidates, ranked...)
	return selection
}

// directProbe is the verdict of step 1 for the destination itself.
type directProbe struct {
	candidate RouteCandidate
	admission peerAdmission
	present   bool
}

// directSessionCandidate applies step 1 of §4.3: the direct session goes
// first, but ONLY after the same gates every other candidate faces. A live
// session is not enough — today's file router sends directly only to peers
// advertising file_transfer_v1, and an ungated direct branch would quietly
// lose that check.
//
// The exclusions run BEFORE the lookup: avoid_next_hop must cover the
// direct branch, and a frame must never be handed back to the neighbour it
// arrived from.
func (s *Scheduler) directSessionCandidate(
	ctx context.Context,
	frame protocol.DatagramFrame,
	opts selectionOpts,
) directProbe {
	if opts.avoid.Excludes(frame.Dst) {
		return directProbe{}
	}
	if via, ok := splitHorizonExclusion(opts.incomingPeer); ok && via == frame.Dst {
		return directProbe{}
	}
	conn, ok := s.lookupDirectSession(ctx, frame)
	if !ok {
		return directProbe{}
	}
	admission := admitPeer(frame, frame.Dst, conn)
	probe := directProbe{present: true, admission: admission}
	if admission.admitted {
		probe.candidate = s.selector.newCandidate(frame.Dst, 1, conn, true)
	}
	return probe
}

// routeHints picks the source by freshness: a locally created frame reads
// the per-destination lookup so a route accepted a moment ago is visible;
// a transit frame reads the coalesced snapshot.
func (s *Scheduler) routeHints(ctx context.Context, dst domain.PeerIdentity, opts selectionOpts) []RouteHint {
	// A panic in the node's resolver becomes NO HINTS, which is the answer the
	// seam already defines for a destination with no route: the direct branch
	// has already run and keeps whatever it found, and a frame left without
	// candidates ends as `no_route` — an outcome §4.3 makes every caller
	// handle. It is deliberately not an empty-and-therefore-fatal case: this
	// runs on the session reader, which serves every other peer as well.
	site := hookSite{hook: "CachedRoutes", peer: dst}
	call := func() []RouteHint { return s.routes.CachedRoutes(ctx, dst) }
	if opts.incomingPeer.IsLocal() {
		site.hook = "FreshRoutes"
		call = func() []RouteHint { return s.routes.FreshRoutes(ctx, dst) }
	}
	return guardHook(site, nil, call)
}

// lookupDirectSession is the ONE call into the node's direct-session lookup.
// A panic becomes "the destination is not a live neighbour", which demotes the
// frame to the routing table exactly as a genuinely absent session does.
func (s *Scheduler) lookupDirectSession(
	ctx context.Context,
	frame protocol.DatagramFrame,
) (PeerConnection, bool) {
	site := hookSite{hook: "LookupDirectSession", peer: frame.Dst, dtype: frame.DType}
	return guardHookPair(site, PeerConnection{}, func() (PeerConnection, bool) {
		return s.direct.LookupDirectSession(ctx, frame.Dst, frame)
	})
}

// dispatch walks the candidates until one accepts the frame.
//
// Trying only one candidate per hop would mean that an immediate local
// failure parks the frame until the application times out, even though a
// working second next hop was available right away.
//
// The outcome of an unsuccessful walk distinguishes two situations that §4.3
// treats differently, and merging them mistreats both:
//
//   - admitted candidates existed and none took the frame: a temporary local
//     failure. Not `rejected` — the gate refusal of some OTHER peer says
//     nothing about a walk in which admitted peers were tried, and "repeating
//     is pointless" would stop retries whose problem is a saturated queue.
//     Not `no_route` either: a route was there;
//   - nothing was admitted at all: the gate verdict if one was recorded,
//     `no_route` otherwise — and that branch is `selection.publishable()`
//     above, not this one.
//
// publish is supplied by the caller because the layer publishes through the
// emitter, which the scheduler must not know about. The walk and the outcome
// vocabulary stay here, in ONE place.
func (s *Scheduler) dispatch(
	ctx context.Context,
	job sendJob,
	selection candidateSelection,
	publish hopPublisher,
) SendOutcome {
	local := job.isLocal()
	if !selection.publishable() {
		return selection.outcomeWithoutCandidates(local)
	}

	var failure error
	for _, candidate := range selection.candidates {
		outcome := publish(ctx, candidate)
		if outcome.Enqueued() {
			return queuedOutcome(local, candidate.nextHop)
		}
		if failure == nil {
			failure = outcome.Err()
		}
		log.Debug().
			Str("dst", job.frame.Dst.String()).
			Str("next_hop", candidate.nextHop.String()).
			Str("dtype", job.frame.DType.String()).
			Str("outcome", outcome.Kind().String()).
			Msg("datagram: next hop refused the frame, trying next candidate")
	}

	return failedOutcome(local, enqueueFailure(failure, len(selection.candidates)))
}

// enqueueFailure names the walk that found candidates and placed nothing. A
// sender that refused without stating a cause still owes the caller an error:
// `failed` promises a retry makes sense, and an outcome with a nil Err would
// be unloggable.
func enqueueFailure(cause error, tried int) error {
	if cause != nil {
		return fmt.Errorf("datagram: no next hop accepted the frame (%d tried): %w", tried, cause)
	}
	return fmt.Errorf("datagram: no next hop accepted the frame (%d tried)", tried)
}
