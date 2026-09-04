package node

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TODO(presence-gates-delivery): make presence the send gate — `online` ONLY —
// once the network has upgraded.
//
// # Where we are today
//
// Presence does not gate sending in any state. A message goes out whether the
// contact reads `online`, `probing`, `offline` or `unknown`; what decides is
// routing ("is there anywhere to hand this frame"), exactly as before this
// work. Presence only WAKES the retry when a contact comes back. Put the other
// way round, since that is how the end state is phrased below: sending
// currently proceeds at `probing` and at `online` alike, and at everything
// else too.
//
// # Where this is going
//
// The intended end state is that a send is attempted only for a contact who is
// `online` — proven present — and `probing` stops qualifying. That is strictly
// better than routing alone: it stops throwing messages at a path that leads
// to somebody who is not there, and it is what makes the retry schedule mean
// something.
//
// # Why it CANNOT be turned on yet, and what has to be true first
//
// Today most contacts cannot answer a probe at all, so their presence is
// `route_fallback` at best and `unknown` at worst. Gating on `online` now
// would make every un-upgraded contact permanently unreachable — the same
// class of bug this work removes, with a worse blast radius, which is why
// presence_not_a_gate_test.go currently forbids the delivery path from reading
// presence at all.
//
// The precondition is a COMPATIBILITY invariant, and it is not the same as the
// route fallback's retirement — an earlier revision of this note tied the two
// together and that was wrong. They are different steps of the overlay rollout
// and they answer different questions:
//
//   - the fallback is retired when this node stops holding a full routing
//     table (the overlay becomes the primary plane, step 11). That says
//     nothing about whether the peers still in the network can answer a probe;
//   - old clients are excluded when MinimumProtocolVersion rises (step 12).
//
// Turning the gate on at step 11 would leave every peer that cannot prove
// liveness permanently unreachable for messages — the bug this work removes,
// with a worse blast radius. The invariant this needs is narrower than either:
//
//	every peer this node will accept a session from is able to reach a
//	PROVEN `online` — i.e. answers `target_proof` — so that "not online"
//	always means "not there" and never "too old to say".
//
// That is a statement about the accepted population, so its natural home is
// the protocol floor: it becomes true when MinimumProtocolVersion is at or
// past the version that made probe support mandatory. It has to be asserted
// on its own, not inferred from the fallback, and the guard test has to be
// relaxed deliberately in the same change — from "may not read presence" to
// "may read only the online state".
//
// Until then: presence wakes delivery and never blocks it.
//
// ---
//
// presence_service.go connects the projector to the three things it has to ask:
// routing, our own connectivity, and whether a contact can answer a probe.
//
// Every one of those is ASKED at projection time rather than mirrored into an
// event-derived copy. That is not a stylistic preference: a mirrored answer is
// wrong exactly when the underlying fact changed while nobody was watching, and
// that is the case every event-derived version of "is this peer back" has
// missed (see the reachability pass in delivery_retry.go, which learned the
// same lesson the expensive way).
//
// The published result is an atomic.Pointer snapshot, next to routingSnap and
// for the same reason: the contact list repaints on every frame, and a hot
// reader that takes a domain mutex to answer "is this contact online" would put
// the interface behind the routing writers. docs/locking.md carries the class.

// presenceSnapshotInterval bounds how often the projection is recomputed. The
// projector itself is cheap (one pass over the contacts, tens of entries), but
// each pass asks routing per contact, and routing answers under its own lock.
//
// Two seconds matches routingSnapshotMinInterval deliberately: the routing
// answer this pass reads cannot be fresher than that anyway, so a faster
// presence cadence would buy staleness it cannot remove and pay for it in lock
// traffic. Events (a proof, a frame, a session close) do not wait for this
// interval — they are recorded the moment they happen and are visible at the
// next pass.
const presenceSnapshotInterval = 2 * time.Second

// presenceHeartbeatInterval is how often the projection is re-announced even
// when it has not changed.
//
// It exists because the event bus is best-effort: a full subscriber inbox drops
// the notification, and nothing downstream can tell that it happened. Without a
// heartbeat one dropped event leaves the interface stale until the next real
// change, which on a settled node may never come.
//
// A minute is chosen to be far longer than the projection cadence — so a busy
// node pays nothing extra, because it is publishing real changes anyway — and
// far shorter than a person's patience with a wrong contact list.
const presenceHeartbeatInterval = time.Minute

// presenceAliveValidity is how long one proof of life keeps a contact online.
//
// It is the `visible` class of docs/protocol/presence.md §4 (150 s cadence × Detect Mult 3 = 450 s),
// used as the single validity until per-contact cadence classes exist: the
// interface has no notion of a focused conversation at this layer yet, and
// picking the SHORTEST class here would expire proofs that no probe is
// scheduled to renew, turning honest presence into a flicker.
const presenceAliveValidity = 450 * time.Second

// presenceSnapPtr mirrors routingSnapPtr. Zero value usable: Load returns nil
// until the first projection, and every reader treats nil as "nothing known",
// which is the correct answer before the first pass has run.
type presenceSnapPtr = atomic.Pointer[presenceSnapshot]

type presenceSnapshot struct {
	// set is immutable once stored. Readers get it by pointer and clone
	// only when they intend to mutate.
	set domain.PresenceSet
	// builtAt is when this projection was computed. DIAGNOSTIC ONLY — never
	// an ordering key. See generation.
	builtAt presenceInstant
	// generation numbers the projections of this process, starting at 1.
	//
	// It exists because a wall clock cannot order them. Two projections can
	// carry the same instant — on Windows the clock advances in steps of
	// 0.5 to 15.6 ms, which is longer than a projection takes — and a clock
	// that steps backwards makes a newer projection look older for as long as
	// it takes real time to catch up. Both cases end the same way: a genuinely
	// new projection is refused as stale, and on the backwards step even the
	// heartbeat cannot repair it.
	//
	// The counter is assigned under presencePublishMu, which already
	// serialises the whole pass, so it is strictly increasing by construction
	// rather than by assumption. Zero means no projection has run yet.
	generation uint64
}

// PresenceSnapshot returns what this node currently believes about each
// contact's liveness. The returned set is caller-owned.
//
// An identity absent from the set is unknown, and PresenceSet.Get is what
// enforces that — callers must not index the map directly.
func (s *Service) PresenceSnapshot() domain.PresenceSet {
	set, _ := s.PresenceSnapshotAt()
	return set
}

// PresenceSnapshotAt returns the projection together with its generation, and
// the second value is what makes two readers comparable.
//
// The projection is a WHOLE-SET answer: every pass covers every contact, so two
// readers never hold complementary halves — they hold the same picture from two
// moments. Without a generation a consumer merging them has no way to tell
// which moment is later, and "the one that arrived on an event is newer" is not
// a fact about anything: the event handler and a full probe each read this
// pointer independently, and either can win the race to read it first.
//
// BOTH values come from ONE load, and that is a requirement rather than an
// optimisation: reading the set and then re-reading for the stamp can staple a
// new generation onto an old set if a projection lands in between. The really
// new set then arrives carrying a generation already seen and is refused as a
// duplicate — and when the stale half was empty, the interface sits on the
// routing fallback until the next heartbeat.
//
// A zero generation means no projection has run yet. That is not the same as
// "an empty projection", and a consumer must not treat it as one — nothing is
// known, rather than nobody is present.
func (s *Service) PresenceSnapshotAt() (domain.PresenceSet, uint64) {
	snap := s.presenceSnap.Load()
	if snap == nil {
		return nil, 0
	}
	return snap.set.Clone(), snap.generation
}

// presenceFrame answers the local RPC "fetch_presence".
//
// It is a separate command from fetch_reachable_ids rather than a replacement,
// because the two answer different questions and both still have callers:
// reachability is what the delivery paths ask, presence is what a person is
// shown. Merging them is what produced the confusion in the first place.
func (s *Service) presenceFrame() protocol.Frame {
	// ONE load for both halves — see PresenceSnapshotAt. Reading the set and
	// then re-reading for the generation is exactly how an old set acquires a
	// new number.
	set, generation := s.PresenceSnapshotAt()
	entries := make([]protocol.PresenceFrame, 0, len(set))
	for _, identity := range sortedIdentities(presenceIdentities(set)) {
		presence := set.Get(identity)
		entry := protocol.PresenceFrame{
			Identity: identity.String(),
			State:    presence.State.String(),
			Source:   presence.Source.String(),
		}
		if presence.State == domain.PresenceUnknown {
			entry.Reason = presence.Reason.String()
		}
		entries = append(entries, entry)
	}
	// The generation travels with the rows: a reader over RPC has the same
	// ordering problem as one reading the pointer directly, and solving it in
	// only one of the two paths would leave the other silently racy.
	return protocol.Frame{
		Type:               "presence",
		Presence:           entries,
		PresenceGeneration: generation,
	}
}

func presenceIdentities(set domain.PresenceSet) []domain.PeerIdentity {
	out := make([]domain.PeerIdentity, 0, len(set))
	for identity := range set {
		out = append(out, identity)
	}
	return out
}

// presenceLoop recomputes the projection on its own cadence.
//
// It exists because most of what changes presence is NOT a routing event. A
// proof arrives, a probe times out, a validity window expires — none of those
// touch the routing table, and the first version of this code published only
// from the routing snapshot rebuild, which returns immediately when the table
// is clean. On a quiet node that meant `probing → online` and the three-strike
// `→ offline` could sit unpublished until some unrelated route changed.
//
// The pass is cheap by construction — one routing lookup per CONTACT, tens of
// them — so a fixed cadence is affordable and removes a whole class of "the
// interface did not hear about it" bug.
//
// Publication is conditional on the projection actually changing, which on a
// settled node means one event a minute and not one every two seconds: the
// heartbeat below re-announces the unchanged state deliberately, because the
// event bus drops events under load and a subscriber that misses one has no
// way to notice. A node with NO CONTACTS is the only one that publishes
// nothing at all.
func (s *Service) presenceLoop(ctx context.Context) {
	ticker := time.NewTicker(presenceSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshPresenceSnapshot()
		}
	}
}

// refreshPresenceSnapshot recomputes the projection, publishes it if it
// changed, and wakes anything that was waiting for a contact to come back.
//
// Callers: the presence loop above, the routing snapshot rebuild (so a
// route-driven change lands in the same generation as the routing event that
// caused it), and the urgent liveness events directly.
//
// Runs outside every domain mutex: the inputs it gathers reach into routing,
// trust and aggregate status, each of which takes its own lock.
func (s *Service) refreshPresenceSnapshot() {
	if s.presenceProjector == nil {
		return
	}
	// One pass at a time. Several goroutines reach this — the loop, the
	// routing refresher and each liveness event — and the previous-generation
	// comparison below is only meaningful if projection and publication are
	// serialized against each other.
	s.presencePublishMu.Lock()
	defer s.presencePublishMu.Unlock()

	now := s.presenceNow()
	contacts := s.presenceContacts()

	// Nothing to say about nobody. A node with no contacts — a relay, a
	// headless bootstrap node, most of the test suite — does no routing
	// lookups, no reachability queries and no publication at all.
	//
	// Guarded on the PREVIOUS generation too, so the pass that empties a
	// contact list still publishes that emptiness once; it is only the steady
	// state of "there were none and there are none" that is skipped.
	if len(contacts) == 0 {
		if previous := s.presenceSnap.Load(); previous == nil || len(previous.set) == 0 {
			return
		}
	}

	set := s.presenceProjector.project(presenceInputs{
		Now:               now,
		LocalConnectivity: s.presenceLocalConnectivity(),
		Contacts:          contacts,
		RouteState:        s.presenceRouteStateFor,
		Probeable:         s.presenceContactIsProbeable,
	})

	// The prober keeps a schedule per contact, and a removed contact would
	// otherwise leave an entry in it forever. The projection already knows the
	// current set, so pruning here costs one pass and removes a whole class of
	// slow leak rather than relying on anyone remembering to call it.
	if s.presenceProber != nil {
		current := make(map[domain.PeerIdentity]struct{}, len(contacts))
		for _, contact := range contacts {
			current[contact] = struct{}{}
		}
		s.presenceProber.forgetContacts(current)
	}

	previous := s.presenceSnap.Load()
	// The generation is derived from the one being replaced, under
	// presencePublishMu — the mutex that already makes this pass exclusive. So
	// it increases by construction, with no dependence on a clock that can tie
	// or step backwards.
	var generation uint64
	if previous != nil {
		generation = previous.generation
	}
	s.presenceSnap.Store(&presenceSnapshot{set: set, builtAt: now, generation: generation + 1})
	s.lastPresenceSnapAtNanos.Store(time.Now().UnixNano())

	// Ask immediately about contacts with a path and nothing proven along it.
	// Without this the answer waits for the periodic probe — up to the full
	// jittered interval, so nearly three minutes — and a contact who just came
	// back would sit in `probing` for all of it.
	if s.presenceProber != nil {
		s.presenceProber.probeNow(s.presenceProjector.takeProbeNow())
	}

	arrived := presenceArrivals(previousPresenceSet(previous), set)
	// And the returns two projections cannot show between them: a contact who
	// went quiet for a probe or two and answered again before their earlier
	// window ran out reads `online` at both ends. See
	// presenceRecord.returnPending.
	arrived = withPresenceReturns(arrived, s.presenceProjector.takeReturned(), set)
	unchanged := len(arrived) == 0 && presenceSetsEqual(previousPresenceSet(previous), set)

	// A heartbeat republish even when NOTHING changed, at a slow cadence.
	//
	// The event bus is best-effort: publication drops the event when a
	// subscriber's inbox is full (ebus, 64 slots), and the interface has no
	// way to notice it missed one. Publishing only on change meant that a
	// single dropped event left the contact list stale until the NEXT change
	// — and on a settled node the next change can be a very long time away,
	// or never.
	//
	// So the state is re-announced periodically regardless. It is a
	// reconcile, not a poll: the payload is empty and the subscriber re-reads
	// the projection, so the cost is one event a minute against a class of
	// silent staleness that is otherwise unrecoverable without a restart.
	// time.Since on a value that kept its monotonic reading: this is elapsed
	// time, not a difference of wall clocks, so an NTP step or a manual clock
	// change cannot make it negative and suppress the reconcile. The field is
	// read and written under presencePublishMu, held for this whole pass.
	//
	// The zero value is deliberately in the distant past, so the first pass
	// always announces.
	if unchanged && !s.lastPresenceHeartbeatAt.IsZero() &&
		now.Since(s.lastPresenceHeartbeatAt) < presenceHeartbeatInterval {
		return
	}
	s.lastPresenceHeartbeatAt = now

	// A contact who has just become present is the signal the delivery retry
	// has been polling routing for. Presence does not DECIDE anything about
	// sending — the retry re-asks routing itself, and must — but it is the
	// centralized place that notices a return, so it is the right place to
	// ring the bell. The existing route-driven wakeups stay as the fallback for
	// contacts presence cannot speak about.
	//
	// They are NOT "the only mechanism once the routing table is gone" — an
	// earlier revision said that and it is self-contradictory: those wakeups
	// are driven by routing EVENTS, so they disappear together with the table
	// that produces them. What survives the overlay cutover is presence itself;
	// the route-driven half needs a replacement built on whatever the overlay
	// signals instead, and that replacement does not exist yet.
	//
	// BOTH halves of the return handling are rung, because they cover
	// different messages and a return needs both:
	//
	//   - kickDeliveryRetriesForReachable releases what was HELD because the
	//     recipient was unreachable;
	//   - wakeOverdueForReturningPeer clamps what was already SENT and never
	//     confirmed, so it is retried soon instead of sitting out a backoff
	//     that reaches eleven minutes.
	//
	// Waking only the first was the same half-answer that made this worth
	// centralizing: the commonest case — a message that went out just before
	// the recipient vanished — lives entirely in the second.
	if len(arrived) > 0 {
		s.kickDeliveryRetriesForReachable(arrived)
		wakeAt := time.Now().UTC()
		for identity := range arrived {
			s.wakeOverdueForReturningPeer(identity, wakeAt)
		}
	}

	ebus.PublishContactPresenceUpdated(s.eventBus)
}

func previousPresenceSet(snap *presenceSnapshot) domain.PresenceSet {
	if snap == nil {
		return nil
	}
	return snap.set
}

// presenceArrivals is the set of contacts this generation learned something
// better about: they became present, or they were already assumed present and
// have now PROVEN it.
//
// The second half is not a refinement, it is the main case. A contact whose
// route never went away — which is most of them, since a route outlives its
// owner by up to ten minutes — is `online` by the route fallback the whole time
// they are gone. Comparing states alone, their actual return changes nothing:
// `online → online`, no arrival, no wake. And routing sees no transition
// either, for exactly the same reason, so the mechanism that would otherwise
// catch it is blind to the same case. The proof is the ONLY event that marks
// the moment, which makes an upgrade of the source the signal to act on.
//
// A contact who was already proven present is still not an arrival: ringing the
// bell every pass for everybody would turn a wakeup into a poll.
func presenceArrivals(previous, current domain.PresenceSet) map[domain.PeerIdentity]struct{} {
	var arrived map[domain.PeerIdentity]struct{}
	for identity, presence := range current {
		if presence.State != domain.PresenceOnline {
			continue
		}
		was := previous.Get(identity)
		switch {
		case was.State != domain.PresenceOnline:
			// Absent, unknown or merely being probed a moment ago.
		case !was.IsProven() && presence.IsProven():
			// Assumed present, now proven: this is the instant their
			// return became a fact rather than an inference.
		default:
			continue
		}
		if arrived == nil {
			arrived = make(map[domain.PeerIdentity]struct{})
		}
		arrived[identity] = struct{}{}
	}
	return arrived
}

// withPresenceReturns folds the projector's returns into the arrival set.
//
// It is a union and not a replacement: the two witnesses answer different
// questions. presenceArrivals compares projections and catches every return
// that CHANGED what a reader sees; the projector's list catches the ones that
// did not, because the earlier proof was still inside its validity window. A
// contact can be in both, and is then woken once.
//
// The state is re-checked against this generation rather than trusted from the
// list, so a return recorded by an event and then contradicted by the pass that
// projected it — a session closing in the same breath — does not wake anything.
func withPresenceReturns(arrived map[domain.PeerIdentity]struct{}, returned []domain.PeerIdentity, set domain.PresenceSet) map[domain.PeerIdentity]struct{} {
	for _, identity := range returned {
		if set.Get(identity).State != domain.PresenceOnline {
			continue
		}
		if arrived == nil {
			arrived = make(map[domain.PeerIdentity]struct{}, len(returned))
		}
		arrived[identity] = struct{}{}
	}
	return arrived
}

// presenceSetsEqual reports whether two generations say the same thing. Used to
// keep a node with nothing happening from emitting an event every tick.
func presenceSetsEqual(previous, current domain.PresenceSet) bool {
	if len(previous) != len(current) {
		return false
	}
	for identity, presence := range current {
		if previous.Get(identity) != presence {
			return false
		}
	}
	return true
}

// presenceLocalConnectivity reports whether OUR OWN network is in a state where
// conclusions about other people mean anything.
//
// Offline and reconnecting are excluded because with no connected peer at all,
// an empty routing table describes this node and nothing else (docs/protocol/presence.md §3 rule 1).
//
// `limited` (zero or one usable peer) is deliberately NOT excluded, and this is
// an open question of the design note answered in the direction that keeps presence
// useful: a node with one good peer still routes, still probes and still gets
// signed frames back, so treating that as "know nothing about anybody" would
// blank the contact list for every user on a small or freshly started network —
// a much commoner situation than the partition the exclusion would protect
// against. The residual error (a contact reachable only through the peer we
// lack) is bounded by the probe, which is what actually answers the question.
func (s *Service) presenceLocalConnectivity() bool {
	status := s.AggregateStatus().Status
	return status != domain.NetworkStatusOffline && status != domain.NetworkStatusReconnecting
}

// presenceContacts lists the identities presence is kept for. Presence is a
// question about people the user knows, so the address book — not the routing
// table — is what bounds this state.
func (s *Service) presenceContacts() []domain.PeerIdentity {
	if s.trust == nil {
		return nil
	}
	contacts := s.trust.trustedContacts()
	out := make([]domain.PeerIdentity, 0, len(contacts))
	for address := range contacts {
		identity := domain.PeerIdentityFromWire(address)
		if identity.IsZero() {
			continue
		}
		out = append(out, identity)
	}
	return out
}

// presenceRouteStateFor answers the routing question for one contact, and
// separates "no route" from "no route BECAUSE WE SAID SO".
//
// The distinction cannot be read off the reachability set: an identity we
// quarantined and an identity that left look identical there, and they are
// opposite facts. So the table is consulted directly — the same shape as
// recipientHasPath, which asks routing per recipient for the delivery question.
func (s *Service) presenceRouteStateFor(identity domain.PeerIdentity) presenceRouteState {
	if s.routingTable == nil || identity.IsZero() {
		return presenceRouteAbsent
	}
	// LookupWithSuppressed rather than Lookup, because Lookup has already
	// applied this node's OWN exclusions — a dead uplink, a black-hole
	// cooldown — and returns the same empty slice for "the network has no
	// claim" and "we are refusing every claim there is". Those are opposite
	// facts here: the first is an observation about the contact, the second
	// says only that our paths are broken, and a black-hole arm lasts two
	// minutes. Reading it as `offline` is a two-minute lie about somebody who
	// never went anywhere.
	routes, suppressedByFilter := s.routingTable.LookupWithSuppressed(identity)
	if suppressedByFilter {
		return presenceRouteSuppressed
	}
	if len(routes) == 0 {
		// Nothing to suppress: routing has no live claim about this contact
		// at all. That is an absence, not a refusal of ours.
		return presenceRouteAbsent
	}

	suppressed := false
	for _, route := range routes {
		if s.routeIsBlockedByQuarantine(route.NextHop, route.Hops) {
			// A claim exists and we are the reason it is unusable.
			suppressed = true
			continue
		}
		if s.resolveRouteNextHopAddress(route.NextHop, route.Hops) == "" {
			// The next hop is not dialable from here. This is a local
			// deficiency rather than an observation about the contact,
			// so it counts the same way a suppression does.
			suppressed = true
			continue
		}
		return presenceRoutePresent
	}
	if suppressed {
		return presenceRouteSuppressed
	}
	return presenceRouteAbsent
}

// presenceContactIsProbeable reports whether a liveness probe to this contact
// could be answered at all.
//
// Derived from what the contact DECLARED — the dtype set in their identity
// record and the datagram layer's own reachability verdict — and never from
// their silence. Silence from a build that predates the probe is
// indistinguishable from silence from a node that is gone, and reading it as
// the latter is the trap named in docs/protocol/presence.md §4: the contact would be declared offline
// for being old.
//
// presenceContactIsProbeable reports whether a liveness probe to this contact
// can be sent AND will be recognised as coming from a contact.
//
// Three things have to hold, and the third one is the correction of a real
// hole. An earlier revision required only that the datagram layer could reach
// them; a contact whose box key we lacked was still "probeable", the probe went
// out WITHOUT the sealed claim, and the target answered it on the public path.
// That is fail-open: the gate this stage exists for was bypassed by not having
// a key, and — because the contact counted as probeable — they were also kept
// out of the route fallback that is meant to cover exactly this case.
//
//  1. the datagram layer exists and says the type reaches them;
//  2. the contact's box key is known, so the sealed reciprocity claim can be
//     built. Derived from what they DECLARED (their identity record), never
//     from their silence: silence from an old build is indistinguishable from
//     silence from a dead one;
//  3. — and when (2) is missing, a resolution is started, so the next pass has
//     what it needs instead of this being a permanent state.
//
// A contact that fails any of these is not probed at all. They are shown from
// the route fallback, which is honest about being an inference.
func (s *Service) presenceContactIsProbeable(identity domain.PeerIdentity) bool {
	if identity.IsZero() {
		return false
	}
	layer := s.datagramLayer()
	if layer == nil || layer.scheduler == nil {
		return false
	}
	if s.trust == nil {
		return false
	}
	if _, known := s.trust.contactBoxKey(identity); !known {
		// No key, no claim, no probe. Ask for their record so this stops
		// being true — the fallback covers them meanwhile.
		s.startPresenceIdentityResolution(identity)
		return false
	}
	query, err := datagram.NewReachabilityQuery(datagram.ReachabilityQueryOpts{
		Dst:   identity,
		DType: domain.DTypeGetIdentity,
	})
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), presenceReachabilityTimeout)
	defer cancel()
	result, err := layer.scheduler.Reachable(ctx, query)
	if err != nil {
		return false
	}
	return result.Reachable()
}

// startPresenceIdentityResolution asks the lookup engine for a contact's record
// so that a probe to them becomes possible.
//
// Best effort and deliberately quiet: the resolver has its own cooldown and its
// own idea of when a target is worth asking about, and presence must not become
// a second scheduler for it. If it declines, the contact stays on the fallback,
// which is a correct answer rather than a broken one.
func (s *Service) startPresenceIdentityResolution(identity domain.PeerIdentity) {
	if s.identityResolver == nil || identity.IsZero() {
		return
	}
	// Once per contact per interval, not once per projection pass.
	//
	// The projection runs every two seconds, and asking on each pass had a
	// visible cost even though the durable intent deduplicates: an existing
	// resolution re-publishes its state on every call, so a contact with no
	// route produced a `NoRoute` status update twice a second — overwriting
	// whatever the interface was showing and pushing real events out of a
	// best-effort bus inbox.
	if !s.presenceResolutionDue(identity) {
		return
	}

	// Called synchronously, and deliberately not on a background goroutine:
	// StartResolution only registers intent — the resolver's own loop does the
	// asking — so there is nothing here to wait on, and a goroutine started
	// from a projection pass would be one the shutdown does not join.
	if _, err := s.identityResolver.StartResolution(identity, identityIntentReason{Type: identityIntentReasonPresence}); err != nil {
		log.Debug().Err(err).Str("identity", identity.String()).
			Msg("presence_identity_resolution_declined")
		return
	}

	// The contact list this pass is working from was snapshotted before the
	// pass began, so the contact can have been DELETED in between — and the
	// delete's own cleanup (forgetPresenceResolution) then ran before this
	// intent existed. The intent is durable, so the result would be a
	// background lookup for somebody no longer in the address book, surviving
	// restarts.
	//
	// Re-reading membership after the fact closes it without a new lock: the
	// two orders are the only ones possible, and both end clean. If the delete
	// lands first we see it here and undo; if it lands after this check, its
	// own cleanup is what removes the intent. Undoing twice is harmless —
	// forgetPresenceResolution is idempotent.
	if s.trust != nil && !s.trust.isTrustedContact(identity) {
		log.Debug().Str("identity", identity.String()).
			Msg("presence_identity_resolution_undone_contact_removed")
		s.forgetPresenceResolution(identity)
	}
}

// presenceResolutionAskInterval is the floor between two presence-driven
// resolution requests for one contact. Long enough that a contact who cannot be
// resolved is not a source of continuous churn, short enough that a contact who
// becomes resolvable is picked up within a probe cadence.
const presenceResolutionAskInterval = 5 * time.Minute

// presenceResolutionDue reports whether this contact may be asked about again,
// and records the ask when it says yes.
func (s *Service) presenceResolutionDue(identity domain.PeerIdentity) bool {
	now := s.presenceNow()
	s.presenceResolutionMu.Lock()
	defer s.presenceResolutionMu.Unlock()
	if s.presenceResolutionAskedAt == nil {
		s.presenceResolutionAskedAt = make(map[domain.PeerIdentity]presenceInstant)
	}
	if last, asked := s.presenceResolutionAskedAt[identity]; asked &&
		now.Since(last) < presenceResolutionAskInterval {
		return false
	}
	s.presenceResolutionAskedAt[identity] = now
	return true
}

// forgetPresenceResolution drops a removed contact's resolution: the durable
// intent outlives a restart, so a contact deleted while unresolved would keep a
// background lookup running for somebody who is no longer in the address book.
func (s *Service) forgetPresenceResolution(identity domain.PeerIdentity) {
	if identity.IsZero() {
		return
	}
	s.presenceResolutionMu.Lock()
	delete(s.presenceResolutionAskedAt, identity)
	s.presenceResolutionMu.Unlock()

	if s.identityResolver != nil {
		s.identityResolver.CancelReasonType(identity, identityIntentReasonPresence)
	}
}

// sealLivenessClaim builds the sealed reciprocity claim for a probe.
//
// When it returns false there is NO probe: presenceContactIsProbeable has
// already refused the contact for the same reason, and sending an unsealed
// probe anyway is the fail-open hole described there.
func (s *Service) sealLivenessClaim(target, attemptLabel domain.PeerIdentity, at presenceInstant) ([]byte, bool) {
	if s.identity == nil || s.identity.BoxPrivateKey == nil || target.IsZero() {
		return nil, false
	}
	local := domain.PeerIdentityFromWire(s.identity.Address)
	if local.IsZero() || s.trust == nil {
		return nil, false
	}
	// The CONTACT's stored box key, the same one the target's gate will
	// recompute the token against. The general knowledge cache holds keys for
	// every identity this node has heard of, and a stale one there produces a
	// token that verifies against nothing.
	targetBoxKey, known := s.trust.contactBoxKey(target)
	if !known {
		return nil, false
	}
	layer := s.datagramLayer()
	if layer == nil {
		return nil, false
	}
	sealed, err := protocol.SealLivenessProbe(
		// Wall(): the epoch this token is bound to is a wall-clock quantity by
		// protocol — both sides derive it from their own calendars.
		s.identity.BoxPrivateKey, layer.network, local, target, targetBoxKey, attemptLabel, at.Wall())
	if err != nil {
		log.Debug().Err(err).Str("identity", target.String()).Msg("presence_probe_claim_unsealed")
		return nil, false
	}
	return sealed, true
}

// presenceReachabilityTimeout bounds the reachability question. It is a local
// computation over declared capabilities and the routing table — no I/O — so
// this is a guard against a pathological stall, not an expected wait.
const presenceReachabilityTimeout = 2 * time.Second

// contactForPresence reports whether presence is kept for this identity.
// Presence is a question about the address book, so anybody else's proof is a
// perfectly good lookup answer and not a fact this node stores.
func (s *Service) contactForPresence(identity domain.PeerIdentity) bool {
	return s.trust != nil && !identity.IsZero() && s.trust.isTrustedContact(identity)
}

// publishPresenceChange republishes the projection after an event has already
// been RECORDED elsewhere.
//
// The split exists because recording and publishing have different locking
// needs: recording must be serialized with the other outcome of the same probe
// (prober mutex, no I/O), while publishing reaches into routing and the event
// bus and must hold nothing.
func (s *Service) publishPresenceChange() {
	s.refreshPresenceSnapshot()
}

// publishPresenceProof completes a verified proof: the durable "last seen"
// stamp, then the republish.
//
// The durable half matters more than it looks. Without it a contact proven
// present only through transit — never over a session of their own — left no
// trace once the proof expired, and the interface fell back to whatever old
// LastOnlineAt or PeerHealth timestamp it had, or showed nothing at all. A
// proof is this node's own observation, made with its own clock, which is
// exactly what that field is allowed to hold.
func (s *Service) publishPresenceProof(identity domain.PeerIdentity) {
	if s.contactForPresence(identity) {
		// UTC here and only here on this path: the value is persisted, and a
		// monotonic reading neither survives that nor means anything after it.
		// Wall(): the value is PERSISTED, and a monotonic reading neither
		// survives that nor means anything after it.
		observedAt := s.presenceNow().Wall().UTC()
		s.goBackground(func() {
			if _, err := s.trust.recordLastOnlineAt(
				[]domain.PeerIdentity{identity}, observedAt, arrivalPresencePersistInterval,
			); err != nil {
				log.Warn().Err(err).Str("identity", identity.String()).
					Msg("presence_proof_last_online_persist_failed")
			}
		})
		if source, ok := s.identityPresenceSource(); ok {
			// The same event the DM arrival path publishes, for the same
			// reason: the interface lives on events after startup, so a
			// durable write nothing announces would not reach a running
			// sidebar until the next launch.
			ebus.PublishIdentityPresenceObserved(s.eventBus, ebus.IdentityPresenceChange{
				Source:     source,
				Identities: []domain.PeerIdentity{identity},
				ChangedAt:  observedAt,
			})
		}
	}
	s.refreshPresenceSnapshot()
}

// notePresenceProof records that a contact answered one of our probes with a
// valid target_proof. Called from the identity resolver, which is where the
// signature is verified — presence never re-verifies, it is told.
func (s *Service) notePresenceProof(identity domain.PeerIdentity) {
	if s.presenceProjector == nil || !s.contactForPresence(identity) {
		return
	}
	if s.presenceProjector.noteProof(identity, s.presenceNow(), presenceAliveValidity) {
		s.publishPresenceProof(identity)
	}
}

// notePresenceSignedFrame records a frame carrying the contact's verified
// signature. Free evidence: nobody scheduled it, and an active conversation
// therefore costs no probes at all.
func (s *Service) notePresenceSignedFrame(identity domain.PeerIdentity) {
	if s.presenceProjector == nil || identity.IsZero() {
		return
	}
	if s.trust == nil || !s.trust.isTrustedContact(identity) {
		return
	}
	// Republished only when the evidence says something new. A contact who is
	// already alive on the same kind of evidence has simply had their window
	// extended, and an active conversation would otherwise pay for a full
	// projection — a routing lookup per contact — on every single message.
	if s.presenceProjector.notePassive(identity, s.presenceNow(), presenceAliveValidity) {
		s.refreshPresenceSnapshot()
	}
}

// notePresenceSessionClosed records an attributable close of a contact's
// session. It is stronger than the route, which the withdrawal grace period
// deliberately keeps alive for another twenty seconds — and that gap is the
// commonest false green there is.
// observedAt is when the close was OBSERVED, not when this record is written.
// The two differ by however long the close takes to travel through session
// accounting, datagram teardown and route bookkeeping, and using the later of
// them let a close outrank a proof that had genuinely arrived after it.
// epoch is the number of the 1 → 0 transition, minted under peerMu; observedAt
// is only used against evidence of LIFE, which has no such shared lock.
func (s *Service) notePresenceSessionClosed(identity domain.PeerIdentity, observedAt presenceInstant, epoch uint64) {
	if s.presenceProjector == nil || identity.IsZero() {
		return
	}
	if s.trust == nil || !s.trust.isTrustedContact(identity) {
		return
	}
	// One write, and the delivery occasion it opens is part of it
	// (presenceRecord.departures). Nothing is pushed into the delivery domain
	// from here: a separate bump — before or after — leaves a window in which a
	// proof publishes a return that spends an occasion nobody has opened yet,
	// and for a contact reachable through transit nothing else ever opens one.
	//
	// A close older than the evidence we already hold is dropped, which is why
	// this reports whether anything changed.
	if !s.presenceProjector.noteSessionClosed(identity, observedAt, epoch) {
		return
	}
	// Published immediately rather than at the next pass. This is the event
	// the whole feature is measured by — the moment a contact stops being
	// green — and making it wait even one cadence would put part of the delay
	// back.
	s.refreshPresenceSnapshot()
	log.Debug().
		Str("identity", identity.String()).
		Msg("presence_offline_session_closed")
}

// presenceDeparturesFor is how many times presence has seen this contact go.
//
// The delivery retry's second occasion counter, and the accessor exists so the
// counter can be READ from the delivery domain without that domain knowing
// where it lives. Zero when there is no projector or no record — which is
// exactly right for a node that has never observed a departure.
//
// Safe to call under any lock: the projector's mutex is a leaf and nothing
// under it calls back. Callers nonetheless read it BEFORE taking deliveryMu
// where they can, because an early read leaves an occasion unspent and a late
// one could spend the same occasion twice.
func (s *Service) presenceDeparturesFor(identity domain.PeerIdentity) uint64 {
	if s.presenceProjector == nil {
		return 0
	}
	return s.presenceProjector.departuresFor(identity)
}

// notePresenceSessionReturned withdraws a recorded session close because a
// session with that contact is up again.
//
// It exists for the reconnect that lands INSIDE the withdrawal grace window.
// There the deferred withdrawal is cancelled and the route is deliberately left
// untouched, so the projection sees no change at all — and the close, which is
// spent by a route going and coming back, would never be spent. A contact who
// merely reconnected quickly would be stuck.
//
// epoch is the number of the 0 → 1 transition, minted under peerMu, and it is
// recorded even when there is no close to withdraw yet: a reconnect can overtake
// the close of the PREVIOUS session on its way here, and the number is what
// makes that close lose when it finally arrives.
//
// It is not evidence of life on its own: a session proves a socket, not the key
// owner. So it clears the close and nothing more; green still requires a proof.
func (s *Service) notePresenceSessionReturned(identity domain.PeerIdentity, epoch uint64) {
	if s.presenceProjector == nil || identity.IsZero() {
		return
	}
	if s.trust == nil || !s.trust.isTrustedContact(identity) {
		return
	}
	if s.presenceProjector.noteSessionReturned(identity, epoch) {
		s.refreshPresenceSnapshot()
	}
}
