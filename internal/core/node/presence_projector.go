package node

import (
	"sort"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// presence_projector.go decides what this node believes about each CONTACT's
// liveness, from four kinds of evidence that arrive at different times:
//
//	proof     — the contact signed a target_proof for one of our probes;
//	passive   — a frame carrying their verified signature arrived;
//	session   — their session with us closed, attributably;
//	route     — what routing currently says, evaluated at projection time.
//
// The first three are events and are recorded when they happen. The fourth is
// asked, not remembered: a route is a fact about right now, and an event-derived
// copy of it goes stale precisely when it matters — the same lesson the delivery
// re-send learned when it started ASKING routing each tick instead of assembling
// a picture from events observed elsewhere.
//
// # Why this is keyed on contacts and not on identities
//
// Presence is a question about people the user knows, and there are tens of
// those, not tens of thousands. Keeping records only for contacts bounds the
// state by the address book rather than by the size of the network, which is
// what lets this survive the routing table shrinking under it: the projector
// asks routing a question per contact, and if one day routing cannot answer,
// only the fallback disappears — not the mechanism.
//
// It also removes the leak class outright. Records are rebuilt from the contact
// list on every projection, so a removed contact cannot leave one behind; this
// codebase has paid twice for maps that only grew (the ban domain, seenReceipts)
// and the third one is cheaper to prevent than to find.

// presenceDetectMult is how many probes in a row must go unanswered before a
// contact is called offline. Three is the BFD default (RFC 5880 §6.8.4): one
// lost probe is a lost packet, and an exponential penalty of the route-flap
// kind is wrong here because a phone moving from Wi-Fi to LTE is normal
// behaviour, not misbehaviour. See docs/protocol/presence.md §3 rule 3.
const presenceDetectMult = 3

// presenceRouteState is what routing says about one contact AT PROJECTION TIME.
// Three values and not a bool, because the difference between the last two is
// the whole of docs/protocol/presence.md §3 rule 2.
type presenceRouteState uint8

const (
	// presenceRouteAbsent: no usable route, and no suppression of ours
	// explains its absence. This is an observation about the peer.
	presenceRouteAbsent presenceRouteState = iota

	// presenceRoutePresent: a usable route exists.
	presenceRoutePresent

	// presenceRouteSuppressed: routing has entries for this contact but WE
	// are refusing them — quarantine, flap hold-down, seq hold-down, K-cap
	// eviction. This is a fact about us, and it must never be reported as a
	// fact about them.
	presenceRouteSuppressed
)

// presenceInputs is everything the projector needs for one pass. It is passed
// in rather than reached for, so the projector holds no locks of its own while
// routing, trust and aggregate status are being consulted — and so it can be
// tested without a Service.
type presenceInputs struct {
	Now presenceInstant
	// LocalConnectivity is false when OUR network is down or reconnecting.
	// When it is false nothing else in this struct means anything: an empty
	// routing table then describes our own outage, not fifty departures.
	LocalConnectivity bool
	// Contacts is the full contact set for this pass. Records for
	// identities absent from it are dropped.
	Contacts []domain.PeerIdentity
	// RouteState answers the routing question per contact.
	RouteState func(domain.PeerIdentity) presenceRouteState
	// Probeable answers whether the contact can respond to a liveness probe
	// at all. Derived from what they DECLARED, never from their silence:
	// silence from an old build is indistinguishable from silence from a
	// dead one (docs/protocol/presence.md §4).
	Probeable func(domain.PeerIdentity) bool
}

// presenceRecord is the per-contact evidence, kept between passes. Only what
// events deposit lives here; anything derivable from routing is recomputed.
type presenceRecord struct {
	// aliveSince and aliveFor are the evidence window that is currently in
	// force: when the winning evidence was observed, and how long it is good
	// for. A zero aliveFor means no evidence was ever seen, which is not the
	// same as evidence that has expired.
	//
	// Stored as (instant, length) rather than as a deadline because a deadline
	// can only be tested by comparing two instants, and that comparison is
	// exactly what a suspended machine and a stepped clock each get wrong in
	// opposite directions. A length is tested with presenceElapsed, which gets
	// both right.
	aliveSince presenceInstant
	aliveFor   time.Duration
	// aliveSource is which kind of evidence owns that window.
	aliveSource domain.PresenceSource
	// aliveAt is WHEN that evidence was observed, as opposed to when it
	// expires. It exists to order evidence against a session close: the two
	// arrive from different goroutines, and a close observed BEFORE a proof
	// used to wipe it anyway — turning a contact who had just answered into
	// `probing` or `offline` for as long as it took to ask again.
	aliveAt presenceInstant
	// sessionEpoch is the number of the newest session transition applied to
	// this record (Service.nextSessionTransitionLocked). Zero means none.
	//
	// It is what orders a close against a reconnect, and it is a COUNTER and
	// not a timestamp on purpose. The two events are causally ordered in the
	// world — you cannot reconnect before you disconnect — and not ordered in
	// this process: a close travels through session accounting, datagram
	// teardown and route bookkeeping before it lands here, and a reconnect can
	// overtake it. Every attempt to settle that with a clock failed on one of
	// three counts: readings taken before the lock that decides the transition
	// can invert, a coarse clock can tie them, and a clock stepped backwards
	// reverses them. The number is minted under that lock, so it IS the order.
	//
	// A transition whose number is not greater than this one is stale and is
	// dropped whole.
	sessionEpoch uint64
	// departures counts the times PRESENCE has seen this contact go: a probe
	// that went unanswered, or an attributable session close.
	//
	// It is the delivery retry's occasion counter for the departures routing
	// cannot see — a contact reachable through a transit hop shows the
	// delivery pass no absence at all, so their visit never ends and their
	// return used to earn nothing. It lives HERE, beside the evidence it
	// counts, because the two have to move together: a counter the presence
	// service bumped separately left a window in which a proof could publish a
	// return that spent an occasion nobody had opened yet, and no ordering of
	// the two statements closes it — only one lock does.
	//
	// Counting DEPARTURES and not returns is the other half of the rule.
	// Several observers of one return must grant one accelerated attempt, and
	// they do, because none of them moves this number; several observations of
	// one departure may move it twice, which costs nothing, because a return
	// spends the current value once.
	departures uint64
	// missedProbes counts CONSECUTIVE unanswered probes; evidence of life
	// NEWER than the last of them clears the count, so three means three in a
	// row.
	missedProbes int
	// lastStrikeAt is when the most recent of those probes was given up on.
	//
	// The count alone cannot say whether a piece of evidence supersedes it, and
	// evidence arrives from goroutines that do not take turns: a message can be
	// observed before a probe times out and reach the projector after it. Kept
	// so negativeAt can answer "what is the newest thing that said they are not
	// there", which is what positive evidence has to be newer than.
	lastStrikeAt presenceInstant
	// closedAt is when their session with us last closed attributably. It
	// outranks a live route, because a route survives its owner by design
	// (the withdrawal grace period) and that is exactly the 22.5 seconds of
	// false green being removed.
	closedAt presenceInstant
	// routeGoneSinceEvidence records that we have since observed the route to
	// this contact actually disappear.
	//
	// It is what stops our NEGATIVE evidence — a recorded close, accumulated
	// strikes — from becoming permanent. That evidence describes a moment; the
	// route that is still visible immediately after it is the SAME route, held
	// open by the withdrawal grace period, so it proves nothing and must not
	// clear anything. A route that shows up after we have seen the old one go
	// IS new information: it means their announcements stopped and started
	// again, which is a period our evidence predates. At that point the
	// evidence is spent.
	//
	// Without this bit a close outranked the route forever, and a contact who
	// came back was grey until something else happened to them — for a contact
	// who cannot be probed, that meant grey for good. The strike count had the
	// same shape of bug for one round longer: three silent probes pinned a
	// contact to `offline`, rule 4 outranks every route rule below it, and a
	// contact who genuinely returned stayed grey until the next periodic slot
	// — up to a full jittered cadence, with no probe brought forward, because
	// the transition into `probing` never happened.
	routeGoneSinceEvidence bool
	// returnPending marks a contact whose evidence of life arrived while we
	// had a REASON TO DOUBT them — unanswered probes, or a recorded close.
	//
	// It exists because the projection cannot express that event. A proof is
	// valid for 450 s and a probe goes out every 150 s, so a contact can miss
	// one or two probes, be unreachable for the minutes in between, and answer
	// again while the earlier window is still open: rule 2 reads `online` at
	// both ends, and the comparison of two projections sees nothing at all.
	// Nor does routing — the route through a transit outlives the outage that
	// caused the silence, so no route event fires either. Both of the things
	// that would otherwise notice a return are blind to exactly this case,
	// which is the commonest one for a phone changing networks.
	//
	// The flag is the third witness, and it is only ever set by evidence that
	// ENDED a doubt, never by evidence that merely extended a window. That is
	// what keeps it a wake-up rather than a poll: a contact in an active
	// conversation deposits passive evidence per message and none of it sets
	// this.
	returnPending bool
	// lastState is what the previous pass concluded. It exists so a probe is
	// triggered on the TRANSITION into probing, not on every pass that finds
	// the contact still there.
	//
	// Without it the trigger fired every two seconds for as long as the state
	// held, and since an unanswered attempt is dropped after 30 s, the next
	// pass immediately re-armed it — three probes in ~90 s against a stated
	// cadence of 150 s. The cadence was not being slowed by the trigger; it
	// was being replaced by it.
	lastState domain.PresenceState
}

// presenceProjector owns the per-contact evidence.
//
// Its mutex is a leaf: it is taken for the duration of a projection pass and
// for each event, and no I/O, event publication or other domain mutex is
// acquired underneath it. The callbacks in presenceInputs are invoked BEFORE
// the lock is taken, for that reason.
type presenceProjector struct {
	mu      sync.Mutex
	records map[domain.PeerIdentity]presenceRecord
	// probeNow is what the last pass decided is worth asking immediately:
	// contacts with a path and nothing proven along it. Handed to the prober
	// by the caller, outside this lock.
	probeNow []domain.PeerIdentity
	// returned is what the last pass saw come back without the projection
	// changing (see presenceRecord.returnPending). Handed to the delivery
	// wake-up by the caller, outside this lock.
	returned []domain.PeerIdentity
}

func newPresenceProjector() *presenceProjector {
	return &presenceProjector{records: make(map[domain.PeerIdentity]presenceRecord)}
}

// noteProof records that the contact signed a target_proof for us: the owner of
// the secret key answered our question, in this attempt. It is the strongest
// signal in the system and clears the strike count.
func (p *presenceProjector) noteProof(identity domain.PeerIdentity, at presenceInstant, validity time.Duration) bool {
	return p.noteAlive(identity, at, validity, domain.PresenceSourceProof)
}

// notePassive records a frame carrying the contact's verified signature. Nobody
// chose when it would arrive, which makes it weaker in timing and no weaker in
// authorship — and free, which is why an active conversation costs no probes.
func (p *presenceProjector) notePassive(identity domain.PeerIdentity, at presenceInstant, validity time.Duration) bool {
	return p.noteAlive(identity, at, validity, domain.PresenceSourcePassive)
}

// presenceEvidenceKind is which way a piece of evidence points.
//
// It exists so the staleness rule below can be written ONCE. Three separate
// hand-written comparisons is what this file had, and the third was simply
// missing: a probe timeout recorded a strike over evidence newer than itself,
// which broke the consecutive-probe semantics — a contact could reach `offline`
// after two misses instead of three — and moved the delivery occasion counter
// for a departure that had already been contradicted.
type presenceEvidenceKind uint8

const (
	// evidenceAlive: the contact themselves did something — a target_proof, a
	// signed frame.
	evidenceAlive presenceEvidenceKind = iota
	// evidenceAbsent: we observed them not being there — an attributable
	// session close, a probe given up on.
	evidenceAbsent
	// evidenceNone: the observation makes no claim about liveness at all — a
	// session coming back up proves a socket, not the key owner. Ordered by its
	// transition number alone, and never gated against evidence.
	evidenceNone
)

// contradictedAt is the newest observation on record pointing the OTHER way.
// Zero when there is none.
func (r presenceRecord) contradictedAt(kind presenceEvidenceKind) presenceInstant {
	if kind == evidenceAlive {
		if r.lastStrikeAt.ObservedAfter(r.closedAt) {
			return r.lastStrikeAt
		}
		return r.closedAt
	}
	return r.aliveAt
}

// stale reports whether an observation of this kind, made at `at`, is older
// news than what the record already holds from the other direction.
//
// Both orderings happen, and neither is a race between goroutines that take
// turns: an observation is made, its goroutine is descheduled before this
// projector's mutex, the contrary event happens in the meantime, and the older
// observation lands on top. Applied blind, a stale proof clears a close and the
// strikes and opens a fresh 450 s window — a contact who left stays green for
// seven minutes — and a stale strike walks a contact who is demonstrably there
// towards `offline`.
//
// Ties go to the ABSENT observation in both directions, which is the
// recoverable one: a false grey is resolved by the next probe, a false green
// lasts the whole validity window.
func (r presenceRecord) stale(kind presenceEvidenceKind, at presenceInstant) bool {
	if kind == evidenceNone {
		return false
	}
	contrary := r.contradictedAt(kind)
	if contrary.IsZero() {
		return false
	}
	if kind == evidenceAlive {
		return !at.ObservedAfter(contrary)
	}
	return contrary.ObservedAfter(at)
}

// noteObservation is the ONE way anything is written into a record, and it is
// the reason the staleness rule cannot be forgotten by the next kind of
// evidence somebody adds: there is no other path to the map.
//
// apply reports whether it changed something a reader would see.
func (p *presenceProjector) noteObservation(
	identity domain.PeerIdentity,
	obs presenceObservation,
	apply func(record *presenceRecord) bool,
) bool {
	if identity.IsZero() {
		return false
	}
	if obs.kind != evidenceNone && obs.at.IsZero() {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	record := p.records[identity]

	// The session axis, when this observation belongs to one. It is checked and
	// STORED in the same critical section as everything below, and splitting
	// the two was a real bug for exactly one round: the close released the
	// mutex between recording its epoch and recording itself, a reconnect took
	// the lock in that window, found no close to withdraw, and the close then
	// landed on top of a live session with nothing left to re-check it against.
	epochStored := false
	if obs.epoch != 0 {
		if obs.epoch <= record.sessionEpoch {
			return false
		}
		record.sessionEpoch = obs.epoch
		epochStored = true
	}

	if record.stale(obs.kind, obs.at) {
		if epochStored {
			// The transition has been SEEN even though its evidence is older
			// news, so nothing older may claim to be newer afterwards.
			p.records[identity] = record
		}
		return false
	}
	changed := apply(&record)
	p.records[identity] = record
	return changed
}

// presenceObservation is one thing observed about a contact, and it carries
// everything that decides whether it is still news — so that all of it is
// decided under one lock.
type presenceObservation struct {
	kind presenceEvidenceKind
	// at is when it was OBSERVED. Meaningless for evidenceNone.
	at presenceInstant
	// epoch, when non-zero, is the session transition this observation belongs
	// to (Service.nextSessionTransitionLocked). Zero for observations that are
	// not session transitions — a proof, a signed frame, a probe timeout.
	epoch uint64
}

// noteAlive records evidence of life and reports whether it CHANGED anything a
// reader would see.
//
// The return value is what keeps an active conversation cheap. Every incoming
// message from a contact is passive evidence, and republishing the projection
// on each one would run a routing lookup per contact per message — under a
// burst, thousands of lock acquisitions on the message ingest path, to say
// something already said. A contact who is already alive, from the same kind of
// evidence, with no strikes and no close outstanding, has nothing new to
// report: their window simply extends.
func (p *presenceProjector) noteAlive(identity domain.PeerIdentity, at presenceInstant, validity time.Duration, source domain.PresenceSource) bool {
	if validity <= 0 {
		return false
	}
	return p.noteObservation(identity, presenceObservation{kind: evidenceAlive, at: at},
		func(record *presenceRecord) bool {
			return recordAlive(record, at, validity, source)
		})
}

// recordAlive is the body of noteAlive, called with the record already gated by
// noteObservation. Reports whether it changed anything a reader would see.
func recordAlive(record *presenceRecord, at presenceInstant, validity time.Duration, source domain.PresenceSource) bool {
	// Was this contact already, visibly, alive on this same evidence?
	unchanged := record.aliveSource == source &&
		record.aliveFor > 0 && at.Since(record.aliveSince) < record.aliveFor &&
		record.missedProbes == 0 && record.closedAt.IsZero()

	// Whether this is a RETURN and not just a longer window. The two are the
	// same write below — the strikes and the close are cleared either way —
	// but only one of them is news to the delivery retry, and the projection
	// it produces cannot tell them apart afterwards.
	if record.missedProbes > 0 || !record.closedAt.IsZero() {
		record.returnPending = true
	}

	// A later proof never shortens an earlier one: two probes can answer out
	// of order, and the fresher answer is the one that counts. Both candidate
	// deadlines are built from instants of this process, so comparing them is
	// a choice between two windows and not a measurement of elapsed time.
	if at.Add(validity).ObservedAfter(record.aliveSince.Add(record.aliveFor)) {
		record.aliveSince = at
		record.aliveFor = validity
		record.aliveSource = source
	}
	// aliveAt tracks the newest evidence regardless of which window won, so a
	// session close can tell whether it is older news than what we know.
	if at.ObservedAfter(record.aliveAt) {
		record.aliveAt = at
	}
	// Evidence of life clears both the strikes and the close: they came
	// back, whatever we concluded a moment ago.
	record.missedProbes = 0
	record.closedAt = presenceInstant{}
	record.routeGoneSinceEvidence = false
	return !unchanged
}

// noteProbeUnanswered records one probe that went out and was not answered.
//
// The strike and the departure count move in ONE locked section: a delivery
// occasion opened a moment later, or a moment earlier, leaves a window in which
// a proof publishes a return with no occasion to spend.
func (p *presenceProjector) noteProbeUnanswered(identity domain.PeerIdentity, at presenceInstant) {
	p.noteObservation(identity, presenceObservation{kind: evidenceAbsent, at: at}, func(record *presenceRecord) bool {
		record.missedProbes++
		record.departures++
		if at.ObservedAfter(record.lastStrikeAt) {
			record.lastStrikeAt = at
		}
		return true
	})
}

// noteSessionClosed records an attributable close of the contact's session with
// us. It is deliberately stronger than the route, which the grace period keeps
// alive for another twenty seconds.
//
// Reports whether it was recorded. A close is DROPPED when evidence of life
// newer than the close's own observation is already on record: the two events
// reach this projector from different goroutines, and a close applied on top of
// a later proof wipes it, turning a contact who has just answered into
// `probing` or `offline` until somebody asks again. Ties go to the close, which
// is the recoverable direction — a false grey is resolved by the next probe,
// a false green lasts the whole validity window.
func (p *presenceProjector) noteSessionClosed(identity domain.PeerIdentity, at presenceInstant, epoch uint64) bool {
	return p.noteObservation(identity, presenceObservation{kind: evidenceAbsent, at: at, epoch: epoch},
		func(record *presenceRecord) bool {
			record.closedAt = at
			record.departures++
			// The proof they gave us before leaving no longer describes now.
			record.aliveSince = presenceInstant{}
			record.aliveFor = 0
			record.aliveSource = domain.PresenceSourceNone
			return true
		})
}

// departuresFor is how many times presence has seen this contact go. Read by
// the delivery retry to decide whether a return has already been paid for; zero
// for a contact nothing is known about.
//
// Its mutex is a leaf, so this may be called from under a domain mutex — which
// the delivery pass does. Nothing here calls back into the service.
func (p *presenceProjector) departuresFor(identity domain.PeerIdentity) uint64 {
	if identity.IsZero() {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.records[identity].departures
}

// noteSessionReturned records the session-up transition numbered `epoch` and
// withdraws a close older than it. Reports whether the PROJECTION changed — the
// epoch is always kept, whether or not it did.
//
// Keeping it unconditionally is the whole point. The close is normally spent by
// watching the route go and come back; a reconnect inside the withdrawal grace
// window produces neither, and a reconnect that OVERTAKES the close in this
// process finds nothing to withdraw at all. An earlier version returned
// immediately in that case and left no trace, so the stale close landed
// afterwards and stuck: for a contact with no direct route to watch, grey for
// the whole life of a live session.
//
// It is not evidence of life: a session proves a socket, not the key owner. So
// it clears a close and nothing more; green still requires a proof.
func (p *presenceProjector) noteSessionReturned(identity domain.PeerIdentity, epoch uint64) bool {
	if epoch == 0 {
		return false
	}
	// evidenceNone: a session proves a socket, not the key owner, so this is
	// ordered by its transition number alone and is never gated against a
	// strike or a proof. It goes through the same single path as everything
	// else so the epoch it stores is stored under the same lock that reads it.
	return p.noteObservation(identity, presenceObservation{kind: evidenceNone, epoch: epoch},
		func(record *presenceRecord) bool {
			if record.closedAt.IsZero() {
				// Nothing to withdraw — this reconnect got here first. The
				// epoch, already stored, is what makes the close that follows
				// it lose.
				return false
			}
			record.closedAt = presenceInstant{}
			record.routeGoneSinceEvidence = false
			return true
		})
}

// provenBeyond reports whether this contact's proof of life is still valid at
// the given instant. The prober uses it to skip a contact whose window has
// comfortable margin left — demand mode in its cheapest form, and the reason an
// active conversation produces no probe traffic at all.
func (p *presenceProjector) provenBeyond(identity domain.PeerIdentity, at presenceInstant) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, known := p.records[identity]
	if !known || record.aliveFor == 0 {
		return false
	}
	return at.Since(record.aliveSince) < record.aliveFor
}

// recordCount reports how many contacts currently have evidence stored. It
// exists for the leak guard in the tests; nothing in production reads it.
func (p *presenceProjector) recordCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

// project computes the presence of every contact and drops the records of
// identities that are no longer contacts.
//
// The order of the rules below is the contract, not a convenience: our own
// connectivity gates everything, then our own suppressions, then what the
// contact themselves last told us, and only then anything inferred from
// routing.
func (p *presenceProjector) project(in presenceInputs) domain.PresenceSet {
	// Ask every external question BEFORE taking the lock: the callbacks
	// reach into routing and trust, and holding a presence lock across them
	// would invert the acquisition order that the rest of the service keeps.
	type contactInput struct {
		identity  domain.PeerIdentity
		route     presenceRouteState
		probeable bool
	}
	gathered := make([]contactInput, 0, len(in.Contacts))
	for _, identity := range in.Contacts {
		if identity.IsZero() {
			continue
		}
		gathered = append(gathered, contactInput{
			identity:  identity,
			route:     in.routeStateOf(identity),
			probeable: in.probeableOf(identity),
		})
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	set := make(domain.PresenceSet, len(gathered))
	survivors := make(map[domain.PeerIdentity]presenceRecord, len(gathered))
	var probeNow []domain.PeerIdentity
	var returned []domain.PeerIdentity
	for _, contact := range gathered {
		record := p.records[contact.identity]
		// The record is advanced BEFORE it is read: a route we have now
		// observed to be gone, and a close it has therefore outlived, are
		// facts about this pass and belong in the evidence the same pass
		// reasons from.
		record = advanceRouteObservation(record, contact.route)

		presence := presenceFromEvidence(record, contact.route, contact.probeable, in)
		set[contact.identity] = presence

		// A path appeared and nothing has been proven along it. Asking NOW is
		// the difference between a contact turning green in one round trip and
		// turning green whenever the periodic pass next comes round — which,
		// with jitter, can be over three minutes away.
		//
		// Only on the TRANSITION, never for as long as the state holds: the
		// periodic cadence is what governs a contact who is simply not
		// answering, and re-arming it every pass silently replaced that
		// cadence with a much faster one.
		enteredProbing := presence.State == domain.PresenceProbing &&
			record.lastState != domain.PresenceProbing
		if enteredProbing && contact.probeable {
			probeNow = append(probeNow, contact.identity)
		}
		record.lastState = presence.State

		// A return is reported once, and only from a pass that can actually
		// SEE them present: a proof landing during our own outage projects
		// `unknown`, and announcing a return then would wake the delivery
		// retry for a contact this node cannot reach anyway. Holding the flag
		// instead reports it on the pass after connectivity comes back, which
		// is the first moment the wake-up can do anything.
		//
		// It is dropped unreported once the window that carried it has
		// expired: at that point the contact is being probed or is offline
		// again, and the return is no longer news about now.
		if record.returnPending {
			switch {
			case presence.State == domain.PresenceOnline:
				returned = append(returned, contact.identity)
				record.returnPending = false
			case record.aliveFor == 0 ||
				in.Now.Since(record.aliveSince) >= record.aliveFor:
				record.returnPending = false
			}
		}

		// Keep only records that still say something. An all-zero record
		// is what a fresh contact has, and storing it would grow the map
		// by one entry per contact for no information.
		if record != (presenceRecord{}) {
			survivors[contact.identity] = record
		}
	}
	p.records = survivors
	p.probeNow = sortedIdentities(probeNow)
	p.returned = sortedIdentities(returned)
	return set
}

// takeReturned returns and clears the contacts the last pass saw come back.
// Separate from project for the same reason takeProbeNow is: the caller acts
// on them with no presence lock held, and the delivery wake-up it calls takes
// deliveryMu.
func (p *presenceProjector) takeReturned() []domain.PeerIdentity {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.returned
	p.returned = nil
	return out
}

// takeProbeNow returns and clears the contacts the last pass wants asked
// immediately. Separate from project so the caller reaches the prober with no
// presence lock held.
func (p *presenceProjector) takeProbeNow() []domain.PeerIdentity {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.probeNow
	p.probeNow = nil
	return out
}

// advanceRouteObservation folds THIS pass's routing answer into the record.
//
// One transition matters: our negative evidence is spent once the route it was
// recorded against has been seen to disappear and then come back. Until the
// route goes, what we can see is the same route the grace period is holding
// open, and treating that as the contact's return would undo the evidence a
// moment after recording it.
//
// BOTH kinds of negative evidence are spent by it, and treating them
// differently was a bug. A close and three silent probes are the same sort of
// claim — "as of a moment now past, they were not there" — and rules 3 and 4
// both outrank every route rule below them. Clearing only the close left a
// probed contact pinned to `offline` across a genuine departure and return,
// with no transition into `probing` and therefore no probe brought forward:
// they stayed grey until the periodic slot came round, up to a full jittered
// cadence later.
//
// The cost is stated rather than hidden: transit churn that takes a route away
// and brings it back will also clear strikes against a contact who is really
// gone, and they will read `probing` for up to three more cadences before
// returning to `offline`. That is accepted because rule 6 already treats a
// route's DISAPPEARANCE as evidence about them; refusing to treat its return
// the same way is inconsistent in the direction that keeps a returned contact
// grey, which is the direction that lies to the user.
func advanceRouteObservation(record presenceRecord, route presenceRouteState) presenceRecord {
	if record.closedAt.IsZero() && record.missedProbes == 0 {
		// Nothing negative to spend. Leaving early also keeps a contact with
		// no evidence at all on the all-zero record that project() declines
		// to store, so the map stays the size of the address book minus the
		// quiet majority rather than exactly the address book.
		return record
	}
	if route == presenceRouteSuppressed {
		// A suppression is OURS and says nothing about whether the contact's
		// route went anywhere. Counting it as "the route disappeared" would
		// arm the clearing rule below on a fact about this node: a
		// two-minute black-hole cooldown would mark the old route gone, and
		// the moment it lifted, the SAME route would read as new and retire
		// the evidence — putting a contact who genuinely left back on the
		// fallback, green.
		return record
	}
	if route == presenceRouteAbsent {
		record.routeGoneSinceEvidence = true
		return record
	}
	if record.routeGoneSinceEvidence {
		// The route went away and a new one is here. Our evidence described a
		// moment that this route is newer than, so it is spent.
		record.closedAt = presenceInstant{}
		record.missedProbes = 0
		record.routeGoneSinceEvidence = false
	}
	return record
}

// presenceFromEvidence is the state machine of docs/protocol/presence.md §3 as one readable pass. It is
// a free function over a record so that it has no way to mutate anything: the
// projection is a reading of the evidence, and every write happens in the note*
// methods where the event that caused it is named.
func presenceFromEvidence(record presenceRecord, route presenceRouteState, probeable bool, in presenceInputs) domain.Presence {
	// 1. Our own connectivity gates everything. With our network down, an
	//    empty routing table describes US, and every conclusion drawn from
	//    it would be a claim about somebody else made from our own failure.
	if !in.LocalConnectivity {
		return domain.UnknownPresence(domain.PresenceUnknownNoLocalConnectivity)
	}

	// 2. Evidence from the contact themselves outranks everything we could
	//    infer — including a route that still exists, and including a
	//    session close we recorded before they came back.
	if record.aliveFor > 0 && in.Now.Since(record.aliveSince) < record.aliveFor {
		return domain.OnlinePresence(record.aliveSource)
	}

	// 3. An attributable session close is an observation about them, and it
	//    outranks the route that the grace period is still holding open.
	//
	//    What it does NOT do is decide the answer by itself when a path is
	//    still visible. Their session with us ended; whether they are gone
	//    from the network is a different claim, and one we can be wrong about
	//    when the contact is also reachable through transit. So:
	//
	//      - no usable path left  ⇒ offline. They closed and there is nowhere
	//        else they could be answering from.
	//      - a path still visible ⇒ NOT green, and asked immediately. For a
	//        contact we can probe that is `probing`, which resolves in one
	//        round trip; for one we cannot, it is honest ignorance rather
	//        than a route-derived green we have just been given reason to
	//        doubt.
	//
	//    This is what removes the false green at the moment of the close
	//    instead of one withdrawal grace period later, without inventing a
	//    false grey for a contact who is merely no longer OUR neighbour.
	if !record.closedAt.IsZero() {
		if route == presenceRouteSuppressed {
			// A suppression of ours is never evidence of absence, and a
			// recorded close does not change that: we still do not know
			// whether a path exists, only that we are refusing the ones we
			// have. The close is KEPT — it is spent by a real
			// absent → present transition, not by this — so a contact who
			// stays away still reaches offline once the suppression lifts.
			return domain.UnknownPresence(domain.PresenceUnknownRouteSuppressedLocally)
		}
		if route != presenceRoutePresent {
			return domain.OfflinePresence(domain.PresenceSourceSessionClosed)
		}
		if probeable {
			return domain.ProbingPresence()
		}
		return domain.UnknownPresence(domain.PresenceUnknownStale)
	}

	// 4. Enough consecutive silent probes is an observation about THEM, and it
	//    outranks our own route bookkeeping below.
	//
	//    The order here matters and was wrong the first time: a strike is only
	//    ever recorded for a probe that actually REACHED the network — a send
	//    the layer refused drops its attempt instead of arming a timeout
	//    (presence_prober.go, sendProbe) — so accumulated strikes cannot be an
	//    artefact of the same local condition that suppressed the route. Had
	//    suppression been checked first, a contact who genuinely stopped
	//    answering would read as `unknown` for as long as one stale unusable
	//    route sat in the table.
	if record.missedProbes >= presenceDetectMult {
		return domain.OfflinePresence(domain.PresenceSourceProbeTimeout)
	}

	// 5. A suppression of OURS explains a missing route without saying
	//    anything about the contact. It applies to probeable and non-probeable
	//    contacts alike — the fallback does not get to override it.
	if route == presenceRouteSuppressed {
		return domain.UnknownPresence(domain.PresenceUnknownRouteSuppressedLocally)
	}

	// 6. A route that vanished while we were healthy is the fast negative
	//    signal — the one thing presence still takes from routing.
	if route == presenceRouteAbsent {
		return domain.OfflinePresence(domain.PresenceSourceRouteObservation)
	}

	// 7. A route exists and nothing has been proven. For a contact that CAN
	//    answer, this is the short window before the answer arrives. For a
	//    contact that cannot answer at all, it is as much as we will ever
	//    know — see presence_route_fallback.go for why that is reported as
	//    presence rather than as ignorance, and for when it goes away.
	if !probeable {
		return presenceFromRouteFallback()
	}
	return domain.ProbingPresence()
}

func (in presenceInputs) routeStateOf(identity domain.PeerIdentity) presenceRouteState {
	if in.RouteState == nil {
		return presenceRouteAbsent
	}
	return in.RouteState(identity)
}

func (in presenceInputs) probeableOf(identity domain.PeerIdentity) bool {
	if in.Probeable == nil {
		return false
	}
	return in.Probeable(identity)
}

// sortedIdentities is used where a deterministic order matters (logs, tests).
func sortedIdentities(identities []domain.PeerIdentity) []domain.PeerIdentity {
	out := append([]domain.PeerIdentity(nil), identities...)
	sort.Slice(out, func(i, j int) bool { return out[i].Compare(out[j]) < 0 })
	return out
}
