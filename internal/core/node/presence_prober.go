package node

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	mathrand "math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// presence_prober.go asks contacts whether they are there, and believes only a
// signature in reply.
//
// # Why this is not the identity resolver
//
// The probe is byte-identical to a lookup — get_identity with target_proof — so
// reusing the resolver is the obvious move, and it does not work. The resolver
// is a RESOLUTION machine: a verified answer FINISHES it
// (docs/protocol/identity-lookup.md), it arms a 30 s cooldown per target, and
// it carries durable intents with a seven-day life. Every one of those is
// correct for "find out who this is" and wrong for "are you still there",
// which is a question with no terminal state and one this node asks forever.
//
// So the prober keeps its own attempts, its own schedule and its own labels.
// What it does NOT keep is a second copy of the verification: a proof is
// verified exactly once, by the same protocol functions the resolver uses, and
// the outcome is handed to the projector.
//
// # What a valid answer proves
//
// target_proof is signed by the holder of the contact's secret key over this
// attempt's label and the hash of this exact question. A relay cannot forge it,
// a cache cannot replay it into a different attempt, and a machine that merely
// holds the socket open cannot produce it — which is the difference between
// this and a ping. What it does NOT prove is the instant: there is no timestamp
// inside the signature, so the honest reading is "alive somewhere inside this
// attempt's window", and the validity window is sized accordingly.

const (
	// presenceProbeInterval is the base cadence. It is the `visible` class of
	// docs/protocol/presence.md §4: contacts are probed on the assumption that somebody may be
	// looking at the list, because this layer has no signal for whether the
	// window is focused.
	//
	// Choosing the SHORTEST class instead would be worse than wasteful: a
	// 30 s validity renewed by a 150 s probe expires between probes, and the
	// contact would blink out of green for no reason. Cadence and validity
	// are one decision, not two (see presenceAliveValidity).
	presenceProbeInterval = 150 * time.Second

	// presenceProbeJitter spreads probes by ±25 %. An exactly periodic emitter
	// is a fingerprint of the node all by itself — the reason SimpleX
	// randomises its group pings and bitchat jitters its announces.
	presenceProbeJitter = 0.25

	// presenceProbeTimeout is how long one attempt may stay open before it
	// counts as unanswered. It must exceed a multi-hop round trip and stay
	// well below the cadence, so that a timeout is attributed to the probe
	// that caused it and not to the next one.
	presenceProbeTimeout = 30 * time.Second

	// presenceProbeMaxInFlight bounds concurrent open attempts. Presence is a
	// convenience; it must never be the reason this node is busy.
	presenceProbeMaxInFlight = 8

	// presenceProbeTickInterval is how often the scheduler looks for work.
	presenceProbeTickInterval = 5 * time.Second

	// presenceProbeRenewLead is how much margin a proof must have left before
	// the prober stops asking again. It is what decides HOW MANY renewal
	// attempts fit before a proof expires, and 45 s allowed exactly one.
	//
	// That single attempt was the whole safety margin of a green contact: one
	// lost packet and a perfectly live person dropped out of `online` at
	// exactly presenceAliveValidity — 7.5 minutes — and stayed non-green until
	// the next cadence slot came round, another ~150 s later. The three-strike
	// hysteresis does not help here; it guards the way INTO `offline` and
	// nothing guarded the way out of `online`.
	//
	// It also silently replaced the cadence. The demand-mode skip suppresses
	// probes while a proof has margin, so the first probe after a successful
	// one went out at validity − lead = 405 s, not at the stated 150 s. The
	// arithmetic the validity is built on — 150 s × Detect Mult 3 — was
	// therefore never what ran, and reaching `offline` took about twelve
	// minutes instead of 450 s.
	//
	// Sized as "two full jittered cadences plus a timeout" so three attempts
	// fit inside the validity window: a contact survives two consecutive
	// losses, and the probe cadence is the stated 150 s again. Demand mode is
	// untouched — an active conversation renews the same window through
	// passive evidence and still costs no probes.
	presenceProbeRenewLead = 2*presenceProbeInterval + presenceProbeTimeout
)

// presenceProbeAttempt is one question in flight.
type presenceProbeAttempt struct {
	target domain.PeerIdentity
	// firstHop is the neighbour the layer handed this probe to. Remembered so
	// a verified answer can confirm THAT hop — the only moment at which a
	// first hop is known to have really carried our traffic.
	firstHop domain.PeerIdentity
	// qHash binds the answer to THIS question: the proof signs it, so an
	// answer to a different question cannot satisfy this attempt.
	qHash [sha256.Size]byte
	// onWire holds ONE channel per OFFER the send made, each closed by the
	// netcore writer that accepted that offer once the bytes left the process.
	// Empty until the emitter reaches the transport, and empty forever if the
	// frame never got that far.
	//
	// A slice and not one channel, because the walk can hand the frame to more
	// than one socket: a queue may accept an item and then answer a refusal —
	// the gate it reads is checked after the offer — while its writer goes on
	// draining and closes the ack anyway. One shared channel then took two
	// closes and panicked. Per-offer channels make that harmless, and "the
	// frame reached the wire" is simply the OR of them.
	//
	// The whole thing replaced a layer-wide counter of "frames lost after the
	// layer said queued", which was wrong in both directions: it missed losses
	// deeper than the class queue, so a live contact could still be walked to
	// `offline`; and being layer-WIDE, one unrelated datagram dying anywhere
	// suppressed this probe's timeout, so under sustained backpressure no
	// contact could ever be called absent.
	onWire []<-chan struct{}
	sentAt presenceInstant
}

// presenceProber owns the schedule and the open attempts.
//
// Its mutex is a leaf and is never held across a send: the enqueue reaches into
// the datagram pipeline, which takes locks of its own.
type presenceProber struct {
	svc *Service

	mu       sync.Mutex
	attempts map[domain.PeerIdentity]presenceProbeAttempt
	nextDue  map[domain.PeerIdentity]presenceInstant
}

func newPresenceProber(svc *Service) *presenceProber {
	return &presenceProber{
		svc:      svc,
		attempts: make(map[domain.PeerIdentity]presenceProbeAttempt),
		nextDue:  make(map[domain.PeerIdentity]presenceInstant),
	}
}

// Run drives the schedule until ctx is done.
func (p *presenceProber) Run(ctx context.Context) {
	ticker := time.NewTicker(presenceProbeTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.expireStaleAttempts(p.now())
			p.probeDue(ctx, p.now())
		}
	}
}

// now keeps the monotonic reading, because everything this engine does with it
// is an interval: the timeout of an open attempt and the next-due schedule.
// See Service.presenceNow.
func (p *presenceProber) now() presenceInstant { return p.svc.presenceNow() }

// expireStaleAttempts turns questions that were never answered into strikes.
//
// A strike is not an offline: presenceDetectMult of them in a row is, and any
// evidence of life clears the count.
//
// Claim AND record under ONE lock. Claiming alone is not enough: an earlier
// version removed the label under the mutex but wrote the strike after
// releasing it, so a proof arriving in between could clear the count and then
// have the strike land on top of it. Two probes of that shape in a row walk a
// live contact towards a false offline.
//
// The delivery occasion the strike opens rides INSIDE the same projector write
// (presenceRecord.departures), for the same reason and one round later: a
// version that opened it from here, before or after this section, left a window
// in which a proof could publish a return that spends an occasion nobody has
// opened yet. Nothing outside this function has to be told anything.
//
// The projector's mutex is a leaf and does no I/O, so taking it under this one
// adds an edge that cannot deadlock; the PUBLISH is what stays outside.
func (p *presenceProber) expireStaleAttempts(now presenceInstant) {
	projector := p.svc.presenceProjector

	var expired, discarded []domain.PeerIdentity
	p.mu.Lock()
	for label, attempt := range p.attempts {
		if now.Since(attempt.sentAt) < presenceProbeTimeout {
			continue
		}
		delete(p.attempts, label)
		if !probeReachedTheWire(attempt.onWire) {
			// This exact frame never reached a socket. docs/protocol/presence.md §4
			// is explicit: a probe that did not reach the network is not
			// evidence about the contact, so the attempt is DISCARDED — the
			// same treatment a send the layer refused outright gets — and
			// nothing is recorded either way.
			discarded = append(discarded, attempt.target)
			continue
		}
		if projector != nil {
			projector.noteProbeUnanswered(attempt.target, now)
		}
		expired = append(expired, attempt.target)
	}
	p.mu.Unlock()

	for _, target := range discarded {
		log.Debug().Str("identity", target.String()).
			Msg("presence_probe_discarded_local_loss")
	}
	for _, target := range expired {
		// Publishing is separate from recording: the third strike in a row is
		// what turns a contact grey, and nothing else is going to announce it.
		p.svc.publishPresenceChange()
		log.Debug().Str("identity", target.String()).Msg("presence_probe_unanswered")
	}
}

// probeNow brings these contacts' next probe forward to immediately.
//
// Called when the projection notices a path with nothing proven along it —
// typically a contact who has just come back. Without it the answer waits for
// the periodic slot, which with jitter is up to ~187 s away, and a returning
// contact stays un-green for all of it.
func (p *presenceProber) probeNow(contacts []domain.PeerIdentity) {
	if len(contacts) == 0 {
		return
	}
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, contact := range contacts {
		if _, open := p.attemptForTargetLocked(contact); open {
			// One question at a time per contact: bringing a probe forward
			// while its predecessor is still open would make two answers
			// race and both count.
			continue
		}
		p.nextDue[contact] = now
	}
}

// attemptForTargetLocked reports whether a probe to this contact is in flight.
// Caller must hold p.mu.
func (p *presenceProber) attemptForTargetLocked(target domain.PeerIdentity) (domain.PeerIdentity, bool) {
	for label, attempt := range p.attempts {
		if attempt.target == target {
			return label, true
		}
	}
	return domain.PeerIdentity{}, false
}

// probeDue sends probes for every contact whose turn it is.
func (p *presenceProber) probeDue(ctx context.Context, now presenceInstant) {
	layer := p.svc.datagramLayer()
	if layer == nil || layer.pipeline == nil {
		return
	}

	presence := p.svc.PresenceSnapshot()
	inFlight := p.inFlightCount()

	for _, contact := range sortedIdentities(p.svc.presenceContacts()) {
		if inFlight >= presenceProbeMaxInFlight {
			return
		}
		if !p.isDue(contact, now) {
			continue
		}
		// Demand mode (BFD §6.6) in its cheapest form: a contact whose
		// proof still has a comfortable margin is not asked again. This is
		// what makes an active conversation cost nothing — every message
		// they send renews the same window a probe would have.
		if p.provenFreshEnough(presence, contact, now) {
			continue
		}
		if !p.svc.presenceContactIsProbeable(contact) {
			// Not asking is the whole point for these: they cannot answer,
			// and a probe they cannot answer would produce strikes and end
			// as a false offline. They are served by the route fallback.
			p.deferNext(contact, now)
			continue
		}
		if p.sendProbe(ctx, contact, now) {
			inFlight++
		}
		p.deferNext(contact, now)
	}
}

func (p *presenceProber) inFlightCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.attempts)
}

// isDue reports whether this contact's turn has come. It TAKES the mutex — the
// name deliberately avoids the *Locked suffix, which in this codebase means the
// opposite (the caller must already hold it).
func (p *presenceProber) isDue(contact domain.PeerIdentity, now presenceInstant) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	due, known := p.nextDue[contact]
	if !known {
		// First sight of this contact: probe on the next pass rather than
		// immediately, so adding an address book does not produce a burst.
		p.nextDue[contact] = now.Add(p.jittered(presenceProbeTickInterval))
		return false
	}
	return now.Reached(due)
}

// deferNext schedules the following probe, and prunes contacts that have gone
// away so neither map outlives the address book.
func (p *presenceProber) deferNext(contact domain.PeerIdentity, now presenceInstant) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextDue[contact] = now.Add(p.jittered(presenceProbeInterval))
}

// forgetContacts drops schedule entries for identities that are no longer
// contacts. Called from the projection pass, which already knows the set.
func (p *presenceProber) forgetContacts(current map[domain.PeerIdentity]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for contact := range p.nextDue {
		if _, still := current[contact]; !still {
			delete(p.nextDue, contact)
		}
	}
	for label, attempt := range p.attempts {
		if _, still := current[attempt.target]; !still {
			delete(p.attempts, label)
		}
	}
}

func (p *presenceProber) jittered(base time.Duration) time.Duration {
	spread := float64(base) * presenceProbeJitter
	// rand/v2 top-level source: this is scheduling jitter, not a secret.
	return time.Duration(float64(base) - spread + mathrand.Float64()*2*spread)
}

func (p *presenceProber) provenFreshEnough(set domain.PresenceSet, contact domain.PeerIdentity, now presenceInstant) bool {
	presence := set.Get(contact)
	if !presence.IsProven() {
		return false
	}
	return p.svc.presenceProjector.provenBeyond(contact, now.Add(presenceProbeRenewLead))
}

// sendProbe puts one get_identity on the wire and registers the attempt.
//
// The label is fresh entropy per attempt and is NOT this node's address: the
// return path is the chain of reverse-state crumbs each hop keeps, so the
// question does not carry who asked it. Two probes to the same contact are
// unlinkable to a transit for the same reason.
func (p *presenceProber) sendProbe(ctx context.Context, target domain.PeerIdentity, now presenceInstant) bool {
	var label domain.PeerIdentity
	if _, err := cryptorand.Read(label[:]); err != nil {
		log.Error().Err(err).Msg("presence_probe_label_entropy_failed")
		return false
	}

	request := protocol.GetIdentityPayload{
		V:           domain.IdentityLookupSchemaVersion,
		TargetProof: true,
	}
	// The reciprocity claim, bound to THIS attempt's label. It tells the
	// target the probe comes from a contact, and it is sealed so no hop on
	// the way learns who is asking.
	//
	// If it cannot be built, there is NO probe. Sending one without the claim
	// would have the target answer on the public path — the gate bypassed by
	// a missing key, or by a key that changed between the probeability check
	// and this line. presenceContactIsProbeable refuses such a contact for
	// the same reason; this is the second half of that rule, at the only
	// place that can still be sure.
	sealed, ok := p.svc.sealLivenessClaim(target, label, now)
	if !ok {
		log.Debug().Str("identity", target.String()).Msg("presence_probe_no_claim")
		return false
	}
	request.Sealed = sealed

	payload, err := protocol.BuildGetIdentityPayload(request)
	if err != nil {
		log.Error().Err(err).Msg("presence_probe_payload_build_failed")
		return false
	}

	// What this payload's `required` actually contains, and why that is safe.
	//
	// BuildGetIdentityPayload adds `target_proof` to `required` whenever the
	// flag is set — so this probe DOES carry a requirement, and an earlier
	// comment here claimed the opposite. The claim was wrong; the behaviour is
	// nevertheless correct, for a reason worth writing down rather than
	// rediscovering:
	//
	//   `target_proof` is not a NEW name. It shipped in the same change as the
	//   `required` mechanism itself, so every build that can answer
	//   `get_identity` at all also understands it — UnderstoodRequirements
	//   accepts exactly this one name. The trap in the design note is about
	//   requiring something a target might not recognise, because an
	//   unrecognised requirement obliges silence, and silence from an old
	//   build is indistinguishable from silence from a dead one.
	//
	// The rule that follows, and that TestProbeRequiresOnlyTargetProof holds:
	// **the probe must never require a second name.** Adding one would make
	// every target that predates it mute, and this prober would read that
	// muteness as three missed probes and call a live contact gone.

	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRequest,
		Class:       domain.DatagramClassControl,
		Src:         label,
		Dst:         target,
		TTL:         domain.DatagramDefaultMaxHops,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DTypeGetIdentity,
		Payload:     payload,
	}

	// Register before the enqueue: on a short path the answer can arrive
	// before SendLocal returns.
	p.mu.Lock()
	p.attempts[label] = presenceProbeAttempt{
		target: target,
		qHash:  sha256.Sum256(payload),
		sentAt: now,
	}
	p.mu.Unlock()

	layer := p.svc.datagramLayer()
	if layer == nil || layer.pipeline == nil {
		p.dropAttempt(label)
		return false
	}

	// The guard set: the same few neighbours carry every probe, for as long as
	// they are up. Each new first hop is an independent coin flip against an
	// adversary holding some share of the network, and the probability of
	// eventually flipping badly goes to one with the NUMBER of flips — so the
	// policy is to flip as rarely as possible, not as often (first_hop_guards.go).
	//
	// route_policy stays `best` for the same reason: `explore` is the opposite
	// policy, deliberately spreading across first hops, and it must not be the
	// default for a stream of frames all asking about the same contact.
	preferred := p.svc.preferredFirstHops()
	outcome := layer.pipeline.SendLocal(ctx, datagram.LocalSendOpts{Frame: frame, FirstHop: preferred})
	if outcome.Kind() != datagram.SendQueued {
		// Nothing reached the network, so nothing was asked. Dropping the
		// attempt rather than letting it time out is the difference between
		// "they did not answer" and "we never managed to ask" — and only the
		// first is evidence about them.
		p.dropAttempt(label)
		log.Debug().
			Str("identity", target.String()).
			Str("outcome", outcome.Kind().String()).
			Msg("presence_probe_not_sent")
		return false
	}

	// Attribute the outcome to the guard set.
	//
	// Only the guards the walk really OFFERED the frame to and that passed it
	// over are penalised, and the list comes from the walk itself: a guard
	// with no route to this destination never entered the candidate list, and
	// blaming it would put a working neighbour into back-off — after which the
	// set tops itself up with somebody new and the policy widens its own
	// exposure out of an accounting error.
	//
	// Nothing is CONFIRMED here. `queued` means the class queue took the
	// frame, not that a neighbour saw it; confirmation waits for an answer to
	// come back through this hop (HandleAnswer).
	nextHop, sent := outcome.NextHop()
	if !sent {
		return true
	}
	p.svc.noteFirstHopsPassedOver(outcome.Attempted(), nextHop)
	p.mu.Lock()
	if attempt, still := p.attempts[label]; still {
		attempt.firstHop = nextHop
		p.attempts[label] = attempt
	}
	p.mu.Unlock()
	return true
}

// claimAttemptAndNoteProof removes an attempt if it is still the one the caller
// is holding, and reports whether the caller is the one that took it. It also
// returns the attempt AS STORED, because the first hop is written into it after
// SendLocal returns and the caller's own copy predates that.
//
// It is the single point where an attempt's outcome is decided. Two paths race
// for it — a verified answer and the timeout sweep — and the loser must not
// also record a result, or one probe produces both a success and a strike.
func (p *presenceProber) claimAttemptAndNoteProof(label domain.PeerIdentity, attempt presenceProbeAttempt) (bool, presenceProbeAttempt) {
	p.mu.Lock()
	defer p.mu.Unlock()

	current, still := p.attempts[label]
	claimed := still && current.sentAt == attempt.sentAt
	if claimed {
		delete(p.attempts, label)
	}
	if !still {
		// Nothing left to read the first hop from; the caller falls back to
		// the copy it took when it recognised the label.
		current = attempt
	}
	// Recorded either way, and under this lock: evidence of life is evidence
	// whether or not this path was the one that owned the attempt, and doing
	// it here is what stops it from interleaving with a timeout's strike.
	if p.svc.presenceProjector != nil && p.svc.contactForPresence(attempt.target) {
		p.svc.presenceProjector.noteProof(attempt.target, p.now(), presenceAliveValidity)
	}
	return claimed, current
}

func (p *presenceProber) dropAttempt(label domain.PeerIdentity) {
	p.mu.Lock()
	delete(p.attempts, label)
	p.mu.Unlock()
}

// HandleAnswer consumes a post_identity that belongs to one of OUR probes.
//
// Returns false for anything it does not recognise, which is how the ingest
// hands the answer on to the identity resolver: the two share a dtype and are
// told apart by whose label it is.
//
// # An invalid answer must not end the attempt
//
// The attempt is closed ONLY by a verified proof. Everything else — an
// unparsable payload, a record that fails verification, a perfectly good record
// with no proof attached — leaves it open until it times out.
//
// That is not tidiness, it closes an attack. The label travels in the clear, so
// any transit on the path knows it, and an identity record is public: a hostile
// hop could answer first with a cached, correctly signed record carrying no
// proof. If that ended the attempt, it would (a) record a strike, (b) delete
// the attempt, so (c) the target's REAL proof arriving a moment later would be
// an unknown label and be handed to the resolver as somebody else's answer.
// Three rounds of that and a live contact is grey. Nothing an attacker can
// produce without the target's key may cost the target anything, so the only
// thing that turns into a strike is silence, measured by the timeout.
func (p *presenceProber) HandleAnswer(label datagram.Label, payload []byte) bool {
	attemptID := label.Raw()

	p.mu.Lock()
	attempt, ours := p.attempts[attemptID]
	p.mu.Unlock()
	if !ours {
		return false
	}

	// The answer is claimed — it is our label, so the resolver must not see
	// it — but the attempt stays open until something proves the target
	// answered. Losing the race to a forged reply must cost nothing.
	layer := p.svc.datagramLayer()
	if layer == nil {
		return true
	}
	network := layer.network

	parsed, err := protocol.ParsePostIdentityPayload(payload)
	if err != nil {
		log.Debug().Err(err).Str("identity", attempt.target.String()).Msg("presence_probe_answer_unparsable")
		return true
	}
	body, err := protocol.VerifyIdentityRecord(parsed.Record, network, attempt.target)
	if err != nil {
		log.Debug().Err(err).Str("identity", attempt.target.String()).Msg("presence_probe_answer_record_invalid")
		return true
	}
	if len(parsed.TargetProof) == 0 {
		// A record without a proof answers "who are you", not "are you
		// there": it is public, replayable by anyone, and says nothing about
		// whether its owner is awake. Not evidence, and not a strike either.
		log.Debug().Str("identity", attempt.target.String()).Msg("presence_probe_answer_proof_missing")
		return true
	}
	if err := protocol.VerifyTargetProof(parsed.TargetProof, body, network, attemptID, attempt.qHash, parsed.Record); err != nil {
		// No ban: the neighbour that handed this over may be an honest
		// transit of somebody else's garbage.
		log.Debug().Err(err).Str("identity", attempt.target.String()).Msg("presence_probe_answer_proof_invalid")
		return true
	}

	// Verified. Claim the attempt and record the proof under the SAME lock the
	// expiry pass uses, so exactly one outcome is ever applied to one probe.
	//
	// A failed claim means the expiry pass already took this attempt and has
	// already recorded its strike — under that same lock, so the two cannot
	// interleave. The proof is still recorded, because it IS evidence of life
	// and clears the count; it simply arrives second.
	claimed, stored := p.claimAttemptAndNoteProof(attemptID, attempt)
	if !claimed {
		log.Debug().Str("identity", attempt.target.String()).Msg("presence_probe_answer_late")
	}
	// A verified answer is the ONLY proof that the first hop really carried
	// our traffic: the frame reached the target and the reply found its way
	// back along the reverse crumbs. Everything earlier — the class queue, its
	// send deadline, the writer — can lose a frame without telling anyone, so
	// `queued` confirms nothing.
	//
	// A zero hop here means the answer overtook SendLocal's return, which on a
	// one-hop path it can. Nothing is recorded then, and nothing needs to be:
	// confirmation is idempotent and the next probe to this contact does it.
	if !stored.firstHop.IsZero() {
		p.svc.noteFirstHopCarried(stored.firstHop)
	}
	p.svc.publishPresenceProof(attempt.target)
	log.Debug().Str("identity", attempt.target.String()).Msg("presence_probe_answered")
	return true
}

// probeReachedTheWire reports whether this probe's bytes left the process
// through ANY of the sockets the send offered it to.
//
// An empty slice means the frame never got as far as the transport at all — the
// class queue refused it, or dropped it on its send deadline before any writer
// saw it. An open channel means that offer reached a writer that did not write
// it: an expired ticket, a dead link, a session queue discarded on close. Only
// a CLOSED channel is a writer saying the bytes are gone, and one such witness
// is enough — the frame is on the wire however many other offers failed.
func probeReachedTheWire(onWire []<-chan struct{}) bool {
	for _, witness := range onWire {
		if witness == nil {
			continue
		}
		select {
		case <-witness:
			return true
		default:
		}
	}
	return false
}

// ownsAttempt reports whether this label belongs to a probe in flight. The
// emitter asks it to decide whether a frame is worth witnessing, which keeps
// the witness off every other frame the node sends.
func (p *presenceProber) ownsAttempt(label domain.PeerIdentity) bool {
	if p == nil || label.IsZero() {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ours := p.attempts[label]
	return ours
}

// noteProbeOnWire attaches one offer's write witness to an attempt, and reports
// whether it was taken.
//
// Called from the emitter, on the pump goroutine, after SendLocal has returned
// — so the attempt may already be gone, either answered or timed out. A missing
// attempt is not an error: there is simply nobody left to tell, and the caller
// skips minting a channel nobody would read.
func (p *presenceProber) noteProbeOnWire(label domain.PeerIdentity, onWire chan struct{}) bool {
	if p == nil || onWire == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	attempt, ours := p.attempts[label]
	if !ours || len(attempt.onWire) >= maxProbeWriteWitnesses {
		return false
	}
	attempt.onWire = append(attempt.onWire, onWire)
	p.attempts[label] = attempt
	return true
}

// maxProbeWriteWitnesses bounds how many offers one probe collects a witness
// for.
//
// It is a MEMORY bound and never a correctness one, and the difference is the
// whole point. The candidate walk is over one peer's connections, and that is
// not eight: a single IP may hold up to maxConnPerIP inbound connections, one
// identity can present several IPs, and an outbound session sits on top. So the
// number here cannot be derived from a limit elsewhere and must not be trusted
// as an upper bound on the walk.
//
// Correctness comes from what happens when it IS reached: the walk stops rather
// than offering the frame unobserved (writeWitness.mint). An unobserved send
// would put bytes on the wire that nothing is watching, and the silence that
// followed could never be attributed — so a contact who had really gone would
// stay `probing` for good. Stopping costs one probe; continuing costs the
// state machine.
//
// Sized with generous headroom over any plausible per-peer connection count so
// the stop is a safety net rather than an everyday event.
const maxProbeWriteWitnesses = 32
