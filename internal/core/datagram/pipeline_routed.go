package datagram

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline_routed.go is the routed fork of §4.1 — steps 5 to 11 — plus the
// forwarding cycle shared with a locally created frame.
//
// The routed plane is the only one with anti-replay and the only one with a
// signature. It NEVER touches reverse state, and the unsigned planes never
// touch its cache (§4.1).
//
// Reference: docs/protocol/datagram.md §4.1, §4.1.1, §4.4.

// handleRouted runs steps 5..11 in the fixed order.
func (p *Pipeline) handleRouted(ctx context.Context, arrival inboundFrame) InboundResult {
	frame := arrival.frame

	// Step 5. The header view, then the transcript and the replay key —
	// hashing, cheap next to a signature verification.
	header, err := NewHeader(frame)
	if err != nil {
		return droppedWithBan(DropMalformed, err)
	}
	transcript, err := protocol.BuildDatagramTranscript(frame, p.network)
	if err != nil {
		return dropped(DropTranscript, err)
	}
	key := protocol.DatagramReplayKey(transcript)

	// Step 6. Anti-replay: presence only, no insertion. Inserting before
	// authenticity is proven would let an attacker poison the cache with a key
	// copied out of a legitimate frame.
	//
	// The probe answers hit or miss and nothing else — the cache is in memory
	// and has no third answer to give (replay_cache.go) — so a duplicate is A CHEAP
	// SILENT DROP and everything else goes on.
	if p.replay.Has(ctx, key).Outcome() == HasHit {
		return dropped(DropReplayDuplicate, nil)
	}

	// Step 7. Deliverability.
	if !p.deliverable(ctx, arrival) {
		return dropped(DropUndeliverable, nil)
	}

	// Where the frame ends is settled HERE, before anything is paid for, and
	// not at the fork below: one of step 8's timing verdicts depends on it.
	fate := p.routedFate(frame.Dst)

	// Step 8. Cheap checks, then the crypto token, then the signature.
	deadlines, refusal, refused := p.verifyRouted(arrival, header, fate)
	if refused {
		return refusal
	}

	// Step 9. Clamp the ttl — after the sender's budget has been checked, so
	// the clamp cannot hide an inflated value from that check.
	clamped := frame.Clone()
	clamped.TTL = ClampTTL(frame.TTL)

	deliveryHeader, err := NewDeliveryHeader(clamped)
	if err != nil {
		return droppedWithBan(DropMalformed, err)
	}

	// THE TRANSIT INTERCEPTOR STEP IS GONE — it stood here, between the clamp
	// and the fork, and its absence is the design rather than a shortcut. The
	// document no longer numbers it, so the fork below is step 10 and step 11.
	//
	// It ran a type's interceptor over a frame in TRANSIT, and the verdicts it
	// could return were `drop` and `answer` — a relay ending somebody else's
	// frame, or replying in the destination's name out of its own cache. Both
	// make the relay a participant in a protocol it is not part of, with no way
	// for either endpoint to tell that it happened.

	delivery := routedDelivery{
		arrival:   arrival,
		frame:     clamped,
		delivery:  deliveryHeader,
		deadlines: deadlines,
		key:       key,
	}
	if fate == routedFateLocal {
		// Step 10.
		return p.deliverRoutedLocally(ctx, delivery)
	}
	// Step 11.
	return p.transitRouted(ctx, delivery)
}

// routedFate is where a routed frame ends. Two steps read it — the timing rule
// of step 8 and the fork between steps 10 and 11 — and they must not be able to
// disagree, which is why it is decided once and carried rather than asked
// twice.
type routedFate uint8

const (
	// routedFateLocal is dst == self: the frame is consumed by a handler on
	// this node and crosses no socket again.
	routedFateLocal routedFate = iota
	// routedFateTransit is somebody else's frame, which this node still has
	// to queue and write.
	routedFateTransit
)

// carriesOn reports whether a socket write is still ahead of the frame. That is
// the only question the send window has any bearing on.
func (f routedFate) carriesOn() bool { return f == routedFateTransit }

// routedFate answers "is this ours" once per frame.
func (p *Pipeline) routedFate(dst domain.PeerIdentity) routedFate {
	if p.isSelf(dst) {
		return routedFateLocal
	}
	return routedFateTransit
}

// routedDelivery carries everything steps 10 and 11 need, so neither of them
// recomputes a transcript or a deadline.
type routedDelivery struct {
	arrival   inboundFrame
	frame     protocol.DatagramFrame
	delivery  DeliveryHeader
	deadlines Deadlines
	key       domain.ReplayKey
}

// verifyRouted is step 8 in one place.
//
// The order is the anti-DoS design: `ttl <= max_ttl` on the raw value and the
// validity interval cost nanoseconds and reject the whole frame, so paying for
// them with a signature would be backwards. Only then is a fixed-price crypto
// token charged — immediately before ed25519.Verify, so anything sieved out by
// the early Has or by the cheap gates never spends one.
//
// The fate is an argument for exactly one reason: one of the timing verdicts is
// a refusal of the SEND path only, and a refusal has to be decided here or it
// is decided at the writer queue, after the whole price of the frame has been
// paid.
func (p *Pipeline) verifyRouted(
	arrival inboundFrame,
	header Header,
	fate routedFate,
) (Deadlines, InboundResult, bool) {
	frame := arrival.frame
	if !TTLWithinBudget(frame.TTL, frame.Auth.MaxTTL) {
		// A stable-header violation every datagram transit is obliged to
		// check, so it is one of the few ban-worthy refusals (§4.4).
		return Deadlines{}, droppedWithBan(DropTTLBudget, fmt.Errorf(
			"datagram: ttl %d exceeds signed max_ttl %d", frame.TTL, frame.Auth.MaxTTL)), true
	}
	decision := ComputeDeadlines(header, arrival.receivedAt)
	if reason, refused := timingRefusal(decision.Outcome(), fate); refused {
		return Deadlines{}, dropped(reason, nil), true
	}
	deadlines, ok := decision.Deadlines()
	if !ok {
		return Deadlines{}, dropped(DropStale, nil), true
	}
	if !protocol.DatagramSignerMatchesSrc(frame) {
		return Deadlines{}, droppedWithBan(DropFingerprint, nil), true
	}
	// The BUDGET KEY, never arrival.peer. The two are the same neighbour on an
	// accepted connection and a different thing entirely on an outbound session,
	// where peer is the fingerprint the sender wrote into its welcome — so
	// charging peer let a neighbour burn the verification tokens of any node it
	// cared to name, and get a fresh burst by reconnecting under a new one.
	if !p.chargeVerify(arrival.budgetKey) {
		return Deadlines{}, dropped(DropCryptoBudget, nil), true
	}
	if err := protocol.VerifyDatagramSignature(frame, p.network); err != nil {
		return Deadlines{}, droppedWithBan(DropSignature, err), true
	}
	return deadlines, InboundResult{}, false
}

// unconditionalTimingRefusals are the verdicts that end a frame whatever this
// node was going to do with it: it is outside the interval this node admits
// frames by, so there is nothing to deliver and nothing to carry.
var unconditionalTimingRefusals = map[DeadlineOutcome]DropReason{
	DeadlinesNotYetValid: DropNotYetValid,
	DeadlinesStale:       DropStale,
}

// timingRefusal turns a timing verdict into the reason the frame is dropped
// with, or reports that the frame lives on.
//
// DeadlinesExpired is the one verdict that is NOT in the map, because it is not
// a fact about the frame alone: §2.2 makes it a refusal of the send path — the
// frame is alive, only the room reserved for writing it is gone — so it ends a
// frame this node still has to write and says nothing about one addressed here.
// Answering it uniformly in either direction is a bug either way round: refuse
// everything and a live datagram is dropped at its destination, refuse nothing
// and the frame is verified, reserved and walked before the writer queue turns
// it away as backpressure.
func timingRefusal(outcome DeadlineOutcome, fate routedFate) (DropReason, bool) {
	if reason, refused := unconditionalTimingRefusals[outcome]; refused {
		return reason, true
	}
	if outcome == DeadlinesExpired && fate.carriesOn() {
		return DropSendWindowExpired, true
	}
	return DropReasonUnset, false
}

// ---------------------------------------------------------------------------
// Step 11 — transit
// ---------------------------------------------------------------------------

// transitRouted is step 11: the transit gate, then the candidates, then
// Reserve, then the candidate walk.
//
// A transited frame occupies ONE piece of state on this node, its replay key.
// Nothing here can write the frame anywhere: the layer keeps no copy of somebody
// else's datagram, and it runs no code of the protocol that frame belongs to —
// this node accepts it and passes it on, exactly as it arrived.
func (p *Pipeline) transitRouted(ctx context.Context, delivery routedDelivery) InboundResult {
	if !p.transitAllowed() {
		// Before the reservation and before any state: an endpoint-only node
		// spends nothing on somebody else's frame even when it has a route.
		return dropped(DropTransitGate, nil)
	}

	// Rule 4 of §4.1.1: exactly ONE decrement, and only when forwarding
	// somebody else's frame. It happens here and not inside forwardRouted,
	// which is shared with the locally created path — the origin does not
	// decrement, and the shared code must not be able to.
	forwarded, alive := forwardedFrame(delivery.frame)
	if !alive {
		return dropped(DropTTLExhausted, nil)
	}

	outcome := p.forwardRouted(ctx, forwardPlan{
		frame:     forwarded,
		key:       delivery.key,
		incoming:  delivery.arrival.ingress(),
		deadlines: delivery.deadlines,
	})
	return transitResult(outcome)
}

// forwardedFrame applies the single per-hop decrement to a transited routed
// frame. The ttl was clamped at step 9, so the result never exceeds
// defaultMaxHops − 1.
func forwardedFrame(frame protocol.DatagramFrame) (protocol.DatagramFrame, bool) {
	ttl, alive := DecrementTTL(frame.TTL)
	if !alive {
		return protocol.DatagramFrame{}, false
	}
	outgoing := frame.Clone()
	outgoing.TTL = ttl
	return outgoing, true
}

// transitResult maps a scheduling outcome onto the inbound vocabulary. A
// transit frame with nowhere to go is a SILENT drop — the layer is
// unguaranteed and recovery belongs to the originator — while the identical
// situation on a locally created frame is a synchronous refusal to its caller.
func transitResult(outcome SendOutcome) InboundResult {
	switch outcome.Kind() {
	case SendQueued:
		return handled(InboundForwarded)
	case SendNoRoute:
		return dropped(DropNoCandidates, nil)
	case SendRejected:
		return dropped(DropForwardFailed, outcome.Err())
	default:
		return dropped(DropForwardFailed, outcome.Err())
	}
}

// reserveRefusal maps the two negative reservation outcomes. They are kept
// apart because they say different things to an operator: a duplicate is the
// cache doing its job on a racing second instance of one frame, while a
// rejection is the cache out of room for this neighbour (§5).
func reserveRefusal(result ReserveResult) InboundResult {
	if result.Outcome() == ReserveDuplicate {
		return dropped(DropReserveDuplicate, nil)
	}
	return dropped(DropReserveRejected, result.Err())
}

// ---------------------------------------------------------------------------
// The forwarding cycle, shared by transit and by locally created frames
// ---------------------------------------------------------------------------

// forwardPlan is one frame on its way out.
type forwardPlan struct {
	deadlines Deadlines
	frame     protocol.DatagramFrame
	incoming  IngressPeer
	key       domain.ReplayKey
	avoid     AvoidedNextHop
}

// forwardRouted places a routed frame, running the cycle of §4.1 step 11 in the
// normative order:
//
//	candidates → Reserve → publish → Commit | Release
//
// Everything before Reserve is read-only, and that is the whole reason the
// order is this way round: an empty candidate list must not occupy a
// reservation slot even briefly. It is also why NONE of those branches calls
// Release — the key is one per frame, and a release there would strip a
// reservation a concurrent instance of the same frame is holding at that very
// moment.
//
// The cycle is identical for a transited frame and for one created here.
func (p *Pipeline) forwardRouted(ctx context.Context, plan forwardPlan) SendOutcome {
	local := plan.incoming.IsLocal()

	// One selection, one outcome vocabulary. The scheduler answers with the
	// FULL §4.3 verdict — including `rejected(unsupported_dtype)` from the
	// last-hop gate and `rejected(missing_capability)` from the role gate —
	// and this path passes it straight through instead of flattening every
	// negative answer into `no_route`, which would tell a sender to wait for a
	// route when the destination has told us it cannot handle the type at all.
	job := sendJob{frame: plan.frame, incoming: plan.incoming, avoid: plan.avoid}
	selection := p.selectFor(ctx, job)
	if !selection.publishable() {
		return selection.outcomeWithoutCandidates(local)
	}

	reserve := p.replay.Reserve(ctx, plan.key, plan.incoming, plan.deadlines.ReplayUntil())
	rsv, reserved := reserve.Reservation()
	if !reserved {
		return reserveRefusalOutcome(local, reserve)
	}

	placed := p.scheduler.dispatch(ctx, job, selection, p.forwardPublisher(plan))
	if placed.Kind() != SendQueued {
		// Every candidate refused AFTER the reservation: this is one of the
		// Release branches of §4.1. The outcome itself is NOT rewritten —
		// §4.3 wants `failed` here (a fallible branch before the enqueue) or
		// the hop's own `rejected`, and never a `no_route` that would send the
		// caller waiting for a route it already has.
		p.release(ctx, rsv)
		return placed
	}
	if !p.replay.Commit(ctx, rsv).IsApplied() {
		// Commit.fail on the transit branch: Release and a log. The frame is
		// already queued and the outcome is FINAL — a repeat of the frame will
		// pass and at worst yield one extra duplicate, which the neighbours'
		// anti-replay puts out.
		p.release(ctx, rsv)
		log.Warn().
			Str("replay_key", shortKey(plan.key)).
			Msg("datagram: replay commit failed after the frame was queued")
	}
	return placed
}

// forwardPublisher is the routed plane's half of the candidate walk: hand the
// frame to one neighbour and report whether its queue took it.
func (p *Pipeline) forwardPublisher(plan forwardPlan) hopPublisher {
	return func(ctx context.Context, candidate RouteCandidate) hopSendOutcome {
		// nextHopEgress and NOT the candidate's channel: on this plane the
		// scheduler picks a NEIGHBOUR and the transport picks the socket among
		// that peer's gated connections (§4.3 item 3). There is no reverse record
		// here to name a return path, so pinning would buy nothing and would cost
		// the gated fall-back the emitter's walk exists for.
		if !p.emit(ctx, nextHopEgress(candidate.NextHop()), plan.frame, plan.deadlines.SendUntil()) {
			return hopFailedOutcome(errEnqueueRefused)
		}
		return hopEnqueuedOutcome()
	}
}

// reserveRefusalOutcome maps a refused reservation onto a send outcome. Both
// refusals of an in-memory cache are `rejected`: the key is already taken, or
// there is no room for it, and in either case nothing was written and repeating
// the SAME frame would be refused for the same reason.
//
// `failed` would be the wrong answer and not merely a coarser one: §4.3 reads it
// as a fallible step of this node BEFORE the enqueue, worth repeating with a
// backoff, and it does not license the transport fallback that `rejected` and
// `no_route` do (see SendFailed).
func reserveRefusalOutcome(local bool, result ReserveResult) SendOutcome {
	return SendOutcome{kind: SendRejected, local: local, err: result.Err()}
}

// release is the ONE place the layer gives a reservation back, so the branch
// list of §4.1 can be read off the call sites.
//
// The outcome is deliberately not read: BaseReplayCache.Release either drops the
// record or reports a stale token as ok — that is the ABA guard, not a failure —
// so there is no "the key stayed occupied" case left to handle. The warning that
// used to stand here spoke for a durable store whose write could refuse, and
// this seam can no longer be handed one.
func (p *Pipeline) release(ctx context.Context, rsv ReservationToken) {
	p.replay.Release(ctx, rsv)
}

// sendDeadlineFor is the writer deadline of an UNSIGNED frame: the moment of
// arrival plus queue_residence(control) (§4.2). It is computed locally
// because there is no wire field for it and there cannot be one.
func sendDeadlineFor(receivedAt time.Time) time.Time { return ResponseSendDeadline(receivedAt) }
