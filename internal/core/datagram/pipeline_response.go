package datagram

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
)

// pipeline_response.go is the response fork of §4.1 — steps 4 to 6.
//
// A response is NOT routed by dst: it travels along stored state alone, which
// is why "check deliverability before cryptography" would break this plane
// outright — there is no route to its dst and there must not be one.
// Deliverability here means a live record with a live upstream.
//
// The whole fork is built around one scarcity: a record has EXACTLY ONE answer
// slot. Everything below follows from that — the read-only checks before the
// claim, the probe budget reserved before the expensive check, and the local
// gates and the forwarding ttl before the claim.
//
// The fork also draws the line the whole plane rests on: the SHARED part reads
// nothing but the arriving bytes and this node's own reverse state, while
// anything that consults the type registry — an opinion about somebody's
// application protocol — lives on the LOCAL branch alone. A relay that judged a
// foreign exchange by its own registrations would have to be upgraded before any
// new endpoint protocol could move through it.
//
// The rule that generates all of them: the CAS is the LAST refusable step. A
// check that reads only the arriving frame and the stored record belongs before
// it, because a drop after it holds the slot until expires_at and loses the
// genuine answer. What legitimately stays after the CAS is only the two
// mutating steps themselves — the enqueue to the upstream and the resolver —
// and it is their FAILURE, on either branch, that §4.2 keeps the record claimed
// for. Their success frees it on either branch too.
//
// Reference: docs/protocol/datagram.md §4.1, §4.1.1, §4.2, §4.4.

// handleResponse runs steps 4..6.
func (p *Pipeline) handleResponse(ctx context.Context, arrival inboundFrame) InboundResult {
	header, err := NewDeliveryHeader(arrival.frame)
	if err != nil {
		return droppedWithBan(DropMalformed, err)
	}
	label, ok := header.Label()
	if !ok {
		return dropped(DropMalformed, errors.New("datagram: response without an echoed label"))
	}

	// Step 4. The record and the TRANSPORT invariants — everything a node can
	// judge without knowing a single application protocol.
	record, live := p.reverse.Lookup(label)
	if !live {
		return dropped(DropReverseUnknownLabel, nil)
	}
	if reason := checkResponseTransport(arrival, header, record); reason != DropReasonUnset {
		return dropped(reason, nil)
	}

	// Step 5. The probe budget is reserved ATOMICALLY: without atomicity several
	// forged answers would each see a free budget and all of them would reach
	// the expensive check, so the limit would protect exactly the case that was
	// already safe. Exhaustion does not free the slot — the record stays pending
	// until expires_at.
	//
	// The budget is charged to the RECORD validated just above, not to the
	// label: the label is chosen by whoever sent the request, so the entry may
	// have been replaced by a fresh exchange since the Lookup, and charging
	// that one would let answers to a finished exchange drain the budget of a
	// live one.
	ticket, refusal := p.reserveProbe(record)
	if refusal != DropReasonUnset {
		return dropped(refusal, nil)
	}

	// Step 6. The fork and the addressability of the upstream are ONE decision:
	// Upstream.Channel() answers false for exactly the local marker and for a
	// record whose channel is gone, so the network branch receives a CHANNEL it
	// cannot fail to address. Asking again after the claim would be a drop that
	// leaves the record claimed for nothing.
	upstream := record.Upstream()
	channel, network := upstream.Channel()
	if !network {
		return p.deliverResponseLocally(ctx, arrival, header, record, ticket)
	}
	name, _ := upstream.Peer()
	return p.forwardResponse(ctx, arrival, record, channelEgress(channel, name), ticket)
}

// reserveProbe is step 5 as the fork performs it: the reservation and the
// reason a refusal is reported with, in ONE function.
//
// They are together because apart they are untestable as a pair: a mapping
// tested on its own stays green while the call site drops with a constant, and
// the interleaving the stale verdict answers to cannot be staged from outside
// the fork. Here a test can hand this function a record whose exchange has
// ended and read the reason the frame would actually carry.
//
// DropReasonUnset means granted, and the ticket is held.
func (p *Pipeline) reserveProbe(record ReverseRecord) (ProbeTicket, DropReason) {
	ticket, outcome := p.reverse.ReserveProbe(record)
	if outcome != ReverseProbeGranted {
		return ProbeTicket{}, probeRefusalReason(outcome)
	}
	return ticket, DropReasonUnset
}

// probeRefusalReasons names every refusal of the probe reservation. The two
// refusals stay APART because they say opposite things about the record an
// operator would go and look at: "the budget ran out" is a live exchange that
// will pay for no more expensive validation, while "the record is stale" is an
// answer to an exchange that has already ended and that spent nothing. Folding
// the second into the first would show a live record exhausting a budget nobody
// took from it — the very number the probe limit exists to make readable.
var probeRefusalReasons = map[ReverseProbeOutcome]DropReason{
	ReverseProbeExhausted: DropReverseProbeExhausted,
	ReverseProbeStale:     DropReverseRecordStale,
}

// probeRefusalReason maps a refusal to the reason the frame is dropped with.
//
// An outcome nobody mapped is reported as stale rather than as an exhausted
// budget: stale is the refusal that accuses no record of having spent anything,
// so a verdict added later without a reason costs a wrong label and never a
// wrong number under reverse_probe_exhausted.
func probeRefusalReason(outcome ReverseProbeOutcome) DropReason {
	if reason, named := probeRefusalReasons[outcome]; named {
		return reason
	}
	return DropReverseRecordStale
}

// checkResponseTransport is every read-only invariant of §4.2 that BOTH
// upstream kinds share, in one place so no branch can quietly skip one.
//
// What lives here is decided by ONE question: could a node answer it while
// knowing nothing about the application protocol the frame belongs to? The
// return path, the echoed subject and the state of the slot are all facts about
// THIS node's own reverse state and the bytes that arrived, so a transit node
// judges them exactly as an endpoint does. The type-level pairing of §4.2 is not
// one of them and is not here — it belongs to the endpoint alone
// (deliverResponseLocally), because it is an opinion about somebody's protocol
// and a relay holding an older opinion would drop the correct answer of a newer
// one.
//
// It is a FUNCTION and not a method for the same reason: a check with no
// Pipeline to reach through has no registry, no clock and no store to consult,
// so "transport-only" is a property the signature carries rather than a promise
// the comment makes.
//
// The `ttl > 0` rule is not repeated here: the common part of §4.1 already
// dropped a zero ttl on the RAW value, and a second check would be a second
// opinion about the same field. The stricter forwarding rule — `ttl ≥ 2`,
// because the decrement pays for the hop about to be made (§4.1.1 rule 4) — is
// not here either, and deliberately so: it holds for the network branch alone,
// while local delivery has no hop to pay for and answers a `ttl = 1` frame
// perfectly well. Moving it here would drop legitimate answers at the
// initiator.
func checkResponseTransport(
	arrival inboundFrame,
	header DeliveryHeader,
	record ReverseRecord,
) DropReason {
	downstream, forwarded := record.Downstream()
	if !forwarded || downstream.Channel() != arrival.channel {
		// The answer must come back over the CHANNEL the request left on.
		//
		// It is a CHANNEL comparison and no longer a name one, which is what
		// makes both ends of the record defensible. arrival.peer is what the
		// answering neighbour PRESENTS, and on a session this node dialled that
		// name is the neighbour's own choice — so comparing names admitted any
		// session willing to write the expected fingerprint into its welcome to
		// the single unsigned answer slot of somebody else's exchange. The
		// channel is this node's own socket: the question's return path, which
		// nobody can present themselves as.
		//
		// `forwarded` is false only before the first candidate is fixed. That
		// window is not a race with an in-flight answer — FixDownstream runs
		// BEFORE the frame reaches the writer (§4.2 phase 3) — it is a record
		// whose walk found no publishable candidate at all.
		return DropReverseWrongDownstream
	}
	if subject, ok := header.Subject(); !ok || subject != record.Dst() {
		// `response.src` must equal the stored `request.dst`: a consistency
		// check, not an authenticity one — nothing on this plane is signed.
		return DropReverseSubjectMismatch
	}
	if record.State() != ReverseSlotPending {
		return DropReverseNotPending
	}
	return DropReasonUnset
}

// pairingHolds is the type-level pairing check of §4.2, performed at the
// ENDPOINT — the node whose own exchange the record is — and BEFORE the claim.
//
// It runs on the local branch ONLY, and that is the whole rule rather than an
// optimisation. The pairing is an opinion about an application protocol, read
// out of THIS node's registry: the node that asked the question is the one
// entitled to hold it, because the slot the rule protects is its own and the
// request dtype in the record is a type it registered itself. A transit node
// holding the same opinion is judging a protocol it is not part of — and a relay
// whose registration of a response dtype is a version behind would then drop the
// correct answer of a newer endpoint protocol, making every new protocol wait
// for the whole path to upgrade.
//
// A node that does NOT know the answer type performs no check and delivers as
// before: demanding knowledge of future pairs is impossible, which is why the
// check is typed rather than transport-level. A node that DOES know it refuses
// an answer whose type never declared this request dtype — otherwise a formally
// valid answer of another protocol would take the single claimed slot of this
// node's exchange, and would reach that other type's authorization hook on the
// way.
func (p *Pipeline) pairingHolds(answer domain.DType, record ReverseRecord) bool {
	entry, known := p.types.Lookup(answer)
	if !known {
		return true
	}
	return entry.AnswersRequest(record.DType())
}

// forwardResponse is the network-upstream branch, and its order mirrors the
// local one: EVERY refusal first, CAS second, enqueue last.
//
// The clamp and the decrement are a refusal like the local gates are, so they
// stand before the claim for the same reason: the record has one answer slot,
// and an answer that will not be forwarded anyway must not eat it while the
// real one is refused as "already claimed" (§4.2). They are pure
// functions of the ARRIVING frame — nothing about them needs the claimed
// state — which is what makes hoisting them possible at all.
//
// The enqueue below is the opposite case and the only drop left after the CAS:
// it is the mutating step itself, so §4.2 keeps the record claimed until
// expires_at on purpose.
func (p *Pipeline) forwardResponse(
	ctx context.Context,
	arrival inboundFrame,
	record ReverseRecord,
	upstream egress,
	ticket ProbeTicket,
) InboundResult {
	// The clamp is explicit here for the same reason as on the request plane:
	// there is no auth.max_ttl to check against, and a ttl inflated by a
	// hostile transit is cut by this clamp and nothing else.
	ttl, alive := ClampAndDecrement(arrival.frame.TTL)
	if !alive {
		return dropped(DropTTLExhausted, nil)
	}
	outgoing := arrival.frame.Clone()
	outgoing.TTL = ttl

	slot, claimed := p.reverse.Claim(record)
	if !claimed {
		return dropped(DropReverseClaimLost, nil)
	}
	// A successful `forward` followed by a claim does NOT spend budget: only
	// refused attempts do (§4.2).
	p.reverse.RefundProbe(ticket)

	// The hand-over is CHANNEL-PINNED, for the reason emitAnswer states: an
	// answer belongs to the exchange's own return path, and resolving that path
	// through the identity map hands it to whoever the asker named.
	if !p.emit(ctx, upstream, outgoing, sendDeadlineFor(arrival.receivedAt)) {
		// The record stays CLAIMED until expires_at: the answer is lost, the
		// initiator retries with a fresh label, and no second chance is given —
		// or repeats could hammer the upstream for free.
		return dropped(DropAnswerNotDelivered, nil)
	}
	// The slot is freed ONLY after a successful enqueue.
	p.reverse.Complete(slot)
	return handled(InboundForwarded)
}

// deliverResponseLocally is the `upstream = local` branch: the answer is ours,
// and the order is LOCAL GATES FIRST, CAS SECOND.
//
// That order is the whole point: the record has one answer slot, so taking it
// before the gates would let an answer of an unknown or forbidden type eat the
// single attempt while the real one is refused as "already claimed". A gate
// refusal leaves the record pending.
//
// The PAIRING is the first of those gates and it exists only on this branch —
// this is the node that asked, so this is the node whose registry may speak
// about the answer (pairingHolds). It stands before the registry/authorization
// sequence because a mispaired answer must not reach the hook of the type it
// mispaired itself as.
//
// After the CAS the resolver is the mutating step, exactly as the enqueue is on
// the network branch, and the record's fate follows the SAME rule there: it is
// freed when the answer reached its upstream and held until expires_at when it
// did not. `accepted` is what "reached" means on this side — the resolver is
// the local upstream — while `rejected` and `failed` are the twin of an enqueue
// the writer would not take.
func (p *Pipeline) deliverResponseLocally(
	ctx context.Context,
	arrival inboundFrame,
	header DeliveryHeader,
	record ReverseRecord,
	ticket ProbeTicket,
) InboundResult {
	if !p.pairingHolds(arrival.frame.DType, record) {
		return dropped(DropReversePairing, nil)
	}
	admitted, reason, err := p.admitLocalDelivery(ctx, gateOfArrival(arrival, header))
	if reason != DropReasonUnset {
		return dropped(reason, err)
	}

	slot, claimed := p.reverse.Claim(record)
	if !claimed {
		return dropped(DropReverseClaimLost, nil)
	}
	// A successful `forward` followed by a claim does NOT spend budget: only
	// refused attempts do (§4.2).
	p.reverse.RefundProbe(ticket)

	// No decrement and no enqueue: there is no hop to pay for (§4.1.1 rule 4).
	switch result := runHandler(ctx, admitted.entry.Handler(), admitted.delivery, arrival.frame.Payload); result.Outcome() {
	case HandlerAccepted:
		// The exchange is over: this node asked, this node was answered, and a
		// record kept past that point holds a slot of the LocalUpstream bucket
		// for the whole reverse window — which caps how many requests this node
		// may have outstanding, however promptly they were answered.
		p.reverse.Complete(slot)
		return handled(InboundDelivered)
	case HandlerRejected:
		// The record stays CLAIMED until expires_at, for the reason a failed
		// enqueue does: the answer never reached the asker, and a second chance
		// would let repeats run the resolver for free.
		return dropped(DropHandlerRejected, result.Err())
	default:
		return dropped(DropHandlerFailed, result.Err())
	}
}
