package datagram

import (
	"context"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline_request.go is the request fork of §4.1 — steps 4 to 6.
//
// The fork exists because the common conveyor breaks this plane: a request is
// addressed by `dst` but travels with an answer slot attached, so the reverse
// record has to be taken before the frame is published and rolled back if it
// never leaves.
//
// There is no auth stage here at all, therefore no anti-replay: a replay key
// without a transcript does not exist. What protects the plane instead is the
// reverse state, the probe budget and the per-neighbour limits (§4.1, §4.2).
//
// Reference: docs/protocol/datagram.md §4.1, §4.1.1, §4.2, §4.4.

// handleRequest runs steps 4..6.
func (p *Pipeline) handleRequest(ctx context.Context, arrival inboundFrame) InboundResult {
	header, err := NewDeliveryHeader(arrival.frame)
	if err != nil {
		return droppedWithBan(DropMalformed, err)
	}
	label, ok := header.Label()
	if !ok {
		return dropped(DropMalformed, errors.New("datagram: request without a label"))
	}

	// Step 4. dst == self is terminal handling, bypassing everything routing:
	// no reverse state is created, because there is nowhere to return an
	// answer to except this very neighbour.
	if p.isSelf(arrival.frame.Dst) {
		return p.answerLocally(ctx, arrival, header, label)
	}

	// Step 5. A READ-ONLY look at the slot: an occupied label is a drop with no
	// mutation. The record is not overwritten, because a repeated — possibly
	// looped — request would re-point downstream and the answer to the first
	// forward would lose its way home. No ban: a loop can be honest, and the
	// initiator's retry arrives with a FRESH label and takes its own slot.
	if _, taken := p.reverse.Lookup(label); taken {
		return dropped(DropReverseSlotBusy, nil)
	}

	// THE TRANSIT INTERCEPTOR STEPS ARE GONE — they stood here, and the document
	// no longer numbers them, so the forward below is step 6: a transit node no
	// longer looks inside the request
	// and no longer answers it. Answering from a relay's own cache put a reply
	// on the wire in the destination's name, which neither endpoint can tell
	// from the real one; dropping it there ended somebody else's exchange with
	// no trace on either side. Both are the endpoints' business.

	// Step 6.
	return p.forwardRequest(ctx, arrival, header, label)
}

// answerLocally is step 4: the registry gate, the authorization hook, the
// handler, and an answer ONLY on `accepted`.
//
// `rejected` and `failed` are a silent drop of the request WITHOUT an answer:
// answering on a refusal would disguise the refusal as success, and a
// "negative answer", where a type needs one, is application content inside an
// accepted answer rather than a transport branch.
func (p *Pipeline) answerLocally(
	ctx context.Context,
	arrival inboundFrame,
	header DeliveryHeader,
	label Label,
) InboundResult {
	result, reason := p.deliverUnsigned(ctx, arrival, header)
	if reason != DropReasonUnset {
		return dropped(reason, nil)
	}
	switch result.Outcome() {
	case HandlerAccepted:
		answer, produced := result.Response()
		if !produced {
			return handled(InboundDelivered)
		}
		return p.emitAnswer(ctx, arrival, label, answer)
	case HandlerRejected:
		return dropped(DropHandlerRejected, result.Err())
	default:
		return dropped(DropHandlerFailed, result.Err())
	}
}

// forwardRequest is step 6: the transit gate, deliverability, and only then
// the two-phase reservation of §4.2.
//
// The order inside is normative: reserve the slot, fix the downstream, clamp
// the ttl, decrement, publish. The clamp stands HERE, in the conveyor, and is
// not implied by §4.1.1: this plane has no auth.max_ttl to check a budget
// against, and without an explicit clamp an unsigned request with `ttl = 255`
// would travel twenty-five times further than the reverse state is sized for.
func (p *Pipeline) forwardRequest(
	ctx context.Context,
	arrival inboundFrame,
	header DeliveryHeader,
	label Label,
) InboundResult {
	if !p.transitAllowed() {
		return dropped(DropTransitGate, nil)
	}
	job := sendJob{
		frame:    arrival.frame,
		incoming: arrival.ingress(),
		avoid:    NoAvoidedNextHop(),
	}
	selection := p.selectFor(ctx, job)
	if !selection.publishable() {
		// A transit frame with nowhere to go is a silent drop: the layer is
		// unguaranteed and recovery belongs to the originator.
		return dropped(DropNoCandidates, nil)
	}

	destination, _ := header.Destination()
	reservation := p.reverse.Reserve(ReverseReserveOpts{
		ReceivedAt: arrival.receivedAt,
		Label:      label,
		Dst:        destination,
		DType:      arrival.frame.DType,
		// The three facts of the arrival, kept apart because they answer three
		// different questions (§4.2). The CHANNEL is where the answer goes back;
		// the BUDGET KEY owns the slot, so the per-upstream quota survives the
		// neighbour reconnecting on a fresh channel; the presented NAME is for
		// the log and nothing else.
		//
		// Answering any two of them with one value was the defect, twice over: the
		// name addressed answers to whatever node the asker NAMED, and the channel
		// — right for the return path — handed out a brand new quota per reconnect
		// and split one flooder into many quiet-looking upstreams at eviction time.
		Upstream: ChannelUpstream(arrival.channel, arrival.budgetKey, arrival.peer),
	})
	slot, reserved := reservation.Slot()
	if !reserved {
		return reverseReserveRefusal(reservation)
	}

	outgoing, forwarded := requestForwardFrame(arrival.frame)
	if !forwarded {
		p.reverse.Rollback(slot)
		return dropped(DropTTLExhausted, nil)
	}

	publisher := p.requestPublisher(slot, outgoing, sendDeadlineFor(arrival.receivedAt))
	if placed := p.scheduler.dispatch(ctx, job, selection, publisher); placed.Kind() == SendQueued {
		return handled(InboundForwarded)
	}
	// Candidates exhausted: the slot is released ENTIRELY.
	p.reverse.Rollback(slot)
	return dropped(DropForwardFailed, nil)
}

// requestForwardFrame applies the clamp and the single decrement of §4.1.1 to
// a forwarded request. The bool is false when there is no budget left, which
// the common part has already excluded — it is checked anyway so no caller can
// underflow a uint8 into a full new life.
func requestForwardFrame(frame protocol.DatagramFrame) (protocol.DatagramFrame, bool) {
	ttl, ok := ClampAndDecrement(frame.TTL)
	if !ok {
		return protocol.DatagramFrame{}, false
	}
	outgoing := frame.Clone()
	outgoing.TTL = ttl
	return outgoing, true
}

// reverseReserveRefusal maps a refused reverse reservation.
func reverseReserveRefusal(result ReverseReserveResult) InboundResult {
	if result.Outcome() == ReverseSlotBusy {
		return dropped(DropReverseSlotBusy, nil)
	}
	return dropped(DropReverseSlotCapped, nil)
}

// emitAnswer builds and sends a response frame.
//
// Its shape is fixed by §2.1.1 and §4.4: mode = response, the request's label
// echoed in dst, `src` = the address the question was ADDRESSED TO, because src
// here is a logical subject and not the sender — no auth, no route_policy, and
// ttl = defaultMaxHops whoever produced it.
//
// The send deadline is computed LOCALLY as arrival + queue_residence(control):
// there is no wire field for it, and the node forming an answer knows nobody
// else's expires_at.
func (p *Pipeline) emitAnswer(
	ctx context.Context,
	arrival inboundFrame,
	label Label,
	answer HandlerResponse,
) InboundResult {
	// THE PAIRING IS CHECKED HERE TOO, not only where an answer arrives.
	//
	// §4.2 pairs an answer with its request by type: the answer's dtype must
	// declare `answers_to` the request's. The receiving side already refuses a
	// pair that does not hold — otherwise a formally valid answer of another
	// protocol would take somebody else's single claimed slot — and this node,
	// which PRODUCED the answer, was not asking the same question. A handler
	// answering with a mispaired dtype therefore killed the exchange in both
	// directions, permanently and repeatably for that (type, node) pair, while
	// this node logged it as answered.
	//
	// The predicate is the same one pairingHolds uses, and the "unknown type,
	// no check" rule is the same: a node cannot be asked to know pairs it never
	// registered. Here that case is a handler answering with a dtype this node
	// does not implement, which the receiver will judge for itself.
	if !p.answerPairs(answer.DType(), arrival.frame.DType) {
		return dropped(DropReversePairing, nil)
	}
	response := protocol.DatagramFrame{
		Version: domain.DatagramHeaderVersion,
		Mode:    domain.DatagramModeResponse,
		Class:   domain.DatagramClassControl,
		Src:     arrival.frame.Dst,
		Dst:     label.Raw(),
		TTL:     ResponseTTL(),
		DType:   answer.DType(),
		Payload: answer.Payload(),
	}
	if err := response.Validate(); err != nil {
		return dropped(DropMalformed, err)
	}
	// THE ANSWER IS PINNED TO THE CHANNEL THE QUESTION ARRIVED ON.
	//
	// "The neighbour the request came from" used to be spelled as that
	// neighbour's identity, and the transport resolved it through the session
	// map — so on a session this node dialled, where the identity is a name the
	// asker chose, the answer left over a session belonging to whoever was
	// named. A channel is this node's own socket and cannot be borrowed, and an
	// emitter that can no longer reach it must refuse the frame rather than fall
	// back to the name (OutboundFrame.Channel).
	if !p.emit(ctx, channelEgress(arrival.channel, arrival.peer), response, sendDeadlineFor(arrival.receivedAt)) {
		return dropped(DropAnswerNotDelivered, nil)
	}
	return handled(InboundAnswered)
}

// answerPairs is pairingHolds asked at the ANSWERING node, where the request
// dtype comes from the frame being answered rather than from a reverse record.
func (p *Pipeline) answerPairs(answer, request domain.DType) bool {
	entry, known := p.types.Lookup(answer)
	if !known {
		return true
	}
	return entry.AnswersRequest(request)
}

// sendLocalRequest is the origin half of §4.2: a request created here takes
// its own reverse slot with the LOCAL upstream marker — a marker, never this
// node's address, so the transit path and the local path do not mix in any
// comparison.
//
// The origin does NOT decrement the ttl (§4.1.1 rule 4): the frame leaves for
// the first hop with the full budget, which is what makes a maximum-length
// round trip arrive with ttl = 1 rather than 0.
func (p *Pipeline) sendLocalRequest(ctx context.Context, opts LocalSendOpts) SendOutcome {
	frame := opts.Frame
	header, err := NewDeliveryHeader(frame)
	if err != nil {
		return failedOutcome(true, err)
	}
	label, ok := header.Label()
	if !ok {
		return failedOutcome(true, errors.New("datagram: local request without a label"))
	}
	// avoid_next_hop reaches the SELECTION, not a filter behind it: §4.3
	// applies the exclusion before direct-first, so the excluded peer is
	// invisible to the direct branch and to the explore modulus alike.
	job := sendJob{
		frame:    frame,
		incoming: LocalIngress(),
		avoid:    opts.Avoid,
		firstHop: opts.FirstHop,
	}
	selection := p.selectFor(ctx, job)
	if !selection.publishable() {
		return selection.outcomeWithoutCandidates(true)
	}

	now := p.clock()
	destination, _ := header.Destination()
	reservation := p.reverse.Reserve(ReverseReserveOpts{
		ReceivedAt: now,
		Label:      label,
		Dst:        destination,
		DType:      frame.DType,
		Upstream:   LocalUpstream(),
	})
	slot, reserved := reservation.Slot()
	if !reserved {
		return localRefusal()
	}

	placed := p.scheduler.dispatch(ctx, job, selection, p.requestPublisher(slot, frame, sendDeadlineFor(now)))
	if placed.Kind() != SendQueued {
		p.reverse.Rollback(slot)
	}
	return placed
}

// requestPublisher is the request plane's half of the candidate walk: phase 3
// of §4.2 (the downstream is written into the taken slot BEFORE the frame is
// published, so an answer cannot physically outrun the record) followed by the
// enqueue. Phase 4 is the failure branch — the frame never left towards this
// candidate, so the next one simply rewrites downstream.
//
// # Why the request is PINNED to the candidate's channel
//
// The record has to hold the channel the request left over, or an answer can
// only be judged by the name the answering neighbour presents — which on a
// dialled session that neighbour chose (Downstream states the attack). The
// channel therefore has to be known at phase 3, and there are exactly two ways
// to know it: DECIDE it here, or LEARN it from the hand-over. The second is not
// available, for two independent reasons:
//
//   - phase 3 is BEFORE the publication by definition. A channel learned from
//     the emitter's return arrives after the frame is already in a write queue,
//     which is the very race the two-phase record exists to close;
//   - with the class queue of §5 in front of the writer — the production wiring
//     — the socket is chosen on the far side of a lane the frame may sit in for
//     seconds, so the enqueue call has no channel to report at all.
//
// So the layer decides, and the candidate is what it decides from. That is not
// the name check coming back in a new spelling: a candidate describes ONE
// CONCRETE CONNECTION and is gated as such (§4.3 line 574), the channel is this
// node's own socket rather than anything the neighbour said about itself, and
// the pin is binding on the emitter — a frame carrying a channel goes over that
// channel or is refused (OutboundFrame.Channel), never over a second socket of
// whatever peer answered to the name.
//
// A candidate with no channel is refused instead. It means the resolver names no
// connections, and the alternatives are a record with no return path or a record
// keyed on a name; the walk simply moves on, and a walk that finds nothing ends
// as `failed`, which is the honest answer for a plane whose reverse state cannot
// be built.
func (p *Pipeline) requestPublisher(
	slot ReverseSlot,
	frame protocol.DatagramFrame,
	deadline time.Time,
) hopPublisher {
	return func(ctx context.Context, candidate RouteCandidate) hopSendOutcome {
		hop := candidate.NextHop()
		channel, addressable := candidate.Channel()
		if !addressable {
			return hopFailedOutcome(errCandidateWithoutChannel)
		}
		if !p.reverse.FixDownstream(slot, ChannelDownstream(channel, hop)) {
			return hopFailedOutcome(errReverseSlotLost)
		}
		if !p.emit(ctx, channelEgress(channel, hop), frame, deadline) {
			return hopFailedOutcome(errEnqueueRefused)
		}
		return hopEnqueuedOutcome()
	}
}

// errReverseSlotLost names a reverse record that vanished under an in-flight
// publication — swept, rolled back or replaced. It is a local failure and not
// a policy refusal: nothing about the route or the gates changed.
var errReverseSlotLost = errors.New("datagram: the reverse slot is no longer owned by this send")

// errEnqueueRefused names a write queue that did not CONFIRM the frame. §4.3
// keeps it apart from `rejected` because repeating IS worth it, once the queue
// drains — and not because the bytes are known to have stayed home: a queue
// answers about admission, so a refusal read after the frame is already in it
// can follow a completed write on a link that died afterwards. What makes the
// repeat safe is the receiver's anti-replay cache, which drops the duplicate.
var errEnqueueRefused = errors.New("datagram: the next hop's write queue refused the frame")

// errCandidateWithoutChannel names a candidate the request plane cannot use: the
// resolver ranked a connection but did not say WHICH connection it is, so there
// is no channel to pin the request to and none to store as the record's return
// path. It is a local failure and a wiring fault of the resolver, never a policy
// refusal — the same shape as errInboundNoChannel on the receive side.
var errCandidateWithoutChannel = errors.New(
	"datagram: the ranked connection names no transport channel, so a request cannot be pinned to it")
