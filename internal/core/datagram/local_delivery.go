package datagram

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// local_delivery.go implements the outcome table of
// docs/protocol/datagram.md §4.1 step 10.
//
// One ordering carries the whole design and it is not negotiable: the COMMIT
// happens AFTER the handler, never before. Commit first, crash next, and the
// frame stays undelivered forever. In the stated order the crash leaves an
// uncommitted reservation, which dies with the process — so the sender's repeat
// is delivered again, and the handler is idempotent by contract
// (docs/protocol/datagram.md §4.5): a duplicate beats a loss.
//
// Reference: docs/protocol/datagram.md §4.1, §4.5, §7.

// deliverRoutedLocally is step 10: the registry gate, the authorization hook,
// the reservation, the handler, and then the fate of the replay key according
// to the outcome table.
func (p *Pipeline) deliverRoutedLocally(ctx context.Context, delivery routedDelivery) InboundResult {
	admitted, reason, err := p.admitLocalDelivery(ctx, localDeliveryGate{
		header:   delivery.delivery,
		mode:     delivery.frame.Mode,
		class:    delivery.frame.Class,
		dtype:    delivery.frame.DType,
		payload:  delivery.frame.Payload,
		incoming: delivery.arrival.ingress(),
	})
	if reason != DropReasonUnset {
		// A silent drop WITHOUT committing the replay key, so an authentic but
		// untrusted sender cannot evict other people's records from the
		// bounded LRU (docs/protocol/datagram.md §7).
		return dropped(reason, err)
	}
	entry, deliveryContext := admitted.entry, admitted.delivery

	reserve := p.replay.Reserve(ctx, delivery.key, delivery.arrival.ingress(), delivery.deadlines.ReplayUntil())
	rsv, reserved := reserve.Reservation()
	if !reserved {
		return reserveRefusal(reserve)
	}

	return p.runLocalDelivery(ctx, localDelivery{
		entry:    entry,
		delivery: deliveryContext,
		payload:  delivery.frame.Payload,
		rsv:      rsv,
		key:      delivery.key,
	})
}

// localDelivery is one local delivery in flight.
type localDelivery struct {
	entry    RegisteredType
	delivery DeliveryContext
	payload  []byte
	key      domain.ReplayKey
	rsv      ReservationToken
}

// runLocalDelivery applies the table of docs/protocol/datagram.md §4.1 step 10.
//
//	| handler outcome | the fate of the replay key |
//	| accepted        | Commit(rsv)                |
//	| rejected        | Commit(rsv)                |
//	| failed or panic | Release(rsv)               |
//
// `accepted` and `rejected` share a row because the KEY is all the cache holds:
// what the node did with the frame is counted (docs/rpc/datagram.md, `metrics`)
// and logged, and no reader of the cache is entitled to it. A duplicate of
// either is dropped identically.
//
// The mutations address the RESERVATION and never the replay key: the key is one
// per frame, so a release naming it would cancel whatever reservation of that key
// exists at that instant — which, for a late release, is the one a parallel
// instance of the same frame is holding right now (ReservationToken,
// docs/protocol/datagram.md §4.1).
//
// The three outcomes differ ONLY in the fate of the replay key. `rejected` is
// a deliberate PERMANENT refusal, so its key is committed and a repeat is
// dropped by the early Has without a second verification and without a second
// handler call — otherwise a frame known to be refused could be run through
// Verify and the handler for free until its validity ran out. `failed` is a
// fault after which a repeat makes sense, so the key is released.
func (p *Pipeline) runLocalDelivery(ctx context.Context, run localDelivery) InboundResult {
	result := p.runHandler(ctx, run.entry, run.delivery, run.payload)
	switch result.Outcome() {
	case HandlerAccepted:
		return p.completeAccepted(ctx, run, result)
	case HandlerRejected:
		return p.completeRejected(ctx, run, result)
	default:
		return p.completeFailed(ctx, run, result)
	}
}

// completeAccepted is the `accepted` row.
func (p *Pipeline) completeAccepted(ctx context.Context, run localDelivery, result HandlerResult) InboundResult {
	delivered := p.commitDelivered(ctx, run)
	if response, produced := result.Response(); produced {
		// A routed type has no reverse exchange to answer into; only the
		// request plane does, and it never reaches this function.
		log.Debug().
			Str("dtype", response.DType().String()).
			Msg("datagram: routed handler produced an answer, which the routed plane has no path for")
	}
	return delivered
}

// commitDelivered is the layer's half of the `accepted` row: the commit of the
// replay key after the handler accepted, with Release when the commit fails.
func (p *Pipeline) commitDelivered(ctx context.Context, run localDelivery) InboundResult {
	if !p.replay.Commit(ctx, run.rsv).IsApplied() {
		// Commit.fail: Release. The delivery has already been accepted, so the
		// sender's repeat will arrive again and reach the handler a second time
		// — which is lawful, because docs/protocol/datagram.md §4.5 makes the
		// handler idempotent.
		p.release(ctx, run.rsv)
		log.Warn().
			Str("replay_key", shortKey(run.key)).
			Msg("datagram: replay commit failed after the handler accepted")
	}
	return handled(InboundDelivered)
}

// completeRejected is the `rejected` row. It is NOT a rollback: the outcome is
// terminal and negative, and its key is committed.
//
// A failed commit ends with Release, which docs/protocol/datagram.md §4.1
// states directly: the repeat reaches the handler again and is refused again —
// an extra Verify and an extra call, but no loss and no permanently occupied
// slot.
func (p *Pipeline) completeRejected(ctx context.Context, run localDelivery, result HandlerResult) InboundResult {
	if !p.replay.Commit(ctx, run.rsv).IsApplied() {
		log.Warn().
			Str("replay_key", shortKey(run.key)).
			Msg("datagram: replay commit failed after the handler refused, the repeat will reach the handler again")
		p.release(ctx, run.rsv)
	}
	return dropped(DropHandlerRejected, result.Err())
}

// completeFailed is the `failed` row: the key is released so the sender's
// repeat is worth making.
func (p *Pipeline) completeFailed(ctx context.Context, run localDelivery, result HandlerResult) InboundResult {
	p.release(ctx, run.rsv)
	return dropped(DropHandlerFailed, result.Err())
}

// runHandler calls the type handler and turns a panic into `failed`.
//
// A panic is the `failed` row and not a separate one
// (docs/protocol/datagram.md §4.1): the key is released and the sender's repeat
// is worth making. `rejected` would be the wrong conversion for the same reason
// it is the wrong answer generally — it is a deliberate PERMANENT refusal whose
// key is committed, and a crash says nothing about whether a repeat would
// succeed.
//
// Recovering here rather than letting it unwind also keeps one broken type from
// killing the reader goroutine that serves every other type on the same
// session. It is the boundary of hook_guard.go, reached through the same helper
// as every other seam so the crash report has the same shape.
//
// It is also the ONE place a handler call is counted (handler_stats.go): every
// plane calls a handler through here and only after every gate, so one record
// per call covers routed, request and response without a second counting site.
// It takes the registry ENTRY rather than the bare Handler for that reason —
// the entry carries the type's counters.
func (p *Pipeline) runHandler(
	ctx context.Context,
	entry RegisteredType,
	delivery DeliveryContext,
	payload []byte,
) HandlerResult {
	site := hookSite{hook: "Handle", dtype: delivery.Header().DType()}
	// The PRESENTED name: a crash report has to say which neighbour it happened
	// on, proven or not, and the level travels with the value it came from.
	site.peer, _ = delivery.IncomingPeer().PresentedIdentity()

	// returned is set only by a Handle that came back: the guard's fallback is
	// an ordinary FailDelivery, so the result alone cannot tell a panic from an
	// honest failure.
	returned := false
	var wall time.Duration
	result := guardHook(site, FailDelivery(errHookPanicked), func() HandlerResult {
		// The clock stops in a defer of THIS frame, which a panic unwinds
		// before guardHook's recovery writes the crash report: the report and
		// its stack trace are the guard's cost, not the handler's.
		started := p.monotonic()
		defer func() { wall = p.monotonic().Sub(started) }()
		answer := entry.Handler().Handle(ctx, delivery, payload)
		returned = true
		return answer
	})
	entry.stats.record(handlerCall{
		mode:     delivery.Header().Mode(),
		outcome:  result.Outcome(),
		wall:     wall,
		panicked: !returned,
	})
	return result
}

// ---------------------------------------------------------------------------
// The unsigned planes
// ---------------------------------------------------------------------------

// deliverUnsigned is local delivery on the request and response planes.
//
// There is no anti-replay here and nothing to commit, so the three handler
// outcomes differ only in the metric and the log — with ONE exception that
// matters: an answer is admissible only together with `accepted`
// (docs/protocol/datagram.md §4.1), and that rule lives in the caller, which is
// the only place that knows whether an answer has anywhere to go.
//
// The authorization hook is the last gate before the handler, exactly as in
// the routed plane; the difference is only its neighbourhood to a commit that
// does not exist here.
func (p *Pipeline) deliverUnsigned(
	ctx context.Context,
	arrival inboundFrame,
	header DeliveryHeader,
) (HandlerResult, DropReason) {
	admitted, reason, _ := p.admitLocalDelivery(ctx, gateOfArrival(arrival, header))
	if reason != DropReasonUnset {
		return HandlerResult{}, reason
	}
	return p.runHandler(ctx, admitted.entry, admitted.delivery, arrival.frame.Payload), DropReasonUnset
}

// ---------------------------------------------------------------------------
// The gates every local delivery passes, in every mode
// ---------------------------------------------------------------------------

// localDeliveryGate is the input of the shared gate sequence. It is a struct
// rather than five arguments because the routed plane runs the gates over the
// CLAMPED frame while the unsigned planes run them over the arrival, and a
// positional signature would let those two drift silently.
type localDeliveryGate struct {
	header   DeliveryHeader
	mode     domain.DatagramMode
	class    domain.DatagramClass
	dtype    domain.DType
	payload  []byte
	incoming IngressPeer
}

// gateOfArrival builds the gate input of an unsigned plane, where the frame
// reaching the handler is the frame that arrived.
func gateOfArrival(arrival inboundFrame, header DeliveryHeader) localDeliveryGate {
	return localDeliveryGate{
		header:   header,
		mode:     arrival.frame.Mode,
		class:    arrival.frame.Class,
		dtype:    arrival.frame.DType,
		payload:  arrival.frame.Payload,
		incoming: arrival.ingress(),
	}
}

// admittedDelivery is a delivery that passed every gate: the registry entry
// whose handler may now run, and the context it runs with.
type admittedDelivery struct {
	entry    RegisteredType
	delivery DeliveryContext
}

// admitLocalDelivery runs the fixed sequence every local delivery passes, in
// ALL THREE modes:
//
//	types.Lookup → observeUnknownDType → admitRegisteredFrame →
//	NewDeliveryContext → authorizeLocalDelivery
//
// It exists as one function because the sequence is a SECURITY gate, and it
// was written out three times — routed, request/response local delivery and
// the local response resolver. Three copies of a gate is three chances for one
// plane to lose a check quietly, which is exactly the failure mode
// docs/protocol/datagram.md §7 cannot tolerate: the authorization hook is what
// stops an authentic but untrusted sender from reaching a handler.
//
// An unknown dtype never reaches the authorization hook and occupies no replay
// slot: it is refused at the registry step, silently, with a metric, a live
// connection and no ban (docs/protocol/datagram.md §7).
//
// The DropReason is DropReasonUnset exactly when the delivery was admitted.
func (p *Pipeline) admitLocalDelivery(
	ctx context.Context,
	gate localDeliveryGate,
) (admittedDelivery, DropReason, error) {
	entry, known := p.types.Lookup(gate.dtype)
	if !known {
		p.observeUnknownDType(gate.dtype)
		return admittedDelivery{}, DropUnknownDType, nil
	}
	if reason := admitRegisteredFrame(entry, gate.mode, gate.class); reason != DropReasonUnset {
		return admittedDelivery{}, reason, nil
	}
	deliveryContext, err := NewDeliveryContext(DeliveryContextOpts{
		Header:        gate.header,
		IncomingPeer:  gate.incoming,
		LocalIdentity: p.localID,
	})
	if err != nil {
		return admittedDelivery{}, DropMalformed, err
	}
	if decision := authorizeLocalDelivery(ctx, entry, deliveryContext, gate.payload); !decision.Accepted() {
		return admittedDelivery{}, DropUnauthorized, decision.Err()
	}
	return admittedDelivery{entry: entry, delivery: deliveryContext}, DropReasonUnset, nil
}
