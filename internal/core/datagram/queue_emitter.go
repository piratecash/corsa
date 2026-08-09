package datagram

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
)

// queue_emitter.go puts the weighted class queue of §5 ON THE REAL PATH.
//
// The queue itself decides an ORDER; until this file existed nothing produced
// into it and nothing consumed from it outside its own unit tests, so §5's
// promise — "bulk gets a guaranteed share under a constant control stream" —
// held in the type and nowhere else: the pipeline published straight to
// FrameEmitter, first come first served.
//
// The boundary is unchanged and deliberate: a frame leaves this queue when the
// layer is ready to hand it to the writer, and everything after that — the
// per-peer write queue, the deadline re-check and the write grace — belongs to
// netcore, and none of it reports back: the ticket travels one way
// (netcore/write_ticket.go). Per-PEER fairness is therefore still
// not this queue's job; the only dimension §5 names here is between CLASSES.
//
// Reference: docs/refactoring/datagram-transport.md §4.2, §5, §9.

// ClassQueueEmitter is the FrameEmitter the pipeline talks to when the layer
// is wired with a class queue: Enqueue on the way in, the writer's emitter on
// the way out, and the deficit round robin of §5 in between.
//
// Its EmitTo answers the same question the direct emitter did — "did the queue
// take it" — so the finality rule of §4.3 is untouched: a `queued` outcome
// still means queued, and the frame may still be dropped later on send_until,
// here or in the writer.
type ClassQueueEmitter struct {
	queue   *WeightedQueue
	out     FrameEmitter
	metrics dropSink
}

// dropSink counts a frame lost after the queue released it. It is the narrow
// half of the metrics surface this file needs, so the emitter cannot reach for
// an inbound counter it has no business touching.
type dropSink interface {
	ObserveDrop(reason DropReason)
}

// ClassQueueEmitterConfig wires the emitter. An opts struct because the type
// needs three collaborators and a forgotten one must be a constructor error,
// not a nil dereference on the pump goroutine (CLAUDE.md).
type ClassQueueEmitterConfig struct {
	// Queue is the deficit round robin of §5.
	Queue *WeightedQueue
	// Out is the writer's emitter.
	Out FrameEmitter
	// Metrics counts frames the writer refused after the dequeue. Optional:
	// a nil sink simply counts nothing.
	Metrics dropSink
}

// NewClassQueueEmitter wires the queue in front of the writer's emitter.
func NewClassQueueEmitter(cfg ClassQueueEmitterConfig) (*ClassQueueEmitter, error) {
	if isNilValue(cfg.Queue) {
		return nil, errors.New("datagram: the class queue emitter requires a queue")
	}
	if isNilValue(cfg.Out) {
		return nil, errors.New("datagram: the class queue emitter requires a downstream emitter")
	}
	return &ClassQueueEmitter{queue: cfg.Queue, out: cfg.Out, metrics: normaliseOptional(cfg.Metrics)}, nil
}

// EmitTo places the frame in its class lane.
//
// The size is taken from the line the layer already serialized rather than
// measured again: §5 accounts in serialized bytes, and a second serialization
// of the same frame is the duplicated work the OutboundFrame contract exists
// to remove.
func (e *ClassQueueEmitter) EmitTo(_ context.Context, out OutboundFrame) bool {
	return e.queue.Enqueue(QueuedFrame{
		SendUntil: out.SendUntil,
		Peer:      out.Peer,
		Frame:     out.Frame,
		Bytes:     out.Bytes(),
		Line:      out.Line,
		Channel:   out.Channel,
		Class:     out.Class,
	})
}

// Drain hands every frame the queue is ready to release to the writer and
// reports how many went. It is the pump body, exposed on its own so a test —
// and a caller that owns its own scheduling — can advance the queue by hand
// instead of racing a goroutine.
//
// A frame the writer's emitter refuses is NOT put back: the queue's contract
// is that already-queued frames are never re-ordered or resurrected, and a
// refusal downstream is a lost frame on an unguaranteed layer, not a reason to
// let one class jump ahead of another on the next round. A writer that CRASHES
// takes the same path — see handOver.
func (e *ClassQueueEmitter) Drain(ctx context.Context) int {
	sent := 0
	for {
		item, ok := e.queue.Dequeue()
		if !ok {
			return sent
		}
		outcome := e.handOver(ctx, item)
		if !outcome.taken {
			// A crash is its OWN reason, not a shade of refusal: a refusal is
			// ordinary backpressure — the session died between the enqueue and
			// the dequeue — while a crash is a defect in the adapter that an
			// operator has to go and fix. Counted, not just logged, because §10
			// requires a drop to be observable by reason and a log line is not
			// a counter.
			if e.metrics != nil {
				e.metrics.ObserveDrop(outcome.reason())
			}
			log.Debug().
				Str("peer", item.Peer.String()).
				Str("class", item.Class.String()).
				Bool("crashed", outcome.crashed).
				Msg("datagram: the writer did not take a dequeued frame")
			continue
		}
		sent++
	}
}

// handOver gives ONE dequeued frame to the writer behind the panic boundary of
// hook_guard.go, which lists the node's frame emitter among the foreign seams
// it guards.
//
// This is the writer's SECOND call site: Pipeline.emit guards the direct
// hand-over, and until this existed a layer wired with a class queue reached
// the very same adapter from the pump goroutine with nothing in between — so a
// crashing writer met crashlog.DeferRecover, which logs and RE-PANICS by
// design, and took the process down together with a frame the queue had already
// released.
//
// "Did not take it" is the writer's own documented failure value, so the crash
// lands on the refusal path the drain already has instead of inventing a second
// one. The Error line guardHook writes carries the stack and the identity of
// the frame, while Drain's own Debug line states what the drain concluded about
// it: the two answer different questions and neither repeats the other.
//
// The failure value carries `crashed` as well, because the two are different
// numbers to whoever reads them: an ordinary refusal is backpressure and rises
// with load, while a crash is a defect in the adapter. Reading "it crashed" out
// of the same `false` the honest refusal returns is exactly the implicit signal
// CLAUDE.md forbids.
func (e *ClassQueueEmitter) handOver(ctx context.Context, item QueuedFrame) handOverOutcome {
	site := hookSite{
		hook:  "EmitTo",
		peer:  item.Peer,
		dtype: item.Frame.DType,
	}
	// The hand-over contract is rebuilt OUTSIDE the boundary: what belongs
	// behind it is the foreign call and nothing else, and a panic in the
	// layer's own code converted into the writer's failure would hide a bug of
	// this package behind a degraded mode (hook_guard.go).
	outbound := outboundOf(item)
	return guardHook(site, handOverOutcome{crashed: true}, func() handOverOutcome {
		return handOverOutcome{taken: e.out.EmitTo(ctx, outbound)}
	})
}

// handOverOutcome is what became of one hand-over: whether the writer took the
// frame, and whether it was still alive when it answered.
type handOverOutcome struct {
	taken   bool
	crashed bool
}

// reason names the drop of a hand-over that failed. It is only meaningful when
// the frame was not taken; a taken frame is not a drop at all.
func (o handOverOutcome) reason() DropReason {
	if o.crashed {
		return DropWriterPanicked
	}
	return DropWriterRefused
}

// Run pumps the queue until the context is cancelled. It reacts to the
// coalescing Ready signal, which is why one wake-up per burst is enough: Drain
// empties the queue before waiting again.
func (e *ClassQueueEmitter) Run(ctx context.Context) {
	for {
		e.Drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.queue.Ready():
		}
	}
}

// Queue exposes the queue for its stats and for the owner's expiry sweep.
func (e *ClassQueueEmitter) Queue() *WeightedQueue { return e.queue }

// outboundOf rebuilds the hand-over contract from a dequeued frame. The line
// travels through the queue, so nothing is serialized twice on this path
// either.
func outboundOf(item QueuedFrame) OutboundFrame {
	return OutboundFrame{
		SendUntil: item.SendUntil,
		Peer:      item.Peer,
		Frame:     item.Frame,
		Line:      item.Line,
		Channel:   item.Channel,
		Class:     item.Class,
	}
}
