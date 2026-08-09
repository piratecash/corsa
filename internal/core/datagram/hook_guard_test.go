package datagram

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// hook_guard_test.go pins the boundary of hook_guard.go hook by hook.
//
// Every case here used to be a process kill: the layer calls foreign code from
// two shared goroutines — the session reader and the outbound pump — and the
// only thing between a panicking hook and exit(2) was crashlog.DeferRecover,
// which logs and RE-PANICS by design.
//
// Two assertions per hook, and the second one is the reason a naive recover()
// is worse than none: the converted value must be the hook's documented
// FAILURE, and the resources the caller was holding at panic time must end up
// exactly where that failure leaves them.

var errBoom = errors.New("datagram: test hook explosion")

// panickingHandler crashes instead of answering, so the layer's own conversion
// is what the caller sees.
func panickingHandler() *recordingHandler {
	return &recordingHandler{
		result: func(DeliveryContext, []byte) HandlerResult { panic(errBoom) },
	}
}

// TestPanickingTypeHooksFailClosedOnTheirOwnPlane covers the two hooks a TYPE
// supplies. They differ in direction, and each direction is the hook's own
// documented one: a handler that crashed is `failed` (release the key, the
// repeat is worth making), and an authorizer that crashed is a REJECT (accepted
// by omission is never inferred).
func TestPanickingTypeHooksFailClosedOnTheirOwnPlane(t *testing.T) {
	t.Run("handler", func(t *testing.T) {
		net := newFakeNetwork()
		node := newPipelineNode(t, net, nodeOpts{name: "handler-panic"})
		sender := newPipelineNode(t, net, nodeOpts{name: "sender"})
		link(sender, node, false, false)
		registration := requestType(dtypeQuery, panickingHandler())
		registerType(t, node, registration)

		result := node.deliver(t, sender.id, requestFrame(t, requestOpts{
			label: newLabel(t, "handler"), dst: node.id,
		}))
		requireDrop(t, result, DropHandlerFailed)
	})

	t.Run("authorizer", func(t *testing.T) {
		net := newFakeNetwork()
		node := newPipelineNode(t, net, nodeOpts{name: "auth-panic"})
		sender := newPipelineNode(t, net, nodeOpts{name: "sender"})
		link(sender, node, false, false)
		registration := requestType(dtypeQuery, acceptingHandler())
		registration.Authorizer = AuthorizerFunc(
			func(context.Context, DeliveryContext, []byte) AuthorizationDecision {
				panic(errBoom)
			})
		registerType(t, node, registration)

		result := node.deliver(t, sender.id, requestFrame(t, requestOpts{
			label: newLabel(t, "auth"), dst: node.id,
		}))
		requireDrop(t, result, DropUnauthorized)
	})
}

// TestPanickingNodeAdaptersDegradeInsteadOfCrashing covers the four seams the
// node implements. Each conversion is the answer the seam already defines for
// "nothing here", so the frame ends on a path the layer walks every day.
func TestPanickingNodeAdaptersDegradeInsteadOfCrashing(t *testing.T) {
	t.Run("route resolver", func(t *testing.T) {
		selection := selectWithPanickingSeam(t, viaRoute, func(node *pipelineNode) {
			node.pipeline.scheduler.routes = panickingRoutes{}
		})
		if selection.publishable() {
			t.Fatal("a crashed resolver must leave no candidates")
		}
	})

	t.Run("peer metadata", func(t *testing.T) {
		selection := selectWithPanickingSeam(t, viaRoute, func(node *pipelineNode) {
			node.pipeline.scheduler.selector.peers = panickingPeers{}
		})
		if selection.publishable() {
			t.Fatal("a crashed peer lookup must leave no candidates")
		}
	})

	t.Run("direct session", func(t *testing.T) {
		// The destination IS the neighbour and there is no route to it, so the
		// direct branch is the only source of a candidate: a crash there leaves
		// the frame with none, exactly as an absent session would.
		selection := selectWithPanickingSeam(t, toNeighbour, func(node *pipelineNode) {
			node.pipeline.scheduler.direct = panickingDirect{}
		})
		if selection.publishable() {
			t.Fatal("a crashed direct lookup must leave no candidates")
		}
	})

	t.Run("a crashed direct lookup still leaves the routing table", func(t *testing.T) {
		// The other direction of the same conversion, and the one that says the
		// degradation is bounded: with a route present the frame travels on.
		selection := selectWithPanickingSeam(t, viaRoute, func(node *pipelineNode) {
			node.pipeline.scheduler.direct = panickingDirect{}
		})
		if !selection.publishable() {
			t.Fatal("a crashed direct lookup must only demote the frame to the routing table")
		}
	})

	t.Run("frame emitter", func(t *testing.T) {
		net := newFakeNetwork()
		node := newPipelineNode(t, net, nodeOpts{name: "emitter-panic"})
		node.pipeline.emitter = panickingEmitter{}
		if node.pipeline.emit(
			context.Background(),
			nextHopEgress(domaintest.ID("peer")),
			requestFrame(t, requestOpts{label: newLabel(t, "emit"), dst: domaintest.ID("dst")}),
			node.clock().Add(time.Second),
		) {
			t.Fatal("a crashed writer must report the frame as not taken")
		}
	})
}

// seamDestination says which of the two candidate sources the selection under
// test is allowed to use.
type seamDestination uint8

const (
	// viaRoute addresses a destination reachable only through the routing
	// table, one hop behind the neighbour.
	viaRoute seamDestination = iota
	// toNeighbour addresses the neighbour itself, with no route to it, so the
	// direct branch is the only source of a candidate.
	toNeighbour
)

// selectWithPanickingSeam runs one candidate selection over a node whose given
// seam crashes, and hands back what the scheduler concluded.
func selectWithPanickingSeam(
	t *testing.T,
	destination seamDestination,
	break_ func(*pipelineNode),
) candidateSelection {
	t.Helper()

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "seam"})
	peer := newPipelineNode(t, net, nodeOpts{name: "peer"})
	link(node, peer, true, true)

	dst := peer.id
	if destination == viaRoute {
		dst = domaintest.ID("far")
		route(node, dst, peer.id, 2)
	}
	break_(node)

	return node.pipeline.selectFor(context.Background(), sendJob{
		frame:    requestFrame(t, requestOpts{label: newLabel(t, "seam"), dst: dst}),
		incoming: LocalIngress(),
		avoid:    NoAvoidedNextHop(),
	})
}

type panickingRoutes struct{}

func (panickingRoutes) FreshRoutes(context.Context, domain.PeerIdentity) []RouteHint {
	panic(errBoom)
}

func (panickingRoutes) CachedRoutes(context.Context, domain.PeerIdentity) []RouteHint {
	panic(errBoom)
}

type panickingPeers struct{}

func (panickingPeers) SendableConnection(
	context.Context, domain.PeerIdentity, protocol.DatagramFrame,
) (PeerConnection, bool) {
	panic(errBoom)
}

type panickingDirect struct{}

func (panickingDirect) LookupDirectSession(
	context.Context, domain.PeerIdentity, protocol.DatagramFrame,
) (PeerConnection, bool) {
	panic(errBoom)
}

type panickingEmitter struct{}

func (panickingEmitter) EmitTo(context.Context, OutboundFrame) bool { panic(errBoom) }

// ---------------------------------------------------------------------------
// The SECOND call site of the writer: the class queue
// ---------------------------------------------------------------------------

// crashingThenRecordingWriter crashes on the FIRST frame handed to it and
// serves every later one. Both halves are the point: a fixture that only ever
// crashed would keep a "boundary" that answers false to everything green, so
// the healthy frame is the control that says the drain still delivers.
type crashingThenRecordingWriter struct {
	healthy recordingWriter
	crashed bool
}

func (w *crashingThenRecordingWriter) EmitTo(ctx context.Context, out OutboundFrame) bool {
	if !w.crashed {
		w.crashed = true
		// The same crash the direct hand-over is asserted against, so one
		// fixture covers both call sites of one seam.
		return panickingEmitter{}.EmitTo(ctx, out)
	}
	return w.healthy.EmitTo(ctx, out)
}

// TestACrashedWriterOnTheQueuePathIsARefusalNotAProcessExit is the boundary of
// hook_guard.go on the writer's OTHER call site.
//
// Pipeline.emit guards the direct hand-over, but a layer wired with a
// class queue reaches the same foreign writer from ClassQueueEmitter.Drain, and
// that call was bare: the panic left the pump goroutine, met
// crashlog.DeferRecover — which logs and RE-PANICS by design — and killed the
// process together with a frame the queue had already released and never puts
// back.
//
// The conversion is the writer's own documented failure value, `false`, so a
// crash lands on the refusal path the drain already walks: one
// DropWriterRefused, and on to the next frame.
func TestACrashedWriterOnTheQueuePathIsARefusalNotAProcessExit(t *testing.T) {
	ctx := context.Background()
	writer := &crashingThenRecordingWriter{}
	metrics := NewMetrics()
	queue := NewWeightedQueue(WeightedQueueConfig{Clock: time.Now})
	emitter, err := NewClassQueueEmitter(ClassQueueEmitterConfig{Queue: queue, Out: writer, Metrics: metrics})
	if err != nil {
		t.Fatalf("NewClassQueueEmitter: %v", err)
	}

	// Both frames take the control lane, and a lane is FIFO: the crash meets
	// the first one, the second waits behind it.
	doomed, survivor := domaintest.ID("crashing-hop"), domaintest.ID("healthy-hop")
	for _, peer := range []domain.PeerIdentity{doomed, survivor} {
		if !emitter.EmitTo(ctx, OutboundFrame{
			Peer:  peer,
			Line:  []byte("{}\n"),
			Class: domain.DatagramClassControl,
		}) {
			t.Fatalf("the queue refused the frame for %s", peer)
		}
	}

	// Before the boundary existed this call did not return.
	sent := emitter.Drain(ctx)

	if !writer.crashed {
		t.Fatal("the writer never crashed: the case under test did not run")
	}
	if sent != 1 {
		t.Fatalf("Drain reported %d frames sent, want 1: a crash costs its own frame and no other", sent)
	}
	if got := metrics.DropCount(DropWriterPanicked); got != 1 {
		t.Fatalf("writer_panicked drops = %d, want 1: a writer that crashed did not take the frame", got)
	}
	// A crash is not filed as backpressure: writer_refused rises with load and
	// is read as normal, so a defect hidden inside it is a defect nobody looks
	// for. The mirror case — an honest refusal counted as writer_refused and
	// NOT as a crash — is pinned by TestFrameRefusedAfterTheDequeueIsCounted.
	if got := metrics.DropCount(DropWriterRefused); got != 0 {
		t.Fatalf("writer_refused drops = %d, want 0: the crash was filed as ordinary backpressure", got)
	}
	released := writer.healthy.released()
	if len(released) != 1 || released[0].Peer != survivor {
		t.Fatalf("the writer got %d frames after the crash, want exactly the one for %s", len(released), survivor)
	}
	if queue.Len() != 0 {
		t.Fatalf("the queue still holds %d frames: the drain stopped at the crash", queue.Len())
	}
}
