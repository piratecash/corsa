package datagram

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// queue_emitter_test.go is the END-TO-END half of §5: the class queue sits on
// the path a real frame takes, not beside it.
//
// The gap it closes: the weighted queue had no producer and no consumer
// outside its own unit tests, so "bulk keeps a guaranteed share under a
// constant control stream" (§9) was a property of a type nobody called. The
// pipeline published straight to the writer, first come first served.

// recordingWriter is the writer end of the layer: it records what the class
// queue released, in release order.
type recordingWriter struct {
	mu    sync.Mutex
	sent  []OutboundFrame
	lines int
}

func (w *recordingWriter) EmitTo(_ context.Context, out OutboundFrame) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, out)
	if len(out.Line) > 0 {
		w.lines++
	}
	return true
}

func (w *recordingWriter) released() []OutboundFrame {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]OutboundFrame(nil), w.sent...)
}

// TestBulkKeepsItsShareThroughTheLayersOwnPath drives the queue exactly as the
// conveyor does — Pipeline.SendLocal → Pipeline.emit → ClassQueueEmitter — and
// asserts §5's guarantee on what actually came out.
//
// The control stream is kept PERMANENT for the whole run, refilled through the
// pipeline after every dispatch: that is the situation §5 is about, and it is
// also what separates a weighted queue from no queue at all. Without one the
// conveyor hands frames to the writer in submission order, so a control stream
// that never stops means the bulk frames behind it never move.
func TestBulkKeepsItsShareThroughTheLayersOwnPath(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	writer := &recordingWriter{}
	caps := DefaultLimits().Queue
	node := newQueuedNode(t, net, QueueCaps{
		ControlWeight: caps.ControlWeight,
		BulkWeight:    caps.BulkWeight,
		QuantumBytes:  caps.QuantumBytes,
		ControlFrames: 4096,
		ControlBytes:  1 << 30,
		BulkFrames:    4096,
		BulkBytes:     1 << 30,
	}, writer)

	private, signer := newSigner(t)
	dst := domaintest.ID("far-destination")
	hop := newPipelineNode(t, net, nodeOpts{name: "queued-hop", transit: true})
	link(node, hop, true, true)
	route(node, dst, hop.id, 2)

	send := func(class domain.DatagramClass, payload []byte) {
		outcome := node.pipeline.SendLocal(context.Background(), LocalSendOpts{
			Frame: signedRouted(t, routedOpts{
				private: private, src: signer, dst: dst, now: node.clock(),
				class: class, payload: payload,
			}),
		})
		if outcome.Kind() != SendQueued {
			t.Fatalf("%s send outcome = %s", class, outcome)
		}
	}

	// The frame sizes matter: deficit round robin hands out a BYTE share, and
	// it converges on it only when a lane's quantum fits a few of its frames.
	// A bulk frame near four kilobytes sits comfortably inside the 16 KiB bulk
	// quantum, while the 48 KiB control quantum takes about a hundred of the
	// small control frames — so a round is short enough that four hundred
	// dispatches cover several of them.
	const (
		backlog     = 2000
		dispatches  = 1600
		bulkPayload = 4 << 10
	)
	bulk := make([]byte, bulkPayload)
	// Control goes in FIRST and keeps coming; bulk waits behind it. In
	// submission order not one bulk frame would move within this window.
	for i := 0; i < backlog; i++ {
		send(domain.DatagramClassControl, []byte("control"))
	}
	for i := 0; i < backlog; i++ {
		send(domain.DatagramClassBulk, bulk)
	}

	emitter, wired := node.pipeline.OutboundQueue()
	if !wired {
		t.Fatal("the pipeline must expose the class queue it was wired with")
	}

	var controlBytes, bulkBytes int
	for i := 0; i < dispatches; i++ {
		item, ok := emitter.Queue().Dequeue()
		if !ok {
			t.Fatalf("the queue ran dry after %d dispatches", i)
		}
		switch item.Class {
		case domain.DatagramClassControl:
			controlBytes += item.Bytes
			// Refill: the control stream never stops.
			send(domain.DatagramClassControl, []byte("control"))
		case domain.DatagramClassBulk:
			bulkBytes += item.Bytes
		}
	}

	if bulkBytes == 0 {
		t.Fatal("bulk was starved completely: the layer is not putting the class queue on the path")
	}
	share := float64(bulkBytes) / float64(bulkBytes+controlBytes)
	guaranteed := float64(caps.BulkWeight) / float64(caps.BulkWeight+caps.ControlWeight)
	if share < guaranteed*0.9 {
		t.Fatalf("bulk got %.3f of the dispatched bytes, guaranteed share is %.3f", share, guaranteed)
	}

	// And the frames the pump releases reach the writer with their line, so
	// nothing downstream serializes them a second time.
	emitter.Drain(context.Background())
	released := writer.released()
	if len(released) == 0 {
		t.Fatal("the pump released nothing to the writer")
	}
	if writer.lines != len(released) {
		t.Fatalf("%d of %d frames reached the writer without their serialized line",
			len(released)-writer.lines, len(released))
	}
}

// TestQueuedEmissionCarriesTheClassAndTheDeadline pins the other half of the
// hand-over contract: the writer receives the class and the send deadline of
// EVERY frame the layer emits, so §4.2's "the writer drops a frame that
// outlived its deadline" is true of the real path and not of one plane.
func TestQueuedEmissionCarriesTheClassAndTheDeadline(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	writer := &recordingWriter{}
	node := newQueuedNode(t, net, QueueCaps{}, writer)

	private, signer := newSigner(t)
	dst := domaintest.ID("far-destination")
	hop := newPipelineNode(t, net, nodeOpts{name: "deadline-hop", transit: true})
	link(node, hop, true, true)
	route(node, dst, hop.id, 2)

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: dst, now: node.clock(),
		class: domain.DatagramClassBulk, payload: []byte("chunk"),
	})
	if outcome := node.pipeline.SendLocal(context.Background(), LocalSendOpts{Frame: frame}); outcome.Kind() != SendQueued {
		t.Fatalf("outcome = %s", outcome)
	}
	emitter, _ := node.pipeline.OutboundQueue()
	emitter.Drain(context.Background())

	released := writer.released()
	if len(released) != 1 {
		t.Fatalf("the writer saw %d frames, want 1", len(released))
	}
	out := released[0]
	if out.Class != domain.DatagramClassBulk {
		t.Fatalf("class = %s, want bulk — the writer picks its write grace from it", out.Class)
	}
	if out.SendUntil.IsZero() {
		t.Fatal("a bulk frame reached the writer without a send deadline")
	}
	// The bulk queue residence is what bounds it (§4.2), and the layer's own
	// clamps may only shorten it.
	residence, err := domain.QueueResidence(domain.DatagramClassBulk)
	if err != nil {
		t.Fatalf("QueueResidence: %v", err)
	}
	if out.SendUntil.After(node.clock().Add(residence)) {
		t.Fatalf("send deadline %s is past the bulk queue residence", out.SendUntil)
	}
	// And the line the layer serialized is the line handed over: nothing
	// downstream has to serialize the frame a second time.
	line, err := protocol.MarshalDatagramFrameLine(out.Frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	if string(out.Line) != line {
		t.Fatal("the hand-over line does not match the frame it carries")
	}
	if out.Bytes() != len(line) {
		t.Fatalf("Bytes() = %d, want %d", out.Bytes(), len(line))
	}
}

// newQueuedNode builds a pipeline wired with a class queue in front of the
// given writer.
func newQueuedNode(t *testing.T, net *fakeNetwork, caps QueueCaps, writer FrameEmitter) *pipelineNode {
	t.Helper()
	node := newPipelineNode(t, net, nodeOpts{name: "queued-node", transit: true})
	// The queue reads the SAME clock as the pipeline: a queue on wall time
	// would call every frame of a clock-pinned fixture expired on the way in.
	queue := NewWeightedQueue(WeightedQueueConfig{Clock: node.clock, Caps: caps})
	pipeline, err := NewPipeline(PipelineConfig{
		Clock:       node.clock,
		Types:       node.types,
		ReplayCache: node.replay,
		Reverse:     node.reverse,
		Scheduler:   node.scheduler,
		Emitter:     writer,
		Queue:       queue,
		Metrics:     node.metrics,
		Advertised:  NewAdvertisedCapabilities([]string{CapabilityDatagramV1.String(), CapabilityDatagramTransitV1.String()}),
		Network:     testNetwork,
		LocalID:     node.id,
	})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	node.pipeline = pipeline
	return node
}

// refusingWriter turns every frame away, the way a peer whose session died
// between the enqueue and the dequeue does.
type refusingWriter struct{}

func (refusingWriter) EmitTo(context.Context, OutboundFrame) bool { return false }

// TestFrameRefusedAfterTheDequeueIsCounted pins the §10 rule "dropped by
// reason" on the one path that used to lose a frame silently: the queue never
// puts a refused frame back — already-queued frames are not re-ordered or
// resurrected — so a frame the writer turns away here is gone from the layer
// and appeared in no counter at all.
func TestFrameRefusedAfterTheDequeueIsCounted(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	queue := NewWeightedQueue(WeightedQueueConfig{Clock: time.Now})
	emitter, err := NewClassQueueEmitter(ClassQueueEmitterConfig{
		Queue:   queue,
		Out:     refusingWriter{},
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("NewClassQueueEmitter: %v", err)
	}
	if !emitter.EmitTo(context.Background(), OutboundFrame{
		Peer:  domaintest.ID("hop"),
		Line:  []byte("{}\n"),
		Class: domain.DatagramClassControl,
	}) {
		t.Fatal("the queue refused the frame")
	}

	if sent := emitter.Drain(context.Background()); sent != 0 {
		t.Fatalf("the writer refused everything, Drain reported %d sent", sent)
	}
	if got := metrics.DropCount(DropWriterRefused); got != 1 {
		t.Fatalf("writer_refused drops = %d, want 1 — a frame lost between the queue and the socket must be observable", got)
	}
	// It moves the reason breakdown ONLY: the frame was never an inbound one,
	// so counting it as observed would make "observed" mean two things.
	if got := metrics.Snapshot().Observed; got != 0 {
		t.Fatalf("observed = %d, want 0: an outbound drop is not an inbound frame", got)
	}
}
