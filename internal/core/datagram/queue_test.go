package datagram

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// queue_test.go covers the weighted class queue of §5. The headline property
// is the one §9 names: bulk keeps its guaranteed share under a permanent
// control stream. Everything else here is the overflow and send_until
// contract the doc comment on WeightedQueue promises.

func queuedFrame(class domain.DatagramClass, bytes int, sendUntil time.Time) QueuedFrame {
	return QueuedFrame{
		SendUntil: sendUntil,
		Peer:      domaintest.ID("queue-peer"),
		Frame:     protocol.DatagramFrame{Class: class},
		Class:     class,
		Bytes:     bytes,
	}
}

// Strict priority would starve bulk completely under a permanent control
// stream, and a permanent control stream is cheap to produce. The queue must
// hand bulk its guaranteed share of the DISPATCHED BYTES instead (§5).
func TestQueueBulkKeepsItsShareUnderConstantControl(t *testing.T) {
	t.Parallel()

	caps := DefaultLimits().Queue
	queue := NewWeightedQueue(WeightedQueueConfig{Caps: QueueCaps{
		ControlWeight: caps.ControlWeight,
		BulkWeight:    caps.BulkWeight,
		QuantumBytes:  caps.QuantumBytes,
		ControlFrames: 4_096,
		ControlBytes:  1 << 30,
		BulkFrames:    4_096,
		BulkBytes:     1 << 30,
	}})

	const (
		controlBytes = 1 << 10
		bulkBytes    = 16 << 10
		dispatches   = 400
	)
	// Both classes stay saturated for the whole run: the control lane is
	// refilled after every dispatch, which is exactly the "permanent control
	// stream" §5 is worried about.
	for i := 0; i < dispatches*2; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassControl, controlBytes, time.Time{}))
	}
	for i := 0; i < dispatches; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassBulk, bulkBytes, time.Time{}))
	}

	var control, bulk int
	for i := 0; i < dispatches; i++ {
		item, ok := queue.Dequeue()
		if !ok {
			t.Fatalf("queue ran dry after %d dispatches", i)
		}
		switch item.Frame.Class {
		case domain.DatagramClassControl:
			control += item.Bytes
			queue.Enqueue(queuedFrame(domain.DatagramClassControl, controlBytes, time.Time{}))
		case domain.DatagramClassBulk:
			bulk += item.Bytes
		}
	}

	if bulk == 0 {
		t.Fatal("bulk was starved completely: this is strict priority, not a weighted queue")
	}
	share := float64(bulk) / float64(bulk+control)
	guaranteed := float64(caps.BulkWeight) / float64(caps.BulkWeight+caps.ControlWeight)
	if share < guaranteed*0.9 {
		t.Fatalf("bulk got %.3f of the dispatched bytes, guaranteed share is %.3f (control %d B, bulk %d B)",
			share, guaranteed, control, bulk)
	}
	if share > guaranteed*1.5 {
		t.Fatalf("bulk got %.3f of the bytes, well above its %.3f share: control is no longer served first",
			share, guaranteed)
	}
}

// Within its share control goes first: with both lanes loaded, the very first
// dispatches are control, and bulk appears only once control has spent its
// quantum.
func TestQueueServesControlFirstWithinItsShare(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{Caps: QueueCaps{
		ControlWeight: 3,
		BulkWeight:    1,
		QuantumBytes:  4_000,
		ControlFrames: 100,
		ControlBytes:  1 << 20,
		BulkFrames:    100,
		BulkBytes:     1 << 20,
	}})

	// Bulk is enqueued FIRST, so a plain FIFO would hand it out first.
	for i := 0; i < 4; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassBulk, 4_000, time.Time{}))
	}
	for i := 0; i < 4; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassControl, 3_000, time.Time{}))
	}

	first, ok := queue.Dequeue()
	if !ok || first.Frame.Class != domain.DatagramClassControl {
		t.Fatalf("first dispatch = %v (ok %v), want control", first.Frame.Class, ok)
	}
	// Control's quantum is 12 000 bytes, so four 3 000-byte control frames
	// fit in one round and bulk waits behind all of them.
	for i := 1; i < 4; i++ {
		item, ok := queue.Dequeue()
		if !ok || item.Frame.Class != domain.DatagramClassControl {
			t.Fatalf("dispatch %d = %v (ok %v), want control", i, item.Frame.Class, ok)
		}
	}
	item, ok := queue.Dequeue()
	if !ok || item.Frame.Class != domain.DatagramClassBulk {
		t.Fatalf("dispatch 4 = %v (ok %v), want bulk: control's share must end", item.Frame.Class, ok)
	}
}

// An idle lane banks nothing. Without the reset a class that went quiet would
// come back with a round's worth of credit for every round it missed and
// starve the other one.
func TestQueueIdleLaneBanksNothing(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{Caps: QueueCaps{
		ControlWeight: 1,
		BulkWeight:    1,
		QuantumBytes:  1_000,
		ControlFrames: 100,
		ControlBytes:  1 << 20,
		BulkFrames:    100,
		BulkBytes:     1 << 20,
	}})

	// Control alone for a long while.
	for i := 0; i < 20; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassControl, 1_000, time.Time{}))
	}
	for i := 0; i < 20; i++ {
		if _, ok := queue.Dequeue(); !ok {
			t.Fatalf("control drain stopped at %d", i)
		}
	}

	// Now both classes arrive. Bulk must not be able to spend twenty rounds
	// of banked credit in a row.
	for i := 0; i < 10; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassBulk, 1_000, time.Time{}))
		queue.Enqueue(queuedFrame(domain.DatagramClassControl, 1_000, time.Time{}))
	}
	streak := 0
	for i := 0; i < 10; i++ {
		item, ok := queue.Dequeue()
		if !ok {
			t.Fatalf("queue ran dry at %d", i)
		}
		if item.Frame.Class != domain.DatagramClassBulk {
			break
		}
		streak++
	}
	if streak > 2 {
		t.Fatalf("bulk dispatched %d frames in a row after an idle spell: it banked credit", streak)
	}
}

// At the bound the NEW frame is refused and the queued ones stay: a queued
// frame may already have a reservation or a fixed reverse downstream behind
// it, and evicting it would turn a queued answer into a silent loss.
func TestQueueRefusesTheNewcomerAtTheBound(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{Caps: QueueCaps{
		ControlFrames: 3,
		ControlBytes:  1 << 20,
		BulkFrames:    2,
		BulkBytes:     1 << 20,
	}})

	for i := 0; i < 3; i++ {
		if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 100+i, time.Time{})) {
			t.Fatalf("control frame %d refused below the bound", i)
		}
	}
	if queue.Enqueue(queuedFrame(domain.DatagramClassControl, 999, time.Time{})) {
		t.Fatal("the control lane accepted a frame past its frame bound")
	}
	// The bulk lane has its own room and is unaffected: the bound is per
	// lane, and a full control lane must not stop file traffic.
	if !queue.Enqueue(queuedFrame(domain.DatagramClassBulk, 4_000, time.Time{})) {
		t.Fatal("a full control lane refused a bulk frame")
	}

	// Nothing queued was evicted: the three original control frames are
	// still there, in order.
	for i := 0; i < 3; i++ {
		item, ok := queue.Dequeue()
		if !ok {
			t.Fatalf("queued control frame %d disappeared", i)
		}
		if item.Frame.Class != domain.DatagramClassControl || item.Bytes != 100+i {
			t.Fatalf("dispatch %d = %v/%d B, want the queued control frame of %d B",
				i, item.Frame.Class, item.Bytes, 100+i)
		}
	}
	if stats := queue.Stats(); stats.RefusedFull != 1 {
		t.Fatalf("RefusedFull = %d, want 1", stats.RefusedFull)
	}
}

// The byte bound binds independently of the frame bound: a lane sized in
// bytes must not be filled by a handful of maximum frames.
func TestQueueByteBoundBindsIndependently(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{Caps: QueueCaps{
		ControlFrames: 1_000,
		ControlBytes:  10_000,
		BulkFrames:    1_000,
		BulkBytes:     10_000,
	}})

	if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 6_000, time.Time{})) {
		t.Fatal("the first frame was refused")
	}
	if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 4_000, time.Time{})) {
		t.Fatal("the second frame was refused at exactly the bound")
	}
	if queue.Enqueue(queuedFrame(domain.DatagramClassControl, 1, time.Time{})) {
		t.Fatal("the lane accepted a frame past its byte bound")
	}
}

// The send deadline is checked on both sides: a frame that is already dead is
// refused on the way in, and one that died while waiting is dropped on the
// way out instead of being handed to the writer (§4.2, §5).
func TestQueueHonoursSendUntilOnBothSides(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	queue := NewWeightedQueue(WeightedQueueConfig{Clock: clock.Now, Caps: QueueCaps{
		ControlFrames: 100,
		ControlBytes:  1 << 20,
		BulkFrames:    100,
		BulkBytes:     1 << 20,
	}})
	now := clock.Now()

	if queue.Enqueue(queuedFrame(domain.DatagramClassControl, 100, now.Add(-time.Second))) {
		t.Fatal("a frame past its deadline was queued")
	}
	// The deadline itself is alive, exactly as it is for validity (§2.2).
	if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 100, now)) {
		t.Fatal("a frame exactly at its deadline was refused")
	}
	if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 200, now.Add(10*time.Second))) {
		t.Fatal("a live frame was refused")
	}

	clock.advance(time.Second)
	item, ok := queue.Dequeue()
	if !ok {
		t.Fatal("the live frame was not dispatched")
	}
	if item.Bytes != 200 {
		t.Fatalf("dispatched the %d B frame, want the live 200 B one", item.Bytes)
	}

	stats := queue.Stats()
	if stats.RefusedExpired != 1 {
		t.Fatalf("RefusedExpired = %d, want 1", stats.RefusedExpired)
	}
	if stats.DroppedExpired != 1 {
		t.Fatalf("DroppedExpired = %d, want 1", stats.DroppedExpired)
	}
}

// A queue nobody is draining must not hold dead frames against its bound, and
// the sweep has to reach EVERY lane rather than the one Dequeue happens to walk:
// a lane that is never served would otherwise keep its dead frames — and their
// bytes — against the bound forever, refusing live traffic on a queue that is
// empty in every sense that matters.
func TestQueueDropExpiredSweepsEveryLane(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	queue := NewWeightedQueue(WeightedQueueConfig{Clock: clock.Now, Caps: QueueCaps{
		ControlFrames: 10,
		ControlBytes:  1 << 20,
		BulkFrames:    10,
		BulkBytes:     1 << 20,
	}})
	now := clock.Now()

	queue.Enqueue(queuedFrame(domain.DatagramClassControl, 100, now.Add(time.Second)))
	queue.Enqueue(queuedFrame(domain.DatagramClassControl, 100, now.Add(time.Hour)))
	queue.Enqueue(queuedFrame(domain.DatagramClassBulk, 5_000, now.Add(time.Second)))
	queue.Enqueue(queuedFrame(domain.DatagramClassBulk, 100, now.Add(time.Second)))

	clock.advance(time.Minute)
	if dropped := queue.DropExpired(); dropped != 3 {
		t.Fatalf("DropExpired = %d, want 3", dropped)
	}
	if queue.Len() != 1 {
		t.Fatalf("queue holds %d frames after the sweep, want 1", queue.Len())
	}
	stats := queue.Stats()
	if stats.ControlBytes != 100 || stats.BulkBytes != 0 {
		t.Fatalf("byte accounting drifted after the sweep: %+v", stats)
	}
	if stats.DroppedExpired != 3 {
		t.Fatalf("DroppedExpired = %d, want 3: a dropped frame is observable only here", stats.DroppedExpired)
	}
}

// The head check inside Dequeue removes frames on the same deadline: it is the
// other half of the sweep, and a leak in it would be invisible to a queue that
// IS being drained.
func TestQueueDequeueDropsTheDeadHeadsItStepsOver(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	queue := NewWeightedQueue(WeightedQueueConfig{Clock: clock.Now, Caps: QueueCaps{
		ControlFrames: 10,
		ControlBytes:  1 << 20,
		BulkFrames:    10,
		BulkBytes:     1 << 20,
	}})
	now := clock.Now()

	queue.Enqueue(queuedFrame(domain.DatagramClassControl, 100, now.Add(time.Second)))
	live := queuedFrame(domain.DatagramClassControl, 200, now.Add(time.Hour))
	queue.Enqueue(live)

	clock.advance(time.Minute)
	item, ok := queue.Dequeue()
	if !ok {
		t.Fatal("the live frame was not dispatched")
	}
	if item.Bytes != live.Bytes {
		t.Fatalf("dispatched a frame of %d bytes, want the live %d", item.Bytes, live.Bytes)
	}
	if stats := queue.Stats(); stats.DroppedExpired != 1 {
		t.Fatalf("DroppedExpired = %d, want 1", stats.DroppedExpired)
	}
}

// A frame without a serialized size, or of a class the layer has no lane for,
// is refused: a guessed size is a wrong share (§5).
func TestQueueRefusesUnmeasuredAndUnknownClass(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{})

	if queue.Enqueue(queuedFrame(domain.DatagramClassControl, 0, time.Time{})) {
		t.Fatal("a frame with no serialized size was queued")
	}
	if queue.Enqueue(queuedFrame(domain.DatagramClass("gossip"), 100, time.Time{})) {
		t.Fatal("a frame of an unknown class found a lane")
	}
	if stats := queue.Stats(); stats.RefusedInvalid != 2 {
		t.Fatalf("RefusedInvalid = %d, want 2", stats.RefusedInvalid)
	}
}

// The wake-up signal coalesces: one poke per burst is all a pump that drains
// to empty needs, and Enqueue must never block on it.
func TestQueueReadySignalCoalesces(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{})
	for i := 0; i < 100; i++ {
		if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 10, time.Time{})) {
			t.Fatalf("enqueue %d refused", i)
		}
	}
	select {
	case <-queue.Ready():
	default:
		t.Fatal("no wake-up after 100 enqueues")
	}
	select {
	case <-queue.Ready():
		t.Fatal("the wake-up channel is not coalescing")
	default:
	}
}

// The queue is shared by every publishing goroutine, so concurrent enqueue
// and drain must not lose or duplicate a frame.
func TestQueueConcurrentEnqueueAndDrain(t *testing.T) {
	t.Parallel()

	queue := NewWeightedQueue(WeightedQueueConfig{Caps: QueueCaps{
		ControlFrames: 10_000,
		ControlBytes:  1 << 30,
		BulkFrames:    10_000,
		BulkBytes:     1 << 30,
	}})

	const (
		writers = 8
		each    = 200
	)
	var producers sync.WaitGroup
	producers.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer producers.Done()
			class := domain.DatagramClassControl
			if w%2 == 1 {
				class = domain.DatagramClassBulk
			}
			for i := 0; i < each; i++ {
				queue.Enqueue(queuedFrame(class, 100, time.Time{}))
			}
		}(w)
	}

	var (
		drained sync.WaitGroup
		counts  = make([]int, 4)
	)
	drained.Add(len(counts))
	for d := range counts {
		go func(d int) {
			defer drained.Done()
			for {
				if _, ok := queue.Dequeue(); ok {
					counts[d]++
					continue
				}
				select {
				case <-queue.Ready():
				case <-time.After(5 * time.Millisecond):
				}
				if _, ok := queue.Dequeue(); ok {
					counts[d]++
					continue
				}
				return
			}
		}(d)
	}
	producers.Wait()
	drained.Wait()

	total := 0
	for _, count := range counts {
		total += count
	}
	total += queue.Len()
	// A drainer may finish before the last producer; the invariant is that
	// nothing was lost or duplicated, not that everything was drained.
	stats := queue.Stats()
	if uint64(total) != stats.Enqueued {
		t.Fatalf("drained %d + queued %d frames for %d enqueued", total-queue.Len(), queue.Len(), stats.Enqueued)
	}
}

// The serialized size the queue accounts in is the WIRE size, including
// base64 and the auth block (§5) — the same number admission is charged.
func TestQueueMeasureFrameMatchesTheWireLine(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "measurer"})
	private, sender := newSigner(t)
	frame := signedRouted(t, routedOpts{
		now:     node.clock(),
		private: private,
		src:     sender,
		dst:     node.id,
		payload: []byte("payload"),
	})

	measured, err := MeasureFrame(frame)
	if err != nil {
		t.Fatalf("MeasureFrame: %v", err)
	}
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	if measured != len(line) {
		t.Fatalf("MeasureFrame = %d, wire line is %d bytes", measured, len(line))
	}
}
