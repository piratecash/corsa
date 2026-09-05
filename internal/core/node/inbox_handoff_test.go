package node

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// inbox_handoff_test.go weighs the two ways a session's inbox can carry a
// frame, because the choice is a trade and not an improvement.
//
// The inbox is the single largest fixed cost of a live session: 256 slots of
// protocol.Frame, a struct of 1224 bytes, is 313 KB per connection before a
// single frame arrives (13-measurements.md §8.4). Carrying pointers instead
// makes an idle queue cost 2 KB — but it moves the frame onto the heap on
// every arrival, so what is saved in resident memory is paid in allocation
// rate on the receive path.
//
// Both sides of that trade are measured HERE, in one run, rather than by
// changing production and comparing two runs: a benchmark that only exists in
// the "after" state cannot show what was given up.
//
// # SCOPE — read this before quoting any number from this file
//
// Everything here exercises a QUEUE MODEL: channels built by this file, filled
// by helpers in this file. **None of it drives the production reader loop**
// (node/peer_management.go), and none of it proves anything about session
// behaviour:
//
//   - the distinct-frames test cannot fail on a mistake in the real sender. It
//     shows the property holds for THIS file's helper, and the real sender's
//     safety was established by reading it, not by this test;
//   - the overflow test models "count the refusal and keep sending". The real
//     sender does something else: it reports errPeerSessionInboxOverflow on
//     errCh and STOPS reading. Those are different scenarios;
//   - the benchmarks measure ONE hand-over of a synthetic frame, not receive
//     plus parse plus dispatch under load. Per-frame cost from here does not
//     become application cost by multiplication.
//
// If the pointer form is ever adopted, the integration checks it needs are
// listed in 14-memory-cleanup.md §8 — they belong against the real loop, and
// they do not exist yet.
//
// Reference: docs/refactoring/dht/14-memory-cleanup.md §8.

// inboxBenchCapacity mirrors peerSessionInboxBuffer so the measurement
// describes the queue the node actually builds.
const inboxBenchCapacity = peerSessionInboxBuffer

// benchFrame is a frame of realistic shape: a type, a raw line and a small
// payload, so the nested slices a real frame carries are present. They are
// shared by BOTH variants — a value copy of a struct copies slice HEADERS,
// not their contents, so nested data is aliased either way and is not part of
// what this measures.
func benchFrame(i int) protocol.Frame {
	return protocol.Frame{
		Type:    "route_announce_v3",
		RawLine: fmt.Sprintf(`{"type":"route_announce_v3","seq":%d}`, i),
		Node:    "peer",
	}
}

// fillValueQueue and fillPointerQueue bring a queue to the requested
// occupancy, which is the axis that decides the answer: an empty queue costs
// its whole capacity in the value form and almost nothing in the pointer form,
// while a full one costs the same in both.
func fillValueQueue(queue chan protocol.Frame, occupancy int) {
	for i := range occupancy {
		queue <- benchFrame(i)
	}
}

func fillPointerQueue(queue chan *protocol.Frame, occupancy int) {
	for i := range occupancy {
		frame := benchFrame(i)
		queue <- &frame
	}
}

// TestInboxRetainedByQueueShape measures what one queue HOLDS at three
// occupancies, which is the number the per-session budget is built from.
//
// It is a test rather than a benchmark because the question is retention, not
// throughput: the queues are held alive across a GC and the heap is read on
// both sides.
func TestInboxRetainedByQueueShape(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates several hundred queues")
	}

	const queues = 100
	occupancies := []struct {
		name string
		held int
	}{
		{name: "empty", held: 0},
		{name: "half full", held: inboxBenchCapacity / 2},
		{name: "full", held: inboxBenchCapacity},
	}

	for _, occupancy := range occupancies {
		var valueQueues []chan protocol.Frame
		perValue := retainedBytes(func() any {
			valueQueues = make([]chan protocol.Frame, 0, queues)
			for range queues {
				queue := make(chan protocol.Frame, inboxBenchCapacity)
				fillValueQueue(queue, occupancy.held)
				valueQueues = append(valueQueues, queue)
			}
			return valueQueues
		}) / queues

		var pointerQueues []chan *protocol.Frame
		perPointer := retainedBytes(func() any {
			pointerQueues = make([]chan *protocol.Frame, 0, queues)
			for range queues {
				queue := make(chan *protocol.Frame, inboxBenchCapacity)
				fillPointerQueue(queue, occupancy.held)
				pointerQueues = append(pointerQueues, queue)
			}
			return pointerQueues
		}) / queues

		t.Logf("%-10s value %8d B/queue, pointer %8d B/queue", occupancy.name, perValue, perPointer)

		// The one assertion worth making, and it is about the EMPTY queue —
		// the state a session spends nearly all of its life in. Anything else
		// is a number to read, not a property to pin: a full queue holds the
		// same frames either way, and pinning its ratio would pin the
		// allocator.
		if occupancy.held == 0 && perPointer >= perValue {
			t.Fatalf("an empty pointer queue (%d B) is not cheaper than a value queue (%d B): the whole premise of this change is that capacity costs nothing until it is used",
				perPointer, perValue)
		}
	}
}

// consumeFrame is the work BOTH variants do with a delivered frame, so the
// two benchmarks differ only in how the frame arrived.
//
// It exists because the first version of these benchmarks drained the queue
// with a bare `for range queue {}`. That form binds no variable, so the
// runtime receives with a nil destination and **skips the element copy
// entirely** — the value variant was never charged for the receive-side copy
// the whole comparison is about, and the numbers flattered it. A consumer that
// does not touch the frame does not measure delivering one.
//
// The result is returned and checked so neither the compiler nor the reader
// can treat the work as dead.
func consumeFrame(frame *protocol.Frame) int {
	return len(frame.RawLine) + len(frame.Type) + len(frame.Node)
}

// BenchmarkInboxHandoffValue and its pointer twin measure the OTHER side of
// the trade: what one hand-over costs on the receive path, in time and in
// allocation, WITH the receiver actually reading the frame.
//
// These two are same-goroutine and are the weaker measurement: the channel
// slot stays hot in cache. The cross-goroutine pair below is the one the
// production shape resembles.
func BenchmarkInboxHandoffValue(b *testing.B) {
	queue := make(chan protocol.Frame, inboxBenchCapacity)
	sink := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		queue <- benchFrame(i)
		frame := <-queue
		sink += consumeFrame(&frame)
	}
	b.StopTimer()
	if sink == 0 {
		b.Fatal("the consumer read nothing: the benchmark measured an elided copy")
	}
}

func BenchmarkInboxHandoffPointer(b *testing.B) {
	queue := make(chan *protocol.Frame, inboxBenchCapacity)
	sink := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		frame := benchFrame(i)
		queue <- &frame
		sink += consumeFrame(<-queue)
	}
	b.StopTimer()
	if sink == 0 {
		b.Fatal("the consumer read nothing: the benchmark measured an elided copy")
	}
}

// BenchmarkInboxCrossGoroutineValue and its pointer twin are the CLOSEST of
// the four to the production shape — and still not close enough to decide
// anything on their own (see SCOPE at the top of the file).
//
// In production the socket reader sends and the session loop receives — two
// goroutines, usually two cores. A value hand-over then copies 1224 bytes
// TWICE across that boundary: the sender writes them into the channel slot and
// the receiver reads them out, both cache-cold. A same-goroutine benchmark
// hides that: the slot is still in L1 and the copy looks nearly free, which
// flatters the form this step is trying to replace.
func BenchmarkInboxCrossGoroutineValue(b *testing.B) {
	queue := make(chan protocol.Frame, inboxBenchCapacity)
	done := make(chan int, 1)
	go func() {
		sink := 0
		for frame := range queue {
			sink += consumeFrame(&frame)
		}
		done <- sink
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		queue <- benchFrame(i)
	}
	b.StopTimer()
	close(queue)
	if sink := <-done; sink == 0 {
		b.Fatal("the consumer read nothing: the benchmark measured an elided copy")
	}
}

func BenchmarkInboxCrossGoroutinePointer(b *testing.B) {
	queue := make(chan *protocol.Frame, inboxBenchCapacity)
	done := make(chan int, 1)
	go func() {
		sink := 0
		for frame := range queue {
			sink += consumeFrame(frame)
		}
		done <- sink
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		frame := benchFrame(i)
		queue <- &frame
	}
	b.StopTimer()
	close(queue)
	if sink := <-done; sink == 0 {
		b.Fatal("the consumer read nothing: the benchmark measured an elided copy")
	}
}

// TestInboxQueueModelOrderingCapacityAndClose pins three properties OF THE
// QUEUE in its pointer form: order, capacity, and what a full one does.
//
// Named "queue model" deliberately. It does not show that session behaviour is
// unchanged — the session's behaviour on a full inbox is to report overflow
// and stop reading, and that lives in the reader loop, not here.
func TestInboxQueueModelOrderingCapacityAndClose(t *testing.T) {
	t.Parallel()

	queue := make(chan *protocol.Frame, inboxBenchCapacity)
	fillPointerQueue(queue, inboxBenchCapacity)

	// Capacity: the queue is full and the non-blocking send fails, which is
	// exactly what the reader loop relies on to report overflow instead of
	// stalling the socket.
	extra := benchFrame(-1)
	select {
	case queue <- &extra:
		t.Fatal("a full queue accepted one more frame: backpressure is what turns overflow into a reported error rather than a stalled reader")
	default:
	}

	// Order: FIFO, and each frame is the one that was sent, not a neighbour.
	for i := range inboxBenchCapacity {
		got := <-queue
		want := benchFrame(i)
		if got.RawLine != want.RawLine {
			t.Fatalf("frame %d came back as %q, want %q: the queue reordered", i, got.RawLine, want.RawLine)
		}
	}

	// Close: what matters is that closing a NON-EMPTY queue still hands over
	// everything already in it, in order, and only then reports the end. The
	// first version of this closed an already-drained queue and checked the
	// nil/false alone — which asserts a property of `close` and says nothing
	// about the frames a session would lose if the socket died with work
	// queued.
	refilled := make(chan *protocol.Frame, inboxBenchCapacity)
	fillPointerQueue(refilled, inboxBenchCapacity)
	close(refilled)

	drained := 0
	for frame := range refilled {
		want := benchFrame(drained)
		if frame.RawLine != want.RawLine {
			t.Fatalf("after close, frame %d came back as %q, want %q: closing reordered or dropped",
				drained, frame.RawLine, want.RawLine)
		}
		drained++
	}
	if drained != inboxBenchCapacity {
		t.Fatalf("a closed queue handed over %d of %d frames: closing must not discard what was already queued",
			drained, inboxBenchCapacity)
	}
	if frame, open := <-refilled; open || frame != nil {
		t.Fatalf("a drained closed queue answered %v/%v, want nil/false", frame, open)
	}
}

// TestInboxQueueModelHandsOverDistinctFrames shows what aliasing would look
// like, and is explicitly NOT a guard on the production sender.
//
// Value semantics gave the receiver its own copy for free. Pointers do not: a
// sender that reused one frame variable across iterations would put N pointers
// to one object in the queue, and the receiver would see the last frame N
// times. This test demonstrates the property for THIS FILE's helper.
//
// **The real sender is not exercised here.** Its safety was established by
// reading it — the frame is declared inside the read loop with `:=` and is not
// touched after the send — and a regression there would leave this test green.
// A guard that covers it has to drive the reader loop itself, which is one of
// the integration checks 14-memory-cleanup.md §8 lists as missing.
func TestInboxQueueModelHandsOverDistinctFrames(t *testing.T) {
	t.Parallel()

	const frames = 8
	queue := make(chan *protocol.Frame, frames)
	fillPointerQueue(queue, frames)

	seen := make(map[*protocol.Frame]struct{}, frames)
	for i := range frames {
		frame := <-queue
		if _, duplicate := seen[frame]; duplicate {
			t.Fatalf("frame %d is the SAME object as an earlier one: the sender is reusing its variable, and every reader sees the last frame", i)
		}
		seen[frame] = struct{}{}
		if want := benchFrame(i).RawLine; frame.RawLine != want {
			t.Fatalf("frame %d carries %q, want %q", i, frame.RawLine, want)
		}
	}
}

// frameLedger records every frame a consumer took and fails the moment one
// object arrives twice.
//
// It exists because the first version of the concurrency test checked for
// duplicates in ONE of its two receive branches. When the sender finished
// before the consumer — which is the common outcome, not an edge case — most
// frames arrived through the OTHER branch and were recorded unchecked, while
// `received + dropped == sent` still balanced. The aliasing this file is here
// to detect could have passed unnoticed with the arithmetic looking right.
//
// One recorder, used by every branch, is the fix: a check that lives in a
// branch is a check that a branch can skip.
type frameLedger struct {
	seen map[*protocol.Frame]struct{}
}

func newFrameLedger(capacity int) *frameLedger {
	return &frameLedger{seen: make(map[*protocol.Frame]struct{}, capacity)}
}

func (l *frameLedger) record(t *testing.T, frame *protocol.Frame) {
	t.Helper()
	if frame == nil {
		t.Fatal("the queue handed over a nil frame")
	}
	if _, duplicate := l.seen[frame]; duplicate {
		t.Fatalf("two hand-overs carried the SAME object (%q): the sender is reusing its variable",
			frame.RawLine)
	}
	l.seen[frame] = struct{}{}
}

func (l *frameLedger) count() int { return len(l.seen) }

// TestInboxQueueModelUnderConcurrentSenderAndSlowConsumer is the concurrency
// half, written to be run under -race.
//
// It covers BOTH orderings of sender and consumer, because they exercise
// different code and only one of them was covered before:
//
//   - "consumer keeps up": the consumer runs while the sender sends, so frames
//     arrive on the live branch;
//   - "sender finishes first": the consumer does not start until the sender is
//     done, so the queue is FULL when draining begins and every frame arrives
//     on the drain branch. This is the ordering that used to skip the
//     duplicate check entirely.
//
// What it shows: nothing is lost between "delivered" and "refused", a full
// queue refuses rather than blocks, and no two hand-overs alias.
//
// What it does NOT show: the session's response to overflow. Here a refusal is
// counted and sending continues; the real sender reports
// errPeerSessionInboxOverflow and stops reading the socket. The scenarios
// differ on purpose — this one keeps the queue saturated long enough to be
// worth measuring, which the real one, by design, does not.
func TestInboxQueueModelUnderConcurrentSenderAndSlowConsumer(t *testing.T) {
	t.Parallel()

	const sent = 2_000

	cases := map[string]bool{
		"consumer keeps up":     false,
		"sender finishes first": true,
	}
	for name, drainOnlyAfterSender := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			queue := make(chan *protocol.Frame, inboxBenchCapacity)
			refused := make(chan int, 1)
			done := make(chan struct{})

			go func() {
				defer close(done)
				dropped := 0
				for i := range sent {
					frame := benchFrame(i)
					select {
					case queue <- &frame:
					default:
						// A refusal is the documented outcome of a full inbox,
						// not a failure: the reader turns it into an overflow
						// error.
						dropped++
					}
				}
				refused <- dropped
			}()

			ledger := newFrameLedger(sent)
			deadline := time.After(30 * time.Second)

			if drainOnlyAfterSender {
				// Deliberately consume NOTHING until the sender is finished.
				// The queue is then full and every frame below arrives through
				// the drain path — the one that used to record without
				// checking.
				select {
				case <-done:
				case <-deadline:
					t.Fatal("the sender did not finish while nobody was consuming")
				}
				if len(queue) != inboxBenchCapacity {
					t.Fatalf("the queue holds %d of %d frames: this case is only meaningful with a full queue",
						len(queue), inboxBenchCapacity)
				}
			}

			for {
				select {
				case frame := <-queue:
					ledger.record(t, frame)
					// A deliberately slow consumer, so the queue really fills.
					if ledger.count()%64 == 0 {
						runtime.Gosched()
					}
				case <-done:
					// Same recorder on the drain path — the whole point of the
					// ledger.
					for {
						select {
						case frame := <-queue:
							ledger.record(t, frame)
							continue
						default:
						}
						break
					}
					dropped := <-refused
					if ledger.count()+dropped != sent {
						t.Fatalf("received %d + refused %d = %d, want %d: a frame was lost between the two",
							ledger.count(), dropped, ledger.count()+dropped, sent)
					}
					if drainOnlyAfterSender && ledger.count() != inboxBenchCapacity {
						t.Fatalf("draining a full queue after the sender finished yielded %d frames, want %d",
							ledger.count(), inboxBenchCapacity)
					}
					return
				case <-deadline:
					t.Fatalf("the consumer stalled after %d frames", ledger.count())
				}
			}
		})
	}
}

// inboxFrameBytes reports what one protocol.Frame occupies, so the numbers in
// 14-memory-cleanup.md can be recomputed rather than trusted.
func TestInboxFrameSizeIsWorthCarryingByPointer(t *testing.T) {
	t.Parallel()

	frameBytes := domain.SizeOfAll(protocol.Frame{})
	pointerBytes := domain.SizeOfAll((*protocol.Frame)(nil))
	if frameBytes <= pointerBytes*8 {
		t.Skipf("protocol.Frame is only %d B against a %d B pointer: the trade this step makes is no longer worth its allocation cost",
			frameBytes, pointerBytes)
	}
	t.Logf("protocol.Frame = %d B, pointer = %d B; an idle %d-slot inbox holds %d B by value against %d B by pointer",
		frameBytes, pointerBytes, inboxBenchCapacity,
		frameBytes*uint64(inboxBenchCapacity), pointerBytes*uint64(inboxBenchCapacity))
}
