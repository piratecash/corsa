package netcoretest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// unknownConnIDOffset puts the id the contract asks about far past every id a
// harness hands out, so "unknown" never depends on how an implementation
// numbers its connections.
const unknownConnIDOffset domain.ConnID = 1 << 40

// contractFrameCount is how many frames the ordering case sends. It stays far
// below every queue depth involved, so a refusal here is a contract failure and
// never back-pressure.
const contractFrameCount = 8

// saturationAttemptLimit bounds the saturation case. Every implementation's
// queue is far shallower (512 frames), so reaching it means the queue never
// reported itself full.
const saturationAttemptLimit = 1 << 16

// contractWaitBound bounds a send the contract expects to be refused at once
// or to wait for room. It runs in the bubble's virtual time, so it costs a
// working implementation nothing. It must stay well below the bridge writer's
// per-write deadline on an inbound conn (the direction every saturation case
// registers): past that deadline the writer parked on the unread remote fails
// its write and the link dies, and a bounded wait would end in teardown
// instead of in its own ctx.
const contractWaitBound = time.Second

// contractDirections are both directions a connection can be registered in.
var contractDirections = []netcore.Direction{netcore.Inbound, netcore.Outbound}

// Harness is one netcore.Network implementation wired the way its owner wires
// it in production, plus the observables the contract needs and Network does
// not expose. RunNetworkContract builds a fresh Harness for every case, so a
// harness never has to undo what an earlier case did.
//
// The contract exists because the Backend's tests stand in for the production
// bridge: a Backend that answers differently lets a test pass on a path that
// fails in production. The header of this package calls such drift a Backend
// bug; this suite is what makes the rule checkable instead of aspirational.
//
// Every case runs inside a testing/synctest bubble (see RunNetworkContract),
// and so does everything a harness does: newHarness, Register, ReadFrame and
// the cleanups they register are all called in it. That puts three
// requirements on a harness:
//
//   - no real sockets. A loopback TCP conn blocks in the netpoller, which is
//     not durably blocking, so synctest.Wait would never see the case's
//     goroutines parked and virtual time would never advance; use net.Pipe
//     or channels, which are.
//   - no channels, timers or WaitGroups created outside the bubble. Waiting
//     on one is not durably blocking either, and operating from outside on
//     anything the bubble created panics.
//   - every goroutine the implementation starts — a NetCore writer, a
//     session — must have exited when the case's cleanups return, because
//     synctest.Test waits for the bubble to empty and fails a case that
//     leaves a goroutine blocked behind.
type Harness struct {
	// Network is the implementation under test.
	Network netcore.Network

	// Register registers a live connection in direction dir the way the
	// implementation's owner does and returns its id together with the
	// address RemoteAddr must report for it.
	Register func(t *testing.T, dir netcore.Direction) (domain.ConnID, string)

	// ReadFrame waits for the next frame the remote end of id receives and
	// returns it, or returns ended=true once the connection has ended with
	// nothing left to read. It fails t when neither happens within its own
	// bound, so a missing frame is a failure and not a hung package run; the
	// bound runs in the bubble's virtual time.
	ReadFrame func(t *testing.T, id domain.ConnID) (frame []byte, ended bool)

	// Closed reports whether Close has released id. It must be exact at any
	// moment for an id the harness registered — a write failure, in
	// particular, is not a release.
	Closed func(id domain.ConnID) bool
}

// RunNetworkContract runs the cases every netcore.Network implementation must
// answer identically, each against a fresh Harness from newHarness. Each case
// runs in its own testing/synctest bubble, harness included; Harness lists
// what that requires of newHarness.
//
// Excluded on purpose — differences of the medium rather than of the
// contract, where encoding either side would make a rule the interface does
// not state:
//
//   - Frames still queued when Close runs. The bridge's writer discards them
//     unwritten; the Backend leaves them readable on Outbound(id), because
//     closing a Go channel keeps what is buffered (docs/protocol/network_core.md,
//     "Test backend"). No case here reads a frame sent before a Close.
//   - What a nil from SendFrameSync promises. On the bridge the bytes were
//     written to the socket; on the Backend the frame is in a channel. The
//     sync case reads concurrently with the send, so it holds on both, and
//     no case asserts that an unread remote keeps SendFrameSync waiting.
func RunNetworkContract(t *testing.T, newHarness func(t *testing.T) Harness) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, h Harness)
	}{
		{"unknown_id_is_answered_ErrUnknownConn", contractUnknownID},
		{"a_registered_conn_is_listed_under_its_direction_only", contractRegisteredConn},
		{"enumerate_stops_when_fn_returns_false", contractEnumerateStops},
		{"frames_reach_the_remote_in_send_order", contractSendOrder},
		{"send_frame_sync_delivers_the_frame", contractSendFrameSync},
		{"send_frame_on_a_saturated_queue_is_answered_ErrSendBufferFull", contractSendFrameSaturated},
		{"send_frame_sync_on_a_saturated_queue_is_answered_ErrSendBufferFull", contractSendFrameSyncSaturated},
		{"send_frame_blocking_waits_for_room_and_delivers_in_order", contractSendFrameBlockingWaitsForRoom},
		{"send_frame_blocking_is_not_overtaken_while_it_waits", contractSendFrameBlockingNotOvertaken},
		{"send_frame_blocking_on_a_queue_that_stays_full_ends_with_ctx", contractSendFrameBlockingEndsWithCtx},
		{"send_frame_blocking_parked_on_a_full_queue_is_released_by_close", contractSendFrameBlockingReleasedByClose},
		{"a_ctx_cancelled_on_entry_sends_nothing", contractCancelledOnEntry},
		{"close_releases_the_conn_and_keeps_its_registration", contractCloseKeepsRegistration},
		{"close_is_not_cancellable", contractCloseNotCancellable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Every case runs in a synctest bubble, harness included, so the
			// implementation's own goroutines belong to it: a case can wait
			// until they are parked (saturateQueue) and a bounded wait runs in
			// virtual time.
			synctest.Test(t, func(t *testing.T) {
				tc.run(t, newHarness(t))
			})
		})
	}
}

func contractUnknownID(t *testing.T, h Harness) {
	// One live registration, so "unknown" is answered by a lookup in a
	// non-empty registry rather than by an empty one.
	known, _ := h.Register(t, netcore.Inbound)
	unknown := known + unknownConnIDOffset
	ctx := context.Background()

	if err := h.Network.SendFrame(ctx, unknown, []byte("x\n")); !errors.Is(err, netcore.ErrUnknownConn) {
		t.Errorf("SendFrame(unknown) = %v, want ErrUnknownConn", err)
	}
	if err := h.Network.SendFrameSync(ctx, unknown, []byte("x\n")); !errors.Is(err, netcore.ErrUnknownConn) {
		t.Errorf("SendFrameSync(unknown) = %v, want ErrUnknownConn", err)
	}
	if err := h.Network.SendFrameBlocking(ctx, unknown, []byte("x\n")); !errors.Is(err, netcore.ErrUnknownConn) {
		t.Errorf("SendFrameBlocking(unknown) = %v, want ErrUnknownConn", err)
	}
	if err := h.Network.Close(ctx, unknown); !errors.Is(err, netcore.ErrUnknownConn) {
		t.Errorf("Close(unknown) = %v, want ErrUnknownConn", err)
	}
	if got := h.Network.RemoteAddr(unknown); got != "" {
		t.Errorf("RemoteAddr(unknown) = %q, want empty", got)
	}
	for _, dir := range contractDirections {
		if enumerated(ctx, h.Network, dir)[unknown] {
			t.Errorf("Enumerate(%v) listed an id that was never registered", dir)
		}
	}
}

func contractRegisteredConn(t *testing.T, h Harness) {
	ctx := context.Background()
	ids := make(map[netcore.Direction]domain.ConnID)
	addrs := make(map[netcore.Direction]string)
	for _, dir := range contractDirections {
		ids[dir], addrs[dir] = h.Register(t, dir)
	}

	for _, dir := range contractDirections {
		listed := enumerated(ctx, h.Network, dir)
		for _, registered := range contractDirections {
			if want := registered == dir; listed[ids[registered]] != want {
				t.Errorf("Enumerate(%v) lists the %v conn = %v, want %v", dir, registered, !want, want)
			}
		}
		if got := h.Network.RemoteAddr(ids[dir]); got != addrs[dir] {
			t.Errorf("RemoteAddr of the %v conn = %q, want %q", dir, got, addrs[dir])
		}
		if h.Closed(ids[dir]) {
			t.Errorf("a %v conn nobody closed reports Closed", dir)
		}
	}
}

func contractEnumerateStops(t *testing.T, h Harness) {
	h.Register(t, netcore.Inbound)
	h.Register(t, netcore.Inbound)

	calls := 0
	h.Network.Enumerate(context.Background(), netcore.Inbound, func(domain.ConnID) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("fn returning false was called %d times, want 1", calls)
	}
}

func contractSendOrder(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	ctx := context.Background()

	for i := 0; i < contractFrameCount; i++ {
		if err := h.Network.SendFrame(ctx, id, contractFrame(i)); err != nil {
			t.Fatalf("SendFrame #%d = %v", i, err)
		}
	}
	for i := 0; i < contractFrameCount; i++ {
		expectFrame(t, h, id, contractFrame(i))
	}
}

func contractSendFrameSync(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	want := []byte("sync\n")

	// The send runs off the test goroutine because on a real socket it
	// returns only once the remote has read the bytes, and the remote here is
	// this goroutine.
	sent := make(chan error, 1)
	go func() { sent <- h.Network.SendFrameSync(context.Background(), id, want) }()

	expectFrame(t, h, id, want)
	if err := <-sent; err != nil {
		t.Fatalf("SendFrameSync = %v, want nil", err)
	}
}

// contractSendFrameSaturated sends into a remote that never reads until the
// queue refuses. The refusal must be ErrSendBufferFull — the slow-peer signal
// callers evict on — and nothing else may come before it. The depth is not
// asserted: it is the implementation's, and only the answer is the contract's.
func contractSendFrameSaturated(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	ctx := context.Background()

	for i := 0; i < saturationAttemptLimit; i++ {
		err := h.Network.SendFrame(ctx, id, contractFrame(i))
		if err == nil {
			continue
		}
		if !errors.Is(err, netcore.ErrSendBufferFull) {
			t.Fatalf("SendFrame #%d into an unread conn = %v, want nil until ErrSendBufferFull", i, err)
		}
		return
	}
	t.Fatalf("%d frames into an unread conn and the queue never answered ErrSendBufferFull", saturationAttemptLimit)
}

// contractSendFrameSyncSaturated: the sync send has the same sentinel set as
// SendFrame, so a saturated queue refuses it at once with ErrSendBufferFull
// rather than parking the caller until room appears. The ctx bound only
// decides how an implementation that waits fails.
func contractSendFrameSyncSaturated(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	saturateQueue(t, h, id)

	ctx, cancel := context.WithTimeout(context.Background(), contractWaitBound)
	defer cancel()
	if err := h.Network.SendFrameSync(ctx, id, []byte("sync-into-full\n")); !errors.Is(err, netcore.ErrSendBufferFull) {
		t.Fatalf("SendFrameSync into a saturated queue = %v, want ErrSendBufferFull", err)
	}
}

// contractSendFrameBlockingWaitsForRoom: on a saturated queue the blocking
// send parks instead of answering ErrSendBufferFull, and once the remote reads
// — which is what frees a slot — it is queued behind everything sent before
// it. synctest.Wait returns only once the sender is parked, so an
// implementation that does not wait has already returned by then.
func contractSendFrameBlockingWaitsForRoom(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	queued := saturateQueue(t, h, id)

	blocked := []byte("blocked\n")
	sent := make(chan error, 1)
	go func() { sent <- h.Network.SendFrameBlocking(context.Background(), id, blocked) }()
	synctest.Wait()
	select {
	case err := <-sent:
		t.Fatalf("SendFrameBlocking into a saturated queue returned %v before any room appeared; want it to wait", err)
	default:
	}

	for _, frame := range queued {
		expectFrame(t, h, id, frame)
	}
	expectFrame(t, h, id, blocked)
	if err := <-sent; err != nil {
		t.Fatalf("SendFrameBlocking after room appeared = %v, want nil", err)
	}
}

// contractSendFrameBlockingNotOvertaken pins the ordering half of
// SendFrameBlocking's contract: a parked sender keeps its place. While it
// waits, a non-waiting send still finds the queue full; when the remote reads
// one frame, the slot that frees goes to the parked sender, so the queue is
// full again and a send right after the read is still refused; and a frame
// sent once the parked one is queued arrives after it. A queue that handed
// the freed slot to whoever asked first would let a later reply overtake the
// one that has been waiting.
func contractSendFrameBlockingNotOvertaken(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	queued := saturateQueue(t, h, id)
	ctx := context.Background()

	blocked := []byte("blocked\n")
	sent := make(chan error, 1)
	go func() { sent <- h.Network.SendFrameBlocking(ctx, id, blocked) }()
	synctest.Wait()

	if err := h.Network.SendFrame(ctx, id, []byte("overtaker-while-parked\n")); !errors.Is(err, netcore.ErrSendBufferFull) {
		t.Fatalf("SendFrame while a SendFrameBlocking is parked = %v, want ErrSendBufferFull", err)
	}

	expectFrame(t, h, id, queued[0])
	// Let the writer take the next frame and park on the remote again, and
	// the parked sender take the slot that freed, before asking.
	synctest.Wait()
	if err := h.Network.SendFrame(ctx, id, []byte("overtaker-after-read\n")); !errors.Is(err, netcore.ErrSendBufferFull) {
		t.Fatalf("SendFrame right after the remote read one frame = %v, want ErrSendBufferFull: "+
			"the freed slot belongs to the parked sender", err)
	}
	if err := <-sent; err != nil {
		t.Fatalf("SendFrameBlocking after room appeared = %v, want nil", err)
	}

	later := []byte("later\n")
	sentLater := make(chan error, 1)
	go func() { sentLater <- h.Network.SendFrameBlocking(ctx, id, later) }()
	for _, frame := range queued[1:] {
		expectFrame(t, h, id, frame)
	}
	expectFrame(t, h, id, blocked)
	expectFrame(t, h, id, later)
	if err := <-sentLater; err != nil {
		t.Fatalf("SendFrameBlocking of the later frame = %v, want nil", err)
	}
}

// contractSendFrameBlockingEndsWithCtx: a queue that never drains keeps the
// blocking send waiting until its ctx ends, never shorter, and the answer is
// ctx.Err() verbatim — not ErrSendBufferFull, which is what the non-waiting
// sends answer.
func contractSendFrameBlockingEndsWithCtx(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	saturateQueue(t, h, id)

	ctx, cancel := context.WithTimeout(context.Background(), contractWaitBound)
	defer cancel()
	start := time.Now()
	err := h.Network.SendFrameBlocking(ctx, id, []byte("never-room\n"))
	waited := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendFrameBlocking into a queue that never drains = %v, want context.DeadlineExceeded", err)
	}
	if waited != contractWaitBound {
		t.Fatalf("SendFrameBlocking gave up after %v, want its ctx deadline (%v)", waited, contractWaitBound)
	}
}

// contractSendFrameBlockingReleasedByClose: teardown is the other way out of
// the wait. A sender parked on a full queue when Close runs is answered
// ErrSendChanClosed — the connection is released — and is not left waiting
// for a slot that will never be its.
func contractSendFrameBlockingReleasedByClose(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	saturateQueue(t, h, id)

	sent := make(chan error, 1)
	go func() { sent <- h.Network.SendFrameBlocking(context.Background(), id, []byte("parked\n")) }()
	synctest.Wait()

	if err := h.Network.Close(context.Background(), id); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if err := <-sent; !errors.Is(err, netcore.ErrSendChanClosed) {
		t.Fatalf("SendFrameBlocking parked across Close = %v, want ErrSendChanClosed", err)
	}
}

func contractCancelledOnEntry(t *testing.T, h Harness) {
	id, _ := h.Register(t, netcore.Inbound)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if err := h.Network.SendFrame(cancelled, id, []byte("cancelled\n")); !errors.Is(err, context.Canceled) {
		t.Errorf("SendFrame(cancelled ctx) = %v, want context.Canceled", err)
	}
	if err := h.Network.SendFrameSync(cancelled, id, []byte("cancelled-sync\n")); !errors.Is(err, context.Canceled) {
		t.Errorf("SendFrameSync(cancelled ctx) = %v, want context.Canceled", err)
	}
	if err := h.Network.SendFrameBlocking(cancelled, id, []byte("cancelled-blocking\n")); !errors.Is(err, context.Canceled) {
		t.Errorf("SendFrameBlocking(cancelled ctx) = %v, want context.Canceled", err)
	}
	// The ctx is answered before the registry is: a caller whose request has
	// ended learns that, not a fact about a connection it no longer cares for.
	unknown := id + unknownConnIDOffset
	if err := h.Network.SendFrame(cancelled, unknown, []byte("x\n")); !errors.Is(err, context.Canceled) {
		t.Errorf("SendFrame(cancelled ctx, unknown id) = %v, want context.Canceled", err)
	}
	called := false
	h.Network.Enumerate(cancelled, netcore.Inbound, func(domain.ConnID) bool {
		called = true
		return true
	})
	if called {
		t.Error("Enumerate(cancelled ctx) started an iteration")
	}

	// The first frame the remote sees must be the marker: no refused frame
	// may have been queued.
	marker := []byte("marker\n")
	if err := h.Network.SendFrame(context.Background(), id, marker); err != nil {
		t.Fatalf("SendFrame(marker) = %v", err)
	}
	expectFrame(t, h, id, marker)
}

func contractCloseKeepsRegistration(t *testing.T, h Harness) {
	for _, dir := range contractDirections {
		id, addr := h.Register(t, dir)
		ctx := context.Background()

		if err := h.Network.Close(ctx, id); err != nil {
			t.Fatalf("Close(%v conn) = %v, want nil", dir, err)
		}
		expectReleased(t, h, id, dir, addr)
		if err := h.Network.Close(ctx, id); err != nil {
			t.Errorf("second Close(%v conn) = %v, want nil (Close is idempotent)", dir, err)
		}
		if err := h.Network.SendFrameSync(ctx, id, []byte("late-sync\n")); !errors.Is(err, netcore.ErrSendChanClosed) {
			t.Errorf("SendFrameSync after Close(%v conn) = %v, want ErrSendChanClosed", dir, err)
		}
		if err := h.Network.SendFrameBlocking(ctx, id, []byte("late-blocking\n")); !errors.Is(err, netcore.ErrSendChanClosed) {
			t.Errorf("SendFrameBlocking after Close(%v conn) = %v, want ErrSendChanClosed", dir, err)
		}
	}
}

func contractCloseNotCancellable(t *testing.T, h Harness) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, dir := range contractDirections {
		id, addr := h.Register(t, dir)
		if err := h.Network.Close(cancelled, id); err != nil {
			t.Fatalf("Close(cancelled ctx, %v conn) = %v, want nil", dir, err)
		}
		expectReleased(t, h, id, dir, addr)
	}
}

// expectReleased asserts every observable of a closed connection: the harness
// reports it Closed, the remote sees the stream end, and a send is refused
// with the teardown sentinel rather than ErrUnknownConn. And the registration
// is untouched: Close releases a connection, it does not unregister it.
// Removing the entry belongs to the owner's lifecycle (unregisterInboundConn,
// an outbound session's onClose, Unregister on the Backend).
func expectReleased(t *testing.T, h Harness, id domain.ConnID, dir netcore.Direction, addr string) {
	t.Helper()
	ctx := context.Background()
	if !h.Closed(id) {
		t.Errorf("Close returned but the %v conn is not released", dir)
	}
	if frame, ended := h.ReadFrame(t, id); !ended {
		t.Errorf("the remote of the %v conn read %q after Close, want the end of the stream", dir, frame)
	}
	if err := h.Network.SendFrame(ctx, id, []byte("late\n")); !errors.Is(err, netcore.ErrSendChanClosed) {
		t.Errorf("SendFrame after Close(%v conn) = %v, want ErrSendChanClosed", dir, err)
	}
	if !enumerated(ctx, h.Network, dir)[id] {
		t.Errorf("Enumerate(%v) stopped listing a closed conn that was never unregistered", dir)
	}
	if got := h.Network.RemoteAddr(id); got != addr {
		t.Errorf("RemoteAddr after Close(%v conn) = %q, want %q", dir, got, addr)
	}
}

// saturateQueue sends frames into a remote that never reads until the queue
// refuses one and STAYS full, and returns them in the order the remote will
// read them. A writer that takes the first frame out of the queue only after
// the first fill found it full would free a slot behind the case's back, so
// the queue is refilled once every goroutine in the bubble is parked — the
// writer then sits inside its write to the unread remote.
func saturateQueue(t *testing.T, h Harness, id domain.ConnID) [][]byte {
	t.Helper()
	var queued [][]byte
	fill := func() {
		for len(queued) < saturationAttemptLimit {
			frame := contractFrame(len(queued))
			err := h.Network.SendFrame(context.Background(), id, frame)
			if errors.Is(err, netcore.ErrSendBufferFull) {
				return
			}
			if err != nil {
				t.Fatalf("SendFrame #%d into an unread conn = %v, want nil until ErrSendBufferFull", len(queued), err)
			}
			queued = append(queued, frame)
		}
		t.Fatalf("%d frames into an unread conn and the queue never answered ErrSendBufferFull", saturationAttemptLimit)
	}
	fill()
	synctest.Wait()
	fill()
	return queued
}

func expectFrame(t *testing.T, h Harness, id domain.ConnID, want []byte) {
	t.Helper()
	got, ended := h.ReadFrame(t, id)
	if ended {
		t.Fatalf("the stream ended while waiting for %q", want)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("remote read %q, want %q", got, want)
	}
}

func enumerated(ctx context.Context, nw netcore.Network, dir netcore.Direction) map[domain.ConnID]bool {
	seen := make(map[domain.ConnID]bool)
	nw.Enumerate(ctx, dir, func(id domain.ConnID) bool {
		seen[id] = true
		return true
	})
	return seen
}

func contractFrame(i int) []byte {
	return []byte(fmt.Sprintf("frame-%d\n", i))
}
