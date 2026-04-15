package netcoretest_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
)

// TestBackend_SendFrameDeliversToOutbound verifies the happy path: a
// frame sent through the Network interface surfaces verbatim on the
// per-ConnID Outbound channel. This is the baseline of every test that
// uses the backend to observe Service-emitted frames.
func TestBackend_SendFrameDeliversToOutbound(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	id := domain.ConnID(1)
	b.Register(id, netcore.Outbound, "203.0.113.1:9999")

	frame := []byte(`{"type":"ping"}`)
	if err := b.Network().SendFrame(context.Background(), id, frame); err != nil {
		t.Fatalf("SendFrame: %v", err)
	}

	select {
	case got := <-b.Outbound(id):
		if !bytes.Equal(got, frame) {
			t.Fatalf("outbound frame mismatch: got %q want %q", got, frame)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for frame on Outbound")
	}
}

// TestBackend_SendFrame_UnknownConn pins the ErrUnknownConn sentinel.
// Production networkBridge returns this for unregistered ConnIDs, so the
// backend must mirror it exactly — errors.Is is the only correct
// discriminator in test assertions.
func TestBackend_SendFrame_UnknownConn(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	err := b.Network().SendFrame(context.Background(), domain.ConnID(42), []byte("x"))
	if !errors.Is(err, netcore.ErrUnknownConn) {
		t.Fatalf("expected ErrUnknownConn, got %v", err)
	}
}

// TestBackend_SendFrame_BufferFull pins the ErrSendBufferFull sentinel
// by saturating the per-ConnID outbound channel. Tests that want
// deterministic buffer-full behaviour set OutboundBuffer explicitly.
func TestBackend_SendFrame_BufferFull(t *testing.T) {
	b := netcoretest.NewWithOptions(netcoretest.Options{OutboundBuffer: 1})
	defer b.Shutdown()

	id := domain.ConnID(7)
	b.Register(id, netcore.Outbound, "198.51.100.2:8080")

	ctx := context.Background()
	if err := b.Network().SendFrame(ctx, id, []byte("first")); err != nil {
		t.Fatalf("first SendFrame: %v", err)
	}

	err := b.Network().SendFrame(ctx, id, []byte("second"))
	if !errors.Is(err, netcore.ErrSendBufferFull) {
		t.Fatalf("expected ErrSendBufferFull, got %v", err)
	}
}

// TestBackend_SendFrame_CtxCancelled pins ctx.Err() propagation on entry.
// Pre-cancelled contexts must bypass the Network entirely — matches the
// networkBridge contract.
func TestBackend_SendFrame_CtxCancelled(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	id := domain.ConnID(3)
	b.Register(id, netcore.Outbound, "192.0.2.9:443")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Network().SendFrame(ctx, id, []byte("x"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	select {
	case got, ok := <-b.Outbound(id):
		if ok {
			t.Fatalf("pre-cancelled SendFrame must not enqueue frame, got %q", got)
		}
	default:
		// expected: nothing enqueued.
	}
}

// TestBackend_SendFrameSync_Blocks asserts that SendFrameSync waits for a
// consumer rather than returning ErrSendBufferFull. This is the
// observable difference between the two send methods.
func TestBackend_SendFrameSync_Blocks(t *testing.T) {
	b := netcoretest.NewWithOptions(netcoretest.Options{OutboundBuffer: 1})
	defer b.Shutdown()

	id := domain.ConnID(11)
	b.Register(id, netcore.Outbound, "203.0.113.44:5050")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Prefill the buffer.
	if err := b.Network().SendFrame(ctx, id, []byte("first")); err != nil {
		t.Fatalf("prefill SendFrame: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- b.Network().SendFrameSync(ctx, id, []byte("second"))
	}()

	// Drain to unblock the sync sender.
	select {
	case <-b.Outbound(id):
	case <-time.After(time.Second):
		t.Fatal("timed out draining first frame")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SendFrameSync returned %v after drain", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SendFrameSync did not complete after drain")
	}
}

// TestBackend_Enumerate_DirectionFilter verifies that Enumerate yields
// exactly the connections matching the requested Direction and ignores
// the rest. Direction-keyed filtering is the Enumerate contract from
// netcore.Network; if it drifts, downstream Service peer-walks drift too.
func TestBackend_Enumerate_DirectionFilter(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	inA, inB := domain.ConnID(1), domain.ConnID(2)
	out := domain.ConnID(3)
	b.Register(inA, netcore.Inbound, "10.0.0.1:1")
	b.Register(inB, netcore.Inbound, "10.0.0.2:2")
	b.Register(out, netcore.Outbound, "10.0.0.3:3")

	seen := make(map[domain.ConnID]bool)
	b.Network().Enumerate(context.Background(), netcore.Inbound, func(id domain.ConnID) bool {
		seen[id] = true
		return true
	})

	if !seen[inA] || !seen[inB] {
		t.Fatalf("inbound walk missed: seen=%v", seen)
	}
	if seen[out] {
		t.Fatalf("inbound walk leaked outbound conn %d", out)
	}
}

// TestBackend_Close_PerConnID verifies that Close(ctx, id) removes the
// connection, closes its channels and returns ErrUnknownConn on a
// second call — matching the networkBridge contract. Distinct from
// TestBackend_Shutdown which tears down the whole backend.
func TestBackend_Close_PerConnID(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	id := domain.ConnID(99)
	b.Register(id, netcore.Inbound, "203.0.113.77:7777")

	// Capture the outbound channel reference before Close. Outbound's
	// documented contract returns nil for an unregistered id (and Close
	// removes the registration), so observing the closed-channel signal
	// requires holding the channel reference from the registered window.
	out := b.Outbound(id)
	if out == nil {
		t.Fatal("Outbound returned nil for registered id")
	}

	ctx := context.Background()
	if err := b.Network().Close(ctx, id); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, ok := <-out; ok {
		t.Fatal("Outbound channel must be closed after per-ConnID Close")
	}
	if err := b.Network().Close(ctx, id); !errors.Is(err, netcore.ErrUnknownConn) {
		t.Fatalf("expected ErrUnknownConn on second Close, got %v", err)
	}
}

// TestBackend_Shutdown_RejectsSends verifies the lifecycle contract:
// after Shutdown, every SendFrame / SendFrameSync returns
// ErrSendChanClosed rather than silently succeeding into a dead channel.
func TestBackend_Shutdown_RejectsSends(t *testing.T) {
	b := netcoretest.New()
	id := domain.ConnID(5)
	b.Register(id, netcore.Outbound, "203.0.113.5:5555")

	b.Shutdown()

	err := b.Network().SendFrame(context.Background(), id, []byte("late"))
	if !errors.Is(err, netcore.ErrSendChanClosed) {
		t.Fatalf("expected ErrSendChanClosed after Shutdown, got %v", err)
	}
}

// TestBackend_RemoteAddr verifies the address accessor: registered id
// returns the addr passed to Register; unknown id returns "" (not an
// error, matches networkBridge zero-value convention).
func TestBackend_RemoteAddr(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	id := domain.ConnID(77)
	want := "198.51.100.77:4242"
	b.Register(id, netcore.Outbound, want)

	if got := b.Network().RemoteAddr(id); got != want {
		t.Fatalf("RemoteAddr(id) = %q, want %q", got, want)
	}
	if got := b.Network().RemoteAddr(domain.ConnID(123456)); got != "" {
		t.Fatalf("RemoteAddr(unknown) = %q, want empty string", got)
	}
}

// TestBackend_Inject_UnknownConn pins the Inject sentinel. Inject is the
// inbound-side primitive: tests that use it rely on ErrUnknownConn being
// the single signal for "id was never registered" so they can distinguish
// programmer bugs from expected teardown races.
func TestBackend_Inject_UnknownConn(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	err := b.Inject(domain.ConnID(404), []byte("x"))
	if !errors.Is(err, netcore.ErrUnknownConn) {
		t.Fatalf("expected ErrUnknownConn from Inject, got %v", err)
	}
}

// TestBackend_Inject_DeliversOnInbound verifies the full inbound
// primitive: Inject'd frames appear on Inbound(id) in order. The
// Backend does not simulate a dispatch pipeline, so this is the bare
// channel contract.
func TestBackend_Inject_DeliversOnInbound(t *testing.T) {
	b := netcoretest.New()
	defer b.Shutdown()

	id := domain.ConnID(12)
	b.Register(id, netcore.Inbound, "10.1.2.3:9000")

	if err := b.Inject(id, []byte("one")); err != nil {
		t.Fatalf("Inject one: %v", err)
	}
	if err := b.Inject(id, []byte("two")); err != nil {
		t.Fatalf("Inject two: %v", err)
	}

	got := make([][]byte, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case f := <-b.Inbound(id):
			got = append(got, f)
		case <-time.After(time.Second):
			t.Fatalf("timed out after %d frames", i)
		}
	}
	if string(got[0]) != "one" || string(got[1]) != "two" {
		t.Fatalf("inbound ordering violated: %q %q", got[0], got[1])
	}
}
