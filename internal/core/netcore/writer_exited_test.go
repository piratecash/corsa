package netcore

import (
	"net"
	"testing"
	"time"
)

// TestWriterExitedClosesOnlyWhenCloseReleasesTheCore pins the difference that
// makes WriterExited the witness of a release and WriterDone not: after a
// write failure WriterDone is closed while the writer still owns the queue in
// drain-only mode, and only Close ends it.
func TestWriterExitedClosesOnlyWhenCloseReleasesTheCore(t *testing.T) {
	t.Parallel()
	near, far := net.Pipe()
	// The far end is gone, so the first write fails.
	_ = far.Close()
	pc := New(9, near, Inbound, Options{})
	t.Cleanup(pc.Close)

	// Either answer is correct: the writer may take the frame, fail the write
	// and shut the gate before SendRaw re-reads it after its offer, and then
	// SendWriterDone is the honest answer (docs/protocol/network_core.md). The
	// frame reached the queue either way, which is all this test needs.
	switch st := pc.SendRaw([]byte("doomed\n")); st {
	case SendOK, SendWriterDone:
	default:
		t.Fatalf("SendRaw = %v, want SendOK or SendWriterDone", st)
	}
	select {
	case <-pc.WriterDone():
	case <-time.After(2 * time.Second):
		t.Fatal("the writer never reported the write failure")
	}
	if isClosed(pc.WriterExited()) {
		t.Fatal("WriterExited closed on a write failure, before anything released the core")
	}

	pc.Close()
	if !isClosed(pc.WriterExited()) {
		t.Fatal("WriterExited still open after Close returned")
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
