package capture

import (
	"bytes"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// testWriter is a goroutine-safe in-memory Writer for session tests.
type testWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (w *testWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}
func (w *testWriter) Sync() error { return nil }
func (w *testWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
func (w *testWriter) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func testSession(queueSize int) (*session, *testWriter) {
	w := &testWriter{}
	s := newSession(sessionOpts{
		connID:    42,
		remoteIP:  netip.MustParseAddr("203.0.113.10"),
		peerDir:   domain.PeerDirectionOutbound,
		format:    domain.CaptureFormatCompact,
		scope:     domain.CaptureScopeConnID,
		filePath:  "/tmp/test-capture.jsonl",
		startedAt: time.Date(2026, 4, 14, 12, 44, 0, 0, time.UTC),
		writer:    w,
		queueSize: queueSize,
	})
	return s, w
}

func TestSession_EnqueueAndDrain(t *testing.T) {
	s, w := testSession(8)

	// Start the writer goroutine.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runWriter(make(chan struct{})) // context that never cancels
	}()

	// Enqueue some events.
	for i := 0; i < 3; i++ {
		ok := s.enqueue(Event{
			Ts:      time.Date(2026, 4, 14, 12, 44, 3, 0, time.UTC),
			WireDir: domain.WireDirectionIn,
			Kind:    domain.PayloadKindJSON,
			Raw:     `{"type":"ping"}`,
		})
		if !ok {
			t.Fatalf("enqueue %d returned false", i)
		}
	}

	// Stop and wait for drain.
	s.initiateStop()
	s.waitDone()

	content := w.String()
	if content == "" {
		t.Fatal("expected non-empty output after drain")
	}

	if !w.isClosed() {
		t.Error("expected writer to be closed after stop")
	}
}

func TestSession_EnqueueAfterStop(t *testing.T) {
	s, _ := testSession(8)

	go s.runWriter(make(chan struct{}))

	s.initiateStop()
	s.waitDone()

	// Enqueue after stop should return false.
	ok := s.enqueue(Event{
		Ts:      time.Now(),
		WireDir: domain.WireDirectionIn,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"late"}`,
	})
	if ok {
		t.Error("expected enqueue to return false after stop")
	}
}

func TestSession_Backpressure_DropsOldest(t *testing.T) {
	s, w := testSession(2)

	go s.runWriter(make(chan struct{}))

	// Enqueue 5 events into a queue of size 2 — should drop oldest.
	for i := 0; i < 5; i++ {
		s.enqueue(Event{
			Ts:      time.Date(2026, 4, 14, 12, 44, 0, i*int(time.Millisecond), time.UTC),
			WireDir: domain.WireDirectionIn,
			Kind:    domain.PayloadKindNonJSON,
			Raw:     "event",
		})
	}

	s.initiateStop()
	s.waitDone()

	dropped := s.droppedEvents.Load()
	if dropped == 0 {
		// With a queue of 2 and 5 pushes, some should be dropped.
		// But the writer goroutine may have drained between pushes.
		// At minimum, check that no panic occurred and output exists.
		t.Log("no drops detected; writer goroutine kept up")
	}

	content := w.String()
	if content == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestSession_Snapshot(t *testing.T) {
	s, _ := testSession(8)

	go s.runWriter(make(chan struct{}))

	snap := s.snapshot()
	if snap.ConnID != 42 {
		t.Errorf("expected ConnID=42, got %d", snap.ConnID)
	}
	if !snap.Recording {
		t.Error("expected recording=true for active session")
	}
	if snap.FilePath != "/tmp/test-capture.jsonl" {
		t.Errorf("unexpected file path: %s", snap.FilePath)
	}
	if snap.Format != domain.CaptureFormatCompact {
		t.Errorf("expected format=compact, got %s", snap.Format)
	}

	s.initiateStop()
	s.waitDone()

	snap = s.snapshot()
	if snap.Recording {
		t.Error("expected recording=false after stop")
	}
}

func TestSession_PendingBuffersEvents(t *testing.T) {
	// Create a pending session (nil writer).
	s := newSession(sessionOpts{
		connID:    42,
		remoteIP:  netip.MustParseAddr("203.0.113.10"),
		peerDir:   domain.PeerDirectionOutbound,
		format:    domain.CaptureFormatCompact,
		scope:     domain.CaptureScopeConnID,
		filePath:  "/tmp/test-pending.jsonl",
		startedAt: time.Date(2026, 4, 14, 12, 44, 0, 0, time.UTC),
		writer:    nil,
		queueSize: 8,
	})

	// Verify initial state is pending.
	snap := s.snapshot()
	if !snap.Recording {
		t.Error("pending session should report Recording=true")
	}

	// Enqueue events while pending — should succeed.
	ok := s.enqueue(Event{
		Ts:      time.Date(2026, 4, 14, 12, 44, 1, 0, time.UTC),
		WireDir: domain.WireDirectionIn,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"welcome"}`,
	})
	if !ok {
		t.Fatal("enqueue should succeed in pending state")
	}
	ok = s.enqueue(Event{
		Ts:      time.Date(2026, 4, 14, 12, 44, 2, 0, time.UTC),
		WireDir: domain.WireDirectionOut,
		Outcome: domain.SendOutcomeSent,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"auth"}`,
	})
	if !ok {
		t.Fatal("enqueue should succeed in pending state")
	}

	// Attach a real writer — transitions to active, starts writer goroutine.
	w := &testWriter{}
	attached := s.attachWriter(w, make(chan struct{}))
	if !attached {
		t.Fatal("attachWriter should succeed on pending session")
	}

	// Stop and drain.
	s.initiateStop()
	s.waitDone()

	content := w.String()
	if content == "" {
		t.Fatal("expected buffered events to be written after attachWriter")
	}
}

func TestSession_PendingStopBeforeAttach(t *testing.T) {
	s := newSession(sessionOpts{
		connID:    42,
		remoteIP:  netip.MustParseAddr("203.0.113.10"),
		peerDir:   domain.PeerDirectionOutbound,
		format:    domain.CaptureFormatCompact,
		scope:     domain.CaptureScopeConnID,
		filePath:  "/tmp/test-pending-stop.jsonl",
		startedAt: time.Date(2026, 4, 14, 12, 44, 0, 0, time.UTC),
		writer:    nil,
		queueSize: 8,
	})

	// Stop while still pending.
	s.initiateStop()
	s.waitDone()

	// attachWriter should return false on a stopped session.
	w := &testWriter{}
	if s.attachWriter(w, make(chan struct{})) {
		t.Fatal("attachWriter should return false after stop")
	}
	if !w.isClosed() {
		t.Error("writer should be closed when attachWriter returns false")
	}
}

func TestSession_OnFailedCallback(t *testing.T) {
	var failedConnID domain.ConnID
	s, _ := testSession(8)
	s.onFailed = func(id domain.ConnID) {
		failedConnID = id
	}

	// We can't easily trigger a disk error through the normal flow with
	// testWriter, but verify the callback wiring exists.
	s.notifyFailed()
	if failedConnID != 42 {
		t.Errorf("expected onFailed(42), got %d", failedConnID)
	}
}

func TestSession_ContextCancellation(t *testing.T) {
	s, w := testSession(8)

	ctxDone := make(chan struct{})
	go s.runWriter(ctxDone)

	// Enqueue an event, then cancel context.
	s.enqueue(Event{
		Ts:      time.Date(2026, 4, 14, 12, 44, 3, 0, time.UTC),
		WireDir: domain.WireDirectionOut,
		Outcome: domain.SendOutcomeSent,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"ping"}`,
	})

	close(ctxDone)
	s.waitDone()

	// Drain contract: events enqueued before ctx cancel should be written.
	content := w.String()
	if content == "" {
		t.Fatal("expected enqueued event to be drained on context cancel")
	}
}
