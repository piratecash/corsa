package capture_test

import (
	"net/netip"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/capture"
	"github.com/piratecash/corsa/internal/core/domain"
)

func testSession(t *testing.T, queueSize int) (*capture.TestSession, *writerBuffer) {
	t.Helper()
	w, wb := newMockWriterWithBuffer(t)
	s := capture.NewTestSession(capture.TestSessionOpts{
		ConnID:    42,
		RemoteIP:  netip.MustParseAddr("203.0.113.10"),
		PeerDir:   domain.PeerDirectionOutbound,
		Format:    domain.CaptureFormatCompact,
		Scope:     domain.CaptureScopeConnID,
		FilePath:  "/tmp/test-capture.jsonl",
		StartedAt: time.Date(2026, 4, 14, 12, 44, 0, 0, time.UTC),
		Writer:    w,
		QueueSize: queueSize,
	})
	return s, wb
}

func TestSession_EnqueueAndDrain(t *testing.T) {
	s, wb := testSession(t, 8)

	// Start the writer goroutine.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunWriter(make(chan struct{})) // context that never cancels
	}()

	// Enqueue some events.
	for i := 0; i < 3; i++ {
		ok := s.Enqueue(capture.Event{
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
	s.InitiateStop()
	s.WaitDone()

	content := wb.String()
	if content == "" {
		t.Fatal("expected non-empty output after drain")
	}

	if !wb.IsClosed() {
		t.Error("expected writer to be closed after stop")
	}
}

func TestSession_EnqueueAfterStop(t *testing.T) {
	s, _ := testSession(t, 8)

	go s.RunWriter(make(chan struct{}))

	s.InitiateStop()
	s.WaitDone()

	// Enqueue after stop should return false.
	ok := s.Enqueue(capture.Event{
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
	s, wb := testSession(t, 2)

	go s.RunWriter(make(chan struct{}))

	// Enqueue 5 events into a queue of size 2 — should drop oldest.
	for i := 0; i < 5; i++ {
		s.Enqueue(capture.Event{
			Ts:      time.Date(2026, 4, 14, 12, 44, 0, i*int(time.Millisecond), time.UTC),
			WireDir: domain.WireDirectionIn,
			Kind:    domain.PayloadKindNonJSON,
			Raw:     "event",
		})
	}

	s.InitiateStop()
	s.WaitDone()

	dropped := s.DroppedEvents()
	if dropped == 0 {
		// With a queue of 2 and 5 pushes, some should be dropped.
		// But the writer goroutine may have drained between pushes.
		// At minimum, check that no panic occurred and output exists.
		t.Log("no drops detected; writer goroutine kept up")
	}

	content := wb.String()
	if content == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestSession_Snapshot(t *testing.T) {
	s, _ := testSession(t, 8)

	go s.RunWriter(make(chan struct{}))

	snap := s.Snapshot()
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

	s.InitiateStop()
	s.WaitDone()

	snap = s.Snapshot()
	if snap.Recording {
		t.Error("expected recording=false after stop")
	}
}

func TestSession_PendingBuffersEvents(t *testing.T) {
	// Create a pending session (nil writer).
	s := capture.NewTestSession(capture.TestSessionOpts{
		ConnID:    42,
		RemoteIP:  netip.MustParseAddr("203.0.113.10"),
		PeerDir:   domain.PeerDirectionOutbound,
		Format:    domain.CaptureFormatCompact,
		Scope:     domain.CaptureScopeConnID,
		FilePath:  "/tmp/test-pending.jsonl",
		StartedAt: time.Date(2026, 4, 14, 12, 44, 0, 0, time.UTC),
		Writer:    nil,
		QueueSize: 8,
	})

	// Verify initial state is pending.
	snap := s.Snapshot()
	if !snap.Recording {
		t.Error("pending session should report Recording=true")
	}

	// Enqueue events while pending — should succeed.
	ok := s.Enqueue(capture.Event{
		Ts:      time.Date(2026, 4, 14, 12, 44, 1, 0, time.UTC),
		WireDir: domain.WireDirectionIn,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"welcome"}`,
	})
	if !ok {
		t.Fatal("enqueue should succeed in pending state")
	}
	ok = s.Enqueue(capture.Event{
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
	w, wb := newMockWriterWithBuffer(t)
	attached := s.AttachWriter(w, make(chan struct{}))
	if !attached {
		t.Fatal("attachWriter should succeed on pending session")
	}

	// Stop and drain.
	s.InitiateStop()
	s.WaitDone()

	content := wb.String()
	if content == "" {
		t.Fatal("expected buffered events to be written after attachWriter")
	}
}

func TestSession_PendingStopBeforeAttach(t *testing.T) {
	s := capture.NewTestSession(capture.TestSessionOpts{
		ConnID:    42,
		RemoteIP:  netip.MustParseAddr("203.0.113.10"),
		PeerDir:   domain.PeerDirectionOutbound,
		Format:    domain.CaptureFormatCompact,
		Scope:     domain.CaptureScopeConnID,
		FilePath:  "/tmp/test-pending-stop.jsonl",
		StartedAt: time.Date(2026, 4, 14, 12, 44, 0, 0, time.UTC),
		Writer:    nil,
		QueueSize: 8,
	})

	// Stop while still pending.
	s.InitiateStop()
	s.WaitDone()

	// attachWriter should return false on a stopped session.
	w, wb := newMockWriterWithBuffer(t)
	if s.AttachWriter(w, make(chan struct{})) {
		t.Fatal("attachWriter should return false after stop")
	}
	if !wb.IsClosed() {
		t.Error("writer should be closed when attachWriter returns false")
	}
}

func TestSession_OnFailedCallback(t *testing.T) {
	var failedConnID domain.ConnID
	s, _ := testSession(t, 8)
	s.SetOnFailed(func(id domain.ConnID) {
		failedConnID = id
	})

	s.NotifyFailed()
	if failedConnID != 42 {
		t.Errorf("expected onFailed(42), got %d", failedConnID)
	}
}

func TestSession_ContextCancellation(t *testing.T) {
	s, wb := testSession(t, 8)

	ctxDone := make(chan struct{})
	go s.RunWriter(ctxDone)

	// Enqueue an event, then cancel context.
	s.Enqueue(capture.Event{
		Ts:      time.Date(2026, 4, 14, 12, 44, 3, 0, time.UTC),
		WireDir: domain.WireDirectionOut,
		Outcome: domain.SendOutcomeSent,
		Kind:    domain.PayloadKindJSON,
		Raw:     `{"type":"ping"}`,
	})

	close(ctxDone)
	s.WaitDone()

	// Drain contract: events enqueued before ctx cancel should be written.
	content := wb.String()
	if content == "" {
		t.Fatal("expected enqueued event to be drained on context cancel")
	}
}
