package capture

import (
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// defaultQueueSize is the per-session ring buffer capacity (plan §9).
const defaultQueueSize = 256

// sessionState describes the lifecycle of a single capture session.
type sessionState int

const (
	sessionPending  sessionState = iota // registered but writer not yet attached
	sessionActive                      // accepting events, writer running
	sessionDraining                    // no new events; writer draining queue
	sessionStopped                     // terminal: clean stop
	sessionFailed                      // terminal: disk error or other failure
)

// session owns the bounded queue and writer goroutine for a single
// conn_id capture. One session = one open file = one writer goroutine.
type session struct {
	connID    domain.ConnID
	remoteIP  netip.Addr
	peerDir   domain.PeerDirection
	format    domain.CaptureFormat
	scope     domain.CaptureScope
	filePath  string
	startedAt time.Time

	// mu protects ring, state, lastError. Network goroutine takes mu briefly
	// to push events; writer goroutine takes mu to pop them.
	mu    sync.Mutex
	ring  *ringBuffer[Event]
	state sessionState

	// droppedEvents is incremented atomically on ring overflow (plan §9).
	droppedEvents atomic.Int64

	lastError error

	// notify signals the writer goroutine that new events are available
	// or that the session should transition to drain/stop.
	notify chan struct{}

	// writerDone is closed when the writer goroutine exits.
	writerDone chan struct{}

	writer Writer

	// onFailed is called when the session enters sessionFailed state.
	// The manager uses this callback to self-evict dead sessions from
	// m.sessions so that subsequent start commands can re-create them.
	onFailed func(domain.ConnID)
}

type sessionOpts struct {
	connID    domain.ConnID
	remoteIP  netip.Addr
	peerDir   domain.PeerDirection
	format    domain.CaptureFormat
	scope     domain.CaptureScope
	filePath  string
	startedAt time.Time
	writer    Writer // nil → starts in sessionPending, attach later
	queueSize int

	// onFailed is called by the writer goroutine when the session enters
	// sessionFailed. The manager uses this to self-evict dead sessions.
	onFailed func(domain.ConnID)
}

func newSession(opts sessionOpts) *session {
	qsz := opts.queueSize
	if qsz <= 0 {
		qsz = defaultQueueSize
	}
	initialState := sessionActive
	if opts.writer == nil {
		initialState = sessionPending
	}
	return &session{
		connID:     opts.connID,
		remoteIP:   opts.remoteIP,
		peerDir:    opts.peerDir,
		format:     opts.format,
		scope:      opts.scope,
		filePath:   opts.filePath,
		startedAt:  opts.startedAt,
		ring:       newRingBuffer[Event](qsz),
		state:      initialState,
		notify:     make(chan struct{}, 1),
		writerDone: make(chan struct{}),
		writer:     opts.writer,
		onFailed:   opts.onFailed,
	}
}

// attachWriter transitions a pending session to active with the given writer
// and starts the writer goroutine. If the session was already stopped/failed
// (e.g. connection closed before file was ready), it closes w and returns false.
func (s *session) attachWriter(w Writer, ctxDone <-chan struct{}) bool {
	s.mu.Lock()
	if s.state != sessionPending {
		// Connection closed or stop arrived before file was ready.
		s.mu.Unlock()
		_ = w.Close()
		return false
	}
	s.writer = w
	s.state = sessionActive
	s.mu.Unlock()

	go s.runWriter(ctxDone)
	return true
}

// enqueue adds a capture event to the ring buffer. Non-blocking: if the
// ring is full the oldest event is dropped. Returns false if the session
// is no longer accepting events (draining, stopped, or failed).
func (s *session) enqueue(ev Event) bool {
	s.mu.Lock()
	if s.state != sessionActive && s.state != sessionPending {
		s.mu.Unlock()
		return false
	}
	if s.ring.push(ev) {
		s.droppedEvents.Add(1)
	}
	s.mu.Unlock()

	select {
	case s.notify <- struct{}{}:
	default:
	}
	return true
}

// initiateStop transitions to draining. Writer will drain and close the file.
// If the session is still pending (writer not yet attached), it goes directly
// to stopped so that attachWriter returns false and cleans up.
func (s *session) initiateStop() {
	s.mu.Lock()
	switch s.state {
	case sessionActive:
		s.state = sessionDraining
	case sessionPending:
		// Writer goroutine never started — go straight to terminal.
		s.state = sessionStopped
		s.mu.Unlock()
		close(s.writerDone)
		return
	}
	s.mu.Unlock()

	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// waitDone blocks until the writer goroutine has exited.
func (s *session) waitDone() {
	<-s.writerDone
}

// snapshot returns the current diagnostic state for PeerHealth / getActivePeers.
func (s *session) snapshot() SessionSnapshot {
	s.mu.Lock()
	state := s.state
	lastErr := s.lastError
	s.mu.Unlock()

	snap := SessionSnapshot{
		ConnID:        s.connID,
		RemoteIP:      s.remoteIP,
		PeerDirection: s.peerDir,
		Format:        s.format,
		Scope:         s.scope,
		FilePath:      s.filePath,
		StartedAt:     s.startedAt,
		DroppedEvents: s.droppedEvents.Load(),
		Recording:     state == sessionPending || state == sessionActive || state == sessionDraining,
	}
	if lastErr != nil {
		snap.Error = lastErr.Error()
	}
	return snap
}

// runWriter is the writer goroutine entry point. It drains the ring buffer
// and writes events to the file, respecting the drain contract (plan §9.1):
//   - ctxDone → stop accepting new events, drain remaining queue
//   - explicit stop → drain, close file
//   - disk error → mark session as failed, close file
func (s *session) runWriter(ctxDone <-chan struct{}) {
	defer close(s.writerDone)
	defer func() {
		if err := s.writer.Sync(); err != nil {
			s.setError(fmt.Errorf("capture sync: %w", err))
		}
		if err := s.writer.Close(); err != nil {
			s.setError(fmt.Errorf("capture close: %w", err))
		}
	}()

	eventIndex := 0

	for {
		select {
		case <-s.notify:
		case <-ctxDone:
			s.mu.Lock()
			if s.state == sessionActive {
				s.state = sessionDraining
			}
			s.mu.Unlock()
		}

		s.mu.Lock()
		events := s.ring.drain()
		currentState := s.state
		s.mu.Unlock()

		for _, ev := range events {
			if err := s.writeEvent(ev, eventIndex); err != nil {
				s.setError(fmt.Errorf("capture write: %w", err))
				s.mu.Lock()
				s.state = sessionFailed
				s.mu.Unlock()
				log.Warn().
					Uint64("conn_id", uint64(s.connID)).
					Str("file", s.filePath).
					Err(err).
					Msg("capture_writer: disk error, session failed")
				s.notifyFailed()
				return
			}
			eventIndex++
		}

		if len(events) > 0 {
			if err := s.writer.Sync(); err != nil {
				s.setError(fmt.Errorf("capture sync: %w", err))
				s.mu.Lock()
				s.state = sessionFailed
				s.mu.Unlock()
				s.notifyFailed()
				return
			}
		}

		if currentState == sessionDraining {
			s.mu.Lock()
			remaining := s.ring.drain()
			s.state = sessionStopped
			s.mu.Unlock()

			for _, ev := range remaining {
				if err := s.writeEvent(ev, eventIndex); err != nil {
					s.setError(fmt.Errorf("capture write (drain): %w", err))
					break
				}
				eventIndex++
			}
			return
		}

		if currentState != sessionActive {
			return
		}
	}
}

func (s *session) writeEvent(ev Event, eventIndex int) error {
	switch s.format {
	case domain.CaptureFormatPretty:
		return renderPrettyLine(s.writer, ev, eventIndex > 0)
	default:
		return renderCompactLine(s.writer, ev)
	}
}

func (s *session) setError(err error) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

// notifyFailed calls the onFailed callback so the manager can self-evict
// the dead session. Safe to call from the writer goroutine.
func (s *session) notifyFailed() {
	if s.onFailed != nil {
		s.onFailed(s.connID)
	}
}
