package capture

import (
	"net/netip"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ForceSessionFailed sets a session to the failed state for testing eviction.
func (m *Manager) ForceSessionFailed(connID domain.ConnID) {
	m.mu.Lock()
	s := m.sessions[connID]
	m.mu.Unlock()
	if s != nil {
		s.mu.Lock()
		s.state = sessionFailed
		s.mu.Unlock()
	}
}

// EvictFailedSessionForTest wraps the unexported evictFailedSession.
func (m *Manager) EvictFailedSessionForTest(connID domain.ConnID) {
	m.evictFailedSession(connID)
}

// ---------------------------------------------------------------------------
// TestSession — export bridge for the unexported *session type
// ---------------------------------------------------------------------------

// TestSessionOpts mirrors sessionOpts with exported fields for package
// capture_test.
type TestSessionOpts struct {
	ConnID    domain.ConnID
	RemoteIP  netip.Addr
	PeerDir   domain.PeerDirection
	Format    domain.CaptureFormat
	Scope     domain.CaptureScope
	FilePath  string
	StartedAt time.Time
	Writer    Writer
	QueueSize int
}

// TestSession wraps the unexported *session so that package capture_test
// can exercise session-level logic without duplicating internal types.
type TestSession struct{ s *session }

// NewTestSession creates a TestSession from exported opts.
func NewTestSession(opts TestSessionOpts) *TestSession {
	return &TestSession{s: newSession(sessionOpts{
		connID:    opts.ConnID,
		remoteIP:  opts.RemoteIP,
		peerDir:   opts.PeerDir,
		format:    opts.Format,
		scope:     opts.Scope,
		filePath:  opts.FilePath,
		startedAt: opts.StartedAt,
		writer:    opts.Writer,
		queueSize: opts.QueueSize,
	})}
}

func (ts *TestSession) RunWriter(ctxDone <-chan struct{}) { ts.s.runWriter(ctxDone) }
func (ts *TestSession) Enqueue(ev Event) bool             { return ts.s.enqueue(ev) }
func (ts *TestSession) InitiateStop()                     { ts.s.initiateStop() }
func (ts *TestSession) WaitDone()                         { ts.s.waitDone() }
func (ts *TestSession) DroppedEvents() int64              { return ts.s.droppedEvents.Load() }
func (ts *TestSession) Snapshot() SessionSnapshot         { return ts.s.snapshot() }
func (ts *TestSession) AttachWriter(w Writer, ctxDone <-chan struct{}) bool {
	return ts.s.attachWriter(w, ctxDone)
}
func (ts *TestSession) NotifyFailed()                      { ts.s.notifyFailed() }
func (ts *TestSession) SetOnFailed(fn func(domain.ConnID)) { ts.s.onFailed = fn }
