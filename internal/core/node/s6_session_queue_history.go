package node

import (
	"crypto/rand"
	"sort"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
)

// MaxSessionQueueHistory bounds the outbound peerSession population retained
// from construction. Exhaustion invalidates coverage for this source.
const MaxSessionQueueHistory = 256

type SessionQueueHistoryRow struct {
	ConnID        domain.ConnID
	ReaderStarted bool
	Closed        bool
	Retired       bool
	Send          QueueDepthSnapshot
	Inbox         SessionInboxQueueSnapshot
}

// SessionQueueHistorySnapshot covers one stable population. Queue cuts are
// independent. Pending counts nonretired sessions, including unpublished ones.
type SessionQueueHistorySnapshot struct {
	Instance      [16]byte
	Born, Pending int
	Rows          []SessionQueueHistoryRow
	UnknownReason string
}

// Only live entries hold session pointers. Retirement replaces the pointer
// with a value snapshot. The mutex also protects each attached tracker.
// Taking a cut nests queue locks inside mu;
// queue operations release those locks before calling a lifecycle method.
type sessionQueueHistory struct {
	mu       sync.Mutex
	instance [16]byte
	capacity int
	sessions map[domain.ConnID]*peerSession
	rows     []SessionQueueHistoryRow
	reason   string
}

type sessionQueueLifetime struct {
	history       *sessionQueueHistory
	users         int
	readerStarted bool
	closed        bool
}

func newSessionQueueHistory(capacity int) *sessionQueueHistory {
	h := &sessionQueueHistory{capacity: capacity, sessions: make(map[domain.ConnID]*peerSession)}
	if _, err := rand.Read(h.instance[:]); err != nil || h.instance == ([16]byte{}) {
		h.reason = "source_identity_unavailable"
	}
	return h
}

func (h *sessionQueueHistory) failLocked(reason string) {
	h.reason = reason
	// Keep the count but release every live reference on terminal source failure.
	for id := range h.sessions {
		h.sessions[id] = nil
	}
	h.rows = nil
}

func (s *Service) observeSessionQueues(ps *peerSession) {
	s.observeSessionSendQueue(ps)
	s.observeSessionInboxQueue(ps)
	h := s.sessionQueueHistory
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reason != "" {
		return
	}
	if _, duplicate := h.sessions[ps.connID]; duplicate || ps.connID == 0 {
		h.failLocked("session_identity_collision")
		return
	}
	if len(h.sessions) == h.capacity {
		h.failLocked("history_capacity_exhausted")
		return
	}
	ps.queueLifetime = &sessionQueueLifetime{history: h}
	h.sessions[ps.connID] = ps
}

// Reserve before starting a reader or handing the inbox consumer to a new
// goroutine. Close may run while either is still using the queues.
func (ps *peerSession) beginSessionQueueUse(reader bool) {
	l := ps.queueLifetime
	if l == nil {
		return
	}
	h := l.history
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reason != "" {
		return
	}
	if l.closed || (reader && l.readerStarted) {
		h.failLocked("unexpected_queue_owner")
		return
	}
	l.readerStarted = l.readerStarted || reader
	l.users++
}

func (ps *peerSession) endSessionQueueUse() {
	l := ps.queueLifetime
	if l == nil {
		return
	}
	h := l.history
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reason != "" {
		return
	}
	if l.users == 0 {
		h.failLocked("unexpected_queue_owner_exit")
		return
	}
	l.users--
	ps.retainSessionQueuesLocked(l)
}

func (ps *peerSession) closeSessionQueueLifetime() {
	l := ps.queueLifetime
	if l == nil {
		return
	}
	h := l.history
	h.mu.Lock()
	defer h.mu.Unlock()
	l.closed = true
	ps.retainSessionQueuesLocked(l)
}

func (ps *peerSession) retainSessionQueuesLocked(l *sessionQueueLifetime) {
	h := l.history
	if h.reason != "" || !l.closed || l.users != 0 {
		return
	}
	row := ps.sessionQueueRowLocked()
	if !row.valid() || !row.Send.Stopped || row.Inbox.ReaderDone != row.ReaderStarted ||
		h.sessions[ps.connID] != ps {
		h.failLocked("session_final_cut_unavailable")
		return
	}
	row.Retired = true
	h.sessions[ps.connID] = nil
	h.rows = append(h.rows, row)
}

func (ps *peerSession) sessionQueueRowLocked() SessionQueueHistoryRow {
	l := ps.queueLifetime
	return SessionQueueHistoryRow{
		ConnID: ps.connID, ReaderStarted: l.readerStarted, Closed: l.closed,
		Send: ps.sendQueueObservation(), Inbox: ps.inboxQueueObservation(),
	}
}

func (r SessionQueueHistoryRow) valid() bool {
	return r.Send.UnknownReason == "" && r.Inbox.UnknownReason == "" &&
		(r.ReaderStarted || (!r.Inbox.ReaderDone && r.Inbox.Enqueued == 0))
}

func (o *PendingObservation) HasSessionQueueHistory() bool {
	return o != nil && o.service != nil && o.service.sessionQueueHistory != nil
}

// SessionQueueHistory covers outbound peerSessions born with both sources
// installed, including those that failed before registration.
func (o *PendingObservation) SessionQueueHistory() SessionQueueHistorySnapshot {
	if !o.HasSessionQueueHistory() {
		return SessionQueueHistorySnapshot{UnknownReason: "source_unavailable"}
	}
	h := o.service.sessionQueueHistory
	h.mu.Lock()
	defer h.mu.Unlock()
	out := SessionQueueHistorySnapshot{Instance: h.instance, Born: len(h.sessions), UnknownReason: h.reason}
	if h.reason != "" {
		return out
	}
	rows := append(make([]SessionQueueHistoryRow, 0, out.Born), h.rows...)
	for _, session := range h.sessions {
		if session == nil {
			continue
		}
		row := session.sessionQueueRowLocked()
		if !row.valid() {
			h.failLocked("session_live_cut_unavailable")
			out.UnknownReason = h.reason
			return out
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ConnID < rows[j].ConnID })
	out.Pending = out.Born - len(h.rows)
	out.Rows = rows
	return out
}
