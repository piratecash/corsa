package node

import (
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// SessionInboxQueueSnapshot separates the depth at the current cut from the
// growth prefix, which ends when the transport reader exits. The consumer may
// still take an already buffered frame after that exit.
type SessionInboxQueueSnapshot struct {
	QueueDepthSnapshot
	ReaderDone bool
}

type sessionInboxObservation struct {
	mu sync.Mutex
	queueDepthEvidence
	ready       chan protocol.Frame
	readerDone  bool
	finalGrowth GossipQueueGrowth
}

func (s *Service) observeSessionInboxQueue(session *peerSession) {
	if !s.observeSessionInboxDepth {
		return
	}
	o := &sessionInboxObservation{ready: make(chan protocol.Frame, 1)}
	o.start()
	session.inboxObservation = o
}

func (o *sessionInboxObservation) signalLocked() {
	select {
	case o.ready <- protocol.Frame{}:
	default:
	}
}

func (ps *peerSession) offerInbox(frame protocol.Frame) bool {
	o := ps.inboxObservation
	if o == nil {
		select {
		case ps.inboxCh <- frame:
			return true
		default:
			return false
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readerDone || len(ps.inboxCh) == cap(ps.inboxCh) {
		return false
	}
	before, err := o.clock()
	select {
	case ps.inboxCh <- frame:
		o.recordLocked(&o.state.Enqueued)
		o.changeLocked(len(ps.inboxCh), before, err)
		o.signalLocked()
		return true
	default:
		return false
	}
}

func (ps *peerSession) inboxQueueInput() <-chan protocol.Frame {
	if o := ps.inboxObservation; o != nil {
		return o.ready
	}
	return ps.inboxCh
}

func (ps *peerSession) takeObservedInbox() (protocol.Frame, bool) {
	o := ps.inboxObservation
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(ps.inboxCh) == 0 {
		return protocol.Frame{}, false
	}
	before, err := o.clock()
	frame := <-ps.inboxCh
	depth := len(ps.inboxCh)
	o.recordLocked(&o.state.Taken)
	o.changeLocked(depth, before, err)
	if depth != 0 {
		o.signalLocked()
	}
	return frame, true
}

func (ps *peerSession) finishInboxReader() {
	o := ps.inboxObservation
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readerDone {
		o.invalidateLocked("inbox_reader_finished_twice")
		return
	}
	o.finalGrowth = o.snapshotLocked(len(ps.inboxCh)).Growth
	o.readerDone = true
}

func (ps *peerSession) inboxQueueObservation() SessionInboxQueueSnapshot {
	o := ps.inboxObservation
	if o == nil {
		return SessionInboxQueueSnapshot{QueueDepthSnapshot: QueueDepthSnapshot{UnknownReason: "source_unavailable"}}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	cut := o.snapshotLocked(len(ps.inboxCh))
	if o.readerDone {
		cut.Growth = o.finalGrowth
	}
	return SessionInboxQueueSnapshot{QueueDepthSnapshot: cut, ReaderDone: o.readerDone}
}

// SessionInboxQueue reads the exact registered ConnID. ReaderDone alone does
// not certify that the consumer has retired or that no unread residue remains.
func (o *PendingObservation) SessionInboxQueue(connID domain.ConnID) SessionInboxQueueSnapshot {
	if o == nil || o.service == nil || !o.service.observeSessionInboxDepth {
		return SessionInboxQueueSnapshot{QueueDepthSnapshot: QueueDepthSnapshot{UnknownReason: "source_unavailable"}}
	}
	session, reason := o.service.sessionForQueueObservation(connID)
	if reason != "" {
		return SessionInboxQueueSnapshot{QueueDepthSnapshot: QueueDepthSnapshot{UnknownReason: reason}}
	}
	return session.inboxQueueObservation()
}
