package node

import "github.com/piratecash/corsa/internal/core/domain"

// A token makes the existing session select ready. Only takeObservedSend
// touches the actual data channel on this path. The payload is never used.
type sessionSendObservation struct {
	queueDepthEvidence
	ready chan peerSendItem
}

func (s *Service) observeSessionSendQueue(session *peerSession) {
	if !s.observeSessionSendDepth {
		return
	}
	o := &sessionSendObservation{ready: make(chan peerSendItem, 1)}
	o.start()
	session.sendObservation = o
}

func (o *sessionSendObservation) signalLocked() {
	select {
	case o.ready <- peerSendItem{}:
	default:
	}
}

func (ps *peerSession) sendQueueInput() <-chan peerSendItem {
	if o := ps.sendObservation; o != nil {
		return o.ready
	}
	return ps.sendCh
}

func (ps *peerSession) takeObservedSend() (peerSendItem, bool) {
	ps.sendMu.Lock()
	defer ps.sendMu.Unlock()
	if ps.sendClosed || len(ps.sendCh) == 0 {
		return peerSendItem{}, false
	}
	o := ps.sendObservation
	before, err := o.clock()
	item := <-ps.sendCh
	depth := len(ps.sendCh)
	o.recordLocked(&o.state.Taken)
	o.changeLocked(depth, before, err)
	if depth != 0 {
		o.signalLocked()
	}
	return item, true
}

func (ps *peerSession) sendQueueObservation() QueueDepthSnapshot {
	ps.sendMu.Lock()
	defer ps.sendMu.Unlock()
	if o := ps.sendObservation; o != nil {
		return o.snapshotLocked(len(ps.sendCh))
	}
	return QueueDepthSnapshot{UnknownReason: "source_unavailable"}
}

// SessionSendQueue returns a cut for the exact registered outbound ConnID.
// The lookup does not certify complete coverage of a session's lifetime.
// Unpublished and already retired sessions do not appear in this readout.
func (o *PendingObservation) SessionSendQueue(connID domain.ConnID) QueueDepthSnapshot {
	if o == nil || o.service == nil || !o.service.observeSessionSendDepth {
		return QueueDepthSnapshot{UnknownReason: "source_unavailable"}
	}
	session, reason := o.service.sessionForQueueObservation(connID)
	if reason != "" {
		return QueueDepthSnapshot{UnknownReason: reason}
	}
	return session.sendQueueObservation()
}

func (s *Service) sessionForQueueObservation(connID domain.ConnID) (*peerSession, string) {
	if connID == 0 {
		return nil, "invalid_conn_id"
	}
	s.peerMu.RLock()
	var session *peerSession
	for _, candidate := range s.sessions {
		if candidate == nil || candidate.connID != connID {
			continue
		}
		if session != nil && session != candidate {
			s.peerMu.RUnlock()
			return nil, "session_identity_collision"
		}
		session = candidate
	}
	s.peerMu.RUnlock()
	if session == nil {
		return nil, "session_not_registered"
	}
	return session, ""
}
