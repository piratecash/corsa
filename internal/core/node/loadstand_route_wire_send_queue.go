package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// routeWireSendQueue mirrors the original sendCh slots under sendMu.
// A zero timestamp still occupies one slot so interleaved unselected
// frames cannot shift the origin of a measured frame.
type routeWireSendQueue struct {
	observer *LoadstandRouteWireObservation
	id       domain.ConnID
	first    []time.Duration
	head     int
}

func (q *routeWireSendQueue) beforeOffer(frame protocol.Frame) time.Duration {
	if q == nil {
		return 0
	}
	selected := false
	switch frame.Type {
	case protocol.RouteProbeFrameType, protocol.RouteProbeAckFrameType,
		protocol.RouteSyncDigestFrameType, protocol.RouteSyncSummaryFrameType, protocol.RouteAnnounceV3FrameType:
		selected = true
	}
	if frame.RawLine != "" {
		_, wireSelected, err := routeWireClass(frame.RawLine)
		if err != nil || wireSelected != selected {
			q.observer.fail()
		}
		selected = wireSelected
	}
	if !selected {
		return 0
	}
	o := q.observer
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[q.id]
	if o.reportSealed || conn == nil || conn.finished[routeWireWrite] || o.window.advance() != nil || o.window.last <= 0 {
		o.incomplete = true
		return 0
	}
	conn.upperPending++
	return o.window.last
}

func (q *routeWireSendQueue) push(first time.Duration, depth int) {
	if q != nil {
		q.first[(q.head+depth-1)%len(q.first)] = first
	}
}

func (q *routeWireSendQueue) pop() time.Duration {
	if q == nil {
		return 0
	}
	first := q.first[q.head]
	q.first[q.head] = 0
	q.head = (q.head + 1) % len(q.first)
	return first
}

// retire ends the upper queue's responsibility. A successful handoff has
// already given the writer the same lower bound, including if it has since
// completed. A discarded admitted frame must be entirely outside the window.
func (q *routeWireSendQueue) retire(first time.Duration, admitted, handedOff bool) {
	if q == nil || first == 0 {
		return
	}
	o := q.observer
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[q.id]
	if o.reportSealed || conn == nil || conn.upperPending == 0 {
		o.incomplete = true
		return
	}
	conn.upperPending--
	if admitted && !handedOff && (o.window.advance() != nil || !o.window.outside(first)) {
		o.incomplete = true
	}
}
