package node

import (
	"cmp"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Once installed, all reads and updates are serialized by the observer lock.
type routeWireFrameWindow struct {
	begin, windowEnd, last time.Duration
	clock                  func() (time.Duration, error)
}

func newRouteWireFrameWindow(begin, end time.Duration, clock func() (time.Duration, error)) (*routeWireFrameWindow, error) {
	if begin <= 0 || end <= begin || clock == nil {
		return nil, errRouteWireIncomplete
	}
	w := &routeWireFrameWindow{begin: begin, windowEnd: end, clock: clock}
	if err := w.advance(); err != nil || w.last >= begin {
		return nil, errRouteWireIncomplete
	}
	return w, nil
}

func (w *routeWireFrameWindow) advance() error {
	now, err := w.clock()
	if err != nil || now < w.last {
		return errRouteWireIncomplete
	}
	w.last = now
	return nil
}

func (w *routeWireFrameWindow) includes() (bool, error) {
	if err := w.advance(); err != nil || w.last == w.begin || w.last == w.windowEnd {
		return false, errRouteWireIncomplete
	}
	return w.last > w.begin && w.last < w.windowEnd, nil
}

func (w *routeWireFrameWindow) outside(first time.Duration) bool {
	return first < w.begin && w.last < w.begin || first > w.windowEnd && w.last > w.windowEnd
}

// SealWindow requires independent registry coverage, including connections
// that had no selected frames. An in-window count on a connection absent
// from the census invalidates the measurement.
func (o *LoadstandRouteWireObservation) SealWindow(census []RouteWireCensusEndpoint) ([]RouteWireEndpoint, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed || o.incomplete || o.window == nil || census == nil || o.fullPassesPending != 0 || o.fullBudget > 0 && !o.fullTaken {
		return nil, errRouteWireIncomplete
	}
	o.sealed = true
	if err := o.window.advance(); err != nil || o.window.last <= o.window.windowEnd || len(census) > len(o.connections) {
		return nil, errRouteWireIncomplete
	}
	rows := make([]RouteWireEndpoint, 0, len(census))
	covered := make(map[domain.ConnID]struct{}, len(census))
	for _, endpoint := range census {
		conn := o.connections[endpoint.ConnID]
		_, duplicate := covered[endpoint.ConnID]
		if duplicate || conn == nil || conn.peer.IsZero() || conn.peer != endpoint.Peer || conn.direction != endpoint.Direction ||
			endpoint.SessionID == ([32]byte{}) || conn.sessionID != endpoint.SessionID ||
			!endpoint.Interval.Within(o.window.begin, o.window.windowEnd) ||
			(conn.direction != domain.PeerDirectionInbound && conn.direction != domain.PeerDirectionOutbound) ||
			!conn.finished[routeWireRead] || !conn.finished[routeWireWrite] {
			return nil, errRouteWireIncomplete
		}
		covered[endpoint.ConnID] = struct{}{}
		for _, span := range conn.spans {
			if span != (domain.RouteWireFrameSpan{}) && !span.Within(endpoint.Interval.BeginAfter, endpoint.Interval.EndBefore) {
				return nil, errRouteWireIncomplete
			}
		}
		row := RouteWireEndpoint{ConnID: endpoint.ConnID, Peer: endpoint.Peer, Direction: endpoint.Direction, SessionID: conn.sessionID,
			Interval: endpoint.Interval,
			SentSpan: conn.spans[routeWireWrite], ReceivedSpan: conn.spans[routeWireRead],
			Sent: routeWireSnapshot(conn.counts[routeWireWrite]), Received: routeWireSnapshot(conn.counts[routeWireRead])}
		if conn.fullReceive != nil {
			row.FullReceive = conn.fullReceive.receipt
		}
		if o.fullBudget > 0 {
			var ok bool
			row.FullSend, ok = conn.fullSendProof(row.Sent.FullV3)
			if !ok {
				return nil, errRouteWireIncomplete
			}
		}
		rows = append(rows, row)
	}
	for id, conn := range o.connections {
		if conn.upperPending != 0 {
			return nil, errRouteWireIncomplete
		}
		if _, ok := covered[id]; ok {
			continue
		}
		for _, direction := range conn.counts {
			for _, counter := range direction {
				if counter.frames != 0 {
					return nil, errRouteWireIncomplete
				}
			}
		}
	}
	slices.SortFunc(rows, func(a, b RouteWireEndpoint) int { return cmp.Compare(a.ConnID, b.ConnID) })
	return rows, nil
}
