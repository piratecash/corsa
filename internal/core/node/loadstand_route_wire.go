package node

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

var errRouteWireIncomplete = errors.New("loadstand route wire observation incomplete")

const (
	routeWireProbe = iota
	routeWireAck
	routeWireDigest
	routeWireSummary
	routeWireFullV3
	routeWireDeltaV3
	routeWireClasses
)

const (
	routeWireRead = iota
	routeWireWrite
)

const maxRouteWireQueueOffers = 1024

type RouteWireCount struct {
	Frames, Bytes uint64
	SHA256        string
}

type RouteWireCounts struct {
	Probe, Ack, Digest, Summary, FullV3, DeltaV3 RouteWireCount
}

type RouteWireEndpoint struct {
	ConnID                 domain.ConnID
	Peer                   domain.PeerIdentity
	Direction              domain.PeerDirection
	SessionID              [32]byte
	Interval               domain.RouteWireSessionInterval
	Sent, Received         RouteWireCounts
	SentSpan, ReceivedSpan domain.RouteWireFrameSpan
	FullReceive            *domain.RouteWireFullReceipt
	FullSend               *domain.RouteWireFullSend
}

type routeWireCounter struct {
	frames, bytes uint64
	digest        hash.Hash
}

type routeWireConnection struct {
	peer         domain.PeerIdentity
	direction    domain.PeerDirection
	sessionID    [32]byte
	finished     [2]bool
	counts       [2][routeWireClasses]routeWireCounter
	spans        [2]domain.RouteWireFrameSpan
	writeStarted bool
	writeBegin   time.Duration
	writeClass   int
	queued       map[*byte]time.Duration
	upperPending int
	fullReceive  *routeWireFullReceive
	fullSend     *routeWireFullSend
}

// LoadstandRouteWireObservation measures authenticated connections during an
// explicitly configured FullNode run. It retains counters, digests and bounded
// references to selected frames whose queue disposition is still pending.
type LoadstandRouteWireObservation struct {
	mu                sync.Mutex
	maximum           int
	connections       map[domain.ConnID]*routeWireConnection
	incomplete        bool
	reportSealed      bool
	window            *routeWireFrameWindow
	fullBudget        int
	fullRows          int
	fullTaken         bool
	fullPassesPending int
}

var routeWireDisabled = &LoadstandRouteWireObservation{}

func (s *Service) EnableLoadstandRouteWire(maxEndpoints int) (*LoadstandRouteWireObservation, error) {
	return s.enableLoadstandRouteWire(maxEndpoints, nil, 0)
}

// EnableLoadstandRouteWireWindow selects a BOOTTIME interval for plaintext
// reader and writer callbacks. Cross-process clock attribution and matching
// the two ends of each stream remain controller responsibilities.
func (s *Service) EnableLoadstandRouteWireWindow(maxEndpoints int, begin, end time.Duration) (*LoadstandRouteWireObservation, error) {
	return s.EnableLoadstandRouteWireWindowWithFullReceipts(maxEndpoints, begin, end, 0)
}

// EnableLoadstandRouteWireWindowWithFullReceipts optionally bounds the
// number of distinct received (connection, destination) tuples to retain.
func (s *Service) EnableLoadstandRouteWireWindowWithFullReceipts(maxEndpoints int, begin, end time.Duration, maxFullRoutes int) (*LoadstandRouteWireObservation, error) {
	window, err := newRouteWireFrameWindow(begin, end, trustedclock.SinceBoot)
	if err != nil {
		return nil, err
	}
	return s.enableLoadstandRouteWire(maxEndpoints, window, maxFullRoutes)
}

func (s *Service) enableLoadstandRouteWire(maxEndpoints int, window *routeWireFrameWindow, maxFullRoutes int) (*LoadstandRouteWireObservation, error) {
	if maxEndpoints <= 0 || s.captureManager == nil || maxFullRoutes < 0 ||
		maxFullRoutes > 0 && (window == nil || s.routingTable == nil || (maxFullRoutes-1)/domain.RouteWireFullMaxDestinations >= maxEndpoints) {
		return nil, errRouteWireIncomplete
	}
	obs := &LoadstandRouteWireObservation{maximum: maxEndpoints, connections: make(map[domain.ConnID]*routeWireConnection), window: window, fullBudget: maxFullRoutes}
	if !s.loadstandRouteWire.CompareAndSwap(nil, obs) {
		return nil, errRouteWireIncomplete
	}
	return obs, nil
}

func (s *Service) routeWireObserver() *LoadstandRouteWireObservation {
	obs := s.loadstandRouteWire.Load()
	if obs == routeWireDisabled {
		return nil
	}
	return obs
}

func (o *LoadstandRouteWireObservation) begin(id domain.ConnID) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reportSealed || id == 0 || o.connections[id] != nil || len(o.connections) >= o.maximum {
		o.incomplete = true
		return
	}
	o.connections[id] = &routeWireConnection{}
}

func (o *LoadstandRouteWireObservation) bind(id domain.ConnID, peer domain.PeerIdentity, direction domain.PeerDirection, sessionID [32]byte) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	row := o.connections[id]
	if o.reportSealed || row == nil || peer.IsZero() || !row.peer.IsZero() || (o.window != nil && sessionID == ([32]byte{})) {
		o.incomplete = true
		return
	}
	row.peer, row.direction = peer, direction
	row.sessionID = sessionID
}

func (o *LoadstandRouteWireObservation) finish(id domain.ConnID, direction int) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	row := o.connections[id]
	if o.reportSealed || row == nil || row.finished[direction] || (direction == routeWireWrite && (row.writeStarted || len(row.queued) != 0)) {
		o.incomplete = true
		return
	}
	row.finished[direction] = true
}

// prepareQueue records a conservative lower bound before the offer. The copy
// lets two offers of the same bytes have independent lifetimes.
func (o *LoadstandRouteWireObservation) prepareQueue(id domain.ConnID, data []byte, first time.Duration) []byte {
	if o == nil || o.window == nil {
		return data
	}
	_, selected, err := routeWireClass(string(data))
	if err != nil {
		o.fail()
	}
	if !selected {
		if first != 0 {
			o.fail()
		}
		return data
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[id]
	if o.reportSealed || conn == nil || conn.finished[routeWireWrite] || len(conn.queued) >= maxRouteWireQueueOffers || o.window.advance() != nil ||
		first < 0 || first > o.window.last || first != 0 && conn.upperPending == 0 {
		o.incomplete = true
		return data
	}
	data = slices.Clone(data)
	if conn.queued == nil {
		conn.queued = make(map[*byte]time.Duration)
	}
	if first == 0 {
		first = o.window.last
	}
	conn.queued[&data[0]] = first
	return data
}

func (o *LoadstandRouteWireObservation) discardQueue(id domain.ConnID, data []byte, admitted bool) {
	if o == nil || o.window == nil {
		return
	}
	_, selected, err := routeWireClass(string(data))
	if err != nil {
		o.fail()
	}
	if !selected {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[id]
	if conn == nil {
		o.incomplete = true
		return
	}
	key := &data[0]
	first, found := conn.queued[key]
	delete(conn.queued, key)
	if !admitted {
		return
	}
	if o.reportSealed || conn.finished[routeWireWrite] || !found || o.window.advance() != nil {
		o.incomplete = true
		return
	}
	if !o.window.outside(first) {
		o.incomplete = true
	}
}

func (o *LoadstandRouteWireObservation) startWrite(id domain.ConnID, data []byte) {
	if o.window == nil {
		return
	}
	class, selected, err := routeWireClass(string(data))
	if err != nil {
		o.fail()
		return
	}
	if !selected {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[id]
	if o.reportSealed || conn == nil || conn.finished[routeWireWrite] || conn.writeStarted || o.window.advance() != nil {
		o.incomplete = true
		return
	}
	key := &data[0]
	first, found := conn.queued[key]
	if !found {
		o.incomplete = true
		return
	}
	delete(conn.queued, key)
	conn.writeStarted, conn.writeBegin, conn.writeClass = true, first, class
}

func (o *LoadstandRouteWireObservation) fail() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.incomplete = true
	o.mu.Unlock()
}

func (o *LoadstandRouteWireObservation) readError(line string, err error, hasBytes bool) {
	if o != nil && (hasBytes || line != "" || (!errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed))) {
		o.fail()
	}
}

func routeWireClass(line string) (int, bool, error) {
	typ, ok := topLevelFrameType(line)
	if !ok {
		return 0, false, errRouteWireIncomplete
	}
	switch typ {
	case protocol.RouteProbeFrameType:
		return routeWireProbe, true, nil
	case protocol.RouteProbeAckFrameType:
		return routeWireAck, true, nil
	case protocol.RouteSyncDigestFrameType:
		return routeWireDigest, true, nil
	case protocol.RouteSyncSummaryFrameType:
		return routeWireSummary, true, nil
	case protocol.RouteAnnounceV3FrameType:
		var frame struct{ Kind string }
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			return 0, false, errRouteWireIncomplete
		}
		switch frame.Kind {
		case protocol.RouteAnnounceV3KindFull:
			return routeWireFullV3, true, nil
		case protocol.RouteAnnounceV3KindDelta:
			return routeWireDeltaV3, true, nil
		default:
			return 0, false, errRouteWireIncomplete
		}
	default:
		return 0, false, nil
	}
}

func (o *LoadstandRouteWireObservation) observe(id domain.ConnID, direction int, line string, complete bool, first time.Duration) {
	if o == nil {
		return
	}
	if !complete {
		o.fail()
		return
	}
	class, selected, err := routeWireClass(line)
	o.mu.Lock()
	defer o.mu.Unlock()
	conn := o.connections[id]
	if o.reportSealed || conn == nil || conn.finished[direction] || err != nil {
		o.incomplete = true
		return
	}
	if !selected {
		return
	}
	if len(line) > protocol.MaxFrameLine || !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
		o.incomplete = true
		return
	}
	if o.window != nil {
		inside, err := o.window.includes()
		if err != nil {
			o.incomplete = true
			return
		}
		if direction == routeWireWrite {
			if !conn.writeStarted || conn.writeClass != class {
				o.incomplete = true
				return
			}
			first = conn.writeBegin
			conn.writeStarted = false
		}
		before := first < o.window.begin && o.window.last < o.window.begin
		after := first > o.window.windowEnd && o.window.last > o.window.windowEnd
		inWindow := inside && first > o.window.begin
		if first <= 0 || first > o.window.last || !before && !after && !inWindow {
			o.incomplete = true
			return
		}
		if !inside {
			return
		}
		if o.fullBudget > 0 && direction == routeWireRead && class == routeWireFullV3 && !o.recordFullReceive(conn, line) {
			o.incomplete = true
			return
		}
		span := &conn.spans[direction]
		if span.First == 0 || first < span.First {
			span.First = first
		}
		span.Last = o.window.last
	}
	row := &conn.counts[direction][class]
	if row.digest == nil {
		row.digest = sha256.New()
	}
	row.frames++
	row.bytes += uint64(len(line))
	_, _ = row.digest.Write([]byte(line))
}

func routeWireSnapshot(rows [routeWireClasses]routeWireCounter) RouteWireCounts {
	var out [routeWireClasses]RouteWireCount
	for i, row := range rows {
		out[i] = RouteWireCount{Frames: row.frames, Bytes: row.bytes}
		if row.digest != nil {
			out[i].SHA256 = hex.EncodeToString(row.digest.Sum(nil))
		}
	}
	return RouteWireCounts{Probe: out[routeWireProbe], Ack: out[routeWireAck], Digest: out[routeWireDigest], Summary: out[routeWireSummary], FullV3: out[routeWireFullV3], DeltaV3: out[routeWireDeltaV3]}
}

// Seal is called after the original Run and its background tasks have joined.
func (o *LoadstandRouteWireObservation) Seal() ([]RouteWireEndpoint, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reportSealed || o.incomplete || o.window != nil {
		return nil, errRouteWireIncomplete
	}
	o.reportSealed = true
	rows := make([]RouteWireEndpoint, 0, len(o.connections))
	for id, conn := range o.connections {
		if conn.peer.IsZero() || !conn.finished[routeWireRead] || !conn.finished[routeWireWrite] {
			return nil, errRouteWireIncomplete
		}
		rows = append(rows, RouteWireEndpoint{ConnID: id, Peer: conn.peer, Direction: conn.direction, Sent: routeWireSnapshot(conn.counts[routeWireWrite]), Received: routeWireSnapshot(conn.counts[routeWireRead])})
	}
	slices.SortFunc(rows, func(a, b RouteWireEndpoint) int { return cmp.Compare(a.ConnID, b.ConnID) })
	return rows, nil
}
