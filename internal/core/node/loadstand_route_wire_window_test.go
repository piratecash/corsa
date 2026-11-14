package node

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestLoadStandRouteWireWindowExcludesOutsideFrames(t *testing.T) {
	now := time.Duration(1)
	window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
	if err != nil {
		t.Fatal(err)
	}
	o := &LoadstandRouteWireObservation{window: window, maximum: 3, connections: make(map[domain.ConnID]*routeWireConnection)}
	interval := domain.RouteWireSessionInterval{BeginBefore: 10, BeginAfter: 10, EndBefore: 20, EndAfter: 20}
	active := RouteWireCensusEndpoint{ConnID: 1, Peer: idPeerB, Direction: domain.PeerDirectionInbound, SessionID: [32]byte{1}, Interval: interval}
	idle := RouteWireCensusEndpoint{ConnID: 2, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: [32]byte{2}, Interval: interval}
	outside := RouteWireCensusEndpoint{ConnID: 3, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: [32]byte{3}}
	for _, endpoint := range []RouteWireCensusEndpoint{active, idle, outside} {
		o.begin(endpoint.ConnID)
		o.bind(endpoint.ConnID, endpoint.Peer, endpoint.Direction, endpoint.SessionID)
	}
	const frame = "{\"type\":\"route_announce_v3\",\"kind\":\"full\"}\n"
	const before = "{\"type\":\"route_announce_v3\",\"kind\":\"full\",\"id\":\"before\"}\n"
	const after = "{\"type\":\"route_announce_v3\",\"kind\":\"full\",\"id\":\"after\"}\n"
	now = 5
	o.observe(active.ConnID, routeWireRead, before, true, now)
	o.startWrite(outside.ConnID, o.prepareQueue(outside.ConnID, []byte(before), 0))
	o.observe(outside.ConnID, routeWireWrite, before, true, 0)
	now = 13
	o.startWrite(active.ConnID, o.prepareQueue(active.ConnID, []byte(frame), 0))
	now = 15
	o.observe(active.ConnID, routeWireRead, frame, true, 14)
	o.observe(active.ConnID, routeWireWrite, frame, true, 0)
	now = 25
	o.observe(active.ConnID, routeWireRead, after, true, now)
	o.startWrite(outside.ConnID, o.prepareQueue(outside.ConnID, []byte(after), 0))
	o.observe(outside.ConnID, routeWireWrite, after, true, 0)
	for _, endpoint := range []RouteWireCensusEndpoint{active, idle, outside} {
		o.finish(endpoint.ConnID, routeWireRead)
		o.finish(endpoint.ConnID, routeWireWrite)
	}
	if rows, err := o.Seal(); !errors.Is(err, errRouteWireIncomplete) || rows != nil {
		t.Fatalf("accepted window through lifetime seal: %+v, %v", rows, err)
	}
	rows, err := o.SealWindow([]RouteWireCensusEndpoint{idle, active})
	digest := sha256.Sum256([]byte(frame))
	want := RouteWireCount{Frames: 1, Bytes: uint64(len(frame)), SHA256: hex.EncodeToString(digest[:])}
	if err != nil || len(rows) != 2 || rows[0].ConnID != active.ConnID || rows[0].SessionID != active.SessionID || rows[0].Interval != interval || rows[0].Received.FullV3 != want ||
		rows[0].ReceivedSpan != (domain.RouteWireFrameSpan{First: 14, Last: 15}) ||
		rows[0].Sent.FullV3 != want || rows[0].SentSpan != (domain.RouteWireFrameSpan{First: 13, Last: 15}) ||
		rows[1].ConnID != idle.ConnID || rows[1].Received != (RouteWireCounts{}) || rows[1].Sent != (RouteWireCounts{}) {
		t.Fatalf("unexpected window rows %+v, %v", rows, err)
	}
}

func TestLoadStandRouteWireWindowRefusals(t *testing.T) {
	const frame = "{\"type\":\"route_announce_v3\",\"kind\":\"full\"}\n"
	for _, scenario := range []struct {
		name                           string
		observedAt, sealedAt           time.Duration
		omitCensus, omitEnd, clockFail bool
		wrongSession                   bool
		lateRegistration               bool
		queueStartsAt                  time.Duration
		writeStartsAt                  time.Duration
		omitWriteStart                 bool
	}{
		{name: "opening boundary", observedAt: 10, sealedAt: 21},
		{name: "closing boundary", observedAt: 20, sealedAt: 21},
		{name: "clock regression", observedAt: 15, sealedAt: 14},
		{name: "clock unavailable", observedAt: 15, sealedAt: 21, clockFail: true},
		{name: "unconfirmed closing", observedAt: 15, sealedAt: 20},
		{name: "counted endpoint absent from census", observedAt: 15, sealedAt: 21, omitCensus: true},
		{name: "reader still active", observedAt: 15, sealedAt: 21, omitEnd: true},
		{name: "census session differs", observedAt: 15, sealedAt: 21, wrongSession: true},
		{name: "frame before proven registration", observedAt: 15, sealedAt: 21, lateRegistration: true},
		{name: "write crosses opening", writeStartsAt: 9, observedAt: 15, sealedAt: 21},
		{name: "write crosses closing", writeStartsAt: 19, observedAt: 21, sealedAt: 22},
		{name: "queue crosses opening with write inside", queueStartsAt: 9, writeStartsAt: 13, observedAt: 15, sealedAt: 21},
		{name: "write starts on opening", writeStartsAt: 10, observedAt: 15, sealedAt: 21},
		{name: "write completion lacks original start", observedAt: 15, sealedAt: 21, omitWriteStart: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) {
				if now > 1 && scenario.clockFail {
					return 0, errRouteWireIncomplete
				}
				return now, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			o := &LoadstandRouteWireObservation{window: window, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			endpoint := RouteWireCensusEndpoint{ConnID: 1, Peer: idPeerB, Direction: domain.PeerDirectionInbound, SessionID: [32]byte{1}, Interval: domain.RouteWireSessionInterval{BeginBefore: 10, BeginAfter: 10, EndBefore: 20, EndAfter: 20}}
			o.begin(endpoint.ConnID)
			o.bind(endpoint.ConnID, endpoint.Peer, endpoint.Direction, endpoint.SessionID)
			now = scenario.writeStartsAt
			if now == 0 {
				now = scenario.observedAt
			}
			if !scenario.omitWriteStart {
				writeAt := now
				if scenario.queueStartsAt != 0 {
					now = scenario.queueStartsAt
				}
				data := o.prepareQueue(endpoint.ConnID, []byte(frame), 0)
				now = writeAt
				o.startWrite(endpoint.ConnID, data)
			}
			now = scenario.observedAt
			o.observe(endpoint.ConnID, routeWireWrite, frame, true, 0)
			o.finish(endpoint.ConnID, routeWireWrite)
			if !scenario.omitEnd {
				o.finish(endpoint.ConnID, routeWireRead)
			}
			census := []RouteWireCensusEndpoint{}
			if !scenario.omitCensus {
				if scenario.wrongSession {
					endpoint.SessionID[0]++
				}
				if scenario.lateRegistration {
					endpoint.Interval.BeginAfter = 16
				}
				census = append(census, endpoint)
			}
			now = scenario.sealedAt
			if rows, err := o.SealWindow(census); !errors.Is(err, errRouteWireIncomplete) || rows != nil {
				t.Fatalf("accepted incomplete window: %+v, %v", rows, err)
			}
		})
	}
}

type routeWireReadFunc func([]byte) (int, error)

func (f routeWireReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestLoadStandRouteWireReaderFillIntervals(t *testing.T) {
	const route = "{\"type\":\"route_announce_v3\",\"kind\":\"full\"}\n"
	const unselected = "{\"type\":\"ping\"}\n"
	for _, scenario := range []struct {
		name       string
		firstAt    time.Duration
		nextAt     time.Duration
		firstChunk string
		second     string
		prelude    bool
		wantFirst  time.Duration
		incomplete bool
	}{
		{name: "one fill", firstAt: 11, nextAt: 14, firstChunk: route, wantFirst: 11},
		{name: "multiple fills", firstAt: 11, nextAt: 14, firstChunk: route[:12], second: route[12:], wantFirst: 11},
		{name: "partial before opening", firstAt: 9, nextAt: 14, firstChunk: route[:12], second: route[12:], wantFirst: 9, incomplete: true},
		{name: "partial before closing", firstAt: 19, nextAt: 21, firstChunk: route[:12], second: route[12:], wantFirst: 19, incomplete: true},
		{name: "buffered after another line", firstAt: 9, nextAt: 14, firstChunk: unselected + route, prelude: true, wantFirst: 9, incomplete: true},
		{name: "refill after consuming previous buffer", firstAt: 9, nextAt: 14, firstChunk: unselected, second: route, prelude: true, wantFirst: 14},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			const id domain.ConnID = 1
			o := &LoadstandRouteWireObservation{window: window, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			o.begin(id)
			reader := bufio.NewReader(strings.NewReader(""))
			chunks := []string{scenario.firstChunk, scenario.second}
			source := routeWireReadFunc(func(p []byte) (int, error) {
				if len(chunks) == 0 || chunks[0] == "" {
					return 0, io.EOF
				}
				n := copy(p, chunks[0])
				chunks = chunks[1:]
				now = scenario.nextAt
				return n, nil
			})
			tracker := o.trackPlaintextReader(reader, source)
			now = scenario.firstAt
			if scenario.prelude {
				tracker.beginFrame(reader)
				line, readErr := readFrameLine(reader, 4096)
				first, _ := tracker.endFrame()
				if readErr != nil || line != unselected {
					t.Fatalf("prelude = %q, %v", line, readErr)
				}
				o.observe(id, routeWireRead, line, true, first)
			}
			tracker.beginFrame(reader)
			line, readErr := readFrameLine(reader, 4096)
			first, hasBytes := tracker.endFrame()
			if readErr != nil || !hasBytes || line != route || first != scenario.wantFirst {
				t.Fatalf("line = %q, err = %v, first = %v, has bytes = %t", line, readErr, first, hasBytes)
			}
			o.observe(id, routeWireRead, line, true, first)
			if o.incomplete != scenario.incomplete {
				t.Fatalf("incomplete = %t, want %t", o.incomplete, scenario.incomplete)
			}
			if !scenario.incomplete && o.connections[id].spans[routeWireRead] != (domain.RouteWireFrameSpan{First: first, Last: scenario.nextAt}) {
				t.Fatalf("read span = %+v", o.connections[id].spans[routeWireRead])
			}
		})
	}
}

func TestLoadStandRouteWireReaderClosedWithPartialFrame(t *testing.T) {
	for _, payload := range []string{"", "{\"type\":"} {
		t.Run(payload, func(t *testing.T) {
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return 1, nil })
			if err != nil {
				t.Fatal(err)
			}
			o := &LoadstandRouteWireObservation{window: window}
			reader := bufio.NewReader(strings.NewReader(""))
			tracker := o.trackPlaintextReader(reader, routeWireReadFunc(func(p []byte) (int, error) {
				return copy(p, payload), net.ErrClosed
			}))
			tracker.beginFrame(reader)
			line, readErr := readFrameLine(reader, 4096)
			_, hasBytes := tracker.endFrame()
			if line != "" || !errors.Is(readErr, net.ErrClosed) {
				t.Fatalf("line = %q, error = %v", line, readErr)
			}
			o.readError(line, readErr, hasBytes)
			if o.incomplete != (payload != "") {
				t.Fatalf("incomplete = %t for partial payload %q", o.incomplete, payload)
			}
		})
	}
}

func TestLoadStandRouteWireQueueDisposition(t *testing.T) {
	const id domain.ConnID = 1
	const frame = "{\"type\":\"route_announce_v3\",\"kind\":\"full\"}\n"
	for _, scenario := range []struct {
		name       string
		dropAt     time.Duration
		admitted   bool
		incomplete bool
	}{
		{name: "refused", dropAt: 12},
		{name: "discarded before opening", dropAt: 9, admitted: true},
		{name: "discarded across opening", dropAt: 12, admitted: true, incomplete: true},
		{name: "disposition missing", incomplete: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			now := time.Duration(8)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			o := &LoadstandRouteWireObservation{window: window, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			o.begin(id)
			data := []byte(frame)
			first := o.prepareQueue(id, data, 0)
			second := o.prepareQueue(id, data, 0)
			if &first[0] == &second[0] || &first[0] == &data[0] || &second[0] == &data[0] {
				t.Fatal("queue offers did not get independent identities")
			}
			o.discardQueue(id, first, false)
			if scenario.dropAt != 0 {
				now = scenario.dropAt
				o.discardQueue(id, second, scenario.admitted)
			}
			o.finish(id, routeWireWrite)
			if o.incomplete != scenario.incomplete {
				t.Fatalf("incomplete = %t, want %t", o.incomplete, scenario.incomplete)
			}
		})
	}
}
