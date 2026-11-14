package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestLoadStandRouteWireUpperQueueHandoff(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first      time.Duration
		blocking   bool
		incomplete bool
	}{
		{name: "asynchronous wholly inside", first: 11},
		{name: "asynchronous crosses opening", first: 9, incomplete: true},
		{name: "blocking wholly inside", first: 11, blocking: true},
		{name: "blocking crosses opening", first: 9, blocking: true, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, id := pendingConstructionInputs(t)
			s := NewService(cfg, id, nil)
			t.Cleanup(s.WaitBackground)
			var clock atomic.Int64
			clock.Store(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return time.Duration(clock.Load()), nil })
			if err != nil {
				t.Fatal(err)
			}
			const connID domain.ConnID = 1
			o := &LoadstandRouteWireObservation{window: window, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			s.loadstandRouteWire.Store(o)
			o.begin(connID)
			sessionID := [32]byte{1}
			o.bind(connID, idPeerB, domain.PeerDirectionOutbound, sessionID)
			local, remote := net.Pipe()
			defer func() { _ = local.Close() }()
			core := newTestNetCore(t, connID, remote, netcore.Outbound, netcore.Options{LoadstandSessionID: sessionID})
			core.SetCaptureSink(&captureSinkAdapter{observer: o, connID: connID, manager: s.captureManager})
			session := &peerSession{
				address: "127.0.0.1:64646", connID: connID, conn: remote, netCore: core,
				proven: &sessionv2.Peer{}, sendCh: make(chan peerSendItem, 2),
				inboxCh: make(chan protocol.Frame, 1), errCh: make(chan error, 1),
			}
			s.observeSessionSendQueue(session)
			defer func() { _ = session.Close() }()
			frameType := protocol.RouteAnnounceV3FrameType
			route := "{\"type\":\"route_announce_v3\",\"kind\":\"full\"}\n"
			if tc.blocking {
				frameType = protocol.RouteProbeFrameType
				route = "{\"type\":\"route_probe_v1\"}\n"
			}
			clock.Store(int64(tc.first))
			for _, frame := range []protocol.Frame{{Type: "relay_hop_ack", RawLine: "{\"type\":\"relay_hop_ack\"}\n"}, {Type: frameType, RawLine: route}} {
				if !session.enqueueSend(legacyPeerSendItem(frame)) {
					t.Fatal("refused an upper offer")
				}
			}
			if session.enqueueSend(legacyPeerSendItem(protocol.Frame{Type: frameType, RawLine: route})) {
				t.Fatal("accepted overflow")
			}
			clock.Store(15)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.servePeerSession(ctx, session) }()
			if err := local.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(local)
			for _, want := range []string{"{\"type\":\"relay_hop_ack\"}\n", route} {
				line, err := reader.ReadString('\n')
				if err != nil || line != want {
					t.Fatalf("read %q, %v, want %q", line, err, want)
				}
			}
			if tc.blocking {
				session.inboxCh <- protocol.Frame{Type: "ok"}
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("session consumer did not stop")
			}
			// The writer can still be publishing completion when the peer reads
			// its last byte. Join it before asserting the report.
			core.Close()
			o.finish(connID, routeWireRead)
			o.finish(connID, routeWireWrite)
			clock.Store(21)
			rows, err := o.SealWindow([]RouteWireCensusEndpoint{{
				ConnID: connID, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: sessionID,
				Interval: domain.RouteWireSessionInterval{BeginBefore: 10, BeginAfter: 10, EndBefore: 20, EndAfter: 20},
			}})
			if tc.incomplete {
				if !errors.Is(err, errRouteWireIncomplete) || rows != nil {
					t.Fatalf("accepted upper-boundary crossing: %+v, %v", rows, err)
				}
				return
			}
			if err != nil || len(rows) != 1 || rows[0].SentSpan.First != tc.first || rows[0].SentSpan.Last != 15 {
				t.Fatalf("invalid session handoff span: %+v, %v", rows, err)
			}
			count := rows[0].Sent.FullV3
			if tc.blocking {
				count = rows[0].Sent.Probe
			}
			if count.Frames != 1 || count.Bytes != uint64(len(route)) {
				t.Fatalf("invalid session handoff count: %+v", count)
			}
		})
	}
}

func TestLoadStandRouteWireUpperQueueDrain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		drainAt    time.Duration
		incomplete bool
	}{
		{name: "before opening", drainAt: 9},
		{name: "across opening", drainAt: 11, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			const connID domain.ConnID = 1
			o := &LoadstandRouteWireObservation{window: window, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			o.begin(connID)
			s := &Service{}
			s.loadstandRouteWire.Store(o)
			session := &peerSession{connID: connID, proven: &sessionv2.Peer{}, sendCh: make(chan peerSendItem, 2)}
			s.observeSessionSendQueue(session)
			now = 8
			frame := legacyPeerSendItem(protocol.Frame{Type: protocol.RouteSyncDigestFrameType})
			for range 2 {
				if !session.enqueueSend(frame) {
					t.Fatal("upper offer refused")
				}
			}
			now = tc.drainAt
			session.discardSendQueue()
			if o.incomplete != tc.incomplete || o.connections[connID].upperPending != 0 || session.enqueueSend(frame) {
				t.Fatalf("invalid drain: incomplete=%t, pending=%d", o.incomplete, o.connections[connID].upperPending)
			}
		})
	}
}
