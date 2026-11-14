package node

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

func TestLoadStandRouteWireFullSendMatchesOriginalWriter(t *testing.T) {
	for _, tc := range []string{"two full passes", "connect and periodic", "missing write", "different bytes"} {
		t.Run(tc, func(t *testing.T) {
			s := newTestServiceWithRouting(t, idNodeA)
			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			o := &LoadstandRouteWireObservation{window: window, fullBudget: 101, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			s.loadstandRouteWire.Store(o)
			const connID domain.ConnID = 1
			sessionID := [32]byte{1}
			o.begin(connID)
			o.bind(connID, idPeerB, domain.PeerDirectionOutbound, sessionID)
			addr := domain.PeerAddress("addr-peerB-v3")
			q := make(chan peerSendItem, 4)
			s.sessions[addr] = &peerSession{
				address: addr, connID: connID, peerIdentity: idPeerB, sendCh: q,
				capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
			}
			s.health = map[domain.PeerAddress]*peerHealth{addr: {Connected: true}}
			if err := seedLoadstandRouteWireTable(s.routingTable, idNodeA, 101); err != nil {
				t.Fatal(err)
			}
			projected := s.routingTable.AnnounceTo(idPeerB)
			defer s.routingTable.ReleaseAnnounceEntries(projected)
			entries := routing.BuildAnnounceSnapshot(projected).Entries
			epoch := s.routingTable.Epoch()
			now = 12
			for i := range 2 {
				if tc == "connect and periodic" && i == 1 {
					s.sendConnectTimeFullSync(t.Context(), idPeerB, addr)
				} else if !s.SendRouteAnnounceV3Full(t.Context(), addr, epoch, entries, domain.RouteWireFullPeriodic) {
					t.Fatal("complete full send was refused")
				}
			}
			now = 15
			if len(q) != 4 {
				t.Fatalf("expected two passes with two frames each, got %d frames", len(q))
			}
			for i := range 4 {
				line := (<-q).RawLine
				if i == 3 && tc == "missing write" {
					continue
				}
				if i == 3 && tc == "different bytes" {
					line = strings.Replace(line, `"seq_no":1`, `"seq_no":2`, 1)
				}
				data := o.prepareQueue(connID, []byte(line), 0)
				o.startWrite(connID, data)
				o.observe(connID, routeWireWrite, string(data), true, 0)
			}
			now = 21
			if err := o.captureFullReceipts(nil); err != nil {
				t.Fatal(err)
			}
			o.finish(connID, routeWireRead)
			o.finish(connID, routeWireWrite)
			rows, err := o.SealWindow([]RouteWireCensusEndpoint{{
				ConnID: connID, Peer: idPeerB, Direction: domain.PeerDirectionOutbound, SessionID: sessionID,
				Interval: domain.RouteWireSessionInterval{BeginBefore: 10, BeginAfter: 10, EndBefore: 20, EndAfter: 20},
			}})
			if tc == "missing write" || tc == "different bytes" {
				if !errors.Is(err, errRouteWireIncomplete) || rows != nil {
					t.Fatalf("writer discrepancy produced proof: %+v, %v", rows, err)
				}
				return
			}
			var connect uint64
			if tc == "connect and periodic" {
				connect = 1
			}
			if err != nil || len(rows) != 1 || rows[0].FullSend == nil || rows[0].FullSend.Passes != 2 ||
				rows[0].FullSend.ConnectPasses != connect || rows[0].FullSend.PeriodicPasses != 2-connect ||
				rows[0].FullSend.Destinations != 101 || rows[0].FullSend.Epoch != epoch || rows[0].Sent.FullV3.Frames != 4 {
				t.Fatalf("sender projection: %+v, %v", rows, err)
			}
		})
	}
}

func TestLoadStandRouteWireFullPassRefusesConnectionOrBoundaryChange(t *testing.T) {
	for _, tc := range []string{"other connection", "past window end"} {
		t.Run(tc, func(t *testing.T) {
			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			o := &LoadstandRouteWireObservation{window: window, fullBudget: 2, maximum: 2, connections: make(map[domain.ConnID]*routeWireConnection)}
			for _, id := range []domain.ConnID{1, 2} {
				o.begin(id)
				o.bind(id, idPeerB, domain.PeerDirectionOutbound, [32]byte{byte(id)})
			}
			entries := buildSmallEntries(2)
			now = 12
			pass := o.startFullPass(7, entries, domain.RouteWireFullTriggered)
			for i := range entries {
				frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 7, entries[i:i+1])
				if err != nil {
					t.Fatal(err)
				}
				id := domain.ConnID(1)
				if tc == "other connection" && i == 1 {
					id = 2
				}
				pass.frame(id, frame.RawLine)
			}
			if tc == "past window end" {
				now = 21
			}
			pass.finish(true)
			if !o.incomplete || o.fullPassesPending != 0 {
				t.Fatal("invalid pass did not invalidate observation")
			}
		})
	}
}
