package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// A deterministic reachable mechanism, not a replay of either saved native run:
// an initial full sync drains the real identity-keyed limiter, a real table
// withdrawal makes old rows unusable, and a valid newer full projection arrives
// before/after refill. No socket, Service.Run, sleep, limiter reset or oracle
// relaxation is used. Only the replacement pass is inside the observer window.
func TestLoadStandRouteWireFullReceiptRefillContrast(t *testing.T) {
	const count = 10_000
	for _, tc := range []struct {
		name        string
		after       time.Duration
		accepted    int
		wantReceipt bool
	}{
		{"sixteen_seconds", 16 * time.Second, 3200, false},
		{"fifty_seconds", 50 * time.Second, count, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender, receiver := idPeerB, domain.PeerIdentity{0xcc, 1}
			senderTable := routing.NewTable(routing.WithLocalOrigin(sender))
			if err := seedLoadstandRouteWireTable(senderTable, sender, count); err != nil {
				t.Fatal(err)
			}
			if _, err := senderTable.AddDirectPeer(receiver); err != nil {
				t.Fatal(err)
			}
			raw, _ := senderTable.AnnounceToWithChangeHead(receiver)
			defer senderTable.ReleaseAnnounceEntries(raw)
			entries := routing.BuildAnnounceSnapshot(raw).Entries
			if len(entries) != count {
				t.Fatalf("initial projection=%d", len(entries))
			}
			epoch := senderTable.Epoch()

			service := newTestServiceWithRouting(t, receiver)
			service.eventBus = newStormBus(t)
			wall := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
			limiter := newAnnounceRateLimiter(func() time.Time { return wall })
			service.announceLimiter = limiter
			caps := []routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1}
			service.announceLoop.StateRegistry().MarkReconnected(sender, caps)
			receive := service.provenRoutingSender(sender)
			subject := receive.penalty
			if _, err := service.routingTable.AddDirectPeer(sender); err != nil {
				t.Fatal(err)
			}

			// The initial actual handler pass is wholly outside the observed window.
			chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, protocol.RouteAnnounceV3KindFull, epoch, protocol.MaxFrameLine)
			if len(chunks) != 100 || len(skipped) != 0 {
				t.Fatalf("initial chunks=%d skips=%d", len(chunks), len(skipped))
			}
			for _, chunk := range chunks {
				wire, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, epoch, chunk)
				if err != nil {
					t.Fatal(err)
				}
				frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(wire.RawLine))
				if err != nil {
					t.Fatal(err)
				}
				service.handleRouteAnnounceV3(receive, "addr-peerB", frame)
			}
			if got := announceLimiterTokens(t, limiter, subject); got != 0 {
				t.Fatalf("initial full did not drain exact burst: %v", got)
			}
			initial := service.routingTable.Snapshot()
			for _, entry := range entries {
				found := false
				for _, row := range initial.Routes[entry.Identity] {
					if row.NextHop == sender && !row.IsWithdrawn() && row.SeqNo == entry.SeqNo {
						found = true
					}
				}
				if !found {
					t.Fatal("initial actual handler did not apply every seed claim")
				}
			}

			// Use the actual withdrawal operation; don't let retained old rows satisfy
			// the second receipt after their replacement frames are rate-limited.
			if _, err := service.routingTable.RemoveDirectPeer(sender); err != nil {
				t.Fatal(err)
			}
			withdrawn := service.routingTable.Snapshot()
			replacement := append([]routing.AnnounceEntry(nil), entries...)
			for i := range replacement {
				found := false
				var watermark uint64
				for _, row := range withdrawn.Routes[replacement[i].Identity] {
					if row.NextHop == sender {
						if !row.IsWithdrawn() {
							t.Fatal("withdrawal left an old claim usable")
						}
						if found {
							t.Fatal("duplicate original uplink claim")
						}
						found = true
						watermark = row.SeqNo
					}
				}
				if !found || watermark == ^uint64(0) {
					t.Fatal("missing withdrawal watermark")
				}
				replacement[i].SeqNo = watermark + 1 // strictly valid recovery, no stale-seq confound
			}
			wall = wall.Add(tc.after)
			service.announceLoop.StateRegistry().MarkReconnected(sender, caps)
			receive = service.provenRoutingSender(sender)
			if service.announceLimiter != limiter || receive.penalty != subject {
				t.Fatal("reconnect reset/rekeyed the rate budget")
			}
			if _, err := service.routingTable.AddDirectPeer(sender); err != nil {
				t.Fatal(err)
			}

			now := time.Duration(1)
			window, err := newRouteWireFrameWindow(10, 20, func() (time.Duration, error) { return now, nil })
			if err != nil {
				t.Fatal(err)
			}
			obs := &LoadstandRouteWireObservation{window: window, fullBudget: count, maximum: 1, connections: make(map[domain.ConnID]*routeWireConnection)}
			const connID domain.ConnID = 1
			obs.begin(connID)
			obs.bind(connID, sender, domain.PeerDirectionInbound, [32]byte{1})
			now = 15
			chunks, skipped = chunkRouteAnnounceV3EntriesBySize(replacement, protocol.RouteAnnounceV3KindFull, epoch+1, protocol.MaxFrameLine)
			if len(chunks) != 100 || len(skipped) != 0 {
				t.Fatalf("replacement chunks=%d skips=%d", len(chunks), len(skipped))
			}
			for _, chunk := range chunks {
				wire, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, epoch+1, chunk)
				if err != nil {
					t.Fatal(err)
				}
				frame, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(wire.RawLine))
				if err != nil {
					t.Fatal(err)
				}
				obs.observe(connID, routeWireRead, wire.RawLine, true, 11)
				service.handleRouteAnnounceV3(receive, "addr-peerB", frame)
			}
			if obs.incomplete || obs.fullRows != count || obs.connections[connID].counts[routeWireRead][routeWireFullV3].frames != 100 {
				t.Fatal("wire observation itself incomplete or over budget")
			}
			table := service.routingTable.Snapshot()
			accepted := 0
			for _, entry := range replacement {
				found := false
				for _, row := range table.Routes[entry.Identity] {
					if row.NextHop == sender && !row.IsWithdrawn() && row.SeqNo == entry.SeqNo {
						found = true
					}
				}
				if found {
					accepted++
				}
			}
			if accepted != tc.accepted {
				t.Fatalf("actual accepted=%d want=%d", accepted, tc.accepted)
			}
			now = 21
			err = obs.captureFullReceipts(service.routingTable.Snapshot)
			if tc.wantReceipt {
				if err != nil || !obs.fullTaken || obs.incomplete || obs.connections[connID].fullReceive.receipt == nil || obs.connections[connID].fullReceive.receipt.Destinations != count {
					t.Fatalf("fully replenished actual handler has no strict receipt: %v", err)
				}
			} else if !errors.Is(err, errRouteWireIncomplete) || !obs.incomplete || obs.fullTaken {
				t.Fatalf("partial handler falsely satisfied full receipt: %v", err)
			}
			t.Logf("after=%s wire_frames=100 wire_claims=%d accepted=%d full_receipt=%t", tc.after, count, accepted, tc.wantReceipt)
		})
	}
}
