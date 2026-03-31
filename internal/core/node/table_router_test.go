package node

import (
	"testing"

	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"
)

func TestTableRouterImplementsRouter(t *testing.T) {
	var _ Router = (*TableRouter)(nil)
}

func TestTableRouterLookupReturnsRelayNextHop(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	// Add a direct peer so there is a route.
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Add a route to "target-X" via "peer-B".
	ok, err := table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "peer-B",
		NextHop:  "peer-B",
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || !ok {
		t.Fatalf("UpdateRoute failed: ok=%v, err=%v", ok, err)
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// Mock: peer-B has an active session.
		sessionChecker: func(peerIdentity string, hops int) string {
			if peerIdentity == "peer-B" {
				return "addr-B"
			}
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "target-X",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set")
	}
	if *decision.RelayNextHop != "peer-B" {
		t.Fatalf("expected RelayNextHop=peer-B, got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterNoRouteGossipFallback(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity string, hops int) string {
			return "addr-" + peerIdentity
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "unknown-target",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop != nil {
		t.Fatal("expected RelayNextHop to be nil when no route exists")
	}
}

func TestTableRouterNoSessionGossipFallback(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	// Route exists but no session available.
	ok, err := table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "peer-B",
		NextHop:  "peer-B",
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || !ok {
		t.Fatalf("UpdateRoute failed: ok=%v, err=%v", ok, err)
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// No session for any peer.
		sessionChecker: func(peerIdentity string, hops int) string {
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "target-X",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop != nil {
		t.Fatal("expected RelayNextHop to be nil when no session is available")
	}
}

func TestTableRouterPrefersBetterRoute(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	// Two routes: hop_ack via peer-B (2 hops) and announcement via peer-C (1 hop).
	// hop_ack should be preferred (higher trust).
	ok, _ := table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "target-X",
		NextHop:  "peer-C",
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if !ok {
		t.Fatal("first UpdateRoute should accept")
	}

	ok, _ = table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "target-X",
		NextHop:  "peer-B",
		Hops:     3,
		SeqNo:    1,
		Source:   routing.RouteSourceHopAck,
	})
	if !ok {
		t.Fatal("second UpdateRoute should accept")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity string, hops int) string {
			return "addr-" + peerIdentity
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "target-X",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set")
	}
	// hop_ack peer-B should be preferred over announcement peer-C.
	if *decision.RelayNextHop != "peer-B" {
		t.Fatalf("expected RelayNextHop=peer-B (hop_ack), got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterFallsBackToSecondRoute(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	// Two routes: best via peer-B (no session), secondary via peer-C (has session).
	ok, _ := table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "target-X",
		NextHop:  "peer-B",
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceHopAck,
	})
	if !ok {
		t.Fatal("first UpdateRoute should accept")
	}

	ok, _ = table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "target-X",
		NextHop:  "peer-C",
		Hops:     3,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if !ok {
		t.Fatal("second UpdateRoute should accept")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity string, hops int) string {
			if peerIdentity == "peer-C" {
				return "addr-C"
			}
			return "" // peer-B has no session
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "target-X",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set via fallback route")
	}
	if *decision.RelayNextHop != "peer-C" {
		t.Fatalf("expected RelayNextHop=peer-C (fallback), got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterDirectPeerRelayOnlyCap(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	// Add a direct route to peer-B (hops=1).
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// Simulate: peer-B has only relay cap (no routing cap).
		// For direct routes (hops=1), relay-only should suffice.
		sessionChecker: func(peerIdentity string, hops int) string {
			if peerIdentity == "peer-B" && hops <= 1 {
				return "addr-B"
			}
			// For transit (hops>1), reject relay-only peers.
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "peer-B",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop for direct peer with relay-only cap")
	}
	if *decision.RelayNextHop != "peer-B" {
		t.Fatalf("expected RelayNextHop=peer-B, got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterTransitPeerNeedsBothCaps(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	// Route to target-X via peer-B (hops=2, transit).
	ok, err := table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "peer-B",
		NextHop:  "peer-B",
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute failed")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// Simulate: peer-B has only relay cap. Transit requires both.
		sessionChecker: func(peerIdentity string, hops int) string {
			if hops <= 1 {
				return "addr-" + peerIdentity
			}
			// Transit: reject relay-only peers.
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "target-X",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop != nil {
		t.Fatal("expected RelayNextHop=nil for transit peer with relay-only cap")
	}
}
