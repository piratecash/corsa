package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// These fingerprints are used across this file. TableRouter.Route
// decodes Envelope.Recipient via domain.PeerIdentityFromWire, so any
// identity that flows through a Recipient (the destination, and the
// direct-peer in the relay-only-cap test) must be valid 40-char hex
// for the table lookup to resolve to the seeded route Identity.
const (
	tableRouterTargetXHex = "dd00000000000000000000000000000000000001"
	tableRouterPeerBHex   = "bb00000000000000000000000000000000000002"
	tableRouterPeerCHex   = "bb00000000000000000000000000000000000003"
	tableRouterNodeAHex   = "aa00000000000000000000000000000000000001"
)

func TestTableRouterImplementsRouter(t *testing.T) {
	var _ Router = (*TableRouter)(nil)
}

func TestTableRouterLookupReturnsRelayNextHop(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	// Add a direct peer so there is a route.
	if _, err := table.AddDirectPeer(domain.PeerIdentityFromWire(tableRouterPeerBHex)); err != nil {
		t.Fatal(err)
	}

	// Add a route to "target-X" via "peer-B".
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterPeerBHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerBHex),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("UpdateRoute failed: status=%v, err=%v", status, err)
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// Mock: peer-B has an active session.
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			if peerIdentity == domain.PeerIdentityFromWire(tableRouterPeerBHex) {
				return domain.PeerAddress("addr-B")
			}
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: tableRouterTargetXHex,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set")
	}
	if *decision.RelayNextHop != domain.PeerIdentityFromWire(tableRouterPeerBHex) {
		t.Fatalf("expected RelayNextHop=peer-B, got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterNoRouteGossipFallback(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			return domain.PeerAddress("addr-" + peerIdentity.String())
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
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	// Route exists but no session available.
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterPeerBHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerBHex),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("UpdateRoute failed: status=%v, err=%v", status, err)
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// No session for any peer.
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: tableRouterTargetXHex,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop != nil {
		t.Fatal("expected RelayNextHop to be nil when no session is available")
	}
}

func TestTableRouterPrefersBetterRoute(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	// Phase 2 changed Lookup ranking from "source-tier first, then
	// hops" to a single CompositeScore (base − hops×10 + RTTBonus +
	// healthPenalty + sourceBonus). At equal hops the source bonus
	// (+20 Direct / +10 HopAck / +0 Announcement) decides the
	// ordering, which is what this test asserts. At unequal hops
	// the new ranking is additive — see
	// docs/protocol/route_health.md §4.2. The original
	// test used hops=3 vs hops=2, which after Phase 2 ties at score
	// 80 vs 80; equalised here to 2 vs 2 so the source-priority
	// invariant the test is actually about remains observable.

	// announcement via peer-C, 2 hops → score 80
	status, _ := table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterTargetXHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerCHex),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if status != routing.RouteAccepted {
		t.Fatal("first UpdateRoute should accept")
	}

	// hop_ack via peer-B, 2 hops → score 90 (wins by +10 source bonus)
	status, _ = table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterTargetXHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerBHex),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceHopAck,
	})
	if status != routing.RouteAccepted {
		t.Fatal("second UpdateRoute should accept")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			return domain.PeerAddress("addr-" + peerIdentity.String())
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: tableRouterTargetXHex,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set")
	}
	// hop_ack peer-B should be preferred over announcement peer-C.
	if *decision.RelayNextHop != domain.PeerIdentityFromWire(tableRouterPeerBHex) {
		t.Fatalf("expected RelayNextHop=peer-B (hop_ack), got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterFallsBackToSecondRoute(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	// Two routes: best via peer-B (no session), secondary via peer-C (has session).
	status, _ := table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterTargetXHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerBHex),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceHopAck,
	})
	if status != routing.RouteAccepted {
		t.Fatal("first UpdateRoute should accept")
	}

	status, _ = table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterTargetXHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerCHex),
		Hops:     3,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if status != routing.RouteAccepted {
		t.Fatal("second UpdateRoute should accept")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			if peerIdentity == domain.PeerIdentityFromWire(tableRouterPeerCHex) {
				return domain.PeerAddress("addr-C")
			}
			return "" // peer-B has no session
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: tableRouterTargetXHex,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop to be set via fallback route")
	}
	if *decision.RelayNextHop != domain.PeerIdentityFromWire(tableRouterPeerCHex) {
		t.Fatalf("expected RelayNextHop=peer-C (fallback), got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterDirectPeerRelayOnlyCap(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	// Add a direct route to peer-B (hops=1).
	if _, err := table.AddDirectPeer(domain.PeerIdentityFromWire(tableRouterPeerBHex)); err != nil {
		t.Fatal(err)
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// Simulate: peer-B has only relay cap (no routing cap).
		// For direct routes (hops=1), relay-only should suffice.
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			if peerIdentity == domain.PeerIdentityFromWire(tableRouterPeerBHex) && hops <= 1 {
				return domain.PeerAddress("addr-B")
			}
			// For transit (hops>1), reject relay-only peers.
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: tableRouterPeerBHex,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop for direct peer with relay-only cap")
	}
	if *decision.RelayNextHop != domain.PeerIdentityFromWire(tableRouterPeerBHex) {
		t.Fatalf("expected RelayNextHop=peer-B, got %s", *decision.RelayNextHop)
	}
}

func TestTableRouterTransitPeerNeedsBothCaps(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domain.PeerIdentityFromWire(tableRouterNodeAHex)))

	// Route to target-X via peer-B (hops=2, transit).
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(tableRouterTargetXHex),
		Origin:   domain.PeerIdentityFromWire(tableRouterPeerBHex),
		NextHop:  domain.PeerIdentityFromWire(tableRouterPeerBHex),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute failed")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		// Simulate: peer-B has only relay cap. Transit requires both.
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			if hops <= 1 {
				return domain.PeerAddress("addr-" + peerIdentity.String())
			}
			// Transit: reject relay-only peers.
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: tableRouterTargetXHex,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop != nil {
		t.Fatal("expected RelayNextHop=nil for transit peer with relay-only cap")
	}
}
