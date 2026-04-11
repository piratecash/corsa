package rpc_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// mockRoutingProvider implements rpc.RoutingProvider for tests.
type mockRoutingProvider struct {
	snapshot       routing.Snapshot
	peerTransports map[domain.PeerIdentity][2]string // identity → [address, network]
}

func (m *mockRoutingProvider) RoutingSnapshot() routing.Snapshot {
	return m.snapshot
}

func (m *mockRoutingProvider) PeerTransport(peerIdentity domain.PeerIdentity) (domain.PeerAddress, domain.NetGroup) {
	if m.peerTransports == nil {
		return "", ""
	}
	t := m.peerTransports[peerIdentity]
	return domain.PeerAddress(t[0]), domain.NetGroup(t[1])
}

// --- Tests ---

func TestRoutingCommandsUnavailableWhenProviderNil(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &mockNodeProvider{}, nil, nil, nil, nil)

	commands := []string{"fetchRouteTable", "fetchRouteSummary", "fetchRouteLookup"}
	for _, cmd := range commands {
		// Should not appear in help.
		for _, info := range table.Commands() {
			if info.Name == cmd {
				t.Errorf("%s should be hidden from Commands() when RoutingProvider is nil", cmd)
			}
		}

		// Should return 503 on execution.
		resp := table.Execute(rpc.CommandRequest{Name: cmd})
		if resp.ErrorKind != rpc.ErrUnavailable {
			t.Errorf("%s: expected ErrUnavailable, got %v", cmd, resp.ErrorKind)
		}
	}
}

func TestRoutingCommandsVisibleWhenProviderSet(t *testing.T) {
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes:  make(map[routing.PeerIdentity][]routing.RouteEntry),
			TakenAt: time.Now(),
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &mockNodeProvider{}, nil, nil, nil, provider)

	expected := map[string]bool{
		"fetchRouteTable":   false,
		"fetchRouteSummary": false,
		"fetchRouteLookup":  false,
	}

	for _, cmd := range table.Commands() {
		if _, ok := expected[cmd.Name]; ok {
			expected[cmd.Name] = true
			if cmd.Category != "routing" {
				t.Errorf("%s: expected category 'routing', got %q", cmd.Name, cmd.Category)
			}
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("%s not found in Commands() when RoutingProvider is set", name)
		}
	}
}

func TestFetchRouteTable(t *testing.T) {
	now := time.Now()
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"peer-A": {
					{
						Identity:  "peer-A",
						Origin:    "self",
						NextHop:   "peer-A",
						Hops:      1,
						SeqNo:     5,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(100 * time.Second),
					},
				},
				"peer-B": {
					{
						Identity:  "peer-B",
						Origin:    "peer-A",
						NextHop:   "peer-A",
						Hops:      2,
						SeqNo:     3,
						Source:    routing.RouteSourceAnnouncement,
						ExpiresAt: now.Add(50 * time.Second),
					},
				},
			},
			TakenAt:       now,
			TotalEntries:  3,
			ActiveEntries: 2,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteTable"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if total, _ := result["total"].(float64); int(total) != 3 {
		t.Errorf("expected total=3, got %v", total)
	}
	if active, _ := result["active"].(float64); int(active) != 2 {
		t.Errorf("expected active=2, got %v", active)
	}

	routes, ok := result["routes"].([]interface{})
	if !ok {
		t.Fatal("routes field missing or not an array")
	}
	if len(routes) != 2 {
		t.Errorf("expected 2 routes, got %d", len(routes))
	}
}

func TestFetchRouteTableNextHopObject(t *testing.T) {
	now := time.Now()
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"peer-A": {
					{
						Identity:  "peer-A",
						Origin:    "self",
						NextHop:   "peer-A",
						Hops:      1,
						SeqNo:     1,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(60 * time.Second),
					},
				},
			},
			TakenAt:       now,
			TotalEntries:  1,
			ActiveEntries: 1,
		},
		peerTransports: map[domain.PeerIdentity][2]string{
			"peer-A": {"65.108.204.190:64646", "ipv4"},
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteTable"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	route, _ := routes[0].(map[string]interface{})

	nextHop, ok := route["next_hop"].(map[string]interface{})
	if !ok {
		t.Fatal("next_hop should be an object")
	}
	if id, _ := nextHop["identity"].(string); id != "peer-A" {
		t.Errorf("expected next_hop.identity='peer-A', got %q", id)
	}
	if addr, _ := nextHop["address"].(string); addr != "65.108.204.190:64646" {
		t.Errorf("expected next_hop.address='65.108.204.190:64646', got %q", addr)
	}
	if net, _ := nextHop["network"].(string); net != "ipv4" {
		t.Errorf("expected next_hop.network='ipv4', got %q", net)
	}
}

func TestFetchRouteTableNextHopDisconnected(t *testing.T) {
	now := time.Now()
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"peer-A": {
					{
						Identity:  "peer-A",
						Origin:    "self",
						NextHop:   "peer-A",
						Hops:      1,
						SeqNo:     1,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(60 * time.Second),
					},
				},
			},
			TakenAt:       now,
			TotalEntries:  1,
			ActiveEntries: 1,
		},
		// No peerTransports — peer disconnected.
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteTable"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	route, _ := routes[0].(map[string]interface{})

	nextHop, ok := route["next_hop"].(map[string]interface{})
	if !ok {
		t.Fatal("next_hop should be an object")
	}
	if id, _ := nextHop["identity"].(string); id != "peer-A" {
		t.Errorf("expected next_hop.identity='peer-A', got %q", id)
	}
	// address and network omitted when peer is disconnected.
	if _, exists := nextHop["address"]; exists {
		t.Error("next_hop.address should be omitted when peer is disconnected")
	}
	if _, exists := nextHop["network"]; exists {
		t.Error("next_hop.network should be omitted when peer is disconnected")
	}
}

func TestFetchRouteSummary(t *testing.T) {
	now := time.Now()
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"peer-A": {
					{
						Identity:  "peer-A",
						Origin:    "self",
						NextHop:   "peer-A",
						Hops:      1,
						SeqNo:     5,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(100 * time.Second),
					},
				},
			},
			TakenAt:       now,
			TotalEntries:  1,
			ActiveEntries: 1,
			FlapState: []routing.FlapEntry{
				{
					PeerIdentity:      "flappy-peer",
					RecentWithdrawals: 4,
					InHoldDown:        true,
					HoldDownUntil:     now.Add(20 * time.Second),
				},
			},
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteSummary"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if v, _ := result["total_entries"].(float64); int(v) != 1 {
		t.Errorf("expected total_entries=1, got %v", v)
	}
	if v, _ := result["active_entries"].(float64); int(v) != 1 {
		t.Errorf("expected active_entries=1, got %v", v)
	}
	if v, _ := result["reachable_identities"].(float64); int(v) != 1 {
		t.Errorf("expected reachable_identities=1, got %v", v)
	}
	if v, _ := result["direct_peers"].(float64); int(v) != 1 {
		t.Errorf("expected direct_peers=1, got %v", v)
	}
	if v, _ := result["withdrawn_entries"].(float64); int(v) != 0 {
		t.Errorf("expected withdrawn_entries=0, got %v", v)
	}
	snapAt, _ := result["snapshot_at"].(string)
	if snapAt == "" {
		t.Error("expected non-empty snapshot_at")
	}

	flap, ok := result["flap_state"].([]interface{})
	if !ok || len(flap) != 1 {
		t.Fatalf("expected 1 flap entry, got %v", result["flap_state"])
	}
	flapEntry, _ := flap[0].(map[string]interface{})
	if peer, _ := flapEntry["peer_identity"].(string); peer != "flappy-peer" {
		t.Errorf("expected peer_identity='flappy-peer', got %q", peer)
	}
	if inHD, _ := flapEntry["in_hold_down"].(bool); !inHD {
		t.Error("expected in_hold_down=true")
	}
}

func TestFetchRouteLookup(t *testing.T) {
	now := time.Now()
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44": {
					{
						Identity:  "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44",
						Origin:    "self",
						NextHop:   "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44",
						Hops:      1,
						SeqNo:     10,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(100 * time.Second),
					},
					{
						Identity:  "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44",
						Origin:    "neighbor",
						NextHop:   "neighbor",
						Hops:      2,
						SeqNo:     7,
						Source:    routing.RouteSourceAnnouncement,
						ExpiresAt: now.Add(80 * time.Second),
					},
				},
			},
			TakenAt: now,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if v, _ := result["identity"].(string); v != "aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44" {
		t.Errorf("expected identity='target-peer', got %q", v)
	}
	if v, _ := result["count"].(float64); int(v) != 2 {
		t.Errorf("expected count=2, got %v", v)
	}

	routes, ok := result["routes"].([]interface{})
	if !ok || len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %v", result["routes"])
	}
}

func TestFetchRouteLookupRequiresIdentity(t *testing.T) {
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes:  make(map[routing.PeerIdentity][]routing.RouteEntry),
			TakenAt: time.Now(),
		},
	}
	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{},
	})
	if resp.ErrorKind != rpc.ErrValidation {
		t.Errorf("expected ErrValidation, got %v", resp.ErrorKind)
	}
}

func TestFetchRouteLookupRejectsMalformedIdentity(t *testing.T) {
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes:  make(map[routing.PeerIdentity][]routing.RouteEntry),
			TakenAt: time.Now(),
		},
	}
	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	malformed := []string{
		"too-short",
		"AABB",
		"GGHHIIJJ00112233445566778899aabbccddeeff",
		"../../../etc/passwd",
		"a1b2c3d4", // only 8 chars
	}

	for _, id := range malformed {
		resp := table.Execute(rpc.CommandRequest{
			Name: "fetchRouteLookup",
			Args: map[string]interface{}{"identity": id},
		})
		if resp.ErrorKind != rpc.ErrValidation {
			t.Errorf("identity %q: expected ErrValidation, got %v", id, resp.ErrorKind)
		}
	}
}

func TestFetchRouteLookupNoRoutes(t *testing.T) {
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes:  map[routing.PeerIdentity][]routing.RouteEntry{},
			TakenAt: time.Now(),
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": "ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00"},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if v, _ := result["count"].(float64); int(v) != 0 {
		t.Errorf("expected count=0, got %v", v)
	}
}

func TestFetchRouteTableUsesSnapshotTime(t *testing.T) {
	// Snapshot taken 10s ago with a route expiring 50s after snapshot time.
	// TTL and expired should be computed from snap.TakenAt, not wall clock.
	snapTime := time.Now().Add(-10 * time.Second)
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"peer-A": {
					{
						Identity:  "peer-A",
						Origin:    "self",
						NextHop:   "peer-A",
						Hops:      1,
						SeqNo:     1,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: snapTime.Add(50 * time.Second),
					},
				},
			},
			TakenAt:       snapTime,
			TotalEntries:  1,
			ActiveEntries: 1,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteTable"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	route, _ := routes[0].(map[string]interface{})

	// TTL should be ~50s (relative to snapshot time), not ~40s (relative to now).
	ttl, _ := route["ttl_seconds"].(float64)
	if ttl < 49.0 || ttl > 51.0 {
		t.Errorf("TTL should be ~50s (from snapshot time), got %.1f", ttl)
	}

	expired, _ := route["expired"].(bool)
	if expired {
		t.Error("route should not be expired at snapshot time")
	}
}

func TestFetchRouteSummaryUsesSnapshotTime(t *testing.T) {
	// Route expired 5s before wall clock but still alive at snapshot time.
	snapTime := time.Now().Add(-10 * time.Second)
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"peer-A": {
					{
						Identity:  "peer-A",
						Origin:    "self",
						NextHop:   "peer-A",
						Hops:      1,
						SeqNo:     1,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: snapTime.Add(8 * time.Second), // expired by now, alive at snapshot
					},
				},
			},
			TakenAt:       snapTime,
			TotalEntries:  1,
			ActiveEntries: 1,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteSummary"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	// Route was alive at snapshot time, so it should be counted as reachable.
	reachable, _ := result["reachable_identities"].(float64)
	if int(reachable) != 1 {
		t.Errorf("expected reachable_identities=1 (alive at snapshot time), got %v", reachable)
	}
}

func TestFetchRouteLookupUsesSnapshotTime(t *testing.T) {
	// Snapshot taken 10s ago with a route expiring 50s after snapshot time.
	// TTL should be ~50s (from snapshot), not ~40s (from wall clock).
	snapTime := time.Now().Add(-10 * time.Second)
	targetID := routing.PeerIdentity("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				targetID: {
					{
						Identity:  targetID,
						Origin:    "self",
						NextHop:   targetID,
						Hops:      1,
						SeqNo:     1,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: snapTime.Add(50 * time.Second),
					},
				},
			},
			TakenAt: snapTime,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": string(targetID)},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	route, _ := routes[0].(map[string]interface{})

	ttl, _ := route["ttl_seconds"].(float64)
	if ttl < 49.0 || ttl > 51.0 {
		t.Errorf("TTL should be ~50s (from snapshot time), got %.1f", ttl)
	}
}

func TestFetchRouteLookupSortsByPreference(t *testing.T) {
	now := time.Now()
	targetID := routing.PeerIdentity("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				targetID: {
					{
						Identity:  targetID,
						Origin:    "remote",
						NextHop:   "remote",
						Hops:      3,
						SeqNo:     1,
						Source:    routing.RouteSourceAnnouncement,
						ExpiresAt: now.Add(60 * time.Second),
					},
					{
						Identity:  targetID,
						Origin:    "self",
						NextHop:   targetID,
						Hops:      1,
						SeqNo:     5,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(100 * time.Second),
					},
				},
			},
			TakenAt: now,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": string(targetID)},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	routes, _ := result["routes"].([]interface{})
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	// Direct route should come first (higher trust).
	first, _ := routes[0].(map[string]interface{})
	if src, _ := first["source"].(string); src != "direct" {
		t.Errorf("expected first route source='direct', got %q", src)
	}
}

func TestFetchRouteLookupFiltersWithdrawnAndExpired(t *testing.T) {
	now := time.Now()
	targetID := routing.PeerIdentity("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				targetID: {
					{
						Identity:  targetID,
						Origin:    "self",
						NextHop:   targetID,
						Hops:      1,
						SeqNo:     5,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(100 * time.Second),
					},
					{
						Identity:  targetID,
						Origin:    "remote-withdrawn",
						NextHop:   "relay",
						Hops:      16, // withdrawn
						SeqNo:     3,
						Source:    routing.RouteSourceAnnouncement,
						ExpiresAt: now.Add(60 * time.Second),
					},
					{
						Identity:  targetID,
						Origin:    "remote-expired",
						NextHop:   "relay2",
						Hops:      2,
						SeqNo:     4,
						Source:    routing.RouteSourceAnnouncement,
						ExpiresAt: now.Add(-10 * time.Second), // expired
					},
				},
			},
			TakenAt: now,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": string(targetID)},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("expected 1 active route (withdrawn and expired filtered out), got %d", len(routes))
	}

	first, _ := routes[0].(map[string]interface{})
	if src, _ := first["source"].(string); src != "direct" {
		t.Errorf("expected remaining route source='direct', got %q", src)
	}

	count, _ := result["count"].(float64)
	if int(count) != 1 {
		t.Errorf("expected count=1, got %v", count)
	}
}

func TestConsoleParserRoutingCommands(t *testing.T) {
	// fetch_route_table — no args
	req, err := rpc.ParseConsoleInput("fetch_route_table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetch_route_table" {
		t.Errorf("expected command 'fetch_route_table', got %q", req.Name)
	}

	// fetch_route_summary — no args
	req, err = rpc.ParseConsoleInput("fetch_route_summary")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetch_route_summary" {
		t.Errorf("expected command 'fetch_route_summary', got %q", req.Name)
	}

	// fetch_route_lookup with identity arg (snake_case still works, canonicalized to camelCase)
	req, err = rpc.ParseConsoleInput("fetch_route_lookup peer-abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetchRouteLookup" {
		t.Errorf("expected command 'fetchRouteLookup', got %q", req.Name)
	}
	if id, _ := req.Args["identity"].(string); id != "peer-abc123" {
		t.Errorf("expected identity='peer-abc123', got %q", id)
	}

	// fetchRouteLookup without arg — should error (camelCase input)
	_, err = rpc.ParseConsoleInput("fetchRouteLookup")
	if err == nil {
		t.Error("expected error for fetchRouteLookup without identity")
	}
}

func TestFetchRouteSummaryExcludesSelfRoute(t *testing.T) {
	now := time.Now()
	// Snapshot that includes a synthetic local self-route alongside a real
	// direct peer route. The summary must not count the self-route in
	// reachable_identities or direct_peers.
	provider := &mockRoutingProvider{
		snapshot: routing.Snapshot{
			Routes: map[routing.PeerIdentity][]routing.RouteEntry{
				"nodeA": {
					// Synthetic local self-route (as injected by Table.Snapshot).
					{
						Identity: "nodeA",
						Origin:   "nodeA",
						NextHop:  "nodeA",
						Hops:     0,
						SeqNo:    0,
						Source:   routing.RouteSourceLocal,
					},
				},
				"peer-B": {
					{
						Identity:  "peer-B",
						Origin:    "nodeA",
						NextHop:   "peer-B",
						Hops:      1,
						SeqNo:     3,
						Source:    routing.RouteSourceDirect,
						ExpiresAt: now.Add(100 * time.Second),
					},
				},
			},
			TakenAt: now,
			// Counters describe persisted state only — the synthetic
			// self-route is not counted, matching Table.Snapshot() behaviour.
			TotalEntries:  1,
			ActiveEntries: 1,
		},
	}

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteSummary"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	// Only peer-B should be counted — the local self-route is excluded.
	reachable, _ := result["reachable_identities"].(float64)
	if int(reachable) != 1 {
		t.Errorf("expected reachable_identities=1 (self-route excluded), got %v", reachable)
	}
	direct, _ := result["direct_peers"].(float64)
	if int(direct) != 1 {
		t.Errorf("expected direct_peers=1 (self-route excluded), got %v", direct)
	}

	// Verify counters from snapshot pass through correctly.
	total, _ := result["total_entries"].(float64)
	if int(total) != 1 {
		t.Errorf("expected total_entries=1 (self-route not counted), got %v", total)
	}
	active, _ := result["active_entries"].(float64)
	if int(active) != 1 {
		t.Errorf("expected active_entries=1 (self-route not counted), got %v", active)
	}
}
