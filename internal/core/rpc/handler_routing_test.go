package rpc_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// nodeWithRouting combines MockNodeProvider + MockRoutingProvider so that
// RegisterAllCommands can discover the RoutingProvider capability via type assertion.
type nodeWithRouting struct {
	*rpcmocks.MockNodeProvider
	*rpcmocks.MockRoutingProvider
}

// newMockRoutingProvider creates a MockRoutingProvider that returns the given
// snapshot from RoutingSnapshot() and resolves PeerTransport calls using the
// supplied map. When peerTransports is nil the mock returns empty values for
// any peer identity (simulating disconnected peers).
func newMockRoutingProvider(
	t *testing.T,
	snapshot routing.Snapshot,
	peerTransports map[domain.PeerIdentity][2]string,
) *rpcmocks.MockRoutingProvider {
	t.Helper()
	m := rpcmocks.NewMockRoutingProvider(t)
	m.On("RoutingSnapshot").Return(snapshot).Maybe()
	// Phase 0 overload-gate counters surface through fetchRouteSummary.
	// Default to a zero stats struct in handler tests; specific tests
	// asserting overload reporting can override with their own mock setup.
	m.On("OverloadStats").Return(routing.OverloadStats{}).Maybe()
	// fetchRouteLookup reads HealthSnapshot (full per-pair tiers) for its
	// Dead/cooldown filters + CompositeScore ranking, because the published
	// Snapshot.Health now carries only the routing-relevant {Dead ∪ cooled}
	// subset. Echo the snapshot's Health through HealthSnapshot so a test that
	// seeds Snapshot.Health drives the lookup filters too; health-specific
	// tests can still override with their own .On call.
	m.On("HealthSnapshot").Return(snapshot.Health).Maybe()

	if peerTransports != nil {
		m.EXPECT().PeerTransport(mock.Anything).RunAndReturn(
			func(id domain.PeerIdentity) (domain.PeerAddress, domain.NetGroup) {
				if entry, ok := peerTransports[id]; ok {
					return domain.PeerAddress(entry[0]), domain.NetGroup(entry[1])
				}
				return "", ""
			},
		).Maybe()
	} else {
		m.On("PeerTransport", mock.Anything).Return(domain.PeerAddress(""), domain.NetGroup("")).Maybe()
	}

	return m
}

// --- Tests ---

func TestRoutingCommandsUnavailableWhenProviderNil(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)

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
	node := &nodeWithRouting{
		MockNodeProvider: newDefaultNodeProvider(t),
		MockRoutingProvider: newMockRoutingProvider(t, routing.Snapshot{
			Routes:  make(map[routing.PeerIdentity][]routing.RouteEntry),
			TakenAt: time.Now(),
		}, nil),
	}

	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("peer-A"): {
				{
					Identity:  domaintest.ID("peer-A"),
					Origin:    domaintest.ID("self"),
					NextHop:   domaintest.ID("peer-A"),
					Hops:      1,
					SeqNo:     5,
					Source:    routing.RouteSourceDirect,
					ExpiresAt: now.Add(100 * time.Second),
				},
			},
			domaintest.ID("peer-B"): {
				{
					Identity:  domaintest.ID("peer-B"),
					Origin:    domaintest.ID("peer-A"),
					NextHop:   domaintest.ID("peer-A"),
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
	}, nil)

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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("peer-A"): {
				{
					Identity:  domaintest.ID("peer-A"),
					Origin:    domaintest.ID("self"),
					NextHop:   domaintest.ID("peer-A"),
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
	}, map[domain.PeerIdentity][2]string{
		domaintest.ID("peer-A"): {"65.108.204.190:64646", "ipv4"},
	})

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
	if id, _ := nextHop["identity"].(string); id != domaintest.ID("peer-A").String() {
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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("peer-A"): {
				{
					Identity:  domaintest.ID("peer-A"),
					Origin:    domaintest.ID("self"),
					NextHop:   domaintest.ID("peer-A"),
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
	}, nil)

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
	if id, _ := nextHop["identity"].(string); id != domaintest.ID("peer-A").String() {
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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("peer-A"): {
				{
					Identity:  domaintest.ID("peer-A"),
					Origin:    domaintest.ID("self"),
					NextHop:   domaintest.ID("peer-A"),
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
				PeerIdentity:      domaintest.ID("flappy-peer"),
				RecentWithdrawals: 4,
				InHoldDown:        true,
				HoldDownUntil:     now.Add(20 * time.Second),
			},
		},
	}, nil)

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
	if peer, _ := flapEntry["peer_identity"].(string); peer != domaintest.ID("flappy-peer").String() {
		t.Errorf("expected peer_identity='flappy-peer', got %q", peer)
	}
	if inHD, _ := flapEntry["in_hold_down"].(bool); !inHD {
		t.Error("expected in_hold_down=true")
	}
}

// TestFetchRouteSummary_OverloadEngagedCyclesSurfaced pins the
// operator-visible contract for the overload-gate counter (strict
// "actually shed work" semantic). The fetchRouteSummary response
// MUST include an `overload` JSON object with `engaged_cycles` set
// to the cumulative number of cycles where at least one delta-due
// peer was skipped specifically because of overload — i.e. cycles
// where the gate actually shed CPU. Cycles where the gate engaged
// but every peer was forced-due (no delta to suppress) do NOT count.
// Without this contract surfaced through RPC, the counter would only
// be observable via the in-process `AnnounceLoop.OverloadCycleCount()`
// accessor — no operator surface — and a regression in
// routing_commands.go that drops the field from the JSON response
// would have gone unnoticed by tests. This test pins the contract by
// configuring a deliberately non-zero value on the mock and
// asserting the JSON path is non-zero with the exact value plumbed
// through.
func TestFetchRouteSummary_OverloadEngagedCyclesSurfaced(t *testing.T) {
	now := time.Now()
	const expectedEngagedCycles uint64 = 7

	// Build a mock that returns a non-zero OverloadStats. The
	// helper newMockRoutingProvider defaults to zero via .Maybe(),
	// which would mask a regression where the handler dropped the
	// field. Construct the mock directly here so the .On("OverloadStats")
	// expectation is mandatory and asserts both invocation and
	// the value flow through to the JSON response.
	provider := rpcmocks.NewMockRoutingProvider(t)
	provider.On("RoutingSnapshot").Return(routing.Snapshot{
		TakenAt:       now,
		Routes:        map[routing.PeerIdentity][]routing.RouteEntry{},
		TotalEntries:  0,
		ActiveEntries: 0,
	}).Once()
	provider.On("OverloadStats").Return(routing.OverloadStats{
		EngagedCycles: expectedEngagedCycles,
	}).Once()

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

	overloadRaw, ok := result["overload"]
	if !ok {
		t.Fatalf("response missing 'overload' object: %v", result)
	}
	overload, ok := overloadRaw.(map[string]interface{})
	if !ok {
		t.Fatalf("'overload' is not a JSON object: %T", overloadRaw)
	}

	engagedRaw, ok := overload["engaged_cycles"]
	if !ok {
		t.Fatalf("'overload.engaged_cycles' missing: %v", overload)
	}
	engaged, ok := engagedRaw.(float64) // JSON numbers decode as float64
	if !ok {
		t.Fatalf("'overload.engaged_cycles' is not a number: %T", engagedRaw)
	}
	if uint64(engaged) != expectedEngagedCycles {
		t.Fatalf("expected overload.engaged_cycles=%d, got %v",
			expectedEngagedCycles, engaged)
	}
}

func TestFetchRouteLookup(t *testing.T) {
	now := time.Now()
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"): {
				{
					Identity:  domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"),
					Origin:    domaintest.ID("self"),
					NextHop:   domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"),
					Hops:      1,
					SeqNo:     10,
					Source:    routing.RouteSourceDirect,
					ExpiresAt: now.Add(100 * time.Second),
				},
				{
					Identity:  domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44"),
					Origin:    domaintest.ID("neighbor"),
					NextHop:   domaintest.ID("neighbor"),
					Hops:      2,
					SeqNo:     7,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(80 * time.Second),
				},
			},
		},
		TakenAt: now,
	}, nil)

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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes:  make(map[routing.PeerIdentity][]routing.RouteEntry),
		TakenAt: time.Now(),
	}, nil)

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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes:  make(map[routing.PeerIdentity][]routing.RouteEntry),
		TakenAt: time.Now(),
	}, nil)

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

// TestFetchRouteLookupRejectsZeroIdentity pins the second half of the boundary:
// the all-zero 40-hex address parses with a nil error through
// domain.ParsePeerIdentity, so without the explicit IsZero gate the absent
// sentinel would slip through and look up the zero key. It must be rejected
// with ErrValidation and the provider snapshot must never be read.
func TestFetchRouteLookupRejectsZeroIdentity(t *testing.T) {
	// Build the mock WITHOUT a RoutingSnapshot expectation so that any
	// attempt to reach past the validation gate fails the test.
	provider := rpcmocks.NewMockRoutingProvider(t)

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": "0000000000000000000000000000000000000000"},
	})
	if resp.ErrorKind != rpc.ErrValidation {
		t.Errorf("expected ErrValidation for zero identity, got %v", resp.ErrorKind)
	}
	provider.AssertNotCalled(t, "RoutingSnapshot")
}

func TestFetchRouteLookupNoRoutes(t *testing.T) {
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes:  map[routing.PeerIdentity][]routing.RouteEntry{},
		TakenAt: time.Now(),
	}, nil)

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
	// Perf invariant: a lookup miss must NOT pay for the full health
	// deep-copy (RLock + copy of the whole set). The live-route guard in
	// routeLookupHandler skips rp.HealthSnapshot() when the target has no
	// live route — a regression that drops the guard would reintroduce the
	// per-request copy churn the Snapshot.Health narrowing removed.
	provider.AssertNotCalled(t, "HealthSnapshot")
}

// TestFetchRouteLookupMissSkipsHealthSnapshot pins the live-route guard for the
// other miss shapes: a target whose only routes are withdrawn or expired must
// also skip rp.HealthSnapshot().
func TestFetchRouteLookupMissSkipsHealthSnapshot(t *testing.T) {
	target := domaintest.ID("miss-target")
	now := time.Now()

	cases := map[string]routing.RouteEntry{
		"withdrawn": {
			Identity: target, Origin: domaintest.ID("o"), NextHop: domaintest.ID("nh"),
			Hops: routing.HopsInfinity, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
			ExpiresAt: now.Add(100 * time.Second),
		},
		"expired": {
			Identity: target, Origin: domaintest.ID("o"), NextHop: domaintest.ID("nh"),
			Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
			ExpiresAt: now.Add(-1 * time.Second), // already expired at snapshot time
		},
	}

	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			provider := newMockRoutingProvider(t, routing.Snapshot{
				Routes:  map[routing.PeerIdentity][]routing.RouteEntry{target: {entry}},
				TakenAt: now,
			}, nil)

			table := rpc.NewCommandTable()
			rpc.RegisterRoutingCommands(table, provider)

			resp := table.Execute(rpc.CommandRequest{
				Name: "fetchRouteLookup",
				Args: map[string]interface{}{"identity": target.String()},
			})
			if resp.Error != nil {
				t.Fatalf("unexpected error: %v", resp.Error)
			}
			var result map[string]interface{}
			if err := json.Unmarshal(resp.Data, &result); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if v, _ := result["count"].(float64); int(v) != 0 {
				t.Errorf("expected count=0 (no live route), got %v", v)
			}
			provider.AssertNotCalled(t, "HealthSnapshot")
		})
	}
}

func TestFetchRouteTableUsesSnapshotTime(t *testing.T) {
	// Snapshot taken 10s ago with a route expiring 50s after snapshot time.
	// TTL and expired should be computed from snap.TakenAt, not wall clock.
	snapTime := time.Now().Add(-10 * time.Second)
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("peer-A"): {
				{
					Identity:  domaintest.ID("peer-A"),
					Origin:    domaintest.ID("self"),
					NextHop:   domaintest.ID("peer-A"),
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
	}, nil)

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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("peer-A"): {
				{
					Identity:  domaintest.ID("peer-A"),
					Origin:    domaintest.ID("self"),
					NextHop:   domaintest.ID("peer-A"),
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
	}, nil)

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
	targetID := domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			targetID: {
				{
					Identity:  targetID,
					Origin:    domaintest.ID("self"),
					NextHop:   targetID,
					Hops:      1,
					SeqNo:     1,
					Source:    routing.RouteSourceDirect,
					ExpiresAt: snapTime.Add(50 * time.Second),
				},
			},
		},
		TakenAt: snapTime,
	}, nil)

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": targetID.String()},
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
	targetID := domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			targetID: {
				{
					Identity:  targetID,
					Origin:    domaintest.ID("remote"),
					NextHop:   domaintest.ID("remote"),
					Hops:      3,
					SeqNo:     1,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(60 * time.Second),
				},
				{
					Identity:  targetID,
					Origin:    domaintest.ID("self"),
					NextHop:   targetID,
					Hops:      1,
					SeqNo:     5,
					Source:    routing.RouteSourceDirect,
					ExpiresAt: now.Add(100 * time.Second),
				},
			},
		},
		TakenAt: now,
	}, nil)

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": targetID.String()},
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
	targetID := domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			targetID: {
				{
					Identity:  targetID,
					Origin:    domaintest.ID("self"),
					NextHop:   targetID,
					Hops:      1,
					SeqNo:     5,
					Source:    routing.RouteSourceDirect,
					ExpiresAt: now.Add(100 * time.Second),
				},
				{
					Identity:  targetID,
					Origin:    domaintest.ID("remote-withdrawn"),
					NextHop:   domaintest.ID("relay"),
					Hops:      16, // withdrawn
					SeqNo:     3,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(60 * time.Second),
				},
				{
					Identity:  targetID,
					Origin:    domaintest.ID("remote-expired"),
					NextHop:   domaintest.ID("relay2"),
					Hops:      2,
					SeqNo:     4,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(-10 * time.Second), // expired
				},
			},
		},
		TakenAt: now,
	}, nil)

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": targetID.String()},
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

// TestFetchRouteLookupFiltersDeadTransit — PR 11.26 P2#1
// regression. The RPC handler must mirror Table.Lookup's Phase 2
// filter: HealthDead transit claims are dropped. A transit route
// with Health=Dead must not appear in the response, even when
// the snapshot still carries it as a live (non-withdrawn,
// non-expired) entry.
func TestFetchRouteLookupFiltersDeadTransit(t *testing.T) {
	now := time.Now()
	targetID := domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	// PR 11.27 P2#1: Health travels alongside Routes inside the
	// cached routing.Snapshot, so the test fixture seeds both
	// halves in the same struct.
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			targetID: {
				{
					Identity:  targetID,
					Origin:    domaintest.ID("good"),
					NextHop:   domaintest.ID("uplink-good"),
					Hops:      3,
					SeqNo:     1,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(100 * time.Second),
				},
				{
					Identity:  targetID,
					Origin:    domaintest.ID("dead"),
					NextHop:   domaintest.ID("uplink-dead"),
					Hops:      2, // would normally win on hops alone
					SeqNo:     2,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(100 * time.Second),
				},
			},
		},
		TakenAt: now,
		Health: []routing.RouteHealthState{
			{Identity: targetID, Uplink: domaintest.ID("uplink-dead"), Health: routing.HealthDead},
		},
	}, nil)

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": targetID.String()},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("expected 1 route after Dead filter, got %d: %v", len(routes), routes)
	}
	first, _ := routes[0].(map[string]interface{})
	if nh, _ := first["next_hop"].(string); nh != domaintest.ID("uplink-good").String() {
		t.Fatalf("Dead transit leaked into response: NextHop = %q, want uplink-good", nh)
	}
}

// TestFetchRouteLookupExemptsDirectFromDeadFilter — PR 11.26 P2#1
// + 11.25 alignment. Direct claims with Health=Dead must STAY in
// the response (Direct exemption mirrors Lookup-side behaviour).
// The session is alive even when no organic hop_ack flowed
// lately; the announce plane keeps advertising the Direct route
// (PR 11.24 P2#1), so the relay-path RPC view must agree.
func TestFetchRouteLookupExemptsDirectFromDeadFilter(t *testing.T) {
	now := time.Now()
	targetID := domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			targetID: {
				{
					Identity:  targetID,
					Origin:    domaintest.ID("self"),
					NextHop:   targetID,
					Hops:      1,
					SeqNo:     1,
					Source:    routing.RouteSourceDirect,
					ExpiresAt: time.Time{}, // Direct: ExpiresAt zero
				},
			},
		},
		TakenAt: now,
		Health: []routing.RouteHealthState{
			{Identity: targetID, Uplink: targetID, Health: routing.HealthDead},
		},
	}, nil)

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchRouteLookup",
		Args: map[string]interface{}{"identity": targetID.String()},
	})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	routes, _ := result["routes"].([]interface{})
	if len(routes) != 1 {
		t.Fatalf("Direct Dead claim excluded from response; expected exemption (PR 11.26 P2#1). Got %d routes: %v", len(routes), routes)
	}
}

func TestConsoleParserRoutingCommands(t *testing.T) {
	// fetch_route_table — no args
	req, err := rpc.ParseConsoleInput("fetch_route_table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetchRouteTable" {
		t.Errorf("expected command 'fetchRouteTable', got %q", req.Name)
	}

	// fetch_route_summary — no args (canonicalized to camelCase)
	req, err = rpc.ParseConsoleInput("fetch_route_summary")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetchRouteSummary" {
		t.Errorf("expected command 'fetchRouteSummary', got %q", req.Name)
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
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			domaintest.ID("nodeA"): {
				// Synthetic local self-route (as injected by Table.Snapshot).
				{
					Identity: domaintest.ID("nodeA"),
					Origin:   domaintest.ID("nodeA"),
					NextHop:  domaintest.ID("nodeA"),
					Hops:     0,
					SeqNo:    0,
					Source:   routing.RouteSourceLocal,
				},
			},
			domaintest.ID("peer-B"): {
				{
					Identity:  domaintest.ID("peer-B"),
					Origin:    domaintest.ID("nodeA"),
					NextHop:   domaintest.ID("peer-B"),
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
	}, nil)

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
