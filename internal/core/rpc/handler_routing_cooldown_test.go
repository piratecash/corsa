package rpc_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// handler_routing_cooldown_test.go covers the Phase 3 review fix: the
// fetchRouteSummary reachability count and the fetchRouteLookup selectable
// list must apply the same black-hole cooldown filter the data-plane
// Table.Lookup applies (no Direct exemption). A cooled-down pair is not
// selectable by the relay path, so the RPC must not present it as
// reachable/selectable.

// futureCooldown returns a CooldownUntil deadline after `base` so the
// pair reads as actively cooled at snapshot time.
func futureCooldown(base time.Time) time.Time { return base.Add(time.Minute) }

// TestFetchRouteLookupFiltersCooledDownTransit — a cooled-down transit
// pair must be dropped from the lookup result, leaving only the healthy
// alternative.
func TestFetchRouteLookupFiltersCooledDownTransit(t *testing.T) {
	now := time.Now()
	targetID := domain.PeerIdentityFromWire("aa11bb22cc33dd44ee55ff66aa11bb22cc33dd44")
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
					Origin:    domaintest.ID("cooled"),
					NextHop:   domaintest.ID("uplink-cooled"),
					Hops:      2, // would win on hops alone
					SeqNo:     2,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(100 * time.Second),
				},
			},
		},
		TakenAt: now,
		Health: []routing.RouteHealthState{
			// Health tier is Good — only the cooldown arm should drop it,
			// proving the filter keys on CooldownUntil, not the tier.
			{Identity: targetID, Uplink: domaintest.ID("uplink-cooled"), Health: routing.HealthGood, CooldownUntil: futureCooldown(now)},
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
		t.Fatalf("expected 1 route after cooldown filter, got %d: %v", len(routes), routes)
	}
	first, _ := routes[0].(map[string]interface{})
	if nh, _ := first["next_hop"].(string); nh != domaintest.ID("uplink-good").String() {
		t.Fatalf("cooled-down transit leaked into response: NextHop = %q, want uplink-good", nh)
	}
}

// TestFetchRouteLookupCooldownHasNoDirectExemption — unlike the Dead
// filter, the cooldown filter drops a Direct pair too. A Direct route
// inside an armed cooldown must NOT appear (contrast with
// TestFetchRouteLookupExemptsDirectFromDeadFilter).
func TestFetchRouteLookupCooldownHasNoDirectExemption(t *testing.T) {
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
					ExpiresAt: time.Time{},
				},
			},
		},
		TakenAt: now,
		Health: []routing.RouteHealthState{
			{Identity: targetID, Uplink: targetID, Health: routing.HealthGood, CooldownUntil: futureCooldown(now)},
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
	if len(routes) != 0 {
		t.Fatalf("cooled-down Direct pair must be filtered (no Direct exemption), got %d routes: %v", len(routes), routes)
	}
}

// TestFetchRouteSummaryExcludesCooledDownFromReachable — a destination
// whose only route is cooled-down must not be counted as reachable,
// matching the relay path which would not select it.
func TestFetchRouteSummaryExcludesCooledDownFromReachable(t *testing.T) {
	now := time.Now()
	cooledTarget := domain.PeerIdentityFromWire("bb22cc33dd44ee55ff66aa11bb22cc33dd44ee55")
	reachableTarget := domain.PeerIdentityFromWire("cc33dd44ee55ff66aa11bb22cc33dd44ee55ff66")
	provider := newMockRoutingProvider(t, routing.Snapshot{
		Routes: map[routing.PeerIdentity][]routing.RouteEntry{
			cooledTarget: {
				{
					Identity:  cooledTarget,
					Origin:    domaintest.ID("cooled"),
					NextHop:   domaintest.ID("uplink-cooled"),
					Hops:      2,
					SeqNo:     1,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(100 * time.Second),
				},
			},
			reachableTarget: {
				{
					Identity:  reachableTarget,
					Origin:    domaintest.ID("good"),
					NextHop:   domaintest.ID("uplink-good"),
					Hops:      2,
					SeqNo:     1,
					Source:    routing.RouteSourceAnnouncement,
					ExpiresAt: now.Add(100 * time.Second),
				},
			},
		},
		TakenAt: now,
		Health: []routing.RouteHealthState{
			{Identity: cooledTarget, Uplink: domaintest.ID("uplink-cooled"), Health: routing.HealthGood, CooldownUntil: futureCooldown(now)},
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
		t.Fatalf("unmarshal: %v", err)
	}
	// Only reachableTarget should be counted; the cooled-down target's
	// sole route is not selectable.
	reachable, _ := result["reachable_identities"].(float64)
	if int(reachable) != 1 {
		t.Fatalf("expected reachable_identities=1 (cooled-down target excluded), got %v", reachable)
	}
}
