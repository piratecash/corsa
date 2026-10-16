package rpc_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// TestFetchRouteSummaryReportsListenerRefusals pins the RPC route to the
// listener's refusal counters: an operator reads from a separate process which
// bound refuses inbound connections and whether the alert pool keeps up, by
// comparing two readings with the same started_at.
func TestFetchRouteSummaryReportsListenerRefusals(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 31, 44, 512837401, time.UTC)
	started := now.Add(-time.Hour)
	const above53 = uint64(1)<<60 + 5 // a float64 decode would round it

	provider := rpcmocks.NewMockRoutingProvider(t)
	expectRouteSummaryReads(provider, now)
	provider.On("SessionKindStats").Return(domain.SessionKindStats{}).Once()
	provider.On("ListenerRefusalStats").Return(domain.ListenerRefusalStats{
		StartedAt:              started,
		ReadAt:                 now,
		ConnectionRate:         above53,
		ConnectionsPerIP:       2,
		ConnectionBudget:       3,
		Blacklisted:            4,
		InboundLimit:           5,
		HandshakesNode:         6,
		HandshakesPerIP:        7,
		HandshakesLoopback:     9,
		LoopbackConnectionRate: 10,
		LoopbackConnections:    11,
		ClosedWithoutAlert:     8,
	}).Once()
	provider.On("LoopbackInboundStats").Return(domain.LoopbackInboundStats{}).Once()

	section := routeSummarySection(t, provider, "listener_refusals")

	for field, want := range map[string]string{
		"started_at": started.Format(time.RFC3339Nano),
		"read_at":    now.Format(time.RFC3339Nano),
	} {
		if got, _ := section[field].(string); got != want {
			t.Fatalf("listener_refusals.%s = %v, want %q", field, section[field], want)
		}
	}
	for field, want := range map[string]uint64{
		"connection_rate":           above53,
		"connections_per_ip":        2,
		"connection_budget":         3,
		"blacklisted":               4,
		"inbound_limit":             5,
		"handshakes_node_limit":     6,
		"handshakes_ip_limit":       7,
		"handshakes_loopback_limit": 9,
		"loopback_connection_rate":  10,
		"loopback_active_limit":     11,
		"closed_without_alert":      8,
	} {
		number, ok := section[field].(json.Number)
		if !ok {
			t.Fatalf("listener_refusals.%s is not a number: %T", field, section[field])
		}
		if got := number.String(); got != jsonUint(want) {
			t.Fatalf("listener_refusals.%s = %s, want %d", field, got, want)
		}
	}
	if len(section) != 13 {
		t.Fatalf("listener_refusals has %d fields, want 13: %v", len(section), section)
	}
}

// TestFetchRouteSummaryReportsTheLoopbackInboundClass pins the RPC route to the
// loopback inbound class: its bounds as the node enforces them, what it holds
// now and what it refused or closed, by cause.
func TestFetchRouteSummaryReportsTheLoopbackInboundClass(t *testing.T) {
	now := time.Date(2026, 10, 16, 9, 31, 44, 512837401, time.UTC)
	started := now.Add(-time.Hour)
	const above53 = uint64(1)<<60 + 5 // a float64 decode would round it

	provider := rpcmocks.NewMockRoutingProvider(t)
	expectRouteSummaryReads(provider, now)
	provider.On("SessionKindStats").Return(domain.SessionKindStats{}).Once()
	provider.On("ListenerRefusalStats").Return(domain.ListenerRefusalStats{}).Once()
	provider.On("LoopbackInboundStats").Return(domain.LoopbackInboundStats{
		StartedAt: started,
		ReadAt:    now,
		Bounds: domain.LoopbackInboundBounds{
			Enforced:        true,
			MaxActive:       64,
			MaxNewPerWindow: 100,
			Window:          10 * time.Second,
			MaxHandshakes:   16,
		},
		Active:             12,
		HandshakesInFlight: 3,
		Admitted:           above53,
		Refusals: domain.LoopbackInboundRefusals{
			ConnectionRate:     1,
			ActiveLimit:        2,
			ConnectionBudget:   3,
			InboundLimit:       4,
			HandshakesLoopback: 5,
			HandshakesNode:     6,
			ViolationsClosed:   7,
			IdentityBanned:     8,
		},
		IdentitiesScored: 9,
		IdentitiesBanned: 10,
	}).Once()

	section := routeSummarySection(t, provider, "loopback_inbound")

	for field, want := range map[string]string{
		"started_at":    started.Format(time.RFC3339Nano),
		"read_at":       now.Format(time.RFC3339Nano),
		"classified_by": "source_address",
	} {
		if got, _ := section[field].(string); got != want {
			t.Fatalf("loopback_inbound.%s = %v, want %q", field, section[field], want)
		}
	}
	if enforced, _ := section["enforced"].(bool); !enforced {
		t.Fatalf("loopback_inbound.enforced = %v, want true", section["enforced"])
	}
	numbers := func(object map[string]interface{}, name string, want map[string]uint64) {
		t.Helper()
		for field, value := range want {
			number, ok := object[field].(json.Number)
			if !ok {
				t.Fatalf("%s.%s is not a number: %T", name, field, object[field])
			}
			if got := number.String(); got != jsonUint(value) {
				t.Fatalf("%s.%s = %s, want %d", name, field, got, value)
			}
		}
		if len(object) < len(want) {
			t.Fatalf("%s has %d fields, want at least %d: %v", name, len(object), len(want), object)
		}
	}
	numbers(section, "loopback_inbound", map[string]uint64{
		"max_active":           64,
		"max_new_per_window":   100,
		"window_seconds":       10,
		"max_handshakes":       16,
		"active":               12,
		"handshakes_in_flight": 3,
		"admitted":             above53,
		"identities_scored":    9,
		"identities_banned":    10,
	})
	refusals, ok := section["refusals"].(map[string]interface{})
	if !ok {
		t.Fatalf("loopback_inbound.refusals is not an object: %T", section["refusals"])
	}
	numbers(refusals, "loopback_inbound.refusals", map[string]uint64{
		"connection_rate":           1,
		"active_limit":              2,
		"connection_budget":         3,
		"inbound_limit":             4,
		"handshakes_loopback_limit": 5,
		"handshakes_node_limit":     6,
		"violations_closed":         7,
		"identity_banned":           8,
	})
	if len(refusals) != 8 {
		t.Fatalf("loopback_inbound.refusals has %d fields, want 8: %v", len(refusals), refusals)
	}
	if len(section) != 14 {
		t.Fatalf("loopback_inbound has %d fields, want 14: %v", len(section), section)
	}
}
