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
		StartedAt:          started,
		ReadAt:             now,
		ConnectionRate:     above53,
		ConnectionsPerIP:   2,
		ConnectionBudget:   3,
		Blacklisted:        4,
		InboundLimit:       5,
		HandshakesNode:     6,
		HandshakesPerIP:    7,
		HandshakesLoopback: 9,
		ClosedWithoutAlert: 8,
	}).Once()

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
	if len(section) != 11 {
		t.Fatalf("listener_refusals has %d fields, want 11: %v", len(section), section)
	}
}
