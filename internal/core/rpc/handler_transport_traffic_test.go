package rpc_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// TestFetchRouteSummaryReportsTransportTraffic pins the RPC route to the
// transport byte counters. They exist for measurements taken from a SEPARATE
// process, which cannot send the in-process fetch_traffic_totals frame — so
// the RPC answer must carry the counts, the period they belong to with
// sub-second stamps, and values above 2^53 without float rounding.
func TestFetchRouteSummaryReportsTransportTraffic(t *testing.T) {
	now := time.Date(2026, 10, 2, 9, 31, 44, 512837401, time.UTC)
	started := now.Add(-time.Hour)
	const sent = uint64(1)<<60 + 3 // above 2^53: a float64 decode would round it
	const received = uint64(42)

	provider := rpcmocks.NewMockRoutingProvider(t)
	provider.On("RoutingSnapshot").Return(routing.Snapshot{
		TakenAt: now.Add(-time.Minute),
		Routes:  map[routing.PeerIdentity][]routing.RouteEntry{},
	}).Once()
	provider.On("OverloadStats").Return(routing.OverloadStats{}).Once()
	provider.On("DigestHeartbeatStats").Return(routing.DigestHeartbeatStats{}).Once()
	provider.On("JournalCauseStats").Return(map[string]uint64(nil)).Once()
	provider.On("ModeSelectionStats").Return(routing.ModeSelectionStats{}).Once()
	provider.On("SessionOutcomeStats").Return(domain.SessionOutcomeStats{}).Once()
	provider.On("NeighbourComposition").Return(domain.NeighbourComposition{}).Once()
	provider.On("SecureSessionStoreStats").Return(domain.SecureSessionStoreStats{}).Once()
	provider.On("TransportTrafficStats").Return(domain.TransportTrafficStats{
		StartedAt:     started,
		ReadAt:        now,
		BytesSent:     sent,
		BytesReceived: received,
	}).Once()

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)

	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteSummary"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	decoder := json.NewDecoder(bytes.NewReader(resp.Data))
	decoder.UseNumber()
	var result map[string]interface{}
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	section, ok := result["transport_traffic"].(map[string]interface{})
	if !ok {
		t.Fatalf("transport_traffic is not a JSON object: %T (%v)", result["transport_traffic"], result["transport_traffic"])
	}

	for field, want := range map[string]string{
		"started_at": started.Format(time.RFC3339Nano),
		"read_at":    now.Format(time.RFC3339Nano),
	} {
		if got, _ := section[field].(string); got != want {
			t.Fatalf("transport_traffic.%s = %v, want %q", field, section[field], want)
		}
	}
	for field, want := range map[string]uint64{
		"bytes_sent":     sent,
		"bytes_received": received,
	} {
		number, ok := section[field].(json.Number)
		if !ok {
			t.Fatalf("transport_traffic.%s is not a number: %T", field, section[field])
		}
		if got := number.String(); got != jsonUint(want) {
			t.Fatalf("transport_traffic.%s = %s, want %d", field, got, want)
		}
	}
}

func jsonUint(value uint64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// TestFetchRouteSummaryReportsThePinStore pins the diagnostics of a full pin
// store: a v2 session of a new identity is then refused with pin_store_full,
// and the operator has to be able to see that — the bound, how full it is and
// how many sessions it refused — without reading logs.
func TestFetchRouteSummaryReportsThePinStore(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	provider := rpcmocks.NewMockRoutingProvider(t)
	provider.On("RoutingSnapshot").Return(routing.Snapshot{
		TakenAt: now.Add(-time.Minute),
		Routes:  map[routing.PeerIdentity][]routing.RouteEntry{},
	}).Once()
	provider.On("OverloadStats").Return(routing.OverloadStats{}).Once()
	provider.On("DigestHeartbeatStats").Return(routing.DigestHeartbeatStats{}).Once()
	provider.On("JournalCauseStats").Return(map[string]uint64(nil)).Once()
	provider.On("ModeSelectionStats").Return(routing.ModeSelectionStats{}).Once()
	provider.On("SessionOutcomeStats").Return(domain.SessionOutcomeStats{}).Once()
	provider.On("NeighbourComposition").Return(domain.NeighbourComposition{}).Once()
	provider.On("TransportTrafficStats").Return(domain.TransportTrafficStats{}).Once()
	provider.On("SecureSessionStoreStats").Return(domain.SecureSessionStoreStats{
		ReadAt:               now,
		PinnedIdentities:     20000,
		PinCapacity:          20000,
		Full:                 true,
		PinRefusalsStoreFull: 7,
	}).Once()

	table := rpc.NewCommandTable()
	rpc.RegisterRoutingCommands(table, provider)
	resp := table.Execute(rpc.CommandRequest{Name: "fetchRouteSummary"})
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	decoder := json.NewDecoder(bytes.NewReader(resp.Data))
	decoder.UseNumber()
	var result map[string]interface{}
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	section, ok := result["secure_sessions"].(map[string]interface{})
	if !ok {
		t.Fatalf("secure_sessions is not a JSON object: %T (%v)", result["secure_sessions"], result["secure_sessions"])
	}
	if section["pin_store_full"] != true || section["store_unreadable"] != false {
		t.Fatalf("flags = full %v, unreadable %v", section["pin_store_full"], section["store_unreadable"])
	}
	for field, want := range map[string]string{
		"pinned_identities":           "20000",
		"pin_capacity":                "20000",
		"pin_refusals_pin_store_full": "7",
	} {
		if got, _ := section[field].(json.Number); got.String() != want {
			t.Fatalf("secure_sessions.%s = %v, want %s", field, section[field], want)
		}
	}
	if got, _ := section["read_at"].(string); got != now.Format(time.RFC3339Nano) {
		t.Fatalf("secure_sessions.read_at = %v", section["read_at"])
	}
}
