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
	provider.On("SessionKindStats").Return(domain.SessionKindStats{}).Once()
	provider.On("ListenerRefusalStats").Return(domain.ListenerRefusalStats{}).Once()
	provider.On("LoopbackInboundStats").Return(domain.LoopbackInboundStats{}).Once()

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
	provider.On("SessionKindStats").Return(domain.SessionKindStats{}).Once()
	provider.On("ListenerRefusalStats").Return(domain.ListenerRefusalStats{}).Once()
	provider.On("LoopbackInboundStats").Return(domain.LoopbackInboundStats{}).Once()
	provider.On("SecureSessionStoreStats").Return(domain.SecureSessionStoreStats{
		ReadAt:                              now,
		PinnedIdentities:                    20000,
		SelectedAllocations:                 4097,
		SelectedConfirmed:                   100,
		SelectedCapacity:                    4096,
		SelectedOverCapacity:                true,
		SelectedAccountingAvailable:         true,
		PinCapacity:                         20000,
		Full:                                true,
		PinRefusalsStoreFull:                7,
		V1RefusalsIdentityPinned:            3,
		V1RefusalsEndpointBound:             5,
		V1DialsHeldByRefusalBackoff:         11,
		DialsHeldByV2RetryBackoff:           13,
		ProtectedBindingsContact:            8192,
		ProtectedBindingsRelation:           24576,
		ProtectedCapacity:                   32768,
		ProtectedReserveContact:             8192,
		ProtectedPerIdentityCapacity:        4,
		GeneralBindings:                     100,
		GeneralCapacity:                     16384,
		ProtectedRefusalsPerIdentity:        1,
		ProtectedRefusalsPoolRFull:          2,
		ProtectedRefusalsStoreFull:          3,
		GeneralCreationsPerGroupPerDay:      64,
		GeneralRefusalsSourceGroupQuota:     17,
		EndpointBindingsSkippedNotCanonical: 19,
		NonCanonicalBindings:                23,
		LocationUnsafe:                      true,
		ClockJumpGuarded:                    true,
		ClockJumpsDetected:                  29,
		ClockJumpActive:                     true,
		TLSFirstDisabled:                    true,
		DialsOpenedByProbe:                  31,
		V1RefusalsAfterV2Welcome:            37,
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
	if section["pin_store_full"] != true || section["store_unreadable"] != false || section["pin_store_unsafe"] != true {
		t.Fatalf("flags = full %v, unreadable %v, unsafe %v", section["pin_store_full"], section["store_unreadable"], section["pin_store_unsafe"])
	}
	if section["selected_over_capacity"] != true || section["selected_accounting_available"] != true {
		t.Fatalf("selected diagnostics flags: %v", section)
	}
	if section["v2_tls_first_disabled"] != true {
		t.Fatalf("v2_tls_first_disabled = %v, want true", section["v2_tls_first_disabled"])
	}
	if section["clock_jump_guarded"] != true || section["clock_jump_active"] != true {
		t.Fatalf("clock flags = guarded %v, active %v", section["clock_jump_guarded"], section["clock_jump_active"])
	}
	for field, want := range map[string]string{
		"pinned_identities":                       "20000",
		"selected_allocations":                    "4097",
		"selected_confirmed":                      "100",
		"selected_capacity":                       "4096",
		"pin_capacity":                            "20000",
		"pin_refusals_pin_store_full":             "7",
		"v1_refusals_identity_pinned":             "3",
		"v1_refusals_endpoint_bound":              "5",
		"v1_dials_held_by_refusal_backoff":        "11",
		"dials_held_by_v2_retry_backoff":          "13",
		"protected_bindings_contact":              "8192",
		"protected_bindings_relation":             "24576",
		"protected_capacity":                      "32768",
		"protected_reserve_contact":               "8192",
		"protected_per_identity_capacity":         "4",
		"general_bindings":                        "100",
		"general_capacity":                        "16384",
		"protected_refusals_per_identity":         "1",
		"protected_refusals_pool_r_full":          "2",
		"protected_refusals_store_full":           "3",
		"general_creations_per_group_per_day":     "64",
		"general_refusals_source_group_quota":     "17",
		"endpoint_bindings_skipped_not_canonical": "19",
		"non_canonical_bindings":                  "23",
		"clock_jumps_detected":                    "29",
		"dials_opened_by_probe":                   "31",
		"v1_refusals_after_v2_welcome":            "37",
	} {
		if got, _ := section[field].(json.Number); got.String() != want {
			t.Fatalf("secure_sessions.%s = %v, want %s", field, section[field], want)
		}
	}
	if got, _ := section["read_at"].(string); got != now.Format(time.RFC3339Nano) {
		t.Fatalf("secure_sessions.read_at = %v", section["read_at"])
	}
}
