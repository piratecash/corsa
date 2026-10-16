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

// TestFetchRouteSummaryReportsSessionKinds pins the RPC route to the session
// kind counts: the decision to switch the legacy path off is taken from a
// separate process by comparing two readings, so the answer must carry every
// count, the period it belongs to with sub-second stamps, and values above
// 2^53 without float rounding.
func TestFetchRouteSummaryReportsSessionKinds(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 31, 44, 512837401, time.UTC)
	started := now.Add(-time.Hour)
	const above53 = uint64(1)<<60 + 3 // a float64 decode would round it

	provider := rpcmocks.NewMockRoutingProvider(t)
	expectRouteSummaryReads(provider, now)
	provider.On("SessionKindStats").Return(domain.SessionKindStats{
		StartedAt:                  started,
		ReadAt:                     now,
		InboundV2:                  above53,
		InboundLegacy:              2,
		OutboundV2:                 3,
		OutboundLegacy:             4,
		HellosBelowSecureVersion:   5,
		WelcomesBelowSecureVersion: 6,
		LegacyAtClaimedV2:          7,
		Live: domain.LiveSessionKinds{
			Ready:          true,
			UpdatedAt:      now,
			InboundV2:      domain.LiveSessionKindCount{Sessions: 9, Identities: 8},
			InboundLegacy:  domain.LiveSessionKindCount{Sessions: 2, Identities: 2},
			OutboundV2:     domain.LiveSessionKindCount{Sessions: 3, Identities: 3},
			OutboundLegacy: domain.LiveSessionKindCount{Sessions: 1, Identities: 1},
		},
	}).Once()

	provider.On("ListenerRefusalStats").Return(domain.ListenerRefusalStats{}).Once()

	section := routeSummarySection(t, provider, "session_kinds")

	for field, want := range map[string]string{
		"started_at": started.Format(time.RFC3339Nano),
		"read_at":    now.Format(time.RFC3339Nano),
	} {
		if got, _ := section[field].(string); got != want {
			t.Fatalf("session_kinds.%s = %v, want %q", field, section[field], want)
		}
	}
	for field, want := range map[string]uint64{
		"inbound_v2":                    above53,
		"inbound_legacy":                2,
		"outbound_v2":                   3,
		"outbound_legacy":               4,
		"hellos_below_secure_version":   5,
		"welcomes_below_secure_version": 6,
		"legacy_at_claimed_v2":          7,
	} {
		number, ok := section[field].(json.Number)
		if !ok {
			t.Fatalf("session_kinds.%s is not a number: %T", field, section[field])
		}
		if got := number.String(); got != jsonUint(want) {
			t.Fatalf("session_kinds.%s = %s, want %d", field, got, want)
		}
	}
	if len(section) != 10 {
		t.Fatalf("session_kinds has %d fields, want 10: %v", len(section), section)
	}

	live, ok := section["live"].(map[string]interface{})
	if !ok {
		t.Fatalf("session_kinds.live is not a JSON object: %T", section["live"])
	}
	if ready, _ := live["ready"].(bool); !ready {
		t.Fatalf("session_kinds.live.ready = %v, want true", live["ready"])
	}
	if got, _ := live["updated_at"].(string); got != now.Format(time.RFC3339Nano) {
		t.Fatalf("session_kinds.live.updated_at = %v, want %q", live["updated_at"], now.Format(time.RFC3339Nano))
	}
	for bucket, want := range map[string][2]string{
		"inbound_v2":      {"9", "8"},
		"inbound_legacy":  {"2", "2"},
		"outbound_v2":     {"3", "3"},
		"outbound_legacy": {"1", "1"},
	} {
		obj, ok := live[bucket].(map[string]interface{})
		if !ok {
			t.Fatalf("session_kinds.live.%s is not a JSON object: %T", bucket, live[bucket])
		}
		sessions, _ := obj["sessions"].(json.Number)
		identities, _ := obj["identities"].(json.Number)
		if sessions.String() != want[0] || identities.String() != want[1] {
			t.Fatalf("session_kinds.live.%s = sessions %s, identities %s; want %s, %s", bucket, sessions, identities, want[0], want[1])
		}
	}
}

// expectRouteSummaryReads expects every provider read of fetchRouteSummary
// except SessionKindStats, each with an empty value.
func expectRouteSummaryReads(provider *rpcmocks.MockRoutingProvider, now time.Time) {
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
	provider.On("TransportTrafficStats").Return(domain.TransportTrafficStats{}).Once()
}

// routeSummarySection runs fetchRouteSummary and returns one of its sections,
// numbers kept as json.Number.
func routeSummarySection(t *testing.T, provider rpc.RoutingProvider, name string) map[string]interface{} {
	t.Helper()
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
	section, ok := result[name].(map[string]interface{})
	if !ok {
		t.Fatalf("%s is not a JSON object: %T (%v)", name, result[name], result[name])
	}
	return section
}
