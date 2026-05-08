package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestLoadPeerStateReturnsEmptyForMissingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing-peers.json")
	state, err := loadPeerState(path)
	if err != nil {
		t.Fatalf("loadPeerState missing file: %v", err)
	}
	if len(state.Peers) != 0 {
		t.Fatalf("expected empty peers, got %d", len(state.Peers))
	}
	if state.Version != peerStateVersion {
		t.Fatalf("expected version %d, got %d", peerStateVersion, state.Version)
	}
}

func TestLoadPeerStateReturnsEmptyForEmptyPath(t *testing.T) {
	t.Parallel()

	state, err := loadPeerState("")
	if err != nil {
		t.Fatalf("loadPeerState empty path: %v", err)
	}
	if len(state.Peers) != 0 {
		t.Fatalf("expected empty peers, got %d", len(state.Peers))
	}
}

func TestSaveAndLoadPeerStateRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "peers.json")
	now := time.Now().UTC().Truncate(time.Second)
	past5 := now.Add(-5 * time.Minute)
	past10 := now.Add(-10 * time.Minute)
	past1h := now.Add(-1 * time.Hour)
	want := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{
				Address:             "198.51.100.1:64646",
				NodeType:            domain.NodeTypeFull,
				LastConnectedAt:     &now,
				LastDisconnectedAt:  &past5,
				ConsecutiveFailures: 0,
				Source:              domain.PeerSourceBootstrap,
				AddedAt:             &now,
				Score:               10,
			},
			{
				Address:             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.onion:64646",
				NodeType:            domain.NodeTypeFull,
				LastConnectedAt:     &past10,
				ConsecutiveFailures: 2,
				LastError:           "connection refused",
				Source:              domain.PeerSourcePeerExchange,
				AddedAt:             &past1h,
				Score:               -5,
			},
		},
	}

	if err := savePeerState(path, want); err != nil {
		t.Fatalf("savePeerState: %v", err)
	}

	got, err := loadPeerState(path)
	if err != nil {
		t.Fatalf("loadPeerState: %v", err)
	}
	if len(got.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(got.Peers))
	}
	if got.Peers[0].Address != "198.51.100.1:64646" {
		t.Fatalf("unexpected first peer address: %s", got.Peers[0].Address)
	}
	if got.Peers[0].Score != 10 {
		t.Fatalf("expected score 10, got %d", got.Peers[0].Score)
	}
	if got.Peers[1].Address != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.onion:64646" {
		t.Fatalf("unexpected second peer address: %s", got.Peers[1].Address)
	}
	if got.Peers[1].ConsecutiveFailures != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", got.Peers[1].ConsecutiveFailures)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("expected non-zero UpdatedAt")
	}
}

func TestLoadPeerStateRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "peers.json")
	if err := os.WriteFile(path, []byte("{broken"), 0o600); err != nil {
		t.Fatalf("write invalid file: %v", err)
	}

	if _, err := loadPeerState(path); err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestLoadPeerStateHandlesNullPeers(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "peers.json")
	data, _ := json.Marshal(peerStateFile{Version: 1, Peers: nil})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	state, err := loadPeerState(path)
	if err != nil {
		t.Fatalf("loadPeerState: %v", err)
	}
	if state.Peers == nil {
		t.Fatal("expected non-nil Peers slice")
	}
}

// TestLoadPeerStateDropsLegacyV12CleanupFields pins the cleanup-migration
// contract for the v12 advertise-address phase 2 cleanup: a pre-v12
// peers.json that still carries the removed peerEntry fields
// (advertise_mismatch_count, forgivable_misadvertise_points,
// misadvertise_points_repaid) must load without error and the next save
// must drop those keys silently. The migration is implemented as
// "encoding/json ignores unknown keys on Unmarshal + omitempty drops
// the absent struct field on Marshal", so no explicit upgrade code is
// required, but the contract is a guarantee for operators rolling
// forward and must not regress.
func TestLoadPeerStateDropsLegacyV12CleanupFields(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "peers.json")

	// Pre-v12 layout: known peerEntry fields plus the three legacy
	// keys that were removed in the v12 cleanup phase. Hand-rolled
	// JSON so the test does not depend on a struct literal that
	// matches the new shape.
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	legacyJSON := `{
		"version": 2,
		"updated_at": "` + now + `",
		"peers": [
			{
				"address": "203.0.113.50:64646",
				"score": 10,
				"source": "peer_exchange",
				"announce_state": "announceable",
				"trusted_advertise_ip": "203.0.113.50",
				"trusted_advertise_source": "inbound_confirmed",
				"trusted_advertise_port": "64646",
				"advertise_mismatch_count": 7,
				"forgivable_misadvertise_points": 250,
				"misadvertise_points_repaid": 100,
				"unrecognised_extension_key": "should also pass through silently"
			}
		]
	}`
	if err := os.WriteFile(path, []byte(legacyJSON), 0o600); err != nil {
		t.Fatalf("write legacy peers.json: %v", err)
	}

	state, err := loadPeerState(path)
	if err != nil {
		t.Fatalf("loadPeerState must accept pre-v12 layout silently, got %v", err)
	}
	if len(state.Peers) != 1 {
		t.Fatalf("expected one peer to survive load, got %d", len(state.Peers))
	}
	pm := state.Peers[0]
	if pm.Address != "203.0.113.50:64646" {
		t.Fatalf("address: got %q want %q", pm.Address, "203.0.113.50:64646")
	}
	if pm.Score != 10 {
		t.Fatalf("score: got %d want 10", pm.Score)
	}
	if pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("announce_state: got %q want %q", pm.AnnounceState, announceStateAnnounceable)
	}
	if pm.TrustedAdvertiseIP != "203.0.113.50" {
		t.Fatalf("trusted_advertise_ip: got %q want %q", pm.TrustedAdvertiseIP, "203.0.113.50")
	}
	if pm.TrustedAdvertiseSource != trustedAdvertiseSourceInbound {
		t.Fatalf("trusted_advertise_source: got %q want %q", pm.TrustedAdvertiseSource, trustedAdvertiseSourceInbound)
	}
	if pm.TrustedAdvertisePort != "64646" {
		t.Fatalf("trusted_advertise_port: got %q want %q", pm.TrustedAdvertisePort, "64646")
	}

	// Round-trip: save the loaded state and assert the rewritten file
	// contains none of the removed legacy keys. omitempty + the absent
	// struct field together drop them silently — without an explicit
	// migration step.
	rewrittenPath := filepath.Join(t.TempDir(), "peers-rewritten.json")
	if err := savePeerState(rewrittenPath, state); err != nil {
		t.Fatalf("savePeerState: %v", err)
	}
	rewritten, err := os.ReadFile(rewrittenPath)
	if err != nil {
		t.Fatalf("read rewritten peers.json: %v", err)
	}
	deprecatedKeys := []string{
		`"advertise_mismatch_count"`,
		`"forgivable_misadvertise_points"`,
		`"misadvertise_points_repaid"`,
	}
	for _, key := range deprecatedKeys {
		if bytesContains(rewritten, []byte(key)) {
			t.Fatalf("rewritten peers.json must not carry %s, got %s", key, rewritten)
		}
	}

	// Sanity: the surviving keys must still be present after the
	// round-trip so the test fails on a regression that drops too
	// much rather than too little.
	survivingKeys := []string{
		`"address"`,
		`"announce_state"`,
		`"trusted_advertise_ip"`,
		`"trusted_advertise_source"`,
		`"trusted_advertise_port"`,
	}
	for _, key := range survivingKeys {
		if !bytesContains(rewritten, []byte(key)) {
			t.Fatalf("rewritten peers.json must still carry %s, got %s", key, rewritten)
		}
	}
}

func TestSavePeerStateNoopForEmptyPath(t *testing.T) {
	t.Parallel()

	err := savePeerState("", peerStateFile{Peers: []peerEntry{{Address: "1.2.3.4:64646", Source: domain.PeerSourcePeerExchange}}})
	if err != nil {
		t.Fatalf("expected nil error for empty path, got: %v", err)
	}
}

func TestSavePeerStateCreatesDirectory(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "subdir", "peers.json")
	err := savePeerState(path, peerStateFile{Version: 1, Peers: []peerEntry{}})
	if err != nil {
		t.Fatalf("savePeerState: %v", err)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected file to exist")
	}
}

func TestClampScore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input, want int
	}{
		{0, 0},
		{50, 50},
		{peerScoreMax, peerScoreMax},
		{peerScoreMax + 1, peerScoreMax},
		{200, peerScoreMax},
		{peerScoreMin, peerScoreMin},
		{peerScoreMin - 1, peerScoreMin},
		{-200, peerScoreMin},
	}
	for _, tt := range tests {
		if got := clampScore(tt.input); got != tt.want {
			t.Errorf("clampScore(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestSortPeerEntries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	past1h := now.Add(-1 * time.Hour)
	past2h := now.Add(-2 * time.Hour)

	// Sorting matches PeerProvider.Candidates() ordering:
	// score descending → AddedAt ascending (older first) → address lexicographic.
	entries := []peerEntry{
		{Address: "low:1", Score: -10, AddedAt: &now},
		{Address: "high:1", Score: 50, AddedAt: &past1h},
		{Address: "mid-new:1", Score: 10, AddedAt: &now},
		{Address: "mid-old:1", Score: 10, AddedAt: &past2h},
		{Address: "mid-old:2", Score: 10, AddedAt: &past2h}, // same score + AddedAt → address tiebreak
	}

	sortPeerEntries(entries)

	expected := []domain.PeerAddress{"high:1", "mid-old:1", "mid-old:2", "mid-new:1", "low:1"}
	for i, want := range expected {
		if entries[i].Address != want {
			t.Fatalf("entries[%d]: expected %q, got %q", i, want, entries[i].Address)
		}
	}
}

func TestTrimPeerEntries(t *testing.T) {
	t.Parallel()

	entries := make([]peerEntry, maxPersistedPeers+50)
	for i := range entries {
		entries[i] = peerEntry{Address: domain.PeerAddress(fmt.Sprintf("peer-%d", i)), Score: maxPersistedPeers + 50 - i}
	}
	sortPeerEntries(entries)
	trimmed := trimPeerEntries(entries)
	if len(trimmed) != maxPersistedPeers {
		t.Fatalf("expected %d entries, got %d", maxPersistedPeers, len(trimmed))
	}
	// The highest-score entry should be first.
	if trimmed[0].Score < trimmed[len(trimmed)-1].Score {
		t.Fatal("expected entries sorted by score descending")
	}
}

func TestTrimPeerEntriesNoopWhenUnderLimit(t *testing.T) {
	t.Parallel()

	entries := []peerEntry{
		{Address: "a", Score: 10},
		{Address: "b", Score: 5},
	}
	trimmed := trimPeerEntries(entries)
	if len(trimmed) != 2 {
		t.Fatalf("expected 2, got %d", len(trimmed))
	}
}

func TestPeerSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id   string
		want domain.PeerSource
	}{
		{"bootstrap-0", domain.PeerSourceBootstrap},
		{"bootstrap-12", domain.PeerSourceBootstrap},
		{"persisted-0", domain.PeerSourcePersisted},
		{"persisted-99", domain.PeerSourcePersisted},
		{"peer-5", domain.PeerSourcePeerExchange},
		{"", domain.PeerSourcePeerExchange},
	}
	for _, tt := range tests {
		if got := peerSource(tt.id); got != tt.want {
			t.Errorf("peerSource(%q) = %q, want %q", tt.id, got, tt.want)
		}
	}
}

func TestPeerCooldownDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failures int
		want     time.Duration
	}{
		{0, 0},
		{-1, 0},
		{1, peerCooldownBase},     // 30s
		{2, peerCooldownBase * 2}, // 60s
		{3, peerCooldownBase * 4}, // 2m
		{4, peerCooldownBase * 8}, // 4m
		{100, peerCooldownMax},    // capped at 30m
	}
	for _, tt := range tests {
		got := peerCooldownDuration(tt.failures)
		if got != tt.want {
			t.Errorf("peerCooldownDuration(%d) = %v, want %v", tt.failures, got, tt.want)
		}
	}
}
