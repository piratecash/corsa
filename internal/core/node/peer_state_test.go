package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"corsa/internal/core/domain"
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
	entries := []peerEntry{
		{Address: "low", Score: -10, LastConnectedAt: &now},
		{Address: "high", Score: 50, LastConnectedAt: &past1h},
		{Address: "mid", Score: 10, LastConnectedAt: &now},
		{Address: "mid-old", Score: 10, LastConnectedAt: &past2h},
	}

	sortPeerEntries(entries)

	if entries[0].Address != "high" {
		t.Fatalf("expected 'high' first, got %s", entries[0].Address)
	}
	if entries[1].Address != "mid" {
		t.Fatalf("expected 'mid' second (more recent), got %s", entries[1].Address)
	}
	if entries[2].Address != "mid-old" {
		t.Fatalf("expected 'mid-old' third, got %s", entries[2].Address)
	}
	if entries[3].Address != "low" {
		t.Fatalf("expected 'low' last, got %s", entries[3].Address)
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
