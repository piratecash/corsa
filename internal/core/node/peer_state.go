package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// peerEntry represents a persisted peer address with metadata and scoring.
// Each entry carries enough context to prioritise dial candidates on restart
// without a full peer-exchange round.
type peerEntry struct {
	Address             string     `json:"address"`
	NodeType            string     `json:"node_type,omitempty"`
	LastConnectedAt     *time.Time `json:"last_connected_at,omitempty"`
	LastDisconnectedAt  *time.Time `json:"last_disconnected_at,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastError           string     `json:"last_error,omitempty"`
	Source              string     `json:"source"`            // "bootstrap", "peer_exchange", "manual"
	AddedAt             *time.Time `json:"added_at,omitempty"` // first time this address was seen
	Score               int        `json:"score"`             // higher = better; decays on failure, grows on success
}

type peerStateFile struct {
	Version   int         `json:"version"`
	UpdatedAt time.Time   `json:"updated_at"`
	Peers     []peerEntry `json:"peers"`
}

const (
	peerStateVersion     = 1
	peerScoreConnect     = 10 // awarded on successful TCP handshake
	peerScoreDisconnect  = -2 // applied on clean disconnect
	peerScoreFailure     = -5 // applied on dial/protocol failure
	peerScoreMax         = 100
	peerScoreMin         = -50
	maxPersistedPeers    = 500
	peerStateSaveMinutes = 5  // minimum interval between periodic saves
	peerCooldownBase     = 30 * time.Second // base cooldown after first failure
	peerCooldownMax      = 30 * time.Minute // cap on exponential backoff

	// Eviction thresholds.
	// A peer is evictable when its score drops below the threshold AND it has
	// not been successfully connected for longer than the stale window.
	peerEvictScoreThreshold = -20           // score at or below this → candidate for eviction
	peerEvictStaleWindow    = 24 * time.Hour // must also be unseen for this long
	peerEvictInterval       = 10 * time.Minute // how often the eviction sweep runs
)

// peerCooldownDuration returns the exponential backoff duration for a peer
// based on its consecutive failure count: min(base * 2^(failures-1), max).
// Returns zero if failures <= 0 (no cooldown).
func peerCooldownDuration(consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		return 0
	}
	d := peerCooldownBase
	for i := 1; i < consecutiveFailures; i++ {
		d *= 2
		if d >= peerCooldownMax {
			return peerCooldownMax
		}
	}
	return d
}

func loadPeerState(path string) (peerStateFile, error) {
	state := peerStateFile{
		Version: peerStateVersion,
		Peers:   []peerEntry{},
	}
	if path == "" {
		return state, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, fmt.Errorf("read peer state %s: %w", path, err)
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode peer state %s: %w", path, err)
	}
	if state.Peers == nil {
		state.Peers = []peerEntry{}
	}
	return state, nil
}

func savePeerState(path string, state peerStateFile) error {
	if path == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create peer state directory: %w", err)
	}

	state.UpdatedAt = time.Now().UTC()
	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal peer state: %w", err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		return fmt.Errorf("write peer state: %w", err)
	}
	return nil
}

// clampScore keeps peer score within [peerScoreMin, peerScoreMax].
func clampScore(score int) int {
	if score > peerScoreMax {
		return peerScoreMax
	}
	if score < peerScoreMin {
		return peerScoreMin
	}
	return score
}

// sortPeerEntries orders peers by score descending, then by last-connected
// time descending so that recently-successful peers are dialled first.
func sortPeerEntries(entries []peerEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}
		ti := peerTime(entries[i].LastConnectedAt)
		tj := peerTime(entries[j].LastConnectedAt)
		return ti.After(tj)
	})
}

// peerTime returns the time value or zero time if the pointer is nil.
func peerTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// trimPeerEntries caps the persisted list at maxPersistedPeers, keeping
// the highest-scoring entries. Entries must be pre-sorted.
func trimPeerEntries(entries []peerEntry) []peerEntry {
	if len(entries) <= maxPersistedPeers {
		return entries
	}
	return entries[:maxPersistedPeers]
}
