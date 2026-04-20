package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type peerEntry struct {
	Address             domain.PeerAddress `json:"address"`
	NodeType            domain.NodeType    `json:"node_type,omitempty"`
	Network             domain.NetGroup    `json:"network,omitempty"` // network group classification for this address
	LastConnectedAt     *time.Time         `json:"last_connected_at,omitempty"`
	LastDisconnectedAt  *time.Time         `json:"last_disconnected_at,omitempty"`
	ConsecutiveFailures int                `json:"consecutive_failures"`
	LastError           string             `json:"last_error,omitempty"`
	Source              domain.PeerSource  `json:"source"`                 // bootstrap, peer_exchange, manual, announce
	AddedAt             *time.Time         `json:"added_at,omitempty"`     // first time this address was seen
	Score               int                `json:"score"`                  // higher = better; decays on failure, grows on success
	BannedUntil         *time.Time         `json:"banned_until,omitempty"` // peer is not dialled until this time expires

	// Machine-readable version diagnostics — persisted so the operator-visible
	// peerHealthFrames() snapshot retains the exact evidence that created a
	// lockout across restarts. Without persistence these fields reset to zero
	// while the lockout itself survives, creating an information gap.
	LastErrorCode               string                 `json:"last_error_code,omitempty"`
	LastDisconnectCode          string                 `json:"last_disconnect_code,omitempty"`
	IncompatibleVersionAttempts domain.AttemptCount    `json:"incompatible_version_attempts,omitempty"`
	LastIncompatibleVersionAt   *time.Time             `json:"last_incompatible_version_at,omitempty"`
	ObservedPeerVersion         domain.ProtocolVersion `json:"observed_peer_version,omitempty"`
	ObservedPeerMinimumVersion  domain.ProtocolVersion `json:"observed_peer_minimum_version,omitempty"`

	// Version lockout: persisted context from a confirmed incompatible-version
	// rejection. When active, this peer is excluded from dial candidates until
	// the local version changes. Cleared on startup when LocalVersionFingerprint
	// differs from the running node (protocol version or client build increased).
	VersionLockout domain.VersionLockoutSnapshot `json:"version_lockout,omitempty"`
}

// bannedIPEntry is the on-disk representation of an IP-wide ban.
// Stored separately from per-peer entries because banned IPs must
// survive the top-500 trimming — a trimmed peer should not become
// dialable just because it fell off the peer list.
type bannedIPStateEntry struct {
	IP            string    `json:"ip"`
	BannedUntil   time.Time `json:"banned_until"`
	BanOrigin     string    `json:"ban_origin,omitempty"`
	BanReason     string    `json:"ban_reason,omitempty"`
	AffectedPeers []string  `json:"affected_peers,omitempty"` // addresses on this IP at ban time; survives top-500 trim
}

type peerStateFile struct {
	Version   int                  `json:"version"`
	UpdatedAt time.Time            `json:"updated_at"`
	Peers     []peerEntry          `json:"peers"`
	BannedIPs []bannedIPStateEntry `json:"banned_ips,omitempty"`
}

const (
	peerStateVersion             = 2              // v2: added banned_ips section with ban_origin/ban_reason
	peerScoreConnect             = 10             // awarded on successful TCP handshake
	peerScoreDisconnect          = -2             // applied on clean disconnect
	peerScoreFailure             = -5             // applied on dial/protocol failure
	peerScoreOldProtocol         = -50            // applied when peer protocol version is too old; pushes to bottom of dial list
	peerBanIncompatible          = 24 * time.Hour // ban duration for peers with incompatible protocol version
	peerBanIncrementIncompatible = 250            // overlay-level penalty per incompatible-version attempt
	peerBanThresholdIncompatible = 1000           // overlay penalty sum that triggers the timed ban
	peerScoreMax                 = 100
	peerScoreMin                 = -50
	maxPersistedPeers            = 500
	peerStateSaveMinutes         = 5                // minimum interval between periodic saves
	peerCooldownBase             = 30 * time.Second // base cooldown after first failure
	peerCooldownMax              = 30 * time.Minute // cap on exponential backoff

	// Eviction thresholds.
	// A peer is evictable when its score drops below the threshold AND it has
	// not been successfully connected for longer than the stale window.
	peerEvictScoreThreshold = -20              // score at or below this → candidate for eviction
	peerEvictStaleWindow    = 24 * time.Hour   // must also be unseen for this long
	peerEvictInterval       = 10 * time.Minute // how often the eviction sweep runs

	// Orphaned health entries — inbound-only peers not present in s.peers.
	// These accumulate from ephemeral inbound connections (e.g.
	// 127.0.0.1:<random_port>) that connected once, disconnected, and will
	// never be dialled. A shorter window than peerEvictStaleWindow is used
	// because there is no outbound dial target to preserve.
	orphanedHealthEvictWindow = 10 * time.Minute
)

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

// sortPeerEntries orders entries the same way PeerProvider.Candidates()
// does: score descending → AddedAt ascending (older peers first) →
// address lexicographic. This ensures that the top-500 trim evicts the
// same peers the dialer would have ranked lowest, making persistence
// deterministic relative to the live candidate order.
func sortPeerEntries(entries []peerEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}
		ai := peerTime(entries[i].AddedAt)
		aj := peerTime(entries[j].AddedAt)
		if !ai.Equal(aj) {
			return ai.Before(aj)
		}
		return entries[i].Address < entries[j].Address
	})
}

func peerTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// trimPeerEntries caps the persisted peer list at maxPersistedPeers.
// Entries with an active VersionLockout are always retained regardless
// of score — without this, a low-scoring locked-out peer would be
// discarded by the top-N trim and become dialable again after restart,
// defeating the purpose of the lockout. The remaining budget is filled
// with the highest-scored non-lockout entries (already sorted by
// sortPeerEntries). This mirrors the separate persistence bucket for
// IP-wide bans (bannedIPs section in peerStateFile).
func trimPeerEntries(entries []peerEntry) []peerEntry {
	if len(entries) <= maxPersistedPeers {
		return entries
	}

	// Partition: locked-out entries are always kept.
	var locked, rest []peerEntry
	for _, e := range entries {
		if e.VersionLockout.IsActive() {
			locked = append(locked, e)
		} else {
			rest = append(rest, e)
		}
	}

	// If locked entries alone exceed the budget, keep all of them
	// (this is an extreme edge case — it would require 500+ distinct
	// incompatible peers, which is operationally implausible).
	budget := maxPersistedPeers - len(locked)
	if budget <= 0 {
		return locked
	}

	if len(rest) > budget {
		rest = rest[:budget]
	}
	return append(locked, rest...)
}
