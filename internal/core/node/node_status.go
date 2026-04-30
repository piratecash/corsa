package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// NodeStatus returns the PIP-0001 integration snapshot used by the
// getNodeStatus RPC endpoint. Captures identity, public key material,
// protocol/version, uptime, and the live distinct-peer count.
//
// Locking contract: identity / startedAt / cfg are immutable after
// construction; the connected-peer count is sourced from the
// pre-built peerHealthFrames atomic snapshot, so this method takes
// NO domain mutex. Safe to call from any goroutine without ordering
// concerns.
func (s *Service) NodeStatus() domain.NodeStatus {
	now := time.Now().UTC()

	// Truncate uptime to whole seconds. Sub-second jitter is uninformative
	// for liveness reporting and would make every consecutive call diff —
	// PirateCash Core caches the response and we do not want spurious
	// "changed" diffs.
	uptime := int64(now.Sub(s.startedAt) / time.Second)
	if uptime < 0 {
		// Defensive: clock went backwards between construction and call.
		// Reporting a negative uptime would confuse downstream consumers
		// — clamp to zero so the field stays monotonically non-negative.
		uptime = 0
	}

	return domain.NodeStatus{
		Identity:               s.identity.Address,
		Address:                s.identity.Address,
		PublicKey:              identity.PublicKeyBase64(s.identity.PublicKey),
		BoxPublicKey:           identity.BoxPublicKeyBase64(s.identity.BoxPublicKey),
		ProtocolVersion:        config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		ClientVersion:          s.ClientVersion(),
		ClientBuild:            config.ClientBuild,
		ConnectedPeers:         s.connectedPeerCount(),
		StartedAt:              s.startedAt,
		UptimeSeconds:          uptime,
		CurrentTime:            now,
	}
}

// connectedPeerCount returns the number of distinct peer identities
// the node currently has at least one live connection with — outbound
// or inbound. Sourced from peerHealthFrames so it sees the same view
// as getActiveConnections; a public node with only inbound peers (no
// outbound peerSession entries) reports a non-zero count, which is
// the correct PIP-0001 Stage-2 service-health signal.
//
// Filters mirror ActiveConnectionsJSON: only frames with Connected,
// non-zero ConnID, a state in {healthy, degraded, stalled}, and an
// empty / active / initializing slot state are counted. Frames
// without a PeerID (handshake not yet complete) are skipped — until
// the peer is identified, it is not a "connected peer" PirateCash
// Core can rely on for relay.
//
// Distinct identities, not connections: a peer with one outbound and
// three inbound TCP sessions counts once. The "connected_peers" wire
// field is meant as a measure of relay reach, not socket count;
// callers that need per-connection detail use getActiveConnections.
//
// Lock-free via the peerHealthFrames atomic snapshot. Returns 0 when
// the snapshot has not yet been primed (test paths that bypass Run);
// the fresh-Service invariant test pins this to zero so the case is
// observable.
func (s *Service) connectedPeerCount() int {
	frames := s.peerHealthFrames()
	if len(frames) == 0 {
		return 0
	}
	seen := make(map[domain.PeerIdentity]struct{}, len(frames))
	for _, f := range frames {
		if !f.Connected {
			continue
		}
		if f.ConnID == 0 {
			continue
		}
		if f.PeerID == "" {
			continue
		}
		switch f.State {
		case peerStateHealthy, peerStateDegraded, peerStateStalled:
		default:
			continue
		}
		slotState := domain.SlotState(f.SlotState)
		if slotState != "" && slotState != domain.SlotStateActive && slotState != domain.SlotStateInitializing {
			continue
		}
		seen[domain.PeerIdentity(f.PeerID)] = struct{}{}
	}
	return len(seen)
}
