package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// TestNodeStatusReflectsServiceIdentity verifies that Service.NodeStatus
// returns the live identity material PirateCash Core consumes via
// PIP-0001's getNodeStatus endpoint. The contract: the address in the
// snapshot equals identity.Address; PublicKey decodes to a 32-byte
// ed25519 key; BoxPublicKey decodes to a 32-byte curve25519 key.
//
// Field-rename guard: a future maintainer who tweaks
// domain.NodeStatus's JSON tags would not cause this test to fail —
// JSON drift is covered by the rpc handler test. This test owns the
// in-memory identity → snapshot mapping invariant.
func TestNodeStatusReflectsServiceIdentity(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	status := svc.NodeStatus()

	if status.Identity != svc.identity.Address {
		t.Errorf("Identity = %q, want %q", status.Identity, svc.identity.Address)
	}
	if status.Address != svc.identity.Address {
		t.Errorf("Address = %q, want %q", status.Address, svc.identity.Address)
	}
	if got := identity.PublicKeyBase64(svc.identity.PublicKey); status.PublicKey != got {
		t.Errorf("PublicKey = %q, want %q", status.PublicKey, got)
	}
	if got := identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey); status.BoxPublicKey != got {
		t.Errorf("BoxPublicKey = %q, want %q", status.BoxPublicKey, got)
	}
	if status.ProtocolVersion != config.ProtocolVersion {
		t.Errorf("ProtocolVersion = %d, want %d", status.ProtocolVersion, config.ProtocolVersion)
	}
	if status.MinimumProtocolVersion != config.MinimumProtocolVersion {
		t.Errorf("MinimumProtocolVersion = %d, want %d", status.MinimumProtocolVersion, config.MinimumProtocolVersion)
	}
	if status.ClientBuild != config.ClientBuild {
		t.Errorf("ClientBuild = %d, want %d", status.ClientBuild, config.ClientBuild)
	}
	if status.ClientVersion == "" {
		t.Error("ClientVersion is empty")
	}
}

// TestNodeStatusUptimeAdvances exercises the uptime/current_time fields
// across two snapshots taken at least one second apart. PirateCash
// Core uses uptime_seconds as a monotonic liveness signal — if it
// stayed stuck at zero or went backwards, masternode health checks
// would mis-diagnose a healthy Corsa as a fresh restart loop.
func TestNodeStatusUptimeAdvances(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	first := svc.NodeStatus()
	if first.UptimeSeconds < 0 {
		t.Fatalf("first uptime = %d, must be non-negative", first.UptimeSeconds)
	}
	if first.CurrentTime.Before(first.StartedAt) {
		t.Fatalf("first.CurrentTime (%v) is before StartedAt (%v)", first.CurrentTime, first.StartedAt)
	}

	// Sleep just over one second so UptimeSeconds (whole-second
	// truncated) is guaranteed to advance. A shorter sleep would still
	// leave us inside the same second slot and the assertion below
	// would flake.
	time.Sleep(1100 * time.Millisecond)

	second := svc.NodeStatus()
	if second.UptimeSeconds <= first.UptimeSeconds {
		t.Errorf("uptime did not advance: first=%d, second=%d", first.UptimeSeconds, second.UptimeSeconds)
	}
	if !second.CurrentTime.After(first.CurrentTime) {
		t.Errorf("CurrentTime did not advance: first=%v, second=%v", first.CurrentTime, second.CurrentTime)
	}
	if !second.StartedAt.Equal(first.StartedAt) {
		t.Errorf("StartedAt drifted across calls: first=%v, second=%v", first.StartedAt, second.StartedAt)
	}
}

// TestNodeStatusConnectedPeersZeroForFreshService pins the fresh-state
// invariant: a Service that has no sessions reports zero connected
// peers. Without this guard PirateCash Core would have no way to tell
// "no peers yet" from "field broken".
func TestNodeStatusConnectedPeersZeroForFreshService(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	if got := svc.NodeStatus().ConnectedPeers; got != 0 {
		t.Errorf("ConnectedPeers on fresh Service = %d, want 0", got)
	}
}

// TestNodeStatusConnectedPeersCountsInboundOnly is the regression test
// for the P2 review finding: a public masternode that only has inbound
// connections (no outbound peerSession entries) must still report
// non-zero connected_peers. The previous implementation iterated
// s.sessions and silently undercounted those nodes — exactly the
// "false negative" the PIP-0001 Stage-2 service-health check would
// have keyed off.
func TestNodeStatusConnectedPeersCountsInboundOnly(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	// addInboundPeer is the canonical inbound-seed helper from
	// active_connections_test.go: populates s.health, s.peerIDs and
	// the conn registry, then re-primes the peer-health snapshot the
	// hot RPC path reads. Reusing it keeps the seeding contract in
	// lock-step with the ActiveConnectionsJSON test suite — drift in
	// either direction would mean the two consumers see different
	// "connected" semantics.
	cleanup := addInboundPeer(t, svc, "10.0.0.1:9000", "inbound-peer-id-001", domain.ConnID(101))
	t.Cleanup(cleanup)

	if got := svc.NodeStatus().ConnectedPeers; got != 1 {
		t.Errorf("ConnectedPeers with one inbound-only peer = %d, want 1", got)
	}
}

// TestNodeStatusConnectedPeersCountsOutbound is the symmetric
// regression for outbound-only peers. Together with the inbound-only
// test it pins the "regardless of direction" invariant the PIP-0001
// connected_peers field promises.
func TestNodeStatusConnectedPeersCountsOutbound(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	addOutboundPeer(svc, "10.0.0.2:9000", "outbound-peer-id-001", domain.ConnID(202))

	if got := svc.NodeStatus().ConnectedPeers; got != 1 {
		t.Errorf("ConnectedPeers with one outbound peer = %d, want 1", got)
	}
}

// TestNodeStatusConnectedPeersDeduplicatesByIdentity verifies that a
// peer with multiple inbound connections counts once. The wire field
// is named connected_peers (not connections); a chatty peer opening
// five inbound sockets is still one peer in PoSe terms.
func TestNodeStatusConnectedPeersDeduplicatesByIdentity(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	// Two inbound conns from the same peer identity at different
	// remote addresses (the masternode-vs-relay case where one peer
	// dials in twice from the same operator).
	c1 := addInboundPeer(t, svc, "10.0.0.3:9001", "shared-peer-id", domain.ConnID(301))
	t.Cleanup(c1)
	c2 := addInboundPeer(t, svc, "10.0.0.3:9002", "shared-peer-id", domain.ConnID(302))
	t.Cleanup(c2)

	if got := svc.NodeStatus().ConnectedPeers; got != 1 {
		t.Errorf("ConnectedPeers with two inbound conns from same identity = %d, want 1 (dedupe by identity)", got)
	}
}

// TestNodeStatusConnectedPeersExcludesDisconnected guards against a
// regression where a peer that has fallen out of Connected health
// would still count. PirateCash Core uses the field to estimate
// service health; a node that lost every peer 30 seconds ago must
// not still report the previous live count.
func TestNodeStatusConnectedPeersExcludesDisconnected(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	addDisconnectedPeer(svc, "10.0.0.4:9000")

	if got := svc.NodeStatus().ConnectedPeers; got != 0 {
		t.Errorf("ConnectedPeers with one disconnected peer = %d, want 0", got)
	}
}
