package node

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
)

func TestMarkPeerConnectedIncrementsScore(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, "outbound")

	svc.mu.RLock()
	health := svc.health[peerAddr]
	svc.mu.RUnlock()

	if health == nil {
		t.Fatal("expected health entry")
	}
	if health.Score != peerScoreConnect {
		t.Fatalf("expected score %d after connect, got %d", peerScoreConnect, health.Score)
	}
	if health.ConsecutiveFailures != 0 {
		t.Fatalf("expected 0 failures, got %d", health.ConsecutiveFailures)
	}
}

func TestMarkPeerDisconnectedWithErrorDecrementsScore(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, "outbound")
	svc.markPeerDisconnected(peerAddr, errors.New("connection reset"))

	svc.mu.RLock()
	health := svc.health[peerAddr]
	svc.mu.RUnlock()

	expectedScore := peerScoreConnect + peerScoreFailure
	if health.Score != expectedScore {
		t.Fatalf("expected score %d, got %d", expectedScore, health.Score)
	}
	if health.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 failure, got %d", health.ConsecutiveFailures)
	}
}

func TestMarkPeerDisconnectedCleanDecrementsLess(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, "outbound")
	svc.markPeerDisconnected(peerAddr, nil)

	svc.mu.RLock()
	health := svc.health[peerAddr]
	svc.mu.RUnlock()

	expectedScore := peerScoreConnect + peerScoreDisconnect
	if health.Score != expectedScore {
		t.Fatalf("expected score %d, got %d", expectedScore, health.Score)
	}
	// Clean disconnect resets consecutive failures.
	if health.ConsecutiveFailures != 0 {
		t.Fatalf("expected 0 consecutive failures after clean disconnect, got %d", health.ConsecutiveFailures)
	}
}

func TestScoreClampedOnRepeatedFailures(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	for i := 0; i < 50; i++ {
		svc.markPeerDisconnected(peerAddr, errors.New("timeout"))
	}

	svc.mu.RLock()
	health := svc.health[peerAddr]
	svc.mu.RUnlock()

	if health.Score < peerScoreMin {
		t.Fatalf("score %d below minimum %d", health.Score, peerScoreMin)
	}
}

func TestNewServiceMergesPersistedPeersWithBootstrap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	now := time.Now().UTC()
	persisted := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{Address: "10.0.0.1:64646", Score: 50, Source: domain.PeerSourcePeerExchange, LastConnectedAt: &now},
			{Address: "10.0.0.2:64646", Score: 20, Source: domain.PeerSourcePeerExchange},
			{Address: "10.0.0.3:64646", Score: 10, Source: domain.PeerSourcePersisted},
		},
	}
	data, _ := json.MarshalIndent(persisted, "", "  ")
	if err := os.WriteFile(peersPath, data, 0o600); err != nil {
		t.Fatalf("write peers file: %v", err)
	}

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	cfg := config.Node{
		ListenAddress:    ":0",
		AdvertiseAddress: "",
		BootstrapPeers:   []string{"10.0.0.1:64646", "bootstrap.example.com:64646"},
		PeersStatePath:   peersPath,
		Type:             config.NodeTypeClient,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := NewService(cfg, id)
	go func() { _ = svc.Run(ctx) }()

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// Bootstrap peers come first.
	if len(svc.peers) < 2 {
		t.Fatalf("expected at least 2 peers, got %d", len(svc.peers))
	}
	if svc.peers[0].Address != "10.0.0.1:64646" {
		t.Fatalf("expected first peer to be bootstrap 10.0.0.1:64646, got %s", svc.peers[0].Address)
	}
	if svc.peers[0].Source != domain.PeerSourceBootstrap {
		t.Fatalf("expected bootstrap source, got %s", svc.peers[0].Source)
	}
	if svc.peers[1].Address != "bootstrap.example.com:64646" {
		t.Fatalf("expected second peer bootstrap.example.com:64646, got %s", svc.peers[1].Address)
	}

	// Persisted peers follow, but 10.0.0.1 is a duplicate and should be skipped.
	// So we expect: bootstrap (10.0.0.1), bootstrap (bootstrap.example.com),
	// persisted (10.0.0.2), persisted (10.0.0.3).
	if len(svc.peers) != 4 {
		addrs := make([]string, len(svc.peers))
		for i, p := range svc.peers {
			addrs[i] = string(p.Address)
		}
		t.Fatalf("expected 4 peers (2 bootstrap + 2 unique persisted), got %d: %v", len(svc.peers), addrs)
	}

	// Persisted peers should be sorted by score desc: 10.0.0.2 (20) before 10.0.0.3 (10).
	if svc.peers[2].Address != "10.0.0.2:64646" {
		t.Fatalf("expected third peer 10.0.0.2:64646, got %s", svc.peers[2].Address)
	}
	if svc.peers[3].Address != "10.0.0.3:64646" {
		t.Fatalf("expected fourth peer 10.0.0.3:64646, got %s", svc.peers[3].Address)
	}

	// Health should be seeded from persisted metadata — even for the bootstrap-overlap peer.
	h1 := svc.health[domain.PeerAddress("10.0.0.1:64646")]
	if h1 == nil {
		t.Fatal("expected health entry for 10.0.0.1:64646 (bootstrap+persisted overlap)")
	}
	if h1.Score != 50 {
		t.Fatalf("expected seeded Score=50 for overlap peer, got %d", h1.Score)
	}
	h2 := svc.health[domain.PeerAddress("10.0.0.2:64646")]
	if h2 == nil {
		t.Fatal("expected health entry for 10.0.0.2:64646")
	}
	if h2.Score != 20 {
		t.Fatalf("expected seeded Score=20, got %d", h2.Score)
	}
}

func TestNewServiceWithEmptyPeersFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")
	// File doesn't exist — should still start fine.

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	cfg := config.Node{
		ListenAddress:    ":0",
		AdvertiseAddress: "",
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		PeersStatePath:   peersPath,
		Type:             config.NodeTypeClient,
	}

	ctx, cancel := context.WithCancel(context.Background())

	svc := NewService(cfg, id)
	done := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(done)
	}()

	svc.mu.RLock()
	count := len(svc.peers)
	svc.mu.RUnlock()

	cancel()
	<-done

	if count != 1 {
		t.Fatalf("expected 1 bootstrap peer, got %d", count)
	}
}

func TestFlushPeerStateWritesFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		PeersStatePath:   peersPath,
	})
	defer stop()

	// Add a peer and mark it connected so it has health data.
	peerAddr := domain.PeerAddress("10.0.0.2:64646")
	svc.addPeerAddress(peerAddr, "full", "peer-test")
	svc.markPeerConnected(peerAddr, "outbound")

	svc.flushPeerState()

	// Verify the file exists and can be loaded.
	state, err := loadPeerState(peersPath)
	if err != nil {
		t.Fatalf("loadPeerState after flush: %v", err)
	}
	if len(state.Peers) < 2 {
		t.Fatalf("expected at least 2 peers in state file, got %d", len(state.Peers))
	}

	// Find the connected peer.
	found := false
	for _, p := range state.Peers {
		if p.Address == peerAddr {
			found = true
			if p.Score != peerScoreConnect {
				t.Fatalf("expected score %d, got %d", peerScoreConnect, p.Score)
			}
			if p.LastConnectedAt == nil {
				t.Fatal("expected non-nil LastConnectedAt")
			}
			break
		}
	}
	if !found {
		t.Fatal("connected peer not found in saved state")
	}
}

// TestPersistedMetadataSurvivesFlushWithoutReconnect verifies that loading
// a peers.json with scored metadata, then flushing without any peer activity,
// preserves ALL persisted fields: Score, ConsecutiveFailures, LastConnectedAt,
// LastError, NodeType, Source, and AddedAt.
// This is the regression test for the P1 issue where metadata was lost on
// the first flush cycle after restart.
func TestPersistedMetadataSurvivesFlushWithoutReconnect(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	peersPath := filepath.Join(dir, "peers.json")

	// Write initial peer state with non-trivial metadata.
	connTime := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	discTime := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	addedTime := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second)
	original := peerStateFile{
		Version: peerStateVersion,
		Peers: []peerEntry{
			{
				Address:             "10.0.0.99:64646",
				NodeType:            "full",
				LastConnectedAt:     &connTime,
				LastDisconnectedAt:  &discTime,
				ConsecutiveFailures: 3,
				LastError:           "read timeout",
				Source:              domain.PeerSourcePeerExchange,
				AddedAt:             &addedTime,
				Score:               42,
			},
		},
	}
	data, _ := json.MarshalIndent(original, "", "  ")
	if err := os.WriteFile(peersPath, data, 0o600); err != nil {
		t.Fatalf("write initial peers: %v", err)
	}

	// Create a node that loads this peer state.
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	cfg := config.Node{
		ListenAddress:    ":0",
		AdvertiseAddress: "",
		BootstrapPeers:   []string{},
		PeersStatePath:   peersPath,
		Type:             config.NodeTypeClient,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := NewService(cfg, id)
	go func() { _ = svc.Run(ctx) }()

	// Flush immediately — the peer hasn't reconnected, so health should
	// come entirely from persisted state.
	svc.flushPeerState()

	// Reload and verify.
	reloaded, err := loadPeerState(peersPath)
	if err != nil {
		t.Fatalf("loadPeerState after flush: %v", err)
	}

	var found *peerEntry
	for i := range reloaded.Peers {
		if reloaded.Peers[i].Address == "10.0.0.99:64646" {
			found = &reloaded.Peers[i]
			break
		}
	}
	if found == nil {
		addrs := make([]string, len(reloaded.Peers))
		for i, p := range reloaded.Peers {
			addrs[i] = string(p.Address)
		}
		t.Fatalf("peer 10.0.0.99:64646 not found in flushed state; got: %v", addrs)
	}

	if found.Score != 42 {
		t.Fatalf("expected persisted Score=42, got %d", found.Score)
	}
	if found.ConsecutiveFailures != 3 {
		t.Fatalf("expected persisted ConsecutiveFailures=3, got %d", found.ConsecutiveFailures)
	}
	if found.LastError != "read timeout" {
		t.Fatalf("expected persisted LastError=%q, got %q", "read timeout", found.LastError)
	}
	if found.LastConnectedAt == nil || !found.LastConnectedAt.Equal(connTime) {
		t.Fatalf("expected persisted LastConnectedAt=%v, got %v", connTime, found.LastConnectedAt)
	}
	if found.LastDisconnectedAt == nil || !found.LastDisconnectedAt.Equal(discTime) {
		t.Fatalf("expected persisted LastDisconnectedAt=%v, got %v", discTime, found.LastDisconnectedAt)
	}
	// P1 regression: NodeType, Source, AddedAt must survive restart+flush.
	if found.NodeType != "full" {
		t.Fatalf("expected persisted NodeType=%q, got %q", "full", found.NodeType)
	}
	if found.Source != domain.PeerSourcePeerExchange {
		t.Fatalf("expected persisted Source=%q, got %q", domain.PeerSourcePeerExchange, found.Source)
	}
	if found.AddedAt == nil || !found.AddedAt.Equal(addedTime) {
		t.Fatalf("expected persisted AddedAt=%v, got %v", addedTime, found.AddedAt)
	}
}

// TestCleanDisconnectResetsConsecutiveFailures verifies that a clean
// disconnect (err=nil) resets the failure counter and does NOT increment it.
func TestCleanDisconnectResetsConsecutiveFailures(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	// Simulate 3 failed connections.
	for i := 0; i < 3; i++ {
		svc.markPeerDisconnected(peerAddr, errors.New("timeout"))
	}

	svc.mu.RLock()
	failsBefore := svc.health[peerAddr].ConsecutiveFailures
	svc.mu.RUnlock()
	if failsBefore != 3 {
		t.Fatalf("expected 3 failures before clean disconnect, got %d", failsBefore)
	}

	// Clean disconnect should reset the counter.
	svc.markPeerConnected(peerAddr, "outbound")
	svc.markPeerDisconnected(peerAddr, nil)

	svc.mu.RLock()
	health := svc.health[peerAddr]
	svc.mu.RUnlock()

	if health.ConsecutiveFailures != 0 {
		t.Fatalf("expected 0 consecutive failures after clean disconnect, got %d", health.ConsecutiveFailures)
	}
}

// TestFlushPeerStateRetryOnWriteFailure verifies that when savePeerState
// fails, lastPeerSave is NOT updated, allowing an immediate retry on the
// next cycle instead of waiting the full 5-minute interval.
func TestFlushPeerStateRetryOnWriteFailure(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	// Use a path that will fail (directory that doesn't exist, is a file).
	badDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(badDir, []byte("blocker"), 0o600); err != nil {
		t.Fatalf("create blocker file: %v", err)
	}
	badPath := filepath.Join(badDir, "peers.json")

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{"10.0.0.1:64646"},
		PeersStatePath:   badPath,
	})
	defer stop()

	svc.flushPeerState()

	svc.mu.RLock()
	lastSave := svc.lastPeerSave
	svc.mu.RUnlock()

	if !lastSave.IsZero() {
		t.Fatalf("expected lastPeerSave to remain zero after failed save, got %v", lastSave)
	}
}

// TestMarkPeerConnectedSetsLastUsefulReceiveAt verifies that markPeerConnected
// seeds LastUsefulReceiveAt so that computePeerStateLocked does not immediately
// return "degraded" before any ping/pong exchange has occurred.
func TestMarkPeerConnectedSetsLastUsefulReceiveAt(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	before := time.Now().UTC().Add(-time.Second)
	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, peerDirectionInbound)

	svc.mu.RLock()
	health := svc.health[peerAddr]
	state := svc.computePeerStateLocked(health)
	svc.mu.RUnlock()

	if health == nil {
		t.Fatal("expected health entry")
	}
	if health.LastUsefulReceiveAt.Before(before) {
		t.Fatalf("LastUsefulReceiveAt not set on connect: %v", health.LastUsefulReceiveAt)
	}
	if state != peerStateHealthy {
		t.Fatalf("expected state %q immediately after connect, got %q", peerStateHealthy, state)
	}
}

// TestComputePeerStateHealthyAfterInboundConnect verifies that a freshly
// connected inbound peer is reported as healthy when the UI polls for
// peer_health (which recomputes state via computePeerStateLocked).
func TestComputePeerStateHealthyAfterInboundConnect(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, peerDirectionInbound)

	// Simulate the UI polling fetch_peer_health which calls peerHealthFrames.
	frames := svc.peerHealthFrames()

	found := false
	for _, f := range frames {
		if f.Address == string(peerAddr) {
			found = true
			if f.State != peerStateHealthy {
				t.Fatalf("expected inbound peer state %q in health frame, got %q", peerStateHealthy, f.State)
			}
			break
		}
	}
	if !found {
		t.Fatal("inbound peer not found in health frames")
	}
}

// TestInboundPingUpdatesHealth verifies that receiving a ping from an inbound
// peer (handled by markPeerRead) updates LastUsefulReceiveAt and keeps the
// peer in healthy state.
func TestInboundPingUpdatesHealth(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, peerDirectionInbound)

	// Simulate receiving a ping from the inbound peer.
	svc.markPeerRead(peerAddr, protocol.Frame{Type: "ping"})

	svc.mu.RLock()
	health := svc.health[peerAddr]
	state := svc.computePeerStateLocked(health)
	svc.mu.RUnlock()

	if state != peerStateHealthy {
		t.Fatalf("expected %q after inbound ping, got %q", peerStateHealthy, state)
	}
	if health.LastUsefulReceiveAt.IsZero() {
		t.Fatal("LastUsefulReceiveAt should be set after receiving ping")
	}
}

// TestInboundPongUpdatesHealth verifies that receiving a pong from an inbound
// peer (in response to our heartbeat ping) updates LastPongAt and keeps the
// peer healthy.
func TestInboundPongUpdatesHealth(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")
	svc.markPeerConnected(peerAddr, peerDirectionInbound)

	// Simulate our heartbeat sending a ping.
	svc.markPeerWrite(peerAddr, protocol.Frame{Type: "ping"})

	// Simulate receiving a pong response.
	svc.markPeerRead(peerAddr, protocol.Frame{Type: "pong"})

	svc.mu.RLock()
	health := svc.health[peerAddr]
	state := svc.computePeerStateLocked(health)
	svc.mu.RUnlock()

	if health.LastPongAt.IsZero() {
		t.Fatal("LastPongAt should be set after receiving pong")
	}
	if health.LastPingAt.IsZero() {
		t.Fatal("LastPingAt should be set after sending ping")
	}
	if state != peerStateHealthy {
		t.Fatalf("expected %q after inbound pong, got %q", peerStateHealthy, state)
	}
}

// TestTrackedInboundPeerAddressIsPerConnection verifies that
// trackedInboundPeerAddress gates on the concrete conn, not just on
// whether the address is globally tracked. A second unauthenticated
// connection claiming the same address must not pass the guard.
func TestTrackedInboundPeerAddressIsPerConnection(t *testing.T) {
	t.Parallel()

	address := freeAddress(t)
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:    address,
		AdvertiseAddress: normalizeAddress(address),
		BootstrapPeers:   []string{},
	})
	defer stop()

	peerAddr := domain.PeerAddress("10.0.0.1:64646")

	// Create two fake connections.
	authConn, authPeer := net.Pipe()
	defer func() { _ = authPeer.Close() }()
	spoofConn, spoofPeer := net.Pipe()
	defer func() { _ = spoofPeer.Close() }()

	// Simulate hello on both connections (stores connPeerInfo).
	svc.rememberConnPeerAddr(authConn, protocol.Frame{Address: string(peerAddr), Listen: string(peerAddr)})
	svc.rememberConnPeerAddr(spoofConn, protocol.Frame{Address: string(peerAddr), Listen: string(peerAddr)})

	// Only the first connection completes auth and is promoted.
	svc.trackInboundConnect(authConn, peerAddr, "test-identity")

	// The authenticated connection should return the address.
	if got := svc.trackedInboundPeerAddress(authConn); got != peerAddr {
		t.Fatalf("expected tracked address %q for auth conn, got %q", peerAddr, got)
	}

	// The spoofed connection must NOT return the address even though
	// the same address is globally tracked via the auth connection.
	if got := svc.trackedInboundPeerAddress(spoofConn); got != "" {
		t.Fatalf("expected empty address for untracked conn, got %q", got)
	}

	// Clean up.
	_ = authConn.Close()
	svc.trackInboundDisconnect(authConn, peerAddr)
	_ = spoofConn.Close()
	svc.trackInboundDisconnect(spoofConn, peerAddr)
}
