package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// newTestServiceWithHealth creates a minimal Service with the given peerHealth
// entries for testing aggregate status computation. It does not start any
// goroutines or open network connections.
func newTestServiceWithHealth(healthEntries []*peerHealth) *Service {
	healthMap := make(map[domain.PeerAddress]*peerHealth, len(healthEntries))
	pendingMap := make(map[domain.PeerAddress][]pendingFrame)
	for _, h := range healthEntries {
		healthMap[h.Address] = h
		pendingMap[h.Address] = nil
	}
	return &Service{
		health:          healthMap,
		pending:         pendingMap,
		aggregateStatus: domain.AggregateStatusSnapshot{Status: domain.NetworkStatusOffline},
	}
}

func TestComputeAggregateStatusLocked_Offline(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth(nil)
	snap := svc.computeAggregateStatusLocked(time.Now().UTC())

	if snap.Status != domain.NetworkStatusOffline {
		t.Fatalf("expected offline, got %s", snap.Status)
	}
	if snap.TotalPeers != 0 {
		t.Fatalf("expected 0 total peers, got %d", snap.TotalPeers)
	}
}

func TestComputeAggregateStatusLocked_Reconnecting(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: false, State: peerStateReconnecting},
		{Address: "10.0.0.2:1000", Connected: false, State: peerStateReconnecting},
	})
	snap := svc.computeAggregateStatusLocked(time.Now().UTC())

	if snap.Status != domain.NetworkStatusReconnecting {
		t.Fatalf("expected reconnecting, got %s", snap.Status)
	}
	if snap.TotalPeers != 2 {
		t.Fatalf("expected 2 total peers, got %d", snap.TotalPeers)
	}
	if snap.ConnectedPeers != 0 {
		t.Fatalf("expected 0 connected peers, got %d", snap.ConnectedPeers)
	}
}

func TestComputeAggregateStatusLocked_LimitedZeroUsable(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// Stalled: connected but not usable (no useful traffic for > heartbeat + stall timeout).
	stalledTime := now.Add(-(heartbeatInterval + pongStallTimeout + time.Second))
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
	})
	snap := svc.computeAggregateStatusLocked(now)

	if snap.Status != domain.NetworkStatusLimited {
		t.Fatalf("expected limited, got %s", snap.Status)
	}
	if snap.UsablePeers != 0 {
		t.Fatalf("expected 0 usable peers, got %d", snap.UsablePeers)
	}
	if snap.ConnectedPeers != 2 {
		t.Fatalf("expected 2 connected peers, got %d", snap.ConnectedPeers)
	}
}

func TestComputeAggregateStatusLocked_LimitedOneUsable(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	snap := svc.computeAggregateStatusLocked(now)

	if snap.Status != domain.NetworkStatusLimited {
		t.Fatalf("expected limited, got %s", snap.Status)
	}
	if snap.UsablePeers != 1 {
		t.Fatalf("expected 1 usable peer, got %d", snap.UsablePeers)
	}
}

func TestComputeAggregateStatusLocked_Warning(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	stalledTime := now.Add(-(heartbeatInterval + pongStallTimeout + time.Second))
	// 2 healthy + 3 stalled = 5 connected, usable (2) * 2 < connected (5) => warning
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.3:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
		{Address: "10.0.0.4:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
		{Address: "10.0.0.5:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
	})
	snap := svc.computeAggregateStatusLocked(now)

	if snap.Status != domain.NetworkStatusWarning {
		t.Fatalf("expected warning, got %s", snap.Status)
	}
	if snap.UsablePeers != 2 {
		t.Fatalf("expected 2 usable peers, got %d", snap.UsablePeers)
	}
	if snap.ConnectedPeers != 5 {
		t.Fatalf("expected 5 connected peers, got %d", snap.ConnectedPeers)
	}
}

func TestComputeAggregateStatusLocked_Healthy(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// DefaultOutgoingPeers = 8, so we need >= 8 usable peers for Healthy.
	entries := make([]*peerHealth, 8)
	for i := range entries {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	svc := newTestServiceWithHealth(entries)
	snap := svc.computeAggregateStatusLocked(now)

	if snap.Status != domain.NetworkStatusHealthy {
		t.Fatalf("expected healthy, got %s", snap.Status)
	}
	if snap.UsablePeers != 8 {
		t.Fatalf("expected 8 usable peers, got %d", snap.UsablePeers)
	}
}

func TestComputeAggregateStatusLocked_WarningBelowTarget(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// 5 usable out of target 8: >= target/2 (4) but < target (8) → Warning.
	entries := make([]*peerHealth, 5)
	for i := range entries {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	svc := newTestServiceWithHealth(entries)
	snap := svc.computeAggregateStatusLocked(now)

	if snap.Status != domain.NetworkStatusWarning {
		t.Fatalf("expected warning, got %s", snap.Status)
	}
	if snap.UsablePeers != 5 {
		t.Fatalf("expected 5 usable peers, got %d", snap.UsablePeers)
	}
}

func TestComputeAggregateStatusLocked_LimitedBelowHalfTarget(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// 3 usable out of target 8: >= 2 but < target/2 (4) → Limited.
	entries := make([]*peerHealth, 3)
	for i := range entries {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	svc := newTestServiceWithHealth(entries)
	snap := svc.computeAggregateStatusLocked(now)

	if snap.Status != domain.NetworkStatusLimited {
		t.Fatalf("expected limited, got %s", snap.Status)
	}
	if snap.UsablePeers != 3 {
		t.Fatalf("expected 3 usable peers, got %d", snap.UsablePeers)
	}
}

func TestComputeAggregateStatusLocked_PendingMessages(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	// Add pending messages to first peer.
	svc.pending[domain.PeerAddress("10.0.0.1:1000")] = make([]pendingFrame, 3)
	svc.pending[domain.PeerAddress("10.0.0.2:1000")] = make([]pendingFrame, 2)

	snap := svc.computeAggregateStatusLocked(now)

	if snap.PendingMessages != 5 {
		t.Fatalf("expected 5 pending messages, got %d", snap.PendingMessages)
	}
}

func TestComputeAggregateStatusLocked_PendingWithoutHealthEntry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.pending[domain.PeerAddress("10.0.0.1:1000")] = make([]pendingFrame, 2)
	// Peer without a health entry but with queued frames.
	svc.pending[domain.PeerAddress("10.0.0.99:1000")] = make([]pendingFrame, 4)

	snap := svc.computeAggregateStatusLocked(now)

	if snap.PendingMessages != 6 {
		t.Fatalf("expected 6 pending messages (2 + 4 from peer without health), got %d", snap.PendingMessages)
	}
}

func TestComputeAggregateStatusLocked_OrphanedIncludedInPending(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.pending[domain.PeerAddress("10.0.0.1:1000")] = make([]pendingFrame, 2)

	// Orphaned frames — persisted legacy backlog that could not be migrated.
	svc.orphaned = map[domain.PeerAddress][]pendingFrame{
		"legacy-peer-a": make([]pendingFrame, 3),
		"legacy-peer-b": make([]pendingFrame, 5),
	}

	snap := svc.computeAggregateStatusLocked(now)

	// 2 pending + 3 orphaned(a) + 5 orphaned(b) = 10
	if snap.PendingMessages != 10 {
		t.Fatalf("expected 10 pending messages (2 pending + 8 orphaned), got %d", snap.PendingMessages)
	}
}

func TestRefreshAggregateStatusLocked_TransitionsCorrectly(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// Use 8 peers to hit Healthy with default target = 8.
	entries := make([]*peerHealth, 8)
	for i := range entries {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	svc := newTestServiceWithHealth(entries)

	// Initial state is offline (set in constructor).
	if svc.aggregateStatus.Status != domain.NetworkStatusOffline {
		t.Fatalf("expected initial offline, got %s", svc.aggregateStatus.Status)
	}

	// Refresh should transition to healthy (8 usable == target).
	svc.refreshAggregateStatusLocked()
	if svc.aggregateStatus.Status != domain.NetworkStatusHealthy {
		t.Fatalf("expected healthy after refresh, got %s", svc.aggregateStatus.Status)
	}

	// Disconnect peers until below target → warning (5 usable < 8 target).
	for i := 5; i < 8; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		svc.health[addr].Connected = false
		svc.health[addr].State = peerStateReconnecting
	}
	svc.refreshAggregateStatusLocked()
	if svc.aggregateStatus.Status != domain.NetworkStatusWarning {
		t.Fatalf("expected warning after partial disconnect, got %s", svc.aggregateStatus.Status)
	}

	// Disconnect more until below target/2 → limited (2 usable < 4 half-target).
	for i := 2; i < 5; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		svc.health[addr].Connected = false
		svc.health[addr].State = peerStateReconnecting
	}
	svc.refreshAggregateStatusLocked()
	if svc.aggregateStatus.Status != domain.NetworkStatusLimited {
		t.Fatalf("expected limited after more disconnects, got %s", svc.aggregateStatus.Status)
	}

	// Disconnect all peers → reconnecting.
	for i := 0; i < 2; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		svc.health[addr].Connected = false
		svc.health[addr].State = peerStateReconnecting
	}
	svc.refreshAggregateStatusLocked()
	if svc.aggregateStatus.Status != domain.NetworkStatusReconnecting {
		t.Fatalf("expected reconnecting after all disconnect, got %s", svc.aggregateStatus.Status)
	}
}

func TestAggregateStatus_ConcurrentSafe(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// Use 8 peers to reach Healthy with default target.
	entries := make([]*peerHealth, 8)
	for i := range entries {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	svc := newTestServiceWithHealth(entries)
	svc.refreshAggregateStatusLocked()

	// AggregateStatus() takes RLock, should not deadlock.
	snap := svc.AggregateStatus()
	if snap.Status != domain.NetworkStatusHealthy {
		t.Fatalf("expected healthy, got %s", snap.Status)
	}
}

func TestNetworkStatus_IsHealthy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status domain.NetworkStatus
		want   bool
	}{
		{domain.NetworkStatusOffline, false},
		{domain.NetworkStatusReconnecting, false},
		{domain.NetworkStatusLimited, false},
		{domain.NetworkStatusWarning, false},
		{domain.NetworkStatusHealthy, true},
	}
	for _, tc := range cases {
		if got := tc.status.IsHealthy(); got != tc.want {
			t.Errorf("NetworkStatus(%q).IsHealthy() = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestAggregateStatusFrame_ReturnsCorrectFrame(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// 8 usable + 1 reconnecting = healthy with default target 8.
	entries := make([]*peerHealth, 9)
	for i := 0; i < 8; i++ {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	entries[8] = &peerHealth{Address: "10.0.0.9:1000", Connected: false, State: peerStateReconnecting}
	svc := newTestServiceWithHealth(entries)
	svc.refreshAggregateStatusLocked()

	frame := svc.aggregateStatusFrame()

	if frame.Type != "aggregate_status" {
		t.Fatalf("expected frame type aggregate_status, got %s", frame.Type)
	}
	if frame.AggregateStatus == nil {
		t.Fatal("expected AggregateStatus to be non-nil")
	}
	if frame.AggregateStatus.Status != "healthy" {
		t.Fatalf("expected status healthy, got %s", frame.AggregateStatus.Status)
	}
	if frame.AggregateStatus.UsablePeers != 8 {
		t.Fatalf("expected 8 usable peers, got %d", frame.AggregateStatus.UsablePeers)
	}
	if frame.AggregateStatus.ConnectedPeers != 8 {
		t.Fatalf("expected 8 connected peers, got %d", frame.AggregateStatus.ConnectedPeers)
	}
	if frame.AggregateStatus.TotalPeers != 9 {
		t.Fatalf("expected 9 total peers, got %d", frame.AggregateStatus.TotalPeers)
	}
}

func TestHandleLocalFrame_FetchAggregateStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	// 8 usable peers to reach Healthy with default target.
	entries := make([]*peerHealth, 8)
	for i := range entries {
		addr := domain.PeerAddress(fmt.Sprintf("10.0.0.%d:1000", i+1))
		entries[i] = &peerHealth{Address: addr, Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now}
	}
	svc := newTestServiceWithHealth(entries)
	svc.refreshAggregateStatusLocked()

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_aggregate_status"})

	if reply.Type != "aggregate_status" {
		t.Fatalf("expected aggregate_status, got %s", reply.Type)
	}
	if reply.AggregateStatus == nil {
		t.Fatal("expected AggregateStatus in reply")
	}
	if reply.AggregateStatus.Status != "healthy" {
		t.Fatalf("expected healthy, got %s", reply.AggregateStatus.Status)
	}
}

func TestHandleLocalFrame_FetchAggregateStatus_Offline(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth(nil)

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_aggregate_status"})

	if reply.Type != "aggregate_status" {
		t.Fatalf("expected aggregate_status, got %s", reply.Type)
	}
	if reply.AggregateStatus == nil {
		t.Fatal("expected AggregateStatus in reply")
	}
	// No peers, initial state is offline.
	if reply.AggregateStatus.Status != "offline" {
		t.Fatalf("expected offline, got %s", reply.AggregateStatus.Status)
	}
}

// TestAggregateStatus_TimeBasedDrift verifies that periodic recompute
// detects peers that silently aged from healthy into degraded/stalled
// without any explicit disconnect events.
func TestAggregateStatus_TimeBasedDrift(t *testing.T) {
	t.Parallel()

	past := time.Now().UTC().Add(-10 * time.Minute)
	svc := newTestServiceWithHealth([]*peerHealth{
		// Both peers were healthy in the past but haven't sent anything since.
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: past},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: past},
	})

	// Initial refresh with the peers' old timestamps — at current wall
	// clock these peers have aged past heartbeatInterval + pongStallTimeout
	// so computePeerStateAtLocked will report them as stalled.
	svc.refreshAggregateStatus()

	snap := svc.AggregateStatus()
	if snap.Status == domain.NetworkStatusHealthy {
		t.Fatalf("expected non-healthy after time drift, got %s (usable=%d, connected=%d)",
			snap.Status, snap.UsablePeers, snap.ConnectedPeers)
	}
	// Both connected but stalled → limited.
	if snap.Status != domain.NetworkStatusLimited {
		t.Fatalf("expected limited, got %s", snap.Status)
	}
}

// TestAggregateStatus_InitialRecomputeFromRestoredHealth verifies that
// the aggregate status reflects restored health entries immediately,
// rather than staying at the zero-value "offline".
func TestAggregateStatus_InitialRecomputeFromRestoredHealth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: false, State: peerStateReconnecting},
		{Address: "10.0.0.2:1000", Connected: false, State: peerStateReconnecting},
	})
	// Simulate what NewService does after restoring health.
	svc.refreshAggregateStatusLocked()

	if svc.aggregateStatus.Status != domain.NetworkStatusReconnecting {
		t.Fatalf("expected reconnecting after initial recompute, got %s",
			svc.aggregateStatus.Status)
	}
}

// TestAggregateStatus_PendingCountUpdatedByPeriodicRefresh verifies that
// the periodic refreshAggregateStatus() picks up pending queue changes
// even without peer-state transitions.
func TestAggregateStatus_PendingCountUpdatedByPeriodicRefresh(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.refreshAggregateStatus()

	if svc.AggregateStatus().PendingMessages != 0 {
		t.Fatalf("expected 0 pending initially, got %d", svc.AggregateStatus().PendingMessages)
	}

	// Add pending messages without changing peer state.
	svc.deliveryMu.Lock()
	svc.pending[domain.PeerAddress("10.0.0.1:1000")] = make([]pendingFrame, 7)
	svc.deliveryMu.Unlock()

	// Periodic refresh should pick up the new pending count.
	svc.refreshAggregateStatus()

	if svc.AggregateStatus().PendingMessages != 7 {
		t.Fatalf("expected 7 pending after refresh, got %d", svc.AggregateStatus().PendingMessages)
	}
}
