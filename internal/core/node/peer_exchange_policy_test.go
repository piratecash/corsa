package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestShouldRequestPeers_Offline verifies that the policy allows get_peers
// when no peers are known at all (cold start scenario).
func TestShouldRequestPeers_Offline(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth(nil)
	// aggregateStatus defaults to offline in newTestServiceWithHealth.

	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true when aggregate status is offline")
	}
}

// TestShouldRequestPeers_Reconnecting verifies that the policy allows
// get_peers when all peers are reconnecting (none connected yet).
func TestShouldRequestPeers_Reconnecting(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: false, State: peerStateReconnecting},
		{Address: "10.0.0.2:1000", Connected: false, State: peerStateReconnecting},
	})
	svc.refreshAggregateStatusLocked()

	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true when aggregate status is reconnecting")
	}
}

// TestShouldRequestPeers_Limited verifies that the policy allows get_peers
// when only one usable peer exists (bootstrap not yet complete).
func TestShouldRequestPeers_Limited(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.refreshAggregateStatusLocked()

	if svc.aggregateStatus.Status != domain.NetworkStatusLimited {
		t.Fatalf("precondition: expected limited, got %s", svc.aggregateStatus.Status)
	}
	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true when aggregate status is limited")
	}
}

// TestShouldRequestPeers_Warning verifies that the policy allows get_peers
// when less than half of connected peers are usable.
func TestShouldRequestPeers_Warning(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	stalledTime := now.Add(-(heartbeatInterval + pongStallTimeout + time.Second))
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.3:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
		{Address: "10.0.0.4:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
		{Address: "10.0.0.5:1000", Connected: true, State: peerStateStalled, LastUsefulReceiveAt: stalledTime},
	})
	svc.refreshAggregateStatusLocked()

	if svc.aggregateStatus.Status != domain.NetworkStatusWarning {
		t.Fatalf("precondition: expected warning, got %s", svc.aggregateStatus.Status)
	}
	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true when aggregate status is warning")
	}
}

// TestShouldRequestPeers_Healthy verifies that the policy forbids get_peers
// when the node has reached steady-state healthy connectivity.
func TestShouldRequestPeers_Healthy(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.3:1000", Connected: true, State: peerStateDegraded, LastUsefulReceiveAt: now.Add(-heartbeatInterval - time.Second)},
	})
	svc.refreshAggregateStatusLocked()

	if svc.aggregateStatus.Status != domain.NetworkStatusHealthy {
		t.Fatalf("precondition: expected healthy, got %s", svc.aggregateStatus.Status)
	}
	if svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=false when aggregate status is healthy")
	}
}

// TestShouldRequestPeers_TransitionHealthyToLimited verifies that the policy
// re-evaluates correctly when the node transitions from healthy back to a
// non-healthy state (e.g. after losing peers).
func TestShouldRequestPeers_TransitionHealthyToLimited(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.refreshAggregateStatusLocked()

	// Precondition: healthy — policy forbids get_peers.
	if svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=false in healthy state")
	}

	// Simulate losing one peer: limited (only 1 usable).
	svc.health[domain.PeerAddress("10.0.0.2:1000")].Connected = false
	svc.health[domain.PeerAddress("10.0.0.2:1000")].State = peerStateReconnecting
	svc.refreshAggregateStatusLocked()

	if svc.aggregateStatus.Status != domain.NetworkStatusLimited {
		t.Fatalf("precondition: expected limited after disconnect, got %s", svc.aggregateStatus.Status)
	}
	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true after transition to limited")
	}
}

// TestShouldRequestPeers_ForcedRefreshAfterTotalLoss verifies that the
// policy allows get_peers when all connections are lost (forced refresh
// scenario). In this case aggregate status goes to reconnecting or offline,
// so the policy naturally returns true.
func TestShouldRequestPeers_ForcedRefreshAfterTotalLoss(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: false, State: peerStateReconnecting},
		{Address: "10.0.0.2:1000", Connected: false, State: peerStateReconnecting},
		{Address: "10.0.0.3:1000", Connected: false, State: peerStateReconnecting},
	})
	svc.refreshAggregateStatusLocked()

	if svc.aggregateStatus.Status != domain.NetworkStatusReconnecting {
		t.Fatalf("precondition: expected reconnecting, got %s", svc.aggregateStatus.Status)
	}
	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true after total connection loss")
	}
}

// TestShouldRequestPeers_PeersJsonDoesNotOverrideNodeState verifies that
// the secondary heuristic (peers.json size) does not override the primary
// signal from the aggregate node status. Even if the node has many known
// addresses, if aggregate status is not healthy, get_peers should proceed.
// Conversely, even with few known addresses, if aggregate status IS healthy,
// get_peers should be skipped.
//
// Currently peers.json is not used as a signal in the first implementation,
// so this test verifies that only aggregate status matters.
func TestShouldRequestPeers_PeersJsonDoesNotOverrideNodeState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	// Healthy with only 2 peers — even though peer set is small,
	// aggregate status is healthy, so policy says skip.
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
		{Address: "10.0.0.2:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.refreshAggregateStatusLocked()

	if svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=false: aggregate status is healthy regardless of peer set size")
	}
}

// TestShouldRequestPeers_SnapshotBeforeRegistration verifies that evaluating
// the policy BEFORE registering a new peer (markPeerConnected) produces the
// same result in both session-based call paths.
//
// Scenario: node has 1 usable peer (aggregate=limited). A second peer
// connects. If we evaluate shouldRequestPeers() before markPeerConnected,
// the snapshot still shows limited → get_peers allowed (true).
// If we evaluated after markPeerConnected, the snapshot would show
// healthy → get_peers skipped (false). This test confirms the "before"
// behavior, which is the normalized contract for both call sites.
func TestShouldRequestPeers_SnapshotBeforeRegistration(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	svc := newTestServiceWithHealth([]*peerHealth{
		{Address: "10.0.0.1:1000", Connected: true, State: peerStateHealthy, LastUsefulReceiveAt: now},
	})
	svc.refreshAggregateStatusLocked()

	// Precondition: 1 usable peer → limited.
	if svc.aggregateStatus.Status != domain.NetworkStatusLimited {
		t.Fatalf("precondition: expected limited, got %s", svc.aggregateStatus.Status)
	}

	// Snapshot taken BEFORE registering the second peer — both call paths
	// do this now, so aggregate still shows limited.
	if !svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=true before second peer registration (limited)")
	}

	// Now simulate what markPeerConnected does for the second peer.
	svc.health[domain.PeerAddress("10.0.0.2:1000")] = &peerHealth{
		Address:             "10.0.0.2:1000",
		Connected:           true,
		State:               peerStateHealthy,
		LastUsefulReceiveAt: now,
	}
	svc.pending[domain.PeerAddress("10.0.0.2:1000")] = nil
	svc.refreshAggregateStatusLocked()

	// After registration: 2 usable peers → healthy → policy says skip.
	if svc.aggregateStatus.Status != domain.NetworkStatusHealthy {
		t.Fatalf("after registration: expected healthy, got %s", svc.aggregateStatus.Status)
	}
	if svc.shouldRequestPeers() {
		t.Fatal("expected shouldRequestPeers=false after second peer registration (healthy)")
	}
}

// TestShouldRequestPeers_AllStatuses is a table-driven test that exhaustively
// verifies the policy for every defined NetworkStatus value.
func TestShouldRequestPeers_AllStatuses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status domain.NetworkStatus
		want   bool
	}{
		{domain.NetworkStatusOffline, true},
		{domain.NetworkStatusReconnecting, true},
		{domain.NetworkStatusLimited, true},
		{domain.NetworkStatusWarning, true},
		{domain.NetworkStatusHealthy, false},
	}

	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()

			svc := &Service{
				health:          make(map[domain.PeerAddress]*peerHealth),
				pending:         make(map[domain.PeerAddress][]pendingFrame),
				aggregateStatus: domain.AggregateStatusSnapshot{Status: tc.status},
			}

			got := svc.shouldRequestPeers()
			if got != tc.want {
				t.Fatalf("shouldRequestPeers() with status %q = %v, want %v",
					tc.status, got, tc.want)
			}
		})
	}
}
