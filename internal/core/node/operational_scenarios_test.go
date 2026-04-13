package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Operational scenario tests for Шаг 8 of
// docs/peer-discovery-conditional-get-peers.ru.md.
//
// These tests drive the conditional-get-peers policy end-to-end at the
// scenario level: aggregate status → shouldRequestPeers → syncPeerSession.
// The intent is not to re-cover wire compatibility (Шаг 7 already does
// that in compatibility_get_peers_test.go) but to prove that the policy
// produces the expected decision for each operational scenario called out
// by the design document:
//
//   - cold start: aggregate is not healthy → get_peers is sent;
//   - steady-state: aggregate is healthy → get_peers is skipped;
//   - reconnect while healthy: repeated syncs do not silently revive
//     peer exchange;
//   - reconnect after reaching healthy: policy does not roll back on the
//     next sync cycle;
//   - loss of all connections: once aggregate drops out of healthy, the
//     recovery path regains access to get_peers;
//   - mixed-version: already proven by the Шаг 7 compatibility suite —
//     see the comment on TestScenario_MixedVersion_CoveredByCompatSuite.
//
// The tests deliberately exercise the full syncPeerSession happy path
// (over net.Pipe + mockResponder) rather than shouldRequestPeers() alone,
// because the guarantee the document makes is about the wire-level effect
// of the policy, not just the boolean. Pure shouldRequestPeers() tests
// already live in peer_exchange_policy_test.go and are not duplicated
// here.

// scenarioSession wires a peerSession over a net.Pipe and starts the
// mockResponder goroutine that collects the sequence of frame types sent
// by syncPeerSession. Callers drive the session via syncPeerSession and
// then close the local end to unblock the mock.
type scenarioSession struct {
	session    *peerSession
	local      net.Conn
	remote     net.Conn
	receivedCh chan []string
}

func newScenarioSession(
	t *testing.T,
	peerAddr domain.PeerAddress,
	responses map[string]protocol.Frame,
) *scenarioSession {
	t.Helper()
	local, remote := net.Pipe()

	s := &peerSession{
		address: peerAddr,
		conn:    local,
		metered: NewMeteredConn(local),
		inboxCh: make(chan protocol.Frame, 16),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 16),
	}
	// syncPeerSession routes only through session.netCore.sendRawSyncBlocking,
	// never through writeJSONFrame, so the NetCore does not need to be
	// registered in any Service.conns.
	attachTestNetCore(nil, s)

	recv := make(chan []string, 1)
	go func() {
		recv <- mockResponder(t, remote, s, responses)
	}()

	return &scenarioSession{
		session:    s,
		local:      local,
		remote:     remote,
		receivedCh: recv,
	}
}

func (s *scenarioSession) finish(t *testing.T) []string {
	t.Helper()
	_ = s.local.Close()
	return <-s.receivedCh
}

// fullSyncResponses returns the response map that a healthy peer would
// produce if the initiator sent both get_peers and fetch_contacts. When
// the initiator only sends fetch_contacts, the get_peers entry is simply
// unused — this keeps all scenarios on a single response surface.
func fullSyncResponses(peers []string) map[string]protocol.Frame {
	return map[string]protocol.Frame{
		"get_peers": {
			Type:  "peers",
			Peers: peers,
		},
		"fetch_contacts": {
			Type:     "contacts",
			Contacts: []protocol.ContactFrame{},
		},
	}
}

// containsFrame reports whether the given frame type appears anywhere in
// the sequence the mock responder received. Used to assert presence /
// absence of get_peers across scenarios.
func containsFrame(seq []string, ftype string) bool {
	for _, s := range seq {
		if s == ftype {
			return true
		}
	}
	return false
}

// TestScenario_ColdStart_UsesGetPeers — cold start scenario.
//
// The node is Offline (no peers connected at all), which is the standard
// shouldRequestPeers() == true case. syncPeerSession must send get_peers
// followed by fetch_contacts, and the peers returned must be imported
// via addPeerAddress (peer set seeding).
func TestScenario_ColdStart_UsesGetPeers(t *testing.T) {
	t.Parallel()

	svc := newSyncTestService()
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusOffline,
	}

	if !svc.shouldRequestPeers() {
		t.Fatal("cold start: shouldRequestPeers() must be true when aggregate is offline")
	}

	discovered := []string{"198.51.100.30:9000", "198.51.100.31:9000"}
	sc := newScenarioSession(
		t,
		domain.PeerAddress("198.51.100.1:9000"),
		fullSyncResponses(discovered),
	)

	if err := svc.syncPeerSession(sc.session, true, peerExchangePathSessionOutbound); err != nil {
		t.Fatalf("cold start syncPeerSession: %v", err)
	}
	received := sc.finish(t)

	if !containsFrame(received, "get_peers") {
		t.Fatalf("cold start must send get_peers, got %v", received)
	}
	if !containsFrame(received, "fetch_contacts") {
		t.Fatalf("cold start must still send fetch_contacts, got %v", received)
	}

	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != len(discovered) {
		t.Fatalf("cold start must import discovered peers: got %d, want %d",
			peerCount, len(discovered))
	}
}

// TestScenario_SteadyState_SkipsGetPeers — steady-state scenario.
//
// The node's aggregate is already Healthy. shouldRequestPeers() must
// return false, and syncPeerSession called with that decision must send
// only fetch_contacts on the wire. No address is imported through
// peer exchange — discovery is expected to continue via announce_peer
// gossip (outside this test's scope).
func TestScenario_SteadyState_SkipsGetPeers(t *testing.T) {
	t.Parallel()

	svc := newSyncTestService()
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusHealthy,
	}

	if svc.shouldRequestPeers() {
		t.Fatal("steady state: shouldRequestPeers() must be false when aggregate is healthy")
	}

	sc := newScenarioSession(
		t,
		domain.PeerAddress("198.51.100.2:9000"),
		fullSyncResponses(nil),
	)

	if err := svc.syncPeerSession(sc.session, false, peerExchangePathSessionOutbound); err != nil {
		t.Fatalf("steady state syncPeerSession: %v", err)
	}
	received := sc.finish(t)

	if containsFrame(received, "get_peers") {
		t.Fatalf("steady state must NOT send get_peers, got %v", received)
	}
	if !containsFrame(received, "fetch_contacts") {
		t.Fatalf("steady state must still send fetch_contacts, got %v", received)
	}

	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 0 {
		t.Fatalf("steady state must not import peers via peer exchange: got %d", peerCount)
	}
}

// TestScenario_ReconnectWhileHealthy_DoesNotReinitiate — reconnect to
// the same peer while aggregate stays Healthy.
//
// Two consecutive syncPeerSession calls on the same service instance,
// each simulating a separate reconnect, must both skip get_peers. This
// is the main invariant the design document asks for: a stable node
// must not keep firing peer exchange just because peers reconnect.
func TestScenario_ReconnectWhileHealthy_DoesNotReinitiate(t *testing.T) {
	t.Parallel()

	svc := newSyncTestService()
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusHealthy,
	}
	peerAddr := domain.PeerAddress("198.51.100.3:9000")

	for i := 0; i < 2; i++ {
		if svc.shouldRequestPeers() {
			t.Fatalf("reconnect #%d: shouldRequestPeers() must remain false while healthy", i+1)
		}

		sc := newScenarioSession(t, peerAddr, fullSyncResponses(nil))
		if err := svc.syncPeerSession(sc.session, false, peerExchangePathSessionOutbound); err != nil {
			t.Fatalf("reconnect #%d syncPeerSession: %v", i+1, err)
		}
		received := sc.finish(t)

		if containsFrame(received, "get_peers") {
			t.Fatalf("reconnect #%d must NOT send get_peers while healthy, got %v", i+1, received)
		}
	}

	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 0 {
		t.Fatalf("reconnect while healthy must not import peers: got %d", peerCount)
	}
}

// TestScenario_ReconnectAfterReachingHealthy_NoRollback — the node was
// Offline and did one peer exchange; then it reached Healthy. The next
// reconnect must observe the new state and skip get_peers. This closes
// the "policy is evaluated at each sync call site, not fixed at
// startup" requirement of the design document.
func TestScenario_ReconnectAfterReachingHealthy_NoRollback(t *testing.T) {
	t.Parallel()

	svc := newSyncTestService()
	peerAddr := domain.PeerAddress("198.51.100.4:9000")

	// Phase 1: still bootstrapping — aggregate Offline.
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusOffline,
	}
	if !svc.shouldRequestPeers() {
		t.Fatal("phase 1: shouldRequestPeers must be true while not healthy")
	}

	sc1 := newScenarioSession(
		t,
		peerAddr,
		fullSyncResponses([]string{"198.51.100.40:9000"}),
	)
	if err := svc.syncPeerSession(sc1.session, true, peerExchangePathSessionOutbound); err != nil {
		t.Fatalf("phase 1 syncPeerSession: %v", err)
	}
	received1 := sc1.finish(t)
	if !containsFrame(received1, "get_peers") {
		t.Fatalf("phase 1 must send get_peers, got %v", received1)
	}

	// Phase 2: aggregate flips to Healthy — next reconnect must skip.
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusHealthy,
	}
	if svc.shouldRequestPeers() {
		t.Fatal("phase 2: shouldRequestPeers must be false once healthy")
	}

	sc2 := newScenarioSession(t, peerAddr, fullSyncResponses(nil))
	if err := svc.syncPeerSession(sc2.session, false, peerExchangePathSessionOutbound); err != nil {
		t.Fatalf("phase 2 syncPeerSession: %v", err)
	}
	received2 := sc2.finish(t)
	if containsFrame(received2, "get_peers") {
		t.Fatalf("phase 2 must NOT send get_peers after reaching healthy, got %v", received2)
	}
}

// TestScenario_LossOfAllConnections_ReenablesRecovery — loss of all
// connections scenario.
//
// A previously Healthy node drops out of healthy (usable peers fall to
// 0, aggregate becomes Offline). The next sync on the recovery path
// must be allowed to send get_peers again. This keeps get_peers as a
// full-recovery emergency tool, as promised by the design document.
func TestScenario_LossOfAllConnections_ReenablesRecovery(t *testing.T) {
	t.Parallel()

	svc := newSyncTestService()
	peerAddr := domain.PeerAddress("198.51.100.5:9000")

	// Start Healthy — steady-state, no get_peers expected.
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusHealthy,
	}
	if svc.shouldRequestPeers() {
		t.Fatal("pre-loss: shouldRequestPeers must be false while healthy")
	}

	// Simulate loss of all connections: aggregate drops to Offline.
	svc.aggregateStatus = domain.AggregateStatusSnapshot{
		Status: domain.NetworkStatusOffline,
	}
	if !svc.shouldRequestPeers() {
		t.Fatal("post-loss: shouldRequestPeers must be true after losing all connections")
	}

	sc := newScenarioSession(
		t,
		peerAddr,
		fullSyncResponses([]string{"198.51.100.50:9000"}),
	)
	if err := svc.syncPeerSession(sc.session, true, peerExchangePathSessionOutbound); err != nil {
		t.Fatalf("recovery syncPeerSession: %v", err)
	}
	received := sc.finish(t)
	if !containsFrame(received, "get_peers") {
		t.Fatalf("recovery path must re-enable get_peers after loss of all connections, got %v", received)
	}

	// The recovered sync must actually import the advertised peers so
	// that announce_peer gossip can resume from a seeded peer set.
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount == 0 {
		t.Fatalf("recovery path must import peers via get_peers, got %d", peerCount)
	}
}

// TestScenario_IntermediateStates_KeepPeerExchangeEnabled verifies that
// the three non-healthy aggregate states besides Offline — reconnecting,
// limited, warning — all keep peer exchange enabled. The design document
// lists them explicitly as allowed get_peers triggers, so we pin each of
// them with a table test. Without this, a future change that narrows
// shouldRequestPeers() to "only Offline" would silently slip through.
func TestScenario_IntermediateStates_KeepPeerExchangeEnabled(t *testing.T) {
	t.Parallel()

	cases := []domain.NetworkStatus{
		domain.NetworkStatusReconnecting,
		domain.NetworkStatusLimited,
		domain.NetworkStatusWarning,
	}

	for _, status := range cases {
		status := status
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			svc := newSyncTestService()
			svc.aggregateStatus = domain.AggregateStatusSnapshot{
				Status: status,
			}
			if !svc.shouldRequestPeers() {
				t.Fatalf("status %s: shouldRequestPeers must be true (not healthy)", status)
			}

			sc := newScenarioSession(
				t,
				domain.PeerAddress("198.51.100.6:9000"),
				fullSyncResponses([]string{"198.51.100.60:9000"}),
			)
			if err := svc.syncPeerSession(sc.session, true, peerExchangePathSessionOutbound); err != nil {
				t.Fatalf("status %s: syncPeerSession: %v", status, err)
			}
			received := sc.finish(t)
			if !containsFrame(received, "get_peers") {
				t.Fatalf("status %s must send get_peers, got %v", status, received)
			}
		})
	}
}

// TestScenario_MixedVersion_CoveredByCompatSuite is a pointer test: it
// does not exercise additional behaviour itself, but it documents the
// mixed-version cross-link required by Шаг 8. The actual coverage lives
// in compatibility_get_peers_test.go:
//
//   - TestCompatibility_NewInitiatorAgainstLegacyResponder
//     (new initiator → legacy responder)
//   - TestCompatibility_LegacyInitiatorRespondedPeersShape
//     (legacy initiator → new responder)
//   - TestMixedVersionLegacyPeerExchange (service_test.go)
//     (full hello/auth/get_peers/fetch_contacts flow)
//
// Duplicating the wire-level assertions here would add maintenance cost
// without adding coverage; keeping a single pointer test guarantees that
// a future split of the compatibility suite still surfaces in the
// operational scenario list.
func TestScenario_MixedVersion_CoveredByCompatSuite(t *testing.T) {
	t.Parallel()

	// Sanity-check that the compatibility tests referenced above still
	// exist as package-level test symbols. If someone deletes or renames
	// them, this test fails and forces the scenario list to be updated.
	// We do this by name via reflection-free means: the test binary
	// only links if the symbols are valid identifiers in this package.
	_ = TestCompatibility_NewInitiatorAgainstLegacyResponder
	_ = TestCompatibility_LegacyInitiatorRespondedPeersShape
	_ = TestMixedVersionLegacyPeerExchange
}
