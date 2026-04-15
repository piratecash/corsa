package node

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

// attachTestNetCore wires a standalone outbound NetCore onto a peerSession
// that was constructed manually (without going through attachOutboundNetCore
// and the Service-level registration). peerSessionRequest requires
// session.netCore to be non-nil under the single-writer invariant; tests
// that drive an outbound session over net.Pipe use this helper to mirror
// production.
// The svc parameter is retained for call-site compatibility but is not
// used: session-local reply paths probe s.Network() first and fall back to
// session.netCore when the ConnID is absent from the backend/registry — so
// a manually-built session whose ConnID was never registered still
// delivers reply frames through this attached NetCore.
func attachTestNetCore(_ *Service, session *peerSession) {
	if session == nil || session.conn == nil || session.netCore != nil {
		return
	}
	session.netCore = netcore.New(netcore.ConnID(1), session.conn, netcore.Outbound, netcore.Options{})
}

// newSyncTestService creates a minimal Service for testing syncPeerSession().
// It has enough fields populated to avoid nil-pointer panics in the code paths
// exercised by syncPeerSession (peerSessionRequest, addPeerAddress,
// syncContactsViaSession).
func newSyncTestService() *Service {
	return &Service{
		health:        make(map[domain.PeerAddress]*peerHealth),
		pending:       make(map[domain.PeerAddress][]pendingFrame),
		peers:         []transport.Peer{},
		known:         make(map[string]struct{}),
		peerTypes:     make(map[domain.PeerAddress]domain.NodeType),
		peerIDs:       make(map[domain.PeerAddress]domain.PeerIdentity),
		persistedMeta: make(map[domain.PeerAddress]*peerEntry),
		dialOrigin:    make(map[domain.PeerAddress]domain.PeerAddress),
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
		aggregateStatus: domain.AggregateStatusSnapshot{
			Status: domain.NetworkStatusOffline,
		},
	}
}

// mockResponder reads JSON frame lines from the remote side of a net.Pipe
// and feeds corresponding responses into session.inboxCh. It allows the test
// to verify which frame types were sent and in what order.
//
// Returns a slice of request types that were received.
func mockResponder(t *testing.T, remote net.Conn, session *peerSession, responses map[string]protocol.Frame) []string {
	t.Helper()
	reader := bufio.NewReader(remote)
	var received []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		frame, err := protocol.ParseFrameLine(line)
		if err != nil {
			t.Logf("mockResponder: parse error: %v (line: %q)", err, line)
			continue
		}
		received = append(received, frame.Type)
		if resp, ok := responses[frame.Type]; ok {
			session.inboxCh <- resp
		}
	}
	return received
}

// TestSyncPeerSession_RequestPeersTrue verifies that when requestPeers=true,
// syncPeerSession sends get_peers, processes the response (addPeerAddress),
// and then proceeds with fetch_contacts.
func TestSyncPeerSession_RequestPeersTrue(t *testing.T) {
	t.Parallel()

	peerAddr := domain.PeerAddress("10.0.0.50:9000")
	svc := newSyncTestService()

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()

	session := &peerSession{
		address: peerAddr,
		conn:    local,
		metered: netcore.NewMeteredConn(local),
		inboxCh: make(chan protocol.Frame, 16),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 16),
	}
	attachTestNetCore(svc, session)

	// Responses the mock peer will send for each request type.
	responses := map[string]protocol.Frame{
		"get_peers": {
			Type:  "peers",
			Peers: []string{"10.0.0.100:9000", "10.0.0.101:9000"},
		},
		"fetch_contacts": {
			Type:     "contacts",
			Contacts: []protocol.ContactFrame{},
		},
	}

	receivedCh := make(chan []string, 1)
	go func() {
		received := mockResponder(t, remote, session, responses)
		receivedCh <- received
	}()

	err := svc.syncPeerSession(session, true, peerExchangePathSessionOutbound)
	// Close local to unblock mockResponder.
	_ = local.Close()

	if err != nil {
		t.Fatalf("syncPeerSession(requestPeers=true) error: %v", err)
	}

	received := <-receivedCh

	// Expect both get_peers and fetch_contacts were sent.
	if len(received) != 2 {
		t.Fatalf("expected 2 requests (get_peers + fetch_contacts), got %d: %v", len(received), received)
	}
	if received[0] != "get_peers" {
		t.Errorf("expected first request to be get_peers, got %q", received[0])
	}
	if received[1] != "fetch_contacts" {
		t.Errorf("expected second request to be fetch_contacts, got %q", received[1])
	}

	// Verify peers were added via addPeerAddress.
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 2 {
		t.Errorf("expected 2 peers added, got %d", peerCount)
	}
}

// TestSyncPeerSession_RequestPeersFalse verifies that when requestPeers=false
// (healthy state), syncPeerSession skips get_peers entirely and only sends
// fetch_contacts. Crucially:
//   - get_peers is NOT sent
//   - addPeerAddress is NOT called
//   - NewPeersDiscovered hint is NOT emitted
//   - fetch_contacts (syncContactsViaSession) still executes
func TestSyncPeerSession_RequestPeersFalse(t *testing.T) {
	t.Parallel()

	peerAddr := domain.PeerAddress("10.0.0.51:9000")
	svc := newSyncTestService()

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()

	session := &peerSession{
		address: peerAddr,
		conn:    local,
		metered: netcore.NewMeteredConn(local),
		inboxCh: make(chan protocol.Frame, 16),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 16),
	}
	attachTestNetCore(svc, session)

	// Only fetch_contacts response needed — get_peers should not be sent.
	responses := map[string]protocol.Frame{
		"fetch_contacts": {
			Type:     "contacts",
			Contacts: []protocol.ContactFrame{},
		},
	}

	receivedCh := make(chan []string, 1)
	go func() {
		received := mockResponder(t, remote, session, responses)
		receivedCh <- received
	}()

	err := svc.syncPeerSession(session, false, peerExchangePathSessionOutbound)
	_ = local.Close()

	if err != nil {
		t.Fatalf("syncPeerSession(requestPeers=false) error: %v", err)
	}

	received := <-receivedCh

	// Expect only fetch_contacts — get_peers must NOT be present.
	if len(received) != 1 {
		t.Fatalf("expected 1 request (fetch_contacts only), got %d: %v", len(received), received)
	}
	if received[0] != "fetch_contacts" {
		t.Errorf("expected only fetch_contacts, got %q", received[0])
	}

	// Verify no peers were added (addPeerAddress was not called).
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 0 {
		t.Errorf("expected 0 peers (get_peers skipped), got %d", peerCount)
	}
}

// TestSyncPeerSession_SkipDoesNotEmitNewPeersDiscovered verifies that
// when get_peers is skipped (requestPeers=false), the NewPeersDiscovered
// hint is not emitted to ConnectionManager, even when a CM is present.
func TestSyncPeerSession_SkipDoesNotEmitNewPeersDiscovered(t *testing.T) {
	t.Parallel()

	peerAddr := domain.PeerAddress("10.0.0.52:9000")
	svc := newSyncTestService()

	// Create a CM with the emit gate open but no event loop running.
	// This lets EmitHint deliver into hintEvents without the event loop
	// draining the channel before the test can observe it.
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 4 },
		NowFn:      func() time.Time { return time.Now() },
	})
	cm.accepting.Store(1)
	svc.connManager = cm

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()

	session := &peerSession{
		address: peerAddr,
		conn:    local,
		metered: netcore.NewMeteredConn(local),
		inboxCh: make(chan protocol.Frame, 16),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 16),
	}
	attachTestNetCore(svc, session)

	responses := map[string]protocol.Frame{
		"fetch_contacts": {
			Type:     "contacts",
			Contacts: []protocol.ContactFrame{},
		},
	}

	receivedCh := make(chan []string, 1)
	go func() {
		received := mockResponder(t, remote, session, responses)
		receivedCh <- received
	}()

	err := svc.syncPeerSession(session, false, peerExchangePathSessionOutbound)
	_ = local.Close()

	if err != nil {
		t.Fatalf("syncPeerSession error: %v", err)
	}

	<-receivedCh

	// Directly verify hintEvents channel is empty — no NewPeersDiscovered
	// should have been emitted because the get_peers block was skipped.
	select {
	case hint := <-cm.hintEvents:
		t.Fatalf("expected no hint events, but received: %T", hint)
	default:
		// Channel empty — correct: no hint was emitted.
	}
}

// TestSyncPeerSession_RequestPeersTrue_EmitsNewPeersDiscovered verifies
// that when requestPeers=true and peers are returned, the NewPeersDiscovered
// hint IS emitted to ConnectionManager.
func TestSyncPeerSession_RequestPeersTrue_EmitsNewPeersDiscovered(t *testing.T) {
	t.Parallel()

	peerAddr := domain.PeerAddress("10.0.0.53:9000")
	svc := newSyncTestService()

	// Create a CM with the emit gate open but no event loop running.
	// This lets EmitHint deliver into hintEvents without the event loop
	// draining the channel before the test can observe it.
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 4 },
		NowFn:      func() time.Time { return time.Now() },
	})
	cm.accepting.Store(1)
	svc.connManager = cm

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()

	session := &peerSession{
		address: peerAddr,
		conn:    local,
		metered: netcore.NewMeteredConn(local),
		inboxCh: make(chan protocol.Frame, 16),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 16),
	}
	attachTestNetCore(svc, session)

	responses := map[string]protocol.Frame{
		"get_peers": {
			Type:  "peers",
			Peers: []string{"10.0.0.200:9000"},
		},
		"fetch_contacts": {
			Type:     "contacts",
			Contacts: []protocol.ContactFrame{},
		},
	}

	receivedCh := make(chan []string, 1)
	go func() {
		received := mockResponder(t, remote, session, responses)
		receivedCh <- received
	}()

	err := svc.syncPeerSession(session, true, peerExchangePathSessionOutbound)
	_ = local.Close()

	if err != nil {
		t.Fatalf("syncPeerSession error: %v", err)
	}

	received := <-receivedCh

	// Both get_peers and fetch_contacts should have been sent.
	if len(received) < 2 {
		t.Fatalf("expected at least 2 requests, got %d: %v", len(received), received)
	}

	// Verify peers were added.
	svc.mu.RLock()
	peerCount := len(svc.peers)
	svc.mu.RUnlock()
	if peerCount != 1 {
		t.Errorf("expected 1 peer added, got %d", peerCount)
	}

	// Directly verify that NewPeersDiscovered hint was emitted to CM.
	// No event loop is running, so the hint sits in hintEvents until we
	// drain it here. Non-blocking read is sufficient: EmitHint already
	// completed synchronously before syncPeerSession returned.
	select {
	case hint := <-cm.hintEvents:
		npd, ok := hint.(NewPeersDiscovered)
		if !ok {
			t.Fatalf("expected NewPeersDiscovered hint, got %T", hint)
		}
		if npd.Count != 1 {
			t.Errorf("expected NewPeersDiscovered.Count=1, got %d", npd.Count)
		}
	default:
		t.Fatal("expected NewPeersDiscovered hint in hintEvents, but channel was empty")
	}
}
