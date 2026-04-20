package node

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/gazeta"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// drainPipe creates a net.Pipe and starts a goroutine that drains reads on
// the remote end so that writes to the returned conn never block.  The
// remote end is closed automatically when the test finishes.
func drainPipe(t *testing.T) net.Conn {
	t.Helper()
	conn, remote := net.Pipe()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = remote.Close()
	})
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	return conn
}

// TestRespondToInboxRequestNilSession verifies that respondToInboxRequest
// does not panic when called with a nil session.
func TestRespondToInboxRequestNilSession(t *testing.T) {
	svc := &Service{}
	// Must not panic.
	svc.respondToInboxRequest(nil)
}

// TestRespondToInboxRequestEmptyInbox verifies that respondToInboxRequest
// produces no frames when there are no pending messages for the peer.
func TestRespondToInboxRequestEmptyInbox(t *testing.T) {
	svc := &Service{}
	svc.initMaps()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	session := &peerSession{
		address: "peer-addr",
		conn:    clientConn,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.respondToInboxRequest(session)
	}()

	// Close the writer side so the reader unblocks.
	select {
	case <-done:
		// respondToInboxRequest finished without writing anything.
	case <-time.After(2 * time.Second):
		t.Fatal("respondToInboxRequest hung — expected it to return quickly for empty inbox")
	}
}

// TestHandleInboundPushMessageNilItem verifies that handleInboundPushMessage
// silently returns when frame.Item is nil.
func TestHandleInboundPushMessageNilItem(t *testing.T) {
	svc := &Service{}
	svc.initMaps()

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	// Must not panic.
	svc.handleInboundPushMessage(mustConnIDForTest(svc, conn), protocol.Frame{
		Type: "push_message",
		Item: nil,
	})
}

// TestHandleInboundPushDeliveryReceiptNilReceipt verifies that
// handleInboundPushDeliveryReceipt silently returns when frame.Receipt is nil.
func TestHandleInboundPushDeliveryReceiptNilReceipt(t *testing.T) {
	svc := &Service{}
	svc.initMaps()

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	// Must not panic.
	svc.handleInboundPushDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:    "push_delivery_receipt",
		Receipt: nil,
	})
}

// TestRespondToInboxRequestPushesMessages is an integration-level test that
// verifies messages stored for a peer are pushed over the session connection
// when respondToInboxRequest is called. It requires a fully initialised
// Service with identity and stored messages.
func TestRespondToInboxRequestPushesMessages(t *testing.T) {
	svc := &Service{}
	svc.initMaps()

	peerIdentity := "fcb566d960d139435d317a7e50127f293285b1eb"

	// Manually insert a message addressed to the peer by identity fingerprint.
	envelope := protocol.Envelope{
		ID:        "msg-001",
		Topic:     "dm",
		Sender:    "some-sender",
		Recipient: peerIdentity,
		Payload:   []byte("encrypted-payload"),
		CreatedAt: time.Now().UTC(),
	}
	svc.mu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.mu.Unlock()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	// respondToInboxRequest's session-local reply path probes s.Network()
	// first and falls back to session.netCore when the ConnID is absent
	// from the backend/registry. This test deliberately skips backend
	// registration to exercise the fallback, so attach a NetCore just
	// like attachOutboundNetCore does in production — the managed writer
	// loop is what drains the pipe on the fallback branch.
	session := &peerSession{
		address:      domain.PeerAddress(peerIdentity),
		peerIdentity: domain.PeerIdentity(peerIdentity),
		conn:         clientConn,
		netCore:      netcore.New(netcore.ConnID(1), clientConn, netcore.Outbound, netcore.Options{}),
	}

	go svc.respondToInboxRequest(session)

	reader := bufio.NewReader(serverConn)
	_ = serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("expected push_message frame, got error: %v", err)
	}

	var pushed protocol.Frame
	if err := json.Unmarshal([]byte(line), &pushed); err != nil {
		t.Fatalf("failed to parse pushed frame: %v", err)
	}
	if pushed.Type != "push_message" {
		t.Errorf("expected type push_message, got %s", pushed.Type)
	}
	if pushed.Recipient != peerIdentity {
		t.Errorf("expected recipient %s, got %s", peerIdentity, pushed.Recipient)
	}
	if pushed.Item == nil {
		t.Fatal("expected non-nil item in push_message")
	}
	if pushed.Item.ID != "msg-001" {
		t.Errorf("expected message id msg-001, got %s", pushed.Item.ID)
	}
}

// TestRespondToInboxRequestUsesIdentityNotTransportAddress verifies that
// respondToInboxRequest fetches messages by peer identity fingerprint, not
// by the transport/dial address.  This is the regression test for the P1 bug
// where dial address != recipient identity caused empty inbox responses.
func TestRespondToInboxRequestUsesIdentityNotTransportAddress(t *testing.T) {
	svc := &Service{}
	svc.initMaps()

	transportAddr := "127.0.0.1:9090"
	peerIdentity := "fcb566d960d139435d317a7e50127f293285b1eb"

	// Store a message keyed by identity fingerprint (as real code does).
	envelope := protocol.Envelope{
		ID:        "msg-002",
		Topic:     "dm",
		Sender:    "another-sender",
		Recipient: peerIdentity,
		Payload:   []byte("encrypted-payload"),
		CreatedAt: time.Now().UTC(),
	}
	svc.mu.Lock()
	svc.topics["dm"] = append(svc.topics["dm"], envelope)
	svc.mu.Unlock()

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	// Transport address differs from identity — this is the normal case
	// in production where the dial address is an IP:port and the identity
	// is the Ed25519 fingerprint.
	// Attach a NetCore so the session-local reply path has a working
	// transport: this test does not register the ConnID with a backend,
	// so the Network() probe misses and the path falls back to
	// session.netCore.
	session := &peerSession{
		address:      domain.PeerAddress(transportAddr),
		peerIdentity: domain.PeerIdentity(peerIdentity),
		conn:         clientConn,
		netCore:      netcore.New(netcore.ConnID(1), clientConn, netcore.Outbound, netcore.Options{}),
	}

	go svc.respondToInboxRequest(session)

	reader := bufio.NewReader(serverConn)
	_ = serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("expected push_message frame when dial addr != identity, got error: %v", err)
	}

	var pushed protocol.Frame
	if err := json.Unmarshal([]byte(line), &pushed); err != nil {
		t.Fatalf("failed to parse pushed frame: %v", err)
	}
	if pushed.Type != "push_message" {
		t.Errorf("expected type push_message, got %s", pushed.Type)
	}
	if pushed.Recipient != peerIdentity {
		t.Errorf("expected recipient to be identity %s, got %s", peerIdentity, pushed.Recipient)
	}
	if pushed.Item == nil {
		t.Fatal("expected non-nil item in push_message")
	}
	if pushed.Item.ID != "msg-002" {
		t.Errorf("expected message id msg-002, got %s", pushed.Item.ID)
	}
}

// initMaps initialises the Service maps required for the tests.  In
// production the maps are set up by New(), but tests that create a bare
// Service need this helper.
func (s *Service) initMaps() {
	// Match the production NewService default: keep runCtx non-nil so that
	// code paths deriving ctx from s.runCtx (sender-key recovery etc.)
	// do not panic in tests that construct &Service{} directly.
	if s.runCtx == nil {
		s.runCtx = context.Background()
	}
	s.topics = make(map[string][]protocol.Envelope)
	s.seen = make(map[string]struct{})
	s.seenReceipts = make(map[string]struct{})
	s.known = make(map[string]struct{})
	s.sessions = make(map[domain.PeerAddress]*peerSession)
	s.subs = make(map[string]map[string]*subscriber)
	s.receipts = make(map[string][]protocol.DeliveryReceipt)
	s.notices = make(map[string]gazeta.Notice)
	s.conns = make(map[netcore.ConnID]*connEntry)
	s.connIDByNetConn = make(map[net.Conn]netcore.ConnID)
	s.pending = make(map[domain.PeerAddress][]pendingFrame)
	s.pendingKeys = make(map[string]struct{})
	s.orphaned = make(map[domain.PeerAddress][]pendingFrame)
	s.relayRetry = make(map[string]relayAttempt)
	s.outbound = make(map[string]outboundDelivery)
	s.upstream = make(map[domain.PeerAddress]struct{})
	s.health = make(map[domain.PeerAddress]*peerHealth)
	s.peerTypes = make(map[domain.PeerAddress]domain.NodeType)
	s.peerIDs = make(map[domain.PeerAddress]domain.PeerIdentity)
	s.peerVersions = make(map[domain.PeerAddress]string)
	s.peerBuilds = make(map[domain.PeerAddress]int)
	s.pubKeys = make(map[string]string)
	s.boxKeys = make(map[string]string)
	s.boxSigs = make(map[string]string)
	s.bans = make(map[string]banEntry)
	s.events = make(map[chan protocol.LocalChangeEvent]struct{})
	s.inboundHealthRefs = make(map[domain.PeerAddress]int)
	s.dialOrigin = make(map[domain.PeerAddress]domain.PeerAddress)
	s.persistedMeta = make(map[domain.PeerAddress]*peerEntry)
	s.observedAddrs = make(map[domain.PeerIdentity]string)
	s.reachableGroups = make(map[domain.NetGroup]struct{})
	s.relayStates = newRelayStateStore()
}

// ---------------------------------------------------------------------------
// push_delivery_receipt identity binding tests
// ---------------------------------------------------------------------------

// TestPushDeliveryReceiptRejectsUnrelatedRecipient verifies that
// handleInboundPushDeliveryReceipt drops a receipt whose Recipient does
// not match the local identity and has no active subscriber. This prevents
// an authenticated peer from spoofing delivery state for foreign conversations.
func TestPushDeliveryReceiptRejectsUnrelatedRecipient(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "local-identity-aaa"}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	receiptFrame := protocol.ReceiptFrame{
		MessageID:   "msg-1",
		Sender:      "foreign-sender",
		Recipient:   "foreign-recipient",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	svc.handleInboundPushDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:    "push_delivery_receipt",
		Receipt: &receiptFrame,
	})

	svc.mu.RLock()
	count := len(svc.receipts["foreign-recipient"])
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("receipt with unrelated Recipient should not be stored, got %d", count)
	}
}

// TestPushDeliveryReceiptAcceptsOwnIdentity verifies that
// handleInboundPushDeliveryReceipt stores a receipt whose Recipient matches
// the local node identity.
func TestPushDeliveryReceiptAcceptsOwnIdentity(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	conn := drainPipe(t)

	receiptFrame := protocol.ReceiptFrame{
		MessageID:   "msg-2",
		Sender:      "some-sender",
		Recipient:   id.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	svc.handleInboundPushDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:    "push_delivery_receipt",
		Receipt: &receiptFrame,
	})

	svc.mu.RLock()
	count := len(svc.receipts[id.Address])
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("receipt for own identity should be stored, got %d", count)
	}
}

// TestPushDeliveryReceiptAcceptsActiveSubscriber verifies that a full-node
// relay accepts a receipt whose Recipient matches an active subscriber
// identity, even if it differs from the local node identity.
func TestPushDeliveryReceiptAcceptsActiveSubscriber(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// Register a subscriber for a client identity.
	// hasSubscriber is the only behaviour exercised here; it reads
	// len(s.subs[recipient]) and never touches connID. A zero connID keeps
	// the subscriber synthetic and compatible with the post-PR 10.3b schema.
	clientID := "client-identity-bbb"
	_ = drainPipe(t) // preserved for test symmetry; no longer wired to the subscriber.
	svc.subs[clientID] = map[string]*subscriber{
		"sub-1": {id: "sub-1", recipient: clientID},
	}

	conn := drainPipe(t)

	receiptFrame := protocol.ReceiptFrame{
		MessageID:   "msg-3",
		Sender:      "some-sender",
		Recipient:   clientID,
		Status:      "seen",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	svc.handleInboundPushDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:    "push_delivery_receipt",
		Receipt: &receiptFrame,
	})

	svc.mu.RLock()
	count := len(svc.receipts[clientID])
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("receipt for active subscriber should be stored, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// relay_delivery_receipt identity binding tests
// ---------------------------------------------------------------------------

// TestRelayDeliveryReceiptGossipsFallbackForUnrelatedRecipient verifies that
// handleInboundRelayDeliveryReceipt falls back to gossip for a receipt whose
// Recipient does not match the local identity, has no active subscriber, and
// has no relay forwarding state. The receipt is not stored locally but is
// broadcast via gossipReceipt to routing targets — consistent with the
// relay contract in retryRelayDeliveries.
func TestRelayDeliveryReceiptGossipsFallbackForUnrelatedRecipient(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "local-identity-aaa"}

	// Seed a gossip target peer so routingTargetsForRecipient returns
	// a non-empty list and gossipReceipt has somewhere to send.
	gossipTarget := domain.PeerAddress("gossip-target-peer:64646")
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	svc.handleInboundRelayDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-relay-1",
		Address:     "foreign-sender",
		Recipient:   "foreign-recipient",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	// Receipt must NOT be stored locally.
	svc.mu.RLock()
	count := len(svc.receipts["foreign-recipient"])
	banCount := len(svc.bans)
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("relay receipt with unrelated Recipient should not be stored, got %d", count)
	}
	// No ban scoring — transit receipts are legitimate traffic, not abuse.
	if banCount != 0 {
		t.Fatalf("transit receipt must not populate blacklist, got %d ban entries", banCount)
	}

	// Verify that gossipReceipt actually forwarded the receipt to the
	// routing target. gossipReceipt runs in a goroutine and spawns
	// another goroutine per target, so use a short deadline.
	select {
	case gossipped := <-svc.sessions[gossipTarget].sendCh:
		if gossipped.Type != "relay_delivery_receipt" {
			t.Fatalf("gossipped frame type = %q, want relay_delivery_receipt", gossipped.Type)
		}
		if gossipped.ID != "msg-relay-1" {
			t.Fatalf("gossipped frame ID = %q, want msg-relay-1", gossipped.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gossip fallback did not deliver receipt to routing target within timeout")
	}
}

// TestRelayDeliveryReceiptForwardsTransitReceipt verifies that
// handleInboundRelayDeliveryReceipt forwards a non-local receipt
// along the relay chain when relay forwarding state exists for the
// message ID. The receipt must not be stored locally and must not
// trigger ban scoring.
func TestRelayDeliveryReceiptForwardsTransitReceipt(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "transit-node-aaa"}

	// Seed relay state: receipt for "msg-transit-1" should be forwarded
	// to "original-sender-bbb".
	forwardTarget := domain.PeerAddress("original-sender-bbb")
	svc.relayStates.states["msg-transit-1"] = &relayForwardState{
		MessageID:        "msg-transit-1",
		ReceiptForwardTo: forwardTarget,
		RemainingTTL:     5,
	}

	// Create a session for the forward target with mesh_relay_v1
	// capability and a buffered sendCh so enqueuePeerFrame succeeds.
	svc.sessions[forwardTarget] = &peerSession{
		address:      forwardTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 1),
		authOK:       true,
	}
	// activePeerSession requires Connected health with a recent pong
	// to avoid peerStateStalled — seed a minimal health entry.
	svc.health[forwardTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	svc.handleInboundRelayDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-transit-1",
		Address:     "some-sender",
		Recipient:   "remote-recipient-ccc",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	// Receipt must NOT be stored locally — this node is only a transit hop.
	svc.mu.RLock()
	localCount := len(svc.receipts["remote-recipient-ccc"])
	transitBanCount := len(svc.bans)
	svc.mu.RUnlock()
	if localCount != 0 {
		t.Fatalf("transit receipt should not be stored locally, got %d", localCount)
	}
	// No ban scoring — forwarded transit receipts are legitimate traffic.
	if transitBanCount != 0 {
		t.Fatalf("transit forwarding must not populate blacklist, got %d ban entries", transitBanCount)
	}

	// Verify the receipt was forwarded via the session sendCh.
	select {
	case forwarded := <-svc.sessions[forwardTarget].sendCh:
		if forwarded.Type != "relay_delivery_receipt" {
			t.Fatalf("forwarded frame type = %q, want relay_delivery_receipt", forwarded.Type)
		}
		if forwarded.ID != "msg-transit-1" {
			t.Fatalf("forwarded frame ID = %q, want msg-transit-1", forwarded.ID)
		}
	default:
		t.Fatal("receipt was not forwarded to the relay chain target")
	}
}

// TestSessionRelayDeliveryReceiptForwardsTransitReceipt verifies that
// dispatchPeerSessionFrame forwards a non-local relay_delivery_receipt
// along the relay chain when relay state exists. This is the primary
// production path — authenticated peers exchange frames via sessions,
// not raw inbound TCP.
func TestSessionRelayDeliveryReceiptForwardsTransitReceipt(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "transit-session-node"}

	// Seed relay state: receipt for "msg-sess-transit-1" should be
	// forwarded to "original-sender-sess".
	forwardTarget := domain.PeerAddress("original-sender-sess")
	svc.relayStates.states["msg-sess-transit-1"] = &relayForwardState{
		MessageID:        "msg-sess-transit-1",
		ReceiptForwardTo: forwardTarget,
		RemainingTTL:     5,
	}

	// Create a session for the forward target with mesh_relay_v1 and
	// a buffered sendCh so enqueuePeerFrame succeeds.
	svc.sessions[forwardTarget] = &peerSession{
		address:      forwardTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 1),
		authOK:       true,
	}
	svc.health[forwardTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	// Simulate the sending peer (where the receipt arrives from).
	senderAddr := domain.PeerAddress("relay-hop-peer")
	senderSession := &peerSession{
		address: senderAddr,
		sendCh:  make(chan protocol.Frame, 1),
		authOK:  true,
	}

	svc.dispatchPeerSessionFrame(senderAddr, senderSession, protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-sess-transit-1",
		Address:     "some-sender",
		Recipient:   "remote-recipient-ddd",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	// Receipt must NOT be stored locally.
	svc.mu.RLock()
	localCount := len(svc.receipts["remote-recipient-ddd"])
	sessBanCount := len(svc.bans)
	svc.mu.RUnlock()
	if localCount != 0 {
		t.Fatalf("transit receipt via session should not be stored locally, got %d", localCount)
	}
	if sessBanCount != 0 {
		t.Fatalf("session transit forwarding must not populate blacklist, got %d ban entries", sessBanCount)
	}

	// Verify the receipt was forwarded to the relay chain target.
	select {
	case forwarded := <-svc.sessions[forwardTarget].sendCh:
		if forwarded.Type != "relay_delivery_receipt" {
			t.Fatalf("forwarded frame type = %q, want relay_delivery_receipt", forwarded.Type)
		}
		if forwarded.ID != "msg-sess-transit-1" {
			t.Fatalf("forwarded frame ID = %q, want msg-sess-transit-1", forwarded.ID)
		}
	default:
		t.Fatal("session-path receipt was not forwarded to the relay chain target")
	}
}

// TestSessionRelayDeliveryReceiptGossipsFallbackForUnknown verifies that
// dispatchPeerSessionFrame falls back to gossip for a relay_delivery_receipt
// with no local relevance and no relay forwarding state. The receipt is not
// stored locally but is broadcast via gossipReceipt to routing targets.
func TestSessionRelayDeliveryReceiptGossipsFallbackForUnknown(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "session-node-local"}

	// Seed a gossip target peer for routingTargetsForRecipient.
	gossipTarget := domain.PeerAddress("gossip-target-sess:64646")
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	senderAddr := domain.PeerAddress("some-peer")
	senderSession := &peerSession{
		address: senderAddr,
		sendCh:  make(chan protocol.Frame, 1),
		authOK:  true,
	}

	svc.dispatchPeerSessionFrame(senderAddr, senderSession, protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-unknown-1",
		Address:     "foreign-sender",
		Recipient:   "foreign-recipient",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	// Receipt must NOT be stored locally.
	svc.mu.RLock()
	count := len(svc.receipts["foreign-recipient"])
	sessGossipBanCount := len(svc.bans)
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("unknown receipt via session should not be stored, got %d", count)
	}
	if sessGossipBanCount != 0 {
		t.Fatalf("session gossip fallback must not populate blacklist, got %d ban entries", sessGossipBanCount)
	}

	// Verify gossip fallback delivered the receipt to the routing target.
	select {
	case gossipped := <-svc.sessions[gossipTarget].sendCh:
		if gossipped.Type != "relay_delivery_receipt" {
			t.Fatalf("gossipped frame type = %q, want relay_delivery_receipt", gossipped.Type)
		}
		if gossipped.ID != "msg-unknown-1" {
			t.Fatalf("gossipped frame ID = %q, want msg-unknown-1", gossipped.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session-path gossip fallback did not deliver receipt to routing target within timeout")
	}
}

// ---------------------------------------------------------------------------
// Transit receipt deduplication tests
// ---------------------------------------------------------------------------

// TestRelayDeliveryReceiptDedupeGossipInbound verifies that a duplicate
// transit receipt arriving on the inbound TCP path is silently dropped
// instead of being re-gossiped. The first call must gossip; the second
// identical call must produce no additional frame on the gossip target.
func TestRelayDeliveryReceiptDedupeGossipInbound(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "dedupe-inbound-local"}

	gossipTarget := domain.PeerAddress("dedupe-gossip-target:7777")
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 8),
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-dedupe-gossip-1",
		Address:     "some-sender",
		Recipient:   "foreign-recipient-zzz",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	connID := mustConnIDForTest(svc, conn)

	// First call — must gossip.
	svc.handleInboundRelayDeliveryReceipt(connID, frame)

	select {
	case got := <-svc.sessions[gossipTarget].sendCh:
		if got.ID != "msg-dedupe-gossip-1" {
			t.Fatalf("first gossip frame ID = %q, want msg-dedupe-gossip-1", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first call did not gossip receipt to routing target within timeout")
	}

	// Second call with identical receipt — must be suppressed.
	svc.handleInboundRelayDeliveryReceipt(connID, frame)

	select {
	case extra := <-svc.sessions[gossipTarget].sendCh:
		t.Fatalf("duplicate transit receipt was re-gossiped: got frame ID=%q", extra.ID)
	case <-time.After(200 * time.Millisecond):
		// Expected: no frame within a reasonable window.
	}
}

// TestRelayDeliveryReceiptDedupeForwardInbound verifies that a duplicate
// transit receipt arriving on the inbound TCP path is not re-forwarded via
// the relay chain. The first call must forward; the second must be dropped.
func TestRelayDeliveryReceiptDedupeForwardInbound(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "dedupe-forward-local"}

	forwardTarget := domain.PeerAddress("dedupe-forward-target")
	svc.relayStates.states["msg-dedupe-fwd-1"] = &relayForwardState{
		MessageID:        "msg-dedupe-fwd-1",
		ReceiptForwardTo: forwardTarget,
		RemainingTTL:     5,
	}
	svc.sessions[forwardTarget] = &peerSession{
		address:      forwardTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health[forwardTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-dedupe-fwd-1",
		Address:     "some-sender",
		Recipient:   "remote-recipient-yyy",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	connID := mustConnIDForTest(svc, conn)

	// First call — must forward via relay chain.
	svc.handleInboundRelayDeliveryReceipt(connID, frame)

	select {
	case got := <-svc.sessions[forwardTarget].sendCh:
		if got.ID != "msg-dedupe-fwd-1" {
			t.Fatalf("first forwarded frame ID = %q, want msg-dedupe-fwd-1", got.ID)
		}
	default:
		t.Fatal("first call did not forward receipt via relay chain")
	}

	// Second call with identical receipt — must be suppressed by dedupe.
	svc.handleInboundRelayDeliveryReceipt(connID, frame)

	select {
	case extra := <-svc.sessions[forwardTarget].sendCh:
		t.Fatalf("duplicate transit receipt was re-forwarded: got frame ID=%q", extra.ID)
	default:
		// Expected: no frame — dedupe suppressed the duplicate.
	}
}

// TestSessionRelayDeliveryReceiptDedupeGossip verifies that a duplicate
// transit receipt arriving on the session path is silently dropped instead
// of being re-gossiped. Mirrors TestRelayDeliveryReceiptDedupeGossipInbound
// for the dispatchPeerSessionFrame entry point.
func TestSessionRelayDeliveryReceiptDedupeGossip(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "dedupe-sess-gossip-local"}

	gossipTarget := domain.PeerAddress("dedupe-sess-gossip-target:8888")
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 8),
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	senderAddr := domain.PeerAddress("dedupe-sender-peer")
	senderSession := &peerSession{
		address: senderAddr,
		sendCh:  make(chan protocol.Frame, 1),
		authOK:  true,
	}

	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-dedupe-sess-gossip-1",
		Address:     "some-sender",
		Recipient:   "foreign-recipient-xxx",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	// First call — must gossip.
	svc.dispatchPeerSessionFrame(senderAddr, senderSession, frame)

	select {
	case got := <-svc.sessions[gossipTarget].sendCh:
		if got.ID != "msg-dedupe-sess-gossip-1" {
			t.Fatalf("first gossip frame ID = %q, want msg-dedupe-sess-gossip-1", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first call did not gossip receipt to routing target within timeout")
	}

	// Second call — must be suppressed.
	svc.dispatchPeerSessionFrame(senderAddr, senderSession, frame)

	select {
	case extra := <-svc.sessions[gossipTarget].sendCh:
		t.Fatalf("duplicate transit receipt was re-gossiped via session: got frame ID=%q", extra.ID)
	case <-time.After(200 * time.Millisecond):
		// Expected: no frame.
	}
}

// TestSessionRelayDeliveryReceiptDedupeForward verifies that a duplicate
// transit receipt arriving on the session path is not re-forwarded via
// the relay chain. Mirrors TestRelayDeliveryReceiptDedupeForwardInbound
// for the dispatchPeerSessionFrame entry point.
func TestSessionRelayDeliveryReceiptDedupeForward(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "dedupe-sess-fwd-local"}

	forwardTarget := domain.PeerAddress("dedupe-sess-fwd-target")
	svc.relayStates.states["msg-dedupe-sess-fwd-1"] = &relayForwardState{
		MessageID:        "msg-dedupe-sess-fwd-1",
		ReceiptForwardTo: forwardTarget,
		RemainingTTL:     5,
	}
	svc.sessions[forwardTarget] = &peerSession{
		address:      forwardTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health[forwardTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	senderAddr := domain.PeerAddress("dedupe-sess-sender")
	senderSession := &peerSession{
		address: senderAddr,
		sendCh:  make(chan protocol.Frame, 1),
		authOK:  true,
	}

	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-dedupe-sess-fwd-1",
		Address:     "some-sender",
		Recipient:   "remote-recipient-www",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	// First call — must forward via relay chain.
	svc.dispatchPeerSessionFrame(senderAddr, senderSession, frame)

	select {
	case got := <-svc.sessions[forwardTarget].sendCh:
		if got.ID != "msg-dedupe-sess-fwd-1" {
			t.Fatalf("first forwarded frame ID = %q, want msg-dedupe-sess-fwd-1", got.ID)
		}
	default:
		t.Fatal("first call did not forward receipt via relay chain (session)")
	}

	// Second call — must be suppressed by dedupe.
	svc.dispatchPeerSessionFrame(senderAddr, senderSession, frame)

	select {
	case extra := <-svc.sessions[forwardTarget].sendCh:
		t.Fatalf("duplicate transit receipt was re-forwarded via session: got frame ID=%q", extra.ID)
	default:
		// Expected: no frame.
	}
}

// ---------------------------------------------------------------------------
// Transit receipt dedupe recovery tests
// ---------------------------------------------------------------------------

// TestRelayDeliveryReceiptRetryAfterNoTargetsInbound verifies that a transit
// receipt is NOT permanently suppressed when the first arrival finds no routing
// targets. Scenario: receipt arrives → no relay path, no gossip targets → NOT
// marked as seen → gossip target appears → same receipt arrives again → gossip
// succeeds. This prevents one-shot loss of transiently unroutable receipts.
func TestRelayDeliveryReceiptRetryAfterNoTargetsInbound(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "recovery-inbound-local"}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-recovery-1",
		Address:     "some-sender",
		Recipient:   "foreign-recipient-recovery",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	connID := mustConnIDForTest(svc, conn)

	// First call — no relay state, no gossip targets. Receipt must NOT be
	// marked as seen so it can be retried.
	svc.handleInboundRelayDeliveryReceipt(connID, frame)

	// Verify: receipt not stored locally and not marked in seenReceipts.
	svc.mu.RLock()
	key := "foreign-recipient-recovery:msg-recovery-1:delivered"
	_, markedAfterFirst := svc.seenReceipts[key]
	svc.mu.RUnlock()
	if markedAfterFirst {
		t.Fatal("receipt must NOT be marked as seen when no targets exist on first arrival")
	}

	// Now simulate route recovery: add a gossip target peer.
	gossipTarget := domain.PeerAddress("recovery-gossip-target:7777")
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	// Second call — same receipt, now with targets. Must be gossiped.
	svc.handleInboundRelayDeliveryReceipt(connID, frame)

	select {
	case got := <-svc.sessions[gossipTarget].sendCh:
		if got.ID != "msg-recovery-1" {
			t.Fatalf("recovered gossip frame ID = %q, want msg-recovery-1", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receipt was not gossiped after route recovery — one-shot loss detected")
	}

	// Verify: now marked as seen after successful gossip.
	// gossipTransitReceipt is synchronous, so the mark is immediate.
	svc.mu.RLock()
	_, markedAfterSecond := svc.seenReceipts[key]
	svc.mu.RUnlock()
	if !markedAfterSecond {
		t.Fatal("receipt must be marked as seen after successful gossip delivery")
	}
}

// TestSessionRelayDeliveryReceiptRetryAfterNoTargets mirrors the inbound
// recovery test for the session path (dispatchPeerSessionFrame).
func TestSessionRelayDeliveryReceiptRetryAfterNoTargets(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "recovery-session-local"}

	senderAddr := domain.PeerAddress("recovery-sender-peer")
	senderSession := &peerSession{
		address: senderAddr,
		sendCh:  make(chan protocol.Frame, 1),
		authOK:  true,
	}

	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-recovery-sess-1",
		Address:     "some-sender",
		Recipient:   "foreign-recipient-sess-recovery",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	// First call — no relay state, no gossip targets.
	svc.dispatchPeerSessionFrame(senderAddr, senderSession, frame)

	// dispatchPeerSessionFrame pre-marks the receipt as seen (to suppress
	// rapid-fire duplicates) and then launches gossipTransitReceipt as a
	// goroutine. When no routing targets exist the goroutine calls
	// unmarkTransitReceiptSeen, but this happens asynchronously. Poll
	// until the goroutine completes the unmark.
	key := "foreign-recipient-sess-recovery:msg-recovery-sess-1:delivered"
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		svc.mu.RLock()
		_, marked := svc.seenReceipts[key]
		svc.mu.RUnlock()
		if !marked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	svc.mu.RLock()
	_, markedAfterFirst := svc.seenReceipts[key]
	svc.mu.RUnlock()
	if markedAfterFirst {
		t.Fatal("receipt must NOT be marked as seen when no targets exist (session)")
	}

	// Simulate route recovery.
	gossipTarget := domain.PeerAddress("recovery-sess-gossip-target:8888")
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       make(chan protocol.Frame, 4),
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}

	// Second call — same receipt, targets available.
	svc.dispatchPeerSessionFrame(senderAddr, senderSession, frame)

	select {
	case got := <-svc.sessions[gossipTarget].sendCh:
		if got.ID != "msg-recovery-sess-1" {
			t.Fatalf("recovered gossip frame ID = %q, want msg-recovery-sess-1", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receipt was not gossiped after route recovery (session) — one-shot loss detected")
	}

	// Verify: marked as seen after success.
	// The pre-mark in dispatchPeerSessionFrame marks the receipt before
	// the goroutine runs; gossipTransitReceipt only unmarks on failure.
	svc.mu.RLock()
	_, markedAfterSecond := svc.seenReceipts[key]
	svc.mu.RUnlock()
	if !markedAfterSecond {
		t.Fatal("receipt must be marked as seen after successful gossip delivery (session)")
	}
}

// TestRelayDeliveryReceiptRetryAfterAllSendsFail verifies that
// gossipTransitReceipt does NOT permanently suppress a transit receipt when
// routing targets exist but every sendReceiptToPeer call fails (all sendCh
// channels full, pending queues saturated). The receipt must remain eligible
// for retry on a future call after capacity recovers.
//
// gossipTransitReceipt is the shared helper invoked by both
// handleInboundRelayDeliveryReceipt and dispatchPeerSessionFrame, so a
// single direct-call test covers the retry/dedupe logic for both paths.
// Dispatcher wiring (handler → gossipTransitReceipt) is independently
// pinned by TestRelayDeliveryReceiptGossipsFallbackForUnrelatedRecipient
// (inbound) and TestSessionRelayDeliveryReceiptGossipsFallbackForUnknown
// (session).
//
// Direct invocation makes all assertions fully synchronous — no sleeps or
// timing assumptions.
func TestRelayDeliveryReceiptRetryAfterAllSendsFail(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "sendfail-inbound-local"}

	// Create a gossip target with a sendCh of capacity 1, pre-filled so
	// the next non-blocking send fails. Also fill the pending queue to
	// maxPendingFramesPerPeer so queuePeerFrame also fails.
	gossipTarget := domain.PeerAddress("sendfail-gossip-target:9999")
	fullCh := make(chan protocol.Frame, 1)
	fullCh <- protocol.Frame{Type: "filler"}
	svc.sessions[gossipTarget] = &peerSession{
		address:      gossipTarget,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       fullCh,
		authOK:       true,
	}
	svc.health[gossipTarget] = &peerHealth{
		Connected:  true,
		LastPongAt: time.Now().UTC(),
	}
	// Fill the pending queue so queuePeerFrame also returns false.
	fillerFrames := make([]pendingFrame, maxPendingFramesPerPeer)
	for i := range fillerFrames {
		fillerFrames[i] = pendingFrame{Frame: protocol.Frame{Type: "filler", ID: fmt.Sprintf("filler-%d", i)}}
	}
	svc.pending[gossipTarget] = fillerFrames

	receipt := protocol.DeliveryReceipt{
		MessageID:   "msg-sendfail-1",
		Sender:      "some-sender",
		Recipient:   "foreign-recipient-sendfail",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC(),
	}

	// Pre-mark before gossip (matches production caller contract).
	// All sends fail → gossipTransitReceipt unmarks → receipt eligible for retry.
	svc.markTransitReceiptSeen(receipt)
	svc.gossipTransitReceipt(receipt)

	svc.mu.RLock()
	key := "foreign-recipient-sendfail:msg-sendfail-1:delivered"
	_, markedAfterFail := svc.seenReceipts[key]
	svc.mu.RUnlock()
	if markedAfterFail {
		t.Fatal("receipt must NOT be marked as seen when all sends failed")
	}

	// Simulate recovery: drain the filler frame to free sendCh capacity,
	// and clear the pending queue so queuePeerFrame can also succeed.
	<-svc.sessions[gossipTarget].sendCh
	svc.mu.Lock()
	svc.pending[gossipTarget] = nil
	svc.mu.Unlock()

	// Retry — pre-mark again, capacity recovered. Must deliver and stay marked.
	svc.markTransitReceiptSeen(receipt)
	svc.gossipTransitReceipt(receipt)

	select {
	case got := <-svc.sessions[gossipTarget].sendCh:
		if got.ID != "msg-sendfail-1" {
			t.Fatalf("recovered gossip frame ID = %q, want msg-sendfail-1", got.ID)
		}
	default:
		t.Fatal("receipt was not gossiped after send recovery — one-shot loss detected")
	}

	// Verify: now marked as seen after successful delivery.
	svc.mu.RLock()
	_, markedAfterSuccess := svc.seenReceipts[key]
	svc.mu.RUnlock()
	if !markedAfterSuccess {
		t.Fatal("receipt must be marked as seen after successful send")
	}
}

// TestRelayDeliveryReceiptAcceptsOwnIdentity verifies that
// handleInboundRelayDeliveryReceipt stores a relay receipt whose
// Recipient matches the local node identity.
func TestRelayDeliveryReceiptAcceptsOwnIdentity(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	svc.handleInboundRelayDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-relay-2",
		Address:     "some-sender",
		Recipient:   id.Address,
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	svc.mu.RLock()
	count := len(svc.receipts[id.Address])
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("relay receipt for own identity should be stored, got %d", count)
	}
}

// TestRelayDeliveryReceiptAcceptsActiveSubscriber verifies that a full-node
// relay accepts a relay receipt whose Recipient matches an active subscriber.
func TestRelayDeliveryReceiptAcceptsActiveSubscriber(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// hasSubscriber only reads len(s.subs[recipient]); connID is irrelevant
	// for this path after the PR 10.3b subscriber re-key.
	clientID := "client-identity-ccc"
	svc.subs[clientID] = map[string]*subscriber{
		"sub-1": {id: "sub-1", recipient: clientID},
	}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	svc.handleInboundRelayDeliveryReceipt(mustConnIDForTest(svc, conn), protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-relay-3",
		Address:     "some-sender",
		Recipient:   clientID,
		Status:      "seen",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	svc.mu.RLock()
	count := len(svc.receipts[clientID])
	svc.mu.RUnlock()
	if count != 1 {
		t.Fatalf("relay receipt for active subscriber should be stored, got %d", count)
	}
}
