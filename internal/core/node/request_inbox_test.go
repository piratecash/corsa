package node

import (
	"bufio"
	"context"
	"encoding/json"
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
	svc.handleInboundPushMessage(conn, protocol.Frame{
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
	svc.handleInboundPushDeliveryReceipt(conn, protocol.Frame{
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

	// respondToInboxRequest writes through writeSessionFrame, which
	// routes via session.netCore — attach a NetCore just like
	// attachOutboundNetCore does in production so the managed writer
	// loop drains the pipe.
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
	// Attach a NetCore to satisfy writeSessionFrame's session.netCore
	// precondition (session-local reply path, PR 9.4a P1 fix).
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
	s.conns = make(map[net.Conn]*connEntry)
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

	svc.handleInboundPushDeliveryReceipt(conn, protocol.Frame{
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

	svc.handleInboundPushDeliveryReceipt(conn, protocol.Frame{
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
	clientID := "client-identity-bbb"
	subConn := drainPipe(t)
	svc.subs[clientID] = map[string]*subscriber{
		"sub-1": {id: "sub-1", recipient: clientID, conn: subConn},
	}

	conn := drainPipe(t)

	receiptFrame := protocol.ReceiptFrame{
		MessageID:   "msg-3",
		Sender:      "some-sender",
		Recipient:   clientID,
		Status:      "seen",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	svc.handleInboundPushDeliveryReceipt(conn, protocol.Frame{
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

// TestRelayDeliveryReceiptRejectsUnrelatedRecipient verifies that
// handleInboundRelayDeliveryReceipt drops a relay receipt whose Recipient
// does not match the local identity and has no active subscriber.
func TestRelayDeliveryReceiptRejectsUnrelatedRecipient(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	svc.initMaps()
	svc.identity = &identity.Identity{Address: "local-identity-aaa"}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	svc.handleInboundRelayDeliveryReceipt(conn, protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-relay-1",
		Address:     "foreign-sender",
		Recipient:   "foreign-recipient",
		Status:      "delivered",
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})

	svc.mu.RLock()
	count := len(svc.receipts["foreign-recipient"])
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("relay receipt with unrelated Recipient should not be stored, got %d", count)
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

	svc.handleInboundRelayDeliveryReceipt(conn, protocol.Frame{
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

	clientID := "client-identity-ccc"
	subConn, _ := net.Pipe()
	defer func() { _ = subConn.Close() }()
	svc.subs[clientID] = map[string]*subscriber{
		"sub-1": {id: "sub-1", recipient: clientID, conn: subConn},
	}

	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()

	svc.handleInboundRelayDeliveryReceipt(conn, protocol.Frame{
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
