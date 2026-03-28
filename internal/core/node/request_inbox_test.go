package node

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/gazeta"
	"corsa/internal/core/protocol"
)

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

	session := &peerSession{
		address:      peerIdentity,
		peerIdentity: peerIdentity,
		conn:         clientConn,
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
	session := &peerSession{
		address:      transportAddr,
		peerIdentity: peerIdentity,
		conn:         clientConn,
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
	s.topics = make(map[string][]protocol.Envelope)
	s.seen = make(map[string]struct{})
	s.seenReceipts = make(map[string]struct{})
	s.known = make(map[string]struct{})
	s.sessions = make(map[string]*peerSession)
	s.subs = make(map[string]map[string]*subscriber)
	s.receipts = make(map[string][]protocol.DeliveryReceipt)
	s.notices = make(map[string]gazeta.Notice)
	s.connAuth = make(map[net.Conn]*connAuthState)
	s.connPeerInfo = make(map[net.Conn]*connPeerHello)
	s.connSendCh = make(map[net.Conn]chan sendItem)
	s.connWriterDone = make(map[net.Conn]chan struct{})
	s.pending = make(map[string][]pendingFrame)
	s.pendingKeys = make(map[string]struct{})
	s.orphaned = make(map[string][]pendingFrame)
	s.relayRetry = make(map[string]relayAttempt)
	s.outbound = make(map[string]outboundDelivery)
	s.upstream = make(map[string]struct{})
	s.health = make(map[string]*peerHealth)
	s.peerTypes = make(map[string]config.NodeType)
	s.peerIDs = make(map[string]string)
	s.peerVersions = make(map[string]string)
	s.peerBuilds = make(map[string]int)
	s.pubKeys = make(map[string]string)
	s.boxKeys = make(map[string]string)
	s.boxSigs = make(map[string]string)
	s.bans = make(map[string]banEntry)
	s.events = make(map[chan protocol.LocalChangeEvent]struct{})
	s.inboundConns = make(map[net.Conn]struct{})
	s.inboundMetered = make(map[net.Conn]*MeteredConn)
	s.inboundHealthRefs = make(map[string]int)
	s.inboundTracked = make(map[net.Conn]struct{})
	s.dialOrigin = make(map[string]string)
	s.persistedMeta = make(map[string]*peerEntry)
	s.observedAddrs = make(map[string]string)
	s.reachableGroups = make(map[NetGroup]struct{})
}
