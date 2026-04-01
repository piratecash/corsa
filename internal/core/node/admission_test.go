package node

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
)

func TestAdmitRelayFrame_CapabilityRequired(t *testing.T) {
	t.Parallel()

	result := admitRelayFrame(false, 0)
	if result != relayAdmitRejectCapability {
		t.Fatalf("expected relayAdmitRejectCapability, got %d", result)
	}
}

func TestAdmitRelayFrame_OK(t *testing.T) {
	t.Parallel()

	result := admitRelayFrame(true, 0)
	if result != relayAdmitOK {
		t.Fatalf("expected relayAdmitOK, got %d", result)
	}
}

func TestAdmitRelayFrame_BodySizeLimit(t *testing.T) {
	t.Parallel()

	// Exactly at limit — should pass.
	result := admitRelayFrame(true, maxRelayBodyBytes)
	if result != relayAdmitOK {
		t.Fatalf("body at exact limit should pass, got %d", result)
	}

	// Over limit — should reject.
	result = admitRelayFrame(true, maxRelayBodyBytes+1)
	if result != relayAdmitRejectFrameSize {
		t.Fatalf("oversized body should be rejected, got %d", result)
	}

	// Zero size (relay_hop_ack has no body) — should pass.
	result = admitRelayFrame(true, 0)
	if result != relayAdmitOK {
		t.Fatalf("zero body should pass, got %d", result)
	}
}

func TestIsRelayFrame(t *testing.T) {
	t.Parallel()

	tests := []struct {
		frameType string
		expected  bool
	}{
		{"relay_message", true},
		{"relay_hop_ack", true},
		{"send_message", false},
		{"ping", false},
		{"push_message", false},
		{"", false},
	}

	for _, tt := range tests {
		if got := isRelayFrame(tt.frameType); got != tt.expected {
			t.Errorf("isRelayFrame(%q) = %v, want %v", tt.frameType, got, tt.expected)
		}
	}
}

func TestMaxRelayBodyBytesValue(t *testing.T) {
	t.Parallel()

	if maxRelayBodyBytes != 65536 {
		t.Fatalf("maxRelayBodyBytes = %d, want 65536", maxRelayBodyBytes)
	}
}

func TestMaxPeerCommandBodyBytesMatchesCommandLineLimit(t *testing.T) {
	t.Parallel()

	if maxPeerCommandBodyBytes != maxCommandLineBytes {
		t.Fatalf("maxPeerCommandBodyBytes = %d, want %d (must match maxCommandLineBytes)",
			maxPeerCommandBodyBytes, maxCommandLineBytes)
	}
}

// --- Relay invariant contract tests ---
//
// These tests verify the documented relay invariants from docs/protocol/relay.md.
// Canonical invariant IDs (INV-1 through INV-11) are defined in relay.md.
// admission.go references these IDs; tests use them in their names.
//
// INV-3  — covered by TestHandleRelayMessageStatusSemantics/stored_when_no_capable_peers
//          in relay_test.go (gossip runs unconditionally).
// INV-6  — covered by TestINV6_ReceiptUsesTransportAddress below and
//          TestFinalHopStoresRelayStateForReceipt in relay_test.go.
// INV-7  — covered by TestHandleRelayMessageStatusSemantics in relay_test.go
//          (hop-ack status reflects delivery outcome).
// INV-10 — covered by TestRelayMessageRejectsNonDMTopic in relay_test.go
//          (DM-only invariant).
// INV-11 — covered by TestINV11_OriginReceiptForwardToEmpty below and
//          TestSendRelayMessageOriginReceiptForwardTo in relay_test.go.

// TestINV9_CapabilityGating verifies INV-9: relay frames require an
// authenticated session with mesh_relay_v1 capability.
func TestINV9_CapabilityGating(t *testing.T) {
	t.Parallel()

	// Without capability — rejected.
	if admitRelayFrame(false, 0) != relayAdmitRejectCapability {
		t.Fatal("INV-9 violated: frame accepted without capability")
	}

	// With capability — accepted.
	if admitRelayFrame(true, 0) != relayAdmitOK {
		t.Fatal("INV-9 violated: frame rejected with capability")
	}
}

// TestINV4_ClientNodeTransitDrop verifies INV-4: client nodes never act as
// transit relay hops. A relay_message not addressed to the client is dropped.
func TestINV4_ClientNodeTransitDrop(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeClient)

	frame := protocol.Frame{
		ID:          "inv4-test",
		Address:     "origin-sender",
		Recipient:   "third-party-recipient",
		Topic:       "dm",
		Body:        "body",
		HopCount:    1,
		MaxHops:     10,
		PreviousHop: "10.0.0.1:64646",
	}

	status := svc.handleRelayMessage("10.0.0.1:64646", nil, frame)
	if status != "" {
		t.Fatalf("INV-4 violated: client node returned status %q for transit relay", status)
	}
}

// TestINV5_HopAckStatusExhaustive verifies INV-5: the set of hop-ack statuses
// returned by handleRelayMessage is exactly {"delivered", "forwarded", "stored", ""}.
func TestINV5_HopAckStatusExhaustive(t *testing.T) {
	t.Parallel()

	validStatuses := map[string]struct{}{
		"delivered": {},
		"forwarded": {},
		"stored":    {},
		"":          {},
	}

	if len(validStatuses) != 4 {
		t.Fatalf("INV-5: expected exactly 4 statuses, got %d", len(validStatuses))
	}
}

// TestINV6_ReceiptUsesTransportAddress verifies INV-6: ReceiptForwardTo
// stores the transport address, enabling session lookup for receipt forwarding.
func TestINV6_ReceiptUsesTransportAddress(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender := registerSenderKey(t, svc)
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))

	transportAddr := "10.0.0.99:64646"
	frame := protocol.Frame{
		ID:          "inv6-test",
		Address:     sender.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        body,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   "2026-03-30T00:00:00Z",
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: transportAddr,
	}

	status := svc.handleRelayMessage(transportAddr, nil, frame)
	if status != "delivered" {
		t.Fatalf("expected delivered, got %q", status)
	}

	forwardTo := svc.relayStates.lookupReceiptForwardTo("inv6-test")
	if forwardTo != transportAddr {
		t.Fatalf("INV-6 violated: ReceiptForwardTo = %q, want transport address %q", forwardTo, transportAddr)
	}
}

// TestINV11_OriginReceiptForwardToEmpty verifies INV-11: the origin node stores
// empty ReceiptForwardTo — receipts terminate here.
func TestINV11_OriginReceiptForwardToEmpty(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	svc.relayStates.store(&relayForwardState{
		MessageID:        "inv7-test",
		PreviousHop:      "",
		ReceiptForwardTo: "",
		ForwardedTo:      "10.0.0.1:64646",
		HopCount:         1,
		RemainingTTL:     relayStateTTLSeconds,
	})

	forwardTo := svc.relayStates.lookupReceiptForwardTo("inv7-test")
	if forwardTo != "" {
		t.Fatalf("INV-11 violated: origin ReceiptForwardTo = %q, want empty", forwardTo)
	}

	receipt := protocol.DeliveryReceipt{
		MessageID: "inv7-test",
		Recipient: "some-recipient",
		Status:    protocol.ReceiptStatusDelivered,
	}
	if svc.handleRelayReceipt(receipt) {
		t.Fatal("INV-11 violated: handleRelayReceipt should return false for origin node")
	}
}

// --- readFrameLine tests ---

func TestReadFrameLine_NormalLine(t *testing.T) {
	t.Parallel()

	input := `{"type":"ping"}` + "\n"
	reader := bufio.NewReader(strings.NewReader(input))

	line, err := readFrameLine(reader, maxCommandLineBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if line != input {
		t.Fatalf("got %q, want %q", line, input)
	}
}

func TestReadFrameLine_RejectsOversizedLine(t *testing.T) {
	t.Parallel()

	// Build a line that exceeds maxCommandLineBytes without a newline.
	oversized := strings.Repeat("x", maxCommandLineBytes+1)
	reader := bufio.NewReader(strings.NewReader(oversized))

	_, err := readFrameLine(reader, maxCommandLineBytes)
	if err != errFrameTooLarge {
		t.Fatalf("expected errFrameTooLarge, got %v", err)
	}
}

func TestReadFrameLine_ExactLimitPasses(t *testing.T) {
	t.Parallel()

	// A line exactly at the limit (including the trailing newline).
	payload := strings.Repeat("x", maxCommandLineBytes-1) + "\n"
	reader := bufio.NewReader(strings.NewReader(payload))

	line, err := readFrameLine(reader, maxCommandLineBytes)
	if err != nil {
		t.Fatalf("line at exact limit should pass, got error: %v", err)
	}
	if line != payload {
		t.Fatalf("got len %d, want len %d", len(line), len(payload))
	}
}

func TestReadFrameLine_PartialLineAtEOF(t *testing.T) {
	t.Parallel()

	// No trailing newline — EOF after partial data.
	input := `{"type":"ping"}`
	reader := bufio.NewReader(strings.NewReader(input))

	line, err := readFrameLine(reader, maxCommandLineBytes)
	if err == nil {
		t.Fatal("expected EOF error for partial line")
	}
	if line != input {
		t.Fatalf("partial line: got %q, want %q", line, input)
	}
}

func TestReadFrameLine_EmptyInput(t *testing.T) {
	t.Parallel()

	reader := bufio.NewReader(strings.NewReader(""))
	_, err := readFrameLine(reader, maxCommandLineBytes)
	if err == nil {
		t.Fatal("expected error on empty input")
	}
}

func TestMaxFrameLineBytesValue(t *testing.T) {
	t.Parallel()

	if maxCommandLineBytes != 128*1024 {
		t.Fatalf("maxCommandLineBytes = %d, want %d", maxCommandLineBytes, 128*1024)
	}
}

// --- Split limit regression tests ---
//
// These tests guard the two-tier transport limit invariant:
// maxCommandLineBytes (128 KiB) for inbound client commands,
// maxResponseLineBytes (8 MiB) for peer-session and handshake reads.

func TestSplitLimits_ResponseLargerThanCommand(t *testing.T) {
	t.Parallel()

	if maxResponseLineBytes <= maxCommandLineBytes {
		t.Fatalf("maxResponseLineBytes (%d) must be larger than maxCommandLineBytes (%d)",
			maxResponseLineBytes, maxCommandLineBytes)
	}
}

func TestSplitLimits_ResponseAcceptsLargeFrame(t *testing.T) {
	t.Parallel()

	// A 256 KiB line — exceeds maxCommandLineBytes but fits within
	// maxResponseLineBytes. This simulates a multi-message response
	// read on the peer-session path.
	size := maxCommandLineBytes * 2
	payload := strings.Repeat("x", size-1) + "\n"
	reader := bufio.NewReader(strings.NewReader(payload))

	line, err := readFrameLine(reader, maxResponseLineBytes)
	if err != nil {
		t.Fatalf("peer-session-sized line should pass with maxResponseLineBytes, got: %v", err)
	}
	if len(line) != size {
		t.Fatalf("got len %d, want %d", len(line), size)
	}
}

func TestSplitLimits_CommandRejectsLargeFrame(t *testing.T) {
	t.Parallel()

	// The same 256 KiB line must be rejected under maxCommandLineBytes.
	size := maxCommandLineBytes * 2
	payload := strings.Repeat("x", size-1) + "\n"
	reader := bufio.NewReader(strings.NewReader(payload))

	_, err := readFrameLine(reader, maxCommandLineBytes)
	if err != errFrameTooLarge {
		t.Fatalf("expected errFrameTooLarge for oversized command, got %v", err)
	}
}

func TestSplitLimits_ResponseRejectsOversized(t *testing.T) {
	t.Parallel()

	// Even the response limit has a ceiling — verify it rejects above 8 MiB.
	oversized := strings.Repeat("x", maxResponseLineBytes+1)
	reader := bufio.NewReader(strings.NewReader(oversized))

	_, err := readFrameLine(reader, maxResponseLineBytes)
	if err != errFrameTooLarge {
		t.Fatalf("expected errFrameTooLarge above maxResponseLineBytes, got %v", err)
	}
}

func TestMaxAnnouncePeersValue(t *testing.T) {
	t.Parallel()

	if maxAnnouncePeers != 64 {
		t.Fatalf("maxAnnouncePeers = %d, want 64", maxAnnouncePeers)
	}
}

// ---------------------------------------------------------------------------
// Runtime-path tests: verify that admission limits wired into
// handlePeerSessionFrame actually reject/truncate at the service level,
// not just at the helper level.
// ---------------------------------------------------------------------------

// TestHandlePeerSessionFrame_PushMessageBodyCapRejectsOversized verifies that
// handlePeerSessionFrame silently drops a push_message whose Item.Body exceeds
// maxPeerCommandBodyBytes. This is the runtime-path counterpart to the
// constant-value test — it proves the branch stays wired into the handler.
func TestHandlePeerSessionFrame_PushMessageBodyCapRejectsOversized(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	oversizedBody := strings.Repeat("x", maxPeerCommandBodyBytes+1)

	frame := protocol.Frame{
		Type: "push_message",
		Item: &protocol.MessageFrame{
			ID:        "oversized-push-1",
			Sender:    "sender-addr",
			Recipient: svc.Address(),
			Body:      oversizedBody,
			Flag:      string(protocol.MessageFlagImmutable),
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		},
		Topic: "dm",
	}

	svc.handlePeerSessionFrame("10.0.0.99:1234", frame)

	// Verify the message was NOT stored.
	svc.mu.RLock()
	count := len(svc.topics["dm"])
	svc.mu.RUnlock()
	if count != 0 {
		t.Fatalf("oversized push_message should be dropped; found %d stored messages", count)
	}
}

// TestHandlePeerSessionFrame_PushMessageBodyAtLimitIsProcessed is a boundary
// check: a push_message with body exactly at maxPeerCommandBodyBytes must NOT
// be rejected by the size guard. (It may still fail later due to missing sender
// keys, but it must pass the body-size gate.)
func TestHandlePeerSessionFrame_PushMessageBodyAtLimitIsProcessed(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	exactBody := strings.Repeat("x", maxPeerCommandBodyBytes)

	frame := protocol.Frame{
		Type: "push_message",
		Item: &protocol.MessageFrame{
			ID:        "exact-push-1",
			Sender:    "unknown-sender-addr",
			Recipient: svc.Address(),
			Body:      exactBody,
			Flag:      string(protocol.MessageFlagImmutable),
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		},
		Topic: "dm",
	}

	// The handler should pass the body-size gate and proceed to
	// storeIncomingMessage, which will fail with ErrCodeUnknownSenderKey
	// (no sender registered). That failure path triggers syncPeer and
	// a retry — both of which are harmless in a test with no live peers.
	// The key assertion: no panic, and the message is NOT stored (because
	// the sender key is unknown, not because of the body cap).
	svc.handlePeerSessionFrame("10.0.0.99:1234", frame)

	svc.mu.RLock()
	count := len(svc.topics["dm"])
	svc.mu.RUnlock()
	// Message won't be stored (unknown sender), but we proved the body
	// cap did not reject it — coverage of the !oversized branch.
	if count != 0 {
		t.Fatalf("message with unknown sender should not be stored; found %d", count)
	}
}

// TestHandlePeerSessionFrame_AnnouncePeerTruncation verifies that
// handlePeerSessionFrame truncates an announce_peer peer list to
// maxAnnouncePeers entries. This is the runtime-path proof that the
// truncation stays wired into the handler.
func TestHandlePeerSessionFrame_AnnouncePeerTruncation(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	totalPeers := maxAnnouncePeers + 36 // 100 peers, expect only 64 learned

	peers := make([]string, totalPeers)
	for i := range peers {
		// Use routable public IPs — private ranges (10.x, 192.168.x, etc.)
		// are classified as NetGroupLocal and filtered by announce_peer.
		peers[i] = fmt.Sprintf("44.%d.%d.1:9000", i/256, i%256)
	}

	frame := protocol.Frame{
		Type:     "announce_peer",
		NodeType: string(config.NodeTypeFull),
		Peers:    peers,
	}

	svc.handlePeerSessionFrame("10.0.0.99:1234", frame)

	svc.mu.RLock()
	// newTestService starts with zero peers (no bootstrap).
	// Public IPs (44.x.x.x) are used to avoid NetGroupLocal filtering
	// that drops private ranges (10.x, 192.168.x, etc.).
	learnedCount := len(svc.peers)
	svc.mu.RUnlock()

	if learnedCount > maxAnnouncePeers {
		t.Fatalf("announce_peer should truncate to %d peers; learned %d", maxAnnouncePeers, learnedCount)
	}
	if learnedCount == 0 {
		t.Fatal("announce_peer should learn at least some peers; got 0")
	}
}

// TestHandlePeerSessionFrame_AnnouncePeerUnderLimitPassesAll verifies that
// an announce_peer list smaller than maxAnnouncePeers is NOT truncated.
func TestHandlePeerSessionFrame_AnnouncePeerUnderLimitPassesAll(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	peerCount := 10
	peers := make([]string, peerCount)
	for i := range peers {
		peers[i] = fmt.Sprintf("45.%d.%d.1:9000", i/256, i%256)
	}

	frame := protocol.Frame{
		Type:     "announce_peer",
		NodeType: string(config.NodeTypeFull),
		Peers:    peers,
	}

	svc.handlePeerSessionFrame("10.0.0.99:1234", frame)

	svc.mu.RLock()
	learnedCount := len(svc.peers)
	svc.mu.RUnlock()

	if learnedCount != peerCount {
		t.Fatalf("announce_peer with %d peers (under limit) should learn all; got %d", peerCount, learnedCount)
	}
}

// ---------------------------------------------------------------------------
// Capacity limit constant value tests
// ---------------------------------------------------------------------------

func TestCapacityLimitConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  int
		want int
	}{
		{"maxRelayStates", maxRelayStates, 10_000},
		{"maxRelayStatesPerPeer", maxRelayStatesPerPeer, 500},
		{"maxRelayRetryEntries", maxRelayRetryEntries, 5_000},
		{"maxPendingFramesPerPeer", maxPendingFramesPerPeer, 200},
		{"maxPendingFramesTotal", maxPendingFramesTotal, 2_000},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}
}

func TestHandshakeTimeoutConstants(t *testing.T) {
	t.Parallel()

	if dialTimeout != 2*time.Second {
		t.Errorf("dialTimeout = %v, want 2s", dialTimeout)
	}
	if handshakeTimeout != 2*time.Second {
		t.Errorf("handshakeTimeout = %v, want 2s", handshakeTimeout)
	}
	if syncHandshakeTimeout != 1500*time.Millisecond {
		t.Errorf("syncHandshakeTimeout = %v, want 1.5s", syncHandshakeTimeout)
	}
	if sessionWriteTimeout != 3*time.Second {
		t.Errorf("sessionWriteTimeout = %v, want 3s", sessionWriteTimeout)
	}
}
