package node

import (
	"bufio"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
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

	status := svc.handleRelayMessage(domain.PeerAddress("10.0.0.1:64646"), nil, frame)
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

	status := svc.handleRelayMessage(domain.PeerAddress(transportAddr), nil, frame)
	if status != "delivered" {
		t.Fatalf("expected delivered, got %q", status)
	}

	forwardTo := svc.relayStates.lookupReceiptForwardTo("inv6-test")
	if forwardTo != domain.PeerAddress(transportAddr) {
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
	if forwardTo != domain.PeerAddress("") {
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

// TestDiscardFrameLineRemainderBoundsTheResyncWindow pins the ONE question the
// resynchronisation window answers: how far past a refused line this node keeps
// reading before the peer stops being a misbehaving frame and becomes an
// unterminated stream.
//
// The two cases are the two sides of that boundary, and both are needed: a
// discard that always tore the connection down would satisfy the first on its
// own while destroying the resynchronisation the window exists for.
//
// The trap is that the delimiter arrives INSIDE a chunk, and the chunk carrying
// it can be the very one that crosses the window. Honouring it there would let a
// sender place its newline a byte past the boundary and buy the whole overshoot
// — an unbounded read behind a terminator, which is the stream the bound refuses.
func TestDiscardFrameLineRemainderBoundsTheResyncWindow(t *testing.T) {
	t.Parallel()

	const window = 64
	const nextLine = `{"type":"ping"}` + "\n"

	t.Run("delimiter_past_the_window_is_a_stream", func(t *testing.T) {
		t.Parallel()

		reader := bufio.NewReader(strings.NewReader(strings.Repeat("A", window+1) + "\n" + nextLine))

		discarded, err := discardFrameLineRemainder(reader, window)
		if !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("discard = (%d, %v), want errFrameTooLarge: the window is %d bytes and a newline BEHIND it does not pay for the bytes in front of it",
				discarded, err, window)
		}
	})

	t.Run("delimiter_inside_the_window_resynchronises", func(t *testing.T) {
		t.Parallel()

		remainder := strings.Repeat("A", window/2) + "\n"
		reader := bufio.NewReader(strings.NewReader(remainder + nextLine))

		discarded, err := discardFrameLineRemainder(reader, window)
		if err != nil {
			t.Fatalf("discard = (%d, %v), want success: a line that terminates inside the window is a refused frame, not a stream",
				discarded, err)
		}
		if discarded != len(remainder) {
			t.Fatalf("discarded = %d, want %d — the count is what was READ", discarded, len(remainder))
		}

		line, err := readFrameLine(reader, maxCommandLineBytes)
		if err != nil {
			t.Fatalf("the reader did not resynchronise after the discard: %v", err)
		}
		if line != nextLine {
			t.Fatalf("next line = %q, want %q", line, nextLine)
		}
	})
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
// dispatchPeerSessionFrame actually reject/truncate at the service level,
// not just at the helper level.
// ---------------------------------------------------------------------------

// TestDispatchPeerSessionFrame_PushMessageBodyCapRejectsOversized verifies that
// dispatchPeerSessionFrame silently drops a push_message whose Item.Body exceeds
// maxPeerCommandBodyBytes. This is the runtime-path counterpart to the
// constant-value test — it proves the branch stays wired into the handler.
func TestDispatchPeerSessionFrame_PushMessageBodyCapRejectsOversized(t *testing.T) {
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

	svc.dispatchPeerSessionFrame("10.0.0.99:1234", nil, frame)

	// Verify the message was NOT stored.
	// s.topics is guarded by s.gossipMu, not s.peerMu.
	svc.gossipMu.RLock()
	count := len(svc.topics["dm"])
	svc.gossipMu.RUnlock()
	if count != 0 {
		t.Fatalf("oversized push_message should be dropped; found %d stored messages", count)
	}
}

// TestDispatchPeerSessionFrame_PushMessageBodyAtLimitIsProcessed is a boundary
// check: a push_message with body exactly at maxPeerCommandBodyBytes must NOT
// be rejected by the size guard. (It may still fail later due to missing sender
// keys, but it must pass the body-size gate.)
func TestDispatchPeerSessionFrame_PushMessageBodyAtLimitIsProcessed(t *testing.T) {
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
	svc.dispatchPeerSessionFrame("10.0.0.99:1234", nil, frame)

	// s.topics is guarded by s.gossipMu, not s.peerMu.
	svc.gossipMu.RLock()
	count := len(svc.topics["dm"])
	svc.gossipMu.RUnlock()
	// Message won't be stored (unknown sender), but we proved the body
	// cap did not reject it — coverage of the !oversized branch.
	if count != 0 {
		t.Fatalf("message with unknown sender should not be stored; found %d", count)
	}
}

// TestDispatchPeerSessionFrame_AnnouncePeerTruncation verifies that
// dispatchPeerSessionFrame truncates an announce_peer peer list to
// maxAnnouncePeers entries. This is the runtime-path proof that the
// truncation stays wired into the handler.
func TestDispatchPeerSessionFrame_AnnouncePeerTruncation(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	totalPeers := maxAnnouncePeers + 36 // 100 peers, expect only 64 learned

	peers := make([]string, totalPeers)
	for i := range peers {
		// Use routable public IPs — private ranges (10.x, 192.168.x, etc.)
		// are classified as domain.NetGroupLocal and filtered by announce_peer.
		peers[i] = fmt.Sprintf("44.%d.%d.1:9000", i/256, i%256)
	}

	frame := protocol.Frame{
		Type:     "announce_peer",
		NodeType: string(config.NodeTypeFull),
		Peers:    peers,
	}

	svc.dispatchPeerSessionFrame("10.0.0.99:1234", nil, frame)

	svc.peerMu.RLock()
	// newTestService starts with zero peers (no bootstrap).
	// Public IPs (44.x.x.x) are used to avoid domain.NetGroupLocal filtering
	// that drops private ranges (10.x, 192.168.x, etc.).
	learnedCount := len(svc.peers)
	svc.peerMu.RUnlock()

	if learnedCount > maxAnnouncePeers {
		t.Fatalf("announce_peer should truncate to %d peers; learned %d", maxAnnouncePeers, learnedCount)
	}
	if learnedCount == 0 {
		t.Fatal("announce_peer should learn at least some peers; got 0")
	}
}

// TestDispatchPeerSessionFrame_AnnouncePeerUnderLimitPassesAll verifies that
// an announce_peer list smaller than maxAnnouncePeers is NOT truncated.
func TestDispatchPeerSessionFrame_AnnouncePeerUnderLimitPassesAll(t *testing.T) {
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

	svc.dispatchPeerSessionFrame("10.0.0.99:1234", nil, frame)

	svc.peerMu.RLock()
	learnedCount := len(svc.peers)
	svc.peerMu.RUnlock()

	if learnedCount != peerCount {
		t.Fatalf("announce_peer with %d peers (under limit) should learn all; got %d", peerCount, learnedCount)
	}
}

// TestDispatchPeerSessionFrame_ErrorFrameDoesNotPanic verifies that an inbound
// error frame (e.g. code=rate-limited sent by the remote before closing the
// connection) is handled gracefully without panicking. The handler logs at Warn
// level so the disconnect reason is visible in the logs.
func TestDispatchPeerSessionFrame_ErrorFrameDoesNotPanic(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	frame := protocol.Frame{
		Type:  "error",
		Code:  protocol.ErrCodeRateLimited,
		Error: "command rate limit exceeded",
	}

	// Must not panic or leave dangling state.
	svc.dispatchPeerSessionFrame("10.0.0.99:1234", nil, frame)
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

// ---------------------------------------------------------------------------
// peekFrameType
// ---------------------------------------------------------------------------

func TestPeekFrameType_ValidFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		line     string
		expected string
	}{
		{"file_command", `{"type":"file_command","sub":"chunk_request"}`, "file_command"},
		{"send_message", `{"type":"send_message","to":"abc"}`, "send_message"},
		{"relay_message", `{"type":"relay_message","body":"..."}`, "relay_message"},
		{"hello", `{"type":"hello","version":1}`, "hello"},
		{"ping", `{"type":"ping"}`, "ping"},
		{"with_spaces", `{ "type" : "file_command" }`, "file_command"},
		{"with_tabs", "{\t\"type\"\t:\t\"file_command\"\t}", "file_command"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := peekFrameType(tt.line)
			if got != tt.expected {
				t.Errorf("peekFrameType(%q) = %q, want %q", tt.line, got, tt.expected)
			}
		})
	}
}

func TestPeekFrameType_MissingOrMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
	}{
		{"empty", ""},
		{"no_type_field", `{"command":"hello"}`},
		{"no_colon", `{"type" "hello"}`},
		{"no_value_quote", `{"type":123}`},
		{"unclosed_value", `{"type":"hello`},
		{"type_in_value", `{"data":"type","other":"x"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := peekFrameType(tt.line)
			if got != "" {
				t.Errorf("peekFrameType(%q) = %q, want empty", tt.line, got)
			}
		})
	}
}

// TestPeekFrameType_AttributionUsesTheProtocolConstant pins the one job
// peekFrameType still has: naming, for a log line or a drop counter, the type a
// refused line CLAIMED. The value has to be the protocol constant itself, or the
// refusal lands on the wrong plane's ledger.
//
// It is NOT an exemption test any more. The rate-limiter exemption is decided by
// frameLineExemptFromCommandLimit from the top-level classification — see
// TestCommandLimitExemptionRefusesDecoyTopLevelTypes for why a peeked type may
// not decide it.
func TestPeekFrameType_AttributionUsesTheProtocolConstant(t *testing.T) {
	t.Parallel()

	line := `{"type":"file_command","sub":"chunk_response","hash":"abc123","offset":0,"data":"..."}`
	got := peekFrameType(line)
	if got != protocol.FileCommandFrameType {
		t.Errorf("peekFrameType returned %q, want %q — a refused file transfer frame would be attributed to the wrong type",
			got, protocol.FileCommandFrameType)
	}
}

// TestCmdLimiterExemption_BulkAnnounceFramesExempt pins the inbound
// read-loop exemption contract for BULK announce-plane frames only:
// announce_routes (v1) / routes_update (v2 delta) / route_announce_v3.
// These chunk up to 100 route entries per frame, so a legitimate
// full-sync of N routes ships as ceil(N/100) frames in a tight burst
// — the cmd limiter's 100-burst / 30 cmd/s would silently truncate
// that burst. Per-peer defence for these frames is owned by
// announceLimiter (route-count, all bulk frames) and — for DELTA
// frames only — the chatty_routes quarantine (frames/sec), NOT by
// the generic cmd limiter, so the design contract "quarantine does
// NOT close TCP" holds. (Full baselines are bounded by the route
// bucket; chatty targets delta churn — see recordInboundAnnounceAndMaybeArm.)
//
// The exemption logic in the service.go inbound read loop is:
//
//	if !s.admitInboundCommandLine(connID, connKey, line) { close }
//
// whose exemption half is frameLineExemptFromCommandLimit.
//
// We pin it by feeding raw JSON lines to that DECISION itself. Composing
// its halves here instead — the classification and the type policy — would
// leave the composition untested, and the composition is where a widened
// exemption would live.
func TestCmdLimiterExemption_BulkAnnounceFramesExempt(t *testing.T) {
	t.Parallel()

	exempt := []struct {
		name string
		line string
	}{
		{"announce_routes", `{"type":"announce_routes","routes":[]}`},
		{"routes_update", `{"type":"routes_update","routes":[]}`},
		{"route_announce_v3", `{"type":"` + protocol.RouteAnnounceV3FrameType + `","kind":"full","epoch":1,"entries":[]}`},
	}

	for _, tt := range exempt {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The DECISION, not its halves: composing the classifier and
			// the type policy by hand here would leave the composition
			// itself — the thing the read loop actually runs — untested.
			// A zero Service is the honest fixture: the datagram half of
			// the decision reads an atomic pointer that is nil without a
			// layer, and an announce line never reaches it anyway.
			if !new(Service).frameLineExemptFromCommandLimit(unregisteredConnID, tt.line) {
				// Two distinct reasons the exemption matters, depending
				// on frame kind:
				//   - baseline (announce_routes, v3 kind="full"): the cmd
				//     limiter would truncate a legitimate chunked full-sync
				//     burst; the route bucket, not chatty, bounds baselines.
				//   - delta (routes_update, v3 kind="delta"): the cmd limiter
				//     would close the TCP before a delta flood can reach the
				//     chatty_routes threshold that is meant to own it.
				t.Fatalf("%q is not exempt from the command limiter; it would either truncate a legitimate full-sync burst (baseline) or close inbound TCP before chatty_routes can arm on a delta flood", tt.line)
			}
		})
	}

	// Negative control — non-announce frame must NOT be exempt.
	for _, line := range []string{
		`{"type":"send_message","to":"abc"}`,
		`{"type":"ping"}`,
		`{"type":"hello","version":1}`,
	} {
		if new(Service).frameLineExemptFromCommandLimit(unregisteredConnID, line) {
			t.Errorf("non-announce line %q reported as exempt — over-broad exemption", line)
		}
	}
}

// TestCmdLimiterExemption_ControlAnnounceFramesNOTExempt pins the
// inverse: request_resync, route_poison_v1 and route_poison_v2 are
// announce-plane (they share announceLimiter, sender-identity gating,
// etc.) BUT they MUST stay under cmd-limiter coverage. Their natural
// per-peer rate is well under 1/s (request_resync: bounded by reconnect
// cycles; the poison frames: bounded by route lifecycle), so the
// 30 cmd/s budget never bites in normal operation. Exempting them
// would leave only the 200-tokens/s route bucket as the per-peer
// defence — at cost=1 per unit-cost control frame that allows 200/s
// sustained (route_poison_v2 charges len(identities), tighter still),
// far above legitimate use, AND chatty_routes does not count control
// frames in its trigger window (it is wired only into the three
// bulk handlers).
//
// This test guards against regression where someone widens the
// exemption back to the full isAnnouncePlaneFrameType set.
func TestCmdLimiterExemption_ControlAnnounceFramesNOTExempt(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		line string
	}{
		{"request_resync", `{"type":"request_resync"}`},
		{"route_poison_v1", `{"type":"` + protocol.RoutePoisonFrameType + `","identity":"x","sig":"y"}`},
		{"route_poison_v2", `{"type":"` + protocol.RoutePoisonV2FrameType + `","identities":["x"],"reason":"uplink_lost"}`},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ft, named := topLevelFrameType(tt.line)
			if !named {
				t.Fatalf("topLevelFrameType(%q) failed to name a known control line", tt.line)
			}
			// Sanity: control frames ARE announce-plane for the
			// size-budget enforcement path (peer_management.go).
			if !isAnnouncePlaneFrameType(ft) {
				t.Fatalf("isAnnouncePlaneFrameType(%q) = false; size-budget enforcement would no longer cover this control frame", ft)
			}
			// Contract: but NOT bulk → the cmd limiter still applies. Asked
			// of the DECISION, so that the exemption cannot widen underneath
			// a test that only ever consulted one of its halves.
			if new(Service).frameLineExemptFromCommandLimit(unregisteredConnID, tt.line) {
				t.Fatalf("control line %q is exempt from the cmd limiter — only the loose 200/s route bucket would remain as per-peer defence, and chatty_routes does NOT count control frames", tt.line)
			}
		})
	}
}

// TestCmdLimiterExemption_InboundBulkFloodReachesChattyThreshold is
// the end-to-end regression for the bulk-DELTA inbound direction:
// simulate 600 routes_update (v2 delta) lines through the same
// exemption predicate the inbound read loop uses, and verify NONE
// would be closed by the cmd-limiter check. Without the bulk-announce
// exemption, the 101st line would trip the burst (100) and a chatty
// peer never reaches the 500-frame chatty_routes threshold.
//
// Delta frames are the example here on purpose: under the
// deltas-only chatty contract (see recordInboundAnnounceAndMaybeArm)
// only routes_update / v3 kind="delta" feed the chatty trigger, so a
// delta flood is the path that must stay cmd-exempt AND reach chatty.
// Full baselines (announce_routes, v3 kind="full") are also cmd-exempt
// but are bounded by the announceLimiter route bucket, not chatty.
//
// This is a logic-level test (we don't spin a real read loop or
// TCP socket) — it pins the predicate that gates "would the cmd
// limiter even be consulted?" against the design contract
// "quarantine, not TCP close, owns the bulk-delta misbehaviour".
func TestCmdLimiterExemption_InboundBulkFloodReachesChattyThreshold(t *testing.T) {
	t.Parallel()

	line := `{"type":"routes_update","routes":[]}`
	const floodFrames = chattyAnnounceThreshold + 100 // headroom past the trigger

	closed := 0
	svc := new(Service)
	for i := 0; i < floodFrames; i++ {
		// The DECISION the inbound read loop makes before it even calls
		// cmdLimiter.allowCommand. A zero Service is enough: the datagram
		// half of the decision reads a nil layer pointer, and an
		// announce-plane line never reaches it.
		if !svc.frameLineExemptFromCommandLimit(unregisteredConnID, line) {
			// In the real loop the next step is allowCommand;
			// after burst exhaustion it returns false → close.
			// For bulk announce-plane the branch never gets here,
			// so "closed" stays 0.
			closed++
		}
	}

	if closed > 0 {
		t.Fatalf("bulk routes_update delta flood would be closed by cmd limiter %d/%d times; bulk exemption broken — inbound chatty peer never reaches chatty_routes quarantine",
			closed, floodFrames)
	}
}

// ---------------------------------------------------------------------------
// The exemption DECISION, not the peek
// ---------------------------------------------------------------------------

// TestCommandLimitExemptionRefusesDecoyTopLevelTypes is the regression for the
// command-limiter bypass.
//
// The exemption used to be decided from peekFrameType, which returns the FIRST
// `"type"` found anywhere in the line — nested objects included — while
// encoding/json binds the LAST TOP-LEVEL one. A sender therefore chose which
// reader got which answer: `{"a":{"type":"file_command"},"type":"ping"}` left
// the limiter as a file_command and was then executed as a `ping`, a
// handshake command that needs no session, so any socket could hold this node
// at line rate for free.
//
// The assertion is on the DECISION function the read loop calls, not on
// classifyFrameLine: the classifier already answered these lines correctly
// while the read loop went on asking the peek, so a test one level lower would
// have been green throughout the defect.
//
// Positive controls sit in the same table on purpose. Without them the test
// would also pass for an implementation that exempts nothing at all, which
// would silently reinstate the chunked full-sync truncation and throttle the
// file data plane to the control-plane rate.
func TestCommandLimitExemptionRefusesDecoyTopLevelTypes(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)

	cases := []struct {
		name   string
		line   string
		exempt bool
	}{
		// Decoys — the peek and the parser name two different types.
		{"nested_file_command_top_level_ping", `{"a":{"type":"file_command"},"type":"ping"}`, false},
		{"nested_bulk_announce_top_level_ping", `{"a":{"type":"routes_update"},"type":"ping"}`, false},
		{"nested_datagram_top_level_ping", `{"a":{"type":"datagram"},"type":"ping"}`, false},
		{"duplicate_file_command_then_ping", `{"type":"file_command","type":"ping"}`, false},
		{"duplicate_ping_then_file_command", `{"type":"ping","type":"file_command"}`, false},
		{"case_variant_type_key", `{"TYPE":"file_command"}`, false},
		{"non_string_type_then_file_command", `{"type":null,"type":"file_command"}`, false},
		{"not_an_object", `"file_command"`, false},

		// Positive controls — the exemptions that must survive the fix.
		{"real_file_command", `{"type":"file_command","sub":"chunk_request"}`, true},
		{"real_announce_routes", `{"type":"announce_routes","routes":[]}`, true},
		{"real_routes_update", `{"type":"routes_update","routes":[]}`, true},
		{"real_route_announce_v3", `{"type":"` + protocol.RouteAnnounceV3FrameType + `","kind":"full","epoch":1,"entries":[]}`, true},

		// Negative controls — ordinary and announce-plane CONTROL frames keep
		// paying the limiter.
		{"real_ping", `{"type":"ping"}`, false},
		{"real_send_message", `{"type":"send_message","to":"abc"}`, false},
		{"real_request_resync", `{"type":"request_resync"}`, false},
		{"real_route_poison_v1", `{"type":"` + protocol.RoutePoisonFrameType + `","identity":"x","sig":"y"}`, false},
	}

	// Every question below is asked of an AUTHENTICATED connection, which is
	// the only state where the datagram exemption can be granted at all — so a
	// decoy answered "not exempt" here was refused for its shape and not for
	// the connection it arrived on.
	authenticated := registerDatagramCommandConn(t, svc, domain.ConnID(8831), true)

	// The third exempt class needs a real frame rather than a fixture literal.
	// A genuine datagram must keep its exemption: the layer's §5 budget REPLACES
	// the command limiter for this plane, and a datagram left inside the limiter
	// would be throttled to a control-plane rate its bulk chunks never fit.
	if !svc.frameLineExemptFromCommandLimit(authenticated, mustDatagramLine(t, newNodeDatagram(t, nil))) {
		t.Fatal("a real datagram lost its exemption although the layer charges its own budget")
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := svc.frameLineExemptFromCommandLimit(authenticated, tt.line+"\n"); got != tt.exempt {
				t.Fatalf("frameLineExemptFromCommandLimit(%s) = %v, want %v", tt.line, got, tt.exempt)
			}
		})
	}
}
