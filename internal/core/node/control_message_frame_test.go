package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestIncomingMessageFromControlFrame is the contract pin for the
// service-layer control-DM send path: a Frame shaped exactly like the
// one DMCrypto.SendControlMessage submits (via buildControlMessageFrame
// in service/dm_crypto_control.go) must pass incomingMessageFromFrame
// without error.
//
// Without this guarantee, storeMessageFrame would short-circuit with
// ErrCodeInvalidSendMessage before storeIncomingMessage's topic-aware
// control branch ever runs — exactly the failure mode the P2 review on
// the control-DM send path flagged.
//
// We rebuild the frame inline here (instead of importing the service
// package, which would create a cycle) and assert that every required
// field passes the parser and that the parsed message preserves the
// control-DM contract (TopicControlDM, valid Flag, RFC3339 timestamp).
func TestIncomingMessageFromControlFrame(t *testing.T) {
	t.Parallel()

	const (
		sender    = "11ed04572e7d37cbfb36f297e3027c72ae14b385"
		recipient = "8c2c5b6f1c0e1234567890abcdefabcdef123456"
		messageID = "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"
		body      = "ciphertext-blob-stand-in"
	)
	createdAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	// Mirrors service/dm_crypto_control.go buildControlMessageFrame.
	// If that helper changes shape, this fixture must change with it —
	// the cross-package coupling is intentional: a wire-format drift on
	// the send side must light up here before it reaches a node in
	// production.
	frame := protocol.Frame{
		Type:       "send_control_message",
		Topic:      protocol.TopicControlDM,
		ID:         messageID,
		Address:    sender,
		Recipient:  recipient,
		Flag:       string(protocol.MessageFlagSenderDelete),
		CreatedAt:  createdAt.Format(time.RFC3339),
		TTLSeconds: 0,
		Body:       body,
	}

	msg, err := incomingMessageFromFrame(frame)
	if err != nil {
		t.Fatalf("incomingMessageFromFrame: %v — control-DM send path would die at parser before storeIncomingMessage runs", err)
	}

	if msg.Topic != protocol.TopicControlDM {
		t.Errorf("msg.Topic = %q, want %q", msg.Topic, protocol.TopicControlDM)
	}
	if string(msg.ID) != messageID {
		t.Errorf("msg.ID = %q, want %q", msg.ID, messageID)
	}
	if msg.Sender != sender {
		t.Errorf("msg.Sender = %q, want %q", msg.Sender, sender)
	}
	if msg.Recipient != recipient {
		t.Errorf("msg.Recipient = %q, want %q", msg.Recipient, recipient)
	}
	if msg.Body != body {
		t.Errorf("msg.Body = %q, want %q", msg.Body, body)
	}
	if !msg.Flag.Valid() {
		t.Errorf("msg.Flag = %q is not a valid MessageFlag — node parser accepted but control path would later fail", msg.Flag)
	}
	if msg.CreatedAt.IsZero() {
		t.Error("msg.CreatedAt is zero — RFC3339 timestamp did not propagate through the parser")
	}
}
