package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestSendDirectMessageRejectsControlCommands is the regression test for
// the guard added in DMCrypto.SendDirectMessage that refuses any
// DMCommand.IsControl() command. Without this guard, a caller that
// accidentally fed a control DM through DMRouter.SendMessage (the
// "regular" send path) would persist it in chatlog and surface it as a
// visible "[delete]" row in the sender's chat — which is the exact
// failure mode docs/dm-commands.md is designed to avoid.
//
// The guard runs *before* any call into rpc / chatlog / identity, so a
// zero-value DMCrypto suffices to drive the test without test doubles
// or live infrastructure.
func TestSendDirectMessageRejectsControlCommands(t *testing.T) {
	t.Parallel()

	d := &DMCrypto{}
	controlCommands := []domain.DMCommand{
		domain.DMCommandMessageDelete,
		domain.DMCommandMessageDeleteAck,
	}

	for _, cmd := range controlCommands {
		cmd := cmd
		t.Run(string(cmd), func(t *testing.T) {
			t.Parallel()

			// Body is non-empty so the older "recipient and message are
			// required" check would NOT fire — we want to prove the new
			// control-DM guard intercepts first, regardless of whether
			// the rest of the message looks well-formed.
			_, err := d.SendDirectMessage(
				context.Background(),
				domain.PeerIdentity("any-peer"),
				domain.OutgoingDM{
					Body:    "looks like a normal message",
					Command: cmd,
				},
			)
			if err == nil {
				t.Fatalf("SendDirectMessage(%s) returned nil error; control DM must be rejected", cmd)
			}
			if !strings.Contains(err.Error(), "control DM") {
				t.Fatalf("SendDirectMessage(%s) error = %q; want substring %q", cmd, err.Error(), "control DM")
			}
			if !strings.Contains(err.Error(), "SendControlMessage") {
				t.Fatalf("SendDirectMessage(%s) error = %q; want a hint to use SendControlMessage", cmd, err.Error())
			}
		})
	}
}

// TestSendControlMessageRejectsNonControlCommands is the symmetric
// regression to the guard added in SendControlMessage. Submitting a
// data DM (or an empty command) through the control wire path must be
// refused before any encryption / I/O happens — control DMs are the
// only valid traffic on TopicControlDM, and a data command leaking
// into that path would corrupt the no-storage invariant on the wire.
//
// The check fires before any rpc/chatlog/identity touch, so a
// zero-value DMCrypto suffices.
func TestSendControlMessageRejectsNonControlCommands(t *testing.T) {
	t.Parallel()

	d := &DMCrypto{}
	cases := []domain.DMCommand{
		"",
		domain.DMCommandFileAnnounce,
		domain.DMCommand("unknown"),
	}

	for _, cmd := range cases {
		cmd := cmd
		t.Run(string(cmd), func(t *testing.T) {
			t.Parallel()

			_, err := d.SendControlMessage(
				context.Background(),
				domain.PeerIdentity("any-peer"),
				cmd,
				`{}`,
			)
			if err == nil {
				t.Fatalf("SendControlMessage(%q): err = nil; want a control-command guard error", cmd)
			}
			if !strings.Contains(err.Error(), "control command") {
				t.Fatalf("SendControlMessage(%q): err = %q; want substring %q", cmd, err.Error(), "control command")
			}
		})
	}
}

// TestSendControlMessageRequiresRecipient proves an empty recipient is
// caught by SendControlMessage *before* the control-command guard or
// any I/O. We feed a control command (so the next guard could fire if
// recipient validation didn't), and assert the error is the recipient
// one.
func TestSendControlMessageRequiresRecipient(t *testing.T) {
	t.Parallel()

	d := &DMCrypto{}
	_, err := d.SendControlMessage(
		context.Background(),
		domain.PeerIdentity("   "), // whitespace-only — trims to empty
		domain.DMCommandMessageDelete,
		`{}`,
	)
	if err == nil {
		t.Fatal("SendControlMessage with empty recipient: err = nil; want recipient validation error")
	}
	if !strings.Contains(err.Error(), "recipient is required") {
		t.Fatalf("SendControlMessage with empty recipient: err = %q; want substring %q", err.Error(), "recipient is required")
	}
}

// TestBuildControlMessageFrame pins the exact set of Frame fields the
// node requires to accept a send_control_message: ID, Topic, Address,
// Recipient, Flag (must satisfy Flag.Valid), CreatedAt (RFC3339), and
// Body. If any field becomes empty or invalid, storeMessageFrame would
// reject the frame with ErrCodeInvalidSendMessage before
// storeIncomingMessage ever runs the topic-aware control branch — this
// test catches that regression at the unit level without spinning up
// node/identity/rpc.
func TestBuildControlMessageFrame(t *testing.T) {
	t.Parallel()

	const (
		sender    = "sender-address"
		recipient = "recipient-address"
		messageID = "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"
		body      = "ciphertext-blob"
	)
	createdAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	frame := buildControlMessageFrame(sender, recipient, messageID, body, createdAt)

	if frame.Type != "send_control_message" {
		t.Errorf("frame.Type = %q, want send_control_message", frame.Type)
	}
	if frame.Topic != protocol.TopicControlDM {
		t.Errorf("frame.Topic = %q, want %q", frame.Topic, protocol.TopicControlDM)
	}
	if frame.ID != messageID {
		t.Errorf("frame.ID = %q, want %q", frame.ID, messageID)
	}
	if frame.Address != sender {
		t.Errorf("frame.Address = %q, want %q", frame.Address, sender)
	}
	if frame.Recipient != recipient {
		t.Errorf("frame.Recipient = %q, want %q", frame.Recipient, recipient)
	}
	if frame.Body != body {
		t.Errorf("frame.Body = %q, want %q", frame.Body, body)
	}

	// CreatedAt must be a non-empty RFC3339 string — node parses it
	// with time.Parse(time.RFC3339, ...), and an empty value would fail
	// at the very first line of incomingMessageFromFrame.
	if frame.CreatedAt == "" {
		t.Fatal("frame.CreatedAt is empty; node will fail to parse the timestamp")
	}
	if _, err := time.Parse(time.RFC3339, frame.CreatedAt); err != nil {
		t.Errorf("frame.CreatedAt %q does not parse as RFC3339: %v", frame.CreatedAt, err)
	}

	// Flag must be one of the four MessageFlag values that
	// MessageFlag.Valid accepts. SendDirectMessage stamps
	// MessageFlagSenderDelete; control DMs follow the same default.
	if !protocol.MessageFlag(frame.Flag).Valid() {
		t.Errorf("frame.Flag = %q is not a valid MessageFlag — node would reject the frame", frame.Flag)
	}

	// TTLSeconds must be non-negative; AutoDeleteTTL would additionally
	// require a positive value, but the default flag is sender-delete.
	if frame.TTLSeconds < 0 {
		t.Errorf("frame.TTLSeconds = %d, want >= 0", frame.TTLSeconds)
	}
}

// TestSendDirectMessageGuardIgnoresDataCommands proves the guard does
// not over-reach: DMCommandFileAnnounce and the empty (plain text)
// command must NOT be rejected by the control-DM check.
//
// To stay independent of rpc / chatlog / identity test doubles, we
// intentionally feed an empty Body. The order of guards inside
// SendDirectMessage is fixed:
//
//  1. control-DM guard ("control DM" / "SendControlMessage")
//  2. body+recipient guard ("recipient and message are required")
//  3. reply_to validation
//  4. ... rpc/chatlog/identity work begins here
//
// For a data-command DM with empty Body, guard #1 must let it through
// and guard #2 must fire — yielding the "recipient and message are
// required" error without ever reaching the I/O layer. If the
// control-DM guard ever started over-reaching to data commands, we
// would see the "control DM" error here instead, and the test fails.
func TestSendDirectMessageGuardIgnoresDataCommands(t *testing.T) {
	t.Parallel()

	d := &DMCrypto{}
	dataCommands := []domain.DMCommand{
		"", // plain text DM
		domain.DMCommandFileAnnounce,
	}

	const expectErrSubstr = "recipient and message are required"

	for _, cmd := range dataCommands {
		cmd := cmd
		t.Run(string(cmd), func(t *testing.T) {
			t.Parallel()

			_, err := d.SendDirectMessage(
				context.Background(),
				domain.PeerIdentity("peer"),
				domain.OutgoingDM{
					Body:    "", // forces guard #2 to fire before any I/O
					Command: cmd,
				},
			)
			if err == nil {
				t.Fatalf("SendDirectMessage(%q): err = nil; want a synchronous error from the body+recipient guard", cmd)
			}
			if strings.Contains(err.Error(), "control DM") {
				t.Fatalf("SendDirectMessage(%q): rejected by the control-DM guard (err=%q); the guard over-reached to a data command", cmd, err.Error())
			}
			if !strings.Contains(err.Error(), expectErrSubstr) {
				t.Fatalf("SendDirectMessage(%q): err = %q; want substring %q (the body+recipient guard)", cmd, err.Error(), expectErrSubstr)
			}
		})
	}
}
