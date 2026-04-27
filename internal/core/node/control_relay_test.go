package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// sealControlDMBody mirrors sealDMBody for the control-DM wire path:
// it produces an encrypted envelope whose plaintext carries
// DMCommandMessageDelete with an empty body. Used by the relay-side
// tests to drive handleRelayMessage with a TopicControlDM frame that
// passes signature verification.
func sealControlDMBody(t *testing.T, sender *identity.Identity, recipientAddress, recipientBoxKeyBase64 string) string {
	t.Helper()
	sealed, err := directmsg.EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipientAddress),
			BoxKeyBase64: recipientBoxKeyBase64,
		},
		domain.OutgoingDM{
			Command:     domain.DMCommandMessageDelete,
			CommandData: `{"target_id":"a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"}`,
		},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants (control DM): %v", err)
	}
	return sealed
}

// TestStoreMessageFrameTypeTopicMismatch pins the Type ↔ Topic
// invariant in storeMessageFrame. Without it, a low-level caller could
// pick the wrong storage behaviour by mismatching Frame.Type against
// Frame.Topic:
//
//   - send_message + TopicControlDM would let a regular DM caller
//     bypass chatlog and skip the LocalChangeNewMessage event that
//     drives the chat thread, hiding the message from the sender's UI.
//   - send_control_message + topic "dm" would force a control DM
//     through the data-DM path, surfacing a "[delete]"-shaped row in
//     the sender's chat thread — the exact failure mode
//     docs/dm-commands.md is designed to avoid.
//
// Both must return ErrCodeInvalidSendMessage synchronously, before
// any chatlog write or event publication happens.
func TestStoreMessageFrameTypeTopicMismatch(t *testing.T) {
	t.Parallel()

	mismatched := []struct {
		name      string
		frameType string
		topic     string
	}{
		{"send_message_with_control_topic", "send_message", protocol.TopicControlDM},
		{"send_control_message_with_dm_topic", "send_control_message", "dm"},
		{"send_control_message_with_global_topic", "send_control_message", "global"},
	}

	for _, tc := range mismatched {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			svc := newTestService(t, config.NodeTypeFull)

			// Other fields are filled to plausible values so the
			// Type↔Topic guard is the *only* thing that can reject
			// the frame. If the guard were absent, the request would
			// hit incomingMessageFromFrame and either succeed (wrong
			// behaviour) or fail later with a different error code.
			frame := protocol.Frame{
				Type:      tc.frameType,
				Topic:     tc.topic,
				ID:        "type-topic-mismatch-1",
				Address:   svc.Address(),
				Recipient: svc.Address(),
				Body:      "ciphertext-stand-in",
				Flag:      string(protocol.MessageFlagSenderDelete),
				CreatedAt: time.Now().UTC().Format(time.RFC3339),
			}

			reply := svc.storeMessageFrame(frame)

			if reply.Type != "error" {
				t.Fatalf("(%s, %s): reply.Type = %q, want \"error\"", tc.frameType, tc.topic, reply.Type)
			}
			if reply.Code != protocol.ErrCodeInvalidSendMessage {
				t.Fatalf("(%s, %s): reply.Code = %q, want %q", tc.frameType, tc.topic, reply.Code, protocol.ErrCodeInvalidSendMessage)
			}
		})
	}
}

// TestHandleRelayMessageControlDMNoNextHopReturnsEmpty pins the
// control-DM divergence in handleRelayMessage's no-next-hop fallback.
//
// For data DMs the fallback calls deliverRelayedMessage so the envelope
// lands in s.topics["dm"] and gossip can carry it; the relay returns
// "stored" upstream to signal a successful local store. Control DMs
// (TopicControlDM) intentionally bypass chatlog AND s.topics AND
// relayRetry — see docs/dm-commands.md "Storage rules for control DMs".
// The same fallback would call storeIncomingMessage, see no recoverable
// envelope land anywhere, yet still pass the wire-dedup check and
// return stored=true; the relay would then ack "stored" upstream
// while the control envelope is in fact lost. The application-level
// retry on the sender (DMRouter.pendingDelete in slice B) is the
// canonical recovery, but only fires if upstream learns the attempt
// failed.
//
// Therefore: a transit-only control DM with no next-hop must yield the
// empty status ("") so the previous hop does NOT consider this a
// successful relay.
func TestHandleRelayMessageControlDMNoNextHopReturnsEmpty(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)

	// Recipient is a third party that this node has no direct peer,
	// no table route, and no gossip-capable target for. With those
	// three lookups exhausted, handleRelayMessage hits the no-next-hop
	// fallback — this is the branch under test.
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	body := sealControlDMBody(t, sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))

	frame := protocol.Frame{
		ID:          "control-no-next-hop-1",
		Address:     sender.Address,
		Recipient:   recipient.Address,
		Topic:       protocol.TopicControlDM,
		Body:        body,
		Flag:        string(protocol.MessageFlagSenderDelete),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    1,
		MaxHops:     10,
		PreviousHop: "10.0.0.1:64646",
	}

	status := svc.handleRelayMessage(domain.PeerAddress("10.0.0.1:64646"), nil, frame)

	// The contract this test pins:
	//   - status MUST NOT be "stored" — that would falsely tell the
	//     previous hop the relay succeeded.
	//   - status SHOULD be "" — the no-ack signal that lets the
	//     sender's pendingDelete retry treat this attempt as a miss.
	if status == "stored" {
		t.Fatalf("control DM with no next-hop returned %q — upstream would believe relay succeeded while envelope is lost", status)
	}
	if status != "" {
		t.Fatalf("control DM with no next-hop: expected empty status, got %q", status)
	}

	// Side-effect invariants: the control envelope must NOT have
	// landed in s.topics["dm-control"] or in relayRetry. Both paths
	// would create dead state that no consumer reads (see
	// docs/dm-commands.md §"Storage rules for control DMs" §5–6).
	svc.gossipMu.RLock()
	topicCount := len(svc.topics[protocol.TopicControlDM])
	svc.gossipMu.RUnlock()
	if topicCount != 0 {
		t.Errorf("s.topics[%q] = %d entries, want 0 — control DMs must never enter topics", protocol.TopicControlDM, topicCount)
	}

	svc.deliveryMu.RLock()
	relayRetryCount := len(svc.relayRetry)
	svc.deliveryMu.RUnlock()
	if relayRetryCount != 0 {
		t.Errorf("len(s.relayRetry) = %d, want 0 — control DMs must never enter relayRetry", relayRetryCount)
	}
}
