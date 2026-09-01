package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestUndeliveredReseedHasNoHorizon pins the decision that a restart is
// not a reason to stop delivering. The adapter must ask the chatlog for
// EVERY row still in "sent", not for a recent window — a message ends
// when the recipient confirms it, when its author withdraws it, or when
// its own TTL expires, and none of those is "the app was reopened".
func TestUndeliveredReseedHasNoHorizon(t *testing.T) {
	t.Parallel()
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	store := newTestChatlogStore(t, self)

	ancient := time.Now().UTC().Add(-400 * 24 * time.Hour)
	if _, err := store.AppendReportNew(context.Background(), "dm", self, chatlog.Entry{
		ID: "ancient-undelivered", Sender: self.String(), Recipient: peer.String(),
		Body: "still waiting", CreatedAt: ancient.Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSent,
	}); err != nil {
		t.Fatalf("AppendReportNew: %v", err)
	}

	adapter := &MessageStoreAdapter{chatlog: NewChatlogGateway(store, self)}
	rows, err := adapter.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	for _, row := range rows {
		if row.Envelope.ID == "ancient-undelivered" {
			return
		}
	}
	t.Fatalf("a message over a year old was dropped from the reseed; got %d rows", len(rows))
}

// TestLegacyRetryCapDoesNotBlockAnOrdinaryMessage is the upgrade path. The
// engine used to give up on ordinary messages after twenty attempts and
// record the id in delivery_failed; abandonment now has exactly one cause,
// a message outliving its own TTL. A row with no TTL therefore cannot
// legitimately be in that table, and honouring the ones already there
// would leave every message the old cap abandoned permanently
// undeliverable — including to a recipient who has since come back.
func TestLegacyRetryCapDoesNotBlockAnOrdinaryMessage(t *testing.T) {
	t.Parallel()
	self := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	peer := domain.PeerIdentityFromWire(strings.Repeat("d", 40))
	store := newTestChatlogStore(t, self)
	ctx := context.Background()

	rows := []struct {
		id  string
		ttl int
	}{
		{"legacy-capped", 0},              // no TTL — the old cap wrote this one
		{"legacy-capped-with-ttl", 86400}, // a day to live, abandoned an hour in
		{"genuinely-expired", 60},         // this one really did outlive its TTL
	}
	for _, row := range rows {
		if _, err := store.AppendReportNew(ctx, "dm", self, chatlog.Entry{
			ID: row.id, Sender: self.String(), Recipient: peer.String(),
			Body: "x", CreatedAt: time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
			DeliveryStatus: chatlog.StatusSent, TTLSeconds: row.ttl,
		}); err != nil {
			t.Fatalf("AppendReportNew %s: %v", row.id, err)
		}
		if err := store.MarkDeliveryFailed(ctx, row.id); err != nil {
			t.Fatalf("MarkDeliveryFailed %s: %v", row.id, err)
		}
	}

	entries, err := store.UndeliveredOutgoing(ctx, self, time.Time{}, time.Now().UTC())
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	got := make(map[string]bool, len(entries))
	for _, entry := range entries {
		got[entry.ID] = true
	}
	if !got["legacy-capped"] {
		t.Error("a message with no TTL is still blocked by the retry cap that no longer exists")
	}
	if !got["legacy-capped-with-ttl"] {
		t.Error("a day-long message abandoned an hour in is still blocked: the test has to be created_at + ttl, not merely 'has a TTL'")
	}
	if got["genuinely-expired"] {
		t.Error("a message that outlived its own TTL was resurrected")
	}
}

// TestOutgoingRowIsBornNeverEmitted pins the crash-consistency of the
// queued badge. The claim has to be true at EVERY instant, not only after
// a follow-up write: a crash between storing the row and a sink confirming
// it used to leave a message that never left the machine reading as sent
// for good.
func TestOutgoingRowIsBornNeverEmitted(t *testing.T) {
	t.Parallel()
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	selfID := domain.PeerIdentityFromWire(self.Address)
	store := newTestChatlogStore(t, selfID)
	adapter := NewMessageStoreAdapter(NewChatlogGateway(store, selfID), self, nil, nil)

	now := time.Now().UTC()
	outgoing := protocol.Envelope{
		ID: "born-queued", Topic: "dm",
		Sender: self.Address, Recipient: strings.Repeat("e", 40),
		Payload: []byte("sealed"), CreatedAt: now,
	}
	if got := adapter.StoreMessage(outgoing, true); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want StoreInserted", got)
	}

	rows, err := adapter.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	for _, row := range rows {
		if row.Envelope.ID != "born-queued" {
			continue
		}
		if row.Emitted {
			t.Fatal("a freshly stored outgoing row reads as emitted, so a crash before the first send shows it as sent for good")
		}
		return
	}
	t.Fatal("the stored row was not reported as undelivered")
}

// TestStoredMessageStatusReadsTheNodesAnswer covers the echo the sender
// sees the instant they press send.
func TestStoredMessageStatusReadsTheNodesAnswer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		reply protocol.Frame
		want  string
	}{
		{
			name:  "held for an unreachable recipient",
			reply: protocol.Frame{Type: "message_stored", Status: protocol.MessageStatusQueued},
			want:  MessageStatusQueued,
		},
		{
			name:  "went out",
			reply: protocol.Frame{Type: "message_stored"},
			want:  MessageStatusSent,
		},
		{
			// A node that predates the field says nothing, and silence has
			// to keep meaning what it always meant.
			name:  "older node says nothing",
			reply: protocol.Frame{Type: "message_stored", Status: ""},
			want:  MessageStatusSent,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := storedMessageStatus(tc.reply); got != tc.want {
				t.Fatalf("storedMessageStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestUnconfirmedRowReadsAsQueued covers the durable half: after a restart
// the echo is gone and the chatlog is the only witness. A row with no
// on-wire stamp must read as queued rather than sent — and a receipt,
// which proves the message plainly did go out, must outrank the absence.
//
// The badge reads THIS bit, not the never-emitted claim. That claim is
// withdrawn before the first frame is handed to a writer, so it turns
// "sent" the moment an attempt BEGINS: a message whose every writer then
// refused reopened as sent, which is the reported bug in its original
// shape. The two bits answer different questions for different readers —
// see chatlog/emission.go.
func TestUnconfirmedRowReadsAsQueued(t *testing.T) {
	t.Parallel()
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient: %v", err)
	}
	ciphertext, err := directmsg.EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "held for an absent recipient"},
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	contacts := map[string]Contact{
		sender.Address: {
			BoxKey: identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(sender.PublicKey),
		},
	}
	msgTime := time.Date(2026, 8, 30, 3, 0, 0, 0, time.UTC)

	cases := []struct {
		name            string
		awaitingWire    bool
		persistedStatus string
		want            string
	}{
		{"held row", true, MessageStatusSent, MessageStatusQueued},
		// The attempt that was made and refused: its never-emitted claim
		// is gone (the gate withdrew it), and only the missing on-wire
		// stamp keeps the badge honest.
		{"attempted but refused", true, MessageStatusSent, MessageStatusQueued},
		{"confirmed row", false, MessageStatusSent, MessageStatusSent},
		// A row written before the bit existed says nothing about the
		// wire, so its persisted status stands — an upgrade must not
		// re-badge a user's whole history.
		{"legacy row", false, MessageStatusSent, MessageStatusSent},
		{"unstamped row the peer confirmed anyway", true, MessageStatusDelivered, MessageStatusDelivered},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			records := []MessageRecord{{
				ID:              "queued-" + tc.name,
				Sender:          sender.Address,
				Recipient:       recipient.Address,
				Body:            ciphertext,
				Timestamp:       msgTime,
				PersistedStatus: tc.persistedStatus,
				AwaitingWire:    tc.awaitingWire,
			}}
			got := decryptDirectMessages(sender, contacts, records, nil, nil)
			if len(got) != 1 {
				t.Fatalf("expected 1 message, got %d", len(got))
			}
			if got[0].ReceiptStatus != tc.want {
				t.Fatalf("ReceiptStatus = %q, want %q", got[0].ReceiptStatus, tc.want)
			}
		})
	}
}
