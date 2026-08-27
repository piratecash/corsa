package service

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestOutboxCarriesTheEmissionProofAcrossAReopen walks the whole durable
// path the node depends on: the adapter writes the claim, a fresh
// repository on the same file reads it back, and the reseed row says the
// peer cannot have the message.
func TestOutboxCarriesTheEmissionProofAcrossAReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	owner := domain.PeerIdentityFromWire(self.Address)
	database := newTestStateDB(t, owner)
	store := testChatlogStore(database.Executor(), owner)
	adapter := NewMessageStoreAdapter(NewChatlogGateway(store, owner), self, nil, nil)

	const (
		withheld = protocol.MessageID("11111111-1111-4111-8111-111111111111")
		ordinary = protocol.MessageID("22222222-2222-4222-8222-222222222222")
	)
	for _, id := range []protocol.MessageID{withheld, ordinary} {
		if err := store.Append(ctx, "dm", owner, chatlog.Entry{
			ID:        string(id),
			Sender:    self.Address,
			Recipient: domain.PeerIdentityFromWire("3333333333333333333333333333333333333333").String(),
			Body:      "sealed",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	if err := adapter.MarkNeverEmitted([]protocol.MessageID{withheld}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	// A second repository over the same database is what the node meets
	// after a restart.
	reopened := NewMessageStoreAdapter(
		NewChatlogGateway(testChatlogStore(database.Executor(), owner), owner), self, nil, nil)
	rows, err := reopened.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	emitted := map[protocol.MessageID]bool{}
	for _, row := range rows {
		emitted[row.Envelope.ID] = row.Emitted
	}
	if len(emitted) != 2 {
		t.Fatalf("reseed returned %d rows, want 2: %v", len(emitted), emitted)
	}
	if emitted[withheld] {
		t.Error("the withheld message reads as emitted after a reopen")
	}
	if !emitted[ordinary] {
		t.Error("a message with no mark must read as emitted")
	}

	// And the claim is withdrawn once it goes out.
	if err := adapter.ClearNeverEmitted([]protocol.MessageID{withheld}); err != nil {
		t.Fatalf("ClearNeverEmitted: %v", err)
	}
	rows, err = reopened.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing after clear: %v", err)
	}
	for _, row := range rows {
		if !row.Emitted {
			t.Errorf("%s still claims it never went out", row.Envelope.ID)
		}
	}
}

// TestAdapterSatisfiesTheEmissionJournal: the node picks the journal up by
// type assertion on the outbox, so a signature drift would silently turn
// the durable proof back off instead of failing to compile.
func TestAdapterSatisfiesTheEmissionJournal(t *testing.T) {
	t.Parallel()

	var outbox node.DeliveryOutbox = (*MessageStoreAdapter)(nil)
	if _, ok := outbox.(node.DeliveryEmissionJournal); !ok {
		t.Fatal("MessageStoreAdapter no longer implements node.DeliveryEmissionJournal")
	}
}

// TestIncomingMessagesAreStoredUnderTheSharedDeletePolicy: the flag decides who
// may have a message removed from this side, and the product's answer is
// "either participant". It has no interface, so `sender-delete` on the wire is
// not a choice anybody made — it is what an un-updated build stamps.
//
// Migration 0007 rewrote the history carrying it. Without the same rule at the
// door, the next message from that build brings it back, and with it the
// refusal the user reads as "the peer refused to delete the message".
func TestIncomingMessagesAreStoredUnderTheSharedDeletePolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	owner := domain.PeerIdentityFromWire(self.Address)
	store := testChatlogStore(newTestStateDB(t, owner).Executor(), owner)
	adapter := NewMessageStoreAdapter(NewChatlogGateway(store, owner), self, nil, nil)

	peer := domain.PeerIdentityFromWire("3333333333333333333333333333333333333333")
	cases := []struct {
		name  string
		id    protocol.MessageID
		topic string
		flag  protocol.MessageFlag
		want  protocol.MessageFlag
	}{
		{"an older build's author-only default", "44444444-4444-4444-8444-444444444444", "dm", protocol.MessageFlagSenderDelete, protocol.MessageFlagAnyDelete},
		{"the value that predates the column having one", "55555555-5555-4555-8555-555555555555", "dm", "", protocol.MessageFlagAnyDelete},
		{"a refusal somebody meant", "66666666-6666-4666-8666-666666666666", "dm", protocol.MessageFlagImmutable, protocol.MessageFlagImmutable},
		{"an expiry contract, not a policy", "77777777-7777-4777-8777-777777777777", "dm", protocol.MessageFlagAutoDeleteTTL, protocol.MessageFlagAutoDeleteTTL},
		{"outside a conversation there is no second participant", "88888888-8888-4888-8888-888888888888", "global", protocol.MessageFlagSenderDelete, protocol.MessageFlagSenderDelete},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recipient := owner.String()
			if tc.topic != "dm" {
				recipient = "*"
			}
			if got := adapter.StoreMessage(protocol.Envelope{
				ID:        tc.id,
				Topic:     tc.topic,
				Sender:    peer.String(),
				Recipient: recipient,
				Flag:      tc.flag,
				CreatedAt: time.Now().UTC(),
				Payload:   []byte("sealed"),
			}, false); got != node.StoreInserted {
				t.Fatalf("StoreMessage = %v, want inserted", got)
			}
			entry, found, err := store.EntryByID(ctx, domain.MessageID(tc.id))
			if err != nil || !found {
				t.Fatalf("EntryByID: found=%v err=%v", found, err)
			}
			if protocol.MessageFlag(entry.Flag) != tc.want {
				t.Errorf("stored flag = %q, want %q", entry.Flag, tc.want)
			}
		})
	}
}
