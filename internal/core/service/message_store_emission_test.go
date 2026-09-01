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
	"github.com/piratecash/corsa/internal/core/storage"
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
	// In production the INSERT carries the claim; here the rows were
	// appended plain, so the store's own setter stands in for it. The
	// adapter deliberately has no such method: the delivery path must not
	// be able to re-set this bit — see chatlog/emission.go.
	if err := store.MarkNeverEmitted(ctx, []domain.MessageID{domain.MessageID(withheld)}); err != nil {
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

// TestRefusedAttemptStillReadsAsQueuedFromTheRow is the reported bug, taken
// end to end through the DURABLE state rather than through the node's
// memory.
//
// The gate withdraws the never-emitted claim before the first frame is
// handed to a writer, so after a refused attempt the row no longer carries
// it. If the badge were read off that claim, reopening the conversation
// would show "sent" for a message nothing ever carried — which is exactly
// the symptom this whole change started from, for a recipient who is
// online but unreachable.
//
// What keeps it honest is the OTHER bit: no sink confirmed, so no on-wire
// stamp, so the row reads as queued.
func TestRefusedAttemptStillReadsAsQueuedFromTheRow(t *testing.T) {
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

	const refused = protocol.MessageID("44444444-4444-4444-8444-444444444444")
	if err := store.Append(ctx, "dm", owner, chatlog.Entry{
		ID:        string(refused),
		Sender:    self.Address,
		Recipient: domain.PeerIdentityFromWire("5555555555555555555555555555555555555555").String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Metadata:  chatlog.NeverEmittedMetadata,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// The gate runs, the writer then refuses, and nothing else is written.
	if err := adapter.ClearNeverEmitted([]protocol.MessageID{refused}); err != nil {
		t.Fatalf("ClearNeverEmitted: %v", err)
	}

	rows, err := adapter.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	var found bool
	for _, row := range rows {
		if row.Envelope.ID != refused {
			continue
		}
		found = true
		if !row.Emitted {
			t.Error("the deletion would skip a peer that may have been handed the frame")
		}
		if row.OnWire {
			t.Error("a refused attempt reads as on the wire; the sender is shown a message nothing carried")
		}
	}
	if !found {
		t.Fatal("the refused message was dropped from the reseed")
	}

	// And once a sink does confirm it, the same row reads as sent.
	if err := adapter.MarkOnWire([]protocol.MessageID{refused}); err != nil {
		t.Fatalf("MarkOnWire: %v", err)
	}
	rows, err = adapter.UndeliveredOutgoing()
	if err != nil {
		t.Fatalf("UndeliveredOutgoing: %v", err)
	}
	for _, row := range rows {
		if row.Envelope.ID == refused && !row.OnWire {
			t.Fatal("the confirmation did not reach the row")
		}
	}
}

// TestUpgradeKeepsHeldLegacyMessagesQueued is the migration's own version
// of the reported bug.
//
// The previous release had ONE flag and set it for exactly the messages
// this node was holding for an unreachable recipient. Those rows carry
// never_emitted and no on_wire, and reading the missing stamp as "this row
// predates all of it, leave the status alone" would show them as sent —
// a message nothing ever carried, which is where this whole change began.
//
// The three shapes have to be told apart, and a row from BEFORE the flag
// existed must keep its persisted status: reading its silence as "not
// sent" would re-badge a user's whole unreceipted history as queued.
func TestUpgradeKeepsHeldLegacyMessagesQueued(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		metadata string
		want     bool
	}{
		{"held by the previous release", `{"never_emitted":true}`, true},
		{"born under the two-bit model, unconfirmed", chatlog.NeverEmittedMetadata, true},
		{"confirmed", `{"never_emitted":false,"on_wire":true}`, false},
		{"predates every flag", "", false},
		{"predates every flag, other metadata", `{"decrypt_failed":true}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := awaitingWire(tc.metadata); got != tc.want {
				t.Errorf("awaitingWire(%q) = %v, want %v", tc.metadata, got, tc.want)
			}
		})
	}
}

// TestMarkOnWireSkipsRowsThatAlreadyCarryIt: the predicate has to actually
// exclude them. json_extract returns SQLite's INTEGER 1 for a JSON boolean,
// so comparing it against json('true') — TEXT — was true for every row, and
// the guard silently did nothing while claiming a confirmed message pays no
// write.
func TestMarkOnWireSkipsRowsThatAlreadyCarryIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	owner := domain.PeerIdentityFromWire(self.Address)
	database := newTestStateDB(t, owner)
	store := testChatlogStore(database.Executor(), owner)

	const target = domain.MessageID("66666666-6666-4666-8666-666666666666")
	if err := store.Append(ctx, "dm", owner, chatlog.Entry{
		ID:        string(target),
		Sender:    self.Address,
		Recipient: domain.PeerIdentityFromWire("7777777777777777777777777777777777777777").String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Metadata:  chatlog.NeverEmittedMetadata,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// First stamp lands.
	if err := store.MarkOnWire(ctx, []domain.MessageID{target}); err != nil {
		t.Fatalf("MarkOnWire: %v", err)
	}
	if onWire, known := chatlog.OnWire(readMetadata(t, database, target)); !known || !onWire {
		t.Fatal("the first stamp did not land")
	}

	// Whether the SECOND one wrote cannot be seen in the value — json_set
	// on an already-true key produces the same JSON. So the row is given
	// an equivalent blob with deliberate whitespace: json_set would
	// normalise it away, and its survival is the proof that the predicate
	// excluded the row instead of rewriting it.
	const spaced = `{"never_emitted": false,  "on_wire": true}`
	if _, err := database.Executor().ExecContext(ctx,
		"UPDATE messages SET metadata = ? WHERE id = ?", spaced, string(target)); err != nil {
		t.Fatalf("seed spaced metadata: %v", err)
	}
	if err := store.MarkOnWire(ctx, []domain.MessageID{target}); err != nil {
		t.Fatalf("MarkOnWire (repeat): %v", err)
	}
	if after := readMetadata(t, database, target); after != spaced {
		t.Errorf("a repeat confirmation rewrote a row that already carried the stamp: %q → %q", spaced, after)
	}
}

func readMetadata(t *testing.T, database *storage.Database, id domain.MessageID) string {
	t.Helper()
	var metadata string
	row := database.Executor().QueryRowContext(context.Background(),
		"SELECT COALESCE(metadata, '') FROM messages WHERE id = ?", string(id))
	if err := row.Scan(&metadata); err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	return metadata
}
