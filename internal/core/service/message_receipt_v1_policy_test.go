package service

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

func TestDMV1SignedReceiptPolicyRestoredOnBothSides(t *testing.T) {
	ctx := t.Context()
	author, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: "0b6f3f2e-6c55-4a8e-9a8e-6f3c2b1a0d61", Sender: author.Address, Recipient: reader.Address}
	payload, err := directmsg.EncryptForMessageWithSignedReceipts(author, domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(reader.Address),
		BoxKeyBase64: identity.BoxPublicKeyBase64(reader.BoxPublicKey),
	}, domain.OutgoingDM{Body: "persist signed receipt policy"}, ref.Topic, ref.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	protected := protocol.Envelope{ID: protocol.MessageID(ref.ID), Topic: ref.Topic, Sender: ref.Sender,
		Recipient: ref.Recipient, Flag: protocol.MessageFlagAnyDelete, CreatedAt: time.Now().UTC(),
		Payload: []byte(payload), SignedReceiptRequired: true}
	legacyRef := ref
	legacyRef.ID = "08c79c6e-8e95-4429-b293-499018eaa6af"
	legacy := protected
	legacy.ID = protocol.MessageID(legacyRef.ID)
	legacy.Payload = []byte("legacy payload")
	legacy.SignedReceiptRequired = false
	for _, tc := range []struct {
		name     string
		self     *identity.Identity
		outgoing bool
	}{
		{"sender", author, true},
		{"recipient", reader, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := domain.PeerIdentityFromWire(tc.self.Address)
			cfg := storage.Config{ExplicitPath: filepath.Join(t.TempDir(), "state.db"), Owner: owner, Catalog: migrations.Catalog()}
			open := func() (*storage.Database, *MessageStoreAdapter) {
				t.Helper()
				db, err := storage.Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				store := testChatlogStore(db.Executor(), owner)
				return db, NewMessageStoreAdapter(NewChatlogGateway(store, owner), tc.self, nil, nil)
			}
			db, adapter := open()
			for _, msg := range []protocol.Envelope{protected, legacy} {
				if got := adapter.StoreMessage(msg, tc.outgoing); got != node.StoreInserted {
					t.Fatalf("StoreMessage(%s) = %v", msg.ID, got)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			_, adapter = open()
			if required, err := adapter.RequiresSignedReceipt(ref); err != nil || !required {
				t.Fatalf("protected receipt policy after reopening = %v, %v", required, err)
			}
			if required, err := adapter.RequiresSignedReceipt(legacyRef); err != nil || required {
				t.Fatalf("legacy receipt policy after reopening = %v, %v", required, err)
			}
			if tc.outgoing {
				rows, err := adapter.UndeliveredOutgoing()
				if err != nil || len(rows) != 2 {
					t.Fatalf("reseeded outbox rows = %d, %v", len(rows), err)
				}
				for _, row := range rows {
					if want := row.Envelope.ID == protected.ID; row.Envelope.SignedReceiptRequired != want {
						t.Fatalf("reseeded receipt policy for %s = %v, want %v", row.Envelope.ID, row.Envelope.SignedReceiptRequired, want)
					}
				}
			}
		})
	}
}
