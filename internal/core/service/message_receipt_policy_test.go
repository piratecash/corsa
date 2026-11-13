package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

func TestSignedReceiptPolicySurvivesOutboxReopenAndDamagedBody(t *testing.T) {
	ctx := context.Background()
	author, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	owner := domain.PeerIdentityFromWire(author.Address)
	cfg := storage.Config{ExplicitPath: filepath.Join(t.TempDir(), "state.db"), Owner: owner, Catalog: migrations.Catalog()}
	open := func() *storage.Database {
		t.Helper()
		db, err := storage.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	db := open()
	store := testChatlogStore(db.Executor(), owner)
	adapter := NewMessageStoreAdapter(NewChatlogGateway(store, owner), author, nil, nil)
	now := time.Now().UTC().Truncate(time.Second)
	ref := domain.MessageRef{Topic: "dm", ID: "0b6f3f2e-6c55-4a8e-9a8e-6f3c2b1a0d61", Sender: author.Address, Recipient: recipient.Address}
	payload, err := directmsg.SealV2(author, domain.DMRecipient{
		Address: domain.PeerIdentityFromWire(recipient.Address), BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
	}, directmsg.V2Metadata{
		V2Binding: directmsg.V2Binding{Network: "gazeta-devnet", ID: ref.ID, Topic: ref.Topic,
			Flag: string(protocol.MessageFlagAnyDelete), Sender: ref.Sender, Recipient: ref.Recipient},
		CreatedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
	}, directmsg.PlainMessage{Body: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := protocol.Envelope{ID: protocol.MessageID(ref.ID), Topic: ref.Topic, Sender: ref.Sender,
		Recipient: ref.Recipient, Flag: protocol.MessageFlagAnyDelete, CreatedAt: now, Payload: []byte(payload)}
	if got := adapter.StoreMessage(envelope, true); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v", got)
	}
	readMetadata := func(store *chatlog.Store) string {
		t.Helper()
		entry, found, err := store.EntryByRef(ctx, ref)
		if err != nil || !found {
			t.Fatalf("read exact outbox row: found=%v err=%v", found, err)
		}
		return entry.Metadata
	}
	if initial := readMetadata(store); !chatlog.NeverEmitted(initial) {
		t.Fatal("new row lost its initial emission mark")
	}
	if err := adapter.ClearNeverEmittedRefs([]domain.MessageRef{ref}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.MarkOnWireRefs([]domain.MessageRef{ref}); err != nil {
		t.Fatal(err)
	}
	marker := readMetadata(store)
	if onWire, known := chatlog.OnWire(marker); !known || !onWire || chatlog.NeverEmitted(marker) {
		t.Fatal("emission marks did not transition before reopening the outbox")
	}
	if _, err := db.Executor().ExecContext(ctx, "UPDATE messages SET metadata = '' WHERE id=?", ref.ID); err != nil {
		t.Fatal(err)
	}
	if required, err := adapter.RequiresSignedReceipt(ref); err != nil || !required {
		t.Fatalf("v2 row without receipt metadata = %v, %v", required, err)
	}
	legacyRef := ref
	legacyRef.ID = "legacy-message"
	legacy := envelope
	legacy.ID = protocol.MessageID(legacyRef.ID)
	legacy.Payload = []byte("legacy")
	if got := adapter.StoreMessage(legacy, true); got != node.StoreInserted {
		t.Fatalf("legacy StoreMessage = %v", got)
	}
	if required, err := adapter.RequiresSignedReceipt(legacyRef); err != nil || required {
		t.Fatalf("legacy receipt policy = %v, %v", required, err)
	}
	if _, err := db.Executor().ExecContext(ctx, "UPDATE messages SET body = 'damaged', metadata=? WHERE id=?", marker, ref.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	reopenedStore := testChatlogStore(db.Executor(), owner)
	reopened := NewMessageStoreAdapter(NewChatlogGateway(reopenedStore, owner), author, nil, nil)
	if required, err := reopened.RequiresSignedReceipt(ref); err != nil || !required {
		t.Fatalf("receipt policy after reopen = %v, %v", required, err)
	}
	if onWire, known := chatlog.OnWire(readMetadata(reopenedStore)); !known || !onWire {
		t.Fatal("outbox lost its on-wire mark after reopening")
	}
	ref.Recipient = author.Address
	if required, err := reopened.RequiresSignedReceipt(ref); err != nil || required {
		t.Fatalf("policy leaked to a different reference = %v, %v", required, err)
	}
}
