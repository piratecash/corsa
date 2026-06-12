package chatlog

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestSeenAckJournal pins the durable seen-confirmation journal: an inbound
// DM marked "seen" surfaces in UnconfirmedSeen until MarkSeenConfirmed
// records the original sender's seen_ack.
func TestSeenAckJournal(t *testing.T) {
	t.Parallel()
	self := domain.PeerIdentity("self-identity-aaaaaaaaaaaaaaaaaaaaaaaaaa")
	store := NewStore(t.TempDir(), self, domain.ListenAddress(":0"))
	t.Cleanup(func() { _ = store.Close() })

	entry := Entry{
		ID:        "seen-journal-1",
		Sender:    "remote-sender-bbbbbbbbbbbbbbbbbbbbbbbbbb",
		Recipient: string(self),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	if err := store.Append("dm", self, entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	since := time.Now().UTC().Add(-time.Hour)

	// Still "sent" (the append default) — not eligible.
	unconfirmed, err := store.UnconfirmedSeen(self, since)
	if err != nil {
		t.Fatalf("unconfirmed seen: %v", err)
	}
	if len(unconfirmed) != 0 {
		t.Fatalf("non-seen entry must not be reported, got %#v", unconfirmed)
	}

	if _, err := store.UpdateStatus("dm", domain.PeerIdentity(entry.Sender), domain.MessageID(entry.ID), StatusSeen); err != nil {
		t.Fatalf("update status: %v", err)
	}

	unconfirmed, err = store.UnconfirmedSeen(self, since)
	if err != nil {
		t.Fatalf("unconfirmed seen: %v", err)
	}
	if len(unconfirmed) != 1 || unconfirmed[0].ID != entry.ID {
		t.Fatalf("seen entry must be reported until confirmed, got %#v", unconfirmed)
	}

	if err := store.MarkSeenConfirmed(entry.ID); err != nil {
		t.Fatalf("mark confirmed: %v", err)
	}
	// Idempotent.
	if err := store.MarkSeenConfirmed(entry.ID); err != nil {
		t.Fatalf("mark confirmed (repeat): %v", err)
	}

	unconfirmed, err = store.UnconfirmedSeen(self, since)
	if err != nil {
		t.Fatalf("unconfirmed seen: %v", err)
	}
	if len(unconfirmed) != 0 {
		t.Fatalf("confirmed entry must not be reported, got %#v", unconfirmed)
	}
}

// TestDeliveryFailedJournalExcludesFromOutbox pins the durable abandonment:
// once a locally-sent message is journaled as failed, UndeliveredOutgoing
// stops reseeding it even though the row is still in "sent".
func TestDeliveryFailedJournalExcludesFromOutbox(t *testing.T) {
	t.Parallel()
	self := domain.PeerIdentity("self-identity-cccccccccccccccccccccccccc")
	store := NewStore(t.TempDir(), self, domain.ListenAddress(":0"))
	t.Cleanup(func() { _ = store.Close() })

	entry := Entry{
		ID:        "fail-journal-1",
		Sender:    string(self),
		Recipient: "remote-recipient-dddddddddddddddddddddddd",
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	if err := store.Append("dm", self, entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	undelivered, err := store.UndeliveredOutgoing(self)
	if err != nil {
		t.Fatalf("undelivered outgoing: %v", err)
	}
	if len(undelivered) != 1 || undelivered[0].ID != entry.ID {
		t.Fatalf("sent row must be reported before abandonment, got %#v", undelivered)
	}

	if err := store.MarkDeliveryFailed(entry.ID); err != nil {
		t.Fatalf("mark delivery failed: %v", err)
	}
	// Idempotent.
	if err := store.MarkDeliveryFailed(entry.ID); err != nil {
		t.Fatalf("mark delivery failed (repeat): %v", err)
	}

	undelivered, err = store.UndeliveredOutgoing(self)
	if err != nil {
		t.Fatalf("undelivered outgoing: %v", err)
	}
	if len(undelivered) != 0 {
		t.Fatalf("journaled-failed row must not be reseeded, got %#v", undelivered)
	}
}
