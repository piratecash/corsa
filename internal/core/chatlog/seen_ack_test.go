package chatlog

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestSeenAckJournal pins the durable seen-confirmation journal: an inbound
// DM marked "seen" surfaces in UnconfirmedSeen until MarkSeenConfirmed
// records the original sender's seen_ack.
func TestSeenAckJournal(t *testing.T) {
	t.Parallel()
	self := domaintest.ID("self-identity-aaaaaaaaaaaaaaaaaaaaaaaaaa")
	store := newTestStore(t, self)

	entry := Entry{
		ID:        "seen-journal-1",
		Sender:    "remote-sender-bbbbbbbbbbbbbbbbbbbbbbbbbb",
		Recipient: self.String(),
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	if err := store.Append(context.Background(), "dm", self, entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	since := time.Now().UTC().Add(-time.Hour)

	// Still "sent" (the append default) — not eligible.
	unconfirmed, err := store.UnconfirmedSeen(context.Background(), self, since)
	if err != nil {
		t.Fatalf("unconfirmed seen: %v", err)
	}
	if len(unconfirmed) != 0 {
		t.Fatalf("non-seen entry must not be reported, got %#v", unconfirmed)
	}

	if _, err := store.UpdateStatus(context.Background(), "dm", domaintest.ID(entry.Sender), domain.MessageID(entry.ID), StatusSeen); err != nil {
		t.Fatalf("update status: %v", err)
	}

	unconfirmed, err = store.UnconfirmedSeen(context.Background(), self, since)
	if err != nil {
		t.Fatalf("unconfirmed seen: %v", err)
	}
	if len(unconfirmed) != 1 || unconfirmed[0].ID != entry.ID {
		t.Fatalf("seen entry must be reported until confirmed, got %#v", unconfirmed)
	}

	if err := store.MarkSeenConfirmed(context.Background(), entry.ID); err != nil {
		t.Fatalf("mark confirmed: %v", err)
	}
	// Idempotent.
	if err := store.MarkSeenConfirmed(context.Background(), entry.ID); err != nil {
		t.Fatalf("mark confirmed (repeat): %v", err)
	}

	unconfirmed, err = store.UnconfirmedSeen(context.Background(), self, since)
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
	self := domaintest.ID("self-identity-cccccccccccccccccccccccccc")
	store := newTestStore(t, self)

	entry := Entry{
		ID:        "fail-journal-1",
		Sender:    self.String(),
		Recipient: "remote-recipient-dddddddddddddddddddddddd",
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	if err := store.Append(context.Background(), "dm", self, entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	undelivered, err := store.UndeliveredOutgoing(context.Background(), self, time.Time{})
	if err != nil {
		t.Fatalf("undelivered outgoing: %v", err)
	}
	if len(undelivered) != 1 || undelivered[0].ID != entry.ID {
		t.Fatalf("sent row must be reported before abandonment, got %#v", undelivered)
	}

	if err := store.MarkDeliveryFailed(context.Background(), entry.ID); err != nil {
		t.Fatalf("mark delivery failed: %v", err)
	}
	// Idempotent.
	if err := store.MarkDeliveryFailed(context.Background(), entry.ID); err != nil {
		t.Fatalf("mark delivery failed (repeat): %v", err)
	}

	undelivered, err = store.UndeliveredOutgoing(context.Background(), self, time.Time{})
	if err != nil {
		t.Fatalf("undelivered outgoing: %v", err)
	}
	if len(undelivered) != 0 {
		t.Fatalf("journaled-failed row must not be reseeded, got %#v", undelivered)
	}
}

// TestUndeliveredOutgoing_AgeBounded pins the reseed horizon: an undelivered
// DM older than the `since` bound is NOT reseeded, so a restart cannot
// re-inject months-old zombies into the mesh. A recent one still is.
func TestUndeliveredOutgoing_AgeBounded(t *testing.T) {
	t.Parallel()
	self := domaintest.ID("self-identity-eeeeeeeeeeeeeeeeeeeeeeeeee")
	store := newTestStore(t, self)

	recipient := "remote-recipient-ffffffffffffffffffffffff"
	old := Entry{
		ID: "old-undelivered", Sender: self.String(), Recipient: recipient, Body: "sealed",
		CreatedAt: time.Now().UTC().Add(-30 * 24 * time.Hour).Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	recent := Entry{
		ID: "recent-undelivered", Sender: self.String(), Recipient: recipient, Body: "sealed",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	if err := store.Append(context.Background(), "dm", self, old); err != nil {
		t.Fatalf("append old: %v", err)
	}
	if err := store.Append(context.Background(), "dm", self, recent); err != nil {
		t.Fatalf("append recent: %v", err)
	}

	out, err := store.UndeliveredOutgoing(context.Background(), self, time.Now().UTC().Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("undelivered outgoing: %v", err)
	}
	ids := map[string]bool{}
	for _, e := range out {
		ids[e.ID] = true
	}
	if ids["old-undelivered"] {
		t.Fatal("a DM older than the reseed horizon must NOT be reseeded (zombie re-injection)")
	}
	if !ids["recent-undelivered"] {
		t.Fatal("a recent undelivered DM must still be reseeded")
	}
}
