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

	// The sender is written the way the peer identity renders it: the
	// status update is scoped to the CONVERSATION, so a fixture whose row
	// and peer disagree textually is testing nothing the production path
	// can produce.
	remote := domaintest.ID("remote-sender-bbbbbbbbbbbbbbbbbbbbbbbbbb")
	entry := Entry{
		ID:        "seen-journal-1",
		Sender:    remote.String(),
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

	if _, err := store.UpdateStatus(context.Background(), "dm", remote, domain.MessageID(entry.ID), StatusSeen); err != nil {
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

	// The journal speaks only for messages that have ACTUALLY expired.
	// Abandonment has exactly one cause now — a message outliving its own
	// TTL — so a journalled row still within its TTL, or carrying none at
	// all, was put there by a rule that no longer exists: the retry cap
	// this engine used to have, which gave up after ~3.5 hours regardless
	// of how long the message was entitled to live.
	base := Entry{
		Sender:    self.String(),
		Recipient: "remote-recipient-dddddddddddddddddddddddd",
		Body:      "sealed",
		CreatedAt: time.Now().UTC().Add(-4 * time.Hour).Format(time.RFC3339Nano),
		Flag:      "immutable",
	}
	rows := map[string]int{
		// Sent to live one hour, four hours ago: genuinely expired.
		"fail-journal-expired": 3600,
		// Sent to live a full day, abandoned three and a half hours in by
		// the old cap. Still perfectly deliverable.
		"fail-journal-capped-early": 86400,
		// No TTL at all — the ordinary case, and the one the old cap hit
		// most often.
		"fail-journal-perpetual": 0,
	}
	for id, ttl := range rows {
		entry := base
		entry.ID = id
		entry.TTLSeconds = ttl
		if err := store.Append(context.Background(), "dm", self, entry); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
		if err := store.MarkDeliveryFailed(context.Background(), id); err != nil {
			t.Fatalf("mark delivery failed %s: %v", id, err)
		}
		// Idempotent.
		if err := store.MarkDeliveryFailed(context.Background(), id); err != nil {
			t.Fatalf("mark delivery failed %s (repeat): %v", id, err)
		}
	}

	undelivered, err := store.UndeliveredOutgoing(context.Background(), self, time.Time{}, time.Now().UTC())
	if err != nil {
		t.Fatalf("undelivered outgoing: %v", err)
	}
	got := make(map[string]bool, len(undelivered))
	for _, entry := range undelivered {
		got[entry.ID] = true
	}
	if got["fail-journal-expired"] {
		t.Error("a message that outlived its own TTL was resurrected")
	}
	if !got["fail-journal-capped-early"] {
		t.Error("a day-long message abandoned three hours in is still blocked: testing for the PRESENCE of a TTL is not enough")
	}
	if !got["fail-journal-perpetual"] {
		t.Error("a message with no TTL is still blocked by the retry cap that no longer exists")
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

	out, err := store.UndeliveredOutgoing(context.Background(), self, time.Now().UTC().Add(-7*24*time.Hour), time.Now().UTC())
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
