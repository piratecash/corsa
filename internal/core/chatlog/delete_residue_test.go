package chatlog

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// journalRowCount counts the rows a message id still owns in the per-message
// journals. Queried directly: what matters is what is left in the FILE, not
// what a repository method chooses to report.
func journalRowCount(t *testing.T, store *Store, messageID string) int {
	t.Helper()

	total := 0
	for _, query := range []string{
		`SELECT COUNT(*) FROM seen_ack WHERE id = ?`,
		`SELECT COUNT(*) FROM delivery_failed WHERE id = ?`,
		`SELECT COUNT(*) FROM decrypt_resend_intents WHERE original_id = ? OR replacement_id = ?`,
	} {
		args := []any{messageID}
		if query == `SELECT COUNT(*) FROM decrypt_resend_intents WHERE original_id = ? OR replacement_id = ?` {
			args = append(args, messageID)
		}
		var count int
		if err := store.db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
			t.Fatalf("count %q: %v", query, err)
		}
		total += count
	}
	return total
}

// seedMessageWithJournals writes a message plus every per-message trace the
// other repositories keep under its id.
func seedMessageWithJournals(t *testing.T, store *Store, self, peer domain.PeerIdentity, id domain.MessageID) {
	t.Helper()

	ctx := context.Background()
	if _, err := store.AppendReportNew(ctx, "dm", peer, Entry{
		ID:        string(id),
		Sender:    self.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("AppendReportNew: %v", err)
	}
	if err := store.MarkSeenConfirmed(ctx, string(id)); err != nil {
		t.Fatalf("MarkSeenConfirmed: %v", err)
	}
	if err := store.MarkDeliveryFailed(ctx, string(id)); err != nil {
		t.Fatalf("MarkDeliveryFailed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO decrypt_resend_intents (root, original_id, peer, replacement_id, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		string(id), string(id), peer.String(), "", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed resend intent: %v", err)
	}

	if got := journalRowCount(t, store, string(id)); got != 3 {
		t.Fatalf("seeded journal rows = %d, want 3 — the fixture stopped covering the tables it is meant to", got)
	}
}

// TestDeleteByIDClearsPerMessageJournals pins that deleting a message takes
// its traces with it. A row in seen_ack or delivery_failed is a durable
// record that a message with this id existed and how its delivery went —
// exactly the metadata a deletion is supposed to remove.
func TestDeleteByIDClearsPerMessageJournals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self := domain.PeerIdentityFromWire(intentSelf)
	peer := domain.PeerIdentityFromWire(intentPeer)
	store := newTestStore(t, self)

	seedMessageWithJournals(t, store, self, peer, intentMsgID)

	removed, err := store.DeleteByID(ctx, intentMsgID)
	if err != nil || !removed {
		t.Fatalf("DeleteByID: removed=%v err=%v", removed, err)
	}
	if got := journalRowCount(t, store, string(intentMsgID)); got != 0 {
		t.Errorf("journal rows left after the delete = %d, want 0", got)
	}
}

// TestDeleteByPeerClearsPerMessageJournals is the same contract for the
// whole-conversation wipe, plus the intents: after it, nothing in the
// database still names the thread.
func TestDeleteByPeerClearsPerMessageJournals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self := domain.PeerIdentityFromWire(intentSelf)
	peer := domain.PeerIdentityFromWire(intentPeer)
	store := newTestStore(t, self)

	seedMessageWithJournals(t, store, self, peer, intentMsgID)
	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgB, Peer: peer, CreatedAt: time.Now().UTC(), NextAttemptAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	removed, err := store.DeleteByPeer(ctx, peer)
	if err != nil || removed != 1 {
		t.Fatalf("DeleteByPeer: removed=%d err=%v, want 1", removed, err)
	}
	if got := journalRowCount(t, store, string(intentMsgID)); got != 0 {
		t.Errorf("journal rows left after the wipe = %d, want 0", got)
	}
	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil {
		t.Fatalf("DeleteIntentCountsByPeer: %v", err)
	}
	if len(counts) != 0 {
		t.Errorf("delete intents left after the wipe = %v; they are the last rows naming the erased thread", counts)
	}
}

// TestCheckpointWALSucceedsOnAQuietDatabase — the deletion paths call this to
// retire the pages holding the removed content from the -wal file. It must
// work on an ordinary open database, not just in theory.
func TestCheckpointWALSucceedsOnAQuietDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self := domain.PeerIdentityFromWire(intentSelf)
	peer := domain.PeerIdentityFromWire(intentPeer)
	store := newTestStore(t, self)

	seedMessageWithJournals(t, store, self, peer, intentMsgID)
	if _, err := store.DeleteByID(ctx, intentMsgID); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}
	if err := store.CheckpointWAL(ctx); err != nil {
		t.Fatalf("CheckpointWAL: %v", err)
	}
}

// TestDeleteMessagesBatchesOneTransaction pins the receiver's side of the
// cost: a thread goes in batches, not one commit per row, and every id in
// the batch is refused afterwards — including ones that were already gone,
// which can still be replayed.
func TestDeleteMessagesBatchesOneTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self := domain.PeerIdentityFromWire(intentSelf)
	peer := domain.PeerIdentityFromWire(intentPeer)
	store := newTestStore(t, self)
	now := time.Now().UTC()

	present := domain.MessageID("6a000000-1111-4222-8333-444444444444")
	absent := domain.MessageID("6b000000-1111-4222-8333-444444444444")
	if _, err := store.AppendReportNew(ctx, "dm", peer, Entry{
		ID:        string(present),
		Sender:    self.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("AppendReportNew: %v", err)
	}

	removed, err := store.DeleteMessages(ctx, []domain.MessageID{present, absent}, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("DeleteMessages: %v", err)
	}
	if len(removed) != 1 || removed[0] != present {
		t.Fatalf("removed = %v, want only the row that was there", removed)
	}

	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	for _, id := range []domain.MessageID{present, absent} {
		if _, ok := live[id]; !ok {
			t.Errorf("%s was deleted without a refusal; a replay would put it back", id)
		}
	}
}

// TestDeleteWithIntentCommitsTheTombstoneToo pins the third half of the
// single-message deletion: the row, the request the peer owes us, and the
// refusal of the id land in one commit or not at all.
func TestDeleteWithIntentCommitsTheTombstoneToo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self := domain.PeerIdentityFromWire(intentSelf)
	peer := domain.PeerIdentityFromWire(intentPeer)
	store := newTestStore(t, self)
	now := time.Now().UTC()

	if _, err := store.AppendReportNew(ctx, "dm", peer, Entry{
		ID:        string(intentMsgID),
		Sender:    self.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("AppendReportNew: %v", err)
	}

	if _, err := store.DeleteWithIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}, now.Add(time.Hour)); err != nil {
		t.Fatalf("DeleteWithIntent: %v", err)
	}

	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	if _, ok := live[intentMsgID]; !ok {
		t.Error("the deletion committed without its refusal; a replay would resurrect the message")
	}
}
