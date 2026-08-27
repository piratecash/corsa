package chatlog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
// cost: a thread goes in batches, not one commit per row, and an id that was
// already gone is not an error.
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

	removed, err := store.DeleteMessages(ctx, []domain.MessageID{present, absent})
	if err != nil {
		t.Fatalf("DeleteMessages: %v", err)
	}
	if len(removed) != 1 || removed[0] != present {
		t.Fatalf("removed = %v, want only the row that was there", removed)
	}

	// And the batch leaves NOTHING naming either id. A wipe carries no
	// requests — it names no messages by design — so after it the two ids
	// exist nowhere in this file.
	for _, id := range []domain.MessageID{present, absent} {
		if found := tablesNaming(t, store, string(id)); len(found) > 0 {
			t.Errorf("after the wipe, %s is still named in %v", id, found)
		}
	}
}

// TestDeleteWithIntentCommitsTheRequestToo pins both halves of the
// single-message deletion: the row and the request the peer owes us land in
// one commit or not at all.
//
// And the request is what refuses a replay across a restart — the id is on the
// owed list, which is what the router loads at startup. That is the whole of
// the durable protection, and it is durable only while the job is.
func TestDeleteWithIntentCommitsTheRequestToo(t *testing.T) {
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
	}); err != nil {
		t.Fatalf("DeleteWithIntent: %v", err)
	}

	owed, err := store.OwedDeleteIntentMessageIDs(ctx)
	if err != nil {
		t.Fatalf("OwedDeleteIntentMessageIDs: %v", err)
	}
	if len(owed) != 1 || owed[0] != intentMsgID {
		t.Fatalf("owed = %v, want the deleted id: nothing would refuse its replay after a restart", owed)
	}
}

// TestASettledDeletionIsForgottenEntirely is the contract the user asked for in
// one sentence: once the peer confirms, this database says nothing about the
// message ever having existed.
//
// It checks the FILE and not a repository method — every table, then the raw
// bytes of the database and its write-ahead log. A refusal kept "just for the
// replay window" would show up here, which is exactly how the previous design
// failed the promise while passing every test it had.
func TestASettledDeletionIsForgottenEntirely(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self := domain.PeerIdentityFromWire(intentSelf)
	peer := domain.PeerIdentityFromWire(intentPeer)
	path := filepath.Join(t.TempDir(), "state.db")
	store := newTestStoreAt(t, path, self)
	now := time.Now().UTC()

	seedMessageWithJournals(t, store, self, peer, intentMsgID)
	if _, err := store.DeleteWithIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("DeleteWithIntent: %v", err)
	}
	// The peer answers: the task is over.
	if _, err := store.DropDeleteIntent(ctx, intentMsgID); err != nil {
		t.Fatalf("DropDeleteIntent: %v", err)
	}
	if err := store.CheckpointWAL(ctx); err != nil {
		t.Fatalf("CheckpointWAL: %v", err)
	}

	if found := tablesNaming(t, store, string(intentMsgID)); len(found) > 0 {
		t.Errorf("a settled deletion is still recorded in %v", found)
	}
	for _, file := range []string{path, path + "-wal"} {
		raw, err := os.ReadFile(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if bytes.Contains(raw, []byte(intentMsgID)) {
			t.Errorf("%s still contains the id of a message whose deletion is finished", file)
		}
	}
}

// tablesNaming reports every table holding the value in any column. The schema
// is read from sqlite_master rather than listed here, so a table added later is
// searched without anybody remembering to add it.
func tablesNaming(t *testing.T, store *Store, value string) []string {
	t.Helper()

	ctx := context.Background()
	rows, err := store.db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan table name: %v", err)
		}
		tables = append(tables, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("list tables: %v", err)
	}

	var found []string
	for _, table := range tables {
		columns, err := store.db.QueryContext(ctx,
			`SELECT name FROM pragma_table_info(?)`, table)
		if err != nil {
			t.Fatalf("columns of %s: %v", table, err)
		}
		var conditions []string
		for columns.Next() {
			var column string
			if err := columns.Scan(&column); err != nil {
				_ = columns.Close()
				t.Fatalf("scan column of %s: %v", table, err)
			}
			conditions = append(conditions, `CAST("`+column+`" AS TEXT) = ?`)
		}
		if err := errors.Join(columns.Err(), columns.Close()); err != nil {
			t.Fatalf("columns of %s: %v", table, err)
		}
		if len(conditions) == 0 {
			continue
		}
		args := make([]any, len(conditions))
		for i := range args {
			args[i] = value
		}
		var hits int
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM "`+table+`" WHERE `+strings.Join(conditions, " OR "), args...).Scan(&hits); err != nil {
			t.Fatalf("search %s: %v", table, err)
		}
		if hits > 0 {
			found = append(found, table)
		}
	}
	return found
}
