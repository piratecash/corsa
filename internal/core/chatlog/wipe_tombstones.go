package chatlog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// wipe_tombstones.go is the REFUSAL half of message_delete_intents: for how
// long a deleted id must be ignored if it comes back.
//
// A deletion removes the chatlog row AND clears the router's dedup gate for
// its id, which is precisely what lets a relay or inbox replay re-insert it.
// The refusal is the answer. It lives in the same row as the request the
// peer owes us because both are facts about one message, and they only
// differ in how long they last: the request can outlive a contact who never
// returns, the refusal expires within the hour. Neither implies the other —
// a receiver asked to delete a message it has not received yet owes nobody
// anything and only refuses — so the row carries both and dies when it
// carries neither.
//
// On disk rather than in memory alone because the replay window and a
// restart overlap: the process that erased the message can be gone by the
// time the echo lands.

// noteRefusalBatch bounds one statement so a wipe of a very long thread
// cannot build one with more placeholders than SQLite takes.
const noteRefusalBatch = 128

// NoteWipeTombstones refuses every id until expiresAt, creating a row that
// owes nobody anything where there is none.
func (s *Store) NoteWipeTombstones(ctx context.Context, ids []domain.MessageID, expiresAt time.Time) error {
	return noteWipeTombstones(ctx, s.db, ids, expiresAt)
}

// noteWipeTombstones is NoteWipeTombstones against any executor, so the
// transactions that delete the rows can plant their refusals in the same
// commit.
func noteWipeTombstones(ctx context.Context, db execContext, ids []domain.MessageID, expiresAt time.Time) error {
	stamp := expiresAt.UTC().Format(time.RFC3339Nano)
	for start := 0; start < len(ids); start += noteRefusalBatch {
		end := min(start+noteRefusalBatch, len(ids))
		if err := noteWipeTombstoneChunk(ctx, db, ids[start:end], stamp); err != nil {
			return err
		}
	}
	return nil
}

func noteWipeTombstoneChunk(ctx context.Context, db execContext, ids []domain.MessageID, stamp string) error {
	placeholders := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids)*3)
	for _, id := range ids {
		// peer is empty on a refusal-only row: nobody is being asked, so
		// there is no conversation to name. owed=0 keeps it out of the
		// sweep and out of the "waiting for the peer" count.
		placeholders = append(placeholders, "(?, '', ?, ?, 0, 0, 0, ?)")
		args = append(args, string(id), stamp, stamp, stamp)
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO message_delete_intents
			(message_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until)
		VALUES `+strings.Join(placeholders, ", ")+`
		ON CONFLICT(message_id) DO UPDATE SET
			-- The new expiry wins outright. Every caller passes
			-- now + wipeTombstoneTTL, so it is always the later of the
			-- two, and MAX() here would compare RFC3339Nano as TEXT —
			-- where "…:00Z" sorts AFTER "…:00.5Z", so a whole-second
			-- expiry would beat a later fractional one and SHORTEN the
			-- refusal instead of extending it.
			refuse_until = excluded.refuse_until`, args...)
	if err != nil {
		return fmt.Errorf("chatlog: refuse %d message ids: %w", len(ids), err)
	}
	return nil
}

// LiveWipeTombstones returns the refusals that have not expired, as
// id → expiry. Read once at startup: the set is small (the deletions of the
// last hour) and the alternative — a query per inbound message — puts a
// database round-trip on the hot path of every arrival.
func (s *Store) LiveWipeTombstones(ctx context.Context, now time.Time) (map[domain.MessageID]time.Time, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id, refuse_until FROM message_delete_intents WHERE refuse_until > ?`,
		now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, fmt.Errorf("chatlog: select live refusals: %w", err)
	}
	defer func() { _ = rows.Close() }()

	live := make(map[domain.MessageID]time.Time)
	for rows.Next() {
		var (
			id          string
			refuseUntil string
		)
		if err := rows.Scan(&id, &refuseUntil); err != nil {
			return nil, fmt.Errorf("chatlog: scan refusal: %w", err)
		}
		live[domain.MessageID(id)] = parseOptionalTime(refuseUntil)
	}
	return live, rows.Err()
}

// ReapWipeTombstones drops the rows that have nothing left to say — the
// refusal has run out and the peer owes nothing — and reports how many
// went. A row whose refusal expired while the request is still owed keeps
// living; it simply stops refusing.
func (s *Store) ReapWipeTombstones(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM message_delete_intents WHERE owed = 0 AND refuse_until <= ?`,
		now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("chatlog: reap spent delete rows: %w", err)
	}
	reaped, _ := res.RowsAffected()
	return reaped, nil
}

// DropWipeTombstones lifts the refusal on the given ids. Called when the
// deletion that recorded them rolled back: the rows are alive after all, so
// a refusal for one is both a record of a message that still exists and a
// trap that would swallow its next legitimate re-delivery.
//
// A row that owes nothing else goes entirely; one that is still owed keeps
// its request and merely stops refusing.
func (s *Store) DropWipeTombstones(ctx context.Context, ids []domain.MessageID) error {
	if len(ids) == 0 {
		return nil
	}

	// One transaction for the whole call. Lifting a refusal takes two
	// statements — clear it where a request still needs the row, remove
	// the row where nothing else does — and they are one act: "stop
	// refusing these ids". Half of it is the trap this function exists to
	// remove, still set. And the only caller is the rollback of a failed
	// wipe, so the database has ALREADY misbehaved once by the time we
	// get here; a second statement failing on its own is exactly the case
	// to expect.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("chatlog: begin lifting %d refusals: %w", len(ids), err)
	}
	defer func() { _ = tx.Rollback() }()

	for start := 0; start < len(ids); start += noteRefusalBatch {
		end := min(start+noteRefusalBatch, len(ids))
		chunk := ids[start:end]

		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			placeholders = append(placeholders, "?")
			args = append(args, string(id))
		}
		list := strings.Join(placeholders, ", ")

		if _, err := tx.ExecContext(ctx,
			`UPDATE message_delete_intents SET refuse_until = '' WHERE owed = 1 AND message_id IN (`+list+`)`,
			args...); err != nil {
			return fmt.Errorf("chatlog: lift %d refusals: %w", len(chunk), err)
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM message_delete_intents WHERE owed = 0 AND message_id IN (`+list+`)`,
			args...); err != nil {
			return fmt.Errorf("chatlog: drop %d refusal-only rows: %w", len(chunk), err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("chatlog: commit lifting %d refusals: %w", len(ids), err)
	}
	return nil
}
