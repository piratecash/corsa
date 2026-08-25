package chatlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// delete_intents.go is the storage half of the peer-side delete scheduler
// (docs/dm-commands.md §"Scheduled deletion"). Deleting one's own message
// removes the local row at once — a copy of something the user asked to
// destroy is exactly what must not survive on disk — so what is kept is the
// INTENT: which peer still has to be told, about which message id.
//
// The row carries no body, no sender and no original timestamp. Everything
// the intent needs is addressing plus the scheduler's own bookkeeping; that
// is why this is a table of its own rather than a blanked-out message row,
// which would leave a tombstone in the conversation for anyone reading the
// database.
//
// The table is declared by the shared migration catalog
// (internal/core/storage/migrations/0005_message_delete_intents.sql); this
// file only queries it.

// DeleteIntent is one outstanding "ask this peer to delete this message"
// request. It lives until the peer acknowledges it (any terminal status) or
// until the scheduler gives up on it.
type DeleteIntent struct {
	MessageID domain.MessageID
	Peer      domain.PeerIdentity
	// CreatedAt is when the user asked, kept for diagnostics and for the
	// log line that reports a written-off request. It is NOT a deadline:
	// the give-up budget is spent in attempts, because a calendar clock
	// runs while the peer is unreachable — the stretch this row exists
	// to survive. A re-issue does not refresh it, for the same reason it
	// does not refill the attempts.
	CreatedAt time.Time
	// NextAttemptAt is when the request may be dispatched again. The
	// scheduler moves it two ways: a backoff after a charged attempt, and
	// an uncharged park when the peer cannot answer or has already had
	// its share of this sweep.
	NextAttemptAt time.Time
	// Attempts counts the dispatches this node actually made, whether or
	// not they succeeded. A parked intent is not an attempt.
	Attempts int
	// Hold says why the row is not due, when it is not. The two reasons
	// are not interchangeable: one waits for the peer, the other waits
	// for this node to finish deciding, and a peer reconnecting must
	// release the first without touching the second.
	Hold DeleteIntentHold
}

// DeleteIntentHold is why a request is parked.
type DeleteIntentHold int

const (
	// HoldNone — the row is on its ordinary schedule, either due or
	// waiting out the backoff of an attempt that did go out. A backoff
	// is NOT a park: resetting it would ask a peer whose transport
	// reconnects every few seconds once per handshake.
	HoldNone DeleteIntentHold = 0

	// HoldPeerAbsent — parked by the sweep because the peer could not be
	// asked at all. Released the moment they reconnect.
	//
	// There is no park for "not classified yet". A wipe decides whether a
	// message ever reached the wire from the row it is deleting, inside
	// the same transaction, so a request either exists and is due or was
	// never written at all. A park a timeout could expire would be a
	// privacy rule with a deadline on it.
	HoldPeerAbsent DeleteIntentHold = 1
)

// NoteDeleteIntent records (or re-arms) the intent to have the peer delete
// the message.
//
// A re-issue takes the new due time, so a delete the user asks for again is
// not stuck behind the previous backoff, but keeps the original CreatedAt
// AND the attempts already spent. Both are the same rule: the give-up
// budget belongs to the request, not to the click, and one that resets on
// every re-issue is one a user can make immortal without meaning to.
//
// The peer is NOT rewritten on conflict. The message id is a UUID v4 owned
// by exactly one conversation, so a second intent naming a different peer
// for the same id is a caller bug; re-pointing a pending deletion at
// somebody else is the one thing that must never follow from it.
func (s *Store) NoteDeleteIntent(ctx context.Context, intent DeleteIntent) error {
	return noteDeleteIntent(ctx, s.db, intent)
}

// noteDeleteIntent is NoteDeleteIntent against any executor, so the
// delete-and-schedule transaction can reuse it.
func noteDeleteIntent(ctx context.Context, db execContext, intent DeleteIntent) error {
	if !intent.MessageID.IsValid() {
		return fmt.Errorf("chatlog: delete intent for %q: message id is not a valid UUID v4", intent.MessageID)
	}
	if intent.Peer.IsZero() {
		return fmt.Errorf("chatlog: delete intent for %s: peer is required", intent.MessageID)
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO message_delete_intents (message_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until)
		VALUES (?, ?, ?, ?, ?, ?, 1, '')
		ON CONFLICT(message_id) DO UPDATE SET
			-- A refusal-only row names no peer; an owed one does, and its
			-- peer is never rewritten. A message id belongs to exactly one
			-- conversation, so re-pointing a pending deletion at somebody
			-- else is the one thing that must not follow from a caller bug.
			peer = CASE WHEN message_delete_intents.peer = '' THEN excluded.peer ELSE message_delete_intents.peer END,
			next_attempt_at = excluded.next_attempt_at,
			attempts = MAX(message_delete_intents.attempts, excluded.attempts),
			held = excluded.held,
			owed = 1`,
		string(intent.MessageID),
		intent.Peer.String(),
		intent.CreatedAt.UTC().Format(time.RFC3339Nano),
		intent.NextAttemptAt.UTC().Format(time.RFC3339Nano),
		intent.Attempts,
		int(intent.Hold),
	)
	if err != nil {
		return fmt.Errorf("chatlog: note delete intent %s: %w", intent.MessageID, err)
	}
	return nil
}

// execContext is the write half of storage.Executor, satisfied by both the
// database handle and a transaction.
type execContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// queryContext is execContext's read half, for the one transaction that
// has to consult a row before destroying it.
type queryContext interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readWriteContext is both halves, for a transaction that has to read rows
// it is about to destroy in order to record what they said.
type readWriteContext interface {
	execContext
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// DeleteWithIntent removes the message and records the peer-side delete
// intent in ONE transaction.
//
// The three halves are one invariant seen from three sides: the user's copy
// is gone, somebody still owes us the peer's copy, and a replay of the same
// envelope will be refused rather than re-inserted. Committing them
// separately leaves crash windows in which the local row is destroyed and
// nothing will ever ask the peer, or in which the row is gone but its
// refusal is not — and the next relay retry hands the message back.
//
// Reports whether a row was actually removed; a missing row is not an error
// (the recovery path deletes an id that is already gone) and the intent is
// recorded either way.
func (s *Store) DeleteWithIntent(ctx context.Context, intent DeleteIntent, tombstoneUntil time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("chatlog: begin delete-with-intent %s: %w", intent.MessageID, err)
	}
	defer func() { _ = tx.Rollback() }()

	removed, err := deleteMessageTx(ctx, tx, s.identityAddr, intent.MessageID)
	if err != nil {
		return false, err
	}
	if err := noteDeleteIntent(ctx, tx, intent); err != nil {
		return false, err
	}
	// The refusal is the third face of the same fact. A row deleted
	// without its tombstone can be put straight back by a replay of the
	// same envelope, and a tombstone written after the commit leaves that
	// window open for however long the second write takes — or forever,
	// if the process dies in it.
	if err := noteWipeTombstones(ctx, tx, []domain.MessageID{intent.MessageID}, tombstoneUntil); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("chatlog: commit delete-with-intent %s: %w", intent.MessageID, err)
	}
	return removed, nil
}

// ReviveDeleteIntentsForPeer pulls the peer's PARKED intents forward to
// `now`, so the next sweep dispatches them.
//
// Called when a peer becomes reachable. Without it a parked intent would
// wait out the interval the sweep put it behind, and the promise "the
// request goes out as soon as they are back" would be a promise about that
// interval instead.
//
// Only rows parked FOR THIS PEER move (HoldPeerAbsent). An intent waiting
// out the backoff of an attempt that actually went out is not parked at
// all, and resetting it would hand a peer whose application is dead — but
// whose transport reconnects every few seconds — one request per handshake
// instead of the exponential schedule the backoff exists to impose.
// Attempts are untouched either way: this is the removal of a wait that
// lost its reason, not a retry.
func (s *Store) ReviveDeleteIntentsForPeer(ctx context.Context, peer domain.PeerIdentity, now time.Time) (int64, error) {
	if peer.IsZero() {
		return 0, nil
	}
	stamp := now.UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE message_delete_intents
		SET next_attempt_at = ?, held = 0
		WHERE peer = ? AND owed = 1 AND held = ? AND next_attempt_at > ?`, stamp, peer.String(), int(HoldPeerAbsent), stamp)
	if err != nil {
		return 0, fmt.Errorf("chatlog: revive delete intents for %s: %w", peer, err)
	}
	revived, _ := res.RowsAffected()
	return revived, nil
}

// HoldDeleteIntents parks a batch of intents in ONE statement, WITHOUT
// charging any of them.
//
// One statement rather than one per row because the parked set has no
// upper bound in time: a request to a contact who never comes back is
// kept indefinitely, so a per-row park is a write floor the sweep pays
// every tick for the life of the install.
func (s *Store) HoldDeleteIntents(ctx context.Context, ids []domain.MessageID, until time.Time) error {
	stamp := until.UTC().Format(time.RFC3339Nano)

	for start := 0; start < len(ids); start += holdDeleteIntentBatch {
		end := min(start+holdDeleteIntentBatch, len(ids))
		chunk := ids[start:end]

		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)+2)
		args = append(args, stamp, int(HoldPeerAbsent))
		for _, id := range chunk {
			placeholders = append(placeholders, "?")
			args = append(args, string(id))
		}
		if _, err := s.db.ExecContext(ctx, `
			UPDATE message_delete_intents
			SET next_attempt_at = ?, held = ?
			WHERE message_id IN (`+strings.Join(placeholders, ", ")+`)`, args...); err != nil {
			return fmt.Errorf("chatlog: hold %d delete intents: %w", len(chunk), err)
		}
	}
	return nil
}

// holdDeleteIntentBatch bounds one UPDATE so a large sweep cannot build a
// statement with more placeholders than SQLite takes.
const holdDeleteIntentBatch = 128

// DeleteIntentCountsByPeer reports how many deletions each peer still owes
// us. Drives the "waiting for the peer" indicator in the UI, so it is a
// user-facing number, not diagnostics.
func (s *Store) DeleteIntentCountsByPeer(ctx context.Context) (map[domain.PeerIdentity]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT peer, COUNT(*) FROM message_delete_intents WHERE owed = 1 GROUP BY peer`)
	if err != nil {
		return nil, fmt.Errorf("chatlog: count delete intents per peer: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[domain.PeerIdentity]int)
	for rows.Next() {
		var (
			peer  string
			count int
		)
		if err := rows.Scan(&peer, &count); err != nil {
			return nil, fmt.Errorf("chatlog: scan delete intent count: %w", err)
		}
		counts[domain.PeerIdentityFromWire(peer)] = count
	}
	return counts, rows.Err()
}

// DueDeleteIntents returns the intents whose next attempt is due, oldest
// first, capped at limit. A non-positive limit returns nothing rather than
// the whole table: an unbounded sweep is a way to stall the scheduler
// goroutine, not a feature.
func (s *Store) DueDeleteIntents(ctx context.Context, now time.Time, limit int) ([]DeleteIntent, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, peer, created_at, next_attempt_at, attempts, held
		FROM message_delete_intents
		WHERE owed = 1 AND next_attempt_at <= ?
		ORDER BY next_attempt_at ASC
		LIMIT ?`,
		now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("chatlog: select due delete intents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []DeleteIntent
	for rows.Next() {
		intent, err := scanDeleteIntent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

// DeleteIntentByID reads one OWED intent. The scheduler uses it to check
// an inbound ack against the peer the request was actually addressed to;
// a row that has settled and only lingers to refuse the id is not a
// request any more, so an ack cannot match it.
func (s *Store) DeleteIntentByID(ctx context.Context, messageID domain.MessageID) (DeleteIntent, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT message_id, peer, created_at, next_attempt_at, attempts, held
		FROM message_delete_intents
		WHERE message_id = ? AND owed = 1`, string(messageID))

	intent, err := scanDeleteIntent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return DeleteIntent{}, false, nil
	}
	if err != nil {
		return DeleteIntent{}, false, err
	}
	return intent, true, nil
}

// RecordDeleteIntentAttempt charges one dispatch to the intent and moves it
// to its next due time. A no-op when the intent is already gone (the ack
// won the race), which is why it reports nothing but an error.
func (s *Store) RecordDeleteIntentAttempt(ctx context.Context, messageID domain.MessageID, nextAttemptAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE message_delete_intents
		SET attempts = attempts + 1, next_attempt_at = ?, held = 0
		WHERE message_id = ?`,
		nextAttemptAt.UTC().Format(time.RFC3339Nano), string(messageID))
	if err != nil {
		return fmt.Errorf("chatlog: record delete intent attempt %s: %w", messageID, err)
	}
	return nil
}

// DropDeleteIntent removes the intent and reports whether it was there.
// Called on any terminal outcome: the peer acknowledged the request, or the
// scheduler gave up on it.
func (s *Store) DropDeleteIntent(ctx context.Context, messageID domain.MessageID) (bool, error) {
	// Settled, not forgotten: the peer owes nothing further, but the id
	// must keep being refused for as long as a replay of it is plausible.
	// The row goes when its refusal runs out (ReapWipeTombstones).
	//
	// What survives is only what a refusal needs — the id and its expiry.
	// The peer, the schedule and the attempt count described a request
	// that no longer exists, and keeping them would leave "this id
	// belonged to a conversation with that identity" on disk for a day
	// after the deletion the user asked for.
	res, err := s.db.ExecContext(ctx, `
		UPDATE message_delete_intents
		SET owed = 0, held = 0, peer = '', created_at = '', next_attempt_at = '', attempts = 0
		WHERE message_id = ? AND owed = 1`, string(messageID))
	if err != nil {
		return false, fmt.Errorf("chatlog: settle delete intent %s: %w", messageID, err)
	}
	settled, _ := res.RowsAffected()
	return settled > 0, nil
}

// rowScanner is the shared shape of *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanDeleteIntent(scanner rowScanner) (DeleteIntent, error) {
	var (
		messageID     string
		peer          string
		createdAt     string
		nextAttemptAt string
		attempts      int
		held          int
	)
	if err := scanner.Scan(&messageID, &peer, &createdAt, &nextAttemptAt, &attempts, &held); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeleteIntent{}, err
		}
		return DeleteIntent{}, fmt.Errorf("chatlog: scan delete intent: %w", err)
	}
	return DeleteIntent{
		MessageID:     domain.MessageID(messageID),
		Peer:          domain.PeerIdentityFromWire(peer),
		CreatedAt:     parseOptionalTime(createdAt),
		NextAttemptAt: parseOptionalTime(nextAttemptAt),
		Attempts:      attempts,
		Hold:          DeleteIntentHold(held),
	}, nil
}

// ConversationWipeScope is one reading of a conversation: the rows a wipe
// would take. The wipe deletes exactly this set — not a fresh read of its
// own — so what the caller marked against replays, what the transaction
// destroys and what the peers are asked for are all the same list. A row
// that lands after the reading is outside the wipe on both sides, which is
// the documented asymmetry, and is preferable to one destroyed here while
// falling outside what the peer is told about.
type ConversationWipeScope struct {
	IDs []domain.MessageID
}

// DeleteConversationWithIntents erases the deletable rows of a thread and
// records what the peer now owes us — ONE intent per message, in the same
// transaction.
//
// A conversation wipe is N message deletions, not a different kind of
// thing. Saying so in the data is what removes the whole parallel
// apparatus a bulk request used to need: its own request table, its own
// row-set table, its own scheduler, its own retry and ack, and — on the
// receiving side — a frozen candidate set, a survivor set, a cache of
// answers, and a boundary to describe rows the request could not name.
// None of it survives the observation that "delete this thread" and
// "delete this message" differ only in how many ids are involved.
//
// What follows from it is not just less code: the request is exact (ids,
// never a timestamp, so no clock enters it), it is idempotent (a re-issued
// wipe re-notes the same ids), it needs no size cap (each id travels on
// its own), and a partly-delivered wipe is simply the intents that have
// not settled yet.
//
// Immutable rows are left standing: the flag is a promise no bulk gesture
// overrides. Returns the ids actually removed, so the caller can run the
// side effects a database cannot — file-transfer cleanup and UI eviction.
//
// The requests are written PARKED, due at `dueAt`. Whether a message ever
// reached the wire is the node's answer and cannot be had inside a
// transaction, so the requests are written where they belong — atomically
// with the rows — and released by the caller once it has that answer.
// Nothing can dispatch a request naming a message the peer never saw,
// because until then there is nothing due to dispatch.
func (s *Store) DeleteConversationWithIntents(ctx context.Context, peer domain.PeerIdentity, scope ConversationWipeScope, unsent ConversationWipeClassification, now, tombstoneUntil time.Time) (ConversationWipeResult, error) {
	if peer.IsZero() {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: delete conversation: peer is required")
	}
	if len(scope.IDs) == 0 {
		return ConversationWipeResult{}, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: begin delete-conversation %s: %w", peer, err)
	}
	defer func() { _ = tx.Rollback() }()

	result := ConversationWipeResult{Removed: make([]domain.MessageID, 0, len(scope.IDs))}
	for _, id := range scope.IDs {
		// Classify BEFORE the delete and inside the transaction. The
		// answer to "can the peer have this?" is written on the very row
		// about to be destroyed, so reading it here makes the deletion
		// and its classification one fact. Asking the node for it
		// afterwards cannot: the row is gone by then, and a crash in
		// between leaves a request nobody can classify any more.
		neverEmitted, err := unsent.covers(ctx, tx, id)
		if err != nil {
			return ConversationWipeResult{}, err
		}

		gone, err := deleteMessageTx(ctx, tx, s.identityAddr, id)
		if err != nil {
			return ConversationWipeResult{}, err
		}
		if gone {
			result.Removed = append(result.Removed, id)
		}
		if neverEmitted {
			// Nobody has ever seen this message, so nobody is asked
			// about it: the request would be how the peer learns its id.
			// Only the refusal below is recorded.
			result.Recalled = append(result.Recalled, id)
			continue
		}
		// The intent is noted for every remaining id in scope, not only
		// the rows that were still here: one already removed by a
		// per-message delete keeps that deletion's own intent
		// (noteDeleteIntent is keyed by message id and preserves the
		// schedule it finds), and one removed by something else is
		// still owed by the peer.
		if err := noteDeleteIntent(ctx, tx, DeleteIntent{
			MessageID:     id,
			Peer:          peer,
			CreatedAt:     now,
			NextAttemptAt: now,
		}); err != nil {
			return ConversationWipeResult{}, err
		}
		result.Owed++
	}

	// The conversation's ORPHANED reactions go last, once the messages are
	// gone, because "orphaned" is decided against the rows that are left.
	//
	// It has to be a scope-wide statement rather than a consequence of deleting
	// the messages: a fact still WAITING for a message this node never received
	// has no message row to be deleted through, and if that message ever arrives
	// the repair pass would make it visible in a conversation the user erased.
	//
	// And it deliberately spares the reactions of a message that SURVIVED — an
	// immutable one, which this wipe keeps by design. Erasing those would not
	// stick: the peer re-offers the fact, the message is there, and the chip
	// comes back. See DeleteOrphanReactions.
	if _, err := deleteOrphanReactionsTx(ctx, tx, s.identityAddr, domain.ReactionScopeForPeer(peer)); err != nil {
		return ConversationWipeResult{}, err
	}

	// The refusals go in the same commit as the deletions, so no row can
	// be gone while a replay of its envelope is still welcome. The caller
	// also marks the ids in memory BEFORE calling — that covers the
	// window between reading the scope and this commit, which no
	// transaction can reach into.
	if err := noteWipeTombstones(ctx, tx, scope.IDs, tombstoneUntil); err != nil {
		return ConversationWipeResult{}, err
	}

	// The REACTION refusals go the other way — they are dropped — but ONLY if
	// the conversation is actually gone.
	//
	// That condition is the whole of it. With no message left, an offer from
	// this peer is refused at the door, because the admission check asks whether
	// a conversation exists at all; keeping a row per erased id would then spend
	// a bounded table on ids nothing will ever ask about. But this wipe KEEPS
	// immutable messages, and one survivor is enough to keep the conversation
	// admitting offers — and then every refusal dropped here is an hour of
	// held rows waiting to happen, once per id, as soon as the tombstones go.
	//
	// By SCOPE, not by the ids this wipe touched: everything deleted from this
	// conversation EARLIER is refused too, and those ids are in no list the wipe
	// has — the scope column exists so they can be found at all.
	if _, err := forgetRefusalsIfConversationGoneTx(ctx, tx, s.identityAddr, peer); err != nil {
		return ConversationWipeResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: commit delete-conversation %s: %w", peer, err)
	}
	return result, nil
}

// ConversationWipeClassification is the caller's answer to "which of these
// can the peer not possibly hold?", and how much it is worth.
//
// Two witnesses contribute. Proven names the ids the DELIVERY ENGINE
// reported while it was frozen — it covers a message withheld so recently
// that its durable mark had not landed yet. The rows carry the other half
// and cover a message whose retry entry the engine dropped long ago.
//
// Trusted is what makes either of them usable. The row's mark is only
// meaningful while nothing can emit the message behind the transaction's
// back, which is exactly what the freeze buys. A caller that could NOT
// stop the engine passes Trusted=false, and every message in scope becomes
// a request — the peer is asked about ids they may not resolve, rather
// than a message being deleted here while a copy escapes to them with
// nothing left to recall it.
type ConversationWipeClassification struct {
	Trusted bool
	Proven  map[domain.MessageID]struct{}
}

func (c ConversationWipeClassification) covers(ctx context.Context, tx queryContext, id domain.MessageID) (bool, error) {
	if !c.Trusted {
		return false, nil
	}
	if _, proven := c.Proven[id]; proven {
		return true, nil
	}
	return messageNeverEmittedTx(ctx, tx, id)
}

// ConversationWipeResult is what one wipe transaction did.
type ConversationWipeResult struct {
	// Removed are the ids whose row was actually deleted — what the UI
	// evicts, never the ids merely considered.
	Removed []domain.MessageID
	// Recalled are the ids the rows themselves proved never reached the
	// wire. No request was written for them and none ever will be.
	Recalled []domain.MessageID
	// Owed is how many requests the peer now has to answer.
	Owed int
}

// messageNeverEmittedTx reads the row's durable proof that the message
// never went out. A row that is already gone answers "no proof", which is
// the cautious direction: the request is written and the peer is asked.
func messageNeverEmittedTx(ctx context.Context, tx queryContext, id domain.MessageID) (bool, error) {
	var metadata sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT metadata FROM messages WHERE id = ?`, string(id)).Scan(&metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("chatlog: read emission mark %s: %w", id, err)
	}
	return NeverEmitted(metadata.String), nil
}

// ConversationCandidateIDs lists the rows a wipe of this conversation
// would remove, read on its own so the caller can mark those ids BEFORE
// the transaction takes them: inside a transaction there is no moment at
// which anything could act on them.
func (s *Store) ConversationCandidateIDs(ctx context.Context, peer domain.PeerIdentity) (ConversationWipeScope, error) {
	if peer.IsZero() {
		return ConversationWipeScope{}, fmt.Errorf("chatlog: conversation candidate ids: peer is required")
	}

	id := peer.String()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM messages
		WHERE topic = 'dm'
		  AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))
		  AND COALESCE(flag, '') <> ?
		ORDER BY created_at ASC`,
		s.identityAddr, id, id, s.identityAddr, FlagImmutable)
	if err != nil {
		return ConversationWipeScope{}, fmt.Errorf("chatlog: select conversation %s rows: %w", peer, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []domain.MessageID
	for rows.Next() {
		var messageID string
		if err := rows.Scan(&messageID); err != nil {
			return ConversationWipeScope{}, fmt.Errorf("chatlog: scan conversation %s row: %w", peer, err)
		}
		ids = append(ids, domain.MessageID(messageID))
	}
	if err := rows.Err(); err != nil {
		return ConversationWipeScope{}, fmt.Errorf("chatlog: read conversation %s rows: %w", peer, err)
	}
	return ConversationWipeScope{IDs: ids}, nil
}
