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
	// Kind is what the peer is being asked for. It is read before anything
	// else about the row: a message request names an id, a conversation
	// request names none, and the two are dispatched as different commands.
	Kind DeleteIntentKind
	// MessageID is set on a message request and empty on a conversation
	// one. A thread wipe deliberately carries no ids — naming them is how a
	// peer would learn of messages that never reached them — so the request
	// outlives the ids it was born from.
	MessageID domain.MessageID
	// RequestID binds a conversation request to the ack that settles it, so
	// an answer to a wipe the user has already replaced cannot retire the
	// current one.
	//
	// On a MESSAGE request it names the wipe that is CARRYING it, if any: a
	// per-message request made before a wipe is neither dispatched nor counted
	// while that wipe stands, and goes with it when the peer answers (see
	// coveredByAStandingWipe). Non-empty is therefore a normal state for a
	// message request, not a corrupt one — what settles such a request is still
	// its id, never this field.
	RequestID domain.ConversationDeleteRequestID
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
	// Ref is the exact deleted message when known; empty parties mean a
	// legacy unresolved task, never evidence for a duplicate-delivery receipt.
	Ref domain.MessageRef
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
// The conflict key includes the peer and original message endpoints. A wire
// UUID alone is not conversation authority; another peer or direction may
// legitimately use that same ID without changing this pending request.
func (s *Store) NoteDeleteIntent(ctx context.Context, intent DeleteIntent) error {
	if intent.Ref.Sender == "" && intent.Ref.Recipient == "" {
		ref, found, err := s.RefInConversation(ctx, intent.Peer, intent.MessageID)
		if err != nil {
			return err
		}
		if found {
			intent.Ref = ref
		}
	}
	return noteDeleteIntent(ctx, s.db, intent, s.identityAddr)
}

// noteDeleteIntent is NoteDeleteIntent against any executor, so the
// delete-and-schedule transaction can reuse it.
func noteDeleteIntent(ctx context.Context, db execContext, intent DeleteIntent, self domain.PeerIdentity) error {
	ref := intent.Ref
	if ref.ID != "" && ref.ID != string(intent.MessageID) {
		return fmt.Errorf("delete intent reference differs from id")
	}
	if ref.Sender != "" || ref.Recipient != "" {
		validParties := (ref.Sender == self.String() && ref.Recipient == intent.Peer.String()) || (ref.Sender == intent.Peer.String() && ref.Recipient == self.String())
		if ref.Topic != "dm" || !validParties {
			return fmt.Errorf("delete intent reference differs from conversation")
		}
	}
	if !intent.MessageID.IsValid() {
		return fmt.Errorf("chatlog: delete intent for %q: message id is not a valid UUID v4", intent.MessageID)
	}
	if intent.Peer.IsZero() {
		return fmt.Errorf("chatlog: delete intent for %s: peer is required", intent.MessageID)
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO message_delete_intents (kind, message_id, request_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until,original_sender,original_recipient)
		VALUES ('message', ?, '', ?, ?, ?, ?, ?, 1, '',?,?)
		ON CONFLICT(kind,peer,message_id,original_sender,original_recipient) DO UPDATE SET
			next_attempt_at = excluded.next_attempt_at,
			attempts = MAX(message_delete_intents.attempts, excluded.attempts),
			held = excluded.held,
			owed = 1`,
		string(intent.MessageID),
		intent.Peer.String(),
		intent.CreatedAt.UTC().Format(time.RFC3339Nano),
		intent.NextAttemptAt.UTC().Format(time.RFC3339Nano),
		intent.Attempts,
		int(intent.Hold), ref.Sender, ref.Recipient,
	)
	if err != nil {
		return fmt.Errorf("chatlog: note delete intent: %w", err)
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
// The two halves are one invariant seen from both sides: the user's copy is
// gone, and somebody still owes us the peer's copy. Committing them separately
// leaves a crash window in which the local row is destroyed and nothing will
// ever ask the peer.
//
// The intent is also what refuses a REPLAY of the same envelope, and it does
// that without being a record of anything: while it exists this node is openly
// carrying the task "have this id deleted at the peer", so recognising the id
// is inherent to the job. When the peer answers, the row goes, and with it the
// last durable mention of the message anywhere here — which is why no separate
// tombstone outlives it. See wipe_tombstone_set.go for what carries the answer
// after that, and for the window this leaves open.
//
// Reports whether a row was actually removed; a missing row is not an error
// (the recovery path deletes an id that is already gone) and the intent is
// recorded either way.
func (s *Store) DeleteWithIntent(ctx context.Context, intent DeleteIntent) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("chatlog: begin delete-with-intent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ref := intent.Ref
	if ref.Sender == "" && ref.Recipient == "" {
		query, args := s.peerQuery("dm", intent.Peer, `SELECT topic,id,sender,recipient FROM messages WHERE id=? AND `, ` LIMIT 2`)
		args = append([]interface{}{intent.MessageID}, args...)
		resolved, found, err := uniqueMessageRefQuery(ctx, tx, query, args...)
		if err != nil {
			return false, err
		}
		if found {
			ref = resolved
			intent.Ref = ref
		}
	}
	var removed bool
	if ref.Sender != "" && ref.Recipient != "" {
		removed, err = deleteMessageRefTx(ctx, tx, ref, s.identityAddr)
	}

	if err != nil {
		return false, err
	}
	if err := noteDeleteIntent(ctx, tx, intent, s.identityAddr); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("chatlog: commit delete-with-intent: %w", err)
	}
	return removed, nil
}

// deleteIntentNextAttemptKey is the exact UTC/nanosecond ordering shared with
// migration 0008's expression indexes. RFC3339Nano itself does not sort as time:
// ".1Z" sorts after ".11Z". The migration validates the active legacy domain;
// every repository writer already emits UTC. Keep this expression identical
// to both index definitions; do not replace it with a floating-point date.
const deleteIntentNextAttemptKey = `substr(next_attempt_at, 1, 19) || '.' || substr(replace(substr(next_attempt_at, 21), 'Z', '') || '000000000', 1, 9)`

const deleteIntentComparisonLayout = "2006-01-02T15:04:05.000000000Z"

const reviveDeleteIntentsSQL = `
	UPDATE message_delete_intents
	SET next_attempt_at = ?, held = 0
	WHERE peer = ? AND owed = 1 AND held = ? AND (` + deleteIntentNextAttemptKey + `) > ?`

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
	res, err := s.db.ExecContext(ctx, reviveDeleteIntentsSQL,
		stamp, peer.String(), int(HoldPeerAbsent), now.UTC().Format(deleteIntentComparisonLayout))
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
	intents := make([]DeleteIntent, 0, len(ids))
	for _, id := range ids {
		intent, found, err := s.DeleteIntentByID(ctx, id)
		if err != nil {
			return err
		}
		if found {
			intents = append(intents, intent)
		}
	}
	return s.HoldDeleteIntentRefs(ctx, intents, until)
}

func (s *Store) HoldDeleteIntentRefs(ctx context.Context, intents []DeleteIntent, until time.Time) error {
	for start := 0; start < len(intents); start += holdDeleteIntentBatch {
		end := min(start+holdDeleteIntentBatch, len(intents))
		terms := make([]string, 0, end-start)
		args := []any{until.UTC().Format(time.RFC3339Nano), int(HoldPeerAbsent)}
		for _, intent := range intents[start:end] {
			terms = append(terms, "(?,?,?,?)")
			args = append(args, deleteIntentArgs(intent)...)
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE message_delete_intents SET next_attempt_at=?,held=? WHERE kind='message' AND (peer,message_id,original_sender,original_recipient) IN (`+strings.Join(terms, ",")+`)`, args...); err != nil {
			return err
		}
	}
	return nil
}

// holdDeleteIntentBatch bounds one UPDATE so a large sweep cannot build a
// statement with more placeholders than SQLite takes.
const holdDeleteIntentBatch = 128

// PendingDeletes is what one peer still owes us. The two are counted apart
// because they are different sentences on screen: a number of messages the
// peer has not confirmed erasing, and a whole conversation they have not
// confirmed clearing. Folding the wipe into the number would report "1
// message waiting" for a thread of a thousand.
type PendingDeletes struct {
	Messages     int
	Conversation bool
}

// Any reports whether anything at all is outstanding for this peer.
func (p PendingDeletes) Any() bool {
	return p.Messages > 0 || p.Conversation
}

// DeleteIntentCountsByPeer reports what each peer still owes us. Drives the
// "waiting for the peer" indicator in the UI, so it is user-facing, not
// diagnostics.
func (s *Store) DeleteIntentCountsByPeer(ctx context.Context) (map[domain.PeerIdentity]PendingDeletes, error) {
	// The same exclusion as the sweep: a request the wipe speaks for is not a
	// second thing the user is waiting for, and counting it would put "N
	// messages waiting to be deleted" on a chat whose wipe has just been
	// confirmed.
	rows, err := s.db.QueryContext(ctx, `
		SELECT i.peer, i.kind, COUNT(*) FROM message_delete_intents AS i
		WHERE i.owed = 1
		  AND NOT (`+coveredByAStandingWipe+`)
		GROUP BY i.peer, i.kind`)
	if err != nil {
		return nil, fmt.Errorf("chatlog: count delete intents per peer: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[domain.PeerIdentity]PendingDeletes)
	for rows.Next() {
		var (
			peer  string
			kind  string
			count int
		)
		if err := rows.Scan(&peer, &kind, &count); err != nil {
			return nil, fmt.Errorf("chatlog: scan delete intent count: %w", err)
		}
		identity := domain.PeerIdentityFromWire(peer)
		pending := counts[identity]
		switch DeleteIntentKind(kind) {
		case DeleteIntentConversation:
			pending.Conversation = true
		case DeleteIntentMessage:
			pending.Messages += count
		default:
			return nil, fmt.Errorf("chatlog: delete intent of unknown kind %q owed by %s", kind, identity)
		}
		counts[identity] = pending
	}
	return counts, rows.Err()
}

// coveredByAStandingWipe matches a per-message request whose peer also has a
// conversation request that was made no earlier than it.
//
// Such a request is CARRIED but not SENT. The wipe asks that peer to erase
// everything they hold, which is everything the narrower request asks for, so
// sending both puts two questions about the same rows on the wire and gives the
// peer two chances to answer one of them with a refusal — a refusal the user
// reads as "the peer would not delete it" about a chat they have already
// cleared.
//
// It is kept rather than deleted because the row is still the only thing on
// this disk that names the deleted id, and naming it is what refuses a late
// re-delivery after a restart (OwedDeleteIntentMessageIDs). It goes when the
// wipe that covers it is answered, in the same transaction — see
// SettleConversationDeleteIntent.
//
// The link is the wipe's request id, stamped onto these rows when the wipe is
// written (noteConversationDeleteIntent). A request made AFTER the wipe carries
// no stamp and is therefore not covered — the peer was already asked to erase
// everything before that message existed, so that wipe cannot answer for it.
const coveredByAStandingWipe = `
	i.kind = 'message' AND i.request_id <> '' AND EXISTS (
		SELECT 1 FROM message_delete_intents w
		WHERE w.kind = 'conversation'
		  AND w.peer = i.peer
		  AND w.owed = 1
		  AND w.request_id = i.request_id)`

// SQLite expression indexes supply this exact time ordering. rowid is the
// index's existing implicit tie key; it does not make equal-time requests
// different kinds of work or promise a persistent delivery order.
const dueDeleteIntentsSQL = `
	SELECT ` + deleteIntentColumns + `
	FROM message_delete_intents AS i
	WHERE i.owed = 1 AND (` + deleteIntentNextAttemptKey + `) <= ?
	  AND NOT (` + coveredByAStandingWipe + `)
	ORDER BY (` + deleteIntentNextAttemptKey + `) ASC, i.rowid ASC
	LIMIT ?`

// DueDeleteIntents returns the intents whose next attempt is due, oldest
// first, capped at limit. A non-positive limit returns nothing rather than
// the whole table: an unbounded sweep is a way to stall the scheduler
// goroutine, not a feature.
func (s *Store) DueDeleteIntents(ctx context.Context, now time.Time, limit int) ([]DeleteIntent, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, dueDeleteIntentsSQL,
		now.UTC().Format(deleteIntentComparisonLayout), limit)
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

// OwedDeleteIntentMessageIDs lists the ids whose deletion this node has still
// not had confirmed by the peer.
//
// This is the one durable answer to a replay, and it is allowed to be durable
// because it is not a record of anything: the row is a TASK — "have this id
// deleted at the peer" — that this node is openly carrying, and recognising the
// id it names is inherent to carrying it. It disappears when the task settles,
// which is also the moment the peer's own copy is gone and the replay has no
// source left.
//
// Read once at startup into the in-memory window rather than queried per
// arrival: the set is the deletions still in flight, and the alternative puts a
// database round-trip on the hot path of every message.
func (s *Store) OwedDeleteIntentMessageIDs(ctx context.Context) ([]domain.MessageID, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id FROM message_delete_intents
		WHERE kind = 'message' AND owed = 1 AND message_id IS NOT NULL AND message_id <> ''`)
	if err != nil {
		return nil, fmt.Errorf("chatlog: select owed delete intent ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []domain.MessageID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("chatlog: scan an owed delete intent id: %w", err)
		}
		ids = append(ids, domain.MessageID(id))
	}
	return ids, rows.Err()
}

// DeleteIntentByID reads one OWED intent. The scheduler uses it to check
// an inbound ack against the peer the request was actually addressed to;
// a row that has settled and only lingers to refuse the id is not a
// request any more, so an ack cannot match it.
func (s *Store) DeleteIntentByID(ctx context.Context, id domain.MessageID) (DeleteIntent, bool, error) {
	return uniqueDeleteIntent(ctx, s.db, `message_id=?`, id)
}

func (s *Store) DeleteIntentForPeer(ctx context.Context, peer domain.PeerIdentity, id domain.MessageID) (DeleteIntent, bool, error) {
	return uniqueDeleteIntent(ctx, s.db, `peer=? AND message_id=?`, peer.String(), id)
}

// RecordDeleteIntentAttempt charges one dispatch to the intent and moves it
// to its next due time. A no-op when the intent is already gone (the ack
// won the race), which is why it reports nothing but an error.
func (s *Store) RecordDeleteIntentAttempt(ctx context.Context, id domain.MessageID, next time.Time) error {
	intent, found, err := s.DeleteIntentByID(ctx, id)
	if err != nil || !found {
		return err
	}
	return s.RecordDeleteIntentAttemptFor(ctx, intent, next)
}

func (s *Store) RecordDeleteIntentAttemptFor(ctx context.Context, intent DeleteIntent, next time.Time) error {
	args := append([]any{next.UTC().Format(time.RFC3339Nano)}, deleteIntentArgs(intent)...)
	_, err := s.db.ExecContext(ctx, `UPDATE message_delete_intents SET attempts=attempts+1,next_attempt_at=?,held=0 WHERE `+exactDeleteIntent, args...)
	return err
}

// DropDeleteIntentUnlessCarried retires a per-message request unless a standing
// wipe is carrying it, and says which of the two happened.
//
// A request that a wipe carries is not answerable on its own any more. It was
// sent before the wipe existed, so the peer may still answer it — and if that
// answer is a refusal, publishing it puts "the peer would not delete it" on a
// chat the user has already cleared, which is the message this whole feature
// exists to stop. The wipe asks for that message too and will be answered in
// its own right.
//
// The row is left alone in that case, not deleted: it is what refuses a late
// re-delivery of the id across a restart, and it goes when the wipe it rides on
// is answered (DropConversationDeleteIntent).
//
// One transaction, because the caller decides whether to tell the user based on
// the answer: reading "not carried" and deleting a moment later would let a
// wipe started in between lose the row it had just taken over.
func (s *Store) DropDeleteIntentUnlessCarried(ctx context.Context, id domain.MessageID) (bool, bool, error) {
	intent, found, err := s.DeleteIntentByID(ctx, id)
	if err != nil || !found {
		return false, false, err
	}
	return s.DropDeleteIntentUnlessCarriedFor(ctx, intent)
}

func (s *Store) DropDeleteIntentUnlessCarriedFor(ctx context.Context, intent DeleteIntent) (bool, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, err
	}
	defer func() { _ = tx.Rollback() }()
	var carried int
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM message_delete_intents AS i WHERE i.kind='message' AND i.peer=? AND i.message_id=? AND i.original_sender=? AND i.original_recipient=? AND (`+coveredByAStandingWipe+`))`, deleteIntentArgs(intent)...).Scan(&carried)
	if err != nil {
		return false, false, err
	}
	if carried == 1 {
		return false, true, nil
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM message_delete_intents WHERE `+exactDeleteIntent, deleteIntentArgs(intent)...)
	if err != nil {
		return false, false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, false, err
	}
	if err := tx.Commit(); err != nil {
		return false, false, err
	}
	return n > 0, false, nil
}

// DropDeleteIntent removes the intent and reports whether it was there.
// Called on any terminal outcome: the peer acknowledged the request, or the
// scheduler gave up on it.
func (s *Store) DropDeleteIntent(ctx context.Context, id domain.MessageID) (bool, error) {
	intent, found, err := s.DeleteIntentByID(ctx, id)
	if err != nil || !found {
		return false, err
	}
	return s.DropDeleteIntentFor(ctx, intent)
}

func (s *Store) DropDeleteIntentFor(ctx context.Context, intent DeleteIntent) (bool, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM message_delete_intents WHERE `+exactDeleteIntent, deleteIntentArgs(intent)...)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n > 0, err
}

// rowScanner is the shared shape of *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// deleteIntentColumns is the column list every intent read shares, in the
// order scanDeleteIntent expects. One constant so a new column cannot be
// added to one query and forgotten in another.
const deleteIntentColumns = `kind, message_id, request_id, peer, created_at, next_attempt_at, attempts, held, original_sender, original_recipient`

func scanDeleteIntent(scanner rowScanner) (DeleteIntent, error) {
	var (
		kind              string
		messageID         sql.NullString
		requestID         string
		peer              string
		createdAt         string
		nextAttemptAt     string
		attempts          int
		held              int
		originalSender    string
		originalRecipient string
	)
	if err := scanner.Scan(&kind, &messageID, &requestID, &peer, &createdAt, &nextAttemptAt, &attempts, &held, &originalSender, &originalRecipient); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeleteIntent{}, err
		}
		return DeleteIntent{}, fmt.Errorf("chatlog: scan delete intent: %w", err)
	}
	// A row whose kind the scheduler cannot read is worse than no row: it
	// would be dispatched as whatever the zero value happens to mean.
	if !DeleteIntentKind(kind).Valid() {
		return DeleteIntent{}, fmt.Errorf("chatlog: delete intent of unknown kind %q", kind)
	}
	return DeleteIntent{
		Kind:          DeleteIntentKind(kind),
		Ref:           domain.MessageRef{Topic: "dm", ID: messageID.String, Sender: originalSender, Recipient: originalRecipient},
		MessageID:     domain.MessageID(messageID.String),
		RequestID:     domain.ConversationDeleteRequestID(requestID),
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
	IDs  []domain.MessageID
	Refs []domain.MessageRef
}

// ConversationWipeResult is what one wipe transaction did.
type ConversationWipeResult struct {
	// Removed are the ids whose row was actually deleted — what the UI
	// evicts, never the ids merely considered. A wipe of a thread this node
	// has already emptied removes nothing and is still a wipe: the request
	// it leaves behind is the whole point of it.
	Removed     []domain.MessageID
	RemovedRefs []domain.MessageRef
}

// ConversationCandidateIDs lists the rows a wipe of this conversation would
// remove: every non-immutable message of the thread, whoever wrote it.
//
// Read on its own so the caller can refuse those ids BEFORE the transaction
// takes them — inside a transaction there is no moment at which anything could
// act on them — and read WITHOUT a bound of any kind. The request that arrives
// from a peer says "erase this conversation", not "erase this conversation as
// it stood at some moment": it carried a moment once, and reconciling one
// machine's clock with rows stamped by another's went wrong in one direction or
// the other every time. What a repeat costs is stated where the repeat is
// handled: it erases whatever is there again, and is answered again.
func (s *Store) ConversationCandidateIDs(ctx context.Context, peer domain.PeerIdentity) (ConversationWipeScope, error) {
	if peer.IsZero() {
		return ConversationWipeScope{}, fmt.Errorf("chatlog: conversation candidate ids: peer is required")
	}

	id := peer.String()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id,sender,recipient FROM messages
		WHERE topic = 'dm'
		  AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))
		  AND COALESCE(flag, '') <> ?
		ORDER BY created_at ASC`,
		s.identityAddr, id, id, s.identityAddr, FlagImmutable)
	if err != nil {
		return ConversationWipeScope{}, fmt.Errorf("chatlog: select conversation rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []domain.MessageID
	var refs []domain.MessageRef
	for rows.Next() {
		var messageID, sender, recipient string
		if err := rows.Scan(&messageID, &sender, &recipient); err != nil {
			return ConversationWipeScope{}, fmt.Errorf("chatlog: scan conversation row: %w", err)
		}
		ids = append(ids, domain.MessageID(messageID))
		refs = append(refs, domain.MessageRef{Topic: "dm", ID: messageID, Sender: sender, Recipient: recipient})
	}
	if err := rows.Err(); err != nil {
		return ConversationWipeScope{}, fmt.Errorf("chatlog: read conversation rows: %w", err)
	}
	return ConversationWipeScope{IDs: ids, Refs: refs}, nil
}

// OwedDeleteIntentRefs preserves exact tasks and explicit legacy-unbound ones.
// The volatile refusal set normalizes unknown parties to an ID-only hold.
func (s *Store) OwedDeleteIntentRefs(ctx context.Context) ([]domain.MessageRef, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT message_id,original_sender,original_recipient FROM message_delete_intents WHERE kind='message' AND owed=1 AND message_id IS NOT NULL AND message_id<>''`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var refs []domain.MessageRef
	for rows.Next() {
		ref := domain.MessageRef{Topic: "dm"}
		if err := rows.Scan(&ref.ID, &ref.Sender, &ref.Recipient); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

// OwedDeleteIntents carries the known peer even when a legacy task has no
// recoverable original author. Its caller must not invent an exact reference.
func (s *Store) OwedDeleteIntents(ctx context.Context) ([]DeleteIntent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+deleteIntentColumns+` FROM message_delete_intents WHERE kind='message' AND owed=1 AND message_id IS NOT NULL AND message_id<>''`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var intents []DeleteIntent
	for rows.Next() {
		intent, err := scanDeleteIntent(rows)
		if err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

// OwnIDs names only this side's sends; an incoming colliding id must not
// freeze or cancel an immutable/outgoing message in the opposite direction.
func (scope ConversationWipeScope) OwnIDs(self domain.PeerIdentity) []domain.MessageID {
	var ids []domain.MessageID
	for _, ref := range scope.Refs {
		if ref.Sender == self.String() {
			ids = append(ids, domain.MessageID(ref.ID))
		}
	}
	return ids
}

func uniqueDeleteIntent(ctx context.Context, db messageRows, where string, args ...any) (DeleteIntent, bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+deleteIntentColumns+` FROM message_delete_intents WHERE kind='message' AND owed=1 AND (`+where+`) LIMIT 2`, args...)
	if err != nil {
		return DeleteIntent{}, false, err
	}
	defer func() { _ = rows.Close() }()
	var intent DeleteIntent
	count := 0
	for rows.Next() {
		value, err := scanDeleteIntent(rows)
		if err != nil {
			return DeleteIntent{}, false, err
		}
		intent = value
		count++
	}
	if err := rows.Err(); err != nil {
		return DeleteIntent{}, false, err
	}
	if count > 1 {
		return DeleteIntent{}, false, ErrAmbiguousMessage
	}
	return intent, count == 1, nil
}

func deleteIntentArgs(intent DeleteIntent) []any {
	return []any{intent.Peer.String(), string(intent.MessageID), intent.Ref.Sender, intent.Ref.Recipient}
}

const exactDeleteIntent = `kind='message' AND peer=? AND message_id=? AND original_sender=? AND original_recipient=?`

func (result ConversationWipeResult) OwnIDs(self domain.PeerIdentity) []domain.MessageID {
	var ids []domain.MessageID
	for _, ref := range result.RemovedRefs {
		if ref.Sender == self.String() {
			ids = append(ids, domain.MessageID(ref.ID))
		}
	}
	return ids
}
