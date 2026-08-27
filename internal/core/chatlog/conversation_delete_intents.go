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

// conversation_delete_intents.go is the storage half of "clear this chat for
// both of us".
//
// The gesture used to be written down as one request per message, and that is
// what made it fail: the peer answered every id on its own authority, so a
// thread wipe removed the messages the requester had not written and refused
// the rest. The user was left with an empty conversation while their own half
// of it stood on the other side — and nothing could ask again, because the ids
// go with the rows and a settled request forgets which peer it belonged to.
//
// A wipe is therefore ONE request about the conversation. It carries no ids at
// all, which is both what makes it re-issuable after the local thread is gone
// and what keeps it from telling the peer about messages that never reached
// them.
//
// It shares the table, the scheduler and the ack path with per-message
// requests (see delete_intents.go): the two differ in what they name, not in
// how they are paced, parked, retried or given up on.

// DeleteIntentKind is what a stored request asks the peer for.
type DeleteIntentKind string

const (
	// DeleteIntentMessage asks for one message, named by id.
	DeleteIntentMessage DeleteIntentKind = "message"

	// DeleteIntentConversation asks the peer to erase everything they still
	// hold of one conversation. Non-immutable rows only, whoever wrote them:
	// a thread wipe is a mutual forgetting, confirmed by the user twice
	// before it is sent, and the per-message authorship rule would leave
	// half the thread standing on each side.
	DeleteIntentConversation DeleteIntentKind = "conversation"
)

// Valid reports whether the kind is one the scheduler can dispatch.
func (k DeleteIntentKind) Valid() bool {
	switch k {
	case DeleteIntentMessage, DeleteIntentConversation:
		return true
	default:
		return false
	}
}

// NoteConversationDeleteIntent records (or re-arms) the request that the peer
// clear their side of the conversation.
//
// One live request per peer, enforced by the partial unique index rather than
// by this function remembering to look: a second row would be a second answer
// to wait for. A re-issue takes the NEW request id and the new due time — the
// user asked again, and the ack that settles it must be the answer to THIS
// asking — while the attempts already spent stay, for the same reason a
// per-message re-issue keeps them: a budget a click can refill is one a user
// can make immortal without meaning to.
func (s *Store) NoteConversationDeleteIntent(ctx context.Context, intent DeleteIntent) error {
	// In a transaction because it is two statements: the request, and the
	// stamp that binds this peer's per-message requests to it. A crash between
	// them would leave those requests bound to a wipe that no longer exists —
	// dispatched again on their own, and answered with a refusal about a chat
	// the user has already cleared.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("chatlog: begin note conversation delete intent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := noteConversationDeleteIntent(ctx, tx, intent); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("chatlog: commit note conversation delete intent: %w", err)
	}
	return nil
}

// noteConversationDeleteIntent is NoteConversationDeleteIntent against any
// executor, so the wipe transaction can plant the request in the same commit
// that erases the rows.
func noteConversationDeleteIntent(ctx context.Context, db execContext, intent DeleteIntent) error {
	if intent.Kind != DeleteIntentConversation {
		return fmt.Errorf("chatlog: conversation delete intent for %s: kind is %q, want %q",
			intent.Peer, intent.Kind, DeleteIntentConversation)
	}
	if intent.Peer.IsZero() {
		return fmt.Errorf("chatlog: conversation delete intent: peer is required")
	}
	if intent.RequestID == "" {
		return fmt.Errorf("chatlog: conversation delete intent for %s: request id is required", intent.Peer)
	}
	if intent.MessageID != "" {
		// A conversation request that names a message would be dispatched
		// as a wipe and settled as a single deletion, or the other way
		// round, depending on which query found it first.
		return fmt.Errorf("chatlog: conversation delete intent for %s names message %s", intent.Peer, intent.MessageID)
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO message_delete_intents (kind, message_id, request_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until)
		VALUES ('conversation', NULL, ?, ?, ?, ?, ?, ?, 1, '')
		ON CONFLICT(peer) WHERE kind = 'conversation' DO UPDATE SET
			request_id = excluded.request_id,
			-- The re-issue's own moment, unlike a per-message re-issue which
			-- keeps the original: the user asked again, and the row now stands
			-- for THAT asking. Nothing compares it against anything — the wipe
			-- carries no boundary — so it is a diagnostic and an ordering hint,
			-- not a value the peer applies.
			created_at = excluded.created_at,
			next_attempt_at = excluded.next_attempt_at,
			attempts = MAX(message_delete_intents.attempts, excluded.attempts),
			held = excluded.held,
			owed = 1`,
		string(intent.RequestID),
		intent.Peer.String(),
		intent.CreatedAt.UTC().Format(time.RFC3339Nano),
		intent.NextAttemptAt.UTC().Format(time.RFC3339Nano),
		intent.Attempts,
		int(intent.Hold),
	)
	if err != nil {
		return fmt.Errorf("chatlog: note conversation delete intent: %w", err)
	}

	// Bind this peer's per-message requests to the wipe, which from here CARRIES
	// them: it asks the peer for everything they ask for, so they are neither
	// sent nor counted while it stands, and they go with it when it is answered
	// (see coveredByAStandingWipe and DropConversationDeleteIntent).
	//
	// The binding is the request id, not a comparison of timestamps. The stamps
	// are RFC3339Nano text with a variable-length fraction, so SQLite orders
	// ".1Z" after ".11Z" — a boundary built on them would sometimes send a
	// carried request on its own and sometimes fail to retire it.
	//
	// ALL of them, including any already bound to an earlier wipe: a re-issued
	// wipe asks for everything present now, which is exactly what the requests
	// made since the previous one name.
	if _, err := db.ExecContext(ctx, `
		UPDATE message_delete_intents SET request_id = ?
		WHERE peer = ? AND kind = 'message'`,
		string(intent.RequestID), intent.Peer.String()); err != nil {
		return fmt.Errorf("chatlog: bind the per-message requests to the wipe: %w", err)
	}
	return nil
}

// ConversationDeleteIntentForPeer reads the peer's outstanding wipe request.
// The scheduler uses it to check an inbound ack against the request that is
// actually pending: an ack echoing an older request id answers a wipe the user
// has already replaced.
func (s *Store) ConversationDeleteIntentForPeer(ctx context.Context, peer domain.PeerIdentity) (DeleteIntent, bool, error) {
	if peer.IsZero() {
		return DeleteIntent{}, false, nil
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT `+deleteIntentColumns+`
		FROM message_delete_intents
		WHERE peer = ? AND kind = 'conversation' AND owed = 1`, peer.String())

	intent, err := scanDeleteIntent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return DeleteIntent{}, false, nil
	}
	if err != nil {
		return DeleteIntent{}, false, err
	}
	return intent, true, nil
}

// RecordConversationDeleteAttempt charges one dispatch to the peer's wipe
// request and moves it to its next due time. A no-op when the request is
// already gone (the ack won the race), which is why it reports nothing but an
// error.
//
// Scoped by request id, like every other write to this row. The dispatch that
// is being charged was made under a request the user may have replaced since;
// charging the row by peer alone would put that attempt — and its backoff — on
// a request that has not been sent once.
func (s *Store) RecordConversationDeleteAttempt(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID, nextAttemptAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE message_delete_intents
		SET attempts = attempts + 1, next_attempt_at = ?, held = 0
		WHERE peer = ? AND kind = 'conversation' AND owed = 1 AND request_id = ?`,
		nextAttemptAt.UTC().Format(time.RFC3339Nano), peer.String(), string(requestID))
	if err != nil {
		return fmt.Errorf("chatlog: record conversation delete attempt: %w", err)
	}
	return nil
}

// DropConversationDeleteIntent removes the peer's wipe request and reports
// whether it was there. Called on any terminal outcome: the peer answered, or
// the scheduler gave up.
//
// The request id is part of the WHERE clause, not a condition the caller
// checked a moment ago. Reading the row, deciding, and deleting by peer alone
// is a window in which a fresh wipe can be reserved and then retired by the
// answer to the previous one — the user would be told their chat is cleared on
// both sides while nothing had been asked of the peer at all.
//
// DELETED, like a settled per-message request. Neither leaves anything behind:
// what refuses a replay lives in memory (service/wipe_tombstone_set.go); a wipe
// names no id, so there is nothing here that could name one either.
// It also takes the per-message requests THIS wipe was carrying — the ones made
// before it, which it asked the peer to satisfy along with everything else (see
// coveredByAStandingWipe). They were kept while the wipe stood because the row
// is what refuses a late re-delivery of its id across a restart; the answer that
// retires the wipe is the moment that stops being needed, since the peer has
// erased their whole side and there is nothing left to re-deliver from.
//
// One transaction, so the two cannot come apart: a crash between them would
// leave requests nothing carries — the wipe gone, the narrow ones dispatched
// again on their own, and a `denied` for a message in a chat the user has
// already been told is cleared.
//
// Per-message requests made AFTER the wipe are untouched. Those name messages
// that arrived after the peer was asked to erase everything, so this wipe never
// asked about them and cannot answer for them.
func (s *Store) DropConversationDeleteIntent(ctx context.Context, peer domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) (bool, error) {
	if peer.IsZero() {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("chatlog: begin drop conversation delete intent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		DELETE FROM message_delete_intents
		WHERE peer = ? AND kind = 'conversation' AND request_id = ?`,
		peer.String(), string(requestID))
	if err != nil {
		return false, fmt.Errorf("chatlog: drop conversation delete intent: %w", err)
	}
	if dropped, _ := res.RowsAffected(); dropped == 0 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM message_delete_intents
		WHERE peer = ? AND kind = 'message' AND request_id = ?`,
		peer.String(), string(requestID)); err != nil {
		return false, fmt.Errorf("chatlog: drop the requests the wipe carried: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("chatlog: commit drop conversation delete intent: %w", err)
	}
	return true, nil
}

// HoldConversationDeleteIntents parks a batch of wipe requests in ONE
// statement, WITHOUT charging any of them — the conversation-level twin of
// HoldDeleteIntents, and for the same reason: a request to a contact who never
// comes back is kept indefinitely, so a per-row park is a write floor the
// sweep pays every tick for the life of the install.
func (s *Store) HoldConversationDeleteIntents(ctx context.Context, peers []domain.PeerIdentity, until time.Time) error {
	stamp := until.UTC().Format(time.RFC3339Nano)

	for start := 0; start < len(peers); start += holdDeleteIntentBatch {
		end := min(start+holdDeleteIntentBatch, len(peers))
		chunk := peers[start:end]

		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)+2)
		args = append(args, stamp, int(HoldPeerAbsent))
		for _, peer := range chunk {
			placeholders = append(placeholders, "?")
			args = append(args, peer.String())
		}
		if _, err := s.db.ExecContext(ctx, `
			UPDATE message_delete_intents
			SET next_attempt_at = ?, held = ?
			WHERE kind = 'conversation' AND owed = 1 AND peer IN (`+strings.Join(placeholders, ", ")+`)`, args...); err != nil {
			return fmt.Errorf("chatlog: hold %d conversation delete intents: %w", len(chunk), err)
		}
	}
	return nil
}

// DeleteConversationWithIntent erases the deletable rows of a thread and
// records, in the SAME transaction, the one request the peer now owes us.
//
// Either the conversation is gone from this side AND somebody is bound to ask
// the peer, or nothing happened and the user can click again. A half-applied
// wipe is the one outcome they cannot see.
//
// An EMPTY scope still writes the request. That is not a corner case, it is
// the repair path: a thread this node already erased — because an older build
// asked per message and the peer refused the requester's own rows — has no ids
// left to name, and the whole reason the request carries none is so it can
// still be made.
//
// Immutable rows are left standing, here and on the peer's side: that flag is
// a promise no gesture overrides. Returns the ids actually removed, so the
// caller can run the side effects a database cannot — file-transfer cleanup
// and UI eviction.
func (s *Store) DeleteConversationWithIntent(ctx context.Context, peer domain.PeerIdentity, scope ConversationWipeScope, intent DeleteIntent) (ConversationWipeResult, error) {
	if peer.IsZero() {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: delete conversation: peer is required")
	}
	if intent.Peer != peer {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: delete conversation %s: the request names %s", peer, intent.Peer)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: begin delete-conversation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := wipeConversationTx(ctx, tx, s.identityAddr, peer, scope)
	if err != nil {
		return ConversationWipeResult{}, err
	}
	if err := noteConversationDeleteIntent(ctx, tx, intent); err != nil {
		return ConversationWipeResult{}, err
	}
	// This peer's per-message requests are LEFT STANDING, and the reason is not
	// that they are harmless.
	//
	// They look redundant: the wipe asks the same peer for everything the
	// narrower request asks for. But each of them is a request the peer has not
	// answered yet, and a request is not written off until it is answered —
	// dropping it here would write off a deletion the user asked for, on the
	// strength of a second one that has not been delivered either.
	//
	// The second reason is the one that bites after a restart. A per-message
	// request is also what refuses a late re-delivery of its id (the row names
	// the message, and service/wipe_tombstone_set.go reads those ids back at
	// startup). The wipe names nothing, so it cannot refuse anything. Delete an
	// incoming message while the peer is offline, clear the chat, restart before
	// the peer returns, and a copy still held by a relay would be taken back in.
	//
	// What the overlap actually costs is small and self-clearing: once the peer
	// applies the wipe, the per-message request is answered `not_found`, which
	// retires it.
	if err := tx.Commit(); err != nil {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: commit delete-conversation: %w", err)
	}
	return result, nil
}

// DeleteConversationForPeerRequest is the receiving half: the peer asked for
// the thread to be cleared, and this erases it here.
//
// Same transaction, and deliberately NO request of our own — we owe the peer
// nothing for a deletion they asked for, and writing an intent here would send
// the wipe back to them for as long as both sides kept answering each other.
//
// NOTHING is recorded about having done it, and a repeat is therefore not
// harmless: it erases whatever the thread holds the second time as well. The
// alternative was a note on this disk saying which conversation was erased and
// when, which is the one thing a deletion must not leave behind. Deleted is
// deleted.
func (s *Store) DeleteConversationForPeerRequest(
	ctx context.Context,
	peer domain.PeerIdentity,
	scope ConversationWipeScope,
) (ConversationWipeResult, error) {
	if peer.IsZero() {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: apply conversation delete: peer is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: begin apply-conversation-delete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := wipeConversationTx(ctx, tx, s.identityAddr, peer, scope)
	if err != nil {
		return ConversationWipeResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ConversationWipeResult{}, fmt.Errorf("chatlog: commit apply-conversation-delete: %w", err)
	}
	return result, nil
}

// wipeConversationTx erases the scope and everything that hangs off it, in
// somebody else's transaction. Both sides of a wipe do exactly this much; what
// differs is only whether a request is left behind.
func wipeConversationTx(
	ctx context.Context,
	tx interface {
		execContext
		queryContext
		readWriteContext
	},
	self, peer domain.PeerIdentity,
	scope ConversationWipeScope,
) (ConversationWipeResult, error) {
	result := ConversationWipeResult{Removed: make([]domain.MessageID, 0, len(scope.IDs))}
	for _, id := range scope.IDs {
		gone, err := deleteMessageTx(ctx, tx, id)
		if err != nil {
			return ConversationWipeResult{}, err
		}
		if gone {
			result.Removed = append(result.Removed, id)
		}
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
	if _, err := deleteOrphanReactionsTx(ctx, tx, self, domain.ReactionScopeForPeer(peer)); err != nil {
		return ConversationWipeResult{}, err
	}

	// And NOTHING is written down about the ids that just left. The caller
	// marks them in memory (wipe_tombstone_set.go) so a replay arriving in this
	// process is refused; a row saying "these ids are refused" would survive the
	// wipe, name what it was for, and be the trace the wipe exists to remove.
	return result, nil
}
