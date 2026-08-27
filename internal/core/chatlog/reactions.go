package chatlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// reactions.go is the storage half of message reactions. The model it serves
// is in docs/refactoring/reactions-protocol.md; the short version is that one
// row is one FACT by one actor about one emoji on one message, the count under
// a message is a fold over rows, and a cleared reaction stays as a tombstone so
// a late duplicate of the "set" cannot resurrect it.
//
// The tables are declared by the shared migration catalog
// (internal/core/storage/migrations/0006_message_reactions.sql); this file only
// queries them.

// Four bounds keep this table finite against a peer that is not trying to be
// reasonable. Each closes a different door, and none of them is redundant:
//
//   - HasConversationWith decides WHO may make a row wait at all;
//   - MaxHeldReactionsPerActor bounds how MANY may wait per actor;
//   - HeldReactionTTL bounds how LONG one may wait;
//   - MaxReactionsPerActorPerMessage bounds the width of one message.
//
// The first is what makes the rest mean anything. The three ceilings are per
// ACTOR, and an identity costs nothing to mint, so alone they bound "as many
// identities as an attacker cares to make, times the ceiling" — which is not a
// bound. Requiring an existing conversation makes that multiplier the number of
// people the user really talks to.
//
// And a TTL alone would not do either: it limits the age of a row, not the
// number of them, and a peer sending at datagram rate fills an hour's worth.

// HeldReactionTTL is how long a fact may wait for a message that has not
// arrived.
//
// Measured from `first_seen_at`, which never moves, and NOT from `updated_at`,
// which every accepted write refreshes. That is the whole point of carrying two
// timestamps: the sender chooses when to re-state a fact, so a TTL measured
// from the last write is one the sender can extend forever.
//
// An hour is well past any plausible ordering between a message and a reaction
// to it; past that the fact is not early, it is about a message that is not
// coming.
const HeldReactionTTL = time.Hour

// MaxHeldReactionsPerActor bounds how many facts one actor may have waiting for
// messages this node does not have.
//
// A held row names a message this node has never had, so no user action ever
// names it: a deletion is issued for a message somebody can SEE. The only two
// things that reach one are this ceiling and the sweep, and the ceiling is what
// bounds the table WITHIN one TTL window — the shape that has already cost this
// project memory twice (see the ban maps and seenReceipts).
//
// Generous against the real case, which is a handful of reactions arriving in
// the seconds before their message.
const MaxHeldReactionsPerActor = 256

// MaxReactionsPerActorPerMessage bounds how many distinct emoji one actor may
// have TOUCHED on one message — set ones and cleared ones alike.
//
// Tombstones are counted because they are rows, and rows are what this bounds:
// a cleared reaction keeps its row on purpose (see the migration), so counting
// only the standing ones would leave the table as large as a peer likes. The
// visible consequence is that after this many distinct emoji on one message,
// including ones taken back, no further emoji can be added to it — while the
// ones already there stay settable and clearable. Far above what a person does:
// the design's picker offers seven quick choices.
const MaxReactionsPerActorPerMessage = 32

// ApplyReactionFact merges one fact into the state and reports whether the
// state changed.
//
// The merge is a single comparison because the key has exactly one writer (see
// domain.ReactionFact): an older or repeated fact is a no-op, which is what
// makes the whole feature safe over a transport that delivers zero or more
// times in no order.
//
// `false, nil` means the row was not touched. Three causes, and they are not
// told apart: the fact is a repeat at the same clock, it was superseded by a
// newer one, or it is a NEW key that would have crossed a ceiling. The caller
// does the same thing for all three (nothing), and distinguishing them would
// mean a second query whose answer is stale the moment it returns.
func (s *Store) ApplyReactionFact(ctx context.Context, fact domain.ReactionFact, now time.Time) (bool, error) {
	return s.writeReactionFact(ctx, fact, false, now)
}

// HoldReactionFact records a fact whose message has not arrived yet, to be
// released when it does.
//
// A reaction can overtake the message it is about: nothing orders the two and
// they travel by different paths. Dropping it would lose a legitimate reaction
// for good, since the sender has no reason to repeat a fact it believes
// delivered.
//
// This is NOT the path for a fact about a message this node DELETED. That one
// is dropped by the caller, because storing it would rebuild exactly the
// metadata the deletion existed to destroy.
// Reports whether the row was written, on the same terms ApplyReactionFact
// does: a hold can be refused by a ceiling, and a caller that then goes on to
// "release" it would report a change that never happened.
func (s *Store) HoldReactionFact(ctx context.Context, fact domain.ReactionFact, now time.Time) (bool, error) {
	return s.writeReactionFact(ctx, fact, true, now)
}

// writeReactionFact is the one INSERT both entry points go through. Applied and
// held facts differ by a column, not by a table: the two would otherwise have
// been the same statement written twice and kept in step by hand.
//
// `pending` is taken from the caller on every write, including an update. A
// fact that supersedes a held one while the message is still missing stays
// held; one that arrives after the message lands is applied outright, whatever
// the row said before.
//
// # Why the merge admits an EQUAL clock in one direction
//
// The clock comparison is strictly greater, because two different decisions
// never share a counter value and a duplicate must be a no-op. One case breaks
// that symmetry: a fact held at clock N, then re-delivered at the same clock N
// after its message has landed. The decision has not changed, so nothing may
// move — except `pending`, which is not part of the decision at all but of what
// this node knows. Refusing it left the fact hidden until the sweep took it an
// hour later, on a node that by then HAD the message.
//
// So the update also fires for an equal clock when it would take a row OUT of
// pending, and never the other way: an applied fact cannot be pushed back into
// waiting by a repeat.
//
// And on that path it changes NOTHING ELSE. `op` and `scope` are held at their
// stored values, because a peer that sends `set` and `cleared` for one key under
// the same counter is stating two different decisions with one number — which no
// honest actor does — and the outcome must not depend on which of them happened
// to be the one that was waiting. Only the clock's own equality gets it through
// the WHERE; the SET makes sure it buys only the flag.
//
// The ceilings it applies are the two constants above; how they are written is
// the subject of the next comment.

// The ceilings are enforced INSIDE the upsert rather than by a count taken
// before it. A read followed by a write is a check-then-act: two facts for the
// same actor arriving together would both read one short of the limit and both
// insert, which is exactly the case a ceiling against a hostile peer has to
// survive.
//
// # The existing-key escape is load-bearing, not a nicety
//
// `INSERT … SELECT … WHERE <false>` produces ZERO ROWS, and a statement that
// produces no rows never reaches its `ON CONFLICT` branch. So a guard written
// as a bare `WHERE ceiling-not-reached` does not "guard only the insert": it
// silently blocks every SUPERSEDE too, and an actor at the ceiling can no
// longer clear or change the reactions they already hold — including the local
// user, after enough set/clear cycles on one message.
//
// Every clause therefore starts with "this key already exists", which routes an
// update straight past the ceilings to the clock comparison that belongs there.
const reactionKeyExists = `
	EXISTS (SELECT 1 FROM message_reactions
	        WHERE message_id = ? AND actor = ? AND emoji = ?)`

// reactionPerMessageGuard bounds how wide one actor may make one message.
const reactionPerMessageGuard = `
	AND (` + reactionKeyExists + ` OR (SELECT COUNT(*) FROM message_reactions
	     WHERE message_id = ? AND actor = ?) < ?)`

// reactionHeldGuard bounds how many facts one actor may keep WAITING.
//
// Applied only to a write that is itself held. An applied fact adds no held
// row, so gating it on the held count would mute an actor entirely for the
// whole TTL — including on messages this node does have — which a peer returning
// after a long absence reaches legitimately.
const reactionHeldGuard = `
	AND (` + reactionKeyExists + ` OR (SELECT COUNT(*) FROM message_reactions
	     WHERE actor = ? AND pending = 1) < ?)`

// reactionMessageInConversation refuses an APPLIED fact unless its message is
// in the conversation the fact names.
//
// Two properties in one clause, and both were bugs found separately:
//
//   - the message must EXIST. The caller's own check is a check-then-act: the
//     incoming path asks whether the message is present and then writes, and a
//     per-message delete completing between the two erases the reactions first
//     and then has the write put one back — for a message that no longer exists.
//     Nothing reaches that row afterwards (the per-message delete needs a
//     message, the wipe runs by scope and has already run, the sweep only takes
//     held rows) and it is offered to the peer for as long as the process lives.
//   - the message must belong to THAT conversation. `ToggleReaction` is handed a
//     peer and a message id as separate arguments, so a stale UI event — a tap
//     landing after the user switched conversations — could attach a message of
//     conversation A to the scope of conversation B. The row would then be
//     offered to B: the id of a message they never saw, and which emoji was put
//     on it. Scope is also what the ON CONFLICT branch overwrites, so the same
//     mistake could move an existing reaction into another conversation.
//
// Written as a guard on the INSERT so SQLite orders it against the delete's
// transaction: either the fact lands first and the delete takes it with the
// message, or the delete lands first and this produces no row at all.
//
// No existing-key escape here, unlike the ceilings: the point is not to bound a
// count but to refuse a message that is gone or is somebody else's, and neither
// gets better because a row for that key already exists.
//
// A HELD fact is exempt because it is about a message that is legitimately not
// here yet — there is nothing to check membership against. The exemption costs
// nothing: a held row is invisible until its message arrives, and the release
// path re-applies it through this same guard.
const reactionMessageInConversation = `
	AND EXISTS (SELECT 1 FROM messages WHERE id = ? AND `

// conversationClause turns a reaction scope into the WHERE fragment that says
// "this message belongs to that conversation", with its arguments.
//
// A scope is the conversation, and today the only conversation is a pair of
// identities, so the scope IS the peer. A scope that does not parse as one is
// refused rather than waved through: groups (§8) have no membership question
// answerable from `messages` yet, and until they do, a scope that is not a peer
// is a bug in the caller — one whose consequence is a row attributed to a
// conversation nobody can name.
func (s *Store) conversationClause(scope domain.ReactionScope) (string, []any, error) {
	peer := domain.PeerIdentityFromWire(string(scope))
	if peer.IsZero() {
		return "", nil, fmt.Errorf("chatlog: reaction scope %q names no conversation this store can check", scope)
	}
	clause, args := s.peerQuery("dm", peer, "", "")
	return clause, args, nil
}

func (s *Store) writeReactionFact(ctx context.Context, fact domain.ReactionFact, pending bool, now time.Time) (bool, error) {
	if fact.Clock > math.MaxInt64 {
		// SQLite has no unsigned integer, so a clock past MaxInt64 would be
		// stored negative and silently REVERSE the merge for that key. Refused
		// at the door rather than truncated: the clock is the merge's only
		// ordering input, and a lossy conversion of it is not a rounding error.
		return false, fmt.Errorf(
			"chatlog: reaction %s/%s carries clock %d, past what the store can order",
			fact.Key.MessageID, fact.Key.Emoji, fact.Clock)
	}
	id, actor, emoji := string(fact.Key.MessageID), fact.Key.Actor.String(), fact.Key.Emoji
	// Sortable and fixed width, unlike RFC3339Nano, which strips trailing zeros
	// and therefore is NOT order-preserving as a string — and this column is
	// compared as a string by the sweep.
	stamp := now.UTC().Format(sortableStamp)

	guard := reactionPerMessageGuard
	args := []any{
		string(fact.Scope), id, actor, emoji,
		int(fact.Op), int64(fact.Clock), boolToInt(pending), stamp, stamp,
		id, actor, emoji, id, actor, MaxReactionsPerActorPerMessage,
	}
	if pending {
		guard += reactionHeldGuard
		args = append(args, id, actor, emoji, actor, MaxHeldReactionsPerActor)
	} else {
		// A HELD fact is about a message that is legitimately not here yet, so
		// only the applied write asks for one.
		clause, clauseArgs, err := s.conversationClause(fact.Scope)
		if err != nil {
			return false, err
		}
		guard += reactionMessageInConversation + clause + `)`
		args = append(args, id)
		args = append(args, clauseArgs...)
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT INTO message_reactions (scope, message_id, actor, emoji, op, clock, pending, first_seen_at, updated_at)
		SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
		WHERE TRUE`+guard+`
		ON CONFLICT(message_id, actor, emoji) DO UPDATE SET
			-- At an EQUAL clock nothing about the decision may move: only the
			-- pending flag, which is not part of the decision. Writing op from
			-- excluded there would make the outcome of two same-clock facts
			-- depend on which of them happened to be the held one.
			scope      = CASE WHEN excluded.clock > message_reactions.clock
			                  THEN excluded.scope ELSE message_reactions.scope END,
			op         = CASE WHEN excluded.clock > message_reactions.clock
			                  THEN excluded.op ELSE message_reactions.op END,
			clock      = excluded.clock,
			pending    = excluded.pending,
			updated_at = excluded.updated_at
		WHERE excluded.clock > message_reactions.clock
		   OR (excluded.clock = message_reactions.clock
		       AND message_reactions.pending = 1 AND excluded.pending = 0)`, args...)
	if err != nil {
		return false, fmt.Errorf("chatlog: write reaction %s/%s: %w", fact.Key.MessageID, fact.Key.Emoji, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// sortableStamp is the layout of `first_seen_at`: fixed width, so lexical order
// is chronological order. RFC3339Nano is not — it drops trailing zeros, so
// "…:00Z" sorts after "…:00.5Z" — and this column is compared as a string.
const sortableStamp = "2006-01-02T15:04:05.000000000Z07:00"

// HasConversationWith reports whether this node holds any message exchanged
// with the peer.
//
// It is the admission question for a fact about a message we do not have. The
// per-actor ceilings bound rows per identity, and an identity costs nothing to
// mint, so on their own they bound "N × the ceiling" with N chosen by whoever
// is attacking. Asking for an existing conversation makes N the number of
// people the user actually talks to.
func (s *Store) HasConversationWith(ctx context.Context, peer domain.PeerIdentity) (bool, error) {
	query, args := s.peerQuery("dm", peer, `SELECT 1 FROM messages WHERE `, ` LIMIT 1`)
	var found int
	switch err := s.db.QueryRowContext(ctx, query, args...).Scan(&found); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("chatlog: look for a conversation with %s: %w", peer, err)
	}
}

// SweepHeldReactions removes facts that waited past HeldReactionTTL for a
// message that never came, and reports how many.
//
// Together with MaxHeldReactionsPerActor this is what bounds the held rows: the
// ceiling caps how many exist at once, the sweep caps how long they last. The
// cutoff is `first_seen_at` because `updated_at` is refreshable by the sender —
// see HeldReactionTTL.
// What it drops is NOT written down. A fact that waited out the window is a
// fact whose message did not come, and its author keeps offering it, so the
// next offer is held again and swept again — churn this used to end by
// recording the id for good. That record could not be told apart from "the user
// deleted this message here", and it outlived the deletion by design, so it was
// the one durable trace left after a wipe. The churn is invisible (held facts
// draw nothing), bounded by the per-actor ceilings, and it stops the moment the
// sender stops offering — which the reaction transport's own acknowledgement is
// what settles.
//
// One transaction: the delete is all of it.
func (s *Store) SweepHeldReactions(ctx context.Context, now time.Time) (int, error) {
	cutoff := now.UTC().Add(-HeldReactionTTL).Format(sortableStamp)
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM message_reactions
		WHERE pending = 1 AND first_seen_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("chatlog: sweep held reactions: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// ReleaseHeldReactions makes the facts that were waiting for this message IN
// THIS CONVERSATION visible, and reports how many.
//
// The scope is part of the question and not a convenience. Message ids are
// chosen by their sender, so a peer can hold facts on an id it expects a
// DIFFERENT peer to use; releasing by id alone would then make a stranger's
// facts visible the moment the unrelated message landed. The row carries the
// conversation it belongs to, and this is one of the places that has to read it.
//
// No clock comparison is needed here and none is done: the comparison happened
// when each fact was written, against the same row this statement is about to
// clear the flag on. Releasing is only ever "the message is here now".
func (s *Store) ReleaseHeldReactions(
	ctx context.Context,
	scope domain.ReactionScope,
	messageID domain.MessageID,
	now time.Time,
) (int, error) {
	// The same membership condition the applied write is guarded by, and for the
	// same reason: releasing is what MAKES a fact applied, so a release that
	// only matches on scope and id is a second door into the state the guard
	// exists to protect. Two ways in without it — an id that exists in another
	// conversation, and a delete landing between the caller's "the message is
	// here" and this UPDATE — and both leave an applied row for a message this
	// conversation does not have, which no deletion path reaches afterwards.
	clause, clauseArgs, err := s.conversationClause(scope)
	if err != nil {
		return 0, err
	}
	args := append([]any{
		now.UTC().Format(sortableStamp), string(scope), string(messageID), string(messageID),
	}, clauseArgs...)
	res, err := s.db.ExecContext(ctx, `
		UPDATE message_reactions SET pending = 0, updated_at = ?
		WHERE scope = ? AND message_id = ? AND pending = 1
		  AND EXISTS (SELECT 1 FROM messages WHERE id = ? AND `+clause+`)`, args...)
	if err != nil {
		return 0, fmt.Errorf("chatlog: release held reactions %s: %w", messageID, err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ReactionFacts returns every applied fact for one message, tombstones
// included. The wire side needs them all: a cleared reaction is a fact a peer
// may be missing just as much as a set one.
//
// Facts still waiting for their message are left out: they describe a message
// this node cannot show, and handing them on would spread a claim we cannot
// place ourselves.
func (s *Store) ReactionFacts(ctx context.Context, messageID domain.MessageID) ([]domain.ReactionFact, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT scope, actor, emoji, op, clock
		FROM message_reactions WHERE message_id = ? AND pending = 0
		ORDER BY actor, emoji`, string(messageID))
	if err != nil {
		return nil, fmt.Errorf("chatlog: read reactions %s: %w", messageID, err)
	}
	defer func() { _ = rows.Close() }()
	return scanReactionFacts(rows, messageID)
}

func scanReactionFacts(rows *sql.Rows, messageID domain.MessageID) ([]domain.ReactionFact, error) {
	var facts []domain.ReactionFact
	for rows.Next() {
		var scope, actor, emoji string
		var op int
		var clock int64
		if err := rows.Scan(&scope, &actor, &emoji, &op, &clock); err != nil {
			return nil, fmt.Errorf("chatlog: scan reaction %s: %w", messageID, err)
		}
		facts = append(facts, domain.ReactionFact{
			Scope: domain.ReactionScope(scope),
			Key: domain.ReactionKey{
				MessageID: messageID,
				Actor:     domain.PeerIdentityFromWire(actor),
				Emoji:     emoji,
			},
			Op:    domain.ReactionOp(op),
			Clock: domain.ReactionClock(clock),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chatlog: read reactions %s: %w", messageID, err)
	}
	return facts, nil
}

// ReactionsForScope is every message's reactions in one conversation, in one
// query.
//
// The UI needs this shape rather than a per-message call: a chat view draws
// dozens of bubbles per frame, and a database read per bubble per frame is a
// read in the wrong place entirely. The caller loads the conversation once and
// redraws from memory.
func (s *Store) ReactionsForScope(ctx context.Context, scope domain.ReactionScope, self domain.PeerIdentity) (map[domain.MessageID][]domain.Reaction, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, actor, emoji, op, clock
		FROM message_reactions WHERE scope = ? AND pending = 0
		ORDER BY message_id, actor, emoji`, string(scope))
	if err != nil {
		return nil, fmt.Errorf("chatlog: read reactions for %s: %w", scope, err)
	}
	defer func() { _ = rows.Close() }()

	byMessage := map[domain.MessageID][]domain.ReactionFact{}
	for rows.Next() {
		var messageID, actor, emoji string
		var op int
		var clock int64
		if err := rows.Scan(&messageID, &actor, &emoji, &op, &clock); err != nil {
			return nil, fmt.Errorf("chatlog: scan reactions for %s: %w", scope, err)
		}
		id := domain.MessageID(messageID)
		byMessage[id] = append(byMessage[id], domain.ReactionFact{
			Scope: scope,
			Key: domain.ReactionKey{
				MessageID: id,
				Actor:     domain.PeerIdentityFromWire(actor),
				Emoji:     emoji,
			},
			Op:    domain.ReactionOp(op),
			Clock: domain.ReactionClock(clock),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chatlog: read reactions for %s: %w", scope, err)
	}

	folded := make(map[domain.MessageID][]domain.Reaction, len(byMessage))
	for id, facts := range byMessage {
		if reactions := FoldReactions(facts, self); len(reactions) > 0 {
			folded[id] = reactions
		}
	}
	return folded, nil
}

// The three reads the re-offer runs on a timer, once per conversation.
//
// Named constants rather than literals at the call site because their PLAN is
// part of the contract: they run per conversation on every pass, and the test
// that pins them to the index has to ask SQLite about the same text this
// executes. A test carrying its own copy of the SQL passes while the query it
// is supposed to guard drifts away from the index — which is exactly what
// happened to the first version of this index.
const (
	reactionsAuthoredByQuery = `
		SELECT message_id, emoji, op, clock
		FROM message_reactions
		WHERE actor = ? AND scope = ? AND pending = 0
		ORDER BY updated_at DESC, message_id DESC, emoji DESC
		LIMIT ? OFFSET ?`

	countReactionsAuthoredByQuery = `
		SELECT COUNT(*) FROM message_reactions
		WHERE actor = ? AND scope = ? AND pending = 0`

	conversationsWithReactionsByQuery = `
		SELECT DISTINCT scope FROM message_reactions
		WHERE actor = ? AND pending = 0`
)

// ReactionsAuthoredBy is one conversation's facts by ONE actor, newest first,
// capped.
//
// It is what a re-offer reads. Nothing on this transport reports that a fact
// arrived, so the sender cannot know what to skip; it offers the bounded set
// again whenever a session comes up, and the receiver's merge — one clock
// comparison — makes everything it already has free.
//
// Bounded by COUNT and not by age: "until the peer upgrades" has no deadline,
// and a window measured from the tap would have expired long before a peer that
// returns after a month reconnects — with a decision of ours it has never seen,
// which is exactly what this exists to carry.
//
// PAGED, and that is what makes the bound honest rather than a shorter deadline
// in disguise. One page is what fits a frame or two; successive re-offers walk
// the whole set and wrap, so a conversation with more facts than one page holds
// still offers all of them — just across several passes instead of one.
//
// Tombstones are included, because "I took it back" is exactly as much news as
// "I set it", and pending rows are not: a fact still waiting for its own message
// is not one this node can vouch for.
func (s *Store) ReactionsAuthoredBy(
	ctx context.Context,
	actor domain.PeerIdentity,
	scope domain.ReactionScope,
	limit, offset int,
) ([]domain.ReactionFact, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, reactionsAuthoredByQuery,
		actor.String(), string(scope), limit, max(offset, 0))
	if err != nil {
		return nil, fmt.Errorf("chatlog: read reactions authored in %s: %w", scope, err)
	}
	defer func() { _ = rows.Close() }()

	var facts []domain.ReactionFact
	for rows.Next() {
		var messageID, emoji string
		var op int
		var clock int64
		if err := rows.Scan(&messageID, &emoji, &op, &clock); err != nil {
			return nil, fmt.Errorf("chatlog: scan authored reaction: %w", err)
		}
		facts = append(facts, domain.ReactionFact{
			Scope: scope,
			Key: domain.ReactionKey{
				MessageID: domain.MessageID(messageID),
				Actor:     actor,
				Emoji:     emoji,
			},
			Op:    domain.ReactionOp(op),
			Clock: domain.ReactionClock(clock),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chatlog: read reactions authored in %s: %w", scope, err)
	}
	return facts, nil
}

// reactionKeyChunk is how many keys one ReactionFactsByKey statement asks about.
//
// Chunked because the caller's list is bounded by the outbox cap, not by
// SQLite's parameter limit, and a single statement for 512 keys would ask for
// more variables than some builds allow. The chunking is invisible above: the
// rows come back in one slice.
const reactionKeyChunk = 100

// ReactionFactsByKey reads the CURRENT state of the named keys in one
// conversation, and returns nothing for the ones that are no longer there.
//
// It is what lets the send queue hold keys instead of copies of facts. A queue
// of copies has to be told about every deletion — of the message, of the
// reaction, of the whole conversation — and every such notification is a race
// waiting to be found. Reading the state at the moment of sending means a key
// whose row is gone simply resolves to nothing, and a key whose fact has changed
// resolves to what is true now rather than to what was true when it was queued.
//
// Keys whose row is held (pending = 1) are skipped: a fact this node cannot
// vouch for is not one to state to the peer.
func (s *Store) ReactionFactsByKey(
	ctx context.Context,
	scope domain.ReactionScope,
	actor domain.PeerIdentity,
	keys []domain.ReactionKey,
) ([]domain.ReactionFact, error) {
	if len(keys) == 0 || actor.IsZero() {
		return nil, nil
	}
	found := make(map[domain.ReactionKey]domain.ReactionFact, len(keys))
	for start := 0; start < len(keys); start += reactionKeyChunk {
		chunk := keys[start:min(start+reactionKeyChunk, len(keys))]
		args := []any{string(scope), actor.String()}
		placeholders := make([]string, 0, len(chunk))
		for _, key := range chunk {
			placeholders = append(placeholders, "(message_id = ? AND emoji = ?)")
			args = append(args, string(key.MessageID), key.Emoji)
		}
		rows, err := s.db.QueryContext(ctx, `
			SELECT message_id, emoji, op, clock
			FROM message_reactions
			WHERE scope = ? AND actor = ? AND pending = 0
			  AND (`+strings.Join(placeholders, " OR ")+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("chatlog: read reactions by key in %s: %w", scope, err)
		}
		if err := collectReactionFactsByKey(rows, scope, actor, found); err != nil {
			return nil, err
		}
	}

	// Returned in the order asked for: the caller queued them least-recently-
	// decided first, and that order is what its cap drops from.
	facts := make([]domain.ReactionFact, 0, len(found))
	for _, key := range keys {
		if fact, ok := found[domain.ReactionKey{MessageID: key.MessageID, Actor: actor, Emoji: key.Emoji}]; ok {
			facts = append(facts, fact)
		}
	}
	return facts, nil
}

// collectReactionFactsByKey collects one chunk's rows into found.
func collectReactionFactsByKey(
	rows *sql.Rows,
	scope domain.ReactionScope,
	actor domain.PeerIdentity,
	found map[domain.ReactionKey]domain.ReactionFact,
) error {
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, emoji string
		var op int
		var clock int64
		if err := rows.Scan(&id, &emoji, &op, &clock); err != nil {
			return fmt.Errorf("chatlog: scan reaction by key: %w", err)
		}
		key := domain.ReactionKey{MessageID: domain.MessageID(id), Actor: actor, Emoji: emoji}
		found[key] = domain.ReactionFact{
			Scope: scope,
			Key:   key,
			Op:    domain.ReactionOp(op),
			Clock: domain.ReactionClock(clock),
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("chatlog: read reactions by key: %w", err)
	}
	return nil
}

// CountReactionsAuthoredBy is how many facts one actor has in one conversation,
// so a pager knows where to wrap.
func (s *Store) CountReactionsAuthoredBy(
	ctx context.Context,
	actor domain.PeerIdentity,
	scope domain.ReactionScope,
) (int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, countReactionsAuthoredByQuery,
		actor.String(), string(scope)).Scan(&total); err != nil {
		return 0, fmt.Errorf("chatlog: count reactions authored in %s: %w", scope, err)
	}
	return total, nil
}

// ConversationsWithReactionsBy lists the conversations this actor has facts in.
//
// It drives the periodic re-offer, which is what reaches a peer this node has no
// SESSION with: a reaction can travel three hops, and the two nodes at the ends
// of that path may never be neighbours, so "re-offer when a session comes up"
// alone would never retry for them.
func (s *Store) ConversationsWithReactionsBy(
	ctx context.Context,
	actor domain.PeerIdentity,
) ([]domain.ReactionScope, error) {
	rows, err := s.db.QueryContext(ctx, conversationsWithReactionsByQuery, actor.String())
	if err != nil {
		return nil, fmt.Errorf("chatlog: list conversations with our reactions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var scopes []domain.ReactionScope
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			return nil, fmt.Errorf("chatlog: scan conversation scope: %w", err)
		}
		scopes = append(scopes, domain.ReactionScope(scope))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("chatlog: list conversations with our reactions: %w", err)
	}
	return scopes, nil
}

// DeleteOrphanReactions erases the reactions of one conversation that no longer
// have a message in it — the ones still WAITING for a message this node never
// received, and any left by a message that is already gone.
//
// It exists because a reaction is not reachable from the message row in every
// case that matters: a held fact has no message row to be deleted through, so a
// wipe that erases the thread message by message leaves it behind, and if the
// message it names ever arrives the repair pass (ReleaseArrivedReactions) makes
// it visible in a conversation the user erased.
//
// What it deliberately does NOT touch is a reaction whose message SURVIVED the
// wipe. An immutable message is kept on purpose — the flag is a promise no bulk
// gesture overrides — and erasing the reactions of a message that is still on
// screen would not stick: the peer re-offers their fact, the message is there,
// the guard admits it, and the chip comes back. The alternative, refusing it
// forever, would leave a visible message with a permanently wrong count. A
// message that survives keeps its metadata.
func (s *Store) DeleteOrphanReactions(ctx context.Context, scope domain.ReactionScope) (int, error) {
	return deleteOrphanReactionsTx(ctx, s.db, s.identityAddr, scope)
}

// deleteOrphanReactionsTx is the same statement inside somebody else's
// transaction, so a wipe destroys the thread and its orphaned reactions as one
// fact. Run it AFTER the messages are deleted: "orphaned" is decided against the
// rows that are left.
func deleteOrphanReactionsTx(ctx context.Context, db execContext, self domain.PeerIdentity, scope domain.ReactionScope) (int, error) {
	if domain.PeerIdentityFromWire(string(scope)).IsZero() {
		return 0, fmt.Errorf("chatlog: reaction scope %q names no conversation this store can check", scope)
	}
	res, err := db.ExecContext(ctx, `
		DELETE FROM message_reactions
		WHERE scope = ?
		  AND NOT EXISTS (
			SELECT 1 FROM messages m
			WHERE m.id = message_reactions.message_id AND m.topic = 'dm'
			  AND ((m.sender = ? AND m.recipient = message_reactions.scope)
			    OR (m.sender = message_reactions.scope AND m.recipient = ?))
		  )`, string(scope), self.String(), self.String())
	if err != nil {
		return 0, fmt.Errorf("chatlog: delete orphaned reactions of %s: %w", scope, err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ReleaseArrivedReactions makes visible every held fact whose message is here
// after all, and reports the conversations that changed.
//
// It is the local self-heal for the one gap the per-message release cannot
// cover: that release is best-effort — its error is logged and the message is
// still stored — so a fact can be left pending on a node that HAS the message,
// and nothing else would come back for it. The sender will not repeat a fact it
// believes delivered, and even if it does, the receiver may never see another
// copy of the MESSAGE to trigger the per-message path.
//
// Scoped in the join, not just by id: a held fact belongs to one conversation,
// and an unrelated message that happens to share an id is not the one it was
// waiting for.
// The scopes are what the caller needs: a released fact is one the UI is not
// drawing, and the chips come from a per-conversation cache that only a
// TopicReactionsChanged for THAT conversation reloads. A count alone would leave
// the reaction invisible until the user switched chats.
func (s *Store) ReleaseArrivedReactions(ctx context.Context, now time.Time) ([]domain.ReactionScope, error) {
	const arrived = `
		pending = 1 AND EXISTS (
			SELECT 1 FROM messages m
			WHERE m.id = message_reactions.message_id AND m.topic = 'dm'
			  AND ((m.sender = ? AND m.recipient = message_reactions.scope)
			    OR (m.sender = message_reactions.scope AND m.recipient = ?))
		)`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("chatlog: begin release of arrived reactions: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Read the scopes first and update in the SAME transaction, so the answer
	// names exactly the rows the update touches.
	rows, err := tx.QueryContext(ctx,
		`SELECT DISTINCT scope FROM message_reactions WHERE `+arrived,
		s.identityAddr, s.identityAddr)
	if err != nil {
		return nil, fmt.Errorf("chatlog: find arrived reactions: %w", err)
	}
	var scopes []domain.ReactionScope
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("chatlog: scan arrived reaction scope: %w", err)
		}
		scopes = append(scopes, domain.ReactionScope(scope))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("chatlog: find arrived reactions: %w", err)
	}
	_ = rows.Close()
	if len(scopes) == 0 {
		return nil, nil
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE message_reactions SET pending = 0, updated_at = ? WHERE `+arrived,
		now.UTC().Format(sortableStamp), s.identityAddr, s.identityAddr); err != nil {
		return nil, fmt.Errorf("chatlog: release arrived reactions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("chatlog: commit release of arrived reactions: %w", err)
	}
	return scopes, nil
}

// FoldReactions turns facts into what the UI draws. Split out of Reactions so
// the rule can be exercised without a database, and so the wire side can fold
// a set it has just received without writing it first.
func FoldReactions(facts []domain.ReactionFact, self domain.PeerIdentity) []domain.Reaction {
	order := make([]string, 0, len(facts))
	byEmoji := make(map[string]*domain.Reaction, len(facts))
	for _, fact := range facts {
		if fact.Op != domain.ReactionSet {
			continue
		}
		reaction := byEmoji[fact.Key.Emoji]
		if reaction == nil {
			order = append(order, fact.Key.Emoji)
			reaction = &domain.Reaction{Emoji: fact.Key.Emoji}
			byEmoji[fact.Key.Emoji] = reaction
		}
		reaction.Actors = append(reaction.Actors, fact.Key.Actor)
		reaction.Mine = reaction.Mine || fact.Key.Actor == self
	}
	out := make([]domain.Reaction, 0, len(order))
	for _, emoji := range order {
		out = append(out, *byEmoji[emoji])
	}
	return out
}

// NextReactionClock is the counter value the local user's next decision gets.
//
// Derived from what is stored rather than kept in a counter of its own: a
// separate counter is a second source of truth that a restore-from-backup or a
// half-applied transaction can put behind the facts it is supposed to order.
//
// What this does NOT promise is that the value never repeats. Deleting the
// message that carried the highest-clock fact takes that row with it, and the
// next decision reuses the value. That is harmless as long as clocks are only
// ever compared per key — an actor cannot hold two facts about one key — and it
// is the reason the comparison is written per key everywhere. A mechanism that
// compares an actor's clocks ACROSS keys, such as a watermark, cannot be built
// on this function as it stands.
func (s *Store) NextReactionClock(ctx context.Context, self domain.PeerIdentity) (domain.ReactionClock, error) {
	var highest sql.NullInt64
	if err := s.db.QueryRowContext(ctx,
		`SELECT MAX(clock) FROM message_reactions WHERE actor = ?`, self.String()).Scan(&highest); err != nil {
		return 0, fmt.Errorf("chatlog: next reaction clock: %w", err)
	}
	if !highest.Valid || highest.Int64 < 0 {
		return 1, nil
	}
	return domain.ReactionClock(highest.Int64) + 1, nil
}
