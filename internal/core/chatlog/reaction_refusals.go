package chatlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// reaction_refusals.go answers one question durably: is this an id whose
// reactions this node will not store?
//
// It says "the message is not here and did not come", which is not the same as
// "it was deleted" — and the wider statement is the one that can be made
// reliably. The deletion tombstone (message_delete_intents.refuse_until) is
// sized by the sender's reseed horizon: past a week no copy of the MESSAGE is
// re-sent, so the refusal has nothing left to refuse. A reaction has no
// horizon — its author re-offers the facts it holds for as long as it holds
// them, because nothing on that transport reports arrival. So the two ran out
// of step: the tombstone expired, the next offer was taken as a fact waiting
// for its message, the hourly sweep dropped it, and the offer after that put it
// back, for ever.
//
// Three writers, in order of how much they know:
//
//   - the delete of a message that HAD reactions, in the same transaction, so
//     the loop never starts for the case we can see coming;
//   - an offer refused while the tombstone is still alive — a reaction made
//     after the deletion, which nothing at delete time could have noticed;
//   - the sweep of a fact that waited out HeldReactionTTL. This is the one that
//     needs no foresight and closes the rest: whatever the reason a message did
//     not come, a fact that waited an hour for it is not going to be applied by
//     the next offer either.
//
// And one eraser: storing a message with that id. A message that arrives late
// — a re-delivery, a reseed days afterwards — makes the refusal wrong, and the
// reaction its author is still offering must apply. That is what keeps "did not
// come" from silently meaning "never will".

// MaxReactionRefusals is how many ids the table keeps. It bounds the one
// structure here that has no TTL, and the trim drops the least recently touched
// first — an id somebody still offers reactions for is touched on every offer,
// so what falls out is what nobody has mentioned in a long time.
//
// Evicting is not free: an evicted id is one whose next offer is held again for
// an hour before the sweep records it anew. The number is therefore generous
// rather than tight — the rows are two short strings.
const MaxReactionRefusals = 4096

// refusalTouchFloor is the least time between two writes of the same id's
// `refused_at`.
//
// The trim orders by last touch, so a refusal that is being offered at must
// move; without that, an id under constant pressure ages exactly like one
// nobody has mentioned since it was deleted and can be trimmed while the offers
// are still arriving. But the touch is on the arrival path of every refused
// fact — a batch of 64 would be 64 writes — and the trim measures in days, so
// an hour of resolution costs it nothing.
const refusalTouchFloor = time.Hour

// RefuseReactionsFor records that reactions naming this message id in this
// conversation are not to be stored, and moves the id to the front of the trim
// order when it is already recorded.
func (s *Store) RefuseReactionsFor(
	ctx context.Context,
	scope domain.ReactionScope,
	messageID domain.MessageID,
	now time.Time,
) error {
	return refuseReactionsForTx(ctx, s.db, s.identityAddr,
		[]scopedID{{Scope: scope, MessageID: messageID}}, now)
}

// scopedID is a refusal's key: the conversation and the message in it. Message
// ids are chosen by their sender, so the conversation is part of the question
// and not decoration — the same id can name a different message to a different
// peer.
type scopedID struct {
	Scope     domain.ReactionScope
	MessageID domain.MessageID
}

// refuseReactionsForTx is RefuseReactionsFor against any executor, so the
// transaction that deletes a message — or the one that sweeps a fact which
// waited for it — can record the refusal in the same commit: a deletion that
// committed without it is one the sender can undo, one offer at a time.
//
// The write is guarded by the message not being IN THAT CONVERSATION, and the
// guard is what makes the sweep safe to use as a writer: a message landing
// between the sweep's read and its commit would otherwise be refused with its
// own row present, a reaction that could never appear on a message the user can
// see. Membership and not mere existence, because message ids are chosen by
// their sender: a peer can put a message in ITS conversation under an id this
// node deleted in another, and a guard that only asked "does this id exist"
// would then refuse to record anything for the deleted one, ever.
func refuseReactionsForTx(
	ctx context.Context,
	db execContext,
	self domain.PeerIdentity,
	keys []scopedID,
	now time.Time,
) error {
	stamp := now.UTC().Format(sortableStamp)
	for _, key := range keys {
		peer, err := scopePeer(key.Scope)
		if err != nil {
			return err
		}
		clause, args := dmPairClause(self, peer)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO reaction_refusals (scope, message_id, refused_at)
			SELECT ?, ?, ?
			WHERE NOT EXISTS (SELECT 1 FROM messages WHERE id = ? AND `+clause+`)
			ON CONFLICT(scope, message_id) DO UPDATE SET refused_at = excluded.refused_at`,
			append([]any{string(key.Scope), string(key.MessageID), stamp,
				string(key.MessageID)}, args...)...); err != nil {
			return fmt.Errorf("chatlog: refuse reactions for %s in %s: %w",
				key.MessageID, key.Scope, err)
		}
	}
	return nil
}

// dmScopeOf names the conversation an arriving entry belongs to, and reports
// false when there is none to name.
//
// Only DMs have one today: a reaction's scope is a peer (§8 leaves group ids for
// later), so a broadcast row has no conversation whose refusals its arrival
// could lift.
func dmScopeOf(topic string, self domain.PeerIdentity, entry Entry) (domain.ReactionScope, bool) {
	if topic != "dm" {
		return "", false
	}
	peer := domain.PeerIdentityFromWire(entry.Sender)
	if peer == self {
		peer = domain.PeerIdentityFromWire(entry.Recipient)
	}
	if peer.IsZero() || peer == self {
		return "", false
	}
	return domain.ReactionScopeForPeer(peer), true
}

// scopePeer reads the conversation a scope names.
//
// Refused rather than waved through when it names none, for the reason
// conversationClause gives: groups (§8) have no membership question answerable
// from `messages` yet, and until they do, a scope that is not a peer is a bug in
// the caller.
func scopePeer(scope domain.ReactionScope) (domain.PeerIdentity, error) {
	peer := domain.PeerIdentityFromWire(string(scope))
	if peer.IsZero() {
		return domain.PeerIdentity{}, fmt.Errorf(
			"chatlog: reaction scope %q names no conversation this store can check", scope)
	}
	return peer, nil
}

// dmPairClause is "this message belongs to that conversation", for the
// transaction-level helpers that have no Store to ask peerQuery.
func dmPairClause(self, peer domain.PeerIdentity) (string, []any) {
	return `topic = 'dm' AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))`,
		[]any{self.String(), peer.String(), peer.String(), self.String()}
}

// reactionScopesOfTx names the conversations that hold a fact about this id.
//
// The fact carries the conversation it belongs to, which is the only place to
// read it once the message row is gone — and for a fact that is still WAITING
// for its message, the only place there has ever been.
func reactionScopesOfTx(
	ctx context.Context,
	db readWriteContext,
	messageID domain.MessageID,
) ([]domain.ReactionScope, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT scope FROM message_reactions WHERE message_id = ?`, string(messageID))
	if err != nil {
		return nil, fmt.Errorf("chatlog: read the conversations of %s reactions: %w", messageID, err)
	}
	var scopes []domain.ReactionScope
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("chatlog: scan the conversation of a reaction: %w", err)
		}
		scopes = append(scopes, domain.ReactionScope(scope))
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("chatlog: read the conversations of %s reactions: %w", messageID, err)
	}
	return scopes, nil
}

// WipeEmptyConversationReactions is the whole wipe of a conversation with no
// message to delete: the facts still WAITING for messages that never arrived,
// and the refusals of everything removed from it one message at a time before
// today. It reports how many facts went and whether the refusals were forgotten.
//
// That thread never opens the deleting transaction — there is nothing for it to
// delete — so this is the only path that reaches either, and they are ONE
// transaction because the caller reports "nothing was wiped" on an error. Two
// commits would let it say that over rows that are already gone, and then skip
// the event that redraws the chips and the conversation state it clears.
//
// Whether the refusals go is the store's decision, not the caller's: a thread of
// immutable messages also has no candidates, and there they must stay.
func (s *Store) WipeEmptyConversationReactions(
	ctx context.Context,
	peer domain.PeerIdentity,
) (dropped int, forgotten bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("chatlog: begin wiping the reactions of %s: %w", peer, err)
	}
	defer func() { _ = tx.Rollback() }()

	dropped, err = deleteOrphanReactionsTx(ctx, tx, s.identityAddr, domain.ReactionScopeForPeer(peer))
	if err != nil {
		return 0, false, err
	}
	forgotten, err = forgetRefusalsIfConversationGoneTx(ctx, tx, s.identityAddr, peer)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("chatlog: commit wiping the reactions of %s: %w", peer, err)
	}
	return dropped, forgotten, nil
}

// DropReactionRefusalsForScope forgets a whole conversation's refusals,
// unconditionally. For the contact-removal path, which takes every message of
// the peer — immutable ones included — so nothing can be left to keep the
// conversation admitting offers.
func (s *Store) DropReactionRefusalsForScope(ctx context.Context, scope domain.ReactionScope) error {
	return dropReactionRefusalsForScopeTx(ctx, s.db, scope)
}

// forgetRefusalsIfConversationGoneTx is the same decision inside a transaction
// that has just done its own deleting.
//
// The condition is the whole of it. With no message left, an offer from this
// peer is refused at the door by the admission check, so the rows would be a
// bounded table spent on ids nothing will ever ask about. But a wipe KEEPS
// immutable messages, and one survivor is enough to keep the conversation
// admitting offers — and then every refusal dropped here is an hour of held
// rows waiting to happen, once per id, as soon as the tombstones go.
func forgetRefusalsIfConversationGoneTx(
	ctx context.Context,
	db interface {
		execContext
		queryContext
	},
	self, peer domain.PeerIdentity,
) (bool, error) {
	empty, err := conversationIsEmptyTx(ctx, db, self, peer)
	if err != nil || !empty {
		return false, err
	}
	if err := dropReactionRefusalsForScopeTx(
		ctx, db, domain.ReactionScopeForPeer(peer)); err != nil {
		return false, err
	}
	return true, nil
}

// conversationIsEmptyTx reports whether no message with this peer is left.
//
// The same question HasConversationWith answers on the arrival path, asked
// inside the transaction that has just done the deleting — which is what makes
// "the conversation is gone" a fact rather than an expectation: this wipe keeps
// immutable messages, and a survivor keeps the conversation admitting offers.
func conversationIsEmptyTx(
	ctx context.Context,
	db queryContext,
	self, peer domain.PeerIdentity,
) (bool, error) {
	var found int
	switch err := db.QueryRowContext(ctx, `
		SELECT 1 FROM messages
		WHERE topic = 'dm'
		  AND ((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?))
		LIMIT 1`,
		self.String(), peer.String(), peer.String(), self.String()).Scan(&found); {
	case errors.Is(err, sql.ErrNoRows):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("chatlog: look for what is left of %s: %w", peer, err)
	default:
		return false, nil
	}
}

// ReactionsRefusedFor reports whether this id is one whose reactions are
// refused, and keeps the trim order honest by touching what it finds.
//
// Asked only when the message is NOT here: a message that exists answers the
// question by existing, and putting this read in front of every arriving fact
// would pay for the rare case on the common path.
func (s *Store) ReactionsRefusedFor(
	ctx context.Context,
	scope domain.ReactionScope,
	messageID domain.MessageID,
	now time.Time,
) (bool, error) {
	var touched string
	switch err := s.db.QueryRowContext(ctx,
		`SELECT refused_at FROM reaction_refusals WHERE scope = ? AND message_id = ?`,
		string(scope), string(messageID)).Scan(&touched); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("chatlog: look up the refusal of %s: %w", messageID, err)
	}

	if last, err := time.Parse(sortableStamp, touched); err == nil &&
		now.UTC().Sub(last) < refusalTouchFloor {
		return true, nil
	}
	// An unparseable stamp is touched rather than trusted: the row is what
	// matters, and a stamp nothing can read would freeze this id at the front
	// or the back of the trim order for good.
	if _, err := s.db.ExecContext(ctx,
		`UPDATE reaction_refusals SET refused_at = ? WHERE scope = ? AND message_id = ?`,
		now.UTC().Format(sortableStamp), string(scope), string(messageID)); err != nil {
		// The answer stands; what a failure costs is this id's place in the
		// trim order, which the next offer will try to correct again.
		return true, fmt.Errorf("chatlog: touch the refusal of %s: %w", messageID, err)
	}
	return true, nil
}

// dropReactionRefusalTx lifts the refusal of one id in ONE conversation,
// because its message has just been stored there.
//
// Scoped exactly like the write it undoes. Lifting id-wide was the first cut and
// it was wrong: two conversations can hold the same id, and a message arriving
// in one would then lift the other's refusal — which the sweep cannot record
// again, because the id now exists. The result was the loop this table exists to
// end, with no way back.
func dropReactionRefusalTx(
	ctx context.Context,
	db execContext,
	scope domain.ReactionScope,
	messageID domain.MessageID,
) error {
	if _, err := db.ExecContext(ctx,
		`DELETE FROM reaction_refusals WHERE scope = ? AND message_id = ?`,
		string(scope), string(messageID)); err != nil {
		return fmt.Errorf("chatlog: lift the refusal of %s in %s: %w", messageID, scope, err)
	}
	return nil
}

// dropReactionRefusalsForScopeTx forgets a whole conversation's refusals.
//
// By SCOPE and not by a list of ids, and that is the point rather than a
// convenience: the ids worth forgetting include everything deleted from this
// conversation EARLIER, which no query over `messages` can name any more. The
// scope column exists for this statement.
func dropReactionRefusalsForScopeTx(ctx context.Context, db execContext, scope domain.ReactionScope) error {
	if _, err := db.ExecContext(ctx,
		`DELETE FROM reaction_refusals WHERE scope = ?`, string(scope)); err != nil {
		return fmt.Errorf("chatlog: forget the refusals of %s: %w", scope, err)
	}
	return nil
}

// TrimReactionRefusals keeps the `keep` most recently touched ids and reports
// how many went.
func (s *Store) TrimReactionRefusals(ctx context.Context, keep int) (int, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM reaction_refusals
		WHERE rowid IN (
			SELECT rowid FROM reaction_refusals
			ORDER BY refused_at DESC, scope DESC, message_id DESC
			LIMIT -1 OFFSET ?)`, max(keep, 0))
	if err != nil {
		return 0, fmt.Errorf("chatlog: trim reaction refusals: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
