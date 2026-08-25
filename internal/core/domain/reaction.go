package domain

// reaction.go is the state model of a message reaction, and it is smaller than
// the feature sounds because of one property: every key has exactly one writer.
//
// A reaction is the fact "this actor set (or cleared) this emoji on this
// message". Only the actor may state their own, so no two parties ever write
// the same key, and the actor's own counter totally orders their decisions.
// Merging is therefore a single comparison rather than a conflict resolution,
// and the same fact arriving twice, late, or out of order changes nothing.
//
// The wire and storage sides both derive from this; see
// docs/refactoring/reactions-protocol.md.

// ReactionOp is what a fact says about one emoji.
type ReactionOp uint8

const (
	// ReactionCleared is a TOMBSTONE, not an absence. It records that the actor
	// took the reaction back at a known point in their own order, which is what
	// stops a delayed or duplicated ReactionSet from putting it back.
	ReactionCleared ReactionOp = 0
	// ReactionSet is the reaction standing.
	ReactionSet ReactionOp = 1
)

// ReactionClock is one actor's monotonic counter over their own reaction
// decisions. It is compared, never displayed, and never mixed with a wall
// clock: two devices agreeing on the time is not something this design assumes.
//
// The counter is per ACTOR and spans conversations. That is deliberate — a
// counter per conversation would have to be created and persisted per peer for
// no gain, since facts are only ever compared against facts about the same key.
type ReactionClock uint64

// ReactionScope identifies the conversation a fact belongs to: the peer today,
// a group id once groups exist. It travels with the fact because
// reconciliation runs per conversation, and by then the message the fact talks
// about may already be gone from this node.
type ReactionScope string

// ReactionScopeForPeer is the conversation id of a one-to-one chat.
//
// Derived rather than stored so there is one rule for it. When groups arrive
// their scope is the group id and this function simply is not the one used —
// nothing downstream cares which of the two produced the value.
func ReactionScopeForPeer(peer PeerIdentity) ReactionScope {
	return ReactionScope(peer.String())
}

// ReactionKey is the addressed unit of state. Its one writer is Actor.
type ReactionKey struct {
	MessageID MessageID
	Actor     PeerIdentity
	Emoji     string
}

// ReactionFact is one statement by one actor about one emoji on one message.
//
// Which of two facts about one key wins is NOT decided here. It is one
// comparison — the higher Clock, with equal clocks a no-op so a duplicate
// changes nothing — and it lives in the single UPSERT that performs the merge
// (chatlog.writeReactionFact). A second copy of the rule in this package could
// only ever disagree with the one that is actually enforced.
type ReactionFact struct {
	Scope ReactionScope
	Key   ReactionKey
	Op    ReactionOp
	Clock ReactionClock
}

// Reaction is the aggregate one message shows: an emoji, who is behind it, and
// whether the local user is one of them.
//
// The count is a fold over actors and is never stored: a stored number cannot
// say who reacted, which in a group is the only thing worth knowing, and it
// cannot be merged when two nodes disagree.
type Reaction struct {
	Emoji  string
	Actors []PeerIdentity
	Mine   bool
}

// Count is how many actors hold this emoji.
func (r Reaction) Count() int { return len(r.Actors) }
