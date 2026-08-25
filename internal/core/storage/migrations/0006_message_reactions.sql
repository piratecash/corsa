-- Reactions on messages. Owned by internal/core/chatlog; the model these rows
-- serve is in docs/refactoring/reactions-protocol.md.
--
-- One row is one FACT: "this actor set (or cleared) this emoji on this message,
-- and that decision is their Nth". The count under a message is a fold over the
-- rows, never a number in a column — a stored count cannot say who reacted, and
-- in a group that is the only thing worth knowing.
--
-- The key has exactly one writer: an actor is the only party that may state
-- their own reaction, which the signed envelope enforces on arrival. So there
-- are never two concurrent writes to one key, merging is
-- `if incoming.clock > local.clock`, and none of the usual conflict machinery
-- is needed.
--
-- `op = 0` is a TOMBSTONE, not an absence, and the difference is the whole
-- reason the column exists. Deleting the row on "cleared" would leave a delayed
-- or duplicated "set" with nothing to compare its clock against, and the
-- reaction would come back. The transport delivers zero or more times, in no
-- order, and a peer returning from a long absence hands back facts older than
-- any freshness window.
--
-- `pending = 1` marks a fact that arrived BEFORE the message it talks about.
-- Nothing orders the two and they travel by different paths, so a reaction can
-- overtake its target; dropping it would lose it for good, since the sender has
-- no reason to repeat a fact it believes delivered. It waits here and is
-- released when the message lands.
--
-- Pending is a column and not a second table because the two would have had the
-- same columns and the same key, and a row can never be in both states at once:
-- deleting a message takes its facts with it, so nothing can be applied and
-- awaiting the same message simultaneously. Two tables would only have been two
-- copies of one INSERT, kept in step by hand.
--
-- Note what pending is NOT: a fact about a message this node DELETED. That one
-- is dropped, because storing it would rebuild the metadata the deletion
-- existed to destroy. The two cases are told apart by the deletion refusal that
-- already exists, not by this column.
--
-- `clock` is the actor's own monotonic counter, not a wall clock: it is
-- compared, never displayed, and two devices' clocks agreeing is not something
-- this design is willing to assume.
--
-- `scope` is the conversation and is deliberately redundant with what could be
-- joined from `messages`: reconciliation runs per conversation, and by then the
-- message the fact talks about may already be gone from this node. It is also
-- the only handle a conversation wipe has on the facts still WAITING for their
-- message, which no join through `messages` can reach.
--
-- `first_seen_at` and `updated_at` answer two different questions, and the
-- difference is a security property rather than bookkeeping. `updated_at` moves
-- on every accepted write. `first_seen_at` never moves. The sweep that bounds
-- held rows measures against `first_seen_at`, because the sender chooses when
-- to re-send a fact, and a TTL measured from the last write is one a peer can
-- refresh forever by re-stating the same fact with a higher clock.
CREATE TABLE IF NOT EXISTS message_reactions (
    scope         TEXT    NOT NULL,
    message_id    TEXT    NOT NULL,
    actor         TEXT    NOT NULL,
    emoji         TEXT    NOT NULL,
    op            INTEGER NOT NULL,
    clock         INTEGER NOT NULL,
    pending       INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL,
    PRIMARY KEY (message_id, actor, emoji)
);

-- Reading a conversation's reactions, the digest folded from them, and the
-- conversation wipe.
CREATE INDEX IF NOT EXISTS idx_message_reactions_scope
    ON message_reactions(scope, message_id);

-- The sweep of facts that waited for a message that never came, and the
-- per-actor ceiling that bounds how many of them one peer may keep waiting.
CREATE INDEX IF NOT EXISTS idx_message_reactions_pending
    ON message_reactions(pending, actor, first_seen_at);

-- The local user's next counter value (chatlog.NextReactionClock), read on
-- every tap. The primary key leads with message_id, so without this the query
-- is a full scan of a table this design expects to grow and rarely shrink.
CREATE INDEX IF NOT EXISTS idx_message_reactions_actor_clock
    ON message_reactions(actor, clock);

-- A message id whose reactions this node refuses: the message is not here and
-- did not come.
--
-- The deletion tombstone (`message_delete_intents.refuse_until`) answers a
-- narrower version of the same question, and its week is sized by the sender's
-- reseed horizon: past it nobody re-sends the MESSAGE, so a refusal has nothing
-- left to refuse. Reaction offers have no such horizon — a peer re-offers the
-- facts it holds for as long as it holds them — so the two run out of step.
-- Once the tombstone went, the next offer was accepted as a pending fact, the
-- hourly sweep took it an hour later, and the offer after that put it back: a
-- row naming a message the user destroyed, re-created for as long as both nodes
-- run.
--
-- Three writers, by how much they know: the delete of a message that HAD
-- reactions (same transaction), an offer refused while the tombstone is still
-- alive, and — the one that needs no foresight — the sweep of a fact that
-- waited out its window. Not every deletion: messages also expire on their own
-- timer, and a row per deleted id would be a second copy of the message table.
-- One eraser: storing a message with that id, because a re-delivery or a reseed
-- days later makes the refusal wrong and its author is still offering the fact.
--
-- No expiry column: an id that expires is the loop coming back. The table is
-- bounded by count instead (chatlog.MaxReactionRefusals), oldest touch first.
-- `scope` is the conversation, and it is what makes the row findable when the
-- message it names is long gone: a wipe erases a conversation, not a list of
-- ids, and the ids of everything deleted from it earlier are exactly the ones
-- no query over `messages` can reach any more.
CREATE TABLE IF NOT EXISTS reaction_refusals (
    scope      TEXT NOT NULL,
    message_id TEXT NOT NULL,
    refused_at TEXT NOT NULL,
    PRIMARY KEY (scope, message_id)
);

-- The trim that bounds the table. `refused_at` moves on every refused offer, so
-- what it orders is last touch, not birth: an id somebody is still pushing at
-- outlives one nobody has mentioned since it was deleted, which is the opposite
-- of what an insertion-ordered trim would keep.
CREATE INDEX IF NOT EXISTS idx_reaction_refusals_touch
    ON reaction_refusals(refused_at);

-- Forgetting a whole conversation's refusals, and the lookup by id alone that
-- lifts them when the message finally arrives.
CREATE INDEX IF NOT EXISTS idx_reaction_refusals_message
    ON reaction_refusals(message_id);

-- The re-offer reads: how many of this user's facts a conversation holds, the
-- page of them the next offer carries, and the list of conversations to walk.
-- All three run on a timer, once per conversation, and filter on
-- (actor, scope, pending); the page orders by updated_at.
--
-- Without this index every pass scans every fact the user ever stated and sorts
-- the result in a temporary B-tree. The tiebreak columns are what removes the
-- sort entirely: paging needs a TOTAL order, and rowid — the obvious tiebreak —
-- cannot go in an index definition, while (message_id, emoji) can and together
-- with actor are this table's primary key.
CREATE INDEX IF NOT EXISTS idx_message_reactions_reoffer_page
    ON message_reactions(actor, scope, pending, updated_at DESC, message_id DESC, emoji DESC);
