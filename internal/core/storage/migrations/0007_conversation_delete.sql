-- Clearing a chat, and the traces the old deletion left behind.
--
-- One step, because it is one change: the thread wipe became a request about
-- the CONVERSATION instead of a request per message, and everything the
-- per-message model had written down stopped being something this node is
-- allowed to keep. The runner puts the whole file in one transaction, so a
-- database either has all of this or none of it.
--
-- ---------------------------------------------------------------------------
-- 1. The deletion policy of every direct message written before there was one.
--
-- The flag on a row is the answer its holder gives when the OTHER side asks for
-- the message to be removed, and it is never rewritten at runtime.
-- `any-delete` shipped in the same release as the deletion feature and with no
-- backfill, so every message sent before that release still carries the old
-- author-only answer. That is why deleting a peer's message came back as "the
-- peer refused", and why clearing a thread left the requester's own half
-- standing on the other side.
--
-- The old value was never a choice anybody made — the flag has no interface and
-- the sender stamped whatever its build's default was — so the histories are
-- brought to the policy the product actually promises. `immutable` is a real
-- refusal and survives; `auto-delete-ttl` is an expiry contract, not a deletion
-- policy; anything outside a direct conversation has no second participant to
-- grant this to.
UPDATE messages SET flag = 'any-delete'
WHERE topic = 'dm' AND flag IN ('', 'sender-delete');

-- ---------------------------------------------------------------------------
-- 2. One request may now stand for a whole conversation.
--
--   * kind — what the row asks for. 'message' is the existing per-id request;
--     'conversation' asks the peer to erase everything of that thread they
--     still hold. Explicit rather than inferred from a NULL id: a scheduler
--     that has to guess what a row means is one bad WHERE clause away from
--     asking the wrong thing.
--   * message_id — NULL for a conversation request. There is no id to carry:
--     naming them would be how the peer learns of messages that never reached
--     them, and the whole point is that the request outlives the ids.
--   * request_id — minted per wipe, echoed by the ack, so an answer to a wipe
--     the user has already replaced cannot settle the current one.
--
-- One live conversation request per peer, enforced by a partial unique index
-- rather than by the scheduler remembering to check: a second one would be a
-- second answer to wait for, and the ack would settle whichever row it found
-- first.
ALTER TABLE message_delete_intents ADD COLUMN kind TEXT NOT NULL DEFAULT 'message';
ALTER TABLE message_delete_intents ADD COLUMN request_id TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_delete_intents_conversation
	ON message_delete_intents(peer) WHERE kind = 'conversation';

-- ---------------------------------------------------------------------------
-- 3. Erase what earlier builds remembered ABOUT deletions.
--
-- Three tables were keeping an answer to "has this id been deleted here?" long
-- after the deletion itself had finished:
--
--   * message_delete_intents rows with owed = 0 — refusals. Not requests: such
--     a row asks nobody for anything, it exists to recognise the id if a copy
--     of the message comes back, and it was kept for the sender's whole reseed
--     horizon (eight days).
--   * refuse_until on the rows that ARE requests. On an owed row it is not part
--     of the asking; it is a note of when the message was destroyed.
--   * reaction_refusals — the same answer for reactions, with no expiry at all,
--     bounded only by a row count.
--
-- They were written under a keyed digest rather than the id itself, so none of
-- them could be read as a list of what the user deleted. That was not enough.
-- The ROW is the trace: its existence, its conversation (reaction_refusals.
-- scope) and its timestamps say that something in this thread was deleted, and
-- when — to anyone holding the file, including anyone who can later compel the
-- key. A messenger that promises deletion cannot keep a record of having
-- deleted.
--
-- What answers a replay now is the transport: the delivery receipt that stops
-- the sender retrying, the peer's own deletion once they confirm it, and an
-- in-memory window for the rest of this process's life.
--
-- The requests still owed to a peer stay exactly as they are, and that is the
-- line this draws: the one thing this design may write down is WORK NOT YET
-- DONE. A request to a peer and an attachment whose files would not unlink are
-- both descriptions of a future action, and both disappear the moment it
-- happens. A refusal was neither — it described the past and outlived it.
--
-- reaction_refusals itself is left in place, empty. Dropping it would break a
-- rollback to any build that knows the table and whose own ledger says it
-- exists, and an empty table costs a page.
DELETE FROM message_delete_intents WHERE owed = 0;

UPDATE message_delete_intents SET refuse_until = '' WHERE refuse_until <> '';

DELETE FROM reaction_refusals;

-- ---------------------------------------------------------------------------
-- 4. Drop the reactions that are waiting for a message this node does not have.
--
-- A held (`pending = 1`) reaction is a row naming a message id, the
-- conversation it belongs to, the peer who reacted and the moment the fact
-- first arrived — for a message that is not here. It was a good trade when the
-- only reason a message could be missing was that it had not arrived yet: the
-- row let a reaction that overtook its message on the wire still be applied
-- when the message landed.
--
-- It stopped being a good trade when the other reason became common. After a
-- message is deleted, its author keeps offering the reactions they hold, and
-- each offer wrote the deleted id back onto this disk for another hour. The row
-- cannot be told apart from "this node deleted that message", and it outlives
-- the deletion.
--
-- Reactions are no longer held at all (service/reaction_control.go): a fact
-- about a message that is not here is dropped, and the author's periodic
-- re-offer applies it once the message arrives. APPLIED reactions (`pending =
-- 0`) are untouched — they name messages that exist here and are drawn on
-- screen.
DELETE FROM message_reactions WHERE pending = 1;
