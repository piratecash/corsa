-- Everything the deletion subsystem keeps on disk. Owned by
-- internal/core/chatlog; the behaviour these tables serve is in
-- docs/dm-commands.md.
--
-- Deleting a message removes the local copy at once — waiting for a peer who
-- may be offline for days is the exposure the feature exists to end — so what
-- has to survive is the REQUEST the peer still owes us, and the refusal that
-- keeps a late re-delivery from putting the row back.

-- What a peer owes us, one row per message. A conversation wipe is N of
-- these and nothing else: "delete this thread" and "delete this message"
-- differ only in how many ids are involved, so there is no second request
-- table, no row-set table, no scheduler and no receiver-side apparatus for
-- a bulk command — and no timestamp boundary to describe rows a bulk
-- request could not name.
--
-- No body, no sender, no original timestamp: only the addressing the
-- request needs. A blanked-out message row
-- was the alternative and was rejected — it leaves a tombstone in the
-- conversation for anyone reading the database, which is precisely what the
-- deletion was meant to prevent.
--
-- `held` says why a row is not due, when it is not, and the reasons are not
-- interchangeable:
--
--   * 0 — nothing holds it. Either it is due, or it is waiting out the BACKOFF
--     of an attempt that actually went out.
--   * 1 — parked on the PEER: they could not be asked at all. A reconnect pulls
--     these forward. Resetting a real backoff the same way would ask a peer
--     whose transport reconnects every few seconds once per handshake instead
--     of on the exponential schedule.
--   * 2 — parked on US: a wipe has written the request, but this node has not
--     yet learned whether the message ever reached the wire. A reconnect must
--     NOT release these; doing so would send a request naming a message the
--     peer has never seen.
CREATE TABLE IF NOT EXISTS message_delete_intents (
	message_id TEXT PRIMARY KEY,
	peer TEXT NOT NULL,
	created_at TEXT NOT NULL,
	next_attempt_at TEXT NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	held INTEGER NOT NULL DEFAULT 0,
	owed INTEGER NOT NULL DEFAULT 1,
	refuse_until TEXT NOT NULL DEFAULT ''
);

-- `owed` and `refuse_until` are the two things a deleted id needs, and they
-- are two facts about one message, so they live in one row:
--
--   * owed=1  — the peer still has to be asked. Cleared, not deleted, when
--     their ack settles it: the refusal outlives the request.
--   * refuse_until — ignore a re-delivery of this id until then. A deletion
--     removes the chatlog row AND clears the router's dedup gate for its
--     id, which is what lets a relay or inbox replay put the row straight
--     back; the refusal is the answer. It is here rather than in memory
--     alone because the replay window and a restart overlap — the process
--     that erased the message can be gone by the time the echo lands.
--
-- The two are independent: a request to an absent contact is kept
-- indefinitely while its refusal expires within the hour, and a receiver
-- asked to delete a message it has not received yet gets a row that owes
-- nobody anything and only refuses. The row is gone once it owes nothing
-- and refuses nothing.
--
-- The scheduler's only query is "which intents are due now", ordered by due
-- time; the count per peer drives the "N waiting for the peer" indicator;
-- the reaper asks for rows whose refusal has run out.
CREATE INDEX IF NOT EXISTS idx_message_delete_intents_due
	ON message_delete_intents(next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_message_delete_intents_peer
	ON message_delete_intents(peer);
CREATE INDEX IF NOT EXISTS idx_message_delete_intents_refusal
	ON message_delete_intents(refuse_until);
