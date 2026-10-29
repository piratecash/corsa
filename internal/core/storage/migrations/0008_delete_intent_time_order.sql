-- Exact ordering of active scheduler times, without changing stored history.
-- Existing writers (including v2.4.70) emit UTC RFC3339Nano, whose variable
-- fraction does NOT sort as text: .1Z sorts after .11Z. Pad only the index key
-- to nine digits, retaining every nanosecond and every original row byte.
-- The catalog's read-only invariant rejects unsupported active spellings in
-- this same transaction; no partially qualified index or ledger step commits.
-- Keep the expression byte-for-byte in sync with chatlog's private key.
--
-- Building both indexes is a one-time scan/build/disk cost. They avoid a new
-- recurring full sort, not all possible work: carried requests can still be
-- rejected by the scheduler's existing NOT-covered predicate. This is a
-- forward-only version 8; a version-7 catalog will refuse the newer schema.
CREATE INDEX IF NOT EXISTS idx_message_delete_intents_due_ns
	ON message_delete_intents(substr(next_attempt_at, 1, 19) || '.' || substr(replace(substr(next_attempt_at, 21), 'Z', '') || '000000000', 1, 9))
	WHERE owed = 1;

CREATE INDEX IF NOT EXISTS idx_message_delete_intents_peer_held_ns
	ON message_delete_intents(peer, held, substr(next_attempt_at, 1, 19) || '.' || substr(replace(substr(next_attempt_at, 21), 'Z', '') || '000000000', 1, 9))
	WHERE owed = 1;
