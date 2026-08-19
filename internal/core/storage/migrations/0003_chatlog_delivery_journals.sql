-- Durable delivery journals. Owned by internal/core/chatlog.
--
-- Both tables are pure id sets, and both exist to stop a retry scheduler from
-- reseeding work that is already finished: delivery_failed marks an abandoned
-- outgoing message, seen_ack marks a seen receipt the original sender
-- confirmed. Keeping them out of messages is deliberate — the retry state is
-- journal-shaped, and the message row must stay rollback-compatible.
CREATE TABLE IF NOT EXISTS seen_ack (
	id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS delivery_failed (
	id TEXT PRIMARY KEY
);
