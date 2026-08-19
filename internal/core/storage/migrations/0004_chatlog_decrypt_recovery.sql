-- Decrypt-recovery state after a peer key rotation. Owned by
-- internal/core/chatlog (recovery.go).
--
-- Four tables rather than columns on messages, because each has its own
-- lifetime: a job is retried and evicted, a cycle anchor is immutable for the
-- whole recovery window, an established fact is monotonic and never expires,
-- and a resend intent is keyed by the replacement chain root.
CREATE TABLE IF NOT EXISTS decrypt_recovery_jobs (
	peer TEXT PRIMARY KEY,
	state TEXT NOT NULL DEFAULT 'pending_notice',
	notice_attempts INTEGER NOT NULL DEFAULT 0,
	last_notice_at TEXT NOT NULL DEFAULT '',
	wait_until TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS peer_established (
	peer TEXT PRIMARY KEY,
	established_at TEXT NOT NULL,
	established_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decrypt_recovery_cycles (
	peer TEXT PRIMARY KEY,
	anchored_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decrypt_resend_intents (
	root TEXT PRIMARY KEY,
	original_id TEXT NOT NULL,
	peer TEXT NOT NULL,
	replacement_id TEXT NOT NULL,
	created_at TEXT NOT NULL
);
