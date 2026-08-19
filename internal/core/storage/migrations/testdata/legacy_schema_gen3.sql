-- Frozen copy of the chatlog DDL as of commit 9267953 — the third and last
-- pre-versioned schema generation, exactly as chatlog.initSchema plus
-- chatlog.initRecoverySchema emitted it: generation 2 plus the four
-- decrypt-recovery tables.
--
-- That commit landed AFTER the v1.0.64 release bump, so this shape exists only
-- in development builds. Released installations carry generation 2, which is
-- why all three generations have a fixture of their own.
--
-- FIXTURE, never edited. See legacy_schema_gen1.sql.
CREATE TABLE IF NOT EXISTS messages (
	id              TEXT PRIMARY KEY,
	topic           TEXT NOT NULL DEFAULT 'dm' CHECK(topic IN ('dm','global')),
	sender          TEXT NOT NULL,
	recipient       TEXT NOT NULL,
	body            TEXT NOT NULL,
	flag            TEXT NOT NULL DEFAULT '' CHECK(flag IN ('','immutable','sender-delete','any-delete','auto-delete-ttl')),
	delivery_status TEXT NOT NULL DEFAULT 'sent' CHECK(delivery_status IN ('sent','delivered','seen')),
	ttl_seconds     INTEGER NOT NULL DEFAULT 0,
	metadata        TEXT NOT NULL DEFAULT '',
	created_at      TEXT NOT NULL,
	updated_at      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_messages_peer
	ON messages(topic, sender, recipient, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_status
	ON messages(recipient, delivery_status);

CREATE TABLE IF NOT EXISTS seen_ack (
	id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS delivery_failed (
	id TEXT PRIMARY KEY
);

CREATE INDEX IF NOT EXISTS idx_messages_created
	ON messages(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_messages_ttl
	ON messages(flag, created_at) WHERE flag = 'auto-delete-ttl';

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
