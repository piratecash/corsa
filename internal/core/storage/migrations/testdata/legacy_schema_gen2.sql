-- Frozen copy of the chatlog DDL as of commit 5ac6043 — the second schema
-- generation and the one release v1.0.64 shipped, so this is the shape most
-- databases in the field actually have: generation 1 plus the seen_ack and
-- delivery_failed journals.
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
