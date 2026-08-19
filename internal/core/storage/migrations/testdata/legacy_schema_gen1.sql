-- Frozen copy of the chatlog DDL as of commit 834a222 — the first schema
-- generation: the messages table and its four indexes, before the delivery
-- journals existed.
--
-- FIXTURE, never edited. It is the oldest shape still in the field, and the
-- adoption tests are only meaningful while it keeps reproducing that shape
-- byte for byte. A schema change adds a migration; it does not touch this file.
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

	CREATE INDEX IF NOT EXISTS idx_messages_created
		ON messages(created_at DESC);

	CREATE INDEX IF NOT EXISTS idx_messages_ttl
		ON messages(flag, created_at) WHERE flag = 'auto-delete-ttl';
