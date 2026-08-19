-- Chat history. Owned by internal/core/chatlog.
--
-- The table predates the migration ledger, so this statement must reproduce
-- the pre-versioned DDL exactly: on an existing installation it is a no-op and
-- the accompanying verifier is what proves the shape.
--
-- New chatlog facts are carried in the metadata JSON column instead of new
-- columns, so that a rolled-back binary keeps reading the same rows.
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
