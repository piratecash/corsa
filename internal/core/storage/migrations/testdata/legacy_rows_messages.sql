-- Representative message rows, present in every pre-versioned generation.
--
-- FIXTURE, frozen with the schema fixtures: the adoption tests read these rows
-- back after the migration and compare them field by field, so the point is
-- coverage of the column set (every flag, every delivery status, both topics,
-- non-empty metadata and ttl), not realistic content.
INSERT INTO messages (id, topic, sender, recipient, body, flag, delivery_status, ttl_seconds, metadata, created_at, updated_at) VALUES
	('msg-outgoing-sent', 'dm',
	 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
	 'sealed-outgoing-body', '', 'sent', 0, '', '2026-08-01T10:00:00Z', ''),
	('msg-incoming-seen', 'dm',
	 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
	 'sealed-incoming-body', 'immutable', 'seen', 0, '{"undecryptable":true}', '2026-08-01T10:05:00Z', '2026-08-01T10:06:00Z'),
	('msg-outgoing-delivered-ttl', 'dm',
	 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
	 'sealed-ttl-body', 'auto-delete-ttl', 'delivered', 3600, '{"attempts":2}', '2026-08-01T10:10:00Z', '2026-08-01T10:11:00Z'),
	('msg-global', 'global',
	 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '',
	 'broadcast-body', 'any-delete', 'sent', 0, '', '2026-08-01T10:15:00Z', '');
