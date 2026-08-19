-- Decrypt-recovery rows. Generation 3 only — the tables did not exist before
-- commit 9267953, which landed after the v1.0.64 release bump.
--
-- FIXTURE, frozen with the schema fixtures.
INSERT INTO decrypt_recovery_jobs (peer, state, notice_attempts, last_notice_at, wait_until, created_at, expires_at) VALUES
	('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'awaiting_resend', 2, '2026-08-01T11:00:00Z', '2026-08-01T12:00:00Z', '2026-08-01T10:00:00Z', '2026-08-08T10:00:00Z');

INSERT INTO peer_established (peer, established_at, established_reason) VALUES
	('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '2026-08-01T09:00:00Z', 'outgoing');

INSERT INTO decrypt_recovery_cycles (peer, anchored_at) VALUES
	('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '2026-08-01T10:00:00Z');

INSERT INTO decrypt_resend_intents (root, original_id, peer, replacement_id, created_at) VALUES
	('msg-incoming-seen', 'msg-incoming-seen', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'msg-replacement', '2026-08-01T11:30:00Z');
