-- Local message identity: the wire id is unsigned and not globally unique.
-- Preserve every existing arrival rowid, wire value and message byte. This is
-- an ordinary rowid table; WITHOUT ROWID would destroy sidebar ordering.
ALTER TABLE messages RENAME TO h1_messages_v8;
CREATE TABLE messages (
 id TEXT NOT NULL,
 topic TEXT NOT NULL DEFAULT 'dm' CHECK(topic IN ('dm','global')),
 sender TEXT NOT NULL,
 recipient TEXT NOT NULL,
 body TEXT NOT NULL,
 flag TEXT NOT NULL DEFAULT '' CHECK(flag IN ('','immutable','sender-delete','any-delete','auto-delete-ttl')),
 delivery_status TEXT NOT NULL DEFAULT 'sent' CHECK(delivery_status IN ('sent','delivered','seen')),
 ttl_seconds INTEGER NOT NULL DEFAULT 0,
 metadata TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL DEFAULT '',
 PRIMARY KEY (topic,id,sender,recipient)
);
INSERT INTO messages(rowid,id,topic,sender,recipient,body,flag,delivery_status,ttl_seconds,metadata,created_at,updated_at)
 SELECT rowid,id,topic,sender,recipient,body,flag,delivery_status,ttl_seconds,metadata,created_at,updated_at FROM h1_messages_v8;

-- Existing live rows have globally unique ids, so their journal binding is
-- exact. An orphan cannot be assigned invented parties: retain its old fact
-- separately, outside the active delivery journals.
ALTER TABLE seen_ack RENAME TO h1_seen_ack_v8;
CREATE TABLE seen_ack (
 topic TEXT NOT NULL, id TEXT NOT NULL, sender TEXT NOT NULL, recipient TEXT NOT NULL,
 PRIMARY KEY(topic,id,sender,recipient)
);
CREATE TABLE seen_ack_legacy_unbound (id TEXT PRIMARY KEY);
INSERT INTO seen_ack(rowid,topic,id,sender,recipient) SELECT j.rowid,m.topic,j.id,m.sender,m.recipient FROM h1_seen_ack_v8 j JOIN h1_messages_v8 m ON m.id=j.id;
INSERT INTO seen_ack_legacy_unbound(rowid,id) SELECT j.rowid,j.id FROM h1_seen_ack_v8 j WHERE NOT EXISTS(SELECT 1 FROM h1_messages_v8 m WHERE m.id=j.id);
DROP TABLE h1_seen_ack_v8;
ALTER TABLE delivery_failed RENAME TO h1_delivery_failed_v8;
CREATE TABLE delivery_failed (
 topic TEXT NOT NULL, id TEXT NOT NULL, sender TEXT NOT NULL, recipient TEXT NOT NULL,
 PRIMARY KEY(topic,id,sender,recipient)
);
CREATE TABLE delivery_failed_legacy_unbound (id TEXT PRIMARY KEY);
INSERT INTO delivery_failed(rowid,topic,id,sender,recipient) SELECT j.rowid,m.topic,j.id,m.sender,m.recipient FROM h1_delivery_failed_v8 j JOIN h1_messages_v8 m ON m.id=j.id;
INSERT INTO delivery_failed_legacy_unbound(rowid,id) SELECT j.rowid,j.id FROM h1_delivery_failed_v8 j WHERE NOT EXISTS(SELECT 1 FROM h1_messages_v8 m WHERE m.id=j.id);
DROP TABLE h1_delivery_failed_v8;

ALTER TABLE decrypt_resend_intents RENAME TO h1_decrypt_resend_intents_v8;
CREATE TABLE decrypt_resend_intents (
 root TEXT NOT NULL, original_id TEXT NOT NULL, peer TEXT NOT NULL,
 replacement_id TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(peer,root)
);
INSERT INTO decrypt_resend_intents(rowid,root,original_id,peer,replacement_id,created_at) SELECT rowid,root,original_id,peer,replacement_id,created_at FROM h1_decrypt_resend_intents_v8;
DROP TABLE h1_decrypt_resend_intents_v8;

-- Bind existing facts to their sole old message, without merging the local
-- actor's same-ID reactions across different conversations. Unmatched facts
-- retain empty parties and are never promoted to an applied message witness.
ALTER TABLE message_reactions RENAME TO h1_message_reactions_v8;
CREATE TABLE message_reactions (
 scope TEXT NOT NULL, message_id TEXT NOT NULL, actor TEXT NOT NULL,
 emoji TEXT NOT NULL, op INTEGER NOT NULL, clock INTEGER NOT NULL,
 pending INTEGER NOT NULL DEFAULT 0, first_seen_at TEXT NOT NULL,
 updated_at TEXT NOT NULL, message_sender TEXT NOT NULL DEFAULT '',
 message_recipient TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(scope,message_id,message_sender,message_recipient,actor,emoji)
);
INSERT INTO message_reactions(rowid,scope,message_id,actor,emoji,op,clock,pending,first_seen_at,updated_at,message_sender,message_recipient)
 SELECT j.rowid,j.scope,j.message_id,j.actor,j.emoji,j.op,j.clock,j.pending,j.first_seen_at,j.updated_at,
 COALESCE((SELECT m.sender FROM h1_messages_v8 m WHERE m.topic='dm' AND m.id=j.message_id AND (m.sender=j.scope OR m.recipient=j.scope)),''),
 COALESCE((SELECT m.recipient FROM h1_messages_v8 m WHERE m.topic='dm' AND m.id=j.message_id AND (m.sender=j.scope OR m.recipient=j.scope)),'')
 FROM h1_message_reactions_v8 j;
DROP TABLE h1_message_reactions_v8;
CREATE INDEX idx_message_reactions_scope
    ON message_reactions(scope, message_id);
CREATE INDEX idx_message_reactions_pending
    ON message_reactions(pending, actor, first_seen_at);
CREATE INDEX idx_message_reactions_actor_clock
    ON message_reactions(actor, clock);
CREATE INDEX idx_message_reactions_reoffer_page
    ON message_reactions(actor, scope, pending, updated_at DESC, message_id DESC, emoji DESC);

-- Pending requests are scoped independently even when wire IDs collide.
-- Missing original parties remain explicitly unbound, with their known peer.
ALTER TABLE message_delete_intents RENAME TO h1_delete_intents_v8;
CREATE TABLE message_delete_intents (
 message_id TEXT,
 peer TEXT NOT NULL,
 created_at TEXT NOT NULL,
 next_attempt_at TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,
 held INTEGER NOT NULL DEFAULT 0,
 owed INTEGER NOT NULL DEFAULT 1,
 refuse_until TEXT NOT NULL DEFAULT '',
 kind TEXT NOT NULL DEFAULT 'message',
 request_id TEXT NOT NULL DEFAULT '',
 original_sender TEXT NOT NULL DEFAULT '',
 original_recipient TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(kind,peer,message_id,original_sender,original_recipient)
);
INSERT INTO message_delete_intents(rowid,message_id,peer,created_at,next_attempt_at,attempts,held,owed,refuse_until,kind,request_id,original_sender,original_recipient)
 SELECT i.rowid,i.message_id,i.peer,i.created_at,i.next_attempt_at,i.attempts,i.held,i.owed,i.refuse_until,i.kind,i.request_id,
 COALESCE((SELECT m.sender FROM h1_messages_v8 m WHERE m.topic='dm' AND m.id=i.message_id AND (m.sender=i.peer OR m.recipient=i.peer)),''),
 COALESCE((SELECT m.recipient FROM h1_messages_v8 m WHERE m.topic='dm' AND m.id=i.message_id AND (m.sender=i.peer OR m.recipient=i.peer)),'')
 FROM h1_delete_intents_v8 i;
DROP TABLE h1_delete_intents_v8;
CREATE INDEX idx_message_delete_intents_due ON message_delete_intents(next_attempt_at);
CREATE INDEX idx_message_delete_intents_peer ON message_delete_intents(peer);
CREATE INDEX idx_message_delete_intents_refusal ON message_delete_intents(refuse_until);
CREATE INDEX idx_message_delete_intents_id ON message_delete_intents(message_id);
CREATE UNIQUE INDEX idx_message_delete_intents_conversation ON message_delete_intents(peer) WHERE kind='conversation';
CREATE INDEX idx_message_delete_intents_due_ns
	ON message_delete_intents(substr(next_attempt_at, 1, 19) || '.' || substr(replace(substr(next_attempt_at, 21), 'Z', '') || '000000000', 1, 9))
	WHERE owed = 1;

CREATE INDEX idx_message_delete_intents_peer_held_ns
	ON message_delete_intents(peer, held, substr(next_attempt_at, 1, 19) || '.' || substr(replace(substr(next_attempt_at, 21), 'Z', '') || '000000000', 1, 9))
	WHERE owed = 1;

DROP TABLE h1_messages_v8;
CREATE INDEX idx_messages_peer ON messages(topic,sender,recipient,created_at);
CREATE INDEX idx_messages_status ON messages(recipient,delivery_status);
CREATE INDEX idx_messages_created ON messages(created_at DESC);
CREATE INDEX idx_messages_ttl ON messages(flag,created_at) WHERE flag='auto-delete-ttl';

-- Legacy ID-only APIs resolve/refuse with bounded index lookup, not a full scan.
CREATE INDEX idx_messages_wire_id ON messages(id);
