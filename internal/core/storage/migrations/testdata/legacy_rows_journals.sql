-- Delivery journal rows. Generation 2 and later only — the tables did not
-- exist before commit 2987836.
--
-- FIXTURE, frozen with the schema fixtures.
INSERT INTO seen_ack (id) VALUES ('msg-incoming-seen');

INSERT INTO delivery_failed (id) VALUES ('msg-outgoing-sent');
