-- Bootstrap metadata of the shared state database.
--
-- A single row (guarded by the CHECK) rather than a key/value table: the
-- shared database is a set of domain-explicit tables, not a blob store, and
-- the bootstrap facts are a fixed, typed set.
--
-- owner_identity is the FULL identity address. The short prefix in the legacy
-- file name cannot distinguish two identities that collide on eight hex
-- characters, and it cannot catch an operator pointing the node at the wrong
-- file at all.
CREATE TABLE IF NOT EXISTS storage_metadata (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    owner_identity    TEXT NOT NULL,
    bootstrap_version INTEGER NOT NULL,
    created_at        TEXT NOT NULL
);
