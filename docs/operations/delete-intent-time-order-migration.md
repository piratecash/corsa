# Delete-intent time ordering: proposed migration 8

[Русский](delete-intent-time-order-migration.ru.md)

**Candidate status:** Source review and separate native default-backend and `sqlite_cgo` regression checks of schema version 8 are accepted. Each run passed the selected 46 top-level tests and 48 observed subtests across chatlog, migrations and service, without failures or skips. These are scoped candidate checks, not full-suite or release acceptance. This document authorizes no release, deployment or database operation.

## Why the scheduler changes

Delete retries store `next_attempt_at` as UTC RFC3339Nano text. Variable-length fractions do not sort chronologically as text: `…00.1Z` sorts after `…00.11Z`, although 100 ms is earlier than 110 ms. The old comparison can omit a request already due or dispatch a future request early. The peer-reconnect revival comparison has the same defect.

The candidate compares exact UTC nanoseconds. Due requests satisfy `next_attempt_at <= now`, ordered by time before the requested limit. Revival changes only a future request held because that same peer was absent; it does not reset a charged backoff or change attempt counts. Message and conversation requests retain their existing semantics. The observed wipe-test failure is a regression target; its original stored timestamps were not logged, so this source defect is not asserted as its proven sole cause.

## What migration 8 does

`0008_delete_intent_time_order.sql` adds two non-unique expression indexes: `idx_message_delete_intents_due_ns` for due selection/order and `idx_message_delete_intents_peer_held_ns` for peer/hold/time revival. Their comparison key pads the UTC fraction to nine digits without floating-point conversion. Existing timestamp bytes, task rows and published migrations are not rewritten.

A read-only invariant validates active `owed = 1` rows in the migration transaction. It accepts whole-second UTC `Z`, up to nine fractional digits including trailing zeros, and the serialized zero time `0001-01-01T00:00:00Z`. The supported writers in the v2.2.68 deployment baseline and published v2.4.70 use this UTC domain. That source check does not certify a particular database, external edit or backup.

An active empty, offset-form, comma-fraction, excessive-precision or invalid timestamp causes an explicit refusal. Step 8's indexes and ledger entry roll back; data present at entry to that step remains unchanged. Earlier migration steps may already have committed. There is no automatic timestamp conversion, row deletion or ledger repair. Migration 7's existing cleanup of refusal-only `owed = 0` rows is unchanged.

Validation and both index builds have a one-time scan, CPU, disk and temporary-work cost; later writes maintain the indexes. Indexed selection avoids introducing a full time sort, but filtering requests carried by a standing wipe can still visit additional rows. Neither an index plan nor these tests establish a RAM ceiling, a 512 MB result or a latency guarantee.

## Before an approved upgrade

Establish a **verified, SQLite-consistent pre-upgrade backup** for the correct database path, identity and schema. Follow the existing [ordered shutdown procedure](../storage.md#shutdown): stop admissions and drain/join writers before `Database.Close`, which runs last. A failed shutdown stage can leave the database open and is not evidence that copying is safe.

[Storage recovery](../storage.md#failure-is-fatal-and-the-file-is-never-repaired) is a separate explicit operation; the repository does not provide a shared-state backup/export command. Use an approved backup and verification procedure. Do not invent a live `.db` copy, discard its WAL files, or substitute an identity-key export for the state database. If a consistent, verified backup is unavailable, upgrade readiness is unresolved.

After version 8 commits, a binary whose catalog ends at version 7 refuses the newer schema. The unchanged row bytes do **not** make an in-place binary downgrade supported. Keep the pre-upgrade recovery option verified; do not edit the migration ledger or remove indexes to force an older binary to open it.

Both backend checks passed the nanosecond boundary/order and revival regressions, legacy upgrade and atomic-refusal controls, actual index-plan checks, and the original wipe-dispatch regression. The plans used the new indexes without an added temporary time sort; they do not bound the carried-request filter or index-build cost. The original wipe failure's unlogged timestamps still prevent a sole-cause claim.

A candidate still needs integration and full-suite qualification and an approved, verified pre-upgrade backup before upgrade readiness is claimed. A successful readback alone is not a power-loss durability or deployment proof.
