package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ledgerTable is the name of the table the runner owns exclusively.
const ledgerTable = "schema_migrations"

// migrationLedgerDDL creates the table the runner owns exclusively. It is the
// single source of truth for the schema version: PRAGMA user_version is
// deliberately not used as a second one.
const migrationLedgerDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version    INTEGER PRIMARY KEY,
	name       TEXT NOT NULL UNIQUE,
	checksum   TEXT NOT NULL,
	applied_at TEXT NOT NULL
)`

// MigrationResult is the outcome of one attempted step.
type MigrationResult string

const (
	// MigrationApplied means the step committed.
	MigrationApplied MigrationResult = "applied"
	// MigrationFailed means the step was rolled back.
	MigrationFailed MigrationResult = "failed"
)

// AttemptedMigration describes a step this process actually ran, for the
// startup log. Steps already present in the ledger are not reported.
//
// A failed step is reported too, and it is the one the operator needs most:
// without it a crash-on-upgrade leaves only a returned error, with no record
// of which version was being applied or how long it ran.
type AttemptedMigration struct {
	Name     string
	Result   MigrationResult
	Duration time.Duration
	Version  Version
}

// ledgerRow is one schema_migrations record as read back from the file.
type ledgerRow struct {
	name     string
	checksum string
	version  Version
}

// ensureLedger creates the migration table and proves its shape.
//
// Creating it is safe on a legacy chatlog file: the table is new there, and
// creating it records nothing about the schema. Verifying it is not optional
// hardening: IF NOT EXISTS is a no-op against a table that already exists, so
// a schema_migrations without the primary key, without the UNIQUE name, or
// with unexpected columns would be accepted and then trusted as the version
// history everything else in this package relies on.
func ensureLedger(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, migrationLedgerDDL); err != nil {
		return fmt.Errorf("storage: create migration ledger: %w", err)
	}
	reference, err := ledgerReference(ctx)
	if err != nil {
		return err
	}
	// VerifySchema also rejects an undeclared trigger on the table: one would
	// sit between the runner's INSERT and the read-back that confirms it.
	return VerifySchema(ctx, db, reference, reference)
}

// ledgerReference is the required shape of schema_migrations, obtained the same
// way as every other expectation: by executing the DDL that creates it.
func ledgerReference(ctx context.Context) (ReferenceSchema, error) {
	return BuildReferenceSchemaFromSQL(ctx, migrationLedgerDDL)
}

// readLedger returns the applied migrations keyed by version.
func readLedger(ctx context.Context, db *sql.DB) (map[Version]ledgerRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version ASC`)
	if err != nil {
		return nil, fmt.Errorf("storage: read migration ledger: %w", err)
	}
	defer func() { _ = rows.Close() }()

	applied := make(map[Version]ledgerRow)
	for rows.Next() {
		var row ledgerRow
		var version int
		if err := rows.Scan(&version, &row.name, &row.checksum); err != nil {
			return nil, fmt.Errorf("storage: scan migration ledger: %w", err)
		}
		row.version = Version(version)
		// A duplicate cannot happen through the primary key, so seeing one
		// means the ledger was written by something else. Collapsing it into
		// the map would silently pick a winner and call the history verified.
		if previous, exists := applied[row.version]; exists {
			return nil, fmt.Errorf("%w: version %s is recorded twice, as %q and %q",
				ErrMigrationDrift, row.version, previous.name, row.name)
		}
		applied[row.version] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: iterate migration ledger: %w", err)
	}
	return applied, nil
}

// ledgerVersion returns the highest recorded version, LegacyVersion when the
// ledger is empty.
func ledgerVersion(applied map[Version]ledgerRow) Version {
	highest := LegacyVersion
	for version := range applied {
		if version > highest {
			highest = version
		}
	}
	return highest
}

// verifyLedger compares the recorded history against the embedded catalog.
// It runs before any migration so an incompatible file is rejected without
// being modified.
func verifyLedger(applied map[Version]ledgerRow, catalog []Migration) error {
	latest := LatestVersion(catalog)
	byVersion := make(map[Version]Migration, len(catalog))
	for _, migration := range catalog {
		byVersion[migration.Version] = migration
	}

	for version, row := range applied {
		if version > latest {
			return fmt.Errorf("%w: file is at version %s, this binary knows up to %s",
				ErrSchemaTooNew, version, latest)
		}
		known, exists := byVersion[version]
		if !exists {
			return fmt.Errorf("%w: version %s is recorded but absent from the catalog", ErrMigrationDrift, version)
		}
		if known.Name != row.name {
			return fmt.Errorf("%w: version %s is recorded as %q, catalog has %q",
				ErrMigrationDrift, version, row.name, known.Name)
		}
		if known.Checksum() != row.checksum {
			return fmt.Errorf("%w: version %s (%s) has a different checksum than the catalog",
				ErrMigrationDrift, version, known.Name)
		}
	}

	// The ledger must be a contiguous prefix 1..current. A hole means a
	// version was deleted or applied out of order, and applying the missing
	// step now would run it against a schema its author never saw.
	current := ledgerVersion(applied)
	for version := Version(1); version <= current; version++ {
		if _, exists := applied[version]; !exists {
			return fmt.Errorf("%w: version %s is missing while %s is recorded",
				ErrMigrationDrift, version, current)
		}
	}
	return nil
}

// confirmLedgerRow re-reads the row just written and verifies it verbatim.
//
// This runs inside the migration transaction, so what it sees is what COMMIT
// would make durable. Anything that quietly changed the row between the INSERT
// and here — a trigger, a conflict policy — turns into a failed step instead
// of applied DDL with a version that does not match it.
func confirmLedgerRow(ctx context.Context, tx SchemaReader, migration Migration, appliedAt string) error {
	var (
		name       string
		checksum   string
		recordedAt string
	)
	err := ScanOne(ctx, tx,
		[]any{&name, &checksum, &recordedAt},
		`SELECT name, checksum, applied_at FROM schema_migrations WHERE version = ?`,
		int(migration.Version))
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: version %s vanished after it was recorded", ErrSchemaIncompatible, migration.Version)
	}
	if err != nil {
		return fmt.Errorf("storage: confirm migration %s was recorded: %w", migration.Name, err)
	}
	// Every written value, not just the interesting ones: the point of the
	// read-back is that NOTHING altered the row between the INSERT and the
	// COMMIT, and a trigger that rewrote only the timestamp is exactly the
	// kind of alteration a partial comparison would wave through.
	if name != migration.Name || checksum != migration.Checksum() || recordedAt != appliedAt {
		return fmt.Errorf("%w: version %s was recorded as %q, not as written",
			ErrSchemaIncompatible, migration.Version, name)
	}
	return nil
}

// migrate applies every catalog entry up to and including limit that is not
// yet recorded. Steps run strictly in order and a failure stops the run, so
// version N+1 never executes after N failed.
func migrate(ctx context.Context, db *sql.DB, run migrationRun) ([]AttemptedMigration, error) {
	catalog, limit := run.Catalog, run.Limit

	if err := ensureLedger(ctx, db); err != nil {
		return nil, err
	}
	recorded, err := readLedger(ctx, db)
	if err != nil {
		return nil, err
	}
	if err := verifyLedger(recorded, catalog); err != nil {
		return nil, err
	}

	var attempted []AttemptedMigration
	for _, migration := range catalog {
		if migration.Version > limit {
			break
		}
		if _, exists := recorded[migration.Version]; exists {
			continue
		}
		result, err := applyMigration(ctx, db, run, migration)
		if result != nil {
			attempted = append(attempted, *result)
		}
		if err != nil {
			return attempted, err
		}
	}
	return attempted, nil
}

// applyMigration runs one step under an exclusive write lock.
//
// It returns nil and no error when a concurrently starting process recorded
// the version first: the caller has nothing to log and nothing to redo. When
// the step itself fails it returns BOTH the error and a failed result, so the
// caller can log what was attempted instead of only what succeeded.
//
// The DDL, its structural verification and the ledger row commit together, so
// a process killed mid-step leaves either the whole version or none of it.
func applyMigration(ctx context.Context, db *sql.DB, run migrationRun, migration Migration) (*AttemptedMigration, error) {
	catalog, now := run.Catalog, run.Now

	// The clock starts before the connection is acquired, because waiting
	// for the write lock is part of the step — and it is the part most
	// likely to blow a deadline when another process is mid-upgrade. A
	// failure there must produce the same result=failed event as a failure
	// in the DDL, or the log would be silent on the very scenario the busy
	// timeout exists for.
	started := now()
	failed := func(err error) (*AttemptedMigration, error) {
		return &AttemptedMigration{
			Version:  migration.Version,
			Name:     migration.Name,
			Result:   MigrationFailed,
			Duration: now().Sub(started),
		}, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return failed(fmt.Errorf("storage: acquire connection for migration %s: %w", migration.Name, err))
	}
	defer func() { _ = conn.Close() }()

	// BEGIN IMMEDIATE takes the write lock now instead of on the first
	// write, so two processes cannot both read "not applied" and then both
	// execute the DDL.
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return failed(fmt.Errorf("storage: begin migration %s: %w", migration.Name, err))
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// The rollback must run even when ctx is already cancelled —
		// otherwise the transaction stays open on a connection that goes
		// back into the pool.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	}()

	// Re-read under the write lock: the racer that won recorded the version
	// after our pre-flight ledger read. WHAT it recorded is the question —
	// another binary can hold a different migration under the same number,
	// and accepting the row on its version alone would let this process build
	// the rest of the catalog on top of a step it never checked. The final
	// schema check cannot catch that: two catalogs can differ in name or
	// checksum and still produce the same objects.
	var raced struct {
		Name     string
		Checksum string
	}
	err = conn.QueryRowContext(ctx,
		`SELECT name, checksum FROM schema_migrations WHERE version = ?`,
		int(migration.Version)).Scan(&raced.Name, &raced.Checksum)
	switch {
	case err == nil:
		if raced.Name != migration.Name || raced.Checksum != migration.Checksum() {
			return failed(fmt.Errorf("%w: version %s was recorded by another process as %q, this binary knows it as %q",
				ErrMigrationDrift, migration.Version, raced.Name, migration.Name))
		}
		return nil, nil
	case errors.Is(err, sql.ErrNoRows):
		// Not applied yet — proceed.
	default:
		return failed(fmt.Errorf("storage: re-check migration %s: %w", migration.Name, err))
	}

	// What the runner's own rows look like BEFORE this step touches anything.
	// This is the guarantee that the migration left them alone, and it is an
	// observed fact rather than a reading of the SQL: it holds whatever route
	// the statements took — a trigger, a view, a spelling the catalog scan did
	// not recognise.
	before, err := readRunnerState(ctx, conn)
	if err != nil {
		return failed(err)
	}

	if _, err := conn.ExecContext(ctx, migration.SQL); err != nil {
		return failed(fmt.Errorf("storage: apply migration %s (version %s): %w", migration.Name, migration.Version, err))
	}
	if err := confirmRunnerState(ctx, conn, before, migration); err != nil {
		return failed(err)
	}
	// Required: everything the catalog produces UP TO this version. Allowed:
	// everything it ever produces — a pre-versioned database may already hold
	// an index a later version declares, and calling that unexpected would
	// stop the upgrade before reaching the version that declares it.
	required, err := BuildReferenceSchema(ctx, catalog, migration.Version)
	if err != nil {
		return failed(err)
	}
	allowed, err := BuildReferenceSchema(ctx, catalog, LatestVersion(catalog))
	if err != nil {
		return failed(err)
	}
	if err := VerifySchema(ctx, conn, required, allowed); err != nil {
		return failed(fmt.Errorf("storage: verify migration %s (version %s): %w", migration.Name, migration.Version, err))
	}
	// The ledger row is confirmed, not assumed. An INSERT that affects no row
	// is a successful statement that recorded nothing, and committing on top
	// of it would leave applied DDL with no version — the exact split this
	// transaction exists to prevent.
	appliedAt := started.UTC().Format(time.RFC3339Nano)
	result, err := conn.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
		int(migration.Version), migration.Name, migration.Checksum(), appliedAt,
	)
	if err != nil {
		return failed(fmt.Errorf("storage: record migration %s: %w", migration.Name, err))
	}
	recorded, err := result.RowsAffected()
	if err != nil {
		return failed(fmt.Errorf("storage: confirm migration %s was recorded: %w", migration.Name, err))
	}
	if recorded != 1 {
		return failed(fmt.Errorf("%w: recording migration %s affected %d rows, want 1",
			ErrSchemaIncompatible, migration.Name, recorded))
	}
	// RowsAffected is not proof. It counts what the INSERT touched, and an
	// AFTER INSERT trigger is free to delete or rewrite that row afterwards
	// while SQLite still reports one affected row. The only answer that
	// settles it is reading the row back, inside this transaction, and
	// checking it says what we wrote.
	if err := confirmLedgerRow(ctx, conn, migration, appliedAt); err != nil {
		return failed(err)
	}
	// Facts the caller must not be able to lose if this step is rolled back —
	// the owner identity of a database being bootstrapped — are written here,
	// inside the same transaction as the version that makes room for them.
	if run.Seal != nil && migration.Version == run.SealVersion {
		if err := run.Seal(ctx, conn); err != nil {
			return failed(err)
		}
		// The seal writes the runner's own rows deliberately, so from here on
		// THEY are the baseline. Comparing the condition's effect against the
		// state from before the seal reported the owner row this step just
		// recorded as damage, which made a condition on the bootstrap version
		// impossible to write.
		before, err = readRunnerState(ctx, conn)
		if err != nil {
			return failed(err)
		}
	}
	if migration.Invariant != nil {
		if err := runInvariant(ctx, conn, migration); err != nil {
			return failed(err)
		}
		// The same observation again, now including this step's own row: a
		// condition runs on the migration's connection, and a mistake there
		// reaches the ledger exactly like a mistake in the SQL does.
		if err := confirmRunnerState(ctx, conn, before, migration); err != nil {
			return failed(err)
		}
		if err := confirmLedgerRow(ctx, conn, migration, appliedAt); err != nil {
			return failed(err)
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return failed(fmt.Errorf("storage: commit migration %s: %w", migration.Name, err))
	}
	committed = true

	return &AttemptedMigration{
		Version:  migration.Version,
		Name:     migration.Name,
		Result:   MigrationApplied,
		Duration: now().Sub(started),
	}, nil
}

// readOnly is the handle a migration's condition runs on. It hides
// ExecContext, and the connection under it is sealed with PRAGMA query_only
// for the duration of the call, so SQLite refuses a write.
//
// It is not a sandbox and does not try to be one. A migration and its
// condition are reviewed code compiled into this binary, not input: the job
// here is to stop a MISTAKE from reaching the file, and what proves that is
// confirmRunnerState plus the schema check, which observe the result instead
// of reading the statements.
type readOnly struct{ conn *sql.Conn }

func (r readOnly) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return r.conn.QueryContext(ctx, query, args...)
}

// runnerState is what this package keeps for itself in the file: the recorded
// versions and the owner row.
type runnerState struct {
	Ledger map[Version]ledgerEntry
	Owner  ownerRow
	Owned  bool
}

// ownerRow is the bootstrap row, in full. Comparing only the identity left the
// rest of it unguarded, and bootstrap_version is the field that says which
// storage_metadata contract wrote the file.
type ownerRow struct {
	Identity        string
	CreatedAt       string
	BootstrapFormat int
}

// ledgerEntry is one recorded version, as written.
type ledgerEntry struct {
	Name      string
	Checksum  string
	AppliedAt string
}

// readRunnerState reads the runner's own rows.
func readRunnerState(ctx context.Context, tx SchemaReader) (runnerState, error) {
	state := runnerState{Ledger: map[Version]ledgerEntry{}}

	rows, err := tx.QueryContext(ctx, `SELECT version, name, checksum, applied_at FROM schema_migrations`)
	if err != nil {
		return runnerState{}, fmt.Errorf("storage: read the migration ledger: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			version int
			entry   ledgerEntry
		)
		if err := rows.Scan(&version, &entry.Name, &entry.Checksum, &entry.AppliedAt); err != nil {
			return runnerState{}, fmt.Errorf("storage: scan the migration ledger: %w", err)
		}
		state.Ledger[Version(version)] = entry
	}
	if err := rows.Err(); err != nil {
		return runnerState{}, fmt.Errorf("storage: iterate the migration ledger: %w", err)
	}

	// The owner table does not exist yet while the bootstrap version is being
	// applied, and its absence is part of the state rather than a failure.
	err = ScanOne(ctx, tx,
		[]any{&state.Owner.Identity, &state.Owner.BootstrapFormat, &state.Owner.CreatedAt},
		`SELECT owner_identity, bootstrap_version, created_at FROM storage_metadata WHERE id = 1`)
	switch {
	case err == nil:
		state.Owned = true
	case errors.Is(err, sql.ErrNoRows):
	case isMissingTable(err):
	default:
		return runnerState{}, fmt.Errorf("storage: read the owner row: %w", err)
	}
	return state, nil
}

// isMissingTable reports whether the error is SQLite refusing a query against
// a table that does not exist yet.
func isMissingTable(err error) bool {
	return strings.Contains(err.Error(), "no such table")
}

// confirmRunnerState fails when a migration changed the runner's own rows.
//
// This is where the promise "a migration cannot damage the ledger or the owner
// row" actually lives. The catalog is scanned for such statements too, but a
// scan reads TEXT and can be wrong in either direction; this compares the rows
// themselves, so it holds for whatever the statements did and however they
// were written.
//
// The one difference allowed is the row this step records for itself: the
// runner writes it between the two observations.
func confirmRunnerState(ctx context.Context, tx SchemaReader, before runnerState, migration Migration) error {
	after, err := readRunnerState(ctx, tx)
	if err != nil {
		return err
	}

	if before.Owned != after.Owned || before.Owner != after.Owner {
		return fmt.Errorf("%w: migration %s (version %s) changed the owner row",
			ErrSchemaIncompatible, migration.Name, migration.Version)
	}

	for version, entry := range before.Ledger {
		recorded, exists := after.Ledger[version]
		if !exists {
			return fmt.Errorf("%w: migration %s (version %s) removed version %s from the ledger",
				ErrSchemaIncompatible, migration.Name, migration.Version, version)
		}
		if recorded != entry {
			return fmt.Errorf("%w: migration %s (version %s) rewrote the ledger row of version %s",
				ErrSchemaIncompatible, migration.Name, migration.Version, version)
		}
	}
	for version := range after.Ledger {
		if _, expected := before.Ledger[version]; !expected && version != migration.Version {
			return fmt.Errorf("%w: migration %s (version %s) recorded version %s in the ledger",
				ErrSchemaIncompatible, migration.Name, migration.Version, version)
		}
	}
	return nil
}

// migrationRun is one pass of the runner over a catalog.
type migrationRun struct {
	// Catalog is the whole ordered set of steps, which the schema check needs
	// even when only some of them are applied.
	Catalog []Migration

	// Now is the injected clock: applied_at and the reported durations are
	// data, and tests must be able to fix them.
	Now func() time.Time

	// Seal writes the runner's own facts inside the transaction of
	// SealVersion, after that version's ledger row and before COMMIT.
	Seal func(ctx context.Context, conn *sql.Conn) error

	// Limit is the highest version to apply in this pass.
	Limit Version

	// SealVersion is the version Seal belongs to.
	SealVersion Version
}

// runInvariant runs a migration's own condition inside its transaction, after
// the ledger row, with the connection sealed against writes.
//
// The ORDER is what makes this safe. A condition holds the migration's own
// connection, and a callback can end a transaction: COMMIT is not a write, so
// no read-only setting refuses it. With the ledger row already written, what
// such a COMMIT commits is a COMPLETE step — DDL and recorded version
// together — instead of DDL the ledger never mentions, which nothing could
// have repaired afterwards.
//
// query_only refuses writes from the condition, and both forms of tampering
// are detected before this step is allowed to finish: a condition that
// switched the pragma back off, and one that closed the transaction. On either
// the step is rolled back and the run stops, so version N+1 is never applied
// after a failure at N.
func runInvariant(ctx context.Context, conn *sql.Conn, migration Migration) error {
	if _, err := conn.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		return fmt.Errorf("storage: seal connection for the condition of %s: %w", migration.Name, err)
	}
	invariantErr := migration.Invariant(ctx, readOnly{conn})

	var sealed bool
	if err := ScanOne(ctx, readOnly{conn}, &sealed, "PRAGMA query_only"); err != nil {
		return fmt.Errorf("storage: re-read the seal after the condition of %s: %w", migration.Name, err)
	}
	// Restoring write mode must not depend on ctx: the caller still has to
	// commit on this same connection.
	if _, err := conn.ExecContext(context.WithoutCancel(ctx), "PRAGMA query_only = OFF"); err != nil {
		return fmt.Errorf("storage: unseal connection after the condition of %s: %w", migration.Name, err)
	}
	if invariantErr != nil {
		return fmt.Errorf("%w: the condition of migration %s (version %s) does not hold: %w",
			ErrSchemaIncompatible, migration.Name, migration.Version, invariantErr)
	}
	if !sealed {
		return fmt.Errorf("%w: the condition of migration %s (version %s) unsealed its connection",
			ErrSchemaIncompatible, migration.Name, migration.Version)
	}
	// With the transaction still open BEGIN IMMEDIATE must fail, so a BEGIN
	// that SUCCEEDS proves the condition closed it.
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
		return fmt.Errorf("%w: the condition of migration %s (version %s) ended the migration transaction",
			ErrSchemaIncompatible, migration.Name, migration.Version)
	}
	return nil
}
