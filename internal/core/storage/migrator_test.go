package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// The engine tests use synthetic catalogs on purpose: they must fail when the
// runner breaks, not when a production migration changes. The real catalog is
// exercised in internal/core/storage/migrations.

const metadataDDL = `CREATE TABLE IF NOT EXISTS storage_metadata (
	id                INTEGER PRIMARY KEY CHECK (id = 1),
	owner_identity    TEXT NOT NULL,
	bootstrap_version INTEGER NOT NULL,
	created_at        TEXT NOT NULL
);`

const probeDDL = `CREATE TABLE IF NOT EXISTS probe (id TEXT PRIMARY KEY);`

// testCatalog is the minimal valid catalog: version 1 must create
// storage_metadata, because ownership is settled right after it.
func testCatalog() []Migration {
	return []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "probe", SQL: probeDDL},
	}
}

func testIdentity(seed byte) domain.PeerIdentity {
	var identity domain.PeerIdentity
	for i := range identity {
		identity[i] = seed
	}
	return identity
}

func testConfig(t *testing.T, path string, catalog []Migration) Config {
	t.Helper()
	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	return Config{
		ExplicitPath: path,
		Owner:        testIdentity(0xAB),
		Catalog:      catalog,
		Now:          func() time.Time { return fixed },
	}
}

func openTest(t *testing.T, path string, catalog []Migration) *Database {
	t.Helper()
	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// ledgerNames reads the recorded migration names in version order.
func ledgerNames(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open(DriverName(), DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(`SELECT name FROM schema_migrations ORDER BY version ASC`)
	if err != nil {
		t.Fatalf("query ledger: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan ledger: %v", err)
		}
		names = append(names, name)
	}
	return names
}

func TestOpenFreshDatabaseReachesLatestVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	database := openTest(t, path, testCatalog())

	if got, want := database.SchemaVersion(), Version(2); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}
	if got, want := strings.Join(ledgerNames(t, path), ","), "storage_metadata,probe"; got != want {
		t.Fatalf("ledger = %q, want %q", got, want)
	}
}

func TestReopenAppliesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	first := openTest(t, path, testCatalog())
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()

	if got, want := second.SchemaVersion(), Version(2); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}
	if got, want := len(ledgerNames(t, path)), 2; got != want {
		t.Fatalf("ledger rows = %d, want %d", got, want)
	}

	// The applied_at of version 2 must be the one written by the first run:
	// a re-applied step would rewrite it.
	if got, want := appliedAt(t, path, 2), "2026-08-17T12:00:00Z"; got != want {
		t.Fatalf("applied_at = %q, want %q", got, want)
	}
}

// appliedAt reads the recorded timestamp of one ledger row.
func appliedAt(t *testing.T, path string, version Version) string {
	t.Helper()
	db, err := sql.Open(DriverName(), DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer func() { _ = db.Close() }()

	var recorded string
	if err := db.QueryRow(`SELECT applied_at FROM schema_migrations WHERE version = ?`, int(version)).Scan(&recorded); err != nil {
		t.Fatalf("read applied_at: %v", err)
	}
	return recorded
}

func TestFailedMigrationRollsBackDDLAndLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	failing := append(testCatalog(), Migration{
		Version: 3,
		Name:    "failing",
		SQL:     `CREATE TABLE IF NOT EXISTS half_applied (id TEXT PRIMARY KEY);`,
		Invariant: func(ctx context.Context, tx SchemaReader) error {
			return errors.New("deliberate condition failure")
		},
	})

	if _, err := Open(context.Background(), testConfig(t, path, failing)); err == nil {
		t.Fatal("Open with a failing migration returned no error")
	}

	// Reopening with the healthy catalog must find version 2 and no trace of
	// the failed step's DDL.
	database := openTest(t, path, testCatalog())
	if got, want := database.SchemaVersion(), Version(2); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}

	var name string
	err := database.Executor().QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'half_applied'`).Scan(&name)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("half_applied lookup = %v (%q), want sql.ErrNoRows", err, name)
	}
}

func TestMigrationStopsAtFirstFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	catalog := append(testCatalog(),
		Migration{
			Version: 3,
			Name:    "failing",
			SQL:     `CREATE TABLE IF NOT EXISTS failing_step (id TEXT PRIMARY KEY);`,
			Invariant: func(ctx context.Context, tx SchemaReader) error {
				return errors.New("deliberate condition failure")
			},
		},
		Migration{
			Version:   4,
			Name:      "never_reached",
			SQL:       `CREATE TABLE IF NOT EXISTS never_reached (id TEXT PRIMARY KEY);`,
			Invariant: func(ctx context.Context, tx SchemaReader) error { return nil },
		},
	)

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); err == nil {
		t.Fatal("Open returned no error")
	}

	database := openTest(t, path, testCatalog())
	var name string
	err := database.Executor().QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'never_reached'`).Scan(&name)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("never_reached lookup = %v (%q), want sql.ErrNoRows", err, name)
	}
}

func TestConcurrentOpenAppliesEachVersionOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	// No IF NOT EXISTS: a second execution of the same version fails loudly
	// instead of silently succeeding.
	catalog := append(testCatalog(), Migration{
		Version: 3,
		Name:    "once_only",
		SQL:     `CREATE TABLE once_only (id TEXT PRIMARY KEY);`,
	})

	const openers = 4
	var wg sync.WaitGroup
	errs := make([]error, openers)
	databases := make([]*Database, openers)
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			database, err := Open(context.Background(), testConfig(t, path, catalog))
			databases[index], errs[index] = database, err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("opener %d: %v", i, err)
		}
		if databases[i] != nil {
			_ = databases[i].Close()
		}
	}
	if got, want := len(ledgerNames(t, path)), 3; got != want {
		t.Fatalf("ledger rows = %d, want %d", got, want)
	}
}

func TestOpenRejectsSchemaTooNew(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	current := openTest(t, path, testCatalog())
	if err := current.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	older := testCatalog()[:1]
	_, err := Open(context.Background(), testConfig(t, path, older))
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("error = %v, want ErrSchemaTooNew", err)
	}
}

func TestOpenRejectsChecksumDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	current := openTest(t, path, testCatalog())
	if err := current.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	edited := testCatalog()
	edited[1].SQL += "\n-- an edit to a published migration\n"

	_, err := Open(context.Background(), testConfig(t, path, edited))
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("error = %v, want ErrMigrationDrift", err)
	}
}

func TestOpenRejectsNameDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	current := openTest(t, path, testCatalog())
	if err := current.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	renamed := testCatalog()
	renamed[1].Name = "probe_renamed"

	_, err := Open(context.Background(), testConfig(t, path, renamed))
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("error = %v, want ErrMigrationDrift", err)
	}
}

func TestValidateCatalogRejectsMalformedLists(t *testing.T) {
	noop := func(ctx context.Context, tx SchemaReader) error { return nil }

	cases := map[string][]Migration{
		"empty": {},
		"zero version": {
			{Version: 0, Name: "a", SQL: "SELECT 1;", Invariant: noop},
		},
		"duplicate version": {
			{Version: 1, Name: "a", SQL: "SELECT 1;", Invariant: noop},
			{Version: 1, Name: "b", SQL: "SELECT 1;", Invariant: noop},
		},
		"gap": {
			{Version: 1, Name: "a", SQL: "SELECT 1;", Invariant: noop},
			{Version: 3, Name: "b", SQL: "SELECT 1;", Invariant: noop},
		},
		"out of order": {
			{Version: 2, Name: "a", SQL: "SELECT 1;", Invariant: noop},
			{Version: 1, Name: "b", SQL: "SELECT 1;", Invariant: noop},
		},
		"duplicate name": {
			{Version: 1, Name: "a", SQL: "SELECT 1;", Invariant: noop},
			{Version: 2, Name: "a", SQL: "SELECT 2;", Invariant: noop},
		},
		"empty sql": {
			{Version: 1, Name: "a", SQL: "   ", Invariant: noop},
		},
		"missing verifier": {
			{Version: 1, Name: "a", SQL: "SELECT 1;"},
		},
		"owns transaction": {
			{Version: 1, Name: "a", SQL: "BEGIN; SELECT 1;", Invariant: noop},
		},
	}

	for name, catalog := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
				t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
			}
		})
	}
}

func TestInvalidCatalogIsRejectedBeforeTouchingTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	broken := []Migration{
		{Version: 2, Name: "a", SQL: "SELECT 1;", Invariant: func(ctx context.Context, tx SchemaReader) error { return nil }},
	}
	if _, err := Open(context.Background(), testConfig(t, path, broken)); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("error = %v, want ErrCatalogInvalid", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("database file was created despite an invalid catalog")
	}
}

func TestCancelledContextAbortsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Open(ctx, testConfig(t, path, testCatalog())); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

// openRaw returns a driver-level handle for shaping a file before Open sees it.
func openRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName(), DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpenRejectsAForeignLedgerTable(t *testing.T) {
	// CREATE TABLE IF NOT EXISTS is a no-op against a table that already
	// exists, so a schema_migrations of the wrong shape would be adopted and
	// then trusted as the version history. Every other guarantee in this
	// package reads from that table, so it has to be proven like any other.
	cases := map[string]string{
		"no primary key": `CREATE TABLE schema_migrations (
			version    INTEGER,
			name       TEXT NOT NULL UNIQUE,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
		"name not unique": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
		"nullable checksum": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE,
			checksum   TEXT,
			applied_at TEXT NOT NULL
		)`,
		"unexpected column": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL,
			surprise   TEXT
		)`,
		"missing column": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE,
			checksum   TEXT NOT NULL
		)`,
	}

	for name, ddl := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			if _, err := openRaw(t, path).Exec(ddl); err != nil {
				t.Fatalf("create foreign ledger: %v", err)
			}

			_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestReadLedgerRejectsDuplicateVersions(t *testing.T) {
	// Unreachable through Open — the primary key makes it impossible and
	// ensureLedger proves the key is there. Tested directly because the map
	// readLedger builds would otherwise pick a winner silently, and "the
	// ledger is a verified history" is the contract the whole package rests
	// on: it should fail loudly if a future change ever loosens the table.
	path := filepath.Join(t.TempDir(), "state.db")
	db := openRaw(t, path)
	if _, err := db.Exec(`CREATE TABLE schema_migrations (
		version    INTEGER,
		name       TEXT NOT NULL,
		checksum   TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES
		(1, 'storage_metadata', 'aaa', '2026-08-17T12:00:00Z'),
		(1, 'something_else',   'bbb', '2026-08-17T12:00:01Z')`); err != nil {
		t.Fatalf("insert duplicates: %v", err)
	}

	_, err := readLedger(context.Background(), db)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("error = %v, want ErrMigrationDrift", err)
	}
}

func TestFailedMigrationIsReportedForLogging(t *testing.T) {
	// The operator's only record of a crash-on-upgrade is the startup log, so
	// migrate must hand back what it attempted — the steps that committed AND
	// the one that failed — instead of discarding them with the error.
	path := filepath.Join(t.TempDir(), "state.db")

	catalog := append(testCatalog(), Migration{
		Version: 3,
		Name:    "failing",
		SQL:     `CREATE TABLE IF NOT EXISTS failing_step (id TEXT PRIMARY KEY);`,
		Invariant: func(ctx context.Context, tx SchemaReader) error {
			return fmt.Errorf("%w: deliberate condition failure", ErrSchemaIncompatible)
		},
	})

	db, err := sql.Open(DriverName(), DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer func() { _ = db.Close() }()

	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	attempted, err := migrate(context.Background(), db, migrationRun{
		Catalog: catalog,
		Limit:   LatestVersion(catalog),
		Now:     func() time.Time { return fixed },
	})
	if !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}

	if len(attempted) != 3 {
		t.Fatalf("attempted = %d steps, want 3 (two applied, one failed): %+v", len(attempted), attempted)
	}
	for _, step := range attempted[:2] {
		if step.Result != MigrationApplied {
			t.Fatalf("step %s result = %q, want %q", step.Name, step.Result, MigrationApplied)
		}
	}
	last := attempted[2]
	if last.Name != "failing" || last.Version != 3 || last.Result != MigrationFailed {
		t.Fatalf("last step = %+v, want the failing version 3 reported as failed", last)
	}

	if got, want := errorClass(err), "schema-incompatible"; got != want {
		t.Fatalf("errorClass = %q, want %q", got, want)
	}
}

func TestErrorClassNamesEveryStartupFailure(t *testing.T) {
	// The class is what reaches the log instead of the driver message, which
	// can carry the statement and its bound values.
	cases := map[error]string{
		fmt.Errorf("wrapped: %w", ErrSchemaTooNew):       "schema-too-new",
		fmt.Errorf("wrapped: %w", ErrMigrationDrift):     "migration-drift",
		fmt.Errorf("wrapped: %w", ErrOwnerMismatch):      "owner-mismatch",
		fmt.Errorf("wrapped: %w", ErrForeignApplication): "foreign-application",
		fmt.Errorf("wrapped: %w", ErrCorrupt):            "corrupt",
		fmt.Errorf("wrapped: %w", ErrCatalogInvalid):     "catalog-invalid",
		fmt.Errorf("wrapped: %w", context.Canceled):      "cancelled",
		errors.New("no such column: whatever"):           "sql",
	}
	for err, want := range cases {
		if got := errorClass(err); got != want {
			t.Fatalf("errorClass(%v) = %q, want %q", err, got, want)
		}
	}
	if got, want := errorClass(nil), "none"; got != want {
		t.Fatalf("errorClass(nil) = %q, want %q", got, want)
	}
}

func TestLockWaitFailureIsReportedForLogging(t *testing.T) {
	// The wait for the write lock is part of the step and the part most
	// likely to blow a deadline when another process is mid-upgrade. A
	// failure there must produce the same result=failed record as a failure
	// in the DDL — otherwise the log is silent on exactly the scenario the
	// busy timeout exists for.
	path := filepath.Join(t.TempDir(), "state.db")
	db := openRaw(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	result, err := applyMigration(ctx, db, migrationRun{Catalog: testCatalog(), Now: func() time.Time { return fixed }}, testCatalog()[0])
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if result == nil {
		t.Fatal("a failure before the transaction opened produced no AttemptedMigration — nothing would be logged")
	}
	if result.Result != MigrationFailed || result.Name != "storage_metadata" {
		t.Fatalf("result = %+v, want the storage_metadata step reported as failed", result)
	}
	if got, want := errorClass(err), "cancelled"; got != want {
		t.Fatalf("errorClass = %q, want %q", got, want)
	}
}

func TestOpenRejectsALedgerDefinedDifferently(t *testing.T) {
	// The ledger is compared against the DDL that creates it, as a whole. Any
	// difference at all is a schema this code cannot run on, and expressing
	// that as ONE comparison is what removes the need to hunt down every way a
	// definition can differ — each of these used to need its own check, or had
	// none.
	cases := map[string]string{
		"unique moved to a separate index": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		);
		CREATE UNIQUE INDEX schema_migrations_name ON schema_migrations(name);`,
		"no unique at all": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
		"case-insensitive unique": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE COLLATE NOCASE,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
		"extra generated column": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL,
			shadow     TEXT GENERATED ALWAYS AS (name) VIRTUAL
		)`,
		"extra foreign key": `CREATE TABLE parent (id TEXT PRIMARY KEY);
		CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE REFERENCES parent(id),
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
	}

	for name, ddl := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			if _, err := openRaw(t, path).Exec(ddl); err != nil {
				t.Fatalf("create ledger: %v", err)
			}

			_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestOpenAcceptsAReformattedLedger(t *testing.T) {
	// Layout, comments and whether the author wrote IF NOT EXISTS are the only
	// things normalised away — a schema identical in substance must still open.
	path := filepath.Join(t.TempDir(), "state.db")
	if _, err := openRaw(t, path).Exec(
		"CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, /* the step */ name TEXT NOT NULL UNIQUE," +
			"   checksum TEXT NOT NULL,\n\n applied_at   TEXT NOT NULL)"); err != nil {
		t.Fatalf("create ledger: %v", err)
	}

	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
}

func TestOpenRejectsALedgerWithAConflictPolicy(t *testing.T) {
	// index_list and index_xinfo report a UNIQUE identically whichever
	// ON CONFLICT policy declared it, so the structural check cannot see
	// this. IGNORE makes the ledger INSERT succeed without writing a row;
	// REPLACE lets it delete one.
	cases := map[string]string{
		"ignore": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE ON CONFLICT IGNORE,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
		"replace": `CREATE TABLE schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE ON CONFLICT REPLACE,
			checksum   TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)`,
	}

	for name, ddl := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			if _, err := openRaw(t, path).Exec(ddl); err != nil {
				t.Fatalf("create ledger: %v", err)
			}

			_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestOpenRejectsALedgerWithTriggers(t *testing.T) {
	// A trigger sits between the runner's INSERT and the read-back that
	// confirms it, free to rewrite or delete the row — or to touch anything
	// else. No schema of ours puts one on the ledger.
	path := filepath.Join(t.TempDir(), "state.db")
	db := openRaw(t, path)
	if _, err := db.Exec(migrationLedgerDDL); err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if _, err := db.Exec(`CREATE TRIGGER drop_ledger_rows
		AFTER INSERT ON schema_migrations
		BEGIN DELETE FROM schema_migrations WHERE version = NEW.version; END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestMigrationFailsWhenTheLedgerRowDoesNotSurvive(t *testing.T) {
	// The defence that does not depend on spotting the trigger first.
	// RowsAffected counts what the INSERT touched, so an AFTER INSERT trigger
	// that deletes the row still reports one affected row; only reading the
	// row back inside the transaction settles it. applyMigration is called
	// directly here because ensureLedger would reject the trigger earlier.
	cases := map[string]string{
		"row deleted": `CREATE TRIGGER t AFTER INSERT ON schema_migrations
			BEGIN DELETE FROM schema_migrations WHERE version = NEW.version; END`,
		"row rewritten": `CREATE TRIGGER t AFTER INSERT ON schema_migrations
			BEGIN UPDATE schema_migrations SET name = 'something_else' WHERE version = NEW.version; END`,
		"insert swallowed": `CREATE TRIGGER t BEFORE INSERT ON schema_migrations
			BEGIN SELECT RAISE(IGNORE); END`,
	}

	for name, trigger := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			db := openRaw(t, path)
			if _, err := db.Exec(migrationLedgerDDL); err != nil {
				t.Fatalf("create ledger: %v", err)
			}
			if _, err := db.Exec(trigger); err != nil {
				t.Fatalf("create trigger: %v", err)
			}

			fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
			result, err := applyMigration(context.Background(), db, migrationRun{Catalog: testCatalog(), Now: func() time.Time { return fixed }}, testCatalog()[0])
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
			if result == nil || result.Result != MigrationFailed {
				t.Fatalf("result = %+v, want the step reported as failed", result)
			}

			// And the DDL rolled back with it.
			var table string
			err = db.QueryRow(
				`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'storage_metadata'`).Scan(&table)
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("storage_metadata lookup = %v (%q), want sql.ErrNoRows", err, table)
			}
		})
	}
}

func TestOpenRejectsAConflictPolicyHiddenInAComment(t *testing.T) {
	// SQLite accepts and stores this, and a substring search over the raw
	// text walks straight past it.
	path := filepath.Join(t.TempDir(), "state.db")
	if _, err := openRaw(t, path).Exec(`CREATE TABLE schema_migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT NOT NULL UNIQUE ON/**/CONFLICT IGNORE,
		checksum   TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create ledger: %v", err)
	}

	_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestOpenRejectsAConflictPolicyHiddenBehindALiteral(t *testing.T) {
	// The quote-blind scanner read the '/*' in the CHECK as an unterminated
	// comment and dropped the rest of the DDL, taking the policy with it.
	path := filepath.Join(t.TempDir(), "state.db")
	if _, err := openRaw(t, path).Exec(`CREATE TABLE schema_migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT NOT NULL CHECK(name <> '/*') UNIQUE ON CONFLICT IGNORE,
		checksum   TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create ledger: %v", err)
	}

	_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestMigrationFailsWhenOnlyTheTimestampIsRewritten(t *testing.T) {
	// The read-back compares every written value. A trigger that touches only
	// applied_at still means something altered the row between the INSERT and
	// the COMMIT, and a partial comparison would wave it through.
	path := filepath.Join(t.TempDir(), "state.db")
	db := openRaw(t, path)
	if _, err := db.Exec(migrationLedgerDDL); err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if _, err := db.Exec(`CREATE TRIGGER t AFTER INSERT ON schema_migrations
		BEGIN UPDATE schema_migrations SET applied_at = 'rewritten' WHERE version = NEW.version; END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	result, err := applyMigration(context.Background(), db, migrationRun{Catalog: testCatalog(), Now: func() time.Time { return fixed }}, testCatalog()[0])
	if !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
	if result == nil || result.Result != MigrationFailed {
		t.Fatalf("result = %+v, want the step reported as failed", result)
	}
}
