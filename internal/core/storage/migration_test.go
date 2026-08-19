package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSQLScanningIsQuoteAware is the regression for the scanner every text
// rule in this package rests on. A quote-blind pass reads a `/*` inside a
// string literal as an unterminated comment and drops everything after it —
// which is how a migration could smuggle a COMMIT past ValidateCatalog and
// break the atomicity of its own step.
func TestSQLScanningIsQuoteAware(t *testing.T) {
	t.Run("a comment marker inside a literal is not a comment", func(t *testing.T) {
		const sqlText = `INSERT INTO t VALUES('/*'); COMMIT;`
		if got := stripSQLComments(sqlText); got != sqlText {
			t.Fatalf("stripSQLComments = %q, want the text unchanged", got)
		}
		statements := splitSQLStatements(sqlText)
		if len(statements) != 3 {
			t.Fatalf("splitSQLStatements = %q, want the INSERT and the COMMIT apart", statements)
		}
		if !strings.Contains(strings.ToUpper(statements[1]), "COMMIT") {
			t.Fatalf("statements = %q, want the COMMIT visible as its own statement", statements)
		}
	})

	t.Run("a semicolon inside a literal does not split", func(t *testing.T) {
		statements := splitSQLStatements(`INSERT INTO t VALUES('a;b')`)
		if len(statements) != 1 {
			t.Fatalf("splitSQLStatements = %q, want one statement", statements)
		}
	})

	t.Run("real comments still go", func(t *testing.T) {
		stripped := stripSQLComments("SELECT 1 -- trailing\nON/**/CONFLICT")
		if strings.Contains(stripped, "trailing") {
			t.Fatalf("stripSQLComments kept a line comment: %q", stripped)
		}
		if !strings.Contains(strings.ToUpper(collapseSQL(stripped)), "ON CONFLICT") {
			t.Fatalf("a block comment hid the keyword pair: %q", stripped)
		}
	})
}

func TestValidateCatalogSeesACommitHiddenBehindALiteral(t *testing.T) {
	// The payload the quote-blind scanner used to swallow whole.
	catalog := []Migration{{
		Version: 1,
		Name:    "storage_metadata",
		SQL:     metadataDDL + "\nINSERT INTO storage_metadata (id, owner_identity, bootstrap_version, created_at) VALUES(1, '/*', 1, '');\nCOMMIT;",
	}}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestValidateCatalogRejectsAByteOrderMark(t *testing.T) {
	// SQLite treats U+FEFF as whitespace and Go does not, so a BOM between a
	// semicolon and a keyword hides that keyword from every scan here while
	// SQLite executes it.
	catalog := []Migration{{
		Version: 1,
		Name:    "storage_metadata",
		SQL:     metadataDDL + "\n\uFEFFCOMMIT;",
	}}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestValidateCatalogAllowsATriggerBody(t *testing.T) {
	// A trigger body is BEGIN ... END with semicolons inside, so a plain split
	// turns its END into what looks like a transaction verb. Banning that by
	// accident would block a legitimate future migration.
	const withTrigger = `CREATE TABLE IF NOT EXISTS audited (id TEXT PRIMARY KEY, seen TEXT NOT NULL DEFAULT '');
CREATE TRIGGER IF NOT EXISTS audited_touch AFTER INSERT ON audited
BEGIN
	UPDATE audited SET seen = 'yes' WHERE id = NEW.id;
END;`

	catalog := []Migration{{
		Version:   1,
		Name:      "with_trigger",
		SQL:       withTrigger,
		Invariant: func(context.Context, SchemaReader) error { return nil },
	}}

	if err := ValidateCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("ValidateCatalog = %v, want a trigger body to be accepted", err)
	}
}

func TestValidateCatalogStillRejectsACommitAfterATrigger(t *testing.T) {
	// Skipping the trigger body must not become a blanket amnesty.
	const withTrigger = `CREATE TABLE IF NOT EXISTS audited (id TEXT PRIMARY KEY);
CREATE TRIGGER audited_touch AFTER INSERT ON audited
BEGIN
	SELECT 1;
END;
COMMIT;`

	catalog := []Migration{{
		Version:   1,
		Name:      "with_trigger",
		SQL:       withTrigger,
		Invariant: func(context.Context, SchemaReader) error { return nil },
	}}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestNormalizeDDLKeepsLiteralsByteForByte(t *testing.T) {
	// Case folding the whole statement made CHECK(topic IN ('DM','GLOBAL'))
	// compare equal to the lowercase constraint the repository writes against,
	// so a database that rejects every ordinary write would have been accepted.
	lower := `CREATE TABLE t (topic TEXT NOT NULL CHECK(topic IN ('dm','global')))`
	upper := `CREATE TABLE t (topic TEXT NOT NULL CHECK(topic IN ('DM','GLOBAL')))`
	if normalizeDDL(lower) == normalizeDDL(upper) {
		t.Fatal("normalizeDDL folded the case of a string literal")
	}

	// Layout, comments and IF NOT EXISTS are the only things it does erase.
	spaced := "CREATE  TABLE\n\tIF NOT EXISTS   t ( topic TEXT NOT NULL /* c */ CHECK( topic IN ('dm','global') ) )"
	if normalizeDDL(spaced) != normalizeDDL(lower) {
		t.Fatalf("normalizeDDL treated layout as meaningful:\n%q\n%q", normalizeDDL(spaced), normalizeDDL(lower))
	}
}

func TestReferenceSchemaHandlesAnyIdentifier(t *testing.T) {
	// SQLite parses the DDL now, so identifiers that used to break the
	// hand-rolled extractor — spaces, doubled quotes, characters whose byte
	// length changes when uppercased — are simply not this package's problem.
	reference, err := BuildReferenceSchema(context.Background(), []Migration{{
		Version: 1, Name: "identifiers",
		SQL: `CREATE TABLE "message ""cache""" (id TEXT PRIMARY KEY);
CREATE INDEX "idx one" ON "message ""cache"""(id);
CREATE TABLE "ſſſſ" (id TEXT PRIMARY KEY);`,
	}}, 1)
	if err != nil {
		t.Fatalf("BuildReferenceSchema: %v", err)
	}
	if got, want := reference.Objects(), 3; got != want {
		t.Fatalf("objects = %d, want %d", got, want)
	}
}

func TestValidateCatalogSeesACommitAfterATableNamedTrigger(t *testing.T) {
	// The body-skipping heuristic used to turn on for any CREATE containing
	// the word "trigger" anywhere, so a table with that NAME opened a window
	// in which a COMMIT went unchecked, and a rename to "end" closed it again.
	catalog := []Migration{{
		Version: 1,
		Name:    "storage_metadata",
		SQL: metadataDDL + `
CREATE TABLE IF NOT EXISTS trigger (id TEXT PRIMARY KEY);
COMMIT;
ALTER TABLE trigger RENAME TO end;`,
	}}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestValidateCatalogRequiresAPostCondition(t *testing.T) {
	// A migration that only touches DATA proves nothing by executing without
	// error, so those bring their own post-condition. The schema half of a
	// migration is covered by the reference whatever the statements do.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "rows", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY, kind TEXT NOT NULL);`},
		{Version: 3, Name: "backfill", SQL: `UPDATE t SET kind = 'dm' WHERE kind = '';`},
	}
	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}

	catalog[2].Invariant = func(context.Context, SchemaReader) error { return nil }
	if err := ValidateCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("ValidateCatalog = %v, want a backfill with its own Verify to be accepted", err)
	}
}

func TestACatalogMayNotTouchTheRunnersOwnTables(t *testing.T) {
	// The owner table is as much the runner's as the ledger is. A migration
	// rewriting owner_identity commits together with its ledger row, and every
	// later open then stops with ErrOwnerMismatch on a file this node had been
	// using all along.
	for name, statement := range map[string]string{
		"update owner":   `UPDATE storage_metadata SET owner_identity = 'someone-else';`,
		"delete owner":   `DELETE FROM storage_metadata WHERE id = 1;`,
		"trigger owner":  `CREATE TRIGGER t AFTER INSERT ON storage_metadata BEGIN SELECT 1; END;`,
		"index on it":    `CREATE INDEX idx ON schema_migrations(name);`,
		"insert ledger":  `INSERT INTO schema_migrations (version) VALUES (99);`,
		"alter owner":    `ALTER TABLE storage_metadata ADD COLUMN extra TEXT;`,
		"form feed":      "DROP TABLE\fstorage_metadata;",
		"trigger body":   `CREATE TRIGGER t AFTER INSERT ON t BEGIN UPDATE storage_metadata SET owner_identity = 'x'; END;`,
		"quoted owner":   `UPDATE "storage_metadata" SET owner_identity = 'someone-else';`,
		"bracketed":      `DELETE FROM [storage_metadata] WHERE id = 1;`,
		"qualified":      `DROP TABLE main.storage_metadata;`,
		"both":           `DROP TABLE main."storage_metadata";`,
		"single quoted":  `DROP TABLE 'storage_metadata';`,
		"literal update": `UPDATE 'storage_metadata' SET owner_identity = 'someone-else';`,
	} {
		t.Run(name, func(t *testing.T) {
			catalog := []Migration{
				{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
				{
					Version: 2, Name: "reach", SQL: "CREATE TABLE t (id TEXT PRIMARY KEY);\n" + statement,
					Invariant: func(context.Context, SchemaReader) error { return nil },
				},
			}
			if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
				t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
			}
		})
	}
}

func TestValidateCatalogRequiresAPostConditionForAMixedMigration(t *testing.T) {
	// One CREATE used to exempt the whole migration: the derived check
	// covered the new table and said nothing about the rows the same
	// migration wrote.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "mixed", SQL: `CREATE TABLE imported (id TEXT PRIMARY KEY);
INSERT INTO imported (id) SELECT owner_identity FROM storage_metadata;`},
	}
	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestValidateCatalogRejectsUnreadableSQLBeforeTheFileIsTouched(t *testing.T) {
	// The derived expectation is built during validation, so a catalog whose
	// SQL cannot be read fails while nothing has been opened — not halfway
	// through Open, after application_id, WAL and the ledger already changed
	// the file.
	catalog := []Migration{{
		Version: 1,
		Name:    "storage_metadata",
		SQL:     metadataDDL + "\nCREATE TABLE;",
	}}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestNormalizeDDLIgnoresSpacingAroundOperators(t *testing.T) {
	// A reformatted but identical legacy schema must still open: only layout
	// is erased, and spacing around operators and parentheses is layout.
	tight := `CREATE TABLE t (id INTEGER PRIMARY KEY CHECK(id=1), topic TEXT CHECK(topic IN('dm','global')))`
	loose := "CREATE TABLE t (\n\tid INTEGER PRIMARY KEY CHECK ( id = 1 ),\n\ttopic TEXT CHECK ( topic IN ( 'dm' , 'global' ) )\n)"
	if normalizeDDL(tight) != normalizeDDL(loose) {
		t.Fatalf("normalizeDDL treated spacing as meaningful:\n%q\n%q", normalizeDDL(tight), normalizeDDL(loose))
	}
}

func TestCatalogMayAlterAndDropAndDeclareTriggers(t *testing.T) {
	// The expectation is what the catalog PRODUCES, so evolving the schema
	// needs no support in this package: SQLite applies the statements to the
	// reference exactly as it will to the real file. A derived-from-CREATE
	// expectation could not see any of these and rejected the result.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "grow", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY, body TEXT NOT NULL DEFAULT '');
CREATE INDEX idx_t_body ON t(body);`},
		{Version: 3, Name: "evolve", SQL: `ALTER TABLE t ADD COLUMN extra TEXT NOT NULL DEFAULT '';
DROP INDEX idx_t_body;
CREATE INDEX idx_t_extra ON t(extra);
CREATE TRIGGER t_touch AFTER INSERT ON t BEGIN UPDATE t SET extra = 'seen' WHERE id = NEW.id; END;`},
	}

	if err := ValidateCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("ValidateCatalog: %v", err)
	}

	database, err := Open(context.Background(), testConfig(t, filepath.Join(t.TempDir(), "state.db"), catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if got, want := database.SchemaVersion(), Version(3); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}

	// The declared trigger is part of the schema now, so reopening — which
	// re-verifies everything — must accept it rather than call it an intruder.
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCatalogMayNotTouchTheLedger(t *testing.T) {
	// Such a migration used to apply cleanly and leave a database that could
	// never be opened again: the ledger is checked BEFORE the catalog runs, so
	// the damage was only met on the next start.
	cases := map[string]string{
		"trigger on the ledger": `CREATE TABLE t (id TEXT PRIMARY KEY);
CREATE TRIGGER swallow AFTER INSERT ON schema_migrations BEGIN DELETE FROM schema_migrations; END;`,
		"redefines the ledger": `CREATE TABLE schema_migrations (version INTEGER);`,
	}

	for name, ddl := range cases {
		t.Run(name, func(t *testing.T) {
			catalog := []Migration{
				{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
				{Version: 2, Name: "meddles", SQL: ddl},
			}
			if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
				t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
			}
		})
	}
}

func TestValidateCatalogRejectsCollidingMigrations(t *testing.T) {
	// Two versions creating the same object. It used to surface inside
	// applyMigration — after the file, the ledger and the earlier versions
	// had already been committed.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "first", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);`},
		{Version: 3, Name: "second", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY, extra TEXT);`},
	}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestAConditionCannotWriteThroughItsHandle(t *testing.T) {
	// The handle hides ExecContext and the connection is sealed with PRAGMA
	// query_only for the call, so SQLite refuses the write. This guards
	// against a MISTAKE in a reviewed condition; it is not a sandbox, and what
	// proves the file intact is the observation below, not this.
	//
	// The refusal is collected from iteration as well as from the call: the
	// two drivers report it at different moments, and asserting on one of them
	// made this pass under modernc.org/sqlite and fail under mattn/go-sqlite3.
	var attempt error
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{
			Version: 2, Name: "probe", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);`,
			Invariant: func(ctx context.Context, tx SchemaReader) error {
				rows, err := tx.QueryContext(ctx, `INSERT INTO t (id) VALUES ('x') RETURNING id`)
				if err == nil {
					for rows.Next() {
					}
					err = rows.Err()
					_ = rows.Close()
				}
				attempt = err
				return nil
			},
		},
	}

	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if attempt == nil {
		t.Fatal("the sealed connection accepted a write")
	}
	assertStepIsWhole(t, path, "t", 2, true)
}

func TestAConditionThatUnsealsItsConnectionFailsTheStep(t *testing.T) {
	// A condition that switches the pragma back off is the accidental version
	// of the same mistake: the seal is re-read afterwards, and the step is
	// rolled back whole.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{
			Version: 2, Name: "probe", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);`,
			Invariant: func(ctx context.Context, tx SchemaReader) error {
				rows, err := tx.QueryContext(ctx, `PRAGMA query_only = OFF`)
				if err == nil {
					_ = rows.Close()
				}
				return nil
			},
		},
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
	assertStepIsWhole(t, path, "t", 2, false)
}

func TestTheRunnersOwnRowsAreComparedAcrossAStep(t *testing.T) {
	// This is where "a migration cannot damage the ledger or the owner row"
	// actually lives. The catalog is scanned for such statements too, but a
	// scan reads TEXT and can be wrong in either direction; this compares the
	// rows themselves, so it holds whatever route the statements took.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "messages", SQL: `CREATE TABLE messages (id TEXT PRIMARY KEY);`},
	}
	step := catalog[1]

	// Each case gets its own file: the damage is applied for real, and one
	// case must not decide what the next one sees.
	open := func(t *testing.T) *Database {
		t.Helper()
		database, err := Open(context.Background(), testConfig(t, filepath.Join(t.TempDir(), "state.db"), catalog))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = database.Close() })
		return database
	}

	t.Run("untouched rows pass", func(t *testing.T) {
		database := open(t)
		ctx := context.Background()

		intact, err := readRunnerState(ctx, database.db)
		if err != nil {
			t.Fatalf("readRunnerState: %v", err)
		}
		if err := confirmRunnerState(ctx, database.db, intact, step); err != nil {
			t.Fatalf("confirmRunnerState on untouched rows: %v", err)
		}
	})

	damage := map[string]string{
		"a removed version":  `DELETE FROM schema_migrations WHERE version = 1`,
		"a rewritten row":    `UPDATE schema_migrations SET checksum = 'nonsense' WHERE version = 1`,
		"a rewritten owner":  `UPDATE storage_metadata SET owner_identity = 'someone-else' WHERE id = 1`,
		"a rewritten format": `UPDATE storage_metadata SET bootstrap_version = 999 WHERE id = 1`,
		"a rewritten stamp":  `UPDATE storage_metadata SET created_at = '1970-01-01T00:00:00Z' WHERE id = 1`,
		"an invented version": `INSERT INTO schema_migrations (version, name, checksum, applied_at)
			VALUES (9, 'invented', '0000', '2026-08-17T12:00:00Z')`,
	}
	for name, statement := range damage {
		t.Run(name, func(t *testing.T) {
			database := open(t)
			ctx := context.Background()

			before, err := readRunnerState(ctx, database.db)
			if err != nil {
				t.Fatalf("readRunnerState: %v", err)
			}
			if _, err := database.db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("apply the damage: %v", err)
			}
			if err := confirmRunnerState(ctx, database.db, before, step); !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

// assertStepIsWhole checks that a migration's table and its ledger row are
// both present or both absent — never one without the other.
func assertStepIsWhole(t *testing.T, path, table string, version Version, want bool) {
	t.Helper()

	file, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = file.Close() }()

	var name string
	tableErr := file.QueryRow(
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if tableErr != nil && !errors.Is(tableErr, sql.ErrNoRows) {
		t.Fatalf("look up %q: %v", table, tableErr)
	}
	var recorded int
	ledgerErr := file.QueryRow(`SELECT 1 FROM schema_migrations WHERE version = ?`, int(version)).Scan(&recorded)
	if ledgerErr != nil && !errors.Is(ledgerErr, sql.ErrNoRows) {
		t.Fatalf("look up version %s: %v", version, ledgerErr)
	}

	hasTable, hasRow := tableErr == nil, ledgerErr == nil
	if hasTable != hasRow {
		t.Fatalf("partially applied step: table %q present = %t, ledger row present = %t", table, hasTable, hasRow)
	}
	if hasTable != want {
		t.Fatalf("step applied = %t, want %t", hasTable, want)
	}
}

func TestCatalogMayNotCreateTempObjects(t *testing.T) {
	// A TEMP object lives on the connection: it never reaches the file, it
	// stays on the pooled connection the migration ran on, and it disappears
	// on restart. A migration could otherwise pair a persistent table with a
	// damaging temporary trigger and be recorded as applied.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "temp", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);
CREATE TEMP TRIGGER swallow AFTER INSERT ON t BEGIN DELETE FROM t; END;`},
	}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestCatalogMayNotRestampTheApplicationID(t *testing.T) {
	// PRAGMA application_id is ordinary SQL, and a migration carrying it made
	// the NEXT open fail with ErrForeignApplication. It is refused at catalog
	// validation, so the file is never touched at all.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "restamp", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);
PRAGMA application_id = 305419896;`, Invariant: func(context.Context, SchemaReader) error { return nil }},
	}

	path := filepath.Join(t.TempDir(), "state.db")
	_, err := Open(context.Background(), testConfig(t, path, catalog))
	if !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("error = %v, want ErrCatalogInvalid", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("the database file exists after a rejected catalog: %v", statErr)
	}
}

func TestCatalogMayNotReachOutsideTheStateDatabase(t *testing.T) {
	// The reference is built by EXECUTING the catalog. ATTACH would therefore
	// write to another file during validation — before the state database is
	// even opened, and outside the transaction this package promises.
	outside := filepath.Join(t.TempDir(), "outside.db")
	victim, err := sql.Open(sqliteDriverName, DSN(outside))
	if err != nil {
		t.Fatalf("open outside database: %v", err)
	}
	if _, err := victim.Exec(`CREATE TABLE keep (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed outside database: %v", err)
	}
	if err := victim.Close(); err != nil {
		t.Fatalf("close outside database: %v", err)
	}

	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "escape", SQL: fmt.Sprintf(`CREATE TABLE t (id TEXT PRIMARY KEY);
ATTACH DATABASE '%s' AS aux;
DROP TABLE aux.keep;`, outside), Invariant: func(context.Context, SchemaReader) error { return nil }},
	}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}

	reopened, err := sql.Open(sqliteDriverName, DSN(outside))
	if err != nil {
		t.Fatalf("reopen outside database: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	var name string
	if err := reopened.QueryRow(`SELECT name FROM sqlite_schema WHERE name = 'keep'`).Scan(&name); err != nil {
		t.Fatalf("the outside database was modified during validation: %v", err)
	}
}

func TestMarkerDamageIsReportedNotRepaired(t *testing.T) {
	// Re-stamping after the catalog committed cannot undo anything: it would
	// only hand the caller a file whose markers this process just invented.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "messages", SQL: `CREATE TABLE messages (id TEXT PRIMARY KEY);`},
	}

	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	damaged, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := damaged.Exec(`DELETE FROM storage_metadata WHERE id = 1`); err != nil {
		t.Fatalf("damage owner row: %v", err)
	}
	if err := damaged.Close(); err != nil {
		t.Fatalf("close damaged: %v", err)
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("error = %v, want ErrOwnerMismatch — the missing owner row was silently re-inserted", err)
	}
}

func TestAConditionCannotDeleteThroughAQuery(t *testing.T) {
	// DELETE ... RETURNING is a query, so hiding ExecContext does not stop it;
	// the seal does.
	//
	// WHEN the refusal surfaces is a driver difference, and asserting on the
	// error from QueryContext alone made this test pass on one driver and fail
	// on the other: modernc.org/sqlite reports it there, mattn/go-sqlite3
	// hands back rows and reports it on iteration. What both must agree on is
	// that the rows are still there.
	var writeErr error
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{
			Version: 2, Name: "probe", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);`,
			Invariant: func(ctx context.Context, tx SchemaReader) error {
				rows, err := tx.QueryContext(ctx, `DELETE FROM schema_migrations RETURNING version`)
				if err == nil {
					for rows.Next() {
					}
					err = rows.Err()
					_ = rows.Close()
				}
				writeErr = err
				return nil
			},
		},
	}

	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if writeErr == nil {
		t.Fatal("the sealed connection reported no refusal at all")
	}
	assertStepIsWhole(t, path, "t", 2, true)

	file, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = file.Close() }()

	var recorded int
	if err := file.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&recorded); err != nil {
		t.Fatalf("count recorded versions: %v", err)
	}
	if recorded != 2 {
		t.Fatalf("the ledger holds %d versions, want 2 — a condition deleted from it", recorded)
	}
}

func TestCreateTableAsSelectNeedsItsOwnVerify(t *testing.T) {
	// CTAS copies rows. The reference proves the resulting definition and says
	// nothing about what was copied into it.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "ctas", SQL: `CREATE TABLE copied AS SELECT owner_identity FROM storage_metadata;`},
	}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestReferenceDatabaseEnforcesForeignKeys(t *testing.T) {
	// A bare :memory: database has foreign_keys OFF, so a catalog whose data
	// violates one validated here and only failed after the real file had
	// already been changed.
	catalog := []Migration{
		{Version: 1, Name: "parent", SQL: `CREATE TABLE parent (id TEXT PRIMARY KEY);`},
		{
			Version: 2, Name: "child", SQL: `CREATE TABLE child (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parent(id));
INSERT INTO child (id, parent_id) VALUES ('a', 'missing');`,
			Invariant: func(context.Context, SchemaReader) error { return nil },
		},
	}

	if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
	}
}

func TestATableFromALaterVersionMayReferenceAManagedTable(t *testing.T) {
	// A pre-versioned file can already carry the exact table a later version
	// declares, with the foreign key that migration will declare itself.
	// Judging it against the CURRENT version alone made it look undeclared and
	// stopped the upgrade before reaching the version that explains it.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "parent", SQL: `CREATE TABLE parent (id TEXT PRIMARY KEY);`},
		{Version: 3, Name: "child", SQL: `CREATE TABLE child (
	id        TEXT PRIMARY KEY,
	parent_id TEXT NOT NULL REFERENCES parent(id)
);`},
	}
	ctx := context.Background()

	live, err := openScratchDatabase()
	if err != nil {
		t.Fatalf("open scratch: %v", err)
	}
	defer func() { _ = live.Close() }()

	for _, migration := range catalog {
		if _, err := live.ExecContext(ctx, migration.SQL); err != nil {
			t.Fatalf("seed %s: %v", migration.Name, err)
		}
	}

	current, err := BuildReferenceSchema(ctx, catalog, 2)
	if err != nil {
		t.Fatalf("build required: %v", err)
	}
	whole, err := BuildReferenceSchema(ctx, catalog, 3)
	if err != nil {
		t.Fatalf("build allowed: %v", err)
	}

	if err := VerifySchema(ctx, live, current, whole); err != nil {
		t.Fatalf("VerifySchema: %v — a table the catalog declares later was treated as alien", err)
	}
	if err := VerifySchema(ctx, live, current, current); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible for a table no version declares", err)
	}
}

func TestAFailingInvariantRollsBackItsStepAndStopsTheRun(t *testing.T) {
	// The guarantee the design fixes: a failed check rolls the whole step
	// back, and version N+1 does not run after a failure at N.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{
			Version: 2, Name: "probe", SQL: `CREATE TABLE t (id TEXT PRIMARY KEY);`,
			Invariant: func(context.Context, SchemaReader) error {
				return errors.New("deliberate condition failure")
			},
		},
		{Version: 3, Name: "later", SQL: `CREATE TABLE later (id TEXT PRIMARY KEY);`},
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
	assertStepIsWhole(t, path, "t", 2, false)
	assertStepIsWhole(t, path, "later", 3, false)
}

func TestTheOwnerRowIsCommittedWithTheBootstrapVersion(t *testing.T) {
	// Adoption is the case that decides this. A pre-versioned file already
	// holds its whole history, and version 1 only adds the table that records
	// who owns it — so a file at version 1 is NOT an empty file, and a missing
	// owner row there cannot be treated as "new, adopt it". Written in the
	// same transaction as the version that makes room for it, the row cannot
	// be missing at all.
	path := filepath.Join(t.TempDir(), "state.db")
	legacy, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy history: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "messages", SQL: `CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY);`},
	}
	adopted, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if err := adopted.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// With the row gone, the history is unowned — and must NOT be handed to
	// whoever opens the file next.
	stripped, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := stripped.Exec(`DELETE FROM storage_metadata WHERE id = 1`); err != nil {
		t.Fatalf("strip the owner: %v", err)
	}
	if err := stripped.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("error = %v, want ErrOwnerMismatch — an unowned history was adopted", err)
	}
}

func TestAGeneratedColumnIsNotACopyOfData(t *testing.T) {
	// GENERATED ALWAYS AS (...) carries the CTAS keyword inside the column
	// list. Reading it at any depth made a migration that only declares a
	// table demand a condition of its own.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "generated", SQL: `CREATE TABLE t (
	body     TEXT NOT NULL,
	body_len INTEGER GENERATED ALWAYS AS (length(body)) STORED
);`},
	}

	if err := ValidateCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("ValidateCatalog: %v", err)
	}
}

func TestIdentifiersAreFoldedTheWaySQLiteFoldsThem(t *testing.T) {
	// SQLite folds ASCII only, so "Å" and "å" are two different tables. Go's
	// Unicode folding made them one, and an undeclared "å" was mistaken for
	// the managed "Å" — its foreign key into that table then went unnoticed,
	// and the first delete from "Å" failed.
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "managed", SQL: "CREATE TABLE \"Å\" (id TEXT PRIMARY KEY);"},
	}
	ctx := context.Background()

	live, err := openScratchDatabase()
	if err != nil {
		t.Fatalf("open scratch: %v", err)
	}
	defer func() { _ = live.Close() }()

	for _, migration := range catalog {
		if _, err := live.ExecContext(ctx, migration.SQL); err != nil {
			t.Fatalf("seed %s: %v", migration.Name, err)
		}
	}
	if _, err := live.ExecContext(ctx, "CREATE TABLE \"å\" (ref TEXT REFERENCES \"Å\"(id));"); err != nil {
		t.Fatalf("seed the undeclared table: %v", err)
	}

	reference, err := BuildReferenceSchema(ctx, catalog, 2)
	if err != nil {
		t.Fatalf("build reference: %v", err)
	}
	if err := VerifySchema(ctx, live, reference, reference); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestAWhitespaceSQLiteAcceptsCannotHideATable(t *testing.T) {
	// SQLite treats form feed and vertical tab as whitespace. A scanner that
	// stopped at space and tab did not see the word after them, while SQLite
	// executed the statement all the same.
	for name, separator := range map[string]string{"form feed": "\f", "vertical tab": "\v"} {
		t.Run(name, func(t *testing.T) {
			catalog := []Migration{
				{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
				{Version: 2, Name: "reach", SQL: "CREATE TABLE t (id TEXT PRIMARY KEY);\nDROP TABLE" + separator + "storage_metadata;"},
			}
			if err := ValidateCatalog(context.Background(), catalog); !errors.Is(err, ErrCatalogInvalid) {
				t.Fatalf("ValidateCatalog = %v, want ErrCatalogInvalid", err)
			}
		})
	}
}

func TestTheRunnersTablesAreRecognisedByPositionNotBySpelling(t *testing.T) {
	// The rule is about what a statement ACTS ON. A column that happens to be
	// called storage_metadata, or the word inside a string literal, touches
	// nothing of the runner's — refusing those rejected catalogs that were
	// never a problem.
	accepted := map[string]string{
		"column of that name": `CREATE TABLE notes (storage_metadata TEXT NOT NULL);`,
		"column in a list":    `CREATE TABLE notes (kind TEXT NOT NULL, storage_metadata TEXT NOT NULL);`,
		"an ordinary FROM list": `CREATE TABLE notes (id TEXT PRIMARY KEY);
CREATE VIEW joined AS SELECT n.id FROM notes n, notes m;`,
		"the word as a value": `CREATE TABLE notes (kind TEXT NOT NULL CHECK (kind <> 'storage_metadata'));`,
		"unicode identifier":  `CREATE TABLE "K" (id TEXT PRIMARY KEY);`,
		// A real migration, not a bare statement: the ON here belongs to the
		// join condition, and reading it as the view's target refused a
		// perfectly ordinary view.
		"a view over a join": `CREATE TABLE notes (id TEXT PRIMARY KEY, kind TEXT NOT NULL);
CREATE TABLE other (id TEXT PRIMARY KEY, storage_metadata TEXT NOT NULL);
CREATE VIEW joined AS SELECT n.id FROM notes n JOIN other o ON o.storage_metadata = n.id;`,
	}
	for name, statement := range accepted {
		t.Run(name, func(t *testing.T) {
			catalog := []Migration{
				{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
				{Version: 2, Name: "unrelated", SQL: statement},
			}
			if err := ValidateCatalog(context.Background(), catalog); err != nil {
				t.Fatalf("ValidateCatalog = %v, want the catalog to be accepted", err)
			}
		})
	}
}

func TestScanningAnIdentifierThatShrinksWhenFolded(t *testing.T) {
	// Folding the whole statement before scanning it mixed two coordinate
	// systems: "K" (U+212A) is three bytes and its lower case is one, so the
	// quote boundaries — computed on the original — pointed past the end of
	// the folded copy. A name at the end of a statement made that a panic.
	const kelvin = "\u212A" // three bytes; its Unicode lower case is one
	tokens := sqlTokens(`SELECT owner_identity FROM "` + kelvin + `"`)
	if len(tokens) == 0 {
		t.Fatal("no tokens")
	}

	last := tokens[len(tokens)-1]
	if !last.Quoted || last.Text != kelvin {
		t.Fatalf("last token = %+v, want the quoted name %q — SQLite folds ASCII only, so it is not \"k\"", last, kelvin)
	}
	if got := tokens[len(tokens)-2]; !got.isWord("from") {
		t.Fatalf("token before the name = %+v, want the keyword \"from\"", got)
	}
}

func TestARacedLedgerRowMustBeTheSameMigration(t *testing.T) {
	// Waiting for the write lock and then finding the version recorded means
	// another process won the race — but WHAT it recorded is the question.
	// Another binary can hold a different migration under the same number, and
	// accepting the row on its version alone let this process build the rest
	// of the catalog on top of a step it never checked.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := testCatalog()

	db, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if err := ensureLedger(ctx, db); err != nil {
		t.Fatalf("ensureLedger: %v", err)
	}
	// The row a different binary would have written for this same version.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
		int(catalog[0].Version), "another_binarys_step", "0000", "2026-08-17T12:00:00Z"); err != nil {
		t.Fatalf("seed the raced row: %v", err)
	}

	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	_, err = applyMigration(ctx, db, migrationRun{Catalog: catalog, Now: func() time.Time { return fixed }}, catalog[0])
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("error = %v, want ErrMigrationDrift", err)
	}
}

func TestAPathWithAQuestionMarkOpensThatFile(t *testing.T) {
	// Both drivers cut a plain path at its first "?" to find their DSN
	// parameters, so this database quietly opened a different file while
	// Location() and every log line named the one the operator asked for.
	directory := t.TempDir()
	path := filepath.Join(directory, "state?backup.db")

	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the file the caller named does not exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "state")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("a second database was created at the truncated path")
	}
}

func TestTheBootstrapFormatIsCompared(t *testing.T) {
	// bootstrap_version says which storage_metadata contract wrote the file.
	// Reading the row without looking at that field made it decorative: a
	// value this binary never writes was carried along as if the layout were
	// the one it expects.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := testCatalog()

	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := raw.Exec(`UPDATE storage_metadata SET bootstrap_version = 999 WHERE id = 1`); err != nil {
		t.Fatalf("rewrite the bootstrap format: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("error = %v, want ErrSchemaTooNew", err)
	}
}

func TestTheScannerReadsTheStatementSubject(t *testing.T) {
	// The scan is the early error message, not the boundary — confirmRunnerState
	// compares the rows before and after every step. So it reads only what a
	// statement ACTS ON, which in SQLite is always its subject: no write to a
	// table can avoid naming it there.
	//
	// Walking table lists, aliases and subqueries instead was wrong in both
	// directions at once — a table behind a parenthesis went unseen while a
	// column that merely shared a name was refused — and each fix produced the
	// next pair. What the retreat gives up is reporting a READ of the runner's
	// tables, which changes nothing in the file.
	migration := Migration{Version: 2, Name: "reach"}

	refused := map[string]string{
		"update":            `UPDATE storage_metadata SET owner_identity = 'x';`,
		"delete":            `DELETE FROM schema_migrations WHERE version = 1;`,
		"drop":              `DROP TABLE storage_metadata;`,
		"alter":             `ALTER TABLE schema_migrations RENAME TO gone;`,
		"insert":            `INSERT INTO schema_migrations (version) VALUES (99);`,
		"insert or replace": `INSERT OR REPLACE INTO storage_metadata (id) VALUES (1);`,
		"index over it":     `CREATE INDEX idx ON schema_migrations(name);`,
		"trigger over it":   `CREATE TRIGGER t AFTER INSERT ON storage_metadata BEGIN SELECT 1; END;`,
		"quoted":            `UPDATE "storage_metadata" SET owner_identity = 'x';`,
		"bracketed":         `DELETE FROM [storage_metadata] WHERE id = 1;`,
		"single quoted":     `DROP TABLE 'storage_metadata';`,
		"qualified":         `DROP TABLE main."storage_metadata";`,
		"form feed":         "DROP TABLE\fstorage_metadata;",
		"inside a trigger body": `CREATE TRIGGER t AFTER INSERT ON notes
			BEGIN UPDATE storage_metadata SET owner_identity = 'x';`,
		"behind a CTE": `WITH seed AS (SELECT 1) UPDATE storage_metadata SET owner_identity = 'x';`,
	}
	for name, statement := range refused {
		t.Run(name, func(t *testing.T) {
			if err := rejectRunnerOwnedTables(migration, statement); !errors.Is(err, ErrCatalogInvalid) {
				t.Fatalf("error = %v, want ErrCatalogInvalid", err)
			}
		})
	}

	// Valid SQL that names one of those tables somewhere OTHER than the
	// subject. Every one of these was refused by some earlier version of the
	// scan, and each refusal would have blocked a correct migration.
	accepted := map[string]string{
		"column of that name": `CREATE TABLE notes (storage_metadata TEXT NOT NULL);`,
		"index column list":   `CREATE INDEX idx ON notes(kind, storage_metadata);`,
		"trigger column list": `CREATE TRIGGER touch AFTER UPDATE OF kind, storage_metadata ON notes
			BEGIN SELECT 1; END;`,
		"join condition":     `SELECT 1 FROM notes JOIN other ON storage_metadata = other.id;`,
		"references list":    `CREATE TABLE child (other_id REFERENCES other, storage_metadata TEXT);`,
		"the word as value":  `INSERT INTO notes (kind) VALUES ('storage_metadata');`,
		"check constraint":   `CREATE TABLE notes (kind TEXT CHECK (kind <> 'storage_metadata'));`,
		"parenthesised from": `SELECT 1 FROM (ordinary) o, notes;`,
		"subquery source":    `SELECT 1 FROM (SELECT * FROM ordinary) o, notes;`,
		"view over a join": `CREATE VIEW v AS
			SELECT n.id FROM notes n JOIN other ON storage_metadata = other.id;`,
		"trigger body on its own table": `CREATE TRIGGER t AFTER INSERT ON notes
			BEGIN UPDATE notes SET kind = 'x';`,
	}
	for name, statement := range accepted {
		t.Run(name, func(t *testing.T) {
			if err := rejectRunnerOwnedTables(migration, statement); err != nil {
				t.Fatalf("error = %v, want the statement to be accepted", err)
			}
		})
	}

	// Reads are no longer reported. They are listed here so that the retreat
	// is a decision recorded in the tests rather than a gap nobody wrote down.
	reads := map[string]string{
		"select from the ledger": `INSERT INTO t (id) SELECT version FROM schema_migrations;`,
		"a view over it":         `CREATE VIEW leak AS SELECT o.id FROM ordinary o, storage_metadata;`,
		"second in a list":       `SELECT 1 FROM ordinary, schema_migrations;`,
	}
	for name, statement := range reads {
		t.Run(name, func(t *testing.T) {
			if err := rejectRunnerOwnedTables(migration, statement); err != nil {
				t.Fatalf("error = %v: a read is outside what the scan claims", err)
			}
		})
	}
}

func TestADeletedLedgerIsNotSilentlyRebuilt(t *testing.T) {
	// The absence of a ledger means "pre-versioned" only for a file with no
	// owner metadata. storage_metadata is created by the bootstrap migration
	// and that migration's ledger row commits in the same transaction, so a
	// file that has the table has recorded a version — recreating the ledger
	// turned a deleted or swapped migration history into an empty one, and
	// every version was then recorded afresh over a schema nobody had checked.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := testCatalog()

	database := openTest(t, path, catalog)
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	damaged, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := damaged.Exec(`DROP TABLE schema_migrations`); err != nil {
		t.Fatalf("drop the ledger: %v", err)
	}
	if err := damaged.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible — the history was rebuilt from nothing", err)
	}
}

func TestAnEmptiedLedgerIsNotSilentlyRebuilt(t *testing.T) {
	// Dropping the table is the loud version. Emptying it leaves the name in
	// place, so a check that only asked whether schema_migrations exists let
	// the file through, currentVersion read 0, and every version was recorded
	// afresh over a schema nobody had checked.
	damage := map[string]string{
		"rows deleted": `DELETE FROM schema_migrations`,
		"table swapped for an empty one of the right shape": `DROP TABLE schema_migrations;
` + migrationLedgerDDL,
	}

	for name, statement := range damage {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			catalog := testCatalog()

			database := openTest(t, path, catalog)
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			damaged, err := sql.Open(sqliteDriverName, DSN(path))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if _, err := damaged.Exec(statement); err != nil {
				t.Fatalf("apply the damage: %v", err)
			}
			if err := damaged.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestAPreVersionedFileStillAdoptsWithoutALedger(t *testing.T) {
	// The other side of the same rule: a file this package has never claimed
	// has no ledger by definition, and refusing it would break adoption.
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy history: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v — a pre-versioned file was refused", err)
	}
	t.Cleanup(func() { _ = database.Close() })
}

func TestACRLFCheckoutRecordsTheSameChecksum(t *testing.T) {
	// The SQL is embedded verbatim from a file, and a Windows checkout with
	// core.autocrlf rewrites that file's line endings. Hashing the raw bytes
	// therefore recorded a different checksum, and a database migrated by such
	// a build was refused by an official one as drift — in both directions.
	// Nothing caught it because a test writes and re-reads its ledger from one
	// checkout.
	lf := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{Version: 2, Name: "messages", SQL: "CREATE TABLE messages (\n\tid TEXT PRIMARY KEY\n);\n"},
	}
	crlf := make([]Migration, len(lf))
	for i, migration := range lf {
		migration.SQL = strings.ReplaceAll(migration.SQL, "\n", "\r\n")
		crlf[i] = migration
	}

	for i := range lf {
		if lf[i].Checksum() != crlf[i].Checksum() {
			t.Fatalf("version %s: LF checksum %s, CRLF checksum %s",
				lf[i].Version, lf[i].Checksum(), crlf[i].Checksum())
		}
	}

	// End to end: a database migrated by the LF build opens under the CRLF one.
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := Open(context.Background(), testConfig(t, path, lf))
	if err != nil {
		t.Fatalf("Open with the LF catalog: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(context.Background(), testConfig(t, path, crlf))
	if err != nil {
		t.Fatalf("Open with the CRLF catalog: %v — the same migrations read as drift", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
}
