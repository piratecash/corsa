package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// owner is the identity every test database is opened under.
func owner(t *testing.T) domain.PeerIdentity {
	t.Helper()
	identity, err := domain.ParsePeerIdentity(strings.Repeat("a", 40))
	if err != nil {
		t.Fatalf("parse owner identity: %v", err)
	}
	return identity
}

func openState(t *testing.T, path string) *storage.Database {
	t.Helper()
	database, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        owner(t),
		Catalog:      migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// legacyGeneration is one historical shape of the pre-versioned chatlog
// database. All three still exist in the field, and only the SECOND one
// shipped in a release (v1.0.64) — the recovery tables of generation 3 landed
// after that release bump, so most real databases are at generation 2.
type legacyGeneration struct {
	name   string
	schema string
	rows   []string
	// tables is what the generation's own DDL created; everything the
	// catalog adds on top must be created by the migration, not assumed.
	tables []string
}

func legacyGenerations() []legacyGeneration {
	messagesOnly := []string{"messages"}
	withJournals := append(append([]string{}, messagesOnly...), "seen_ack", "delivery_failed")
	withRecovery := append(append([]string{}, withJournals...),
		"decrypt_recovery_jobs", "peer_established", "decrypt_recovery_cycles", "decrypt_resend_intents")

	return []legacyGeneration{
		{
			name:   "gen1_messages_only",
			schema: "legacy_schema_gen1.sql",
			rows:   []string{"legacy_rows_messages.sql"},
			tables: messagesOnly,
		},
		{
			name:   "gen2_released_v1.0.64",
			schema: "legacy_schema_gen2.sql",
			rows:   []string{"legacy_rows_messages.sql", "legacy_rows_journals.sql"},
			tables: withJournals,
		},
		{
			name:   "gen3_decrypt_recovery",
			schema: "legacy_schema_gen3.sql",
			rows:   []string{"legacy_rows_messages.sql", "legacy_rows_journals.sql", "legacy_rows_recovery.sql"},
			tables: withRecovery,
		},
	}
}

// latestGeneration is the newest pre-versioned shape — what a database written
// by the commit right before this layer looks like.
func latestGeneration() legacyGeneration {
	generations := legacyGenerations()
	return generations[len(generations)-1]
}

// buildLegacyDatabase writes a database in the given pre-versioned generation,
// schema and rows, exactly as the binary of that era left it.
func buildLegacyDatabase(t *testing.T, path string, generation legacyGeneration) {
	t.Helper()

	db := writeLegacySchema(t, path, generation, nil)
	for _, fixture := range generation.rows {
		if _, err := db.Exec(readFixture(t, fixture)); err != nil {
			t.Fatalf("apply %s: %v", fixture, err)
		}
	}
}

// writeLegacySchema applies a frozen DDL, optionally bent by rewriteSchema to
// model a partially created or foreign shape. Malformed variants carry no
// rows: the frozen INSERTs assume the frozen columns.
func writeLegacySchema(t *testing.T, path string, generation legacyGeneration, rewriteSchema func(string) string) *sql.DB {
	t.Helper()

	schema := readFixture(t, generation.schema)
	if rewriteSchema != nil {
		rewritten := rewriteSchema(schema)
		if rewritten == schema {
			t.Fatal("schema rewrite changed nothing — the frozen fixture drifted")
		}
		schema = rewritten
	}

	db, err := sql.Open(storage.DriverName(), storage.DSN(path))
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("apply legacy schema: %v", err)
	}
	return db
}

func readFixture(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(content)
}

func TestCatalogIsValid(t *testing.T) {
	if err := storage.ValidateCatalog(context.Background(), migrations.Catalog()); err != nil {
		t.Fatalf("ValidateCatalog: %v", err)
	}
}

func TestFreshDatabaseReachesLatestVersion(t *testing.T) {
	database := openState(t, filepath.Join(t.TempDir(), "state.db"))

	if got, want := database.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}
}

func TestEveryLegacyGenerationIsAdoptedWithoutDataLoss(t *testing.T) {
	// The three generations differ only in which tables their own DDL
	// created. A generation that predates a table must have it created by the
	// migration, and every row the generation did write must survive
	// unchanged — that combination is the whole upgrade contract.
	for _, generation := range legacyGenerations() {
		t.Run(generation.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "chatlog-legacy.db")
			buildLegacyDatabase(t, path, generation)

			// Guard against a vacuous pass: a fixture that quietly grew the
			// later tables would make "the migration created them" untestable.
			assertTablesAre(t, rawOpen(t, path), generation.tables)

			before := tableDump(t, rawOpen(t, path), generation.tables)

			database := openState(t, path)
			if got, want := database.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
				t.Fatalf("SchemaVersion = %s, want %s", got, want)
			}

			after := tableDump(t, database.Executor(), generation.tables)
			for table, rows := range before {
				if got := after[table]; got != rows {
					t.Fatalf("table %s changed across the migration:\nbefore:\n%s\nafter:\n%s", table, rows, got)
				}
			}

			// Every table the catalog knows must exist afterwards, including
			// the ones this generation never had.
			for _, table := range chatlogTables {
				var name string
				if err := database.Executor().QueryRowContext(context.Background(),
					`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&name); err != nil {
					t.Fatalf("table %q missing after the migration: %v", table, err)
				}
			}

			var integrity string
			if err := database.Executor().QueryRowContext(context.Background(), "PRAGMA integrity_check").Scan(&integrity); err != nil {
				t.Fatalf("integrity_check: %v", err)
			}
			if integrity != "ok" {
				t.Fatalf("integrity_check = %q, want ok", integrity)
			}
		})
	}
}

func TestEveryAdoptedGenerationEndsWithTheFreshSchema(t *testing.T) {
	// Whatever the starting generation, the file must end up structurally
	// identical to one this binary created from scratch. Otherwise "version 4"
	// would mean different things on different machines.
	fresh := schemaDump(t, openState(t, filepath.Join(t.TempDir(), "fresh.db")).Executor())

	for _, generation := range legacyGenerations() {
		t.Run(generation.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			buildLegacyDatabase(t, path, generation)

			adopted := schemaDump(t, openState(t, path).Executor())
			if adopted != fresh {
				t.Fatalf("adopted %s schema differs from a fresh one:\nadopted:\n%s\nfresh:\n%s",
					generation.name, adopted, fresh)
			}
		})
	}
}

func TestPartiallyCreatedLegacyTableIsRejected(t *testing.T) {
	// CREATE TABLE IF NOT EXISTS is a no-op against an existing table, so a
	// wrong column set or a dropped constraint survives the DDL untouched.
	// Only the verifier can see it — and it must, because the repository
	// would otherwise write against a schema nobody checked.
	cases := map[string]func(string) string{
		"missing column": func(schema string) string {
			return strings.Replace(schema, "\tmetadata        TEXT NOT NULL DEFAULT '',\n", "", 1)
		},
		"nullable column": func(schema string) string {
			return strings.Replace(schema, "\tcreated_at      TEXT NOT NULL,\n", "\tcreated_at      TEXT,\n", 1)
		},
		"unexpected column": func(schema string) string {
			return strings.Replace(schema,
				"\tupdated_at      TEXT NOT NULL DEFAULT ''\n",
				"\tupdated_at      TEXT NOT NULL DEFAULT '',\n\tsurprise        TEXT NOT NULL DEFAULT ''\n", 1)
		},
		"missing check constraint": func(schema string) string {
			return strings.Replace(schema,
				" CHECK(delivery_status IN ('sent','delivered','seen'))", "", 1)
		},
		// CREATE INDEX IF NOT EXISTS leaves a same-named index alone whatever
		// it indexes, so a wrong one is invisible unless the definition is
		// compared.
		"index over the wrong columns": func(schema string) string {
			return strings.Replace(schema,
				"ON messages(topic, sender, recipient, created_at);",
				"ON messages(sender, recipient);", 1)
		},
		// The nastiest variant: the repository stores several messages per
		// (recipient, delivery_status), so a stray UNIQUE here rejects
		// ordinary writes rather than merely slowing queries down.
		"unexpected unique index": func(schema string) string {
			return strings.Replace(schema,
				"CREATE INDEX IF NOT EXISTS idx_messages_status",
				"CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_status", 1)
		},
		// A UNIQUE table CONSTRAINT is worse still: its index is generated by
		// SQLite, carries no CREATE INDEX text, and so is invisible to any
		// check that reads sqlite_schema. It survives every idempotent CREATE
		// and then rejects the second message of a conversation.
		"unexpected unique constraint": func(schema string) string {
			return strings.Replace(schema,
				"\ttopic           TEXT NOT NULL DEFAULT 'dm' CHECK(topic IN ('dm','global')),",
				"\ttopic           TEXT NOT NULL DEFAULT 'dm' UNIQUE CHECK(topic IN ('dm','global')),", 1)
		},
	}

	for name, rewrite := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			writeLegacySchema(t, path, latestGeneration(), rewrite)

			_, err := storage.Open(context.Background(), storage.Config{
				ExplicitPath: path,
				Owner:        owner(t),
				Catalog:      migrations.Catalog(),
			})
			if !errors.Is(err, storage.ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}

			// The chatlog version must NOT be recorded: a half-known schema
			// that reports itself as migrated is exactly the silent data
			// loss this layer exists to prevent.
			if version := recordedVersion(t, path); version >= 2 {
				t.Fatalf("recorded version = %d, want < 2", version)
			}
		})
	}
}

func TestExtraNonUniqueIndexIsTolerated(t *testing.T) {
	// A plain index is a performance decision: it changes no row the
	// repository may write. An operator who added one by hand must not find
	// the node refusing to start — unlike a UNIQUE one, which is covered by
	// TestPartiallyCreatedLegacyTableIsRejected.
	path := filepath.Join(t.TempDir(), "legacy.db")
	writeLegacySchema(t, path, latestGeneration(), func(schema string) string {
		return schema + "\nCREATE INDEX IF NOT EXISTS idx_messages_operator_added ON messages(body);\n"
	})

	database := openState(t, path)
	if got, want := database.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}
}

func TestMissingAdditiveObjectsAreCreated(t *testing.T) {
	// An index and a whole table can be added by idempotent DDL, unlike a
	// column, so a legacy file that predates them is completed rather than
	// rejected.
	path := filepath.Join(t.TempDir(), "legacy.db")
	writeLegacySchema(t, path, latestGeneration(), func(schema string) string {
		withoutIndex := strings.Replace(schema,
			"CREATE INDEX IF NOT EXISTS idx_messages_ttl\n\tON messages(flag, created_at) WHERE flag = 'auto-delete-ttl';", "", 1)
		return strings.Replace(withoutIndex, `CREATE TABLE IF NOT EXISTS decrypt_resend_intents (
	root TEXT PRIMARY KEY,
	original_id TEXT NOT NULL,
	peer TEXT NOT NULL,
	replacement_id TEXT NOT NULL,
	created_at TEXT NOT NULL
);`, "", 1)
	})

	database := openState(t, path)
	if got, want := database.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}

	for _, object := range []string{"idx_messages_ttl", "decrypt_resend_intents"} {
		var name string
		if err := database.Executor().QueryRowContext(context.Background(),
			`SELECT name FROM sqlite_schema WHERE name = ?`, object).Scan(&name); err != nil {
			t.Fatalf("object %q was not created: %v", object, err)
		}
	}
}

func TestDriverContract(t *testing.T) {
	database := openState(t, filepath.Join(t.TempDir(), "state.db"))
	executor := database.Executor()

	var journalMode string
	if err := executor.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var busyTimeout int
	if err := executor.QueryRowContext(context.Background(), "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", busyTimeout)
	}

	var foreignKeys int
	if err := executor.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("read foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
	}

	// A transaction opened through the injected executor must commit and be
	// visible to a plain read afterwards — the contract every repository
	// relies on.
	tx, err := executor.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `INSERT INTO seen_ack (id) VALUES ('tx-row')`); err != nil {
		t.Fatalf("insert in transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var count int
	if err := executor.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM seen_ack WHERE id = 'tx-row'`).Scan(&count); err != nil {
		t.Fatalf("count committed row: %v", err)
	}
	if count != 1 {
		t.Fatalf("committed rows = %d, want 1", count)
	}

	// A WAL checkpoint must succeed on the same connection pool: this is the
	// operation a clean shutdown depends on.
	if _, err := executor.ExecContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("wal_checkpoint: %v", err)
	}
}

// rawOpen returns a driver-level handle for reading a database the storage
// layer has not opened yet.
func rawOpen(t *testing.T, path string) storage.Executor {
	t.Helper()
	db, err := sql.Open(storage.DriverName(), storage.DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// chatlogTables is the set the adoption test compares before and after.
var chatlogTables = []string{
	"messages",
	"seen_ack",
	"delivery_failed",
	"decrypt_recovery_jobs",
	"peer_established",
	"decrypt_recovery_cycles",
	"decrypt_resend_intents",
}

// tableDump renders the given tables as sorted text so the comparison covers
// all columns, not just the row count.
func tableDump(t *testing.T, executor storage.Executor, tables []string) map[string]string {
	t.Helper()

	dump := make(map[string]string, len(tables))
	for _, table := range tables {
		rows, err := executor.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			t.Fatalf("select from %s: %v", table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatalf("columns of %s: %v", table, err)
		}

		var lines []string
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				_ = rows.Close()
				t.Fatalf("scan %s: %v", table, err)
			}
			var fields []string
			for i, column := range columns {
				fields = append(fields, fmt.Sprintf("%s=%v", column, values[i]))
			}
			lines = append(lines, strings.Join(fields, " "))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s: %v", table, err)
		}
		_ = rows.Close()

		sortStrings(lines)
		dump[table] = strings.Join(lines, "\n")
	}
	return dump
}

// schemaDump renders the stored DDL of every object except the ledger, which
// legitimately differs in applied_at.
func schemaDump(t *testing.T, executor storage.Executor) string {
	t.Helper()

	rows, err := executor.QueryContext(context.Background(),
		`SELECT type, name, COALESCE(sql, '') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var lines []string
	for rows.Next() {
		var objectType, name, ddl string
		if err := rows.Scan(&objectType, &name, &ddl); err != nil {
			t.Fatalf("scan schema: %v", err)
		}
		lines = append(lines, fmt.Sprintf("%s %s: %s", objectType, name, strings.Join(strings.Fields(ddl), " ")))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema: %v", err)
	}
	return strings.Join(lines, "\n")
}

// recordedVersion reads the highest ledger version straight from the file,
// returning 0 when there is no ledger at all.
func recordedVersion(t *testing.T, path string) int {
	t.Helper()

	db := rawOpen(t, path)
	var version sql.NullInt64
	err := db.QueryRowContext(context.Background(), `SELECT MAX(version) FROM schema_migrations`).Scan(&version)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return 0
		}
		t.Fatalf("read recorded version: %v", err)
	}
	if !version.Valid {
		return 0
	}
	return int(version.Int64)
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// assertTablesAre fails unless the database holds exactly the named chatlog
// tables — no more, no fewer. Non-chatlog objects (the ledger, metadata) are
// ignored, so it can be used before and after a migration alike.
func assertTablesAre(t *testing.T, executor storage.Executor, want []string) {
	t.Helper()

	expected := make(map[string]struct{}, len(want))
	for _, table := range want {
		expected[table] = struct{}{}
	}

	for _, table := range chatlogTables {
		var name string
		err := executor.QueryRowContext(context.Background(),
			`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&name)
		_, shouldExist := expected[table]
		switch {
		case shouldExist && err != nil:
			t.Fatalf("table %q should already exist in this generation: %v", table, err)
		case !shouldExist && err == nil:
			t.Fatalf("table %q exists before the migration — the fixture is not from this generation", table)
		}
	}
}

func TestCheckConstraintsAreMatchedExactly(t *testing.T) {
	// The verifier claims to describe the exact shape of the table, so the
	// CHECK set has to hold in both directions: a commented-out constraint
	// must not satisfy it, and an extra one — which changes what the
	// repository is allowed to write — must not pass unnoticed.
	cases := map[string]func(string) string{
		"constraint commented out": func(schema string) string {
			return strings.Replace(schema,
				" CHECK(delivery_status IN ('sent','delivered','seen'))",
				" /* CHECK(delivery_status IN ('sent','delivered','seen')) */", 1)
		},
		"extra constraint": func(schema string) string {
			return strings.Replace(schema,
				"\tbody            TEXT NOT NULL,",
				"\tbody            TEXT NOT NULL CHECK(body <> ''),", 1)
		},
	}

	for name, rewrite := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			writeLegacySchema(t, path, latestGeneration(), rewrite)

			_, err := storage.Open(context.Background(), storage.Config{
				ExplicitPath: path,
				Owner:        owner(t),
				Catalog:      migrations.Catalog(),
			})
			if !errors.Is(err, storage.ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestGeneratedColumnAndForeignKeyAreRejected(t *testing.T) {
	// Neither is visible to PRAGMA table_info, which is why a property-by-
	// property verifier missed both. A generated NOT NULL column makes ordinary
	// inserts fail; an unexpected foreign key makes the first write to an empty
	// legacy table fail on a parent row that does not exist.
	cases := map[string]func(string) string{
		"generated column": func(schema string) string {
			return strings.Replace(schema,
				"\tupdated_at      TEXT NOT NULL DEFAULT ''\n",
				"\tupdated_at      TEXT NOT NULL DEFAULT '',\n\tshadow          TEXT GENERATED ALWAYS AS (body) VIRTUAL\n", 1)
		},
		"foreign key": func(schema string) string {
			return "CREATE TABLE IF NOT EXISTS parent (id TEXT PRIMARY KEY);\n" +
				strings.Replace(schema,
					"\tsender          TEXT NOT NULL,",
					"\tsender          TEXT NOT NULL REFERENCES parent(id),", 1)
		},
	}

	for name, rewrite := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			writeLegacySchema(t, path, latestGeneration(), rewrite)

			_, err := storage.Open(context.Background(), storage.Config{
				ExplicitPath: path,
				Owner:        owner(t),
				Catalog:      migrations.Catalog(),
			})
			if !errors.Is(err, storage.ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestATriggerOnAProductionTableIsRejected(t *testing.T) {
	// An AFTER INSERT on messages can delete the row a repository just stored,
	// leaving the caller with a successful INSERT and nothing saved. Only the
	// ledger used to be protected from this.
	path := filepath.Join(t.TempDir(), "legacy.db")
	writeLegacySchema(t, path, latestGeneration(), func(schema string) string {
		return schema + "\nCREATE TRIGGER swallow_messages AFTER INSERT ON messages" +
			" BEGIN DELETE FROM messages WHERE id = NEW.id; END;\n"
	})

	_, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        owner(t),
		Catalog:      migrations.Catalog(),
	})
	if !errors.Is(err, storage.ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestSchemaIsReVerifiedOnEveryOpen(t *testing.T) {
	// A recorded version used to be skipped outright, so damage done AFTER a
	// successful start was never noticed again: the ledger checksum still
	// matched, Open returned a ready database, and the repository met the
	// broken schema at runtime instead.
	cases := map[string]string{
		"table dropped":  `DROP TABLE seen_ack;`,
		"trigger added":  `CREATE TRIGGER swallow AFTER INSERT ON messages BEGIN DELETE FROM messages WHERE id = NEW.id; END;`,
		"table replaced": `DROP TABLE seen_ack; CREATE TABLE seen_ack (id TEXT PRIMARY KEY, surprise TEXT);`,
	}

	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			first := openState(t, path)
			if got, want := first.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
				t.Fatalf("SchemaVersion = %s, want %s", got, want)
			}
			if _, err := first.Executor().ExecContext(context.Background(), damage); err != nil {
				t.Fatalf("apply damage: %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			_, err := storage.Open(context.Background(), storage.Config{
				ExplicitPath: path,
				Owner:        owner(t),
				Catalog:      migrations.Catalog(),
			})
			if !errors.Is(err, storage.ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
		})
	}
}

func TestALaterMigrationMayIndexAnEarlierTable(t *testing.T) {
	// The ordinary way a shared schema grows. The expectation is built from
	// the whole catalog, so the new index belongs to a table an earlier
	// version created — and the earlier version's own check must not call it
	// an intruder.
	const addIndex = `CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender);`

	catalog := append(migrations.Catalog(), storage.Migration{
		Version: storage.LatestVersion(migrations.Catalog()) + 1,
		Name:    "messages_sender_index",
		SQL:     addIndex,
	})
	if err := storage.ValidateCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("ValidateCatalog: %v", err)
	}

	path := filepath.Join(t.TempDir(), "state.db")
	buildLegacyDatabase(t, path, latestGeneration())

	database, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        owner(t),
		Catalog:      catalog,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	var name string
	if err := database.Executor().QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_schema WHERE name = 'idx_messages_sender'`).Scan(&name); err != nil {
		t.Fatalf("the added index is missing: %v", err)
	}
}

func TestAnIncomingForeignKeyToAManagedTableIsRejected(t *testing.T) {
	// foreign_key_check passes for such a table as long as its rows resolve,
	// so the file opened cleanly — and then the first delete of a referenced
	// message failed, held by a table nobody declared.
	path := filepath.Join(t.TempDir(), "legacy.db")
	writeLegacySchema(t, path, latestGeneration(), func(schema string) string {
		return schema + "\nCREATE TABLE alien (mid TEXT REFERENCES messages(id));\n"
	})

	_, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        owner(t),
		Catalog:      migrations.Catalog(),
	})
	if !errors.Is(err, storage.ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestATableAndATriggerMayShareAName(t *testing.T) {
	// SQLite allows it, and keying the schema snapshot by name alone silently
	// dropped one of the two — a trigger could evict the table it shares a
	// name with, and the incompatible table then went unchecked.
	path := filepath.Join(t.TempDir(), "legacy.db")
	writeLegacySchema(t, path, latestGeneration(), func(schema string) string {
		return strings.Replace(schema,
			"\tbody            TEXT NOT NULL,",
			"\tbody            TEXT NOT NULL DEFAULT 'changed',", 1) +
			"\nCREATE TRIGGER messages AFTER INSERT ON seen_ack BEGIN SELECT 1; END;\n"
	})

	_, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        owner(t),
		Catalog:      migrations.Catalog(),
	})
	if !errors.Is(err, storage.ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible — the altered messages table was masked by the same-named trigger", err)
	}
}

func TestAnIncomingForeignKeyIsMatchedCaseInsensitively(t *testing.T) {
	// SQLite resolves REFERENCES MeSsAgEs to messages, so a case-sensitive
	// lookup let exactly the table this check exists to refuse through.
	path := filepath.Join(t.TempDir(), "legacy.db")
	writeLegacySchema(t, path, latestGeneration(), func(schema string) string {
		return schema + "\nCREATE TABLE alien (mid TEXT REFERENCES MeSsAgEs(id));\n"
	})

	_, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        owner(t),
		Catalog:      migrations.Catalog(),
	})
	if !errors.Is(err, storage.ErrSchemaIncompatible) {
		t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
	}
}

func TestADeferredForeignKeyMigrationRunsAsInTheRunner(t *testing.T) {
	// A deferred foreign key is checked at COMMIT. Building the reference in
	// autocommit measured this migration under semantics the runner never
	// uses, and rejected a step that applies correctly.
	catalog := append(migrations.Catalog(),
		storage.Migration{
			Version: storage.LatestVersion(migrations.Catalog()) + 1,
			Name:    "deferred_parent",
			SQL: `CREATE TABLE label_group (id TEXT PRIMARY KEY);
CREATE TABLE label_member (
	id       TEXT PRIMARY KEY,
	group_id TEXT NOT NULL REFERENCES label_group(id) DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO label_member (id, group_id) VALUES ('m1', 'g1');
INSERT INTO label_group (id) VALUES ('g1');`,
			Invariant: func(ctx context.Context, tx storage.SchemaReader) error {
				var members int
				if err := storage.ScanOne(ctx, tx, &members, `SELECT COUNT(*) FROM label_member`); err != nil {
					return err
				}
				if members != 1 {
					return fmt.Errorf("label_member has %d rows, want 1", members)
				}
				return nil
			},
		})

	database, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: filepath.Join(t.TempDir(), "state.db"),
		Owner:        owner(t),
		Catalog:      catalog,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
}

func TestAViewNeedsNoPostCondition(t *testing.T) {
	// CREATE VIEW ... AS SELECT copies nothing, and demanding a verifier for
	// it made a plain schema object look like a data migration.
	catalog := append(migrations.Catalog(), storage.Migration{
		Version: storage.LatestVersion(migrations.Catalog()) + 1,
		Name:    "recent_messages",
		SQL:     "CREATE VIEW recent_messages AS SELECT id, peer_id FROM messages;\n",
	})

	if err := storage.ValidateCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("ValidateCatalog: %v", err)
	}
}
