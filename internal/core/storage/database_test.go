package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestLegacyDefaultPathKeepsPreVersionedFileName(t *testing.T) {
	dir := t.TempDir()
	owner := testIdentity(0xAB)

	database, err := Open(context.Background(), Config{
		DataDir:       dir,
		ListenAddress: domain.ListenAddress(":64646"),
		Owner:         owner,
		Catalog:       testCatalog(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()

	// The name is a rollback contract: an older binary must find the same
	// history file, so this assertion spells the format out instead of
	// reusing LegacyFileName.
	want := filepath.Join(dir, "chatlog-"+owner.String()[:8]+"-64646.db")
	if got := database.Location().Path; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
	if got, want := database.Location().Source, PathSourceLegacyDefault; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}

	// The marker must land on a brand-new file too: it is what makes a later
	// open able to tell a Corsa database from an unrelated SQLite file.
	var stamped int64
	if err := database.Executor().QueryRowContext(context.Background(), "PRAGMA application_id").Scan(&stamped); err != nil {
		t.Fatalf("read application_id: %v", err)
	}
	if stamped != applicationID {
		t.Fatalf("application_id = %d, want %d", stamped, applicationID)
	}
}

func TestLegacyDefaultPathFallsBackToDefaultPort(t *testing.T) {
	owner := testIdentity(0x0C)
	if got, want := LegacyFileName(owner, ""), "chatlog-"+owner.String()[:8]+"-default.db"; got != want {
		t.Fatalf("LegacyFileName = %q, want %q", got, want)
	}
}

func TestExplicitPathWinsAndImportsNothing(t *testing.T) {
	dir := t.TempDir()
	owner := testIdentity(0xAB)
	legacyConfig := Config{
		DataDir:       dir,
		ListenAddress: domain.ListenAddress(":64646"),
		Owner:         owner,
		Catalog:       testCatalog(),
	}

	legacy, err := Open(context.Background(), legacyConfig)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Executor().ExecContext(context.Background(), `INSERT INTO probe (id) VALUES ('legacy-row')`); err != nil {
		t.Fatalf("insert into legacy: %v", err)
	}
	legacyPath := legacy.Location().Path
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	explicitConfig := legacyConfig
	explicitConfig.ExplicitPath = filepath.Join(dir, "explicit", "state.db")
	explicit, err := Open(context.Background(), explicitConfig)
	if err != nil {
		t.Fatalf("open explicit: %v", err)
	}
	defer func() { _ = explicit.Close() }()

	if got, want := explicit.Location().Source, PathSourceExplicit; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}

	var rows int
	if err := explicit.Executor().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM probe`).Scan(&rows); err != nil {
		t.Fatalf("count explicit rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("explicit database has %d rows, want 0 — nothing may be imported from the legacy file", rows)
	}
	if _, err := os.Stat(legacyPath); err != nil {
		t.Fatalf("legacy file must stay in place: %v", err)
	}
}

func TestOpenRejectsForeignApplicationID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign.db")

	raw, err := sql.Open(DriverName(), DSN(path))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE unrelated (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create unrelated table: %v", err)
	}
	if _, err := raw.Exec(`PRAGMA application_id = 305419896`); err != nil {
		t.Fatalf("stamp foreign application_id: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	_, err = Open(context.Background(), testConfig(t, path, testCatalog()))
	if !errors.Is(err, ErrForeignApplication) {
		t.Fatalf("error = %v, want ErrForeignApplication", err)
	}
}

func TestOpenRejectsDatabaseOwnedByAnotherIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	first := openTest(t, path, testCatalog())
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	other := testConfig(t, path, testCatalog())
	other.Owner = testIdentity(0xCD)

	_, err := Open(context.Background(), other)
	if !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("error = %v, want ErrOwnerMismatch", err)
	}
}

func TestOpenRejectsAZeroOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	cfg := testConfig(t, path, testCatalog())
	cfg.Owner = domain.PeerIdentity{}

	if _, err := Open(context.Background(), cfg); err == nil {
		t.Fatal("Open accepted a zero owner identity")
	}
}

func TestOpenLeavesACorruptFileUntouched(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")

	garbage := []byte("this is not a SQLite database, but it is somebody's data")
	if err := os.WriteFile(path, garbage, 0o600); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	err := func() error {
		_, err := Open(context.Background(), testConfig(t, path, testCatalog()))
		return err
	}()
	if err == nil {
		t.Fatal("Open accepted a corrupt file")
	}
	// The CLASS matters as much as the refusal: an operator triaging this
	// needs "corrupt", not a generic SQL failure. Reserving ErrCorrupt for
	// checks that reported violations dropped this case to error_class "sql".
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt", err)
	}
	if class := errorClass(err); class != "corrupt" {
		t.Fatalf("errorClass = %q, want %q", class, "corrupt")
	}

	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read back: %v", readErr)
	}
	if string(after) != string(garbage) {
		t.Fatal("the corrupt file was modified")
	}

	entries, dirErr := os.ReadDir(dir)
	if dirErr != nil {
		t.Fatalf("read dir: %v", dirErr)
	}
	for _, entry := range entries {
		if entry.Name() != "state.db" {
			t.Fatalf("unexpected sibling file %q — a corrupt database must not be renamed or replaced", entry.Name())
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestOwnerIsRecordedOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	fixed := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

	first := openTest(t, path, testCatalog())
	if got, want := first.Owner(), testIdentity(0xAB); got != want {
		t.Fatalf("Owner = %s, want %s", got, want)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	later := testConfig(t, path, testCatalog())
	later.Now = func() time.Time { return fixed.Add(24 * time.Hour) }
	second, err := Open(context.Background(), later)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()

	var createdAt string
	if err := second.Executor().QueryRowContext(context.Background(),
		`SELECT created_at FROM storage_metadata WHERE id = 1`).Scan(&createdAt); err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if got, want := createdAt, fixed.Format(time.RFC3339Nano); got != want {
		t.Fatalf("created_at = %q, want %q — the owner row must be written once", got, want)
	}
}

func TestTheIntegrityChecksReportTheirResult(t *testing.T) {
	// Both checks run on every open, and their PASSING result is what the
	// ready log line carries: an operator reading it must be able to tell a
	// database that was checked from one where the checks never ran.
	path := filepath.Join(t.TempDir(), "state.db")
	database := openTest(t, path, testCatalog())
	ctx := context.Background()

	integrity, err := checkIntegrity(ctx, database.db)
	if err != nil {
		t.Fatalf("checkIntegrity: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check = %q, want %q", integrity, "ok")
	}

	violations, err := checkForeignKeys(ctx, database.db)
	if err != nil {
		t.Fatalf("checkForeignKeys: %v", err)
	}
	if violations != 0 {
		t.Fatalf("foreign_key_check = %d violations, want 0", violations)
	}
}

func TestEveryRefusalIsLoggedWithItsClass(t *testing.T) {
	// Not parallel: the global logger is redirected for the duration.
	//
	// A migration step that started and failed was the only refusal that
	// produced a structured event. Corruption, a foreign application, an owner
	// mismatch and a catalog rejected before the file is even located all
	// aborted startup with nothing for the operator to read — and the first
	// two of those happen before the opening line is written.
	healthy := testCatalog()

	established := filepath.Join(t.TempDir(), "state.db")
	if database := openTest(t, established, healthy); database != nil {
		if err := database.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	foreign := filepath.Join(t.TempDir(), "foreign.db")
	stampForeignApplication(t, foreign)

	cases := map[string]struct {
		config Config
		class  string
	}{
		"catalog rejected before the file is located": {
			config: Config{ExplicitPath: filepath.Join(t.TempDir(), "unused.db"), Owner: testIdentity(0xAB)},
			class:  "catalog-invalid",
		},
		"another identity owns the file": {
			config: Config{ExplicitPath: established, Owner: testIdentity(0xCD), Catalog: healthy},
			class:  "owner-mismatch",
		},
		"another application owns the file": {
			config: Config{ExplicitPath: foreign, Owner: testIdentity(0xAB), Catalog: healthy},
			class:  "foreign-application",
		},
		"the file is newer than this binary": {
			config: Config{ExplicitPath: established, Owner: testIdentity(0xAB), Catalog: healthy[:1]},
			class:  "schema-too-new",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			var captured bytes.Buffer
			original := log.Logger
			log.Logger = zerolog.New(&captured)
			defer func() { log.Logger = original }()

			if _, err := Open(context.Background(), testCase.config); err == nil {
				t.Fatal("Open returned no error")
			}

			want := `"error_class":"` + testCase.class + `"`
			if !strings.Contains(captured.String(), want) {
				t.Fatalf("the refusal was not logged with %s\nlog: %s", want, captured.String())
			}
		})
	}
}

// stampForeignApplication writes a database claimed by another application.
func stampForeignApplication(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("PRAGMA application_id = 305419896"); err != nil {
		t.Fatalf("stamp application_id: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE somebody_elses (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestARefusalNamesTheResolvedPath(t *testing.T) {
	// Not parallel: the global logger is redirected, and the working
	// directory is changed to make a relative path meaningful.
	//
	// The refusal has to name the file in the same terms as the startup line —
	// resolved, with its source — or an operator chasing a corrupt database is
	// handed a path that was never opened.
	directory := t.TempDir()
	t.Chdir(directory)

	var captured bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&captured)
	defer func() { log.Logger = original }()

	// Relative, with whitespace around it: exactly what resolution fixes.
	_, err := Open(context.Background(), Config{
		ExplicitPath: "  ./state.db ",
		Owner:        testIdentity(0xAB),
	})
	if err == nil {
		t.Fatal("Open returned no error for an empty catalog")
	}

	logged := captured.String()
	resolved := filepath.Join(directory, "state.db")
	for _, want := range []string{
		`"error_class":"catalog-invalid"`,
		`"path_source":"explicit"`,
		`"path_resolved":true`,
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("the refusal is missing %s\nlog: %s", want, logged)
		}
	}
	// The resolved path, not the configured one.
	if !strings.Contains(logged, resolved) {
		t.Fatalf("the refusal names something other than %q\nlog: %s", resolved, logged)
	}
	if strings.Contains(logged, `"db_path":"  ./state.db "`) {
		t.Fatalf("the refusal names the configured path verbatim\nlog: %s", logged)
	}
}

func TestAnUnresolvableLocationKeepsTheSourceEnum(t *testing.T) {
	// Not parallel: the global logger is redirected.
	//
	// path_source is a closed set — explicit or legacy-default — and the
	// operator's CHOICE is known even when the location cannot be worked out.
	// Reporting a third value there would make every consumer of these logs
	// learn one; the unknown half is path_resolved.
	var captured bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&captured)
	defer func() { log.Logger = original }()

	// Neither an explicit path nor a data directory: resolution cannot
	// produce a location at all.
	_, err := Open(context.Background(), Config{Owner: testIdentity(0xAB), Catalog: testCatalog()})
	if err == nil {
		t.Fatal("Open returned no error without a location")
	}

	logged := captured.String()
	if !strings.Contains(logged, `"path_source":"legacy-default"`) {
		t.Fatalf("the refusal dropped the configured source\nlog: %s", logged)
	}
	if !strings.Contains(logged, `"path_resolved":false`) {
		t.Fatalf("the refusal does not report that the location is unknown\nlog: %s", logged)
	}
	if strings.Contains(logged, `"path_source":"unresolved"`) {
		t.Fatalf("path_source carries a value outside its enum\nlog: %s", logged)
	}
}

func TestANewDatabaseIsOwnerOnlyInAWorldReadableDirectory(t *testing.T) {
	// The directory mode is only set when this package creates it, so an
	// explicit StateDBPath inside an existing 0755 directory left the file to
	// SQLite — which creates it 0644 minus umask, with the -wal and -shm
	// sidecars inheriting that mode. The bodies are encrypted; the rows still
	// say who talked to whom and when.
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	path := filepath.Join(directory, "state.db")

	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	// Written to while open, so the WAL sidecars exist to be inspected.
	if _, err := database.db.ExecContext(context.Background(),
		`INSERT INTO probe (id) VALUES ('m1')`); err != nil {
		t.Fatalf("write: %v", err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(path + suffix)
		if err != nil {
			t.Fatalf("stat %q: %v", path+suffix, err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			t.Fatalf("%q has mode %04o — readable outside its owner", path+suffix, mode)
		}
	}
}

func TestAnAdoptedDatabaseIsNarrowedOnceItIsProvenOurs(t *testing.T) {
	// Pre-creating an empty StateDBPath is the documented way to place a new
	// database, and an adopted pre-versioned file was written before this
	// package existed — both arrive with whatever permissions they were given.
	// Leaving them was the earlier reading of "do not touch what is not yours";
	// after a successful open it IS ours.
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("set the pre-existing mode: %v", err)
	}

	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if _, err := database.db.ExecContext(context.Background(),
		`INSERT INTO probe (id) VALUES ('m1')`); err != nil {
		t.Fatalf("write: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(path + suffix)
		if err != nil {
			t.Fatalf("stat %q: %v", path+suffix, err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			t.Fatalf("%q has mode %04o after a successful open", path+suffix, mode)
		}
	}
}

func TestARefusedDatabaseKeepsItsPermissions(t *testing.T) {
	// The other half of the same rule: until the open succeeds the file may
	// belong to another application or another identity, so nothing about it
	// is this package's to rewrite.
	path := filepath.Join(t.TempDir(), "foreign.db")
	stampForeignApplication(t, path)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("set the pre-existing mode: %v", err)
	}

	if _, err := Open(context.Background(), testConfig(t, path, testCatalog())); !errors.Is(err, ErrForeignApplication) {
		t.Fatalf("error = %v, want ErrForeignApplication", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o644 {
		t.Fatalf("mode = %04o, want 0644 unchanged — a refused file was modified", mode)
	}
}

func TestANewDatabaseIsSecuredBeforeTheBootstrapMigration(t *testing.T) {
	// A file THIS run created cannot belong to anyone else, so its sidecars
	// are secured immediately — before the ledger and before the bootstrap
	// migration.
	//
	// What this test can prove is limited, and deliberately stated: on Unix
	// the sidecars inherit the MAIN file's mode, which createOwnerOnly already
	// set, so the assertion below holds with or without that early step. The
	// step exists for Windows, where SQLite creates them without a security
	// descriptor and they inherit the DIRECTORY's ACL instead — which no test
	// here observes. What this does guard is the main file being secured at
	// creation, and the sidecars not being left behind it.
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	path := filepath.Join(directory, "state.db")

	var duringBootstrap error
	catalog := []Migration{
		{
			Version: 1, Name: "storage_metadata", SQL: metadataDDL,
			Invariant: func(context.Context, SchemaReader) error {
				duringBootstrap = assertOwnerOnly(path, "", "-wal", "-shm")
				return nil
			},
		},
		{Version: 2, Name: "probe", SQL: probeDDL},
	}

	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if duringBootstrap != nil {
		t.Fatalf("during the bootstrap migration: %v", duringBootstrap)
	}
}

func TestAnAdoptedDatabaseIsSecuredOnceTheOwnerIsVerified(t *testing.T) {
	// A file that was already there is a different matter: it may belong to
	// another identity, and its permissions are not this package's to rewrite
	// until the owner check says so. That check runs after the bootstrap
	// migration has COMMITTED, which is also what makes the change safe — a
	// rollback after a permission change could not put the permissions back.
	//
	// So: nothing during bootstrap, everything by the next migration.
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	path := filepath.Join(directory, "legacy.db")

	legacy, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("set the pre-existing mode: %v", err)
	}

	var afterOwnership error
	catalog := []Migration{
		{Version: 1, Name: "storage_metadata", SQL: metadataDDL},
		{
			Version: 2, Name: "probe", SQL: probeDDL,
			Invariant: func(context.Context, SchemaReader) error {
				afterOwnership = assertOwnerOnly(path, "", "-wal", "-shm")
				return nil
			},
		},
	}

	database, err := Open(context.Background(), testConfig(t, path, catalog))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if afterOwnership != nil {
		t.Fatalf("during the migration after the owner check: %v", afterOwnership)
	}
}

func TestARolledBackBootstrapLeavesPermissionsAlone(t *testing.T) {
	// The claim is not durable until its transaction commits, and a permission
	// change cannot be rolled back with it. This drives the failure that a
	// commit-time change would have survived: the bootstrap's own condition
	// fails, so the owner row and the version are rolled back — and the file
	// must be exactly as it was, including while another connection is holding
	// its sidecars open.
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	defer func() { _ = legacy.Close() }()

	if _, err := legacy.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("set WAL: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Chmod(path+suffix, 0o644); err != nil {
			t.Fatalf("set the pre-existing mode on %q: %v", path+suffix, err)
		}
	}

	catalog := []Migration{
		{
			Version: 1, Name: "storage_metadata", SQL: metadataDDL,
			Invariant: func(context.Context, SchemaReader) error {
				return errors.New("deliberate bootstrap failure")
			},
		},
	}
	if _, err := Open(context.Background(), testConfig(t, path, catalog)); err == nil {
		t.Fatal("Open succeeded although the bootstrap condition failed")
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, statErr := os.Stat(path + suffix)
		if statErr != nil {
			t.Fatalf("stat %q: %v", path+suffix, statErr)
		}
		if mode := info.Mode().Perm(); mode != 0o644 {
			t.Fatalf("%q has mode %04o after a rolled-back claim, want 0644", path+suffix, mode)
		}
	}

	// And the claim itself really did roll back. Asserted positively: a
	// swallowed Scan error — a lock, a damaged file — would otherwise read as
	// a successful rollback.
	var present int
	err = legacy.QueryRow(
		`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'storage_metadata'`).Scan(&present)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("storage_metadata lookup = %v (%d), want sql.ErrNoRows: the owner table survived a rolled-back claim", err, present)
	}

	var versions int
	if err := legacy.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&versions); err != nil {
		t.Fatalf("count recorded versions: %v", err)
	}
	if versions != 0 {
		t.Fatalf("the ledger holds %d versions after a rolled-back claim, want 0", versions)
	}
}

// assertOwnerOnly reports the first of the named files readable outside its
// owner.
func assertOwnerOnly(path string, suffixes ...string) error {
	for _, suffix := range suffixes {
		info, err := os.Stat(path + suffix)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("stat %q: %w", path+suffix, err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			return fmt.Errorf("%q has mode %04o", path+suffix, mode)
		}
	}
	return nil
}

func TestADatabaseOfAnotherIdentityKeepsItsPermissions(t *testing.T) {
	// The application_id says "a Corsa database", not "this node's". Narrowing
	// anything before the owner is read applied an owner-only mode — on
	// Windows, a protected DACL for the current account — to a database about
	// to be refused as somebody else's.
	//
	// The first database stays OPEN for the whole test. That is the case that
	// matters: -wal and -shm belong to the database, not to a connection, so a
	// second identity re-permissioning them would be doing it to files their
	// owner is using right now.
	path := filepath.Join(t.TempDir(), "state.db")
	owner := Config{
		ExplicitPath: path,
		Owner:        testIdentity(0xAB),
		Catalog:      testCatalog(),
		Now:          func() time.Time { return time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC) },
	}

	database, err := Open(context.Background(), owner)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	// A write so the sidecars exist and stay: they are what the intruder would
	// re-permission.
	if _, err := database.db.ExecContext(context.Background(),
		`INSERT INTO probe (id) VALUES ('m1')`); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Distinguishable modes, so any touch shows up.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Chmod(path+suffix, 0o644); err != nil {
			t.Fatalf("set the owner's mode on %q: %v", path+suffix, err)
		}
	}

	intruder := owner
	intruder.Owner = testIdentity(0xCD)
	if _, err := Open(context.Background(), intruder); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("error = %v, want ErrOwnerMismatch", err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, statErr := os.Stat(path + suffix)
		if statErr != nil {
			t.Fatalf("stat %q: %v", path+suffix, statErr)
		}
		if mode := info.Mode().Perm(); mode != 0o644 {
			t.Fatalf("%q has mode %04o, want 0644 unchanged — another identity's file was modified", path+suffix, mode)
		}
	}
}

func TestADamagedMarkerDoesNotMakeADatabaseLookFresh(t *testing.T) {
	// A cleared application_id on a file that already holds a ledger and an
	// owner row is not a fresh database — it is a versioned one with a damaged
	// marker, and it is refused. Reading it as "unclaimed" was how that
	// refusal still got to re-permission somebody else's live sidecars.
	//
	// The database stays OPEN throughout: -wal and -shm belong to it, not to a
	// connection.
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := Open(context.Background(), testConfig(t, path, testCatalog()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if _, err := database.db.ExecContext(context.Background(),
		`INSERT INTO probe (id) VALUES ('m1')`); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The damage, from outside: the marker is gone, the history is not.
	intruderView, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := intruderView.Exec(`PRAGMA application_id = 0`); err != nil {
		t.Fatalf("clear the marker: %v", err)
	}
	if err := intruderView.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Chmod(path+suffix, 0o644); err != nil {
			t.Fatalf("set a distinguishable mode on %q: %v", path+suffix, err)
		}
	}

	if _, err := Open(context.Background(), testConfig(t, path, testCatalog())); !errors.Is(err, ErrForeignApplication) {
		t.Fatalf("error = %v, want ErrForeignApplication", err)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, statErr := os.Stat(path + suffix)
		if statErr != nil {
			t.Fatalf("stat %q: %v", path+suffix, statErr)
		}
		if mode := info.Mode().Perm(); mode != 0o644 {
			t.Fatalf("%q has mode %04o, want 0644 unchanged — a refused database was modified", path+suffix, mode)
		}
	}
}

func TestConcurrentAdoptionProducesExactlyOneOwner(t *testing.T) {
	// Two identities adopting the same pre-versioned file at once. Deciding
	// "this database is unclaimed, narrow its sidecars" outside the write lock
	// was a race: the other process could finish its bootstrap in between, and
	// the loser would re-permission the WINNER's live sidecars before being
	// refused. The decision now lives inside the transaction that records the
	// owner, so only the claimant ever touches them.
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open(sqliteDriverName, DSN(path))
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	const contenders = 4
	var ready sync.WaitGroup
	ready.Add(contenders)
	results := make(chan error, contenders)
	databases := make(chan *Database, contenders)

	for i := 0; i < contenders; i++ {
		go func(seed byte) {
			ready.Done()
			ready.Wait()

			database, err := Open(context.Background(), Config{
				ExplicitPath: path,
				Owner:        testIdentity(seed),
				Catalog:      testCatalog(),
				Now:          func() time.Time { return time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC) },
			})
			if database != nil {
				databases <- database
			}
			results <- err
		}(byte(0xA0 + i))
	}

	claimed := 0
	for i := 0; i < contenders; i++ {
		switch err := <-results; {
		case err == nil:
			claimed++
		case errors.Is(err, ErrOwnerMismatch):
		default:
			t.Fatalf("contender %d: error = %v, want nil or ErrOwnerMismatch", i, err)
		}
	}
	close(databases)
	for database := range databases {
		t.Cleanup(func() { _ = database.Close() })
	}

	if claimed != 1 {
		t.Fatalf("%d contenders claimed the database, want exactly 1", claimed)
	}
	// What this proves is the CLAIM, and only that: exactly one contender
	// records itself as the owner and the rest are refused. It does not prove
	// that the losers touched no permissions — every contender here runs in
	// one process under one OS account, so their modes and DACLs are
	// indistinguishable, and both the current and the earlier implementation
	// end at the winner's 0600. That property is covered deterministically
	// instead, by TestADatabaseOfAnotherIdentityKeepsItsPermissions and
	// TestARolledBackBootstrapLeavesPermissionsAlone, where the refused open
	// is sequential and the file's mode before it is known.
	if err := assertOwnerOnly(path, "", "-wal", "-shm"); err != nil {
		t.Fatalf("after the race: %v", err)
	}
}

func TestAPreCreatedEmptyFileIsNotRePermissionedBeforeTheClaim(t *testing.T) {
	// Pre-creating an empty StateDBPath is documented, and an empty file could
	// equally have been made by another process a moment ago. Zero length says
	// "not a database"; it does not say "mine". So it is left exactly as it is
	// until the claim is durable — a refused open must change nothing, and a
	// permission change cannot be rolled back with the transaction.
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatalf("create the directory: %v", err)
	}
	path := filepath.Join(directory, "state.db")

	placed, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatalf("pre-create the file: %v", err)
	}
	if err := placed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	catalog := []Migration{
		{
			Version: 1, Name: "storage_metadata", SQL: metadataDDL,
			Invariant: func(context.Context, SchemaReader) error {
				return errors.New("deliberate bootstrap failure")
			},
		},
	}
	if _, err := Open(context.Background(), testConfig(t, path, catalog)); err == nil {
		t.Fatal("Open succeeded although the bootstrap condition failed")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o644 {
		t.Fatalf("mode = %04o, want 0644 unchanged — a file whose claim never completed was modified", mode)
	}
}

func TestACancelledCheckIsNotReportedAsCorruption(t *testing.T) {
	// errorClass tests ErrCorrupt before cancellation, so wrapping a PRAGMA
	// that failed to RUN in ErrCorrupt told the operator their healthy
	// database was damaged and hid the real cause. ErrCorrupt now means only
	// one thing: the check ran and reported a problem.
	//
	// Driven at the checks themselves: a pre-cancelled Open fails earlier, in
	// catalog validation, so it never reaches them.
	path := filepath.Join(t.TempDir(), "state.db")
	database := openTest(t, path, testCatalog())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := checkIntegrity(ctx, database.db); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkIntegrity error = %v, want context.Canceled", err)
	} else if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a cancelled integrity_check was reported as corruption: %v", err)
	}

	if _, err := checkForeignKeys(ctx, database.db); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkForeignKeys error = %v, want context.Canceled", err)
	} else if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a cancelled foreign_key_check was reported as corruption: %v", err)
	}
}

func TestAFailureUnderACancelledCallerKeepsTheContextCause(t *testing.T) {
	// keepContextCause is what makes the classification exhaustive: the
	// reference is built by EXECUTING the catalog, and pinning the connection,
	// running a migration and reading the schema back can each fail because
	// the caller went away. Only the first of those is reachable with a
	// context cancelled up front, so the rule itself is driven here.
	live, cancel := context.WithCancel(context.Background())
	defer cancel()

	unrelated := fmt.Errorf("%w: version 2 does not execute", ErrCatalogInvalid)
	if got := keepContextCause(live, unrelated); !errors.Is(got, ErrCatalogInvalid) {
		t.Fatalf("with a live caller the error was rewritten: %v", got)
	}
	if got := keepContextCause(live, nil); got != nil {
		t.Fatalf("keepContextCause(nil) = %v, want nil", got)
	}

	gone, stop := context.WithCancel(context.Background())
	stop()

	got := keepContextCause(gone, unrelated)
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", got)
	}
	if errors.Is(got, ErrCatalogInvalid) {
		t.Fatalf("a cancelled caller was still reported as an invalid catalog: %v", got)
	}
}

func TestACancelledReferenceBuildIsNotAnInvalidCatalog(t *testing.T) {
	// The same mistake in the other classifier: a correct catalog became
	// matchable as ErrCatalogInvalid, and a healthy process shutting down was
	// logged as a programming error.
	//
	// With the context cancelled up front the failure comes from pinning the
	// connection, which reports the cause plainly; the rule that covers the
	// deeper failures is driven by the test above.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := BuildReferenceSchema(ctx, testCatalog(), LatestVersion(testCatalog()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("a cancelled reference build was reported as an invalid catalog: %v", err)
	}
	if class := errorClass(err); class != "cancelled" {
		t.Fatalf("errorClass = %q, want %q", class, "cancelled")
	}
}

func TestAClearedMarkerCannotHideALostHistory(t *testing.T) {
	// The combination the previous rule missed: clear application_id AND
	// remove the history, but leave storage_metadata. That used to read as
	// "pre-versioned", so the ledger was recreated, the marker restamped and
	// every version recorded afresh over the existing schema — the loss of the
	// checksum history hidden behind a clean start.
	//
	// storage_metadata is created by the bootstrap migration and that
	// migration's ledger row commits in the same transaction, so the table
	// without a history can only be damage.
	damage := map[string]string{
		"ledger dropped": `PRAGMA application_id = 0;
DROP TABLE schema_migrations;`,
		"ledger emptied": `PRAGMA application_id = 0;
DELETE FROM schema_migrations;`,
	}

	for name, statements := range damage {
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
			for _, statement := range strings.Split(statements, ";\n") {
				if strings.TrimSpace(statement) == "" {
					continue
				}
				if _, err := damaged.Exec(statement); err != nil {
					t.Fatalf("apply %q: %v", statement, err)
				}
			}
			if err := damaged.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible — the history was rebuilt from nothing", err)
			}
		})
	}
}

func TestAVersionRecordedAfterTheLastMigrationIsRefused(t *testing.T) {
	// Another process running a newer catalog can record a version between
	// this run's last migration and the final checks. The schema check proves
	// the ledger's DDL, not its content, so this binary used to hand back a
	// ready Database sitting on a version it does not know.
	//
	// Driven at the seam, deliberately: a version seeded before Open is
	// already caught by the pre-flight verifyLedger inside migrate, so an
	// end-to-end test would pass with the final check deleted. This calls the
	// final check itself, on a ledger in exactly the state the race leaves.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := testCatalog()
	database := openTest(t, path, catalog)

	ctx := context.Background()
	if version, err := verifyRecordedHistory(ctx, database.db, catalog); err != nil {
		t.Fatalf("verifyRecordedHistory on a healthy database: %v", err)
	} else if version != LatestVersion(catalog) {
		t.Fatalf("version = %s, want %s", version, LatestVersion(catalog))
	}

	// What the other process leaves behind, after this run migrated.
	if _, err := database.db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name, checksum, applied_at)
		 VALUES (?, 'from_a_newer_binary', 'ffff', '2026-08-19T12:00:00Z')`,
		int(LatestVersion(catalog))+1); err != nil {
		t.Fatalf("record a newer version: %v", err)
	}

	if _, err := verifyRecordedHistory(ctx, database.db, catalog); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("error = %v, want ErrSchemaTooNew", err)
	}
}

func TestAnOpenOnANewerFileIsRefused(t *testing.T) {
	// The end-to-end half of the same rule: a file already carrying a newer
	// version is refused at the pre-flight, before any migration runs.
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := testCatalog()

	database := openTest(t, path, catalog)
	if _, err := database.db.ExecContext(context.Background(),
		`INSERT INTO schema_migrations (version, name, checksum, applied_at)
		 VALUES (?, 'from_a_newer_binary', 'ffff', '2026-08-19T12:00:00Z')`,
		int(LatestVersion(catalog))+1); err != nil {
		t.Fatalf("record a newer version: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := Open(context.Background(), testConfig(t, path, catalog)); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("error = %v, want ErrSchemaTooNew", err)
	}
}

func TestADamagedOwnerTableIsASchemaFailure(t *testing.T) {
	// The owner read happens long before the full schema check, so a dropped
	// table or a removed column surfaced as a plain driver error: the operator
	// got error_class "sql" for a database whose shape was the problem.
	damage := map[string]string{
		"table dropped":  `DROP TABLE storage_metadata`,
		"column removed": `ALTER TABLE storage_metadata DROP COLUMN created_at`,
		// The case that message-matching missed: the table is there and every
		// column is there, so the read gets as far as converting values —
		// and fails on a NULL, or on a bootstrap_version that is not a
		// number, with an error that says nothing about the schema.
		"recreated as nullable": `DROP TABLE storage_metadata;
CREATE TABLE storage_metadata (
	id                INTEGER PRIMARY KEY CHECK (id = 1),
	owner_identity    TEXT,
	bootstrap_version TEXT,
	created_at        TEXT
);
INSERT INTO storage_metadata (id, owner_identity, bootstrap_version, created_at)
VALUES (1, NULL, 'not a number', NULL)`,
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
			for _, part := range strings.Split(statement, ";\n") {
				if strings.TrimSpace(part) == "" {
					continue
				}
				if _, err := damaged.Exec(part); err != nil {
					t.Fatalf("apply %q: %v", part, err)
				}
			}
			if err := damaged.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			_, err = Open(context.Background(), testConfig(t, path, catalog))
			if err == nil {
				t.Fatal("Open accepted a damaged owner table")
			}
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error = %v, want ErrSchemaIncompatible", err)
			}
			if class := errorClass(err); class != "schema-incompatible" {
				t.Fatalf("errorClass = %q, want %q", class, "schema-incompatible")
			}
		})
	}
}
