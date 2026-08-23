// Package storage owns the shared SQLite state database of a Corsa node.
//
// One file per local identity and listen port holds every SQLite-backed
// subsystem. The database is opened once by the composition root, migrated to
// the version this binary knows, and handed to repositories as a non-owning
// Executor: a repository issues SQL, but never opens, migrates or closes the
// file. Schema changes go through the single global migration catalog, which
// is forward-only — see docs/storage.md.
//
// Any problem that leaves the schema unproven (corruption, a foreign file, a
// newer version, an edited migration, an unexpected table shape) aborts Open.
// The file is never renamed, replaced or repaired automatically: it holds
// durable message history, and a silent rebuild would destroy it.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"

	"github.com/rs/zerolog/log"
)

// ownerTable records whose database this file is. Like the migration ledger,
// it belongs to this package and is off limits to the catalog.
const ownerTable = "storage_metadata"

// applicationID is the value stamped into PRAGMA application_id: the ASCII
// bytes "CRSA". A zero value means the file has never been claimed (a fresh
// database, or the pre-versioned chatlog one); any other non-zero value means
// the node was pointed at somebody else's SQLite file.
const applicationID = 0x43525341

// bootstrapMigrationVersion is the version that creates storage_metadata.
// Ownership is validated immediately after it and before the rest of the
// catalog runs, so a database belonging to another identity is rejected
// without having its schema modified.
const bootstrapMigrationVersion Version = 1

// storageBootstrapFormat is the version of the storage_metadata contract
// itself, recorded so a future bootstrap change can recognise old rows.
const storageBootstrapFormat = 1

// PathSource records how the database location was chosen, for the startup
// log and for diagnosing "where did my history go" reports.
type PathSource string

const (
	// PathSourceExplicit means the operator set an absolute path.
	PathSourceExplicit PathSource = "explicit"
	// PathSourceLegacyDefault means the historical
	// chatlog-<identity_short>-<port>.db name inside the data directory.
	// The name is kept deliberately: an older binary rolled back onto the
	// same machine finds the full history where it expects it.
	PathSourceLegacyDefault PathSource = "legacy-default"
)

// Location is a resolved database file together with the reason it was chosen.
type Location struct {
	Path   string
	Source PathSource
}

// Executor is the non-owning database handle a repository receives.
//
// It deliberately omits Close — the composition root is the only owner of the
// connection pool — and it deliberately omits the context-free Exec, Query,
// QueryRow and Begin. Every statement a repository issues carries the caller's
// context, so a cancelled RPC or a shutdown deadline actually stops the query
// instead of leaving it holding a connection while the database is closing
// underneath it.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Config is the complete input of Open.
type Config struct {
	// ExplicitPath overrides the derived location (CORSA_STATE_DB_PATH).
	// When empty, the legacy default file inside DataDir is used. Nothing
	// is ever copied between the two: an empty explicit file means a
	// deliberately new database, not a lost one.
	ExplicitPath string

	// DataDir is the base directory holding node-local state. Only used to
	// derive the legacy default location.
	DataDir string

	// ListenAddress supplies the port half of the legacy file name.
	ListenAddress domain.ListenAddress

	// Owner is the local identity. It is recorded on first use and verified
	// on every subsequent open.
	Owner domain.PeerIdentity

	// Catalog is the ordered, gapless migration list. It is passed in
	// explicitly rather than registered through init() so the exact set and
	// order are visible at the composition root and in tests.
	Catalog []Migration

	// Now supplies ledger and metadata timestamps. nil means time.Now.
	Now func() time.Time
}

// Database is the opened, migrated state database. Only the composition root
// that called Open may Close it.
type Database struct {
	db        *sql.DB
	now       func() time.Time
	location  Location
	owner     domain.PeerIdentity
	version   Version
	closeOnce sync.Once
	closeErr  error
}

// Open resolves the database location, verifies the file, brings it to the
// latest catalog version and returns the ready database.
//
// The sequence is fixed: the catalog is validated before the file is touched,
// integrity is checked before anything is written, ownership is settled as
// soon as the metadata table exists, and referential integrity is confirmed
// after the last migration.
func Open(ctx context.Context, cfg Config) (*Database, error) {
	database, err := open(ctx, cfg)
	if err != nil {
		// One event for EVERY refusal, not only for a migration step that
		// started and failed. Corruption, a foreign application, an owner
		// mismatch, a schema newer than this binary and a failed final check
		// all abort startup with nothing recorded, and several of them happen
		// before the first startup line is written — the operator would have
		// had the process exit with no structured trace of why.
		path, source, resolved := refusedLocation(cfg)
		log.Error().
			Str("error_class", errorClass(err)).
			Str("db_path", path).
			Str("path_source", string(source)).
			Bool("path_resolved", resolved).
			Msg("storage state database refused")
		return nil, err
	}
	return database, nil
}

// databaseFileMode is the permission a database this package creates gets:
// readable and writable by its owner and by nobody else.
const databaseFileMode = 0o600

// createOwnerOnly creates the database file with owner-only permissions if it
// does not exist yet, and leaves an existing one alone.
//
// O_EXCL is what makes the second half true. A file that is already there has
// not been validated yet — it may belong to another application or another
// identity, and may be about to be refused — so its permissions are not this
// package's to rewrite. Only the one created here is chmod-ed, because umask
// could otherwise take the mode further than intended.
//
// An empty file IS an empty SQLite database, so creating it changes nothing
// about what the driver then does with it.
func createOwnerOnly(path string) (created bool, err error) {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE|os.O_EXCL, databaseFileMode)
	if errors.Is(err, fs.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("storage: create %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("storage: create %s: %w", path, err)
	}
	return true, restrictFileAccess(path)
}

// open is Open without the refusal log: one place to fail from, so every exit
// is reported the same way.
func open(ctx context.Context, cfg Config) (*Database, error) {
	if err := ValidateCatalog(ctx, cfg.Catalog); err != nil {
		return nil, err
	}
	if cfg.Owner.IsZero() {
		return nil, fmt.Errorf("storage: owner identity is required")
	}

	location, err := resolveLocation(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(location.Path), 0o700); err != nil {
		return nil, fmt.Errorf("storage: create data directory for %s: %w", location.Path, err)
	}

	// The file is created HERE, owner-only, rather than by SQLite on first
	// write: SQLite creates it 0644 minus umask, and the -wal and -shm
	// sidecars inherit that mode. The rows are not message bodies, which are
	// encrypted, but they are who talked to whom and when — enough for any
	// other local account to reconstruct the correspondence.
	createdHere, err := createOwnerOnly(location.Path)
	if err != nil {
		return nil, err
	}

	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	db, err := sql.Open(sqliteDriverName, DSN(location.Path))
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", location.Path, err)
	}

	database, err := prepare(ctx, db, cfg, location, now, createdHere)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return database, nil
}

// refusedLocation names the file a refusal is about, in the same terms as the
// startup line: the RESOLVED path and where it came from.
//
// Resolution trims, absolutises and cleans, so logging the configured value
// instead could name a different file from the one that was actually opened —
// a relative StateDBPath being the everyday case.
//
// When resolution is what failed there is still a source: the operator either
// set a path or did not. Only the location is unknown, and that is reported by
// path_resolved rather than by inventing a third PathSource, which every
// consumer of these logs would then have to learn.
func refusedLocation(cfg Config) (path string, source PathSource, resolved bool) {
	source = PathSourceLegacyDefault
	configured := strings.TrimSpace(cfg.DataDir)
	if explicit := strings.TrimSpace(cfg.ExplicitPath); explicit != "" {
		source, configured = PathSourceExplicit, explicit
	}

	location, err := resolveLocation(cfg)
	if err != nil {
		return configured, source, false
	}
	return location.Path, location.Source, true
}

// materialiseSidecars brings the -wal and -shm into existence so that they can
// be secured before anything is written into them.
//
// Setting WAL mode does not create them: SQLite does that at the first write
// transaction, which here would be the ledger. An empty transaction is the
// cheapest way to ask for them.
func materialiseSidecars(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("storage: pin connection to create the WAL sidecars: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("storage: create the WAL sidecars: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return fmt.Errorf("storage: create the WAL sidecars: %w", err)
	}
	return nil
}

// restrictSidecars narrows the WAL sidecars alone, for the window in which
// the database's owner is not yet known.
func restrictSidecars(path string) error {
	return restrictSuffixes(path, "-wal", "-shm")
}

// restrictDatabaseFiles narrows the database and its WAL sidecars to their
// owner. The sidecars are included because SQLite creates them from the main
// file's permissions, and a -wal holds committed rows until the next
// checkpoint.
func restrictDatabaseFiles(path string) error {
	return restrictSuffixes(path, "", "-wal", "-shm")
}

// restrictSuffixes narrows whichever of the named files exist.
func restrictSuffixes(path string, suffixes ...string) error {
	for _, suffix := range suffixes {
		if _, err := os.Stat(path + suffix); errors.Is(err, fs.ErrNotExist) {
			continue // the sidecars exist only while a connection is open
		} else if err != nil {
			return fmt.Errorf("storage: inspect %s: %w", path+suffix, err)
		}
		if err := restrictFileAccess(path + suffix); err != nil {
			return err
		}
	}
	return nil
}

// prepare runs every check and migration on an already opened pool. Split out
// so Open has a single place that closes the pool on any failure.
func prepare(ctx context.Context, db *sql.DB, cfg Config, location Location, now func() time.Time, createdHere bool) (*Database, error) {
	if err := db.PingContext(ctx); err != nil {
		// A file that is not a database announces itself here, before any
		// check runs: the first read of the header is what fails.
		return nil, classifyCheckFailure("opening "+location.Path, err)
	}
	// Read-only checks first. Switching the journal mode is a WRITE to the
	// file, so doing it earlier would convert somebody else's database to
	// WAL and only then reject it with ErrForeignApplication — a refusal
	// that already modified what it refused.
	integrity, err := checkIntegrity(ctx, db)
	if err != nil {
		return nil, err
	}
	if err := rejectForeignApplication(ctx, db); err != nil {
		return nil, err
	}
	if err := ensureWALMode(ctx, db); err != nil {
		return nil, err
	}

	// A file THIS run created cannot belong to anyone else, so its sidecars
	// can be secured immediately — which matters on Windows, where SQLite
	// creates them without a security descriptor and they inherit the
	// directory's ACL. Setting WAL mode does not create them; the first write
	// does, so an empty transaction asks for them first.
	//
	// For every OTHER file — including one this package left behind when an
	// earlier open failed after writing — nothing is touched until
	// verifyOwnership below. Not before it, because the file may belong to
	// another identity and a refusal must leave it exactly as it was; and not
	// from inside the claiming transaction either, because a rollback after
	// that point — a failed read-back, a failed condition, a cancelled
	// context — would undo the claim and leave the permission change behind,
	// with no way to put it back.
	//
	// The cost is a retry after a failed bootstrap: its file is no longer
	// empty, so on Windows its sidecars carry the directory's ACL until the
	// owner check. That is the same window every adoption of an existing file
	// has, and closing it would mean re-permissioning a file whose owner is
	// still unknown — which is the larger risk of the two.
	if createdHere {
		if err := materialiseSidecars(ctx, db); err != nil {
			return nil, err
		}
		if err := restrictSidecars(location.Path); err != nil {
			return nil, err
		}
	}

	// A file that already carries this package's marks must already carry a
	// ledger WITH history in it. Creating one for it turned a deleted or swapped migration
	// history into an empty one, and every version was then recorded afresh
	// over a schema nobody had checked — the one state this whole mechanism
	// exists to detect.
	if err := rejectEmptyLedger(ctx, db); err != nil {
		return nil, err
	}

	// The ledger's shape is proven before anything reads it: a
	// schema_migrations without a version column would otherwise surface as
	// a raw driver error from currentVersion instead of ErrSchemaIncompatible.
	if err := ensureLedger(ctx, db); err != nil {
		return nil, err
	}

	latest := LatestVersion(cfg.Catalog)
	before, err := currentVersion(ctx, db)
	if err != nil {
		return nil, err
	}

	// Stamping waits for the recorded version: an unstamped file is a fresh or
	// pre-versioned one, but an unstamped file that already carries a ledger
	// had its marker cleared from outside, and adopting it would erase the
	// evidence.
	if err := stampApplicationID(ctx, db, before); err != nil {
		return nil, err
	}

	log.Info().
		Str("db_path", location.Path).
		Str("path_source", string(location.Source)).
		Str("schema_version", before.String()).
		Str("target_version", latest.String()).
		Msg("storage opening state database")

	// Ownership is settled between the bootstrap migration and the rest of
	// the catalog: storage_metadata must exist to read the owner, and no
	// further schema of ours belongs in a file owned by somebody else.
	bootstrap, err := migrate(ctx, db, migrationRun{
		Catalog:     cfg.Catalog,
		Limit:       bootstrapMigrationVersion,
		Now:         now,
		SealVersion: bootstrapMigrationVersion,
		Seal: func(ctx context.Context, conn *sql.Conn) error {
			return recordOwner(ctx, conn, cfg.Owner, now)
		},
	})
	logAttempted(bootstrap, err)
	if err != nil {
		return nil, err
	}

	// The SHAPE of the owner table is proven before its row is read. The full
	// schema check runs much later, so a table recreated with different
	// columns — or with nullable ones holding NULL — used to surface as a
	// driver error from the read itself: the operator got error_class "sql"
	// for a database whose shape was the problem. Classifying driver messages
	// covered only the spellings I had thought of; comparing the object with
	// what the catalog declares covers the class.
	if err := verifyOwnerTableShape(ctx, db, cfg.Catalog); err != nil {
		return nil, err
	}
	if err := verifyOwnership(ctx, db, cfg.Owner); err != nil {
		return nil, err
	}

	// Ownership is proven, so the file itself can be narrowed now: an
	// operator pre-creating an empty StateDBPath is the documented way to
	// place a new database, and an adopted pre-versioned file was written
	// before this package existed.
	if err := restrictDatabaseFiles(location.Path); err != nil {
		return nil, err
	}

	applied, err := migrate(ctx, db, migrationRun{Catalog: cfg.Catalog, Limit: latest, Now: now})
	logAttempted(applied, err)
	if err != nil {
		return nil, err
	}

	// Every version the catalog knows is re-verified on every open, not just
	// the ones applied on this run. A recorded version is otherwise never
	// looked at again: a table dropped, or a swallowing trigger added, after
	// a successful start would then survive every later start too, and the
	// repository would meet it at runtime instead.
	full, err := BuildReferenceSchema(ctx, cfg.Catalog, latest)
	if err != nil {
		return nil, err
	}
	if err := VerifySchema(ctx, db, full, full); err != nil {
		return nil, err
	}
	// The ledger is verified again here, not only before the catalog ran: a
	// migration that reached into it is refused at catalog validation, but a
	// database damaged between runs must not be declared ready either.
	ledger, err := ledgerReference(ctx)
	if err != nil {
		return nil, err
	}
	if err := VerifySchema(ctx, db, ledger, ledger); err != nil {
		return nil, err
	}

	// The runner-owned markers are re-checked AFTER the catalog ran. A
	// migration cannot legitimately touch them — PRAGMA is refused at catalog
	// validation, and the ledger and storage_metadata are refused as targets —
	// so anything different here is damage from outside this package, and this
	// check reports it rather than repairing it. Restamping would hide the
	// damage and hand the caller a database whose markers this process just
	// invented.
	if err := confirmMarkers(ctx, db, cfg.Owner); err != nil {
		return nil, err
	}

	violations, err := checkForeignKeys(ctx, db)
	if err != nil {
		return nil, err
	}

	after, err := verifyRecordedHistory(ctx, db, cfg.Catalog)
	if err != nil {
		return nil, err
	}

	// Re-applied here because a sidecar can appear at any point above, and
	// because NOTHING that can fail may run after the ready line: a monitor
	// reading it would otherwise count a process that never started as up.
	if err := restrictDatabaseFiles(location.Path); err != nil {
		return nil, err
	}

	// The two integrity checks report their PASSING result as well: an
	// operator reading this line must be able to tell a database that was
	// checked from one where the checks never ran.
	log.Info().
		Str("db_path", location.Path).
		Str("schema_version_before", before.String()).
		Str("schema_version_after", after.String()).
		Int("migrations_applied", len(bootstrap)+len(applied)).
		Str("integrity_check", integrity).
		Int("foreign_key_violations", violations).
		Msg("storage state database ready")

	return &Database{
		db:       db,
		now:      now,
		location: location,
		owner:    cfg.Owner,
		version:  after,
	}, nil
}

// DriverName reports the database/sql driver name this build links. Exported
// so the driver contract suite and the legacy-adoption tests address the same
// driver the node uses instead of duplicating the build-tag selection.
func DriverName() string { return sqliteDriverName }

// DSN builds the connection string for a database file.
//
// The path is carried as a file: URI rather than pasted in front of the
// options. Both drivers cut a plain path at its first "?" to find their
// parameters, so a database at /data/state?backup.db silently opened /data/state
// while Location() and every log line named the file the operator asked for.
// Percent-encoding puts the whole path back inside the name.
func DSN(path string) string {
	return fileURI(path, os.PathSeparator) + sqliteDSNOptions
}

// fileURI renders a local absolute path as a file: URI, for a filesystem whose
// separator is the given one.
//
// The separator is a parameter rather than read from the host, because the two
// cases that differ are the whole point of this function and only one of them
// can be observed on any single machine: a Windows path has to be tested on
// Linux and the other way round.
//
// Both adjustments below exist for Windows and are no-ops for a POSIX path,
// which already starts at "/" and already separates with it:
//
//   - the separators become "/". A file: URI has no other path separator, and
//     a backslash left in place is percent-encoded as %5C, i.e. a file name
//     containing that character rather than a directory boundary. Backslashes
//     are NOT rewritten under a "/" separator — there they are an ordinary,
//     legal character in a POSIX file name.
//   - a leading "/" is prepended when the path does not have one. The URI
//     authority runs from "//" to the next "/", so C:/dir/state.db turned the
//     drive into the host and SQLite refused the whole DSN with "invalid uri
//     authority" — this crashed every Windows start of the node. A UNC path
//     already starts with its own two separators and keeps them: file:////host
//     leaves the authority empty and hands SQLite back the \\host\share form.
func fileURI(path string, separator rune) string {
	if separator != '/' {
		path = strings.ReplaceAll(path, string(separator), "/")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	uri := url.URL{Scheme: "file", Path: path}
	return uri.String()
}

// Executor returns the non-owning handle for repositories.
func (d *Database) Executor() Executor { return d.db }

// Location reports the resolved file and how it was chosen.
func (d *Database) Location() Location { return d.location }

// SchemaVersion reports the version the file is at after Open.
func (d *Database) SchemaVersion() Version { return d.version }

// Owner reports the identity the database is bound to.
func (d *Database) Owner() domain.PeerIdentity { return d.owner }

// Close releases the connection pool. Safe to call more than once; only the
// first call does the work, and every caller sees the same result.
//
// The caller must have stopped every writer first: SQLite has no way to
// distinguish "shutting down" from "lost the database" for an in-flight
// message write. See docs/storage.md for the shutdown order.
func (d *Database) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = d.db.Close()
		if d.closeErr != nil {
			log.Error().Err(d.closeErr).Str("db_path", d.location.Path).Msg("storage close failed")
			return
		}
		log.Info().Str("db_path", d.location.Path).Msg("storage state database closed")
	})
	return d.closeErr
}

// resolveLocation picks the file to open. An explicit path always wins.
func resolveLocation(cfg Config) (Location, error) {
	if explicit := strings.TrimSpace(cfg.ExplicitPath); explicit != "" {
		absolute, err := filepath.Abs(explicit)
		if err != nil {
			return Location{}, fmt.Errorf("storage: resolve explicit path %q: %w", explicit, err)
		}
		return Location{Path: filepath.Clean(absolute), Source: PathSourceExplicit}, nil
	}

	dataDir := strings.TrimSpace(cfg.DataDir)
	if dataDir == "" {
		return Location{}, fmt.Errorf("storage: data directory is required when no explicit path is set")
	}
	absolute, err := filepath.Abs(dataDir)
	if err != nil {
		return Location{}, fmt.Errorf("storage: resolve data directory %q: %w", dataDir, err)
	}
	return Location{
		Path:   filepath.Join(filepath.Clean(absolute), LegacyFileName(cfg.Owner, cfg.ListenAddress)),
		Source: PathSourceLegacyDefault,
	}, nil
}

// LegacyFileName returns the historical chatlog file name. Exported because
// the rollback contract depends on it staying byte-identical to what the
// pre-versioned binary wrote, so tests assert it directly.
func LegacyFileName(owner domain.PeerIdentity, listenAddress domain.ListenAddress) string {
	short := owner.String()
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("chatlog-%s-%s.db", short, portSuffix(string(listenAddress)))
}

// portSuffix extracts the port from a listen address, matching the name the
// pre-versioned chatlog produced for the same input.
func portSuffix(listenAddress string) string {
	if index := strings.LastIndex(listenAddress, ":"); index >= 0 && index < len(listenAddress)-1 {
		return listenAddress[index+1:]
	}
	return "default"
}

// walSwitchTimeout bounds how long Open waits for another process to finish
// switching the file to WAL. It matches the drivers' busy_timeout: the wait is
// the same kind of contention, just on a pragma SQLite does not route through
// the busy handler.
const walSwitchTimeout = 5 * time.Second

// walSwitchInterval is the poll interval of that wait. The switch itself takes
// microseconds; this only has to be short enough not to add visible startup
// latency.
const walSwitchInterval = 25 * time.Millisecond

// ensureWALMode puts the FILE into WAL journal mode, retrying while another
// process is doing the same.
//
// Journal mode belongs to the file, not to a connection, and switching it
// needs an exclusive lock that SQLite refuses with SQLITE_BUSY immediately —
// it does NOT consult busy_timeout for this one. Two nodes starting at the
// same moment against the same fresh database therefore raced, and the loser
// failed to start.
//
// The retry needs no driver-specific error inspection: the pragma reports the
// resulting mode, so success is confirmed by the returned value rather than by
// the absence of an error. Once the file is in WAL the statement is a no-op
// that takes no exclusive lock, so every later open returns on the first try.
func ensureWALMode(ctx context.Context, db *sql.DB) error {
	deadline := walSwitchTimeout
	var lastErr error
	for waited := time.Duration(0); ; waited += walSwitchInterval {
		var mode string
		err := db.QueryRowContext(ctx, "PRAGMA journal_mode = wal").Scan(&mode)
		if err == nil && strings.EqualFold(mode, "wal") {
			return nil
		}
		if err == nil {
			lastErr = fmt.Errorf("journal_mode is %q", mode)
		} else {
			lastErr = err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("storage: enable WAL: %w", ctx.Err())
		}
		if waited >= deadline {
			return fmt.Errorf("storage: enable WAL after %s: %w", deadline, lastErr)
		}

		timer := time.NewTimer(walSwitchInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("storage: enable WAL: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

// checkIntegrity runs PRAGMA integrity_check before anything is written.
func checkIntegrity(ctx context.Context, db *sql.DB) (string, error) {
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return "", classifyCheckFailure("integrity_check", err)
	}
	if !strings.EqualFold(result, "ok") {
		return result, fmt.Errorf("%w: integrity_check reported %q", ErrCorrupt, result)
	}
	return result, nil
}

// checkForeignKeys confirms no orphaned references after the migrations.
func checkForeignKeys(ctx context.Context, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return 0, classifyCheckFailure("foreign_key_check", err)
	}
	defer func() { _ = rows.Close() }()

	var violations int
	for rows.Next() {
		violations++
	}
	if err := rows.Err(); err != nil {
		return 0, classifyCheckFailure("foreign_key_check", err)
	}
	if violations > 0 {
		return violations, fmt.Errorf("%w: foreign_key_check reported %d violations", ErrCorrupt, violations)
	}
	return violations, nil
}

// rejectForeignApplication refuses a file stamped by another application
// before anything in it is written to.
func rejectForeignApplication(ctx context.Context, db *sql.DB) error {
	stamped, err := readApplicationID(ctx, db)
	if err != nil {
		return err
	}
	if stamped != 0 && stamped != applicationID {
		return fmt.Errorf("%w: application_id is %d, want %d", ErrForeignApplication, stamped, applicationID)
	}
	return nil
}

// stampApplicationID adopts an unclaimed file. A file that already records a
// schema version is not unclaimed: its marker was cleared, and re-stamping
// would present damage as a normal open.
func stampApplicationID(ctx context.Context, db *sql.DB, recorded Version) error {
	stamped, err := readApplicationID(ctx, db)
	if err != nil {
		return err
	}
	if stamped == applicationID {
		return nil
	}
	if stamped != 0 {
		return fmt.Errorf("%w: application_id is %d, want %d", ErrForeignApplication, stamped, applicationID)
	}
	if recorded != LegacyVersion {
		return fmt.Errorf("%w: application_id is unset on a database that records version %s",
			ErrForeignApplication, recorded)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", applicationID)); err != nil {
		return fmt.Errorf("storage: stamp application_id: %w", err)
	}
	return nil
}

// readApplicationID reads the file's application marker.
func readApplicationID(ctx context.Context, db *sql.DB) (int64, error) {
	var stamped int64
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&stamped); err != nil {
		return 0, fmt.Errorf("storage: read application_id: %w", err)
	}
	return stamped, nil
}

// rejectEmptyLedger refuses a database that records an owner and no history.
//
// The OWNER TABLE is the proof, on its own: storage_metadata is created by the
// bootstrap migration, and that migration's ledger row commits in the same
// transaction, so a database with the table has recorded at least one version.
// An absent or empty ledger next to it is damage, and bootstrapping over it
// would rebuild a history that never happened.
//
// The marker deliberately plays no part. Requiring application_id as well was
// a hole: clearing it on a versioned database and dropping the ledger made the
// file look pre-versioned, and every version was then recorded afresh over the
// existing schema.
//
// The rows are what is checked, not the table: DELETE FROM schema_migrations,
// or a swap for an empty table of the right shape, leaves the name in place
// and the history gone.
func rejectEmptyLedger(ctx context.Context, db *sql.DB) error {
	// The OWNER TABLE alone decides that a history must exist, and the marker
	// plays no part. storage_metadata is created by the bootstrap migration
	// and that migration's ledger row commits in the same transaction, so
	// "the table is here, the history is not" cannot come from an interrupted
	// bootstrap — it can only come from damage.
	//
	// Requiring the marker as well was a hole: clearing application_id on a
	// versioned database and dropping its ledger made it look pre-versioned,
	// and every version was then recorded afresh over the existing schema,
	// hiding the loss of the checksum history this mechanism exists to keep.
	owned, err := tableExists(ctx, db, ownerTable)
	if err != nil {
		return err
	}
	if !owned {
		return nil // a fresh file, or a pre-versioned one being adopted
	}

	recorded, err := tableExists(ctx, db, ledgerTable)
	if err != nil {
		return err
	}
	if !recorded {
		return fmt.Errorf("%w: %s is missing from a database that records an owner",
			ErrSchemaIncompatible, ledgerTable)
	}

	var versions int
	if err := ScanOne(ctx, db, &versions, `SELECT COUNT(*) FROM `+ledgerTable); err != nil {
		return fmt.Errorf("storage: count recorded versions: %w", err)
	}
	if versions == 0 {
		return fmt.Errorf("%w: %s records no version in a database that records an owner",
			ErrSchemaIncompatible, ledgerTable)
	}
	return nil
}

// tableExists reports whether a table of that name is present.
func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var present int
	err := ScanOne(ctx, db, &present,
		`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?`, table)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("storage: look up table %q: %w", table, err)
	}
	return true, nil
}

// corruptionMessages are what SQLite says when the FILE is the problem:
// SQLITE_CORRUPT and SQLITE_NOTADB. Both drivers surface them as text, and
// neither exposes a shared typed error to match instead.
var corruptionMessages = []string{
	"database disk image is malformed",
	"file is not a database",
	"file is encrypted or is not a database",
	"database corrupt",
	"malformed database schema",
}

// classifyCheckFailure decides what a check that did not RUN means.
//
// ErrCorrupt is reserved for the file actually being broken — either the check
// reported violations, or the driver refused it as not-a-database. Everything
// else keeps its own class: a cancelled caller is the everyday case, and
// errorClass tests ErrCorrupt before cancellation, so classing it as
// corruption told the operator their healthy database was damaged. Blanket
// removal was the opposite mistake: a genuinely malformed file was reported as
// a plain SQL error.
func classifyCheckFailure(check string, err error) error {
	if isCorruption(err) {
		return fmt.Errorf("%w: %s could not read the file: %w", ErrCorrupt, check, err)
	}
	return fmt.Errorf("storage: %s failed to run: %w", check, err)
}

// isCorruption reports whether the driver is saying the file itself is broken.
func isCorruption(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, corruption := range corruptionMessages {
		if strings.Contains(message, corruption) {
			return true
		}
	}
	return false
}

// verifyRecordedHistory re-reads the ledger's CONTENT at the end of an open
// and returns the version the file is actually at.
//
// Not just its shape: another process running a newer catalog can record a
// version between this run's last migration and here, and the schema check
// only proves the ledger's DDL — so this binary used to hand back a ready
// Database sitting on a version it does not know, instead of ErrSchemaTooNew.
// The version it returns comes from the same snapshot it verified, so what is
// logged is what was checked.
func verifyRecordedHistory(ctx context.Context, db *sql.DB, catalog []Migration) (Version, error) {
	recorded, err := readLedger(ctx, db)
	if err != nil {
		return LegacyVersion, err
	}
	if err := verifyLedger(recorded, catalog); err != nil {
		return LegacyVersion, err
	}
	return highestRecorded(recorded), nil
}

// highestRecorded returns the highest version in a ledger snapshot, or
// LegacyVersion for an empty one.
func highestRecorded(recorded map[Version]ledgerRow) Version {
	highest := LegacyVersion
	for version := range recorded {
		if version > highest {
			highest = version
		}
	}
	return highest
}

// currentVersion reads the highest recorded schema version, LegacyVersion
// when the file has no ledger yet.
func currentVersion(ctx context.Context, db *sql.DB) (Version, error) {
	var present int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return LegacyVersion, nil
	}
	if err != nil {
		return LegacyVersion, fmt.Errorf("storage: look up migration ledger: %w", err)
	}

	var version sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		return LegacyVersion, fmt.Errorf("storage: read schema version: %w", err)
	}
	if !version.Valid {
		return LegacyVersion, nil
	}
	return Version(version.Int64), nil
}

// recordOwner writes the owner identity INSIDE the transaction of the
// bootstrap version.
//
// Writing it afterwards, in a transaction of its own, left a window that no
// version number can close. The obvious reading — "a file at version 1 holds
// no data, so adopting it loses nothing" — is false for the case that matters:
// a pre-versioned chatlog file already holds its whole message history, and
// version 1 only adds the table that records who owns it. A process killed
// between the two commits left that history unowned, and the next identity to
// open the file could write itself in as the owner. Committed together, the
// history and its owner cannot come apart.
func recordOwner(ctx context.Context, conn *sql.Conn, owner domain.PeerIdentity, now func() time.Time) error {
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO storage_metadata (id, owner_identity, bootstrap_version, created_at)
		VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING`,
		owner.String(), storageBootstrapFormat, now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("storage: record owner identity: %w", err)
	}
	return nil
}

// verifyOwnerTableShape asserts that storage_metadata is exactly what the
// bootstrap migration declares, before anything reads a row out of it.
//
// Reading first and classifying the failure afterwards can only ever cover the
// driver messages someone thought of: a table recreated with nullable columns
// fails on the value conversion, not with "no such column", and a database
// whose shape is wrong then looked like a plain SQL fault. The catalog already
// says what the table must be, so that is what it is compared against.
func verifyOwnerTableShape(ctx context.Context, db *sql.DB, catalog []Migration) error {
	reference, err := BuildReferenceSchema(ctx, catalog, bootstrapMigrationVersion)
	if err != nil {
		return err
	}
	key := objectKey{Type: "table", Name: ownerTable}
	want, declared := reference.snapshot.objects[key]
	if !declared {
		return fmt.Errorf("%w: the catalog does not declare %s", ErrCatalogInvalid, ownerTable)
	}

	live, err := readSchemaSnapshot(ctx, db)
	if err != nil {
		return err
	}
	got, exists := live.objects[key]
	if !exists {
		return fmt.Errorf("%w: %s does not exist", ErrSchemaIncompatible, ownerTable)
	}
	return sameObject(got, want)
}

// verifyOwnership reads the recorded owner and refuses anything else. It never
// writes: the row is created by recordOwner, with the version that creates its
// table, so a file missing it at this point is damaged rather than new.
func verifyOwnership(ctx context.Context, db *sql.DB, owner domain.PeerIdentity) error {
	var recorded ownerRow
	err := ScanOne(ctx, db,
		[]any{&recorded.Identity, &recorded.BootstrapFormat, &recorded.CreatedAt},
		`SELECT owner_identity, bootstrap_version, created_at FROM storage_metadata WHERE id = 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: the database has no owner row", ErrOwnerMismatch)
	}
	if err != nil {
		return fmt.Errorf("storage: read owner identity: %w", err)
	}

	stored, err := domain.ParsePeerIdentity(recorded.Identity)
	if err != nil {
		return fmt.Errorf("%w: recorded owner %q is not a valid identity: %w", ErrOwnerMismatch, recorded.Identity, err)
	}
	if stored != owner {
		return fmt.Errorf("%w: database belongs to %s, this node is %s", ErrOwnerMismatch, stored, owner)
	}
	// bootstrap_version says which storage_metadata contract wrote this file.
	// Reading the row without looking at it made the field decorative: a value
	// this binary never writes would have been carried along as if the layout
	// were the one it expects.
	switch {
	case recorded.BootstrapFormat == storageBootstrapFormat:
		return nil
	case recorded.BootstrapFormat > storageBootstrapFormat:
		return fmt.Errorf("%w: bootstrap format is %d, this binary knows %d",
			ErrSchemaTooNew, recorded.BootstrapFormat, storageBootstrapFormat)
	default:
		return fmt.Errorf("%w: bootstrap format is %d, this binary writes %d",
			ErrSchemaIncompatible, recorded.BootstrapFormat, storageBootstrapFormat)
	}
}

// confirmMarkers re-reads the runner-owned markers without writing them.
//
// It runs after the catalog, where the migration transaction has already been
// committed and a repair is no longer part of any atomic step: an
// application_id silently restamped from 0, or a storage_metadata row silently
// re-inserted, would leave a file that opens cleanly and whose ownership no
// longer means what it recorded.
func confirmMarkers(ctx context.Context, db *sql.DB, owner domain.PeerIdentity) error {
	var stamped int64
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&stamped); err != nil {
		return fmt.Errorf("storage: re-read application_id: %w", err)
	}
	if stamped != applicationID {
		return fmt.Errorf("%w: application_id changed to %d while opening, want %d",
			ErrForeignApplication, stamped, applicationID)
	}

	return verifyOwnership(ctx, db, owner)
}

// logAttempted emits one structured line per migration this process ran,
// failures included. Nothing derived from user rows is logged — only version,
// name, timing, outcome and, for a failure, the error class.
//
// It must be called on the error path too: a step that fails is exactly the
// one an operator needs in the log, and the steps that succeeded before it
// tell them how far the upgrade got.
func logAttempted(attempted []AttemptedMigration, err error) {
	for _, migration := range attempted {
		event := log.Info()
		if migration.Result == MigrationFailed {
			event = log.Error().Str("error_class", errorClass(err))
		}
		event.
			Str("version", migration.Version.String()).
			Str("name", migration.Name).
			Dur("duration", migration.Duration).
			Str("result", string(migration.Result)).
			Msg("storage migration step finished")
	}
}

// errorClass names the failure without leaking the statement or any row data
// the driver may have put in the message.
func errorClass(err error) string {
	classes := []struct {
		sentinel error
		name     string
	}{
		{ErrSchemaIncompatible, "schema-incompatible"},
		{ErrMigrationDrift, "migration-drift"},
		{ErrSchemaTooNew, "schema-too-new"},
		{ErrCatalogInvalid, "catalog-invalid"},
		{ErrOwnerMismatch, "owner-mismatch"},
		{ErrForeignApplication, "foreign-application"},
		{ErrCorrupt, "corrupt"},
		{context.Canceled, "cancelled"},
		{context.DeadlineExceeded, "deadline-exceeded"},
	}
	for _, class := range classes {
		if errors.Is(err, class.sentinel) {
			return class.name
		}
	}
	if err == nil {
		return "none"
	}
	return "sql"
}
