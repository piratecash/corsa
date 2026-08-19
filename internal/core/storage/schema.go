package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Structural verification of an existing schema.
//
// Every chatlog migration is written with CREATE ... IF NOT EXISTS so that a
// pre-versioned database is adopted rather than rebuilt. That makes the DDL a
// no-op there, and an unverified no-op would let the runner stamp a version
// onto objects whose real shape nobody checked.
//
// The expectation is obtained by RUNNING the catalog into a scratch in-memory
// database and reading the schema it produces. Earlier versions derived it
// from the migration text instead — first property by property, then by
// parsing out the CREATE statements — and both were wrong in the same way:
// they only ever understood what they had been taught to look for. A parser
// that reads CREATE cannot see what an ALTER, a DROP or a trigger does, and
// every gap in it was a way for a real schema and its stated shape to differ.
// Executing the catalog has no such gap: whatever the SQL does, the reference
// database ends up in the state the migration actually produces, and SQLite —
// not this package — is what interprets the DDL.

// schemaObject is one entry of sqlite_schema.
type schemaObject struct {
	Type  string // table, index, trigger, view
	Name  string
	Table string // the table the object belongs to
	DDL   string
}

// objectKey identifies one schema object. The name alone is not enough:
// SQLite lets a table and a trigger carry the same name, and keying by name
// silently dropped one of them — a declared trigger could evict the table it
// shares a name with, and the incompatible legacy table then went unchecked.
type objectKey struct {
	Type string
	Name string
}

// schemaSnapshot is every object of one database.
type schemaSnapshot struct {
	objects map[objectKey]schemaObject
}

func (s schemaSnapshot) keys() []objectKey {
	keys := make([]objectKey, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Type != keys[j].Type {
			return keys[i].Type < keys[j].Type
		}
		return keys[i].Name < keys[j].Name
	})
	return keys
}

// tables returns the set of table names in the snapshot.
func (s schemaSnapshot) tables() map[string]struct{} {
	tables := map[string]struct{}{}
	for _, object := range s.objects {
		if object.Type == "table" {
			tables[object.Name] = struct{}{}
		}
	}
	return tables
}

// ReferenceSchema is the schema a catalog produces, read back from a scratch
// database the catalog was executed into.
type ReferenceSchema struct {
	snapshot schemaSnapshot
}

// BuildReferenceSchema runs every migration up to and including limit against a
// private in-memory database and captures the resulting schema.
//
// Executing rather than parsing is the whole point: ALTER, DROP and CREATE
// TRIGGER need no special handling, because the reference simply ends up in
// whatever state the statements produce. It also means a migration whose SQL
// does not execute — or two migrations that collide over one object — is
// caught here, which is why ValidateCatalog does this before the real database
// file is opened.
func BuildReferenceSchema(ctx context.Context, catalog []Migration, limit Version) (ReferenceSchema, error) {
	reference, err := buildReferenceSchema(ctx, catalog, limit)
	return reference, keepContextCause(ctx, err)
}

func buildReferenceSchema(ctx context.Context, catalog []Migration, limit Version) (ReferenceSchema, error) {
	scratch, err := openScratchDatabase()
	if err != nil {
		return ReferenceSchema{}, err
	}
	defer func() { _ = scratch.Close() }()

	conn, err := scratch.Conn(ctx)
	if err != nil {
		return ReferenceSchema{}, fmt.Errorf("storage: pin reference connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	for _, migration := range catalog {
		if migration.Version > limit {
			break
		}
		if err := execAsRunner(ctx, conn, migration.SQL); err != nil {
			return ReferenceSchema{}, fmt.Errorf("%w: version %s does not execute: %w",
				ErrCatalogInvalid, migration.Version, err)
		}
	}

	reference, err := captureReference(ctx, conn)
	if err != nil {
		return ReferenceSchema{}, err
	}
	// Only the catalog is held to this: the ledger's own DDL is built through
	// BuildReferenceSchemaFromSQL and of course declares the ledger.
	if err := rejectLedgerObjects(reference.snapshot); err != nil {
		return ReferenceSchema{}, err
	}
	return reference, nil
}

// execAsRunner executes one migration the way applyMigration does: on a pinned
// connection, inside BEGIN IMMEDIATE.
//
// The transaction boundary is part of what the statements mean. A deferred
// foreign key is only checked at COMMIT, so a migration that inserts a child
// before its parent is correct inside a transaction and fails in autocommit —
// validating it outside one would reject a migration that works. The reverse
// holds too: statements whose effect differs between the two would be measured
// here under semantics the runner never uses.
func execAsRunner(ctx context.Context, conn *sql.Conn, statements string) error {
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, statements); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	return nil
}

// captureReference reads back the schema the executed statements produced.
func captureReference(ctx context.Context, conn *sql.Conn) (ReferenceSchema, error) {
	reader := readOnly{conn: conn}
	if err := rejectTempObjects(ctx, reader); err != nil {
		return ReferenceSchema{}, err
	}
	snapshot, err := readSchemaSnapshot(ctx, reader)
	if err != nil {
		return ReferenceSchema{}, err
	}
	return ReferenceSchema{snapshot: snapshot}, nil
}

// keepContextCause reports the caller's cancellation as itself.
//
// The reference is built by EXECUTING the catalog, and every step of that can
// fail because the caller went away: pinning the connection, running a
// migration, reading the schema back. Each of those failures used to be
// wrapped in ErrCatalogInvalid, which made a correct catalog matchable as
// invalid and logged a healthy process shutting down as a programming error.
// One place decides it, so no path can be missed.
func keepContextCause(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("storage: build reference schema: %w", ctxErr)
	}
	return err
}

// openScratchDatabase opens the private in-memory database the reference is
// built in, with the SAME connection semantics as production.
//
// The DSN matters: a bare :memory: database has foreign_keys OFF, so a catalog
// whose statements violate a foreign key would validate here and only fail
// after the real file had already been changed.
func openScratchDatabase() (*sql.DB, error) {
	scratch, err := sql.Open(sqliteDriverName, ":memory:"+sqliteDSNOptions)
	if err != nil {
		return nil, fmt.Errorf("storage: open reference database: %w", err)
	}
	// One connection: an in-memory database is per-connection, so a pooled
	// second one would see an empty schema.
	scratch.SetMaxOpenConns(1)
	return scratch, nil
}

// rejectTempObjects refuses a catalog that creates temporary objects.
//
// A TEMP object lives on the connection, not in the file: it would never
// appear in the reference, it would stay on the pooled connection the
// migration ran on — able to affect writes for the rest of the process — and
// it would vanish on restart. Nothing durable can be expressed that way.
func rejectTempObjects(ctx context.Context, tx SchemaReader) error {
	var name string
	err := ScanOne(ctx, tx, &name, `SELECT name FROM sqlite_temp_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: read temporary schema: %w", err)
	}
	return fmt.Errorf("%w: a migration creates the temporary object %q, which cannot be part of a durable schema",
		ErrCatalogInvalid, name)
}

// BuildReferenceSchemaFromSQL is BuildReferenceSchema for a single statement
// block that is not part of the catalog — the migration ledger's own DDL.
func BuildReferenceSchemaFromSQL(ctx context.Context, ddl string) (ReferenceSchema, error) {
	reference, err := buildReferenceSchemaFromSQL(ctx, ddl)
	return reference, keepContextCause(ctx, err)
}

func buildReferenceSchemaFromSQL(ctx context.Context, ddl string) (ReferenceSchema, error) {
	scratch, err := openScratchDatabase()
	if err != nil {
		return ReferenceSchema{}, err
	}
	defer func() { _ = scratch.Close() }()

	conn, err := scratch.Conn(ctx)
	if err != nil {
		return ReferenceSchema{}, fmt.Errorf("storage: pin reference connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := execAsRunner(ctx, conn, ddl); err != nil {
		return ReferenceSchema{}, fmt.Errorf("%w: the reference DDL does not execute: %w", ErrCatalogInvalid, err)
	}
	return captureReference(ctx, conn)
}

// Objects reports how many schema objects the reference contains. A migration
// that adds none has no post-condition of its own.
func (r ReferenceSchema) Objects() int { return len(r.snapshot.objects) }

// rejectLedgerObjects refuses a catalog that reaches into the migration ledger.
//
// The ledger belongs to the runner: it writes one row per version and reads it
// back in the same transaction. A migration that put a trigger on it — or
// redefined it — would pass this open, because the runner checked the ledger
// before applying anything, and then make the database impossible to open
// again.
func rejectLedgerObjects(snapshot schemaSnapshot) error {
	for _, key := range snapshot.keys() {
		object := snapshot.objects[key]
		if object.Name == ledgerTable || object.Table == ledgerTable {
			return fmt.Errorf("%w: a migration declares %s %q on the migration ledger, which belongs to the runner",
				ErrCatalogInvalid, object.Type, object.Name)
		}
	}
	return nil
}

// readSchemaSnapshot reads every non-internal object of a database.
func readSchemaSnapshot(ctx context.Context, tx SchemaReader) (schemaSnapshot, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return schemaSnapshot{}, fmt.Errorf("storage: read schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	snapshot := schemaSnapshot{objects: map[objectKey]schemaObject{}}
	for rows.Next() {
		var object schemaObject
		if err := rows.Scan(&object.Type, &object.Name, &object.Table, &object.DDL); err != nil {
			return schemaSnapshot{}, fmt.Errorf("storage: scan schema: %w", err)
		}
		snapshot.objects[objectKey{Type: object.Type, Name: object.Name}] = object
	}
	if err := rows.Err(); err != nil {
		return schemaSnapshot{}, fmt.Errorf("storage: iterate schema: %w", err)
	}
	return snapshot, nil
}

// VerifySchema asserts the live schema matches the reference: every object the
// catalog produces exists with the same definition, and the tables it owns
// carry nothing else that changes their meaning.
//
// allowed is the reference for the WHOLE catalog and governs what may exist
// beyond required. They differ mid-upgrade: a pre-versioned database can
// already contain an index a later version declares, and calling that
// unexpected would stop the upgrade before reaching the version that declares
// it.
//
// Extra NON-unique indexes are tolerated even then: they are a performance
// decision that changes no row a repository may write, so an operator's
// hand-added index must not stop the node from starting.
func VerifySchema(ctx context.Context, tx SchemaReader, required, allowed ReferenceSchema) error {
	live, err := readSchemaSnapshot(ctx, tx)
	if err != nil {
		return err
	}

	for _, key := range required.snapshot.keys() {
		want := required.snapshot.objects[key]
		got, exists := live.objects[key]
		if !exists {
			return fmt.Errorf("%w: %s %q does not exist", ErrSchemaIncompatible, want.Type, want.Name)
		}
		if err := sameObject(got, want); err != nil {
			return err
		}
	}

	if err := rejectUndeclaredObjects(ctx, tx, live, required, allowed); err != nil {
		return err
	}
	return rejectIncomingForeignKeys(ctx, tx, live, required, allowed)
}

// rejectIncomingForeignKeys refuses a foreign key pointing AT a managed table
// from one the catalog knows nothing about.
//
// foreign_key_check passes for such a table as long as its rows resolve, so
// the file opens cleanly — and then the first delete of a referenced message
// fails, because a table nobody declared is holding it.
//
// A table the catalog declares at a LATER version is not such a table: a
// pre-versioned file can already carry it, and its foreign key back into the
// current version is exactly what the later migration will declare. It is
// skipped only when it matches that declaration object-for-object.
func rejectIncomingForeignKeys(ctx context.Context, tx SchemaReader, live schemaSnapshot, required, allowed ReferenceSchema) error {
	managed := asciiFoldedSet(required.snapshot.tables())

	for _, key := range live.keys() {
		object := live.objects[key]
		if object.Type != "table" {
			continue
		}
		if _, ours := managed[asciiFold(object.Name)]; ours {
			continue
		}
		if declaredLater(object, allowed) {
			continue
		}

		parents, err := foreignKeyParents(ctx, tx, object.Name)
		if err != nil {
			return err
		}
		for _, parent := range parents {
			// SQLite matches identifiers case-insensitively, so
			// REFERENCES MeSsAgEs binds to messages just as well.
			if _, ours := managed[asciiFold(parent)]; ours {
				return fmt.Errorf("%w: table %q references the managed table %q",
					ErrSchemaIncompatible, object.Name, parent)
			}
		}
	}
	return nil
}

// declaredLater reports whether the live object is exactly what a later
// version of the catalog declares.
func declaredLater(object schemaObject, allowed ReferenceSchema) bool {
	want, declared := allowed.snapshot.objects[objectKey{Type: object.Type, Name: object.Name}]
	return declared && sameObject(object, want) == nil
}

// asciiFoldedSet folds a set of identifiers for case-insensitive lookup.
func asciiFoldedSet(names map[string]struct{}) map[string]struct{} {
	folded := make(map[string]struct{}, len(names))
	for name := range names {
		folded[asciiFold(name)] = struct{}{}
	}
	return folded
}

// asciiFold lower-cases ASCII letters and leaves everything else untouched,
// which is how SQLite compares identifiers by default.
//
// strings.ToLower folds Unicode too, and that is not the same relation: SQLite
// keeps "Å" and "å" as two different tables, while Unicode folding makes them
// one — so an undeclared table could be mistaken for the managed table whose
// name it merely resembles, and its foreign key into that table would go
// unnoticed.
func asciiFold(identifier string) string {
	folded := []byte(identifier)
	for i, char := range folded {
		if char >= 'A' && char <= 'Z' {
			folded[i] = char + ('a' - 'A')
		}
	}
	return string(folded)
}

// foreignKeyParents returns the tables the given table has foreign keys to.
func foreignKeyParents(ctx context.Context, tx SchemaReader, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%s)", quoteIdentifier(table)))
	if err != nil {
		return nil, fmt.Errorf("storage: read foreign keys of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("storage: read foreign keys of %q: %w", table, err)
	}

	var parents []string
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("storage: scan foreign keys of %q: %w", table, err)
		}
		for i, column := range columns {
			if column != "table" {
				continue
			}
			if parent, ok := values[i].(string); ok {
				parents = append(parents, parent)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: iterate foreign keys of %q: %w", table, err)
	}
	return parents, nil
}

// rejectUndeclaredObjects fails on anything the catalog never declares that
// could change what the managed tables accept.
func rejectUndeclaredObjects(ctx context.Context, tx SchemaReader, live schemaSnapshot, required, allowed ReferenceSchema) error {
	managed := required.snapshot.tables()

	for _, key := range live.keys() {
		object := live.objects[key]
		if _, ours := managed[object.Table]; !ours {
			continue // a table this catalog knows nothing about
		}
		if _, checked := required.snapshot.objects[key]; checked {
			continue // already compared against what this version requires
		}
		if declared, known := allowed.snapshot.objects[key]; known {
			// Matching by NAME alone let a pre-versioned database carry a
			// same-named but different future object — a trigger that fires
			// before the version that declares it and changes data no later
			// check can restore.
			if err := sameObject(object, declared); err != nil {
				return err
			}
			continue
		}

		switch object.Type {
		case "trigger":
			// A trigger runs between the statement a repository issues and
			// the row it expects afterwards: an AFTER INSERT can delete the
			// message just stored and hand the caller a successful INSERT
			// with nothing saved.
			return fmt.Errorf("%w: table %q has an undeclared trigger (%s)",
				ErrSchemaIncompatible, object.Table, object.Name)
		case "index":
			unique, err := isUniqueIndex(ctx, tx, object.Table, object.Name)
			if err != nil {
				return err
			}
			if unique {
				// A unique index nobody declared changes which rows the table
				// accepts, and no idempotent DDL would ever repair that.
				return fmt.Errorf("%w: table %q has an undeclared UNIQUE index %q",
					ErrSchemaIncompatible, object.Table, object.Name)
			}
		}
	}
	return nil
}

// sameObject compares one live object against the one the catalog produces.
func sameObject(got, want schemaObject) error {
	if got.Type != want.Type || got.Table != want.Table {
		return fmt.Errorf("%w: %q is a %s on %q, want a %s on %q",
			ErrSchemaIncompatible, want.Name, got.Type, got.Table, want.Type, want.Table)
	}
	if normalizeDDL(got.DDL) != normalizeDDL(want.DDL) {
		return fmt.Errorf("%w: %s %q is defined as %q, want %q",
			ErrSchemaIncompatible, want.Type, want.Name, collapseSQL(got.DDL), collapseSQL(want.DDL))
	}
	return nil
}

// isUniqueIndex reports whether the named index on the table is unique.
// PRAGMA index_list is used rather than the stored text because it also covers
// the indexes SQLite generates for constraints, which have no text at all.
func isUniqueIndex(ctx context.Context, tx SchemaReader, table, index string) (bool, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quoteIdentifier(table)))
	if err != nil {
		return false, fmt.Errorf("storage: read indexes of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			seq     int
			name    string
			unique  int
			origin  string
			partial int
		)
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return false, fmt.Errorf("storage: scan indexes of %q: %w", table, err)
		}
		if name == index {
			return unique != 0, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("storage: iterate indexes of %q: %w", table, err)
	}
	return false, nil
}

// normalizeDDL makes two CREATE statements comparable regardless of layout and
// of whether the author wrote IF NOT EXISTS.
//
// Only whitespace, comments and that one clause are normalised. Case is NOT
// folded and string literals are preserved byte for byte: `('dm','global')`
// and `('DM','GLOBAL')` are different constraints, and a comparison that
// treated them as equal would accept a database that rejects every ordinary
// write.
func normalizeDDL(ddl string) string {
	normalized := collapseSQL(stripSQLComments(ddl))
	normalized = strings.TrimSuffix(strings.TrimSpace(normalized), ";")
	return strings.TrimSpace(removeIfNotExists(normalized))
}

// removeIfNotExists drops the optional clause, whatever case it was written
// in, without touching anything inside a literal.
func removeIfNotExists(sqlText string) string {
	const clause = "IF NOT EXISTS "
	for i := 0; i+len(clause) <= len(sqlText); {
		if isQuoteOpen(sqlText[i]) {
			i = quoteEnd(sqlText, i)
			continue
		}
		if strings.EqualFold(sqlText[i:i+len(clause)], clause) {
			return sqlText[:i] + sqlText[i+len(clause):]
		}
		i++
	}
	return sqlText
}

// sqlPunctuation are the characters whose surrounding whitespace carries no
// meaning. Formatting differences around them are exactly what a stored
// definition and a source file disagree on: `CHECK(id=1)` and `CHECK( id = 1 )`
// are the same constraint.
const sqlPunctuation = "(),;=<>!+-*/%|&~"

// collapseSQL reduces every run of whitespace to a single space OUTSIDE string
// literals and quoted identifiers, and drops spaces next to punctuation, so
// that layout stops mattering entirely. Inside a literal every byte is kept.
func collapseSQL(sqlText string) string {
	var out []byte
	pendingSpace := false

	write := func(text string, first byte) {
		if pendingSpace {
			pendingSpace = false
			afterPunctuation := len(out) > 0 && strings.IndexByte(sqlPunctuation, out[len(out)-1]) >= 0
			if len(out) > 0 && !afterPunctuation && strings.IndexByte(sqlPunctuation, first) < 0 {
				out = append(out, ' ')
			}
		}
		out = append(out, text...)
	}

	for i := 0; i < len(sqlText); {
		if isQuoteOpen(sqlText[i]) {
			end := quoteEnd(sqlText, i)
			write(sqlText[i:end], sqlText[i])
			i = end
			continue
		}
		c := sqlText[i]
		i++
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' {
			pendingSpace = true
			continue
		}
		write(string(c), c)
	}
	return string(out)
}

// quoteIdentifier makes a name safe to interpolate into a PRAGMA, which cannot
// take bound parameters.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
