package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Version is the schema version of the shared state database. Versions start
// at 1 and increase by one without gaps.
type Version int

// LegacyVersion is the version of a database that carries no migration
// ledger yet — the shape every pre-versioned chatlog file has on its first
// open by a versioned binary.
const LegacyVersion Version = 0

func (v Version) String() string { return strconv.Itoa(int(v)) }

// SchemaReader is the READ-ONLY handle a post-condition gets.
//
// It deliberately cannot execute: a verifier runs on the same pinned
// connection as the migration, inside its BEGIN IMMEDIATE, so anything it
// wrote would land in that transaction — and a COMMIT or a DELETE against the
// ledger from there could finish the step with a hole in the recorded history,
// or leave changes behind after a step that formally failed. A post-condition
// asks questions; it does not change anything.
type SchemaReader interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ScanOne reads the single row a query returns into dest, which is one pointer
// or a []any of them. It replaces database/sql's QueryRow for this interface:
// QueryRow reports a refusal only when the caller gets around to Scan, and the
// handle a condition runs on refuses statements outright.
func ScanOne(ctx context.Context, tx SchemaReader, dest any, query string, args ...any) error {
	targets, ok := dest.([]any)
	if !ok {
		targets = []any{dest}
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := rows.Scan(targets...); err != nil {
		return err
	}
	return rows.Err()
}

// Migration is a single forward-only schema step. There is no Down: rollback
// of a binary is served by expand/contract releases, not by reversing DDL on
// a file that holds live message history.
type Migration struct {
	// Version is the position of this step in the single global order.
	Version Version

	// Name is the stable human identifier recorded in the ledger. It never
	// changes once published — a rename is indistinguishable from a reused
	// version number and is rejected as drift.
	Name string

	// SQL is the embedded statement text. It must not contain BEGIN or
	// COMMIT: the runner owns the transaction.
	SQL string

	// Invariant is an OPTIONAL extra post-condition of this step, checked
	// inside its transaction on a connection sealed against writes.
	//
	// The declared-object check is derived from SQL itself and always runs,
	// so a migration that creates tables or indexes needs nothing here. One
	// that does neither — an ALTER, a backfill UPDATE — declares no condition
	// of its own, and for those Invariant is required: otherwise the version
	// would be recorded on the strength of the statement not returning an
	// error.
	//
	// It runs INSIDE the migration's transaction, after the ledger row, so
	// that its failure rolls the whole step back and version N+1 never runs
	// after a failure at N. Moving it outside would make a failure arrive too
	// late to undo anything.
	//
	// See runInvariant for what that costs and how it is contained: a
	// callback holding the migration's own connection can write, and can end
	// the transaction outright.
	Invariant func(ctx context.Context, tx SchemaReader) error
}

// Checksum is the SHA-256 of the embedded SQL. It is recorded in the ledger
// so that editing an already published migration is detected instead of
// silently changing what a version means.
func (m Migration) Checksum() string {
	sum := sha256.Sum256([]byte(canonicalNewlines(m.SQL)))
	return hex.EncodeToString(sum[:])
}

// canonicalNewlines folds CRLF to LF so the checksum describes the migration
// rather than the checkout it came from.
//
// The SQL is embedded verbatim from a file, and a Windows checkout with
// core.autocrlf rewrites that file's line endings — different bytes, different
// checksum, and a database migrated by such a build is then refused by an
// official one as drift, in both directions. The tests never saw it because
// they write and re-read a ledger from the same checkout.
//
// .gitattributes pins the files to LF as well. That keeps the SQL that RUNS
// byte-for-byte the reviewed SQL; this keeps an already-recorded checksum
// valid whatever the file went through. Folding changes nothing for an LF
// checkout, so no existing ledger is invalidated.
func canonicalNewlines(sqlText string) string {
	return strings.ReplaceAll(sqlText, "\r\n", "\n")
}

// LatestVersion returns the highest version in a validated catalog, or
// LegacyVersion for an empty one.
func LatestVersion(catalog []Migration) Version {
	if len(catalog) == 0 {
		return LegacyVersion
	}
	return catalog[len(catalog)-1].Version
}

// ValidateCatalog rejects a malformed catalog before the database file is
// opened, so a programming error can never half-migrate a user's history.
// The catalog must be a dense ascending run 1..N with unique names, non-empty
// SQL that owns no transaction of its own, and a verifier per step.
func ValidateCatalog(ctx context.Context, catalog []Migration) error {
	if len(catalog) == 0 {
		return fmt.Errorf("%w: catalog is empty", ErrCatalogInvalid)
	}

	names := make(map[string]Version, len(catalog))
	for i, migration := range catalog {
		want := Version(i + 1)
		if migration.Version != want {
			return fmt.Errorf("%w: entry %d has version %s, want %s (versions must be positive, unique, ascending and gapless)",
				ErrCatalogInvalid, i, migration.Version, want)
		}
		if strings.TrimSpace(migration.Name) == "" {
			return fmt.Errorf("%w: version %s has an empty name", ErrCatalogInvalid, migration.Version)
		}
		if previous, exists := names[migration.Name]; exists {
			return fmt.Errorf("%w: name %q is used by versions %s and %s",
				ErrCatalogInvalid, migration.Name, previous, migration.Version)
		}
		names[migration.Name] = migration.Version

		if strings.TrimSpace(migration.SQL) == "" {
			return fmt.Errorf("%w: version %s has empty SQL", ErrCatalogInvalid, migration.Version)
		}
		// SQLite treats U+FEFF as whitespace and Go does not, so a byte-order
		// mark between a semicolon and a keyword hides that keyword from every
		// scan here while SQLite executes it. Nothing legitimate puts one in
		// an embedded migration.
		if strings.ContainsRune(migration.SQL, '\uFEFF') {
			return fmt.Errorf("%w: version %s contains a byte-order mark", ErrCatalogInvalid, migration.Version)
		}
		if err := rejectForbiddenStatements(migration); err != nil {
			return err
		}

		// A migration that touches DATA proves nothing about it by executing
		// without error, so those bring their own post-condition. The schema
		// half is covered by the reference below whatever it does.
		if touchesData(migration.SQL) && migration.Invariant == nil {
			return fmt.Errorf("%w: version %s changes data and supplies no Invariant — the result would be recorded with nothing checked",
				ErrCatalogInvalid, migration.Version)
		}
	}

	// The reference is built by EXECUTING the catalog, which is what proves
	// the whole of it: unexecutable SQL, two versions colliding over one
	// object, a migration reaching into the ledger. It happens here, before
	// the real database file is opened, so none of those can be discovered
	// after application_id, WAL and the ledger have already changed it.
	reference, err := BuildReferenceSchema(ctx, catalog, LatestVersion(catalog))
	if err != nil {
		return err
	}

	// A migration that adds no schema object and no Invariant would be
	// recorded on the strength of its statements merely not failing.
	for i, migration := range catalog {
		if migration.Invariant != nil {
			continue
		}
		before, err := BuildReferenceSchema(ctx, catalog, migration.Version-1)
		if err != nil {
			return err
		}
		after, err := BuildReferenceSchema(ctx, catalog[:i+1], migration.Version)
		if err != nil {
			return err
		}
		if after.Objects() == before.Objects() && !changesSchema(before, after) {
			return fmt.Errorf("%w: version %s adds no schema object and supplies no Invariant — it would be recorded with nothing checked",
				ErrCatalogInvalid, migration.Version)
		}
	}
	_ = reference
	return nil
}

// changesSchema reports whether two references differ in any object.
func changesSchema(before, after ReferenceSchema) bool {
	if len(before.snapshot.objects) != len(after.snapshot.objects) {
		return true
	}
	for name, object := range after.snapshot.objects {
		previous, existed := before.snapshot.objects[name]
		if !existed || normalizeDDL(previous.DDL) != normalizeDDL(object.DDL) {
			return true
		}
	}
	return false
}

// touchesData reports whether any statement changes rows rather than schema.
//
// A trigger body is BEGIN ... END with semicolons inside, so it is skipped as
// one unit — its UPDATE belongs to the trigger definition, not to this
// migration's effect. Only CREATE TABLE ... AS counts among the CREATEs:
// CREATE VIEW ... AS SELECT defines a view and copies nothing.
func touchesData(migrationSQL string) bool {
	insideTriggerBody := false

	for _, statement := range splitSQLStatements(stripSQLComments(migrationSQL)) {
		fields := strings.Fields(strings.ToLower(statement))
		if len(fields) == 0 {
			continue
		}
		if insideTriggerBody {
			if fields[0] == "end" {
				insideTriggerBody = false
			}
			continue
		}
		if isCreateTrigger(fields) {
			insideTriggerBody = true
			continue
		}

		switch fields[0] {
		case "alter", "drop":
			continue
		case "create":
			if isCreateTableAsSelect(statement) {
				return true
			}
			continue
		default:
			return true
		}
	}
	return false
}

// isCreateTableAsSelect reports whether the statement is CREATE TABLE ... AS,
// which copies rows the reference schema says nothing about.
//
// Depth matters: a column defined GENERATED ALWAYS AS (...) carries the same
// keyword inside the column list, and treating that as a copy demanded a
// post-condition from a migration that only declares a table.
func isCreateTableAsSelect(statement string) bool {
	tokens := sqlTokens(statement)
	if len(tokens) == 0 || !tokens[0].isWord("create") {
		return false
	}

	table := false
	for _, token := range tokens[1:] {
		switch {
		case token.Quoted:
			table = true // a quoted name, never a keyword
		case !table && token.isWord("table"):
			table = true
		case !table && (token.isWord("temp") || token.isWord("temporary")):
			continue
		case !table:
			return false // some other CREATE
		case token.isWord("as") && token.Depth == 0:
			return true
		}
	}
	return false
}

// isCreateTrigger reports whether the statement is CREATE [TEMP|TEMPORARY]
// TRIGGER. Matching the word anywhere in the statement made a table NAMED
// "trigger" open the body-skipping mode below, and everything up to the next
// "END" — a COMMIT included — went unchecked.
func isCreateTrigger(fields []string) bool {
	if len(fields) < 2 || fields[0] != "create" {
		return false
	}
	for _, field := range fields[1:] {
		switch field {
		case "trigger":
			return true
		case "temp", "temporary":
			continue
		default:
			return false
		}
	}
	return false
}

// transactionKeywords are the statement verbs a migration must not use: the
// runner owns the transaction.
var transactionKeywords = []string{"begin", "commit", "rollback", "savepoint", "release", "end"}

// escapeKeywords are the verbs whose effect leaves the state database.
//
// ATTACH reaches another file, and the reference schema is built by EXECUTING
// the catalog — so such a statement would write to that file during
// ValidateCatalog, before the state database is even opened, and any change it
// made there would sit outside the WAL transaction this package promises.
// VACUUM INTO creates a file the same way. PRAGMA is here because the runner's
// own markers are pragmas: a migration restamping application_id makes every
// later open fail, and nothing a migration legitimately needs is written as
// one — journal mode and foreign keys belong to the connection, which storage
// configures before the first migration runs.
var escapeKeywords = []string{"attach", "detach", "vacuum", "pragma"}

// rejectForbiddenStatements fails a migration that opens or closes a
// transaction, or that reaches outside the state database.
// The runner commits the DDL and the ledger row together; a nested BEGIN or an
// early COMMIT would split them and could leave applied DDL with no recorded
// version.
//
// Only the leading verb of each statement is inspected: the words themselves
// appear all over the explanatory comments, and a naive substring scan would
// reject a migration for what its documentation says.
func rejectForbiddenStatements(migration Migration) error {
	// A trigger body is written as BEGIN ... END and its statements are
	// separated by semicolons, so a plain split turns its END into what looks
	// like a transaction verb. Skipping the body keeps CREATE TRIGGER usable
	// in a future migration instead of banning it by accident.
	insideTriggerBody := false

	for _, statement := range splitSQLStatements(stripSQLComments(migration.SQL)) {
		fields := strings.Fields(strings.ToLower(statement))
		if len(fields) == 0 {
			continue
		}
		// Checked for every statement, trigger bodies included: a trigger on
		// the owner table rewrites it on somebody else's INSERT.
		if err := rejectRunnerOwnedTables(migration, statement); err != nil {
			return err
		}
		if insideTriggerBody {
			if fields[0] == "end" {
				insideTriggerBody = false
			}
			continue
		}
		// CREATE [TEMP|TEMPORARY] TRIGGER, and nothing else. Matching the word
		// anywhere in the statement made a table NAMED "trigger" open the
		// body-skipping mode, and everything up to the next "END" — a COMMIT
		// included — went unchecked.
		if isCreateTrigger(fields) {
			insideTriggerBody = true
			continue
		}
		for _, keyword := range transactionKeywords {
			if fields[0] == keyword {
				return fmt.Errorf("%w: version %s starts a statement with %q — the runner owns the transaction",
					ErrCatalogInvalid, migration.Version, keyword)
			}
		}
		for _, keyword := range escapeKeywords {
			if fields[0] == keyword {
				return fmt.Errorf("%w: version %s starts a statement with %q — a migration may only change the state database",
					ErrCatalogInvalid, migration.Version, keyword)
			}
		}
	}
	if insideTriggerBody {
		return fmt.Errorf("%w: version %s has an unterminated CREATE TRIGGER body", ErrCatalogInvalid, migration.Version)
	}
	return nil
}

// runnerOwnedTables are the tables this package writes itself.
var runnerOwnedTables = []string{ledgerTable, ownerTable}

// subjectSkipped are the words that may stand between a verb, or the keyword
// that follows it, and the name itself.
var subjectSkipped = []string{
	"if", "not", "exists", "temp", "temporary", "or", "rollback",
	"abort", "replace", "fail", "ignore", "unique", "virtual",
}

// rejectRunnerOwnedTables refuses a statement whose SUBJECT is a table the
// runner owns, other than the bootstrap CREATE TABLE of the owner table.
//
// The ledger records what has been applied and the owner table records whose
// database this is. A migration that rewrites either commits together with its
// own ledger row, and every later open then stops — ErrOwnerMismatch on a file
// this node had been using all along.
//
// Only the SUBJECT is read, and that is a deliberate retreat from walking
// table lists, aliases and subqueries. Every earlier version of that walk was
// wrong in both directions at once — it missed a table behind a parenthesis
// and refused a column that merely shared a name — and each fix produced the
// next pair. The subject is enough for what this rule is for: in SQLite a
// statement that MODIFIES a table always names it as its subject, so no write
// can hide from this. What it no longer reports is a READ of those tables,
// which changes nothing in the file.
//
// The boundary itself is elsewhere: confirmRunnerState compares the rows
// before and after every step. This is the early, precise error message.
func rejectRunnerOwnedTables(migration Migration, statement string) error {
	for _, subject := range statementSubjects(sqlTokens(statement)) {
		if !slices.Contains(runnerOwnedTables, subject) || isBootstrapOfOwnerTable(statement) {
			continue
		}
		return fmt.Errorf("%w: version %s acts on %q, which belongs to the runner",
			ErrCatalogInvalid, migration.Version, subject)
	}
	return nil
}

// statementSubjects returns the tables a fragment acts on.
//
// Usually that is one table, but CREATE TRIGGER carries a second: the fragment
// ends at the first semicolon, which falls INSIDE the body, so
// "CREATE TRIGGER t AFTER INSERT ON notes BEGIN UPDATE storage_metadata ..."
// arrives here as a single statement with two subjects. Reading only the first
// let a trigger on an ordinary table rewrite the owner row — and the row
// comparison cannot see it, because the trigger does not fire during the
// migration that installs it, but on some ordinary write long afterwards.
func statementSubjects(tokens []sqlToken) []string {
	var subjects []string
	if subject, named := statementSubject(tokens); named {
		subjects = append(subjects, subject)
	}
	if body := indexOfWord(tokens, "begin"); body >= 0 {
		if subject, named := statementSubject(tokens[body+1:]); named {
			subjects = append(subjects, subject)
		}
	}
	return subjects
}

// indexOfWord returns the position of the first occurrence of word, or -1.
func indexOfWord(tokens []sqlToken, word string) int {
	for i, token := range tokens {
		if token.isWord(word) {
			return i
		}
	}
	return -1
}

// statementSubject returns the table a statement acts on.
//
// The subject sits in a fixed place per verb: after INTO for INSERT, after
// FROM for DELETE, after the verb itself for UPDATE, after TABLE for DROP and
// ALTER, and for CREATE after TABLE, or after ON when the statement creates an
// index or a trigger over one.
func statementSubject(tokens []sqlToken) (string, bool) {
	if len(tokens) == 0 {
		return "", false
	}

	switch {
	case tokens[0].isWord("with"):
		// A CTE only prepares rows for the statement that follows it, and
		// WITH seed AS (...) UPDATE storage_metadata ... is that statement.
		// Stopping at the WITH left it to be caught when the migration ran
		// rather than while the catalog was still being validated.
		return statementSubject(tokens[afterCommonTableExpressions(tokens):])
	case tokens[0].isWord("insert"), tokens[0].isWord("replace"):
		return nameAfter(tokens, "into")
	case tokens[0].isWord("delete"):
		return nameAfter(tokens, "from")
	case tokens[0].isWord("update"):
		return nameAt(tokens, 1)
	case tokens[0].isWord("drop"), tokens[0].isWord("alter"):
		return nameAfter(tokens, "table")
	case tokens[0].isWord("create"):
		if name, found := nameAfter(tokens, "table"); found {
			return name, true
		}
		// CREATE INDEX ... ON notes(...) and CREATE TRIGGER ... ON notes name
		// the table the object is attached to. No other CREATE does: the ON of
		// CREATE VIEW v AS SELECT ... JOIN other ON storage_metadata = ...
		// belongs to a join condition, and reading it as a target refused a
		// perfectly ordinary view.
		if indexOfWord(tokens, "index") < 0 && indexOfWord(tokens, "trigger") < 0 {
			return "", false
		}
		return nameAfter(tokens, "on")
	default:
		return "", false
	}
}

// afterCommonTableExpressions returns the index of the statement that follows
// a WITH prefix: the first verb at the depth the WITH itself sits at.
func afterCommonTableExpressions(tokens []sqlToken) int {
	verbs := []string{"insert", "replace", "update", "delete", "select"}
	for i, token := range tokens[1:] {
		if token.Depth != tokens[0].Depth {
			continue
		}
		if slices.ContainsFunc(verbs, token.isWord) {
			return i + 1
		}
	}
	return len(tokens)
}

// nameAfter returns the name that follows the first occurrence of keyword.
func nameAfter(tokens []sqlToken, keyword string) (string, bool) {
	for i, token := range tokens {
		if token.isWord(keyword) {
			return nameAt(tokens, i+1)
		}
	}
	return "", false
}

// nameAt returns the name starting at index, following a qualifier to its last
// segment: in main.storage_metadata the table is the second half.
func nameAt(tokens []sqlToken, index int) (string, bool) {
	for ; index < len(tokens); index++ {
		if slices.ContainsFunc(subjectSkipped, tokens[index].isWord) {
			continue
		}
		if !tokens[index].isName() {
			return "", false
		}
		name := tokens[index].Text
		for index+1 < len(tokens) && tokens[index+1].Qualified {
			index++
			name = tokens[index].Text
		}
		return name, true
	}
	return "", false
}

// isBootstrapOfOwnerTable reports whether the statement is the CREATE TABLE
// that brings the owner table into existence — the one place the catalog is
// allowed to name it.
func isBootstrapOfOwnerTable(statement string) bool {
	var names []string
	for _, token := range sqlTokens(statement) {
		if token.Depth == 0 && token.Text != "" {
			names = append(names, token.Text)
		}
	}
	if len(names) < 3 || names[0] != "create" || names[1] != "table" {
		return false
	}
	names = names[2:]
	for len(names) > 1 && (names[0] == "if" || names[0] == "not" || names[0] == "exists") {
		names = names[1:]
	}
	return names[0] == ownerTable
}

// SQL text scanning.
//
// Every rule this package expresses over SQL text — "no COMMIT of your own",
// "no ON CONFLICT policy on the ledger", "these CHECK constraints and no
// others" — is only as good as its ability to tell code from the things that
// merely look like code. A quote-blind scan is trivially defeated:
// `INSERT INTO t VALUES('/*'); COMMIT;` reads as an unterminated comment that
// swallows the COMMIT, and `CHECK(name <> '/*') UNIQUE ON CONFLICT IGNORE`
// hides the policy the same way. So the scanners below always step over string
// literals and quoted identifiers first.

// isQuoteOpen reports whether b opens a SQLite string literal or quoted
// identifier.
func isQuoteOpen(b byte) bool {
	return b == '\'' || b == '"' || b == '`' || b == '['
}

// quoteEnd returns the index just past the literal or quoted identifier that
// starts at open. An unterminated one consumes the rest of the text, which is
// what SQLite itself does with it.
func quoteEnd(sqlText string, open int) int {
	closing := sqlText[open]
	if closing == '[' {
		closing = ']'
	}
	for i := open + 1; i < len(sqlText); i++ {
		if sqlText[i] != closing {
			continue
		}
		// '', "" and `` escape the quote character; [] has no escape form.
		if closing != ']' && i+1 < len(sqlText) && sqlText[i+1] == closing {
			i++
			continue
		}
		return i + 1
	}
	return len(sqlText)
}

// stripSQLComments replaces -- line comments and /* */ block comments with a
// single space, leaving string literals and quoted identifiers untouched.
//
// A comment becomes a space rather than nothing: it separates tokens, so
// "ON/**/CONFLICT" must not collapse into "ONCONFLICT" and hide the keyword
// pair from a search.
func stripSQLComments(sqlText string) string {
	var out strings.Builder
	for i := 0; i < len(sqlText); {
		switch {
		case isQuoteOpen(sqlText[i]):
			end := quoteEnd(sqlText, i)
			out.WriteString(sqlText[i:end])
			i = end
		case strings.HasPrefix(sqlText[i:], "--"):
			out.WriteByte(' ')
			end := strings.IndexByte(sqlText[i:], '\n')
			if end < 0 {
				return out.String()
			}
			i += end // the newline itself is kept by the default branch
		case strings.HasPrefix(sqlText[i:], "/*"):
			out.WriteByte(' ')
			end := strings.Index(sqlText[i+2:], "*/")
			if end < 0 {
				return out.String()
			}
			i += 2 + end + 2
		default:
			out.WriteByte(sqlText[i])
			i++
		}
	}
	return out.String()
}

// splitSQLStatements splits on semicolons that actually separate statements —
// never on one inside a string literal or a quoted identifier. Callers strip
// comments first.
func splitSQLStatements(sqlText string) []string {
	var statements []string
	start := 0
	for i := 0; i < len(sqlText); {
		switch {
		case isQuoteOpen(sqlText[i]):
			i = quoteEnd(sqlText, i)
		case sqlText[i] == ';':
			statements = append(statements, sqlText[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	return append(statements, sqlText[start:])
}

// sqlToken is one word of a statement with everything the scanners need to
// tell its ROLE apart from its spelling.
type sqlToken struct {
	// Text is the word, folded the way SQLite folds identifiers.
	Text string

	// Depth is the parenthesis nesting it appears at: CREATE TABLE t AS
	// SELECT copies rows, a column GENERATED ALWAYS AS (...) does not.
	Depth int

	// Quoted marks a quoted identifier: still a name, never a keyword.
	Quoted bool

	// Literal marks a single-quoted string: a VALUE, never a name. Reading
	// 'storage_metadata' as a table name refused catalogs that only mention
	// the word.
	Literal bool

	// Qualified marks a name that directly followed a dot, so that the table
	// in main.storage_metadata is the last segment rather than the first.
	Qualified bool

	// Listed marks a name that directly followed a comma, which is how the
	// second table of FROM ordinary, storage_metadata is written.
	Listed bool
}

// sqlWhitespace is the set SQLite treats as whitespace. Form feed and vertical
// tab belong to it: a scanner that stopped at space and tab did not see the
// word after them, while SQLite executed the statement all the same.
const sqlWhitespace = " \t\n\v\f\r"

// sqlTokens splits a statement into folded words, tracking parenthesis depth
// and reading quoted names as names.
//
// The scan runs over the ORIGINAL text and folds each token afterwards. Folding
// the whole statement first mixed two coordinate systems: "K" (U+212A) is three
// bytes and its lower case is one, so every offset past it — the quote
// boundaries among them — pointed somewhere else in the folded copy.
func sqlTokens(statement string) []sqlToken {
	var (
		tokens    []sqlToken
		word      strings.Builder
		depth     int
		qualified bool
		listed    bool
	)
	emit := func(token sqlToken) {
		token.Depth = depth
		token.Qualified = qualified
		token.Listed = listed
		qualified, listed = false, false
		tokens = append(tokens, token)
	}
	flush := func() {
		if word.Len() == 0 {
			return
		}
		emit(sqlToken{Text: asciiFold(word.String())})
		word.Reset()
	}

	for i := 0; i < len(statement); i++ {
		char := statement[i]
		switch {
		case isQuoteOpen(char):
			flush()
			end := quoteEnd(statement, i)
			emit(sqlToken{
				Text:    asciiFold(unquoteIdentifier(statement[i:end])),
				Quoted:  char != '\'',
				Literal: char == '\'',
			})
			i = end - 1
		case char == '(':
			flush()
			depth++
		case char == ')':
			flush()
			if depth > 0 {
				depth--
			}
		case char == '.':
			flush()
			qualified = true
		case char == ',':
			flush()
			listed = true
		case strings.IndexByte(sqlWhitespace, char) >= 0, strings.IndexByte(sqlPunctuation, char) >= 0:
			flush()
		default:
			word.WriteByte(char)
		}
	}
	flush()
	return tokens
}

// unquoteIdentifier strips the quote characters around a name or literal and
// collapses the doubled quotes SQLite uses to escape them.
func unquoteIdentifier(quoted string) string {
	if len(quoted) < 2 {
		return ""
	}
	opening := quoted[0]
	closing := opening
	if opening == '[' {
		closing = ']'
	}
	inner := strings.TrimSuffix(quoted[1:], string(closing))
	if closing == ']' {
		return inner
	}
	return strings.ReplaceAll(inner, string(closing)+string(closing), string(closing))
}

// isName reports whether the token can stand as a table name.
//
// A single-quoted string counts HERE and only here: SQLite accepts
// DROP TABLE 'storage_metadata' and reads the literal as the identifier, so a
// rule that skipped it in this position missed the statement entirely. In
// every other position a literal is a value — 'storage_metadata' inside a
// CHECK names nothing.
func (t sqlToken) isName() bool { return t.Text != "" }

// isWord reports whether the token can be a keyword — quoted names never are.
func (t sqlToken) isWord(word string) bool { return !t.Quoted && !t.Literal && t.Text == word }
