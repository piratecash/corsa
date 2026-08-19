package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The shared state database only stays shared while nobody else opens or
// shapes it. These tests read the production sources and fail on the two moves
// that would quietly bring back a second owner: another package calling
// sql.Open, or a repository creating its own tables.

// repoRoot walks up from this package to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s: %v", root, err)
	}
	return root
}

// productionGoFiles returns every non-test .go file under internal, sdk and
// cmd, keyed by its repository-relative path.
func productionGoFiles(t *testing.T) map[string]string {
	t.Helper()

	root := repoRoot(t)
	sources := make(map[string]string)
	for _, tree := range []string{"internal", "sdk", "cmd"} {
		err := filepath.Walk(filepath.Join(root, tree), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			sources[relative] = string(content)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", tree, err)
		}
	}
	if len(sources) == 0 {
		t.Fatal("no production sources found — the boundary check would pass vacuously")
	}
	return sources
}

func TestOnlyStorageOpensTheStateDatabase(t *testing.T) {
	for path, content := range productionGoFiles(t) {
		if filepath.Dir(path) == filepath.Join("internal", "core", "storage") {
			continue
		}
		if strings.Contains(content, "sql.Open(") {
			t.Errorf("%s calls sql.Open — the state database has exactly one owner, internal/core/storage", path)
		}
	}
}

func TestRepositoriesContainNoDDL(t *testing.T) {
	// Schema statements belong to the migration catalog. A repository that
	// creates its own table makes the recorded version a lie: the file would
	// hold objects no migration knows about, and the next verifier would see
	// a shape nobody declared.
	statements := []string{
		"CREATE TABLE",
		"CREATE INDEX",
		"CREATE UNIQUE INDEX",
		"ALTER TABLE",
		"DROP TABLE",
		"DROP INDEX",
	}

	for path, content := range productionGoFiles(t) {
		directory := filepath.Dir(path)
		if directory == filepath.Join("internal", "core", "storage") ||
			directory == filepath.Join("internal", "core", "storage", "migrations") {
			continue
		}
		// Only string literals are inspected: prose is allowed to describe
		// the schema, executable SQL is not.
		for _, literal := range stringLiterals(t, path, content) {
			upper := strings.ToUpper(literal)
			for _, statement := range statements {
				if strings.Contains(upper, statement) {
					t.Errorf("%s contains %q — schema changes belong to internal/core/storage/migrations", path, statement)
				}
			}
		}
	}
}

// stringLiterals returns the decoded string literals of a Go source file.
// Scanning literals rather than raw text keeps the DDL check on executable
// SQL: a comment may describe the schema, a query may not create it.
func stringLiterals(t *testing.T, path, content string) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, content, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var literals []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		decoded, err := strconv.Unquote(literal.Value)
		if err != nil {
			// Unparsable literals are not a boundary problem.
			return true
		}
		literals = append(literals, decoded)
		return true
	})
	return literals
}

func TestStorageImportsNoJSONStateFiles(t *testing.T) {
	// Scope guard. The shared database exists so that a future subsystem can
	// add a table; it is not an importer for the JSON stores. Trust, peers,
	// identity and intents keep their files and their formats, and a
	// migration that quietly starts reading one of them would change what
	// "rollback" means for data this task never touched.
	jsonState := []string{
		"identity-",
		"trust-",
		"identity-intents-",
		"peers-",
		".json",
	}

	for path, content := range productionGoFiles(t) {
		directory := filepath.Dir(path)
		if directory != filepath.Join("internal", "core", "storage") &&
			directory != filepath.Join("internal", "core", "storage", "migrations") {
			continue
		}
		for _, literal := range stringLiterals(t, path, content) {
			for _, marker := range jsonState {
				if strings.Contains(literal, marker) {
					t.Errorf("%s references JSON state %q — importing the JSON stores is out of this layer's scope", path, marker)
				}
			}
		}
	}
}
