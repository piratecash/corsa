package migrations_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// TestNextSubsystemAddsATableWithoutTouchingChatlog is the extensibility
// contract: a new subsystem gets its table by appending one migration and
// reading the same executor. It imports neither chatlog nor a second database,
// and no generic CRUD layer is involved.
//
// The test builds the "next" migration inline rather than adding a real one to
// the catalog, so the point being proven — that the seam works — does not
// require a production table nobody uses yet.
func TestNextSubsystemAddsATableWithoutTouchingChatlog(t *testing.T) {
	const exampleDDL = `CREATE TABLE IF NOT EXISTS example_subsystem_state (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);`

	nextVersion := storage.LatestVersion(migrations.Catalog()) + 1
	catalog := append(migrations.Catalog(), storage.Migration{
		Version: nextVersion,
		Name:    "example_subsystem",
		SQL:     exampleDDL,
	})

	// The existing chatlog rows are already there: the new subsystem arrives
	// on a live database, not a fresh one.
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

	if got := database.SchemaVersion(); got != nextVersion {
		t.Fatalf("SchemaVersion = %s, want %s", got, nextVersion)
	}

	// The new repository writes through the same executor the chatlog uses.
	executor := database.Executor()
	if _, err := executor.ExecContext(context.Background(),
		`INSERT INTO example_subsystem_state (key, value) VALUES ('k', 'v')`); err != nil {
		t.Fatalf("write through the shared executor: %v", err)
	}
	var value string
	if err := executor.QueryRowContext(context.Background(),
		`SELECT value FROM example_subsystem_state WHERE key = 'k'`).Scan(&value); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if value != "v" {
		t.Fatalf("value = %q, want %q", value, "v")
	}

	// The chatlog rows are untouched by the new subsystem's arrival.
	var messages int
	if err := executor.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM messages`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messages != 5 {
		t.Fatalf("messages = %d, want the 5 fixture rows", messages)
	}
}
