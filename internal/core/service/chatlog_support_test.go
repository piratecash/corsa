package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// newTestStateDB opens a real shared state database in a temporary directory,
// migrated by the production catalog.
//
// Service tests deliberately do not hand-roll chatlog DDL: a test-local
// CREATE TABLE is how the schema the repository queries and the schema the
// migrations create drift apart, and the drift only surfaces in production.
func newTestStateDB(t *testing.T, owner domain.PeerIdentity) *storage.Database {
	t.Helper()

	database, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: filepath.Join(t.TempDir(), "state.db"),
		Owner:        owner,
		Catalog:      migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("open state database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// newTestChatlogStore is newTestStateDB plus the repository on top of it.
func newTestChatlogStore(t *testing.T, owner domain.PeerIdentity) *chatlog.Store {
	t.Helper()
	return chatlog.NewStore(newTestStateDB(t, owner).Executor(), owner)
}

// newClosedChatlogStore returns a repository whose database is already closed,
// so every query fails. Used to assert the UI paths that must survive a dead
// chatlog rather than panic.
func newClosedChatlogStore(t *testing.T, owner domain.PeerIdentity) *chatlog.Store {
	t.Helper()

	database := newTestStateDB(t, owner)
	executor := database.Executor()
	if err := database.Close(); err != nil {
		t.Fatalf("close state database: %v", err)
	}
	// The pool is closed but the handle stays usable as an Executor: every
	// call now returns sql.ErrConnDone.
	return chatlog.NewStore(executor, owner)
}
