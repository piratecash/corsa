package chatlog

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// newTestStore opens a fresh shared state database in a temporary directory
// and returns the repository on top of it.
//
// The tests deliberately go through the real storage layer and the real
// migration catalog rather than hand-rolling the DDL: what the repository
// queries and what the migrations create must be the same schema, and a
// test-only CREATE TABLE is exactly how those two drift apart.
func newTestStore(t *testing.T, identity domain.PeerIdentity) *Store {
	t.Helper()
	return newTestStoreAt(t, filepath.Join(t.TempDir(), "state.db"), identity)
}

// newTestStoreAt is newTestStore against a caller-chosen file, for tests that
// reopen the same database.
func newTestStoreAt(t *testing.T, path string, identity domain.PeerIdentity) *Store {
	t.Helper()

	database, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        identity,
		Catalog:      migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("open state database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	return NewStore(database.Executor(), identity)
}

// storeFor is the common shorthand: a store for an identity given as hex.
func storeFor(t *testing.T, identityHex string) *Store {
	t.Helper()
	return newTestStore(t, domain.PeerIdentityFromWire(identityHex))
}
