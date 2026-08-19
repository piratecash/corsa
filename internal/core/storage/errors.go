package storage

import "errors"

// Startup failure classes. Every one of them aborts Open: a node must never
// come up on a state database whose shape it cannot prove, because the file
// holds durable message history that a "repair by recreating" path would
// silently destroy.
var (
	// ErrCatalogInvalid means the embedded migration catalog itself is
	// malformed (duplicate, non-positive, out-of-order or gapped version,
	// empty name/SQL, missing verifier). Detected before the file is
	// touched — this is a programming error, not a state problem.
	ErrCatalogInvalid = errors.New("storage: invalid migration catalog")

	// ErrSchemaTooNew means the file was migrated by a newer binary. The
	// old binary refuses rather than guessing forward compatibility.
	ErrSchemaTooNew = errors.New("storage: database schema is newer than this binary")

	// ErrMigrationDrift means a version recorded in the ledger has a
	// different name or checksum than the catalog entry with that version:
	// a published migration was edited or a number was reused.
	ErrMigrationDrift = errors.New("storage: applied migration differs from the catalog")

	// ErrSchemaIncompatible means an object required by a migration already
	// exists with an unexpected shape — a partially created or foreign
	// schema. The version is not recorded, so the file is never mistaken
	// for a migrated one.
	ErrSchemaIncompatible = errors.New("storage: existing schema does not match the expected shape")

	// ErrOwnerMismatch means the database records a different owner
	// identity than the one this process runs under.
	ErrOwnerMismatch = errors.New("storage: database belongs to a different identity")

	// ErrForeignApplication means PRAGMA application_id holds a non-zero
	// value that is not Corsa's — the operator pointed the node at an
	// unrelated SQLite file.
	ErrForeignApplication = errors.New("storage: file is not a Corsa state database")

	// ErrCorrupt means PRAGMA integrity_check or foreign_key_check failed.
	// The file is left untouched; recovery from backup is a separate,
	// explicit operation.
	ErrCorrupt = errors.New("storage: database integrity check failed")
)
