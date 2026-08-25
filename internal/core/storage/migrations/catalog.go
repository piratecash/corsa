// Package migrations holds the single global, forward-only migration catalog
// of the shared state database.
//
// The list is explicit and ordered. Registration through init() is deliberately
// not used: the exact set and order of schema steps must be readable at the
// composition root and assertable in tests, not assembled by import side
// effects.
//
// Rules for adding a migration:
//
//   - append a new NNNN_<domain>.sql file and a catalog entry with the next
//     version; versions are dense and start at 1;
//   - never edit or renumber a published migration — the ledger records its
//     checksum, and a changed one aborts startup as drift. Line endings are
//     part of that: .gitattributes pins these files to LF, and the checksum
//     folds CRLF to LF so a Windows checkout cannot record a different one;
//   - never write BEGIN/COMMIT: the runner owns the transaction, and never
//     name schema_migrations or storage_metadata: they belong to it too;
//   - a migration that creates tables or indexes needs no verifier of its own.
//     Every statement here is idempotent so that a pre-versioned database is
//     adopted instead of rebuilt, which means the DDL is a no-op there and
//     something has to prove the existing shape — the runner does it, deriving
//     the expectation from this file. The SQL IS the specification, so the two
//     cannot drift. A migration that creates nothing (an ALTER, a backfill)
//     must supply Migration.Invariant instead — a post-condition checked
//     inside the step's own transaction, whose failure rolls the step back.
package migrations

import (
	_ "embed"

	"github.com/piratecash/corsa/internal/core/storage"
)

//go:embed 0001_storage_metadata.sql
var storageMetadataSQL string

//go:embed 0002_chatlog_messages.sql
var chatlogMessagesSQL string

//go:embed 0003_chatlog_delivery_journals.sql
var chatlogDeliveryJournalsSQL string

//go:embed 0004_chatlog_decrypt_recovery.sql
var chatlogDecryptRecoverySQL string

//go:embed 0005_message_deletion.sql
var messageDeletionSQL string

//go:embed 0006_message_reactions.sql
var messageReactionsSQL string

// Catalog returns the ordered migration list for storage.Open.
func Catalog() []storage.Migration {
	return []storage.Migration{
		{
			Version: 1,
			Name:    "storage_metadata",
			SQL:     storageMetadataSQL,
		},
		{
			Version: 2,
			Name:    "chatlog_messages",
			SQL:     chatlogMessagesSQL,
		},
		{
			Version: 3,
			Name:    "chatlog_delivery_journals",
			SQL:     chatlogDeliveryJournalsSQL,
		},
		{
			Version: 4,
			Name:    "chatlog_decrypt_recovery",
			SQL:     chatlogDecryptRecoverySQL,
		},
		{
			Version: 5,
			Name:    "message_deletion",
			SQL:     messageDeletionSQL,
		},
		{
			Version: 6,
			Name:    "message_reactions",
			SQL:     messageReactionsSQL,
		},
	}
}
