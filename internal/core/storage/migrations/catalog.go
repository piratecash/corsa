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
	"context"
	_ "embed"
	"fmt"

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

//go:embed 0007_conversation_delete.sql
var conversationDeleteSQL string

// conversationDeleteHolds is the post-condition of 0007.
//
// The step is a dozen statements and only two of them declare a schema object,
// so almost none of it would be noticed by the runner's own shape check: a
// WHERE clause that drifted would record the version on the strength of an
// UPDATE that matched nothing. Every claim the file makes is therefore asserted
// here, inside the step's own transaction, and a failure rolls the whole thing
// back rather than reporting a half-migrated database as migrated.
//
// What each check is really guarding is worth naming, because they are not the
// same kind of thing: the first two are the FEATURE (deletions can be asked for
// and answered at all), and the rest are the PROMISE (nothing on this disk
// still says a deletion happened).
func conversationDeleteHolds(ctx context.Context, tx storage.SchemaReader) error {
	for _, column := range []string{"kind", "request_id"} {
		var present int
		if err := storage.ScanOne(ctx, tx, &present, `
			SELECT COUNT(*) FROM pragma_table_info('message_delete_intents') WHERE name = ?`, column); err != nil {
			return fmt.Errorf("look for the %s column of message_delete_intents: %w", column, err)
		}
		if present != 1 {
			return fmt.Errorf("message_delete_intents has no %s column", column)
		}
	}

	for _, check := range []struct {
		what  string
		query string
	}{
		{"direct messages still carrying an author-only deletion policy",
			`SELECT COUNT(*) FROM messages WHERE topic = 'dm' AND flag IN ('', 'sender-delete')`},
		{"refusal rows in message_delete_intents",
			`SELECT COUNT(*) FROM message_delete_intents WHERE owed = 0`},
		{"deletion timestamps on the requests that remain",
			`SELECT COUNT(*) FROM message_delete_intents WHERE refuse_until <> ''`},
		{"rows in reaction_refusals",
			`SELECT COUNT(*) FROM reaction_refusals`},
		{"reactions still waiting for a message this node does not have",
			`SELECT COUNT(*) FROM message_reactions WHERE pending = 1`},
	} {
		var remaining int
		if err := storage.ScanOne(ctx, tx, &remaining, check.query); err != nil {
			return fmt.Errorf("count the %s: %w", check.what, err)
		}
		if remaining > 0 {
			return fmt.Errorf("%d %s", remaining, check.what)
		}
	}
	return nil
}

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
		{
			Version:   7,
			Name:      "conversation_delete",
			SQL:       conversationDeleteSQL,
			Invariant: conversationDeleteHolds,
		},
	}
}
