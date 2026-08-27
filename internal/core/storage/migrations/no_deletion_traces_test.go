package migrations_test

import (
	"context"
	"path/filepath"
	"testing"
)

// TestNothingRemembersADeletionAfterTheUpgrade pins migration 0007 against the
// database an earlier build left behind.
//
// Those builds answered a replayed message by keeping a row that recognised its
// id — a refusal in message_delete_intents, or one in reaction_refusals — long
// after the deletion itself was over. The id was stored as a keyed digest, so
// the row could not be read as a list of what the user deleted; the row's
// EXISTENCE was still the record. This is the step that takes them away, and
// the test asserts on the rows rather than on the digests precisely because the
// row is what the promise is about.
//
// The requests still owed to a peer are not touched, and that is the line this
// draws: a request is work outstanding, and it disappears when the peer answers
// it. Their `refuse_until` does go — on an owed row it is not part of the
// asking, it is a note of when the message was destroyed.
func TestNothingRemembersADeletionAfterTheUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "chatlog-legacy.db")
	buildLegacyDatabase(t, path, latestGeneration())

	// The shape the previous build wrote: two refusal-only rows, one owed
	// request carrying a deletion stamp, and a reaction refusal.
	raw := rawOpen(t, path)
	for _, statement := range []string{
		// The two tables verbatim as 0005 and 0006 declare them: the runner
		// checks the shape it finds against the SQL, so a fixture that drifts
		// would fail as schema drift instead of testing the step.
		`CREATE TABLE IF NOT EXISTS message_delete_intents (
			message_id TEXT PRIMARY KEY,
			peer TEXT NOT NULL,
			created_at TEXT NOT NULL,
			next_attempt_at TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			held INTEGER NOT NULL DEFAULT 0,
			owed INTEGER NOT NULL DEFAULT 1,
			refuse_until TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS reaction_refusals (
			scope      TEXT NOT NULL,
			message_id TEXT NOT NULL,
			refused_at TEXT NOT NULL,
			PRIMARY KEY (scope, message_id)
		)`,
		`INSERT INTO message_delete_intents (message_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until)
			VALUES ('digest-of-a-deleted-id', '', '2026-08-20T10:00:00Z', '2026-08-20T10:00:00Z', 0, 0, 0, '2026-08-28T10:00:00Z')`,
		`INSERT INTO message_delete_intents (message_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until)
			VALUES ('digest-of-another-deleted-id', '', '2026-08-21T10:00:00Z', '2026-08-21T10:00:00Z', 0, 0, 0, '2026-08-29T10:00:00Z')`,
		`INSERT INTO message_delete_intents (message_id, peer, created_at, next_attempt_at, attempts, held, owed, refuse_until)
			VALUES ('11111111-2222-4333-8444-555555555555', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
				'2026-08-25T10:00:00Z', '2026-08-25T10:05:00Z', 3, 0, 1, '2026-09-02T10:00:00Z')`,
		`INSERT INTO reaction_refusals (scope, message_id, refused_at)
			VALUES ('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'digest-of-a-deleted-id', '2026-08-20T10:00:00Z')`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed the pre-migration rows: %v", err)
		}
	}

	database := openState(t, path)

	for _, check := range []struct {
		what  string
		query string
	}{
		{"refusal rows", `SELECT COUNT(*) FROM message_delete_intents WHERE owed = 0`},
		{"deletion stamps", `SELECT COUNT(*) FROM message_delete_intents WHERE refuse_until <> ''`},
		{"reaction refusals", `SELECT COUNT(*) FROM reaction_refusals`},
	} {
		var remaining int
		if err := database.Executor().QueryRowContext(ctx, check.query).Scan(&remaining); err != nil {
			t.Fatalf("count the %s: %v", check.what, err)
		}
		if remaining != 0 {
			t.Errorf("%d %s survived the upgrade: the database still remembers a deletion", remaining, check.what)
		}
	}

	// The work outstanding is untouched — the peer is still going to be asked,
	// and the attempts already spent are still spent.
	var (
		peer     string
		attempts int
	)
	if err := database.Executor().QueryRowContext(ctx,
		`SELECT peer, attempts FROM message_delete_intents WHERE message_id = ?`,
		"11111111-2222-4333-8444-555555555555").Scan(&peer, &attempts); err != nil {
		t.Fatalf("read the request that must survive: %v", err)
	}
	if peer != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" || attempts != 3 {
		t.Errorf("the surviving request came out as peer=%q attempts=%d, want it unchanged", peer, attempts)
	}
}
