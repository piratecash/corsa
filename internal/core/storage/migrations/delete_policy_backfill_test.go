package migrations_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// withDeletePolicyBackfilled applies the rule of migration 0007 to a table
// dump, so the adoption test can SAY what an upgrade is allowed to change
// instead of dropping the column from its comparison. Excluding it would also
// stop that test noticing a backfill that reached rows it must never touch.
func withDeletePolicyBackfilled(t *testing.T, dump string) string {
	t.Helper()
	if dump == "" {
		return dump
	}

	lines := strings.Split(dump, "\n")
	for i, line := range lines {
		fields := strings.Split(line, " ")
		topic := ""
		flagAt := -1
		for j, field := range fields {
			name, value, ok := strings.Cut(field, "=")
			if !ok {
				// A fixture value containing a space would split into
				// fields this cannot read, and the normalizer would
				// quietly stop normalizing. Fail loudly instead.
				t.Fatalf("dump field %q of row %q is not name=value", field, line)
			}
			switch name {
			case "topic":
				topic = value
			case "flag":
				flagAt = j
			}
		}
		if flagAt < 0 {
			t.Fatalf("row %q carries no flag column", line)
		}
		if topic != "dm" {
			continue
		}
		switch fields[flagAt] {
		case "flag=", "flag=sender-delete":
			fields[flagAt] = "flag=any-delete"
		}
		lines[i] = strings.Join(fields, " ")
	}
	return strings.Join(lines, "\n")
}

// TestLegacyDeletePolicyFlagsBecomeAnyDelete pins the backfill that repairs
// the histories written before the shared-delete policy existed.
//
// The flag on a row is the answer its holder gives when the OTHER side asks
// for the message to be removed, and it is never rewritten at runtime. Every
// message sent before the policy changed therefore still carries the old
// author-only answer, which is why deleting a peer-authored message came back
// as "the peer refused" and why clearing a thread left the requester's own
// half standing on the other side. The old value was never a choice anybody
// made — the flag has no UI and the sender stamped whatever its build's
// default was — so the histories are brought to the policy the product
// actually promises.
//
// Scoped deliberately: `immutable` is a real refusal and survives,
// `auto-delete-ttl` carries expiry semantics this must not touch, and rows
// outside a direct conversation are not part of the promise at all.
func TestLegacyDeletePolicyFlagsBecomeAnyDelete(t *testing.T) {
	for _, generation := range legacyGenerations() {
		t.Run(generation.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "chatlog-legacy.db")
			buildLegacyDatabase(t, path, generation)

			// Probes the scope from the side the shared fixture cannot: a
			// broadcast row with the same empty flag must come out untouched,
			// so a passing test cannot be explained by "the backfill rewrote
			// everything it found".
			raw := rawOpen(t, path)
			if _, err := raw.ExecContext(context.Background(), `
				INSERT INTO messages (id, topic, sender, recipient, body, flag, delivery_status, ttl_seconds, metadata, created_at, updated_at)
				VALUES ('msg-global-empty-flag', 'global',
					'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '',
					'broadcast-body', '', 'sent', 0, '', '2026-08-01T10:20:00Z', '')`); err != nil {
				t.Fatalf("insert the broadcast probe: %v", err)
			}

			database := openState(t, path)

			want := map[string]string{
				// The two the backfill exists for: the old default and the
				// pre-flag empty value, both meaning "only the author".
				"msg-outgoing-sent":          "any-delete",
				"msg-incoming-sender-delete": "any-delete",
				// A refusal somebody meant stays a refusal.
				"msg-incoming-seen": "immutable",
				// Expiry semantics, not a deletion policy.
				"msg-outgoing-delivered-ttl": "auto-delete-ttl",
				// Already at the policy; nothing to do.
				"msg-global": "any-delete",
				// Out of scope: not a conversation between two identities.
				"msg-global-empty-flag": "",
			}
			for id, wantFlag := range want {
				var flag string
				if err := database.Executor().QueryRowContext(context.Background(),
					`SELECT flag FROM messages WHERE id = ?`, id).Scan(&flag); err != nil {
					t.Fatalf("read the flag of %s: %v", id, err)
				}
				if flag != wantFlag {
					t.Errorf("flag of %s = %q, want %q", id, flag, wantFlag)
				}
			}
		})
	}
}

// TestDeletePolicyBackfillIsIdempotent runs the whole catalog against a
// database that has already been through it. The step is recorded in the
// ledger and must not run twice, but a backfill that would corrupt anything
// on a second pass is a backfill nobody can re-apply after a restore, so the
// statement itself is proven re-runnable rather than merely never re-run.
func TestDeletePolicyBackfillIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chatlog-legacy.db")
	buildLegacyDatabase(t, path, latestGeneration())

	first := openState(t, path)
	before := tableDump(t, first.Executor(), []string{"messages"})
	if err := first.Close(); err != nil {
		t.Fatalf("close after the first open: %v", err)
	}

	second := openState(t, path)
	if got, want := second.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
		t.Fatalf("SchemaVersion = %s, want %s", got, want)
	}
	after := tableDump(t, second.Executor(), []string{"messages"})

	if before["messages"] != after["messages"] {
		t.Fatalf("re-opening the database changed the rows:\nbefore:\n%s\nafter:\n%s",
			before["messages"], after["messages"])
	}
}
