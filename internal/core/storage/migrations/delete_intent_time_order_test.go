package migrations_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

func deleteIntentTimeIndexRows(t *testing.T, db storage.Executor) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT quote(message_id)||'|'||quote(peer)||'|'||quote(created_at)||'|'||quote(next_attempt_at)||'|'||quote(attempts)||'|'||quote(held)||'|'||quote(owed)||'|'||quote(refuse_until)||'|'||quote(kind)||'|'||quote(request_id) FROM message_delete_intents ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	var result []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		result = append(result, raw)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDeleteIntentTimeIndexesUpgradeWithoutRewritingLegacyTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := migrations.Catalog()
	if len(catalog) != 8 || catalog[7].Version != 8 {
		t.Fatal("test must qualify the exact version8 transition")
	}
	oldConfig := storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog[:7]}
	old, err := storage.Open(ctx, oldConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	})
	stamps := []string{"0001-01-01T00:00:00Z", "2026-10-29T12:00:00Z", "2026-10-29T12:00:00.1Z", "2026-10-29T12:00:00.11Z", "2026-10-29T12:00:00.100000000Z", "2026-10-29T12:00:00.123456789Z"}
	for i, stamp := range stamps {
		_, err := old.Executor().ExecContext(ctx, `INSERT INTO message_delete_intents(message_id,peer,created_at,next_attempt_at,attempts,held,owed,refuse_until,kind,request_id) VALUES(?,?,?,?,?,?,1,'','message','')`, fmt.Sprintf("%08d-2222-4333-8444-555555555555", i), strings.Repeat("2", 40), stamp, stamp, i, i%2)
		if err != nil {
			t.Fatal(err)
		}
	}
	before := deleteIntentTimeIndexRows(t, old.Executor())
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := storage.Open(ctx, storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := upgraded.Close(); err != nil {
			t.Error(err)
		}
	})
	if upgraded.SchemaVersion() != 8 {
		t.Fatal("upgrade did not commit version8")
	}
	if after := deleteIntentTimeIndexRows(t, upgraded.Executor()); !reflect.DeepEqual(before, after) {
		t.Fatal("index migration rewrote original task bytes/state")
	}
	var indexes int
	if err := upgraded.Executor().QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name IN ('idx_message_delete_intents_due_ns','idx_message_delete_intents_peer_held_ns')`).Scan(&indexes); err != nil || indexes != 2 {
		t.Fatalf("new indexes=%d err=%v", indexes, err)
	}
	if err := upgraded.Close(); err != nil {
		t.Fatal(err)
	}
	// The forward-only catalog refuses a downgrade; it does not erase the
	// ledger/indexes or rewrite history to make an old binary open the file.
	if oldAgain, err := storage.Open(ctx, oldConfig); !errors.Is(err, storage.ErrSchemaTooNew) {
		if oldAgain != nil {
			if closeErr := oldAgain.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
		t.Fatalf("old catalog downgrade err=%v", err)
	}
	reopened, err := storage.Open(ctx, storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if after := deleteIntentTimeIndexRows(t, reopened.Executor()); !reflect.DeepEqual(before, after) {
		t.Fatal("reopen changed the qualified legacy tasks")
	}
}

func TestDeleteIntentTimeIndexRejectsUnsupportedActiveRowsAtomically(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, stamp string }{
		{"empty", ""},
		{"active-offset", "2026-10-29T12:00:00.1+00:00"},
		{"comma-fraction", "2026-10-29T12:00:00,1Z"},
		{"excess-precision", "2026-10-29T12:00:00.1234567891Z"},
		{"invalid-date", "2026-02-30T12:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			catalog := migrations.Catalog()
			if len(catalog) != 8 {
				t.Fatal("expected exact eight-step catalog")
			}
			cfg := storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog[:7]}
			old, err := storage.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := old.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := old.Executor().ExecContext(ctx, `INSERT INTO message_delete_intents(message_id,peer,created_at,next_attempt_at,attempts,held,owed,refuse_until,kind,request_id) VALUES('81111111-2222-4333-8444-555555555555',?,'2026-10-29T12:00:00Z',?,3,1,1,'','message','')`, strings.Repeat("2", 40), tc.stamp); err != nil {
				t.Fatal(err)
			}
			before := deleteIntentTimeIndexRows(t, old.Executor())
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			latest, err := storage.Open(ctx, storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog})
			if latest != nil {
				if closeErr := latest.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if err == nil || !strings.Contains(err.Error(), "active delete intent time") {
				t.Fatalf("unsupported active timestamp was not refused: %v", err)
			}
			if tc.stamp != "" && strings.Contains(err.Error(), tc.stamp) {
				t.Fatal("migration refusal exposed raw task timestamp")
			}
			unchanged, err := storage.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("version7 could not reopen after refused migration: %v", err)
			}
			t.Cleanup(func() {
				if err := unchanged.Close(); err != nil {
					t.Error(err)
				}
			})
			if unchanged.SchemaVersion() != 7 {
				t.Fatal("failed invariant advanced ledger")
			}
			if after := deleteIntentTimeIndexRows(t, unchanged.Executor()); !reflect.DeepEqual(before, after) {
				t.Fatal("failed migration rewrote active task")
			}
			var indexes int
			if err := unchanged.Executor().QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('idx_message_delete_intents_due_ns','idx_message_delete_intents_peer_held_ns')`).Scan(&indexes); err != nil || indexes != 0 {
				t.Fatalf("refused migration left indexes=%d err=%v", indexes, err)
			}
		})
	}
}
