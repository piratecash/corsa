package migrations_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// TestUpgradeWriteRestartRead is the end-to-end rollout check: a database
// written by a pre-versioned binary is upgraded in place, keeps serving its
// old rows through the repository, accepts new writes, and returns everything
// after a restart.
//
// The individual pieces are covered elsewhere; this test exists because the
// failure that matters in the field is the combination — an upgrade that looks
// successful but loses history the moment the process restarts. It runs for
// every historical generation, because the one that actually shipped
// (generation 2) is not the one this branch was developed against.
func TestUpgradeWriteRestartRead(t *testing.T) {
	for _, generation := range legacyGenerations() {
		t.Run(generation.name, func(t *testing.T) {
			upgradeWriteRestartRead(t, generation)
		})
	}
}

func upgradeWriteRestartRead(t *testing.T, generation legacyGeneration) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "chatlog-legacy.db")
	buildLegacyDatabase(t, path, generation)

	self := owner(t)
	peer, err := domain.ParsePeerIdentity(strings.Repeat("b", 40))
	if err != nil {
		t.Fatalf("parse peer identity: %v", err)
	}

	// --- upgrade + read the pre-existing history --------------------------
	upgraded, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: path,
		Owner:        self,
		Catalog:      migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("upgrade open: %v", err)
	}

	store := chatlog.NewStore(upgraded.Executor(), self)
	before, err := store.Read(context.Background(), "dm", peer)
	if err != nil {
		t.Fatalf("read legacy conversation: %v", err)
	}
	if len(before) != 3 {
		t.Fatalf("legacy conversation has %d entries, want the 3 fixture rows: %+v", len(before), before)
	}

	// --- write through the upgraded database ------------------------------
	fresh := chatlog.Entry{
		ID:             "msg-after-upgrade",
		Sender:         self.String(),
		Recipient:      peer.String(),
		Body:           "written after the migration",
		CreatedAt:      time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSent,
	}
	if err := store.Append(context.Background(), "dm", self, fresh); err != nil {
		t.Fatalf("append after upgrade: %v", err)
	}
	if _, err := store.UpdateStatus(context.Background(), "dm", peer, domain.MessageID(fresh.ID), chatlog.StatusDelivered); err != nil {
		t.Fatalf("update status after upgrade: %v", err)
	}
	if err := upgraded.Close(); err != nil {
		t.Fatalf("close after upgrade: %v", err)
	}

	// --- restart ----------------------------------------------------------
	restarted := openState(t, path)
	if got, want := restarted.SchemaVersion(), storage.LatestVersion(migrations.Catalog()); got != want {
		t.Fatalf("SchemaVersion after restart = %s, want %s", got, want)
	}

	after, err := chatlog.NewStore(restarted.Executor(), self).Read(context.Background(), "dm", peer)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if len(after) != 4 {
		t.Fatalf("conversation has %d entries after restart, want 3 legacy + 1 new: %+v", len(after), after)
	}

	byID := make(map[string]chatlog.Entry, len(after))
	for _, entry := range after {
		byID[entry.ID] = entry
	}
	for _, legacy := range before {
		got, exists := byID[legacy.ID]
		if !exists {
			t.Fatalf("legacy entry %s disappeared across the restart", legacy.ID)
		}
		if got != legacy {
			t.Fatalf("legacy entry %s changed:\nbefore: %+v\nafter:  %+v", legacy.ID, legacy, got)
		}
	}
	written, exists := byID[fresh.ID]
	if !exists {
		t.Fatal("the message written after the upgrade did not survive the restart")
	}
	if written.DeliveryStatus != chatlog.StatusDelivered {
		t.Fatalf("delivery status = %q, want %q", written.DeliveryStatus, chatlog.StatusDelivered)
	}
	if written.Body != fresh.Body {
		t.Fatalf("body = %q, want %q", written.Body, fresh.Body)
	}
}

// TestRollbackToThePreviousBinaryStillReadsTheFile models the other direction:
// an upgraded database opened again by the binary that predates this layer.
//
// That binary knows nothing about schema_migrations or storage_metadata, and
// it starts by running its own CREATE ... IF NOT EXISTS DDL. The rollback
// contract is that this is a no-op, the extra tables are simply ignored, and
// every row is still there — which is why the default file name did not move
// and why every migration so far is additive.
func TestRollbackToThePreviousBinaryStillReadsTheFile(t *testing.T) {
	for _, generation := range legacyGenerations() {
		t.Run(generation.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "chatlog-legacy.db")
			buildLegacyDatabase(t, path, generation)

			before := tableDump(t, rawOpen(t, path), generation.tables)

			upgraded, err := storage.Open(context.Background(), storage.Config{
				ExplicitPath: path,
				Owner:        owner(t),
				Catalog:      migrations.Catalog(),
			})
			if err != nil {
				t.Fatalf("upgrade open: %v", err)
			}
			if err := upgraded.Close(); err != nil {
				t.Fatalf("close after upgrade: %v", err)
			}

			// The old binary: its DDL, its queries, no knowledge of the ledger.
			old := rawOpen(t, path)
			if _, err := old.ExecContext(context.Background(), readFixture(t, generation.schema)); err != nil {
				t.Fatalf("the previous binary's schema init failed on the upgraded file: %v", err)
			}

			after := tableDump(t, old, generation.tables)
			for table, rows := range before {
				if got := after[table]; got != rows {
					t.Fatalf("table %s changed for the rolled-back binary:\nbefore:\n%s\nafter:\n%s", table, rows, got)
				}
			}
		})
	}
}
