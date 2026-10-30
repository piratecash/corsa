package migrations_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

func messageIdentityProjection(t *testing.T, db storage.Executor, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
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
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestMessageIdentityUpgradePreservesArrivalRowsAndBindsJournals(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := migrations.Catalog()
	if len(catalog) != 9 || catalog[8].Version != 9 {
		t.Fatal("expected exact message identity transition")
	}
	cfg := storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog[:8]}
	old, err := storage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Close() })
	self := owner(t).String()
	peer := strings.Repeat("b", 40)
	for _, statement := range []string{
		`INSERT INTO messages(rowid,id,topic,sender,recipient,body,flag,delivery_status,ttl_seconds,metadata,created_at,updated_at) VALUES(101,'shared','dm','` + peer + `','` + self + `','sealed-old','any-delete','delivered',7,'{"on_wire":true}','2026-10-30T00:00:00Z','unchanged')`,
		`INSERT INTO seen_ack(rowid,id) VALUES(71,'shared'),(72,'orphan')`,
		`INSERT INTO delivery_failed(rowid,id) VALUES(81,'shared'),(82,'orphan')`,
		`INSERT INTO decrypt_resend_intents(rowid,root,original_id,peer,replacement_id,created_at) VALUES(91,'root','original','` + peer + `','replacement','2026-10-30T00:00:00Z')`,
		`INSERT INTO message_reactions(rowid,scope,message_id,actor,emoji,op,clock,pending,first_seen_at,updated_at) VALUES(111,'` + peer + `','shared','` + self + `','x',1,4,0,'first','last')`,
		`INSERT INTO message_delete_intents(rowid,message_id,peer,created_at,next_attempt_at,attempts,held,owed,refuse_until,kind,request_id) VALUES(121,'shared','` + peer + `','2026-10-30T00:00:00Z','2026-10-30T00:00:00.1Z',2,1,1,'','message',''),(122,'gone','` + peer + `','2026-10-30T00:00:00Z','2026-10-30T00:00:00Z',3,0,1,'','message','')`,
	} {
		if _, err := old.Executor().ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	projections := map[string]string{
		"messages":  `SELECT rowid||'|'||quote(id)||'|'||quote(topic)||'|'||quote(sender)||'|'||quote(recipient)||'|'||quote(body)||'|'||quote(flag)||'|'||quote(delivery_status)||'|'||ttl_seconds||'|'||quote(metadata)||'|'||quote(created_at)||'|'||quote(updated_at) FROM messages ORDER BY rowid`,
		"resends":   `SELECT rowid||'|'||quote(root)||'|'||quote(original_id)||'|'||quote(peer)||'|'||quote(replacement_id)||'|'||quote(created_at) FROM decrypt_resend_intents ORDER BY rowid`,
		"reactions": `SELECT rowid||'|'||quote(scope)||'|'||quote(message_id)||'|'||quote(actor)||'|'||quote(emoji)||'|'||op||'|'||clock||'|'||pending||'|'||quote(first_seen_at)||'|'||quote(updated_at) FROM message_reactions ORDER BY rowid`,
		"deletions": `SELECT rowid||'|'||quote(message_id)||'|'||quote(peer)||'|'||quote(created_at)||'|'||quote(next_attempt_at)||'|'||attempts||'|'||held||'|'||owed||'|'||quote(refuse_until)||'|'||quote(kind)||'|'||quote(request_id) FROM message_delete_intents ORDER BY rowid`,
		"ledger":    `SELECT version||'|'||name||'|'||checksum||'|'||applied_at FROM schema_migrations WHERE version<=8 ORDER BY version`,
	}
	before := map[string][]string{}
	for name, q := range projections {
		before[name] = messageIdentityProjection(t, old.Executor(), q)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Catalog = catalog
	upgraded, err := storage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upgraded.Close() })
	for name, q := range projections {
		if got := messageIdentityProjection(t, upgraded.Executor(), q); !reflect.DeepEqual(got, before[name]) {
			t.Fatalf("%s changed during identity migration: before=%v after=%v", name, before[name], got)
		}
	}
	for _, table := range []string{"seen_ack", "delivery_failed"} {
		var rowid int
		var topic, id, sender, recipient string
		if err := upgraded.Executor().QueryRowContext(ctx, `SELECT rowid,topic,id,sender,recipient FROM `+table).Scan(&rowid, &topic, &id, &sender, &recipient); err != nil {
			t.Fatal(err)
		}
		expected := 71
		if table == "delivery_failed" {
			expected = 81
		}
		if rowid != expected || topic != "dm" || id != "shared" || sender != peer || recipient != self {
			t.Fatalf("%s wrong live binding: %d %s/%s/%s/%s", table, rowid, topic, id, sender, recipient)
		}
		var orphan string
		var orphanRow int
		if err := upgraded.Executor().QueryRowContext(ctx, `SELECT rowid,id FROM `+table+`_legacy_unbound`).Scan(&orphanRow, &orphan); err != nil {
			t.Fatal(err)
		}
		if orphan != "orphan" || orphanRow != expected+1 {
			t.Fatalf("%s lost unbound old fact", table)
		}
	}
	var bound, unbound int
	if err := upgraded.Executor().QueryRowContext(ctx, `SELECT COUNT(*) FROM message_delete_intents WHERE message_id='shared' AND original_sender=? AND original_recipient=?`, peer, self).Scan(&bound); err != nil {
		t.Fatal(err)
	}
	if err := upgraded.Executor().QueryRowContext(ctx, `SELECT COUNT(*) FROM message_delete_intents WHERE message_id='gone' AND original_sender='' AND original_recipient=''`).Scan(&unbound); err != nil {
		t.Fatal(err)
	}
	if bound != 1 || unbound != 1 {
		t.Fatal("delete intent binding guessed or lost original parties")
	}
	store := chatlog.NewStore(upgraded.Executor(), owner(t))
	other := strings.Repeat("c", 40)
	if fresh, err := store.AppendReportNew(ctx, "dm", owner(t), chatlog.Entry{ID: "shared", Sender: other, Recipient: self, Body: "second author", Flag: "any-delete", CreatedAt: "2026-10-30T00:00:01Z"}); err != nil || !fresh {
		t.Fatalf("colliding author insert=%v err=%v", fresh, err)
	}
	if _, _, err := store.EntryByID(ctx, "shared"); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("legacy ambiguous lookup=%v", err)
	}
	original, found, err := store.EntryByRef(ctx, domain.MessageRef{Topic: "dm", ID: "shared", Sender: peer, Recipient: self})
	if err != nil || !found || original.RowID != 101 || original.Body != "sealed-old" {
		t.Fatalf("original exact row=%+v found=%v err=%v", original, found, err)
	}
	if err := upgraded.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Catalog = catalog[:8]
	if db, err := storage.Open(ctx, cfg); !errors.Is(err, storage.ErrSchemaTooNew) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("version8 downgrade=%v", err)
	}
	cfg.Catalog = catalog
	reopened, err := storage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.SchemaVersion() != 9 {
		t.Fatal("reopen lost version9")
	}
}

func TestMessageIdentityUpgradeRejectsUnrepresentableLegacyRowAtomically(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	catalog := migrations.Catalog()
	cfg := storage.Config{ExplicitPath: path, Owner: owner(t), Catalog: catalog[:8]}
	old, err := storage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Executor().ExecContext(ctx, `INSERT INTO messages(id,sender,recipient,body,created_at) VALUES(NULL,'a','b','invalid old row','')`); err != nil {
		t.Fatal(err)
	}
	before := messageIdentityProjection(t, old.Executor(), `SELECT rowid||'|'||quote(id)||'|'||quote(body) FROM messages`)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Catalog = catalog
	if db, err := storage.Open(ctx, cfg); err == nil {
		_ = db.Close()
		t.Fatal("migration accepted null legacy identity")
	}
	cfg.Catalog = catalog[:8]
	again, err := storage.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.Close() }()
	if again.SchemaVersion() != 8 {
		t.Fatal("failed migration advanced the ledger")
	}
	if got := messageIdentityProjection(t, again.Executor(), `SELECT rowid||'|'||quote(id)||'|'||quote(body) FROM messages`); !reflect.DeepEqual(got, before) {
		t.Fatal("failed migration changed original row")
	}
	var additions int
	if err := again.Executor().QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('idx_messages_wire_id','seen_ack_legacy_unbound','delivery_failed_legacy_unbound') OR name GLOB 'h1_*_v8'`).Scan(&additions); err != nil {
		t.Fatal(err)
	}
	if additions != 0 {
		t.Fatal("failed migration retained partial objects")
	}
}
