package chatlog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func appendOutgoing(t *testing.T, store *Store, id, metadata string) {
	t.Helper()
	if err := store.Append(context.Background(), "dm", domain.PeerIdentityFromWire(intentSelf), Entry{
		ID:        id,
		Sender:    intentSelf,
		Recipient: intentPeer,
		Body:      "sealed",
		Metadata:  metadata,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("Append %s: %v", id, err)
	}
}

func emissionMarkOf(t *testing.T, store *Store, id string) bool {
	t.Helper()
	var metadata string
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT metadata FROM messages WHERE id = ?`, id).Scan(&metadata); err != nil {
		t.Fatalf("read metadata %s: %v", id, err)
	}
	return NeverEmitted(metadata)
}

// TestEmissionMarkSurvivesReopen is the whole point of the mark being on
// disk: the process that withheld a message is not the one that later has
// to answer whether the peer could ever have seen it.
func TestEmissionMarkSurvivesReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	identity := domain.PeerIdentityFromWire(intentSelf)

	store := newTestStoreAt(t, path, identity)
	appendOutgoing(t, store, string(intentMsgID), "")
	appendOutgoing(t, store, string(intentMsgB), "")
	if err := store.MarkNeverEmitted(ctx, []domain.MessageID{intentMsgID}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	reopened := newTestStoreAt(t, path, identity)
	if !emissionMarkOf(t, reopened, string(intentMsgID)) {
		t.Error("the withheld message reads as emitted after a restart")
	}
	if emissionMarkOf(t, reopened, string(intentMsgB)) {
		t.Error("an unmarked message must read as emitted")
	}
}

// TestAbsentMarkReadsAsEmitted pins the direction of the default. Every
// row written before this feature carries no mark, and the answer for
// them must be the cautious one — asking a peer about an id they cannot
// resolve costs one control DM, while the reverse leaves a delivered
// message with them and nothing left to retry it.
func TestAbsentMarkReadsAsEmitted(t *testing.T) {
	t.Parallel()

	for name, metadata := range map[string]string{
		"empty":      "",
		"invalid":    "{not json",
		"non-object": `["array"]`,
		"other keys": `{"superseded_by":"x"}`,
		"explicit":   `{"never_emitted":false}`,
	} {
		if NeverEmitted(metadata) {
			t.Errorf("%s metadata read as never-emitted, want emitted", name)
		}
	}
}

// TestClearNeverEmittedKeepsOtherMarks: the mark shares the column with
// the recovery marks, and withdrawing it must not take them with it.
func TestClearNeverEmittedKeepsOtherMarks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	appendOutgoing(t, store, string(intentMsgID), `{"retry_root_id":"root-1"}`)

	if err := store.MarkNeverEmitted(ctx, []domain.MessageID{intentMsgID}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}
	marks, found, err := store.EntryRecoveryMarks(ctx, string(intentMsgID))
	if err != nil || !found {
		t.Fatalf("EntryRecoveryMarks: found=%v err=%v", found, err)
	}
	if marks.RetryRootID != "root-1" {
		t.Fatalf("marking lost an unrelated key: retry_root_id = %q", marks.RetryRootID)
	}

	if err := store.ClearNeverEmitted(ctx, []domain.MessageID{intentMsgID}); err != nil {
		t.Fatalf("ClearNeverEmitted: %v", err)
	}
	if emissionMarkOf(t, store, string(intentMsgID)) {
		t.Error("the mark survived ClearNeverEmitted")
	}
	marks, found, err = store.EntryRecoveryMarks(ctx, string(intentMsgID))
	if err != nil || !found {
		t.Fatalf("EntryRecoveryMarks after clear: found=%v err=%v", found, err)
	}
	if marks.RetryRootID != "root-1" {
		t.Errorf("clearing lost an unrelated key: retry_root_id = %q", marks.RetryRootID)
	}
}

// TestMarkNeverEmittedOnNonObjectMetadata: json_set on a non-object blob
// returns it unchanged while still reporting the row as affected, so the
// statement replaces such a blob instead of reporting a mark that never
// landed.
func TestMarkNeverEmittedOnNonObjectMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	appendOutgoing(t, store, string(intentMsgID), `"a bare string"`)

	if err := store.MarkNeverEmitted(ctx, []domain.MessageID{intentMsgID}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}
	if !emissionMarkOf(t, store, string(intentMsgID)) {
		t.Error("the mark did not land on a row whose metadata was not an object")
	}
}

// TestEmissionMarksAreBatched: a backlog replay clears in one call with
// more ids than one statement may carry placeholders for.
func TestEmissionMarksAreBatched(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	ids := make([]domain.MessageID, 0, emissionMarkBatch*2+3)
	for i := range cap(ids) {
		id := domain.MessageID(idAtIndex(i))
		appendOutgoing(t, store, string(id), "")
		ids = append(ids, id)
	}

	if err := store.MarkNeverEmitted(ctx, ids); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}
	for _, id := range ids {
		if !emissionMarkOf(t, store, string(id)) {
			t.Fatalf("%s was not marked", id)
		}
	}
	if err := store.ClearNeverEmitted(ctx, ids); err != nil {
		t.Fatalf("ClearNeverEmitted: %v", err)
	}
	for _, id := range ids {
		if emissionMarkOf(t, store, string(id)) {
			t.Fatalf("%s kept its mark", id)
		}
	}
}

func idAtIndex(i int) string {
	const digits = "0123456789abcdef"
	return "00000000-0000-4000-8000-0000" + string([]byte{
		digits[(i>>12)&0xf], digits[(i>>8)&0xf], digits[(i>>4)&0xf], digits[i&0xf],
	}) + "0000"
}
