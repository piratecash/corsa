package chatlog

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestWipeTombstonesSurviveReopen is why the refusal is on disk: the
// replay window of a deleted message and a restart overlap, and a refusal
// lost with the process lets the echo re-insert the row the user erased.
func TestWipeTombstonesSurviveReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	identity := domain.PeerIdentityFromWire(intentSelf)
	expiry := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)

	store := newTestStoreAt(t, path, identity)
	if err := store.NoteWipeTombstones(ctx, []domain.MessageID{intentMsgID, intentMsgB}, expiry); err != nil {
		t.Fatalf("NoteWipeTombstones: %v", err)
	}

	reopened := newTestStoreAt(t, path, identity)
	live, err := reopened.LiveWipeTombstones(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	if len(live) != 2 {
		t.Fatalf("live tombstones = %d, want 2", len(live))
	}
	if got := live[intentMsgID]; !got.Equal(expiry) {
		t.Errorf("expiry = %s, want %s", got, expiry)
	}
}

// TestLiveWipeTombstonesSkipsExpired pins that the loader hands back only
// refusals that still apply — a stale one would suppress a legitimate
// re-delivery months later.
func TestLiveWipeTombstonesSkipsExpired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()

	if err := store.NoteWipeTombstones(ctx, []domain.MessageID{intentMsgID}, now.Add(-time.Minute)); err != nil {
		t.Fatalf("NoteWipeTombstones (expired): %v", err)
	}
	if err := store.NoteWipeTombstones(ctx, []domain.MessageID{intentMsgB}, now.Add(time.Hour)); err != nil {
		t.Fatalf("NoteWipeTombstones (live): %v", err)
	}

	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	if _, ok := live[intentMsgID]; ok {
		t.Error("an expired tombstone was loaded")
	}
	if _, ok := live[intentMsgB]; !ok {
		t.Error("a live tombstone was not loaded")
	}

	reaped, err := store.ReapWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("ReapWipeTombstones: %v", err)
	}
	if reaped != 1 {
		t.Errorf("reaped = %d, want 1", reaped)
	}
}

// TestNoteWipeTombstonesExtendsAnExistingRefusal pins the conflict rule:
// a second wipe touching the same id must push the expiry out, not leave
// the first one's clock to end the refusal early.
func TestNoteWipeTombstonesExtendsAnExistingRefusal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()
	later := now.Add(2 * time.Hour).Truncate(time.Millisecond)

	for _, expiry := range []time.Time{now.Add(time.Minute), later} {
		if err := store.NoteWipeTombstones(ctx, []domain.MessageID{intentMsgID}, expiry); err != nil {
			t.Fatalf("NoteWipeTombstones: %v", err)
		}
	}

	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	if got := live[intentMsgID]; !got.Equal(later) {
		t.Errorf("expiry = %s, want the extended %s", got, later)
	}
}

// TestNoteWipeTombstonesBatchesALongThread pins that a wipe of a thread
// larger than one statement's worth of placeholders still records every
// id — a silently truncated batch would leave the tail resurrectable.
func TestNoteWipeTombstonesBatchesALongThread(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()

	const count = noteRefusalBatch*2 + 7
	ids := make([]domain.MessageID, 0, count)
	for i := range count {
		ids = append(ids, domain.MessageID(fmt.Sprintf("00000000-0000-4000-8000-%012d", i)))
	}

	if err := store.NoteWipeTombstones(ctx, ids, now.Add(time.Hour)); err != nil {
		t.Fatalf("NoteWipeTombstones: %v", err)
	}

	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	if len(live) != count {
		t.Fatalf("live tombstones = %d, want %d", len(live), count)
	}
}

// TestSettledRequestKeepsRefusing pins why the two facts share a row and
// still have to be independent: an ack means the peer owes nothing more,
// not that a stale copy of the message can come back. The request settles;
// the refusal stands until it expires.
func TestSettledRequestKeepsRefusing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	if _, err := store.DeleteWithIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}, now.Add(time.Hour)); err != nil {
		t.Fatalf("DeleteWithIntent: %v", err)
	}

	if settled, err := store.DropDeleteIntent(ctx, intentMsgID); err != nil || !settled {
		t.Fatalf("DropDeleteIntent: settled=%v err=%v", settled, err)
	}

	// Nothing is owed any more...
	if _, found, err := store.DeleteIntentByID(ctx, intentMsgID); err != nil || found {
		t.Fatalf("the request survived its ack: found=%v err=%v", found, err)
	}
	if counts, err := store.DeleteIntentCountsByPeer(ctx); err != nil || counts[peer] != 0 {
		t.Fatalf("counts = %v (err=%v), want the settled request uncounted", counts, err)
	}
	if due, err := store.DueDeleteIntents(ctx, now.Add(time.Hour), 16); err != nil || len(due) != 0 {
		t.Fatalf("due = %d rows (err=%v), want none: a settled request is not re-sent", len(due), err)
	}

	// ...but the id is still refused.
	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	if _, ok := live[intentMsgID]; !ok {
		t.Error("the ack lifted the refusal; a replayed copy would come straight back")
	}

	// And what is left says nothing about who the message was with: the
	// request is over, so its addressing is metadata the deletion was
	// supposed to remove.
	var (
		leftoverPeer string
		attempts     int
	)
	if err := store.db.QueryRowContext(ctx,
		`SELECT peer, attempts FROM message_delete_intents WHERE message_id = ?`,
		string(intentMsgID)).Scan(&leftoverPeer, &attempts); err != nil {
		t.Fatalf("read the settled row: %v", err)
	}
	if leftoverPeer != "" || attempts != 0 {
		t.Errorf("the settled row still names peer=%q attempts=%d", leftoverPeer, attempts)
	}

	// And the row goes once it refuses nothing either.
	reaped, err := store.ReapWipeTombstones(ctx, now.Add(2*time.Hour))
	if err != nil || reaped != 1 {
		t.Fatalf("ReapWipeTombstones = %d (err=%v), want 1", reaped, err)
	}
}

// TestRefusalWithoutARequestOwesNobody pins the other direction: a
// receiver asked to delete a message it has not received yet records a
// refusal alone. It must not become a request to anyone.
func TestRefusalWithoutARequestOwesNobody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()

	if err := store.NoteWipeTombstones(ctx, []domain.MessageID{intentMsgID}, now.Add(time.Hour)); err != nil {
		t.Fatalf("NoteWipeTombstones: %v", err)
	}

	if due, err := store.DueDeleteIntents(ctx, now.Add(time.Hour), 16); err != nil || len(due) != 0 {
		t.Fatalf("due = %d rows (err=%v), want none: a refusal asks nobody for anything", len(due), err)
	}
	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil {
		t.Fatalf("DeleteIntentCountsByPeer: %v", err)
	}
	for peer, n := range counts {
		t.Errorf("a refusal was counted as %d pending deletions for %s", n, peer)
	}
}
