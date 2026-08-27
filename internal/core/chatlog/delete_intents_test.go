package chatlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	intentSelf  = "1111111111111111111111111111111111111111"
	intentPeer  = "2222222222222222222222222222222222222222"
	intentMsgID = domain.MessageID("6c1f0a6e-6f1f-4a9d-9b7e-1c2d3e4f5a6b")
	intentMsgB  = domain.MessageID("7d2f1b7f-7f2f-4b8e-8c6d-2d3e4f5a6b7c")
)

func newIntentStore(t *testing.T) *Store {
	t.Helper()
	return storeFor(t, intentSelf)
}

// TestDeleteIntentSurvivesReopen is the reason the intent is on disk at all:
// a delete the user asked for while the peer was offline must still be owed
// to that peer after a restart.
func TestDeleteIntentSurvivesReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	identity := domain.PeerIdentityFromWire(intentSelf)
	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	store := newTestStoreAt(t, path, identity)
	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID:     intentMsgID,
		Peer:          domain.PeerIdentityFromWire(intentPeer),
		CreatedAt:     created,
		NextAttemptAt: created,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	reopened := newTestStoreAt(t, path, identity)
	intent, found, err := reopened.DeleteIntentByID(ctx, intentMsgID)
	if err != nil || !found {
		t.Fatalf("DeleteIntentByID after reopen: found=%v err=%v", found, err)
	}
	if intent.Peer != domain.PeerIdentityFromWire(intentPeer) {
		t.Errorf("peer = %s, want %s", intent.Peer, intentPeer)
	}
	if !intent.CreatedAt.Equal(created) {
		t.Errorf("created_at = %s, want %s", intent.CreatedAt, created)
	}
	if intent.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", intent.Attempts)
	}
}

// TestNoteDeleteIntentReArmsWithoutResettingTheDeadline pins the re-issue
// rule: the due time moves so the request goes out again promptly, while
// CreatedAt and the attempts already spent stay put — the give-up budget
// belongs to the request, and one a click can refill is one a user can
// make immortal without meaning to.
func TestNoteDeleteIntentReArmsWithoutResettingTheDeadline(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	first := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Millisecond)
	peer := domain.PeerIdentityFromWire(intentPeer)

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: first, NextAttemptAt: first.Add(time.Hour),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	if err := store.RecordDeleteIntentAttempt(ctx, intentMsgID, first.Add(2*time.Hour)); err != nil {
		t.Fatalf("RecordDeleteIntentAttempt: %v", err)
	}

	reissued := time.Now().UTC().Truncate(time.Millisecond)
	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: reissued, NextAttemptAt: reissued,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent (re-issue): %v", err)
	}

	intent, found, err := store.DeleteIntentByID(ctx, intentMsgID)
	if err != nil || !found {
		t.Fatalf("DeleteIntentByID: found=%v err=%v", found, err)
	}
	if !intent.CreatedAt.Equal(first) {
		t.Errorf("created_at = %s, want the original %s", intent.CreatedAt, first)
	}
	if !intent.NextAttemptAt.Equal(reissued) {
		t.Errorf("next_attempt_at = %s, want the re-issued %s", intent.NextAttemptAt, reissued)
	}
	if intent.Attempts != 1 {
		t.Errorf("attempts = %d, want the spent budget kept: a re-issue must not buy a fresh one", intent.Attempts)
	}
}

// TestDueDeleteIntentsOrdersByDueTimeAndRespectsTheLimit pins what the
// scheduler sweep sees: only intents that are actually due, oldest first,
// and never more than it asked for.
func TestDueDeleteIntentsOrdersByDueTimeAndRespectsTheLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()
	peer := domain.PeerIdentityFromWire(intentPeer)

	notDue := domain.MessageID("8e3f2c8f-8f3f-4c9f-9d7e-3e4f5a6b7c8d")
	seed := []struct {
		id  domain.MessageID
		due time.Time
	}{
		{intentMsgB, now.Add(-time.Minute)},
		{intentMsgID, now.Add(-time.Hour)},
		{notDue, now.Add(time.Hour)},
	}
	for _, row := range seed {
		if err := store.NoteDeleteIntent(ctx, DeleteIntent{
			MessageID: row.id, Peer: peer, CreatedAt: now, NextAttemptAt: row.due,
		}); err != nil {
			t.Fatalf("NoteDeleteIntent %s: %v", row.id, err)
		}
	}

	due, err := store.DueDeleteIntents(ctx, now, 10)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("due count = %d, want 2 (the future one must not be swept)", len(due))
	}
	if due[0].MessageID != intentMsgID {
		t.Errorf("first due = %s, want the oldest %s", due[0].MessageID, intentMsgID)
	}

	limited, err := store.DueDeleteIntents(ctx, now, 1)
	if err != nil {
		t.Fatalf("DueDeleteIntents(limit=1): %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("limited count = %d, want 1", len(limited))
	}

	none, err := store.DueDeleteIntents(ctx, now, 0)
	if err != nil {
		t.Fatalf("DueDeleteIntents(limit=0): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("limit 0 returned %d rows; an unbounded sweep must not be reachable by accident", len(none))
	}
}

// TestDropDeleteIntentIsIdempotent — the ack path and the give-up path can
// both reach the same intent; the second one must be a quiet no-op.
func TestDropDeleteIntentIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: domain.PeerIdentityFromWire(intentPeer), CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	dropped, err := store.DropDeleteIntent(ctx, intentMsgID)
	if err != nil || !dropped {
		t.Fatalf("first drop: dropped=%v err=%v", dropped, err)
	}
	dropped, err = store.DropDeleteIntent(ctx, intentMsgID)
	if err != nil {
		t.Fatalf("second drop: %v", err)
	}
	if dropped {
		t.Error("second drop reported a removal; the row was already gone")
	}

	if _, found, err := store.DeleteIntentByID(ctx, intentMsgID); err != nil || found {
		t.Fatalf("DeleteIntentByID after drop: found=%v err=%v", found, err)
	}
	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil || len(counts) != 0 {
		t.Fatalf("delete intents left = %v (err=%v), want none", counts, err)
	}
}

// TestNoteDeleteIntentKeepsTheOriginalPeer pins the one rewrite the upsert
// must never do. A message id belongs to exactly one conversation, so a
// second intent naming a different peer is a caller bug — and honouring it
// would re-point a pending deletion at somebody who was never part of it.
func TestNoteDeleteIntentKeepsTheOriginalPeer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()
	original := domain.PeerIdentityFromWire(intentPeer)
	stranger := domain.PeerIdentityFromWire("4444444444444444444444444444444444444444")

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: original, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: stranger, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent (stranger): %v", err)
	}

	intent, found, err := store.DeleteIntentByID(ctx, intentMsgID)
	if err != nil || !found {
		t.Fatalf("DeleteIntentByID: found=%v err=%v", found, err)
	}
	if intent.Peer != original {
		t.Errorf("peer = %s, want the original %s", intent.Peer, original)
	}
}

// TestDeleteWithIntentCommitsBothHalves pins the atomicity the table exists
// for: the row and the intent land together, so no crash window can leave a
// destroyed local copy that nobody will ever ask the peer about.
func TestDeleteWithIntentCommitsBothHalves(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	identity := domain.PeerIdentityFromWire(intentSelf)
	store := newTestStoreAt(t, path, identity)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	if _, err := store.AppendReportNew(ctx, "dm", peer, Entry{
		ID:        string(intentMsgID),
		Sender:    identity.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("AppendReportNew: %v", err)
	}

	removed, err := store.DeleteWithIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	})
	if err != nil || !removed {
		t.Fatalf("DeleteWithIntent: removed=%v err=%v", removed, err)
	}

	reopened := newTestStoreAt(t, path, identity)
	if _, found, err := reopened.EntryByID(ctx, intentMsgID); err != nil || found {
		t.Fatalf("row survived the committed delete: found=%v err=%v", found, err)
	}
	if _, found, err := reopened.DeleteIntentByID(ctx, intentMsgID); err != nil || !found {
		t.Fatalf("intent missing after the committed delete: found=%v err=%v", found, err)
	}
}

// TestDeleteWithIntentRollsBackOnABadIntent pins the other direction: if the
// intent cannot be written, the message must still be there. A half-applied
// deletion is the one outcome with no way back.
func TestDeleteWithIntentRollsBackOnABadIntent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	self := domain.PeerIdentityFromWire(intentSelf)
	now := time.Now().UTC()

	if _, err := store.AppendReportNew(ctx, "dm", peer, Entry{
		ID:        string(intentMsgID),
		Sender:    self.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("AppendReportNew: %v", err)
	}

	// A zero peer cannot be scheduled, so the intent write fails.
	if _, err := store.DeleteWithIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, CreatedAt: now, NextAttemptAt: now,
	}); err == nil {
		t.Fatal("DeleteWithIntent accepted an unschedulable intent")
	}

	if _, found, err := store.EntryByID(ctx, intentMsgID); err != nil || !found {
		t.Fatalf("message lost to a rolled-back delete: found=%v err=%v", found, err)
	}
}

// TestReviveAndHoldDeleteIntents pins the pair the sweep uses to stay fair:
// parking an intent whose peer cannot answer, and pulling it back the moment
// they can. Neither touches the attempt count — an offline peer is not a
// failed delivery.
func TestReviveAndHoldDeleteIntents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	held := now.Add(5 * time.Minute)
	if err := store.HoldDeleteIntents(ctx, []domain.MessageID{intentMsgID}, held); err != nil {
		t.Fatalf("HoldDeleteIntent: %v", err)
	}
	due, err := store.DueDeleteIntents(ctx, now, 10)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("held intent still due: %d rows — it would keep the head of the queue", len(due))
	}

	revived, err := store.ReviveDeleteIntentsForPeer(ctx, peer, now)
	if err != nil {
		t.Fatalf("ReviveDeleteIntentsForPeer: %v", err)
	}
	if revived != 1 {
		t.Fatalf("revived = %d, want 1", revived)
	}
	due, err = store.DueDeleteIntents(ctx, now, 10)
	if err != nil {
		t.Fatalf("DueDeleteIntents after revive: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("revived intent not due: %d rows", len(due))
	}
	if due[0].Attempts != 0 {
		t.Errorf("attempts = %d, want 0 (holding and reviving are not delivery attempts)", due[0].Attempts)
	}
}

// TestAWipeDoesNotDisarmTheRefusalsThatSurviveARestart is the half that a count
// of rows does not see.
//
// A per-message request is the only thing on this disk that still names a
// deleted id, and it is what a fresh process reads to know which late copies to
// turn away (service/wipe_tombstone_set.go hydrates from exactly this list).
// The in-memory window does not survive a restart and the wipe request names no
// ids, so if the wipe took those rows with it, the sequence "delete a message
// while the peer is away, clear the chat, restart" would leave a relay's held
// copy free to walk back in.
func TestAWipeDoesNotDisarmTheRefusalsThatSurviveARestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("ConversationCandidateIDs: %v", err)
	}
	if _, err := store.DeleteConversationWithIntent(ctx, peer, scope, conversationIntentFor(peer, now)); err != nil {
		t.Fatalf("DeleteConversationWithIntent: %v", err)
	}

	// What the next process would load.
	owed, err := store.OwedDeleteIntentMessageIDs(ctx)
	if err != nil {
		t.Fatalf("OwedDeleteIntentMessageIDs: %v", err)
	}
	found := false
	for _, id := range owed {
		if id == intentMsgID {
			found = true
		}
	}
	if !found {
		t.Fatalf("after the wipe a restart would refuse %v, and the deleted message is not among them", owed)
	}
}

// TestTheWipeCarriesWhatItAsksForWhateverTheStampsSortLike pins that the link
// between a wipe and the requests it carries is the request id, not the order
// of two timestamps.
//
// The stamps are RFC3339Nano text with a variable-length fraction, and SQLite
// compares them as TEXT: ".1Z" sorts AFTER ".11Z", although a tenth of a second
// is earlier than eleven hundredths. A boundary built on them would send a
// carried request on its own — the exact case here — and, in the mirror case,
// swallow a deletion the user asked for after clearing the chat.
func TestTheWipeCarriesWhatItAsksForWhateverTheStampsSortLike(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)

	base := time.Date(2026, 3, 4, 5, 6, 0, 0, time.UTC)
	askedAt := base.Add(100 * time.Millisecond) // renders as ...00.1Z
	wipedAt := base.Add(110 * time.Millisecond) // renders as ...00.11Z, sorts BEFORE it
	if got, want := askedAt.Format(time.RFC3339Nano) > wipedAt.Format(time.RFC3339Nano), true; got != want {
		t.Fatalf("the fixture no longer reproduces the text ordering trap: %q vs %q",
			askedAt.Format(time.RFC3339Nano), wipedAt.Format(time.RFC3339Nano))
	}

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: askedAt, NextAttemptAt: askedAt,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	wipe := conversationIntentFor(peer, wipedAt)
	if err := store.NoteConversationDeleteIntent(ctx, wipe); err != nil {
		t.Fatalf("NoteConversationDeleteIntent: %v", err)
	}

	due, err := store.DueDeleteIntents(ctx, wipedAt.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	for _, intent := range due {
		if intent.Kind == DeleteIntentMessage {
			t.Error("a request the wipe carries was dispatched on its own")
		}
	}

	settled, err := store.DropConversationDeleteIntent(ctx, peer, wipe.RequestID)
	if err != nil || !settled {
		t.Fatalf("DropConversationDeleteIntent: settled=%v err=%v", settled, err)
	}
	if _, found, err := store.DeleteIntentByID(ctx, intentMsgID); err != nil || found {
		t.Errorf("the answered wipe left the request it carried behind: found=%v err=%v", found, err)
	}
}

// TestConversationWipeKeepsThePeersPerMessageIntents — a wipe does not write
// off the per-message requests of the same thread, although it asks the same
// peer for everything they ask for.
//
// Two reasons, and neither is "they are harmless". A request stands until the
// peer answers it, and the wipe has not been delivered either — dropping one on
// the strength of the other writes off a deletion the user asked for. And a
// per-message request is what refuses a late re-delivery of its id after a
// restart, because the row names the message; the wipe names nothing and can
// refuse nothing. Delete an incoming message while the peer is away, clear the
// chat, restart, and a copy still held by a relay would be taken back in.
//
// The overlap costs little and clears itself: once the peer applies the wipe,
// the per-message request is answered not_found, which retires it.
//
// Another peer's requests are untouched: a wipe is one conversation.
func TestConversationWipeKeepsThePeersPerMessageIntents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	other := domain.PeerIdentityFromWire("5555555555555555555555555555555555555555")
	now := time.Now().UTC()

	for id, p := range map[domain.MessageID]domain.PeerIdentity{intentMsgID: peer, intentMsgB: other} {
		if err := store.NoteDeleteIntent(ctx, DeleteIntent{
			MessageID: id, Peer: p, CreatedAt: now, NextAttemptAt: now,
		}); err != nil {
			t.Fatalf("NoteDeleteIntent %s: %v", id, err)
		}
	}

	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("ConversationCandidateIDs: %v", err)
	}
	if _, err := store.DeleteConversationWithIntent(ctx, peer, scope, conversationIntentFor(peer, now)); err != nil {
		t.Fatalf("DeleteConversationWithIntent: %v", err)
	}

	// The row is still there — it is what refuses a late re-delivery of that id
	// after a restart.
	if _, found, err := store.DeleteIntentByID(ctx, intentMsgID); err != nil || !found {
		t.Fatalf("the wipe wrote off a per-message request the peer has not answered: found=%v err=%v", found, err)
	}
	// But it is no longer something the user is waiting for separately: the
	// wipe speaks for it, and "N messages waiting" on a chat the user has just
	// cleared is the same false alarm as a late `denied`.
	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil {
		t.Fatalf("DeleteIntentCountsByPeer: %v", err)
	}
	if counts[peer].Messages != 0 {
		t.Errorf("the cleared chat still counts %d message(s) waiting to be deleted", counts[peer].Messages)
	}
	if !counts[peer].Conversation {
		t.Error("the wipe left no conversation request behind")
	}
	if counts[other].Messages != 1 || counts[other].Conversation {
		t.Errorf("other peer's pending = %+v, want one message and no wipe (a wipe must not touch another conversation)", counts[other])
	}
}

// TestTheWipeCarriesTheRequestsItCoversAndRetiresThemWithItself walks the whole
// life of a per-message request that a wipe swallowed: it is not sent on its
// own, it survives as the refusal of its id, and it goes when the wipe it was
// riding on is answered — while a deletion asked for AFTER the wipe keeps its
// own request.
func TestTheWipeCarriesTheRequestsItCoversAndRetiresThemWithItself(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	// Asked for before the wipe.
	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now.Add(-time.Hour), NextAttemptAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent (before the wipe): %v", err)
	}
	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("ConversationCandidateIDs: %v", err)
	}
	wipe := conversationIntentFor(peer, now)
	if _, err := store.DeleteConversationWithIntent(ctx, peer, scope, wipe); err != nil {
		t.Fatalf("DeleteConversationWithIntent: %v", err)
	}
	// And one asked for after it — a message that arrived once the peer had
	// already been asked to erase everything.
	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgB, Peer: peer, CreatedAt: now.Add(time.Hour), NextAttemptAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent (after the wipe): %v", err)
	}

	// The sweep sends the wipe and the later request, and NOT the one the wipe
	// carries: two questions about the same rows give the peer two chances to
	// refuse one of them.
	due, err := store.DueDeleteIntents(ctx, now.Add(2*time.Hour), 10)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	dispatched := make(map[domain.MessageID]bool, len(due))
	wipes := 0
	for _, intent := range due {
		if intent.Kind == DeleteIntentConversation {
			wipes++
			continue
		}
		dispatched[intent.MessageID] = true
	}
	if wipes != 1 {
		t.Errorf("the sweep found %d wipes, want 1", wipes)
	}
	if dispatched[intentMsgID] {
		t.Error("the request the wipe carries was dispatched on its own as well")
	}
	if !dispatched[intentMsgB] {
		t.Error("a deletion asked for after the wipe was swallowed by it")
	}

	// The peer answers the wipe: what it carried goes with it, in one step.
	settled, err := store.DropConversationDeleteIntent(ctx, peer, wipe.RequestID)
	if err != nil || !settled {
		t.Fatalf("DropConversationDeleteIntent: settled=%v err=%v", settled, err)
	}
	if _, found, err := store.DeleteIntentByID(ctx, intentMsgID); err != nil || found {
		t.Errorf("the answered wipe left the request it carried behind: found=%v err=%v", found, err)
	}
	if _, found, err := store.DeleteIntentByID(ctx, intentMsgB); err != nil || !found {
		t.Errorf("the answered wipe took a deletion it never asked about: found=%v err=%v", found, err)
	}
	owed, err := store.OwedDeleteIntentMessageIDs(ctx)
	if err != nil {
		t.Fatalf("OwedDeleteIntentMessageIDs: %v", err)
	}
	for _, id := range owed {
		if id == intentMsgID {
			t.Error("the id of the settled deletion is still on disk after the wipe was answered")
		}
	}
}

// TestNoteDeleteIntentRejectsIncompleteAddressing — an intent that cannot
// name both the message and the peer is not schedulable, and storing it
// would leave a row the sweep can never resolve.
func TestNoteDeleteIntentRejectsIncompleteAddressing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	now := time.Now().UTC()

	err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: "not-a-uuid", Peer: domain.PeerIdentityFromWire(intentPeer), CreatedAt: now, NextAttemptAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "message id") {
		t.Errorf("malformed id: err = %v, want a message-id rejection", err)
	}

	err = store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, CreatedAt: now, NextAttemptAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "peer") {
		t.Errorf("zero peer: err = %v, want a peer rejection", err)
	}

	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil || len(counts) != 0 {
		t.Fatalf("delete intents left = %v (err=%v), want none", counts, err)
	}
}

// TestRecordDeleteIntentAttemptOnMissingRowIsQuiet — the ack can retire an
// intent between the sweep reading it and the dispatch being charged.
func TestRecordDeleteIntentAttemptOnMissingRowIsQuiet(t *testing.T) {
	t.Parallel()

	store := newIntentStore(t)
	if err := store.RecordDeleteIntentAttempt(context.Background(), intentMsgID, time.Now().UTC()); err != nil {
		t.Fatalf("RecordDeleteIntentAttempt on a missing row: %v", err)
	}
}

// TestReviveLeavesAnHonestBackoffAlone pins the line between the two
// reasons an intent is not due: a park, which loses its reason the moment
// the peer is back, and the backoff of an attempt that actually went out,
// which does not. Without the distinction a peer whose application is
// dead but whose transport reconnects every few seconds would be asked
// again on every handshake.
func TestReviveLeavesAnHonestBackoffAlone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(ctx, DeleteIntent{
		MessageID: intentMsgID, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	// One real dispatch, then its backoff.
	backoffUntil := now.Add(30 * time.Second)
	if err := store.RecordDeleteIntentAttempt(ctx, intentMsgID, backoffUntil); err != nil {
		t.Fatalf("RecordDeleteIntentAttempt: %v", err)
	}

	revived, err := store.ReviveDeleteIntentsForPeer(ctx, peer, now)
	if err != nil {
		t.Fatalf("ReviveDeleteIntentsForPeer: %v", err)
	}
	if revived != 0 {
		t.Fatalf("revived = %d, want 0 — a backoff is not a park", revived)
	}

	intent, found, err := store.DeleteIntentByID(ctx, intentMsgID)
	if err != nil || !found {
		t.Fatalf("DeleteIntentByID: found=%v err=%v", found, err)
	}
	if !intent.NextAttemptAt.Equal(backoffUntil) {
		t.Errorf("next_attempt_at = %s, want the backoff %s left intact", intent.NextAttemptAt, backoffUntil)
	}
	if intent.Hold != HoldNone {
		t.Error("an intent that was dispatched is marked parked")
	}
}

// TestHoldDeleteIntentsParksABatchWithoutCharging pins the cost of an
// absent contact. The parked set has no upper bound in time — a request
// to somebody who never returns is kept indefinitely — so a park that
// cost one write per row would be a floor the sweep pays every tick for
// the life of the install.
func TestHoldDeleteIntentsParksABatchWithoutCharging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	ids := []domain.MessageID{intentMsgID, intentMsgB}
	for _, id := range ids {
		if err := store.NoteDeleteIntent(ctx, DeleteIntent{
			MessageID: id, Peer: peer, CreatedAt: now, NextAttemptAt: now.Add(-time.Minute),
		}); err != nil {
			t.Fatalf("NoteDeleteIntent %s: %v", id, err)
		}
	}

	until := now.Add(5 * time.Minute).Truncate(time.Millisecond)
	if err := store.HoldDeleteIntents(ctx, ids, until); err != nil {
		t.Fatalf("HoldDeleteIntents: %v", err)
	}

	for _, id := range ids {
		intent, found, err := store.DeleteIntentByID(ctx, id)
		if err != nil || !found {
			t.Fatalf("DeleteIntentByID(%s): found=%v err=%v", id, found, err)
		}
		if intent.Hold != HoldPeerAbsent {
			t.Errorf("%s was not parked", id)
		}
		if intent.Attempts != 0 {
			t.Errorf("%s attempts = %d, want 0: a park is not an attempt", id, intent.Attempts)
		}
		if !intent.NextAttemptAt.Equal(until) {
			t.Errorf("%s next attempt = %s, want %s", id, intent.NextAttemptAt, until)
		}
	}

	if due, err := store.DueDeleteIntents(ctx, now, 16); err != nil || len(due) != 0 {
		t.Fatalf("DueDeleteIntents after parking = %d rows (err=%v), want none", len(due), err)
	}
}

// TestConversationWipeAsksForTheThreadAndNamesNoMessage is what replaced the
// per-message privacy rule: a wipe used to write one request per id and had to
// decide, row by row and inside the transaction, which of those ids the peer
// could possibly hold — a message that never reached the wire must not be
// named, because the request would be how they learn it existed.
//
// One request about the CONVERSATION settles that question by not asking it.
// It carries no ids at all, so there is nothing to leak and nothing to
// classify, and the same row is what a peer owes for a thread of one message
// or of ten thousand.
func TestConversationWipeAsksForTheThreadAndNamesNoMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	appendOutgoing(t, store, string(intentMsgID), "")
	appendOutgoing(t, store, string(intentMsgB), "")
	// A message this node proved never went out. Under the old model it was
	// the one id the peer must not be told about; now no id travels at all.
	if err := store.MarkNeverEmitted(ctx, []domain.MessageID{intentMsgB}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("ConversationCandidateIDs: %v", err)
	}
	wiped, err := store.DeleteConversationWithIntent(ctx, peer, scope, conversationIntentFor(peer, now))
	if err != nil {
		t.Fatalf("DeleteConversationWithIntent: %v", err)
	}
	if len(wiped.Removed) != 2 {
		t.Fatalf("removed %d rows, want both", len(wiped.Removed))
	}

	// Nothing is owed per message — not for the message the peer holds and
	// not for the one they never saw.
	for _, id := range []domain.MessageID{intentMsgID, intentMsgB} {
		if _, found, err := store.DeleteIntentByID(ctx, id); err != nil || found {
			t.Errorf("a per-message request was written for %s: found=%v err=%v", id, found, err)
		}
	}

	intent, found, err := store.ConversationDeleteIntentForPeer(ctx, peer)
	if err != nil || !found {
		t.Fatalf("ConversationDeleteIntentForPeer: found=%v err=%v", found, err)
	}
	if intent.MessageID != "" {
		t.Errorf("the wipe request names message %s; it must name none", intent.MessageID)
	}
	if intent.Kind != DeleteIntentConversation {
		t.Errorf("kind = %q, want %q", intent.Kind, DeleteIntentConversation)
	}
	if intent.Hold != HoldNone {
		t.Errorf("hold = %d, want the request due immediately", intent.Hold)
	}

	// And neither id is written down anywhere. A wipe names no messages: not
	// to the peer, and not to this disk. What refuses a replay of one of them
	// is the in-memory window the router keeps (service/wipe_tombstone_set.go),
	// which is the price of the request carrying no ids at all.
	for _, id := range []domain.MessageID{intentMsgID, intentMsgB} {
		if found := tablesNaming(t, store, string(id)); len(found) > 0 {
			t.Errorf("after the wipe, %s is still named in %v", id, found)
		}
	}

	due, err := store.DueDeleteIntents(ctx, now, 16)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("due = %d requests, want exactly the one wipe: %+v", len(due), due)
	}
	if due[0].Kind != DeleteIntentConversation || due[0].Peer != peer {
		t.Errorf("due request = %+v, want the conversation wipe for %s", due[0], peer)
	}
}

// conversationIntentFor is the request a wipe leaves behind, for tests that
// care about the rows rather than about who minted the id.
func conversationIntentFor(peer domain.PeerIdentity, now time.Time) DeleteIntent {
	return DeleteIntent{
		Kind:          DeleteIntentConversation,
		Peer:          peer,
		RequestID:     domain.ConversationDeleteRequestID("3f8b1c2d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"),
		CreatedAt:     now,
		NextAttemptAt: now,
	}
}

// TestConversationRequestWritesAreScopedToTheirRequestID is the race the
// scheduler cannot win by checking first and writing after.
//
// Between reading the pending request and acting on it, the user can clear the
// chat again — a new gesture, a new id, a fresh attempt budget. Every write
// that follows an old answer therefore carries the id it answers, so the
// database refuses it rather than retiring, charging or writing off the wipe
// that has not been sent once.
func TestConversationRequestWritesAreScopedToTheirRequestID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	stale := domain.ConversationDeleteRequestID("11111111-1111-4111-8111-111111111111")
	current := domain.ConversationDeleteRequestID("22222222-2222-4222-8222-222222222222")

	first := conversationIntentFor(peer, now)
	first.RequestID = stale
	if err := store.NoteConversationDeleteIntent(ctx, first); err != nil {
		t.Fatalf("NoteConversationDeleteIntent: %v", err)
	}
	// The user clears the chat again: same peer, new request.
	second := conversationIntentFor(peer, now)
	second.RequestID = current
	if err := store.NoteConversationDeleteIntent(ctx, second); err != nil {
		t.Fatalf("NoteConversationDeleteIntent (re-issue): %v", err)
	}

	// An answer to the wipe that has been replaced settles nothing.
	dropped, err := store.DropConversationDeleteIntent(ctx, peer, stale)
	if err != nil {
		t.Fatalf("DropConversationDeleteIntent: %v", err)
	}
	if dropped {
		t.Error("an answer to a superseded wipe retired the current request; the user would be told both sides are cleared while the peer was never asked")
	}
	intent, found, err := store.ConversationDeleteIntentForPeer(ctx, peer)
	if err != nil || !found {
		t.Fatalf("ConversationDeleteIntentForPeer: found=%v err=%v", found, err)
	}
	if intent.RequestID != current {
		t.Fatalf("request id = %s, want the current %s", intent.RequestID, current)
	}

	// A dispatch made under the old request charges nothing to the new one:
	// its budget belongs to it, and its backoff describes attempts that were
	// actually made for it.
	if err := store.RecordConversationDeleteAttempt(ctx, peer, stale, now.Add(time.Hour)); err != nil {
		t.Fatalf("RecordConversationDeleteAttempt: %v", err)
	}
	intent, _, err = store.ConversationDeleteIntentForPeer(ctx, peer)
	if err != nil {
		t.Fatalf("ConversationDeleteIntentForPeer: %v", err)
	}
	if intent.Attempts != 0 {
		t.Errorf("attempts = %d, want 0: the attempt belonged to the request that was replaced", intent.Attempts)
	}

	// And the current request settles on its own answer.
	dropped, err = store.DropConversationDeleteIntent(ctx, peer, current)
	if err != nil || !dropped {
		t.Fatalf("dropping the current request: dropped=%v err=%v", dropped, err)
	}
}
