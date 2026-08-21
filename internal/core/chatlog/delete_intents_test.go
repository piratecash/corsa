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
	}, now.Add(time.Hour))
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
	}, now.Add(time.Hour)); err == nil {
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

// TestConversationWipeKeepsThePeersPerMessageIntents — a wipe covers the
// thread up to its bound, and a per-message request may name a message
// PAST it: delete the last message while the peer is away, then wipe the
// older thread, and the bound stops short of the one already requested.
// Dropping those requests as "subsumed" would silently discard a deletion
// the user asked for. The overlap is harmless the other way: a request for
// a row the peer has already wiped is answered not_found, which retires it.
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
	if _, err := store.DeleteConversationWithIntents(ctx, peer, scope, ConversationWipeClassification{Trusted: true}, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("DeleteConversationWithIntents: %v", err)
	}

	counts, err := store.DeleteIntentCountsByPeer(ctx)
	if err != nil {
		t.Fatalf("DeleteIntentCountsByPeer: %v", err)
	}
	if counts[peer] != 1 {
		t.Errorf("the wipe discarded a per-message deletion the user asked for: peer owes %d, want 1", counts[peer])
	}
	if counts[other] != 1 {
		t.Errorf("other peer's intent = %d, want 1 (a wipe must not touch another conversation)", counts[other])
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

// TestConversationWipeAsksOnlyAboutMessagesThePeerMayHold is the privacy
// rule, decided where it can be decided atomically: the row carries the
// proof that the message never went out, and the transaction that destroys
// the row is the last moment anything can read it. A request for such a
// message is never written — not written-and-parked, which is a rule with
// a timeout on it.
func TestConversationWipeAsksOnlyAboutMessagesThePeerMayHold(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newIntentStore(t)
	peer := domain.PeerIdentityFromWire(intentPeer)
	now := time.Now().UTC()

	appendOutgoing(t, store, string(intentMsgID), "")
	appendOutgoing(t, store, string(intentMsgB), "")
	if err := store.MarkNeverEmitted(ctx, []domain.MessageID{intentMsgB}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("ConversationCandidateIDs: %v", err)
	}
	wiped, err := store.DeleteConversationWithIntents(ctx, peer, scope, ConversationWipeClassification{Trusted: true}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("DeleteConversationWithIntents: %v", err)
	}

	if len(wiped.Removed) != 2 {
		t.Fatalf("removed %d rows, want both", len(wiped.Removed))
	}
	if wiped.Owed != 1 {
		t.Errorf("owed = %d, want 1: only the message the peer may hold", wiped.Owed)
	}
	if len(wiped.Recalled) != 1 || wiped.Recalled[0] != intentMsgB {
		t.Errorf("recalled = %v, want [%s]", wiped.Recalled, intentMsgB)
	}

	if _, found, err := store.DeleteIntentByID(ctx, intentMsgB); err != nil || found {
		t.Errorf("a request was written for a message that never went out: found=%v err=%v", found, err)
	}
	intent, found, err := store.DeleteIntentByID(ctx, intentMsgID)
	if err != nil || !found {
		t.Fatalf("DeleteIntentByID: found=%v err=%v", found, err)
	}
	if intent.Hold != HoldNone {
		t.Errorf("hold = %d, want the request due immediately", intent.Hold)
	}

	// Both ids are still refused — the classification decides who is
	// ASKED, never whether a replay may re-create the row.
	live, err := store.LiveWipeTombstones(ctx, now)
	if err != nil {
		t.Fatalf("LiveWipeTombstones: %v", err)
	}
	for _, id := range []domain.MessageID{intentMsgID, intentMsgB} {
		if _, refused := live[id]; !refused {
			t.Errorf("%s is not refused after the wipe", id)
		}
	}

	due, err := store.DueDeleteIntents(ctx, now, 16)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	for _, intent := range due {
		if intent.MessageID == intentMsgB {
			t.Fatal("a request for a message the peer never saw became due")
		}
	}
}
