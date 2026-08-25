package chatlog

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Deleting a message that carried reactions has to leave something behind that
// says so, and it has to outlive the deletion tombstone: the peer whose
// reaction it was re-offers that fact for as long as it holds it, while the
// tombstone expires on the message clock within the week.
func TestDeletingAMessageThatHadReactionsRefusesItsIdForGood(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	seedReactionMessages(t, store, self, peer, "reacted", "bare")
	if _, err := store.ApplyReactionFact(ctx,
		reactionFact("reacted", "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("apply: %v", err)
	}

	for _, id := range []domain.MessageID{"reacted", "bare"} {
		if _, err := store.DeleteMessageWithTombstone(ctx, id, now.Add(time.Hour)); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
	}

	if !refusedFor(t, store, peer, "reacted", now) {
		t.Fatal("a deleted message that had reactions did not refuse them: the next offer puts them back")
	}

	// And NOT for every deletion. Messages also go on their own timer, so a row
	// per deleted id would be a second copy of the message table that never
	// shrinks — the table is bounded by count, and rows nobody needs spend that
	// budget on ids nobody will ever offer a reaction for. The sweep records the
	// ones that turn out to matter.
	if refusedFor(t, store, peer, "bare", now) {
		t.Fatal("a deletion with no reactions to refuse still spent a row")
	}
}

// The sweep is the writer that needs no foresight, and it is what closes the
// case nothing at delete time could see: a reaction first offered long after
// the message went, when no tombstone is left to answer for it. The fact is
// held once — from here it is indistinguishable from one whose message is
// merely late — and the sweep turns "waited a whole window for nothing" into
// the answer for every offer after it.
func TestSweepingAFactThatWaitedRefusesItsIdFromThenOn(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	// A conversation exists (that is what allows a fact to wait at all), but the
	// message this fact names does not.
	seedReactionMessages(t, store, self, peer, "present")
	if held, err := store.HoldReactionFact(ctx,
		reactionFact("missing", "👍", peer, domain.ReactionSet, 1), now); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}
	// And one whose message IS here, standing in for a release that lost the
	// race with the sweep. Refusing this id would hide a reaction on a message
	// the user can see — which is why the write is guarded by the message's
	// absence rather than by the sweep's good intentions.
	if held, err := store.HoldReactionFact(ctx,
		reactionFact("present", "🔥", peer, domain.ReactionSet, 1), now); err != nil || !held {
		t.Fatalf("hold the raced one: held=%v err=%v", held, err)
	}

	swept, err := store.SweepHeldReactions(ctx, now.Add(2*HeldReactionTTL))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 2 {
		t.Fatalf("the sweep took %d facts, want both", swept)
	}

	if !refusedFor(t, store, peer, "missing", now) {
		t.Fatal("a fact that waited out its window left nothing behind: the next offer holds it again")
	}
	if refusedFor(t, store, peer, "present", now) {
		t.Fatal("an id whose message is here was refused: its reactions can never appear")
	}
}

// A refusal says "the message is not here and did not come", so the message
// coming is what ends it. Late is normal on this transport — a re-delivery,
// a reseed days afterwards — and the author is still offering the fact.
func TestStoringTheMessageLiftsTheRefusalOfItsId(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.RefuseReactionsFor(ctx, scopeOf(peer), "late", now); err != nil {
		t.Fatalf("refuse: %v", err)
	}
	seedReactionMessages(t, store, self, peer, "late")

	if refusedFor(t, store, peer, "late", now) {
		t.Fatal("the message arrived and its id is still refused: the reaction can never appear")
	}
}

// Message ids are chosen by their sender, so two conversations can hold the
// same one — and the id is not what a refusal is about. A message arriving from
// B must leave A's refusal exactly where it was, and the sweep must still be
// able to record one for A while B's copy of the id sits in `messages`.
//
// Both halves are the same bug seen twice: lifting id-wide cleared a refusal
// nothing could record again, because the guard asked only whether the id
// existed anywhere.
func TestARefusalSurvivesTheSameIdArrivingInAnotherConversation(t *testing.T) {
	self := reactionPeer(t, "11")
	deleted := reactionPeer(t, "aa")
	other := reactionPeer(t, "bb")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	// The id is deleted in one conversation...
	seedReactionMessages(t, store, self, deleted, "shared")
	if _, err := store.ApplyReactionFact(ctx,
		reactionFact("shared", "👍", deleted, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := store.DeleteMessageWithTombstone(ctx, "shared", now.Add(time.Hour)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !refusedFor(t, store, deleted, "shared", now) {
		t.Fatal("the fixture proves nothing: the deletion recorded no refusal")
	}

	// ...and then somebody else sends a message under it.
	if err := store.Append(ctx, "dm", self, Entry{
		ID: "shared", Sender: other.String(), Recipient: self.String(),
		Body: "ciphertext", CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("the other conversation's message: %v", err)
	}
	if !refusedFor(t, store, deleted, "shared", now) {
		t.Fatal("a stranger's message with the same id lifted the refusal of a deleted one")
	}

	// And the sweep can still record one, with that id present in `messages`.
	if err := store.DropReactionRefusalsForScope(ctx, scopeOf(deleted)); err != nil {
		t.Fatalf("clear the refusal to test the recording path: %v", err)
	}
	if held, err := store.HoldReactionFact(ctx,
		reactionFact("shared", "🔥", deleted, domain.ReactionSet, 2), now); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}
	if _, err := store.SweepHeldReactions(ctx, now.Add(2*HeldReactionTTL)); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if !refusedFor(t, store, deleted, "shared", now) {
		t.Fatal("the sweep could not record a refusal because the id exists in another conversation")
	}
}

// The trim is what bounds a table with no expiry, and what it must keep is the
// ids somebody is still pushing at. That means the PRODUCTION read has to move
// `refused_at` — a test that touches the row by re-recording it would be
// pinning a write this path does not make.
func TestReadingARefusalKeepsItOutOfTheTrimsWay(t *testing.T) {
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	// Written oldest first, so an insertion-ordered trim and a touch-ordered
	// one would keep the same rows; the read below is what tells them apart.
	for i, id := range []domain.MessageID{"pushed", "quiet"} {
		if err := store.RefuseReactionsFor(ctx, scopeOf(peer), id, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("refuse %s: %v", id, err)
		}
	}
	// An offer arrives for the older one — the only thing this node ever sees of
	// a peer that still holds the fact.
	if !refusedFor(t, store, peer, "pushed", now.Add(2*refusalTouchFloor)) {
		t.Fatal("the refusal stopped answering")
	}

	trimmed, err := store.TrimReactionRefusals(ctx, 1)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if trimmed != 1 {
		t.Fatalf("the trim dropped %d ids, want the single one over the bound", trimmed)
	}
	if !refusedFor(t, store, peer, "pushed", now) {
		t.Fatal("the trim dropped an id that is still being offered at: the hold-and-sweep loop starts again")
	}
	if refusedFor(t, store, peer, "quiet", now) {
		t.Fatal("the trim kept the id nobody has mentioned and dropped the hot one")
	}
}

// The touch has a floor, because it runs on the arrival path of every refused
// fact and a batch of them would otherwise be a write each. What the floor
// costs is resolution the trim does not have: it measures in days.
func TestARepeatedOfferWithinTheFloorDoesNotWriteAgain(t *testing.T) {
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.RefuseReactionsFor(ctx, scopeOf(peer), "early", now); err != nil {
		t.Fatalf("refuse early: %v", err)
	}
	if err := store.RefuseReactionsFor(ctx, scopeOf(peer), "later", now.Add(time.Minute)); err != nil {
		t.Fatalf("refuse later: %v", err)
	}
	// Well inside the floor, so the order must not change.
	if !refusedFor(t, store, peer, "early", now.Add(refusalTouchFloor/2)) {
		t.Fatal("the refusal stopped answering")
	}

	if _, err := store.TrimReactionRefusals(ctx, 1); err != nil {
		t.Fatalf("trim: %v", err)
	}
	if refusedFor(t, store, peer, "early", now) {
		t.Fatal("a read inside the floor still wrote: every refused fact in a batch pays for a write")
	}
}

// A whole-conversation wipe is the case that does NOT need these rows: with no
// message left, an offer from that peer is refused at the door by the admission
// check. Keeping one row per erased id would spend a bounded table on ids
// nothing will ask about, and evict the ones a live conversation still needs.
func TestWipingAConversationForgetsItsRefusals(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	// A real UUID: the wipe writes delete intents, and those refuse any other
	// shape of id.
	const wiped = "550e8400-e29b-41d4-a716-446655440000"
	seedReactionMessages(t, store, self, peer, wiped)
	if _, err := store.ApplyReactionFact(ctx,
		reactionFact(wiped, "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// A refusal from some earlier per-message delete in this conversation.
	if err := store.RefuseReactionsFor(ctx, scopeOf(peer), "earlier", now); err != nil {
		t.Fatalf("refuse: %v", err)
	}

	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if _, err := store.DeleteConversationWithIntents(ctx, peer, scope,
		ConversationWipeClassification{}, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("wipe: %v", err)
	}

	if refusedFor(t, store, peer, wiped, now) {
		t.Fatal("a wiped conversation left a permanent row per message it erased")
	}
	// And the ones it has no list of. Everything deleted from this conversation
	// EARLIER is refused too, and no query over `messages` can name those ids
	// any more — which is what the scope column is for.
	if refusedFor(t, store, peer, "earlier", now) {
		t.Fatal("the refusal of a message deleted earlier outlived the wipe of its conversation")
	}
}

// The other half of that rule, and the half that costs something to get wrong:
// a wipe KEEPS immutable messages, and one survivor keeps the conversation
// admitting offers. Dropping its refusals then is an hour of held rows per id,
// as soon as the tombstones expire — the loop this table exists to end.
func TestAWipeThatLeavesAMessageKeepsItsRefusals(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	const survivor = "9f8e7d6c-5b4a-4938-8271-605f4e3d2c1b"
	if err := store.Append(ctx, "dm", self, Entry{
		ID: survivor, Sender: peer.String(), Recipient: self.String(),
		Body: "ciphertext", Flag: FlagImmutable,
		CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("seed the immutable message: %v", err)
	}
	if err := store.RefuseReactionsFor(ctx, scopeOf(peer), "earlier", now); err != nil {
		t.Fatalf("refuse: %v", err)
	}

	scope, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if _, err := store.DeleteConversationWithIntents(ctx, peer, scope,
		ConversationWipeClassification{}, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("wipe: %v", err)
	}
	if _, found, err := store.EntryByID(ctx, survivor); err != nil || !found {
		t.Fatalf("the fixture proves nothing: the immutable message is gone (found=%v err=%v)", found, err)
	}

	if !refusedFor(t, store, peer, "earlier", now) {
		t.Fatal("a wipe that left the conversation alive still forgot its refusals")
	}
}

// The wipe of a thread with nothing to delete is also ONE commit: the waiting
// facts and the refusals go together or not at all.
//
// The caller reports "nothing was wiped" on an error and, on that report, leaves
// the conversation state alone and publishes no event. Two commits would let it
// say that with the facts already gone — rows the user asked to destroy, erased
// while the UI still draws them from a cache only that event reloads.
func TestWipingAnEmptyThreadsReactionsIsOneCommit(t *testing.T) {
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()

	if held, err := store.HoldReactionFact(ctx,
		reactionFact("never-arrived", "👍", peer, domain.ReactionSet, 1), now); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}
	// The second half of the transaction is made to fail, which is the only way
	// to reach the state from outside.
	if _, err := store.db.ExecContext(ctx, `DROP TABLE reaction_refusals`); err != nil {
		t.Fatalf("break the refusals: %v", err)
	}

	dropped, forgotten, err := store.WipeEmptyConversationReactions(ctx, peer)
	if err == nil {
		t.Fatal("a broken refusal table was not reported")
	}
	if dropped != 0 || forgotten {
		t.Fatalf("the failure was reported as work done: dropped=%d forgotten=%v", dropped, forgotten)
	}
	// The waiting fact is still here, so the caller's "nothing was wiped" is
	// true. Counted directly: a held row is invisible to every ordinary read,
	// and the sweep — which would otherwise be the way to see it — is itself a
	// writer to the table this test just broke.
	var waiting int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_reactions WHERE pending = 1`).Scan(&waiting); err != nil {
		t.Fatalf("count what is left waiting: %v", err)
	}
	if waiting != 1 {
		t.Fatalf("%d facts were waiting after a reported failure, want the one that was never wiped", waiting)
	}
}

// Storing the message and lifting its refusal are ONE commit, and the failure
// path is why. The caller reads an error as "not stored" and publishes no
// arrival event; if the row had been committed anyway, the next copy of the
// message would be a duplicate — silent by design — and the message would sit
// in the database, invisible until the conversation is loaded again.
//
// The refusal table is dropped to make the second statement fail, which is the
// only way to reach the state from outside.
func TestAMessageWhoseRefusalCannotBeLiftedIsNotStored(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "aa")
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()

	if _, err := store.db.ExecContext(ctx, `DROP TABLE reaction_refusals`); err != nil {
		t.Fatalf("break the refusals: %v", err)
	}

	inserted, err := store.AppendReportNew(ctx, "dm", self, Entry{
		ID: "m1", Sender: peer.String(), Recipient: self.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		t.Fatal("a broken refusal table was not reported")
	}
	if inserted {
		t.Fatal("the failure was reported as an insert")
	}
	if _, found, err := store.EntryByID(ctx, "m1"); err != nil || found {
		t.Fatalf("the message was committed under a reported failure: found=%v err=%v", found, err)
	}
}

func scopeOf(peer domain.PeerIdentity) domain.ReactionScope {
	return domain.ReactionScopeForPeer(peer)
}

func refusedFor(t *testing.T, store *Store, peer domain.PeerIdentity, id domain.MessageID, now time.Time) bool {
	t.Helper()
	refused, err := store.ReactionsRefusedFor(context.Background(), scopeOf(peer), id, now)
	if err != nil {
		t.Fatalf("look up the refusal of %s: %v", id, err)
	}
	return refused
}
