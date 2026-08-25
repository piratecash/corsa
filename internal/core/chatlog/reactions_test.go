package chatlog

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Identities for these tests, spelled out at full length so they parse as real
// peer identities rather than as whatever a short string happens to widen to.
func reactionPeer(t *testing.T, prefix string) domain.PeerIdentity {
	t.Helper()
	return domain.PeerIdentityFromWire(strings.Repeat(prefix, 20))
}

func reactionFact(id, emoji string, actor domain.PeerIdentity, op domain.ReactionOp, clock uint64) domain.ReactionFact {
	return domain.ReactionFact{
		Scope: domain.ReactionScope(actor.String()),
		Key: domain.ReactionKey{
			MessageID: domain.MessageID(id),
			Actor:     actor,
			Emoji:     emoji,
		},
		Op:    op,
		Clock: domain.ReactionClock(clock),
	}
}

// seedReactionMessages puts the messages a fact can be APPLIED to in the store.
//
// An applied reaction is about a message this node has, and the upsert enforces
// that: without the rows here every apply below writes nothing, which is the
// point of the guard and not a quirk of the fixture.
func seedReactionMessages(t *testing.T, store *Store, self, peer domain.PeerIdentity, ids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		if err := store.Append(ctx, "dm", self, Entry{
			ID: id, Sender: self.String(), Recipient: peer.String(),
			Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("seed message %s: %v", id, err)
		}
	}
}

// The merge is one comparison, and everything the transport can do to a fact —
// deliver it twice, deliver it late, deliver it out of order — has to leave the
// state where it would have been anyway.
func TestReactionFactsConvergeUnderReplayAndReorder(t *testing.T) {
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "aa")
	seedReactionMessages(t, store, reactionPeer(t, "11"), peer, "m1")

	set := reactionFact("m1", "👍", peer, domain.ReactionSet, 5)
	cleared := reactionFact("m1", "👍", peer, domain.ReactionCleared, 6)

	changed, err := store.ApplyReactionFact(ctx, set, now)
	if err != nil || !changed {
		t.Fatalf("first apply: changed=%v err=%v", changed, err)
	}

	// A duplicate of the same decision must not count as a change: it is the
	// same fact, and the caller uses "changed" to decide whether to redraw.
	changed, err = store.ApplyReactionFact(ctx, set, now)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if changed {
		t.Fatal("replaying the same fact reported a state change")
	}

	if _, err := store.ApplyReactionFact(ctx, cleared, now); err != nil {
		t.Fatalf("clear: %v", err)
	}

	// The delayed duplicate of the OLD decision. Without a tombstone there
	// would be no row to compare 5 against and the reaction would come back.
	changed, err = store.ApplyReactionFact(ctx, set, now)
	if err != nil {
		t.Fatalf("late set: %v", err)
	}
	if changed {
		t.Fatal("a stale set superseded a newer clear: the reaction was resurrected")
	}

	got := reactionsOn(t, store, "m1", peer)
	if len(got) != 0 {
		t.Fatalf("message still shows %d reactions after the clear: %#v", len(got), got)
	}
}

// Counting is a fold over actors, so it says who — which is the half a stored
// number cannot express.
func TestReactionsFoldActorsRatherThanCounting(t *testing.T) {
	self := reactionPeer(t, "11")
	other := reactionPeer(t, "22")
	third := reactionPeer(t, "33")

	folded := FoldReactions([]domain.ReactionFact{
		reactionFact("m1", "👍", other, domain.ReactionSet, 1),
		reactionFact("m1", "👍", self, domain.ReactionSet, 1),
		reactionFact("m1", "🔥", third, domain.ReactionSet, 1),
		reactionFact("m1", "🔥", self, domain.ReactionCleared, 2),
	}, self)

	if len(folded) != 2 {
		t.Fatalf("folded %d emoji, want 👍 and 🔥: %#v", len(folded), folded)
	}
	if folded[0].Emoji != "👍" || folded[1].Emoji != "🔥" {
		t.Fatalf("order is not first-seen-first: %#v", folded)
	}
	if got := folded[0].Count(); got != 2 {
		t.Fatalf("👍 count = %d, want 2", got)
	}
	if !folded[0].Mine {
		t.Fatal("👍 is not marked as mine though this user is one of its actors")
	}
	if got := folded[1].Count(); got != 1 {
		t.Fatalf("🔥 count = %d, want 1 — the tombstone must not be counted", got)
	}
	if folded[1].Mine {
		t.Fatal("🔥 is marked as mine though this user cleared it")
	}
}

// A reaction can overtake the message it is about. Dropping it would lose it
// for good: the sender has no reason to repeat a fact it believes delivered.
func TestReactionsHeldForAMessageThatHasNotArrived(t *testing.T) {
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "bb")

	if _, err := store.HoldReactionFact(ctx, reactionFact("m2", "🔥", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	got := reactionsOn(t, store, "m2", peer)
	if len(got) != 0 {
		t.Fatal("a held fact is already visible: it must wait for its message")
	}

	// The message arrives — which is the only thing that makes a release
	// legitimate, and what the release checks for itself.
	seedReactionMessages(t, store, reactionPeer(t, "11"), peer, "m2")

	applied, err := store.ReleaseHeldReactions(ctx, domain.ReactionScope(peer.String()), "m2", now)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if applied != 1 {
		t.Fatalf("released %d facts, want 1", applied)
	}
	if got = reactionsOn(t, store, "m2", peer); len(got) != 1 {
		t.Fatalf("after release: %d reactions", len(got))
	}

	// And releasing twice is not a way to apply a fact that is no longer held.
	if applied, err = store.ReleaseHeldReactions(ctx, domain.ReactionScope(peer.String()), "m2", now); err != nil || applied != 0 {
		t.Fatalf("second release applied %d facts, err=%v", applied, err)
	}
}

// A held fact that was superseded while it waited loses, exactly as it would
// have if the two had arrived in the other order.
func TestHeldReactionLosesToANewerDecision(t *testing.T) {
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "cc")
	seedReactionMessages(t, store, reactionPeer(t, "11"), peer, "m3")

	if _, err := store.HoldReactionFact(ctx, reactionFact("m3", "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, err := store.ApplyReactionFact(ctx, reactionFact("m3", "👍", peer, domain.ReactionCleared, 2), now); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, err := store.ReleaseHeldReactions(ctx, domain.ReactionScope(peer.String()), "m3", now); err != nil {
		t.Fatalf("release: %v", err)
	}

	got := reactionsOn(t, store, "m3", peer)
	if len(got) != 0 {
		t.Fatalf("the held fact overwrote a newer decision: %#v", got)
	}
}

// Reactions are metadata about a message in exactly the sense deleteMessageTx
// means: who responded to what. Left behind they would also let the next
// reconciliation re-assert that the id existed.
func TestDeletingAMessageTakesItsReactions(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "22")

	if err := store.Append(ctx, "dm", self, Entry{
		ID: "m5", Sender: self.String(), Recipient: peer.String(),
		Body: "hi", CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := store.ApplyReactionFact(ctx, reactionFact("m5", "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := store.HoldReactionFact(ctx, reactionFact("m5", "🔥", peer, domain.ReactionSet, 2), now); err != nil {
		t.Fatalf("hold: %v", err)
	}

	if _, err := store.DeleteByID(ctx, "m5"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	facts, err := store.ReactionFacts(ctx, "m5")
	if err != nil {
		t.Fatalf("read after delete: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("%d reactions outlived the message they describe: %#v", len(facts), facts)
	}
	// Including the one that was still waiting for its message: it names the
	// same id and is the same claim that the id existed.
	released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScope(peer.String()), "m5", now)
	if err != nil {
		t.Fatalf("release after delete: %v", err)
	}
	if released != 0 {
		t.Fatalf("%d held reactions outlived the message", released)
	}
}

// The local counter is derived from what is stored, so it cannot fall behind
// the facts it is supposed to order — which is what a separate counter does
// after a restore or a half-applied transaction.
func TestNextReactionClockFollowsTheStoredFacts(t *testing.T) {
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()
	self := reactionPeer(t, "dd")
	other := reactionPeer(t, "ee")
	seedReactionMessages(t, store, reactionPeer(t, "11"), self, "m4")
	// The other actor reacts in THEIR OWN conversation: a fact may only name a
	// message of the conversation it is scoped to, so borrowing m4 would be
	// refused and the assertion below would pass for the wrong reason.
	seedReactionMessages(t, store, reactionPeer(t, "11"), other, "m4-elsewhere")

	clock, err := store.NextReactionClock(ctx, self)
	if err != nil || clock != 1 {
		t.Fatalf("first clock = %d, err=%v, want 1", clock, err)
	}

	if _, err := store.ApplyReactionFact(ctx, reactionFact("m4", "👍", self, domain.ReactionSet, 7), now); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Somebody else's much higher counter must not move ours: the counters are
	// per actor and are never compared across actors.
	stored, err := store.ApplyReactionFact(ctx,
		reactionFact("m4-elsewhere", "🔥", other, domain.ReactionSet, 99), now)
	if err != nil || !stored {
		t.Fatalf("apply other: stored=%v err=%v", stored, err)
	}

	if clock, err = store.NextReactionClock(ctx, self); err != nil || clock != 8 {
		t.Fatalf("clock after our own 7 = %d, err=%v, want 8", clock, err)
	}
}

// A held fact names a message this node has never had, so no user action ever
// names it: deletions are issued for messages someone can SEE. Without this
// sweep the table is a map any peer can grow by naming ids at datagram rate and
// never shrink — the leak shape this project has already paid for twice.
func TestHeldReactionsAreSweptAfterTheirTTL(t *testing.T) {
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "ff")

	if _, err := store.HoldReactionFact(ctx, reactionFact("ghost", "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	swept, err := store.SweepHeldReactions(ctx, now.Add(HeldReactionTTL-time.Minute))
	if err != nil {
		t.Fatalf("early sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("the sweep took %d facts that were still within their TTL", swept)
	}
	if swept, err = store.SweepHeldReactions(ctx, now.Add(HeldReactionTTL+time.Minute)); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("the sweep took %d facts, want the one that timed out", swept)
	}
	// And what it took really is gone, not merely hidden.
	if released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScope(peer.String()), "ghost", now); err != nil || released != 0 {
		t.Fatalf("a swept fact came back: released=%d err=%v", released, err)
	}
}

// An applied fact is NOT swept, however old: it names a message this node has,
// and its age says nothing about whether the reaction still stands.
func TestTheSweepLeavesAppliedFactsAlone(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC().Add(-30 * HeldReactionTTL)
	peer := reactionPeer(t, "ee")

	if err := store.Append(ctx, "dm", self, Entry{
		ID: "m9", Sender: peer.String(), Recipient: self.String(),
		Body: "hi", CreatedAt: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := store.ApplyReactionFact(ctx, reactionFact("m9", "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if swept, err := store.SweepHeldReactions(ctx, time.Now().UTC()); err != nil || swept != 0 {
		t.Fatalf("the sweep took %d applied facts, err=%v", swept, err)
	}
	if shown := reactionsOn(t, store, "m9", self); len(shown) != 1 {
		t.Fatalf("an applied fact was swept: %d reactions", len(shown))
	}
}

// One actor cannot occupy unlimited rows on one message by varying the emoji.
// The merge is per (message, actor, emoji), so without a ceiling the table is
// as large as a peer cares to make it.
func TestOneActorCannotFloodOneMessage(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "dd")
	seedReactionMessages(t, store, self, peer, "m8")

	for i := range MaxReactionsPerActorPerMessage {
		emoji := "e" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if _, err := store.ApplyReactionFact(ctx,
			reactionFact("m8", emoji, peer, domain.ReactionSet, uint64(i+1)), now); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	stored, err := store.ApplyReactionFact(ctx,
		reactionFact("m8", "one-too-many", peer, domain.ReactionSet, 999), now)
	if err != nil {
		t.Fatalf("the ceiling reported an error rather than refusing: %v", err)
	}
	if stored {
		t.Fatal("an actor got past the ceiling on one message")
	}
	if held := reactionsOn(t, store, "m8", self); len(held) != MaxReactionsPerActorPerMessage {
		t.Fatalf("the message carries %d reactions, want the ceiling of %d",
			len(held), MaxReactionsPerActorPerMessage)
	}

	// But an actor AT the ceiling can still change their mind about a reaction
	// they already hold: that supersedes a row rather than adding one, and
	// refusing it would make the limit stop people CLEARING reactions.
	//
	// Checked on the RESULT and not just on the error, because the failure mode
	// is (false, nil): `INSERT … SELECT … WHERE <false>` yields no rows and so
	// never reaches ON CONFLICT, which is exactly how a guard written without
	// the existing-key escape blocks every supersede in silence.
	cleared, err := store.ApplyReactionFact(ctx,
		reactionFact("m8", "eaa", peer, domain.ReactionCleared, 1000), now)
	if err != nil {
		t.Fatalf("an actor at the ceiling could not clear a reaction they hold: %v", err)
	}
	if !cleared {
		t.Fatal("the ceiling blocked a supersede: an actor at the limit can no longer clear what they hold")
	}
	if held := reactionsOn(t, store, "m8", self); len(held) != MaxReactionsPerActorPerMessage-1 {
		t.Fatalf("after clearing one, %d reactions stand; want %d",
			len(held), MaxReactionsPerActorPerMessage-1)
	}

	// And the ceiling is per actor: somebody else's reactions are their own.
	other := reactionPeer(t, "cc")
	if _, err := store.ApplyReactionFact(ctx,
		reactionFact("m8", "👍", other, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("one actor's flood blocked another actor entirely: %v", err)
	}
}

// reactionsOn is what one message shows: the facts, folded. Production reads a
// whole conversation at once (ReactionsForScope), so this pairing exists only
// where a test wants to talk about one message.
func reactionsOn(t *testing.T, store *Store, id domain.MessageID, self domain.PeerIdentity) []domain.Reaction {
	t.Helper()
	facts, err := store.ReactionFacts(context.Background(), id)
	if err != nil {
		t.Fatalf("read reactions on %s: %v", id, err)
	}
	return FoldReactions(facts, self)
}

// A conversation wipe takes the reactions that are still WAITING for a message
// with it. They are matched by scope, because a held fact has no message row a
// join could reach — and left behind they would be "peer X reacted to something
// in this conversation" surviving the conversation the user erased.
func TestWipingAConversationTakesItsHeldReactions(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "ab")

	// The scope of a conversation is the OTHER party, which is what both the
	// wipe and the fact call it.
	fact := domain.ReactionFact{
		Scope: domain.ReactionScopeForPeer(peer),
		Key:   domain.ReactionKey{MessageID: "never-arrived", Actor: peer, Emoji: "👍"},
		Op:    domain.ReactionSet,
		Clock: 1,
	}
	if _, err := store.HoldReactionFact(ctx, fact, now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if _, err := store.DeleteByPeer(ctx, peer); err != nil {
		t.Fatalf("wipe: %v", err)
	}
	released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScopeForPeer(peer), "never-arrived", now)
	if err != nil {
		t.Fatalf("release after the wipe: %v", err)
	}
	if released != 0 {
		t.Fatalf("%d held reactions outlived the conversation wipe", released)
	}
}

// One actor cannot keep unlimited facts waiting for messages this node does not
// have. The TTL alone bounds their AGE, not their number, and a peer sending at
// datagram rate fills a whole TTL window.
func TestOneActorCannotFloodTheHeldFacts(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "ba")

	for i := range MaxHeldReactionsPerActor {
		if _, err := store.HoldReactionFact(ctx,
			reactionFact(fmt.Sprintf("ghost-%d", i), "👍", peer, domain.ReactionSet, uint64(i+1)), now); err != nil {
			t.Fatalf("hold %d: %v", i, err)
		}
	}
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("one-too-many", "👍", peer, domain.ReactionSet, 1<<20), now); err != nil {
		t.Fatalf("the ceiling reported an error rather than refusing: %v", err)
	}
	// Read through the release, not through ReactionFacts: that one filters
	// pending = 0, so a held row is invisible to it whether or not the ceiling
	// worked, and the assertion would hold with the ceiling removed.
	if released, err := store.ReleaseHeldReactions(ctx,
		domain.ReactionScope(peer.String()), "one-too-many", now); err != nil || released != 0 {
		t.Fatalf("a fact past the held ceiling was stored: released=%d err=%v", released, err)
	}

	// The held ceiling gates HELD writes only. An applied fact adds no held row,
	// so gating it would mute the actor entirely for the whole TTL — which a
	// peer returning after a long absence reaches legitimately.
	appendFor(t, store, self, peer, "arrived")
	stored, err := store.ApplyReactionFact(ctx,
		reactionFact("arrived", "👍", peer, domain.ReactionSet, 1<<21), now)
	if err != nil {
		t.Fatalf("apply while the held quota is full: %v", err)
	}
	if !stored {
		t.Fatal("a full held quota silenced the actor on a message this node has")
	}
	// And a supersede of one of the held facts still lands. Asserted on the
	// RESULT, not on the error: the failure mode of a guard written without the
	// existing-key escape is (false, nil), so an error-only check would hold
	// while the property it names was broken.
	superseded, err := store.HoldReactionFact(ctx,
		reactionFact("ghost-0", "👍", peer, domain.ReactionCleared, 1<<22), now)
	if err != nil {
		t.Fatalf("supersede a held fact while the quota is full: %v", err)
	}
	if !superseded {
		t.Fatal("a full held quota blocked a supersede: the actor can no longer change what they hold")
	}
	facts, err := store.ReactionFacts(ctx, "arrived")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("the applied fact is not there: %#v", facts)
	}

	// And the ceiling is per actor: another peer is not blocked by this flood.
	other := reactionPeer(t, "bc")
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("theirs", "🔥", other, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("one actor's flood blocked another: %v", err)
	}
	seedReactionMessages(t, store, reactionPeer(t, "11"), other, "theirs")
	if released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScope(other.String()), "theirs", now); err != nil || released != 1 {
		t.Fatalf("the other actor's fact was not stored: released=%d err=%v", released, err)
	}
}

// Message ids are chosen by whoever sends them, so a peer can hold facts on an
// id it expects a DIFFERENT peer to use. The release is scoped to one
// conversation AND checks that the message is in it: a stranger's fact must not
// become visible because an unrelated conversation received a message with the
// same id.
func TestReleasingIsScopedToOneConversation(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	mine, stranger := reactionPeer(t, "ca"), reactionPeer(t, "cb")

	held := func(actor domain.PeerIdentity) domain.ReactionFact {
		return domain.ReactionFact{
			Scope: domain.ReactionScopeForPeer(actor),
			Key:   domain.ReactionKey{MessageID: "shared-id", Actor: actor, Emoji: "👍"},
			Op:    domain.ReactionSet,
			Clock: 1,
		}
	}
	for _, actor := range []domain.PeerIdentity{mine, stranger} {
		if _, err := store.HoldReactionFact(ctx, held(actor), now); err != nil {
			t.Fatalf("hold for %s: %v", actor, err)
		}
	}
	// The message with that id arrives in ONE of the two conversations.
	seedReactionMessages(t, store, self, mine, "shared-id")

	released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScopeForPeer(mine), "shared-id", now)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released != 1 {
		t.Fatalf("released %d facts, want only the one in this conversation", released)
	}

	// The stranger's is still waiting — its conversation has no such message,
	// and releasing it would make a fact applied against a message that
	// conversation never had.
	again, err := store.ReleaseHeldReactions(ctx, domain.ReactionScopeForPeer(stranger), "shared-id", now)
	if err != nil {
		t.Fatalf("release the stranger's: %v", err)
	}
	if again != 0 {
		t.Fatalf("%d of the stranger's facts were released by another conversation's message", again)
	}
	if shown := reactionsOn(t, store, "shared-id", self); len(shown) != 1 {
		t.Fatalf("the message shows %d reactions, want only the one from its own conversation", len(shown))
	}
}

func appendFor(t *testing.T, store *Store, self, peer domain.PeerIdentity, id string) {
	t.Helper()
	if err := store.Append(context.Background(), "dm", self, Entry{
		ID: id, Sender: peer.String(), Recipient: self.String(),
		Body: "hi", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
}

// The sweep compares first_seen_at as a STRING, so the layout has to be
// order-preserving. RFC3339Nano is not: it strips trailing zeros, so a whole
// second sorts after the same second plus a fraction.
func TestTheHeldStampIsOrderPreserving(t *testing.T) {
	base := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	for _, later := range []time.Duration{
		time.Nanosecond, time.Millisecond, 500 * time.Millisecond, time.Second,
	} {
		earlier := base.Format(sortableStamp)
		after := base.Add(later).Format(sortableStamp)
		if earlier >= after {
			t.Fatalf("%q does not sort before %q (+%s)", earlier, after, later)
		}
		if len(earlier) != len(after) {
			t.Fatalf("the stamp is not fixed width: %d vs %d bytes", len(earlier), len(after))
		}
	}
}

// `first_seen_at` must not move when a fact is superseded. It is the whole of
// the two-timestamp design: the sender chooses when to re-state a fact, so a TTL
// measured from the last write is one the sender extends forever by repeating
// itself with a higher clock.
//
// Adding `first_seen_at = excluded.first_seen_at` to the upsert's SET list is a
// one-line edit a future reader would find plausible; this is what stops it.
func TestASupersededHeldFactDoesNotRenewItsTTL(t *testing.T) {
	store := storeFor(t, strings.Repeat("11", 20))
	ctx := context.Background()
	start := time.Now().UTC()
	peer := reactionPeer(t, "fa")

	if _, err := store.HoldReactionFact(ctx,
		reactionFact("ghost", "👍", peer, domain.ReactionSet, 1), start); err != nil {
		t.Fatalf("hold: %v", err)
	}
	// The same fact re-stated most of a TTL later, exactly as a peer re-sending
	// its batch would.
	later := start.Add(HeldReactionTTL - 10*time.Minute)
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("ghost", "👍", peer, domain.ReactionSet, 2), later); err != nil {
		t.Fatalf("re-state: %v", err)
	}

	swept, err := store.SweepHeldReactions(ctx, start.Add(HeldReactionTTL+time.Minute))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("the sweep took %d facts: re-stating one extended its TTL", swept)
	}
	if released, err := store.ReleaseHeldReactions(ctx,
		domain.ReactionScope(peer.String()), "ghost", start); err != nil || released != 0 {
		t.Fatalf("the fact outlived the sweep: released=%d err=%v", released, err)
	}
}

// A fact held at clock N and re-delivered at the SAME clock N after its message
// has landed must become visible. The decision has not changed, so nothing about
// it may move — but `pending` is not part of the decision, it is what this node
// knows, and refusing the write left the fact hidden until the sweep took it an
// hour later on a node that by then HAD the message.
func TestARepeatOfAHeldFactIsAppliedOnceItsMessageIsHere(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "ad")

	fact := reactionFact("m1", "👍", peer, domain.ReactionSet, 7)
	if _, err := store.HoldReactionFact(ctx, fact, now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	appendFor(t, store, self, peer, "m1")

	// The same decision again, at the same clock, now that the message is here.
	applied, err := store.ApplyReactionFact(ctx, fact, now)
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if !applied {
		t.Fatal("a repeat of a held fact was refused, leaving it hidden until the sweep")
	}
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 1 {
		t.Fatalf("the fact is still hidden: %d reactions", len(shown))
	}

	// And the asymmetry holds: a repeat cannot push an APPLIED fact back into
	// waiting.
	if _, err := store.HoldReactionFact(ctx, fact, now); err != nil {
		t.Fatalf("re-hold: %v", err)
	}
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 1 {
		t.Fatal("a repeat pushed an applied fact back into waiting")
	}
}

// Nothing on this transport reports that a fact ARRIVED, so a re-offer is what
// delivery rests on: this conversation's own facts, offered again whenever a
// session comes up.
//
// Bounded by COUNT and not by age, and that is as much the point of the test as
// of the code: "until the peer upgrades" has no deadline, so a fact decided long
// ago must still be in the set a returning peer is offered.
func TestReactionsAuthoredByOffersThisConversationsOwnFacts(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	first, second := reactionPeer(t, "ae"), reactionPeer(t, "af")
	seedReactionMessages(t, store, self, first, "m1", "m2", "m4")
	seedReactionMessages(t, store, self, second, "m3")

	mine := func(peer domain.PeerIdentity, id, emoji string, clock uint64) domain.ReactionFact {
		return domain.ReactionFact{
			Scope: domain.ReactionScopeForPeer(peer),
			Key:   domain.ReactionKey{MessageID: domain.MessageID(id), Actor: self, Emoji: emoji},
			Op:    domain.ReactionSet,
			Clock: domain.ReactionClock(clock),
		}
	}
	if _, err := store.ApplyReactionFact(ctx, mine(first, "m1", "👍", 1), now.Add(-90*24*time.Hour)); err != nil {
		t.Fatalf("apply an old one: %v", err)
	}
	if _, err := store.ApplyReactionFact(ctx, mine(first, "m2", "🔥", 2), now); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := store.ApplyReactionFact(ctx, mine(second, "m3", "😮", 3), now); err != nil {
		t.Fatalf("apply elsewhere: %v", err)
	}
	if _, err := store.ApplyReactionFact(ctx,
		reactionFact("m4", "👍", first, domain.ReactionSet, 9), now); err != nil {
		t.Fatalf("apply theirs: %v", err)
	}

	offered, err := store.ReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(first), 256, 0)
	if err != nil {
		t.Fatalf("re-offer: %v", err)
	}
	if len(offered) != 2 {
		t.Fatalf("offered %d facts, want both of ours in this conversation: %#v", len(offered), offered)
	}
	seen := map[domain.MessageID]bool{}
	for _, fact := range offered {
		if fact.Key.Actor != self {
			t.Fatalf("offered a fact by %s, which is not ours to state", fact.Key.Actor)
		}
		if fact.Scope != domain.ReactionScopeForPeer(first) {
			t.Fatalf("offered a fact from %s to the wrong conversation", fact.Scope)
		}
		seen[fact.Key.MessageID] = true
	}
	if !seen["m1"] {
		t.Fatal("the oldest fact was left out: a peer returning after a month would never receive it")
	}

	// The page is a count and it starts at the NEWEST — and the next page walks
	// on, which is what makes "retry until the peer updates" true past the
	// first page rather than a shorter deadline in disguise.
	first0, err := store.ReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(first), 1, 0)
	if err != nil {
		t.Fatalf("page 0: %v", err)
	}
	if len(first0) != 1 || first0[0].Key.MessageID != "m2" {
		t.Fatalf("page 0 held %#v, want the newest", first0)
	}
	first1, err := store.ReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(first), 1, 1)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(first1) != 1 || first1[0].Key.MessageID != "m1" {
		t.Fatalf("page 1 held %#v, want the older one — nothing else would ever offer it", first1)
	}
	total, err := store.CountReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(first))
	if err != nil || total != 2 {
		t.Fatalf("the pager was told there are %d facts, err=%v", total, err)
	}
	convs, err := store.ConversationsWithReactionsBy(ctx, self)
	if err != nil {
		t.Fatalf("conversations: %v", err)
	}
	if len(convs) != 2 {
		t.Fatalf("the periodic re-offer would walk %d conversations, want both: %#v", len(convs), convs)
	}

	// A fact still waiting for its own message is not one we can vouch for.
	if _, err := store.HoldReactionFact(ctx, mine(first, "unseen", "🙏", 4), now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	again, err := store.ReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(first), 256, 0)
	if err != nil {
		t.Fatalf("re-offer: %v", err)
	}
	if len(again) != 2 {
		t.Fatalf("a pending fact was offered: %#v", again)
	}
}

// The per-message release is best-effort, so a fact can be left pending on a
// node that HAS the message — and nothing else comes back for it: the sender
// will not repeat what it believes delivered, and a repeat need not bring
// another copy of the MESSAGE, which is what the per-message path keys on.
func TestArrivedReactionsAreReleasedWithoutThePeer(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer, stranger := reactionPeer(t, "ba"), reactionPeer(t, "bb")

	appendFor(t, store, self, peer, "m1")
	// The state a lost release leaves: pending, on a message that is here.
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("m1", "👍", peer, domain.ReactionSet, 1), now); err != nil {
		t.Fatalf("hold: %v", err)
	}
	// One that is genuinely still early: no message with this id at all.
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("m2", "🔥", peer, domain.ReactionSet, 2), now); err != nil {
		t.Fatalf("hold early: %v", err)
	}
	// And a stranger's fact naming the id of OUR message: a different
	// conversation, so the message here is not the one it is waiting for.
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("m1", "😮", stranger, domain.ReactionSet, 3), now); err != nil {
		t.Fatalf("hold a stranger's: %v", err)
	}

	scopes, err := store.ReleaseArrivedReactions(ctx, now)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	// The SCOPES, not a count: the chips come from a per-conversation cache
	// that only an event naming that conversation reloads.
	if len(scopes) != 1 || scopes[0] != domain.ReactionScopeForPeer(peer) {
		t.Fatalf("released %#v, want only the conversation whose message is here", scopes)
	}
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 1 || shown[0].Emoji != "👍" {
		t.Fatalf("the wrong facts became visible: %#v", shown)
	}
	// Idempotent: a second pass has nothing left to do.
	if again, err := store.ReleaseArrivedReactions(ctx, now); err != nil || len(again) != 0 {
		t.Fatalf("a second pass released %#v, err=%v", again, err)
	}
}

// At an EQUAL clock only the pending flag may move. A peer that states `set` and
// `cleared` for one key under one counter is stating two decisions with one
// number, and the outcome must not depend on which of them happened to be the
// one that was waiting.
func TestAnEqualClockCannotRewriteTheDecision(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "bc")

	appendFor(t, store, self, peer, "m1")
	if _, err := store.HoldReactionFact(ctx,
		reactionFact("m1", "👍", peer, domain.ReactionSet, 5), now); err != nil {
		t.Fatalf("hold the set: %v", err)
	}
	// The same key and the same clock, but the opposite decision, applied.
	if _, err := store.ApplyReactionFact(ctx,
		reactionFact("m1", "👍", peer, domain.ReactionCleared, 5), now); err != nil {
		t.Fatalf("apply the clear: %v", err)
	}

	facts, err := store.ReactionFacts(ctx, "m1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected one row, got %#v", facts)
	}
	if facts[0].Op != domain.ReactionSet {
		t.Fatal("an equal clock overwrote the decision: the outcome now depends on which arrived pending")
	}
	// The flag DID move, which is the only thing that path is for.
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 1 {
		t.Fatalf("the fact is still hidden: %#v", shown)
	}
}

// The re-offer runs on a timer, once per conversation, and its two reads filter
// on (actor, scope, pending) while the page orders by updated_at. Without an
// index shaped like that, each pass scans every fact this user ever stated and
// builds a temporary B-tree for the order.
//
// Asserted on the PLAN rather than on a timing, because a query fast enough on
// a test table says nothing about one on a real one.
func TestTheReofferReadsUseTheirIndex(t *testing.T) {
	self := reactionPeer(t, "11")
	store := newTestStore(t, self)

	for _, query := range []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "the page",
			sql:  reactionsAuthoredByQuery,
			args: []any{self.String(), "scope", 64, 0},
		},
		{
			name: "the count that tells the pager where to wrap",
			sql:  countReactionsAuthoredByQuery,
			args: []any{self.String(), "scope"},
		},
	} {
		plan := queryPlan(t, store, "EXPLAIN QUERY PLAN "+query.sql, query.args...)
		if !strings.Contains(plan, "idx_message_reactions_reoffer_page") {
			t.Fatalf("%s does not use the re-offer index:\n%s", query.name, plan)
		}
		if strings.Contains(plan, "SCAN message_reactions") {
			t.Fatalf("%s scans the whole table:\n%s", query.name, plan)
		}
		// Including the LAST term of the order: an index that covers the filter
		// and the leading term still materialises and sorts every page.
		if strings.Contains(plan, "TEMP B-TREE") {
			t.Fatalf("%s sorts into a temporary B-tree:\n%s", query.name, plan)
		}
	}

	// The list of conversations to walk runs on the same timer and is served by
	// the same index, without a temporary B-tree for the DISTINCT.
	plan := queryPlan(t, store,
		"EXPLAIN QUERY PLAN "+conversationsWithReactionsByQuery, self.String())
	if !strings.Contains(plan, "idx_message_reactions_reoffer_page") {
		t.Fatalf("listing the conversations does not use the re-offer index:\n%s", plan)
	}
	if strings.Contains(plan, "TEMP B-TREE") {
		t.Fatalf("listing the conversations sorts into a temporary B-tree:\n%s", plan)
	}
}

func queryPlan(t *testing.T, store *Store, sql string, args ...any) string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var plan strings.Builder
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		plan.WriteString(detail)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("explain rows: %v", err)
	}
	return plan.String()
}

// The per-message delete and a reaction write are two transactions, and the
// caller's "is the message here" check is not part of the second one. A delete
// that lands between the check and the write erases the reactions first and then
// has the write put one back — for a message that no longer exists. Nothing
// reaches such a row afterwards: the per-message delete needs a message, the
// conversation wipe runs by scope and has already run, and the sweep takes only
// held rows. It is offered to the peer for as long as the process lives.
func TestAReactionCannotBeAppliedToAMessageAlreadyDeleted(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "ab")
	seedReactionMessages(t, store, self, peer, "m1")

	// The caller has decided the message is here — it is — and now the delete
	// runs before the write reaches the database.
	if _, err := store.DeleteByID(ctx, "m1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	applied, err := store.ApplyReactionFact(ctx,
		reactionFact("m1", "👍", peer, domain.ReactionSet, 1), now)
	if err != nil {
		t.Fatalf("the write reported an error rather than refusing: %v", err)
	}
	if applied {
		t.Fatal("a reaction was applied to a message that had just been deleted")
	}
	if got := reactionsOn(t, store, "m1", self); len(got) != 0 {
		t.Fatalf("the deleted message carries %d reactions again: %#v", len(got), got)
	}

	// A fact whose message is genuinely not here yet is a different case and
	// still waits: this guard is about a message that is GONE, not about one
	// that has not arrived.
	held, err := store.HoldReactionFact(ctx,
		reactionFact("m2", "🔥", peer, domain.ReactionSet, 2), now)
	if err != nil || !held {
		t.Fatalf("a held fact was refused: held=%v err=%v", held, err)
	}
}

// A reaction may only name a message of the conversation it is scoped to.
//
// ToggleReaction takes the peer and the message id as separate arguments, so a
// tap that lands after the user switched conversations — a stale UI event, a
// mistaken call — would otherwise attach a message of conversation A to the
// scope of conversation B. The re-offer then hands B the id of a message they
// never saw and the emoji somebody put on it, and the ON CONFLICT branch can
// move an existing reaction into another conversation on the way.
func TestAReactionCannotNameAnotherConversationsMessage(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	first, second := reactionPeer(t, "b1"), reactionPeer(t, "b2")
	seedReactionMessages(t, store, self, first, "m-first")

	mine := func(scope domain.PeerIdentity, id string, clock uint64) domain.ReactionFact {
		return domain.ReactionFact{
			Scope: domain.ReactionScopeForPeer(scope),
			Key:   domain.ReactionKey{MessageID: domain.MessageID(id), Actor: self, Emoji: "👍"},
			Op:    domain.ReactionSet,
			Clock: domain.ReactionClock(clock),
		}
	}

	stored, err := store.ApplyReactionFact(ctx, mine(second, "m-first", 1), now)
	if err != nil {
		t.Fatalf("the write reported an error rather than refusing: %v", err)
	}
	if stored {
		t.Fatal("a message of one conversation was reacted to under another's scope")
	}

	// The same fact in its own conversation is ordinary and goes in.
	if stored, err = store.ApplyReactionFact(ctx, mine(first, "m-first", 1), now); err != nil || !stored {
		t.Fatalf("the fact was refused in its own conversation: stored=%v err=%v", stored, err)
	}
	// And it cannot be MOVED afterwards either: a later clock under the wrong
	// scope must not take the row with it.
	if stored, err = store.ApplyReactionFact(ctx, mine(second, "m-first", 2), now); err != nil || stored {
		t.Fatalf("a later fact moved the reaction into another conversation: stored=%v err=%v", stored, err)
	}
	offered, err := store.ReactionsAuthoredBy(ctx, self, domain.ReactionScopeForPeer(second), 64, 0)
	if err != nil {
		t.Fatalf("read the other conversation: %v", err)
	}
	if len(offered) != 0 {
		t.Fatalf("the other conversation would be offered %d facts about a message it never saw: %#v",
			len(offered), offered)
	}
}

// A scope that names no conversation this store can check is refused loudly
// rather than written. Groups (§8) have no membership question answerable from
// `messages` yet, so until they do, such a scope is a caller's bug — and the row
// it would write is attributed to a conversation nobody can name.
func TestAReactionWithAnUncheckableScopeIsRefused(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	peer := reactionPeer(t, "b3")
	seedReactionMessages(t, store, self, peer, "m1")

	_, err := store.ApplyReactionFact(ctx, domain.ReactionFact{
		Scope: domain.ReactionScope("group:standup"),
		Key:   domain.ReactionKey{MessageID: "m1", Actor: self, Emoji: "👍"},
		Op:    domain.ReactionSet,
		Clock: 1,
	}, time.Now().UTC())
	if err == nil {
		t.Fatal("a fact scoped to something that is not a conversation was accepted")
	}
}

// ReactionFactsByKey is what lets the send queue hold keys instead of copies:
// it answers what a reaction is worth NOW, and says nothing at all about the
// ones that are no longer there.
func TestReactionFactsByKeyAnswerWhatIsTrueNow(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer, other := reactionPeer(t, "c1"), reactionPeer(t, "c2")
	scope := domain.ReactionScopeForPeer(peer)
	seedReactionMessages(t, store, self, peer, "m1", "m2", "m3")
	seedReactionMessages(t, store, self, other, "m-elsewhere")

	mine := func(scope domain.ReactionScope, id, emoji string, op domain.ReactionOp, clock uint64) domain.ReactionFact {
		return domain.ReactionFact{
			Scope: scope,
			Key:   domain.ReactionKey{MessageID: domain.MessageID(id), Actor: self, Emoji: emoji},
			Op:    op,
			Clock: domain.ReactionClock(clock),
		}
	}
	for _, fact := range []domain.ReactionFact{
		mine(scope, "m1", "👍", domain.ReactionSet, 1),
		// A tombstone is as much news as a set, so it must come back too.
		mine(scope, "m2", "🔥", domain.ReactionCleared, 2),
		mine(domain.ReactionScopeForPeer(other), "m-elsewhere", "👍", domain.ReactionSet, 3),
	} {
		if stored, err := store.ApplyReactionFact(ctx, fact, now); err != nil || !stored {
			t.Fatalf("seed %s: stored=%v err=%v", fact.Key.MessageID, stored, err)
		}
	}
	// One that is still waiting for its message: not something to state.
	if held, err := store.HoldReactionFact(ctx, mine(scope, "m-unseen", "😮", domain.ReactionSet, 4), now); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}

	key := func(id, emoji string) domain.ReactionKey {
		return domain.ReactionKey{MessageID: domain.MessageID(id), Actor: self, Emoji: emoji}
	}
	facts, err := store.ReactionFactsByKey(ctx, scope, self, []domain.ReactionKey{
		key("m1", "👍"),
		key("m2", "🔥"),
		key("m3", "🙏"),          // never existed
		key("m-unseen", "😮"),    // held
		key("m-elsewhere", "👍"), // another conversation's
	})
	if err != nil {
		t.Fatalf("read by key: %v", err)
	}
	if len(facts) != 2 {
		t.Fatalf("got %d facts, want the two this conversation actually holds: %#v", len(facts), facts)
	}
	// In the order asked for, because that order is what the queue's cap drops
	// from.
	if facts[0].Key.MessageID != "m1" || facts[1].Key.MessageID != "m2" {
		t.Fatalf("the facts came back as %s, %s — not in the order asked for",
			facts[0].Key.MessageID, facts[1].Key.MessageID)
	}
	if facts[1].Op != domain.ReactionCleared || facts[1].Clock != 2 {
		t.Fatalf("the tombstone came back as %#v, want what the record says", facts[1])
	}

	// And a reaction deleted with its message stops being an answer at all.
	if _, err := store.DeleteByID(ctx, "m1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if facts, err = store.ReactionFactsByKey(ctx, scope, self, []domain.ReactionKey{key("m1", "👍")}); err != nil {
		t.Fatalf("read after the delete: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("a reaction of a deleted message is still offered: %#v", facts)
	}
}

// Releasing is what MAKES a fact applied, so it carries the same guard the
// applied write does. Without it the release is a second door into the state the
// guard protects: a delete landing between the caller's "the message is here"
// and the UPDATE leaves an applied row for a message that no longer exists —
// and nothing reaches such a row afterwards.
func TestAHeldFactIsNotReleasedOntoADeletedMessage(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "cd")
	scope := domain.ReactionScopeForPeer(peer)
	seedReactionMessages(t, store, self, peer, "m1")

	if held, err := store.HoldReactionFact(ctx,
		reactionFact("m1", "👍", peer, domain.ReactionSet, 1), now); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}
	// The caller has seen the message and is on its way to release the fact
	// when the message is deleted.
	if _, err := store.DeleteByID(ctx, "m1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	released, err := store.ReleaseHeldReactions(ctx, scope, "m1", now)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released != 0 {
		t.Fatalf("%d facts were made visible against a deleted message", released)
	}
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 0 {
		t.Fatalf("the deleted message shows %d reactions: %#v", len(shown), shown)
	}
}

// A wipe erases the thread message by message, and a held fact has no message
// row to be erased through: it is WAITING for one this node never received. So
// it survives — and if that message ever arrives, the repair pass makes it
// visible in a conversation the user erased.
func TestWipingAConversationTakesTheReactionsStillWaitingInIt(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "d1")
	seedReactionMessages(t, store, self, peer, "8a111111-2222-4333-8444-555555555555")

	// One fact about a message we have, one waiting for a message we do not.
	if stored, err := store.ApplyReactionFact(ctx,
		reactionFact("8a111111-2222-4333-8444-555555555555", "👍", peer, domain.ReactionSet, 1), now); err != nil || !stored {
		t.Fatalf("apply: stored=%v err=%v", stored, err)
	}
	if held, err := store.HoldReactionFact(ctx,
		reactionFact("not-here-yet", "🔥", peer, domain.ReactionSet, 2), now); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}

	ids, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if _, err := store.DeleteConversationWithIntents(ctx, peer, ids,
		ConversationWipeClassification{}, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("wipe: %v", err)
	}

	// The message the held fact was waiting for arrives after the wipe, which is
	// exactly when a survivor would become visible.
	seedReactionMessages(t, store, self, peer, "not-here-yet")
	released, err := store.ReleaseArrivedReactions(ctx, now)
	if err != nil {
		t.Fatalf("repair pass: %v", err)
	}
	if len(released) != 0 {
		t.Fatalf("the wipe left facts behind: %v", released)
	}
	if shown := reactionsOn(t, store, "not-here-yet", self); len(shown) != 0 {
		t.Fatalf("an erased conversation's reaction came back: %#v", shown)
	}
}

// A TTL expiry is a deletion like any other, so it takes what the message left
// in the other tables. Without that a reaction outlives its message — and both
// the UI and the re-offer read reactions without joining the messages, so it is
// drawn and sent for as long as the process lives.
func TestExpiringAMessageTakesItsReactions(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "d2")

	if err := store.Append(ctx, "dm", self, Entry{
		ID: "ttl-1", Sender: self.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: now.Add(-time.Hour).Format(time.RFC3339Nano),
		Flag: "auto-delete-ttl", TTLSeconds: 60,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if stored, err := store.ApplyReactionFact(ctx,
		reactionFact("ttl-1", "👍", peer, domain.ReactionSet, 1), now); err != nil || !stored {
		t.Fatalf("apply: stored=%v err=%v", stored, err)
	}

	gone, err := store.DeleteExpired(ctx)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if gone != 1 {
		t.Fatalf("the TTL sweep removed %d messages, want 1", gone)
	}
	facts, err := store.ReactionFacts(ctx, "ttl-1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("the expired message left %d reactions behind: %#v", len(facts), facts)
	}
}

// An immutable message survives a wipe by design, so its reactions survive with
// it. Erasing them would not stick: the peer re-offers the fact, the message is
// still there, the guard admits it and the chip comes back — and refusing it
// forever instead would leave a visible message with a permanently wrong count.
func TestAWipeKeepsTheReactionsOfTheMessagesItKeeps(t *testing.T) {
	self := reactionPeer(t, "11")
	store := storeFor(t, self.String())
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionPeer(t, "d3")

	const (
		ordinary  = "8b111111-2222-4333-8444-555555555555"
		immutable = "8b222222-3333-4444-8555-666666666666"
	)
	seedReactionMessages(t, store, self, peer, ordinary)
	if err := store.Append(ctx, "dm", self, Entry{
		ID: immutable, Sender: self.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: now.Format(time.RFC3339Nano), Flag: FlagImmutable,
	}); err != nil {
		t.Fatalf("append the immutable one: %v", err)
	}
	for _, id := range []string{ordinary, immutable} {
		if stored, err := store.ApplyReactionFact(ctx,
			reactionFact(id, "👍", peer, domain.ReactionSet, 1), now); err != nil || !stored {
			t.Fatalf("apply on %s: stored=%v err=%v", id, stored, err)
		}
	}

	ids, err := store.ConversationCandidateIDs(ctx, peer)
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if _, err := store.DeleteConversationWithIntents(ctx, peer, ids,
		ConversationWipeClassification{}, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("wipe: %v", err)
	}

	if left := reactionsOn(t, store, ordinary, self); len(left) != 0 {
		t.Fatalf("the wiped message kept %d reactions", len(left))
	}
	kept := reactionsOn(t, store, immutable, self)
	if len(kept) != 1 {
		t.Fatalf("the surviving message shows %d reactions, want the one it had: %#v", len(kept), kept)
	}
}

// The deletion invariant, checked over every path that removes messages.
//
// One rule, and every finding of the last several review rounds was a violation
// of it on a path nobody had checked:
//
//	after any deletion, no reaction may name a message this conversation does
//	not have — and anything that SURVIVED the deletion keeps its reactions.
//
// With one exception, which is a state and not a leak: a fact still WAITING for
// a message that has not arrived yet is legitimate while the conversation
// exists, because the message may still come. It stops being legitimate the
// moment the conversation does — which is why the wipe and the contact removal
// must take it and a per-message deletion must not.
//
// The first half is what leaks: a reaction whose message is gone is metadata the
// deletion existed to destroy, no ordinary read shows it (a held one is
// invisible until its message arrives), nothing reaches it afterwards, and the
// re-offer keeps sending it. The second half is what over-deleting breaks: an
// immutable message survives a wipe by design, and erasing its reactions would
// not stick — the peer re-offers, the message is there, the chip comes back.
//
// A table over PATHS rather than one test per path, because the failure mode is
// not "this path is broken" but "a path was added and nobody rechecked it".
func TestEveryDeletionPathLeavesNoOrphanedReactions(t *testing.T) {
	self := reactionPeer(t, "11")
	peer := reactionPeer(t, "e1")
	scope := domain.ReactionScopeForPeer(peer)

	const (
		ordinary  = "9a111111-2222-4333-8444-555555555555"
		immutable = "9a222222-3333-4444-8555-666666666666"
		expiring  = "9a333333-4444-4555-8666-777777777777"
		absent    = "never-arrived"
	)

	// seed puts one conversation in a known state: three messages of different
	// kinds, an applied reaction on each, and one still WAITING for a message
	// that never came.
	seed := func(t *testing.T) (*Store, context.Context, time.Time) {
		t.Helper()
		store := storeFor(t, self.String())
		ctx := context.Background()
		now := time.Now().UTC()

		for _, message := range []struct {
			id   string
			flag string
			ttl  int
			age  time.Duration
		}{
			{id: ordinary, flag: string(FlagAnyDelete)},
			{id: immutable, flag: FlagImmutable},
			{id: expiring, flag: "auto-delete-ttl", ttl: 60, age: time.Hour},
		} {
			if err := store.Append(ctx, "dm", self, Entry{
				ID: message.id, Sender: self.String(), Recipient: peer.String(),
				Body: "ciphertext", Flag: message.flag, TTLSeconds: message.ttl,
				CreatedAt: now.Add(-message.age).Format(time.RFC3339Nano),
			}); err != nil {
				t.Fatalf("seed %s: %v", message.id, err)
			}
			if stored, err := store.ApplyReactionFact(ctx,
				reactionFact(message.id, "\U0001F44D", peer, domain.ReactionSet, 1), now); err != nil || !stored {
				t.Fatalf("react on %s: stored=%v err=%v", message.id, stored, err)
			}
		}
		if held, err := store.HoldReactionFact(ctx,
			reactionFact(absent, "\U0001F525", peer, domain.ReactionSet, 2), now); err != nil || !held {
			t.Fatalf("hold: held=%v err=%v", held, err)
		}
		return store, ctx, now
	}

	for _, path := range []struct {
		name string
		// run performs the deletion.
		run func(t *testing.T, store *Store, ctx context.Context, now time.Time)
		// survivors are the messages that must KEEP their reactions.
		survivors []string
		// heldKept says whether the fact waiting for a message that never
		// arrived is still legitimately waiting after this path ran.
		heldKept bool
	}{
		{
			name: "one message by id",
			run: func(t *testing.T, store *Store, ctx context.Context, _ time.Time) {
				if _, err := store.DeleteByID(ctx, ordinary); err != nil {
					t.Fatalf("delete: %v", err)
				}
			},
			survivors: []string{immutable, expiring},
			heldKept:  true,
		},
		{
			name: "one message with a tombstone",
			run: func(t *testing.T, store *Store, ctx context.Context, now time.Time) {
				if _, err := store.DeleteMessageWithTombstone(ctx, ordinary, now.Add(time.Hour)); err != nil {
					t.Fatalf("delete: %v", err)
				}
			},
			survivors: []string{immutable, expiring},
			heldKept:  true,
		},
		{
			name: "the TTL sweep",
			run: func(t *testing.T, store *Store, ctx context.Context, _ time.Time) {
				if _, err := store.DeleteExpired(ctx); err != nil {
					t.Fatalf("expire: %v", err)
				}
			},
			survivors: []string{ordinary, immutable},
			heldKept:  true,
		},
		{
			name: "the conversation wipe",
			run: func(t *testing.T, store *Store, ctx context.Context, now time.Time) {
				ids, err := store.ConversationCandidateIDs(ctx, peer)
				if err != nil {
					t.Fatalf("scope: %v", err)
				}
				if _, err := store.DeleteConversationWithIntents(ctx, peer, ids,
					ConversationWipeClassification{}, now, now.Add(time.Hour)); err != nil {
					t.Fatalf("wipe: %v", err)
				}
			},
			// The immutable one is kept BY DESIGN, so its reaction is kept too.
			survivors: []string{immutable},
		},
		{
			name: "removing the contact",
			run: func(t *testing.T, store *Store, ctx context.Context, _ time.Time) {
				if _, err := store.DeleteByPeer(ctx, peer); err != nil {
					t.Fatalf("remove: %v", err)
				}
			},
			survivors: nil,
		},
	} {
		t.Run(path.name, func(t *testing.T) {
			store, ctx, now := seed(t)
			path.run(t, store, ctx, now)

			// Half one: nothing may be left naming a message this conversation
			// does not have — except a fact still legitimately waiting, when the
			// conversation itself survived. Held rows are invisible to every
			// ordinary read, so they are counted through the sweep with the TTL
			// already past.
			waiting, err := store.SweepHeldReactions(ctx, now.Add(2*HeldReactionTTL))
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			want := 0
			if path.heldKept {
				want = 1
			}
			if waiting != want {
				t.Fatalf("%d reactions are waiting for a message that never came, want %d", waiting, want)
			}
			facts, err := store.ReactionsAuthoredBy(ctx, peer, scope, 64, 0)
			if err != nil {
				t.Fatalf("read what is left: %v", err)
			}
			for _, fact := range facts {
				if _, found, err := store.EntryByID(ctx, fact.Key.MessageID); err != nil {
					t.Fatalf("look for %s: %v", fact.Key.MessageID, err)
				} else if !found {
					t.Fatalf("a reaction on %s outlived its message", fact.Key.MessageID)
				}
			}

			// Half two: what survived kept its reactions.
			for _, id := range path.survivors {
				if _, found, err := store.EntryByID(ctx, domain.MessageID(id)); err != nil || !found {
					t.Fatalf("the fixture expected %s to survive: found=%v err=%v", id, found, err)
				}
				if kept := reactionsOn(t, store, domain.MessageID(id), self); len(kept) != 1 {
					t.Fatalf("%s survived but shows %d reactions, want the one it had", id, len(kept))
				}
			}
		})
	}
}
