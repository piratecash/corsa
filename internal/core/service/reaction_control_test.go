package service

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// reactionControlFixture is a real chatlog behind the adapter: what is being
// checked here is the apply-or-hold DECISION, and that decision is made by
// asking the database whether the message is there.
func reactionControlFixture(t *testing.T, self domain.PeerIdentity) (*ReactionControlAdapter, *chatlog.Store, *ebus.Bus) {
	t.Helper()
	adapter, store, bus, _ := reactionControlFixtureWithRefusals(t, self)
	return adapter, store, bus
}

// reactionControlFixtureWithRefusals also hands back the deletion gate, so a
// test can say "this id was deleted here" the way the real deletion path does.
func reactionControlFixtureWithRefusals(
	t *testing.T,
	self domain.PeerIdentity,
) (*ReactionControlAdapter, *chatlog.Store, *ebus.Bus, *wipeTombstoneSet) {
	t.Helper()
	store := newTestChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	refusals := newWipeTombstoneSet(func() wipeTombstoneJournal { return store })
	refusals.Hydrate(context.Background(), time.Now().UTC())
	adapter := NewReactionControlAdapter(
		NewChatlogGateway(store, self), refusals, newRemovalGate(), bus, nil)
	return adapter, store, bus, refusals
}

func controlIdentity(prefix string) domain.PeerIdentity {
	return domain.PeerIdentityFromWire(strings.Repeat(prefix, 20))
}

func remoteFact(sender domain.PeerIdentity, id, emoji string, clock uint64) domain.ReactionFact {
	return domain.ReactionFact{
		Scope: domain.ReactionScopeForPeer(sender),
		Key:   domain.ReactionKey{MessageID: domain.MessageID(id), Actor: sender, Emoji: emoji},
		Op:    domain.ReactionSet,
		Clock: domain.ReactionClock(clock),
	}
}

// A reaction can arrive before the message it is about — nothing orders the two
// and they travel by different paths. Dropping it would lose it for good: the
// sender has no reason to repeat a fact it believes delivered.
func TestAFactAheadOfItsMessageWaitsForIt(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("22")
	adapter, store, _ := reactionControlFixture(t, self)
	ctx := context.Background()
	// A conversation has to exist before a fact may WAIT in it — otherwise the
	// held rows are bounded per identity only, and identities are free (§9.5).
	appendMessage(t, store, self, sender, "earlier")

	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "m1", "👍", 1),
	}); err != nil {
		t.Fatalf("apply ahead of the message: %v", err)
	}
	shown := reactionsOn(t, store, "m1", self)
	if len(shown) != 0 {
		t.Fatal("a reaction is showing on a message this node does not have")
	}

	// The message lands. Releasing is what MessageStoreAdapter does on an
	// insert; the point checked here is that the held fact is still there to
	// release.
	appendMessage(t, store, self, sender, "m1")
	released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScopeForPeer(sender), "m1", time.Now().UTC())
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released != 1 {
		t.Fatalf("released %d facts, want the one that was waiting", released)
	}
	if shown = reactionsOn(t, store, "m1", self); len(shown) != 1 {
		t.Fatalf("after release: %d reactions", len(shown))
	}
}

// A fact about a message this node already has is applied outright and wakes
// the UI. Both halves matter: the chips are drawn from an in-memory cache, so a
// write nobody is told about is a reaction the user does not see until they
// switch conversations.
func TestAFactAboutAKnownMessageShowsAndWakesTheUI(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("33")
	adapter, store, bus := reactionControlFixture(t, self)
	ctx := context.Background()
	appendMessage(t, store, self, sender, "m2")

	woken := make(chan domain.PeerIdentity, 1)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) { woken <- peer })

	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "m2", "🔥", 1),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if shown := reactionsOn(t, store, "m2", self); len(shown) != 1 {
		t.Fatalf("the fact did not show: %d reactions", len(shown))
	}
	select {
	case peer := <-woken:
		if peer != sender {
			t.Fatalf("the UI was told about %s, want %s", peer, sender)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the UI was never told the conversation changed")
	}
}

// The same batch twice is the same state: the layer promises zero or more
// deliveries, so a re-delivery must not double a count or wake the UI again.
func TestARepeatedBatchChangesNothing(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("44")
	adapter, store, bus := reactionControlFixture(t, self)
	ctx := context.Background()
	appendMessage(t, store, self, sender, "m3")

	wakes := make(chan struct{}, 4)
	bus.Subscribe(ebus.TopicReactionsChanged, func(domain.PeerIdentity) { wakes <- struct{}{} })

	batch := []domain.ReactionFact{remoteFact(sender, "m3", "👍", 1)}
	for range 2 {
		if err := adapter.ApplyReactionFacts(ctx, sender, batch); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if shown := reactionsOn(t, store, "m3", self); len(shown) != 1 || shown[0].Count() != 1 {
		t.Fatalf("a repeated batch changed the state: %#v", shown)
	}
	// Two publishes would be harmless but wrong: the second describes a change
	// that did not happen, and a reader that reloads on it reads for nothing.
	<-wakes
	select {
	case <-wakes:
		t.Fatal("a repeat of an already applied batch woke the UI again")
	case <-time.After(200 * time.Millisecond):
	}
}

// This is the door into the chatlog for anything a peer says. A fact attributed
// to somebody other than the peer that signed for it is refused here, because
// nothing downstream would ever notice a reaction stored under the wrong name.
func TestAFactNotAttributedToItsSenderIsRefused(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("55")
	stranger := controlIdentity("66")
	adapter, store, _ := reactionControlFixture(t, self)
	ctx := context.Background()
	appendMessage(t, store, self, sender, "m4")

	err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(stranger, "m4", "👍", 1),
	})
	if err == nil {
		t.Fatal("a fact in a third party's name was accepted")
	}
	facts, readErr := store.ReactionFacts(ctx, "m4")
	if readErr != nil {
		t.Fatalf("read: %v", readErr)
	}
	if len(facts) != 0 {
		t.Fatalf("the refused fact was stored anyway: %#v", facts)
	}
}

func appendMessage(t *testing.T, store *chatlog.Store, self, peer domain.PeerIdentity, id string) {
	t.Helper()
	if err := store.Append(context.Background(), "dm", self, chatlog.Entry{
		ID:        id,
		Sender:    peer.String(),
		Recipient: self.String(),
		Body:      "hi",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
}

// A reaction naming a message this node DELETED is dropped, not held. Storing
// it would rebuild exactly the metadata — who responded to what — that the
// deletion existed to destroy, and a held row naming a message that no longer
// exists is one nothing else can ever reach.
func TestAFactAboutADeletedMessageIsDropped(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("77")
	adapter, store, _, refusals := reactionControlFixtureWithRefusals(t, self)
	ctx := context.Background()
	now := time.Now().UTC()

	appendMessage(t, store, self, sender, "m6")
	if _, err := store.DeleteMessageWithTombstone(ctx, "m6", now.Add(time.Hour)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	refusals.Note(ctx, []domain.MessageID{"m6"}, now)

	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "m6", "👍", 1),
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	facts, err := store.ReactionFacts(ctx, "m6")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("a reaction on a deleted message was stored: %#v", facts)
	}
	// Including as a held row, which is the form that would have survived
	// everything: no deletion reaches a fact whose message is gone.
	released, err := store.ReleaseHeldReactions(ctx, domain.ReactionScopeForPeer(sender), "m6", now)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released != 0 {
		t.Fatalf("%d reactions on a deleted message were waiting to appear", released)
	}
}

// Facts already written before a batch fails are committed, so the UI is told
// about them. Reporting only the failure would leave chips in the database that
// nothing draws until the user leaves the conversation and comes back.
func TestAPartlyAppliedBatchStillWakesTheUI(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("88")
	stranger := controlIdentity("99")
	adapter, store, bus := reactionControlFixture(t, self)
	ctx := context.Background()
	appendMessage(t, store, self, sender, "m7")

	woken := make(chan domain.PeerIdentity, 1)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) { woken <- peer })

	err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "m7", "👍", 1),
		remoteFact(stranger, "m7", "🔥", 2),
	})
	if err == nil {
		t.Fatal("the batch's bad fact was accepted")
	}
	if shown := reactionsOn(t, store, "m7", self); len(shown) != 1 {
		t.Fatalf("the good fact did not land: %d reactions", len(shown))
	}
	select {
	case <-woken:
	case <-time.After(2 * time.Second):
		t.Fatal("a partly applied batch never told the UI about what did land")
	}
}

// reactionsOn is what one message shows: the facts, folded. Production reads a
// whole conversation at once (DMRouter.MessageReactions), so this pairing exists
// only where a test wants to talk about one message.
func reactionsOn(t *testing.T, store *chatlog.Store, id domain.MessageID, self domain.PeerIdentity) []domain.Reaction {
	t.Helper()
	facts, err := store.ReactionFacts(context.Background(), id)
	if err != nil {
		t.Fatalf("read reactions on %s: %v", id, err)
	}
	return chatlog.FoldReactions(facts, self)
}

// The INCOMING door takes the removal lease. A fact written while a wipe is
// running lands behind it as an APPLIED row naming a message that no longer
// exists — and no deletion path reaches that row afterwards: the per-message
// delete needs a message, the wipe by scope has already run, and the sweep only
// takes held rows.
//
// The local user's own tap goes through the other door; that one is pinned by
// TestALocalToggleIsRefusedWhileTheConversationIsBeingRemoved.
func TestReactionsAreRefusedWhileTheConversationIsBeingRemoved(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("a1")
	store := newTestChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	removals := newRemovalGate()
	adapter := NewReactionControlAdapter(NewChatlogGateway(store, self), nil, removals, bus, nil)
	ctx := context.Background()
	appendMessage(t, store, self, sender, "m10")

	// The wipe is in progress: begin() holds the conversation until its
	// returned function runs.
	finish := removals.begin(sender)

	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "m10", "👍", 1),
	}); err != nil {
		t.Fatalf("apply during a removal: %v", err)
	}
	if shown := reactionsOn(t, store, "m10", self); len(shown) != 0 {
		t.Fatalf("a fact was written into a conversation being removed: %#v", shown)
	}

	// And once the removal is over the door opens again.
	finish()
	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "m10", "👍", 2),
	}); err != nil {
		t.Fatalf("apply after the removal: %v", err)
	}
	if shown := reactionsOn(t, store, "m10", self); len(shown) != 1 {
		t.Fatalf("the door stayed shut after the removal: %d reactions", len(shown))
	}
}

// A database that cannot answer "is this message here" must not be read as "no".
// That answer sends the fact to the HELD path, where it occupies the actor's
// quota and is swept an hour later having never been shown.
func TestAnUnreadableChatlogIsNotReadAsAMissingMessage(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("a2")
	store := newClosedChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	adapter := NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, newRemovalGate(), bus, nil)

	err := adapter.ApplyReactionFacts(context.Background(), sender, []domain.ReactionFact{
		remoteFact(sender, "m11", "👍", 1),
	})
	if err == nil {
		t.Fatal("a dead database was read as 'the message is not here' and the fact was held")
	}
}

// A fact about a message we do not have, from someone we have never exchanged
// one with, is dropped rather than held. Holding is the only unbounded thing in
// this table, and the ceilings that bound it are per identity — which costs
// nothing to mint, so on their own they bound nothing.
func TestAStrangersFactAboutAnUnknownMessageIsNotHeld(t *testing.T) {
	self := controlIdentity("11")
	stranger := controlIdentity("b1")
	known := controlIdentity("b2")
	adapter, store, _ := reactionControlFixture(t, self)
	ctx := context.Background()

	if err := adapter.ApplyReactionFacts(ctx, stranger, []domain.ReactionFact{
		remoteFact(stranger, "unknown", "👍", 1),
	}); err != nil {
		t.Fatalf("apply from a stranger: %v", err)
	}
	if held := heldFactsWaiting(t, store); held != 0 {
		t.Fatalf("a stranger's fact was held: %d waiting", held)
	}

	// Someone we do talk to is still allowed to be early.
	appendMessage(t, store, self, known, "earlier")
	if err := adapter.ApplyReactionFacts(ctx, known, []domain.ReactionFact{
		remoteFact(known, "unknown", "🔥", 1),
	}); err != nil {
		t.Fatalf("apply from a known peer: %v", err)
	}
	if held := heldFactsWaiting(t, store); held != 1 {
		t.Fatalf("a known peer could not be early: %d waiting", held)
	}
}

// heldFactsWaiting counts what is still WAITING for its message, and takes it
// away in the process.
//
// Asked of the sweep rather than of a release, because a release now also
// requires the message to be here: using one as a probe for "was it held" would
// answer a different question. Destructive on purpose — each call reports what
// was waiting since the last one.
func heldFactsWaiting(t *testing.T, store *chatlog.Store) int {
	t.Helper()
	swept, err := store.SweepHeldReactions(context.Background(),
		time.Now().UTC().Add(2*chatlog.HeldReactionTTL))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	return swept
}

// The whole point of holding a fact is that the message arriving makes it
// visible — and that has to hold through the REAL path, MessageStoreAdapter's
// insert, not a hand-rolled call to ReleaseHeldReactions.
//
// It also pins the other half: the UI is told. The chips are drawn from a cache
// loaded once per conversation, so a release nobody announces is a reaction the
// user does not see until they leave the chat and come back — which is exactly
// the scenario holding the fact existed for.
func TestAHeldFactAppearsWhenItsMessageIsStored(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("c1")
	store := newTestChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	adapter := NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, newRemovalGate(), bus, nil)
	messages := NewMessageStoreAdapter(
		NewChatlogGateway(store, self),
		&identity.Identity{Address: self.String()}, nil, nil)
	messages.attachEventBus(bus)
	ctx := context.Background()

	// A conversation exists, so an early fact may wait.
	appendMessage(t, store, self, sender, "earlier")
	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "late", "👍", 1),
	}); err != nil {
		t.Fatalf("apply ahead of the message: %v", err)
	}
	if shown := reactionsOn(t, store, "late", self); len(shown) != 0 {
		t.Fatal("a reaction is showing on a message this node does not have")
	}

	woken := make(chan domain.PeerIdentity, 1)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) { woken <- peer })

	// The message lands through the door the node really uses.
	result := messages.StoreMessage(protocol.Envelope{
		ID: "late", Topic: "dm", Sender: sender.String(), Recipient: self.String(),
		Payload: []byte("hi"), Flag: protocol.MessageFlagSenderDelete, CreatedAt: time.Now().UTC(),
	}, false)
	if result != node.StoreInserted {
		t.Fatalf("the message was not inserted: %v", result)
	}

	if shown := reactionsOn(t, store, "late", self); len(shown) != 1 {
		t.Fatalf("the held fact did not appear when its message landed: %d reactions", len(shown))
	}
	select {
	case peer := <-woken:
		if peer != sender {
			t.Fatalf("the UI was told about %s, want %s", peer, sender)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the release never told the UI: the chip waits for a conversation change")
	}
}

// A re-delivery of a message releases too, and that is the recovery path rather
// than a nicety. The release is best-effort — its error is logged and the
// message is still stored — so a fact can be left pending on a node that HAS the
// message. Nothing else would ever come back for it: a repeat of the reaction at
// the same clock changes no decision, and the sweep would take it an hour later.
//
// The held row here stands in for that lost release; what is pinned is that the
// next copy of the message finds it.
func TestARedeliveredMessageReleasesWhatAnEarlierPassLeftPending(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("d1")
	store := newTestChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	messages := NewMessageStoreAdapter(
		NewChatlogGateway(store, self),
		&identity.Identity{Address: self.String()}, nil, nil)
	messages.attachEventBus(bus)
	ctx := context.Background()

	envelope := protocol.Envelope{
		ID: "m1", Topic: "dm", Sender: sender.String(), Recipient: self.String(),
		Payload: []byte("hi"), Flag: protocol.MessageFlagSenderDelete, CreatedAt: time.Now().UTC(),
	}
	if got := messages.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("first store: %v", got)
	}
	// The state a lost release leaves behind: a pending fact on a message that
	// is already here.
	if _, err := store.HoldReactionFact(ctx, remoteFact(sender, "m1", "👍", 1), time.Now().UTC()); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 0 {
		t.Fatal("the fixture did not leave the fact pending")
	}

	if got := messages.StoreMessage(envelope, false); got != node.StoreDuplicate {
		t.Fatalf("the re-delivery was not classified as a duplicate: %v", got)
	}
	if shown := reactionsOn(t, store, "m1", self); len(shown) != 1 {
		t.Fatalf("a re-delivery did not release what was pending: %d reactions", len(shown))
	}
}

// The refusal of a deleted id and the re-offer of a reaction run on different
// clocks, and until this was fixed the difference was a loop with no end.
//
// The tombstone is sized by the sender's MESSAGE reseed horizon — past a week
// nobody re-sends the envelope, so a refusal has nothing left to refuse. A
// reaction has no such horizon: a peer offers the facts it holds for as long as
// it holds them. So the tombstone expired first, the next offer was taken as a
// pending fact, the sweep dropped it an hour later, and the offer after that
// put it back — for ever, each round re-creating a row that names a message the
// user destroyed.
//
// What ends it is knowledge about the id that outlives the tombstone. The
// second half of the test is the one that matters: a fresh process, whose
// memory of the deletion can only come from the database.
func TestAnOfferForADeletedMessageIsStillRefusedAfterItsTombstoneExpires(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("b9")
	store := newTestChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	ctx := context.Background()

	start := time.Now().UTC()
	now := start
	refusals := newWipeTombstoneSet(func() wipeTombstoneJournal { return store })
	refusals.Hydrate(ctx, now)
	adapter := NewReactionControlAdapter(
		NewChatlogGateway(store, self), refusals, newRemovalGate(), bus,
		func() time.Time { return now })

	// The conversation stays — only the one message goes. A wipe would be a
	// different (and already closed) path: with no conversation left, the whole
	// batch is refused at the door.
	appendMessage(t, store, self, sender, "kept")
	appendMessage(t, store, self, sender, "gone")
	if _, err := store.DeleteMessageWithTombstone(ctx, "gone", now.Add(wipeTombstoneTTL)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	refusals.Note(ctx, []domain.MessageID{"gone"}, now)

	// The peer reacts AFTER the deletion, so nothing at delete time could have
	// recorded that this id carries reactions. The live tombstone is the only
	// thing that knows, and this offer is its one chance to say so.
	now = start.Add(time.Hour)
	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "gone", "👍", 1),
	}); err != nil {
		t.Fatalf("apply while the tombstone is alive: %v", err)
	}

	// Two weeks on, in a process that never saw the deletion happen.
	now = start.Add(2 * wipeTombstoneTTL)
	reloaded := newWipeTombstoneSet(func() wipeTombstoneJournal { return store })
	reloaded.Hydrate(ctx, now)
	if refused, _ := reloaded.Refuses("gone", now); refused {
		t.Fatal("the fixture proves nothing: the message tombstone is still refusing this id")
	}
	restarted := NewReactionControlAdapter(
		NewChatlogGateway(store, self), reloaded, newRemovalGate(), bus,
		func() time.Time { return now })
	if err := adapter.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "gone", "👍", 1),
	}); err != nil {
		t.Fatalf("apply after the tombstone expired: %v", err)
	}
	if err := restarted.ApplyReactionFacts(ctx, sender, []domain.ReactionFact{
		remoteFact(sender, "gone", "👍", 1),
	}); err != nil {
		t.Fatalf("apply in the restarted process: %v", err)
	}

	if shown := reactionsOn(t, store, "gone", self); len(shown) != 0 {
		t.Fatalf("a reaction on a deleted message is showing: %#v", shown)
	}
	// The sweep runs on the test's clock, not the wall one: the rows this is
	// looking for were written two weeks into the future, and a sweep measured
	// from now() would find nothing and pass while the loop is wide open.
	waiting, err := store.SweepHeldReactions(ctx, now.Add(2*chatlog.HeldReactionTTL))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if waiting != 0 {
		t.Fatalf("%d reactions on a deleted message were waiting for it to come back", waiting)
	}
}

// The case nothing at delete time could have seen: the message went while it
// had no reactions, the node was down or the peer silent for longer than the
// tombstone lasts, and the FIRST offer ever arrives with nothing left to answer
// it. That offer is held — from here it is indistinguishable from a fact whose
// message is merely late — and what must not happen is the next one being held
// too, and the one after that, for ever.
//
// The sweep is what ends it. The router runs it on a timer (sweepHeldReactions);
// this drives the same store call directly, on the test's clock.
func TestTheFirstOfferAfterTheTombstoneIsHeldOnceAndThenRefused(t *testing.T) {
	self := controlIdentity("11")
	sender := controlIdentity("b9")
	store := newTestChatlogStore(t, self)
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	ctx := context.Background()

	start := time.Now().UTC()
	now := start
	refusals := newWipeTombstoneSet(func() wipeTombstoneJournal { return store })
	refusals.Hydrate(ctx, now)
	appendMessage(t, store, self, sender, "kept")
	appendMessage(t, store, self, sender, "gone")
	if _, err := store.DeleteMessageWithTombstone(ctx, "gone", now.Add(wipeTombstoneTTL)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	refusals.Note(ctx, []domain.MessageID{"gone"}, now)

	// Two weeks later. Nothing has ever been offered for this id, so no refusal
	// could have been recorded on the way.
	now = start.Add(2 * wipeTombstoneTTL)
	reloaded := newWipeTombstoneSet(func() wipeTombstoneJournal { return store })
	reloaded.Hydrate(ctx, now)
	if refused, _ := reloaded.Refuses("gone", now); refused {
		t.Fatal("the fixture proves nothing: the message tombstone is still refusing this id")
	}
	restarted := NewReactionControlAdapter(
		NewChatlogGateway(store, self), reloaded, newRemovalGate(), bus,
		func() time.Time { return now })

	offer := []domain.ReactionFact{remoteFact(sender, "gone", "👍", 1)}
	if err := restarted.ApplyReactionFacts(ctx, sender, offer); err != nil {
		t.Fatalf("first offer: %v", err)
	}
	// It waits, and that is correct: at this point the node cannot tell a
	// deleted message from one that has not arrived yet.
	swept, err := store.SweepHeldReactions(ctx, now.Add(2*chatlog.HeldReactionTTL))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("the sweep took %d facts, want the one that was waiting", swept)
	}

	// Everything after the sweep is answered without a row.
	now = now.Add(4 * chatlog.HeldReactionTTL)
	for i := range 3 {
		if err := restarted.ApplyReactionFacts(ctx, sender, offer); err != nil {
			t.Fatalf("offer %d after the sweep: %v", i, err)
		}
	}
	if shown := reactionsOn(t, store, "gone", self); len(shown) != 0 {
		t.Fatalf("a reaction on a deleted message is showing: %#v", shown)
	}
	again, err := store.SweepHeldReactions(ctx, now.Add(2*chatlog.HeldReactionTTL))
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if again != 0 {
		t.Fatalf("%d reactions were waiting again: the hold-and-sweep loop is still turning", again)
	}
}

// A sender with no conversation here is answered for the WHOLE batch at once:
// nothing of it is held, and nothing of it is looked up one fact at a time. The
// count of reads is not observable from here — the adapter holds a concrete
// store — so what this pins is the outcome the early return must not change.
func TestAStrangersWholeBatchIsDropped(t *testing.T) {
	self := controlIdentity("11")
	stranger := controlIdentity("b7")
	adapter, store, _ := reactionControlFixture(t, self)
	ctx := context.Background()

	facts := make([]domain.ReactionFact, 0, 32)
	for i := range 32 {
		facts = append(facts, remoteFact(stranger, "m"+strconv.Itoa(i), "👍", uint64(i+1)))
	}
	if err := adapter.ApplyReactionFacts(ctx, stranger, facts); err != nil {
		t.Fatalf("apply from a stranger: %v", err)
	}
	if held := heldFactsWaiting(t, store); held != 0 {
		t.Fatalf("a stranger's batch left %d facts waiting", held)
	}
	for i := range 32 {
		if shown := reactionsOn(t, store, domain.MessageID("m"+strconv.Itoa(i)), self); len(shown) != 0 {
			t.Fatalf("a stranger's fact on m%d was applied: %#v", i, shown)
		}
	}
}
