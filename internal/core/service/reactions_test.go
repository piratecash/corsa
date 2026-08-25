package service

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// fakeReactionStore is the store surface without a database, so the toggle's
// DECISION can be tested apart from how it is persisted.
type fakeReactionStore struct {
	facts   []domain.ReactionFact
	clock   domain.ReactionClock
	applied []domain.ReactionFact
	// superseded makes every write lose, which is what a concurrent decision on
	// the same key looks like from here.
	superseded bool
}

func (f *fakeReactionStore) ApplyReactionFact(_ context.Context, fact domain.ReactionFact, _ time.Time) (bool, error) {
	if f.superseded {
		// What the real store reports when a concurrent decision took the clock
		// value this one computed: the row is there, but not the one we wrote.
		return false, nil
	}
	f.applied = append(f.applied, fact)
	f.facts = append(f.facts, fact)
	if fact.Clock >= f.clock {
		f.clock = fact.Clock
	}
	return true, nil
}

func (f *fakeReactionStore) ReactionFacts(context.Context, domain.MessageID) ([]domain.ReactionFact, error) {
	return f.facts, nil
}

func (f *fakeReactionStore) ReactionsForScope(context.Context, domain.ReactionScope, domain.PeerIdentity) (map[domain.MessageID][]domain.Reaction, error) {
	return nil, nil
}

func (f *fakeReactionStore) NextReactionClock(context.Context, domain.PeerIdentity) (domain.ReactionClock, error) {
	return f.clock + 1, nil
}

func (f *fakeReactionStore) ReleaseHeldReactions(context.Context, domain.ReactionScope, domain.MessageID, time.Time) (int, error) {
	return 0, nil
}

func (f *fakeReactionStore) SweepHeldReactions(context.Context, time.Time) (int, error) {
	return 0, nil
}

func (f *fakeReactionStore) TrimReactionRefusals(context.Context, int) (int, error) {
	return 0, nil
}

func (f *fakeReactionStore) ReleaseArrivedReactions(context.Context, time.Time) ([]domain.ReactionScope, error) {
	return nil, nil
}

func (f *fakeReactionStore) ConversationsWithReactionsBy(
	context.Context, domain.PeerIdentity,
) ([]domain.ReactionScope, error) {
	return nil, nil
}

func (f *fakeReactionStore) ReactionFactsByKey(
	_ context.Context,
	scope domain.ReactionScope,
	actor domain.PeerIdentity,
	keys []domain.ReactionKey,
) ([]domain.ReactionFact, error) {
	var facts []domain.ReactionFact
	for _, key := range keys {
		for _, fact := range f.facts {
			if fact.Key == key && fact.Key.Actor == actor && fact.Scope == scope {
				facts = append(facts, fact)
			}
		}
	}
	return facts, nil
}

func reactionIdentity(prefix string) domain.PeerIdentity {
	return domain.PeerIdentityFromWire(strings.Repeat(prefix, 20))
}

// One tap means "the opposite of what I have now", and the opposite is decided
// against stored state rather than against what a caller believes: a caller's
// copy is a frame old, and two quick taps read from it would both decide "set".
func TestToggleReactionFlipsAgainstStoredState(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := &fakeReactionStore{}
	ctx := context.Background()
	now := time.Now().UTC()

	first, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", now)
	if err != nil {
		t.Fatalf("first toggle: %v", err)
	}
	if first.Op != domain.ReactionSet {
		t.Fatalf("first tap decided %v, want set", first.Op)
	}
	if first.Clock != 1 {
		t.Fatalf("first clock = %d, want 1", first.Clock)
	}

	second, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", now)
	if err != nil {
		t.Fatalf("second toggle: %v", err)
	}
	if second.Op != domain.ReactionCleared {
		t.Fatalf("second tap decided %v, want cleared", second.Op)
	}
	if second.Clock <= first.Clock {
		t.Fatalf("second decision (clock %d) does not supersede the first (clock %d)", second.Clock, first.Clock)
	}

	third, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", now)
	if err != nil {
		t.Fatalf("third toggle: %v", err)
	}
	if third.Op != domain.ReactionSet {
		t.Fatalf("third tap decided %v, want set again", third.Op)
	}
}

// Somebody else's reaction with the same emoji is not ours to clear, and their
// counter is not ours to continue.
func TestToggleReactionIgnoresOtherActors(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := &fakeReactionStore{facts: []domain.ReactionFact{{
		Key:   domain.ReactionKey{MessageID: "m1", Actor: peer, Emoji: "👍"},
		Op:    domain.ReactionSet,
		Clock: 40,
	}}}

	fact, err := toggleReactionWith(context.Background(), store, self, peer, "m1", "👍", time.Now().UTC())
	if err != nil {
		t.Fatalf("toggle: %v", err)
	}
	if fact.Op != domain.ReactionSet {
		t.Fatalf("their reaction cleared ours: decided %v, want set", fact.Op)
	}
	if fact.Key.Actor != self {
		t.Fatalf("the fact is attributed to %s, want this user", fact.Key.Actor)
	}
}

// Scope travels with the fact, because reconciliation runs per conversation and
// by then the message may be gone from this node.
func TestToggleReactionCarriesTheConversation(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := &fakeReactionStore{}

	fact, err := toggleReactionWith(context.Background(), store, self, peer, "m1", "🔥", time.Now().UTC())
	if err != nil {
		t.Fatalf("toggle: %v", err)
	}
	if want := domain.ReactionScopeForPeer(peer); fact.Scope != want {
		t.Fatalf("scope = %q, want %q", fact.Scope, want)
	}
}

func TestToggleReactionRefusesWhatItCannotAttribute(t *testing.T) {
	store := &fakeReactionStore{}
	ctx := context.Background()
	now := time.Now().UTC()
	peer := reactionIdentity("22")

	if _, err := toggleReactionWith(ctx, store, domain.PeerIdentity{}, peer, "m1", "👍", now); err == nil {
		t.Fatal("a node with no identity of its own recorded a reaction anyway")
	}
	if _, err := toggleReactionWith(ctx, store, reactionIdentity("11"), peer, "m1", "", now); err == nil {
		t.Fatal("an empty emoji was recorded as a reaction")
	}
	if len(store.applied) != 0 {
		t.Fatalf("%d refused decisions were written anyway", len(store.applied))
	}
}

// Reading, counting and writing are three statements with no transaction around
// them, so a decision made concurrently on the same key can take the clock value
// this one computed — and the merge keeps the higher clock. The loser has to say
// so: returning the fact anyway would hand the caller a decision this node does
// not hold and send it to the peer, leaving the two sides apart with nothing to
// notice it.
func TestAToggleThatLostTheWriteIsNotReportedAsMade(t *testing.T) {
	store := &fakeReactionStore{superseded: true}
	_, err := toggleReactionWith(
		context.Background(), store, reactionIdentity("11"), reactionIdentity("22"),
		"m1", "\U0001F44D", time.Now().UTC())
	if err == nil {
		t.Fatal("a superseded write was reported as a made decision")
	}
}

// The local user's own tap writes the same table a peer's facts do, so it takes
// the same removal lease. Without it the tap lands behind a running wipe as an
// applied row naming a message that no longer exists — and nothing reaches that
// row afterwards.
func TestALocalToggleIsRefusedWhileTheConversationIsBeingRemoved(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := newTestChatlogStore(t, self)
	client := &DesktopClient{id: &identity.Identity{Address: self.String()}, chatLog: store}
	client.wireSubServices()
	router := &DMRouter{client: client, removals: newRemovalGate()}
	ctx := context.Background()
	// A tap is about a message this node HAS: the store refuses an applied fact
	// whose message is gone, so without this the second half below would pass
	// for the wrong reason.
	appendMessage(t, store, self, peer, "m1")

	// The wipe is in progress: begin() holds the conversation until its
	// returned function runs.
	finish := router.removals.begin(peer)

	_, err := router.ToggleReaction(ctx, peer, "m1", "👍", time.Now().UTC())
	if !errors.Is(err, ErrConversationDeleteInflight) {
		t.Fatalf("a tap during a wipe returned %v, want ErrConversationDeleteInflight", err)
	}
	facts, readErr := store.ReactionFacts(ctx, "m1")
	if readErr != nil {
		t.Fatalf("read: %v", readErr)
	}
	if len(facts) != 0 {
		t.Fatalf("the tap was written into a conversation being removed: %#v", facts)
	}

	// And once the removal is over the door opens again.
	finish()
	if _, err := router.ToggleReaction(ctx, peer, "m1", "👍", time.Now().UTC()); err != nil {
		t.Fatalf("a tap after the removal: %v", err)
	}
	if facts, readErr = store.ReactionFacts(ctx, "m1"); readErr != nil || len(facts) != 1 {
		t.Fatalf("the door stayed shut: %d facts, err=%v", len(facts), readErr)
	}
}

// reofferPacer is the adapter with nothing but its clock and a jitter that does
// not jitter: what is under test is the pacing rule, and a random gap can only
// be asserted about more weakly than the rule itself.
func reofferPacer(now *time.Time) *ReactionControlAdapter {
	pacer := NewReactionControlAdapter(nil, nil, nil, nil, func() time.Time { return *now })
	pacer.jitter = func(interval time.Duration) time.Duration { return interval }
	return pacer
}

// Re-offers get rarer and NEVER stop. Nothing on this transport reports that a
// fact arrived, so "the peer already has it" is not a state this side can reach;
// and a peer reached only through transit may never open a session, so an end to
// the retries is an end to the delivery for that pair.
func TestReoffersWidenAndNeverStop(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	pacer := reofferPacer(&now)
	scope := domain.ReactionScopeForPeer(controlIdentity("77"))

	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the first pass over a conversation was refused")
	}
	// The gap actually waited is the shortest interval, not twice it.
	now = now.Add(ReofferMinInterval - time.Second)
	if pacer.claimReofferSlot(scope) {
		t.Fatal("a pass ran before the first gap was up")
	}
	now = now.Add(time.Second)
	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the pass due after the shortest gap was refused")
	}
	// And the next gap is twice as long: at exactly one more minimum it is
	// still too early.
	now = now.Add(ReofferMinInterval)
	if pacer.claimReofferSlot(scope) {
		t.Fatal("the gap did not widen after a pass")
	}
	now = now.Add(ReofferMinInterval)
	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the pass due after the widened gap was refused")
	}

	// A month of passes later it is still offering, at the widest gap and no
	// wider.
	for range 400 {
		now = now.Add(ReofferMaxInterval)
		if !pacer.claimReofferSlot(scope) {
			t.Fatalf("the re-offers stopped: nothing due %v after the last pass", ReofferMaxInterval)
		}
	}
}

// A local decision is news the peer demonstrably does not have — the one thing
// this side can know without an acknowledgement — so it takes the conversation
// back to the shortest gap instead of leaving it at the widest.
func TestANewDecisionOffersAgainAtOnce(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	pacer := reofferPacer(&now)
	scope := domain.ReactionScopeForPeer(controlIdentity("78"))

	// Walk the backoff out to its widest.
	for range 10 {
		now = now.Add(ReofferMaxInterval)
		if !pacer.claimReofferSlot(scope) {
			t.Fatal("a pass at the widest gap was refused")
		}
	}
	now = now.Add(ReofferMinInterval)
	if pacer.claimReofferSlot(scope) {
		t.Fatal("the conversation was still at the shortest gap")
	}

	pacer.RestartReoffer(scope)
	if !pacer.claimReofferSlot(scope) {
		t.Fatal("a fresh decision did not offer the conversation at once")
	}
	now = now.Add(ReofferMinInterval)
	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the gap after a fresh decision was wider than the shortest")
	}
}

// The pacing state is keyed by conversation and nothing else prunes it: the
// database stops returning a removed conversation, so without this the entry
// would sit in memory until the process ended.
func TestForgettingAConversationDropsItsReofferState(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	pacer := reofferPacer(&now)
	scope := domain.ReactionScopeForPeer(controlIdentity("79"))

	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the first pass over a conversation was refused")
	}
	if got := len(pacer.reoffer); got != 1 {
		t.Fatalf("re-offer states = %d, want 1", got)
	}

	pacer.ForgetConversation(scope)
	if got := len(pacer.reoffer); got != 0 {
		t.Fatalf("re-offer states after forgetting = %d, want 0", got)
	}
}

// The periodic pass reads a conversation's facts and queues them: two moments,
// and a removal that starts between them finds nothing to clean — the rows are
// gone, but a copy of them is already in the node's outbox, addressed to a
// contact the user has erased. So the pass holds the same lease the incoming
// door does, across both.
func TestThePeriodicReofferStandsAsideForARemoval(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := newTestChatlogStore(t, self)
	client := &DesktopClient{id: &identity.Identity{Address: self.String()}, chatLog: store}
	client.wireSubServices()
	removals := newRemovalGate()
	client.reactionControl = NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, removals, nil, nil)
	router := &DMRouter{client: client, removals: removals}
	control := client.reactionControl
	ctx := context.Background()
	// A fact of our own to offer: a conversation with nothing in it is not
	// paced at all, so without this the second half would pass for the wrong
	// reason.
	appendMessage(t, store, self, peer, "m1")
	if _, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", time.Now().UTC()); err != nil {
		t.Fatalf("toggle: %v", err)
	}

	finish := removals.begin(peer)
	router.reofferConversation(ctx, control, peer)
	if got := len(control.reoffer); got != 0 {
		t.Fatalf("the pass went ahead during a removal: %d conversations paced", got)
	}

	// And with the removal over it runs, so the gate is what stopped it and not
	// something else about the fixture.
	finish()
	router.reofferConversation(ctx, control, peer)
	if got := len(control.reoffer); got != 1 {
		t.Fatalf("the pass did not run after the removal: %d conversations paced", got)
	}
}

// The session path reads and queues in the same two moments the periodic one
// does — the node queues inside the callback — so it stands aside for a removal
// on the same lease.
func TestTheSessionReofferStandsAsideForARemoval(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := newTestChatlogStore(t, self)
	removals := newRemovalGate()
	control := NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, removals, nil, nil)
	ctx := context.Background()

	// A fact of our own to offer, so an empty conversation is not what makes
	// this pass.
	appendMessage(t, store, self, peer, "m1")
	if _, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", time.Now().UTC()); err != nil {
		t.Fatalf("toggle: %v", err)
	}

	offered := 0
	finish := removals.begin(peer)
	if err := control.ReactionsToReoffer(ctx, peer, func([]domain.ReactionFact) error {
		offered++
		return nil
	}); err != nil {
		t.Fatalf("re-offer during a removal: %v", err)
	}
	if offered != 0 {
		t.Fatal("a session re-offer handed over facts of a conversation being removed")
	}

	finish()
	if err := control.ReactionsToReoffer(ctx, peer, func([]domain.ReactionFact) error {
		offered++
		return nil
	}); err != nil {
		t.Fatalf("re-offer after the removal: %v", err)
	}
	if offered != 1 {
		t.Fatalf("the session re-offer handed over %d pages after the removal, want 1", offered)
	}
}

// The default spread keeps the gap near its interval but off any fixed rhythm:
// a padded frame hides its contents, not its addressee, and a conversation that
// wakes on the same second every time is a pattern regardless.
func TestTheReofferGapIsSpreadAroundItsInterval(t *testing.T) {
	const interval = time.Hour
	low, high := interval-interval/4, interval+interval/4

	seen := map[time.Duration]int{}
	for range 200 {
		gap := reofferJitter(interval)
		if gap < low || gap > high {
			t.Fatalf("a gap of %v fell outside %v..%v", gap, low, high)
		}
		seen[gap]++
	}
	if len(seen) < 2 {
		t.Fatal("every gap came out the same: the cadence is still a clock")
	}
}

// And the pacer actually spreads the gap rather than computing a spread it does
// not use: with a jitter that stretches, the next pass is due later.
func TestThePacerWaitsTheSpreadGap(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	pacer := reofferPacer(&now)
	pacer.jitter = func(interval time.Duration) time.Duration { return interval * 2 }
	scope := domain.ReactionScopeForPeer(controlIdentity("7a"))

	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the first pass over a conversation was refused")
	}
	now = now.Add(ReofferMinInterval)
	if pacer.claimReofferSlot(scope) {
		t.Fatal("the pass ran at the bare interval: the spread was computed and dropped")
	}
	now = now.Add(ReofferMinInterval)
	if !pacer.claimReofferSlot(scope) {
		t.Fatal("the pass due after the spread gap was refused")
	}
}

// A local failure is known synchronously, so it must not be paid for twice: the
// page it never offered stays next in line, and the gap stays short.
func TestALocalFailureCostsTheReofferNeitherItsPageNorItsSlot(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := newTestChatlogStore(t, self)
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	control := NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, newRemovalGate(), nil, func() time.Time { return now })
	control.jitter = func(interval time.Duration) time.Duration { return interval }
	ctx := context.Background()
	scope := domain.ReactionScopeForPeer(peer)

	// More facts than one page holds, so a cursor that moved would be visible:
	// with a single page the advance wraps straight back to zero and proves
	// nothing.
	for i := range ReofferPage + 1 {
		id := "m" + strconv.Itoa(i)
		appendMessage(t, store, self, peer, id)
		if _, err := toggleReactionWith(ctx, store, self, peer, domain.MessageID(id), "👍", now); err != nil {
			t.Fatalf("toggle %s: %v", id, err)
		}
	}

	// The node has nothing running to queue onto: the offer fails, and the
	// caller is told.
	refused := errors.New("nothing running to send on")
	if err := control.reofferDue(ctx, peer, func([]domain.ReactionFact) error {
		return refused
	}); !errors.Is(err, refused) {
		t.Fatalf("the failed offer returned %v, want the caller's own error", err)
	}
	if page := control.reofferPageAt(scope, ReofferPage+1); page != 0 {
		t.Fatalf("the cursor moved past a page that was never offered: now at %d", page)
	}

	// And the next pass is due at once rather than after a widened gap.
	var offered int
	if err := control.reofferDue(ctx, peer, func([]domain.ReactionFact) error {
		offered++
		return nil
	}); err != nil {
		t.Fatalf("the pass after the failure: %v", err)
	}
	if offered != 1 {
		t.Fatal("the pass after a local failure had to wait out a backoff it did not earn")
	}
	// And a page that WAS offered moves the cursor on, or the conversation
	// would keep offering its first page and never reach the rest.
	if page := control.reofferPageAt(scope, ReofferPage+1); page != ReofferPage {
		t.Fatalf("the cursor is at %d after an offered page, want %d", page, ReofferPage)
	}
}

// One conversation leaves state in three places outside the database, and
// forgetting it has to take all three: the node's send queue, what this node
// believes about the peer's build, and the re-offer cursor. The first two live
// in the node and are pinned there
// (TestForgettingAPeerEmptiesWhatWeWereGoingToSayToThem); this is the third, and
// it is the one nothing else prunes — the database stops returning the
// conversation, but the entry would sit in memory until the process ended.
//
// It also pins that the single door reaches BOTH layers, which is the point of
// having one: a caller that had to remember two would eventually remember one.
func TestForgettingAConversationDropsWhatTheServiceLayerHolds(t *testing.T) {
	client, _ := newTestDesktopClientWithNode(t)
	peer := reactionIdentity("22")
	other := reactionIdentity("33")
	client.reactionControl = NewReactionControlAdapter(
		client.chatlog, client.wipeTombstones, client.removals, nil, nil)
	for _, target := range []domain.PeerIdentity{peer, other} {
		client.reactionControl.RestartReoffer(domain.ReactionScopeForPeer(target))
	}

	client.ForgetConversationState(peer)

	if _, held := client.reactionControl.reoffer[domain.ReactionScopeForPeer(peer)]; held {
		t.Fatal("the re-offer cursor of the removed conversation was kept")
	}
	if client.localNode.QueuedReactionsFor(peer) != 0 {
		t.Fatal("the removed conversation still has reactions waiting to be sent")
	}
	// One conversation only: removing a contact says nothing about another.
	if _, held := client.reactionControl.reoffer[domain.ReactionScopeForPeer(other)]; !held {
		t.Fatal("another conversation's re-offer cursor was dropped too")
	}
}

// A session with a peer this node has no conversation with must leave nothing
// behind. The re-offer runs on EVERY session, including with transit peers, and
// an identity costs nothing to mint: an entry made before knowing there is
// anything to offer is one no deletion path ever names again.
func TestASessionWithNothingToOfferLeavesNoTrace(t *testing.T) {
	self := reactionIdentity("11")
	store := newTestChatlogStore(t, self)
	control := NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, newRemovalGate(), nil, nil)
	ctx := context.Background()

	for i := range 50 {
		stranger := reactionIdentity(strconv.FormatInt(int64(40+i), 16))
		offered := 0
		if err := control.ReactionsToReoffer(ctx, stranger, func([]domain.ReactionFact) error {
			offered++
			return nil
		}); err != nil {
			t.Fatalf("re-offer to a stranger: %v", err)
		}
		if offered != 0 {
			t.Fatalf("a conversation that does not exist offered %d pages", offered)
		}
	}
	if got := len(control.reoffer); got != 0 {
		t.Fatalf("%d conversations are being paced after 50 sessions with strangers", got)
	}

	// A conversation that HAS something is paced, so the emptiness above is the
	// reason and not a re-offer that never runs.
	peer := reactionIdentity("22")
	appendMessage(t, store, self, peer, "m1")
	if _, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", time.Now().UTC()); err != nil {
		t.Fatalf("toggle: %v", err)
	}
	if err := control.ReactionsToReoffer(ctx, peer, func([]domain.ReactionFact) error { return nil }); err != nil {
		t.Fatalf("re-offer: %v", err)
	}
	if got := len(control.reoffer); got != 1 {
		t.Fatalf("%d conversations are being paced, want the one with facts in it", got)
	}

	// And when its last fact goes, the pacing entry goes with it: nothing else
	// would take it once the conversation stops being returned by the database.
	if _, err := store.DeleteByID(ctx, "m1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := control.ReactionsToReoffer(ctx, peer, func([]domain.ReactionFact) error { return nil }); err != nil {
		t.Fatalf("re-offer after the delete: %v", err)
	}
	if got := len(control.reoffer); got != 0 {
		t.Fatalf("%d conversations are still paced after their last fact went", got)
	}
}

// A session's re-offer is a pass like any other and books the next slot. It used
// to reset the backoff and send without taking a slot, so the conversation was
// left due immediately and the next reaper tick sent the same page again — for a
// conversation with one page, a straight duplicate a second later.
func TestASessionReofferBooksTheNextSlot(t *testing.T) {
	self := reactionIdentity("11")
	peer := reactionIdentity("22")
	store := newTestChatlogStore(t, self)
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	control := NewReactionControlAdapter(
		NewChatlogGateway(store, self), nil, newRemovalGate(), nil, func() time.Time { return now })
	control.jitter = func(interval time.Duration) time.Duration { return interval }
	ctx := context.Background()

	appendMessage(t, store, self, peer, "m1")
	if _, err := toggleReactionWith(ctx, store, self, peer, "m1", "👍", now); err != nil {
		t.Fatalf("toggle: %v", err)
	}

	offered := 0
	count := func([]domain.ReactionFact) error {
		offered++
		return nil
	}
	if err := control.ReactionsToReoffer(ctx, peer, count); err != nil {
		t.Fatalf("session re-offer: %v", err)
	}
	if offered != 1 {
		t.Fatalf("the session offered %d pages", offered)
	}

	// The reaper tick that follows must find the conversation booked.
	if err := control.reofferDue(ctx, peer, count); err != nil {
		t.Fatalf("the periodic pass: %v", err)
	}
	if offered != 1 {
		t.Fatal("the tick after a session re-offer sent the same page again")
	}

	// And it is due again after the shortest gap, not never.
	now = now.Add(ReofferMinInterval)
	if err := control.reofferDue(ctx, peer, count); err != nil {
		t.Fatalf("the pass after the gap: %v", err)
	}
	if offered != 2 {
		t.Fatal("the conversation stopped being offered after the session")
	}
}
