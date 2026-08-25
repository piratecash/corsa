package node

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/dmcontrol"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// echoControlStore is the conversation store the sender resolves keys against.
//
// The queue holds keys, so a fixture without a store sends nothing at all: this
// one answers "yes, that reaction exists and says SET", which is the ordinary
// case, and a test that wants a deleted reaction takes it out of `gone`.
type echoControlStore struct {
	mu   sync.Mutex
	gone map[domain.ReactionKey]bool
	// strangers are the peers this store says it has no conversation with. Empty
	// by default, because most tests are about a conversation that exists.
	strangers map[domain.PeerIdentity]bool
	// asked records the keys the sender resolved, so a test can show that the
	// values go on the wire from the RECORD and not from the queue.
	asked int
}

func (e *echoControlStore) ApplyReactionFacts(context.Context, domain.PeerIdentity, []domain.ReactionFact) error {
	return nil
}

func (e *echoControlStore) HasConversationWith(_ context.Context, peer domain.PeerIdentity) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.strangers[peer], nil
}

func (e *echoControlStore) ReactionsToReoffer(
	context.Context, domain.PeerIdentity, func([]domain.ReactionFact) error,
) error {
	return nil
}

func (e *echoControlStore) ReactionFactsFor(
	_ context.Context,
	peer domain.PeerIdentity,
	keys []domain.ReactionKey,
) ([]domain.ReactionFact, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.asked++
	facts := make([]domain.ReactionFact, 0, len(keys))
	for _, key := range keys {
		if e.gone[key] {
			continue
		}
		facts = append(facts, domain.ReactionFact{
			Scope: domain.ReactionScopeForPeer(peer),
			Key:   key,
			Op:    domain.ReactionSet,
			Clock: 1,
		})
	}
	return facts, nil
}

func (e *echoControlStore) forget(keys ...domain.ReactionKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gone == nil {
		e.gone = map[domain.ReactionKey]bool{}
	}
	for _, key := range keys {
		e.gone[key] = true
	}
}

// withEchoStore registers the store and hands it back for the tests that care.
func withEchoStore(svc *Service) *echoControlStore {
	store := &echoControlStore{gone: map[domain.ReactionKey]bool{}}
	svc.RegisterConversationControlStore(store)
	return store
}

// awaitRemovalStarted blocks until ForgetPeerReactions has done its clearing and
// is waiting for the frames in flight.
//
// The stamp it looks for is written in the SAME critical section as the
// clearing, before the wait begins, so seeing it proves the ordering the test
// depends on: the removal has already emptied everything it empties. A sleep
// proves nothing — a removal goroutine scheduled late would let the send finish
// first, and the test would pass with the race back in place.
func awaitRemovalStarted(t *testing.T, d *dmControlSender, peer domain.PeerIdentity) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d.mu.Lock()
		_, started := d.forgot[peer]
		d.mu.Unlock()
		if started {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the removal never began")
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitPauseStarted blocks until HoldReactionSends has raised the pause and is
// waiting for the frames in flight.
//
// Same rule as awaitRemovalStarted: the flag is raised in the critical section
// that precedes the wait, so seeing it proves the goroutine got there. Without
// it, "the delete has not returned within 150ms" is also what a delete that has
// not STARTED looks like.
func awaitPauseStarted(t *testing.T, svc *Service, peer domain.PeerIdentity) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !svc.ReactionSendsHeldFor(peer) {
		if time.Now().After(deadline) {
			t.Fatal("the delete never raised its pause")
		}
		time.Sleep(time.Millisecond)
	}
}

// controlSenderAt builds a sender on a clock the test moves by hand, so the
// debounce can be exercised without waiting for it.
func controlSenderAt(t *testing.T, now *time.Time, jitter time.Duration) *dmControlSender {
	t.Helper()
	// A real node behind it, because queueReactions refuses to accept anything
	// on a node whose plane is off — and that refusal is what keeps the outbox
	// bounded, so a fixture that bypassed it would test a sender that cannot
	// exist.
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	withEchoStore(svc)
	sender := svc.dmControl
	sender.clock = func() time.Time { return *now }
	sender.jitter = func() time.Duration { return jitter }
	// The loop raises this while it runs, and canSendLocked asks it. A fixture
	// that drives takeDue by hand is standing in for that loop, so it says so.
	sender.setDraining(true)
	return sender
}

// queuedFor and refusalCount read the sender's state the way the sender does,
// under its mutex. The fixtures below never start the loop, so a bare read is
// safe today — but this subsystem's whole contract is that the state is behind
// d.mu, and a test that reads around it teaches the opposite.
func queuedFor(d *dmControlSender, peer domain.PeerIdentity) *dmControlOutbox {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pending[peer]
}

func queuedPeers(d *dmControlSender) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

func refusalCount(d *dmControlSender) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.refusedAt)
}

func typeRefusedFor(d *dmControlSender, peer domain.PeerIdentity) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, held := d.refusedTypeAt[peer]
	return held
}

func refusalHeld(d *dmControlSender, key refusalKey) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, held := d.refusedAt[key]
	return held
}

func reactionFactFor(peer domain.PeerIdentity, id, emoji string, clock uint64) domain.ReactionFact {
	return domain.ReactionFact{
		Scope: domain.ReactionScopeForPeer(peer),
		Key:   domain.ReactionKey{MessageID: domain.MessageID(id), Actor: peer, Emoji: emoji},
		Op:    domain.ReactionSet,
		Clock: domain.ReactionClock(clock),
	}
}

func controlTestPeer(prefix string) domain.PeerIdentity {
	wire := ""
	for range 20 {
		wire += prefix
	}
	return domain.PeerIdentityFromWire(wire)
}

// Facts wait before they leave. The wait is what batches a burst into one frame
// and what stops the frame from being a timestamp of the tap that caused it.
func TestOutgoingFactsWaitBeforeTheyLeave(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 300*time.Millisecond)
	peer := controlTestPeer("aa")

	if err := sender.queueReactions(peer, []domain.ReactionFact{reactionFactFor(peer, "m1", "👍", 1)}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	if due, _ := sender.takeDue(now); len(due) != 0 {
		t.Fatal("a fact left immediately: the debounce did not hold it")
	}
	if due, _ := sender.takeDue(now.Add(dmControlDebounceFloor)); len(due) != 0 {
		t.Fatal("a fact left at the floor, ignoring its jitter")
	}
	due, _ := sender.takeDue(now.Add(dmControlDebounceFloor + 300*time.Millisecond))
	if len(due) != 1 || len(due[peer].entries) != 1 {
		t.Fatalf("the fact did not come due: %#v", due)
	}
	if queuedPeers(sender) != 0 {
		t.Fatal("a batch that came due was left in the outbox and would be sent twice")
	}
}

// Later facts join the batch already waiting and do NOT push its deadline back.
// An extended deadline is a debounce that never fires while the user keeps
// tapping — which is exactly when they most expect the reaction to have gone.
func TestABatchKeepsItsFirstDeadline(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("bb")

	if err := sender.queueReactions(peer, []domain.ReactionFact{reactionFactFor(peer, "m1", "👍", 1)}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	now = now.Add(dmControlDebounceFloor - time.Millisecond)
	if err := sender.queueReactions(peer, []domain.ReactionFact{reactionFactFor(peer, "m2", "🔥", 2)}); err != nil {
		t.Fatalf("queue again: %v", err)
	}

	due, _ := sender.takeDue(now.Add(time.Millisecond))
	if len(due) != 1 {
		t.Fatalf("the batch did not come due on its original deadline: %#v", due)
	}
	if got := len(due[peer].entries); got != 2 {
		t.Fatalf("the batch carries %d facts, want both", got)
	}
}

// Each peer has its own batch and its own deadline: one silent conversation
// must not hold up another.
func TestEachPeerHasItsOwnBatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	first, second := controlTestPeer("cc"), controlTestPeer("dd")

	if err := sender.queueReactions(first, []domain.ReactionFact{reactionFactFor(first, "m1", "👍", 1)}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	now = now.Add(dmControlDebounceFloor)
	if err := sender.queueReactions(second, []domain.ReactionFact{reactionFactFor(second, "m2", "🔥", 1)}); err != nil {
		t.Fatalf("queue: %v", err)
	}

	due, _ := sender.takeDue(now)
	if len(due) != 1 || due[first] == nil {
		t.Fatalf("the due batch is not the first peer's: %#v", due)
	}
	if queuedPeers(sender) != 1 || queuedFor(sender, second) == nil {
		t.Fatal("the second peer's batch did not stay waiting")
	}
}

// A fact that cannot be merged is refused at the door, where the caller can
// still be told, rather than at flush time where it could only be logged.
func TestQueueRefusesAFactThatCannotBeMerged(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("ee")

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		{Key: domain.ReactionKey{MessageID: "m1", Emoji: "👍"}},
	}); err == nil {
		t.Fatal("a fact with no clock was queued")
	}
	if err := sender.queueReactions(domain.PeerIdentity{}, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "👍", 1),
	}); err == nil {
		t.Fatal("facts were queued for nobody")
	}
	if queuedPeers(sender) != 0 {
		t.Fatal("a refused batch was queued anyway")
	}
}

// What we believe about a peer's build has to expire. The peer may update and
// nothing else would tell us; and a map keyed by peer identity that is only
// ever written to is the unbounded-growth shape this project has paid for
// before.
func TestARefusalIsForgottenAfterItsTTL(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("ff")

	key := refusalKey{peer: peer, command: domain.DMControlReactions}
	sender.markRefused(peer, domain.DMControlReactions)
	if !sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("the refusal was not remembered at all")
	}
	now = now.Add(dmControlUnsupportedTTL)
	if sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("the refusal outlived its TTL")
	}

	// The READ answers by freshness and leaves the row where it is: the sweep is
	// what removes it, and it is also what tells the UI the peer can receive
	// reactions again. A read that deleted made that news disappear — the sweep
	// then found nothing to report.
	if !refusalHeld(sender, key) {
		t.Fatal("a read removed the entry the sweep has to report on")
	}
	_, cleared := sender.takeDue(now)
	if refusalHeld(sender, key) {
		t.Fatal("an expired refusal nobody queried was never swept")
	}
	if len(cleared) != 1 || cleared[0] != peer {
		t.Fatalf("the sweep reported %v as able to receive reactions again, want %s", cleared, peer)
	}
}

// A refusal names ONE command. dm_control carries several by design, so a peer
// that cannot do deletions says nothing about whether it does reactions — and
// treating it as a blanket refusal would have the UI tell the user their
// reaction went nowhere on the strength of an unrelated answer.
func TestARefusalIsRememberedPerCommand(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("77")

	sender.markRefused(peer, "message_delete")
	if sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("a refusal of one command silenced another")
	}
	if !sender.refuses(peer, "message_delete") {
		t.Fatal("the refusal was not remembered against the command it named")
	}
}

// Queueing is refused where nothing will drain it. The drain is the send loop,
// which runs only between Run and its cancellation — and outside that window
// the outbox is not a delay but a map that grows for the life of the process.
//
// The node fixture here has the plane BUILT and the loop not running, which is
// the state a check on the layer handle would have got wrong: the handle is
// stored once and never cleared, so it stays non-nil before Run and after it.
func TestNothingIsQueuedWithoutAPlaneToSendOn(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	peer := controlTestPeer("88")
	if svc.datagramLayer() == nil {
		t.Fatal("the fixture must have a plane, or this test proves nothing")
	}

	if err := svc.QueueReactionFacts(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err == nil {
		t.Fatal("facts were queued on a node with no datagram plane")
	}
	if queuedPeers(svc.dmControl) != 0 {
		t.Fatal("the outbox grew on a node that will never drain it")
	}
}

// A refusal is remembered only for a command this build actually sends. The
// name in an incoming refusal is written by the PEER, so remembering every one
// it invents would be an hour-long map with a remote pen in it.
func TestARefusalIsIgnoredForACommandWeNeverSend(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	svc := sender.svc
	peer := controlTestPeer("9b")

	for i := range 100 {
		svc.noteCommandRefused(peer, domain.DMControlCommand(fmt.Sprintf("invented_%d", i)))
	}
	if got := refusalCount(sender); got != 0 {
		t.Fatalf("%d invented command names were remembered", got)
	}
	// One we really send is remembered, so the filter is not simply off.
	svc.noteCommandRefused(peer, domain.DMControlReactions)
	if !sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("a refusal of a command we do send was dropped too")
	}
}

// A peer inventing command names cannot make this node queue an answer per
// name: each queued refusal becomes a padded frame at flush.
func TestQueuedRefusalsAreCappedPerPeer(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	svc := sender.svc
	peer := controlTestPeer("9c")

	for i := range maxQueuedRefusalsPerPeer * 4 {
		svc.answerCommandUnsupported(peer, domain.DMControlCommand(fmt.Sprintf("invented_%d", i)))
	}
	outbox := queuedFor(sender, peer)
	if outbox == nil {
		t.Fatal("no answers were queued at all")
	}
	if len(outbox.refusals) != maxQueuedRefusalsPerPeer {
		t.Fatalf("queued %d answers, want the cap of %d", len(outbox.refusals), maxQueuedRefusalsPerPeer)
	}
}

// Facts are not offered to a peer whose build has already said it cannot read
// them: sealing, signing and enqueueing a frame for a certainty costs the
// network for nothing, and acting on the refusal is why it is remembered.
func TestFactsAreNotOfferedToAPeerThatRefusedThem(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("9d")

	sender.markRefused(peer, domain.DMControlReactions)
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "👍", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	// A refusal answer is a DIFFERENT command and still goes out; it is the
	// facts that are held back.
	sender.svc.answerCommandUnsupported(peer, "message_edit")

	sent, held := sender.framesFor(context.Background(), peer, queuedFor(sender, peer))
	if len(sent) != 1 {
		t.Fatalf("built %d frames, want only the refusal answer", len(sent))
	}
	if sent[0].command != domain.DMControlUnsupported {
		t.Fatalf("the surviving frame is %q", sent[0].command)
	}
	// The facts are HELD, not discarded: a peer that upgrades has to receive
	// what was made while it was old.
	if len(held) != 1 {
		t.Fatalf("%d facts were held for the refusing peer, want the one queued", len(held))
	}
}

// Stopping closes the door and drops what was still waiting, in one step.
//
// Dropping rather than sending is deliberate and argued on dmControlSendLoop:
// this loop and the outbound pump end together, so a frame sealed here would be
// handed to a queue with no reader left. What must NOT happen is the third
// thing — leaving the outbox behind a door nobody opens again, which is the
// shape that grows for the life of the process.
func TestStoppingClosesTheDoorAndDropsWhatWasWaiting(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("99")

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	if queuedPeers(sender) != 1 {
		t.Fatal("the batch was not queued; the test would prove nothing")
	}

	sender.stop()

	if queuedPeers(sender) != 0 {
		t.Fatal("the outbox survived the stop, behind a door nobody will open again")
	}
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m2", "\U0001F525", 2),
	}); err == nil {
		t.Fatal("facts were accepted after the loop stopped: nothing will ever send them")
	}
	if queuedPeers(sender) != 0 {
		t.Fatal("the outbox grew after the loop stopped")
	}
}

// controlSenderWithKey is a sender whose peer has a box key, so a frame can be
// sealed and reach the dispatch seam. Without one, sendOne stops before it.
func controlSenderWithKey(t *testing.T, now *time.Time, peer *identity.Identity) *dmControlSender {
	t.Helper()
	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	svc.addKnownBoxKey(peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey))
	withEchoStore(svc)
	sender := svc.dmControl
	sender.clock = func() time.Time { return *now }
	sender.jitter = func() time.Duration { return 0 }
	sender.setDraining(true)
	return sender
}

// The destination declared its dtypes at handshake and dm_control was not among
// them. Three things follow, and the third is the one an earlier cut got wrong.
//
//  1. It is remembered against the TYPE, not against whichever command was in
//     the frame: the gate answers before anything is opened, so it cannot know
//     what was inside, and a peer with no dm_control refuses every command in it.
//  2. The rest of the batch is not attempted — every further frame fails
//     identically right now.
//  3. The facts COME BACK. Not attempting is not discarding: this transport has
//     no retry of its own, so a peer that upgrades would otherwise never receive
//     what was made while it was old.
func TestAnUnsupportedDTypeIsRememberedAgainstTheType(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)

	attempts := 0
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		attempts++
		return dmControlDispatch{
			kind:      datagram.SendRejected,
			rejection: datagram.RejectionUnsupportedDType,
			summary:   "rejected",
		}
	}
	facts := manyOutgoingFacts(peer, 200)
	if err := sender.queueReactions(peer, facts); err != nil {
		t.Fatalf("queue: %v", err)
	}
	built, _ := sender.framesFor(context.Background(), peer, queuedFor(sender, peer))
	if len(built) < 2 {
		t.Fatalf("the batch is %d frame(s); it has to span several to prove the rest is not attempted", len(built))
	}
	sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))

	if attempts != 1 {
		t.Fatalf("the batch kept going after unsupported_dtype: %d frames attempted", attempts)
	}
	if !sender.cannotTakeReactions(peer) {
		t.Fatal("the transport's refusal of the type was not remembered")
	}
	if sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("a gate refusal about the TYPE was recorded as an inner refusal of a command")
	}

	// Everything is back in the outbox, waiting, and nothing was dropped.
	outbox := queuedFor(sender, peer)
	if outbox == nil {
		t.Fatal("the facts were discarded when the peer refused the type")
	}
	if len(outbox.entries) != len(facts) {
		t.Fatalf("%d of %d facts came back", len(outbox.entries), len(facts))
	}

	// While the peer refuses, they wait rather than being sealed and signed for
	// a certainty.
	attempts = 0
	sender.flushDue(context.Background(), now.Add(dmControlOutboxMaxAge/2))
	if attempts != 0 {
		t.Fatalf("%d frames were built for a peer known to refuse the type", attempts)
	}
	if got := len(queuedFor(sender, peer).entries); got != len(facts) {
		t.Fatalf("%d facts survived the pass that offered nothing", got)
	}

	// The peer's next session says the answer may have changed: the belief is
	// cleared, the batch becomes due, and what was waiting finally goes out.
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		attempts++
		return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
	}
	sender.svc.forgetDMControlRefusal(peer)
	sender.flushDue(context.Background(), now.Add(dmControlOutboxMaxAge/2))

	if attempts != len(built) {
		t.Fatalf("after the peer upgraded %d of %d frames went out", attempts, len(built))
	}
	if queuedPeers(sender) != 0 {
		t.Fatalf("facts stayed queued after they were delivered: %#v", queuedFor(sender, peer))
	}
}

// A batch that has been waiting longer than the outbox allows is dropped, with
// a bound stated rather than hidden. The queue lives in memory and the facts are
// already stored; a peer unreachable this long is one the digest reconciliation
// of §6.3 is meant to repair.
func TestTheOutboxGivesUpEventually(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("9f")

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "👍", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	queuedAt := now
	// No box key, so every pass fails to hand anything over and requeues.
	for now.Sub(queuedAt) < dmControlOutboxMaxAge-dmControlRetryDelay {
		now = now.Add(dmControlDebounceFloor + dmControlRetryDelay)
		sender.flushDue(context.Background(), now)
		if queuedPeers(sender) != 1 {
			t.Fatalf("the batch stopped being retried after %s", now.Sub(queuedAt))
		}
	}
	now = now.Add(dmControlOutboxMaxAge)
	sender.flushDue(context.Background(), now)
	if queuedPeers(sender) != 0 {
		t.Fatal("a batch older than the outbox allows is still being retried")
	}
}

// One peer's facts cannot grow without bound while it is unreachable: past the
// cap the OLDEST go, because the newest are the state the peer is missing.
func TestTheOutboxIsBoundedPerPeer(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("ac")

	// No box key, so nothing is ever handed over and everything requeues.
	for round := range 4 {
		batch := make([]domain.ReactionFact, 0, dmControlOutboxMaxKeys/2)
		for i := range dmControlOutboxMaxKeys / 2 {
			batch = append(batch, reactionFactFor(
				peer, fmt.Sprintf("%036d", round*dmControlOutboxMaxKeys+i), "👍",
				uint64(round*dmControlOutboxMaxKeys+i+1)))
		}
		if err := sender.queueReactions(peer, batch); err != nil {
			t.Fatalf("queue round %d: %v", round, err)
		}
		now = now.Add(dmControlDebounceFloor + dmControlRetryDelay)
		sender.flushDue(context.Background(), now)
	}

	outbox := queuedFor(sender, peer)
	if outbox == nil {
		t.Fatal("the outbox was emptied entirely")
	}
	if len(outbox.entries) > dmControlOutboxMaxKeys {
		t.Fatalf("the outbox holds %d facts, over the %d cap", len(outbox.entries), dmControlOutboxMaxKeys)
	}
	// The survivors are the NEWEST: the last reaction queued is still there.
	last := outbox.entries[len(outbox.entries)-1].key
	want := domain.MessageID(fmt.Sprintf("%036d", 3*dmControlOutboxMaxKeys+dmControlOutboxMaxKeys/2-1))
	if last.MessageID != want {
		t.Fatalf("the newest queued reaction is on %s, want %s — the cap dropped from the wrong end",
			last.MessageID, want)
	}
}

// A momentary "no route" says nothing about the peer's build and nothing about
// the NEXT frame. Collapsing it with a refusal is the regression the three-way
// verdict exists for: one such outcome on the first of several frames silently
// dropped all the rest, and there is no queue behind them.
func TestATransientFailureDoesNotAbandonTheBatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)

	attempts := 0
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		attempts++
		if attempts == 1 {
			return dmControlDispatch{kind: datagram.SendNoRoute, summary: "no_route"}
		}
		return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
	}
	facts := manyOutgoingFacts(peer, 200)
	if err := sender.queueReactions(peer, facts); err != nil {
		t.Fatalf("queue: %v", err)
	}
	built, _ := sender.framesFor(context.Background(), peer, queuedFor(sender, peer))
	want := len(built)
	if want < 2 {
		t.Fatalf("the batch is %d frame(s); it has to span several to prove anything", want)
	}
	sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))

	if attempts != want {
		t.Fatalf("attempted %d of %d frames after one no-route", attempts, want)
	}
	if sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("a no-route was remembered as the peer being unable to receive reactions")
	}
}

// Without the peer's box key there is nothing to seal to. That is a gap in what
// we know about them, not a refusal by them, so it must not be remembered as
// one — a peer whose key we later learn would otherwise stay silenced.
func TestAMissingBoxKeyIsNotRememberedAsARefusal(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("9e")

	reached := false
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		reached = true
		return dmControlDispatch{kind: datagram.SendQueued}
	}
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "👍", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))

	if reached {
		t.Fatal("a frame was dispatched for a peer whose box key we do not have")
	}
	if sender.refuses(peer, domain.DMControlReactions) {
		t.Fatal("a gap in what we know about a peer was remembered as their refusal")
	}
}

func manyOutgoingFacts(peer domain.PeerIdentity, n int) []domain.ReactionFact {
	facts := make([]domain.ReactionFact, 0, n)
	for i := range n {
		facts = append(facts, reactionFactFor(
			peer, fmt.Sprintf("%036d", i), "👍", uint64(i+1)))
	}
	return facts
}

// A session coming up is where delivery actually rests: nothing on this
// transport reports that a fact ARRIVED, so this node offers the conversation's
// own facts again every time it reconnects.
//
// This is what makes "until the peer upgrades" true. The in-memory queue gives
// up after dmControlOutboxMaxAge; the re-offer has no deadline, because it reads
// the durable record rather than a batch someone remembered to keep.
func TestASessionReoffersTheConversationsOwnReactions(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc

	// What the durable record holds for this conversation — including a fact
	// decided long ago, which a time-boxed re-announce would have dropped.
	store := &recordingControlStore{reoffer: []domain.ReactionFact{
		reactionFactFor(peer, "old", "👍", 1),
		reactionFactFor(peer, "new", "🔥", 2),
	}}
	svc.RegisterConversationControlStore(store)

	svc.reofferReactions(context.Background(), peer)

	outbox := queuedFor(sender, peer)
	if outbox == nil {
		t.Fatal("a session offered nothing: the facts have no other way to arrive")
	}
	if len(outbox.entries) != 2 {
		t.Fatalf("the session offered %d facts, want the conversation's own two", len(outbox.entries))
	}
}

// The outbox keeps ONE entry per reaction, and what that entry is worth is read
// from the record when the frame is built. Twenty taps on one reaction are one
// thing to send, and the value on the wire is the one that is true at send time
// rather than the one that was true when the tap happened.
func TestTheOutboxKeepsOneEntryPerReactionAndReadsItAtSendTime(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	store := withEchoStore(sender.svc)

	toggles := make([]domain.ReactionFact, 0, 20)
	for i := range 20 {
		fact := reactionFactFor(peer, "m1", "👍", uint64(i+1))
		if i%2 == 1 {
			fact.Op = domain.ReactionCleared
		}
		toggles = append(toggles, fact)
	}
	toggles = append(toggles, reactionFactFor(peer, "m2", "🔥", 99))
	if err := sender.queueReactions(peer, toggles); err != nil {
		t.Fatalf("queue: %v", err)
	}

	outbox := queuedFor(sender, peer)
	if len(outbox.entries) != 2 {
		t.Fatalf("the outbox holds %d entries, want one per reaction: %#v", len(outbox.entries), outbox.entries)
	}

	// The record is what the frame is built from: with one of the two reactions
	// deleted since it was queued, only the other is offered.
	store.forget(domain.ReactionKey{MessageID: "m1", Actor: peer, Emoji: "👍"})
	frames, held := sender.framesFor(context.Background(), peer, outbox)
	if len(held) != 0 {
		t.Fatalf("%d entries were held back by a peer that takes reactions", len(held))
	}
	if len(frames) != 1 {
		t.Fatalf("the batch became %d frames, want one", len(frames))
	}
	decoded, err := dmcontrol.Decode(frames[0].plain)
	if err != nil {
		t.Fatalf("decode our own frame: %v", err)
	}
	if len(decoded.Facts) != 1 || decoded.Facts[0].MessageID != "m2" {
		t.Fatalf("the frame carries %#v, want only the reaction that still exists", decoded.Facts)
	}
	if store.asked == 0 {
		t.Fatal("the frame was built without asking the record what the reactions say")
	}
}

// The cap drops the LEAST RECENTLY DECIDED key, which is not the same as "the
// key that was queued first". A key that is updated moves to the back; without
// that, refreshing the front key and then overflowing would throw away exactly
// the fresh decision the cap was supposed to protect.
func TestTheOutboxCapDropsTheStalestKeyNotTheFirstQueued(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("be")

	first := reactionFactFor(peer, "first", "👍", 1)
	if err := sender.queueReactions(peer, []domain.ReactionFact{first}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	// Fill the rest of the cap with other keys, then refresh the FIRST one.
	filler := make([]domain.ReactionFact, 0, dmControlOutboxMaxKeys-1)
	for i := range dmControlOutboxMaxKeys - 1 {
		filler = append(filler, reactionFactFor(peer, fmt.Sprintf("%036d", i), "🔥", uint64(i+2)))
	}
	if err := sender.queueReactions(peer, filler); err != nil {
		t.Fatalf("queue filler: %v", err)
	}
	refreshed := reactionFactFor(peer, "first", "👍", 1<<20)
	if err := sender.queueReactions(peer, []domain.ReactionFact{refreshed}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// One more key overflows the cap by one.
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "last", "🙏", 1<<21),
	}); err != nil {
		t.Fatalf("queue one more: %v", err)
	}
	// The cap is applied on requeue, which is where an overflowing outbox is
	// trimmed; queueing itself only coalesces.
	sender.requeue(peer, nil, nil, &dmControlOutbox{}, dmControlRetryDelay)

	held := queuedFor(sender, peer)
	if held == nil {
		t.Fatal("the outbox was emptied")
	}
	if len(held.entries) > dmControlOutboxMaxKeys {
		t.Fatalf("the outbox holds %d facts, over the %d cap", len(held.entries), dmControlOutboxMaxKeys)
	}
	for _, entry := range held.entries {
		if entry.key.MessageID == "first" {
			return
		}
	}
	t.Fatal("the cap dropped the key that had just been refreshed")
}

// Removing a contact takes the queue with it. The queue is keys rather than
// facts, so a stale entry resolves to nothing at the next flush anyway — but
// waiting for that would leave the ids and emoji of an erased conversation in
// memory, and would keep the beliefs about that peer's build alive.
func TestForgettingAPeerEmptiesWhatWeWereGoingToSayToThem(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	svc := sender.svc
	peer := controlTestPeer("a1")
	other := controlTestPeer("a2")

	for _, target := range []domain.PeerIdentity{peer, other} {
		if err := sender.queueReactions(target, []domain.ReactionFact{
			reactionFactFor(target, "m1", "\U0001F44D", 1),
		}); err != nil {
			t.Fatalf("queue for %s: %v", target, err)
		}
	}
	svc.noteCommandRefused(peer, domain.DMControlReactions)
	sender.markTypeRefused(peer)

	svc.ForgetPeerReactions(peer)

	if queuedFor(sender, peer) != nil {
		t.Fatal("the removed contact still has facts waiting to be sent to them")
	}
	if refusalHeld(sender, refusalKey{peer: peer, command: domain.DMControlReactions}) {
		t.Fatal("what we believed about a removed contact's build was kept")
	}
	if typeRefusedFor(sender, peer) {
		t.Fatal("the dtype refusal for a removed contact was kept")
	}
	// One conversation only: the queue is per peer and removing one contact
	// says nothing about another.
	if queuedFor(sender, other) == nil {
		t.Fatal("removing one contact emptied another contact's queue")
	}
}

// A batch that takeDue has already handed to send is not in the map any more,
// so emptying the map cannot reach it. Removing the contact mid-send must still
// stop it — and must not return while a frame of it is between "allowed to go"
// and "handed to the plane", because sealing and building run unlocked and a
// frame in that gap would reach the transport after the conversation is gone.
func TestARemovalMidSendStopsTheBatchAndWaitsForTheFrameInFlight(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc

	var attempts atomic.Int32
	sending := make(chan struct{})
	release := make(chan struct{})
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		if attempts.Add(1) == 1 {
			close(sending)
			<-release
		}
		return dmControlDispatch{kind: datagram.SendNoRoute, summary: "no_route"}
	}
	if err := sender.queueReactions(peer, manyOutgoingFacts(peer, 200)); err != nil {
		t.Fatalf("queue: %v", err)
	}
	built, _ := sender.framesFor(context.Background(), peer, queuedFor(sender, peer))
	if len(built) < 2 {
		t.Fatalf("the batch is %d frame(s); it has to span several to prove the rest is not attempted", len(built))
	}

	flushed := make(chan struct{})
	go func() {
		sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
		close(flushed)
	}()
	<-sending

	// The user removes the contact while the first frame is in that gap.
	forgotten := make(chan struct{})
	go func() {
		svc.ForgetPeerReactions(peer)
		close(forgotten)
	}()
	// It has done its clearing and is waiting — proven, not assumed: a removal
	// that had not started yet would also look like "has not returned", and the
	// frame's own failure would then explain everything the test checks below.
	awaitRemovalStarted(t, sender, peer)
	select {
	case <-forgotten:
		t.Fatal("the removal returned while a frame of the conversation was still on its way")
	default:
		// Still waiting, which is the whole point.
	}

	close(release)
	select {
	case <-forgotten:
	case <-time.After(5 * time.Second):
		t.Fatal("the removal never returned after the frame was handed over")
	}
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass never finished")
	}

	if got := attempts.Load(); got != 1 {
		t.Fatalf("%d frames were sent to a contact removed after the first", got)
	}
	if queuedFor(sender, peer) != nil {
		t.Fatal("the undelivered half of the batch rebuilt the queue of a removed contact")
	}
}

// What the peer says BACK can outlive the conversation: an answer to a frame
// sent before the removal arrives after it. Recording it would put back the
// belief the removal cleared, and that belief silences reactions for an hour if
// the user adds the contact again.
func TestAnAnswerArrivingAfterARemovalIsNotRemembered(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	svc := sender.svc
	peer := controlTestPeer("c1")

	svc.ForgetPeerReactions(peer)
	svc.noteCommandRefused(peer, domain.DMControlReactions)
	sender.markTypeRefused(peer)

	if refusalHeld(sender, refusalKey{peer: peer, command: domain.DMControlReactions}) {
		t.Fatal("a refusal about a removed conversation was remembered")
	}
	if typeRefusedFor(sender, peer) {
		t.Fatal("a dtype refusal about a removed conversation was remembered")
	}

	// A fact for that peer does not end the window: their answer to the removed
	// conversation can still be in flight, and believing it against the new one
	// would tell the user for an hour that reactions cannot get there.
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue after the removal: %v", err)
	}
	svc.noteCommandRefused(peer, domain.DMControlReactions)
	if refusalHeld(sender, refusalKey{peer: peer, command: domain.DMControlReactions}) {
		t.Fatal("a new fact reopened the door to answers about the removed conversation")
	}

	// Once the window is up they count again, so this is a window and not a ban.
	now = now.Add(dmControlForgetGrace)
	svc.noteCommandRefused(peer, domain.DMControlReactions)
	if !refusalHeld(sender, refusalKey{peer: peer, command: domain.DMControlReactions}) {
		t.Fatal("an answer after the window was still ignored")
	}
}

// The window ends on its own too, so a peer that is never spoken to again does
// not keep an entry for the life of the process.
func TestTheForgetWindowIsSweptWithTheRest(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("c2")

	sender.svc.ForgetPeerReactions(peer)
	_, _ = sender.takeDue(now.Add(dmControlForgetGrace - time.Second))
	sender.mu.Lock()
	held := len(sender.forgot)
	sender.mu.Unlock()
	if held != 1 {
		t.Fatalf("the window was swept after %v, before it was up", dmControlForgetGrace-time.Second)
	}

	_, _ = sender.takeDue(now.Add(dmControlForgetGrace))
	sender.mu.Lock()
	held = len(sender.forgot)
	sender.mu.Unlock()
	if held != 0 {
		t.Fatal("the window outlived its grace")
	}
}

// The frame is the last one of its batch, so the mark has no next frame to stop:
// the leftovers come back to requeue, which is where the removal has to be
// noticed the second time.
func TestALateFailureDoesNotRebuildAForgottenQueue(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc

	sending := make(chan struct{})
	release := make(chan struct{})
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		close(sending)
		<-release
		return dmControlDispatch{kind: datagram.SendNoRoute, summary: "no_route"}
	}
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	built, _ := sender.framesFor(context.Background(), peer, queuedFor(sender, peer))
	if len(built) != 1 {
		t.Fatalf("the batch is %d frames; this test needs exactly one", len(built))
	}

	flushed := make(chan struct{})
	go func() {
		sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
		close(flushed)
	}()
	<-sending

	forgotten := make(chan struct{})
	go func() {
		svc.ForgetPeerReactions(peer)
		close(forgotten)
	}()
	// Wait for the removal to have done its clearing, so what follows is a
	// removal that ran before the requeue rather than one that never started.
	awaitRemovalStarted(t, sender, peer)
	close(release)

	select {
	case <-forgotten:
	case <-time.After(5 * time.Second):
		t.Fatal("the removal never returned")
	}
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass never finished")
	}

	if queuedFor(sender, peer) != nil {
		t.Fatal("the undelivered frame rebuilt the queue of a removed contact")
	}
}

// The mark travels with the BATCH, not with a per-peer counter, and that is
// what survives the sweep. A counter is an ABA: an ordinary batch is taken at
// zero, the removal moves it to one, and the first thing that drops the peer's
// entry — a sweep, or a new fact — puts it back at zero, where the abandoned
// batch matches again and goes out against the conversation that replaced it.
func TestAnAbandonedBatchStaysAbandonedAfterTheWindowIsSwept(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("c3")

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	due, _ := sender.takeDue(now.Add(2 * dmControlDebounceFloor))
	batch := due[peer]
	if batch == nil {
		t.Fatal("the batch was not taken")
	}

	sender.svc.ForgetPeerReactions(peer)

	// Long enough that the peer's forget entry is swept, and a new conversation
	// has queued facts of its own.
	now = now.Add(dmControlForgetGrace)
	_, _ = sender.takeDue(now)
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m2", "\U0001F525", 2),
	}); err != nil {
		t.Fatalf("queue for the new conversation: %v", err)
	}

	// The age bound must not be what saves us: it drops anything older than
	// dmControlOutboxMaxAge, and the sweep above needs a longer wait than that,
	// so the entries are kept young on purpose and only the mark can refuse them.
	fresh := make([]dmControlEntry, 0, len(batch.entries))
	for _, entry := range batch.entries {
		fresh = append(fresh, dmControlEntry{key: entry.key, queuedAt: now})
	}
	sender.requeue(peer, fresh, nil, batch, dmControlRetryDelay)

	held := queuedFor(sender, peer)
	if held == nil {
		t.Fatal("the new conversation's own queue disappeared")
	}
	for _, entry := range held.entries {
		if entry.key.MessageID == "m1" {
			t.Fatal("a fact of the removed conversation joined the queue of the one that replaced it")
		}
	}
}

// The window has to outlast the whole chain that can produce a late answer, not
// just the middle leg of it. Pinned because the parts are constants of three
// different subsystems: raising the outbox age, or the plane's validity ceiling,
// silently shortens the window relative to what it guards against.
func TestTheForgetWindowOutlastsALateAnswer(t *testing.T) {
	t.Parallel()
	// Our frame waiting on the plane, their outbox retrying its answer, and the
	// answer travelling back.
	longest := domain.DatagramBaseReplayWindow +
		dmControlOutboxMaxAge + dmControlRetryDelay +
		domain.DatagramBaseReplayWindow
	if dmControlForgetGrace < longest {
		t.Fatalf("the forget window is %v, shorter than the %v an answer can take",
			dmControlForgetGrace, longest)
	}
}

// The refusal is learned a second after the tap, when the debounced frame goes
// out, so the UI has to be TOLD. Without this the first reaction to an old
// client looks delivered and the user hears about it only on the next tap.
func TestLearningAPeerCannotReceiveReactionsIsAnnounced(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	sender.svc.eventBus = bus

	announced := make(chan domain.PeerIdentity, 4)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) {
		announced <- peer
	})

	peer := controlTestPeer("d1")
	sender.markTypeRefused(peer)
	select {
	case got := <-announced:
		if got != peer {
			t.Fatalf("the announcement named %s, want %s", got, peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("learning that a peer cannot receive reactions was never announced")
	}

	// Said once. The answer has not changed, and every later refusal from the
	// same peer would otherwise reload their conversation for nothing.
	sender.markTypeRefused(peer)
	sender.svc.noteCommandRefused(peer, domain.DMControlReactions)
	select {
	case got := <-announced:
		t.Fatalf("a refusal that changed nothing was announced again for %s", got)
	case <-time.After(200 * time.Millisecond):
	}

	// A different peer is different news.
	other := controlTestPeer("d2")
	sender.svc.noteCommandRefused(other, domain.DMControlReactions)
	select {
	case got := <-announced:
		if got != other {
			t.Fatalf("the announcement named %s, want %s", got, other)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an inner refusal by another peer was never announced")
	}
}

// A single message being deleted is invisible to this queue — it names
// reactions, not messages — and the frames are built from what the record said a
// moment earlier. So the delete brackets itself: nothing new goes out while it
// runs, and the frames already past the gate are waited for.
func TestASingleMessageDeleteStopsTheFramesItWouldHaveContradicted(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc

	var attempts atomic.Int32
	sending := make(chan struct{})
	release := make(chan struct{})
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		if attempts.Add(1) == 1 {
			close(sending)
			<-release
		}
		return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
	}
	if err := sender.queueReactions(peer, manyOutgoingFacts(peer, 200)); err != nil {
		t.Fatalf("queue: %v", err)
	}
	built, _ := sender.framesFor(context.Background(), peer, queuedFor(sender, peer))
	if len(built) < 2 {
		t.Fatalf("the batch is %d frame(s); it has to span several to prove the rest is stopped", len(built))
	}

	flushed := make(chan struct{})
	go func() {
		sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
		close(flushed)
	}()
	<-sending

	// The delete starts while the first frame is between "allowed" and "handed
	// over": it must not return until that frame is done.
	held := make(chan func(), 1)
	go func() { held <- svc.HoldReactionSends(peer) }()
	// The pause is up and the delete is waiting on the frame — proven rather
	// than assumed, for the reason awaitPauseStarted gives.
	awaitPauseStarted(t, svc, peer)
	select {
	case <-held:
		t.Fatal("the delete went ahead while a frame of that conversation was still on its way")
	default:
	}

	close(release)
	var resume func()
	select {
	case resume = <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the delete never got its turn after the frame was handed over")
	}
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass never finished")
	}

	if got := attempts.Load(); got != 1 {
		t.Fatalf("%d frames went out across a message delete, want only the one already in flight", got)
	}
	// What was stopped is not lost: it waits for the delete to finish and is
	// built again from the record as it then stands.
	outbox := queuedFor(sender, peer)
	if outbox == nil || len(outbox.entries) == 0 {
		t.Fatal("the paused frames were dropped instead of being offered again")
	}
	resume()

	// And with the delete over, the queue drains normally again.
	now = now.Add(dmControlRetryDelay + dmControlDebounceFloor)
	sender.flushDue(context.Background(), now)
	if attempts.Load() < 2 {
		t.Fatal("the queue stayed paused after the delete finished")
	}
}

// Two deletes of the same conversation can overlap — a local one and an ack for
// a row that came back — so the pause is COUNTED. The first release must not
// open the gate under the second, and a release run twice (the delete path calls
// it explicitly and defers it as a net) must not open it either.
func TestOverlappingDeletesHoldTheGateUntilTheLastOneIsDone(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	svc := sender.svc
	peer := controlTestPeer("e1")
	batch := &dmControlOutbox{}

	first := svc.HoldReactionSends(peer)
	second := svc.HoldReactionSends(peer)
	if got := sender.beginFrame(peer, batch, sender.pauseGeneration(peer)); got != framePaused {
		t.Fatalf("the gate answered %v while two deletes were running", got)
	}

	first()
	first() // idempotent: the delete path releases explicitly AND defers it.
	if got := sender.beginFrame(peer, batch, sender.pauseGeneration(peer)); got != framePaused {
		t.Fatalf("one delete's release opened the gate under the other: %v", got)
	}

	second()
	if got := sender.beginFrame(peer, batch, sender.pauseGeneration(peer)); got != frameAllowed {
		t.Fatalf("the gate stayed shut after the last delete finished: %v", got)
	}
	sender.endFrame(peer)

	// And a removal of the conversation still outranks it: that batch is not
	// paused, it is over.
	batch.abandoned = true
	if got := sender.beginFrame(peer, batch, sender.pauseGeneration(peer)); got != frameAbandoned {
		t.Fatalf("an abandoned batch was treated as %v", got)
	}
}

// A conversation wipe is not a contact removal: the contact stays, and whether
// their build can receive reactions is a property of THEM, not of the thread.
// Clearing it would make the next reaction look delivered until the refusal is
// learned again — and the answer-refusing window on top would make even the
// fresh answer be ignored, for over half an hour.
func TestWipingAThreadKeepsWhatWeKnowAboutThePeersBuild(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	svc := sender.svc
	peer := controlTestPeer("f1")

	svc.noteCommandRefused(peer, domain.DMControlReactions)
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}

	svc.DropQueuedReactions(peer)

	if queuedFor(sender, peer) != nil {
		t.Fatal("the wiped thread still has reactions waiting to be sent")
	}
	if !svc.ReactionsUnsupportedBy(peer) {
		t.Fatal("a thread wipe threw away what we know about the contact's build")
	}
	// And a FRESH answer still counts: no refusing window was opened.
	svc.noteCommandRefused(peer, domain.DMControlReactions)
	if !refusalHeld(sender, refusalKey{peer: peer, command: domain.DMControlReactions}) {
		t.Fatal("an answer after a thread wipe was ignored as if the contact were gone")
	}

	// Removing the CONTACT is the other case and takes everything.
	svc.ForgetPeerReactions(peer)
	if svc.ReactionsUnsupportedBy(peer) {
		t.Fatal("removing the contact kept what we knew about their build")
	}
}

// A peer that becomes able to receive reactions is news the UI needs: it drew a
// notice saying the opposite, and that notice stands until something reloads the
// conversation. The clearing runs on every session and on every reaction
// received, so it is announced only when the answer actually changes.
func TestAPeerThatCanReceiveReactionsAgainIsAnnounced(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	sender.svc.eventBus = bus

	announced := make(chan domain.PeerIdentity, 4)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) {
		announced <- peer
	})

	peer := controlTestPeer("f7")
	sender.markTypeRefused(peer)
	select {
	case <-announced:
	case <-time.After(2 * time.Second):
		t.Fatal("learning the refusal was never announced")
	}

	// The peer upgrades and a session comes up.
	sender.svc.forgetDMControlRefusal(peer)
	select {
	case got := <-announced:
		if got != peer {
			t.Fatalf("the announcement named %s, want %s", got, peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a peer that can receive reactions again was never announced")
	}

	// Clearing what was already clear is not news: this runs on every session
	// and on every reaction received.
	sender.svc.forgetDMControlRefusal(peer)
	select {
	case got := <-announced:
		t.Fatalf("a clearing that changed nothing was announced for %s", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// A belief that expires is a peer that becomes able to receive reactions again,
// as far as anything here can tell — and the UI drew a notice saying the
// opposite. Nothing else reports it: a session after the sweep clears an entry
// that is already gone and therefore announces nothing.
func TestABeliefThatExpiresIsAnnouncedLikeAnUpgrade(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	sender.svc.eventBus = bus

	announced := make(chan domain.PeerIdentity, 8)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) {
		announced <- peer
	})

	peer := controlTestPeer("f9")
	sender.markTypeRefused(peer)
	select {
	case <-announced:
	case <-time.After(2 * time.Second):
		t.Fatal("learning the refusal was never announced")
	}

	// A pass before the TTL is up changes nothing.
	sender.flushDue(context.Background(), now.Add(dmControlUnsupportedTTL-time.Second))
	select {
	case got := <-announced:
		t.Fatalf("a belief still in force was announced as gone for %s", got)
	case <-time.After(200 * time.Millisecond):
	}

	// The pass that sweeps it is the one that has to say so.
	now = now.Add(dmControlUnsupportedTTL)
	sender.flushDue(context.Background(), now)
	select {
	case got := <-announced:
		if got != peer {
			t.Fatalf("the announcement named %s, want %s", got, peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a belief that expired was never announced")
	}
	if sender.svc.ReactionsUnsupportedBy(peer) {
		t.Fatal("the belief outlived its TTL")
	}

	// And it is said once: the entry is gone, so later passes have nothing to
	// report.
	sender.flushDue(context.Background(), now.Add(time.Minute))
	select {
	case got := <-announced:
		t.Fatalf("the expiry was announced again for %s", got)
	case <-time.After(200 * time.Millisecond):
	}

	// One belief expiring while the OTHER still stands is not news: the answer
	// the UI asks for is the union of the two, and it has not changed.
	other := controlTestPeer("fa")
	sender.markTypeRefused(other)
	select {
	case <-announced:
	case <-time.After(2 * time.Second):
		t.Fatal("learning the type refusal was never announced")
	}
	now = now.Add(dmControlUnsupportedTTL / 2)
	sender.svc.noteCommandRefused(other, domain.DMControlReactions)

	// The older of the two — the type refusal — expires here; the command one
	// has half a TTL left.
	now = now.Add(dmControlUnsupportedTTL/2 + time.Second)
	sender.flushDue(context.Background(), now)
	select {
	case got := <-announced:
		t.Fatalf("a peer still held back by another belief was announced as clear: %s", got)
	case <-time.After(200 * time.Millisecond):
	}
	if !sender.svc.ReactionsUnsupportedBy(other) {
		t.Fatal("the surviving belief was swept with the expired one")
	}
}

// The window between a belief expiring and the sweep reaching it is where the
// news used to be lost: a session in that window cleared the entry through a
// read, the read reported "not blocked", nothing was published, and the sweep
// then found nothing to report. The UI kept the notice.
func TestASessionInsideTheExpiryWindowStillAnnouncesTheChange(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	sender.svc.eventBus = bus

	announced := make(chan domain.PeerIdentity, 4)
	bus.Subscribe(ebus.TopicReactionsChanged, func(peer domain.PeerIdentity) {
		announced <- peer
	})

	peer := controlTestPeer("fb")
	sender.markTypeRefused(peer)
	select {
	case <-announced:
	case <-time.After(2 * time.Second):
		t.Fatal("learning the refusal was never announced")
	}

	// The TTL is up, but no pass has run yet — and a session comes up.
	now = now.Add(dmControlUnsupportedTTL)
	sender.svc.forgetDMControlRefusal(peer)
	select {
	case got := <-announced:
		if got != peer {
			t.Fatalf("the announcement named %s, want %s", got, peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a session in the expiry window announced nothing, and the sweep will find nothing")
	}

	// And the sweep afterwards has nothing left to say: the entry is gone.
	_, cleared := sender.takeDue(now.Add(time.Minute))
	if len(cleared) != 0 {
		t.Fatalf("the sweep reported %v after the session had already cleared it", cleared)
	}
}

// Two windows in which a frame already in flight could write control state back
// AFTER the removal that cleared it. Both are "the removal ran between the
// decision and the write", which is the shape this subsystem has had to close
// once per surface.
func TestNothingWritesControlStateBackAfterARemoval(t *testing.T) {
	t.Parallel()

	t.Run("an answer whose command arrived before the removal", func(t *testing.T) {
		now := time.Now().UTC()
		sender := controlSenderAt(t, &now, 0)
		svc := sender.svc
		peer := controlTestPeer("b9")

		// The handler has decided to answer — the sender is known — and the
		// removal completes before the answer is queued.
		svc.ForgetPeerReactions(peer)
		svc.answerCommandUnsupported(peer, domain.DMControlCommand("message_edit"))

		if outbox := queuedFor(sender, peer); outbox != nil {
			t.Fatalf("the answer rebuilt the queue the removal emptied: %#v", outbox)
		}
	})

	t.Run("a frame accepted while the removal was waiting for it", func(t *testing.T) {
		now := time.Now().UTC()
		peerID, err := identity.Generate()
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		peer := domain.PeerIdentityFromWire(peerID.Address)
		sender := controlSenderWithKey(t, &now, peerID)
		svc := sender.svc

		sending := make(chan struct{})
		release := make(chan struct{})
		sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
			close(sending)
			<-release
			return dmControlDispatch{kind: datagram.SendQueued, summary: "queued"}
		}
		if err := sender.queueReactions(peer, []domain.ReactionFact{
			reactionFactFor(peer, "m1", "\U0001F44D", 1),
		}); err != nil {
			t.Fatalf("queue: %v", err)
		}

		flushed := make(chan struct{})
		go func() {
			sender.flushDue(context.Background(), now.Add(2*dmControlDebounceFloor))
			close(flushed)
		}()
		<-sending

		// The removal starts while that frame is between "allowed" and "handed
		// over", so it waits — and must not have the admission written back
		// under it when the frame finishes.
		forgotten := make(chan struct{})
		go func() {
			svc.ForgetPeerReactions(peer)
			close(forgotten)
		}()
		awaitRemovalStarted(t, sender, peer)
		close(release)

		select {
		case <-forgotten:
		case <-time.After(5 * time.Second):
			t.Fatal("the removal never returned")
		}
		select {
		case <-flushed:
		case <-time.After(5 * time.Second):
			t.Fatal("the pass never finished")
		}

		sender.mu.Lock()
		_, admitted := sender.sentAt[peer]
		sender.mu.Unlock()
		if admitted {
			t.Fatal("a frame finishing under the removal wrote the admission back")
		}
	})
}

// A reaction made a second ago must not inherit the age, or the deadline, of a
// batch that has been failing for half an hour. Both were batch-wide once, and
// both punished the fresh key for somebody else's failure: it aged out before it
// had been tried, and it waited a retry delay it had not earned.
func TestAFreshReactionDoesNotInheritAFailingBatchsClock(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("bb")

	// An old key that has been retried nearly to the age limit.
	old := dmControlEntry{
		key:      domain.ReactionKey{MessageID: "old", Actor: peer, Emoji: "\U0001F44D"},
		queuedAt: now,
	}
	now = now.Add(dmControlOutboxMaxAge - time.Minute)
	sender.requeue(peer, []dmControlEntry{old}, nil, &dmControlOutbox{}, dmControlRetryDelay)
	outbox := queuedFor(sender, peer)
	if outbox == nil || len(outbox.entries) != 1 {
		t.Fatalf("the retried key was not queued: %#v", outbox)
	}
	retryDeadline := outbox.dueAt

	// A tap now joins the same outbox.
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "fresh", "\U0001F525", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	outbox = queuedFor(sender, peer)

	// It waits the debounce, not the retry delay the old key set.
	if !outbox.dueAt.Before(retryDeadline) {
		t.Fatalf("the fresh reaction waits until %v, the retry's own deadline", outbox.dueAt)
	}
	if want := now.Add(dmControlDebounceFloor); outbox.dueAt.After(want) {
		t.Fatalf("the fresh reaction waits until %v, past the debounce at %v", outbox.dueAt, want)
	}

	// And past the age limit the OLD key goes while the fresh one stays: the
	// bound is each key's own. Taken out of the map first, the way a pass does,
	// so what comes back is only what requeue puts back.
	now = now.Add(2 * time.Minute)
	due, _ := sender.takeDue(now)
	batch := due[peer]
	if batch == nil {
		t.Fatal("the batch was not due")
	}
	sender.requeue(peer, batch.entries, nil, batch, dmControlRetryDelay)

	held := queuedFor(sender, peer)
	if held == nil {
		t.Fatal("the whole outbox aged out on the oldest key's clock")
	}
	for _, entry := range held.entries {
		if entry.key.MessageID == "old" {
			t.Fatal("a key past the age limit was retried anyway")
		}
	}
	if len(held.entries) != 1 || held.entries[0].key.MessageID != "fresh" {
		t.Fatalf("the outbox holds %#v, want only the fresh reaction", held.entries)
	}

	// And a tap made WHILE a batch is in flight keeps its own deadline: the
	// requeue that follows must not push it back to the retry delay.
	now = now.Add(time.Minute)
	inflight, _ := sender.takeDue(now)
	batch = inflight[peer]
	if batch == nil {
		t.Fatal("the fresh reaction was not due")
	}
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "newest", "\U0001F64F", 2),
	}); err != nil {
		t.Fatalf("queue while in flight: %v", err)
	}
	tapped := queuedFor(sender, peer).dueAt
	sender.requeue(peer, batch.entries, nil, batch, dmControlRetryDelay)
	if after := queuedFor(sender, peer).dueAt; after.After(tapped) {
		t.Fatalf("the requeue pushed the tap's deadline from %v to %v", tapped, after)
	}
}

// An answer this node owes a peer is bounded by the same age its reactions are.
// It used to be bounded by the batch's own clock; when that became per-reaction,
// the answers were left with none — and an undeliverable one would be retried
// every thirty seconds for as long as the process lived.
func TestAnUndeliverableAnswerAgesOutToo(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("bc")

	sender.svc.answerCommandUnsupported(peer, domain.DMControlCommand("message_edit"))
	if outbox := queuedFor(sender, peer); outbox == nil || len(outbox.refusals) != 1 {
		t.Fatalf("the answer was not queued: %#v", outbox)
	}

	// Nothing can be handed over — no box key — so every pass requeues it.
	queuedAt := now
	for now.Sub(queuedAt) < dmControlOutboxMaxAge-dmControlRetryDelay {
		now = now.Add(dmControlDebounceFloor + dmControlRetryDelay)
		sender.flushDue(context.Background(), now)
		if queuedPeers(sender) != 1 {
			t.Fatalf("the answer stopped being retried after %s", now.Sub(queuedAt))
		}
	}
	now = now.Add(dmControlOutboxMaxAge)
	sender.flushDue(context.Background(), now)
	if queuedPeers(sender) != 0 {
		t.Fatal("an answer older than the outbox allows is still being retried")
	}
}

// A deadline that has already come due means "send it now". A requeue must not
// turn that into another thirty seconds of silence — which is what a reaction
// tapped while the previous batch was in flight gets if the batch comes back
// after its debounce has passed.
func TestARequeueDoesNotPostponeAnAlreadyDueReaction(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("bd")

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "first", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	now = now.Add(2 * dmControlDebounceFloor)
	due, _ := sender.takeDue(now)
	batch := due[peer]
	if batch == nil {
		t.Fatal("the batch was not due")
	}

	// The user taps again while it is in flight, and the send finishes AFTER
	// that tap's own debounce has run out.
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "second", "\U0001F525", 2),
	}); err != nil {
		t.Fatalf("queue while in flight: %v", err)
	}
	now = now.Add(2 * dmControlDebounceFloor)
	sender.requeue(peer, batch.entries, nil, batch, dmControlRetryDelay)

	if dueAt := queuedFor(sender, peer).dueAt; dueAt.After(now) {
		t.Fatalf("the requeue postponed an already due reaction to %v (now %v)", dueAt, now)
	}
}

// The debounce is one draw per BATCH, not per tap.
//
// Two things follow, and both were wrong before. A second tap joins the batch it
// finds without moving its deadline — otherwise the delay depends on how many
// times the user pressed, and a fresh draw each time pulls the whole
// distribution towards the floor. And a batch carrying a RETRY deadline is
// re-armed by the next tap, because half a minute of somebody else's failure is
// not what that tap earned.
func TestTheDebounceJitterIsDrawnOncePerBatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, 0)
	peer := controlTestPeer("be")

	draws := 0
	next := dmControlDebounceJitter
	sender.jitter = func() time.Duration {
		draws++
		return next
	}
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	armed := queuedFor(sender, peer).dueAt
	if draws != 1 {
		t.Fatalf("the first tap drew its jitter %d times", draws)
	}
	if want := now.Add(dmControlDebounceFloor + dmControlDebounceJitter); !armed.Equal(want) {
		t.Fatalf("the deadline is %v, want the one draw at %v", armed, want)
	}

	// A second tap, with a draw that would land much earlier if it were taken.
	next = 0
	now = now.Add(100 * time.Millisecond)
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m2", "\U0001F525", 2),
	}); err != nil {
		t.Fatalf("queue again: %v", err)
	}
	if draws != 1 {
		t.Fatalf("a tap joining an armed batch drew the jitter again: %d draws", draws)
	}
	if got := queuedFor(sender, peer).dueAt; !got.Equal(armed) {
		t.Fatalf("the second tap moved the batch from %v to %v", armed, got)
	}

	// A batch carrying a retry deadline is a different case: the next tap arms
	// it again rather than waiting out a delay it did not earn.
	now = now.Add(2 * dmControlDebounceFloor)
	due, _ := sender.takeDue(now)
	batch := due[peer]
	if batch == nil {
		t.Fatal("the batch was not due")
	}
	sender.requeue(peer, batch.entries, nil, batch, dmControlRetryDelay)
	retrying := queuedFor(sender, peer).dueAt
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m3", "\U0001F64F", 3),
	}); err != nil {
		t.Fatalf("queue onto the retry: %v", err)
	}
	if draws != 2 {
		t.Fatalf("the tap onto a retrying batch drew %d times in total, want a second draw", draws)
	}
	if got := queuedFor(sender, peer).dueAt; !got.Before(retrying) {
		t.Fatalf("the tap waits until %v, the retry's own deadline at %v", got, retrying)
	}
}

// Clearing a belief runs on EVERY session, incoming and outgoing. It may bring
// forward only what that belief was holding back: pulling an ordinary debounced
// batch forward would put a reaction on the wire the moment a peer reconnects,
// which is the timing the debounce and its jitter exist to hide.
func TestASessionDoesNotHurryAnOrdinaryBatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	sender := controlSenderAt(t, &now, dmControlDebounceJitter/2)
	svc := sender.svc
	peer := controlTestPeer("c7")

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	debounced := queuedFor(sender, peer).dueAt

	// A peer reconnects while the tap is still waiting out its debounce.
	svc.forgetDMControlRefusal(peer)
	if got := queuedFor(sender, peer).dueAt; !got.Equal(debounced) {
		t.Fatalf("a session moved the tap's deadline from %v to %v", debounced, got)
	}

	// A batch the belief WAS holding back is the case this exists for: it goes
	// as soon as the belief is gone.
	other := controlTestPeer("c8")
	svc.noteCommandRefused(other, domain.DMControlReactions)
	if err := sender.queueReactions(other, []domain.ReactionFact{
		reactionFactFor(other, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue for the refusing peer: %v", err)
	}
	now = now.Add(2 * dmControlDebounceFloor)
	due, _ := sender.takeDue(now)
	batch := due[other]
	if batch == nil {
		t.Fatal("the held batch was not due")
	}
	sender.requeue(other, batch.entries, nil, batch, dmControlRetryDelay)
	retrying := queuedFor(sender, other).dueAt
	if !retrying.After(now) {
		t.Fatalf("the fixture needs a retry deadline in the future, got %v", retrying)
	}

	svc.forgetDMControlRefusal(other)
	if got := queuedFor(sender, other).dueAt; got.After(now) {
		t.Fatalf("the unblocked batch still waits until %v", got)
	}
}

// A delete that comes AND GOES while a pass is building its frames is the case
// waiting for the frames in flight does not cover: that pass had not reached the
// gate yet, so it was never in flight — and what it built was read from the
// record before the row was removed.
//
// Both edges of the pause count, and the peers are kept apart. Everything here
// is one property seen from three sides: what a pass may send is what the record
// said with no delete of ITS conversation anywhere across the read.
func TestFramesBuiltAroundADeleteAreNotSentAfterIt(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc

	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	due, _ := sender.takeDue(now.Add(2 * dmControlDebounceFloor))
	batch := due[peer]
	if batch == nil {
		t.Fatal("the batch was not due")
	}

	// The pass reads the record and builds its frames.
	builtAt := sender.pauseGeneration(peer)
	frames, held := sender.framesFor(context.Background(), peer, batch)
	if len(frames) == 0 || len(held) != 0 {
		t.Fatalf("the fixture built %d frames and held %d reactions", len(frames), len(held))
	}

	// A whole per-message delete happens before the pass reaches the gate.
	svc.HoldReactionSends(peer)()

	if got := sender.beginFrame(peer, batch, builtAt); got != framePaused {
		t.Fatalf("the gate answered %v for frames built before a delete", got)
	}
	// And a pass that starts AFTER it is admitted, so this is a repetition and
	// not a stall.
	if got := sender.beginFrame(peer, batch, sender.pauseGeneration(peer)); got != frameAllowed {
		t.Fatalf("the gate answered %v for a pass that read the record after the delete", got)
	}
	sender.endFrame(peer)

	// The other half of the same window: a pass that STARTS while the pause is
	// up reads the record before the delete commits, and by the time it reaches
	// the gate the pause is gone. The raise is behind it, so only the release
	// can tell it apart from an ordinary pass.
	resume := svc.HoldReactionSends(peer)
	duringPause := sender.pauseGeneration(peer)
	resume()
	if got := sender.beginFrame(peer, batch, duringPause); got != framePaused {
		t.Fatalf("the gate answered %v for frames built while the delete was running", got)
	}

	// A delete in ANOTHER conversation is not this one's business: a shared
	// counter made every pass everywhere start again.
	other := controlTestPeer("c9")
	before := sender.pauseGeneration(peer)
	svc.HoldReactionSends(other)()
	if after := sender.pauseGeneration(peer); after != before {
		t.Fatalf("a delete in another conversation moved this peer's generation from %d to %d", before, after)
	}
}

// Frames sent back by a pause are tried again SOON. They are not a failure —
// the delete holds the queue for one transaction — and half a minute of silence
// would be the user's reaction held hostage to a message they deleted.
func TestPausedFramesComeBackSoonerThanFailedOnes(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	peer := domain.PeerIdentityFromWire(peerID.Address)
	sender := controlSenderWithKey(t, &now, peerID)
	svc := sender.svc

	var resume func()
	sender.dispatch = func(context.Context, protocol.DatagramFrame) dmControlDispatch {
		t.Fatal("a frame was sent while the conversation was paused")
		return dmControlDispatch{}
	}
	if err := sender.queueReactions(peer, []domain.ReactionFact{
		reactionFactFor(peer, "m1", "\U0001F44D", 1),
	}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	resume = svc.HoldReactionSends(peer)
	now = now.Add(2 * dmControlDebounceFloor)
	sender.flushDue(context.Background(), now)
	resume()

	outbox := queuedFor(sender, peer)
	if outbox == nil {
		t.Fatal("the paused frames were dropped instead of being offered again")
	}
	if want := now.Add(dmControlRetryDelay); !outbox.dueAt.Before(want) {
		t.Fatalf("the paused frames wait until %v, as long as a failure would", outbox.dueAt)
	}
	if want := now.Add(dmControlPausedRetryDelay); outbox.dueAt.After(want) {
		t.Fatalf("the paused frames wait until %v, past the short retry at %v", outbox.dueAt, want)
	}
}
