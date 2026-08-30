package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// awaitRemovalInFlight blocks until a removal of that conversation has entered
// the gate — which it does before waiting for the writes already admitted.
func awaitRemovalInFlight(t *testing.T, gate *removalGate, peer domain.PeerIdentity) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !gate.removing(peer) {
		if time.Now().After(deadline) {
			t.Fatal("the removal never entered the gate")
		}
		time.Sleep(time.Millisecond)
	}
}

// newTestDMRouterForConversationDelete assembles a DMRouter bound to a
// real DesktopClient + chatlog with counted conversation-delete
// dispatches, so the wipe can be exercised without the rpc/identity
// stack on the wire side.
func newTestDMRouterForConversationDelete(t *testing.T) (*DMRouter, *DesktopClient, domain.PeerIdentity, *convDispatchCounter) {
	t.Helper()
	c, id := newTestDesktopClientWithNode(t)
	counter := &convDispatchCounter{}
	r := &DMRouter{
		client:                              c,
		seenMessageIDs:                      make(map[string]messageGate),
		peers:                               make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:                             make(map[domain.PeerIdentity]uint64),
		cache:                               NewConversationCache(),
		convDeleteRetry:                     newConversationDeleteRetryState(),
		uiEvents:                            make(chan UIEvent, 32),
		startupDone:                         make(chan struct{}),
		dispatchControlConversationDeleteFn: counter.record,
		withdrawals:                         newWithdrawalBacklog(),
	}
	r.wipeTombstones = c.wipeTombstones
	return r, c, domain.PeerIdentityFromWire(id.Address), counter
}

// convDispatchCounter records conversation_delete dispatches. The request
// carries nothing but its own id — no moment, no message ids — so that is all
// there is to record.
type convDispatchCounter struct {
	mu    sync.Mutex
	calls []domain.ConversationDeleteRequestID
}

func (d *convDispatchCounter) record(_ context.Context, _ domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, requestID)
	return nil
}

// seedConversation writes n messages of the thread with peer, alternating
// direction, and returns their ids.
func seedConversation(t *testing.T, c *DesktopClient, myAddr, peer domain.PeerIdentity, ids ...string) {
	t.Helper()
	for i, id := range ids {
		sender, recipient := myAddr, peer
		if i%2 == 1 {
			sender, recipient = peer, myAddr
		}
		insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
			ID:        id,
			Sender:    sender.String(),
			Recipient: recipient.String(),
			Body:      "ciphertext",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Flag:      string(protocol.MessageFlagAnyDelete),
		})
	}
}

// pendingWorkFor reports what the peer still owes us: the per-message
// deletions the conversation header counts, and whether a whole-thread wipe
// is outstanding.
func pendingWorkFor(t *testing.T, c *DesktopClient, peer domain.PeerIdentity) chatlog.PendingDeletes {
	t.Helper()
	counts, err := c.chatlog.Store().DeleteIntentCountsByPeer(context.Background())
	if err != nil {
		t.Fatalf("DeleteIntentCountsByPeer: %v", err)
	}
	return counts[peer]
}

// conversationIntentFor reads the outstanding wipe request for the peer.
func conversationIntentFor(t *testing.T, c *DesktopClient, peer domain.PeerIdentity) (chatlog.DeleteIntent, bool) {
	t.Helper()
	intent, found, err := c.chatlog.Store().ConversationDeleteIntentForPeer(context.Background(), peer)
	if err != nil {
		t.Fatalf("ConversationDeleteIntentForPeer: %v", err)
	}
	return intent, found
}

// TestConversationDeleteWipesLocallyForAnOfflinePeer is the case the old
// model refused outright: "Delete chat for both sides" with the peer
// offline. The thread must be gone here the moment the user confirms,
// and the peer's half owed as a durable request.
func TestConversationDeleteWipesLocallyForAnOfflinePeer(t *testing.T) {
	t.Parallel()

	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer,
		"f1000000-2222-4444-8888-cccccccccccc",
		"f2000000-2222-4444-8888-cccccccccccc",
		"f3000000-2222-4444-8888-cccccccccccc")

	if err := r.SendConversationDelete(context.Background(), peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	entries, err := c.chatlog.Store().Read(context.Background(), "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d rows survived the wipe; an offline peer must not keep the local thread alive", len(entries))
	}

	// The peer's half is ONE request about the conversation. Not three, one
	// per message: those are answered per message, under each message's own
	// flag, which is what used to leave the requester's own half standing on
	// the other side.
	pending := pendingWorkFor(t, c, peer)
	if !pending.Conversation {
		t.Error("the wipe left no request for the peer's side")
	}
	if pending.Messages != 0 {
		t.Errorf("the wipe wrote %d per-message requests; a thread is asked for as a thread", pending.Messages)
	}

	// The barrier is down: the user can write to the conversation again
	// without waiting for the peer.
	if r.IsConversationDeletePending(peer) {
		t.Error("the outgoing barrier is still up after the wipe; the user cannot send until the peer returns")
	}
}

// TestConversationDeleteDispatchesToAReachablePeer pins the online path:
// same local wipe, and the scheduler starts asking at once rather than
// waiting for its next tick.
func TestConversationDeleteDispatchesToAReachablePeer(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "f4000000-2222-4444-8888-cccccccccccc")

	if err := r.SendConversationDelete(context.Background(), peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	if !pendingWorkFor(t, c, peer).Conversation {
		t.Fatal("the wipe left no request for the peer's side")
	}

	// One sweep is all it takes: the request is an ordinary row of the delete
	// scheduler, so it is picked up like any other.
	r.processDeleteRetryDue(context.Background(), time.Now().UTC())

	intent, found := conversationIntentFor(t, c, peer)
	if !found {
		t.Fatal("the request vanished instead of being dispatched")
	}
	if intent.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", intent.Attempts)
	}
	if !intent.NextAttemptAt.After(time.Now().UTC()) {
		t.Errorf("next_attempt_at = %s, want it behind the backoff", intent.NextAttemptAt)
	}

	// And what went out is the wipe, carrying the id of this gesture.
	counter.mu.Lock()
	calls := append([]domain.ConversationDeleteRequestID(nil), counter.calls...)
	counter.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("dispatched %d requests, want exactly one conversation_delete", len(calls))
	}
	if calls[0] != intent.RequestID {
		t.Errorf("dispatched request id %s, want the stored %s", calls[0], intent.RequestID)
	}
}

// TestConversationDeleteKeepsImmutableRows — immutable is immutable in
// the bulk wipe too, and the flag survives the thread it belonged to.
func TestConversationDeleteKeepsImmutableRows(t *testing.T) {
	t.Parallel()

	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const immutable = "f5000000-2222-4444-8888-cccccccccccc"
	seedConversation(t, c, myAddr, peer, "f6000000-2222-4444-8888-cccccccccccc")
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        immutable,
		Sender:    myAddr.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagImmutable),
	})

	if err := r.SendConversationDelete(context.Background(), peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	entries, err := c.chatlog.Store().Read(context.Background(), "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != immutable {
		t.Fatalf("rows after the wipe = %+v, want only the immutable one", entries)
	}
}

// TestWipeAsksForTheThreadWithoutNamingMessages is the privacy rule, kept by
// construction instead of by classification.
//
// The old model wrote one request per id and therefore had to decide, per row
// and inside the transaction, which ids the peer could possibly hold: a
// message that never reached the wire must not be named, because the request
// would be how they learn it existed. One request about the conversation
// answers that by never asking it — nothing on the wire names a message.
func TestWipeAsksForTheThreadWithoutNamingMessages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, counter := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const (
		sent   = "81111111-2222-4333-8444-555555555555"
		unsent = "82222222-3333-4444-8555-666666666666"
	)
	seedConversation(t, c, myAddr, peer, sent)
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        unsent,
		Sender:    myAddr.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagAnyDelete),
	})
	if err := c.chatlog.Store().MarkNeverEmitted(ctx, []domain.MessageID{domain.MessageID(unsent)}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	// Neither id is owed per message — not the one the peer holds, and not
	// the one they never saw.
	for _, id := range []string{sent, unsent} {
		if _, found := deleteIntentFor(t, c, id); found {
			t.Errorf("a per-message request was written for %s", id)
		}
	}
	intent, found := conversationIntentFor(t, c, peer)
	if !found {
		t.Fatal("the wipe left no request at all")
	}
	if intent.MessageID != "" {
		t.Errorf("the request names %s; a wipe must name no message", intent.MessageID)
	}

	// Both rows are gone locally either way.
	entries, err := c.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d rows survived the wipe", len(entries))
	}

	r.processDeleteRetryDue(ctx, time.Now().UTC())
	counter.mu.Lock()
	dispatched := len(counter.calls)
	counter.mu.Unlock()
	if dispatched != 1 {
		t.Errorf("dispatched %d requests for a two-message thread, want one", dispatched)
	}
}

// TestAWipeThatCannotStopItsOwnDeliveriesDoesNotRun.
//
// The freeze is not a nicety here. A message still queued would go out AFTER
// the local rows are erased, and — because the request carries no ids — it
// would land on the peer after their side has been cleared and answered for:
// their request is settled, their refusals cover the ids they erased, and this
// one is not among them. It would sit in a conversation both users believe is
// gone, and nothing would ever name it again.
//
// So the wipe stops before it destroys anything. The user is told it did not
// run and can click again; the earlier behaviour — erase anyway, ask the peer
// anyway — traded a retry for a message that outlives the conversation.
func TestAWipeThatCannotStopItsOwnDeliveriesDoesNotRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const target = "85555555-4444-4555-8666-777777777777"
	seedConversation(t, c, myAddr, peer, target)

	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{}, errors.New("node unreachable")
	}
	cancelled := false
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		cancelled = true
		return ConversationCancellation{}, nil
	}

	if err := r.SendConversationDelete(ctx, peer); err == nil {
		t.Fatal("the wipe reported success although it could not stop the messages it was erasing")
	}

	entries, err := c.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("%d rows left; the thread was erased although a copy could still go out", len(entries))
	}
	if pendingWorkFor(t, c, peer).Conversation {
		t.Error("a wipe that did not run still asked the peer to clear their side")
	}
	if cancelled {
		t.Error("a wipe that did not run withdrew the deliveries anyway")
	}
}

// TestAbortedWipeThawsTheDeliveries: the freeze is reversible on purpose.
// A transaction that fails leaves the messages on screen and still the
// user's, so they have to be sendable again.
func TestAbortedWipeThawsTheDeliveries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const target = "86666666-4444-4555-8666-777777777777"
	seedConversation(t, c, myAddr, peer, target)

	var thawed []domain.MessageID
	r.client.thawConversationDeliveryFn = func(_ context.Context, _ domain.PeerIdentity, scope []domain.MessageID) error {
		thawed = append(thawed, scope...)
		return nil
	}
	cancelled := false
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		cancelled = true
		return ConversationCancellation{}, nil
	}

	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{Frozen: 1}, nil
	}

	// A request with no id is refused by the intent writer, so the wipe's
	// transaction rolls back with the rows still in place — the failure the
	// abort path is for, reached through the code that would meet it.
	if deleted, ok := r.wipeConversationLocally(ctx, peer, ""); ok {
		t.Fatalf("a wipe whose request could not be written reported success (%d rows)", deleted)
	}
	if cancelled {
		t.Error("an aborted wipe withdrew the deliveries anyway; they can never be sent again")
	}
	if len(thawed) == 0 {
		t.Fatal("an aborted wipe left the deliveries frozen; they would sit unsent forever")
	}
	if string(thawed[0]) != target {
		t.Errorf("thawed %v, want the wipe's own scope", thawed)
	}

	// The thread is untouched: all or nothing, so the user can click again.
	entries, err := c.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("%d rows after a rolled-back wipe, want the thread untouched", len(entries))
	}
	if pendingWorkFor(t, c, peer).Conversation {
		t.Error("a rolled-back wipe still left a request for the peer")
	}
}

// The wipe must wait for a re-offer that has already read its page.
//
// convDeleteRetry's barrier stops this node's own sends and says nothing about
// the paths that write the conversation from the side: the re-offer reads facts
// and hands a COPY of them to the queue, so a wipe landing between those two
// steps deletes rows that are already on their way out again, and then empties
// a queue the callback refills a moment later.
func TestTheWipeWaitsForAReofferThatHasAlreadyReadItsPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.removals = c.removals
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "81111111-2222-4333-8444-555555555555")

	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{Frozen: 1}, nil
	}
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, nil
	}
	r.client.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error {
		return nil
	}

	// A re-offer that has read its page and not yet queued it: exactly the
	// moment between the two steps.
	releaseOffer, admitted := c.removals.admitWrite(peer)
	if !admitted {
		t.Fatal("the fixture could not take a re-offer lease")
	}

	wiped := make(chan error, 1)
	go func() { wiped <- r.SendConversationDelete(ctx, peer) }()

	// Proven to be waiting rather than assumed: the gate counts the removal in
	// before it waits for the leases, so "removing" means the wipe is inside
	// begin(). Without this, "it has not finished within 150ms" is also what a
	// wipe that has not started yet looks like.
	awaitRemovalInFlight(t, c.removals, peer)
	select {
	case err := <-wiped:
		t.Fatalf("the wipe ran while a re-offer was mid-flight: %v", err)
	default:
		// Still waiting, which is the whole point.
	}

	releaseOffer()
	select {
	case err := <-wiped:
		if err != nil {
			t.Fatalf("SendConversationDelete: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wipe never finished after the re-offer released its lease")
	}

	// And with the wipe over, a new re-offer is admitted again: the gate is a
	// window, not a ban.
	release, admitted := c.removals.admitWrite(peer)
	if !admitted {
		t.Fatal("the conversation stayed closed to writes after the wipe")
	}
	release()
}

// The removal gate stops writes and re-offers; it does not stop the send queue,
// which may already hold RESOLVED facts of this thread. Without a pause on it, a
// pass can read them before the wipe, clear the frame gate during it, and hand
// the frame over after the commit — and forgetting the queue afterwards only
// waits for a frame that has already gone.
func TestTheWipeStopsTheSendQueueForTheLengthOfIt(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.removals = c.removals
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "81111111-2222-4333-8444-555555555555")

	paused := make(chan struct{}, 1)
	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		// Called inside the wipe: by now the send queue must already be shut.
		if c.localNode.ReactionSendsHeldFor(peer) {
			paused <- struct{}{}
		}
		return ConversationFreeze{Frozen: 1}, nil
	}
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, nil
	}
	r.client.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error {
		return nil
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	select {
	case <-paused:
	default:
		t.Fatal("the wipe ran with the send queue still open")
	}
	// And it is open again afterwards: the pause is for the length of the wipe,
	// not a state the conversation is left in.
	if c.localNode.ReactionSendsHeldFor(peer) {
		t.Fatal("the send queue stayed shut after the wipe")
	}
}

// A conversation whose only trace is reactions still WAITING for messages that
// never arrived is wiped like any other — and if those rows cannot be dropped,
// the wipe has FAILED. Reporting success there would have the caller empty the
// send queue and tell the user the thread is gone while the only thing in it is
// still on this disk, ready to surface when its message arrives.
func TestWipingAThreadOfOnlyHeldReactionsIsAllOrNothing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, _, _ := newTestDMRouterForConversationDelete(t)
	r.removals = c.removals
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	store := c.chatlog.Store()

	// No messages at all, one fact waiting for one that never came.
	if held, err := store.HoldReactionFact(ctx, domain.ReactionFact{
		Scope: domain.ReactionScopeForPeer(peer),
		Key:   domain.ReactionKey{MessageID: "never-arrived", Actor: peer, Emoji: "👍"},
		Op:    domain.ReactionSet,
		Clock: 1,
	}, time.Now().UTC()); err != nil || !held {
		t.Fatalf("hold: held=%v err=%v", held, err)
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	// Nothing of that conversation is left to become visible later. Asked of the
	// sweep with the TTL already past, because a held row is invisible to every
	// ordinary read — which is exactly why it can outlive a wipe unnoticed.
	swept, err := store.SweepHeldReactions(ctx, time.Now().UTC().Add(2*chatlog.HeldReactionTTL))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("the wipe left %d waiting reactions behind", swept)
	}
}

// The chips are drawn from a per-conversation cache in the window, and the only
// thing that reloads it is TopicReactionsChanged. A wipe that publishes only
// message events leaves the chips of a SURVIVING message — an immutable one —
// on screen against rows that are gone.
func TestAWipeTellsTheUIToReloadTheChips(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.removals = c.removals
	r.eventBus = ebus.New()
	t.Cleanup(r.eventBus.Shutdown)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "8c111111-2222-4333-8444-555555555555")

	reloaded := make(chan domain.PeerIdentity, 4)
	r.eventBus.Subscribe(ebus.TopicReactionsChanged, func(p domain.PeerIdentity) {
		reloaded <- p
	})

	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{Frozen: 1}, nil
	}
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, nil
	}
	r.client.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error {
		return nil
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	select {
	case got := <-reloaded:
		if got != peer {
			t.Fatalf("the reload named %s, want %s", got, peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the wipe never told the UI to reload the conversation's chips")
	}
}
