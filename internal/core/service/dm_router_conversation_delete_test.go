package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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
		seenMessageIDs:                      make(map[string]struct{}),
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

// convDispatchCounter records conversation_delete dispatches.
type convDispatchCounter struct {
	mu    sync.Mutex
	calls []domain.ConversationDeleteRequestID
}

func (d *convDispatchCounter) record(_ context.Context, _ domain.PeerIdentity, requestID domain.ConversationDeleteRequestID) error {
	d.mu.Lock()
	d.calls = append(d.calls, requestID)
	d.mu.Unlock()
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

// pendingDeletesFor reports how many deletions the peer still owes us —
// the count the conversation header renders, and, after a wipe, the whole
// of what "the peer's half" now is.
func pendingDeletesFor(t *testing.T, c *DesktopClient, peer domain.PeerIdentity) int {
	t.Helper()
	counts, err := c.chatlog.Store().DeleteIntentCountsByPeer(context.Background())
	if err != nil {
		t.Fatalf("DeleteIntentCountsByPeer: %v", err)
	}
	return counts[peer]
}

// TestConversationDeleteWipesLocallyForAnOfflinePeer is the case the old
// model refused outright: "Delete chat and ask the peer" with the peer
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

	// The peer's half is three ordinary delete requests, one per
	// message — there is no separate conversation request to carry.
	if owed := pendingDeletesFor(t, c, peer); owed != 3 {
		t.Errorf("the peer owes %d deletions, want 3: one per wiped message", owed)
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

	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "f4000000-2222-4444-8888-cccccccccccc")

	if err := r.SendConversationDelete(context.Background(), peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	if owed := pendingDeletesFor(t, c, peer); owed != 1 {
		t.Fatalf("the peer owes %d deletions, want 1", owed)
	}

	// One sweep is all it takes: the request is an ordinary intent, so
	// the delete scheduler picks it up like any other.
	r.processDeleteRetryDue(context.Background(), time.Now().UTC())

	intent, found := deleteIntentFor(t, c, "f4000000-2222-4444-8888-cccccccccccc")
	if !found {
		t.Fatal("the request vanished instead of being dispatched")
	}
	if intent.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", intent.Attempts)
	}
	if !intent.NextAttemptAt.After(time.Now().UTC()) {
		t.Errorf("next_attempt_at = %s, want it behind the backoff", intent.NextAttemptAt)
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

// TestWipeDoesNotAskAboutMessagesThatNeverWentOut pins the privacy rule
// the single-message withdrawal already keeps under the name `recalled`:
// a message the node can prove never reached the wire is not requested
// from the peer, because the request naming it would be how they learn it
// existed.
func TestWipeDoesNotAskAboutMessagesThatNeverWentOut(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
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

	// The freeze is what says which never reached the wire, and it says
	// so while nothing can emit them any more.
	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{
			Frozen:       2,
			NeverEmitted: map[domain.MessageID]struct{}{domain.MessageID(unsent): {}},
		}, nil
	}
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{Cancelled: 1}, nil
	}
	r.client.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error {
		return nil
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	if _, found := deleteIntentFor(t, c, sent); !found {
		t.Error("no request for a message that did go out")
	}
	if _, found := deleteIntentFor(t, c, unsent); found {
		t.Error("the peer is being asked to delete a message that never left this node")
	}

	// Both rows are gone locally either way.
	entries, err := c.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d rows survived the wipe", len(entries))
	}
}

// TestWipeNeedsNoAnswerToWithholdARequest pins what replaced the parking.
// The proof that a message never went out is written on the row, so the
// transaction that destroys the row decides, and no request for such a
// message is ever written — not written-and-parked, which is a privacy
// rule with a timeout on it. A cancellation that never answers changes
// nothing.
func TestWipeNeedsNoAnswerToWithholdARequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const (
		sent   = "83333333-4444-4555-8666-777777777777"
		unsent = "84444444-4444-4555-8666-777777777777"
	)
	seedConversation(t, c, myAddr, peer, sent)
	seedConversation(t, c, myAddr, peer, unsent)
	if err := c.chatlog.Store().MarkNeverEmitted(ctx, []domain.MessageID{domain.MessageID(unsent)}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	// The node cannot be reached at all, so it contributes nothing.
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, errors.New("node unreachable")
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	if _, found := deleteIntentFor(t, c, unsent); found {
		t.Error("the peer is being asked about a message that never left this node")
	}
	if _, found := deleteIntentFor(t, c, sent); !found {
		t.Error("a failed cancellation swallowed the request for a message that did go out")
	}

	// Nothing is parked: what exists is due, what is not due does not
	// exist. There is no state a timeout could turn into a leak.
	due, err := c.chatlog.Store().DueDeleteIntents(ctx, time.Now().UTC(), 16)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("due = %d, want exactly the one request", len(due))
	}
	if string(due[0].MessageID) != sent {
		t.Errorf("due request names %s, want %s", due[0].MessageID, sent)
	}
}

// TestFailedFreezeAsksAboutEverything: a row's mark only means something
// while nothing can emit the message behind the transaction's back. If the
// delivery engine could not be stopped, the classification is not made at
// all — the peer is asked about ids they may not resolve, rather than a
// message being deleted here while a copy escapes to them.
func TestFailedFreezeAsksAboutEverything(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const unsent = "85555555-4444-4555-8666-777777777777"
	seedConversation(t, c, myAddr, peer, unsent)
	if err := c.chatlog.Store().MarkNeverEmitted(ctx, []domain.MessageID{domain.MessageID(unsent)}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	r.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{}, errors.New("node unreachable")
	}
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, errors.New("node unreachable")
	}

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	if _, found := deleteIntentFor(t, c, unsent); !found {
		t.Error("the mark was trusted although nothing could stop the message from going out")
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

	// An id the intent writer refuses (not a UUID v4) makes the wipe's
	// transaction roll back — the failure the abort path is for.
	const target = "not-a-uuid"
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

	if err := r.SendConversationDelete(ctx, peer); err == nil {
		t.Fatal("the wipe reported success against a dead database")
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
}
