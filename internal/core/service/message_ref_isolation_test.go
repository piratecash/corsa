package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestDeletedMessageRefNeverConfirmsAnotherAuthor(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	self := domain.PeerIdentityFromWire(id.Address)
	bob := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	carol := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	store := newTestChatlogStore(t, self)
	ctx := context.Background()
	now := time.Now().UTC()
	refusals := newWipeTombstoneSet(func() deleteTaskList { return store })
	refusals.Hydrate(ctx, now)
	adapter := NewMessageStoreAdapter(NewChatlogGateway(store, self), id, refusals, nil)
	envelope := protocol.Envelope{Topic: "dm", ID: "23456789-1234-4234-8234-123456789abc", Sender: bob.String(), Recipient: id.Address, Payload: []byte("sealed-bob"), CreatedAt: now}
	if got := adapter.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("initial store=%v", got)
	}
	ref := envelopeRefForServiceTest(envelope)
	intent := chatlog.DeleteIntent{MessageID: domain.MessageID(ref.ID), Peer: bob, Ref: ref, CreatedAt: now, NextAttemptAt: now}
	if removed, err := store.DeleteWithIntent(ctx, intent); err != nil || !removed {
		t.Fatalf("delete=%v %v", removed, err)
	}
	// The durable intent restores exactly the previously deleted tuple.
	restarted := newWipeTombstoneSet(func() deleteTaskList { return store })
	restarted.Hydrate(ctx, now)
	adapter = NewMessageStoreAdapter(NewChatlogGateway(store, self), id, restarted, nil)
	if got := adapter.StoreMessage(envelope, false); got != node.StoreDuplicate {
		t.Fatalf("exact refusal=%v", got)
	}
	envelope.Sender = carol.String()
	envelope.Payload = []byte("sealed-carol")
	if got := adapter.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("another author received false duplicate=%v", got)
	}
	if row, found, err := store.EntryByRef(ctx, envelopeRefForServiceTest(envelope)); err != nil || !found || row.Body != "sealed-carol" {
		t.Fatalf("author row=%+v %v %v", row, found, err)
	}
	// An old task without endpoints can prevent resurrection, but cannot be
	// evidence that an arriving message was already delivered.
	restarted.NoteForPeer(bob, []domain.MessageID{"unknown-old-target"}, now)
	envelope.ID = "unknown-old-target"
	envelope.Sender = bob.String()
	if got := adapter.StoreMessage(envelope, false); got != node.StoreDeferred {
		t.Fatalf("unbound refusal falsely confirmed delivery=%v", got)
	}
	if _, found, err := store.EntryByRef(ctx, envelopeRefForServiceTest(envelope)); err != nil || found {
		t.Fatalf("deferred row appeared=%v %v", found, err)
	}
	envelope.Sender = carol.String()
	if got := adapter.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("another conversation was held by legacy target=%v", got)
	}
}

func envelopeRefForServiceTest(e protocol.Envelope) domain.MessageRef {
	return domain.MessageRef{Topic: e.Topic, ID: string(e.ID), Sender: e.Sender, Recipient: e.Recipient}
}

func TestConversationCacheSeparatesOppositeDirectionMessageRefs(t *testing.T) {
	self := domain.PeerIdentityFromWire(strings.Repeat("a", 40))
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	outgoing := DirectMessage{ID: "same", Sender: self, Recipient: peer, Body: "outgoing", ReceiptStatus: MessageStatusSent, Seq: 1}
	incoming := DirectMessage{ID: "same", Sender: peer, Recipient: self, Body: "incoming", ReceiptStatus: MessageStatusDelivered, Seq: 2}
	cache := NewConversationCache()
	cache.Load(peer, []DirectMessage{outgoing, incoming}, 0)
	if !cache.HasMessageRef(directMessageRef(outgoing)) || !cache.HasMessageRef(directMessageRef(incoming)) {
		t.Fatal("cache merged opposite directions")
	}
	if cache.UpdateStatus("same", MessageStatusSeen, domain.OptionalTime{}, true) || cache.RemoveMessage("same") {
		t.Fatal("legacy ambiguous cache request mutated a row")
	}
	if !cache.UpdateStatusRef(directMessageRef(outgoing), MessageStatusSeen, domain.OptionalTime{}, true) {
		t.Fatal("exact receipt did not update outgoing")
	}
	// Reconciliation must retain the live exact receipt without copying it to
	// the opposite-direction neighbour that shares the ID.
	cache.Load(peer, []DirectMessage{outgoing, incoming}, 2)
	messages := cache.Messages()
	if len(messages) != 2 {
		t.Fatalf("cache rows=%d", len(messages))
	}
	for _, message := range messages {
		if message.Sender == self && message.ReceiptStatus != MessageStatusSeen {
			t.Fatal("live outgoing status regressed")
		}
		if message.Sender == peer && message.ReceiptStatus != MessageStatusDelivered {
			t.Fatal("receipt crossed original direction")
		}
	}
	if !cache.RemoveMessageRef(directMessageRef(outgoing)) || !cache.HasMessageRef(directMessageRef(incoming)) {
		t.Fatal("exact cache removal removed the neighbour")
	}
}

// Deferred is the Node contract that emits no delivered receipt or dedup mark.
// Invalid CHECK values must not pass through the transient StoreFailed fallback.
func TestInvalidStoredEntryCannotBecomeAConfirmedDuplicate(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	self := domain.PeerIdentityFromWire(id.Address)
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	store := newTestChatlogStore(t, self)
	adapter := NewMessageStoreAdapter(NewChatlogGateway(store, self), id, nil, nil)
	envelope := protocol.Envelope{Topic: "dm", ID: "invalid-flag", Sender: peer.String(), Recipient: self.String(), Flag: protocol.MessageFlag("invalid-flag"), Payload: []byte("sealed"), CreatedAt: time.Now().UTC()}
	if got := adapter.StoreMessage(envelope, false); got != node.StoreDeferred {
		t.Fatalf("rejected entry could be confirmed: result=%v", got)
	}
	if _, found, err := store.EntryByRef(context.Background(), envelopeRefForServiceTest(envelope)); err != nil || found {
		t.Fatalf("invalid entry stored: found=%v err=%v", found, err)
	}
}

func TestReceiptEventUpdatesOnlyItsOriginalMessageReference(t *testing.T) {
	r := newSyncTestRouter()
	self := r.client.Address()
	peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	outgoing := DirectMessage{ID: "same-receipt", Sender: self, Recipient: peer, ReceiptStatus: MessageStatusSent}
	incoming := DirectMessage{ID: "same-receipt", Sender: peer, Recipient: self, ReceiptStatus: MessageStatusDelivered}
	r.cache.Load(peer, []DirectMessage{outgoing, incoming}, 0)
	r.mu.Lock()
	r.activePeer = peer
	r.activeMessages = r.cache.Messages()
	r.mu.Unlock()
	// This is the unchanged receiptUpdateEvent producer's direction: the
	// reader issues seen to the original author, not the message's direction.
	r.onReceiptUpdate(protocol.LocalChangeEvent{Type: protocol.LocalChangeReceiptUpdate, Topic: "dm", MessageID: outgoing.ID, Sender: peer.String(), Recipient: self.String(), Status: protocol.ReceiptStatusSeen})
	messages := r.cache.Messages()
	if len(messages) != 2 {
		t.Fatalf("receipt lost collision rows: got %d, want 2", len(messages))
	}
	seenOutgoing, seenIncoming := false, false
	for _, msg := range messages {
		if directMessageRef(msg) == directMessageRef(outgoing) {
			seenOutgoing = true
			if msg.ReceiptStatus != MessageStatusSeen {
				t.Fatalf("original outgoing status=%s", msg.ReceiptStatus)
			}
		}
		if directMessageRef(msg) == directMessageRef(incoming) {
			seenIncoming = true
			if msg.ReceiptStatus != MessageStatusDelivered {
				t.Fatalf("receipt crossed direction: %s", msg.ReceiptStatus)
			}
		}
	}
	if !seenOutgoing || !seenIncoming {
		t.Fatalf("receipt replaced collision references: outgoing=%v incoming=%v", seenOutgoing, seenIncoming)
	}
}

func TestLegacyDeleteRequiresOneGlobalReferenceBeforeMutation(t *testing.T) {
	r, c, self, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	bob := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
	carol := domain.PeerIdentityFromWire(strings.Repeat("c", 40))
	const target = "6d6d6d6d-2222-4444-8888-cccccccccccc"
	for _, peer := range []domain.PeerIdentity{bob, carol} {
		insertUnconfirmedOutgoing(t, c, self, peer, target)
	}
	froze, thawed := 0, 0
	c.freezeMessageDeliveryToFn = func(_ context.Context, peer domain.PeerIdentity, id domain.MessageID) (bool, error) {
		if peer != bob || string(id) != target {
			t.Fatalf("wrong scoped freeze %s/%s", peer, id)
		}
		froze++
		return true, nil
	}
	c.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error { thawed++; return nil }
	if _, err := r.SendMessageDelete(context.Background(), bob, domain.MessageID(target)); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("legacy ambiguous deletion=%v", err)
	}
	if froze != 0 || thawed != 0 || counter.count() != 0 {
		t.Fatalf("ambiguous selector mutated: freeze=%d thaw=%d dispatch=%d", froze, thawed, counter.count())
	}
	for _, peer := range []domain.PeerIdentity{bob, carol} {
		ref := domain.MessageRef{Topic: "dm", ID: target, Sender: self.String(), Recipient: peer.String()}
		if _, found, err := c.chatLog.EntryByRef(context.Background(), ref); err != nil || !found {
			t.Fatalf("ambiguous selector lost row %v: %v %v", ref, found, err)
		}
	}
	selected := domain.MessageRef{Topic: "dm", ID: target, Sender: self.String(), Recipient: bob.String()}
	if route, err := r.SendMessageDeleteRef(context.Background(), selected); err != nil || route != domain.MessageDeleteRouteRecalled {
		t.Fatalf("exact delete route=%s err=%v", route, err)
	}
	if _, found, err := c.chatLog.EntryByRef(context.Background(), selected); err != nil || found {
		t.Fatalf("selected row remains: %v %v", found, err)
	}
	other := domain.MessageRef{Topic: "dm", ID: target, Sender: self.String(), Recipient: carol.String()}
	if _, found, err := c.chatLog.EntryByRef(context.Background(), other); err != nil || !found {
		t.Fatalf("other recipient lost row: %v %v", found, err)
	}
	// Once only Carol's row remains, the historical wrapper must derive
	// Carol even when the caller still supplies Bob. The exact freeze is an
	// observation of the actual selected conversation before local deletion.
	c.freezeMessageDeliveryToFn = func(_ context.Context, peer domain.PeerIdentity, id domain.MessageID) (bool, error) {
		if peer != carol || string(id) != target {
			t.Fatalf("legacy wrapper trusted wrong caller: %s/%s", peer, id)
		}
		return true, nil
	}
	if route, err := r.SendMessageDelete(context.Background(), bob, domain.MessageID(target)); err != nil || route != domain.MessageDeleteRouteRecalled {
		t.Fatalf("unique legacy delete route=%s err=%v", route, err)
	}
	if _, found, err := c.chatLog.EntryByRef(context.Background(), other); err != nil || found {
		t.Fatalf("derived unique row remains: %v %v", found, err)
	}
}

func TestUnattemptedScopedFreezeHasNoCompensation(t *testing.T) {
	for _, kind := range []string{"incoming", "absent"} {
		t.Run(kind, func(t *testing.T) {
			r, c, self, counter := newTestDMRouterForDelete(t)
			r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
			peer := domain.PeerIdentityFromWire(strings.Repeat("b", 40))
			const target = "6e6e6e6e-2222-4444-8888-cccccccccccc"
			if kind == "incoming" {
				insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{ID: target, Sender: peer.String(), Recipient: self.String(), Body: "sealed", Flag: chatlog.FlagAnyDelete, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
			}
			froze, thawed := 0, 0
			c.freezeMessageDeliveryToFn = func(context.Context, domain.PeerIdentity, domain.MessageID) (bool, error) { froze++; return false, nil }
			c.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error { thawed++; return nil }
			if _, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target)); err != nil {
				t.Fatal(err)
			}
			if froze != 0 || thawed != 0 {
				t.Fatalf("%s changed unrelated own freeze: acquired=%d thawed=%d", kind, froze, thawed)
			}
			if counter.count() != 0 {
				t.Fatal("unreachable peer was dispatched")
			}
		})
	}
}
