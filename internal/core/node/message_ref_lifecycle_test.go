package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOwnMessageRefLifecycleKeepsRecipientsIndependent(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	bob, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	carol, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	const id = protocol.MessageID("own-shared-id")
	for _, peer := range []*identity.Identity{bob, carol} {
		body := sealDMBody(t, svc.identity, peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey))
		stored, _, code := svc.storeIncomingMessage(incomingMessage{ID: id, Topic: "dm", Sender: svc.Address(), Recipient: peer.Address, Body: body, Flag: protocol.MessageFlagImmutable, CreatedAt: now}, true)
		if !stored || code != "" {
			t.Fatalf("own send to %s=%v %s", peer.Address, stored, code)
		}
	}
	bobRef := svc.ownMessageIdentity(id, bob.Address)
	carolRef := svc.ownMessageIdentity(id, carol.Address)
	svc.deliveryMu.RLock()
	count := len(svc.awaitingDelivered)
	a, b := svc.awaitingDelivered[bobRef], svc.awaitingDelivered[carolRef]
	svc.deliveryMu.RUnlock()
	if count != 2 || a == nil || b == nil || a.Envelope.Recipient != bob.Address || b.Envelope.Recipient != carol.Address {
		t.Fatal("own retry registration merged recipients")
	}
	if _, err := svc.freezeDeliveries([]protocol.MessageID{id}); !errors.Is(err, domain.ErrAmbiguousMessage) {
		t.Fatalf("legacy freeze did not refuse ambiguity: %v", err)
	}
	if frozen, err := svc.FreezeOutgoingDeliveriesTo(domain.PeerIdentityFromWire(bob.Address), []protocol.MessageID{id}); err != nil || frozen.Frozen != 1 {
		t.Fatalf("scoped freeze=%+v %v", frozen, err)
	}
	svc.deliveryMu.RLock()
	bobFrozen, carolFrozen := svc.deliveryFrozenLocked(bobRef), svc.deliveryFrozenLocked(carolRef)
	svc.deliveryMu.RUnlock()
	if !bobFrozen || carolFrozen {
		t.Fatal("scoped freeze crossed recipient")
	}
	if err := svc.ThawOutgoingDeliveriesTo(domain.PeerIdentityFromWire(bob.Address), []protocol.MessageID{id}); err != nil {
		t.Fatal(err)
	}
	svc.storeDeliveryReceipt(protocol.DeliveryReceipt{MessageID: id, Sender: bob.Address, Recipient: svc.Address(), Status: protocol.ReceiptStatusDelivered, DeliveredAt: now})
	svc.deliveryMu.RLock()
	_, bobPending := svc.awaitingDelivered[bobRef]
	_, carolPending := svc.awaitingDelivered[carolRef]
	svc.deliveryMu.RUnlock()
	if bobPending || !carolPending {
		t.Fatal("one recipient's receipt retired another recipient's retry")
	}
	result, err := svc.CancelOutgoingDelivery(id, domain.PeerIdentityFromWire(carol.Address))
	if err != nil || !result.RetryCancelled {
		t.Fatalf("scoped cancellation=%+v %v", result, err)
	}
	svc.gossipMu.RLock()
	var retainedBob, retainedCarol bool
	for _, e := range svc.topics["dm"] {
		if messageIdentityOf(e) == bobRef {
			retainedBob = true
		}
		if messageIdentityOf(e) == carolRef {
			retainedCarol = true
		}
	}
	svc.gossipMu.RUnlock()
	if !retainedBob || retainedCarol {
		t.Fatal("cancellation removed a colliding recipient's backlog row")
	}
}

func TestLegacyAckDeleteRefusesCollidingAuthorsBeforeMutation(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	now := time.Now().UTC()
	rows := []protocol.Envelope{{Topic: "dm", ID: "ack-collision", Sender: "author-a", Recipient: svc.Address(), Payload: []byte("a"), StoredAt: now}, {Topic: "dm", ID: "ack-collision", Sender: "author-b", Recipient: svc.Address(), Payload: []byte("b"), StoredAt: now}}
	svc.gossipMu.Lock()
	svc.topics["dm"] = rows
	svc.gossipMu.Unlock()
	if removed := svc.deleteBacklogMessageForRecipient(svc.Address(), "ack-collision"); removed != -1 {
		t.Fatalf("ambiguous ack_delete result=%d", removed)
	}
	svc.gossipMu.RLock()
	defer svc.gossipMu.RUnlock()
	if len(svc.topics["dm"]) != 2 || string(svc.topics["dm"][0].Payload) != "a" || string(svc.topics["dm"][1].Payload) != "b" {
		t.Fatal("ambiguous ack_delete mutated backlog")
	}
}

func TestExpiryRemovesOnlyTheExpiredMessageRef(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	now := time.Now().UTC()
	svc.gossipMu.Lock()
	svc.topics["dm"] = []protocol.Envelope{
		{Topic: "dm", ID: "same-expiry", Sender: "author-a", Recipient: svc.Address(), Payload: []byte("expired"), Flag: protocol.MessageFlagAutoDeleteTTL, TTLSeconds: 1, CreatedAt: now.Add(-2 * time.Second), StoredAt: now},
		{Topic: "dm", ID: "same-expiry", Sender: "author-b", Recipient: svc.Address(), Payload: []byte("live"), Flag: protocol.MessageFlagAutoDeleteTTL, TTLSeconds: 600, CreatedAt: now.Add(-2 * time.Second), StoredAt: now},
	}
	svc.gossipMu.Unlock()
	svc.cleanupExpiredMessagesForce()
	svc.gossipMu.RLock()
	defer svc.gossipMu.RUnlock()
	if rows := svc.topics["dm"]; len(rows) != 1 || rows[0].Sender != "author-b" || string(rows[0].Payload) != "live" {
		t.Fatalf("expiry crossed author: %+v", rows)
	}
}

func TestMessagePendingNamespaceDoesNotMergeDMAndControl(t *testing.T) {
	frame := protocol.Frame{Type: "send_message", ID: "namespace", Topic: "dm", Address: "author", Recipient: "recipient"}
	message := messageIdentityOf(frameEnvelope(frame))
	control := frame
	control.Topic = protocol.TopicControlDM
	if pendingFrameKey("peer", frame) == pendingFrameKey("peer", control) {
		t.Fatal("pending ring merged different message topics")
	}
	if !frameCarriesMessage(frame, message) || frameCarriesMessage(control, message) {
		t.Fatal("DM lifecycle matcher crossed control namespace")
	}
}

func TestPendingEvictionRetiresOnlyTheExactDMReference(t *testing.T) {
	base := protocol.Frame{Type: "send_message", ID: "eviction-collision", Topic: "dm", Address: "author", Recipient: "recipient"}
	ref := messageIdentityOf(frameEnvelope(base))
	for _, tc := range []struct {
		name    string
		change  func(*protocol.Frame)
		removed bool
	}{
		{"exact", func(*protocol.Frame) {}, true},
		{"other-author", func(f *protocol.Frame) { f.Address = "other-author" }, false},
		{"other-recipient", func(f *protocol.Frame) { f.Recipient = "other-recipient" }, false},
		{"global-topic", func(f *protocol.Frame) { f.Topic = "global" }, false},
		{"control-topic", func(f *protocol.Frame) { f.Topic = protocol.TopicControlDM }, false},
		{"receipt-namespace", func(f *protocol.Frame) { f.Type = "relay_delivery_receipt" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := base
			tc.change(&frame)
			peer := domain.PeerAddress("fixture-peer")
			key := pendingFrameKey(peer, frame)
			svc := &Service{outbound: map[messageIdentity]outboundDelivery{ref: {MessageID: string(base.ID), Recipient: base.Recipient, Status: "queued"}}, pendingKeys: map[pendingKey]struct{}{key: {}}}
			svc.deliveryMu.Lock()
			svc.releaseEvictedPendingLocked(peer, frame, 1)
			_, retained := svc.outbound[ref]
			_, keyRetained := svc.pendingKeys[key]
			svc.deliveryMu.Unlock()
			if retained == tc.removed || keyRetained {
				t.Fatalf("exact DM retained=%v want=%v; evicted frame key retained=%v", retained, !tc.removed, keyRetained)
			}
		})
	}
}

func TestFetchMessageRequiresAnUnambiguousLocalReference(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	now := time.Now().UTC()
	svc.gossipMu.Lock()
	svc.topics["dm"] = []protocol.Envelope{
		{Topic: "dm", ID: "fetch-collision", Sender: "author-a", Recipient: svc.Address(), Payload: []byte("first"), CreatedAt: now, StoredAt: now},
		{Topic: "dm", ID: "fetch-collision", Sender: "author-b", Recipient: svc.Address(), Payload: []byte("second"), CreatedAt: now, StoredAt: now},
	}
	svc.gossipMu.Unlock()
	legacy := svc.fetchMessageFrame("dm", "fetch-collision")
	if legacy.Type != "error" || legacy.Code != protocol.ErrCodeInvalidFetchMessage || legacy.Item != nil {
		t.Fatalf("legacy fetch picked first author: %+v", legacy)
	}
	scoped := svc.fetchMessageFrame("dm", "fetch-collision", "author-b", svc.Address())
	if scoped.Type != "message" || scoped.Item == nil || scoped.Item.Sender != "author-b" || scoped.Item.Body != "second" {
		t.Fatalf("exact fetch=%+v", scoped)
	}
	partial := svc.fetchMessageFrame("dm", "fetch-collision", "author-b", "")
	if partial.Type != "error" || partial.Code != protocol.ErrCodeInvalidFetchMessage {
		t.Fatal("partial reference became wildcard")
	}
}

func TestPendingMessagesDoesNotProjectDMStateIntoAnotherTopic(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	svc.deliveryMu.Lock()
	svc.outbound[svc.ownMessageIdentity("topic-collision", "peer")] = outboundDelivery{Recipient: "peer", Status: "queued"}
	svc.deliveryMu.Unlock()
	frame := svc.pendingMessagesFrame("global")
	if frame.Count != 0 || len(frame.PendingMessages) != 0 || len(frame.PendingIDs) != 0 {
		t.Fatalf("DM state projected into global namespace: %+v", frame)
	}
}
