package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// receipt_backlog_test.go pins the own-identity delivery-receipt backlog
// bounds added to storeDeliveryReceipt: the unsolicited-receipt gate (a
// delivered/seen receipt for a message we never sent is dropped) and the
// per-identity backlog cap (legitimate own receipts are bounded, oldest
// evicted).

func ownIdentityReceipt(messageID, recipient string) protocol.DeliveryReceipt {
	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(messageID),
		Sender:      "some-peer",
		Recipient:   recipient,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
}

func TestStoreDeliveryReceipt_DropsUnsolicitedOwnIdentity(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// We never sent this message — the receipt is unsolicited and must be
	// dropped without entering the backlog.
	stored, count := svc.storeDeliveryReceipt(ownIdentityReceipt("phantom-1", id.Address))
	if stored {
		t.Fatal("unsolicited own-identity receipt must not be stored")
	}
	if count != 0 {
		t.Fatalf("backlog must stay empty for an unsolicited receipt, got %d", count)
	}

	svc.deliveryMu.RLock()
	got := len(svc.receipts[id.Address])
	svc.deliveryMu.RUnlock()
	if got != 0 {
		t.Fatalf("s.receipts must not retain an unsolicited receipt, got %d", got)
	}
}

func TestStoreDeliveryReceipt_AcceptsSolicitedOwnIdentity(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// Record that we sent the message — now its receipt is solicited.
	svc.sentDMIDs.Add("real-1")
	stored, _ := svc.storeDeliveryReceipt(ownIdentityReceipt("real-1", id.Address))
	if !stored {
		t.Fatal("solicited own-identity receipt must be stored")
	}

	svc.deliveryMu.RLock()
	got := len(svc.receipts[id.Address])
	svc.deliveryMu.RUnlock()
	if got != 1 {
		t.Fatalf("solicited receipt must enter the backlog, got %d", got)
	}
}

func TestStoreDeliveryReceipt_OwnBacklogCapped(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// Store more than the cap's worth of solicited own-identity receipts.
	total := maxReceiptBacklogPerRecipient + 500
	for i := 0; i < total; i++ {
		mid := fmt.Sprintf("own-%d", i)
		svc.sentDMIDs.Add(mid)
		svc.storeDeliveryReceipt(ownIdentityReceipt(mid, id.Address))
	}

	svc.deliveryMu.RLock()
	got := len(svc.receipts[id.Address])
	svc.deliveryMu.RUnlock()
	if got > maxReceiptBacklogPerRecipient {
		t.Fatalf("own backlog must be bounded by the cap, got %d > %d", got, maxReceiptBacklogPerRecipient)
	}
	if got != maxReceiptBacklogPerRecipient {
		t.Fatalf("own backlog should sit at the cap after overflow, got %d", got)
	}
	// The most recent receipt must survive; the oldest must have been evicted.
	if !svc.seenReceipts.Has(id.Address + ":own-" + fmt.Sprint(total-1) + ":delivered") {
		t.Fatal("most recent receipt's dedup entry must be present")
	}
}

func TestStoreDeliveryReceipt_EvictionClearsShadows(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// Own-identity receipts exercise the cap + shadow-cleanup without the async
	// subscriber-push path (own identity returns before distributeReceipt), so
	// the eviction logic — shared by every recipient — is tested in isolation.
	// Each receipt is registered as locally-sent so the unsolicited gate admits
	// it.
	recipient := id.Address
	first := ownIdentityReceipt("evict-0", recipient)
	svc.sentDMIDs.Add("evict-0")
	svc.storeDeliveryReceipt(first)

	// Simulate the relayRetry entry trackRelayReceipt would attach, and confirm
	// the dedup entry exists before eviction.
	firstKey := recipient + ":evict-0:delivered"
	svc.deliveryMu.Lock()
	svc.relayRetry[relayReceiptKey(first)] = relayAttempt{FirstSeen: time.Now().UTC()}
	svc.deliveryMu.Unlock()
	if !svc.seenReceipts.Has(firstKey) {
		t.Fatal("first receipt must be deduped before eviction")
	}

	// Overflow the cap by exactly one so `first` (the oldest) is evicted.
	for i := 1; i <= maxReceiptBacklogPerRecipient; i++ {
		mid := fmt.Sprintf("evict-%d", i)
		svc.sentDMIDs.Add(mid)
		svc.storeDeliveryReceipt(ownIdentityReceipt(mid, recipient))
	}

	svc.deliveryMu.RLock()
	_, retryShadow := svc.relayRetry[relayReceiptKey(first)]
	backlog := len(svc.receipts[recipient])
	svc.deliveryMu.RUnlock()

	if backlog != maxReceiptBacklogPerRecipient {
		t.Fatalf("backlog must sit at the cap, got %d", backlog)
	}
	if svc.seenReceipts.Has(firstKey) {
		t.Fatal("evicted receipt's seenReceipts shadow must be cleared (re-send can restore backlog)")
	}
	if retryShadow {
		t.Fatal("evicted receipt's relayRetry shadow must be cleared (no orphan toward maxRelayRetryEntries)")
	}
}

func TestRemoveSubscriber_DropsReceiptBacklog(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	const recipient = "remote-subscriber-identity"
	const connID = domain.ConnID(42)

	// Register an active subscriber for the recipient over connID.
	svc.gossipMu.Lock()
	svc.subs[recipient] = map[string]*subscriber{
		"s1": {id: "s1", recipient: recipient, connID: connID},
	}
	svc.gossipMu.Unlock()

	// Seed the backlog + a dedup entry directly. storeDeliveryReceipt would
	// kick off an async push to this fake-connID subscriber, whose failed write
	// removes the subscriber itself — populating the maps directly isolates the
	// disconnect-drop behaviour under test.
	dedupKey := recipient + ":sub-msg-1:delivered"
	svc.deliveryMu.Lock()
	svc.receipts[recipient] = []protocol.DeliveryReceipt{ownIdentityReceipt("sub-msg-1", recipient)}
	svc.seenReceipts.Add(dedupKey)
	svc.deliveryMu.Unlock()

	// Subscriber disconnects → its backlog must be dropped (no store-and-forward
	// for an offline recipient; the sender's end-to-end retry re-delivers).
	svc.removeSubscriberConnID(connID)

	svc.deliveryMu.RLock()
	_, stillBuffered := svc.receipts[recipient]
	svc.deliveryMu.RUnlock()
	if stillBuffered {
		t.Fatal("receipt backlog must be dropped when the subscriber disconnects")
	}
	if svc.seenReceipts.Has(dedupKey) {
		t.Fatal("dedup shadow must be cleared so a later re-send is not suppressed")
	}
}

func TestDropReceiptBacklog_SkipsReconnectedRecipient(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	const recipient = "reconnected-subscriber-identity"

	// Recipient is online (active subscriber) with a buffered receipt. This
	// stands in for the reconnect race: a fresh subscriber exists when the stale
	// teardown's drop runs. Seed the maps directly (storeDeliveryReceipt's async
	// push to the fake-connID subscriber would remove it and skew the test).
	svc.gossipMu.Lock()
	svc.subs[recipient] = map[string]*subscriber{
		"s1": {id: "s1", recipient: recipient, connID: domain.ConnID(7)},
	}
	svc.gossipMu.Unlock()
	svc.deliveryMu.Lock()
	svc.receipts[recipient] = []protocol.DeliveryReceipt{ownIdentityReceipt("rc-1", recipient)}
	svc.deliveryMu.Unlock()

	// The stale teardown invokes the drop for this recipient — but it is online
	// (subscriber present), so its backlog must be preserved (no delivery
	// regression). The re-check inside the drop sees the subscriber and skips.
	svc.dropReceiptBacklogForOfflineRecipients([]string{recipient})

	svc.deliveryMu.RLock()
	n := len(svc.receipts[recipient])
	svc.deliveryMu.RUnlock()
	if n == 0 {
		t.Fatal("backlog of a reconnected recipient (active subscriber) must NOT be dropped")
	}
}

func TestStoreDeliveryReceipt_DropsRaceReceiptWithoutTouchingState(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id

	// Pre-existing live delivery state keyed by a MessageID the dropped receipt
	// collides with — it must survive untouched.
	const msgID = "race-msg-1"
	const recipient = "offline-non-subscriber-identity"
	svc.deliveryMu.Lock()
	svc.outbound[msgID] = outboundDelivery{MessageID: msgID, Status: "queued"}
	svc.deliveryMu.Unlock()

	// Non-self sender, non-self recipient, no active subscriber: the race where
	// the recipient's last subscriber disconnected between the handler's
	// hasSubscriber gate and the store. The receipt must be dropped cleanly.
	stored, _ := svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID:   msgID,
		Sender:      "some-other-peer",
		Recipient:   recipient,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	})

	if stored {
		t.Fatal("a receipt for an offline non-subscriber recipient must report not-stored")
	}
	svc.deliveryMu.RLock()
	_, outboundKept := svc.outbound[msgID]
	_, buffered := svc.receipts[recipient]
	svc.deliveryMu.RUnlock()
	if !outboundKept {
		t.Fatal("dropped race receipt must NOT erase live outbound state on a colliding MessageID")
	}
	if buffered {
		t.Fatal("dropped race receipt must not be buffered")
	}
	if svc.seenReceipts.Has(recipient + ":" + msgID + ":delivered") {
		t.Fatal("dropped race receipt must not poison the dedup set")
	}
}
