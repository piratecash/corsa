package node

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// receipt_relay_retry_commit_test.go pins the invariant that ties a transit
// receipt's relay retry entry to its backlog entry: a `receipt|` key in
// s.relayRetry exists only while the receipt it names is in s.receipts.
// Almost every reaper of those keys (retryableRelayReceipts and its TTL, the
// offline-recipient drop, ack_delete, the per-recipient cap) walks the
// backlog. The only one that goes by key, clearRelayRetryForOutbound, fires
// when a relay_delivery_receipt frame is accepted into an outbound peer
// session's writer queue, so a key without its receipt whose sends never got
// that far holds a maxRelayRetryEntries slot until the process exits.

func transitReceiptFor(messageID, recipient string) protocol.DeliveryReceipt {
	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(messageID),
		Sender:      "remote-receipt-author",
		Recipient:   recipient,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
}

// newInitMapsTestService is a Service with its maps initialised and nothing
// else; id may be nil for a test that never reads the local identity.
func newInitMapsTestService(t *testing.T, id *identity.Identity) *Service {
	t.Helper()
	svc := &Service{}
	svc.initMaps()
	svc.identity = id
	return svc
}

// commitTransitReceiptForTest runs the real commit for a transit receipt,
// behind a subscriber on a registered connection so the commit's subscriber
// re-check admits it, for tests about what happens to the committed state
// afterwards.
func commitTransitReceiptForTest(t *testing.T, svc *Service, receipt protocol.DeliveryReceipt, connID netcore.ConnID) {
	t.Helper()
	_ = attachPushObserver(t, svc, receipt.Recipient, connID)
	if _, _, _, result := svc.finishReceipt(receiptKeyOf(receipt), receipt, false); result != committedFirst {
		t.Fatalf("fixture commit result = %v, want committedFirst", result)
	}
}

// The commit that appends a transit receipt to the backlog is the one that
// arms its relay retry, so there is no moment at which a teardown can reclaim
// the backlog and leave the retry entry to be created after it.
func TestFinishReceipt_TransitCommitArmsRelayRetryWithTheBacklog(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := newInitMapsTestService(t, id)

	const recipient = "transit-subscriber-identity"
	_ = attachPushObserver(t, svc, recipient, netcore.ConnID(9401))
	receipt := transitReceiptFor("commit-arms-1", recipient)

	_, _, _, result := svc.finishReceipt(receiptKeyOf(receipt), receipt, false)
	if result != committedFirst {
		t.Fatalf("commit result = %v, want committedFirst", result)
	}

	svc.deliveryMu.RLock()
	backlog := len(svc.receipts[recipient])
	_, armed := svc.relayRetry[relayReceiptKey(receipt)]
	svc.deliveryMu.RUnlock()
	if backlog != 1 {
		t.Fatalf("backlog after commit = %d, want 1", backlog)
	}
	if !armed {
		t.Fatal("the commit that put the transit receipt in the backlog must also arm its relay retry")
	}

	// The subscriber's teardown reclaims both halves together.
	svc.removeSubscriberByID(recipient, "observer")

	svc.deliveryMu.RLock()
	backlog = len(svc.receipts[recipient])
	_, armed = svc.relayRetry[relayReceiptKey(receipt)]
	svc.deliveryMu.RUnlock()
	if backlog != 0 || armed {
		t.Fatalf("after teardown: backlog=%d relayRetry armed=%v, want both gone", backlog, armed)
	}
}

// A receipt addressed to this node is backlogged by the same commit but has
// arrived: it is not relayed onward, so the commit must not arm it. (seen_ack
// is not covered here: the commit never backlogs it, so the arm is not
// reached for it at all.)
func TestFinishReceipt_OwnIdentityCommitDoesNotArmRelayRetry(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := newInitMapsTestService(t, id)
	receipt := transitReceiptFor("own-1", id.Address)

	_, _, _, result := svc.finishReceipt(receiptKeyOf(receipt), receipt, false)
	if result != committedFirst {
		t.Fatalf("commit result = %v, want committedFirst", result)
	}
	svc.deliveryMu.RLock()
	backlog := len(svc.receipts[id.Address])
	_, armed := svc.relayRetry[relayReceiptKey(receipt)]
	svc.deliveryMu.RUnlock()
	if backlog != 1 {
		t.Fatalf("own-identity backlog after commit = %d, want 1", backlog)
	}
	if armed {
		t.Fatal("a receipt addressed to this node must not be armed for relay retry")
	}
}

// A refused status write commits nothing, so it must not arm anything
// either: the receipt never reaches the backlog, and an entry armed for it
// would be visited by no backlog walk.
func TestStoreDeliveryReceipt_RefusedWriteArmsNoRelayRetry(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := newInitMapsTestService(t, id)
	store := NewMockMessageStore(t)
	store.EXPECT().UpdateDeliveryStatus(mock.Anything).Return(false)
	svc.messageStore = store

	const recipient = "transit-subscriber-identity"
	_ = attachPushObserver(t, svc, recipient, netcore.ConnID(9403))
	receipt := transitReceiptFor("refused-write-1", recipient)

	outcome := svc.storeDeliveryReceipt(receipt)
	svc.WaitBackground()
	if outcome.ackable {
		t.Fatal("a receipt whose write was refused must not be ackable")
	}

	svc.deliveryMu.RLock()
	backlog := len(svc.receipts[recipient])
	_, armed := svc.relayRetry[relayReceiptKey(receipt)]
	svc.deliveryMu.RUnlock()
	if backlog != 0 {
		t.Fatalf("backlog after a refused write = %d, want 0", backlog)
	}
	if armed {
		t.Fatal("a refused write left a relayRetry entry with no backlog receipt behind it")
	}
}

// End to end through the store door. This is a stress test, not a
// deterministic one: it needs real parallelism for the unsubscribe to land
// inside the store, and at GOMAXPROCS=1 it gives no signal (on the old code
// the commit won ~995 of 1000 races and 0 of 50 runs went red). At one CPU
// the regression is carried by the deterministic
// TestFinishReceipt_TransitCommitArmsRelayRetryWithTheBacklog, which pins
// that the arm happens inside the commit's own deliveryMu section.
//
// The recipient's last subscriber is removed concurrently with the store, at
// varying offsets. Whatever the interleaving, the settled state may hold the
// receipt with its retry entry, or neither — never a retry entry without the
// receipt behind it.
func TestStoreDeliveryReceipt_ConcurrentUnsubscribeLeavesNoOrphanRelayRetry(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	const iterations = 1000
	const recipient = "transit-subscriber-identity"
	orphans := 0
	for i := 0; i < iterations; i++ {
		svc := newInitMapsTestService(t, id)
		_ = attachPushObserver(t, svc, recipient, netcore.ConnID(10000+i))
		receipt := transitReceiptFor(fmt.Sprintf("race-%d", i), recipient)

		start := make(chan struct{})
		done := make(chan struct{}, 2)
		go func() {
			<-start
			svc.storeDeliveryReceipt(receipt)
			done <- struct{}{}
		}()
		go func(delay int) {
			<-start
			for j := 0; j < delay; j++ {
				runtime.Gosched()
			}
			svc.removeSubscriberByID(recipient, "observer")
			done <- struct{}{}
		}(i % 200)
		close(start)
		<-done
		<-done
		svc.WaitBackground()

		svc.deliveryMu.RLock()
		_, armed := svc.relayRetry[relayReceiptKey(receipt)]
		backlog := len(svc.receipts[recipient])
		svc.deliveryMu.RUnlock()
		if armed && backlog == 0 {
			orphans++
		}
	}
	if orphans > 0 {
		t.Fatalf("%d of %d interleavings left a relayRetry entry with no backlog receipt behind it", orphans, iterations)
	}
}
