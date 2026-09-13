package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// receipt_clock_test.go pins whose clock the delivery badge speaks with.
//
// A delivery receipt is stamped by the node that took delivery, and that
// node's clock is not ours. A peer running a minute slow confirms a message
// we sent at 13:47 with "delivered at 13:46", and the badge drawn under our
// own bubble then reads as delivery happening before the send. What we can
// state truthfully is when WE learned of it, so that is what the client is
// given — while the remote claim is still carried, unaltered, for anyone who
// forwards it.

func laggingPeerReceipt(messageID, recipient string, lag time.Duration) protocol.DeliveryReceipt {
	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(messageID),
		Sender:      "some-peer",
		Recipient:   recipient,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC().Add(-lag),
	}
}

func TestTheBadgeShowsWhenWeLearnedOfDelivery(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	svc := &Service{}
	svc.initMaps()
	svc.identity = id
	svc.sentDMIDs.Add("m-1")

	const lag = time.Minute
	receipt := laggingPeerReceipt("m-1", id.Address, lag)
	claimed := receipt.DeliveredAt

	before := time.Now().UTC().Add(-time.Second)
	if !svc.storeDeliveryReceipt(receipt).stored {
		t.Fatal("receipt was not stored")
	}
	after := time.Now().UTC().Add(time.Second)

	frame := svc.fetchDeliveryReceiptsFrame(id.Address)
	if len(frame.Receipts) != 1 {
		t.Fatalf("backlog holds %d receipts, want 1", len(frame.Receipts))
	}
	got := frame.Receipts[0]

	observed, err := time.Parse(time.RFC3339, got.ObservedAt)
	if err != nil {
		t.Fatalf("observed_at %q is not a time: %v — the badge falls back to the "+
			"remote clock without it", got.ObservedAt, err)
	}
	if observed.Before(before) || observed.After(after) {
		t.Fatalf("observed_at = %s, want the moment this node admitted the receipt (%s..%s)",
			observed, before, after)
	}

	// The remote claim is not overwritten: it is someone else's statement and
	// this node forwards it.
	declared, err := time.Parse(time.RFC3339, got.DeliveredAt)
	if err != nil {
		t.Fatalf("delivered_at %q is not a time: %v", got.DeliveredAt, err)
	}
	if !declared.Equal(claimed.Truncate(time.Second)) {
		t.Fatalf("delivered_at = %s, want the peer's own %s", declared, claimed)
	}
}

func TestARelayedReceiptCarriesOnlyTheAuthorsClaim(t *testing.T) {
	receipt := laggingPeerReceipt("m-1", "someone", time.Minute)
	receipt.ObservedAt = time.Now().UTC()

	if frame := receiptFrame(receipt); frame.ObservedAt != "" {
		t.Fatalf("relayed frame carries observed_at %q: our note about when we saw "+
			"a receipt is not part of what its author said", frame.ObservedAt)
	}
	if frame := localReceiptFrame(receipt); frame.ObservedAt == "" {
		t.Fatal("the local reply dropped observed_at")
	}
}

func TestReceiptDisplayTimeFallsBackToTheClaim(t *testing.T) {
	// A receipt with no local stamp — reseeded after a restart, or built by
	// a fixture — must still date the badge rather than dating it to zero.
	receipt := laggingPeerReceipt("m-1", "someone", time.Minute)
	if got := receiptDisplayTime(receipt); !got.Equal(receipt.DeliveredAt) {
		t.Fatalf("display time = %s, want the claim %s", got, receipt.DeliveredAt)
	}

	receipt.ObservedAt = time.Now().UTC()
	if got := receiptDisplayTime(receipt); !got.Equal(receipt.ObservedAt) {
		t.Fatalf("display time = %s, want our own %s", got, receipt.ObservedAt)
	}
}
