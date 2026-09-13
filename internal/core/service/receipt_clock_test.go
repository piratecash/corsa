package service

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// receipt_clock_test.go pins the client half of the delivery-badge clock:
// the node offers two times for one receipt, and the one drawn under the
// user's own message has to be on the user's own clock. See
// protocol.DeliveryReceipt.ObservedAt.

func TestTheBadgePrefersTheTimeThisNodeObserved(t *testing.T) {
	claimed := time.Date(2026, 1, 1, 13, 46, 11, 0, time.UTC) // their slow clock
	observed := time.Date(2026, 1, 1, 13, 47, 3, 0, time.UTC) // ours

	got := receiptRecordsFromFrames([]protocol.ReceiptFrame{{
		MessageID:   "m-1",
		Sender:      "peer",
		Recipient:   "self",
		Status:      "delivered",
		DeliveredAt: claimed.Format(time.RFC3339),
		ObservedAt:  observed.Format(time.RFC3339),
	}})

	if len(got) != 1 {
		t.Fatalf("decoded %d receipts, want 1", len(got))
	}
	if !got[0].DeliveredAt.Equal(observed) {
		t.Fatalf("badge time = %s, want %s: a message sent at 13:47 cannot be "+
			"confirmed at 13:46", got[0].DeliveredAt, observed)
	}
}

func TestTheBadgeKeepsTheRemoteClaimWhenTheNodeOffersNothingElse(t *testing.T) {
	// An older node, or any path that does not stamp: the remote claim is
	// all there is, and dropping the receipt over it would lose the badge.
	claimed := time.Date(2026, 1, 1, 13, 46, 11, 0, time.UTC)

	got := receiptRecordsFromFrames([]protocol.ReceiptFrame{{
		MessageID:   "m-1",
		Sender:      "peer",
		Recipient:   "self",
		Status:      "delivered",
		DeliveredAt: claimed.Format(time.RFC3339),
	}})

	if len(got) != 1 {
		t.Fatalf("decoded %d receipts, want 1", len(got))
	}
	if !got[0].DeliveredAt.Equal(claimed) {
		t.Fatalf("badge time = %s, want the claim %s", got[0].DeliveredAt, claimed)
	}
}

func TestAnUnparseableObservedTimeDoesNotDiscardTheReceipt(t *testing.T) {
	claimed := time.Date(2026, 1, 1, 13, 46, 11, 0, time.UTC)

	got := receiptRecordsFromFrames([]protocol.ReceiptFrame{{
		MessageID:   "m-1",
		Sender:      "peer",
		Recipient:   "self",
		Status:      "delivered",
		DeliveredAt: claimed.Format(time.RFC3339),
		ObservedAt:  "not a time",
	}})

	if len(got) != 1 {
		t.Fatalf("decoded %d receipts, want 1: a junk local field must not cost "+
			"the badge the receipt carries", len(got))
	}
	if !got[0].DeliveredAt.Equal(claimed) {
		t.Fatalf("badge time = %s, want the claim %s", got[0].DeliveredAt, claimed)
	}
}
