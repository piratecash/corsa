package node

import (
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Queue mutations use the real owner, and observations use its typed readout.
// The fixed ring is a fixture capacity, not a deployment capacity selection.
func newH1K12CollisionService(t *testing.T) (*Service, *PendingObservation, *pendingConstructionClock) {
	t.Helper()
	cfg, id := pendingConstructionInputs(t)
	cfg.PendingRingSize = 8
	clock := &pendingConstructionClock{}
	s, o, err := newServiceWithPendingObservation(cfg, id, nil, pendingConstructionKey(), clock.read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { newUnstartedRuntimeCleanup(s, s.runCancel)(); s.WaitBackground() })
	return s, o, clock
}

func h1K12CollisionFrames(s *Service) (protocol.Frame, []protocol.Frame) {
	target := protocol.Frame{Type: "send_message", Topic: "dm", ID: "h1-k12-shared-id", Address: s.Address(), Recipient: idTargetX.String(), Body: "target"}
	foreignAuthor := target
	foreignAuthor.Address, foreignAuthor.Body = idPeerB.String(), "foreign-author"
	otherRecipient := target
	otherRecipient.Recipient, otherRecipient.Body = idPeerB.String(), "other-recipient"
	otherTopic := target
	otherTopic.Topic, otherTopic.Body = "h1-k12-public", "other-topic"
	return target, []protocol.Frame{foreignAuthor, otherRecipient, otherTopic}
}

func requireH1K12CollisionSnapshot(t *testing.T, o *PendingObservation, count int64, sequence uint64) domain.PendingQueueObservationSnapshot {
	t.Helper()
	v := o.Snapshot()
	if !v.Available || v.SchemaVersion != 1 || !v.CountValid || !v.CoverageValid || !v.DurationValid || v.UnknownReason != "" || v.AttributionBound || v.Count != count || v.Sequence != sequence {
		t.Fatalf("typed queue snapshot = %+v, want count=%d sequence=%d", v, count, sequence)
	}
	return v
}

// Compare original frame values rather than using the production identity
// matcher/key builder as the expected answer. Nothing seeds pending directly.
func requireH1K12CollisionFrames(t *testing.T, s *Service, address domain.PeerAddress, want []protocol.Frame) {
	t.Helper()
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	got := make([]protocol.Frame, 0, len(s.pending[address]))
	for _, item := range s.pending[address] {
		got = append(got, item.Frame)
	}
	if !reflect.DeepEqual(got, want) || len(s.pendingKeys) != len(want) || len(s.pending) != 1 {
		t.Fatalf("remaining queued frames=%+v keys=%d peers=%d; want exact frames=%+v", got, len(s.pendingKeys), len(s.pending), want)
	}
}

func requireH1K12CollisionDecrease(t *testing.T, before, after domain.PendingQueueObservationSnapshot, count int64) {
	t.Helper()
	boundary := domain.PendingQueueTimeBounds{Earliest: 2 * time.Hour, Latest: 2 * time.Hour}
	duration := domain.PendingQueueDurationBounds{Lower: 2 * time.Hour, Upper: 2 * time.Hour}
	if after.SourceID != before.SourceID || after.Baseline != before.Baseline || after.RunStart != boundary || after.RunStartCount != count || after.RunHasIncrease || !after.HasGrowingRun || after.LongestGrowingRun != duration {
		t.Fatalf("owned decrease did not reset the exact run: before=%+v after=%+v", before, after)
	}
}

func TestS6PendingCollidingMessageRemoval(t *testing.T) {
	for _, path := range []string{"single", "batch", "failed"} {
		t.Run(path, func(t *testing.T) {
			s, o, clock := newH1K12CollisionService(t)
			address := domain.PeerAddress("10.80.0.19:64646")
			target, controls := h1K12CollisionFrames(s)
			for _, frame := range append([]protocol.Frame{target}, controls...) {
				if !s.queuePeerFrame(address, frame) {
					t.Fatal("real queue admission refused collision fixture")
				}
			}
			before := requireH1K12CollisionSnapshot(t, o, 4, 4)
			if !before.RunHasIncrease || before.RunStartCount != 0 {
				t.Fatal("actual appends did not establish a growing run", before)
			}
			if path == "batch" {
				// Batch selects owed own messages from delivery bookkeeping.
				// This is not a queue event or an observation-state injection.
				s.deliveryMu.Lock()
				s.outbound[s.ownMessageIdentity(protocol.MessageID(target.ID), target.Recipient)] = outboundDelivery{MessageID: target.ID, Recipient: target.Recipient, Status: "failed"}
				s.deliveryMu.Unlock()
			}
			remove := func(wantRemoved int) {
				t.Helper()
				switch path {
				case "single":
					result, err := s.CancelOutgoingDelivery(protocol.MessageID(target.ID), idTargetX)
					if err != nil || result.PendingFrames != wantRemoved {
						t.Fatalf("single cancellation = %+v, %v; want removed=%d", result, err, wantRemoved)
					}
				case "batch":
					if _, err := s.CancelOutgoingDeliveriesTo(idTargetX, nil); err != nil {
						t.Fatal(err)
					}
				case "failed":
					s.failDelivery(protocol.Envelope{ID: protocol.MessageID(target.ID), Topic: "dm", Sender: target.Address, Recipient: target.Recipient}, "failed", "H1 K12 collision fixture")
				}
			}
			clock.set(2*time.Hour, nil)
			remove(1)
			after := requireH1K12CollisionSnapshot(t, o, 3, 5)
			requireH1K12CollisionDecrease(t, before, after, 3)
			requireH1K12CollisionFrames(t, s, address, controls)
			clock.set(3*time.Hour, nil)
			remove(0)
			again := requireH1K12CollisionSnapshot(t, o, 3, 6)
			requireH1K12CollisionDecrease(t, before, again, 3)
			requireH1K12CollisionFrames(t, s, address, controls)
			if before.Count != 4 || before.Sequence != 4 || after.Count != 3 || after.Sequence != 5 {
				t.Fatal("later read mutated prior value snapshots")
			}
		})
	}
}

func TestS6PendingCollidingReceiptCommit(t *testing.T) {
	for _, status := range []string{protocol.ReceiptStatusDelivered, protocol.ReceiptStatusSeen, protocol.ReceiptStatusSeenAck} {
		t.Run(status, func(t *testing.T) {
			s, o, clock := newH1K12CollisionService(t)
			address := domain.PeerAddress("10.80.0.20:64646")
			target, controls := h1K12CollisionFrames(s)
			// Exercise Item-shaped message identity too; controls keep flat fields.
			push := protocol.Frame{Type: "push_message", Topic: "dm", Item: &protocol.MessageFrame{ID: target.ID, Sender: target.Address, Recipient: target.Recipient, Body: target.Body}}
			receipt := protocol.DeliveryReceipt{MessageID: protocol.MessageID(target.ID), Sender: target.Recipient, Recipient: target.Address, Status: status, DeliveredAt: time.Now().UTC()}
			if status == protocol.ReceiptStatusSeenAck {
				// The author acknowledges the reader; this status travels back.
				receipt.Sender, receipt.Recipient = target.Address, target.Recipient
			}
			queuedReceipt := protocol.Frame{Type: "relay_delivery_receipt", ID: target.ID, Address: receipt.Sender, Recipient: receipt.Recipient, Status: status}
			foreignReceipt := queuedReceipt
			foreignReceipt.Address = idPeerB.String()
			controls = append(controls, foreignReceipt)
			frames := append([]protocol.Frame{push, queuedReceipt}, controls...)
			for _, frame := range frames {
				if !s.queuePeerFrame(address, frame) {
					t.Fatal("real queue admission refused receipt collision fixture")
				}
			}
			before := requireH1K12CollisionSnapshot(t, o, 6, 6)
			clock.set(2*time.Hour, nil)
			if _, _, _, result := s.finishReceipt(receiptKeyOf(receipt), receipt, false); result != committedFirst {
				t.Fatal("real receipt owner did not commit", result)
			}
			after := requireH1K12CollisionSnapshot(t, o, 4, 7)
			requireH1K12CollisionDecrease(t, before, after, 4)
			requireH1K12CollisionFrames(t, s, address, controls)
			clock.set(3*time.Hour, nil)
			if _, _, _, result := s.finishReceipt(receiptKeyOf(receipt), receipt, false); result != committedElsewhere {
				t.Fatal("exact duplicate receipt did not retain original commit", result)
			}
			again := requireH1K12CollisionSnapshot(t, o, 4, 8)
			requireH1K12CollisionDecrease(t, before, again, 4)
			requireH1K12CollisionFrames(t, s, address, controls)
			if before.Count != 6 || before.Sequence != 6 || after.Count != 4 || after.Sequence != 7 {
				t.Fatal("later receipt mutated prior value snapshots")
			}
		})
	}
}
