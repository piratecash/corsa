package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// message_identity_collision_test.go pins that the delivery state of one
// direct message is settled only by something about THAT message. The outer
// id is chosen by the sender and is not covered by the signature, so two
// messages from different authors can carry the same id: a receipt, a
// withdrawal or a give-up for one of them must leave the other's relay retry
// entry and queued frames alone.

// queueTestPendingFrame puts frame into the pending ring of address the way
// queuePeerFrame does, without the peer-domain lookup.
func queueTestPendingFrame(svc *Service, address domain.PeerAddress, frame protocol.Frame) {
	svc.deliveryMu.Lock()
	defer svc.deliveryMu.Unlock()
	svc.pending[address] = append(svc.pending[address], pendingFrame{Frame: frame, QueuedAt: time.Now().UTC()})
	svc.pendingKeys[pendingFrameKey(address, frame)] = struct{}{}
}

// pendingFrameQueued reports whether the pending ring of address still holds
// a frame carrying exactly this message — id, author and addressee. It
// compares the fields itself rather than through frameCarriesMessage, so the
// oracle stays independent of the matcher the code under test uses.
func pendingFrameQueued(svc *Service, address domain.PeerAddress, id protocol.MessageID, sender, recipient string) bool {
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	for _, item := range svc.pending[address] {
		envelope := frameEnvelope(item.Frame)
		if envelope.ID == id && envelope.Sender == sender && envelope.Recipient == recipient {
			return true
		}
	}
	return false
}

// transitRelayFrame is the relay_message this node queues for a transit DM
// when the next hop has no session (sendRelayMessage).
func transitRelayFrame(f transitDMFixture, id protocol.MessageID) protocol.Frame {
	return protocol.Frame{
		Type:      "relay_message",
		ID:        string(id),
		Address:   f.sender.Address,
		Recipient: f.recipient.Address,
		Topic:     "dm",
	}
}

const transitFramePeer = domain.PeerAddress("10.20.0.1:64646")

// newArmedTransitWithQueuedFrame admits a transit DM sender→recipient under id
// and queues its relay frame, so a test can watch both halves of its
// delivery state.
func newArmedTransitWithQueuedFrame(t *testing.T, id protocol.MessageID) transitDMFixture {
	t.Helper()
	f := newTransitDMFixture(t)
	f.store(t, f.message(t, id))
	if !f.svc.hasRelayRetryEntry(id) {
		t.Fatal("fixture: the transit DM must be armed, or the assertions below prove nothing")
	}
	queueTestPendingFrame(f.svc, transitFramePeer, transitRelayFrame(f, id))
	return f
}

// assertTransitDeliveryStateIntact checks that the transit DM still owns its
// relay retry entry and its queued frame, including after the relay retry
// walk, which drops an entry whose message it believes confirmed.
func assertTransitDeliveryStateIntact(t *testing.T, f transitDMFixture, id protocol.MessageID) {
	t.Helper()
	if !backlogHasMessage(f.svc, id) {
		t.Fatal("fixture: the transit DM must still be backlogged")
	}
	if !f.svc.hasRelayRetryEntry(id) {
		t.Fatal("the msg| entry of a live transit DM was deleted by state about another message that shares its id")
	}
	if !pendingFrameQueued(f.svc, transitFramePeer, id, f.sender.Address, f.recipient.Address) {
		t.Fatal("the queued relay frame of a live transit DM was dropped by state about another message that shares its id")
	}
	_ = f.svc.retryableRelayMessages(time.Now().UTC())
	if !f.svc.hasRelayRetryEntry(id) {
		t.Fatal("the relay retry walk took a receipt for another message as confirmation of the transit DM and dropped its msg| entry")
	}
}

// A receipt confirms the message whose recipient made it and whose sender it
// is addressed to — and no other message under the same id. Each row stores a
// receipt that names the transit DM's id but not the transit DM itself.
func TestReceiptForAnotherMessageLeavesTransitDeliveryStateAlone(t *testing.T) {
	t.Parallel()
	const shared = protocol.MessageID("receipt-collision-id-1")
	for _, tc := range []struct {
		name string
		// receipt sets up and returns the receipt to store, and a check of
		// what that receipt DOES settle, run after the commit (nil when it
		// confirms nothing this node holds).
		receipt func(t *testing.T, f transitDMFixture) (protocol.DeliveryReceipt, func(t *testing.T))
	}{
		{"receipt for this node's own message", func(t *testing.T, f transitDMFixture) (protocol.DeliveryReceipt, func(t *testing.T)) {
			ownRecipient := mustGenerateIdentity(t)
			ownPeer := domain.PeerAddress("10.20.0.2:64646")
			f.svc.deliveryMu.Lock()
			f.svc.sentDMIDs.Add(string(shared))
			f.svc.deliveryMu.Unlock()
			queueTestPendingFrame(f.svc, ownPeer, protocol.Frame{
				Type: "relay_message", ID: string(shared), Address: f.svc.Address(), Recipient: ownRecipient.Address, Topic: "dm",
			})
			receipt := protocol.DeliveryReceipt{
				MessageID: shared, Sender: ownRecipient.Address, Recipient: f.svc.Address(),
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}
			return receipt, func(t *testing.T) {
				if pendingFrameQueued(f.svc, ownPeer, shared, f.svc.Address(), ownRecipient.Address) {
					t.Error("the receipt for this node's own message left that message's queued frame behind")
				}
			}
		}},
		{"transit receipt from someone other than the transit recipient", func(t *testing.T, f transitDMFixture) (protocol.DeliveryReceipt, func(t *testing.T)) {
			_ = attachPushObserver(t, f.svc, f.sender.Address, netcore.ConnID(9611))
			return protocol.DeliveryReceipt{
				MessageID: shared, Sender: mustGenerateIdentity(t).Address, Recipient: f.sender.Address,
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}, nil
		}},
		{"transit receipt addressed to someone other than the transit sender", func(t *testing.T, f transitDMFixture) (protocol.DeliveryReceipt, func(t *testing.T)) {
			other := mustGenerateIdentity(t)
			_ = attachPushObserver(t, f.svc, other.Address, netcore.ConnID(9612))
			return protocol.DeliveryReceipt{
				MessageID: shared, Sender: f.recipient.Address, Recipient: other.Address,
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newArmedTransitWithQueuedFrame(t, shared)
			receipt, settled := tc.receipt(t, f)
			outcome := f.svc.storeDeliveryReceipt(receipt)
			f.svc.WaitBackground()
			if !outcome.stored {
				t.Fatalf("fixture: the receipt must be committed, got %+v", outcome)
			}
			if settled != nil {
				settled(t)
			}
			assertTransitDeliveryStateIntact(t, f, shared)
		})
	}
}

// Positive control: the transit DM's own receipt — made by its recipient,
// addressed to its sender — ends its relay. The envelope stays backlogged for
// the recipient's subscribers; what ends is the onward re-sending.
func TestReceiptForTheTransitMessageEndsItsRelay(t *testing.T) {
	t.Parallel()
	const id = protocol.MessageID("receipt-collision-id-2")
	f := newArmedTransitWithQueuedFrame(t, id)
	_ = attachPushObserver(t, f.svc, f.sender.Address, netcore.ConnID(9613))

	outcome := f.svc.storeDeliveryReceipt(protocol.DeliveryReceipt{
		MessageID: id, Sender: f.recipient.Address, Recipient: f.sender.Address,
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
	})
	f.svc.WaitBackground()
	if !outcome.stored {
		t.Fatalf("fixture: the receipt must be committed, got %+v", outcome)
	}

	if f.svc.hasRelayRetryEntry(id) {
		t.Error("the transit DM's own receipt left its msg| entry armed")
	}
	if pendingFrameQueued(f.svc, transitFramePeer, id, f.sender.Address, f.recipient.Address) {
		t.Error("the transit DM's own receipt left its queued relay frame behind")
	}
	for _, msg := range f.svc.retryableRelayMessages(time.Now().UTC().Add(time.Hour)) {
		if msg.ID == id {
			t.Error("the relay retry walk still re-sends a transit DM whose receipt is backlogged")
		}
	}
}

// The author's withdrawal and the sender engine's give-up settle an own
// message: a peer's transit DM under the same id keeps its queued frame.
func TestOwnDeliveryWithdrawalLeavesTransitPendingFrameAlone(t *testing.T) {
	t.Parallel()
	const shared = protocol.MessageID("withdraw-collision-id-1")
	ownRecipient := mustGenerateIdentity(t)
	for _, tc := range []struct {
		name     string
		withdraw func(t *testing.T, svc *Service)
	}{
		{"CancelOutgoingDelivery", func(t *testing.T, svc *Service) {
			if _, err := svc.CancelOutgoingDelivery(shared, domain.PeerIdentityFromWire(ownRecipient.Address)); err != nil {
				t.Fatalf("CancelOutgoingDelivery: %v", err)
			}
		}},
		{"CancelOutgoingDeliveriesTo", func(t *testing.T, svc *Service) {
			svc.deliveryMu.Lock()
			svc.outbound[string(shared)] = outboundDelivery{MessageID: string(shared), Recipient: ownRecipient.Address, Status: "failed"}
			svc.deliveryMu.Unlock()
			if _, err := svc.CancelOutgoingDeliveriesTo(domain.PeerIdentityFromWire(ownRecipient.Address), nil); err != nil {
				t.Fatalf("CancelOutgoingDeliveriesTo: %v", err)
			}
		}},
		{"failDelivery", func(t *testing.T, svc *Service) {
			svc.failDelivery(protocol.Envelope{ID: shared, Topic: "dm", Sender: svc.identity.Address, Recipient: ownRecipient.Address}, "expired", "ttl")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newArmedTransitWithQueuedFrame(t, shared)
			ownPeer := domain.PeerAddress("10.20.0.3:64646")
			queueTestPendingFrame(f.svc, ownPeer, protocol.Frame{
				Type: "relay_message", ID: string(shared), Address: f.svc.Address(), Recipient: ownRecipient.Address, Topic: "dm",
			})

			tc.withdraw(t, f.svc)
			f.svc.WaitBackground()

			if pendingFrameQueued(f.svc, ownPeer, shared, f.svc.Address(), ownRecipient.Address) {
				t.Error("the withdrawn own message's queued frame survived")
			}
			assertTransitDeliveryStateIntact(t, f, shared)
		})
	}
}

func mustGenerateIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

// A delivered or seen receipt is made by the message's recipient and
// addressed to its author, so the roles swap; a seen_ack is made by the author
// and addressed to the recipient, so they stay; any other status confirms no
// message.
func TestConfirmedMessageOfReadsTheRolesByStatus(t *testing.T) {
	t.Parallel()
	const id = protocol.MessageID("confirmed-message-of-1")
	authorToReader := messageIdentity{MessageID: id, Sender: "author", Recipient: "reader"}
	// Each receipt is built the way its status travels: delivered and seen
	// from the reader to the author, seen_ack from the author to the reader.
	// All three confirm the same author→reader message.
	for _, tc := range []struct {
		status   string
		from, to string
		confirms bool
		want     messageIdentity
	}{
		{protocol.ReceiptStatusDelivered, "reader", "author", true, authorToReader},
		{protocol.ReceiptStatusSeen, "reader", "author", true, authorToReader},
		{protocol.ReceiptStatusSeenAck, "author", "reader", true, authorToReader},
		{"unknown", "reader", "author", false, messageIdentity{}},
	} {
		got, ok := confirmedMessageOf(protocol.DeliveryReceipt{MessageID: id, Sender: tc.from, Recipient: tc.to, Status: tc.status})
		if ok != tc.confirms || got != tc.want {
			t.Errorf("status %q: confirmed %+v (ok=%v), want %+v (ok=%v)", tc.status, got, ok, tc.want, tc.confirms)
		}
	}
}

// storeTestReceipt stores receipt through the door and fails the test unless
// it was committed.
func storeTestReceipt(t *testing.T, svc *Service, receipt protocol.DeliveryReceipt) {
	t.Helper()
	outcome := svc.storeDeliveryReceipt(receipt)
	svc.WaitBackground()
	if !outcome.stored {
		t.Fatalf("fixture: the receipt must be committed, got %+v", outcome)
	}
}

// s.outbound tracks this node's own messages by their bare id. A receipt
// clears an entry only when it confirms that own message — the author is this
// node and the confirming party is the entry's recipient. Anything else under
// the same id, a transit receipt in particular, leaves the entry: its status
// is what fetch_pending_messages shows and what CancelOutgoingDeliveriesTo
// learns the id from.
func TestReceiptClearsOutboundOnlyForTheOwnMessageItConfirms(t *testing.T) {
	t.Parallel()
	const shared = protocol.MessageID("outbound-collision-id-1")
	for _, tc := range []struct {
		name    string
		receipt func(t *testing.T, f transitDMFixture, ownRecipient *identity.Identity) protocol.DeliveryReceipt
		cleared bool
	}{
		{"transit receipt for a peer's message", func(t *testing.T, f transitDMFixture, _ *identity.Identity) protocol.DeliveryReceipt {
			_ = attachPushObserver(t, f.svc, f.sender.Address, netcore.ConnID(9621))
			return protocol.DeliveryReceipt{
				MessageID: shared, Sender: f.recipient.Address, Recipient: f.sender.Address,
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}
		}, false},
		{"receipt to this node from someone other than the entry's recipient", func(t *testing.T, f transitDMFixture, _ *identity.Identity) protocol.DeliveryReceipt {
			return protocol.DeliveryReceipt{
				MessageID: shared, Sender: mustGenerateIdentity(t).Address, Recipient: f.svc.Address(),
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}
		}, false},
		{"transit receipt made by the own message's recipient for someone else", func(t *testing.T, f transitDMFixture, ownRecipient *identity.Identity) protocol.DeliveryReceipt {
			// Same id and the same confirming party as the own message, but
			// addressed to a third identity: the confirmed message is that
			// identity's, not ours, so the author is what tells them apart.
			third := mustGenerateIdentity(t)
			_ = attachPushObserver(t, f.svc, third.Address, netcore.ConnID(9622))
			return protocol.DeliveryReceipt{
				MessageID: shared, Sender: ownRecipient.Address, Recipient: third.Address,
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}
		}, false},
		{"receipt from the own message's recipient", func(t *testing.T, f transitDMFixture, ownRecipient *identity.Identity) protocol.DeliveryReceipt {
			return protocol.DeliveryReceipt{
				MessageID: shared, Sender: ownRecipient.Address, Recipient: f.svc.Address(),
				Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTransitDMFixture(t)
			ownRecipient := mustGenerateIdentity(t)
			// The own message gave up earlier: failDelivery left a terminal
			// status entry, and the id is still one this node sent.
			f.svc.deliveryMu.Lock()
			f.svc.sentDMIDs.Add(string(shared))
			f.svc.outbound[string(shared)] = outboundDelivery{MessageID: string(shared), Recipient: ownRecipient.Address, Status: "expired"}
			f.svc.deliveryMu.Unlock()

			storeTestReceipt(t, f.svc, tc.receipt(t, f, ownRecipient))

			f.svc.deliveryMu.RLock()
			_, kept := f.svc.outbound[string(shared)]
			f.svc.deliveryMu.RUnlock()
			if kept == tc.cleared {
				t.Fatalf("outbound entry kept = %v, want %v", kept, !tc.cleared)
			}
		})
	}
}

// A message frame queued for a peer is one message: the dedup key names its
// author like frameCarriesMessage does. Keyed by id and recipient alone, a
// peer's transit DM under the same id to the same recipient took the own
// message's key, and the duplicate branch REPLACED the queued own frame with
// the transit one.
func TestQueuedFrameOfAnotherAuthorDoesNotReplaceOwnFrame(t *testing.T) {
	t.Parallel()
	const shared = protocol.MessageID("pending-key-collision-id-1")
	f := newTransitDMFixture(t)
	const peer = domain.PeerAddress("10.20.0.4:64646")
	own := protocol.Frame{Type: "relay_message", ID: string(shared), Address: f.svc.Address(), Recipient: f.recipient.Address, Topic: "dm"}
	if !f.svc.queuePeerFrame(peer, own) {
		t.Fatal("fixture: the own frame must be queued")
	}
	if !f.svc.queuePeerFrame(peer, transitRelayFrame(f, shared)) {
		t.Fatal("fixture: the transit frame must be queued")
	}

	if !pendingFrameQueued(f.svc, peer, shared, f.svc.Address(), f.recipient.Address) {
		t.Error("a peer's frame under the same id and recipient replaced this node's queued own frame")
	}
	if !pendingFrameQueued(f.svc, peer, shared, f.sender.Address, f.recipient.Address) {
		t.Error("the transit frame was not queued next to the own frame")
	}
}

// A seen_ack is the message's author acknowledging its recipient's seen
// receipt: it travels author → recipient (sendSeenAck), the opposite way to
// delivered/seen, and it is evidence the recipient holds the message. It
// therefore ends the relay of exactly that message, and of no other under the
// same id.
func TestSeenAckEndsTheRelayOfTheMessageItAcknowledges(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		ack   func(f transitDMFixture, id protocol.MessageID) protocol.DeliveryReceipt
		ended bool
	}{
		{"seen_ack from the transit sender to the transit recipient", func(f transitDMFixture, id protocol.MessageID) protocol.DeliveryReceipt {
			return protocol.DeliveryReceipt{MessageID: id, Sender: f.sender.Address, Recipient: f.recipient.Address, Status: protocol.ReceiptStatusSeenAck, DeliveredAt: time.Now().UTC()}
		}, true},
		{"seen_ack the other way round", func(f transitDMFixture, id protocol.MessageID) protocol.DeliveryReceipt {
			return protocol.DeliveryReceipt{MessageID: id, Sender: f.recipient.Address, Recipient: f.sender.Address, Status: protocol.ReceiptStatusSeenAck, DeliveredAt: time.Now().UTC()}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = protocol.MessageID("seen-ack-collision-id-1")
			f := newArmedTransitWithQueuedFrame(t, id)
			storeTestReceipt(t, f.svc, tc.ack(f, id))
			assertRelayEnded(t, f, id, tc.ended)
		})
	}
}

// The roles swap between a delivered/seen receipt and the message it
// confirms: the receipt's author is the message's recipient. A seen receipt
// the other way round is about the reverse message and leaves this one's
// relay running.
func TestSeenReceiptEndsTheRelayOnlyWithTheRolesSwapped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		seen  func(t *testing.T, f transitDMFixture, id protocol.MessageID) protocol.DeliveryReceipt
		ended bool
	}{
		{"seen from the transit recipient to the transit sender", func(t *testing.T, f transitDMFixture, id protocol.MessageID) protocol.DeliveryReceipt {
			_ = attachPushObserver(t, f.svc, f.sender.Address, netcore.ConnID(9631))
			return protocol.DeliveryReceipt{MessageID: id, Sender: f.recipient.Address, Recipient: f.sender.Address, Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Now().UTC()}
		}, true},
		{"seen from the transit sender to the transit recipient", func(t *testing.T, f transitDMFixture, id protocol.MessageID) protocol.DeliveryReceipt {
			_ = attachPushObserver(t, f.svc, f.recipient.Address, netcore.ConnID(9632))
			return protocol.DeliveryReceipt{MessageID: id, Sender: f.sender.Address, Recipient: f.recipient.Address, Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Now().UTC()}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const id = protocol.MessageID("seen-role-swap-id-1")
			f := newArmedTransitWithQueuedFrame(t, id)
			storeTestReceipt(t, f.svc, tc.seen(t, f, id))
			assertRelayEnded(t, f, id, tc.ended)
		})
	}
}

// assertRelayEnded checks both halves of a transit DM's relay state against
// the expectation: ended means its msg| entry and its queued frame are gone,
// otherwise both are still there.
func assertRelayEnded(t *testing.T, f transitDMFixture, id protocol.MessageID, ended bool) {
	t.Helper()
	if armed := f.svc.hasRelayRetryEntry(id); armed == ended {
		t.Errorf("msg| entry armed = %v, want %v", armed, !ended)
	}
	if queued := pendingFrameQueued(f.svc, transitFramePeer, id, f.sender.Address, f.recipient.Address); queued == ended {
		t.Errorf("transit relay frame queued = %v, want %v", queued, !ended)
	}
}

// fetch_inbox hides a message only behind a receipt that confirms it. A
// receipt under the same id made by someone other than the message's
// recipient is about another message. It is seeded directly: the door binds
// a receipt's author only while a retry entry is live, so one like this is
// admitted for an id with no entry left (delivered earlier, or reseeded
// after a restart).
func TestFetchInboxHidesOnlyAConfirmedMessage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		author func(recipient *identity.Identity) string
		hidden bool
	}{
		{"receipt from someone other than the recipient", func(*identity.Identity) string { return "" }, false},
		{"receipt from the recipient", func(recipient *identity.Identity) string { return recipient.Address }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			recipient := mustGenerateIdentity(t)
			const id = protocol.MessageID("inbox-collision-id-1")
			storeOwnOutgoingDM(t, svc, recipient, id)
			author := tc.author(recipient)
			if author == "" {
				author = mustGenerateIdentity(t).Address
			}
			svc.deliveryMu.Lock()
			svc.receipts[svc.Address()] = append(svc.receipts[svc.Address()], protocol.DeliveryReceipt{
				MessageID: id, Sender: author, Recipient: svc.Address(), Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			})
			svc.deliveryMu.Unlock()

			inbox := svc.fetchInboxFrame("dm", recipient.Address)
			listed := false
			for _, item := range inbox.Messages {
				listed = listed || protocol.MessageID(item.ID) == id
			}
			if listed == tc.hidden {
				t.Fatalf("message listed in fetch_inbox = %v, want %v", listed, !tc.hidden)
			}
		})
	}
}

// failDelivery skips the abandonment only when a receipt CONFIRMING the own
// message arrived in the meantime. A receipt under the same id from someone
// else is no such confirmation, and the message must still go terminal.
func TestFailDeliveryLateReceiptRecheckNeedsAConfirmingReceipt(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		fromOwnPeer  bool
		wantTerminal bool
	}{
		{"receipt from someone other than the recipient", false, true},
		{"receipt from the recipient", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			recipient := mustGenerateIdentity(t)
			const id = protocol.MessageID("fail-recheck-collision-id-1")
			author := mustGenerateIdentity(t).Address
			if tc.fromOwnPeer {
				author = recipient.Address
			}
			svc.deliveryMu.Lock()
			svc.receipts[svc.Address()] = append(svc.receipts[svc.Address()], protocol.DeliveryReceipt{
				MessageID: id, Sender: author, Recipient: svc.Address(), Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
			})
			svc.deliveryMu.Unlock()

			svc.failDelivery(protocol.Envelope{ID: id, Topic: "dm", Sender: svc.Address(), Recipient: recipient.Address}, "expired", "ttl")

			svc.deliveryMu.RLock()
			status := svc.outbound[string(id)].Status
			svc.deliveryMu.RUnlock()
			if terminal := status == "expired"; terminal != tc.wantTerminal {
				t.Fatalf("own message went terminal = %v (status %q), want %v", terminal, status, tc.wantTerminal)
			}
		})
	}
}

// A `msg|` entry with no envelope behind it breaks the invariant that ties
// the entry to its backlogged envelope, and it is owned by no live message:
// nothing walks s.relayRetry itself, so it would hold a maxRelayRetryEntries
// slot until exit and keep the ack_delete gate treating the id as relayed.
// Its ack_delete suppression set is forgotten only by envelope-removal paths,
// which never run for an orphan, so it would outlive it and hand its stale
// suppression to the next transit DM under the id. The branch is unreachable
// while the invariant holds, so the breach is built directly; a receipt
// confirming a message under that id reaps both.
func TestReceiptReapsAnOrphanRelayRetryEntry(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ownRecipient := mustGenerateIdentity(t)
	const id = protocol.MessageID("orphan-relay-retry-id-1")
	svc.deliveryMu.Lock()
	svc.sentDMIDs.Add(string(id))
	svc.relayRetry[relayMessageKey(id)] = relayAttempt{FirstSeen: time.Now().UTC()}
	svc.deliveryMu.Unlock()
	svc.recordRelayDeliveredTo(id, domain.PeerIdentityFromWire(mustGenerateIdentity(t).Address))
	if backlogHasMessage(svc, id) {
		t.Fatal("fixture: no envelope may be backlogged under the id, or this is no orphan")
	}
	if !relayDeliveredTracked(svc, id) {
		t.Fatal("fixture: the orphan must carry a suppression set, or the assertion below proves nothing")
	}

	storeTestReceipt(t, svc, protocol.DeliveryReceipt{
		MessageID: id, Sender: ownRecipient.Address, Recipient: svc.Address(),
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: time.Now().UTC(),
	})

	if svc.hasRelayRetryEntry(id) {
		t.Fatal("a msg| entry with no backlogged envelope survived a receipt confirming a message under its id")
	}
	if relayDeliveredTracked(svc, id) {
		t.Fatal("the orphan's ack_delete suppression set survived the reap of its msg| entry")
	}
}
