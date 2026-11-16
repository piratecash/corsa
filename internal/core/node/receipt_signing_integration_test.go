package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestNegotiatedSignedReceiptLifecycle(t *testing.T) {
	t.Parallel()

	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	for _, peers := range [][2]*Service{{dialer, listener}, {listener, dialer}} {
		from, to := peers[0], peers[1]
		waitForConditionMsg(t, 10*time.Second, "no bilateral negotiation for signed receipts", func() bool {
			return from.SupportsSignedReceipts(domain.PeerIdentityFromWire(to.Address())) &&
				to.SupportsSignedReceipts(domain.PeerIdentityFromWire(from.Address()))
		})
		ref := sendDMRequiringSignedReceipts(t, from, to)
		waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusDelivered)

		storeSeenReceipt(t, from, to, ref)
		waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusSeen)

		waitForSeenAck(t, from, to, ref, true)
	}
}

func TestNegotiatedReceiptRequirementSurvivesLossOfProvedSession(t *testing.T) {
	t.Parallel()

	from, address, stopFrom := listenerNode(t, nil)
	defer stopFrom()
	to, stopTo := dialerNode(t, address, nil)
	t.Cleanup(func() {
		if stopTo != nil {
			stopTo()
		}
	})
	peer := domain.PeerIdentityFromWire(to.Address())
	waitForConditionMsg(t, 10*time.Second, "no bilateral negotiation for signed receipts", func() bool {
		return from.SupportsSignedReceipts(peer) && to.SupportsSignedReceipts(domain.PeerIdentityFromWire(from.Address()))
	})
	ref := sendDMRequiringSignedReceipts(t, from, to)
	waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusDelivered)
	stopTo()
	stopTo = nil
	waitForConditionMsg(t, 10*time.Second, "proved receipt-capable session was not removed", func() bool { return !from.SupportsSignedReceipts(peer) })

	// At this point the original delivery retry has already ended. The
	// message policy must still protect a later read receipt.
	seen := protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(ref.ID), Sender: to.Address(), Recipient: from.Address(),
		Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Now().UTC().Truncate(time.Second),
	}
	if got := from.storeDeliveryReceipt(seen); got.stored || got.ackable {
		t.Fatalf("unsigned seen accepted after loss of negotiated session: %+v", got)
	}
	key := receiptKeyOf(seen)
	from.deliveryMu.RLock()
	unsignedStored := from.seenReceipts.Has(key)
	from.deliveryMu.RUnlock()
	if unsignedStored {
		t.Fatal("unsigned seen altered receipt deduplication")
	}
}

func storeSeenReceipt(t *testing.T, from, to *Service, ref domain.MessageRef) {
	t.Helper()
	seen := to.HandleLocalFrame(protocol.Frame{
		Type:        "send_delivery_receipt",
		ID:          ref.ID,
		Address:     to.Address(),
		Recipient:   from.Address(),
		Status:      protocol.ReceiptStatusSeen,
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	})
	if seen.Type != "receipt_stored" {
		t.Fatalf("send seen: %#v", seen)
	}
}

func waitForSeenAck(t *testing.T, from, to *Service, ref domain.MessageRef, signed bool) {
	t.Helper()
	// The wire-only acknowledgment is indexed by signature presence after
	// validation, rather than exposed in the delivery receipt backlog.
	ack := protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(ref.ID), Sender: from.Address(), Recipient: to.Address(),
		Status: protocol.ReceiptStatusSeenAck,
	}
	unsignedKey := receiptKeyOf(ack)
	ack.Signature = "signed-key"
	signedKey := receiptKeyOf(ack)
	wantedKey, otherKey := unsignedKey, signedKey
	if signed {
		wantedKey, otherKey = signedKey, unsignedKey
	}
	waitForConditionMsg(t, 10*time.Second, "seen_ack did not end the retry", func() bool {
		to.deliveryMu.RLock()
		defer to.deliveryMu.RUnlock()
		if to.seenReceipts.Has(otherKey) {
			t.Fatalf("seen_ack used an unexpected signing policy, want signed=%v", signed)
		}
		return to.seenReceipts.Has(wantedKey) && to.awaitingSeenAck[madeByMessageAuthor(ack)] == nil
	})
}

func sendDMRequiringSignedReceipts(t *testing.T, from, to *Service) domain.MessageRef {
	t.Helper()
	_, binding := to.SelfIdentityRecord()
	contacts := from.HandleLocalFrame(protocol.Frame{Type: "import_contacts", Contacts: []protocol.ContactFrame{{
		Address: to.Address(), PubKey: string(binding.PubKey), BoxKey: string(binding.BoxKey), BoxSig: string(binding.BoxSig),
	}}})
	if contacts.Type != "contacts_imported" {
		t.Fatalf("import recipient contact: %s", contacts.Type)
	}
	id, err := protocol.NewMessageID()
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: string(id), Sender: from.Address(), Recipient: to.Address()}
	body, err := directmsg.EncryptForMessageWithSignedReceipts(from.identity, domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(to.Address()),
		BoxKeyBase64: identity.BoxPublicKeyBase64(to.identity.BoxPublicKey),
	}, domain.OutgoingDM{Body: "check required receipt lifecycle"}, ref.Topic, ref.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	stored := from.HandleLocalFrame(sendMessageFrame(ref.Topic, ref.ID, ref.Sender, ref.Recipient,
		"sender-delete", time.Now().UTC().Format(time.RFC3339), 0, body))
	if stored.Type != "message_stored" {
		t.Fatalf("send protected DM: %#v", stored)
	}
	return ref
}

func waitForSignedReceipt(t *testing.T, from, to *Service, ref domain.MessageRef, status string) {
	t.Helper()
	waitForConditionMsg(t, 10*time.Second, "missing signed "+status+" receipt", func() bool {
		for _, frame := range from.fetchDeliveryReceiptsFrame(from.Address()).Receipts {
			if frame.MessageID != ref.ID || frame.Status != status {
				continue
			}
			if frame.Signature == "" {
				t.Fatalf("unsigned %s receipt stored for protected message", status)
			}
			receipt, err := receiptFromReceiptFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			key := identity.PublicKeyBase64(to.identity.PublicKey)
			if err := protocol.VerifyDeliveryReceipt(domain.NetworkID(networkName), ref, receipt, key, frame.Signature); err != nil {
				t.Fatalf("%s receipt signature: %v", status, err)
			}
			return true
		}
		return false
	})
}

func TestDeliveredReceiptSigningUsesOnlyNegotiatedProvedSessions(t *testing.T) {
	t.Parallel()

	t.Run("signed in both session directions", func(t *testing.T) {
		listener, address, stopListener := listenerNode(t, nil)
		defer stopListener()
		dialer, stopDialer := dialerNode(t, address, nil)
		defer stopDialer()
		waitKinds(t, dialer, "no outbound v2 session", func(stats domain.SessionKindStats) bool { return stats.OutboundV2 > 0 })
		waitKinds(t, listener, "no inbound v2 session", func(stats domain.SessionKindStats) bool { return stats.InboundV2 > 0 })

		for _, peers := range [][2]*Service{{dialer, listener}, {listener, dialer}} {
			sender, recipient := peers[0], peers[1]
			id, err := protocol.NewMessageID()
			if err != nil {
				t.Fatal(err)
			}
			deliverDM(t, sender, recipient, string(id), "check delivery receipt")
			waitForConditionMsg(t, 10*time.Second, "missing signed delivered receipt", func() bool {
				for _, frame := range sender.fetchDeliveryReceiptsFrame(sender.Address()).Receipts {
					if frame.MessageID != string(id) || frame.Status != protocol.ReceiptStatusDelivered || frame.Signature == "" {
						continue
					}
					receipt, err := receiptFromReceiptFrame(frame)
					if err != nil {
						t.Fatal(err)
					}
					ref := domain.MessageRef{Topic: "dm", ID: string(id), Sender: sender.Address(), Recipient: recipient.Address()}
					key := identity.PublicKeyBase64(recipient.identity.PublicKey)
					if err := protocol.VerifyDeliveryReceipt(domain.NetworkID(networkName), ref, receipt, key, frame.Signature); err != nil {
						t.Fatalf("delivered receipt signature: %v", err)
					}
					return true
				}
				return false
			})
		}
	})

	t.Run("unnegotiated message keeps an unsigned receipt over TLS relay", func(t *testing.T) {
		sender, _, recipient := startTLSRelayPair(t)
		id, err := protocol.NewMessageID()
		if err != nil {
			t.Fatal(err)
		}
		deliverDM(t, sender, recipient, string(id), "check ordinary receipt through a TLS relay")
		waitForConditionMsg(t, 10*time.Second, "missing unsigned delivered receipt", func() bool {
			for _, frame := range sender.fetchDeliveryReceiptsFrame(sender.Address()).Receipts {
				if frame.MessageID == string(id) && frame.Status == protocol.ReceiptStatusDelivered {
					if frame.Signature != "" || frame.SignerPublicKey != "" {
						t.Fatal("unnegotiated receipt included signing fields")
					}
					return true
				}
			}
			return false
		})
	})
}
