package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestDMV2ReseedRestoresAuthorFlag(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	binding := directmsg.V2Binding{
		Network: networkName, ID: "v2-reseed", Topic: "dm",
		Sender: svc.Address(), Recipient: recipient.Address, Flag: string(protocol.MessageFlagSenderDelete),
	}
	body, err := directmsg.SealV2(svc.identity, domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(recipient.Address),
		BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
	}, directmsg.V2Metadata{V2Binding: binding, CreatedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()},
		directmsg.PlainMessage{Body: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	row := protocol.Envelope{
		ID: protocol.MessageID(binding.ID), Topic: "dm", Sender: svc.Address(), Recipient: recipient.Address,
		Flag: protocol.MessageFlagAnyDelete, Payload: []byte(body), CreatedAt: now,
	}
	mismatched := row
	mismatched.ID = "v2-reseed-wrong-id"
	damaged := row
	damaged.ID = "v2-reseed-damaged"
	damaged.Payload = []byte(body[:32])
	svc.RegisterDeliveryOutbox(stubDeliveryOutbox{envelopes: []protocol.Envelope{row, mismatched, damaged}})

	svc.deliveryMu.RLock()
	id := messageIdentityOf(row)
	entry := svc.awaitingDelivered[id]
	wrongIDQueued := svc.awaitingDelivered[messageIdentityOf(mismatched)] != nil
	damagedQueued := svc.awaitingDelivered[messageIdentityOf(damaged)] != nil
	svc.deliveryMu.RUnlock()
	if entry == nil || entry.Envelope.Flag != protocol.MessageFlagSenderDelete || string(entry.Envelope.Payload) != body {
		t.Fatalf("signed wire fields were not restored: %+v", entry)
	}
	if wrongIDQueued || damagedQueued {
		t.Fatalf("invalid v2 rows entered retry: wrong ID=%v damaged=%v", wrongIDQueued, damagedQueued)
	}

	receipt := protocol.DeliveryReceipt{
		MessageID: row.ID, Sender: recipient.Address, Recipient: svc.Address(),
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: now,
	}
	if result := svc.storeDeliveryReceipt(receipt); result.stored || result.ackable {
		t.Fatalf("unsigned receipt accepted after reseed: %+v", result)
	}
	svc.deliveryMu.RLock()
	stillPending := svc.awaitingDelivered[id] != nil
	seen := svc.seenReceipts.Has(receiptKeyOf(receipt))
	svc.deliveryMu.RUnlock()
	if !stillPending || seen {
		t.Fatalf("unsigned receipt changed retry or dedup: pending=%v seen=%v", stillPending, seen)
	}
	svc.addKnownPubKey(recipient.Address, identity.PublicKeyBase64(recipient.PublicKey))
	receipt.Signature, err = protocol.SignDeliveryReceipt(domain.NetworkID(networkName), deliveryMessageRef(id), receipt, recipient)
	if err != nil {
		t.Fatal(err)
	}
	if result := svc.storeDeliveryReceipt(receipt); !result.stored || !result.ackable {
		t.Fatalf("valid signed receipt refused: %+v", result)
	}
	svc.deliveryMu.RLock()
	defer svc.deliveryMu.RUnlock()
	if svc.awaitingDelivered[id] != nil || !svc.seenReceipts.Has(receiptKeyOf(receipt)) {
		t.Fatal("signed receipt failed to commit retry and dedup")
	}
}

type requiredReceiptStore struct {
	staticResultMessageStore
	ref domain.MessageRef
}

func (s requiredReceiptStore) RequiresSignedReceipt(ref domain.MessageRef) (bool, error) {
	return ref == s.ref, nil
}

func TestStoredDMV2ReceiptPolicyAppliesWithoutDeliveryRetry(t *testing.T) {
	self, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{protocol.ReceiptStatusSeen, protocol.ReceiptStatusSeenAck} {
		t.Run(status, func(t *testing.T) {
			svc := &Service{identity: self}
			svc.initMaps()
			ref := domain.MessageRef{Topic: "dm", ID: "stored-dm", Sender: self.Address, Recipient: peer.Address}
			if status == protocol.ReceiptStatusSeenAck {
				ref.Sender, ref.Recipient = ref.Recipient, ref.Sender
				now := time.Now().UTC().Truncate(time.Second)
				svc.registerAwaitingSeenAckLocked(protocol.DeliveryReceipt{
					MessageID: protocol.MessageID(ref.ID), Sender: self.Address, Recipient: peer.Address,
					Status: protocol.ReceiptStatusSeen, DeliveredAt: now,
				}, now)
			} else {
				svc.sentDMIDs.Add(ref.Key())
			}
			svc.RegisterMessageStore(requiredReceiptStore{ref: ref})
			receipt := protocol.DeliveryReceipt{
				MessageID: protocol.MessageID(ref.ID), Sender: peer.Address,
				Recipient: self.Address, Status: status, DeliveredAt: time.Now().UTC().Truncate(time.Second),
			}
			if result := svc.storeDeliveryReceipt(receipt); result.stored || result.ackable {
				t.Fatalf("unsigned receipt accepted: %+v", result)
			}
			if svc.seenReceipts.Has(receiptKeyOf(receipt)) || len(svc.receipts[self.Address]) != 0 {
				t.Fatal("unsigned receipt mutated local state")
			}
			if status == protocol.ReceiptStatusSeenAck && svc.awaitingSeenAck[madeByMessageAuthor(receipt)] == nil {
				t.Fatal("unsigned seen_ack ended the seen retry")
			}
		})
	}
}

func TestDMV2SeenReseedSignsAndAcceptsOnlySignedAck(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	author, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: "reseed-seen-v2", Sender: author.Address, Recipient: svc.Address()}
	svc.RegisterMessageStore(requiredReceiptStore{ref: ref})
	seen := protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(ref.ID), Sender: svc.Address(), Recipient: author.Address,
		Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Unix(1700000000, 123).UTC(),
	}
	journal := &stubSeenAckJournal{unconfirmed: []protocol.DeliveryReceipt{seen}}
	svc.RegisterDeliveryOutbox(journal)
	id := madeByMessageRecipient(seen)
	svc.deliveryMu.RLock()
	entry := svc.awaitingSeenAck[id]
	svc.deliveryMu.RUnlock()
	if entry == nil {
		t.Fatal("seen retry not restored")
	}
	network := domain.NetworkID(networkName)
	if err := protocol.VerifyDeliveryReceipt(network, ref, entry.Receipt, identity.PublicKeyBase64(svc.identity.PublicKey), entry.Receipt.Signature); err != nil {
		t.Fatalf("reseeded seen signature: %v", err)
	}

	ack := protocol.DeliveryReceipt{
		MessageID: seen.MessageID, Sender: author.Address, Recipient: svc.Address(),
		Status: protocol.ReceiptStatusSeenAck, DeliveredAt: seen.DeliveredAt.Truncate(time.Second),
	}
	if got := svc.storeDeliveryReceipt(ack); got.ackable {
		t.Fatal("unsigned seen_ack accepted")
	}
	svc.addKnownPubKey(author.Address, identity.PublicKeyBase64(author.PublicKey))
	ack.Signature, err = protocol.SignDeliveryReceipt(network, ref, ack, author)
	if err != nil {
		t.Fatal(err)
	}
	if got := svc.storeDeliveryReceipt(ack); !got.ackable {
		t.Fatal("signed seen_ack refused")
	}
	svc.WaitBackground()
	svc.deliveryMu.RLock()
	pending := svc.awaitingSeenAck[id] != nil
	svc.deliveryMu.RUnlock()
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if pending || len(journal.confirmed) != 1 || journal.confirmed[0] != ref {
		t.Fatalf("signed seen_ack failed to settle: pending=%v confirmed=%v", pending, journal.confirmed)
	}
}
