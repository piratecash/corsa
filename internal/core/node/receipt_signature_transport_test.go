package node

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestDeliveryReceiptUsesOriginalTransport(t *testing.T) {
	for _, direction := range []string{"inbound", "outbound"} {
		for _, path := range []string{"push", "relay"} {
			t.Run(direction+"/"+path, func(t *testing.T) {
				svc := newTestService(t, config.NodeTypeClient)
				svc.runCtx = t.Context()
				signer := registerSenderKey(t, svc)
				id := svc.ownMessageIdentity("receipt-transport", signer.Address)
				ref := deliveryMessageRef(id)
				svc.sentDMIDs.Add(ref.Key())
				svc.awaitingDelivered[id] = &deliveryRetryEntry{Envelope: protocol.Envelope{
					ID: id.MessageID, Topic: "dm", Sender: svc.Address(), Recipient: signer.Address,
				}}
				receipt := protocol.DeliveryReceipt{
					MessageID: id.MessageID, Sender: signer.Address, Recipient: svc.Address(),
					Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Now().UTC().Truncate(time.Second),
					SignerPublicKey: identity.PublicKeyBase64(signer.PublicKey),
				}
				var err error
				receipt.Signature, err = protocol.SignDeliveryReceipt(domain.NetworkID(networkName), ref, receipt, signer)
				if err != nil {
					t.Fatal(err)
				}
				frame := relayReceiptFrame(receipt)
				if path == "push" {
					body := receiptFrame(receipt)
					frame = protocol.Frame{Type: "push_delivery_receipt", Receipt: &body}
				}
				old, dispatchOld := newApplicationReceiveTransport(t, svc, direction == "inbound", 5960, "192.0.2.107:64646")
				_, dispatchLive := newApplicationReceiveTransport(t, svc, direction == "inbound", 5961, "192.0.2.108:64646")
				old.ShutSendQueue()
				if svc.netCoreForID(old.ConnID()) != old {
					t.Fatal("test must exercise the interval before unregister")
				}
				assertReceipt := func(want bool, wantCount int) {
					t.Helper()
					svc.deliveryMu.RLock()
					_, retry := svc.awaitingDelivered[id]
					seen := svc.seenReceipts.Has(receiptKeyOf(receipt))
					count := len(svc.receipts[svc.Address()])
					svc.deliveryMu.RUnlock()
					if seen != want || retry == want || count != wantCount {
						t.Fatalf("receipt state: seen=%v retry=%v count=%d; want seen=%v retry=%v count=%d", seen, retry, count, want, !want, wantCount)
					}
				}
				dispatchOld(frame)
				assertReceipt(false, 0)
				dispatchLive(frame)
				assertReceipt(true, 1)
			})
		}
	}
}

func TestSignedReceiptSurvivesRelayAndPushRebuild(t *testing.T) {
	signer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	author, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	network := domain.NetworkID(networkName)
	ref := domain.MessageRef{Topic: "dm", ID: "message-1", Sender: author.Address, Recipient: signer.Address}
	original := protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(ref.ID), Sender: signer.Address, Recipient: author.Address,
		Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Unix(1700000000, 0).UTC(),
	}
	original.SignerPublicKey = identity.PublicKeyBase64(signer.PublicKey)
	original.Signature, err = protocol.SignDeliveryReceipt(network, ref, original, signer)
	if err != nil {
		t.Fatal(err)
	}

	roundTrip := func(frame protocol.Frame) protocol.Frame {
		t.Helper()
		raw, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		var decoded protocol.Frame
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}

	flat := roundTrip(relayReceiptFrame(original))
	transit, err := receiptFromFrame(flat)
	if err != nil {
		t.Fatal(err)
	}
	nested := receiptFrame(transit)
	delivered := roundTrip(protocol.Frame{Type: "push_delivery_receipt", Receipt: &nested})
	endpoint, err := receiptFromReceiptFrame(*delivered.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Signature != original.Signature || endpoint.SignerPublicKey != original.SignerPublicKey {
		t.Fatal("receipt signature changed in transit")
	}
	if err := protocol.VerifyDeliveryReceipt(network, ref, endpoint, identity.PublicKeyBase64(signer.PublicKey), endpoint.Signature); err != nil {
		t.Fatalf("receipt no longer authenticates after transport rebuild: %v", err)
	}
}

func TestSignedReceiptIngressRejectsForgeryBeforeDedup(t *testing.T) {
	for _, path := range []string{"push", "relay"} {
		t.Run(path, func(t *testing.T) {
			author, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			recipient, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			relay, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			svc := &Service{}
			svc.initMaps()
			svc.identity = author
			svc.pubKeys[recipient.Address] = identity.PublicKeyBase64(recipient.PublicKey)
			id := svc.ownMessageIdentity("0b6f3f2e-6c55-4a8e-9a8e-6f3c2b1a0d31", recipient.Address)
			svc.awaitingDelivered[id] = &deliveryRetryEntry{Envelope: protocol.Envelope{
				ID: id.MessageID, Topic: "dm", Sender: author.Address, Recipient: recipient.Address,
			}}
			svc.sentDMIDs.Add(deliveryMessageRef(id).Key())
			conn := mustConnIDForTest(svc, drainPipe(t))
			receipt := protocol.DeliveryReceipt{MessageID: id.MessageID, Sender: recipient.Address,
				Recipient: author.Address, Status: protocol.ReceiptStatusSeen, DeliveredAt: time.Now().UTC().Truncate(time.Second)}
			ref := deliveryMessageRef(id)
			sig, err := protocol.SignDeliveryReceipt(domain.NetworkID(networkName), ref, receipt, recipient)
			if err != nil {
				t.Fatal(err)
			}
			arrive := func(signature string) {
				stamp := receipt.DeliveredAt.Format(time.RFC3339)
				if path == "push" {
					svc.handleInboundPushDeliveryReceipt(conn, protocol.Frame{Type: "push_delivery_receipt", Receipt: &protocol.ReceiptFrame{
						MessageID: string(id.MessageID), Sender: receipt.Sender, Recipient: receipt.Recipient,
						Status: receipt.Status, DeliveredAt: stamp, Signature: signature,
					}})
				} else {
					svc.handleInboundRelayDeliveryReceipt(conn, protocol.Frame{Type: "relay_delivery_receipt",
						ID: string(id.MessageID), Address: receipt.Sender, Recipient: receipt.Recipient,
						Status: receipt.Status, DeliveredAt: stamp, Signature: signature,
					})
				}
			}
			for _, forged := range []string{"invalid", identity.SignPayload(relay, []byte("forged"))} {
				arrive(forged)
				svc.deliveryMu.RLock()
				_, retry := svc.awaitingDelivered[id]
				seen := svc.seenReceipts.Has(receiptKeyOf(receipt))
				count := len(svc.receipts[author.Address])
				svc.deliveryMu.RUnlock()
				if !retry || seen || count != 0 {
					t.Fatalf("bad signature mutated state: retry=%v seen=%v count=%d", retry, seen, count)
				}
			}
			arrive(sig)
			receipt.Signature = sig
			svc.deliveryMu.RLock()
			_, retry := svc.awaitingDelivered[id]
			seen := svc.seenReceipts.Has(receiptKeyOf(receipt))
			count := len(svc.receipts[author.Address])
			svc.deliveryMu.RUnlock()
			if retry || !seen || count != 1 {
				t.Fatalf("genuine signature was not accepted: retry=%v seen=%v count=%d", retry, seen, count)
			}
		})
	}
}

func TestTransitSignedReceiptSurvivesUnsignedAndForgedCopies(t *testing.T) {
	author, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{identity: &identity.Identity{Address: "transit-node"}}
	svc.initMaps()
	now := time.Now().UTC().Truncate(time.Second)
	ref := domain.MessageRef{Topic: "dm", ID: "signed-transit-1", Sender: author.Address, Recipient: recipient.Address}
	target := domain.PeerAddress("signed-transit-target")
	if !svc.relayStates.store(&relayForwardState{Message: ref, ReceiptForwardTo: target, RemainingTTL: 5}) {
		t.Fatal("missing reverse route")
	}
	svc.sessions[target] = &peerSession{address: target, authOK: true, sendCh: make(chan peerSendItem, 4),
		capabilities: []domain.Capability{domain.CapMeshRelayV1}}
	svc.health[target] = &peerHealth{Connected: true, LastPongAt: now}

	unsigned := protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(ref.ID), Sender: recipient.Address, Recipient: author.Address,
		Status: protocol.ReceiptStatusDelivered, DeliveredAt: now,
	}
	signed := unsigned
	signed.SignerPublicKey = identity.PublicKeyBase64(recipient.PublicKey)
	signed.Signature, err = protocol.SignDeliveryReceipt(domain.NetworkID(networkName), ref, signed, recipient)
	if err != nil {
		t.Fatal(err)
	}
	forged := signed
	forged.SignerPublicKey = identity.PublicKeyBase64(author.PublicKey)
	sender := legacyOutboundSession("other-hop", domain.PeerIdentity{})
	arrive := func(receipt protocol.DeliveryReceipt) {
		svc.dispatchPeerSessionFrame(sender.address, sender, relayReceiptFrame(receipt))
	}
	arrive(forged)
	select {
	case <-svc.sessions[target].sendCh:
		t.Fatal("forwarded forged receipt")
	default:
	}
	if svc.isTransitReceiptSeen(signed) {
		t.Fatal("forged receipt poisoned the signed key")
	}
	arrive(unsigned)
	select {
	case <-svc.sessions[target].sendCh:
	default:
		t.Fatal("legacy receipt was not forwarded")
	}
	if pendingFrameKey(target, relayReceiptFrame(unsigned)) == pendingFrameKey(target, relayReceiptFrame(signed)) {
		t.Fatal("legacy copy shares the signed pending slot")
	}
	arrive(signed)
	select {
	case got := <-svc.sessions[target].sendCh:
		if got.Signature != signed.Signature || got.PubKey != signed.SignerPublicKey {
			t.Fatal("signature or signer key was lost in forwarding")
		}
	default:
		t.Fatal("genuine signed receipt was suppressed")
	}
	arrive(signed)
	select {
	case <-svc.sessions[target].sendCh:
		t.Fatal("forwarded duplicate signed receipt")
	default:
	}
}
