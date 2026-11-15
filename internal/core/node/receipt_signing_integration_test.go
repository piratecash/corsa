package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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

	t.Run("legacy delivery keeps an unsigned receipt", func(t *testing.T) {
		pair := startLegacyPair(t, 30)
		waitKinds(t, pair.new, "no outbound legacy session", func(stats domain.SessionKindStats) bool { return stats.OutboundLegacy > 0 })
		id, err := protocol.NewMessageID()
		if err != nil {
			t.Fatal(err)
		}
		deliverDM(t, pair.old, pair.new, string(id), "check legacy receipt")
		waitForConditionMsg(t, 10*time.Second, "missing legacy delivered receipt", func() bool {
			for _, frame := range pair.old.fetchDeliveryReceiptsFrame(pair.old.Address()).Receipts {
				if frame.MessageID == string(id) && frame.Status == protocol.ReceiptStatusDelivered {
					if frame.Signature != "" || frame.SignerPublicKey != "" {
						t.Fatal("legacy peer received signing fields")
					}
					return true
				}
			}
			return false
		})
	})
}
