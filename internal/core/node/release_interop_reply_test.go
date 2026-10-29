//go:build interop

package node

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestReleasedV30OriginReplyWithPresenceDiagnostics exercises the real published node's
// reverse-origin send_message path, which the existing new-to-old inbox and
// old-to-new authentication scenarios do not exercise. The old daemon has no
// DMRouter (sendDm is unavailable), so encryption/decryption here uses the
// owned fixture identities and current dm-v1 codec. This is NOT a claim about
// the released Desktop's composer, encryption, rendering or read badge.
//
// Presence is observed separately, including on a DM failure. It must not be
// used as permission to send. Both old and new sides start with fresh data;
// no pins, routes, contact keys or presence states are injected or removed.
func TestReleasedV30OriginReplyWithPresenceDiagnostics(t *testing.T) {
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	binaries := interopBinaries(t, 30)
	if len(binaries) != 1 {
		t.Fatalf("reverse-origin fixture requires one published V30 binary, got %d", len(binaries))
	}
	release := describeRelease(t, fuse, binaries[0], 30)
	if release.release != "2.4.70" || release.sha256 != "5cf33cc61c7009be436a1eeed606cfb51ac279a264cc3a92a5e536fb0b74f6e5" {
		t.Fatalf("reverse-origin fixture requires the qualified published Darwin arm64 v2.4.70 binary; got version=%q SHA256=%s", release.release, release.sha256)
	}
	for _, orientation := range []string{"dev_dials_old", "old_dials_dev"} {
		t.Run(orientation, func(t *testing.T) {
			newAddress := freeAddress(t)
			cfg := config.Node{ListenAddress: newAddress, Type: config.NodeTypeFull}
			var old *releasedNode
			var dev *Service
			var stop func()
			if orientation == "dev_dials_old" {
				// Old's own bootstrap target is a different unused owned
				// loopback port, so it cannot make the reverse connection.
				old = startReleasedNode(t, fuse, release, freeAddress(t))
				cfg.BootstrapPeers = []string{string(old.listen)}
				cfg.ConnectOnly = string(old.listen)
				dev, stop = startTestNodeWithSetup(t, cfg, func(s *Service) {
					s.disableRateLimiting = false
				})
				t.Cleanup(stop)
			} else {
				dev, stop = startTestNodeWithSetup(t, cfg, withoutOwnDials(func(s *Service) {
					s.disableRateLimiting = false
				}))
				t.Cleanup(stop)
				old = startReleasedNode(t, fuse, release, newAddress)
			}
			var replyID string
			defer func() {
				observeReleasedReplyPresence(t, dev, old)
				if replyID != "" {
					observeReleasedReplyPending(t, old, replyID)
				}
			}()
			waitForConditionMsg(t, 40*time.Second, "the original legacy session never authenticated", func() bool {
				kinds := dev.SessionKindStats()
				if orientation == "dev_dials_old" {
					return kinds.OutboundLegacy > 0
				}
				return kinds.InboundLegacy > 0
			})

			waitForConditionMsg(t, 40*time.Second, "DEV never learned the published peer's original key set", func() bool {
				contacts := dev.HandleLocalFrame(protocol.Frame{Type: "fetch_contacts"})
				if contacts.Type != "contacts" {
					t.Fatalf("DEV contacts returned %q/%q", contacts.Type, contacts.Code)
				}
				for _, contact := range contacts.Contacts {
					if contact.Address == old.keys.Address {
						if contact.PubKey != identity.PublicKeyBase64(old.keys.PublicKey) ||
							contact.BoxKey != identity.BoxPublicKeyBase64(old.keys.BoxPublicKey) || contact.BoxSig == "" {
							t.Fatal("DEV learned a different published-peer key set")
						}
						return true
					}
				}
				return false
			})
			initialID := newMessageID(t)
			const initialBody = "dev-to-published-v30"
			sendDMToRelease(t, dev, old, initialID, initialBody)
			var initial protocol.MessageFrame
			waitForConditionMsg(t, 40*time.Second, "DEV message never reached the published node", func() bool {
				initial = releasedReplyMessage(t, old, initialID)
				return initial.ID != ""
			})
			requireReleasedReplyPlaintext(t, old.keys, initial, dev.Address(), identity.PublicKeyBase64(dev.identity.PublicKey), old.keys.Address, initialBody)

			// Fetch the recipient key from what the released process learned
			// itself. Do not supply the DEV box key directly or import it to
			// make a missing-key regression disappear.
			var contact protocol.ContactFrame
			waitForConditionMsg(t, 40*time.Second, "published node never learned the DEV recipient contact", func() bool {
				contact = releasedReplyContact(t, old, dev.Address())
				return contact.Address != ""
			})
			if contact.PubKey != identity.PublicKeyBase64(dev.identity.PublicKey) ||
				contact.BoxKey != identity.BoxPublicKeyBase64(dev.identity.BoxPublicKey) ||
				contact.BoxSig == "" {
				t.Fatal("published node did not learn the original DEV recipient key set")
			}
			if err := identity.VerifyBoxKeyBinding(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig); err != nil {
				t.Fatalf("published node's learned DEV key binding: %v", err)
			}
			const replyBody = "published-v30-origin-reply"
			ciphertext, err := directmsg.EncryptForParticipants(old.keys, domain.DMRecipient{
				Address: domain.PeerIdentityFromWire(contact.Address), BoxKeyBase64: contact.BoxKey,
			}, domain.OutgoingDM{Body: replyBody, ReplyTo: domain.MessageID(initialID)})
			if err != nil {
				t.Fatalf("encrypt owned old-origin reply: %v", err)
			}
			replyID = newMessageID(t)
			frame := sendMessageFrame("dm", replyID, old.keys.Address, dev.Address(), "sender-delete", time.Now().UTC().Format(time.RFC3339), 0, ciphertext)
			encoded, err := json.Marshal(frame)
			if err != nil {
				t.Fatalf("encode old-origin frame: %v", err)
			}
			var args map[string]any
			if err := json.Unmarshal(encoded, &args); err != nil {
				t.Fatalf("decode old-origin RPC arguments: %v", err)
			}
			raw, err := old.rpc("sendMessage", args)
			if err != nil {
				t.Fatalf("real published node sendMessage: %v", err)
			}
			var stored protocol.Frame
			if err := json.Unmarshal(raw, &stored); err != nil || stored.Type != "message_stored" {
				t.Fatalf("real published node refused old-origin reply: type=%q code=%q decode=%v", stored.Type, stored.Code, err)
			}
			t.Logf("published local acceptance: message=%s type=%s status=%s (not a delivery verdict)", replyID, stored.Type, stored.Status)
			observeReleasedReplyPending(t, old, replyID)
			if own := releasedReplyMessage(t, old, replyID); own.ID != replyID {
				t.Fatal("published node acknowledged reply but has no corresponding stored row")
			}
			var reply protocol.MessageFrame
			waitForConditionMsg(t, 40*time.Second, "published-origin reply never reached DEV", func() bool {
				inbox := dev.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
				if inbox.Type != "messages" {
					t.Fatalf("DEV inbox returned %q/%q", inbox.Type, inbox.Code)
				}
				for _, item := range inbox.Messages {
					if item.ID == replyID {
						reply = item
						return true
					}
				}
				return false
			})
			requireReleasedReplyPlaintext(t, dev.identity, reply, old.keys.Address, identity.PublicKeyBase64(old.keys.PublicKey), dev.Address(), replyBody)
			plain, err := directmsg.DecryptForIdentity(dev.identity, reply.Sender, identity.PublicKeyBase64(old.keys.PublicKey), reply.Recipient, reply.Body)
			if err != nil || plain.ReplyTo != initialID {
				t.Fatalf("old-origin reply reference did not survive: decrypt=%v", err)
			}
			// Read-status controls may work even if an old client cannot
			// originate a message. Observe receipts, but do not invent a UI
			// 'read' action or make receipt success replace DM arrival.
			receiptRaw, err := old.rpc("fetchDeliveryReceipts", nil)
			if err != nil {
				t.Errorf("published receipt observation: %v", err)
			} else {
				var receipts protocol.Frame
				if err := json.Unmarshal(receiptRaw, &receipts); err != nil {
					t.Errorf("published receipt observation decode: %v", err)
				} else {
					for _, receipt := range receipts.Receipts {
						if receipt.MessageID == replyID {
							t.Logf("old-origin receipt: message=%s status=%s", replyID, receipt.Status)
						}
					}
				}
			}
		})
	}
}

func releasedReplyContact(t *testing.T, old *releasedNode, address string) protocol.ContactFrame {
	t.Helper()
	raw, err := old.rpc("fetchContacts", nil)
	if err != nil {
		t.Fatalf("published contact observation: %v", err)
	}
	var frame protocol.Frame
	if err := json.Unmarshal(raw, &frame); err != nil || frame.Type != "contacts" {
		t.Fatalf("published contacts: type=%q decode=%v", frame.Type, err)
	}
	for _, contact := range frame.Contacts {
		if contact.Address == address {
			return contact
		}
	}
	return protocol.ContactFrame{}
}

func releasedReplyMessage(t *testing.T, old *releasedNode, id string) protocol.MessageFrame {
	t.Helper()
	raw, err := old.rpc("fetchMessages", map[string]any{"topic": "dm"})
	if err != nil {
		t.Fatalf("published inbox: %v", err)
	}
	var frame protocol.Frame
	if err := json.Unmarshal(raw, &frame); err != nil || frame.Type != "messages" {
		t.Fatalf("published inbox: type=%q decode=%v", frame.Type, err)
	}
	for _, item := range frame.Messages {
		if item.ID == id {
			return item
		}
	}
	return protocol.MessageFrame{}
}

func requireReleasedReplyPlaintext(t *testing.T, recipient *identity.Identity, frame protocol.MessageFrame, sender, senderKey, to, body string) {
	t.Helper()
	if frame.Sender != sender || frame.Recipient != to {
		t.Fatalf("message endpoint mismatch: sender=%s recipient=%s", frame.Sender, frame.Recipient)
	}
	plain, err := directmsg.DecryptForIdentity(recipient, frame.Sender, senderKey, frame.Recipient, frame.Body)
	if err != nil {
		t.Fatalf("decrypt received message: %v", err)
	}
	if plain.Body != body {
		t.Fatal("decrypted message body differs from the original fixture body")
	}
}

func observeReleasedReplyPresence(t *testing.T, dev *Service, old *releasedNode) {
	t.Helper()
	// This reports diagnostic outcomes, not presence acceptance. A missing
	// row is ABSENT, not offline or online; no state is manufactured here.
	// Never infer presence from the route count or from the DM assertion.
	current := dev.HandleLocalFrame(protocol.Frame{Type: "fetch_presence"})
	found := false
	for _, row := range current.Presence {
		if row.Identity == old.keys.Address {
			found = true
			t.Logf("DEV presence of published peer: state=%s source=%s reason=%s", row.State, row.Source, row.Reason)
		}
	}
	t.Logf("DEV published-peer presence row present=%v; route count=%d; recipient path=%v", found, len(dev.routingTable.Lookup(old.peerIdentity())), dev.recipientHasPath(old.keys.Address))
	raw, err := old.rpc("fetchPresence", nil)
	if err != nil {
		t.Errorf("published presence observation: %v", err)
		return
	}
	var oldFrame protocol.Frame
	if err := json.Unmarshal(raw, &oldFrame); err != nil || oldFrame.Type != "presence" {
		t.Errorf("published presence response: type=%q decode=%v", oldFrame.Type, err)
		return
	}
	found = false
	for _, row := range oldFrame.Presence {
		if row.Identity == dev.Address() {
			found = true
			t.Logf("published presence of DEV: state=%s source=%s reason=%s", row.State, row.Source, row.Reason)
		}
	}
	t.Logf("published DEV presence row present=%v", found)
	lookupRaw, err := old.rpc("fetchRouteLookup", map[string]any{"identity": dev.Address()})
	if err != nil {
		t.Errorf("published route observation: %v", err)
		return
	}
	var lookup struct {
		Identity string `json:"identity"`
		Count    int    `json:"count"`
	}
	if err := json.Unmarshal(lookupRaw, &lookup); err != nil || lookup.Identity != dev.Address() {
		t.Errorf("published route observation: identity=%s decode=%v", lookup.Identity, err)
		return
	}
	t.Logf("published route lookup of DEV: identity=%s count=%d (diagnostic snapshot)", lookup.Identity, lookup.Count)
}

func observeReleasedReplyPending(t *testing.T, old *releasedNode, id string) {
	t.Helper()
	raw, err := old.rpc("fetchPendingMessages", map[string]any{"topic": "dm"})
	if err != nil {
		t.Errorf("published pending observation: %v", err)
		return
	}
	var frame protocol.Frame
	if err := json.Unmarshal(raw, &frame); err != nil || frame.Type != "pending_messages" {
		t.Errorf("published pending response: type=%q decode=%v", frame.Type, err)
		return
	}
	found := false
	for _, row := range frame.PendingMessages {
		if row.ID == id {
			found = true
			t.Logf("published old-origin queue: message=%s recipient=%s status=%s retries=%d last_attempt_at=%s error=%s", id, row.Recipient, row.Status, row.Retries, row.LastAttemptAt, row.Error)
		}
	}
	t.Logf("published old-origin queue row present=%v; message=%s; total=%d", found, id, frame.Count)
}
