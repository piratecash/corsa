//go:build interop

package node

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestReceiptLifecycleAcrossSecureReleaseRelay(t *testing.T) {
	const secureVersion = 31
	if os.Getenv(interopBinaryEnv(secureVersion)) == "" {
		t.Skip("set CORSA_INTEROP_BIN_V31 to the v2.5.71 node binary")
	}
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	for _, binary := range interopBinaries(t, secureVersion) {
		release := describeRelease(t, fuse, binary, secureVersion)
		if release.release != "2.5.71" {
			t.Fatalf("secure release binary reports %s, want 2.5.71", release.release)
		}
		checkReceiptLifecyclesAcrossReleaseRelay(t, fuse, release)
	}
}

func TestFloor31AcceptsV2571ProbeFirst(t *testing.T) {
	const previousSecureVersion = 31
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	for _, binary := range interopBinaries(t, previousSecureVersion) {
		release := describeRelease(t, fuse, binary, previousSecureVersion)
		if release.release != "2.5.71" {
			t.Fatalf("secure release binary reports %s, want 2.5.71", release.release)
		}
		listener, address, stop := listenerNode(t, nil)
		t.Cleanup(stop)
		node := prepareReleasedNode(t)
		node.probeFirst = true
		node.start(t, fuse, release, address)
		waitForConditionMsg(t, 10*time.Second, "v2.5.71 probe-first did not follow the hint to TLS", func() bool {
			v2, v1 := authenticatedInbound(listener)
			return v2 == 1 && v1 == 0
		})
	}
}

func TestFloor31DeliversToV2571(t *testing.T) {
	const secureVersion = 31
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	for _, binary := range interopBinaries(t, secureVersion) {
		release := describeRelease(t, fuse, binary, secureVersion)
		if release.release != "2.5.71" {
			t.Fatalf("secure release binary reports %s, want 2.5.71", release.release)
		}
		t.Run("current dials the released recipient", func(t *testing.T) {
			old := startReleasedNode(t, fuse, release, freeAddress(t))
			from, stop := startTestNodeWithSetup(t, config.Node{
				ListenerSet: true, ListenerEnabled: false, BootstrapPeers: []string{string(old.listen)},
				ConnectOnly: string(old.listen), Type: config.NodeTypeFull,
			}, nil)
			t.Cleanup(stop)
			waitForConditionMsg(t, 10*time.Second, "no outbound TLS session with the released recipient", func() bool {
				return from.SessionKindStats().OutboundV2 > 0
			})
			checkDirectReleasedReceiptLifecycle(t, from, old)
		})
		t.Run("released recipient dials current", func(t *testing.T) {
			from, address, stop := listenerNode(t, withoutOwnDials(nil))
			t.Cleanup(stop)
			old := startReleasedNode(t, fuse, release, address)
			waitForConditionMsg(t, 10*time.Second, "no inbound TLS session with the released recipient", func() bool {
				return from.SessionKindStats().InboundV2 > 0
			})
			checkDirectReleasedReceiptLifecycle(t, from, old)
		})
	}
}

func TestReleasedUnsignedReceiptsThroughRelayDoNotCompleteDelivery(t *testing.T) {
	const secureVersion = 31
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	for _, binary := range interopBinaries(t, secureVersion) {
		release := describeRelease(t, fuse, binary, secureVersion)
		if release.release != "2.5.71" {
			t.Fatalf("secure release binary reports %s, want 2.5.71", release.release)
		}
		t.Run("legacy recipient behind a relay", func(t *testing.T) {
			output, err := os.Create(filepath.Join(t.TempDir(), "relay-receipt.log"))
			if err != nil {
				t.Fatal(err)
			}
			originalLogger := log.Logger
			log.Logger = originalLogger.Output(output)
			t.Cleanup(func() {
				log.Logger = originalLogger
				if err := output.Close(); err != nil {
					t.Error(err)
				}
			})
			relay, address, stopRelay := listenerNode(t, withoutOwnDials(nil))
			t.Cleanup(stopRelay)
			old := startReleasedNode(t, fuse, release, address)
			from, stopFrom := startTestNodeWithSetup(t, config.Node{
				ListenerSet: true, ListenerEnabled: false, BootstrapPeers: []string{address},
				ConnectOnly: address, Type: config.NodeTypeFull,
			}, nil)
			t.Cleanup(stopFrom)
			relayID := domain.PeerIdentityFromWire(relay.Address())
			waitForConditionMsg(t, 10*time.Second, "no two-hop route to the released recipient", func() bool {
				route := from.RoutingSnapshot().BestRoute(old.peerIdentity())
				return route != nil && route.NextHop == relayID && route.Hops == 2
			})
			assertNoDirectContact(t, from, old.peerIdentity(), "before sending to the released recipient through a relay")
			ref := sendRequiredReceiptDMToRelease(t, from, old)
			markReleasedMessageSeen(t, old, ref)
			for _, status := range []string{protocol.ReceiptStatusDelivered, protocol.ReceiptStatusSeen} {
				waitForConditionMsg(t, 10*time.Second, "author transport did not receive legacy "+status, func() bool {
					return receiptInLog(output.Name(), func(record receiptLogRecord) bool {
						return (record.Message == "received relay_delivery_receipt" || record.Message == "received pushed delivery receipt") &&
							record.Peer == address && record.MessageID == ref.ID && record.Recipient == ref.Sender && record.Status == status
					})
				})
			}
			for _, receipt := range from.fetchDeliveryReceiptsFrame(from.Address()).Receipts {
				if receipt.MessageID == ref.ID {
					t.Fatalf("relayed legacy %s appeared as an authoritative receipt", receipt.Status)
				}
			}
			from.deliveryMu.RLock()
			pending := from.awaitingDelivered[from.ownMessageIdentity(protocol.MessageID(ref.ID), ref.Recipient)] != nil
			from.deliveryMu.RUnlock()
			if !pending {
				t.Fatal("an unsigned receipt from the relay retired the end-to-end delivery retry")
			}
			assertNoDirectContact(t, from, old.peerIdentity(), "after the relay returned both unsigned receipts")
		})
	}
}

func checkDirectReleasedReceiptLifecycle(t *testing.T, from *Service, old *releasedNode) {
	t.Helper()
	ref := sendRequiredReceiptDMToRelease(t, from, old)
	waitForLegacyReceipt(t, from, ref, protocol.ReceiptStatusDelivered)
	markReleasedMessageSeen(t, old, ref)
	waitForLegacyReceipt(t, from, ref, protocol.ReceiptStatusSeen)
	waitForConditionMsg(t, 10*time.Second, "released peer never accepted the direct seen_ack", func() bool {
		return releasedReceiptLogged(old, ref.ID, ref.Sender, ref.Recipient, protocol.ReceiptStatusSeenAck)
	})
}

func sendRequiredReceiptDMToRelease(t *testing.T, from *Service, old *releasedNode) domain.MessageRef {
	t.Helper()
	const body = "read the bound DM v1 on the preceding release"
	ref := domain.MessageRef{Topic: "dm", ID: newMessageID(t), Sender: from.Address(), Recipient: old.keys.Address}
	payload, err := directmsg.EncryptForMessageWithSignedReceipts(from.identity, domain.DMRecipient{
		Address: old.peerIdentity(), BoxKeyBase64: identity.BoxPublicKeyBase64(old.keys.BoxPublicKey),
	}, domain.OutgoingDM{Body: body}, ref.Topic, ref.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	frame := sendMessageFrame(ref.Topic, ref.ID, ref.Sender, ref.Recipient, string(protocol.MessageFlagSenderDelete), time.Now().UTC().Format(time.RFC3339), 0, payload)
	if stored := from.HandleLocalFrame(frame); stored.Type != "message_stored" {
		t.Fatalf("store outgoing DM: %s/%s", stored.Type, stored.Code)
	}
	waitForConditionMsg(t, 10*time.Second, "released recipient did not receive the DM", func() bool {
		raw, err := old.rpc("fetchMessages", map[string]any{"topic": ref.Topic})
		var reply protocol.Frame
		if err != nil || json.Unmarshal(raw, &reply) != nil {
			return false
		}
		for _, message := range reply.Messages {
			if message.ID != ref.ID || message.Sender != ref.Sender || message.Recipient != ref.Recipient {
				continue
			}
			plaintext, err := directmsg.DecryptForMessage(old.keys, ref, identity.PublicKeyBase64(from.identity.PublicKey), message.Body)
			if err != nil || plaintext.Body != body {
				t.Fatalf("the released recipient's stored DM did not decrypt to the expected body: %v", err)
			}
			return true
		}
		return false
	})
	return ref
}

func markReleasedMessageSeen(t *testing.T, old *releasedNode, ref domain.MessageRef) {
	t.Helper()
	raw, err := old.rpc("sendDeliveryReceipt", map[string]any{
		"id": ref.ID, "address": ref.Recipient, "recipient": ref.Sender,
		"status": protocol.ReceiptStatusSeen, "delivered_at": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	var reply protocol.Frame
	if err := json.Unmarshal(raw, &reply); err != nil || reply.Type != "receipt_stored" {
		t.Fatalf("released recipient did not store seen: %s/%s, %v", reply.Type, reply.Code, err)
	}
}

func releasedReceiptLogged(old *releasedNode, id, sender, recipient, status string) bool {
	contents, err := os.ReadFile(filepath.Join(old.dataDir, "corsa.log"))
	if err != nil {
		return false
	}
	for line := range strings.SplitSeq(string(contents), "\n") {
		if !strings.Contains(line, " > stored delivery receipt ") {
			continue
		}
		fields := strings.Fields(line)
		if slices.Contains(fields, "message_id="+id) && slices.Contains(fields, "sender="+sender) &&
			slices.Contains(fields, "recipient="+recipient) && slices.Contains(fields, "status="+status) {
			return true
		}
	}
	return false
}

type receiptLogRecord struct {
	Message   string `json:"message"`
	MessageID string `json:"message_id"`
	Peer      string `json:"peer"`
	Recipient string `json:"recipient"`
	Status    string `json:"status"`
}

func receiptInLog(path string, matches func(receiptLogRecord) bool) bool {
	contents, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range bytes.Split(contents, []byte{'\n'}) {
		var record receiptLogRecord
		if json.Unmarshal(line, &record) == nil && matches(record) {
			return true
		}
	}
	return false
}

func checkReceiptLifecyclesAcrossReleaseRelay(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	t.Helper()
	t.Run("protected without datagrams", func(t *testing.T) {
		const enableDatagrams = false
		from, to := startMixedReceiptNodes(t, fuse, release, enableDatagrams)
		peer := domain.PeerIdentityFromWire(to.Address())
		assertNoDirectContact(t, from, peer, "before sending through the release relay")
		ref := sendDMRequiringSignedReceipts(t, from, to)
		waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusDelivered)
		storeSeenReceipt(t, from, to, ref)
		waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusSeen)
		waitForSeenAck(t, from, to, ref, true)
		assertNoDirectContact(t, from, peer, "after the complete signed receipt lifecycle")
	})
}

func startMixedReceiptNodes(t *testing.T, fuse networkFuse, release releaseUnderTest, enableDatagrams bool) (*Service, *Service) {
	t.Helper()
	fromAddress, toAddress := freeAddress(t), freeAddress(t)
	relay := startReleasedNode(t, fuse, release, fromAddress)
	start := func(address string) *Service {
		svc, stop := startTestNodeWithSetup(t, config.Node{
			ListenAddress: address, BootstrapPeers: []string{string(relay.listen)},
			ConnectOnly: string(relay.listen), Type: config.NodeTypeFull, EnableDatagramV1: enableDatagrams,
		}, nil)
		t.Cleanup(stop)
		return svc
	}
	from, to := start(fromAddress), start(toAddress)
	waitForConditionMsg(t, 40*time.Second, "new nodes did not both reach the old relay", func() bool {
		fromStats, toStats := from.SessionKindStats(), to.SessionKindStats()
		return fromStats.OutboundV2 > 0 && toStats.OutboundV2 > 0
	})
	return from, to
}

func waitForLegacyReceipt(t *testing.T, from *Service, ref domain.MessageRef, status string) {
	t.Helper()
	waitForConditionMsg(t, 10*time.Second, "missing legacy "+status+" receipt", func() bool {
		for _, frame := range from.fetchDeliveryReceiptsFrame(from.Address()).Receipts {
			if frame.MessageID != ref.ID || frame.Status != status {
				continue
			}
			if frame.Sender != ref.Recipient || frame.Recipient != ref.Sender || frame.Signature != "" || frame.SignerPublicKey != "" {
				t.Fatalf("unexpected signing fields in legacy %s receipt", status)
			}
			return true
		}
		return false
	})
}
