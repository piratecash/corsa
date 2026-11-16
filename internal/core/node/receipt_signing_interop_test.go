//go:build interop

package node

import (
	"os"
	"testing"
	"time"

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
		address := freeAddress(t)
		old := startReleasedNode(t, fuse, release, address)
		from, stop := startTestNodeWithSetup(t, config.Node{
			ListenAddress: address, BootstrapPeers: []string{string(old.listen)},
			ConnectOnly: string(old.listen), Type: config.NodeTypeFull,
		}, nil)
		t.Cleanup(stop)
		waitForConditionMsg(t, 10*time.Second, "no TLS session with the released recipient", func() bool {
			return from.SessionKindStats().OutboundV2 > 0
		})
		ref := domain.MessageRef{Topic: "dm", ID: newMessageID(t), Sender: from.Address(), Recipient: old.keys.Address}
		boxKey := identity.BoxPublicKeyBase64(old.keys.BoxPublicKey)
		payload, err := directmsg.EncryptForMessage(from.identity, domain.DMRecipient{
			Address: old.peerIdentity(), BoxKeyBase64: boxKey,
		}, domain.OutgoingDM{Body: "read the bound DM v1 on the preceding release"}, ref.Topic, ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		frame := sendMessageFrame(ref.Topic, ref.ID, ref.Sender, ref.Recipient, string(protocol.MessageFlagSenderDelete), time.Now().UTC().Format(time.RFC3339), 0, payload)
		if stored := from.HandleLocalFrame(frame); stored.Type != "message_stored" {
			t.Fatalf("store outgoing DM: %s/%s", stored.Type, stored.Code)
		}
		waitForConditionMsg(t, 10*time.Second, "released recipient did not receive the DM", func() bool {
			return old.hasDM(ref.ID)
		})
		waitForLegacyReceipt(t, from, ref, protocol.ReceiptStatusDelivered)
	}
}

func checkReceiptLifecyclesAcrossReleaseRelay(t *testing.T, fuse networkFuse, release releaseUnderTest) {
	t.Helper()
	t.Run("unnegotiated", func(t *testing.T) {
		from, to := startMixedReceiptNodes(t, fuse, release)
		peer := domain.PeerIdentityFromWire(to.Address())
		if from.SupportsSignedReceipts(peer) {
			t.Fatal("receipt signing negotiated without a direct proved session")
		}
		ref := domain.MessageRef{Topic: "dm", ID: newMessageID(t), Sender: from.Address(), Recipient: to.Address()}
		deliverDM(t, from, to, ref.ID, "confirm release relay delivery")
		waitForLegacyReceipt(t, from, ref, protocol.ReceiptStatusDelivered)
		storeSeenReceipt(t, from, to, ref)
		waitForLegacyReceipt(t, from, ref, protocol.ReceiptStatusSeen)
		waitForSeenAck(t, from, to, ref, false)
		assertNoDirectContact(t, from, peer, "after the complete receipt lifecycle")
	})
	t.Run("protected", func(t *testing.T) {
		from, to := startMixedReceiptNodes(t, fuse, release)
		peer := domain.PeerIdentityFromWire(to.Address())
		// Model an envelope sealed under the signed receipt policy after the
		// direct session used to choose that policy is no longer available.
		ref := sendDMRequiringSignedReceipts(t, from, to)
		waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusDelivered)
		storeSeenReceipt(t, from, to, ref)
		waitForSignedReceipt(t, from, to, ref, protocol.ReceiptStatusSeen)
		waitForSeenAck(t, from, to, ref, true)
		assertNoDirectContact(t, from, peer, "after the complete signed receipt lifecycle")
	})
}

func startMixedReceiptNodes(t *testing.T, fuse networkFuse, release releaseUnderTest) (*Service, *Service) {
	t.Helper()
	fromAddress, toAddress := freeAddress(t), freeAddress(t)
	relay := startReleasedNode(t, fuse, release, fromAddress)
	start := func(address string) *Service {
		svc, stop := startTestNodeWithSetup(t, config.Node{
			ListenAddress: address, BootstrapPeers: []string{string(relay.listen)},
			ConnectOnly: string(relay.listen), Type: config.NodeTypeFull, EnableDatagramV1: true,
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
			if frame.Signature != "" || frame.SignerPublicKey != "" {
				t.Fatalf("unexpected signing fields in legacy %s receipt", status)
			}
			return true
		}
		return false
	})
}
