//go:build interop

package node

import (
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestReceiptLifecycleAcrossLegacyRelay(t *testing.T) {
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	skipped := interopSkips(t)
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			if skipped[interopVersionKey(version)] {
				t.Skipf("protocol %d omitted by CORSA_INTEROP_SKIP", version)
			}
			for _, binary := range interopBinaries(t, version) {
				release := describeRelease(t, fuse, binary, version)
				t.Run(release.release, func(t *testing.T) {
					checkReceiptLifecyclesAcrossReleaseRelay(t, fuse, release)
				})
			}
		})
	}
}

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
		if release.protocol >= config.ProtocolVersionSecureSession {
			return fromStats.OutboundV2 > 0 && toStats.OutboundV2 > 0
		}
		return fromStats.OutboundLegacy > 0 && toStats.OutboundLegacy > 0
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
