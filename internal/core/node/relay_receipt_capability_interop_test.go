//go:build interop

package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestRelayReceiptCapabilityAcrossSecureRelease(t *testing.T) {
	const releaseVersion = 31
	if os.Getenv(interopBinaryEnv(releaseVersion)) == "" {
		t.Skip("set CORSA_INTEROP_BIN_V31 to the v2.5.71 node binary")
	}
	fuse := requireNetworkFuse(t)
	requireHeavyRunLock(t)
	for _, binary := range interopBinaries(t, releaseVersion) {
		release := describeRelease(t, fuse, binary, releaseVersion)
		if release.release != "2.5.71" {
			t.Fatalf("secure release binary reports %s, want 2.5.71", release.release)
		}
		t.Run("through_old_relay", func(t *testing.T) {
			from, to := startMixedReceiptNodes(t, fuse, release)
			target := domain.PeerIdentityFromWire(to.Address())
			boxKey := identity.BoxPublicKeyBase64(to.identity.BoxPublicKey)
			waitForConditionMsg(t, 10*time.Second, "missing live receipt capability through the released relay", func() bool {
				ctx, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
				defer cancel()
				return from.ProveRelaySignedReceipts(ctx, target, boxKey)
			})
			assertNoDirectContact(t, from, target, "after live receipt capability proof")
			assertNoDirectContact(t, to, domain.PeerIdentityFromWire(from.Address()), "after live receipt capability proof")
		})
		t.Run("old_recipient", func(t *testing.T) {
			address := freeAddress(t)
			old := startReleasedNode(t, fuse, release, address)
			from, stop := startTestNodeWithSetup(t, config.Node{
				ListenAddress: address, BootstrapPeers: []string{string(old.listen)},
				ConnectOnly: string(old.listen), Type: config.NodeTypeFull, EnableDatagramV1: true,
			}, nil)
			t.Cleanup(stop)
			waitForConditionMsg(t, 40*time.Second, "no proved session with the older recipient", func() bool {
				return from.SessionKindStats().OutboundV2 > 0
			})
			if from.SupportsSignedReceipts(old.peerIdentity()) {
				t.Fatal("older recipient advertised mandatory receipt signatures")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
			defer cancel()
			if from.ProveRelaySignedReceipts(ctx, old.peerIdentity(), identity.BoxPublicKeyBase64(old.keys.BoxPublicKey)) {
				t.Fatal("older recipient confirmed mandatory receipt signatures")
			}
		})
	}
}
