//go:build linux

package loadstand

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// The native harness supplies exactly these two immutable cross-built files
// and the owned mounts. Ordinary Go tests never start this privileged fixture.
func TestLinuxOwnedFixtureNative(t *testing.T) {
	if os.Getenv("LOADSTAND_LINUX_NATIVE") != "1" {
		t.Skip("requires separately admitted own-container native fixture")
	}
	helperBinary, targetBinary := os.Getenv("LOADSTAND_NATIVE_HELPER"), os.Getenv("LOADSTAND_NATIVE_TARGET")
	helperHash, targetHash := os.Getenv("LOADSTAND_NATIVE_HELPER_SHA"), os.Getenv("LOADSTAND_NATIVE_TARGET_SHA")
	for _, base := range []string{"/profiles", "/unsupported"} {
		if !t.Run(base, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			var nonce LaunchNonce
			if _, err := rand.Read(nonce[:]); err != nil {
				t.Fatal(err)
			}
			parent, err := CreateAuditedProfileParent(ctx, base, nonce)
			if err != nil {
				t.Fatal("actual parent creation refused", err)
			}
			defer linuxTestClose(t, parent)
			namespace, err := CreateOwnedNetNS(ctx)
			if namespace != nil {
				defer func() {
					cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
					defer stop()
					if err := namespace.Close(cleanup); err != nil {
						t.Error("owned namespace cleanup unconfirmed", err)
					}
				}()
			}
			if err != nil {
				t.Fatal("actual namespace creation refused", err)
			}
			credentials, err := NewCredentials()
			if err != nil {
				t.Fatal(err)
			}
			config := ChildConfig{DataDir: parent.MemberDirectory(), Listen: domain.ListenAddress("127.0.0.1:42050"), RPC: domain.ListenAddress("127.0.0.1:42051"), Bootstrap: []domain.PeerAddress{"127.0.0.1:1"}, Listener: false, Procs: 1}
			owner, err := CreateLinuxFixture(ctx, LinuxFixtureSpec{HelperBinary: helperBinary, HelperSHA256: ExecutableSHA256(helperHash), TargetBinary: targetBinary, TargetSHA256: ExecutableSHA256(targetHash), Config: config, Credentials: credentials, UID: 65532, GID: 65532}, parent, namespace)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
				defer stop()
				if err := owner.Close(cleanup); err != nil && !errors.Is(err, ErrLinuxOwnerPoisoned) {
					t.Error("owned fixture cleanup unconfirmed", err)
				}
			}()
			startCtx, startCancel := context.WithTimeout(ctx, 5*time.Second)
			startErr := owner.Start(startCtx)
			startCancel()
			if base == "/unsupported" {
				if !errors.Is(startErr, ErrLinuxOwnerUnsupported) {
					t.Fatal("actual unsupported filesystem was not typed before target", startErr)
				}
				closeCtx, closeCancel := context.WithTimeout(ctx, 3*time.Second)
				defer closeCancel()
				closeErr := owner.Close(closeCtx)
				if closeErr != nil && !errors.Is(closeErr, ErrLinuxOwnerPoisoned) {
					t.Fatal("unsupported owner did not join", closeErr)
				}
				result := owner.Snapshot()
				if result.TargetKnown || !result.HelperWaitKnown || !owner.beforeTargetRefused {
					t.Fatal("unsupported profile created target authority")
				}
				return
			}
			if startErr != nil {
				t.Fatal("actual helper/target initialization refused", startErr)
			}
			// Root's current namespace cannot reach the target's loopback RPC. The
			// exact same endpoint is reachable via the single locked namespace dial.
			plain := net.Dialer{Timeout: 100 * time.Millisecond}
			if conn, err := plain.DialContext(ctx, "tcp4", string(config.RPC)); err == nil {
				linuxTestClose(t, conn)
				t.Fatal("target escaped private network namespace")
			}
			readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
			for {
				_, err = NewProcessSource(owner.RPC()).Status(readyCtx)
				if err == nil {
					break
				}
				select {
				case <-readyCtx.Done():
					readyCancel()
					t.Fatal("namespace-bound RPC unavailable", err)
				case <-time.After(10 * time.Millisecond):
				}
			}
			readyCancel()
			sampleCtx, sampleCancel := context.WithTimeout(ctx, 2*time.Second)
			sample, err := owner.Sample(sampleCtx)
			sampleCancel()
			if err != nil || !sample.Provenance.RunningExecutableVerified || sample.Provenance.UID != 65532 || sample.Provenance.Incarnation.StartTicks == 0 || sample.FinalWait4.Reason != ProcessNotWaitOwner {
				t.Fatal("actual same-UID process observation failed", err)
			}
			if _, err := sample.Descriptors.Get(); err != nil {
				t.Fatal("actual FD observation unavailable", err)
			}
			stopCtx, stopCancel := context.WithTimeout(ctx, 8*time.Second)
			defer stopCancel()
			if err := owner.Close(stopCtx); err != nil {
				t.Fatal("owned target/helper did not gracefully join", err, owner.Snapshot())
			}
			result := owner.Snapshot()
			if !result.TargetKnown || !result.HelperJoined || !result.HelperWaitKnown || !result.TargetExit.Clean() || result.Poisoned || result.Finality != FinalUnavailable {
				t.Fatal("missing actual wait/join facts or fabricated finality", result)
			}
			if _, err := owner.Sample(ctx); !errors.Is(err, ErrLinuxOwnerPoisoned) {
				t.Fatal("closed owner admitted another Sample", err)
			}
		}) {
			return
		} // no second owner after any unconfirmed previous fixture
	}
}
