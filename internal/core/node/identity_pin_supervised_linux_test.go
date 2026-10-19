//go:build linux

package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/startupfence"
	"github.com/piratecash/corsa/internal/testutil/startupfencetest"
	"github.com/stretchr/testify/mock"
)

// The child is a separate UID using the actual private supervisor channel and
// the actual node store. No test interface can mint the verified session.
func TestIdentityPinSupervisedChild(t *testing.T) {
	split := -1
	for i, arg := range os.Args {
		if arg == "--" {
			split = i
			break
		}
	}
	if split < 0 {
		return
	}
	args := os.Args[split+1:]
	if len(args) != 4 || args[0] != "pin-child" {
		t.Fatal("invalid child arguments")
	}
	dir, mode, storeID := args[1], args[2], startupfence.StoreID(args[3])
	id, err := identity.Load(filepath.Join(dir, "identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(config.Node{ListenAddress: "127.0.0.1:0", IdentityPath: filepath.Join(dir, "identity.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json"), PeersStatePath: filepath.Join(dir, "peers.json"), TrustStorePath: filepath.Join(dir, "trust.json"), BootstrapPeers: []string{}, Type: domain.NodeTypeFull}, id, nil)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	session, err := startupfence.ConnectSupervisor(os.NewFile(3, "supervisor"), exe, domain.PeerIdentityFromWire(id.Address), storeID)
	if err != nil {
		t.Fatal(err)
	}
	// The final close must follow all local writes and Service teardown.
	defer func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, recovery := session.RecoveryPermit(); recovery {
		if mode != "recover" && mode != "recover-crash" {
			t.Fatal("unexpected recovery role")
		}
		if err := svc.AdmitSupervisedIdentityPinStartup(session); !errors.Is(err, domain.ErrIdentityPinRecoveryOfflineOnly) {
			t.Fatalf("recovery admitted networking: %v", err)
		}
		if err := svc.RecoverSupervisedIdentityPinProtection(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		if !svc.secureSessions.store.identityPinned(domain.PeerIdentity{21}) {
			t.Fatal("recovery did not restore pin")
		}
		if err := svc.Run(context.Background()); !errors.Is(err, domain.ErrIdentityPinRecoveryOfflineOnly) {
			t.Fatalf("recovery started Run: %v", err)
		}
		if mode == "recover-crash" {
			t.Error("simulated crash after restored receipt before CompleteRecovery")
		}
		return
	}
	if err := svc.AdmitSupervisedIdentityPinStartup(session); err != nil {
		t.Fatal(err)
	}
	if err := svc.StartupRefusal(); err != nil {
		t.Fatal(err)
	}
	pinned := domain.PeerIdentity{21}
	if mode == "restart-clean" || mode == "restart-restored" {
		if got := svc.secureSessions.store.identityPinned(pinned); got != (mode == "restart-restored") {
			t.Fatalf("wrong pin after finalized restart: %v", got)
		}
		if !svc.secureSessions.store.endpointRequiresV2("192.0.2.21:64646") {
			t.Fatal("restart lost endpoint")
		}
		return
	}
	preview, err := svc.PreviewIdentityPinRemoval(context.Background(), pinned)
	if err != nil {
		t.Fatal(err)
	}
	if preview.RemovalAvailable {
		t.Fatal("private adapter enabled public confirm")
	}
	if _, err := svc.ConfirmIdentityPinRemoval(context.Background(), pinned, preview.Token); !errors.Is(err, domain.ErrIdentityPinRemovalUnavailable) {
		t.Fatalf("public confirm enabled: %v", err)
	}
	if mode == "clean" {
		result, err := svc.removeSupervisedIdentityPin(context.Background(), session, pinned, preview.Token)
		if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
			t.Fatalf("actual removal: %+v %v", result, err)
		}
		if svc.secureSessions.store.identityPinned(pinned) {
			t.Fatal("committed pin retained")
		}
		if !svc.secureSessions.store.endpointRequiresV2("192.0.2.21:64646") {
			t.Fatal("removal erased endpoint")
		}
		return
	}
	if mode != "fatal" {
		t.Fatal("unknown mode")
	}
	// Obtain a real external permit and receipt, then simulate unknown local
	// rollback. The Run fatal defer must beat a successful ordinary final flush.
	store := svc.secureSessions.store
	store.mu.Lock()
	entry := store.identities[pinned]
	store.mu.Unlock()
	supervisedBinding, err := session.Binding(pinned, startupfence.PinRevision(entry.evidenceRevision), entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding := identityPinBindingFromSupervisor(supervisedBinding)
	fence := supervisedIdentityPinFence{session: session}
	permit, err := fence.BeginRemoval(context.Background(), binding)
	if err != nil {
		t.Fatal(err)
	}
	fake := NewMockIdentityPinRemovalFence(t)
	fake.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	real, err := store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = real.Close() }()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(real.Preflight).Once()
	files.EXPECT().ReadGuard(mock.Anything).RunAndReturn(real.ReadGuard).Once()
	files.EXPECT().WriteGuard(mock.Anything, mock.Anything).RunAndReturn(real.WriteGuard).Once()
	injected := errors.New("unknown local main and rollback")
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, injected).Twice()
	result, err := svc.removeIdentityPinUsingFence(context.Background(), binding, preview.Token, fake, files)
	if result.Outcome != domain.IdentityPinRemovalRecoveryRequired || !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatalf("fatal outcome: %+v %v", result, err)
	}
	// A stale acknowledged clean receipt must never become finalizable on a
	// nonzero exit even when local Run is cancelled before opening a listener.
	receipt := domain.IdentityPinReceipt{Permit: permit, Kind: domain.IdentityPinReceiptCleanCommitted, SnapshotDigest: domain.IdentityPinSnapshotDigest(strings.Repeat("a", 64))}
	if err := fence.RecordReceipt(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	runErr := svc.Run(context.Background())
	if !errors.Is(runErr, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatalf("fatal Run returned success: %v", runErr)
	}
	t.Error(runErr) // Test-process nonzero is observed by the real supervisor.
}

func TestIdentityPinActualSupervisedTransactionAndOfflineRecovery(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Linux supervisor and a separate child UID")
	}
	for _, mode := range []string{"clean", "fatal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			dir, err := os.MkdirTemp("", "pin-adapter-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(dir) }()
			if err := os.Chmod(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			if err := identity.Save(filepath.Join(dir, "identity.json"), id); err != nil {
				t.Fatal(err)
			}
			store := loadSecureSessionStore(filepath.Join(dir, "pins.json"), (&storeClock{now: time.Now()}).trusted())
			if _, err := store.noteProvenOutbound(dialledAt("192.0.2.21:64646"), domain.PeerIdentity{21}, endpointProtectionNone()); err != nil {
				t.Fatal(err)
			}
			if !store.identityPinned(domain.PeerIdentity{21}) {
				t.Fatal("fixture did not persist original pin")
			}
			if err := os.Chown(dir, 65534, 65534); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if err := os.Chown(filepath.Join(dir, entry.Name()), 65534, 65534); err != nil {
					t.Fatal(err)
				}
			}
			ledger, client, enrollment, closeBroker := startupfencetest.NewBroker(t, domain.PeerIdentityFromWire(id.Address))
			defer closeBroker()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := func(mode string) startupfence.ChildCommand {
				return startupfence.ChildCommand{Path: exe, Args: []string{"-test.run=^TestIdentityPinSupervisedChild$", "--", "pin-child", dir, mode, string(enrollment.Store)}, UID: 65534, GID: 65534}
			}
			controller, err := startupfence.NewController(client, enrollment.Node, enrollment.Store)
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.Start(ctx, command(mode)); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = controller.Stop() }()
			if err := controller.Wait(); (err == nil) != (mode == "clean") {
				t.Fatalf("actual child exit mode %s: %v", mode, err)
			}
			status, err := ledger.Status(ctx, enrollment.Node)
			if err != nil {
				t.Fatal(err)
			}
			if status.State != startupfence.StateFenced || status.Receipt == nil {
				t.Fatalf("missing durable fence/receipt: %+v", status)
			}
			if _, err := ledger.PrepareStartup(ctx, enrollment.Node, "bypass"); !errors.Is(err, startupfence.ErrFenced) {
				t.Fatalf("unfinalized startup admitted: %v", err)
			}
			if mode == "fatal" {
				if !status.WriterActive {
					t.Fatal("fatal process sealed a stale clean receipt")
				}
				if _, err := ledger.Finalize(ctx, *status.Receipt); err == nil {
					t.Fatal("fatal clean receipt finalized")
				}
				interrupted, err := startupfence.NewController(client, enrollment.Node, enrollment.Store)
				if err != nil {
					t.Fatal(err)
				}
				if err := interrupted.StartRecovery(ctx, command("recover-crash")); err != nil {
					t.Fatal(err)
				}
				if err := interrupted.Wait(); err == nil {
					t.Fatal("interrupted recovery exited successfully")
				}
				prior, err := ledger.Status(ctx, enrollment.Node)
				if err != nil {
					t.Fatal(err)
				}
				if !prior.WriterActive || prior.Receipt == nil || prior.Receipt.Kind != startupfence.ReceiptProtectionRestored {
					t.Fatal("interrupted recovery did not retain restored selection and active writer")
				}
				recovery, err := startupfence.NewController(client, enrollment.Node, enrollment.Store)
				if err != nil {
					t.Fatal(err)
				}
				if err := recovery.StartRecovery(ctx, command("recover")); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = recovery.Stop() }()
				if err := recovery.Wait(); err != nil {
					t.Fatalf("actual offline recovery: %v", err)
				}
				status, err = ledger.Status(ctx, enrollment.Node)
				if err != nil {
					t.Fatal(err)
				}
				if status.Receipt == nil || status.Receipt.Kind != startupfence.ReceiptProtectionRestored {
					t.Fatal("recovery did not replace stale clean selection")
				}
			}
			if status.WriterActive {
				t.Fatal("successful actual exit did not seal exact acknowledged receipt")
			}
			if _, err := ledger.Finalize(ctx, *status.Receipt); err != nil {
				t.Fatal(err)
			}
			restarted, err := startupfence.NewController(client, enrollment.Node, enrollment.Store)
			if err != nil {
				t.Fatal(err)
			}
			next := "restart-clean"
			if mode == "fatal" {
				next = "restart-restored"
			}
			if err := restarted.Start(ctx, command(next)); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = restarted.Stop() }()
			if err := restarted.Wait(); err != nil {
				t.Fatalf("finalized restart lost correct pin state: %v", err)
			}
		})
	}
}
