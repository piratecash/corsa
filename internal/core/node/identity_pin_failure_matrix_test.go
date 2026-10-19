package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func TestIdentityPinTransactionFailureAndCrashImages(t *testing.T) {
	for _, phase := range []string{"guard_before", "guard_unknown", "main_before", "main_unknown", "cleanup_before", "cleanup_unknown", "receipt_unknown"} {
		for _, rollback := range []string{"confirmed", "before", "unknown"} {
			t.Run(phase+"/rollback_"+rollback, func(t *testing.T) {
				store, binding, token, real := classedPinTransactionFixture(t)
				store.mu.Lock()
				originalBinding := store.snapshotLocked().Endpoints
				store.mu.Unlock()
				permit := domain.IdentityPinRemovalPermit{OperationID: domain.IdentityPinOperationID(phase + "-" + rollback), Binding: binding, Generation: 2}
				injected := errors.New("injected transaction boundary")
				files := NewMockidentityPinDurableFiles(t)
				files.EXPECT().Preflight(mock.Anything).RunAndReturn(real.Preflight).Once()
				files.EXPECT().ReadGuard(mock.Anything).RunAndReturn(real.ReadGuard).Once()
				files.EXPECT().WriteGuard(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
					if phase == "guard_before" {
						return secretfile.DurableNotCommitted, injected
					}
					outcome, err := real.WriteGuard(ctx, raw)
					if phase == "guard_unknown" {
						return secretfile.DurableIndeterminate, injected
					}
					return outcome, err
				}).Once()
				files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
					var file secureSessionFile
					if err := json.Unmarshal(raw, &file); err != nil {
						return secretfile.DurableNotCommitted, err
					}
					_, protected := file.Identities[binding.Identity]
					if protected && rollback == "before" {
						return secretfile.DurableNotCommitted, injected
					}
					if !protected && phase == "main_before" {
						return secretfile.DurableNotCommitted, injected
					}
					outcome, err := real.WriteMain(ctx, raw)
					if protected && rollback == "unknown" {
						return secretfile.DurableIndeterminate, injected
					}
					if !protected && phase == "main_unknown" {
						return secretfile.DurableIndeterminate, injected
					}
					return outcome, err
				}).Maybe()
				cleanupCalls := 0
				files.EXPECT().RemoveGuard(mock.Anything).RunAndReturn(func(ctx context.Context) (secretfile.DurableCommit, error) {
					cleanupCalls++
					if cleanupCalls == 1 && phase == "cleanup_before" {
						return secretfile.DurableNotCommitted, injected
					}
					outcome, err := real.RemoveGuard(ctx)
					if cleanupCalls == 1 && phase == "cleanup_unknown" {
						return secretfile.DurableIndeterminate, injected
					}
					return outcome, err
				}).Maybe()
				fence := NewMockIdentityPinRemovalFence(t)
				fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
				fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, receipt domain.IdentityPinReceipt) error {
					if !store.identityPinned(binding.Identity) {
						t.Error("failed transaction deleted runtime pin")
					}
					if receipt.Kind == domain.IdentityPinReceiptCleanCommitted && phase == "receipt_unknown" {
						return injected
					}
					if receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
						t.Errorf("unexpected receipt: %+v", receipt)
					}
					return nil
				}).Maybe()
				result, err := store.removeIdentityPin(context.Background(), binding, token, fence, files)
				store.mu.Lock()
				epoch := store.removalEpoch
				store.mu.Unlock()
				if epoch != 1 {
					t.Fatal("post-BeginRemoval failure/rollback lost destructive epoch")
				}
				want := domain.IdentityPinRemovalNotCommitted
				if rollback != "confirmed" {
					want = domain.IdentityPinRemovalRecoveryRequired
				}
				if !errors.Is(err, injected) || result.Outcome != want {
					t.Fatalf("result=%+v err=%v want=%s", result, err, want)
				}
				if !store.identityPinned(binding.Identity) {
					t.Fatal("reported failure lost runtime pin")
				}
				store.mu.Lock()
				after := store.snapshotLocked()
				fatal := store.pinRecovery.fatal
				store.mu.Unlock()
				if !reflect.DeepEqual(after.IdentityMetadata[binding.Identity], pinClassMetadataFixture(t)) {
					t.Fatal("rollback erased runtime metadata")
				}
				if !reflect.DeepEqual(originalBinding, after.Endpoints) {
					t.Fatal("failed transaction changed endpoint bindings")
				}
				if fatal != (rollback != "confirmed") {
					t.Fatal("fatal latch does not match unconfirmed rollback")
				}
				// A fresh process must block before networking and may restore only the
				// externally authorised identity; it also covers a guard already removed.
				restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
				diskPin := restarted.identityPinned(binding.Identity)
				if rollback == "confirmed" && !diskPin {
					t.Fatal("confirmed protective rollback lost pin across restart")
				}
				recoveryFiles, err := restarted.openIdentityPinFiles()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = recoveryFiles.Close() }()
				recoveryFence := NewMockIdentityPinRemovalFence(t)
				recoveryFence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, receipt domain.IdentityPinReceipt) error {
					if receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
						t.Error("recovery did not attest protection restoration")
					}
					return nil
				}).Once()
				receipt, err := restarted.recoverIdentityPinProtection(context.Background(), permit, recoveryFence, recoveryFiles)
				if err != nil || receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
					t.Fatalf("recovery %v %v", receipt, err)
				}
				last := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
				if !last.identityPinned(binding.Identity) {
					t.Fatal("offline recovery did not persist pin")
				}
				last.mu.Lock()
				lastFile := last.snapshotLocked()
				last.mu.Unlock()
				if !reflect.DeepEqual(lastFile.IdentityMetadata[binding.Identity], pinClassMetadataFixture(t)) {
					t.Fatal("crash/recovery erased persisted full metadata")
				}
				if !reflect.DeepEqual(originalBinding, lastFile.Endpoints) {
					t.Fatal("recovery changed endpoint bindings")
				}
				if _, err := os.Stat(filepath.Join(store.dir, identityPinGuardName(store.name))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("recovery retained guard: %v", err)
				}
			})
		}
	}
}

func TestIdentityPinRecoveryRejectsDamagedForeignAndOversizedGuard(t *testing.T) {
	for _, kind := range []string{"damaged", "foreign", "oversized", "unsupported", "legacy"} {
		t.Run(kind, func(t *testing.T) {
			store, binding, _, files := pinTransactionFixture(t)
			permit := domain.IdentityPinRemovalPermit{OperationID: "recovery-operation", Binding: binding, Generation: 2}
			guard := identityPinGuardRecord{Format: identityPinGuardFormat, Permit: permit}
			switch kind {
			case "foreign":
				guard.Permit.Binding.Identity = domain.PeerIdentity{99}
			case "unsupported":
				guard.Format = identityPinGuardFormat + 1
			case "legacy":
				guard.Format = 1
			}
			raw, err := json.Marshal(guard)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "damaged":
				raw = []byte("broken")
			case "oversized":
				raw = make([]byte, maxIdentityPinGuardBytes+1)
			}
			path := filepath.Join(store.dir, identityPinGuardName(store.name))
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(store.dir, store.name))
			if err != nil {
				t.Fatal(err)
			}
			restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
			if restarted.startupRefusal() == nil {
				t.Fatal("invalid guard allowed startup")
			}
			fence := NewMockIdentityPinRemovalFence(t)
			if _, err := restarted.recoverIdentityPinProtection(context.Background(), permit, fence, files); err == nil {
				t.Fatal("invalid/foreign guard was repaired without evidence")
			}
			after, err := os.ReadFile(filepath.Join(store.dir, store.name))
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("rejected recovery changed main file")
			}
		})
	}
}

func TestIdentityPinRecoveryFromMainWithoutGuardPreservesOriginalTimestamp(t *testing.T) {
	store, binding, _, files := classedPinTransactionFixture(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "missing-guard", Binding: binding, Generation: 2}
	file, _ := store.identityPinSnapshot(permit, identityPinAuditRemovalWritten)
	raw, _, err := encodeIdentityPinSnapshot(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := confirmedIdentityPinWrite(files.WriteMain(context.Background(), raw)); err != nil {
		t.Fatal(err)
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if restarted.startupRefusal() == nil {
		t.Fatal("enrolled store bypassed mandatory supervisor")
	}
	recoveryFiles, err := restarted.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recoveryFiles.Close() }()
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := restarted.recoverIdentityPinProtection(context.Background(), permit, fence, recoveryFiles); err != nil {
		t.Fatal(err)
	}
	restarted.mu.Lock()
	entry := restarted.identities[binding.Identity]
	restarted.mu.Unlock()
	if !entry.pinnedAt.Equal(binding.PreviousPinnedAt) {
		t.Fatalf("recovery timestamp=%v want=%v", entry.pinnedAt, binding.PreviousPinnedAt)
	}
	if !reflect.DeepEqual(entry.classMetadata(), pinClassMetadataFixture(t)) {
		t.Fatal("independent broker backup lost full pin metadata without local guard/main entry")
	}
}

func TestIdentityPinRecoveryNeverSynthesizesAMissingOriginalMain(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "pins.json")
	marker := filepath.Join(root, "marker.json")
	clock := newStoreClock()
	opts := secureSessionStoreOptions{path: path, markerPath: marker, clock: clock.trusted(), logger: zerolog.Nop(), audit: secretfile.AuditLocation}
	store := openSecureSessionStore(opts)
	id, other := domain.PeerIdentity{31}, domain.PeerIdentity{32}
	if _, err := store.noteProvenOutbound(dialledAt("192.0.2.31:64646"), id, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	if err := store.noteProvenInbound(other); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	binding := pinBindingBackup(t, domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{33}, Store: "lost-original-store"}, RuntimeEpoch: "prior-process", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt})
	permit := domain.IdentityPinRemovalPermit{OperationID: "lost-main-and-marker", Binding: binding, Generation: 2}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	restarted := openSecureSessionStore(opts)
	files, err := restarted.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Maybe()
	if _, err := restarted.recoverIdentityPinProtection(context.Background(), permit, fence, files); err == nil {
		t.Error("recovery replaced a missing multi-pin main with a single permitted pin")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("failed recovery synthesized a new main: %v", err)
	}
}

func TestIdentityPinRecoveryNeverOverwritesACorruptOriginalMain(t *testing.T) {
	store, binding, _, files := pinTransactionFixture(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "corrupt-main", Binding: binding, Generation: 2}
	path := filepath.Join(store.dir, store.name)
	if err := os.WriteFile(path, []byte("unreadable original main"), 0600); err != nil {
		t.Fatal(err)
	}
	restarted := loadSecureSessionStore(path, newStoreClock().trusted())
	fence := NewMockIdentityPinRemovalFence(t)
	if _, err := restarted.recoverIdentityPinProtection(context.Background(), permit, fence, files); !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
		t.Fatalf("corrupt main recovery: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "unreadable original main" {
		t.Fatal("recovery overwrote the original main")
	}
}

func TestIdentityPinAuditIsBoundedAndScopeSnapshotsAreOwned(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	scope := binding.Scope
	store.mu.Lock()
	store.pinRemovalScope = &scope
	store.mu.Unlock()
	for i := range maxIdentityPinAuditRecords + 5 {
		record := identityPinAuditRecord{OperationID: domain.IdentityPinOperationID(fmt.Sprintf("bounded-%d", i)), Identity: binding.Identity, At: binding.PreviousPinnedAt, Phase: identityPinAuditProtectionRestored}
		store.pinRemovalAudit = appendIdentityPinAudit(store.pinRemovalAudit, record)
	}
	if len(store.pinRemovalAudit) != maxIdentityPinAuditRecords || store.pinRemovalAudit[0].OperationID != "bounded-5" {
		t.Fatal("audit was not bounded")
	}
	store.mu.Lock()
	snapshot := store.snapshotLocked()
	store.mu.Unlock()
	snapshot.PinStoreIdentity.Node = domain.PeerIdentity{99}
	if store.pinRemovalScope.Node != binding.Scope.Node {
		t.Fatal("snapshot aliased mutable scope ownership")
	}
}

func TestIdentityPinRecoveryPreservesNewerRepinMetadataAcrossRestart(t *testing.T) {
	store, binding, token, files := classedPinTransactionFixture(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "repinned-before-recovery", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := store.removeIdentityPin(t.Context(), binding, token, fence, files); err != nil {
		t.Fatal(err)
	}
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatal(err)
	}
	newer := pinClassMetadataFixture(t)
	newer.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginOutgoingSend, At: binding.PreviousPinnedAt.Add(1), Reference: "newer-sender-owned-message"}
	stageMetadata(t, store, binding.Identity, newer)
	store.mu.Lock()
	entry := store.identities[binding.Identity]
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	recoveryFiles, err := restarted.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recoveryFiles.Close() }()
	recoveryFence := NewMockIdentityPinRemovalFence(t)
	recoveryFence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, receipt domain.IdentityPinReceipt) error {
		if receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
			t.Error("protective recovery mislabeled as successful removal")
		}
		return nil
	}).Once()
	if _, err := restarted.recoverIdentityPinProtection(t.Context(), permit, recoveryFence, recoveryFiles); err != nil {
		t.Fatal(err)
	}
	persisted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	got := persisted.identities[binding.Identity]
	if !got.pinnedAt.Equal(entry.pinnedAt) || got.classMetadata() != newer {
		t.Fatal("older external backup replaced the newer persisted repin")
	}
}
