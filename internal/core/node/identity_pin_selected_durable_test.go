package node

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func TestSelectedStrictCommitOutcomesAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		commit    secretfile.DurableCommit
		fail      bool
		cancel    bool
		confirmed bool
	}{
		{name: "not-committed", commit: secretfile.DurableNotCommitted},
		{name: "indeterminate", commit: secretfile.DurableIndeterminate},
		{name: "write-error", commit: secretfile.DurableNotCommitted, fail: true},
		{name: "committed-with-error", commit: secretfile.DurableCommitted, fail: true},
		{name: "late-cancel", commit: secretfile.DurableCommitted, cancel: true, confirmed: true},
		{name: "committed", commit: secretfile.DurableCommitted, confirmed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, lease, real := selectedFixture(t)
			if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			files := NewMockidentityPinDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			injected := errors.New("injected storage failure")
			files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
				if tc.confirmed {
					if _, err := real.WriteMain(ctx, raw); err != nil {
						t.Fatal(err)
					}
				}
				if tc.cancel {
					cancel()
				}
				if tc.fail {
					return tc.commit, injected
				}
				return tc.commit, nil
			}).Once()
			_, err := store.confirmPinMetadata(ctx, files)
			if (err == nil) != tc.confirmed {
				t.Fatalf("commit=%v error=%v", tc.commit, err)
			}
			status := store.selectedPinStatus(lease)
			if status.confirmed != tc.confirmed || !status.allocated {
				t.Fatalf("unexpected acknowledgment/allocation: %+v", status)
			}
		})
	}
	t.Run("cancel-before-work", func(t *testing.T) {
		store, lease, _ := selectedFixture(t)
		if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
			t.Fatal(err)
		}
		files := NewMockidentityPinDurableFiles(t) // No filesystem method may be invoked.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := store.confirmPinMetadata(ctx, files); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if store.selectedPinStatus(lease).confirmed {
			t.Fatal("cancelled work was acknowledged")
		}
	})
}

func TestSelectedStrictCapturedRevisionAndDirtyGeneration(t *testing.T) {
	store, lease, real := selectedFixture(t)
	if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.pinMetadataSnapshot(lease.identity)
	if err != nil {
		t.Fatal(err)
	}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	var covered uint64
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		store.mu.Lock()
		covered = store.changes
		store.mu.Unlock()
		next := snapshot.metadata
		next.Selected.Reference = "cm:newer:revision"
		if _, err := store.stagePinMetadata(snapshot, next); err != nil {
			t.Fatal(err)
		}
		// The proof/endpoint mutation uses the same owner primitives while the
		// independent strict writer owns writeMu and has released mu.
		target, err := bindingTargetOf(dialledAt(domain.PeerAddress("127.0.0.1:39545")))
		if err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		if err := store.recordPinEvidenceLocked(lease.identity, store.now()); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
		store.bindTargetLocked(target, lease.identity, endpointProtectionNone(), store.now())
		store.changes++
		store.mu.Unlock()
		return real.WriteMain(ctx, raw)
	}).Once()
	ack, err := store.confirmPinMetadata(t.Context(), files)
	if err != nil {
		t.Fatal(err)
	}
	if ack.confirmed != 0 || store.selectedPinStatus(lease).confirmed {
		t.Fatal("older snapshot acknowledged newer metadata")
	}
	store.mu.Lock()
	dirty := store.changes > store.written
	written := store.written
	store.mu.Unlock()
	if !dirty || written != covered {
		t.Fatalf("later dirty generation erased: dirty=%v written=%d covered=%d", dirty, written, covered)
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if restarted.identities[lease.identity].classMetadata().Selected.Reference == "cm:newer:revision" {
		t.Fatal("fixture failed to write the older captured snapshot")
	}
	if _, err := store.confirmPinMetadata(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	if !store.selectedPinStatus(lease).confirmed {
		t.Fatal("retry did not confirm current revision")
	}
}

func TestSelectedStrictAcknowledgmentUsesCopyOnWrite(t *testing.T) {
	store, lease, files := selectedFixture(t)
	if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	old := store.identities[lease.identity].metadata
	store.mu.Unlock()
	if _, err := store.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	current := store.identities[lease.identity].metadata
	store.mu.Unlock()
	if old == current || old.confirmedRevision != 0 || current.confirmedRevision == 0 || old.value != current.value {
		t.Fatal("acknowledgment mutated a previously published metadata value")
	}
	if err := store.noteProvenInbound(lease.identity); err != nil {
		t.Fatal(err)
	}
	if !store.selectedPinStatus(lease).confirmed {
		t.Fatal("reproof revoked unchanged metadata ACK")
	}
}

func TestSelectedCancellationWhileWaitingForRemovalGateDoesNotStage(t *testing.T) {
	store, lease, _ := selectedFixture(t)
	store.mu.Lock()
	revision, changes := store.protectionRevision, store.changes
	store.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	producer := newSelectedPinProducer(selectedPinProducerOptions{service: &Service{secureSessions: &secureSessions{store: store}}, clock: time.Now})
	fact := selectedSessionFact{proof: &selectedSessionProof{binding: lease, group: domain.SourceGroupLoopback}, connID: 1, generation: 1}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	store.pinMutationGate.Lock()
	locked := true
	defer func() {
		if locked {
			store.pinMutationGate.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- producer.promote(ctx, []selectedSessionFact{fact}, files) }()
	// Observe the real call parked at the production RLock; elapsed sleeps
	// would permit cancellation before the tested race was reached.
	waitForConditionMsg(t, 5*time.Second, "promotion did not wait for removal gate", func() bool {
		raw := make([]byte, 1<<20)
		n := runtime.Stack(raw, true)
		for _, stack := range strings.Split(string(raw[:n]), "\n\n") {
			if strings.Contains(stack, "(*secureSessionStore).stageSelectedPin") && strings.Contains(stack, "sync.runtime_SemacquireRWMutexR") {
				return true
			}
		}
		return false
	})
	cancel()
	store.pinMutationGate.Unlock()
	locked = false
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.protectionRevision != revision || store.changes != changes || store.selectedAllocations != 0 || store.identities[lease.identity].metadata != nil {
		t.Fatal("cancelled gate waiter staged class metadata")
	}
}
