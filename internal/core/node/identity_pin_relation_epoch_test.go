package node

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

type relationEpochCheckedContext struct {
	context.Context
	armed   atomic.Bool
	once    sync.Once
	checked chan struct{}
}

func (c *relationEpochCheckedContext) Err() error {
	err := c.Context.Err()
	if c.armed.Load() {
		c.once.Do(func() { close(c.checked) })
	}
	return err
}

func TestRelationPinCancellationDuringEpochMutexWaitRemainsBeforeBoundary(t *testing.T) {
	store, binding, token, _ := pinTransactionFixture(t)
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &relationEpochCheckedContext{Context: base, checked: make(chan struct{})}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	var held atomic.Bool
	files.EXPECT().ReadGuard(mock.Anything).RunAndReturn(func(context.Context) ([]byte, error) {
		store.mu.Lock()
		held.Store(true)
		ctx.armed.Store(true)
		return nil, fs.ErrNotExist
	}).Once()
	fence := NewMockIdentityPinRemovalFence(t)
	var began atomic.Bool
	fence.EXPECT().BeginRemoval(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ domain.IdentityPinFenceBinding) (domain.IdentityPinRemovalPermit, error) {
		began.Store(true)
		return domain.IdentityPinRemovalPermit{}, ctx.Err()
	}).Maybe()
	done := make(chan error, 1)
	go func() { _, err := store.removeIdentityPin(ctx, binding, token, fence, files); done <- err }()
	select {
	case <-ctx.checked:
	case <-time.After(time.Second):
		cancel()
		if held.Swap(false) {
			store.mu.Unlock()
		}
		t.Fatal("pre-boundary context check was not observed")
	}
	// Real cancellation follows a successful Err read and precedes actual mu
	// acquisition. The next locked check must refuse without revoking absence.
	cancel()
	if !held.Swap(false) {
		t.Fatal("test did not hold the actual epoch mutex")
	}
	store.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("removal: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled removal stayed blocked")
	}
	store.mu.Lock()
	epoch := store.removalEpoch
	store.mu.Unlock()
	if epoch != 0 || began.Load() {
		t.Fatalf("canceled before boundary advanced epoch=%d began=%t", epoch, began.Load())
	}
}

func TestRelationPinRemovalEpochActualBoundary(t *testing.T) {
	for _, phase := range []string{"preflight", "token", "guard", "canceled", "begin_unknown", "permit_mismatch", "committed", "epoch_exhausted"} {
		t.Run(phase, func(t *testing.T) {
			store, binding, token, real := pinTransactionFixture(t)
			f := signedRelationFixture(t)
			before := captureRelationOwner(t, store, f.question.target)
			files := real
			fence := NewMockIdentityPinRemovalFence(t)
			ctx := t.Context()
			injected := errors.New("boundary failed")
			wantAdvance := false
			switch phase {
			case "preflight":
				broken := NewMockidentityPinDurableFiles(t)
				broken.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
				files = broken
			case "token":
				token = "invalid-preview"
			case "guard":
				broken := NewMockidentityPinDurableFiles(t)
				broken.EXPECT().Preflight(mock.Anything).Return(nil).Once()
				broken.EXPECT().ReadGuard(mock.Anything).Return([]byte("existing"), nil).Once()
				files = broken
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "epoch_exhausted":
				store.mu.Lock()
				store.removalEpoch = math.MaxUint64
				store.mu.Unlock()
				before = captureRelationOwner(t, store, f.question.target)
			case "begin_unknown":
				wantAdvance = true
				fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(domain.IdentityPinRemovalPermit{}, injected).Once()
			case "permit_mismatch":
				wantAdvance = true
				fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(domain.IdentityPinRemovalPermit{}, nil).Once()
			case "committed":
				wantAdvance = true
				permit := domain.IdentityPinRemovalPermit{OperationID: "epoch-commit", Binding: binding, Generation: 2}
				fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
				fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
			}
			_, err := store.removeIdentityPin(ctx, binding, token, fence, files)
			if phase == "committed" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("expected boundary refusal/error")
			}
			store.mu.Lock()
			same := before.matchesLocked(store)
			epoch := store.removalEpoch
			store.mu.Unlock()
			if same == wantAdvance {
				t.Fatalf("absent witness match=%t epoch=%d after %s", same, epoch, phase)
			}
			if phase == "epoch_exhausted" && !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
				t.Fatal(err)
			}
			if phase == "begin_unknown" && !errors.Is(err, injected) {
				t.Fatal(err)
			}
			// Unknown/mismatch may not mutate the old pin, but must still revoke
			// pre-boundary absence authority. The same actual record must not rebind.
			if wantAdvance {
				if _, err := store.stageRelationRecord(t.Context(), before, f.verified(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
					t.Fatalf("destructive boundary did not stale old record: %v", err)
				}
			}
		})
	}
}

func TestRelationPinRecordOnlyEntryCanUseExistingRemovalTransaction(t *testing.T) {
	store, template, _, files := pinTransactionFixture(t)
	f := signedRelationFixture(t)
	stage, err := store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, f.question.target), f.verified(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, f.question.target)
	store.mu.Lock()
	entry := store.identities[f.question.target]
	store.mu.Unlock()
	backup, err := domain.NewIdentityPinBackup(f.question.target, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.IdentityPinFenceBinding{Scope: template.Scope, RuntimeEpoch: template.RuntimeEpoch, Generation: 1, Identity: f.question.target, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt, Backup: backup}
	permit := domain.IdentityPinRemovalPermit{OperationID: "record-without-session", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	result, err := store.removeIdentityPin(t.Context(), binding, preview.Token, fence, files)
	if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
		t.Fatalf("record-only entry was incompatible with C3: %v %v", result, err)
	}
	if stage.binding.matchesForTest(store) || store.identityPinned(f.question.target) {
		t.Fatal("old publication binding survived C3")
	}
	if stats := store.stats(); stats.RelationAllocations != 1 || stats.RelationConfirmed != 0 {
		t.Fatalf("C3 lost reserved Relation allocation: %+v", stats)
	}
}
