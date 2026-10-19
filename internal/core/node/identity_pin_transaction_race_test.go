package node

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func TestIdentityPinTransactionRejectsStaleProofBeforeFencing(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatal(err)
	}
	fence := NewMockIdentityPinRemovalFence(t) // No call may reach the broker.
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatalf("stale token %v", err)
	}
}

func TestIdentityPinTransactionDoesNotAbandonCleanupOnCallerCancellation(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	fence := NewMockIdentityPinRemovalFence(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "cancelled-request", Binding: binding, Generation: 2}
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Run(func(context.Context, domain.IdentityPinFenceBinding) { cancel() }).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(work context.Context, _ domain.IdentityPinReceipt) error {
		if work.Err() != nil {
			t.Error("request cancellation abandoned the begun transaction")
		}
		return nil
	}).Once()
	result, err := store.removeIdentityPin(ctx, binding, token, fence, files)
	if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
		t.Fatalf("cancelled begun transaction %v %v", result, err)
	}
}

func TestIdentityPinProofWaitsForRemovalAndRepinsNormally(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	fence := NewMockIdentityPinRemovalFence(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "concurrent-proof", Binding: binding, Generation: 2}
	entered, release := make(chan struct{}), make(chan struct{})
	fence.EXPECT().BeginRemoval(mock.Anything, binding).RunAndReturn(func(context.Context, domain.IdentityPinFenceBinding) (domain.IdentityPinRemovalPermit, error) {
		close(entered)
		<-release
		return permit, nil
	}).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	removed := make(chan error, 1)
	go func() {
		_, err := store.removeIdentityPin(context.Background(), binding, token, fence, files)
		removed <- err
	}()
	<-entered
	if store.pinMutationGate.TryRLock() {
		store.pinMutationGate.RUnlock()
		t.Fatal("transaction did not hold the proof publication gate")
	}
	proved := make(chan error, 1)
	go func() { proved <- store.noteProvenInbound(binding.Identity) }()
	close(release)
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	if err := <-proved; err != nil {
		t.Fatal(err)
	}
	if !store.identityPinned(binding.Identity) {
		t.Fatal("a later proof failed to pin normally")
	}
	if err := pinRemovalTokenError(store, binding.Identity, token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatal("remove/re-pin revived old preview")
	}
}

func TestIdentityPinRemovalPreservesDirtyEndpointChangesAndConcurrentFlush(t *testing.T) {
	store, binding, _, real := pinTransactionFixture(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "endpoint-flush", Binding: binding, Generation: 2}
	victim := domain.PeerAddress("192.0.2.30:64646")
	if _, err := store.noteProvenOutbound(dialledAt(victim), binding.Identity, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	// That new proof invalidates the original preview; confirm the current one.
	preview := pinRemovalPreview(t, store, binding.Identity)
	token := preview.Token
	store.mu.Lock()
	binding.ProofRevision = domain.IdentityPinProofRevision(store.identities[binding.Identity].evidenceRevision)
	store.mu.Unlock()
	permit.Binding = binding
	// Confirm only the current exact protection.
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	flushed, releaseFlush := make(chan []byte, 1), make(chan struct{})
	ordinaryWrite := store.writeFile
	store.writeFile = func(raw []byte) error {
		flushed <- append([]byte(nil), raw...)
		<-releaseFlush
		return ordinaryWrite(raw)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFlush) }) }
	defer release()
	flushDone := make(chan error, 1)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(real.Preflight).Once()
	files.EXPECT().ReadGuard(mock.Anything).RunAndReturn(real.ReadGuard).Once()
	files.EXPECT().WriteGuard(mock.Anything, mock.Anything).RunAndReturn(real.WriteGuard).Once()
	files.EXPECT().RemoveGuard(mock.Anything).RunAndReturn(real.RemoveGuard).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		// An endpoint mutation during I/O is outside this snapshot. Like expiry,
		// it happens under mu and waits for the periodic writer after publication.
		key := storedKeyOf(victim)
		store.mu.Lock()
		store.deleteBindingLocked(key)
		store.changes++
		store.renewalsUnwritten = true
		store.mu.Unlock()
		go func() { flushDone <- store.flushRenewalsNow() }()
		waitForConditionMsg(t, 5*time.Second, "flush did not publish dirty counters before waiting on writeMu", func() bool {
			store.mu.Lock()
			defer store.mu.Unlock()
			return store.lastFlush != nil
		})
		return real.WriteMain(ctx, raw)
	}).Once()
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); err != nil {
		t.Fatal(err)
	}
	var pending []byte
	select {
	case pending = <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent flush did not acquire the writer")
	}
	store.mu.Lock()
	dirty := store.changes > store.written
	store.mu.Unlock()
	if !dirty {
		t.Fatal("removal skipped dirty changes absent from its snapshot")
	}
	var file secureSessionFile
	if err := json.Unmarshal(pending, &file); err != nil {
		t.Fatal(err)
	}
	if _, found := file.Identities[binding.Identity]; found {
		t.Fatal("flush resurrected removed identity")
	}
	if _, found := file.Endpoints[victim]; found {
		t.Fatal("flush resurrected an endpoint mutation")
	}
	// Let the existing flush finish, then restore its write hook before teardown.
	release()
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
}
