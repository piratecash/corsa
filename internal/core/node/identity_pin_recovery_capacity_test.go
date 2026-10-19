package node

import (
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/stretchr/testify/mock"
	"path/filepath"
	"testing"
)

func TestIdentityPinPendingRemovalReservesRoomForProtectiveRecovery(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	store.mu.Lock()
	for i := uint32(1); len(store.identities) < maxPinnedIdentities; i++ {
		id := domain.PeerIdentity{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		if _, found := store.identities[id]; found {
			continue
		}
		if err := store.recordPinEvidenceLocked(id, binding.PreviousPinnedAt); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
	}
	store.changes++
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	permit := domain.IdentityPinRemovalPermit{OperationID: "pending-at-capacity", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); err != nil {
		t.Fatal(err)
	}
	extra := domain.PeerIdentity{250, 250, 250, 250}
	if err := store.noteProvenInbound(extra); err != nil {
		if !errors.Is(err, errPinStoreFull) {
			t.Fatal(err)
		}
		return // A pending recovery reservation correctly refuses the newcomer.
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	recoveryFiles, err := restarted.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recoveryFiles.Close() }()
	recoveryFence := NewMockIdentityPinRemovalFence(t)
	recoveryFence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := restarted.recoverIdentityPinProtection(context.Background(), permit, recoveryFence, recoveryFiles); err != nil {
		t.Fatal(err)
	}
	restarted.mu.Lock()
	count := len(restarted.identities)
	restarted.mu.Unlock()
	if count > maxPinnedIdentities {
		t.Fatalf("late proof filled pending removal's recovery place: %d pins exceed hard total %d", count, maxPinnedIdentities)
	}
}

func TestIdentityPinPendingReservationAllowsOwnReproofAndReportsFull(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	store.mu.Lock()
	for i := uint32(1); len(store.identities) < maxPinnedIdentities; i++ {
		id := domain.PeerIdentity{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		if _, found := store.identities[id]; found {
			continue
		}
		if err := store.recordPinEvidenceLocked(id, binding.PreviousPinnedAt); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
	}
	store.changes++
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	permit := domain.IdentityPinRemovalPermit{OperationID: "reserved-own-reproof", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); err != nil {
		t.Fatal(err)
	}
	if stats := store.stats(); stats.PinnedIdentities != maxPinnedIdentities-1 || !stats.Full || !store.pinStoreFull() {
		t.Fatalf("reserved place reported as free: %+v", stats)
	}
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatalf("own reproof could not consume restoration reservation: %v", err)
	}
	if stats := store.stats(); stats.PinnedIdentities != maxPinnedIdentities || !stats.Full {
		t.Fatalf("own reproof exceeded or lost capacity: %+v", stats)
	}
	if err := store.noteProvenInbound(domain.PeerIdentity{250, 250, 250, 250}); !errors.Is(err, errPinStoreFull) {
		t.Fatalf("other identity consumed occupied reservation: %v", err)
	}
}
