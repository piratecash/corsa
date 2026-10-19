package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func TestIdentityPinFatalTransactionStopsLiveRunAndSurvivesFinalFlush(t *testing.T) {
	store, binding, token, real := pinTransactionFixture(t)
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{}, Type: domain.NodeTypeFull, AllowPrivatePeers: true})
	svc := NewService(cfg, id, nil)
	svc.secureSessions.store = store // Unpublished setup, before Run owns it.
	svc.connManager.config.MaxSlotsFn = func() int { return 0 }
	binding.Scope.Node = peerIdentityOf(t, svc)
	permit := domain.IdentityPinRemovalPermit{OperationID: "fatal-live-run", Binding: binding, Generation: 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running := runServiceForTest(ctx, svc)
	guard, release := context.WithTimeout(context.Background(), 5*time.Second)
	defer release()
	if err := running.AwaitReady(guard); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = running.Stop(guard); _ = running.DrainBackground(guard) }()
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(real.Preflight).Once()
	files.EXPECT().ReadGuard(mock.Anything).RunAndReturn(real.ReadGuard).Once()
	files.EXPECT().WriteGuard(mock.Anything, mock.Anything).RunAndReturn(real.WriteGuard).Once()
	injected := errors.New("unconfirmed main and protective rollback")
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, injected).Twice()
	// The normal final flush is successful; it must not erase the fatal outcome.
	store.mu.Lock()
	store.changes++
	store.mu.Unlock()
	result, err := svc.removeIdentityPinUsingFence(context.Background(), binding, token, fence, files)
	if result.Outcome != domain.IdentityPinRemovalRecoveryRequired || !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatalf("fatal result %v %v", result, err)
	}
	if err := running.Stop(guard); !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatalf("live Run exit lost fatal storage: %v", err)
	}
	if err := running.DrainBackground(guard); err != nil {
		t.Fatal(err)
	}
	if !store.identityPinned(binding.Identity) {
		t.Fatal("fatal teardown lost runtime pin")
	}
	if !errors.Is(svc.runCtx.Err(), context.Canceled) {
		t.Fatal("fatal storage did not stop networking lifecycle")
	}
}
