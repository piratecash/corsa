package sdk

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/stretchr/testify/mock"
)

func TestIdentityPinSDKUsesSeparateInProcessPreviewAndConfirmation(t *testing.T) {
	t.Parallel()
	provider := rpcmocks.NewMockIdentityPinProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterIdentityPinCommands(table, provider)
	runtime := &Runtime{cmdTable: table, commands: context.Background()}
	id := domain.PeerIdentity{7}
	token := IdentityPinConfirmationToken(strings.Repeat("cd", 32))
	provider.EXPECT().PreviewIdentityPinRemoval(mock.Anything, id).Return(domain.IdentityPinRemovalPreview{Identity: id, Consequence: domain.IdentityPinRemovalConsequence, Token: token, RemovalAvailable: false}, nil).Once()
	provider.EXPECT().ConfirmIdentityPinRemoval(mock.Anything, id, token).Return(domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinRemovalUnavailable).Once()
	preview, err := runtime.PreviewIdentityPinRemoval(context.Background(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	if preview.Identity != id.String() || preview.ConfirmToken != token || preview.Consequence != domain.IdentityPinRemovalConsequence || preview.RemovalAvailable {
		t.Fatalf("wrong preview: %+v", preview)
	}
	if _, err := runtime.ConfirmIdentityPinRemoval(context.Background(), preview.Identity, preview.ConfirmToken); !errors.Is(err, domain.ErrIdentityPinRemovalUnavailable) {
		t.Fatalf("unfenced confirm: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.PreviewIdentityPinRemoval(canceled, id.String()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preview: %v", err)
	}
	runtime.lifecycle.Lock()
	runtime.stopping = true
	runtime.lifecycle.Unlock()
	if _, err := runtime.PreviewIdentityPinRemoval(context.Background(), id.String()); !errors.Is(err, errClosed) {
		t.Fatalf("closed preview: %v", err)
	}
}
