package node

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// IdentityPinRemovalFence is a business port, not an admission capability.
// An implementation or mock alone can never enable public removal. The real
// adapter must independently verify external enrollment and supervised startup.
type IdentityPinRemovalFence interface {
	BeginRemoval(context.Context, domain.IdentityPinFenceBinding) (domain.IdentityPinRemovalPermit, error)
	RecordReceipt(context.Context, domain.IdentityPinReceipt) error
}

// identityPinDurableFiles is one pinned directory and its two bounded artifacts.
// Every operation resolves through that handle, never a reopened pathname.
type identityPinDurableFiles interface {
	Preflight(context.Context) error
	ReadGuard(context.Context) ([]byte, error)
	WriteGuard(context.Context, []byte) (secretfile.DurableCommit, error)
	WriteMain(context.Context, []byte) (secretfile.DurableCommit, error)
	RemoveGuard(context.Context) (secretfile.DurableCommit, error)
	Close() error
}
