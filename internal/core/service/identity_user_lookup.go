package service

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
)

// ResolveUserIdentity is the typed Window action port. Generic ResolveIdentity
// and generic RPC retain their ordinary non-authoritative lookup behavior.
func (r *DMRouter) ResolveUserIdentity(ctx context.Context, target domain.PeerIdentity) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !r.beginOp() {
		return "", ErrRouterShuttingDown
	}
	defer r.endOp()
	if r.client == nil {
		return "", errors.New("identity lookup: embedded node unavailable")
	}
	action, err := r.client.StartUserIdentityLookup(ctx, target)
	if err != nil {
		return "", err
	}
	return action.ResolutionID(), nil
}

// StartUserIdentityLookup reaches only the embedded node's typed Window port.
func (c *DesktopClient) StartUserIdentityLookup(ctx context.Context, target domain.PeerIdentity) (node.UserIdentityLookup, error) {
	if c == nil || c.rpc == nil || c.rpc.LocalNode() == nil {
		return node.UserIdentityLookup{}, errors.New("identity lookup: embedded node unavailable")
	}
	return c.rpc.LocalNode().StartUserIdentityLookup(ctx, target)
}
