package service

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
)

// PublishGlobalMessage uses the embedded node's own identity and registered
// SQLite store. Global sender claims remain unsigned on the network.
func (c *DesktopClient) PublishGlobalMessage(ctx context.Context, command domain.OutgoingGlobalMessage) (domain.GlobalPublication, error) {
	if c == nil || c.rpc == nil || c.chatlog == nil || c.rpc.LocalNode() == nil || c.chatlog.Store() == nil {
		return domain.GlobalPublication{}, domain.ErrGlobalPublisherUnavailable
	}
	return c.rpc.LocalNode().PublishGlobalMessage(ctx, command)
}
