//go:build !s6_first_contact_probe

package node

import (
	"context"
	"net"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func dialSessionV2(ctx context.Context, _ domain.ConnID, wire net.Conn, local sessionv2.Local, intro protocol.Frame, expect sessionv2.ExpectedPeer, timeouts sessionv2.Timeouts) (sessionv2.ProvenSession, error) {
	return sessionv2.Dial(ctx, wire, local, intro, expect, timeouts)
}
