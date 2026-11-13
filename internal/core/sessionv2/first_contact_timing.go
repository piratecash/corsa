//go:build s6_first_contact_probe

package sessionv2

import (
	"context"
	"net"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// DialWithTiming reports elapsed durations for the phases that started.
// The callback must return immediately and must not record the error text.
func DialWithTiming(ctx context.Context, wire net.Conn, local Local, intro protocol.Frame, expect ExpectedPeer, timeouts Timeouts, record func(string, time.Duration, error)) (ProvenSession, error) {
	return Dial(ctx, wire, local, intro, expect, timeouts, withHooks(handshakeHooks{timing: record}))
}
