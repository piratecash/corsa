package loadstand

import (
	"context"
	"net"
	"time"
)

type linuxRPCDialFunc func(context.Context, string, string) (net.Conn, error)

// HTTP Transport deliberately detaches request deadlines from new dials. Keep
// the strict namespace owner finite using the SAME one-second bound as the
// existing RPCClient default dialer. This is not a measurement/profile policy.
func boundedLinuxRPCDial(dial linuxRPCDialFunc) linuxRPCDialFunc {
	return func(parent context.Context, network, address string) (net.Conn, error) {
		if err := parent.Err(); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(parent, time.Second)
		defer cancel()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return dial(ctx, network, address) // successful transfer owns its Conn
	}
}
