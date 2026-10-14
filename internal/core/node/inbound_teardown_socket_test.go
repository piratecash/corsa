package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// inboundTeardownBound bounds every wait on a running node in these tests.
const inboundTeardownBound = 10 * time.Second

// TestAServerEndedInboundSessionClosesItsSocket pins, end to end on a running
// node, that the handler's teardown closes the socket of a session the SERVER
// ended. The only close left on that path is the one unregisterInboundConn
// performs through Network().Close on the core, and the core is built on the
// session's wire: the metered socket for v1, but the v2 session's TLS
// connection for v2 — a different net.Conn, whose Close must still reach the
// peer. A malformed frame makes the server answer with an error and end the
// handler; the client must read that answer and then the end of the stream,
// and the registry must be left with no entry and no live writer.
func TestAServerEndedInboundSessionClosesItsSocket(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		dial func(t *testing.T, address string) net.Conn
		// wireIsMetered is what the core's conn must be, so each case is
		// known to exercise the wire it names.
		wireIsMetered bool
	}{
		{name: "v1", dial: dialV1Inbound, wireIsMetered: true},
		{name: "v2", dial: dialV2Inbound, wireIsMetered: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc, address, stop := listenerNode(t, nil)
			t.Cleanup(stop)
			client := tc.dial(t, address)
			t.Cleanup(func() { _ = client.Close() })

			var core *netcore.NetCore
			waitFor(t, inboundTeardownBound, "the inbound conn registered", func() bool {
				core = soleInboundCore(svc)
				return core != nil
			})
			if _, metered := core.Conn().(*netcore.MeteredConn); metered != tc.wireIsMetered {
				t.Fatalf("fixture: the core's conn is %T, want metered=%v", core.Conn(), tc.wireIsMetered)
			}

			if _, err := client.Write([]byte("garbage\n")); err != nil {
				t.Fatalf("write the malformed frame: %v", err)
			}
			lines, err := readUntilStreamEnds(client)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("the server never closed the socket after ending the session (%d lines read)", lines)
			}
			if lines == 0 {
				t.Fatalf("the stream ended (%v) without the server's answer to the malformed frame", err)
			}
			waitFor(t, inboundTeardownBound, "the registry released the conn", func() bool {
				return soleInboundCore(svc) == nil
			})
			if !writerReturned(core) {
				t.Fatal("the handler's teardown left the NetCore writer alive")
			}
		})
	}
}

// readUntilStreamEnds reads lines until the read fails and returns how many it
// read and the error that ended the stream; os.ErrDeadlineExceeded means the
// server never closed it.
func readUntilStreamEnds(c net.Conn) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(inboundTeardownBound)); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(c)
	lines := 0
	for {
		if _, err := reader.ReadString('\n'); err != nil {
			return lines, err
		}
		lines++
	}
}

// dialV1Inbound opens a v1 session: a first byte that is not a TLS record.
func dialV1Inbound(t *testing.T, address string) net.Conn {
	t.Helper()
	conn := dialWhenListening(t, address)
	if _, err := conn.Write([]byte("{")); err != nil {
		_ = conn.Close()
		t.Fatalf("write the v1 first byte: %v", err)
	}
	return conn
}

// dialV2Inbound completes a v2 handshake with keys of a throwaway node and
// returns the session's connection.
func dialV2Inbound(t *testing.T, address string) net.Conn {
	t.Helper()
	dialer := newTestService(t, config.NodeTypeFull)
	if dialer.secureSessions == nil || dialer.sessionMode() == sessionv2.ModeLegacyOnly {
		t.Fatal("fixture: a default node does not speak v2, so the v2 case would prove nothing")
	}
	raw := dialWhenListening(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), inboundTeardownBound)
	defer cancel()
	session, err := sessionv2.Dial(ctx, raw, dialer.secureSessions.local, dialer.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("v2 dial: %v", err)
	}
	conn, err := session.Conn()
	if err != nil {
		_ = raw.Close()
		t.Fatalf("v2 session conn: %v", err)
	}
	return conn
}
