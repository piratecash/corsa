package node

import (
	"bufio"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/netcore"
)

// inboundTeardownBound bounds every wait on a running node in these tests.
const inboundTeardownBound = 10 * time.Second

// TestAServerEndedInboundSessionClosesItsSocket pins, end to end on a running
// node, that the handler's teardown closes the socket of a session the SERVER
// ended. The only close left on that path is the one unregisterInboundConn
// performs through Network().Close on the core, which wraps the TLS connection.
// Its Close must still reach the underlying socket. A malformed frame makes
// the server answer with an error and end the handler. The client reads the
// answer and the end of the stream, and teardown releases the registry entry
// and stops the writer.
func TestAServerEndedInboundSessionClosesItsSocket(t *testing.T) {
	t.Parallel()

	svc, address, stop := listenerNode(t, nil)
	t.Cleanup(stop)
	client := dialTestTLSClient(t, address)
	t.Cleanup(func() { _ = client.Close() })

	var core *netcore.NetCore
	waitFor(t, inboundTeardownBound, "the inbound conn registered", func() bool {
		core = soleInboundCore(svc)
		return core != nil
	})
	if _, metered := core.Conn().(*netcore.MeteredConn); metered {
		t.Fatal("fixture: expected the core to wrap the protected session connection")
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
