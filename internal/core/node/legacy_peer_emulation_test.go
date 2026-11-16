//go:build interop

package node

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// oldPeerSide identifies the released binary's side of a recording proxy.
type oldPeerSide int

const (
	// oldPeerListens: the old node is the proxy's target; the node under test
	// dials the proxy.
	oldPeerListens oldPeerSide = iota + 1
	// oldPeerDials: the old node dials the proxy; the node under test is the
	// target.
	oldPeerDials
)

// legacyVersionProxy forwards TCP bytes without rewriting the release's frames.
type legacyVersionProxy struct {
	listener net.Listener
	target   string
	side     oldPeerSide

	mu             sync.Mutex
	conns          []net.Conn
	nodeFrameTypes []string
	// nodeHelloIdentified says, per hello the node under test sent, whether
	// it carried an identity: the probe does not, the full hello does.
	nodeHelloIdentified []bool
	// nodeClientHellos is how many connections the node under test opened
	// with a TLS ClientHello rather than a v1 line.
	nodeClientHellos int
	// stopped is set by close: a connection accepted just before the
	// listener closed is then closed at once instead of escaping the
	// close that already ran over conns.
	stopped bool
	done    sync.WaitGroup
}

func launchLegacyProxy(t *testing.T, proxy *legacyVersionProxy) *legacyVersionProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	proxy.listener = listener
	proxy.done.Add(1)
	go proxy.acceptLoop()
	t.Cleanup(proxy.close)
	return proxy
}

func (p *legacyVersionProxy) address() string { return p.listener.Addr().String() }

func (p *legacyVersionProxy) peerAddress() domain.PeerAddress {
	return domain.PeerAddress(p.address())
}

func (p *legacyVersionProxy) acceptLoop() {
	defer p.done.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.relay(client)
	}
}

// relay forwards client to the target.
func (p *legacyVersionProxy) relay(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		_ = client.Close()
		_ = server.Close()
		return
	}
	p.conns = append(p.conns, client, server)
	p.done.Add(2)
	p.mu.Unlock()
	go p.forwardFromDialer(client, server)
	go p.forwardFromListener(server, client)
}

var errProxyWrite = errors.New("proxy: write to the destination failed")

// forwardFromDialer carries what the dialling side sends. When the dialler is
// the node under test (oldPeerListens) its frame types are recorded.
//
// It never closes the dialler's connection: only forwardFromListener does,
// once the listener ended. An old listener answers the first line of a
// ClientHello and closes while the dialler is still writing the rest of it,
// so the write toward the listener fails before the answer has crossed the
// proxy; closing the dialler here would hand it a FIN in place of the answer
// the old node did send.
func (p *legacyVersionProxy) forwardFromDialer(from, to net.Conn) {
	defer p.done.Done()
	err := p.copy(to, from, p.side == oldPeerListens)
	switch {
	case errors.Is(err, errProxyWrite):
		// The listener stopped reading. What the dialler still writes is
		// consumed, so the close forwardFromListener makes is decided by
		// the listener's own end and not by bytes left unread in the proxy.
		_, _ = io.Copy(io.Discard, from)
	case errors.Is(err, syscall.ECONNRESET):
		resetConn(to)
	default:
		// The dialler finished writing; the listener sees the EOF and may
		// still answer.
		if tcp, ok := to.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}
}

// forwardFromListener records frames if the listener is the node under test.
// It reads the listener to its end before closing either connection.
func (p *legacyVersionProxy) forwardFromListener(from, to net.Conn) {
	defer p.done.Done()
	err := p.copy(to, from, p.side == oldPeerDials)
	if errors.Is(err, errProxyWrite) {
		// The dialler is gone; the listener's end is still waited for, so
		// the close below mirrors how the listener ended.
		_, err = io.Copy(io.Discard, from)
	}
	closeLikeThePeer(to, from, err)
}

// closeLikeThePeer ends both connections once the listener ended. A listener
// that reset its connection is answered with a reset, not a FIN: an old
// listener closes a ClientHello connection with the unread rest of the
// ClientHello in its buffer, which resets it, and the dialler must see what it
// would see without the proxy.
func closeLikeThePeer(to, from net.Conn, err error) {
	if errors.Is(err, syscall.ECONNRESET) {
		resetConn(to)
	} else {
		_ = to.Close()
	}
	_ = from.Close()
}

// resetConn closes conn with a reset instead of a FIN.
func resetConn(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

// copy forwards bytes in order and records frame types sent by the node under test.
func (p *legacyVersionProxy) copy(dst io.Writer, src io.Reader, record bool) error {
	reader := bufio.NewReader(src)
	opening := true
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if record && opening {
				p.recordOpening(line)
			}
			opening = false
			if record {
				p.recordFrameType(line)
			}
			if _, werr := dst.Write(line); werr != nil {
				return fmt.Errorf("%w: %w", errProxyWrite, werr)
			}
		}
		if err != nil {
			return err
		}
	}
}

// recordFrameType remembers the type of a JSON frame the node under test sent.
// Non-JSON bytes (a TLS ClientHello) carry no type and are ignored.
func (p *legacyVersionProxy) recordFrameType(line []byte) {
	var head struct {
		Type    string `json:"type"`
		Address string `json:"address"`
	}
	if json.Unmarshal(line, &head) != nil || head.Type == "" {
		return
	}
	p.mu.Lock()
	p.nodeFrameTypes = append(p.nodeFrameTypes, head.Type)
	if head.Type == "hello" {
		p.nodeHelloIdentified = append(p.nodeHelloIdentified, head.Address != "")
	}
	p.mu.Unlock()
}

// recordOpening remembers whether the node under test opened a connection
// with a TLS ClientHello: its first byte is a handshake record, never JSON.
func (p *legacyVersionProxy) recordOpening(line []byte) {
	if line[0] != sessionv2.TLSRecordHandshake {
		return
	}
	p.mu.Lock()
	p.nodeClientHellos++
	p.mu.Unlock()
}

// drain stops accepting and waits until every forwarded connection has ended
// on its own, i.e. both nodes closed it. Only then has every frame the node
// under test wrote been read and recorded: closing the connections first
// would drop the frames still in the socket buffer, and a check that the node
// did NOT send a frame would pass on a frame the proxy never got to read.
func (p *legacyVersionProxy) drain(t *testing.T) {
	t.Helper()
	_ = p.listener.Close()
	finished := make(chan struct{})
	go func() {
		p.done.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the proxied connections did not end within 10s: a node kept its side open")
	}
}

func (p *legacyVersionProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.stopped = true
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	p.done.Wait()
}
