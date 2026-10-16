package node

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// legacy_peer_emulation_test.go emulates a released build that speaks only
// v1 — ProtocolVersion 28, 29 or 30 — in front of a test node of this build.
//
// The old peer is a real Service turned into a v1-only node (asOldNode): it
// never dials TLS, and its listener serves every connection as v1 — on a
// ClientHello it answers the first line it reads (the ClientHello always
// carries a 0x0a byte: the supported_groups extension type) with
// {"type":"error","code":"invalid-json"} and closes the connection without
// charging anything, exactly as handleCommand did in every release from
// v1.0.64 to v2.4.70. What a v1-only Service of this build cannot do is
// announce an older version: hello and welcome carry config.ProtocolVersion.
// legacyVersionProxy stands between the two nodes and rewrites the version of
// the hello or welcome the old node sends, so the node under test sees the
// version a released build would announce and applies every gate that
// depends on it.
//
// What the emulation does NOT reproduce: the rest of the old node's
// behaviour is this build's — the frames it emits, the features it uses
// toward a peer of version 31, its own gates. The tests built on it check
// what the node under test does with an old peer, not what an old binary
// does with it.

// oldPeerSide says on which side of a legacyVersionProxy the old node stands:
// its frames are the ones rewritten.
type oldPeerSide int

const (
	// oldPeerListens: the old node is the proxy's target; the node under test
	// dials the proxy.
	oldPeerListens oldPeerSide = iota + 1
	// oldPeerDials: the old node dials the proxy; the node under test is the
	// target.
	oldPeerDials
)

// legacyVersionProxy forwards TCP connections to target, rewriting the
// version announced in the hello and welcome lines the old node sends.
type legacyVersionProxy struct {
	listener net.Listener
	target   string
	side     oldPeerSide
	version  int
	// victimAddress, when set, is written into the address field of every
	// hello and welcome the old node sends — the man-in-the-middle move of
	// A1: claiming to be a peer the node under test knows over v2. The old
	// node's own pubkey/boxkey/signature are left as they are; the node
	// under test refuses on the identity before it ever checks them.
	victimAddress string
	// victim, when set, replaces the whole identity of every hello and
	// welcome the old node sends — address, pubkey, box key and its
	// signature, all public — so the relabelled frame is self-consistent and
	// passes every check a v1 frame can be given without a proof.
	victim *identity.Identity
	// verbatim forwards the old node's frames untouched: the old node is a
	// released binary that announces its own version (startRecordingProxy in
	// release_interop_test.go, build tag interop).
	verbatim bool

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

// startLegacyVersionProxy listens on a loopback port and forwards every
// connection to target. version is what the old node's hello or welcome
// announces once rewritten.
func startLegacyVersionProxy(t *testing.T, target string, side oldPeerSide, version int) *legacyVersionProxy {
	t.Helper()
	return launchLegacyProxy(t, &legacyVersionProxy{target: target, side: side, version: version})
}

// startImpersonatingProxy is startLegacyVersionProxy that also relabels the
// old node's hello/welcome as coming from victim.
func startImpersonatingProxy(t *testing.T, target string, side oldPeerSide, version int, victim domain.PeerIdentity) *legacyVersionProxy {
	t.Helper()
	return launchLegacyProxy(t, &legacyVersionProxy{target: target, side: side, version: version, victimAddress: victim.String()})
}

// startIdentitySwappingProxy is startLegacyVersionProxy that also relabels
// the old node's hello/welcome with victim's whole public identity.
func startIdentitySwappingProxy(t *testing.T, target string, side oldPeerSide, version int, victim *identity.Identity) *legacyVersionProxy {
	t.Helper()
	return launchLegacyProxy(t, &legacyVersionProxy{target: target, side: side, version: version, victim: victim})
}

// launchLegacyProxy starts a fully configured proxy. Every field is set before
// the accept loop starts: a field written after it would race with the copy
// goroutines that read it.
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

var (
	legacyAddressField = regexp.MustCompile(`"address":"[^"]*"`)
	legacyPubKeyField  = regexp.MustCompile(`"pubkey":"[^"]*"`)
	legacyBoxKeyField  = regexp.MustCompile(`"boxkey":"[^"]*"`)
	legacyBoxSigField  = regexp.MustCompile(`"boxsig":"[^"]*"`)
)

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
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		if p.stopped {
			p.mu.Unlock()
			_ = client.Close()
			_ = server.Close()
			continue
		}
		p.conns = append(p.conns, client, server)
		p.done.Add(2)
		p.mu.Unlock()
		go p.forwardFromDialer(client, server)
		go p.forwardFromListener(server, client)
	}
}

// errProxyWrite marks a copy that ended because the destination refused a
// write, as opposed to the source ending its stream.
var errProxyWrite = errors.New("proxy: write to the destination failed")

// forwardFromDialer carries what the dialling side sends. When the dialler is
// the old node (oldPeerDials) its frames are rewritten; when it is the node
// under test (oldPeerListens) its frame types are recorded.
//
// It never closes the dialler's connection: only forwardFromListener does,
// once the listener ended. An old listener answers the first line of a
// ClientHello and closes while the dialler is still writing the rest of it,
// so the write toward the listener fails before the answer has crossed the
// proxy; closing the dialler here would hand it a FIN in place of the answer
// the old node did send.
func (p *legacyVersionProxy) forwardFromDialer(from, to net.Conn) {
	defer p.done.Done()
	err := p.copy(to, from, p.side == oldPeerDials, p.side == oldPeerListens)
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

// forwardFromListener carries what the listening side sends. When the listener
// is the old node (oldPeerListens) its frames are rewritten; when it is the
// node under test (oldPeerDials) its frame types are recorded. It reads the
// listener to its end before closing both connections, so everything the
// listener wrote reaches the dialler first.
func (p *legacyVersionProxy) forwardFromListener(from, to net.Conn) {
	defer p.done.Done()
	err := p.copy(to, from, p.side == oldPeerListens, p.side == oldPeerDials)
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

// copy forwards bytes line by line. When rewrite is set, the version (and, for
// an impersonating proxy, the address) of a hello/welcome is replaced; nothing
// else is touched and v1 hello/welcome are not signed. When record is set, the
// frame type of each JSON line is remembered — this is how a test reads what
// the node under test actually put on the wire. A failed write is returned
// wrapped in errProxyWrite; a read that ended returns the read error.
func (p *legacyVersionProxy) copy(dst io.Writer, src io.Reader, rewrite, record bool) error {
	reader := bufio.NewReader(src)
	opening := true
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if record && opening {
				p.recordOpening(line)
			}
			opening = false
			out := line
			if rewrite {
				out = p.announceOldVersion(line)
			}
			if record {
				p.recordFrameType(line)
			}
			if _, werr := dst.Write(out); werr != nil {
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

// clientHellos is how many connections the node under test opened with a
// TLS ClientHello so far.
func (p *legacyVersionProxy) clientHellos() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nodeClientHellos
}

// nodeHellos reports, per hello the node under test sent so far, whether it
// carried an identity.
func (p *legacyVersionProxy) nodeHellos() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.nodeHelloIdentified...)
}

// connections is how many connections the proxy has forwarded.
func (p *legacyVersionProxy) connections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns) / 2
}

// nodeSentFrame reports whether the node under test sent a frame of this type.
func (p *legacyVersionProxy) nodeSentFrame(frameType string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.nodeFrameTypes {
		if t == frameType {
			return true
		}
	}
	return false
}

func (p *legacyVersionProxy) announceOldVersion(line []byte) []byte {
	if p.verbatim {
		return line
	}
	var head struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(line, &head) != nil || (head.Type != "hello" && head.Type != "welcome") {
		return line
	}
	current := []byte(fmt.Sprintf(`"version":%d`, config.ProtocolVersion))
	line = bytes.Replace(line, current, []byte(fmt.Sprintf(`"version":%d`, p.version)), 1)
	if p.victimAddress != "" {
		line = legacyAddressField.ReplaceAll(line, []byte(fmt.Sprintf(`"address":%q`, p.victimAddress)))
	}
	if p.victim != nil {
		line = legacyAddressField.ReplaceAll(line, []byte(fmt.Sprintf(`"address":%q`, p.victim.Address)))
		line = legacyPubKeyField.ReplaceAll(line, []byte(fmt.Sprintf(`"pubkey":%q`, identity.PublicKeyBase64(p.victim.PublicKey))))
		line = legacyBoxKeyField.ReplaceAll(line, []byte(fmt.Sprintf(`"boxkey":%q`, identity.BoxPublicKeyBase64(p.victim.BoxPublicKey))))
		line = legacyBoxSigField.ReplaceAll(line, []byte(fmt.Sprintf(`"boxsig":%q`, identity.SignBoxKeyBinding(p.victim))))
	}
	return line
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

// releasedLegacyVersions are the versions of the released builds that speak
// only v1 and that this build still serves: v28 (v1.0.64 … v2.1.67), v29
// (v2.2.68), v30 (v2.3.69, v2.4.70).
func releasedLegacyVersions() []int {
	versions := make([]int, 0, config.ProtocolVersionSecureSession-config.MinimumProtocolVersion)
	for version := config.MinimumProtocolVersion; version < config.ProtocolVersionSecureSession; version++ {
		versions = append(versions, version)
	}
	return versions
}
