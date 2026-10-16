package node

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// notice_dial_proxy_test.go covers the notice fallback's socket: it is opened
// by the same proxy-aware, budgeted dial as every other auxiliary path. An
// overlay address goes through the SOCKS5 proxy with its name handed to the
// proxy, a proxy that cannot be reached ends the attempt without a direct
// connection, the v2/pin policy of the session dial holds behind the proxy,
// and a cancelled dial gives back its socket and its unit of the budget.

// socksConnect is one CONNECT request the fake proxy read.
type socksConnect struct {
	addressType byte
	host        string
	port        int
}

// fakeSOCKS5 is a SOCKS5 proxy for tests. It records every CONNECT and, when
// it has a target, forwards the connection there whatever name was asked
// for; without one it reads the greeting and never answers, the way a stuck
// proxy does.
type fakeSOCKS5 struct {
	listener net.Listener
	target   string

	mu       sync.Mutex
	connects []socksConnect
	accepted chan struct{}
	released chan struct{}
	wg       sync.WaitGroup
}

// startForwardingSOCKS5 is a proxy that sends every CONNECT to target.
func startForwardingSOCKS5(t *testing.T, target domain.PeerAddress) *fakeSOCKS5 {
	t.Helper()
	return startFakeSOCKS5(t, string(target))
}

// startStalledSOCKS5 is a proxy that accepts and then never answers.
func startStalledSOCKS5(t *testing.T) *fakeSOCKS5 {
	t.Helper()
	return startFakeSOCKS5(t, "")
}

func startFakeSOCKS5(t *testing.T, target string) *fakeSOCKS5 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	proxy := &fakeSOCKS5{
		listener: listener,
		target:   target,
		accepted: make(chan struct{}, 16),
		released: make(chan struct{}, 16),
	}
	proxy.wg.Add(1)
	go proxy.acceptLoop()
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.wg.Wait()
	})
	return proxy
}

func (p *fakeSOCKS5) address() string { return p.listener.Addr().String() }

func (p *fakeSOCKS5) acceptLoop() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.serve(client)
		}()
	}
}

func (p *fakeSOCKS5) serve(client net.Conn) {
	defer func() { _ = client.Close() }()
	_ = client.SetDeadline(time.Now().Add(10 * time.Second))
	p.accepted <- struct{}{}
	if p.target == "" {
		// Stalled: the client must give up on its own. A read that ends
		// before the deadline is the client closing its socket.
		_, _ = io.Copy(io.Discard, client)
		p.released <- struct{}{}
		return
	}
	connect, ok := readSOCKS5Connect(client)
	if !ok {
		return
	}
	p.mu.Lock()
	p.connects = append(p.connects, connect)
	p.mu.Unlock()
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		_, _ = client.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer func() { _ = upstream.Close() }()
	if _, err := client.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0}); err != nil {
		return
	}
	_ = client.SetDeadline(time.Time{})
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(upstream, client)
		_ = upstream.Close()
		close(done)
	}()
	_, _ = io.Copy(client, upstream)
	_ = client.Close()
	<-done
}

// readSOCKS5Connect answers the no-auth greeting and reads one CONNECT.
func readSOCKS5Connect(client net.Conn) (socksConnect, bool) {
	var greeting [3]byte
	if _, err := io.ReadFull(client, greeting[:]); err != nil {
		return socksConnect{}, false
	}
	if _, err := client.Write([]byte{0x05, 0x00}); err != nil {
		return socksConnect{}, false
	}
	var header [4]byte
	if _, err := io.ReadFull(client, header[:]); err != nil {
		return socksConnect{}, false
	}
	connect := socksConnect{addressType: header[3]}
	var hostLength int
	switch header[3] {
	case 0x01:
		hostLength = 4
	case 0x04:
		hostLength = 16
	case 0x03:
		var length [1]byte
		if _, err := io.ReadFull(client, length[:]); err != nil {
			return socksConnect{}, false
		}
		hostLength = int(length[0])
	default:
		return socksConnect{}, false
	}
	hostAndPort := make([]byte, hostLength+2)
	if _, err := io.ReadFull(client, hostAndPort); err != nil {
		return socksConnect{}, false
	}
	connect.host = string(hostAndPort[:hostLength])
	connect.port = int(binary.BigEndian.Uint16(hostAndPort[hostLength:]))
	return connect, true
}

func (p *fakeSOCKS5) connectRequests() []socksConnect {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]socksConnect(nil), p.connects...)
}

// awaitSignal waits for one signal on ch, failing the test after timeout.
func awaitSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s did not happen within %s", what, timeout)
	}
}

// closedLoopbackAddress is an address nothing listens on.
func closedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

// newProxiedNoticeService is a full node that reaches overlay addresses
// through proxyAddress, with short session dial timeouts.
func newProxiedNoticeService(t *testing.T, proxyAddress string) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(t.TempDir(), "trust.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
		ProxyAddress:      proxyAddress,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	svc.secureSessions.dialTimeouts = testDialTimeouts()
	return svc
}

// overlayNoticeAddress is the onion address every notice test dials.
var overlayNoticeAddress = domain.PeerAddress(net.JoinHostPort(testV3Onion, "64646"))

// assertAuxiliaryBudgetReturned fails when a unit of the auxiliary share is
// still held after the dial ended.
func assertAuxiliaryBudgetReturned(t *testing.T, svc *Service) {
	t.Helper()
	if held := svc.connBudget.Snapshot().Auxiliary; held != 0 {
		t.Fatalf("%d auxiliary unit(s) still held after the dial ended", held)
	}
}

// A notice to an onion peer reaches it through the proxy, and the proxy is
// handed the onion NAME: nothing resolves it on this machine.
func TestANoticeToAnOverlayPeerGoesThroughTheProxyByName(t *testing.T) {
	t.Parallel()
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	frames := &recordedLines{}
	peer := startAnsweringOldNode(t, frames, probe, full, protocol.Frame{Type: "auth_ok"}, protocol.Frame{Type: "ok"})
	proxy := startForwardingSOCKS5(t, peer)
	svc := newProxiedNoticeService(t, proxy.address())

	svc.sendNoticeToPeer(overlayNoticeAddress, time.Minute, "proxied-notice-ciphertext")

	connects := proxy.connectRequests()
	if len(connects) == 0 {
		t.Fatal("the notice to an onion peer never reached the proxy")
	}
	for i, connect := range connects {
		if connect.addressType != 0x03 || connect.host != testV3Onion || strconv.Itoa(connect.port) != "64646" {
			t.Fatalf("CONNECT %d = %+v, want the onion name %s:64646 as a domain (ATYP 3)", i, connect, testV3Onion)
		}
	}
	got := frames.all()
	if len(got) == 0 || got[len(got)-1] != "push_notice" {
		t.Fatalf("the peer behind the proxy read %v, want the exchange to end with push_notice", got)
	}
	assertAuxiliaryBudgetReturned(t, svc)
}

// tcpDialSpy records every address the node opens a TCP socket to and then
// dials it for real. A name a direct dial would resolve locally shows up
// here as the dialled address, so "not in the list" means "neither dialled
// nor resolved on this machine".
type tcpDialSpy struct {
	mu        sync.Mutex
	addresses []string
}

func (s *tcpDialSpy) dial(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
	s.mu.Lock()
	s.addresses = append(s.addresses, address)
	s.mu.Unlock()
	return dialTCPDirect(ctx, address, timeout)
}

func (s *tcpDialSpy) dialled() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.addresses...)
}

// spyOnPeerDials puts a tcpDialSpy under svc's peer dials.
func spyOnPeerDials(svc *Service) *tcpDialSpy {
	spy := &tcpDialSpy{}
	svc.peerTCPDial = spy.dial
	return spy
}

// assertOnlyDialled fails unless the spy saw exactly want, in order.
func assertOnlyDialled(t *testing.T, spy *tcpDialSpy, want ...string) {
	t.Helper()
	got := spy.dialled()
	if len(got) != len(want) {
		t.Fatalf("sockets opened to %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sockets opened to %v, want %v", got, want)
		}
	}
}

// A proxy that cannot be reached ends the dial with an error that says so;
// the one socket attempted is the one to the proxy — the attempt does not
// turn into a direct connection — and the unit of the budget is returned.
func TestAnUnreachableProxyEndsTheDialWithoutADirectConnection(t *testing.T) {
	t.Parallel()
	proxy := closedLoopbackAddress(t)
	svc := newProxiedNoticeService(t, proxy)
	spy := spyOnPeerDials(svc)

	conn, err := svc.dialPeerWithBudget(context.Background(), overlayNoticeAddress, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("an overlay dial succeeded with the proxy down")
	}
	if !errors.Is(err, errProxyUnavailable) {
		t.Fatalf("err = %v, want errProxyUnavailable", err)
	}
	assertOnlyDialled(t, spy, proxy)
	assertAuxiliaryBudgetReturned(t, svc)

	svc.sendNoticeToPeer(overlayNoticeAddress, time.Minute, "unreachable-proxy-ciphertext")
	for _, address := range spy.dialled() {
		if address != proxy {
			t.Fatalf("the notice opened a socket to %s with the proxy down, want the proxy alone", address)
		}
	}
	assertAuxiliaryBudgetReturned(t, svc)
}

// A name in an overlay TLD (.onion, .i2p — any case, with or without the
// trailing dot of a fully qualified name) is never dialled or resolved
// directly: a valid one goes to the proxy, by its name; an invalid one is
// refused before any socket, whether a proxy is configured or not. Such names
// reach the node from other peers (peer exchange, announcements), so the
// validator that admits an overlay name must not be the thing that decides
// whether it is resolved locally.
func TestANameInAnOverlayTLDIsNeverDialledDirectly(t *testing.T) {
	t.Parallel()
	v3 := strings.Repeat("b", 56)
	refused := map[string]domain.PeerAddress{
		"a subdomain of a v3 name":      domain.PeerAddress("www." + v3 + ".onion:64646"),
		"an onion name of wrong length": domain.PeerAddress("abc.onion:64646"),
		"an i2p name that is not b32":   domain.PeerAddress("foo.i2p:64646"),
		"an upper-case bad onion":       domain.PeerAddress("WWW." + strings.ToUpper(v3) + ".ONION:64646"),
		"a bad onion with a dot":        domain.PeerAddress("abc.onion.:64646"),
	}
	for name, address := range refused {
		for _, withProxy := range []bool{true, false} {
			t.Run(name+proxyCaseName(withProxy), func(t *testing.T) {
				t.Parallel()
				proxy := ""
				if withProxy {
					proxy = startStalledSOCKS5(t).address()
				}
				svc := newProxiedNoticeService(t, proxy)
				spy := spyOnPeerDials(svc)

				conn, err := svc.dialPeerWithBudget(context.Background(), address, time.Second)
				if err == nil {
					_ = conn.Close()
				}
				if !errors.Is(err, errInvalidOverlayAddress) {
					t.Fatalf("err = %v, want errInvalidOverlayAddress", err)
				}
				assertOnlyDialled(t, spy)
				assertAuxiliaryBudgetReturned(t, svc)
			})
		}
	}

	t.Run("a valid name with the trailing dot goes to the proxy by its name", func(t *testing.T) {
		t.Parallel()
		forwarder := startForwardingSOCKS5(t, domain.PeerAddress(listenLoopback(t).Addr().String()))
		svc := newProxiedNoticeService(t, forwarder.address())
		spy := spyOnPeerDials(svc)

		conn, err := svc.dialPeerWithBudget(context.Background(), domain.PeerAddress(strings.ToUpper(v3)+".OnIoN.:64646"), time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		_ = conn.Close()
		assertOnlyDialled(t, spy, forwarder.address())
		connects := forwarder.connectRequests()
		if len(connects) != 1 || connects[0].addressType != 0x03 || connects[0].host != v3+".onion" {
			t.Fatalf("CONNECTs %+v, want the canonical name %s.onion as a domain", connects, v3)
		}
		assertAuxiliaryBudgetReturned(t, svc)
	})
}

func proxyCaseName(withProxy bool) string {
	if withProxy {
		return " with a proxy"
	}
	return " without a proxy"
}

// An overlay address with no proxy configured is refused before any socket:
// there is no direct form of an onion name to fall back to.
func TestAnOverlayDialWithoutAProxyIsRefused(t *testing.T) {
	t.Parallel()
	svc := newProxiedNoticeService(t, "")

	_, err := svc.dialPeerWithBudget(context.Background(), overlayNoticeAddress, time.Second)
	if !errors.Is(err, errOverlayWithoutProxy) {
		t.Fatalf("err = %v, want errOverlayWithoutProxy", err)
	}
	assertAuxiliaryBudgetReturned(t, svc)
}

// The v2 policy holds behind the proxy: an old release whose identity is
// pinned hears only the identity-less probe, and an endpoint bound to v2 is
// never probed at all — the notice ends at the TLS attempt.
func TestANoticeThroughTheProxyRefusesLegacyForAPinnedPeer(t *testing.T) {
	t.Parallel()
	t.Run("identity pinned", func(t *testing.T) {
		t.Parallel()
		old := generateProbeIdentity(t)
		probe, full := oldNodeWelcomes(t, old, config.MinimumProtocolVersion)
		frames := &recordedLines{}
		proxy := startForwardingSOCKS5(t, startLineRecordingOldNode(t, probe, full, frames))
		svc := newProxiedNoticeService(t, proxy.address())
		pinned, err := domain.ParsePeerIdentity(old.Address)
		if err != nil {
			t.Fatalf("identity: %v", err)
		}
		if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
			t.Fatalf("pin: %v", err)
		}

		svc.sendNoticeToPeer(overlayNoticeAddress, time.Minute, "pinned-peer-ciphertext")

		if got := frames.all(); len(got) != 1 || got[0] != "hello" {
			t.Fatalf("the pinned old release read %v, want the probe alone", got)
		}
		assertAuxiliaryBudgetReturned(t, svc)
	})
	t.Run("endpoint bound", func(t *testing.T) {
		t.Parallel()
		probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
		frames := &recordedLines{}
		proxy := startForwardingSOCKS5(t, startLineRecordingOldNode(t, probe, full, frames))
		svc := newProxiedNoticeService(t, proxy.address())
		if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(overlayNoticeAddress), domain.PeerIdentity{1}, endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}

		svc.sendNoticeToPeer(overlayNoticeAddress, time.Minute, "bound-endpoint-ciphertext")

		if got := frames.all(); len(got) != 0 {
			t.Fatalf("the endpoint bound to v2 read %v over v1, want nothing", got)
		}
		if connects := proxy.connectRequests(); len(connects) != 1 {
			t.Fatalf("%d CONNECTs through the proxy, want the TLS attempt alone", len(connects))
		}
		assertAuxiliaryBudgetReturned(t, svc)
	})
}

// A dial cancelled while the proxy has not answered ends at once: its socket
// is closed — the proxy sees it go — and its unit of the budget is returned,
// instead of both being held until the dial's own timeout.
func TestACancelledProxiedDialReleasesItsSocketAndBudget(t *testing.T) {
	t.Parallel()
	proxy := startStalledSOCKS5(t)
	svc := newProxiedNoticeService(t, proxy.address())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type dialResult struct {
		conn net.Conn
		err  error
	}
	result := make(chan dialResult, 1)
	go func() {
		conn, err := svc.dialPeerWithBudget(ctx, overlayNoticeAddress, time.Minute)
		result <- dialResult{conn: conn, err: err}
	}()
	awaitSignal(t, proxy.accepted, 5*time.Second, "the dial reaching the proxy")
	if held := svc.connBudget.Snapshot().Auxiliary; held != 1 {
		t.Fatalf("auxiliary units held during the dial = %d, want 1", held)
	}
	cancel()

	select {
	case got := <-result:
		if got.err == nil {
			_ = got.conn.Close()
			t.Fatal("a cancelled dial returned a connection")
		}
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled dial was still waiting on the proxy after 5s")
	}
	awaitSignal(t, proxy.released, 5*time.Second, "the proxy seeing the cancelled socket closed")
	assertAuxiliaryBudgetReturned(t, svc)
}
