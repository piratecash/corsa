package node

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// Two real nodes that talk only through a counting TCP proxy:
//
//	dialler (no listener) --dial--> proxy --dial--> listener node
//
// The proxy counts, per pipe and per direction, what it read from each side
// and what each side accepted from it. At a quiescent instant the node
// counters must equal the proxy's independent count:
//
//	listener.sent == Σ readFromServer   listener.received == Σ writtenToServer
//	dialler.sent  == Σ readFromClient   dialler.received  == Σ writtenToClient
//
// This is the only test that drives openPeerSessionForCM — the session dial of
// the connection manager, i.e. every outbound session in production — end to
// end. A socket left unwrapped shows as node < proxy, a double wrap as
// node > proxy.

// countingPipe is one proxied connection.
type countingPipe struct {
	client, server *net.TCPConn
	// readFromClient / writtenToClient and their server twins are what the
	// proxy saw, independently of either node's MeteredConn.
	readFromClient, writtenToClient atomic.Uint64
	readFromServer, writtenToServer atomic.Uint64
	// holdToClient / holdToServer stop forwarding in one direction while the
	// source keeps being drained, so a break can be staged without the far
	// node's last writes being lost to the count.
	holdToClient, holdToServer             atomic.Bool
	clientToServerDone, serverToClientDone chan struct{}
}

// countingProxy forwards every accepted connection to target.
type countingProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	pipes  []*countingPipe
}

func newCountingProxy(t *testing.T, target string) *countingProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &countingProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(p.closeAll)
	return p
}

// closeAll closes the listener and both ends of every pipe. It is the
// proxy's cleanup and the escape hatch of a break that did not finish;
// closing twice is harmless.
func (p *countingProxy) closeAll() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pipe := range p.pipes {
		_ = pipe.client.Close()
		_ = pipe.server.Close()
	}
}

func (p *countingProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		clientTCP, clientOK := client.(*net.TCPConn)
		serverTCP, serverOK := server.(*net.TCPConn)
		if !clientOK || !serverOK {
			_ = client.Close()
			_ = server.Close()
			continue
		}
		pipe := &countingPipe{
			client:             clientTCP,
			server:             serverTCP,
			clientToServerDone: make(chan struct{}),
			serverToClientDone: make(chan struct{}),
		}
		p.mu.Lock()
		p.pipes = append(p.pipes, pipe)
		p.mu.Unlock()
		go pumpCounted(pipe.client, pipe.server, &pipe.readFromClient, &pipe.writtenToServer, &pipe.holdToServer, pipe.clientToServerDone)
		go pumpCounted(pipe.server, pipe.client, &pipe.readFromServer, &pipe.writtenToClient, &pipe.holdToClient, pipe.serverToClientDone)
	}
}

// pumpCounted copies src to dst, counting what it read and what dst accepted.
// While hold is set it keeps draining src but forwards nothing.
func pumpCounted(src, dst *net.TCPConn, read, written *atomic.Uint64, hold *atomic.Bool, done chan struct{}) {
	defer close(done)
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			read.Add(uint64(n))
			if !hold.Load() {
				accepted, _ := dst.Write(buf[:n])
				written.Add(uint64(accepted))
			}
		}
		if err != nil {
			if !hold.Load() {
				_ = dst.CloseWrite()
			}
			return
		}
	}
}

// pipeDirection names one forwarding direction of a proxied connection.
type pipeDirection string

const (
	pipeDiallerToListener pipeDirection = "dialler→listener"
	pipeListenerToDialler pipeDirection = "listener→dialler"
)

// proxyPipeStuckError is a staged break the nodes did not finish: which
// directions of the proxied connection were still open when the budget ran
// out.
type proxyPipeStuckError struct {
	Budget     time.Duration
	Unfinished []pipeDirection
}

func (e *proxyPipeStuckError) Error() string {
	return fmt.Sprintf("proxied connection still open after %s in %v", e.Budget, e.Unfinished)
}

// pipeDirectionDone is one direction and the channel its pump closes.
type pipeDirectionDone struct {
	direction pipeDirection
	done      <-chan struct{}
}

func (pipe *countingPipe) directions() []pipeDirectionDone {
	return []pipeDirectionDone{
		{direction: pipeDiallerToListener, done: pipe.clientToServerDone},
		{direction: pipeListenerToDialler, done: pipe.serverToClientDone},
	}
}

// awaitFinished waits, within budget, for both directions of the pipe to
// end, and names every direction still open when it runs out.
func (pipe *countingPipe) awaitFinished(budget time.Duration) error {
	deadline := time.NewTimer(budget)
	defer deadline.Stop()
	for _, direction := range pipe.directions() {
		select {
		case <-direction.done:
		case <-deadline.C:
			return &proxyPipeStuckError{Budget: budget, Unfinished: pipe.unfinished()}
		}
	}
	return nil
}

func (pipe *countingPipe) unfinished() []pipeDirection {
	var open []pipeDirection
	for _, direction := range pipe.directions() {
		select {
		case <-direction.done:
		default:
			open = append(open, direction.direction)
		}
	}
	return open
}

// proxyBreakBudget bounds a staged break: a node closes its end within
// milliseconds of seeing EOF, and the budget is for a loaded machine.
const proxyBreakBudget = 10 * time.Second

// awaitBreak waits for a staged break to finish or fails the test naming the
// break and the directions still open. On failure it closes every socket the
// proxy owns first, so neither a pump nor a node is left blocked on one
// while the test unwinds through its cleanups.
func awaitBreak(t *testing.T, stage string, proxy *countingProxy, pipe *countingPipe) {
	t.Helper()

	if err := pipe.awaitFinished(proxyBreakBudget); err != nil {
		proxy.closeAll()
		t.Fatalf("%s: %v", stage, err)
	}
}

func (p *countingProxy) last() *countingPipe {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pipes) == 0 {
		return nil
	}
	return p.pipes[len(p.pipes)-1]
}

func (p *countingProxy) pipeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pipes)
}

// proxyReading puts both sides' counts next to each other.
type proxyReading struct {
	pipes                           int
	readFromClient, writtenToClient uint64
	readFromServer, writtenToServer uint64
	listenerSent, listenerReceived  uint64
	diallerSent, diallerReceived    uint64
}

func (r proxyReading) matches() bool {
	return r.listenerSent == r.readFromServer && r.listenerReceived == r.writtenToServer &&
		r.diallerSent == r.readFromClient && r.diallerReceived == r.writtenToClient
}

func (r proxyReading) String() string {
	return fmt.Sprintf("pipes=%d | listener sent=%d received=%d vs proxy read=%d wrote=%d | dialler sent=%d received=%d vs proxy read=%d wrote=%d",
		r.pipes, r.listenerSent, r.listenerReceived, r.readFromServer, r.writtenToServer,
		r.diallerSent, r.diallerReceived, r.readFromClient, r.writtenToClient)
}

func readThroughProxy(listener, dialler *Service, proxy *countingProxy) proxyReading {
	var r proxyReading
	proxy.mu.Lock()
	r.pipes = len(proxy.pipes)
	for _, pipe := range proxy.pipes {
		r.readFromClient += pipe.readFromClient.Load()
		r.writtenToClient += pipe.writtenToClient.Load()
		r.readFromServer += pipe.readFromServer.Load()
		r.writtenToServer += pipe.writtenToServer.Load()
	}
	proxy.mu.Unlock()
	listenerStats, diallerStats := listener.TransportTrafficStats(), dialler.TransportTrafficStats()
	r.listenerSent, r.listenerReceived = listenerStats.BytesSent, listenerStats.BytesReceived
	r.diallerSent, r.diallerReceived = diallerStats.BytesSent, diallerStats.BytesReceived
	return r
}

// awaitProxyMatch waits for a quiescent instant: the equalities hold on two
// readings 30 ms apart that are identical. Live sessions keep talking, so a
// single matching reading could be a coincidence between two writes.
func awaitProxyMatch(t *testing.T, stage string, listener, dialler *Service, proxy *countingProxy) proxyReading {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	var last proxyReading
	for time.Now().Before(deadline) {
		first := readThroughProxy(listener, dialler, proxy)
		last = first
		if first.matches() {
			time.Sleep(30 * time.Millisecond)
			second := readThroughProxy(listener, dialler, proxy)
			last = second
			if second == first {
				return second
			}
			continue
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("%s: node counters never matched the proxy: %s", stage, last)
	return last
}

func requirePipeOpen(t *testing.T, stage string, pipe *countingPipe) {
	t.Helper()
	if pipe == nil {
		t.Fatalf("%s: no proxied connection", stage)
	}
	select {
	case <-pipe.clientToServerDone:
		t.Fatalf("%s: client->server already finished before the break", stage)
	case <-pipe.serverToClientDone:
		t.Fatalf("%s: server->client already finished before the break", stage)
	default:
	}
}

func peerPortOf(t *testing.T, address string) *domain.PeerPort {
	t.Helper()
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split %q: %v", address, err)
	}
	number, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("port %q: %v", portText, err)
	}
	port := domain.PeerPort(number)
	return &port
}

// TestTransportTrafficMatchesAProxyThroughManagedSession checks the node
// counters against the proxy across a live managed session, an outbound break
// (the dialler sees EOF first) and an inbound break (the listener sees EOF
// first), with a reconnect between the two breaks.
//
// Both nodes advertise ports that lead nowhere they must not go. The listener
// advertises the PROXY's port, so a peer exchange that teaches it the proxy
// address is recognised as itself and never dialled (a self-dial through the
// proxy would make the listener a client too and break the equations). The
// dialler has no listener and advertises a freshly released port, so nothing
// is ever dialled back to the default 64646 — where a developer's own node may
// be listening.
func TestTransportTrafficMatchesAProxyThroughManagedSession(t *testing.T) {
	listenAddress := freeAddress(t)
	proxy := newCountingProxy(t, listenAddress)
	proxyAddress := proxy.ln.Addr().String()

	listener, stopListener := startTestNode(t, config.Node{
		ListenAddress:  listenAddress,
		AdvertisePort:  peerPortOf(t, proxyAddress),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	})
	t.Cleanup(stopListener)
	dialler, stopDialler := startTestNode(t, config.Node{
		ListenerSet:     true,
		ListenerEnabled: false,
		AdvertisePort:   peerPortOf(t, freeAddress(t)),
		BootstrapPeers:  []string{proxyAddress},
		Type:            domain.NodeTypeFull,
	})
	t.Cleanup(stopDialler)

	waitForCondition(t, 15*time.Second, func() bool {
		return hasOutboundSession(dialler) && hasConnectedPeer(listener)
	})
	live := awaitProxyMatch(t, "live session", listener, dialler, proxy)
	if live.readFromClient == 0 || live.readFromServer == 0 {
		t.Fatalf("precondition: no traffic through the proxy: %s", live)
	}

	outbound := proxy.last()
	requirePipeOpen(t, "outbound break", outbound)
	outbound.holdToClient.Store(true)
	_ = outbound.client.CloseWrite()
	awaitBreak(t, "outbound break", proxy, outbound)
	awaitProxyMatch(t, "after outbound break", listener, dialler, proxy)

	waitForCondition(t, 30*time.Second, func() bool {
		return proxy.pipeCount() >= 2 && hasOutboundSession(dialler)
	})
	awaitProxyMatch(t, "reconnected", listener, dialler, proxy)

	inbound := proxy.last()
	requirePipeOpen(t, "inbound break", inbound)
	inbound.holdToServer.Store(true)
	_ = inbound.server.CloseWrite()
	awaitBreak(t, "inbound break", proxy, inbound)
	awaitProxyMatch(t, "after inbound break", listener, dialler, proxy)
}

// holdingListener accepts connections and keeps them open, reading nothing
// and closing nothing until the test ends: a node that never answers a break.
func holdingListener(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("holding listener: %v", err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	return ln
}

// A staged break waits for the nodes to close their ends. A node that never
// does must fail the wait within its budget, naming the directions still
// open — not hang the test until -timeout, past every cleanup.
func TestCountingPipeWaitIsBoundedWhenANodeKeepsItsSocketOpen(t *testing.T) {
	t.Parallel()

	proxy := newCountingProxy(t, holdingListener(t).Addr().String())
	client, err := net.Dial("tcp", proxy.ln.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	waitForCondition(t, 5*time.Second, func() bool { return proxy.last() != nil })

	pipe := proxy.last()
	pipe.holdToClient.Store(true)
	_ = pipe.client.CloseWrite()
	const budget = 200 * time.Millisecond
	started := time.Now()
	err = pipe.awaitFinished(budget)
	var stuck *proxyPipeStuckError
	if !errors.As(err, &stuck) {
		t.Fatalf("awaitFinished = %v, want a *proxyPipeStuckError", err)
	}
	want := []pipeDirection{pipeDiallerToListener, pipeListenerToDialler}
	if !reflect.DeepEqual(stuck.Unfinished, want) {
		t.Fatalf("unfinished directions %v, want %v", stuck.Unfinished, want)
	}
	if elapsed := time.Since(started); elapsed > 10*budget {
		t.Fatalf("the wait took %s against a budget of %s", elapsed, budget)
	}
}
