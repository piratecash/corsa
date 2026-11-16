package node

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// readTransportTraffic polls fetch_traffic_totals the way the metrics
// collector does and returns the transport block.
func readTransportTraffic(t *testing.T, svc *Service) protocol.TransportTrafficFrame {
	t.Helper()
	reply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_traffic_totals"})
	if reply.NetworkStats == nil {
		t.Fatal("fetch_traffic_totals returned nil NetworkStats")
	}
	if reply.NetworkStats.Transport == nil {
		t.Fatal("fetch_traffic_totals returned no transport block")
	}
	return *reply.NetworkStats.Transport
}

// assertTransportBytes fails unless the transport block reports exactly the
// given counts.
func assertTransportBytes(t *testing.T, stage string, got protocol.TransportTrafficFrame, sent, received uint64) {
	t.Helper()
	if got.BytesSent != sent || got.BytesReceived != received {
		t.Fatalf("%s: transport = %d sent / %d received, want %d / %d",
			stage, got.BytesSent, got.BytesReceived, sent, received)
	}
}

// newTestMeteredConn is netcore.NewMeteredConn for fixtures whose accumulator
// is known to be present.
func newTestMeteredConn(t *testing.T, conn net.Conn, totals *netcore.TransportTotals) *netcore.MeteredConn {
	t.Helper()
	metered, err := netcore.NewMeteredConn(conn, totals)
	if err != nil {
		t.Fatalf("NewMeteredConn: %v", err)
	}
	return metered
}

// meteredPipeWithTraffic returns a peer-socket wrapper of svc that has
// already written `sent` and read `received` bytes over an in-memory pipe.
func meteredPipeWithTraffic(t *testing.T, svc *Service, sent, received int) *netcore.MeteredConn {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	metered := newTestMeteredConn(t, local, &svc.transportTotals)
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	go func() { _, _ = remote.Write(make([]byte, received)) }()
	if _, err := metered.Write(make([]byte, sent)); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if _, err := io.ReadFull(metered, make([]byte, received)); err != nil {
		t.Fatalf("seed read: %v", err)
	}
	return metered
}

// TestTransportTrafficSurvivesOutboundSessionTeardown walks the teardown order
// of a managed outbound session: the session leaves s.sessions first and its
// bytes are folded into health afterwards. The per-peer totals lose the
// session's bytes between those two steps; the transport totals must not move
// at all, because nothing crossed the socket in between.
func TestTransportTrafficSurvivesOutboundSessionTeardown(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	const sent, received = 300, 50
	metered := meteredPipeWithTraffic(t, svc, sent, received)
	address := domain.PeerAddress("10.0.0.7:64646")

	svc.peerMu.Lock()
	svc.sessions[address] = &peerSession{address: address, metered: metered}
	svc.peerMu.Unlock()
	assertTransportBytes(t, "live session", readTransportTraffic(t, svc), sent, received)

	svc.peerMu.Lock()
	delete(svc.sessions, address)
	svc.peerMu.Unlock()
	assertTransportBytes(t, "session removed, bytes not yet in health", readTransportTraffic(t, svc), sent, received)

	svc.accumulateSessionTraffic(address, metered)
	assertTransportBytes(t, "bytes folded into health", readTransportTraffic(t, svc), sent, received)
}

// TestTransportTrafficCountsInboundCloseOnce walks the inbound teardown order:
// handleConn folds the connection's bytes into health while the connection is
// still registered, then unregisters it. In between, the per-peer totals see
// the same bytes twice; the transport totals must see them once throughout.
func TestTransportTrafficCountsInboundCloseOnce(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	const sent, received = 128, 32
	metered := meteredPipeWithTraffic(t, svc, sent, received)
	core := newTestNetCore(t, netcore.ConnID(501), metered, netcore.Inbound, netcore.Options{Address: "10.0.0.8:64646"})

	svc.peerMu.Lock()
	svc.registerInboundConnLocked(metered, core, metered, nil)
	svc.peerMu.Unlock()
	assertTransportBytes(t, "live inbound", readTransportTraffic(t, svc), sent, received)

	svc.accumulateInboundTraffic(metered)
	assertTransportBytes(t, "bytes in health, conn still registered", readTransportTraffic(t, svc), sent, received)

	svc.unregisterInboundConn(metered)
	assertTransportBytes(t, "conn unregistered", readTransportTraffic(t, svc), sent, received)
}

// TestTransportTrafficSurvivesOrphanedHealthEviction pins that evicting a
// health row — which carries the per-peer byte history with it — leaves the
// transport totals untouched.
func TestTransportTrafficSurvivesOrphanedHealthEviction(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	const sent, received = 400, 90
	metered := meteredPipeWithTraffic(t, svc, sent, received)
	address := domain.PeerAddress("127.0.0.1:55999")
	svc.accumulateSessionTraffic(address, metered)

	stale := time.Now().Add(-2 * orphanedHealthEvictWindow)
	svc.peerMu.Lock()
	health := svc.health[address]
	if health == nil {
		svc.peerMu.Unlock()
		t.Fatal("accumulateSessionTraffic did not create a health row")
	}
	health.Connected = false
	health.LastConnectedAt = stale.Add(-time.Minute)
	health.LastDisconnectedAt = stale
	svc.peerMu.Unlock()
	before := readTransportTraffic(t, svc)

	svc.evictOrphanedHealthEntries()

	svc.peerMu.RLock()
	_, stillThere := svc.health[address]
	svc.peerMu.RUnlock()
	if stillThere {
		t.Fatal("precondition: the orphaned health row was not evicted")
	}
	assertTransportBytes(t, "after eviction", readTransportTraffic(t, svc), before.BytesSent, before.BytesReceived)
	assertTransportBytes(t, "before eviction", before, sent, received)
}

// TestTransportTrafficPollDoesNotReset pins that reading is not consuming: two
// polls with no traffic between them report the same counts, the same
// started_at, and a read_at that does not go backwards.
func TestTransportTrafficPollDoesNotReset(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	meteredPipeWithTraffic(t, svc, 77, 11)

	first := readTransportTraffic(t, svc)
	second := readTransportTraffic(t, svc)
	assertTransportBytes(t, "first poll", first, 77, 11)
	assertTransportBytes(t, "second poll", second, 77, 11)
	if !second.StartedAt.Equal(first.StartedAt) {
		t.Fatalf("started_at moved between polls: %v -> %v", first.StartedAt, second.StartedAt)
	}
	if second.ReadAt.Before(first.ReadAt) {
		t.Fatalf("read_at went backwards: %v -> %v", first.ReadAt, second.ReadAt)
	}
	if first.ReadAt.Before(first.StartedAt) {
		t.Fatalf("read_at %v precedes started_at %v", first.ReadAt, first.StartedAt)
	}
}

// TestTransportTrafficStartedAtIsServiceStart pins the reset marker: the
// period starts when the Service was built, the same instant uptime is
// measured from, so a reader seeing a new started_at knows the process
// restarted and the counters began again from zero.
func TestTransportTrafficStartedAtIsServiceStart(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	got := readTransportTraffic(t, svc)
	if svc.startedAt.IsZero() {
		t.Fatal("precondition: NewService left startedAt zero")
	}
	if !got.StartedAt.Equal(svc.startedAt) {
		t.Fatalf("started_at = %v, want Service.startedAt %v", got.StartedAt, svc.startedAt)
	}
	assertTransportBytes(t, "fresh service", got, 0, 0)
}

// TestTransportTrafficReadAtIsTheReadingInstant pins read_at to the moment of
// the read — it closes the period a rate is computed over, so any other
// instant (the start, a cached snapshot's time) makes every rate wrong. The
// service started an hour ago so that a read_at borrowed from started_at is
// unmistakable.
func TestTransportTrafficReadAtIsTheReadingInstant(t *testing.T) {
	t.Parallel()

	svc := &Service{startedAt: time.Now().UTC().Add(-time.Hour)}

	before := time.Now()
	got := svc.TransportTrafficStats()
	after := time.Now()

	if got.ReadAt.Before(before) || got.ReadAt.After(after) {
		t.Fatalf("read_at = %v, want within the read [%v, %v]", got.ReadAt, before, after)
	}
	if !got.StartedAt.Equal(svc.startedAt) {
		t.Fatalf("started_at = %v, want %v", got.StartedAt, svc.startedAt)
	}
}

// countingConn records the bytes the far end of a peer socket saw, so a test
// can compare the node's counters against what really crossed the wire.
type countingConn struct {
	net.Conn
	read    *atomic.Uint64
	written *atomic.Uint64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(uint64(n))
	return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.written.Add(uint64(n))
	return n, err
}

// countingListener hands out countingConn so an existing mock peer can be
// reused unchanged.
type countingListener struct {
	net.Listener
	read    atomic.Uint64
	written atomic.Uint64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: conn, read: &l.read, written: &l.written}, nil
}

// TestTransportTrafficCountsSyncPeerDial pins that the one-shot recovery dial
// is a peer socket like any other: every byte it exchanged is in the
// transport totals, once. The far end counts independently, so equality
// proves both coverage and the absence of a second count.
func TestTransportTrafficCountsSyncPeerDial(t *testing.T) {
	t.Parallel()

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := &countingListener{Listener: raw}
	t.Cleanup(func() { _ = raw.Close() })

	svc := newSyncPeerTestService(t, domain.NetworkStatusOffline)
	serverDone := make(chan []string, 1)
	go func() { serverDone <- syncPeerMockServer(t, ln, nil) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	svc.syncPeer(ctx, domain.PeerAddress(raw.Addr().String()), true)

	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mock peer did not finish")
	}
	if ln.read.Load() == 0 || ln.written.Load() == 0 {
		t.Fatalf("precondition: no traffic exchanged (peer read %d, wrote %d)", ln.read.Load(), ln.written.Load())
	}
	got := svc.TransportTrafficStats()
	if got.BytesSent != ln.read.Load() || got.BytesReceived != ln.written.Load() {
		t.Fatalf("transport = %d sent / %d received, peer saw %d read / %d written",
			got.BytesSent, got.BytesReceived, ln.read.Load(), ln.written.Load())
	}
}

// TestTransportTrafficCountsNoticeFallbackDial pins the same for the push_notice
// fallback, the other dial that never becomes a session.
func TestTransportTrafficCountsNoticeFallbackDial(t *testing.T) {
	t.Parallel()

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := &countingListener{Listener: raw}
	t.Cleanup(func() { _ = raw.Close() })

	svc := newTestService(t, config.NodeTypeFull)
	peerDone := serveV2Intro(ln, newDialWelcomeResponder(t), currentSecureIntro())
	svc.sendNoticeToPeer(domain.PeerAddress(raw.Addr().String()), time.Minute, "transport-ciphertext")

	select {
	case result := <-peerDone:
		if result.err != nil || len(result.received) != 1 || result.received[0] != "push_notice" {
			t.Fatalf("notice recipient got %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mock peer did not finish")
	}
	got := svc.TransportTrafficStats()
	if got.BytesSent != ln.read.Load() || got.BytesReceived != ln.written.Load() {
		t.Fatalf("transport = %d sent / %d received, peer saw %d read / %d written",
			got.BytesSent, got.BytesReceived, ln.read.Load(), ln.written.Load())
	}
}

// TestTransportTrafficCountsAcceptedConnection drives the real accept path:
// a raw client talks to a running node, and once the node has torn the
// connection down its transport totals equal what the client exchanged —
// proving the listener wraps every accepted socket exactly once.
func TestTransportTrafficCountsAcceptedConnection(t *testing.T) {
	t.Parallel()

	svc, stop := startTestNode(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
	})
	defer stop()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("dial returned %T, want *net.TCPConn", conn)
	}
	defer func() { _ = tcp.Close() }()
	_ = tcp.SetDeadline(time.Now().Add(5 * time.Second))
	var clientRead, clientWritten atomic.Uint64
	client := countingConn{Conn: tcp, read: &clientRead, written: &clientWritten}

	dialler := newTestService(t, config.NodeTypeClient)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := sessionv2.Dial(ctx, client, dialler.secureSessions.local, dialler.nodeHelloFrame(), sessionv2.ExpectedPeer{}, sessionv2.DefaultTimeouts); err != nil {
		t.Fatalf("secure handshake: %v", err)
	}
	// Half-close: the node sees EOF and tears the connection down, while
	// everything it still writes reaches us, so the two sides' counts can be
	// compared byte for byte.
	if err := tcp.CloseWrite(); err != nil {
		t.Fatalf("close write: %v", err)
	}
	if _, err := io.Copy(io.Discard, client); err != nil {
		t.Fatalf("drain: %v", err)
	}

	matches := func() bool {
		got := svc.TransportTrafficStats()
		return got.BytesReceived == clientWritten.Load() && got.BytesSent == clientRead.Load()
	}
	deadline := time.Now().Add(5 * time.Second)
	for !matches() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := svc.TransportTrafficStats(); !matches() {
		t.Fatalf("transport = %d sent / %d received, client saw %d read / %d written",
			got.BytesSent, got.BytesReceived, clientRead.Load(), clientWritten.Load())
	}
}
