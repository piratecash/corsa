package node

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// listener_refusal_test.go pins how the listener says no: every refusal puts
// exactly sessionv2.RefusalAlert on the wire and then ends the connection with
// a FIN, from a bounded pool of alert senders that never holds up the accept
// loop and that one source cannot take whole, and the inbound v2 handshakes
// are bounded per source group from the first byte 0x16 and per node (with a
// loopback share) from the whole ClientHello record on.

// tcpPair is one accepted loopback connection: server is the accepted side,
// client the dialled one. Both are closed when the test ends.
func tcpPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	client, err = net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	server, ok := <-accepted
	if !ok {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return server, client
}

// readAnswer reads everything the other side sends until it ends the
// connection, and how it ended. A reset is reported as the error; a FIN as
// nil.
func readAnswer(t *testing.T, conn net.Conn, within time.Duration) ([]byte, error) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	return io.ReadAll(conn)
}

// v1HelloBytes is what an old dialler has already sent when the refusal
// reaches it — input the listener never reads, which is what turns a close
// into a reset unless it is drained first.
var v1HelloBytes = []byte(`{"type":"hello","version":30,"client":"desktop","listen":"","listener":"0"}` + "\n")

// sourcedConn is a connection that reports a chosen remote address: the
// per-source bounds are about external sources, which a loopback test cannot
// dial from.
type sourcedConn struct {
	net.Conn
	remote net.Addr
}

func (c sourcedConn) RemoteAddr() net.Addr { return c.remote }

func addrOf(ip string) net.Addr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: 40000} }

// The alert, then the FIN right behind it: a dialler that keeps its socket
// open reads the end at once, not after the listener's drain.
func TestARefusedConnectionReadsExactlyTheAlertThenAnOrderlyEnd(t *testing.T) {
	t.Parallel()
	refusals := newListenerRefusals(productionAlertPoolBounds(), time.Now())
	server, client := tcpPair(t)
	if _, err := client.Write(bytes.Repeat(v1HelloBytes, 8)); err != nil {
		t.Fatalf("the dialler's hello: %v", err)
	}

	refusals.refuse(server, refusalConnectionsPerIP)

	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	alert := make([]byte, len(sessionv2.RefusalAlert()))
	if _, err := io.ReadFull(client, alert); err != nil || !bytes.Equal(alert, sessionv2.RefusalAlert()) {
		t.Fatalf("on the wire: % x (%v), want exactly % x", alert, err, sessionv2.RefusalAlert())
	}
	alertAt := time.Now()
	rest, err := io.ReadAll(client)
	if err != nil || len(rest) != 0 {
		t.Fatalf("after the alert: % x then %v, want an orderly end", rest, err)
	}
	if late := time.Since(alertAt); late > refusalAlertDrainTimeout/2 {
		t.Fatalf("the end came %v after the alert: the FIN waited for the drain", late)
	}
	refusals.wait()
	stats := refusals.stats(time.Now())
	if stats.ConnectionsPerIP != 1 || stats.ClosedWithoutAlert != 0 {
		t.Fatalf("stats = %+v, want one per-IP refusal answered with the alert", stats)
	}
}

// A socket that cannot half-close still has what the peer sent drained before
// it is closed: the alert is not followed by a close over unread input.
func TestTheRefusalDrainsEvenWhereTheSocketCannotHalfClose(t *testing.T) {
	t.Parallel()
	refusals := newListenerRefusals(productionAlertPoolBounds(), time.Now())
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	var totals netcore.TransportTotals
	metered, err := netcore.NewMeteredConn(server, &totals)
	if err != nil {
		t.Fatalf("meter: %v", err)
	}

	refusals.refuse(metered, refusalBlacklisted)

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	alert := make([]byte, len(sessionv2.RefusalAlert()))
	if _, err := io.ReadFull(client, alert); err != nil || !bytes.Equal(alert, sessionv2.RefusalAlert()) {
		t.Fatalf("on the wire: % x (%v), want the alert", alert, err)
	}
	// A pipe write completes only when the other end reads it: this one
	// goes through only if the listener drains instead of closing.
	if _, err := client.Write(v1HelloBytes); err != nil {
		t.Fatalf("the listener closed without draining: %v", err)
	}
	refusals.wait()
}

// Ten times more refusals than alert senders, all at once, from peers that
// neither read nor hang up — the case where every sender is held for its
// whole drain. The refusals beyond the pool are closed at once and counted,
// none waits for a sender, and the pool does not grow.
func TestTenTimesThePoolOfRefusalsNeitherGrowsThePoolNorWaits(t *testing.T) {
	t.Parallel()
	const pool = 4
	refusals := newListenerRefusals(alertPoolBounds{total: pool, perSource: 10 * pool}, time.Now())
	servers := make([]net.Conn, 0, 10*pool)
	clients := make([]net.Conn, 0, 10*pool)
	for range 10 * pool {
		server, client := tcpPair(t)
		servers = append(servers, server)
		clients = append(clients, client)
	}

	started := time.Now()
	for _, server := range servers {
		refusals.refuse(server, refusalConnectionRate)
	}
	if spent := time.Since(started); spent >= refusalAlertDrainTimeout {
		t.Fatalf("refusing %d connections took %v: refuse waited for a sender", len(servers), spent)
	}
	stats := refusals.stats(time.Now())
	if stats.ConnectionRate != 10*pool {
		t.Fatalf("ConnectionRate = %d, want %d", stats.ConnectionRate, 10*pool)
	}
	if stats.ClosedWithoutAlert != 9*pool {
		t.Fatalf("ClosedWithoutAlert = %d, want %d: the pool is %d senders", stats.ClosedWithoutAlert, 9*pool, pool)
	}

	refusals.wait()
	alerted := 0
	for _, client := range clients {
		got, _ := readAnswer(t, client, 3*time.Second)
		if bytes.Equal(got, sessionv2.RefusalAlert()) {
			alerted++
		} else if len(got) != 0 {
			t.Fatalf("a refused dialler read % x, want the alert or nothing", got)
		}
	}
	if alerted != pool {
		t.Fatalf("%d dialers got the alert, want exactly the pool of %d", alerted, pool)
	}
}

// One source — an IPv4 address or an IPv6 /64 — holds at most its share of
// the alert senders, so it cannot take the whole pool from everybody else.
func TestOneSourceHoldsOnlyItsShareOfTheAlertSenders(t *testing.T) {
	t.Parallel()
	const share = 2
	refusals := newListenerRefusals(alertPoolBounds{total: 10, perSource: share}, time.Now())
	refuseFrom := func(ip string) net.Conn {
		server, client := tcpPair(t)
		refusals.refuse(sourcedConn{Conn: server, remote: addrOf(ip)}, refusalConnectionRate)
		return client
	}
	// Five addresses of one /64: one source.
	sameSource := make([]net.Conn, 0, 5)
	for _, ip := range []string{"2001:db8:5:6::1", "2001:db8:5:6::2", "2001:db8:5:6::3", "2001:db8:5:6:ffff::4", "2001:db8:5:6::5"} {
		sameSource = append(sameSource, refuseFrom(ip))
	}
	other := refuseFrom("198.51.100.9")

	if stats := refusals.stats(time.Now()); stats.ClosedWithoutAlert != 5-share {
		t.Fatalf("ClosedWithoutAlert = %d, want %d: one /64 holds %d senders", stats.ClosedWithoutAlert, 5-share, share)
	}
	refusals.wait()
	alerted := 0
	for _, client := range sameSource {
		if got, _ := readAnswer(t, client, 3*time.Second); bytes.Equal(got, sessionv2.RefusalAlert()) {
			alerted++
		}
	}
	if alerted != share {
		t.Fatalf("one /64 got %d alerts, want %d", alerted, share)
	}
	if got, _ := readAnswer(t, other, 3*time.Second); !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("another source read % x, want the alert", got)
	}
}

func TestInboundHandshakeBounds(t *testing.T) {
	t.Parallel()
	refusedBy := func(t *testing.T, err error) listenerRefusalReason {
		t.Helper()
		var refusal *listenerRefusalError
		if !errors.As(err, &refusal) {
			t.Fatalf("got %v, want a listener refusal", err)
		}
		return refusal.reason
	}
	source := func(ip string) handshakeSource { return handshakeSourceOf(addrOf(ip), true) }

	t.Run("production values", func(t *testing.T) {
		if got, want := productionHandshakeBounds(), (handshakeBounds{node: 64, loopback: 16, perSource: 4}); got != want {
			t.Fatalf("bounds = %+v, want %+v", got, want)
		}
		if clientHelloRecordTimeout != time.Second {
			t.Fatalf("ClientHello record deadline = %v, want 1s", clientHelloRecordTimeout)
		}
	})

	t.Run("per source, from the first byte", func(t *testing.T) {
		h := newInboundHandshakes(productionHandshakeBounds())
		releases := make([]func(), 0, 4)
		for _, ip := range []string{"2001:db8:1:2::1", "2001:db8:1:2::2", "2001:db8:1:2:abcd::3", "2001:db8:1:2::4"} {
			release, err := h.admitSource(source(ip))
			if err != nil {
				t.Fatalf("admit %s within the per-source bound: %v", ip, err)
			}
			releases = append(releases, release)
		}
		_, err := h.admitSource(source("2001:db8:1:2::99"))
		if reason := refusedBy(t, err); reason != refusalHandshakesPerIP {
			t.Fatalf("a fifth address of one /64 refused by %v, want %v", reason, refusalHandshakesPerIP)
		}
		if _, err := h.admitSource(source("2001:db8:1:3::1")); err != nil {
			t.Fatalf("another /64 was refused: %v", err)
		}
		for i := range 4 {
			if _, err := h.admitSource(source("203.0.113.7")); err != nil && i < 4 {
				t.Fatalf("IPv4 address %d: %v", i, err)
			}
		}
		if _, err := h.admitSource(source("203.0.113.8")); err != nil {
			t.Fatalf("an IPv4 source is its own address, not its /24: %v", err)
		}
		if h.running() != 0 {
			t.Fatalf("admitting sources took %d node slots", h.running())
		}
		releases[0]()
		releases[0]() // a second release gives nothing back twice
		if _, err := h.admitSource(source("2001:db8:1:2::99")); err != nil {
			t.Fatalf("a released source slot was not given back: %v", err)
		}
		if _, err := h.admitSource(source("2001:db8:1:2::98")); err == nil {
			t.Fatal("a double release gave a source slot back twice")
		}
	})

	t.Run("loopback and per-source-off sources are not counted per source", func(t *testing.T) {
		h := newInboundHandshakes(productionHandshakeBounds())
		for range 10 {
			if _, err := h.admitSource(handshakeSourceOf(addrOf("127.0.0.1"), true)); err != nil {
				t.Fatalf("loopback: %v", err)
			}
			if _, err := h.admitSource(handshakeSourceOf(addrOf("203.0.113.7"), false)); err != nil {
				t.Fatalf("per-IP limits off: %v", err)
			}
		}
	})

	t.Run("per node, with a loopback share", func(t *testing.T) {
		h := newInboundHandshakes(productionHandshakeBounds())
		loopback := handshakeSourceOf(addrOf("127.0.0.1"), true)
		releases := make([]func(), 0, 64)
		for i := range 16 {
			release, err := h.enter(loopback)
			if err != nil {
				t.Fatalf("loopback handshake %d within its share: %v", i, err)
			}
			releases = append(releases, release)
		}
		_, err := h.enter(loopback)
		if reason := refusedBy(t, err); reason != refusalHandshakesLoopback {
			t.Fatalf("the 17th loopback handshake refused by %v, want %v", reason, refusalHandshakesLoopback)
		}
		for i := range 64 - 16 {
			release, err := h.enter(source("203.0.113.7"))
			if err != nil {
				t.Fatalf("external handshake %d within the node bound: %v", i, err)
			}
			releases = append(releases, release)
		}
		_, err = h.enter(source("198.51.100.9"))
		if reason := refusedBy(t, err); reason != refusalHandshakesNode {
			t.Fatalf("the 65th handshake refused by %v, want %v", reason, refusalHandshakesNode)
		}
		for _, release := range releases {
			release()
		}
		if running := h.running(); running != 0 {
			t.Fatalf("running = %d after every release, want 0", running)
		}
		if _, err := h.enter(loopback); err != nil {
			t.Fatalf("the loopback share was not given back: %v", err)
		}
	})

	t.Run("source", func(t *testing.T) {
		for _, tc := range []struct {
			ip          string
			perIPLimits bool
			want        handshakeSource
		}{
			{"203.0.113.7", true, handshakeSource{group: "203.0.113.7", perSource: true}},
			{"203.0.113.7", false, handshakeSource{group: "203.0.113.7"}},
			{"2001:db8:1:2:3:4:5:6", true, handshakeSource{group: "2001:db8:1:2::/64", perSource: true}},
			{"::ffff:203.0.113.7", true, handshakeSource{group: "203.0.113.7", perSource: true}},
			{"127.0.0.1", true, handshakeSource{group: "127.0.0.1", loopback: true}},
			{"::1", true, handshakeSource{group: "::/64", loopback: true}},
		} {
			if got := handshakeSourceOf(addrOf(tc.ip), tc.perIPLimits); got != tc.want {
				t.Fatalf("%s with per-IP limits %v: %+v, want %+v", tc.ip, tc.perIPLimits, got, tc.want)
			}
		}
	})
}

// refusingNode starts a quiet node whose listener limits are on, after setup
// arranged the bound under test to refuse the next connection from loopback.
func refusingNode(t *testing.T, cfg config.Node, setup func(*Service)) *Service {
	t.Helper()
	cfg.ListenAddress = freeAddress(t)
	cfg.BootstrapPeers = []string{}
	cfg.Type = domain.NodeTypeFull
	svc, stop := startTestNodeWithSetup(t, cfg, withoutOwnDials(func(svc *Service) {
		svc.disableRateLimiting = false
		setup(svc)
	}))
	t.Cleanup(stop)
	return svc
}

// dialAndRead connects to address, sends a v1 hello the listener will not
// read, and returns everything the listener answers until it ends the
// connection.
func dialAndRead(t *testing.T, address string) []byte {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write(v1HelloBytes)
	got, err := readAnswer(t, conn, 5*time.Second)
	if err != nil {
		t.Fatalf("read the refusal: %q then %v, want the alert and an orderly end", got, err)
	}
	return got
}

// All five paths on which the listener used to close silently answer with
// exactly the alert, each counted under its own bound.
func TestEveryListenerRefusalPutsExactlyTheAlertOnTheWire(t *testing.T) {
	t.Parallel()
	alert := sessionv2.RefusalAlert()

	t.Run("connection rate", func(t *testing.T) {
		t.Parallel()
		svc := refusingNode(t, config.Node{}, func(svc *Service) {
			svc.connLimiter.windows["127.0.0.1"] = &connWindow{count: defaultConnRateLimit, windowStart: time.Now()}
		})
		if got := dialAndRead(t, svc.cfg.ListenAddress); !bytes.Equal(got, alert) {
			t.Fatalf("on the wire: % x, want % x", got, alert)
		}
		if stats := svc.ListenerRefusalStats(); stats.ConnectionRate != 1 {
			t.Fatalf("stats = %+v, want one rate refusal", stats)
		}
	})

	t.Run("connections per IP", func(t *testing.T) {
		t.Parallel()
		svc := refusingNode(t, config.Node{}, func(svc *Service) {
			svc.inboundByIP["127.0.0.1"] = maxConnPerIP
		})
		if got := dialAndRead(t, svc.cfg.ListenAddress); !bytes.Equal(got, alert) {
			t.Fatalf("on the wire: % x, want % x", got, alert)
		}
		if stats := svc.ListenerRefusalStats(); stats.ConnectionsPerIP != 1 {
			t.Fatalf("stats = %+v, want one per-IP refusal", stats)
		}
	})

	t.Run("connection budget", func(t *testing.T) {
		t.Parallel()
		svc := refusingNode(t, config.Node{MaxIncomingPeers: 1}, func(*Service) {})
		holder, err := net.Dial("tcp", svc.cfg.ListenAddress)
		if err != nil {
			t.Fatalf("dial the budget holder: %v", err)
		}
		t.Cleanup(func() { _ = holder.Close() })
		waitForConditionMsg(t, 5*time.Second, "the holder never took the inbound budget", func() bool {
			return svc.connBudget.Snapshot().Inbound == 1
		})
		if got := dialAndRead(t, svc.cfg.ListenAddress); !bytes.Equal(got, alert) {
			t.Fatalf("on the wire: % x, want % x", got, alert)
		}
		if stats := svc.ListenerRefusalStats(); stats.ConnectionBudget != 1 {
			t.Fatalf("stats = %+v, want one budget refusal", stats)
		}
	})

	t.Run("blacklist", func(t *testing.T) {
		t.Parallel()
		svc := refusingNode(t, config.Node{}, func(svc *Service) {
			svc.bans["127.0.0.1"] = banEntry{Blacklisted: time.Now().UTC().Add(time.Hour)}
		})
		if got := dialAndRead(t, svc.cfg.ListenAddress); !bytes.Equal(got, alert) {
			t.Fatalf("on the wire: % x, want % x", got, alert)
		}
		if stats := svc.ListenerRefusalStats(); stats.Blacklisted != 1 {
			t.Fatalf("stats = %+v, want one blacklist refusal", stats)
		}
	})

	// The inbound limit at registration is held from accept on by the shared
	// budget, which refuses first (the case above); the registration bound is
	// the defence behind it and is driven through handleConn directly, with
	// no budget reservation.
	t.Run("inbound limit", func(t *testing.T) {
		t.Parallel()
		svc := refusingNode(t, config.Node{MaxIncomingPeers: 1}, func(*Service) {})
		holder, err := net.Dial("tcp", svc.cfg.ListenAddress)
		if err != nil {
			t.Fatalf("dial the registered holder: %v", err)
		}
		t.Cleanup(func() { _ = holder.Close() })
		if _, err := holder.Write([]byte("{")); err != nil {
			t.Fatalf("holder's first byte: %v", err)
		}
		waitForConditionMsg(t, 5*time.Second, "the holder was never registered", func() bool {
			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			return svc.countInboundConnsLocked() == 1
		})

		server, client := tcpPair(t)
		handled := make(chan struct{})
		go func() {
			defer close(handled)
			svc.handleConn(server, nil)
		}()
		_, _ = client.Write(v1HelloBytes)
		got, err := readAnswer(t, client, 5*time.Second)
		if err != nil || !bytes.Equal(got, alert) {
			t.Fatalf("on the wire: % x then %v, want exactly % x and an orderly end", got, err, alert)
		}
		<-handled
		if stats := svc.ListenerRefusalStats(); stats.InboundLimit != 1 {
			t.Fatalf("stats = %+v, want one inbound-limit refusal", stats)
		}
	})
}

// A flood of refusals ten times the alert pool, at a running node: every
// connection is answered at once — the accept loop never waits for a sender
// — the pool does not grow past its bound, and once the flood's cause is gone
// an honest v2 dial gets in.
func TestAFloodOfRefusalsTenTimesThePoolKeepsTheListenerServing(t *testing.T) {
	t.Parallel()
	const pool = 4
	clock := &steppedClock{now: time.Now()}
	listener := refusingNode(t, config.Node{}, func(svc *Service) {
		svc.listenerRefusals = newListenerRefusals(alertPoolBounds{total: pool, perSource: 10 * pool}, svc.startedAt)
		svc.connLimiter = newConnRateLimiter(clock.Now)
		svc.inboundByIP["127.0.0.1"] = maxConnPerIP
	})

	flood := make([]net.Conn, 0, 10*pool)
	t.Cleanup(func() {
		for _, conn := range flood {
			_ = conn.Close()
		}
	})
	started := time.Now()
	var answered sync.WaitGroup
	for range 10 * pool {
		conn, err := net.Dial("tcp", listener.cfg.ListenAddress)
		if err != nil {
			t.Fatalf("flood dial: %v", err)
		}
		flood = append(flood, conn)
		_, _ = conn.Write(v1HelloBytes)
		// The flood reads its answer and keeps its socket open, so every
		// alert sender is held for its whole drain.
		answered.Go(func() { _, _ = readAnswer(t, conn, 5*time.Second) })
	}
	answered.Wait()
	if spent := time.Since(started); spent > time.Second {
		t.Fatalf("answering %d refused connections took %v: the accept loop waited for alert senders", 10*pool, spent)
	}
	stats := listener.ListenerRefusalStats()
	if refused := stats.ConnectionRate + stats.ConnectionsPerIP; refused != 10*pool {
		t.Fatalf("refused %d, want %d: %+v", refused, 10*pool, stats)
	}
	if stats.ClosedWithoutAlert < 10*pool-2*pool {
		t.Fatalf("ClosedWithoutAlert = %d of %d: the alert pool of %d grew", stats.ClosedWithoutAlert, 10*pool, pool)
	}

	// The flood's cause is gone: the rate window has run out and the per-IP
	// count the fixture held is released.
	clock.Advance(defaultConnRateWindow)
	listener.ipStateMu.Lock()
	delete(listener.inboundByIP, "127.0.0.1")
	listener.ipStateMu.Unlock()
	dialer, stopDialer := startQuietNode(t, nil)
	t.Cleanup(stopDialer)
	if established, err := dialPastTransport(t, dialer, domain.PeerAddress(listener.cfg.ListenAddress)); !established {
		t.Fatalf("an honest dial after the flood did not get in: %v", err)
	}
	waitForConditionMsg(t, 5*time.Second, "the honest dial was not a v2 session", func() bool {
		return listener.SessionKindStats().InboundV2 == 1
	})
}

// steppedClock is a clock a test moves by hand; the rate limiter reads it
// from the accept loop while the test advances it.
type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// clientHelloRecord is a whole first record of a real TLS 1.3 dial offering
// corsa/2: what a dialler sends before it waits for the ServerHello.
func clientHelloRecord(t *testing.T) []byte {
	t.Helper()
	dialler, listener := net.Pipe()
	defer func() { _ = listener.Close() }()
	go func() {
		defer func() { _ = dialler.Close() }()
		_ = tls.Client(dialler, &tls.Config{ServerName: "corsa.invalid", NextProtos: []string{"corsa/2"}, MinVersion: tls.VersionTLS13}).Handshake()
	}()
	header := make([]byte, 5)
	if _, err := io.ReadFull(listener, header); err != nil {
		t.Fatalf("ClientHello header: %v", err)
	}
	record := make([]byte, 5+int(binary.BigEndian.Uint16(header[3:5])))
	copy(record, header)
	if _, err := io.ReadFull(listener, record[5:]); err != nil {
		t.Fatalf("ClientHello body: %v", err)
	}
	return record
}

// stalledHandshake opens a connection to address that sends a whole
// ClientHello record and then nothing: a v2 handshake that holds its node
// slot until the connection ends or the TLS phase times out.
func stalledHandshake(t *testing.T, address string, record []byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(record); err != nil {
		t.Fatalf("ClientHello: %v", err)
	}
	return conn
}

// partialClientHello opens a connection to address that sends only the first
// bytes of a ClientHello record.
func partialClientHello(t *testing.T, address string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write([]byte{sessionv2.TLSRecordHandshake, 0x03, 0x01}); err != nil {
		t.Fatalf("first bytes: %v", err)
	}
	return conn
}

// A node slot is taken once the whole ClientHello record is in, never at
// accept and never at the first byte: sockets that say nothing and
// ClientHellos that stop halfway hold none, while a whole one does.
func TestOnlyAWholeClientHelloTakesANodeHandshakeSlot(t *testing.T) {
	t.Parallel()
	listener, stop := startQuietNode(t, nil)
	t.Cleanup(stop)
	for range 64 + 1 {
		conn, err := net.Dial("tcp", listener.cfg.ListenAddress)
		if err != nil {
			t.Fatalf("silent dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
	}
	partialClientHello(t, listener.cfg.ListenAddress)
	stalledHandshake(t, listener.cfg.ListenAddress, clientHelloRecord(t))
	waitForConditionMsg(t, 5*time.Second, "the whole ClientHello never took its slot", func() bool {
		return listener.inboundHandshakes.running() == 1
	})
	time.Sleep(100 * time.Millisecond)
	if running := listener.inboundHandshakes.running(); running != 1 {
		t.Fatalf("running = %d: a silent socket or a partial ClientHello took a node slot", running)
	}

	dialer, stopDialer := startQuietNode(t, nil)
	t.Cleanup(stopDialer)
	if established, err := dialPastTransport(t, dialer, domain.PeerAddress(listener.cfg.ListenAddress)); !established {
		t.Fatalf("a v2 dial next to 65 silent sockets did not get in: %v", err)
	}
	waitForConditionMsg(t, 5*time.Second, "the finished handshake kept its slot", func() bool {
		return listener.inboundHandshakes.running() == 1
	})
}

// A ClientHello trickled a byte at a time holds its source slot only for the
// record deadline, never a node slot, and is then closed — well before the
// TLS phase would have ended it.
func TestASlowClientHelloHoldsItsSourceOnlyForTheRecordDeadline(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(*Service) {})
	server, client := net.Pipe()
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		svc.handleConn(sourcedConn{Conn: server, remote: addrOf("203.0.113.7")}, nil)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-handled
	})

	started := time.Now()
	ended := make(chan time.Duration, 1)
	go func() {
		record := clientHelloRecord(t)
		for _, b := range record {
			if _, err := client.Write([]byte{b}); err != nil {
				ended <- time.Since(started)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		ended <- 0
	}()
	waitForConditionMsg(t, time.Second, "the trickle never took its source slot", func() bool {
		return svc.inboundHandshakes.heldBy("203.0.113.7") == 1
	})
	var took time.Duration
	select {
	case took = <-ended:
	case <-time.After(clientHelloRecordTimeout + 2*time.Second):
		t.Fatal("the trickling ClientHello was not closed")
	}
	if took == 0 || took < clientHelloRecordTimeout-200*time.Millisecond || took > clientHelloRecordTimeout+time.Second {
		t.Fatalf("closed after %v, want about the record deadline %v", took, clientHelloRecordTimeout)
	}
	<-handled
	if held, running := svc.inboundHandshakes.heldBy("203.0.113.7"), svc.inboundHandshakes.running(); held != 0 || running != 0 {
		t.Fatalf("after the close: source %d, node %d slots held, want none", held, running)
	}
}

// The node bound: with every node slot held, the next whole ClientHello gets
// exactly the alert; slots come back when handshakes fail and when they
// succeed.
func TestTheNodeBoundOfV2HandshakesRefusesWithTheAlertAndFreesOnEveryOutcome(t *testing.T) {
	t.Parallel()
	const bound = 8
	listener, stop := startQuietNode(t, func(svc *Service) {
		svc.inboundHandshakes = newInboundHandshakes(handshakeBounds{node: bound, loopback: bound, perSource: 4})
	})
	t.Cleanup(stop)
	record := clientHelloRecord(t)
	stalled := make([]net.Conn, 0, bound)
	for range bound {
		stalled = append(stalled, stalledHandshake(t, listener.cfg.ListenAddress, record))
	}
	waitForConditionMsg(t, 5*time.Second, "the stalled handshakes never took every slot", func() bool {
		return listener.inboundHandshakes.running() == bound
	})

	refused := stalledHandshake(t, listener.cfg.ListenAddress, record)
	got, err := readAnswer(t, refused, 5*time.Second)
	if err != nil || !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("the handshake over the node bound read % x then %v, want exactly the alert", got, err)
	}
	if stats := listener.ListenerRefusalStats(); stats.HandshakesNode != 1 {
		t.Fatalf("stats = %+v, want one node-bound refusal", stats)
	}

	for _, conn := range stalled {
		_ = conn.Close()
	}
	waitForConditionMsg(t, 5*time.Second, "failed handshakes kept their slots", func() bool {
		return listener.inboundHandshakes.running() == 0
	})

	dialer, stopDialer := startQuietNode(t, nil)
	t.Cleanup(stopDialer)
	if established, err := dialPastTransport(t, dialer, domain.PeerAddress(listener.cfg.ListenAddress)); !established {
		t.Fatalf("a v2 dial after the bound cleared did not get in: %v", err)
	}
	waitForConditionMsg(t, 5*time.Second, "a successful handshake kept its slot", func() bool {
		return listener.inboundHandshakes.running() == 0
	})
}

// Loopback sources — the onion entry — share a part of the node bound: once
// they hold it, the next loopback ClientHello gets the alert, and the rest of
// the node bound stays for everybody else.
func TestLoopbackHandshakesHoldOnlyTheirShareOfTheNodeBound(t *testing.T) {
	t.Parallel()
	listener, stop := startQuietNode(t, nil)
	t.Cleanup(stop)
	record := clientHelloRecord(t)
	share := productionHandshakeBounds().loopback
	for range share {
		stalledHandshake(t, listener.cfg.ListenAddress, record)
	}
	waitForConditionMsg(t, 5*time.Second, "the loopback handshakes never took their share", func() bool {
		return listener.inboundHandshakes.running() == share
	})
	refused := stalledHandshake(t, listener.cfg.ListenAddress, record)
	got, err := readAnswer(t, refused, 5*time.Second)
	if err != nil || !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("the loopback handshake over its share read % x then %v, want exactly the alert", got, err)
	}
	if stats := listener.ListenerRefusalStats(); stats.HandshakesLoopback != 1 || stats.HandshakesNode != 0 {
		t.Fatalf("stats = %+v, want one loopback-share refusal", stats)
	}
}

// The per-source bound of v2 handshakes: a fifth concurrent ClientHello from
// one IPv4 address, or from any addresses of one IPv6 /64, gets the alert,
// another source does not, and with the node's per-IP limits off the bound
// does not apply — the same switch as maxConnPerIP.
func TestThePerSourceBoundOfV2Handshakes(t *testing.T) {
	t.Parallel()
	// handshakeFrom hands a connection from ip to the node's handler and
	// sends the first bytes of a ClientHello on it.
	handshakeFrom := func(t *testing.T, svc *Service, ip string) net.Conn {
		t.Helper()
		server, client := net.Pipe()
		handled := make(chan struct{})
		go func() {
			defer close(handled)
			svc.handleConn(sourcedConn{Conn: server, remote: addrOf(ip)}, nil)
		}()
		t.Cleanup(func() {
			_ = client.Close()
			<-handled
		})
		go func() { _, _ = client.Write([]byte{sessionv2.TLSRecordHandshake, 0x03, 0x01}) }()
		return client
	}

	for name, tc := range map[string]struct {
		group  string
		sample []string
		next   string
	}{
		"IPv4 address": {"203.0.113.7", []string{"203.0.113.7", "203.0.113.7", "203.0.113.7", "203.0.113.7"}, "203.0.113.7"},
		"IPv6 /64":     {"2001:db8:5:6::/64", []string{"2001:db8:5:6::1", "2001:db8:5:6::2", "2001:db8:5:6:1::3", "2001:db8:5:6:ffff:ffff:ffff:fffe"}, "2001:db8:5:6::77"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := refusingNode(t, config.Node{}, func(*Service) {})
			held := make([]net.Conn, 0, len(tc.sample))
			for _, ip := range tc.sample {
				held = append(held, handshakeFrom(t, svc, ip))
			}
			waitForConditionMsg(t, clientHelloRecordTimeout/2, "the handshakes never took their source slots", func() bool {
				return svc.inboundHandshakes.heldBy(tc.group) == len(tc.sample)
			})

			fifth := handshakeFrom(t, svc, tc.next)
			got, err := readAnswer(t, fifth, 5*time.Second)
			if err != nil || !bytes.Equal(got, sessionv2.RefusalAlert()) {
				t.Fatalf("the fifth handshake from one source read % x then %v, want exactly the alert", got, err)
			}
			if stats := svc.ListenerRefusalStats(); stats.HandshakesPerIP != 1 {
				t.Fatalf("stats = %+v, want one per-source handshake refusal", stats)
			}
			handshakeFrom(t, svc, "198.51.100.9")
			for _, conn := range held {
				_ = conn.Close()
			}
			waitForConditionMsg(t, 5*time.Second, "failed handshakes kept their source slots", func() bool {
				return svc.inboundHandshakes.heldBy(tc.group) == 0
			})
			if stats := svc.ListenerRefusalStats(); stats.HandshakesPerIP != 1 {
				t.Fatalf("stats = %+v: another source was refused", stats)
			}
		})
	}

	t.Run("limits off", func(t *testing.T) {
		t.Parallel()
		svc, stop := startQuietNode(t, nil)
		t.Cleanup(stop)
		for range productionHandshakeBounds().perSource + 1 {
			handshakeFrom(t, svc, "203.0.113.7")
		}
		time.Sleep(200 * time.Millisecond)
		if stats := svc.ListenerRefusalStats(); stats.HandshakesPerIP != 0 {
			t.Fatalf("stats = %+v, want no per-source handshake refusal with per-IP limits off", stats)
		}
	})
}

// A dialler refused by a busy listener's alert is not charged for it: the
// address keeps its score and failure count, gets no v1 mark, and its next v2
// attempt waits for the v2 retry backoff instead of following at once.
func TestAListenerRefusalIsNotChargedAndHoldsTheNextV2Attempt(t *testing.T) {
	t.Parallel()
	listener, stop := startQuietNode(t, func(svc *Service) {
		svc.inboundHandshakes = newInboundHandshakes(handshakeBounds{node: 0, loopback: 0, perSource: 4})
	})
	t.Cleanup(stop)
	dialer, stopDialer := startQuietNode(t, nil)
	t.Cleanup(stopDialer)
	address := domain.PeerAddress(listener.cfg.ListenAddress)

	established, err := dialPastTransport(t, dialer, address)
	if established || !errors.Is(err, errV2RefusedByListener) {
		t.Fatalf("dial = %v, want errV2RefusedByListener", err)
	}
	dialer.onCMDialFailed(address, err, false)

	dialer.peerMu.RLock()
	health := dialer.health[address]
	setup := dialer.setupFailures[address]
	dialer.peerMu.RUnlock()
	if health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0 || !health.BannedUntil.IsZero()) {
		t.Fatalf("the refusal was charged: failures %d, score %d, cooldown until %v", health.ConsecutiveFailures, health.Score, health.BannedUntil)
	}
	if setup != nil && setup.Consecutive > 0 {
		t.Fatalf("the refusal was counted as a setup failure: %+v", *setup)
	}
	if dialer.secureSessions.marks.v1Seen(address) {
		t.Fatal("a TLS refusal marked the address as an old node")
	}
	if !dialer.v2RetryHeld(address) {
		t.Fatal("the next v2 attempt is not held: a busy listener would be redialled at once")
	}
	if _, err := dialPastTransport(t, dialer, address); !errors.Is(err, errV2RetryHeld) {
		t.Fatalf("the next dial = %v, want it held by the v2 retry backoff", err)
	}
}

// The ClientHello record is read whole and as it came, and a first record
// that cannot be one is refused before any slot of the node is taken.
func TestTheClientHelloRecordIsReadWholeOrRefused(t *testing.T) {
	t.Parallel()
	record := clientHelloRecord(t)
	got, err := readTLSRecord(bufio.NewReader(bytes.NewReader(append(append([]byte{}, record...), 0x99))))
	if err != nil || !bytes.Equal(got, record) {
		t.Fatalf("read %d bytes (%v), want the %d-byte record and nothing after it", len(got), err, len(record))
	}
	for name, header := range map[string][]byte{
		"empty":         {0x16, 0x03, 0x01, 0x00, 0x00},
		"over 2^14":     {0x16, 0x03, 0x01, 0x40, 0x01},
		"not handshake": {0x17, 0x03, 0x03, 0x00, 0x10},
	} {
		if _, err := readTLSRecord(bufio.NewReader(bytes.NewReader(header))); !errors.Is(err, errClientHelloRecord) {
			t.Fatalf("%s: %v, want errClientHelloRecord", name, err)
		}
	}
}

// The wait after a listener's refusal is the v2 retry backoff plus a jitter
// of at most half of it, and the jitter never accumulates: each wait is
// counted from now, so repeated refusals stay within the backoff's ceiling
// plus half of it.
func TestTheListenerRefusalWaitIsTheV2BackoffPlusABoundedJitter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	marks := newSessionAddressMarks(func() time.Time { return now })
	var asked []time.Duration
	// The largest jitter the contract allows: just under its bound.
	marks.jitter = func(upTo time.Duration) time.Duration {
		asked = append(asked, upTo)
		return upTo - time.Nanosecond
	}
	const address = domain.PeerAddress("203.0.113.7:64646")

	first := marks.noteListenerRefused(address)
	if first < v2RetryBase || first >= v2RetryBase+v2RetryBase/2 {
		t.Fatalf("first wait = %v, want within [%v, %v)", first, v2RetryBase, v2RetryBase+v2RetryBase/2)
	}
	if asked[0] != v2RetryBase/2 {
		t.Fatalf("jitter bound = %v, want half the backoff %v", asked[0], v2RetryBase/2)
	}
	var wait time.Duration
	for range 20 {
		wait = marks.noteListenerRefused(address)
		if wait >= v2RetryMax+v2RetryMax/2 {
			t.Fatalf("wait = %v, want under the ceiling plus half of it (%v): the jitter accumulates", wait, v2RetryMax+v2RetryMax/2)
		}
	}
	if wait < v2RetryMax {
		t.Fatalf("wait after 21 refusals = %v, want at least the ceiling %v", wait, v2RetryMax)
	}
	if got := asked[len(asked)-1]; got != v2RetryMax/2 {
		t.Fatalf("jitter bound at the ceiling = %v, want %v", got, v2RetryMax/2)
	}
	if v2RetryBase != 30*time.Second || v2RetryMax != 30*time.Minute {
		t.Fatalf("v2 retry backoff = %v…%v, want 30s…30min", v2RetryBase, v2RetryMax)
	}
}

// Loopback sources — the onion entry, many clients behind 127.0.0.1 — are not
// held to one source's share of the alert senders: only the pool bounds them.
func TestLoopbackRefusalsAreBoundOnlyByTheAlertPool(t *testing.T) {
	t.Parallel()
	refusals := newListenerRefusals(alertPoolBounds{total: 10, perSource: 2}, time.Now())
	clients := make([]net.Conn, 0, 6)
	for range 6 {
		server, client := tcpPair(t)
		refusals.refuse(server, refusalConnectionRate)
		clients = append(clients, client)
	}
	if stats := refusals.stats(time.Now()); stats.ClosedWithoutAlert != 0 {
		t.Fatalf("ClosedWithoutAlert = %d, want 0: loopback refusals were held to one source's share", stats.ClosedWithoutAlert)
	}
	refusals.wait()
	for _, client := range clients {
		if got, _ := readAnswer(t, client, 3*time.Second); !bytes.Equal(got, sessionv2.RefusalAlert()) {
			t.Fatalf("a loopback dialler read % x, want the alert", got)
		}
	}
}

// running is how many handshakes hold a node slot.
func (h *inboundHandshakes) running() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inFlight
}

// heldBy is how many source slots group holds.
func (h *inboundHandshakes) heldBy(group string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.bySource[group]
}
