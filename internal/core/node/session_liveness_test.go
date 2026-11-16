package node

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// session_liveness_test.go pins the agreement between the heartbeat schedule
// and the read bounds of a session: a healthy session that carries nothing
// but heartbeats lives on in either direction, while a peer that stops
// answering is still dropped within one heartbeat
// cycle.
//
// The nodes are real and talk over loopback TCP; only the clock of the
// schedule is compressed. Every duration of the schedule is divided by the
// same factor, so the ratios — which are the contract — are the production
// ones, and every ping takes the largest jitter, the schedule's worst case.

// livenessCompression is how many times faster than production the
// schedule runs in these tests. It keeps the slack of the production
// schedule (tens of seconds) above the scheduling noise of a loaded test run.
const livenessCompression = 20

// compressedWorstCase is l with every duration divided by
// livenessCompression and every ping taking the largest jitter.
func compressedWorstCase(l livenessSchedule) livenessSchedule {
	l.diallerPingInterval /= livenessCompression
	l.listenerPingInterval /= livenessCompression
	l.maxPingJitter /= livenessCompression
	l.pongStall /= livenessCompression
	l.inboundFrameRead /= livenessCompression
	l.inboundSessionIdle /= livenessCompression
	l.pickJitter = func(max time.Duration) time.Duration { return max }
	return l
}

// withLiveness installs l on a node before Run.
func withLiveness(l livenessSchedule) func(*Service) {
	return func(svc *Service) { svc.liveness = &l }
}

// sessionsEstablished is how many authenticated sessions svc has set up
// since it started, both kinds and both directions.
func sessionsEstablished(svc *Service) uint64 {
	k := svc.SessionKindStats()
	return k.InboundV2 + k.InboundLegacy + k.OutboundV2 + k.OutboundLegacy
}

// awaitSessionsSettled waits until no node has set up a new session for a
// second, and returns how many each has set up by then.
func awaitSessionsSettled(t *testing.T, nodes map[string]*Service) map[string]uint64 {
	t.Helper()
	const quiet = time.Second
	deadline := time.Now().Add(15 * time.Second)
	last := make(map[string]uint64, len(nodes))
	for name, svc := range nodes {
		last[name] = sessionsEstablished(svc)
	}
	stableSince := time.Now()
	for time.Since(stableSince) < quiet {
		if time.Now().After(deadline) {
			t.Fatalf("sessions never settled: %v", last)
		}
		time.Sleep(50 * time.Millisecond)
		for name, svc := range nodes {
			if now := sessionsEstablished(svc); now != last[name] {
				last[name] = now
				stableSince = time.Now()
			}
		}
	}
	return last
}

// requireNoSessionReestablished fails as soon as any node sets up a session
// during window: on an idle mesh nothing but a broken session makes a node
// set up another one.
func requireNoSessionReestablished(t *testing.T, window time.Duration, nodes map[string]*Service, settled map[string]uint64) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < window {
		time.Sleep(50 * time.Millisecond)
		for name, svc := range nodes {
			if now := sessionsEstablished(svc); now != settled[name] {
				elapsed := time.Since(start) * livenessCompression
				t.Fatalf("%s set up a session again %v (production time) into an idle window of %v: established %d → %d — an idle healthy session was torn down",
					name, elapsed.Round(time.Second), window*livenessCompression, settled[name], now)
			}
		}
	}
}

// idleWindow is several heartbeat cycles of the slowest side of l, at the
// largest jitter.
func idleWindow(l livenessSchedule) time.Duration {
	slowest := max(l.diallerPingInterval, l.listenerPingInterval) + l.maxPingJitter
	return 4 * slowest
}

// Two nodes of this build over v2, one dialling and one listening, exchange
// nothing but heartbeats for four heartbeat cycles: the session — both its
// ends — stays up and the dialler never needs a second connection.
func TestAnIdleV2SessionOutlivesSeveralHeartbeatCycles(t *testing.T) {
	t.Parallel()
	liveness := compressedWorstCase(productionLiveness())

	listenAddress := freeAddress(t)
	proxy := newCountingProxy(t, listenAddress)
	proxyAddress := proxy.ln.Addr().String()
	listener, stopListener := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  listenAddress,
		AdvertisePort:  peerPortOf(t, proxyAddress),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	}, withLiveness(liveness))
	t.Cleanup(stopListener)
	dialler, stopDialler := startTestNodeWithSetup(t, config.Node{
		ListenerSet:     true,
		ListenerEnabled: false,
		AdvertisePort:   peerPortOf(t, freeAddress(t)),
		BootstrapPeers:  []string{proxyAddress},
		Type:            domain.NodeTypeFull,
	}, withLiveness(liveness))
	t.Cleanup(stopDialler)

	waitForConditionMsg(t, 15*time.Second, "no v2 session between the two nodes", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 == 1 && hasOutboundSession(dialler)
	})
	nodes := map[string]*Service{"dialler": dialler, "listener": listener}
	settled := awaitSessionsSettled(t, nodes)
	pipes := proxy.pipeCount()

	requireNoSessionReestablished(t, idleWindow(liveness), nodes, settled)

	if v2, _ := authenticatedInbound(listener); v2 != 1 || !hasOutboundSession(dialler) {
		t.Fatalf("after the idle window: listener v2 inbound = %d, dialler has outbound session = %v; want the one session alive", v2, hasOutboundSession(dialler))
	}
	if got := proxy.pipeCount(); got != pipes {
		t.Fatalf("the dialler opened %d more connections during the idle window", got-pipes)
	}
}

// A peer that stops answering — the proxy drains both directions and
// forwards nothing — is dropped by each end of the session within that end's
// own bound on a v2 session. They differ because the two ends learn of the
// silence differently:
//
//   - the dialler, from its own heartbeat: its next ping comes at most
//     diallerPingInterval + maxPingJitter after the last pong, and the session
//     ends pongStall after it — 75 s + 14 s + 45 s = 134 s in production;
//   - the listener, from its read bound: no frame within inboundSessionIdle
//     ends the session — 165 s in production — or, should the read deadline
//     not fire, the inbound sweep does at its next tick (peerSweepInterval).
//
// The listener's own heartbeat (2 min + 14 s + 45 s = 179 s) is longer than
// its read bound and never decides. These are the bounds a dead peer may hold
// a session for at each end, and they must not grow while the idle bounds are
// fitted to the schedule.
func TestAnUnresponsivePeerIsDroppedWithinOneHeartbeatCycle(t *testing.T) {
	t.Parallel()
	liveness := compressedWorstCase(productionLiveness())
	production := productionLiveness()
	productionBounds := map[string]time.Duration{
		"dialler":  production.diallerPingInterval + production.maxPingJitter + production.pongStall,
		"listener": production.inboundSessionIdle + peerSweepInterval,
	}
	if want := 134 * time.Second; productionBounds["dialler"] != want {
		t.Fatalf("the dialler's production bound is %v, want %v: the dialler schedule changed", productionBounds["dialler"], want)
	}
	if want := 165*time.Second + peerSweepInterval; productionBounds["listener"] != want {
		t.Fatalf("the listener's production bound is %v, want %v: the inbound idle bound or the sweep tick changed", productionBounds["listener"], want)
	}
	const slack = time.Second
	bounds := make(map[string]time.Duration, len(productionBounds))
	for end, bound := range productionBounds {
		bounds[end] = bound/livenessCompression + slack
	}

	listenAddress := freeAddress(t)
	proxy := newCountingProxy(t, listenAddress)
	proxyAddress := proxy.ln.Addr().String()
	listener, stopListener := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  listenAddress,
		AdvertisePort:  peerPortOf(t, proxyAddress),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	}, withLiveness(liveness))
	t.Cleanup(stopListener)
	dialler, stopDialler := startTestNodeWithSetup(t, config.Node{
		ListenerSet:     true,
		ListenerEnabled: false,
		AdvertisePort:   peerPortOf(t, freeAddress(t)),
		BootstrapPeers:  []string{proxyAddress},
		Type:            domain.NodeTypeFull,
	}, withLiveness(liveness))
	t.Cleanup(stopDialler)

	waitForConditionMsg(t, 15*time.Second, "no v2 session between the two nodes", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 == 1 && hasOutboundSession(dialler)
	})
	awaitSessionsSettled(t, map[string]*Service{"dialler": dialler, "listener": listener})

	pipe := proxy.last()
	requirePipeOpen(t, "before the peer goes silent", pipe)
	// No redial may reach the listener: a new session of the same
	// identity would end the old connection for its own reason.
	_ = proxy.ln.Close()
	silentAt := time.Now()
	pipe.holdToServer.Store(true)
	pipe.holdToClient.Store(true)

	ends := map[string]<-chan struct{}{
		"dialler":  pipe.clientToServerDone,
		"listener": pipe.serverToClientDone,
	}
	for len(ends) > 0 {
		for name, closed := range ends {
			select {
			case <-closed:
				t.Logf("the %s dropped the silent peer after %v (production %v)", name,
					time.Since(silentAt).Round(time.Millisecond), (time.Since(silentAt) * livenessCompression).Round(time.Second))
				delete(ends, name)
				continue
			default:
			}
			if elapsed := time.Since(silentAt); elapsed > bounds[name] {
				t.Fatalf("%v after the peer went silent the %s still holds the session; its bound is %v in production",
					elapsed.Round(time.Millisecond), name, productionBounds[name])
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// On an inbound session a frame that has started must end within the frame
// bound, not the idle bound: a peer that sends the first bytes of a TLS
// record and stops is dropped as soon as the frame bound runs out.
// The schedule here makes the idle bound far longer than the frame bound and
// than the heartbeat cycle, so only the frame bound can explain a close.
func TestAStartedFrameOnAnIdleSessionIsBoundByTheFrameBound(t *testing.T) {
	t.Parallel()
	liveness := compressedWorstCase(productionLiveness())
	liveness.inboundFrameRead = time.Second
	liveness.inboundSessionIdle = time.Minute
	const slack = time.Second

	listenAddress := freeAddress(t)
	proxy := newCountingProxy(t, listenAddress)
	proxyAddress := proxy.ln.Addr().String()
	listener, stopListener := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  listenAddress,
		AdvertisePort:  peerPortOf(t, proxyAddress),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	}, withLiveness(liveness))
	t.Cleanup(stopListener)
	dialler, stopDialler := startTestNodeWithSetup(t, config.Node{
		ListenerSet:     true,
		ListenerEnabled: false,
		AdvertisePort:   peerPortOf(t, freeAddress(t)),
		BootstrapPeers:  []string{proxyAddress},
		Type:            domain.NodeTypeFull,
	}, withLiveness(liveness))
	t.Cleanup(stopDialler)

	waitForConditionMsg(t, 15*time.Second, "no v2 session between the two nodes", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 == 1 && hasOutboundSession(dialler)
	})
	awaitSessionsSettled(t, map[string]*Service{"dialler": dialler, "listener": listener})

	pipe := proxy.last()
	requirePipeOpen(t, "before the partial frame", pipe)
	_ = proxy.ln.Close()
	pipe.holdToServer.Store(true)
	startedAt := time.Now()
	// An incomplete TLS application-data record.
	partial := []byte{0x17, 0x03, 0x03, 0x00, 0x40, 0x01, 0x02, 0x03}
	if _, err := pipe.server.Write(partial); err != nil {
		t.Fatalf("write the partial frame: %v", err)
	}
	select {
	case <-pipe.serverToClientDone:
		t.Logf("the listener dropped the stalled frame after %v", time.Since(startedAt).Round(time.Millisecond))
	case <-time.After(liveness.inboundFrameRead + slack):
		t.Fatalf("the listener still waits for a frame that started %v ago; the frame bound is %v", time.Since(startedAt).Round(time.Millisecond), liveness.inboundFrameRead)
	}
}

// A connection that never becomes a session — silent from its first byte,
// or stopping inside its first frame — is dropped within the per-frame bound
// it always had: fitting the idle bound of a SESSION to the heartbeat must
// not lengthen how long a stalled handshake holds a connection slot.
func TestAStalledHandshakeIsDroppedWithinTheSetupBound(t *testing.T) {
	t.Parallel()
	liveness := compressedWorstCase(productionLiveness())
	bound := liveness.inboundFrameRead + time.Second

	stalls := []struct {
		name  string
		bytes []byte
	}{
		{"silent", nil},
		{"inside the TLS handshake", []byte{0x16, 0x03, 0x01, 0x00, 0x40, 0x01}},
	}
	for _, stall := range stalls {
		t.Run(stall.name, func(t *testing.T) {
			t.Parallel()
			listenAddress := freeAddress(t)
			_, stop := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  listenAddress,
				BootstrapPeers: []string{},
				Type:           domain.NodeTypeFull,
			}, withLiveness(liveness))
			t.Cleanup(stop)

			conn, err := net.Dial("tcp", listenAddress)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			openedAt := time.Now()
			if len(stall.bytes) != 0 {
				if _, err := conn.Write(stall.bytes); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			if err := conn.SetReadDeadline(openedAt.Add(bound)); err != nil {
				t.Fatalf("deadline: %v", err)
			}
			buf := make([]byte, 4096)
			for {
				if _, err := conn.Read(buf); err != nil {
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						t.Fatalf("the node still holds a stalled handshake %v after it opened; the bound is %v in production", time.Since(openedAt).Round(time.Millisecond), productionLiveness().inboundFrameRead)
					}
					return
				}
			}
		})
	}
}

// The TLS dialler heartbeat fits the listener's idle bound with room for
// message delivery and scheduling.
func TestTheProductionTLSHeartbeatFitsInboundDeadline(t *testing.T) {
	t.Parallel()
	const deliveryMargin = 30 * time.Second
	schedule := productionLiveness()
	diallerGap := schedule.diallerPingInterval + schedule.maxPingJitter
	if diallerGap+deliveryMargin > schedule.inboundSessionIdle {
		t.Errorf("dialler pings up to %v apart; inbound idle bound %v lacks room for delivery",
			diallerGap, schedule.inboundSessionIdle)
	}

	if schedule.inboundFrameRead > 120*time.Second {
		t.Errorf("the frame bound exceeds 120s: %v", schedule.inboundFrameRead)
	}
	if schedule.pongStall > 45*time.Second {
		t.Errorf("the pong stall exceeds 45s: %v", schedule.pongStall)
	}
	slowestCycle := max(schedule.diallerPingInterval, schedule.listenerPingInterval) + schedule.maxPingJitter + schedule.pongStall
	if slowestCycle > 179*time.Second {
		t.Errorf("a dead peer may hold a TLS session for %v, want at most 179s", slowestCycle)
	}
}

// compressedWorstCase compresses every duration of the schedule: a field it
// forgets would keep its production value and the tests above would measure
// a mixture of clocks.
func TestCompressedWorstCaseCompressesEveryDuration(t *testing.T) {
	t.Parallel()
	production := productionLiveness()
	compressed := compressedWorstCase(production)
	p, c := reflect.ValueOf(production), reflect.ValueOf(compressed)
	durationType := reflect.TypeFor[time.Duration]()
	for i := range p.NumField() {
		field := p.Type().Field(i)
		if field.Type != durationType {
			continue
		}
		want := time.Duration(p.Field(i).Int()) / livenessCompression
		if got := time.Duration(c.Field(i).Int()); got != want {
			t.Errorf("%s = %v, want %v", field.Name, got, want)
		}
	}
}
