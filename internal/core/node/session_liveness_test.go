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
// but heartbeats lives on, in either direction and with a released peer,
// while a peer that stops answering is still dropped within one heartbeat
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

// releasedLiveness is the schedule of every released build that speaks only
// v1 (v28 … v30, v1.0.64 … v2.4.70): both sides ping every 2 min plus up to
// 14 s, a pong is awaited 45 s, and an inbound connection is closed when no
// whole frame arrived within 120 s of the start of the wait for it — one
// deadline per frame, session or not.
func releasedLiveness() livenessSchedule {
	return livenessSchedule{
		diallerPingInterval:  2 * time.Minute,
		listenerPingInterval: 2 * time.Minute,
		maxPingJitter:        14 * time.Second,
		pongStall:            45 * time.Second,
		inboundFrameRead:     120 * time.Second,
		inboundSessionIdle:   120 * time.Second,
		pickJitter:           uniformPingJitter,
	}
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

// A node of this build and an emulated released build (v28, v29, v30) — the
// old node running the released schedule — exchange nothing but heartbeats
// for four heartbeat cycles over the two legacy sessions between them: the
// one the new node dialled (an old listener reads it with the released
// 120-second per-frame deadline) and the one the old node dialled (its pings
// come on the released schedule). Neither is torn down.
func TestAnIdleLegacySessionOutlivesSeveralHeartbeatCycles(t *testing.T) {
	t.Parallel()
	newLiveness := compressedWorstCase(productionLiveness())
	oldLiveness := compressedWorstCase(releasedLiveness())
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			pair := startLegacyPairWithSetup(t, version, withLiveness(newLiveness), withLiveness(oldLiveness))

			waitForConditionMsg(t, 20*time.Second, "no legacy session each way between the node and the old peer", func() bool {
				kinds := pair.new.SessionKindStats()
				return kinds.InboundLegacy > 0 && kinds.OutboundLegacy > 0
			})
			nodes := map[string]*Service{"new node": pair.new, "old node": pair.old}
			settled := awaitSessionsSettled(t, nodes)

			requireNoSessionReestablished(t, idleWindow(oldLiveness), nodes, settled)

			if _, v1 := authenticatedInbound(pair.new); v1 == 0 {
				t.Fatal("after the idle window the new node has no legacy inbound session")
			}
			if _, v1 := authenticatedInbound(pair.old); v1 == 0 {
				t.Fatal("after the idle window the old node has no legacy inbound session")
			}
		})
	}
}

// A peer that stops answering — the proxy drains both directions and
// forwards nothing — is dropped by each end of the session within that end's
// own bound, for v2 and v1. They differ because the two ends learn of the
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

	kinds := []struct {
		name  string
		setup func(*Service)
	}{
		{"v2", withLiveness(liveness)},
		{"v1", func(svc *Service) { asOldNode(svc); withLiveness(liveness)(svc) }},
	}
	for _, kind := range kinds {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()
			listenAddress := freeAddress(t)
			proxy := newCountingProxy(t, listenAddress)
			proxyAddress := proxy.ln.Addr().String()
			listener, stopListener := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  listenAddress,
				AdvertisePort:  peerPortOf(t, proxyAddress),
				BootstrapPeers: []string{},
				Type:           domain.NodeTypeFull,
			}, kind.setup)
			t.Cleanup(stopListener)
			dialler, stopDialler := startTestNodeWithSetup(t, config.Node{
				ListenerSet:     true,
				ListenerEnabled: false,
				AdvertisePort:   peerPortOf(t, freeAddress(t)),
				BootstrapPeers:  []string{proxyAddress},
				Type:            domain.NodeTypeFull,
			}, kind.setup)
			t.Cleanup(stopDialler)

			waitForConditionMsg(t, 15*time.Second, "no session between the two nodes", func() bool {
				v2, v1 := authenticatedInbound(listener)
				return v2+v1 == 1 && hasOutboundSession(dialler)
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
		})
	}
}

// On an inbound session a frame that has started must end within the frame
// bound, not the idle bound: a peer that sends the first bytes of a frame and
// stops is dropped as soon as the frame bound runs out, for v1 and for v2,
// where the bytes are a TLS record the reader cannot open until it is whole.
// The schedule here makes the idle bound far longer than the frame bound and
// than the heartbeat cycle, so only the frame bound can explain a close.
func TestAStartedFrameOnAnIdleSessionIsBoundByTheFrameBound(t *testing.T) {
	t.Parallel()
	liveness := compressedWorstCase(productionLiveness())
	liveness.inboundFrameRead = time.Second
	liveness.inboundSessionIdle = time.Minute
	const slack = time.Second

	kinds := []struct {
		name    string
		setup   func(*Service)
		partial []byte
	}{
		// A TLS application-data record that announces 64 bytes and
		// delivers three.
		{"v2", withLiveness(liveness), []byte{0x17, 0x03, 0x03, 0x00, 0x40, 0x01, 0x02, 0x03}},
		{"v1", func(svc *Service) { asOldNode(svc); withLiveness(liveness)(svc) }, []byte(`{"type":"ping","node":`)},
	}
	for _, kind := range kinds {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()
			listenAddress := freeAddress(t)
			proxy := newCountingProxy(t, listenAddress)
			proxyAddress := proxy.ln.Addr().String()
			listener, stopListener := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  listenAddress,
				AdvertisePort:  peerPortOf(t, proxyAddress),
				BootstrapPeers: []string{},
				Type:           domain.NodeTypeFull,
			}, kind.setup)
			t.Cleanup(stopListener)
			dialler, stopDialler := startTestNodeWithSetup(t, config.Node{
				ListenerSet:     true,
				ListenerEnabled: false,
				AdvertisePort:   peerPortOf(t, freeAddress(t)),
				BootstrapPeers:  []string{proxyAddress},
				Type:            domain.NodeTypeFull,
			}, kind.setup)
			t.Cleanup(stopDialler)

			waitForConditionMsg(t, 15*time.Second, "no session between the two nodes", func() bool {
				v2, v1 := authenticatedInbound(listener)
				return v2+v1 == 1 && hasOutboundSession(dialler)
			})
			awaitSessionsSettled(t, map[string]*Service{"dialler": dialler, "listener": listener})

			pipe := proxy.last()
			requirePipeOpen(t, "before the partial frame", pipe)
			_ = proxy.ln.Close()
			pipe.holdToServer.Store(true)
			startedAt := time.Now()
			if _, err := pipe.server.Write(kind.partial); err != nil {
				t.Fatalf("write the partial frame: %v", err)
			}
			select {
			case <-pipe.serverToClientDone:
				t.Logf("the listener dropped the stalled frame after %v", time.Since(startedAt).Round(time.Millisecond))
			case <-time.After(liveness.inboundFrameRead + slack):
				t.Fatalf("the listener still waits for a frame that started %v ago; the frame bound is %v", time.Since(startedAt).Round(time.Millisecond), liveness.inboundFrameRead)
			}
		})
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

	modes := []struct {
		name  string
		setup func(*Service)
	}{
		{"v2 node", withLiveness(liveness)},
		{"v1-only node", func(svc *Service) { asOldNode(svc); withLiveness(liveness)(svc) }},
	}
	stalls := []struct {
		name  string
		bytes string
	}{
		{"silent", ""},
		{"inside the first frame", `{"type":"hello","version":`},
	}
	for _, mode := range modes {
		for _, stall := range stalls {
			t.Run(mode.name+"/"+stall.name, func(t *testing.T) {
				t.Parallel()
				listenAddress := freeAddress(t)
				_, stop := startTestNodeWithSetup(t, config.Node{
					ListenAddress:  listenAddress,
					BootstrapPeers: []string{},
					Type:           domain.NodeTypeFull,
				}, mode.setup)
				t.Cleanup(stop)

				conn, err := net.Dial("tcp", listenAddress)
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				t.Cleanup(func() { _ = conn.Close() })
				openedAt := time.Now()
				if stall.bytes != "" {
					if _, err := conn.Write([]byte(stall.bytes)); err != nil {
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
}

// The production schedule fits the read bound of every released listener,
// and the read bound of this build fits the schedule of every released
// dialler, each with room for the delivery and scheduling delay of a ping.
func TestTheProductionScheduleFitsReleasedPeers(t *testing.T) {
	t.Parallel()
	// The delay a ping may take on its way — the round trip of the previous
	// pong, the writer queue, the scheduler — before a reader on the far
	// side must still have it.
	const deliveryMargin = 30 * time.Second
	ours := productionLiveness()
	released := releasedLiveness()

	ourDiallerGap := ours.diallerPingInterval + ours.maxPingJitter
	if ourDiallerGap+deliveryMargin > released.inboundSessionIdle {
		t.Errorf("our dialler pings up to %v apart; a released listener drops a connection with no frame for %v, which leaves less than %v for delivery",
			ourDiallerGap, released.inboundSessionIdle, deliveryMargin)
	}
	releasedDiallerGap := released.diallerPingInterval + released.maxPingJitter
	if releasedDiallerGap+deliveryMargin > ours.inboundSessionIdle {
		t.Errorf("a released dialler pings up to %v apart; our listener drops a session with no frame for %v, which leaves less than %v for delivery",
			releasedDiallerGap, ours.inboundSessionIdle, deliveryMargin)
	}
	if ourDiallerGap+deliveryMargin > ours.inboundSessionIdle {
		t.Errorf("our dialler pings up to %v apart; our listener drops a session with no frame for %v", ourDiallerGap, ours.inboundSessionIdle)
	}
	// What the fit must not cost: the bounds that guard a connection slot
	// against a stalled handshake or a trickled frame stay where they were,
	// and a dead peer is still dropped within one cycle of the slower side.
	if ours.inboundFrameRead > released.inboundFrameRead {
		t.Errorf("the frame bound grew from %v to %v", released.inboundFrameRead, ours.inboundFrameRead)
	}
	if ours.pongStall > released.pongStall {
		t.Errorf("the pong stall grew from %v to %v", released.pongStall, ours.pongStall)
	}
	slowestCycle := max(ours.diallerPingInterval, ours.listenerPingInterval) + ours.maxPingJitter + ours.pongStall
	releasedCycle := max(released.diallerPingInterval, released.listenerPingInterval) + released.maxPingJitter + released.pongStall
	if slowestCycle > releasedCycle {
		t.Errorf("a dead peer may now hold a session %v instead of %v", slowestCycle, releasedCycle)
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
