package node

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// loopback_inbound_test.go: the loopback inbound class at a running node
// (docs/protocol/network_security.md, "Loopback inbound class"). Every
// connection here really arrives from 127.0.0.1 — the onion entry as the
// listener sees it — and the node runs with its production bounds.

// loopbackHelloWelcome opens a loopback v1 connection as a fresh identity,
// sends its hello and returns the connection once the listener answered it
// with a welcome: the connection was admitted and holds its slot of the
// class.
func loopbackHelloWelcome(t *testing.T, address string) net.Conn {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	writeJSONFrame(t, conn, legacyNodeHello(id))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		_ = conn.Close()
		t.Fatalf("the listener did not answer the hello: %v", err)
	}
	frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
	if err != nil || frame.Type != "welcome" {
		_ = conn.Close()
		t.Fatalf("answer to the hello: %q (%v), want a welcome", line, err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn
}

// legacyNodeHello is the v1 hello of a node named id.
func legacyNodeHello(id *identity.Identity) protocol.Frame {
	return protocol.Frame{
		Type:          "hello",
		Version:       config.ProtocolVersion,
		Client:        "node",
		ClientVersion: config.CorsaVersion,
		Address:       id.Address,
		PubKey:        identity.PublicKeyBase64(id.PublicKey),
		BoxKey:        identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:        identity.SignBoxKeyBinding(id),
	}
}

// legacyBadAuth runs one legacy session from loopback that names id and fails
// its auth_session signature, and returns the frame the listener answered the
// signature with and the reader positioned after it.
func legacyBadAuth(t *testing.T, address string, id *identity.Identity) (net.Conn, *bufio.Reader, protocol.Frame) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	writeJSONFrame(t, conn, legacyNodeHello(id))
	if welcome := readJSONTestFrame(t, reader); welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("hello answered with %#v, want a welcome with a challenge", welcome)
	}
	writeJSONFrame(t, conn, protocol.Frame{Type: "auth_session", Address: id.Address, Signature: "bad-signature"})
	return conn, reader, readJSONTestFrame(t, reader)
}

// endsWithin reports whether the listener ends conn within the bound.
func endsWithin(conn net.Conn, reader io.Reader, within time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(within))
	_, err := io.Copy(io.Discard, reader)
	return err == nil
}

// A legacy peer that fails its signature again and again from loopback loses
// only its own connections: each is closed after the error reply, loopback
// is never scored or banned, and an honest loopback peer still gets in. The
// identity the hellos claim is not charged either — nothing proved it.
func TestALoopbackLegacyViolatorLosesItsConnectionAndBansNobody(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(*Service) {})
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	// One more than it took to blacklist an IP: a ban would have landed.
	attempts := banThreshold/banIncrementInvalidSig + 1
	for i := range attempts {
		conn, reader, reply := legacyBadAuth(t, svc.cfg.ListenAddress, id)
		if reply.Code != protocol.ErrCodeInvalidAuthSignature {
			_ = conn.Close()
			t.Fatalf("attempt %d: reply %#v, want the invalid-signature error and no ban notice", i, reply)
		}
		if !endsWithin(conn, reader, 5*time.Second) {
			_ = conn.Close()
			t.Fatalf("attempt %d: the violating connection was not closed", i)
		}
		_ = conn.Close()
	}

	svc.ipStateMu.RLock()
	entry, scored := svc.bans["127.0.0.1"]
	svc.ipStateMu.RUnlock()
	if scored {
		t.Fatalf("loopback was scored by its IP: %+v", entry)
	}
	stats := svc.LoopbackInboundStats()
	if stats.Refusals.ViolationsClosed != uint64(attempts) || stats.IdentitiesScored != 0 {
		t.Fatalf("loopback stats = %+v, want %d connections closed and no identity charged", stats, attempts)
	}
	honest := loopbackHelloWelcome(t, svc.cfg.ListenAddress)
	_ = honest.Close()
}

// Connections held at once: the class holds loopbackMaxActiveCeiling of them,
// far beyond the eight one IP may hold, and refuses the next with the alert.
// Closing one frees its slot for the next.
func TestTheLoopbackClassBoundsItsConnectionsHeldAtOnce(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(*Service) {})
	held := make([]net.Conn, 0, loopbackMaxActiveCeiling)
	t.Cleanup(func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	for range loopbackMaxActiveCeiling {
		held = append(held, loopbackHelloWelcome(t, svc.cfg.ListenAddress))
	}
	if got := dialAndRead(t, svc.cfg.ListenAddress); !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("connection over the class bound read % x, want the alert", got)
	}
	if stats := svc.LoopbackInboundStats(); stats.Refusals.ActiveLimit != 1 || stats.Active != loopbackMaxActiveCeiling {
		t.Fatalf("loopback stats = %+v, want the bound held and one refusal", stats)
	}

	_ = held[0].Close()
	held = held[1:]
	waitForConditionMsg(t, 5*time.Second, "a closed loopback connection kept its slot", func() bool {
		return loopbackActive(svc) == loopbackMaxActiveCeiling-1
	})
	held = append(held, loopbackHelloWelcome(t, svc.cfg.ListenAddress))
}

// Reconnecting in a loop opens no new budget: every connection is closed
// right after its welcome, so the class never holds more than a few at once,
// and still the connection after loopbackMaxNewPerWindow admissions within
// the window is refused.
func TestReconnectingFromLoopbackDoesNotEscapeTheRateOfTheClass(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(*Service) {})
	started := time.Now()
	for range loopbackMaxNewPerWindow {
		conn := loopbackHelloWelcome(t, svc.cfg.ListenAddress)
		_ = conn.Close()
	}
	if elapsed := time.Since(started); elapsed >= loopbackNewWindow {
		t.Skipf("the reconnect loop took %v, longer than the window %v: the bound cannot be observed", elapsed, loopbackNewWindow)
	}
	if got := dialAndRead(t, svc.cfg.ListenAddress); !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("connection over the class rate read % x, want the alert", got)
	}
	if stats := svc.LoopbackInboundStats(); stats.Refusals.ConnectionRate != 1 || stats.Admitted != loopbackMaxNewPerWindow {
		t.Fatalf("loopback stats = %+v, want the rate admitted and one refusal", stats)
	}
}

// dialV2As completes a v2 handshake from loopback as the dialler's identity
// and returns the session's connection.
func dialV2As(t *testing.T, address string, dialer *Service) (net.Conn, error) {
	t.Helper()
	raw := dialWhenListening(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := sessionv2.Dial(ctx, raw, dialer.secureSessions.local, dialer.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	conn, err := session.Conn()
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

// answersPing reports whether the session answers a ping with a pong.
func answersPing(t *testing.T, conn net.Conn) bool {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "ping"})
	if err != nil {
		t.Fatalf("marshal ping: %v", err)
	}
	if _, err := conn.Write([]byte(line)); err != nil {
		return false
	}
	reader := bufio.NewReader(conn)
	for {
		answer, err := reader.ReadString('\n')
		if err != nil {
			return false
		}
		if frame, err := protocol.ParseFrameLine(strings.TrimSpace(answer)); err == nil && frame.Type == "pong" {
			return true
		}
	}
}

// A v2 peer that misbehaves from loopback is charged by the identity it
// proved, never by 127.0.0.1: once its score crosses the threshold it gets
// the ban notice and loses the session, its next loopback session is closed
// as soon as it proves the identity, and another identity from the same
// loopback still gets in.
func TestALoopbackV2ViolatorIsChargedByItsProvenIdentity(t *testing.T) {
	t.Parallel()
	listener, stop := startQuietNode(t, withProductionLimits)
	t.Cleanup(stop)
	violator := newTestService(t, config.NodeTypeFull)

	// Every session commits one violation — an ack_delete naming another
	// identity — and the listener ends it after the error reply. The score
	// the sessions add up crosses the threshold on the last one, which gets
	// the ban notice instead.
	attempts := banThreshold / banIncrementInvalidSig
	for i := range attempts {
		conn, err := dialV2As(t, listener.cfg.ListenAddress, violator)
		if err != nil {
			t.Fatalf("attempt %d: the violator's v2 dial: %v", i, err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		writeJSONFrame(t, conn, protocol.Frame{Type: "ack_delete", Address: "not-this-session", ID: "x"})
		notice := false
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			frame, parseErr := protocol.ParseFrameLine(strings.TrimSpace(line))
			if parseErr == nil && frame.Type == protocol.FrameTypeConnectionNotice && frame.Code == protocol.ErrCodePeerBanned {
				notice = true
				if details, _, err := protocol.ParsePeerBannedDetails(frame.Details); err != nil || details.Reason != protocol.PeerBannedReasonPeerBan {
					t.Fatalf("attempt %d: ban notice %+v (%v), want a ban of the peer, not of an IP", i, details, err)
				}
			}
		}
		_ = conn.Close()
		if last := i == attempts-1; notice != last {
			t.Fatalf("attempt %d of %d: ban notice %v, want it only once the threshold is crossed", i, attempts, notice)
		}
	}

	listener.ipStateMu.RLock()
	entry, scored := listener.bans["127.0.0.1"]
	listener.ipStateMu.RUnlock()
	if scored {
		t.Fatalf("loopback was scored by its IP: %+v", entry)
	}

	again, err := dialV2As(t, listener.cfg.ListenAddress, violator)
	if err == nil {
		if answersPing(t, again) {
			t.Fatal("the banned identity got a working session back")
		}
		_ = again.Close()
	}
	waitForConditionMsg(t, 5*time.Second, "the banned identity's next session was not refused by its identity", func() bool {
		return listener.LoopbackInboundStats().Refusals.IdentityBanned == 2
	})
	if stats := listener.LoopbackInboundStats(); stats.IdentitiesBanned != 1 || stats.Refusals.ViolationsClosed != 0 {
		t.Fatalf("loopback stats = %+v, want one banned identity and no unproven connection charged", stats)
	}

	honest := newTestService(t, config.NodeTypeFull)
	other, err := dialV2As(t, listener.cfg.ListenAddress, honest)
	if err != nil {
		t.Fatalf("another identity from loopback: %v", err)
	}
	defer func() { _ = other.Close() }()
	if !answersPing(t, other) {
		t.Fatal("another identity from loopback was shut out with the violator")
	}
}

// The node's shutdown — the cancel of its context — gives back every slot of
// the class, as closes do.
func TestStoppingTheNodeGivesBackTheLoopbackSlots(t *testing.T) {
	t.Parallel()
	listener := freeAddress(t)
	svc, stop := startTestNodeWithSetup(t, config.Node{ListenAddress: listener, BootstrapPeers: []string{}, Type: config.NodeTypeFull}, withoutOwnDials(withProductionLimits))
	held := make([]net.Conn, 0, 4)
	for range 4 {
		held = append(held, loopbackHelloWelcome(t, listener))
	}
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	waitForConditionMsg(t, 5*time.Second, "the held connections never took their slots", func() bool {
		return loopbackActive(svc) == len(held)
	})
	stop()
	if got := loopbackActive(svc); got != 0 {
		t.Fatalf("after the node stopped the class holds %d slots, want none", got)
	}
}

// withProductionLimits switches back on the per-IP limits, and with them the
// loopback class's bounds, that the test node helpers switch off.
func withProductionLimits(svc *Service) {
	svc.disableRateLimiting = false
}

// loopbackActive is the connections the class holds now.
func loopbackActive(svc *Service) int {
	return int(svc.loopbackInbound.activeGauge.Load())
}

// spoofedSourceConn is an accepted socket whose source the listener reads as
// remote. The sandbox has only loopback to connect from, so this is how a
// test reaches the external-source path: every classification on that path —
// admission, blacklist, ban score, handshake bounds — reads RemoteAddr.
type spoofedSourceConn struct {
	net.Conn
	remote net.Addr
}

func (c spoofedSourceConn) RemoteAddr() net.Addr { return c.remote }

// testExternalIP is the external source of the tests (TEST-NET-3).
const testExternalIP = "203.0.113.9"

// dialFromSource hands a connection from ip to svc's accept path, exactly
// as the accept loop does, and returns the dialler's end. The port is the
// real client port, so each connection has an address of its own.
func dialFromSource(t *testing.T, svc *Service, ip string) net.Conn {
	t.Helper()
	server, client := tcpPair(t)
	port := client.LocalAddr().(*net.TCPAddr).Port
	svc.acceptInbound(spoofedSourceConn{Conn: server, remote: &net.TCPAddr{IP: net.ParseIP(ip), Port: port}})
	return client
}

// The class's bound on connections held at once: the ceiling on a node that
// set no inbound capacity, a quarter of a configured one, never below the
// eight the whole onion entry had before the class.
func TestLoopbackActiveBound(t *testing.T) {
	t.Parallel()
	cases := map[int]int{
		0:    loopbackMaxActiveCeiling,
		3:    loopbackMinActiveFloor,
		8:    loopbackMinActiveFloor,
		31:   loopbackMinActiveFloor,
		36:   9,
		100:  25,
		256:  loopbackMaxActiveCeiling,
		4096: loopbackMaxActiveCeiling,
	}
	for capacity, want := range cases {
		if got := loopbackActiveBound(capacity); got != want {
			t.Errorf("loopbackActiveBound(%d) = %d, want %d", capacity, got, want)
		}
	}
}

// The inbound capacity is the smaller of the inbound limit and the part of
// the shared budget not reserved for outbound, whichever is set.
func TestInboundCapacityOf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		budget connbudget.Stats
		want   int
	}{
		{"nothing set", connbudget.Stats{}, 0},
		{"inbound limit only", connbudget.Stats{MaxInbound: 40}, 40},
		{"shared budget only", connbudget.Stats{Enabled: true, NonSlotCapacity: 120}, 120},
		{"budget below the limit", connbudget.Stats{Enabled: true, NonSlotCapacity: 30, MaxInbound: 40}, 30},
		{"limit below the budget", connbudget.Stats{Enabled: true, NonSlotCapacity: 120, MaxInbound: 40}, 40},
	}
	for _, tc := range cases {
		if got := inboundCapacityOf(tc.budget); got != tc.want {
			t.Errorf("%s: inboundCapacityOf = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// The held-connections budget: the bound refuses, a release frees exactly one
// slot, and a second release frees nothing more.
func TestTheLoopbackClassHeldBudgetIsReleasedOnceByEachSlot(t *testing.T) {
	t.Parallel()
	class := newLoopbackInboundClass(loopbackInboundBounds{maxActive: 2, maxNewPerWindow: 100, window: time.Second}, productionIdentityPenaltyPolicy(), newManualTestClock().now)
	first, refusal := class.admit(true)
	if refusal != nil {
		t.Fatalf("first: %v", refusal)
	}
	if _, refusal := class.admit(true); refusal != nil {
		t.Fatalf("second: %v", refusal)
	}
	if _, refusal := class.admit(true); refusal == nil || refusal.reason != refusalLoopbackConnections {
		t.Fatalf("third: %v, want the held-connections refusal", refusal)
	}
	first()
	first()
	if got := class.activeGauge.Load(); got != 1 {
		t.Fatalf("after one slot released twice: %d held, want 1", got)
	}
	if _, refusal := class.admit(true); refusal != nil {
		t.Fatalf("after a release: %v", refusal)
	}
	if _, refusal := class.admit(true); refusal == nil {
		t.Fatal("a double release opened a second slot")
	}
}

// The rate is a sliding window: the bound holds over ANY window, including one
// that straddles where a fixed window would have started over — a fixed
// window lets twice the bound through across that boundary.
func TestTheLoopbackClassRateIsASlidingWindow(t *testing.T) {
	t.Parallel()
	clock := newManualTestClock()
	class := newLoopbackInboundClass(loopbackInboundBounds{maxActive: 100, maxNewPerWindow: 3, window: 10 * time.Second}, productionIdentityPenaltyPolicy(), clock.now)
	admitAndClose := func() *listenerRefusalError {
		release, refusal := class.admit(true)
		if refusal == nil {
			release()
		}
		return refusal
	}
	if refusal := admitAndClose(); refusal != nil {
		t.Fatalf("t=0: %v", refusal)
	}
	clock.advance(9 * time.Second)
	for i := range 2 {
		if refusal := admitAndClose(); refusal != nil {
			t.Fatalf("t=9s #%d: %v", i, refusal)
		}
	}
	if refusal := admitAndClose(); refusal == nil || refusal.reason != refusalLoopbackConnectionRate {
		t.Fatalf("a fourth within 10 s: %v, want the rate refusal", refusal)
	}
	clock.advance(time.Second)
	// t=10s: the admission at t=0 left the window, those at t=9s did not.
	if refusal := admitAndClose(); refusal != nil {
		t.Fatalf("t=10s: %v, want one admission freed by the oldest", refusal)
	}
	if refusal := admitAndClose(); refusal == nil {
		t.Fatal("t=10s: a fixed window would start over here; the sliding one must not")
	}
	clock.advance(9 * time.Second)
	for i := range 2 {
		if refusal := admitAndClose(); refusal != nil {
			t.Fatalf("t=19s #%d: %v", i, refusal)
		}
	}
	if got := class.admitted.Load(); got != 6 {
		t.Fatalf("admitted = %d, want 6", got)
	}
}

// With the per-IP limits off the class bounds nothing, and still counts.
func TestTheLoopbackClassNotEnforcedCountsWithoutRefusing(t *testing.T) {
	t.Parallel()
	class := newLoopbackInboundClass(loopbackInboundBounds{maxActive: 1, maxNewPerWindow: 1, window: time.Hour}, productionIdentityPenaltyPolicy(), newManualTestClock().now)
	for i := range 3 {
		if _, refusal := class.admit(false); refusal != nil {
			t.Fatalf("#%d: %v", i, refusal)
		}
	}
	if got := class.activeGauge.Load(); got != 3 {
		t.Fatalf("held = %d, want 3 counted", got)
	}
}

// A condemned connection is counted once, and forgetting it leaves nothing.
func TestCondemningAConnectionIsCountedOnce(t *testing.T) {
	t.Parallel()
	class := newLoopbackInboundClass(productionLoopbackInboundBounds(0), productionIdentityPenaltyPolicy(), nil)
	class.track(7)
	if !class.condemn(7) || class.condemn(7) {
		t.Fatal("condemn reported the first mark wrongly")
	}
	if !class.isCondemned(7) || class.isCondemned(8) {
		t.Fatal("isCondemned does not match what was condemned")
	}
	class.forget(7)
	if class.isCondemned(7) {
		t.Fatal("a forgotten connection is still condemned")
	}
}

// The identity score: the charge that crosses the threshold bans, once; the
// ban ends after its term and takes the score with it; an idle score is swept;
// at capacity the entry scored longest ago gives way.
func TestProvenIdentityPenalties(t *testing.T) {
	t.Parallel()
	clock := newManualTestClock()
	policy := identityPenaltyPolicy{threshold: 300, banFor: time.Hour, idleAge: 2 * time.Hour, capacity: 2}
	penalties := newProvenIdentityPenalties(policy, clock.now)
	a := domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001")
	b := domain.PeerIdentityFromWire("bb00000000000000000000000000000000000002")
	c := domain.PeerIdentityFromWire("cc00000000000000000000000000000000000003")

	for i, wantJust := range []bool{false, false, true, false} {
		verdict := penalties.charge(a, 100)
		if verdict.justBanned != wantJust {
			t.Fatalf("charge %d: justBanned = %v, want %v", i, verdict.justBanned, wantJust)
		}
	}
	if !penalties.isBanned(a) || penalties.isBanned(b) {
		t.Fatal("only the identity over the threshold is banned")
	}
	if scored, banned := penalties.gauges(); scored != 1 || banned != 1 {
		t.Fatalf("gauges = %d scored, %d banned, want 1 and 1", scored, banned)
	}
	clock.advance(time.Hour)
	if penalties.isBanned(a) {
		t.Fatal("the ban outlived its term")
	}
	if verdict := penalties.charge(a, 100); verdict.score != 100 {
		t.Fatalf("score after the ban ended = %d, want a fresh 100", verdict.score)
	}

	clock.advance(time.Minute)
	penalties.charge(b, 100)
	clock.advance(time.Minute)
	penalties.charge(c, 100)
	if scored, _ := penalties.gauges(); scored != 2 {
		t.Fatalf("scored = %d at capacity 2", scored)
	}
	if verdict := penalties.charge(a, 100); verdict.score != 100 {
		t.Fatalf("the entry scored longest ago (a) survived the capacity: score %d", verdict.score)
	}

	clock.advance(policy.idleAge)
	penalties.sweep()
	if scored, banned := penalties.gauges(); scored != 0 || banned != 0 {
		t.Fatalf("after the idle age: %d scored, %d banned, want none", scored, banned)
	}
}

// Every loopback form is one class: IPv4 127/8, IPv6 ::1 and the IPv4-mapped
// form. Everything else is external, an unparseable host included.
func TestClassifyInboundSource(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.InboundSourceClass{
		"127.0.0.1":        domain.InboundSourceLoopback,
		"127.8.9.10":       domain.InboundSourceLoopback,
		"::1":              domain.InboundSourceLoopback,
		"::ffff:127.0.0.1": domain.InboundSourceLoopback,
		"203.0.113.9":      domain.InboundSourceExternal,
		"10.0.0.1":         domain.InboundSourceExternal,
		"2001:db8::1":      domain.InboundSourceExternal,
		"::":               domain.InboundSourceExternal,
		"not-an-ip":        domain.InboundSourceExternal,
		"":                 domain.InboundSourceExternal,
	}
	for host, want := range cases {
		if got := domain.ClassifyInboundSource(host); got != want {
			t.Errorf("ClassifyInboundSource(%q) = %v, want %v", host, got, want)
		}
	}
}

// A connection from ::1 is admitted by the loopback class, not counted per IP,
// and an IPv6 loopback flood is bounded by the same class budget as IPv4.
func TestIPv6LoopbackIsTheSameClass(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(svc *Service) {
		svc.loopbackInbound = newLoopbackInboundClass(loopbackInboundBounds{maxActive: 2, maxNewPerWindow: 100, window: time.Hour}, productionIdentityPenaltyPolicy(), nil)
	})
	held := []net.Conn{
		dialFromSource(t, svc, "127.0.0.1"),
		dialFromSource(t, svc, "::1"),
	}
	waitForConditionMsg(t, 5*time.Second, "the loopback connections never took their slots", func() bool {
		return loopbackActive(svc) == len(held)
	})
	svc.ipStateMu.RLock()
	perIP := len(svc.inboundByIP)
	svc.ipStateMu.RUnlock()
	if perIP != 0 {
		t.Fatalf("loopback connections were counted per IP: %d entries", perIP)
	}
	refused := dialFromSource(t, svc, "::1")
	_, _ = refused.Write(v1HelloBytes)
	if got, err := readAnswer(t, refused, 5*time.Second); err != nil || !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("::1 over the class bound read % x then %v, want the alert", got, err)
	}
	if stats := svc.LoopbackInboundStats(); stats.Refusals.ActiveLimit != 1 {
		t.Fatalf("loopback stats = %+v, want one held-connections refusal", stats)
	}
}

// A real connection over ::1, where the host has IPv6 loopback: classified as
// loopback end to end.
func TestARealIPv6LoopbackConnectionIsInTheClass(t *testing.T) {
	t.Parallel()
	probe, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback on this host: %v", err)
	}
	address := probe.Addr().String()
	_ = probe.Close()
	svc, stop := startTestNodeWithSetup(t, config.Node{ListenAddress: address, BootstrapPeers: []string{}, Type: config.NodeTypeFull}, withoutOwnDials(withProductionLimits))
	t.Cleanup(stop)
	conn := loopbackHelloWelcome(t, address)
	defer func() { _ = conn.Close() }()
	if got := loopbackActive(svc); got != 1 {
		t.Fatalf("a ::1 connection held %d loopback slots, want 1", got)
	}
}

// An external source keeps the per-IP rules unchanged: eight connections held
// at once, the ninth refused, and loopback is not touched by them.
func TestAnExternalSourceKeepsItsPerIPCap(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(*Service) {})
	for range maxConnPerIP {
		dialFromSource(t, svc, testExternalIP)
	}
	waitForConditionMsg(t, 5*time.Second, "the external connections never took their per-IP slots", func() bool {
		svc.ipStateMu.RLock()
		defer svc.ipStateMu.RUnlock()
		return svc.inboundByIP[testExternalIP] == maxConnPerIP
	})
	if got := externalDialAndRead(t, svc); !bytes.Equal(got, sessionv2.RefusalAlert()) {
		t.Fatalf("the ninth external connection read % x, want the alert", got)
	}
	if stats := svc.ListenerRefusalStats(); stats.ConnectionsPerIP != 1 || stats.LoopbackConnections != 0 {
		t.Fatalf("stats = %+v, want one per-IP refusal", stats)
	}
	if got := loopbackActive(svc); got != 0 {
		t.Fatalf("external connections took %d loopback slots", got)
	}
	honest := loopbackHelloWelcome(t, svc.cfg.ListenAddress)
	_ = honest.Close()
}

// A ban entry for a loopback IP — one an earlier build wrote — is not
// consulted: loopback is never refused as blacklisted.
func TestALoopbackBanEntryIsNotConsulted(t *testing.T) {
	t.Parallel()
	svc := refusingNode(t, config.Node{}, func(svc *Service) {
		svc.bans["127.0.0.1"] = banEntry{Score: banThreshold, Blacklisted: time.Now().UTC().Add(time.Hour)}
	})
	conn := loopbackHelloWelcome(t, svc.cfg.ListenAddress)
	_ = conn.Close()
	if stats := svc.ListenerRefusalStats(); stats.Blacklisted != 0 {
		t.Fatalf("stats = %+v, want no blacklist refusal", stats)
	}
}

// A ring of zero admissions would refuse nothing and index nothing: the
// constructor makes the rate at least one.
func TestTheLoopbackClassRateIsAtLeastOne(t *testing.T) {
	t.Parallel()
	class := newLoopbackInboundClass(loopbackInboundBounds{maxActive: 10, maxNewPerWindow: 0, window: time.Hour}, productionIdentityPenaltyPolicy(), newManualTestClock().now)
	release, refusal := class.admit(true)
	if refusal != nil {
		t.Fatalf("first: %v", refusal)
	}
	release()
	if _, refusal := class.admit(true); refusal == nil || refusal.reason != refusalLoopbackConnectionRate {
		t.Fatalf("second within the window: %v, want the rate refusal", refusal)
	}
}

// A connection is condemned only while its read loop lives: a violation
// charged after the loop forgot the connection leaves nothing behind.
func TestCondemningAForgottenConnectionLeavesNothing(t *testing.T) {
	t.Parallel()
	class := newLoopbackInboundClass(productionLoopbackInboundBounds(0), productionIdentityPenaltyPolicy(), nil)
	class.track(7)
	class.forget(7)
	if class.condemn(7) {
		t.Fatal("a forgotten connection was condemned")
	}
	class.condemnedMu.Lock()
	left := len(class.condemned)
	class.condemnedMu.Unlock()
	if left != 0 {
		t.Fatalf("%d entries left after the connection was forgotten", left)
	}
}

// At capacity a ban is never what gives way: the oldest UNBANNED entry does,
// and with every entry banned a new identity's charge is reported as
// unrecorded, so the caller can close the session instead.
func TestEvictionNeverLiftsABan(t *testing.T) {
	t.Parallel()
	clock := newManualTestClock()
	policy := identityPenaltyPolicy{threshold: 100, banFor: time.Hour, idleAge: 2 * time.Hour, capacity: 2}
	penalties := newProvenIdentityPenalties(policy, clock.now)
	banned := domain.PeerIdentityFromWire("aa00000000000000000000000000000000000001")
	scored := domain.PeerIdentityFromWire("bb00000000000000000000000000000000000002")
	fresh := domain.PeerIdentityFromWire("cc00000000000000000000000000000000000003")
	other := domain.PeerIdentityFromWire("dd00000000000000000000000000000000000004")

	penalties.charge(banned, 100)
	clock.advance(time.Minute)
	penalties.charge(scored, 10)
	clock.advance(time.Minute)
	if verdict := penalties.charge(fresh, 10); verdict.unrecorded {
		t.Fatal("a slot held by an unbanned entry was not given up")
	}
	if !penalties.isBanned(banned) {
		t.Fatal("eviction lifted the ban of the entry scored longest ago")
	}
	penalties.charge(fresh, 100)
	if verdict := penalties.charge(other, 10); !verdict.unrecorded {
		t.Fatalf("with every entry banned a new charge was recorded: %+v", verdict)
	}
	if !penalties.isBanned(banned) || !penalties.isBanned(fresh) {
		t.Fatal("a ban was lifted to make room")
	}
	if scoredNow, bannedNow := penalties.gauges(); scoredNow != 2 || bannedNow != 2 {
		t.Fatalf("gauges = %d scored, %d banned, want 2 and 2", scoredNow, bannedNow)
	}
}

// soleInboundConnID is the ConnID of the node's only inbound connection.
func soleInboundConnID(t *testing.T, svc *Service) domain.ConnID {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	var ids []domain.ConnID
	svc.forEachConnLocked(func(info connInfo) bool {
		if info.dir == netcore.Inbound {
			ids = append(ids, info.id)
		}
		return true
	})
	if len(ids) != 1 {
		t.Fatalf("%d inbound connections, want one", len(ids))
	}
	return ids[0]
}

// Every session of a banned identity is closed at its next violation, not
// only the one whose violation crossed the threshold: the identity is banned
// meanwhile (here through another session's score), and session A, open since
// before the ban, violates afterwards.
func TestAViolationOnAnySessionOfABannedIdentityClosesIt(t *testing.T) {
	t.Parallel()
	listener, stop := startQuietNode(t, withProductionLimits)
	t.Cleanup(stop)
	violator := newTestService(t, config.NodeTypeFull)
	sessionA, err := dialV2As(t, listener.cfg.ListenAddress, violator)
	if err != nil {
		t.Fatalf("session A: %v", err)
	}
	defer func() { _ = sessionA.Close() }()
	waitForConditionMsg(t, 5*time.Second, "session A was never accepted", func() bool {
		return answersPing(t, sessionA)
	})
	connA := soleInboundConnID(t, listener)

	// Session B's violations have banned the identity.
	peer := domain.PeerIdentityFromWire(violator.identity.Address)
	if verdict := listener.loopbackInbound.identities.charge(peer, banThreshold); !verdict.justBanned {
		t.Fatalf("fixture: the identity was not banned: %+v", verdict)
	}

	listener.addBanScore(connA, banIncrementInvalidSig)
	reader := bufio.NewReader(sessionA)
	_ = sessionA.SetDeadline(time.Now().Add(5 * time.Second))
	writeJSONFrame(t, sessionA, protocol.Frame{Type: "ping"})
	if !endsWithin(sessionA, reader, 5*time.Second) {
		t.Fatal("a session of a banned identity stayed open after its violation")
	}
}
