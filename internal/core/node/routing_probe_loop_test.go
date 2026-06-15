package node

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_probe_loop_test.go tests the PR 11.3c sender bookkeeping
// (probeRegistry) and the receive-side RTT plumbing into
// handleRouteProbeAck. The sender ticker goroutine itself is tested
// indirectly via probeTick() — a synchronous unit-testable
// counterpart of the goroutine body. Production lifecycle (ticker
// start/stop) is covered by the service-level integration tests
// since it shares the standard ctx-cancel pattern with the
// announce loop.

// --- probeRegistry: data structure tests ---

// TestProbeRegistry_RegisterResolveHappyPath — basic round-trip.
// Register returns a non-zero ID; Resolve recovers target+uplink and
// reports a non-negative RTT computed from the injected clock.
func TestProbeRegistry_RegisterResolveHappyPath(t *testing.T) {
	t0 := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := t0
	reg := newProbeRegistry(5*time.Second, func() time.Time { return clk }, nil)

	id := reg.Register(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	if id == 0 {
		t.Fatal("Register returned probe ID 0; must be non-zero so wire frames with probe_id=0 are unambiguous")
	}
	if got := reg.Len(); got != 1 {
		t.Fatalf("after Register: Len = %d, want 1", got)
	}

	clk = t0.Add(30 * time.Millisecond)
	target, uplink, rtt, ok := reg.Resolve(id)
	if !ok {
		t.Fatal("Resolve returned ok=false for a freshly registered probe")
	}
	if target != domaintest.ID("id-target") || uplink != domaintest.ID("id-uplink") {
		t.Fatalf("Resolve returned (%q, %q), want (id-target, id-uplink)", target, uplink)
	}
	if rtt != 30*time.Millisecond {
		t.Fatalf("Resolve rtt = %v, want 30ms", rtt)
	}
	if got := reg.Len(); got != 0 {
		t.Fatalf("after Resolve: Len = %d, want 0 (entry must be removed)", got)
	}
}

// TestProbeRegistry_ResolveUnknownIDReturnsFalse — the stray-ack
// guard. Resolve on an unknown probe ID returns ok=false so the
// caller drops the ack without touching health.
func TestProbeRegistry_ResolveUnknownIDReturnsFalse(t *testing.T) {
	reg := newProbeRegistry(time.Second, nil, nil)
	if _, _, _, ok := reg.Resolve(999); ok {
		t.Fatal("Resolve returned ok=true for unknown probe ID")
	}
}

// TestProbeRegistry_TimeoutFiresOnTimeoutCallback — the watcher
// arms HealthProbeTimeout (5s in production, shorter in tests) and
// fires the onTimeout callback when no Resolve arrived in time.
// Uses real time with a tight budget so the test stays under one
// CI second.
func TestProbeRegistry_TimeoutFiresOnTimeoutCallback(t *testing.T) {
	var (
		mu       sync.Mutex
		fired    bool
		target   domain.PeerIdentity
		uplink   domain.PeerIdentity
		callback = make(chan struct{}, 1)
	)
	onTimeout := func(tgt, up domain.PeerIdentity) {
		mu.Lock()
		fired = true
		target = tgt
		uplink = up
		mu.Unlock()
		select {
		case callback <- struct{}{}:
		default:
		}
	}
	reg := newProbeRegistry(20*time.Millisecond, nil, onTimeout)
	reg.Register(domaintest.ID("id-target"), domaintest.ID("id-uplink"))

	select {
	case <-callback:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout callback did not fire within 500ms")
	}

	mu.Lock()
	defer mu.Unlock()
	if !fired {
		t.Fatal("onTimeout was not invoked")
	}
	if target != domaintest.ID("id-target") || uplink != domaintest.ID("id-uplink") {
		t.Fatalf("onTimeout received (%q, %q), want (id-target, id-uplink)", target, uplink)
	}
	if got := reg.Len(); got != 0 {
		t.Fatalf("after timeout fire: Len = %d, want 0", got)
	}
}

// TestProbeRegistry_ResolveBeforeTimeoutCancelsCallback — when an
// ack arrives before the deadline, the timeout watcher is cancelled
// and onTimeout is never invoked.
func TestProbeRegistry_ResolveBeforeTimeoutCancelsCallback(t *testing.T) {
	var fired atomic.Bool
	onTimeout := func(domain.PeerIdentity, domain.PeerIdentity) {
		fired.Store(true)
	}
	reg := newProbeRegistry(50*time.Millisecond, nil, onTimeout)
	id := reg.Register(domaintest.ID("id-target"), domaintest.ID("id-uplink"))

	// Resolve before the watcher deadline.
	if _, _, _, ok := reg.Resolve(id); !ok {
		t.Fatal("Resolve returned ok=false; the probe should still be pending")
	}

	// Wait past the would-have-fired deadline to confirm cancellation.
	time.Sleep(200 * time.Millisecond)
	if fired.Load() {
		t.Fatal("onTimeout fired after Resolve; the watcher should have been cancelled")
	}
}

// TestProbeRegistry_HasOutstanding — rate-limit guard for the
// sender. Confirms HasOutstanding flips between true/false as
// probes are registered/resolved for the same pair.
func TestProbeRegistry_HasOutstanding(t *testing.T) {
	reg := newProbeRegistry(time.Second, nil, nil)

	if reg.HasOutstanding(domaintest.ID("id-target"), domaintest.ID("id-uplink")) {
		t.Fatal("HasOutstanding=true on a cold registry")
	}
	id := reg.Register(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	if !reg.HasOutstanding(domaintest.ID("id-target"), domaintest.ID("id-uplink")) {
		t.Fatal("HasOutstanding=false after Register for the same pair")
	}
	if reg.HasOutstanding(domaintest.ID("id-target"), domaintest.ID("id-other-uplink")) {
		t.Fatal("HasOutstanding=true for a different uplink — must be per-pair")
	}
	if _, _, _, ok := reg.Resolve(id); !ok {
		t.Fatal("Resolve failed unexpectedly")
	}
	if reg.HasOutstanding(domaintest.ID("id-target"), domaintest.ID("id-uplink")) {
		t.Fatal("HasOutstanding=true after Resolve")
	}
}

// TestProbeRegistry_IDsAreUniqueAndNonZero — across many Register
// calls, IDs are unique and never zero. uint64 wrap-around is
// practically unreachable but the Register logic skips zero
// explicitly; this test pins that behaviour.
func TestProbeRegistry_IDsAreUniqueAndNonZero(t *testing.T) {
	reg := newProbeRegistry(time.Second, nil, nil)
	seen := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		id := reg.Register(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
		if id == 0 {
			t.Fatalf("Register returned 0 at iteration %d", i)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate probe ID %d at iteration %d", id, i)
		}
		seen[id] = struct{}{}
	}
}

// --- Service.handleRouteProbeAck with registry plumbing ---

// newTestServiceWithProbeRegistry builds a routing-equipped Service
// with the probe registry attached. Used by tests that exercise
// the full receive-side RTT-plumbing path.
func newTestServiceWithProbeRegistry(t *testing.T, localIdentity domain.PeerIdentity) *Service {
	t.Helper()
	svc := newTestServiceWithRouting(t, localIdentity)
	svc.probeRegistry = newProbeRegistry(
		routing.HealthProbeTimeout,
		nil,
		svc.routingTable.MarkProbeFailure,
	)
	return svc
}

// installCapableUplinkForTest registers a synthetic outbound
// peerSession plus a Connected peerHealth entry under the same
// address, so peerSendableConnectionsLocked sees one usable
// candidate for the given identity and capability. Without this
// the PR 11.7 capability gate inside sendProbe short-circuits and
// no probe is registered — see the comment on the gate in
// routing_probe_loop.go::sendProbe.
//
// PR 11.11 P2#1 cancels probe registrations whose send fails,
// so test fixtures that want sendProbe to actually register must
// also satisfy sendFrameToIdentity. We do that by giving the
// session a fake net.Conn (one half of a net.Pipe — never read
// from, but non-nil for the conn-presence check) and a buffered
// sendCh so the non-blocking enqueue inside
// tryEnqueuePeerSessionFrame succeeds. The test does not
// otherwise consume the channel.
//
// Test-only helper: callers must ensure the address is unique
// across sessions in the fixture; the helper does not handle
// collisions because the production code never juggles fake
// addresses.
func installCapableUplinkForTest(t *testing.T, svc *Service, identity domain.PeerIdentity, cap domain.Capability) {
	t.Helper()
	addr := domain.PeerAddress("addr-" + identity.String())
	// net.Pipe is the cheapest way to get a non-nil net.Conn
	// without spinning up a real listener. We close one half
	// immediately because nothing reads from it; the other half
	// stays attached to the peerSession so the conn-presence
	// check in sendFrameToIdentity passes.
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: identity,
		capabilities: []domain.Capability{cap},
		conn:         clientConn,
		sendCh:       make(chan protocol.Frame, 8), // buffered so non-blocking enqueue succeeds
	}
	if svc.health == nil {
		svc.health = make(map[domain.PeerAddress]*peerHealth)
	}
	svc.health[addr] = &peerHealth{
		Address:         addr,
		Connected:       true,
		Direction:       peerDirectionOutbound,
		LastConnectedAt: time.Now().UTC(),
		LastPongAt:      time.Now().UTC(),
	}
}

// TestHandleRouteProbeAck_ResolvedAckUpdatesHealth — the happy
// path: registry has an outstanding probe, the ack matches by ID,
// the sender's measured RTT (from registry.Resolve) is folded into
// the health state through MarkProbeAck.
func TestHandleRouteProbeAck_ResolvedAckUpdatesHealth(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	// PR 11.24 P2#2: MarkProbeAck(reachable=true) now verifies a
	// live storage claim before ensuring health, so seed one for
	// (idTargetX, idPeerB).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("setup: UpdateRoute: %v", err)
	}

	probeID := svc.probeRegistry.Register(idTargetX, idPeerB)

	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   probeID,
		Reachable: true,
		// RTTMs (the responder-supplied estimate) is deliberately
		// large to confirm the sender's locally measured RTT wins.
		// Sender-measured RTT is non-zero because Resolve uses
		// time.Now and at least nanoseconds elapse between Register
		// and Resolve.
		RTTMs: 9999,
	}
	svc.handleRouteProbeAck(idPeerB, ack)

	snap := svc.routingTable.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthGood {
		t.Fatalf("Health = %s, want good", snap[0].Health)
	}
	if snap[0].RTT >= 9999*time.Millisecond {
		t.Fatalf("RTT = %v looks like the responder's RTTMs field leaked into ranking (zero-trust violation)", snap[0].RTT)
	}
	if svc.probeRegistry.Len() != 0 {
		t.Fatalf("probeRegistry.Len = %d after successful Resolve, want 0", svc.probeRegistry.Len())
	}
}

// TestHandleRouteProbeAck_StrayAckDropped — defensive contract:
// when the ack's probe ID is unknown (timeout already fired, or a
// malicious peer fabricated an ack), the handler drops it without
// touching health.
func TestHandleRouteProbeAck_StrayAckDropped(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	// No Register call → probe ID 42 is unknown.
	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   42,
		Reachable: true,
	}
	svc.handleRouteProbeAck(idPeerB, ack)

	if snap := svc.routingTable.HealthSnapshot(); snap != nil {
		t.Fatalf("stray ack altered health state: %v", snap)
	}
}

// TestHandleRouteProbeAck_UplinkMismatchDropped — the receive-side
// zero-trust guard. If peer V sends an ack carrying the probe ID
// that was minted for a probe sent to peer U, the handler refuses
// to apply the ack to (target, V) — V would otherwise be able to
// spoof acks for U's probes.
//
// PR 11.8 P2#3 also requires that the mismatched ack does NOT
// consume the pending entry: the real uplink's ack (or its
// timeout) must still apply. Earlier this test asserted the
// opposite contract (Resolve consumed unconditionally), which
// allowed any capable peer to drop another peer's outstanding
// probe by guessing the monotonic probe_id.
func TestHandleRouteProbeAck_UplinkMismatchDropped(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	probeID := svc.probeRegistry.Register(idTargetX, idPeerB)

	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   probeID,
		Reachable: true,
	}
	// Note: sender identity is idPeerC, NOT idPeerB.
	svc.handleRouteProbeAck(idPeerC, ack)

	if snap := svc.routingTable.HealthSnapshot(); snap != nil {
		t.Fatalf("uplink-mismatch ack altered health state: %v", snap)
	}
	// Mismatch must NOT consume the pending entry — the real
	// uplink's ack or timeout still has to apply.
	if svc.probeRegistry.Len() != 1 {
		t.Fatalf("probeRegistry.Len = %d after mismatched ack, want 1 (mismatch must NOT consume pending probe)", svc.probeRegistry.Len())
	}
}

// TestHandleRouteProbeAck_NoRegistryIsNoop — defensive guard for
// test fixtures (and pre-PR-11.3c code paths) that don't wire a
// registry. The handler must not panic.
func TestHandleRouteProbeAck_NoRegistryIsNoop(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA) // no probeRegistry

	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   42,
		Reachable: true,
	}
	svc.handleRouteProbeAck(idPeerB, ack)

	if snap := svc.routingTable.HealthSnapshot(); snap != nil {
		t.Fatalf("no-registry path created health state: %v", snap)
	}
}

// --- Service.probeTick: scheduling logic ---

// TestProbeTick_SkipsWhenOverloadGateEngaged — Phase 0 invariant:
// the probe loop respects the same overload gate the announce loop
// uses. When overload is engaged, the tick is a no-op and no
// probes are registered. Routes accumulating failures during
// overload converge to Bad through the slower passive hop_ack
// timeline (122s) — see phase-2 §4.7 Resolved decision #4.
func TestProbeTick_SkipsWhenOverloadGateEngaged(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	// Seed a Questionable pair so probeTick would otherwise probe.
	svc.routingTable.MarkHopAck(idTargetX, idPeerB, 0)
	svc.routingTable.ForceHealthForTest(idTargetX, idPeerB, routing.HealthQuestionable)

	// Trip the overload gate with a goroutine-count threshold of 1.
	// runtime.NumGoroutine() is well above 1 in any real test
	// execution, so IsOverloaded returns true.
	svc.overloadMonitor = newOverloadMonitor(1)
	if !svc.overloadMonitor.IsOverloaded() {
		t.Skip("overload gate did not engage at threshold=1 — runtime layout may have changed; not a Phase 2 regression")
	}

	svc.probeTick()

	if got := svc.probeRegistry.Len(); got != 0 {
		t.Fatalf("probeTick scheduled %d probes under engaged overload gate, want 0", got)
	}
}

// TestProbeTick_SchedulesProbeForQuestionablePair — happy path:
// a Questionable pair leads to one Register call (no actual send is
// asserted — that requires a netcore writer; the test fixture has
// none, but the registry record proves the scheduling decision).
//
// After PR 11.7 P2#4 sendProbe also requires a capable session
// for the uplink — without one, the probe is skipped before
// Register so the registry stays empty. The test installs a
// minimal session+health pair to satisfy peerSendableConnectionsLocked.
func TestProbeTick_SchedulesProbeForQuestionablePair(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	installCapableUplinkForTest(t, svc, idPeerB, domain.CapMeshRouteProbeV1)

	svc.routingTable.MarkHopAck(idTargetX, idPeerB, 0)
	svc.routingTable.ForceHealthForTest(idTargetX, idPeerB, routing.HealthQuestionable)

	svc.probeTick()

	if got := svc.probeRegistry.Len(); got != 1 {
		t.Fatalf("probeTick scheduled %d probes for one Questionable pair, want 1", got)
	}
}

// TestProbeTick_SkipsNonQuestionablePairs — Good/Bad/Dead pairs are
// not probed. Probes are an active check intended for the
// Questionable state specifically (overview §7.6).
func TestProbeTick_SkipsNonQuestionablePairs(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	svc.routingTable.MarkHopAck(idTargetX, idPeerB, 0) // Good
	// Don't transition: pair stays Good.

	svc.probeTick()

	if got := svc.probeRegistry.Len(); got != 0 {
		t.Fatalf("probeTick scheduled %d probes for Good pair, want 0", got)
	}
}

// TestProbeTick_RespectsPerPairRateLimit — the sender invariant:
// a Questionable pair with an outstanding probe is not probed
// again. This prevents request amplification when consecutive
// ticks see the same pair.
func TestProbeTick_RespectsPerPairRateLimit(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)

	installCapableUplinkForTest(t, svc, idPeerB, domain.CapMeshRouteProbeV1)

	svc.routingTable.MarkHopAck(idTargetX, idPeerB, 0)
	svc.routingTable.ForceHealthForTest(idTargetX, idPeerB, routing.HealthQuestionable)

	svc.probeTick()
	first := svc.probeRegistry.Len()
	svc.probeTick()
	second := svc.probeRegistry.Len()

	if first != 1 {
		t.Fatalf("first tick scheduled %d probes, want 1", first)
	}
	if second != 1 {
		t.Fatalf("second tick added probes despite outstanding: now %d, want 1", second)
	}
}
