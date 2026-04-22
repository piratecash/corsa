package node

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/routing"
)

// Test cluster: withdrawal fanout inside onPeerSessionClosed.
//
// The bug under regression: when a relay-capable direct peer disappears,
// onPeerSessionClosed synthesised an own-origin withdrawal and fanned it
// out to every routing-capable peer sequentially:
//
//	for _, peer := range peers {
//	    s.SendAnnounceRoutes(peer.Address, result.Withdrawals)
//	}
//
// For inbound peers, SendAnnounceRoutes is a synchronous send bounded by
// syncFlushTimeout (5s). Three stuck sockets therefore wedged the caller
// for ~15s, which in production matched exactly the fetch_network_stats
// ticker stall observed in the desktop traffic chart. The fix
// parallelises the fanout so one stuck peer cannot serialise the others.
//
// The tests in this file pin the parallelism contract, the observability
// counters, the runCtx cancellation behaviour, and the degenerate empty-
// peer case. They share a single fanoutTestNetwork fixture.

// fanoutPeerBehaviour drives one target's SendFrameSync outcome.
// Production collapses on netcore.ErrSendTimeout after syncFlushTimeout;
// tests use a much shorter Delay so the suite finishes quickly.
type fanoutPeerBehaviour struct {
	remoteAddr string
	delay      time.Duration
	err        error // nil = success
}

// fanoutTestNetwork is a netcore.Network fixture focused on the
// onPeerSessionClosed withdrawal-fanout path.
//
// It honours the `Network.SendFrameSync` contract — block until either
// the configured err after `delay` or ctx cancellation — and counts
// calls per ConnID so tests can assert exactly-once delivery semantics.
// The other Network methods are implemented as safe no-ops so the
// fixture can be installed through svc.networkOverride without bringing
// up the full netcoretest.Backend (which, as of this commit, has no
// per-id delay knob).
type fanoutTestNetwork struct {
	mu       sync.Mutex
	peers    map[domain.ConnID]fanoutPeerBehaviour
	calls    map[domain.ConnID]*int32
	closeCnt int32
}

func newFanoutTestNetwork() *fanoutTestNetwork {
	return &fanoutTestNetwork{
		peers: make(map[domain.ConnID]fanoutPeerBehaviour),
		calls: make(map[domain.ConnID]*int32),
	}
}

// register wires a per-ConnID behaviour into the fixture. Must be
// called before the Service's fanout path reaches this id.
func (n *fanoutTestNetwork) register(id domain.ConnID, beh fanoutPeerBehaviour) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[id] = beh
	var c int32
	n.calls[id] = &c
}

// callCount returns the number of SendFrameSync invocations observed
// for the given id. Safe for concurrent use.
func (n *fanoutTestNetwork) callCount(id domain.ConnID) int32 {
	n.mu.Lock()
	c := n.calls[id]
	n.mu.Unlock()
	if c == nil {
		return 0
	}
	return atomic.LoadInt32(c)
}

// closeCount returns the number of Close invocations observed across
// all ids. Production calls Close on timeout to evict a stuck peer; the
// test Network keeps the count as a sanity check.
func (n *fanoutTestNetwork) closeCount() int32 {
	return atomic.LoadInt32(&n.closeCnt)
}

// --- netcore.Network implementation ---

var _ netcore.Network = (*fanoutTestNetwork)(nil)

func (n *fanoutTestNetwork) SendFrame(context.Context, domain.ConnID, []byte) error {
	return nil
}

func (n *fanoutTestNetwork) SendFrameSync(ctx context.Context, id domain.ConnID, _ []byte) error {
	n.mu.Lock()
	beh, ok := n.peers[id]
	counter := n.calls[id]
	n.mu.Unlock()
	if !ok {
		return netcore.ErrUnknownConn
	}
	if counter != nil {
		atomic.AddInt32(counter, 1)
	}
	if beh.delay <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return beh.err
	}
	timer := time.NewTimer(beh.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return beh.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *fanoutTestNetwork) Enumerate(context.Context, netcore.Direction, func(domain.ConnID) bool) {
}

func (n *fanoutTestNetwork) Close(context.Context, domain.ConnID) error {
	atomic.AddInt32(&n.closeCnt, 1)
	return nil
}

func (n *fanoutTestNetwork) RemoteAddr(id domain.ConnID) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.peers[id].remoteAddr
}

// --- fixture builder ---

// fanoutTarget describes one routing-capable inbound peer the test
// wants the fanout to reach. Identity is the peer's PeerIdentity
// (used by routingCapablePeers for dedup); remoteAddr is the string
// returned by both the NetCore and the fanoutTestNetwork for that id —
// the two must agree or writeFrameToInbound will not resolve the id.
type fanoutTarget struct {
	identity   domain.PeerIdentity
	remoteAddr string
	delay      time.Duration
	err        error // nil = success
}

// fanoutFixture is the set of handles tests need after setup: the
// Service under test, the injected Network, and the ConnIDs of the
// routing-capable inbound peers in registration order.
type fanoutFixture struct {
	svc     *Service
	net     *fanoutTestNetwork
	connIDs []domain.ConnID
}

// setupFanoutFixture builds a Service primed to execute the withdrawal
// fanout for a synthetic relay-capable peer (disconnectingIdentity).
//
// The Service has:
//   - runCtx: Background (caller overrides when needed, e.g. test #9).
//   - routingTable + announceLoop: real, no-op mock PeerSender.
//   - identitySessions / identityRelaySessions primed to 1 so
//     onPeerSessionClosed transitions through the last-relay branch.
//   - routingTable.AddDirectPeer(disconnectingIdentity) so
//     RemoveDirectPeer produces a non-empty withdrawal on close.
//   - len(targets) routing-capable inbound connections registered in
//     s.conns with tracked=true and both CapMeshRoutingV1 + CapMeshRelayV1.
//   - svc.networkOverride pointing at the fanoutTestNetwork, so every
//     inbound SendFrameSync lands on the fixture's per-id behaviour.
func setupFanoutFixture(t *testing.T, disconnectingIdentity domain.PeerIdentity, targets []fanoutTarget) *fanoutFixture {
	t.Helper()

	svc := newTestServiceWithRouting(t, idNodeA)
	svc.conns = make(map[netcore.ConnID]*connEntry)
	svc.connIDByNetConn = make(map[net.Conn]netcore.ConnID)

	// Seed the session counters so onPeerSessionClosed's
	// decrement-from-one path triggers RemoveDirectPeer.
	svc.identitySessions[disconnectingIdentity] = 1
	svc.identityRelaySessions[disconnectingIdentity] = 1

	if _, err := svc.routingTable.AddDirectPeer(routing.PeerIdentity(disconnectingIdentity)); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	netFixture := newFanoutTestNetwork()
	svc.networkOverride = netFixture

	connIDs := make([]domain.ConnID, 0, len(targets))
	for i, target := range targets {
		id := domain.ConnID(100 + i)

		pipeLocal, pipeRemote := net.Pipe()
		t.Cleanup(func() { _ = pipeLocal.Close() })
		t.Cleanup(func() { _ = pipeRemote.Close() })

		host, port := splitHostPortOrPanic(t, target.remoteAddr)
		conn := &fakeConn{
			Conn:       pipeLocal,
			remoteAddr: &net.TCPAddr{IP: net.ParseIP(host), Port: port},
		}

		pc := netcore.New(id, conn, netcore.Inbound, netcore.Options{
			Address:  domain.PeerAddress(target.identity),
			Identity: target.identity,
			Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
		})
		t.Cleanup(pc.Close)

		svc.peerMu.Lock()
		svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
		svc.peerMu.Unlock()

		netFixture.register(id, fanoutPeerBehaviour{
			remoteAddr: target.remoteAddr,
			delay:      target.delay,
			err:        target.err,
		})
		connIDs = append(connIDs, id)
	}

	return &fanoutFixture{svc: svc, net: netFixture, connIDs: connIDs}
}

// splitHostPortOrPanic parses "host:port" into its pieces; tests pass
// only literal strings so a parse error is a test-authoring bug.
func splitHostPortOrPanic(t *testing.T, hp string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(hp)
	if err != nil {
		t.Fatalf("net.SplitHostPort(%q): %v", hp, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("strconv.Atoi(%q): %v", portStr, err)
	}
	return host, port
}

// --- test cases ---

// TestOnPeerSessionClosed_FanoutIsParallel pins the parallelism
// contract. With three routing-capable inbound peers, two of which
// take stuckDelay=300ms to return ErrSendTimeout and one of which
// returns nil immediately, the elapsed wall time must be bounded by
// stuckDelay + slack, not 2*stuckDelay. If the loop is serial the
// elapsed time would be ~2*stuckDelay and the test fails — exactly
// the regression this PR fixes.
func TestOnPeerSessionClosed_FanoutIsParallel(t *testing.T) {
	t.Parallel()

	const (
		stuckDelay = 300 * time.Millisecond
		slack      = 250 * time.Millisecond
	)

	targets := []fanoutTarget{
		{identity: idPeerA, remoteAddr: "10.0.0.1:7001", delay: stuckDelay, err: netcore.ErrSendTimeout},
		{identity: idPeerC, remoteAddr: "10.0.0.3:7003", delay: 0, err: nil},
		{identity: idPeerD, remoteAddr: "10.0.0.4:7004", delay: stuckDelay, err: netcore.ErrSendTimeout},
	}
	fx := setupFanoutFixture(t, idPeerB, targets)

	start := time.Now()
	fx.svc.onPeerSessionClosed(idPeerB, true)
	elapsed := time.Since(start)

	if elapsed > stuckDelay+slack {
		t.Fatalf("fanout ran serially: elapsed=%v, want <= %v (stuckDelay=%v + slack=%v)",
			elapsed, stuckDelay+slack, stuckDelay, slack)
	}
	for i, id := range fx.connIDs {
		if got := fx.net.callCount(id); got != 1 {
			t.Fatalf("target %d (id=%d): SendFrameSync calls=%d, want=1", i, id, got)
		}
	}
}

// TestOnPeerSessionClosed_FanoutRespectsRunCtx pins that a fanout in
// flight exits promptly when the Service's runCtx is cancelled —
// otherwise a Service.Shutdown() during a stuck-peer storm would leak
// the fanout goroutines until every per-peer syncFlushTimeout elapses.
func TestOnPeerSessionClosed_FanoutRespectsRunCtx(t *testing.T) {
	t.Parallel()

	const (
		stuckDelay = 2 * time.Second // long enough that cancel wins
		cancelAt   = 100 * time.Millisecond
		slack      = 500 * time.Millisecond
	)

	targets := []fanoutTarget{
		{identity: idPeerA, remoteAddr: "10.1.0.1:7001", delay: stuckDelay, err: netcore.ErrSendTimeout},
		{identity: idPeerC, remoteAddr: "10.1.0.3:7003", delay: stuckDelay, err: netcore.ErrSendTimeout},
	}
	fx := setupFanoutFixture(t, idPeerB, targets)

	ctx, cancel := context.WithCancel(context.Background())
	fx.svc.runCtx = ctx

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.svc.onPeerSessionClosed(idPeerB, true)
	}()

	time.Sleep(cancelAt)
	cancel()

	select {
	case <-done:
		// OK: fanout observed runCtx cancellation and returned.
	case <-time.After(cancelAt + slack):
		t.Fatalf("onPeerSessionClosed did not exit within %v after runCtx cancel", slack)
	}
}

// TestOnPeerSessionClosed_NoPanicOnZeroPeers covers the degenerate
// branch: the withdrawal is produced (direct peer existed) but no
// routing-capable peers remain to receive it. The fanout must return
// cleanly without touching the Network.
func TestOnPeerSessionClosed_NoPanicOnZeroPeers(t *testing.T) {
	t.Parallel()

	fx := setupFanoutFixture(t, idPeerB, nil)

	// Sanity: no targets, so SendFrameSync must never be reached.
	fx.svc.onPeerSessionClosed(idPeerB, true)

	if got := fx.net.closeCount(); got != 0 {
		t.Fatalf("unexpected Close calls with zero fanout targets: %d", got)
	}
}

// TestOnPeerSessionClosed_CollectsSentAndDroppedCounters pins the
// observability contract of the new fanout helper: after the fanout
// returns, the helper's sent/dropped counters reflect the number of
// peers that accepted vs. timed out. The counters are surfaced on the
// routing_direct_peer_removed log line so operators can see degraded
// fan-out health without re-parsing per-peer noise. Here we assert on
// the per-id SendFrameSync call counts directly — the log aggregation
// is an implementation detail of fanoutAnnounceRoutes and will be
// asserted once that helper lands.
func TestOnPeerSessionClosed_CollectsSentAndDroppedCounters(t *testing.T) {
	t.Parallel()

	const stuckDelay = 100 * time.Millisecond

	targets := []fanoutTarget{
		{identity: idPeerA, remoteAddr: "10.2.0.1:7001", delay: 0, err: nil},                           // sent
		{identity: idPeerC, remoteAddr: "10.2.0.3:7003", delay: stuckDelay, err: netcore.ErrSendTimeout}, // dropped
		{identity: idPeerD, remoteAddr: "10.2.0.4:7004", delay: 0, err: nil},                           // sent
		{identity: "ee00000000000000000000000000000000000005", remoteAddr: "10.2.0.5:7005", delay: stuckDelay, err: netcore.ErrSendTimeout}, // dropped
	}
	fx := setupFanoutFixture(t, idPeerB, targets)

	fx.svc.onPeerSessionClosed(idPeerB, true)

	// All four peers must have received exactly one SendFrameSync.
	for i, id := range fx.connIDs {
		if got := fx.net.callCount(id); got != 1 {
			t.Fatalf("target %d (id=%d): SendFrameSync calls=%d, want=1", i, id, got)
		}
	}

	// The two stuck peers must have been force-closed by
	// sendFrameBytesViaNetworkSync on ErrSendTimeout.
	if got := fx.net.closeCount(); got != 2 {
		t.Fatalf("Close count = %d, want 2 (two stuck peers)", got)
	}
}

// Guard against a subtle fixture bug: if setupFanoutFixture ever stops
// plumbing runCtx, test #9 would observe a nil-deref instead of the
// intended cancellation behaviour. Keep the check visible.
func TestFanoutFixture_HasRunCtx(t *testing.T) {
	t.Parallel()
	fx := setupFanoutFixture(t, idPeerB, nil)
	if fx.svc.runCtx == nil {
		t.Fatal("fixture service must have a non-nil runCtx")
	}
	if errors.Is(fx.svc.runCtx.Err(), context.Canceled) {
		t.Fatal("fixture service must start with a live runCtx")
	}
}
