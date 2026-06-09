package node

import (
	"io"
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestFetchTrafficTotalsDoesNotArmRebuildGate pins the core of the metrics
// collector decoupling: the per-second traffic poll must report correct
// cumulative totals while leaving the full network_stats rebuild-gate asleep on
// a node with no UI reader. Previously the collector polled fetch_network_stats,
// which stamped networkStatsAccessNanos and kept rebuildNetworkStatsSnapshot
// (knownSet + peerAddrs + peerTraffic allocations) running every 500 ms forever.
func TestFetchTrafficTotalsDoesNotArmRebuildGate(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	svc.peerMu.Lock()
	svc.health[domain.PeerAddress("peer-a")] = &peerHealth{
		Address:       "peer-a",
		BytesSent:     1000,
		BytesReceived: 2000,
		Connected:     true,
	}
	svc.health[domain.PeerAddress("peer-b")] = &peerHealth{
		Address:       "peer-b",
		BytesSent:     7,
		BytesReceived: 5,
		Connected:     false,
	}
	svc.peerMu.Unlock()

	// Gate starts disarmed.
	svc.networkStatsAccessNanos.Store(0)

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_traffic_totals"})
	if reply.NetworkStats == nil {
		t.Fatal("fetch_traffic_totals returned nil NetworkStats")
	}
	if got, want := reply.NetworkStats.TotalBytesSent, int64(1007); got != want {
		t.Fatalf("TotalBytesSent = %d, want %d", got, want)
	}
	if got, want := reply.NetworkStats.TotalBytesReceived, int64(2005); got != want {
		t.Fatalf("TotalBytesReceived = %d, want %d", got, want)
	}
	if got, want := reply.NetworkStats.TotalTraffic, int64(3012); got != want {
		t.Fatalf("TotalTraffic = %d, want %d", got, want)
	}

	// The whole point: the totals path must NOT stamp the reader-access clock,
	// so the background rebuild-gate can idle out on a headless node.
	if n := svc.networkStatsAccessNanos.Load(); n != 0 {
		t.Fatalf("fetch_traffic_totals armed the rebuild gate (access nanos = %d, want 0)", n)
	}

	// Consequently maybeRebuildNetworkStatsSnapshot is a no-op: no snapshot is
	// built as a side effect of the totals poll.
	svc.maybeRebuildNetworkStatsSnapshot()
	if snap := svc.loadNetworkStatsSnapshot(); snap != nil {
		t.Fatalf("totals poll caused a snapshot rebuild; snapshot = %+v", snap)
	}

	// Contrast guard: fetch_network_stats DOES arm the gate. If a future change
	// accidentally routes the collector back through the heavy path, the test
	// above starts failing while this one keeps passing — making the regression
	// unambiguous.
	svc.HandleLocalFrame(protocol.Frame{Type: "fetch_network_stats"})
	if svc.networkStatsAccessNanos.Load() == 0 {
		t.Fatal("fetch_network_stats should arm the rebuild gate but did not")
	}
}

// TestTrafficTotalsMatchFullSnapshot pins that the lightweight totals path
// produces byte-identical totals to the full rebuildNetworkStatsSnapshot path,
// so decoupling the collector did not silently change what it records. live
// traffic is left empty here (no active MeteredConn in the unit harness); the
// persisted-health summation is the part the collector depends on.
func TestTrafficTotalsMatchFullSnapshot(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	svc.peerMu.Lock()
	svc.health[domain.PeerAddress("p1")] = &peerHealth{Address: "p1", BytesSent: 11, BytesReceived: 22, Connected: true}
	svc.health[domain.PeerAddress("p2")] = &peerHealth{Address: "p2", BytesSent: 33, BytesReceived: 44, Connected: true}
	svc.health[domain.PeerAddress("p3")] = &peerHealth{Address: "p3", BytesSent: 0, BytesReceived: 9, Connected: false}
	svc.peerMu.Unlock()

	svc.rebuildNetworkStatsSnapshot()
	full := svc.loadNetworkStatsSnapshot()
	if full == nil {
		t.Fatal("expected primed snapshot")
	}

	totals := svc.trafficTotalsFrame()
	if totals.NetworkStats == nil {
		t.Fatal("trafficTotalsFrame returned nil NetworkStats")
	}
	if totals.NetworkStats.TotalBytesSent != full.totalSent {
		t.Fatalf("TotalBytesSent: totals=%d full=%d", totals.NetworkStats.TotalBytesSent, full.totalSent)
	}
	if totals.NetworkStats.TotalBytesReceived != full.totalReceived {
		t.Fatalf("TotalBytesReceived: totals=%d full=%d", totals.NetworkStats.TotalBytesReceived, full.totalReceived)
	}
}

// TestTrafficTotalsMatchFullSnapshotWithLiveConn extends the equivalence check
// to include LIVE traffic from an active MeteredConn — the part that
// sumLiveTrafficLocked (fast path) and liveTrafficLocked (full snapshot) compute
// independently. Without a live-conn case the two could drift in their
// filtering/summation (trusted gate, empty-address skip, per-address grouping)
// and the persisted-only test would never notice.
func TestTrafficTotalsMatchFullSnapshotWithLiveConn(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	const persistedSent = int64(100)
	const persistedRecv = int64(200)
	svc.peerMu.Lock()
	svc.health[domain.PeerAddress("p1")] = &peerHealth{
		Address:       "p1",
		BytesSent:     persistedSent,
		BytesReceived: persistedRecv,
		Connected:     true,
	}
	svc.peerMu.Unlock()

	// An outbound session backed by a MeteredConn carrying real written bytes.
	const liveWritten = 512
	m := newMeteredWithWrittenBytes(t, liveWritten)
	addr := domain.PeerAddress("live-peer:64646")
	svc.peerMu.Lock()
	svc.sessions[addr] = &peerSession{address: addr, metered: m}
	svc.peerMu.Unlock()

	svc.rebuildNetworkStatsSnapshot()
	full := svc.loadNetworkStatsSnapshot()
	if full == nil {
		t.Fatal("expected primed snapshot")
	}

	totals := svc.trafficTotalsFrame()
	if totals.NetworkStats == nil {
		t.Fatal("trafficTotalsFrame returned nil NetworkStats")
	}
	if totals.NetworkStats.TotalBytesSent != full.totalSent {
		t.Fatalf("TotalBytesSent: totals=%d full=%d", totals.NetworkStats.TotalBytesSent, full.totalSent)
	}
	if totals.NetworkStats.TotalBytesReceived != full.totalReceived {
		t.Fatalf("TotalBytesReceived: totals=%d full=%d", totals.NetworkStats.TotalBytesReceived, full.totalReceived)
	}

	// Guard against both paths silently dropping live traffic and still
	// "agreeing" at the persisted-only sum: the live conn must have contributed.
	if want := persistedSent + liveWritten; full.totalSent != want {
		t.Fatalf("live traffic not included in totals: totalSent=%d want %d", full.totalSent, want)
	}
}

// TestTrafficTotalsInboundConnsMatchSnapshot exercises the inbound walk that the
// latest optimisation swapped from forEachInboundConnLocked to the
// capability-free forEachInboundConnIDLocked. It registers three inbound metered
// connections — trusted, untrusted, and trusted-but-empty-address — and asserts
// that the fast totals path applies EXACTLY the same trust / empty-address
// filtering as liveTrafficLocked (via the full snapshot), so the two cannot
// drift precisely where the inbound path was just sped up.
func TestTrafficTotalsInboundConnsMatchSnapshot(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	const persistedSent = int64(10)
	const persistedRecv = int64(20)
	svc.peerMu.Lock()
	svc.health[domain.PeerAddress("p1")] = &peerHealth{
		Address:       "p1",
		BytesSent:     persistedSent,
		BytesReceived: persistedRecv,
		Connected:     true,
	}
	svc.peerMu.Unlock()

	// Trusted (Auth nil ⇒ verified-or-no-auth-required) — must be counted.
	const trustedWritten = 256
	registerInboundMeteredConn(t, svc, 101, "in-trusted:64646", trustedWritten, nil)
	// Untrusted (Auth present, Verified=false) — must be skipped.
	registerInboundMeteredConn(t, svc, 102, "in-untrusted:64646", 1024, &connauth.State{Verified: false})
	// Trusted but empty overlay address — must be skipped (anti-spoof guard).
	registerInboundMeteredConn(t, svc, 103, "", 2048, nil)

	svc.rebuildNetworkStatsSnapshot()
	full := svc.loadNetworkStatsSnapshot()
	if full == nil {
		t.Fatal("expected primed snapshot")
	}

	totals := svc.trafficTotalsFrame()
	if totals.NetworkStats == nil {
		t.Fatal("trafficTotalsFrame returned nil NetworkStats")
	}

	// Fast path agrees with the full snapshot on both totals.
	if totals.NetworkStats.TotalBytesSent != full.totalSent {
		t.Fatalf("TotalBytesSent: totals=%d full=%d", totals.NetworkStats.TotalBytesSent, full.totalSent)
	}
	if totals.NetworkStats.TotalBytesReceived != full.totalReceived {
		t.Fatalf("TotalBytesReceived: totals=%d full=%d", totals.NetworkStats.TotalBytesReceived, full.totalReceived)
	}

	// And the absolute value proves the filtering is correct, not merely
	// equal-because-both-wrong: only the trusted, non-empty-address conn counts.
	if want := persistedSent + trustedWritten; full.totalSent != want {
		t.Fatalf("inbound filtering wrong: totalSent=%d want %d (only trusted+addressed conn should count)", full.totalSent, want)
	}
	if full.totalReceived != persistedRecv {
		t.Fatalf("inbound filtering wrong: totalReceived=%d want %d", full.totalReceived, persistedRecv)
	}
}

// registerInboundMeteredConn builds an inbound NetCore over a MeteredConn seeded
// with `written` bytes and registers it in the connection registry. auth==nil
// leaves the connection unauthenticated (treated as trusted: no session-auth
// required); a non-nil auth sets the verified flag explicitly.
func registerInboundMeteredConn(t *testing.T, svc *Service, id uint64, addr domain.PeerAddress, written int, auth *connauth.State) {
	t.Helper()
	local, remote := net.Pipe()
	m := netcore.NewMeteredConn(local)
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	if written > 0 {
		if _, err := m.Write(make([]byte, written)); err != nil {
			t.Fatalf("seed inbound write: %v", err)
		}
	}
	core := netcore.New(netcore.ConnID(id), m, netcore.Inbound, netcore.Options{Address: addr})
	if auth != nil {
		core.SetAuth(auth)
	}
	svc.peerMu.Lock()
	svc.registerInboundConnLocked(m, core, m)
	svc.peerMu.Unlock()
	t.Cleanup(func() {
		core.Close()
		_ = local.Close()
		_ = remote.Close()
	})
}

// newMeteredWithWrittenBytes returns a MeteredConn whose BytesWritten counter
// has been advanced by exactly n via a real Write. net.Pipe is synchronous, so
// a background drain keeps the Write from blocking; the pipe is closed on
// cleanup, unblocking the drainer.
func newMeteredWithWrittenBytes(t *testing.T, n int) *netcore.MeteredConn {
	t.Helper()
	local, remote := net.Pipe()
	m := netcore.NewMeteredConn(local)
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	if _, err := m.Write(make([]byte, n)); err != nil {
		t.Fatalf("seed metered write: %v", err)
	}
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	return m
}
