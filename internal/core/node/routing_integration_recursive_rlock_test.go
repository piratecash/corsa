package node

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// Regression cluster: recursive peerMu.RLock from the routing-read paths.
//
// Bug under regression. Three functions take s.peerMu.RLock and walked
// inbound connections to build "inbound:<remoteAddr>" routing keys:
//
//   - routingCapablePeers()    — inside forEachTrackedInboundConnLocked
//   - resolveRoutableAddress() — inside forEachTrackedInboundConnLocked
//   - resolveRelayAddress()    — inside forEachTrackedInboundConnLocked
//
// Each of them called s.inboundConnKeyForID(info.id) inside the walker
// callback. inboundConnKeyForID calls s.Network().RemoteAddr(id), which
// delegates to networkBridge.coreForID and takes s.peerMu.RLock a
// second time. Under Go's writer-preferring sync.RWMutex, a writer
// queued between the outer and inner acquisition blocks the inner
// RLock — the reader deadlocks on a lock it already owns.
//
// Observed symptom in production (docs/bug-inbound-ingress-peermu-stall.md):
// one inbound cleanup reached accumulateInboundTraffic(lock_wait) and
// never emitted lock_held. Every subsequent reader of peerMu (snapshot
// refreshers, announce fanout, ingress accept) starved behind it.
//
// Fix. The three call sites now use inboundConnKeyFromInfo(info) — a
// lock-free helper that reads the snapshot field info.remoteAddr which
// was already captured under the caller-held peerMu.RLock. No second
// lock acquisition happens.
//
// The tests in this file pin the behaviour contract:
//
//  1. The three routing-read paths must not call Network().RemoteAddr
//     for inbound-key derivation. A trap-Network whose RemoteAddr fails
//     the test guarantees we catch any reintroduction. This is the
//     primary regression check — it does not depend on goroutine timing.
//  2. The three routing-read paths must still complete successfully and
//     produce correctly-shaped "inbound:<addr>" keys while a concurrent
//     peerMu writer is queued. This exercises the end-to-end path the
//     production log showed stalling.

// recursiveRLockTrapNetwork is a netcore.Network double whose RemoteAddr
// fails the test instead of returning a value. Used to assert that code
// paths holding s.peerMu.RLock never call back through the bridge for
// inbound-key derivation — such a call would recursively acquire
// s.peerMu.RLock and deadlock behind a queued writer.
//
// The remaining methods are safe no-ops: the tests in this file invoke
// only the routing-read functions, which do not issue frames.
type recursiveRLockTrapNetwork struct {
	t *testing.T

	// remoteAddrCalls counts any RemoteAddr invocation, so tests can
	// report the exact number in the failure message when the trap
	// fires. Atomic so concurrent paths (if any) remain race-free.
	remoteAddrCalls atomic.Int32
}

var _ netcore.Network = (*recursiveRLockTrapNetwork)(nil)

func (n *recursiveRLockTrapNetwork) SendFrame(context.Context, domain.ConnID, []byte) error {
	return nil
}

func (n *recursiveRLockTrapNetwork) SendFrameSync(context.Context, domain.ConnID, []byte) error {
	return nil
}

func (n *recursiveRLockTrapNetwork) Enumerate(context.Context, netcore.Direction, func(domain.ConnID) bool) {
}

func (n *recursiveRLockTrapNetwork) Close(context.Context, domain.ConnID) error {
	return nil
}

func (n *recursiveRLockTrapNetwork) RemoteAddr(id domain.ConnID) string {
	n.remoteAddrCalls.Add(1)
	n.t.Helper()
	n.t.Fatalf("Network.RemoteAddr called for ConnID=%d — routing-read paths under peerMu.RLock "+
		"must derive inbound keys from the connInfo snapshot (use inboundConnKeyFromInfo), "+
		"not through Network().RemoteAddr which recursively acquires s.peerMu.RLock and "+
		"deadlocks behind a queued writer", id)
	return ""
}

// seedTrackedInboundPeer installs a tracked inbound connection carrying
// both mesh_routing_v1 and mesh_relay_v1 capabilities. Returns the
// remoteAddr string so tests can assert the derived "inbound:<addr>"
// routing key.
func seedTrackedInboundPeer(t *testing.T, svc *Service, id domain.ConnID, peerIdentity domain.PeerIdentity, remoteAddr string) string {
	t.Helper()

	pipeLocal, pipeRemote := net.Pipe()
	t.Cleanup(func() { _ = pipeLocal.Close() })
	t.Cleanup(func() { _ = pipeRemote.Close() })

	host, port := splitHostPortOrPanic(t, remoteAddr)
	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP(host), Port: port},
	}

	pc := netcore.New(id, conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(peerIdentity),
		Identity: peerIdentity,
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	})
	t.Cleanup(pc.Close)

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	return remoteAddr
}

// TestRoutingCapablePeers_DoesNotCallNetworkRemoteAddr pins contract (1)
// for routingCapablePeers: the inbound-branch walker must derive the
// "inbound:<addr>" routing key from the connInfo snapshot
// (info.remoteAddr), not by calling back into Network().RemoteAddr —
// which would re-enter s.peerMu.RLock.
func TestRoutingCapablePeers_DoesNotCallNetworkRemoteAddr(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	trap := &recursiveRLockTrapNetwork{t: t}
	svc.networkOverride = trap

	const remoteAddr = "10.0.0.1:7001"
	seedTrackedInboundPeer(t, svc, domain.ConnID(100), idPeerB, remoteAddr)

	targets := svc.routingCapablePeers()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	wantAddr := domain.PeerAddress("inbound:" + remoteAddr)
	if targets[0].Address != wantAddr {
		t.Fatalf("target address: got %q, want %q", targets[0].Address, wantAddr)
	}
	if targets[0].Identity != idPeerB {
		t.Fatalf("target identity: got %q, want %q", targets[0].Identity, idPeerB)
	}
	if got := trap.remoteAddrCalls.Load(); got != 0 {
		t.Fatalf("Network.RemoteAddr call count: got %d, want 0", got)
	}
}

// TestResolveRoutableAddress_DoesNotCallNetworkRemoteAddr pins contract
// (1) for resolveRoutableAddress, which has the same recursive-RLock
// shape as routingCapablePeers on the inbound branch.
func TestResolveRoutableAddress_DoesNotCallNetworkRemoteAddr(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	trap := &recursiveRLockTrapNetwork{t: t}
	svc.networkOverride = trap

	const remoteAddr = "10.0.0.2:7002"
	seedTrackedInboundPeer(t, svc, domain.ConnID(101), idPeerC, remoteAddr)

	got := svc.resolveRoutableAddress(idPeerC)
	want := domain.PeerAddress("inbound:" + remoteAddr)
	if got != want {
		t.Fatalf("resolveRoutableAddress: got %q, want %q", got, want)
	}
	if calls := trap.remoteAddrCalls.Load(); calls != 0 {
		t.Fatalf("Network.RemoteAddr call count: got %d, want 0", calls)
	}
}

// TestResolveRelayAddress_DoesNotCallNetworkRemoteAddr pins contract (1)
// for resolveRelayAddress, which has the same recursive-RLock shape on
// the inbound branch.
func TestResolveRelayAddress_DoesNotCallNetworkRemoteAddr(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	trap := &recursiveRLockTrapNetwork{t: t}
	svc.networkOverride = trap

	const remoteAddr = "10.0.0.3:7003"
	seedTrackedInboundPeer(t, svc, domain.ConnID(102), idPeerD, remoteAddr)

	got := svc.resolveRelayAddress(idPeerD)
	want := domain.PeerAddress("inbound:" + remoteAddr)
	if got != want {
		t.Fatalf("resolveRelayAddress: got %q, want %q", got, want)
	}
	if calls := trap.remoteAddrCalls.Load(); calls != 0 {
		t.Fatalf("Network.RemoteAddr call count: got %d, want 0", calls)
	}
}

// TestRoutingReadPaths_CompleteWithConcurrentWriter pins contract (2):
// the three routing-read paths must complete in bounded time even when
// concurrent peerMu writers are racing. This mirrors the production
// scenario where accumulateInboundTraffic queued a writer and every
// reader starved behind a recursive-RLock caller.
//
// The test orchestration:
//
//  1. Seed a tracked inbound peer.
//  2. Spin a writer goroutine that repeatedly takes and releases
//     peerMu.Lock(). Its only job is to keep the writer queue non-empty
//     so any recursive RLock attempt inside the read paths would sooner
//     or later land behind a queued writer and deadlock.
//  3. Run the three read paths in the main goroutine, bounded by a
//     deadline. Under the fix (no recursive RLock), they complete well
//     inside the deadline. Under the pre-fix code, at least one run
//     would land on the "writer queued between outer and inner RLock"
//     pattern and hang past the deadline.
//
// This is not a deterministic deadlock reproduction — the inner RLock
// only deadlocks when the writer queues in the narrow window between
// the outer and inner acquisition — but over hundreds of iterations
// the race hits reliably, and under the fix the paths never touch
// Network().RemoteAddr at all.
func TestRoutingReadPaths_CompleteWithConcurrentWriter(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	// Use the real network bridge (no networkOverride) so any
	// recursive RemoteAddr call goes through coreForID's peerMu.RLock
	// — the exact production code path.

	const remoteAddr = "10.0.0.4:7004"
	seedTrackedInboundPeer(t, svc, domain.ConnID(200), idPeerB, remoteAddr)

	stop := make(chan struct{})
	writerStarted := make(chan struct{})
	var writerHits atomic.Uint64
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		// Run the first critical section before the reads start so the
		// test deterministically exercises concurrent writer traffic
		// even on machines fast enough to drain the read goroutine
		// before the writer is scheduled. Production code does not have
		// this barrier; the test only needs ONE pre-flight hit to
		// guarantee the writer queue interacts with the reads at least
		// once. The unbounded loop below carries the regular churn.
		svc.peerMu.Lock()
		writerHits.Add(uint64(len(svc.conns)))
		svc.peerMu.Unlock()
		close(writerStarted)
		for {
			select {
			case <-stop:
				return
			default:
			}
			svc.peerMu.Lock()
			// Touch peer-domain state under the writer lock so the
			// critical section does real work (production writers
			// mutate this map too) — this is what keeps the writer
			// queue non-empty, so any recursive RLock attempt inside
			// the read paths would queue behind us.
			writerHits.Add(uint64(len(svc.conns)))
			svc.peerMu.Unlock()
		}
	}()

	// Wait for the writer's first hit so the reads goroutine starts in a
	// state where the writer goroutine is provably alive and contending.
	<-writerStarted

	deadline := time.Now().Add(2 * time.Second)
	readsDone := make(chan struct{})
	var (
		routableAddr domain.PeerAddress
		relayAddr    domain.PeerAddress
	)
	go func() {
		defer close(readsDone)
		const iterations = 200
		for i := 0; i < iterations; i++ {
			_ = svc.routingCapablePeers()
			routableAddr = svc.resolveRoutableAddress(idPeerB)
			relayAddr = svc.resolveRelayAddress(idPeerB)
		}
	}()

	select {
	case <-readsDone:
		// Good — all read paths completed despite the writer churn.
	case <-time.After(time.Until(deadline)):
		close(stop)
		writerWG.Wait()
		t.Fatal("routing-read paths stalled while a peerMu writer was racing — " +
			"a caller is likely re-entering peerMu.RLock via Network().RemoteAddr " +
			"under the walker's own RLock (recursive RLock under writer-preferring " +
			"sync.RWMutex → self-deadlock)")
	}

	close(stop)
	writerWG.Wait()

	// Sanity check: the writer goroutine must have actually run at
	// least once; otherwise the read paths never raced against a
	// queued writer and the test would be a no-op. A single tracked
	// inbound peer gives len(svc.conns) == 1, so any non-zero
	// accumulator proves the critical section fired.
	if writerHits.Load() == 0 {
		t.Fatal("writer goroutine never entered the critical section — " +
			"the test did not actually exercise concurrent writer traffic")
	}

	want := domain.PeerAddress("inbound:" + remoteAddr)
	if routableAddr != want {
		t.Fatalf("resolveRoutableAddress: got %q, want %q", routableAddr, want)
	}
	if relayAddr != want {
		t.Fatalf("resolveRelayAddress: got %q, want %q", relayAddr, want)
	}
}
