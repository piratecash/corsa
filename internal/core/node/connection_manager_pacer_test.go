package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Dial pacer — global rate limiter on outbound dial goroutines.
//
// Without the pacer, fill() spawns N dial workers in a single tight loop
// (one per free slot). When the CM is in a reconnect storm against the
// same handful of bootstrap nodes, this produces a thundering herd: every
// 2-3 seconds, 7 simultaneous TCP handshakes + welcome + auth + peer-side
// announce_routes flush. CPU climbs to 100% while no session ever
// completes setup.
//
// The pacer is a token bucket:
//   - capacity = DialPacerBurst (default 3)
//   - refill rate = 1 token per DialPacerInterval (default 300ms)
//
// Acquired before every dial worker (fill, retryAfterBackoff, reconnect)
// EXCEPT for ManualPeerRequested — operator-driven actions bypass the
// pacer so `addpeer` is immediate.
// ---------------------------------------------------------------------------

// TestCM_DialPacer_LimitsSpawnRate verifies that with a small burst and a
// non-trivial interval, fill() does NOT launch all N dial workers in one
// instant — they are spaced out by the pacer.
func TestCM_DialPacer_LimitsSpawnRate(t *testing.T) {
	t.Parallel()

	const peers = 5
	addrs := make([]string, peers)
	for i := range addrs {
		addrs[i] = mustAddrN(i)
	}

	b := testCMConfig(addrs...)
	b.Cfg.MaxSlotsFn = func() int { return peers }
	b.Cfg.DialPacerInterval = 100 * time.Millisecond
	b.Cfg.DialPacerBurst = 2

	dialTimes := make(chan time.Time, peers)
	b.Cfg.DialFn = func(ctx context.Context, a []domain.PeerAddress) (DialResult, error) {
		dialTimes <- time.Now()
		session := fakePeerSession(a[0], "id-"+domain.PeerIdentity(a[0]))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 5*time.Second, "all dials issued", func() bool {
		return len(dialTimes) >= peers
	})

	// First two dials are within the burst window; the remaining (peers-2)
	// must be spaced by at least DialPacerInterval each.
	stamps := make([]time.Time, 0, peers)
	for i := 0; i < peers; i++ {
		stamps = append(stamps, <-dialTimes)
	}

	// Sanity: post-burst dials must be paced by at least
	// DialPacerInterval (minus a small jitter tolerance to absorb
	// timer granularity, GC pauses, and the -race overhead — observed
	// real-world jitter is ~80μs but we allow up to 10% slack).
	//
	// Asserting on per-interval spacing rather than total elapsed
	// from t=0 isolates the pacer from any setup latency between
	// NotifyBootstrapReady and the first dial. Without this isolation
	// the test was flaky under -race: total elapsed could miss the
	// target by ~74μs (299.926ms vs 300ms) without anything actually
	// being wrong.
	const (
		intervalNominal = 100 * time.Millisecond
		jitterTolerance = intervalNominal / 10 // 10ms slack
	)
	minInterval := intervalNominal - jitterTolerance
	// Post-burst index range: burst-th dial (index = burst) is the
	// first paced one; each gap to the next must be >= minInterval.
	const burst = 2
	for i := burst; i < peers; i++ {
		gap := stamps[i].Sub(stamps[i-1])
		if gap < minInterval {
			t.Fatalf("post-burst dial %d fired %v after dial %d, want >= %v (pacer is too lax)", i, gap, i-1, minInterval)
		}
	}
}

// TestCM_DialPacer_ManualBypasses checks that ManualPeerRequested dials
// fire immediately even when the pacer would otherwise hold them.
func TestCM_DialPacer_ManualBypasses(t *testing.T) {
	t.Parallel()

	b := testCMConfig() // empty bootstrap set
	b.Cfg.MaxSlotsFn = func() int { return 4 }
	// Very slow pacer — non-manual dials would block on it.
	b.Cfg.DialPacerInterval = 1 * time.Second
	b.Cfg.DialPacerBurst = 0

	var manualDials int32
	b.Cfg.DialFn = func(_ context.Context, a []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&manualDials, 1)
		session := fakePeerSession(a[0], "id-"+domain.PeerIdentity(a[0]))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Emit a manual peer request. Must dial immediately despite pacer.
	addr := mustAddr("10.0.0.99:64646")
	cm.EmitSlot(ManualPeerRequested{
		Address:       addr,
		DialAddresses: []domain.PeerAddress{addr},
	})

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&manualDials) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("manual peer dial did not fire within 200ms — pacer is not being bypassed (count = %d)", atomic.LoadInt32(&manualDials))
}

// TestCM_DialPacer_BurstAllowsParallelStart verifies that with burst=N,
// the first N dials are issued immediately (no pacing delay between them).
// This is the cold-start optimisation: when there are no pending dials,
// we should not artificially slow down filling empty slots.
func TestCM_DialPacer_BurstAllowsParallelStart(t *testing.T) {
	t.Parallel()

	const burst = 3
	addrs := make([]string, burst)
	for i := range addrs {
		addrs[i] = mustAddrN(i)
	}

	b := testCMConfig(addrs...)
	b.Cfg.MaxSlotsFn = func() int { return burst }
	b.Cfg.DialPacerInterval = 500 * time.Millisecond
	b.Cfg.DialPacerBurst = burst

	dialTimes := make(chan time.Time, burst)
	b.Cfg.DialFn = func(_ context.Context, a []domain.PeerAddress) (DialResult, error) {
		dialTimes <- time.Now()
		session := fakePeerSession(a[0], "id-"+domain.PeerIdentity(a[0]))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	start := time.Now()
	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "burst dials issued", func() bool {
		return len(dialTimes) >= burst
	})

	stamps := make([]time.Time, 0, burst)
	for i := 0; i < burst; i++ {
		stamps = append(stamps, <-dialTimes)
	}

	// All burst dials should be within a small window — the burst tokens
	// are consumed in tight succession.
	maxGap := stamps[burst-1].Sub(start)
	if maxGap > 200*time.Millisecond {
		t.Fatalf("burst dials too spread out: last fired %v after start, want < 200ms", maxGap)
	}
}

// TestCM_DialPacer_NilDisabled ensures the pacer is opt-in: a
// configuration with DialPacerInterval=0 behaves exactly like before
// (no rate limit). Critical so existing tests and production code without
// explicit pacer config are not accidentally throttled.
func TestCM_DialPacer_NilDisabled(t *testing.T) {
	t.Parallel()

	const peers = 4
	addrs := make([]string, peers)
	for i := range addrs {
		addrs[i] = mustAddrN(i)
	}

	b := testCMConfig(addrs...)
	b.Cfg.MaxSlotsFn = func() int { return peers }
	// DialPacerInterval zero → disabled.
	b.Cfg.DialPacerInterval = 0
	b.Cfg.DialPacerBurst = 0

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, a []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		session := fakePeerSession(a[0], "id-"+domain.PeerIdentity(a[0]))
		return DialResult{Session: session, ConnectedAddress: a[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	start := time.Now()
	cm.NotifyBootstrapReady()

	waitFor(t, 1*time.Second, "all dials fired", func() bool {
		return atomic.LoadInt32(&dialCount) >= int32(peers)
	})

	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("dials took %v with pacer disabled — expected near-instant", elapsed)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// mustAddrN returns "10.0.0.{i+1}:64646" for use as a synthetic test peer.
// Starts at .1 to avoid the .0 broadcast/network edge that some parsers
// treat as invalid, and caps at .254.
func mustAddrN(i int) string {
	if i < 0 || i > 253 {
		panic("mustAddrN: index out of range")
	}
	return fmt.Sprintf("10.0.0.%d:64646", i+1)
}
