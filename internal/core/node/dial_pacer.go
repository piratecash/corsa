package node

import (
	"context"
	"sync"
	"time"
)

// Production tuning for the global dial pacer. See dial_pacer.go.
//
//   - dialPacerProductionInterval: minimum spacing between paced dial
//     spawns. 300ms gives ~3.3 dials/sec sustained ceiling, which at 7
//     candidate peers maps to a per-peer attempt cadence of ~2s — the
//     same as the pre-pacer behaviour, but smeared in time instead of a
//     7-wide thundering herd every fill().
//
//   - dialPacerProductionBurst: cold-start parallelism. Allows up to 3
//     simultaneous initial dials so an empty CM does not pay the
//     interval cost for the first few slots. After the burst is
//     consumed every further dial waits for a refill.
//
// These constants must stay in this file so the test override seam in
// connection_manager_pacer_test.go (overrides on b.Cfg.DialPacer*) is
// the single knob — production wiring (service.go) and tests share the
// same field names.
const (
	dialPacerProductionInterval = 300 * time.Millisecond
	dialPacerProductionBurst    = 3
)

// ---------------------------------------------------------------------------
// dialPacer — token bucket that paces outbound dial spawns.
//
// Without pacing, ConnectionManager.fill() can launch N dial workers in a
// single tight loop (one per free slot). Under reconnect-storm conditions —
// when the candidate set has shrunk to a handful of bootstrap nodes and
// every attempt fails on setup — this produces a periodic thundering herd
// that pegs the CPU even though no session ever reaches Active. The pacer
// upper-bounds outbound dial concurrency so a storm degrades into a slow
// trickle instead of a tight retry loop.
//
// Implementation: classic leaky-bucket / token-bucket variant. Capacity is
// the maximum burst (cold-start) and refill granularity is one token per
// pacer interval. Acquire() blocks until a token is available OR ctx is
// cancelled. The single goroutine that owns dial spawn (dialWorker /
// retryAfterBackoff) consults the pacer before invoking DialFn.
//
// Manual operator-driven dials (handleManualPeer) bypass the pacer — they
// use a dedicated dialWorkerImmediate path that does not Acquire. This is
// the documented contract: `addpeer 1.2.3.4` must be immediate even when
// the storm-protection pacer would otherwise hold it.
// ---------------------------------------------------------------------------

// dialPacer is a thread-safe token bucket. Zero value is unusable; call
// newDialPacer.
type dialPacer struct {
	mu sync.Mutex

	// capacity is the maximum number of tokens held (cold-start burst).
	capacity int

	// interval is the period between token refills. One token is added
	// per interval, up to capacity.
	interval time.Duration

	// tokens is the current number of tokens. Mutated under mu.
	tokens int

	// lastRefill is the wall-clock instant of the last token grant.
	// Used by acquireLocked to compute how many tokens have accrued
	// since the previous call. Mutated under mu.
	lastRefill time.Time

	// nowFn returns the current time. Injected so tests can drive the
	// clock without sleeping.
	nowFn func() time.Time
}

// newDialPacer constructs a pacer with the given interval and burst.
// Returns nil when interval <= 0 — callers treat nil as "disabled" so
// production configurations that opt out of pacing produce no pacer
// goroutine, no token math, no measurable overhead.
//
// burst < 0 is normalised to 0 (one-by-one pacing). burst == 0 means
// every Acquire pays the full interval — the most defensive setting.
// The bucket capacity is internally clamped to >= 1 so a refilled
// token is not immediately discarded; the initial token count still
// honours the requested burst.
func newDialPacer(interval time.Duration, burst int) *dialPacer {
	if interval <= 0 {
		return nil
	}
	if burst < 0 {
		burst = 0
	}
	capacity := burst
	if capacity < 1 {
		capacity = 1
	}
	return &dialPacer{
		capacity:   capacity,
		interval:   interval,
		tokens:     burst, // start with the requested burst (may be 0)
		lastRefill: time.Now(),
		nowFn:      time.Now,
	}
}

// Acquire blocks until a token is granted or ctx is cancelled. Returns
// true on a successful acquire, false on ctx cancellation. The bucket
// is decremented only on the success branch.
//
// The wait loop sleeps for the next refill boundary rather than busy
// polling, so an idle pacer holds a single sleeping goroutine per
// waiter — fine for the small number of in-flight dial workers (bounded
// by maxSlots).
func (p *dialPacer) Acquire(ctx context.Context) bool {
	for {
		wait, ok := p.tryAcquire()
		if ok {
			return true
		}
		// Cap the sleep so a long interval still respects ctx.Done()
		// promptly — without the cap a cancelled ctx would have to wait
		// up to `wait` before noticing.
		if wait > p.interval {
			wait = p.interval
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			// loop and retry
		case <-ctx.Done():
			timer.Stop()
			return false
		}
	}
}

// tryAcquire is the non-blocking primitive. Returns (0, true) on success;
// otherwise returns the duration to wait before the next token is
// expected and (wait, false). Refill is computed on every call so the
// pacer does not need a background goroutine.
func (p *dialPacer) tryAcquire() (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.nowFn()
	elapsed := now.Sub(p.lastRefill)
	if elapsed >= p.interval {
		gained := int(elapsed / p.interval)
		p.tokens += gained
		if p.tokens > p.capacity {
			p.tokens = p.capacity
		}
		// Re-anchor lastRefill to the latest refill boundary (not now)
		// so fractional time is preserved for the next call.
		p.lastRefill = p.lastRefill.Add(time.Duration(gained) * p.interval)
	}

	if p.tokens >= 1 {
		p.tokens--
		return 0, true
	}

	// Time until the next token becomes available.
	wait := p.interval - now.Sub(p.lastRefill)
	if wait <= 0 {
		wait = p.interval
	}
	return wait, false
}
