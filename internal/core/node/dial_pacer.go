package node

import (
	"context"
	"sync"
	"time"
)

// Production tuning for the global dial pacer.
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
// Production wiring (service.go) passes them through
// ConnectionManagerConfig.DialPacerInterval / DialPacerBurst; tests set those
// same fields directly, so the config fields are the single knob.
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
// Implementation: a token bucket with whole tokens. Capacity is the burst
// (at least one) and one token is earned per interval. Acquire() blocks until
// a token is available OR ctx is cancelled. Every paced dial worker
// (dialWorker / retryAfterBackoff) acquires a token before invoking DialFn.
//
// The bucket never holds more than capacity tokens, and a full bucket earns
// nothing: when a refill reaches capacity, the part of an interval already
// elapsed is discarded, and accrual restarts when the bucket stops being full
// (the first take from it). So in any window of length T at most
// capacity + floor(T/interval) dials start, and the first paced dial after a
// full bucket comes one interval after the bucket stopped being full —
// however long it sat full before.
//
// With capacity 1 (burst 0 or 1) a waiter woken late by δ finds the bucket
// full, so accrual restarts at its wake-up rather than at the interval
// boundary, and such a pacer runs slightly slower than one dial per interval
// under real timers. That only lowers the rate, and it is what "a full bucket
// earns nothing" costs. With a larger capacity (production: 3) the bucket is
// not full while dials are waiting, so the boundary is kept.
//
// Manual operator-driven dials (handleManualPeer) bypass the pacer — they
// use a dedicated dialWorkerImmediate path that does not Acquire. This is
// the documented contract: `addpeer 1.2.3.4` must be immediate even when
// the storm-protection pacer would otherwise hold it.
//
// Why not the package's shared tokenBucketSet (token_bucket.go): that is a
// keyed set of buckets that answer yes/no and start full. The pacer is one
// bucket that must say how long to wait for the next token, may start below
// capacity (burst 0 starts empty with capacity 1), and counts whole tokens so
// a grant lands on an exact interval boundary instead of a float rounding of
// one.
// ---------------------------------------------------------------------------

// dialPacer is a thread-safe token bucket. Zero value is unusable; call
// newDialPacer.
//
// mu is a leaf: nothing is called under it except the clock, which is a pure
// time source, and Acquire waits for the next token with mu released.
type dialPacer struct {
	mu sync.Mutex

	// capacity is the maximum number of tokens held (cold-start burst).
	capacity int

	// interval is the period between token refills. One token is added
	// per interval, up to capacity.
	interval time.Duration

	// tokens is the current number of tokens. Mutated under mu.
	tokens int

	// lastRefill is the instant the token being earned started accruing:
	// the last interval boundary a token was credited at, or the last
	// instant the bucket was seen full. Mutated under mu.
	lastRefill time.Time

	// clock and after are set once in newDialPacer and never written again,
	// which is why they need no protection from mu.
	clock func() time.Time
	after func(time.Duration) <-chan time.Time
}

// dialPacerOpts carries the pacer's dependencies on the passage of time, so a
// test steps the pacer through virtual time instead of waiting for it.
//
// Two function fields rather than a Clock interface, for the reason
// updatecheck.CheckerOpts gives: Acquire selects on the wait channel together
// with ctx.Done(), so the timer must hand back a channel the caller selects
// on, which a sleeping provider or a generated mock cannot.
type dialPacerOpts struct {
	// clock is what refill reads. Nil means time.Now.
	clock func() time.Time
	// after is the timer Acquire waits on before retrying. Nil means
	// time.After: an abandoned wait leaves a timer of at most one interval,
	// which the runtime reclaims once it is unreferenced (Go 1.23+).
	after func(time.Duration) <-chan time.Time
}

// newDialPacer constructs a pacer with the given interval and burst.
// Returns nil when interval <= 0 — callers treat nil as "disabled" so
// production configurations that opt out of pacing produce no token
// math and no measurable overhead.
//
// burst < 0 is normalised to 0 (one-by-one pacing). burst == 0 starts
// the bucket empty, so the first token is earned one interval after
// construction — the most defensive setting. The bucket capacity is
// internally clamped to >= 1 so a refilled token is not immediately
// discarded (an idle pacer therefore holds one token); the initial token
// count still honours the requested burst.
func newDialPacer(interval time.Duration, burst int, opts dialPacerOpts) *dialPacer {
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
	if opts.clock == nil {
		opts.clock = time.Now
	}
	if opts.after == nil {
		opts.after = time.After
	}
	return &dialPacer{
		capacity:   capacity,
		interval:   interval,
		tokens:     burst, // start with the requested burst (may be 0)
		lastRefill: opts.clock(),
		clock:      opts.clock,
		after:      opts.after,
	}
}

// Acquire blocks until a token is granted or ctx is cancelled. Returns
// true on a successful acquire, false on ctx cancellation. The bucket
// is decremented only on the success branch.
//
// The wait loop sleeps until the next token is due rather than busy
// polling, so an idle pacer holds a single sleeping goroutine per
// waiter — fine for the small number of in-flight dial workers (bounded
// by maxSlots). Several waiters woken for the same token race for it in
// tryAcquire; the losers wait for the next one.
func (p *dialPacer) Acquire(ctx context.Context) bool {
	for {
		wait, ok := p.tryAcquire()
		if ok {
			return true
		}
		select {
		case <-p.after(wait):
		case <-ctx.Done():
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

	now := p.clock()
	p.refillLocked(now)

	if p.tokens >= 1 {
		p.tokens--
		return 0, true
	}

	// After refillLocked the token being earned has accrued for less than
	// one interval, so the wait is in (0, interval]. Only a clock that reads
	// earlier than lastRefill — a test clock stepped back; time.Now carries
	// a monotonic reading — could ask for more, and the cap bounds every
	// wait by one interval regardless.
	wait := p.interval - now.Sub(p.lastRefill)
	if wait > p.interval {
		wait = p.interval
	}
	return wait, false
}

// refillLocked credits one token per whole interval since lastRefill and
// keeps the remainder of the current interval — unless that fills the
// bucket. A full bucket earns nothing, so its accrual restarts at now: a
// fraction carried while full would be a token beyond capacity, granted one
// part of an interval after the bucket stops being full instead of a whole
// one. Caller holds p.mu.
func (p *dialPacer) refillLocked(now time.Time) {
	if elapsed := now.Sub(p.lastRefill); elapsed >= p.interval {
		whole := int64(elapsed / p.interval)
		room := int64(p.capacity - p.tokens)
		gained := min(whole, room)
		p.tokens += int(gained)
		p.lastRefill = p.lastRefill.Add(time.Duration(gained) * p.interval)
	}
	if p.tokens >= p.capacity {
		p.tokens = p.capacity
		p.lastRefill = now
	}
}
