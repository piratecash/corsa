// overload_monitor.go provides the Phase 0 backpressure primitive that
// the routing announce loop consults to throttle non-critical sends
// when the host is CPU/backlog-saturated. The implementation is
// intentionally minimal: goroutine count is a sufficient proxy for our
// workload because each inbound frame spawns at least one goroutine
// (peer reader, frame dispatcher, per-peer announce sender), so
// sustained backlog manifests as goroutine pile-up. Operators tune the
// threshold via CORSA_OVERLOAD_GOROUTINE_THRESHOLD; the default is
// disabled (zero threshold) so existing deployments observe pre-fix
// behaviour exactly until they opt in.
//
// Counter ownership: this file's overloadMonitor.EngagedTotal counts
// per-call IsOverloaded() returns of true — useful as an internal
// debug counter when the gate is wired but not as the operator-facing
// signal. The OPERATOR-VISIBLE counter that surfaces through
// fetchRouteSummary.overload.engaged_cycles lives at the loop layer
// (routing.AnnounceLoop.OverloadCycleCount) — it counts cycles where
// AT LEAST ONE DELTA-DUE PEER WAS SKIPPED DUE TO OVERLOAD (the strict
// "gate actually shed work" semantic). Cycles where the gate engaged
// but every peer was forced-due (initial sync, periodic forced-full
// deadline) do NOT advance the loop counter. The two counters
// therefore diverge in three ways:
//
//  1. The monitor counter increments on EVERY IsOverloaded() call
//     that returned true (currently one per cycle, but a future
//     caller could query the gate multiple times per cycle).
//  2. The loop counter is per-cycle — each cycle increments by at
//     most 1 regardless of how many peers were suppressed.
//  3. The loop counter requires that the gate ACTUALLY suppressed a
//     delta send; the monitor counter only requires that the proxy
//     reported overloaded.
//
// routing_provider.go's `Service.OverloadStats()` reads from the
// loop counter, not from this file's monitor counter. Two counters
// with similar names but different semantics is intentional but
// risks confusion for future readers — hence this header. Renaming/
// dropping EngagedTotal would require ripping out the monitor's
// tests too; kept as-is.
//
// Future iteration may replace the goroutine proxy with explicit inbox
// backlog measurement (sum of len(peerSession.inboxCh)) once the
// per-peer state lifecycle is decoupled enough to expose those
// channels safely; for now goroutine count is universally observable
// without extra plumbing and correlates well in practice under
// announce-storm conditions.
package node

import (
	"runtime"
	"sync/atomic"
)

// overloadMonitor implements routing.OverloadGate. It is safe for
// concurrent use: the threshold is set once at construction and the
// enabled flag is atomic. IsOverloaded() runs in O(1) per call —
// runtime.NumGoroutine() is a single atomic read inside the runtime,
// so this method can sit on the announce-loop hot path without
// observable overhead.
type overloadMonitor struct {
	// goroutineThreshold is the goroutine-count cutoff above which
	// IsOverloaded reports true. Set to <=0 to disable. Configured
	// once at construction; immutable thereafter.
	goroutineThreshold int

	// enabled toggles the gate at runtime. Operators that want a
	// kill-switch without restart can flip this via a future RPC;
	// for now it tracks whether goroutineThreshold > 0 was supplied.
	enabled atomic.Bool

	// engagedTotal counts cumulative IsOverloaded() invocations that
	// returned true — internal debug counter for the monitor itself.
	// NOT surfaced through RPC; fetchRouteSummary uses the loop-level
	// counter routing.AnnounceLoop.OverloadCycleCount instead, which
	// counts cycles (the operator-meaningful unit) rather than gate
	// calls. See the file header for the rationale on keeping both.
	engagedTotal atomic.Uint64
}

// newOverloadMonitor builds a monitor with the given goroutine
// threshold. Pass <=0 to disable (IsOverloaded always returns false).
// The monitor is cheap; constructing one when disabled is fine.
func newOverloadMonitor(goroutineThreshold int) *overloadMonitor {
	m := &overloadMonitor{
		goroutineThreshold: goroutineThreshold,
	}
	if goroutineThreshold > 0 {
		m.enabled.Store(true)
	}
	return m
}

// IsOverloaded reports whether the host is currently CPU/backlog
// saturated by goroutine-count proxy. Engagement is counted in
// engagedTotal for observability. Disabled monitors (threshold <= 0)
// always return false; this is the rollout-default so existing
// deployments observe pre-Phase-0 behaviour exactly until operators
// set CORSA_OVERLOAD_GOROUTINE_THRESHOLD.
func (m *overloadMonitor) IsOverloaded() bool {
	if !m.enabled.Load() {
		return false
	}
	if runtime.NumGoroutine() <= m.goroutineThreshold {
		return false
	}
	m.engagedTotal.Add(1)
	return true
}

// EngagedTotal returns the cumulative number of IsOverloaded() calls
// that returned true since process start. Monotonic; safe for
// concurrent reads. Internal debug counter — NOT the RPC-visible
// signal. Operators reading `fetchRouteSummary.overload.engaged_cycles`
// see the loop-level counter sourced from
// `routing.AnnounceLoop.OverloadCycleCount()`, which counts cycles
// where AT LEAST ONE DELTA-DUE PEER WAS SUPPRESSED (the strict
// "gate actually shed work" semantic).
//
// The two counters DIVERGE in the current call pattern:
//   - EngagedTotal increments on every IsOverloaded()==true return,
//     including initial-sync cycles, periodic forced-full-deadline
//     cycles, and forced-only wakes. In all those cases the loop
//     counter does NOT advance because no delta was suppressed.
//   - OverloadCycleCount only advances when the per-cycle
//     suppressedAny flag was set (overload-suppression early-return
//     fired for at least one delta-due peer).
//
// Concretely, on a stable mesh under sustained overload: EngagedTotal
// rises ~1 per announce-loop tick (the gate is queried once per
// cycle); OverloadCycleCount rises only on cycles where actual
// suppression happened (peer had baseline AND not forced-due AND
// gate engaged). EngagedTotal can therefore exceed OverloadCycleCount
// by a non-trivial factor in production. See the file header for
// the rationale on keeping both.
func (m *overloadMonitor) EngagedTotal() uint64 {
	return m.engagedTotal.Load()
}
