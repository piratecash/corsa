package datagram

import (
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// handler_stats.go counts what the type handlers did, per registered dtype and
// mode (docs/rpc/datagram.md, `handlers`).
//
// The inbound counters in metrics.go answer "what happened to frames of mode
// M"; they cannot answer "which type is eating the receive path", because the
// dtype is not one of their dimensions. This file adds exactly that dimension,
// and nothing else: the same handler calls, cut by type.
//
// Three choices carry the design:
//
//   - the counters live IN the registry entry and are created by Register, so
//     the receive path reaches them through the entry it already holds — no
//     lookup by name, and no key that could come from a frame. An unknown
//     dtype has no entry and therefore no row; it stays in UnknownDType;
//   - one record per call, taken in runHandler — the one wrapper every plane
//     calls a handler through — after every gate and after the handler
//     returned. A frame dropped earlier never reaches it, so nothing here
//     double-counts DropsByReason;
//   - the time is elapsed (wall) time, from entering Handle to leaving it,
//     measured on the pipeline's MONOTONIC duration source rather than on its
//     clock, so an NTP step of the wall clock does not distort it. It
//     includes whatever the handler waited for — locks, disk, a channel — and
//     is not CPU time. On a panic it stops before the crash report is written.

// HandlerCounts is one (dtype, mode) row.
//
// Calls is not a separate counter: it is the sum of the three outcomes as read
// for this row, so calls == accepted + rejected + failed holds inside every
// snapshot. Panics is a SUBSET of Failed — a panic is the `failed` row
// (docs/protocol/datagram.md §4.1) — and is never added to the sum.
type HandlerCounts struct {
	// Calls is every handler call of this type in this mode.
	Calls uint64 `json:"calls"`
	// Accepted counts HandlerAccepted.
	Accepted uint64 `json:"accepted"`
	// Rejected counts HandlerRejected.
	Rejected uint64 `json:"rejected"`
	// Failed counts HandlerFailed, the unset outcome and every panic.
	Failed uint64 `json:"failed"`
	// Panics counts the calls the guard recovered from; already in Failed.
	Panics uint64 `json:"panics"`
	// WallNs is the summed elapsed time of the calls, in nanoseconds of the
	// pipeline's monotonic duration source. Not CPU time.
	WallNs uint64 `json:"wall_ns"`
}

// HandlerStatsSnapshot is the published `handlers` block.
//
// StartedAt is the layer's accumulation start — the instant its Metrics began
// counting. A pair of readings is comparable only under the same StartedAt;
// a different one is a restart and the pair is discarded, not subtracted.
type HandlerStatsSnapshot struct {
	// ByDType maps a registered dtype to its rows, keyed by mode name.
	ByDType map[string]map[string]HandlerCounts `json:"by_dtype"`
	// StartedAt is when the counters began accumulating; null when unknown.
	StartedAt *time.Time `json:"started_at"`
	// ReadAt is when the counters were read.
	ReadAt *time.Time `json:"read_at"`
}

// handlerModeCounters is one mode's atomics. Writers bump Failed BEFORE Panics
// and readers load Panics BEFORE Failed, so a snapshot taken mid-call can
// never show more panics than failures.
type handlerModeCounters struct {
	accepted atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
	panics   atomic.Uint64
	wallNs   atomic.Uint64
}

// handlerStats is one registered type's counters, one slot per mode.
type handlerStats struct {
	modes [modeSlots]handlerModeCounters
}

// handlerCall is what one finished call reports.
type handlerCall struct {
	mode     domain.DatagramMode
	outcome  HandlerOutcome
	wall     time.Duration
	panicked bool
}

// record adds one finished call. A nil receiver records nothing: an entry
// that did not come out of Register has no counters to charge.
func (s *handlerStats) record(call handlerCall) {
	if s == nil {
		return
	}
	counters := &s.modes[modeSlot(call.mode)]
	switch {
	case call.panicked:
		counters.failed.Add(1)
		counters.panics.Add(1)
	case call.outcome == HandlerAccepted:
		counters.accepted.Add(1)
	case call.outcome == HandlerRejected:
		counters.rejected.Add(1)
	default:
		// HandlerFailed and the unset outcome alike: "accepted by omission"
		// is never inferred (HandlerOutcomeUnset).
		counters.failed.Add(1)
	}
	if call.wall > 0 {
		// The monotonic source never runs backwards, but an injected one can,
		// and a negative duration converted to uint64 would add ~1.8e19 ns to
		// the sum. Charging nothing loses one sample instead of the counter.
		counters.wallNs.Add(uint64(call.wall))
	}
}

// read loads one mode's row.
func (c *handlerModeCounters) read() HandlerCounts {
	counts := HandlerCounts{Panics: c.panics.Load()}
	counts.Accepted = c.accepted.Load()
	counts.Rejected = c.rejected.Load()
	counts.Failed = c.failed.Load()
	counts.WallNs = c.wallNs.Load()
	counts.Calls = counts.Accepted + counts.Rejected + counts.Failed
	return counts
}

// rows renders the type's counters: one row per declared mode, always, plus
// any other slot that was ever charged — which admission makes unreachable,
// and which is shown rather than lost if that ever stops being true.
func (s *handlerStats) rows(declared map[domain.DatagramMode]struct{}) map[string]HandlerCounts {
	rows := make(map[string]HandlerCounts, len(declared))
	if s == nil {
		return rows
	}
	for mode := range declared {
		rows[mode.String()] = s.modes[modeSlot(mode)].read()
	}
	for slot := 0; slot < modeSlots; slot++ {
		if _, listed := rows[modeSlotNames[slot]]; listed {
			continue
		}
		if counts := s.modes[slot].read(); counts.Calls > 0 {
			rows[modeSlotNames[slot]] = counts
		}
	}
	return rows
}

// HandlerStats reads every registered type's rows, lock-free: the generation
// is an atomic snapshot and every counter an atomic.
func (r *TypeRegistry) HandlerStats() map[string]map[string]HandlerCounts {
	entries := r.current()
	stats := make(map[string]map[string]HandlerCounts, len(entries))
	for dtype, entry := range entries {
		stats[dtype.String()] = entry.stats.rows(entry.modes)
	}
	return stats
}

// newHandlerStatsSnapshot publishes the registry's rows with the period they
// describe. A nil registry answers an empty, non-nil map: "no types" is a real
// state, and `null` would read as "this build cannot say".
func newHandlerStatsSnapshot(types *TypeRegistry, startedAt *time.Time) HandlerStatsSnapshot {
	snapshot := HandlerStatsSnapshot{ByDType: map[string]map[string]HandlerCounts{}, StartedAt: startedAt}
	if types != nil {
		snapshot.ByDType = types.HandlerStats()
	}
	readAt := time.Now().UTC()
	snapshot.ReadAt = &readAt
	return snapshot
}
