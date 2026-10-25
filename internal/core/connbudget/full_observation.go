package connbudget

import (
	"time"
	"unsafe"
)

// Private inactive source for the actual total connection ceiling only.
// Not handshake-K5, UTC evidence, benign traffic attribution or deployment.
// Clock/key/capacity are frozen before the unpublished Budget is returned.
type totalSourceKey struct {
	anchor                            string
	epoch, clockDomain, configuration [16]byte
}
type totalCounts struct{ outbound, inbound, auxiliary int }
type totalFullTransition struct {
	sequence     uint64
	full         bool
	count        int
	lower, upper time.Duration
	done, valid  bool
}
type totalFullCut struct {
	available bool
	rows      int
	sequence  uint64
	at        time.Duration
	full      bool
}
type totalFullObservation struct {
	key                                totalSourceKey
	cfg                                Config
	clock                              func() (time.Duration, error)
	disabled                           bool
	rows                               []totalFullTransition
	used                               int
	counts                             totalCounts
	full, initialized                  bool
	invalid                            string
	start, maximum                     time.Duration
	nextRead, reads, sequence, pending uint64
	sealed                             totalFullCut
}
type totalFullRead struct {
	owner            *totalFullObservation
	id               uint64
	floor            time.Duration
	registered, done bool
}
type totalFullCompletion struct {
	owner                   *totalFullObservation
	index                   int
	sequence                uint64
	lower                   time.Duration
	valid, registered, done bool
}
type totalFullSnapshot struct {
	status, reason string
	key            totalSourceKey
	cfg            Config
	coverageStart  time.Duration
	cut            totalFullCut
	rows           []totalFullTransition
	pastOnly       bool
}

const totalMaxUint = ^uint64(0)

func newObservedBudget(cfg Config, key totalSourceKey, capacity int, clock func() (time.Duration, error)) (*Budget, error) {
	b, err := New(cfg)
	if err != nil {
		return nil, err
	}
	o := &totalFullObservation{key: key, cfg: cfg, clock: clock, disabled: cfg.Total == 0}
	b.fullObservation = o
	if o.disabled {
		return b, nil
	} // Direction limits still work, no full-duration evidence or clock.
	rowBytes := uint64(unsafe.Sizeof(totalFullTransition{}))
	switch {
	case key.anchor == "" || key.epoch == ([16]byte{}) || key.clockDomain == ([16]byte{}) || key.configuration == ([16]byte{}):
		o.fail("invalid-source-identity")
	case capacity <= 0 || uint64(capacity) > uint64(^uint(0)>>1)/rowBytes:
		o.fail("invalid-capacity")
	case clock == nil:
		o.fail("unsupported-clock")
	}
	if o.invalid != "" {
		return b, nil
	}
	// Caller-sized bounded allocation, before publication and outside the leaf.
	// This does not approve a deployment capacity or prove an allocation fits.
	o.rows = make([]totalFullTransition, capacity)
	r := b.beginFullRead()
	at, readErr := clock()
	b.mu.Lock()
	valid := b.validateFullReadLocked(&r, at, readErr)
	b.retireFullReadLocked(&r)
	if valid && o.invalid == "" {
		o.start, o.initialized = at, true
		o.sealed = totalFullCut{available: true, at: at}
	}
	b.mu.Unlock()
	return b, nil
}
func (o *totalFullObservation) fail(reason string) {
	if o.invalid == "" {
		o.invalid = reason
	}
}
func (h *Budget) beginFullRead() totalFullRead {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.beginFullReadLocked()
}
func (h *Budget) beginFullReadLocked() totalFullRead {
	o := h.fullObservation
	r := totalFullRead{owner: o, floor: o.maximum}
	if o.reads == totalMaxUint || o.nextRead == totalMaxUint {
		o.fail("read-overflow")
		return r
	}
	o.nextRead++
	o.reads++
	r.id, r.registered = o.nextRead, true
	return r
}
func (h *Budget) validateFullReadLocked(r *totalFullRead, at time.Duration, err error) bool {
	o := h.fullObservation
	if r.owner != o || r.done || !r.registered || r.id == 0 {
		o.fail("invalid-read-owner")
		return false
	}
	if err != nil {
		o.fail("clock-error")
		return false
	}
	if at < 0 {
		o.fail("negative-clock")
		return false
	}
	if at < r.floor {
		o.fail("clock-regression")
		return false
	}
	if at > o.maximum {
		o.maximum = at
	}
	return true
}
func (h *Budget) retireFullReadLocked(r *totalFullRead) {
	o := h.fullObservation
	if r.owner != o || r.done {
		o.fail("duplicate-read-retirement")
		return
	}
	r.done = true
	if !r.registered {
		return
	}
	if o.reads == 0 {
		o.fail("read-underflow")
		return
	}
	o.reads--
}
func (h *Budget) fullClock() (time.Duration, error) {
	o := h.fullObservation
	if o.clock == nil {
		return 0, totalClockUnavailable{}
	}
	return o.clock()
}

// Local sentinel; no policy, user callback or source call under the leaf.
type totalClockUnavailable struct{}

func (totalClockUnavailable) Error() string { return "connection-total observation clock unavailable" }

func (h *Budget) totalStateLocked() (totalCounts, int, bool) {
	counts := totalCounts{h.outbound, h.inbound, h.auxiliary}
	used := 0
	for _, n := range [...]int{counts.outbound, counts.inbound, counts.auxiliary} {
		if n < 0 || n > h.cfg.Total-used {
			return counts, 0, false
		}
		used += n
	}
	return counts, used, true
}
func (h *Budget) checkFullOwnerLocked() {
	o := h.fullObservation
	counts, used, valid := h.totalStateLocked()
	if h.cfg != o.cfg || !valid || counts != o.counts || (used == h.cfg.Total) != o.full {
		o.fail("untracked-owner-mutation")
	}
}
func (h *Budget) fullMutationLocked(at time.Duration, valid bool) totalFullCompletion {
	o := h.fullObservation
	counts, used, coherent := h.totalStateLocked()
	if !coherent {
		o.fail("invalid-owner-count")
	}
	full := coherent && used == h.cfg.Total
	before := o.full
	o.counts, o.full = counts, full
	p := totalFullCompletion{owner: o, index: -1}
	if before == full || o.invalid != "" {
		return p
	}
	if !o.initialized || !valid || at < o.start {
		o.fail("invalid-transition-lower")
		return p
	}
	if o.sequence == totalMaxUint || o.pending == totalMaxUint {
		o.fail("transition-overflow")
		return p
	}
	if o.used == len(o.rows) {
		o.fail("transition-capacity-gap")
		return p
	}
	o.sequence++
	o.pending++
	index := o.used
	o.used++
	lower := at - o.start
	o.rows[index] = totalFullTransition{sequence: o.sequence, full: full, count: used, lower: lower}
	return totalFullCompletion{owner: o, index: index, sequence: o.sequence, lower: lower, valid: true, registered: true}
}
func (h *Budget) finishFullTransition(p *totalFullCompletion) {
	if !p.registered {
		return
	}
	r := h.beginFullRead()
	at, err := h.fullClock()
	h.mu.Lock()
	defer h.mu.Unlock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.retireFullReadLocked(&r)
	h.completeFullRowLocked(p, at, valid)
}
func (h *Budget) completeFullRowLocked(p *totalFullCompletion, at time.Duration, valid bool) {
	o := h.fullObservation
	if p.owner != o || p.done || !p.registered || p.index < 0 || p.index >= o.used {
		o.fail("invalid-completion-owner")
		return
	}
	p.done = true
	row := &o.rows[p.index]
	if row.sequence != p.sequence || row.done {
		o.fail("completion-sequence-mismatch")
		return
	}
	if o.pending == 0 {
		o.fail("pending-underflow")
	} else {
		o.pending--
	}
	valid = valid && p.valid && at >= o.start && at-o.start >= p.lower
	if !valid {
		o.fail("invalid-transition-bracket")
	}
	row.done, row.valid = true, valid
	if valid {
		row.upper = at - o.start
	}
}

// Public Reserve already validated direction; admission is the original authority.
func (b *Budget) reserveObserved(direction Direction) (*Reservation, error) {
	r := b.beginFullRead()
	at, err := b.fullClock()
	b.mu.Lock()
	valid := b.validateFullReadLocked(&r, at, err)
	b.retireFullReadLocked(&r)
	b.checkFullOwnerLocked()
	if err := b.admitLocked(direction); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	switch direction {
	case DirectionOutbound:
		b.outbound++
	case DirectionInbound:
		b.inbound++
	case DirectionAuxiliary:
		b.auxiliary++
	}
	p := b.fullMutationLocked(at, valid)
	b.mu.Unlock()
	b.finishFullTransition(&p)
	return &Reservation{budget: b, direction: direction}, nil
}

// Original Reservation.Release CAS owns this call, still exactly once.
func (b *Budget) releaseObserved(direction Direction) {
	r := b.beginFullRead()
	at, err := b.fullClock()
	b.mu.Lock()
	valid := b.validateFullReadLocked(&r, at, err)
	b.retireFullReadLocked(&r)
	b.checkFullOwnerLocked()
	switch direction {
	case DirectionOutbound:
		if b.outbound > 0 {
			b.outbound--
		}
	case DirectionInbound:
		if b.inbound > 0 {
			b.inbound--
		}
	case DirectionAuxiliary:
		if b.auxiliary > 0 {
			b.auxiliary--
		}
	}
	p := b.fullMutationLocked(at, valid)
	b.mu.Unlock()
	b.finishFullTransition(&p)
}
func (h *Budget) fullSnapshot() totalFullSnapshot {
	if h == nil || h.fullObservation == nil {
		return totalFullSnapshot{status: "INACTIVE"}
	}
	if h.fullObservation.disabled {
		h.mu.Lock()
		defer h.mu.Unlock()
		o := h.fullObservation
		status, reason := "DISABLED", "total-disabled"
		if h.cfg != o.cfg {
			o.fail("untracked-owner-mutation")
			status, reason = "UNKNOWN", o.invalid
		}
		return totalFullSnapshot{status: status, reason: reason, key: o.key, cfg: o.cfg, pastOnly: true}
	}
	h.mu.Lock()
	o := h.fullObservation
	h.checkFullOwnerLocked()
	status, reason := "", o.invalid
	if o.invalid != "" || !o.initialized {
		status = "UNKNOWN"
	} else if o.reads != 0 || o.pending != 0 {
		status, reason = "IN_FLIGHT", "unvalidated-read-or-row"
	}
	if status != "" {
		proof := o.sealed
		h.mu.Unlock()
		return h.copyFullPrefix(status, reason, proof)
	}
	seq, full := o.sequence, o.full
	r := h.beginFullReadLocked()
	h.mu.Unlock()
	at, err := h.fullClock()
	h.mu.Lock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.checkFullOwnerLocked()
	stable := o.reads == 1 && o.pending == 0 && o.sequence == seq && o.full == full
	h.retireFullReadLocked(&r)
	if !valid || o.invalid != "" {
		status, reason = "UNKNOWN", o.invalid
	} else if !stable || o.reads != 0 {
		status, reason = "UNSTABLE_CUT", "owner-or-provider-overlap"
	} else {
		o.sealed = totalFullCut{available: true, rows: o.used, sequence: seq, at: at, full: full}
		status = "COMPLETE"
	}
	proof := o.sealed
	h.mu.Unlock()
	return h.copyFullPrefix(status, reason, proof)
}
func (h *Budget) copyFullPrefix(status, reason string, cut totalFullCut) totalFullSnapshot {
	o := h.fullObservation
	// These fixed immutable fields and completed prefix slots cannot be changed by
	// later appends. No O(M) copy or allocation runs under h.mu.
	rows := make([]totalFullTransition, cut.rows)
	copy(rows, o.rows[:cut.rows])
	return totalFullSnapshot{status: status, reason: reason, key: o.key, cfg: o.cfg, coverageStart: o.start, cut: cut, rows: rows, pastOnly: status != "COMPLETE"}
}

// Bounds concern only the returned covered local prefix, including an open
// interval clipped at that cut. A past-only cut never certifies current time.
// No UTC conversion, load witness, daily K5 reducer or release threshold.
func totalFullDurationBounds(s totalFullSnapshot) (lower, upper time.Duration, valid bool) {
	if s.cfg.Total <= 0 || !s.cut.available || s.cut.at < s.coverageStart || s.cut.rows != len(s.rows) || s.cut.sequence != uint64(len(s.rows)) {
		return 0, 0, false
	}
	end := s.cut.at - s.coverageStart
	var entry *totalFullTransition
	add := func(lo, hi time.Duration) bool {
		if lo < 0 || hi < lo || lo > end-lower {
			return false
		}
		lower += lo
		// Binary fullness cannot occupy more than the complete covered span.
		// Cap before addition so overlapping possible brackets cannot overflow.
		if hi >= end-upper {
			upper = end
		} else {
			upper += hi
		}
		return true
	}
	for i := range s.rows {
		row := &s.rows[i]
		if !row.done || !row.valid || row.sequence != uint64(i)+1 || row.lower < 0 || row.upper < row.lower || row.upper > end {
			return 0, 0, false
		}
		if row.full {
			if entry != nil || row.count != s.cfg.Total {
				return 0, 0, false
			}
			entry = row
		} else {
			if entry == nil || row.count != s.cfg.Total-1 || row.upper < entry.lower {
				return 0, 0, false
			}
			lo := row.lower - entry.upper
			if lo < 0 {
				lo = 0
			}
			if !add(lo, row.upper-entry.lower) {
				return 0, 0, false
			}
			entry = nil
		}
	}
	if (entry != nil) != s.cut.full {
		return 0, 0, false
	}
	if entry != nil {
		if !add(end-entry.upper, end-entry.lower) {
			return 0, 0, false
		}
	}
	return lower, upper, true
}
