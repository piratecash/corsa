package node

import (
	"sync"
	"time"
	"unsafe"
)

// Package-private, inactive first source. No Service/Run/config/RPC installer.
// New ordinary limiters leave fullObservation nil. Identity is not a benign
// traffic label. The duration provider must be qualified separately for suspend.
type k5CeilingKind uint8

const (
	k5HandshakeNode k5CeilingKind = iota + 1
	k5HandshakeLoopbackShare
	k5HandshakeExternalSource
	k5ConnectionBudgetTotal
	k5ConnectionBudgetDirection
	k5ConnectionBudgetProtectedReserve
)

type k5SourceKey struct {
	anchor                            string
	epoch, clockDomain, configuration [16]byte
	kind                              k5CeilingKind
}
type handshakeFullTransition struct {
	sequence     uint64
	full         bool
	count        int
	lower, upper time.Duration
	done, valid  bool
}
type handshakeFullCut struct {
	available bool
	rows      int
	sequence  uint64
	at        time.Duration
	full      bool
}
type handshakeFullObservation struct {
	key                                k5SourceKey
	bounds                             handshakeBounds
	clock                              func() (time.Duration, error)
	rows                               []handshakeFullTransition // fixed length; completed prefix never rewritten
	used, count                        int
	full                               bool
	initialized                        bool
	invalid                            string
	start, maximum                     time.Duration
	nextRead, reads, sequence, pending uint64
	sealed                             handshakeFullCut
	segment                            *handshakeSegmentState // explicit private V2, frozen before publication
}
type handshakeFullRead struct {
	owner            *handshakeFullObservation
	id               uint64
	floor            time.Duration
	registered, done bool
}
type handshakeFullCompletion struct {
	owner                   *handshakeFullObservation
	index                   int
	sequence                uint64
	generation              uint64 // zero on unchanged V1 path
	lower                   time.Duration
	valid, registered, done bool
}
type handshakeFullSnapshot struct {
	status, reason string
	key            k5SourceKey
	bounds         handshakeBounds
	coverageStart  time.Duration
	cut            handshakeFullCut
	rows           []handshakeFullTransition
	// Non-COMPLETE returns only a previously sealed past prefix, not current state.
	pastOnly bool
}

const k5MaxUint = ^uint64(0)

func newObservedInboundHandshakes(bounds handshakeBounds, key k5SourceKey, capacity int, clock func() (time.Duration, error)) *inboundHandshakes {
	h := newInboundHandshakes(bounds)
	o := &handshakeFullObservation{key: key, bounds: bounds, clock: clock}
	h.fullObservation = o
	rowBytes := uint64(unsafe.Sizeof(handshakeFullTransition{}))
	switch {
	case key.anchor == "" || key.epoch == ([16]byte{}) || key.clockDomain == ([16]byte{}) || key.configuration == ([16]byte{}) || key.kind != k5HandshakeNode:
		o.fail("invalid-source-identity")
	case bounds.node <= 0 || bounds.loopback < 0 || bounds.perSource < 0:
		o.fail("invalid-bounds")
	case capacity <= 0 || uint64(capacity) > uint64(^uint(0)>>1)/rowBytes:
		o.fail("invalid-capacity")
	case clock == nil:
		o.fail("unsupported-clock")
	}
	if o.invalid != "" {
		return h
	}
	// Allocation is outside the owner leaf and before publication. Capacity is
	// proposed, not a deployment value or a proof an arbitrary allocation fits.
	o.rows = make([]handshakeFullTransition, capacity)
	r := h.beginFullRead()
	at, err := clock()
	h.mu.Lock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.retireFullReadLocked(&r)
	if valid && o.invalid == "" {
		o.start, o.initialized = at, true
		o.sealed = handshakeFullCut{available: true, at: at}
	}
	h.mu.Unlock()
	return h
}
func (o *handshakeFullObservation) fail(reason string) {
	if o.invalid == "" {
		o.invalid = reason
	}
}
func (h *inboundHandshakes) beginFullRead() handshakeFullRead {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.beginFullReadLocked()
}
func (h *inboundHandshakes) beginFullReadLocked() handshakeFullRead {
	o := h.fullObservation
	r := handshakeFullRead{owner: o, floor: o.maximum}
	if o.reads == k5MaxUint || o.nextRead == k5MaxUint {
		o.fail("read-overflow")
		return r
	}
	o.nextRead++
	o.reads++
	r.id, r.registered = o.nextRead, true
	return r
}
func (h *inboundHandshakes) validateFullReadLocked(r *handshakeFullRead, at time.Duration, err error) bool {
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
func (h *inboundHandshakes) retireFullReadLocked(r *handshakeFullRead) {
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
func (h *inboundHandshakes) fullClock() (time.Duration, error) {
	o := h.fullObservation
	if o.clock == nil {
		return 0, k5ClockUnavailable{}
	}
	return o.clock()
}

// Local sentinel; no policy, user callback or source call under the leaf.
type k5ClockUnavailable struct{}

func (k5ClockUnavailable) Error() string { return "K5 observation clock unavailable" }

func (h *inboundHandshakes) checkFullOwnerLocked() {
	o := h.fullObservation
	if h.bounds != o.bounds || h.inFlight != o.count || (h.inFlight >= h.bounds.node) != o.full {
		o.fail("untracked-owner-mutation")
	}
}
func (h *inboundHandshakes) fullMutationLocked(at time.Duration, valid bool) handshakeFullCompletion {
	o := h.fullObservation
	full := h.inFlight >= h.bounds.node
	before := o.full
	o.count, o.full = h.inFlight, full
	p := handshakeFullCompletion{owner: o, index: -1}
	if before == full || o.invalid != "" {
		return p
	}
	if !o.initialized || !valid || at < o.start {
		o.fail("invalid-transition-lower")
		return p
	}
	if o.sequence == k5MaxUint || o.pending == k5MaxUint {
		o.fail("transition-overflow")
		return p
	}
	if o.used == len(o.rows) {
		o.fail("transition-capacity-gap")
		return p
	}
	index, generation := o.used, uint64(0)
	if o.segment != nil {
		index = o.segment.slot(o.used, len(o.rows))
		if o.segment.generations[index] == k5MaxUint {
			o.fail("segment-slot-generation-overflow")
			return p
		}
		o.segment.generations[index]++
		generation = o.segment.generations[index]
	}
	o.sequence++
	o.pending++
	o.used++
	lower := at - o.start
	o.rows[index] = handshakeFullTransition{sequence: o.sequence, full: full, count: h.inFlight, lower: lower}
	if o.segment != nil {
		if full {
			o.segment.liveCarry = handshakeSegmentCarry{full: true, entry: o.rows[index]}
		} else {
			o.segment.liveCarry = handshakeSegmentCarry{}
		}
	}
	return handshakeFullCompletion{owner: o, index: index, sequence: o.sequence, generation: generation, lower: lower, valid: true, registered: true}
}
func (h *inboundHandshakes) finishFullTransition(p *handshakeFullCompletion) {
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
func (h *inboundHandshakes) completeFullRowLocked(p *handshakeFullCompletion, at time.Duration, valid bool) {
	o := h.fullObservation
	if p.owner != o || p.done || !p.registered || p.index < 0 || p.index >= len(o.rows) || (o.segment == nil && p.index >= o.used) {
		o.fail("invalid-completion-owner")
		return
	}
	p.done = true
	row := &o.rows[p.index]
	if row.sequence != p.sequence || row.done || (o.segment != nil && o.segment.generations[p.index] != p.generation) {
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
	if o.segment != nil && o.segment.liveCarry.full && o.segment.liveCarry.entry.sequence == row.sequence {
		o.segment.liveCarry.entry = *row
	}
}
func (h *inboundHandshakes) enterObserved(source handshakeSource) (func(), error) {
	r := h.beginFullRead()
	at, err := h.fullClock()
	h.mu.Lock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.retireFullReadLocked(&r)
	h.checkFullOwnerLocked()
	switch {
	case h.inFlight >= h.bounds.node:
		h.mu.Unlock()
		return nil, &listenerRefusalError{reason: refusalHandshakesNode}
	case source.loopback && h.loopbackInFlight >= h.bounds.loopback:
		h.mu.Unlock()
		return nil, &listenerRefusalError{reason: refusalHandshakesLoopback}
	}
	h.inFlight++
	if source.loopback {
		h.loopbackInFlight++
		h.loopbackGauge.Store(int64(h.loopbackInFlight))
	}
	p := h.fullMutationLocked(at, valid)
	h.mu.Unlock()
	h.finishFullTransition(&p)
	var once sync.Once
	return func() { once.Do(func() { h.leave(source.loopback) }) }, nil
}
func (h *inboundHandshakes) leaveObserved(loopback bool) {
	r := h.beginFullRead()
	at, err := h.fullClock()
	h.mu.Lock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.retireFullReadLocked(&r)
	h.checkFullOwnerLocked()
	h.inFlight--
	if loopback {
		h.loopbackInFlight--
		h.loopbackGauge.Store(int64(h.loopbackInFlight))
	}
	p := h.fullMutationLocked(at, valid)
	h.mu.Unlock()
	h.finishFullTransition(&p)
}

func (h *inboundHandshakes) fullSnapshot() handshakeFullSnapshot {
	if h.fullObservation == nil {
		return handshakeFullSnapshot{status: "INACTIVE"}
	}
	h.mu.Lock()
	o := h.fullObservation
	h.checkFullOwnerLocked()
	if o.segment != nil {
		// A V2 window is never a V1 whole-epoch prefix, even before its first
		// retirement or after invalid construction. No available legacy cut.
		result := handshakeFullSnapshot{status: "UNKNOWN", reason: "segmented-source-requires-v2", key: o.key, bounds: o.bounds, coverageStart: o.start, pastOnly: true}
		h.mu.Unlock()
		return result
	}
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
		o.sealed = handshakeFullCut{available: true, rows: o.used, sequence: seq, at: at, full: full}
		status = "COMPLETE"
	}
	proof := o.sealed
	h.mu.Unlock()
	return h.copyFullPrefix(status, reason, proof)
}
func (h *inboundHandshakes) copyFullPrefix(status, reason string, cut handshakeFullCut) handshakeFullSnapshot {
	o := h.fullObservation
	// These fixed immutable fields and completed prefix slots cannot be changed by
	// later appends. No O(M) copy or allocation runs under h.mu.
	rows := make([]handshakeFullTransition, cut.rows)
	copy(rows, o.rows[:cut.rows])
	return handshakeFullSnapshot{status: status, reason: reason, key: o.key, bounds: o.bounds, coverageStart: o.start, cut: cut, rows: rows, pastOnly: status != "COMPLETE"}
}

// Only local duration coordinates. No conversion to UTC and no load witness.
type handshakeFullRange struct{ definiteStart, definiteEnd, possibleStart, possibleEnd time.Duration }

func handshakeFullRanges(s handshakeFullSnapshot) ([]handshakeFullRange, bool) {
	if !s.cut.available || s.cut.at < s.coverageStart || s.cut.rows != len(s.rows) || s.cut.sequence != uint64(len(s.rows)) {
		return nil, false
	}
	end := s.cut.at - s.coverageStart
	var ranges []handshakeFullRange
	var entry *handshakeFullTransition
	for i := range s.rows {
		row := &s.rows[i]
		if !row.done || !row.valid || row.sequence != uint64(i)+1 || row.lower < 0 || row.upper < row.lower || row.upper > end {
			return nil, false
		}
		if row.full {
			if entry != nil || row.count != s.bounds.node {
				return nil, false
			}
			entry = row
		} else {
			if entry == nil || row.count != s.bounds.node-1 || row.upper < entry.lower {
				return nil, false
			}
			definiteEnd := row.lower
			if definiteEnd < entry.upper {
				definiteEnd = entry.upper
			}
			ranges = append(ranges, handshakeFullRange{entry.upper, definiteEnd, entry.lower, row.upper})
			entry = nil
		}
	}
	if (entry != nil) != s.cut.full {
		return nil, false
	}
	if entry != nil {
		ranges = append(ranges, handshakeFullRange{entry.upper, end, entry.lower, end})
	}
	return ranges, true
}
