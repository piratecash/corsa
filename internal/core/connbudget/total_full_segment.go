package connbudget

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"time"
	"unsafe"
)

// Private Budget-owner V2 protocol; ordinary/V1 observations remain append-only.
// Explicit typed file acquisition can bind a real issuer before publication;
// ordinary defaults stay off and V1 is unchanged. TEST issuers exercise authority
// only. Storage qualification is separate. Config and Reserve/Release own admission.
// Caller M/C allocation requests select no deployment capacity or cadence.
type totalSegmentCarry struct {
	full  bool
	entry totalFullTransition
}
type totalSegmentDescriptor struct {
	key                      totalSourceKey
	cfg                      Config
	coverageStart            time.Duration
	checkpointID, previousID uint64
	previousDigest           [32]byte
	// fromSeq is an exclusive cursor, including EMPTY. Never compute a
	// synthetic first=fromSeq+1 for an empty range at MaxUint64.
	fromSeq, throughSeq     uint64
	fromAt, throughAt       time.Duration
	rowCount                int
	fromCarry, throughCarry totalSegmentCarry
}
type totalSegmentWitness struct {
	owner      *totalFullObservation
	session    [16]byte
	descriptor totalSegmentDescriptor
	digest     [32]byte
}
type totalSegmentReceipt struct {
	witness totalSegmentWitness
	// Opaque issuer proof. A digest alone never constructs a verified commit.
	proof [32]byte
}
type totalSegmentCommit struct{ witness totalSegmentWitness }
type totalSegmentIssuer interface {
	verifyTotalSegment(totalSegmentReceipt) (totalSegmentCommit, bool)
}
type totalSegmentStream struct {
	owner                         *totalFullObservation
	checkpointID, streamAttemptID uint64
}
type totalSegmentLoan struct {
	stream        totalSegmentStream
	loanID        uint64
	offset, count int
}
type totalSegmentChunk struct {
	descriptor totalSegmentDescriptor
	offset     int
	// Staging only, never ring aliases. Valid until exact Release. The trusted
	// consumer must finish using this loan before releasing it; arbitrary
	// retained consumer copies are outside the source-owned memory bound.
	rows  []totalFullTransition
	final bool
}
type totalSegmentState struct {
	head                                        int
	generations                                 []uint64
	staging                                     []totalFullTransition
	issuer                                      totalSegmentIssuer
	session                                     [16]byte
	liveCarry, committedCarry                   totalSegmentCarry
	retiredThrough, committedID, nextCheckpoint uint64
	committedAt                                 time.Duration
	committedDigest                             [32]byte
	q                                           totalSegmentDescriptor
	qActive, digestKnown                        bool
	qIntegrityFault                             string // sticky for THIS Q, distinct from later outside-Q gaps
	qDigest                                     [32]byte
	last, pendingReceipt                        totalSegmentReceipt
	lastValid, verifiedPending                  bool
	nextStream, nextLoan                        uint64
	stream                                      totalSegmentStream
	streamOffset                                int
	streamFinished                              bool
	streamHash                                  hash.Hash
	loan                                        totalSegmentLoan
	// 0 idle, 1 reserved copy, 2 copying/hashing, 3 consumer staging loan,
	// 4 header initialization. Nonzero pins both Q and the one hash/buffer.
	loanPhase uint8
	loanValid bool
}

func newSegmentedBudget(cfg Config, key totalSourceKey, capacity, chunkCapacity int, clock func() (time.Duration, error), issuer totalSegmentIssuer, session [16]byte) (*Budget, error) {
	h, err := New(cfg)
	if err != nil {
		return nil, err
	}
	o := &totalFullObservation{key: key, cfg: cfg, clock: clock, disabled: cfg.Total == 0}
	h.fullObservation = o
	// Total0 is DISABLED before M/C, observation clock, issuer or buffer work.
	// Original direction limits still apply through the ordinary owner path.
	if o.disabled {
		return h, nil
	}
	s := &totalSegmentState{issuer: issuer, session: session}
	o.segment = s // explicit V2 even if any subsequent validation fails
	rowBytes, generationBytes := uint64(unsafe.Sizeof(totalFullTransition{})), uint64(unsafe.Sizeof(uint64(0)))
	maxBytes := uint64(^uint(0) >> 1)
	switch {
	case key.anchor == "" || key.epoch == ([16]byte{}) || key.clockDomain == ([16]byte{}) || key.configuration == ([16]byte{}):
		o.fail("invalid-source-identity")
	case capacity <= 0 || chunkCapacity <= 0 || chunkCapacity > capacity:
		o.fail("invalid-segment-capacity")
	case uint64(capacity) > maxBytes/(rowBytes+generationBytes):
		o.fail("invalid-segment-capacity")
	case uint64(chunkCapacity) > (maxBytes-uint64(capacity)*(rowBytes+generationBytes))/rowBytes:
		o.fail("invalid-segment-capacity")
	case clock == nil:
		o.fail("unsupported-clock")
	case issuer == nil || session == ([16]byte{}):
		o.fail("unavailable-segment-issuer")
	}
	if o.invalid != "" {
		return h, nil
	}
	// Fixed M rows + M generation counters + C staging; no whole-Q allocation.
	// All allocation/hash work occurs before publication, outside the leaf.
	o.rows = make([]totalFullTransition, capacity)
	s.generations = make([]uint64, capacity)
	s.staging = make([]totalFullTransition, chunkCapacity)
	s.streamHash = sha256.New()
	r := h.beginFullRead()
	at, readErr := clock()
	h.mu.Lock()
	valid := h.validateFullReadLocked(&r, at, readErr)
	h.retireFullReadLocked(&r)
	if valid && o.invalid == "" {
		o.start, o.initialized = at, true
		o.sealed = totalFullCut{available: true, at: at}
		s.committedAt = at
	}
	h.mu.Unlock()
	return h, nil
}

// An offset smaller than capacity, without unchecked head+offset arithmetic.
func (s *totalSegmentState) slot(offset, capacity int) int {
	if offset >= capacity-s.head {
		return offset - (capacity - s.head)
	}
	return s.head + offset
}

func (h *Budget) sealTotalSegment() (totalSegmentDescriptor, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil {
		h.mu.Unlock()
		return totalSegmentDescriptor{}, "INACTIVE"
	}
	if o.disabled {
		h.mu.Unlock()
		return totalSegmentDescriptor{}, "DISABLED"
	}
	if o.segment == nil {
		h.mu.Unlock()
		return totalSegmentDescriptor{}, "INACTIVE"
	}
	s := o.segment
	h.checkFullOwnerLocked()
	// Pending Q is frozen even when a later clock sample could advance time,
	// or the source has since developed an evidence gap.
	if s.qActive {
		q := s.q
		h.mu.Unlock()
		return q, "OFFERED"
	}
	if o.invalid != "" || !o.initialized {
		h.mu.Unlock()
		return totalSegmentDescriptor{}, "UNKNOWN"
	}
	if o.reads != 0 || o.pending != 0 {
		h.mu.Unlock()
		return totalSegmentDescriptor{}, "IN_FLIGHT"
	}
	sequence, full := o.sequence, o.full
	r := h.beginFullReadLocked()
	readGeneration := o.nextRead // AFTER registration; catches even completed overlapping refused reads
	h.mu.Unlock()
	at, err := h.fullClock()
	h.mu.Lock()
	defer h.mu.Unlock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.checkFullOwnerLocked()
	stable := o.reads == 1 && o.pending == 0 && o.sequence == sequence && o.full == full && o.nextRead == readGeneration
	h.retireFullReadLocked(&r)
	if !valid || o.invalid != "" {
		return totalSegmentDescriptor{}, "UNKNOWN"
	}
	if !stable || o.reads != 0 {
		return totalSegmentDescriptor{}, "UNSTABLE_CUT"
	}
	if s.qActive {
		return s.q, "OFFERED"
	}
	if sequence < s.retiredThrough || sequence-s.retiredThrough > uint64(o.used) || sequence-s.retiredThrough != uint64(o.used) || at < s.committedAt {
		o.fail("segment-cursor-mismatch")
		return totalSegmentDescriptor{}, "UNKNOWN"
	}
	if sequence == s.retiredThrough && at == s.committedAt {
		return totalSegmentDescriptor{}, "NO_PROGRESS"
	}
	if s.nextCheckpoint == totalMaxUint {
		o.fail("segment-checkpoint-overflow")
		return totalSegmentDescriptor{}, "UNKNOWN"
	}
	s.nextCheckpoint++
	s.q = totalSegmentDescriptor{key: o.key, cfg: o.cfg, coverageStart: o.start, checkpointID: s.nextCheckpoint, previousID: s.committedID, previousDigest: s.committedDigest, fromSeq: s.retiredThrough, throughSeq: sequence, fromAt: s.committedAt, throughAt: at, rowCount: o.used, fromCarry: s.committedCarry, throughCarry: s.liveCarry}
	s.qActive, s.digestKnown, s.verifiedPending = true, false, false
	s.qIntegrityFault = ""
	s.qDigest = [32]byte{}
	return s.q, "OFFERED"
}

func (h *Budget) beginTotalSegmentStream(q totalSegmentDescriptor) (totalSegmentStream, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		h.mu.Unlock()
		return totalSegmentStream{}, "REFUSED"
	}
	s := o.segment
	if !s.qActive || s.qIntegrityFault != "" || s.q != q || s.loanPhase != 0 {
		h.mu.Unlock()
		return totalSegmentStream{}, "REFUSED"
	}
	if s.nextStream == totalMaxUint {
		o.fail("segment-stream-overflow")
		h.mu.Unlock()
		return totalSegmentStream{}, "UNKNOWN"
	}
	s.nextStream++
	token := totalSegmentStream{owner: o, checkpointID: q.checkpointID, streamAttemptID: s.nextStream}
	s.stream, s.streamOffset, s.streamFinished = token, 0, false
	s.loanPhase = 4 // header/hash initialization pins Q, including EMPTY
	h.mu.Unlock()
	// Reset transport state only. A known completed Q digest remains immutable
	// and can authenticate an old receipt during this retry.
	s.streamHash.Reset()
	writeTotalSegmentHeader(s.streamHash, q)
	h.mu.Lock()
	defer h.mu.Unlock()
	if !s.qActive || s.q != q || s.stream != token || s.loanPhase != 4 {
		return totalSegmentStream{}, "REFUSED"
	}
	s.loanPhase = 0
	return token, "STREAMING"
}

func (h *Budget) borrowTotalSegmentChunk(token totalSegmentStream) (totalSegmentLoan, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		return totalSegmentLoan{}, "REFUSED"
	}
	s := o.segment
	// Validate BEFORE any ring address or copy lease. Retirement atomically
	// invalidates idle attempts; an old attempt cannot read reused slots.
	if token.owner != o || !s.qActive || s.qIntegrityFault != "" || s.q.checkpointID != token.checkpointID || s.stream != token || s.loanPhase != 0 || s.streamFinished {
		return totalSegmentLoan{}, "REFUSED"
	}
	if s.nextLoan == totalMaxUint {
		o.fail("segment-loan-overflow")
		return totalSegmentLoan{}, "UNKNOWN"
	}
	if s.streamOffset < 0 || s.streamOffset > s.q.rowCount {
		o.fail("segment-stream-offset")
		return totalSegmentLoan{}, "UNKNOWN"
	}
	n := s.q.rowCount - s.streamOffset
	if n > len(s.staging) {
		n = len(s.staging)
	}
	s.nextLoan++
	s.loan = totalSegmentLoan{stream: token, loanID: s.nextLoan, offset: s.streamOffset, count: n}
	s.loanPhase, s.loanValid = 1, false
	return s.loan, "BORROWED"
}

func (h *Budget) copyTotalSegmentChunk(loan totalSegmentLoan) (totalSegmentChunk, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		h.mu.Unlock()
		return totalSegmentChunk{}, "REFUSED"
	}
	s := o.segment
	if loan.stream.owner != o || !s.qActive || s.qIntegrityFault != "" || s.stream != loan.stream || s.q.checkpointID != loan.stream.checkpointID || s.loan != loan || s.loanPhase != 1 {
		h.mu.Unlock()
		return totalSegmentChunk{}, "REFUSED"
	}
	q := s.q
	s.loanPhase = 2
	head := s.head
	h.mu.Unlock()
	// Completed Q rows cannot be overwritten while this exact copy/staging
	// loan is active. Only appends outside this frozen range may occur.
	valid := true
	for i := 0; i < loan.count; i++ {
		offset := loan.offset + i
		index := head
		if offset >= len(o.rows)-head {
			index = offset - (len(o.rows) - head)
		} else {
			index += offset
		}
		row := o.rows[index]
		if !row.done || !row.valid || row.sequence != q.fromSeq+uint64(offset)+1 {
			valid = false
		}
		s.staging[i] = row
		writeTotalSegmentRow(s.streamHash, row)
	}
	final := loan.offset+loan.count == q.rowCount
	var digest [32]byte
	if final {
		s.streamHash.Sum(digest[:0])
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !s.qActive || s.q != q || s.stream != loan.stream || s.loan != loan || s.loanPhase != 2 {
		return totalSegmentChunk{}, "REFUSED"
	}
	if !valid {
		s.qIntegrityFault = "segment-copy-sequence"
		o.fail(s.qIntegrityFault)
		s.loanPhase = 3
		return totalSegmentChunk{}, "UNKNOWN"
	}
	if final {
		if s.digestKnown && s.qDigest != digest {
			s.qIntegrityFault = "segment-retry-digest-mismatch"
			o.fail(s.qIntegrityFault)
			s.loanPhase = 3
			return totalSegmentChunk{}, "UNKNOWN"
		}
		if !s.digestKnown {
			s.qDigest, s.digestKnown = digest, true
		}
	}
	s.loanPhase, s.loanValid = 3, true
	return totalSegmentChunk{descriptor: q, offset: loan.offset, rows: s.staging[:loan.count], final: final}, "COPIED"
}

func (h *Budget) releaseTotalSegmentChunk(loan totalSegmentLoan) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		return false
	}
	s := o.segment
	if loan.stream.owner != o || !s.qActive || s.stream != loan.stream || s.q.checkpointID != loan.stream.checkpointID || s.loan != loan || s.loanPhase != 3 {
		return false
	}
	// UNKNOWN Copy still owns staging until cleanup, but it delivered no
	// valid progress. Releasing it may not complete or advance the attempt.
	if s.loanValid && s.qIntegrityFault == "" {
		s.streamOffset = loan.offset + loan.count
		s.streamFinished = s.streamOffset == s.q.rowCount
	}
	s.loanPhase, s.loanValid = 0, false
	return true
}

func (h *Budget) expectedTotalSegmentWitness() (totalSegmentWitness, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.fullObservation
	if o == nil || o.segment == nil || !o.segment.qActive || !o.segment.digestKnown || o.segment.qIntegrityFault != "" {
		return totalSegmentWitness{}, false
	}
	s := o.segment
	return totalSegmentWitness{owner: o, session: s.session, descriptor: s.q, digest: s.qDigest}, true
}

func (h *Budget) acknowledgeTotalSegment(receipt totalSegmentReceipt) (string, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		h.mu.Unlock()
		return "REFUSED", "inactive"
	}
	s := o.segment
	if s.lastValid && receipt == s.last {
		h.mu.Unlock()
		return "ALREADY_RETIRED", "exact-last-replay"
	}
	if !s.qActive || !s.digestKnown {
		h.mu.Unlock()
		return "REFUSED", "no-completed-checkpoint-digest"
	}
	if s.qIntegrityFault != "" {
		h.mu.Unlock()
		return "REFUSED", "checkpoint-integrity-fault"
	}
	expected := totalSegmentWitness{owner: o, session: s.session, descriptor: s.q, digest: s.qDigest}
	if receipt.witness != expected {
		h.mu.Unlock()
		return "REFUSED", "witness-mismatch"
	}
	issuer := s.issuer
	h.mu.Unlock()
	// Only the bound package-private issuer can verify the opaque proof. The
	// conditional TEST fixture proves owner protocol only; the explicit typed
	// file issuer separately confirms real storage under its conditional contract.
	commit, valid := issuer.verifyTotalSegment(receipt)
	h.mu.Lock()
	defer h.mu.Unlock()
	if !valid || commit.witness != expected {
		return "REFUSED", "unverified-commit"
	}
	if s.lastValid && receipt == s.last {
		return "ALREADY_RETIRED", "exact-last-replay"
	}
	// Verification can overlap another ACK/retirement. Revalidate the actual
	// current Q/session, never install a verified result onto a later Q.
	if !s.qActive || !s.digestKnown || s.qIntegrityFault != "" || (totalSegmentWitness{owner: o, session: s.session, descriptor: s.q, digest: s.qDigest}) != commit.witness {
		return "REFUSED", "checkpoint-changed-during-verification"
	}
	s.verifiedPending, s.pendingReceipt = true, receipt
	return h.retireVerifiedTotalSegmentLocked()
}

func (h *Budget) retireVerifiedTotalSegment() (string, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fullObservation == nil || h.fullObservation.segment == nil {
		return "REFUSED", "inactive"
	}
	return h.retireVerifiedTotalSegmentLocked()
}
func (h *Budget) retireVerifiedTotalSegmentLocked() (string, string) {
	o, s := h.fullObservation, h.fullObservation.segment
	if s.qIntegrityFault != "" {
		return "REFUSED", "checkpoint-integrity-fault"
	}
	if !s.qActive || !s.digestKnown || !s.verifiedPending {
		return "REFUSED", "no-verified-commit"
	}
	if s.loanPhase != 0 {
		return "DEFERRED", "copy-or-staging-loan"
	}
	if o.reads != 0 || o.pending != 0 {
		return "DEFERRED", "read-or-completion"
	}
	q := s.q
	if q.fromSeq != s.retiredThrough || q.throughSeq < q.fromSeq || q.throughSeq-q.fromSeq != uint64(q.rowCount) || q.rowCount > o.used {
		o.fail("segment-retire-cursor")
		return "UNKNOWN", "cursor-mismatch"
	}
	// O(1) prefix retirement. No zero/copy/scan/reset of provider state, live
	// reservations, start or absolute sequence. Per-cell generation increments
	// only when that one cell is subsequently reserved by the actual owner.
	if q.rowCount != 0 {
		s.head = s.slot(q.rowCount, len(o.rows))
	}
	o.used -= q.rowCount
	s.retiredThrough, s.committedAt = q.throughSeq, q.throughAt
	s.committedCarry = q.throughCarry
	s.committedID, s.committedDigest = q.checkpointID, s.qDigest
	s.last = s.pendingReceipt // exact already-verified proof, constant-size cache
	s.pendingReceipt = totalSegmentReceipt{}
	s.lastValid = true
	s.qActive, s.digestKnown, s.verifiedPending = false, false, false
	s.stream = totalSegmentStream{} // invalidate idle retry atomically
	s.streamOffset, s.streamFinished = 0, false
	return "RETIRED", "verified-whole-checkpoint"
}

// Canonical versioned encoding is incremental, bounded and entirely outside
// the owner leaf. Integers have fixed little-endian width, strings are length
// delimited, and all original row/carry fields are included. No pointer bytes.
func writeTotalSegmentUint(w hash.Hash, n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	_, _ = w.Write(b[:])
}
func writeTotalSegmentBool(w hash.Hash, b bool) {
	var n uint64
	if b {
		n = 1
	}
	writeTotalSegmentUint(w, n)
}
func writeTotalSegmentRow(w hash.Hash, row totalFullTransition) {
	writeTotalSegmentUint(w, row.sequence)
	writeTotalSegmentBool(w, row.full)
	writeTotalSegmentUint(w, uint64(row.count))
	writeTotalSegmentUint(w, uint64(row.lower))
	writeTotalSegmentUint(w, uint64(row.upper))
	writeTotalSegmentBool(w, row.done)
	writeTotalSegmentBool(w, row.valid)
}
func writeTotalSegmentCarry(w hash.Hash, carry totalSegmentCarry) {
	writeTotalSegmentBool(w, carry.full)
	writeTotalSegmentRow(w, carry.entry)
}

// String input is already caller-owned immutable storage. Feed fixed blocks
// instead of allocating a []byte proportional to a caller reference length.
func writeTotalSegmentString(w hash.Hash, value string) {
	var block [64]byte
	for len(value) != 0 {
		n := copy(block[:], value)
		_, _ = w.Write(block[:n])
		value = value[n:]
	}
}
func writeTotalSegmentHeader(w hash.Hash, q totalSegmentDescriptor) {
	_, _ = w.Write([]byte("corsa-k5-connection-total-segment-v2\x00"))
	writeTotalSegmentUint(w, uint64(len(q.key.anchor)))
	writeTotalSegmentString(w, q.key.anchor)
	_, _ = w.Write(q.key.epoch[:])
	_, _ = w.Write(q.key.clockDomain[:])
	_, _ = w.Write(q.key.configuration[:])
	writeTotalSegmentUint(w, uint64(q.cfg.Total))
	writeTotalSegmentUint(w, uint64(q.cfg.OutboundReserve))
	writeTotalSegmentUint(w, uint64(q.cfg.MaxOutbound))
	writeTotalSegmentUint(w, uint64(q.cfg.MaxInbound))
	writeTotalSegmentUint(w, uint64(q.cfg.MaxAuxiliary))
	writeTotalSegmentUint(w, uint64(q.coverageStart))
	writeTotalSegmentUint(w, q.checkpointID)
	writeTotalSegmentUint(w, q.previousID)
	_, _ = w.Write(q.previousDigest[:])
	writeTotalSegmentUint(w, q.fromSeq)
	writeTotalSegmentUint(w, q.throughSeq)
	writeTotalSegmentUint(w, uint64(q.fromAt))
	writeTotalSegmentUint(w, uint64(q.throughAt))
	writeTotalSegmentUint(w, uint64(q.rowCount))
	writeTotalSegmentCarry(w, q.fromCarry)
	writeTotalSegmentCarry(w, q.throughCarry)
}

// Small total-specific streaming reconstruction core, not a collector or
// UTC/load issuer. It holds O(1) state. Caller dedup key is SourceKey/Qid/digest;
// it must also verify contiguous previousID/digest/cut/sequence before calling.
// Emitted ranges are PROVISIONAL until finish succeeds and the complete Q
// digest/trusted receipt is validated. A chunk alone certifies no temporal cut.
// Every contribution is clipped to [previous committed cut, this actual cut].
type totalSegmentRange struct {
	definiteStart, definiteEnd time.Duration
	possibleStart, possibleEnd time.Duration
}

type totalSegmentFold struct {
	q                 totalSegmentDescriptor
	throughSeq        uint64
	entry             totalSegmentCarry
	invalid, finished bool
}

func newTotalSegmentFold(q totalSegmentDescriptor) (totalSegmentFold, bool) {
	f := totalSegmentFold{q: q, throughSeq: q.fromSeq, entry: q.fromCarry}
	valid := q.cfg.Total > 0 && q.cfg.OutboundReserve >= 0 && q.cfg.OutboundReserve <= q.cfg.Total && q.cfg.MaxOutbound >= 0 && q.cfg.MaxInbound >= 0 && q.cfg.MaxAuxiliary >= 0 && q.checkpointID != 0 && q.throughSeq >= q.fromSeq && q.rowCount >= 0 && q.throughSeq-q.fromSeq == uint64(q.rowCount) && q.coverageStart >= 0 && q.fromAt >= q.coverageStart && q.throughAt >= q.fromAt
	if q.fromCarry.full {
		r := q.fromCarry.entry
		valid = valid && r.full && r.done && r.valid && r.count == q.cfg.Total && r.sequence <= q.fromSeq && r.sequence != 0 && r.lower >= 0 && r.upper >= r.lower && r.upper <= q.fromAt-q.coverageStart
	} else {
		valid = valid && q.fromCarry.entry == (totalFullTransition{})
	}
	f.invalid = !valid
	return f, valid
}
func (f *totalSegmentFold) clip(r totalSegmentRange) (totalSegmentRange, bool) {
	lo, hi := f.q.fromAt-f.q.coverageStart, f.q.throughAt-f.q.coverageStart
	clamp := func(v time.Duration) time.Duration {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	r.definiteStart, r.definiteEnd = clamp(r.definiteStart), clamp(r.definiteEnd)
	r.possibleStart, r.possibleEnd = clamp(r.possibleStart), clamp(r.possibleEnd)
	return r, r.possibleStart < r.possibleEnd
}
func (f *totalSegmentFold) row(row totalFullTransition) (totalSegmentRange, bool, bool) {
	if f.invalid || f.finished || f.throughSeq == totalMaxUint || f.throughSeq >= f.q.throughSeq || row.sequence != f.throughSeq+1 || !row.done || !row.valid || row.lower < f.q.fromAt-f.q.coverageStart || row.upper < row.lower || row.upper > f.q.throughAt-f.q.coverageStart {
		f.invalid = true
		return totalSegmentRange{}, false, false
	}
	f.throughSeq = row.sequence
	if row.full {
		if f.entry.full || row.count != f.q.cfg.Total {
			f.invalid = true
			return totalSegmentRange{}, false, false
		}
		f.entry = totalSegmentCarry{full: true, entry: row}
		return totalSegmentRange{}, false, true
	}
	if !f.entry.full || row.count != f.q.cfg.Total-1 || row.upper < f.entry.entry.lower {
		f.invalid = true
		return totalSegmentRange{}, false, false
	}
	entry := f.entry.entry
	through := row.lower
	if through < entry.upper {
		through = entry.upper
	}
	f.entry = totalSegmentCarry{}
	r, emitted := f.clip(totalSegmentRange{entry.upper, through, entry.lower, row.upper})
	return r, emitted, true
}
func (f *totalSegmentFold) finish() (totalSegmentRange, bool, bool) {
	if f.invalid || f.finished || f.throughSeq != f.q.throughSeq || f.entry != f.q.throughCarry {
		f.invalid = true
		return totalSegmentRange{}, false, false
	}
	f.finished = true
	if !f.entry.full {
		return totalSegmentRange{}, false, true
	}
	entry := f.entry.entry
	r, emitted := f.clip(totalSegmentRange{entry.upper, f.q.throughAt - f.q.coverageStart, entry.lower, f.q.throughAt - f.q.coverageStart})
	return r, emitted, true
}
