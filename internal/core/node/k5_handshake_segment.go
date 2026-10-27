package node

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"time"
	"unsafe"
)

// Private prepublication V2 protocol. Ordinary/V1 sources remain append-only.
// An explicit inactive file-backed construction supplies a concrete issuer;
// default constructors select no V2 source, worker, transport or scheduler.
// An issuer is a TRUSTED boundary, not a caller assertion of fsync success.
// Backend durability/resource qualification and operational activation remain
// separate from this protocol and its explicit construction.
type handshakeSegmentCarry struct {
	full  bool
	entry handshakeFullTransition
}
type handshakeSegmentDescriptor struct {
	key                      k5SourceKey
	bounds                   handshakeBounds
	coverageStart            time.Duration
	checkpointID, previousID uint64
	previousDigest           [32]byte
	// fromSeq is an exclusive cursor, including EMPTY. Never compute a
	// synthetic first=fromSeq+1 for an empty range at MaxUint64.
	fromSeq, throughSeq     uint64
	fromAt, throughAt       time.Duration
	rowCount                int
	fromCarry, throughCarry handshakeSegmentCarry
}
type handshakeSegmentWitness struct {
	owner      *handshakeFullObservation
	session    [16]byte
	descriptor handshakeSegmentDescriptor
	digest     [32]byte
}
type handshakeSegmentReceipt struct {
	witness handshakeSegmentWitness
	// Opaque issuer proof. A digest alone never constructs a verified commit.
	proof [32]byte
}
type handshakeSegmentCommit struct{ witness handshakeSegmentWitness }
type handshakeSegmentIssuer interface {
	verifyHandshakeSegment(handshakeSegmentReceipt) (handshakeSegmentCommit, bool)
}
type handshakeSegmentStream struct {
	owner                         *handshakeFullObservation
	checkpointID, streamAttemptID uint64
}
type handshakeSegmentLoan struct {
	stream        handshakeSegmentStream
	loanID        uint64
	offset, count int
}
type handshakeSegmentChunk struct {
	descriptor handshakeSegmentDescriptor
	offset     int
	// Staging only, never ring aliases. Valid until exact Release. The trusted
	// consumer must finish using this loan before releasing it; arbitrary
	// retained consumer copies are outside the source-owned memory bound.
	rows  []handshakeFullTransition
	final bool
}
type handshakeSegmentState struct {
	head                                        int
	generations                                 []uint64
	staging                                     []handshakeFullTransition
	issuer                                      handshakeSegmentIssuer
	session                                     [16]byte
	liveCarry, committedCarry                   handshakeSegmentCarry
	retiredThrough, committedID, nextCheckpoint uint64
	committedAt                                 time.Duration
	committedDigest                             [32]byte
	q                                           handshakeSegmentDescriptor
	qActive, digestKnown                        bool
	qIntegrityFault                             string // sticky for THIS Q, distinct from later outside-Q gaps
	qDigest                                     [32]byte
	last, pendingReceipt                        handshakeSegmentReceipt
	lastValid, verifiedPending                  bool
	nextStream, nextLoan                        uint64
	stream                                      handshakeSegmentStream
	streamOffset                                int
	streamFinished                              bool
	streamHash                                  hash.Hash
	loan                                        handshakeSegmentLoan
	// 0 idle, 1 reserved copy, 2 copying/hashing, 3 consumer staging loan,
	// 4 header initialization. Nonzero pins both Q and the one hash/buffer.
	loanPhase uint8
	loanValid bool
}

func newSegmentedInboundHandshakes(bounds handshakeBounds, key k5SourceKey, capacity, chunkCapacity int, clock func() (time.Duration, error), issuer handshakeSegmentIssuer, session [16]byte) *inboundHandshakes {
	h := newInboundHandshakes(bounds)
	s := &handshakeSegmentState{issuer: issuer, session: session}
	o := &handshakeFullObservation{key: key, bounds: bounds, clock: clock, segment: s}
	h.fullObservation = o // explicit V2 even if any subsequent validation fails
	rowBytes, generationBytes := uint64(unsafe.Sizeof(handshakeFullTransition{})), uint64(unsafe.Sizeof(uint64(0)))
	maxBytes := uint64(^uint(0) >> 1)
	switch {
	case !validObservationReference(key.anchor) || key.epoch == ([16]byte{}) || key.clockDomain == ([16]byte{}) || key.configuration == ([16]byte{}) || key.kind != k5HandshakeNode:
		o.fail("invalid-source-identity")
	case bounds.node <= 0 || bounds.loopback < 0 || bounds.perSource < 0:
		o.fail("invalid-bounds")
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
		return h
	}
	// Allocation/hash construction outside the leaf, before publication. These
	// are caller allocation requests, not chosen deployment/resource limits.
	o.rows = make([]handshakeFullTransition, capacity)
	s.generations = make([]uint64, capacity)
	s.staging = make([]handshakeFullTransition, chunkCapacity)
	s.streamHash = sha256.New()
	r := h.beginFullRead()
	at, err := clock()
	h.mu.Lock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.retireFullReadLocked(&r)
	if valid && o.invalid == "" {
		o.start, o.initialized = at, true
		o.sealed = handshakeFullCut{available: true, at: at}
		s.committedAt = at
	}
	h.mu.Unlock()
	return h
}

// An offset smaller than capacity, without unchecked head+offset arithmetic.
func (s *handshakeSegmentState) slot(offset, capacity int) int {
	if offset >= capacity-s.head {
		return offset - (capacity - s.head)
	}
	return s.head + offset
}

func (h *inboundHandshakes) sealHandshakeSegment() (handshakeSegmentDescriptor, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		h.mu.Unlock()
		return handshakeSegmentDescriptor{}, "INACTIVE"
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
		return handshakeSegmentDescriptor{}, "UNKNOWN"
	}
	if o.reads != 0 || o.pending != 0 {
		h.mu.Unlock()
		return handshakeSegmentDescriptor{}, "IN_FLIGHT"
	}
	sequence, full := o.sequence, o.full
	r := h.beginFullReadLocked()
	h.mu.Unlock()
	at, err := h.fullClock()
	h.mu.Lock()
	defer h.mu.Unlock()
	valid := h.validateFullReadLocked(&r, at, err)
	h.checkFullOwnerLocked()
	stable := o.reads == 1 && o.pending == 0 && o.sequence == sequence && o.full == full
	h.retireFullReadLocked(&r)
	if !valid || o.invalid != "" {
		return handshakeSegmentDescriptor{}, "UNKNOWN"
	}
	if !stable || o.reads != 0 {
		return handshakeSegmentDescriptor{}, "UNSTABLE_CUT"
	}
	if s.qActive {
		return s.q, "OFFERED"
	}
	if sequence < s.retiredThrough || sequence-s.retiredThrough > uint64(o.used) || sequence-s.retiredThrough != uint64(o.used) || at < s.committedAt {
		o.fail("segment-cursor-mismatch")
		return handshakeSegmentDescriptor{}, "UNKNOWN"
	}
	if sequence == s.retiredThrough && at == s.committedAt {
		return handshakeSegmentDescriptor{}, "NO_PROGRESS"
	}
	if s.nextCheckpoint == k5MaxUint {
		o.fail("segment-checkpoint-overflow")
		return handshakeSegmentDescriptor{}, "UNKNOWN"
	}
	s.nextCheckpoint++
	s.q = handshakeSegmentDescriptor{key: o.key, bounds: o.bounds, coverageStart: o.start, checkpointID: s.nextCheckpoint, previousID: s.committedID, previousDigest: s.committedDigest, fromSeq: s.retiredThrough, throughSeq: sequence, fromAt: s.committedAt, throughAt: at, rowCount: o.used, fromCarry: s.committedCarry, throughCarry: s.liveCarry}
	s.qActive, s.digestKnown, s.verifiedPending = true, false, false
	s.qIntegrityFault = ""
	s.qDigest = [32]byte{}
	return s.q, "OFFERED"
}

func (h *inboundHandshakes) beginHandshakeSegmentStream(q handshakeSegmentDescriptor) (handshakeSegmentStream, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		h.mu.Unlock()
		return handshakeSegmentStream{}, "REFUSED"
	}
	s := o.segment
	if !s.qActive || s.qIntegrityFault != "" || s.q != q || s.loanPhase != 0 {
		h.mu.Unlock()
		return handshakeSegmentStream{}, "REFUSED"
	}
	if s.nextStream == k5MaxUint {
		o.fail("segment-stream-overflow")
		h.mu.Unlock()
		return handshakeSegmentStream{}, "UNKNOWN"
	}
	s.nextStream++
	token := handshakeSegmentStream{owner: o, checkpointID: q.checkpointID, streamAttemptID: s.nextStream}
	s.stream, s.streamOffset, s.streamFinished = token, 0, false
	s.loanPhase = 4 // header/hash initialization pins Q, including EMPTY
	h.mu.Unlock()
	// Reset transport state only. A known completed Q digest remains immutable
	// and can authenticate an old receipt during this retry.
	s.streamHash.Reset()
	writeHandshakeSegmentHeader(s.streamHash, q)
	h.mu.Lock()
	defer h.mu.Unlock()
	if !s.qActive || s.q != q || s.stream != token || s.loanPhase != 4 {
		return handshakeSegmentStream{}, "REFUSED"
	}
	s.loanPhase = 0
	return token, "STREAMING"
}

func (h *inboundHandshakes) borrowHandshakeSegmentChunk(token handshakeSegmentStream) (handshakeSegmentLoan, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		return handshakeSegmentLoan{}, "REFUSED"
	}
	s := o.segment
	// Validate BEFORE any ring address or copy lease. Retirement atomically
	// invalidates idle attempts; an old attempt cannot read reused slots.
	if token.owner != o || !s.qActive || s.qIntegrityFault != "" || s.q.checkpointID != token.checkpointID || s.stream != token || s.loanPhase != 0 || s.streamFinished {
		return handshakeSegmentLoan{}, "REFUSED"
	}
	if s.nextLoan == k5MaxUint {
		o.fail("segment-loan-overflow")
		return handshakeSegmentLoan{}, "UNKNOWN"
	}
	if s.streamOffset < 0 || s.streamOffset > s.q.rowCount {
		o.fail("segment-stream-offset")
		return handshakeSegmentLoan{}, "UNKNOWN"
	}
	n := s.q.rowCount - s.streamOffset
	if n > len(s.staging) {
		n = len(s.staging)
	}
	s.nextLoan++
	s.loan = handshakeSegmentLoan{stream: token, loanID: s.nextLoan, offset: s.streamOffset, count: n}
	s.loanPhase, s.loanValid = 1, false
	return s.loan, "BORROWED"
}

func (h *inboundHandshakes) copyHandshakeSegmentChunk(loan handshakeSegmentLoan) (handshakeSegmentChunk, string) {
	h.mu.Lock()
	o := h.fullObservation
	if o == nil || o.segment == nil {
		h.mu.Unlock()
		return handshakeSegmentChunk{}, "REFUSED"
	}
	s := o.segment
	if loan.stream.owner != o || !s.qActive || s.qIntegrityFault != "" || s.stream != loan.stream || s.q.checkpointID != loan.stream.checkpointID || s.loan != loan || s.loanPhase != 1 {
		h.mu.Unlock()
		return handshakeSegmentChunk{}, "REFUSED"
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
		writeHandshakeSegmentRow(s.streamHash, row)
	}
	final := loan.offset+loan.count == q.rowCount
	var digest [32]byte
	if final {
		s.streamHash.Sum(digest[:0])
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !s.qActive || s.q != q || s.stream != loan.stream || s.loan != loan || s.loanPhase != 2 {
		return handshakeSegmentChunk{}, "REFUSED"
	}
	if !valid {
		s.qIntegrityFault = "segment-copy-sequence"
		o.fail(s.qIntegrityFault)
		s.loanPhase = 3
		return handshakeSegmentChunk{}, "UNKNOWN"
	}
	if final {
		if s.digestKnown && s.qDigest != digest {
			s.qIntegrityFault = "segment-retry-digest-mismatch"
			o.fail(s.qIntegrityFault)
			s.loanPhase = 3
			return handshakeSegmentChunk{}, "UNKNOWN"
		}
		if !s.digestKnown {
			s.qDigest, s.digestKnown = digest, true
		}
	}
	s.loanPhase, s.loanValid = 3, true
	return handshakeSegmentChunk{descriptor: q, offset: loan.offset, rows: s.staging[:loan.count], final: final}, "COPIED"
}

func (h *inboundHandshakes) releaseHandshakeSegmentChunk(loan handshakeSegmentLoan) bool {
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

func (h *inboundHandshakes) expectedHandshakeSegmentWitness() (handshakeSegmentWitness, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.fullObservation
	if o == nil || o.segment == nil || !o.segment.qActive || !o.segment.digestKnown || o.segment.qIntegrityFault != "" {
		return handshakeSegmentWitness{}, false
	}
	s := o.segment
	return handshakeSegmentWitness{owner: o, session: s.session, descriptor: s.q, digest: s.qDigest}, true
}

func (h *inboundHandshakes) acknowledgeHandshakeSegment(receipt handshakeSegmentReceipt) (string, string) {
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
	expected := handshakeSegmentWitness{owner: o, session: s.session, descriptor: s.q, digest: s.qDigest}
	if receipt.witness != expected {
		h.mu.Unlock()
		return "REFUSED", "witness-mismatch"
	}
	issuer := s.issuer
	h.mu.Unlock()
	// Only the bound trusted boundary can verify the opaque proof. A concrete
	// inactive file issuer verifies actual records under its scoped sync
	// contract; test issuers remain conditional fixtures. Activation is separate.
	commit, valid := issuer.verifyHandshakeSegment(receipt)
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
	if !s.qActive || !s.digestKnown || s.qIntegrityFault != "" || (handshakeSegmentWitness{owner: o, session: s.session, descriptor: s.q, digest: s.qDigest}) != commit.witness {
		return "REFUSED", "checkpoint-changed-during-verification"
	}
	s.verifiedPending, s.pendingReceipt = true, receipt
	return h.retireVerifiedHandshakeSegmentLocked()
}

func (h *inboundHandshakes) retireVerifiedHandshakeSegment() (string, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fullObservation == nil || h.fullObservation.segment == nil {
		return "REFUSED", "inactive"
	}
	return h.retireVerifiedHandshakeSegmentLocked()
}
func (h *inboundHandshakes) retireVerifiedHandshakeSegmentLocked() (string, string) {
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
	s.pendingReceipt = handshakeSegmentReceipt{}
	s.lastValid = true
	s.qActive, s.digestKnown, s.verifiedPending = false, false, false
	s.stream = handshakeSegmentStream{} // invalidate idle retry atomically
	s.streamOffset, s.streamFinished = 0, false
	return "RETIRED", "verified-whole-checkpoint"
}

// Canonical versioned encoding is incremental, bounded and entirely outside
// the owner leaf. Integers have fixed little-endian width, strings are length
// delimited, and all original row/carry fields are included. No pointer bytes.
func writeHandshakeSegmentUint(w hash.Hash, n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	_, _ = w.Write(b[:])
}
func writeHandshakeSegmentBool(w hash.Hash, b bool) {
	var n uint64
	if b {
		n = 1
	}
	writeHandshakeSegmentUint(w, n)
}
func writeHandshakeSegmentRow(w hash.Hash, row handshakeFullTransition) {
	writeHandshakeSegmentUint(w, row.sequence)
	writeHandshakeSegmentBool(w, row.full)
	writeHandshakeSegmentUint(w, uint64(row.count))
	writeHandshakeSegmentUint(w, uint64(row.lower))
	writeHandshakeSegmentUint(w, uint64(row.upper))
	writeHandshakeSegmentBool(w, row.done)
	writeHandshakeSegmentBool(w, row.valid)
}
func writeHandshakeSegmentCarry(w hash.Hash, carry handshakeSegmentCarry) {
	writeHandshakeSegmentBool(w, carry.full)
	writeHandshakeSegmentRow(w, carry.entry)
}
func writeHandshakeSegmentHeader(w hash.Hash, q handshakeSegmentDescriptor) {
	_, _ = w.Write([]byte("corsa-k5-handshake-segment-v2\x00"))
	writeHandshakeSegmentUint(w, uint64(len(q.key.anchor)))
	_, _ = w.Write([]byte(q.key.anchor))
	_, _ = w.Write(q.key.epoch[:])
	_, _ = w.Write(q.key.clockDomain[:])
	_, _ = w.Write(q.key.configuration[:])
	writeHandshakeSegmentUint(w, uint64(q.key.kind))
	writeHandshakeSegmentUint(w, uint64(q.bounds.node))
	writeHandshakeSegmentUint(w, uint64(q.bounds.loopback))
	writeHandshakeSegmentUint(w, uint64(q.bounds.perSource))
	writeHandshakeSegmentUint(w, uint64(q.coverageStart))
	writeHandshakeSegmentUint(w, q.checkpointID)
	writeHandshakeSegmentUint(w, q.previousID)
	_, _ = w.Write(q.previousDigest[:])
	writeHandshakeSegmentUint(w, q.fromSeq)
	writeHandshakeSegmentUint(w, q.throughSeq)
	writeHandshakeSegmentUint(w, uint64(q.fromAt))
	writeHandshakeSegmentUint(w, uint64(q.throughAt))
	writeHandshakeSegmentUint(w, uint64(q.rowCount))
	writeHandshakeSegmentCarry(w, q.fromCarry)
	writeHandshakeSegmentCarry(w, q.throughCarry)
}

// Small handshake-specific streaming reconstruction core, not a collector or
// UTC/load issuer. It holds O(1) state. Caller dedup key is SourceKey/Qid/digest;
// it must also verify contiguous previousID/digest/cut/sequence before calling.
// Emitted ranges are PROVISIONAL until finish succeeds and the complete Q
// digest/trusted receipt is validated. A chunk alone certifies no temporal cut.
// Every contribution is clipped to [previous committed cut, this actual cut].
type handshakeSegmentFold struct {
	q                 handshakeSegmentDescriptor
	throughSeq        uint64
	entry             handshakeSegmentCarry
	invalid, finished bool
}

func newHandshakeSegmentFold(q handshakeSegmentDescriptor) (handshakeSegmentFold, bool) {
	f := handshakeSegmentFold{q: q, throughSeq: q.fromSeq, entry: q.fromCarry}
	valid := q.key.kind == k5HandshakeNode && q.bounds.node > 0 && q.checkpointID != 0 && q.throughSeq >= q.fromSeq && q.rowCount >= 0 && q.throughSeq-q.fromSeq == uint64(q.rowCount) && q.coverageStart >= 0 && q.fromAt >= q.coverageStart && q.throughAt >= q.fromAt
	if q.fromCarry.full {
		r := q.fromCarry.entry
		valid = valid && r.full && r.done && r.valid && r.count == q.bounds.node && r.sequence <= q.fromSeq && r.sequence != 0 && r.lower >= 0 && r.upper >= r.lower && r.upper <= q.fromAt-q.coverageStart
	} else {
		valid = valid && q.fromCarry.entry == (handshakeFullTransition{})
	}
	f.invalid = !valid
	return f, valid
}
func (f *handshakeSegmentFold) clip(r handshakeFullRange) (handshakeFullRange, bool) {
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
func (f *handshakeSegmentFold) row(row handshakeFullTransition) (handshakeFullRange, bool, bool) {
	if f.invalid || f.finished || f.throughSeq == k5MaxUint || f.throughSeq >= f.q.throughSeq || row.sequence != f.throughSeq+1 || !row.done || !row.valid || row.lower < f.q.fromAt-f.q.coverageStart || row.upper < row.lower || row.upper > f.q.throughAt-f.q.coverageStart {
		f.invalid = true
		return handshakeFullRange{}, false, false
	}
	f.throughSeq = row.sequence
	if row.full {
		if f.entry.full || row.count != f.q.bounds.node {
			f.invalid = true
			return handshakeFullRange{}, false, false
		}
		f.entry = handshakeSegmentCarry{full: true, entry: row}
		return handshakeFullRange{}, false, true
	}
	if !f.entry.full || row.count != f.q.bounds.node-1 || row.upper < f.entry.entry.lower {
		f.invalid = true
		return handshakeFullRange{}, false, false
	}
	entry := f.entry.entry
	through := row.lower
	if through < entry.upper {
		through = entry.upper
	}
	f.entry = handshakeSegmentCarry{}
	r, emitted := f.clip(handshakeFullRange{entry.upper, through, entry.lower, row.upper})
	return r, emitted, true
}
func (f *handshakeSegmentFold) finish() (handshakeFullRange, bool, bool) {
	if f.invalid || f.finished || f.throughSeq != f.q.throughSeq || f.entry != f.q.throughCarry {
		f.invalid = true
		return handshakeFullRange{}, false, false
	}
	f.finished = true
	if !f.entry.full {
		return handshakeFullRange{}, false, true
	}
	entry := f.entry.entry
	r, emitted := f.clip(handshakeFullRange{entry.upper, f.q.throughAt - f.q.coverageStart, entry.lower, f.q.throughAt - f.q.coverageStart})
	return r, emitted, true
}
