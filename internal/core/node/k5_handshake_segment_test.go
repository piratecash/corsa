package node

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"hash"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// CONDITIONAL TEST ISSUER ONLY. Signatures assume verified persistence; no
// disk write, fsync or recovery is performed. No production issuer exists.
type k5SegmentTestIssuer struct {
	owner             *handshakeFullObservation
	session           [16]byte
	secret            [32]byte
	gate              *inboundHandshakes
	accept, underLeaf atomic.Bool
	calls             atomic.Int64
	after             func(int64)
}

func (i *k5SegmentTestIssuer) proof(w handshakeSegmentWitness) [32]byte {
	h := hmac.New(sha256.New, i.secret[:])
	writeHandshakeSegmentHeader(h, w.descriptor)
	_, _ = h.Write(w.session[:])
	_, _ = h.Write(w.digest[:])
	var b [32]byte
	h.Sum(b[:0])
	return b
}
func (i *k5SegmentTestIssuer) sign(w handshakeSegmentWitness) handshakeSegmentReceipt {
	return handshakeSegmentReceipt{witness: w, proof: i.proof(w)}
}
func (i *k5SegmentTestIssuer) verifyHandshakeSegment(r handshakeSegmentReceipt) (handshakeSegmentCommit, bool) {
	if !i.gate.mu.TryLock() {
		i.underLeaf.Store(true)
	} else {
		i.gate.mu.Unlock()
	}
	n := i.calls.Add(1)
	if i.after != nil {
		i.after(n)
	}
	if !i.accept.Load() || r.witness.owner != i.owner || r.witness.session != i.session || r.proof != i.proof(r.witness) {
		return handshakeSegmentCommit{}, false
	}
	return handshakeSegmentCommit{witness: r.witness}, true
}

// Test wrapper pauses the actual Copy hash write in phase2; the SHA state is
// still real. This adds no production hook and mutates no gate counters.
type k5SegmentPausedHash struct {
	hash.Hash
	armed   atomic.Bool
	once    sync.Once
	started chan struct{}
	resume  <-chan struct{}
}

func (h *k5SegmentPausedHash) Write(b []byte) (int, error) {
	if h.armed.Load() {
		h.once.Do(func() { close(h.started); <-h.resume })
	}
	return h.Hash.Write(b)
}

type k5SegmentFixture struct {
	h              *inboundHandshakes
	d              *k5TestClock
	issuer         *k5SegmentTestIssuer
	releases       []func()
	clockUnderLeaf atomic.Bool
}

func newK5SegmentFixture(t *testing.T, capacity, chunkCapacity int, bounds handshakeBounds) *k5SegmentFixture {
	t.Helper()
	f := &k5SegmentFixture{d: &k5TestClock{}}
	f.issuer = &k5SegmentTestIssuer{session: [16]byte{9}, secret: [32]byte{71}}
	f.issuer.accept.Store(true)
	clock := func() (time.Duration, error) {
		if f.h != nil {
			if !f.h.mu.TryLock() {
				f.clockUnderLeaf.Store(true)
			} else {
				f.h.mu.Unlock()
			}
		}
		return f.d.read()
	}
	f.h = newSegmentedInboundHandshakes(bounds, k5TestKey(41), capacity, chunkCapacity, clock, f.issuer, f.issuer.session)
	f.issuer.owner, f.issuer.gate = f.h.fullObservation, f.h
	t.Cleanup(f.releaseAll)
	return f
}
func (f *k5SegmentFixture) releaseAll() {
	for _, release := range f.releases {
		release()
	}
}
func (f *k5SegmentFixture) admit(t *testing.T) func() {
	t.Helper()
	r := k5Admit(t, f.h, handshakeSource{group: "fixture-external"})
	f.releases = append(f.releases, r)
	return r
}
func (f *k5SegmentFixture) episode(t *testing.T, at time.Duration) {
	t.Helper()
	f.d.set(at, nil)
	r := f.admit(t)
	f.d.set(at+time.Second, nil)
	r()
	r()
}
func (f *k5SegmentFixture) offer(t *testing.T, at time.Duration) handshakeSegmentDescriptor {
	t.Helper()
	f.d.set(at, nil)
	q, status := f.h.sealHandshakeSegment()
	if status != "OFFERED" {
		t.Fatal("actual owner seal", status, q)
	}
	return q
}
func (f *k5SegmentFixture) stream(t *testing.T, q handshakeSegmentDescriptor) ([]handshakeFullTransition, handshakeSegmentReceipt) {
	t.Helper()
	a, status := f.h.beginHandshakeSegmentStream(q)
	if status != "STREAMING" {
		t.Fatal("begin", status)
	}
	var rows []handshakeFullTransition
	for {
		loan, status := f.h.borrowHandshakeSegmentChunk(a)
		if status != "BORROWED" {
			t.Fatal("borrow", status)
		}
		chunk, status := f.h.copyHandshakeSegmentChunk(loan)
		if status != "COPIED" || chunk.descriptor != q || chunk.offset != len(rows) {
			t.Fatal("copy", status, chunk)
		}
		rows = append(rows, chunk.rows...) // test oracle copy BEFORE Release
		if !f.h.releaseHandshakeSegmentChunk(loan) {
			t.Fatal("release")
		}
		if chunk.final {
			break
		}
	}
	w, known := f.h.expectedHandshakeSegmentWitness()
	if !known || w.descriptor != q {
		t.Fatal("missing whole-Q digest")
	}
	return rows, f.issuer.sign(w)
}
func k5SegmentAck(t *testing.T, h *inboundHandshakes, r handshakeSegmentReceipt, want string) {
	t.Helper()
	status, reason := h.acknowledgeHandshakeSegment(r)
	if status != want {
		t.Fatal("ACK", status, reason, "want", want)
	}
}
func k5SegmentRanges(t *testing.T, q handshakeSegmentDescriptor, rows []handshakeFullTransition) []handshakeFullRange {
	t.Helper()
	fold, ok := newHandshakeSegmentFold(q)
	if !ok {
		t.Fatal("invalid descriptor", q)
	}
	var ranges []handshakeFullRange
	for _, row := range rows {
		r, emitted, ok := fold.row(row)
		if !ok {
			t.Fatal("fold row", row)
		}
		if emitted {
			ranges = append(ranges, r)
		}
	}
	r, emitted, ok := fold.finish()
	if !ok {
		t.Fatal("incomplete checkpoint", q, rows)
	}
	if emitted {
		ranges = append(ranges, r)
	}
	return ranges
}
func k5SegmentMeasures(ranges []handshakeFullRange) (time.Duration, time.Duration) {
	var lower, upper time.Duration
	for _, r := range ranges {
		lower += r.definiteEnd - r.definiteStart
		upper += r.possibleEnd - r.possibleStart
	}
	return lower, upper
}

type k5SegmentRetained struct {
	used, head               int
	sequence, retiredThrough uint64
	committedAt              time.Duration
	carry                    handshakeSegmentCarry
	q                        handshakeSegmentDescriptor
	qActive                  bool
}

func k5SegmentRetainedState(h *inboundHandshakes) k5SegmentRetained {
	h.mu.Lock()
	defer h.mu.Unlock()
	o, s := h.fullObservation, h.fullObservation.segment
	return k5SegmentRetained{o.used, s.head, o.sequence, s.retiredThrough, s.committedAt, s.committedCarry, s.q, s.qActive}
}

func TestK5SegmentOwnerTurnover(t *testing.T) {
	t.Run("whole-Q-open-carry-and-zero-rows", func(t *testing.T) {
		f := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1, loopback: 1, perSource: 1})
		f.d.set(10*time.Second, nil)
		release := f.admit(t)
		if r, err := f.h.enter(handshakeSource{}); r != nil || k5RefusedBy(t, err) != refusalHandshakesNode {
			t.Fatal("original refusal", err)
		}
		q1 := f.offer(t, 20*time.Second)
		rows1, receipt1 := f.stream(t, q1)
		ranges := k5SegmentRanges(t, q1, rows1)
		k5SegmentAck(t, f.h, receipt1, "RETIRED")
		if q1.throughCarry.entry.sequence != 1 || q1.throughCarry.entry.lower != 10*time.Second {
			t.Fatal("carry reset", q1)
		}
		q2 := f.offer(t, 40*time.Second)
		rows2, receipt2 := f.stream(t, q2)
		if len(rows2) != 0 || q2.fromSeq != q2.throughSeq || q2.checkpointID == q1.checkpointID || q2.previousDigest != receipt1.witness.digest {
			t.Fatal("EMPTY identity", q2)
		}
		f.issuer.accept.Store(false)
		k5SegmentAck(t, f.h, receipt2, "REFUSED")
		f.d.set(60*time.Second, nil)
		frozen, status := f.h.sealHandshakeSegment()
		if status != "OFFERED" || frozen != q2 || k5SegmentRetainedState(f.h).committedAt != q1.throughAt {
			t.Fatal("uncertain EMPTY advanced commit")
		}
		_, retry := f.stream(t, q2)
		if retry != receipt2 {
			t.Fatal("EMPTY retry identity")
		}
		f.issuer.accept.Store(true)
		k5SegmentAck(t, f.h, receipt2, "RETIRED")
		ranges = append(ranges, k5SegmentRanges(t, q2, rows2)...)
		f.d.set(40*time.Second, nil)
		beforeID := f.h.fullObservation.segment.nextCheckpoint
		if _, status := f.h.sealHandshakeSegment(); status != "NO_PROGRESS" || f.h.fullObservation.segment.nextCheckpoint != beforeID {
			t.Fatal("same-cut progress", status)
		}
		f.d.set(50*time.Second, nil)
		release()
		release()
		q3 := f.offer(t, 60*time.Second)
		rows3, receipt3 := f.stream(t, q3)
		ranges = append(ranges, k5SegmentRanges(t, q3, rows3)...)
		if q3.fromCarry != q1.throughCarry || q3.throughCarry.full {
			t.Fatal("carry provenance", q3)
		}
		allRows := append(append([]handshakeFullTransition(nil), rows1...), rows3...)
		oracle, ok := handshakeFullRanges(handshakeFullSnapshot{bounds: q1.bounds, coverageStart: q1.coverageStart, cut: handshakeFullCut{available: true, rows: 2, sequence: 2, at: q3.throughAt}, rows: allRows})
		l, u := k5SegmentMeasures(ranges)
		ol, ou := k5SegmentMeasures(oracle)
		if !ok || l != ol || u != ou || l != 40*time.Second {
			t.Fatal("clipping lost/doubled duration", ranges, oracle)
		}
		k5SegmentAck(t, f.h, receipt3, "RETIRED")
		ordinary := newInboundHandshakes(f.h.bounds)
		for n := 0; n < 8; n++ {
			at := time.Duration(70+3*n) * time.Second
			f.d.set(at, nil)
			r := f.admit(t)
			original, err := ordinary.enter(handshakeSource{})
			if err != nil {
				t.Fatal(err)
			}
			_, oe := f.h.enter(handshakeSource{})
			_, be := ordinary.enter(handshakeSource{})
			if k5RefusedBy(t, oe) != k5RefusedBy(t, be) {
				t.Fatal("refusal differs")
			}
			f.d.set(at+time.Second, nil)
			r()
			r()
			original()
			original()
			q := f.offer(t, at+2*time.Second)
			rows, receipt := f.stream(t, q)
			if len(rows) != 2 || rows[0].sequence != uint64(3+2*n) || len(f.h.fullObservation.rows) != 4 || len(f.h.fullObservation.segment.generations) != 4 || len(f.h.fullObservation.segment.staging) != 1 {
				t.Fatal("finite rollover bound/sequence", q)
			}
			k5SegmentAck(t, f.h, receipt, "RETIRED")
		}
		if f.h.inFlight != 0 || ordinary.inFlight != 0 || ordinary.fullObservation != nil || f.clockUnderLeaf.Load() || f.issuer.underLeaf.Load() {
			t.Fatal("original ownership/default or outside-leaf callbacks")
		}
		for _, g := range f.h.fullObservation.segment.generations {
			if g < 2 {
				t.Fatal("actual cells not reused", g)
			}
		}
		v1 := newObservedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(1), 2, f.d.read)
		r := k5Admit(t, v1, handshakeSource{})
		r()
		if v1.fullObservation.segment != nil || v1.fullSnapshot().status != "COMPLETE" {
			t.Fatal("V1 changed")
		}
		legacy := f.h.fullSnapshot()
		if legacy.status != "UNKNOWN" || legacy.cut.available || legacy.rows != nil || !legacy.pastOnly {
			t.Fatal("V2 suffix as V1", legacy)
		}
	})
}

func TestK5SegmentOwnerOwnershipSchedules(t *testing.T) {
	t.Run("reverse-completion-at-chunk-boundary", func(t *testing.T) {
		f := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 2})
		entryStarted, exitStarted := make(chan struct{}), make(chan struct{})
		entryBlock, exitBlock := make(chan struct{}), make(chan struct{})
		entryDone, exitDone := make(chan struct{}), make(chan struct{})
		unblockEntry := sync.OnceFunc(func() { close(entryBlock) })
		unblockExit := sync.OnceFunc(func() { close(exitBlock) })
		track := k5OwnedCleanup(t, unblockEntry, unblockExit, f.releaseAll)
		f.d.after = func(n int) {
			switch n {
			case 3:
				f.d.set(30*time.Second, nil)
			case 4:
				close(entryStarted)
				<-entryBlock
			case 5:
				f.d.set(25*time.Second, nil)
			case 6:
				close(exitStarted)
				<-exitBlock
			}
		}
		a := f.admit(t)
		f.d.set(10*time.Second, nil)
		track(entryDone)
		go func() {
			defer close(entryDone)
			r, err := f.h.enter(handshakeSource{})
			if err != nil {
				t.Error(err)
				return
			}
			r()
			r()
		}()
		k5Wait(t, entryStarted)
		f.d.set(20*time.Second, nil)
		track(exitDone)
		go func() { defer close(exitDone); a(); a() }()
		k5Wait(t, exitStarted)
		if _, status := f.h.sealHandshakeSegment(); status != "IN_FLIGHT" {
			t.Fatal("unresolved reservation certified", status)
		}
		unblockExit()
		k5Wait(t, exitDone)
		if _, status := f.h.sealHandshakeSegment(); status != "IN_FLIGHT" {
			t.Fatal("older upper unpinned", status)
		}
		f.d.set(40*time.Second, nil)
		unblockEntry()
		k5Wait(t, entryDone)
		q := f.offer(t, 40*time.Second)
		rows, receipt := f.stream(t, q)
		if len(rows) != 2 || rows[0].lower != 10*time.Second || rows[0].upper != 30*time.Second || rows[1].lower != 20*time.Second || rows[1].upper != 25*time.Second {
			t.Fatal("reversed reservations", rows)
		}
		got := k5SegmentRanges(t, q, rows)
		if len(got) != 1 || got[0] != (handshakeFullRange{30 * time.Second, 30 * time.Second, 10 * time.Second, 25 * time.Second}) {
			t.Fatal("synthetic chunk cut", got)
		}
		partial, _ := newHandshakeSegmentFold(q)
		_, _, _ = partial.row(rows[0])
		if _, _, ok := partial.finish(); ok {
			t.Fatal("partial chunk became Q cut")
		}
		k5SegmentAck(t, f.h, receipt, "RETIRED")
		// Fault injection ONLY: a stale token cannot touch a later row. Positive
		// evidence above came solely from actual enter/leave reservations.
		f.episode(t, 50*time.Second)
		f.episode(t, 52*time.Second)
		f.h.mu.Lock()
		o := f.h.fullObservation
		index := 0
		before := o.rows[index]
		stale := handshakeFullCompletion{owner: o, index: index, sequence: rows[0].sequence, generation: 1, valid: true, registered: true}
		f.h.completeFullRowLocked(&stale, 54*time.Second, true)
		unchanged := o.rows[index] == before
		invalid := o.invalid
		f.h.mu.Unlock()
		if !unchanged || invalid == "" || f.h.inFlight != 0 {
			t.Fatal("stale completion wrote reused cell")
		}
	})
	t.Run("copy-lease-and-global-quiescence", func(t *testing.T) {
		f := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		_, receipt := f.stream(t, q)
		a, _ := f.h.beginHandshakeSegmentStream(q)
		started, allow, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(allow) })
		track := k5OwnedCleanup(t, unblock, f.releaseAll)
		track(done)
		loans := make(chan handshakeSegmentLoan, 1)
		go func() {
			defer close(done)
			loan, status := f.h.borrowHandshakeSegmentChunk(a)
			if status != "BORROWED" {
				t.Error(status)
				return
			}
			loans <- loan
			close(started)
			<-allow
			if _, status := f.h.copyHandshakeSegmentChunk(loan); status != "COPIED" {
				t.Error(status)
			}
			if !f.h.releaseHandshakeSegmentChunk(loan) {
				t.Error("release")
			}
		}()
		k5Wait(t, started)
		oldLoan := <-loans
		if w, ok := f.h.expectedHandshakeSegmentWitness(); !ok || w != receipt.witness {
			t.Fatal("retry erased known Q digest")
		}
		before := k5SegmentRetainedState(f.h)
		if status, reason := f.h.acknowledgeHandshakeSegment(receipt); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("wrong causal guard", status, reason)
		}
		if k5SegmentRetainedState(f.h) != before {
			t.Fatal("lease allowed retire")
		}
		unblock()
		k5Wait(t, done)
		b, _ := f.h.beginHandshakeSegmentStream(q)
		current, _ := f.h.borrowHandshakeSegmentChunk(b)
		if _, status := f.h.copyHandshakeSegmentChunk(current); status != "COPIED" {
			t.Fatal(status)
		}
		if f.h.releaseHandshakeSegmentChunk(oldLoan) || f.h.fullObservation.segment.loanPhase != 3 || f.h.fullObservation.segment.loan != current {
			t.Fatal("stale release freed new loan")
		}
		if _, status := f.h.copyHandshakeSegmentChunk(current); status != "REFUSED" {
			t.Fatal("duplicate copy rewrote staging/hash")
		}
		if status, reason := f.h.acknowledgeHandshakeSegment(receipt); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal(status, reason)
		}
		if !f.h.releaseHandshakeSegmentChunk(current) {
			t.Fatal("exact release")
		}
		if status, _ := f.h.retireVerifiedHandshakeSegment(); status != "RETIRED" {
			t.Fatal(status)
		}
		// Pause inside the actual outside-leaf hash/copy phase. Duplicate
		// Copy and premature Release must refuse before touching the lane.
		c := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
		c.episode(t, time.Second)
		cq := c.offer(t, 3*time.Second)
		_, cr := c.stream(t, cq)
		copyStarted, copyBlock, copyDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseCopy := sync.OnceFunc(func() { close(copyBlock) })
		trackCopy := k5OwnedCleanup(t, releaseCopy, c.releaseAll)
		trackCopy(copyDone)
		paused := &k5SegmentPausedHash{Hash: c.h.fullObservation.segment.streamHash, started: copyStarted, resume: copyBlock}
		c.h.fullObservation.segment.streamHash = paused
		ca, _ := c.h.beginHandshakeSegmentStream(cq)
		cl, _ := c.h.borrowHandshakeSegmentChunk(ca)
		paused.armed.Store(true)
		go func() {
			defer close(copyDone)
			if _, status := c.h.copyHandshakeSegmentChunk(cl); status != "COPIED" {
				t.Error(status)
			}
		}()
		k5Wait(t, copyStarted)
		if _, status := c.h.copyHandshakeSegmentChunk(cl); status != "REFUSED" {
			t.Fatal("duplicate active Copy")
		}
		if c.h.releaseHandshakeSegmentChunk(cl) {
			t.Fatal("Release freed active copy")
		}
		if status, reason := c.h.acknowledgeHandshakeSegment(cr); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("active copy lease missing", status, reason)
		}
		releaseCopy()
		k5Wait(t, copyDone)
		if !c.h.releaseHandshakeSegmentChunk(cl) {
			t.Fatal("delivered release")
		}
		if status, _ := c.h.retireVerifiedHandshakeSegment(); status != "RETIRED" {
			t.Fatal(status)
		}
		// EMPTY retry header initialization is a distinct, actual phase4
		// lease. A known valid old whole-Q receipt must reach that guard.
		headerFixture := newK5SegmentFixture(t, 2, 1, handshakeBounds{node: 1})
		headerFixture.d.set(10*time.Second, nil)
		headerRelease := headerFixture.admit(t)
		initialHeaderQ := headerFixture.offer(t, 20*time.Second)
		_, initialHeaderReceipt := headerFixture.stream(t, initialHeaderQ)
		k5SegmentAck(t, headerFixture.h, initialHeaderReceipt, "RETIRED")
		emptyHeaderQ := headerFixture.offer(t, 40*time.Second)
		emptyHeaderRows, emptyHeaderReceipt := headerFixture.stream(t, emptyHeaderQ)
		if len(emptyHeaderRows) != 0 || emptyHeaderQ.rowCount != 0 || emptyHeaderQ.fromSeq != emptyHeaderQ.throughSeq {
			t.Fatal("phase4 premise requires a fully streamed EMPTY Q")
		}
		headerStarted, headerBlock, headerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseHeader := sync.OnceFunc(func() { close(headerBlock) })
		trackHeader := k5OwnedCleanup(t, releaseHeader, headerFixture.releaseAll)
		trackHeader(headerDone)
		headerHash := &k5SegmentPausedHash{Hash: headerFixture.h.fullObservation.segment.streamHash, started: headerStarted, resume: headerBlock}
		headerFixture.h.fullObservation.segment.streamHash = headerHash
		headerHash.armed.Store(true)
		var headerRetry handshakeSegmentStream
		go func() {
			defer close(headerDone)
			var status string
			headerRetry, status = headerFixture.h.beginHandshakeSegmentStream(emptyHeaderQ)
			if status != "STREAMING" {
				t.Error("retry header did not finish", status)
			}
		}()
		k5Wait(t, headerStarted)
		if !headerFixture.h.mu.TryLock() {
			t.Fatal("header hashing held the original owner leaf")
		}
		headerSource := headerFixture.h.fullObservation
		headerPhase := headerSource.segment.loanPhase
		headerReads, headerPending := headerSource.reads, headerSource.pending
		headerFixture.h.mu.Unlock()
		if headerPhase != 4 || headerReads != 0 || headerPending != 0 {
			t.Fatal("phase4 causal premise", headerPhase, headerReads, headerPending)
		}
		if witness, known := headerFixture.h.expectedHandshakeSegmentWitness(); !known || witness != emptyHeaderReceipt.witness {
			t.Fatal("retry header erased the already completed Q digest")
		}
		headerBefore := k5SegmentRetainedState(headerFixture.h)
		if status, reason := headerFixture.h.acknowledgeHandshakeSegment(emptyHeaderReceipt); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("valid EMPTY receipt missed phase4 guard", status, reason)
		}
		if status, reason := headerFixture.h.retireVerifiedHandshakeSegment(); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("pending verified EMPTY receipt bypassed phase4", status, reason)
		}
		if duplicate, status := headerFixture.h.beginHandshakeSegmentStream(emptyHeaderQ); status != "REFUSED" || duplicate != (handshakeSegmentStream{}) {
			t.Fatal("duplicate begin escaped active header ownership", status, duplicate)
		}
		if k5SegmentRetainedState(headerFixture.h) != headerBefore {
			t.Fatal("phase4 changed cursor, carry or frozen EMPTY Q")
		}
		releaseHeader()
		k5Wait(t, headerDone)
		if headerRetry.owner != headerFixture.h.fullObservation || headerRetry.checkpointID != emptyHeaderQ.checkpointID || headerRetry.streamAttemptID == 0 {
			t.Fatal("resumed header returned no real retry token")
		}
		k5SegmentAck(t, headerFixture.h, emptyHeaderReceipt, "RETIRED")
		if loan, status := headerFixture.h.borrowHandshakeSegmentChunk(headerRetry); status != "REFUSED" || loan != (handshakeSegmentLoan{}) {
			t.Fatal("retired header attempt borrowed old Q", status, loan)
		}
		nextHeaderQ := headerFixture.offer(t, 60*time.Second)
		nextHeaderBefore := k5SegmentRetainedState(headerFixture.h)
		if loan, status := headerFixture.h.borrowHandshakeSegmentChunk(headerRetry); status != "REFUSED" || loan != (handshakeSegmentLoan{}) || headerFixture.h.fullObservation.segment.loanPhase != 0 {
			t.Fatal("old header attempt acquired the next Q", status, loan)
		}
		k5SegmentAck(t, headerFixture.h, emptyHeaderReceipt, "ALREADY_RETIRED")
		if k5SegmentRetainedState(headerFixture.h) != nextHeaderBefore || nextHeaderQ.checkpointID == emptyHeaderQ.checkpointID {
			t.Fatal("old header/receipt changed later EMPTY Q")
		}
		_, nextHeaderReceipt := headerFixture.stream(t, nextHeaderQ)
		k5SegmentAck(t, headerFixture.h, nextHeaderReceipt, "RETIRED")
		headerFixture.d.set(70*time.Second, nil)
		headerRelease()
		// A separate idle retry has no lease: retirement must invalidate it.
		f.episode(t, 4*time.Second)
		q2 := f.offer(t, 6*time.Second)
		_, receipt2 := f.stream(t, q2)
		idle, _ := f.h.beginHandshakeSegmentStream(q2)
		first, _ := f.h.borrowHandshakeSegmentChunk(idle)
		if _, status := f.h.copyHandshakeSegmentChunk(first); status != "COPIED" {
			t.Fatal(status)
		}
		if !f.h.releaseHandshakeSegmentChunk(first) {
			t.Fatal("idle release")
		}
		k5SegmentAck(t, f.h, receipt2, "RETIRED")
		f.episode(t, 7*time.Second)
		if loan, status := f.h.borrowHandshakeSegmentChunk(idle); status != "REFUSED" || loan != (handshakeSegmentLoan{}) || f.h.fullObservation.segment.loanPhase != 0 {
			t.Fatal("idle attempt reached reused ring", status, loan)
		}
		q3 := f.offer(t, 9*time.Second)
		_, receipt3 := f.stream(t, q3)
		readStarted, readBlock, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseRead := sync.OnceFunc(func() { close(readBlock) })
		trackRead := k5OwnedCleanup(t, releaseRead, f.releaseAll)
		trackRead(readDone)
		f.d.mu.Lock()
		next := f.d.calls + 1
		f.d.mu.Unlock()
		f.d.after = func(n int) {
			if n == next {
				close(readStarted)
				<-readBlock
			}
		}
		f.d.set(10*time.Second, nil)
		go func() {
			defer close(readDone)
			r, err := f.h.enter(handshakeSource{})
			if err != nil {
				t.Error(err)
				return
			}
			r()
		}()
		k5Wait(t, readStarted)
		if status, reason := f.h.acknowledgeHandshakeSegment(receipt3); status != "DEFERRED" || reason != "read-or-completion" {
			t.Fatal("post-Q read ignored", status, reason)
		}
		releaseRead()
		k5Wait(t, readDone)
		if status, _ := f.h.retireVerifiedHandshakeSegment(); status != "RETIRED" {
			t.Fatal(status)
		}
		g := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
		g.episode(t, time.Second)
		gq := g.offer(t, 3*time.Second)
		_, gr := g.stream(t, gq)
		upperStarted, upperBlock, upperDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		releaseUpper := sync.OnceFunc(func() { close(upperBlock) })
		trackUpper := k5OwnedCleanup(t, releaseUpper, g.releaseAll)
		trackUpper(upperDone)
		g.d.mu.Lock()
		upperNext := g.d.calls + 2
		g.d.mu.Unlock()
		g.d.after = func(n int) {
			if n == upperNext {
				close(upperStarted)
				<-upperBlock
			}
		}
		g.d.set(4*time.Second, nil)
		go func() {
			defer close(upperDone)
			r, err := g.h.enter(handshakeSource{})
			if err != nil {
				t.Error(err)
				return
			}
			r()
		}()
		k5Wait(t, upperStarted)
		g.h.mu.Lock()
		pending := g.h.fullObservation.pending
		g.h.mu.Unlock()
		if pending == 0 {
			t.Fatal("no actual pending upper completion")
		}
		if status, reason := g.h.acknowledgeHandshakeSegment(gr); status != "DEFERRED" || reason != "read-or-completion" {
			t.Fatal("post-Q completion ignored", status, reason)
		}
		releaseUpper()
		k5Wait(t, upperDone)
		if status, _ := g.h.retireVerifiedHandshakeSegment(); status != "RETIRED" {
			t.Fatal(status)
		}
	})
}

func TestK5SegmentOwnerACKAuthority(t *testing.T) {
	for _, name := range []string{"foreign-source-epoch-session", "wrong-generation", "wrong-digest-carry-or-chain", "duplicate-and-stale-replay", "premature-before-stream-complete", "future-or-sparse-range", "partial-boundary"} {
		t.Run(name, func(t *testing.T) {
			f := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			if name == "premature-before-stream-complete" || name == "partial-boundary" {
				before := k5SegmentRetainedState(f.h)
				fake := f.issuer.sign(handshakeSegmentWitness{owner: f.h.fullObservation, session: f.issuer.session, descriptor: q})
				k5SegmentAck(t, f.h, fake, "REFUSED")
				a, _ := f.h.beginHandshakeSegmentStream(q)
				loan, _ := f.h.borrowHandshakeSegmentChunk(a)
				if f.h.releaseHandshakeSegmentChunk(loan) {
					t.Fatal("premature release skipped copy/hash")
				}
				chunk, status := f.h.copyHandshakeSegmentChunk(loan)
				if status != "COPIED" || chunk.final || len(chunk.rows) != 1 {
					t.Fatal("first C chunk", status, chunk)
				}
				if !f.h.releaseHandshakeSegmentChunk(loan) {
					t.Fatal("first chunk release")
				}
				k5SegmentAck(t, f.h, fake, "REFUSED")
				if k5SegmentRetainedState(f.h) != before {
					t.Fatal("partial stream retired prefix")
				}
				_, receipt := f.stream(t, q)
				k5SegmentAck(t, f.h, receipt, "RETIRED")
				return
			}
			_, receipt := f.stream(t, q)
			before := k5SegmentRetainedState(f.h)
			refuse := func(r handshakeSegmentReceipt) {
				t.Helper()
				k5SegmentAck(t, f.h, r, "REFUSED")
				k5SegmentAck(t, f.h, r, "REFUSED")
				if k5SegmentRetainedState(f.h) != before {
					t.Fatal("wrong ACK mutated retained source")
				}
			}
			switch name {
			case "foreign-source-epoch-session":
				other := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
				other.episode(t, time.Second)
				oq := other.offer(t, 3*time.Second)
				_, or := other.stream(t, oq)
				refuse(or)
				for _, mutate := range []func(*handshakeSegmentWitness){func(w *handshakeSegmentWitness) { w.descriptor.key.epoch[0]++ }, func(w *handshakeSegmentWitness) { w.session[0]++ }, func(w *handshakeSegmentWitness) { w.descriptor.bounds.node++ }, func(w *handshakeSegmentWitness) { w.descriptor.key.configuration[0]++ }} {
					r := receipt
					mutate(&r.witness)
					refuse(f.issuer.sign(r.witness))
				}
				badProof := receipt
				badProof.proof[0]++
				refuse(badProof)
			case "wrong-generation":
				r := receipt
				r.witness.descriptor.checkpointID++
				refuse(f.issuer.sign(r.witness))
				// Boundary faults only: no huge allocation, counter reset or
				// synthetic positive owner evidence.
				for _, fault := range []string{"invalid-M", "invalid-C", "size", "stream", "loan", "checkpoint", "generation", "absolute"} {
					g := newK5SegmentFixture(t, 2, 1, handshakeBounds{node: 1})
					if fault == "invalid-M" || fault == "invalid-C" || fault == "size" {
						m, c := 0, 1
						if fault == "invalid-C" {
							m, c = 2, 3
						}
						if fault == "size" {
							m, c = int(^uint(0)>>1), 1
						}
						clock := &k5TestClock{}
						h := newSegmentedInboundHandshakes(handshakeBounds{node: 1}, k5TestKey(1), m, c, clock.read, g.issuer, [16]byte{1})
						if clock.calls != 0 || len(h.fullObservation.rows) != 0 || h.fullObservation.segment == nil || h.fullSnapshot().cut.available {
							t.Fatal("invalid V2 fell back/allocated/sampled", fault)
						}
						r := k5Admit(t, h, handshakeSource{})
						r()
						if h.inFlight != 0 || h.fullObservation.invalid == "" {
							t.Fatal("invalid allocation changed gate")
						}
						continue
					}
					if fault == "absolute" {
						g.h.mu.Lock()
						g.h.fullObservation.sequence = k5MaxUint
						g.h.fullObservation.segment.retiredThrough = k5MaxUint
						g.h.mu.Unlock()
						gq := g.offer(t, time.Second)
						if gq.rowCount != 0 || gq.fromSeq != k5MaxUint || gq.throughSeq != k5MaxUint {
							t.Fatal("empty cursor wrapped")
						}
						_, gr := g.stream(t, gq)
						k5SegmentAck(t, g.h, gr, "RETIRED")
						r := g.admit(t)
						r()
						if g.h.fullObservation.sequence != k5MaxUint || g.h.fullObservation.invalid != "transition-overflow" {
							t.Fatal("sequence wrapped")
						}
						continue
					}
					if fault == "generation" {
						g.h.mu.Lock()
						g.h.fullObservation.segment.generations[0] = k5MaxUint
						g.h.mu.Unlock()
						r := g.admit(t)
						r()
						if g.h.fullObservation.invalid != "segment-slot-generation-overflow" {
							t.Fatal("generation wrapped")
						}
						continue
					}
					if fault == "checkpoint" {
						g.h.mu.Lock()
						g.h.fullObservation.segment.nextCheckpoint = k5MaxUint
						g.h.mu.Unlock()
						g.d.set(time.Second, nil)
						if _, status := g.h.sealHandshakeSegment(); status != "UNKNOWN" {
							t.Fatal("checkpoint wrapped")
						}
						continue
					}
					g.episode(t, time.Second)
					gq := g.offer(t, 3*time.Second)
					if fault == "stream" {
						g.h.fullObservation.segment.nextStream = k5MaxUint
						if _, status := g.h.beginHandshakeSegmentStream(gq); status != "UNKNOWN" {
							t.Fatal("stream wrapped")
						}
					}
					if fault == "loan" {
						a, _ := g.h.beginHandshakeSegmentStream(gq)
						g.h.fullObservation.segment.nextLoan = k5MaxUint
						if _, status := g.h.borrowHandshakeSegmentChunk(a); status != "UNKNOWN" {
							t.Fatal("loan wrapped")
						}
					}
				}
			case "wrong-digest-carry-or-chain":
				for _, mutate := range []func(*handshakeSegmentWitness){func(w *handshakeSegmentWitness) { w.digest[0]++ }, func(w *handshakeSegmentWitness) { w.descriptor.fromCarry.full = true }, func(w *handshakeSegmentWitness) { w.descriptor.throughCarry.entry.upper++ }, func(w *handshakeSegmentWitness) { w.descriptor.previousID++ }, func(w *handshakeSegmentWitness) { w.descriptor.previousDigest[0]++ }} {
					r := receipt
					mutate(&r.witness)
					refuse(f.issuer.sign(r.witness))
				}
			case "duplicate-and-stale-replay":
				k5SegmentAck(t, f.h, receipt, "RETIRED")
				k5SegmentAck(t, f.h, receipt, "ALREADY_RETIRED")
				wrongReplay := receipt
				wrongReplay.proof[0]++
				k5SegmentAck(t, f.h, wrongReplay, "REFUSED")
				q2 := f.offer(t, 4*time.Second)
				_, r2 := f.stream(t, q2)
				k5SegmentAck(t, f.h, receipt, "ALREADY_RETIRED")
				if !f.h.fullObservation.segment.qActive {
					t.Fatal("old ACK retired later EMPTY")
				}
				k5SegmentAck(t, f.h, r2, "RETIRED")
				q3 := f.offer(t, 5*time.Second)
				_, r3 := f.stream(t, q3)
				before = k5SegmentRetainedState(f.h)
				refuse(receipt)
				k5SegmentAck(t, f.h, r2, "ALREADY_RETIRED")
				if k5SegmentRetainedState(f.h) != before {
					t.Fatal("same rowcursor replay advanced time")
				}
				k5SegmentAck(t, f.h, r3, "RETIRED")
				// Real verifier interleaving: a late verified result must not
				// attach to a newer Q after another caller retires this one.
				q4 := f.offer(t, 6*time.Second)
				_, r4 := f.stream(t, q4)
				started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(block) })
				track := k5OwnedCleanup(t, unblock, f.releaseAll)
				track(done)
				call := f.issuer.calls.Load() + 1
				f.issuer.after = func(n int64) {
					if n == call {
						close(started)
						<-block
					}
				}
				go func() {
					defer close(done)
					status, _ := f.h.acknowledgeHandshakeSegment(r4)
					if status != "ALREADY_RETIRED" && status != "REFUSED" {
						t.Error("late verification", status)
					}
				}()
				k5Wait(t, started)
				k5SegmentAck(t, f.h, r4, "RETIRED")
				q5 := f.offer(t, 7*time.Second)
				_, r5 := f.stream(t, q5)
				unblock()
				k5Wait(t, done)
				if !f.h.fullObservation.segment.qActive || f.h.fullObservation.segment.q != q5 {
					t.Fatal("late verifier retired Q5")
				}
				k5SegmentAck(t, f.h, r5, "RETIRED")
				return
			case "future-or-sparse-range":
				for _, mutate := range []func(*handshakeSegmentWitness){func(w *handshakeSegmentWitness) { w.descriptor.throughSeq++ }, func(w *handshakeSegmentWitness) { w.descriptor.fromSeq++ }, func(w *handshakeSegmentWitness) { w.descriptor.rowCount-- }} {
					r := receipt
					mutate(&r.witness)
					refuse(f.issuer.sign(r.witness))
				}
				a, _ := f.h.beginHandshakeSegmentStream(q)
				bad := a
				bad.streamAttemptID++
				if _, status := f.h.borrowHandshakeSegmentChunk(bad); status != "REFUSED" {
					t.Fatal("foreign attempt")
				}
				loan, _ := f.h.borrowHandshakeSegmentChunk(a)
				sparse := loan
				sparse.offset++
				if _, status := f.h.copyHandshakeSegmentChunk(sparse); status != "REFUSED" {
					t.Fatal("sparse copy")
				}
				if f.h.releaseHandshakeSegmentChunk(sparse) {
					t.Fatal("sparse release")
				}
				if _, status := f.h.copyHandshakeSegmentChunk(loan); status != "COPIED" {
					t.Fatal(status)
				}
				if !f.h.releaseHandshakeSegmentChunk(loan) {
					t.Fatal("release")
				}
			}
			k5SegmentAck(t, f.h, receipt, "RETIRED")
		})
	}
}

func TestK5SegmentOwnerPersistenceLoss(t *testing.T) {
	t.Run("partial-stream-hash-reset-and-lost-commit", func(t *testing.T) {
		f := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		rows, receipt := f.stream(t, q)
		before := k5SegmentRetainedState(f.h)
		a, _ := f.h.beginHandshakeSegmentStream(q)
		loan, _ := f.h.borrowHandshakeSegmentChunk(a)
		if _, status := f.h.copyHandshakeSegmentChunk(loan); status != "COPIED" {
			t.Fatal(status)
		}
		if !f.h.releaseHandshakeSegmentChunk(loan) {
			t.Fatal("prefix release")
		}
		if f.h.fullObservation.segment.streamOffset != 1 {
			t.Fatal("fixture has no nonzero prefix")
		}
		retried, retryReceipt := f.stream(t, q)
		if !reflect.DeepEqual(rows, retried) || receipt != retryReceipt {
			t.Fatal("partial retry did not reset offset/hash or changed immutable Q digest")
		}
		if _, status := f.h.borrowHandshakeSegmentChunk(a); status != "REFUSED" {
			t.Fatal("superseded attempt still active")
		}
		f.issuer.accept.Store(false)
		k5SegmentAck(t, f.h, receipt, "REFUSED")
		if k5SegmentRetainedState(f.h) != before {
			t.Fatal("uncertain commit retired")
		}
		f.d.set(100*time.Second, nil)
		same, status := f.h.sealHandshakeSegment()
		if status != "OFFERED" || same != q {
			t.Fatal("lost response replaced Q")
		}
		f.issuer.accept.Store(true)
		k5SegmentAck(t, f.h, receipt, "RETIRED")
		// Negative content faults are Q-specific, even with a known digest
		// and a valid receipt already verified/deferred before the bad copy.
		for _, fault := range []string{"sequence", "done", "valid", "digest"} {
			g := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
			g.episode(t, time.Second)
			gq := g.offer(t, 3*time.Second)
			_, gr := g.stream(t, gq)
			state := k5SegmentRetainedState(g.h)
			ga, _ := g.h.beginHandshakeSegmentStream(gq)
			gl, _ := g.h.borrowHandshakeSegmentChunk(ga)
			if status, reason := g.h.acknowledgeHandshakeSegment(gr); status != "DEFERRED" || reason != "copy-or-staging-loan" {
				t.Fatal("missing verified pending premise", status, reason)
			}
			g.h.mu.Lock()
			goSource, gs := g.h.fullObservation, g.h.fullObservation.segment
			cell := &goSource.rows[gs.head]
			switch fault {
			case "sequence":
				cell.sequence++
			case "done":
				cell.done = false
			case "valid":
				cell.valid = false
			case "digest":
				cell.count++
			}
			g.h.mu.Unlock()
			_, copyStatus := g.h.copyHandshakeSegmentChunk(gl)
			if fault == "digest" {
				if copyStatus != "COPIED" || !g.h.releaseHandshakeSegmentChunk(gl) {
					t.Fatal("digest prefix setup")
				}
				gl, _ = g.h.borrowHandshakeSegmentChunk(ga)
				_, copyStatus = g.h.copyHandshakeSegmentChunk(gl)
			}
			if copyStatus != "UNKNOWN" {
				t.Fatal("Q corruption not detected", fault, copyStatus)
			}
			offset := g.h.fullObservation.segment.streamOffset
			if !g.h.releaseHandshakeSegmentChunk(gl) {
				t.Fatal("UNKNOWN loan cleanup")
			}
			gs = g.h.fullObservation.segment
			if gs.loanPhase != 0 || gs.streamOffset != offset || gs.streamFinished || gs.qIntegrityFault == "" {
				t.Fatal("UNKNOWN cleanup advanced stream", fault)
			}
			if _, known := g.h.expectedHandshakeSegmentWitness(); known {
				t.Fatal("bad Q still offers witness")
			}
			if _, status := g.h.beginHandshakeSegmentStream(gq); status != "REFUSED" {
				t.Fatal("bad Q restarted", status)
			}
			if _, status := g.h.borrowHandshakeSegmentChunk(ga); status != "REFUSED" {
				t.Fatal("bad Q borrowed", status)
			}
			k5SegmentAck(t, g.h, gr, "REFUSED")
			if status, _ := g.h.retireVerifiedHandshakeSegment(); status != "REFUSED" {
				t.Fatal("bad Q retired via pending old receipt", status)
			}
			if k5SegmentRetainedState(g.h) != state {
				t.Fatal("bad Q cursor/storage released")
			}
		}
		// EMPTY has an independent temporal cursor despite equal row cursor.
		empty := f.offer(t, 4*time.Second)
		_, er := f.stream(t, empty)
		before = k5SegmentRetainedState(f.h)
		f.issuer.accept.Store(false)
		k5SegmentAck(t, f.h, er, "REFUSED")
		f.d.set(200*time.Second, nil)
		same, status = f.h.sealHandshakeSegment()
		if status != "OFFERED" || same != empty || k5SegmentRetainedState(f.h) != before {
			t.Fatal("uncertain EMPTY advanced or replaced time")
		}
		_, retryEmpty := f.stream(t, empty)
		if retryEmpty != er || empty.fromSeq != empty.throughSeq {
			t.Fatal("EMPTY retry identity")
		}
		f.issuer.accept.Store(true)
		k5SegmentAck(t, f.h, er, "RETIRED")
		// Error in an original owner clock still releases its original gate.
		f.d.set(5*time.Second, nil)
		r := f.admit(t)
		goodQ := f.offer(t, 6*time.Second)
		_, goodReceipt := f.stream(t, goodQ)
		f.d.set(7*time.Second, errors.New("test-clock-failure"))
		r()
		r()
		if f.h.inFlight != 0 || f.h.fullObservation.invalid != "clock-error" {
			t.Fatal("clock error changed real release")
		}
		k5SegmentAck(t, f.h, goodReceipt, "RETIRED")
		f.d.set(8*time.Second, nil)
		if _, status := f.h.sealHandshakeSegment(); status != "UNKNOWN" {
			t.Fatal("old good-Q retirement cleared later clock gap")
		}
	})
	t.Run("no-ack-capacity-gap", func(t *testing.T) {
		f := newK5SegmentFixture(t, 2, 1, handshakeBounds{node: 1})
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		rows, receipt := f.stream(t, q)
		before := k5SegmentRetainedState(f.h)
		f.d.set(4*time.Second, nil)
		r := f.admit(t)
		r()
		r()
		if f.h.inFlight != 0 || f.h.fullObservation.invalid != "transition-capacity-gap" || f.h.fullObservation.used != before.used || f.h.fullObservation.sequence != before.sequence {
			t.Fatal("capacity gap grew/overwrote or blocked admission")
		}
		same, status := f.h.sealHandshakeSegment()
		if status != "OFFERED" || same != q {
			t.Fatal("lost confirmed Q")
		}
		retried, retry := f.stream(t, q)
		if !reflect.DeepEqual(rows, retried) || receipt != retry {
			t.Fatal("gap overwrote retained Q")
		}
		k5SegmentAck(t, f.h, receipt, "RETIRED")
		f.d.set(5*time.Second, nil)
		if _, status := f.h.sealHandshakeSegment(); status != "UNKNOWN" {
			t.Fatal("ACK cleared sticky evidence gap", status)
		}
		if s := f.h.fullSnapshot(); s.cut.available || !s.pastOnly {
			t.Fatal("legacy fabricated coverage after gap", s)
		}
		for n := 0; n < 4; n++ {
			r := f.admit(t)
			r()
		}
		if len(f.h.fullObservation.rows) != 2 || len(f.h.fullObservation.segment.staging) != 1 || f.h.inFlight != 0 {
			t.Fatal("gap changed storage/gate")
		}
	})
}

// Synthetic already-qualified UTC/load certificates only. Mapping duration
// coordinates directly is a fixture assumption, never production UTC proof.
func k5SegmentMapped(key k5SourceKey, ranges []handshakeFullRange) k5MappedFull {
	f := k5MappedFull{key: key, mappingReference: "qualified-test-mapping", mappingValid: true, upperValid: true}
	for _, r := range ranges {
		f.definite = append(f.definite, k5TestSpan(r.definiteStart, r.definiteEnd))
		f.possible = append(f.possible, k5TestSpan(r.possibleStart, r.possibleEnd))
	}
	return f
}
func TestK5SegmentOwnerReconstructionUTC(t *testing.T) {
	t.Run("midnight-open-carry-and-empty-checkpoint", func(t *testing.T) {
		f := newK5SegmentFixture(t, 4, 1, handshakeBounds{node: 1})
		f.d.set(235*time.Hour/10, nil)
		release := f.admit(t)
		q1 := f.offer(t, 24*time.Hour)
		rows1, r1 := f.stream(t, q1)
		ranges := k5SegmentRanges(t, q1, rows1)
		k5SegmentAck(t, f.h, r1, "RETIRED")
		q2 := f.offer(t, 97*time.Hour/4)
		rows2, r2 := f.stream(t, q2)
		ranges = append(ranges, k5SegmentRanges(t, q2, rows2)...)
		k5SegmentAck(t, f.h, r2, "RETIRED")
		f.d.set(245*time.Hour/10, nil)
		release()
		q3 := f.offer(t, 25*time.Hour)
		rows3, r3 := f.stream(t, q3)
		ranges = append(ranges, k5SegmentRanges(t, q3, rows3)...)
		k5SegmentAck(t, f.h, r3, "RETIRED")
		if q2.rowCount != 0 || q2.fromCarry != q1.throughCarry || q3.fromCarry != q1.throughCarry {
			t.Fatal("open carry lost original entry")
		}
		f.d.set(25*time.Hour, nil)
		if _, status := f.h.sealHandshakeSegment(); status != "NO_PROGRESS" {
			t.Fatal(status)
		}
		allRows := append(append([]handshakeFullTransition(nil), rows1...), rows3...)
		oracle, ok := handshakeFullRanges(handshakeFullSnapshot{bounds: q1.bounds, coverageStart: q1.coverageStart, cut: handshakeFullCut{available: true, rows: 2, sequence: 2, at: q3.throughAt}, rows: allRows})
		if !ok {
			t.Fatal("whole-prefix fixture invalid")
		}
		actual := k5SegmentMapped(q1.key, ranges)
		reference := k5SegmentMapped(q1.key, oracle)
		load := k5TestLoad(q1.key, 23*time.Hour, 26*time.Hour)
		for _, day := range []int64{0, k5DayNanos} {
			got := reduceK5UTCday(q1.key.anchor, q1.key.configuration, day, []k5MappedFull{actual, actual}, []k5ActualLoad{load, load}, k5ReduceLimits{8, 8, 64}, true, "qualified-test-day")
			want := reduceK5UTCday(q1.key.anchor, q1.key.configuration, day, []k5MappedFull{reference}, []k5ActualLoad{load}, k5ReduceLimits{8, 8, 64}, true, "qualified-test-day")
			if got != want || got.lower != 30*time.Minute {
				t.Fatal("chunk/receipt duplicate or midnight pooling", got, want)
			}
		}
	})
	t.Run("strict-hour-and-uncertain-bounds", func(t *testing.T) {
		for _, duration := range []time.Duration{time.Hour, time.Hour + time.Nanosecond} {
			f := newK5SegmentFixture(t, 2, 1, handshakeBounds{node: 1})
			release := f.admit(t)
			f.d.set(duration, nil)
			release()
			q := f.offer(t, duration)
			rows, receipt := f.stream(t, q)
			mapped := k5SegmentMapped(q.key, k5SegmentRanges(t, q, rows))
			k5SegmentAck(t, f.h, receipt, "RETIRED")
			got := k5Assess([]k5MappedFull{mapped, mapped}, []k5ActualLoad{k5TestLoad(q.key, 0, 2*time.Hour)}, true)
			want := "NO_P_TRIGGER_IN_SCOPE"
			if duration > time.Hour {
				want = "P_INVESTIGATE"
			}
			if got.verdict != want || got.lower != duration {
				t.Fatal("strict threshold", duration, got)
			}
		}
		// Actual delayed lower/upper provider values produce uncertainty.
		f := newK5SegmentFixture(t, 2, 1, handshakeBounds{node: 1})
		f.d.after = func(n int) {
			if n == 2 {
				f.d.set(200*time.Second, nil)
			}
			if n == 4 {
				f.d.set(3700*time.Second, nil)
			}
		}
		release := f.admit(t)
		f.d.set(3500*time.Second, nil)
		release()
		q := f.offer(t, 3700*time.Second)
		rows, receipt := f.stream(t, q)
		mapped := k5SegmentMapped(q.key, k5SegmentRanges(t, q, rows))
		k5SegmentAck(t, f.h, receipt, "RETIRED")
		got := k5Assess([]k5MappedFull{mapped}, []k5ActualLoad{k5TestLoad(q.key, 0, 4000*time.Second)}, true)
		if got.verdict != "UNKNOWN" || got.lower != 3300*time.Second || got.upper != 3700*time.Second {
			t.Fatal("uncertainty became point estimate", got, rows)
		}
	})
	t.Run("missing-or-unqualified-input-and-legacy-refusal", func(t *testing.T) {
		f := newK5SegmentFixture(t, 2, 1, handshakeBounds{node: 1})
		release := f.admit(t)
		f.d.set(time.Hour, nil)
		release()
		q := f.offer(t, time.Hour)
		rows, receipt := f.stream(t, q)
		fold, ok := newHandshakeSegmentFold(q)
		if !ok {
			t.Fatal("descriptor")
		}
		_, _, _ = fold.row(rows[0])
		if _, _, ok := fold.finish(); ok {
			t.Fatal("missing chunk closed checkpoint")
		}
		reordered, _ := newHandshakeSegmentFold(q)
		if _, _, ok := reordered.row(rows[1]); ok {
			t.Fatal("reordered rows accepted")
		}
		// Negative fault only: a new row cannot predate the committed cut.
		// An original carry entry may predate it, but new provider reads may not.
		impossible := q
		impossible.fromAt = 40 * time.Second
		impossible.throughAt = 80 * time.Second
		impossible.fromSeq = 10
		impossible.throughSeq = 12
		impossible.rowCount = 2
		impossible.fromCarry = handshakeSegmentCarry{}
		impossible.throughCarry = handshakeSegmentCarry{}
		badFold, ok := newHandshakeSegmentFold(impossible)
		if !ok {
			t.Fatal("negative descriptor fixture")
		}
		if _, _, ok := badFold.row(handshakeFullTransition{sequence: 11, full: true, count: 1, lower: 10 * time.Second, upper: 30 * time.Second, done: true, valid: true}); ok {
			t.Fatal("clipping hid a new row before committed cut")
		}
		mapped := k5SegmentMapped(q.key, k5SegmentRanges(t, q, rows))
		load := k5TestLoad(q.key, 0, 2*time.Hour)
		if got := k5Assess([]k5MappedFull{mapped}, []k5ActualLoad{load}, false); got.verdict != "UNKNOWN" || got.lower != time.Hour {
			t.Fatal("missing receipt chain fabricated full coverage", got)
		}
		badMap := mapped
		badMap.mappingValid = false
		if got := k5Assess([]k5MappedFull{badMap}, []k5ActualLoad{load}, true); got.verdict != "UNKNOWN" || got.lower != 0 {
			t.Fatal("unqualified map", got)
		}
		load.actual = false
		if got := k5Assess([]k5MappedFull{mapped}, []k5ActualLoad{load}, true); got.verdict != "UNKNOWN" || got.lower != 0 {
			t.Fatal("scheduled load", got)
		}
		load = k5TestLoad(q.key, 0, 2*time.Hour)
		load.permitted = false
		if got := k5Assess([]k5MappedFull{mapped}, []k5ActualLoad{load}, true); got.verdict != "UNKNOWN" {
			t.Fatal("identity became permission", got)
		}
		for _, afterRetire := range []bool{false, true} {
			if afterRetire {
				k5SegmentAck(t, f.h, receipt, "RETIRED")
			}
			legacy := f.h.fullSnapshot()
			if legacy.status != "UNKNOWN" || legacy.cut.available || legacy.rows != nil || !legacy.pastOnly {
				t.Fatal("legacy false prefix", legacy)
			}
		}
		other := mapped
		other.key.kind = k5ConnectionBudgetTotal
		if got := k5Assess([]k5MappedFull{other}, []k5ActualLoad{k5TestLoad(q.key, 0, 2*time.Hour)}, true); got.verdict != "UNKNOWN" {
			t.Fatal("silent cross-gate union")
		}
	})
}
