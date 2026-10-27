package connbudget

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// CONDITIONAL TEST ISSUER ONLY: opaque proofs assume verified persistence.
// No disk write, fsync, recovery or production durability is exercised.
type totalSegmentTestIssuer struct {
	mu          sync.Mutex
	verifyCalls uint64
	owner       *totalFullObservation
	session     [16]byte
	secret      [32]byte
	accept      atomic.Bool
	after       func()
}

func (i *totalSegmentTestIssuer) proof(w totalSegmentWitness) [32]byte {
	h := hmac.New(sha256.New, i.secret[:])
	writeTotalSegmentHeader(h, w.descriptor)
	_, _ = h.Write(w.session[:])
	_, _ = h.Write(w.digest[:])
	var result [32]byte
	h.Sum(result[:0])
	return result
}
func (i *totalSegmentTestIssuer) sign(w totalSegmentWitness) totalSegmentReceipt {
	return totalSegmentReceipt{witness: w, proof: i.proof(w)}
}
func (i *totalSegmentTestIssuer) verifyCallCount() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.verifyCalls
}
func (i *totalSegmentTestIssuer) verifyTotalSegment(r totalSegmentReceipt) (totalSegmentCommit, bool) {
	i.mu.Lock()
	i.verifyCalls++
	i.mu.Unlock() // no callback/hash/barrier while the TEST issuer mutex is held
	if i.after != nil {
		i.after()
	}
	if !i.accept.Load() || r.witness.owner != i.owner || r.witness.session != i.session || r.proof != i.proof(r.witness) {
		return totalSegmentCommit{}, false
	}
	return totalSegmentCommit{witness: r.witness}, true
}

// Pause the real source SHA Write outside Budget.mu, with no production hook.
type totalSegmentPausedHash struct {
	hash.Hash
	armed   atomic.Bool
	once    sync.Once
	started chan struct{}
	resume  <-chan struct{}
}

func (h *totalSegmentPausedHash) Write(b []byte) (int, error) {
	if h.armed.Load() {
		h.once.Do(func() { close(h.started); <-h.resume })
	}
	return h.Hash.Write(b)
}

type totalSegmentFixture struct {
	b            *Budget
	d            *totalTestClock
	issuer       *totalSegmentTestIssuer
	reservations []*Reservation
}

func newTotalSegmentFixture(t *testing.T, cfg Config, capacity, chunkCapacity int) *totalSegmentFixture {
	t.Helper()
	f := &totalSegmentFixture{d: &totalTestClock{}}
	f.issuer = &totalSegmentTestIssuer{session: [16]byte{9}, secret: [32]byte{71}}
	f.issuer.accept.Store(true)
	var err error
	f.b, err = newSegmentedBudget(cfg, totalKey(51), capacity, chunkCapacity, f.d.read, f.issuer, f.issuer.session)
	if err != nil {
		t.Fatal(err)
	}
	f.issuer.owner = f.b.fullObservation
	t.Cleanup(f.releaseAll)
	return f
}
func (f *totalSegmentFixture) releaseAll() {
	for _, r := range f.reservations {
		r.Release()
	}
}
func (f *totalSegmentFixture) take(t *testing.T, dir Direction) *Reservation {
	t.Helper()
	r := totalTake(t, f.b, dir)
	f.reservations = append(f.reservations, r)
	return r
}
func (f *totalSegmentFixture) episode(t *testing.T, at time.Duration) {
	t.Helper()
	f.d.set(at, nil)
	r := f.take(t, DirectionOutbound)
	f.d.set(at+time.Second, nil)
	r.Release()
}
func (f *totalSegmentFixture) offer(t *testing.T, at time.Duration) totalSegmentDescriptor {
	t.Helper()
	f.d.set(at, nil)
	q, status := f.b.sealTotalSegment()
	if status != "OFFERED" {
		t.Fatal("actual Budget Seal", status, q)
	}
	return q
}
func (f *totalSegmentFixture) stream(t *testing.T, q totalSegmentDescriptor) ([]totalFullTransition, totalSegmentReceipt) {
	t.Helper()
	a, status := f.b.beginTotalSegmentStream(q)
	if status != "STREAMING" {
		t.Fatal("Begin", status)
	}
	var rows []totalFullTransition
	for {
		loan, status := f.b.borrowTotalSegmentChunk(a)
		if status != "BORROWED" {
			t.Fatal("Borrow", status)
		}
		chunk, status := f.b.copyTotalSegmentChunk(loan)
		if status != "COPIED" || chunk.descriptor != q || chunk.offset != len(rows) {
			t.Fatal("Copy", status, chunk)
		}
		rows = append(rows, chunk.rows...) // finite TEST oracle copied BEFORE Release
		if !f.b.releaseTotalSegmentChunk(loan) {
			t.Fatal("Release")
		}
		if chunk.final {
			break
		}
	}
	w, known := f.b.expectedTotalSegmentWitness()
	if !known || w.descriptor != q {
		t.Fatal("whole-Q digest absent")
	}
	return rows, f.issuer.sign(w)
}
func totalSegmentAck(t *testing.T, b *Budget, r totalSegmentReceipt, want string) {
	t.Helper()
	status, reason := b.acknowledgeTotalSegment(r)
	if status != want {
		t.Fatal("ACK", status, reason, "want", want)
	}
}
func totalSegmentLeafFree(t *testing.T, b *Budget) {
	t.Helper()
	if !b.mu.TryLock() {
		t.Fatal("paused outside-leaf work holds Budget.mu")
	}
	b.mu.Unlock()
}
func totalSegmentMeasures(t *testing.T, q totalSegmentDescriptor, rows []totalFullTransition) (time.Duration, time.Duration) {
	t.Helper()
	f, ok := newTotalSegmentFold(q)
	if !ok {
		t.Fatal("invalid fold descriptor", q)
	}
	var lower, upper time.Duration
	add := func(r totalSegmentRange) {
		lower += r.definiteEnd - r.definiteStart
		upper += r.possibleEnd - r.possibleStart
	}
	for _, row := range rows {
		r, emitted, ok := f.row(row)
		if !ok {
			t.Fatal("invalid actual owner row", row)
		}
		if emitted {
			add(r)
		}
	}
	r, emitted, ok := f.finish()
	if !ok {
		t.Fatal("incomplete fold", q, rows)
	}
	if emitted {
		add(r)
	}
	return lower, upper
}
func totalSegmentLegacyRefused(t *testing.T, b *Budget) {
	t.Helper()
	s := b.fullSnapshot()
	if s.status != "UNKNOWN" || s.reason != "segmented-source-requires-v2" || !s.pastOnly || s.cut.available || s.rows != nil {
		t.Fatal("legacy V2 suffix/copy exposed", s)
	}
	e := (&TotalObservation{budget: b}).Snapshot()
	if e.Status != "UNKNOWN" || e.Cut.Available || e.Rows != nil || !e.PastOnly {
		t.Fatal("legacy adapter exposed V2 prefix", e)
	}
}

func TestTotalSegmentActualOwner(t *testing.T) {
	t.Run("original-reservations-cas-and-refusals", func(t *testing.T) {
		cfg := Config{Total: 3, OutboundReserve: 1, MaxOutbound: 3, MaxInbound: 2, MaxAuxiliary: 2}
		f := newTotalSegmentFixture(t, cfg, 4, 1)
		a := f.take(t, DirectionOutbound)
		c := f.take(t, DirectionInbound)
		f.d.set(10*time.Second, nil)
		r := f.take(t, DirectionAuxiliary)
		if _, err := f.b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
			t.Fatal("total authority", err)
		}
		f.d.set(30*time.Second, nil)
		r.Release() // real rollback of the SAME successful CAS token
		calls := f.d.count()
		r.Release()
		var nilReservation *Reservation
		nilReservation.Release()
		if f.d.count() != calls {
			t.Fatal("duplicate/nil Release clock read")
		}
		q := f.offer(t, 40*time.Second)
		rows, receipt := f.stream(t, q)
		if q.cfg != cfg || len(rows) != 2 || !rows[0].full || rows[0].count != 3 || rows[1].full || rows[1].count != 2 {
			t.Fatal("actual transitions/config", q, rows)
		}
		lo, hi := totalSegmentMeasures(t, q, rows)
		if lo != 20*time.Second || hi != lo {
			t.Fatal("actual occupancy", lo, hi)
		}
		totalSegmentAck(t, f.b, receipt, "RETIRED")
		a.Release()
		c.Release()
		if f.b.Snapshot().Used != 0 {
			t.Fatal("counters not released")
		}
		g := newTotalSegmentFixture(t, Config{Total: 4, OutboundReserve: 2, MaxInbound: 1, MaxAuxiliary: 2}, 2, 1)
		g.take(t, DirectionInbound)
		if _, err := g.b.Reserve(DirectionInbound); !errors.Is(err, ErrDirectionLimit) {
			t.Fatal("direction precedence", err)
		}
		g.take(t, DirectionAuxiliary)
		if _, err := g.b.Reserve(DirectionAuxiliary); !errors.Is(err, ErrOutboundReserved) {
			t.Fatal("protected spare total", err)
		}
		if g.b.Snapshot().Used != 2 || g.b.fullObservation.sequence != 0 {
			t.Fatal("spare-total refusal fabricated fullness")
		}
		before := g.d.count()
		if _, err := g.b.Reserve(Direction(99)); !errors.Is(err, ErrUnknownDirection) {
			t.Fatal(err)
		}
		if g.d.count() != before {
			t.Fatal("invalid direction entered observer")
		}
	})
	t.Run("small-ring-real-turnover-and-stable-buffers", func(t *testing.T) {
		f := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		o, s := f.b.fullObservation, f.b.fullObservation.segment
		rowBase, stageBase := &o.rows[0], &s.staging[0]
		for n := 0; n < 4; n++ {
			f.episode(t, time.Duration(10*n+1)*time.Second)
			q := f.offer(t, time.Duration(10*n+4)*time.Second)
			rows, r := f.stream(t, q)
			if len(rows) != 2 || q.fromSeq != uint64(2*n) || q.throughSeq != uint64(2*n+2) {
				t.Fatal("absolute sequence", q, rows)
			}
			totalSegmentLegacyRefused(t, f.b)
			totalSegmentAck(t, f.b, r, "RETIRED")
			totalSegmentAck(t, f.b, r, "ALREADY_RETIRED")
			if o.used != 0 || o.sequence != uint64(2*n+2) || s.retiredThrough != o.sequence || s.generations[0] != uint64(n+1) || s.generations[1] != uint64(n+1) {
				t.Fatal("ring reuse/reset", o.sequence, s.generations)
			}
			if &o.rows[0] != rowBase || &s.staging[0] != stageBase || len(o.rows) != 2 || len(s.generations) != 2 || len(s.staging) != 1 || o.start != 0 {
				t.Fatal("storage/origin replaced")
			}
		}
		totalSegmentLegacyRefused(t, f.b)
		// Deliberately forged OLD completion is negative authority input. Reused
		// cell bytes/generation and pending bookkeeping may not be overwritten.
		f.b.mu.Lock()
		beforeRow, beforePending := o.rows[0], o.pending
		stale := totalFullCompletion{owner: o, index: 0, sequence: 1, generation: 1, valid: true, registered: true}
		f.b.completeFullRowLocked(&stale, 40*time.Second, true)
		unchanged := o.rows[0] == beforeRow && o.pending == beforePending && s.generations[0] == 4
		f.b.mu.Unlock()
		if !unchanged {
			t.Fatal("stale completion wrote a reused slot")
		}
	})
	t.Run("provider-overlap-and-reversed-completions", func(t *testing.T) {
		f := newTotalSegmentFixture(t, Config{Total: 2}, 2, 1)
		anchor := f.take(t, DirectionOutbound) // init1, non-full read2
		entryStarted, exitStarted, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
		entryResume, exitResume, hold := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var entryOnce, exitOnce, holdOnce sync.Once
		join := totalCleanup(t, func() { entryOnce.Do(func() { close(entryResume) }) }, func() { exitOnce.Do(func() { close(exitResume) }) }, func() { holdOnce.Do(func() { close(hold) }) }, f.releaseAll)
		f.d.after = func(n int) {
			switch n {
			case 3:
				f.d.set(30*time.Second, nil)
			case 4:
				close(entryStarted)
				<-entryResume
			case 5:
				f.d.set(25*time.Second, nil)
			case 6:
				close(exitStarted)
				<-exitResume
			}
		}
		f.d.set(10*time.Second, nil)
		entryDone := make(chan struct{})
		join(entryDone)
		go func() {
			defer close(entryDone)
			r, err := f.b.Reserve(DirectionInbound)
			if err != nil {
				t.Error(err)
				return
			}
			defer r.Release()
			close(published)
			<-hold
		}()
		totalWait(t, entryStarted)
		totalSegmentLeafFree(t, f.b)
		if _, status := f.b.sealTotalSegment(); status != "IN_FLIGHT" {
			t.Fatal("cut with pending entry", status)
		}
		f.d.set(20*time.Second, nil)
		exitDone := make(chan struct{})
		join(exitDone)
		go func() { defer close(exitDone); anchor.Release() }()
		totalWait(t, exitStarted)
		totalSegmentLeafFree(t, f.b)
		// Both post tokens are registered before upper30 raises maximum. Real row1
		// completes first, row2 upper25 later, valid against its own captured floor20.
		entryOnce.Do(func() { close(entryResume) })
		totalWait(t, published)
		if f.b.Snapshot().Used != 1 {
			t.Fatal("real exit did not own count mutation")
		}
		exitOnce.Do(func() { close(exitResume) })
		totalWait(t, exitDone)
		q := f.offer(t, 40*time.Second)
		rows, r := f.stream(t, q)
		if len(rows) != 2 || rows[0].lower != 10*time.Second || rows[0].upper != 30*time.Second || rows[1].lower != 20*time.Second || rows[1].upper != 25*time.Second {
			t.Fatal("completion order replaced mutation order", rows)
		}
		lo, hi := totalSegmentMeasures(t, q, rows)
		v1 := totalFullSnapshot{cfg: q.cfg, coverageStart: q.coverageStart, cut: totalFullCut{available: true, rows: 2, sequence: 2, at: q.throughAt}, rows: rows}
		vlo, vhi, ok := totalFullDurationBounds(v1)
		if !ok || lo != 0 || hi != 15*time.Second || lo != vlo || hi != vhi {
			t.Fatal("C1 reversal/V1 history", lo, hi, vlo, vhi, ok)
		}
		totalSegmentAck(t, f.b, r, "RETIRED")
		f.d.set(50*time.Second, nil)
		holdOnce.Do(func() { close(hold) })
		totalWait(t, entryDone)

		g := newTotalSegmentFixture(t, Config{Total: 1, MaxOutbound: 1}, 2, 1)
		g.take(t, DirectionOutbound)
		started, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		joinG := totalCleanup(t, func() { once.Do(func() { close(resume) }) }, g.releaseAll)
		joinG(done)
		sealRead := g.d.count() + 1
		g.d.after = func(n int) {
			if n == sealRead {
				close(started)
				<-resume
			}
		}
		g.d.set(50*time.Second, nil)
		statusCh := make(chan string, 1)
		go func() { defer close(done); _, status := g.b.sealTotalSegment(); statusCh <- status }()
		totalWait(t, started)
		totalSegmentLeafFree(t, g.b)
		g.b.mu.Lock()
		seq, full := g.b.fullObservation.sequence, g.b.fullObservation.full
		g.b.mu.Unlock()
		if _, err := g.b.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
			t.Fatal("real refused provider read", err)
		}
		g.b.mu.Lock()
		o := g.b.fullObservation
		restored := o.reads == 1 && o.pending == 0 && o.sequence == seq && o.full == full
		g.b.mu.Unlock()
		if !restored {
			t.Fatal("scalars did not return to previous values")
		}
		once.Do(func() { close(resume) })
		totalWait(t, done)
		if status := <-statusCh; status != "UNSTABLE_CUT" {
			t.Fatal("transient overlap certified", status)
		}
		g.offer(t, 60*time.Second)

		k := newTotalSegmentFixture(t, Config{Total: 1}, 4, 1)
		k.episode(t, time.Second)
		qk := k.offer(t, 5*time.Second)
		_, rk := k.stream(t, qk)
		preStarted, preResume, preDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var preOnce sync.Once
		joinK := totalCleanup(t, func() { preOnce.Do(func() { close(preResume) }) }, k.releaseAll)
		joinK(preDone)
		preRead := k.d.count() + 1
		k.d.after = func(n int) {
			if n == preRead {
				close(preStarted)
				<-preResume
			}
		}
		k.d.set(10*time.Second, nil)
		go func() {
			defer close(preDone)
			r, err := k.b.Reserve(DirectionAuxiliary)
			if err != nil {
				t.Error(err)
				return
			}
			r.Release()
		}()
		totalWait(t, preStarted)
		totalSegmentLeafFree(t, k.b)
		beforeVerified := k.issuer.verifyCallCount()
		if status, reason := k.b.retireVerifiedTotalSegment(); status != "REFUSED" || reason != "no-verified-commit" {
			t.Fatal("Retire accepted unverified completed Q", status, reason)
		}
		totalSegmentAck(t, k.b, rk, "DEFERRED")
		verifiedCalls := k.issuer.verifyCallCount()
		if verifiedCalls != beforeVerified+1 {
			t.Fatal("deferred ACK did not verify exactly once")
		}
		if status, reason := k.b.retireVerifiedTotalSegment(); status != "DEFERRED" || reason != "read-or-completion" {
			t.Fatal("Retire ignored actual paused provider read", status, reason)
		}
		preOnce.Do(func() { close(preResume) })
		totalWait(t, preDone)
		if status, reason := k.b.retireVerifiedTotalSegment(); status != "RETIRED" || reason != "verified-whole-checkpoint" {
			t.Fatal("pending verified Q did not retire after real read/completions joined", status, reason)
		}
		if k.issuer.verifyCallCount() != verifiedCalls {
			t.Fatal("Retire repeated issuer verification")
		}
		if k.b.fullObservation.sequence != 4 || k.b.fullObservation.used != 2 || k.b.fullObservation.pending != 0 || k.b.fullObservation.nextRead != 10 {
			t.Fatal("retire reset original provider/completion state")
		}
	})
	t.Run("empty-header-and-copy-staging-leases", func(t *testing.T) {
		f := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		q := f.offer(t, 10*time.Second)
		_, receipt := f.stream(t, q)
		resume, done := make(chan struct{}), make(chan struct{})
		var once sync.Once
		join := totalCleanup(t, func() { once.Do(func() { close(resume) }) }, f.releaseAll)
		join(done)
		ph := &totalSegmentPausedHash{Hash: f.b.fullObservation.segment.streamHash, started: make(chan struct{}), resume: resume}
		ph.armed.Store(true)
		f.b.fullObservation.segment.streamHash = ph
		type beginResult struct {
			token  totalSegmentStream
			status string
		}
		result := make(chan beginResult, 1)
		go func() {
			defer close(done)
			a, status := f.b.beginTotalSegmentStream(q)
			result <- beginResult{a, status}
		}()
		totalWait(t, ph.started)
		totalSegmentLeafFree(t, f.b)
		totalSegmentAck(t, f.b, receipt, "DEFERRED")
		headerVerifiedCalls := f.issuer.verifyCallCount()
		if status, reason := f.b.retireVerifiedTotalSegment(); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("Retire ignored actual HEADER hash loan", status, reason)
		}
		if _, status := f.b.beginTotalSegmentStream(q); status != "REFUSED" {
			t.Fatal("duplicate HEADER Begin", status)
		}
		once.Do(func() { close(resume) })
		totalWait(t, done)
		a := <-result
		if a.status != "STREAMING" {
			t.Fatal(a.status)
		}
		if status, reason := f.b.retireVerifiedTotalSegment(); status != "RETIRED" || reason != "verified-whole-checkpoint" {
			t.Fatal("verified EMPTY Q did not retire after HEADER join", status, reason)
		}
		if f.issuer.verifyCallCount() != headerVerifiedCalls {
			t.Fatal("HEADER Retire repeated issuer verification")
		}
		if _, status := f.b.borrowTotalSegmentChunk(a.token); status != "REFUSED" {
			t.Fatal("stale EMPTY attempt", status)
		}
		q2 := f.offer(t, 20*time.Second)
		_, r2 := f.stream(t, q2)
		totalSegmentAck(t, f.b, receipt, "ALREADY_RETIRED")
		if !f.b.fullObservation.segment.qActive || f.b.fullObservation.segment.q != q2 {
			t.Fatal("same row cursor replay retired later EMPTY")
		}
		totalSegmentAck(t, f.b, r2, "RETIRED")

		g := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		g.episode(t, time.Second)
		qg := g.offer(t, 5*time.Second)
		_, old := g.stream(t, qg)
		a2, status := g.b.beginTotalSegmentStream(qg)
		if status != "STREAMING" {
			t.Fatal(status)
		}
		copyResume, copyDone := make(chan struct{}), make(chan struct{})
		var copyOnce sync.Once
		joinCopy := totalCleanup(t, func() { copyOnce.Do(func() { close(copyResume) }) }, g.releaseAll)
		joinCopy(copyDone)
		p2 := &totalSegmentPausedHash{Hash: g.b.fullObservation.segment.streamHash, started: make(chan struct{}), resume: copyResume}
		p2.armed.Store(true)
		g.b.fullObservation.segment.streamHash = p2
		loan, status := g.b.borrowTotalSegmentChunk(a2)
		if status != "BORROWED" {
			t.Fatal(status)
		}
		copied := make(chan string, 1)
		go func() { defer close(copyDone); _, status := g.b.copyTotalSegmentChunk(loan); copied <- status }()
		totalWait(t, p2.started)
		totalSegmentLeafFree(t, g.b)
		if _, status := g.b.copyTotalSegmentChunk(loan); status != "REFUSED" {
			t.Fatal("duplicate Copy", status)
		}
		if g.b.releaseTotalSegmentChunk(loan) {
			t.Fatal("Release freed COPYING lane")
		}
		if status, reason := g.b.retireVerifiedTotalSegment(); status != "REFUSED" || reason != "no-verified-commit" {
			t.Fatal("Retire accepted an unverified copying Q", status, reason)
		}
		totalSegmentAck(t, g.b, old, "DEFERRED")
		copyVerifiedCalls := g.issuer.verifyCallCount()
		if status, reason := g.b.retireVerifiedTotalSegment(); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("Retire ignored COPYING protection", status, reason)
		}
		copyOnce.Do(func() { close(copyResume) })
		totalWait(t, copyDone)
		if <-copied != "COPIED" {
			t.Fatal("real Copy did not publish")
		}
		if status, reason := g.b.retireVerifiedTotalSegment(); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("Retire ignored delivered staging protection", status, reason)
		}
		bad := loan
		bad.loanID++
		if g.b.releaseTotalSegmentChunk(bad) {
			t.Fatal("wrong loan freed staging")
		}
		if !g.b.releaseTotalSegmentChunk(loan) {
			t.Fatal("delivered loan Release")
		}
		if status, reason := g.b.retireVerifiedTotalSegment(); status != "RETIRED" || reason != "verified-whole-checkpoint" {
			t.Fatal("verified Q did not retire after exact delivered Release", status, reason)
		}
		if g.issuer.verifyCallCount() != copyVerifiedCalls {
			t.Fatal("copy/staging Retire repeated issuer verification")
		}
	})
	t.Run("idle-retry-invalidation-and-partition-digest", func(t *testing.T) {
		f := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		f.episode(t, time.Second)
		q := f.offer(t, 5*time.Second)
		rows, r := f.stream(t, q)
		if hex.EncodeToString(r.witness.digest[:]) != "9add77d141789e3f853b03649f683fc9e76cf825f73f2a59e8496e6e2bcf1ff6" {
			t.Fatal("canonical total-v2 Config5/header/rows encoding changed")
		}
		a, status := f.b.beginTotalSegmentStream(q)
		if status != "STREAMING" {
			t.Fatal(status)
		}
		oldLoan, status := f.b.borrowTotalSegmentChunk(a)
		if status != "BORROWED" {
			t.Fatal(status)
		}
		if _, status := f.b.copyTotalSegmentChunk(oldLoan); status != "COPIED" {
			t.Fatal(status)
		}
		if !f.b.releaseTotalSegmentChunk(oldLoan) {
			t.Fatal("partial retry Release")
		}
		totalSegmentAck(t, f.b, r, "RETIRED")
		f.episode(t, 10*time.Second)
		q2 := f.offer(t, 15*time.Second)
		b, status := f.b.beginTotalSegmentStream(q2)
		if status != "STREAMING" {
			t.Fatal(status)
		}
		newLoan, status := f.b.borrowTotalSegmentChunk(b)
		if status != "BORROWED" {
			t.Fatal(status)
		}
		s := f.b.fullObservation.segment
		before := s.nextLoan
		if _, status := f.b.borrowTotalSegmentChunk(a); status != "REFUSED" {
			t.Fatal("retired retry borrowed reused ring", status)
		}
		if f.b.releaseTotalSegmentChunk(oldLoan) || s.nextLoan != before || s.loan != newLoan || s.loanPhase != 1 {
			t.Fatal("stale token altered newer loan")
		}
		if _, status := f.b.copyTotalSegmentChunk(newLoan); status != "COPIED" {
			t.Fatal(status)
		}
		if f.b.releaseTotalSegmentChunk(oldLoan) || s.loanPhase != 3 || s.loan != newLoan {
			t.Fatal("stale Release freed delivered newer staging")
		}
		if !f.b.releaseTotalSegmentChunk(newLoan) {
			t.Fatal("new loan Release")
		}
		last, status := f.b.borrowTotalSegmentChunk(b)
		if status != "BORROWED" {
			t.Fatal(status)
		}
		if _, status := f.b.copyTotalSegmentChunk(last); status != "COPIED" {
			t.Fatal(status)
		}
		if !f.b.releaseTotalSegmentChunk(last) {
			t.Fatal("final new loan Release")
		}
		w, known := f.b.expectedTotalSegmentWitness()
		if !known {
			t.Fatal("new Q digest lost")
		}
		totalSegmentAck(t, f.b, f.issuer.sign(w), "RETIRED")
		g := newTotalSegmentFixture(t, Config{Total: 1}, 2, 2)
		g.episode(t, time.Second)
		qg := g.offer(t, 5*time.Second)
		grow, gr := g.stream(t, qg)
		if qg != q || !reflect.DeepEqual(rows, grow) || r.witness.digest != gr.witness.digest {
			t.Fatal("C transport partition changed whole-Q digest")
		}
		// Partial C1 retry also resets its SHA offset/hash rather than appending.
		h := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		h.episode(t, time.Second)
		qh := h.offer(t, 5*time.Second)
		_, original := h.stream(t, qh)
		retry, st := h.b.beginTotalSegmentStream(qh)
		if st != "STREAMING" {
			t.Fatal(st)
		}
		l, st := h.b.borrowTotalSegmentChunk(retry)
		if st != "BORROWED" {
			t.Fatal(st)
		}
		if _, st := h.b.copyTotalSegmentChunk(l); st != "COPIED" {
			t.Fatal(st)
		}
		if !h.b.releaseTotalSegmentChunk(l) {
			t.Fatal("partial Release")
		}
		_, again := h.stream(t, qh)
		if again.witness.digest != original.witness.digest {
			t.Fatal("retry hash not reset")
		}
	})
	t.Run("source-config-proof-and-sticky-integrity", func(t *testing.T) {
		f := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		f.episode(t, time.Second)
		q := f.offer(t, 5*time.Second)
		if _, known := f.b.expectedTotalSegmentWitness(); known {
			t.Fatal("premature digest")
		}
		totalSegmentAck(t, f.b, totalSegmentReceipt{}, "REFUSED")
		_, r := f.stream(t, q)
		mutations := []func(*totalSegmentWitness){
			func(w *totalSegmentWitness) { w.owner = &totalFullObservation{} }, func(w *totalSegmentWitness) { w.session[0]++ },
			func(w *totalSegmentWitness) { w.descriptor.key.anchor += "foreign" }, func(w *totalSegmentWitness) { w.descriptor.key.epoch[0]++ }, func(w *totalSegmentWitness) { w.descriptor.key.clockDomain[0]++ }, func(w *totalSegmentWitness) { w.descriptor.key.configuration[0]++ },
			func(w *totalSegmentWitness) { w.descriptor.cfg.Total++ }, func(w *totalSegmentWitness) { w.descriptor.cfg.OutboundReserve++ }, func(w *totalSegmentWitness) { w.descriptor.cfg.MaxOutbound++ }, func(w *totalSegmentWitness) { w.descriptor.cfg.MaxInbound++ }, func(w *totalSegmentWitness) { w.descriptor.cfg.MaxAuxiliary++ },
			func(w *totalSegmentWitness) { w.descriptor.coverageStart++ }, func(w *totalSegmentWitness) { w.descriptor.checkpointID++ }, func(w *totalSegmentWitness) { w.descriptor.previousID++ }, func(w *totalSegmentWitness) { w.descriptor.previousDigest[0]++ }, func(w *totalSegmentWitness) { w.descriptor.fromSeq++ }, func(w *totalSegmentWitness) { w.descriptor.throughSeq++ }, func(w *totalSegmentWitness) { w.descriptor.fromAt++ }, func(w *totalSegmentWitness) { w.descriptor.throughAt++ }, func(w *totalSegmentWitness) { w.descriptor.rowCount++ }, func(w *totalSegmentWitness) { w.descriptor.fromCarry.full = true }, func(w *totalSegmentWitness) { w.descriptor.throughCarry.full = true }, func(w *totalSegmentWitness) { w.digest[0]++ },
		}
		for _, mutate := range mutations {
			bad := r.witness
			mutate(&bad)
			totalSegmentAck(t, f.b, f.issuer.sign(bad), "REFUSED")
		}
		bad := r
		bad.proof[0]++
		totalSegmentAck(t, f.b, bad, "REFUSED")
		f.issuer.accept.Store(false)
		totalSegmentAck(t, f.b, r, "REFUSED")
		if !f.b.fullObservation.segment.qActive {
			t.Fatal("uncertain test issuer retired")
		}
		f.issuer.accept.Store(true)
		totalSegmentAck(t, f.b, r, "RETIRED")
		totalSegmentAck(t, f.b, bad, "REFUSED")
		totalSegmentAck(t, f.b, r, "ALREADY_RETIRED")
		for _, digestMismatch := range []bool{false, true} {
			g := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
			g.episode(t, time.Second)
			qg := g.offer(t, 5*time.Second)
			_, old := g.stream(t, qg)
			// Deliberate negative integrity corruption, never positive owner evidence.
			g.b.mu.Lock()
			if digestMismatch {
				g.b.fullObservation.rows[0].upper++
			} else {
				g.b.fullObservation.rows[0].valid = false
			}
			g.b.mu.Unlock()
			a, status := g.b.beginTotalSegmentStream(qg)
			if status != "STREAMING" {
				t.Fatal(status)
			}
			for {
				offset := g.b.fullObservation.segment.streamOffset
				loan, status := g.b.borrowTotalSegmentChunk(a)
				if status != "BORROWED" {
					t.Fatal(status)
				}
				_, status = g.b.copyTotalSegmentChunk(loan)
				if !g.b.releaseTotalSegmentChunk(loan) {
					t.Fatal("UNKNOWN cleanup Release")
				}
				if status == "UNKNOWN" {
					if g.b.fullObservation.segment.streamOffset != offset || g.b.fullObservation.segment.streamFinished {
						t.Fatal("UNKNOWN Release advanced progress")
					}
					break
				}
				if status != "COPIED" {
					t.Fatal(status)
				}
			}
			s := g.b.fullObservation.segment
			if s.qIntegrityFault == "" || !s.qActive || s.loanPhase != 0 {
				t.Fatal("bad Q not sticky/owned")
			}
			if _, known := g.b.expectedTotalSegmentWitness(); known {
				t.Fatal("bad Q witness available")
			}
			totalSegmentAck(t, g.b, old, "REFUSED")
			if _, status := g.b.beginTotalSegmentStream(qg); status != "REFUSED" {
				t.Fatal("retry cleared Q fault", status)
			}
		}
		g := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		g.episode(t, time.Second)
		qg := g.offer(t, 5*time.Second)
		_, old := g.stream(t, qg)
		g.d.set(10*time.Second, nil)
		g.take(t, DirectionInbound) // admission succeeds with no observation row capacity
		if g.b.fullObservation.invalid != "transition-capacity-gap" || g.b.Snapshot().Used != 1 {
			t.Fatal("gap changed admission")
		}
		totalSegmentAck(t, g.b, old, "RETIRED")
		if _, status := g.b.sealTotalSegment(); status != "UNKNOWN" {
			t.Fatal("later global gap cleared", status)
		}

		// The verifier is outside mu and its result is rechecked against CURRENT Q.
		k := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		qk := k.offer(t, time.Second)
		_, rk := k.stream(t, qk)
		started, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		var verifies atomic.Int64
		join := totalCleanup(t, func() { once.Do(func() { close(resume) }) }, k.releaseAll)
		join(done)
		k.issuer.after = func() {
			if verifies.Add(1) == 1 {
				close(started)
				<-resume
			}
		}
		result := make(chan string, 1)
		go func() { defer close(done); st, _ := k.b.acknowledgeTotalSegment(rk); result <- st }()
		totalWait(t, started)
		totalSegmentLeafFree(t, k.b)
		totalSegmentAck(t, k.b, rk, "RETIRED")
		qnext := k.offer(t, 2*time.Second)
		once.Do(func() { close(resume) })
		totalWait(t, done)
		if <-result != "ALREADY_RETIRED" || !k.b.fullObservation.segment.qActive || k.b.fullObservation.segment.q != qnext {
			t.Fatal("late verification changed later Q")
		}
	})
	t.Run("open-carry-empty-time-and-v1-history", func(t *testing.T) {
		f := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
		f.d.after = func(n int) {
			if n == 2 {
				f.d.set(30*time.Second, nil)
			}
			if n == 6 {
				f.d.set(80*time.Second, nil)
			}
		}
		f.d.set(10*time.Second, nil)
		r := f.take(t, DirectionOutbound)
		q1 := f.offer(t, 40*time.Second)
		rows1, r1 := f.stream(t, q1)
		lo1, hi1 := totalSegmentMeasures(t, q1, rows1)
		totalSegmentAck(t, f.b, r1, "RETIRED")
		q2 := f.offer(t, 60*time.Second)
		rows2, r2 := f.stream(t, q2)
		lo2, hi2 := totalSegmentMeasures(t, q2, rows2)
		if len(rows2) != 0 || q2.fromSeq != q2.throughSeq || q2.checkpointID == q1.checkpointID || q2.fromCarry != q1.throughCarry || lo2 != 20*time.Second || hi2 != lo2 {
			t.Fatal("EMPTY lost temporal carry", q2, rows2, lo2, hi2)
		}
		f.issuer.accept.Store(false)
		totalSegmentAck(t, f.b, r2, "REFUSED")
		if f.b.fullObservation.segment.committedAt != 40*time.Second {
			t.Fatal("uncertain EMPTY advanced time")
		}
		if same := f.offer(t, 65*time.Second); same != q2 {
			t.Fatal("later clock replaced uncertain frozen EMPTY")
		}
		f.issuer.accept.Store(true)
		totalSegmentAck(t, f.b, r2, "RETIRED")
		// init1, entry pre2/post3, Seal4, EMPTY Seal5, exit pre6/post7.
		f.d.set(70*time.Second, nil)
		r.Release()
		q3 := f.offer(t, 90*time.Second)
		rows3, r3 := f.stream(t, q3)
		lo3, hi3 := totalSegmentMeasures(t, q3, rows3)
		if lo3 != 10*time.Second || hi3 != 20*time.Second || len(rows3) != 1 || rows3[0].lower != 70*time.Second || rows3[0].upper != 80*time.Second {
			t.Fatal("clipped exit/carry", lo3, hi3, rows3)
		}
		// Finite reconstructed FULL epoch history, not a retired suffix presented
		// to the original V1 reducer and not a second modeled admission FSM.
		history := append(append([]totalFullTransition{}, rows1...), rows3...)
		v1 := totalFullSnapshot{cfg: q3.cfg, coverageStart: q3.coverageStart, cut: totalFullCut{available: true, rows: len(history), sequence: uint64(len(history)), at: q3.throughAt}, rows: history}
		lo, hi, ok := totalFullDurationBounds(v1)
		if !ok || lo1+lo2+lo3 != lo || hi1+hi2+hi3 != hi || lo != 40*time.Second || hi != 70*time.Second {
			t.Fatal("full-epoch V1 comparison", lo, hi, ok)
		}
		totalSegmentAck(t, f.b, r3, "RETIRED")
		before := f.b.fullObservation.segment.nextCheckpoint
		if _, status := f.b.sealTotalSegment(); status != "NO_PROGRESS" || f.b.fullObservation.segment.nextCheckpoint != before {
			t.Fatal("identical cut fabricated progress", status)
		}
		// Negative row: clipping cannot conceal an impossible lower before the
		// committed cut; old original carry entry remains valid and exempt.
		badQ := q3
		badQ.fromCarry = totalSegmentCarry{}
		fold, valid := newTotalSegmentFold(badQ)
		if !valid {
			t.Fatal("negative schedule descriptor invalid")
		}
		badRow := totalFullTransition{sequence: q3.fromSeq + 1, full: true, count: 1, lower: 10 * time.Second, upper: 30 * time.Second, done: true, valid: true}
		if _, _, ok := fold.row(badRow); ok {
			t.Fatal("clipping concealed cross-committed-cut row")
		}
		totalSegmentLegacyRefused(t, f.b)
	})
	t.Run("disabled-invalid-and-nonwrapping-boundaries", func(t *testing.T) {
		var calls atomic.Int64
		clock := func() (time.Duration, error) { calls.Add(1); return 0, nil }
		b, err := newSegmentedBudget(Config{Total: 0, MaxOutbound: 1}, totalSourceKey{}, -1, -1, clock, nil, [16]byte{})
		if err != nil || b == nil || calls.Load() != 0 || b.fullObservation.rows != nil || b.fullObservation.segment != nil || b.fullSnapshot().status != "DISABLED" {
			t.Fatal("Total0 observer work", err)
		}
		r := totalTake(t, b, DirectionOutbound)
		if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
			t.Fatal(err)
		}
		r.Release()
		r.Release()
		if calls.Load() != 0 {
			t.Fatal("disabled clock")
		}
		if _, status := b.sealTotalSegment(); status != "DISABLED" {
			t.Fatal(status)
		}
		if invalid, err := newSegmentedBudget(Config{Total: -1}, totalKey(1), 2, 1, clock, nil, [16]byte{}); invalid != nil || !errors.Is(err, ErrInvalidConfig) || calls.Load() != 0 {
			t.Fatal("original invalidConfig", err)
		}
		for _, caps := range [][2]int{{0, 1}, {1, 0}, {1, 2}, {int(^uint(0) >> 1), 1}} {
			i := &totalSegmentTestIssuer{session: [16]byte{9}}
			before := calls.Load()
			x, err := newSegmentedBudget(Config{Total: 1}, totalKey(1), caps[0], caps[1], clock, i, i.session)
			if err != nil || x.fullObservation.segment == nil || len(x.fullObservation.rows) != 0 || calls.Load() != before {
				t.Fatal("invalid M/C allocation/fallback", caps, err)
			}
			totalSegmentLegacyRefused(t, x)
			rr := totalTake(t, x, DirectionOutbound)
			rr.Release()
		}
		// Enabled identity/issuer/clock failures remain explicit V2 and keep the
		// original Budget valid; they never select a certifying V1 fallback.
		i := &totalSegmentTestIssuer{session: [16]byte{9}}
		badClock := func() (time.Duration, error) { return 0, errors.New("unavailable") }
		x, err := newSegmentedBudget(Config{Total: 1}, totalKey(1), 2, 1, badClock, i, i.session)
		if err != nil {
			t.Fatal(err)
		}
		totalSegmentLegacyRefused(t, x)
		rr := totalTake(t, x, DirectionOutbound)
		rr.Release()
		if x.Snapshot().Used != 0 {
			t.Fatal("clock error changed admission")
		}
		for _, which := range []string{"key", "nil-clock", "nil-issuer", "session"} {
			key, provider, issuer, session := totalKey(1), clock, totalSegmentIssuer(i), i.session
			switch which {
			case "key":
				key.epoch = [16]byte{}
			case "nil-clock":
				provider = nil
			case "nil-issuer":
				issuer = nil
			case "session":
				session = [16]byte{}
			}
			before := calls.Load()
			z, err := newSegmentedBudget(Config{Total: 1}, key, 2, 1, provider, issuer, session)
			if err != nil || z.fullObservation.segment == nil || z.fullObservation.invalid == "" || len(z.fullObservation.rows) != 0 || calls.Load() != before {
				t.Fatal("failed observer fallback/allocation", which, err)
			}
			totalSegmentLegacyRefused(t, z)
			r := totalTake(t, z, DirectionOutbound)
			r.Release()
		}
		// Deliberate counter-boundary injections are NEGATIVE overflow checks,
		// never synthetic occupancy or positive rollover evidence.
		for _, which := range []string{"checkpoint", "stream", "loan", "generation", "sequence", "pending", "read", "reads"} {
			f := newTotalSegmentFixture(t, Config{Total: 1}, 2, 1)
			s := f.b.fullObservation.segment
			switch which {
			case "checkpoint":
				s.nextCheckpoint = totalMaxUint
				f.d.set(time.Second, nil)
				if _, status := f.b.sealTotalSegment(); status != "UNKNOWN" {
					t.Fatal(which, status)
				}
			case "stream":
				q := f.offer(t, time.Second)
				s.nextStream = totalMaxUint
				if _, status := f.b.beginTotalSegmentStream(q); status != "UNKNOWN" {
					t.Fatal(which, status)
				}
			case "loan":
				q := f.offer(t, time.Second)
				a, status := f.b.beginTotalSegmentStream(q)
				if status != "STREAMING" {
					t.Fatal(status)
				}
				s.nextLoan = totalMaxUint
				if _, status := f.b.borrowTotalSegmentChunk(a); status != "UNKNOWN" {
					t.Fatal(which, status)
				}
			case "generation":
				s.generations[0] = totalMaxUint
				f.take(t, DirectionOutbound)
			case "sequence":
				f.b.fullObservation.sequence = totalMaxUint
				f.take(t, DirectionOutbound)
			case "pending":
				f.b.fullObservation.pending = totalMaxUint
				f.take(t, DirectionOutbound)
			case "read":
				f.b.fullObservation.nextRead = totalMaxUint
				f.take(t, DirectionOutbound)
			case "reads":
				f.b.fullObservation.reads = totalMaxUint
				f.take(t, DirectionOutbound)
			}
			if f.b.fullObservation.invalid == "" {
				t.Fatal("overflow did not fail closed", which)
			}
			totalSegmentLegacyRefused(t, f.b)
		}
		q := totalSegmentDescriptor{key: totalKey(1), cfg: Config{Total: 1}, checkpointID: 1, fromSeq: totalMaxUint, throughSeq: totalMaxUint, fromAt: 0, throughAt: time.Second}
		fold, ok := newTotalSegmentFold(q)
		if !ok {
			t.Fatal("EMPTY Max cursor first+1 arithmetic")
		}
		if _, _, ok := fold.finish(); !ok {
			t.Fatal("EMPTY Max fold")
		}
	})
}
