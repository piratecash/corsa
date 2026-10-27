package node

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

// All numbers here are tiny fixture budgets, never deployment selections.
func handshakeFileTestLimits() handshakeFileLimits {
	return handshakeFileLimits{recordBytes: 2048, storeBytes: 131072, orphanBytes: 65536, scanBytes: 1048576, storeRecords: 128, orphanRecords: 64, scanBatch: 4, scanSteps: 4096}
}

type handshakeFileFixture struct {
	h        *inboundHandshakes
	d        *k5TestClock
	i        *handshakeFileIssuer
	dir      *secretfile.Dir
	releases []func()
	closeDir func()
}

func newHandshakeFileFixture(t *testing.T, limits handshakeFileLimits) *handshakeFileFixture {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("directory-inode evidence lease intentionally unsupported on this platform")
	}
	dir, err := secretfile.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &handshakeFileFixture{dir: dir, d: &k5TestClock{}}
	f.closeDir = sync.OnceFunc(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(f.closeDir)
	f.h, f.i, err = newFileObservedInboundHandshakes(handshakeBounds{node: 1, loopback: 1, perSource: 1}, k5TestKey(81), 4, 1, f.d.read, dir, limits, [16]byte{19})
	if f.i != nil {
		t.Cleanup(func() {
			f.releaseAll()
			if err := f.i.Close(); err != nil {
				t.Error("issuer close", err)
			}
		})
	}
	if err != nil {
		t.Fatal("real filesystem issuer construction", err)
	}
	return f
}
func (f *handshakeFileFixture) releaseAll() {
	for _, r := range f.releases {
		r()
	}
}
func (f *handshakeFileFixture) admit(t *testing.T) func() {
	t.Helper()
	r := k5Admit(t, f.h, handshakeSource{group: "file-fixture"})
	f.releases = append(f.releases, r)
	return r
}
func (f *handshakeFileFixture) episode(t *testing.T, at time.Duration) {
	t.Helper()
	f.d.set(at, nil)
	r := f.admit(t)
	f.d.set(at+time.Second, nil)
	r()
	r()
}
func (f *handshakeFileFixture) offer(t *testing.T, at time.Duration) handshakeSegmentDescriptor {
	t.Helper()
	f.d.set(at, nil)
	q, status := f.h.sealHandshakeSegment()
	if status != "OFFERED" {
		t.Fatal(status)
	}
	return q
}
func handshakeFileCommit(t *testing.T, f *handshakeFileFixture, q handshakeSegmentDescriptor) handshakeSegmentReceipt {
	t.Helper()
	r, outcome, err := f.i.Commit(q)
	if err != nil || outcome != secretfile.DurableCommitted || r.witness.owner != f.h.fullObservation || r.witness.descriptor != q {
		t.Fatal("actual whole-Q commit", outcome, err)
	}
	return r
}
func handshakeFileRead(t *testing.T, dir *secretfile.Dir, name string, limit int) []byte {
	t.Helper()
	file, err := dir.OpenFileForRead(name)
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > limit {
		t.Fatal("bounded test record read", readErr, closeErr)
	}
	return raw
}

func TestHandshakeFileIssuerActualStore(t *testing.T) {
	t.Run("owner-chunks-empty-reuse", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		var first []byte
		for n := 0; n < 6; n++ {
			at := time.Duration(1+3*n) * time.Second
			f.episode(t, at)
			q := f.offer(t, at+2*time.Second)
			r := handshakeFileCommit(t, f, q)
			if n == 0 {
				first = handshakeFileRead(t, f.dir, handshakeChunkName(q.checkpointID, 0), 2048)
			}
			k5SegmentAck(t, f.h, r, "RETIRED")
			if f.h.inFlight != 0 || f.h.fullObservation.used != 0 || f.h.fullObservation.sequence != uint64(2*(n+1)) || len(f.h.fullObservation.rows) != 4 || len(f.h.fullObservation.segment.staging) != 1 {
				t.Fatal("real owner rollover changed admission/storage")
			}
		}
		if now := handshakeFileRead(t, f.dir, handshakeChunkName(1, 0), 2048); !bytes.Equal(now, first) {
			t.Fatal("committed bytes disappeared/changed after ring reuse")
		}
		f.d.set(30*time.Second, nil)
		release := f.admit(t)
		q := f.offer(t, 31*time.Second)
		r := handshakeFileCommit(t, f, q)
		entry := q.throughCarry
		k5SegmentAck(t, f.h, r, "RETIRED")
		empty := f.offer(t, 40*time.Second)
		er := handshakeFileCommit(t, f, empty)
		if empty.rowCount != 0 || empty.fromSeq != empty.throughSeq || empty.fromCarry != entry || empty.throughCarry != entry {
			t.Fatal("EMPTY/open carry identity")
		}
		if _, err := f.dir.Lstat(handshakeChunkName(empty.checkpointID, 0)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("EMPTY materialized a fake chunk", err)
		}
		k5SegmentAck(t, f.h, er, "RETIRED")
		f.d.set(50*time.Second, nil)
		release()
		exit := f.offer(t, 51*time.Second)
		xr := handshakeFileCommit(t, f, exit)
		k5SegmentAck(t, f.h, xr, "RETIRED")
		if exit.fromCarry != entry || exit.throughCarry.full {
			t.Fatal("entry bracket replaced across file checkpoints")
		}
		legacy := f.h.fullSnapshot()
		if legacy.cut.available || legacy.status != "UNKNOWN" || !legacy.pastOnly {
			t.Fatal("V2 suffix leaked as legacy cut")
		}
	})
	t.Run("fresh-source-refuses-old-store", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		r := handshakeFileCommit(t, f, q)
		before := handshakeFileRead(t, f.dir, handshakeFinalName(1), 2048)
		if err := f.i.Close(); err != nil {
			t.Fatal(err)
		}
		clock := &k5TestClock{}
		h, newIssuer, err := newFileObservedInboundHandshakes(f.h.bounds, k5TestKey(81), 4, 1, clock.read, f.dir, handshakeFileTestLimits(), [16]byte{19})
		if !errors.Is(err, errHandshakeStoreNotFresh) || h == nil || newIssuer == nil {
			t.Fatal("old records adopted by fresh source", err)
		}
		defer func() {
			if err := newIssuer.Close(); err != nil {
				t.Error(err)
			}
		}()
		clock.set(time.Second, nil)
		release := k5Admit(t, h, handshakeSource{})
		clock.set(2*time.Second, nil)
		release()
		clock.set(3*time.Second, nil)
		sameQ, status := h.sealHandshakeSegment()
		if status != "OFFERED" || sameQ != q {
			t.Fatal("identical-text negative fixture differs", status)
		}
		if _, err := newIssuer.warmWitnessLocked(sameQ); err != nil {
			t.Fatal(err)
		}
		k5SegmentAck(t, h, r, "REFUSED")
		if commit, valid := newIssuer.verifyHandshakeSegment(r); valid || commit != (handshakeSegmentCommit{}) {
			t.Fatal("serialized tokens revived runtime authority")
		}
		if h.fullObservation.used != 2 || h.inFlight != 0 || !bytes.Equal(before, handshakeFileRead(t, f.dir, handshakeFinalName(1), 2048)) {
			t.Fatal("new source changed old evidence or its own real gate")
		}
	})
}

func TestHandshakeFileIssuerCommitRecovery(t *testing.T) {
	t.Run("final-indeterminate-visible-readback", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		real := f.i.writeRecord
		f.i.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
			outcome, err := real(name, raw)
			if strings.HasSuffix(name, ".final") && err == nil {
				return secretfile.DurableIndeterminate, errors.New("test lost confirmation after REAL write")
			}
			return outcome, err
		}
		r, outcome, err := f.i.Commit(q)
		if err == nil || outcome != secretfile.DurableIndeterminate || r != (handshakeSegmentReceipt{}) || f.i.committedID != 0 {
			t.Fatal("uncertain final minted receipt", outcome, err)
		}
		visible := handshakeFileRead(t, f.dir, handshakeFinalName(q.checkpointID), 2048)
		if !handshakeCheckedRecord(visible) {
			t.Fatal("fixture final is not visible/hash valid")
		}
		if status, _ := f.h.retireVerifiedHandshakeSegment(); status != "REFUSED" {
			t.Fatal("readability alone retired source")
		}
		// Exhaust the actual fresh-confirmation budget: present/hash valid is
		// still not enough. This is a budget/control fault, not a syscall fault.
		budget := f.i.limits.scanBytes
		f.i.limits.scanBytes = 1
		if r, outcome, err := f.i.Commit(q); err == nil || r != (handshakeSegmentReceipt{}) || f.i.committedID != 0 || outcome != secretfile.DurableIndeterminate {
			t.Fatal("visible bytes promoted/forgotten without fresh confirmation", outcome, err)
		}
		f.i.limits.scanBytes = budget
		// Explicit negative namespace fault after an uncertain publication:
		// a later missing-final/rewrite refusal must not downgrade logical Q.
		if err := f.dir.Remove(handshakeFinalName(q.checkpointID)); err != nil {
			t.Fatal(err)
		}
		f.i.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
			if strings.HasSuffix(name, ".final") {
				return secretfile.DurableNotCommitted, errors.New("test retry final refused")
			}
			return real(name, raw)
		}
		if retry, outcome, err := f.i.Commit(q); err == nil || retry != (handshakeSegmentReceipt{}) || outcome != secretfile.DurableIndeterminate || f.i.committedID != 0 {
			t.Fatal("late final refusal downgraded prior visibility", outcome, err)
		}
		f.i.writeRecord = real
		r = handshakeFileCommit(t, f, q)
		if f.i.committedID != q.checkpointID {
			t.Fatal("same-live-source recovery failed")
		}
		k5SegmentAck(t, f.h, r, "RETIRED")
		if !bytes.Equal(visible, handshakeFileRead(t, f.dir, handshakeFinalName(q.checkpointID), 2048)) {
			t.Fatal("retry overwrote immutable final bytes")
		}
		knownID := f.i.committedID
		f.i.limits.scanBytes = 1
		if retry, outcome, err := f.i.Commit(q); err == nil || retry != (handshakeSegmentReceipt{}) || outcome != secretfile.DurableIndeterminate || f.i.committedID != knownID {
			t.Fatal("failed revalidation undid historical commit", outcome, err)
		}
		f.i.limits.scanBytes = budget

	})
	t.Run("returned-status-paths", func(t *testing.T) {
		for _, fault := range []string{"not-committed", "committed-error", "fake-success"} {
			f := newHandshakeFileFixture(t, handshakeFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			real := f.i.writeRecord
			f.i.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
				if !strings.HasSuffix(name, ".final") {
					return real(name, raw)
				}
				if fault == "not-committed" {
					return secretfile.DurableNotCommitted, errors.New("test before final write")
				}
				if fault == "fake-success" {
					return secretfile.DurableCommitted, nil
				}
				out, err := real(name, raw)
				if err != nil {
					return out, err
				}
				return secretfile.DurableCommitted, errors.New("test error after REAL final write")
			}
			r, outcome, err := f.i.Commit(q)
			if err == nil || r != (handshakeSegmentReceipt{}) || f.i.committedID != 0 {
				t.Fatal("status alone accepted", fault, outcome, err)
			}
			want := secretfile.DurableIndeterminate
			if fault == "not-committed" {
				want = secretfile.DurableNotCommitted
			}
			if outcome != want {
				t.Fatal("tri-state outcome", fault, outcome, want)
			}
			if f.h.fullObservation.used != 2 || f.h.inFlight != 0 {
				t.Fatal("unconfirmed status altered source/admission")
			}
			f.i.writeRecord = real
			r = handshakeFileCommit(t, f, q)
			k5SegmentAck(t, f.h, r, "RETIRED")
		}
	})
	t.Run("missing-corrupt-conflict", func(t *testing.T) {
		for _, fault := range []string{"missing", "corrupt", "conflict"} {
			f := newHandshakeFileFixture(t, handshakeFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			r := handshakeFileCommit(t, f, q)
			name := handshakeChunkName(q.checkpointID, 0)
			if fault == "missing" {
				if err := f.dir.Remove(name); err != nil {
					t.Fatal(err)
				}
			} else {
				raw := handshakeFileRead(t, f.dir, name, 2048)
				if fault == "corrupt" {
					raw[0] ^= 1
				} else {
					raw[20] ^= 1
					sum := sha256.Sum256(raw[:len(raw)-32])
					copy(raw[len(raw)-32:], sum[:])
				}
				if out, err := f.dir.DurableWrite(name, raw); out != secretfile.DurableCommitted || err != nil {
					t.Fatal(out, err)
				}
			}
			k5SegmentAck(t, f.h, r, "REFUSED")
			if _, _, err := f.i.Commit(q); err == nil || f.h.fullObservation.used != 2 {
				t.Fatal("missing/conflicting committed evidence accepted", fault, err)
			}
		}
	})
}

func TestHandshakeFileIssuerBudgets(t *testing.T) {
	t.Run("record-and-checked-codec", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		w, err := f.i.warmWitnessLocked(q)
		if err != nil {
			t.Fatal(err)
		}
		stream, status := f.h.beginHandshakeSegmentStream(q)
		if status != "STREAMING" {
			t.Fatal(status)
		}
		loan, _ := f.h.borrowHandshakeSegmentChunk(stream)
		chunk, status := f.h.copyHandshakeSegmentChunk(loan)
		if status != "COPIED" {
			t.Fatal(status)
		}
		if raw, err := handshakeChunkRecord(w, 0, chunk, 1, 64); raw != nil || !errors.Is(err, errHandshakeFileBudget) {
			t.Fatal("ignored Write overflow escaped checked capture", err)
		}
		raw, err := handshakeChunkRecord(w, 0, chunk, 1, 2048)
		if err != nil || !handshakeCheckedRecord(raw) {
			t.Fatal(err)
		}
		if !f.h.releaseHandshakeSegmentChunk(loan) {
			t.Fatal("release actual staging")
		}
		if len(raw) > 2048 {
			t.Fatal("record cap")
		}
		// Shared source SHA format is preserved, not a second row codec.
		capture := newHandshakeFileCapture(2048)
		writeHandshakeSegmentHeader(capture, q)
		hash := sha256.New()
		writeHandshakeSegmentHeader(hash, q)
		if capture.err != nil || !bytes.Equal(capture.Sum(nil), hash.Sum(nil)) {
			t.Fatal("canonical header drift")
		}
		for _, size := range []int{0, 1, 32} {
			c := newHandshakeFileCapture(size)
			writeHandshakeSegmentHeader(c, q)
			if b, err := c.finish(); b != nil || err == nil {
				t.Fatal("malformed record limit")
			}
		}
		for _, name := range []string{"q0000000000000000.final", "q0000000000000001-c0000000000000000.chunkx", "other", "q0000000000000001-cGGGGGGGGGGGGGGGG.chunk"} {
			if _, _, _, ok := handshakeFileName(name); ok {
				t.Fatal("foreign/malformed name accepted", name)
			}
		}
	})
	t.Run("store-and-orphan-headroom", func(t *testing.T) {
		for _, kind := range []string{"records", "orphans", "bytes"} {
			limits := handshakeFileTestLimits()
			if kind == "records" {
				limits.storeRecords = 2
			}
			if kind == "orphans" {
				limits.orphanRecords = 2
			}
			if kind == "bytes" {
				limits.recordBytes = 1024
				limits.storeBytes = 1024
			}
			f := newHandshakeFileFixture(t, limits)
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			r, _, err := f.i.Commit(q)
			if r != (handshakeSegmentReceipt{}) || err == nil || f.i.committedID != 0 || f.h.fullObservation.used != 2 || f.h.inFlight != 0 {
				t.Fatal("budget exhaustion minted receipt/altered gate", kind, err)
			}
			// No pruning or implicit capacity increase on repeated attempts.
			for retry := 0; retry < 2; retry++ {
				if r, _, err := f.i.Commit(q); err == nil || r != (handshakeSegmentReceipt{}) {
					t.Fatal("repeat silently freed/increased budget", kind)
				}
			}
			if _, err := f.dir.Lstat(handshakeFinalName(q.checkpointID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("cap exhaustion wrote final", kind, err)
			}
		}
	})
	t.Run("recovery-complete-or-refuse", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		r := handshakeFileCommit(t, f, q)
		for _, fault := range []string{"names", "steps", "bytes"} {
			old := f.i.limits
			switch fault {
			case "names":
				f.i.limits.storeRecords = 1
			case "steps":
				f.i.limits.scanSteps = 1
			case "bytes":
				f.i.limits.scanBytes = 1
			}
			k5SegmentAck(t, f.h, r, "REFUSED")
			if f.h.fullObservation.used != 2 || f.h.fullObservation.segment.retiredThrough != 0 {
				t.Fatal("partial recovery certified prefix", fault)
			}
			f.i.limits = old
		}
		k5SegmentAck(t, f.h, r, "RETIRED")
	})
}

func TestHandshakeFileIssuerAuthority(t *testing.T) {
	t.Run("wrong-source-session-proof", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		r := handshakeFileCommit(t, f, q)
		for _, mutate := range []func(*handshakeSegmentReceipt){func(r *handshakeSegmentReceipt) { r.proof[0]++ }, func(r *handshakeSegmentReceipt) { r.witness.session[0]++ }, func(r *handshakeSegmentReceipt) { r.witness.descriptor.previousDigest[0]++ }, func(r *handshakeSegmentReceipt) { r.witness.owner = nil }} {
			wrong := r
			mutate(&wrong)
			k5SegmentAck(t, f.h, wrong, "REFUSED")
			if _, valid := f.i.verifyHandshakeSegment(wrong); valid {
				t.Fatal("raw caller witness/proof supplied authority")
			}
		}
		k5SegmentAck(t, f.h, r, "RETIRED")
		if err := f.i.Close(); err != nil {
			t.Fatal(err)
		}
		if _, valid := f.i.verifyHandshakeSegment(r); valid {
			t.Fatal("closed issuer minted authority")
		}
	})
	t.Run("real-files-header-loan-defer", func(t *testing.T) {
		f := newHandshakeFileFixture(t, handshakeFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		r := handshakeFileCommit(t, f, q)
		started, block, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(block) })
		track := k5OwnedCleanup(t, unblock, f.releaseAll)
		track(done)
		paused := &k5SegmentPausedHash{Hash: f.h.fullObservation.segment.streamHash, started: started, resume: block}
		f.h.fullObservation.segment.streamHash = paused
		paused.armed.Store(true)
		go func() {
			defer close(done)
			if _, status := f.h.beginHandshakeSegmentStream(q); status != "STREAMING" {
				t.Error(status)
			}
		}()
		k5Wait(t, started)
		if status, reason := f.h.acknowledgeHandshakeSegment(r); status != "DEFERRED" || reason != "copy-or-staging-loan" {
			t.Fatal("REAL filesystem receipt bypassed actual source lease", status, reason)
		}
		if !f.h.fullObservation.segment.verifiedPending {
			t.Fatal("test never reached verified real-store authority")
		}
		unblock()
		k5Wait(t, done)
		if status, _ := f.h.retireVerifiedHandshakeSegment(); status != "RETIRED" {
			t.Fatal(status)
		}
	})
	t.Run("namespace-membership-and-no-GC", func(t *testing.T) {
		for _, kind := range []string{"past-surplus", "current-surplus", "empty-surplus", "missing-declared-with-surplus", "orphan-headroom"} {
			f := newHandshakeFileFixture(t, handshakeFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			var receipt handshakeSegmentReceipt
			if kind == "past-surplus" || kind == "missing-declared-with-surplus" {
				receipt = handshakeFileCommit(t, f, q)
				k5SegmentAck(t, f.h, receipt, "RETIRED")
			}
			w, err := f.i.warmWitnessLocked(q)
			if kind == "past-surplus" || kind == "missing-declared-with-surplus" {
				w = receipt.witness
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "empty-surplus" {
				receipt = handshakeFileCommit(t, f, q)
				k5SegmentAck(t, f.h, receipt, "RETIRED")
				q = f.offer(t, 4*time.Second)
				w, err = f.i.warmWitnessLocked(q)
				if err != nil || q.rowCount != 0 {
					t.Fatal("EMPTY fixture", err)
				}
			}
			// A structurally valid, unreferenced record is never a committed
			// member merely because its qid is older/current.
			extra := handshakeSegmentChunk{descriptor: q, offset: 0, rows: nil, final: true}
			raw, err := handshakeChunkRecord(w, 999, extra, 1, 2048)
			if err != nil {
				t.Fatal(err)
			}
			name := handshakeChunkName(q.checkpointID, 999)
			if out, err := f.dir.DurableWrite(name, raw); out != secretfile.DurableCommitted || err != nil {
				t.Fatal(out, err)
			}
			if kind == "missing-declared-with-surplus" {
				if err := f.dir.Remove(handshakeChunkName(q.checkpointID, 0)); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "orphan-headroom" {
				f.i.limits.orphanRecords = 1
			}
			if kind == "past-surplus" || kind == "missing-declared-with-surplus" {
				q = f.offer(t, 4*time.Second)
			}
			if r, _, err := f.i.Commit(q); err == nil || r != (handshakeSegmentReceipt{}) {
				t.Fatal("surplus/unknown namespace minted receipt", kind, err)
			}
			if now := handshakeFileRead(t, f.dir, name, 2048); !bytes.Equal(now, raw) {
				t.Fatal("unconfirmed artifact auto-GC/overwrite", kind)
			}
		}
	})
}
