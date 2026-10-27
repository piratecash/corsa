package connbudget

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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

// Tiny functional-test budgets only; no deployment capacity/cadence choice.
func totalFileTestLimits() totalFileLimits {
	return totalFileLimits{recordBytes: 2048, storeBytes: 131072, orphanBytes: 65536, scanBytes: 1048576, storeRecords: 128, orphanRecords: 64, scanBatch: 4, scanSteps: 4096}
}

type totalFileFixture struct {
	b        *Budget
	d        *totalTestClock
	i        *totalFileIssuer
	dir      *secretfile.Dir
	releases []func()
}

func newTotalFileFixture(t *testing.T, limits totalFileLimits) *totalFileFixture {
	t.Helper()
	return newTotalFileConfiguredFixture(t, Config{Total: 1}, 4, 1, limits)
}
func newTotalFileConfiguredFixture(t *testing.T, cfg Config, capacity, chunkRows int, limits totalFileLimits) *totalFileFixture {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("directory-inode evidence lease intentionally unsupported on this platform")
	}
	dir, err := secretfile.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &totalFileFixture{dir: dir, d: &totalTestClock{}}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	f.b, f.i, err = newFileSegmentedBudget(cfg, totalKey(81), capacity, chunkRows, f.d.read, dir, limits, [16]byte{19})
	if f.i != nil {
		t.Cleanup(func() {
			f.releaseAll()
			if err := f.i.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal("real total filesystem construction", err)
	}
	return f
}
func (f *totalFileFixture) releaseAll() {
	for _, release := range f.releases {
		release()
	}
}
func (f *totalFileFixture) admit(t *testing.T) func() {
	t.Helper()
	reservation := totalTake(t, f.b, DirectionOutbound)
	f.releases = append(f.releases, reservation.Release)
	return reservation.Release
}
func (f *totalFileFixture) episode(t *testing.T, at time.Duration) {
	t.Helper()
	f.d.set(at, nil)
	release := f.admit(t)
	f.d.set(at+time.Second, nil)
	release()
	release()
}
func (f *totalFileFixture) offer(t *testing.T, at time.Duration) totalSegmentDescriptor {
	t.Helper()
	f.d.set(at, nil)
	q, status := f.b.sealTotalSegment()
	if status != "OFFERED" {
		t.Fatal("real Budget Seal", status)
	}
	return q
}
func totalFileCommit(t *testing.T, f *totalFileFixture, q totalSegmentDescriptor) totalSegmentReceipt {
	t.Helper()
	r, outcome, err := f.i.Commit(q)
	if err != nil || outcome != secretfile.DurableCommitted || r.witness.owner != f.b.fullObservation || r.witness.descriptor != q {
		t.Fatal("actual whole-Q commit", outcome, err)
	}
	return r
}
func totalFileRead(t *testing.T, dir *secretfile.Dir, name string, limit int) []byte {
	t.Helper()
	f, err := dir.OpenFileForRead(name)
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || len(raw) > limit {
		t.Fatal("bounded record read", readErr, closeErr)
	}
	return raw
}
func totalFileWrite(t *testing.T, dir *secretfile.Dir, name string, raw []byte) {
	t.Helper()
	if outcome, err := dir.DurableWrite(name, raw); err != nil || outcome != secretfile.DurableCommitted {
		t.Fatal(outcome, err)
	}
}

// Count actual verifier invocations, never replace its filesystem authority.
type totalFileVerificationCounter struct {
	mu    sync.Mutex
	real  *totalFileIssuer
	calls uint64
}

func (c *totalFileVerificationCounter) verifyTotalSegment(r totalSegmentReceipt) (totalSegmentCommit, bool) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.real.verifyTotalSegment(r)
}
func (c *totalFileVerificationCounter) count() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func totalFileRows(t *testing.T, f *totalFileFixture, q totalSegmentDescriptor) []totalFullTransition {
	t.Helper()
	count, ok := totalChunkCount(q.rowCount, f.i.chunkRows)
	if !ok {
		t.Fatal("chunk count")
	}
	var rows []totalFullTransition
	for index := uint64(0); index < count; index++ {
		raw := totalFileRead(t, f.dir, totalChunkName(q.checkpointID, index), f.i.limits.recordBytes)
		d := totalFileDecoder{raw: raw[:len(raw)-sha256.Size]}
		d.magic(totalChunkMagic)
		d.take(16)
		d.uint()
		d.take(32)
		d.uint()
		d.uint()
		d.uint()
		n := d.integer()
		if n < 0 || n > q.rowCount-len(rows) {
			t.Fatal("test decoder row count")
		}
		for j := 0; j < n; j++ {
			rows = append(rows, d.row())
		}
		if d.err != nil || d.cursor != len(d.raw) {
			t.Fatal("real record oracle", d.err)
		}
	}
	if len(rows) != q.rowCount {
		t.Fatal("incomplete real record oracle")
	}
	return rows
}

func TestTotalFileIssuerActualStore(t *testing.T) {
	t.Run("real-owner-files-rollover-empty-and-fresh-store", func(t *testing.T) {
		{ // owner-chunks-empty-reuse

			f := newTotalFileFixture(t, totalFileTestLimits())
			var first []byte
			vd := &totalTestClock{}
			v1 := totalObserved(t, Config{Total: 1}, vd, 64)
			var lower, upper time.Duration
			add := func(q totalSegmentDescriptor) {
				lo, hi := totalSegmentMeasures(t, q, totalFileRows(t, f, q))
				lower += lo
				upper += hi
			}
			mirrorEpisode := func(at time.Duration) {
				vd.set(at, nil)
				r := totalTake(t, v1, DirectionOutbound)
				vd.set(at+time.Second, nil)
				r.Release()
			}

			for n := 0; n < 6; n++ {
				at := time.Duration(1+3*n) * time.Second
				f.episode(t, at)
				mirrorEpisode(at)
				q := f.offer(t, at+2*time.Second)
				r := totalFileCommit(t, f, q)
				add(q)
				if n == 0 {
					first = totalFileRead(t, f.dir, totalChunkName(q.checkpointID, 0), 2048)
				}
				totalSegmentAck(t, f.b, r, "RETIRED")
				if f.b.Snapshot().Used != 0 || f.b.fullObservation.used != 0 || f.b.fullObservation.sequence != uint64(2*(n+1)) || len(f.b.fullObservation.rows) != 4 || len(f.b.fullObservation.segment.staging) != 1 {
					t.Fatal("real owner rollover changed admission/storage")
				}
			}
			if now := totalFileRead(t, f.dir, totalChunkName(1, 0), 2048); !bytes.Equal(now, first) {
				t.Fatal("committed bytes disappeared/changed after ring reuse")
			}
			f.d.set(30*time.Second, nil)
			release := f.admit(t)
			vd.set(30*time.Second, nil)
			mirrorRelease := totalTake(t, v1, DirectionOutbound).Release
			t.Cleanup(mirrorRelease)
			q := f.offer(t, 31*time.Second)
			r := totalFileCommit(t, f, q)
			add(q)
			entry := q.throughCarry
			totalSegmentAck(t, f.b, r, "RETIRED")
			empty := f.offer(t, 40*time.Second)
			er := totalFileCommit(t, f, empty)
			add(empty)
			if empty.rowCount != 0 || empty.fromSeq != empty.throughSeq || empty.fromCarry != entry || empty.throughCarry != entry {
				t.Fatal("EMPTY/open carry identity")
			}
			if _, err := f.dir.Lstat(totalChunkName(empty.checkpointID, 0)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("EMPTY materialized a fake chunk", err)
			}
			totalSegmentAck(t, f.b, er, "RETIRED")
			f.d.set(50*time.Second, nil)
			release()
			vd.set(50*time.Second, nil)
			mirrorRelease()
			exit := f.offer(t, 51*time.Second)
			xr := totalFileCommit(t, f, exit)
			add(exit)
			totalSegmentAck(t, f.b, xr, "RETIRED")
			if exit.fromCarry != entry || exit.throughCarry.full {
				t.Fatal("entry bracket replaced across file checkpoints")
			}
			vd.set(51*time.Second, nil)
			totalBounds(t, totalComplete(t, v1), lower, upper)
			legacy := f.b.fullSnapshot()
			if legacy.cut.available || legacy.status != "UNKNOWN" || !legacy.pastOnly {
				t.Fatal("V2 suffix leaked as legacy cut")
			}
		}
		{ // fresh-source-refuses-old-store

			f := newTotalFileFixture(t, totalFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			r := totalFileCommit(t, f, q)
			before := totalFileRead(t, f.dir, totalFinalName(1), 2048)
			if err := f.i.Close(); err != nil {
				t.Fatal(err)
			}
			clock := &totalTestClock{}
			h, newIssuer, err := newFileSegmentedBudget(Config{Total: 1}, totalKey(81), 4, 1, clock.read, f.dir, totalFileTestLimits(), [16]byte{19})
			if !errors.Is(err, errTotalStoreNotFresh) || h == nil || newIssuer == nil {
				t.Fatal("old records adopted by fresh source", err)
			}
			defer func() {
				if err := newIssuer.Close(); err != nil {
					t.Error(err)
				}
			}()
			clock.set(time.Second, nil)
			reservation := totalTake(t, h, DirectionOutbound)
			t.Cleanup(reservation.Release)
			release := reservation.Release
			clock.set(2*time.Second, nil)
			release()
			clock.set(3*time.Second, nil)
			sameQ, status := h.sealTotalSegment()
			if status != "OFFERED" || sameQ != q {
				t.Fatal("identical-text negative fixture differs", status)
			}
			if _, err := newIssuer.warmWitnessLocked(sameQ); err != nil {
				t.Fatal(err)
			}
			totalSegmentAck(t, h, r, "REFUSED")
			if commit, valid := newIssuer.verifyTotalSegment(r); valid || commit != (totalSegmentCommit{}) {
				t.Fatal("serialized tokens revived runtime authority")
			}
			if h.fullObservation.used != 2 || h.Snapshot().Used != 0 || !bytes.Equal(before, totalFileRead(t, f.dir, totalFinalName(1), 2048)) {
				t.Fatal("new source changed old evidence or its own real gate")
			}
		}
	})
	t.Run("total-canonical-identity-and-opaque-authority", func(t *testing.T) {
		{ // record-and-checked-codec

			f := newTotalFileFixture(t, totalFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			w, err := f.i.warmWitnessLocked(q)
			if err != nil {
				t.Fatal(err)
			}
			stream, status := f.b.beginTotalSegmentStream(q)
			if status != "STREAMING" {
				t.Fatal(status)
			}
			loan, _ := f.b.borrowTotalSegmentChunk(stream)
			chunk, status := f.b.copyTotalSegmentChunk(loan)
			if status != "COPIED" {
				t.Fatal(status)
			}
			if raw, err := totalChunkRecord(w, 0, chunk, 1, 64); raw != nil || !errors.Is(err, errTotalFileBudget) {
				t.Fatal("ignored Write overflow escaped checked capture", err)
			}
			raw, err := totalChunkRecord(w, 0, chunk, 1, 2048)
			if err != nil || !totalCheckedRecord(raw) {
				t.Fatal(err)
			}
			if !f.b.releaseTotalSegmentChunk(loan) {
				t.Fatal("release actual staging")
			}
			if len(raw) > 2048 {
				t.Fatal("record cap")
			}
			// Shared source SHA format is preserved, not a second row codec.
			capture := newTotalFileCapture(2048)
			writeTotalSegmentHeader(capture, q)
			hash := sha256.New()
			writeTotalSegmentHeader(hash, q)
			if capture.err != nil || !bytes.Equal(capture.Sum(nil), hash.Sum(nil)) {
				t.Fatal("canonical header drift")
			}
			for _, size := range []int{0, 1, 32} {
				c := newTotalFileCapture(size)
				writeTotalSegmentHeader(c, q)
				if b, err := c.finish(); b != nil || err == nil {
					t.Fatal("malformed record limit")
				}
			}
			for _, name := range []string{"q0000000000000000.final", "q0000000000000001-c0000000000000000.chunkx", "other", "q0000000000000001-cGGGGGGGGGGGGGGGG.chunk"} {
				if _, _, _, ok := totalFileName(name); ok {
					t.Fatal("foreign/malformed name accepted", name)
				}
			}
		}
		{ // wrong-source-session-proof

			f := newTotalFileFixture(t, totalFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			r := totalFileCommit(t, f, q)
			for _, mutate := range []func(*totalSegmentReceipt){func(r *totalSegmentReceipt) { r.proof[0]++ }, func(r *totalSegmentReceipt) { r.witness.session[0]++ }, func(r *totalSegmentReceipt) { r.witness.descriptor.previousDigest[0]++ }, func(r *totalSegmentReceipt) { r.witness.owner = nil }} {
				wrong := r
				mutate(&wrong)
				totalSegmentAck(t, f.b, wrong, "REFUSED")
				if _, valid := f.i.verifyTotalSegment(wrong); valid {
					t.Fatal("raw caller witness/proof supplied authority")
				}
			}
			totalSegmentAck(t, f.b, r, "RETIRED")
			if err := f.i.Close(); err != nil {
				t.Fatal(err)
			}
			if _, valid := f.i.verifyTotalSegment(r); valid {
				t.Fatal("closed issuer minted authority")
			}
		}

		{ // Mutated records remain checksum-valid: actual source/store identity rejects them.
			f := newTotalFileConfiguredFixture(t, Config{Total: 3, OutboundReserve: 1, MaxOutbound: 3, MaxInbound: 2, MaxAuxiliary: 2}, 4, 1, totalFileTestLimits())
			a := totalTake(t, f.b, DirectionOutbound)
			f.releases = append(f.releases, a.Release)
			b := totalTake(t, f.b, DirectionInbound)
			f.releases = append(f.releases, b.Release)
			f.d.set(time.Second, nil)
			c := totalTake(t, f.b, DirectionAuxiliary)
			f.releases = append(f.releases, c.Release)
			if _, err := f.b.Reserve(DirectionOutbound); !errors.Is(err, ErrTotalExhausted) {
				t.Fatal("original total refusal", err)
			}
			f.d.set(2*time.Second, nil)
			c.Release()
			c.Release()
			q := f.offer(t, 3*time.Second)
			r := totalFileCommit(t, f, q)
			name := totalFinalName(q.checkpointID)
			original := totalFileRead(t, f.dir, name, 2048)
			session, decoded, digest, layout, count, chain, err := totalDecodeFinal(original)
			if err != nil || decoded != q || session != r.witness.session || digest != r.witness.digest {
				t.Fatal("total file decode", err)
			}
			for _, mutate := range []func(*totalSegmentDescriptor){
				func(q *totalSegmentDescriptor) { q.cfg.Total++ },
				func(q *totalSegmentDescriptor) { q.cfg.OutboundReserve++ },
				func(q *totalSegmentDescriptor) { q.cfg.MaxOutbound++ },
				func(q *totalSegmentDescriptor) { q.cfg.MaxInbound++ },
				func(q *totalSegmentDescriptor) { q.cfg.MaxAuxiliary++ },
				func(q *totalSegmentDescriptor) { q.key.anchor += "-foreign" },
				func(q *totalSegmentDescriptor) { q.key.epoch[0]++ },
				func(q *totalSegmentDescriptor) { q.key.clockDomain[0]++ },
				func(q *totalSegmentDescriptor) { q.key.configuration[0]++ },
				func(q *totalSegmentDescriptor) { q.previousDigest[0]++ },
				func(q *totalSegmentDescriptor) { q.fromAt++ },
				func(q *totalSegmentDescriptor) { q.throughSeq++ },
			} {
				wrong := r.witness
				mutate(&wrong.descriptor)
				raw, err := totalFinalRecord(wrong, layout, count, chain, 2048)
				if err != nil || !totalCheckedRecord(raw) {
					t.Fatal("valid-checksum negative", err)
				}
				totalFileWrite(t, f.dir, name, raw)
				totalSegmentAck(t, f.b, r, "REFUSED")
				if f.b.fullObservation.used != 2 {
					t.Fatal("invalid exact descriptor retired real rows")
				}
				totalFileWrite(t, f.dir, name, original)
			}
			// The exact canonical and envelope domains are not interchangeable.
			for _, domain := range []string{totalCanonicalMagic, totalFinalMagic} {
				foreign := bytes.Replace(original, []byte(domain), []byte("corsa-k5-handshake-foreign-domain"), 1)
				sum := sha256.Sum256(foreign[:len(foreign)-sha256.Size])
				copy(foreign[len(foreign)-sha256.Size:], sum[:])
				if !totalCheckedRecord(foreign) {
					t.Fatal("domain negative checksum")
				}
				totalFileWrite(t, f.dir, name, foreign)
				totalSegmentAck(t, f.b, r, "REFUSED")
				totalFileWrite(t, f.dir, name, original)
			}
			malformed := append([]byte(nil), original...)
			offset := len(totalFinalMagic) + 16 + len(totalCanonicalMagic)
			binary.LittleEndian.PutUint64(malformed[offset:offset+8], totalMaxUint)
			malformedSum := sha256.Sum256(malformed[:len(malformed)-sha256.Size])
			copy(malformed[len(malformed)-sha256.Size:], malformedSum[:])
			totalFileWrite(t, f.dir, name, malformed)
			totalSegmentAck(t, f.b, r, "REFUSED")
			totalFileWrite(t, f.dir, name, original)
			identity := totalFileRead(t, f.dir, "identity", 2048)
			other := append([]byte(nil), identity...)
			other[len(totalStoreMagic)] ^= 1
			sum := sha256.Sum256(other[:len(other)-sha256.Size])
			copy(other[len(other)-sha256.Size:], sum[:])
			totalFileWrite(t, f.dir, "identity", other)
			totalSegmentAck(t, f.b, r, "REFUSED")
			totalFileWrite(t, f.dir, "identity", identity)
			totalSegmentAck(t, f.b, r, "RETIRED")
			a.Release()
			b.Release()
		}
	})
	t.Run("strict-confirmation-tristate-and-same-live-recovery", func(t *testing.T) {
		{ // final-indeterminate-visible-readback

			f := newTotalFileFixture(t, totalFileTestLimits())
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
			if err == nil || outcome != secretfile.DurableIndeterminate || r != (totalSegmentReceipt{}) || f.i.committedID != 0 {
				t.Fatal("uncertain final minted receipt", outcome, err)
			}
			visible := totalFileRead(t, f.dir, totalFinalName(q.checkpointID), 2048)
			if !totalCheckedRecord(visible) {
				t.Fatal("fixture final is not visible/hash valid")
			}
			if status, _ := f.b.retireVerifiedTotalSegment(); status != "REFUSED" {
				t.Fatal("readability alone retired source")
			}
			// Exhaust the actual fresh-confirmation budget: present/hash valid is
			// still not enough. This is a budget/control fault, not a syscall fault.
			budget := f.i.limits.scanBytes
			f.i.limits.scanBytes = 1
			if r, outcome, err := f.i.Commit(q); err == nil || r != (totalSegmentReceipt{}) || f.i.committedID != 0 || outcome != secretfile.DurableIndeterminate {
				t.Fatal("visible bytes promoted/forgotten without fresh confirmation", outcome, err)
			}
			f.i.limits.scanBytes = budget
			// Explicit negative namespace fault after an uncertain publication:
			// a later missing-final/rewrite refusal must not downgrade logical Q.
			if err := f.dir.Remove(totalFinalName(q.checkpointID)); err != nil {
				t.Fatal(err)
			}
			f.i.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
				if strings.HasSuffix(name, ".final") {
					return secretfile.DurableNotCommitted, errors.New("test retry final refused")
				}
				return real(name, raw)
			}
			if retry, outcome, err := f.i.Commit(q); err == nil || retry != (totalSegmentReceipt{}) || outcome != secretfile.DurableIndeterminate || f.i.committedID != 0 {
				t.Fatal("late final refusal downgraded prior visibility", outcome, err)
			}
			f.i.writeRecord = real
			r = totalFileCommit(t, f, q)
			if f.i.committedID != q.checkpointID {
				t.Fatal("same-live-source recovery failed")
			}
			totalSegmentAck(t, f.b, r, "RETIRED")
			if !bytes.Equal(visible, totalFileRead(t, f.dir, totalFinalName(q.checkpointID), 2048)) {
				t.Fatal("retry overwrote immutable final bytes")
			}
			knownID := f.i.committedID
			f.i.limits.scanBytes = 1
			if retry, outcome, err := f.i.Commit(q); err == nil || retry != (totalSegmentReceipt{}) || outcome != secretfile.DurableIndeterminate || f.i.committedID != knownID {
				t.Fatal("failed revalidation undid historical commit", outcome, err)
			}
			f.i.limits.scanBytes = budget

		}
		{ // returned-status-paths

			for _, fault := range []string{"not-committed", "committed-error", "fake-success"} {
				f := newTotalFileFixture(t, totalFileTestLimits())
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
				if err == nil || r != (totalSegmentReceipt{}) || f.i.committedID != 0 {
					t.Fatal("status alone accepted", fault, outcome, err)
				}
				want := secretfile.DurableIndeterminate
				if fault == "not-committed" {
					want = secretfile.DurableNotCommitted
				}
				if outcome != want {
					t.Fatal("tri-state outcome", fault, outcome, want)
				}
				if f.b.fullObservation.used != 2 || f.b.Snapshot().Used != 0 {
					t.Fatal("unconfirmed status altered source/admission")
				}
				f.i.writeRecord = real
				r = totalFileCommit(t, f, q)
				totalSegmentAck(t, f.b, r, "RETIRED")
			}
		}
		{ // missing-corrupt-conflict

			for _, fault := range []string{"missing", "corrupt", "conflict"} {
				f := newTotalFileFixture(t, totalFileTestLimits())
				f.episode(t, time.Second)
				q := f.offer(t, 3*time.Second)
				r := totalFileCommit(t, f, q)
				name := totalChunkName(q.checkpointID, 0)
				if fault == "missing" {
					if err := f.dir.Remove(name); err != nil {
						t.Fatal(err)
					}
				} else {
					raw := totalFileRead(t, f.dir, name, 2048)
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
				totalSegmentAck(t, f.b, r, "REFUSED")
				if _, _, err := f.i.Commit(q); err == nil || f.b.fullObservation.used != 2 {
					t.Fatal("missing/conflicting committed evidence accepted", fault, err)
				}
			}
		}
	})
	t.Run("finite-accounting-namespace-and-no-GC", func(t *testing.T) {
		{ // store-and-orphan-headroom

			for _, kind := range []string{"records", "orphans", "bytes"} {
				limits := totalFileTestLimits()
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
				f := newTotalFileFixture(t, limits)
				f.episode(t, time.Second)
				q := f.offer(t, 3*time.Second)
				r, _, err := f.i.Commit(q)
				if r != (totalSegmentReceipt{}) || err == nil || f.i.committedID != 0 || f.b.fullObservation.used != 2 || f.b.Snapshot().Used != 0 {
					t.Fatal("budget exhaustion minted receipt/altered gate", kind, err)
				}
				// No pruning or implicit capacity increase on repeated attempts.
				for retry := 0; retry < 2; retry++ {
					if r, _, err := f.i.Commit(q); err == nil || r != (totalSegmentReceipt{}) {
						t.Fatal("repeat silently freed/increased budget", kind)
					}
				}
				if _, err := f.dir.Lstat(totalFinalName(q.checkpointID)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("cap exhaustion wrote final", kind, err)
				}
			}
		}
		{ // recovery-complete-or-refuse

			f := newTotalFileFixture(t, totalFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			r := totalFileCommit(t, f, q)
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
				totalSegmentAck(t, f.b, r, "REFUSED")
				if f.b.fullObservation.used != 2 || f.b.fullObservation.segment.retiredThrough != 0 {
					t.Fatal("partial recovery certified prefix", fault)
				}
				f.i.limits = old
			}
			totalSegmentAck(t, f.b, r, "RETIRED")
		}
		{ // namespace-membership-and-no-GC

			for _, kind := range []string{"past-surplus", "current-surplus", "empty-surplus", "missing-declared-with-surplus", "orphan-headroom"} {
				f := newTotalFileFixture(t, totalFileTestLimits())
				f.episode(t, time.Second)
				q := f.offer(t, 3*time.Second)
				var receipt totalSegmentReceipt
				if kind == "past-surplus" || kind == "missing-declared-with-surplus" {
					receipt = totalFileCommit(t, f, q)
					totalSegmentAck(t, f.b, receipt, "RETIRED")
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
					receipt = totalFileCommit(t, f, q)
					totalSegmentAck(t, f.b, receipt, "RETIRED")
					q = f.offer(t, 4*time.Second)
					w, err = f.i.warmWitnessLocked(q)
					if err != nil || q.rowCount != 0 {
						t.Fatal("EMPTY fixture", err)
					}
				}
				// A structurally valid, unreferenced record is never a committed
				// member merely because its qid is older/current.
				extra := totalSegmentChunk{descriptor: q, offset: 0, rows: nil, final: true}
				raw, err := totalChunkRecord(w, 999, extra, 1, 2048)
				if err != nil {
					t.Fatal(err)
				}
				name := totalChunkName(q.checkpointID, 999)
				if out, err := f.dir.DurableWrite(name, raw); out != secretfile.DurableCommitted || err != nil {
					t.Fatal(out, err)
				}
				if kind == "missing-declared-with-surplus" {
					if err := f.dir.Remove(totalChunkName(q.checkpointID, 0)); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "orphan-headroom" {
					f.i.limits.orphanRecords = 1
				}
				if kind == "past-surplus" || kind == "missing-declared-with-surplus" {
					q = f.offer(t, 4*time.Second)
				}
				if r, _, err := f.i.Commit(q); err == nil || r != (totalSegmentReceipt{}) {
					t.Fatal("surplus/unknown namespace minted receipt", kind, err)
				}
				if now := totalFileRead(t, f.dir, name, 2048); !bytes.Equal(now, raw) {
					t.Fatal("unconfirmed artifact auto-GC/overwrite", kind)
				}
			}
		}

		{ // Valid opaque temp residue is charged, never checkpoint evidence.
			f := newTotalFileFixture(t, totalFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			w, err := f.i.warmWitnessLocked(q)
			if err != nil {
				t.Fatal(err)
			}
			name := secretfile.TempPrefix + "retained-residual"
			body := []byte("not-a-checkpoint")
			totalFileWrite(t, f.dir, name, body)
			budget := f.i.budget()
			f.i.mu.Lock()
			account, err := f.i.accountLocked(&budget, w)
			f.i.mu.Unlock()
			if err != nil || account.orphans != 1 || account.orphanBytes != int64(len(body)) {
				t.Fatal("residual escaped orphan accounting", account, err)
			}
			f.i.limits.orphanRecords = 1
			for retry := 0; retry < 2; retry++ {
				if r, _, err := f.i.Commit(q); err == nil || r != (totalSegmentReceipt{}) {
					t.Fatal("temp residual refunded on retry", err)
				}
			}
			if !bytes.Equal(totalFileRead(t, f.dir, name, 2048), body) {
				t.Fatal("residual silently cleaned to fit")
			}
		}
		{
			f := newTotalFileFixture(t, totalFileTestLimits())
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			totalFileWrite(t, f.dir, "unknown-record", []byte("unknown"))
			if r, _, err := f.i.Commit(q); err == nil || r != (totalSegmentReceipt{}) {
				t.Fatal("unknown namespace accepted", err)
			}
			if !bytes.Equal(totalFileRead(t, f.dir, "unknown-record", 2048), []byte("unknown")) {
				t.Fatal("unknown record silently pruned")
			}
		}
	})
	t.Run("actual-file-proof-owner-loans-and-quiescent-retire", func(t *testing.T) {
		f := newTotalFileFixture(t, totalFileTestLimits())
		g := newTotalFileFixture(t, totalFileTestLimits())
		readFixture := newTotalFileFixture(t, totalFileTestLimits())
		var unblockers []func()
		// One cleanup joins ALL workers below against the existing shared
		// absolute 3s cleanup deadline, after every blocker/release is freed.
		join := totalCleanup(t, func() {
			for _, release := range unblockers {
				release()
			}
		}, f.releaseAll, g.releaseAll, readFixture.releaseAll)
		{
			counterF := &totalFileVerificationCounter{real: f.i}
			f.b.fullObservation.segment.issuer = counterF
			q := f.offer(t, 10*time.Second)
			receipt := totalFileCommit(t, f, q)
			resume, done := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblockers = append(unblockers, func() { once.Do(func() { close(resume) }) })
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
			headerVerifiedCalls := counterF.count()
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
			if counterF.count() != headerVerifiedCalls {
				t.Fatal("HEADER Retire repeated issuer verification")
			}
			if _, status := f.b.borrowTotalSegmentChunk(a.token); status != "REFUSED" {
				t.Fatal("stale EMPTY attempt", status)
			}
			q2 := f.offer(t, 20*time.Second)
			r2 := totalFileCommit(t, f, q2)
			totalSegmentAck(t, f.b, receipt, "ALREADY_RETIRED")
			if !f.b.fullObservation.segment.qActive || f.b.fullObservation.segment.q != q2 {
				t.Fatal("same row cursor replay retired later EMPTY")
			}
			totalSegmentAck(t, f.b, r2, "RETIRED")

			counterG := &totalFileVerificationCounter{real: g.i}
			g.b.fullObservation.segment.issuer = counterG
			g.episode(t, time.Second)
			qg := g.offer(t, 5*time.Second)
			old := totalFileCommit(t, g, qg)
			a2, status := g.b.beginTotalSegmentStream(qg)
			if status != "STREAMING" {
				t.Fatal(status)
			}
			copyResume, copyDone := make(chan struct{}), make(chan struct{})
			var copyOnce sync.Once
			unblockers = append(unblockers, func() { copyOnce.Do(func() { close(copyResume) }) })
			p2 := &totalSegmentPausedHash{Hash: g.b.fullObservation.segment.streamHash, started: make(chan struct{}), resume: copyResume}
			p2.armed.Store(true)
			g.b.fullObservation.segment.streamHash = p2
			loan, status := g.b.borrowTotalSegmentChunk(a2)
			if status != "BORROWED" {
				t.Fatal(status)
			}
			copied := make(chan string, 1)
			join(copyDone)
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
			copyVerifiedCalls := counterG.count()
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
			if counterG.count() != copyVerifiedCalls {
				t.Fatal("copy/staging Retire repeated issuer verification")
			}
		}

		{ // A real provider read also pins already-verified file evidence.
			f := readFixture
			f.episode(t, time.Second)
			q := f.offer(t, 3*time.Second)
			receipt := totalFileCommit(t, f, q)
			counter := &totalFileVerificationCounter{real: f.i}
			f.b.fullObservation.segment.issuer = counter
			started, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			unblockers = append(unblockers, unblock)
			join(done)
			next := f.d.count() + 1
			f.d.set(4*time.Second, nil)
			f.d.after = func(n int) {
				if n == next {
					close(started)
					<-resume
				}
			}
			workerErr := make(chan error, 1)
			go func() {
				defer close(done)
				r, err := f.b.Reserve(DirectionOutbound)
				if err == nil {
					r.Release()
				}
				workerErr <- err
			}()
			totalWait(t, started)
			totalSegmentLeafFree(t, f.b)
			if status, reason := f.b.acknowledgeTotalSegment(receipt); status != "DEFERRED" || reason != "read-or-completion" {
				t.Fatal("real file proof ignored provider pin", status, reason)
			}
			calls := counter.count()
			unblock()
			totalWait(t, done)
			if err := <-workerErr; err != nil {
				t.Fatal(err)
			}
			if status, _ := f.b.retireVerifiedTotalSegment(); status != "RETIRED" {
				t.Fatal("read release did not permit verified retirement", status)
			}
			if counter.count() != calls {
				t.Fatal("quiescent Retire verified again")
			}
		}
	})

	t.Run("disabled-invalid-backend-and-borrowed-lifetime", func(t *testing.T) {
		calls := 0
		clock := func() (time.Duration, error) { calls++; return 0, nil }
		// This Dir has no backing Root: Total0 must avoid every filesystem method.
		b, i, err := newFileSegmentedBudget(Config{Total: 0, MaxOutbound: 1}, totalSourceKey{}, -1, -1, clock, &secretfile.Dir{}, totalFileLimits{}, [16]byte{})
		if err != nil || b == nil || i == nil || calls != 0 || i.lease != nil || i.writeRecord != nil || b.fullObservation.rows != nil || b.fullObservation.segment != nil || b.fullSnapshot().status != "DISABLED" {
			t.Fatal("Total0 observation work", calls, err)
		}
		r := totalTake(t, b, DirectionOutbound)
		if _, err := b.Reserve(DirectionOutbound); !errors.Is(err, ErrDirectionLimit) {
			t.Fatal("Total0 changed original direction policy", err)
		}
		r.Release()
		if err := i.Close(); err != nil {
			t.Fatal(err)
		}
		if invalid, issuer, err := newFileSegmentedBudget(Config{Total: -1}, totalKey(1), 2, 1, clock, nil, totalFileLimits{}, [16]byte{1}); invalid != nil || issuer != nil || !errors.Is(err, ErrInvalidConfig) || calls != 0 {
			t.Fatal("invalid original Config", err)
		}
		for _, bad := range []string{"nil-directory", "bad-record-limit"} {
			limits := totalFileTestLimits()
			if bad == "bad-record-limit" {
				limits.recordBytes = sha256.Size
			}
			b, issuer, err := newFileSegmentedBudget(Config{Total: 1}, totalKey(7), 2, 1, clock, nil, limits, [16]byte{1})
			if err == nil || b == nil || issuer == nil {
				t.Fatal("bad backend changed admission startup", bad, err)
			}
			reservation := totalTake(t, b, DirectionOutbound)
			reservation.Release()
			if snapshot := b.fullSnapshot(); snapshot.status != "UNKNOWN" || snapshot.rows != nil || snapshot.cut.available {
				t.Fatal("backend failure leaked legacy cut", bad, snapshot)
			}
			if receipt, _, err := issuer.Commit(totalSegmentDescriptor{}); err == nil || receipt != (totalSegmentReceipt{}) {
				t.Fatal("unavailable backend minted proof")
			}
			if err := issuer.Close(); err != nil {
				t.Fatal(err)
			}
		}
		ordinary, err := New(Config{Total: 1})
		if err != nil || ordinary.fullObservation != nil {
			t.Fatal("ordinary default activated", err)
		}
		// Close serializes with a real write; Dir is borrowed and remains usable.
		f := newTotalFileFixture(t, totalFileTestLimits())
		f.episode(t, time.Second)
		q := f.offer(t, 3*time.Second)
		real := f.i.writeRecord
		started, resume, commitDone, closeDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		join := totalCleanup(t, unblock, f.releaseAll)
		join(commitDone)
		var once sync.Once
		f.i.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
			once.Do(func() { close(started); <-resume })
			return real(name, raw)
		}
		type commitResult struct {
			r       totalSegmentReceipt
			outcome secretfile.DurableCommit
			err     error
		}
		committed := make(chan commitResult, 1)
		closed := make(chan error, 1)
		go func() { defer close(commitDone); r, o, e := f.i.Commit(q); committed <- commitResult{r, o, e} }()
		totalWait(t, started)
		totalSegmentLeafFree(t, f.b)
		if f.i.mu.TryLock() {
			f.i.mu.Unlock()
			t.Fatal("write not inside issuer ownership")
		}
		join(closeDone)
		go func() { defer close(closeDone); closed <- f.i.Close() }()
		unblock()
		totalWait(t, commitDone)
		totalWait(t, closeDone)
		result := <-committed
		if result.err != nil || result.outcome != secretfile.DurableCommitted {
			t.Fatal("inflight write lost before joined close", result.outcome, result.err)
		}
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if _, valid := f.i.verifyTotalSegment(result.r); valid {
			t.Fatal("closed issuer retained commit authority")
		}
		totalFileWrite(t, f.dir, "caller-after-issuer-close", []byte("caller still owns directory"))
		if !bytes.Equal(totalFileRead(t, f.dir, "caller-after-issuer-close", 2048), []byte("caller still owns directory")) {
			t.Fatal("issuer closed borrowed Dir")
		}
	})
}
