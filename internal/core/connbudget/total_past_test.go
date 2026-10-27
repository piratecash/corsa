package connbudget

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// Tiny caller-selected fixtures only, not deployment or RAM limits.
func totalPastTestLimits() TotalPastLimits {
	return TotalPastLimits{RecordBytes: 2048, StoreBytes: 131072, OrphanBytes: 65536, ReadBytes: 4194304, OutputBytes: 1048576, OutputRows: 1024, StoreRecords: 128, OrphanRecords: 64, ScanBatch: 4, WorkSteps: 8192, BatchRows: 1, OutputCheckpoints: 32}
}

type totalPastCaptureFixture struct {
	path        string
	dir         *secretfile.Dir
	cfg         Config
	key         TotalObservationKey
	b           *Budget
	o           *TotalFileObservation
	clock       *totalTestClock
	expected    TotalPastExpected
	checkpoints []TotalFileCheckpoint
	rows        []TotalObservationTransition
	releases    []func()
}

func newTotalPastCaptureFixture(t *testing.T) *totalPastCaptureFixture {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("directory-inode lease unsupported")
	}
	f := &totalPastCaptureFixture{path: t.TempDir(), clock: &totalTestClock{}, cfg: Config{Total: 3, OutboundReserve: 1, MaxOutbound: 2, MaxInbound: 1, MaxAuxiliary: 2}, key: TotalObservationKey{Anchor: strings.Repeat("long-total-anchor/", 12), Epoch: [16]byte{1}, ClockDomain: [16]byte{2}, Configuration: [16]byte{3}}}
	var err error
	f.dir, err = secretfile.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.dir.Close(); err != nil {
			t.Error(err)
		}
	})
	f.b, f.o, err = NewWithTotalFileObservation(f.cfg, f.key, 2, 1, f.clock.read, f.dir, exportFileLimits(), [16]byte{191})
	if f.o != nil {
		t.Cleanup(func() {
			for _, release := range f.releases {
				release()
			}
			if err := f.o.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil || f.o == nil || f.o.Err() != nil {
		t.Fatal("actual total file source", err)
	}
	// These two real reservations leave one shared slot for outbound. Both
	// directions contribute to actual total, never a synthetic owner counter.
	for _, d := range []Direction{DirectionInbound, DirectionAuxiliary} {
		r := totalTake(t, f.b, d)
		f.releases = append(f.releases, r.Release)
	}
	return f
}
func (f *totalPastCaptureFixture) rememberRow(t *testing.T) {
	t.Helper()
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	o := f.b.fullObservation
	for _, r := range o.rows {
		if r.sequence == o.sequence && r.done && r.valid {
			f.rows = append(f.rows, TotalObservationTransition{Sequence: r.sequence, Full: r.full, Count: r.count, Lower: r.lower, Upper: r.upper, Done: r.done, Valid: r.valid})
			return
		}
	}
	t.Fatal("actual Budget row absent")
}
func (f *totalPastCaptureFixture) enter(t *testing.T, at time.Duration) func() {
	t.Helper()
	f.clock.set(at, nil)
	r := totalTake(t, f.b, DirectionOutbound)
	f.releases = append(f.releases, r.Release)
	f.rememberRow(t)
	return r.Release
}
func (f *totalPastCaptureFixture) leave(t *testing.T, release func(), at time.Duration) {
	t.Helper()
	f.clock.set(at, nil)
	release()
	calls := f.clock.count()
	release()
	if f.clock.count() != calls {
		t.Fatal("duplicate Release bypassed original CAS")
	}
	f.rememberRow(t)
}
func (f *totalPastCaptureFixture) capture(t *testing.T, at time.Duration) TotalFileObservationResult {
	t.Helper()
	f.clock.set(at, nil)
	v := exportCapture(t, f.o)
	f.expected = TotalPastExpected{Checkpoint: v.Checkpoint, Digest: v.Digest, Capacity: 2, ChunkCapacity: 1}
	f.checkpoints = append(f.checkpoints, v.Checkpoint)
	return v
}
func (f *totalPastCaptureFixture) closeWriter(t *testing.T) {
	t.Helper()
	if err := f.o.Close(); err != nil {
		t.Fatal(err)
	}
}
func (f *totalPastCaptureFixture) reopen(t *testing.T) *secretfile.Dir {
	t.Helper()
	d, err := secretfile.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}
func totalPastSimpleStore(t *testing.T) *totalPastCaptureFixture {
	t.Helper()
	f := newTotalPastCaptureFixture(t)
	release := f.enter(t, time.Second)
	f.leave(t, release, 2*time.Second)
	f.capture(t, 3*time.Second)
	f.closeWriter(t)
	return f
}
func totalPastOpen(t *testing.T, d *secretfile.Dir, e TotalPastExpected, l TotalPastLimits) *TotalPastReader {
	t.Helper()
	r, err := OpenTotalPast(d, e, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = r.Close() })
	return r
}
func totalPastCollect(t *testing.T, r *TotalPastReader) ([]TotalPastBatch, TotalPastResult) {
	t.Helper()
	var batches []TotalPastBatch
	for {
		b, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, b)
	}
	result, err := r.Close()
	if err != nil || result.Status != "MATCHED_EXPECTED" || result.Qualification != "UNKNOWN" || result.AttributionBound || !result.PastOnly {
		t.Fatal(result, err)
	}
	return batches, result
}
func totalPastRead(t *testing.T, d *secretfile.Dir, name string) []byte {
	t.Helper()
	return totalFileRead(t, d, name, 2048)
}
func totalPastWrite(t *testing.T, d *secretfile.Dir, name string, raw []byte) {
	t.Helper()
	if status, err := d.DurableWrite(name, raw); err != nil || status != secretfile.DurableCommitted {
		t.Fatal(status, err)
	}
}
func totalPastReseal(raw []byte) {
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	copy(raw[len(raw)-sha256.Size:], sum[:])
}
func totalPastFiles(t *testing.T, path string) map[string][32]byte {
	t.Helper()
	names, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string][32]byte{}
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(path, name.Name()))
		if err != nil {
			t.Fatal(err)
		}
		m[name.Name()] = sha256.Sum256(raw)
	}
	return m
}
func totalPastRefusedOpen(t *testing.T, d *secretfile.Dir, e TotalPastExpected, l TotalPastLimits) {
	t.Helper()
	if r, err := OpenTotalPast(d, e, l); err == nil || r != nil {
		if r != nil {
			_, _ = r.Close()
		}
		t.Fatal("partial/mismatched input opened", err)
	}
}
func TestTotalPastExpectedReader(t *testing.T) {
	t.Run("real-Capture-reopen-carry-empty-and-ring-reuse", func(t *testing.T) {
		f := newTotalPastCaptureFixture(t)
		if len(f.key.Anchor) <= 128 {
			t.Fatal("fixture must exercise total record-bounded long anchor")
		}
		// Class and protected-spare refusals happen while actual total is B-1.
		beforeRefusal := f.b.Snapshot()
		beforeSequence := f.b.fullObservation.sequence
		if beforeRefusal.Used != f.cfg.Total-1 {
			t.Fatal("missing real shared spare", beforeRefusal)
		}
		if r, err := f.b.Reserve(DirectionInbound); r != nil || !errors.Is(err, ErrDirectionLimit) {
			if r != nil {
				r.Release()
			}
			t.Fatal("actual direction refusal", err)
		}
		if r, err := f.b.Reserve(DirectionAuxiliary); r != nil || !errors.Is(err, ErrOutboundReserved) {
			if r != nil {
				r.Release()
			}
			t.Fatal("actual outbound protection", err)
		}
		if f.b.Snapshot().Used != beforeRefusal.Used || f.b.fullObservation.sequence != beforeSequence {
			t.Fatal("refusal invented total transition")
		}
		release := f.enter(t, time.Second)
		if f.b.Snapshot().Used != f.cfg.Total || len(f.rows) != 1 || !f.rows[0].Full || f.rows[0].Count != f.cfg.Total {
			t.Fatal("actual outbound did not fill total B")
		}

		first := f.capture(t, time.Second)
		empty := f.capture(t, 2*time.Second)
		if empty.Checkpoint.Rows != 0 || empty.Checkpoint.ThroughSequence != first.Checkpoint.ThroughSequence || empty.Checkpoint.ID == first.Checkpoint.ID || empty.Checkpoint.FromEntry != first.Checkpoint.ThroughEntry {
			t.Fatal("actual EMPTY temporal Q", empty)
		}
		f.leave(t, release, 3*time.Second)
		if f.b.Snapshot().Used != f.cfg.Total-1 || len(f.rows) != 2 || f.rows[1].Full || f.rows[1].Count != f.cfg.Total-1 {
			t.Fatal("successful reservation rollback must retain B to B-1 interval")
		}
		f.capture(t, 3*time.Second)
		for n := 0; n < 2; n++ {
			at := time.Duration(4+2*n) * time.Second
			release = f.enter(t, at)
			f.leave(t, release, at+time.Second)
			f.capture(t, at+time.Second)
		}
		if len(f.b.fullObservation.rows) != 2 || f.b.fullObservation.sequence != 6 {
			t.Fatal("real bounded ring fixture")
		}
		expected := f.expected
		f.closeWriter(t)
		before := totalPastFiles(t, f.path)
		borrowed := f.reopen(t)
		r := totalPastOpen(t, borrowed, expected, totalPastTestLimits())
		batches, result := totalPastCollect(t, r)
		var rows []TotalObservationTransition
		var checkpoints []TotalFileCheckpoint
		for _, b := range batches {
			if len(b.Rows) > 1 {
				t.Fatal("whole-Q output")
			}
			rows = append(rows, b.Rows...)
			if b.Final {
				checkpoints = append(checkpoints, b.Checkpoint)
			}
		}
		if !reflect.DeepEqual(rows, f.rows) || !reflect.DeepEqual(checkpoints, f.checkpoints) || result.Expected != expected || result.DeliveredRows != int64(len(f.rows)) || result.DeliveredCheckpoints != uint64(len(f.checkpoints)) {
			t.Fatal("real handoff differs", result, rows, checkpoints)
		}
		if !reflect.DeepEqual(before, totalPastFiles(t, f.path)) {
			t.Fatal("read-only path changed namespace/bodies")
		}
		if _, err := borrowed.Lstat("identity"); err != nil {
			t.Fatal("reader closed borrowed directory", err)
		}
	})
	t.Run("exact-temporal-terminal-source-and-untrusted-reference", func(t *testing.T) {
		f := newTotalPastCaptureFixture(t)
		f.enter(t, time.Second)
		first := f.capture(t, time.Second)
		last := f.capture(t, 2*time.Second)
		expected := f.expected
		f.closeWriter(t)
		// Removing a whole EMPTY suffix leaves the same row cursor and a valid
		// shorter chain, but cannot satisfy the retained exact temporal reference.
		name := totalFinalName(last.Checkpoint.ID)
		raw := totalPastRead(t, f.dir, name)
		if err := f.dir.Remove(name); err != nil {
			t.Fatal(err)
		}
		totalPastRefusedOpen(t, f.reopen(t), expected, totalPastTestLimits())
		earlier := TotalPastExpected{Checkpoint: first.Checkpoint, Digest: first.Digest, Capacity: 2, ChunkCapacity: 1}
		_, result := totalPastCollect(t, totalPastOpen(t, f.reopen(t), earlier, totalPastTestLimits()))
		if result.Qualification != "UNKNOWN" || result.Expected.Checkpoint.ID != first.Checkpoint.ID {
			t.Fatal("earlier range became whole epoch")
		}
		totalPastWrite(t, f.dir, name, raw)
		for _, mutate := range []func(*TotalPastExpected){func(e *TotalPastExpected) { e.Digest[0]++ }, func(e *TotalPastExpected) { e.Checkpoint.Session[0]++ }, func(e *TotalPastExpected) { e.Checkpoint.Source.Epoch[0]++ }, func(e *TotalPastExpected) { e.Checkpoint.Config.Total++ }, func(e *TotalPastExpected) { e.Checkpoint.Config.OutboundReserve++ }, func(e *TotalPastExpected) { e.Checkpoint.Config.MaxOutbound++ }, func(e *TotalPastExpected) { e.Checkpoint.Config.MaxInbound++ }, func(e *TotalPastExpected) { e.Checkpoint.Config.MaxAuxiliary++ }, func(e *TotalPastExpected) { e.Capacity++ }, func(e *TotalPastExpected) { e.ChunkCapacity++ }, func(e *TotalPastExpected) { e.Checkpoint.FromEntry.Upper++ }} {
			wrong := expected
			mutate(&wrong)
			totalPastRefusedOpen(t, f.reopen(t), wrong, totalPastTestLimits())
		}
		// Forge every stored session and both final chunk chains consistently.
		// A matching caller reference is data consistency only, never authority.
		session := expected.Checkpoint.Session
		session[0]++
		identity := totalPastRead(t, f.dir, "identity")
		copy(identity[len(totalStoreMagic):len(totalStoreMagic)+16], session[:])
		totalPastReseal(identity)
		totalPastWrite(t, f.dir, "identity", identity)
		for id := uint64(1); id <= last.Checkpoint.ID; id++ {
			final := totalPastRead(t, f.dir, totalFinalName(id))
			_, q, digest, c, count, _, err := totalDecodeFinal(final)
			if err != nil {
				t.Fatal(err)
			}
			var chain [32]byte
			for i := uint64(0); i < count; i++ {
				chunk := totalPastRead(t, f.dir, totalChunkName(id, i))
				copy(chunk[len(totalChunkMagic):len(totalChunkMagic)+16], session[:])
				totalPastReseal(chunk)
				totalPastWrite(t, f.dir, totalChunkName(id, i), chunk)
				chain = totalNextChunkChain(chain, i, chunk)
			}
			final, err = totalFinalRecord(totalSegmentWitness{session: session, descriptor: q, digest: digest}, c, count, chain, 2048)
			if err != nil {
				t.Fatal(err)
			}
			totalPastWrite(t, f.dir, totalFinalName(id), final)
		}
		totalPastRefusedOpen(t, f.reopen(t), expected, totalPastTestLimits())
		forged := expected
		forged.Checkpoint.Session = session
		_, result = totalPastCollect(t, totalPastOpen(t, f.reopen(t), forged, totalPastTestLimits()))
		if result.Qualification != "UNKNOWN" || result.AttributionBound {
			t.Fatal("untrusted matched forgery promoted")
		}
	})
	t.Run("visible-Indeterminate-is-not-commit-or-ACK-authority", func(t *testing.T) {
		f := newTotalPastCaptureFixture(t)
		release := f.enter(t, time.Second)
		f.leave(t, release, 2*time.Second)
		f.clock.set(3*time.Second, nil)
		real := f.o.issuer.writeRecord
		f.o.issuer.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
			status, err := real(name, raw)
			if len(name) >= 6 && name[len(name)-6:] == ".final" && err == nil {
				return secretfile.DurableIndeterminate, errors.New("test lost confirmation after REAL write")
			}
			return status, err
		}
		v, err := f.o.Capture()
		if err == nil || v.CommitOutcome != secretfile.DurableIndeterminate || v.AckStatus == "RETIRED" || v.Digest != ([32]byte{}) {
			t.Fatal("actual failed Capture minted anchor", v, err)
		}
		visible := totalPastRead(t, f.dir, totalFinalName(v.Checkpoint.ID))
		session, q, digest, c, _, _, err := totalDecodeFinal(visible)
		if err != nil {
			t.Fatal(err)
		}
		untrusted := TotalPastExpected{Checkpoint: totalFileCheckpoint(q, session), Digest: digest, Capacity: 2, ChunkCapacity: c} //derived from visible bytes, NOT a qualified live result.
		f.closeWriter(t)
		before := totalPastFiles(t, f.path)
		_, result := totalPastCollect(t, totalPastOpen(t, f.reopen(t), untrusted, totalPastTestLimits()))
		if result.Qualification != "UNKNOWN" || result.AttributionBound || f.b.fullObservation.segment.retiredThrough != 0 || !reflect.DeepEqual(before, totalPastFiles(t, f.path)) {
			t.Fatal("reader confirmed/ACKed uncertain publication", result)
		}
	})
	t.Run("whole-bounded-membership-canonical-and-overflow-refusal", func(t *testing.T) {
		for _, fault := range []string{"missing", "footer", "valid-row", "kind", "layout", "surplus", "partial-tail", "unknown", "huge-anchor", "huge-chunk-count"} {
			f := totalPastSimpleStore(t)
			name := totalChunkName(1, 0)
			raw := totalPastRead(t, f.dir, name)
			switch fault {
			case "missing":
				if err := f.dir.Remove(name); err != nil {
					t.Fatal(err)
				}
			case "footer":
				raw[len(raw)-1] ^= 1
				totalPastWrite(t, f.dir, name, raw)
			case "valid-row":
				offset := len(totalChunkMagic) + 16 + 8 + 32 + 8 + 8 + 8 + 8
				binary.LittleEndian.PutUint64(raw[offset+8:offset+16], 0)
				totalPastReseal(raw)
				totalPastWrite(t, f.dir, name, raw)
			case "kind":
				raw = bytes.Replace(raw, []byte(totalChunkMagic), []byte("corsa-k5-chunk-v1\x00"), 1)
				totalPastReseal(raw)
				totalPastWrite(t, f.dir, name, raw)
			case "layout":
				offset := len(totalChunkMagic) + 16 + 8 + 32
				binary.LittleEndian.PutUint64(raw[offset:offset+8], 2)
				totalPastReseal(raw)
				totalPastWrite(t, f.dir, name, raw)
			case "surplus":
				totalPastWrite(t, f.dir, totalChunkName(1, 999), raw)
			case "partial-tail":
				totalPastWrite(t, f.dir, totalChunkName(2, 0), raw)
			case "unknown":
				totalPastWrite(t, f.dir, "unknown", []byte("unreferenced"))
			case "huge-anchor":
				name = totalFinalName(1)
				raw = totalPastRead(t, f.dir, name)
				offset := len(totalFinalMagic) + 16 + len(totalCanonicalMagic)
				binary.LittleEndian.PutUint64(raw[offset:offset+8], totalMaxUint)
				totalPastReseal(raw)
				totalPastWrite(t, f.dir, name, raw)
			case "huge-chunk-count":
				offset := len(totalChunkMagic) + 16 + 8 + 32 + 8 + 8 + 8
				binary.LittleEndian.PutUint64(raw[offset:offset+8], uint64(^uint(0)>>1))
				totalPastReseal(raw)
				totalPastWrite(t, f.dir, name, raw)
			}
			before := totalPastFiles(t, f.path)
			totalPastRefusedOpen(t, f.reopen(t), f.expected, totalPastTestLimits())
			if !reflect.DeepEqual(before, totalPastFiles(t, f.path)) {
				t.Fatal("reader modified bad input", fault)
			}
		}
		{ // Derive the boundary from an actual production final, not a second codec.
			f := totalPastSimpleStore(t)
			raw := totalPastRead(t, f.dir, totalFinalName(1))
			if len(raw) != 509+len(f.key.Anchor) {
				t.Fatal("canonical total final framing changed", len(raw), len(f.key.Anchor))
			}
			names, err := os.ReadDir(f.path)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range names {
				info, err := n.Info()
				if err != nil || info.Size() > int64(len(raw)) {
					t.Fatal("fixture member exceeds actual final boundary", n.Name(), err)
				}
			}
			l := totalPastTestLimits()
			l.RecordBytes = len(raw)
			totalPastCollect(t, totalPastOpen(t, f.reopen(t), f.expected, l))
			l.RecordBytes--
			if r, err := OpenTotalPast(&secretfile.Dir{}, f.expected, l); r != nil || !errors.Is(err, errTotalPastRefused) {
				t.Fatal("one-less framing reached lease/IO", err)
			}
			l.RecordBytes++
			e := f.expected
			e.Checkpoint.Source.Anchor += "x"
			if r, err := OpenTotalPast(&secretfile.Dir{}, e, l); r != nil || !errors.Is(err, errTotalPastRefused) {
				t.Fatal("anchor exceeded record before lease", err)
			}
			// A delivered DTO explicitly charges its long anchor as nominal payload.
			probe := totalPastOpen(t, f.reopen(t), f.expected, totalPastTestLimits())
			batch, err := probe.Next()
			if err != nil {
				t.Fatal(err)
			}
			nominal := int64(unsafe.Sizeof(TotalPastBatch{})) + int64(len(f.key.Anchor)) + int64(len(batch.Rows))*int64(unsafe.Sizeof(TotalObservationTransition{}))
			if totalPastTestLimits().OutputBytes-probe.outputBytesLeft != nominal {
				t.Fatal("output charge omitted actual long anchor")
			}
			if _, err := probe.Close(); !errors.Is(err, errTotalPastIncomplete) {
				t.Fatal(err)
			}
		}
		{ // MaxUint terminal cannot start a walk/loop, even with an invalid Dir.
			f := totalPastSimpleStore(t)
			e := f.expected
			e.Checkpoint.ID = totalMaxUint
			e.Checkpoint.PreviousID = totalMaxUint - 1
			l := totalPastTestLimits()
			l.StoreRecords = int(^uint(0) >> 1)
			l.OutputCheckpoints = int(^uint(0) >> 1)
			if r, err := OpenTotalPast(&secretfile.Dir{}, e, l); r != nil || !errors.Is(err, errTotalPastRefused) {
				t.Fatal("MaxUint terminal reached IO/loop", err)
			}
		}
		{ // Opaque residuals are accounted and preserved, never checkpoint data.
			f := totalPastSimpleStore(t)
			name := secretfile.TempPrefix + "past-residue"
			body := []byte("incomplete opaque temp")
			totalPastWrite(t, f.dir, name, body)
			before := totalPastFiles(t, f.path)
			_, result := totalPastCollect(t, totalPastOpen(t, f.reopen(t), f.expected, totalPastTestLimits()))
			if result.OrphanRecords != 1 || result.OrphanBytes != int64(len(body)) || !reflect.DeepEqual(before, totalPastFiles(t, f.path)) {
				t.Fatal("opaque temp not charged/preserved", result)
			}
			l := totalPastTestLimits()
			l.OrphanRecords = 0
			totalPastRefusedOpen(t, f.reopen(t), f.expected, l)
			l = totalPastTestLimits()
			l.OrphanBytes = int64(len(body) - 1)
			totalPastRefusedOpen(t, f.reopen(t), f.expected, l)
		}
		for _, budget := range []string{"records-cap-plus-one", "store-bytes", "read-bytes", "work", "batch-rows"} {
			f := totalPastSimpleStore(t)
			l := totalPastTestLimits()
			switch budget {
			case "records-cap-plus-one":
				l.StoreRecords = 3
			case "store-bytes":
				l.StoreBytes = 2048
			case "read-bytes":
				l.ReadBytes = 1
			case "work":
				l.WorkSteps = 1
			case "batch-rows":
				l.BatchRows = 0
			}
			// Store-byte cap below the real store rather than relying on a guess.
			if budget == "store-bytes" {
				var sum int64
				names, err := os.ReadDir(f.path)
				if err != nil {
					t.Fatal(err)
				}
				for _, n := range names {
					info, err := n.Info()
					if err != nil {
						t.Fatal(err)
					}
					sum += info.Size()
				}
				l.StoreBytes = sum - 1
				if int64(l.RecordBytes) > l.StoreBytes {
					l.RecordBytes = int(l.StoreBytes)
				}
			}
			totalPastRefusedOpen(t, f.reopen(t), f.expected, l)
		}
	})
	t.Run("partial-delivery-sticky-budgets-and-read-rechecks", func(t *testing.T) {
		{ // Cooperating live writer is not observed without its lease.
			f := newTotalPastCaptureFixture(t)
			release := f.enter(t, time.Second)
			f.leave(t, release, 2*time.Second)
			f.capture(t, 3*time.Second)
			if r, err := OpenTotalPast(f.reopen(t), f.expected, totalPastTestLimits()); r != nil || !errors.Is(err, secretfile.ErrDirectoryLeaseBusy) {
				t.Fatal("live writer lease bypassed", err)
			}
			f.closeWriter(t)
		}
		{ // Final batch is not EOF: Close must still say incomplete.
			f := totalPastSimpleStore(t)
			r := totalPastOpen(t, f.reopen(t), f.expected, totalPastTestLimits())
			for {
				batch, err := r.Next()
				if err != nil {
					t.Fatal(err)
				}
				if batch.Final {
					break
				}
			}
			if result, err := r.Close(); err == nil || result.Status != "INCOMPLETE" || result.Qualification != "UNKNOWN" {
				t.Fatal("last batch implied complete delivery", result, err)
			}
		}
		for _, budget := range []string{"output-rows", "output-bytes", "input-read-after-first", "work-after-first"} {
			f := totalPastSimpleStore(t)
			l := totalPastTestLimits()
			if budget != "output-rows" {
				probe := totalPastOpen(t, f.reopen(t), f.expected, l)
				if _, err := probe.Next(); err != nil {
					t.Fatal(err)
				}
				switch budget {
				case "output-bytes":
					l.OutputBytes -= probe.outputBytesLeft
				case "input-read-after-first":
					l.ReadBytes -= probe.readsLeft
				case "work-after-first":
					l.WorkSteps -= probe.workLeft
				}
				if _, err := probe.Close(); !errors.Is(err, errTotalPastIncomplete) {
					t.Fatal(err)
				}
			} else {
				l.OutputRows = 1
			}
			r := totalPastOpen(t, f.reopen(t), f.expected, l)
			if _, err := r.Next(); err != nil {
				t.Fatal("first bounded batch unavailable", budget, err)
			}
			_, firstErr := r.Next()
			if firstErr == nil || errors.Is(firstErr, io.EOF) {
				t.Fatal("partial cap returned success", budget, firstErr)
			}
			if _, err := r.Next(); !errors.Is(err, firstErr) {
				t.Fatal("budget failure not sticky", budget, err)
			}
			if result, err := r.Close(); err == nil || result.Status == "MATCHED_EXPECTED" || result.Qualification != "UNKNOWN" {
				t.Fatal("partial prefix matched", budget, result, err)
			}
		}
		for _, fault := range []string{"unread-chunk", "unread-final", "late-tail"} {
			f := totalPastSimpleStore(t)
			r := totalPastOpen(t, f.reopen(t), f.expected, totalPastTestLimits())
			if fault == "late-tail" {
				for {
					b, err := r.Next()
					if err != nil {
						t.Fatal(err)
					}
					if b.Final {
						break
					}
				}
				raw := totalPastRead(t, f.dir, totalFinalName(1))
				totalPastWrite(t, f.dir, totalFinalName(2), raw)
			} else {
				name := totalChunkName(1, 0)
				if fault == "unread-final" {
					name = totalFinalName(1)
				}
				raw := totalPastRead(t, f.dir, name)
				if fault == "unread-chunk" {
					offset := len(totalChunkMagic) + 16 + 8 + 32 + 8 + 8 + 8 + 8
					binary.LittleEndian.PutUint64(raw[offset+8:offset+16], 0)
				} else {
					offset := len(totalFinalMagic) + 16 + len(totalCanonicalMagic)
					binary.LittleEndian.PutUint64(raw[offset:offset+8], totalMaxUint)
				}
				totalPastReseal(raw)
				totalPastWrite(t, f.dir, name, raw)
			}
			// Deliberate noncooperating input edits test only these actual rereads;
			// they do not claim atomic snapshots against arbitrary hostile writers.
			if _, err := r.Next(); err == nil || errors.Is(err, io.EOF) {
				t.Fatal("unread changed input passed", fault, err)
			}
			if result, err := r.Close(); err == nil || result.Status == "MATCHED_EXPECTED" {
				t.Fatal("changed input matched", fault, result, err)
			}
		}
	})
	t.Run("fixed-unknown-no-revival-nils-and-defaults", func(t *testing.T) {
		var absent *TotalPastReader
		result, err := absent.Close()
		if err == nil || result.Qualification != "UNKNOWN" || !result.PastOnly || result.AttributionBound {
			t.Fatal("nil fixed metadata", result, err)
		}
		directZero := &TotalPastReader{}
		direct, closeErr := directZero.Close()
		if closeErr == nil || direct.Qualification != "UNKNOWN" || !direct.PastOnly || direct.AttributionBound || direct.Status == "MATCHED_EXPECTED" {
			t.Fatal("direct zero Close fixed metadata", direct, closeErr)
		}
		zero := &TotalPastReader{}
		if _, err := zero.Next(); err == nil || errors.Is(err, io.EOF) {
			t.Fatal("zero reader matched")
		}
		result, err = zero.Close()
		if err == nil || result.Status == "MATCHED_EXPECTED" || result.Qualification != "UNKNOWN" || !result.PastOnly || result.AttributionBound {
			t.Fatal("zero fixed metadata", result, err)
		}
		again, againErr := zero.Close()
		if !reflect.DeepEqual(again, result) || !errors.Is(againErr, err) {
			t.Fatal("zero repeatedClose changed result")
		}
		f := totalPastSimpleStore(t)
		expected := f.expected
		r := totalPastOpen(t, f.reopen(t), expected, totalPastTestLimits())
		expected.Checkpoint.Source.Anchor = "changed"
		expected.Checkpoint.Session[0]++
		expected.Digest[0]++
		_, result = totalPastCollect(t, r)
		if result.Expected != f.expected {
			t.Fatal("caller mutated expected binding")
		}
		result.Qualification = "QUALIFIED"
		result.AttributionBound = true
		again, err = r.Close()
		if err != nil || again.Qualification != "UNKNOWN" || again.AttributionBound {
			t.Fatal("caller changed internal qualifier", again, err)
		}
		before := totalPastFiles(t, f.path)
		clock := &totalTestClock{}
		b, issuer, err := newFileSegmentedBudget(f.cfg, totalSourceKey{anchor: f.key.Anchor, epoch: f.key.Epoch, clockDomain: f.key.ClockDomain, configuration: f.key.Configuration}, 2, 1, clock.read, f.dir, totalFileTestLimits(), f.expected.Checkpoint.Session)
		if b == nil || issuer == nil || !errors.Is(err, errTotalStoreNotFresh) {
			t.Fatal("past reader revived live source", err)
		}
		if err := issuer.Close(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, totalPastFiles(t, f.path)) {
			t.Fatal("reader/new-source refusal modified archive")
		}
		ordinary, err := New(f.cfg)
		if err != nil || ordinary.fullObservation != nil {
			t.Fatal("reader activated default observation", err)
		}
		reservation := totalTake(t, ordinary, DirectionOutbound)
		reservation.Release()
		// Disabled is no evidence: no directory, limits, tokens or observer clock.
		calls := 0
		disabled, observation, err := NewWithTotalFileObservation(Config{MaxOutbound: 1}, TotalObservationKey{}, -1, -1, func() (time.Duration, error) { calls++; return 0, errors.New("disabled clock must not run") }, &secretfile.Dir{}, TotalFileObservationLimits{}, [16]byte{})
		if err != nil || disabled == nil || observation == nil || calls != 0 {
			t.Fatal("disabled constructor performed observer work", err, calls)
		}
		t.Cleanup(func() {
			if err := observation.Close(); err != nil {
				t.Error(err)
			}
		})
		v, err := observation.Capture()
		if err != nil || v.Status != "DISABLED" || v.Checkpoint.ID != 0 || calls != 0 {
			t.Fatal("disabled fabricated measured evidence", v, err, calls)
		}
		releaseDisabled := totalTake(t, disabled, DirectionOutbound)
		releaseDisabled.Release()
		empty, err := secretfile.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := empty.Close(); err != nil {
				t.Error(err)
			}
		})
		totalPastRefusedOpen(t, empty, f.expected, totalPastTestLimits())
		for _, change := range []func(*TotalPastExpected){func(e *TotalPastExpected) { e.Checkpoint.Config.Total = 0 }, func(e *TotalPastExpected) { e.Checkpoint.ID = 0; e.Checkpoint.PreviousID = 0 }} {
			e := f.expected
			change(&e)
			if r, err := OpenTotalPast(&secretfile.Dir{}, e, totalPastTestLimits()); r != nil || !errors.Is(err, errTotalPastRefused) {
				t.Fatal("disabled/ID0 reached lease or claimed zero", err)
			}
		}
	})
}
