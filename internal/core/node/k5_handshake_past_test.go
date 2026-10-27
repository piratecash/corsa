package node

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
	"testing"
	"time"
)

// Tiny functional caps only. No deployment retention/M-C/RAM policy.
func pastTestLimits() HandshakePastLimits {
	return HandshakePastLimits{RecordBytes: 2048, StoreBytes: 131072, OrphanBytes: 65536, ReadBytes: 4194304, OutputBytes: 1048576, OutputRows: 1024, StoreRecords: 128, OrphanRecords: 64, ScanBatch: 4, WorkSteps: 8192, BatchRows: 1, OutputCheckpoints: 32}
}

type pastCaptureFixture struct {
	path        string
	dir         *secretfile.Dir
	service     *Service
	observation *HandshakeFileObservation
	request     HandshakeFileObservationRequest
	clock       *pendingConstructionClock
	expected    HandshakePastExpected
	checkpoints []HandshakeFileCheckpoint
	rows        []HandshakeObservationTransition
}

func newPastCaptureFixture(t *testing.T) *pastCaptureFixture {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("directory-inode lease unsupported")
	}
	f := &pastCaptureFixture{path: t.TempDir(), clock: &pendingConstructionClock{}}
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
	f.request = fileConstructionRequest(f.dir)
	var readouts ObservationReadouts
	f.service, readouts = constructFileService(t, f.request, f.clock)
	f.observation = readouts.HandshakeFile
	handshakeFill(t, f.service.inboundHandshakes, 15) //63 original owners, each valid source admission.
	return f
}
func (f *pastCaptureFixture) rememberRow(t *testing.T) {
	t.Helper()
	h := f.service.inboundHandshakes
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.fullObservation
	for _, r := range o.rows {
		if r.sequence == o.sequence && r.done && r.valid {
			f.rows = append(f.rows, HandshakeObservationTransition{Sequence: r.sequence, Full: r.full, Count: r.count, Lower: r.lower, Upper: r.upper, Done: r.done, Valid: r.valid})
			return
		}
	}
	t.Fatal("actual owner row unavailable")
}
func (f *pastCaptureFixture) enter(t *testing.T, at time.Duration) func() {
	t.Helper()
	f.clock.set(at, nil)
	release := handshakeEnterFrom(t, f.service.inboundHandshakes, "198.18.2.1")
	t.Cleanup(release)
	f.rememberRow(t)
	return release
}
func (f *pastCaptureFixture) leave(t *testing.T, release func(), at time.Duration) {
	t.Helper()
	f.clock.set(at, nil)
	release()
	release()
	f.rememberRow(t)
}
func (f *pastCaptureFixture) capture(t *testing.T, at time.Duration) HandshakeFileObservationResult {
	t.Helper()
	f.clock.set(at, nil)
	v := captureFile(t, f.observation)
	// Retain the actual result outside the store before Close. This is still an
	// UNTRUSTED reader expectation, not a production controller/attribution proof.
	f.expected = HandshakePastExpected{Checkpoint: v.Checkpoint, Digest: v.Digest, Capacity: f.request.Capacity, ChunkCapacity: f.request.ChunkCapacity}
	f.checkpoints = append(f.checkpoints, v.Checkpoint)
	return v
}
func (f *pastCaptureFixture) closeWriter(t *testing.T) {
	t.Helper()
	if err := f.observation.Close(); err != nil {
		t.Fatal(err)
	}
}
func (f *pastCaptureFixture) reopen(t *testing.T) *secretfile.Dir {
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
func pastSimpleStore(t *testing.T) *pastCaptureFixture {
	t.Helper()
	f := newPastCaptureFixture(t)
	release := f.enter(t, time.Second)
	f.leave(t, release, 2*time.Second)
	f.capture(t, 3*time.Second)
	f.closeWriter(t)
	return f
}
func pastOpen(t *testing.T, d *secretfile.Dir, e HandshakePastExpected, l HandshakePastLimits) *HandshakePastReader {
	t.Helper()
	r, err := OpenHandshakePast(d, e, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = r.Close() })
	return r
}
func pastCollect(t *testing.T, r *HandshakePastReader) ([]HandshakePastBatch, HandshakePastResult) {
	t.Helper()
	var batches []HandshakePastBatch
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
func pastWrite(t *testing.T, d *secretfile.Dir, name string, raw []byte) {
	t.Helper()
	if status, err := d.DurableWrite(name, raw); err != nil || status != secretfile.DurableCommitted {
		t.Fatal(status, err)
	}
}
func pastReseal(raw []byte) {
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	copy(raw[len(raw)-sha256.Size:], sum[:])
}
func pastFiles(t *testing.T, path string) map[string][32]byte {
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
	return m //finite fixture oracle, never production enumeration.
}
func pastRefusedOpen(t *testing.T, d *secretfile.Dir, e HandshakePastExpected, l HandshakePastLimits) {
	t.Helper()
	if r, err := OpenHandshakePast(d, e, l); err == nil || r != nil {
		if r != nil {
			_, _ = r.Close()
		}
		t.Fatal("incomplete/mismatched input opened", err)
	}
}
func TestHandshakePastExpectedReader(t *testing.T) {
	t.Run("real-Capture-reopen-carry-empty-and-ring-reuse", func(t *testing.T) {
		f := newPastCaptureFixture(t)
		release := f.enter(t, time.Second)
		first := f.capture(t, time.Second)
		empty := f.capture(t, 2*time.Second)
		if empty.Checkpoint.Rows != 0 || empty.Checkpoint.ThroughSequence != first.Checkpoint.ThroughSequence || empty.Checkpoint.ID == first.Checkpoint.ID || empty.Checkpoint.FromEntry != first.Checkpoint.ThroughEntry {
			t.Fatal("actual EMPTY temporal Q", empty)
		}
		f.leave(t, release, 3*time.Second)
		f.capture(t, 3*time.Second)
		for n := 0; n < 2; n++ {
			at := time.Duration(4+2*n) * time.Second
			release = f.enter(t, at)
			f.leave(t, release, at+time.Second)
			f.capture(t, at+time.Second)
		}
		if len(f.service.inboundHandshakes.fullObservation.rows) != 2 || f.service.inboundHandshakes.fullObservation.sequence != 6 {
			t.Fatal("real bounded ring fixture")
		}
		expected := f.expected
		f.closeWriter(t)
		before := pastFiles(t, f.path)
		borrowed := f.reopen(t)
		r := pastOpen(t, borrowed, expected, pastTestLimits())
		batches, result := pastCollect(t, r)
		var rows []HandshakeObservationTransition
		var checkpoints []HandshakeFileCheckpoint
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
		if !reflect.DeepEqual(before, pastFiles(t, f.path)) {
			t.Fatal("read-only path changed namespace/bodies")
		}
		if _, err := borrowed.Lstat("identity"); err != nil {
			t.Fatal("reader closed borrowed directory", err)
		}
	})
	t.Run("exact-temporal-terminal-source-and-untrusted-reference", func(t *testing.T) {
		f := newPastCaptureFixture(t)
		f.enter(t, time.Second)
		first := f.capture(t, time.Second)
		last := f.capture(t, 2*time.Second)
		expected := f.expected
		f.closeWriter(t)
		// Removing a whole EMPTY suffix leaves the same row cursor and a valid
		// shorter chain, but cannot satisfy the retained exact temporal reference.
		name := handshakeFinalName(last.Checkpoint.ID)
		raw := fileConstructionRead(t, f.dir, name)
		if err := f.dir.Remove(name); err != nil {
			t.Fatal(err)
		}
		pastRefusedOpen(t, f.reopen(t), expected, pastTestLimits())
		earlier := HandshakePastExpected{Checkpoint: first.Checkpoint, Digest: first.Digest, Capacity: 2, ChunkCapacity: 1}
		_, result := pastCollect(t, pastOpen(t, f.reopen(t), earlier, pastTestLimits()))
		if result.Qualification != "UNKNOWN" || result.Expected.Checkpoint.ID != first.Checkpoint.ID {
			t.Fatal("earlier range became whole epoch")
		}
		pastWrite(t, f.dir, name, raw)
		for _, mutate := range []func(*HandshakePastExpected){func(e *HandshakePastExpected) { e.Digest[0]++ }, func(e *HandshakePastExpected) { e.Checkpoint.Session[0]++ }, func(e *HandshakePastExpected) { e.Checkpoint.Source.Epoch[0]++ }, func(e *HandshakePastExpected) { e.Checkpoint.Bounds.Node++ }, func(e *HandshakePastExpected) { e.Capacity++ }, func(e *HandshakePastExpected) { e.ChunkCapacity++ }, func(e *HandshakePastExpected) { e.Checkpoint.FromEntry.Upper++ }} {
			wrong := expected
			mutate(&wrong)
			pastRefusedOpen(t, f.reopen(t), wrong, pastTestLimits())
		}
		// Forge every stored session and both final chunk chains consistently.
		// A matching caller reference is data consistency only, never authority.
		session := expected.Checkpoint.Session
		session[0]++
		identity := fileConstructionRead(t, f.dir, "identity")
		copy(identity[len(handshakeStoreMagic):len(handshakeStoreMagic)+16], session[:])
		pastReseal(identity)
		pastWrite(t, f.dir, "identity", identity)
		for id := uint64(1); id <= last.Checkpoint.ID; id++ {
			final := fileConstructionRead(t, f.dir, handshakeFinalName(id))
			_, q, digest, c, count, _, err := handshakeDecodeFinal(final)
			if err != nil {
				t.Fatal(err)
			}
			var chain [32]byte
			for i := uint64(0); i < count; i++ {
				chunk := fileConstructionRead(t, f.dir, handshakeChunkName(id, i))
				copy(chunk[len(handshakeChunkMagic):len(handshakeChunkMagic)+16], session[:])
				pastReseal(chunk)
				pastWrite(t, f.dir, handshakeChunkName(id, i), chunk)
				chain = handshakeNextChunkChain(chain, i, chunk)
			}
			final, err = handshakeFinalRecord(handshakeSegmentWitness{session: session, descriptor: q, digest: digest}, c, count, chain, 2048)
			if err != nil {
				t.Fatal(err)
			}
			pastWrite(t, f.dir, handshakeFinalName(id), final)
		}
		pastRefusedOpen(t, f.reopen(t), expected, pastTestLimits())
		forged := expected
		forged.Checkpoint.Session = session
		_, result = pastCollect(t, pastOpen(t, f.reopen(t), forged, pastTestLimits()))
		if result.Qualification != "UNKNOWN" || result.AttributionBound {
			t.Fatal("untrusted matched forgery promoted")
		}
	})
	t.Run("visible-Indeterminate-is-not-commit-or-ACK-authority", func(t *testing.T) {
		f := newPastCaptureFixture(t)
		release := f.enter(t, time.Second)
		f.leave(t, release, 2*time.Second)
		f.clock.set(3*time.Second, nil)
		real := f.observation.issuer.writeRecord
		f.observation.issuer.writeRecord = func(name string, raw []byte) (secretfile.DurableCommit, error) {
			status, err := real(name, raw)
			if len(name) >= 6 && name[len(name)-6:] == ".final" && err == nil {
				return secretfile.DurableIndeterminate, errors.New("test lost confirmation after REAL write")
			}
			return status, err
		}
		v, err := f.observation.Capture()
		if err == nil || v.CommitOutcome != secretfile.DurableIndeterminate || v.AckStatus == "RETIRED" || v.Digest != ([32]byte{}) {
			t.Fatal("actual failed Capture minted anchor", v, err)
		}
		visible := fileConstructionRead(t, f.dir, handshakeFinalName(v.Checkpoint.ID))
		session, q, digest, c, _, _, err := handshakeDecodeFinal(visible)
		if err != nil {
			t.Fatal(err)
		}
		untrusted := HandshakePastExpected{Checkpoint: fileCheckpoint(q, session), Digest: digest, Capacity: 2, ChunkCapacity: c} //derived from visible bytes, NOT a qualified live result.
		f.closeWriter(t)
		before := pastFiles(t, f.path)
		_, result := pastCollect(t, pastOpen(t, f.reopen(t), untrusted, pastTestLimits()))
		if result.Qualification != "UNKNOWN" || result.AttributionBound || f.service.inboundHandshakes.fullObservation.segment.retiredThrough != 0 || !reflect.DeepEqual(before, pastFiles(t, f.path)) {
			t.Fatal("reader confirmed/ACKed uncertain publication", result)
		}
	})

	t.Run("whole-bounded-membership-canonical-and-overflow-refusal", func(t *testing.T) {
		for _, fault := range []string{"missing", "footer", "valid-row", "kind", "layout", "surplus", "partial-tail", "unknown", "huge-anchor", "huge-chunk-count"} {
			f := pastSimpleStore(t)
			name := handshakeChunkName(1, 0)
			raw := fileConstructionRead(t, f.dir, name)
			switch fault {
			case "missing":
				if err := f.dir.Remove(name); err != nil {
					t.Fatal(err)
				}
			case "footer":
				raw[len(raw)-1] ^= 1
				pastWrite(t, f.dir, name, raw)
			case "valid-row":
				offset := len(handshakeChunkMagic) + 16 + 8 + 32 + 8 + 8 + 8 + 8
				binary.LittleEndian.PutUint64(raw[offset+8:offset+16], 0)
				pastReseal(raw)
				pastWrite(t, f.dir, name, raw)
			case "kind":
				raw = bytes.Replace(raw, []byte(handshakeChunkMagic), []byte("corsa-k5-total-chunk-v1\x00"), 1)
				pastReseal(raw)
				pastWrite(t, f.dir, name, raw)
			case "layout":
				offset := len(handshakeChunkMagic) + 16 + 8 + 32
				binary.LittleEndian.PutUint64(raw[offset:offset+8], 2)
				pastReseal(raw)
				pastWrite(t, f.dir, name, raw)
			case "surplus":
				pastWrite(t, f.dir, handshakeChunkName(1, 999), raw)
			case "partial-tail":
				pastWrite(t, f.dir, handshakeChunkName(2, 0), raw)
			case "unknown":
				pastWrite(t, f.dir, "unknown", []byte("unreferenced"))
			case "huge-anchor":
				name = handshakeFinalName(1)
				raw = fileConstructionRead(t, f.dir, name)
				offset := len(handshakeFinalMagic) + 16 + len(handshakeCanonicalMagic)
				binary.LittleEndian.PutUint64(raw[offset:offset+8], k5MaxUint)
				pastReseal(raw)
				pastWrite(t, f.dir, name, raw)
			case "huge-chunk-count":
				offset := len(handshakeChunkMagic) + 16 + 8 + 32 + 8 + 8 + 8
				binary.LittleEndian.PutUint64(raw[offset:offset+8], uint64(^uint(0)>>1))
				pastReseal(raw)
				pastWrite(t, f.dir, name, raw)
			}
			before := pastFiles(t, f.path)
			pastRefusedOpen(t, f.reopen(t), f.expected, pastTestLimits())
			if !reflect.DeepEqual(before, pastFiles(t, f.path)) {
				t.Fatal("reader modified bad input", fault)
			}
		}
		{ // MaxUint terminal cannot start a walk/loop, even with an invalid Dir.
			f := pastSimpleStore(t)
			e := f.expected
			e.Checkpoint.ID = k5MaxUint
			e.Checkpoint.PreviousID = k5MaxUint - 1
			l := pastTestLimits()
			l.StoreRecords = int(^uint(0) >> 1)
			l.OutputCheckpoints = int(^uint(0) >> 1)
			if r, err := OpenHandshakePast(&secretfile.Dir{}, e, l); r != nil || !errors.Is(err, errHandshakePastRefused) {
				t.Fatal("MaxUint terminal reached IO/loop", err)
			}
		}
		{ // Opaque residuals are accounted and preserved, never checkpoint data.
			f := pastSimpleStore(t)
			name := secretfile.TempPrefix + "past-residue"
			body := []byte("incomplete opaque temp")
			pastWrite(t, f.dir, name, body)
			before := pastFiles(t, f.path)
			_, result := pastCollect(t, pastOpen(t, f.reopen(t), f.expected, pastTestLimits()))
			if result.OrphanRecords != 1 || result.OrphanBytes != int64(len(body)) || !reflect.DeepEqual(before, pastFiles(t, f.path)) {
				t.Fatal("opaque temp not charged/preserved", result)
			}
			l := pastTestLimits()
			l.OrphanRecords = 0
			pastRefusedOpen(t, f.reopen(t), f.expected, l)
			l = pastTestLimits()
			l.OrphanBytes = int64(len(body) - 1)
			pastRefusedOpen(t, f.reopen(t), f.expected, l)
		}
		for _, budget := range []string{"records-cap-plus-one", "store-bytes", "read-bytes", "work", "batch-rows"} {
			f := pastSimpleStore(t)
			l := pastTestLimits()
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
			pastRefusedOpen(t, f.reopen(t), f.expected, l)
		}
	})
	t.Run("partial-delivery-sticky-budgets-and-read-rechecks", func(t *testing.T) {
		{ // Cooperating live writer is not observed without its lease.
			f := newPastCaptureFixture(t)
			release := f.enter(t, time.Second)
			f.leave(t, release, 2*time.Second)
			f.capture(t, 3*time.Second)
			if r, err := OpenHandshakePast(f.reopen(t), f.expected, pastTestLimits()); r != nil || !errors.Is(err, secretfile.ErrDirectoryLeaseBusy) {
				t.Fatal("live writer lease bypassed", err)
			}
			f.closeWriter(t)
		}
		{ // Final batch is not EOF: Close must still say incomplete.
			f := pastSimpleStore(t)
			r := pastOpen(t, f.reopen(t), f.expected, pastTestLimits())
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
			f := pastSimpleStore(t)
			l := pastTestLimits()
			if budget != "output-rows" {
				probe := pastOpen(t, f.reopen(t), f.expected, l)
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
				if _, err := probe.Close(); !errors.Is(err, errHandshakePastIncomplete) {
					t.Fatal(err)
				}
			} else {
				l.OutputRows = 1
			}
			r := pastOpen(t, f.reopen(t), f.expected, l)
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
			f := pastSimpleStore(t)
			r := pastOpen(t, f.reopen(t), f.expected, pastTestLimits())
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
				raw := fileConstructionRead(t, f.dir, handshakeFinalName(1))
				pastWrite(t, f.dir, handshakeFinalName(2), raw)
			} else {
				name := handshakeChunkName(1, 0)
				if fault == "unread-final" {
					name = handshakeFinalName(1)
				}
				raw := fileConstructionRead(t, f.dir, name)
				if fault == "unread-chunk" {
					offset := len(handshakeChunkMagic) + 16 + 8 + 32 + 8 + 8 + 8 + 8
					binary.LittleEndian.PutUint64(raw[offset+8:offset+16], 0)
				} else {
					offset := len(handshakeFinalMagic) + 16 + len(handshakeCanonicalMagic)
					binary.LittleEndian.PutUint64(raw[offset:offset+8], k5MaxUint)
				}
				pastReseal(raw)
				pastWrite(t, f.dir, name, raw)
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
		var absent *HandshakePastReader
		result, err := absent.Close()
		if err == nil || result.Qualification != "UNKNOWN" || !result.PastOnly || result.AttributionBound {
			t.Fatal("nil fixed metadata", result, err)
		}
		directZero := &HandshakePastReader{}
		direct, closeErr := directZero.Close()
		if closeErr == nil || direct.Qualification != "UNKNOWN" || !direct.PastOnly || direct.AttributionBound || direct.Status == "MATCHED_EXPECTED" {
			t.Fatal("direct zero Close fixed metadata", direct, closeErr)
		}
		zero := &HandshakePastReader{}
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
		f := pastSimpleStore(t)
		expected := f.expected
		r := pastOpen(t, f.reopen(t), expected, pastTestLimits())
		expected.Checkpoint.Source.Anchor = "changed"
		expected.Checkpoint.Session[0]++
		expected.Digest[0]++
		_, result = pastCollect(t, r)
		if result.Expected != f.expected {
			t.Fatal("caller mutated expected binding")
		}
		result.Qualification = "QUALIFIED"
		result.AttributionBound = true
		again, err = r.Close()
		if err != nil || again.Qualification != "UNKNOWN" || again.AttributionBound {
			t.Fatal("caller changed internal qualifier", again, err)
		}
		before := pastFiles(t, f.path)
		clock := &k5TestClock{}
		h, issuer, err := newFileObservedInboundHandshakes(productionHandshakeBounds(), k5SourceKey{anchor: f.expected.Checkpoint.Source.Anchor, epoch: f.expected.Checkpoint.Source.Epoch, clockDomain: f.expected.Checkpoint.Source.ClockDomain, configuration: f.expected.Checkpoint.Source.Configuration, kind: k5HandshakeNode}, f.expected.Capacity, f.expected.ChunkCapacity, clock.read, f.dir, handshakeFileTestLimits(), f.expected.Checkpoint.Session)
		if h == nil || issuer == nil || !errors.Is(err, errHandshakeStoreNotFresh) {
			t.Fatal("past reader revived live source", err)
		}
		if err := issuer.Close(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, pastFiles(t, f.path)) {
			t.Fatal("reader/new-source refusal modified archive")
		}
		cfg, id := pendingConstructionInputs(t)
		ordinary := NewService(cfg, id, nil)
		ownTerminalConstruction(t, ordinary, time.Now().Add(5*time.Second))
		if ordinary.handshakeFileObservation != nil || ordinary.inboundHandshakes.fullObservation != nil {
			t.Fatal("reader activated default observation")
		}
	})
}
