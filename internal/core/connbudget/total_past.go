package connbudget

import (
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

var errTotalPastRefused = errors.New("connection-total past input refused")
var errTotalPastLimit = errors.New("connection-total past caller budget exhausted")
var errTotalPastIncomplete = errors.New("connection-total past delivery incomplete")

// Required UNTRUSTED expectation. These are only persisted fields, not Service
// identity, artifact/boot/process labels, verified-controller status or proof.
type TotalPastExpected struct {
	Checkpoint              TotalFileCheckpoint
	Digest                  [32]byte
	Capacity, ChunkCapacity int
}

// Caller-selected bounds, not deployment values or a physical resource claim.
// OutputBytes charges nominal Go batch/anchor/row payload, not wire/RSS size.
type TotalPastLimits struct {
	RecordBytes                                                 int
	StoreBytes, OrphanBytes, ReadBytes, OutputBytes, OutputRows int64
	StoreRecords, OrphanRecords, ScanBatch, WorkSteps           int
	BatchRows, OutputCheckpoints                                int
}

// Rows are a caller-owned bounded transport batch. Final ends this recorded Q,
// not a source epoch or qualified interval. All output remains provisional
// until EOF and checked reader Close. No receipt/ACK authority is exposed.
type TotalPastBatch struct {
	Checkpoint TotalFileCheckpoint
	Digest     [32]byte
	Offset     int
	Rows       []TotalObservationTransition
	Final      bool
}
type TotalPastResult struct {
	Status, Reason, Qualification string
	AttributionBound              bool
	PastOnly                      bool
	Expected                      TotalPastExpected
	DeliveredRows                 int64
	DeliveredCheckpoints          uint64
	OrphanBytes                   int64
	OrphanRecords                 int
}
type totalPastFinal struct {
	q      totalSegmentDescriptor
	digest [32]byte
	chunks uint64
	chain  [32]byte
}
type totalPastCursor struct {
	id, sequence uint64
	digest       [32]byte
	at           int64
	carry        totalSegmentCarry
}

// No gate/owner/session witness, live issuer, clock, sync or write operation.
// Directory is borrowed through Close. Its exclusive advisory inode lease
// excludes cooperating writers only; caller excludes own writes. No atomic
// snapshot or filesystem/ACL/power-loss/attribution guarantee is introduced.
// Next/Close serialize owned IO; Close fences new Next before joining IO.
// Do not copy a TotalPastReader after first use.
type TotalPastReader struct {
	mu                                         sync.Mutex
	closing                                    atomic.Bool
	dir                                        *secretfile.Dir
	lease                                      *secretfile.DirectoryLease
	expected                                   TotalPastExpected
	target, origin                             totalSegmentDescriptor
	limits                                     TotalPastLimits
	readsLeft, outputBytesLeft, outputRowsLeft int64
	workLeft                                   int
	previous                                   totalPastCursor
	current                                    totalPastFinal
	currentActive                              bool
	chunkIndex                                 uint64
	offset                                     int
	fold                                       totalSegmentFold
	sourceHash                                 hash.Hash
	chunkChain                                 [32]byte
	eof, closed                                bool
	failure, closeErr                          error
	result                                     TotalPastResult
}

func totalPastDescriptor(p TotalFileCheckpoint) totalSegmentDescriptor {
	row := func(r TotalObservationTransition) totalFullTransition {
		return totalFullTransition{sequence: r.Sequence, full: r.Full, count: r.Count, lower: r.Lower, upper: r.Upper, done: r.Done, valid: r.Valid}
	}
	return totalSegmentDescriptor{key: totalSourceKey{anchor: p.Source.Anchor, epoch: p.Source.Epoch, clockDomain: p.Source.ClockDomain, configuration: p.Source.Configuration}, cfg: p.Config, coverageStart: p.CoverageStart, checkpointID: p.ID, previousID: p.PreviousID, previousDigest: p.PreviousDigest, fromSeq: p.FromSequence, throughSeq: p.ThroughSequence, fromAt: p.FromAt, throughAt: p.ThroughAt, rowCount: p.Rows, fromCarry: totalSegmentCarry{full: p.FromFull, entry: row(p.FromEntry)}, throughCarry: totalSegmentCarry{full: p.ThroughFull, entry: row(p.ThroughEntry)}}
}
func validTotalPastExpected(e TotalPastExpected, l TotalPastLimits) bool {
	q := totalPastDescriptor(e.Checkpoint)
	maxBytes := uint64(^uint(0) >> 1)
	rowBytes := uint64(unsafe.Sizeof(totalFullTransition{}))
	generationBytes := uint64(8)
	if l.RecordBytes <= sha256.Size || l.StoreBytes <= 0 || int64(l.RecordBytes) > l.StoreBytes || l.OrphanBytes < 0 || l.ReadBytes <= 0 || l.OutputBytes <= 0 || l.OutputRows < 0 || l.StoreRecords <= 1 || l.OrphanRecords < 0 || l.ScanBatch <= 0 || l.WorkSteps <= 0 || l.BatchRows <= 0 || l.OutputCheckpoints <= 0 {
		return false
	}
	if uint64(l.BatchRows) > maxBytes/uint64(unsafe.Sizeof(TotalObservationTransition{})) {
		return false
	}
	if e.Capacity <= 0 || e.ChunkCapacity <= 0 || e.ChunkCapacity > e.Capacity || e.ChunkCapacity > l.BatchRows {
		return false
	}
	if uint64(e.Capacity) > maxBytes/(rowBytes+generationBytes) || uint64(e.ChunkCapacity) > (maxBytes-uint64(e.Capacity)*(rowBytes+generationBytes))/rowBytes {
		return false
	}
	if q.checkpointID == 0 || q.checkpointID > uint64(l.StoreRecords-1) || q.checkpointID > uint64(l.OutputCheckpoints) || q.previousID != q.checkpointID-1 {
		return false
	}
	// Exact minimum final-record framing: canonical magic + anchor length,
	// three tokens, Config5, coverage/IDs/digest/cursors, two full carries,
	// then final session/digest/layout/count/chain/footer. No mirrored encoding.
	const minimumFinal = len(totalFinalMagic) + 16 + len(totalCanonicalMagic) + 8 + 3*16 + 5*8 + 8 + 2*8 + 32 + 5*8 + 2*(8+7*8) + 32 + 8 + 8 + 32 + sha256.Size
	if l.RecordBytes < minimumFinal || q.key.anchor == "" || len(q.key.anchor) > l.RecordBytes-minimumFinal {
		return false
	}
	// Bound nominal output payload before hashing or allocation; long anchors
	// are valid within this explicit record cap, unlike Node's 128-byte refs.
	base := uint64(unsafe.Sizeof(TotalPastBatch{}))
	maxOutput := uint64(1<<63 - 1)
	if base > maxOutput || uint64(len(q.key.anchor)) > maxOutput-base {
		return false
	}
	base += uint64(len(q.key.anchor))
	if uint64(l.BatchRows) > (maxOutput-base)/uint64(unsafe.Sizeof(TotalObservationTransition{})) {
		return false
	}
	if q.key.epoch == ([16]byte{}) || q.key.clockDomain == ([16]byte{}) || q.key.configuration == ([16]byte{}) || e.Checkpoint.Session == ([16]byte{}) || q.cfg.Total <= 0 || q.rowCount > e.Capacity {
		return false
	}
	_, ok := newTotalSegmentFold(q)
	return ok
}

// Open validates the complete current namespace and origin-to-exact-target
// chain before delivering any rows. No caller trust/commit flag is accepted.
// Canonical later/partial tails refuse; known opaque temps are only accounted.
func OpenTotalPast(dir *secretfile.Dir, expected TotalPastExpected, limits TotalPastLimits) (*TotalPastReader, error) {
	if dir == nil || !validTotalPastExpected(expected, limits) {
		return nil, errTotalPastRefused
	}
	r := &TotalPastReader{dir: dir, expected: expected, target: totalPastDescriptor(expected.Checkpoint), limits: limits, readsLeft: limits.ReadBytes, outputBytesLeft: limits.OutputBytes, outputRowsLeft: limits.OutputRows, workLeft: limits.WorkSteps, result: TotalPastResult{Status: "INCOMPLETE", Reason: "delivery_not_finished", Qualification: "UNKNOWN", PastOnly: true, Expected: expected}}
	var err error
	r.lease, err = dir.TryDirectoryLease()
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*TotalPastReader, error) { return nil, errors.Join(err, r.lease.Close()) }
	if err = r.identity(); err != nil {
		return fail(err)
	}
	if err = r.namespace(); err != nil {
		return fail(err)
	}
	previous := r.originCursor()
	for id := uint64(1); id <= r.target.checkpointID; id++ {
		f, err := r.final(id, previous)
		if err != nil {
			return fail(err)
		}
		fold, ok := newTotalSegmentFold(f.q)
		if !ok {
			return fail(errTotalPastRefused)
		}
		h := sha256.New()
		writeTotalSegmentHeader(h, f.q)
		var chain [32]byte
		offset := 0
		for index := uint64(0); index < f.chunks; index++ {
			n, raw, err := r.chunk(f, index, offset, &fold, h, nil)
			if err != nil {
				return fail(err)
			}
			offset += n
			chain = totalNextChunkChain(chain, index, raw)
		}
		if err = r.finish(f, fold, h, chain, offset); err != nil {
			return fail(err)
		}
		previous = r.after(f)
	}
	r.previous = r.originCursor()
	return r, nil
}
func (r *TotalPastReader) step() error {
	if r.workLeft <= 0 {
		return errTotalPastLimit
	}
	r.workLeft--
	return nil
}
func (r *TotalPastReader) read(name string) ([]byte, error) {
	if err := r.step(); err != nil {
		return nil, err
	}
	match, err := r.lease.Matches(r.dir)
	if err != nil || !match {
		return nil, errors.Join(errTotalPastRefused, err)
	}
	f, err := r.dir.OpenFileForRead(name)
	if err != nil {
		return nil, err
	}
	before, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	size := before.Size()
	if size < sha256.Size || size > int64(r.limits.RecordBytes) || size >= r.readsLeft {
		return nil, errors.Join(errTotalPastLimit, f.Close())
	}
	r.readsLeft -= size + 1
	raw := make([]byte, int(size))
	_, err = io.ReadFull(f, raw)
	var tail [1]byte
	if err == nil {
		n, e := f.Read(tail[:])
		if n != 0 || !errors.Is(e, io.EOF) {
			err = errTotalPastRefused
		}
	}
	after, statErr := f.Stat()
	if statErr != nil || after != nil && (!os.SameFile(before, after) || before.Size() != after.Size()) {
		err = errors.Join(err, statErr, errTotalPastRefused)
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	if !totalCheckedRecord(raw) {
		return nil, errTotalPastRefused
	}
	return raw, nil
}
func (r *TotalPastReader) identity() error {
	raw, err := r.read("identity")
	if err != nil {
		return err
	}
	d := totalFileDecoder{raw: raw[:len(raw)-sha256.Size]}
	d.magic(totalStoreMagic)
	var session [16]byte
	copy(session[:], d.take(16))
	q := d.header()
	capacity, chunk := d.integer(), d.integer()
	origin := totalSegmentDescriptor{key: r.target.key, cfg: r.target.cfg, coverageStart: r.target.coverageStart, fromAt: r.target.coverageStart, throughAt: r.target.coverageStart}
	if d.err != nil || d.cursor != len(d.raw) || q != origin || session != r.expected.Checkpoint.Session || capacity != r.expected.Capacity || chunk != r.expected.ChunkCapacity {
		return errTotalPastRefused
	}
	r.origin = origin
	return nil
}
func (r *TotalPastReader) originCursor() totalPastCursor {
	return totalPastCursor{at: int64(r.origin.coverageStart)}
}
func (r *TotalPastReader) after(f totalPastFinal) totalPastCursor {
	return totalPastCursor{id: f.q.checkpointID, sequence: f.q.throughSeq, digest: f.digest, at: int64(f.q.throughAt), carry: f.q.throughCarry}
}
func (r *TotalPastReader) decodeFinal(id uint64) (totalPastFinal, error) {
	raw, err := r.read(totalFinalName(id))
	if err != nil {
		return totalPastFinal{}, err
	}
	session, q, digest, layout, chunks, chain, err := totalDecodeFinal(raw)
	want, ok := totalChunkCount(q.rowCount, r.expected.ChunkCapacity)
	if err != nil || !ok || session != r.expected.Checkpoint.Session || q.key != r.origin.key || q.cfg != r.origin.cfg || q.coverageStart != r.origin.coverageStart || q.checkpointID != id || q.rowCount < 0 || q.rowCount > r.expected.Capacity || layout != r.expected.ChunkCapacity || chunks != want || chunks > uint64(r.limits.StoreRecords) {
		return totalPastFinal{}, errors.Join(errTotalPastRefused, err)
	}
	if _, ok := newTotalSegmentFold(q); !ok {
		return totalPastFinal{}, errTotalPastRefused
	}
	return totalPastFinal{q: q, digest: digest, chunks: chunks, chain: chain}, nil
}
func (r *TotalPastReader) final(id uint64, previous totalPastCursor) (totalPastFinal, error) {
	f, err := r.decodeFinal(id)
	if err != nil {
		return f, err
	}
	q := f.q
	if q.previousID != previous.id || q.previousDigest != previous.digest || q.fromSeq != previous.sequence || int64(q.fromAt) != previous.at || q.fromCarry != previous.carry {
		return f, errTotalPastRefused
	}
	if id == r.target.checkpointID && (q != r.target || f.digest != r.expected.Digest) {
		return f, errTotalPastRefused
	}
	return f, nil
}
func (r *TotalPastReader) namespace() error {
	scan, err := r.lease.OpenNameScan(r.limits.ScanBatch, r.limits.StoreRecords)
	if err != nil {
		return err
	}
	var failure error
	var total, orphanBytes int64
	records, orphans, finals := 0, 0, 0
	identity := false
	for failure == nil {
		names, e := scan.Next()
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			failure = e
			break
		}
		for _, name := range names {
			if failure = r.step(); failure != nil {
				break
			}
			f, e := r.dir.OpenFileForRead(name)
			if e != nil {
				failure = e
				break
			}
			info, e := f.Stat()
			closeErr := f.Close()
			if e != nil || closeErr != nil {
				failure = errors.Join(e, closeErr)
				break
			}
			size := info.Size()
			if size < 0 || size > r.limits.StoreBytes-total || records == r.limits.StoreRecords {
				failure = errTotalPastLimit
				break
			}
			total += size
			records++
			if name == "identity" {
				identity = true
				continue
			}
			if len(name) >= len(secretfile.TempPrefix) && name[:len(secretfile.TempPrefix)] == secretfile.TempPrefix {
				if size > r.limits.OrphanBytes-orphanBytes || orphans == r.limits.OrphanRecords {
					failure = errTotalPastLimit
					break
				}
				orphanBytes += size
				orphans++
				continue
			}
			id, index, chunk, ok := totalFileName(name)
			if !ok || id > r.target.checkpointID {
				failure = errTotalPastRefused
				break
			}
			fmeta, e := r.decodeFinal(id)
			if e != nil {
				failure = e
				break
			}
			if chunk {
				if index >= fmeta.chunks {
					failure = errTotalPastRefused
					break
				}
			} else {
				finals++
			}
		}
	}
	closeErr := scan.Close()
	if failure != nil || closeErr != nil || !scan.Complete() {
		return errors.Join(failure, closeErr, errTotalPastRefused)
	}
	if !identity || uint64(finals) != r.target.checkpointID {
		return errTotalPastRefused
	}
	r.result.OrphanBytes, r.result.OrphanRecords = orphanBytes, orphans
	return nil
}

// rows, when nonnil, is a caller-budgeted fresh output slice, never a ring or
// retained history alias. Existing canonical decoder/fold/hash remain sole format.
func (r *TotalPastReader) chunk(f totalPastFinal, index uint64, offset int, fold *totalSegmentFold, h hash.Hash, rows []TotalObservationTransition) (int, []byte, error) {
	raw, err := r.read(totalChunkName(f.q.checkpointID, index))
	if err != nil {
		return 0, nil, err
	}
	d := totalFileDecoder{raw: raw[:len(raw)-sha256.Size]}
	d.magic(totalChunkMagic)
	var session [16]byte
	copy(session[:], d.take(16))
	id := d.uint()
	var digest [32]byte
	copy(digest[:], d.take(32))
	layout, gotIndex, gotOffset, n := d.integer(), d.uint(), d.integer(), d.integer()
	want := f.q.rowCount - offset
	if want > r.expected.ChunkCapacity {
		want = r.expected.ChunkCapacity
	}
	if d.err != nil || session != r.expected.Checkpoint.Session || id != f.q.checkpointID || digest != f.digest || layout != r.expected.ChunkCapacity || gotIndex != index || gotOffset != offset || n != want || n < 0 || n > (len(d.raw)-d.cursor)/(7*8) || rows != nil && n != len(rows) {
		return 0, nil, errTotalPastRefused
	}
	for j := 0; j < n; j++ {
		if err := r.step(); err != nil {
			return 0, nil, err
		}
		row := d.row()
		if d.err != nil {
			return 0, nil, d.err
		}
		if _, _, ok := fold.row(row); !ok {
			return 0, nil, errTotalPastRefused
		}
		writeTotalSegmentRow(h, row)
		if rows != nil {
			rows[j] = TotalObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}
		}
	}
	if d.err != nil || d.cursor != len(d.raw) {
		return 0, nil, errTotalPastRefused
	}
	return n, raw, nil
}
func (r *TotalPastReader) finish(f totalPastFinal, fold totalSegmentFold, h hash.Hash, chain [32]byte, offset int) error {
	if _, _, ok := fold.finish(); !ok || offset != f.q.rowCount || chain != f.chain {
		return errTotalPastRefused
	}
	var digest [32]byte
	h.Sum(digest[:0])
	if digest != f.digest {
		return errTotalPastRefused
	}
	return nil
}
func (r *TotalPastReader) fail(err error) (TotalPastBatch, error) {
	if r.failure == nil {
		r.failure = err
	}
	return TotalPastBatch{}, r.failure
}
func (r *TotalPastReader) chargeOutput(n int, q totalSegmentDescriptor) error {
	base := uint64(unsafe.Sizeof(TotalPastBatch{})) + uint64(len(q.key.anchor))
	size := uint64(unsafe.Sizeof(TotalObservationTransition{}))
	max := uint64(1<<63 - 1)
	if base > max || n < 0 || uint64(n) > (max-base)/size || int64(n) > r.outputRowsLeft {
		return errTotalPastLimit
	}
	amount := base + uint64(n)*size
	if amount > uint64(r.outputBytesLeft) {
		return errTotalPastLimit
	}
	r.outputRowsLeft -= int64(n)
	r.outputBytesLeft -= int64(amount)
	return nil
}

// Next emits one actual stored chunk (at most C rows), or one EMPTY metadata
// batch. No transport boundary is converted to a new temporal/source cut.
// Consumers abort on processing error by Close BEFORE requesting final EOF;
// no claim is made about sink persistence or post-handoff consumer processing.
func (r *TotalPastReader) Next() (TotalPastBatch, error) {
	if r == nil {
		return TotalPastBatch{}, os.ErrInvalid
	}
	if r.closing.Load() {
		return TotalPastBatch{}, os.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing.Load() || r.closed {
		return TotalPastBatch{}, os.ErrClosed
	}
	if r.failure != nil {
		return TotalPastBatch{}, r.failure
	}
	if r.eof {
		return TotalPastBatch{}, io.EOF
	}
	if !r.currentActive {
		if r.previous.id == r.target.checkpointID {
			if err := r.identity(); err != nil {
				return r.fail(err)
			}
			if err := r.namespace(); err != nil {
				return r.fail(err)
			}
			r.eof = true
			return TotalPastBatch{}, io.EOF
		}
		if r.previous.id == totalMaxUint {
			return r.fail(errTotalPastRefused)
		}
		f, err := r.final(r.previous.id+1, r.previous)
		if err != nil {
			return r.fail(err)
		}
		r.current = f
		r.fold, _ = newTotalSegmentFold(f.q)
		r.sourceHash = sha256.New()
		writeTotalSegmentHeader(r.sourceHash, f.q)
		r.chunkChain = [32]byte{}
		r.offset = 0
		r.chunkIndex = 0
		r.currentActive = true
	}
	f := r.current
	n := f.q.rowCount - r.offset
	if n > r.expected.ChunkCapacity {
		n = r.expected.ChunkCapacity
	}
	if err := r.chargeOutput(n, f.q); err != nil {
		return r.fail(err)
	}
	var rows []TotalObservationTransition
	if f.chunks != 0 {
		rows = make([]TotalObservationTransition, n)
		count, raw, err := r.chunk(f, r.chunkIndex, r.offset, &r.fold, r.sourceHash, rows)
		if err != nil {
			return r.fail(err)
		}
		r.offset += count
		r.chunkChain = totalNextChunkChain(r.chunkChain, r.chunkIndex, raw)
		r.chunkIndex++
	}
	final := r.chunkIndex == f.chunks
	batch := TotalPastBatch{Checkpoint: totalFileCheckpoint(f.q, r.expected.Checkpoint.Session), Digest: f.digest, Offset: r.offset - n, Rows: rows, Final: final}
	if final {
		if err := r.finish(f, r.fold, r.sourceHash, r.chunkChain, r.offset); err != nil {
			return r.fail(err)
		}
		r.previous = r.after(f)
		r.currentActive = false
		r.result.DeliveredCheckpoints++
	}
	r.result.DeliveredRows += int64(n)
	return batch, nil
}

// Close joins owned IO, checks release, and only then may report a structural
// exact expectation match. UNKNOWN qualification and false attribution are
// fixed even for a match; this method does not Sync/confirm/commit anything.
func (r *TotalPastReader) Close() (TotalPastResult, error) {
	if r == nil {
		return TotalPastResult{Status: "INCOMPLETE", Reason: "reader_unavailable", Qualification: "UNKNOWN", PastOnly: true}, os.ErrInvalid
	}
	r.closing.Store(true)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.result.Qualification = "UNKNOWN"
	r.result.AttributionBound = false
	r.result.PastOnly = true
	if r.closed {
		return r.result, r.closeErr
	}
	r.closed = true
	r.closeErr = r.lease.Close()
	switch {
	case r.failure != nil:
		r.result.Status, r.result.Reason = "REFUSED", "input_or_budget_failure"
		r.closeErr = errors.Join(r.failure, r.closeErr)
	case r.closeErr != nil:
		r.result.Status, r.result.Reason = "REFUSED", "reader_close_failed"
	case !r.eof:
		r.result.Status, r.result.Reason = "INCOMPLETE", "delivery_not_finished"
		r.closeErr = errTotalPastIncomplete
	default:
		r.result.Status, r.result.Reason = "MATCHED_EXPECTED", "structural_past_only"
	}
	return r.result, r.closeErr
}
