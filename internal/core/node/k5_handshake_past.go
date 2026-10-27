package node

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

var errHandshakePastRefused = errors.New("handshake past input refused")
var errHandshakePastLimit = errors.New("handshake past caller budget exhausted")
var errHandshakePastIncomplete = errors.New("handshake past delivery incomplete")

// Required UNTRUSTED expectation. These are only persisted fields, not Service
// identity, artifact/boot/process labels, verified-controller status or proof.
type HandshakePastExpected struct {
	Checkpoint              HandshakeFileCheckpoint
	Digest                  [32]byte
	Capacity, ChunkCapacity int
}

// Caller-selected bounds, not deployment values or a physical resource claim.
// OutputBytes charges nominal Go batch/anchor/row payload, not wire/RSS size.
type HandshakePastLimits struct {
	RecordBytes                                                 int
	StoreBytes, OrphanBytes, ReadBytes, OutputBytes, OutputRows int64
	StoreRecords, OrphanRecords, ScanBatch, WorkSteps           int
	BatchRows, OutputCheckpoints                                int
}

// Rows are a caller-owned bounded transport batch. Final ends this recorded Q,
// not a source epoch or qualified interval. All output remains provisional
// until EOF and checked reader Close. No receipt/ACK authority is exposed.
type HandshakePastBatch struct {
	Checkpoint HandshakeFileCheckpoint
	Digest     [32]byte
	Offset     int
	Rows       []HandshakeObservationTransition
	Final      bool
}
type HandshakePastResult struct {
	Status, Reason, Qualification string
	AttributionBound              bool
	PastOnly                      bool
	Expected                      HandshakePastExpected
	DeliveredRows                 int64
	DeliveredCheckpoints          uint64
	OrphanBytes                   int64
	OrphanRecords                 int
}
type handshakePastFinal struct {
	q      handshakeSegmentDescriptor
	digest [32]byte
	chunks uint64
	chain  [32]byte
}
type handshakePastCursor struct {
	id, sequence uint64
	digest       [32]byte
	at           int64
	carry        handshakeSegmentCarry
}

// No gate/owner/session witness, live issuer, clock, sync or write operation.
// Directory is borrowed through Close. Its exclusive advisory inode lease
// excludes cooperating writers only; caller excludes own writes. No atomic
// snapshot or filesystem/ACL/power-loss/attribution guarantee is introduced.
// Next/Close serialize owned IO; Close fences new Next before joining IO.
// Do not copy a HandshakePastReader after first use.
type HandshakePastReader struct {
	mu                                         sync.Mutex
	closing                                    atomic.Bool
	dir                                        *secretfile.Dir
	lease                                      *secretfile.DirectoryLease
	expected                                   HandshakePastExpected
	target, origin                             handshakeSegmentDescriptor
	limits                                     HandshakePastLimits
	readsLeft, outputBytesLeft, outputRowsLeft int64
	workLeft                                   int
	previous                                   handshakePastCursor
	current                                    handshakePastFinal
	currentActive                              bool
	chunkIndex                                 uint64
	offset                                     int
	fold                                       handshakeSegmentFold
	sourceHash                                 hash.Hash
	chunkChain                                 [32]byte
	eof, closed                                bool
	failure, closeErr                          error
	result                                     HandshakePastResult
}

func handshakePastDescriptor(p HandshakeFileCheckpoint) handshakeSegmentDescriptor {
	row := func(r HandshakeObservationTransition) handshakeFullTransition {
		return handshakeFullTransition{sequence: r.Sequence, full: r.Full, count: r.Count, lower: r.Lower, upper: r.Upper, done: r.Done, valid: r.Valid}
	}
	return handshakeSegmentDescriptor{key: k5SourceKey{anchor: p.Source.Anchor, epoch: p.Source.Epoch, clockDomain: p.Source.ClockDomain, configuration: p.Source.Configuration, kind: k5CeilingKind(p.Source.Kind)}, bounds: handshakeBounds{node: p.Bounds.Node, loopback: p.Bounds.Loopback, perSource: p.Bounds.PerSource}, coverageStart: p.CoverageStart, checkpointID: p.ID, previousID: p.PreviousID, previousDigest: p.PreviousDigest, fromSeq: p.FromSequence, throughSeq: p.ThroughSequence, fromAt: p.FromAt, throughAt: p.ThroughAt, rowCount: p.Rows, fromCarry: handshakeSegmentCarry{full: p.FromFull, entry: row(p.FromEntry)}, throughCarry: handshakeSegmentCarry{full: p.ThroughFull, entry: row(p.ThroughEntry)}}
}
func validHandshakePastExpected(e HandshakePastExpected, l HandshakePastLimits) bool {
	q := handshakePastDescriptor(e.Checkpoint)
	maxBytes := uint64(^uint(0) >> 1)
	rowBytes := uint64(unsafe.Sizeof(handshakeFullTransition{}))
	generationBytes := uint64(8)
	if l.RecordBytes <= sha256.Size || l.StoreBytes <= 0 || int64(l.RecordBytes) > l.StoreBytes || l.OrphanBytes < 0 || l.ReadBytes <= 0 || l.OutputBytes <= 0 || l.OutputRows < 0 || l.StoreRecords <= 1 || l.OrphanRecords < 0 || l.ScanBatch <= 0 || l.WorkSteps <= 0 || l.BatchRows <= 0 || l.OutputCheckpoints <= 0 {
		return false
	}
	if uint64(l.BatchRows) > maxBytes/uint64(unsafe.Sizeof(HandshakeObservationTransition{})) {
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
	if !validObservationReference(q.key.anchor) || q.key.epoch == ([16]byte{}) || q.key.clockDomain == ([16]byte{}) || q.key.configuration == ([16]byte{}) || q.key.kind != k5HandshakeNode || e.Checkpoint.Session == ([16]byte{}) || q.bounds.node <= 0 || q.bounds.loopback < 0 || q.bounds.perSource < 0 || q.rowCount > e.Capacity {
		return false
	}
	_, ok := newHandshakeSegmentFold(q)
	return ok
}

// Open validates the complete current namespace and origin-to-exact-target
// chain before delivering any rows. No caller trust/commit flag is accepted.
// Canonical later/partial tails refuse; known opaque temps are only accounted.
func OpenHandshakePast(dir *secretfile.Dir, expected HandshakePastExpected, limits HandshakePastLimits) (*HandshakePastReader, error) {
	if dir == nil || !validHandshakePastExpected(expected, limits) {
		return nil, errHandshakePastRefused
	}
	r := &HandshakePastReader{dir: dir, expected: expected, target: handshakePastDescriptor(expected.Checkpoint), limits: limits, readsLeft: limits.ReadBytes, outputBytesLeft: limits.OutputBytes, outputRowsLeft: limits.OutputRows, workLeft: limits.WorkSteps, result: HandshakePastResult{Status: "INCOMPLETE", Reason: "delivery_not_finished", Qualification: "UNKNOWN", PastOnly: true, Expected: expected}}
	var err error
	r.lease, err = dir.TryDirectoryLease()
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*HandshakePastReader, error) { return nil, errors.Join(err, r.lease.Close()) }
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
		fold, ok := newHandshakeSegmentFold(f.q)
		if !ok {
			return fail(errHandshakePastRefused)
		}
		h := sha256.New()
		writeHandshakeSegmentHeader(h, f.q)
		var chain [32]byte
		offset := 0
		for index := uint64(0); index < f.chunks; index++ {
			n, raw, err := r.chunk(f, index, offset, &fold, h, nil)
			if err != nil {
				return fail(err)
			}
			offset += n
			chain = handshakeNextChunkChain(chain, index, raw)
		}
		if err = r.finish(f, fold, h, chain, offset); err != nil {
			return fail(err)
		}
		previous = r.after(f)
	}
	r.previous = r.originCursor()
	return r, nil
}
func (r *HandshakePastReader) step() error {
	if r.workLeft <= 0 {
		return errHandshakePastLimit
	}
	r.workLeft--
	return nil
}
func (r *HandshakePastReader) read(name string) ([]byte, error) {
	if err := r.step(); err != nil {
		return nil, err
	}
	match, err := r.lease.Matches(r.dir)
	if err != nil || !match {
		return nil, errors.Join(errHandshakePastRefused, err)
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
		return nil, errors.Join(errHandshakePastLimit, f.Close())
	}
	r.readsLeft -= size + 1
	raw := make([]byte, int(size))
	_, err = io.ReadFull(f, raw)
	var tail [1]byte
	if err == nil {
		n, e := f.Read(tail[:])
		if n != 0 || !errors.Is(e, io.EOF) {
			err = errHandshakePastRefused
		}
	}
	after, statErr := f.Stat()
	if statErr != nil || after != nil && (!os.SameFile(before, after) || before.Size() != after.Size()) {
		err = errors.Join(err, statErr, errHandshakePastRefused)
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	if !handshakeCheckedRecord(raw) {
		return nil, errHandshakePastRefused
	}
	return raw, nil
}
func (r *HandshakePastReader) identity() error {
	raw, err := r.read("identity")
	if err != nil {
		return err
	}
	d := handshakeFileDecoder{raw: raw[:len(raw)-sha256.Size]}
	d.magic(handshakeStoreMagic)
	var session [16]byte
	copy(session[:], d.take(16))
	q := d.header()
	capacity, chunk := d.integer(), d.integer()
	origin := handshakeSegmentDescriptor{key: r.target.key, bounds: r.target.bounds, coverageStart: r.target.coverageStart, fromAt: r.target.coverageStart, throughAt: r.target.coverageStart}
	if d.err != nil || d.cursor != len(d.raw) || q != origin || session != r.expected.Checkpoint.Session || capacity != r.expected.Capacity || chunk != r.expected.ChunkCapacity {
		return errHandshakePastRefused
	}
	r.origin = origin
	return nil
}
func (r *HandshakePastReader) originCursor() handshakePastCursor {
	return handshakePastCursor{at: int64(r.origin.coverageStart)}
}
func (r *HandshakePastReader) after(f handshakePastFinal) handshakePastCursor {
	return handshakePastCursor{id: f.q.checkpointID, sequence: f.q.throughSeq, digest: f.digest, at: int64(f.q.throughAt), carry: f.q.throughCarry}
}
func (r *HandshakePastReader) decodeFinal(id uint64) (handshakePastFinal, error) {
	raw, err := r.read(handshakeFinalName(id))
	if err != nil {
		return handshakePastFinal{}, err
	}
	session, q, digest, layout, chunks, chain, err := handshakeDecodeFinal(raw)
	want, ok := handshakeChunkCount(q.rowCount, r.expected.ChunkCapacity)
	if err != nil || !ok || session != r.expected.Checkpoint.Session || q.key != r.origin.key || q.bounds != r.origin.bounds || q.coverageStart != r.origin.coverageStart || q.checkpointID != id || q.rowCount < 0 || q.rowCount > r.expected.Capacity || layout != r.expected.ChunkCapacity || chunks != want || chunks > uint64(r.limits.StoreRecords) {
		return handshakePastFinal{}, errors.Join(errHandshakePastRefused, err)
	}
	if _, ok := newHandshakeSegmentFold(q); !ok {
		return handshakePastFinal{}, errHandshakePastRefused
	}
	return handshakePastFinal{q: q, digest: digest, chunks: chunks, chain: chain}, nil
}
func (r *HandshakePastReader) final(id uint64, previous handshakePastCursor) (handshakePastFinal, error) {
	f, err := r.decodeFinal(id)
	if err != nil {
		return f, err
	}
	q := f.q
	if q.previousID != previous.id || q.previousDigest != previous.digest || q.fromSeq != previous.sequence || int64(q.fromAt) != previous.at || q.fromCarry != previous.carry {
		return f, errHandshakePastRefused
	}
	if id == r.target.checkpointID && (q != r.target || f.digest != r.expected.Digest) {
		return f, errHandshakePastRefused
	}
	return f, nil
}
func (r *HandshakePastReader) namespace() error {
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
				failure = errHandshakePastLimit
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
					failure = errHandshakePastLimit
					break
				}
				orphanBytes += size
				orphans++
				continue
			}
			id, index, chunk, ok := handshakeFileName(name)
			if !ok || id > r.target.checkpointID {
				failure = errHandshakePastRefused
				break
			}
			fmeta, e := r.decodeFinal(id)
			if e != nil {
				failure = e
				break
			}
			if chunk {
				if index >= fmeta.chunks {
					failure = errHandshakePastRefused
					break
				}
			} else {
				finals++
			}
		}
	}
	closeErr := scan.Close()
	if failure != nil || closeErr != nil || !scan.Complete() {
		return errors.Join(failure, closeErr, errHandshakePastRefused)
	}
	if !identity || uint64(finals) != r.target.checkpointID {
		return errHandshakePastRefused
	}
	r.result.OrphanBytes, r.result.OrphanRecords = orphanBytes, orphans
	return nil
}

// rows, when nonnil, is a caller-budgeted fresh output slice, never a ring or
// retained history alias. Existing canonical decoder/fold/hash remain sole format.
func (r *HandshakePastReader) chunk(f handshakePastFinal, index uint64, offset int, fold *handshakeSegmentFold, h hash.Hash, rows []HandshakeObservationTransition) (int, []byte, error) {
	raw, err := r.read(handshakeChunkName(f.q.checkpointID, index))
	if err != nil {
		return 0, nil, err
	}
	d := handshakeFileDecoder{raw: raw[:len(raw)-sha256.Size]}
	d.magic(handshakeChunkMagic)
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
		return 0, nil, errHandshakePastRefused
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
			return 0, nil, errHandshakePastRefused
		}
		writeHandshakeSegmentRow(h, row)
		if rows != nil {
			rows[j] = HandshakeObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}
		}
	}
	if d.err != nil || d.cursor != len(d.raw) {
		return 0, nil, errHandshakePastRefused
	}
	return n, raw, nil
}
func (r *HandshakePastReader) finish(f handshakePastFinal, fold handshakeSegmentFold, h hash.Hash, chain [32]byte, offset int) error {
	if _, _, ok := fold.finish(); !ok || offset != f.q.rowCount || chain != f.chain {
		return errHandshakePastRefused
	}
	var digest [32]byte
	h.Sum(digest[:0])
	if digest != f.digest {
		return errHandshakePastRefused
	}
	return nil
}
func (r *HandshakePastReader) fail(err error) (HandshakePastBatch, error) {
	if r.failure == nil {
		r.failure = err
	}
	return HandshakePastBatch{}, r.failure
}
func (r *HandshakePastReader) chargeOutput(n int, q handshakeSegmentDescriptor) error {
	base := uint64(unsafe.Sizeof(HandshakePastBatch{})) + uint64(len(q.key.anchor))
	size := uint64(unsafe.Sizeof(HandshakeObservationTransition{}))
	max := uint64(1<<63 - 1)
	if n < 0 || uint64(n) > (max-base)/size || int64(n) > r.outputRowsLeft {
		return errHandshakePastLimit
	}
	amount := base + uint64(n)*size
	if amount > uint64(r.outputBytesLeft) {
		return errHandshakePastLimit
	}
	r.outputRowsLeft -= int64(n)
	r.outputBytesLeft -= int64(amount)
	return nil
}

// Next emits one actual stored chunk (at most C rows), or one EMPTY metadata
// batch. No transport boundary is converted to a new temporal/source cut.
// Consumers abort on processing error by Close BEFORE requesting final EOF;
// no claim is made about sink persistence or post-handoff consumer processing.
func (r *HandshakePastReader) Next() (HandshakePastBatch, error) {
	if r == nil {
		return HandshakePastBatch{}, os.ErrInvalid
	}
	if r.closing.Load() {
		return HandshakePastBatch{}, os.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing.Load() || r.closed {
		return HandshakePastBatch{}, os.ErrClosed
	}
	if r.failure != nil {
		return HandshakePastBatch{}, r.failure
	}
	if r.eof {
		return HandshakePastBatch{}, io.EOF
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
			return HandshakePastBatch{}, io.EOF
		}
		if r.previous.id == k5MaxUint {
			return r.fail(errHandshakePastRefused)
		}
		f, err := r.final(r.previous.id+1, r.previous)
		if err != nil {
			return r.fail(err)
		}
		r.current = f
		r.fold, _ = newHandshakeSegmentFold(f.q)
		r.sourceHash = sha256.New()
		writeHandshakeSegmentHeader(r.sourceHash, f.q)
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
	var rows []HandshakeObservationTransition
	if f.chunks != 0 {
		rows = make([]HandshakeObservationTransition, n)
		count, raw, err := r.chunk(f, r.chunkIndex, r.offset, &r.fold, r.sourceHash, rows)
		if err != nil {
			return r.fail(err)
		}
		r.offset += count
		r.chunkChain = handshakeNextChunkChain(r.chunkChain, r.chunkIndex, raw)
		r.chunkIndex++
	}
	final := r.chunkIndex == f.chunks
	batch := HandshakePastBatch{Checkpoint: fileCheckpoint(f.q, r.expected.Checkpoint.Session), Digest: f.digest, Offset: r.offset - n, Rows: rows, Final: final}
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
func (r *HandshakePastReader) Close() (HandshakePastResult, error) {
	if r == nil {
		return HandshakePastResult{Status: "INCOMPLETE", Reason: "reader_unavailable", Qualification: "UNKNOWN", PastOnly: true}, os.ErrInvalid
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
		r.closeErr = errHandshakePastIncomplete
	default:
		r.result.Status, r.result.Reason = "MATCHED_EXPECTED", "structural_past_only"
	}
	return r.result, r.closeErr
}
