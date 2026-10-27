package node

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/secretfile"
)

var errHandshakeStoreNotFresh = errors.New("handshake evidence store must be empty for a new source")
var errHandshakeStoreClosed = errors.New("handshake evidence issuer closed or unavailable")
var errHandshakeCommitUnconfirmed = errors.New("handshake evidence commit unconfirmed")

// Caller allocation/storage requests, not deployment values or physical RAM,
// disk-block/inode/latency guarantees. All uncommitted records (including a
// current partial Q), random temps and failed cleanup count against orphans.
type handshakeFileLimits struct {
	recordBytes                                       int
	storeBytes, orphanBytes, scanBytes                int64
	storeRecords, orphanRecords, scanBatch, scanSteps int
}
type handshakeFileBudget struct {
	bytes int64
	steps int
}

func (b *handshakeFileBudget) step() error {
	if b.steps <= 0 {
		return errHandshakeFileBudget
	}
	b.steps--
	return nil
}
func (b *handshakeFileBudget) read(n int64) error {
	if n < 0 || n > b.bytes {
		return errHandshakeFileBudget
	}
	b.bytes -= n
	return nil
}

type handshakeFileAccount struct {
	bytes, orphanBytes int64
	records, orphans   int
	identity           bool
}

// One concrete inactive issuer, bound to one actual live source and directory.
// Selected only by an explicit typed observation request; no config default,
// background worker or production test signer.
// mu serializes ALL own writes/recovery/verification/Close, outside gate.mu.
// Other writers must participate in the same advisory directory-inode lease.
// The existing File.Sync+directory.Sync contract is conditional on the chosen
// OS/FS/device and access/ACL model; this is not a power-loss guarantee.
type handshakeFileIssuer struct {
	mu                  sync.Mutex
	gate                *inboundHandshakes
	owner               *handshakeFullObservation
	dir                 *secretfile.Dir
	lease               *secretfile.DirectoryLease
	limits              handshakeFileLimits
	capacity, chunkRows int
	session             [16]byte
	identityRaw         []byte // bounded constant header, never a whole Q
	identityConfirmed   bool
	committedID         uint64
	finalAttemptID      uint64 // never cleared by late failure; one live logical Q
	committedDigest     [32]byte
	last                handshakeSegmentWitness
	closed              bool
	closeErr            error
	// Initialized ONLY to the real method below. Same-package tests may wrap
	// real writes to exercise returned-status paths, not syscall/powerloss.
	writeRecord func(string, []byte) (secretfile.DurableCommit, error)
}

// Builds the original V2 gate once, then binds its issuer before publication.
// Every new source object requires an EMPTY dedicated store, even if textual
// source/session tokens match old records. No serialized pointer, old ring,
// completion or epoch is resurrected. Errors retain the actual gate; caller
// must close the returned issuer, including initialization uncertainty.
func newFileObservedInboundHandshakes(bounds handshakeBounds, key k5SourceKey, capacity, chunkRows int, clock func() (time.Duration, error), dir *secretfile.Dir, limits handshakeFileLimits, session [16]byte) (*inboundHandshakes, *handshakeFileIssuer, error) {
	i := &handshakeFileIssuer{dir: dir, limits: limits, capacity: capacity, chunkRows: chunkRows, session: session}
	h := newSegmentedInboundHandshakes(bounds, key, capacity, chunkRows, clock, i, session)
	i.gate, i.owner = h, h.fullObservation
	if i.owner.invalid != "" || !i.owner.initialized {
		return h, i, errHandshakeCommitUnconfirmed
	}
	if dir == nil || limits.recordBytes <= sha256.Size || limits.storeBytes <= 0 || int64(limits.recordBytes) > limits.storeBytes || limits.orphanBytes <= 0 || limits.scanBytes <= 0 || limits.storeRecords <= 0 || limits.orphanRecords <= 0 || limits.scanBatch <= 0 || limits.scanSteps <= 0 {
		return h, i, errHandshakeFileBudget
	}
	identity := handshakeSegmentDescriptor{key: key, bounds: bounds, coverageStart: i.owner.start, fromAt: i.owner.start, throughAt: i.owner.start}
	raw, err := handshakeIdentityRecord(identity, session, capacity, chunkRows, limits.recordBytes)
	if err != nil {
		return h, i, err
	}
	i.identityRaw = append([]byte(nil), raw...)
	i.writeRecord = dir.DurableWrite
	i.lease, err = dir.TryDirectoryLease()
	if err != nil {
		return h, i, err
	}
	if err = i.requireFreshLocked(); err != nil {
		i.closed = true
		i.closeErr = i.lease.Close()
		return h, i, errors.Join(err, i.closeErr)
	}
	account := handshakeFileAccount{}
	budget := i.budget()
	_, err = i.ensureRecordLocked("identity", raw, &account, &budget)
	if err == nil {
		i.identityConfirmed = true
	}
	return h, i, err
}
func (i *handshakeFileIssuer) budget() handshakeFileBudget {
	return handshakeFileBudget{bytes: i.limits.scanBytes, steps: i.limits.scanSteps}
}
func (i *handshakeFileIssuer) requireFreshLocked() error {
	scan, err := i.lease.OpenNameScan(i.limits.scanBatch, i.limits.storeRecords)
	if err != nil {
		return err
	}
	names, readErr := scan.Next()
	closeErr := scan.Close()
	if len(names) != 0 {
		return errors.Join(errHandshakeStoreNotFresh, closeErr)
	}
	if !errors.Is(readErr, io.EOF) || closeErr != nil || !scan.Complete() {
		return errors.Join(errHandshakeStoreNotFresh, readErr, closeErr)
	}
	return i.dir.DurablePreflight()
}
func (i *handshakeFileIssuer) boundLocked() error {
	if i.closed || i.lease == nil || i.writeRecord == nil {
		return errHandshakeStoreClosed
	}
	match, err := i.lease.Matches(i.dir)
	if err != nil || !match {
		return errors.Join(errHandshakeStoreClosed, err)
	}
	return nil
}
func (i *handshakeFileIssuer) Close() error {
	if i == nil {
		return os.ErrInvalid
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return i.closeErr
	}
	i.closed = true
	if i.lease != nil {
		i.closeErr = i.lease.Close()
	}
	return i.closeErr
}

// No whole-history list and no scan while own writes occur. Every entry is
// charged; no deletion or best-effort cleanup refund. A full verified walk+
// namespace Sync may re-establish actual accounting after an uncertain call.
func (i *handshakeFileIssuer) accountLocked(b *handshakeFileBudget, current handshakeSegmentWitness) (handshakeFileAccount, error) {
	a := handshakeFileAccount{}
	if err := i.boundLocked(); err != nil {
		return a, err
	}
	scan, err := i.lease.OpenNameScan(i.limits.scanBatch, i.limits.storeRecords)
	if err != nil {
		return a, err
	}
	var failure error
	for failure == nil {
		names, err := scan.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			failure = err
			break
		}
		for _, name := range names {
			if err := b.step(); err != nil {
				failure = err
				break
			}
			file, err := i.dir.OpenFileForRead(name)
			if err != nil {
				failure = err
				break
			}
			info, statErr := file.Stat()
			closeErr := file.Close()
			if statErr != nil || closeErr != nil {
				failure = errors.Join(statErr, closeErr)
				break
			}
			size := info.Size()
			if size < 0 || size > i.limits.storeBytes-a.bytes || a.records == i.limits.storeRecords {
				failure = errHandshakeFileBudget
				break
			}
			a.bytes += size
			a.records++
			orphan := false
			if name == "identity" {
				a.identity = true
				orphan = !i.identityConfirmed
			} else if strings.HasPrefix(name, secretfile.TempPrefix) {
				orphan = true
			} else {
				var memberErr error
				orphan, memberErr = i.memberLocked(name, current, b)
				if memberErr != nil {
					failure = memberErr
					break
				}
			}
			if orphan {
				if size > i.limits.orphanBytes-a.orphanBytes || a.orphans == i.limits.orphanRecords {
					failure = errHandshakeFileBudget
					break
				}
				a.orphanBytes += size
				a.orphans++
			}
		}
	}
	closeErr := scan.Close()
	if failure != nil || closeErr != nil || !scan.Complete() {
		return a, errors.Join(failure, closeErr, errHandshakeCommitUnconfirmed)
	}
	if err := i.dir.DurablePreflight(); err != nil {
		return a, err
	}
	if a.identity {
		raw, err := i.readRecordLocked("identity", false, b)
		if err != nil || !bytes.Equal(raw, i.identityRaw) {
			return a, errors.Join(errHandshakeFileRecord, err)
		}
	}
	return a, nil
}

// Every canonical filename needs exact referenced membership, not just an
// old qid. Descriptor reads are charged; no unbounded history map.
func (i *handshakeFileIssuer) memberLocked(name string, current handshakeSegmentWitness, b *handshakeFileBudget) (bool, error) {
	qid, index, chunk, ok := handshakeFileName(name)
	if !ok {
		return false, errHandshakeFileRecord
	}
	if qid > i.committedID {
		if i.committedID == k5MaxUint || qid != i.committedID+1 || current.owner != i.owner || current.session != i.session || current.descriptor.checkpointID != qid || !i.validDescriptor(current.descriptor) {
			return false, errHandshakeFileRecord
		}
		count, ok := handshakeChunkCount(current.descriptor.rowCount, i.chunkRows)
		if !ok || chunk && index >= count {
			return false, errHandshakeFileRecord
		}
		return true, nil // current partial/final still charges orphan caps
	}
	if chunk {
		raw, err := i.readRecordLocked(handshakeFinalName(qid), false, b)
		if err != nil {
			return false, err
		}
		session, q, _, layout, count, _, err := handshakeDecodeFinal(raw)
		expected, ok := handshakeChunkCount(q.rowCount, i.chunkRows)
		if err != nil || !ok || session != i.session || q.checkpointID != qid || !i.validDescriptor(q) || layout != i.chunkRows || count != expected || index >= count {
			return false, errHandshakeFileRecord
		}
	}
	return false, nil
}

func (i *handshakeFileIssuer) readRecordLocked(name string, confirm bool, b *handshakeFileBudget) ([]byte, error) {
	if err := b.step(); err != nil {
		return nil, err
	}
	file, err := i.dir.OpenFileForRead(name)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	size := info.Size()
	if size < sha256.Size || size > int64(i.limits.recordBytes) || b.read(size) != nil || b.read(1) != nil {
		return nil, errors.Join(errHandshakeFileBudget, file.Close())
	}
	raw := make([]byte, int(size))
	_, err = io.ReadFull(file, raw)
	var tail [1]byte
	if err == nil {
		n, tailErr := file.Read(tail[:])
		if n != 0 || !errors.Is(tailErr, io.EOF) {
			err = errHandshakeFileRecord
		}
	}
	if err == nil && !handshakeCheckedRecord(raw) {
		err = errHandshakeFileRecord
	}
	after, statErr := file.Stat()
	if statErr != nil || after != nil && (!os.SameFile(info, after) || info.Size() != after.Size()) {
		err = errors.Join(err, statErr, errHandshakeFileRecord)
	}
	if err == nil && confirm {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	return raw, nil
}
func (i *handshakeFileIssuer) confirmExactLocked(name string, raw []byte, b *handshakeFileBudget) error {
	actual, err := i.readRecordLocked(name, true, b)
	if err != nil || !bytes.Equal(actual, raw) {
		return errors.Join(errHandshakeCommitUnconfirmed, err)
	}
	return i.dir.DurablePreflight()
}
func (i *handshakeFileIssuer) ensureRecordLocked(name string, raw []byte, a *handshakeFileAccount, b *handshakeFileBudget) (secretfile.DurableCommit, error) {
	actual, err := i.readRecordLocked(name, false, b)
	if err == nil {
		if !bytes.Equal(actual, raw) {
			return secretfile.DurableNotCommitted, errHandshakeFileRecord
		}
		if err := i.confirmExactLocked(name, raw, b); err != nil {
			return secretfile.DurableIndeterminate, err
		}
		return secretfile.DurableCommitted, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return secretfile.DurableNotCommitted, err
	}
	// Conservative target+simultaneous-temp reservations. The post-error
	// accounting is unknown until a fresh complete scan/namespace sync.
	n := int64(len(raw))
	if n > (i.limits.storeBytes-a.bytes)/2 || i.limits.storeRecords-a.records < 2 || n > (i.limits.orphanBytes-a.orphanBytes)/2 || i.limits.orphanRecords-a.orphans < 2 {
		return secretfile.DurableNotCommitted, errHandshakeFileBudget
	}
	outcome, err := i.writeRecord(name, raw)
	if err != nil || outcome != secretfile.DurableCommitted {
		if outcome != secretfile.DurableNotCommitted && outcome != secretfile.DurableIndeterminate {
			outcome = secretfile.DurableIndeterminate
		}
		return outcome, errors.Join(errHandshakeCommitUnconfirmed, err)
	}
	a.bytes += n
	a.records++
	a.orphanBytes += n
	a.orphans++
	if err := i.confirmExactLocked(name, raw, b); err != nil {
		return secretfile.DurableIndeterminate, err
	}
	return secretfile.DurableCommitted, nil
}

func (i *handshakeFileIssuer) warmWitnessLocked(q handshakeSegmentDescriptor) (handshakeSegmentWitness, error) {
	if w, ok := i.gate.expectedHandshakeSegmentWitness(); ok && w.descriptor == q {
		return w, nil
	}
	stream, status := i.gate.beginHandshakeSegmentStream(q)
	if status != "STREAMING" {
		return handshakeSegmentWitness{}, errHandshakeCommitUnconfirmed
	}
	for {
		loan, status := i.gate.borrowHandshakeSegmentChunk(stream)
		if status != "BORROWED" {
			return handshakeSegmentWitness{}, errHandshakeCommitUnconfirmed
		}
		chunk, status := i.gate.copyHandshakeSegmentChunk(loan)
		released := i.gate.releaseHandshakeSegmentChunk(loan)
		if status != "COPIED" || !released {
			return handshakeSegmentWitness{}, errHandshakeCommitUnconfirmed
		}
		if chunk.final {
			break
		}
	}
	w, ok := i.gate.expectedHandshakeSegmentWitness()
	if !ok || w.descriptor != q {
		return handshakeSegmentWitness{}, errHandshakeCommitUnconfirmed
	}
	return w, nil
}
func (i *handshakeFileIssuer) validDescriptor(q handshakeSegmentDescriptor) bool {
	return q.key == i.owner.key && q.bounds == i.owner.bounds && q.coverageStart == i.owner.start && q.rowCount >= 0 && q.rowCount <= i.capacity
}

// verifyWholeLocked uses a constant multiple of recordBytes plus fold/hash
// state (retained final + chunk + confirmation/encoding buffers), not one-E
// memory or measured RSS. All chunks
// must exist; no partial coverage or latest-pointer-only reconstruction.
// confirm freshly File.Syncs exact chunks, then exact final, then parent Sync;
// matching readable bytes alone NEVER promote an indeterminate final commit.
func (i *handshakeFileIssuer) verifyWholeLocked(w handshakeSegmentWitness, confirm bool, b *handshakeFileBudget) ([32]byte, error) {
	var zero [32]byte
	if w.owner != i.owner || w.session != i.session || !i.validDescriptor(w.descriptor) {
		return zero, errHandshakeFileRecord
	}
	name := handshakeFinalName(w.descriptor.checkpointID)
	raw, err := i.readRecordLocked(name, false, b)
	if err != nil {
		return zero, err
	}
	session, q, digest, chunkRows, chunks, wantChain, err := handshakeDecodeFinal(raw)
	count, valid := handshakeChunkCount(q.rowCount, i.chunkRows)
	if err != nil || !valid || session != i.session || q != w.descriptor || digest != w.digest || chunkRows != i.chunkRows || chunks != count || chunks > uint64(i.limits.storeRecords) || chunks > uint64(b.steps) {
		return zero, errHandshakeFileRecord
	}
	fold, ok := newHandshakeSegmentFold(q)
	if !ok {
		return zero, errHandshakeFileRecord
	}
	sourceHash := sha256.New()
	writeHandshakeSegmentHeader(sourceHash, q)
	var chain [32]byte
	offset := 0
	for index := uint64(0); index < chunks; index++ {
		chunkRaw, err := i.readRecordLocked(handshakeChunkName(q.checkpointID, index), confirm, b)
		if err != nil {
			return zero, err
		}
		d := handshakeFileDecoder{raw: chunkRaw[:len(chunkRaw)-sha256.Size]}
		d.magic(handshakeChunkMagic)
		var chunkSession [16]byte
		copy(chunkSession[:], d.take(16))
		qid := d.uint()
		var chunkDigest [32]byte
		copy(chunkDigest[:], d.take(32))
		layout := d.integer()
		gotIndex := d.uint()
		gotOffset := d.integer()
		n := d.integer()
		want := q.rowCount - offset
		if want > i.chunkRows {
			want = i.chunkRows
		}
		if d.err != nil || chunkSession != i.session || qid != q.checkpointID || chunkDigest != w.digest || layout != i.chunkRows || gotIndex != index || gotOffset != offset || n != want || n > (len(d.raw)-d.cursor)/(7*8) {
			return zero, errHandshakeFileRecord
		}
		for rowIndex := 0; rowIndex < n; rowIndex++ {
			row := d.row()
			if d.err != nil {
				return zero, d.err
			}
			if _, _, ok := fold.row(row); !ok {
				return zero, errHandshakeFileRecord
			}
			writeHandshakeSegmentRow(sourceHash, row)
		}
		if d.err != nil || d.cursor != len(d.raw) {
			return zero, errHandshakeFileRecord
		}
		offset += n
		chain = handshakeNextChunkChain(chain, index, chunkRaw)
	}
	if _, _, ok := fold.finish(); !ok || offset != q.rowCount || chain != wantChain {
		return zero, errHandshakeFileRecord
	}
	var actual [32]byte
	sourceHash.Sum(actual[:0])
	if actual != w.digest {
		return zero, errHandshakeFileRecord
	}
	if confirm {
		if err := i.confirmExactLocked(name, raw, b); err != nil {
			return zero, err
		}
	}
	return sha256.Sum256(raw), nil
}

func (i *handshakeFileIssuer) historyLocked(b *handshakeFileBudget) error {
	if i.committedID > uint64(i.limits.storeRecords) {
		return errHandshakeFileBudget
	}
	var previousID, sequence uint64
	previousAt := i.owner.start
	var previousDigest [32]byte
	var carry handshakeSegmentCarry
	for id := uint64(1); id <= i.committedID; id++ {
		raw, err := i.readRecordLocked(handshakeFinalName(id), false, b)
		if err != nil {
			return err
		}
		session, q, digest, _, _, _, err := handshakeDecodeFinal(raw)
		if err != nil || session != i.session || q.checkpointID != id || q.previousID != previousID || q.previousDigest != previousDigest || q.fromSeq != sequence || q.fromAt != previousAt || q.fromCarry != carry {
			return errHandshakeFileRecord
		}
		w := handshakeSegmentWitness{owner: i.owner, session: i.session, descriptor: q, digest: digest}
		if _, err := i.verifyWholeLocked(w, false, b); err != nil {
			return err
		}
		previousID, sequence, previousAt, previousDigest, carry = id, q.throughSeq, q.throughAt, digest, q.throughCarry
	}
	if previousDigest != i.committedDigest {
		return errHandshakeFileRecord
	}
	return nil
}
func (i *handshakeFileIssuer) prepareLocked(b *handshakeFileBudget, current handshakeSegmentWitness) (handshakeFileAccount, error) {
	a, err := i.accountLocked(b, current)
	if err != nil {
		return a, err
	}
	if !a.identity {
		if i.identityConfirmed {
			return a, errHandshakeFileRecord
		}
		if _, err := i.ensureRecordLocked("identity", i.identityRaw, &a, b); err != nil {
			return a, err
		}
	} else if !i.identityConfirmed {
		if err := i.confirmExactLocked("identity", i.identityRaw, b); err != nil {
			return a, err
		}
	}
	i.identityConfirmed = true
	if err := i.historyLocked(b); err != nil {
		return a, err
	}
	return a, nil
}
func (i *handshakeFileIssuer) acceptWitnessLocked(w handshakeSegmentWitness) bool {
	if w.owner != i.owner || w.session != i.session || !i.validDescriptor(w.descriptor) {
		return false
	}
	q := w.descriptor
	if q.checkpointID == i.committedID {
		return i.last == w
	}
	if i.committedID == k5MaxUint || q.checkpointID != i.committedID+1 || q.previousID != i.committedID || q.previousDigest != i.committedDigest {
		return false
	}
	if i.committedID == 0 {
		return q.fromSeq == 0 && q.fromAt == i.owner.start && q.fromCarry == (handshakeSegmentCarry{})
	}
	return q.fromSeq == i.last.descriptor.throughSeq && q.fromAt == i.last.descriptor.throughAt && q.fromCarry == i.last.descriptor.throughCarry
}
func (i *handshakeFileIssuer) recordCommittedLocked(w handshakeSegmentWitness) {
	i.committedID = w.descriptor.checkpointID
	i.committedDigest = w.digest
	i.last = w
}

// Outcomes describe this call's ability to confirm the logical Q, never
// undo a historical commit. Earlier possible visibility or confirmation plus
// failed revalidation is INDETERMINATE/no capsule; history is not reset.
func (i *handshakeFileIssuer) failedConfirmation(q handshakeSegmentDescriptor) secretfile.DurableCommit {
	if q.checkpointID != 0 && (q.checkpointID <= i.committedID || q.checkpointID == i.finalAttemptID) {
		return secretfile.DurableIndeterminate
	}
	return secretfile.DurableNotCommitted
}

// Commit writes actual Q data in bounded C chunks and a fixed final record.
// It never calls ACK while holding issuer.mu; caller submits the returned
// capsule through the original source's independent lease/quiescence checks.
func (i *handshakeFileIssuer) Commit(q handshakeSegmentDescriptor) (handshakeSegmentReceipt, secretfile.DurableCommit, error) {
	var empty handshakeSegmentReceipt
	if i == nil {
		return empty, secretfile.DurableNotCommitted, errHandshakeStoreClosed
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.boundLocked(); err != nil {
		return empty, i.failedConfirmation(q), err
	}
	w, err := i.warmWitnessLocked(q)
	if err != nil || !i.acceptWitnessLocked(w) {
		return empty, i.failedConfirmation(q), errors.Join(errHandshakeCommitUnconfirmed, err)
	}
	budget := i.budget()
	account, err := i.prepareLocked(&budget, w)
	if err != nil {
		return empty, i.failedConfirmation(q), err
	}
	if _, err := i.dir.Lstat(handshakeFinalName(q.checkpointID)); err == nil {
		i.finalAttemptID = q.checkpointID
		proof, err := i.verifyWholeLocked(w, true, &budget)
		if err != nil {
			return empty, secretfile.DurableIndeterminate, err
		}
		i.recordCommittedLocked(w)
		return handshakeSegmentReceipt{witness: w, proof: proof}, secretfile.DurableCommitted, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return empty, i.failedConfirmation(q), err
	}
	stream, status := i.gate.beginHandshakeSegmentStream(q)
	if status != "STREAMING" {
		return empty, i.failedConfirmation(q), errHandshakeCommitUnconfirmed
	}
	var chain [32]byte
	var chunks uint64
	for {
		loan, status := i.gate.borrowHandshakeSegmentChunk(stream)
		if status != "BORROWED" {
			return empty, i.failedConfirmation(q), errHandshakeCommitUnconfirmed
		}
		chunk, status := i.gate.copyHandshakeSegmentChunk(loan)
		if status != "COPIED" {
			_ = i.gate.releaseHandshakeSegmentChunk(loan)
			return empty, i.failedConfirmation(q), errHandshakeCommitUnconfirmed
		}
		var raw []byte
		if len(chunk.rows) != 0 {
			raw, err = handshakeChunkRecord(w, chunks, chunk, i.chunkRows, i.limits.recordBytes)
			if err == nil {
				_, err = i.ensureRecordLocked(handshakeChunkName(q.checkpointID, chunks), raw, &account, &budget)
			}
		}
		released := i.gate.releaseHandshakeSegmentChunk(loan)
		if err != nil || !released {
			return empty, i.failedConfirmation(q), errors.Join(errHandshakeCommitUnconfirmed, err)
		}
		if len(raw) != 0 {
			chain = handshakeNextChunkChain(chain, chunks, raw)
			chunks++
		}
		if chunk.final {
			break
		}
	}
	final, err := handshakeFinalRecord(w, i.chunkRows, chunks, chain, i.limits.recordBytes)
	if err != nil {
		return empty, i.failedConfirmation(q), err
	}
	priorFinal := i.failedConfirmation(q) == secretfile.DurableIndeterminate
	i.finalAttemptID = q.checkpointID // BEFORE possible final visibility, never clear on error
	outcome, err := i.ensureRecordLocked(handshakeFinalName(q.checkpointID), final, &account, &budget)
	if err != nil {
		if priorFinal || outcome == secretfile.DurableCommitted {
			outcome = secretfile.DurableIndeterminate
		}
		return empty, outcome, err
	}
	proof, err := i.verifyWholeLocked(w, true, &budget)
	if err != nil {
		return empty, secretfile.DurableIndeterminate, err
	}
	i.recordCommittedLocked(w)
	return handshakeSegmentReceipt{witness: w, proof: proof}, secretfile.DurableCommitted, nil
}

// Real filesystem authority, not a signer: proof identifies the exact final
// record but is insufficient without this bound source/store and fresh strict
// readback+sync confirmation. Recovery is ONLY on this still-live issuer;
// fresh construction over any old record is refused before it can bind.
func (i *handshakeFileIssuer) verifyHandshakeSegment(receipt handshakeSegmentReceipt) (handshakeSegmentCommit, bool) {
	if i == nil {
		return handshakeSegmentCommit{}, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.boundLocked(); err != nil {
		return handshakeSegmentCommit{}, false
	}
	current, known := i.gate.expectedHandshakeSegmentWitness()
	if !known || current != receipt.witness || !i.acceptWitnessLocked(current) {
		return handshakeSegmentCommit{}, false
	}
	budget := i.budget()
	if _, err := i.prepareLocked(&budget, current); err != nil {
		return handshakeSegmentCommit{}, false
	}
	proof, err := i.verifyWholeLocked(receipt.witness, true, &budget)
	if err != nil || proof != receipt.proof {
		return handshakeSegmentCommit{}, false
	}
	i.recordCommittedLocked(receipt.witness)
	return handshakeSegmentCommit{witness: receipt.witness}, true
}
