package connbudget

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

var errTotalStoreNotFresh = errors.New("total evidence store must be empty for a new source")
var errTotalStoreClosed = errors.New("total evidence issuer closed or unavailable")
var errTotalCommitUnconfirmed = errors.New("total evidence commit unconfirmed")

// Caller allocation/storage requests, not deployment values or physical RAM,
// disk-block/inode/latency guarantees. All uncommitted records (including a
// current partial Q), random temps and failed cleanup count against orphans.
type totalFileLimits struct {
	recordBytes                                       int
	storeBytes, orphanBytes, scanBytes                int64
	storeRecords, orphanRecords, scanBatch, scanSteps int
}
type totalFileBudget struct {
	bytes int64
	steps int
}

func (b *totalFileBudget) step() error {
	if b.steps <= 0 {
		return errTotalFileBudget
	}
	b.steps--
	return nil
}
func (b *totalFileBudget) read(n int64) error {
	if n < 0 || n > b.bytes {
		return errTotalFileBudget
	}
	b.bytes -= n
	return nil
}

type totalFileAccount struct {
	bytes, orphanBytes int64
	records, orphans   int
	identity           bool
}

// One concrete inactive issuer, bound to one actual live source and directory.
// Reached only through explicit typed acquisition; no config default,
// background worker or test signer. Ordinary admission defaults stay unchanged.
// mu serializes ALL own writes/recovery/verification/Close, outside gate.mu.
// Other writers must participate in the same advisory directory-inode lease.
// The existing File.Sync+directory.Sync contract is conditional on the chosen
// OS/FS/device and access/ACL model; this is not a power-loss guarantee.
type totalFileIssuer struct {
	mu                  sync.Mutex
	gate                *Budget
	owner               *totalFullObservation
	dir                 *secretfile.Dir
	lease               *secretfile.DirectoryLease
	limits              totalFileLimits
	capacity, chunkRows int
	session             [16]byte
	identityRaw         []byte // bounded constant header, never a whole Q
	identityConfirmed   bool
	committedID         uint64
	finalAttemptID      uint64 // never cleared by late failure; one live logical Q
	committedDigest     [32]byte
	last                totalSegmentWitness
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
// Dir is borrowed: Close releases only the issuer lease after its serialized
// operations join; the caller retains ownership of Dir.Close.
func newFileSegmentedBudget(cfg Config, key totalSourceKey, capacity, chunkRows int, clock func() (time.Duration, error), dir *secretfile.Dir, limits totalFileLimits, session [16]byte) (*Budget, *totalFileIssuer, error) {
	i := &totalFileIssuer{dir: dir, limits: limits, capacity: capacity, chunkRows: chunkRows, session: session}
	h, err := newSegmentedBudget(cfg, key, capacity, chunkRows, clock, i, session)
	if err != nil {
		return nil, nil, err
	}
	i.gate, i.owner = h, h.fullObservation
	// Disabled before limits, directory/lease/FS or observation clock work.
	// The original New still validates Config and direction admission.
	if i.owner.disabled {
		return h, i, nil
	}
	if i.owner.invalid != "" || !i.owner.initialized {
		return h, i, errTotalCommitUnconfirmed
	}
	if dir == nil || limits.recordBytes <= sha256.Size || limits.storeBytes <= 0 || int64(limits.recordBytes) > limits.storeBytes || limits.orphanBytes <= 0 || limits.scanBytes <= 0 || limits.storeRecords <= 0 || limits.orphanRecords <= 0 || limits.scanBatch <= 0 || limits.scanSteps <= 0 {
		return h, i, errTotalFileBudget
	}
	identity := totalSegmentDescriptor{key: key, cfg: cfg, coverageStart: i.owner.start, fromAt: i.owner.start, throughAt: i.owner.start}
	raw, err := totalIdentityRecord(identity, session, capacity, chunkRows, limits.recordBytes)
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
	account := totalFileAccount{}
	budget := i.budget()
	_, err = i.ensureRecordLocked("identity", raw, &account, &budget)
	if err == nil {
		i.identityConfirmed = true
	}
	return h, i, err
}
func (i *totalFileIssuer) budget() totalFileBudget {
	return totalFileBudget{bytes: i.limits.scanBytes, steps: i.limits.scanSteps}
}
func (i *totalFileIssuer) requireFreshLocked() error {
	scan, err := i.lease.OpenNameScan(i.limits.scanBatch, i.limits.storeRecords)
	if err != nil {
		return err
	}
	names, readErr := scan.Next()
	closeErr := scan.Close()
	if len(names) != 0 {
		return errors.Join(errTotalStoreNotFresh, closeErr)
	}
	if !errors.Is(readErr, io.EOF) || closeErr != nil || !scan.Complete() {
		return errors.Join(errTotalStoreNotFresh, readErr, closeErr)
	}
	return i.dir.DurablePreflight()
}
func (i *totalFileIssuer) boundLocked() error {
	if i.closed || i.lease == nil || i.writeRecord == nil {
		return errTotalStoreClosed
	}
	match, err := i.lease.Matches(i.dir)
	if err != nil || !match {
		return errors.Join(errTotalStoreClosed, err)
	}
	return nil
}
func (i *totalFileIssuer) Close() error {
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
func (i *totalFileIssuer) accountLocked(b *totalFileBudget, current totalSegmentWitness) (totalFileAccount, error) {
	a := totalFileAccount{}
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
				failure = errTotalFileBudget
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
					failure = errTotalFileBudget
					break
				}
				a.orphanBytes += size
				a.orphans++
			}
		}
	}
	closeErr := scan.Close()
	if failure != nil || closeErr != nil || !scan.Complete() {
		return a, errors.Join(failure, closeErr, errTotalCommitUnconfirmed)
	}
	if err := i.dir.DurablePreflight(); err != nil {
		return a, err
	}
	if a.identity {
		raw, err := i.readRecordLocked("identity", false, b)
		if err != nil || !bytes.Equal(raw, i.identityRaw) {
			return a, errors.Join(errTotalFileRecord, err)
		}
	}
	return a, nil
}

// Every canonical filename needs exact referenced membership, not just an
// old qid. Descriptor reads are charged; no unbounded history map.
func (i *totalFileIssuer) memberLocked(name string, current totalSegmentWitness, b *totalFileBudget) (bool, error) {
	qid, index, chunk, ok := totalFileName(name)
	if !ok {
		return false, errTotalFileRecord
	}
	if qid > i.committedID {
		if i.committedID == totalMaxUint || qid != i.committedID+1 || current.owner != i.owner || current.session != i.session || current.descriptor.checkpointID != qid || !i.validDescriptor(current.descriptor) {
			return false, errTotalFileRecord
		}
		count, ok := totalChunkCount(current.descriptor.rowCount, i.chunkRows)
		if !ok || chunk && index >= count {
			return false, errTotalFileRecord
		}
		return true, nil // current partial/final still charges orphan caps
	}
	if chunk {
		raw, err := i.readRecordLocked(totalFinalName(qid), false, b)
		if err != nil {
			return false, err
		}
		session, q, _, layout, count, _, err := totalDecodeFinal(raw)
		expected, ok := totalChunkCount(q.rowCount, i.chunkRows)
		if err != nil || !ok || session != i.session || q.checkpointID != qid || !i.validDescriptor(q) || layout != i.chunkRows || count != expected || index >= count {
			return false, errTotalFileRecord
		}
	}
	return false, nil
}

func (i *totalFileIssuer) readRecordLocked(name string, confirm bool, b *totalFileBudget) ([]byte, error) {
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
		return nil, errors.Join(errTotalFileBudget, file.Close())
	}
	raw := make([]byte, int(size))
	_, err = io.ReadFull(file, raw)
	var tail [1]byte
	if err == nil {
		n, tailErr := file.Read(tail[:])
		if n != 0 || !errors.Is(tailErr, io.EOF) {
			err = errTotalFileRecord
		}
	}
	if err == nil && !totalCheckedRecord(raw) {
		err = errTotalFileRecord
	}
	after, statErr := file.Stat()
	if statErr != nil || after != nil && (!os.SameFile(info, after) || info.Size() != after.Size()) {
		err = errors.Join(err, statErr, errTotalFileRecord)
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
func (i *totalFileIssuer) confirmExactLocked(name string, raw []byte, b *totalFileBudget) error {
	actual, err := i.readRecordLocked(name, true, b)
	if err != nil || !bytes.Equal(actual, raw) {
		return errors.Join(errTotalCommitUnconfirmed, err)
	}
	return i.dir.DurablePreflight()
}
func (i *totalFileIssuer) ensureRecordLocked(name string, raw []byte, a *totalFileAccount, b *totalFileBudget) (secretfile.DurableCommit, error) {
	actual, err := i.readRecordLocked(name, false, b)
	if err == nil {
		if !bytes.Equal(actual, raw) {
			return secretfile.DurableNotCommitted, errTotalFileRecord
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
		return secretfile.DurableNotCommitted, errTotalFileBudget
	}
	outcome, err := i.writeRecord(name, raw)
	if err != nil || outcome != secretfile.DurableCommitted {
		if outcome != secretfile.DurableNotCommitted && outcome != secretfile.DurableIndeterminate {
			outcome = secretfile.DurableIndeterminate
		}
		return outcome, errors.Join(errTotalCommitUnconfirmed, err)
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

func (i *totalFileIssuer) warmWitnessLocked(q totalSegmentDescriptor) (totalSegmentWitness, error) {
	if w, ok := i.gate.expectedTotalSegmentWitness(); ok && w.descriptor == q {
		return w, nil
	}
	stream, status := i.gate.beginTotalSegmentStream(q)
	if status != "STREAMING" {
		return totalSegmentWitness{}, errTotalCommitUnconfirmed
	}
	for {
		loan, status := i.gate.borrowTotalSegmentChunk(stream)
		if status != "BORROWED" {
			return totalSegmentWitness{}, errTotalCommitUnconfirmed
		}
		chunk, status := i.gate.copyTotalSegmentChunk(loan)
		released := i.gate.releaseTotalSegmentChunk(loan)
		if status != "COPIED" || !released {
			return totalSegmentWitness{}, errTotalCommitUnconfirmed
		}
		if chunk.final {
			break
		}
	}
	w, ok := i.gate.expectedTotalSegmentWitness()
	if !ok || w.descriptor != q {
		return totalSegmentWitness{}, errTotalCommitUnconfirmed
	}
	return w, nil
}
func (i *totalFileIssuer) validDescriptor(q totalSegmentDescriptor) bool {
	return q.key == i.owner.key && q.cfg == i.owner.cfg && q.coverageStart == i.owner.start && q.rowCount >= 0 && q.rowCount <= i.capacity
}

// verifyWholeLocked uses a constant multiple of recordBytes plus fold/hash
// state (retained final + chunk + confirmation/encoding buffers), not one-E
// memory or measured RSS. All chunks
// must exist; no partial coverage or latest-pointer-only reconstruction.
// confirm freshly File.Syncs exact chunks, then exact final, then parent Sync;
// matching readable bytes alone NEVER promote an indeterminate final commit.
func (i *totalFileIssuer) verifyWholeLocked(w totalSegmentWitness, confirm bool, b *totalFileBudget) ([32]byte, error) {
	var zero [32]byte
	if w.owner != i.owner || w.session != i.session || !i.validDescriptor(w.descriptor) {
		return zero, errTotalFileRecord
	}
	name := totalFinalName(w.descriptor.checkpointID)
	raw, err := i.readRecordLocked(name, false, b)
	if err != nil {
		return zero, err
	}
	session, q, digest, chunkRows, chunks, wantChain, err := totalDecodeFinal(raw)
	count, valid := totalChunkCount(q.rowCount, i.chunkRows)
	if err != nil || !valid || session != i.session || q != w.descriptor || digest != w.digest || chunkRows != i.chunkRows || chunks != count || chunks > uint64(i.limits.storeRecords) || chunks > uint64(b.steps) {
		return zero, errTotalFileRecord
	}
	fold, ok := newTotalSegmentFold(q)
	if !ok {
		return zero, errTotalFileRecord
	}
	sourceHash := sha256.New()
	writeTotalSegmentHeader(sourceHash, q)
	var chain [32]byte
	offset := 0
	for index := uint64(0); index < chunks; index++ {
		chunkRaw, err := i.readRecordLocked(totalChunkName(q.checkpointID, index), confirm, b)
		if err != nil {
			return zero, err
		}
		d := totalFileDecoder{raw: chunkRaw[:len(chunkRaw)-sha256.Size]}
		d.magic(totalChunkMagic)
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
			return zero, errTotalFileRecord
		}
		for rowIndex := 0; rowIndex < n; rowIndex++ {
			row := d.row()
			if d.err != nil {
				return zero, d.err
			}
			if _, _, ok := fold.row(row); !ok {
				return zero, errTotalFileRecord
			}
			writeTotalSegmentRow(sourceHash, row)
		}
		if d.err != nil || d.cursor != len(d.raw) {
			return zero, errTotalFileRecord
		}
		offset += n
		chain = totalNextChunkChain(chain, index, chunkRaw)
	}
	if _, _, ok := fold.finish(); !ok || offset != q.rowCount || chain != wantChain {
		return zero, errTotalFileRecord
	}
	var actual [32]byte
	sourceHash.Sum(actual[:0])
	if actual != w.digest {
		return zero, errTotalFileRecord
	}
	if confirm {
		if err := i.confirmExactLocked(name, raw, b); err != nil {
			return zero, err
		}
	}
	return sha256.Sum256(raw), nil
}

func (i *totalFileIssuer) historyLocked(b *totalFileBudget) error {
	if i.committedID > uint64(i.limits.storeRecords) {
		return errTotalFileBudget
	}
	var previousID, sequence uint64
	previousAt := i.owner.start
	var previousDigest [32]byte
	var carry totalSegmentCarry
	for id := uint64(1); id <= i.committedID; id++ {
		raw, err := i.readRecordLocked(totalFinalName(id), false, b)
		if err != nil {
			return err
		}
		session, q, digest, _, _, _, err := totalDecodeFinal(raw)
		if err != nil || session != i.session || q.checkpointID != id || q.previousID != previousID || q.previousDigest != previousDigest || q.fromSeq != sequence || q.fromAt != previousAt || q.fromCarry != carry {
			return errTotalFileRecord
		}
		w := totalSegmentWitness{owner: i.owner, session: i.session, descriptor: q, digest: digest}
		if _, err := i.verifyWholeLocked(w, false, b); err != nil {
			return err
		}
		previousID, sequence, previousAt, previousDigest, carry = id, q.throughSeq, q.throughAt, digest, q.throughCarry
	}
	if previousDigest != i.committedDigest {
		return errTotalFileRecord
	}
	return nil
}
func (i *totalFileIssuer) prepareLocked(b *totalFileBudget, current totalSegmentWitness) (totalFileAccount, error) {
	a, err := i.accountLocked(b, current)
	if err != nil {
		return a, err
	}
	if !a.identity {
		if i.identityConfirmed {
			return a, errTotalFileRecord
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
func (i *totalFileIssuer) acceptWitnessLocked(w totalSegmentWitness) bool {
	if w.owner != i.owner || w.session != i.session || !i.validDescriptor(w.descriptor) {
		return false
	}
	q := w.descriptor
	if q.checkpointID == i.committedID {
		return i.last == w
	}
	if i.committedID == totalMaxUint || q.checkpointID != i.committedID+1 || q.previousID != i.committedID || q.previousDigest != i.committedDigest {
		return false
	}
	if i.committedID == 0 {
		return q.fromSeq == 0 && q.fromAt == i.owner.start && q.fromCarry == (totalSegmentCarry{})
	}
	return q.fromSeq == i.last.descriptor.throughSeq && q.fromAt == i.last.descriptor.throughAt && q.fromCarry == i.last.descriptor.throughCarry
}
func (i *totalFileIssuer) recordCommittedLocked(w totalSegmentWitness) {
	i.committedID = w.descriptor.checkpointID
	i.committedDigest = w.digest
	i.last = w
}

// Outcomes describe this call's ability to confirm the logical Q, never
// undo a historical commit. Earlier possible visibility or confirmation plus
// failed revalidation is INDETERMINATE/no capsule; history is not reset.
func (i *totalFileIssuer) failedConfirmation(q totalSegmentDescriptor) secretfile.DurableCommit {
	if q.checkpointID != 0 && (q.checkpointID <= i.committedID || q.checkpointID == i.finalAttemptID) {
		return secretfile.DurableIndeterminate
	}
	return secretfile.DurableNotCommitted
}

// Commit writes actual Q data in bounded C chunks and a fixed final record.
// It never calls ACK while holding issuer.mu; caller submits the returned
// capsule through the original source's independent lease/quiescence checks.
func (i *totalFileIssuer) Commit(q totalSegmentDescriptor) (totalSegmentReceipt, secretfile.DurableCommit, error) {
	var empty totalSegmentReceipt
	if i == nil {
		return empty, secretfile.DurableNotCommitted, errTotalStoreClosed
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.boundLocked(); err != nil {
		return empty, i.failedConfirmation(q), err
	}
	w, err := i.warmWitnessLocked(q)
	if err != nil || !i.acceptWitnessLocked(w) {
		return empty, i.failedConfirmation(q), errors.Join(errTotalCommitUnconfirmed, err)
	}
	budget := i.budget()
	account, err := i.prepareLocked(&budget, w)
	if err != nil {
		return empty, i.failedConfirmation(q), err
	}
	if _, err := i.dir.Lstat(totalFinalName(q.checkpointID)); err == nil {
		i.finalAttemptID = q.checkpointID
		proof, err := i.verifyWholeLocked(w, true, &budget)
		if err != nil {
			return empty, secretfile.DurableIndeterminate, err
		}
		i.recordCommittedLocked(w)
		return totalSegmentReceipt{witness: w, proof: proof}, secretfile.DurableCommitted, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return empty, i.failedConfirmation(q), err
	}
	stream, status := i.gate.beginTotalSegmentStream(q)
	if status != "STREAMING" {
		return empty, i.failedConfirmation(q), errTotalCommitUnconfirmed
	}
	var chain [32]byte
	var chunks uint64
	for {
		loan, status := i.gate.borrowTotalSegmentChunk(stream)
		if status != "BORROWED" {
			return empty, i.failedConfirmation(q), errTotalCommitUnconfirmed
		}
		chunk, status := i.gate.copyTotalSegmentChunk(loan)
		if status != "COPIED" {
			_ = i.gate.releaseTotalSegmentChunk(loan)
			return empty, i.failedConfirmation(q), errTotalCommitUnconfirmed
		}
		var raw []byte
		if len(chunk.rows) != 0 {
			raw, err = totalChunkRecord(w, chunks, chunk, i.chunkRows, i.limits.recordBytes)
			if err == nil {
				_, err = i.ensureRecordLocked(totalChunkName(q.checkpointID, chunks), raw, &account, &budget)
			}
		}
		released := i.gate.releaseTotalSegmentChunk(loan)
		if err != nil || !released {
			return empty, i.failedConfirmation(q), errors.Join(errTotalCommitUnconfirmed, err)
		}
		if len(raw) != 0 {
			chain = totalNextChunkChain(chain, chunks, raw)
			chunks++
		}
		if chunk.final {
			break
		}
	}
	final, err := totalFinalRecord(w, i.chunkRows, chunks, chain, i.limits.recordBytes)
	if err != nil {
		return empty, i.failedConfirmation(q), err
	}
	priorFinal := i.failedConfirmation(q) == secretfile.DurableIndeterminate
	i.finalAttemptID = q.checkpointID // BEFORE possible final visibility, never clear on error
	outcome, err := i.ensureRecordLocked(totalFinalName(q.checkpointID), final, &account, &budget)
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
	return totalSegmentReceipt{witness: w, proof: proof}, secretfile.DurableCommitted, nil
}

// Real filesystem authority, not a signer: proof identifies the exact final
// record but is insufficient without this bound source/store and fresh strict
// readback+sync confirmation. Recovery is ONLY on this still-live issuer;
// fresh construction over any old record is refused before it can bind.
func (i *totalFileIssuer) verifyTotalSegment(receipt totalSegmentReceipt) (totalSegmentCommit, bool) {
	if i == nil {
		return totalSegmentCommit{}, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.boundLocked(); err != nil {
		return totalSegmentCommit{}, false
	}
	current, known := i.gate.expectedTotalSegmentWitness()
	if !known || current != receipt.witness || !i.acceptWitnessLocked(current) {
		return totalSegmentCommit{}, false
	}
	budget := i.budget()
	if _, err := i.prepareLocked(&budget, current); err != nil {
		return totalSegmentCommit{}, false
	}
	proof, err := i.verifyWholeLocked(receipt.witness, true, &budget)
	if err != nil || proof != receipt.proof {
		return totalSegmentCommit{}, false
	}
	i.recordCommittedLocked(receipt.witness)
	return totalSegmentCommit{witness: receipt.witness}, true
}
