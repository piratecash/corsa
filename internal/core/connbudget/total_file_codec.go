package connbudget

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var errTotalFileRecord = errors.New("invalid bounded total evidence record")
var errTotalFileBudget = errors.New("total evidence budget exhausted")

const totalStoreMagic = "corsa-k5-total-store-v1\x00"
const totalChunkMagic = "corsa-k5-total-chunk-v1\x00"
const totalFinalMagic = "corsa-k5-total-final-v1\x00"
const totalCanonicalMagic = "corsa-k5-connection-total-segment-v2\x00"

// Checked bounded capture implements hash.Hash ONLY to reuse the exact
// existing SHA-format writers. Their ignored Write errors never reach a file:
// this sticky status is checked before any DurableWrite. No whole-Q encoding.
type totalFileCapture struct {
	raw   []byte
	limit int
	err   error
}

func newTotalFileCapture(limit int) *totalFileCapture {
	e := &totalFileCapture{limit: limit - sha256.Size}
	if limit <= sha256.Size {
		e.err = errTotalFileBudget
		return e
	}
	e.raw = make([]byte, 0, limit)
	return e
}
func (e *totalFileCapture) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	if len(p) > e.limit-len(e.raw) {
		e.err = errTotalFileBudget
		return 0, e.err
	}
	e.raw = append(e.raw, p...)
	return len(p), nil
}
func (e *totalFileCapture) Sum(p []byte) []byte {
	h := sha256.Sum256(e.raw)
	return append(p, h[:]...)
}
func (e *totalFileCapture) Reset() {
	e.raw = e.raw[:0]
	e.err = nil
	if e.limit <= 0 {
		e.err = errTotalFileBudget
	}
}
func (*totalFileCapture) Size() int      { return sha256.Size }
func (*totalFileCapture) BlockSize() int { return sha256.BlockSize }
func (e *totalFileCapture) finish() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	sum := sha256.Sum256(e.raw)
	return append(e.raw, sum[:]...), nil
}
func totalIdentityRecord(q totalSegmentDescriptor, session [16]byte, capacity, chunkRows, limit int) ([]byte, error) {
	e := newTotalFileCapture(limit)
	_, _ = e.Write([]byte(totalStoreMagic))
	_, _ = e.Write(session[:])
	writeTotalSegmentHeader(e, q)
	writeTotalSegmentUint(e, uint64(capacity))
	writeTotalSegmentUint(e, uint64(chunkRows))
	return e.finish()
}
func totalChunkRecord(w totalSegmentWitness, index uint64, chunk totalSegmentChunk, chunkRows, limit int) ([]byte, error) {
	e := newTotalFileCapture(limit)
	_, _ = e.Write([]byte(totalChunkMagic))
	_, _ = e.Write(w.session[:])
	writeTotalSegmentUint(e, w.descriptor.checkpointID)
	_, _ = e.Write(w.digest[:])
	writeTotalSegmentUint(e, uint64(chunkRows))
	writeTotalSegmentUint(e, index)
	writeTotalSegmentUint(e, uint64(chunk.offset))
	writeTotalSegmentUint(e, uint64(len(chunk.rows)))
	for _, row := range chunk.rows {
		writeTotalSegmentRow(e, row)
	}
	return e.finish()
}
func totalFinalRecord(w totalSegmentWitness, chunkRows int, chunks uint64, chain [32]byte, limit int) ([]byte, error) {
	e := newTotalFileCapture(limit)
	_, _ = e.Write([]byte(totalFinalMagic))
	_, _ = e.Write(w.session[:])
	writeTotalSegmentHeader(e, w.descriptor)
	_, _ = e.Write(w.digest[:])
	writeTotalSegmentUint(e, uint64(chunkRows))
	writeTotalSegmentUint(e, chunks)
	_, _ = e.Write(chain[:])
	return e.finish()
}
func totalNextChunkChain(previous [32]byte, index uint64, raw []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(previous[:])
	writeTotalSegmentUint(h, index)
	sum := sha256.Sum256(raw)
	_, _ = h.Write(sum[:])
	var result [32]byte
	h.Sum(result[:0])
	return result
}
func totalChunkName(qid, index uint64) string {
	return fmt.Sprintf("q%016x-c%016x.chunk", qid, index)
}
func totalFinalName(qid uint64) string { return fmt.Sprintf("q%016x.final", qid) }
func totalFileName(name string) (qid, index uint64, chunk, recognized bool) {
	if len(name) == 23 && name[0] == 'q' && name[17:] == ".final" {
		q, err := strconv.ParseUint(name[1:17], 16, 64)
		return q, 0, false, err == nil && q != 0 && name == totalFinalName(q)
	}
	if len(name) == 41 && name[0] == 'q' && name[17:19] == "-c" && name[35:] == ".chunk" {
		q, err := strconv.ParseUint(name[1:17], 16, 64)
		if err != nil || q == 0 {
			return 0, 0, false, false
		}
		n, err := strconv.ParseUint(name[19:35], 16, 64)
		return q, n, true, err == nil && name == totalChunkName(q, n)
	}
	return 0, 0, false, false
}
func totalCheckedRecord(raw []byte) bool {
	if len(raw) < sha256.Size {
		return false
	}
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	return bytes.Equal(sum[:], raw[len(raw)-sha256.Size:])
}

type totalFileDecoder struct {
	raw    []byte
	cursor int
	err    error
}

func (d *totalFileDecoder) take(n int) []byte {
	if d.err != nil || n < 0 || n > len(d.raw)-d.cursor {
		d.err = errTotalFileRecord
		return nil
	}
	b := d.raw[d.cursor : d.cursor+n]
	d.cursor += n
	return b
}
func (d *totalFileDecoder) magic(m string) {
	if !bytes.Equal(d.take(len(m)), []byte(m)) {
		d.err = errTotalFileRecord
	}
}
func (d *totalFileDecoder) uint() uint64 {
	b := d.take(8)
	if len(b) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (d *totalFileDecoder) integer() int {
	n := d.uint()
	if n > uint64(^uint(0)>>1) {
		d.err = errTotalFileRecord
		return 0
	}
	return int(n)
}
func (d *totalFileDecoder) duration() time.Duration {
	n := d.uint()
	if n > uint64(1<<63-1) {
		d.err = errTotalFileRecord
		return 0
	}
	return time.Duration(n)
}
func (d *totalFileDecoder) boolean() bool {
	n := d.uint()
	if n > 1 {
		d.err = errTotalFileRecord
	}
	return n == 1
}
func (d *totalFileDecoder) row() totalFullTransition {
	return totalFullTransition{sequence: d.uint(), full: d.boolean(), count: d.integer(), lower: d.duration(), upper: d.duration(), done: d.boolean(), valid: d.boolean()}
}
func (d *totalFileDecoder) carry() totalSegmentCarry {
	return totalSegmentCarry{full: d.boolean(), entry: d.row()}
}
func (d *totalFileDecoder) header() totalSegmentDescriptor {
	d.magic(totalCanonicalMagic)
	n := d.uint()
	if n == 0 || n > uint64(len(d.raw)-d.cursor) || n > uint64(^uint(0)>>1) {
		d.err = errTotalFileRecord
		return totalSegmentDescriptor{}
	}
	q := totalSegmentDescriptor{}
	q.key.anchor = string(d.take(int(n)))
	copy(q.key.epoch[:], d.take(16))
	copy(q.key.clockDomain[:], d.take(16))
	copy(q.key.configuration[:], d.take(16))
	q.cfg = Config{Total: d.integer(), OutboundReserve: d.integer(), MaxOutbound: d.integer(), MaxInbound: d.integer(), MaxAuxiliary: d.integer()}
	q.coverageStart = d.duration()
	q.checkpointID = d.uint()
	q.previousID = d.uint()
	copy(q.previousDigest[:], d.take(32))
	q.fromSeq = d.uint()
	q.throughSeq = d.uint()
	q.fromAt = d.duration()
	q.throughAt = d.duration()
	q.rowCount = d.integer()
	q.fromCarry = d.carry()
	q.throughCarry = d.carry()
	return q
}
func totalDecodeFinal(raw []byte) (session [16]byte, q totalSegmentDescriptor, digest [32]byte, chunkRows int, chunks uint64, chain [32]byte, err error) {
	if !totalCheckedRecord(raw) {
		err = errTotalFileRecord
		return
	}
	d := totalFileDecoder{raw: raw[:len(raw)-sha256.Size]}
	d.magic(totalFinalMagic)
	copy(session[:], d.take(16))
	q = d.header()
	copy(digest[:], d.take(32))
	chunkRows = d.integer()
	chunks = d.uint()
	copy(chain[:], d.take(32))
	if d.err != nil || d.cursor != len(d.raw) {
		err = errTotalFileRecord
	}
	return
}
func totalChunkCount(rows, chunkRows int) (uint64, bool) {
	if rows < 0 || chunkRows <= 0 {
		return 0, false
	}
	n := rows / chunkRows
	if rows%chunkRows != 0 {
		n++
	}
	return uint64(n), true
}
