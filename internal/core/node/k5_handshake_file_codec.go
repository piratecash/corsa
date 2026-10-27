package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var errHandshakeFileRecord = errors.New("invalid bounded handshake evidence record")
var errHandshakeFileBudget = errors.New("handshake evidence budget exhausted")

const handshakeStoreMagic = "corsa-k5-store-v1\x00"
const handshakeChunkMagic = "corsa-k5-chunk-v1\x00"
const handshakeFinalMagic = "corsa-k5-final-v1\x00"
const handshakeCanonicalMagic = "corsa-k5-handshake-segment-v2\x00"

// Checked bounded capture implements hash.Hash ONLY to reuse the exact
// existing SHA-format writers. Their ignored Write errors never reach a file:
// this sticky status is checked before any DurableWrite. No whole-Q encoding.
type handshakeFileCapture struct {
	raw   []byte
	limit int
	err   error
}

func newHandshakeFileCapture(limit int) *handshakeFileCapture {
	e := &handshakeFileCapture{limit: limit - sha256.Size}
	if limit <= sha256.Size {
		e.err = errHandshakeFileBudget
		return e
	}
	e.raw = make([]byte, 0, limit)
	return e
}
func (e *handshakeFileCapture) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	if len(p) > e.limit-len(e.raw) {
		e.err = errHandshakeFileBudget
		return 0, e.err
	}
	e.raw = append(e.raw, p...)
	return len(p), nil
}
func (e *handshakeFileCapture) Sum(p []byte) []byte {
	h := sha256.Sum256(e.raw)
	return append(p, h[:]...)
}
func (e *handshakeFileCapture) Reset()       { e.raw = e.raw[:0]; e.err = nil }
func (*handshakeFileCapture) Size() int      { return sha256.Size }
func (*handshakeFileCapture) BlockSize() int { return sha256.BlockSize }
func (e *handshakeFileCapture) finish() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	sum := sha256.Sum256(e.raw)
	return append(e.raw, sum[:]...), nil
}
func handshakeIdentityRecord(q handshakeSegmentDescriptor, session [16]byte, capacity, chunkRows, limit int) ([]byte, error) {
	e := newHandshakeFileCapture(limit)
	_, _ = e.Write([]byte(handshakeStoreMagic))
	_, _ = e.Write(session[:])
	writeHandshakeSegmentHeader(e, q)
	writeHandshakeSegmentUint(e, uint64(capacity))
	writeHandshakeSegmentUint(e, uint64(chunkRows))
	return e.finish()
}
func handshakeChunkRecord(w handshakeSegmentWitness, index uint64, chunk handshakeSegmentChunk, chunkRows, limit int) ([]byte, error) {
	e := newHandshakeFileCapture(limit)
	_, _ = e.Write([]byte(handshakeChunkMagic))
	_, _ = e.Write(w.session[:])
	writeHandshakeSegmentUint(e, w.descriptor.checkpointID)
	_, _ = e.Write(w.digest[:])
	writeHandshakeSegmentUint(e, uint64(chunkRows))
	writeHandshakeSegmentUint(e, index)
	writeHandshakeSegmentUint(e, uint64(chunk.offset))
	writeHandshakeSegmentUint(e, uint64(len(chunk.rows)))
	for _, row := range chunk.rows {
		writeHandshakeSegmentRow(e, row)
	}
	return e.finish()
}
func handshakeFinalRecord(w handshakeSegmentWitness, chunkRows int, chunks uint64, chain [32]byte, limit int) ([]byte, error) {
	e := newHandshakeFileCapture(limit)
	_, _ = e.Write([]byte(handshakeFinalMagic))
	_, _ = e.Write(w.session[:])
	writeHandshakeSegmentHeader(e, w.descriptor)
	_, _ = e.Write(w.digest[:])
	writeHandshakeSegmentUint(e, uint64(chunkRows))
	writeHandshakeSegmentUint(e, chunks)
	_, _ = e.Write(chain[:])
	return e.finish()
}
func handshakeNextChunkChain(previous [32]byte, index uint64, raw []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(previous[:])
	writeHandshakeSegmentUint(h, index)
	sum := sha256.Sum256(raw)
	_, _ = h.Write(sum[:])
	var result [32]byte
	h.Sum(result[:0])
	return result
}
func handshakeChunkName(qid, index uint64) string {
	return fmt.Sprintf("q%016x-c%016x.chunk", qid, index)
}
func handshakeFinalName(qid uint64) string { return fmt.Sprintf("q%016x.final", qid) }
func handshakeFileName(name string) (qid, index uint64, chunk, recognized bool) {
	if len(name) == 23 && name[0] == 'q' && name[17:] == ".final" {
		q, err := strconv.ParseUint(name[1:17], 16, 64)
		return q, 0, false, err == nil && q != 0 && name == handshakeFinalName(q)
	}
	if len(name) == 41 && name[0] == 'q' && name[17:19] == "-c" && name[35:] == ".chunk" {
		q, err := strconv.ParseUint(name[1:17], 16, 64)
		if err != nil || q == 0 {
			return 0, 0, false, false
		}
		n, err := strconv.ParseUint(name[19:35], 16, 64)
		return q, n, true, err == nil && name == handshakeChunkName(q, n)
	}
	return 0, 0, false, false
}
func handshakeCheckedRecord(raw []byte) bool {
	if len(raw) < sha256.Size {
		return false
	}
	sum := sha256.Sum256(raw[:len(raw)-sha256.Size])
	return bytes.Equal(sum[:], raw[len(raw)-sha256.Size:])
}

type handshakeFileDecoder struct {
	raw    []byte
	cursor int
	err    error
}

func (d *handshakeFileDecoder) take(n int) []byte {
	if d.err != nil || n < 0 || n > len(d.raw)-d.cursor {
		d.err = errHandshakeFileRecord
		return nil
	}
	b := d.raw[d.cursor : d.cursor+n]
	d.cursor += n
	return b
}
func (d *handshakeFileDecoder) magic(m string) {
	if !bytes.Equal(d.take(len(m)), []byte(m)) {
		d.err = errHandshakeFileRecord
	}
}
func (d *handshakeFileDecoder) uint() uint64 {
	b := d.take(8)
	if len(b) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (d *handshakeFileDecoder) integer() int {
	n := d.uint()
	if n > uint64(^uint(0)>>1) {
		d.err = errHandshakeFileRecord
		return 0
	}
	return int(n)
}
func (d *handshakeFileDecoder) duration() time.Duration {
	n := d.uint()
	if n > uint64(1<<63-1) {
		d.err = errHandshakeFileRecord
		return 0
	}
	return time.Duration(n)
}
func (d *handshakeFileDecoder) boolean() bool {
	n := d.uint()
	if n > 1 {
		d.err = errHandshakeFileRecord
	}
	return n == 1
}
func (d *handshakeFileDecoder) row() handshakeFullTransition {
	return handshakeFullTransition{sequence: d.uint(), full: d.boolean(), count: d.integer(), lower: d.duration(), upper: d.duration(), done: d.boolean(), valid: d.boolean()}
}
func (d *handshakeFileDecoder) carry() handshakeSegmentCarry {
	return handshakeSegmentCarry{full: d.boolean(), entry: d.row()}
}
func (d *handshakeFileDecoder) header() handshakeSegmentDescriptor {
	d.magic(handshakeCanonicalMagic)
	n := d.uint()
	if n == 0 || n > 128 {
		d.err = errHandshakeFileRecord
		return handshakeSegmentDescriptor{}
	}
	q := handshakeSegmentDescriptor{}
	q.key.anchor = string(d.take(int(n)))
	copy(q.key.epoch[:], d.take(16))
	copy(q.key.clockDomain[:], d.take(16))
	copy(q.key.configuration[:], d.take(16))
	kind := d.uint()
	if kind != uint64(k5HandshakeNode) {
		d.err = errHandshakeFileRecord
	}
	q.key.kind = k5CeilingKind(kind)
	q.bounds = handshakeBounds{node: d.integer(), loopback: d.integer(), perSource: d.integer()}
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
func handshakeDecodeFinal(raw []byte) (session [16]byte, q handshakeSegmentDescriptor, digest [32]byte, chunkRows int, chunks uint64, chain [32]byte, err error) {
	if !handshakeCheckedRecord(raw) {
		err = errHandshakeFileRecord
		return
	}
	d := handshakeFileDecoder{raw: raw[:len(raw)-sha256.Size]}
	d.magic(handshakeFinalMagic)
	copy(session[:], d.take(16))
	q = d.header()
	copy(digest[:], d.take(32))
	chunkRows = d.integer()
	chunks = d.uint()
	copy(chain[:], d.take(32))
	if d.err != nil || d.cursor != len(d.raw) {
		err = errHandshakeFileRecord
	}
	return
}
func handshakeChunkCount(rows, chunkRows int) (uint64, bool) {
	if rows < 0 || chunkRows <= 0 {
		return 0, false
	}
	n := rows / chunkRows
	if rows%chunkRows != 0 {
		n++
	}
	return uint64(n), true
}
