// Package qrcode is a dependency-free QR encoder for the corsa: contact
// link (docs/protocol/identity-lookup.md §4.8): byte mode, error-correction
// level M, versions 1–14 — comfortably above the ~230–260-byte link with
// the margin §4.8 budgets (version ~10–11 at M).
//
// Level M is a deliberate §4.8 choice: ECC here trades density against
// physical damage to the printed code, not against forgery — the box-key
// binding signature inside the link catches substitution, but cannot repair
// scuffed modules.
//
// The implementation follows ISO/IEC 18004 directly; correctness is pinned
// by the package tests two independent ways — a published codeword vector,
// and a from-the-spec decoder that unmasks, reads and syndrome-checks every
// produced symbol.
package qrcode

import (
	"errors"
	"fmt"
)

// Matrix is one encoded symbol: Size×Size modules, true = dark. The quiet
// zone (4 modules of light on every side) is the renderer's to add.
type Matrix struct {
	modules [][]bool
	size    int
}

// Size returns the symbol width in modules.
func (m *Matrix) Size() int { return m.size }

// Dark reports whether the module at (x, y) is dark.
func (m *Matrix) Dark(x, y int) bool { return m.modules[y][x] }

// ErrPayloadTooLarge marks input beyond version 14 at level M.
var ErrPayloadTooLarge = errors.New("qrcode: payload exceeds the supported capacity")

// versionSpec is one (version, level) row of the ISO 18004 table 9: the
// error-correction codewords per block and the two block groups.
type versionSpec struct {
	ecPerBlock  int
	group1Count int
	group1Data  int
	group2Count int
	group2Data  int
}

func (v versionSpec) dataCodewords() int {
	return v.group1Count*v.group1Data + v.group2Count*v.group2Data
}

// mediumSpecs are the level-M rows for versions 1–14 (index 0 = version 1).
var mediumSpecs = []versionSpec{
	{10, 1, 16, 0, 0},
	{16, 1, 28, 0, 0},
	{26, 1, 44, 0, 0},
	{18, 2, 32, 0, 0},
	{24, 2, 43, 0, 0},
	{16, 4, 27, 0, 0},
	{18, 4, 31, 0, 0},
	{22, 2, 38, 2, 39},
	{22, 3, 36, 2, 37},
	{26, 4, 43, 1, 44},
	{30, 1, 50, 4, 51},
	{22, 6, 36, 2, 37},
	{22, 8, 37, 1, 38},
	{24, 4, 40, 5, 41},
}

// alignmentCenters lists the alignment-pattern centre coordinates per
// version (index 0 = version 1, which has none).
var alignmentCenters = [][]int{
	{},
	{6, 18},
	{6, 22},
	{6, 26},
	{6, 30},
	{6, 34},
	{6, 22, 38},
	{6, 24, 42},
	{6, 26, 46},
	{6, 28, 52},
	{6, 30, 56},
	{6, 32, 60},
	{6, 34, 64},
	{6, 26, 46, 66},
}

// Encode renders payload as a level-M symbol in the smallest version that
// fits.
func Encode(payload []byte) (*Matrix, error) {
	version, spec, err := pickVersion(len(payload))
	if err != nil {
		return nil, err
	}
	data := buildDataCodewords(payload, version, spec)
	final := interleave(data, spec)
	return buildSymbol(version, final), nil
}

// pickVersion finds the smallest version whose data capacity holds the
// byte-mode header plus payload.
func pickVersion(payloadLen int) (int, versionSpec, error) {
	for index, spec := range mediumSpecs {
		version := index + 1
		headerBits := 4 + charCountBits(version)
		if (headerBits+8*payloadLen+7)/8 <= spec.dataCodewords() {
			return version, spec, nil
		}
	}
	return 0, versionSpec{}, fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, payloadLen)
}

// charCountBits is the byte-mode character-count width: 8 bits through
// version 9, 16 bits from version 10.
func charCountBits(version int) int {
	if version <= 9 {
		return 8
	}
	return 16
}

// bitWriter accumulates the data bit stream.
type bitWriter struct {
	bits []byte
	used int
}

func (w *bitWriter) write(value uint32, count int) {
	for shift := count - 1; shift >= 0; shift-- {
		if w.used%8 == 0 {
			w.bits = append(w.bits, 0)
		}
		if value>>uint(shift)&1 == 1 {
			w.bits[w.used/8] |= 0x80 >> uint(w.used%8)
		}
		w.used++
	}
}

// buildDataCodewords encodes the byte-mode segment, the terminator and the
// alternating pad bytes.
func buildDataCodewords(payload []byte, version int, spec versionSpec) []byte {
	writer := &bitWriter{}
	writer.write(0b0100, 4) // byte mode
	writer.write(uint32(len(payload)), charCountBits(version))
	for _, b := range payload {
		writer.write(uint32(b), 8)
	}

	capacityBits := spec.dataCodewords() * 8
	terminator := capacityBits - writer.used
	if terminator > 4 {
		terminator = 4
	}
	writer.write(0, terminator)
	if pad := writer.used % 8; pad != 0 {
		writer.write(0, 8-pad)
	}
	padBytes := [2]byte{0xEC, 0x11}
	for i := 0; writer.used < capacityBits; i++ {
		writer.write(uint32(padBytes[i%2]), 8)
	}
	return writer.bits
}

// interleave splits the data into the spec's blocks, appends RS codewords
// and interleaves both halves per ISO 18004 §8.6.
func interleave(data []byte, spec versionSpec) []byte {
	type block struct{ data, ec []byte }
	blocks := make([]block, 0, spec.group1Count+spec.group2Count)
	offset := 0
	for i := 0; i < spec.group1Count; i++ {
		chunk := data[offset : offset+spec.group1Data]
		blocks = append(blocks, block{data: chunk, ec: rsEncode(chunk, spec.ecPerBlock)})
		offset += spec.group1Data
	}
	for i := 0; i < spec.group2Count; i++ {
		chunk := data[offset : offset+spec.group2Data]
		blocks = append(blocks, block{data: chunk, ec: rsEncode(chunk, spec.ecPerBlock)})
		offset += spec.group2Data
	}

	maxData := spec.group1Data
	if spec.group2Data > maxData {
		maxData = spec.group2Data
	}
	out := make([]byte, 0, len(data)+len(blocks)*spec.ecPerBlock)
	for i := 0; i < maxData; i++ {
		for _, b := range blocks {
			if i < len(b.data) {
				out = append(out, b.data[i])
			}
		}
	}
	for i := 0; i < spec.ecPerBlock; i++ {
		for _, b := range blocks {
			out = append(out, b.ec[i])
		}
	}
	return out
}
