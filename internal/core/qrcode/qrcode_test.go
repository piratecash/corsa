package qrcode

import (
	"bytes"
	"strings"
	"testing"
)

// The tests verify the encoder the only way possible without a camera: an
// INDEPENDENT decoder written from the specification's read-side tables.
// The two mathematical checks make it strong: the format/version words must
// be valid BCH/Golay codewords when read in the standard order (a
// misplaced or reversed sequence is almost surely not a codeword), and
// every deinterleaved Reed-Solomon block must have all-zero syndromes (any
// placement or interleaving slip scrambles them with overwhelming
// probability).

// --- read-side helpers (independent of the builder's code paths) ---

// readFormat reads the first format-info copy in the standard order, LSB
// first, and returns (maskID, ok) after the BCH validity check.
func readFormat(m *Matrix) (int, bool) {
	coords := [15][2]int{
		{8, 0},
		{8, 1},
		{8, 2},
		{8, 3},
		{8, 4},
		{8, 5},
		{8, 7},
		{8, 8},
		{7, 8},
		{5, 8},
		{4, 8},
		{3, 8},
		{2, 8},
		{1, 8},
		{0, 8},
	}
	bits := uint32(0)
	for i, c := range coords {
		if m.Dark(c[0], c[1]) {
			bits |= 1 << uint(i)
		}
	}
	bits ^= formatInfoMask
	// BCH(15,5) validity: the whole word must divide by 0x537.
	remainder := bits
	for i := 14; i >= 10; i-- {
		if remainder>>uint(i)&1 == 1 {
			remainder ^= formatInfoPoly << uint(i-10)
		}
	}
	if remainder != 0 {
		return 0, false
	}
	if level := bits >> 13; level != 0b00 { // M
		return 0, false
	}
	return int(bits >> 10 & 0b111), true
}

// readVersionInfo reads the bottom-left version block and Golay-checks it.
func readVersionInfo(m *Matrix) (int, bool) {
	size := m.Size()
	bits := uint32(0)
	for i := 0; i < 18; i++ {
		if m.Dark(i/3, size-11+i%3) {
			bits |= 1 << uint(i)
		}
	}
	remainder := bits
	for i := 17; i >= 12; i-- {
		if remainder>>uint(i)&1 == 1 {
			remainder ^= versionInfoPoly << uint(i-12)
		}
	}
	if remainder != 0 {
		return 0, false
	}
	return int(bits >> 12), true
}

// functionMap rebuilds the reserved-module map from the spec alone.
func functionMap(version, size int) [][]bool {
	reserved := make([][]bool, size)
	for i := range reserved {
		reserved[i] = make([]bool, size)
	}
	mark := func(x, y int) {
		if x >= 0 && y >= 0 && x < size && y < size {
			reserved[y][x] = true
		}
	}
	for _, origin := range [][2]int{{0, 0}, {size - 7, 0}, {0, size - 7}} {
		for dy := -1; dy <= 7; dy++ {
			for dx := -1; dx <= 7; dx++ {
				mark(origin[0]+dx, origin[1]+dy)
			}
		}
	}
	centers := alignmentCenters[version-1]
	for _, cy := range centers {
		for _, cx := range centers {
			if reserved[cy][cx] {
				continue
			}
			for dy := -2; dy <= 2; dy++ {
				for dx := -2; dx <= 2; dx++ {
					mark(cx+dx, cy+dy)
				}
			}
		}
	}
	for i := 0; i < size; i++ {
		mark(i, 6)
		mark(6, i)
	}
	for i := 0; i <= 8; i++ {
		mark(8, i)
		mark(i, 8)
	}
	for i := 0; i < 8; i++ {
		mark(size-1-i, 8)
		mark(8, size-1-i)
	}
	if version >= 7 {
		for i := 0; i < 18; i++ {
			mark(i/3, size-11+i%3)
			mark(size-11+i%3, i/3)
		}
	}
	return reserved
}

// decode reads the symbol back to its payload, failing loudly at the first
// inconsistency.
func decode(t *testing.T, m *Matrix) []byte {
	t.Helper()
	size := m.Size()
	version := (size - 17) / 4

	mask, ok := readFormat(m)
	if !ok {
		t.Fatal("format information is not a valid BCH codeword for level M")
	}
	if version >= 7 {
		got, ok := readVersionInfo(m)
		if !ok || got != version {
			t.Fatalf("version info invalid or mismatched: got %d ok=%v want %d", got, ok, version)
		}
	}

	reserved := functionMap(version, size)
	predicate := maskPredicates[mask]

	var stream []byte
	bitCount := 0
	upward := true
	for right := size - 1; right >= 1; right -= 2 {
		if right == 6 {
			right = 5
		}
		for step := 0; step < size; step++ {
			y := step
			if upward {
				y = size - 1 - step
			}
			for _, x := range [2]int{right, right - 1} {
				if reserved[y][x] {
					continue
				}
				dark := m.Dark(x, y)
				if predicate(x, y) {
					dark = !dark
				}
				if bitCount%8 == 0 {
					stream = append(stream, 0)
				}
				if dark {
					stream[bitCount/8] |= 0x80 >> uint(bitCount%8)
				}
				bitCount++
			}
		}
		upward = !upward
	}

	spec := mediumSpecs[version-1]
	blocks := deinterleave(t, stream, spec)
	var data []byte
	for _, block := range blocks {
		syndromeCheck(t, block, spec.ecPerBlock)
		data = append(data, block[:len(block)-spec.ecPerBlock]...)
	}

	// Byte-mode parse.
	reader := &bitReader{data: data}
	if mode := reader.read(4); mode != 0b0100 {
		t.Fatalf("mode = %04b, want byte mode", mode)
	}
	count := reader.read(charCountBits(version))
	payload := make([]byte, 0, count)
	for i := 0; i < int(count); i++ {
		payload = append(payload, byte(reader.read(8)))
	}
	return payload
}

type bitReader struct {
	data []byte
	pos  int
}

func (r *bitReader) read(count int) uint32 {
	out := uint32(0)
	for i := 0; i < count; i++ {
		out <<= 1
		if r.data[r.pos/8]&(0x80>>uint(r.pos%8)) != 0 {
			out |= 1
		}
		r.pos++
	}
	return out
}

// deinterleave undoes the §8.6 interleaving back into per-block
// data||ec slices.
func deinterleave(t *testing.T, stream []byte, spec versionSpec) [][]byte {
	t.Helper()
	blockCount := spec.group1Count + spec.group2Count
	sizes := make([]int, 0, blockCount)
	for i := 0; i < spec.group1Count; i++ {
		sizes = append(sizes, spec.group1Data)
	}
	for i := 0; i < spec.group2Count; i++ {
		sizes = append(sizes, spec.group2Data)
	}
	total := 0
	for _, s := range sizes {
		total += s + spec.ecPerBlock
	}
	if len(stream) < total {
		t.Fatalf("stream carries %d codewords, want at least %d", len(stream), total)
	}

	data := make([][]byte, blockCount)
	pos := 0
	maxData := spec.group1Data
	if spec.group2Data > maxData {
		maxData = spec.group2Data
	}
	for i := 0; i < maxData; i++ {
		for b := 0; b < blockCount; b++ {
			if i < sizes[b] {
				data[b] = append(data[b], stream[pos])
				pos++
			}
		}
	}
	for i := 0; i < spec.ecPerBlock; i++ {
		for b := 0; b < blockCount; b++ {
			data[b] = append(data[b], stream[pos])
			pos++
		}
	}
	return data
}

// syndromeCheck evaluates the codeword polynomial at α^0…α^(n-1): all
// zeros iff the block is a valid RS codeword.
func syndromeCheck(t *testing.T, block []byte, ecCount int) {
	t.Helper()
	for i := 0; i < ecCount; i++ {
		alpha := gfExp[i]
		sum := byte(0)
		for _, coefficient := range block {
			sum = gfMul(sum, alpha) ^ coefficient
		}
		if sum != 0 {
			t.Fatalf("syndrome %d non-zero: the RS block is not a codeword", i)
		}
	}
}

// --- tests ---

// TestFormatInfoKnownConstant pins the published format word for level M,
// mask 0: 101010000010010.
func TestFormatInfoKnownConstant(t *testing.T) {
	b := &symbolBuilder{size: 21, version: 1}
	b.modules = make([][]bool, 21)
	b.function = make([][]bool, 21)
	for i := range b.modules {
		b.modules[i] = make([]bool, 21)
		b.function[i] = make([]bool, 21)
	}
	b.drawFormatInfo(0)
	got := uint32(0)
	coords := [15][2]int{
		{8, 0},
		{8, 1},
		{8, 2},
		{8, 3},
		{8, 4},
		{8, 5},
		{8, 7},
		{8, 8},
		{7, 8},
		{5, 8},
		{4, 8},
		{3, 8},
		{2, 8},
		{1, 8},
		{0, 8},
	}
	for i, c := range coords {
		if b.modules[c[1]][c[0]] {
			got |= 1 << uint(i)
		}
	}
	if got != 0x5412 {
		t.Fatalf("format word for M/mask0 = %015b, want 101010000010010", got)
	}
}

// TestEncodeDecodeRoundtrip covers every supported version band, the two
// char-count widths and the multi-block interleaving.
func TestEncodeDecodeRoundtrip(t *testing.T) {
	payloads := [][]byte{
		[]byte("x"),
		[]byte("corsa:0123456789abcdef?v=1"),
		[]byte(strings.Repeat("corsa-link-", 12)),  // ~132 B → mid versions
		[]byte(strings.Repeat("Q", 250)),           // the §4.8 link budget
		[]byte(strings.Repeat("padding-", 44)),     // 352 B → v14, two block groups
		bytes.Repeat([]byte{0x00, 0xFF, 0x7F}, 60), // binary content
	}
	for _, payload := range payloads {
		matrix, err := Encode(payload)
		if err != nil {
			t.Fatalf("encode %d bytes: %v", len(payload), err)
		}
		if got := decode(t, matrix); !bytes.Equal(got, payload) {
			t.Fatalf("roundtrip lost the payload at %d bytes", len(payload))
		}
	}
}

// TestEncodeContactLinkSizeBand: the real link lands in the §4.8 band —
// version ~10–11 at level M (57–61 modules).
func TestEncodeContactLinkSizeBand(t *testing.T) {
	link := "corsa:aabbccddeeff00112233445566778899aabbccdd?v=1&net=gazeta-devnet" +
		"&pk=" + strings.Repeat("A", 43) + "&bk=" + strings.Repeat("B", 43) + "&bs=" + strings.Repeat("C", 86)
	matrix, err := Encode([]byte(link))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	version := (matrix.Size() - 17) / 4
	if version < 9 || version > 12 {
		t.Fatalf("link of %d bytes landed in version %d, expected the 9–12 band", len(link), version)
	}
	if got := decode(t, matrix); string(got) != link {
		t.Fatal("link roundtrip failed")
	}
}

// TestEncodeTooLarge: beyond version 14 the encoder refuses instead of
// silently truncating.
func TestEncodeTooLarge(t *testing.T) {
	if _, err := Encode(bytes.Repeat([]byte{'a'}, 600)); err == nil {
		t.Fatal("oversized payload accepted")
	}
}

// TestReedSolomonSyndromes: the RS encoder's output must be a codeword for
// arbitrary data, checked at every supported block size.
func TestReedSolomonSyndromes(t *testing.T) {
	for _, spec := range mediumSpecs {
		data := make([]byte, spec.group1Data)
		for i := range data {
			data[i] = byte(i*31 + 7)
		}
		block := append(append([]byte(nil), data...), rsEncode(data, spec.ecPerBlock)...)
		syndromeCheck(t, block, spec.ecPerBlock)
	}
}
