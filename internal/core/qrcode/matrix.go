package qrcode

// matrix.go places function patterns and data into the symbol, tries all
// eight masks and picks the lowest-penalty one (ISO 18004 §8.7–§8.9).

// symbolBuilder carries the two module planes: the values and the function
// map (true = function/reserved module, never masked, never data).
type symbolBuilder struct {
	modules  [][]bool
	function [][]bool
	size     int
	version  int
}

func buildSymbol(version int, codewords []byte) *Matrix {
	size := 17 + 4*version
	builder := &symbolBuilder{size: size, version: version}
	builder.modules = make([][]bool, size)
	builder.function = make([][]bool, size)
	for i := range builder.modules {
		builder.modules[i] = make([]bool, size)
		builder.function[i] = make([]bool, size)
	}

	builder.drawFinders()
	builder.drawAlignment()
	builder.drawTiming()
	builder.reserveFormat()
	builder.drawVersionInfo()
	builder.placeData(codewords)

	mask := builder.chooseMask()
	builder.applyMask(mask)
	builder.drawFormatInfo(mask)
	return &Matrix{size: size, modules: builder.modules}
}

func (b *symbolBuilder) setFunction(x, y int, dark bool) {
	b.modules[y][x] = dark
	b.function[y][x] = true
}

// drawFinders paints the three finder patterns with their separators.
func (b *symbolBuilder) drawFinders() {
	origins := [][2]int{{0, 0}, {b.size - 7, 0}, {0, b.size - 7}}
	for _, origin := range origins {
		ox, oy := origin[0], origin[1]
		for dy := -1; dy <= 7; dy++ {
			for dx := -1; dx <= 7; dx++ {
				x, y := ox+dx, oy+dy
				if x < 0 || y < 0 || x >= b.size || y >= b.size {
					continue
				}
				onRing := dx >= 0 && dx <= 6 && dy >= 0 && dy <= 6 &&
					(dx == 0 || dx == 6 || dy == 0 || dy == 6)
				inCore := dx >= 2 && dx <= 4 && dy >= 2 && dy <= 4
				b.setFunction(x, y, onRing || inCore)
			}
		}
	}
}

// drawAlignment paints every alignment pattern whose centre does not touch
// a finder.
func (b *symbolBuilder) drawAlignment() {
	centers := alignmentCenters[b.version-1]
	for _, cy := range centers {
		for _, cx := range centers {
			if b.function[cy][cx] {
				continue // overlaps a finder corner
			}
			for dy := -2; dy <= 2; dy++ {
				for dx := -2; dx <= 2; dx++ {
					dark := dx == -2 || dx == 2 || dy == -2 || dy == 2 || (dx == 0 && dy == 0)
					b.setFunction(cx+dx, cy+dy, dark)
				}
			}
		}
	}
}

// drawTiming paints the two alternating timing lines.
func (b *symbolBuilder) drawTiming() {
	for i := 8; i < b.size-8; i++ {
		if !b.function[6][i] {
			b.setFunction(i, 6, i%2 == 0)
		}
		if !b.function[i][6] {
			b.setFunction(6, i, i%2 == 0)
		}
	}
}

// reserveFormat blocks out the format-information modules (filled after
// masking) and the always-dark module.
func (b *symbolBuilder) reserveFormat() {
	for i := 0; i <= 8; i++ {
		if i != 6 {
			b.setFunction(8, i, false)
			b.setFunction(i, 8, false)
		}
	}
	b.setFunction(8, 8, false)
	for i := 0; i < 8; i++ {
		b.setFunction(b.size-1-i, 8, false)
		b.setFunction(8, b.size-1-i, false)
	}
	// The dark module, 18004 §8.9: always dark, at (8, 4·version + 9).
	b.setFunction(8, b.size-8, true)
}

// versionInfoPoly is the (18,6) Golay generator 0x1F25.
const versionInfoPoly = 0x1F25

// drawVersionInfo paints the two 3×6 version blocks for versions ≥ 7.
func (b *symbolBuilder) drawVersionInfo() {
	if b.version < 7 {
		return
	}
	bits := uint32(b.version) << 12
	remainder := bits
	for i := 17; i >= 12; i-- {
		if remainder>>uint(i)&1 == 1 {
			remainder ^= versionInfoPoly << uint(i-12)
		}
	}
	bits |= remainder
	for i := 0; i < 18; i++ {
		dark := bits>>uint(i)&1 == 1
		x := i / 3
		y := b.size - 11 + i%3
		b.setFunction(x, y, dark)
		b.setFunction(y, x, dark)
	}
}

// placeData walks the zigzag of §8.7.3 and writes the final codeword
// sequence into every non-function module, most significant bit first.
func (b *symbolBuilder) placeData(codewords []byte) {
	bitIndex := 0
	totalBits := len(codewords) * 8
	upward := true
	for right := b.size - 1; right >= 1; right -= 2 {
		if right == 6 {
			right = 5 // the vertical timing column is skipped whole
		}
		for step := 0; step < b.size; step++ {
			y := step
			if upward {
				y = b.size - 1 - step
			}
			for _, x := range [2]int{right, right - 1} {
				if b.function[y][x] {
					continue
				}
				dark := false
				if bitIndex < totalBits {
					dark = codewords[bitIndex/8]>>uint(7-bitIndex%8)&1 == 1
				}
				// Remainder bits past the last codeword stay light.
				b.modules[y][x] = dark
				bitIndex++
			}
		}
		upward = !upward
	}
}

// maskPredicates are the eight §8.8.1 conditions; true = invert.
var maskPredicates = [8]func(x, y int) bool{
	func(x, y int) bool { return (x+y)%2 == 0 },
	func(x, y int) bool { return y%2 == 0 },
	func(x, y int) bool { return x%3 == 0 },
	func(x, y int) bool { return (x+y)%3 == 0 },
	func(x, y int) bool { return (y/2+x/3)%2 == 0 },
	func(x, y int) bool { return (x*y)%2+(x*y)%3 == 0 },
	func(x, y int) bool { return ((x*y)%2+(x*y)%3)%2 == 0 },
	func(x, y int) bool { return ((x+y)%2+(x*y)%3)%2 == 0 },
}

func (b *symbolBuilder) applyMask(mask int) {
	predicate := maskPredicates[mask]
	for y := 0; y < b.size; y++ {
		for x := 0; x < b.size; x++ {
			if !b.function[y][x] && predicate(x, y) {
				b.modules[y][x] = !b.modules[y][x]
			}
		}
	}
}

// chooseMask scores all eight masks with the format info in place and
// returns the lowest-penalty one. Each candidate is applied and reverted
// on the same plane (XOR is its own inverse).
func (b *symbolBuilder) chooseMask() int {
	best, bestPenalty := 0, -1
	for mask := 0; mask < 8; mask++ {
		b.applyMask(mask)
		b.drawFormatInfo(mask)
		penalty := b.penalty()
		b.applyMask(mask)
		if bestPenalty < 0 || penalty < bestPenalty {
			best, bestPenalty = mask, penalty
		}
	}
	return best
}

// formatInfoPoly is the BCH(15,5) generator 0x537; formatInfoMask the XOR
// pattern 0x5412. Level M is the two bits 00.
const (
	formatInfoPoly = 0x537
	formatInfoMask = 0x5412
)

// drawFormatInfo paints both copies of the format information for level M
// and the given mask.
func (b *symbolBuilder) drawFormatInfo(mask int) {
	data := uint32(0b00<<3 | mask) // M = 00
	bits := data << 10
	remainder := bits
	for i := 14; i >= 10; i-- {
		if remainder>>uint(i)&1 == 1 {
			remainder ^= formatInfoPoly << uint(i-10)
		}
	}
	bits = (data<<10 | remainder) ^ formatInfoMask

	// First copy around the top-left finder (§8.9 figure 25 ordering).
	for i := 0; i <= 5; i++ {
		b.setFunction(8, i, bits>>uint(i)&1 == 1)
	}
	b.setFunction(8, 7, bits>>6&1 == 1)
	b.setFunction(8, 8, bits>>7&1 == 1)
	b.setFunction(7, 8, bits>>8&1 == 1)
	for i := 9; i <= 14; i++ {
		b.setFunction(14-i, 8, bits>>uint(i)&1 == 1)
	}
	// Second copy split across the other two finders.
	for i := 0; i <= 7; i++ {
		b.setFunction(b.size-1-i, 8, bits>>uint(i)&1 == 1)
	}
	for i := 8; i <= 14; i++ {
		b.setFunction(8, b.size-15+i, bits>>uint(i)&1 == 1)
	}
	// Restore the always-dark module the loop above may sit next to.
	b.setFunction(8, b.size-8, true)
}

// penalty is the §8.8.2 score: runs, blocks, finder-like patterns and dark
// balance.
func (b *symbolBuilder) penalty() int {
	total := 0

	// N1: runs of five or more same-colored modules, both directions.
	for y := 0; y < b.size; y++ {
		total += runPenalty(func(i int) bool { return b.modules[y][i] }, b.size)
		total += runPenalty(func(i int) bool { return b.modules[i][y] }, b.size)
	}

	// N2: 2×2 blocks of one color.
	for y := 0; y < b.size-1; y++ {
		for x := 0; x < b.size-1; x++ {
			c := b.modules[y][x]
			if b.modules[y][x+1] == c && b.modules[y+1][x] == c && b.modules[y+1][x+1] == c {
				total += 3
			}
		}
	}

	// N3: the 1:1:3:1:1 finder-like run with four light modules on either
	// side, both directions.
	for y := 0; y < b.size; y++ {
		total += finderLikePenalty(func(i int) bool { return b.modules[y][i] }, b.size)
		total += finderLikePenalty(func(i int) bool { return b.modules[i][y] }, b.size)
	}

	// N4: dark-module balance, 10 points per 5% deviation step.
	dark := 0
	for y := 0; y < b.size; y++ {
		for x := 0; x < b.size; x++ {
			if b.modules[y][x] {
				dark++
			}
		}
	}
	percent := dark * 100 / (b.size * b.size)
	deviation := percent - 50
	if deviation < 0 {
		deviation = -deviation
	}
	total += deviation / 5 * 10
	return total
}

func runPenalty(at func(int) bool, size int) int {
	total := 0
	run := 1
	for i := 1; i < size; i++ {
		if at(i) == at(i-1) {
			run++
			continue
		}
		if run >= 5 {
			total += 3 + run - 5
		}
		run = 1
	}
	if run >= 5 {
		total += 3 + run - 5
	}
	return total
}

var (
	finderLikeA = []bool{true, false, true, true, true, false, true, false, false, false, false}
	finderLikeB = []bool{false, false, false, false, true, false, true, true, true, false, true}
)

func finderLikePenalty(at func(int) bool, size int) int {
	total := 0
	for i := 0; i+len(finderLikeA) <= size; i++ {
		matchA, matchB := true, true
		for j := 0; j < len(finderLikeA); j++ {
			if at(i+j) != finderLikeA[j] {
				matchA = false
			}
			if at(i+j) != finderLikeB[j] {
				matchB = false
			}
			if !matchA && !matchB {
				break
			}
		}
		if matchA || matchB {
			total += 40
		}
	}
	return total
}
