package qrcode

// galois.go is GF(256) arithmetic over the QR polynomial x⁸+x⁴+x³+x²+1
// (0x11D) and the Reed-Solomon encoder built on it.

var (
	gfExp [512]byte
	gfLog [256]byte
)

func init() {
	x := 1
	for i := 0; i < 255; i++ {
		gfExp[i] = byte(x)
		gfLog[x] = byte(i)
		x <<= 1
		if x >= 256 {
			x ^= 0x11D
		}
	}
	// Doubled so products of two logs index without a modulo.
	for i := 255; i < 512; i++ {
		gfExp[i] = gfExp[i-255]
	}
}

func gfMul(a, b byte) byte {
	if a == 0 || b == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])+int(gfLog[b])]
}

// rsGenerator builds the degree-n generator polynomial ∏(x - α^i),
// i = 0…n-1, highest coefficient first.
func rsGenerator(n int) []byte {
	gen := []byte{1}
	for i := 0; i < n; i++ {
		next := make([]byte, len(gen)+1)
		for j, coefficient := range gen {
			// (gen · x): the coefficient keeps its index in the longer
			// slice; (gen · α^i) lands one position lower.
			next[j] ^= coefficient
			next[j+1] ^= gfMul(coefficient, gfExp[i])
		}
		gen = next
	}
	return gen
}

// rsEncode returns the n error-correction codewords of one data block —
// the remainder of data·xⁿ divided by the generator.
func rsEncode(data []byte, n int) []byte {
	gen := rsGenerator(n)
	remainder := make([]byte, n)
	for _, b := range data {
		factor := b ^ remainder[0]
		copy(remainder, remainder[1:])
		remainder[n-1] = 0
		if factor == 0 {
			continue
		}
		for i := 0; i < n; i++ {
			remainder[i] ^= gfMul(gen[i+1], factor)
		}
	}
	return remainder
}
