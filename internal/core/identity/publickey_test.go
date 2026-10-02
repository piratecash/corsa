package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// smallOrderEncodingsHex is the complete list of 32-byte strings a permissive
// Ed25519 decoder turns into a point of order 1, 2, 4 or 8. It is written out
// by hand on purpose: TestSmallOrderVectorsAreTheWholeTorsionSubgroup derives
// the same set from the curve equation, so a typo here, or a point the list
// forgot, fails a test instead of silently narrowing the rejection.
var smallOrderEncodingsHex = []string{
	"0100000000000000000000000000000000000000000000000000000000000000", // neutral, order 1
	"0100000000000000000000000000000000000000000000000000000000000080", // neutral, x = -0
	"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f", // order 2
	"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // order 2, x = -0
	"0000000000000000000000000000000000000000000000000000000000000000", // order 4
	"0000000000000000000000000000000000000000000000000000000000000080", // order 4
	"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05", // order 8
	"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc85", // order 8
	"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a", // order 8
	"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac03fa", // order 8
	"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f", // y = p, order 4
	"edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // y = p, order 4
	"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f", // y = p+1, order 1
	"eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // y = p+1, order 1
}

// ---------------------------------------------------------------------------
// Reference arithmetic
//
// The vectors are checked against the curve itself, not against the code under
// test: an independent, slow, obviously-correct affine implementation over
// math/big. It mirrors the PERMISSIVE decoding stdlib applies (y reduced mod p,
// x = -0 accepted), because that is the decoder an attacker's key meets.
// ---------------------------------------------------------------------------

type refPoint struct{ x, y *big.Int }

type refCurve struct {
	p, d, sqrtM1, l *big.Int
}

func newRefCurve() refCurve {
	p := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(19))
	d := new(big.Int).Mul(big.NewInt(-121665), new(big.Int).ModInverse(big.NewInt(121666), p))
	d.Mod(d, p)
	exp := new(big.Int).Rsh(new(big.Int).Sub(p, big.NewInt(1)), 2)
	sqrtM1 := new(big.Int).Exp(big.NewInt(2), exp, p)
	l, _ := new(big.Int).SetString("7237005577332262213973186563042994240857116359379907606001950938285454250989", 10)
	return refCurve{p: p, d: d, sqrtM1: sqrtM1, l: l}
}

func (c refCurve) identity() refPoint { return refPoint{x: big.NewInt(0), y: big.NewInt(1)} }

func (c refCurve) equal(a, b refPoint) bool { return a.x.Cmp(b.x) == 0 && a.y.Cmp(b.y) == 0 }

func (c refCurve) mod(v *big.Int) *big.Int { return v.Mod(v, c.p) }

func (c refCurve) add(a, b refPoint) refPoint {
	x1y2 := c.mod(new(big.Int).Mul(a.x, b.y))
	y1x2 := c.mod(new(big.Int).Mul(a.y, b.x))
	y1y2 := c.mod(new(big.Int).Mul(a.y, b.y))
	x1x2 := c.mod(new(big.Int).Mul(a.x, b.x))
	t := c.mod(new(big.Int).Mul(c.d, c.mod(new(big.Int).Mul(x1x2, y1y2))))
	xDen := new(big.Int).ModInverse(c.mod(new(big.Int).Add(big.NewInt(1), t)), c.p)
	yDen := new(big.Int).ModInverse(c.mod(new(big.Int).Sub(big.NewInt(1), t)), c.p)
	x := c.mod(new(big.Int).Mul(c.mod(new(big.Int).Add(x1y2, y1x2)), xDen))
	y := c.mod(new(big.Int).Mul(c.mod(new(big.Int).Add(y1y2, x1x2)), yDen))
	return refPoint{x: x, y: y}
}

func (c refCurve) mul(k *big.Int, a refPoint) refPoint {
	result := c.identity()
	for i := k.BitLen() - 1; i >= 0; i-- {
		result = c.add(result, result)
		if k.Bit(i) == 1 {
			result = c.add(result, a)
		}
	}
	return result
}

// decode is the permissive decoder: y is reduced mod p and the sign bit picks
// x, with x = 0 accepted for either sign. ok=false only when no x exists.
func (c refCurve) decode(raw []byte) (refPoint, bool) {
	little := make([]byte, 32)
	for i := range little {
		little[i] = raw[31-i]
	}
	sign := little[0] >> 7
	little[0] &= 0x7f
	y := c.mod(new(big.Int).SetBytes(little))
	y2 := c.mod(new(big.Int).Mul(y, y))
	u := c.mod(new(big.Int).Sub(y2, big.NewInt(1)))
	v := c.mod(new(big.Int).Add(c.mod(new(big.Int).Mul(c.d, y2)), big.NewInt(1)))
	xx := c.mod(new(big.Int).Mul(u, new(big.Int).ModInverse(v, c.p)))
	if xx.Sign() == 0 {
		return refPoint{x: big.NewInt(0), y: y}, true
	}
	x := new(big.Int).ModSqrt(xx, c.p)
	if x == nil {
		return refPoint{}, false
	}
	if uint8(x.Bit(0)) != sign {
		x.Sub(c.p, x)
	}
	return refPoint{x: x, y: y}, true
}

// encodings lists every 32-byte string the permissive decoder maps to a — the
// canonical one, the y+p alias when it still fits in 255 bits, and the x = -0
// twin of a point whose x is zero.
func (c refCurve) encodings(a refPoint) [][]byte {
	limit := new(big.Int).Lsh(big.NewInt(1), 255)
	var out [][]byte
	for _, y := range []*big.Int{new(big.Int).Set(a.y), new(big.Int).Add(a.y, c.p)} {
		if y.Cmp(limit) >= 0 {
			continue
		}
		signs := []uint{a.x.Bit(0)}
		if a.x.Sign() == 0 {
			signs = []uint{0, 1}
		}
		for _, sign := range signs {
			value := new(big.Int).Set(y)
			value.SetBit(value, 255, sign)
			out = append(out, littleEndian32(value))
		}
	}
	return out
}

func littleEndian32(v *big.Int) []byte {
	big32 := v.FillBytes(make([]byte, 32))
	out := make([]byte, 32)
	for i := range out {
		out[i] = big32[31-i]
	}
	return out
}

func (c refCurve) encodeCanonical(a refPoint) []byte {
	value := new(big.Int).Set(a.y)
	value.SetBit(value, 255, a.x.Bit(0))
	return littleEndian32(value)
}

func mustHex32(t *testing.T, s string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 32 {
		t.Fatalf("bad 32-byte hex %q: %v", s, err)
	}
	return raw
}

// yEncoding is the 32-byte encoding of the integer y with the given sign bit,
// for y that does not fit the field (the non-canonical range) as well.
func yEncoding(y *big.Int, sign uint) []byte {
	value := new(big.Int).Set(y)
	value.SetBit(value, 255, sign)
	return littleEndian32(value)
}

// ---------------------------------------------------------------------------
// The vectors are what they claim to be
// ---------------------------------------------------------------------------

// TestSmallOrderVectorsHaveSmallOrder checks every hand-written vector against
// the curve: it decodes, and 8·P is the neutral element.
func TestSmallOrderVectorsHaveSmallOrder(t *testing.T) {
	t.Parallel()
	c := newRefCurve()
	for _, h := range smallOrderEncodingsHex {
		point, ok := c.decode(mustHex32(t, h))
		if !ok {
			t.Fatalf("%s does not decode, so it is not a small-order encoding", h)
		}
		if !c.equal(c.mul(big.NewInt(8), point), c.identity()) {
			t.Fatalf("8·P != O for %s", h)
		}
	}
}

// TestSmallOrderVectorsAreTheWholeTorsionSubgroup derives the list from the
// curve: the 8-torsion of Ed25519 is cyclic, so the multiples of one point of
// order exactly 8 are ALL the small-order points, and their encodings under
// the permissive decoder are exactly the 14 strings. A vector missing from the
// hand-written list would be a key ParsePublicKey lets through.
func TestSmallOrderVectorsAreTheWholeTorsionSubgroup(t *testing.T) {
	t.Parallel()
	c := newRefCurve()
	generator, ok := c.decode(mustHex32(t, smallOrderEncodingsHex[6]))
	if !ok {
		t.Fatal("the order-8 vector does not decode")
	}
	if c.equal(c.mul(big.NewInt(4), generator), c.identity()) {
		t.Fatal("the generator has order below 8; its multiples would not cover the subgroup")
	}

	derived := map[string]bool{}
	for k := int64(0); k < 8; k++ {
		for _, enc := range c.encodings(c.mul(big.NewInt(k), generator)) {
			derived[hex.EncodeToString(enc)] = true
		}
	}
	listed := map[string]bool{}
	for _, h := range smallOrderEncodingsHex {
		listed[h] = true
	}
	if len(derived) != 14 || len(listed) != 14 {
		t.Fatalf("derived %d encodings, listed %d; want 14 each", len(derived), len(listed))
	}
	for h := range derived {
		if !listed[h] {
			t.Errorf("small-order encoding %s is missing from the list", h)
		}
	}
}

// ---------------------------------------------------------------------------
// ParsePublicKey
// ---------------------------------------------------------------------------

func TestParsePublicKeyRejectsEverySmallOrderEncoding(t *testing.T) {
	t.Parallel()
	for _, h := range smallOrderEncodingsHex {
		_, err := ParsePublicKey(mustHex32(t, h))
		if !errors.Is(err, ErrPublicKeySmallOrder) {
			t.Errorf("ParsePublicKey(%s) = %v, want ErrPublicKeySmallOrder", h, err)
		}
		if !errors.Is(err, ErrInvalidPublicKey) {
			t.Errorf("ParsePublicKey(%s) = %v, want it to match ErrInvalidPublicKey", h, err)
		}
	}
}

// TestParsePublicKeyRejectsNonCanonicalY walks the whole non-canonical range,
// y = p .. 2^255-1 with both sign bits: 38 encodings, four of which are small
// order and reported as such, the rest as non-canonical.
func TestParsePublicKeyRejectsNonCanonicalY(t *testing.T) {
	t.Parallel()
	c := newRefCurve()
	smallOrder := map[string]bool{}
	for _, h := range smallOrderEncodingsHex {
		smallOrder[h] = true
	}
	limit := new(big.Int).Lsh(big.NewInt(1), 255)
	count := 0
	for y := new(big.Int).Set(c.p); y.Cmp(limit) < 0; y.Add(y, big.NewInt(1)) {
		for _, sign := range []uint{0, 1} {
			raw := yEncoding(y, sign)
			count++
			_, err := ParsePublicKey(raw)
			want := ErrPublicKeyNonCanonical
			if smallOrder[hex.EncodeToString(raw)] {
				want = ErrPublicKeySmallOrder
			}
			if !errors.Is(err, want) {
				t.Errorf("ParsePublicKey(%x) = %v, want %v", raw, err, want)
			}
		}
	}
	if count != 38 {
		t.Fatalf("walked %d non-canonical encodings, want 38", count)
	}
}

// TestParsePublicKeyBoundary pins the edge of the canonical range. y = p-1 is
// the order-2 point (rejected), y = p-2 has no x on the curve, and y = p-3 is
// the largest canonical y of a point that is not of small order — the largest
// encoding an honest decoder must keep accepting. y = p is the first
// non-canonical value.
func TestParsePublicKeyBoundary(t *testing.T) {
	t.Parallel()
	c := newRefCurve()
	pMinus := func(k int64) *big.Int { return new(big.Int).Sub(c.p, big.NewInt(k)) }

	boundary := yEncoding(pMinus(3), 0)
	if got := hex.EncodeToString(boundary); got != "eaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f" {
		t.Fatalf("p-3 encodes as %s", got)
	}
	for _, sign := range []uint{0, 1} {
		raw := yEncoding(pMinus(3), sign)
		point, ok := c.decode(raw)
		if !ok {
			t.Fatalf("y = p-3 (sign %d) is not on the curve", sign)
		}
		if c.equal(c.mul(big.NewInt(8), point), c.identity()) {
			t.Fatalf("y = p-3 (sign %d) is of small order", sign)
		}
		if _, err := ParsePublicKey(raw); err != nil {
			t.Fatalf("ParsePublicKey(y = p-3, sign %d) = %v, want accepted", sign, err)
		}
	}

	for _, sign := range []uint{0, 1} {
		if _, ok := c.decode(yEncoding(pMinus(2), sign)); ok {
			t.Fatalf("y = p-2 (sign %d) is on the curve, so p-3 is not the boundary", sign)
		}
	}
	if _, err := ParsePublicKey(yEncoding(pMinus(1), 0)); !errors.Is(err, ErrPublicKeySmallOrder) {
		t.Fatalf("ParsePublicKey(y = p-1) = %v, want ErrPublicKeySmallOrder", err)
	}
	if _, err := ParsePublicKey(yEncoding(c.p, 0)); !errors.Is(err, ErrInvalidPublicKey) {
		t.Fatalf("ParsePublicKey(y = p) = %v, want rejected", err)
	}

	// y = p − 256 differs from p only in the SECOND byte (ed fe ff … 7f): a
	// canonicality check that looks at the low byte alone would call it
	// non-canonical. It is canonical and must be accepted.
	secondByte := yEncoding(pMinus(256), 0)
	if got := hex.EncodeToString(secondByte); got != "edfeffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f" {
		t.Fatalf("p-256 encodes as %s", got)
	}
	if _, err := ParsePublicKey(secondByte); err != nil {
		t.Fatalf("ParsePublicKey(y = p-256) = %v, want accepted", err)
	}
}

// TestParsePublicKeyLeavesCurveMembershipToVerify pins the documented scope:
// an encoding with no point behind it parses, and no signature ever verifies
// under it, because the verifier's own decompression refuses it.
func TestParsePublicKeyLeavesCurveMembershipToVerify(t *testing.T) {
	t.Parallel()
	c := newRefCurve()
	offCurve := yEncoding(new(big.Int).Sub(c.p, big.NewInt(2)), 0)
	key, err := ParsePublicKey(offCurve)
	if err != nil {
		t.Fatalf("ParsePublicKey(off-curve) = %v; curve membership is the verifier's check", err)
	}
	signature := append(append([]byte(nil), offCurve...), make([]byte, 32)...)
	if key.Verify([]byte("anything"), signature) {
		t.Fatal("a key with no point behind it verified a signature")
	}
}

func TestParsePublicKeyRejectsWrongSize(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 1, 31, 33, 64} {
		if _, err := ParsePublicKey(make([]byte, size)); !errors.Is(err, ErrPublicKeySize) {
			t.Errorf("ParsePublicKey(%d bytes) = %v, want ErrPublicKeySize", size, err)
		}
	}
}

func TestParsePublicKeyBase64RejectsUndecodable(t *testing.T) {
	t.Parallel()
	_, err := ParsePublicKeyBase64("not base64 at all!")
	if !errors.Is(err, ErrPublicKeyEncoding) || !errors.Is(err, ErrInvalidPublicKey) {
		t.Fatalf("ParsePublicKeyBase64(garbage) = %v, want ErrPublicKeyEncoding", err)
	}
}

// TestParsePublicKeyAcceptsMixedOrderKeys pins a deliberate non-rejection: a
// key with a torsion component (A + T, T of order 8) is accepted. Telling it
// apart needs [L]·A = O, a scalar multiplication stdlib does not expose, and
// the key harms only its own identity — the address is bound to the exact
// bytes — while every node verifies with the same cofactorless Go verifier,
// so no two of our nodes disagree about a signature under it.
func TestParsePublicKeyAcceptsMixedOrderKeys(t *testing.T) {
	t.Parallel()
	c := newRefCurve()
	torsion, ok := c.decode(mustHex32(t, smallOrderEncodingsHex[6]))
	if !ok {
		t.Fatal("the order-8 vector does not decode")
	}
	for i := 0; i < 8; i++ {
		honest, _, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
		point, ok := c.decode(honest)
		if !ok {
			t.Fatal("an honest key does not decode")
		}
		mixed := c.add(point, torsion)
		if c.equal(c.mul(c.l, mixed), c.identity()) {
			t.Fatal("A + T landed in the prime-order subgroup; the vector is not mixed-order")
		}
		if c.equal(c.mul(big.NewInt(8), mixed), c.identity()) {
			t.Fatal("A + T is of small order; the vector is not mixed-order")
		}
		raw := c.encodeCanonical(mixed)
		if _, err := ParsePublicKey(raw); err != nil {
			t.Fatalf("ParsePublicKey(mixed-order %x) = %v, want accepted", raw, err)
		}
	}
}

// TestParsePublicKeyAcceptsHonestKeys is the compatibility half: every key an
// honest node can hold — freshly generated, the fixed-seed fixtures, and the
// published datagram vector — parses, and signatures under it verify exactly
// as they did with stdlib.
func TestParsePublicKeyAcceptsHonestKeys(t *testing.T) {
	t.Parallel()
	var seeds [][]byte
	for _, start := range []byte{0, 7, 1} {
		seed := make([]byte, ed25519.SeedSize)
		for i := range seed {
			seed[i] = byte(i) + start
		}
		seeds = append(seeds, seed)
	}
	for i := 0; i < 256; i++ {
		seed := sha256.Sum256([]byte(fmt.Sprintf("honest-key-%d", i)))
		seeds = append(seeds, seed[:])
	}
	message := []byte("corsa-session-auth-v1|challenge|address")
	for _, seed := range seeds {
		private := ed25519.NewKeyFromSeed(seed)
		public := private.Public().(ed25519.PublicKey)
		key, err := ParsePublicKey(public)
		if err != nil {
			t.Fatalf("ParsePublicKey(honest %x) = %v", public, err)
		}
		if !key.Verify(message, ed25519.Sign(private, message)) {
			t.Fatalf("a genuine signature under %x did not verify", public)
		}
		if !bytes.Equal(key.Bytes(), public) {
			t.Fatalf("Bytes() = %x, want %x", key.Bytes(), public)
		}
	}

	// docs/protocol/datagram.md §3.3: seed 00..1f, pubkey 03a107…31b8.
	documented := ed25519.NewKeyFromSeed(seeds[0]).Public().(ed25519.PublicKey)
	if got := hex.EncodeToString(documented); got != "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8" {
		t.Fatalf("seed 00..1f derives %s, not the documented vector", got)
	}
}

func TestPublicKeyBytesIsACopy(t *testing.T) {
	t.Parallel()
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	input := append([]byte(nil), public...)
	key, err := ParsePublicKey(input)
	if err != nil {
		t.Fatalf("ParsePublicKey: %v", err)
	}
	input[0] ^= 0xff
	out := key.Bytes()
	out[1] ^= 0xff
	if !bytes.Equal(key.Bytes(), public) {
		t.Fatal("PublicKey shares memory with its input or its output")
	}
}

func TestZeroPublicKeyVerifiesNothing(t *testing.T) {
	t.Parallel()
	var key PublicKey
	if key.Verify([]byte("m"), make([]byte, ed25519.SignatureSize)) {
		t.Fatal("the zero PublicKey verified a signature")
	}
}

// ---------------------------------------------------------------------------
// The universal signature
// ---------------------------------------------------------------------------

// TestStdlibAcceptsTheUniversalSignature is the hazard this file exists for,
// measured on the verifier every node runs: if stdlib ever starts refusing it,
// this test says so and the rest of the file is defence in depth.
func TestStdlibAcceptsTheUniversalSignature(t *testing.T) {
	t.Parallel()
	neutral := edforgery.NeutralPublicKey()
	if !bytes.Equal(neutral, mustHex32(t, smallOrderEncodingsHex[0])) {
		t.Fatal("edforgery.NeutralPublicKey is not the neutral encoding")
	}
	order4 := mustHex32(t, smallOrderEncodingsHex[4])
	order4Accepted := 0
	for i := 0; i < 400; i++ {
		message := []byte(fmt.Sprintf("message-%d", i))
		if !ed25519.Verify(neutral, message, edforgery.UniversalSignature()) {
			t.Fatalf("stdlib refused the universal signature for %q under the neutral key", message)
		}
		if ed25519.Verify(order4, message, edforgery.UniversalSignature()) {
			order4Accepted++
		}
	}
	if order4Accepted == 0 {
		t.Fatal("stdlib accepted no message under the order-4 key; the hazard measurement is stale")
	}
}

func TestPublicKeyVerifyIsUnreachableForSmallOrderKeys(t *testing.T) {
	t.Parallel()
	for _, h := range smallOrderEncodingsHex {
		if _, err := ParsePublicKey(mustHex32(t, h)); err == nil {
			t.Fatalf("%s parsed, so its universal signature would verify", h)
		}
	}
}

// TestIdentityVerifiersRefuseTheUniversalSignature drives every verifier the
// identity package exports with a self-consistent forgery: the address IS the
// fingerprint of the neutral key, so the only thing that can refuse it is the
// key check.
func TestIdentityVerifiersRefuseTheUniversalSignature(t *testing.T) {
	t.Parallel()
	for _, h := range smallOrderEncodingsHex {
		raw := mustHex32(t, h)
		address := Fingerprint(raw)
		keyB64 := base64.StdEncoding.EncodeToString(raw)
		sigB64 := base64.RawURLEncoding.EncodeToString(edforgery.UniversalSignature())
		boxKey := base64.StdEncoding.EncodeToString(make([]byte, 32))

		if err := VerifyPayload(address, keyB64, []byte("payload"), sigB64); !errors.Is(err, ErrInvalidPublicKey) {
			t.Errorf("VerifyPayload(%s) = %v, want ErrInvalidPublicKey", h, err)
		}
		if err := VerifyBoxKeyBinding(address, keyB64, boxKey, sigB64); !errors.Is(err, ErrInvalidPublicKey) {
			t.Errorf("VerifyBoxKeyBinding(%s) = %v, want ErrInvalidPublicKey", h, err)
		}
		if err := VerifyPublicKeyFingerprint(address, keyB64); !errors.Is(err, ErrInvalidPublicKey) {
			t.Errorf("VerifyPublicKeyFingerprint(%s) = %v, want ErrInvalidPublicKey", h, err)
		}
	}
}

// TestIdentityVerifiersAcceptHonestKeys is the compatibility control for the
// test above: the same three verifiers keep accepting a genuine identity.
func TestIdentityVerifiersAcceptHonestKeys(t *testing.T) {
	t.Parallel()
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	keyB64 := PublicKeyBase64(id.PublicKey)
	if err := VerifyPayload(id.Address, keyB64, []byte("payload"), SignPayload(id, []byte("payload"))); err != nil {
		t.Fatalf("VerifyPayload: %v", err)
	}
	if err := VerifyBoxKeyBinding(id.Address, keyB64, BoxPublicKeyBase64(id.BoxPublicKey), SignBoxKeyBinding(id)); err != nil {
		t.Fatalf("VerifyBoxKeyBinding: %v", err)
	}
	if err := VerifyPublicKeyFingerprint(id.Address, keyB64); err != nil {
		t.Fatalf("VerifyPublicKeyFingerprint: %v", err)
	}
}
