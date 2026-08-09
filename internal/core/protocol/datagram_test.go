package protocol

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Shared wire fragments. They are the canonical spellings of the golden
// vector (testdata/datagram_vector_v2.json), so a reject test differs from
// an accept test by exactly the fragment under test.
const (
	// testDatagramNetwork is the network id of the golden vector
	// (testdata/datagram_vector_v2.json) and today's node-level network name.
	testDatagramNetwork = domain.NetworkID("gazeta-devnet")

	testDatagramSrcHex = "56475aa75463474c0285df5dbf2bcab73da65135"
	testDatagramDstHex = "00f39d89f345eb1613bb2fa02ee883a214a6a697"
	testDatagramPubKey = "A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg"
	testDatagramSalt   = "oKGio6SlpqeoqaqrrK2urw"
	// testDatagramSig is a well-formed signature VALUE — 64 canonical
	// base64url bytes — and nothing more. The fragments it appears in are
	// only ever PARSED, never verified, so what these tests need from it is
	// its shape. The bytes a real signature has are pinned in exactly one
	// place, the golden vector, so no second copy can drift from it.
	testDatagramSig = "iBwfrY2oV0ak9ldu8lQaExBcGWT8F8ikGEaDNzw6xiNzqzbs7IYioCMoLsXMO3lVIim4-hlJCtX5qrTL6ChoCw"

	fragType         = `"type":"datagram"`
	fragVersion      = `"v":2`
	fragModeRouted   = `"mode":"routed"`
	fragModeRequest  = `"mode":"request"`
	fragModeResponse = `"mode":"response"`
	fragClassControl = `"class":"control"`
	fragClassBulk    = `"class":"bulk"`
	fragSrc          = `"src":"` + testDatagramSrcHex + `"`
	fragDst          = `"dst":"` + testDatagramDstHex + `"`
	fragTTL          = `"ttl":10`
	fragPolicyBest   = `"route_policy":"best"`
	fragDType        = `"dtype":"delivery_receipt"`
	fragPayload      = `"payload":"EBESExQVFhcYGRobHB0eHw"`
	fragAuth         = `"auth":{"av":1,"pubkey":"` + testDatagramPubKey + `","salt":"` + testDatagramSalt +
		`","max_ttl":10,"time":1780000000,"sig":"` + testDatagramSig + `"}`
)

// buildDatagramJSON assembles a frame from ordered JSON fragments so each
// test states exactly the wire shape it exercises.
func buildDatagramJSON(fragments ...string) string {
	return "{" + strings.Join(fragments, ",") + "}"
}

func routedFrameJSON(extra ...string) string {
	base := []string{
		fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc, fragDst,
		fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth,
	}
	return buildDatagramJSON(append(base, extra...)...)
}

func testDatagramKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	return ed25519.NewKeyFromSeed(seed)
}

func mustIdentity(t *testing.T, hexID string) domain.PeerIdentity {
	t.Helper()
	id, err := domain.ParsePeerIdentity(hexID)
	if err != nil {
		t.Fatalf("ParsePeerIdentity(%q): %v", hexID, err)
	}
	return id
}

// newSignedDatagram builds the reference routed datagram of the golden
// vector, signed with the deterministic test key.
func newSignedDatagram(t *testing.T) DatagramFrame {
	t.Helper()
	key := testDatagramKey(t)
	payload := make([]byte, 16)
	for i := range payload {
		payload[i] = byte(0x10 + i)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	for i := range salt {
		salt[i] = byte(0xa0 + i)
	}
	d := DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         mustIdentity(t, identity.Fingerprint(key.Public().(ed25519.PublicKey))),
		Dst:         mustIdentity(t, testDatagramDstHex),
		TTL:         10,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DType("delivery_receipt"),
		Payload:     payload,
		Auth: &DatagramAuth{
			AuthVersion: domain.AuthVersionBase,
			Salt:        salt,
			MaxTTL:      10,
			Time:        1780000000,
		},
	}
	signed, err := SignDatagram(d, testDatagramNetwork, key)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	return signed
}

func TestMarshalDatagramIsCanonical(t *testing.T) {
	d := newSignedDatagram(t)
	got, err := MarshalDatagramFrame(d)
	if err != nil {
		t.Fatalf("MarshalDatagram: %v", err)
	}
	// The signature is spliced in from the frame rather than written out: the
	// bytes of a real signature belong to the golden vector, and a second copy
	// here would be one more place to update on every transcript change. What
	// this test owns is the SHAPE — key order, spelling, and the absence of any
	// field the v2 envelope no longer carries.
	want := `{"type":"datagram","v":2,"mode":"routed","class":"control","src":"` + testDatagramSrcHex +
		`","dst":"` + testDatagramDstHex + `","ttl":10,"route_policy":"best","dtype":"delivery_receipt",` +
		`"payload":"EBESExQVFhcYGRobHB0eHw","auth":{"av":1,"pubkey":"` + testDatagramPubKey +
		`","salt":"` + testDatagramSalt + `","max_ttl":10,"time":1780000000,"sig":"` +
		base64.RawURLEncoding.EncodeToString(d.Auth.Sig) + `"}}`
	if string(got) != want {
		t.Fatalf("canonical form mismatch:\n got %s\nwant %s", got, want)
	}
}

func TestMarshalDatagramOmitsAbsentOptionalFields(t *testing.T) {
	d := DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeResponse,
		Class:       domain.DatagramClassControl,
		Src:         mustIdentity(t, testDatagramSrcHex),
		Dst:         mustIdentity(t, testDatagramDstHex),
		TTL:         10,
		RoutePolicy: domain.RoutePolicyNone,
		DType:       domain.DType("post_identity"),
		Payload:     nil,
	}
	got, err := MarshalDatagramFrame(d)
	if err != nil {
		t.Fatalf("MarshalDatagram: %v", err)
	}
	want := `{"type":"datagram","v":2,"mode":"response","class":"control","src":"` + testDatagramSrcHex +
		`","dst":"` + testDatagramDstHex + `","ttl":10,"dtype":"post_identity","payload":""}`
	if string(got) != want {
		t.Fatalf("canonical form mismatch:\n got %s\nwant %s", got, want)
	}
}

func TestDatagramRoundTripIsByteStable(t *testing.T) {
	signed := newSignedDatagram(t)

	bulk := signed.Clone()
	bulk.Class = domain.DatagramClassBulk
	bulk.Payload = bytes.Repeat([]byte{0x5a}, domain.DatagramBulkPayloadCap)
	bulk, err := SignDatagram(bulk, testDatagramNetwork, testDatagramKey(t))
	if err != nil {
		t.Fatalf("SignDatagram(bulk): %v", err)
	}

	request := DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRequest,
		Class:       domain.DatagramClassControl,
		Src:         mustIdentity(t, testDatagramSrcHex),
		Dst:         mustIdentity(t, testDatagramDstHex),
		TTL:         255,
		RoutePolicy: domain.RoutePolicyExplore,
		DType:       domain.DType("get_identity"),
		Payload:     []byte("q"),
	}
	response := DatagramFrame{
		Version: domain.DatagramHeaderVersion,
		Mode:    domain.DatagramModeResponse,
		Class:   domain.DatagramClassControl,
		Src:     mustIdentity(t, testDatagramSrcHex),
		Dst:     mustIdentity(t, testDatagramDstHex),
		TTL:     1,
		DType:   domain.DType("post_identity"),
		Payload: bytes.Repeat([]byte{0x00}, domain.DatagramControlPayloadCap),
	}

	cases := map[string]DatagramFrame{
		"routed control":                signed,
		"routed bulk at every boundary": bulk,
		"request":                       request,
		"response without route_policy": response,
	}
	for name, original := range cases {
		t.Run(name, func(t *testing.T) {
			wire, err := MarshalDatagramFrame(original)
			if err != nil {
				t.Fatalf("MarshalDatagram: %v", err)
			}
			parsed, err := ParseDatagramFrame(wire)
			if err != nil {
				t.Fatalf("ParseDatagram: %v", err)
			}
			again, err := MarshalDatagramFrame(parsed)
			if err != nil {
				t.Fatalf("MarshalDatagramFrame(parsed): %v", err)
			}
			if !bytes.Equal(wire, again) {
				t.Fatalf("round trip changed the wire bytes:\n got %s\nwant %s", again, wire)
			}
			if original.Auth == nil {
				return
			}
			originalTranscript, err := BuildDatagramTranscript(original, testDatagramNetwork)
			if err != nil {
				t.Fatalf("BuildDatagramTranscript(original, testDatagramNetwork): %v", err)
			}
			parsedTranscript, err := BuildDatagramTranscript(parsed, testDatagramNetwork)
			if err != nil {
				t.Fatalf("BuildDatagramTranscript(parsed, testDatagramNetwork): %v", err)
			}
			if !bytes.Equal(originalTranscript, parsedTranscript) {
				t.Fatal("round trip changed the transcript")
			}
			if err := VerifyDatagramSignature(parsed, testDatagramNetwork); err != nil {
				t.Fatalf("VerifyDatagramSignature after round trip: %v", err)
			}
		})
	}
}

func TestParseDatagramKeepsDecodedFields(t *testing.T) {
	original := newSignedDatagram(t)
	wire, err := MarshalDatagramFrame(original)
	if err != nil {
		t.Fatalf("MarshalDatagram: %v", err)
	}
	parsed, err := ParseDatagramFrame(wire)
	if err != nil {
		t.Fatalf("ParseDatagram: %v", err)
	}
	if !reflect.DeepEqual(original, parsed) {
		t.Fatalf("parsed datagram differs from the original:\n got %+v\nwant %+v", parsed, original)
	}
}

// TestParseDatagramKeepsRawTTL pins that the parser neither clamps nor
// decrements: `ttl <= auth.max_ttl` is checked by the pipeline against the
// raw value, and a clamp here would turn a hostile ttl=255 into a legal 10.
func TestParseDatagramKeepsRawTTL(t *testing.T) {
	line := buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc, fragDst,
		`"ttl":255`, fragPolicyBest, fragDType, fragPayload, fragAuth)
	parsed, err := ParseDatagramFrame([]byte(line))
	if err != nil {
		t.Fatalf("ParseDatagram: %v", err)
	}
	if parsed.TTL != 255 {
		t.Fatalf("TTL = %d, want the raw 255", parsed.TTL)
	}
	if parsed.Auth.MaxTTL != 10 {
		t.Fatalf("MaxTTL = %d, want 10", parsed.Auth.MaxTTL)
	}
}

func TestDatagramModeMatrix(t *testing.T) {
	const authFree = ``
	cases := []struct {
		name    string
		mode    string
		class   string
		policy  string
		auth    string
		wantErr error
	}{
		{"routed control signed", fragModeRouted, fragClassControl, fragPolicyBest, fragAuth, nil},
		{"routed bulk signed", fragModeRouted, fragClassBulk, fragPolicyBest, fragAuth, nil},
		{"request control", fragModeRequest, fragClassControl, fragPolicyBest, authFree, nil},
		{"response control", fragModeResponse, fragClassControl, ``, authFree, nil},

		{"routed without auth", fragModeRouted, fragClassControl, fragPolicyBest, authFree, ErrDatagramAuth},
		{"routed without route_policy", fragModeRouted, fragClassControl, ``, fragAuth, ErrDatagramModeMatrix},
		{"request bulk", fragModeRequest, fragClassBulk, fragPolicyBest, authFree, ErrDatagramModeMatrix},
		{"request with auth", fragModeRequest, fragClassControl, fragPolicyBest, fragAuth, ErrDatagramAuth},
		{"request without route_policy", fragModeRequest, fragClassControl, ``, authFree, ErrDatagramModeMatrix},
		{"response bulk", fragModeResponse, fragClassBulk, ``, authFree, ErrDatagramModeMatrix},
		{"response with auth", fragModeResponse, fragClassControl, ``, fragAuth, ErrDatagramAuth},
		{"response with route_policy", fragModeResponse, fragClassControl, fragPolicyBest, authFree, ErrDatagramModeMatrix},
		{"unknown mode", `"mode":"broadcast"`, fragClassControl, fragPolicyBest, fragAuth, ErrDatagramModeMatrix},
		{"unknown class", fragModeRouted, `"class":"gossip"`, fragPolicyBest, fragAuth, ErrDatagramModeMatrix},
		{"unknown route_policy", fragModeRouted, fragClassControl, `"route_policy":"fastest"`, fragAuth, ErrDatagramModeMatrix},
		{"empty route_policy", fragModeRouted, fragClassControl, `"route_policy":""`, fragAuth, ErrDatagramModeMatrix},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fragments := []string{fragType, fragVersion, tc.mode, tc.class, fragSrc, fragDst, fragTTL}
			if tc.policy != "" {
				fragments = append(fragments, tc.policy)
			}
			fragments = append(fragments, fragDType, fragPayload)
			if tc.auth != "" {
				fragments = append(fragments, tc.auth)
			}
			_, err := ParseDatagramFrame([]byte(buildDatagramJSON(fragments...)))
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("ParseDatagram: unexpected error %v", err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("ParseDatagram error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestDatagramPayloadCapPerClass(t *testing.T) {
	cases := []struct {
		name    string
		class   domain.DatagramClass
		size    int
		wantErr error
	}{
		{"control at cap", domain.DatagramClassControl, domain.DatagramControlPayloadCap, nil},
		{"control over cap", domain.DatagramClassControl, domain.DatagramControlPayloadCap + 1, ErrDatagramPayloadTooLarge},
		{"bulk at cap", domain.DatagramClassBulk, domain.DatagramBulkPayloadCap, nil},
		{"bulk over cap", domain.DatagramClassBulk, domain.DatagramBulkPayloadCap + 1, ErrDatagramPayloadTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x7f}, tc.size)
			line := buildDatagramJSON(fragType, fragVersion, fragModeRouted,
				`"class":"`+tc.class.String()+`"`, fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType,
				`"payload":"`+base64.RawURLEncoding.EncodeToString(payload)+`"`, fragAuth)
			parsed, err := ParseDatagramFrame([]byte(line))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ParseDatagram error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDatagram: %v", err)
			}
			// The cap is on DECODED bytes: the base64url field is ~4/3 longer.
			if len(parsed.Payload) != tc.size {
				t.Fatalf("decoded payload = %d bytes, want %d", len(parsed.Payload), tc.size)
			}
		})
	}
}

// TestMaxBulkDatagramFitsFrameLine is the size boundary of §2.3: a full
// 64 KiB bulk payload must stay under MaxFrameLine once the line includes
// its terminating newline.
func TestMaxBulkDatagramFitsFrameLine(t *testing.T) {
	d := newSignedDatagram(t)
	d.Class = domain.DatagramClassBulk
	d.Payload = bytes.Repeat([]byte{0xff}, domain.DatagramBulkPayloadCap)
	signed, err := SignDatagram(d, testDatagramNetwork, testDatagramKey(t))
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	line, err := MarshalDatagramFrameLine(signed)
	if err != nil {
		t.Fatalf("MarshalDatagramFrameLine: %v", err)
	}
	if !strings.HasSuffix(line, "\n") {
		t.Fatal("frame line must end with a newline")
	}
	if len(line) > MaxFrameLine {
		t.Fatalf("frame line = %d bytes, exceeds MaxFrameLine %d", len(line), MaxFrameLine)
	}
	parsed, err := ParseDatagramFrameLine(line)
	if err != nil {
		t.Fatalf("ParseDatagramFrameLine: %v", err)
	}
	if parsed.DecodedPayloadLen() != domain.DatagramBulkPayloadCap {
		t.Fatalf("decoded payload = %d bytes, want %d", parsed.DecodedPayloadLen(), domain.DatagramBulkPayloadCap)
	}

	// The two measures of §2.3 are deliberately different quantities: the
	// class ceiling counts decoded payload, budgets count the whole line.
	wireSize, err := parsed.WireFrameSize()
	if err != nil {
		t.Fatalf("WireFrameSize: %v", err)
	}
	if wireSize != len(line) {
		t.Fatalf("WireFrameSize = %d, want the line length %d", wireSize, len(line))
	}
	if wireSize <= parsed.DecodedPayloadLen() {
		t.Fatalf("wire size %d must exceed the decoded payload %d", wireSize, parsed.DecodedPayloadLen())
	}
}

// TestDatagramFrameLineBudgetCountsNewline pins the off-by-one: the budget
// covers the newline on both the sender and the receiver side, so a body of
// exactly maxBytes is rejected by both.
func TestDatagramFrameLineBudgetCountsNewline(t *testing.T) {
	d := newSignedDatagram(t)
	body, err := MarshalDatagramFrame(d)
	if err != nil {
		t.Fatalf("MarshalDatagram: %v", err)
	}
	if _, err := MarshalDatagramFrameLineWithLimit(d, len(body)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("limit == len(body): error = %v, want ErrFrameTooLarge", err)
	}
	if _, err := MarshalDatagramFrameLineWithLimit(d, len(body)+1); err != nil {
		t.Fatalf("limit == len(body)+1: unexpected error %v", err)
	}

	oversized := strings.Repeat("a", MaxFrameLine)
	if _, err := ParseDatagramFrameLine(oversized); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ParseDatagramFrameLine(%d bytes without newline): error = %v, want ErrFrameTooLarge", MaxFrameLine, err)
	}
	if _, err := ParseDatagramFrameLine(oversized + "\n"); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ParseDatagramFrameLine(%d bytes with newline): error = %v, want ErrFrameTooLarge", MaxFrameLine+1, err)
	}
}

func TestDatagramCloneIsDeep(t *testing.T) {
	original := newSignedDatagram(t)
	clone := original.Clone()
	clone.Payload[0] ^= 0xff
	clone.Auth.Salt[0] ^= 0xff
	clone.Auth.Sig[0] ^= 0xff
	if original.Payload[0] == clone.Payload[0] ||
		original.Auth.Salt[0] == clone.Auth.Salt[0] ||
		original.Auth.Sig[0] == clone.Auth.Sig[0] {
		t.Fatal("Clone aliases the original")
	}
}

// nonCanonicalBase64 returns the SAME bytes spelled with non-zero trailing
// padding bits — the second wire form §3.4 forbids. It searches the alphabet
// instead of hard-coding a character so the helper states its own premise: a
// spelling is only "second" if the permissive decoder maps both to one value.
func nonCanonicalBase64(t *testing.T, canonical string) string {
	t.Helper()
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	want, err := base64.RawURLEncoding.DecodeString(canonical)
	if err != nil {
		t.Fatalf("decode %q: %v", canonical, err)
	}
	head := canonical[:len(canonical)-1]
	for i := range len(alphabet) {
		candidate := head + alphabet[i:i+1]
		if candidate == canonical {
			continue
		}
		got, err := base64.RawURLEncoding.DecodeString(candidate)
		if err != nil || !bytes.Equal(got, want) {
			continue
		}
		return candidate
	}
	t.Fatalf("%q carries no padding bits, so it has no second spelling", canonical)
	return ""
}

// TestParseDatagramFrameRejectsNonCanonicalBase64 pins the canonical-form rule
// of §3.4 on EVERY base64url field of the wire: one value has exactly one
// spelling. The permissive decoder maps both spellings to the same bytes, so
// without the strict decoder two different lines would produce one frame — and
// two implementations would disagree on which one was signed.
func TestParseDatagramFrameRejectsNonCanonicalBase64(t *testing.T) {
	frame := newSignedDatagram(t)
	line, err := MarshalDatagramFrame(frame)
	if err != nil {
		t.Fatalf("MarshalDatagramFrame: %v", err)
	}
	if _, err := ParseDatagramFrame(line); err != nil {
		t.Fatalf("the canonical line must parse: %v", err)
	}

	tests := []struct {
		name      string
		canonical string
	}{
		{name: "payload", canonical: base64.RawURLEncoding.EncodeToString(frame.Payload)},
		{name: "auth.pubkey", canonical: base64.RawURLEncoding.EncodeToString(frame.Auth.PubKey)},
		{name: "auth.salt", canonical: base64.RawURLEncoding.EncodeToString(frame.Auth.Salt)},
		{name: "auth.sig", canonical: base64.RawURLEncoding.EncodeToString(frame.Auth.Sig)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := nonCanonicalBase64(t, test.canonical)
			quoted := []byte(`"` + test.canonical + `"`)
			if bytes.Count(line, quoted) != 1 {
				t.Fatalf("%s is not a unique fragment of the line", test.name)
			}
			second := bytes.Replace(line, quoted, []byte(`"`+mutated+`"`), 1)

			if _, err := ParseDatagramFrame(second); !errors.Is(err, ErrDatagramEncoding) {
				t.Fatalf("non-canonical %s: error = %v, want ErrDatagramEncoding", test.name, err)
			}
		})
	}
}
