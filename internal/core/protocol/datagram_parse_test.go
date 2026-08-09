package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// authFragment renders an auth block with the given field spellings so a
// case can vary exactly one of them.
func authFragment(av, pubkey, salt, maxTTL, unixTime, sig string) string {
	return `"auth":{"av":` + av + `,"pubkey":"` + pubkey + `","salt":"` + salt +
		`","max_ttl":` + maxTTL + `,"time":` + unixTime + `,"sig":"` + sig + `"}`
}

func TestParseDatagramRejects(t *testing.T) {
	longPayload := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, domain.DatagramControlPayloadCap+1))

	cases := []struct {
		name    string
		frame   string
		wantErr error
	}{
		// --- structure -----------------------------------------------------
		{"not an object", `["datagram"]`, ErrDatagramMalformed},
		{"trailing content", routedFrameJSON() + `{"type":"datagram"}`, ErrDatagramMalformed},
		{"wrong frame type", buildDatagramJSON(`"type":"file_command"`, fragVersion, fragModeRouted,
			fragClassControl, fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"missing type", buildDatagramJSON(fragVersion, fragModeRouted, fragClassControl, fragSrc, fragDst,
			fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"missing dtype", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragPayload, fragAuth), ErrDatagramMalformed},
		{"missing payload", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragAuth), ErrDatagramMalformed},
		{"missing ttl", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"null dtype", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, `"dtype":null`, fragPayload, fragAuth), ErrDatagramMalformed},

		// --- unknown fields ------------------------------------------------
		{"unknown header field", routedFrameJSON(`"hint":"store"`), ErrDatagramUnknownField},
		{"unknown auth field", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			`"auth":{"av":1,"pubkey":"`+testDatagramPubKey+`","salt":"`+testDatagramSalt+
				`","max_ttl":10,"time":1780000000,"sig":"`+testDatagramSig+`","nonce":"x"}`), ErrDatagramUnknownField},
		// `req_caps` and `ext` were KNOWN keys in v1 and are unknown keys in
		// v2: the envelope carries no extension points, so a frame still
		// spelling them is a stable-header violation like any other.
		{"req_caps is no longer a field", routedFrameJSON(`"req_caps":["cap_a"]`), ErrDatagramUnknownField},
		{"ext is no longer a field", routedFrameJSON(`"ext":{"cap":"cap_a","v":1,"data":""}`), ErrDatagramUnknownField},

		// --- duplicate keys ------------------------------------------------
		{"duplicate header key", routedFrameJSON(fragTTL), ErrDatagramDuplicateKey},
		{"duplicate type key", routedFrameJSON(fragType), ErrDatagramDuplicateKey},
		{"duplicate auth key", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			`"auth":{"av":1,"av":2,"pubkey":"`+testDatagramPubKey+`","salt":"`+testDatagramSalt+
				`","max_ttl":10,"time":1780000000,"sig":"`+testDatagramSig+`"}`), ErrDatagramDuplicateKey},

		// --- header version ------------------------------------------------
		{"unknown version", buildDatagramJSON(fragType, `"v":3`, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramUnknownVersion},
		// v1 is the envelope with req_caps/ext and a different transcript. It
		// is refused by the same rule and for the same reason a future version
		// is: this build cannot claim to understand it.
		{"superseded version 1", buildDatagramJSON(fragType, `"v":1`, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramUnknownVersion},
		{"zero version", buildDatagramJSON(fragType, `"v":0`, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"string version", buildDatagramJSON(fragType, `"v":"1"`, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"fractional version", buildDatagramJSON(fragType, `"v":1.0`, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},

		// --- canonical encodings -------------------------------------------
		{"uppercase src", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			`"src":"`+strings.ToUpper(testDatagramSrcHex)+`"`, fragDst, fragTTL, fragPolicyBest, fragDType,
			fragPayload, fragAuth), ErrDatagramEncoding},
		{"uppercase dst", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			`"dst":"`+strings.ToUpper(testDatagramDstHex)+`"`, fragTTL, fragPolicyBest, fragDType,
			fragPayload, fragAuth), ErrDatagramEncoding},
		{"short src", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			`"src":"56475aa7"`, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramEncoding},
		{"empty dst", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			`"dst":""`, fragTTL, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramEncoding},
		{"padded payload", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, `"payload":"EBESExQVFhcYGRobHB0eHw=="`, fragAuth), ErrDatagramEncoding},
		{"standard base64 payload", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, `"payload":"++//"`, fragAuth), ErrDatagramEncoding},
		{"padded pubkey", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", testDatagramPubKey+"=", testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramEncoding},

		// --- auth ----------------------------------------------------------
		{"av zero", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc, fragDst,
			fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("0", testDatagramPubKey, testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramAuth},
		{"av fractional", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1.5", testDatagramPubKey, testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramMalformed},
		{"av 256", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc, fragDst,
			fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("256", testDatagramPubKey, testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramAuth},
		{"av negative", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("-1", testDatagramPubKey, testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramAuth},
		{"av string", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment(`"1"`, testDatagramPubKey, testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramMalformed},
		// An `av` this build does not implement is an unknown VERSION, not a
		// malformed frame: the pipeline drops it without forwarding and
		// without ban, exactly as it does an unknown `v`. Nothing else keeps
		// such a frame away from a verifier that would check it as Ed25519,
		// fail, and charge the neighbour that only relayed it.
		{"unimplemented av", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("2", testDatagramPubKey, testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramUnknownVersion},
		{"pubkey wrong size", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", "AAAA", testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramEncoding},
		{"salt wrong size", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", testDatagramPubKey, "AAAA", "10", "1780000000", testDatagramSig)), ErrDatagramEncoding},
		{"sig wrong size", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", testDatagramPubKey, testDatagramSalt, "10", "1780000000", "AAAA")), ErrDatagramEncoding},
		{"missing pubkey", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			`"auth":{"av":1,"salt":"`+testDatagramSalt+`","max_ttl":10,"time":1780000000,"sig":"`+testDatagramSig+`"}`), ErrDatagramAuth},
		{"missing time", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			`"auth":{"av":1,"pubkey":"`+testDatagramPubKey+`","salt":"`+testDatagramSalt+
				`","max_ttl":10,"sig":"`+testDatagramSig+`"}`), ErrDatagramAuth},
		{"auth not an object", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload, `"auth":"signed"`), ErrDatagramMalformed},
		// A key that is not base64url at all is refused by the encoding gate,
		// before there is anything to fingerprint or verify (§9, public key).
		{"pubkey not base64url", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", "not*base64url!", testDatagramSalt, "10", "1780000000", testDatagramSig)), ErrDatagramEncoding},
		{"salt not base64url", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", testDatagramPubKey, "not*base64url!", "10", "1780000000", testDatagramSig)), ErrDatagramEncoding},
		{"sig not base64url", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", testDatagramPubKey, testDatagramSalt, "10", "1780000000", "not*base64url!")), ErrDatagramEncoding},
		{"payload not base64url", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, `"payload":"not*base64url!"`, fragAuth), ErrDatagramEncoding},

		// --- ttl -----------------------------------------------------------
		{"ttl 256", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc, fragDst,
			`"ttl":256`, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"ttl negative", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, `"ttl":-1`, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},
		{"ttl fractional", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, `"ttl":10.0`, fragPolicyBest, fragDType, fragPayload, fragAuth), ErrDatagramMalformed},

		// --- dtype ---------------------------------------------------------
		{"uppercase dtype", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, `"dtype":"Delivery_Receipt"`, fragPayload, fragAuth), ErrDatagramMalformed},
		{"dashed dtype", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, `"dtype":"delivery-receipt"`, fragPayload, fragAuth), ErrDatagramMalformed},
		{"empty dtype", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, `"dtype":""`, fragPayload, fragAuth), ErrDatagramMalformed},
		{"oversized dtype", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl, fragSrc,
			fragDst, fragTTL, fragPolicyBest, `"dtype":"`+strings.Repeat("d", domain.MaxDTypeLen+1)+`"`,
			fragPayload, fragAuth), ErrDatagramMalformed},

		// --- payload cap ---------------------------------------------------
		{"control payload over cap", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, `"payload":"`+longPayload+`"`, fragAuth), ErrDatagramPayloadTooLarge},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDatagramFrame([]byte(tc.frame))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ParseDatagram error = %v, want %v", err, tc.wantErr)
			}
		})
	}

	// Boundaries that must be ACCEPTED, stated next to the rejects they border.
	accepted := []struct {
		name  string
		frame string
	}{
		{"negative auth time", buildDatagramJSON(fragType, fragVersion, fragModeRouted, fragClassControl,
			fragSrc, fragDst, fragTTL, fragPolicyBest, fragDType, fragPayload,
			authFragment("1", testDatagramPubKey, testDatagramSalt, "10", "-1", testDatagramSig))},
	}
	for _, tc := range accepted {
		t.Run("accepted/"+tc.name, func(t *testing.T) {
			if _, err := ParseDatagramFrame([]byte(tc.frame)); err != nil {
				t.Fatalf("ParseDatagram: unexpected error %v", err)
			}
		})
	}
}

// TestParseDatagramReadsVersionBeforeTheClosedFieldSet pins the ORDER of two
// rejects whose price to the sender differs. A header version this build does
// not implement is dropped without forwarding and WITHOUT ban points (§2),
// while an unknown key under the implemented version is a violation of the
// stable header and IS charged (§3.4, §4.4). The closed key set is therefore
// the key set of a KNOWN version: judging it before `v` is read turns a lawful
// future extension into misbehaviour and punishes the neighbour that relayed
// it.
//
// The legs live in one fixture on purpose. A parser that answered
// ErrDatagramUnknownVersion to everything, and one that refused everything,
// would both satisfy the first leg alone.
func TestParseDatagramReadsVersionBeforeTheClosedFieldSet(t *testing.T) {
	t.Parallel()

	const futureHeaderField = `"hint":"store"`
	futureAuth := `"auth":{"av":1,"pubkey":"` + testDatagramPubKey + `","salt":"` + testDatagramSalt +
		`","max_ttl":10,"time":1780000000,"sig":"` + testDatagramSig + `","nonce":"x"}`

	// Only `v` and the trailing fragments vary, so a leg cannot pass for a
	// reason another leg does not share.
	frameWith := func(version string, tail ...string) string {
		base := []string{
			fragType, version, fragModeRouted, fragClassControl, fragSrc, fragDst,
			fragTTL, fragPolicyBest, fragDType, fragPayload,
		}
		return buildDatagramJSON(append(base, tail...)...)
	}

	cases := []struct {
		name    string
		frame   string
		wantErr error
	}{
		{
			name:    "future version with a header field this build never heard of",
			frame:   frameWith(`"v":3`, fragAuth, futureHeaderField),
			wantErr: ErrDatagramUnknownVersion,
		},
		{
			// This row is a GUARD, not a pin: the auth key set already lived
			// behind the version gate, so putting the header key set back in
			// front of it leaves this row green. It fails only if somebody
			// moves the nested set out in front too. Rows 1 and 3 are what
			// hold the change under test.
			name:    "future version with an auth field this build never heard of",
			frame:   frameWith(`"v":3`, futureAuth),
			wantErr: ErrDatagramUnknownVersion,
		},
		{
			name:    "implemented version with the same header field",
			frame:   frameWith(fragVersion, fragAuth, futureHeaderField),
			wantErr: ErrDatagramUnknownField,
		},
		{
			name:    "implemented version with the same auth field",
			frame:   frameWith(fragVersion, futureAuth),
			wantErr: ErrDatagramUnknownField,
		},
		{
			name:  "implemented version without either field",
			frame: frameWith(fragVersion, fragAuth),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseDatagramFrame([]byte(tc.frame))
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("ParseDatagramFrame: unexpected error %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ParseDatagramFrame error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestParseDatagramReadsVersionBeforeTheStructureItJudges is the STRUCTURAL
// half of the rule the test above states for FIELDS, and it is the half a
// closed key set cannot cover: a version this build does not implement owns its
// own SHAPE as well as its own key names.
//
// maxDatagramJSONDepth is the nesting a version-2 frame needs plus slack. It is
// a v2 number, so applying it to a v3 frame prices a lawful future structure as
// ErrDatagramMalformed — which parseRefusal (datagram/pipeline.go) and
// datagramBanWorthy (node) both read as a stable-header violation and charge ban
// points for. The neighbour that handed the frame over did not write it, so what
// the extension mechanism costs it must be a silent drop and nothing else (§2,
// §4.4).
//
// The legs live in one fixture for the reason the field test states: a parser
// that answered ErrDatagramUnknownVersion to every deep document would pass the
// first leg alone, and the nesting bound must keep biting under the version this
// build DOES judge.
func TestParseDatagramReadsVersionBeforeTheStructureItJudges(t *testing.T) {
	t.Parallel()

	// One container deeper than a version-2 frame is allowed to be: the frame
	// object plus four more levels.
	deepObject := `"pad":` + strings.Repeat(`{"a":`, maxDatagramJSONDepth) + `1` +
		strings.Repeat(`}`, maxDatagramJSONDepth)
	deepArray := `"pad":` + strings.Repeat(`[`, maxDatagramJSONDepth) + `1` +
		strings.Repeat(`]`, maxDatagramJSONDepth)

	frameWith := func(version string, tail ...string) string {
		base := []string{
			fragType, version, fragModeRouted, fragClassControl, fragSrc, fragDst,
			fragTTL, fragPolicyBest, fragDType, fragPayload,
		}
		return buildDatagramJSON(append(base, tail...)...)
	}

	cases := []struct {
		name    string
		frame   string
		wantErr error
	}{
		{
			name:    "future version nested deeper than this build's bound",
			frame:   frameWith(`"v":3`, fragAuth, deepObject),
			wantErr: ErrDatagramUnknownVersion,
		},
		{
			name:    "future version with an array deeper than this build's bound",
			frame:   frameWith(`"v":3`, fragAuth, deepArray),
			wantErr: ErrDatagramUnknownVersion,
		},
		{
			// The bound is not removed, it is scoped: under the version whose
			// schema this build owns, a hostile "[[[[…" is still malformed.
			name:    "implemented version keeps the nesting bound",
			frame:   frameWith(fragVersion, fragAuth, deepObject),
			wantErr: ErrDatagramMalformed,
		},
		{
			// The stable header is stable in EVERY version: two spellings of `v`
			// let two readers route different frames, so the frame is refused
			// before any version can be read out of it.
			name:    "future version naming the version twice",
			frame:   frameWith(`"v":3`, fragAuth, `"v":4`),
			wantErr: ErrDatagramDuplicateKey,
		},
		{
			name:    "future version naming no type at all",
			frame:   buildDatagramJSON(`"v":3`, fragModeRouted, fragPayload),
			wantErr: ErrDatagramMalformed,
		},
		{
			// The control: nothing exotic about this one but the version, and it
			// must still be dropped as an unknown version rather than accepted.
			name:    "future version with an ordinary body",
			frame:   frameWith(`"v":3`, fragAuth),
			wantErr: ErrDatagramUnknownVersion,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseDatagramFrame([]byte(tc.frame))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ParseDatagramFrame error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestParseDatagramRejectsEscapedDuplicateKey covers the two spellings
// encoding/json would silently collapse: a key containing an escaped quote
// and a key written with a unicode escape.
func TestParseDatagramRejectsEscapedDuplicateKey(t *testing.T) {
	cases := map[string]string{
		"escaped quote in key": routedFrameJSON(`"a\"b":1`, `"a\"b":2`),
		"unicode escaped key":  routedFrameJSON(`"\u0068int":1`, `"hint":2`),
		"escaped ttl key":      routedFrameJSON(`"\u0074tl":9`),
	}
	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseDatagramFrame([]byte(frame)); !errors.Is(err, ErrDatagramDuplicateKey) {
				t.Fatalf("ParseDatagram error = %v, want ErrDatagramDuplicateKey", err)
			}
		})
	}
}

func TestScanDuplicateJSONKeys(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr error
	}{
		{"flat object", `{"a":1,"b":2}`, nil},
		{"nested objects", `{"a":{"b":1},"b":{"a":2}}`, nil},
		{"array of objects", `{"a":[{"x":1},{"x":2}]}`, nil},
		{"string value equal to a key name", `{"a":"a","b":"a"}`, nil},
		{"key name appearing inside a value", `{"a":"\"b\":1","b":2}`, nil},
		{"escape before quote in value", `{"a":"c:\\","b":2}`, nil},
		{"duplicate at top level", `{"a":1,"a":2}`, ErrDatagramDuplicateKey},
		{"duplicate in nested object", `{"a":{"b":1,"b":2}}`, ErrDatagramDuplicateKey},
		{"duplicate in object inside array", `{"a":[{"x":1,"x":2}]}`, ErrDatagramDuplicateKey},
		{"duplicate with escaped quote", `{"a\"b":1,"a\"b":2}`, ErrDatagramDuplicateKey},
		{"duplicate via unicode escape", `{"ab":1,"\u0061b":2}`, ErrDatagramDuplicateKey},
		{"unterminated string", `{"a":"b}`, ErrDatagramMalformed},
		{"too deep", strings.Repeat(`{"a":`, maxDatagramJSONDepth+1) + `1` + strings.Repeat(`}`, maxDatagramJSONDepth+1), ErrDatagramMalformed},
		{"unbalanced close", `{"a":1}}`, ErrDatagramMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scanDuplicateJSONKeys([]byte(tc.raw), maxDatagramJSONDepth)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("scanDuplicateJSONKeys: unexpected error %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("scanDuplicateJSONKeys error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestParseDatagramIgnoresKeyOrder pins that a non-canonical (but legal)
// key order still parses: canonical order is a SENDER obligation, and a
// receiver that rejected reordering would drop legitimate traffic.
func TestParseDatagramIgnoresKeyOrder(t *testing.T) {
	reordered := buildDatagramJSON(fragPayload, fragDType, fragAuth, fragPolicyBest, fragTTL, fragDst,
		fragSrc, fragClassControl, fragModeRouted, fragVersion, fragType)
	parsed, err := ParseDatagramFrame([]byte(reordered))
	if err != nil {
		t.Fatalf("ParseDatagram: %v", err)
	}
	canonical, err := MarshalDatagramFrame(parsed)
	if err != nil {
		t.Fatalf("MarshalDatagram: %v", err)
	}
	if !bytes.Equal(canonical, []byte(routedFrameJSON())) {
		t.Fatalf("re-serialization is not canonical:\n got %s\nwant %s", canonical, routedFrameJSON())
	}
}

// The duplicate scan and encoding/json must agree on what "the same key" is,
// or the two parsers diverge exactly the way §3.4 exists to prevent: one
// verifies the signature over one spelling and routes the other.
//
// Invalid UTF-8 is where they part company unless the scan normalises: the
// raw bytes 0x80 and the literal U+FFFD are different byte strings, while
// encoding/json turns both into U+FFFD and silently keeps the last one.
func TestDuplicateScanNormalisesKeysLikeEncodingJSON(t *testing.T) {
	t.Parallel()

	raw := []byte("{\"\x80\":1,\"�\":2}")

	// What encoding/json does with this input is the reference behaviour.
	var decoded map[string]int
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("encoding/json kept %d keys, the premise of this test is that it collapses them", len(decoded))
	}

	if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); !errors.Is(err, ErrDatagramDuplicateKey) {
		t.Fatalf("scan = %v, want a duplicate-key rejection", err)
	}
}

// Escaped and plain spellings of one key still collide, and distinct keys
// still pass: normalisation must not turn the scan into a blunt instrument.
func TestDuplicateScanKeepsDistinctKeysApart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want error
	}{
		{name: "escaped spelling of one key", raw: `{"ab":1,"ab":2}`, want: ErrDatagramDuplicateKey},
		{name: "distinct invalid keys", raw: "{\"\x80a\":1,\"\x80b\":2}", want: nil},
		{name: "distinct plain keys", raw: `{"a":1,"b":2}`, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := scanDuplicateJSONKeys([]byte(test.raw), maxDatagramJSONDepth)
			if test.want == nil {
				if err != nil {
					t.Fatalf("scan = %v, want accepted", err)
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("scan = %v, want %v", err, test.want)
			}
		})
	}
}

// The scan runs BEFORE authentication — before the signature, before
// requireKnownFields — so its allocations are a pre-auth amplifier (§2.2). A
// map per open object turns a 128 KiB frame of tiny objects into ~10k map
// allocations that any unauthenticated peer can ask for.
func TestDuplicateScanDoesNotAllocatePerObject(t *testing.T) {
	var body strings.Builder
	body.WriteString(`{"objects":[`)
	const objects = 2000
	for i := 0; i < objects; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(`{"x":1,"y":2}`)
	}
	body.WriteString(`]}`)
	raw := []byte(body.String())

	allocs := testing.AllocsPerRun(20, func() {
		if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); err != nil {
			t.Fatalf("scan: %v", err)
		}
	})
	// The scan needs its scope stack and nothing else: the per-object storage
	// is reused as the stack pops and pushes again.
	if allocs > float64(maxDatagramJSONDepth)+8 {
		t.Fatalf("allocations = %.0f for %d objects, want a bound independent of the object count", allocs, objects)
	}
}
