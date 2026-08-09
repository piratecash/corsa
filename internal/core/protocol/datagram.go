package protocol

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram.go defines the datagram wire format: the decoded frame, its
// structural contract (the mode matrix of §2.1) and the canonical
// serialization. The strict parser lives in datagram_parse.go and the
// signature transcript in datagram_transcript.go.
//
// Reference: docs/refactoring/datagram-transport.md §2, §2.1, §2.2, §2.3, §3.

// DatagramFrameType is the top-level `type` of every datagram, in every
// header version. The name never changes: dispatch happens on it, so an
// old node always recognises the command, reads `v` and drops an unknown
// version silently instead of answering unknown_command and closing the
// connection (§2).
const DatagramFrameType = "datagram"

// Structural rejects are distinguishable sentinels so the pipeline can act
// on the reason (unknown version → drop without forwarding; matrix
// violation → drop with a metric) without matching error text.
var (
	// ErrDatagramMalformed covers JSON that is not a well-formed datagram
	// frame: bad syntax, wrong top-level type, missing mandatory field,
	// wrong JSON kind for a field.
	ErrDatagramMalformed = errors.New("datagram: malformed frame")

	// ErrDatagramDuplicateKey marks a repeated JSON key anywhere in the
	// frame. Kept separate because it is the one reject encoding/json
	// cannot produce on its own (§3.4).
	ErrDatagramDuplicateKey = errors.New("datagram: duplicate JSON key")

	// ErrDatagramUnknownField marks an unknown key in the header or the auth
	// object. Extension goes through `v` and `av`, never through a field the
	// receiver silently ignores (§3.4).
	ErrDatagramUnknownField = errors.New("datagram: unknown field")

	// ErrDatagramUnknownVersion marks a header version this build does not
	// implement. The pipeline drops such a frame WITHOUT forwarding, which
	// is why it must be distinguishable from every other reject (§2).
	ErrDatagramUnknownVersion = errors.New("datagram: unknown header version")

	// ErrDatagramModeMatrix marks a mode/class/auth/route_policy tuple
	// outside the closed contract of §2.1.
	ErrDatagramModeMatrix = errors.New("datagram: mode combination not allowed")

	// ErrDatagramPayloadTooLarge marks a decoded payload above the class
	// ceiling of §2.3.
	ErrDatagramPayloadTooLarge = errors.New("datagram: payload exceeds class cap")

	// ErrDatagramEncoding marks a non-canonical encoding: uppercase hex,
	// padded base64url, or a binary field of the wrong length.
	ErrDatagramEncoding = errors.New("datagram: non-canonical encoding")

	// ErrDatagramAuth marks an auth block violating §2.1/§3.1: present in
	// the wrong mode, missing, or with a wrong-sized key/salt/signature.
	ErrDatagramAuth = errors.New("datagram: invalid auth block")
)

// DatagramFrame is the decoded frame. Binary values (payload, src/dst,
// pubkey, salt, signature) are held decoded: base64url and hex exist only at
// the wire boundary, so no code path can accidentally sign, size or compare
// the textual form (§3.2).
//
// The envelope carries NO extension points. It used to carry `req_caps` and
// `ext`, and both were path-wide: every transit checked the names and refused
// a frame naming one it does not advertise, which is exactly how a stable
// envelope stops being stable — a protocol released after a relay could not
// travel through it. Extension now lives where the two endpoints are: `dtype`
// names the protocol and the receiver gates on it, and the bytes of that
// protocol live in `payload`.
//
// There are no struct tags because encoding/json handles neither end of
// this contract: it cannot pin key order for the canonical form, cannot
// tell an absent optional field from an empty one, and cannot reject
// duplicate or unknown keys the way §3.4 demands. The wire key of each
// field is named beside it, and the closed key sets live in
// datagram_parse.go.
type DatagramFrame struct {
	Version domain.DatagramVersion // "v"
	Mode    domain.DatagramMode    // "mode"
	Class   domain.DatagramClass   // "class"
	Src     domain.PeerIdentity    // "src", lowercase 40-hex
	Dst     domain.PeerIdentity    // "dst", lowercase 40-hex
	// TTL is the RAW hop budget exactly as it arrived. Clamping to
	// DatagramDefaultMaxHops and the single per-hop decrement belong to the
	// pipeline (§4.1.1); the wire layer must not hide the raw value,
	// because `ttl <= auth.max_ttl` is checked against it.
	TTL         uint8              // "ttl"
	RoutePolicy domain.RoutePolicy // "route_policy", absent in response
	DType       domain.DType       // "dtype"
	Payload     []byte             // "payload", base64url without padding
	// Auth is "auth": present exactly for mode routed (§2.1).
	Auth *DatagramAuth
}

// DatagramAuth is the self-contained signature block of a routed datagram.
// The public key travels in the frame on purpose: a transit relay must be
// able to verify authenticity without resolving src through any trust
// store (§3.1).
type DatagramAuth struct {
	AuthVersion domain.AuthVersion // "av"
	PubKey      []byte             // "pubkey", 32 raw Ed25519 bytes
	Salt        []byte             // "salt", 16 raw random bytes
	MaxTTL      uint8              // "max_ttl"
	Time        int64              // "time", signed epoch seconds
	Sig         []byte             // "sig", 64 raw Ed25519 bytes
}

// Validate enforces the whole structural contract of a datagram: header
// version, mode matrix, canonical field shapes and the class payload
// ceiling. It is shared by the parser and the serializer so a frame this
// node emits is exactly a frame this node would accept.
func (d DatagramFrame) Validate() error {
	return d.validate(true)
}

// validate runs the structural contract. requireSig is false only while
// signing, where the signature is the value being produced and the
// transcript covers everything except it (§3.2).
func (d DatagramFrame) validate(requireSig bool) error {
	if d.Version != domain.DatagramHeaderVersion {
		return fmt.Errorf("%w: %d", ErrDatagramUnknownVersion, d.Version)
	}
	rule, ok := domain.DatagramModeRuleFor(d.Mode)
	if !ok {
		return fmt.Errorf("%w: mode %q", ErrDatagramModeMatrix, string(d.Mode))
	}
	if !rule.AllowsClass(d.Class) {
		return fmt.Errorf("%w: class %q with mode %q", ErrDatagramModeMatrix, string(d.Class), string(d.Mode))
	}
	if err := d.validateAddresses(); err != nil {
		return err
	}
	if _, err := domain.ParseDType(string(d.DType)); err != nil {
		return fmt.Errorf("%w: %w", ErrDatagramMalformed, err)
	}
	if err := validateRoutePolicyPresence(d.RoutePolicy, rule); err != nil {
		return err
	}
	if err := d.validatePayloadSize(); err != nil {
		return err
	}
	return d.validateAuth(rule, requireSig)
}

func (d DatagramFrame) validateAddresses() error {
	if d.Src.IsZero() {
		return fmt.Errorf("%w: empty src", ErrDatagramMalformed)
	}
	if d.Dst.IsZero() {
		return fmt.Errorf("%w: empty dst", ErrDatagramMalformed)
	}
	return nil
}

func validateRoutePolicyPresence(policy domain.RoutePolicy, rule domain.DatagramModeRule) error {
	switch {
	case rule.RoutePolicyRequired && !policy.Valid():
		return fmt.Errorf("%w: route_policy %q", ErrDatagramModeMatrix, string(policy))
	case !rule.RoutePolicyRequired && !policy.IsNone():
		return fmt.Errorf("%w: route_policy forbidden in this mode", ErrDatagramModeMatrix)
	default:
		return nil
	}
}

func (d DatagramFrame) validatePayloadSize() error {
	limit, err := domain.DatagramPayloadCap(d.Class)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatagramModeMatrix, err)
	}
	if len(d.Payload) > limit {
		return fmt.Errorf("%w: %d decoded bytes exceed %d for class %q",
			ErrDatagramPayloadTooLarge, len(d.Payload), limit, string(d.Class))
	}
	return nil
}

func (d DatagramFrame) validateAuth(rule domain.DatagramModeRule, requireSig bool) error {
	if !rule.AuthRequired {
		if d.Auth != nil {
			return fmt.Errorf("%w: auth forbidden in mode %q", ErrDatagramAuth, string(d.Mode))
		}
		return nil
	}
	if d.Auth == nil {
		return fmt.Errorf("%w: auth required in mode %q", ErrDatagramAuth, string(d.Mode))
	}
	if d.Auth.AuthVersion < 1 {
		return fmt.Errorf("%w: av %d outside 1..255", ErrDatagramAuth, d.Auth.AuthVersion)
	}
	if d.Auth.AuthVersion != domain.AuthVersionBase {
		// `av` names the signature and timing profile, and this build
		// implements exactly one. It used to be admissible with an `ext`
		// naming the profile that owned it, and `req_caps` then kept the frame
		// away from nodes without that profile — both are gone, so an
		// unimplemented `av` has nothing left to keep it out of a verifier
		// that would check it as Ed25519, fail, and ban the neighbour that
		// merely relayed it.
		//
		// It is therefore refused as an UNKNOWN VERSION and not as a malformed
		// frame: that is the same reject class an unknown `v` gets, and the
		// pipeline already reacts to it the only correct way — drop without
		// forwarding, without ban, because a version this build never
		// implemented is the extension mechanism working as designed (§2).
		return fmt.Errorf("%w: av %d", ErrDatagramUnknownVersion, d.Auth.AuthVersion)
	}
	if len(d.Auth.PubKey) != domain.DatagramPubKeyBytes {
		return fmt.Errorf("%w: pubkey %d bytes, want %d", ErrDatagramEncoding, len(d.Auth.PubKey), domain.DatagramPubKeyBytes)
	}
	if len(d.Auth.Salt) != domain.DatagramSaltBytes {
		return fmt.Errorf("%w: salt %d bytes, want %d", ErrDatagramEncoding, len(d.Auth.Salt), domain.DatagramSaltBytes)
	}
	if requireSig && len(d.Auth.Sig) != domain.DatagramSigBytes {
		return fmt.Errorf("%w: sig %d bytes, want %d", ErrDatagramEncoding, len(d.Auth.Sig), domain.DatagramSigBytes)
	}
	return nil
}

// Clone returns a deep copy so a forwarding path can adjust ttl without
// aliasing the payload or the auth block of the frame it received.
func (d DatagramFrame) Clone() DatagramFrame {
	out := d
	out.Payload = append([]byte(nil), d.Payload...)
	if d.Auth != nil {
		auth := *d.Auth
		auth.PubKey = append([]byte(nil), d.Auth.PubKey...)
		auth.Salt = append([]byte(nil), d.Auth.Salt...)
		auth.Sig = append([]byte(nil), d.Auth.Sig...)
		out.Auth = &auth
	}
	return out
}

// ---------------------------------------------------------------------------
// Canonical serialization
// ---------------------------------------------------------------------------

// DecodedPayloadLen is the quantity the class ceiling of §2.3 is measured
// in: DECODED payload bytes. Named rather than inlined because the wire
// field is base64url and roughly a third larger, and comparing the wrong
// one against the cap silently changes the admitted frame size.
func (d DatagramFrame) DecodedPayloadLen() int { return len(d.Payload) }

// WireFrameSize is the quantity byte budgets and queue accounting are
// measured in (§2.3, §5): the canonical JSON plus its terminating newline.
// 64 KiB of payload occupy ≈ 88 KiB on the wire, and charging them as 64
// KiB would give away a third of the link for free.
//
// For a frame that ARRIVED, the pipeline charges the received line length
// it already holds — this method sizes a frame this node is about to emit,
// and the two agree byte for byte for a canonically serialized sender.
func (d DatagramFrame) WireFrameSize() (int, error) {
	body, err := MarshalDatagramFrame(d)
	if err != nil {
		return 0, err
	}
	return len(body) + 1, nil
}

// MarshalDatagramFrame serializes d into its canonical JSON form: fixed key
// order, base64url without padding, lowercase 40-hex addresses, and absent
// optional fields omitted entirely. encoding/json is deliberately not used
// — struct tags give no control over key order and `omitempty` cannot
// distinguish "absent" from "empty", while the canonical form is what the
// round-trip Parse(Marshal(d)) contract and the golden vector rest on.
func MarshalDatagramFrame(d DatagramFrame) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	writeJSONString(&buf, "type")
	buf.WriteByte(':')
	writeJSONString(&buf, DatagramFrameType)
	writeJSONUintField(&buf, "v", uint64(d.Version))
	writeJSONStringField(&buf, "mode", d.Mode.String())
	writeJSONStringField(&buf, "class", d.Class.String())
	writeJSONStringField(&buf, "src", d.Src.String())
	writeJSONStringField(&buf, "dst", d.Dst.String())
	writeJSONUintField(&buf, "ttl", uint64(d.TTL))
	if !d.RoutePolicy.IsNone() {
		writeJSONStringField(&buf, "route_policy", d.RoutePolicy.String())
	}
	writeJSONStringField(&buf, "dtype", d.DType.String())
	writeJSONStringField(&buf, "payload", base64.RawURLEncoding.EncodeToString(d.Payload))
	if d.Auth != nil {
		buf.WriteByte(',')
		writeJSONString(&buf, "auth")
		buf.WriteString(":{")
		writeJSONString(&buf, "av")
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatUint(uint64(d.Auth.AuthVersion), 10))
		writeJSONStringField(&buf, "pubkey", base64.RawURLEncoding.EncodeToString(d.Auth.PubKey))
		writeJSONStringField(&buf, "salt", base64.RawURLEncoding.EncodeToString(d.Auth.Salt))
		writeJSONUintField(&buf, "max_ttl", uint64(d.Auth.MaxTTL))
		buf.WriteByte(',')
		writeJSONString(&buf, "time")
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatInt(d.Auth.Time, 10))
		writeJSONStringField(&buf, "sig", base64.RawURLEncoding.EncodeToString(d.Auth.Sig))
		buf.WriteByte('}')
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// MarshalDatagramFrameLine serializes d as a full wire line (canonical JSON
// plus the terminating newline) under the command-plane budget.
func MarshalDatagramFrameLine(d DatagramFrame) (string, error) {
	return MarshalDatagramFrameLineWithLimit(d, MaxFrameLine)
}

// MarshalDatagramFrameLineWithLimit is MarshalDatagramFrameLine with an
// explicit budget. The size is measured on the FULL line INCLUDING the
// trailing newline, byte-for-byte with the receive-side line counter: a
// JSON body of exactly maxBytes would be rejected by the remote reader as
// maxBytes+1, so it is rejected here too (§2.3).
func MarshalDatagramFrameLineWithLimit(d DatagramFrame, maxBytes int) (string, error) {
	body, err := MarshalDatagramFrame(d)
	if err != nil {
		return "", err
	}
	if len(body)+1 > maxBytes {
		return "", fmt.Errorf("MarshalDatagramFrameLineWithLimit: frame size %d (with newline %d) exceeds %d: %w",
			len(body), len(body)+1, maxBytes, ErrFrameTooLarge)
	}
	return string(body) + "\n", nil
}

// writeJSONStringField appends `,"key":"value"` to buf.
func writeJSONStringField(buf *bytes.Buffer, key, value string) {
	buf.WriteByte(',')
	writeJSONString(buf, key)
	buf.WriteByte(':')
	writeJSONString(buf, value)
}

// writeJSONUintField appends `,"key":value` to buf.
func writeJSONUintField(buf *bytes.Buffer, key string, value uint64) {
	buf.WriteByte(',')
	writeJSONString(buf, key)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatUint(value, 10))
}

// writeJSONString writes a quoted JSON string. Every string a datagram can
// carry is drawn from an escape-free alphabet — `[a-z0-9_]` names,
// lowercase hex addresses, base64url bodies — all of which Validate has
// already enforced, so the canonical encoder never has to escape and the
// output stays byte-stable across Go versions (encoding/json would, for
// instance, HTML-escape `<`).
func writeJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	buf.WriteString(s)
	buf.WriteByte('"')
}

// decodeDatagramHex decodes a canonical lowercase 40-hex address. Uppercase
// input is rejected rather than folded: two spellings of one address would
// otherwise produce two different frames with the same meaning (§3.4).
func decodeDatagramHex(field, s string) (domain.PeerIdentity, error) {
	id, err := domain.ParsePeerIdentity(s)
	if err != nil || id.IsZero() {
		return domain.PeerIdentity{}, fmt.Errorf("%w: %s %q is not 40 lowercase hex chars", ErrDatagramEncoding, field, s)
	}
	return id, nil
}

// decodeDatagramBase64 decodes CANONICAL base64url: no padding, and no
// non-zero trailing bits in the last character.
//
// Both halves are the same rule of §3.4 — one value, one representation — and
// both are load-bearing for the same reason. A padded value is a second
// spelling. So is a value whose final character carries data bits that decode
// to nothing: `Strict()` is what rejects it, and without it the two lines
// "...Hw" and "...Hx" parse into byte-identical frames while remaining two
// different transcripts on the wire. Two implementations would then disagree
// on which line was signed, and a relay could rewrite the tail of a payload
// without touching the frame anybody verified.
func decodeDatagramBase64(field, s string) ([]byte, error) {
	out, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not canonical unpadded base64url: %v", ErrDatagramEncoding, field, err)
	}
	return out, nil
}
