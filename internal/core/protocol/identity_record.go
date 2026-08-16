package protocol

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// identity_record.go implements the signed identity record of
// docs/protocol/identity-lookup.md: the {v, body, sig} envelope that lives on
// the wire (post_identity / push_identity payloads), on disk (trust store)
// and inside a corsa: link.
//
// The signature covers the raw body BYTES, never a re-serialisation: nodes
// store and forward the body verbatim without understanding new fields, and
// the signature stays valid — that is the whole extension mechanism, so
// nothing in this file may ever re-marshal a received body.

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Failures are distinguishable sentinels; a transport mapping must branch on
// the type, never on the text.
var (
	// ErrIdentityRecordMalformed covers structural violations: not a JSON
	// object, duplicate keys, missing or mistyped required fields, broken
	// base64.
	ErrIdentityRecordMalformed = errors.New("identity record: malformed")

	// ErrIdentityRecordTooLarge marks a breach of the size caps — checked
	// before any other validation, cryptography included.
	ErrIdentityRecordTooLarge = errors.New("identity record: size cap exceeded")

	// ErrIdentityRecordVersionUnsupported marks a record.v this build does
	// not understand. The caller drops the record without ban: an unknown
	// incompatible version is a future build, not an attack.
	ErrIdentityRecordVersionUnsupported = errors.New("identity record: unsupported version")

	// ErrIdentityRecordSignature marks a signature that does not verify
	// against the pubkey inside the body.
	ErrIdentityRecordSignature = errors.New("identity record: invalid signature")

	// ErrIdentityRecordKeyMismatch marks key material that does not hold
	// together: the address is not the fingerprint of pubkey, or the box-key
	// binding does not verify.
	ErrIdentityRecordKeyMismatch = errors.New("identity record: key material mismatch")

	// ErrIdentityRecordBoxFields marks a violation of the dm branch: box
	// fields present with dm=false, or absent with dm=true.
	ErrIdentityRecordBoxFields = errors.New("identity record: box fields contradict dm flag")

	// ErrIdentityRecordAddressMismatch marks a record whose address is not
	// the identity the caller expected (the dst of the lookup, the session
	// identity of a push, the address inside a corsa: link).
	ErrIdentityRecordAddressMismatch = errors.New("identity record: address does not match expected identity")
)

// ---------------------------------------------------------------------------
// Domain separation
// ---------------------------------------------------------------------------

// identityRecordSigningTag is the domain-separation tag of the record
// signature. The target_proof and requester_sig tags of the discovery
// payloads are SEPARATE tags with the same framing.
const identityRecordSigningTag = "corsa-identity-record-v1"

// identityLookupSigningDomain frames a domain-separation prefix as
// tag || 0x00 || uint16be(len(network)) || network. The length prefix makes
// the framing unambiguous: no network id can collide with another network's
// tag continuation. Shared by every signature of the identity-lookup
// protocol family.
func identityLookupSigningDomain(tag string, network domain.NetworkID) []byte {
	networkBytes := []byte(network.String())
	out := make([]byte, 0, len(tag)+1+2+len(networkBytes))
	out = append(out, tag...)
	out = append(out, 0x00)
	out = binary.BigEndian.AppendUint16(out, uint16(len(networkBytes)))
	out = append(out, networkBytes...)
	return out
}

// identityRecordSignedBytes is the exact byte string the owner signs:
// DOMAIN || body_bytes.
func identityRecordSignedBytes(network domain.NetworkID, body []byte) []byte {
	prefix := identityLookupSigningDomain(identityRecordSigningTag, network)
	return append(prefix, body...)
}

// ---------------------------------------------------------------------------
// SignedIdentityRecord — the {v, body, sig} envelope
// ---------------------------------------------------------------------------

// SignedIdentityRecord is the owner-signed record envelope. Body holds the
// signed bytes verbatim; Sig is the Ed25519 signature over
// DOMAIN || Body by the key whose fingerprint is the address inside Body.
type SignedIdentityRecord struct {
	Body    []byte
	Sig     []byte
	Version int
}

// signedIdentityRecordWire is the JSON shape of the envelope.
type signedIdentityRecordWire struct {
	Body string `json:"body"`
	Sig  string `json:"sig"`
	V    int    `json:"v"`
}

// MarshalJSON renders the {v, body, sig} object with base64url raw encoding.
func (r SignedIdentityRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(signedIdentityRecordWire{
		V:    r.Version,
		Body: base64.RawURLEncoding.EncodeToString(r.Body),
		Sig:  base64.RawURLEncoding.EncodeToString(r.Sig),
	})
}

// UnmarshalJSON parses and structurally validates the envelope; see
// ParseSignedIdentityRecord.
func (r *SignedIdentityRecord) UnmarshalJSON(raw []byte) error {
	parsed, err := ParseSignedIdentityRecord(raw)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// ParseSignedIdentityRecord parses the {v, body, sig} envelope from raw JSON
// bytes. Size caps are checked before everything else; duplicate keys are a
// reject; unknown fields are ignored (the envelope grows additively).
//
// Only structure is validated here — signature, fingerprint and box binding
// belong to Verify, so a store can hold a parsed record without re-running
// cryptography it has already paid for.
func ParseSignedIdentityRecord(raw []byte) (SignedIdentityRecord, error) {
	if len(raw) > domain.MaxIdentityRecordBytes {
		return SignedIdentityRecord{}, fmt.Errorf("%w: record object %d bytes exceeds %d",
			ErrIdentityRecordTooLarge, len(raw), domain.MaxIdentityRecordBytes)
	}
	if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); err != nil {
		return SignedIdentityRecord{}, fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}
	fields, err := decodeJSONObject("identity_record", raw)
	if err != nil {
		return SignedIdentityRecord{}, fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}
	version, err := recordIntField(fields, "v")
	if err != nil {
		return SignedIdentityRecord{}, err
	}
	if version != domain.IdentityRecordVersion {
		return SignedIdentityRecord{}, fmt.Errorf("%w: v=%d", ErrIdentityRecordVersionUnsupported, version)
	}
	body, err := recordBinaryField(fields, "body")
	if err != nil {
		return SignedIdentityRecord{}, err
	}
	if len(body) > domain.MaxIdentityRecordBodyBytes {
		return SignedIdentityRecord{}, fmt.Errorf("%w: body %d bytes exceeds %d",
			ErrIdentityRecordTooLarge, len(body), domain.MaxIdentityRecordBodyBytes)
	}
	sig, err := recordBinaryField(fields, "sig")
	if err != nil {
		return SignedIdentityRecord{}, err
	}
	if len(sig) != ed25519.SignatureSize {
		return SignedIdentityRecord{}, fmt.Errorf("%w: sig is %d bytes, want %d",
			ErrIdentityRecordMalformed, len(sig), ed25519.SignatureSize)
	}
	return SignedIdentityRecord{Version: version, Body: body, Sig: sig}, nil
}

// ---------------------------------------------------------------------------
// IdentityRecordBody — the parsed view of the signed bytes
// ---------------------------------------------------------------------------

// IdentityRecordBody is the parsed projection of the signed body bytes. It
// never travels anywhere by itself — the signed bytes do — so it carries no
// marshalling of its own.
type IdentityRecordBody struct {
	PubKey   domain.PeerPublicKey
	BoxKey   domain.PeerBoxKey
	BoxSig   domain.PeerBoxSignature
	DTypes   domain.DeclaredDTypeSet
	Address  domain.PeerIdentity
	IssuedAt uint64
	Seq      domain.IdentityRecordSeq
	DM       bool
}

// identityRecordBodyWire is the JSON shape the OWNER emits when issuing a
// record. Receivers never use it: they parse field-by-field and keep the
// signed bytes verbatim.
type identityRecordBodyWire struct {
	DTypes   *[]string `json:"dtypes,omitempty"`
	Address  string    `json:"address"`
	PubKey   string    `json:"pubkey"`
	BoxKey   string    `json:"boxkey,omitempty"`
	BoxSig   string    `json:"boxsig,omitempty"`
	IssuedAt uint64    `json:"issued_at"`
	Seq      uint64    `json:"seq"`
	DM       bool      `json:"dm"`
}

// ParseIdentityRecordBody parses the signed body bytes structurally: size
// cap first, duplicate keys reject, required fields present and well-typed,
// box fields consistent with the dm flag, dtypes under the record bounds
// (a bounds breach drops the field to absent, not the record). Unknown
// fields are ignored — the caller keeps the raw bytes, so nothing is lost.
func ParseIdentityRecordBody(body []byte) (IdentityRecordBody, error) {
	if len(body) > domain.MaxIdentityRecordBodyBytes {
		return IdentityRecordBody{}, fmt.Errorf("%w: body %d bytes exceeds %d",
			ErrIdentityRecordTooLarge, len(body), domain.MaxIdentityRecordBodyBytes)
	}
	if err := scanDuplicateJSONKeys(body, maxDatagramJSONDepth); err != nil {
		return IdentityRecordBody{}, fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}
	fields, err := decodeJSONObject("identity_record_body", body)
	if err != nil {
		return IdentityRecordBody{}, fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}

	address, err := recordAddressField(fields, "address")
	if err != nil {
		return IdentityRecordBody{}, err
	}
	pubKey, err := recordStringField(fields, "pubkey")
	if err != nil {
		return IdentityRecordBody{}, err
	}
	dm, err := recordBoolField(fields, "dm")
	if err != nil {
		return IdentityRecordBody{}, err
	}
	issuedAt, err := recordUint64Field(fields, "issued_at")
	if err != nil {
		return IdentityRecordBody{}, err
	}
	seq, err := recordUint64Field(fields, "seq")
	if err != nil {
		return IdentityRecordBody{}, err
	}
	boxKey, boxSig, err := recordBoxFields(fields, dm)
	if err != nil {
		return IdentityRecordBody{}, err
	}
	dtypes, err := recordDTypesField(fields)
	if err != nil {
		return IdentityRecordBody{}, err
	}

	return IdentityRecordBody{
		Address:  address,
		PubKey:   domain.PeerPublicKey(pubKey),
		BoxKey:   domain.PeerBoxKey(boxKey),
		BoxSig:   domain.PeerBoxSignature(boxSig),
		DTypes:   dtypes,
		IssuedAt: issuedAt,
		Seq:      domain.IdentityRecordSeq(seq),
		DM:       dm,
	}, nil
}

// recordBoxFields applies the dm branch: with dm=true both box fields are
// mandatory; with dm=false their very PRESENCE invalidates the record — an
// opt-out record carrying keys is two contradictory statements at once.
func recordBoxFields(fields map[string]json.RawMessage, dm bool) (boxKey, boxSig string, err error) {
	_, boxKeyPresent := fields["boxkey"]
	_, boxSigPresent := fields["boxsig"]
	if !dm {
		if boxKeyPresent || boxSigPresent {
			return "", "", fmt.Errorf("%w: box fields present with dm=false", ErrIdentityRecordBoxFields)
		}
		return "", "", nil
	}
	if !boxKeyPresent || !boxSigPresent {
		return "", "", fmt.Errorf("%w: box fields missing with dm=true", ErrIdentityRecordBoxFields)
	}
	if boxKey, err = recordStringField(fields, "boxkey"); err != nil {
		return "", "", err
	}
	if boxSig, err = recordStringField(fields, "boxsig"); err != nil {
		return "", "", err
	}
	return boxKey, boxSig, nil
}

// recordDTypesField reads the optional dtypes list. A missing field is the
// absent declaration; a present field must be an array of strings (a type
// violation is malformation, not a bounds breach); the record-specific
// bounds are then applied by the domain parser, which degrades a breach to
// the absent declaration.
func recordDTypesField(fields map[string]json.RawMessage) (domain.DeclaredDTypeSet, error) {
	raw, present := fields["dtypes"]
	if !present {
		return domain.AbsentDTypes(), nil
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return domain.DeclaredDTypeSet{}, fmt.Errorf("%w: dtypes must be an array of strings: %v",
			ErrIdentityRecordMalformed, err)
	}
	return domain.ParseIdentityRecordDTypesField(&names), nil
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

// VerifyIdentityRecord runs the full §4.1 order on a parsed envelope:
// caps → signature by the pubkey inside the body → address is the
// fingerprint of pubkey → dm branch (box binding) → address equals the
// expected identity. It returns the parsed body so callers never verify and
// parse in two diverging steps.
//
// expected is the identity the caller is entitled to expect here: the dst of
// the lookup, the authenticated session identity of a push, the address from
// a corsa: link. It is mandatory — a record verified "against nobody" would
// let any valid record occupy any slot.
func VerifyIdentityRecord(record SignedIdentityRecord, network domain.NetworkID, expected domain.PeerIdentity) (IdentityRecordBody, error) {
	if expected.IsZero() {
		return IdentityRecordBody{}, fmt.Errorf("%w: expected identity is unset", ErrIdentityRecordAddressMismatch)
	}
	if record.Version != domain.IdentityRecordVersion {
		return IdentityRecordBody{}, fmt.Errorf("%w: v=%d", ErrIdentityRecordVersionUnsupported, record.Version)
	}
	if len(record.Body) > domain.MaxIdentityRecordBodyBytes {
		return IdentityRecordBody{}, fmt.Errorf("%w: body %d bytes exceeds %d",
			ErrIdentityRecordTooLarge, len(record.Body), domain.MaxIdentityRecordBodyBytes)
	}
	body, err := ParseIdentityRecordBody(record.Body)
	if err != nil {
		return IdentityRecordBody{}, err
	}

	pubKeyBytes, err := base64.StdEncoding.DecodeString(string(body.PubKey))
	if err != nil {
		return IdentityRecordBody{}, fmt.Errorf("%w: decode pubkey: %v", ErrIdentityRecordMalformed, err)
	}
	if len(pubKeyBytes) != ed25519.PublicKeySize {
		return IdentityRecordBody{}, fmt.Errorf("%w: pubkey is %d bytes, want %d",
			ErrIdentityRecordMalformed, len(pubKeyBytes), ed25519.PublicKeySize)
	}
	if !ed25519.Verify(ed25519.PublicKey(pubKeyBytes), identityRecordSignedBytes(network, record.Body), record.Sig) {
		return IdentityRecordBody{}, ErrIdentityRecordSignature
	}
	if err := identity.VerifyPublicKeyFingerprint(body.Address.String(), string(body.PubKey)); err != nil {
		return IdentityRecordBody{}, fmt.Errorf("%w: %v", ErrIdentityRecordKeyMismatch, err)
	}
	if body.DM {
		if err := identity.VerifyBoxKeyBinding(body.Address.String(), string(body.PubKey),
			string(body.BoxKey), string(body.BoxSig)); err != nil {
			return IdentityRecordBody{}, fmt.Errorf("%w: %v", ErrIdentityRecordKeyMismatch, err)
		}
	}
	if body.Address != expected {
		return IdentityRecordBody{}, fmt.Errorf("%w: record is for %s", ErrIdentityRecordAddressMismatch, body.Address)
	}
	return body, nil
}

// ---------------------------------------------------------------------------
// Issuing
// ---------------------------------------------------------------------------

// IdentityRecordSpec is what the owner states when issuing a record; keys
// and address come from the identity itself.
type IdentityRecordSpec struct {
	// DTypes is the declared handler set (§6.1 mechanism 2). Absent when the
	// datagram plane is disabled; explicitly empty when the plane is up with
	// no handlers.
	DTypes domain.DeclaredDTypeSet
	// Network binds the signature to one protocol network.
	Network domain.NetworkID
	// IssuedAt is the issue time in epoch seconds. Informational: merge
	// ignores it, only seq decides.
	IssuedAt uint64
	// Seq is the monotonic issue counter. The caller owns its persistence
	// and atomicity (reserve → persist → publish).
	Seq domain.IdentityRecordSeq
	// DM declares whether the owner accepts direct messages. False issues a
	// keyless record with no box fields.
	DM bool
}

// BuildSignedIdentityRecord issues the owner's own record: marshals the
// body, signs it and validates the result against the same caps every
// receiver enforces, so an oversized dtypes set fails at issue time on the
// owner's machine rather than as a silent drop across the network.
func BuildSignedIdentityRecord(owner *identity.Identity, spec IdentityRecordSpec) (SignedIdentityRecord, error) {
	if owner == nil {
		return SignedIdentityRecord{}, fmt.Errorf("%w: nil owner identity", ErrIdentityRecordMalformed)
	}
	if spec.Seq == 0 {
		return SignedIdentityRecord{}, fmt.Errorf("%w: seq 0 is reserved for \"no record\"", ErrIdentityRecordMalformed)
	}
	wire := identityRecordBodyWire{
		Address:  owner.Address,
		PubKey:   identity.PublicKeyBase64(owner.PublicKey),
		DM:       spec.DM,
		DTypes:   spec.DTypes.WireField(),
		IssuedAt: spec.IssuedAt,
		Seq:      uint64(spec.Seq),
	}
	if spec.DM {
		wire.BoxKey = identity.BoxPublicKeyBase64(owner.BoxPublicKey)
		wire.BoxSig = identity.SignBoxKeyBinding(owner)
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return SignedIdentityRecord{}, fmt.Errorf("marshal identity record body: %w", err)
	}
	if len(body) > domain.MaxIdentityRecordBodyBytes {
		return SignedIdentityRecord{}, fmt.Errorf("%w: issued body %d bytes exceeds %d",
			ErrIdentityRecordTooLarge, len(body), domain.MaxIdentityRecordBodyBytes)
	}
	record := SignedIdentityRecord{
		Version: domain.IdentityRecordVersion,
		Body:    body,
		Sig:     ed25519.Sign(owner.PrivateKey, identityRecordSignedBytes(spec.Network, body)),
	}
	encoded, err := record.MarshalJSON()
	if err != nil {
		return SignedIdentityRecord{}, fmt.Errorf("marshal identity record: %w", err)
	}
	if len(encoded) > domain.MaxIdentityRecordBytes {
		return SignedIdentityRecord{}, fmt.Errorf("%w: issued record %d bytes exceeds %d",
			ErrIdentityRecordTooLarge, len(encoded), domain.MaxIdentityRecordBytes)
	}
	return record, nil
}

// ---------------------------------------------------------------------------
// Field readers
// ---------------------------------------------------------------------------

// The readers wrap the shared strict-JSON helpers into identity-record
// sentinels, so errors.Is answers "is this a broken record" without leaking
// datagram sentinels into a protocol that also lives on disk and in links.

func recordStringField(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("%w: missing %s", ErrIdentityRecordMalformed, name)
	}
	value, err := wireString(name, raw)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}
	return value, nil
}

func recordBinaryField(fields map[string]json.RawMessage, name string) ([]byte, error) {
	value, err := recordStringField(fields, name)
	if err != nil {
		return nil, err
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not base64url: %v", ErrIdentityRecordMalformed, name, err)
	}
	return decoded, nil
}

func recordAddressField(fields map[string]json.RawMessage, name string) (domain.PeerIdentity, error) {
	value, err := recordStringField(fields, name)
	if err != nil {
		return domain.PeerIdentity{}, err
	}
	address, err := domain.ParsePeerIdentity(value)
	if err != nil || address.IsZero() {
		return domain.PeerIdentity{}, fmt.Errorf("%w: %s is not a 40-hex identity", ErrIdentityRecordMalformed, name)
	}
	return address, nil
}

func recordBoolField(fields map[string]json.RawMessage, name string) (bool, error) {
	raw, ok := fields[name]
	if !ok {
		return false, fmt.Errorf("%w: missing %s", ErrIdentityRecordMalformed, name)
	}
	token := string(raw)
	switch token {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%w: %s must be a JSON boolean, got %q", ErrIdentityRecordMalformed, name, token)
	}
}

func recordUint64Field(fields map[string]json.RawMessage, name string) (uint64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("%w: missing %s", ErrIdentityRecordMalformed, name)
	}
	token := string(raw)
	if err := requireIntegerLiteral(name, token); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}
	if len(token) > 0 && token[0] == '-' {
		return 0, fmt.Errorf("%w: %s must be non-negative", ErrIdentityRecordMalformed, name)
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("%w: %s: %v", ErrIdentityRecordMalformed, name, err)
	}
	return value, nil
}

func recordIntField(fields map[string]json.RawMessage, name string) (int, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("%w: missing %s", ErrIdentityRecordMalformed, name)
	}
	number, err := wireInt64(name, raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrIdentityRecordMalformed, err)
	}
	return int(number), nil
}
