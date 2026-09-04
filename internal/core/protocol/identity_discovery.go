package protocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// identity_discovery.go implements the payload schemas of the discovery
// datagram types (docs/protocol/identity-lookup.md) and the two proof
// signatures that make an unauthenticated answer trustworthy:
//
//   - requester_sig — the OPTIONAL, initiator-chosen proof of "who is
//     asking", addressed to the target only. Mandatory freshness: the
//     signed payload would otherwise be replayable forever;
//   - target_proof — the proof that the OWNER answered live and to this
//     very attempt. dtype is not authenticated, so the two kinds of answer
//     are told apart by payload cryptography alone.
//
// Parsing follows the record contract: duplicate JSON keys are a reject at
// every level, unknown fields are ignored (schemas grow additively), size
// caps are checked before anything else.

// Discovery payload failures reuse the identity-record sentinels where the
// violation is the same kind; the ones below are specific to the lookup
// payloads.
var (
	// ErrLookupPayloadMalformed covers structural violations of a discovery
	// payload: not an object, duplicate keys, mistyped or missing fields, a
	// requester without its mandatory signature.
	ErrLookupPayloadMalformed = errors.New("identity lookup: malformed payload")

	// ErrLookupPayloadTooLarge marks a payload above its discovery budget.
	ErrLookupPayloadTooLarge = errors.New("identity lookup: payload exceeds cap")

	// ErrLookupVersionUnsupported marks a discovery-payload schema version
	// this build does not understand. The receiver drops silently.
	ErrLookupVersionUnsupported = errors.New("identity lookup: unsupported payload version")

	// ErrLookupProofInvalid marks a target_proof or requester_sig that does
	// not verify.
	ErrLookupProofInvalid = errors.New("identity lookup: invalid proof signature")
)

// ---------------------------------------------------------------------------
// get_identity
// ---------------------------------------------------------------------------

// GetIdentityPayload is the decoded request. Every field except V is
// optional on the wire.
type GetIdentityPayload struct {
	// Required lists requirement names the target MUST understand to
	// answer; not understanding one (or V) obliges the target to stay
	// silent — there is no refusal frame.
	Required []string
	// RequesterSig is the raw Ed25519 signature of the requester triple;
	// present iff Requester is.
	RequesterSig []byte
	// Requester is the OPT-IN identity of who is asking, addressed to the
	// target (UX "you were looked up", pre-contact). Zero when absent.
	Requester domain.PeerIdentity
	// RequesterIssuedAt is the epoch-seconds freshness of the requester
	// triple; the target rejects outside its window.
	RequesterIssuedAt uint64
	// MinSeq is checked by the INITIATOR against the received record — the
	// answerer is not trusted with it.
	MinSeq domain.IdentityRecordSeq
	// V is the discovery-payload schema version.
	V int
	// TargetProof demands a live proof bound to this attempt. Setting it
	// obliges the builder to also list LookupRequirementTargetProof in
	// Required, so an old build cannot silently answer without the proof.
	TargetProof bool
	// Sealed is the encrypted liveness claim (liveness_probe.go): who is
	// asking, in which epoch, and the reciprocity token that proves it.
	// Present ONLY on a presence probe; a public lookup leaves it empty and
	// is answered exactly as before.
	//
	// Its contents are opaque here on purpose. This layer moves the bytes;
	// only the target's box key opens them, and only the target's contact
	// list can judge what is inside.
	Sealed []byte
}

// getIdentityPayloadWire is the emit-side JSON shape.
type getIdentityPayloadWire struct {
	Required          []string `json:"required,omitempty"`
	Requester         string   `json:"requester,omitempty"`
	RequesterSig      string   `json:"requester_sig,omitempty"`
	MinSeq            uint64   `json:"min_seq,omitempty"`
	RequesterIssuedAt uint64   `json:"requester_issued_at,omitempty"`
	V                 int      `json:"v"`
	TargetProof       bool     `json:"target_proof,omitempty"`
	Sealed            []byte   `json:"sealed,omitempty"`
}

// BuildGetIdentityPayload marshals and validates a request payload,
// enforcing the flag/required consistency and the 512-byte budget.
func BuildGetIdentityPayload(payload GetIdentityPayload) ([]byte, error) {
	if payload.TargetProof && !containsName(payload.Required, domain.LookupRequirementTargetProof) {
		payload.Required = append(append([]string(nil), payload.Required...), domain.LookupRequirementTargetProof)
	}
	if !payload.Requester.IsZero() && len(payload.RequesterSig) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: requester without a valid signature", ErrLookupPayloadMalformed)
	}
	wire := getIdentityPayloadWire{
		V:                 payload.V,
		Required:          payload.Required,
		MinSeq:            uint64(payload.MinSeq),
		TargetProof:       payload.TargetProof,
		RequesterIssuedAt: payload.RequesterIssuedAt,
		Sealed:            payload.Sealed,
	}
	if !payload.Requester.IsZero() {
		wire.Requester = payload.Requester.String()
		wire.RequesterSig = base64.RawURLEncoding.EncodeToString(payload.RequesterSig)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal get_identity payload: %w", err)
	}
	if len(raw) > domain.MaxGetIdentityPayloadBytes {
		return nil, fmt.Errorf("%w: request %d bytes exceeds %d",
			ErrLookupPayloadTooLarge, len(raw), domain.MaxGetIdentityPayloadBytes)
	}
	return raw, nil
}

func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

// ParseGetIdentityPayload parses and structurally validates a request
// payload. The requester triple is all-or-nothing: a requester without a
// well-formed signature and issue time drops the WHOLE payload — a bare
// name would be unfalsifiable slander.
func ParseGetIdentityPayload(raw []byte) (GetIdentityPayload, error) {
	if len(raw) > domain.MaxGetIdentityPayloadBytes {
		return GetIdentityPayload{}, fmt.Errorf("%w: request %d bytes exceeds %d",
			ErrLookupPayloadTooLarge, len(raw), domain.MaxGetIdentityPayloadBytes)
	}
	if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); err != nil {
		return GetIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	fields, err := decodeJSONObject("get_identity", raw)
	if err != nil {
		return GetIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}

	payload := GetIdentityPayload{}
	if payload.V, err = lookupIntField(fields, "v"); err != nil {
		return GetIdentityPayload{}, err
	}
	if payload.V != domain.IdentityLookupSchemaVersion {
		return GetIdentityPayload{}, fmt.Errorf("%w: v=%d", ErrLookupVersionUnsupported, payload.V)
	}
	if payload.Required, err = lookupOptionalStringList(fields, "required"); err != nil {
		return GetIdentityPayload{}, err
	}
	minSeq, err := lookupOptionalUint64(fields, "min_seq")
	if err != nil {
		return GetIdentityPayload{}, err
	}
	payload.MinSeq = domain.IdentityRecordSeq(minSeq)
	if payload.TargetProof, err = lookupOptionalBool(fields, "target_proof"); err != nil {
		return GetIdentityPayload{}, err
	}
	if err := parseRequesterTriple(fields, &payload); err != nil {
		return GetIdentityPayload{}, err
	}
	// A sealed claim is optional and never a reason to refuse the PARSE: an
	// old build ignores the field entirely, and refusing it here would turn a
	// privacy feature into a compatibility break.
	//
	// What happens to a claim that cannot be opened or does not verify is NOT
	// decided here, and it is the opposite of lenient: the handler answers
	// with SILENCE (node/identity_discovery.go, acceptLivenessClaim). The
	// contract is fail-CLOSED, deliberately — a refusal frame would confirm
	// the identity exists, which is the oracle the gate is there to shut.
	// An earlier version of this comment described a fall-through to the
	// public lookup; that behaviour does not exist and must not be
	// reintroduced from here.
	if payload.Sealed, err = lookupOptionalBytes(fields, "sealed"); err != nil {
		return GetIdentityPayload{}, err
	}
	return payload, nil
}

// lookupOptionalBytes reads a base64 JSON string field into raw bytes.
func lookupOptionalBytes(fields map[string]json.RawMessage, name string) ([]byte, error) {
	raw, present := fields[name]
	if !present {
		return nil, nil
	}
	var out []byte
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrLookupPayloadMalformed, name, err)
	}
	return out, nil
}

// parseRequesterTriple reads the optional authenticated "who is asking"
// triple. Presence of any of its three fields makes all three mandatory.
func parseRequesterTriple(fields map[string]json.RawMessage, payload *GetIdentityPayload) error {
	_, hasRequester := fields["requester"]
	_, hasIssuedAt := fields["requester_issued_at"]
	_, hasSig := fields["requester_sig"]
	if !hasRequester && !hasIssuedAt && !hasSig {
		return nil
	}
	if !hasRequester || !hasIssuedAt || !hasSig {
		return fmt.Errorf("%w: partial requester triple", ErrLookupPayloadMalformed)
	}

	requester, err := recordAddressField(fields, "requester")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	issuedAt, err := lookupOptionalUint64(fields, "requester_issued_at")
	if err != nil {
		return err
	}
	sig, err := recordBinaryField(fields, "requester_sig")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	if len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("%w: requester_sig is %d bytes, want %d",
			ErrLookupPayloadMalformed, len(sig), ed25519.SignatureSize)
	}
	payload.Requester = requester
	payload.RequesterIssuedAt = issuedAt
	payload.RequesterSig = sig
	return nil
}

// UnderstoodRequirements reports whether every name in `required` is one
// this build implements. A single unknown name obliges the endpoint to
// stay silent: forwarding is impossible (there is no more capable version
// of itself in the network) and answering past the requirement would fake
// compliance.
func (p GetIdentityPayload) UnderstoodRequirements() bool {
	for _, name := range p.Required {
		if name != domain.LookupRequirementTargetProof {
			return false
		}
	}
	return true
}

// RequiresTargetProof reports whether the answer must carry the live
// proof — demanded by the flag or by the requirement list.
func (p GetIdentityPayload) RequiresTargetProof() bool {
	return p.TargetProof || containsName(p.Required, domain.LookupRequirementTargetProof)
}

// ---------------------------------------------------------------------------
// requester_sig
// ---------------------------------------------------------------------------

// lookupRequesterSigningTag is the domain tag of the requester triple. Same
// framing as the record tag, different domain.
const lookupRequesterSigningTag = "corsa-lookup-requester-v1"

// LookupRequesterTranscript is the exact byte string the requester signs.
// Addresses are signed as their 20 raw bytes, never as 40 ASCII characters;
// the issue time is uint64be. attemptID is the one-shot src label of THIS
// attempt, which is what makes an intercepted signature worthless for
// replay beyond the label's reverse-state window.
func LookupRequesterTranscript(network domain.NetworkID, attemptID domain.PeerIdentity, issuedAt uint64, requester, dst domain.PeerIdentity) []byte {
	out := identityLookupSigningDomain(lookupRequesterSigningTag, network)
	out = append(out, attemptID[:]...)
	out = binary.BigEndian.AppendUint64(out, issuedAt)
	out = append(out, requester[:]...)
	out = append(out, dst[:]...)
	return out
}

// SignLookupRequester produces the requester_sig for one attempt.
func SignLookupRequester(owner *identity.Identity, network domain.NetworkID, attemptID domain.PeerIdentity, issuedAt uint64, requester, dst domain.PeerIdentity) []byte {
	return ed25519.Sign(owner.PrivateKey, LookupRequesterTranscript(network, attemptID, issuedAt, requester, dst))
}

// VerifyLookupRequester checks the requester triple against the requester's
// public key (which the TARGET resolves from its own knowledge — the triple
// itself carries no key material). Freshness is the caller's check: the
// signature proves who and about which attempt, the window proves when.
func VerifyLookupRequester(requesterPubKeyBase64 string, network domain.NetworkID, attemptID domain.PeerIdentity, payload GetIdentityPayload, dst domain.PeerIdentity) error {
	pubKey, err := base64.StdEncoding.DecodeString(requesterPubKeyBase64)
	if err != nil || len(pubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: requester public key undecodable", ErrLookupProofInvalid)
	}
	transcript := LookupRequesterTranscript(network, attemptID, payload.RequesterIssuedAt, payload.Requester, dst)
	if !ed25519.Verify(ed25519.PublicKey(pubKey), transcript, payload.RequesterSig) {
		return ErrLookupProofInvalid
	}
	return nil
}

// ---------------------------------------------------------------------------
// post_identity
// ---------------------------------------------------------------------------

// PostIdentityPayload is the decoded answer: the owner's signed record,
// plus the live proof when the request demanded one.
type PostIdentityPayload struct {
	Record      SignedIdentityRecord
	TargetProof []byte
	V           int
}

// postIdentityPayloadWire is the emit-side JSON shape. The record embeds as
// a plain JSON object — its signature covers the decoded body bytes, so no
// double-base64 is needed.
type postIdentityPayloadWire struct {
	TargetProof string               `json:"target_proof,omitempty"`
	Record      SignedIdentityRecord `json:"record"`
	V           int                  `json:"v"`
}

// BuildPostIdentityPayload marshals and validates an answer payload against
// the 3.2 KiB discovery budget.
func BuildPostIdentityPayload(payload PostIdentityPayload) ([]byte, error) {
	wire := postIdentityPayloadWire{V: payload.V, Record: payload.Record}
	if len(payload.TargetProof) > 0 {
		if len(payload.TargetProof) != ed25519.SignatureSize {
			return nil, fmt.Errorf("%w: target_proof is %d bytes, want %d",
				ErrLookupPayloadMalformed, len(payload.TargetProof), ed25519.SignatureSize)
		}
		wire.TargetProof = base64.RawURLEncoding.EncodeToString(payload.TargetProof)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal post_identity payload: %w", err)
	}
	if len(raw) > domain.MaxPostIdentityPayloadBytes {
		return nil, fmt.Errorf("%w: answer %d bytes exceeds %d",
			ErrLookupPayloadTooLarge, len(raw), domain.MaxPostIdentityPayloadBytes)
	}
	return raw, nil
}

// ParsePostIdentityPayload parses and structurally validates an answer.
// The record is mandatory; the form is closed — extension is additive
// fields only.
func ParsePostIdentityPayload(raw []byte) (PostIdentityPayload, error) {
	if len(raw) > domain.MaxPostIdentityPayloadBytes {
		return PostIdentityPayload{}, fmt.Errorf("%w: answer %d bytes exceeds %d",
			ErrLookupPayloadTooLarge, len(raw), domain.MaxPostIdentityPayloadBytes)
	}
	if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); err != nil {
		return PostIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	fields, err := decodeJSONObject("post_identity", raw)
	if err != nil {
		return PostIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}

	payload := PostIdentityPayload{}
	if payload.V, err = lookupIntField(fields, "v"); err != nil {
		return PostIdentityPayload{}, err
	}
	if payload.V != domain.IdentityLookupSchemaVersion {
		return PostIdentityPayload{}, fmt.Errorf("%w: v=%d", ErrLookupVersionUnsupported, payload.V)
	}
	recordRaw, ok := fields["record"]
	if !ok {
		return PostIdentityPayload{}, fmt.Errorf("%w: missing record", ErrLookupPayloadMalformed)
	}
	if payload.Record, err = ParseSignedIdentityRecord(recordRaw); err != nil {
		return PostIdentityPayload{}, err
	}
	if proofRaw, ok := fields["target_proof"]; ok {
		proofB64, err := wireString("target_proof", proofRaw)
		if err != nil {
			return PostIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
		}
		proof, err := base64.RawURLEncoding.DecodeString(proofB64)
		if err != nil || len(proof) != ed25519.SignatureSize {
			return PostIdentityPayload{}, fmt.Errorf("%w: target_proof undecodable", ErrLookupPayloadMalformed)
		}
		payload.TargetProof = proof
	}
	return payload, nil
}

// ---------------------------------------------------------------------------
// target_proof
// ---------------------------------------------------------------------------

// targetProofSigningTag is the domain tag of the live-answer proof.
const targetProofSigningTag = "corsa-target-proof-v1"

// TargetProofTranscript is the exact byte string the target signs: the
// attempt label plus the SHA-256 of the request payload bytes plus the
// SHA-256 of the record (body || sig). Binding to the attempt excludes
// replay; binding to the request hash excludes answering a different
// question; binding to the record excludes swapping the answer under a
// valid proof.
func TargetProofTranscript(network domain.NetworkID, attemptID domain.PeerIdentity, requestPayloadHash [sha256.Size]byte, record SignedIdentityRecord) []byte {
	recordHash := sha256.Sum256(append(append([]byte(nil), record.Body...), record.Sig...))
	out := identityLookupSigningDomain(targetProofSigningTag, network)
	out = append(out, attemptID[:]...)
	out = append(out, requestPayloadHash[:]...)
	out = append(out, recordHash[:]...)
	return out
}

// SignTargetProof produces the proof for one answered attempt.
func SignTargetProof(owner *identity.Identity, network domain.NetworkID, attemptID domain.PeerIdentity, requestPayload []byte, record SignedIdentityRecord) []byte {
	return ed25519.Sign(owner.PrivateKey,
		TargetProofTranscript(network, attemptID, sha256.Sum256(requestPayload), record))
}

// VerifyTargetProof checks the proof with the pubkey from the (already
// verified) record body, against the request-payload hash the INITIATOR
// stored for this very attempt — the request may have changed between
// attempts, so the hash comes from the attempt entry, never recomputed
// from current state.
func VerifyTargetProof(proof []byte, body IdentityRecordBody, network domain.NetworkID, attemptID domain.PeerIdentity, requestPayloadHash [sha256.Size]byte, record SignedIdentityRecord) error {
	pubKey, err := base64.StdEncoding.DecodeString(string(body.PubKey))
	if err != nil || len(pubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: record pubkey undecodable", ErrLookupProofInvalid)
	}
	transcript := TargetProofTranscript(network, attemptID, requestPayloadHash, record)
	if !ed25519.Verify(ed25519.PublicKey(pubKey), transcript, proof) {
		return ErrLookupProofInvalid
	}
	return nil
}

// ---------------------------------------------------------------------------
// push_identity
// ---------------------------------------------------------------------------

// PushIdentityPayload carries the sender's own record to a session peer.
type PushIdentityPayload struct {
	Record SignedIdentityRecord
	V      int
}

type pushIdentityPayloadWire struct {
	Record SignedIdentityRecord `json:"record"`
	V      int                  `json:"v"`
}

// BuildPushIdentityPayload marshals a push payload. The control-class cap
// of the datagram layer bounds it on the wire; the record's own caps bound
// it here.
func BuildPushIdentityPayload(payload PushIdentityPayload) ([]byte, error) {
	raw, err := json.Marshal(pushIdentityPayloadWire(payload))
	if err != nil {
		return nil, fmt.Errorf("marshal push_identity payload: %w", err)
	}
	if len(raw) > domain.DatagramControlPayloadCap {
		return nil, fmt.Errorf("%w: push %d bytes exceeds %d",
			ErrLookupPayloadTooLarge, len(raw), domain.DatagramControlPayloadCap)
	}
	return raw, nil
}

// ParsePushIdentityPayload parses and structurally validates a push.
func ParsePushIdentityPayload(raw []byte) (PushIdentityPayload, error) {
	if len(raw) > domain.DatagramControlPayloadCap {
		return PushIdentityPayload{}, fmt.Errorf("%w: push %d bytes exceeds %d",
			ErrLookupPayloadTooLarge, len(raw), domain.DatagramControlPayloadCap)
	}
	if err := scanDuplicateJSONKeys(raw, maxDatagramJSONDepth); err != nil {
		return PushIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	fields, err := decodeJSONObject("push_identity", raw)
	if err != nil {
		return PushIdentityPayload{}, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}

	payload := PushIdentityPayload{}
	if payload.V, err = lookupIntField(fields, "v"); err != nil {
		return PushIdentityPayload{}, err
	}
	if payload.V != domain.IdentityLookupSchemaVersion {
		return PushIdentityPayload{}, fmt.Errorf("%w: v=%d", ErrLookupVersionUnsupported, payload.V)
	}
	recordRaw, ok := fields["record"]
	if !ok {
		return PushIdentityPayload{}, fmt.Errorf("%w: missing record", ErrLookupPayloadMalformed)
	}
	if payload.Record, err = ParseSignedIdentityRecord(recordRaw); err != nil {
		return PushIdentityPayload{}, err
	}
	return payload, nil
}

// ---------------------------------------------------------------------------
// Field readers
// ---------------------------------------------------------------------------

func lookupIntField(fields map[string]json.RawMessage, name string) (int, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("%w: missing %s", ErrLookupPayloadMalformed, name)
	}
	number, err := wireInt64(name, raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	return int(number), nil
}

func lookupOptionalUint64(fields map[string]json.RawMessage, name string) (uint64, error) {
	if _, ok := fields[name]; !ok {
		return 0, nil
	}
	value, err := recordUint64Field(fields, name)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	return value, nil
}

func lookupOptionalBool(fields map[string]json.RawMessage, name string) (bool, error) {
	if _, ok := fields[name]; !ok {
		return false, nil
	}
	value, err := recordBoolField(fields, name)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrLookupPayloadMalformed, err)
	}
	return value, nil
}

func lookupOptionalStringList(fields map[string]json.RawMessage, name string) ([]string, error) {
	raw, ok := fields[name]
	if !ok {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return nil, fmt.Errorf("%w: %s must be an array of strings: %v", ErrLookupPayloadMalformed, name, err)
	}
	return names, nil
}
