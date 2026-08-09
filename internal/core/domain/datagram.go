package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// datagram.go holds the vocabulary of the datagram transport layer: the
// header enumerations that transit nodes act on, the name syntaxes shared
// by dtype / capability lists, and the wire-normative constants (payload
// caps, queue residence, replay window). Everything here is normative for
// the wire — a mismatch between two implementations shows up as a frame
// accepted by one node and dropped by its neighbour, so these values are
// NOT local tuning knobs.
//
// Reference: docs/refactoring/datagram-transport.md §2, §2.1, §2.2, §2.3,
// §3.3, §4.2, §4.4.1.

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Parse failures are distinguishable sentinels: every parser in this file
// returns a wrapped sentinel instead of a zero value, because the zero
// value of an enumeration is a legal-looking Go value and would otherwise
// travel silently into routing decisions.
var (
	ErrInvalidDatagramMode    = errors.New("invalid datagram mode")
	ErrInvalidDatagramClass   = errors.New("invalid datagram class")
	ErrInvalidDatagramVersion = errors.New("invalid datagram header version")
	ErrInvalidDType           = errors.New("invalid datagram dtype")
	ErrInvalidRoutePolicy     = errors.New("invalid datagram route policy")
	ErrInvalidAuthVersion     = errors.New("invalid datagram auth version")
	ErrInvalidCapabilityName  = errors.New("invalid capability name")
	ErrInvalidReplayKey       = errors.New("invalid replay key")
	ErrInvalidNetworkID       = errors.New("invalid network id")
)

// ---------------------------------------------------------------------------
// Wire constants
// ---------------------------------------------------------------------------

const (
	// MaxDTypeLen bounds a dtype name (§6.1: `[a-z0-9_]`, ≤ 64 chars).
	MaxDTypeLen = 64

	// MaxDTypesPerNode bounds the advertised dtypes set of one peer (§6.1).
	MaxDTypesPerNode = 64

	// MaxCapabilityNameLen bounds a capability name (§2.2: `[a-z0-9_]`,
	// ≤ 40 chars).
	MaxCapabilityNameLen = 40

	// MaxRawCapabilityNames bounds the raw advertised capability set that a
	// session keeps alongside the typed set. Crossing the bound empties the
	// WHOLE raw set rather than dropping one name: in mixed implementations
	// "drop one" and "drop the set" behave differently.
	MaxRawCapabilityNames = 64

	// DatagramPubKeyBytes / DatagramSaltBytes / DatagramSigBytes fix the
	// binary shape of the auth block (§3.1). The public key is Ed25519 and
	// its size is part of the header version: another algorithm would need
	// another `v`, not another `av` (§2.2).
	DatagramPubKeyBytes = 32
	DatagramSaltBytes   = 16
	DatagramSigBytes    = 64

	// DatagramControlPayloadCap and DatagramBulkPayloadCap are the DECODED
	// payload ceilings per class (§2.3). Budgets and queues are accounted
	// on the serialized frame size instead — see §5.
	DatagramControlPayloadCap = 4 * 1024
	DatagramBulkPayloadCap    = 64 * 1024

	// DatagramDefaultMaxHops is the hop budget of a locally originated
	// datagram and the clamp applied to any received ttl (§4.1.1). Equal to
	// today's file-command default so the migration keeps byte parity.
	DatagramDefaultMaxHops uint8 = 10
)

// DatagramHeaderVersion is the only header version this build understands.
// The frame type name never changes; the version lives here alone, so an
// older node recognises the command, reads `v` and drops silently instead
// of tearing the connection down (§2).
//
// v2 dropped `req_caps` and `ext` from the envelope. Both the field set and
// the signed transcript changed, so the two versions are not interchangeable
// in either direction and had to be told apart by the one field every reader
// consults before anything else.
const DatagramHeaderVersion DatagramVersion = 2

const (
	// DatagramBaseReplayWindow is the LAYER's base replay-cache window,
	// measured from auth.time (§2.2). It is the ceiling for BOTH validity and
	// replay retention, with no exception left to declare: the profiles that
	// could ask to own their own replay store went with the durable half of
	// the layer, so every frame on this plane is clamped to this value.
	DatagramBaseReplayWindow = 5 * time.Minute

	// DatagramFreshnessWindow is the |now − auth.time| tolerance of the
	// av = 1 profile (§3.3). It equals DatagramBaseReplayWindow today but is
	// a SEPARATE constant on purpose: the base pipeline holds no freshness
	// constant of its own (§2.2), so a future av may move this one without
	// touching the layer's replay window.
	DatagramFreshnessWindow = 5 * time.Minute

	// ReverseStateTTL is the lifetime of one request/response reverse-state
	// entry, derived in §4.2 from the round trip:
	// 2 × DatagramDefaultMaxHops × (queue residence + write grace) of the
	// control class + 10 s target budget = 210 s, rounded up to 240 s.
	ReverseStateTTL = 240 * time.Second

	// controlQueueResidence / bulkQueueResidence are the per-class times a
	// frame may sit in a send queue (§4.2). They are wire-normative because
	// ReverseStateTTL is computed from them.
	controlQueueResidence = 5 * time.Second
	bulkQueueResidence    = 30 * time.Second
)

// ---------------------------------------------------------------------------
// DatagramVersion — header `v`
// ---------------------------------------------------------------------------

// DatagramVersion is the datagram header version. Exactly one byte on the
// wire and in the transcript, so the legal range is 1…255: zero is not a
// version, it is a missing field.
type DatagramVersion uint8

// ParseDatagramVersion converts a JSON integer into a header version.
func ParseDatagramVersion(n int64) (DatagramVersion, error) {
	b, err := parseVersionByte(n)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidDatagramVersion, err)
	}
	return DatagramVersion(b), nil
}

// ---------------------------------------------------------------------------
// AuthVersion — auth `av`
// ---------------------------------------------------------------------------

// AuthVersion selects the signature/timing profile of the auth block. This
// build implements AuthVersionBase and refuses every other value as an
// unimplemented version.
type AuthVersion uint8

// AuthVersionBase is the base profile: fingerprint binding, Ed25519 and the
// five-minute window of §3.3.
const AuthVersionBase AuthVersion = 1

// ParseAuthVersion converts a JSON integer into an auth version.
func ParseAuthVersion(n int64) (AuthVersion, error) {
	b, err := parseVersionByte(n)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidAuthVersion, err)
	}
	return AuthVersion(b), nil
}

// parseVersionByte enforces the single rule shared by `v` and `av` (§3.4): a
// JSON integer in 1…255, one byte in the transcript. Zero, negative and
// out-of-range values are rejected here so two parsers cannot disagree on
// what the signed byte was.
func parseVersionByte(n int64) (uint8, error) {
	if n < 1 || n > 255 {
		return 0, fmt.Errorf("version %d outside 1..255", n)
	}
	return uint8(n), nil
}

// ---------------------------------------------------------------------------
// DatagramMode
// ---------------------------------------------------------------------------

// DatagramMode tells transit how to route the frame. It is a header field
// and never derived from dtype: an old transit must handle request and
// response of a type it has never heard of (§2, §4.2).
type DatagramMode string

const (
	// DatagramModeRouted is one-way delivery towards dst, always signed.
	DatagramModeRouted DatagramMode = "routed"
	// DatagramModeRequest is a query carrying a one-shot label in src.
	DatagramModeRequest DatagramMode = "request"
	// DatagramModeResponse travels back along stored reverse state only.
	DatagramModeResponse DatagramMode = "response"
)

// Valid reports whether m is one of the three closed-contract modes. The
// mode matrix is the single source of truth for that: a second list would
// let "known mode" and "mode with a rule" drift apart.
func (m DatagramMode) Valid() bool {
	_, ok := datagramModeMatrix[m]
	return ok
}

// String returns the wire form, which is also the transcript form.
func (m DatagramMode) String() string { return string(m) }

// ParseDatagramMode converts a wire string into a mode.
func ParseDatagramMode(s string) (DatagramMode, error) {
	m := DatagramMode(s)
	if !m.Valid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidDatagramMode, s)
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// Mode matrix (§2.1)
// ---------------------------------------------------------------------------

// DatagramModeRule is one row of the §2.1 matrix: which classes the mode
// admits, and whether auth and route_policy are required or forbidden.
// Presence of both is a boolean contract per mode — never "optional" — so
// one flag each expresses the whole row.
type DatagramModeRule struct {
	classes             map[DatagramClass]struct{}
	AuthRequired        bool
	RoutePolicyRequired bool
}

// AllowsClass reports whether the mode admits this traffic class.
func (r DatagramModeRule) AllowsClass(class DatagramClass) bool {
	_, ok := r.classes[class]
	return ok
}

// datagramModeMatrix is the closed contract of §2.1 expressed as data, not
// as a validation branch per rule. Anything outside this table is a reject,
// and one table means a new mode cannot be half-added by touching a single
// `if`.
var datagramModeMatrix = map[DatagramMode]DatagramModeRule{
	DatagramModeRouted: {
		classes:             map[DatagramClass]struct{}{DatagramClassControl: {}, DatagramClassBulk: {}},
		AuthRequired:        true,
		RoutePolicyRequired: true,
	},
	DatagramModeRequest: {
		classes:             map[DatagramClass]struct{}{DatagramClassControl: {}},
		AuthRequired:        false,
		RoutePolicyRequired: true,
	},
	DatagramModeResponse: {
		classes:             map[DatagramClass]struct{}{DatagramClassControl: {}},
		AuthRequired:        false,
		RoutePolicyRequired: false,
	},
}

// DatagramModeRuleFor returns the §2.1 row of the mode. The bool is false
// for a mode outside the closed contract, which is itself a reject reason.
func DatagramModeRuleFor(mode DatagramMode) (DatagramModeRule, bool) {
	rule, ok := datagramModeMatrix[mode]
	return rule, ok
}

// ---------------------------------------------------------------------------
// DatagramClass
// ---------------------------------------------------------------------------

// DatagramClass picks the size ceiling, the queue and the budget share.
// The enumeration is closed in this header version: a third class would be
// a new wire format, so growth happens as a dtype on top of bulk (§2.3).
type DatagramClass string

const (
	// DatagramClassControl carries small latency-sensitive frames.
	DatagramClassControl DatagramClass = "control"
	// DatagramClassBulk carries file chunks and future DM bodies.
	DatagramClassBulk DatagramClass = "bulk"
)

var datagramPayloadCaps = map[DatagramClass]int{
	DatagramClassControl: DatagramControlPayloadCap,
	DatagramClassBulk:    DatagramBulkPayloadCap,
}

var datagramQueueResidence = map[DatagramClass]time.Duration{
	DatagramClassControl: controlQueueResidence,
	DatagramClassBulk:    bulkQueueResidence,
}

// Valid reports whether c is one of the two closed-contract classes.
func (c DatagramClass) Valid() bool {
	_, ok := datagramPayloadCaps[c]
	return ok
}

// String returns the wire form, which is also the transcript form.
func (c DatagramClass) String() string { return string(c) }

// ParseDatagramClass converts a wire string into a class.
func ParseDatagramClass(s string) (DatagramClass, error) {
	c := DatagramClass(s)
	if !c.Valid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidDatagramClass, s)
	}
	return c, nil
}

// DatagramPayloadCap returns the DECODED payload ceiling of the class.
// An unknown class is an error rather than a zero ceiling, because a zero
// would silently reject every frame instead of surfacing the bug.
func DatagramPayloadCap(class DatagramClass) (int, error) {
	limit, ok := datagramPayloadCaps[class]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDatagramClass, string(class))
	}
	return limit, nil
}

// QueueResidence returns how long a frame of this class may wait in a send
// queue before it is dropped as too late to be worth writing (§4.2).
func QueueResidence(class DatagramClass) (time.Duration, error) {
	d, ok := datagramQueueResidence[class]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDatagramClass, string(class))
	}
	return d, nil
}

// WriteGrace returns the maximum time one frame of this class may spend
// inside the socket write itself. Numerically equal to QueueResidence:
// §4.2 defines the hop budget as queue time plus write time and needs both
// tails bounded, and a second independent constant would only let the two
// drift apart.
func WriteGrace(class DatagramClass) (time.Duration, error) {
	return QueueResidence(class)
}

// ---------------------------------------------------------------------------
// RoutePolicy
// ---------------------------------------------------------------------------

// RoutePolicy selects the candidate picking strategy (§4.3). It is
// mandatory for routed and request, and forbidden for response, where the
// path is fixed by stored state (§2.1).
type RoutePolicy string

const (
	// RoutePolicyNone is the explicit "field absent on the wire" value. It
	// exists so the response mode — where the field is forbidden — is
	// spelled out by a named constant instead of being inferred from an
	// empty string, and so Valid() can reject it wherever the field is
	// mandatory.
	RoutePolicyNone RoutePolicy = ""
	// RoutePolicyBest picks the best candidate by routing metric.
	RoutePolicyBest RoutePolicy = "best"
	// RoutePolicyExplore rotates candidates between attempts.
	RoutePolicyExplore RoutePolicy = "explore"
)

var routePolicies = map[RoutePolicy]struct{}{
	RoutePolicyBest:    {},
	RoutePolicyExplore: {},
}

// Valid reports whether p is a real policy. RoutePolicyNone is not valid:
// absence is a separate state, checked by IsNone.
func (p RoutePolicy) Valid() bool {
	_, ok := routePolicies[p]
	return ok
}

// IsNone reports whether the field is absent from the frame.
func (p RoutePolicy) IsNone() bool { return p == RoutePolicyNone }

// String returns the wire form, which is also the transcript form.
func (p RoutePolicy) String() string { return string(p) }

// ParseRoutePolicy converts a wire string into a policy. The absent form is
// never produced here — a present-but-empty route_policy is a reject.
func ParseRoutePolicy(s string) (RoutePolicy, error) {
	p := RoutePolicy(s)
	if !p.Valid() {
		return RoutePolicyNone, fmt.Errorf("%w: %q", ErrInvalidRoutePolicy, s)
	}
	return p, nil
}

// ---------------------------------------------------------------------------
// DType
// ---------------------------------------------------------------------------

// DType names the datagram protocol carried in payload. Transit never
// interprets it; only endpoints and interceptors resolve it in the type
// registry (§2, §7).
type DType string

// String returns the wire form, which is also the transcript form.
func (d DType) String() string { return string(d) }

// ParseDType validates the `[a-z0-9_]`, non-empty, ≤ MaxDTypeLen syntax.
func ParseDType(s string) (DType, error) {
	if err := validateWireName(s, MaxDTypeLen); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidDType, err)
	}
	return DType(s), nil
}

// ---------------------------------------------------------------------------
// CapabilityName
// ---------------------------------------------------------------------------

// CapabilityName is a capability name as it appears in the raw advertised
// capability set of a session. Kept distinct from Capability: Capability is
// the compile-time typed set this build knows, while CapabilityName is an
// arbitrary validated wire name that may belong to a build released later.
type CapabilityName string

// String returns the wire form.
func (c CapabilityName) String() string { return string(c) }

// Capability narrows the wire name to the typed capability set. Only
// meaningful for names this build knows.
func (c CapabilityName) Capability() Capability { return Capability(c) }

// ParseCapabilityName validates the `[a-z0-9_]`, non-empty,
// ≤ MaxCapabilityNameLen capability-name syntax.
func ParseCapabilityName(s string) (CapabilityName, error) {
	if err := validateWireName(s, MaxCapabilityNameLen); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidCapabilityName, err)
	}
	return CapabilityName(s), nil
}

// ParseRawCapabilityNames validates the raw advertised capability set of a
// peer — the set kept beside the typed one, so a name this build does not
// know is still comparable by string.
//
// Any breach of the bounds empties the WHOLE set rather than dropping the
// offending name, because "drop one" and "drop the set" behave differently in
// mixed implementations. The session survives and the typed capability set is
// untouched; what the peer loses is the datagram role gate, which reads this
// set.
func ParseRawCapabilityNames(names []string) []CapabilityName {
	if len(names) > MaxRawCapabilityNames {
		return nil
	}
	parsed := make([]CapabilityName, 0, len(names))
	for _, name := range names {
		valid, err := ParseCapabilityName(name)
		if err != nil {
			return nil
		}
		parsed = append(parsed, valid)
	}
	return parsed
}

// ---------------------------------------------------------------------------
// The declared dtype set (§6.1)
// ---------------------------------------------------------------------------

// DTypeDeclaration says WHICH of the two shapes §6.1 allows the `dtypes`
// field to take. Both shapes name the same SET when the list is empty — a
// peer that listed nothing is an endpoint for nothing either way — but they
// are different STATEMENTS, and the diagnostics report which one arrived: "it
// told us it handles no type" and "it told us nothing" are distinguishable
// facts about a peer, and a wire form that collapsed them could not be
// un-collapsed later.
type DTypeDeclaration uint8

const (
	// DTypeDeclarationAbsent is a field that was never sent. It names NO
	// type: §6.1 implies nothing on a silent peer's behalf, because unproven
	// support equals no support. Only a node that does not speak the envelope
	// emits this form, and its handshake stays wire-identical to a legacy
	// one.
	//
	// It is the zero value on purpose, and that is not a hidden signal: a
	// peer whose handshake has not been recorded at all made no statement,
	// which is the same thing as sending no field, and both name no type.
	// Every reader goes through Declaration(), so the state is read from the
	// type, never guessed from emptiness.
	DTypeDeclarationAbsent DTypeDeclaration = iota

	// DTypeDeclarationExplicit is a field that WAS sent: the set is exactly
	// the listed names and nothing else. An empty list is the lawful way to
	// say "the envelope yes, handlers no" — the same SET the absent form
	// names, said out loud.
	DTypeDeclarationExplicit
)

var dTypeDeclarationNames = map[DTypeDeclaration]string{
	DTypeDeclarationAbsent:   "absent",
	DTypeDeclarationExplicit: "explicit",
}

// String returns the diagnostic label of the declaration.
func (d DTypeDeclaration) String() string {
	if name, ok := dTypeDeclarationNames[d]; ok {
		return name
	}
	return "unknown"
}

// DeclaredDTypeSet is the `dtypes` field of §6.1 as a value: the names plus
// the fact of whether the field was there at all.
//
// Both halves are kept because the field has three WIRE forms — absent,
// empty, non-empty — while a bare []DType has two. The third form is what
// lets a node with an empty type registry say "the envelope yes, handlers no"
// instead of withholding `mesh_datagram_v1` altogether, which is what it had
// to do while an absent field was read as a set of types it did not carry.
type DeclaredDTypeSet struct {
	types       []DType
	declaration DTypeDeclaration
}

// AbsentDTypes is the field that was not sent, which names no type (§6.1).
func AbsentDTypes() DeclaredDTypeSet {
	return DeclaredDTypeSet{declaration: DTypeDeclarationAbsent}
}

// ExplicitDTypes is the field carrying exactly these names — the emit-side
// constructor, fed from a node's own type registry. An empty (or nil) slice
// yields the explicitly empty set, which is a statement, not a missing field.
//
// The names are NOT re-validated: this side of the wire builds them from
// parsed DType values. Validation belongs to the receive side, where the
// names are attacker-controlled.
func ExplicitDTypes(types []DType) DeclaredDTypeSet {
	return DeclaredDTypeSet{
		types:       append(make([]DType, 0, len(types)), types...),
		declaration: DTypeDeclarationExplicit,
	}
}

// ParseDeclaredDTypes validates a `dtypes` field that WAS PRESENT on the
// wire (§6.1). For the optional field itself use ParseDeclaredDTypesField,
// which maps its absence onto AbsentDTypes.
//
// The wire contract is closed, and every clause of it is expressed here:
//
//   - the names are a SET: order is not significant, duplicates collapse,
//     and the result keeps the order of first appearance so the value stays
//     byte-stable in diagnostics;
//   - an empty list is the explicitly empty set — "the envelope yes,
//     handlers no";
//   - bounds are ≤ MaxDTypesPerNode names, each `[a-z0-9_]` and
//     ≤ MaxDTypeLen chars;
//   - a bounds breach drops the WHOLE field to ABSENT, hence to no declared
//     type at all, and is never an error the caller could escalate into a
//     torn-down handshake: refusing a connection over an extensible field
//     would contradict the point of the layer, and degrading to "this peer
//     is no endpoint" is the conservative direction.
func ParseDeclaredDTypes(names []string) DeclaredDTypeSet {
	if len(names) > MaxDTypesPerNode {
		return AbsentDTypes()
	}
	seen := make(map[DType]struct{}, len(names))
	parsed := make([]DType, 0, len(names))
	for _, name := range names {
		dtype, err := ParseDType(name)
		if err != nil {
			return AbsentDTypes()
		}
		if _, duplicate := seen[dtype]; duplicate {
			continue
		}
		seen[dtype] = struct{}{}
		parsed = append(parsed, dtype)
	}
	return DeclaredDTypeSet{types: parsed, declaration: DTypeDeclarationExplicit}
}

// ParseDeclaredDTypesField maps the OPTIONAL wire field onto the two
// declarations. A nil pointer is the absent field; anything else was sent,
// including an empty array.
//
// The pointer is what carries the third wire state into Go: a plain
// []string cannot distinguish "no field" from "[]" once it has been through
// encoding/json, and the difference is exactly what §6.1 now assigns two
// different meanings to. A JSON `null` decodes to a nil pointer and is
// therefore read as absent — the conservative reading, and the only one that
// keeps a peer's malformed field from being taken as "no handlers".
func ParseDeclaredDTypesField(field *[]string) DeclaredDTypeSet {
	if field == nil {
		return AbsentDTypes()
	}
	return ParseDeclaredDTypes(*field)
}

// Declaration reports which of the two shapes the field had.
func (s DeclaredDTypeSet) Declaration() DTypeDeclaration { return s.declaration }

// Types returns the declared names as a copy — empty for an absent field,
// which names no type (§6.1).
func (s DeclaredDTypeSet) Types() []DType {
	return append(make([]DType, 0, len(s.types)), s.types...)
}

// Len returns the number of explicitly declared names, zero for an absent
// field and zero for an explicitly empty one. It does not tell the two
// apart — Declaration does.
func (s DeclaredDTypeSet) Len() int { return len(s.types) }

// Clone returns a deep copy, so no holder of the value can reach another
// holder's backing array.
func (s DeclaredDTypeSet) Clone() DeclaredDTypeSet {
	if s.declaration == DTypeDeclarationAbsent {
		return AbsentDTypes()
	}
	return ExplicitDTypes(s.types)
}

// WireField renders the set back into the optional wire field: nil for the
// absent form, a pointer to the (possibly empty) name list for the explicit
// one.
//
// The returned slice is never nil for an explicit set, and that is
// load-bearing: a *[]string pointing at a nil slice marshals as `null`,
// which is neither of the two forms §6.1 defines.
func (s DeclaredDTypeSet) WireField() *[]string {
	if s.declaration == DTypeDeclarationAbsent {
		return nil
	}
	names := make([]string, 0, len(s.types))
	for _, dtype := range s.types {
		names = append(names, dtype.String())
	}
	return &names
}

// validateWireName enforces the name alphabet shared by dtype, capability
// names and the dtypes list (§2.2, §6.1). One implementation so the three
// call sites cannot drift apart.
func validateWireName(s string, maxLen int) error {
	if s == "" {
		return errors.New("empty name")
	}
	if len(s) > maxLen {
		return fmt.Errorf("name length %d exceeds %d", len(s), maxLen)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return fmt.Errorf("name %q contains byte %q outside [a-z0-9_]", s, string(c))
	}
	return nil
}

// ---------------------------------------------------------------------------
// NetworkID
// ---------------------------------------------------------------------------

// NetworkID names the network a datagram belongs to. It is bound into every
// transcript (§3.2) so a relay cannot re-bind a signed frame to another
// network. Kept as a value passed in by the node rather than a constant
// duplicated in the protocol package: the network name has exactly one
// declaration, and signing a second copy of it would only let the two drift.
type NetworkID string

// String returns the wire/transcript form: UTF-8 bytes, no BOM (§3.2).
func (n NetworkID) String() string { return string(n) }

// ParseNetworkID enforces the transcript encoding of §3.2: non-empty valid
// UTF-8 without a BOM. Empty would produce a zero-length segment and make
// every network sign identically — precisely the re-binding the field
// exists to prevent — and a BOM would make one network name sign two ways.
func ParseNetworkID(s string) (NetworkID, error) {
	switch {
	case s == "":
		return "", fmt.Errorf("%w: empty network id", ErrInvalidNetworkID)
	case strings.HasPrefix(s, "\ufeff"):
		return "", fmt.Errorf("%w: network id carries a byte order mark", ErrInvalidNetworkID)
	case !utf8.ValidString(s):
		return "", fmt.Errorf("%w: network id is not valid UTF-8", ErrInvalidNetworkID)
	default:
		return NetworkID(s), nil
	}
}

// ---------------------------------------------------------------------------
// ReplayKey
// ---------------------------------------------------------------------------

// ReplayKey is sha256(transcript) — the anti-replay cache key of a routed
// datagram (§3.2). It never travels on the wire: any node holding the frame
// derives it, so there is nothing to forge and nothing to compare against.
type ReplayKey [32]byte

// IsZero reports whether the key is the zero value, i.e. not derived yet.
func (k ReplayKey) IsZero() bool { return k == ReplayKey{} }

// String returns the canonical 64-char lowercase hex form used in logs and
// metrics, or the empty string for the zero value.
func (k ReplayKey) String() string {
	if k.IsZero() {
		return ""
	}
	return hex.EncodeToString(k[:])
}

// ReplayKeyFromBytes builds a ReplayKey from exactly 32 digest bytes.
func ReplayKeyFromBytes(b []byte) (ReplayKey, error) {
	var key ReplayKey
	if len(b) != len(key) {
		return key, fmt.Errorf("%w: replay key must be %d bytes, got %d", ErrInvalidReplayKey, len(key), len(b))
	}
	copy(key[:], b)
	return key, nil
}

// ParseReplayKey decodes the canonical 64-char lowercase hex form.
func ParseReplayKey(s string) (ReplayKey, error) {
	var key ReplayKey
	if len(s) != 2*len(key) {
		return key, fmt.Errorf("%w: replay key must be %d hex chars, got %d", ErrInvalidReplayKey, 2*len(key), len(s))
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ReplayKey{}, fmt.Errorf("%w: replay key must be lowercase hex, got %q", ErrInvalidReplayKey, s)
		}
	}
	if _, err := hex.Decode(key[:], []byte(s)); err != nil {
		return ReplayKey{}, fmt.Errorf("%w: %w", ErrInvalidReplayKey, err)
	}
	return key, nil
}
