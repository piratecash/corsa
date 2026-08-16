package domain

import (
	"bytes"
	"errors"
)

// identity_record.go holds the vocabulary of the signed identity record
// (docs/protocol/identity-lookup.md): the seq counter, the wire-normative
// size caps and the merge decision two stores (trust store, resolver) must
// agree on. The record itself — signing, parsing, verification — lives in
// internal/core/protocol/identity_record.go; this file is only the domain
// language shared by everyone who stores or compares records.

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// ErrIdentityRecordConflict marks two records of one identity with the
// same seq but different body bytes. The record is NOT replaced; the
// owner is obliged to issue a new seq.
var ErrIdentityRecordConflict = errors.New("identity record seq conflict")

// ---------------------------------------------------------------------------
// Wire constants
// ---------------------------------------------------------------------------

const (
	// IdentityRecordVersion is the only signed-record format version this
	// build understands. It versions the {v, body, sig} envelope that lives
	// on the wire, on disk and inside a corsa: link — independently from the
	// datagram header version and from any discovery payload schema.
	IdentityRecordVersion = 1

	// MaxIdentityRecordBodyBytes is the AUTHORITATIVE cap on the raw signed
	// body bytes. It is checked before anything else: per-element maxima
	// (dtypes names, keys) do not guarantee the total fits, so the owner
	// must fit the overall budget rather than max out each field.
	MaxIdentityRecordBodyBytes = 2048

	// MaxIdentityRecordBytes caps the whole {v, body, sig} wire object —
	// the body cap plus base64url expansion plus the envelope fields.
	MaxIdentityRecordBytes = 2900

	// MaxIdentityRecordDTypes bounds the dtypes list INSIDE a record. The
	// record budget is far tighter than a handshake frame, hence the cap is
	// its own constant rather than MaxDTypesPerNode. A bounds breach drops
	// the field to its ABSENT value instead of rejecting the record.
	MaxIdentityRecordDTypes = 8
)

// ---------------------------------------------------------------------------
// IdentityRecordSeq
// ---------------------------------------------------------------------------

// IdentityRecordSeq is the monotonic issue counter of one identity's signed
// record. It is a pure counter: wall-clock lives only in issued_at and takes
// no part in merging. Zero is a legal value only as "no record stored yet" —
// the first issued record carries seq 1.
type IdentityRecordSeq uint64

// Next returns the seq the owner must use for the next issued record.
func (s IdentityRecordSeq) Next() IdentityRecordSeq { return s + 1 }

// ---------------------------------------------------------------------------
// Merge decision
// ---------------------------------------------------------------------------

// IdentityRecordMergeOutcome is the five-way result of merging an incoming
// verified record into a store. The outcomes are distinct because they drive
// different behaviour at every call site: replacement mutates the store,
// duplicate and stale are silent no-ops, and conflict is a loggable protocol
// violation by the record owner.
type IdentityRecordMergeOutcome uint8

const (
	// IdentityRecordMergeInserted — the store had no record for this identity.
	IdentityRecordMergeInserted IdentityRecordMergeOutcome = iota + 1
	// IdentityRecordMergeReplaced — the incoming seq is higher; the stored
	// record is superseded.
	IdentityRecordMergeReplaced
	// IdentityRecordMergeDuplicate — same seq, byte-identical body: a no-op.
	IdentityRecordMergeDuplicate
	// IdentityRecordMergeStale — the incoming seq is lower than the stored
	// one. A legal reorder after reconnects, not an error.
	IdentityRecordMergeStale
	// IdentityRecordMergeConflict — same seq, different body bytes. The
	// store keeps its record; the owner must issue a new seq.
	IdentityRecordMergeConflict
)

var identityRecordMergeNames = map[IdentityRecordMergeOutcome]string{
	IdentityRecordMergeInserted:  "inserted",
	IdentityRecordMergeReplaced:  "replaced",
	IdentityRecordMergeDuplicate: "duplicate",
	IdentityRecordMergeStale:     "stale",
	IdentityRecordMergeConflict:  "conflict",
}

// String returns the log/metric label of the outcome.
func (o IdentityRecordMergeOutcome) String() string {
	if name, ok := identityRecordMergeNames[o]; ok {
		return name
	}
	return "unknown"
}

// Accepted reports whether the incoming record must become the stored one.
func (o IdentityRecordMergeOutcome) Accepted() bool {
	return o == IdentityRecordMergeInserted || o == IdentityRecordMergeReplaced
}

// StoredIdentityRecordState is the merge-relevant projection of a record a
// store already holds. A separate type (rather than two loose arguments) so
// "no record stored" is stated by the Absent constructor instead of being
// guessed from a zero seq.
type StoredIdentityRecordState struct {
	seq    IdentityRecordSeq
	body   []byte
	exists bool
}

// AbsentIdentityRecord states that the store holds nothing for the identity.
func AbsentIdentityRecord() StoredIdentityRecordState {
	return StoredIdentityRecordState{}
}

// ExistingIdentityRecord states the seq and body bytes of the stored record.
func ExistingIdentityRecord(seq IdentityRecordSeq, body []byte) StoredIdentityRecordState {
	return StoredIdentityRecordState{seq: seq, body: body, exists: true}
}

// DecideIdentityRecordMerge applies the seq-merge contract: a higher seq
// wins; an equal seq with identical body bytes is a duplicate; an equal seq
// with different bytes is a conflict — the stored record is kept and the
// owner must bump seq; a lower seq is stale. Body bytes are compared as
// signed — verbatim, never re-serialised.
func DecideIdentityRecordMerge(stored StoredIdentityRecordState, incomingSeq IdentityRecordSeq, incomingBody []byte) IdentityRecordMergeOutcome {
	switch {
	case !stored.exists:
		return IdentityRecordMergeInserted
	case incomingSeq > stored.seq:
		return IdentityRecordMergeReplaced
	case incomingSeq < stored.seq:
		return IdentityRecordMergeStale
	case bytes.Equal(incomingBody, stored.body):
		return IdentityRecordMergeDuplicate
	default:
		return IdentityRecordMergeConflict
	}
}

// ---------------------------------------------------------------------------
// The dtypes list inside a record
// ---------------------------------------------------------------------------

// ParseIdentityRecordDTypesField parses the OPTIONAL dtypes field of a
// record body under the record's own bounds (≤ MaxIdentityRecordDTypes
// names, each `[a-z0-9_]` ≤ MaxDTypeLen; duplicates collapse). The three
// wire forms keep their §6.1 meanings: an absent field declares no type, an
// explicit empty array additionally states "envelope yes, handlers no".
//
// A bounds breach drops the WHOLE field to its absent value rather than
// rejecting the record: the record stays valid and the peer merely declares
// no type — the same conservative direction the handshake field takes.
// Element count is taken from the wire before deduplication, so a sender
// cannot smuggle an oversized list past the cap by repeating names.
func ParseIdentityRecordDTypesField(field *[]string) DeclaredDTypeSet {
	if field == nil {
		return AbsentDTypes()
	}
	if len(*field) > MaxIdentityRecordDTypes {
		return AbsentDTypes()
	}
	return ParseDeclaredDTypes(*field)
}
