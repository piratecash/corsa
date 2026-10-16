package domain

import "time"

// SecureSessionStoreStats is the state of a node's v2 downgrade-protection
// store as diagnostics see it (docs/protocol/session_v2.md, "Downgrade
// protection").
type SecureSessionStoreStats struct {
	// ReadAt is when the figures were read.
	ReadAt time.Time
	// PinnedIdentities is how many identities are pinned to v2.
	PinnedIdentities int
	// PinCapacity is the bound on pinned identities.
	PinCapacity int
	// Full reports that no NEW identity can be pinned: a v2 session of an
	// identity not pinned yet is refused with pin_store_full. Existing pins
	// are kept and keep being served.
	Full bool
	// PinRefusalsStoreFull counts v2 sessions refused because their
	// mandatory pin could not be stored (pin_store_full), in either
	// direction, since the process started.
	PinRefusalsStoreFull uint64
	// V1RefusalsIdentityPinned counts v1 sessions refused because the
	// identity they name requires v2 — pinned, or holding a live v2
	// connection — on accept and on dial, since the process started.
	V1RefusalsIdentityPinned uint64
	// V1RefusalsEndpointBound counts v1 answers refused on dial because the
	// endpoint that gave them is bound to v2 — a downgrade, not followed —
	// since the process started.
	V1RefusalsEndpointBound uint64
	// V1DialsHeldByRefusalBackoff counts v1 dials this node withheld because
	// an earlier v1 answer from the same address was refused (the two
	// counters above) and the hold that follows such a refusal had not run
	// out — 5 minutes, doubled per further refusal, at most 6 hours — since
	// the process started. A v2 attempt is never held.
	V1DialsHeldByRefusalBackoff uint64
	// DialsHeldByV2RetryBackoff counts dials refused before any socket
	// because their address's v2 retry backoff ran: after a v2 attempt there
	// ended without v2 (the v1 answer was refused or its fall back held),
	// the next dial waits 30 s, doubled per further such end, at most 30
	// min, since the process started. Pacing, not a penalty: score and
	// failures are untouched.
	DialsHeldByV2RetryBackoff uint64
	// ProtectedBindingsContact and ProtectedBindingsRelation are the
	// protected endpoint bindings of each pool (ProtectedBindingPool).
	ProtectedBindingsContact  int
	ProtectedBindingsRelation int
	// ProtectedCapacity bounds the protected bindings of both pools
	// together; ProtectedReserveContact of those places are for the contact
	// pool only, so the relation pool holds at most ProtectedCapacity -
	// ProtectedReserveContact. ProtectedPerIdentityCapacity bounds the
	// protected bindings one identity gives.
	ProtectedCapacity            int
	ProtectedReserveContact      int
	ProtectedPerIdentityCapacity int
	// GeneralBindings and GeneralCapacity are the ordinary bindings — 30
	// days from the last v2 session — and their bound, apart from the
	// protected ones.
	GeneralBindings int
	GeneralCapacity int
	// ProtectedRefusals* count the protected bindings refused at a bound,
	// by reason (ProtectedBindingRefusalReason), since the process started.
	// A refused endpoint may still have been bound as an ordinary binding.
	ProtectedRefusalsPerIdentity uint64
	ProtectedRefusalsPoolRFull   uint64
	ProtectedRefusalsStoreFull   uint64
	// GeneralCreationsPerGroupPerDay bounds the NEW ordinary bindings one
	// source group (SourceGroup) may create in any 24 hours.
	// GeneralRefusalsSourceGroupQuota counts new ordinary bindings refused
	// by that bound since the process started; existing bindings in the
	// group are renewed and change owner as before.
	GeneralCreationsPerGroupPerDay  int
	GeneralRefusalsSourceGroupQuota uint64
	// EndpointBindingsSkippedNotCanonical counts proven outbound v2
	// sessions whose dialled address had no canonical form (a numeric or
	// non-ASCII name, say), so their endpoint got no binding; the identity
	// was pinned and the session stood. Since the process started.
	EndpointBindingsSkippedNotCanonical uint64
	// NonCanonicalBindings is how many bindings read from the store file sit
	// under an address with no canonical form — written by an earlier build
	// under the raw address it dialled. They are kept as they were, for
	// comparison with that exact address only: never renewed, raised or
	// created again; protected ones count against the protected bounds and
	// never expire, ordinary ones expire 30 days after their last v2.
	NonCanonicalBindings int
	// Unreadable reports that the store file exists but could not be read:
	// every peer is then treated as pinned and v1 is refused.
	Unreadable bool
	// LocationUnsafe reports that the store's location failed the audit
	// made when it was loaded: the file is a symlink or not this user's
	// 0600 (on Windows, its DACL grants someone else), or a directory on
	// the way to it may be written by someone other than this user or
	// root, or could not be inspected. The node keeps working; the
	// operator is told, in the log, which component and why.
	LocationUnsafe bool
	// ClockJumpGuarded reports that the terms of ordinary bindings run on
	// the trusted time: the wall clock at the start plus the time since on a
	// clock that keeps running through sleep. Without such a clock on the
	// platform it is false and the wall clock is taken as it is.
	ClockJumpGuarded bool
	// ClockJumpsDetected counts the times since the process started that
	// the wall clock was found more than an hour ahead of the trusted time
	// (clock_jump_detected); the excess was not counted toward any term.
	ClockJumpsDetected uint64
	// ClockJumpActive reports that the wall clock is ahead of the trusted
	// time right now, so the terms run on the trusted time instead.
	ClockJumpActive bool
}

// ProtectedBindingPool is the pool of the bound on protected endpoint
// bindings a protected binding counts against. It is fixed when the binding
// is created or raised and never derived again: a contact removed, or an
// identity moving between the classes, leaves its bindings where they were
// counted, or reclassification would carry contact bindings into the
// relation pool past its bound.
type ProtectedBindingPool string

const (
	// ProtectedBindingPoolContact (K): the identity is one of this node's
	// contacts. It may use every protected place, the contact reserve
	// included.
	ProtectedBindingPoolContact ProtectedBindingPool = "K"
	// ProtectedBindingPoolRelation (R): the identity is a relation this
	// node's own outgoing actions established. It never takes the contact
	// reserve.
	ProtectedBindingPoolRelation ProtectedBindingPool = "R"
)

// Valid reports whether p is one of the two pools.
func (p ProtectedBindingPool) Valid() bool {
	return p == ProtectedBindingPoolContact || p == ProtectedBindingPoolRelation
}

// ProtectedBindingRefusalReason is why a protected binding was refused.
type ProtectedBindingRefusalReason string

const (
	// ProtectedBindingRefusedPerIdentity: the identity that would give the
	// protection already gives it to as many endpoints as allowed.
	ProtectedBindingRefusedPerIdentity ProtectedBindingRefusalReason = "per_identity"
	// ProtectedBindingRefusedPoolRFull: a relation, and the places the
	// relation pool may take are all taken.
	ProtectedBindingRefusedPoolRFull ProtectedBindingRefusalReason = "pool_r_full"
	// ProtectedBindingRefusedStoreFull: a contact, and every protected place
	// is taken, the contact reserve included.
	ProtectedBindingRefusedStoreFull ProtectedBindingRefusalReason = "store_full"
	// ProtectedBindingRefusedOwnerChanged: not a bound. The identity proven
	// on the endpoint is not the one last proven there, and a new owner
	// never raises an ordinary binding to protected; the same identity
	// proven there again can.
	ProtectedBindingRefusedOwnerChanged ProtectedBindingRefusalReason = "owner_changed"
	// ProtectedBindingRefusedDerivedKey: not a bound. The endpoint is the
	// address a name dialled direct reached, bound beside the name; such a
	// key stays ordinary for good, since the address behind a name is the
	// provider's fact, not part of the identity.
	ProtectedBindingRefusedDerivedKey ProtectedBindingRefusalReason = "derived_key"
)

// ProtectedBindingRefused is a protected endpoint binding refused — at a bound,
// or because the endpoint changed owner. Nothing existing was removed or
// lowered for it. Returned as
// an error only to an operation that needs the protection; otherwise it is
// reported beside the binding that was recorded instead, if any.
type ProtectedBindingRefused struct {
	Reason ProtectedBindingRefusalReason
}

func (e *ProtectedBindingRefused) Error() string {
	return "protected_binding_refused: " + string(e.Reason)
}
