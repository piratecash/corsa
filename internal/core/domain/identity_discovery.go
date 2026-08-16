package domain

// identity_discovery.go names the discovery datagram types and their
// payload bounds (docs/protocol/identity-lookup.md). The types ride the
// datagram plane: get_identity/post_identity live in the unauthenticated
// request/response modes, push_identity in the signed routed mode.

const (
	// DTypeGetIdentity is the key-lookup request; dst is the identity being
	// looked up, src is the one-shot attempt label.
	DTypeGetIdentity = DType("get_identity")

	// DTypePostIdentity is the owner's answer, returned over the reverse
	// state of the request's label.
	DTypePostIdentity = DType("post_identity")

	// DTypePushIdentity carries the sender's own record to a direct session
	// peer: routed mode, ttl = 1, mandatory transport auth.
	DTypePushIdentity = DType("push_identity")
)

const (
	// MaxGetIdentityPayloadBytes caps the decoded request payload.
	MaxGetIdentityPayloadBytes = 512

	// MaxPostIdentityPayloadBytes caps the decoded answer payload: the
	// 3.2 KiB discovery budget inside the 4 KiB control-class ceiling —
	// a maximal record with a proof is ~3.1 KiB, leaving ~1 KiB of the
	// class cap as reserve.
	MaxPostIdentityPayloadBytes = 3276
)

// LookupRequirementTargetProof is the one requirement name this build
// understands in a get_identity `required` list: the answer must carry a
// live proof by the target, bound to this very attempt.
const LookupRequirementTargetProof = "target_proof"

// IdentityLookupSchemaVersion is the discovery-payload schema version of
// all three types — independent from the datagram header version and from
// the record envelope version.
const IdentityLookupSchemaVersion = 1

// ---------------------------------------------------------------------------
// Resolution state axes (§4.9)
// ---------------------------------------------------------------------------

// IdentityResolutionLifecycle is the lifecycle axis of one resolution:
// pending → active → succeeded | cancelled | exhausted. The progress flags
// (interactive_timeout, no_route) are separate booleans, not lifecycle
// states — they never end the operation.
type IdentityResolutionLifecycle string

const (
	IdentityResolutionPending   IdentityResolutionLifecycle = "pending"
	IdentityResolutionActive    IdentityResolutionLifecycle = "active"
	IdentityResolutionSucceeded IdentityResolutionLifecycle = "succeeded"
	IdentityResolutionCancelled IdentityResolutionLifecycle = "cancelled"
	IdentityResolutionExhausted IdentityResolutionLifecycle = "exhausted"
)

// Terminal reports whether the lifecycle has ended.
func (l IdentityResolutionLifecycle) Terminal() bool {
	switch l {
	case IdentityResolutionSucceeded, IdentityResolutionCancelled, IdentityResolutionExhausted:
		return true
	default:
		return false
	}
}

// IdentityRecordAuthority is the authority axis: none → provisional →
// authoritative. Provisional sources (the epidemic, a corsa: link, v27
// envelope keys) are equal among themselves; only the owner's signed
// record is authoritative.
type IdentityRecordAuthority string

const (
	IdentityAuthorityNone          IdentityRecordAuthority = "none"
	IdentityAuthorityProvisional   IdentityRecordAuthority = "provisional"
	IdentityAuthorityAuthoritative IdentityRecordAuthority = "authoritative"
)

// DMAvailability is the dm_available axis, read from the authoritative
// record alone: unknown until one arrives.
type DMAvailability string

const (
	DMAvailabilityUnknown DMAvailability = "unknown"
	DMAvailabilityYes     DMAvailability = "true"
	DMAvailabilityNo      DMAvailability = "false"
)
