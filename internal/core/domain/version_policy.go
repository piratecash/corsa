// Package domain — version policy types.
//
// These types enforce compile-time distinction between version-related
// scalars used in peer incompatibility detection, ban scoring, update
// signalling and persisted lockout. Mixing a protocol version with a
// build number or an attempt count with a reporter count becomes a
// compile error instead of a silent misuse.
package domain

import "time"

// ---------------------------------------------------------------------------
// Typed scalars — version policy
// ---------------------------------------------------------------------------

// ProtocolVersion is the integer protocol version exchanged during
// handshake. Compared against config.MinimumProtocolVersion and
// config.ProtocolVersion.
type ProtocolVersion int

// AttemptCount is a monotonic counter for events/attempts of a given
// kind (e.g. incompatible-version connections from the same peer).
type AttemptCount int

// ReporterCount is the cardinality of a dedup set of unique peer
// identities that reported an incompatible-version observation.
type ReporterCount int

// ClientVersion is the human-readable client version string
// (e.g. "0.12.0-beta").
type ClientVersion string

// UpdateReason is a closed enum for the reason behind an update_available
// signal. Consumers (Desktop UI, RPC clients) must handle all four values.
//
// Precedence when multiple signals are active simultaneously:
//
//	Both > IncompatibleVersion > PeerBuild > None
//
// "Both" is emitted when the build signal AND (the incompatible-version
// signal OR an active persisted lockout) are true at the same time.
// The lockout signal is grouped with the incompatible-version signal
// because both represent confirmed version evidence, whereas the build
// signal is a softer heuristic.
type UpdateReason string

const (
	// UpdateReasonNone — no update signal. update_available is false.
	UpdateReasonNone UpdateReason = ""
	// UpdateReasonPeerBuild — ≥2 connected peers with distinct identities
	// advertise a higher client_build than ours. Soft heuristic.
	UpdateReasonPeerBuild UpdateReason = "peer_build_newer"
	// UpdateReasonIncompatibleVersion — ≥3 distinct peer identities reported
	// incompatible-protocol-version within the observation window, OR at
	// least one active persisted version lockout exists (confirmed evidence).
	UpdateReasonIncompatibleVersion UpdateReason = "incompatible_version_reporters"
	// UpdateReasonBoth — both the build signal and the incompatible-version
	// (or lockout) signal are active simultaneously.
	UpdateReasonBoth UpdateReason = "peer_build_and_incompatible_version"
)

// VersionLockoutReason describes why a persisted version lockout was set.
// This is a separate enum from UpdateReason — lockout reasons are persisted
// in peers.json and describe the evidence behind a dial-suppression decision,
// whereas UpdateReason is a transient UI-facing signal. The two must not be
// conflated: adding a new lockout reason does not require changing the UI
// update prompt, and vice versa.
type VersionLockoutReason string

const (
	// VersionLockoutReasonNone — no lockout.
	VersionLockoutReasonNone VersionLockoutReason = ""
	// VersionLockoutReasonIncompatible — peer confirmed our version
	// is below their minimum_protocol_version.
	VersionLockoutReasonIncompatible VersionLockoutReason = "incompatible_protocol"
)

// ---------------------------------------------------------------------------
// Composite types — version observation and lockout
// ---------------------------------------------------------------------------

// LocalVersionFingerprint captures the local protocol version and
// client build at the moment a version lockout was recorded. Used to
// determine whether the lockout should be cleared after a local upgrade.
type LocalVersionFingerprint struct {
	ProtocolVersion ProtocolVersion `json:"protocol_version"`
	ClientBuild     int             `json:"client_build"`
}

// VersionLockoutSnapshot is the persisted lockout context for a peer
// that confirmed our version is incompatible. Stored in peers.json.
type VersionLockoutSnapshot struct {
	// ObservedProtocolVersion is the remote peer's advertised protocol version.
	ObservedProtocolVersion ProtocolVersion `json:"observed_protocol_version,omitempty"`
	// ObservedMinimumProtocolVersion is the remote peer's declared minimum.
	ObservedMinimumProtocolVersion ProtocolVersion `json:"observed_minimum_protocol_version,omitempty"`
	// ObservedClientVersion is the remote peer's human-readable version.
	ObservedClientVersion ClientVersion `json:"observed_client_version,omitempty"`
	// LockedAtLocalVersion is the local version fingerprint at lockout time.
	LockedAtLocalVersion LocalVersionFingerprint `json:"locked_at_local_version"`
	// Reason is the machine-readable lockout reason.
	Reason VersionLockoutReason `json:"reason"`
	// LockedAt is the time when the lockout was created. Used as the
	// starting point for the hard expiration TTL. Lockouts without an
	// associated PeerIdentity (address-only, no cryptographic binding)
	// expire after VersionLockoutMaxTTL to prevent stale entries from
	// suppressing addresses that may later belong to a different peer.
	LockedAt time.Time `json:"locked_at,omitempty"`
	// PeerIdentity is the cryptographic identity of the peer that was
	// locked out. When non-empty the lockout is identity-bound and does
	// not expire by TTL — only by local version change. When empty the
	// lockout is address-only and subject to VersionLockoutMaxTTL.
	PeerIdentity PeerIdentity `json:"peer_identity,omitempty"`
}

// VersionLockoutMaxTTL is the maximum lifetime of an address-only
// (identity-less) version lockout. After this period the lockout is
// considered stale and is ignored, because the address may have been
// reassigned to a different peer.
const VersionLockoutMaxTTL = 7 * 24 * time.Hour // 7 days

// IsActive reports whether the lockout carries a non-empty reason.
// Identity-bound lockouts remain active until local version change.
// Address-only lockouts (PeerIdentity == "") expire after VersionLockoutMaxTTL.
func (v VersionLockoutSnapshot) IsActive() bool {
	return v.Reason != VersionLockoutReasonNone
}

// IsActiveAt is like IsActive but additionally enforces the hard TTL
// for address-only lockouts. Callers that have access to the current
// time should prefer this over IsActive.
func (v VersionLockoutSnapshot) IsActiveAt(now time.Time) bool {
	if v.Reason == VersionLockoutReasonNone {
		return false
	}
	// Identity-bound lockout — no TTL, cleared only by version change.
	if v.PeerIdentity != "" {
		return true
	}
	// Address-only lockout — bounded lifetime.
	if v.LockedAt.IsZero() {
		// Legacy entry without timestamp — treat as active to avoid
		// silently dropping lockouts created before this field was added.
		return true
	}
	return now.Before(v.LockedAt.Add(VersionLockoutMaxTTL))
}

// VersionObservation is a runtime record of a single incompatible-version
// report from a peer identity. Stored in VersionPolicyState's dedup set.
type VersionObservation struct {
	PeerIdentity PeerIdentity    `json:"peer_identity"`
	ObservedAt   time.Time       `json:"observed_at"`
	PeerVersion  ProtocolVersion `json:"peer_version"`
	PeerMinimum  ProtocolVersion `json:"peer_minimum"`
}

// ---------------------------------------------------------------------------
// Aggregate version policy snapshot
// ---------------------------------------------------------------------------

// VersionPolicySnapshot is an immutable point-in-time view of the node's
// version policy state. Returned by the node layer to Desktop/RPC.
type VersionPolicySnapshot struct {
	// UpdateAvailable is the computed product signal.
	UpdateAvailable bool `json:"update_available"`
	// UpdateReason describes which signal triggered the update prompt.
	UpdateReason UpdateReason `json:"update_reason"`
	// IncompatibleVersionReporters is the count of distinct peer identities
	// that reported our version as incompatible within the observation window.
	IncompatibleVersionReporters ReporterCount `json:"incompatible_version_reporters"`
	// MaxObservedPeerBuild is the highest client build seen across connected peers.
	MaxObservedPeerBuild int `json:"max_observed_peer_build"`
	// MaxObservedPeerVersion is the highest protocol version reported by peers
	// that are incompatible with us. Computed from two sources:
	//   1. Runtime incompatible-reporter observations (PeerVersion field of each
	//      reporter in the 24-hour dedup set) — cleared on restart.
	//   2. Active persisted version lockouts (ObservedProtocolVersion field) —
	//      survive restarts, cleared by clearStaleVersionLockoutsLocked on
	//      local version upgrade.
	// After restart, only source (2) contributes until peers reconnect and
	// repopulate the reporter set. Zero when no incompatible evidence exists.
	MaxObservedPeerVersion ProtocolVersion `json:"max_observed_peer_version"`
}
