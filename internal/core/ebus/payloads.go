package ebus

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// PeerHealthDelta is the payload for TopicPeerHealthChanged.
// Carries the fields that change discretely at connect/disconnect/ping/pong
// events. Continuous counters (BytesSent, BytesReceived) are delivered
// separately via TopicPeerTrafficUpdated.
//
// Timestamp-pointer semantics differ by group — applyHealthDeltaToRow
// enforces both conventions. Publishers populate these fields exclusively
// through TimePtr (which collapses the zero time to nil), so the wire
// format is effectively two-valued per pointer:
//
//  1. Activity timestamps — LastConnectedAt, LastDisconnectedAt, LastPingAt,
//     LastPongAt, LastUsefulSendAt, LastUsefulReceiveAt:
//     nil     = skip-field (subscriber does not touch the stored value)
//     non-nil = overwrite the stored value via domain.TimeFromPtr.
//     Publishers that want to preserve the field emit nil; partial deltas
//     must carry only the fields that actually changed.
//
//  2. Diagnostic timestamps — BannedUntil, LastIncompatibleVersionAt:
//     nil     = explicit clear (subscriber writes an invalid OptionalTime).
//     non-nil = overwrite with the pointee.
//     These fields are ebus-authoritative after NodeStatusMonitor switched
//     to one-shot FetchAndSeed() + event-driven updates; every delta carries
//     the complete current value, and resetPeerHealthForRecoveryLocked
//     clearing a ban is signalled by nil.
//
// We deliberately use *time.Time here instead of domain.OptionalTime so
// the activity-group can express "skip" without colliding with "clear" —
// domain.OptionalTime only distinguishes Valid vs !Valid and would merge
// those two intents. Do not "unify" this to domain.OptionalTime without
// first introducing a separate boolean-per-field skip mask; otherwise a
// probe-seeded activity timestamp will silently get zeroed on every
// partial delta.
//
// Non-timestamp diagnostic fields (LastErrorCode, LastDisconnectCode,
// IncompatibleVersionAttempts, ObservedPeerVersion,
// ObservedPeerMinimumVersion, VersionLockoutActive) are also
// ebus-authoritative: every delta carries the complete current value,
// and a zero/empty field is an explicit reset, not "unchanged".
// Subscribers must overwrite the stored value even with zeros —
// backfilling from a stale probe snapshot would resurrect a cleared ban.
type PeerHealthDelta struct {
	Address             domain.PeerAddress
	PeerID              domain.PeerIdentity
	Direction           domain.PeerDirection // outbound / inbound / "" (unknown)
	ClientVersion       string
	ClientBuild         int
	ProtocolVersion     int
	ConnID              uint64   // outbound session ConnID (0 when no outbound session)
	InboundConnIDs      []uint64 // active inbound connection IDs (nil when none)
	State               string
	Connected           bool
	Score               int
	PendingCount        int
	ConsecutiveFailures int
	LastConnectedAt     *time.Time
	LastDisconnectedAt  *time.Time
	LastPingAt          *time.Time
	LastPongAt          *time.Time
	LastUsefulSendAt    *time.Time
	LastUsefulReceiveAt *time.Time
	LastError           string

	// Machine-readable diagnostic fields — ebus-authoritative.
	// BannedUntil / LastIncompatibleVersionAt follow the diagnostic-group
	// pointer convention documented on the struct: nil = explicit clear
	// (written by applyHealthDeltaToRow unconditionally), non-nil = set.
	BannedUntil                 *time.Time
	LastErrorCode               string
	LastDisconnectCode          string
	IncompatibleVersionAttempts domain.AttemptCount
	LastIncompatibleVersionAt   *time.Time
	ObservedPeerVersion         domain.ProtocolVersion
	ObservedPeerMinimumVersion  domain.ProtocolVersion
	VersionLockoutActive        bool
}

// TimePtr returns a pointer to t when t is non-zero, nil otherwise.
// Used by publishers to populate optional timestamp fields in PeerHealthDelta.
func TimePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// ContactAddedEvent is the payload for TopicContactAdded.
// Carries all fields so the receiver can upsert the contact locally.
// Address is the peer identity (Ed25519 fingerprint) keying the trust
// store — typed as domain.PeerIdentity so subscribers cannot confuse it
// with a transport address and a reviewer reading the handler signature
// sees the domain intent at a glance.
//
// Note: Bus.Publish takes ...interface{} and dispatches via reflection,
// so the payload type alone does not prevent a caller from passing the
// wrong argument at a publish site (the compiler accepts it; safeCall
// will panic at runtime on a signature mismatch). Compile-time
// enforcement at the publish boundary lives in the typed helper
// PublishContactAdded — always use it instead of bus.Publish directly.
//
// The key fields use dedicated domain aliases so ebus subscribers and
// publishers cannot accidentally swap a public key, box key, or binding
// signature while still keeping the transport layer on raw strings.
type ContactAddedEvent struct {
	Address domain.PeerIdentity
	PubKey  domain.PeerPublicKey
	BoxKey  domain.PeerBoxKey
	BoxSig  domain.PeerBoxSignature
}

// MessageSentResult is the payload for TopicMessageSent.
type MessageSentResult struct {
	To      domain.PeerIdentity
	Body    string
	ReplyTo domain.MessageID // quoted message, empty if not a reply
}

// MessageSendFailedResult is the payload for TopicMessageSendFailed.
type MessageSendFailedResult struct {
	To  domain.PeerIdentity
	Err error
}

// FileSentResult is the payload for TopicFileSent.
type FileSentResult struct {
	To     domain.PeerIdentity
	FileID domain.FileID
}

// FileSendFailedResult is the payload for TopicFileSendFailed.
type FileSendFailedResult struct {
	To     domain.PeerIdentity
	FileID domain.FileID
	Err    error
}

// MessageDeleteOutcome is the payload for TopicMessageDeleteCompleted.
// Emitted by DMRouter when an in-flight message_delete reaches a
// terminal state — either the recipient's message_delete_ack arrived
// (Status carries one of the four ack statuses), or the sender's
// retry budget was exhausted (Abandoned == true). Subscribers
// (UI / RPC tooling) use this to differentiate a successful peer-side
// deletion from a denied / immutable rejection or a transport
// abandonment, all of which look identical at the wire level.
//
// Status is one of domain.MessageDeleteStatus values when Abandoned is
// false, and the empty string when Abandoned is true (no ack was ever
// received).
type MessageDeleteOutcome struct {
	Target    domain.MessageID
	Peer      domain.PeerIdentity
	Status    domain.MessageDeleteStatus
	Abandoned bool
	Attempts  int
}

// RouteTableChange is the payload for TopicRouteTableChanged.
// Carries a lightweight summary of what changed. Subscribers needing the
// full table state should call RoutingSnapshot().
type RouteTableChange struct {
	Reason    domain.RouteChangeReason // why the routing table changed
	PeerID    domain.PeerIdentity      // identity that triggered the change (if applicable)
	Accepted  int                      // number of routes accepted (for announcement batches)
	Withdrawn int                      // number of routes withdrawn
}

// PeerPendingDelta is the payload for TopicPeerPendingChanged.
// Carries the peer address and the current pending frame count after a
// queue mutation (enqueue, flush, or expiry).
type PeerPendingDelta struct {
	Address domain.PeerAddress
	Count   int
}

// PeerTrafficSnapshot holds cumulative byte counters for a single peer.
type PeerTrafficSnapshot struct {
	Address       domain.PeerAddress
	BytesSent     int64
	BytesReceived int64
}

// PeerTrafficBatch is the payload for TopicPeerTrafficUpdated.
// Carries a batch of per-peer snapshots published as a single event so
// subscribers apply all changes under one lock acquisition and issue one
// UI notification — avoiding the r.mu.Lock() contention storm that occurs
// when each peer delta triggers a separate notify()+buildSnapshotLocked.
type PeerTrafficBatch struct {
	Peers []PeerTrafficSnapshot
}

// CaptureSessionStarted is the payload for TopicCaptureSessionStarted.
// Emitted by the node-side capture bridge once the capture.Manager has
// accepted a start request for a specific connection. Carries the fields
// the monitor needs to populate a CaptureSession entry keyed by ConnID
// without an extra RPC round-trip. StartedAt is optional via pointer so
// that "unknown / not yet stamped" is distinguishable from an explicit
// zero time (domain rule: optional state must be visible from the type).
//
// Address/PeerID/Direction are identity metadata copied onto the stored
// CaptureSession so the UI can label the recording when the corresponding
// PeerHealth row does not yet exist. Capture sessions live in their own
// map on NodeStatus — the subscriber never materializes PeerHealth rows
// from capture events. Address may be empty when the publisher could not
// resolve the connection (already torn down between StartCapture and the
// publish, or never tracked); the session is still recorded because the
// writer is still active on the node side.
type CaptureSessionStarted struct {
	ConnID    domain.ConnID
	Address   domain.PeerAddress
	PeerID    domain.PeerIdentity
	Direction domain.PeerDirection
	FilePath  string
	StartedAt *time.Time
	Scope     domain.CaptureScope
	Format    domain.CaptureFormat
}

// CaptureSessionStopped is the payload for TopicCaptureSessionStopped.
// Emitted whenever a capture session terminates — explicit RPC stop,
// writer eviction on disk error, or the owning connection closing.
// Error and DroppedEvents carry the terminal diagnostic state so the UI
// can surface a failure reason even after the session row is cleared.
type CaptureSessionStopped struct {
	ConnID        domain.ConnID
	Error         string
	DroppedEvents int64
}
