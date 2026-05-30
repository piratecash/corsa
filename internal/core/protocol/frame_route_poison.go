package protocol

import (
	"encoding/json"
	"fmt"
)

// frame_route_poison.go defines the wire format for the Phase 4
// explicit poison-reverse signal route_poison_v1
// (docs/cluster-mesh/phase-4-compact-wire-signed.md §3.3, overview
// §7.7), gated by the mesh_poison_reverse_v1 capability.
//
// Like the other Phase 4 capability-gated frames (route_announce_v3,
// route_sync_*, route_probe_*, route_query_*), route_poison_v1
// travels through the Frame dispatcher via the Frame.Type +
// Frame.RawLine bypass rather than as a new field on the universal
// Frame struct. This keeps each capability-gated wire shape
// compile-time isolated; future cleanup is a file delete rather
// than a sweep through the shared Frame struct.
//
// Trust budget (overview §4.2 / §7.7). route_poison_v1 is a
// SINGLE-HOP signal from one peer to its direct neighbour, NOT an
// origin-authority claim. The receiver invalidates ONLY its
// claims[identity][sender] slot — claims for the same identity via
// any other uplink, and the origin's own claim, are untouched.
// This bounds the damage a misbehaving peer can do: it can poison
// the receiver's view of routes through itself (which the peer
// could have done anyway by simply going silent until TTL expiry),
// but cannot spoof an origin withdrawal. Per-Identity SeqNo
// monotonicity governs origin-side updates and is orthogonal to
// poison.
//
// Sender attestation. SenderSig is the optional Ed25519 signature of
// the sending identity over CanonicalSenderSigBytes (identity ||
// reason || issued_at). Because the sender is also the peer at the
// other end of the TCP session, the receiver already knows the
// sender's identity from the session handshake — the signature is
// defence-in-depth (it covers offline transports or future
// store-and-forward delivery where the session-level identity
// binding might not hold). Verification policy: present-and-invalid
// → drop frame; present-and-pubkey-unknown → accept (Tier-2 lenient,
// mirroring mesh_attested_links_v1); absent → accept. Signing and
// verification land in 13.3-A.

// RoutePoisonFrameType is the type identifier used in the unified
// Frame dispatcher for the Phase 4 poison-reverse signal.
const RoutePoisonFrameType = "route_poison_v1"

// RoutePoisonReason values for RoutePoisonFrame.Reason. The receiver
// MAY use the reason for diagnostics / health-score reputation but
// MUST NOT base routing decisions on it beyond invalidating the
// claim — an attacker who controls the sending peer can fabricate
// any reason value, so the only ground truth is "sender no longer
// vouches for the route through it".
const (
	RoutePoisonReasonUplinkLost   = "uplink_lost"
	RoutePoisonReasonHealthDead   = "health_dead"
	RoutePoisonReasonLoopDetected = "loop_detected"
)

// RoutePoisonFrame is the wire form of the Phase 4 single-hop
// poison-reverse signal.
//
// Wire layout (JSON):
//
//	{
//	  "type":       "route_poison_v1",
//	  "identity":   "<destination_fp>",
//	  "reason":     "uplink_lost" | "health_dead" | "loop_detected",
//	  "issued_at":  "2026-05-28T12:00:00Z",
//	  "sender_sig": "<base64 of Ed25519 over canonical bytes>"
//	}
//
// IssuedAt is the sender-local RFC3339 timestamp; it carries no
// routing decision weight (per-Identity SeqNo monotonicity owns
// liveness ordering for origin claims) and is primarily a
// diagnostic / log-correlation hint plus a freshness binding into
// the signed canonical bytes so a captured-then-replayed frame
// becomes detectable when paired with a sender-side replay-window
// policy in a future revision.
type RoutePoisonFrame struct {
	Type      string `json:"type"`
	Identity  string `json:"identity"`
	Reason    string `json:"reason"`
	IssuedAt  string `json:"issued_at"`
	SenderSig string `json:"sender_sig,omitempty"`
}

// validRoutePoisonReason reports whether reason is one of the three
// recognised values. An unknown reason is rejected on parse so a
// future-version sender introducing a new reason cannot accidentally
// poison a current-version receiver under a label the receiver does
// not understand.
func validRoutePoisonReason(reason string) bool {
	switch reason {
	case RoutePoisonReasonUplinkLost, RoutePoisonReasonHealthDead, RoutePoisonReasonLoopDetected:
		return true
	default:
		return false
	}
}

// MarshalRoutePoisonFrame serialises a RoutePoisonFrame to JSON
// bytes intended for Frame.RawLine. The Type field is filled from
// the constant when blank; an unrecognised Reason is rejected
// because the receive side will drop it anyway and forcing the
// error at marshal time surfaces caller bugs rather than producing
// silent on-wire garbage.
func MarshalRoutePoisonFrame(f RoutePoisonFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RoutePoisonFrameType
	}
	if f.Identity == "" {
		return nil, fmt.Errorf("route poison frame: empty identity")
	}
	if !validRoutePoisonReason(f.Reason) {
		return nil, fmt.Errorf("route poison frame: invalid reason %q", f.Reason)
	}
	return json.Marshal(f)
}

// UnmarshalRoutePoisonFrame deserialises a RoutePoisonFrame from JSON
// bytes. Returns an error if Type does not match RoutePoisonFrameType,
// Identity is empty, or Reason is not one of the recognised values.
// SenderSig is NOT validated for emptiness here — the receive handler
// distinguishes the present-vs-absent case and applies the
// Tier-2-lenient verification policy documented at the file header.
func UnmarshalRoutePoisonFrame(data []byte) (RoutePoisonFrame, error) {
	var f RoutePoisonFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RoutePoisonFrame{}, fmt.Errorf("unmarshal route poison frame: %w", err)
	}
	if f.Type != RoutePoisonFrameType {
		return RoutePoisonFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RoutePoisonFrameType)
	}
	if f.Identity == "" {
		return RoutePoisonFrame{}, fmt.Errorf("route poison frame: empty identity")
	}
	if !validRoutePoisonReason(f.Reason) {
		return RoutePoisonFrame{}, fmt.Errorf("route poison frame: invalid reason %q", f.Reason)
	}
	return f, nil
}

// CanonicalSenderSigBytes returns the deterministic byte string that
// SenderSig covers: length-prefixed concatenation of
// (identity || reason || issued_at). Same length-prefixing rationale
// as RouteAnnounceV3Entry.CanonicalSigningBytes — JSON is not
// canonical (key order, whitespace vary by encoder); length-prefixed
// binary makes signatures byte-reproducible across signer / verifier
// implementations and future protocol versions.
func (f RoutePoisonFrame) CanonicalSenderSigBytes() []byte {
	return canonicalRoutePoisonBytes(f.Identity, f.Reason, f.IssuedAt)
}

// canonicalRoutePoisonBytes is the field-level core of
// CanonicalSenderSigBytes, split out so the signer can build the
// canonical bytes from raw fields without constructing a full
// frame.
func canonicalRoutePoisonBytes(identity, reason, issuedAt string) []byte {
	idBytes := []byte(identity)
	rsnBytes := []byte(reason)
	issBytes := []byte(issuedAt)
	out := make([]byte, 0, 8+len(idBytes)+8+len(rsnBytes)+8+len(issBytes))
	out = appendLengthPrefixed(out, idBytes)
	out = appendLengthPrefixed(out, rsnBytes)
	out = appendLengthPrefixed(out, issBytes)
	return out
}
