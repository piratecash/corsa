package protocol

import (
	"encoding/json"
	"fmt"
	"sort"
)

// frame_route_poison_v2.go defines the BATCHED poison-reverse signal
// route_poison_v2, gated by the mesh_poison_reverse_v2 capability.
//
// Motivation. The v1 frame (frame_route_poison.go) carries a single
// identity, so poisonReverseToOtherPeers must emit one frame per
// (transit-identity × peer). When a well-connected uplink drops, the
// transit set can be dozens-to-hundreds of identities, producing a
// burst of hundreds of small frames per disconnect — each charging the
// announce rate-limiter, so the burst is partly throttled/dropped,
// which slows convergence and feeds further churn (observed: ~520
// frames in a single sub-second burst). v2 collapses the per-peer fan
// to ONE frame carrying the whole identity list: the same poison-reverse
// semantics (the receiver invalidates claims[identity][sender] for each
// listed identity), one frame per peer on the wire but one rate-limiter token
// per LISTED IDENTITY at the receiver (throttle-equivalent to the v1 fan-out —
// the win is far fewer frames, not a cheaper limiter cost).
//
// Same trust budget as v1 (see frame_route_poison.go): a single-hop
// neighbour signal, NOT an origin-authority claim. Reason is shared by
// the whole batch — poisonReverseToOtherPeers only ever emits
// uplink_lost, and a batch is by construction "these identities all lost
// the same uplink (the sender)".
//
// Sender attestation. SenderSig is the optional Ed25519 signature over
// CanonicalSenderSigBytes. To make the signature independent of wire
// array ordering, the canonical bytes sort the identities first; the
// verifier sorts the received list the same way before checking. Same
// Tier-2-lenient verification policy as v1 (present-and-invalid → drop;
// present-and-pubkey-unknown → accept; absent → accept).

// RoutePoisonV2FrameType is the type identifier for the batched
// poison-reverse signal.
const RoutePoisonV2FrameType = "route_poison_v2"

// maxRoutePoisonV2Identities bounds how many identities a single batch
// frame may carry, so a malformed/hostile frame cannot force unbounded
// work or memory at the receiver. Sized well above a realistic transit
// set for one uplink.
const maxRoutePoisonV2Identities = 4096

// RoutePoisonV2Frame is the wire form of the batched poison-reverse
// signal.
//
//	{
//	  "type":       "route_poison_v2",
//	  "identities": ["<fp1>", "<fp2>", ...],
//	  "reason":     "uplink_lost" | "health_dead" | "loop_detected",
//	  "issued_at":  "2026-06-15T15:00:00Z",
//	  "sender_sig": "<base64 of Ed25519 over canonical bytes>"
//	}
type RoutePoisonV2Frame struct {
	Type       string   `json:"type"`
	Identities []string `json:"identities"`
	Reason     string   `json:"reason"`
	IssuedAt   string   `json:"issued_at"`
	SenderSig  string   `json:"sender_sig,omitempty"`
}

// MarshalRoutePoisonV2Frame serialises a batched poison frame. Fills
// Type from the constant when blank; rejects an empty/oversized identity
// list, an empty identity element, or an unrecognised reason so caller
// bugs surface at marshal time rather than as on-wire garbage.
func MarshalRoutePoisonV2Frame(f RoutePoisonV2Frame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RoutePoisonV2FrameType
	}
	if err := validateRoutePoisonV2(f); err != nil {
		return nil, err
	}
	return json.Marshal(f)
}

// UnmarshalRoutePoisonV2Frame deserialises and validates a batched
// poison frame. SenderSig is NOT checked for emptiness here — the
// receive handler applies the Tier-2-lenient verification policy.
func UnmarshalRoutePoisonV2Frame(data []byte) (RoutePoisonV2Frame, error) {
	var f RoutePoisonV2Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return RoutePoisonV2Frame{}, fmt.Errorf("unmarshal route poison v2 frame: %w", err)
	}
	if f.Type != RoutePoisonV2FrameType {
		return RoutePoisonV2Frame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RoutePoisonV2FrameType)
	}
	if err := validateRoutePoisonV2(f); err != nil {
		return RoutePoisonV2Frame{}, err
	}
	return f, nil
}

func validateRoutePoisonV2(f RoutePoisonV2Frame) error {
	if len(f.Identities) == 0 {
		return fmt.Errorf("route poison v2 frame: empty identities")
	}
	if len(f.Identities) > maxRoutePoisonV2Identities {
		return fmt.Errorf("route poison v2 frame: %d identities exceeds cap %d", len(f.Identities), maxRoutePoisonV2Identities)
	}
	for i, id := range f.Identities {
		if id == "" {
			return fmt.Errorf("route poison v2 frame: empty identity at index %d", i)
		}
	}
	if !validRoutePoisonReason(f.Reason) {
		return fmt.Errorf("route poison v2 frame: invalid reason %q", f.Reason)
	}
	return nil
}

// CanonicalSenderSigBytes returns the deterministic byte string SenderSig
// covers: length-prefixed (reason || issued_at || SORTED identities).
// Sorting makes the signature independent of the wire array order, so a
// frame whose identities are reordered in transit still verifies (and
// the verifier sorts the received list identically). Length-prefixing
// each field makes the concatenation unambiguous, matching the v1 /
// route_announce_v3 canonical-bytes convention.
func (f RoutePoisonV2Frame) CanonicalSenderSigBytes() []byte {
	return canonicalRoutePoisonV2Bytes(f.Identities, f.Reason, f.IssuedAt)
}

func canonicalRoutePoisonV2Bytes(identities []string, reason, issuedAt string) []byte {
	sorted := append([]string(nil), identities...)
	sort.Strings(sorted)
	out := make([]byte, 0, 16+len(reason)+len(issuedAt)+len(sorted)*48)
	out = appendLengthPrefixed(out, []byte(reason))
	out = appendLengthPrefixed(out, []byte(issuedAt))
	out = appendUint64(out, uint64(len(sorted)))
	for _, id := range sorted {
		out = appendLengthPrefixed(out, []byte(id))
	}
	return out
}
