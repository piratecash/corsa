package protocol

import (
	"encoding/json"
	"fmt"
)

// frame_route_sync.go defines the wire format for the Phase 3 PR 12.5
// route_sync_digest_v1 / route_sync_summary_v1 control frames
// (docs/cluster-mesh/phase-3-multipath-reputation.md §4.5, overview
// §2.2 capability list extension).
//
// Both frames travel through the existing Frame dispatcher via the
// RawLine bypass — the same isolation pattern FileCommandFrame and
// the Phase 2 route_probe/route_query frames use. This keeps each
// capability-gated wire shape compile-time isolated: when Phase 6
// removes legacy capability-gated frames, removal is a file delete
// rather than a sweep through the universal Frame struct.
//
// Wire isolation rules (CLAUDE.md, phase-3 §2.4):
//   - digest / summary are P2P wire commands gated by
//     mesh_route_sync_v1; they must not blur with RPC paths and
//     never reach the CommandTable surface.
//   - Send-side uses Frame.Type + Frame.RawLine, so the writer
//     goroutine emits the canonical JSON line verbatim;
//     receive-side parses RawLine with UnmarshalRouteSync*Frame.
//   - The digest itself is a local-only HINT, not authoritative
//     state. A malicious peer claiming match=true on a mismatched
//     digest only causes us to skip ONE forced full-sync window;
//     the next delta cycle restores correctness. No trust is
//     transferred through this exchange — the announce plane
//     remains the source of truth for routing state.

// RouteSyncDigestFrameType is the type identifier used in the unified
// Frame dispatcher for outgoing digest announcements.
const RouteSyncDigestFrameType = "route_sync_digest_v1"

// RouteSyncSummaryFrameType is the type identifier used for the
// response to a route_sync_digest_v1 frame.
const RouteSyncSummaryFrameType = "route_sync_summary_v1"

// RouteSyncDigestFrame is the wire form of a Phase 3 incremental-sync
// digest announcement. On reconnect to a peer the sender emits the
// digest of (Identity, MaxSeqNo) pairs it last observed THROUGH that
// peer (split-horizon: only Identities whose claim.Uplink matches the
// peer go into the hash). The receiver re-computes the same digest
// over its current view and replies with route_sync_summary_v1.
//
// Wire layout (JSON):
//
//	{
//	  "type":                   "route_sync_digest_v1",
//	  "digest":                 "<hex-sha256>",
//	  "known_identities_count": 42,
//	  "generated_at":           "2026-05-23T12:00:00Z"
//	}
//
// Digest is a deterministic hex-encoded sha256 over the
// canonicalised (Identity, MaxSeqNo) tuples — see
// routing.ComputeSyncDigest for the exact construction. The hash
// itself carries no secret material; sha256 is chosen for
// determinism and CPU cost (≤1 ms for 100 entries) rather than
// any cryptographic property.
//
// KnownIdentitiesCount is the number of distinct identities the
// sender folded into the digest. Carried as a sanity-check signal
// for diagnostics — receivers compare against their own count to
// distinguish "different content, same shape" from "different
// shape entirely".
//
// GeneratedAt is the sender-local timestamp when the digest was
// computed, RFC3339 in UTC. Used by the receiver as a freshness
// hint in structured logs; not consulted for the match decision
// itself.
type RouteSyncDigestFrame struct {
	Type                 string `json:"type"`
	Digest               string `json:"digest"`
	KnownIdentitiesCount uint32 `json:"known_identities_count"`
	GeneratedAt          string `json:"generated_at"`
}

// RouteSyncSummaryFrame is the wire form of the digest comparison
// reply. The responder echoes the original digest so the sender can
// correlate the summary with its outstanding request (no other
// correlation ID is needed — a peer pair is single-channel and the
// sender only issues at most one digest per reconnect).
//
// Wire layout (JSON):
//
//	{
//	  "type":             "route_sync_summary_v1",
//	  "digest":           "<hex-sha256>",
//	  "match":            true,
//	  "expect_full_sync": false
//	}
//
// Match is true when the responder's locally-computed digest over
// its current view (through the sender peer) equals the digest in
// the request. ExpectFullSync mirrors !Match for now and is reserved
// so future protocol revisions can distinguish "mismatch — please
// resync" from "match but I'd like a full sync anyway" without
// adding another wire field.
type RouteSyncSummaryFrame struct {
	Type           string `json:"type"`
	Digest         string `json:"digest"`
	Match          bool   `json:"match"`
	ExpectFullSync bool   `json:"expect_full_sync"`
}

// MarshalRouteSyncDigestFrame serialises a RouteSyncDigestFrame to
// JSON bytes intended for Frame.RawLine. The Type field is filled
// from the constant when blank so call sites do not need to repeat
// it on every emit.
func MarshalRouteSyncDigestFrame(f RouteSyncDigestFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteSyncDigestFrameType
	}
	return json.Marshal(f)
}

// UnmarshalRouteSyncDigestFrame deserialises a RouteSyncDigestFrame
// from JSON bytes. Returns an error if Type does not match
// RouteSyncDigestFrameType or if Digest is empty — both are required
// for the receive path to compute a meaningful match.
func UnmarshalRouteSyncDigestFrame(data []byte) (RouteSyncDigestFrame, error) {
	var f RouteSyncDigestFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteSyncDigestFrame{}, fmt.Errorf("unmarshal route sync digest frame: %w", err)
	}
	if f.Type != RouteSyncDigestFrameType {
		return RouteSyncDigestFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteSyncDigestFrameType)
	}
	if f.Digest == "" {
		return RouteSyncDigestFrame{}, fmt.Errorf("route sync digest frame: empty digest")
	}
	return f, nil
}

// MarshalRouteSyncSummaryFrame serialises a RouteSyncSummaryFrame to
// JSON bytes for placement into Frame.RawLine.
func MarshalRouteSyncSummaryFrame(f RouteSyncSummaryFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteSyncSummaryFrameType
	}
	return json.Marshal(f)
}

// UnmarshalRouteSyncSummaryFrame deserialises a RouteSyncSummaryFrame
// from JSON bytes. Returns an error if Type does not match
// RouteSyncSummaryFrameType. The Digest field is NOT validated for
// emptiness here because a future protocol revision may legitimately
// omit it (e.g. a peer signalling capability disablement); current
// senders always include it, and the receive handler treats an empty
// digest as "no correlation, drop the suppression update" rather than
// as a parse error.
func UnmarshalRouteSyncSummaryFrame(data []byte) (RouteSyncSummaryFrame, error) {
	var f RouteSyncSummaryFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteSyncSummaryFrame{}, fmt.Errorf("unmarshal route sync summary frame: %w", err)
	}
	if f.Type != RouteSyncSummaryFrameType {
		return RouteSyncSummaryFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteSyncSummaryFrameType)
	}
	return f, nil
}
