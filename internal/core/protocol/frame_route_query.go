package protocol

import (
	"encoding/json"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
)

// frame_route_query.go defines the wire format for the Phase 2
// route_query_v1 / route_query_response_v1 control frames
// (docs/protocol/route_health.md, overview §7.5).
//
// Targeted single-hop route query: when all known uplinks for an
// identity transition to Bad/Dead, the sender asks each directly
// connected peer for its best route to the target. The peer answers
// from its local routing.Table.Lookup. Both frames travel through
// the existing Frame dispatcher via the RawLine bypass — same
// isolation pattern as FileCommandFrame and RouteProbeFrame so that
// capability-gated Phase 2 frames stay compile-time isolated.
//
// Schema rationale (overview §7.5, phase-2 §4.7 Resolved decision #2):
//
//   - Single-best response payload (best_uplink + best_hops +
//     best_seq_no + found), NOT a routes array. Receiver applies
//     one claim through the standard RouteSourceAnnouncement
//     admission path. The original roadmap iter-1.5 sketch used a
//     routes array; that was deliberately replaced because per-uplink
//     storage (Phase 1) makes the array harder to ingest atomically
//     and a single-best fan-out across 3 peers (rate-limited 3 per
//     target per 30s) gives the same multi-path coverage.
//
//   - "found" boolean covers the no-known-route case. When false,
//     the best_* fields are zero-valued and the receiver does not
//     attempt to ingest a synthetic claim.
//
// Architectural invariants (CLAUDE.md, phase-2 §2.5):
//
//   - Both frames are P2P wire commands gated by
//     mesh_route_query_v1. They never appear on RPC surface — the
//     CommandTable holds only observability commands. UI / desktop
//     console use fetchRouteHealth (PR 11.5) for snapshots.
//   - Response is single-hop only; receivers MUST NOT forward
//     route_query_v1 to other peers (anti-flood guard).

// RouteQueryFrameType is the type identifier used in the unified
// Frame dispatcher for outgoing targeted route queries.
const RouteQueryFrameType = "route_query_v1"

// RouteQueryResponseFrameType is the type identifier used for the
// response to a route_query_v1 frame.
const RouteQueryResponseFrameType = "route_query_response_v1"

// RouteQueryFrame is the wire form of a Phase 2 targeted route
// query. A node sends one to each directly connected peer that has
// negotiated mesh_route_query_v1 when its own routing.Table has no
// usable route to TargetIdentity (all known uplinks Bad or Dead).
//
// Wire layout (JSON):
//
//	{
//	  "type":            "route_query_v1",
//	  "query_id":        87654321,
//	  "target_identity": "<X_fp>",
//	  "max_hops":        8,
//	  "issued_at":       "2026-05-23T12:00:00Z"
//	}
//
// QueryID is a sender-chosen opaque identifier used to correlate
// the response with the original send. MaxHops bounds the depth of
// routes the responder may consider (responder still does only a
// single-hop Lookup; MaxHops is informational). IssuedAt is an
// RFC3339 timestamp for freshness — responder MAY reject queries
// with implausible drift.
type RouteQueryFrame struct {
	Type           string              `json:"type"`
	QueryID        uint64              `json:"query_id"`
	TargetIdentity domain.PeerIdentity `json:"target_identity"`
	MaxHops        uint8               `json:"max_hops,omitempty"`
	IssuedAt       string              `json:"issued_at,omitempty"`
}

// RouteQueryResponseFrame is the wire form of a query response.
//
// Wire layout (JSON):
//
//	{
//	  "type":            "route_query_response_v1",
//	  "query_id":        87654321,
//	  "target_identity": "<X_fp>",
//	  "found":           true,
//	  "best_uplink":     "<peer_fp>",
//	  "best_hops":       2,
//	  "best_seq_no":     55,
//	  "issued_at":       "2026-05-23T12:00:01Z"
//	}
//
// Found=false zeroes all best_* fields; the receiver does not
// ingest a synthetic claim in that case. Found=true means the
// responder's local Lookup returned at least one non-Dead route to
// TargetIdentity; the chosen claim is the highest-ranked one per
// the responder's CompositeScore — receiver does NOT re-rank.
//
// Trust semantic (phase-2 §4.4 Resolved decision #2): the response
// is ingested as RouteSourceAnnouncement. Composite score gives no
// trust bonus for that source, matching the "claim from a peer
// we don't directly control" semantic of regular announce_routes.
// The standard withdrawal anti-spoof and per-Origin admission
// rules apply unchanged.
type RouteQueryResponseFrame struct {
	Type           string              `json:"type"`
	QueryID        uint64              `json:"query_id"`
	TargetIdentity domain.PeerIdentity `json:"target_identity"`
	Found          bool                `json:"found"`
	BestUplink     domain.PeerIdentity `json:"best_uplink,omitempty"`
	BestHops       uint8               `json:"best_hops,omitempty"`
	BestSeqNo      uint64              `json:"best_seq_no,omitempty"`
	IssuedAt       string              `json:"issued_at,omitempty"`
}

// MarshalRouteQueryFrame serialises a RouteQueryFrame to JSON bytes.
// Type is auto-filled when empty so senders that forget the literal
// still produce a valid frame.
func MarshalRouteQueryFrame(f RouteQueryFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteQueryFrameType
	}
	return json.Marshal(f)
}

// UnmarshalRouteQueryFrame deserialises a RouteQueryFrame from JSON
// bytes. Rejects frames with the wrong Type or empty
// TargetIdentity — both are required for the receive path to
// dispatch the query.
func UnmarshalRouteQueryFrame(data []byte) (RouteQueryFrame, error) {
	var f RouteQueryFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteQueryFrame{}, fmt.Errorf("unmarshal route query frame: %w", err)
	}
	if f.Type != RouteQueryFrameType {
		return RouteQueryFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteQueryFrameType)
	}
	if f.TargetIdentity == "" {
		return RouteQueryFrame{}, fmt.Errorf("route query frame: empty target_identity")
	}
	return f, nil
}

// MarshalRouteQueryResponseFrame serialises a RouteQueryResponseFrame
// to JSON bytes for placement into Frame.RawLine.
func MarshalRouteQueryResponseFrame(f RouteQueryResponseFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteQueryResponseFrameType
	}
	return json.Marshal(f)
}

// UnmarshalRouteQueryResponseFrame deserialises a
// RouteQueryResponseFrame from JSON bytes. Rejects frames with the
// wrong Type. Receivers ingest the payload only when Found=true;
// best_* validation (non-empty BestUplink etc.) is up to the
// caller because some receivers may want to log Found=false
// responses without rejecting them.
func UnmarshalRouteQueryResponseFrame(data []byte) (RouteQueryResponseFrame, error) {
	var f RouteQueryResponseFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteQueryResponseFrame{}, fmt.Errorf("unmarshal route query response frame: %w", err)
	}
	if f.Type != RouteQueryResponseFrameType {
		return RouteQueryResponseFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteQueryResponseFrameType)
	}
	if f.TargetIdentity == "" {
		return RouteQueryResponseFrame{}, fmt.Errorf("route query response frame: empty target_identity")
	}
	return f, nil
}
