package protocol

import (
	"encoding/json"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
)

// frame_route_probe.go defines the wire format for the Phase 2
// route_probe_v1 / route_probe_ack_v1 control frames
// (docs/protocol/route_health.md, overview §7.6).
//
// Both frames travel through the existing Frame dispatcher via the
// RawLine bypass — the same isolation pattern FileCommandFrame uses —
// so the Frame god-object is not extended with capability-gated
// fields. This keeps the frame schema compile-time isolated: when
// Phase 6 removes legacy capability-gated frames, removal is a file
// delete rather than a sweep through the universal Frame struct.
//
// Wire isolation rules (CLAUDE.md, phase-2 §2.5):
//   - probe / probe_ack are P2P wire commands gated by
//     mesh_route_probe_v1; they must not blur with RPC paths.
//   - Send-side uses Frame.Type + Frame.RawLine, so the writer goroutine
//     emits the canonical JSON line verbatim; receive-side parses
//     RawLine with UnmarshalRouteProbeFrame / UnmarshalRouteProbeAckFrame.
//   - Sender ignores the `rtt_ms` field on incoming acks for ranking
//     decisions — RTT is measured locally as the probe-send-to-ack-
//     receive round-trip (zero-trust budget — overview §4.2). The
//     `rtt_ms` field carries the responder's local estimate as
//     informational only.

// RouteProbeFrameType is the type identifier used in the unified Frame
// dispatcher for outgoing reachability probes.
const RouteProbeFrameType = "route_probe_v1"

// RouteProbeAckFrameType is the type identifier used for the response
// to a route_probe_v1 frame.
const RouteProbeAckFrameType = "route_probe_ack_v1"

// RouteProbeFrame is the wire form of a Phase 2 reachability probe.
// A node sends one to a directly connected peer to verify that the
// peer can still reach a specific target identity through whatever
// transit path it currently has. The probe is single-hop — recipients
// do NOT forward it; they answer with a route_probe_ack_v1 derived
// from their own routing.Table.Lookup(TargetIdentity).
//
// Wire layout (JSON):
//
//	{
//	  "type":            "route_probe_v1",
//	  "probe_id":        12345678,
//	  "target_identity": "<X_fp>"
//	}
//
// ProbeID is a sender-chosen opaque identifier used to correlate the
// ack with the original send. Zero is permitted on the wire but
// senders are expected to generate non-zero IDs for outstanding-probe
// bookkeeping.
type RouteProbeFrame struct {
	Type           string              `json:"type"`
	ProbeID        uint64              `json:"probe_id"`
	TargetIdentity domain.PeerIdentity `json:"target_identity"`
}

// RouteProbeAckFrame is the wire form of a probe response.
//
// Wire layout (JSON):
//
//	{
//	  "type":       "route_probe_ack_v1",
//	  "probe_id":   12345678,
//	  "reachable":  true,
//	  "rtt_ms":     45
//	}
//
// Reachable mirrors the responder's local routing.Table.Lookup
// outcome — true when a non-Dead route to the queried target exists,
// false otherwise. RTTMs is the responder's local RTT estimate
// (best-effort, informational); senders MUST NOT trust it for their
// own ranking decisions and instead measure RTT from the probe send
// timestamp to the ack receive timestamp.
type RouteProbeAckFrame struct {
	Type      string `json:"type"`
	ProbeID   uint64 `json:"probe_id"`
	Reachable bool   `json:"reachable"`
	RTTMs     uint32 `json:"rtt_ms"`
}

// MarshalRouteProbeFrame serialises a RouteProbeFrame to JSON bytes.
// The bytes are intended to be placed into Frame.RawLine by the
// sender; the writer goroutine emits the line verbatim.
func MarshalRouteProbeFrame(f RouteProbeFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteProbeFrameType
	}
	return json.Marshal(f)
}

// UnmarshalRouteProbeFrame deserialises a RouteProbeFrame from JSON
// bytes. Returns an error if Type does not match RouteProbeFrameType
// or if TargetIdentity is empty — both are required for the receive
// path to be able to dispatch the probe.
func UnmarshalRouteProbeFrame(data []byte) (RouteProbeFrame, error) {
	var f RouteProbeFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteProbeFrame{}, fmt.Errorf("unmarshal route probe frame: %w", err)
	}
	if f.Type != RouteProbeFrameType {
		return RouteProbeFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteProbeFrameType)
	}
	if f.TargetIdentity == "" {
		return RouteProbeFrame{}, fmt.Errorf("route probe frame: empty target_identity")
	}
	return f, nil
}

// MarshalRouteProbeAckFrame serialises a RouteProbeAckFrame to JSON
// bytes for placement into Frame.RawLine.
func MarshalRouteProbeAckFrame(f RouteProbeAckFrame) ([]byte, error) {
	if f.Type == "" {
		f.Type = RouteProbeAckFrameType
	}
	return json.Marshal(f)
}

// UnmarshalRouteProbeAckFrame deserialises a RouteProbeAckFrame from
// JSON bytes. Returns an error if Type does not match
// RouteProbeAckFrameType.
//
// ProbeID is not validated against zero — a sender that emits
// probe_id=0 receives an ack with probe_id=0; bookkeeping happens
// elsewhere.
func UnmarshalRouteProbeAckFrame(data []byte) (RouteProbeAckFrame, error) {
	var f RouteProbeAckFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return RouteProbeAckFrame{}, fmt.Errorf("unmarshal route probe ack frame: %w", err)
	}
	if f.Type != RouteProbeAckFrameType {
		return RouteProbeAckFrame{}, fmt.Errorf("unexpected frame type %q, expected %q", f.Type, RouteProbeAckFrameType)
	}
	return f, nil
}
