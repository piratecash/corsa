package protocol

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestRouteProbeFrame_MarshalUnmarshalRoundTrip — Marshal+Unmarshal
// must produce the exact original frame. Phase 2 wire-protocol
// stability hinges on this for mixed-version interop.
func TestRouteProbeFrame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RouteProbeFrame{
		Type:           RouteProbeFrameType,
		ProbeID:        12345678,
		TargetIdentity: domaintest.ID("alice-fp"),
	}
	data, err := MarshalRouteProbeFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteProbeFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

// TestRouteProbeFrame_MarshalSetsTypeWhenEmpty — sender code that
// forgets to set Type explicitly must still produce a valid frame
// because Marshal auto-fills the canonical type string.
func TestRouteProbeFrame_MarshalSetsTypeWhenEmpty(t *testing.T) {
	orig := RouteProbeFrame{ProbeID: 1, TargetIdentity: domaintest.ID("x")}
	data, err := MarshalRouteProbeFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"type":"`+RouteProbeFrameType+`"`) {
		t.Fatalf("marshalled bytes do not contain expected type: %s", data)
	}
}

// TestRouteProbeFrame_UnmarshalRejectsWrongType — defence-in-depth:
// receive path must refuse a frame mislabelled as something else.
// Production dispatcher already type-routes via Frame.Type, so this
// guards against accidental misuse of the helper.
func TestRouteProbeFrame_UnmarshalRejectsWrongType(t *testing.T) {
	bad := []byte(`{"type":"announce_routes","probe_id":1,"target_identity":"x"}`)
	if _, err := UnmarshalRouteProbeFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted wrong type, want error")
	}
}

// TestRouteProbeFrame_UnmarshalRejectsEmptyTarget — TargetIdentity is
// required for the receiver to dispatch the probe; the unmarshal
// helper rejects frames without it.
func TestRouteProbeFrame_UnmarshalRejectsEmptyTarget(t *testing.T) {
	bad := []byte(`{"type":"` + RouteProbeFrameType + `","probe_id":1}`)
	if _, err := UnmarshalRouteProbeFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted empty target_identity, want error")
	}
}

// TestRouteProbeFrame_UnmarshalRejectsMalformedJSON — malformed JSON
// is a structural error, not a semantic one.
func TestRouteProbeFrame_UnmarshalRejectsMalformedJSON(t *testing.T) {
	if _, err := UnmarshalRouteProbeFrame([]byte(`{not json`)); err == nil {
		t.Fatal("Unmarshal accepted malformed JSON, want error")
	}
}

// TestRouteProbeAckFrame_MarshalUnmarshalRoundTrip mirrors the
// request-side round-trip test.
func TestRouteProbeAckFrame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RouteProbeAckFrame{
		Type:      RouteProbeAckFrameType,
		ProbeID:   12345678,
		Reachable: true,
		RTTMs:     45,
	}
	data, err := MarshalRouteProbeAckFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteProbeAckFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

// TestRouteProbeAckFrame_MarshalSetsTypeWhenEmpty mirrors the
// auto-fill behaviour for ack frames.
func TestRouteProbeAckFrame_MarshalSetsTypeWhenEmpty(t *testing.T) {
	orig := RouteProbeAckFrame{ProbeID: 1, Reachable: false}
	data, err := MarshalRouteProbeAckFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"type":"`+RouteProbeAckFrameType+`"`) {
		t.Fatalf("marshalled bytes do not contain expected type: %s", data)
	}
}

// TestRouteProbeAckFrame_UnmarshalRejectsWrongType mirrors the
// request-side guard.
func TestRouteProbeAckFrame_UnmarshalRejectsWrongType(t *testing.T) {
	bad := []byte(`{"type":"route_probe_v1","probe_id":1,"reachable":true,"rtt_ms":10}`)
	if _, err := UnmarshalRouteProbeAckFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted wrong type, want error")
	}
}

// TestRouteProbeAckFrame_UnreachableHasZeroRTT — receivers that emit
// reachable=false typically have no useful RTT estimate. The wire
// layout permits omitting the field (default 0); senders must not
// trust it for ranking anyway (zero-trust budget). This test just
// confirms the JSON shape is stable under the omitempty-less encoding.
func TestRouteProbeAckFrame_UnreachableHasZeroRTT(t *testing.T) {
	orig := RouteProbeAckFrame{
		ProbeID:   42,
		Reachable: false,
	}
	data, err := MarshalRouteProbeAckFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteProbeAckFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.RTTMs != 0 {
		t.Fatalf("RTTMs = %d, want 0", got.RTTMs)
	}
	if got.Reachable {
		t.Fatal("Reachable = true, want false")
	}
}

// TestRouteProbeFrame_ZeroProbeIDAllowed — probe_id=0 is permitted on
// the wire. Sender bookkeeping is expected to use non-zero IDs but
// the schema does not enforce this; receivers must echo whatever ID
// arrived.
func TestRouteProbeFrame_ZeroProbeIDAllowed(t *testing.T) {
	orig := RouteProbeFrame{
		Type:           RouteProbeFrameType,
		ProbeID:        0,
		TargetIdentity: domaintest.ID("x"),
	}
	data, err := MarshalRouteProbeFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteProbeFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ProbeID != 0 {
		t.Fatalf("ProbeID = %d, want 0 (zero is permitted)", got.ProbeID)
	}
}
