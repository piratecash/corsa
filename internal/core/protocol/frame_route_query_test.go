package protocol

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestRouteQueryFrame_MarshalUnmarshalRoundTrip — Marshal+Unmarshal
// must produce the exact original frame, mirroring the round-trip
// invariant from the probe-frame tests.
func TestRouteQueryFrame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RouteQueryFrame{
		Type:           RouteQueryFrameType,
		QueryID:        87654321,
		TargetIdentity: domaintest.ID("alice-fp"),
		MaxHops:        8,
		IssuedAt:       "2026-05-23T12:00:00Z",
	}
	data, err := MarshalRouteQueryFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteQueryFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

// TestRouteQueryFrame_MarshalSetsTypeWhenEmpty — auto-fill behaviour
// for senders that forget the literal type string.
func TestRouteQueryFrame_MarshalSetsTypeWhenEmpty(t *testing.T) {
	orig := RouteQueryFrame{QueryID: 1, TargetIdentity: domaintest.ID("x")}
	data, err := MarshalRouteQueryFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"type":"`+RouteQueryFrameType+`"`) {
		t.Fatalf("marshalled bytes do not contain expected type: %s", data)
	}
}

// TestRouteQueryFrame_UnmarshalRejectsWrongType — defence-in-depth
// guard: a frame mislabelled as something else must not deserialise
// into a RouteQueryFrame.
func TestRouteQueryFrame_UnmarshalRejectsWrongType(t *testing.T) {
	bad := []byte(`{"type":"announce_routes","query_id":1,"target_identity":"x"}`)
	if _, err := UnmarshalRouteQueryFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted wrong type, want error")
	}
}

// TestRouteQueryFrame_UnmarshalRejectsEmptyTarget — target_identity
// is required for the receive path to dispatch.
func TestRouteQueryFrame_UnmarshalRejectsEmptyTarget(t *testing.T) {
	bad := []byte(`{"type":"` + RouteQueryFrameType + `","query_id":1}`)
	if _, err := UnmarshalRouteQueryFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted empty target_identity, want error")
	}
}

// TestRouteQueryFrame_UnmarshalRejectsMalformedJSON — structural
// error, not semantic.
func TestRouteQueryFrame_UnmarshalRejectsMalformedJSON(t *testing.T) {
	if _, err := UnmarshalRouteQueryFrame([]byte(`{not json`)); err == nil {
		t.Fatal("Unmarshal accepted malformed JSON, want error")
	}
}

// TestRouteQueryResponseFrame_MarshalUnmarshalRoundTripFound —
// happy-path round-trip with Found=true and populated best_* fields.
func TestRouteQueryResponseFrame_MarshalUnmarshalRoundTripFound(t *testing.T) {
	orig := RouteQueryResponseFrame{
		Type:           RouteQueryResponseFrameType,
		QueryID:        87654321,
		TargetIdentity: domaintest.ID("alice-fp"),
		Found:          true,
		BestUplink:     domaintest.ID("uplink-fp"),
		BestHops:       2,
		BestSeqNo:      55,
		IssuedAt:       "2026-05-23T12:00:01Z",
	}
	data, err := MarshalRouteQueryResponseFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteQueryResponseFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

// TestRouteQueryResponseFrame_FoundFalseHasEmptyBestFields — the
// no-known-route case zeroes best_* fields. omitempty on the JSON
// tags keeps the wire compact for the common case.
func TestRouteQueryResponseFrame_FoundFalseHasEmptyBestFields(t *testing.T) {
	orig := RouteQueryResponseFrame{
		Type:           RouteQueryResponseFrameType,
		QueryID:        99,
		TargetIdentity: domaintest.ID("unknown-fp"),
		Found:          false,
	}
	data, err := MarshalRouteQueryResponseFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Wire contract: best_uplink must be ABSENT (not "") when Found=false.
	if strings.Contains(string(data), "best_uplink") {
		t.Fatalf("Found=false must omit best_uplink on the wire, got: %s", data)
	}
	got, err := UnmarshalRouteQueryResponseFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
	if !got.BestUplink.IsZero() || got.BestHops != 0 || got.BestSeqNo != 0 {
		t.Fatalf("Found=false should zero best_*: got %+v", got)
	}
}

// TestRouteQueryResponseFrame_MarshalSetsTypeWhenEmpty — mirror of
// the request-side auto-fill behaviour.
func TestRouteQueryResponseFrame_MarshalSetsTypeWhenEmpty(t *testing.T) {
	orig := RouteQueryResponseFrame{QueryID: 1, TargetIdentity: domaintest.ID("x"), Found: false}
	data, err := MarshalRouteQueryResponseFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"type":"`+RouteQueryResponseFrameType+`"`) {
		t.Fatalf("marshalled bytes do not contain expected type: %s", data)
	}
}

// TestRouteQueryResponseFrame_UnmarshalRejectsWrongType — request
// frame must not deserialise into the response struct.
func TestRouteQueryResponseFrame_UnmarshalRejectsWrongType(t *testing.T) {
	bad := []byte(`{"type":"route_query_v1","query_id":1,"target_identity":"x"}`)
	if _, err := UnmarshalRouteQueryResponseFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted wrong type, want error")
	}
}

// TestRouteQueryResponseFrame_UnmarshalRejectsEmptyTarget — even
// Found=false responses must carry target_identity so the receiver
// can correlate by (query_id, target).
func TestRouteQueryResponseFrame_UnmarshalRejectsEmptyTarget(t *testing.T) {
	bad := []byte(`{"type":"` + RouteQueryResponseFrameType + `","query_id":1,"found":false}`)
	if _, err := UnmarshalRouteQueryResponseFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted empty target_identity, want error")
	}
}

// TestRouteQueryFrame_OptionalFieldsOmitemptyOnWire — MaxHops=0 and
// IssuedAt="" must not appear on the wire so default values stay
// invisible to operators tracing frames.
func TestRouteQueryFrame_OptionalFieldsOmitemptyOnWire(t *testing.T) {
	orig := RouteQueryFrame{
		Type:           RouteQueryFrameType,
		QueryID:        1,
		TargetIdentity: domaintest.ID("x"),
	}
	data, err := MarshalRouteQueryFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(data)
	if strings.Contains(s, `"max_hops"`) {
		t.Errorf("max_hops=0 should be omitempty, got %s", s)
	}
	if strings.Contains(s, `"issued_at"`) {
		t.Errorf("issued_at=\"\" should be omitempty, got %s", s)
	}
}
