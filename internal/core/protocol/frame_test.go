package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFrameCapabilitiesOmitEmpty(t *testing.T) {
	frame := Frame{Type: "hello"}
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "capabilities") {
		t.Fatal("capabilities field should be omitted when nil")
	}
}

func TestFrameCapabilitiesSerialization(t *testing.T) {
	frame := Frame{
		Type:         "hello",
		Capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
	}
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"capabilities":["mesh_relay_v1","mesh_routing_v1"]`) {
		t.Fatalf("unexpected JSON: %s", string(data))
	}
}

func TestFrameCapabilitiesDeserialization(t *testing.T) {
	raw := `{"type":"hello","capabilities":["mesh_relay_v1"]}`
	var frame Frame
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		t.Fatal(err)
	}
	if len(frame.Capabilities) != 1 || frame.Capabilities[0] != "mesh_relay_v1" {
		t.Fatalf("expected [mesh_relay_v1], got %v", frame.Capabilities)
	}
}

func TestFrameWithoutCapabilitiesFieldDeserializes(t *testing.T) {
	raw := `{"type":"hello","version":3}`
	var frame Frame
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Capabilities != nil {
		t.Fatalf("expected nil capabilities for legacy frame, got %v", frame.Capabilities)
	}
}

func TestParseFrameLineWithCapabilities(t *testing.T) {
	line := `{"type":"welcome","capabilities":["mesh_relay_v1","mesh_routing_v1"]}`
	frame, err := ParseFrameLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != "welcome" {
		t.Fatalf("expected type welcome, got %s", frame.Type)
	}
	if len(frame.Capabilities) != 2 {
		t.Fatalf("expected 2 capabilities, got %d", len(frame.Capabilities))
	}
}

// --- Forward-compatible relay: AnnounceRouteFrame Extra field ---

func TestAnnounceRouteFrameUnmarshalPreservesUnknownFields(t *testing.T) {
	raw := `{"identity":"abc123","origin":"def456","hops":2,"seq":10,"onion_box":"deadbeef","future_field":42}`
	var f AnnounceRouteFrame
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatal(err)
	}
	if f.Identity != "abc123" {
		t.Fatalf("expected identity abc123, got %s", f.Identity)
	}
	if f.Origin != "def456" {
		t.Fatalf("expected origin def456, got %s", f.Origin)
	}
	if f.Hops != 2 {
		t.Fatalf("expected hops 2, got %d", f.Hops)
	}
	if f.SeqNo != 10 {
		t.Fatalf("expected seq 10, got %d", f.SeqNo)
	}
	if f.Extra == nil {
		t.Fatal("expected Extra to contain unknown fields, got nil")
	}

	var extra map[string]json.RawMessage
	if err := json.Unmarshal(f.Extra, &extra); err != nil {
		t.Fatal(err)
	}
	if _, ok := extra["onion_box"]; !ok {
		t.Fatal("expected onion_box in Extra")
	}
	if _, ok := extra["future_field"]; !ok {
		t.Fatal("expected future_field in Extra")
	}
	if len(extra) != 2 {
		t.Fatalf("expected 2 extra fields, got %d", len(extra))
	}
}

func TestAnnounceRouteFrameUnmarshalNoExtraFieldsYieldsNilExtra(t *testing.T) {
	raw := `{"identity":"abc","origin":"def","hops":1,"seq":5}`
	var f AnnounceRouteFrame
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatal(err)
	}
	if f.Extra != nil {
		t.Fatalf("expected nil Extra when no unknown fields, got %s", string(f.Extra))
	}
}

func TestAnnounceRouteFrameMarshalIncludesExtraFields(t *testing.T) {
	f := AnnounceRouteFrame{
		Identity: "abc123",
		Origin:   "def456",
		Hops:     2,
		SeqNo:    10,
		Extra:    json.RawMessage(`{"onion_box":"deadbeef","future_field":42}`),
	}
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if !strings.Contains(s, `"onion_box":"deadbeef"`) {
		t.Fatalf("expected onion_box in output, got %s", s)
	}
	if !strings.Contains(s, `"future_field":42`) {
		t.Fatalf("expected future_field in output, got %s", s)
	}
	if !strings.Contains(s, `"identity":"abc123"`) {
		t.Fatalf("expected identity in output, got %s", s)
	}
}

func TestAnnounceRouteFrameMarshalWithoutExtraOmitsNothing(t *testing.T) {
	f := AnnounceRouteFrame{
		Identity: "abc",
		Origin:   "def",
		Hops:     1,
		SeqNo:    5,
	}
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip map[string]json.RawMessage
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if len(roundTrip) != 4 {
		t.Fatalf("expected 4 fields, got %d: %s", len(roundTrip), string(data))
	}
}

func TestAnnounceRouteFrameRoundTripPreservesExtra(t *testing.T) {
	original := `{"identity":"abc123","origin":"def456","hops":2,"seq":10,"onion_box":"deadbeef","nested":{"a":1}}`
	var f AnnounceRouteFrame
	if err := json.Unmarshal([]byte(original), &f); err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}

	var f2 AnnounceRouteFrame
	if err := json.Unmarshal(data, &f2); err != nil {
		t.Fatal(err)
	}

	if f2.Identity != f.Identity || f2.Origin != f.Origin || f2.Hops != f.Hops || f2.SeqNo != f.SeqNo {
		t.Fatal("known fields differ after round-trip")
	}

	var extra map[string]json.RawMessage
	if err := json.Unmarshal(f2.Extra, &extra); err != nil {
		t.Fatal(err)
	}
	if _, ok := extra["onion_box"]; !ok {
		t.Fatal("onion_box lost after round-trip")
	}
	if _, ok := extra["nested"]; !ok {
		t.Fatal("nested lost after round-trip")
	}
}

func TestAnnounceRouteFrameInFrameRoundTrip(t *testing.T) {
	raw := `{"type":"announce_routes","routes":[{"identity":"aaa","origin":"bbb","hops":1,"seq":1,"future_key":"value"}]}`
	var frame Frame
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		t.Fatal(err)
	}
	if len(frame.AnnounceRoutes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(frame.AnnounceRoutes))
	}
	if frame.AnnounceRoutes[0].Extra == nil {
		t.Fatal("expected Extra to preserve future_key")
	}

	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if !strings.Contains(s, `"future_key":"value"`) {
		t.Fatalf("future_key lost after Frame round-trip: %s", s)
	}
}

func TestAnnounceRouteFrameUnmarshalReusedStructClearsStaleExtra(t *testing.T) {
	// First unmarshal: payload with unknown fields.
	withExtra := `{"identity":"abc","origin":"def","hops":2,"seq":1,"onion_box":"deadbeef"}`
	var f AnnounceRouteFrame
	if err := json.Unmarshal([]byte(withExtra), &f); err != nil {
		t.Fatal(err)
	}
	if f.Extra == nil {
		t.Fatal("expected Extra after first unmarshal")
	}

	// Second unmarshal into the SAME struct: payload without unknown fields.
	withoutExtra := `{"identity":"xyz","origin":"uvw","hops":1,"seq":2}`
	if err := json.Unmarshal([]byte(withoutExtra), &f); err != nil {
		t.Fatal(err)
	}
	if f.Identity != "xyz" {
		t.Fatalf("expected identity xyz, got %s", f.Identity)
	}
	if f.Extra != nil {
		t.Fatalf("stale Extra leaked from previous unmarshal: %s", string(f.Extra))
	}
}

func TestAnnounceRouteFrameMarshalReturnsErrorOnMalformedExtra(t *testing.T) {
	f := AnnounceRouteFrame{
		Identity: "abc",
		Origin:   "def",
		Hops:     1,
		SeqNo:    1,
		Extra:    json.RawMessage(`not valid json`),
	}
	_, err := json.Marshal(f)
	if err == nil {
		t.Fatal("expected error when marshaling malformed Extra, got nil")
	}
}

func TestAnnounceRouteFrameMarshalReturnsErrorOnNullExtra(t *testing.T) {
	f := AnnounceRouteFrame{
		Identity: "abc",
		Origin:   "def",
		Hops:     1,
		SeqNo:    1,
		Extra:    json.RawMessage(`null`),
	}
	_, err := json.Marshal(f)
	if err == nil {
		t.Fatal("expected error when marshaling null Extra, got nil")
	}
}
