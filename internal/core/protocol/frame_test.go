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
