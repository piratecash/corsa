package domain

import "testing"

func TestCapabilityString(t *testing.T) {
	cases := map[Capability]string{
		CapMeshRelayV1:    "mesh_relay_v1",
		CapMeshRoutingV1:  "mesh_routing_v1",
		CapFileTransferV1: "file_transfer_v1",
	}
	for c, want := range cases {
		if got := c.String(); got != want {
			t.Errorf("Capability(%q).String() = %q, want %q", string(c), got, want)
		}
	}
}

func TestParseCapability(t *testing.T) {
	valid := []struct {
		input string
		want  Capability
	}{
		{"mesh_relay_v1", CapMeshRelayV1},
		{"mesh_routing_v1", CapMeshRoutingV1},
		{"file_transfer_v1", CapFileTransferV1},
		{"MESH_RELAY_V1", CapMeshRelayV1},
		{"Mesh_Routing_V1", CapMeshRoutingV1},
		{"FILE_TRANSFER_V1", CapFileTransferV1},
	}
	for _, tc := range valid {
		c, ok := ParseCapability(tc.input)
		if !ok {
			t.Errorf("ParseCapability(%q) returned false, want true", tc.input)
		}
		if c != tc.want {
			t.Errorf("ParseCapability(%q) = %v, want %v", tc.input, c, tc.want)
		}
	}

	invalid := []string{"", "bogus", "mesh_dht_v1"}
	for _, s := range invalid {
		_, ok := ParseCapability(s)
		if ok {
			t.Errorf("ParseCapability(%q) returned true, want false", s)
		}
	}
}

func TestParseCapabilities(t *testing.T) {
	names := []string{"mesh_relay_v1", "bogus", "mesh_routing_v1", "file_transfer_v1"}
	caps := ParseCapabilities(names)
	if len(caps) != 3 {
		t.Fatalf("expected 3 capabilities, got %d: %v", len(caps), caps)
	}
	if caps[0] != CapMeshRelayV1 {
		t.Errorf("caps[0] = %v, want %v", caps[0], CapMeshRelayV1)
	}
	if caps[1] != CapMeshRoutingV1 {
		t.Errorf("caps[1] = %v, want %v", caps[1], CapMeshRoutingV1)
	}
}

func TestCapabilityStrings(t *testing.T) {
	caps := []Capability{CapMeshRelayV1, CapMeshRoutingV1}
	strs := CapabilityStrings(caps)
	if len(strs) != 2 {
		t.Fatalf("expected 2 strings, got %d", len(strs))
	}
	if strs[0] != "mesh_relay_v1" || strs[1] != "mesh_routing_v1" {
		t.Errorf("CapabilityStrings() = %v, want [mesh_relay_v1 mesh_routing_v1]", strs)
	}

	if got := CapabilityStrings(nil); got != nil {
		t.Errorf("CapabilityStrings(nil) = %v, want nil", got)
	}
}
