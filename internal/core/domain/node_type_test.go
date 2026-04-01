package domain

import "testing"

func TestNodeTypeString(t *testing.T) {
	tests := []struct {
		nt   NodeType
		want string
	}{
		{NodeTypeFull, "full"},
		{NodeTypeClient, "client"},
		{NodeTypeUnknown, "unknown"},
	}
	for _, tc := range tests {
		if got := tc.nt.String(); got != tc.want {
			t.Errorf("NodeType(%q).String() = %q, want %q", string(tc.nt), got, tc.want)
		}
	}
}

func TestNodeTypeIsFull(t *testing.T) {
	if !NodeTypeFull.IsFull() {
		t.Error("NodeTypeFull.IsFull() should be true")
	}
	if NodeTypeClient.IsFull() {
		t.Error("NodeTypeClient.IsFull() should be false")
	}
}

func TestNodeTypeIsClient(t *testing.T) {
	if !NodeTypeClient.IsClient() {
		t.Error("NodeTypeClient.IsClient() should be true")
	}
	if NodeTypeFull.IsClient() {
		t.Error("NodeTypeFull.IsClient() should be false")
	}
}

func TestParseNodeType(t *testing.T) {
	tests := []struct {
		input string
		want  NodeType
		ok    bool
	}{
		{"full", NodeTypeFull, true},
		{"client", NodeTypeClient, true},
		{"Full", NodeTypeFull, true},
		{"CLIENT", NodeTypeClient, true},
		{" full ", NodeTypeFull, true},
		{"", NodeTypeUnknown, false},
		{"relay", NodeTypeUnknown, false},
		{"unknown", NodeTypeUnknown, false},
	}
	for _, tc := range tests {
		got, ok := ParseNodeType(tc.input)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ParseNodeType(%q) = (%q, %v), want (%q, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

func TestNodeTypeWireStability(t *testing.T) {
	if NodeTypeFull != "full" {
		t.Errorf("NodeTypeFull wire value changed: got %q", string(NodeTypeFull))
	}
	if NodeTypeClient != "client" {
		t.Errorf("NodeTypeClient wire value changed: got %q", string(NodeTypeClient))
	}
}
