package node

import (
	"net"
	"testing"

	"corsa/internal/core/protocol"
)

func TestLocalCapabilities(t *testing.T) {
	caps := localCapabilities()
	expected := []string{"mesh_relay_v1"}
	if len(caps) != len(expected) {
		t.Fatalf("localCapabilities() = %v, want %v", caps, expected)
	}
	for i, cap := range expected {
		if caps[i] != cap {
			t.Fatalf("localCapabilities()[%d] = %q, want %q", i, caps[i], cap)
		}
	}
}

func TestIntersectCapabilities(t *testing.T) {
	tests := []struct {
		name     string
		local    []string
		remote   []string
		expected []string
	}{
		{
			name:     "both empty",
			local:    nil,
			remote:   nil,
			expected: nil,
		},
		{
			name:     "local empty",
			local:    nil,
			remote:   []string{"mesh_relay_v1"},
			expected: nil,
		},
		{
			name:     "remote empty",
			local:    []string{"mesh_relay_v1"},
			remote:   nil,
			expected: nil,
		},
		{
			name:     "no overlap",
			local:    []string{"mesh_relay_v1"},
			remote:   []string{"mesh_routing_v1"},
			expected: nil,
		},
		{
			name:     "full overlap",
			local:    []string{"mesh_relay_v1", "mesh_routing_v1"},
			remote:   []string{"mesh_routing_v1", "mesh_relay_v1"},
			expected: []string{"mesh_relay_v1", "mesh_routing_v1"},
		},
		{
			name:     "partial overlap",
			local:    []string{"mesh_relay_v1", "mesh_routing_v1"},
			remote:   []string{"mesh_relay_v1", "mesh_dht_v1"},
			expected: []string{"mesh_relay_v1"},
		},
		{
			name:     "single common capability",
			local:    []string{"mesh_relay_v1"},
			remote:   []string{"mesh_relay_v1"},
			expected: []string{"mesh_relay_v1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := intersectCapabilities(tc.local, tc.remote)
			if tc.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %v", result)
				}
				return
			}
			if len(result) != len(tc.expected) {
				t.Fatalf("expected %v, got %v", tc.expected, result)
			}
			for i, cap := range tc.expected {
				if result[i] != cap {
					t.Fatalf("expected[%d] = %q, got %q", i, cap, result[i])
				}
			}
		})
	}
}

func TestSessionHasCapability(t *testing.T) {
	svc := &Service{
		sessions: map[string]*peerSession{
			"peer-a": {
				address:      "peer-a",
				capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
			},
			"peer-b": {
				address:      "peer-b",
				capabilities: nil,
			},
		},
	}

	if !svc.sessionHasCapability("peer-a", "mesh_relay_v1") {
		t.Fatal("peer-a should have mesh_relay_v1")
	}
	if !svc.sessionHasCapability("peer-a", "mesh_routing_v1") {
		t.Fatal("peer-a should have mesh_routing_v1")
	}
	if svc.sessionHasCapability("peer-a", "mesh_dht_v1") {
		t.Fatal("peer-a should not have mesh_dht_v1")
	}
	if svc.sessionHasCapability("peer-b", "mesh_relay_v1") {
		t.Fatal("peer-b has nil capabilities, should not match")
	}
	if svc.sessionHasCapability("unknown-peer", "mesh_relay_v1") {
		t.Fatal("unknown peer should not match")
	}
}

func TestConnHasCapability(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	svc := &Service{
		connPeerInfo: map[net.Conn]*connPeerHello{
			serverConn: {
				address:      "10.0.0.1:64646",
				capabilities: []string{"mesh_relay_v1"},
			},
		},
	}

	if !svc.connHasCapability(serverConn, "mesh_relay_v1") {
		t.Fatal("serverConn should have mesh_relay_v1")
	}
	if svc.connHasCapability(serverConn, "mesh_routing_v1") {
		t.Fatal("serverConn should not have mesh_routing_v1")
	}
	if svc.connHasCapability(clientConn, "mesh_relay_v1") {
		t.Fatal("clientConn is not registered, should not match")
	}
}

func TestRememberConnPeerAddrStoresCapabilities(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	svc := &Service{
		connPeerInfo: make(map[net.Conn]*connPeerHello),
	}

	hello := protocol.Frame{
		Type:         "hello",
		Listen:       "10.0.0.1:64646",
		Capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
	}
	svc.rememberConnPeerAddr(serverConn, hello)

	info := svc.connPeerInfo[serverConn]
	if info == nil {
		t.Fatal("connPeerInfo should be set after rememberConnPeerAddr")
	}

	// localCapabilities() returns ["mesh_relay_v1"] in Iteration 1.
	// The remote hello advertises ["mesh_relay_v1", "mesh_routing_v1"].
	// The intersection should contain only "mesh_relay_v1".
	if len(info.capabilities) != 1 || info.capabilities[0] != "mesh_relay_v1" {
		t.Fatalf("expected [mesh_relay_v1], got %v", info.capabilities)
	}
}
