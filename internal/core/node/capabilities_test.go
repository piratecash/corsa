package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestLocalCapabilities(t *testing.T) {
	caps := localCapabilities()
	expected := []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1, domain.CapFileTransferV1}
	if len(caps) != len(expected) {
		t.Fatalf("localCapabilities() = %v, want %v", caps, expected)
	}
	for i, c := range expected {
		if caps[i] != c {
			t.Fatalf("localCapabilities()[%d] = %q, want %q", i, caps[i], c)
		}
	}
}

func TestLocalCapabilityStrings(t *testing.T) {
	strs := localCapabilityStrings()
	expected := []string{"mesh_relay_v1", "mesh_routing_v1", "file_transfer_v1"}
	if len(strs) != len(expected) {
		t.Fatalf("localCapabilityStrings() = %v, want %v", strs, expected)
	}
	for i, s := range expected {
		if strs[i] != s {
			t.Fatalf("localCapabilityStrings()[%d] = %q, want %q", i, strs[i], s)
		}
	}
}

func TestIntersectCapabilities(t *testing.T) {
	tests := []struct {
		name     string
		local    []domain.Capability
		remote   []string
		expected []domain.Capability
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
			local:    []domain.Capability{domain.CapMeshRelayV1},
			remote:   nil,
			expected: nil,
		},
		{
			name:     "no overlap",
			local:    []domain.Capability{domain.CapMeshRelayV1},
			remote:   []string{"mesh_routing_v1"},
			expected: nil,
		},
		{
			name:     "full overlap",
			local:    []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
			remote:   []string{"mesh_routing_v1", "mesh_relay_v1"},
			expected: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
		},
		{
			name:     "partial overlap",
			local:    []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
			remote:   []string{"mesh_relay_v1", "mesh_dht_v1"},
			expected: []domain.Capability{domain.CapMeshRelayV1},
		},
		{
			name:     "single common capability",
			local:    []domain.Capability{domain.CapMeshRelayV1},
			remote:   []string{"mesh_relay_v1"},
			expected: []domain.Capability{domain.CapMeshRelayV1},
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
		sessions: map[domain.PeerAddress]*peerSession{
			"peer-a": {
				address:      "peer-a",
				capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
			},
			"peer-b": {
				address:      "peer-b",
				capabilities: nil,
			},
		},
	}

	if !svc.sessionHasCapability(domain.PeerAddress("peer-a"), domain.CapMeshRelayV1) {
		t.Fatal("peer-a should have mesh_relay_v1")
	}
	if !svc.sessionHasCapability(domain.PeerAddress("peer-a"), domain.CapMeshRoutingV1) {
		t.Fatal("peer-a should have mesh_routing_v1")
	}
	if svc.sessionHasCapability(domain.PeerAddress("peer-a"), domain.Capability("mesh_dht_v1")) {
		t.Fatal("peer-a should not have mesh_dht_v1")
	}
	if svc.sessionHasCapability(domain.PeerAddress("peer-b"), domain.CapMeshRelayV1) {
		t.Fatal("peer-b has nil capabilities, should not match")
	}
	if svc.sessionHasCapability(domain.PeerAddress("unknown-peer"), domain.CapMeshRelayV1) {
		t.Fatal("unknown peer should not match")
	}
}

func TestConnHasCapability(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	svc := &Service{
		inboundNetCores: make(map[net.Conn]*NetCore),
	}

	pc := newNetCore(connID(1), serverConn, Inbound, NetCoreOpts{
		Address: domain.PeerAddress("10.0.0.1:64646"),
		Caps:    []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.inboundNetCores[serverConn] = pc

	if !svc.connHasCapability(serverConn, domain.CapMeshRelayV1) {
		t.Fatal("serverConn should have mesh_relay_v1")
	}
	if svc.connHasCapability(serverConn, domain.CapMeshRoutingV1) {
		t.Fatal("serverConn should not have mesh_routing_v1")
	}
	if svc.connHasCapability(clientConn, domain.CapMeshRelayV1) {
		t.Fatal("clientConn is not registered, should not match")
	}
}

func TestRememberConnPeerAddrStoresCapabilities(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	svc := &Service{
		inboundNetCores: make(map[net.Conn]*NetCore),
	}

	pc := newNetCore(connID(1), serverConn, Inbound, NetCoreOpts{})
	svc.inboundNetCores[serverConn] = pc

	hello := protocol.Frame{
		Type:         "hello",
		Listen:       "10.0.0.1:64646",
		Capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
	}
	svc.rememberConnPeerAddr(serverConn, hello)

	caps := pc.Capabilities()
	if caps == nil {
		t.Fatal("NetCore capabilities should be set after rememberConnPeerAddr")
	}

	// localCapabilities() returns [mesh_relay_v1, mesh_routing_v1, file_transfer_v1].
	// The remote hello advertises ["mesh_relay_v1", "mesh_routing_v1"].
	// The intersection should contain only the two common capabilities.
	if len(caps) != 2 || caps[0] != domain.CapMeshRelayV1 || caps[1] != domain.CapMeshRoutingV1 {
		t.Fatalf("expected [mesh_relay_v1, mesh_routing_v1], got %v", caps)
	}
}
