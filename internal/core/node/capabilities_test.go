package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestLocalCapabilities_OptOutExcludesV3 pins the opt-out shape:
// localCapabilities(false) is what an operator who set
// CORSA_ENABLE_MESH_ROUTING_V3=0 advertises. Since the env flag now defaults
// to ON, false is the explicit opt-out config, not the default — the v3 and
// poison-reverse caps must be absent here.
func TestLocalCapabilities_OptOutExcludesV3(t *testing.T) {
	caps := localCapabilities(false)
	expected := []domain.Capability{
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapFileTransferV1,
		domain.CapMeshRouteProbeV1,
		domain.CapMeshRouteQueryV1,
		// Phase 3 PR 12.5 — incremental-sync digest exchange.
		// Added in lockstep with the localCapabilities() entry
		// so the order-sensitive check below stays honest.
		domain.CapMeshRouteSyncV1,
	}
	if len(caps) != len(expected) {
		t.Fatalf("localCapabilities(false) = %v, want %v", caps, expected)
	}
	for i, c := range expected {
		if caps[i] != c {
			t.Fatalf("localCapabilities(false)[%d] = %q, want %q", i, caps[i], c)
		}
	}
}

// TestLocalCapabilities_EnabledAppendsV3 pins the Phase 4 enabled shape —
// now the DEFAULT (CORSA_ENABLE_MESH_ROUTING_V3 defaults to on): when v3 is
// enabled the v3 cap and the poison-reverse cap are appended at the end of
// the advertise list. attested-links is INTENTIONALLY ABSENT — the Round-7
// review found that the production emit path produces no real signatures
// (signOwnOriginV3Entries only signs Identity == localIdentity
// entries, but AnnounceProjectionFor never emits those), so the
// advertise would promise a contract no v3 frame on the wire
// fulfils. Phase 5 re-enables the advertise the day the self-
// attestation entry stream lands — see capabilities.go for the
// rationale and docs/protocol/attested_links.md "Production
// advertisement status".
func TestLocalCapabilities_EnabledAppendsV3(t *testing.T) {
	caps := localCapabilities(true)
	want := []domain.Capability{
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapFileTransferV1,
		domain.CapMeshRouteProbeV1,
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRouteSyncV1,
		domain.CapMeshRoutingV3,
		domain.CapMeshPoisonReverseV1,
		domain.CapMeshPoisonReverseV2,
	}
	if len(caps) != len(want) {
		t.Fatalf("localCapabilities(true) = %v, want %v", caps, want)
	}
	for i, c := range want {
		if caps[i] != c {
			t.Fatalf("localCapabilities(true)[%d] = %q, want %q", i, caps[i], c)
		}
	}
}

// TestLocalCapabilities_DoesNotAdvertiseAttestedLinks is the explicit
// negative pin for the Round-7 unadvertise. A future change that
// blindly re-adds CapMeshAttestedLinksV1 to localCapabilities would
// fail this test before re-introducing the dishonest cap.
func TestLocalCapabilities_DoesNotAdvertiseAttestedLinks(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		for _, c := range localCapabilities(v3) {
			if c == domain.CapMeshAttestedLinksV1 {
				t.Fatalf("localCapabilities(%v) advertised mesh_attested_links_v1 — emit path produces no real signatures; re-enable only when the self-attestation entry stream lands (capabilities.go)", v3)
			}
		}
	}
}

func TestLocalCapabilityStrings_OptOutExcludesV3(t *testing.T) {
	strs := localCapabilityStrings(false)
	expected := []string{
		"mesh_relay_v1",
		"mesh_routing_v1",
		"mesh_routing_v2",
		"file_transfer_v1",
		"mesh_route_probe_v1",
		"mesh_route_query_v1",
		// Phase 3 PR 12.5 — see TestLocalCapabilities_OptOutExcludesV3.
		"mesh_route_sync_v1",
	}
	if len(strs) != len(expected) {
		t.Fatalf("localCapabilityStrings(false) = %v, want %v", strs, expected)
	}
	for i, s := range expected {
		if strs[i] != s {
			t.Fatalf("localCapabilityStrings(false)[%d] = %q, want %q", i, strs[i], s)
		}
	}
}

func TestLocalCapabilityStrings_EnabledIncludesV3(t *testing.T) {
	strs := localCapabilityStrings(true)
	if len(strs) < 2 {
		t.Fatalf("localCapabilityStrings(true) must include Phase 4 tail (v3 + poison-reverse); got %v", strs)
	}
	// attested-links is INTENTIONALLY absent — see the Round-7 finding
	// pinned by TestLocalCapabilities_DoesNotAdvertiseAttestedLinks.
	// Phase 5 re-enables the cap together with the self-attestation
	// entry stream.
	tail := strs[len(strs)-3:]
	if tail[0] != "mesh_routing_v3" || tail[1] != "mesh_poison_reverse_v1" || tail[2] != "mesh_poison_reverse_v2" {
		t.Fatalf("localCapabilityStrings(true) tail = %v, want [mesh_routing_v3 mesh_poison_reverse_v1 mesh_poison_reverse_v2]", tail)
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
		conns:           make(map[netcore.ConnID]*connEntry),
		connIDByNetConn: make(map[net.Conn]netcore.ConnID),
	}

	pc := netcore.New(netcore.ConnID(1), serverConn, netcore.Inbound, netcore.Options{
		Address: domain.PeerAddress("10.0.0.1:64646"),
		Caps:    []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.setTestConnEntryLocked(serverConn, &connEntry{core: pc})

	serverID, _ := svc.connIDFor(serverConn)
	clientID, _ := svc.connIDFor(clientConn)
	if !svc.connHasCapability(serverID, domain.CapMeshRelayV1) {
		t.Fatal("serverConn should have mesh_relay_v1")
	}
	if svc.connHasCapability(serverID, domain.CapMeshRoutingV1) {
		t.Fatal("serverConn should not have mesh_routing_v1")
	}
	if svc.connHasCapability(clientID, domain.CapMeshRelayV1) {
		t.Fatal("clientConn is not registered, should not match")
	}
}

func TestRememberConnPeerAddrStoresCapabilities(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	svc := &Service{
		conns:           make(map[netcore.ConnID]*connEntry),
		connIDByNetConn: make(map[net.Conn]netcore.ConnID),
	}

	pc := netcore.New(netcore.ConnID(1), serverConn, netcore.Inbound, netcore.Options{})
	svc.setTestConnEntryLocked(serverConn, &connEntry{core: pc})

	hello := protocol.Frame{
		Type:         "hello",
		Listen:       "10.0.0.1:64646",
		Capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
	}
	id, _ := svc.connIDFor(serverConn)
	svc.rememberConnPeerAddr(id, hello, "10.0.0.1:55555")

	caps := pc.Capabilities()
	if caps == nil {
		t.Fatal("NetCore capabilities should be set after rememberConnPeerAddr")
	}

	// localCapabilities() returns [mesh_relay_v1, mesh_routing_v1,
	// mesh_routing_v2, file_transfer_v1, mesh_route_probe_v1,
	// mesh_route_query_v1]. The remote hello advertises
	// ["mesh_relay_v1", "mesh_routing_v1"]. The intersection should contain
	// only the two common capabilities — v2, file_transfer_v1, route_probe
	// and route_query are absent on the remote side, so they are not part
	// of the negotiated set.
	if len(caps) != 2 || caps[0] != domain.CapMeshRelayV1 || caps[1] != domain.CapMeshRoutingV1 {
		t.Fatalf("expected [mesh_relay_v1, mesh_routing_v1], got %v", caps)
	}
}
