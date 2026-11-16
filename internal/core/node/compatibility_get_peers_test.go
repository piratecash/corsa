package node

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestGetPeersUsesOriginalTransportGate(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	const address = domain.PeerAddress("192.0.2.113:64646")
	id, core := newTrackedInboundHealthConn(t, svc, address)
	core.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: idPeerB.String()}})
	const request = "{\"type\":\"get_peers\"}\n"
	svc.peersExchangeAccessNanos.Store(0)
	svc.dispatchNetworkFrame(id, request)
	if svc.peersExchangeAccessNanos.Load() == 0 {
		t.Fatal("open connection did not build a peer exchange reply")
	}
	core.ShutSendQueue()
	if svc.netCoreForID(id) != core {
		t.Fatal("test must exercise the interval before unregister")
	}
	svc.peersExchangeAccessNanos.Store(0)
	svc.dispatchNetworkFrame(id, request)
	if svc.peersExchangeAccessNanos.Load() != 0 {
		t.Fatal("closed connection built a peer exchange reply")
	}
}

// TestCompatibility_InboundGetPeersFrameShape is the step-7 regression test
// for the inbound get_peers wire contract. It authenticates against a running
// service, issues a get_peers request and checks both the decoded frame
// shape and the underlying JSON keys. Previous versions of the protocol
// promise:
//
//   - type = "peers"
//   - count = len(peers)
//   - peers is a (possibly empty) JSON array of strings
//
// Breaking any of these would silently break mixed-version sync, so we assert
// them explicitly at the wire level rather than relying on Go-struct decoding
// alone.
func TestCompatibility_InboundGetPeersFrameShape(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	// Seed a known candidate so the peers array is non-empty — this lets the
	// test distinguish "field missing" from "field present but empty".
	//
	// buildPeerExchangeResponse reads the candidate list from the
	// peers_exchange snapshot instead of calling peerProvider.Candidates()
	// directly (see peers_exchange_snapshot.go for why).  The snapshot is
	// rebuilt by hotReadsRefreshLoop on an interval, so without an explicit
	// rebuild here the test would race the refresher and frequently observe
	// the pre-Add snapshot with zero candidates.  Production callers tolerate
	// that bounded staleness; test assertions do not, so we force a sync
	// rebuild, mirroring peers_exchange_snapshot_test.go.
	if svc.peerProvider != nil {
		svc.peerProvider.Add(mustAddr("203.0.113.10:64646"), domain.PeerSourceBootstrap)
		svc.rebuildPeersExchangeSnapshot()
	}

	conn, reader := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	line, err := readFrameLine(reader, protocol.MaxFrameLine)
	if err != nil {
		t.Fatalf("read peers response: %v", err)
	}
	var response struct {
		Type  string   `json:"type"`
		Count *int     `json:"count"`
		Peers []string `json:"peers"`
	}
	if err := json.Unmarshal([]byte(line), &response); err != nil {
		t.Fatalf("decode peers response: %v", err)
	}
	if response.Type != "peers" || response.Count == nil || *response.Count != len(response.Peers) || len(response.Peers) == 0 {
		t.Fatalf("unexpected peers wire shape: %s", line)
	}

}

// TestCompatibility_LocalRPCGetPeersKeepsKnownCandidates verifies that the
// local/RPC get_peers path still surfaces known peer candidates even when
// no active outbound connection exists. The design document calls this
// "known_only" — it is a deprecated compatibility concept used by Desktop
// and local operators to learn the address graph before steady-state
// connectivity is reached. Step 7 explicitly requires that this behaviour
// stays intact across the policy change.
//
// The test feeds a PeerProvider with public candidates, does NOT start a
// ConnectionManager, and invokes HandleLocalFrame(get_peers). The expected
// result is that the known candidates appear in the peers array.
func TestCompatibility_LocalRPCGetPeersKeepsKnownCandidates(t *testing.T) {
	t.Parallel()

	ppCfg := testProviderConfig()
	pp := NewPeerProvider(ppCfg)
	// Use public IPv4 addresses — shouldHidePeerExchangeAddress filters out
	// loopback and RFC1918, so private addresses would never be returned
	// even under the old behaviour.
	want := []domain.PeerAddress{
		mustAddr("198.51.100.20:9000"),
		mustAddr("198.51.100.21:9000"),
	}
	for _, a := range want {
		pp.Add(a, domain.PeerSourceBootstrap)
	}

	svc := &Service{
		connManager:  nil,
		peerProvider: pp,
		cfg:          testServiceConfig(),
	}

	// buildPeerExchangeResponse reads the candidate list from the
	// peers_exchange snapshot instead of calling peerProvider.Candidates()
	// directly (see peers_exchange_snapshot.go for why).  Production code
	// gets this invariant from primeHotReadSnapshots() in Run(); this test
	// constructs a bare Service and never goes through Run(), so we rebuild
	// the snapshot explicitly after seeding the PeerProvider.  Without this
	// the RPC path loads a nil snapshot and returns an empty peers array.
	svc.rebuildPeersExchangeSnapshot()

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "get_peers"})

	if reply.Type != "peers" {
		t.Fatalf("local RPC get_peers must return type=peers, got %#v", reply)
	}
	if reply.Count != len(reply.Peers) {
		t.Fatalf("local RPC peers.count must equal len(peers): count=%d len=%d",
			reply.Count, len(reply.Peers))
	}

	got := make(map[string]struct{}, len(reply.Peers))
	for _, a := range reply.Peers {
		got[a] = struct{}{}
	}
	for _, a := range want {
		if _, ok := got[string(a)]; !ok {
			t.Fatalf("known candidate %s missing from local RPC get_peers response: %v",
				a, reply.Peers)
		}
	}
}
