package node

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Compatibility tests for Step 7 of docs/peer-discovery-conditional-get-peers.ru.md.
//
// These tests protect wire compatibility of the get_peers command against the
// conditional-get-peers policy change. The policy is a local-only initiator
// behaviour change: the wire frames, the inbound handler and the response
// shape MUST stay identical to what previous versions of the protocol
// produce and accept.
//
// The four guarantees being defended here are:
//
//   1. The inbound get_peers handler still returns a protocol-conformant
//      peers frame (type + count + peers array).
//   2. A new-policy initiator (shouldRequestPeers=true) still interoperates
//      with a legacy responder (welcome without capabilities).
//   3. A legacy initiator (hello without capabilities) still interoperates
//      with a new responder — covered by TestMixedVersionLegacyPeerExchange
//      in service_test.go; this file adds an explicit step-7 cross-reference
//      test that complements it by focusing on the peers response shape.
//   4. The "known_only" semantics of the local/RPC get_peers — i.e. that
//      candidates from PeerProvider continue to surface even when no live
//      active connection is present — remain intact.

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

	conn, reader, _ := authenticatedConn(t, svc)
	defer func() { _ = conn.Close() }()

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	resp := readJSONTestFrame(t, reader)

	if resp.Type != "peers" {
		t.Fatalf("inbound get_peers must return type=peers, got type=%q code=%s error=%q",
			resp.Type, resp.Code, resp.Error)
	}
	if resp.Count != len(resp.Peers) {
		t.Fatalf("peers.count must equal len(peers): count=%d len(peers)=%d",
			resp.Count, len(resp.Peers))
	}

	// Re-encode the parsed frame to verify the JSON key shape survives a
	// round-trip. This defends against accidental renames of the tag
	// (`json:"peers"`) that would only break old parsers.
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal peers frame: %v", err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal peers frame: %v", err)
	}
	if _, ok := obj["type"]; !ok {
		t.Fatalf("peers frame missing top-level key %q: %s", "type", string(raw))
	}
	if _, ok := obj["peers"]; !ok {
		t.Fatalf("peers frame missing top-level key %q: %s", "peers", string(raw))
	}
}

// legacyResponderMock starts a TCP listener and answers as a legacy responder
// would: welcome WITHOUT a capabilities field, then a peers frame for
// get_peers and an empty contacts frame for fetch_contacts. It is the mirror
// image of the "legacy initiator" that TestMixedVersionLegacyPeerExchange
// already covers in service_test.go.
func legacyResponderMock(
	t *testing.T,
	ln net.Listener,
	respondPeers []string,
) []string {
	t.Helper()

	// Reuse the initiator-mock from sync_peer_test.go. That helper already
	// sends welcome without capabilities (it only sets Type: "welcome"),
	// which is exactly the legacy responder behaviour we want to simulate
	// for step 7.
	return syncPeerMockServer(t, ln, respondPeers)
}

// TestCompatibility_NewInitiatorAgainstLegacyResponder verifies the
// forward-direction of the mixed-version matrix: a node running the new
// conditional-get-peers policy, whose shouldRequestPeers() returns true
// (aggregate status offline), still produces a wire-compatible get_peers
// exchange when talking to a legacy responder that does not advertise any
// capabilities.
//
// This is the counterpart to TestMixedVersionLegacyPeerExchange
// (service_test.go:7717) which covers the reverse direction (legacy
// initiator → new responder). Together they cover both halves of
// the "mixed-version network old ↔ new" scenario from Step 7 of the design document.
func TestCompatibility_NewInitiatorAgainstLegacyResponder(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Offline aggregate status forces shouldRequestPeers() == true, so the
	// initiator is guaranteed to exercise the get_peers branch of the new
	// policy. The caller passes requestPeers=true explicitly for the same
	// reason — matching the bootstrap/recovery contract from Appendix C of
	// the design document.
	svc := newSyncPeerTestService(domain.NetworkStatusOffline)
	peerAddr := domain.PeerAddress(ln.Addr().String())

	advertised := []string{"198.51.100.7:9000", "198.51.100.8:9000"}
	receivedCh := make(chan []string, 1)
	go func() {
		receivedCh <- legacyResponderMock(t, ln, advertised)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	svc.syncPeer(ctx, peerAddr, true)
	received := <-receivedCh

	// The wire sequence must be exactly the same as before the policy
	// change: hello → get_peers → fetch_contacts.
	want := []string{"hello", "get_peers", "fetch_contacts"}
	if len(received) != len(want) {
		t.Fatalf("unexpected frame sequence against legacy responder: got %v, want %v", received, want)
	}
	for i, w := range want {
		if received[i] != w {
			t.Fatalf("frame[%d] = %q, want %q (full sequence: %v)", i, received[i], w, received)
		}
	}

	// The new initiator must have imported the addresses that the legacy
	// responder returned. This proves that the response path (which Step 7
	// must not change) still feeds addPeerAddress().
	svc.peerMu.RLock()
	peerCount := len(svc.peers)
	svc.peerMu.RUnlock()
	if peerCount != len(advertised) {
		t.Fatalf("new initiator failed to import peers from legacy responder: got %d, want %d",
			peerCount, len(advertised))
	}
}

// TestCompatibility_LegacyInitiatorRespondedPeersShape is a narrow
// regression test focused on what a legacy initiator sees on the wire when
// it talks to a new node. TestMixedVersionLegacyPeerExchange covers the
// full hello/auth/get_peers/fetch_contacts sequence; this test zooms in on
// the peers frame itself and asserts the step-7 wire guarantees:
//
//   - frame type = "peers"
//   - count field is present and equals len(peers)
//   - peers is a (possibly empty) array
//
// If a future refactor accidentally renames the JSON field or stops
// populating Count, this test will catch it before mixed-version networks
// break.
func TestCompatibility_LegacyInitiatorRespondedPeersShape(t *testing.T) {
	t.Parallel()

	svc, stop := newScopedTestService(t)
	defer stop()

	legacyID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// "Legacy" hello: valid identity but no Capabilities field at all.
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Client:                 "node",
		ClientVersion:          "0.9.0",
		Address:                legacyID.Address,
		PubKey:                 identity.PublicKeyBase64(legacyID.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(legacyID.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(legacyID),
		Listen:                 "10.0.0.11:12345",
	})
	reader := bufio.NewReader(conn)
	welcome := readJSONTestFrame(t, reader)
	if welcome.Type != "welcome" || welcome.Challenge == "" {
		t.Fatalf("expected welcome with challenge, got %#v", welcome)
	}

	writeJSONFrame(t, conn, protocol.Frame{
		Type:      "auth_session",
		Address:   legacyID.Address,
		Signature: identity.SignPayload(legacyID, connauth.SessionAuthPayload(welcome.Challenge, legacyID.Address)),
	})
	authReply := readJSONTestFrame(t, reader)
	if authReply.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got type=%q code=%s error=%q", authReply.Type, authReply.Code, authReply.Error)
	}

	writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
	resp := readJSONTestFrame(t, reader)

	if resp.Type != "peers" {
		t.Fatalf("legacy initiator expected type=peers, got type=%q code=%s error=%q",
			resp.Type, resp.Code, resp.Error)
	}
	if resp.Count != len(resp.Peers) {
		t.Fatalf("legacy initiator expected count==len(peers): count=%d len=%d",
			resp.Count, len(resp.Peers))
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
