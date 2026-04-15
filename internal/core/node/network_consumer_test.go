package node

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestDispatchInboundPing_WritesPongViaNetworkBackend asserts the
// runtime contract of the inbound-ping handler: when a Service is
// constructed with a caller-supplied netcore.Network, the pong reply
// produced by dispatchNetworkFrame must travel through that injected
// Network surface, not through a legacy ConnID-first helper that
// resolves *netcore.NetCore from s.conns directly.
//
// The assertion chain:
//
//  1. A Service is constructed via NewServiceWithNetwork with a
//     netcoretest.Backend pinned as the Network surface — the
//     injection seam exposed by network_bridge.go.
//  2. A virtual inbound ConnID is registered in the backend; there is
//     no net.Pipe, no net.TCPConn, no real socket on the transport
//     path that the pong travels.
//  3. dispatchNetworkFrame is invoked with a `ping` wire line. The
//     case `"ping"` branch routes the pong through sendFrameViaNetwork,
//     which calls Network.SendFrame — the backend receives the frame
//     on backend.SendFrame.
//  4. backend.Outbound(connID) yields the exact pong bytes the Service
//     produced — the test decodes them back into a protocol.Frame and
//     asserts Type == "pong".
//
// If anyone reverts the pong call-site to writeJSONFrameByID (or any
// other helper that bypasses the injected Network), the Outbound
// channel stays empty, the read times out, and this test fails —
// that is the whole point of keeping this test in the package.
func TestDispatchInboundPing_WritesPongViaNetworkBackend(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	// A minimal NewService-equivalent configuration: identity is generated
	// inline to keep the test self-contained without pulling in the full
	// newTestService helper (which is in relay_test.go and reaches into
	// WaitBackground / tempdir machinery the POC does not exercise).
	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		AdvertiseAddress: "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		QueueStatePath:   t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	// Register a synthetic inbound ConnID in the backend. dispatchNetworkFrame
	// does not consult the production s.conns registry for the pong path —
	// the write travels exclusively through s.Network(). Bookkeeping calls
	// (touchConnActivity, trackedInboundPeerAddress) against an
	// unregistered connID are no-ops returning zero values, which is the
	// exact production behaviour when an inbound peer has not yet
	// announced a hello.
	connID := netcore.ConnID(9001)
	backend.Register(connID, netcore.Inbound, "10.0.0.42:64646")

	// dispatchNetworkFrame guards `core == nil` and reads core.RemoteAddr()
	// for protocol_trace logging. A netcore.NetCore fixture backed by a
	// net.Pipe satisfies both without involving the write path — the
	// actual pong bytes are routed through the Network override, not pc.
	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(connID, serverPipe, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)

	pingLine := `{"type":"ping"}`
	if ok := svc.dispatchNetworkFrame(connID, pc, pingLine); !ok {
		t.Fatalf("dispatchNetworkFrame(ping) returned false; expected accepted=true")
	}

	select {
	case data, ok := <-backend.Outbound(connID):
		if !ok {
			t.Fatal("backend.Outbound(connID) closed before pong arrived")
		}
		frame, err := parseFrameLineForTest(data)
		if err != nil {
			t.Fatalf("parse outbound frame: %v (raw=%q)", err, data)
		}
		if frame.Type != "pong" {
			t.Fatalf("expected pong frame on backend.Outbound, got type %q (raw=%q)", frame.Type, data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pong on backend.Outbound(connID): " +
			"Service did not route the reply through the injected Network surface — the pong was sent via a legacy helper that bypasses s.Network().SendFrame")
	}
}

// TestDispatchNetworkFrame_AsyncReplies_RouteViaNetworkBackend is the
// table-driven companion to the ping test above. It locks in the PR 10.9
// migration: every async reply-write inside dispatchNetworkFrame must
// route through s.Network().SendFrame (i.e. sendFrameViaNetwork), not
// through the legacy ConnID-first helpers that resolve *netcore.NetCore
// from s.conns directly.
//
// The table covers a representative cross-section of the switch cases
// migrated in PR 10.9:
//
//   - "hello" with incompatible protocol version — un-auth path, error reply.
//   - "hello" without identity fields — un-auth path, welcome reply.
//   - "announce_peer" with unknown node_type — auth-gated, early-return ack.
//   - "announce_peer" with known node_type and empty Peers — auth-gated,
//     full-path ack after the (no-op) promotion loop.
//
// The remaining 8 migrated call-sites (get_peers, fetch_contacts,
// ack_delete, subscribe_inbox pair, auth_session success, relay_hop_ack,
// and welcomeFrame-with-challenge) depend on broader Service state
// (connManager, contactStore, DeleteTracker, MeshRelayV1 capability)
// that is out of scope for this POC-style test. Their protection relies
// on the static §2.9 Gate 6 regex in scripts/enforce-netcore-boundary.sh
// — if someone reverts those call-sites to writeJSONFrameByID, the gate
// fails at CI time.
//
// Setup pattern per case:
//
//  1. Build a fresh Service with a pinned netcoretest.Backend via
//     NewServiceWithNetwork so Service.Network() returns the backend.
//  2. Register the synthetic ConnID in backend (for SendFrame) AND in
//     s.conns via setTestConnEntryLocked (so netCoreForID works for
//     auth-gated helpers like isConnAuthenticated).
//  3. For auth-gated cases: attach a Verified=true connauth.State to the
//     NetCore via pc.SetAuth — that is the production auth seam read
//     by isConnAuthenticated → connAuthStateByID → pc.Auth().
//  4. Invoke dispatchNetworkFrame with the crafted frame line.
//  5. Assert the expected reply type arrives on backend.Outbound(connID)
//     within 2 seconds.
func TestDispatchNetworkFrame_AsyncReplies_RouteViaNetworkBackend(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		authVerified      bool
		inboundFrameLine  string
		expectedReplyType string
	}{
		{
			name: "hello_incompatible_protocol",
			// No identity fields; version=1 is below MinimumProtocolVersion, so
			// validateProtocolHandshake fails and the branch replies with a
			// structured error frame via sendFrameViaNetwork.
			authVerified:      false,
			inboundFrameLine:  `{"type":"hello","version":1,"client":"node"}`,
			expectedReplyType: "error",
		},
		{
			name: "hello_no_identity_fields",
			// Current-version hello without Address/PubKey/BoxKey/BoxSig takes
			// the unauthenticated branch and replies with welcomeFrame (empty
			// challenge) via sendFrameViaNetwork.
			authVerified:      false,
			inboundFrameLine:  helloNoIdentityLine(),
			expectedReplyType: "welcome",
		},
		{
			name: "announce_peer_unknown_node_type",
			// Auth-gated: the branch rejects unknown node_type values by
			// sending announce_peer_ack immediately via sendFrameViaNetwork.
			// No peer promotion side-effects execute on this path.
			authVerified:      true,
			inboundFrameLine:  `{"type":"announce_peer","node_type":"future-role-v99"}`,
			expectedReplyType: "announce_peer_ack",
		},
		{
			name: "announce_peer_empty_peers",
			// Auth-gated, known node_type, no peers — the promotion loop is a
			// no-op and the branch sends announce_peer_ack via sendFrameViaNetwork.
			authVerified:      true,
			inboundFrameLine:  `{"type":"announce_peer","node_type":"full","peers":[]}`,
			expectedReplyType: "announce_peer_ack",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := netcoretest.New()
			t.Cleanup(backend.Shutdown)

			svc := NewServiceWithNetwork(config.Node{
				ListenAddress:    "127.0.0.1:0",
				AdvertiseAddress: "127.0.0.1:0",
				Type:             config.NodeTypeFull,
				TrustStorePath:   t.TempDir() + "/trust.json",
				QueueStatePath:   t.TempDir() + "/queue.json",
			}, testIdentityForNetworkConsumerTest(t), backend)
			t.Cleanup(svc.WaitBackground)

			connID := netcore.ConnID(9100 + uint64Hash(tc.name))
			backend.Register(connID, netcore.Inbound, "10.0.0.43:54321")

			clientPipe, serverPipe := net.Pipe()
			t.Cleanup(func() { _ = clientPipe.Close() })
			t.Cleanup(func() { _ = serverPipe.Close() })
			pc := netcore.New(connID, serverPipe, netcore.Inbound, netcore.Options{})
			t.Cleanup(pc.Close)

			if tc.authVerified {
				// Attach Verified=true auth state to the NetCore so
				// isConnAuthenticated returns true for the auth-gated branch.
				// This mirrors the production handleAuthSession →
				// setConnAuthStateByID → pc.SetAuth path at the one seam that
				// matters for the auth gate: pc.Auth().Verified.
				pc.SetAuth(&connauth.State{Verified: true})
			}

			// Dual registration so netCoreForID(connID) — consulted by
			// isConnAuthenticated, addBanScore, rememberConnPeerAddr, and
			// connHasCapability — resolves to this test pc. The backend
			// override still owns the Network.SendFrame path, so the outbound
			// bytes surface on backend.Outbound regardless.
			svc.mu.Lock()
			svc.setTestConnEntryLocked(clientPipe, &connEntry{core: pc})
			svc.mu.Unlock()

			if ok := svc.dispatchNetworkFrame(connID, pc, tc.inboundFrameLine); !ok {
				t.Fatalf("dispatchNetworkFrame(%s) returned false; expected accepted=true", tc.name)
			}

			select {
			case data, ok := <-backend.Outbound(connID):
				if !ok {
					t.Fatalf("%s: backend.Outbound(connID) closed before reply arrived", tc.name)
				}
				frame, err := parseFrameLineForTest(data)
				if err != nil {
					t.Fatalf("%s: parse outbound frame: %v (raw=%q)", tc.name, err, data)
				}
				if frame.Type != tc.expectedReplyType {
					t.Fatalf("%s: expected reply type %q on backend.Outbound, got %q (raw=%q)",
						tc.name, tc.expectedReplyType, frame.Type, data)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: timed out waiting for reply on backend.Outbound(connID): "+
					"Service did not route the reply through the injected Network surface — "+
					"the reply was sent via a legacy helper that bypasses s.Network().SendFrame",
					tc.name)
			}
		})
	}
}

// testIdentityForNetworkConsumerTest mints a fresh identity for the POC
// test. Factored into a helper so the Fatalf on identity.Generate failure
// keeps the test body focused on the Network()-consumer assertion.
func testIdentityForNetworkConsumerTest(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return id
}

// parseFrameLineForTest decodes a single outbound wire line produced by the
// Service. The helper is local to this test so the POC has a single,
// obvious dependency on protocol.Frame and does not leak format assumptions
// into other tests.
func parseFrameLineForTest(line []byte) (protocol.Frame, error) {
	// Outbound lines are newline-terminated; json.Unmarshal accepts the
	// trailing newline without complaint, but stripping it makes the
	// assertion output cleaner on failure.
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	var f protocol.Frame
	if err := json.Unmarshal(line, &f); err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}

// helloNoIdentityLine returns a current-protocol-version hello frame
// carrying no identity fields (no Address / PubKey / BoxKey / BoxSig), so
// dispatchNetworkFrame takes the "unauthenticated hello" branch and
// replies with the welcome frame via sendFrameViaNetwork. Built from a
// protocol.Frame value so the test stays in lockstep with
// config.ProtocolVersion — hardcoding a literal version number would
// silently pass the gate after a protocol bump and regress on the next.
func helloNoIdentityLine() string {
	line, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:    "hello",
		Version: config.ProtocolVersion,
		Client:  "node",
	})
	if err != nil {
		// MarshalFrameLine on a literal struct cannot realistically fail;
		// returning an invalid JSON string surfaces the fault at test time
		// instead of panicking at package init.
		return `{"type":"hello","broken":true}`
	}
	return line
}

// uint64Hash is a tiny test-local string hash used to derive distinct
// synthetic ConnIDs per table case, so parallel sub-tests don't collide
// on backend.Outbound(connID) channel keys.
func uint64Hash(s string) uint64 {
	var h uint64 = 1469598103934665603 // FNV-1a offset basis
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211 // FNV-1a prime
	}
	return h & 0xFFFF // bounded so the resulting ConnID stays small and readable
}

// TestDispatchNetworkFrame_SyncReplies_RouteViaNetworkBackendSync asserts
// the runtime contract of the sync reply paths inside dispatchNetworkFrame:
// every fail-fast error reply with `accepted = false` must travel through
// the Service's injected netcore.Network surface via SendFrameSync, not
// through a legacy helper that resolves *netcore.NetCore from s.conns
// directly. If a reply is sent via the legacy helper, the backend's
// Outbound channel stays empty and this test fails on timeout.
//
// The table covers a representative cross-section of the sync call-sites:
//
//   - invalid_json — un-auth fail-fast at function entry (json.Unmarshal
//     error branch).
//   - auth_required_unknown_command — auth-gate sync path: P2P command
//     received on an unauthenticated connection.
//   - unknown_command_authenticated — default-case sync path: unknown
//     frame type on an authenticated connection.
//
// Other sync reply sites (re-hello-reject, invalid-auth-signature,
// auth_session-reply-on-fail, ack_delete-on-fail,
// subscribe_inbox-identity-mismatch) depend on broader Service state
// (connauth initiation map, ban tracking, auth handler internals,
// DeleteTracker, inboundPeerIdentity) whose fixture-wall is out of
// proportion with the value of a runtime POC. Those sites are protected
// by the architectural boundary check run in CI, which fails if a bare
// legacy-helper call is reintroduced anywhere in the package.
//
// Setup pattern per case — identical to the async variant above: pinned
// netcoretest.Backend, dual registration (backend.Register +
// setTestConnEntryLocked), pc.SetAuth for auth-gated cases.
func TestDispatchNetworkFrame_SyncReplies_RouteViaNetworkBackendSync(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		authVerified      bool
		inboundFrameLine  string
		expectedReplyType string
		expectedReplyCode string
	}{
		{
			name: "invalid_json",
			// json.Unmarshal fails on the leading fail-fast branch and the
			// reply is sent via sendFrameViaNetworkSync. No auth setup.
			authVerified:      false,
			inboundFrameLine:  `{not json`,
			expectedReplyType: "error",
			expectedReplyCode: protocol.ErrCodeInvalidJSON,
		},
		{
			name: "auth_required_unknown_command",
			// isP2PWireCommand("get_peers") == true on an unauthenticated
			// connection → error reply with ErrCodeAuthRequired via
			// sendFrameViaNetworkSync.
			authVerified:      false,
			inboundFrameLine:  `{"type":"get_peers"}`,
			expectedReplyType: "error",
			expectedReplyCode: protocol.ErrCodeAuthRequired,
		},
		{
			name: "unknown_command_authenticated",
			// Verified=true, unknown frame type → falls through to the
			// default-case sync reply with ErrCodeUnknownCommand via
			// sendFrameViaNetworkSync.
			authVerified:      true,
			inboundFrameLine:  `{"type":"some_unknown_cmd_v99"}`,
			expectedReplyType: "error",
			expectedReplyCode: protocol.ErrCodeUnknownCommand,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := netcoretest.New()
			t.Cleanup(backend.Shutdown)

			svc := NewServiceWithNetwork(config.Node{
				ListenAddress:    "127.0.0.1:0",
				AdvertiseAddress: "127.0.0.1:0",
				Type:             config.NodeTypeFull,
				TrustStorePath:   t.TempDir() + "/trust.json",
				QueueStatePath:   t.TempDir() + "/queue.json",
			}, testIdentityForNetworkConsumerTest(t), backend)
			t.Cleanup(svc.WaitBackground)

			connID := netcore.ConnID(9200 + uint64Hash(tc.name))
			backend.Register(connID, netcore.Inbound, "10.0.0.44:55555")

			clientPipe, serverPipe := net.Pipe()
			t.Cleanup(func() { _ = clientPipe.Close() })
			t.Cleanup(func() { _ = serverPipe.Close() })
			pc := netcore.New(connID, serverPipe, netcore.Inbound, netcore.Options{})
			t.Cleanup(pc.Close)

			if tc.authVerified {
				pc.SetAuth(&connauth.State{Verified: true})
			}

			svc.mu.Lock()
			svc.setTestConnEntryLocked(clientPipe, &connEntry{core: pc})
			svc.mu.Unlock()

			// Sync error paths return accepted=false; the dispatch return
			// value still says "false" because the frame was not accepted
			// as a protocol operation. That is orthogonal to the pong /
			// async-reply path asserted above, so we do not check the
			// return here — we check that the reply surfaces on the
			// injected Network surface.
			svc.dispatchNetworkFrame(connID, pc, tc.inboundFrameLine)

			select {
			case data, ok := <-backend.Outbound(connID):
				if !ok {
					t.Fatalf("%s: backend.Outbound(connID) closed before reply arrived", tc.name)
				}
				frame, err := parseFrameLineForTest(data)
				if err != nil {
					t.Fatalf("%s: parse outbound frame: %v (raw=%q)", tc.name, err, data)
				}
				if frame.Type != tc.expectedReplyType {
					t.Fatalf("%s: expected reply type %q on backend.Outbound, got %q (raw=%q)",
						tc.name, tc.expectedReplyType, frame.Type, data)
				}
				if frame.Code != tc.expectedReplyCode {
					t.Fatalf("%s: expected reply code %q on backend.Outbound, got %q (raw=%q)",
						tc.name, tc.expectedReplyCode, frame.Code, data)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: timed out waiting for sync reply on backend.Outbound(connID): "+
					"Service did not route the reply through the injected Network surface — "+
					"the reply was sent via writeJSONFrameSyncByID that bypasses s.Network().SendFrameSync",
					tc.name)
			}
		})
	}
}
