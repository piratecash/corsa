package node

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
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
