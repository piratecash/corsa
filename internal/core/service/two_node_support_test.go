package service

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// two_node_support_test.go is the harness for the tests that have to prove
// something about the WIRE rather than about a function call.
//
// Everything else in this package builds one client and hands payloads to
// handlers directly, which is the right trade for logic: it is fast, it is
// deterministic, and the transport is somebody else's subject. But a feature
// whose whole claim is "both sides end up agreeing" has to be shown doing that
// through the thing that can disagree — two identities, two databases, two
// nodes, a socket between them, and no seam anywhere in the middle.

// freeLoopbackAddress reserves a port and hands it back as an address. The
// listener is closed immediately: nodes bind their own, and the window between
// is the standard, accepted race of every port-picking test helper.
func freeLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return address
}

// runningClient is one side of the pair: a desktop client over a node that is
// actually listening, plus the control DMs its node hands up.
type runningClient struct {
	*DesktopClient
	address string
	control chan protocol.LocalChangeEvent
}

// startClientNode builds a client whose node runs for real on the given
// address, bootstrapping to `peers`.
func startClientNode(t *testing.T, address string, peers ...string) *runningClient {
	t.Helper()

	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	cfg := config.Node{
		ListenAddress:  address,
		BootstrapPeers: peers,
		TrustStorePath: filepath.Join(dir, "trust.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"),
		// The two nodes talk over loopback, which the peer filter refuses by
		// default — it exists to stop a mesh peer advertising a private
		// address, not to stop a test from using one.
		AllowPrivatePeers: true,
	}

	database := newTestStateDB(t, domain.PeerIdentityFromWire(id.Address))
	store := testChatlogStore(database.Executor(), domain.PeerIdentityFromWire(id.Address))

	// Control DMs reach the application through the event bus, not through
	// SubscribeLocalChanges: storeIncomingMessage publishes them on
	// TopicMessageControl and deliberately keeps them out of the chat stream.
	// This is the same subscription DMRouter.start makes.
	control := make(chan protocol.LocalChangeEvent, 16)
	bus := ebus.New()
	svc := node.NewService(cfg, id, bus)
	bus.Subscribe(ebus.TopicMessageControl, func(event protocol.LocalChangeEvent) {
		control <- event
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		bus.Shutdown()
		svc.WaitBackground()
	})

	c := &DesktopClient{
		id:        id,
		appCfg:    config.App{Version: "test"},
		localNode: svc,
		chatLog:   store,
	}
	c.wireSubServices()
	return &runningClient{DesktopClient: c, address: address, control: control}
}

// trustEachOther imports both identities into both nodes. Without the keys
// nothing decrypts, and a control DM from an unknown sender is dropped before
// it is even parsed.
func trustEachOther(t *testing.T, a, b *runningClient) {
	t.Helper()
	trust(t, a.DesktopClient, b.id)
	trust(t, b.DesktopClient, a.id)
}

// trust imports the identity's keys as a contact of the client.
func trust(t *testing.T, c *DesktopClient, peer *identity.Identity) {
	t.Helper()
	reply := c.localNode.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: peer.Address,
			PubKey:  identity.PublicKeyBase64(peer.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(peer),
		}},
	})
	if reply.Type == "error" {
		t.Fatalf("import_contacts: %s", reply.Code)
	}
}

// waitForSession blocks until the client's node reports a live connection to
// the peer. A control DM sent before that is queued rather than delivered, and
// a test that raced it would fail for the wrong reason.
func waitForSession(t *testing.T, c *runningClient, peer domain.PeerIdentity) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if connectedTo(c, peer) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no session with %s after 20s", peer)
}

func connectedTo(c *runningClient, peer domain.PeerIdentity) bool {
	reply := c.localNode.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"})
	for _, health := range reply.PeerHealth {
		if health.PeerID == peer.String() && health.Connected {
			return true
		}
	}
	return false
}

// awaitControlDM waits for the next control DM the node hands up, and fails
// with a useful message rather than a timeout panic.
func awaitControlDM(t *testing.T, c *runningClient) protocol.LocalChangeEvent {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case event, open := <-c.control:
			if !open {
				t.Fatal("the node's local-change stream closed before the control DM arrived")
			}
			if event.Type == protocol.LocalChangeNewControlMessage {
				return event
			}
		case <-deadline:
			t.Fatal("no control DM arrived over the wire within 20s")
		}
	}
}

// String makes a failure message name the side rather than a pointer.
func (c *runningClient) String() string {
	return fmt.Sprintf("node(%s)", c.address)
}
