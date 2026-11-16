package node

import (
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// session_restart_v2_requirement_test.go pins what still keeps v1 out after
// a real restart of the node on the data directory it ran on: the pin store,
// never the in-memory marks (docs/protocol/session_v2.md, "After a restart").
//
// Before the restart this node proves v2 with X through an address it dials,
// which pins X and binds that address. After it, X's identity answers as an
// old release, and the switch decides nothing:
//   - dialled by a bare address, X is known only from the probe's answer — the
//     probe runs and the answer is refused, the full hello never sent;
//   - where the identity is known BEFORE the dial — the address bound to v2,
//     or a dial "to X" — neither the probe nor v1 happens: TLS alone.

// restartSessionForwarder gives the listener a separate dial address.
type restartSessionForwarder struct {
	listener net.Listener
	target   string
	done     sync.WaitGroup
}

func startRestartSessionForwarder(t *testing.T, target string) *restartSessionForwarder {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	forwarder := &restartSessionForwarder{listener: listener, target: target}
	forwarder.done.Add(1)
	go forwarder.acceptLoop()
	t.Cleanup(func() { _ = listener.Close(); forwarder.done.Wait() })
	return forwarder
}

func (f *restartSessionForwarder) address() domain.PeerAddress {
	return domain.PeerAddress(f.listener.Addr().String())
}

func (f *restartSessionForwarder) acceptLoop() {
	defer f.done.Done()
	for {
		client, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.done.Add(1)
		go f.relay(client)
	}
}

func (f *restartSessionForwarder) relay(client net.Conn) {
	defer f.done.Done()
	defer func() { _ = client.Close() }()
	server, err := net.Dial("tcp", f.target)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(server, client)
		_ = server.Close()
		close(copied)
	}()
	_, _ = io.Copy(client, server)
	_ = client.Close()
	<-copied
}

// restartableNodeConfig is a quiet node whose every file lives in dir, so a
// second start on the same config finds what the first one wrote.
func restartableNodeConfig(t *testing.T, dir string) config.Node {
	t.Helper()
	listen := freeAddress(t)
	return config.Node{
		ListenAddress:  listen,
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
		IdentityPath:   filepath.Join(dir, "identity-"+config.PortSuffix(listen)+".json"),
		PeersStatePath: filepath.Join(dir, "peers-"+config.PortSuffix(listen)+".json"),
		TrustStorePath: filepath.Join(dir, "trust-"+config.PortSuffix(listen)+".json"),
		ChatLogDir:     dir,
	}
}

func TestAfterARestartThePinStoreKeepsV1OutForAPinnedIdentity(t *testing.T) {
	t.Parallel()
	nodeX, addressX, stopX := listenerNode(t, withoutOwnDials(nil))
	idX := peerIdentityOf(t, nodeX)
	bound := startRestartSessionForwarder(t, addressX)

	cfg := restartableNodeConfig(t, t.TempDir())
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	// Before the restart: a proven v2 session through the forwarder pins X
	// and binds the forwarder's address.
	first, stopFirst := startTestNodeAs(t, cfg, self, withoutOwnDials(nil))
	if established, err := dialPastTransport(t, first, bound.address()); !established {
		t.Fatalf("the v2 dial before the restart was not established: %v", err)
	}
	if !first.secureSessions.store.identityPinned(idX) || !first.secureSessions.store.endpointRequiresV2(bound.address()) {
		t.Fatal("premise: the v2 session did not pin X and bind its address")
	}
	stopFirst()
	stopX()
	restarted, stopRestarted := startTestNodeAs(t, cfg, self, withoutOwnDials(nil))
	defer stopRestarted()
	marks := restarted.secureSessions.marks
	// The restart contract itself: the marks are memory, the pin is the store.
	if marks.v2Strong(bound.address()) {
		t.Fatal("v2_strong outlived the restart")
	}
	if !restarted.secureSessions.store.identityPinned(idX) || !restarted.secureSessions.store.endpointRequiresV2(bound.address()) {
		t.Fatal("the restart lost the pin of X or the binding of its address")
	}

	for _, dial := range []struct {
		name    string
		address domain.PeerAddress
		intent  domain.DialIntent
	}{
		{"unknown address", "192.0.2.1:64646", domain.DialToAddress()},
		{"named identity", "192.0.2.2:64646", dialToIdentity(t, idX)},
		{"bound address", bound.address(), domain.DialToAddress()},
	} {
		t.Run(dial.name, func(t *testing.T) {
			peer := newScriptedPeer(answerClientHello(v1ErrorLine))
			if _, err := dialThrough(t, restarted, dial.address, dial.intent, peer); !errors.Is(err, errV1RefusedV2Only) {
				t.Fatalf("expected no plaintext dial, got %v", err)
			}
			if sockets := peer.sockets(t); len(sockets) != 1 || !isClientHello(sockets[0]) {
				t.Fatalf("expected one TLS attempt, wrote %q", sockets)
			}
		})
	}
}
