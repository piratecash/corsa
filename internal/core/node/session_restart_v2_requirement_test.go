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
	"github.com/piratecash/corsa/internal/core/sessionv2"
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

// switchableForwarder relays TCP to a target that can change between
// connections, and records the first byte each connection opened with — a
// TLS ClientHello or a v1 line.
type switchableForwarder struct {
	listener net.Listener

	mu       sync.Mutex
	target   string
	openings []byte
	done     sync.WaitGroup
}

func startSwitchableForwarder(t *testing.T, target string) *switchableForwarder {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	forwarder := &switchableForwarder{listener: listener, target: target}
	forwarder.done.Add(1)
	go forwarder.acceptLoop()
	t.Cleanup(func() { _ = listener.Close(); forwarder.done.Wait() })
	return forwarder
}

func (f *switchableForwarder) address() domain.PeerAddress {
	return domain.PeerAddress(f.listener.Addr().String())
}

// retarget sends the connections accepted from now on to target.
func (f *switchableForwarder) retarget(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = target
}

// opened is the first byte of every connection accepted so far that wrote one.
func (f *switchableForwarder) opened() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.openings...)
}

func (f *switchableForwarder) acceptLoop() {
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

func (f *switchableForwarder) relay(client net.Conn) {
	defer f.done.Done()
	defer func() { _ = client.Close() }()
	first := make([]byte, 1)
	if _, err := io.ReadFull(client, first); err != nil {
		return
	}
	f.mu.Lock()
	f.openings = append(f.openings, first[0])
	target := f.target
	f.mu.Unlock()
	server, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	if _, err := server.Write(first); err != nil {
		return
	}
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
func restartableNodeConfig(t *testing.T, dir string, tlsFirstDisabled bool) config.Node {
	t.Helper()
	listen := freeAddress(t)
	return config.Node{
		ListenAddress:     listen,
		BootstrapPeers:    []string{},
		Type:              domain.NodeTypeFull,
		IdentityPath:      filepath.Join(dir, "identity-"+config.PortSuffix(listen)+".json"),
		PeersStatePath:    filepath.Join(dir, "peers-"+config.PortSuffix(listen)+".json"),
		TrustStorePath:    filepath.Join(dir, "trust-"+config.PortSuffix(listen)+".json"),
		ChatLogDir:        dir,
		DisableV2TLSFirst: tlsFirstDisabled,
	}
}

func TestAfterARestartThePinStoreKeepsV1OutForAPinnedIdentity(t *testing.T) {
	t.Parallel()
	for _, tlsFirstDisabled := range []bool{false, true} {
		name := map[bool]string{false: "tls-first on", true: "tls-first off"}[tlsFirstDisabled]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			checkPinStoreKeepsV1OutAfterRestart(t, tlsFirstDisabled)
		})
	}
}

func checkPinStoreKeepsV1OutAfterRestart(t *testing.T, tlsFirstDisabled bool) {
	nodeX, addressX, stopX := listenerNode(t, withoutOwnDials(nil))
	idX := peerIdentityOf(t, nodeX)
	ownerX := nodeX.identity
	bound := startSwitchableForwarder(t, addressX)

	cfg := restartableNodeConfig(t, t.TempDir(), tlsFirstDisabled)
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
	openedBeforeRestart := len(bound.opened())

	// X's identity now answers as an old release: at the bound address, and
	// at two bare ones (one per kind of dial, so a refusal's hold on one
	// address does not decide the other's dial).
	oldAddress := freeAddress(t)
	_, stopOld := startTestNodeAs(t, config.Node{
		ListenAddress:  oldAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, ownerX, withoutOwnDials(asOldNode))
	defer stopOld()
	bare := startLegacyVersionProxy(t, oldAddress, oldPeerListens, config.ProtocolVersionSecureSession-1)
	named := startLegacyVersionProxy(t, oldAddress, oldPeerListens, config.ProtocolVersionSecureSession-1)
	behindBound := startLegacyVersionProxy(t, oldAddress, oldPeerListens, config.ProtocolVersionSecureSession-1)
	bound.retarget(behindBound.address())

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

	t.Run("a bare address is probed and refused on the answer", func(t *testing.T) {
		established, err := dialPastTransport(t, restarted, bare.peerAddress())
		if established || !errors.Is(err, errLegacyRefusedPinned) {
			t.Fatalf("established=%v err=%v, want errLegacyRefusedPinned", established, err)
		}
		bare.drain(t)
		hellos := bare.nodeHellos()
		if len(hellos) != 1 || hellos[0] {
			t.Fatalf("hellos (identified?) = %v, want the identity-less probe alone", hellos)
		}
		if wantTLS := map[bool]int{false: 1, true: 0}[tlsFirstDisabled]; bare.clientHellos() != wantTLS {
			t.Fatalf("%d ClientHellos, want %d", bare.clientHellos(), wantTLS)
		}
		if marks.legacyObserved(bare.peerAddress()) {
			t.Fatal("a refused answer marked the address as an old node")
		}
	})

	t.Run("a dial to X neither probes nor speaks v1", func(t *testing.T) {
		if err := dialWithIntent(t, restarted, named.peerAddress(), dialToIdentity(t, idX)); err == nil {
			t.Fatal("a dial to X reached a v1 node of X's identity")
		}
		named.drain(t)
		if hellos := named.nodeHellos(); len(hellos) != 0 || named.clientHellos() == 0 {
			t.Fatalf("hellos %v, ClientHellos %d; want TLS attempts alone", hellos, named.clientHellos())
		}
	})

	t.Run("a bound address neither probes nor speaks v1", func(t *testing.T) {
		before := restarted.SecureSessionStoreStats().V1RefusalsEndpointBound
		if established, err := dialPastTransport(t, restarted, bound.address()); established {
			t.Fatalf("the bound address was reached: %v", err)
		}
		behindBound.drain(t)
		if hellos := behindBound.nodeHellos(); len(hellos) != 0 {
			t.Fatalf("hellos %v reached the old node behind the bound address", hellos)
		}
		afterRestart := bound.opened()[openedBeforeRestart:]
		if len(afterRestart) == 0 {
			t.Fatal("no connection through the bound address after the restart")
		}
		for i, first := range afterRestart {
			if first != sessionv2.TLSRecordHandshake {
				t.Fatalf("connection %d after the restart opened with %q, want a ClientHello", i, first)
			}
		}
		if after := restarted.SecureSessionStoreStats().V1RefusalsEndpointBound; after <= before {
			t.Fatalf("v1 refusals at a bound endpoint %d → %d, want the answer refused", before, after)
		}
	})
}
