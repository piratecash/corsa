package node

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// connected_hosts_test.go pins which inbound connections reserve their host
// against outbound dials: one that completed authentication (v1 auth_session
// or v2 session_proof) does; a silent socket, a hello without auth and an
// unfinished handshake do not. Only the closing of a tracked connection can
// wake the ConnectionManager (InboundClosed); a host excluded by anything
// else stays excluded until the next periodic fill, thirty seconds later.

// loopbackSocketPair returns both ends of one TCP connection on 127.0.0.1,
// so the accepted end reports a real transport IP.
func loopbackSocketPair(t *testing.T) (client, server net.Conn) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	client, err = net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	server, ok := <-accepted
	if !ok {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() {
		// Both ends only have to live for the test; a close error afterwards
		// cannot change its outcome.
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

func newServiceWithBootstrapPeer(t *testing.T, bootstrap domain.PeerAddress) *Service {
	t.Helper()

	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	cfg := deriveTestAdvertisePort(config.Node{
		ListenAddress:     freeAddress(t),
		BootstrapPeers:    []string{string(bootstrap)},
		Type:              domain.NodeTypeFull,
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		AllowPrivatePeers: true,
	})
	return NewService(cfg, id, nil)
}

func candidatesContain(svc *Service, address domain.PeerAddress) bool {
	for _, candidate := range svc.peerProvider.Candidates() {
		if candidate.Address == address {
			return true
		}
	}
	return false
}

func TestOnlyAnAuthenticatedInboundConnectionReservesItsHost(t *testing.T) {
	t.Parallel()

	// tracked is set by completeInboundAuth alone, after a v1 auth_session or
	// a v2 session_proof. Before that — a silent socket, a hello without
	// auth, a handshake still running — the connection is untracked.
	reservesHost := map[string]struct {
		tracked bool
		want    bool
	}{
		"an unauthenticated inbound socket": {tracked: false, want: false},
		"an authenticated inbound peer":     {tracked: true, want: true},
	}
	for name, tc := range reservesHost {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			bootstrap := domain.PeerAddress("127.0.0.1:" + config.DefaultPeerPort)
			svc := newServiceWithBootstrapPeer(t, bootstrap)
			if !candidatesContain(svc, bootstrap) {
				t.Fatalf("precondition: bootstrap %s is not a candidate with no connection at all", bootstrap)
			}

			_, server := loopbackSocketPair(t)
			core := netcore.New(netcore.ConnID(1), server, netcore.Inbound, netcore.Options{})
			svc.peerMu.Lock()
			svc.setTestConnEntryLocked(server, &connEntry{core: core, tracked: tc.tracked})
			svc.peerMu.Unlock()

			if got := !candidatesContain(svc, bootstrap); got != tc.want {
				t.Fatalf("host 127.0.0.1 excluded from candidates = %v, want %v", got, tc.want)
			}
		})
	}
}

// The flake this pins: an old node's own readiness probe — or any
// unauthenticated socket from the bootstrap peer's host — was registered when the
// first fill ran. The fill dropped the bootstrap peer as "already connected",
// the socket closed without waking the ConnectionManager, and nothing dialled
// until the periodic fill thirty seconds later.
func TestAnOldNodeDialsItsBootstrapPeerWhileASilentSocketFromItsHostIsOpen(t *testing.T) {
	t.Parallel()

	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()

	barrierReached := make(chan struct{})
	releaseFill := make(chan struct{})

	old, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{normalizeAddress(address)},
		Type:           domain.NodeTypeFull,
	}, func(svc *Service) {
		// An old node registers an inbound socket before its first byte;
		// a new one blocks on the peek and never registers a silent socket.
		asOldNode(svc)
		svc.bootstrapReadyBarrier = func() {
			close(barrierReached)
			// The node's own context ends the wait too, so a test that fails
			// before it releases the fill can still stop the node.
			select {
			case <-releaseFill:
			case <-svc.runCtx.Done():
			}
		}
	})
	defer stopOld()

	select {
	case <-barrierReached:
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrapLoop never reached the barrier before the first fill (waited 10s)")
	}
	silent, err := net.Dial("tcp", old.cfg.ListenAddress)
	if err != nil {
		t.Fatalf("dial the old node: %v", err)
	}
	defer func() { _ = silent.Close() }()
	waitForConditionMsg(t, 5*time.Second, "the silent socket was never registered", func() bool {
		return hasInboundFrom(old, silent.LocalAddr().String())
	})

	close(releaseFill)
	waitForConditionMsg(t, 10*time.Second, "the old node never dialled its bootstrap peer", func() bool {
		_, v1 := authenticatedInbound(listener)
		return v1 > 0
	})
}

// hasInboundFrom reports whether svc has registered an inbound connection
// from remote, authenticated or not.
func hasInboundFrom(svc *Service, remote string) bool {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	found := false
	svc.forEachInboundConnLocked(func(info connInfo) bool {
		found = info.remoteAddr == remote
		return !found
	})
	return found
}
