package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// session_secure_test.go pins how two nodes pick their session kind:
// v2 between new nodes, v1 with an old one in either direction, and no way
// back to v1 for an address that spoke v2.

// asOldNode makes a test node behave like a build before v2: it neither
// accepts nor dials v2 and serves every connection as v1.
func asOldNode(svc *Service) {
	svc.secureSessions = &secureSessions{mode: sessionv2.ModeLegacyOnly}
}

// markOldNode records address as an old node, the state a node is in once
// that address answered a v2 attempt with a v1 line. Tests whose peer is a
// hand-written v1 server that accepts a single connection use it: the
// fallback dial itself is pinned by TestANewNodeDiallingAnOldOneFallsBackToV1.
func markOldNode(svc *Service, address domain.PeerAddress) {
	svc.secureSessions.marks.noteV1(address)
}

// authenticatedInbound reports, for each authenticated inbound connection of
// svc, whether it is a v2 session (the frames travel over TLS, not over the
// metered socket itself).
func authenticatedInbound(svc *Service) (v2, v1 int) {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	for _, entry := range svc.conns {
		core := entry.core
		if core == nil || core.Dir() != netcore.Inbound {
			continue
		}
		if auth := core.Auth(); auth == nil || !auth.Verified {
			continue
		}
		if _, plain := core.Conn().(*netcore.MeteredConn); plain {
			v1++
		} else {
			v2++
		}
	}
	return v2, v1
}

// marksOf is how many endpoints svc has bound to v2 and how many addresses
// it has marked as old nodes.
func marksOf(svc *Service) (bound, v1 int) {
	store, marks := svc.secureSessions.store, svc.secureSessions.marks
	store.mu.Lock()
	bound = len(store.endpoints)
	store.mu.Unlock()
	marks.mu.Lock()
	defer marks.mu.Unlock()
	return bound, len(marks.v1)
}

func listenerNode(t *testing.T, setup func(*Service)) (*Service, string, func()) {
	t.Helper()
	address := freeAddress(t)
	svc, stop := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  address,
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	}, setup)
	return svc, address, stop
}

func dialerNode(t *testing.T, listener string, setup func(*Service)) (*Service, func()) {
	t.Helper()
	return startTestNodeWithSetup(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{normalizeAddress(listener)},
		Type:           domain.NodeTypeFull,
	}, setup)
}

func TestTwoNewNodesConnectOverV2(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	waitForConditionMsg(t, 10*time.Second, "no v2 session between two new nodes", func() bool {
		v2, _ := authenticatedInbound(listener)
		seen, _ := marksOf(dialer)
		return v2 > 0 && seen > 0
	})
	if _, v1 := authenticatedInbound(listener); v1 != 0 {
		t.Fatalf("%d v1 sessions between two new nodes", v1)
	}
}

func TestANewNodeDiallingAnOldOneFallsBackToV1(t *testing.T) {
	t.Parallel()
	old, address, stopOld := listenerNode(t, asOldNode)
	defer stopOld()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	waitForConditionMsg(t, 15*time.Second, "the new node never reached the old one over v1", func() bool {
		_, v1 := authenticatedInbound(old)
		_, seenV1 := marksOf(dialer)
		return v1 > 0 && seenV1 > 0
	})
}

func TestAnOldNodeDiallingANewOneIsServedOverV1(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	_, stopOld := dialerNode(t, address, asOldNode)
	defer stopOld()

	waitForConditionMsg(t, 10*time.Second, "the new node never served the old one over v1", func() bool {
		_, v1 := authenticatedInbound(listener)
		return v1 > 0
	})
	if v2, _ := authenticatedInbound(listener); v2 != 0 {
		t.Fatalf("%d v2 sessions with a node that cannot speak v2", v2)
	}
}

// An address that answered with TLS before is not followed back to v1: a
// v1 answer from it now is what a man in the middle stripping TLS looks
// like, and following it would hand him the relayable v1 proof.
func TestAnAddressThatSpokeV2IsNotDowngraded(t *testing.T) {
	t.Parallel()
	old, address, stopOld := listenerNode(t, asOldNode)
	defer stopOld()
	dialer, stopDialer := dialerNode(t, address, func(svc *Service) {
		if err := svc.secureSessions.store.noteProvenOutbound(domain.PeerAddress(normalizeAddress(address)), domain.PeerIdentity{1}, false); err != nil {
			t.Fatalf("bind: %v", err)
		}
	})
	defer stopDialer()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, v1 := authenticatedInbound(old); v1 > 0 {
			t.Fatal("a v2-seen address was downgraded to v1")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, seenV1 := marksOf(dialer); seenV1 != 0 {
		t.Fatal("the downgrade attempt was recorded as a v1 node")
	}
}

// A socket that connects and sends nothing waits for its first byte before
// it is registered, so the registry's close-all at shutdown does not see it.
// Stopping the node must still end its wait at once rather than after the
// read timeout.
func TestASilentInboundSocketDoesNotHoldShutdown(t *testing.T) {
	t.Parallel()
	_, address, stop := listenerNode(t, nil)
	silent := dialWhenListening(t, address)
	defer func() { _ = silent.Close() }()
	time.Sleep(100 * time.Millisecond) // let the handler reach its first-byte wait

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(4 * time.Second):
		t.Fatal("the node did not stop while a silent socket waited for its first byte")
	}
}

func peerIdentityOf(t *testing.T, svc *Service) domain.PeerIdentity {
	t.Helper()
	id, err := domain.ParsePeerIdentity(svc.identity.Address)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return id
}

// assertNoV1SessionFor watches node for a while and fails on any
// authenticated v1 inbound session.
func assertNoV1SessionFor(t *testing.T, node *Service, d time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, v1 := authenticatedInbound(node); v1 > 0 {
			t.Fatal(what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// An identity that proved v2 is refused over v1 on accept: the v1 hello may
// be a man in the middle collecting a relayable proof.
func TestAPinnedIdentityIsRefusedOverV1OnAccept(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	_, stopOld := dialerNode(t, address, func(old *Service) {
		asOldNode(old)
		_ = listener.secureSessions.store.noteProvenInbound(peerIdentityOf(t, old))
	})
	defer stopOld()
	assertNoV1SessionFor(t, listener, 3*time.Second, "a v1 session was accepted for an identity pinned to v2")
}

// The dialling side: an old node answering with a welcome that names a
// pinned identity is not served over v1 — nothing is signed for it.
func TestAPinnedIdentityIsRefusedInAV1Welcome(t *testing.T) {
	t.Parallel()
	var oldID domain.PeerIdentity
	old, address, stopOld := listenerNode(t, func(svc *Service) {
		asOldNode(svc)
		oldID = peerIdentityOf(t, svc)
	})
	defer stopOld()
	_, stopDialer := dialerNode(t, address, func(svc *Service) {
		_ = svc.secureSessions.store.noteProvenInbound(oldID)
	})
	defer stopDialer()
	assertNoV1SessionFor(t, old, 3*time.Second, "the dialler signed a v1 session for an identity pinned to v2")
}

// A recovery dial to a peer that speaks v2 goes over v2 too: the one-shot
// paths are no side door to the relayable v1 proof.
func TestARecoverySyncToAV2PeerUsesV2(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := startTestNode(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{}, Type: domain.NodeTypeFull})
	defer stopDialer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialer.syncPeer(ctx, domain.PeerAddress(normalizeAddress(address)), false)
	waitForConditionMsg(t, 5*time.Second, "the recovery dial did not open a v2 session", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 > 0
	})
	if _, v1 := authenticatedInbound(listener); v1 != 0 {
		t.Fatalf("%d v1 sessions from a recovery dial to a v2 peer", v1)
	}
}

// A recovery dial to an endpoint bound to v2 that now answers like an old
// node fails instead of falling back.
func TestARecoverySyncToABoundEndpointIsNotDowngraded(t *testing.T) {
	t.Parallel()
	old, address, stopOld := listenerNode(t, asOldNode)
	defer stopOld()
	bound := domain.PeerAddress(normalizeAddress(address))
	dialer, stopDialer := startTestNodeWithSetup(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{}, Type: domain.NodeTypeFull}, func(svc *Service) {
		if err := svc.secureSessions.store.noteProvenOutbound(bound, domain.PeerIdentity{1}, false); err != nil {
			t.Fatalf("bind: %v", err)
		}
	})
	defer stopDialer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialer.syncPeer(ctx, bound, false)
	assertNoV1SessionFor(t, old, time.Second, "the recovery dial fell back to v1 on an endpoint bound to v2")
	if _, v1 := marksOf(dialer); v1 != 0 {
		t.Fatal("the downgrade was recorded as an old node")
	}
}

// The pin a v2 session earns must be on disk before the session counts. With
// the store failing to write, the listener establishes nothing; once the
// disk recovers, the reconnect is established AND its pin survives a
// restart (the store reloaded from the same file).
func TestAV2SessionWaitsForItsPinToBeWritten(t *testing.T) {
	t.Parallel()
	var failing atomic.Bool
	failing.Store(true)
	listener, address, stopListener := listenerNode(t, func(svc *Service) {
		store := svc.secureSessions.store
		store.writeFile = func(raw []byte) error {
			if failing.Load() {
				return errors.New("disk full")
			}
			return store.writeToDisk(raw)
		}
	})
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v2, _ := authenticatedInbound(listener); v2 > 0 {
			t.Fatal("a v2 session was established while its pin could not be written")
		}
		time.Sleep(50 * time.Millisecond)
	}
	failing.Store(false)
	waitForConditionMsg(t, 15*time.Second, "no v2 session after the disk recovered", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 > 0
	})
	reloaded := loadSecureSessionStore(secureSessionStorePath(listener.cfg), time.Now)
	if !reloaded.identityPinned(peerIdentityOf(t, dialer)) {
		t.Fatal("the pin of the established session is not on disk: a restart would forget it")
	}
}

// A binding to a contact is protected. Thirty-one days on, whoever answers
// on that endpoint over v1 — here an old node with another, unpinned
// identity — is a downgrade, and the dial does not fall back.
func TestAContactEndpointRefusesASubstituteIdentityAfterThirtyDays(t *testing.T) {
	t.Parallel()
	old, address, stopOld := listenerNode(t, asOldNode)
	defer stopOld()
	bound := domain.PeerAddress(normalizeAddress(address))
	_, stopDialer := dialerNode(t, address, func(svc *Service) {
		clock := newStoreClock()
		store := loadSecureSessionStore("", clock.read)
		if err := store.noteProvenOutbound(bound, domain.PeerIdentity{7}, true); err != nil {
			t.Fatalf("bind: %v", err)
		}
		clock.now = clock.now.Add(endpointBindingTTL + 24*time.Hour)
		svc.secureSessions.store = store
	})
	defer stopDialer()
	assertNoV1SessionFor(t, old, 3*time.Second, "a protected endpoint fell back to v1 for a substitute identity after 30 days")
}

// Shutdown on the boundary between a connection's first byte and its
// registration: the drain has closed every registered socket, and this one
// registers only now. It must be refused, or it holds Run's connWg until its
// read timeout.
func TestAConnectionRegisteringAfterTheShutdownDrainIsRefused(t *testing.T) {
	t.Parallel()
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	listener, address, stop := listenerNode(t, func(svc *Service) {
		svc.inboundBeforeRegister = func() {
			once.Do(func() { close(reached) })
			<-release
		}
	})
	client := dialWhenListening(t, address)
	defer func() { _ = client.Close() }()
	if _, err := client.Write([]byte("{")); err != nil {
		t.Fatalf("first byte: %v", err)
	}
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never reached registration")
	}

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	waitForConditionMsg(t, 5*time.Second, "the shutdown drain never ran", func() bool {
		listener.peerMu.RLock()
		defer listener.peerMu.RUnlock()
		return listener.inboundClosed
	})
	close(release)
	select {
	case <-stopped:
	case <-time.After(4 * time.Second):
		t.Fatal("a connection registered after the shutdown drain held the node open")
	}
}

// The dialling side waits for its protection too: with the store failing to
// write, the dialler establishes no session; once the disk recovers it does,
// and the endpoint binding is on disk.
func TestADialledV2SessionWaitsForItsBindingToBeWritten(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	var failing atomic.Bool
	failing.Store(true)
	dialer, stopDialer := dialerNode(t, address, func(svc *Service) {
		store := svc.secureSessions.store
		store.writeFile = func(raw []byte) error {
			if failing.Load() {
				return errors.New("disk full")
			}
			return store.writeToDisk(raw)
		}
	})
	defer stopDialer()
	sessions := func() int {
		dialer.peerMu.RLock()
		defer dialer.peerMu.RUnlock()
		return len(dialer.sessions)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sessions() > 0 {
			t.Fatal("a dialled v2 session was established while its binding could not be written")
		}
		time.Sleep(50 * time.Millisecond)
	}
	failing.Store(false)
	waitForConditionMsg(t, 15*time.Second, "no dialled session after the disk recovered", func() bool { return sessions() > 0 })
	reloaded := loadSecureSessionStore(secureSessionStorePath(dialer.cfg), time.Now)
	if !reloaded.endpointRequiresV2(domain.PeerAddress(normalizeAddress(address))) {
		t.Fatal("the binding of the established session is not on disk")
	}
}
