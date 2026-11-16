package node

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// session_secure_test.go pins how two nodes pick their session kind:
// v2 between new nodes, v1 with an old one in either direction, and no way
// back to v1 for an address that spoke v2.

// asOldNode makes a test node behave like a build before v2: it neither
// accepts nor dials v2 and serves every connection as v1.
func asOldNode(svc *Service) {
	svc.secureSessions = &secureSessions{mode: sessionv2.ModeLegacyOnly, marks: newSessionAddressMarks(time.Now)}
}

// asLegacyOnlyDialler makes svc dial v1 directly, as a build without v2
// does, for a test whose peer is a hand-written v1 server. Such a server
// answers a v1 hello at once, and it can answer neither a TLS attempt nor the
// identity-less probe the way a released old node does; the test's subject is
// the v1 exchange after the welcome, not how the session kind is chosen.
// That choice — TLS, then the probe, then v1 on the probe's connection — is
// pinned by session_dial_choice_test.go and run against an emulated old
// release by TestDiallingAnOldReleaseGoesTLSThenProbeThenLegacy.
func asLegacyOnlyDialler(svc *Service) {
	asOldNode(svc)
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
		return v2 > 0 && dialer.secureSessions.store.endpointRequiresV2(domain.PeerAddress(address))
	})
	if _, v1 := authenticatedInbound(listener); v1 != 0 {
		t.Fatalf("%d v1 sessions between two new nodes", v1)
	}
}

// v2.5.71 with TLS-first disabled still needs the identity-less probe
// to redirect it to a fresh TLS connection on first contact.
func TestFloor31ProbeRedirectsWithoutServingPlaintext(t *testing.T) {
	t.Parallel()
	listener, address, stop := listenerNode(t, nil)
	defer stop()
	probe, err := legacyProbeLineForVersion(config.ProtocolVersionSecureSession)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", address, handshakeTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(probe); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	line, err := readFrameLine(reader, protocol.MaxFrameLine)
	if err != nil {
		t.Fatal(err)
	}
	hint, err := protocol.ParseFrameLine(line)
	if err != nil {
		t.Fatal(err)
	}
	if hint.Type != "welcome" || hint.Version < 31 || hint.Network != networkName || hint.Challenge != "" {
		t.Fatalf("unexpected TLS hint: %+v", hint)
	}
	if hint.MinimumProtocolVersion != 31 || hint.Address != "" || hint.PubKey != "" {
		t.Fatalf("unexpected protocol hint: %+v", hint)
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("probe connection remained open: %v", err)
	}
	if v2, v1 := authenticatedInbound(listener); v2 != 0 || v1 != 0 {
		t.Fatalf("probe registered a session, v2=%d v1=%d", v2, v1)
	}
}

func TestFloor31RefusesObsoleteProbeAndFullHello(t *testing.T) {
	t.Parallel()
	_, address, stop := listenerNode(t, nil)
	defer stop()
	oldProbe, err := legacyProbeLineForVersion(config.ProtocolVersionSecureSession - 1)
	if err != nil {
		t.Fatal(err)
	}
	fullHello, err := protocol.MarshalFrameLineBytes(protocol.Frame{
		Type: "hello", Version: config.ProtocolVersionSecureSession, Client: "node",
		Network: networkName, Address: "presented-identity",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range [][]byte{oldProbe, fullHello} {
		conn, err := net.DialTimeout("tcp", address, handshakeTimeout)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(line); err != nil {
			t.Fatal(err)
		}
		var one [1]byte
		if n, err := conn.Read(one[:]); n != 0 || err == nil {
			t.Fatalf("refused plaintext request received a reply: %q, err=%v", one[:n], err)
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			t.Fatalf("refused plaintext request left its connection open: %v", err)
		}
	}
}

func TestFloor31RefusesOldVersionOverTLSBeforePin(t *testing.T) {
	t.Parallel()
	listener, address, stop := listenerNode(t, nil)
	defer stop()
	dialer := newTestService(t, config.NodeTypeFull)
	hello := dialer.nodeHelloFrame()
	hello.Version, hello.MinimumProtocolVersion = 30, 30
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	session, err := sessionv2.Dial(ctx, raw, dialer.secureSessions.local, hello, sessionv2.ExpectedPeer{}, sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatalf("establish TLS proof before version refusal: %v", err)
	}
	wire, err := session.Conn()
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if n, err := wire.Read(one[:]); n != 0 || err == nil {
		t.Fatalf("unsupported version received a session frame: n=%d err=%v", n, err)
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("unsupported version left its session open: %v", err)
	}
	if listener.identityRequiresV2(domain.PeerIdentityFromWire(dialer.Address())) {
		t.Fatal("unsupported version acquired an identity pin")
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

// refusalThenHolds counts a first refusal of the kind primary reads and,
// after it, every attempt the backoffs it started held — the fall back to v1
// held, or the dial held by the v2 retry backoff. 0 until the first refusal.
func refusalThenHolds(svc *Service, primary func(domain.SecureSessionStoreStats) uint64) func() uint64 {
	return func() uint64 {
		stats := svc.SecureSessionStoreStats()
		first := primary(stats)
		if first == 0 {
			return 0
		}
		return first + stats.V1DialsHeldByRefusalBackoff + stats.DialsHeldByV2RetryBackoff
	}
}

// awaitRepeatedRefusal waits until refusals has counted at least two refusals
// or breached reports that the protection gave way, whichever comes first.
//
// The refusal itself is the gate — an attempt that ended in any other way
// never reached the protection, and waiting for it to end proves nothing.
// Two of them because the connection manager reacts to a failed attempt
// (onCMDialFailed) before it retries: the second refusal is made by an
// attempt that started after that reaction, so a reaction that undid the
// protection is caught as well as the attempt itself. The caller asserts on
// the breach after this returns.
func awaitRepeatedRefusal(t *testing.T, what string, refusals func() uint64, breached func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for refusals() < 2 && !breached() {
		if time.Now().After(deadline) {
			t.Fatalf("%s was counted %d times, want at least 2, and nothing gave way either (waited 20s)", what, refusals())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// withoutOwnDials wraps a node setup so the node keeps no outbound slots. A
// node that a test dials learns the dialler's address from the session and
// would dial it back; its own sessions would then add refusals, marks and
// learned identities the test reads as the dialler's.
func withoutOwnDials(setup func(*Service)) func(*Service) {
	return func(svc *Service) {
		svc.connManager.config.MaxSlotsFn = func() int { return 0 }
		if setup != nil {
			setup(svc)
		}
	}
}

// startQuietNode starts a node that dials nobody on its own — no bootstrap
// peers and no outbound slots — so the only attempts it makes are the ones a
// test makes through dialPastTransport, and what they leave behind is theirs.
func startQuietNode(t *testing.T, setup func(*Service)) (*Service, func()) {
	t.Helper()
	return startTestNodeWithSetup(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	}, withoutOwnDials(setup))
}

// dialPastTransport makes one connection-manager attempt from svc to address
// on the calling goroutine — the DialFn the manager's workers call — and
// returns how it ended. Call it only while svc's Run is alive: the attempt
// starts goroutines that Run's lifecycle joins, as it does for the manager's
// own dial workers. Only an attempt that could not open its socket at
// all is repeated: it never reached the peer, so it says nothing about what
// the peer or this node decide. A test that keeps getting those fails here
// instead of reading "nothing happened" as a pass. A session the attempt
// established is closed at once: no slot owns it, and the node's stop would
// otherwise wait on its reader.
//
// The caller's negative checks are made after this returns, so they see
// everything the attempt did, including a v1 fallback inside it.
func dialPastTransport(t *testing.T, svc *Service, address domain.PeerAddress) (bool, error) {
	t.Helper()
	_, established, err := dialSessionPastTransport(t, svc, address)
	return established, err
}

// dialSessionPastTransport is dialPastTransport that also names the
// connection the established session ran on.
func dialSessionPastTransport(t *testing.T, svc *Service, address domain.PeerAddress) (domain.ConnID, bool, error) {
	t.Helper()
	session, err := dialEstablishedPastTransport(t, svc, address)
	if err != nil {
		return 0, false, err
	}
	return session.connID, true, nil
}

// dialEstablishedPastTransport is dialPastTransport that returns the
// established — and already closed — session itself, or why there is none.
func dialEstablishedPastTransport(t *testing.T, svc *Service, address domain.PeerAddress) (*peerSession, error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		result, err := svc.dialForCM(ctx, []domain.PeerAddress{address})
		cancel()
		if err == nil {
			_ = result.Session.Close()
			return result.Session, nil
		}
		if !errors.Is(err, errPeerDialTransport) {
			return nil, err
		}
		if time.Now().After(deadline) {
			t.Fatalf("no attempt on %s got past opening its socket (waited 15s): %v", address, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitV2RefusalOrAuthentication waits until dialer has recorded a failed
// session setup with address — its v2 handshake completed, and the listener
// closed the connection before the session could be used — or until the
// listener authenticated the dialler. A v2 dialler finishes its handshake
// before the listener stores the pin, so a refusal of that pin reaches the
// dialler only as a failed setup, and by then the listener has decided. The
// caller asserts which of the two happened.
func awaitV2RefusalOrAuthentication(t *testing.T, dialer, listener *Service, address domain.PeerAddress) {
	t.Helper()
	dialerID := peerIdentityOf(t, dialer)
	waitForConditionMsg(t, 15*time.Second, "the attempt on "+string(address)+" was neither refused nor established", func() bool {
		return setupFailureRecorded(dialer, address) || nodeAuthenticatedIdentity(listener, dialerID)
	})
}

func setupFailureRecorded(svc *Service, address domain.PeerAddress) bool {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	entry := svc.setupFailures[address]
	return entry != nil && entry.Consecutive > 0
}

// A recovery dial to a peer that speaks v2 goes over v2 too: the one-shot
// paths are no side door to the relayable v1 proof.
//
// syncPeer closes its connection before it returns, so the session kind is
// read from what the dial left behind — the dialler's endpoint binding, its
// legacy_observed marks and the listener's pin — never from a live connection, which is
// gone or not depending on which goroutine ran first.
func TestARecoverySyncToAV2PeerUsesV2(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	// No outbound slots: syncPeer teaches the dialler the listener's address
	// (learnIdentityFromWelcome promotes it), and a connection-manager fill
	// could then open a session of its own — which would hold a connection
	// the teardown wait never sees closed, and would leave the binding and
	// the pin itself, hiding a recovery dial that left none. syncPeer is not
	// affected: it dials through dialPeerWithBudget on the auxiliary share of
	// the connection budget, not through a slot.
	dialer, stopDialer := startTestNodeWithoutDials(t, config.Node{ListenAddress: freeAddress(t), BootstrapPeers: []string{}, Type: domain.NodeTypeFull})
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if imported := dialer.syncPeer(ctx, target, false); imported == 0 {
		t.Fatal("the recovery dial imported nothing: the sync did not complete")
	}
	// The checks run after the listener has torn the connection down — the
	// order in which a live-connection check found nothing. The imported
	// contacts prove the listener registered the connection and answered on
	// it, and it pins the dialler before it registers, so by then the pin is
	// on record.
	awaitInboundTeardown(t, listener)

	if !dialer.secureSessions.store.endpointRequiresV2(target) {
		t.Fatal("the recovery dial left no v2 binding for the endpoint it reached")
	}
	if !listener.secureSessions.store.identityPinned(peerIdentityOf(t, dialer)) {
		t.Fatal("the listener did not pin the dialler as proven over v2")
	}
}

// The budget of one recovery dial covers everything that dial may
// legitimately spend once it speaks v2: opening the socket, the TLS phase and
// the session_proof phase of the v2 handshake, and the two reply drains that
// follow. A budget below that sum cancels a healthy recovery mid-drain on a
// slow path, and the sender key it was fetching is not recovered.
func TestTheRecoveryBudgetCoversAV2DialAndBothDrains(t *testing.T) {
	t.Parallel()
	needed := syncHandshakeTimeout + sessionv2.DefaultTimeouts.TLS + sessionv2.DefaultTimeouts.Proof + 2*syncReplyDrainCap
	if syncRecoveryTimeout < needed {
		t.Fatalf("syncRecoveryTimeout = %v, below the %v a v2 recovery dial may need (dial %v + TLS %v + proof %v + two drains of %v)",
			syncRecoveryTimeout, needed, syncHandshakeTimeout, sessionv2.DefaultTimeouts.TLS, sessionv2.DefaultTimeouts.Proof, syncReplyDrainCap)
	}
	if syncRecoveryTimeout != 30*time.Second {
		t.Fatalf("syncRecoveryTimeout = %v, want the documented 30s", syncRecoveryTimeout)
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
		if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(bound), domain.PeerIdentity{1}, endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}
	})
	defer stopDialer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if imported := dialer.syncPeer(ctx, bound, false); imported != 0 {
		t.Fatalf("the recovery dial imported %d contacts from an endpoint bound to v2 that answered like an old node", imported)
	}
	// No wait is needed for the old node: a v1 fallback would have to get
	// auth_ok before syncPeer can import anything, and the old node records
	// the dialler's identity in completeInboundAuth before it sends auth_ok —
	// so a downgrade leaves its trace before syncPeer returns.
	if nodeAuthenticatedIdentity(old, peerIdentityOf(t, dialer)) {
		t.Fatal("the recovery dial fell back to v1 on an endpoint bound to v2: the old node learned the dialler's identity")
	}
	if !dialer.secureSessions.store.endpointRequiresV2(bound) {
		t.Fatal("the refused downgrade lost the endpoint's v2 binding")
	}
}

// awaitInboundTeardown waits until node holds no inbound connection: a
// one-shot dial has closed its socket and node has finished the handler that
// served it. Call it only once something has proven that node registered the
// connection — a reply received on it — because an empty registry is also
// what a node shows before it has accepted the connection at all.
func awaitInboundTeardown(t *testing.T, node *Service) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		held := inboundConnCount(node)
		if held == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the node never tore down the one-shot connection: %d inbound connections still registered (waited 10s)", held)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// nodeAuthenticatedIdentity reports whether node recorded id for any peer
// address — on the inbound side only completeInboundAuth writes that record,
// so id authenticated with a secure session proof. The record is not removed on disconnect, only by
// the evict* sweeps with their long idle windows, so it outlives the
// connection that carried it.
func nodeAuthenticatedIdentity(node *Service, id domain.PeerIdentity) bool {
	node.peerMu.RLock()
	defer node.peerMu.RUnlock()
	for _, recorded := range node.peerIDs {
		if recorded.identity == id {
			return true
		}
	}
	return false
}

// The pin a v2 session earns must be on disk before the session counts. With
// the store failing to write, the listener establishes nothing; once the
// disk recovers, the reconnect is established AND its pin survives a
// restart (the store reloaded from the same file).
func TestAV2SessionWaitsForItsPinToBeWritten(t *testing.T) {
	t.Parallel()
	var failing atomic.Bool
	var failedWrites atomic.Int64
	failing.Store(true)
	listener, address, stopListener := listenerNode(t, func(svc *Service) {
		store := svc.secureSessions.store
		store.writeFile = func(raw []byte) error {
			if failing.Load() {
				failedWrites.Add(1)
				return errors.New("disk full")
			}
			return store.writeToDisk(raw)
		}
	})
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	awaitV2RefusalOrAuthentication(t, dialer, listener, domain.PeerAddress(normalizeAddress(address)))
	if failedWrites.Load() == 0 {
		t.Fatal("the listener never tried to store the pin of the attempt that failed")
	}
	if nodeAuthenticatedIdentity(listener, peerIdentityOf(t, dialer)) {
		t.Fatal("a v2 session was established while its pin could not be written")
	}
	failing.Store(false)
	waitForConditionMsg(t, 15*time.Second, "no v2 session after the disk recovered", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 > 0
	})
	reloaded := loadSecureSessionStore(secureSessionStorePath(listener.cfg), trustedclock.NewSystem())
	if !reloaded.identityPinned(peerIdentityOf(t, dialer)) {
		t.Fatal("the pin of the established session is not on disk: a restart would forget it")
	}
}

// A protected contact binding still names its original identity after the
// ordinary binding deadline. A different TLS identity cannot complete a dial
// explicitly directed to that contact.
func TestAContactEndpointRefusesASubstituteIdentityAfterThirtyDays(t *testing.T) {
	t.Parallel()
	other, address, stopOther := listenerNode(t, nil)
	defer stopOther()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	expected := domain.PeerIdentity{7}
	clock := newStoreClock()
	store := loadSecureSessionStore("", clock.trusted())
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), expected, endpointProtectionWanted(domain.ProtectedBindingPoolContact)); err != nil {
		t.Fatalf("bind: %v", err)
	}
	clock.now = clock.now.Add(endpointBindingTTL + 24*time.Hour)
	binding, ok := bindingOf(store, endpoint)
	if !ok || binding.tier() != bindingTierProtected || !store.endpointsBoundTo(expected).covers(endpoint) {
		t.Fatal("aged protected contact binding did not survive")
	}
	assertNamedTLSRefusalPreservesBinding(t, store, endpoint, expected, peerIdentityOf(t, other))
}

// Hold a proved connection before registration until shutdown drains the
// registry. Its late registration must be refused so Run can join the handler.
func TestAConnectionRegisteringAfterTheShutdownDrainIsRefused(t *testing.T) {
	t.Parallel()
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce, releaseOnce, stopOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	listener, address, stop := listenerNode(t, func(svc *Service) {
		svc.inboundBeforeRegister = func() {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	})
	defer func() {
		unpark()
		stopOnce.Do(stop)
	}()

	client, err := dialV2As(t, address, newTestService(t, config.NodeTypeClient))
	if err != nil {
		t.Fatalf("secure handshake: %v", err)
	}
	defer func() { _ = client.Close() }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never reached registration")
	}

	stopped := make(chan struct{})
	go func() { stopOnce.Do(stop); close(stopped) }()
	waitForConditionMsg(t, 5*time.Second, "the shutdown drain never ran", func() bool {
		listener.peerMu.RLock()
		defer listener.peerMu.RUnlock()
		return listener.inboundClosed
	})
	unpark()
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
	errDiskFull := errors.New("disk full")
	var failing atomic.Bool
	failing.Store(true)
	dialer, stopDialer := startQuietNode(t, func(svc *Service) {
		store := svc.secureSessions.store
		store.writeFile = func(raw []byte) error {
			if failing.Load() {
				return errDiskFull
			}
			return store.writeToDisk(raw)
		}
	})
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))

	// The gate is the failed write of this attempt's binding: an attempt
	// that ended for any other reason never reached it.
	established, err := dialPastTransport(t, dialer, target)
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("the attempt ended with established=%v err=%v, want it refused on the unwritten binding", established, err)
	}

	failing.Store(false)
	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("no dialled session after the disk recovered: %v", err)
	}
	reloaded := loadSecureSessionStore(secureSessionStorePath(dialer.cfg), trustedclock.NewSystem())
	if !reloaded.endpointRequiresV2(target) {
		t.Fatal("the binding of the established session is not on disk")
	}
}

// clearV2Retry forgets the v2 retry backoff of address alone.
func (m *sessionAddressMarks) clearV2Retry(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || mark.v2Retry == nil {
		return
	}
	mark.v2Retry = nil
	m.storeLocked(address, mark, m.clock())
}
