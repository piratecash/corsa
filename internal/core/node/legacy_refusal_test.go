package node

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_refusal_test.go pins how a node treats a v1 session it refused on
// the dialling side because the peer requires v2 — the identity the welcome
// names is pinned (errLegacyRefusedPinned), or the endpoint is bound to v2
// (errLegacyRefusedV2Bound). Such a refusal is this node's decision, and a man
// in the middle can provoke it at will by answering with a forged v1 welcome:
// it is not a failure of the address, so it must not lower the address's
// rating, put it into the candidate cooldown, or count as a setup failure.
// Otherwise the man in the middle would push the address out of rotation and
// keep v2 away from it. A dial "to X" that met another identity on the
// address (errEndpointOwnerMismatch) is not charged either: the address may
// have changed hands, and the dial only learnt it is not X's now.

func newLegacyRefusalService(t *testing.T) *Service {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)
	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)
	return svc
}

// unchargedDialRefusals are the refusals of the dialling side: the two that
// meet a peer requiring v2, the one the refusal backoff makes in their stead
// while it holds the address, and a dial "to X" that met another identity.
func unchargedDialRefusals() map[string]error {
	return map[string]error{
		"identity pinned to v2":        fmt.Errorf("%w: test", errLegacyRefusedPinned),
		"endpoint bound to v2":         fmt.Errorf("%w: test", errLegacyRefusedV2Bound),
		"held by refusal backoff":      fmt.Errorf("%w: test", errLegacyRefusedBackoff),
		"another owner than the named": fmt.Errorf("%w: test", errEndpointOwnerMismatch),
	}
}

// A hundred refusals through the connection manager's failure hook leave the
// address exactly as it was: no failure counted, no score lost, no cooldown,
// no setup failure, no route quarantine.
func TestAnOutboundLegacyRefusalIsNotAFailureOfTheAddress(t *testing.T) {
	t.Parallel()
	for name, refusal := range unchargedDialRefusals() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newLegacyRefusalService(t)
			address := domain.PeerAddress("198.51.100.7:64646")

			for range 100 {
				svc.onCMDialFailed(address, refusal, false)
			}

			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			if health := svc.health[address]; health != nil {
				if health.ConsecutiveFailures != 0 {
					t.Fatalf("ConsecutiveFailures = %d after 100 refusals, want 0", health.ConsecutiveFailures)
				}
				if health.Score != 0 {
					t.Fatalf("Score = %d after 100 refusals, want 0", health.Score)
				}
				if !health.BannedUntil.IsZero() {
					t.Fatalf("the refusals banned the address until %v", health.BannedUntil)
				}
				if !health.LastDisconnectedAt.IsZero() {
					t.Fatal("the refusals published the address as disconnected")
				}
			}
			if entry := svc.setupFailures[address]; entry != nil {
				t.Fatalf("the refusals were counted as setup failures: %+v", *entry)
			}
			if len(svc.peerQuarantine) != 0 {
				t.Fatalf("the refusals armed a route quarantine: %v", svc.peerQuarantine)
			}
		})
	}
}

// A v2 session dialled to a held address ends the hold.
func TestADialledV2SessionEndsTheHoldOfItsAddress(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))
	dialer.secureSessions.marks.noteV2Unanswered(target)
	// The TLS retry wait has elapsed while the retry count remains.
	dialer.secureSessions.marks.mu.Lock()
	mark := dialer.secureSessions.marks.marks[target]
	mark.v2Retry.until = time.Time{}
	dialer.secureSessions.marks.marks[target] = mark
	dialer.secureSessions.marks.mu.Unlock()

	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("the v2 dial was not established: %v", err)
	}
	dialer.secureSessions.marks.mu.Lock()
	counted := dialer.secureSessions.marks.marks[target].counted()
	dialer.secureSessions.marks.mu.Unlock()
	if counted {
		t.Fatal("a v2 session on the address left its retry count in place")
	}
}

// unprovenOutboundSession is a v1 session this node dialled to address, its
// welcome naming id, attached to its NetCore and not yet registered.
func unprovenOutboundSession(t *testing.T, svc *Service, address domain.PeerAddress, id domain.PeerIdentity) (*peerSession, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	svc.peerMu.Lock()
	connID := svc.nextConnIDLocked()
	svc.peerMu.Unlock()
	session := &peerSession{
		address:      address,
		peerIdentity: id,
		connID:       connID,
		authOK:       true,
		conn:         local,
		sendCh:       make(chan peerSendItem, peerSessionSendBuffer),
		inboxCh:      make(chan protocol.Frame, peerSessionInboxBuffer),
		errCh:        make(chan error, 1),
	}
	if err := svc.attachOutboundNetCore(session); err != nil {
		t.Fatalf("attachOutboundNetCore: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, remote
}

// A v1 session whose identity proved v2 between the dial and its
// registration is withdrawn before anything is published for it: the address
// is never marked connected, no serve loop runs, nothing is charged, and the
// fall back to v1 there is held.
func TestUnprovedSessionRefusedAtRegistrationIsWithdrawnUnpublished(t *testing.T) {
	t.Parallel()
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly)
	address := domain.PeerAddress("198.51.100.7:64646")
	pinned := domain.PeerIdentity{0xa1}
	if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
		t.Fatalf("pin: %v", err)
	}
	session, _ := unprovenOutboundSession(t, svc, address, pinned)

	done := make(chan struct{})
	go func() {
		svc.publishAndServeCMSession(session, address, 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the refused session was served instead of withdrawn")
	}

	if !session.closedByLocalDecision() {
		t.Fatal("the refused session was not closed as this node's decision")
	}
	svc.peerMu.RLock()
	_, registered := svc.sessions[address]
	health := svc.health[address]
	svc.peerMu.RUnlock()
	if registered {
		t.Fatal("the refused session stayed registered")
	}
	if health != nil && (!health.LastConnectedAt.IsZero() || health.ConsecutiveFailures != 0 || health.Score != 0) {
		t.Fatalf("the refused session was published or charged: %+v", *health)
	}
}

// The sweep that runs when an identity proves v2 closes its registered v1
// sessions as this node's decision: the address that carried one is not
// charged when its serve loop ends, and the next TLS attempt is paced.
func TestTheV2ProofSweepClosesALegacySessionWithoutChargingItsAddress(t *testing.T) {
	t.Parallel()
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly)
	address := domain.PeerAddress("198.51.100.7:64646")
	peer, _ := sessionv2test.NewProvenPeer(t)
	session, _ := unprovenOutboundSession(t, svc, address, peer.Identity)
	svc.peerMu.Lock()
	svc.deliveryMu.Lock()
	svc.sessions[address] = session
	svc.deliveryMu.Unlock()
	svc.peerMu.Unlock()

	if err := svc.onIdentityProvenV2(context.Background(), sessionv2test.Proof(t, peer)); err != nil {
		t.Fatalf("proof: %v", err)
	}
	if !session.closedByLocalDecision() {
		t.Fatal("the sweep closed the legacy session as the peer's failure")
	}
	if !svc.secureSessions.marks.v2RetryHeld(address) {
		t.Fatal("the sweep did not pace the next TLS dial")
	}

	svc.retirePeerSession(session, io.ErrClosedPipe)
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if health := svc.health[address]; health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0 || health.LastError != "") {
		t.Fatalf("the swept session was charged to its address: %+v", *health)
	}
}

// TLS-only admission refuses a v1 answer because the network has
// left v1, not because a peer requires v2: an old node there is genuinely
// incompatible, so the failure is charged as an ordinary one and holds
// nothing.
func TestAV1AnswerInV2OnlyModeIsAnOrdinaryFailure(t *testing.T) {
	t.Parallel()
	svc := newLegacyRefusalService(t)
	address := domain.PeerAddress("198.51.100.7:64646")
	svc.onCMDialFailed(address, fmt.Errorf("%w: test", errV1RefusedV2Only), false)
	svc.peerMu.RLock()
	health := svc.health[address]
	svc.peerMu.RUnlock()
	if health == nil || health.ConsecutiveFailures != 1 {
		t.Fatalf("health = %+v, want one charged failure", health)
	}
	if svc.secureSessions.marks.v2RetryHeld(address) {
		t.Fatal("a plaintext refusal started a TLS retry backoff")
	}
}

// An address dial creates only an ordinary binding, however the proven
// identity is pinned: the dialler reaching one of its own contacts over v2
// binds the endpoint for 30 days, not for good.
func TestAnAddressDialToAContactCreatesOnlyAnOrdinaryBinding(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	contact := peerIdentityOf(t, listener)
	dialer, stopDialer := startQuietNode(t, func(svc *Service) {
		svc.trust.mu.Lock()
		svc.trust.contacts[contact.String()] = trustedContact{Address: contact.String()}
		svc.trust.mu.Unlock()
	})
	defer stopDialer()
	if !dialer.trust.isTrustedContact(contact) {
		t.Fatal("precondition: the listener is not a contact of the dialler")
	}
	target := domain.PeerAddress(normalizeAddress(address))
	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("the v2 dial was not established: %v", err)
	}
	binding, bound := bindingOf(dialer.secureSessions.store, target)
	if !bound || binding.identity != contact {
		t.Fatalf("binding = %+v (bound %v), want one owned by the contact", binding, bound)
	}
	if binding.protection != nil {
		t.Fatalf("an address dial created a protected binding: %+v", *binding.protection)
	}
}

// A full map of address marks makes room for a new one by dropping, in this
// order: a mark that says nothing any more, the least recently written mark
// without a retry count, and only then the least recently written of all.
// A retry count outlives fresher pin-room waits.
func TestAFullMarkMapEvictsTheLeastRecentlyWrittenMarkWithoutAHold(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	held := domain.PeerAddress("198.51.100.1:64646")
	marks.noteV2Unanswered(held)
	clock.now = clock.now.Add(time.Second)
	for i := range maxSessionAddressMarks - 1 {
		marks.notePinStoreFull(boundEndpoint(i))
		clock.now = clock.now.Add(time.Millisecond)
	}
	marks.notePinStoreFull("203.0.113.1:64646")

	if marks.awaitsPinRoom(boundEndpoint(0)) {
		t.Fatal("the least recently written pin-room mark was kept while the map was full")
	}
	if !marks.awaitsPinRoom(boundEndpoint(1)) || !marks.awaitsPinRoom("203.0.113.1:64646") {
		t.Fatal("a fresher mark was dropped instead of the oldest")
	}
	clock.now = clock.now.Add(v2RetryBase)
	if got := marks.noteV2Unanswered(held); got != 2*v2RetryBase {
		t.Fatalf("the next TLS retry wait = %v, want %v", got, 2*v2RetryBase)
	}
}

// A refusal at registration does not clear the address's setup failures: the
// session was not published, so it proved nothing about the peer.
func TestARefusalAtRegistrationKeepsTheSetupFailures(t *testing.T) {
	t.Parallel()
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly)
	address := domain.PeerAddress("198.51.100.7:64646")
	pinned := domain.PeerIdentity{0xa1}
	if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
		t.Fatalf("pin: %v", err)
	}
	svc.peerMu.Lock()
	svc.recordSetupFailureLocked(address, time.Now())
	svc.peerMu.Unlock()
	session, _ := unprovenOutboundSession(t, svc, address, pinned)
	svc.publishAndServeCMSession(session, address, 1)
	if !setupFailureRecorded(svc, address) {
		t.Fatal("a session refused at registration cleared the address's setup failures")
	}
}

// A v2 session to the address ends its TLS retry backoff.
func TestAV2SessionEndsTheV2RetryBackoff(t *testing.T) {
	t.Parallel()
	marks := newSessionAddressMarks(newStoreClock().read)
	address := domain.PeerAddress("198.51.100.7:64646")
	marks.noteV2Unanswered(address)
	if !marks.v2RetryHeld(address) {
		t.Fatal("precondition: a refusal did not start the v2 retry backoff")
	}
	marks.noteOutboundV2(address)
	if marks.v2RetryHeld(address) {
		t.Fatal("a v2 session left the v2 retry backoff in place")
	}
}
