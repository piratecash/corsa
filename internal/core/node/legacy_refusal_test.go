package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
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

// The backoff of the legacy dial to an address that was refused: 5 minutes
// after the first refusal, doubled after each further one, never more than 6
// hours. It holds the address only until it runs out.
func TestTheLegacyRefusalBackoffDoublesUpToSixHours(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	address := domain.PeerAddress("198.51.100.7:64646")

	want := []time.Duration{
		5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 40 * time.Minute,
		80 * time.Minute, 160 * time.Minute, 320 * time.Minute, 6 * time.Hour, 6 * time.Hour,
	}
	for i, hold := range want {
		if got := marks.noteLegacyRefused(address); got != hold {
			t.Fatalf("refusal %d: hold = %v, want %v", i+1, got, hold)
		}
		clock.now = clock.now.Add(hold - time.Second)
		if !marks.legacyHeld(address) {
			t.Fatalf("refusal %d: the hold ended a second early", i+1)
		}
		clock.now = clock.now.Add(time.Second)
		if marks.legacyHeld(address) {
			t.Fatalf("refusal %d: the hold outlived its %v", i+1, hold)
		}
	}
	if marks.legacyHeld("198.51.100.8:64646") {
		t.Fatal("another address is held by this address's refusals")
	}
}

// A v2 session on the address ends the hold and starts the backoff over.
func TestAV2SessionResetsTheLegacyRefusalBackoff(t *testing.T) {
	t.Parallel()
	marks := newSessionAddressMarks(newStoreClock().read)
	address := domain.PeerAddress("198.51.100.7:64646")
	for range 3 {
		marks.noteLegacyRefused(address)
	}
	marks.clearLegacyRefusal(address)
	if marks.legacyHeld(address) {
		t.Fatal("the hold survived a v2 session on the address")
	}
	if got := marks.noteLegacyRefused(address); got != 5*time.Minute {
		t.Fatalf("the next refusal after a v2 session holds %v, want the first step 5m", got)
	}
}

// v1Answerer is a raw dialer whose peer answers every connection the way an
// old node does: with a v1 line. It records, per socket, the first byte this
// node wrote — 0x16 for a TLS ClientHello, i.e. a v2 attempt — or none, for a
// v1 dial that writes nothing before the caller takes the transport.
type v1Answerer struct {
	mu    sync.Mutex
	first []firstWrite
}

type firstWrite struct {
	wrote bool
	byte  byte
}

func (a *v1Answerer) dialer() rawDialer {
	return func(context.Context) (net.Conn, error) {
		local, remote := net.Pipe()
		a.mu.Lock()
		index := len(a.first)
		a.first = append(a.first, firstWrite{})
		a.mu.Unlock()
		go func() {
			defer func() { _ = remote.Close() }()
			buf := make([]byte, 64<<10)
			n, err := remote.Read(buf)
			if err != nil || n == 0 {
				return
			}
			a.mu.Lock()
			a.first[index] = firstWrite{wrote: true, byte: buf[0]}
			a.mu.Unlock()
			if _, err := remote.Write([]byte("{\"type\":\"error\",\"code\":\"invalid-json\"}\n")); err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, remote)
		}()
		return local, nil
	}
}

func (a *v1Answerer) sockets() []firstWrite {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]firstWrite(nil), a.first...)
}

func dialThroughV1Answerer(t *testing.T, svc *Service, address domain.PeerAddress) ([]firstWrite, error) {
	t.Helper()
	answerer := &v1Answerer{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport, err := svc.dialPeerTransport(ctx, address, domain.DialToAddress(), answerer.dialer())
	if transport != nil {
		_ = transport.wire.Close()
	}
	return answerer.sockets(), err
}

// onlyAV2Attempt reports whether the dial opened exactly one socket and began
// it with a TLS ClientHello.
func onlyAV2Attempt(sockets []firstWrite) bool {
	return len(sockets) == 1 && sockets[0].wrote && sockets[0].byte == sessionv2.TLSRecordHandshake
}

// The "legacy refused" hold withholds the fall back to v1 and nothing else: a
// held address is dialled v2 first — also when it is marked as an old node,
// since the v1 answer behind the mark may have been forged — and only the v1
// dial after a v1 answer is withheld. The hold itself never delays a v2
// attempt; only the separate v2 retry backoff does, so each case here starts
// with that backoff cleared. Without a hold the dialler goes to v1 as before.
func TestTheLegacyRefusalHoldWithholdsOnlyTheV1Fallback(t *testing.T) {
	t.Parallel()

	t.Run("known v1 address, held: exactly one socket, the v2 attempt", func(t *testing.T) {
		t.Parallel()
		svc := newLegacyRefusalService(t)
		address := domain.PeerAddress("198.51.100.7:64646")
		svc.secureSessions.marks.noteLegacyRefused(address)
		svc.secureSessions.marks.clearV2Retry(address)
		svc.secureSessions.marks.noteV1(address)
		sockets, err := dialThroughV1Answerer(t, svc, address)
		if !errors.Is(err, errLegacyRefusedBackoff) {
			t.Fatalf("err = %v, want errLegacyRefusedBackoff", err)
		}
		if !onlyAV2Attempt(sockets) {
			t.Fatalf("sockets = %+v, want exactly one, opened with a ClientHello", sockets)
		}
		if got := svc.SecureSessionStoreStats().V1DialsHeldByRefusalBackoff; got != 1 {
			t.Fatalf("held fall backs counted = %d, want 1", got)
		}
	})

	t.Run("unknown address, held: exactly one socket, the v2 attempt", func(t *testing.T) {
		t.Parallel()
		svc := newLegacyRefusalService(t)
		address := domain.PeerAddress("198.51.100.7:64646")
		svc.secureSessions.marks.noteLegacyRefused(address)
		svc.secureSessions.marks.clearV2Retry(address)
		sockets, err := dialThroughV1Answerer(t, svc, address)
		if !errors.Is(err, errLegacyRefusedBackoff) || !onlyAV2Attempt(sockets) {
			t.Fatalf("err = %v, sockets = %+v; want the held fall back after one v2 attempt", err, sockets)
		}
	})

	t.Run("endpoint bound to v2, held: the v2 attempt runs", func(t *testing.T) {
		t.Parallel()
		svc := newLegacyRefusalService(t)
		address := domain.PeerAddress("198.51.100.7:64646")
		if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(address), domain.PeerIdentity{1}, endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}
		svc.secureSessions.marks.noteLegacyRefused(address)
		svc.secureSessions.marks.clearV2Retry(address)
		sockets, err := dialThroughV1Answerer(t, svc, address)
		if !errors.Is(err, errLegacyRefusedV2Bound) || !onlyAV2Attempt(sockets) {
			t.Fatalf("err = %v, sockets = %+v; want the refused downgrade after one v2 attempt", err, sockets)
		}
	})

	t.Run("known v1 address, not held: v1 as before", func(t *testing.T) {
		t.Parallel()
		svc := newLegacyRefusalService(t)
		address := domain.PeerAddress("198.51.100.7:64646")
		svc.secureSessions.marks.noteV1(address)
		sockets, err := dialThroughV1Answerer(t, svc, address)
		if err != nil {
			t.Fatalf("err = %v, want the v1 transport", err)
		}
		if len(sockets) != 1 || sockets[0].wrote {
			t.Fatalf("sockets = %+v, want the one v1 dial and no ClientHello", sockets)
		}
	})
}

// A refusal forgets the address's v1 mark: the v1 answer behind it may have
// been forged by whoever forged the refused one, and the next dial must try
// v2 first.
func TestALegacyRefusalForgetsTheV1MarkOfItsAddress(t *testing.T) {
	t.Parallel()
	marks := newSessionAddressMarks(newStoreClock().read)
	address := domain.PeerAddress("198.51.100.7:64646")
	marks.noteV1(address)
	marks.noteLegacyRefused(address)
	if marks.v1Seen(address) {
		t.Fatal("the refusal left the v1 mark in place: the next dial would skip v2")
	}
}

// The scenario the hold must survive: a man in the middle answers one
// ClientHello with a v1 line and then forges a v1 welcome naming a pinned
// identity. The refusal costs the address nothing: once its v2 retry backoff
// has run (30 s) it is a candidate again, with its standing untouched, and
// its next dial is a v2 attempt.
func TestAForgedRefusalLeavesTheAddressACandidateForV2(t *testing.T) {
	t.Parallel()
	const endpoint = "1.2.3.4:64646"
	address := domain.PeerAddress(endpoint)
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly, endpoint)
	clock := newStoreClock()
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	pinned := domain.PeerIdentity{0xa1}
	if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
		t.Fatalf("pin: %v", err)
	}
	svc.secureSessions.marks.noteV1(address)
	refusal := svc.refuseLegacyWelcome(address, pinned)
	if !errors.Is(refusal, errLegacyRefusedPinned) {
		t.Fatalf("err = %v, want errLegacyRefusedPinned", refusal)
	}
	svc.onCMDialFailed(address, refusal, false)

	clock.now = clock.now.Add(v2RetryBase)
	if !candidatesContain(svc, address) {
		t.Fatal("the refused address stayed out of the candidates past its v2 retry backoff")
	}
	sockets, err := dialThroughV1Answerer(t, svc, address)
	if !errors.Is(err, errLegacyRefusedBackoff) || !onlyAV2Attempt(sockets) {
		t.Fatalf("the next dial: err = %v, sockets = %+v; want one v2 attempt and the held fall back", err, sockets)
	}
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if health := svc.health[address]; health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0) {
		t.Fatalf("the refusal charged the address: %+v", *health)
	}
}

// Every refusal of the dialling side starts the hold of its address and is
// counted in its own counter: a refused downgrade of a bound endpoint, a v1
// welcome refused on the connection manager's dial, and one refused on a
// one-shot service dial.
func TestEveryOutboundLegacyRefusalHoldsItsAddressAndIsCountedApart(t *testing.T) {
	t.Parallel()

	t.Run("endpoint bound to v2", func(t *testing.T) {
		t.Parallel()
		svc := newLegacyRefusalService(t)
		address := domain.PeerAddress("198.51.100.7:64646")
		if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(address), domain.PeerIdentity{1}, endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}
		if _, err := dialThroughV1Answerer(t, svc, address); !errors.Is(err, errLegacyRefusedV2Bound) {
			t.Fatalf("err = %v, want errLegacyRefusedV2Bound", err)
		}
		if !svc.secureSessions.marks.legacyHeld(address) {
			t.Fatal("the refused downgrade did not hold the address")
		}
		stats := svc.SecureSessionStoreStats()
		if stats.V1RefusalsEndpointBound != 1 || stats.V1RefusalsIdentityPinned != 0 || stats.V1DialsHeldByRefusalBackoff != 0 {
			t.Fatalf("counters = endpoint %d, identity %d, held %d; want 1, 0, 0",
				stats.V1RefusalsEndpointBound, stats.V1RefusalsIdentityPinned, stats.V1DialsHeldByRefusalBackoff)
		}
	})

	t.Run("one-shot service dial welcome", func(t *testing.T) {
		t.Parallel()
		svc := newLegacyRefusalService(t)
		address := domain.PeerAddress("198.51.100.7:64646")
		pinned := domain.PeerIdentity{2}
		if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
			t.Fatalf("pin: %v", err)
		}
		welcome := protocol.Frame{Type: "welcome", Address: pinned.String()}
		if err := svc.admitServiceDialWelcome(serviceDialSyncPeer, address, welcome, false); !errors.Is(err, errLegacyRefusedPinned) {
			t.Fatalf("err = %v, want errLegacyRefusedPinned", err)
		}
		if !svc.secureSessions.marks.legacyHeld(address) {
			t.Fatal("the refused welcome did not hold the address")
		}
		stats := svc.SecureSessionStoreStats()
		if stats.V1RefusalsIdentityPinned != 1 || stats.V1RefusalsEndpointBound != 0 || stats.V1DialsHeldByRefusalBackoff != 0 {
			t.Fatalf("counters = identity %d, endpoint %d, held %d; want 1, 0, 0",
				stats.V1RefusalsIdentityPinned, stats.V1RefusalsEndpointBound, stats.V1DialsHeldByRefusalBackoff)
		}
		other := domain.PeerAddress("198.51.100.8:64646")
		unpinned := protocol.Frame{Type: "welcome", Address: domain.PeerIdentity{3}.String()}
		if err := svc.refuseLegacyWelcome(other, domain.PeerIdentityFromWire(unpinned.Address)); err != nil {
			t.Fatalf("a welcome naming an unpinned identity was refused: %v", err)
		}
		if svc.secureSessions.marks.legacyHeld(other) {
			t.Fatal("an accepted welcome held its address")
		}
	})
}

// A v2 session dialled to a held address ends the hold.
func TestADialledV2SessionEndsTheHoldOfItsAddress(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))
	dialer.secureSessions.marks.noteLegacyRefused(target)
	dialer.secureSessions.marks.noteV2Unanswered(target)
	// The v2 retry backoff has run out; the legacy hold and both counts
	// remain.
	dialer.secureSessions.marks.mu.Lock()
	mark := dialer.secureSessions.marks.marks[target]
	mark.v2Retry.until = time.Time{}
	dialer.secureSessions.marks.marks[target] = mark
	dialer.secureSessions.marks.mu.Unlock()

	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("the v2 dial was not established: %v", err)
	}
	if dialer.secureSessions.marks.legacyHeld(target) {
		t.Fatal("a v2 session on the address left its legacy hold in place")
	}
	dialer.secureSessions.marks.mu.Lock()
	_, remembered := dialer.secureSessions.marks.marks[target]
	dialer.secureSessions.marks.mu.Unlock()
	if remembered {
		t.Fatal("a v2 session on the address left its refusal counts in place")
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
func TestAV1SessionRefusedAtRegistrationIsWithdrawnUnpublished(t *testing.T) {
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
	if !svc.secureSessions.marks.legacyHeld(address) {
		t.Fatal("the refusal did not hold the fall back to v1 at the address")
	}
	if got := svc.SecureSessionStoreStats().V1RefusalsIdentityPinned; got != 1 {
		t.Fatalf("identity refusals counted = %d, want 1", got)
	}
}

// The sweep that runs when an identity proves v2 closes its registered v1
// sessions as this node's decision: the address that carried one is not
// charged when its serve loop ends, and the fall back to v1 there is held.
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
	if !svc.secureSessions.marks.legacyHeld(address) {
		t.Fatal("the sweep did not hold the fall back to v1 at the address")
	}

	svc.retirePeerSession(session, io.ErrClosedPipe)
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if health := svc.health[address]; health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0 || health.LastError != "") {
		t.Fatalf("the swept session was charged to its address: %+v", *health)
	}
}

// The legacy session path (openPeerSession, idle while the connection
// manager runs) speaks v1 only: a welcome naming an identity that requires v2
// is refused before anything is signed, and the address is held, not charged.
func TestTheLegacySessionPathRefusesAPinnedWelcome(t *testing.T) {
	t.Parallel()
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly)
	pinned := domain.PeerIdentity{0xa1}
	if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
		t.Fatalf("pin: %v", err)
	}
	welcome, err := protocol.MarshalFrameLine(protocol.Frame{
		Type:                   "welcome",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Address:                pinned.String(),
		Challenge:              "corsa-pinned-welcome",
	})
	if err != nil {
		t.Fatalf("marshal welcome: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	afterWelcome := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			afterWelcome <- "accept: " + err.Error()
			return
		}
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadString('\n'); err != nil {
			afterWelcome <- "hello: " + err.Error()
			return
		}
		if _, err := conn.Write([]byte(welcome)); err != nil {
			afterWelcome <- "welcome: " + err.Error()
			return
		}
		line, _ := reader.ReadString('\n')
		afterWelcome <- line
	}()

	address := domain.PeerAddress(ln.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := svc.openPeerSession(ctx, address); !errors.Is(err, errLegacyRefusedPinned) {
		t.Fatalf("err = %v, want errLegacyRefusedPinned", err)
	}
	if line := <-afterWelcome; strings.Contains(line, "auth_session") {
		t.Fatalf("the legacy path signed for a pinned identity: %q", line)
	}
	if !svc.secureSessions.marks.legacyHeld(address) {
		t.Fatal("the refusal did not hold the address")
	}
}

// v2-only mode (not wired yet) refuses a v1 answer because the network has
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
	if svc.secureSessions.marks.legacyHeld(address) {
		t.Fatal("a v2-only refusal started the legacy hold")
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
// without a refusal count, and only then the least recently written of all —
// so a refusal count, which keeps the backoff doubling, outlives fresher v1
// marks.
func TestAFullMarkMapEvictsTheLeastRecentlyWrittenMarkWithoutAHold(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	held := domain.PeerAddress("198.51.100.1:64646")
	marks.noteLegacyRefused(held)
	marks.noteLegacyRefused(held)
	clock.now = clock.now.Add(time.Second)
	for i := range maxSessionAddressMarks - 1 {
		marks.noteV1(boundEndpoint(i))
		clock.now = clock.now.Add(time.Millisecond)
	}
	marks.noteV1("203.0.113.1:64646")

	if marks.v1Seen(boundEndpoint(0)) {
		t.Fatal("the least recently written v1 mark was kept while the map was full")
	}
	if !marks.v1Seen(boundEndpoint(1)) || !marks.v1Seen("203.0.113.1:64646") {
		t.Fatal("a fresher mark was dropped instead of the oldest")
	}
	if got := marks.noteLegacyRefused(held); got != 20*time.Minute {
		t.Fatalf("the held address's next hold = %v, want 20m: its refusal count was evicted", got)
	}
}

// A v1 answer during a hold must not mark the address as an old node again:
// the refusal forgot the mark because the v1 answers there may be forged, and
// a mark set now would send the next dial past v2 once the hold is over.
func TestAV1AnswerDuringAHoldDoesNotReviveTheV1Mark(t *testing.T) {
	t.Parallel()
	svc := newLegacyRefusalService(t)
	address := domain.PeerAddress("198.51.100.7:64646")
	svc.secureSessions.marks.noteLegacyRefused(address)
	svc.secureSessions.marks.clearV2Retry(address)
	if _, err := dialThroughV1Answerer(t, svc, address); !errors.Is(err, errLegacyRefusedBackoff) {
		t.Fatalf("err = %v, want errLegacyRefusedBackoff", err)
	}
	if svc.secureSessions.marks.v1Seen(address) {
		t.Fatal("a v1 answer during the hold marked the address as an old node again")
	}
}

// A v1 dial that never got a welcome — the far end hung up on the hello, as
// a v2 node that pinned this node does — forgets the v1 mark, so the next
// dial tries v2 first instead of repeating a v1 dial that cannot succeed.
func TestAV1DialWithoutAWelcomeForgetsTheV1Mark(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = bufio.NewReader(conn).ReadString('\n')
			}()
		}
	}()
	dialer, stop := startQuietNode(t, nil)
	defer stop()
	address := domain.PeerAddress(ln.Addr().String())
	dialer.secureSessions.marks.noteV1(address)

	if established, _ := dialPastTransport(t, dialer, address); established {
		t.Fatal("a dial that got no welcome was established")
	}
	if dialer.secureSessions.marks.v1Seen(address) {
		t.Fatal("the v1 mark survived a v1 dial that never got a welcome")
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

// The v2 retry backoff: an address whose v2 attempts keep ending without v2 —
// a refused v1 welcome, a refused downgrade, a held fall back — is retried
// after 30 s, doubled per further such end, at most 30 min, and is not a
// candidate meanwhile. It is not a penalty: score and failures stay as they
// were. Driven for 30 minutes of virtual time by a dialler that tries every
// second, the address sees a handful of sockets, not one every few seconds.
func TestRepeatedRefusalsBoundTheDialsToAnAddress(t *testing.T) {
	t.Parallel()
	const endpoint = "1.2.3.4:64646"
	address := domain.PeerAddress(endpoint)
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly, endpoint)
	clock := newStoreClock()
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	pinned := domain.PeerIdentity{0xa1}
	if err := svc.secureSessions.store.noteProvenInbound(pinned); err != nil {
		t.Fatalf("pin: %v", err)
	}
	svc.onCMDialFailed(address, svc.refuseLegacyWelcome(address, pinned), false)

	sockets, excluded := 0, 0
	for range 30 * 60 {
		clock.now = clock.now.Add(time.Second)
		if !candidatesContain(svc, address) {
			excluded++
		}
		opened, err := dialThroughV1Answerer(t, svc, address)
		sockets += len(opened)
		if err == nil {
			// The v1 fall back went through; the welcome on it names the
			// pinned identity and is refused, as on a real dial.
			err = svc.refuseLegacyWelcome(address, pinned)
		}
		svc.onCMDialFailed(address, err, false)
	}
	if sockets > 8 || sockets < 3 {
		t.Fatalf("%d sockets in 30 minutes, want a handful (3–8): v2 is retried, but on the backoff", sockets)
	}
	if excluded == 0 {
		t.Fatal("the address was never left out of the candidates during its v2 retry backoff")
	}
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if health := svc.health[address]; health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0) {
		t.Fatalf("the backoff charged the address: failures %d, score %d", health.ConsecutiveFailures, health.Score)
	}
}

// A v2 session to the address ends its v2 retry backoff with its legacy hold.
func TestAV2SessionEndsTheV2RetryBackoff(t *testing.T) {
	t.Parallel()
	marks := newSessionAddressMarks(newStoreClock().read)
	address := domain.PeerAddress("198.51.100.7:64646")
	marks.noteLegacyRefused(address)
	if !marks.v2RetryHeld(address) {
		t.Fatal("precondition: a refusal did not start the v2 retry backoff")
	}
	marks.clearLegacyRefusal(address)
	if marks.v2RetryHeld(address) {
		t.Fatal("a v2 session left the v2 retry backoff in place")
	}
}
