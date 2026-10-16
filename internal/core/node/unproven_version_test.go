package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// unproven_version_test.go pins that a version a peer only CLAIMS — the
// answer to the probe, a v1 welcome before auth_ok, a v1 hello — costs the
// address it came from nothing but pacing: a man in the middle can write any
// version into such a frame, and the honest node behind it must not pay.
// Score, failures, the ban of the address and of its IP, the version lockout
// of its identity and the update_available reporters move only on a version
// proven inside a v2 session.

// forgedLowVersionAnswer is what a man in the middle answers the probe with:
// a welcome of this network with a version below the floor and a minimum far
// above this build's — no identity at all.
func forgedLowVersionAnswer() protocol.Frame {
	return protocol.Frame{
		Type:                   "welcome",
		Version:                20,
		MinimumProtocolVersion: 99,
		Network:                networkName,
	}
}

// victimIdentity is an identity the node under test already knows at two
// addresses: the one the forged answer comes from, and another.
var victimIdentity = domain.PeerIdentity{0xbe, 0xef, 0x01}

// knowVictimAt records victimIdentity at both addresses with persisted
// metadata, so a lockout by identity would show on either.
func knowVictimAt(svc *Service, addresses ...domain.PeerAddress) {
	svc.peerMu.Lock()
	defer svc.peerMu.Unlock()
	for _, address := range addresses {
		svc.peerIDs[address] = claimedAddressIdentity(victimIdentity)
		svc.persistedMeta[address] = &peerEntry{Address: address}
	}
}

// assertNoVersionHarm checks that nothing but pacing touched addresses: no
// score, failure, ban or IP ban, no lockout on any of them, no reporter.
func assertNoVersionHarm(t *testing.T, svc *Service, addresses ...domain.PeerAddress) {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	for _, address := range addresses {
		if health := svc.health[svc.resolveHealthAddress(address)]; health != nil {
			if health.Score != 0 || health.ConsecutiveFailures != 0 || health.IncompatibleVersionAttempts != 0 || !health.BannedUntil.IsZero() {
				t.Fatalf("%s was charged for a claimed version: %+v", address, *health)
			}
		}
		if entry := svc.persistedMeta[address]; entry != nil && entry.VersionLockout.IsActive() {
			t.Fatalf("a claimed version locked out %s", address)
		}
	}
	if len(svc.bannedIPSet) != 0 {
		t.Fatalf("a claimed version banned an IP: %v", svc.bannedIPSet)
	}
	if svc.versionPolicy != nil && len(svc.versionPolicy.incompatibleReporters) != 0 {
		t.Fatalf("a claimed version fed the update_available reporters: %v", svc.versionPolicy.incompatibleReporters)
	}
}

// assertOnlyPaced is assertNoVersionHarm plus the pacing the claim earns.
func assertOnlyPaced(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	assertNoVersionHarm(t, svc, address)
	if !svc.secureSessions.marks.v2RetryHeld(address) {
		t.Fatal("a claimed incompatible version left the address unpaced")
	}
}

// The attack: one forged line answers every probe. Ten dials through the
// connection manager's reaction leave the address, its IP and the identity
// known there untouched — only paced.
func TestAForgedLowVersionInTheProbeAnswerOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	other := domain.PeerAddress("198.51.100.99:64646")
	knowVictimAt(svc, dialChoiceAddress, other)
	forged := forgedLowVersionAnswer()
	for range 10 {
		svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
		peer := newScriptedPeer(answerClientHello(v1ErrorLine), oldNodeSocket(forged, forged))
		_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
		if !errors.Is(err, errIncompatibleProtocol) {
			t.Fatalf("err = %v, want the incompatible version", err)
		}
		svc.onCMDialFailed(dialChoiceAddress, err, true)
	}
	assertOnlyPaced(t, svc, dialChoiceAddress)
	assertNoVersionHarm(t, svc, other)
}

// The same forged answer on a one-shot dial.
func TestAForgedLowVersionOnAOneShotDialOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.secureSessions.dialTimeouts = testDialTimeouts()
	forged := forgedLowVersionAnswer()
	address := startLineRecordingOldNode(t, forged, forged, &recordedLines{})
	knowVictimAt(svc, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	svc.syncPeer(ctx, address, false)
	assertOnlyPaced(t, svc, address)
}

// A v1 welcome read before auth_ok is as unproven as the probe's answer: the
// connection manager's reaction to its version only paces. The same version
// inside a v2 session is proven, and keeps the lockout and the reporters.
func TestOnlyAProvenVersionLocksOutOrReports(t *testing.T) {
	t.Parallel()
	welcome := protocol.Frame{Type: "welcome", Version: 20, MinimumProtocolVersion: 99}

	t.Run("claimed", func(t *testing.T) {
		t.Parallel()
		svc := newDialChoiceService(t)
		knowVictimAt(svc, dialChoiceAddress)
		svc.onCMDialFailed(dialChoiceAddress, incompatibleVersionOf(welcome, versionClaimed, errors.New("test")), true)
		assertOnlyPaced(t, svc, dialChoiceAddress)
	})
	t.Run("proven", func(t *testing.T) {
		t.Parallel()
		svc := newDialChoiceService(t)
		knowVictimAt(svc, dialChoiceAddress)
		svc.onCMDialFailed(dialChoiceAddress, incompatibleVersionOf(welcome, versionProven, errors.New("test")), true)
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		if !svc.persistedMeta[dialChoiceAddress].VersionLockout.IsActive() {
			t.Fatal("a proven incompatible version set no lockout")
		}
		if _, reported := svc.versionPolicy.incompatibleReporters[victimIdentity]; !reported {
			t.Fatal("a proven incompatible version did not report")
		}
	})
}

// A claimed version never reaches the text of the error: a client version is
// whatever the remote side wrote.
func TestAnIncompatibleVersionErrorCarriesNoClientVersion(t *testing.T) {
	t.Parallel()
	welcome := protocol.Frame{Type: "welcome", Version: 20, ClientVersion: "evil\nline"}
	if err := incompatibleVersionOf(welcome, versionClaimed, errors.New("too old")); strings.Contains(err.Error(), "evil") {
		t.Fatalf("the error text carries the remote client version: %q", err.Error())
	}
}

// An inbound v1 hello is a claim too: a hello rewritten below the floor with
// a minimum far above this build's is refused, and refused again, without
// banning the address it names, its dial IP or the identity known there. What
// stays is the transport-level ban score of the TCP source the hellos came
// from (addBanScore): from an external source the fourth refused hello
// blacklists that source for inbound connections. It keys the socket's real
// peer, not the address or identity the hello claims.
func TestAForgedInboundHelloCostsOnlyTheTransportBanOfItsSource(t *testing.T) {
	t.Parallel()
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
	})
	defer stop()
	address := domain.PeerAddress(net.JoinHostPort(testExternalIP, "64651"))
	knowVictimAt(svc, address)

	for range 4 {
		conn := dialFromSource(t, svc, testExternalIP)
		if reply := sendForgedOldHello(t, conn); reply.Code != protocol.ErrCodeIncompatibleProtocol {
			t.Fatalf("reply = %#v, want the incompatible-protocol refusal", reply)
		}
		_ = conn.Close()
	}
	waitForRefusedHelloRecorded(t, svc, address)
	// The refusal is written before the handler scores the source, so the
	// reply to the fourth hello can reach this test while its addBanScore has
	// not run yet: the ban is waited for, not read once.
	waitForConditionMsg(t, 2*time.Second, "four refused hellos left their TCP source unbanned", func() bool {
		svc.ipStateMu.RLock()
		defer svc.ipStateMu.RUnlock()
		return !svc.bans[testExternalIP].Blacklisted.IsZero()
	})
	assertNoVersionHarm(t, svc, address)
}

// The same forged hellos from loopback — the onion entry — cost each only its
// own connection: the refusal still reaches the dialler, the listener then
// ends the connection, and loopback is never scored by its IP.
func TestAForgedInboundHelloFromLoopbackCostsOnlyItsConnection(t *testing.T) {
	t.Parallel()
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
	})
	defer stop()
	address := domain.PeerAddress(net.JoinHostPort("127.0.0.1", "64651"))
	knowVictimAt(svc, address)

	for range 4 {
		conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if reply := sendForgedOldHello(t, conn); reply.Code != protocol.ErrCodeIncompatibleProtocol {
			t.Fatalf("reply = %#v, want the incompatible-protocol refusal", reply)
		}
		if !endsWithin(conn, conn, 2*time.Second) {
			t.Fatal("the listener kept a loopback connection that sent a refused hello")
		}
		_ = conn.Close()
	}
	waitForRefusedHelloRecorded(t, svc, address)
	svc.ipStateMu.RLock()
	entry, scored := svc.bans["127.0.0.1"]
	svc.ipStateMu.RUnlock()
	if scored {
		t.Fatalf("loopback was scored by its IP: %+v", entry)
	}
	if closed := svc.LoopbackInboundStats().Refusals.ViolationsClosed; closed != 4 {
		t.Fatalf("violations closed = %d, want one per refused hello", closed)
	}
	assertNoVersionHarm(t, svc, address)
}

// sendForgedOldHello sends a hello rewritten below the floor with a minimum
// far above this build's, advertising port 64651, and returns the reply.
func sendForgedOldHello(t *testing.T, conn net.Conn) protocol.Frame {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	writeJSONFrame(t, conn, protocol.Frame{
		Type:                   "hello",
		Version:                20,
		MinimumProtocolVersion: 99,
		Client:                 "test",
		AdvertisePort:          domain.PeerPort(64651),
	})
	return readJSONTestFrame(t, bufio.NewReader(conn))
}

// waitForRefusedHelloRecorded waits until the refused hellos reached the
// health record of address — the TCP source with the advertised port.
func waitForRefusedHelloRecorded(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	waitForConditionMsg(t, 2*time.Second, "the refused hellos were never recorded", func() bool {
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		health := svc.health[svc.resolveHealthAddress(address)]
		return health != nil && health.LastErrorCode == protocol.ErrCodeIncompatibleProtocol
	})
}

// An honest release below the floor, emulated end to end, is still refused —
// and the refusals harm neither its address's IP nor its identity.
func TestAnHonestReleaseBelowTheFloorIsRefusedWithoutHarm(t *testing.T) {
	t.Parallel()
	oldAddress := freeAddress(t)
	proxy := startLegacyVersionProxy(t, oldAddress, oldPeerListens, config.MinimumProtocolVersion-1)
	old, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  oldAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, withoutOwnDials(asOldNode))
	defer stopOld()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	target := proxy.peerAddress()
	dialer.peerMu.Lock()
	dialer.peerIDs[target] = claimedAddressIdentity(peerIdentityOf(t, old))
	dialer.persistedMeta[target] = &peerEntry{Address: target}
	dialer.peerMu.Unlock()

	for range 5 {
		dialer.secureSessions.marks.clearOldVersionWait(target)
		established, err := dialPastTransport(t, dialer, target)
		if established || !errors.Is(err, errIncompatibleProtocol) {
			t.Fatalf("established=%v err=%v, want the incompatible version", established, err)
		}
		dialer.onCMDialFailed(target, err, true)
	}
	assertNoVersionHarm(t, dialer, target)
	if !dialer.secureSessions.marks.oldVersionHeld(target) {
		t.Fatal("a correct answer below the floor did not delay its endpoint")
	}
	if _, err := dialPastTransport(t, dialer, target); !errors.Is(err, errOldVersionAwaited) || !isUnchargedDialRefusal(err) {
		t.Fatalf("a dial inside the delay: %v, want the uncharged errOldVersionAwaited", err)
	}
}
