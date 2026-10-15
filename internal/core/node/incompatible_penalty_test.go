package node

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// incompatibleBanState is everything penalizeOldProtocolPeer accumulates for
// one address, plus the IP-level ban it propagates.
type incompatibleBanState struct {
	attempts            domain.AttemptCount
	score               int
	consecutiveFailures int
	bannedUntil         time.Time
	ipBannedUntil       time.Time
}

func incompatibleBanStateOf(t *testing.T, svc *Service, address domain.PeerAddress) incompatibleBanState {
	t.Helper()
	svc.peerMu.RLock()
	health := svc.health[address]
	svc.peerMu.RUnlock()
	if health == nil {
		t.Fatalf("no health record for %s", address)
	}
	ip, _, ok := splitHostPort(string(address))
	if !ok {
		t.Fatalf("address %s has no host", address)
	}
	svc.ipStateMu.RLock()
	ipBan := svc.bannedIPSet[ip]
	svc.ipStateMu.RUnlock()
	return incompatibleBanState{
		attempts:            health.IncompatibleVersionAttempts,
		score:               health.Score,
		consecutiveFailures: health.ConsecutiveFailures,
		bannedUntil:         health.BannedUntil,
		ipBannedUntil:       ipBan.BannedUntil,
	}
}

const attemptsToIncompatibleBan = peerBanThresholdIncompatible / peerBanIncrementIncompatible

func penalizeUntilBanned(t *testing.T, svc *Service, address domain.PeerAddress) incompatibleBanState {
	t.Helper()
	below := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	for i := 0; i < attemptsToIncompatibleBan; i++ {
		svc.penalizeOldProtocolPeer(address, below, below, "0.0.1-below-floor")
	}
	banned := incompatibleBanStateOf(t, svc, address)
	if !banned.bannedUntil.After(time.Now()) || banned.ipBannedUntil.IsZero() {
		t.Fatalf("precondition: %d observations did not ban the peer and its IP: %+v", attemptsToIncompatibleBan, banned)
	}
	return banned
}

// A ban window is fixed once armed. Callers of penalizeOldProtocolPeer can
// observe the same peer again inside it — an inbound hello the IP blacklist
// has not caught yet, a sender-key recovery dial — and none of them carries
// new evidence. Accumulating each one would slide the ban
// (and the IP-wide ban) forward for as long as the old peer stays reachable.
func TestPenalizeOldProtocolPeer_InsideItsBan_NeitherAccumulatesNorSlides(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	address := domain.PeerAddress("203.0.113.9:64646")
	banned := penalizeUntilBanned(t, svc, address)

	below := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	svc.penalizeOldProtocolPeer(address, below, below, "0.0.1-below-floor")

	if got := incompatibleBanStateOf(t, svc, address); got != banned {
		t.Fatalf("an observation inside the ban changed it:\n before %+v\n after  %+v", banned, got)
	}
}

// Once the window is over the next observation counts again and re-arms a
// full ban: the peer is still incompatible.
func TestPenalizeOldProtocolPeer_AfterItsBan_RearmsTheBan(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	address := domain.PeerAddress("203.0.113.10:64646")
	banned := penalizeUntilBanned(t, svc, address)

	expireIncompatibleBan(svc, address)
	below := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	svc.penalizeOldProtocolPeer(address, below, below, "0.0.1-below-floor")

	got := incompatibleBanStateOf(t, svc, address)
	if got.attempts != banned.attempts+1 {
		t.Fatalf("attempts = %d, want %d: an observation after the ban must count", got.attempts, banned.attempts+1)
	}
	if !got.bannedUntil.After(time.Now().Add(peerBanIncompatible - time.Minute)) {
		t.Fatalf("BannedUntil = %v, want a fresh %v window", got.bannedUntil, peerBanIncompatible)
	}
	if !got.ipBannedUntil.After(banned.ipBannedUntil) || !got.ipBannedUntil.Equal(got.bannedUntil) {
		t.Fatalf("IP ban until %v, want it re-armed with the address ban (%v; was %v)", got.ipBannedUntil, got.bannedUntil, banned.ipBannedUntil)
	}
}

// expireIncompatibleBan moves the incompatible-version ban of address into
// the past, as if its window had run out.
func expireIncompatibleBan(svc *Service, address domain.PeerAddress) {
	svc.peerMu.Lock()
	defer svc.peerMu.Unlock()
	expired := time.Now().Add(-time.Second)
	svc.health[address].BannedUntil = expired
	svc.health[address].IncompatibleBanUntil = expired
}

// BannedUntil is shared: the self-identity cooldown writes it too. A
// self-identity cooldown that lands on the address after its
// incompatible-version ban has run out is not that ban, so it must not
// silence the next incompatible observation: the observation counts and
// re-arms the incompatible ban, IP-wide half included.
func TestPenalizeOldProtocolPeer_SelfIdentityCooldownAfterItsBan_StillRearmsTheBan(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	address := domain.PeerAddress("203.0.113.11:64646")
	banned := penalizeUntilBanned(t, svc, address)

	expireIncompatibleBan(svc, address)
	svc.applySelfIdentityCooldown(address, svc.newSelfIdentityError(address))
	below := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	svc.penalizeOldProtocolPeer(address, below, below, "0.0.1-below-floor")

	got := incompatibleBanStateOf(t, svc, address)
	if got.attempts != banned.attempts+1 {
		t.Fatalf("attempts = %d, want %d: a self-identity cooldown silenced an observation outside the incompatible-version ban", got.attempts, banned.attempts+1)
	}
	if !got.ipBannedUntil.After(banned.ipBannedUntil) {
		t.Fatalf("IP ban until %v, want it re-armed past %v", got.ipBannedUntil, banned.ipBannedUntil)
	}
}

// Inside the ban only the accumulators stand still. The version-policy half —
// the reporter set and the persisted lockout with the client version — is
// evidence about THIS observation and is still recorded: a peer that thinks
// we are old must keep feeding update_available while its ban runs.
func TestPenalizeOldProtocolPeer_InsideItsBan_StillRecordsVersionPolicyEvidence(t *testing.T) {
	t.Parallel()
	address := domain.PeerAddress("10.0.0.7:1234")
	peerID := domaintest.ID("peer-ban-policy")
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{address: {Address: address}},
		peerIDs:       map[domain.PeerAddress]domain.PeerIdentity{address: peerID},
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		health:        make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
	}
	higherMin := domain.ProtocolVersion(config.ProtocolVersion) + 1
	for i := 0; i < attemptsToIncompatibleBan; i++ {
		svc.penalizeOldProtocolPeer(address, higherMin, higherMin, "corsa/first")
	}
	banned := incompatibleBanStateOf(t, svc, address)
	if !banned.bannedUntil.After(time.Now()) {
		t.Fatalf("precondition: not banned: %+v", banned)
	}
	svc.statusMu.Lock()
	delete(svc.versionPolicy.incompatibleReporters, peerID)
	svc.statusMu.Unlock()

	svc.penalizeOldProtocolPeer(address, higherMin+1, higherMin+1, "corsa/fifth")

	if got := incompatibleBanStateOf(t, svc, address); got != banned {
		t.Fatalf("the fifth observation changed the ban:\n before %+v\n after  %+v", banned, got)
	}
	svc.statusMu.RLock()
	_, reported := svc.versionPolicy.incompatibleReporters[peerID]
	svc.statusMu.RUnlock()
	if !reported {
		t.Fatal("the fifth observation did not reach the reporter set")
	}
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	lockout := svc.persistedMeta[address].VersionLockout
	if string(lockout.ObservedClientVersion) != "corsa/fifth" || lockout.ObservedMinimumProtocolVersion != higherMin+1 {
		t.Fatalf("lockout = %+v, want the fifth observation's evidence", lockout)
	}
}

// A blank client version is "not announced", not a value: it must not
// overwrite the version the peer is already known by (addPeerVersion skips it
// for the same reason).
func TestPenalizeOldProtocolPeer_BlankClientVersion_KeepsTheKnownOne(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	address := domain.PeerAddress("203.0.113.11:64646")
	svc.peerMu.Lock()
	svc.peerVersions[address] = "corsa/known"
	svc.peerMu.Unlock()

	below := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	svc.penalizeOldProtocolPeer(address, below, below, "   ")

	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if got := svc.peerVersions[address]; got != "corsa/known" {
		t.Fatalf("peerVersions = %q, want the known version kept", got)
	}
}

// The refused peer's client version is read under the health key
// penalizeOldProtocolPeer resolves (resolveHealthAddress). A dial address that
// is a fallback alias of another (dialOrigin) must therefore land its version
// on the primary, or the lockout records no client version at all.
func TestOnCMDialFailed_FallbackAlias_LockoutCarriesClientVersion(t *testing.T) {
	t.Parallel()
	primary := domain.PeerAddress("10.0.0.1:1234")
	fallback := domain.PeerAddress("10.0.0.1:64646")
	higherMin := domain.ProtocolVersion(config.ProtocolVersion) + 1
	svc := &Service{
		persistedMeta: map[domain.PeerAddress]*peerEntry{primary: {Address: primary}},
		peerIDs:       map[domain.PeerAddress]domain.PeerIdentity{primary: domaintest.ID("peer-alias-aaa")},
		peerBuilds:    make(map[domain.PeerAddress]int),
		peerVersions:  make(map[domain.PeerAddress]string),
		health:        make(map[domain.PeerAddress]*peerHealth),
		bannedIPSet:   make(map[string]domain.BannedIPEntry),
		dialOrigin:    map[domain.PeerAddress]domain.PeerAddress{fallback: primary},
	}

	svc.onCMDialFailed(fallback, &incompatibleProtocolError{
		PeerVersion:   higherMin,
		PeerMinimum:   higherMin,
		ClientVersion: "corsa/9.9.9-alias",
		Cause:         fmt.Errorf("protocol version too old"),
	}, true)

	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if got := string(svc.persistedMeta[primary].VersionLockout.ObservedClientVersion); got != "corsa/9.9.9-alias" {
		t.Fatalf("lockout ObservedClientVersion = %q, want the refused peer's client version", got)
	}
	if got := svc.peerVersions[fallback]; got != "" {
		t.Fatalf("peerVersions[%s] = %q: the version went to the alias, not the health key", fallback, got)
	}
}

// Same key on the inbound path: the health key built from the hello
// (observed IP + advertise_port) may itself be a dial alias.
func TestInboundIncompatibleHello_FallbackAlias_ClientVersionUnderHealthKey(t *testing.T) {
	t.Parallel()
	svc, stop := startTestNode(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
	})
	defer stop()

	const advertisePort = domain.PeerPort(64649)
	const peerClientVersion = "0.98-alias"
	alias := domain.PeerAddress(net.JoinHostPort("127.0.0.1", strconv.Itoa(int(advertisePort))))
	primary := domain.PeerAddress("127.0.0.1:7777")
	svc.peerMu.Lock()
	svc.dialOrigin[alias] = primary
	svc.peerMu.Unlock()

	conn, err := net.DialTimeout("tcp", svc.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	writeJSONFrame(t, conn, protocol.Frame{
		Type:          "hello",
		Version:       config.MinimumProtocolVersion - 1,
		Client:        "test",
		ClientVersion: peerClientVersion,
		AdvertisePort: advertisePort,
	})
	reader := bufio.NewReader(conn)
	if errFrame := readJSONTestFrame(t, reader); errFrame.Code != protocol.ErrCodeIncompatibleProtocol {
		t.Fatalf("expected incompatible-protocol error, got %#v", errFrame)
	}

	waitForConditionMsg(t, 2*time.Second, "the refused hello's client version never reached the health key", func() bool {
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		return svc.peerVersions[primary] == peerClientVersion
	})
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if got := svc.peerVersions[alias]; got != "" {
		t.Fatalf("peerVersions[%s] = %q: the version went to the alias, not the health key", alias, got)
	}
}

// IncompatibleBanUntil is not persisted: a node restarted inside a running
// incompatible-version ban derives it from the persisted evidence, so the
// first observation after the restart does not slide the ban.
func TestPenalizeOldProtocolPeer_AfterRestartInsideItsBan_NeitherAccumulatesNorSlides(t *testing.T) {
	t.Parallel()
	peersPath := filepath.Join(t.TempDir(), "peers.json")
	address := domain.PeerAddress("10.99.0.2:64646")
	below := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)

	before, stopBefore := startTestNodeWithoutDials(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
		PeersStatePath: peersPath,
	})
	before.addPeerAddress(address, "full", domaintest.ID("peer-restart-inside-ban"))
	banned := penalizeUntilBanned(t, before, address)
	before.flushPeerState()
	stopBefore()

	after, stopAfter := startTestNodeWithoutDials(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
		PeersStatePath: peersPath,
	})
	defer stopAfter()
	after.penalizeOldProtocolPeer(address, below, below, "0.0.1-below-floor")

	after.peerMu.RLock()
	health := after.health[address]
	attempts, bannedUntil := health.IncompatibleVersionAttempts, health.BannedUntil
	after.peerMu.RUnlock()
	if attempts != banned.attempts {
		t.Fatalf("attempts = %d, want %d: the first observation after a restart inside the ban was counted", attempts, banned.attempts)
	}
	if !bannedUntil.Equal(banned.bannedUntil) {
		t.Fatalf("BannedUntil = %v, want the restored %v: the ban slid", bannedUntil, banned.bannedUntil)
	}
}
