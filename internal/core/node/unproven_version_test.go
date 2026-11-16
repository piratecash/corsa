package node

import (
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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

// A version refusal must not charge the endpoint's previous identity or IP,
// even when the responder proved its own identity over TLSv2.
func TestIncompatibleDialOnlyPacesTheEndpoint(t *testing.T) {
	t.Parallel()
	for name, evidence := range map[string]versionEvidence{"claimed": versionClaimed, "proven": versionProven} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			sibling := domain.PeerAddress("198.51.100.99:64646")
			knowVictimAt(svc, dialChoiceAddress, sibling)
			welcome := protocol.Frame{Type: "welcome", Version: 20, MinimumProtocolVersion: 20}
			for range 5 {
				svc.onCMDialFailed(dialChoiceAddress, incompatibleVersionOf(welcome, evidence, errors.New("test")), true)
			}
			assertOnlyPaced(t, svc, dialChoiceAddress)
			assertNoVersionHarm(t, svc, sibling)
		})
	}
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
