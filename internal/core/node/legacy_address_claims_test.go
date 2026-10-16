package node

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_address_claims_test.go pins what the address → identity map keeps
// once an identity proves itself over v2 (docs/protocol/session_v2.md,
// "Downgrade protection"; docs/locking.md): the addresses a v1 hello, a v1
// welcome or peer exchange merely claimed for that identity are forgotten —
// in memory and in peers.json, so a restart does not bring them back — while
// an entry v2 recorded, and an address bound to the identity by a v2 dial,
// stay. A claim never overwrites what v2 recorded.

// newPeerStateService is a Service whose peers.json lives at path.
func newPeerStateService(t *testing.T, path string) *Service {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)
	svc := NewServiceWithNetwork(configtest.WithTempDataDirs(t, config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: filepath.Join(filepath.Dir(path), "trust.json"),
		PeersStatePath: path,
	}), testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)
	return svc
}

// knownAddressClaiming adds address as a peer-exchange address that claims
// id, as an unsigned peers answer would.
func knownAddressClaiming(t *testing.T, svc *Service, address domain.PeerAddress, id domain.PeerIdentity) {
	t.Helper()
	if !svc.addPeerAddress(address, "full", id) {
		t.Fatalf("precondition: %s was not added as a new peer", address)
	}
}

func identityAt(svc *Service, address domain.PeerAddress) (addressIdentity, bool) {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	entry, ok := svc.peerIDs[address]
	return entry, ok
}

func persistedIdentityAt(svc *Service, address domain.PeerAddress) domain.PeerIdentity {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if meta := svc.persistedMeta[address]; meta != nil {
		return meta.Identity
	}
	return domain.PeerIdentity{}
}

// After X proves itself over v2, an address only a claim tied to X loses it:
// from peerIDs, from the persisted metadata, and from peers.json after a
// flush and a restart. An entry v2 recorded for X, an address bound to X by a
// v2 dial, and claims of other identities stay.
func TestAProofOfV2ForgetsTheAddressesOnlyAClaimTiedToTheIdentity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "peers.json")
	svc := newPeerStateService(t, path)
	peer, _ := sessionv2test.NewProvenPeer(t)
	x, other := peer.Identity, domain.PeerIdentity{0x96, 0x01}
	claimed := domain.PeerAddress("198.51.100.11:64646")
	bound := domain.PeerAddress("198.51.100.12:64646")
	proven := domain.PeerAddress("198.51.100.13:64646")
	another := domain.PeerAddress("198.51.100.14:64646")

	knownAddressClaiming(t, svc, claimed, x)
	knownAddressClaiming(t, svc, bound, x)
	knownAddressClaiming(t, svc, proven, domain.PeerIdentity{})
	knownAddressClaiming(t, svc, another, other)
	svc.addPeerID(proven, provenAddressIdentity(x))
	if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(bound), x, endpointProtectionNone()); err != nil {
		t.Fatalf("bind %s to x: %v", bound, err)
	}
	svc.peerMu.Lock()
	svc.peerStateDirty = false
	svc.peerMu.Unlock()

	if err := svc.onIdentityProvenV2(context.Background(), sessionv2test.Proof(t, peer)); err != nil {
		t.Fatalf("proof: %v", err)
	}

	if entry, ok := identityAt(svc, claimed); ok {
		t.Fatalf("the claimed address still names %s (%s) after the proof", entry.identity, entry.origin)
	}
	if got := persistedIdentityAt(svc, claimed); !got.IsZero() {
		t.Fatalf("the persisted metadata of the claimed address still names %s", got)
	}
	for address, want := range map[domain.PeerAddress]domain.PeerIdentity{bound: x, proven: x, another: other} {
		if entry, ok := identityAt(svc, address); !ok || entry.identity != want {
			t.Fatalf("%s lost its identity: %+v (present %v), want %s", address, entry, ok, want)
		}
	}
	svc.peerMu.RLock()
	dirty := svc.peerStateDirty
	svc.peerMu.RUnlock()
	if !dirty {
		t.Fatal("the forgotten claim was not marked for the next peers.json write")
	}

	svc.flushPeerState()
	restarted := newPeerStateService(t, path)
	if entry, ok := identityAt(restarted, claimed); ok {
		t.Fatalf("after a restart the claimed address names %s again", entry.identity)
	}
	if entry, ok := identityAt(restarted, proven); !ok || entry.identity != x || entry.origin != domain.PeerIdentityProvenV2 {
		t.Fatalf("after a restart the v2 entry is %+v (present %v), want %s proven over v2", entry, ok, x)
	}
	if entry, ok := identityAt(restarted, another); !ok || entry.identity != other || entry.origin != domain.PeerIdentityClaimed {
		t.Fatalf("after a restart the other claim is %+v (present %v), want %s claimed", entry, ok, other)
	}
}

// A claim — a v1 hello or welcome, peer exchange — never overwrites the
// identity v2 recorded for an address; a later v2 record does.
func TestAClaimNeverOverwritesWhatV2RecordedForAnAddress(t *testing.T) {
	t.Parallel()
	svc := newPeerStateService(t, filepath.Join(t.TempDir(), "peers.json"))
	address := domain.PeerAddress("198.51.100.21:64646")
	x, y, z := domain.PeerIdentity{0x96, 0x02}, domain.PeerIdentity{0x96, 0x03}, domain.PeerIdentity{0x96, 0x04}
	knownAddressClaiming(t, svc, address, y)
	svc.addPeerID(address, provenAddressIdentity(x))

	svc.addPeerID(address, claimedAddressIdentity(y))
	svc.addPeerAddress(address, "full", y)
	if entry, _ := identityAt(svc, address); entry.identity != x || entry.origin != domain.PeerIdentityProvenV2 {
		t.Fatalf("a claim overwrote the v2 record: %+v, want %s proven over v2", entry, x)
	}
	if got := persistedIdentityAt(svc, address); got != x {
		t.Fatalf("a claim overwrote the persisted v2 identity: %s, want %s", got, x)
	}

	svc.addPeerID(address, provenAddressIdentity(z))
	if entry, _ := identityAt(svc, address); entry.identity != z || entry.origin != domain.PeerIdentityProvenV2 {
		t.Fatalf("a newer v2 record did not replace the old one: %+v, want %s", entry, z)
	}
}

// A v1 path checks the pin first and records the claim later — the CM dial
// (refuseLegacyWelcome, then the welcome and the session identity), the
// accepted connection (refuseLegacyIdentity, then completeInboundAuth) and
// syncPeer (admitServiceDialWelcome, then auth_session, then the welcome).
// X may prove itself over v2 in between. Each case is the sequence of writes
// its path makes after its gate, run once X's proof (pin and forget) has
// completed: none of them may leave the claim in peerIDs, in the persisted
// metadata or in peers.json.
func TestAClaimRecordedAfterTheIdentityProvedV2IsRefused(t *testing.T) {
	t.Parallel()
	address := domain.PeerAddress("198.51.100.31:64646")
	paths := map[string]func(svc *Service, welcome protocol.Frame, x domain.PeerIdentity){
		"outbound CM dial": func(svc *Service, welcome protocol.Frame, x domain.PeerIdentity) {
			svc.learnIdentityFromWelcome(welcome, address, domain.PeerIdentityClaimed)
			svc.addPeerID(address, claimedAddressIdentity(x))
		},
		"accepted connection": func(svc *Service, welcome protocol.Frame, x domain.PeerIdentity) {
			hello := welcome
			hello.Type = "hello"
			svc.learnPeerFromFrame(string(address), hello, domain.PeerIdentityClaimed)
			svc.addPeerID(address, claimedAddressIdentity(x))
		},
		"syncPeer": func(svc *Service, welcome protocol.Frame, _ domain.PeerIdentity) {
			svc.learnIdentityFromWelcome(welcome, address, domain.PeerIdentityClaimed)
		},
	}
	for name, recordClaim := range paths {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "peers.json")
			svc := newPeerStateService(t, path)
			peer, _ := sessionv2test.NewProvenPeer(t)
			x := peer.Identity
			knownAddressClaiming(t, svc, address, domain.PeerIdentity{})
			if err := svc.secureSessions.store.noteProvenInbound(x); err != nil {
				t.Fatalf("pin: %v", err)
			}
			if err := svc.onIdentityProvenV2(context.Background(), sessionv2test.Proof(t, peer)); err != nil {
				t.Fatalf("proof: %v", err)
			}

			recordClaim(svc, protocol.Frame{Type: "welcome", Address: x.String(), Listener: "1", AdvertisePort: 64646}, x)

			if entry, ok := identityAt(svc, address); ok {
				t.Fatalf("the claim of a proven identity was recorded: %+v", entry)
			}
			if got := persistedIdentityAt(svc, address); !got.IsZero() {
				t.Fatalf("the persisted metadata carries the claim of a proven identity: %s", got)
			}
			svc.flushPeerState()
			if entry, ok := identityAt(newPeerStateService(t, path), address); ok {
				t.Fatalf("peers.json brought the claim back after a restart: %+v", entry)
			}
		})
	}
}

// provenAddressIdentity is an identity a v2 session on the address proved.
func provenAddressIdentity(id domain.PeerIdentity) addressIdentity {
	return addressIdentity{identity: id, origin: domain.PeerIdentityProvenV2}
}
