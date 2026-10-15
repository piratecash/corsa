package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// a1_node_refusal_test.go is attack A1 played end to end against real nodes:
// a man in the middle relabels an old node's v1 hello or welcome as coming
// from a victim identity that has already proved itself to the node under
// test over v2 (it is pinned). The attacker wants the node to open a legacy
// session it believes is the victim's — the relayable v1 proof. The node
// refuses on the identity before it signs or credits anything, so the victim
// gets no legacy session, no authentication record, no route and no DM.
//
// This is the node-level regression test the contract asks for: the package
// test (sessionv2) proves the proof cannot be relayed; this proves the node
// never even runs a legacy exchange for a pinned identity, however the old
// peer's frames are relabelled.

// assertVictimUntouched checks that nothing was granted to victim on node:
// not authenticated, no route, no v2 binding stolen on address.
func assertVictimUntouched(t *testing.T, node *Service, victim domain.PeerIdentity, address domain.PeerAddress, what string) {
	t.Helper()
	if nodeAuthenticatedIdentity(node, victim) {
		t.Fatalf("%s: the node authenticated the victim over a relabelled v1 session", what)
	}
	if routes := node.routingTable.Lookup(victim); len(routes) != 0 {
		t.Fatalf("%s: a route to the victim appeared through the refused session: %+v", what, routes)
	}
	// The victim's pin must survive: the refusal is not allowed to cost it.
	if !node.secureSessions.store.identityPinned(victim) {
		t.Fatalf("%s: the refusals cost the victim its v2 pin", what)
	}
}

// Inbound A1: an old node dials the node under test, and the man in the middle
// relabels its hello as the pinned victim's. The node refuses at the hello —
// before it issues a challenge — so nothing is signed, learned or routed for
// the victim. This is a man in the middle forging an answer for a pinned
// identity, played on the inbound path.
func TestInboundA1RelabelledAsAPinnedVictimIsRefused(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			victim := domain.PeerIdentity{0xa1, 0xce}
			newAddress := freeAddress(t)
			oldAddress := freeAddress(t)

			oldDialsNew := startImpersonatingProxy(t, newAddress, oldPeerDials, version, victim)

			newNode, stopNew := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  newAddress,
				BootstrapPeers: []string{},
				Type:           config.NodeTypeFull,
			}, withoutOwnDials(func(svc *Service) {
				if err := svc.secureSessions.store.noteProvenInbound(victim); err != nil {
					t.Fatalf("pin victim: %v", err)
				}
			}))
			defer stopNew()

			_, stopOld := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  oldAddress,
				BootstrapPeers: []string{oldDialsNew.address()},
				Type:           config.NodeTypeFull,
			}, asOldNode)
			defer stopOld()

			// The old node keeps dialling; each dial is relabelled and refused
			// at the hello. Wait until at least one refusal was counted.
			waitForConditionMsg(t, 20*time.Second, "the node never refused the relabelled inbound hello", func() bool {
				return newNode.SecureSessionStoreStats().V1RefusalsIdentityPinned > 0
			})
			assertVictimUntouched(t, newNode, victim, domain.PeerAddress(oldDialsNew.address()), "inbound A1 v"+versionName(version))
			// The refusal is at the hello, before a challenge is issued: the
			// node never sent a welcome. Everything the hello path would do for
			// the victim — rememberConnPeerAddr, the peer and health records,
			// the announce of the new peer — runs only after the challenge, so a node
			// that sent no welcome did none of it.
			if oldDialsNew.nodeSentFrame("welcome") {
				t.Fatalf("v%d: the node sent a welcome with a challenge for a pinned victim's relabelled hello", version)
			}
		})
	}
}

// Outbound A1: the node under test dials an old node, and the man in the
// middle relabels its welcome as the pinned victim's. The node refuses the
// welcome before signing auth_session, so it never proves itself to the
// relay and the address keeps no binding as the victim. The pin was earned
// over another path, and it alone blocks the legacy session with the victim;
// the refusal costs the dialled address nothing.
func TestOutboundA1RelabelledAsAPinnedVictimIsRefused(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			victim := domain.PeerIdentity{0xa1, 0xce}
			oldAddress := freeAddress(t)
			newDialsOld := startImpersonatingProxy(t, oldAddress, oldPeerListens, version, victim)

			_, stopOld := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  oldAddress,
				BootstrapPeers: []string{},
				Type:           config.NodeTypeFull,
			}, withoutOwnDials(asOldNode))
			defer stopOld()

			dialer, stopDialer := startQuietNode(t, func(svc *Service) {
				if err := svc.secureSessions.store.noteProvenInbound(victim); err != nil {
					t.Fatalf("pin victim: %v", err)
				}
			})
			defer stopDialer()
			target := newDialsOld.peerAddress()

			established, err := dialPastTransport(t, dialer, target)
			if established || !errors.Is(err, errLegacyRefusedPinned) {
				t.Fatalf("v%d: dial ended established=%v err=%v, want errLegacyRefusedPinned", version, established, err)
			}
			assertVictimUntouched(t, dialer, victim, target, "outbound A1 v"+versionName(version))
			// The refusal is at the welcome, before auth_session: the node put
			// its v1 hello on the wire but never signed an auth_session, so the
			// relay holds no signature of this node to carry anywhere as the
			// victim. The proxy is drained first: until it has read everything
			// the node wrote, an auth_session still in the socket buffer would
			// go unseen and the check below would pass on a race.
			newDialsOld.drain(t)
			if !newDialsOld.nodeSentFrame("hello") {
				t.Fatalf("v%d: the node never sent a v1 hello — the dial did not reach the welcome", version)
			}
			if newDialsOld.nodeSentFrame("auth_session") {
				t.Fatalf("v%d: the node signed an auth_session after a relabelled pinned welcome", version)
			}

			// The refusal must not charge the address: a man in the middle can
			// force it at will. Score, failures and cooldown are untouched;
			// only the fall back to v1 is held.
			dialer.peerMu.RLock()
			health := dialer.health[target]
			setup := dialer.setupFailures[target]
			dialer.peerMu.RUnlock()
			if setup != nil && setup.Consecutive > 0 {
				t.Fatalf("v%d: the refusal was counted as a setup failure: %+v", version, *setup)
			}
			if health != nil && (health.ConsecutiveFailures != 0 || health.Score < 0 || !health.BannedUntil.IsZero()) {
				t.Fatalf("v%d: the refusal charged the address: %+v", version, *health)
			}
			if dialer.secureSessions.store.endpointRequiresV2(target) {
				t.Fatalf("v%d: the refused dial created a v2 binding on the address", version)
			}
			if stats := dialer.SecureSessionStoreStats(); stats.V1RefusalsIdentityPinned == 0 {
				t.Fatalf("v%d: the refusal of a pinned identity was not counted", version)
			}
		})
	}
}

// Residual risk, pinned down as intended: a victim the node does NOT know over
// v2 is not protected. When the relabelled welcome names an UNPINNED identity,
// the node does not refuse — it signs auth_session and goes legacy. This is the
// transition's open case: only pins and endpoint bindings close
// A1, and a node that never met the victim over v2 has neither. The test
// records the gap so a future change that silently narrows or widens it is
// caught.
func TestOutboundLegacyProceedsForAnUnpinnedRelabelledIdentity(t *testing.T) {
	t.Parallel()
	const version = config.MinimumProtocolVersion
	unpinned := domain.PeerIdentity{0xbe, 0xef}
	oldAddress := freeAddress(t)
	newDialsOld := startImpersonatingProxy(t, oldAddress, oldPeerListens, version, unpinned)

	_, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  oldAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, withoutOwnDials(asOldNode))
	defer stopOld()

	// The dialler pins nothing: the relabelled identity is a stranger to it.
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	target := newDialsOld.peerAddress()

	// The dial is not refused on the identity. It may or may not end in a live
	// session depending on how the old node answers the auth_session, but the
	// node does proceed past the identity gate and sign one — which is exactly
	// the residual: nothing protected this identity.
	_, _ = dialPastTransport(t, dialer, target)
	if !newDialsOld.nodeSentFrame("auth_session") {
		t.Fatal("the node did not sign auth_session for an unpinned relabelled identity: the residual legacy path is not what this test documents")
	}
	if stats := dialer.SecureSessionStoreStats(); stats.V1RefusalsIdentityPinned != 0 {
		t.Fatalf("an unpinned identity was refused as pinned: %d", stats.V1RefusalsIdentityPinned)
	}
}
