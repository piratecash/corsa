package node

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// anonymity_claim_guard_test.go enforces the FIRST item of the §3 ban in
// docs/refactoring/dht/21-anonymity-transport.md: until 21c has passed, no
// build advertises a capability that claims an anonymous mode.
//
// The guard is a CLOSED SET over the advertisement this node really builds,
// not a search for a forbidden name. A name that does not exist yet cannot be
// asserted absent: `!contains("mesh_anonymous_v1")` passes for every other
// spelling of the same promise, and a check that can only fail on a string
// somebody invented in a test is not protection. Pinning the whole set inverts
// the question — anything NEW in the advertisement turns this test red, and
// whoever added it has to come here, read why, and decide deliberately.
//
// The set is checked at THREE points, because each of them is a place a name
// could enter and the earlier ones do not cover the later:
//
//  1. localCapabilities — the role list, under every combination of its inputs;
//  2. localHandshakeCapabilityStrings(localHandshakeCapabilityNames()) — the
//     projection the wire really uses. NOTE it is NOT localCapabilityStrings:
//     that one feeds the datagram pipeline's own gates (datagram_layer.go), and
//     a review found that pinning it left the real projection unguarded;
//  3. the hello and welcome FRAMES themselves, as built by nodeHelloJSONLine
//     and welcomeFrame — the last point before the bytes leave the node.
//
// What these tests prove, and what they do NOT:
//
//   - proves: every capability name this node puts on the wire is in the
//     reviewed set, at each of the three points above;
//   - does NOT prove that any transport is anonymous, and does NOT cover the
//     other ways a build could claim anonymity — RPC output, UI strings,
//     release notes and documentation are outside the wire advertisement and
//     are checked separately (§3 items 2 and 3 of step 21).
//
// The allow-list lives HERE and not in production code on purpose: step 21
// forbids introducing infrastructure whose only consumer is the check that it
// is unused.
func advertisableCapabilities() map[domain.Capability]struct{} {
	return map[domain.Capability]struct{}{
		domain.CapMeshRelayV1:           {},
		domain.CapMeshRoutingV1:         {},
		domain.CapMeshRoutingV2:         {},
		domain.CapFileTransferV1:        {},
		domain.CapMeshRouteProbeV1:      {},
		domain.CapMeshRouteQueryV1:      {},
		domain.CapMeshRouteSyncV1:       {},
		domain.CapMeshRoutingV3:         {},
		domain.CapMeshPoisonReverseV1:   {},
		domain.CapMeshPoisonReverseV2:   {},
		domain.CapMeshDatagramV1:        {},
		domain.CapMeshDatagramTransitV1: {},
	}
}

// assertAdvertisedNames is the single verdict shared by all three points, so a
// name rejected at one of them is rejected at every other in the same words.
func assertAdvertisedNames(t *testing.T, where string, names []string) {
	t.Helper()
	allowed := advertisableCapabilities()
	for _, name := range names {
		capability, ok := domain.ParseCapability(name)
		if !ok {
			t.Fatalf("%s advertises %q, which domain.ParseCapability does not know: "+
				"a name the domain cannot parse must never reach the wire", where, name)
		}
		if _, ok := allowed[capability]; !ok {
			t.Fatalf("%s advertises %q, which is not in the reviewed set. "+
				"A new capability must be checked against docs/refactoring/dht/21-anonymity-transport.md §3: "+
				"no role claiming an anonymous mode may be advertised until 21c has passed. "+
				"If the name is unrelated to anonymity, add it to advertisableCapabilities in the same "+
				"change that adds it to capabilities.go", where, name)
		}
	}
}

// TestLocalAdvertisementIsAClosedSet walks every combination of the inputs
// localCapabilities takes — the routing-v3 opt-in and the two halves of the
// datagram advertise — because a capability added under one flag combination
// only would otherwise travel unnoticed under the others.
func TestLocalAdvertisementIsAClosedSet(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		for _, endpoint := range []bool{false, true} {
			for _, transit := range []bool{false, true} {
				advertise := datagramAdvertise{Endpoint: endpoint, Transit: transit}
				caps := localCapabilities(v3, advertise)
				assertAdvertisedNames(t, "localCapabilities", domain.CapabilityStrings(caps))
			}
		}
	}
}

// TestHandshakeProjectionIsTheSameClosedSet checks the projection the wire
// really uses — localHandshakeCapabilityStrings over the names the handshake
// builds — element by element against the role list it must reproduce.
//
// A Service with no wired datagram layer claims neither datagram role even
// with the feature flag on: that is localDatagramAdvertise's own honesty rule
// (a node must not advertise a plane that does not exist), and it is asserted
// here so a change that starts advertising from the flag alone is visible.
func TestHandshakeProjectionIsTheSameClosedSet(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		svc := &Service{cfg: config.Node{EnableMeshRoutingV3: v3, EnableDatagramV1: true}}

		names := svc.localHandshakeCapabilityNames()
		strs := localHandshakeCapabilityStrings(names)
		assertAdvertisedNames(t, "localHandshakeCapabilityStrings", strs)

		want := domain.CapabilityStrings(localCapabilities(v3, datagramAdvertise{}))
		if len(strs) != len(want) {
			t.Fatalf("localHandshakeCapabilityStrings(v3=%v) = %v, want %v", v3, strs, want)
		}
		for i, s := range strs {
			if s != want[i] {
				t.Fatalf("localHandshakeCapabilityStrings(v3=%v)[%d] = %q, want %q", v3, i, s, want[i])
			}
		}
	}
}

// TestHandshakeFramesCarryOnlyReviewedCapabilities is the last point before the
// bytes leave: the hello line and the welcome frame as the node really builds
// them. It exists because the two earlier tests check FUNCTIONS, and a frame
// builder is free to put a name into Capabilities without going through either.
func TestHandshakeFramesCarryOnlyReviewedCapabilities(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		svc := &Service{
			cfg:      config.Node{EnableMeshRoutingV3: v3, EnableDatagramV1: true},
			identity: &identity.Identity{Address: "self"},
		}

		welcome := svc.welcomeFrame("challenge", "")
		assertAdvertisedNames(t, "welcome frame", welcome.Capabilities)

		line := svc.nodeHelloJSONLine()
		if line == "" {
			t.Fatal("nodeHelloJSONLine returned an empty line; the hello capability set cannot be checked")
		}
		var hello protocol.Frame
		if err := json.Unmarshal([]byte(line), &hello); err != nil {
			t.Fatalf("hello line does not parse: %v", err)
		}
		if hello.Type != "hello" {
			t.Fatalf("parsed frame type = %q, want hello", hello.Type)
		}
		assertAdvertisedNames(t, "hello frame", hello.Capabilities)

		// Both halves of the handshake must claim the same thing: a node that
		// advertised one set in hello and another in welcome would let a name
		// reach only the peers that dialled it.
		if len(hello.Capabilities) != len(welcome.Capabilities) {
			t.Fatalf("hello advertises %v, welcome advertises %v", hello.Capabilities, welcome.Capabilities)
		}
		for i, name := range hello.Capabilities {
			if welcome.Capabilities[i] != name {
				t.Fatalf("hello[%d] = %q, welcome[%d] = %q", i, name, i, welcome.Capabilities[i])
			}
		}
	}
}
