package datagram

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// gates_test.go checks the ROLE gate of §6 and the candidate filter it
// implies: what a node must advertise to be handed a frame, in either role.
//
// The two path-wide gates that used to sit beside it are gone. The tuple-gate
// compared the (av, ext.v) pair of a frame against a contract this node had
// registered; the `req_caps` gate compared names the SENDER wrote into the
// envelope against the set every node on the path advertises. Both made a
// relay refuse frames of an endpoint protocol released after it.

func advertising(names ...domain.CapabilityName) AdvertisedCapabilities {
	raw := make([]string, 0, len(names))
	for _, name := range names {
		raw = append(raw, name.String())
	}
	return NewAdvertisedCapabilities(raw)
}

// gatesTestFrame builds a routed frame the gate can be asked about. Nothing
// here is signed: the gate is string comparison and never looks at auth.
func gatesTestFrame() protocol.DatagramFrame {
	return protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         domaintest.ID("gates-src"),
		Dst:         domaintest.ID("gates-dst"),
		TTL:         4,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DType("push_identity"),
	}
}

// TestRawCapabilitySetKeepsNamesThisBuildDoesNotKnow is the reason the raw
// set exists at all: intersectCapabilities drops unknown names, and a set
// that only ever held the compile-time list could not answer a question about
// a name released later.
func TestRawCapabilitySetKeepsNamesThisBuildDoesNotKnow(t *testing.T) {
	unknown := domain.CapabilityName("some_future_profile_v3")
	if _, known := domain.ParseCapability(unknown.String()); known {
		t.Fatalf("%q is in the compile-time set, pick a name this build really does not know", unknown)
	}

	set := NewAdvertisedCapabilities([]string{"mesh_datagram_v1", unknown.String()})
	if !set.Has(unknown) {
		t.Fatal("the raw set dropped a name outside the compile-time list")
	}
}

// TestRawCapabilitySetIsEmptiedWholeOnABreach pins the reaction to a breach:
// the WHOLE set goes, not the offending name — "drop one" and "drop the set"
// behave differently in mixed implementations, so the reaction is fixed.
func TestRawCapabilitySetIsEmptiedWholeOnABreach(t *testing.T) {
	tooMany := make([]string, domain.MaxRawCapabilityNames+1)
	for i := range tooMany {
		tooMany[i] = "cap_" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}

	cases := map[string][]string{
		"too many names": tooMany,
		"name too long":  {"mesh_datagram_v1", strings.Repeat("x", domain.MaxCapabilityNameLen+1)},
		"bad charset":    {"mesh_datagram_v1", "mesh-datagram-v2"},
		"empty name":     {"mesh_datagram_v1", ""},
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			set := NewAdvertisedCapabilities(raw)
			if len(set.names) != 0 {
				t.Fatalf("set holds %d names, want the whole set emptied", len(set.names))
			}
			if set.Has(CapabilityDatagramV1) {
				t.Fatal("a valid name survived a breach of the bounds")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Candidate filter
// ---------------------------------------------------------------------------

// TestCandidateFilterAppliesTheLastHopRule pins §6: transit needs
// mesh_datagram_transit_v1, the destination itself does not — an
// endpoint-only client must be able to receive what is addressed to it
// without ever claiming to relay.
func TestCandidateFilterAppliesTheLastHopRule(t *testing.T) {
	frame := gatesTestFrame()
	relay := domaintest.ID("gates-relay")
	endpointOnly := advertising(CapabilityDatagramV1)

	asTransit := AdmitCandidate(frame, relay, endpointOnly)
	if asTransit.Outcome() != CandidateMissingTransit {
		t.Fatalf("transit candidate: outcome = %s, want missing_transit_capability", asTransit.Outcome())
	}
	if asTransit.Role() != CandidateRoleTransit {
		t.Fatalf("role = %s, want transit", asTransit.Role())
	}

	asLastHop := AdmitCandidate(frame, frame.Dst, endpointOnly)
	if !asLastHop.Admitted() {
		t.Fatalf("last hop: outcome = %s, want admitted", asLastHop.Outcome())
	}
	if asLastHop.Role() != CandidateRoleLastHop {
		t.Fatalf("role = %s, want last_hop", asLastHop.Role())
	}
}

// TestCandidateFilterJudgesTheRoleAndNotTheFrame pins the absence of the
// path-wide gate: a candidate advertising the two role names carries ANY
// frame, whatever the endpoints agreed to put in it. This is the rule that
// lets a protocol released after a relay travel through it.
func TestCandidateFilterJudgesTheRoleAndNotTheFrame(t *testing.T) {
	frame := gatesTestFrame()
	frame.DType = domain.DType("a_protocol_this_relay_never_heard_of")
	relay := domaintest.ID("gates-relay")
	roleOnly := advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1)

	if decision := AdmitCandidate(frame, relay, roleOnly); !decision.Admitted() {
		t.Fatalf("transit: outcome = %s, want admitted", decision.Outcome())
	}
	if decision := AdmitCandidate(frame, frame.Dst, roleOnly); !decision.Admitted() {
		t.Fatalf("last hop: outcome = %s, want admitted", decision.Outcome())
	}
}

// TestCandidateFilterNeedsTheEnvelopeCapability pins the first check: a peer
// without mesh_datagram_v1 has no such command at all — sending to it would
// earn an unknown_command and a closed connection (§6).
func TestCandidateFilterNeedsTheEnvelopeCapability(t *testing.T) {
	frame := gatesTestFrame()
	relay := domaintest.ID("gates-relay")

	decision := AdmitCandidate(frame, relay, advertising(CapabilityDatagramTransitV1))
	if decision.Outcome() != CandidateMissingEndpoint {
		t.Fatalf("outcome = %s, want missing_endpoint_capability", decision.Outcome())
	}
	if missing, _ := decision.Missing(); missing != CapabilityDatagramV1 {
		t.Fatalf("missing = %q, want %q", missing, CapabilityDatagramV1)
	}

	full := advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1)
	if !AdmitCandidate(frame, relay, full).Admitted() {
		t.Fatal("a fully capable transit candidate was refused")
	}
}
