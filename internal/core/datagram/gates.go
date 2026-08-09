package datagram

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// gates.go holds the ROLE gate of §6 and the candidate filter it implies: may
// this peer be an endpoint of the plane at all, and may it forward somebody
// else's frame.
//
// THE PATH-WIDE GATES ARE GONE — first the tuple-gate over (av, ext.v), then
// the `req_caps` name gate that outlived it. Both made every relay a judge of
// the endpoint protocol: a frame naming a capability its relay had never heard
// of was refused mid-path, so a protocol released after a node could not travel
// through it. That is the opposite of what a stable envelope is for. What
// remains is a peer's own role — the two capabilities of §6 — and the endpoint
// gate over `dtype`, which only the hop handing the frame to its destination
// applies.
//
// Reference: docs/protocol/datagram.md §4.1 step 3, §4.3, §6.

// The two role capabilities of §6, in the layer's own name type. They are
// CONVERSIONS of the typed domain constants and never literals: the node builds
// its role advertisement from domain.Capability while this layer gates on
// domain.CapabilityName, and a second spelling of the same wire name would
// eventually disagree with the first.
const (
	// CapabilityDatagramV1 means the node understands the envelope and can
	// be an ENDPOINT: accept datagrams addressed to it and send its own.
	CapabilityDatagramV1 = domain.CapabilityName(domain.CapMeshDatagramV1)

	// CapabilityDatagramTransitV1 means the node is willing to FORWARD
	// other nodes' datagrams. Advertised only by nodes that really do.
	CapabilityDatagramTransitV1 = domain.CapabilityName(domain.CapMeshDatagramTransitV1)
)

// ---------------------------------------------------------------------------
// The validated raw advertised set
// ---------------------------------------------------------------------------

// AdvertisedCapabilities is the VALIDATED RAW set of capability names a node
// advertises — the set kept beside the typed one, so a name is comparable by
// string even when this build does not know it.
//
// It exists because intersectCapabilities drops every name this build does
// not know. Dispatch and every existing decision keep running on the typed
// set; the datagram role gate reads this one.
type AdvertisedCapabilities struct {
	names map[domain.CapabilityName]struct{}
}

// NewAdvertisedCapabilities validates a peer's advertised names.
//
// Any breach of the bounds (64 names, 40 chars each, `[a-z0-9_]`) empties
// the WHOLE set rather than dropping the offending name, because "drop one"
// and "drop the set" behave differently in mixed implementations. The session
// is not torn down and the typed capability set is untouched.
func NewAdvertisedCapabilities(names []string) AdvertisedCapabilities {
	parsed := domain.ParseRawCapabilityNames(names)
	set := AdvertisedCapabilities{names: make(map[domain.CapabilityName]struct{}, len(parsed))}
	for _, name := range parsed {
		set.names[name] = struct{}{}
	}
	return set
}

// Has reports whether the name is advertised.
func (a AdvertisedCapabilities) Has(name domain.CapabilityName) bool {
	_, ok := a.names[name]
	return ok
}

// ---------------------------------------------------------------------------
// Candidate filter (§4.3, §6)
// ---------------------------------------------------------------------------

// CandidateRole is the role a peer would play for this frame. It is DERIVED
// from the frame, never passed in: "the candidate is the dst" is the whole
// definition of the last hop, and letting a caller assert the role would
// make the last-hop rule an opinion.
type CandidateRole uint8

const (
	// CandidateRoleUnset is the zero value.
	CandidateRoleUnset CandidateRole = iota
	// CandidateRoleTransit is a peer that would forward the frame onward.
	CandidateRoleTransit
	// CandidateRoleLastHop is a peer that IS the destination.
	CandidateRoleLastHop
)

var candidateRoleNames = map[CandidateRole]string{
	CandidateRoleUnset:   "unset",
	CandidateRoleTransit: "transit",
	CandidateRoleLastHop: "last_hop",
}

// String returns the metric label of the role.
func (r CandidateRole) String() string { return enumName(candidateRoleNames, r) }

// CandidateOutcome is the verdict of the candidate filter.
type CandidateOutcome uint8

const (
	// CandidateOutcomeUnset is the zero value.
	CandidateOutcomeUnset CandidateOutcome = iota
	// CandidateAdmitted means the peer may carry this frame in its role.
	CandidateAdmitted
	// CandidateMissingEndpoint means the peer does not advertise
	// mesh_datagram_v1: the command does not exist for it at all.
	CandidateMissingEndpoint
	// CandidateMissingTransit means the peer will not forward other
	// people's datagrams, and this frame would need it to.
	CandidateMissingTransit
)

var candidateOutcomeNames = map[CandidateOutcome]string{
	CandidateOutcomeUnset:    "unset",
	CandidateAdmitted:        "admitted",
	CandidateMissingEndpoint: "missing_endpoint_capability",
	CandidateMissingTransit:  "missing_transit_capability",
}

// String returns the metric label of the outcome.
func (o CandidateOutcome) String() string { return enumName(candidateOutcomeNames, o) }

// CandidateDecision carries the verdict, the role it was judged in and, on a
// refusal, the missing name.
type CandidateDecision struct {
	outcome CandidateOutcome
	role    CandidateRole
	missing domain.CapabilityName
}

// Outcome reports the verdict.
func (d CandidateDecision) Outcome() CandidateOutcome { return d.outcome }

// Admitted reports whether the peer may carry the frame.
func (d CandidateDecision) Admitted() bool { return d.outcome == CandidateAdmitted }

// Role reports whether the peer was judged as transit or as the last hop.
func (d CandidateDecision) Role() CandidateRole { return d.role }

// Missing returns the capability the peer lacks. The bool is false when the
// candidate was admitted.
func (d CandidateDecision) Missing() (domain.CapabilityName, bool) {
	if d.outcome == CandidateAdmitted {
		return "", false
	}
	return d.missing, true
}

// AdmitCandidate is the ROLE filter of §6 as a pure function of the names a
// candidate advertises. The rule lives here so the scheduler and the
// reachability probe apply ONE implementation.
//
// Two checks, and deliberately no third:
//
//  1. mesh_datagram_v1 always — a peer without it answers unknown_command
//     and closes the connection;
//  2. mesh_datagram_transit_v1 unless the candidate IS the dst — an
//     endpoint-only client must be able to receive what is addressed to it
//     without ever claiming to relay (§6).
//
// Both names describe what the PEER is on this plane, not what the frame
// carries. The gate that used to read the frame — every name from `req_caps`,
// last hop included — is gone with the field: a path-wide requirement is the
// mechanism by which an old relay refuses a new endpoint protocol, and the
// envelope is supposed to outlive both.
func AdmitCandidate(frame protocol.DatagramFrame, candidate domain.PeerIdentity, advertised AdvertisedCapabilities) CandidateDecision {
	role := CandidateRoleTransit
	if !candidate.IsZero() && candidate == frame.Dst {
		role = CandidateRoleLastHop
	}
	if !advertised.Has(CapabilityDatagramV1) {
		return CandidateDecision{outcome: CandidateMissingEndpoint, role: role, missing: CapabilityDatagramV1}
	}
	if role == CandidateRoleTransit && !advertised.Has(CapabilityDatagramTransitV1) {
		return CandidateDecision{outcome: CandidateMissingTransit, role: role, missing: CapabilityDatagramTransitV1}
	}
	return CandidateDecision{outcome: CandidateAdmitted, role: role}
}
