package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// HandshakeObservationKey holds caller assertions, not verified attribution.
// Zero source tokens retain the existing source's UNKNOWN semantics; they
// never change gate admission. No timestamp substitutes for a process epoch.
type HandshakeObservationKey struct {
	Anchor, ArtifactSHA256, Boot, ProcessInstance string
	Epoch, ClockDomain, Configuration             [16]byte
}

// Capacity is a caller-owned prepublication allocation request. No deployment
// limit or arbitrary allocation fit is selected here. Invalid capacity retains
// the actual gate with UNKNOWN evidence, as the existing source specifies.
type HandshakeObservationRequest struct {
	Key      HandshakeObservationKey
	Capacity int
}

type HandshakeObservationBounds struct{ Node, Loopback, PerSource int }
type HandshakeObservationSource struct {
	Anchor                            string
	Epoch, ClockDomain, Configuration [16]byte
	Kind                              uint8
}
type HandshakeObservationCut struct {
	Available bool
	Rows      int
	Sequence  uint64
	At        time.Duration
	Full      bool
}
type HandshakeObservationTransition struct {
	Sequence     uint64
	Full         bool
	Count        int
	Lower, Upper time.Duration
	Done, Valid  bool
}

// All original local snapshot fields are copied verbatim. PastOnly must be
// consulted: a sealed historical prefix is never coverage of current time.
// No UTC, benign-load, daily K5 or release decision is derived from this DTO.
type HandshakeObservationSnapshot struct {
	Available, AttributionBound bool
	SchemaVersion               uint32
	Status, Reason              string
	Source                      HandshakeObservationSource
	Bounds                      HandshakeObservationBounds
	CoverageStart               time.Duration
	Cut                         HandshakeObservationCut
	Rows                        []HandshakeObservationTransition
	PastOnly                    bool
}

type HandshakeObservation struct {
	service  *Service
	gate     *inboundHandshakes
	key      HandshakeObservationKey
	identity domain.PeerIdentity
}

func (o *HandshakeObservation) Key() HandshakeObservationKey {
	if o == nil {
		return HandshakeObservationKey{}
	}
	return o.key
}
func (o *HandshakeObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}
func (o *HandshakeObservation) Snapshot() HandshakeObservationSnapshot {
	result := HandshakeObservationSnapshot{SchemaVersion: 1, Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}
	if o == nil {
		return result
	}
	if o.service == nil || o.gate == nil || o.service.inboundHandshakes != o.gate {
		result.Status, result.Reason = "UNKNOWN", "handshake_gate_unavailable"
		return result
	}
	v := o.gate.fullSnapshot()
	result.Available = o.gate.fullObservation != nil
	result.Status, result.Reason, result.PastOnly = v.status, v.reason, v.pastOnly
	result.Source = HandshakeObservationSource{Anchor: v.key.anchor, Epoch: v.key.epoch, ClockDomain: v.key.clockDomain, Configuration: v.key.configuration, Kind: uint8(v.key.kind)}
	result.Bounds = HandshakeObservationBounds{Node: v.bounds.node, Loopback: v.bounds.loopback, PerSource: v.bounds.perSource}
	result.CoverageStart = v.coverageStart
	result.Cut = HandshakeObservationCut{Available: v.cut.available, Rows: v.cut.rows, Sequence: v.cut.sequence, At: v.cut.at, Full: v.cut.full}
	result.Rows = make([]HandshakeObservationTransition, len(v.rows))
	for i, row := range v.rows {
		result.Rows[i] = HandshakeObservationTransition{Sequence: row.sequence, Full: row.full, Count: row.count, Lower: row.lower, Upper: row.upper, Done: row.done, Valid: row.valid}
	}
	return result
}

type handshakeObservationConstruction struct {
	request HandshakeObservationRequest
	clock   func() (time.Duration, error)
	readout *HandshakeObservation
}

func validHandshakeObservationKey(key HandshakeObservationKey) bool {
	for _, ref := range []string{key.Anchor, key.Boot, key.ProcessInstance} {
		if !validObservationReference(ref) {
			return false
		}
	}
	return validObservationArtifact(key.ArtifactSHA256)
}
func prepareHandshakeObservation(request HandshakeObservationRequest, owner domain.PeerIdentity, clock func() (time.Duration, error)) *handshakeObservationConstruction {
	return &handshakeObservationConstruction{request: request, clock: clock, readout: &HandshakeObservation{key: request.Key, identity: owner}}
}

// Evaluated at the one original gate factory, before publication or any enter.
// Nil constructs the original ordinary gate and reads no observation clock.
func (c *handshakeObservationConstruction) newGate(s *Service, bounds handshakeBounds) *inboundHandshakes {
	if c == nil {
		return newInboundHandshakes(bounds)
	}
	key := c.request.Key
	gate := newObservedInboundHandshakes(bounds, k5SourceKey{anchor: key.Anchor, epoch: key.Epoch, clockDomain: key.ClockDomain, configuration: key.Configuration, kind: k5HandshakeNode}, c.request.Capacity, c.clock)
	c.readout.service, c.readout.gate = s, gate
	return gate
}

func NewServiceWithHandshakeObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, key HandshakeObservationKey, capacity int) (*Service, *HandshakeObservation, error) {
	request := HandshakeObservationRequest{Key: key, Capacity: capacity}
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{Handshake: &request})
	return s, readouts.Handshake, err
}
