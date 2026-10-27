package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// These are bounded unverified references; identity is derived from the real
// Service. Zero tokens and capacity preserve the existing source semantics.
type ConnectionTotalObservationKey struct {
	Anchor, ArtifactSHA256, Boot, ProcessInstance string
	Epoch, ClockDomain, Configuration             [16]byte
}
type ConnectionTotalObservationRequest struct {
	Key      ConnectionTotalObservationKey
	Capacity int
}
type ConnectionTotalObservationSnapshot struct {
	Available, AttributionBound bool
	SchemaVersion               uint32
	connbudget.TotalObservationSnapshot
}
type ConnectionTotalObservation struct {
	service  *Service
	budget   *connbudget.Budget
	source   *connbudget.TotalObservation
	key      ConnectionTotalObservationKey
	identity domain.PeerIdentity
}

func (o *ConnectionTotalObservation) Key() ConnectionTotalObservationKey {
	if o == nil {
		return ConnectionTotalObservationKey{}
	}
	return o.key
}
func (o *ConnectionTotalObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}
func (o *ConnectionTotalObservation) Snapshot() ConnectionTotalObservationSnapshot {
	result := ConnectionTotalObservationSnapshot{SchemaVersion: 1, TotalObservationSnapshot: connbudget.TotalObservationSnapshot{Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}}
	if o == nil {
		return result
	}
	if o.service == nil || o.budget == nil || o.source == nil || o.service.connBudget != o.budget {
		result.Status, result.Reason = "UNKNOWN", "connection_budget_unavailable"
		return result
	}
	result.Available = true
	result.TotalObservationSnapshot = o.source.Snapshot()
	return result
}

type connectionTotalObservationConstruction struct {
	request ConnectionTotalObservationRequest
	clock   func() (time.Duration, error)
	readout *ConnectionTotalObservation
}

func validConnectionTotalObservationKey(key ConnectionTotalObservationKey) bool {
	for _, ref := range []string{key.Anchor, key.Boot, key.ProcessInstance} {
		if !validObservationReference(ref) {
			return false
		}
	}
	return validObservationArtifact(key.ArtifactSHA256)
}
func prepareConnectionTotalObservation(request ConnectionTotalObservationRequest, owner domain.PeerIdentity, clock func() (time.Duration, error)) *connectionTotalObservationConstruction {
	return &connectionTotalObservationConstruction{request: request, clock: clock, readout: &ConnectionTotalObservation{key: request.Key, identity: owner}}
}

// This is the one original factory point, before CM receives the budget.
// The nil path returns ordinary New without extra duration reads or allocation.
func (c *connectionTotalObservationConstruction) newBudget(s *Service, cfg connbudget.Config) (*connbudget.Budget, error) {
	if c == nil {
		return connbudget.New(cfg)
	}
	key := c.request.Key
	budget, source, err := connbudget.NewWithTotalObservation(cfg, connbudget.TotalObservationKey{Anchor: key.Anchor, Epoch: key.Epoch, ClockDomain: key.ClockDomain, Configuration: key.Configuration}, c.request.Capacity, c.clock)
	c.readout.service, c.readout.budget, c.readout.source = s, budget, source
	return budget, err
}
func NewServiceWithConnectionTotalObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, key ConnectionTotalObservationKey, capacity int) (*Service, *ConnectionTotalObservation, error) {
	request := ConnectionTotalObservationRequest{Key: key, Capacity: capacity}
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{ConnectionTotal: &request})
	return s, readouts.ConnectionTotal, err
}
