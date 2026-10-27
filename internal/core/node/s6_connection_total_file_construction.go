package node

import (
	"errors"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"time"
)

// Directory is borrowed through Close; callers exclude own writes during walks.
// All caps/session/refs are caller-supplied, not deployment/UTC/load attribution.
type ConnectionTotalFileObservationRequest struct {
	Key                     ConnectionTotalObservationKey
	Capacity, ChunkCapacity int
	Session                 [16]byte
	Directory               *secretfile.Dir
	Limits                  connbudget.TotalFileObservationLimits
}
type ConnectionTotalFileObservationResult struct {
	connbudget.TotalFileObservationResult
}

// Thin same-Service identity wrapper; the actual consumer/issuer/Close fence
// remain in connbudget. No proof, live installer or second Budget is exposed.
type ConnectionTotalFileObservation struct {
	service         *Service
	budget          *connbudget.Budget
	source          *connbudget.TotalFileObservation
	key             ConnectionTotalObservationKey
	identity        domain.PeerIdentity
	constructionErr error
}

func (o *ConnectionTotalFileObservation) Key() ConnectionTotalObservationKey {
	if o == nil {
		return ConnectionTotalObservationKey{}
	}
	return o.key
}
func (o *ConnectionTotalFileObservation) Identity() domain.PeerIdentity {
	if o == nil {
		return domain.PeerIdentity{}
	}
	return o.identity
}
func (o *ConnectionTotalFileObservation) bound() bool {
	return o != nil && o.service != nil && o.budget != nil && o.source != nil && o.service.connBudget == o.budget
}
func (o *ConnectionTotalFileObservation) Status() ConnectionTotalFileObservationResult {
	if o == nil {
		return ConnectionTotalFileObservationResult{connbudget.TotalFileObservationResult{SchemaVersion: 2, Status: "INACTIVE", Reason: "source_unavailable", PastOnly: true}}
	}
	result := ConnectionTotalFileObservationResult{o.source.Status()}
	if !o.bound() {
		result.Available = false
		result.Status, result.Reason = "UNKNOWN", "connection_budget_unavailable"
	}
	return result
}
func (o *ConnectionTotalFileObservation) Err() error {
	if o == nil {
		return nil
	}
	if o.source == nil {
		return o.constructionErr
	}
	return o.source.Err()
}
func (o *ConnectionTotalFileObservation) Capture() (ConnectionTotalFileObservationResult, error) {
	if o == nil {
		return o.Status(), nil
	}
	if !o.bound() {
		err := o.Err()
		if err == nil {
			err = errors.New("connection-total file source unavailable")
		}
		return o.Status(), err
	}
	result, err := o.source.Capture()
	return ConnectionTotalFileObservationResult{result}, err
}

// Explicit unstarted cleanup; also idempotently called on Run return/unwind.
// Does not close borrowed Dir or stop original admission. No hard FS deadline.
func (o *ConnectionTotalFileObservation) Close() error {
	if o == nil {
		return nil
	}
	return o.source.Close()
}
func (s *Service) CloseConnectionTotalFileObservation() error {
	if s == nil {
		return nil
	}
	return s.connectionTotalFileObservation.Close()
}

type connectionTotalFileObservationConstruction struct {
	request ConnectionTotalFileObservationRequest
	clock   func() (time.Duration, error)
	readout *ConnectionTotalFileObservation
	legacy  *ConnectionTotalObservation
}

func prepareConnectionTotalFileObservation(request ConnectionTotalFileObservationRequest, owner domain.PeerIdentity, clock func() (time.Duration, error)) *connectionTotalFileObservationConstruction {
	return &connectionTotalFileObservationConstruction{request: request, clock: clock, readout: &ConnectionTotalFileObservation{key: request.Key, identity: owner}, legacy: &ConnectionTotalObservation{key: request.Key, identity: owner}}
}
func (c *connectionTotalFileObservationConstruction) newFileBudget(s *Service, cfg connbudget.Config) (*connbudget.Budget, error) {
	r := c.request
	k := r.Key
	budget, source, err := connbudget.NewWithTotalFileObservation(cfg, connbudget.TotalObservationKey{Anchor: k.Anchor, Epoch: k.Epoch, ClockDomain: k.ClockDomain, Configuration: k.Configuration}, r.Capacity, r.ChunkCapacity, c.clock, r.Directory, r.Limits, r.Session)
	c.readout.service, c.readout.budget, c.readout.source, c.readout.constructionErr = s, budget, source, err
	c.legacy.service, c.legacy.budget, c.legacy.source = s, budget, source.LegacyObservation()
	s.connectionTotalFileObservation = c.readout
	return budget, err
}
func NewServiceWithConnectionTotalFileObservation(cfg config.Node, id *identity.Identity, bus *ebus.Bus, request ConnectionTotalFileObservationRequest) (*Service, *ConnectionTotalFileObservation, error) {
	s, readouts, err := NewServiceWithObservations(cfg, id, bus, ObservationOptions{ConnectionTotalFile: &request})
	return s, readouts.ConnectionTotalFile, err
}
