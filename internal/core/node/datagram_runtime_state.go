package node

import (
	"context"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// This dependency is instance-owned during construction, never a global hook or
// a public configuration override. Production always uses the real constructor.
type datagramConstructor interface {
	Build(*Service, *datagram.Metrics) (*datagramLayer, error)
}
type productionDatagramConstructor struct{}

func (productionDatagramConstructor) Build(s *Service, metrics *datagram.Metrics) (*datagramLayer, error) {
	return newDatagramLayer(s, metrics)
}

type datagramStartupObservation struct {
	configured bool
	state      domain.DatagramRuntimeStateKind
}

func observeDatagramConstruction(requested bool, layer *datagramLayer, err error) datagramStartupObservation {
	out := datagramStartupObservation{configured: requested, state: domain.DatagramStateUnknown}
	switch {
	case requested && err != nil:
		out.state = domain.DatagramStateInitializationFailed
	case requested && layer != nil && err == nil:
		out.state = domain.DatagramStateInitialized
	case !requested && layer == nil && err == nil:
		out.state = domain.DatagramStateConfiguredOff
	}
	return out
}

// DatagramRuntimeState reports constructor facts even before Run. Callers must
// independently enforce member ownership/readiness. No mutable lifecycle state
// or domain mutex is read; startup observation/identity/start are immutable.
func (s *Service) DatagramRuntimeState(ctx context.Context) (domain.DatagramRuntimeState, error) {
	if err := ctx.Err(); err != nil {
		return domain.DatagramRuntimeState{}, err
	}
	if s == nil || s.identity == nil || s.datagramStartup.state == "" {
		return domain.DatagramRuntimeState{}, domain.ErrDatagramStateUnavailable
	}
	observed := s.datagramStartup
	layer := s.datagramLayer()
	if observed.state == domain.DatagramStateUnknown || (observed.state == domain.DatagramStateInitialized && layer == nil) || (observed.state != domain.DatagramStateInitialized && layer != nil) {
		return domain.DatagramRuntimeState{}, domain.ErrDatagramStateUnavailable
	}
	out := domain.DatagramRuntimeState{Version: 1, Identity: domain.PeerIdentityFromWire(s.identity.Address), StartedAt: s.startedAt, ReadAt: time.Now().UTC(), ConfiguredEnabled: observed.configured, State: observed.state}
	if err := out.Validate(); err != nil {
		return domain.DatagramRuntimeState{}, err
	}
	return out, ctx.Err()
}
