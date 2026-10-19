package loadstand

import (
	"context"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// RuntimeBinding is supplied from the independently retained completed start
// receipt, never learned from the observation being validated.
type RuntimeBinding struct {
	Identity  domain.PeerIdentity
	StartedAt time.Time
}

func NewBoundProcessSource(client *RPCClient, clock runjournal.Clock, binding RuntimeBinding) (*ProcessSource, error) {
	source, err := NewProcessSourceWithClock(client, clock)
	if err != nil {
		return nil, err
	}
	if binding.Identity.IsZero() || binding.StartedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	source.binding = &binding
	return source, nil
}

func (s *ProcessSource) datagramRuntimeState(ctx context.Context) (domain.DatagramRuntimeState, error) {
	if s.binding == nil {
		return domain.DatagramRuntimeState{}, ErrFamilyUnavailable
	}
	fields, err := s.commandObject(ctx, "getDatagramRuntimeState", "version", "identity", "started_at", "read_at", "configured_enabled", "state")
	if err != nil {
		return domain.DatagramRuntimeState{}, err
	}
	version, err := value[uint8](fields, "version")
	if err != nil {
		return domain.DatagramRuntimeState{}, err
	}
	if version != 1 {
		return domain.DatagramRuntimeState{}, ErrFamilyUnavailable
	}
	if len(fields) != 6 {
		return domain.DatagramRuntimeState{}, ErrInvalidSample
	}
	var out domain.DatagramRuntimeState
	out.Version = version
	if out.Identity, err = value[domain.PeerIdentity](fields, "identity"); err != nil {
		return out, err
	}
	if out.StartedAt, err = timestamp(fields, "started_at"); err != nil {
		return out, err
	}
	if out.ReadAt, err = timestamp(fields, "read_at"); err != nil {
		return out, err
	}
	if out.ConfiguredEnabled, err = value[bool](fields, "configured_enabled"); err != nil {
		return out, err
	}
	if out.State, err = value[domain.DatagramRuntimeStateKind](fields, "state"); err != nil {
		return out, err
	}
	if err := out.Validate(); err != nil {
		return out, ErrInvalidSample
	}
	if out.Identity != s.binding.Identity || !out.StartedAt.Equal(s.binding.StartedAt) {
		return out, ErrInvalidSample
	}
	switch out.State {
	case domain.DatagramStateConfiguredOff, domain.DatagramStateInitialized:
		return out, nil
	default:
		return out, ErrFamilyUnavailable
	}
}
