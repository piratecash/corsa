package loadstand

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/piratecash/corsa/internal/core/domain"
)

const RouteWireCensusCommand = "loadstandRouteWireCensus"
const routeWireCensusWitnessLimit = 1024

type routeWireCensusWitness struct {
	Version     uint8
	Launch      LaunchNonce
	Fingerprint LaunchFingerprint
	Identity    domain.PeerIdentity
	Clock       RouteWireClockDomain
	Census      RouteWireCommitment
}

// NewRouteWireCensusWitness binds the registry digest to the launch without
// exposing the census rows or the final frame counters.
func NewRouteWireCensusWitness(slot *ChildFinalSlot, identity domain.PeerIdentity, plan PlannedRouteWireObservation, clock RouteWireClockDomain, readClock func() (RouteWireClockDomain, error), read func() ([]RouteWireEndpointKey, error)) (func() (json.RawMessage, error), error) {
	if slot == nil || read == nil || identity.IsZero() ||
		!clock.valid() || readClock == nil ||
		slot.binding.nonce == (LaunchNonce{}) || slot.binding.fingerprint == (LaunchFingerprint{}) ||
		plan.Window == (LaunchNonce{}) || !plan.Bounds.valid() || plan.MaxEndpoints <= 0 {
		return nil, ErrInvalidConfiguration
	}
	launch := slot.binding
	return func() (json.RawMessage, error) {
		if current, err := readClock(); err != nil || current != clock {
			return nil, ErrFinalAccountingUnavailable
		}
		rows, err := read()
		if err != nil || rows == nil || len(rows) > plan.MaxEndpoints {
			return nil, ErrFinalAccountingUnavailable
		}
		census, err := commitRouteWireWindow(RouteWireExpectation{Window: plan.Window, Bounds: plan.Bounds, Endpoints: rows}, identity)
		if err != nil {
			return nil, err
		}
		if current, err := readClock(); err != nil || current != clock {
			return nil, ErrFinalAccountingUnavailable
		}
		raw, err := json.Marshal(routeWireCensusWitness{Version: 1, Launch: launch.nonce, Fingerprint: launch.fingerprint, Identity: identity, Clock: clock, Census: census})
		if err != nil || len(raw) > routeWireCensusWitnessLimit {
			return nil, ErrFinalAccountingUnavailable
		}
		return raw, nil
	}, nil
}

// ReadRouteWireCensus queries the currently owned FullNode before its Stop.
// expectedClock is the caller's independently read kernel clock domain.
func (p *Process) ReadRouteWireCensus(ctx context.Context, binding CompletedStartBinding, plan PlannedRouteWireObservation, expectedClock RouteWireClockDomain) (RouteWireCommitment, error) {
	if p == nil || p.finalRole != MemberFull || p.rpc == nil || p.artifacts == nil || p.artifacts.binding != binding.launch || !plan.Bounds.valid() {
		return RouteWireCommitment{}, ErrFinalAccountingUnavailable
	}
	select {
	case <-p.done:
		return RouteWireCommitment{}, ErrFinalAccountingUnavailable
	default:
	}
	var raw json.RawMessage
	if err := p.rpc.Call(ctx, RouteWireCensusCommand, nil, &raw); err != nil {
		return RouteWireCommitment{}, err
	}
	select {
	case <-p.done:
		return RouteWireCommitment{}, ErrFinalAccountingUnavailable
	default:
	}
	if err := ctx.Err(); err != nil {
		return RouteWireCommitment{}, err
	}
	return decodeRouteWireCensusWitness(raw, binding, plan, expectedClock)
}

func decodeRouteWireCensusWitness(raw json.RawMessage, binding CompletedStartBinding, plan PlannedRouteWireObservation, expectedClock RouteWireClockDomain) (RouteWireCommitment, error) {
	var w routeWireCensusWitness
	if len(raw) > routeWireCensusWitnessLimit || !canonicalLinuxJSON(raw, reflect.TypeFor[routeWireCensusWitness](), false) ||
		json.Unmarshal(raw, &w) != nil || w.Version != 1 || w.Launch != binding.launch.nonce || w.Fingerprint != binding.launch.fingerprint ||
		w.Identity != binding.RuntimeBinding().Identity || !expectedClock.valid() || w.Clock != expectedClock || !plan.Bounds.valid() || !acceptsRouteWireCommitment(&plan, w.Census) {
		return RouteWireCommitment{}, ErrFinalReportInvalid
	}
	return w.Census, nil
}
