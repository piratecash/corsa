package node

import (
	"context"
	"crypto/sha256"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Normal Runtime construction, still zero-origin. The cleanup belongs to the
// constructor's ORIGINAL Service and is used only before Run has started.
// No Service method or RPC/wire operation exposes background admission control.
func NewServiceForRuntime(cfg config.Node, id *identity.Identity, bus *ebus.Bus) (*Service, context.CancelFunc) {
	s := NewService(cfg, id, bus)
	return s, newUnstartedRuntimeCleanup(s, s.runCancel)
}

// NewLoadstandFullNodeWithRouteWireCensus attaches registry evidence to the
// original FullNode Service before it can publish connections. The limits
// cover at most one registration and one retirement per measured endpoint.
func NewLoadstandFullNodeWithRouteWireCensus(cfg config.Node, id *identity.Identity, bus *ebus.Bus, maxEndpoints int, window [32]byte, seedRoutes int, dialRole domain.RouteWireDialRole) (*Service, *RegisteredProvenV2Observation, context.CancelFunc, error) {
	if maxEndpoints <= 0 || maxEndpoints > k3RegisteredMaxOwners || maxEndpoints > k3RegisteredMaxHistory/2 || window == ([32]byte{}) ||
		seedRoutes < 0 || seedRoutes > domain.RouteWireFullMaxDestinations ||
		(seedRoutes > 0 || dialRole != "") && (!cfg.DisableDirectMessages || cfg.Type != config.NodeTypeFull) ||
		dialRole != "" && (len(cfg.BootstrapPeers) != 1 || dialRole != domain.RouteWireDialer && dialRole != domain.RouteWireListener) {
		return nil, nil, nil, errRouteWireIncomplete
	}
	digest := sha256.Sum256(window[:])
	s, census, err := NewServiceWithRegisteredProvenV2Observation(cfg, id, bus, RegisteredProvenV2ObservationRequest{
		OwnerCapacity: maxEndpoints, HistoryCapacity: 2 * maxEndpoints, Instance: [16]byte(digest[:16]), routeWireScope: window,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	cleanup := newUnstartedRuntimeCleanup(s, s.runCancel)
	// The manager reads this ceiling for bootstrap, refill and manual peer
	// admission. Both members still run the ordinary inbound listener.
	switch dialRole {
	case domain.RouteWireDialer:
		s.connManager.config.MaxSlotsFn = func() int { return 1 }
	case domain.RouteWireListener:
		s.connManager.config.MaxSlotsFn = func() int { return 0 }
	}
	if seedRoutes > 0 {
		if err := seedLoadstandRouteWireTable(s.routingTable, domain.PeerIdentityFromWire(id.Address), seedRoutes); err != nil {
			cleanup()
			s.WaitBackground()
			return nil, nil, nil, err
		}
	}
	return s, census, cleanup, nil
}

// No new lifecycle/gate: close the existing original admission before a caller
// waits and releases stores. Never invoke while Run is executing; Run must
// retain admission for teardown continuations until its own LAST defer.
func newUnstartedRuntimeCleanup(s *Service, originalCancel context.CancelFunc) context.CancelFunc {
	return func() {
		originalCancel()
		s.closeBackgroundAdmission()
	}
}
