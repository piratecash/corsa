//go:build linux && d1_o1a_native_responder

package main

import (
	"context"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"time"
)

// Sole candidate-only tagged caller. No environment/config/RPC role selector.
func newRouteWireFullNode(cfg config.Node, id *identity.Identity, bus *ebus.Bus, maxEndpoints int, window [32]byte, seedRoutes int, dialRole domain.RouteWireDialRole) (*node.Service, *node.RegisteredProvenV2Observation, context.CancelFunc, func([]node.RouteWireCensusEndpoint, time.Duration, time.Duration) error, error) {
	s, c, readout, cleanup, err := node.NewD1NativeFullResponderWithRouteWireCensus(cfg, id, bus, maxEndpoints, window, seedRoutes, dialRole)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	guard := func(rows []node.RouteWireCensusEndpoint, begin, end time.Duration) error {
		return readout.GuardCurrentForService(s, rows, begin, end)
	}
	return s, c, cleanup, guard, nil
}
