//go:build linux && !d1_o1a_native_responder

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

// Exact stock construction; the ordinary entry has no enabled-role guard.
func newRouteWireFullNode(cfg config.Node, id *identity.Identity, bus *ebus.Bus, maxEndpoints int, window [32]byte, seedRoutes int, dialRole domain.RouteWireDialRole) (*node.Service, *node.RegisteredProvenV2Observation, context.CancelFunc, func([]node.RouteWireCensusEndpoint, time.Duration, time.Duration) error, error) {
	s, c, cleanup, err := node.NewLoadstandFullNodeWithRouteWireCensus(cfg, id, bus, maxEndpoints, window, seedRoutes, dialRole)
	return s, c, cleanup, nil, err
}
