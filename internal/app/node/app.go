package node

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
)

type App struct {
	cfg     config.Config
	service *node.Service
}

func New() *App {
	cfg := config.Default()
	id, err := identity.LoadOrCreate(cfg.Node.IdentityPath)
	if err != nil {
		panic(err)
	}
	return &App{
		cfg:     cfg,
		service: node.NewService(cfg.Node, id, ebus.New()),
	}
}

func (a *App) Run(ctx context.Context) error {
	log.Info().
		Str("version", config.CorsaVersion).
		Str("wire", config.CorsaWireVersion).
		Str("listen", a.service.ListenAddress()).
		Msg("starting node")

	// Metrics collector — samples node traffic every second, keeps 1 hour history.
	// Seed the baseline from current totals before Run so the first Record
	// produces a real delta instead of losing bootstrap traffic (see
	// metrics.Collector.Seed docstring for full rationale).
	metricsCollector := metrics.NewCollector(a.service)
	metricsCollector.Seed()
	go metricsCollector.Run(ctx)

	// Build command table — node-only mode: no chatlog, no dm_router.
	// Pass nil for chatlog and dmRouter — those commands are registered as unavailable (503).
	cmdTable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(cmdTable, a.service, nil, nil, metricsCollector)

	// Fail-fast on partial RPC auth (only username or only password set).
	if err := a.cfg.RPC.ValidateAuth(); err != nil {
		log.Fatal().Err(err).Msg("rpc config invalid")
	}

	// Start HTTP RPC server for external access (corsa-cli).
	// RPC is only started when authentication credentials are configured
	// (CORSA_RPC_USERNAME + CORSA_RPC_PASSWORD). Without auth, the server
	// is not created — prevents port conflicts when running multiple
	// instances and avoids exposing an unauthenticated control plane.
	if a.cfg.RPC.AuthEnabled() {
		rpcServer, err := rpc.NewServer(a.cfg.RPC, cmdTable, a.service)
		if err != nil {
			log.Fatal().Err(err).Msg("rpc server config invalid")
		}

		if err := rpcServer.StartAsync(); err != nil {
			log.Error().Err(err).Msg("rpc server failed to start")
		}
		defer func() {
			if err := rpcServer.Shutdown(); err != nil {
				log.Error().Err(err).Msg("rpc server shutdown failed")
			}
		}()
	} else {
		log.Info().Msg("rpc server disabled: CORSA_RPC_USERNAME and CORSA_RPC_PASSWORD not set")
	}

	a.service.PrimeBootstrapPeers()
	return a.service.Run(ctx)
}
