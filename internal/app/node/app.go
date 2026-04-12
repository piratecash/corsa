package node

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
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
		service: node.NewService(cfg.Node, id),
	}
}

func (a *App) Run(ctx context.Context) error {
	log.Info().
		Str("version", config.CorsaVersion).
		Str("wire", config.CorsaWireVersion).
		Str("listen", a.service.ListenAddress()).
		Msg("starting node")

	// Metrics collector — samples node traffic every second, keeps 1 hour history.
	metricsCollector := metrics.NewCollector(a.service)
	go metricsCollector.Run(ctx)

	// Build command table — node-only mode: no chatlog, no dm_router.
	// Pass nil for chatlog and dmRouter — those commands are registered as unavailable (503).
	cmdTable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(cmdTable, a.service, nil, nil, metricsCollector)

	// Start HTTP RPC server for external access (corsa-cli)
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

	a.service.PrimeBootstrapPeers()
	return a.service.Run(ctx)
}
