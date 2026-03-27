package desktop

import (
	"context"

	"github.com/rs/zerolog/log"

	"corsa/internal/core/config"
	"corsa/internal/core/identity"
	"corsa/internal/core/metrics"
	"corsa/internal/core/node"
	"corsa/internal/core/rpc"
	"corsa/internal/core/service"
)

func Run() error {
	cfg := config.Default()
	ctx := context.Background()

	id, err := identity.LoadOrCreate(cfg.Node.IdentityPath)
	if err != nil {
		return err
	}

	log.Info().Str("address", id.Address).Str("path", cfg.Node.IdentityPath).Msg("desktop identity loaded")

	prefs, err := LoadPreferences(preferencePathForIdentity(cfg.Node.IdentityPath))
	if err != nil {
		return err
	}
	if prefs.Language != "" {
		cfg.App.Language = prefs.Language
	}

	nodeService := node.NewService(cfg.Node, id)
	runtime := NewNodeRuntime(nodeService)
	runtime.Start(ctx)

	client := service.NewDesktopClient(cfg.App, cfg.Node, id, nodeService)
	defer func() {
		if err := client.Close(); err != nil {
			log.Error().Err(err).Msg("chatlog close failed")
		} else {
			log.Info().Msg("chatlog closed")
		}
	}()

	router := service.NewDMRouter(client)

	// Metrics collector — samples node traffic every second, keeps 1 hour history.
	metricsCollector := metrics.NewCollector(nodeService)
	go metricsCollector.Run(ctx)

	// Build command table — single source of truth for all RPC commands.
	// Desktop UI calls this directly (no HTTP), HTTP server wraps it for external clients.
	cmdTable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(cmdTable, nodeService, client, router, metricsCollector)
	rpc.RegisterDesktopOverrides(cmdTable, client, nodeService)

	// Start HTTP RPC server for external access (corsa-cli, third-party tools)
	rpcServer, err := rpc.NewServer(cfg.RPC, cmdTable, nodeService)
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

	// Desktop UI gets CommandTable directly — no HTTP round-trip needed.
	window := NewWindow(client, router, cmdTable, runtime, prefs)
	return window.Run()
}
