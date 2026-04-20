package desktop

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"
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

	eventBus := ebus.New()

	nodeService := node.NewService(cfg.Node, id, eventBus)
	runtime := NewNodeRuntime(nodeService)

	client := service.NewDesktopClient(cfg.App, cfg.Node, id, nodeService)
	defer func() {
		if err := client.Close(); err != nil {
			log.Error().Err(err).Msg("chatlog close failed")
		} else {
			log.Info().Msg("chatlog closed")
		}
	}()

	fileBridge := service.NewFileTransferBridge(client)

	// NodeStatusMonitor aggregates network-layer state from ebus events.
	// It must subscribe BEFORE the node starts publishing — otherwise
	// early bootstrap events (initial peer connections, identity
	// discovery) are lost and the monitor starts with stale state.
	var statusMonitor *service.NodeStatusMonitor
	var router *service.DMRouter

	statusMonitor = service.NewNodeStatusMonitor(service.NodeStatusMonitorOpts{
		EventBus: eventBus,
		Client:   client,
		OnChanged: func() {
			if router != nil {
				router.NotifyStatusChanged()
			}
		},
	})
	statusMonitor.Start()

	router = service.NewDMRouter(client, fileBridge, eventBus, statusMonitor)

	// Metrics collector — samples node traffic every second, keeps 1 hour history.
	// Create it BEFORE runtime.Start and Seed its baseline from the current
	// (zero) counters, so the first Record after runtime.Start captures the
	// genuine bootstrap handshake traffic as a real delta instead of either
	// losing it (previous behavior: first Record skipped delta computation)
	// or spiking it as a single-second burst (alternative: delta = totals with
	// prev=0). The 1s gap between ticker.C firings is the natural granularity
	// of the chart; seeding here makes that first bar honest.
	metricsCollector := metrics.NewCollector(nodeService)
	metricsCollector.Seed()

	// Start the node AFTER all ebus subscribers are registered and the
	// metrics collector baseline is captured.
	runtime.Start(ctx)

	go metricsCollector.Run(ctx)

	// Build command table — single source of truth for all RPC commands.
	// Desktop UI calls this directly (no HTTP), HTTP server wraps it for external clients.
	cmdTable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(cmdTable, nodeService, client, router, metricsCollector)
	rpc.RegisterDesktopOverrides(cmdTable, client, nodeService)

	// Fail-fast on partial RPC auth (only username or only password set).
	if err := cfg.RPC.ValidateAuth(); err != nil {
		log.Fatal().Err(err).Msg("rpc config invalid")
	}

	// Start HTTP RPC server for external access (corsa-cli, third-party tools).
	// RPC is only started when authentication credentials are configured
	// (CORSA_RPC_USERNAME + CORSA_RPC_PASSWORD). Without auth, the server
	// is not created — prevents port conflicts when running multiple
	// instances and avoids exposing an unauthenticated control plane.
	if cfg.RPC.AuthEnabled() {
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
	} else {
		log.Info().Msg("rpc server disabled: CORSA_RPC_USERNAME and CORSA_RPC_PASSWORD not set")
	}

	// Desktop UI gets CommandTable directly — no HTTP round-trip needed.
	window := NewWindow(client, router, eventBus, cmdTable, runtime, prefs)
	return window.Run()
}
