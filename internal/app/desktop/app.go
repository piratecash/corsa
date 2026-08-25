package desktop

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/debugserver"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

func Run() error {
	cfg := config.Default()

	// Configuration is checked FIRST, before anything is opened, started or
	// cleaned up. This check used to sit next to the RPC server, after the
	// shared database was open and the node was running, and it failed through
	// log.Fatal — which is os.Exit: no cancelNode, no drains, no
	// Database.Close, none of the deferred cleanup this function is built
	// around. A half-written pair of credentials is a startup mistake and
	// belongs here, where returning an error costs nothing and nothing has
	// happened yet — not even the staging sweep below, which deletes files.
	if err := cfg.RPC.ValidateAuth(); err != nil {
		return fmt.Errorf("rpc config invalid: %w", err)
	}

	// cancelNode stops the node service (and every ctx-bound worker:
	// metrics collector, resource sampler, status notifier) on the
	// UI-driven shutdown path — see window.SetShutdown below.
	ctx, cancelNode := context.WithCancel(context.Background())

	// uiOwnsShutdown hands every resource below to the UI's own shutdown hook.
	//
	// On desktop app.Main never returns — the UI goroutine exits the process —
	// so the deferred cleanup here is the path for failures BEFORE the window
	// runs. On Android app.Main returns as soon as the Activity is up, while
	// the Activity and the UI goroutine keep running and keep using the node,
	// the bus and the database. Letting these defers fire there closed the
	// state database out from under a live UI: everything after it got
	// "sql: database is closed".
	uiOwnsShutdown := false
	defer func() {
		if uiOwnsShutdown {
			return
		}
		cancelNode()
	}()

	// Mobile runs as a light client, always: config.Default falls back
	// to NodeTypeFull (there is no practical way to set CORSA_NODE_TYPE
	// on Android), which would advertise the "relay" service and open
	// the inbound listener — transit traffic burns battery/data, and the
	// advertised port is unreachable behind mobile NAT anyway. Client
	// mode drops "relay" from ServiceList and disables the listener via
	// EffectiveListenerEnabled.
	if isAndroid {
		cfg.Node.Type = config.NodeTypeClient
	}

	// Wipe attachment staging copies from previous runs before any UI
	// (and thus any new pick) exists — the one moment no draft or
	// failed-send entry can reference them. See file_attach_stream.go.
	cleanupAttachTmp()

	// Sweep console overflow directories orphaned by crashed processes
	// (console_output.go) — same moment, same reasoning: no console window
	// exists yet, so nothing can reference them.
	cleanupOrphanedConsoleOverflow(time.Now())

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

	// Opt-in pprof profiling server (CORSA_PPROF_ADDR). Off by default;
	// loopback-only when set. See internal/core/debugserver. Empty addr
	// is a no-op (nil error); a non-empty addr that fails to start
	// (invalid / non-loopback / port in use) is fatal — the operator
	// explicitly asked for profiling, so silently continuing without it
	// would leave them debugging against a server that never came up.
	pprofShutdown, err := debugserver.Start(cfg.Node.PprofAddr)
	if err != nil {
		return fmt.Errorf("pprof debug server (CORSA_PPROF_ADDR=%q): %w", cfg.Node.PprofAddr, err)
	}
	defer func() {
		if uiOwnsShutdown {
			return
		}
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pprofShutdown(sctx)
	}()

	// The shared state database opens before any service exists: an
	// unreadable, foreign or unmigratable file must stop the process here,
	// while nothing has started writing, rather than surface later as
	// messages that silently fail to persist.
	database, err := storage.Open(ctx, storage.Config{
		ExplicitPath:  cfg.Node.StateDBPath,
		DataDir:       cfg.Node.EffectiveDataDir(),
		ListenAddress: domain.ListenAddress(cfg.Node.ListenAddress),
		Owner:         domain.PeerIdentityFromWire(id.Address),
		Catalog:       migrations.Catalog(),
	})
	if err != nil {
		return fmt.Errorf("open state database: %w", err)
	}
	// The deferred cleanup covers the paths that never reach the UI's shutdown
	// hook — a failure while building the app. Once the window takes over,
	// uiOwnsShutdown alone decides, and it is written on THIS goroutine before
	// the window starts: a second flag set by the hook would be read here
	// while the UI goroutine writes it, which on Android is a real race,
	// because app.Main returns while that goroutine keeps running.
	defer func() {
		if uiOwnsShutdown {
			return
		}
		if err := database.Close(); err != nil {
			log.Error().Err(err).Msg("state database close failed")
		}
	}()

	nodeService := node.NewService(cfg.Node, id, eventBus)
	runtime := NewNodeRuntime(nodeService)

	client := service.NewDesktopClient(cfg.App, cfg.Node, id, nodeService, database)
	if err := client.BackfillEstablished(ctx, time.Now().UTC()); err != nil {
		log.Warn().Err(err).Msg("chatlog established backfill failed")
	}

	// Reactions a peer states arrive on the datagram plane and land in the
	// chatlog through this door. Registered before the node starts, for the
	// same reason the ebus subscribers below are.
	client.RegisterConversationControl(eventBus)

	fileBridge := service.NewFileTransferBridge(client)

	// NodeStatusMonitor aggregates network-layer state from ebus events.
	// It must subscribe BEFORE the node starts publishing — otherwise
	// early bootstrap events (initial peer connections, identity
	// discovery) are lost and the monitor starts with stale state.
	var statusMonitor *service.NodeStatusMonitor
	var router *service.DMRouter

	// Coalesce status-change notifications: NotifyStatusChanged deep-copies
	// the whole NodeStatus, so on a large mesh where peer counts / health /
	// traffic change continuously, forwarding every monitor change straight
	// through would deep-copy on every event and stall the UI. The coalescer
	// collapses a burst into at most one rebuild per window. Signal() is
	// cheap/non-blocking; the actual NotifyStatusChanged runs on the
	// coalescer's Run goroutine (started below).
	statusNotifier := service.NewStatusNotifyCoalescer(0, func() {
		if router != nil {
			router.NotifyStatusChanged()
		}
	})

	statusMonitor = service.NewNodeStatusMonitor(service.NodeStatusMonitorOpts{
		EventBus:  eventBus,
		Client:    client,
		OnChanged: statusNotifier.Signal,
		// Single-domain mutations (resource sample, traffic batch, route /
		// identity / aggregate change) take the lightweight path: patch just
		// the changed field on the cached snapshot instead of deep-copying
		// the whole NodeStatus. Cheap and bounded, so it runs inline on the
		// emitting goroutine rather than through the coalescer.
		OnPartialChanged: func(d service.NodeStatusDomain) {
			if router != nil {
				router.NotifyStatusDomainChanged(d)
			}
		},
	})
	statusMonitor.Start()

	router = service.NewDMRouter(client, fileBridge, eventBus, statusMonitor)
	if isAndroid {
		// Phone layout: come up on the contact list. Without this the
		// router's startup would open the first conversation (and clear
		// its unread badge) before the user has seen the list — the UI's
		// own compact-mode guard only covers UI-initiated selection.
		router.SetStartupAutoSelect(false)
	}

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

	// Resource sampler — refreshes node memory + uptime into
	// service.NodeStatus once a second so the Info tab ticks live.
	// There is no ebus event for resource usage, so this dedicated
	// ticker is the resource-data analogue of the ebus deltas that keep
	// the other status fields fresh. Runs for the app lifetime.
	go statusMonitor.RunResourceSampler(ctx)

	// Drive the coalesced status-notify loop for the app lifetime. Bounds the
	// NodeStatus deep-copy rate under a status-event storm (see
	// service.StatusNotifyCoalescer).
	go statusNotifier.Run(ctx)

	// Build command table — single source of truth for all RPC commands.
	// Desktop UI calls this directly (no HTTP), HTTP server wraps it for external clients.
	cmdTable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(cmdTable, nodeService, client, router, metricsCollector)
	rpc.RegisterDesktopOverrides(cmdTable, client, nodeService)

	// Start HTTP RPC server for external access (corsa-cli, third-party tools).
	// RPC is only started when authentication credentials are configured
	// (CORSA_RPC_USERNAME + CORSA_RPC_PASSWORD). Without auth, the server
	// is not created — prevents port conflicts when running multiple
	// instances and avoids exposing an unauthenticated control plane.
	//
	// Android never starts it, credentials or not: there is no practical
	// way to hand env credentials to an Android app process, no CLI to
	// serve, and an extra listening socket in a mobile app is pure attack
	// surface.
	// stopRPC shuts the RPC server down exactly once and reports whether
	// that shutdown completed cleanly. It exists as a named func because
	// the UI-driven exit terminates the process via os.Exit, which skips
	// the defer below — without an explicit call in the shutdown
	// sequence the server would keep accepting chatlog commands right
	// through the drain and past the router gates.
	stopRPC := func() bool { return true }
	if isAndroid {
		log.Info().Msg("rpc server disabled on android")
	} else if cfg.RPC.AuthEnabled() {
		rpcServer, err := rpc.NewServer(cfg.RPC, cmdTable, nodeService)
		if err != nil {
			// Returned, not log.Fatal: os.Exit here would skip cancelNode,
			// the database close and every other deferred cleanup, with the
			// node already running.
			return fmt.Errorf("rpc server config invalid: %w", err)
		}

		if err := rpcServer.StartAsync(); err != nil {
			log.Error().Err(err).Msg("rpc server failed to start")
		}
		var rpcOnce sync.Once
		rpcStopped := true
		stopRPC = func() bool {
			rpcOnce.Do(func() {
				// Bounded: plain Shutdown waits for active connections
				// with no deadline - a keep-alive client would block
				// application exit forever. A timeout error means an
				// in-flight handler may STILL be running (fasthttp does
				// not abort handlers) — the caller must treat that as
				// an unclean stage and keep the chatlog open.
				if err := rpcServer.ShutdownWithTimeout(5 * time.Second); err != nil {
					log.Error().Err(err).Msg("rpc server shutdown failed")
					rpcStopped = false
				}
			})
			return rpcStopped
		}
		defer func() {
			if uiOwnsShutdown {
				return
			}
			stopRPC()
		}()
	} else {
		log.Info().Msg("rpc server disabled: CORSA_RPC_USERNAME and CORSA_RPC_PASSWORD not set")
	}

	// Desktop UI gets CommandTable directly — no HTTP round-trip needed.
	window, err := NewWindow(client, router, eventBus, cmdTable, runtime, prefs)
	if err != nil {
		return fmt.Errorf("initialize desktop window: %w", err)
	}
	// From here the UI owns the teardown, on both platforms but for two
	// different reasons. On desktop its exit paths (window closed) terminate
	// the process straight from the event loop, so the defers above never get
	// to run. On Android app.Main RETURNS as soon as the Activity is up, while
	// the Activity and the UI goroutine keep working — the defers would get to
	// run, far too early, which is what uiOwnsShutdown stops.
	//
	// So the data-integrity part lives here: stop the node first (no new
	// chatlog writes), then close the chatlog — sql.DB.Close waits out
	// in-flight queries, so sqlite finishes its WAL work instead of dying
	// inside os.Exit.
	window.SetShutdown(func() {
		// Shutdown ordering — producers stop before their consumers'
		// state is torn down, and everything settles before sqlite
		// closes:
		//
		//  1. RPC server — an external client must not inject new
		//     chatlog commands past the gates below (os.Exit skips the
		//     defer that normally stops it);
		//  2. UI-side goroutines (12s bound: their delete/complete
		//     operations carry 10s contexts);
		//  3. outbound sends, via the router's send-only gate
		//     (DrainSends) — while the node is STILL UP, so they can
		//     reach the wire;
		//  4. the router's producer loops (they publish terminal
		//     outcomes), then the node (publishers): cancel and wait
		//     out Service.Run + its background pool;
		//  5. the event bus, AFTER publishers stopped: control DMs
		//     (message_delete / ACKs) are not persisted in the chatlog,
		//     so an event published after handler teardown would be
		//     lost forever. Handler-spawned work still registers with
		//     the router gate, which is why the gate closes after the
		//     bus. Bounded externally — bus handlers can wait up to 10s
		//     per event and Bus.Shutdown itself has no timeout;
		//  6. the router's full gate + remaining in-flight work;
		//  7. the chatlog. Every wait is bounded; on timeout the DB is
		//     deliberately LEFT OPEN — see the stage below — because
		//     closing it under writers that are still running is what
		//     loses their terminal writes, while SQLite recovers an
		//     unclosed WAL crash-consistently on the next start.
		// clean starts from the RPC stage: a false return means a
		// handler may still be running inside fasthttp — it calls
		// straight into the CommandTable → chatlog.
		clean := stopRPC()
		if !window.drainUIOps(12 * time.Second) {
			log.Warn().Msg("ui goroutines did not finish within 12s")
			clean = false
		}
		if !router.DrainSends(12 * time.Second) {
			log.Warn().Msg("outbound sends did not drain within 12s")
			clean = false
		}
		// Producer loops stop BEFORE the bus drain: they publish
		// terminal delete/conversation outcomes, and an event published
		// after the bus is drained would be dropped.
		if !router.StopLoops(5 * time.Second) {
			log.Warn().Msg("router loops did not stop within 5s")
			clean = false
		}
		cancelNode()
		if !runtime.Wait(5 * time.Second) {
			log.Warn().Msg("node did not stop within 5s")
			clean = false
		}
		// A timed-out stage means its goroutines may still be running.
		// Tearing down their dependencies under them (bus, chatlog)
		// would turn a slow exit into lost control DMs and writes to a
		// closed database - so once any stage fails to join, further
		// DESTRUCTIVE teardown is skipped and the process exits with
		// the chatlog left open: sqlite WAL recovers that state
		// crash-consistently on the next start, which closing the DB
		// under active writers would not.
		if clean {
			busDone := make(chan struct{})
			go func() {
				eventBus.Shutdown()
				close(busDone)
			}()
			select {
			case <-busDone:
			case <-time.After(10 * time.Second):
				log.Warn().Msg("event bus did not drain within 10s")
				clean = false
			}
		}
		if clean && !router.ShutdownDrain(5*time.Second) {
			log.Warn().Msg("router did not drain within 5s")
			clean = false
		}
		// Either way the decision is made here; Run handed ownership over
		// before starting the window, so nothing revisits it.
		if clean {
			if err := database.Close(); err != nil {
				log.Error().Err(err).Msg("state database close failed on ui shutdown")
			} else {
				log.Info().Msg("state database closed on ui shutdown")
			}
		} else {
			log.Warn().Msg("shutdown incomplete: leaving state database open for crash-consistent WAL recovery on next start")
		}
	})
	// From here the UI owns the shutdown. On Android this is what keeps the
	// resources alive after app.Main returns; on desktop it changes nothing,
	// because app.Main does not return at all.
	uiOwnsShutdown = true
	return window.Run()
}
