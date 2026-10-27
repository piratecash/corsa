package node

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/debugserver"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
)

type App struct {
	advertisedConnectionObservation *node.AdvertisedConnectionObservation
	rawPinMembershipObservation     *node.RawPinMembershipObservation
	registeredProvenV2Observation   *node.RegisteredProvenV2Observation
	cfg                             config.Config
	service                         *node.Service
	pendingObservation              *node.PendingObservation
	terminalObservation             *node.TerminalObservation
	guardObservation                *node.GuardObservation
	handshakeObservation            *node.HandshakeObservation
	handshakeFileObservation        *node.HandshakeFileObservation
	connectionTotalObservation      *node.ConnectionTotalObservation
	connectionTotalFileObservation  *node.ConnectionTotalFileObservation
}

func New() *App {
	a, err := newApp(nil)
	if err != nil {
		panic(err)
	}
	return a
}

// NewWithAdvertisedConnectionObservation selects only the original typed
// source in this App. Instance remains an unverified label; no Run work is added.
func NewWithAdvertisedConnectionObservation(request node.AdvertisedConnectionObservationRequest) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{AdvertisedConnections: &request})
}

func (a *App) AdvertisedConnectionObservation() *node.AdvertisedConnectionObservation {
	if a == nil {
		return nil
	}
	return a.advertisedConnectionObservation
}

// NewWithRawPinMembershipObservation selects the original store's raw-memory
// source only. No configuration, persistence policy or Run resource is added.
func NewWithRawPinMembershipObservation(request node.RawPinMembershipObservationRequest) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{RawPinMembership: &request})
}

func (a *App) RawPinMembershipObservation() *node.RawPinMembershipObservation {
	if a == nil {
		return nil
	}
	return a.rawPinMembershipObservation
}

// NewWithRegisteredProvenV2Observation selects only the existing typed core
// source. No configuration, worker, resource ownership or Run policy is added.
func NewWithRegisteredProvenV2Observation(request node.RegisteredProvenV2ObservationRequest) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{RegisteredProvenV2: &request})
}
func (a *App) RegisteredProvenV2Observation() *node.RegisteredProvenV2Observation {
	if a == nil {
		return nil
	}
	return a.registeredProvenV2Observation
}

// NewWithPendingObservation explicitly composes the existing K12 source into
// the real headless Service. Key references remain unverified caller assertions.
// There is no configuration flag, listener, collector or policy activation.
func NewWithPendingObservation(key node.PendingObservationKey) (*App, error) {
	return newApp(&key)
}

// PendingObservation returns only the handle bound to this App's Service.
// Ordinary New returns nil; callers decide when to read, without a new worker.
func (a *App) PendingObservation() *node.PendingObservation {
	if a == nil {
		return nil
	}
	return a.pendingObservation
}

// NewWithTerminalObservation opts into only the restricted registry source.
// It does not activate a network policy, RPC, collector or duration clock.
func NewWithTerminalObservation(key node.TerminalObservationKey) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{Terminal: &key})
}

func (a *App) TerminalObservation() *node.TerminalObservation {
	if a == nil {
		return nil
	}
	return a.terminalObservation
}

// NewWithGuardObservation composes only the actual secure-store clock source.
// It does not activate a policy, collector, RPC or global clock replacement.
func NewWithGuardObservation(key node.GuardObservationKey) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{Guard: &key})
}

func (a *App) GuardObservation() *node.GuardObservation {
	if a == nil {
		return nil
	}
	return a.guardObservation
}

// NewWithHandshakeObservation acquires only the existing node-handshake source.
// Capacity is an explicit caller allocation request, not a deployment budget.
func NewWithHandshakeObservation(key node.HandshakeObservationKey, capacity int) (*App, error) {
	request := node.HandshakeObservationRequest{Key: key, Capacity: capacity}
	return newAppWithObservations(node.ObservationOptions{Handshake: &request})
}

func (a *App) HandshakeObservation() *node.HandshakeObservation {
	if a == nil {
		return nil
	}
	return a.handshakeObservation
}

// NewWithHandshakeFileObservation selects one explicit file-backed source.
// The caller keeps the supplied Dir open until the handle has closed; no
// path/config/worker/cadence is chosen here. Ordinary New remains off.
func NewWithHandshakeFileObservation(request node.HandshakeFileObservationRequest) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{HandshakeFile: &request})
}
func (a *App) HandshakeFileObservation() *node.HandshakeFileObservation {
	if a == nil {
		return nil
	}
	return a.handshakeFileObservation
}

// Explicit unstarted cleanup. This closes only observation resources, not the
// Service or the caller's borrowed directory. Run also calls it idempotently.
func (a *App) CloseHandshakeFileObservation() error {
	if a == nil {
		return nil
	}
	return a.handshakeFileObservation.Close()
}

// NewWithConnectionTotalObservation acquires the original shared budget source.
// It selects neither a connection limit nor a deployment observation capacity.
func NewWithConnectionTotalObservation(key node.ConnectionTotalObservationKey, capacity int) (*App, error) {
	request := node.ConnectionTotalObservationRequest{Key: key, Capacity: capacity}
	return newAppWithObservations(node.ObservationOptions{ConnectionTotal: &request})
}

func (a *App) ConnectionTotalObservation() *node.ConnectionTotalObservation {
	if a == nil {
		return nil
	}
	return a.connectionTotalObservation
}

// Explicit inactive typed acquisition. Directory remains caller-owned.
func NewWithConnectionTotalFileObservation(request node.ConnectionTotalFileObservationRequest) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{ConnectionTotalFile: &request})
}
func (a *App) ConnectionTotalFileObservation() *node.ConnectionTotalFileObservation {
	if a == nil {
		return nil
	}
	return a.connectionTotalFileObservation
}
func (a *App) CloseConnectionTotalFileObservation() error {
	if a == nil {
		return nil
	}
	return a.connectionTotalFileObservation.Close()
}

func newApp(key *node.PendingObservationKey) (*App, error) {
	return newAppWithObservations(node.ObservationOptions{Pending: key})
}

// One fixed construction envelope; no public combination-factory family.
func newAppWithObservations(options node.ObservationOptions) (*App, error) {
	// Freeze before config/identity I/O; only scalar values are retained.
	if options.AdvertisedConnections != nil {
		request := *options.AdvertisedConnections
		options.AdvertisedConnections = &request
	}
	// Freeze the scalar request before config/identity I/O.
	if options.RawPinMembership != nil {
		request := *options.RawPinMembership
		options.RawPinMembership = &request
	}
	// Freeze only the new value request before config/identity I/O. Nil adds
	// no observer work; the original App constructor/lifetime remains intact.
	if options.RegisteredProvenV2 != nil {
		request := *options.RegisteredProvenV2
		options.RegisteredProvenV2 = &request
	}
	cfg := config.Default()
	// Headless override: the shared config zero value keeps DMs enabled
	// for the desktop client, but a console node has no user reading
	// messages — DM acceptance is opt-in via CORSA_ACCEPT_DM. See the
	// Node.DisableDirectMessages doc-comment for the full behaviour matrix.
	//
	// Deliberately env-only, no CLI flag (operator decision, confirmed
	// repeatedly in review): corsa-node's argv contract is "unknown
	// process-manager arguments are ignored" (cmd/corsa-node scans argv
	// solely for --version), and every other runtime knob on this binary
	// is already CORSA_*-env-driven.
	cfg.Node.DisableDirectMessages = !config.AcceptDirectMessagesFromEnv()
	id, err := identity.LoadOrCreate(cfg.Node.IdentityPath)
	if err != nil {
		return nil, err
	}
	bus := ebus.New()
	service, readouts, err := node.NewServiceWithObservations(cfg.Node, id, bus, options)
	if err != nil {
		bus.Shutdown()
		return nil, err
	}
	return &App{
		cfg: cfg, service: service,
		advertisedConnectionObservation: readouts.AdvertisedConnections,
		rawPinMembershipObservation:     readouts.RawPinMembership,
		registeredProvenV2Observation:   readouts.RegisteredProvenV2,
		pendingObservation:              readouts.Pending,
		terminalObservation:             readouts.Terminal,
		guardObservation:                readouts.Guard,
		handshakeObservation:            readouts.Handshake,
		handshakeFileObservation:        readouts.HandshakeFile,
		connectionTotalObservation:      readouts.ConnectionTotal,
		connectionTotalFileObservation:  readouts.ConnectionTotalFile,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	// Arm before startup/RPC early returns; errors remain on observer status.
	defer func() { _ = a.CloseHandshakeFileObservation() }()
	defer func() { _ = a.CloseConnectionTotalFileObservation() }()
	log.Info().
		Str("version", config.CorsaVersion).
		Str("listen", a.service.ListenAddress()).
		Msg("starting node")

	// A node that must not start (an unsafe or lost pin store, an
	// unusable connection budget) stops here, before the pprof and RPC
	// listeners below: it is never reachable at all. The error names the
	// path and the fix for the operator reading this process's output.
	if err := a.service.StartupRefusal(); err != nil {
		return fmt.Errorf("node refuses to start: %w", err)
	}

	// Opt-in pprof profiling server (CORSA_PPROF_ADDR). Off by default;
	// loopback-only when set. See internal/core/debugserver. Empty addr
	// is a no-op (nil error); a non-empty addr that fails to start
	// (invalid / non-loopback / port in use) is fatal — the operator
	// explicitly asked for profiling, so silently continuing without it
	// would leave them debugging against a server that never came up.
	pprofShutdown, err := debugserver.Start(a.cfg.Node.PprofAddr)
	if err != nil {
		return fmt.Errorf("pprof debug server (CORSA_PPROF_ADDR=%q): %w", a.cfg.Node.PprofAddr, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pprofShutdown(ctx)
	}()

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
