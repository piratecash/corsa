package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	corsanode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// CommandInfo describes a console/RPC command exposed by the SDK runtime.
type CommandInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Usage       string `json:"usage,omitempty"`
}

// Runtime is a headless CORSA stack that can be embedded into other Go programs.
// It starts a node, exposes the in-process command layer, and supports bot flows.
type Runtime struct {
	cfg Config

	nodeService *corsanode.Service
	database    *storage.Database
	client      *service.DesktopClient
	router      *service.DMRouter
	eventBus    *ebus.Bus
	metrics     *metrics.Collector
	cmdTable    *rpc.CommandTable
	rpcServer   *rpc.Server

	// waitCh is closed when the runtime is fully down. It carries no value:
	// a channel with one buffered result served the FIRST caller and handed
	// every later one a zero error, so a shutdown failure was reported to one
	// waiter and hidden from the rest.
	waitCh chan struct{}

	// waitErr is that outcome, written before waitCh is closed and read only
	// after it — the close is the happens-before edge for every waiter.
	waitErr error

	// startDone closes once the one start attempt has a final outcome, so a
	// concurrent Start waits for it instead of reading a half-written one.
	startDone    chan struct{}
	startSettled sync.Once

	// finished guards the pair: the result is delivered by whichever path ends
	// the runtime — the node's goroutine, a failed Start, or a Close on a
	// runtime that was never started — and exactly one of them may close it.
	finished sync.Once

	// lifecycle guards the whole start/stop state machine. Start and Close
	// are public and may be called from different goroutines — including the
	// runtime's own node goroutine, which calls Close when Run returns — so
	// "started", "closed" and the cancel function they hand each other need
	// one owner rather than a pair of unordered sync.Once.
	lifecycle sync.Mutex
	started   bool
	// stopping is set by the first Close and never cleared: once a shutdown
	// has begun, Start and every public operation are refused for good, even
	// if the teardown itself has to be retried.
	stopping bool
	// closed means the teardown finished and the database was released. A
	// timed-out Close leaves it false so a later Close can finish the job.
	closed      bool
	startErr    error
	closeErr    error
	cancelNode  context.CancelFunc
	nodeStarted bool

	// progress records which teardown stages already finished, so a retried
	// Close only re-runs the ones that timed out.
	progress shutdownProgress

	// subscriptions is cancelled at the start of Close. A long-lived stream
	// such as SubscribeDirectMessages holds an operation lease until its own
	// context ends, and a caller who passed Background has no way to end it
	// — without this the drain below waits for a stream that is waiting for
	// the drain, and Close could never finish.
	subscriptions       context.Context
	cancelSubscriptions context.CancelFunc

	// commands is the context a command runs under when the caller supplies
	// none. It is cancelled when the operation drain begins, so a command that
	// can outlast the drain budget is told to stop rather than making Close
	// report an incomplete shutdown.
	commands       context.Context
	cancelCommands context.CancelFunc

	// operations counts the public SDK calls currently inside the stack.
	// Without it a SendDirectMessage entered just before Close would run
	// against a stopped node and a closing database — the router's own gate
	// covers its loops, not this package's API surface.
	operations sync.WaitGroup

	// nodeStopped is closed by the run goroutine once Service.Run returned.
	// It exists separately from waitCh because that goroutine calls Close
	// itself: waiting on a channel the caller closes after Close returns
	// would deadlock.
	nodeStopped chan struct{}
}

// shutdownProgress records which Close stages already completed.
//
// A retried Close must not simply repeat everything: some stages are not
// safely repeatable on faith — fasthttp's Shutdown in particular — and a stage
// that already joined has nothing left to wait for. Tracking them makes "call
// Close again once the stragglers finish" a real contract instead of an
// assumption about idempotency.
type shutdownProgress struct {
	// completed names the stages that joined, in the order they did. The
	// order IS the contract — writers before the things they write to — so it
	// is recorded rather than inferred from the booleans.
	completed []string

	rpc        bool
	operations bool
	sends      bool
	loops      bool
	node       bool
	bus        bool
	router     bool
	background bool
}

// EnsureIdentityFile creates an identity file at path if one does not exist
// yet. This is a convenience for examples and development — production bots
// should manage identity keys externally and supply them via NodeConfig.PrivateKey.
func EnsureIdentityFile(path string) error {
	if _, err := identity.Load(path); err == nil {
		return nil
	}
	id, err := identity.Generate()
	if err != nil {
		return fmt.Errorf("generate identity: %w", err)
	}
	if err := identity.Save(path, id); err != nil {
		return fmt.Errorf("save identity: %w", err)
	}
	return nil
}

// resolveIdentity determines the node identity using the SDK resolution
// order: PrivateKey string first, then existing file at IdentityPath.
// Auto-generation is intentionally not supported — each SDK consumer must
// supply its own identity explicitly.
func resolveIdentity(cfg Config) (*identity.Identity, error) {
	if cfg.Node.PrivateKey != "" {
		id, err := identity.FromPrivateKeyBase64(cfg.Node.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("identity from private key: %w", err)
		}
		return id, nil
	}

	normalized := normalizeConfig(cfg)
	if normalized.Node.IdentityPath != "" {
		id, err := identity.Load(normalized.Node.IdentityPath)
		if err != nil {
			return nil, fmt.Errorf("load identity from %s: %w (hint: set NodeConfig.PrivateKey to provide identity inline)", normalized.Node.IdentityPath, err)
		}
		return id, nil
	}

	return nil, errors.New("identity required: set NodeConfig.PrivateKey or provide existing identity file at NodeConfig.IdentityPath")
}

// abandonRuntime releases what New has already built when a later step fails.
//
// The event bus is the part that is easy to miss: by this point the status
// monitor holds a dozen subscriptions, each with a goroutine of its own, and
// they outlive the failed New — cancelling the subscription context stops the
// producers, not the bus. A caller retrying New with a corrected config would
// otherwise accumulate them, along with everything they keep referenced.
func abandonRuntime(eventBus *ebus.Bus, database *storage.Database, cancels ...context.CancelFunc) {
	for _, cancel := range cancels {
		cancel()
	}
	if !waitFunc(eventBus.Shutdown, busDrainTimeout) {
		log.Warn().Msg("sdk: event bus did not drain while abandoning a failed runtime")
	}
	if err := database.Close(); err != nil {
		log.Warn().Err(err).Msg("sdk: closing the state database of a failed runtime")
	}
}

// New creates a new SDK runtime from explicit Go configuration, using a
// background context for the work it does at construction time.
func New(cfg Config) (*Runtime, error) {
	return NewWithContext(context.Background(), cfg)
}

// NewWithContext is New with a caller-supplied context.
//
// Construction is not instantaneous: opening the state database runs an
// integrity check over the whole file, may wait for another process to release
// the write lock, and may apply migrations. On a large database that is
// seconds of work with no way out — an application that is already shutting
// down had no way to abandon it, because the context was created here.
//
// The context governs construction only. What it does NOT do is bound the
// runtime's life: the node runs under the context given to Start, and the
// runtime is released by Close.
func NewWithContext(ctx context.Context, cfg Config) (*Runtime, error) {
	internalCfg := cfg.internal()

	id, err := resolveIdentity(cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve identity: %w", err)
	}

	eventBus := ebus.New()
	subscriptions, cancelSubscriptions := context.WithCancel(context.Background())
	commands, cancelCommands := context.WithCancel(context.Background())

	// The state database opens before any service: a failure here must abort
	// New, not leave the caller with a runtime that cannot persist.
	database, err := storage.Open(ctx, storage.Config{
		ExplicitPath:  internalCfg.Node.StateDBPath,
		DataDir:       internalCfg.Node.EffectiveDataDir(),
		ListenAddress: domain.ListenAddress(internalCfg.Node.ListenAddress),
		Owner:         domain.PeerIdentityFromWire(id.Address),
		Catalog:       migrations.Catalog(),
	})
	if err != nil {
		cancelSubscriptions()
		cancelCommands()
		return nil, fmt.Errorf("open state database: %w", err)
	}

	nodeService := corsanode.NewService(internalCfg.Node, id, eventBus)
	client := service.NewDesktopClient(internalCfg.App, internalCfg.Node, id, nodeService, database)
	// Best-effort, exactly as on desktop: the backfill only seeds facts for
	// peers the user already messaged, and failing it degrades Sybil
	// classification rather than persistence.
	if err := client.BackfillEstablished(ctx, time.Now().UTC()); err != nil {
		// Best-effort applies to the backfill FAILING, not to the caller
		// walking away: a cancelled context means nobody is waiting for this
		// runtime, and degrading to a warning handed back a working one with
		// its database open.
		if ctx.Err() != nil {
			abandonRuntime(eventBus, database, cancelSubscriptions, cancelCommands)
			return nil, fmt.Errorf("chatlog established backfill: %w", err)
		}
		log.Warn().Err(err).Msg("sdk: chatlog established backfill failed")
	}
	// Reactions a peer states arrive on the datagram plane and land in the
	// chatlog through this door. Registered before the node starts.
	client.RegisterConversationControl(eventBus)

	fileBridge := service.NewFileTransferBridge(client)

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
		// Single-domain mutations take the lightweight path (patch just the
		// changed field) instead of a full NodeStatus deep copy.
		OnPartialChanged: func(d service.NodeStatusDomain) {
			if router != nil {
				router.NotifyStatusDomainChanged(d)
			}
		},
	})
	statusMonitor.Start()

	router = service.NewDMRouter(client, fileBridge, eventBus, statusMonitor)
	metricsCollector := metrics.NewCollector(nodeService)

	cmdTable := rpc.NewCommandTable()
	rpc.RegisterAllCommands(cmdTable, nodeService, client, router, metricsCollector)
	rpc.RegisterDesktopOverrides(cmdTable, client, nodeService)

	var rpcServer *rpc.Server
	if normalizeConfig(cfg).RPC.Enabled {
		rpcServer, err = rpc.NewServer(internalCfg.RPC, cmdTable, nodeService)
		if err != nil {
			abandonRuntime(eventBus, database, cancelSubscriptions, cancelCommands)
			return nil, fmt.Errorf("create rpc server: %w", err)
		}
	}

	return &Runtime{
		cfg:                 normalizeConfig(cfg),
		nodeService:         nodeService,
		database:            database,
		client:              client,
		router:              router,
		eventBus:            eventBus,
		metrics:             metricsCollector,
		cmdTable:            cmdTable,
		rpcServer:           rpcServer,
		waitCh:              make(chan struct{}),
		startDone:           make(chan struct{}),
		nodeStopped:         make(chan struct{}),
		subscriptions:       subscriptions,
		cancelSubscriptions: cancelSubscriptions,
		commands:            commands,
		cancelCommands:      cancelCommands,
	}, nil
}

// streamContext is the ONE context a long-lived stream runs under: it ends
// when either the caller cancels theirs or the runtime shuts down, and it is
// what every blocking point in the stream must watch.
//
// Two separate contexts were not enough. A consumer that stops reading leaves
// the stream's goroutine parked on its send, and a decrypt can be parked
// inside SQLite; watching only the caller's context there meant a shutdown had
// no way to release either, while both hold the operation lease Close waits
// for — a guaranteed timeout.
func (r *Runtime) streamContext(caller context.Context) (context.Context, context.CancelFunc) {
	stream, stop := context.WithCancel(r.subscriptions)
	go func() {
		defer stop()
		select {
		case <-caller.Done():
		case <-stream.Done():
		}
	}()
	return stream, stop
}

// beginOperation admits a public SDK call and registers it, so Close can wait
// for it. Callers that get true MUST call endOperation.
//
// This is the SDK's half of the shutdown contract: the router gates its own
// loops, but nothing stopped an embedder from calling SendDirectMessage or
// Execute while the node was stopping and the database was closing.
func (r *Runtime) beginOperation() bool {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	if r.stopping {
		return false
	}
	r.operations.Add(1)
	return true
}

func (r *Runtime) endOperation() { r.operations.Done() }

// errClosed is what a public operation returns once a shutdown has begun.
var errClosed = errors.New("sdk: runtime is closed")

// Config returns the normalized runtime configuration.
func (r *Runtime) Config() Config {
	return r.cfg
}

// Address returns the local identity address.
func (r *Runtime) Address() string {
	return r.client.Address().String()
}

// ListenAddress returns the configured local listener address.
func (r *Runtime) ListenAddress() string {
	return r.nodeService.ListenAddress()
}

// Commands returns all available in-process console commands.
func (r *Runtime) Commands() []CommandInfo {
	commands := r.cmdTable.Commands()
	out := make([]CommandInfo, 0, len(commands))
	for _, command := range commands {
		out = append(out, CommandInfo{
			Name:        command.Name,
			Description: command.Description,
			Category:    command.Category,
			Usage:       command.Usage,
		})
	}
	return out
}

// Start launches the node runtime in background mode.
//
// Returns an error if the runtime was already closed: a Close releases the
// state database and the event bus, and starting the node and router back on
// top of them would write into a closed handle.
//
// The whole body runs under the lifecycle lock. Releasing it earlier and only
// guarding the flags was not enough: a concurrent Close would then drain the
// event bus while router.Start was still subscribing to it.
func (r *Runtime) Start(ctx context.Context) error {
	r.lifecycle.Lock()

	if r.started {
		// An attempt is under way or over. Its outcome is not final until
		// startDone closes: the failing path releases the lock to run Close,
		// and a Start entering that window used to read the bare RPC error —
		// or errClosed, once that Close had set stopping — instead of the
		// outcome every other caller is given.
		r.lifecycle.Unlock()
		<-r.startDone
		r.lifecycle.Lock()
		defer r.lifecycle.Unlock()

		if r.startErr != nil {
			return r.startErr
		}
		if r.stopping {
			return errClosed
		}
		return nil
	}
	if r.stopping {
		r.lifecycle.Unlock()
		return errClosed
	}
	r.started = true

	// Own cancellation for everything started here, derived from the
	// caller's context. Close cancels it even when the caller never
	// cancels ctx, so the shutdown order holds in both directions.
	runCtx, cancel := context.WithCancel(ctx)
	r.cancelNode = cancel

	// Capture the current totals as delta baseline before the first
	// Record. Without Seed the first sample either hides bootstrap
	// traffic (old skip-on-first behavior) or reports the entire
	// pre-Seed cumulative as a single-second spike.
	r.metrics.Seed()
	go r.metrics.Run(runCtx)
	r.router.Start()
	r.nodeService.PrimeBootstrapPeers()

	if r.rpcServer != nil {
		if err := r.rpcServer.StartAsync(); err != nil {
			r.startErr = fmt.Errorf("start rpc server: %w", err)
			err := r.startErr
			// The lock is released BEFORE Close, which takes it itself.
			// The router is already running at this point, so Close still
			// has real work to do; the node never started, which
			// nodeStarted tells it so it does not wait for one.
			r.lifecycle.Unlock()

			// Close BEFORE the result is delivered: a caller that returns
			// from Wait is entitled to exit the process, and every write
			// still in flight would go with it.
			outcome := errors.Join(err, r.Close())

			// The caller of Start — and of Run, which returns what Start
			// returns — gets the same answer as a waiter. Returning only the
			// start error hid a shutdown that could not finish and left the
			// database open, which is the half a caller must act on.
			r.lifecycle.Lock()
			r.settleStartLocked(outcome)
			r.lifecycle.Unlock()

			r.finish(outcome)
			return outcome
		}
	}

	r.nodeStarted = true
	r.settleStartLocked(nil)
	go func() {
		err := r.nodeService.Run(runCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		// Signal before Close: Close waits on nodeStopped, and this
		// goroutine is the one that calls it on the ordinary path.
		close(r.nodeStopped)

		// The result reaches Wait only once Close has returned. Delivering it
		// first let the caller exit the process while the background writes,
		// the bus and router drains and the SQLite close were still running —
		// which is exactly what the SDK example does the moment Wait returns.
		r.finish(errors.Join(err, r.Close()))
	}()

	r.lifecycle.Unlock()
	return nil
}

// settleStartLocked records the outcome of the one start attempt and releases
// the Starts that arrived while it was still running. The caller holds
// r.lifecycle.
func (r *Runtime) settleStartLocked(err error) {
	r.startErr = err
	r.startSettled.Do(func() { close(r.startDone) })
}

// finish hands the run's outcome to Wait. It is the last thing either exit
// path does, so a caller waking from Wait knows the runtime is fully down.
func (r *Runtime) finish(err error) {
	r.finished.Do(func() {
		r.waitErr = err
		close(r.waitCh)
	})
}

// Wait blocks until the node runtime stops and the runtime has been closed.
// The error it returns carries both the run's own failure and any failure of
// that shutdown.
func (r *Runtime) Wait() error {
	<-r.waitCh
	return r.waitErr
}

// Run starts the runtime and blocks until it stops.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.Start(ctx); err != nil {
		return err
	}
	return r.Wait()
}

// Close shuts the runtime down in the one order that does not lose writes,
// and releases the state database last.
//
// Every stage above the database can still be issuing SQL: the RPC handlers
// call straight into the command table, the router's loops read and write the
// chatlog, the node's background jobs finish durable writes, and ebus
// subscribers run on their own goroutines. Closing SQLite while any of them is
// live turns a shutdown into "database is closed" errors and lost messages, so
// each is stopped and joined first.
//
// Every wait is bounded, because a library Close must return. A stage that
// times out means its goroutines may still be running, so the database is
// deliberately NOT closed: the process exits with the file open and SQLite
// recovers the WAL crash-consistently on the next start, which closing it
// under an active writer would not. Close then reports that as an error.
//
// Safe to call more than once and safe without Start.
func (r *Runtime) Close() error {
	// Held for the whole teardown: a Start racing it must not bring the
	// router and node back up on half-released resources. Start never calls
	// Close while holding it, so this cannot deadlock.
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()

	if r.closed {
		return r.closeErr
	}
	r.stopping = true

	// Long-lived streams end before anything waits on them: they hold an
	// operation lease for as long as they run, and a caller who subscribed
	// with a context they never cancel cannot release it themselves.
	if r.cancelSubscriptions != nil {
		r.cancelSubscriptions()
	}
	cancelNode := r.cancelNode
	nodeStarted := r.nodeStarted
	// Whether anyone else will report the outcome. A Start that failed
	// delivers its own error AFTER this Close returns, so finishing here
	// would win the race with sync.Once and report success for a runtime
	// that never started.
	startAttempted := r.started

	// clean tracks whether every stage joined. It starts at the RPC server:
	// a handler still inside fasthttp calls straight into the command table,
	// and from there into the chatlog.
	clean := true
	fail := func(stage string, timeout time.Duration) {
		clean = false
		log.Warn().Str("stage", stage).Dur("timeout", timeout).Msg("sdk: shutdown stage did not finish")
	}

	// Each stage runs only if it has not already succeeded in an earlier,
	// timed-out Close.
	stage := func(done *bool, name string, timeout time.Duration, join func() bool) {
		if *done {
			return
		}
		if join() {
			*done = true
			r.progress.completed = append(r.progress.completed, name)
			return
		}
		fail(name, timeout)
	}

	// 1. No new external commands, and no new SDK calls. Bounded, unlike
	//    plain Shutdown, which waits indefinitely on a keep-alive connection
	//    or a stuck handler.
	if r.rpcServer != nil {
		stage(&r.progress.rpc, "rpc-server", rpcShutdownTimeout, func() bool {
			// nil here means the whole server went quiet — listeners closed
			// AND every connection idle — and stays honest across retries.
			return r.rpcServer.ShutdownWithTimeout(rpcShutdownTimeout) == nil
		})
	}
	// Told to stop BEFORE they are waited for: a command running under the
	// runtime's context can otherwise take longer than this budget and turn a
	// clean shutdown into an incomplete one.
	if r.cancelCommands != nil {
		r.cancelCommands()
	}
	stage(&r.progress.operations, "sdk-operations", operationDrainTimeout, func() bool {
		return waitFunc(r.operations.Wait, operationDrainTimeout)
	})

	// 2. Router: refuse new sends, drain the in-flight ones, then stop the
	//    long-lived loops. Both paths write to the chatlog.
	if r.router != nil {
		stage(&r.progress.sends, "router-sends", routerDrainTimeout, func() bool {
			return r.router.DrainSends(routerDrainTimeout)
		})
		stage(&r.progress.loops, "router-loops", routerDrainTimeout, func() bool {
			return r.router.StopLoops(routerDrainTimeout)
		})
	}

	// 3. Node: cancel it even if the caller's context is still live, and wait
	//    for Run to return before anything below it goes away.
	if cancelNode != nil {
		cancelNode()
	}
	if nodeStarted {
		stage(&r.progress.node, "node-run", nodeStopTimeout, func() bool {
			return waitClosed(r.nodeStopped, nodeStopTimeout)
		})
	}

	// 4. The node's fire-and-forget jobs are the last WRITERS, and they are
	//    joined here — straight after the node, before anything they talk to
	//    goes away. A job like emitDeliveryReceipt finishes a durable write
	//    and publishes the result; draining the bus first left it publishing
	//    to subscribers that no longer existed.
	if clean {
		stage(&r.progress.background, "node-background", backgroundDrainTimeout, func() bool {
			return waitFunc(r.nodeService.WaitBackground, backgroundDrainTimeout)
		})
	}

	// From here every stage is DESTRUCTIVE to something that may still be
	// running, so each one is gated on the joins above having succeeded.
	// Draining the bus out from under live handlers, or cancelling their
	// context in ShutdownDrain, loses exactly the terminal writes this
	// ordering exists to protect — leaving the database open afterwards does
	// not undo that.
	if clean && r.eventBus != nil {
		stage(&r.progress.bus, "event-bus", busDrainTimeout, func() bool {
			return waitFunc(r.eventBus.Shutdown, busDrainTimeout)
		})
	}
	if clean && r.router != nil {
		stage(&r.progress.router, "router-drain", routerDrainTimeout, func() bool {
			return r.router.ShutdownDrain(routerDrainTimeout)
		})
	}

	// 5. Only now can nothing write any more.
	if !clean {
		log.Warn().Msg("sdk: shutdown incomplete, leaving the state database open for crash-consistent WAL recovery")
		r.closeErr = errors.New("sdk: shutdown incomplete, state database left open — call Close again once the stragglers finish")
		// closed stays false on purpose: every stage above is idempotent, so
		// a later Close retries the joins that timed out and can still
		// release the database instead of leaking it for the process's life.
		return r.closeErr
	}

	r.closeErr = r.database.Close()
	r.closed = true

	// A runtime on which Start was never called has nobody to report the
	// outcome, and Close is documented as safe without Start — so a caller
	// who was already waiting, or who calls Wait afterwards, would have
	// waited forever on a runtime whose database is already closed.
	if !startAttempted {
		r.finish(r.closeErr)
	}
	return r.closeErr
}

// Execute runs a command by name through the same command handlers as the
// console/UI, under the runtime's own command context.
func (r *Runtime) Execute(command string, args map[string]interface{}) (json.RawMessage, error) {
	return r.ExecuteContext(r.commands, command, args)
}

// ExecuteContext is Execute with a caller-supplied context.
//
// A command can legitimately block for longer than the shutdown budget — a
// resolve waits up to 8 seconds while Close gives the operation drain 5 — so
// running it without a context meant a Close arriving mid-command declared the
// shutdown incomplete, Wait reported an error, and the database was
// deliberately left open. The runtime's own context is cancelled when that
// drain begins, which is what makes the budget honest.
func (r *Runtime) ExecuteContext(ctx context.Context, command string, args map[string]interface{}) (json.RawMessage, error) {
	if !r.beginOperation() {
		return nil, errClosed
	}
	defer r.endOperation()

	commandCtx, release := r.commandContext(ctx)
	defer release()

	resp := r.cmdTable.Execute(rpc.CommandRequest{Name: command, Args: args, Ctx: commandCtx})
	return unwrapCommandResponse(resp)
}

// ExecuteCommand parses and runs a console command string in-process, under
// the runtime's own command context.
func (r *Runtime) ExecuteCommand(input string) (json.RawMessage, error) {
	return r.ExecuteCommandContext(r.commands, input)
}

// ExecuteCommandContext is ExecuteCommand with a caller-supplied context.
func (r *Runtime) ExecuteCommandContext(ctx context.Context, input string) (json.RawMessage, error) {
	if !r.beginOperation() {
		return nil, errClosed
	}
	defer r.endOperation()

	req, err := rpc.ParseConsoleInput(input)
	if err != nil {
		return nil, err
	}
	commandCtx, release := r.commandContext(ctx)
	defer release()

	req.Ctx = commandCtx
	resp := r.cmdTable.Execute(req)
	return unwrapCommandResponse(resp)
}

// commandContext ends when EITHER the caller's context or the runtime's
// shutdown does.
//
// Taking the caller's context alone was the same hole in a different place: a
// command passed context.Background never saw cancelCommands, so it could
// outlast the operation drain and turn a clean shutdown into an incomplete one
// with the database left open.
func (r *Runtime) commandContext(ctx context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.commands, cancel)
	return merged, func() {
		stop()
		cancel()
	}
}

func unwrapCommandResponse(resp rpc.CommandResponse) (json.RawMessage, error) {
	if resp.Error != nil {
		return nil, resp.Error
	}
	return append(json.RawMessage(nil), resp.Data...), nil
}

// Shutdown stage budgets. Each bounds one join in Close; the sum is the worst
// case a caller can see from a single Close.
const (
	rpcShutdownTimeout     = 5 * time.Second
	operationDrainTimeout  = 5 * time.Second
	routerDrainTimeout     = 5 * time.Second
	nodeStopTimeout        = 5 * time.Second
	busDrainTimeout        = 10 * time.Second
	backgroundDrainTimeout = 5 * time.Second
)

// waitClosed reports whether done was closed within timeout.
func waitClosed(done <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// waitFunc runs a blocking join on its own goroutine and reports whether it
// finished within timeout. The joins it wraps (ebus.Shutdown,
// Service.WaitBackground) are unbounded by design; a library Close cannot be.
func waitFunc(join func(), timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		join()
		close(done)
	}()
	return waitClosed(done, timeout)
}
