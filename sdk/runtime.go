package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/metrics"
	corsanode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"
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
	client      *service.DesktopClient
	router      *service.DMRouter
	metrics     *metrics.Collector
	cmdTable    *rpc.CommandTable
	rpcServer   *rpc.Server

	startOnce sync.Once
	waitCh    chan error
	startErr  error

	closeOnce sync.Once
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

// New creates a new SDK runtime from explicit Go configuration.
func New(cfg Config) (*Runtime, error) {
	internalCfg := cfg.internal()

	id, err := resolveIdentity(cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve identity: %w", err)
	}

	eventBus := ebus.New()

	nodeService := corsanode.NewService(internalCfg.Node, id, eventBus)
	client := service.NewDesktopClient(internalCfg.App, internalCfg.Node, id, nodeService)
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
			_ = client.Close()
			return nil, fmt.Errorf("create rpc server: %w", err)
		}
	}

	return &Runtime{
		cfg:         normalizeConfig(cfg),
		nodeService: nodeService,
		client:      client,
		router:      router,
		metrics:     metricsCollector,
		cmdTable:    cmdTable,
		rpcServer:   rpcServer,
		waitCh:      make(chan error, 1),
	}, nil
}

// Config returns the normalized runtime configuration.
func (r *Runtime) Config() Config {
	return r.cfg
}

// Address returns the local identity address.
func (r *Runtime) Address() string {
	return string(r.client.Address())
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
func (r *Runtime) Start(ctx context.Context) error {
	r.startOnce.Do(func() {
		// Capture the current totals as delta baseline before the first
		// Record. Without Seed the first sample either hides bootstrap
		// traffic (old skip-on-first behavior) or reports the entire
		// pre-Seed cumulative as a single-second spike.
		r.metrics.Seed()
		go r.metrics.Run(ctx)
		r.router.Start()
		r.nodeService.PrimeBootstrapPeers()

		if r.rpcServer != nil {
			if err := r.rpcServer.StartAsync(); err != nil {
				r.startErr = fmt.Errorf("start rpc server: %w", err)
				r.waitCh <- r.startErr
				close(r.waitCh)
				_ = r.Close()
				return
			}
		}

		go func() {
			err := r.nodeService.Run(ctx)
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			r.waitCh <- err
			close(r.waitCh)
			_ = r.Close()
		}()
	})

	return r.startErr
}

// Wait blocks until the node runtime stops.
func (r *Runtime) Wait() error {
	return <-r.waitCh
}

// Run starts the runtime and blocks until it stops.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.Start(ctx); err != nil {
		return err
	}
	return r.Wait()
}

// Close releases auxiliary resources owned by the SDK runtime.
func (r *Runtime) Close() error {
	var firstErr error

	r.closeOnce.Do(func() {
		if r.rpcServer != nil {
			if err := r.rpcServer.Shutdown(); err != nil {
				firstErr = err
			}
		}
		if err := r.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	})

	return firstErr
}

// Execute runs a command by name through the same command handlers as the console/UI.
func (r *Runtime) Execute(command string, args map[string]interface{}) (json.RawMessage, error) {
	resp := r.cmdTable.Execute(rpc.CommandRequest{Name: command, Args: args})
	return unwrapCommandResponse(resp)
}

// ExecuteCommand parses and runs a console command string in-process.
func (r *Runtime) ExecuteCommand(input string) (json.RawMessage, error) {
	req, err := rpc.ParseConsoleInput(input)
	if err != nil {
		return nil, err
	}
	resp := r.cmdTable.Execute(req)
	return unwrapCommandResponse(resp)
}

func unwrapCommandResponse(resp rpc.CommandResponse) (json.RawMessage, error) {
	if resp.Error != nil {
		return nil, resp.Error
	}
	return append(json.RawMessage(nil), resp.Data...), nil
}
