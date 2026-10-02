package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
)

// A load-stand node is one REAL Service on a real loopback socket, built the
// way internal/app/node builds a production node, that can be stopped and
// started again as the same node: same identity, same directory, same
// address. Its configuration is config.Default().Node with only addresses and
// paths replaced; every other departure from production is a named field of
// loadStandHooks, so a measurement can never be taken against a setting
// nobody declared.
//
// What the node does NOT confine to its directory: a panic recovered by
// crashlog is written to crashlog's own log directory (CORSA_CHATLOG_DIR,
// which the stand refuses, else the application data dir — relative to the
// working directory under go test), and the zerolog logger is process-wide.

var (
	errLoadStandCorsaEnvironment = errors.New("environment sets CORSA_* variables that would change the config.Default() the stand measures; unset")
	errLoadStandInvalidOpts      = errors.New("invalid load stand node arguments")
	errLoadStandNodeRunning      = errors.New("load stand node is still running")
	errLoadStandNodeNotRunning   = errors.New("load stand node is not running")
	errLoadStandIdentityChanged  = errors.New("load stand node identity file no longer holds the node's identity")
	errLoadStandPathOutsideDir   = errors.New("load stand node config points outside the node's directory")
	errLoadStandPortExhausted    = errors.New("no loopback port that was not already issued to another stand node")
	// errLoadStandAddressInUse is the outcome of a full node whose reserved
	// address was taken while it was stopped. It is a fact about the
	// machine, not about the node, and the journal records it as such.
	errLoadStandAddressInUse = errors.New("load stand node address is in use by another socket")
)

// loadStandStartBudget bounds how long Start waits for the node to be ready.
// It is wider than startTestService's 3 s because the stand starts nodes on a
// machine it is loading on purpose.
const loadStandStartBudget = 10 * time.Second

// loadStandProfileLabel is the pprof label key the goroutines of a node carry.
const loadStandProfileLabel = "node"

// loadStandPortAttempts bounds how many times the registry asks the OS for a
// port before concluding it keeps getting ports already handed out.
const loadStandPortAttempts = 16

// loadStandInertCorsaVariables are the CORSA_* names tolerated in the
// environment. The Makefile's bare `export` puts its CORSA_VERSION build
// variable into every recipe, and config.Default() does not read it
// (TestLoadStandNode/ToleratesOnlyTheMakefileVersionVariable pins that), so
// refusing it would only make the stand unrunnable through make.
var loadStandInertCorsaVariables = map[string]struct{}{
	"CORSA_VERSION": {},
}

// loadStandNodeID names a node across all its incarnations: its pprof label,
// and the name the stand's journal and plan refer to it by.
type loadStandNodeID string

type loadStandRole string

const (
	// loadStandRoleFull is a listening full node.
	loadStandRoleFull loadStandRole = "full"
	// loadStandRoleEdge is a client node without a listener — a desktop
	// install minus the desktop client and its SQLite store.
	loadStandRoleEdge loadStandRole = "edge"
)

var loadStandRoleNodeTypes = map[loadStandRole]domain.NodeType{
	loadStandRoleFull: domain.NodeTypeFull,
	loadStandRoleEdge: domain.NodeTypeClient,
}

// loadStandRoleAcceptsDirectMessages is the main profile's DM policy: full
// nodes serve the network, edges are the users DMs are addressed to. It gates
// only RECEIVING — a node that refuses DMs still originates them — and it is
// stated here because config.Default().Node accepts DMs and the stand bypasses
// the headless wiring (CORSA_ACCEPT_DM) that would turn that off.
var loadStandRoleAcceptsDirectMessages = map[loadStandRole]bool{
	loadStandRoleFull: false,
	loadStandRoleEdge: true,
}

// loadStandHooks are the stand's declared departures from production. The
// zero value is production itself: no departure.
type loadStandHooks struct {
	// DisableRateLimiting sets Service.disableRateLimiting, which switches
	// off exactly three ACCEPT-path checks: the per-IP connect-rate limiter,
	// the per-IP inbound connection cap (maxConnPerIP), and the refusal of
	// a connection from a blacklisted IP. Every stand node dials from
	// 127.0.0.1, so the per-IP cap would refuse the ninth connection to any
	// node and measure the cap instead of the network.
	//
	// It does NOT switch off how bans are EARNED or OBEYED elsewhere: the
	// per-connection command rate limit still scores the IP (addBanScore),
	// a peer-banned notice still records an IP-wide remote ban, and
	// PeerProvider still widens a per-address ban to every port on that IP.
	// On 127.0.0.1 any one of those cuts the node off from the whole stand
	// and survives a restart through peers.json — which is why every sample
	// is taken only after checkLoadStandBanFree has passed.
	DisableRateLimiting bool
	// AllowPrivatePeers replaces config's value. Without it loopback peers
	// are neither dialled nor persisted to peers.json, so a loopback stand
	// cannot work at all; it also lifts the forbidden-IP filter, which on a
	// stand that only ever sees 127.0.0.1 has nothing else to filter.
	AllowPrivatePeers bool
	// MarkPeerStateInterval sets Service.markPeerStateIntervalTest. Zero
	// keeps the production 1 s throttle on peer-state recomputes; the
	// ordinary test fixtures set -1 (recompute on every frame), which is
	// cheaper to reason about in a test and wrong to measure.
	MarkPeerStateInterval time.Duration
	// OverloadGoroutineThreshold, when set, replaces config's value; nil
	// keeps Default's. Zero disables the announce-loop overload gate: its
	// proxy is runtime.NumGoroutine, which in a process hosting many nodes
	// counts all of them, so any threshold would trip on the stand's size
	// rather than on one node's backlog.
	OverloadGoroutineThreshold *int
}

// loadStandLoopbackHooks are the departures a many-node loopback stand needs.
// MarkPeerStateInterval and the overload threshold are production's values,
// stated so that a change to either default is a change to this function.
func loadStandLoopbackHooks() loadStandHooks {
	gateDisabled := 0
	return loadStandHooks{
		DisableRateLimiting:        true,
		AllowPrivatePeers:          true,
		MarkPeerStateInterval:      0,
		OverloadGoroutineThreshold: &gateDisabled,
	}
}

func (h loadStandHooks) applyToConfig(cfg *config.Node) {
	cfg.AllowPrivatePeers = h.AllowPrivatePeers
	if h.OverloadGoroutineThreshold != nil {
		cfg.OverloadGoroutineThreshold = *h.OverloadGoroutineThreshold
	}
}

func (h loadStandHooks) applyToService(svc *Service) {
	svc.disableRateLimiting = h.DisableRateLimiting
	svc.markPeerStateIntervalTest = h.MarkPeerStateInterval
}

// loadStandNodeOpts are a node's optional settings; the always-required ones
// are newLoadStandNode's positional arguments.
type loadStandNodeOpts struct {
	BootstrapPeers []domain.PeerAddress
	Hooks          loadStandHooks
}

// loadStandPortRegistry hands out loopback addresses for full nodes and never
// the same one twice. A reserved port is released before the node binds it,
// so the OS is free to hand it out again; two nodes holding one address would
// make the second "ready" on the first one's listener. One registry serves the
// whole stand. The mutex is there because reservation is cheap to make safe
// and a stand that builds its nodes from several goroutines is easy to write.
type loadStandPortRegistry struct {
	allocate func() (domain.ListenAddress, error)

	mu     sync.Mutex
	issued map[domain.ListenAddress]struct{}
}

func newLoadStandPortRegistry(allocate func() (domain.ListenAddress, error)) *loadStandPortRegistry {
	return &loadStandPortRegistry{
		allocate: allocate,
		issued:   map[domain.ListenAddress]struct{}{},
	}
}

func (r *loadStandPortRegistry) Reserve() (domain.ListenAddress, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for range loadStandPortAttempts {
		address, err := r.allocate()
		if err != nil {
			return "", err
		}
		if _, taken := r.issued[address]; taken {
			continue
		}
		r.issued[address] = struct{}{}
		return address, nil
	}
	return "", fmt.Errorf("%w after %d attempts", errLoadStandPortExhausted, loadStandPortAttempts)
}

// reserveLoopbackAddress picks a free loopback port from the OS. It is the
// registry's production allocator.
func reserveLoopbackAddress() (domain.ListenAddress, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve loopback port: %w", err)
	}
	address := domain.ListenAddress(listener.Addr().String())
	if err := listener.Close(); err != nil {
		return "", fmt.Errorf("release reserved loopback port: %w", err)
	}
	return address, nil
}

// loadStandNode is owned by one goroutine — the stand's — and is not safe for
// concurrent use. The Service it runs has its own goroutines; the node only
// starts and stops it.
type loadStandNode struct {
	id    loadStandNodeID
	role  loadStandRole
	cfg   config.Node
	hooks loadStandHooks
	// identity is the public address of the key pair in cfg.IdentityPath,
	// fixed at construction; every Start re-reads the file and refuses a
	// different one.
	identity domain.PeerIdentity
	// current is the live incarnation, nil while the node is stopped. A
	// stop that ran out of budget leaves it in place: the incarnation is
	// still running and still the node's to stop.
	current *loadStandIncarnation
	// lives counts the incarnations Start has launched, so every sample can
	// say which life of the node it describes.
	lives loadStandLife
	// ended is every finished incarnation with its final transport reading,
	// in life order: the byte balance sums a node's bytes across the lives
	// a span contains, and a life that ended inside the span is only known
	// through this reading.
	ended []loadStandEndedIncarnation
}

type loadStandIncarnation struct {
	// life is this incarnation's number in the stand; its start time is the
	// Service's own and is read with every sample, so a stand whose nodes
	// run elsewhere learns it the same way.
	life    loadStandLife
	running *runningTestService
	bus     *ebus.Bus
}

// newLoadStandNode builds a node in dir, an absolute directory that outlives
// every incarnation and whose lifetime belongs to the caller. ports serves
// full nodes, which reserve their address here once for their whole life.
func newLoadStandNode(id loadStandNodeID, role loadStandRole, dir string, ports *loadStandPortRegistry, opts loadStandNodeOpts) (*loadStandNode, error) {
	if err := validateLoadStandNodeArgs(id, role, dir, ports); err != nil {
		return nil, err
	}
	if err := refuseCorsaEnvironment(os.Environ()); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("load stand node %s: create directory: %w", id, err)
	}
	cfg, err := buildLoadStandConfig(role, dir, ports, opts)
	if err != nil {
		return nil, fmt.Errorf("load stand node %s: %w", id, err)
	}
	// The identity is created here rather than on first Start so the stand
	// can name every node — in contact lists, in its plan — before any of
	// them runs.
	keys, err := identity.LoadOrCreate(cfg.IdentityPath)
	if err != nil {
		return nil, fmt.Errorf("load stand node %s: identity: %w", id, err)
	}
	return &loadStandNode{
		id:       id,
		role:     role,
		cfg:      cfg,
		hooks:    opts.Hooks,
		identity: domain.PeerIdentityFromWire(keys.Address),
	}, nil
}

func validateLoadStandNodeArgs(id loadStandNodeID, role loadStandRole, dir string, ports *loadStandPortRegistry) error {
	nodeType, knownRole := loadStandRoleNodeTypes[role]
	switch {
	case id == "":
		return fmt.Errorf("%w: empty node id", errLoadStandInvalidOpts)
	case !knownRole:
		return fmt.Errorf("%w: node %s: unknown role %q", errLoadStandInvalidOpts, id, role)
	case !filepath.IsAbs(dir):
		return fmt.Errorf("%w: node %s: directory %q is not absolute", errLoadStandInvalidOpts, id, dir)
	case nodeType == domain.NodeTypeFull && ports == nil:
		return fmt.Errorf("%w: node %s: a full node needs a port registry", errLoadStandInvalidOpts, id)
	default:
		return nil
	}
}

// refuseCorsaEnvironment names the offending variables and never their
// values: a CORSA_* value can be an RPC password.
func refuseCorsaEnvironment(environ []string) error {
	var offending []string
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if _, inert := loadStandInertCorsaVariables[name]; inert || !strings.HasPrefix(name, "CORSA_") {
			continue
		}
		offending = append(offending, name)
	}
	if len(offending) == 0 {
		return nil
	}
	sort.Strings(offending)
	return fmt.Errorf("%w: %s", errLoadStandCorsaEnvironment, strings.Join(offending, ", "))
}

// buildLoadStandConfig is config.Default().Node with exactly these replaced:
// the role's node type and direct-message policy; the listen and advertised address of a full node; the
// bootstrap list (Default's is the main network's seeds); every path, into the
// node's directory; and whatever the hooks declare. An edge keeps Default's
// listen address: it never binds it, and with the listener off nothing
// advertises it.
func buildLoadStandConfig(role loadStandRole, dir string, ports *loadStandPortRegistry, opts loadStandNodeOpts) (config.Node, error) {
	cfg := config.Default().Node
	cfg.Type = loadStandRoleNodeTypes[role]
	cfg.DisableDirectMessages = !loadStandRoleAcceptsDirectMessages[role]
	cfg.BootstrapPeers = make([]string, 0, len(opts.BootstrapPeers))
	for _, peer := range opts.BootstrapPeers {
		cfg.BootstrapPeers = append(cfg.BootstrapPeers, string(peer))
	}
	cfg.IdentityPath = filepath.Join(dir, "identity.json")
	cfg.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.IdentityIntentsPath = filepath.Join(dir, "identity-intents.json")
	cfg.PeersStatePath = filepath.Join(dir, "peers.json")
	// The data directory: file-transfer store, downloads, identity backups
	// and traffic captures all derive from it.
	cfg.ChatLogDir = dir
	opts.Hooks.applyToConfig(&cfg)

	if err := requireConfigPathsInside(cfg, dir); err != nil {
		return config.Node{}, err
	}
	if !cfg.EffectiveListenerEnabled() {
		return cfg, nil
	}
	address, err := ports.Reserve()
	if err != nil {
		return config.Node{}, err
	}
	cfg.ListenAddress = string(address)
	// Default's AdvertisePort is absent (no CORSA_ADVERTISE_PORT), which
	// would advertise 64646; the node must advertise the port it binds.
	return deriveTestAdvertisePort(cfg), nil
}

// requireConfigPathsInside refuses a config that holds an absolute path
// outside dir in any string field, or a relative path in a path field (one
// named *Path or *Dir): a relative path resolves against the working
// directory, which is outside dir too. It walks the fields instead of naming
// them, so a path field config.Default() gains later — which the stand does
// not know to replace — stops the stand instead of pointing a node at the
// user's own data. An empty path field is allowed: it means "derive from the
// data directory" or "not used".
func requireConfigPathsInside(cfg config.Node, dir string) error {
	value := reflect.ValueOf(cfg)
	for i := range value.NumField() {
		field := value.Field(i)
		if field.Kind() != reflect.String {
			continue
		}
		name := value.Type().Field(i).Name
		if path := field.String(); configPathEscapes(name, path, dir) {
			return fmt.Errorf("%w: %s = %q", errLoadStandPathOutsideDir, name, path)
		}
	}
	return nil
}

func configPathEscapes(fieldName, path, dir string) bool {
	isPathField := strings.HasSuffix(fieldName, "Path") || strings.HasSuffix(fieldName, "Dir")
	switch {
	case filepath.IsAbs(path):
		return !pathIsInside(dir, path)
	case isPathField:
		return path != ""
	default:
		return false
	}
}

func pathIsInside(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// Config returns a copy of the node's configuration, which is the same for
// every incarnation; the slice and pointer fields are cloned so a caller
// cannot change what the next Start runs with.
func (n *loadStandNode) Config() config.Node {
	cfg := n.cfg
	cfg.BootstrapPeers = slices.Clone(n.cfg.BootstrapPeers)
	if n.cfg.AdvertisePort != nil {
		port := *n.cfg.AdvertisePort
		cfg.AdvertisePort = &port
	}
	return cfg
}

func (n *loadStandNode) PeerIdentity() domain.PeerIdentity { return n.identity }

// DialAddress is the address other nodes bootstrap to; only a node with a
// listener has one.
func (n *loadStandNode) DialAddress() (domain.PeerAddress, bool) {
	if !n.cfg.EffectiveListenerEnabled() {
		return "", false
	}
	return domain.PeerAddress(n.cfg.ListenAddress), true
}

// Service returns the running incarnation's Service.
func (n *loadStandNode) Service() (*Service, bool) {
	if n.current == nil {
		return nil, false
	}
	return n.current.running.svc, true
}

// CheckBanFree is the validity check a sample of this node is taken behind:
// see checkLoadStandBanFree.
func (n *loadStandNode) CheckBanFree(now time.Time, policy loadStandBanPolicy) error {
	svc, ok := n.Service()
	if !ok {
		return fmt.Errorf("load stand node %s: %w", n.id, errLoadStandNodeNotRunning)
	}
	if err := checkLoadStandBanFree(svc, now, policy); err != nil {
		return fmt.Errorf("load stand node %s: %w", n.id, err)
	}
	return nil
}

// Start runs a new incarnation of the node under ctx — cancelling ctx stops
// it as Stop would, without waiting. It is built the way production builds a
// node: a fresh event bus, PrimeBootstrapPeers between NewService and Run.
//
// Start returns once the incarnation is ready in the sense of
// runningTestService.AwaitReady — for EVERY role, Run has finished its
// start-up (bootstrap priming, hot-read snapshots), and a full node accepts
// connections on its own listener. A Start that fails after Run was launched
// leaves the incarnation in place, because only Stop can tell when it is
// gone.
//
// The node's pprof label reaches every goroutine Run starts, and nothing
// else: a goroutine the stand starts itself — a load generator — must run
// under its own pprof.Do to be attributed.
func (n *loadStandNode) Start(ctx context.Context) error {
	if n.current != nil {
		return fmt.Errorf("load stand node %s: %w", n.id, errLoadStandNodeRunning)
	}
	// Re-read from disk on every Start, as a restarted production node
	// does: an identity that survives only in memory would not be one that
	// survives a restart.
	keys, err := identity.LoadOrCreate(n.cfg.IdentityPath)
	if err != nil {
		return fmt.Errorf("load stand node %s: identity: %w", n.id, err)
	}
	if loaded := domain.PeerIdentityFromWire(keys.Address); loaded != n.identity {
		return fmt.Errorf("load stand node %s: %w: want %s, file holds %s", n.id, errLoadStandIdentityChanged, n.identity, loaded)
	}

	// internal/app/node gives every node a bus; the identity resolver
	// subscribes to it, so a nil bus would silently switch that work off.
	bus := ebus.New()
	svc := NewService(n.cfg, keys, bus)
	n.hooks.applyToService(svc)
	svc.PrimeBootstrapPeers()

	var running *runningTestService
	// Goroutines inherit the labels of the goroutine that starts them, so
	// labelling the launch labels everything Run starts.
	pprof.Do(ctx, pprof.Labels(loadStandProfileLabel, string(n.id)), func(labelled context.Context) {
		running = runServiceForTest(labelled, svc)
	})
	n.lives++
	n.current = &loadStandIncarnation{
		life:    n.lives,
		running: running,
		bus:     bus,
	}

	readyCtx, cancel := context.WithTimeout(ctx, loadStandStartBudget)
	defer cancel()
	if err := running.AwaitReady(readyCtx); err != nil {
		return fmt.Errorf("load stand node %s: %w", n.id, classifyLoadStandRunError(err))
	}
	return nil
}

// Stop ends the running incarnation within ctx. An error matching
// errStopBudgetExceeded means part of the node is still running: the node
// keeps the incarnation and Stop may be called again. Any other outcome —
// including Run's own error — leaves the node stopped.
func (n *loadStandNode) Stop(ctx context.Context) error {
	if n.current == nil {
		return fmt.Errorf("load stand node %s: %w", n.id, errLoadStandNodeNotRunning)
	}
	incarnation := n.current
	err := incarnation.shutdown(ctx)
	if !errors.Is(err, errStopBudgetExceeded) {
		n.recordEnded(incarnation)
		n.current = nil
	}
	if err != nil {
		return fmt.Errorf("load stand node %s: %w", n.id, classifyLoadStandRunError(err))
	}
	return nil
}

// recordEnded keeps the final transport reading of an incarnation whose Run
// has returned. It is read AFTER the stop: the counters are atomics on the
// Service and stay readable, and with every socket closed nothing moves them
// any more, so the reading is exact — no tail allowance.
func (n *loadStandNode) recordEnded(incarnation *loadStandIncarnation) {
	n.ended = append(n.ended, loadStandEndedIncarnation{
		Node:  n.id,
		Life:  incarnation.life,
		Final: incarnation.running.svc.TransportTrafficStats(),
	})
}

// EndedIncarnations returns the finished incarnations, oldest first.
func (n *loadStandNode) EndedIncarnations() []loadStandEndedIncarnation {
	return slices.Clone(n.ended)
}

// classifyLoadStandRunError marks a Run that could not bind its address, so
// the journal can tell "the machine took our port" from "the node failed".
func classifyLoadStandRunError(err error) error {
	if errors.Is(err, syscall.EADDRINUSE) {
		return fmt.Errorf("%w: %w", errLoadStandAddressInUse, err)
	}
	return err
}

// stopStageEventBus names the third shutdown stage of a stand incarnation:
// the event bus subscribers were still draining. It lives here, not beside
// the harness's own stages, because only a stand node owns a bus — the run
// harness stops Run and its background jobs and nothing else.
const stopStageEventBus stopStage = "event_bus"

// shutdown stops the three things an incarnation owns in the order each
// depends on the previous: Run, then the jobs Run left behind, then the bus
// those jobs publish to. Run's own error is kept through the later stages so
// a node that failed AND was slow to drain reports both.
func (inc *loadStandIncarnation) shutdown(ctx context.Context) error {
	runErr := inc.running.Stop(ctx)
	if errors.Is(runErr, errStopBudgetExceeded) {
		return runErr
	}
	if err := inc.running.DrainBackground(ctx); err != nil {
		return errors.Join(runErr, err)
	}
	if err := awaitWithinBudget(ctx, stopStageEventBus, inc.bus.Shutdown); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}
