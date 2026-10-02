package node

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestLoadStandNode groups every test that builds a stand node, because a
// stand node is built from config.Default(), which reads the process
// environment. The parent clears every CORSA_* variable for the whole group
// — a developer's own setting must not turn the package red — and that is
// only possible here: t.Setenv is refused in a parallel test, while a serial
// parent may set the environment for parallel subtests, which all finish
// before its cleanup restores it.
func TestLoadStandNode(t *testing.T) {
	isolateFromCorsaEnvironment(t)

	// Serial: these two set environment variables of their own.
	t.Run("RefusesCorsaEnvironment", testLoadStandNodeRefusesCorsaEnvironment)
	t.Run("ToleratesOnlyTheMakefileVersionVariable", testLoadStandNodeToleratesOnlyTheMakefileVersionVariable)

	parallel := map[string]func(*testing.T){
		"ConfigIsDefaultExceptAddressesAndPaths":       testLoadStandNodeConfigIsDefaultExceptAddressesAndPaths,
		"EdgeKeepsDefaultListenAddressAndNoListener":   testLoadStandNodeEdgeKeepsDefaultListenAddressAndNoListener,
		"KeepsEveryPathInsideItsDirectory":             testLoadStandNodeKeepsEveryPathInsideItsDirectory,
		"RefusesConfigPathOutsideItsDirectory":         testLoadStandNodeRefusesConfigPathOutsideItsDirectory,
		"RejectsInvalidArguments":                      testLoadStandNodeRejectsInvalidArguments,
		"AppliesDeclaredHooks":                         testLoadStandNodeAppliesDeclaredHooks,
		"KeepsDefaultOverloadThresholdWhenNotDeclared": testLoadStandNodeKeepsDefaultOverloadThresholdWhenNotDeclared,
		"PrimesBootstrapPeersLikeProduction":           testLoadStandNodePrimesBootstrapPeersLikeProduction,
		"RestartKeepsIdentityAddressAndPeersFile":      testLoadStandNodeRestartKeepsIdentityAddressAndPeersFile,
		"RestartOnTakenAddressIsAddressInUse":          testLoadStandNodeRestartOnTakenAddressIsAddressInUse,
		"RefusesChangedIdentityFile":                   testLoadStandNodeRefusesChangedIdentityFile,
		"StopOverBudgetKeepsNodeOwnedUntilRetried":     testLoadStandNodeStopOverBudgetKeepsNodeOwnedUntilRetried,
		"LabelsItsGoroutinesWithNodeID":                testLoadStandNodeLabelsItsGoroutinesWithNodeID,
		"BanCheckPassesOnACleanNode":                   testLoadStandNodeBanCheckPassesOnACleanNode,
		"BanCheckFailsOnAnyBan":                        testLoadStandBanCheckFailsOnAnyBan,
		"PortRegistryNeverIssuesAPortTwice":            testLoadStandPortRegistryNeverIssuesAPortTwice,
		"SelfIdentityCooldownIsNotABanByDefault":       testLoadStandSelfIdentityCooldownIsNotABanByDefault,
		"RefusesRelativeConfigPath":                    testLoadStandNodeRefusesRelativeConfigPath,
		"DirectMessagePolicyFollowsTheRole":            testLoadStandNodeDirectMessagePolicyFollowsTheRole,
	}
	for name, test := range parallel {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			test(t)
		})
	}
}

// isolateFromCorsaEnvironment unsets every CORSA_* variable for the test and
// restores it afterwards. t.Setenv registers the restore; the unset that
// follows is what makes the variable ABSENT rather than empty.
func isolateFromCorsaEnvironment(t *testing.T) {
	t.Helper()

	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "CORSA_") {
			continue
		}
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}

// newFullNodeForTest and newEdgeNodeForTest build a node with the loopback
// hooks in its own temporary directory.
func newFullNodeForTest(t *testing.T, ports *loadStandPortRegistry, id loadStandNodeID, opts loadStandNodeOpts) *loadStandNode {
	t.Helper()

	return newLoadStandNodeForTest(t, id, loadStandRoleFull, ports, opts)
}

func newEdgeNodeForTest(t *testing.T, id loadStandNodeID, opts loadStandNodeOpts) *loadStandNode {
	t.Helper()

	return newLoadStandNodeForTest(t, id, loadStandRoleEdge, newLoadStandPortRegistry(reserveLoopbackAddress), opts)
}

func newLoadStandNodeForTest(t *testing.T, id loadStandNodeID, role loadStandRole, ports *loadStandPortRegistry, opts loadStandNodeOpts) *loadStandNode {
	t.Helper()

	node, err := newLoadStandNode(id, role, t.TempDir(), ports, opts)
	if err != nil {
		t.Fatalf("newLoadStandNode(%s): %v", id, err)
	}
	return node
}

func loopbackOpts() loadStandNodeOpts {
	return loadStandNodeOpts{Hooks: loadStandLoopbackHooks()}
}

// startLoadStandNodeForTest starts the node and arranges for whatever
// incarnation is live at the end of the test to be stopped.
func startLoadStandNodeForTest(t *testing.T, node *loadStandNode) {
	t.Helper()

	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopLoadStandNodeOnCleanup(t, node)
}

func stopLoadStandNodeOnCleanup(t *testing.T, node *loadStandNode) {
	t.Helper()

	t.Cleanup(func() {
		err := node.Stop(withBudget(t, harnessGenerousBudget))
		if err != nil && !errors.Is(err, errLoadStandNodeNotRunning) {
			t.Errorf("cleanup Stop: %v", err)
		}
	})
}

func stopLoadStandNodeForTest(t *testing.T, node *loadStandNode) {
	t.Helper()

	if err := node.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func runningServiceForTest(t *testing.T, node *loadStandNode) *Service {
	t.Helper()

	svc, ok := node.Service()
	if !ok {
		t.Fatal("node has no running service")
	}
	return svc
}

// boundListenerAddress is the address the node's OWN listener is bound to,
// read where Run publishes it — not the configured address, which says what
// the node was asked to bind rather than what it did.
func boundListenerAddress(t *testing.T, svc *Service) string {
	t.Helper()

	svc.peerMu.RLock()
	listener := svc.listener
	svc.peerMu.RUnlock()
	if listener == nil {
		t.Fatal("service has no bound listener")
	}
	return listener.Addr().String()
}

// The stand measures the configuration config.Default() produces; an
// operator variable in the environment would silently make it measure a
// different one.
func testLoadStandNodeRefusesCorsaEnvironment(t *testing.T) {
	const secretValue = "hunter2-not-for-logs"
	t.Setenv("CORSA_RPC_PASSWORD", secretValue)
	t.Setenv("CORSA_HOLD_DM_UNTIL_REACHABLE", "false")

	_, err := newLoadStandNode("refused", loadStandRoleEdge, t.TempDir(), newLoadStandPortRegistry(reserveLoopbackAddress), loopbackOpts())
	if !errors.Is(err, errLoadStandCorsaEnvironment) {
		t.Fatalf("newLoadStandNode = %v, want errLoadStandCorsaEnvironment", err)
	}
	for _, name := range []string{"CORSA_RPC_PASSWORD", "CORSA_HOLD_DM_UNTIL_REACHABLE"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
	// Only names are reported: a CORSA_* value can be a credential.
	if strings.Contains(err.Error(), secretValue) {
		t.Fatalf("error %q carries a variable's value", err)
	}
}

// The Makefile's bare `export` puts its CORSA_VERSION build variable into
// every recipe's environment, so refusing it would make the stand
// unrunnable through make. It is tolerated only because config.Default()
// does not read it — which this test pins.
func testLoadStandNodeToleratesOnlyTheMakefileVersionVariable(t *testing.T) {
	withoutVersion := config.Default()
	t.Setenv("CORSA_VERSION", "0.0.0-from-make")
	withVersion := config.Default()
	if !reflect.DeepEqual(withoutVersion, withVersion) {
		t.Fatal("config.Default() reads CORSA_VERSION; the stand may no longer tolerate it")
	}

	if _, err := newLoadStandNode("version-only", loadStandRoleEdge, t.TempDir(), newLoadStandPortRegistry(reserveLoopbackAddress), loopbackOpts()); err != nil {
		t.Fatalf("newLoadStandNode with only CORSA_VERSION set = %v, want nil", err)
	}
}

func testLoadStandNodeConfigIsDefaultExceptAddressesAndPaths(t *testing.T) {
	peer := domain.PeerAddress("127.0.0.1:1")
	opts := loopbackOpts()
	opts.BootstrapPeers = []domain.PeerAddress{peer}
	node := newFullNodeForTest(t, newLoadStandPortRegistry(reserveLoopbackAddress), "full-config", opts)
	got := node.Config()
	want := config.Default().Node

	// Fields whose zero value would be the wrong answer: a config assembled
	// from a struct literal fails here even if it gets every address right.
	if !got.HoldDMUntilReachable || got.HoldDMUntilReachable != want.HoldDMUntilReachable {
		t.Errorf("HoldDMUntilReachable = %v, want Default's %v", got.HoldDMUntilReachable, want.HoldDMUntilReachable)
	}
	if !got.EnvelopeRetentionEnabled || got.EnvelopeRetentionEnabled != want.EnvelopeRetentionEnabled {
		t.Errorf("EnvelopeRetentionEnabled = %v, want Default's %v", got.EnvelopeRetentionEnabled, want.EnvelopeRetentionEnabled)
	}
	if got.MaxNextHopsPerOrigin == 0 || got.MaxNextHopsPerOrigin != want.MaxNextHopsPerOrigin {
		t.Errorf("MaxNextHopsPerOrigin = %d, want Default's %d", got.MaxNextHopsPerOrigin, want.MaxNextHopsPerOrigin)
	}
	if got.Type != domain.NodeTypeFull || !got.EffectiveListenerEnabled() {
		t.Errorf("full role: Type = %q, listener = %v", got.Type, got.EffectiveListenerEnabled())
	}
	if len(got.BootstrapPeers) != 1 || got.BootstrapPeers[0] != string(peer) {
		t.Errorf("BootstrapPeers = %v, want exactly the opts peer (never Default's internet seeds)", got.BootstrapPeers)
	}
	if !got.AllowPrivatePeers {
		t.Error("AllowPrivatePeers = false although the loopback hooks declare it")
	}

	// Everything else is Default's, field for field: restoring the replaced
	// fields to Default's values must leave nothing to tell the two apart.
	restored := got
	restored.ListenAddress = want.ListenAddress
	restored.AdvertisePort = want.AdvertisePort
	restored.BootstrapPeers = want.BootstrapPeers
	restored.IdentityPath = want.IdentityPath
	restored.TrustStorePath = want.TrustStorePath
	restored.IdentityIntentsPath = want.IdentityIntentsPath
	restored.PeersStatePath = want.PeersStatePath
	restored.ChatLogDir = want.ChatLogDir
	restored.Type = want.Type
	restored.DisableDirectMessages = want.DisableDirectMessages
	restored.AllowPrivatePeers = want.AllowPrivatePeers
	restored.OverloadGoroutineThreshold = want.OverloadGoroutineThreshold
	if !reflect.DeepEqual(restored, want) {
		t.Fatalf("config differs from config.Default().Node beyond what the stand declares:\n got  %+v\n want %+v", restored, want)
	}
}

func testLoadStandNodeEdgeKeepsDefaultListenAddressAndNoListener(t *testing.T) {
	node := newEdgeNodeForTest(t, "edge-config", loopbackOpts())
	got := node.Config()

	if got.Type != domain.NodeTypeClient || got.EffectiveListenerEnabled() {
		t.Fatalf("edge role: Type = %q, listener = %v; want client without a listener", got.Type, got.EffectiveListenerEnabled())
	}
	if got.ListenAddress != config.Default().Node.ListenAddress {
		t.Fatalf("edge ListenAddress = %q, want Default's %q: an edge never binds it", got.ListenAddress, config.Default().Node.ListenAddress)
	}
	if _, ok := node.DialAddress(); ok {
		t.Fatal("an edge node reports a dial address")
	}
}

func testLoadStandNodeKeepsEveryPathInsideItsDirectory(t *testing.T) {
	node := newFullNodeForTest(t, newLoadStandPortRegistry(reserveLoopbackAddress), "paths", loopbackOpts())
	cfg := node.Config()
	dir := cfg.ChatLogDir

	paths := map[string]string{
		"IdentityPath":               cfg.IdentityPath,
		"TrustStorePath":             cfg.TrustStorePath,
		"IdentityIntentsPath":        cfg.IdentityIntentsPath,
		"PeersStatePath":             cfg.PeersStatePath,
		"EffectiveDataDir":           cfg.EffectiveDataDir(),
		"EffectiveDownloadDir":       cfg.EffectiveDownloadDir(),
		"EffectiveIdentityBackupDir": cfg.EffectiveIdentityBackupDir(),
	}
	for name, path := range paths {
		if path == "" || !pathIsInside(dir, path) {
			t.Errorf("%s = %q escapes the node directory %q", name, path, dir)
		}
	}
	// The node never opens the SQLite state database; a path here would be
	// a file the stand silently shares with a desktop install.
	if cfg.StateDBPath != "" {
		t.Errorf("StateDBPath = %q, want empty", cfg.StateDBPath)
	}
}

// A path field Default fills in that the stand does not know to replace must
// stop the stand, not quietly point a node at the user's own data.
func testLoadStandNodeRefusesConfigPathOutsideItsDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Node{
		ChatLogDir:  dir,
		DownloadDir: filepath.Join(filepath.Dir(dir), "somebody-elses-downloads"),
	}

	err := requireConfigPathsInside(cfg, dir)
	if !errors.Is(err, errLoadStandPathOutsideDir) {
		t.Fatalf("requireConfigPathsInside = %v, want errLoadStandPathOutsideDir", err)
	}
	if !strings.Contains(err.Error(), "DownloadDir") {
		t.Fatalf("error %q does not name the field", err)
	}
	cfg.DownloadDir = filepath.Join(dir, "downloads")
	if err := requireConfigPathsInside(cfg, dir); err != nil {
		t.Fatalf("requireConfigPathsInside with every path inside = %v, want nil", err)
	}
}

func testLoadStandNodeRejectsInvalidArguments(t *testing.T) {
	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	cases := map[string]struct {
		id    loadStandNodeID
		role  loadStandRole
		dir   string
		ports *loadStandPortRegistry
	}{
		"empty id":            {role: loadStandRoleEdge, dir: t.TempDir(), ports: ports},
		"unknown role":        {id: "x", role: "hub", dir: t.TempDir(), ports: ports},
		"relative dir":        {id: "x", role: loadStandRoleEdge, dir: "relative/dir", ports: ports},
		"full without a port": {id: "x", role: loadStandRoleFull, dir: t.TempDir()},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newLoadStandNode(tc.id, tc.role, tc.dir, tc.ports, loopbackOpts()); !errors.Is(err, errLoadStandInvalidOpts) {
				t.Fatalf("newLoadStandNode = %v, want errLoadStandInvalidOpts", err)
			}
		})
	}
}

// The departures from production are applied exactly as declared in opts,
// never as a fixed set the harness decides on its own.
func testLoadStandNodeAppliesDeclaredHooks(t *testing.T) {
	threshold := 5000
	declared := loadStandHooks{
		DisableRateLimiting:        true,
		AllowPrivatePeers:          true,
		MarkPeerStateInterval:      250 * time.Millisecond,
		OverloadGoroutineThreshold: &threshold,
	}
	hooked := newEdgeNodeForTest(t, "hooked", loadStandNodeOpts{Hooks: declared})
	production := newEdgeNodeForTest(t, "production", loadStandNodeOpts{})
	startLoadStandNodeForTest(t, hooked)
	startLoadStandNodeForTest(t, production)

	hookedSvc := runningServiceForTest(t, hooked)
	if !hookedSvc.disableRateLimiting || hookedSvc.markPeerStateIntervalTest != declared.MarkPeerStateInterval {
		t.Errorf("hooked: disableRateLimiting = %v, markPeerStateIntervalTest = %s; want true, %s",
			hookedSvc.disableRateLimiting, hookedSvc.markPeerStateIntervalTest, declared.MarkPeerStateInterval)
	}
	if !hookedSvc.cfg.AllowPrivatePeers {
		t.Error("hooked: AllowPrivatePeers not applied")
	}
	if hookedSvc.cfg.OverloadGoroutineThreshold != threshold || !hookedSvc.overloadMonitor.enabled.Load() {
		t.Errorf("hooked: overload threshold = %d, gate enabled = %v; want %d and enabled",
			hookedSvc.cfg.OverloadGoroutineThreshold, hookedSvc.overloadMonitor.enabled.Load(), threshold)
	}

	productionSvc := runningServiceForTest(t, production)
	if productionSvc.disableRateLimiting || productionSvc.markPeerStateIntervalTest != 0 || productionSvc.cfg.AllowPrivatePeers {
		t.Errorf("zero hooks: disableRateLimiting = %v, markPeerStateIntervalTest = %s, AllowPrivatePeers = %v; want production values",
			productionSvc.disableRateLimiting, productionSvc.markPeerStateIntervalTest, productionSvc.cfg.AllowPrivatePeers)
	}

	// The loopback hooks restate two production values; these pins turn a
	// change of either default red instead of letting the stand silently
	// depart from production.
	if threshold := config.Default().Node.OverloadGoroutineThreshold; threshold != 0 {
		t.Errorf("config.Default() overload threshold = %d; loadStandLoopbackHooks no longer states production's value", threshold)
	}
	if interval := newHarnessProbeService(t, domain.NodeTypeClient).markPeerStateIntervalTest; interval != 0 {
		t.Errorf("NewService peer-state throttle override = %s; loadStandLoopbackHooks no longer states production's value", interval)
	}

	loopback := loadStandLoopbackHooks()
	if !loopback.DisableRateLimiting || !loopback.AllowPrivatePeers || loopback.MarkPeerStateInterval != 0 ||
		loopback.OverloadGoroutineThreshold == nil || *loopback.OverloadGoroutineThreshold != 0 {
		t.Errorf("loadStandLoopbackHooks() = %+v, want rate limiting off, private peers on, production throttle, overload gate explicitly off", loopback)
	}
}

func testLoadStandNodeKeepsDefaultOverloadThresholdWhenNotDeclared(t *testing.T) {
	node := newEdgeNodeForTest(t, "undeclared-threshold", loadStandNodeOpts{})

	if got, want := node.Config().OverloadGoroutineThreshold, config.Default().Node.OverloadGoroutineThreshold; got != want {
		t.Fatalf("OverloadGoroutineThreshold = %d, want Default's %d when the hooks declare none", got, want)
	}
}

// Every production entry point calls PrimeBootstrapPeers between NewService
// and Run; a stand node that skipped it would start with a different
// bootstrap path from the one it is meant to measure.
func testLoadStandNodePrimesBootstrapPeersLikeProduction(t *testing.T) {
	opts := loopbackOpts()
	opts.BootstrapPeers = []domain.PeerAddress{"127.0.0.1:1"}
	node := newEdgeNodeForTest(t, "primed", opts)
	startLoadStandNodeForTest(t, node)

	svc := runningServiceForTest(t, node)
	svc.peerMu.RLock()
	primed := svc.primeBootstrapOnRun
	svc.peerMu.RUnlock()
	if !primed {
		t.Fatal("the stand node was started without PrimeBootstrapPeers")
	}
}

// A restart is a new incarnation of the SAME node: the identity, the
// address peers dial and the peers file it learned all survive it.
func testLoadStandNodeRestartKeepsIdentityAddressAndPeersFile(t *testing.T) {
	full := newFullNodeForTest(t, newLoadStandPortRegistry(reserveLoopbackAddress), "full-a", loopbackOpts())
	fullAddress, ok := full.DialAddress()
	if !ok {
		t.Fatal("full node has no dial address")
	}
	edgeOpts := loopbackOpts()
	edgeOpts.BootstrapPeers = []domain.PeerAddress{fullAddress}
	edge := newEdgeNodeForTest(t, "edge-b", edgeOpts)

	startLoadStandNodeForTest(t, full)
	startLoadStandNodeForTest(t, edge)
	fullIdentity := domain.PeerIdentityFromWire(runningServiceForTest(t, full).Address())
	edgeIdentity := domain.PeerIdentityFromWire(runningServiceForTest(t, edge).Address())
	if fullIdentity != full.PeerIdentity() || edgeIdentity != edge.PeerIdentity() {
		t.Fatal("running identities differ from the ones the nodes were constructed with")
	}
	waitForCondition(t, 10*time.Second, func() bool {
		return hasOutboundSession(runningServiceForTest(t, edge))
	})

	stopLoadStandNodeForTest(t, edge)
	stopLoadStandNodeForTest(t, full)

	// bootstrapLoop flushes peers.json on its way out, so the file a stopped
	// node leaves behind already names the full node by the identity the
	// edge LEARNED in the handshake — nothing in the config carries it.
	state, err := loadPeerState(edge.Config().PeersStatePath)
	if err != nil {
		t.Fatalf("load edge peers file: %v", err)
	}
	if !peerStateNames(state, fullAddress, fullIdentity) {
		t.Fatalf("edge peers file %+v does not record %s as %s", state.Peers, fullAddress, fullIdentity)
	}

	startLoadStandNodeForTest(t, full)
	startLoadStandNodeForTest(t, edge)

	if got := domain.PeerIdentityFromWire(runningServiceForTest(t, full).Address()); got != fullIdentity {
		t.Errorf("full identity after restart = %s, want %s", got, fullIdentity)
	}
	if got := domain.PeerIdentityFromWire(runningServiceForTest(t, edge).Address()); got != edgeIdentity {
		t.Errorf("edge identity after restart = %s, want %s", got, edgeIdentity)
	}
	if got := boundListenerAddress(t, runningServiceForTest(t, full)); got != string(fullAddress) {
		t.Errorf("full listener bound to %s after restart, want %s", got, fullAddress)
	}

	restartedEdge := runningServiceForTest(t, edge)
	restartedEdge.peerMu.RLock()
	meta := restartedEdge.persistedMeta[fullAddress]
	var loadedIdentity domain.PeerIdentity
	if meta != nil {
		loadedIdentity = meta.Identity
	}
	restartedEdge.peerMu.RUnlock()
	if loadedIdentity != fullIdentity {
		t.Fatalf("restarted edge loaded identity %s for %s from its peers file, want %s", loadedIdentity, fullAddress, fullIdentity)
	}
}

func peerStateNames(state peerStateFile, address domain.PeerAddress, id domain.PeerIdentity) bool {
	for _, entry := range state.Peers {
		if entry.Address == address && entry.Identity == id {
			return true
		}
	}
	return false
}

// The address is reserved once and bound again on every restart; if
// something else took it in between, the restart is a recorded outcome of
// its own — never a node "ready" on somebody else's listener.
func testLoadStandNodeRestartOnTakenAddressIsAddressInUse(t *testing.T) {
	node := newFullNodeForTest(t, newLoadStandPortRegistry(reserveLoopbackAddress), "taken", loopbackOpts())
	startLoadStandNodeForTest(t, node)
	stopLoadStandNodeForTest(t, node)

	address, _ := node.DialAddress()
	foreign, err := net.Listen("tcp", string(address))
	if err != nil {
		t.Fatalf("take the node's address: %v", err)
	}

	err = node.Start(context.Background())
	if !errors.Is(err, errLoadStandAddressInUse) {
		t.Fatalf("Start on a taken address = %v, want errLoadStandAddressInUse", err)
	}
	if err := node.Stop(withBudget(t, harnessGenerousBudget)); !errors.Is(err, errLoadStandAddressInUse) {
		t.Fatalf("Stop after the failed Start = %v, want errLoadStandAddressInUse", err)
	}

	if err := foreign.Close(); err != nil {
		t.Fatalf("release the address: %v", err)
	}
	startLoadStandNodeForTest(t, node)
}

func testLoadStandNodeRefusesChangedIdentityFile(t *testing.T) {
	node := newEdgeNodeForTest(t, "swapped-identity", loopbackOpts())
	other, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	if err := identity.Save(node.Config().IdentityPath, other); err != nil {
		t.Fatalf("overwrite identity file: %v", err)
	}

	if err := node.Start(context.Background()); !errors.Is(err, errLoadStandIdentityChanged) {
		t.Fatalf("Start with a different identity on disk = %v, want errLoadStandIdentityChanged", err)
	}
	if _, ok := node.Service(); ok {
		t.Fatal("a refused Start left a running service behind")
	}
}

// A node that does not stop inside its budget stays owned by the node:
// it cannot be started over the top of itself, and the stop can be retried.
func testLoadStandNodeStopOverBudgetKeepsNodeOwnedUntilRetried(t *testing.T) {
	node := newEdgeNodeForTest(t, "slow-stop", loopbackOpts())
	startLoadStandNodeForTest(t, node)

	release := make(chan struct{})
	// See TestRunServiceForTestStopReportsUnfinishedRunAsError: a loop that
	// ignores cancellation holds Run open until released.
	runningServiceForTest(t, node).goRunLoop(func() { <-release })

	err := node.Stop(withBudget(t, harnessExpiredBudget))
	requireStopStage(t, err, stopStageRunExit)

	if err := node.Start(context.Background()); !errors.Is(err, errLoadStandNodeRunning) {
		t.Fatalf("Start over an unfinished stop = %v, want errLoadStandNodeRunning", err)
	}

	close(release)
	stopLoadStandNodeForTest(t, node)
	if _, ok := node.Service(); ok {
		t.Fatal("a stopped node still reports a running service")
	}
	if err := node.Stop(withBudget(t, harnessGenerousBudget)); !errors.Is(err, errLoadStandNodeNotRunning) {
		t.Fatalf("second Stop = %v, want errLoadStandNodeNotRunning", err)
	}
	startLoadStandNodeForTest(t, node)
}

// CPU profiles of a many-node process are only attributable per node if
// every goroutine a node starts carries the node's label.
func testLoadStandNodeLabelsItsGoroutinesWithNodeID(t *testing.T) {
	const id loadStandNodeID = "labelled-edge-7"
	node := newEdgeNodeForTest(t, id, loopbackOpts())
	startLoadStandNodeForTest(t, node)

	var profile bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&profile, 1); err != nil {
		t.Fatalf("write goroutine profile: %v", err)
	}
	if want := `"node":"` + string(id) + `"`; !strings.Contains(profile.String(), want) {
		t.Fatalf("goroutine profile carries no %s label", want)
	}
}

func testLoadStandNodeBanCheckPassesOnACleanNode(t *testing.T) {
	node := newEdgeNodeForTest(t, "clean", loopbackOpts())
	if err := node.CheckBanFree(time.Now(), loadStandBanPolicy{}); !errors.Is(err, errLoadStandNodeNotRunning) {
		t.Fatalf("CheckBanFree on a stopped node = %v, want errLoadStandNodeNotRunning", err)
	}
	startLoadStandNodeForTest(t, node)

	if err := node.CheckBanFree(time.Now(), loadStandBanPolicy{}); err != nil {
		t.Fatalf("CheckBanFree on a fresh node = %v, want nil", err)
	}
}

// Every place a ban can live turns the check red: on 127.0.0.1 one ban cuts
// a node off from the whole stand, and a sample taken after it measures the
// ban, not the network.
func testLoadStandBanCheckFailsOnAnyBan(t *testing.T) {
	const loopbackIP = "127.0.0.1"
	const peer = domain.PeerAddress("127.0.0.1:40000")

	injections := map[loadStandBanKind]func(svc *Service, until time.Time){
		loadStandBanScore: func(svc *Service, _ time.Time) {
			svc.ipStateMu.Lock()
			svc.bans[loopbackIP] = banEntry{Score: 1}
			svc.ipStateMu.Unlock()
		},
		loadStandBanBlacklist: func(svc *Service, until time.Time) {
			svc.ipStateMu.Lock()
			svc.bans[loopbackIP] = banEntry{Score: banThreshold, Blacklisted: until}
			svc.ipStateMu.Unlock()
		},
		loadStandBanIPWide: func(svc *Service, until time.Time) {
			svc.ipStateMu.Lock()
			svc.bannedIPSet[loopbackIP] = domain.BannedIPEntry{IP: loopbackIP, BannedUntil: until}
			svc.ipStateMu.Unlock()
		},
		loadStandBanRemoteIPWide: func(svc *Service, until time.Time) {
			svc.ipStateMu.Lock()
			svc.remoteBannedIPs[loopbackIP] = remoteIPBanEntry{Until: until}
			svc.ipStateMu.Unlock()
		},
		loadStandBanPeer: func(svc *Service, until time.Time) {
			svc.peerMu.Lock()
			svc.health[peer] = &peerHealth{Address: peer, BannedUntil: until}
			svc.peerMu.Unlock()
		},
		loadStandBanRemotePeer: func(svc *Service, until time.Time) {
			svc.peerMu.Lock()
			svc.persistedMeta[peer] = &peerEntry{Address: peer, RemoteBannedUntil: &until}
			svc.peerMu.Unlock()
		},
	}
	for kind, inject := range injections {
		t.Run(string(kind), func(t *testing.T) {
			svc := newHarnessProbeService(t, domain.NodeTypeClient)
			now := time.Now()
			if err := checkLoadStandBanFree(svc, now, loadStandBanPolicy{}); err != nil {
				t.Fatalf("check before the ban = %v, want nil", err)
			}

			inject(svc, now.Add(time.Hour))

			err := checkLoadStandBanFree(svc, now, loadStandBanPolicy{})
			if !errors.Is(err, errLoadStandBansPresent) {
				t.Fatalf("check after a %s = %v, want errLoadStandBansPresent", kind, err)
			}
			var present *loadStandBansPresentError
			if !errors.As(err, &present) || len(present.Findings) != 1 || present.Findings[0].Kind != kind {
				t.Fatalf("findings = %+v, want exactly one %s", present, kind)
			}
		})
	}
}

// The OS may hand a just-released port out again; a port two nodes believe
// is theirs makes one of them "ready" on the other's listener.
func testLoadStandPortRegistryNeverIssuesAPortTwice(t *testing.T) {
	const first, second = domain.ListenAddress("127.0.0.1:41001"), domain.ListenAddress("127.0.0.1:41002")
	answers := []domain.ListenAddress{first, first, second}
	registry := newLoadStandPortRegistry(func() (domain.ListenAddress, error) {
		next := answers[0]
		answers = answers[1:]
		return next, nil
	})

	got := []domain.ListenAddress{}
	for range 2 {
		address, err := registry.Reserve()
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		got = append(got, address)
	}
	if got[0] != first || got[1] != second {
		t.Fatalf("reserved %v, want [%s %s]: the re-issued port must be skipped", got, first, second)
	}

	stuck := newLoadStandPortRegistry(func() (domain.ListenAddress, error) { return first, nil })
	if _, err := stuck.Reserve(); err != nil {
		t.Fatalf("first Reserve from a stuck allocator: %v", err)
	}
	if _, err := stuck.Reserve(); !errors.Is(err, errLoadStandPortExhausted) {
		t.Fatalf("Reserve from an allocator that only repeats = %v, want errLoadStandPortExhausted", err)
	}
}

// A node that saw its own identity at an address stops dialling THAT address
// for a while. PeerProvider does not widen it to the IP, so it cuts nothing
// else off: it is reported, but it invalidates a sample only when the stand
// says so.
func testLoadStandSelfIdentityCooldownIsNotABanByDefault(t *testing.T) {
	const peer = domain.PeerAddress("127.0.0.1:40001")
	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	now := time.Now()
	svc.peerMu.Lock()
	svc.health[peer] = &peerHealth{Address: peer, BannedUntil: now.Add(time.Hour), LastErrorCode: protocol.ErrCodeSelfIdentity}
	svc.peerMu.Unlock()

	findings := loadStandBanFindings(svc, now)
	if len(findings) != 1 || findings[0].Kind != loadStandSelfIdentityCooldown {
		t.Fatalf("findings = %+v, want exactly one %s", findings, loadStandSelfIdentityCooldown)
	}
	if err := checkLoadStandBanFree(svc, now, loadStandBanPolicy{}); err != nil {
		t.Fatalf("default policy = %v, want nil: a self-identity cooldown is not a ban", err)
	}
	strict := loadStandBanPolicy{SelfIdentityCooldownInvalidates: true}
	if err := checkLoadStandBanFree(svc, now, strict); !errors.Is(err, errLoadStandBansPresent) {
		t.Fatalf("strict policy = %v, want errLoadStandBansPresent", err)
	}
}

// A relative path resolves against the working directory — outside the node
// directory — and would put a node's files wherever the test happens to run.
func testLoadStandNodeRefusesRelativeConfigPath(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Node{ChatLogDir: dir, PeersStatePath: "peers.json"}

	err := requireConfigPathsInside(cfg, dir)
	if !errors.Is(err, errLoadStandPathOutsideDir) || !strings.Contains(err.Error(), "PeersStatePath") {
		t.Fatalf("requireConfigPathsInside = %v, want errLoadStandPathOutsideDir naming PeersStatePath", err)
	}
}

// The main profile is "full nodes serve the network, edges receive DMs".
// config.Default().Node accepts DMs; the headless binary turns that off
// through its own wiring (CORSA_ACCEPT_DM), which the stand bypasses — so the
// role states the policy and the config must carry it.
func testLoadStandNodeDirectMessagePolicyFollowsTheRole(t *testing.T) {
	full := newFullNodeForTest(t, newLoadStandPortRegistry(reserveLoopbackAddress), "full-dm", loopbackOpts())
	edge := newEdgeNodeForTest(t, "edge-dm", loopbackOpts())
	if !full.Config().DisableDirectMessages {
		t.Error("full role accepts direct messages; the main profile's full nodes must refuse them as recipients")
	}
	if edge.Config().DisableDirectMessages {
		t.Error("edge role refuses direct messages; edges are the profile's DM recipients")
	}
	for role := range loadStandRoleNodeTypes {
		if _, declared := loadStandRoleAcceptsDirectMessages[role]; !declared {
			t.Errorf("role %s declares no direct-message policy", role)
		}
	}
}
