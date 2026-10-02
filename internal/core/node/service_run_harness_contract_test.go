package node

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// harnessExpiredBudget is short enough that a stop which is being held open
// on purpose can never fit into it, and long enough that the select has a
// real deadline to wait on rather than an already-closed channel.
const harnessExpiredBudget = 50 * time.Millisecond

// harnessGenerousBudget bounds the waits that are expected to succeed; it is
// the same 5 s startTestService allows a node to unwind in.
const harnessGenerousBudget = 5 * time.Second

func newHarnessProbeService(t *testing.T, nodeType domain.NodeType) *Service {
	t.Helper()

	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	// ChatLogDir is pinned so the file-transfer store Run creates lands in
	// the test's own directory instead of the user's application data dir.
	cfg := deriveTestAdvertisePort(config.Node{
		ListenAddress:     freeAddress(t),
		Type:              nodeType,
		PeersStatePath:    filepath.Join(dir, "peers.json"),
		ChatLogDir:        dir,
		AllowPrivatePeers: true,
	})
	svc := NewService(cfg, id, nil)
	svc.disableRateLimiting = true
	return svc
}

func requireStopStage(t *testing.T, err error, want stopStage) {
	t.Helper()

	if !errors.Is(err, errStopBudgetExceeded) {
		t.Fatalf("error = %v, want one matching errStopBudgetExceeded", err)
	}
	var stageErr *stopStageTimeoutError
	if !errors.As(err, &stageErr) {
		t.Fatalf("error = %v, want a *stopStageTimeoutError", err)
	}
	if stageErr.Stage != want {
		t.Fatalf("stage = %q, want %q", stageErr.Stage, want)
	}
}

func withBudget(t *testing.T, budget time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	t.Cleanup(cancel)
	return ctx
}

// A stop that does not fit its budget is a measurement the load stand has to
// record, not a reason to kill the process; the same node must still be
// stoppable once whatever held it open lets go.
func TestRunServiceForTestStopReportsUnfinishedRunAsError(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	release := make(chan struct{})
	// A lifecycle loop that ignores cancellation holds stopRunLifecycle's
	// join open, so Run cannot return until the test says so — a hang made
	// deterministic instead of waited for.
	svc.goRunLoop(func() { <-release })

	running := runServiceForTest(context.Background(), svc)

	err := running.Stop(withBudget(t, harnessExpiredBudget))
	requireStopStage(t, err, stopStageRunExit)

	close(release)
	if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("retried Stop = %v, want nil once the blocking loop is released", err)
	}
	if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("DrainBackground = %v, want nil", err)
	}
}

func TestRunServiceForTestDrainReportsUnfinishedBackgroundAsError(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	running := runServiceForTest(context.Background(), svc)
	if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	release := make(chan struct{})
	svc.goBackground(func() { <-release })

	err := running.DrainBackground(withBudget(t, harnessExpiredBudget))
	requireStopStage(t, err, stopStageBackgroundDrain)

	close(release)
	if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("retried DrainBackground = %v, want nil once the job is released", err)
	}
}

// Run failing before it ever listens must surface as THAT failure at once,
// not as a listener that silently never came up within the readiness budget.
func TestRunServiceForTestAwaitReadyReportsEarlyRunExit(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeFull)
	runFailure := errors.New("injected connection budget failure")
	// Run's first statement refuses a contradictory connection budget, which
	// makes it return before the listener branch is reached.
	svc.connBudgetErr = runFailure

	running := runServiceForTest(context.Background(), svc)

	err := running.AwaitReady(withBudget(t, harnessGenerousBudget))
	if !errors.Is(err, errServiceExitedBeforeReady) {
		t.Fatalf("AwaitReady = %v, want errServiceExitedBeforeReady", err)
	}
	if !errors.Is(err, runFailure) {
		t.Fatalf("AwaitReady = %v, want it to carry Run's own error", err)
	}

	stopErr := running.Stop(withBudget(t, harnessGenerousBudget))
	if !errors.Is(stopErr, runFailure) {
		t.Fatalf("Stop = %v, want Run's own error", stopErr)
	}
	if errors.Is(stopErr, errStopBudgetExceeded) {
		t.Fatalf("Stop = %v, a node that exited is not a stop that ran out of budget", stopErr)
	}
}

func neverOwnsListener() bool { return false }

func TestAwaitListenerAcceptsReportsListenerThatNeverAccepts(t *testing.T) {
	t.Parallel()

	// freeAddress hands back a port nobody is bound to, and the never-closed
	// exit channel stands for a Run that is still alive: the shape of a node
	// wedged before its listener came up.
	stillRunning := make(chan struct{})

	err := awaitListenerAccepts(withBudget(t, harnessExpiredBudget), freeAddress(t), stillRunning, neverOwnsListener)
	if !errors.Is(err, errServiceNotReady) {
		t.Fatalf("awaitListenerAccepts = %v, want errServiceNotReady", err)
	}
}

// A port handed out twice — the reservation closes its socket, so the OS may
// give it to someone else — puts ANOTHER listener at the node's address. A
// connection that listener accepts says nothing about this node.
func TestAwaitListenerAcceptsIgnoresListenerItDoesNotOwn(t *testing.T) {
	t.Parallel()

	foreign := listenForeign(t, "127.0.0.1:0")
	stillRunning := make(chan struct{})

	err := awaitListenerAccepts(withBudget(t, harnessExpiredBudget), foreign.Addr().String(), stillRunning, neverOwnsListener)
	if !errors.Is(err, errServiceNotReady) {
		t.Fatalf("awaitListenerAccepts = %v, want errServiceNotReady while the listener is not the node's", err)
	}
}

func listenForeign(t *testing.T, address string) net.Listener {
	t.Helper()

	foreign, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("bind foreign listener on %s: %v", address, err)
	}
	t.Cleanup(func() {
		// The listener only has to exist for the test's duration; a close
		// error after the test cannot change its outcome.
		_ = foreign.Close()
	})
	return foreign
}

func TestRunServiceForTestAwaitReadyRejectsForeignListener(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeFull)
	// The port freeAddress picked is taken by someone else before Run binds
	// it — exactly what a reused reservation produces.
	listenForeign(t, svc.cfg.ListenAddress)

	running := runServiceForTest(context.Background(), svc)

	err := running.AwaitReady(withBudget(t, harnessGenerousBudget))
	if !errors.Is(err, errServiceExitedBeforeReady) || !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("AwaitReady = %v, want errServiceExitedBeforeReady carrying EADDRINUSE", err)
	}
}

// The connection manager's event loop is up long before Run has primed the
// snapshots the hot local reads serve from; ready must mean the latter.
func TestRunServiceForTestAwaitReadyWaitsForPrimedHotReads(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	cmCtx, cmCancel := context.WithCancel(context.Background())
	cmDone := make(chan struct{})
	go func() {
		svc.connManager.Run(cmCtx)
		close(cmDone)
	}()
	t.Cleanup(func() {
		cmCancel()
		<-cmDone
	})
	<-svc.connManager.Ready()

	// No Service.Run: the event loop is ready, nothing was ever primed.
	running := &runningTestService{svc: svc, cancel: func() {}, exited: make(chan struct{})}

	err := running.AwaitReady(withBudget(t, harnessExpiredBudget))
	if !errors.Is(err, errServiceNotReady) {
		t.Fatalf("AwaitReady = %v, want errServiceNotReady before the hot reads are primed", err)
	}
}

// A Run that has already returned is a finished stop whatever budget the
// caller has left, including none at all.
func TestRunServiceForTestStopAfterRunExitIgnoresSpentBudget(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	running := runServiceForTest(context.Background(), svc)
	if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	spent, cancel := context.WithCancel(context.Background())
	cancel()
	if err := running.Stop(spent); err != nil {
		t.Fatalf("Stop with a spent budget after Run returned = %v, want nil", err)
	}
}

// A node without a listener has no socket to prove it is up, yet Run still
// initialises state (the capture manager among it) that local frames read.
// Readiness must be a happens-before edge with that initialisation, or the
// first frame a caller sends races it — which only -race can see.
func TestRunServiceForTestAwaitReadyOrdersEdgeStartBeforeLocalFrames(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	running := runServiceForTest(context.Background(), svc)
	t.Cleanup(func() {
		if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("AwaitReady: %v", err)
	}
	if reply := svc.HandleLocalFrame(protocol.Frame{Type: "fetch_peer_health"}); reply.Type != "peer_health" {
		t.Fatalf("fetch_peer_health reply = %q", reply.Type)
	}
}

func TestRunServiceForTestAwaitReadyReportsStartupThatNeverCompletes(t *testing.T) {
	t.Parallel()

	// No Run at all: the handle stands for a Run that is alive but has not
	// got through its start-up.
	running := &runningTestService{
		svc:    newHarnessProbeService(t, domain.NodeTypeClient),
		cancel: func() {},
		exited: make(chan struct{}),
	}

	err := running.AwaitReady(withBudget(t, harnessExpiredBudget))
	if !errors.Is(err, errServiceNotReady) {
		t.Fatalf("AwaitReady = %v, want errServiceNotReady", err)
	}
}

// A node reported ready and then stopped is not ready any more. Asked again,
// AwaitReady must say so — with or without a listener — and must not call
// the stop a failure to come up.
func TestRunServiceForTestAwaitReadyRefusesAStoppedNode(t *testing.T) {
	t.Parallel()

	nodeTypes := map[string]domain.NodeType{
		"with listener":    domain.NodeTypeFull,
		"without listener": domain.NodeTypeClient,
	}
	for name, nodeType := range nodeTypes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			svc := newHarnessProbeService(t, nodeType)
			if svc.cfg.EffectiveListenerEnabled() != (nodeType == domain.NodeTypeFull) {
				t.Fatalf("precondition: listener enabled = %v for %s", svc.cfg.EffectiveListenerEnabled(), nodeType)
			}
			running := runServiceForTest(context.Background(), svc)
			if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
				t.Fatalf("first AwaitReady: %v", err)
			}
			if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
				t.Fatalf("DrainBackground: %v", err)
			}

			err := running.AwaitReady(withBudget(t, harnessGenerousBudget))
			if !errors.Is(err, errServiceStoppedAfterReady) || errors.Is(err, errServiceExitedBeforeReady) {
				t.Fatalf("AwaitReady after Stop = %v, want errServiceStoppedAfterReady only", err)
			}
		})
	}
}
