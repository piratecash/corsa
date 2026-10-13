package node

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// errStopBudgetExceeded matches every stopStageTimeoutError, whatever stage
// ran out of time: callers that only need "the stop did not fit" ask for
// this, callers that need the stage use errors.As.
var errStopBudgetExceeded = errors.New("stop budget exceeded")

var (
	errServiceExitedBeforeReady = errors.New("service exited before it was ready")
	errServiceNotReady          = errors.New("service was not ready within budget")
	// errServiceStoppedAfterReady: Run has returned after this harness had
	// already reported the node ready — the node was stopped (or failed)
	// since, and a caller asking again is asking a node that no longer runs.
	errServiceStoppedAfterReady = errors.New("service has stopped since it was ready")
)

// stopStage names the step of a shutdown that ran out of budget, so a journal
// entry says WHAT was still running, not only that something was.
type stopStage string

const (
	// stopStageRunExit: Run had not returned — some lifecycle loop or
	// connection handler was still unwinding.
	stopStageRunExit stopStage = "run_exit"
	// stopStageBackgroundDrain: Run had returned, but fire-and-forget jobs
	// tracked by backgroundWg were still running.
	stopStageBackgroundDrain stopStage = "background_drain"
)

type stopStageTimeoutError struct {
	Stage stopStage
	Cause error
}

func (e *stopStageTimeoutError) Error() string {
	return fmt.Sprintf("%s: stage %s: %v", errStopBudgetExceeded, e.Stage, e.Cause)
}

func (e *stopStageTimeoutError) Is(target error) bool { return target == errStopBudgetExceeded }

func (e *stopStageTimeoutError) Unwrap() error { return e.Cause }

// runningTestService is one Run of one Service. It is owned by the goroutine
// that started it: Stop, DrainBackground and AwaitReady are not meant to
// be called from two goroutines at once, but each may be called again after
// it reported a budget overrun.
type runningTestService struct {
	svc    *Service
	cancel context.CancelFunc
	// exited is closed when Run returns; runErr is written before the close
	// and read only after it, so the channel is the only synchronisation the
	// pair needs and any number of readers may wait on it.
	exited chan struct{}
	runErr error
	// readyReported is set once AwaitReady has answered "ready", and read
	// only by AwaitReady — the owner's goroutine. It is what tells "Run
	// exited before it came up" from "Run exited after it was up".
	readyReported bool
}

// runServiceForTest is the core every test that runs a real Service goes
// through. It reports every way a run can go wrong as an ERROR rather than
// failing the test itself: startTestService turns those errors into t.Fatal
// for ordinary tests, while the load stand records them — a node that does not
// stop inside its budget is a measurement there, not a reason to abort the
// whole run.
func runServiceForTest(ctx context.Context, svc *Service) *runningTestService {
	runCtx, cancel := context.WithCancel(ctx)
	running := &runningTestService{
		svc:    svc,
		cancel: cancel,
		exited: make(chan struct{}),
	}
	go func() {
		running.runErr = svc.Run(runCtx)
		close(running.exited)
	}()
	return running
}

// readinessProbeInterval is how often AwaitReady looks at the node's state for
// the end of Run's start-up and for its bound listener; the wait is normally a
// few milliseconds. Each look is an atomic load or a short peerMu.RLock — no
// socket and no deadline of its own, so a probe goroutine that is scheduled
// late only answers late, never wrong.
const readinessProbeInterval = 5 * time.Millisecond

// AwaitReady returns once Run has finished its start-up and, for a node with a
// listener, once Run has bound that listener itself.
//
// Both are the node's own state, published by Run, and AwaitReady reads them;
// it measures nothing over the network. A dial to the node's address used to
// stand in for the listener check, with its own 200 ms deadline: under CPU
// starvation the probe goroutine woke after that deadline while the listener
// had already accepted the connection, and a node that was up was reported as
// not ready. The guarantee the dial gave is kept by the state: Run publishes
// s.listener only after net.Listen succeeded on the configured address, and
// from that moment the kernel accepts connections into the listen backlog,
// which Run's accept loop drains right after — a caller that dials once
// AwaitReady returned is connected. ctx is a guard against a node that hangs,
// not a deadline the start-up is expected to meet.
//
// "Start-up finished" is the publication of the routing snapshot, the last of
// the hot-read snapshots Run primes before it opens the listener. It is
// observed through that snapshot's atomic pointer, which makes it a
// happens-before edge with everything Run did before it: the capture manager
// that local frames read, the bootstrap priming, the other snapshots. Without
// that edge the first HandleLocalFrame races Run's start-up, and a hot read
// issued before priming answers from an empty snapshot.
//
// A node whose Run has returned is never ready, whatever it published while
// it ran: the routing snapshot and the bound listener outlive Run, so both
// probes would otherwise answer "ready" for a stopped node. Asked after it
// had already answered "ready", it reports errServiceStoppedAfterReady
// rather than errServiceExitedBeforeReady — the node did come up, and a
// caller that asks a stopped node is a different defect from a node that
// never started.
func (r *runningTestService) AwaitReady(ctx context.Context) error {
	err := r.awaitStartupAndListener(ctx)
	if err == nil {
		err = r.refuseIfExited()
	}
	if err != nil {
		return r.describeExit(err)
	}
	r.readyReported = true
	return nil
}

func (r *runningTestService) awaitStartupAndListener(ctx context.Context) error {
	if err := awaitRunStartup(ctx, r.svc, r.exited); err != nil {
		return err
	}
	if !r.svc.cfg.EffectiveListenerEnabled() {
		return nil
	}
	return awaitListenerBound(ctx, r.exited, r.svc.ownsBoundListenerForTest)
}

// refuseIfExited is the last word before a "ready" answer.
func (r *runningTestService) refuseIfExited() error {
	select {
	case <-r.exited:
		return errServiceExitedBeforeReady
	default:
		return nil
	}
}

// describeExit names a report that Run has exited by when it exited, and
// attaches Run's own error when there is one. Reading runErr is safe there:
// errServiceExitedBeforeReady is only ever returned after exited was
// observed closed.
func (r *runningTestService) describeExit(err error) error {
	if !errors.Is(err, errServiceExitedBeforeReady) {
		return err
	}
	if r.readyReported {
		err = errServiceStoppedAfterReady
	}
	if r.runErr != nil {
		return fmt.Errorf("%w: %w", err, r.runErr)
	}
	return err
}

func awaitRunStartup(ctx context.Context, svc *Service, exited <-chan struct{}) error {
	startupPublished := func() bool { return svc.routingSnap.Load() != nil }
	if err := awaitNodeState(ctx, exited, startupPublished); err != nil {
		return describeNotReady(err, "start-up not finished")
	}
	return nil
}

// ownsBoundListenerForTest reports whether Run has bound its listener. Run
// publishes s.listener under peerMu only after its own net.Listen on the
// configured address succeeded, so true means the node's socket is in the
// kernel's listen state and connections to it are queued for Run's accept
// loop; nothing another process binds at that address can make it true.
func (s *Service) ownsBoundListenerForTest() bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.listener != nil
}

// awaitListenerBound waits until ownsListener reports the node's OWN bound
// listener. A socket bound by somebody else on the node's address — a port
// reserved, released and handed to another process or stand node — never
// satisfies it: the node's Run fails its own net.Listen instead, which
// AwaitReady reports as an early exit.
func awaitListenerBound(ctx context.Context, exited <-chan struct{}, ownsListener func() bool) error {
	if err := awaitNodeState(ctx, exited, ownsListener); err != nil {
		return describeNotReady(err, "listener not bound")
	}
	return nil
}

// awaitNodeState polls reached until it holds, Run exits, or ctx ends.
func awaitNodeState(ctx context.Context, exited <-chan struct{}, reached func() bool) error {
	ticker := time.NewTicker(readinessProbeInterval)
	defer ticker.Stop()
	for !reached() {
		select {
		case <-exited:
			return errServiceExitedBeforeReady
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

// describeNotReady names the stage a budget ran out in; an early exit is
// passed through unchanged, describeExit owns its wording.
func describeNotReady(err error, stage string) error {
	if errors.Is(err, errServiceExitedBeforeReady) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", errServiceNotReady, stage, err)
}

// Stop cancels Run and waits for it to return within ctx. A nil error means
// Run returned cleanly; a stopStageTimeoutError means Run is STILL running
// and Stop may be called again; any other error is Run's own and final.
func (r *runningTestService) Stop(ctx context.Context) error {
	r.cancel()
	// An exit that has already happened wins over an expired budget, so a
	// retry after a timeout never reports a node that has in fact stopped.
	select {
	case <-r.exited:
		return r.exitResult()
	default:
	}
	select {
	case <-r.exited:
		return r.exitResult()
	case <-ctx.Done():
		return &stopStageTimeoutError{Stage: stopStageRunExit, Cause: ctx.Err()}
	}
}

func (r *runningTestService) exitResult() error {
	if r.runErr != nil {
		return fmt.Errorf("node run: %w", r.runErr)
	}
	return nil
}

// DrainBackground waits, within ctx, for the goroutines the Service keeps
// running past Run (trust-store writes, gossip fan-outs) so that a caller
// may delete the node's directory without racing an async disk write.
func (r *runningTestService) DrainBackground(ctx context.Context) error {
	return awaitWithinBudget(ctx, stopStageBackgroundDrain, r.svc.WaitBackground)
}

// awaitWithinBudget runs a blocking join and gives up waiting on it when ctx
// ends. The join itself cannot be interrupted, so on a timeout its goroutine
// keeps waiting and finishes on its own once whatever it joins does.
//
// Unlike Stop, it cannot first ask whether the join is already over: a
// sync.WaitGroup has no non-blocking probe. With a budget that is already
// spent on entry it may therefore report a timeout for a join that had
// nothing left to wait for: such a report means "not proven finished", not
// "stuck", and a caller that needs the difference passes a live budget.
func awaitWithinBudget(ctx context.Context, stage stopStage, join func()) error {
	done := make(chan struct{})
	go func() {
		join()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return &stopStageTimeoutError{Stage: stage, Cause: ctx.Err()}
	}
}

// goroutineDumpLimit caps the dump logGoroutinesOnStopOverrun writes. A -race
// run of a multi-node test holds thousands of goroutines; past this size the
// dump stops being something anyone reads, and the goroutine that holds the
// join is near the top in practice (runtime.Stack lists the caller first,
// then the rest in creation order).
const goroutineDumpLimit = 4 << 20

// logGoroutinesOnStopOverrun writes the stack of every goroutine to the test
// log when a stop ran out of its budget while Run was still running. That is
// the one moment the goroutine holding Run's teardown open can still be seen:
// once it lets go, nothing is left to say what it was waiting for.
func logGoroutinesOnStopOverrun(t testing.TB, err error) {
	t.Helper()

	var stageErr *stopStageTimeoutError
	if !errors.As(err, &stageErr) || stageErr.Stage != stopStageRunExit {
		return
	}
	logAllGoroutines(t, "Run still running after the stop budget")
}

// logAllGoroutines writes the stack of every goroutine to the test log,
// headed by what made it worth looking at.
func logAllGoroutines(t testing.TB, reason string) {
	t.Helper()

	buf := make([]byte, goroutineDumpLimit)
	n := runtime.Stack(buf, true)
	t.Logf("%s; all goroutines (%d bytes%s):\n%s",
		reason, n, truncationNote(n, len(buf)), buf[:n])
}

func truncationNote(written, capacity int) string {
	if written == capacity {
		return ", truncated"
	}
	return ""
}
