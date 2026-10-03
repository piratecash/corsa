package node

import (
	"context"
	"errors"
	"fmt"
	"net"
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

// listenerProbeInterval and listenerProbeDialTimeout are the cadence the
// readiness probe dials at; they are the values startTestService always used.
const (
	listenerProbeInterval    = 100 * time.Millisecond
	listenerProbeDialTimeout = 200 * time.Millisecond
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

// startupProbeInterval is how often AwaitReady looks for the end of Run's
// start-up; the wait is normally a few milliseconds.
const startupProbeInterval = 5 * time.Millisecond

// AwaitReady returns once Run has finished its start-up and, for a node with a
// listener, once that node's OWN listener accepts a TCP connection.
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
	return awaitListenerAccepts(ctx, r.svc.externalListenAddress(), r.exited, r.svc.ownsBoundListenerForTest)
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
	ticker := time.NewTicker(startupProbeInterval)
	defer ticker.Stop()
	for svc.routingSnap.Load() == nil {
		select {
		case <-exited:
			return errServiceExitedBeforeReady
		case <-ctx.Done():
			return fmt.Errorf("%w: start-up not finished: %w", errServiceNotReady, ctx.Err())
		case <-ticker.C:
		}
	}
	return nil
}

// ownsBoundListenerForTest reports whether Run has bound its listener. Run publishes
// it under peerMu only after net.Listen succeeded, so a connection accepted
// while this is false was accepted by somebody else's socket on the same
// address.
func (s *Service) ownsBoundListenerForTest() bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.listener != nil
}

// awaitListenerAccepts treats the node as listening only when a dial succeeds
// AND ownsListener confirms the socket is the node's own: a port reserved and
// released can be handed to another process — or another stand node — which
// would then answer for this one.
func awaitListenerAccepts(ctx context.Context, address string, exited <-chan struct{}, ownsListener func() bool) error {
	dialer := net.Dialer{Timeout: listenerProbeDialTimeout}
	ticker := time.NewTicker(listenerProbeInterval)
	defer ticker.Stop()
	for {
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err == nil {
			// The probe connection carries nothing; a failed close changes
			// nothing about whether the listener accepted it.
			_ = conn.Close()
			if ownsListener() {
				return nil
			}
		}
		select {
		case <-exited:
			return errServiceExitedBeforeReady
		case <-ctx.Done():
			return fmt.Errorf("%w: listener %s: %w", errServiceNotReady, address, ctx.Err())
		case <-ticker.C:
		}
	}
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
	buf := make([]byte, goroutineDumpLimit)
	n := runtime.Stack(buf, true)
	t.Logf("Run still running after the stop budget; all goroutines (%d bytes%s):\n%s",
		n, truncationNote(n, len(buf)), buf[:n])
}

func truncationNote(written, capacity int) string {
	if written == capacity {
		return ", truncated"
	}
	return ""
}
