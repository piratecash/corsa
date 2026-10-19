package loadstand

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// processSignaler belongs to one owned command and is never a public hook.
type processSignaler interface {
	Signal(os.Signal) error
	Kill() error
}
type ProcessStopCause uint8

const (
	StopExplicit ProcessStopCause = 1 << iota
	StopLifetime
	StopDeadline
)

type SignalResult uint8

const (
	SignalNotAttempted SignalResult = iota
	SignalSucceeded
	SignalAlreadyDone
	SignalFailed
)

type ProcessWaitKind uint8

const (
	WaitUnknown ProcessWaitKind = iota
	WaitNormal
	WaitSignaled
)

type ProcessExitCode int32
type ProcessSignal uint32

type ProcessSignalFacts struct {
	TermAttempted   bool
	ForceAttempted  bool
	ForceRequested  bool
	StopUnconfirmed bool
	TermCause       ProcessStopCause
	ForceCause      ProcessStopCause // Actual attempt cause, or requested cause if no syscall was admitted.
	TermResult      SignalResult
	ForceResult     SignalResult
}

// FinalUsageObservation is a value-only sealed fact. Projection to the older
// pointer-shaped ProcessMetric allocates a fresh value for every caller.
type ProcessUsageScope string

const OwnedCommandUsage ProcessUsageScope = "owned_command"

type FinalUsageObservation struct {
	Scope  ProcessUsageScope
	State  ProcessMetricState
	Reason ProcessUnavailableReason
	Value  ProcessFinalUsage
}

func (u FinalUsageObservation) Metric() ProcessMetric[ProcessFinalUsage] {
	if u.State == ProcessMetricMeasured && u.Reason == "" {
		return measuredProcessMetric(u.Value)
	}
	return unavailableProcessMetric[ProcessFinalUsage](u.Reason)
}

// ProcessExitResult contains no ProcessState, ExitError/error chain, mutable
// slices/maps or pointer-shaped metric. Only the sole waiter publishes it.
type ProcessExitResult struct {
	Kind             ProcessWaitKind
	Code             ProcessExitCode
	Signal           ProcessSignal
	WaitFailed       bool
	Signals          ProcessSignalFacts
	Usage            FinalUsageObservation
	Provenance       Provenance
	LogCloseFailed   bool
	ExtraCloseFailed bool
}

func (r ProcessExitResult) Clean() bool {
	return r.Kind == WaitNormal && r.Code == 0 && !r.WaitFailed && !r.Signals.ForceAttempted && !r.Signals.ForceRequested && !r.Signals.StopUnconfirmed && !r.LogCloseFailed && !r.ExtraCloseFailed && r.Signals.TermResult != SignalFailed && r.Signals.ForceResult != SignalFailed
}

var ErrOwnedWaitFailed = errors.New("owned process wait failed")
var ErrParentProcessClose = errors.New("parent process resource close failed")
var ErrProcessStopDeadline = errors.New("owned process stop deadline was reached")

type processExitStatusError struct {
	kind   ProcessWaitKind
	code   ProcessExitCode
	signal ProcessSignal
}

func (e processExitStatusError) Error() string {
	return fmt.Sprintf("owned command terminated (kind=%d code=%d signal=%d)", e.kind, e.code, e.signal)
}
func (r ProcessExitResult) Error() error {
	var err error
	if r.Kind != WaitNormal || r.Code != 0 {
		err = processExitStatusError{r.Kind, r.Code, r.Signal}
	}
	if r.WaitFailed && err == nil {
		err = ErrOwnedWaitFailed
	}
	if r.Signals.ForceAttempted {
		err = errors.Join(err, ErrForcedStop)
	} else if r.Signals.ForceRequested {
		err = errors.Join(err, ErrProcessStopDeadline)
	}
	if r.Signals.StopUnconfirmed {
		err = errors.Join(err, ErrStopUnconfirmed)
	}
	if r.LogCloseFailed || r.ExtraCloseFailed {
		err = errors.Join(err, ErrParentProcessClose)
	}
	if r.Signals.TermResult == SignalFailed || r.Signals.ForceResult == SignalFailed {
		err = errors.Join(err, ErrOwnedWaitFailed)
	}
	return err
}

type processRequests struct {
	term          bool
	force         bool
	cause         ProcessStopCause
	deadline      time.Time
	deadlineCause ProcessStopCause
	unconfirmed   bool
}
type processTerminalKind uint8

const (
	terminalRunning processTerminalKind = iota
	terminalReaped
	terminalPublished
)

// submitMu never spans a syscall or terminal gate. Public Stop only coalesces
// constant-size requests here and selects its context; a held Signal cannot
// obstruct its deadline. ONE watcher performs every actual signal.
type processController struct {
	submitMu      sync.Mutex
	requests      processRequests
	donePublished bool
	wake          chan struct{}
	stop          chan struct{}
	watcherDone   chan struct{}
	reaped        atomic.Bool
	terminalMu    sync.Mutex
	state         processTerminalKind
	facts         ProcessSignalFacts
	signaler      processSignaler
}

func newProcessController(ctx context.Context, signaler processSignaler) *processController {
	c := &processController{wake: make(chan struct{}, 1), stop: make(chan struct{}), watcherDone: make(chan struct{}), signaler: signaler}
	go c.watch(ctx)
	return c
}
func (c *processController) submit(cause ProcessStopCause, force bool, deadline time.Time, unconfirmed bool) {
	c.submitMu.Lock()
	if c.reaped.Load() {
		c.submitMu.Unlock()
		return
	}
	c.requests.term = true
	c.requests.cause |= cause
	c.requests.force = c.requests.force || force
	c.requests.unconfirmed = c.requests.unconfirmed || unconfirmed
	if !deadline.IsZero() && (c.requests.deadline.IsZero() || deadline.Before(c.requests.deadline)) {
		c.requests.deadline = deadline
		c.requests.deadlineCause = cause
	}
	c.submitMu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}
func (c *processController) snapshotRequests() processRequests {
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	return c.requests
}
func signalOutcome(err error) SignalResult {
	if err == nil {
		return SignalSucceeded
	}
	if errors.Is(err, os.ErrProcessDone) {
		return SignalAlreadyDone
	}
	return SignalFailed
}
func (c *processController) attempt(force bool, cause ProcessStopCause) {
	c.terminalMu.Lock()
	defer c.terminalMu.Unlock()
	if c.state != terminalRunning {
		return
	}
	if force {
		if c.facts.ForceAttempted {
			return
		}
		c.facts.ForceAttempted = true
		c.facts.ForceCause = cause
		c.facts.ForceResult = signalOutcome(c.signaler.Kill())
		return
	}
	if c.facts.TermAttempted {
		return
	}
	c.facts.TermAttempted = true
	c.facts.TermCause = cause
	c.facts.TermResult = signalOutcome(c.signaler.Signal(syscall.SIGTERM))
}
func (c *processController) watch(ctx context.Context) {
	defer close(c.watcherDone)
	lifetime := ctx.Done()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		request := c.snapshotRequests()
		if request.term {
			c.attempt(false, request.cause)
		}
		if request.force {
			c.attempt(true, request.cause)
		}
		var deadline <-chan time.Time
		if !request.force && !request.deadline.IsZero() {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(max(time.Until(request.deadline), time.Duration(0)))
			deadline = timer.C
		}
		select {
		case <-c.stop:
			return
		case <-lifetime:
			lifetime = nil
			c.submit(StopLifetime, false, time.Now().Add(5*time.Second), false)
		case <-c.wake:
		case <-deadline:
			// A deadline is a request, not evidence that Kill already happened.
			c.submit(request.deadlineCause, true, time.Time{}, false)
		}
	}
}
func (c *processController) reap() ProcessSignalFacts {
	c.terminalMu.Lock()
	c.state = terminalReaped
	c.reaped.Store(true)
	request := c.snapshotRequests()
	expired := !request.deadline.IsZero() && !time.Now().Before(request.deadline)
	c.facts.ForceRequested = request.force || expired
	if !c.facts.ForceAttempted && c.facts.ForceRequested {
		c.facts.ForceCause = request.cause
		if expired {
			c.facts.ForceCause = request.deadlineCause
		}
	}
	c.facts.StopUnconfirmed = request.unconfirmed
	facts := c.facts
	close(c.stop)
	c.terminalMu.Unlock()
	<-c.watcherDone
	return facts
}

// markUnconfirmed orders a timed-out Stop against publication without taking
// the potentially held signal gate. Once Done is committed it wins the race.
func (c *processController) markUnconfirmed() bool {
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	if c.donePublished {
		return false
	}
	c.requests.unconfirmed = true
	return true
}
func (c *processController) publish(p *Process, result ProcessExitResult) {
	c.terminalMu.Lock()
	c.state = terminalPublished
	c.terminalMu.Unlock()
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	result.Signals.StopUnconfirmed = result.Signals.StopUnconfirmed || c.requests.unconfirmed
	p.exit = result
	c.donePublished = true
	close(p.done)
}
func (p *Process) ExitResult() (ProcessExitResult, error) {
	select {
	case <-p.done:
		return p.exit, nil
	default:
		return ProcessExitResult{}, ErrFinalAccountingUnavailable
	}
}
func (p *Process) waitError() error { return p.exit.Error() }
func startOwnedProcess(ctx context.Context, command *exec.Cmd, client *RPCClient, log io.Closer, provenance Provenance, artifacts *launchArtifacts, extraCloseFailed bool) *Process {
	p := &Process{command: command, done: make(chan struct{}), rpc: client, provenance: provenance, artifacts: artifacts}
	p.terminal = newProcessController(ctx, command.Process)
	go func() {
		waitErr := command.Wait()
		facts := p.terminal.reap()
		result := captureOwnedExit(command.ProcessState, waitErr != nil)
		result.Signals = facts
		result.Provenance = provenance
		result.ExtraCloseFailed = extraCloseFailed
		p.rpc.Close()
		if log != nil {
			result.LogCloseFailed = log.Close() != nil
		}
		p.terminal.publish(p, result)
	}()
	return p
}
