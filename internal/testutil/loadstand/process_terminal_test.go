package loadstand

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func terminalRPC(t *testing.T) *RPCClient {
	t.Helper()
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewRPCClient(RPCOptions{Address: domain.ListenAddress("127.0.0.1:1"), Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func TestProcessStopDeadlineDoesNotBlockOnHeldSignalGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var forces atomic.Int32
		signaler := NewMockprocessSignaler(t)
		signaler.EXPECT().Signal(syscall.SIGTERM).RunAndReturn(func(os.Signal) error { close(entered); <-release; return nil }).Once()
		signaler.EXPECT().Kill().RunAndReturn(func() error { forces.Add(1); return os.ErrProcessDone }).Maybe()
		control := newProcessController(t.Context(), signaler)
		p := &Process{done: make(chan struct{}), terminal: control, rpc: terminalRPC(t)}
		firstCtx, firstCancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer firstCancel()
		first := make(chan error, 1)
		go func() { first <- p.Stop(firstCtx) }()
		<-entered
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		second := make(chan error, 1)
		go func() { second <- p.Stop(ctx) }()
		time.Sleep(3*time.Second + time.Nanosecond)
		synctest.Wait()
		select {
		case err := <-second:
			if !errors.Is(err, ErrStopUnconfirmed) {
				t.Error("lost bounded unconfirmed outcome", err)
			}
		default:
			t.Error("Stop blocked past deadline and cleanup on held signal gate")
		}
		if forces.Load() != 0 {
			t.Error("force claimed/performed while Signal still held")
		}
		select {
		case <-p.Done():
			t.Error("published before admitted syscall returned")
		default:
		}
		if _, err := p.ExitResult(); !errors.Is(err, ErrFinalAccountingUnavailable) {
			t.Error("unsealed result escaped", err)
		}
		close(release)
		synctest.Wait()
		facts := control.reap()
		control.publish(p, ProcessExitResult{Kind: WaitNormal, Signals: facts})
		synctest.Wait()
		<-first
		result, err := p.ExitResult()
		if err != nil {
			t.Fatal(err)
		}
		if result.Signals.ForceAttempted != (forces.Load() != 0) || result.Clean() {
			t.Fatal("fabricated force/clean outcome", result)
		}
		previous := forces.Load()
		_ = p.Stop(t.Context())
		if forces.Load() != previous {
			t.Fatal("signal after Reaped")
		}
		result.Usage.Value.HighWaterRSS = 999
		metric := result.Usage.Metric()
		if metric.Value != nil {
			metric.Value.HighWaterRSS = 888
		}
		again, _ := p.ExitResult()
		if again.Usage.Value.HighWaterRSS != 0 {
			t.Fatal("mutable sealed exit alias")
		}
	})
}
func TestOwnedWatcherPreservesFiveSecondLifetimeEscalation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		term, force := make(chan struct{}), make(chan struct{})
		signaler := NewMockprocessSignaler(t)
		signaler.EXPECT().Signal(syscall.SIGTERM).RunAndReturn(func(os.Signal) error { close(term); return nil }).Once()
		signaler.EXPECT().Kill().RunAndReturn(func() error { close(force); return nil }).Once()
		ctx, cancel := context.WithCancel(t.Context())
		control := newProcessController(ctx, signaler)
		cancel()
		<-term
		time.Sleep(5*time.Second - time.Nanosecond)
		synctest.Wait()
		select {
		case <-force:
			t.Fatal("early automatic escalation")
		default:
		}
		time.Sleep(time.Nanosecond)
		<-force
		facts := control.reap()
		control.publish(&Process{done: make(chan struct{})}, ProcessExitResult{Kind: WaitNormal, Signals: facts})
		if !facts.TermAttempted || !facts.ForceAttempted || facts.ForceCause&StopLifetime == 0 {
			t.Fatal("unrecorded automatic signal", facts)
		}
	})
}

// Unlike virtual-time tests, this also runs a concurrent waiter blocked on the
// real terminal mutex. Stop must remain bounded while neither waiter nor
// watcher can complete; the owner is retained until that admitted call returns.
func TestProcessHeldSignalAndConcurrentReapRetainBoundedOwnership(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	signaler := NewMockprocessSignaler(t)
	signaler.EXPECT().Signal(syscall.SIGTERM).RunAndReturn(func(os.Signal) error { close(entered); <-release; return nil }).Once()
	signaler.EXPECT().Kill().Return(os.ErrProcessDone).Maybe()
	c := newProcessController(t.Context(), signaler)
	p := &Process{done: make(chan struct{}), terminal: c, rpc: terminalRPC(t)}
	c.submit(StopExplicit, false, time.Time{}, false)
	<-entered
	reaping := make(chan struct{})
	go func() {
		close(reaping)
		facts := c.reap()
		c.publish(p, ProcessExitResult{Kind: WaitNormal, Signals: facts})
	}()
	<-reaping
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(ctx) }()
	var stopErr error
	select {
	case stopErr = <-stopped:
	case <-time.After(3 * time.Second):
		close(release)
		<-p.done
		<-stopped
		t.Fatal("Stop deadline blocked by admitted Signal/waiter terminal gate")
	}
	if !errors.Is(stopErr, ErrStopUnconfirmed) {
		t.Error("termination incorrectly confirmed", stopErr)
	}
	select {
	case <-p.done:
		t.Error("waiter published while admitted signal held")
	default:
	}
	if _, err := p.ExitResult(); !errors.Is(err, ErrFinalAccountingUnavailable) {
		t.Error("unsealed result", err)
	}
	close(release)
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("waiter/watcher failed to join")
	}
	r, err := p.ExitResult()
	if err != nil || !r.Signals.StopUnconfirmed || !r.Signals.ForceRequested || r.Clean() {
		t.Fatal("lost immutable incomplete stop evidence", r, err)
	}
}

func TestUnconfirmedStopBeforePublicationIsPartOfSealedOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		signaler := NewMockprocessSignaler(t)
		c := newProcessController(t.Context(), signaler)
		p := &Process{terminal: c, done: make(chan struct{}), rpc: terminalRPC(t)}
		facts := c.reap() // kernel wait returned, parent close/publication still pending
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		stopped := make(chan error, 1)
		go func() { stopped <- p.Stop(ctx) }()
		time.Sleep(3*time.Second + time.Nanosecond)
		synctest.Wait()
		if err := <-stopped; !errors.Is(err, ErrStopUnconfirmed) {
			t.Fatal(err)
		}
		c.publish(p, ProcessExitResult{Kind: WaitNormal, Signals: facts})
		r, err := p.ExitResult()
		if err != nil || !r.Signals.StopUnconfirmed || r.Clean() {
			t.Fatal("late publication erased prior unconfirmed Stop", r, err)
		}
	})
}

// Reaped may win before the watcher consumes an already-fired deadline. The
// known deadline is frozen evidence, while no Kill syscall is invented.
func TestReapSealsExpiredKnownDeadlineWithoutInventingSignal(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "future", true: "expired"}[expired], func(t *testing.T) {
			deadline := time.Now().Add(time.Hour)
			if expired {
				deadline = time.Now().Add(-time.Nanosecond)
			}
			joined := make(chan struct{})
			close(joined)
			// The actual watcher is deliberately settled just before its deadline
			// select. This is a reachable terminal state independent of scheduling.
			c := &processController{stop: make(chan struct{}), watcherDone: joined, requests: processRequests{term: true, deadline: deadline, deadlineCause: StopLifetime}}
			facts := c.reap()
			r := ProcessExitResult{Kind: WaitNormal, Signals: facts}
			if facts.ForceAttempted || facts.ForceRequested != expired || r.Clean() == expired {
				t.Fatal("deadline lost or fabricated Kill", facts, r.Clean())
			}
			if expired && facts.ForceCause&StopLifetime == 0 {
				t.Fatal("lost expiration cause", facts)
			}
		})
	}
}
func TestOwnedWatcherHeldSignalCannotEraseExpiredLifetimeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		signaler := NewMockprocessSignaler(t)
		signaler.EXPECT().Signal(syscall.SIGTERM).RunAndReturn(func(os.Signal) error { close(entered); <-release; return nil }).Once()
		signaler.EXPECT().Kill().Return(os.ErrProcessDone).Maybe()
		ctx, cancel := context.WithCancel(t.Context())
		c := newProcessController(ctx, signaler)
		cancel()
		<-entered
		time.Sleep(5*time.Second + time.Nanosecond)
		close(release)
		facts := c.reap()
		p := &Process{done: make(chan struct{})}
		c.publish(p, ProcessExitResult{Kind: WaitNormal, Signals: facts})
		r, err := p.ExitResult()
		if err != nil || !r.Signals.ForceRequested || r.Clean() {
			t.Fatal("expired automatic deadline became clean exit", r, err)
		}
	})
}
