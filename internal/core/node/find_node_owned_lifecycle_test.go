package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// A worker already inside a call owns that call until it actually returns.
// Cancellation and closing its admission gate cannot stand in for the join.
func TestOwnedFindNodeLoopsJoinBeforeRunReturns(t *testing.T) {
	for _, early := range []bool{false, true} {
		name := "normal"
		if early {
			name = "early_refusal"
		}
		t.Run(name, func(t *testing.T) {
			svc := newHarnessProbeService(t, domain.NodeTypeClient)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var finished atomic.Int32
			if !svc.goOwnedFindNodeLoop(svc.runCtx.Done(), func() {
				close(entered)
				<-release
				finished.Add(1)
			}) {
				t.Fatal("original construction worker was refused before Run")
			}
			select {
			case <-entered:
			case <-time.After(harnessGenerousBudget):
				t.Fatal("admitted original worker did not enter")
			}
			if early {
				// Same pre-start failure used by the existing background gate tests.
				svc.connBudgetErr = errInjectedRunFailure
			}
			running := runServiceForTest(context.Background(), svc)
			t.Cleanup(func() {
				unblock()
				err := running.Stop(withBudget(t, harnessGenerousBudget))
				if err != nil && (!early || !errors.Is(err, errInjectedRunFailure)) {
					t.Error("cleanup Run", err)
				}
				if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
					t.Error("cleanup owned workers", err)
				}
			})
			if !early {
				if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
					t.Fatal(err)
				}
			}
			err := running.Stop(withBudget(t, harnessExpiredBudget))
			requireStopStage(t, err, stopStageRunExit)
			// A false early Run return must not be masked by a later background
			// drain: the timed Stop above is specifically waiting for Run.
			if finished.Load() != 0 {
				t.Fatal("worker finished before its original call was released")
			}
			var late atomic.Bool
			if svc.goOwnedFindNodeLoop(svc.runCtx.Done(), func() { late.Store(true) }) {
				t.Fatal("owned worker admitted after shutdown began")
			}
			unblock()
			err = running.Stop(withBudget(t, harnessGenerousBudget))
			if early {
				if !errors.Is(err, errInjectedRunFailure) {
					t.Fatalf("early refusal lost original error: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := running.DrainBackground(withBudget(t, harnessGenerousBudget)); err != nil {
				t.Fatal(err)
			}
			if finished.Load() != 1 || late.Load() {
				t.Fatal("owned worker was not joined exactly once or a late worker ran")
			}
		})
	}
}

func TestOwnedFindNodeLoopsNoRunJoinAndOriginalLifetime(t *testing.T) {
	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	foreign, cancelForeign := context.WithCancel(context.Background())
	defer cancelForeign()
	var foreignRan atomic.Bool
	if svc.goOwnedFindNodeLoop(foreign.Done(), func() { foreignRan.Store(true) }) {
		t.Fatal("foreign loop lifetime admitted into the original Service join")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	if !svc.goOwnedFindNodeLoop(svc.runCtx.Done(), func() { close(entered); <-release }) {
		t.Fatal("no-Run original worker refused")
	}
	select {
	case <-entered:
	case <-time.After(harnessGenerousBudget):
		t.Fatal("no-Run worker did not enter")
	}
	svc.runCancel()
	svc.closeBackgroundAdmission()
	joined := make(chan struct{})
	go func() { svc.WaitBackground(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("WaitBackground returned while no-Run worker still owned its call")
	case <-time.After(harnessExpiredBudget):
	}
	unblock()
	select {
	case <-joined:
	case <-time.After(harnessGenerousBudget):
		t.Fatal("no-Run worker was not joined after release")
	}
	if foreignRan.Load() {
		t.Fatal("foreign worker ran")
	}
	// Independently test the admission fence while the original context is
	// still live, so cancellation cannot hide a missing closed-gate check.
	fresh := newHarnessProbeService(t, domain.NodeTypeClient)
	defer fresh.runCancel()
	fresh.stopOwnedFindNodeLoops()
	if fresh.goOwnedFindNodeLoop(fresh.runCtx.Done(), func() { foreignRan.Store(true) }) {
		t.Fatal("closed owned-worker gate admitted a live-context worker")
	}
	fresh.closeBackgroundAdmission()
	fresh.WaitBackground()
}

func TestOwnedFindNodeLoopsRegistrationRacesShutdown(t *testing.T) {
	svc := newHarnessProbeService(t, domain.NodeTypeClient)
	start := make(chan struct{})
	var producers sync.WaitGroup
	var admitted, completed atomic.Int32
	for i := 0; i < 32; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			<-start
			if svc.goOwnedFindNodeLoop(svc.runCtx.Done(), func() {
				<-svc.runCtx.Done()
				completed.Add(1)
			}) {
				admitted.Add(1)
			}
		}()
	}
	stopped := make(chan struct{})
	go func() { <-start; svc.stopRunLifecycle(svc.runCancel); close(stopped) }()
	close(start)
	producers.Wait()
	select {
	case <-stopped:
	case <-time.After(harnessGenerousBudget):
		t.Fatal("registration/shutdown fence stranded an original worker")
	}
	if admitted.Load() != completed.Load() {
		t.Fatalf("admitted workers %d != joined completions %d", admitted.Load(), completed.Load())
	}
	if svc.goOwnedFindNodeLoop(svc.runCtx.Done(), func() { t.Error("late worker ran") }) {
		t.Fatal("closed owned-worker admission reopened")
	}
	svc.closeBackgroundAdmission()
	svc.WaitBackground()
}
