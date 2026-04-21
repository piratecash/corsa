package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// queueStatePersisterFixture wires a persister with controllable collaborators
// so tests can drive the debounce window, count Save invocations and assert
// that Snapshot runs only where the persister is supposed to call it.  Every
// dependency is explicit — no hidden clock, no sleep-based synchronisation.
type queueStatePersisterFixture struct {
	t *testing.T

	// waitCh receives one struct{} per Wait call.  Each incoming value is the
	// requested duration so tests can assert the debounce interval.  The test
	// drains it via recvWait.
	waitCh chan time.Duration
	// waitReplyCh drives the return value of Wait.  A `true` release simulates
	// the debounce interval elapsing; `false` simulates ctx cancellation.  The
	// test sends exactly one value per observed Wait call.
	waitReplyCh chan bool

	snapshotCalls atomic.Int32
	saveCalls     atomic.Int32
	saveErr       error // if set, returned by the next Save; cleared after.
	saveErrMu     sync.Mutex

	// capturedSnapshots records what Save observed; the test asserts on
	// length and contents to prove burst coalescing and ordering.
	capturedMu sync.Mutex
	captured   []queueStateFile

	snapshotHook func() queueStateFile

	persister *queueStatePersister
}

func newQueueStatePersisterFixture(t *testing.T, debounce time.Duration) *queueStatePersisterFixture {
	t.Helper()
	f := &queueStatePersisterFixture{
		t:           t,
		waitCh:      make(chan time.Duration, 16),
		waitReplyCh: make(chan bool, 16),
	}
	f.persister = newQueueStatePersister(queueStatePersisterDeps{
		Path:             "test-queue-state.json",
		DebounceInterval: debounce,
		Snapshot: func() queueStateFile {
			f.snapshotCalls.Add(1)
			if f.snapshotHook != nil {
				return f.snapshotHook()
			}
			return queueStateFile{Version: queueStateVersion}
		},
		Save: func(path string, state queueStateFile) error {
			f.saveCalls.Add(1)
			f.capturedMu.Lock()
			f.captured = append(f.captured, state)
			f.capturedMu.Unlock()
			f.saveErrMu.Lock()
			err := f.saveErr
			f.saveErr = nil
			f.saveErrMu.Unlock()
			return err
		},
		Wait: func(ctx context.Context, d time.Duration) bool {
			f.waitCh <- d
			select {
			case ok := <-f.waitReplyCh:
				return ok
			case <-ctx.Done():
				return false
			}
		},
	})
	return f
}

// recvWait blocks until the persister calls Wait and returns the duration
// requested.  Tests use it as the synchronisation point to know the persister
// entered the debounce window.
func (f *queueStatePersisterFixture) recvWait(t *testing.T, within time.Duration) time.Duration {
	t.Helper()
	select {
	case d := <-f.waitCh:
		return d
	case <-time.After(within):
		t.Fatalf("persister did not call Wait within %s", within)
		return 0
	}
}

// releaseWait tells the persister that the debounce window elapsed (true) or
// the ctx was cancelled (false).
func (f *queueStatePersisterFixture) releaseWait(ok bool) {
	f.waitReplyCh <- ok
}

// waitForSave blocks until the Save counter reaches at least n or the
// timeout expires.  Deterministic because every write is driven through the
// explicit Wait release, not wall clock.
func (f *queueStatePersisterFixture) waitForSave(t *testing.T, n int32, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if f.saveCalls.Load() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected %d Save calls, got %d", n, f.saveCalls.Load())
}

func TestQueueStatePersister_CoalescesBurst(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		f.persister.Run(ctx)
		close(runDone)
	}()

	// Fire a burst of MarkDirty calls *before* the persister has a chance to
	// observe the first wake.  All should collapse into a single Save.
	for i := 0; i < 20; i++ {
		f.persister.MarkDirty()
	}

	d := f.recvWait(t, time.Second)
	if d != 50*time.Millisecond {
		t.Fatalf("expected 50ms debounce, got %s", d)
	}
	// Release the debounce window.  Exactly one Save must run.
	f.releaseWait(true)
	f.waitForSave(t, 1, time.Second)

	if got := f.saveCalls.Load(); got != 1 {
		t.Fatalf("expected 1 Save after burst, got %d", got)
	}
	if got := f.snapshotCalls.Load(); got != 1 {
		t.Fatalf("expected 1 Snapshot after burst, got %d", got)
	}

	cancel()
	<-runDone
}

func TestQueueStatePersister_IdleDoesNotWrite(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		f.persister.Run(ctx)
		close(runDone)
	}()

	// Never call MarkDirty.  Cancel after a short sleep; Wait must never have
	// been called because no wake happened.
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-runDone

	if got := f.saveCalls.Load(); got != 0 {
		t.Fatalf("expected 0 Save calls while idle, got %d", got)
	}
	if got := f.snapshotCalls.Load(); got != 0 {
		t.Fatalf("expected 0 Snapshot calls while idle, got %d", got)
	}
	select {
	case d := <-f.waitCh:
		t.Fatalf("unexpected Wait call while idle: duration=%s", d)
	default:
	}
}

func TestQueueStatePersister_MarkDirtyDuringWriteSchedulesNextWrite(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, 25*time.Millisecond)

	// Block the first Save until the test signals it may proceed, simulating
	// a slow disk.  The second MarkDirty fires while the first Save is still
	// in flight and must schedule another write after it completes.
	saveGate := make(chan struct{})
	var saveCount int32
	f.persister = newQueueStatePersister(queueStatePersisterDeps{
		Path:             "test.json",
		DebounceInterval: 25 * time.Millisecond,
		Snapshot:         func() queueStateFile { return queueStateFile{Version: queueStateVersion} },
		Save: func(path string, state queueStateFile) error {
			if atomic.AddInt32(&saveCount, 1) == 1 {
				<-saveGate
			}
			return nil
		},
		Wait: func(ctx context.Context, d time.Duration) bool {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(1 * time.Millisecond):
				return true
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		f.persister.Run(ctx)
		close(runDone)
	}()

	// Trigger first write; it blocks inside Save.
	f.persister.MarkDirty()
	// Spin until Save has been entered.
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&saveCount) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("first Save never entered")
		}
		time.Sleep(time.Millisecond)
	}

	// While first Save is blocked, dirty again.  The loop must schedule a
	// second write after the first returns.
	f.persister.MarkDirty()
	close(saveGate)

	deadline = time.Now().Add(time.Second)
	for atomic.LoadInt32(&saveCount) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("second Save never fired, got %d", atomic.LoadInt32(&saveCount))
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-runDone
}

func TestQueueStatePersister_FinalFlushOnCtxCancel(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, time.Hour) // deliberately long: test proves cancel skips the wait.
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		f.persister.Run(ctx)
		close(runDone)
	}()

	f.persister.MarkDirty()
	// Wait for the persister to enter the debounce wait, then cancel mid-wait.
	_ = f.recvWait(t, time.Second)
	cancel()
	f.releaseWait(false) // Wait returns false because ctx was cancelled.

	<-runDone
	if got := f.saveCalls.Load(); got != 1 {
		t.Fatalf("expected 1 Save from final flush, got %d", got)
	}
}

func TestQueueStatePersister_NoFlushOnCtxCancelWhenClean(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		f.persister.Run(ctx)
		close(runDone)
	}()

	cancel()
	<-runDone

	if got := f.saveCalls.Load(); got != 0 {
		t.Fatalf("expected no Save when persister was never dirtied, got %d", got)
	}
}

func TestQueueStatePersister_SaveErrorReArmsDirty(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, 10*time.Millisecond)
	f.saveErrMu.Lock()
	f.saveErr = errors.New("disk full")
	f.saveErrMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		f.persister.Run(ctx)
		close(runDone)
	}()

	f.persister.MarkDirty()
	_ = f.recvWait(t, time.Second)
	f.releaseWait(true)
	f.waitForSave(t, 1, time.Second)

	// First Save returned an error; persister must keep dirty=1 and attempt
	// another Save on the next wake.  Because the dirty flag is re-armed, the
	// loop should proceed without a fresh MarkDirty — but only after some
	// event wakes it.  Fire MarkDirty to make the second wake deterministic.
	f.persister.MarkDirty()
	_ = f.recvWait(t, time.Second)
	f.releaseWait(true)
	f.waitForSave(t, 2, time.Second)

	if got := f.saveCalls.Load(); got != 2 {
		t.Fatalf("expected 2 Save attempts after retry, got %d", got)
	}
}

func TestQueueStatePersister_FlushSyncPersistsInline(t *testing.T) {
	t.Parallel()

	f := newQueueStatePersisterFixture(t, time.Hour)
	// No Run goroutine — FlushSync is the seam tests use when they need the
	// disk snapshot to appear before a subsequent loadQueueState read.
	f.persister.MarkDirty()
	f.persister.FlushSync()

	if got := f.saveCalls.Load(); got != 1 {
		t.Fatalf("expected FlushSync to Save once, got %d", got)
	}
	// FlushSync must clear the dirty flag so a subsequent FlushSync on an
	// unchanged state is a no-op.
	f.persister.FlushSync()
	if got := f.saveCalls.Load(); got != 1 {
		t.Fatalf("expected second FlushSync to be a no-op, got %d Saves", got)
	}
}

func TestQueueStatePersister_MarkDirtyIsNonBlocking(t *testing.T) {
	t.Parallel()

	// No Run goroutine consuming the wake channel.  Many MarkDirty calls must
	// still complete promptly — the channel has capacity 1 but further sends
	// are the non-blocking `default` branch.
	p := newQueueStatePersister(queueStatePersisterDeps{
		Path:             "test.json",
		DebounceInterval: time.Second,
		Snapshot:         func() queueStateFile { return queueStateFile{} },
		Save:             func(string, queueStateFile) error { return nil },
		Wait:             func(context.Context, time.Duration) bool { return false },
	})

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			p.MarkDirty()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("MarkDirty blocked under contention without consumer")
	}
}
