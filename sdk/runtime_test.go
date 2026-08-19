package sdk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/rpc"
)

func TestRuntimeExecuteHelp(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = t.TempDir()
	cfg.Node.IdentityPath = filepath.Join(cfg.Node.ChatLogDir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(cfg.Node.ChatLogDir, "trust.json")
	cfg.Node.PeersStatePath = filepath.Join(cfg.Node.ChatLogDir, "peers.json")

	// SDK no longer auto-generates identity — create one for the test.
	if err := EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		t.Fatalf("EnsureIdentityFile() error = %v", err)
	}

	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	raw, err := runtime.Execute("help", nil)
	if err != nil {
		t.Fatalf("Execute(help) error = %v", err)
	}

	var result struct {
		Commands []CommandInfo `json:"commands"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal help result: %v", err)
	}

	if len(result.Commands) == 0 {
		t.Fatal("expected help to return commands")
	}

	hasSendDM := false
	for _, command := range result.Commands {
		if command.Name == "sendDm" {
			hasSendDM = true
			break
		}
	}
	if !hasSendDM {
		t.Fatal("expected SDK runtime to expose sendDm")
	}
}

func TestResolveIdentityFromPrivateKey(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Node.PrivateKey = base64.StdEncoding.EncodeToString(id.PrivateKey)
	cfg.Node.IdentityPath = "" // no file — PrivateKey takes priority

	resolved, err := resolveIdentity(cfg)
	if err != nil {
		t.Fatalf("resolveIdentity: %v", err)
	}
	if resolved.Address != id.Address {
		t.Fatalf("address = %q, want %q", resolved.Address, id.Address)
	}
}

func TestResolveIdentityFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")

	if err := EnsureIdentityFile(path); err != nil {
		t.Fatalf("EnsureIdentityFile: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Node.PrivateKey = ""
	cfg.Node.IdentityPath = path

	_, err := resolveIdentity(cfg)
	if err != nil {
		t.Fatalf("resolveIdentity: %v", err)
	}
}

func TestResolveIdentityFailsWithoutKeyOrFile(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Node.PrivateKey = ""
	cfg.Node.IdentityPath = filepath.Join(t.TempDir(), "nonexistent.json")

	_, err := resolveIdentity(cfg)
	if err == nil {
		t.Fatal("expected error when neither PrivateKey nor identity file provided")
	}
}

func TestEnsureIdentityFileCreatesAndReuses(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "identity.json")

	// First call creates the file.
	if err := EnsureIdentityFile(path); err != nil {
		t.Fatalf("first EnsureIdentityFile: %v", err)
	}

	first, err := identity.Load(path)
	if err != nil {
		t.Fatalf("Load after create: %v", err)
	}

	// Second call reuses existing file.
	if err := EnsureIdentityFile(path); err != nil {
		t.Fatalf("second EnsureIdentityFile: %v", err)
	}

	second, err := identity.Load(path)
	if err != nil {
		t.Fatalf("Load after reuse: %v", err)
	}

	if first.Address != second.Address {
		t.Fatalf("EnsureIdentityFile regenerated identity: %q vs %q", first.Address, second.Address)
	}
}

// TestNewReleasesStateDatabaseWhenALaterStepFails covers the composition
// root's error path: New opens the state database early, so every failure
// after that point must release it before returning.
//
// A leaked *sql.DB is invisible to the caller — but not to SQLite: the last
// connection closing is what checkpoints and removes the -wal sidecar, so its
// presence after a failed New is the leak.
func TestNewReleasesStateDatabaseWhenALaterStepFails(t *testing.T) {
	// Not parallel: the goroutine assertion below reads process-global state.

	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")

	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = dir
	cfg.Node.StateDBPath = statePath
	cfg.Node.IdentityPath = filepath.Join(dir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.Node.PeersStatePath = filepath.Join(dir, "peers.json")
	// Half-configured auth: rpc.NewServer rejects it, which is the step
	// after the database is already open.
	cfg.RPC.Enabled = true
	cfg.RPC.Username = "operator"
	cfg.RPC.Password = ""

	if err := EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		t.Fatalf("EnsureIdentityFile() error = %v", err)
	}

	subscriberBaseline := countBusGoroutines()

	runtime, err := New(cfg)
	if err == nil {
		_ = runtime.Close()
		t.Fatal("New() accepted a partially configured RPC auth")
	}
	if runtime != nil {
		t.Fatal("New() returned a runtime alongside an error")
	}

	if _, statErr := os.Stat(statePath); statErr != nil {
		t.Fatalf("the state database was not created at all: %v", statErr)
	}
	if _, statErr := os.Stat(statePath + "-wal"); statErr == nil {
		t.Fatal("the -wal sidecar is still present — the state database was not closed on the error path")
	}

	// The database is only half of it. By this point the status monitor holds
	// a dozen subscriptions, each with a goroutine of its own, and cancelling
	// the subscription context stops the producers rather than the bus — so a
	// caller retrying New with a corrected config used to accumulate them.
	if remaining := waitForBusGoroutines(subscriberBaseline); remaining > subscriberBaseline {
		t.Fatalf("%d event-bus goroutines are still running (baseline %d): the bus was not drained on the error path",
			remaining, subscriberBaseline)
	}
}

// countBusGoroutines counts the goroutines parked inside the event bus.
func countBusGoroutines() int {
	buffer := make([]byte, 1<<20)
	buffer = buffer[:goruntime.Stack(buffer, true)]
	return strings.Count(string(buffer), "internal/core/ebus.")
}

// waitForBusGoroutines gives the bus a moment to finish draining before the
// count is believed: Shutdown returns once the subscribers are told to stop,
// and their stacks disappear shortly after.
func waitForBusGoroutines(want int) int {
	remaining := countBusGoroutines()
	for attempt := 0; attempt < 50 && remaining > want; attempt++ {
		time.Sleep(100 * time.Millisecond)
		remaining = countBusGoroutines()
	}
	return remaining
}

// testRuntimeConfig is a self-contained runtime: its own data directory, an
// ephemeral loopback port and no bootstrap peers, so Start touches nothing
// outside the test.
func testRuntimeConfig(t *testing.T) Config {
	t.Helper()

	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = dir
	cfg.Node.StateDBPath = filepath.Join(dir, "state.db")
	cfg.Node.IdentityPath = filepath.Join(dir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(dir, "trust.json")
	cfg.Node.PeersStatePath = filepath.Join(dir, "peers.json")

	if err := EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		t.Fatalf("EnsureIdentityFile() error = %v", err)
	}
	return cfg
}

// TestCloseStopsTheNodeItselfBeforeReleasingTheDatabase covers the shutdown
// order for the caller that closes the runtime without cancelling the context
// it passed to Start — the ordinary way to shut a bot down.
//
// Close must drive the whole sequence itself: stop the router loops, cancel
// and join the node, then release SQLite. Without that, the database went away
// under a live router and node, and their next write hit a closed handle.
func TestCloseStopsTheNodeItselfBeforeReleasingTheDatabase(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Deliberately NOT cancellable: only Close can stop this runtime.
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Close() hung — a shutdown stage is not bounded")
	}

	// The node must have stopped as part of Close, not been left running.
	stopped := make(chan error, 1)
	go func() { stopped <- runtime.Wait() }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the node was still running after Close returned")
	}

	// SQLite deletes the -wal sidecar when the last connection closes, so its
	// absence is the proof the database was actually released.
	if _, err := os.Stat(cfg.Node.StateDBPath + "-wal"); err == nil {
		t.Fatal("the -wal sidecar survived Close — the database was not released")
	}
}

// TestCloseWithoutStartReleasesTheDatabase covers the caller that builds a
// runtime and abandons it: Close must not sit out the node timeouts waiting
// for goroutines that were never launched.
func TestCloseWithoutStartReleasesTheDatabase(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	started := time.Now()
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Close() on a never-started runtime took %s — it waited for a node that does not exist", elapsed)
	}
	if _, err := os.Stat(cfg.Node.StateDBPath + "-wal"); err == nil {
		t.Fatal("the -wal sidecar survived Close — the database was not released")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// TestStartAfterCloseIsRefused covers the lifecycle contract: Close releases
// the state database and the event bus, so a Start on top of them would put
// the node and router back to work against closed handles.
func TestStartAfterCloseIsRefused(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := runtime.Start(context.Background()); err == nil {
		t.Fatal("Start() on a closed runtime was accepted")
	}
}

// TestConcurrentStartAndCloseAreSerialized exercises the lifecycle lock: the
// two entry points are public, they hand each other the node's cancel
// function, and the runtime's own node goroutine calls Close. Run with -race
// this fails on an unsynchronised field.
func TestConcurrentStartAndCloseAreSerialized(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Either outcome is legal — started, or refused because Close won.
		_ = runtime.Start(context.Background())
	}()
	go func() {
		defer wg.Done()
		_ = runtime.Close()
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent Start/Close deadlocked")
	}

	// Whatever the interleaving, the runtime ends up closed exactly once.
	_ = runtime.Close()
	if err := runtime.Start(context.Background()); err == nil {
		t.Fatal("Start() after the dust settled was accepted on a closed runtime")
	}
}

// TestPublicOperationsAreRefusedAfterClose covers the SDK's own shutdown gate.
// The router gates its loops; nothing stopped an embedder from calling into
// the stack while the node was stopping and the database closing.
func TestPublicOperationsAreRefusedAfterClose(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := runtime.Execute("help", nil); err == nil {
		t.Fatal("Execute() was accepted after Close")
	}
	if _, err := runtime.ExecuteCommand("help"); err == nil {
		t.Fatal("ExecuteCommand() was accepted after Close")
	}
	if _, err := runtime.SendDirectMessage(context.Background(), strings.Repeat("b", 40), "hi"); err == nil {
		t.Fatal("SendDirectMessage() was accepted after Close")
	}

	stream := runtime.SubscribeDirectMessages(context.Background())
	select {
	case _, open := <-stream:
		if open {
			t.Fatal("SubscribeDirectMessages() delivered on a closed runtime")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SubscribeDirectMessages() on a closed runtime never finished")
	}
}

// TestCloseWaitsForInFlightOperations proves the gate is a drain and not just
// a flag: a call already inside the stack must finish before the database is
// released, or its write lands on a closed handle.
func TestCloseWaitsForInFlightOperations(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Occupy an operation slot the way a real in-flight call does.
	if !runtime.beginOperation() {
		t.Fatal("beginOperation() refused on a fresh runtime")
	}

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- runtime.Close() }()

	select {
	case <-closeReturned:
		t.Fatal("Close() released the runtime while an operation was still in flight")
	case <-time.After(300 * time.Millisecond):
	}

	runtime.endOperation()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close() did not finish after the operation completed")
	}
}

// TestCloseEndsALiveSubscription is the deadlock the operation gate would
// otherwise create: a stream started with a context the caller never cancels
// holds its lease until it exits, and it exits when its events channel closes
// — which only happens once it cancels its own subscription. Close waiting for
// operations first would wait forever, so it ends the streams itself.
func TestCloseEndsALiveSubscription(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Background on purpose: the caller has no handle to stop this stream.
	stream := runtime.SubscribeDirectMessages(context.Background())

	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v — a live subscription blocked the shutdown", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Close() hung on a live subscription")
	}

	select {
	case _, open := <-stream:
		if open {
			t.Fatal("the stream delivered after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close returned but the stream was never finished")
	}
}

// TestCloseReportsTheSameOutcomeToEveryCaller drives the RETRY branch: a stuck
// operation makes the shutdown time out, so Close returns a non-nil error,
// keeps the database open, and a later Close still completes the teardown once
// the straggler finishes.
func TestCloseReportsTheSameOutcomeToEveryCaller(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Occupy a slot so the operations drain cannot finish.
	if !runtime.beginOperation() {
		t.Fatal("beginOperation() refused on a fresh runtime")
	}

	first := runtime.Close()
	if first == nil {
		t.Fatal("Close() succeeded while an operation was stuck")
	}
	if second := runtime.Close(); second == nil || second.Error() != first.Error() {
		t.Fatalf("second Close() = %v, want the recorded %v", second, first)
	}
	if _, err := os.Stat(cfg.Node.StateDBPath + "-wal"); err != nil {
		t.Fatal("the database was released despite an unclean shutdown")
	}

	// The straggler finishes; the retry must now complete what timed out.
	runtime.endOperation()
	if err := runtime.Close(); err != nil {
		t.Fatalf("retried Close() error = %v", err)
	}
	if _, err := os.Stat(cfg.Node.StateDBPath + "-wal"); err == nil {
		t.Fatal("the retried Close did not release the database")
	}
	if again := runtime.Close(); again != nil {
		t.Fatalf("Close() after success = %v, want nil", again)
	}
}

// TestCloseEndsASubscriptionNobodyIsReading covers the outer wait: a stream
// parked on its event source, with a caller context that never ends, must
// still be released by Close.
// TestCloseReturnsTheRecordedErrorAfterATerminalFailure covers the one branch
// the retry test cannot reach: a shutdown that COMPLETED but whose
// database.Close failed. Every later caller must be told the same thing.
//
// The terminal state is seeded directly because a failing sql.DB.Close cannot
// be provoked through the public API — and without seeding it the assertion
// would pass against the version that returned a bare nil from that branch.
func TestCloseReturnsTheRecordedErrorAfterATerminalFailure(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.database.Close() })

	recorded := errors.New("release the state database: disk went away")
	runtime.lifecycle.Lock()
	runtime.stopping = true
	runtime.closed = true
	runtime.closeErr = recorded
	runtime.lifecycle.Unlock()

	if got := runtime.Close(); !errors.Is(got, recorded) {
		t.Fatalf("Close() = %v, want the recorded %v", got, recorded)
	}
}

func TestCloseEndsASubscriptionNobodyIsReading(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Background context, and the channel is deliberately never read.
	_ = runtime.SubscribeDirectMessages(context.Background())

	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v — an unread subscription blocked the shutdown", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Close() hung on an unread subscription")
	}
}

// TestStreamContextEndsFromEitherSide is the regression for the blocked-send
// path. That path parks the stream's goroutine inside `case out <- msg` with a
// full buffer, where the only escape is the context it selects on — so what
// has to be proven is that this context ends from BOTH sides.
//
// It is asserted here rather than end-to-end because filling the buffer needs
// inbound events from another node, which the SDK package cannot inject: the
// event source is the node's own local-change fan-out. The stream uses exactly
// this context at every blocking point (see SubscribeDirectMessages).
func TestStreamContextEndsFromEitherSide(t *testing.T) {
	t.Parallel()

	t.Run("caller cancels", func(t *testing.T) {
		t.Parallel()
		runtime, err := New(testRuntimeConfig(t))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		t.Cleanup(func() { _ = runtime.Close() })

		callerCtx, cancelCaller := context.WithCancel(context.Background())
		stream, stop := runtime.streamContext(callerCtx)
		defer stop()

		cancelCaller()
		select {
		case <-stream.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("the stream context ignored the caller's cancellation")
		}
	})

	t.Run("runtime shuts down", func(t *testing.T) {
		t.Parallel()
		runtime, err := New(testRuntimeConfig(t))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		// A caller context that never ends — the case that used to hang.
		stream, stop := runtime.streamContext(context.Background())
		defer stop()

		if err := runtime.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		select {
		case <-stream.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("the stream context survived the shutdown — a blocked send would never be released")
		}
	})
}

func TestWaitReturnsOnlyAfterTheDatabaseIsClosed(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := runtime.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start() error = %v", err)
	}
	cancel()

	waited := make(chan error, 1)
	go func() { waited <- runtime.Wait() }()

	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Wait() never returned")
	}

	// Checked with no delay at all: a caller returning from Wait is entitled
	// to exit the process right here, and the SDK example does exactly that.
	// The -wal sidecar is gone only once the last connection closed, so its
	// presence would mean the shutdown was still running.
	if _, statErr := os.Stat(cfg.Node.StateDBPath + "-wal"); statErr == nil {
		t.Fatal("Wait() returned while the state database was still open")
	}
}

func TestBackgroundWritersAreJoinedBeforeTheConsumers(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// The node's fire-and-forget jobs finish durable writes AND publish the
	// result, so they must be joined while the bus and the router are still
	// there to receive it.
	order := runtime.progress.completed
	position := func(stage string) int {
		for i, name := range order {
			if name == stage {
				return i
			}
		}
		t.Fatalf("stage %q never completed: %v", stage, order)
		return -1
	}

	node, background := position("node-run"), position("node-background")
	bus, router := position("event-bus"), position("router-drain")
	if node >= background || background >= bus || background >= router {
		t.Fatalf("shutdown order %v: background writers must join after the node and before the consumers", order)
	}
}

func TestWaitEndsForARuntimeClosedWithoutStart(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// A waiter that was already blocked when Close ran, and one that arrives
	// afterwards: neither has a node goroutine to report the outcome, and
	// Close is documented as safe without Start.
	early := make(chan error, 1)
	go func() { early <- runtime.Wait() }()
	time.Sleep(50 * time.Millisecond)

	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case err := <-early:
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a waiter blocked before Close never woke up")
	}

	late := make(chan error, 1)
	go func() { late <- runtime.Wait() }()
	select {
	case <-late:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait() after Close never returned")
	}
}

func TestNewWithContextAbandonsAnAlreadyCancelledConstruction(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runtime, err := NewWithContext(ctx, cfg)
	if err == nil {
		_ = runtime.Close()
		t.Fatal("NewWithContext() ignored a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if runtime != nil {
		t.Fatal("NewWithContext() returned a runtime alongside an error")
	}
	// Nothing half-built is left behind either.
	if _, statErr := os.Stat(cfg.Node.StateDBPath + "-wal"); statErr == nil {
		t.Fatal("the -wal sidecar exists after a refused construction")
	}
}

func TestAFailedStartReportsItsErrorToEveryWaiter(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	// An address no listener can bind: rpc.NewServer accepts it and
	// StartAsync is what fails — after the router is already running.
	cfg.RPC.Enabled = true
	cfg.RPC.Host = "256.256.256.256"
	cfg.RPC.Port = "1"

	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	startErr := runtime.Start(context.Background())
	if startErr == nil {
		t.Fatal("Start() accepted an unusable RPC listener")
	}

	// Start returns what a waiter is told, shutdown failure included: a caller
	// that only ever calls Start or Run would otherwise never learn that the
	// database was left open.
	// Identity, not a matching message: when the shutdown itself succeeds the
	// joined error reads the same as the bare one, so comparing text would
	// pass whether or not the shutdown's outcome was carried at all.
	if waited := runtime.Wait(); startErr != waited {
		t.Fatalf("Start() returned %#v, want the same outcome Wait reports: %#v", startErr, waited)
	}

	// Close runs inside that failure path and the node never started, so an
	// unconditional finish there reported success and buried the real cause.
	for _, waiter := range []string{"first", "second"} {
		waited := make(chan error, 1)
		go func() { waited <- runtime.Wait() }()
		select {
		case err := <-waited:
			if err == nil {
				t.Fatalf("the %s Wait() reported success after Start failed", waiter)
			}
			if !errors.Is(err, startErr) {
				t.Fatalf("the %s Wait() error = %v, want it to carry %v", waiter, err, startErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the %s Wait() never returned", waiter)
		}
	}
}

func TestConcurrentStartsShareTheSameOutcome(t *testing.T) {
	t.Parallel()

	cfg := testRuntimeConfig(t)
	cfg.RPC.Enabled = true
	cfg.RPC.Host = "256.256.256.256"
	cfg.RPC.Port = "1"

	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	// The failing path releases the lifecycle lock to run Close, and a Start
	// entering that window read the bare RPC error — or errClosed, once that
	// Close had set stopping — instead of the outcome every other caller gets.
	const callers = 8
	outcomes := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			ready.Done()
			ready.Wait()
			outcomes <- runtime.Start(context.Background())
		}()
	}

	waited := runtime.Wait()
	for i := 0; i < callers; i++ {
		select {
		case err := <-outcomes:
			if err != waited {
				t.Fatalf("a concurrent Start returned %#v, want the shared outcome %#v", err, waited)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("a concurrent Start never returned")
		}
	}
}

func TestACommandIsCancelledWhenCloseBegins(t *testing.T) {
	t.Parallel()

	runtime, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entered := make(chan struct{})
	released := make(chan error, 1)
	runtime.cmdTable.Register(
		rpc.CommandInfo{Name: "blockUntilCancelled", Description: "blocks until its context ends", Category: "test"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			close(entered)
			if req.Ctx == nil {
				released <- errors.New("the command was given no context at all")
				return rpc.CommandResponse{Data: []byte(`{}`)}
			}
			<-req.Ctx.Done()
			released <- req.Ctx.Err()
			return rpc.CommandResponse{Data: []byte(`{}`)}
		},
	)

	// Through the context-taking entry point, with a context of the caller's
	// that never ends: replacing the runtime's context with it left the
	// command blind to the shutdown.
	go func() { _, _ = runtime.ExecuteContext(context.Background(), "blockUntilCancelled", nil) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the command was never reached")
	}

	// A command that outlasts the drain budget turns a clean shutdown into an
	// incomplete one — Close reports it, Wait returns an error and the
	// database is deliberately left open — so the shutdown tells commands to
	// stop before it waits for them.
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()

	select {
	case err := <-released:
		if err == nil {
			t.Fatal("the command returned without its context being cancelled")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the command ended with %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the command never noticed the shutdown")
	}

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v — the command overran the drain budget", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Close() hung")
	}
}
