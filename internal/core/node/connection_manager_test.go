package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// fakePeerSession creates a minimal peerSession backed by a fake net.Conn.
func fakePeerSession(address domain.PeerAddress, identity domain.PeerIdentity) *peerSession {
	server, client := net.Pipe()
	// We only need the client side; close server to avoid leaks.
	_ = server.Close()
	return &peerSession{
		address:      address,
		peerIdentity: identity,
		conn:         client,
		connID:       1,
		capabilities: []domain.Capability{},
	}
}

// fakeDialFn builds a DialFn that succeeds with a fakePeerSession.
// The returned channel receives every address slice that was dialled.
func fakeDialFn() (func(context.Context, []domain.PeerAddress) (DialResult, error), chan []domain.PeerAddress) {
	dialled := make(chan []domain.PeerAddress, 64)
	fn := func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		dialled <- addrs
		session := fakePeerSession(addrs[0], "id-"+domain.PeerIdentity(addrs[0]))
		return DialResult{
			Session:          session,
			ConnectedAddress: addrs[0],
		}, nil
	}
	return fn, dialled
}

// testCMBuilder allows two-phase construction: customize config, then build CM
// with QueuedFn and HealthFn properly wired.
//
// The builder maintains a test-side health map. OnDialFailed updates this map
// (incrementing ConsecutiveFailures, setting BannedUntil for incompatible peers).
// PeerProvider's HealthFn reads from it. This ensures that after replace + fill(),
// Candidates() won't return the same peer we just gave up on — mirroring how
// Service.markPeerDisconnected + penalizeOldProtocolPeer work in production.
type testCMBuilder struct {
	Cfg    ConnectionManagerConfig
	cmPtr  atomic.Pointer[ConnectionManager]
	mu     sync.Mutex
	health map[domain.PeerAddress]*PeerHealthView
}

// testCMConfig creates a builder with PeerProvider pre-populated from addresses.
// Callers can modify b.Cfg (DialFn, MaxSlotsFn, etc.) before calling b.Build().
func testCMConfig(addresses ...string) *testCMBuilder {
	b := &testCMBuilder{
		health: make(map[domain.PeerAddress]*PeerHealthView),
	}

	ppCfg := testProviderConfig()
	ppCfg.QueuedFn = func() map[string]struct{} {
		if cm := b.cmPtr.Load(); cm != nil {
			return cm.QueuedIPs()
		}
		return nil
	}
	ppCfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		b.mu.Lock()
		defer b.mu.Unlock()
		if h, ok := b.health[addr]; ok {
			cp := *h
			return &cp
		}
		return nil
	}

	pp := NewPeerProvider(ppCfg)
	for _, addr := range addresses {
		pp.Add(mustAddr(addr), domain.PeerSourceBootstrap)
	}

	dialFn, _ := fakeDialFn()
	b.Cfg = ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 8 },
		Provider:   pp,
		DialFn:     dialFn,
		OnSessionEstablished: func(info SessionInfo) {
			// Simulate successful initPeerSession by promoting the
			// slot from Initializing → Active immediately.
			if cm := b.cmPtr.Load(); cm != nil {
				cm.EmitSlot(SessionInitReady{
					Address:        info.Address,
					SlotGeneration: info.SlotGeneration,
				})
			}
		},
		OnSessionTeardown: func(SessionInfo) {},
		OnDialFailed: func(addr domain.PeerAddress, err error, incompatible bool) {
			b.mu.Lock()
			defer b.mu.Unlock()
			h := b.health[addr]
			if h == nil {
				h = &PeerHealthView{}
				b.health[addr] = h
			}
			h.ConsecutiveFailures++
			h.Score += peerScoreFailure
			h.LastDisconnectedAt = time.Now()
			if incompatible {
				h.Score += peerScoreOldProtocol
				h.BannedUntil = time.Now().Add(peerBanIncompatible)
			}
		},
		BackoffFn: func(attempt int) time.Duration { return 0 }, // zero backoff in tests
		NowFn:     func() time.Time { return time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC) },
	}
	return b
}

// Build creates the ConnectionManager and wires QueuedFn.
func (b *testCMBuilder) Build() *ConnectionManager {
	cm := NewConnectionManager(b.Cfg)
	b.cmPtr.Store(cm)
	return cm
}

// runCM starts the ConnectionManager event loop in a goroutine, waits for
// the event loop to be ready (accepting gate open), and returns a cancel
// function. The CM will run until cancel() is called.
func runCM(cm *ConnectionManager) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go cm.Run(ctx)
	<-cm.Ready()
	return cancel
}

// waitFor polls a condition with a timeout.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
}

// ---------------------------------------------------------------------------
// Tests: BootstrapReady → fill → DialSucceeded → Active
// ---------------------------------------------------------------------------

func TestCM_BootstrapReady_FillsSlots(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646")
	b.Cfg.MaxSlotsFn = func() int { return 3 }

	dialFn, dialled := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Wait for 3 dial workers to be launched.
	waitFor(t, 2*time.Second, "3 dials", func() bool {
		return len(dialled) >= 3
	})

	// Wait for all slots to become Active.
	waitFor(t, 2*time.Second, "3 active slots", func() bool {
		return cm.ActiveCount() == 3
	})

	if cm.SlotCount() != 3 {
		t.Fatalf("SlotCount() = %d, want 3", cm.SlotCount())
	}
}

// ---------------------------------------------------------------------------
// Tests: ActiveSessionLost → reconnect → DialSucceeded → Active
// ---------------------------------------------------------------------------

func TestCM_ActiveSessionLost_Reconnects(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		session := fakePeerSession(addrs[0], "id-peer1")
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Wait for initial connection.
	waitFor(t, 2*time.Second, "initial active", func() bool {
		return cm.ActiveCount() == 1
	})

	// Capture the generation of the active slot.
	slots := cm.Slots()
	gen := slots[0].Generation

	// Simulate session loss.
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-peer1",
		Error:          errors.New("connection reset"),
		WasHealthy:     true,
		SlotGeneration: gen,
	})

	// Should reconnect (dial count increases).
	waitFor(t, 2*time.Second, "reconnect dial", func() bool {
		return atomic.LoadInt32(&dialCount) >= 2
	})

	// Should be active again.
	waitFor(t, 2*time.Second, "active after reconnect", func() bool {
		return cm.ActiveCount() == 1
	})
}

// ---------------------------------------------------------------------------
// Tests: ActiveSessionLost → reconnect fail × 3 → replace → new peer
// ---------------------------------------------------------------------------

func TestCM_ReconnectFail_Replace(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var mu sync.Mutex
	failAddr := mustAddr("10.0.0.1:64646")
	var dialCount int32

	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		mu.Lock()
		shouldFail := addrs[0] == failAddr
		mu.Unlock()

		if shouldFail {
			return DialResult{}, errors.New("connection refused")
		}
		session := fakePeerSession(addrs[0], "id-"+domain.PeerIdentity(addrs[0]))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	// Initially let peer1 connect.
	mu.Lock()
	failAddr = mustAddr("never-fail")
	mu.Unlock()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "initial active", func() bool {
		return cm.ActiveCount() == 1
	})

	slots := cm.Slots()
	gen := slots[0].Generation

	// Now make peer1 fail on reconnect.
	mu.Lock()
	failAddr = mustAddr("10.0.0.1:64646")
	mu.Unlock()

	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-10.0.0.1:64646",
		Error:          errors.New("connection reset"),
		WasHealthy:     true,
		SlotGeneration: gen,
	})

	// After maxRetries+1 failures, slot should be replaced with peer2.
	waitFor(t, 10*time.Second, "replaced with peer2", func() bool {
		s := cm.Slots()
		if len(s) != 1 {
			return false
		}
		return s[0].Address == mustAddr("10.0.0.2:64646") && s[0].State == "active"
	})
}

// ---------------------------------------------------------------------------
// Tests: DialFailed (incompatible) → immediate replace without retry
// ---------------------------------------------------------------------------

func TestCM_DialFailed_Incompatible_ImmediateReplace(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		count := atomic.AddInt32(&dialCount, 1)
		// First dial (peer1) is incompatible; subsequent (peer2) succeeds.
		if count == 1 {
			return DialResult{}, errIncompatibleProtocol
		}
		session := fakePeerSession(addrs[0], "id-"+domain.PeerIdentity(addrs[0]))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Should skip retries and replace with peer2.
	waitFor(t, 5*time.Second, "peer2 active", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].Address == mustAddr("10.0.0.2:64646") && s[0].State == "active"
	})

	// Should have dialled exactly twice: peer1 (fail) + peer2 (success).
	if got := atomic.LoadInt32(&dialCount); got != 2 {
		t.Fatalf("dialCount = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: InboundClosed → fill when slots < max
// ---------------------------------------------------------------------------

func TestCM_InboundClosed_TriggersFilll(t *testing.T) {
	// Start with only 1 candidate; second will be added after first fill.
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "1 active slot", func() bool {
		return cm.ActiveCount() == 1
	})

	// Add second peer and emit InboundClosed.
	b.Cfg.Provider.Add(mustAddr("10.0.0.2:64646"), domain.PeerSourceBootstrap)
	cm.EmitHint(InboundClosed{IP: "192.168.1.1", Identity: "some-id"})

	waitFor(t, 2*time.Second, "2 active slots", func() bool {
		return cm.ActiveCount() == 2
	})
}

// ---------------------------------------------------------------------------
// Tests: InboundClosed at max → fill NOT called
// ---------------------------------------------------------------------------

func TestCM_InboundClosed_AtMax_NoFill(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		session := fakePeerSession(addrs[0], "id-peer")
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "1 active", func() bool {
		return cm.ActiveCount() == 1
	})

	before := atomic.LoadInt32(&dialCount)
	cm.EmitHint(InboundClosed{IP: "192.168.1.1"})

	// Give the event loop time to process.
	time.Sleep(100 * time.Millisecond)

	after := atomic.LoadInt32(&dialCount)
	if after != before {
		t.Fatalf("fill was called at max slots: dialCount went from %d to %d", before, after)
	}
}

// ---------------------------------------------------------------------------
// Tests: NewPeersDiscovered at max → fill NOT called
// ---------------------------------------------------------------------------

func TestCM_NewPeersDiscovered_AtMax_NoFill(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		session := fakePeerSession(addrs[0], "id-peer")
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "1 active", func() bool {
		return cm.ActiveCount() == 1
	})

	before := atomic.LoadInt32(&dialCount)
	cm.EmitHint(NewPeersDiscovered{Count: 5})
	time.Sleep(100 * time.Millisecond)

	after := atomic.LoadInt32(&dialCount)
	if after != before {
		t.Fatalf("fill was called at max slots: dialCount went from %d to %d", before, after)
	}
}

// ---------------------------------------------------------------------------
// Tests: MaxSlotsFn() limit not exceeded
// ---------------------------------------------------------------------------

func TestCM_MaxSlots_NotExceeded(t *testing.T) {
	b := testCMConfig(
		"10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646",
		"10.0.0.4:64646", "10.0.0.5:64646",
	)
	b.Cfg.MaxSlotsFn = func() int { return 3 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "3 active", func() bool {
		return cm.ActiveCount() == 3
	})

	if cm.SlotCount() != 3 {
		t.Fatalf("SlotCount() = %d, want 3", cm.SlotCount())
	}
}

// ---------------------------------------------------------------------------
// Tests: Generation guard — stale DialFailed ignored
// ---------------------------------------------------------------------------

func TestCM_GenerationGuard_StaleDialFailed(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active", func() bool {
		return cm.ActiveCount() == 1
	})

	// Send a DialFailed with a stale generation.
	cm.EmitSlot(DialFailed{
		Address:        mustAddr("10.0.0.1:64646"),
		Error:          errors.New("stale error"),
		SlotGeneration: 0, // definitely stale
	})

	time.Sleep(100 * time.Millisecond)

	// Slot should still be active — stale event was ignored.
	if cm.ActiveCount() != 1 {
		t.Fatalf("ActiveCount() = %d, want 1 after stale DialFailed", cm.ActiveCount())
	}
}

// ---------------------------------------------------------------------------
// Tests: Generation guard — stale DialSucceeded → session closed, no score
// ---------------------------------------------------------------------------

func TestCM_GenerationGuard_StaleDialSucceeded_SessionClosed(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	var teardownCount int32
	b.Cfg.OnSessionTeardown = func(SessionInfo) {
		atomic.AddInt32(&teardownCount, 1)
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active", func() bool {
		return cm.ActiveCount() == 1
	})

	// Send a stale DialSucceeded — session should be closed, no teardown callback.
	staleSession := fakePeerSession(mustAddr("10.0.0.1:64646"), "id-stale")
	cm.EmitSlot(DialSucceeded{
		Address:          mustAddr("10.0.0.1:64646"),
		ConnectedAddress: mustAddr("10.0.0.1:64646"),
		Session:          staleSession,
		SlotGeneration:   0, // stale
	})

	time.Sleep(100 * time.Millisecond)

	// onSessionTeardown should NOT have been called for the stale session.
	if got := atomic.LoadInt32(&teardownCount); got != 0 {
		t.Fatalf("teardownCount = %d, want 0 for stale DialSucceeded", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: Generation guard — stale ActiveSessionLost after deactivate
// ---------------------------------------------------------------------------

func TestCM_GenerationGuard_StaleActiveSessionLost(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	var teardownCount int32
	b.Cfg.OnSessionTeardown = func(SessionInfo) {
		atomic.AddInt32(&teardownCount, 1)
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active", func() bool {
		return cm.ActiveCount() == 1
	})

	slots := cm.Slots()
	oldGen := slots[0].Generation

	// Simulate a session loss and reconnect (which increments generation).
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-10.0.0.1:64646",
		Error:          errors.New("reset"),
		SlotGeneration: oldGen,
	})

	// Wait for reconnect to COMPLETE: generation must advance past oldGen
	// (proving the full deactivate → reconnect → activate cycle ran).
	// Checking ActiveCount alone is racy with zero backoff — the count
	// may still be 1 from the initial activation if the event loop
	// hasn't processed ActiveSessionLost yet.
	waitFor(t, 3*time.Second, "reconnected (generation advanced)", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].State == "active" && s[0].Generation != oldGen
	})

	teardownBefore := atomic.LoadInt32(&teardownCount)

	// Send stale ActiveSessionLost with old generation.
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-10.0.0.1:64646",
		Error:          errors.New("late stale event"),
		SlotGeneration: oldGen,
	})

	time.Sleep(100 * time.Millisecond)

	teardownAfter := atomic.LoadInt32(&teardownCount)
	if teardownAfter != teardownBefore {
		t.Fatalf("onSessionTeardown called for stale ActiveSessionLost: before=%d, after=%d", teardownBefore, teardownAfter)
	}
}

// ---------------------------------------------------------------------------
// Tests: Atomicity of fill — no duplicate slots
// ---------------------------------------------------------------------------

func TestCM_Fill_Atomicity_NoDuplicates(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 4 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	// NotifyBootstrapReady is idempotent — second call is a no-op.
	cm.NotifyBootstrapReady()
	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active slots", func() bool {
		return cm.ActiveCount() >= 2
	})

	// Check no IP appears twice.
	slots := cm.Slots()
	seen := make(map[string]bool)
	for _, s := range slots {
		addr := string(s.Address)
		if seen[addr] {
			t.Fatalf("duplicate slot address: %s", addr)
		}
		seen[addr] = true
	}
}

// ---------------------------------------------------------------------------
// Tests: Backpressure — hint event overflow drops without blocking
// ---------------------------------------------------------------------------

func TestCM_HintEvent_Overflow_Drops(t *testing.T) {
	b := testCMConfig()
	b.Cfg.MaxSlotsFn = func() int { return 8 }

	// Block all dials so event loop stays in handle and doesn't drain hints.
	dialBlock := make(chan struct{})
	b.Cfg.DialFn = func(ctx context.Context, _ []domain.PeerAddress) (DialResult, error) {
		select {
		case <-dialBlock:
		case <-ctx.Done():
		}
		return DialResult{}, ctx.Err()
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer func() {
		close(dialBlock)
		cancel()
	}()

	// Flood hint channel beyond buffer. EmitHint is non-blocking —
	// excess events are silently dropped without blocking.
	for i := 0; i < hintEventBuffer+5; i++ {
		cm.EmitHint(NewPeersDiscovered{Count: i})
	}

	// If we get here without blocking, the test passes.
}

func TestCM_EmitHint_RejectedBeforeRun(t *testing.T) {
	b := testCMConfig()
	cm := b.Build()

	// Before Run(), EmitHint should not buffer events.
	for i := 0; i < hintEventBuffer+5; i++ {
		cm.EmitHint(NewPeersDiscovered{Count: i})
	}

	// Channel should be empty — nothing was buffered.
	select {
	case <-cm.hintEvents:
		t.Fatal("hintEvents should be empty before Run()")
	default:
		// expected
	}
}

// ---------------------------------------------------------------------------
// Tests: beginInitSlot triggers OnSessionEstablished
// ---------------------------------------------------------------------------

func TestCM_ActivateSlot_OnSessionEstablished(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var established int32
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		if info.Address != mustAddr("10.0.0.1:64646") {
			t.Errorf("unexpected address: %s", info.Address)
		}
		atomic.AddInt32(&established, 1)
	}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "established callback", func() bool {
		return atomic.LoadInt32(&established) == 1
	})
}

// ---------------------------------------------------------------------------
// Tests: deactivateSlot triggers OnSessionTeardown
// ---------------------------------------------------------------------------

func TestCM_DeactivateSlot_OnSessionTeardown(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var teardownAddr atomic.Value
	b.Cfg.OnSessionTeardown = func(info SessionInfo) {
		teardownAddr.Store(string(info.Address))
	}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active", func() bool {
		return cm.ActiveCount() == 1
	})

	// Simulate session loss → deactivateSlot should be called.
	slots := cm.Slots()
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-10.0.0.1:64646",
		Error:          errors.New("reset"),
		WasHealthy:     true,
		SlotGeneration: slots[0].Generation,
	})

	waitFor(t, 2*time.Second, "teardown callback", func() bool {
		v := teardownAddr.Load()
		return v != nil && v.(string) == "10.0.0.1:64646"
	})
}

// ---------------------------------------------------------------------------
// Tests: replaceSlot for Active slot — deactivate + session close + cleanup
// ---------------------------------------------------------------------------

func TestCM_ReplaceSlot_ActiveSlot_Cleanup(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var teardownCount int32
	b.Cfg.OnSessionTeardown = func(SessionInfo) {
		atomic.AddInt32(&teardownCount, 1)
	}

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		count := atomic.AddInt32(&dialCount, 1)
		if count == 1 {
			// First dial succeeds.
			session := fakePeerSession(addrs[0], "id-peer1")
			return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
		}
		if addrs[0] == mustAddr("10.0.0.1:64646") {
			// Reconnect of peer1 fails (incompatible).
			return DialResult{}, errIncompatibleProtocol
		}
		// Peer2 succeeds.
		session := fakePeerSession(addrs[0], "id-peer2")
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "initial active", func() bool {
		return cm.ActiveCount() == 1
	})

	slots := cm.Slots()
	gen := slots[0].Generation

	// Session loss → reconnect → fail incompatible → replace.
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-peer1",
		Error:          errors.New("reset"),
		WasHealthy:     true,
		SlotGeneration: gen,
	})

	// Wait for replacement to complete.
	waitFor(t, 5*time.Second, "replaced with peer2", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].Address == mustAddr("10.0.0.2:64646") && s[0].State == "active"
	})

	// Teardown should have been called for peer1 (deactivate before reconnect).
	if got := atomic.LoadInt32(&teardownCount); got < 1 {
		t.Fatalf("teardownCount = %d, want >= 1", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: replaceSlot for non-Active (Dialing) slot — no session to close
// ---------------------------------------------------------------------------

func TestCM_ReplaceSlot_NonActive_NoTeardown(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var teardownCount int32
	b.Cfg.OnSessionTeardown = func(SessionInfo) {
		atomic.AddInt32(&teardownCount, 1)
	}

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		count := atomic.AddInt32(&dialCount, 1)
		if addrs[0] == mustAddr("10.0.0.1:64646") {
			_ = count
			return DialResult{}, errIncompatibleProtocol
		}
		session := fakePeerSession(addrs[0], "id-peer2")
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Peer1 fails incompatible (never was active) → replaced by peer2.
	waitFor(t, 5*time.Second, "peer2 active", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].Address == mustAddr("10.0.0.2:64646") && s[0].State == "active"
	})

	// No teardown for peer1 — it was never active.
	if got := atomic.LoadInt32(&teardownCount); got != 0 {
		t.Fatalf("teardownCount = %d, want 0 for never-active slot", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: Shutdown — all sessions closed, Run() returns
// ---------------------------------------------------------------------------

func TestCM_Shutdown_ClosesAllSessions(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }

	var teardownCount int32
	b.Cfg.OnSessionTeardown = func(SessionInfo) {
		atomic.AddInt32(&teardownCount, 1)
	}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "2 active", func() bool {
		return cm.ActiveCount() == 2
	})

	// Cancel context → shutdown.
	cancel()

	select {
	case <-done:
		// Run() returned.
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}

	// All sessions should have been torn down.
	if got := atomic.LoadInt32(&teardownCount); got != 2 {
		t.Fatalf("teardownCount = %d, want 2", got)
	}

	// No slots left.
	if cm.SlotCount() != 0 {
		t.Fatalf("SlotCount() = %d after shutdown, want 0", cm.SlotCount())
	}
}

// ---------------------------------------------------------------------------
// Tests: Shutdown — channels not closed (no panic from late writers)
// ---------------------------------------------------------------------------

func TestCM_Shutdown_NoPanic(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	// Dial blocks until ctx is cancelled.
	b.Cfg.DialFn = func(ctx context.Context, _ []domain.PeerAddress) (DialResult, error) {
		<-ctx.Done()
		return DialResult{}, ctx.Err()
	}

	cm := b.Build()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	cm.NotifyBootstrapReady()
	time.Sleep(50 * time.Millisecond) // let dial worker start

	cancel()
	<-done

	// Late writes to channels should not panic.
	// EmitSlot will return false (ctx done), EmitHint will drop.
	cm.EmitHint(NewPeersDiscovered{Count: 1})
	// If we get here, no panic occurred.
}

// ---------------------------------------------------------------------------
// Tests: Shutdown — drain stale DialSucceeded sessions
// ---------------------------------------------------------------------------

func TestCM_Shutdown_DrainStaleSession(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialStarted := make(chan struct{})
	dialBlock := make(chan struct{})

	b.Cfg.DialFn = func(ctx context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		close(dialStarted)
		select {
		case <-dialBlock:
			session := fakePeerSession(addrs[0], "id-peer")
			return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
		case <-ctx.Done():
			return DialResult{}, ctx.Err()
		}
	}

	cm := b.Build()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	cm.NotifyBootstrapReady()
	<-dialStarted

	// Release the dial and immediately shutdown.
	close(dialBlock)
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return")
	}
}

// ---------------------------------------------------------------------------
// Tests: QueuedIPs reflects all slot states
// ---------------------------------------------------------------------------

func TestCM_QueuedIPs(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }

	// Dial blocks forever so slots stay in Dialing state.
	b.Cfg.DialFn = func(ctx context.Context, _ []domain.PeerAddress) (DialResult, error) {
		<-ctx.Done()
		return DialResult{}, ctx.Err()
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "2 slots", func() bool {
		return cm.SlotCount() == 2
	})

	ips := cm.QueuedIPs()
	if _, ok := ips["10.0.0.1"]; !ok {
		t.Error("QueuedIPs missing 10.0.0.1")
	}
	if _, ok := ips["10.0.0.2"]; !ok {
		t.Error("QueuedIPs missing 10.0.0.2")
	}
}

// ---------------------------------------------------------------------------
// Tests: Slots() RPC view
// ---------------------------------------------------------------------------

func TestCM_Slots_RPCView(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active", func() bool {
		return cm.ActiveCount() == 1
	})

	slots := cm.Slots()
	if len(slots) != 1 {
		t.Fatalf("len(Slots()) = %d, want 1", len(slots))
	}

	s := slots[0]
	if s.Address != mustAddr("10.0.0.1:64646") {
		t.Errorf("Address = %s, want 10.0.0.1:64646", s.Address)
	}
	if s.State != "active" {
		t.Errorf("State = %s, want active", s.State)
	}
	if s.Identity == nil {
		t.Error("Identity is nil for active slot")
	}
	if s.ConnectedAddress == nil {
		t.Error("ConnectedAddress is nil for active slot")
	}
}

// ---------------------------------------------------------------------------
// Tests: Slot order — replace → shift up + new at end
// ---------------------------------------------------------------------------

func TestCM_SlotOrder_ReplaceShiftsUp(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }

	var mu sync.Mutex
	failAddrs := make(map[domain.PeerAddress]bool)

	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		mu.Lock()
		shouldFail := failAddrs[addrs[0]]
		mu.Unlock()
		if shouldFail {
			return DialResult{}, errIncompatibleProtocol
		}
		session := fakePeerSession(addrs[0], "id-"+domain.PeerIdentity(addrs[0]))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "2 active", func() bool {
		return cm.ActiveCount() == 2
	})

	// Find slot[0] and trigger its replacement.
	slots := cm.Slots()
	firstAddr := slots[0].Address
	firstGen := slots[0].Generation

	// Make first peer fail on reconnect.
	mu.Lock()
	failAddrs[firstAddr] = true
	mu.Unlock()

	cm.EmitSlot(ActiveSessionLost{
		Address:        firstAddr,
		Identity:       domain.PeerIdentity("id-" + string(firstAddr)),
		Error:          errors.New("reset"),
		WasHealthy:     true,
		SlotGeneration: firstGen,
	})

	// Wait for replacement.
	waitFor(t, 5*time.Second, "replaced", func() bool {
		s := cm.Slots()
		if len(s) != 2 {
			return false
		}
		// firstAddr should be gone, replaced by 10.0.0.3.
		for _, sl := range s {
			if sl.Address == firstAddr {
				return false
			}
		}
		return true
	})
}

// ---------------------------------------------------------------------------
// Tests: CM-initiated close — no double-count disconnect
// ---------------------------------------------------------------------------

func TestCM_CMInitiatedClose_NoDoubleCount(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }

	var teardownCount int32
	b.Cfg.OnSessionTeardown = func(SessionInfo) {
		atomic.AddInt32(&teardownCount, 1)
	}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "2 active", func() bool {
		return cm.ActiveCount() == 2
	})

	// Record the generations.
	slots := cm.Slots()
	gens := make(map[string]uint64)
	for _, s := range slots {
		gens[string(s.Address)] = s.Generation
	}

	// Shutdown → deactivateSlot called for each.
	cancel()
	<-done

	// Simulate late ActiveSessionLost events (from servePeerSession goroutines
	// that saw the conn close caused by deactivateSlot).
	// These should be stale because:
	// 1. slots are nil after shutdown
	// 2. even if we somehow had the slot, generation changed
	//
	// Since Run() returned, these events go to the channel but nobody reads them.
	// The important thing is teardownCount == 2 (once per slot, not doubled).
	if got := atomic.LoadInt32(&teardownCount); got != 2 {
		t.Fatalf("teardownCount = %d, want exactly 2", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: emitSlot returns false on shutdown
// ---------------------------------------------------------------------------

func TestCM_EmitSlot_ReturnsFalse_BeforeRun(t *testing.T) {
	// EmitSlot must return false when called before Run() starts —
	// no event loop is consuming, so blocking would hang forever.
	b := testCMConfig()
	cm := b.Build()

	ok := cm.EmitSlot(DialFailed{
		Address:        mustAddr("10.0.0.1:64646"),
		Error:          errors.New("test"),
		SlotGeneration: 1,
	})
	if ok {
		t.Fatal("EmitSlot should return false before Run()")
	}
}

func TestCM_NoFillBeforeBootstrapReady(t *testing.T) {
	// NewPeersDiscovered emitted before BootstrapReady must NOT trigger fill().
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount atomic.Int32
	b.Cfg.DialFn = func(ctx context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		dialCount.Add(1)
		session := fakePeerSession(addrs[0], "id-peer1")
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	// Emit non-bootstrap hints first.
	cm.EmitHint(NewPeersDiscovered{Count: 1})
	cm.EmitHint(InboundClosed{})

	// Give event loop time to process.
	time.Sleep(100 * time.Millisecond)

	// No dials should have happened.
	if got := dialCount.Load(); got != 0 {
		t.Fatalf("dialCount = %d before BootstrapReady, want 0", got)
	}

	// Now send BootstrapReady — should trigger fill.
	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "1 active after bootstrap", func() bool {
		return cm.ActiveCount() == 1
	})
}

func TestCM_BootstrapReady_BeforeRun(t *testing.T) {
	// NotifyBootstrapReady called BEFORE Run() must still trigger fill()
	// once the event loop starts — close(bootstrapCh) is permanent.
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()

	// Signal bootstrap BEFORE Run starts.
	cm.NotifyBootstrapReady()

	cancel := runCM(cm)
	defer cancel()

	// Event loop should see the closed bootstrapCh and fill.
	waitFor(t, 2*time.Second, "1 active", func() bool {
		return cm.ActiveCount() == 1
	})
}

func TestCM_EmitSlot_ReturnsFalse_OnShutdown(t *testing.T) {
	// After Run() returns, shutdown() has set running=0.
	// EmitSlot checks running first and returns false deterministically,
	// regardless of channel buffer size.
	b := testCMConfig()
	cm := b.Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediate shutdown — Run will enter event loop and exit on ctx.Done()
	cm.Run(ctx)

	ok := cm.EmitSlot(DialFailed{
		Address:        mustAddr("10.0.0.1:64646"),
		Error:          errors.New("test"),
		SlotGeneration: 1,
	})
	if ok {
		t.Fatal("EmitSlot should return false after shutdown")
	}
}

// TestCM_Shutdown_DrainsInFlightDials verifies the buffered-channel shutdown path:
// a dial worker that completes AFTER ctx is cancelled but BEFORE shutdown()
// finishes will enqueue its DialSucceeded into the buffered slotEvents channel.
// shutdown() calls dialWg.Wait() then drainChannels() — guaranteeing the
// session is closed regardless of whether EmitSlot returned true or false.
func TestCM_Shutdown_DrainsInFlightDials(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialStarted := make(chan struct{})
	dialRelease := make(chan struct{})
	var closedSessions atomic.Int32

	// DialFn blocks on dialRelease (ignores ctx.Done) so we can
	// guarantee the worker creates a session AFTER ctx is cancelled.
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		close(dialStarted) // signal that dial is in progress
		<-dialRelease      // wait for test to release — unconditionally

		session := fakePeerSession(addrs[0], "id-peer1")
		origConn := session.conn
		session.conn = &closeCountingConn{Conn: origConn, counter: &closedSessions}
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)

	cm.NotifyBootstrapReady()

	// Wait for dial worker to start.
	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("dial worker did not start")
	}

	// 1. Cancel ctx — event loop exits, shutdown() begins.
	// 2. shutdown() deactivates active slots, then blocks on dialWg.Wait().
	// 3. Release the dial worker — it creates a session and calls EmitSlot.
	//    On a buffered channel EmitSlot may return true (enqueued) or false
	//    (ctx done). Either path is safe: false → worker closes session;
	//    true → drainChannels() closes it.
	// 4. dialWg.Wait() returns → drainChannels() runs → test completes.
	cancel()
	close(dialRelease)

	// Give shutdown time to complete (dialWg.Wait + drainChannels).
	time.Sleep(500 * time.Millisecond)

	// The session must have been closed — no leak.
	if got := closedSessions.Load(); got == 0 {
		t.Fatal("session was not closed during shutdown — session leak")
	}
}

// closeCountingConn wraps net.Conn and increments counter on Close.
type closeCountingConn struct {
	net.Conn
	counter *atomic.Int32
}

func (c *closeCountingConn) Close() error {
	c.counter.Add(1)
	return c.Conn.Close()
}

// ---------------------------------------------------------------------------
// Tests: backoffDuration
// ---------------------------------------------------------------------------

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 10 * time.Second}, // capped
		{5, 10 * time.Second}, // still capped
	}

	for _, tt := range tests {
		got := backoffDuration(tt.attempt)
		if got != tt.want {
			t.Errorf("backoffDuration(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests: NewPeersDiscovered with free slots triggers fill
// ---------------------------------------------------------------------------

func TestCM_NewPeersDiscovered_FreeSlots_Fill(t *testing.T) {
	// Start with only 1 peer; second will be added after first fill.
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 2 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "1 active", func() bool {
		return cm.ActiveCount() == 1
	})

	// Now add a second peer and notify.
	b.Cfg.Provider.Add(mustAddr("10.0.0.2:64646"), domain.PeerSourcePeerExchange)
	cm.EmitHint(NewPeersDiscovered{Count: 1})

	waitFor(t, 2*time.Second, "2 active", func() bool {
		return cm.ActiveCount() == 2
	})
}

// ---------------------------------------------------------------------------
// Tests: No duplicate workers — events for same slot don't spawn extra workers
// ---------------------------------------------------------------------------

func TestCM_NoDuplicateWorkers(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount int32
	dialBlock := make(chan struct{})
	b.Cfg.DialFn = func(ctx context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		count := atomic.AddInt32(&dialCount, 1)
		if count == 1 {
			// First dial succeeds immediately.
			session := fakePeerSession(addrs[0], "id-peer1")
			return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
		}
		// Subsequent dials block to let us count active workers.
		select {
		case <-dialBlock:
			session := fakePeerSession(addrs[0], "id-peer1")
			return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
		case <-ctx.Done():
			return DialResult{}, ctx.Err()
		}
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer func() {
		close(dialBlock)
		cancel()
	}()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "active", func() bool {
		return cm.ActiveCount() == 1
	})

	slots := cm.Slots()
	gen := slots[0].Generation

	// Emit ActiveSessionLost — triggers reconnect (one dial worker).
	cm.EmitSlot(ActiveSessionLost{
		Address:        mustAddr("10.0.0.1:64646"),
		Identity:       "id-peer1",
		Error:          errors.New("reset"),
		WasHealthy:     true,
		SlotGeneration: gen,
	})

	// Wait for the reconnect dial to start.
	waitFor(t, 2*time.Second, "reconnect started", func() bool {
		return atomic.LoadInt32(&dialCount) >= 2
	})

	// Send a stale DialFailed for the same slot (old generation).
	// This should be ignored — no additional worker should spawn.
	cm.EmitSlot(DialFailed{
		Address:        mustAddr("10.0.0.1:64646"),
		Error:          errors.New("old error"),
		SlotGeneration: gen, // old generation
	})

	time.Sleep(200 * time.Millisecond)

	// Only 2 dials should have happened: initial + reconnect.
	if got := atomic.LoadInt32(&dialCount); got != 2 {
		t.Fatalf("dialCount = %d, want 2 (initial + reconnect, no duplicate)", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: dynamic slot-limit decrease (shrinkToLimit)
// ---------------------------------------------------------------------------

func TestCM_ShrinkToLimit_EvictsNonActiveFirst(t *testing.T) {
	// Start with 3 slots, 2 active + 1 dialing.
	// Then drop MaxSlotsFn to 2 — the dialing slot should be evicted,
	// active slots should remain.
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646")

	var maxSlots atomic.Int32
	maxSlots.Store(3)
	b.Cfg.MaxSlotsFn = func() int { return int(maxSlots.Load()) }

	// First two dials succeed instantly; third blocks.
	dialBlock := make(chan struct{})
	var dialCount atomic.Int32
	b.Cfg.DialFn = func(ctx context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		n := dialCount.Add(1)
		if n <= 2 {
			session := fakePeerSession(addrs[0], domain.PeerIdentity("id-"+string(addrs[0])))
			return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
		}
		// Third dial blocks until released or cancelled.
		select {
		case <-dialBlock:
			session := fakePeerSession(addrs[0], domain.PeerIdentity("id-"+string(addrs[0])))
			return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
		case <-ctx.Done():
			return DialResult{}, ctx.Err()
		}
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer func() {
		close(dialBlock)
		cancel()
	}()

	cm.NotifyBootstrapReady()

	// Wait for 2 active slots.
	waitFor(t, 2*time.Second, "2 active", func() bool {
		return cm.ActiveCount() == 2
	})
	// Total should be 3 (2 active + 1 dialing).
	waitFor(t, 2*time.Second, "3 total slots", func() bool {
		return cm.SlotCount() == 3
	})

	// Drop limit to 2.
	maxSlots.Store(2)

	// Trigger fill() which calls shrinkToLimit first.
	cm.EmitHint(NewPeersDiscovered{})

	// Should shrink to 2: the non-active (dialing) slot is evicted.
	waitFor(t, 2*time.Second, "shrunk to 2", func() bool {
		return cm.SlotCount() == 2
	})

	// Both remaining should be active.
	if got := cm.ActiveCount(); got != 2 {
		t.Fatalf("ActiveCount = %d, want 2 (non-active evicted first)", got)
	}
}

func TestCM_ShrinkToLimit_EvictsActiveWhenNoNonActive(t *testing.T) {
	// Start with 3 active slots, drop limit to 1.
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646", "10.0.0.3:64646")
	b.Cfg.MaxSlotsFn = func() int { return 3 }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	var teardownCount atomic.Int32
	b.Cfg.OnSessionTeardown = func(info SessionInfo) {
		teardownCount.Add(1)
	}

	cm := b.Build()

	// Use a mutable max for post-startup shrink.
	var maxSlots atomic.Int32
	maxSlots.Store(3)
	cm.config.MaxSlotsFn = func() int { return int(maxSlots.Load()) }

	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "3 active", func() bool {
		return cm.ActiveCount() == 3
	})

	// Drop to 1.
	maxSlots.Store(1)
	cm.EmitHint(NewPeersDiscovered{})

	waitFor(t, 2*time.Second, "shrunk to 1", func() bool {
		return cm.SlotCount() == 1
	})

	if got := cm.ActiveCount(); got != 1 {
		t.Fatalf("ActiveCount = %d, want 1", got)
	}

	// 2 active slots were evicted → 2 teardown callbacks.
	if got := teardownCount.Load(); got != 2 {
		t.Fatalf("teardownCount = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// Tests: Run() single-start guard
// ---------------------------------------------------------------------------

func TestCM_Run_PanicsOnSecondCall(t *testing.T) {
	b := testCMConfig()
	cm := b.Build()

	ctx, cancel := context.WithCancel(context.Background())
	// First Run — starts and immediately shuts down.
	cancel()
	cm.Run(ctx)

	// Second Run — must panic.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on second Run(), got none")
		}
		msg, ok := r.(string)
		if !ok || msg != "connection_manager: Run called more than once" {
			t.Fatalf("unexpected panic value: %v", r)
		}
	}()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	cm.Run(ctx2)
}

// ---------------------------------------------------------------------------
// Tests: slotState.String()
// ---------------------------------------------------------------------------

func TestSlotState_String(t *testing.T) {
	tests := []struct {
		state slotState
		want  string
	}{
		{slotStateQueued, "queued"},
		{slotStateDialing, "dialing"},
		{slotStateInitializing, "initializing"},
		{slotStateActive, "active"},
		{slotStateReconnecting, "reconnecting"},
		{slotStateRetryWait, "retry_wait"},
		{slotState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("slotState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests: slot stays Initializing until SessionInitReady
// ---------------------------------------------------------------------------

// TestCM_SlotInitializing_NotVisibleAsActive verifies that after DialSucceeded
// the slot enters "initializing" (not "active") and is NOT counted by
// ActiveCount or included by buildPeerExchangeResponse until SessionInitReady
// promotes it.
func TestCM_SlotInitializing_NotVisibleAsActive(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	// Block the SessionInitReady emission — custom callback that does NOT
	// emit the promotion event.
	var savedInfo atomic.Value
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		savedInfo.Store(info)
	}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Wait for the slot to appear as "initializing".
	waitFor(t, 2*time.Second, "initializing", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].State == "initializing"
	})

	// ActiveCount must be 0 — the slot is not yet promoted.
	if got := cm.ActiveCount(); got != 0 {
		t.Fatalf("ActiveCount() = %d during init, want 0", got)
	}

	// Slot should expose identity for diagnostics even while initializing.
	slots := cm.Slots()
	if slots[0].Identity == nil {
		t.Fatal("expected Identity to be set on initializing slot")
	}

	// Now emit SessionInitReady to promote the slot.
	info := savedInfo.Load().(SessionInfo)
	cm.EmitSlot(SessionInitReady{
		Address:        info.Address,
		SlotGeneration: info.SlotGeneration,
	})

	waitFor(t, 2*time.Second, "active after promotion", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].State == "active"
	})

	if got := cm.ActiveCount(); got != 1 {
		t.Fatalf("ActiveCount() = %d after promotion, want 1", got)
	}
}

// TestCM_InitFailure_SlotNeverBecomesActive verifies that when the init
// goroutine emits ActiveSessionLost (init failed) without SessionInitReady,
// the slot transitions to retry/replace without ever reaching "active".
func TestCM_InitFailure_SlotNeverBecomesActive(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var dialCount int32
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		atomic.AddInt32(&dialCount, 1)
		session := fakePeerSession(addrs[0], "id-"+domain.PeerIdentity(addrs[0]))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		// Simulate initPeerSession failure — emit ActiveSessionLost
		// without SessionInitReady, so the slot never becomes active.
		if cm := b.cmPtr.Load(); cm != nil {
			cm.EmitSlot(ActiveSessionLost{
				Address:        info.Address,
				Identity:       info.Session.peerIdentity,
				Error:          errors.New("subscribe_inbox: timeout"),
				WasHealthy:     false,
				SlotGeneration: info.SlotGeneration,
			})
		}
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// The first peer should fail init and be retried/replaced.
	// Eventually peer2 should get its turn (also fails, but that's OK).
	waitFor(t, 5*time.Second, "multiple dial attempts", func() bool {
		return atomic.LoadInt32(&dialCount) >= 2
	})

	// ActiveCount should be 0 — no slot ever completed init.
	if got := cm.ActiveCount(); got != 0 {
		t.Fatalf("ActiveCount() = %d, want 0 — init always fails", got)
	}
}

// TestCM_SessionInitReady_StaleGeneration verifies that a SessionInitReady
// event with a stale generation is silently ignored.
func TestCM_SessionInitReady_StaleGeneration(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	var initInfo atomic.Value
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		initInfo.Store(info)
		// Do NOT emit SessionInitReady — we'll do it manually with a stale gen.
	}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 2*time.Second, "initializing", func() bool {
		s := cm.Slots()
		return len(s) == 1 && s[0].State == "initializing"
	})

	info := initInfo.Load().(SessionInfo)

	// Emit SessionInitReady with a wrong generation — should be ignored.
	cm.EmitSlot(SessionInitReady{
		Address:        info.Address,
		SlotGeneration: info.SlotGeneration + 999,
	})

	// Give event loop time to process.
	time.Sleep(50 * time.Millisecond)

	// Slot should still be initializing.
	slots := cm.Slots()
	if len(slots) != 1 || slots[0].State != "initializing" {
		t.Fatalf("slot state = %q, want 'initializing' (stale gen should be ignored)", slots[0].State)
	}
}

// TestCM_PeriodicFill_RetriesAfterCooldownExpiry verifies that the periodic
// fill ticker picks up peers whose cooldown has expired, even when no other
// events arrive. Without the periodic ticker, the CM is purely reactive:
// if bootstrap fill() finds 0 eligible candidates (all in cooldown), no
// slots are created, no slot events fire, and fill() is never called again.
func TestCM_PeriodicFill_RetriesAfterCooldownExpiry(t *testing.T) {
	t.Parallel()

	b := testCMConfig("10.0.0.1:64646", "10.0.0.2:64646")

	// Put both peers in cooldown: ConsecutiveFailures = 5, disconnected
	// 1 minute ago. With the fixed NowFn (2026-04-11 12:00:00), cooldown
	// = peerCooldownDuration(4) = 30s * 2^3 = 240s. Time since disconnect
	// = 60s < 240s → in cooldown.
	nowFixed := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	b.health["10.0.0.1:64646"] = &PeerHealthView{
		ConsecutiveFailures: 5,
		LastDisconnectedAt:  nowFixed.Add(-1 * time.Minute),
		Score:               -25,
	}
	b.health["10.0.0.2:64646"] = &PeerHealthView{
		ConsecutiveFailures: 5,
		LastDisconnectedAt:  nowFixed.Add(-1 * time.Minute),
		Score:               -25,
	}

	// Fast periodic fill for testing.
	b.Cfg.FillInterval = 50 * time.Millisecond

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	// Trigger bootstrap.
	cm.NotifyBootstrapReady()
	time.Sleep(30 * time.Millisecond)

	// After bootstrap, 0 slots should be created (all in cooldown).
	if count := cm.ActiveCount(); count != 0 {
		t.Fatalf("expected 0 active slots after bootstrap (all in cooldown), got %d", count)
	}
	if len(cm.Slots()) != 0 {
		t.Fatalf("expected 0 slots after bootstrap, got %d", len(cm.Slots()))
	}

	// Simulate cooldown expiry: clear the health failures so the peers
	// become eligible on the next fill() call.
	b.mu.Lock()
	b.health["10.0.0.1:64646"] = &PeerHealthView{
		ConsecutiveFailures: 0,
		Score:               0,
	}
	b.health["10.0.0.2:64646"] = &PeerHealthView{
		ConsecutiveFailures: 0,
		Score:               0,
	}
	b.mu.Unlock()

	// Wait for periodic fill ticker to fire and create slots.
	waitFor(t, 2*time.Second, "periodic fill creates slots after cooldown expiry", func() bool {
		return len(cm.Slots()) > 0
	})

	slots := cm.Slots()
	if len(slots) < 1 {
		t.Fatalf("periodic fill should have created slots, got %d", len(slots))
	}
}

// TestCM_PeriodicFill_SuppressedBeforeBootstrap verifies that the periodic
// fill ticker does not call fill() before bootstrap completes. This ensures
// the peer list is fully loaded before any dial attempts begin.
func TestCM_PeriodicFill_SuppressedBeforeBootstrap(t *testing.T) {
	t.Parallel()

	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.FillInterval = 20 * time.Millisecond

	dialCalled := make(chan struct{}, 10)
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		dialCalled <- struct{}{}
		session := fakePeerSession(addrs[0], "id-"+domain.PeerIdentity(addrs[0]))
		return DialResult{Session: session, ConnectedAddress: addrs[0]}, nil
	}

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	// Do NOT call NotifyBootstrapReady. Wait for several periodic ticks.
	time.Sleep(150 * time.Millisecond)

	// No slots should exist — fill() is suppressed before bootstrap.
	if len(cm.Slots()) != 0 {
		t.Fatalf("expected 0 slots before bootstrap, got %d", len(cm.Slots()))
	}

	select {
	case <-dialCalled:
		t.Fatal("dial should not be called before bootstrap")
	default:
		// expected: no dial
	}
}
