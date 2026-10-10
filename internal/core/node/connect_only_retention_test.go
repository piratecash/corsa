package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// connect_only_retention_test.go drives connect_only end to end through the
// ConnectionManager that NewService builds — its real ConnectOnlyFn wiring to
// connectOnlyTarget, its real PeerProvider pin gate — with only the dial
// replaced, and pins the level-triggered rule: while a pin is live, the slot
// table holds only the live pin, whichever command wrote it.

var (
	retentionPinned = domain.PeerAddress("10.0.0.1:64646")
	retentionOthers = []domain.PeerAddress{"10.0.0.2:64646", "10.0.0.3:64646"}
)

// retentionUnadmittable is a connect_only target applyAddPeer rejects (a
// multicast address is structurally undialable), so the command takes its
// rollback path.
const retentionUnadmittable = "224.0.0.1:64646"

// newRetentionService returns a Service whose own ConnectionManager runs with
// three outbound slots — the pin-to-be and two others — all parked in a dial
// that never completes, so the table changes only by eviction.
func newRetentionService(t *testing.T) *Service {
	t.Helper()
	svc := newTestService(t, config.NodeTypeFull)
	svc.connManager.config.MaxSlotsFn = func() int { return 3 }
	svc.connManager.config.FillInterval = time.Hour
	svc.connManager.config.DialFn = func(ctx context.Context, _ []domain.PeerAddress) (DialResult, error) {
		<-ctx.Done()
		return DialResult{}, ctx.Err()
	}
	for _, address := range append([]domain.PeerAddress{retentionPinned}, retentionOthers...) {
		svc.peerProvider.Add(address, domain.PeerSourceBootstrap)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		svc.connManager.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	<-svc.connManager.Ready()
	svc.connManager.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "3 outbound slots", func() bool { return svc.connManager.SlotCount() == 3 })
	return svc
}

func assertOnlyPinnedSlot(t *testing.T, svc *Service, pin domain.PeerAddress, after string) {
	t.Helper()
	if got := slotAddresses(svc.connManager); len(got) != 1 || got[0] != pin {
		t.Errorf("slots after %s = %v, want only the pinned %s", after, got, pin)
	}
}

func TestEnableConnectOnly_DropsOtherOutboundSlotsBeforeReplying(t *testing.T) {
	svc := newRetentionService(t)

	reply := svc.enableConnectOnly(context.Background(), string(retentionPinned))
	if reply.Type != "ok" {
		t.Fatalf("connect_only reply = %+v, want ok", reply)
	}
	// No wait: the reply is only sent once the other slots are gone.
	assertOnlyPinnedSlot(t, svc, retentionPinned, "connect_only")
}

// A connect_only that fails leaves the node as it was: once the command has
// returned — and a fill and a retention have run after it — the pin is
// unchanged and the pinned peer's slot is the same slot, at the same
// generation. This checks the end state only; that the failing target is
// never live even while the command is in flight is
// TestConnectOnly_AFailedCommandNeverMakesItsTargetLive.
func TestConnectOnlyFailure_LeavesThePinnedSlotAlone(t *testing.T) {
	svc := newRetentionService(t)
	if reply := svc.enableConnectOnly(context.Background(), string(retentionPinned)); reply.Type != "ok" {
		t.Fatalf("connect_only %s reply = %+v, want ok", retentionPinned, reply)
	}
	assertOnlyPinnedSlot(t, svc, retentionPinned, "connect_only")
	before := svc.connManager.Slots()[0].Generation

	reply := svc.enableConnectOnly(context.Background(), retentionUnadmittable)
	if reply.Type != "error" {
		t.Fatalf("connect_only %s reply = %+v, want an admission error", retentionUnadmittable, reply)
	}
	// A fill after the failure must still see the old pin.
	svc.connManager.EmitHint(NewPeersDiscovered{Count: 1})
	if !svc.connManager.RetainOnly(context.Background(), retentionPinned) {
		t.Fatal("RetainOnly = false on a running event loop")
	}

	if got, ok := svc.connectOnlyTarget(); !ok || got != retentionPinned {
		t.Fatalf("pin after a failed connect_only = %q (%v), want %s unchanged", got, ok, retentionPinned)
	}
	assertOnlyPinnedSlot(t, svc, retentionPinned, "the failed connect_only")
	if after := svc.connManager.Slots()[0].Generation; after != before {
		t.Errorf("pinned slot generation %d → %d: a failed connect_only disturbed the pinned peer's slot", before, after)
	}
}

// The fail-closed startup seed pins egress to a sentinel no peer can equal.
// It is applied right after the ConnectionManager starts and before bootstrap
// lets it fill, so the only outbound slot that can already exist is one an
// operator add_peer opened in that window. The sentinel is a live pin like any
// other: that slot must go.
func TestApplyStartupConnectOnly_FailClosedDropsAnEarlyOperatorSlot(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	svc.connManager.config.FillInterval = time.Hour
	svc.connManager.config.DialFn = func(ctx context.Context, _ []domain.PeerAddress) (DialResult, error) {
		<-ctx.Done()
		return DialResult{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		svc.connManager.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	<-svc.connManager.Ready()

	if reply := svc.addPeerFrame(protocol.Frame{Type: "add_peer", Peers: []string{string(retentionPinned)}}); reply.Type != "ok" {
		t.Fatalf("early add_peer reply = %+v, want ok", reply)
	}
	waitFor(t, 2*time.Second, "the early operator slot", func() bool { return svc.connManager.SlotCount() == 1 })

	svc.cfg.ConnectOnly = retentionUnadmittable
	svc.applyStartupConnectOnly(context.Background())

	if got, ok := svc.connectOnlyTarget(); !ok || got != connectOnlyBlockedSentinel {
		t.Fatalf("pin after an invalid startup seed = %q (%v), want the fail-closed sentinel", got, ok)
	}
	if got := svc.connManager.SlotCount(); got != 0 {
		t.Errorf("outbound slots under the fail-closed sentinel = %v, want none", slotAddresses(svc.connManager))
	}
}

// Without a running event loop there is nothing to drop and nothing to wait
// for: the command must neither block nor fail, and the pin is still set.
func TestEnableConnectOnly_CMNotRunning_PinsWithoutBlocking(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if svc.retainOnlyPinnedOutbound(context.Background(), retentionPinned) {
			t.Error("retainOnlyPinnedOutbound = true without an event loop")
		}
		if reply := svc.enableConnectOnly(context.Background(), string(retentionPinned)); reply.Type != "ok" {
			t.Errorf("connect_only reply = %+v, want ok", reply)
		}
	}()
	awaitClosed(t, done, "connect_only to return without a running ConnectionManager")

	if got, ok := svc.connectOnlyTarget(); !ok || got != retentionPinned {
		t.Errorf("pin = %q (%v), want %s", got, ok, retentionPinned)
	}
}

// A connect_only whose caller has gone away must not stay parked on a busy
// ConnectionManager: the operator dial it enqueues waits on the request's
// context, not only on the manager's.
func TestEnableConnectOnly_AbandonedRequestDoesNotWaitOnABusyLoop(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	cm := svc.connManager
	cm.config.MaxSlotsFn = func() int { return 1 }
	cm.config.FillInterval = time.Hour
	cm.config.DialFn = func(_ context.Context, addresses []domain.PeerAddress) (DialResult, error) {
		return DialResult{Session: fakePeerSession(addresses[0], domaintest.ID("busy-loop-peer")), ConnectedAddress: addresses[0]}, nil
	}
	gate := make(chan struct{})
	var releaseOnce, enteredOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }
	entered := make(chan struct{})
	cm.config.OnSessionEstablished = func(SessionInfo) {
		enteredOnce.Do(func() { close(entered) })
		<-gate
	}
	svc.peerProvider.Add(retentionOthers[0], domain.PeerSourceBootstrap)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cm.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	// Registered after the manager's cleanup, so it runs first.
	t.Cleanup(releaseGate)
	<-cm.Ready()
	cm.NotifyBootstrapReady()
	awaitClosed(t, entered, "the event loop to be held inside OnSessionEstablished")
	// Fill the slot-event queue behind the held loop with harmless stale
	// events, so any further send blocks for as long as the loop is held.
	for len(cm.slotEvents) < cap(cm.slotEvents) {
		cm.slotEvents <- SessionInitReady{Address: "203.0.113.1:64646", SlotGeneration: 1 << 62}
	}

	requestCtx, abandon := context.WithCancel(context.Background())
	abandon()
	done := make(chan protocol.Frame, 1)
	go func() { done <- svc.enableConnectOnly(requestCtx, string(retentionPinned)) }()

	select {
	case reply := <-done:
		if reply.Type != "ok" {
			t.Errorf("connect_only reply = %+v, want ok: the pin is set even when nothing could be enqueued", reply)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an abandoned connect_only stayed blocked on the busy event loop")
	}
}
