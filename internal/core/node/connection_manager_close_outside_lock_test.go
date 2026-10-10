package node

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// connection_manager_close_outside_lock_test.go pins that the
// ConnectionManager never closes a session while it holds cm.mu. Closing a
// session is I/O — NetCore.Close closes the socket and waits for its writer —
// and the session's onClose takes peerMu, so a close under cm.mu is both I/O
// under a lock and a cm.mu → peerMu edge. Every deactivation path is checked.

// closeLockProbe records, from inside a session's onClose, whether cm.mu was
// free at that moment. TryLock is decisive here: the tests never read the slot
// table while an eviction runs, so the only possible holder is the manager.
type closeLockProbe struct {
	cm     atomic.Pointer[ConnectionManager]
	closed chan bool
}

func newCloseLockProbe() *closeLockProbe {
	return &closeLockProbe{closed: make(chan bool, 4)}
}

func (p *closeLockProbe) onClose() {
	cm := p.cm.Load()
	free := cm.mu.TryLock()
	if free {
		cm.mu.Unlock()
	}
	p.closed <- free
}

func (p *closeLockProbe) dial() func(context.Context, []domain.PeerAddress) (DialResult, error) {
	var dials atomic.Int32
	return func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
		if dials.Add(1) > 1 {
			// Only the first dial gets a probed session; any later one (a
			// reconnect, the manual peer's slot) parks so it adds no close
			// of its own.
			<-ctx.Done()
			return DialResult{}, ctx.Err()
		}
		session := fakePeerSession(addresses[0], domaintest.ID("probe-"+string(addresses[0])))
		session.onClose = p.onClose
		return DialResult{Session: session, ConnectedAddress: addresses[0]}, nil
	}
}

func (p *closeLockProbe) awaitClose(t *testing.T, path string) {
	t.Helper()
	select {
	case free := <-p.closed:
		if !free {
			t.Errorf("%s closed the session while holding cm.mu", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s never closed the session", path)
	}
}

func TestCM_DeactivationClosesTheSessionOutsideCMMu(t *testing.T) {
	slotAddress := mustAddr("10.0.0.1:64646")
	other := mustAddr("10.0.0.2:64646")

	cases := []struct {
		name    string
		banned  bool
		promote bool
		evict   func(t *testing.T, cm *ConnectionManager, maxSlots *atomic.Int32, info SessionInfo, cancel context.CancelFunc)
	}{
		{name: "shrinkToLimit", promote: true, evict: func(_ *testing.T, cm *ConnectionManager, maxSlots *atomic.Int32, _ SessionInfo, _ context.CancelFunc) {
			maxSlots.Store(0)
			cm.EmitHint(NewPeersDiscovered{Count: 1})
		}},
		{name: "RetainOnly", promote: true, evict: func(t *testing.T, cm *ConnectionManager, _ *atomic.Int32, _ SessionInfo, _ context.CancelFunc) {
			if !cm.RetainOnly(context.Background(), other) {
				t.Error("RetainOnly = false on a running event loop")
			}
		}},
		{name: "manual peer eviction", promote: true, evict: func(t *testing.T, cm *ConnectionManager, _ *atomic.Int32, _ SessionInfo, _ context.CancelFunc) {
			if !cm.EmitSlot(ManualPeerRequested{Address: other}) {
				t.Error("the manager refused the manual peer request")
			}
		}},
		{name: "shutdown", promote: true, evict: func(_ *testing.T, _ *ConnectionManager, _ *atomic.Int32, _ SessionInfo, cancel context.CancelFunc) {
			cancel()
		}},
		{name: "active session lost", promote: true, evict: func(t *testing.T, cm *ConnectionManager, _ *atomic.Int32, info SessionInfo, _ context.CancelFunc) {
			if !cm.EmitSlot(ActiveSessionLost{Address: info.Address, WasHealthy: true, SlotGeneration: info.SlotGeneration}) {
				t.Error("the manager refused ActiveSessionLost")
			}
		}},
		{name: "setup failure replacing the slot", banned: true, evict: func(t *testing.T, cm *ConnectionManager, _ *atomic.Int32, info SessionInfo, _ context.CancelFunc) {
			if !cm.EmitSlot(ActiveSessionLost{Address: info.Address, WasHealthy: false, SlotGeneration: info.SlotGeneration}) {
				t.Error("the manager refused ActiveSessionLost")
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := newCloseLockProbe()
			var maxSlots atomic.Int32
			maxSlots.Store(1)
			b := testCMConfig(string(slotAddress))
			b.Cfg.MaxSlotsFn = func() int { return int(maxSlots.Load()) }
			b.Cfg.FillInterval = time.Hour
			b.Cfg.DialFn = probe.dial()
			b.Cfg.IsSetupFailureBannedFn = func(domain.PeerAddress) bool { return tc.banned }
			established := make(chan SessionInfo, 1)
			b.Cfg.OnSessionEstablished = func(info SessionInfo) {
				reportSessionEstablished(t, established, info)
				if tc.promote {
					b.cmPtr.Load().EmitSlot(SessionInitReady{Address: info.Address, SlotGeneration: info.SlotGeneration})
				}
			}
			cm := b.Build()
			probe.cm.Store(cm)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			go cm.Run(ctx)
			<-cm.Ready()
			cm.NotifyBootstrapReady()
			info := awaitSessionEstablished(t, established)
			if tc.promote {
				waitFor(t, 2*time.Second, "the slot to become active", func() bool { return cm.ActiveCount() == 1 })
			}

			tc.evict(t, cm, &maxSlots, info, cancel)
			probe.awaitClose(t, tc.name)
		})
	}
}

// The add_peer eviction picks its victim by PeerProvider.Score, which in
// production reads peer health under peerMu. Scoring under cm.mu would be the
// same cm.mu → peerMu edge as a close under it.
func TestCM_ManualPeerEvictionScoresItsVictimOutsideCMMu(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.FillInterval = time.Hour
	var armed atomic.Bool
	scoredUnderLock := make(chan bool, 8)
	health := b.Cfg.Provider.config.HealthFn
	b.Cfg.Provider.config.HealthFn = func(address domain.PeerAddress) *PeerHealthView {
		if armed.Load() {
			cm := b.cmPtr.Load()
			free := cm.mu.TryLock()
			if free {
				cm.mu.Unlock()
			}
			scoredUnderLock <- !free
		}
		return health(address)
	}
	cm := b.Build()
	cancel := runCM(cm)
	t.Cleanup(cancel)
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "the slot to become active", func() bool { return cm.ActiveCount() == 1 })

	armed.Store(true)
	if !cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.2:64646")}) {
		t.Fatal("the manager refused the manual peer request")
	}
	select {
	case underLock := <-scoredUnderLock:
		if underLock {
			t.Error("the manual-peer eviction scored its victim while holding cm.mu")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the manual-peer eviction never scored a victim")
	}
}

// syncSlotStateRig is a ConnectionManager with a synchronous slot-state
// subscriber that reads the slot table on every publication. A publication
// made while cm.mu is held would make that read wait on the very goroutine
// that publishes — a self-deadlock of the event loop — so every path is
// driven until its publication is seen, or the test fails.
type syncSlotStateRig struct {
	cm          *ConnectionManager
	established chan SessionInfo
	published   chan slotPublication
}

type slotPublication struct {
	address domain.PeerAddress
	state   string
}

// syncSlotStateRigOptions shapes the path a case drives. Zero values keep the
// defaults: a dial that succeeds, promotion to active, no setup ban.
type syncSlotStateRigOptions struct {
	dial      func(context.Context, []domain.PeerAddress) (DialResult, error)
	noPromote bool
	banned    bool
	maxSlots  int
}

func newSyncSlotStateRig(t *testing.T, opts syncSlotStateRigOptions) *syncSlotStateRig {
	t.Helper()
	rig := &syncSlotStateRig{
		established: make(chan SessionInfo, 4),
		published:   make(chan slotPublication, 64),
	}
	b := testCMConfig("10.0.0.1:64646")
	maxSlots := opts.maxSlots
	if maxSlots == 0 {
		maxSlots = 1
	}
	b.Cfg.MaxSlotsFn = func() int { return maxSlots }
	b.Cfg.FillInterval = time.Hour
	if opts.dial != nil {
		b.Cfg.DialFn = opts.dial
	}
	b.Cfg.IsSetupFailureBannedFn = func(domain.PeerAddress) bool { return opts.banned }
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		select {
		case rig.established <- info:
		default:
		}
		if !opts.noPromote {
			b.cmPtr.Load().EmitSlot(SessionInitReady{Address: info.Address, SlotGeneration: info.SlotGeneration})
		}
	}
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	b.Cfg.EventBus = bus
	bus.Subscribe(ebus.TopicSlotStateChanged, func(address domain.PeerAddress, state string) {
		_ = b.cmPtr.Load().Slots()
		select {
		case rig.published <- slotPublication{address: address, state: state}:
		default:
		}
	}, ebus.WithSync())
	rig.cm = b.Build()
	cancel := runCM(rig.cm)
	t.Cleanup(cancel)
	rig.cm.NotifyBootstrapReady()
	return rig
}

func (rig *syncSlotStateRig) awaitPublished(t *testing.T, address domain.PeerAddress, state string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case p := <-rig.published:
			if p.address == address && p.state == state {
				return
			}
		case <-deadline:
			t.Fatalf("%s was never published as %q: a synchronous subscriber reading Slots() deadlocked the event loop",
				address, state)
		}
	}
}

func (rig *syncSlotStateRig) awaitEstablished(t *testing.T) SessionInfo {
	t.Helper()
	return awaitSessionEstablished(t, rig.established)
}

func TestCM_SyncSlotStateSubscriberMayReadTheTable(t *testing.T) {
	slotAddress := mustAddr("10.0.0.1:64646")
	other := mustAddr("10.0.0.2:64646")
	failDial := func(err error) func(context.Context, []domain.PeerAddress) (DialResult, error) {
		return func(context.Context, []domain.PeerAddress) (DialResult, error) { return DialResult{}, err }
	}
	removed := ""

	cases := []struct {
		name  string
		opts  syncSlotStateRigOptions
		drive func(t *testing.T, rig *syncSlotStateRig)
	}{
		{name: "fill dialing, initializing, active", drive: func(t *testing.T, rig *syncSlotStateRig) {
			rig.awaitPublished(t, slotAddress, domain.SlotStateDialing.String())
			rig.awaitPublished(t, slotAddress, domain.SlotStateInitializing.String())
			rig.awaitPublished(t, slotAddress, domain.SlotStateActive.String())
		}},
		{name: "removal by eviction", drive: func(t *testing.T, rig *syncSlotStateRig) {
			rig.awaitPublished(t, slotAddress, domain.SlotStateActive.String())
			// Asynchronous, so a deadlocked loop fails the publication wait
			// below instead of hanging the test inside RetainOnly.
			result := retainOnlyAsync(context.Background(), rig.cm, other)
			rig.awaitPublished(t, slotAddress, removed)
			if !awaitRetainOnlyResult(t, result, "after the eviction was published") {
				t.Fatal("RetainOnly = false on a running event loop")
			}
		}},
		{name: "manual peer dialing", opts: syncSlotStateRigOptions{maxSlots: 2}, drive: func(t *testing.T, rig *syncSlotStateRig) {
			rig.awaitPublished(t, slotAddress, domain.SlotStateActive.String())
			if !rig.cm.EmitSlot(ManualPeerRequested{Address: other}) {
				t.Fatal("the manager refused the manual peer request")
			}
			rig.awaitPublished(t, other, domain.SlotStateDialing.String())
		}},
		{name: "retry wait after a dial failure", opts: syncSlotStateRigOptions{dial: failDial(errors.New("refused"))}, drive: func(t *testing.T, rig *syncSlotStateRig) {
			rig.awaitPublished(t, slotAddress, domain.SlotStateRetryWait.String())
		}},
		{name: "removal of a slot with no session", opts: syncSlotStateRigOptions{dial: failDial(errIncompatibleProtocol)}, drive: func(t *testing.T, rig *syncSlotStateRig) {
			rig.awaitPublished(t, slotAddress, removed)
		}},
		{name: "reconnecting after a healthy session was lost", drive: func(t *testing.T, rig *syncSlotStateRig) {
			info := rig.awaitEstablished(t)
			rig.awaitPublished(t, slotAddress, domain.SlotStateActive.String())
			rig.cm.EmitSlot(ActiveSessionLost{Address: slotAddress, WasHealthy: true, SlotGeneration: info.SlotGeneration})
			rig.awaitPublished(t, slotAddress, domain.SlotStateReconnecting.String())
		}},
		{name: "retry wait after a setup failure", opts: syncSlotStateRigOptions{noPromote: true}, drive: func(t *testing.T, rig *syncSlotStateRig) {
			info := rig.awaitEstablished(t)
			rig.cm.EmitSlot(ActiveSessionLost{Address: slotAddress, WasHealthy: false, SlotGeneration: info.SlotGeneration})
			rig.awaitPublished(t, slotAddress, domain.SlotStateRetryWait.String())
		}},
		{name: "removal after a banned setup failure", opts: syncSlotStateRigOptions{noPromote: true, banned: true}, drive: func(t *testing.T, rig *syncSlotStateRig) {
			info := rig.awaitEstablished(t)
			rig.cm.EmitSlot(ActiveSessionLost{Address: slotAddress, WasHealthy: false, SlotGeneration: info.SlotGeneration})
			rig.awaitPublished(t, slotAddress, removed)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.drive(t, newSyncSlotStateRig(t, tc.opts))
		})
	}
}

// handleManualPeer reads the slot limit through MaxSlotsFn, a callback into
// the embedder; it must run with cm.mu released like every other one.
func TestCM_ManualPeerReadsTheSlotLimitOutsideCMMu(t *testing.T) {
	b := testCMConfig("10.0.0.1:64646")
	b.Cfg.FillInterval = time.Hour
	var armed atomic.Bool
	readUnderLock := make(chan bool, 8)
	b.Cfg.MaxSlotsFn = func() int {
		if armed.Load() {
			cm := b.cmPtr.Load()
			free := cm.mu.TryLock()
			if free {
				cm.mu.Unlock()
			}
			readUnderLock <- !free
		}
		return 1
	}
	cm := b.Build()
	cancel := runCM(cm)
	t.Cleanup(cancel)
	cm.NotifyBootstrapReady()
	waitFor(t, 2*time.Second, "the slot to become active", func() bool { return cm.ActiveCount() == 1 })

	armed.Store(true)
	if !cm.EmitSlot(ManualPeerRequested{Address: mustAddr("10.0.0.2:64646")}) {
		t.Fatal("the manager refused the manual peer request")
	}
	select {
	case underLock := <-readUnderLock:
		if underLock {
			t.Error("handleManualPeer called MaxSlotsFn while holding cm.mu")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handleManualPeer never read the slot limit")
	}
}
