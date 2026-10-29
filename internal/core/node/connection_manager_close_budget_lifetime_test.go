package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// Hold the actual owned net.Conn.Close before delegation, not merely the
// session callback (which can run before conn.Close). This is an in-process
// transport lifetime test, not a kernel TCP/socket count or Close hard bound.
type heldCMBudgetConn struct {
	net.Conn
	cm          atomic.Pointer[ConnectionManager]
	entered     chan bool
	allow       chan struct{}
	returned    chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	calls       atomic.Int32
	afterClose  error
	err         error
	cleanupDone <-chan struct{} // Optional parked-fixture failure cleanup only.
}

func (c *heldCMBudgetConn) release() { c.releaseOnce.Do(func() { close(c.allow) }) }

func (c *heldCMBudgetConn) Close() error {
	c.once.Do(func() {
		c.calls.Add(1)
		cm := c.cm.Load()
		free := cm != nil && cm.mu.TryLock()
		if free {
			cm.mu.Unlock()
		}
		c.entered <- free
		select {
		case <-c.allow:
		case <-c.cleanupDone:
		}
		c.err = errors.Join(c.Conn.Close(), c.afterClose)
		close(c.returned)
	})
	return c.err
}

type heldCMCloseRig struct {
	cm        *ConnectionManager
	budget    *connbudget.Budget
	conn      *heldCMBudgetConn
	session   *peerSession
	maxSlots  atomic.Int32
	cancel    context.CancelFunc
	done      chan struct{}
	dials     chan domain.PeerAddress
	teardowns chan SessionInfo
	initial   SessionInfo
	attempts  atomic.Int32
}

func startHeldCMClose(t *testing.T, promote bool, closeError error, bus *ebus.Bus) *heldCMCloseRig {
	t.Helper()
	local, remote := net.Pipe()
	rig := &heldCMCloseRig{
		budget: mustBudget(t, connbudget.Config{Total: 1, MaxOutbound: 1, MaxInbound: 1, MaxAuxiliary: 1}),
		conn:   &heldCMBudgetConn{Conn: local, entered: make(chan bool, 1), allow: make(chan struct{}), returned: make(chan struct{}), afterClose: closeError},
		done:   make(chan struct{}), dials: make(chan domain.PeerAddress, 4), teardowns: make(chan SessionInfo, 4),
	}
	rig.session = &peerSession{address: mustAddr("10.0.0.1:64646"), peerIdentity: domaintest.ID("held-close"), conn: rig.conn, connID: 1}
	rig.maxSlots.Store(1)
	b := testCMConfig(string(rig.session.address))
	b.Cfg.Budget = rig.budget
	b.Cfg.MaxSlotsFn = func() int { return int(rig.maxSlots.Load()) }
	b.Cfg.FillInterval = time.Hour
	b.Cfg.EventBus = bus
	b.Cfg.DialFn = func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
		n := rig.attempts.Add(1)
		select {
		case rig.dials <- addresses[0]:
		default:
		}
		if n == 1 {
			return DialResult{Session: rig.session, ConnectedAddress: addresses[0]}, nil
		}
		// A replacement is observable at actual DialFn entry and remains an
		// accounted attempt until cleanup cancels it. It opens no socket.
		<-ctx.Done()
		return DialResult{}, ctx.Err()
	}
	established := make(chan SessionInfo, 1)
	b.Cfg.OnSessionEstablished = func(info SessionInfo) {
		reportSessionEstablished(t, established, info)
		if promote {
			b.cmPtr.Load().EmitSlot(SessionInitReady{Address: info.Address, SlotGeneration: info.SlotGeneration})
		}
	}
	b.Cfg.OnSessionTeardown = func(info SessionInfo) {
		select {
		case rig.teardowns <- info:
		default:
		}
	}
	rig.cm = b.Build()
	rig.conn.cm.Store(rig.cm)
	ctx, cancel := context.WithCancel(context.Background())
	rig.cancel = cancel
	t.Cleanup(func() {
		rig.conn.release()
		cancel()
		select {
		case <-rig.done:
		case <-time.After(budgetSettleTimeout):
			t.Error("owned CM loop did not join")
		}
		_ = local.Close()
		_ = remote.Close()
	})
	go func() { rig.cm.Run(ctx); close(rig.done) }()
	select {
	case <-rig.cm.Ready():
	case <-time.After(budgetSettleTimeout):
		t.Fatal("CM never became ready")
	}
	rig.cm.NotifyBootstrapReady()
	rig.initial = awaitSessionEstablished(t, established)
	want := domain.SlotStateInitializing
	if promote {
		want = domain.SlotStateActive
	}
	waitFor(t, budgetSettleTimeout, "initial held session state", func() bool { slots := rig.cm.Slots(); return len(slots) == 1 && slots[0].State == want })
	select {
	case address := <-rig.dials:
		if address != rig.session.address {
			t.Fatal("unexpected initial dial")
		}
	default:
		t.Fatal("initial DialFn entry missing")
	}
	return rig
}

func requireHeldCMBudget(t *testing.T, budget *connbudget.Budget, used int) {
	t.Helper()
	if snap := budget.Snapshot(); snap.Used != used || snap.Outbound != used {
		t.Fatalf("capacity escaped held transport: %+v", snap)
	}
	for _, direction := range []connbudget.Direction{connbudget.DirectionInbound, connbudget.DirectionAuxiliary} {
		unit, err := budget.Reserve(direction)
		if unit != nil {
			unit.Release()
		}
		if !errors.Is(err, connbudget.ErrTotalExhausted) {
			t.Fatalf("independent %s reserve spent held capacity: %v", direction, err)
		}
	}
}

func requireOneReusableCMUnit(t *testing.T, budget *connbudget.Budget) {
	t.Helper()
	for _, direction := range []connbudget.Direction{connbudget.DirectionInbound, connbudget.DirectionAuxiliary} {
		unit, err := budget.Reserve(direction)
		if err != nil {
			t.Fatalf("closed unit not reusable by %s: %v", direction, err)
		}
		other := connbudget.DirectionInbound
		if direction == other {
			other = connbudget.DirectionAuxiliary
		}
		extra, extraErr := budget.Reserve(other)
		if extra != nil {
			extra.Release()
		}
		unit.Release()
		if !errors.Is(extraErr, connbudget.ErrTotalExhausted) {
			t.Fatalf("one close invented multiple units: %v", extraErr)
		}
	}
}

func TestCMRemovedSessionHoldsBudgetUntilOwnedCloseReturns(t *testing.T) {
	for _, promote := range []bool{false, true} {
		for _, path := range []string{"shrink", "retain-only", "shutdown"} {
			t.Run(fmt.Sprintf("%s/active-%v", path, promote), func(t *testing.T) {
				rig := startHeldCMClose(t, promote, nil, nil)
				var retained <-chan bool
				switch path {
				case "shrink":
					rig.maxSlots.Store(0)
					rig.cm.EmitHint(NewPeersDiscovered{Count: 1})
				case "retain-only":
					retained = retainOnlyAsync(context.Background(), rig.cm, mustAddr("10.0.0.2:64646"))
				case "shutdown":
					rig.cancel()
				}
				select {
				case free := <-rig.conn.entered:
					if !free {
						t.Fatal("actual Close ran under cm.mu")
					}
				case <-time.After(budgetSettleTimeout):
					t.Fatal("actual Close never entered")
				}
				requireHeldCMBudget(t, rig.budget, 1)
				select {
				case <-rig.conn.returned:
					t.Fatal("underlying close escaped its owned gate")
				default:
				}
				rig.conn.release()
				select {
				case <-rig.teardowns:
				case <-time.After(budgetSettleTimeout):
					t.Fatal("close ownership never settled")
				}
				if retained != nil && !awaitRetainOnlyResult(t, retained, "after actual Close returned") {
					t.Fatal("retain-only not applied")
				}
				if path == "shutdown" {
					awaitClosed(t, rig.done, "shutdown after actual Close")
				}
				requireOneReusableCMUnit(t, rig.budget)
			})
		}
	}
}

func TestCMManualReplacementTransfersBudgetOnlyAfterOwnedClose(t *testing.T) {
	for _, promote := range []bool{false, true} {
		t.Run(fmt.Sprintf("active-%v", promote), func(t *testing.T) {
			rig := startHeldCMClose(t, promote, nil, nil)
			manual := mustAddr("10.0.0.2:64646")
			if !rig.cm.EmitSlot(ManualPeerRequested{Address: manual}) {
				t.Fatal("manual request was not enqueued")
			}
			select {
			case free := <-rig.conn.entered:
				if !free {
					t.Fatal("manual eviction closed under cm.mu")
				}
			case <-time.After(budgetSettleTimeout):
				t.Fatal("manual eviction never closed victim")
			}
			requireHeldCMBudget(t, rig.budget, 1)
			select {
			case address := <-rig.dials:
				t.Fatalf("replacement %s dialled before old Close returned", address)
			default:
			}
			rig.conn.release()
			select {
			case address := <-rig.dials:
				if address != manual {
					t.Fatalf("replacement dialled %s", address)
				}
			case <-time.After(budgetSettleTimeout):
				t.Fatal("manual request lost while victim unit stayed held")
			}
			awaitClosed(t, rig.conn.returned, "victim's underlying Close")
			slots := rig.cm.Slots()
			if len(slots) != 1 || slots[0].Address != manual || slots[0].Generation == rig.initial.SlotGeneration {
				t.Fatalf("manual replacement generation/slot invalid: %+v", slots)
			}
			requireHeldCMBudget(t, rig.budget, 1)
			if rig.attempts.Load() != 2 {
				t.Fatal("replacement dial did not enter exactly once")
			}
		})
	}
}

func TestCMRemovalPublicationCannotSpendStillOpenTransportUnit(t *testing.T) {
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	type observation struct {
		snapshot           connbudget.Stats
		inbound, auxiliary error
	}
	observed := make(chan observation, 1)
	var callbackRig atomic.Pointer[heldCMCloseRig]
	bus.Subscribe(ebus.TopicSlotStateChanged, func(_ domain.PeerAddress, state string) {
		if state != "" {
			return
		}
		rig := callbackRig.Load()
		if rig == nil {
			return
		}
		_ = rig.cm.Slots()
		value := observation{snapshot: rig.budget.Snapshot()}
		unit, err := rig.budget.Reserve(connbudget.DirectionInbound)
		value.inbound = err
		if unit != nil {
			unit.Release()
		}
		unit, err = rig.budget.Reserve(connbudget.DirectionAuxiliary)
		value.auxiliary = err
		if unit != nil {
			unit.Release()
		}
		select {
		case observed <- value:
		default:
		}
	}, ebus.WithSync())
	rig := startHeldCMClose(t, true, nil, bus)
	callbackRig.Store(rig)
	result := retainOnlyAsync(context.Background(), rig.cm, mustAddr("10.0.0.2:64646"))
	select {
	case value := <-observed:
		if value.snapshot.Outbound != 1 || !errors.Is(value.inbound, connbudget.ErrTotalExhausted) || !errors.Is(value.auxiliary, connbudget.ErrTotalExhausted) {
			t.Fatalf("removal publication freed old transport early: %+v", value)
		}
	case <-time.After(budgetSettleTimeout):
		t.Fatal("synchronous removal subscriber did not finish")
	}
	select {
	case free := <-rig.conn.entered:
		if !free {
			t.Fatal("close held cm.mu")
		}
	case <-time.After(budgetSettleTimeout):
		t.Fatal("close never followed publication")
	}
	rig.conn.release()
	if !awaitRetainOnlyResult(t, result, "after publication and owned close") {
		t.Fatal("retention failed")
	}
	requireOneReusableCMUnit(t, rig.budget)
}

func TestCMOrphanGenerationKeepsBudgetThroughLateOwnedClose(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-success-%v", success), func(t *testing.T) {
			budget := mustBudget(t, connbudget.Config{Total: 2, MaxAuxiliary: 1})
			unrelated, err := budget.Reserve(connbudget.DirectionOutbound)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(unrelated.Release)
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
			held := &heldCMBudgetConn{Conn: local, entered: make(chan bool, 1), allow: make(chan struct{}), returned: make(chan struct{})}
			outcome := func(addresses []domain.PeerAddress) (DialResult, error) {
				if !success {
					_ = local.Close()
					return DialResult{}, errors.New("parked dial refused")
				}
				return DialResult{Session: &peerSession{address: addresses[0], peerIdentity: domaintest.ID("late"), conn: held, connID: 1}, ConnectedAddress: addresses[0]}, nil
			}
			dial := startParkedDial(t, budget, outcome, func(cfg *ConnectionManagerConfig) {
				actual := cfg.DialFn
				cfg.DialFn = func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
					// Ensures helper failure cleanup can cancel/join even if its
					// startup wait fails before this test receives the fixture.
					held.cleanupDone = ctx.Done()
					return actual(ctx, addresses)
				}
			})
			t.Cleanup(held.release) // Before parked fixture's cancel/join cleanup.
			held.cm.Store(dial.cm)
			dial.cm.mu.Lock()
			old := dial.cm.slots[0]
			generation := old.Generation
			dial.cm.removeSlotLocked(old)
			parked := dial.cm.orphanReservations[generation] != nil
			dial.cm.mu.Unlock()
			if !parked {
				t.Fatal("removed live dial lost its generation-owned reservation")
			}
			requireHeldCMBudget(t, budget, 2)
			dial.releaseDial()
			if success {
				select {
				case free := <-held.entered:
					if !free {
						t.Fatal("stale Close held cm.mu")
					}
				case <-time.After(budgetSettleTimeout):
					t.Fatal("late success did not enter owned Close")
				}
				requireHeldCMBudget(t, budget, 2)
				held.release()
			}
			waitForBudget(t, budget, "only unrelated unit remains", func(s connbudget.Stats) bool { return s.Outbound == 1 })
			dial.cm.mu.Lock()
			remaining := len(dial.cm.orphanReservations)
			dial.cm.mu.Unlock()
			if remaining != 0 {
				t.Fatal("terminal generation kept orphan reservation")
			}
			if held.calls.Load() != map[bool]int32{false: 0, true: 1}[success] {
				t.Fatal("wrong late outcome close count")
			}
			requireOneReusableCMUnit(t, budget)
			if budget.Snapshot().Outbound != 1 {
				t.Fatal("stale generation released unrelated owner's unit")
			}
		})
	}
}

func TestCMCloseErrorAfterActualCloseReleasesExactlyOnce(t *testing.T) {
	injected := errors.New("close reported after owned endpoint closed")
	rig := startHeldCMClose(t, true, injected, nil)
	result := retainOnlyAsync(context.Background(), rig.cm, mustAddr("10.0.0.2:64646"))
	select {
	case free := <-rig.conn.entered:
		if !free {
			t.Fatal("close held cm.mu")
		}
	case <-time.After(budgetSettleTimeout):
		t.Fatal("close did not enter")
	}
	requireHeldCMBudget(t, rig.budget, 1)
	rig.conn.release()
	if !awaitRetainOnlyResult(t, result, "after error-returning actual Close") {
		t.Fatal("retention did not settle")
	}
	if err := rig.session.Close(); !errors.Is(err, injected) {
		t.Fatalf("repeat Close lost original result: %v", err)
	}
	if rig.conn.calls.Load() != 1 {
		t.Fatal("actual endpoint Close ran more than once")
	}
	requireOneReusableCMUnit(t, rig.budget)
}
