package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// withdrawal_shutdown_test.go pins the shutdown side of the route-withdrawal
// grace period: a pending withdrawal is an armed time.AfterFunc, and a timer
// that is still armed when Run returns fires later against a node whose
// routing plane its owner has already been told is stopped.

// pendingWithdrawalCount reads the number of armed withdrawal timers under the
// mutex that guards them.
func pendingWithdrawalCount(svc *Service) int {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	return len(svc.pendingWithdrawals)
}

func withdrawalTestPeer(t *testing.T) domain.PeerIdentity {
	t.Helper()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer identity: %v", err)
	}
	return domain.PeerIdentityFromWire(id.Address)
}

// TestAWithdrawalArmedBeforeShutdownDoesNotFireInsideTheJoin: a pending
// withdrawal is cancelled BEFORE the lifecycle join, not after it. The join
// waits for every outbound session goroutine to unwind, and that can take as
// long as their sockets take to close; a timer armed before the shutdown and
// still pending when the join starts would fire somewhere inside it — tearing
// a direct route out of the table and fanning the withdrawal out while the
// loops that consume it are being stopped. Arming DURING the join needs no
// such ordering: the shutdown flag refuses it (see the test below).
//
// The join is held open by a lifecycle loop standing in for an unwinding
// session, which keeps it open until the pending withdrawal is gone — cleared
// at once by a cancel that ran before the join, or only by the timer itself,
// one grace period later, if the cancel waits behind the join. What is
// observed is the timer's EFFECT: executeDeferredWithdrawal removes the
// peer's direct route, and a cancelled timer never runs it.
//
// The mutation this kills: registering the withdrawal cancel so that it runs
// after the lifecycle join.
func TestAWithdrawalArmedBeforeShutdownDoesNotFireInsideTheJoin(t *testing.T) {
	t.Parallel()

	// Long enough that a cancel which runs before the join always wins
	// against it even on a loaded machine. It costs nothing on the correct
	// order — the cancel clears the entry within milliseconds — and only the
	// losing order waits for it.
	const grace = 2 * time.Second
	const joinHoldLimit = 6 * time.Second

	svc := newHarnessProbeService(t, domain.NodeTypeFull)
	svc.routeWithdrawalGracePeriodTest = grace
	peer := withdrawalTestPeer(t)

	svc.goRunLoop(func() {
		<-svc.runCtx.Done()
		deadline := time.Now().Add(joinHoldLimit)
		for withdrawalPending(svc, peer) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		// The entry is gone either because the cancel cleared it — which
		// also raised the shutdown flag — or because the timer claimed it and
		// is now running its body. In the second case the route removal lands
		// a moment AFTER the entry disappears; returning at once could let Run
		// return in between and pass the check below on a timing accident.
		// Hold the join until the body has had its effect.
		if !withdrawalsShutDownNow(svc) {
			for len(svc.routingTable.Lookup(peer)) > 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
		}
	})

	running := runServiceForTest(t.Context(), svc)
	if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("AwaitReady = %v, want nil", err)
	}
	if _, err := svc.routingTable.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	if len(svc.routingTable.Lookup(peer)) == 0 {
		t.Fatal("the direct route was not installed: the premise of this test never armed")
	}
	svc.maybeScheduleDeferredWithdrawal(peer, nil)
	if !withdrawalPending(svc, peer) {
		t.Fatal("no withdrawal was armed: the premise of this test never armed")
	}

	if err := running.Stop(withBudget(t, harnessGenerousBudget+joinHoldLimit)); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	if len(svc.routingTable.Lookup(peer)) == 0 {
		t.Fatal("a withdrawal armed before the shutdown fired inside the lifecycle join and removed the " +
			"direct route: the pending withdrawals must be cancelled before the join, not after it")
	}
}

func withdrawalsShutDownNow(svc *Service) bool {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	return svc.withdrawalsShutDown
}

func withdrawalPending(svc *Service, peer domain.PeerIdentity) bool {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	_, pending := svc.pendingWithdrawals[peer]
	return pending
}

// TestAWithdrawalAskedForAfterRunReturnedIsNotArmed: cancelling what is pending
// is not enough on its own, because a session close can still be reported
// after the cancel — by a caller that outlives Run. Once the withdrawals have
// been cancelled for shutdown, a new one is refused rather than armed.
//
// The mutation this kills: dropping the shutdown check from
// maybeScheduleDeferredWithdrawal.
func TestAWithdrawalAskedForAfterRunReturnedIsNotArmed(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeFull)
	running := runServiceForTest(t.Context(), svc)
	if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("AwaitReady = %v, want nil", err)
	}
	if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	svc.maybeScheduleDeferredWithdrawal(withdrawalTestPeer(t), nil)

	if n := pendingWithdrawalCount(svc); n != 0 {
		t.Fatalf("%d route withdrawal timer(s) armed after Run returned, want 0", n)
	}
}

// TestASynchronousWithdrawalOnAStoppedNodeIsDropped: with the grace period
// disabled the withdrawal runs inline instead of on a timer. On a node whose
// Run has returned it is dropped like the timed one — the shutdown already
// discards pending withdrawals, and a stopped node has no announce plane to
// fan a withdrawal out to.
//
// The mutation this kills: checking the shutdown flag only on the timed path.
func TestASynchronousWithdrawalOnAStoppedNodeIsDropped(t *testing.T) {
	t.Parallel()

	svc := newHarnessProbeService(t, domain.NodeTypeFull)
	svc.routeWithdrawalGracePeriodTest = -1
	peer := withdrawalTestPeer(t)

	running := runServiceForTest(t.Context(), svc)
	if err := running.AwaitReady(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("AwaitReady = %v, want nil", err)
	}
	if err := running.Stop(withBudget(t, harnessGenerousBudget)); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}

	if _, err := svc.routingTable.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	svc.maybeScheduleDeferredWithdrawal(peer, nil)

	if len(svc.routingTable.Lookup(peer)) == 0 {
		t.Fatal("a synchronous withdrawal ran on a node whose Run had returned")
	}
}
