package node

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// conn_budget_dial_test.go covers the auxiliary outbound paths — the ones that
// do not belong to a connection-manager slot.
//
// ⚠️ Review found these open real sockets while the shared ceiling was already
// exhausted, and they did not appear in the budget diagnostic at all. A
// ceiling with side doors is not a ceiling; these tests are the doors.

// budgetTestService builds the smallest Service that can dial: a wired budget
// and an identity. Nothing here starts Run, so no background work is involved.
func budgetTestService(t *testing.T, budget *connbudget.Budget) *Service {
	t.Helper()
	return &Service{
		cfg:        config.Node{},
		identity:   &identity.Identity{Address: "self"},
		connBudget: budget,
	}
}

// listenLoopback opens a real listener so the dial has somewhere to land: the
// point of these tests is what the budget does, so the socket must be real
// rather than faked away.
func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold the connection; the tests only care about admission.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	return ln
}

// TestAuxiliaryDialIsRefusedWhenTheCeilingIsFull is the P1 in one assertion: an
// exhausted ceiling must mean no socket, not a socket the budget cannot see.
func TestAuxiliaryDialIsRefusedWhenTheCeilingIsFull(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 1})
	if _, err := budget.Reserve(connbudget.DirectionInbound); err != nil {
		t.Fatalf("pre-reserve: %v", err)
	}

	ln := listenLoopback(t)
	svc := budgetTestService(t, budget)
	address := domain.PeerAddress(ln.Addr().String())

	conn, err := svc.dialPeerWithBudget(context.Background(), address, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("dialPeerWithBudget opened a socket with the ceiling exhausted")
	}
	if !errors.Is(err, connbudget.ErrTotalExhausted) {
		t.Fatalf("error = %v, want ErrTotalExhausted", err)
	}

	conn, err = svc.dialAddressWithBudget(address, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("dialAddressWithBudget opened a socket with the ceiling exhausted")
	}
	if !errors.Is(err, connbudget.ErrTotalExhausted) {
		t.Fatalf("error = %v, want ErrTotalExhausted", err)
	}

	if snap := budget.Snapshot(); snap.Auxiliary != 0 {
		t.Fatalf("auxiliary usage = %d after two refusals, want 0", snap.Auxiliary)
	}
}

// TestAuxiliaryDialAccountsAndReturnsCapacity checks the other half: while the
// socket is open the unit is held — so it is visible in the diagnostic and
// counted against the ceiling — and Close gives it back.
func TestAuxiliaryDialAccountsAndReturnsCapacity(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 2})
	ln := listenLoopback(t)
	svc := budgetTestService(t, budget)
	address := domain.PeerAddress(ln.Addr().String())

	conn, err := svc.dialPeerWithBudget(context.Background(), address, time.Second)
	if err != nil {
		t.Fatalf("dialPeerWithBudget: %v", err)
	}
	if snap := budget.Snapshot(); snap.Auxiliary != 1 {
		t.Fatalf("auxiliary usage while the socket is open = %d, want 1", snap.Auxiliary)
	}

	second, err := svc.dialAddressWithBudget(address, time.Second)
	if err != nil {
		t.Fatalf("dialAddressWithBudget: %v", err)
	}
	if snap := budget.Snapshot(); snap.Auxiliary != 2 {
		t.Fatalf("auxiliary usage with two sockets open = %d, want 2", snap.Auxiliary)
	}

	// The ceiling now binds for everyone, including the connection manager.
	if _, err := budget.Reserve(connbudget.DirectionOutbound); !errors.Is(err, connbudget.ErrTotalExhausted) {
		t.Fatalf("third reserve error = %v, want ErrTotalExhausted", err)
	}

	_ = conn.Close()
	_ = second.Close()

	if snap := budget.Snapshot(); snap.Used != 0 {
		t.Fatalf("used = %d after both closes, want 0", snap.Used)
	}
	// Double close must not invent capacity.
	_ = conn.Close()
	_ = second.Close()
	if snap := budget.Snapshot(); snap.Used != 0 {
		t.Fatalf("used = %d after double close, want 0", snap.Used)
	}
	if _, err := budget.Reserve(connbudget.DirectionOutbound); err != nil {
		t.Fatalf("capacity did not come back: %v", err)
	}
}

// TestAuxiliaryDialReleasesOnDialFailure covers the path that produces no
// socket at all: a refused connect must not leave the unit held, or a node
// that cannot reach anybody would gradually forbid itself from trying.
func TestAuxiliaryDialReleasesOnDialFailure(t *testing.T) {
	budget := mustBudget(t, connbudget.Config{Total: 2})
	svc := budgetTestService(t, budget)

	// Port 1 on loopback: refused fast and deterministically.
	dead := domain.PeerAddress("127.0.0.1:1")

	if _, err := svc.dialPeerWithBudget(context.Background(), dead, 500*time.Millisecond); err == nil {
		t.Fatal("expected the dial to fail")
	}
	if _, err := svc.dialAddressWithBudget(dead, 500*time.Millisecond); err == nil {
		t.Fatal("expected the dial to fail")
	}

	if snap := budget.Snapshot(); snap.Used != 0 {
		t.Fatalf("used = %d after two failed dials, want 0 — capacity leaked on the error path", snap.Used)
	}
}

// productionShapedBudget builds the budget a real node builds, for the given
// shared ceiling. The point is that MaxOutbound is SET — production always
// sets it — because that is the condition under which the regression review
// found appears at all.
func productionShapedBudget(t *testing.T, total int) *connbudget.Budget {
	t.Helper()
	cfg := config.Node{MaxTotalConnections: total}
	return mustBudget(t, connbudget.Config{
		Total:           cfg.EffectiveMaxTotalConnections(),
		OutboundReserve: cfg.EffectiveOutboundConnectionReserve(),
		MaxOutbound:     cfg.EffectiveMaxOutgoingPeers(),
		MaxInbound:      cfg.EffectiveMaxIncomingPeers(),
		MaxAuxiliary:    cfg.EffectiveMaxAuxiliaryConnections(),
	})
}

// fillOutboundSlots occupies every persistent outbound position, the way a
// connection manager does in the steady state.
func fillOutboundSlots(t *testing.T, budget *connbudget.Budget) {
	t.Helper()
	snap := budget.Snapshot()
	for i := 0; i < snap.MaxOutbound; i++ {
		if _, err := budget.Reserve(connbudget.DirectionOutbound); err != nil {
			t.Fatalf("occupying outbound slot %d: %v", i, err)
		}
	}
	if _, err := budget.Reserve(connbudget.DirectionOutbound); !errors.Is(err, connbudget.ErrDirectionLimit) {
		t.Fatalf("outbound slots were not actually full: %v", err)
	}
}

// TestAuxiliaryDialWorksWithTheCeilingOffAndSlotsFull is the regression review
// found: with B off, the shared mechanism must take nothing away. The steady
// state of a node is exactly this — all eight outbound positions held by the
// connection manager — so an auxiliary dial refused here is refused forever,
// and waiting cannot help because the manager keeps the slots full by design.
func TestAuxiliaryDialWorksWithTheCeilingOffAndSlotsFull(t *testing.T) {
	budget := productionShapedBudget(t, 0)
	if budget.Enabled() {
		t.Fatal("precondition: the shared ceiling must be off")
	}
	fillOutboundSlots(t, budget)

	ln := listenLoopback(t)
	svc := budgetTestService(t, budget)
	address := domain.PeerAddress(ln.Addr().String())

	conn, err := svc.dialPeerWithBudget(context.Background(), address, time.Second)
	if err != nil {
		t.Fatalf("auxiliary dial refused with the ceiling OFF and peer slots full: %v — "+
			"the budget removed a capability the node had before it existed", err)
	}
	defer func() { _ = conn.Close() }()

	notice, err := svc.dialAddressWithBudget(address, time.Second)
	if err != nil {
		t.Fatalf("notice fallback refused with the ceiling OFF: %v", err)
	}
	defer func() { _ = notice.Close() }()

	// More than the bound an ENABLED ceiling would impose. With B off there
	// is no auxiliary cap at all — the previous policy — and a test that
	// stayed under the default would pass whether or not that holds.
	for i := 0; i < config.DefaultAuxiliaryConnections+2; i++ {
		extra, err := svc.dialAddressWithBudget(address, time.Second)
		if err != nil {
			t.Fatalf("auxiliary dial %d refused with the ceiling OFF: %v — "+
				"a disabled budget must not impose an auxiliary cap", i, err)
		}
		defer func() { _ = extra.Close() }()
	}

	if snap := budget.Snapshot(); snap.Auxiliary != config.DefaultAuxiliaryConnections+4 {
		t.Fatalf("auxiliary usage = %d, want %d — auxiliary dials are still accounted",
			snap.Auxiliary, config.DefaultAuxiliaryConnections+4)
	}
}

// TestAuxiliaryDialWorksWhenSlotsAreFullButCeilingHasRoom is the same shape
// with the ceiling ON: peer slots are a separate limit, so a full
// neighbourhood must not refuse a short-lived dial while B still has capacity.
func TestAuxiliaryDialWorksWhenSlotsAreFullButCeilingHasRoom(t *testing.T) {
	// 16 total: 8 outbound slots plus room for auxiliary dials.
	budget := productionShapedBudget(t, 16)
	if !budget.Enabled() {
		t.Fatal("precondition: the shared ceiling must be on")
	}
	fillOutboundSlots(t, budget)

	ln := listenLoopback(t)
	svc := budgetTestService(t, budget)
	address := domain.PeerAddress(ln.Addr().String())

	conn, err := svc.dialPeerWithBudget(context.Background(), address, time.Second)
	if err != nil {
		t.Fatalf("auxiliary dial refused while the ceiling had room: %v", err)
	}
	defer func() { _ = conn.Close() }()

	snap := budget.Snapshot()
	if snap.Auxiliary != 1 {
		t.Fatalf("auxiliary usage = %d, want 1", snap.Auxiliary)
	}
	if snap.Used != snap.Outbound+snap.Auxiliary {
		t.Fatalf("used = %d does not match outbound %d + auxiliary %d",
			snap.Used, snap.Outbound, snap.Auxiliary)
	}
}

// TestAuxiliaryDialIsRefusedWhenTheSharedCeilingIsExhausted keeps the other
// half honest: exempt from the OUTBOUND limit is not exempt from B. No socket
// may be opened once the shared ceiling is full.
func TestAuxiliaryDialIsRefusedWhenTheSharedCeilingIsExhausted(t *testing.T) {
	budget := productionShapedBudget(t, 8)
	fillOutboundSlots(t, budget) // 8 of 8 — the ceiling is now full too.

	ln := listenLoopback(t)
	svc := budgetTestService(t, budget)
	address := domain.PeerAddress(ln.Addr().String())

	conn, err := svc.dialPeerWithBudget(context.Background(), address, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("auxiliary dial opened a socket with the shared ceiling exhausted")
	}
	if !errors.Is(err, connbudget.ErrTotalExhausted) {
		t.Fatalf("error = %v, want ErrTotalExhausted", err)
	}
	if snap := budget.Snapshot(); snap.Auxiliary != 0 {
		t.Fatalf("auxiliary usage = %d after a refusal, want 0", snap.Auxiliary)
	}
}

// TestAcceptedSocketsAreAccountedBeforeRegistration is the P1 review found on
// the inbound side.
//
// The handler blocks on peerMu, and while it waits the accept loop keeps
// accepting. If the ceiling were taken inside the handler, every socket in
// that queue would be open, holding a descriptor, and invisible to
// Budget.Used — the node would hold more connections than its ceiling while
// reporting that it does not. Per-IP caps do not close it: they are per-IP,
// and the queue grows with the number of sources.
//
// The test blocks registration by holding peerMu, floods the listener, and
// requires that every accepted socket is either accounted or already closed.
func TestAcceptedSocketsAreAccountedBeforeRegistration(t *testing.T) {
	const ceiling = 4

	budget := mustBudget(t, connbudget.Config{Total: ceiling})
	svc := budgetTestService(t, budget)
	svc.conns = make(map[domain.ConnID]*connEntry)
	svc.connIDByNetConn = make(map[net.Conn]domain.ConnID)
	svc.disableRateLimiting = true

	// Stand in for the accept loop's admission step, which is the thing under
	// test: reserve first, hand the unit to the handler, release on refusal.
	admit := func(conn net.Conn) bool {
		reservation, err := budget.Reserve(connbudget.DirectionInbound)
		if err != nil {
			_ = conn.Close()
			return false
		}
		go func() {
			// The handler's first act is to register, which needs peerMu —
			// held by the test below.
			if !svc.registerInboundConn(conn, reservation) {
				reservation.Release()
				_ = conn.Close()
			}
		}()
		return true
	}

	// Registration is blocked for the whole flood.
	svc.peerMu.Lock()

	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	var admitted int
	for i := 0; i < ceiling*5; i++ {
		local, remote := net.Pipe()
		t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
		if admit(local) {
			admitted++
		}
		// Whatever happens, the accounted usage may never exceed the ceiling
		// — including while handlers are queued behind the lock.
		if used := budget.Snapshot().Used; used > ceiling {
			svc.peerMu.Unlock()
			t.Fatalf("accounted usage = %d exceeds the ceiling %d", used, ceiling)
		}
	}

	if admitted != ceiling {
		svc.peerMu.Unlock()
		t.Fatalf("admitted %d sockets with a ceiling of %d — the queue of unaccounted sockets grew",
			admitted, ceiling)
	}
	if snap := budget.Snapshot(); snap.Inbound != ceiling {
		svc.peerMu.Unlock()
		t.Fatalf("inbound usage = %d while handlers are blocked, want %d — "+
			"sockets accepted but not yet registered must already be accounted",
			snap.Inbound, ceiling)
	}

	// Let the handlers through; the admitted sockets become registered
	// entries, and the ceiling still holds exactly what it granted.
	svc.peerMu.Unlock()

	deadline := time.After(3 * time.Second)
	for {
		svc.peerMu.RLock()
		registered := len(svc.conns)
		svc.peerMu.RUnlock()
		if registered == ceiling {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("registered %d of %d admitted sockets", registered, ceiling)
		case <-time.After(10 * time.Millisecond):
		}
	}

	if snap := budget.Snapshot(); snap.Inbound != ceiling {
		t.Fatalf("inbound usage after registration = %d, want %d — ownership transfer double-counted or lost",
			snap.Inbound, ceiling)
	}
}
