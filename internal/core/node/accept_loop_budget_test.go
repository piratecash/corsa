package node

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
)

// accept_loop_budget_test.go drives the REAL accept loop.
//
// ⚠️ It exists because the earlier inbound tests did not. One reimplemented the
// admission step in the test itself, the other checked by AST where Reserve is
// called — so deleting the `budgetErr != nil` branch from the production loop
// left both of them green. A test that rebuilds the behaviour it is checking
// verifies the author's understanding, not the program.
//
// This one starts Run, blocks registration by holding peerMu, floods the
// listener from outside, and asks the only questions that cannot be answered
// by reimplementation: does the node CLOSE what it refuses, does it hold no
// more than the ceiling while its handlers are stuck, and does the capacity
// come back.

// runningBudgetNode starts a Service with a real listener and the shared
// ceiling set, and returns the address it actually bound.
func runningBudgetNode(t *testing.T, total int) (*Service, string, context.CancelFunc) {
	t.Helper()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	tempDir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:       "127.0.0.1:0",
		TrustStorePath:      filepath.Join(tempDir, "trust.json"),
		PeersStatePath:      filepath.Join(tempDir, "peers.json"),
		ListenerEnabled:     true,
		ListenerSet:         true,
		AllowPrivatePeers:   true,
		MaxTotalConnections: total,
	}, id, nil)
	// Per-IP caps would refuse the flood for their own reasons; this test is
	// about the shared ceiling, and every connection comes from loopback.
	svc.disableRateLimiting = true

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		svc.WaitBackground()
	})

	deadline := time.After(5 * time.Second)
	for {
		svc.peerMu.RLock()
		listener := svc.listener
		svc.peerMu.RUnlock()
		if listener != nil {
			return svc, listener.Addr().String(), cancel
		}
		select {
		case err := <-runErr:
			t.Fatalf("Run returned before the listener came up: %v", err)
		case <-deadline:
			t.Fatal("listener never came up")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// serverClosedUs reports whether the peer closed the connection, which is how a
// budget refusal looks from the outside: no bytes, immediate EOF. An accepted
// connection whose handler is blocked stays open and the read times out
// instead.
func serverClosedUs(t *testing.T, conn net.Conn) bool {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		return false
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return false
	}
	return err == io.EOF || err != nil
}

// TestAcceptLoopClosesWhatTheBudgetRefuses is the end-to-end version: a real
// listener, real sockets, registration blocked, and the ceiling doing the
// refusing.
func TestAcceptLoopClosesWhatTheBudgetRefuses(t *testing.T) {
	// The inbound ceiling is B − R_out, and R_out is the outbound peer limit:
	// the shared budget protects the persistent neighbourhood, so the total
	// has to leave room for it. Setting B below R_out is refused at startup,
	// which is a different (already covered) property.
	const inboundCeiling = 3
	total := config.DefaultOutgoingPeers + inboundCeiling

	svc, address, _ := runningBudgetNode(t, total)
	const ceiling = inboundCeiling

	// Block registration for the whole flood: every accepted handler parks on
	// peerMu, which is exactly the state in which unaccounted sockets used to
	// pile up.
	svc.peerMu.Lock()
	blocked := true
	defer func() {
		if blocked {
			svc.peerMu.Unlock()
		}
	}()

	var conns []net.Conn
	for i := 0; i < ceiling*3; i++ {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, conn)
		t.Cleanup(func() { _ = conn.Close() })
	}

	// Give the accept loop time to drain the backlog.
	time.Sleep(500 * time.Millisecond)

	snap := svc.connBudget.Snapshot()
	if !snap.Enabled {
		t.Fatal("the shared ceiling must be enabled for this test")
	}
	if snap.Used > ceiling {
		t.Fatalf("accounted usage = %d exceeds the ceiling %d while handlers are blocked", snap.Used, ceiling)
	}
	if snap.NonSlotCapacity != ceiling {
		t.Fatalf("non-slot capacity = %d, want %d", snap.NonSlotCapacity, ceiling)
	}
	if snap.Inbound != ceiling {
		t.Fatalf("inbound usage = %d, want %d — accepted sockets must be accounted before registration",
			snap.Inbound, ceiling)
	}

	// The decisive assertion: what the budget refused must be CLOSED, not
	// left open and unaccounted. Deleting the refusal branch from the accept
	// loop shows up here and nowhere else.
	var closed int
	for _, conn := range conns {
		if serverClosedUs(t, conn) {
			closed++
		}
	}
	if want := len(conns) - ceiling; closed != want {
		t.Fatalf("server closed %d sockets, want %d — refused connections were left open", closed, want)
	}
	// The refusal here is the outbound reserve — the inbound half is full
	// while the neighbourhood's capacity stays untouched — so the counter to
	// check is that one; either way a refusal must be visible in the
	// diagnostic rather than silent.
	if snap := svc.connBudget.Snapshot(); snap.RefusedReserved+snap.RefusedTotal+snap.RefusedDirection == 0 {
		t.Fatal("refusals were not counted; the diagnostic cannot say the node is at its ceiling")
	}

	// Let the handlers through, then close the admitted sockets: the capacity
	// must come back.
	svc.peerMu.Unlock()
	blocked = false

	for _, conn := range conns {
		_ = conn.Close()
	}

	deadline := time.After(5 * time.Second)
	for {
		if used := svc.connBudget.Snapshot().Used; used == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("capacity did not come back: %+v", svc.connBudget.Snapshot())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
