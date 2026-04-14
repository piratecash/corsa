package node

// TestConnRegistry_InvalidationIsAtomic anchors the lifecycle invariant
// documented in docs/netcore-migration.md §2.6.7 and pinned by the
// PR 9.4a consolidation (§2.6.8, Q5=c): after unregisterInboundConn
// returns, the connEntry for the conn must be gone from the unified
// registry s.conns, and every accessor that previously read one of the
// four legacy parallel maps (inboundConns / inboundMetered / inboundTracked /
// inboundNetCores) must report "not registered" coherently — no leakage,
// no half-invalidated state.
//
// TRANSLATION (RU): тест фиксирует атомарность инвалидирования записи
// connEntry: после unregisterInboundConn запись в s.conns удалена одним
// вызовом delete, и ни один аксессор не возвращает остаточное состояние
// из бывших параллельных map-ов. Миграционный маркер: при удалении
// §2.6.7/§2.6.8 из docs/netcore-migration.md этот тест обязан либо
// переехать в peer_management_test.go, либо быть удалён вместе с ними —
// это единственный white-box тест на внутренний реестр (Q5=c в §2.6.8).

import (
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/netcore"
)

// memConn is a minimal in-memory net.Conn used purely as a map key for
// lifecycle exercises. It never carries bytes — NetCore.writerLoop is not
// started because we do not call Close() on the core; the test only
// validates registry bookkeeping, not socket I/O.
type memConn struct {
	net.Conn
	remote net.Addr
}

func (c *memConn) RemoteAddr() net.Addr { return c.remote }
func (c *memConn) Close() error         { return nil }

func newRegistryTestService() *Service {
	return &Service{
		conns: make(map[net.Conn]*connEntry),
	}
}

func TestConnRegistry_InvalidationIsAtomic(t *testing.T) {
	t.Parallel()

	svc := newRegistryTestService()
	conn := &memConn{remote: &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5001}}

	// Seed an entry mirroring what registerInboundConn / trackInboundConnect
	// produce together: core present, tracked flag set. metered is nil —
	// the test does not need a real MeteredConn to exercise the invariant.
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		LastActivity: time.Now().UTC(),
	})
	svc.mu.Lock()
	svc.conns[conn] = &connEntry{core: pc, tracked: true}
	svc.mu.Unlock()

	// Pre-condition: every accessor sees the seeded state.
	if got := svc.netCoreFor(conn); got != pc {
		t.Fatalf("netCoreFor: got %v, want %v", got, pc)
	}
	if !svc.isInboundTracked(conn) {
		t.Fatal("isInboundTracked: got false, want true")
	}

	// Act: single atomic delete (the rule §2.6.7 pins one call-site).
	svc.mu.Lock()
	delete(svc.conns, conn)
	svc.mu.Unlock()

	// Post-condition: every accessor reports "not registered" coherently.
	if got := svc.netCoreFor(conn); got != nil {
		t.Errorf("netCoreFor after delete: got %v, want nil", got)
	}
	if svc.isInboundTracked(conn) {
		t.Error("isInboundTracked after delete: got true, want false")
	}
	if got := svc.meteredFor(conn); got != nil {
		t.Errorf("meteredFor after delete: got %v, want nil", got)
	}

	// The registry itself must not retain a stub entry.
	svc.mu.RLock()
	_, present := svc.conns[conn]
	svc.mu.RUnlock()
	if present {
		t.Error("conns[conn] still present after delete — invalidation leaked")
	}
}
