package node

// TestConnRegistry_InvalidationIsAtomic pins the unregister invariant on
// the unified connection registry: once the lifecycle helper in
// conn_registry.go has removed a conn, Service.conns and
// Service.connIDByNetConn must no longer resolve it through any accessor.
// netCoreFor, isInboundTracked, and meteredFor all share the same two-step
// lookup (connIDByNetConn → conns); if one of the two maps is left
// populated, these accessors will silently disagree — one returning nil
// while the other still reports a live entry. The test is white-box by
// necessity: it reaches into the registry directly via the test-only
// helpers in conn_registry_test_helpers_test.go so a half-invalidated
// state cannot hide behind a silent miss further down the stack.
//
// TRANSLATION (RU): тест фиксирует атомарность снятия записи из реестра.
// После удаления через lifecycle-хелпер в conn_registry.go ни primary
// map (conns), ни secondary index (connIDByNetConn) не должны резолвить
// conn — иначе net.Conn-first аксессоры начнут молча расходиться
// (один возвращает nil, второй — живую запись). White-box-доступ идёт
// через test-only хелперы в conn_registry_test_helpers_test.go.

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
		conns:           make(map[netcore.ConnID]*connEntry),
		connIDByNetConn: make(map[net.Conn]netcore.ConnID),
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
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.mu.Unlock()

	// Pre-condition: every accessor sees the seeded state.
	if got := svc.netCoreFor(conn); got != pc {
		t.Fatalf("netCoreFor: got %v, want %v", got, pc)
	}
	if !svc.isInboundTracked(conn) {
		t.Fatal("isInboundTracked: got false, want true")
	}

	// Act: single atomic delete — the registry contract is one call-site
	// (deleteTestConn mirrors unregisterConnLocked in conn_registry.go).
	svc.mu.Lock()
	svc.deleteTestConn(conn)
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

	// The registry itself must not retain a stub entry — neither the
	// primary map (ConnID-keyed after PR 9.7) nor the secondary index
	// (net.Conn → ConnID). Both are checked explicitly so a regression
	// that leaves one half populated cannot silently pass.
	svc.mu.RLock()
	present := svc.testConnEntry(conn) != nil
	_, secondary := svc.connIDByNetConn[conn]
	svc.mu.RUnlock()
	if present {
		t.Error("primary registry still resolves conn after delete — invalidation leaked")
	}
	if secondary {
		t.Error("secondary index still holds conn → ConnID entry after delete — PR 9.7 sync invariant violated")
	}
}

// TestConnRegistry_RegisterSyncsSecondaryIndex locks in the invariant
// introduced by PR 9.7: after registerInboundConnLocked returns, both
// the primary map (s.conns[ConnID]) and the secondary index
// (s.connIDByNetConn[conn]) hold matching entries — neither half can
// be populated without the other. The secondary index is the only path
// net.Conn-first helpers use to reach the primary key, so divergence
// would turn every subsequent coreForConnLocked / isInboundTrackedLocked
// call on this conn into a silent miss even though the entry exists.
func TestConnRegistry_RegisterSyncsSecondaryIndex(t *testing.T) {
	t.Parallel()

	svc := newRegistryTestService()
	conn := &memConn{remote: &net.TCPAddr{IP: net.ParseIP("10.0.0.2"), Port: 5002}}

	pc := netcore.New(netcore.ConnID(42), conn, netcore.Inbound, netcore.Options{
		LastActivity: time.Now().UTC(),
	})

	svc.mu.Lock()
	svc.registerInboundConnLocked(conn, pc, nil)
	svc.mu.Unlock()

	svc.mu.RLock()
	id, hasSecondary := svc.connIDByNetConn[conn]
	entry := svc.conns[id]
	svc.mu.RUnlock()

	if !hasSecondary {
		t.Fatal("secondary index missing after register — net.Conn-first helpers cannot resolve this conn")
	}
	if id != pc.ConnID() {
		t.Fatalf("secondary index points at wrong ConnID: got %d, want %d", id, pc.ConnID())
	}
	if entry == nil || entry.core != pc {
		t.Fatal("primary map entry missing or points at wrong NetCore")
	}

	// Cross-check via the gateway: the net.Conn-first helper must resolve
	// the same entry the test fetched by hand.
	if got := svc.netCoreFor(conn); got != pc {
		t.Fatalf("netCoreFor after register: got %v, want %v", got, pc)
	}
}
