package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// mustConnIDForTest resolves the ConnID for a net.Conn fixture registered
// through setTestConnEntryLocked. Tests call handler methods with a
// ConnID-first signature; this helper keeps the test call sites short by
// performing the lookup inline. Returns 0 when the conn is not registered,
// which preserves the production "unknown conn" semantics handlers already
// guard against.
func mustConnIDForTest(s *Service, conn net.Conn) domain.ConnID {
	id, _ := s.connIDFor(conn)
	return id
}

// closeNetCoreOnCleanup closes a test-built NetCore when the test ends.
//
// Closing the conn underneath is not enough: an idle writerLoop returns only
// on Close, so a helper that skipped it leaked the writer goroutine and its
// buffers on every call, which a stress test calling such a helper a
// thousand times per run multiplies by its -count. The order against the caller's own pipe
// cleanups does not matter: Close closes the conn itself before it waits for
// the writer, so it never blocks on a far end that is still open. Close is
// idempotent, so a test that also closes the core or its session is fine.
func closeNetCoreOnCleanup(t *testing.T, core *netcore.NetCore) {
	t.Helper()
	t.Cleanup(core.Close)
}

// setTestConnEntryLocked is the test-only equivalent of the three lifecycle
// helpers in conn_registry.go (registerInboundConnLocked,
// attachOutboundCoreLocked, unregisterConnLocked). Tests that predate
// PR 9.7 assigned directly to svc.conns[conn]; after the rekey from
// net.Conn to ConnID they must go through this helper so the primary
// map (s.conns) and the secondary index (s.connIDByNetConn) stay
// coherent.
//
// If entry.core is set and carries a non-zero ConnID (i.e. was built via
// netcore.New(...)), that ConnID is used as primary key. Otherwise —
// zero-value NetCore fixtures or nil-core entries — a synthetic ConnID
// is minted from s.connIDCounter so the two fixtures sites in the same
// test do not collide at ConnID(0).
func (s *Service) setTestConnEntryLocked(conn net.Conn, entry *connEntry) {
	var id netcore.ConnID
	if entry != nil && entry.core != nil && entry.core.ConnIDNum() != 0 {
		id = entry.core.ConnID()
	} else {
		s.connIDCounter++
		id = netcore.ConnID(s.connIDCounter)
	}
	s.conns[id] = entry
	s.connIDByNetConn[conn] = id
}

// registerAuthenticatedInboundPeerLocked registers conn as an inbound peer
// that completed authentication — tracked, as completeInboundAuth leaves it —
// which is the only kind of inbound connection that reserves its host against
// outbound dials. The caller must hold s.peerMu.
func (s *Service) registerAuthenticatedInboundPeerLocked(conn net.Conn, core *netcore.NetCore) {
	s.setTestConnEntryLocked(conn, &connEntry{core: core, tracked: true})
}

// testConnEntry is the read counterpart of setTestConnEntryLocked: it
// resolves a net.Conn through the secondary index and returns the
// matching connEntry, or nil if the conn is not registered. Tests that
// previously read svc.conns[conn] directly use this helper post-PR 9.7.
func (s *Service) testConnEntry(conn net.Conn) *connEntry {
	id, ok := s.connIDByNetConn[conn]
	if !ok {
		return nil
	}
	return s.conns[id]
}

// deleteTestConn removes the conn from both the primary map and the
// secondary index. Equivalent to unregisterConnLocked but intended only
// for fixture cleanup in tests; production code must call the lifecycle
// helper in conn_registry.go.
func (s *Service) deleteTestConn(conn net.Conn) {
	id, ok := s.connIDByNetConn[conn]
	if !ok {
		return
	}
	delete(s.conns, id)
	delete(s.connIDByNetConn, conn)
}
