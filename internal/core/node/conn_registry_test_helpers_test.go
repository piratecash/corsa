package node

import (
	"net"

	"github.com/piratecash/corsa/internal/core/netcore"
)

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
