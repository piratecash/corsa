package node

import (
	"net"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// conn_registry.go contains the narrow helper layer that encapsulates all
// direct access to s.conns and its secondary index s.connIDByNetConn.
// Every helper carries the -Locked suffix and assumes the caller holds
// s.mu (either Lock or RLock, matching the existing convention for
// peerTypeForAddressLocked, ensurePeerHealthLocked, computePeerStateAtLocked).
// Helpers do not acquire locks themselves.
//
// PR 9.7 rekeyed s.conns from map[net.Conn]*connEntry to
// map[netcore.ConnID]*connEntry. Pure-lookup helpers
// (coreForIDLocked / meteredForIDLocked / isInboundTrackedByIDLocked) take
// ConnID directly — this moves the inner shape of the registry off net.Conn
// and keeps ConnID as the single identity currency inside the gate.
// Callers that start from a net.Conn go through connIDForLocked once to
// cross the boundary, then operate on ConnID.
//
// Mutation helpers (setTrackedLocked, registerInboundConnLocked,
// attachOutboundCoreLocked, unregisterConnLocked) and iteration helpers
// (forEachInboundConnLocked, forEachTrackedInboundConnLocked,
// inboundConnCountLocked) still take/return net.Conn and are the scope of
// PR 9.10b — they require coordinated changes with their call sites.
//
// This seam prevents unbounded churn if the internal shape of s.conns
// changes in the future: only the helpers here need to be updated, not
// dozens of call sites throughout the codebase.

// connIDForLocked resolves a net.Conn to its domain.ConnID via the
// secondary index. Returns zero value and false if the connection is not
// registered. This is the single point where net.Conn is translated into
// ConnID for read paths; all pure-lookup helpers below take ConnID directly.
// The caller must hold s.mu.
func (s *Service) connIDForLocked(conn net.Conn) (domain.ConnID, bool) {
	id, ok := s.connIDByNetConn[conn]
	if !ok {
		var zero domain.ConnID
		return zero, false
	}
	return id, true
}

// connIDFor resolves a net.Conn to its domain.ConnID without requiring the
// caller to hold s.mu. Returns zero value and false if the connection is
// not registered. This is the public adapter over connIDForLocked used by
// non-lock-holding call sites to cross the net.Conn → ConnID boundary
// exactly once; downstream lookups (netCoreForID, meteredForID,
// isInboundTrackedByID) take ConnID directly.
func (s *Service) connIDFor(conn net.Conn) (domain.ConnID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connIDForLocked(conn)
}

// connEntryLocked resolves a net.Conn to its *connEntry via the secondary
// index, or returns nil if the connection is not registered. Retained as
// the lookup shape used by setTrackedLocked and the connEntryForLocked
// escape hatch. The caller must hold s.mu.
func (s *Service) connEntryLocked(conn net.Conn) *connEntry {
	id, ok := s.connIDByNetConn[conn]
	if !ok {
		return nil
	}
	return s.conns[id]
}

// coreForIDLocked returns the NetCore for a given ConnID, or nil if the
// connection is not registered. The caller must hold s.mu.
func (s *Service) coreForIDLocked(id domain.ConnID) *netcore.NetCore {
	entry := s.conns[id]
	if entry == nil {
		return nil
	}
	return entry.core
}

// meteredForIDLocked returns the MeteredConn wrapper for a given ConnID,
// or nil if the connection is not registered or not metered.
// The caller must hold s.mu.
func (s *Service) meteredForIDLocked(id domain.ConnID) *netcore.MeteredConn {
	entry := s.conns[id]
	if entry == nil {
		return nil
	}
	return entry.metered
}

// isInboundTrackedByIDLocked returns whether the connection is marked as
// tracked (i.e., has completed authentication and health management).
// The caller must hold s.mu.
func (s *Service) isInboundTrackedByIDLocked(id domain.ConnID) bool {
	entry := s.conns[id]
	if entry == nil {
		return false
	}
	return entry.tracked
}

// connEntryForLocked returns the entire connEntry for a given connection,
// or nil if not registered. This is an escape hatch used only by lifecycle
// methods (registerInboundConn, attachOutboundNetCore, unregisterInboundConnLocked)
// that need access to the whole entry. Regular call sites must not use this.
// The caller must hold s.mu.
func (s *Service) connEntryForLocked(conn net.Conn) *connEntry {
	return s.connEntryLocked(conn)
}

// forEachInboundConnLocked iterates over all registered inbound connections
// (Direction == Inbound), calling fn for each (conn, core) pair. Iteration stops
// if fn returns false. The caller must hold s.mu.
func (s *Service) forEachInboundConnLocked(fn func(net.Conn, *netcore.NetCore) bool) {
	for _, entry := range s.conns {
		if entry == nil || entry.core == nil {
			continue
		}
		if entry.core.Dir() != netcore.Inbound {
			continue
		}
		if !fn(entry.core.Conn(), entry.core) {
			break
		}
	}
}

// forEachTrackedInboundConnLocked iterates over all registered inbound connections
// that are marked as tracked (i.e., have completed authentication), calling fn for
// each (conn, core) pair. Iteration stops if fn returns false. The caller must hold s.mu.
func (s *Service) forEachTrackedInboundConnLocked(fn func(net.Conn, *netcore.NetCore) bool) {
	for _, entry := range s.conns {
		if entry == nil || entry.core == nil {
			continue
		}
		if entry.core.Dir() != netcore.Inbound {
			continue
		}
		if !entry.tracked {
			continue
		}
		if !fn(entry.core.Conn(), entry.core) {
			break
		}
	}
}

// inboundConnCountLocked returns the number of registered inbound connections.
// The caller must hold s.mu.
func (s *Service) inboundConnCountLocked() int {
	count := 0
	for _, entry := range s.conns {
		if entry != nil && entry.core != nil && entry.core.Dir() == netcore.Inbound {
			count++
		}
	}
	return count
}

// setTrackedLocked marks the tracked flag on a connection entry.
// If the connection is not registered, the call is a no-op.
// The caller must hold s.mu.
func (s *Service) setTrackedLocked(conn net.Conn, tracked bool) {
	if entry := s.connEntryLocked(conn); entry != nil {
		entry.tracked = tracked
	}
}

// registerInboundConnLocked registers an inbound NetCore in the connection registry.
// It writes to both the primary (ConnID-keyed) map and the secondary (net.Conn-keyed)
// index in lock-step — the only place in the codebase where a (conn, id, entry)
// triple is created for an inbound connection. The caller must hold s.mu and must
// ensure the connection is not already registered.
func (s *Service) registerInboundConnLocked(conn net.Conn, core *netcore.NetCore, metered *netcore.MeteredConn) {
	entry := &connEntry{core: core}
	if metered != nil {
		entry.metered = metered
	}
	id := core.ConnID()
	s.conns[id] = entry
	s.connIDByNetConn[conn] = id
}

// attachOutboundCoreLocked registers an outbound NetCore in the connection registry.
// Mirrors registerInboundConnLocked on the outbound side and preserves the
// invariant that every entry in s.conns has a matching entry in s.connIDByNetConn.
// The caller must hold s.mu and must ensure the connection is not already registered.
func (s *Service) attachOutboundCoreLocked(conn net.Conn, core *netcore.NetCore) {
	id := core.ConnID()
	s.conns[id] = &connEntry{core: core}
	s.connIDByNetConn[conn] = id
}

// unregisterConnLocked removes a connection from the registry, clearing both
// the primary ConnID entry and the secondary net.Conn index in lock-step.
// If the connection is not present in the secondary index (e.g. a second
// teardown path racing with the first), the call is a no-op — both deletes
// are idempotent. The caller must hold s.mu.
func (s *Service) unregisterConnLocked(conn net.Conn) {
	id, ok := s.connIDByNetConn[conn]
	if !ok {
		return
	}
	delete(s.conns, id)
	delete(s.connIDByNetConn, conn)
}
