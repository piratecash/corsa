package node

import (
	"net"

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
// map[netcore.ConnID]*connEntry. Helpers keep their net.Conn-first
// signatures (OQ-9.7/1=A): callers typically already hold a net.Conn from
// the read-loop or writer path, and forcing every call site to resolve
// the ConnID itself would materially expand PR 9.7. The resolution is
// performed inside the gate via s.connIDByNetConn — a secondary index
// kept in strict sync with s.conns by the lifecycle helpers in this file.
//
// This seam prevents unbounded churn if the internal shape of s.conns
// changes in the future: only the helpers here need to be updated, not
// dozens of call sites throughout the codebase.

// connEntryLocked resolves a net.Conn to its *connEntry via the secondary
// index, or returns nil if the connection is not registered. Centralised
// here so every read-only helper shares the same two-step lookup shape.
// The caller must hold s.mu.
func (s *Service) connEntryLocked(conn net.Conn) *connEntry {
	id, ok := s.connIDByNetConn[conn]
	if !ok {
		return nil
	}
	return s.conns[id]
}

// coreForConnLocked returns the NetCore for a given connection, or nil if
// the connection is not registered. The caller must hold s.mu.
func (s *Service) coreForConnLocked(conn net.Conn) *netcore.NetCore {
	entry := s.connEntryLocked(conn)
	if entry == nil {
		return nil
	}
	return entry.core
}

// meteredForConnLocked returns the MeteredConn wrapper for a given connection,
// or nil if the connection is not registered or not metered. The caller must hold s.mu.
func (s *Service) meteredForConnLocked(conn net.Conn) *netcore.MeteredConn {
	entry := s.connEntryLocked(conn)
	if entry == nil {
		return nil
	}
	return entry.metered
}

// isInboundTrackedLocked returns whether the connection is marked as tracked
// (i.e., has completed authentication and health management). The caller must hold s.mu.
func (s *Service) isInboundTrackedLocked(conn net.Conn) bool {
	entry := s.connEntryLocked(conn)
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
