package node

import (
	"net"

	"github.com/piratecash/corsa/internal/core/netcore"
)

// conn_registry.go contains the narrow helper layer that encapsulates all
// direct access to s.conns. Every helper carries the -Locked suffix and assumes
// the caller holds s.mu (either Lock or RLock, matching the existing convention
// for peerTypeForAddressLocked, ensurePeerHealthLocked, computePeerStateAtLocked).
// Helpers do not acquire locks themselves.
//
// This seam prevents unbounded churn if the internal shape of s.conns changes
// in the future: only the helpers here need to be updated, not dozens of
// call sites throughout the codebase.

// coreForConnLocked returns the NetCore for a given connection, or nil if
// the connection is not registered. The caller must hold s.mu.
func (s *Service) coreForConnLocked(conn net.Conn) *netcore.NetCore {
	entry := s.conns[conn]
	if entry == nil {
		return nil
	}
	return entry.core
}

// meteredForConnLocked returns the MeteredConn wrapper for a given connection,
// or nil if the connection is not registered or not metered. The caller must hold s.mu.
func (s *Service) meteredForConnLocked(conn net.Conn) *netcore.MeteredConn {
	entry := s.conns[conn]
	if entry == nil {
		return nil
	}
	return entry.metered
}

// isInboundTrackedLocked returns whether the connection is marked as tracked
// (i.e., has completed authentication and health management). The caller must hold s.mu.
func (s *Service) isInboundTrackedLocked(conn net.Conn) bool {
	entry := s.conns[conn]
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
	return s.conns[conn]
}

// forEachInboundConnLocked iterates over all registered inbound connections
// (Direction == Inbound), calling fn for each (conn, core) pair. Iteration stops
// if fn returns false. The caller must hold s.mu.
func (s *Service) forEachInboundConnLocked(fn func(net.Conn, *netcore.NetCore) bool) {
	for conn, entry := range s.conns {
		if entry == nil || entry.core == nil {
			continue
		}
		if entry.core.Dir() != netcore.Inbound {
			continue
		}
		if !fn(conn, entry.core) {
			break
		}
	}
}

// forEachTrackedInboundConnLocked iterates over all registered inbound connections
// that are marked as tracked (i.e., have completed authentication), calling fn for
// each (conn, core) pair. Iteration stops if fn returns false. The caller must hold s.mu.
func (s *Service) forEachTrackedInboundConnLocked(fn func(net.Conn, *netcore.NetCore) bool) {
	for conn, entry := range s.conns {
		if entry == nil || entry.core == nil {
			continue
		}
		if entry.core.Dir() != netcore.Inbound {
			continue
		}
		if !entry.tracked {
			continue
		}
		if !fn(conn, entry.core) {
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
	if entry := s.conns[conn]; entry != nil {
		entry.tracked = tracked
	}
}

// registerInboundConnLocked registers an inbound NetCore in the connection registry.
// The caller must hold s.mu and must ensure the connection is not already registered.
func (s *Service) registerInboundConnLocked(conn net.Conn, core *netcore.NetCore, metered *netcore.MeteredConn) {
	entry := &connEntry{core: core}
	if metered != nil {
		entry.metered = metered
	}
	s.conns[conn] = entry
}

// attachOutboundCoreLocked registers an outbound NetCore in the connection registry.
// The caller must hold s.mu and must ensure the connection is not already registered.
func (s *Service) attachOutboundCoreLocked(conn net.Conn, core *netcore.NetCore) {
	s.conns[conn] = &connEntry{core: core}
}

// unregisterConnLocked removes a connection from the registry.
// The caller must hold s.mu.
func (s *Service) unregisterConnLocked(conn net.Conn) {
	delete(s.conns, conn)
}
