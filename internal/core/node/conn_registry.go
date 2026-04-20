package node

import (
	"net"
	"net/netip"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// connInfo is a read-only snapshot of registry state for a single connection,
// captured under s.mu and handed to walker callbacks. It is the abstraction
// boundary that keeps callers off the *netcore.NetCore handle and off the
// connEntry shape: walkers no longer leak the live core pointer, so callbacks
// cannot inadvertently mutate identity/address/auth or race with the
// handshake-time writes that coreForIDLocked still permits.
//
// All fields are value-typed; the capabilities slice is a defensive copy so
// callers cannot scribble back into the live *netcore.NetCore. Walkers must
// populate every field — partial snapshots are a contract violation.
//
// remoteIP and peerDir are resolved at snapshot time so consumers that need
// typed network metadata (capture resolver, traffic bridge) never dereference
// net.Conn or entry.core themselves. Keeping the resolution inside the
// registry is the only way to enforce the §2.9 net-import whitelist: the
// raw socket lives here, and only here.
type connInfo struct {
	id           domain.ConnID
	remoteAddr   string
	remoteIP     netip.Addr
	address      domain.PeerAddress
	identity     domain.PeerIdentity
	capabilities []domain.Capability
	dir          netcore.Direction
	peerDir      domain.PeerDirection
	lastActivity time.Time
	tracked      bool
}

// HasCapability reports whether the snapshot lists cap. The lookup is linear
// because peer capability sets are tiny (typically <=4 entries) and the
// snapshot is short-lived; introducing a map here would cost more than it
// saves.
func (c connInfo) HasCapability(cap domain.Capability) bool {
	for _, have := range c.capabilities {
		if have == cap {
			return true
		}
	}
	return false
}

// snapshotEntryLocked builds a connInfo from a registry entry. The capability
// slice is copied so the caller never aliases NetCore-owned storage. Returns
// the zero value and false if the entry or its core is missing — the caller
// must treat that as "skip this connection". The caller must hold s.mu.
func snapshotEntryLocked(id domain.ConnID, entry *connEntry) (connInfo, bool) {
	if entry == nil || entry.core == nil {
		return connInfo{}, false
	}
	core := entry.core
	caps := core.Capabilities()
	var capsCopy []domain.Capability
	if len(caps) > 0 {
		capsCopy = make([]domain.Capability, len(caps))
		copy(capsCopy, caps)
	}
	return connInfo{
		id:           id,
		remoteAddr:   core.RemoteAddr(),
		remoteIP:     coreRemoteIP(core),
		address:      core.Address(),
		identity:     core.Identity(),
		capabilities: capsCopy,
		dir:          core.Dir(),
		peerDir:      coreToPeerDirection(core.Dir()),
		lastActivity: core.LastActivity(),
		tracked:      entry.tracked,
	}, true
}

// resolveRemoteIPFromAddr normalises a net.Addr (TCP or otherwise) into a
// netip.Addr. netcore-plan §4.4 mandates netip.Addr for the runtime model,
// not string. IPv4-mapped addresses are unmapped so equality comparisons
// against caller-supplied netip.Addr values behave as expected.
//
// The helper accepts net.Addr rather than net.Conn deliberately: net.Addr
// carries the same information we need without adding another entry to the
// §2.9 net.Conn-accepting carve-out. Callers that start from a net.Conn
// pass conn.RemoteAddr() inline; the single nil-guard inside the helper
// handles the case where a freshly-accepted connection has not published
// its remote yet.
func resolveRemoteIPFromAddr(addr net.Addr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	if tcpAddr, ok := addr.(*net.TCPAddr); ok && tcpAddr.IP != nil {
		if mapped, ok := netip.AddrFromSlice(tcpAddr.IP); ok {
			return mapped.Unmap()
		}
	}
	addrPort, err := netip.ParseAddrPort(addr.String())
	if err == nil {
		return addrPort.Addr().Unmap()
	}
	return netip.Addr{}
}

// coreRemoteIP returns the remote peer IP for a connection core, or the
// zero netip.Addr when the core (or its underlying raw socket) is missing.
// This is the ConnID-path companion to resolveRemoteIPFromAddr: snapshot
// builders hold a *netcore.NetCore from entry.core, so centralising the
// nil-guarded .Conn().RemoteAddr() chain here keeps the §2.9 boundary
// invariant that only conn_registry.go dereferences the raw net.Conn.
func coreRemoteIP(core *netcore.NetCore) netip.Addr {
	if core == nil {
		return netip.Addr{}
	}
	conn := core.Conn()
	if conn == nil {
		return netip.Addr{}
	}
	return resolveRemoteIPFromAddr(conn.RemoteAddr())
}

// coreToPeerDirection maps netcore.Direction to domain.PeerDirection.
// Centralised in conn_registry.go so external consumers never need to
// import netcore just to translate a direction value read through the
// snapshot boundary.
func coreToPeerDirection(d netcore.Direction) domain.PeerDirection {
	if d == netcore.Outbound {
		return domain.PeerDirectionOutbound
	}
	return domain.PeerDirectionInbound
}

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
// PR 9.10b-2 moved iteration helpers (forEachInboundConnLocked,
// forEachTrackedInboundConnLocked) onto *netcore.NetCore as the single
// identity currency, and made the tracked-flag mutation helper ConnID-first
// (setTrackedByIDLocked). PR 10.16 completed the closure: those walkers and
// forEachConnLocked now hand callers a value-typed connInfo snapshot, so
// the live *netcore.NetCore pointer no longer escapes the registry. Reads
// of identity / address / capabilities / tracked / direction / activity at
// callback sites go through the snapshot fields; mutating writes
// (SetIdentity, SetAddress, SetAuth) keep their handshake-time path through
// coreForIDLocked, which remains the single carve-out for the live handle.
// Two ConnID-first lookup helpers — trackedInboundAddressByIDLocked and
// connIdentityByIDLocked — replace the last entry.core.Address() /
// entry.core.Identity() reads in service.go.
// Lifecycle helpers (registerInboundConnLocked, attachOutboundCoreLocked,
// unregisterConnLocked) are the intentional carve-out: they are the
// entry/exit boundary that creates and tears down the (net.Conn, ConnID)
// binding, so they must accept a raw net.Conn by definition — there is
// no ConnID to key them on before registerInboundConnLocked runs, and
// the secondary index s.connIDByNetConn cannot be trimmed on shutdown
// without the same net.Conn that was registered.
//
// This seam prevents unbounded churn if the internal shape of s.conns
// changes in the future: only the helpers here need to be updated, not
// dozens of call sites throughout the codebase.
//
// net.Conn-first functions in internal/core/node.
//
// The package distinguishes two disjoint sets of functions that accept a
// raw net.Conn: a permanent carve-out whose signatures are frozen by
// structural necessity, and a transitional set of bridges that are still
// net.Conn-first pending later migration. This block-comment is the
// normative source of truth for which functions belong to each set.
// Introducing a net.Conn-accepting function that is not covered here is a
// boundary violation and is rejected at review.
//
// Permanent carve-out (frozen).
//
// These functions accept net.Conn because the signature is dictated by
// structural role, not by an incomplete migration. The criterion is one
// of: entry boundary for a raw socket that has no ConnID yet, lifecycle
// binding that creates or tears down the (net.Conn, ConnID) pairing,
// pre-registration network-level policy that runs before a ConnID exists,
// or the signature of an external interface that pins net.Conn. The
// canonical list is:
//
//   - in this file (conn_registry.go): connIDForLocked, connIDFor,
//     registerInboundConnLocked, attachOutboundCoreLocked,
//     unregisterConnLocked.
//   - in service.go: handleConn (inbound entry boundary), public lifecycle
//     wrappers registerInboundConn / unregisterInboundConn,
//     pre-registration IP policy isBlacklistedConn, and the
//     connauth.AuthStore implementation ConnAuthState / SetConnAuthState
//     (structural carve-out from external interface).
//
// These are not "we'll migrate them later" placeholders — their net.Conn
// signature will not be removed.
//
// Transitional net.Conn-first surface (not part of the frozen carve-out).
//
// PR 10.6 completed the migration of the write-layer bridges and their
// helpers (enqueueFrame → enqueueFrameByID, enqueueFrameSync →
// enqueueFrameSyncByID, writeJSONFrame → writeJSONFrameByID,
// writeJSONFrameSync → writeJSONFrameSyncByID, emitProtocolTrace and
// logUnregisteredWrite now take a string addr, sendAckDeleteOnConn →
// sendAckDeleteByID, isConnTrafficTrustedLocked →
// isConnTrafficTrustedByIDLocked). After that PR only one non-carve-out
// net.Conn-first helper remains:
//
//   - socket-level infrastructure: enableTCPKeepAlive in peer_management.go
//     (operates on the raw socket by definition; may stay net.Conn-first
//     indefinitely, but is classified here rather than in the frozen
//     carve-out because it is not a lifecycle/entry/interface boundary).
//
// New net.Conn-first call sites outside the frozen carve-out require
// explicit justification at review.

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

// connEntryByIDLocked returns the entire connEntry keyed by ConnID, or nil
// if the connection is not registered. Used by ConnID-first call sites that
// need access to entry-level fields beyond what coreForIDLocked /
// isInboundTrackedByIDLocked expose (e.g. trackedInboundPeerAddress needs
// both the tracked flag and core.Address() in a single critical section).
// The caller must hold s.mu.
func (s *Service) connEntryByIDLocked(id domain.ConnID) *connEntry {
	return s.conns[id]
}

// connInfoByIDLocked returns a value-typed connInfo snapshot for id, or
// the zero value and false when the id is unknown or its core is missing.
// This is the narrow ConnID-first read path used by consumers that must
// not see *netcore.NetCore or net.Conn (e.g. the traffic capture bridge
// resolving remote IP / peer direction for a single connection).
// The caller must hold s.mu.
func (s *Service) connInfoByIDLocked(id domain.ConnID) (connInfo, bool) {
	return snapshotEntryLocked(id, s.conns[id])
}

// overlayIdentityByIDLocked returns the overlay address, identity, and
// direction recorded on the live connection at id, or zero values when the
// id is unknown (or the core has been torn down). Callers who need an
// atomic triple read of these three fields use this helper to avoid
// dereferencing entry.core themselves. The caller must hold s.mu.
func (s *Service) overlayIdentityByIDLocked(id domain.ConnID) (domain.PeerAddress, domain.PeerIdentity, domain.PeerDirection) {
	entry := s.conns[id]
	if entry == nil || entry.core == nil {
		return "", "", ""
	}
	return entry.core.Address(), entry.core.Identity(), coreToPeerDirection(entry.core.Dir())
}

// overlayIdentityByID is the lock-free adapter over overlayIdentityByIDLocked.
// Public-enough for in-package callers that need the triple atomically without
// holding s.mu themselves; behaves identically to the locked variant otherwise.
func (s *Service) overlayIdentityByID(id domain.ConnID) (domain.PeerAddress, domain.PeerIdentity, domain.PeerDirection) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.overlayIdentityByIDLocked(id)
}

// attachCaptureSinkByID attaches a netcore.CaptureSink to the NetCore at id
// and returns a snapshot of the fields the caller needs to build a
// capture.ConnInfo (RemoteIP, PeerDir). Returns (zero, zero, false) when id
// is unknown. The method takes s.mu.RLock internally — SetCaptureSink has
// its own sync on *netcore.NetCore, so the registry lock only protects the
// s.conns lookup, not the sink write. This is the single ConnID-first entry
// point for capture-sink lifecycle; the traffic bridge never reaches for
// entry.core.SetCaptureSink directly.
func (s *Service) attachCaptureSinkByID(id domain.ConnID, sink netcore.CaptureSink) (netip.Addr, domain.PeerDirection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.conns[id]
	if entry == nil || entry.core == nil {
		return netip.Addr{}, "", false
	}
	entry.core.SetCaptureSink(sink)
	return coreRemoteIP(entry.core), coreToPeerDirection(entry.core.Dir()), true
}

// forEachConnLocked iterates over every registered connection regardless
// of direction, calling fn with a connInfo snapshot per entry. Iteration
// stops if fn returns false. The caller must hold s.mu. Walkers no longer
// expose *netcore.NetCore — callbacks receive a value-typed snapshot so
// they cannot mutate identity/address/auth concurrently with the
// handshake-time writes that coreForIDLocked still permits. Direction-
// specific walks over inbound connections continue to use
// forEachInboundConnLocked and forEachTrackedInboundConnLocked which are
// tuned for their call sites.
func (s *Service) forEachConnLocked(fn func(connInfo) bool) {
	for id, entry := range s.conns {
		info, ok := snapshotEntryLocked(id, entry)
		if !ok {
			continue
		}
		if !fn(info) {
			return
		}
	}
}

// forEachInboundConnLocked iterates over all registered inbound connections
// (Direction == Inbound), calling fn with a connInfo snapshot per entry.
// Iteration stops if fn returns false. The caller must hold s.mu.
func (s *Service) forEachInboundConnLocked(fn func(connInfo) bool) {
	for id, entry := range s.conns {
		info, ok := snapshotEntryLocked(id, entry)
		if !ok {
			continue
		}
		if info.dir != netcore.Inbound {
			continue
		}
		if !fn(info) {
			break
		}
	}
}

// forEachTrackedInboundConnLocked iterates over all registered inbound
// connections that are marked as tracked (i.e., have completed
// authentication), calling fn with a connInfo snapshot per entry. Iteration
// stops if fn returns false. The caller must hold s.mu.
func (s *Service) forEachTrackedInboundConnLocked(fn func(connInfo) bool) {
	for id, entry := range s.conns {
		info, ok := snapshotEntryLocked(id, entry)
		if !ok {
			continue
		}
		if info.dir != netcore.Inbound {
			continue
		}
		if !info.tracked {
			continue
		}
		if !fn(info) {
			break
		}
	}
}

// trackedInboundAddressByIDLocked returns the overlay address declared by
// the inbound connection at id, but only when the connection has been
// promoted via trackInboundConnect. Returns the zero PeerAddress when the
// id is unknown, points at an outbound entry, or has not been promoted.
// Callers use this to gate health-mutating side-effects on tracked status
// without materialising *netcore.NetCore. The caller must hold s.mu.
func (s *Service) trackedInboundAddressByIDLocked(id domain.ConnID) domain.PeerAddress {
	entry := s.conns[id]
	if entry == nil || entry.core == nil || !entry.tracked {
		return ""
	}
	if entry.core.Dir() != netcore.Inbound {
		return ""
	}
	return entry.core.Address()
}

// connIdentityByIDLocked returns the peer identity recorded on the
// connection at id, or the zero PeerIdentity when the id is unknown.
// Callers use this on disconnect / reconciliation paths that previously
// reached for entry.core.Identity() directly. The caller must hold s.mu.
func (s *Service) connIdentityByIDLocked(id domain.ConnID) domain.PeerIdentity {
	entry := s.conns[id]
	if entry == nil || entry.core == nil {
		return ""
	}
	return entry.core.Identity()
}

// inboundConnCountLocked returns the number of registered inbound connections.
// The caller must hold s.mu. Implementation reads entry.core.Dir() directly
// because this is one of the conn_registry-internal sites that owns the
// entry.core access shape; external call sites must go through the walker
// snapshots above.
func (s *Service) inboundConnCountLocked() int {
	count := 0
	for _, entry := range s.conns {
		if entry != nil && entry.core != nil && entry.core.Dir() == netcore.Inbound {
			count++
		}
	}
	return count
}

// connCountLocked returns the total number of registered connections
// (inbound + outbound) regardless of tracked state. Callers use this as a
// capacity hint before iterating via forEachConnLocked; the abstraction
// keeps direct len(s.conns) reads out of external files. The caller must
// hold s.mu.
func (s *Service) connCountLocked() int {
	return len(s.conns)
}

// setTrackedByIDLocked marks the tracked flag on a connection entry keyed by
// ConnID. If the connection is not registered, the call is a no-op.
// ConnID is the single identity currency inside the registry after PR 9.7 —
// callers that start from a net.Conn must cross the boundary via
// connIDForLocked once, then operate on ConnID.
// The caller must hold s.mu.
func (s *Service) setTrackedByIDLocked(id domain.ConnID, tracked bool) {
	if entry := s.conns[id]; entry != nil {
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
