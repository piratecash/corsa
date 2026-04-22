package node

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// peerHealthRecord is the per-peer data captured from s.mu-protected state in
// a single short RLock section.  After s.mu is released the fields are frozen
// — the RPC handler (fetch_peer_health) reads records from the cached
// snapshot concurrently and must never mutate them.
//
// BytesSent / BytesReceived on the embedded health are adjusted to include
// the live MeteredConn delta at refresh time, so the RPC handler renders the
// combined traffic without a second lookup into liveTraffic.
type peerHealthRecord struct {
	health               peerHealth
	peerID               domain.PeerIdentity
	clientVersion        string
	clientBuild          int
	pendingCount         int
	capabilities         []string
	sessionVersion       int
	sessionConnID        domain.ConnID
	inboundConnIDs       []uint64
	versionLockoutActive bool
}

// inboundLiveRecord captures a peer with live traffic in a MeteredConn but no
// entry in s.health yet — typical for an inbound session still in the
// pre-handshake window.  Rendered as its own row by peerHealthFrames so the
// UI can distinguish it from a fully handshaked peer.
type inboundLiveRecord struct {
	address        domain.PeerAddress
	peerID         domain.PeerIdentity
	clientVersion  string
	clientBuild    int
	capabilities   []string
	sessionVersion int
	sent           int64
	received       int64
	inboundConnIDs []uint64
}

// peerHealthSnapshot is the cached, lock-free view consumed by peerHealthFrames
// (fetch_peer_health RPC).  Primed synchronously by primeHotReadSnapshots()
// in Run() before the listener opens, then rebuilt by hotReadsRefreshLoop
// every networkStatsSnapshotInterval under a short s.mu.RLock.  The RPC
// handler performs a single atomic load and never acquires s.mu — there
// is no synchronous fallback rebuild on the RPC goroutine, so the hot
// path is statically decoupled from s.mu.
//
// If a writer holds s.mu for many seconds, the refresher may stall on its
// RLock but the RPC keeps returning the previous snapshot — the hot read path
// is completely decoupled from writer behaviour.
//
// Peer state transitions (markPeerConnected / markPeerDisconnected) also
// trigger an out-of-band rebuild after releasing s.mu.Lock so UI pollers
// observe the new state without waiting for the next 500 ms tick — see
// refreshHotReadSnapshotsAfterPeerStateChange in hot_reads_refresh.go.
// The eager rebuild is skipped when s.runCtx is done so the shutdown
// disconnect storm does not multiply s.mu.RLock contention onto the
// session-teardown goroutines.  Other state mutations (traffic bytes,
// score adjustments, etc.) rely on the bounded-staleness model: the next
// refresher tick captures them.
type peerHealthSnapshot struct {
	records     []peerHealthRecord
	inboundOnly []inboundLiveRecord
	// now is the wall-clock timestamp used for peerState computation inside
	// the RLock window.  Captured once so every record in this snapshot is
	// classified against the same clock.  Not part of the wire frame.
	now         time.Time
	generatedAt time.Time
}

// loadPeerHealthSnapshot atomically retrieves the last-published peer-health
// snapshot.  Guaranteed non-nil on the RPC path once Run() has executed
// primeHotReadSnapshots() before opening the listener.  Returns nil only
// for unit tests that bypass Run() without priming manually.
func (s *Service) loadPeerHealthSnapshot() *peerHealthSnapshot {
	return s.peerHealthSnap.Load()
}

// rebuildPeerHealthSnapshot constructs a fresh snapshot under a short
// s.mu.RLock, then stores it atomically.  All derived lookups
// (peerCapabilitiesLocked, inboundConnIDsLocked, resolveHealthAddress, etc.)
// run inside the RLock window — they require read access to s.mu-protected
// state — but no formatting, sorting or wire conversion happens here.  The
// RPC handler performs those steps lock-free against the cached records.
//
// The per-peer live traffic delta is folded into record.health.BytesSent /
// BytesReceived so the handler can emit the frame without revisiting
// liveTraffic.
func (s *Service) rebuildPeerHealthSnapshot() {
	log.Trace().Msg("peer_health_snapshot_refresh_begin")

	s.mu.RLock()
	now := time.Now().UTC()

	live := s.liveTrafficLocked()

	records := make([]peerHealthRecord, 0, len(s.health))
	for _, health := range s.health {
		rec := peerHealthRecord{
			health:               *health,
			peerID:               s.peerIDs[health.Address],
			clientVersion:        s.peerVersions[health.Address],
			clientBuild:          s.peerBuilds[health.Address],
			pendingCount:         len(s.pending[health.Address]),
			capabilities:         s.peerCapabilitiesLocked(health.Address),
			versionLockoutActive: s.isPeerVersionLockedOutLocked(health.Address),
			inboundConnIDs:       s.inboundConnIDsLocked(health.Address),
		}
		// Fold live traffic delta into the embedded health's byte counters
		// so the RPC handler does not need a second lookup.  The underlying
		// *peerHealth in s.health is untouched — we mutate the value copy.
		if lv, ok := live[health.Address]; ok {
			rec.health.BytesSent += lv.sent
			rec.health.BytesReceived += lv.received
		}
		rec.health.State = s.computePeerStateAtLocked(health, now)
		for dialAddr, session := range s.sessions {
			if s.resolveHealthAddress(dialAddr) == health.Address {
				rec.sessionVersion = session.version
				rec.sessionConnID = session.connID
				break
			}
		}
		records = append(records, rec)
	}

	healthAddrs := make(map[domain.PeerAddress]struct{}, len(s.health))
	for _, h := range s.health {
		healthAddrs[h.Address] = struct{}{}
	}
	var inboundOnly []inboundLiveRecord
	for addr, lv := range live {
		if _, ok := healthAddrs[addr]; ok {
			continue
		}
		ilr := inboundLiveRecord{
			address:        addr,
			peerID:         s.peerIDs[addr],
			clientVersion:  s.peerVersions[addr],
			clientBuild:    s.peerBuilds[addr],
			capabilities:   s.peerCapabilitiesLocked(addr),
			sent:           lv.sent,
			received:       lv.received,
			inboundConnIDs: s.inboundConnIDsLocked(addr),
		}
		if session, ok := s.sessions[addr]; ok {
			ilr.sessionVersion = session.version
		}
		inboundOnly = append(inboundOnly, ilr)
	}

	s.mu.RUnlock()
	log.Trace().Int("records", len(records)).Int("inbound_only", len(inboundOnly)).Msg("peer_health_snapshot_rlock_released")

	snap := &peerHealthSnapshot{
		records:     records,
		inboundOnly: inboundOnly,
		now:         now,
		generatedAt: time.Now().UTC(),
	}
	s.peerHealthSnap.Store(snap)
	log.Trace().Int("records", len(records)).Msg("peer_health_snapshot_refresh_end")
}

// peerHealthSnapPtr wraps atomic.Pointer[peerHealthSnapshot] so the Service
// struct can declare the field without exposing the generic type syntax
// everywhere.  Zero value is usable (.Load() returns nil).
type peerHealthSnapPtr = atomic.Pointer[peerHealthSnapshot]
