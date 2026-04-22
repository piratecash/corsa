package node

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// cmSlotRecord captures the ConnectionManager slot fields needed by the hot
// RPC handlers (peerHealthFrames, buildPeerExchangeResponse).  Fields are
// frozen at rebuild time — handlers must never mutate a record.
//
// ConnectedAddress is a value-copy of SlotInfo.ConnectedAddress; a nil
// pointer here means "not connected" (slot is dialing, queued, cooling
// down, etc.).
type cmSlotRecord struct {
	Address          domain.PeerAddress
	State            domain.SlotState
	RetryCount       int
	Generation       uint64
	ConnectedAddress *domain.PeerAddress
}

// cmSlotsSnapshot is the cached, lock-free view of ConnectionManager.Slots()
// consumed by fetch_peer_health and get_peers.  Primed synchronously by
// primeHotReadSnapshots() in Run() before the listener opens, then rebuilt
// by hotReadsRefreshLoop every networkStatsSnapshotInterval via
// ConnectionManager.Slots() (which acquires cm.mu.RLock).  The RPC
// handlers perform a single atomic load and never acquire cm.mu — there
// is no synchronous fallback rebuild on the RPC goroutine, so the hot
// path is statically decoupled from cm.mu.
//
// Without this cache the "atomic snapshot / lock-free hot path" property of
// the peer_health and peers_exchange snapshots would be undermined by the
// residual cm.mu.RLock that Slots() takes — Go's RWMutex is writer-
// preferring, so a queued CM writer would block these RPC readers exactly
// the way the pre-split Service lock used to on s.peerMu.  Moving the
// Slots() call onto the refresher
// goroutine pushes that coupling off the RPC path; under slot churn the
// RPC keeps serving the previous snapshot while the refresher may stall on
// cm.mu.RLock.
//
// Bounded-staleness contract matches the other hot-read snapshots: worst-
// case staleness equals networkStatsSnapshotInterval plus the time the
// refresher needs to acquire cm.mu.RLock.  Slot transitions do not trigger
// an eager rebuild — UI pollers observe new slot states on the next tick.
type cmSlotsSnapshot struct {
	// all preserves the full slot list so peers_exchange can iterate it and
	// filter by State == domain.SlotStateActive.  Order matches
	// ConnectionManager.Slots() at refresh time.
	all []cmSlotRecord
	// byAddress indexes all by .Address for O(1) lookup in peerHealthFrames.
	// Multiple slots for the same address are not expected (CM keys slots
	// by address); if one ever appears the last write wins here, matching
	// the original inline-lookup map behaviour.
	byAddress   map[domain.PeerAddress]cmSlotRecord
	generatedAt time.Time
}

// loadCMSlotsSnapshot atomically retrieves the last-published slot snapshot.
// Guaranteed non-nil on the RPC path once Run() has executed
// primeHotReadSnapshots() before opening the listener.  Returns nil only
// for unit tests that bypass Run() without calling rebuildCMSlotsSnapshot
// manually.
func (s *Service) loadCMSlotsSnapshot() *cmSlotsSnapshot {
	return s.cmSlotsSnap.Load()
}

// rebuildCMSlotsSnapshot calls ConnectionManager.Slots() (which takes
// cm.mu.RLock internally), copies the returned SlotInfo values into
// cmSlotRecord, and publishes the result atomically.  Never acquires s.peerMu.
//
// Copies the ConnectedAddress pointer through by dereferencing and re-
// addressing the local so the cached snapshot does not alias SlotInfo's
// internal storage.  Without this step a CM slot mutation could race with
// a handler reading through the snapshot's pointer.
func (s *Service) rebuildCMSlotsSnapshot() {
	if s.connManager == nil {
		// No CM wired (tests that bypass NewService).  Publish an empty
		// snapshot so handlers take the same code path they would with a
		// real CM that currently has zero slots.
		s.cmSlotsSnap.Store(&cmSlotsSnapshot{
			byAddress:   make(map[domain.PeerAddress]cmSlotRecord),
			generatedAt: time.Now().UTC(),
		})
		return
	}

	log.Trace().Msg("cm_slots_snapshot_refresh_begin")
	infos := s.connManager.Slots()

	all := make([]cmSlotRecord, 0, len(infos))
	byAddress := make(map[domain.PeerAddress]cmSlotRecord, len(infos))
	for _, si := range infos {
		rec := cmSlotRecord{
			Address:    si.Address,
			State:      si.State,
			RetryCount: si.RetryCount,
			Generation: si.Generation,
		}
		if si.ConnectedAddress != nil {
			addr := *si.ConnectedAddress
			rec.ConnectedAddress = &addr
		}
		all = append(all, rec)
		byAddress[si.Address] = rec
	}

	snap := &cmSlotsSnapshot{
		all:         all,
		byAddress:   byAddress,
		generatedAt: time.Now().UTC(),
	}
	s.cmSlotsSnap.Store(snap)
	log.Trace().Int("slots", len(all)).Msg("cm_slots_snapshot_refresh_end")
}

// cmSlotsSnapPtr wraps atomic.Pointer[cmSlotsSnapshot] — mirror of
// networkStatsSnapPtr / peerHealthSnapPtr / peersExchangeSnapPtr.  Zero
// value is usable: .Load() returns nil until the first publish.
type cmSlotsSnapPtr = atomic.Pointer[cmSlotsSnapshot]
