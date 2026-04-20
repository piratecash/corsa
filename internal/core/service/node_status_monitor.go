package service

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// NodeStatusProvider abstracts read access to the aggregated node status.
// Components that only need to display or query network state depend on
// this interface, not the concrete NodeStatusMonitor.
type NodeStatusProvider interface {
	// NodeStatus returns a deep copy of the current aggregated status.
	// Safe to call from any goroutine.
	NodeStatus() NodeStatus

	// Contacts returns a shallow copy of the current contact map.
	Contacts() map[string]Contact

	// IsReachable reports whether the given identity has at least one
	// live route in the routing table (direct or via transit).
	IsReachable(id domain.PeerIdentity) bool

	// Reset clears all accumulated state so the next probe seed
	// starts from a clean baseline. Called on identity reset.
	Reset()
}

// defaultCaptureRetention is the default TTL for stopped CaptureSessions —
// long enough that the user notices a "stop with error" toast before the
// entry is swept, short enough that finished sessions do not pile up across
// long uptimes. Used when NodeStatusMonitorOpts.CaptureRetention is zero.
const defaultCaptureRetention = 60 * time.Second

// NodeStatusMonitorOpts holds the required dependencies for constructing
// a NodeStatusMonitor. EventBus, Client, and OnChanged are mandatory;
// Clock and CaptureRetention are optional (defaults applied when zero).
type NodeStatusMonitorOpts struct {
	EventBus         *ebus.Bus
	Client           *DesktopClient
	OnChanged        func() // called after every status mutation (must not block)
	Clock            func() time.Time
	CaptureRetention time.Duration
}

// NodeStatusMonitor aggregates network-layer state from ebus events and
// the initial ProbeNode snapshot. It is the single owner of NodeStatus —
// subscribers (DMRouter, ConsoleWindow) read snapshots without holding any
// foreign lock.
//
// Responsibilities that previously lived in DMRouter:
//   - PeerHealth aggregation (TopicPeerHealthChanged, TopicSlotStateChanged,
//     TopicPeerPendingChanged, TopicPeerTrafficUpdated)
//   - Aggregate network counters (TopicAggregateStatusChanged)
//   - Version policy (TopicVersionPolicyChanged)
//   - Reachability tracking (TopicRouteTableChanged)
//   - Contact management (TopicContactAdded, TopicContactRemoved)
//   - Identity registry (TopicIdentityAdded)
//   - ProbeNode merge logic (mergeNodeStatusLocked, mergePeerHealth,
//     mergeAggregateStatus)
type NodeStatusMonitor struct {
	eventBus  *ebus.Bus
	client    *DesktopClient
	onChanged func()

	mu     sync.RWMutex
	status NodeStatus

	// ebusHealthSeeded records addresses that have received at least one
	// applyPeerHealthDelta call. A health delta sets ALL scalar fields
	// (State, Connected, Score, PendingCount, etc.) to their authoritative
	// values — including legitimate zeros/empties (e.g. Connected=false on
	// disconnect, Score=0). Entries in this set must NOT be enriched from
	// a stale ProbeNode snapshot; only true placeholders (created by
	// applySlotStateDelta / applyPeerPendingDelta without a subsequent
	// health delta) are safe to enrich.
	ebusHealthSeeded map[string]struct{}

	// Ebus-seeded flags: true once the corresponding handler has written
	// at least once. Used by mergeNodeStatusLocked to decide whether to
	// preserve ebus state or let the ProbeNode snapshot seed the field.
	ebusAggregateCountersSeeded bool
	ebusVersionPolicySeeded     bool

	// clock returns the current time; injectable so tests can advance the
	// capture-retention TTL deterministically without sleeping.
	clock func() time.Time

	// captureRetention is the TTL for stopped CaptureSessions. A session
	// is evicted the first time a capture handler runs after StoppedAt +
	// captureRetention has elapsed — the lazy sweep avoids a background
	// goroutine and keeps the monitor independent of a lifecycle context.
	captureRetention time.Duration
}

// NewNodeStatusMonitor creates a monitor with all dependencies injected
// via the opts struct. Clock defaults to time.Now and CaptureRetention
// defaults to defaultCaptureRetention when the caller leaves them zero.
func NewNodeStatusMonitor(opts NodeStatusMonitorOpts) *NodeStatusMonitor {
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	retention := opts.CaptureRetention
	if retention <= 0 {
		retention = defaultCaptureRetention
	}
	return &NodeStatusMonitor{
		eventBus:         opts.EventBus,
		client:           opts.Client,
		onChanged:        opts.OnChanged,
		clock:            clock,
		captureRetention: retention,
	}
}

// Start subscribes to all network-layer ebus events. Must be called
// before any events are published (typically before node.Start()).
func (m *NodeStatusMonitor) Start() {
	m.subscribeEvents()
}

// NodeStatus returns a deep copy of the current aggregated status.
func (m *NodeStatusMonitor) NodeStatus() NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deepCopyNodeStatus(m.status)
}

// Contacts returns a copy of the contact map.
func (m *NodeStatusMonitor) Contacts() map[string]Contact {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status.Contacts == nil {
		return nil
	}
	cp := make(map[string]Contact, len(m.status.Contacts))
	for k, v := range m.status.Contacts {
		cp[k] = v
	}
	return cp
}

// IsReachable reports whether the identity has at least one live route.
func (m *NodeStatusMonitor) IsReachable(id domain.PeerIdentity) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status.ReachableIDs[id]
}

// Reset clears all status and ebus-seeded flags. Called on identity
// reset so the next SeedFromProbe writes fresh data instead of
// preserving stale state from a previous session.
func (m *NodeStatusMonitor) Reset() {
	m.mu.Lock()
	m.status = NodeStatus{}
	m.ebusHealthSeeded = nil
	m.ebusAggregateCountersSeeded = false
	m.ebusVersionPolicySeeded = false
	m.mu.Unlock()
}

// SeedFromProbe applies a full ProbeNode snapshot, merging with any
// ebus-driven data that arrived before the probe completed. Only the
// network-related fields are written; DM-specific fields (DMHeaders,
// DeliveryReceipts) are passed through unchanged so the caller can
// use them separately.
func (m *NodeStatusMonitor) SeedFromProbe(s NodeStatus) {
	m.mu.Lock()
	m.mergeNodeStatusLocked(s)
	m.mu.Unlock()

	m.onChanged()
}

// FetchAndSeed performs the initial ProbeNode RPC and seeds the monitor.
// Returns the full NodeStatus so the caller can extract DM-specific
// fields (DMHeaders, DeliveryReceipts) for its own processing.
func (m *NodeStatusMonitor) FetchAndSeed(ctx context.Context) NodeStatus {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	status := m.client.ProbeNode(probeCtx)
	cancel()

	m.SeedFromProbe(status)
	return status
}

// ── Ebus subscriptions ──

func (m *NodeStatusMonitor) subscribeEvents() {
	if m.eventBus == nil {
		return
	}

	// Aggregate network status changed (peer connected/disconnected, health shift).
	m.eventBus.Subscribe(ebus.TopicAggregateStatusChanged, func(snap domain.AggregateStatusSnapshot) {
		m.mu.Lock()
		if m.status.AggregateStatus == nil {
			m.status.AggregateStatus = &AggregateStatus{}
		}
		m.status.AggregateStatus.Status = string(snap.Status)
		m.status.AggregateStatus.UsablePeers = snap.UsablePeers
		m.status.AggregateStatus.ConnectedPeers = snap.ConnectedPeers
		m.status.AggregateStatus.TotalPeers = snap.TotalPeers
		m.status.AggregateStatus.PendingMessages = snap.PendingMessages
		m.ebusAggregateCountersSeeded = true
		if !snap.ComputedAt.IsZero() {
			m.status.CheckedAt = snap.ComputedAt
		}
		m.mu.Unlock()

		m.onChanged()
	})

	// Version policy recomputed — update the cached version-update signal.
	m.eventBus.Subscribe(ebus.TopicVersionPolicyChanged, func(snap domain.VersionPolicySnapshot) {
		m.mu.Lock()
		if m.status.AggregateStatus == nil {
			m.status.AggregateStatus = &AggregateStatus{}
		}
		m.status.AggregateStatus.UpdateAvailable = snap.UpdateAvailable
		m.status.AggregateStatus.UpdateReason = string(snap.UpdateReason)
		m.status.AggregateStatus.IncompatibleVersionReporters = int(snap.IncompatibleVersionReporters)
		m.status.AggregateStatus.MaxObservedPeerBuild = snap.MaxObservedPeerBuild
		m.status.AggregateStatus.MaxObservedPeerVersion = int(snap.MaxObservedPeerVersion)
		m.ebusVersionPolicySeeded = true
		m.mu.Unlock()

		m.onChanged()
	})

	// Routing table changed — rebuild ReachableIDs from the authoritative
	// routing snapshot. This fires for direct-peer add/remove, announcement
	// acceptance, and transit invalidation — all cases that affect which
	// identities have at least one live route.
	m.eventBus.Subscribe(ebus.TopicRouteTableChanged, func(summary ebus.RouteTableChange) {
		fresh := m.client.BuildReachableIDs()
		m.mu.Lock()
		m.status.ReachableIDs = fresh
		m.mu.Unlock()

		m.onChanged()
	})

	// Individual peer health changed — apply state delta directly.
	m.eventBus.Subscribe(ebus.TopicPeerHealthChanged, func(delta ebus.PeerHealthDelta) {
		m.applyPeerHealthDelta(delta)
	})

	// Slot state changed — update SlotState field in PeerHealth.
	m.eventBus.Subscribe(ebus.TopicSlotStateChanged, func(address domain.PeerAddress, slotState string) {
		m.applySlotStateDelta(address, slotState)
	})

	// Per-peer pending count changed.
	m.eventBus.Subscribe(ebus.TopicPeerPendingChanged, func(delta ebus.PeerPendingDelta) {
		m.applyPeerPendingDelta(delta)
	})

	// Peer traffic updated — apply byte counters from periodic batch snapshot.
	m.eventBus.Subscribe(ebus.TopicPeerTrafficUpdated, func(batch ebus.PeerTrafficBatch) {
		m.applyTrafficBatch(batch)
	})

	// Contact added/updated — upsert in local map.
	m.eventBus.Subscribe(ebus.TopicContactAdded, func(c ebus.ContactAddedEvent) {
		m.mu.Lock()
		if m.status.Contacts == nil {
			m.status.Contacts = make(map[string]Contact)
		}
		m.status.Contacts[c.Address] = Contact{
			PubKey:       c.PubKey,
			BoxKey:       c.BoxKey,
			BoxSignature: c.BoxSig,
		}
		m.mu.Unlock()

		m.onChanged()
	})

	// Contact removed — delete from local map.
	m.eventBus.Subscribe(ebus.TopicContactRemoved, func(address string) {
		m.mu.Lock()
		delete(m.status.Contacts, address)
		m.mu.Unlock()

		m.onChanged()
	})

	// New identity discovered — append to local list.
	m.eventBus.Subscribe(ebus.TopicIdentityAdded, func(address string) {
		m.mu.Lock()
		found := false
		for _, id := range m.status.KnownIDs {
			if id == address {
				found = true
				break
			}
		}
		if !found {
			m.status.KnownIDs = append(m.status.KnownIDs, address)
		}
		m.mu.Unlock()

		m.onChanged()
	})

	// Capture session started — flip Recording* on the matching row so the
	// UI "recording" dot and "Stop all recordings" banner appear without
	// waiting for the next probe. Before this hook the monitor relied on
	// fetchPeerHealth for recording visibility, which went stale after the
	// one-time FetchAndSeed.
	m.eventBus.Subscribe(ebus.TopicCaptureSessionStarted, func(ev ebus.CaptureSessionStarted) {
		m.applyCaptureStarted(ev)
	})

	// Capture session stopped — clear Recording* on the matching row so the
	// indicator goes off immediately when the user stops recording, instead
	// of freezing until the next manual refresh.
	m.eventBus.Subscribe(ebus.TopicCaptureSessionStopped, func(ev ebus.CaptureSessionStopped) {
		m.applyCaptureStopped(ev)
	})
}

// ── Delta applicators ──

// applyPeerHealthDelta updates discrete health fields for a single peer
// address. The delta carries the outbound session ConnID and the full set
// of active inbound ConnIDs, enabling the monitor to maintain one row per
// live connection and automatically prune rows for dead connections.
//
// Reconciliation model:
//  1. Build expectedConnIDs from delta (outbound ConnID + InboundConnIDs).
//  2. Update existing rows: outbound row gets full session-scoped write;
//     inbound rows receive address-level fields only (their ConnID and
//     Direction are immutable row identifiers). A ConnID=0 placeholder
//     is left untouched when the delta carries live InboundConnIDs —
//     its address-level slot metadata will be migrated onto the surviving
//     per-ConnID rows before it is pruned.
//  3. Create new rows for InboundConnIDs not yet present.
//  4. Prune rows whose ConnID is no longer in expectedConnIDs (dead
//     connections). A ConnID=0 "address row" is pruned when per-ConnID
//     rows authoritatively represent the address (expectedConnIDs
//     non-empty) and survives otherwise. Before pruning the placeholder,
//     its SlotState and PendingCount (which ride on separate ebus
//     topics, not on PeerHealthDelta) are migrated onto surviving
//     per-ConnID rows where those fields are still empty.
func (m *NodeStatusMonitor) applyPeerHealthDelta(delta ebus.PeerHealthDelta) {
	m.mu.Lock()
	if m.ebusHealthSeeded == nil {
		m.ebusHealthSeeded = make(map[string]struct{})
	}
	m.ebusHealthSeeded[string(delta.Address)] = struct{}{}

	addr := string(delta.Address)

	// ── 1. Build the expected set of ConnIDs for this address ──
	expectedConnIDs := make(map[uint64]struct{}, 1+len(delta.InboundConnIDs))
	if delta.ConnID != 0 {
		expectedConnIDs[delta.ConnID] = struct{}{}
	}
	for _, cid := range delta.InboundConnIDs {
		expectedConnIDs[cid] = struct{}{}
	}

	// ── 2. Update existing rows ──
	outboundFound := false
	existingConnIDs := make(map[uint64]struct{})
	for i := range m.status.PeerHealth {
		p := &m.status.PeerHealth[i]
		if p.Address != addr {
			continue
		}
		existingConnIDs[p.ConnID] = struct{}{}

		switch {
		case delta.ConnID != 0 && p.ConnID == delta.ConnID:
			// Outbound row — full session write.
			applyHealthDeltaToRow(p, delta, true)
			outboundFound = true
		case delta.ConnID != 0 && p.ConnID == 0 && !outboundFound:
			// ConnID=0 placeholder promoted to outbound row.
			applyHealthDeltaToRow(p, delta, true)
			outboundFound = true
		case p.ConnID == 0 && delta.ConnID == 0:
			// ConnID=0 placeholder for this address. When the delta
			// advertises live inbound ConnIDs, those per-ConnID rows
			// will supersede the placeholder in step 5, so we must
			// preserve the placeholder's address-level slot metadata
			// (SlotState, PendingCount) untouched — it's migrated onto
			// the surviving rows before pruning. Mutating here would
			// clobber PendingCount with delta.PendingCount before the
			// migration could capture it.
			// Without inbound rows, the placeholder IS the address row:
			// apply the full delta as before.
			if len(delta.InboundConnIDs) == 0 {
				applyHealthDeltaToRow(p, delta, true)
			}
			outboundFound = true
		default:
			// Per-ConnID inbound row — address-level fields only;
			// ConnID and Direction are row identity, not mutable.
			applyHealthDeltaToRow(p, delta, false)
		}
	}

	// ── 3. Create outbound row if nothing was found ──
	// For ConnID != 0 (outbound session): always create — the session
	// needs a dedicated row for session-scoped fields.
	// For ConnID == 0 (no outbound): only create when no rows exist for
	// this address. When inbound rows exist and the peer is connected,
	// address-level fields were already applied via the default branch
	// in step 2; adding a ConnID=0 row would be redundant.
	// When disconnected: always create — per-ConnID rows will be pruned
	// in step 5, and the ConnID=0 row is the surviving address row.
	if !outboundFound && (delta.ConnID != 0 || !delta.Connected || len(existingConnIDs) == 0) {
		m.status.PeerHealth = append(m.status.PeerHealth, PeerHealth{
			Address:             addr,
			PeerID:              string(delta.PeerID),
			Direction:           string(delta.Direction),
			ClientVersion:       delta.ClientVersion,
			ClientBuild:         delta.ClientBuild,
			ProtocolVersion:     delta.ProtocolVersion,
			ConnID:              delta.ConnID,
			State:               delta.State,
			Connected:           delta.Connected,
			PendingCount:        delta.PendingCount,
			Score:               delta.Score,
			ConsecutiveFailures: delta.ConsecutiveFailures,
			LastError:           delta.LastError,
			LastConnectedAt:     domain.TimeFromPtr(delta.LastConnectedAt),
			LastDisconnectedAt:  domain.TimeFromPtr(delta.LastDisconnectedAt),
			LastPingAt:          domain.TimeFromPtr(delta.LastPingAt),
			LastPongAt:          domain.TimeFromPtr(delta.LastPongAt),
			LastUsefulSendAt:    domain.TimeFromPtr(delta.LastUsefulSendAt),
			LastUsefulReceiveAt: domain.TimeFromPtr(delta.LastUsefulReceiveAt),
			// Diagnostic fields — ebus is authoritative, so the freshly
			// created row must match what a subsequent applyHealthDeltaToRow
			// would write. Omitting them would leave a window where the UI
			// shows a fake "never banned / no version errors" state until
			// the next delta.
			BannedUntil:                 domain.TimeFromPtr(delta.BannedUntil),
			LastErrorCode:               delta.LastErrorCode,
			LastDisconnectCode:          delta.LastDisconnectCode,
			IncompatibleVersionAttempts: int(delta.IncompatibleVersionAttempts),
			LastIncompatibleVersionAt:   domain.TimeFromPtr(delta.LastIncompatibleVersionAt),
			ObservedPeerVersion:         int(delta.ObservedPeerVersion),
			ObservedPeerMinimumVersion:  int(delta.ObservedPeerMinimumVersion),
			VersionLockoutActive:        delta.VersionLockoutActive,
		})
	}

	// ── 4. Create rows for new inbound ConnIDs ──
	// ClientVersion, ClientBuild and ProtocolVersion are address-scoped
	// (node populates peerHealthFrames() from per-address maps) and are
	// copied onto every inbound row by peerHealthFrames(). They must be
	// carried here too — subsequent deltas for an existing inbound row hit
	// applyHealthDeltaToRow with writeSession=false and will not refill
	// these fields, so blank values would persist until the next full probe.
	for _, cid := range delta.InboundConnIDs {
		if _, exists := existingConnIDs[cid]; !exists {
			m.status.PeerHealth = append(m.status.PeerHealth, PeerHealth{
				Address:             addr,
				PeerID:              string(delta.PeerID),
				ConnID:              cid,
				Direction:           "inbound",
				ClientVersion:       delta.ClientVersion,
				ClientBuild:         delta.ClientBuild,
				ProtocolVersion:     delta.ProtocolVersion,
				State:               delta.State,
				Connected:           delta.Connected,
				Score:               delta.Score,
				PendingCount:        delta.PendingCount,
				ConsecutiveFailures: delta.ConsecutiveFailures,
				LastError:           delta.LastError,
				LastConnectedAt:     domain.TimeFromPtr(delta.LastConnectedAt),
				LastDisconnectedAt:  domain.TimeFromPtr(delta.LastDisconnectedAt),
				LastPingAt:          domain.TimeFromPtr(delta.LastPingAt),
				LastPongAt:          domain.TimeFromPtr(delta.LastPongAt),
				LastUsefulSendAt:    domain.TimeFromPtr(delta.LastUsefulSendAt),
				LastUsefulReceiveAt: domain.TimeFromPtr(delta.LastUsefulReceiveAt),
				// Diagnostic fields — address-level, must match the outbound
				// row created above (both reflect the same peerHealth entry
				// on the node side).
				BannedUntil:                 domain.TimeFromPtr(delta.BannedUntil),
				LastErrorCode:               delta.LastErrorCode,
				LastDisconnectCode:          delta.LastDisconnectCode,
				IncompatibleVersionAttempts: int(delta.IncompatibleVersionAttempts),
				LastIncompatibleVersionAt:   domain.TimeFromPtr(delta.LastIncompatibleVersionAt),
				ObservedPeerVersion:         int(delta.ObservedPeerVersion),
				ObservedPeerMinimumVersion:  int(delta.ObservedPeerMinimumVersion),
				VersionLockoutActive:        delta.VersionLockoutActive,
			})
		}
	}

	// ── 5. Prune rows for dead connections ──
	// A per-ConnID row is pruned when its ConnID is no longer in
	// expectedConnIDs. A ConnID=0 "address row" is pruned when
	// per-ConnID rows authoritatively represent the address
	// (expectedConnIDs non-empty); otherwise it survives to carry the
	// peer's aggregate state.
	//
	// Pruning triggers:
	//   - expectedConnIDs is non-empty: some connections are alive,
	//     per-ConnID rows own the address. Prune rows not in the
	//     expected set AND prune any redundant ConnID=0 placeholder.
	//     Before pruning, capture the placeholder's SlotState and
	//     PendingCount and migrate them onto surviving per-ConnID rows —
	//     those address-level fields arrive via separate ebus topics
	//     (TopicSlotStateChanged, TopicPeerPendingChanged) and are NOT
	//     carried on PeerHealthDelta, so losing the placeholder would
	//     silently discard them.
	//   - !delta.Connected (full disconnect): all connections are dead,
	//     prune all per-ConnID rows. The surviving ConnID=0 row (created
	//     in step 3) carries the disconnected state.
	if len(expectedConnIDs) > 0 || !delta.Connected {
		var migratedSlotState string
		var migratedPendingCount int
		shouldMigrate := false
		if len(expectedConnIDs) > 0 {
			for i := range m.status.PeerHealth {
				p := &m.status.PeerHealth[i]
				if p.Address == addr && p.ConnID == 0 {
					migratedSlotState = p.SlotState
					migratedPendingCount = p.PendingCount
					shouldMigrate = true
					break
				}
			}
		}

		j := 0
		for i := range m.status.PeerHealth {
			p := &m.status.PeerHealth[i]
			if p.Address == addr {
				if p.ConnID != 0 {
					if _, alive := expectedConnIDs[p.ConnID]; !alive {
						continue // prune dead connection row
					}
				} else if len(expectedConnIDs) > 0 {
					continue // prune redundant address-level placeholder
				}
			}
			m.status.PeerHealth[j] = m.status.PeerHealth[i]
			j++
		}
		m.status.PeerHealth = m.status.PeerHealth[:j]

		// Migrate address-level slot metadata from the pruned
		// placeholder onto surviving per-ConnID rows. Only fill
		// empty values — a row that already carries SlotState from a
		// prior applySlotStateDelta is authoritative and must not be
		// stomped by the placeholder's (possibly older) value.
		if shouldMigrate {
			for i := range m.status.PeerHealth {
				p := &m.status.PeerHealth[i]
				if p.Address != addr {
					continue
				}
				if p.SlotState == "" {
					p.SlotState = migratedSlotState
				}
				if p.PendingCount == 0 {
					p.PendingCount = migratedPendingCount
				}
			}
		}
	}

	m.mu.Unlock()
	m.onChanged()
}

// applyHealthDeltaToRow writes delta fields into an existing PeerHealth row.
// Address-level fields (State, Connected, Score, timestamps…) are always
// written. Session-scoped fields (ConnID, Direction, ClientVersion,
// ClientBuild, ProtocolVersion) are written only when writeSession is true —
// this prevents a connect-path delta from writing stale session fields into
// rows where the caller hasn't determined the correct connection context.
func applyHealthDeltaToRow(p *PeerHealth, delta ebus.PeerHealthDelta, writeSession bool) {
	// PeerID: persistent identity, always backfilled.
	if p.PeerID == "" && delta.PeerID != "" {
		p.PeerID = string(delta.PeerID)
	}

	// Session-scoped metadata: on disconnect clear unconditionally
	// (the node clears them); on connect backfill only when empty.
	if writeSession {
		if !delta.Connected {
			p.Direction = string(delta.Direction)
			p.ClientVersion = delta.ClientVersion
			p.ClientBuild = delta.ClientBuild
			p.ConnID = delta.ConnID
			p.ProtocolVersion = delta.ProtocolVersion
		} else {
			if p.Direction == "" && delta.Direction != "" {
				p.Direction = string(delta.Direction)
			}
			if delta.ClientVersion != "" {
				p.ClientVersion = delta.ClientVersion
			}
			if delta.ClientBuild != 0 {
				p.ClientBuild = delta.ClientBuild
			}
			if delta.ConnID != 0 {
				p.ConnID = delta.ConnID
			}
			if delta.ProtocolVersion != 0 {
				p.ProtocolVersion = delta.ProtocolVersion
			}
		}
	}

	// Address-level fields: always written regardless of writeSession.
	p.PendingCount = delta.PendingCount
	p.State = delta.State
	p.Connected = delta.Connected
	p.Score = delta.Score
	p.ConsecutiveFailures = delta.ConsecutiveFailures
	p.LastError = delta.LastError
	// Activity timestamps — nil on the delta means "this delta does not
	// touch the field"; a non-nil pointer (whose pointee may be zero) is
	// the explicit signal to update. domain.TimeFromPtr converts the
	// boundary representation into the value-typed OptionalTime stored
	// on PeerHealth.
	if delta.LastConnectedAt != nil {
		p.LastConnectedAt = domain.TimeFromPtr(delta.LastConnectedAt)
	}
	if delta.LastDisconnectedAt != nil {
		p.LastDisconnectedAt = domain.TimeFromPtr(delta.LastDisconnectedAt)
	}
	if delta.LastPingAt != nil {
		p.LastPingAt = domain.TimeFromPtr(delta.LastPingAt)
	}
	if delta.LastPongAt != nil {
		p.LastPongAt = domain.TimeFromPtr(delta.LastPongAt)
	}
	if delta.LastUsefulSendAt != nil {
		p.LastUsefulSendAt = domain.TimeFromPtr(delta.LastUsefulSendAt)
	}
	if delta.LastUsefulReceiveAt != nil {
		p.LastUsefulReceiveAt = domain.TimeFromPtr(delta.LastUsefulReceiveAt)
	}

	// Diagnostic fields — ebus-authoritative. A nil value on the delta is
	// the explicit "cleared by recovery" signal from the node, not
	// "unchanged", so these are written unconditionally (unlike the
	// activity timestamps above, where nil means "delta does not touch
	// the field"). Without unconditional write, ban clears and
	// incompatible-version resets would never reach the UI.
	p.BannedUntil = domain.TimeFromPtr(delta.BannedUntil)
	p.LastErrorCode = delta.LastErrorCode
	p.LastDisconnectCode = delta.LastDisconnectCode
	p.IncompatibleVersionAttempts = int(delta.IncompatibleVersionAttempts)
	p.LastIncompatibleVersionAt = domain.TimeFromPtr(delta.LastIncompatibleVersionAt)
	p.ObservedPeerVersion = int(delta.ObservedPeerVersion)
	p.ObservedPeerMinimumVersion = int(delta.ObservedPeerMinimumVersion)
	p.VersionLockoutActive = delta.VersionLockoutActive
}

// applySlotStateDelta updates the SlotState field in PeerHealth.
// Slot state is address-level (CM slots track overlay addresses, not
// individual connections), so all per-ConnID rows for the address are
// updated — matching the probe behavior where the base PeerHealthFrame
// carries SlotState before per-ConnID expansion.
func (m *NodeStatusMonitor) applySlotStateDelta(address domain.PeerAddress, slotState string) {
	m.mu.Lock()
	addr := string(address)
	found := false
	for i := range m.status.PeerHealth {
		if m.status.PeerHealth[i].Address == addr {
			m.status.PeerHealth[i].SlotState = slotState
			found = true
		}
	}
	// Peer not in PeerHealth yet — append a minimal entry so the UI can
	// show the slot lifecycle state even before TopicPeerHealthChanged fires.
	if !found && slotState != "" {
		m.status.PeerHealth = append(m.status.PeerHealth, PeerHealth{
			Address:   addr,
			SlotState: slotState,
		})
	}
	m.mu.Unlock()

	m.onChanged()
}

// applyPeerPendingDelta updates the PendingCount field in PeerHealth.
// Pending count is address-level (the pending queue is keyed by overlay
// address), so all per-ConnID rows for the address are updated.
func (m *NodeStatusMonitor) applyPeerPendingDelta(delta ebus.PeerPendingDelta) {
	m.mu.Lock()
	addr := string(delta.Address)
	found := false
	for i := range m.status.PeerHealth {
		if m.status.PeerHealth[i].Address == addr {
			m.status.PeerHealth[i].PendingCount = delta.Count
			found = true
		}
	}
	// Peer not in PeerHealth yet — create a minimal entry so the pending
	// count is visible as soon as the peer card appears.
	if !found && delta.Count > 0 {
		m.status.PeerHealth = append(m.status.PeerHealth, PeerHealth{
			Address:      addr,
			PendingCount: delta.Count,
		})
	}
	m.mu.Unlock()

	m.onChanged()
}

// applyTrafficBatch applies byte counters from a periodic batch snapshot.
// Traffic counters are address-level, so all per-ConnID rows for each
// address are updated with the same snapshot values.
func (m *NodeStatusMonitor) applyTrafficBatch(batch ebus.PeerTrafficBatch) {
	m.mu.Lock()
	updated := false
	for _, snap := range batch.Peers {
		addr := string(snap.Address)
		for i := range m.status.PeerHealth {
			if m.status.PeerHealth[i].Address != addr {
				continue
			}
			m.status.PeerHealth[i].BytesSent = snap.BytesSent
			m.status.PeerHealth[i].BytesReceived = snap.BytesReceived
			m.status.PeerHealth[i].TotalTraffic = snap.BytesSent + snap.BytesReceived
			updated = true
			// don't break — update all rows for this address
		}
		// Peer not in PeerHealth yet — skip. The next TopicPeerHealthChanged
		// will create the entry; traffic catches up next tick.
	}
	m.mu.Unlock()

	if updated {
		m.onChanged()
	}
}

// applyCaptureStarted writes a CaptureSession entry keyed by the event's
// ConnID. PeerHealth rows are not touched — the UI reads recording state
// from NodeStatus.CaptureSessions, so the lifecycle of capture bookkeeping
// is independent of peer-health row pruning.
//
// A fresh start overwrites any lingering stopped entry on the same ConnID
// (quick stop→start cycle): the new session has no error and fresh drop
// counters until it reports otherwise. Before allocating the new entry we
// sweep expired stopped sessions so long-lived monitors do not accumulate
// terminal records forever.
//
// Format defaults to CaptureFormatCompact when the event carries an empty
// value so the stored entry always has a valid format label for the UI.
func (m *NodeStatusMonitor) applyCaptureStarted(ev ebus.CaptureSessionStarted) {
	m.mu.Lock()
	m.evictExpiredCaptureSessionsLocked()
	if m.status.CaptureSessions == nil {
		m.status.CaptureSessions = make(map[domain.ConnID]CaptureSession)
	}
	format := ev.Format
	if !format.IsValid() {
		format = domain.CaptureFormatCompact
	}
	m.status.CaptureSessions[ev.ConnID] = CaptureSession{
		ConnID:    ev.ConnID,
		Address:   ev.Address,
		PeerID:    ev.PeerID,
		Direction: ev.Direction,
		FilePath:  ev.FilePath,
		StartedAt: domain.TimeFromPtr(ev.StartedAt),
		Scope:     ev.Scope,
		Format:    format,
		Active:    true,
	}
	m.mu.Unlock()

	m.onChanged()
}

// applyCaptureStopped marks the CaptureSession for this ConnID as stopped
// and stamps StoppedAt + Error + DroppedEvents so the UI can surface a
// terminal failure reason until the retention TTL elapses. When no entry
// exists the event is logged (if it carried diagnostics) and otherwise
// ignored — a cross-signal event for an already-evicted session is
// harmless.
//
// The TTL sweep also runs here so that a burst of stop events on inactive
// connections eventually reclaims the stopped entries they created.
func (m *NodeStatusMonitor) applyCaptureStopped(ev ebus.CaptureSessionStopped) {
	m.mu.Lock()
	session, ok := m.status.CaptureSessions[ev.ConnID]
	if !ok {
		m.evictExpiredCaptureSessionsLocked()
		m.mu.Unlock()
		if ev.Error != "" || ev.DroppedEvents != 0 {
			log.Warn().
				Uint64("conn_id", uint64(ev.ConnID)).
				Str("error", ev.Error).
				Int64("dropped_events", ev.DroppedEvents).
				Msg("capture session stopped with diagnostics but no entry existed")
		}
		return
	}
	session.Active = false
	session.StoppedAt = domain.TimeOf(m.clock())
	session.Error = ev.Error
	session.DroppedEvents = ev.DroppedEvents
	m.status.CaptureSessions[ev.ConnID] = session
	m.evictExpiredCaptureSessionsLocked()
	m.mu.Unlock()

	m.onChanged()
}

// evictExpiredCaptureSessionsLocked removes stopped entries whose TTL has
// elapsed. Active sessions are always kept. Must be called with m.mu held.
func (m *NodeStatusMonitor) evictExpiredCaptureSessionsLocked() {
	if len(m.status.CaptureSessions) == 0 {
		return
	}
	now := m.clock()
	for id, session := range m.status.CaptureSessions {
		if session.Active || !session.StoppedAt.Valid() {
			continue
		}
		if now.Sub(session.StoppedAt.Time()) >= m.captureRetention {
			delete(m.status.CaptureSessions, id)
		}
	}
}

// ── Merge logic ──

// mergeNodeStatusLocked applies a ProbeNode snapshot without overwriting
// fields that ebus handlers may have already populated with fresher data.
// Must be called with m.mu held.
func (m *NodeStatusMonitor) mergeNodeStatusLocked(s NodeStatus) {
	// Always-write fields: only ProbeNode provides these.
	m.status.Address = s.Address
	m.status.Connected = s.Connected
	m.status.Welcome = s.Welcome
	m.status.NodeID = s.NodeID
	m.status.NodeType = s.NodeType
	m.status.ListenerEnabled = s.ListenerEnabled
	m.status.ListenerAddress = s.ListenerAddress
	m.status.ClientVersion = s.ClientVersion
	m.status.Services = s.Services
	m.status.Capabilities = s.Capabilities
	m.status.DMHeaders = s.DMHeaders
	m.status.DeliveryReceipts = s.DeliveryReceipts
	m.status.Stored = s.Stored
	m.status.Messages = s.Messages
	m.status.MessageIDs = s.MessageIDs
	m.status.DirectMessages = s.DirectMessages
	m.status.DirectMessageIDs = s.DirectMessageIDs
	m.status.PendingMessages = s.PendingMessages
	m.status.Gazeta = s.Gazeta
	m.status.Error = s.Error
	m.status.CheckedAt = s.CheckedAt

	// Ebus-managed fields: merge probe data into existing state.
	// A single early delta (TopicContactAdded, TopicIdentityAdded,
	// TopicRouteTableChanged) makes the field non-nil; if we only seeded
	// when nil, the full probe snapshot would be lost. Instead, merge
	// probe entries into the existing collection, keeping ebus-driven
	// values for any keys that overlap (they are fresher).
	m.status.PeerHealth = mergePeerHealth(m.status.PeerHealth, s.PeerHealth, m.ebusHealthSeeded)
	m.status.Contacts = mergeContacts(m.status.Contacts, s.Contacts)
	m.status.KnownIDs = mergeKnownIDs(m.status.KnownIDs, s.KnownIDs)
	m.status.AggregateStatus = mergeAggregateStatus(
		m.status.AggregateStatus, s.AggregateStatus,
		m.ebusAggregateCountersSeeded, m.ebusVersionPolicySeeded,
	)
	m.status.ReachableIDs = mergeReachableIDs(m.status.ReachableIDs, s.ReachableIDs)
	m.status.CaptureSessions = mergeCaptureSessions(m.status.CaptureSessions, s.CaptureSessions)
}

// ── Package-level merge helpers ──

// peerHealthKey identifies a unique PeerHealth row by (Address, ConnID).
// peerHealthFrames() emits multiple rows for the same overlay address when
// several inbound connections exist, each distinguished by ConnID. Using
// Address alone as the key would collapse per-connection rows.
type peerHealthKey struct {
	Address string
	ConnID  uint64
}

// mergePeerHealth combines ebus-driven PeerHealth entries with a ProbeNode
// snapshot using two-tier enrichment.
//
// Rows are matched by (Address, ConnID) composite key so that multiple
// per-connection rows for the same overlay address (emitted by
// peerHealthFrames for inbound peers) are preserved, not collapsed.
//
// healthSeeded contains addresses that have received at least one full
// applyPeerHealthDelta. Those entries are authoritative for state fields,
// session-scoped fields, slot-lifecycle fields, and recovery-clearable
// fields — all of which use zero/nil as meaningful signals. Only truly
// persistent fields (PeerID, activity timestamps, traffic counters) are
// backfilled from the probe via enrichPeerHealthIdentityFromProbe.
//
// Entries NOT in healthSeeded are true placeholders (created by
// applySlotStateDelta / applyPeerPendingDelta) — all zero fields genuinely
// mean "not yet known" and are fully enrichable from the probe.
//
// Probe entries whose (Address, ConnID) key is not yet in the ebus list
// are appended as-is.
func mergePeerHealth(ebusList, probeList []PeerHealth, healthSeeded map[string]struct{}) []PeerHealth {
	if len(ebusList) == 0 {
		return probeList
	}
	if len(probeList) == 0 {
		return ebusList
	}

	// Index ebus entries by (Address, ConnID) for O(1) lookup + in-place enrichment.
	merged := append([]PeerHealth(nil), ebusList...)
	idxByKey := make(map[peerHealthKey]int, len(merged))
	for i, ph := range merged {
		idxByKey[peerHealthKey{ph.Address, ph.ConnID}] = i
	}

	for _, probe := range probeList {
		key := peerHealthKey{probe.Address, probe.ConnID}
		idx, exists := idxByKey[key]
		_, seeded := healthSeeded[probe.Address]

		if !exists {
			// No exact (Address, ConnID) match. For a ConnID-specific probe
			// row, check if there's a ConnID=0 ebus placeholder for the same
			// address that can be promoted.
			if probe.ConnID != 0 {
				placeholderKey := peerHealthKey{probe.Address, 0}
				if pi, ok := idxByKey[placeholderKey]; ok {
					// Promote only when:
					// - the entry is not seeded (true placeholder), OR
					// - seeded but still connected (inbound delta has
					//   ConnID=0 simply because there's no outbound session;
					//   adopting the probe's real ConnID is safe).
					// A seeded disconnected entry's ConnID=0 is authoritative
					// (session cleared); promoting it would resurrect a stale
					// ConnID from a lagging probe snapshot.
					if !seeded || merged[pi].Connected {
						merged[pi].ConnID = probe.ConnID
						delete(idxByKey, placeholderKey)
						idxByKey[key] = pi
						idx = pi
						exists = true
					}
				}
			}
			if !exists {
				// For seeded disconnected addresses, don't append stale
				// per-ConnID probe rows — the disconnect is authoritative.
				// Instead, enrich the first ebus row (persistent fields only).
				if seeded {
					enriched := false
					for k, i := range idxByKey {
						if k.Address == probe.Address {
							if !merged[i].Connected {
								enrichPeerHealthIdentityFromProbe(&merged[i], &probe)
								enriched = true
							}
							break
						}
					}
					if enriched {
						continue
					}
				}
				idxByKey[key] = len(merged)
				merged = append(merged, probe)
				continue
			}
		}

		if seeded {
			// Seeded entry: only backfill truly persistent fields (PeerID,
			// activity timestamps, traffic counters). State, session-scoped,
			// slot-lifecycle, and recovery-clearable fields are all
			// authoritative — zero/nil values are meaningful signals.
			enrichPeerHealthIdentityFromProbe(&merged[idx], &probe)
		} else {
			// True placeholder: safe to enrich all zero-valued fields.
			enrichPeerHealthFromProbe(&merged[idx], &probe)
		}
	}
	return merged
}

// enrichPeerHealthFromProbe fills zero-valued fields in dst with values from
// src (the probe snapshot). Fields that ebus has already set are left intact.
func enrichPeerHealthFromProbe(dst, src *PeerHealth) {
	if dst.PeerID == "" {
		dst.PeerID = src.PeerID
	}
	if dst.ConnID == 0 {
		dst.ConnID = src.ConnID
	}
	if dst.Direction == "" {
		dst.Direction = src.Direction
	}
	if dst.ClientVersion == "" {
		dst.ClientVersion = src.ClientVersion
	}
	if dst.ClientBuild == 0 {
		dst.ClientBuild = src.ClientBuild
	}
	if dst.ProtocolVersion == 0 {
		dst.ProtocolVersion = src.ProtocolVersion
	}
	if dst.State == "" {
		dst.State = src.State
	}
	// Connected is a bool — only backfill when the ebus entry has the default
	// false AND the probe says the peer is connected. Once ebus sets Connected
	// (either true or false via a health delta), it owns the field.
	// Placeholder entries never set Connected, so this is safe.
	if !dst.Connected && src.Connected {
		dst.Connected = src.Connected
	}
	if dst.PendingCount == 0 {
		dst.PendingCount = src.PendingCount
	}
	if dst.Score == 0 {
		dst.Score = src.Score
	}
	if dst.ConsecutiveFailures == 0 {
		dst.ConsecutiveFailures = src.ConsecutiveFailures
	}
	if dst.LastError == "" {
		dst.LastError = src.LastError
	}
	if !dst.LastConnectedAt.Valid() {
		dst.LastConnectedAt = src.LastConnectedAt
	}
	if !dst.LastDisconnectedAt.Valid() {
		dst.LastDisconnectedAt = src.LastDisconnectedAt
	}
	if !dst.LastPingAt.Valid() {
		dst.LastPingAt = src.LastPingAt
	}
	if !dst.LastPongAt.Valid() {
		dst.LastPongAt = src.LastPongAt
	}
	if !dst.LastUsefulSendAt.Valid() {
		dst.LastUsefulSendAt = src.LastUsefulSendAt
	}
	if !dst.LastUsefulReceiveAt.Valid() {
		dst.LastUsefulReceiveAt = src.LastUsefulReceiveAt
	}
	if !dst.BannedUntil.Valid() {
		dst.BannedUntil = src.BannedUntil
	}
	if dst.BytesSent == 0 {
		dst.BytesSent = src.BytesSent
	}
	if dst.BytesReceived == 0 {
		dst.BytesReceived = src.BytesReceived
	}
	if dst.TotalTraffic == 0 {
		dst.TotalTraffic = src.TotalTraffic
	}
	if dst.SlotState == "" {
		dst.SlotState = src.SlotState
	}
	if dst.SlotRetryCount == 0 {
		dst.SlotRetryCount = src.SlotRetryCount
	}
	if dst.SlotGeneration == 0 {
		dst.SlotGeneration = src.SlotGeneration
	}
	if dst.SlotConnectedAddr == "" {
		dst.SlotConnectedAddr = src.SlotConnectedAddr
	}
	if dst.LastErrorCode == "" {
		dst.LastErrorCode = src.LastErrorCode
	}
	if dst.LastDisconnectCode == "" {
		dst.LastDisconnectCode = src.LastDisconnectCode
	}
	if dst.IncompatibleVersionAttempts == 0 {
		dst.IncompatibleVersionAttempts = src.IncompatibleVersionAttempts
	}
	// Version diagnostics — complete the set so a true placeholder that
	// hits the probe before the first health-delta is fully populated,
	// not partially. Once the health-delta arrives, applyHealthDeltaToRow
	// writes all of these unconditionally.
	if !dst.LastIncompatibleVersionAt.Valid() {
		dst.LastIncompatibleVersionAt = src.LastIncompatibleVersionAt
	}
	if dst.ObservedPeerVersion == 0 {
		dst.ObservedPeerVersion = src.ObservedPeerVersion
	}
	if dst.ObservedPeerMinimumVersion == 0 {
		dst.ObservedPeerMinimumVersion = src.ObservedPeerMinimumVersion
	}
	if !dst.VersionLockoutActive && src.VersionLockoutActive {
		dst.VersionLockoutActive = src.VersionLockoutActive
	}
}

// enrichPeerHealthIdentityFromProbe backfills only persistent identity
// fields from the probe snapshot into a seeded ebus entry. Multiple
// categories of fields are explicitly excluded:
//
//   - State fields (Connected, Score, State, PendingCount,
//     ConsecutiveFailures, LastError) — written unconditionally by every
//     health delta; zero/empty values are authoritative (e.g. Score=0 on
//     disconnect).
//
//   - Session-scoped fields (Direction, ClientVersion, ClientBuild, ConnID,
//     ProtocolVersion) — cleared unconditionally on disconnect by
//     applyPeerHealthDelta so the UI does not show stale session info.
//     Empty/zero is the authoritative post-disconnect value.
//
//   - Slot-lifecycle fields (SlotState, SlotRetryCount, SlotGeneration,
//     SlotConnectedAddr) — SlotState is cleared to "" by
//     TopicSlotStateChanged as a meaningful removal signal; the companion
//     fields share the same lifecycle and become stale when the slot is
//     removed.
//
//   - Diagnostic fields (BannedUntil, LastErrorCode, LastDisconnectCode,
//     IncompatibleVersionAttempts, LastIncompatibleVersionAt,
//     ObservedPeerVersion, ObservedPeerMinimumVersion, VersionLockoutActive)
//     — these travel in PeerHealthDelta so every applyPeerHealthDelta
//     writes the current value (including explicit zeros when
//     resetPeerHealthForRecoveryLocked cleared them). Backfilling from a
//     pre-recovery probe snapshot would resurrect bans and handshake
//     evidence that the node has already cleared. Ebus is authoritative.
//
// This handles the case where the first TopicPeerHealthChanged arrives with
// empty PeerID (identity not yet resolved) and the node resolves it
// out-of-band without emitting another health delta.
func enrichPeerHealthIdentityFromProbe(dst, src *PeerHealth) {
	// PeerID: persistent identity, never cleared by health deltas.
	if dst.PeerID == "" {
		dst.PeerID = src.PeerID
	}
	// Timestamps: invalid OptionalTime means "not yet set" (explicit
	// optionality visible from the type). LastDisconnectedAt is excluded —
	// it is cleared to time.Time{} by resetPeerHealthForRecoveryLocked and
	// mapped to an invalid OptionalTime in health deltas.
	if !dst.LastConnectedAt.Valid() {
		dst.LastConnectedAt = src.LastConnectedAt
	}
	if !dst.LastPingAt.Valid() {
		dst.LastPingAt = src.LastPingAt
	}
	if !dst.LastPongAt.Valid() {
		dst.LastPongAt = src.LastPongAt
	}
	if !dst.LastUsefulSendAt.Valid() {
		dst.LastUsefulSendAt = src.LastUsefulSendAt
	}
	if !dst.LastUsefulReceiveAt.Valid() {
		dst.LastUsefulReceiveAt = src.LastUsefulReceiveAt
	}
	// Traffic counters: cumulative, never cleared by any handler.
	if dst.BytesSent == 0 {
		dst.BytesSent = src.BytesSent
	}
	if dst.BytesReceived == 0 {
		dst.BytesReceived = src.BytesReceived
	}
	if dst.TotalTraffic == 0 {
		dst.TotalTraffic = src.TotalTraffic
	}
}

// mergeContacts merges probe contacts into the ebus-driven map. Ebus entries
// take precedence on key conflicts (they are fresher real-time updates).
// Probe entries for keys not yet in the ebus map are added.
func mergeContacts(ebusCt, probeCt map[string]Contact) map[string]Contact {
	if len(ebusCt) == 0 {
		return probeCt
	}
	if len(probeCt) == 0 {
		return ebusCt
	}
	merged := make(map[string]Contact, len(probeCt))
	for k, v := range probeCt {
		merged[k] = v
	}
	// Ebus entries overwrite probe entries on conflict — they are fresher.
	for k, v := range ebusCt {
		merged[k] = v
	}
	return merged
}

// mergeKnownIDs appends probe IDs that are not already in the ebus list.
func mergeKnownIDs(ebusIDs, probeIDs []string) []string {
	if len(ebusIDs) == 0 {
		return probeIDs
	}
	if len(probeIDs) == 0 {
		return ebusIDs
	}
	existing := make(map[string]struct{}, len(ebusIDs))
	for _, id := range ebusIDs {
		existing[id] = struct{}{}
	}
	merged := append([]string(nil), ebusIDs...)
	for _, id := range probeIDs {
		if _, ok := existing[id]; !ok {
			merged = append(merged, id)
		}
	}
	return merged
}

// mergeCaptureSessions combines ebus-driven CaptureSession state with a
// ProbeNode snapshot. Ebus is authoritative for every ConnID it has already
// observed (start/stop events are the canonical lifecycle signals), so probe
// entries only fill gaps — the key is still absent from the ebus map. This
// lets a late-seed probe surface pre-existing recording sessions that started
// before the monitor subscribed, without clobbering fresher terminal
// diagnostics that ebus has already recorded.
func mergeCaptureSessions(ebusSessions, probeSessions map[domain.ConnID]CaptureSession) map[domain.ConnID]CaptureSession {
	if len(ebusSessions) == 0 {
		return probeSessions
	}
	if len(probeSessions) == 0 {
		return ebusSessions
	}
	merged := make(map[domain.ConnID]CaptureSession, len(ebusSessions)+len(probeSessions))
	for k, v := range probeSessions {
		merged[k] = v
	}
	// Ebus entries overwrite probe entries on conflict — they are fresher.
	for k, v := range ebusSessions {
		merged[k] = v
	}
	return merged
}

// mergeReachableIDs merges probe reachability into the ebus-driven map.
// Ebus entries take precedence on key conflicts.
func mergeReachableIDs(ebusIDs, probeIDs map[domain.PeerIdentity]bool) map[domain.PeerIdentity]bool {
	if len(ebusIDs) == 0 {
		return probeIDs
	}
	if len(probeIDs) == 0 {
		return ebusIDs
	}
	merged := make(map[domain.PeerIdentity]bool, len(probeIDs))
	for k, v := range probeIDs {
		merged[k] = v
	}
	for k, v := range ebusIDs {
		merged[k] = v
	}
	return merged
}

// mergeAggregateStatus combines the ebus-driven aggregate status with the
// ProbeNode snapshot. Fields that ebus has already seeded are preserved;
// the rest are taken from the probe.
func mergeAggregateStatus(ebusSt, probeSt *AggregateStatus, countersSeeded, versionSeeded bool) *AggregateStatus {
	if probeSt == nil {
		return ebusSt
	}
	if ebusSt == nil {
		clone := *probeSt
		return &clone
	}

	merged := *probeSt // start from probe baseline

	// Ebus-seeded counters are fresher — keep them.
	if countersSeeded {
		merged.Status = ebusSt.Status
		merged.UsablePeers = ebusSt.UsablePeers
		merged.ConnectedPeers = ebusSt.ConnectedPeers
		merged.TotalPeers = ebusSt.TotalPeers
		merged.PendingMessages = ebusSt.PendingMessages
	}

	// Ebus-seeded version policy is fresher — keep it.
	if versionSeeded {
		merged.UpdateAvailable = ebusSt.UpdateAvailable
		merged.UpdateReason = ebusSt.UpdateReason
		merged.IncompatibleVersionReporters = ebusSt.IncompatibleVersionReporters
		merged.MaxObservedPeerBuild = ebusSt.MaxObservedPeerBuild
		merged.MaxObservedPeerVersion = ebusSt.MaxObservedPeerVersion
	}

	return &merged
}

// ── Logging ──

func init() {
	// Ensure NodeStatusMonitor satisfies NodeStatusProvider at compile time.
	var _ NodeStatusProvider = (*NodeStatusMonitor)(nil)
	_ = log.Logger // suppress unused import if logging is removed
}
