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

	// ResourceUsageSnapshot returns an independent copy of just the
	// process memory + uptime field, or nil if not yet sampled. It is the
	// cheap counterpart to NodeStatus() for the once-per-second resource
	// tick, which must not deep-copy every peer-health / route-derived
	// collection merely to refresh two scalars.
	ResourceUsageSnapshot() *ResourceUsage

	// PeerHealthSnapshot returns an independent copy of just the PeerHealth
	// slice. Cheap counterpart to NodeStatus() for the periodic traffic
	// batch, which only mutates per-peer byte counters and must not deep-
	// copy KnownIDs / ReachableIDs / contacts / messages to refresh them.
	PeerHealthSnapshot() []PeerHealth

	// ReachableIDsSnapshot returns an independent copy of just the
	// ReachableIDs map. Cheap counterpart to NodeStatus() for route-table
	// changes, which only rebuild reachability.
	ReachableIDsSnapshot() map[domain.PeerIdentity]bool

	// PresenceSnapshot returns an independent copy of the per-contact presence
	// AND the generation it came from — both or neither.
	//
	// It has its own event (NodeStatusDomainContactPresence) rather than riding
	// the route snapshot: most of what moves presence is not a routing event at
	// all, and the two answer different questions of which only one belongs in
	// front of a person.
	//
	// The pair is returned together because the generation is the SET'S OWN
	// label, not a clock: a caller that copies one half publishes a projection
	// under the number of a different one. What that costs depends on who the
	// caller is — inside the monitor it decides ordering, and in a cache like
	// DMRouter.cachedNS it simply makes the composed snapshot describe itself
	// incorrectly to everyone who reads it. Returning both from one read
	// removes the choice rather than documenting it.
	PresenceSnapshot() (domain.PresenceSet, uint64)

	// KnownIDsSnapshot returns an independent copy of just the KnownIDs
	// slice. Cheap counterpart to NodeStatus() for identity-added events.
	KnownIDsSnapshot() []string

	// AggregateStatusSnapshot returns an independent clone of the
	// AggregateStatus pointer plus the CheckedAt timestamp (the two fields
	// the aggregate-status / version-policy events own). Cheap counterpart
	// to NodeStatus() for those events.
	AggregateStatusSnapshot() (*AggregateStatus, time.Time)

	// Contacts returns a shallow copy of the current contact map.
	Contacts() map[string]Contact

	// IsReachable reports whether the given identity has at least one
	// live route in the routing table (direct or via transit).
	IsReachable(id domain.PeerIdentity) bool

	// Reset clears all accumulated state so the next probe seed
	// starts from a clean baseline. Called on identity reset.
	Reset()
}

// NodeStatusDomain identifies which slice of NodeStatus a lightweight
// notify touched. The monitor passes it to OnPartialChanged so the
// subscriber (DMRouter) can patch just that field on its cached snapshot
// instead of deep-copying the whole NodeStatus. Events whose handler
// mutates several unrelated fields (or fields without a dedicated patch)
// keep using the full OnChanged path.
type NodeStatusDomain int

const (
	// NodeStatusDomainResourceUsage — process memory + uptime (1s sampler).
	NodeStatusDomainResourceUsage NodeStatusDomain = iota
	// NodeStatusDomainPeerHealth — per-peer rows (periodic traffic batch).
	NodeStatusDomainPeerHealth
	// NodeStatusDomainReachableIDs — routing reachability (route-table change).
	NodeStatusDomainReachableIDs
	// NodeStatusDomainPresence — a trusted contact's durable LastOnlineAt.
	// ReachableIDs belongs exclusively to NodeStatusDomainReachableIDs.
	NodeStatusDomainPresence
	// NodeStatusDomainContactPresence — the live four-state per-contact
	// belief (NodeStatus.Presence). Distinct from NodeStatusDomainPresence
	// above, which is the durable timestamp: one is "when were they last
	// seen", the other is "are they here now", and they change on different
	// events.
	NodeStatusDomainContactPresence
	// NodeStatusDomainKnownIDs — discovered identity list (identity added).
	NodeStatusDomainKnownIDs
	// NodeStatusDomainAggregate — AggregateStatus + CheckedAt (aggregate
	// status / version policy).
	NodeStatusDomainAggregate
)

// defaultCaptureRetention is the default TTL for stopped CaptureSessions —
// long enough that the user notices a "stop with error" toast before the
// entry is swept, short enough that finished sessions do not pile up across
// long uptimes. Used when NodeStatusMonitorOpts.CaptureRetention is zero.
const defaultCaptureRetention = 60 * time.Second

// NodeStatusMonitorOpts holds the dependencies for constructing a
// NodeStatusMonitor. EventBus and Client are required. OnChanged and
// OnPartialChanged are optional notification hooks; Clock and
// CaptureRetention receive defaults when left zero.
type NodeStatusMonitorOpts struct {
	EventBus  *ebus.Bus
	Client    *DesktopClient
	OnChanged func() // called after every status mutation (must not block)
	// OnPartialChanged is called after a single-domain mutation (resource
	// sample, traffic batch, route-table/identity/aggregate change) with the
	// domain that changed. Optional: when nil the monitor falls back to
	// OnChanged. Wiring it lets the subscriber patch just that field instead
	// of deep-copying the whole NodeStatus — the dominant allocator under a
	// status-event storm. Must not block (see DMRouter.NotifyStatusDomainChanged).
	OnPartialChanged func(NodeStatusDomain)
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
//   - Reachability tracking (TopicRouteTableChanged) and identity presence
//     transitions (TopicIdentityPresenceChanged)
//   - Contact management (TopicContactAdded, TopicContactRemoved)
//   - Identity registry (TopicIdentityAdded)
//   - ProbeNode merge logic (mergeNodeStatusLocked, mergePeerHealth,
//     mergeAggregateStatus)
type NodeStatusMonitor struct {
	eventBus         *ebus.Bus
	client           *DesktopClient
	onChanged        func()
	onPartialChanged func(NodeStatusDomain)

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

	// heldPresence keeps last-online observations whose identity had no
	// contact row when they arrived, keyed by address. The presence topics
	// and TopicContactAdded are independent subscriber goroutines, so a DM
	// from a contact being added right now can be applied first; the
	// contact-added handler claims the held value. Bounded by
	// maxHeldPresenceObservations — the identities are remote parties.
	// Guarded by mu, like status.
	heldPresence map[string]heldPresenceObservation

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
		onPartialChanged: opts.OnPartialChanged,
		clock:            clock,
		captureRetention: retention,
	}
}

// Start subscribes to all network-layer ebus events. Must be called
// before any events are published (typically before node.Start()).
func (m *NodeStatusMonitor) Start() {
	m.subscribeEvents()
}

// resourceSampleInterval is the cadence at which RunResourceSampler
// refreshes status.ResourceUsage. One second gives a live-ticking
// uptime / memory readout in the Info tab while keeping the
// stop-the-world runtime.MemStats read (performed node-side inside
// fetch_resource_usage) to once per second — controlled, off the UI
// render path, and independent of how often the window redraws.
const resourceSampleInterval = time.Second

// RunResourceSampler periodically samples the node's process memory +
// uptime through the fetch_resource_usage local frame and publishes it
// into status.ResourceUsage, firing onChanged() so subscribers redraw.
// Unlike peer health / aggregate status there is no ebus event that
// carries resource usage, so this dedicated ticker is what keeps the
// figure fresh between full probes — the resource-data analogue of the
// ebus deltas that drive the other status fields. Blocks until ctx is
// cancelled; intended to be launched as a goroutine from app startup
// (mirrors metrics.Collector.Run).
func (m *NodeStatusMonitor) RunResourceSampler(ctx context.Context) {
	ticker := time.NewTicker(resourceSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fetchCtx, cancel := context.WithTimeout(ctx, resourceSampleInterval)
			usage := m.client.FetchResourceUsage(fetchCtx)
			cancel()
			if usage == nil {
				continue
			}
			m.mu.Lock()
			m.status.ResourceUsage = usage
			m.mu.Unlock()
			m.notifyPartial(NodeStatusDomainResourceUsage)
		}
	}
}

// notifyPartial fires the single-domain subscriber when one is wired,
// otherwise falls back to the generic onChanged (full rebuild). The
// dedicated path lets subscribers patch just the changed field instead of
// deep-copying the entire NodeStatus — profiling flagged deepCopyNodeStatus
// as the dominant allocator under a status-event storm (resource sampler,
// traffic batches, route/identity/aggregate churn on a large mesh).
func (m *NodeStatusMonitor) notifyPartial(d NodeStatusDomain) {
	if m.onPartialChanged != nil {
		m.onPartialChanged(d)
		return
	}
	m.notifyChanged()
}

// notifyChanged safely emits a full-status notification. Tests and headless
// consumers may intentionally construct a monitor without a UI callback.
func (m *NodeStatusMonitor) notifyChanged() {
	if m.onChanged != nil {
		m.onChanged()
	}
}

// NodeStatus returns a deep copy of the current aggregated status.
func (m *NodeStatusMonitor) NodeStatus() NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deepCopyNodeStatus(m.status)
}

// ResourceUsageSnapshot returns an independent copy of just the
// ResourceUsage field (process memory + uptime), or nil if not yet
// sampled. Cheap counterpart to NodeStatus() for the once-per-second
// resource tick: the subscriber patches this single pointer instead of
// deep-copying every peer-health / route-derived collection. The clone
// keeps the returned value independent of monitor-owned memory, matching
// the deepCopyNodeStatus contract.
func (m *NodeStatusMonitor) ResourceUsageSnapshot() *ResourceUsage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status.ResourceUsage == nil {
		return nil
	}
	clone := *m.status.ResourceUsage
	return &clone
}

// PeerHealthSnapshot returns an independent copy of just the PeerHealth
// slice. Cheap counterpart to NodeStatus() for the periodic traffic batch:
// the subscriber patches this single slice on the cached snapshot instead
// of deep-copying KnownIDs / ReachableIDs / contacts / messages that the
// traffic update never touched. PeerHealth elements are value types
// (scalars + domain.OptionalTime), so the append-copy is fully independent
// of monitor-owned memory, matching the deepCopyNodeStatus contract.
func (m *NodeStatusMonitor) PeerHealthSnapshot() []PeerHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status.PeerHealth == nil {
		return nil
	}
	return append([]PeerHealth(nil), m.status.PeerHealth...)
}

// ReachableIDsSnapshot returns an independent copy of just the ReachableIDs
// map. Cheap counterpart to NodeStatus() for route-table changes: the
// subscriber patches this single map instead of deep-copying PeerHealth /
// KnownIDs / contacts / messages the route change never touched. The values
// are bools, so the per-entry copy is fully independent of monitor-owned
// memory, matching the deepCopyNodeStatus contract.
// PresenceSnapshot returns an independent copy of the per-contact presence.
//
// Nil is preserved rather than turned into an empty map: nil means the node has
// not answered, and an empty map would say "every contact is accounted for and
// none of them are here" — a claim nobody made.
// PresenceSnapshot returns the projection and the generation it came from,
// together, under one lock.
//
// Both values or neither: they are two halves of one answer, and a caller that
// copies only the set leaves the number describing a projection that is no
// longer there. The interface then holds a new set with an old — or zero —
// generation, and the next real update is refused as stale.
//
// That is exactly why there is no set-only accessor: it existed, two callers
// used it to patch a cached status, and the pair silently came apart.
func (m *NodeStatusMonitor) PresenceSnapshot() (domain.PresenceSet, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status.Presence.Clone(), m.status.PresenceGeneration
}

func (m *NodeStatusMonitor) ReachableIDsSnapshot() map[domain.PeerIdentity]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status.ReachableIDs == nil {
		return nil
	}
	clone := make(map[domain.PeerIdentity]bool, len(m.status.ReachableIDs))
	for k, v := range m.status.ReachableIDs {
		clone[k] = v
	}
	return clone
}

// KnownIDsSnapshot returns an independent copy of just the KnownIDs slice.
// Cheap counterpart to NodeStatus() for identity-added events: the
// subscriber patches this single slice instead of deep-copying PeerHealth /
// ReachableIDs / contacts / messages. Elements are strings (immutable), so
// the append-copy is fully independent of monitor-owned memory.
func (m *NodeStatusMonitor) KnownIDsSnapshot() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.status.KnownIDs == nil {
		return nil
	}
	return append([]string(nil), m.status.KnownIDs...)
}

// AggregateStatusSnapshot returns an independent clone of the AggregateStatus
// pointer plus the CheckedAt timestamp — the two fields the aggregate-status
// and version-policy events own. Cheap counterpart to NodeStatus() for those
// events. AggregateStatus is a pure value struct, so cloning the pointee
// keeps the result independent of monitor-owned memory.
func (m *NodeStatusMonitor) AggregateStatusSnapshot() (*AggregateStatus, time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var clone *AggregateStatus
	if m.status.AggregateStatus != nil {
		c := *m.status.AggregateStatus
		clone = &c
	}
	return clone, m.status.CheckedAt
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
	// Held observations belong to the identity that made them. Kept across a
	// reset they would be claimed by whatever contact the NEXT session
	// happens to add under the same address.
	m.heldPresence = nil
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
	// The probe is the other way a contact first appears in this snapshot, so
	// it claims held observations exactly like the contact-added handler.
	// Without this the startup race simply moved: an observation that landed
	// before the first probe would be held forever.
	m.claimHeldPresenceForKnownContactsLocked()
	m.mu.Unlock()

	m.notifyChanged()
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

		m.notifyPartial(NodeStatusDomainAggregate)
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

		m.notifyPartial(NodeStatusDomainAggregate)
	})

	// Routing table changed — rebuild ReachableIDs from the authoritative
	// routing snapshot, but ONLY on the snapshot-published reason. The
	// mutation-time reasons (direct peer add/remove, announcement, transit
	// invalidation) fire while the cached snapshot is still the previous
	// generation: rebuilding on them read stale data and, worse, could
	// overwrite a fresh set with a stale one when handlers interleave. The
	// snapshot reason is emitted strictly AFTER routingSnap.Store, so a
	// reconcile on it is fresh by construction; its worst-case latency is
	// the refresher cadence (~2.5 s), which the mutation events never
	// improved on anyway — they only pretended to.
	m.eventBus.Subscribe(ebus.TopicRouteTableChanged, func(summary ebus.RouteTableChange) {
		if summary.Reason != domain.RouteChangeSnapshot {
			return
		}
		fresh := m.client.BuildReachableIDs()
		m.mu.Lock()
		m.status.ReachableIDs = fresh
		m.mu.Unlock()
		m.notifyPartial(NodeStatusDomainReachableIDs)
	})

	// Identity presence — apply only the timestamp from the dedicated
	// identity-domain events. ReachableIDs is deliberately untouched: the
	// route-snapshot event above is its single writer, avoiding cross-topic
	// generation races between independent ebus subscriber goroutines.
	//
	// Two topics, one rule. They differ in what the node observed — a final
	// route lost, or a DM handed to us by its own sender — and not at all in
	// what the UI does with it, which is why they share this handler instead
	// of having the same monotone apply written twice.
	// Per-contact presence changed on the node.
	//
	// This is the ONLY writer of m.status.Presence outside the full-probe
	// merge, and that is deliberate. The first version also refreshed
	// presence from the route handler above, to keep both fields from one
	// routing generation — but the two handlers run on independent
	// subscriber goroutines and each fetched outside the lock, so an older
	// fetch could land after a newer one and stick until the next event. One
	// field, one writer is worth more here than two fields from one instant:
	// each field is internally consistent, and presence is what the contact
	// list actually reads.
	//
	// Nothing is lost by dropping the other fetch: the node publishes this
	// event whenever the projection changes, INCLUDING from its routing
	// refresher, so a route-driven presence change still arrives — it simply
	// arrives on its own event.
	//
	// The subscription is separate from the route one because most of what
	// moves presence is not a routing event at all: a proof arriving, a probe
	// timing out, a validity window expiring. On a node whose table is not
	// churning, none of those would otherwise reach the interface.
	m.eventBus.Subscribe(ebus.TopicContactPresenceUpdated, func(struct{}) {
		fresh, generation := m.client.BuildPresence()
		m.mu.Lock()
		applied := m.applyPresenceLocked(fresh, generation)
		m.mu.Unlock()
		if !applied {
			// Somebody else already applied this projection or a later one.
			// Repainting on it would be a no-op at best; announcing it would
			// tell subscribers something changed when nothing did.
			return
		}
		m.notifyPartial(NodeStatusDomainContactPresence)
	})

	m.eventBus.Subscribe(ebus.TopicIdentityPresenceChanged, func(change ebus.IdentityPresenceChange) {
		m.applyIdentityPresence(change, presenceFromRouting)
	})
	m.eventBus.Subscribe(ebus.TopicIdentityPresenceObserved, func(change ebus.IdentityPresenceChange) {
		m.applyIdentityPresence(change, presenceFromDirectMessage)
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
		lastOnlineAt := m.status.Contacts[c.Address.String()].LastOnlineAt
		// An observation that arrived before this contact existed is claimed
		// here: the presence handler holds one aside precisely because these
		// two topics are independent subscriber goroutines and can land in
		// either order.
		if held, ok := m.claimHeldPresenceLocked(c.Address.String()); ok {
			if !lastOnlineAt.Valid() || held.After(lastOnlineAt.Time()) {
				lastOnlineAt = domain.TimeOf(held)
			}
		}
		m.status.Contacts[c.Address.String()] = Contact{
			PubKey:       string(c.PubKey),
			BoxKey:       string(c.BoxKey),
			BoxSignature: string(c.BoxSig),
			LastOnlineAt: lastOnlineAt,
		}
		m.mu.Unlock()

		m.notifyChanged()
	})

	// Contact removed — delete from local map.
	m.eventBus.Subscribe(ebus.TopicContactRemoved, func(identity domain.PeerIdentity) {
		m.mu.Lock()
		delete(m.status.Contacts, identity.String())
		m.mu.Unlock()

		m.notifyChanged()
	})

	// New identity discovered — append to local list.
	m.eventBus.Subscribe(ebus.TopicIdentityAdded, func(identity domain.PeerIdentity) {
		address := identity.String()
		m.mu.Lock()
		found := false
		for _, id := range m.status.KnownIDs {
			if id == address {
				found = true
				break
			}
		}
		changed := !found
		if changed {
			m.status.KnownIDs = append(m.status.KnownIDs, address)
		}
		m.mu.Unlock()

		// Only notify when the identity was actually new — a duplicate add
		// must not fire a redundant snapshot rebuild.
		if changed {
			m.notifyPartial(NodeStatusDomainKnownIDs)
		}
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

// applyIdentityPresence records an identity-level presence observation on the
// cached contacts. Observations from another node sharing the same Bus are
// ignored, and an older one never moves a contact's timestamp backwards —
// events from independent subscriber goroutines arrive in no guaranteed
// order, so "latest received" is not "latest observed".
//
// Contacts are only updated, never created: the trust store decides who is a
// contact. An observation about an identity that is not in the map yet is
// therefore held aside rather than dropped, because the two topics run on
// independent subscriber goroutines and a DM can be applied before the
// contact-added event that introduces its sender. Dropping it used to be
// invisible and permanent: the contact would arrive with no timestamp, and
// with the periodic full probe gone nothing would ever fill it in.
func (m *NodeStatusMonitor) applyIdentityPresence(change ebus.IdentityPresenceChange, source presenceSource) {
	if change.Source != m.client.Address() || len(change.Identities) == 0 || change.ChangedAt.IsZero() {
		return
	}
	observedAt := change.ChangedAt.UTC()

	m.mu.Lock()
	applied := 0
	for _, identity := range change.Identities {
		address := identity.String()
		contact, ok := m.status.Contacts[address]
		if !ok {
			m.holdPresenceForUnknownContactLocked(address, observedAt, source)
			continue
		}
		if contact.LastOnlineAt.Valid() && !observedAt.After(contact.LastOnlineAt.Time()) {
			continue
		}
		contact.LastOnlineAt = domain.TimeOf(observedAt)
		m.status.Contacts[address] = contact
		applied++
	}
	m.mu.Unlock()

	if applied == 0 {
		return
	}
	m.notifyPartial(NodeStatusDomainPresence)
}

// maxHeldPresenceObservations bounds the hold-aside map. Observations are
// published for whoever sent us an authenticated DM, contact or not, so the
// map is fed by remote parties and cannot be allowed to grow with them. The
// cap is generous next to any real address book and small next to the memory
// a flood would need to matter.
const maxHeldPresenceObservations = 512

// presenceSource says which topic an observation came from, because the two
// differ in how likely they are ever to be claimed. The routing topic reports
// identities from the ROUTING TABLE — peers, most of which the user has never
// trusted — while the DM topic reports the sender of a message we accepted,
// which the node publishes only for a contact. When the hold is full, that
// difference decides who leaves.
type presenceSource uint8

const (
	// presenceFromRouting — a route transition. The identity may never be a
	// contact, so the hold may never be claimed.
	presenceFromRouting presenceSource = iota
	// presenceFromDirectMessage — an authenticated DM from a contact. The
	// contact-added event that claims this is already on its way.
	presenceFromDirectMessage
)

// heldPresenceObservation is one waiting observation and where it came from.
type heldPresenceObservation struct {
	at     time.Time
	source presenceSource
}

// holdPresenceForUnknownContactLocked remembers an observation whose identity
// has no contact row yet, so the contact-added handler can claim it. Caller
// holds m.mu.
func (m *NodeStatusMonitor) holdPresenceForUnknownContactLocked(address string, observedAt time.Time, source presenceSource) {
	if m.heldPresence == nil {
		m.heldPresence = make(map[string]heldPresenceObservation)
	}
	if current, ok := m.heldPresence[address]; ok {
		if !observedAt.After(current.at) {
			return
		}
		m.heldPresence[address] = heldPresenceObservation{at: observedAt, source: source}
		return
	}
	if len(m.heldPresence) >= maxHeldPresenceObservations {
		// A contact-added event that never comes would otherwise pin its slot
		// for the life of the process, so the cap is swept before it is
		// enforced: an entry only waits for the event that claims it.
		m.expireHeldPresenceLocked(observedAt)
	}
	if len(m.heldPresence) >= maxHeldPresenceObservations {
		// Evict rather than refuse the newcomer, and evict a routing-sourced
		// entry first. A churning mesh fills this map with identities no
		// contact-added event will ever claim; dropping the oldest entry
		// regardless of source would hand those churn entries the slots and
		// throw out the DM observation for the contact that genuinely is
		// being added — the case the hold exists for.
		m.evictHeldPresenceLocked()
	}
	m.heldPresence[address] = heldPresenceObservation{at: observedAt, source: source}
}

// evictHeldPresenceLocked drops one entry: the oldest routing-sourced
// observation if there is one, otherwise the oldest overall. Callers hold
// m.mu.
func (m *NodeStatusMonitor) evictHeldPresenceLocked() {
	var (
		victim      string
		victimEntry heldPresenceObservation
	)
	for address, entry := range m.heldPresence {
		if victim == "" {
			victim, victimEntry = address, entry
			continue
		}
		// A routing entry always loses to a DM entry; within one source the
		// older one loses.
		if victimEntry.source != entry.source {
			if entry.source == presenceFromRouting {
				victim, victimEntry = address, entry
			}
			continue
		}
		if entry.at.Before(victimEntry.at) {
			victim, victimEntry = address, entry
		}
	}
	if victim != "" {
		delete(m.heldPresence, victim)
	}
}

// heldPresenceTTL is how long an observation waits for the contact-added
// event or probe that claims it. The window only has to cover the gap between
// two subscriber goroutines; anything still waiting minutes later is waiting
// for an event that is not coming.
const heldPresenceTTL = 5 * time.Minute

// expireHeldPresenceLocked drops observations older than heldPresenceTTL,
// measured against the newest observation seen rather than the wall clock —
// the values are event timestamps, and tests drive them from an injected
// clock. Caller holds m.mu.
func (m *NodeStatusMonitor) expireHeldPresenceLocked(now time.Time) {
	for address, entry := range m.heldPresence {
		if now.Sub(entry.at) > heldPresenceTTL {
			delete(m.heldPresence, address)
		}
	}
}

// claimHeldPresenceLocked hands over the observation held for an identity, if
// any, and forgets it. Caller holds m.mu.
func (m *NodeStatusMonitor) claimHeldPresenceLocked(address string) (time.Time, bool) {
	entry, ok := m.heldPresence[address]
	if !ok {
		return time.Time{}, false
	}
	delete(m.heldPresence, address)
	return entry.at, true
}

// claimHeldPresenceForKnownContactsLocked applies every held observation whose
// contact now exists. Used by the probe path, which introduces contacts in
// bulk rather than one event at a time. Caller holds m.mu.
func (m *NodeStatusMonitor) claimHeldPresenceForKnownContactsLocked() {
	for address, entry := range m.heldPresence {
		contact, ok := m.status.Contacts[address]
		if !ok {
			continue
		}
		delete(m.heldPresence, address)
		if contact.LastOnlineAt.Valid() && !entry.at.After(contact.LastOnlineAt.Time()) {
			continue
		}
		contact.LastOnlineAt = domain.TimeOf(entry.at)
		m.status.Contacts[address] = contact
	}
}

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
			PeerID:              delta.PeerID.String(),
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
				PeerID:              delta.PeerID.String(),
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
	m.notifyChanged()
}

// applyHealthDeltaToRow writes delta fields into an existing PeerHealth row.
// Address-level fields (State, Connected, Score, timestamps…) are always
// written. Session-scoped fields (ConnID, Direction, ClientVersion,
// ClientBuild, ProtocolVersion) are written only when writeSession is true —
// this prevents a connect-path delta from writing stale session fields into
// rows where the caller hasn't determined the correct connection context.
func applyHealthDeltaToRow(p *PeerHealth, delta ebus.PeerHealthDelta, writeSession bool) {
	// PeerID: persistent identity, always backfilled.
	if p.PeerID == "" && !delta.PeerID.IsZero() {
		p.PeerID = delta.PeerID.String()
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
	changed := false
	for i := range m.status.PeerHealth {
		if m.status.PeerHealth[i].Address == addr {
			if m.status.PeerHealth[i].SlotState != slotState {
				m.status.PeerHealth[i].SlotState = slotState
				changed = true
			}
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
		changed = true
	}
	m.mu.Unlock()

	// Only wake the UI on a real change. A no-op delta (the same slot state
	// re-reported) must NOT trigger a status notify — that path deep-copies
	// the whole NodeStatus and an event storm of no-op deltas was a top
	// allocator / UI-freeze source in profiling. Mirrors applyTrafficBatch's
	// `updated` gate.
	if changed {
		m.notifyChanged()
	}
}

// applyPeerPendingDelta updates the PendingCount field in PeerHealth.
// Pending count is address-level (the pending queue is keyed by overlay
// address), so all per-ConnID rows for the address are updated.
func (m *NodeStatusMonitor) applyPeerPendingDelta(delta ebus.PeerPendingDelta) {
	m.mu.Lock()
	addr := string(delta.Address)
	found := false
	changed := false
	for i := range m.status.PeerHealth {
		if m.status.PeerHealth[i].Address == addr {
			if m.status.PeerHealth[i].PendingCount != delta.Count {
				m.status.PeerHealth[i].PendingCount = delta.Count
				changed = true
			}
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
		changed = true
	}
	m.mu.Unlock()

	// Only wake the UI on a real change. A no-op delta (the same pending count
	// re-reported, very common under route churn / drain cycles) must NOT
	// trigger a status notify — that path deep-copies the whole NodeStatus and
	// was the top allocator / UI-freeze source in profiling. Mirrors
	// applyTrafficBatch's `updated` gate.
	if changed {
		m.notifyChanged()
	}
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
		// Traffic batch touched only per-peer byte counters → PeerHealth
		// domain. This matters because the resource sampler's own loopback
		// RPC generates traffic deltas: without the lightweight path "measure
		// memory" indirectly re-triggers the full deepCopyNodeStatus every
		// couple of seconds via TopicPeerTrafficUpdated.
		m.notifyPartial(NodeStatusDomainPeerHealth)
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

	m.notifyChanged()
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

	m.notifyChanged()
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
	m.status.ProtocolVersion = s.ProtocolVersion
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
	// ResourceUsage is sampled by the resource ticker (RunResourceSampler)
	// and seeded by the probe. Preserve the previous non-nil value when a
	// probe transiently omits it so the Info-tab rows do not flicker out.
	if s.ResourceUsage != nil {
		m.status.ResourceUsage = s.ResourceUsage
	}

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
	// Presence does NOT merge per key, and that is the correction of a real
	// bug rather than a simplification.
	//
	// The projection is a WHOLE-SET answer: every pass of the node covers every
	// contact. So the event handler and this probe never hold complementary
	// halves — they hold the same picture read at two moments, and the only
	// question is which moment is later. The previous rule ("events win per
	// key, they are newer by construction") assumed an ordering that does not
	// exist: both readers fetch the snapshot pointer independently, and a probe
	// that read it a moment later could be overwritten by an event that read it
	// earlier. On a node starting up that showed a stale status, and if the
	// next best-effort event was dropped it stayed stale until the one-minute
	// heartbeat.
	//
	// Applying the LATER of the two also keeps what the per-key merge was
	// protecting: attaching to an already-running node still gets the probe's
	// full projection, because a monitor that has seen no event has generation
	// zero and anything beats it.
	m.applyPresenceLocked(s.Presence, s.PresenceGeneration)
	m.status.CaptureSessions = mergeCaptureSessions(m.status.CaptureSessions, s.CaptureSessions)
}

// applyPresenceLocked stores a projection only if it is later than the one
// already held, and reports whether it did.
//
// A zero generation is refused outright: it means the node has not projected
// yet, which is "nothing is known" and not "an empty projection". Overwriting a
// real set with it would turn silence into a claim that nobody is present.
//
// Equal generations are refused too — the two readers fetched the same
// projection, so there is nothing to apply and nothing to announce.
//
// Caller must hold m.mu.
func (m *NodeStatusMonitor) applyPresenceLocked(set domain.PresenceSet, generation uint64) bool {
	if generation == 0 || generation <= m.status.PresenceGeneration {
		return false
	}
	// Both halves move together, and there is only ONE copy of the generation.
	// A private field beside the published one is two places to forget, and
	// NodeStatus would then hand out a set carrying somebody else's number.
	m.status.Presence = set
	m.status.PresenceGeneration = generation
	return true
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

// mergeContacts merges probe contacts into the ebus-driven map. Ebus key
// material takes precedence on conflicts, while LastOnlineAt is durable node
// state and merges monotonically by timestamp so a dropped ebus notification
// can be repaired by a later probe.
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
	for address, ebusContact := range ebusCt {
		probeContact, exists := probeCt[address]
		if exists {
			// Observations accumulate: each is a fact that happened, so the
			// newest of the two wins and neither side can erase the other's.
			ebusContact.LastOnlineAt = newerOptional(ebusContact.LastOnlineAt, probeContact.LastOnlineAt)
		}
		merged[address] = ebusContact
	}
	return merged
}

// newerOptional returns whichever of the two optional timestamps is later,
// treating "unset" as older than any value.
func newerOptional(current, candidate domain.OptionalTime) domain.OptionalTime {
	if !candidate.Valid() {
		return current
	}
	if !current.Valid() || candidate.Time().After(current.Time()) {
		return candidate
	}
	return current
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
