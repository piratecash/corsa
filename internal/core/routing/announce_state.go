package routing

import (
	"sync"
	"time"
)

// AnnouncePeerState holds per-peer announce send state. Owned by the
// announce state registry (which itself is owned by node.Service or the
// announce loop orchestrator). The routing.Table does not own or mutate
// this state.
//
// All timestamps are initialized to zero value on creation. This ensures
// that rate limits do not block the first forced full sync and that
// periodic full-sync checks immediately qualify a new peer.
//
// Thread safety: all mutable fields are accessed exclusively through
// methods that acquire the embedded mutex. Direct field access from
// outside the package is forbidden.
type AnnouncePeerState struct {
	mu sync.Mutex

	peerIdentity PeerIdentity

	// lastSentSnapshot is the last successfully sent canonical snapshot.
	// nil means no snapshot has been sent yet (empty state). Delta
	// computation against nil is forbidden — forced full sync is required.
	lastSentSnapshot *AnnounceSnapshot

	// needsFullResync indicates that the next send must be a forced full
	// sync regardless of delta state.
	needsFullResync bool

	// lastSuccessfulFullSyncAt is the timestamp of the last successful
	// full sync send.
	lastSuccessfulFullSyncAt time.Time

	// lastFullSyncAttemptAt is the timestamp of the last forced full sync
	// attempt, regardless of success.
	lastFullSyncAttemptAt time.Time

	// lastDeltaSendAt is the timestamp of the last successful delta send.
	lastDeltaSendAt time.Time

	// disconnectedAt records when the peer disconnected. Used for eviction
	// timing. Zero means the peer is connected or state was just created.
	disconnectedAt time.Time

	// capabilities is the peer's negotiated capability set as a derived
	// view of the session AnnounceLoop selected as the per-cycle target.
	// Reconciled on every cycle in announceToAllPeers via UpdateCapabilities
	// (passing the AnnounceTarget.Capabilities returned by peersFn) — this
	// is the single source of truth, not a lifecycle-hook snapshot.
	// MarkReconnected only seeds the initial value when a fresh state
	// record is materialised; after that, every cycle's per-peer goroutine
	// overwrites the slice with the target caps so classifyDeltaMode below
	// reads two agreeing snapshots. The slice is always stored as a
	// defensive copy so the session-owned source can mutate without
	// leaking in.
	//
	// IMPORTANT: this is not a session-stable lifecycle record. Two
	// successive cycles can rewrite the field with caps from different
	// sessions if peersFn picks different sessions on each cycle (map
	// iteration order). Callers reading capabilities outside the cycle
	// goroutine see whatever the most recent cycle observed at its target;
	// nothing pins the value to a particular session boundary. The only
	// reliable production reader is classifyDeltaMode inside the same
	// per-peer goroutine that just synced the field.
	capabilities []PeerCapability

	// hasReceivedBaseline records whether this session has received the
	// legacy announce_routes first-sync frame from the peer yet. Reset on
	// every session boundary (MarkReconnected / MarkDisconnected) because a
	// fresh session has no baseline regardless of whether the identity was
	// known before.
	//
	// Read by the v2 routes_update receive path: if a peer sends a
	// routes_update delta before any baseline arrived, the local state is
	// desynced with respect to the peer's outgoing cache. The receiver
	// replies with a request_resync wire frame and drops the delta — the
	// peer's next cycle picks up the forced full via MarkInvalid on their
	// side. Write via MarkBaselineReceived, read via HasReceivedBaseline.
	hasReceivedBaseline bool

	// wireBaselineSentToPeer is the send-side mirror of hasReceivedBaseline:
	// whether THIS node has put any legacy announce_routes frame on the wire
	// to the peer in the current session. Reset on every session boundary
	// (MarkReconnected / MarkDisconnected); flipped by MarkWireBaselineSent
	// after a successful SendAnnounceRoutes call.
	//
	// Read by the v2 mode selection in AnnounceLoop.announceToAllPeers: the
	// v2 routes_update wire frame is gated on a previously transmitted
	// legacy baseline. An empty connect-time / forced-full snapshot records
	// a successful baseline locally without sending any wire frame, so the
	// flag stays false; the first subsequent non-empty delta is therefore
	// downgraded to legacy SendAnnounceRoutes regardless of v2 capability,
	// preserving the symmetric invariant on the peer side (the peer would
	// otherwise drop the v2 frame and emit request_resync). Once a real
	// announce_routes frame goes out — connect-time non-empty, forced-full
	// non-empty, or any legacy delta — the flag flips and v2 deltas are
	// permitted on subsequent cycles.
	wireBaselineSentToPeer bool
}

// PeerIdentity returns the immutable identity of this peer state.
func (s *AnnouncePeerState) PeerIdentity() PeerIdentity {
	// Immutable after creation — no lock needed.
	return s.peerIdentity
}

// announcePeerStateView is a read-only snapshot of AnnouncePeerState fields
// needed by the announce loop to make send decisions. Captured under a
// single lock acquisition to prevent torn reads.
//
// CapabilitiesSnapshot is included in the view so the announce loop can
// derive wire-format mode (v1 vs v2) without taking a second lock. The
// slice is a defensive copy; callers MUST treat it as immutable.
// HasReceivedBaseline is included because receive-side gating decisions
// (accept / request_resync) rely on the same atomic snapshot that
// produced the other fields.
type announcePeerStateView struct {
	NeedsFullResync          bool
	LastSentSnapshot         *AnnounceSnapshot
	LastSuccessfulFullSyncAt time.Time
	LastFullSyncAttemptAt    time.Time
	CapabilitiesSnapshot     []PeerCapability
	HasReceivedBaseline      bool
	HasSentWireBaseline      bool
}

// View returns a consistent read-only snapshot of the state fields needed
// for announce decision-making. All fields are captured under the lock.
func (s *AnnouncePeerState) View() announcePeerStateView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return announcePeerStateView{
		NeedsFullResync:          s.needsFullResync,
		LastSentSnapshot:         s.lastSentSnapshot,
		LastSuccessfulFullSyncAt: s.lastSuccessfulFullSyncAt,
		LastFullSyncAttemptAt:    s.lastFullSyncAttemptAt,
		CapabilitiesSnapshot:     copyCapabilities(s.capabilities),
		HasReceivedBaseline:      s.hasReceivedBaseline,
		HasSentWireBaseline:      s.wireBaselineSentToPeer,
	}
}

// RecordFullSyncSuccess updates the state after a successful full sync.
func (s *AnnouncePeerState) RecordFullSyncSuccess(snapshot *AnnounceSnapshot, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSentSnapshot = snapshot
	s.needsFullResync = false
	s.lastSuccessfulFullSyncAt = now
}

// RecordFullSyncAttempt records the timestamp of a forced full sync attempt
// (regardless of success). Used for rate limiting.
func (s *AnnouncePeerState) RecordFullSyncAttempt(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFullSyncAttemptAt = now
}

// RecordDeltaSendSuccess updates the state after a successful delta send.
// The full snapshot (not just the delta) is stored as the new baseline.
func (s *AnnouncePeerState) RecordDeltaSendSuccess(fullSnapshot *AnnounceSnapshot, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSentSnapshot = fullSnapshot
	s.lastDeltaSendAt = now
}

// HasCapability returns true when the peer's stored capability snapshot
// contains the given capability. The snapshot is reconciled on every
// announce cycle from the chosen AnnounceTarget.Capabilities (see the
// capabilities-field doc on AnnouncePeerState), so a value read from
// outside the per-peer goroutine reflects whatever the most recent cycle
// observed at its target — it is NOT a session-stable lifecycle record
// and may change between calls without any visible session event. Lookup
// is O(len), acceptable at ≤4 capabilities per peer.
func (s *AnnouncePeerState) HasCapability(capability PeerCapability) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

// CapabilitiesSnapshot returns a defensive copy of the peer's stored
// capability set. The stored snapshot is reconciled by AnnounceLoop on
// every cycle from the AnnounceTarget caps (see the capabilities-field
// doc on AnnouncePeerState); callers reading this from outside the
// per-peer goroutine see whatever the most recent cycle observed at its
// target. Returns nil when no capabilities were captured —
// distinguishable from an empty non-nil slice at the consumer side.
func (s *AnnouncePeerState) CapabilitiesSnapshot() []PeerCapability {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyCapabilities(s.capabilities)
}

// MarkBaselineReceived records that the peer has delivered a legacy
// announce_routes first-sync frame in this session. Subsequent
// routes_update frames from the peer are now safe to apply against the
// known-good baseline. See AnnouncePeerState.hasReceivedBaseline for the
// session-boundary reset contract.
func (s *AnnouncePeerState) MarkBaselineReceived() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hasReceivedBaseline = true
}

// HasReceivedBaseline reports whether the current session has received
// the legacy announce_routes first-sync frame from the peer. Used by the
// v2 routes_update receive path to gate delta application: no baseline
// means the local state is desynced and the receiver must request a
// forced resync instead of silently accepting the delta.
func (s *AnnouncePeerState) HasReceivedBaseline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasReceivedBaseline
}

// MarkWireBaselineSent records that a legacy announce_routes wire frame
// has gone out to the peer in this session. Symmetric counterpart of
// MarkBaselineReceived on the receive side. Callers must flip this flag
// only after PeerSender.SendAnnounceRoutes returned true — never on the
// empty-snapshot short-circuit, which records a baseline locally but
// emits no wire frame and therefore does not establish the v2 receive
// gate on the peer.
func (s *AnnouncePeerState) MarkWireBaselineSent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wireBaselineSentToPeer = true
}

// HasSentWireBaseline reports whether THIS node has emitted a legacy
// announce_routes frame to the peer in the current session. Read by the
// announce loop's v2 mode selection: a v2 routes_update may be sent only
// after the peer has actually observed a baseline frame on the wire, so
// when this flag is false the next delta — even if both capability
// sources agree on v2 — is sent via legacy SendAnnounceRoutes to first
// establish the wire baseline.
func (s *AnnouncePeerState) HasSentWireBaseline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wireBaselineSentToPeer
}

// AnnounceStateRegistry manages per-peer AnnouncePeerState instances.
// Thread-safe for concurrent access from the announce loop and session
// lifecycle callbacks.
type AnnounceStateRegistry struct {
	mu    sync.Mutex
	peers map[PeerIdentity]*AnnouncePeerState

	// flapWindow is used for eviction timing: disconnected state is
	// evicted after 2 * flapWindow.
	flapWindow time.Duration

	// clock is the time source, injectable for tests.
	clock func() time.Time
}

// AnnounceStateRegistryOption configures the registry.
type AnnounceStateRegistryOption func(*AnnounceStateRegistry)

// WithRegistryClock overrides the time source for the registry.
func WithRegistryClock(clock func() time.Time) AnnounceStateRegistryOption {
	return func(r *AnnounceStateRegistry) {
		r.clock = clock
	}
}

// WithRegistryFlapWindow overrides the flap window used for eviction.
func WithRegistryFlapWindow(d time.Duration) AnnounceStateRegistryOption {
	return func(r *AnnounceStateRegistry) {
		r.flapWindow = d
	}
}

// NewAnnounceStateRegistry creates a new registry with default settings.
func NewAnnounceStateRegistry(opts ...AnnounceStateRegistryOption) *AnnounceStateRegistry {
	r := &AnnounceStateRegistry{
		peers:      make(map[PeerIdentity]*AnnouncePeerState),
		flapWindow: DefaultFlapWindow,
		clock:      time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Clock returns the current time from the registry's clock source.
// Exposed so that external callers (e.g. node.Service full-sync helpers)
// can use the same clock as the announce state lifecycle.
func (r *AnnounceStateRegistry) Clock() time.Time {
	return r.clock()
}

// GetOrCreate returns the existing state for a peer, or creates a new one
// in needsFullResync=true state. Used when a routing-capable session is
// established.
func (r *AnnounceStateRegistry) GetOrCreate(peerID PeerIdentity) *AnnouncePeerState {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.peers[peerID]; ok {
		return s
	}

	s := &AnnouncePeerState{
		peerIdentity:    peerID,
		needsFullResync: true,
		// All timestamps zero — see type doc.
	}
	r.peers[peerID] = s
	return s
}

// Get returns the state for a peer, or nil if not found.
func (r *AnnounceStateRegistry) Get(peerID PeerIdentity) *AnnouncePeerState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peers[peerID]
}

// MarkDisconnected sets the peer to needsFullResync and records the
// disconnect timestamp for eviction. Called on peer session close.
// The rate limit timer is reset so that the first announce cycle after
// reconnect is not throttled. The baseline-received flag is cleared
// because a new session starts with no baseline regardless of whether
// the peer identity was previously known — see
// AnnouncePeerState.hasReceivedBaseline for the session-boundary contract.
func (r *AnnounceStateRegistry) MarkDisconnected(peerID PeerIdentity) {
	r.mu.Lock()
	s, ok := r.peers[peerID]
	r.mu.Unlock()

	if !ok {
		return
	}

	now := r.clock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.needsFullResync = true
	s.disconnectedAt = now
	// Reset rate limit timer — reconnect resets forced-full rate limit.
	s.lastFullSyncAttemptAt = time.Time{}
	// A new session must re-deliver the baseline before any v2 routes_update
	// delta is accepted. Leaving the flag set here would let a stale
	// baseline from a previous session unlock delta application on the
	// next session after reconnect.
	s.hasReceivedBaseline = false
	// Symmetric reset of the send-side wire-baseline flag: a fresh session
	// has no observable baseline on the wire even if the previous session
	// emitted one. Without this reset, the first delta in the new session
	// could pick the v2 wire frame against a peer that has never received
	// announce_routes in that session.
	s.wireBaselineSentToPeer = false
}

// MarkReconnected resets a previously disconnected peer for a new announce
// session. Always requires forced full sync. Rate limit timer is reset.
//
// caps is the peer's negotiated capability set at the moment the session
// was established. MarkReconnected stores a defensive copy so downstream
// mutation of the caller's slice cannot leak into AnnouncePeerState — the
// hook boundary passes a session-owned slice whose lifetime extends past
// this call. An empty or nil caps slice is valid and means "peer advertised
// no capabilities" (legacy node).
func (r *AnnounceStateRegistry) MarkReconnected(peerID PeerIdentity, caps []PeerCapability) {
	capsCopy := copyCapabilities(caps)

	r.mu.Lock()
	s, ok := r.peers[peerID]
	if !ok {
		// Create fresh state. Both hasReceivedBaseline and
		// wireBaselineSentToPeer stay at their zero values (false) — the
		// new session has neither received nor emitted any announce_routes
		// yet, so both directions of the v2 baseline gate start closed.
		r.peers[peerID] = &AnnouncePeerState{
			peerIdentity:    peerID,
			needsFullResync: true,
			capabilities:    capsCopy,
		}
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.needsFullResync = true
	s.disconnectedAt = time.Time{}
	s.lastFullSyncAttemptAt = time.Time{}
	s.capabilities = capsCopy
	// Reset the baseline flag for the new session. MarkDisconnected already
	// clears it on session close, but MarkReconnected must also be
	// idempotent: a peer that reconnects without an intervening disconnect
	// hook (e.g. failed teardown path) must not inherit the previous
	// session's baseline.
	s.hasReceivedBaseline = false
	// Symmetric idempotent reset of the send-side wire-baseline flag — same
	// rationale as hasReceivedBaseline above: an overlapping reconnect that
	// skipped MarkDisconnected must not inherit the prior session's wire
	// baseline state, which would unlock v2 delta sends without an
	// observable baseline frame in the new session.
	s.wireBaselineSentToPeer = false
}

// copyCapabilities returns an independent slice with the same contents so
// that callers can keep mutating their own capability snapshot without
// affecting stored AnnouncePeerState. Nil input maps to nil output to
// preserve the "capabilities not advertised" signal documented on the
// AnnouncePeerState.capabilities field.
func copyCapabilities(caps []PeerCapability) []PeerCapability {
	if caps == nil {
		return nil
	}
	out := make([]PeerCapability, len(caps))
	copy(out, caps)
	return out
}

// EvictStale removes state for peers that have been disconnected longer
// than 2 * flapWindow. Returns the count of evicted entries.
func (r *AnnounceStateRegistry) EvictStale() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.clock()
	evictAfter := 2 * r.flapWindow
	evicted := 0

	for id, s := range r.peers {
		s.mu.Lock()
		disc := s.disconnectedAt
		s.mu.Unlock()

		if disc.IsZero() {
			continue
		}
		if now.Sub(disc) > evictAfter {
			delete(r.peers, id)
			evicted++
		}
	}
	return evicted
}

// UpdateCapabilities replaces the persistent capability snapshot for a
// peer without touching any other state field. Called by AnnounceLoop
// at the start of each per-peer goroutine in announceToAllPeers to
// reconcile the persistent snapshot with the AnnounceTarget caps that
// peersFn (in production: node.routingCapablePeers) actually picked
// this cycle.
//
// Why a cycle-time reconciliation is the single source of truth: the
// only production reader of AnnouncePeerState.capabilities is
// classifyDeltaMode inside the same per-peer goroutine. peersFn dedupes
// overlapping sessions for one identity by map iteration order, so
// successive cycles can pick different sessions when several
// routing-capable sessions for the same peer have different negotiated
// caps; a partial close (a routing-capable session that previously set
// the snapshot disappears while an older routing-capable session
// remains) leaves the persistent caps describing a session that is no
// longer the chosen target. No lifecycle-hook variant can match the
// cycle's selection rule reliably without re-implementing the same
// dedup logic — and even then, it would still race the next cycle.
// Reconciling at cycle start makes the persistent snapshot a derived
// view of the same session this cycle picked.
//
// UpdateCapabilities is intentionally narrow: it does NOT reset
// hasReceivedBaseline, wireBaselineSentToPeer, or needsFullResync. Caps
// are an identity-level property reflected from active sessions; they
// must not influence baseline-gate state, which tracks wire-history
// with the peer identity in the current session window. The
// needsFullResync flag is left alone for the same reason — caps refresh
// is metadata-only, not a protocol-level resync trigger.
//
// Call is a no-op when no state record exists for peerID. The call is
// idempotent: passing the same caps twice produces no observable change.
func (r *AnnounceStateRegistry) UpdateCapabilities(peerID PeerIdentity, caps []PeerCapability) {
	capsCopy := copyCapabilities(caps)

	r.mu.Lock()
	s, ok := r.peers[peerID]
	r.mu.Unlock()

	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.capabilities = capsCopy
}

// MarkInvalid marks the peer state as requiring full resync due to
// state loss or any consistency concern. Fallback always goes toward
// full sync.
func (r *AnnounceStateRegistry) MarkInvalid(peerID PeerIdentity) {
	r.mu.Lock()
	s, ok := r.peers[peerID]
	r.mu.Unlock()

	if ok {
		s.mu.Lock()
		s.needsFullResync = true
		s.mu.Unlock()
	}
}

// Cleanup removes all state. Used on owner shutdown.
func (r *AnnounceStateRegistry) Cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = make(map[PeerIdentity]*AnnouncePeerState)
}
