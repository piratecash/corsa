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

	// resyncIsHard distinguishes WHY needsFullResync is set, which the
	// Phase 3 digest-suppression gate needs to decide whether the forced
	// full sync may be elided on a digest match:
	//
	//   - false (soft): the resync was triggered by a session boundary
	//     (MarkReconnected / MarkDisconnected). A reconnecting pair that
	//     agrees on its mutual routing view via the route_sync digest
	//     exchange MAY skip this full sync — that is the entire point of
	//     Phase 3 incremental sync (docs/protocol/route_sync.md).
	//   - true (hard): the resync was demanded by an explicit
	//     request_resync or a consistency-loss signal (MarkInvalid). The
	//     peer is asking for a fresh full table; a digest match must NOT
	//     suppress it.
	//
	// Only meaningful while needsFullResync is true; cleared together with
	// needsFullResync on RecordFullSyncSuccess. A brand-new peer
	// (lastSentSnapshot == nil) is non-suppressible regardless of this
	// flag because the suppression gate also requires a prior baseline.
	resyncIsHard bool

	// lastSuccessfulFullSyncAt is the timestamp of the last successful
	// full sync send.
	lastSuccessfulFullSyncAt time.Time

	// lastFullSyncAttemptAt is the timestamp of the last forced full sync
	// attempt, regardless of success.
	lastFullSyncAttemptAt time.Time

	// lastDeltaSendAt is the timestamp of the last successful delta send.
	lastDeltaSendAt time.Time

	// lastSeenLiveAt is the last time this peer appeared in the announce
	// loop's authoritative live routing-capable set. It is seeded at
	// creation and refreshed every cycle by ReconcileLiveSet. Eviction is
	// driven solely by this watermark: a state that stops appearing in the
	// live set is reclaimed after 2*flapWindow regardless of whether
	// MarkDisconnected ever ran. This removes the old, implicit dependency
	// where eviction needed a disconnectedAt stamp that only the
	// session-close hook wrote — a skipped or asymmetric teardown could
	// then retain per-peer state (including its lastSentSnapshot) forever.
	lastSeenLiveAt time.Time

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

	// hasReceivedBaseline records whether this session has received a
	// self-contained baseline frame from the peer yet — either legacy
	// announce_routes (v1/v2 wire) or route_announce_v3 with kind="full"
	// (Phase 4 v3 wire). Reset on every session boundary (MarkReconnected
	// / MarkDisconnected) because a fresh session has no baseline
	// regardless of whether the identity was known before.
	//
	// Read by both delta receive paths: if a peer sends a delta frame
	// (v2 routes_update or v3 kind="delta") before any baseline arrived,
	// the local state is desynced with respect to the peer's outgoing
	// cache. The receiver replies with a request_resync wire frame and
	// drops the delta — the peer's next cycle picks up the forced full
	// via MarkInvalid on their side. Write via MarkBaselineReceived,
	// read via HasReceivedBaseline.
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

	// knownV3Epoch is the highest route_announce_v3 epoch observed from
	// this peer in the current session; knownV3EpochSet distinguishes
	// "no v3 frame seen yet" (false) from "epoch 0 observed" (true). The
	// epoch is the sender's local table-generation counter: it increments
	// when the peer resets its routing table (e.g. process restart). The
	// v3 receive path (handleRouteAnnounceV3) reads these to detect a
	// stale-process replay (incoming epoch < known → ignore the frame) vs
	// a fresh table that invalidates our diff baseline (incoming epoch >
	// known → force a fresh full resync from the peer). Reset on every
	// session boundary (MarkDisconnected / MarkReconnected) because epoch
	// counters are per-process and a new session may front a restarted
	// peer whose epoch is unrelated to the previous session's value.
	knownV3Epoch    uint64
	knownV3EpochSet bool

	// wireBaselineV3SentToPeer is the v3-generation analogue of
	// wireBaselineSentToPeer: whether THIS node has emitted a
	// route_announce_v3 frame with kind="full" to the peer in the
	// current session. Reset on every session boundary
	// (MarkDisconnected / MarkReconnected) for the same reason the
	// legacy flag is: a fresh session has no observable v3 baseline on
	// the wire even if a previous session emitted one. Flipped only
	// after a real successful SendRouteAnnounceV3 call carrying a
	// non-empty kind="full" frame — empty-snapshot short-circuits
	// record the baseline locally without emitting anything, so the
	// flag stays false and prevents a subsequent v3 kind="delta" from
	// dispatching against a peer that never observed a v3 full.
	//
	// Read by the v3 mode selection in announceToAllPeers: a v3
	// kind="delta" frame is gated on a previously transmitted v3
	// kind="full" so the receiver's baseline gate
	// (handleRouteAnnounceV3) accepts it. The legacy
	// wireBaselineSentToPeer remains the v2-receive-gate signal and is
	// orthogonal — a session that picks the v3 generation never
	// touches the legacy flag, and vice versa.
	wireBaselineV3SentToPeer bool
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
	ResyncIsHard             bool
	LastSentSnapshot         *AnnounceSnapshot
	LastSuccessfulFullSyncAt time.Time
	LastFullSyncAttemptAt    time.Time
	CapabilitiesSnapshot     []PeerCapability
	HasReceivedBaseline      bool
	HasSentWireBaseline      bool
	// HasSentWireBaselineV3 is the v3-baseline analogue of
	// HasSentWireBaseline — see AnnouncePeerState.wireBaselineV3SentToPeer
	// for the gating contract. Read by the Phase 4 mode selection in
	// announceToAllPeers to decide whether a v3 kind="delta" is safe.
	HasSentWireBaselineV3 bool
}

// View returns a consistent read-only snapshot of the state fields needed
// for announce decision-making. All fields are captured under the lock.
func (s *AnnouncePeerState) View() announcePeerStateView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return announcePeerStateView{
		NeedsFullResync:          s.needsFullResync,
		ResyncIsHard:             s.resyncIsHard,
		LastSentSnapshot:         s.lastSentSnapshot,
		LastSuccessfulFullSyncAt: s.lastSuccessfulFullSyncAt,
		LastFullSyncAttemptAt:    s.lastFullSyncAttemptAt,
		CapabilitiesSnapshot:     copyCapabilities(s.capabilities),
		HasReceivedBaseline:      s.hasReceivedBaseline,
		HasSentWireBaseline:      s.wireBaselineSentToPeer,
		HasSentWireBaselineV3:    s.wireBaselineV3SentToPeer,
	}
}

// RecordFullSyncSuccess updates the state after a successful full sync.
func (s *AnnouncePeerState) RecordFullSyncSuccess(snapshot *AnnounceSnapshot, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSentSnapshot = snapshot
	s.needsFullResync = false
	s.resyncIsHard = false
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

// MarkBaselineReceived records that the peer has delivered a
// self-contained baseline frame in this session — either legacy
// announce_routes (v1/v2 wire) or route_announce_v3 with kind="full"
// (Phase 4 v3 wire). Subsequent delta frames from the peer (v2
// routes_update or v3 kind="delta") are now safe to apply against the
// known-good baseline. See AnnouncePeerState.hasReceivedBaseline for
// the session-boundary reset contract.
func (s *AnnouncePeerState) MarkBaselineReceived() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hasReceivedBaseline = true
}

// HasReceivedBaseline reports whether the current session has received
// a self-contained baseline frame from the peer (legacy announce_routes
// OR route_announce_v3 kind="full"). Used by the delta receive paths
// (v2 routes_update and v3 kind="delta") to gate delta application: no
// baseline means the local state is desynced and the receiver must
// request a forced resync instead of silently accepting the delta.
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

// V3EpochVerdict classifies an incoming route_announce_v3 epoch against the
// highest epoch previously observed from the peer this session. The
// decision logic lives in the node receive handler; AnnouncePeerState only
// stores the watermark and reports the comparison so the gating policy and
// the storage are testable in isolation.
type V3EpochVerdict int

const (
	// V3EpochApply means the incoming epoch equals the known watermark (or
	// this is the first v3 frame this session): apply the frame normally.
	V3EpochApply V3EpochVerdict = iota
	// V3EpochStale means the incoming epoch is below the known watermark —
	// a replay from an older peer process. The frame must be ignored; the
	// watermark is left untouched.
	V3EpochStale
	// V3EpochReset means the incoming epoch is above the known watermark —
	// the peer reset its table (e.g. restart), so any diff baseline we hold
	// for it is invalid. The watermark is advanced and the caller must force
	// a fresh full resync from the peer before trusting deltas.
	V3EpochReset
)

// ObserveV3Epoch compares incoming against the per-peer v3 epoch watermark
// and advances it on first-sight or increase. It returns the verdict the
// receive handler acts on. A stale epoch leaves the watermark unchanged so
// a single late replay cannot rewind it.
func (s *AnnouncePeerState) ObserveV3Epoch(incoming uint64) V3EpochVerdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.knownV3EpochSet {
		s.knownV3Epoch = incoming
		s.knownV3EpochSet = true
		return V3EpochApply
	}
	switch {
	case incoming < s.knownV3Epoch:
		return V3EpochStale
	case incoming > s.knownV3Epoch:
		s.knownV3Epoch = incoming
		return V3EpochReset
	default:
		return V3EpochApply
	}
}

// KnownV3Epoch reports the highest v3 epoch observed from the peer this
// session and whether any v3 frame has been seen at all. Exposed for tests
// and diagnostics.
func (s *AnnouncePeerState) KnownV3Epoch() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.knownV3Epoch, s.knownV3EpochSet
}

// MarkWireBaselineV3Sent records that a non-empty route_announce_v3
// kind="full" frame has gone out to the peer in the current session.
// Symmetric counterpart of MarkWireBaselineSent on the legacy/v2 path,
// flipped by SendRouteAnnounceV3 in the announce loop / connect-time
// sync after a successful send. Callers MUST flip this flag only after
// a real wire emit — empty-snapshot short-circuits record the baseline
// locally without emitting anything and so must leave the flag false.
func (s *AnnouncePeerState) MarkWireBaselineV3Sent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wireBaselineV3SentToPeer = true
}

// HasSentWireBaselineV3 reports whether THIS node has emitted a v3
// kind="full" frame to the peer in the current session. Read by the
// announce loop's v3 mode selection: a v3 kind="delta" wire frame is
// gated on a previously transmitted v3 kind="full" so the receiver's
// baseline gate (handleRouteAnnounceV3) accepts it; until this flag
// flips, even a v3-cap pair stays on the legacy / v2 wire path so the
// peer's first v3 frame is always a self-contained baseline.
func (s *AnnouncePeerState) HasSentWireBaselineV3() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wireBaselineV3SentToPeer
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
		// Seed the liveness watermark so a freshly created state is not
		// reclaimed by ReconcileLiveSet before the first cycle observes
		// the peer in the live set.
		lastSeenLiveAt: r.clock(),
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

// MarkDisconnected marks a peer's session boundary on session close: it
// forces a full resync for the next session and resets the per-session
// baseline gates and the forced-full rate-limit timer so the first announce
// cycle after reconnect is not throttled. The baseline-received flag is
// cleared because a new session starts with no baseline regardless of
// whether the peer identity was previously known — see
// AnnouncePeerState.hasReceivedBaseline for the session-boundary contract.
//
// Eviction is NOT tied to this call. ReconcileLiveSet reclaims state purely
// from the live set, so a skipped MarkDisconnected (asymmetric teardown,
// relay-gate drift) can no longer leak per-peer state — at worst the stale
// baseline gates are re-cleared by the next MarkReconnected, which is
// already idempotent for exactly this reason.
func (r *AnnounceStateRegistry) MarkDisconnected(peerID PeerIdentity) {
	r.mu.Lock()
	s, ok := r.peers[peerID]
	r.mu.Unlock()

	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.needsFullResync = true
	// Session-boundary resync is soft: a reconnect that agrees on the
	// digest may suppress this full sync. Reset explicitly so a prior
	// hard resync (request_resync) does not bleed across the boundary.
	s.resyncIsHard = false
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
	// A fresh session may front a restarted peer; its v3 epoch counter is
	// per-process and unrelated to the previous session's value, so forget
	// the watermark to avoid wrongly classifying the next frame as stale.
	s.knownV3Epoch = 0
	s.knownV3EpochSet = false
	// Same session-boundary reset for the v3-baseline flag: a new session
	// has no observable v3 baseline regardless of what the previous one
	// emitted, so the first v3 cycle must re-send kind="full".
	s.wireBaselineV3SentToPeer = false
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
			lastSeenLiveAt:  r.clock(),
		}
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.needsFullResync = true
	// Reconnect resync is soft (digest-suppressible) — see resyncIsHard.
	// Reset explicitly so a hard resync recorded before the reconnect
	// (request_resync that never got serviced) does not survive into the
	// new session and block the digest optimisation.
	s.resyncIsHard = false
	// Refresh the liveness watermark: the peer is live again, so it must
	// not be a reconcile-eviction candidate.
	s.lastSeenLiveAt = r.clock()
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
	// Same session-boundary reset as MarkDisconnected: a reconnect may
	// front a restarted peer, so the v3 epoch watermark starts unset.
	s.knownV3Epoch = 0
	s.knownV3EpochSet = false
	// Idempotent v3-baseline reset — mirrors the legacy
	// wireBaselineSentToPeer reset above. A reconnect that skipped
	// MarkDisconnected must not inherit a stale v3-baseline flag from
	// the previous session, which would unlock v3 deltas without an
	// observable kind="full" frame in the new session.
	s.wireBaselineV3SentToPeer = false
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

// ReconcileLiveSet refreshes the liveness watermark for every peer in the
// authoritative live set and evicts any state not observed in that set for
// longer than 2*flapWindow. Returns the count of evicted entries.
//
// This is the registry's sole garbage collector. It depends ONLY on the
// live routing-capable peer set the announce loop already computes each
// cycle — never on a matching MarkDisconnected. A state created by any path
// (the session-lifecycle MarkReconnected hook or a relay-gated receive-path
// GetOrCreate) cannot outlive the peer's membership in the live set, so a
// skipped or asymmetric teardown hook can no longer leak per-peer state
// (including its lastSentSnapshot). The previous design keyed eviction on a
// disconnectedAt stamp that only MarkDisconnected wrote, which silently
// retained any state whose teardown hook never fired.
//
// Locking: r.mu is held for the whole sweep (map iteration + delete); each
// entry's own s.mu guards the per-state watermark read/write. The nesting
// is always r.mu → s.mu, never the reverse — see GetOrCreate / MarkReconnected.
func (r *AnnounceStateRegistry) ReconcileLiveSet(live []PeerIdentity, now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	liveSet := make(map[PeerIdentity]struct{}, len(live))
	for _, id := range live {
		liveSet[id] = struct{}{}
	}

	evictAfter := 2 * r.flapWindow
	evicted := 0

	for id, s := range r.peers {
		_, isLive := liveSet[id]

		s.mu.Lock()
		if isLive {
			s.lastSeenLiveAt = now
			s.mu.Unlock()
			continue
		}
		stale := now.Sub(s.lastSeenLiveAt) > evictAfter
		s.mu.Unlock()

		if stale {
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
		// Consistency-loss / request_resync is a hard resync: a digest
		// match must never suppress it. The peer is explicitly asking
		// for a fresh full table (or we detected desync), so the
		// optimisation does not apply.
		s.resyncIsHard = true
		s.mu.Unlock()
	}
}

// Cleanup removes all state. Used on owner shutdown.
func (r *AnnounceStateRegistry) Cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = make(map[PeerIdentity]*AnnouncePeerState)
}
