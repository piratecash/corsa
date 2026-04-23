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
}

// PeerIdentity returns the immutable identity of this peer state.
func (s *AnnouncePeerState) PeerIdentity() PeerIdentity {
	// Immutable after creation — no lock needed.
	return s.peerIdentity
}

// announcePeerStateView is a read-only snapshot of AnnouncePeerState fields
// needed by the announce loop to make send decisions. Captured under a
// single lock acquisition to prevent torn reads.
type announcePeerStateView struct {
	NeedsFullResync          bool
	LastSentSnapshot         *AnnounceSnapshot
	LastSuccessfulFullSyncAt time.Time
	LastFullSyncAttemptAt    time.Time
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
// reconnect is not throttled.
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
}

// MarkReconnected resets a previously disconnected peer for a new announce
// session. Always requires forced full sync. Rate limit timer is reset.
func (r *AnnounceStateRegistry) MarkReconnected(peerID PeerIdentity) {
	r.mu.Lock()
	s, ok := r.peers[peerID]
	if !ok {
		// Create fresh state.
		r.peers[peerID] = &AnnouncePeerState{
			peerIdentity:    peerID,
			needsFullResync: true,
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
