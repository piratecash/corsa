package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func TestAnnounceStateRegistry_GetOrCreate(t *testing.T) {
	r := NewAnnounceStateRegistry()

	s := r.GetOrCreate(domaintest.ID("peer-A"))
	if s == nil {
		t.Fatal("expected non-nil state")
	}
	if s.PeerIdentity() != domaintest.ID("peer-A") {
		t.Fatalf("expected peer-A, got %s", s.PeerIdentity())
	}

	view := s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true for new peer")
	}
	if view.LastSentSnapshot != nil {
		t.Fatal("expected nil LastSentSnapshot for new peer")
	}

	// Second call returns same object.
	s2 := r.GetOrCreate(domaintest.ID("peer-A"))
	if s2 != s {
		t.Fatal("expected same state object")
	}
}

func TestAnnounceStateRegistry_TimestampsInitializedToZero(t *testing.T) {
	r := NewAnnounceStateRegistry()
	s := r.GetOrCreate(domaintest.ID("peer-A"))

	view := s.View()
	if !view.LastSuccessfulFullSyncAt.IsZero() {
		t.Fatal("LastSuccessfulFullSyncAt should be zero")
	}
	if !view.LastFullSyncAttemptAt.IsZero() {
		t.Fatal("LastFullSyncAttemptAt should be zero")
	}

	// lastDeltaSendAt is not in View — read under lock.
	s.mu.Lock()
	deltaSendAt := s.lastDeltaSendAt
	s.mu.Unlock()
	if !deltaSendAt.IsZero() {
		t.Fatal("LastDeltaSendAt should be zero")
	}
}

func TestAnnounceStateRegistry_MarkDisconnected(t *testing.T) {
	now := time.Now()
	r := NewAnnounceStateRegistry(
		WithRegistryClock(func() time.Time { return now }),
	)

	s := r.GetOrCreate(domaintest.ID("peer-A"))

	// Set up initial state: resync done, attempt recorded.
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, now.Add(-time.Minute))

	r.MarkDisconnected(domaintest.ID("peer-A"))

	view := s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true after disconnect")
	}
	if !view.LastFullSyncAttemptAt.IsZero() {
		t.Fatal("expected rate limit timer reset after disconnect")
	}

	s.mu.Lock()
	disc := s.disconnectedAt
	s.mu.Unlock()
	if disc.IsZero() {
		t.Fatal("expected disconnectedAt to be set")
	}
}

func TestAnnounceStateRegistry_MarkReconnected(t *testing.T) {
	r := NewAnnounceStateRegistry()

	s := r.GetOrCreate(domaintest.ID("peer-A"))

	// Set up: mark as synced, then record an attempt.
	now := time.Now()
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, now)
	s.RecordFullSyncAttempt(now)

	r.MarkReconnected(domaintest.ID("peer-A"), nil)

	view := s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true after reconnect")
	}
	if !view.LastFullSyncAttemptAt.IsZero() {
		t.Fatal("expected rate limit timer reset after reconnect")
	}

	s.mu.Lock()
	disc := s.disconnectedAt
	s.mu.Unlock()
	if !disc.IsZero() {
		t.Fatal("expected disconnectedAt cleared after reconnect")
	}
}

func TestAnnounceStateRegistry_MarkReconnectedCreatesNew(t *testing.T) {
	r := NewAnnounceStateRegistry()

	// MarkReconnected for unknown peer should create new state.
	r.MarkReconnected(domaintest.ID("peer-new"), nil)

	s := r.Get(domaintest.ID("peer-new"))
	if s == nil {
		t.Fatal("expected state to be created")
	}

	view := s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true")
	}
}

// TestAnnounceStateRegistry_MarkReconnectedStoresCapabilitiesNewPeer verifies
// that the caps slice passed to MarkReconnected is saved into
// AnnouncePeerState.capabilities when the peer is not yet known to the
// registry (the "create fresh state" branch). This is a write-only contract
// in the current PR — routing-announce v2 will add the read path.
func TestAnnounceStateRegistry_MarkReconnectedStoresCapabilitiesNewPeer(t *testing.T) {
	r := NewAnnounceStateRegistry()

	caps := []PeerCapability{
		PeerCapability("mesh_relay_v1"),
		PeerCapability("mesh_routing_v1"),
	}
	r.MarkReconnected(domaintest.ID("peer-new"), caps)

	s := r.Get(domaintest.ID("peer-new"))
	if s == nil {
		t.Fatal("expected state to be created")
	}

	s.mu.Lock()
	stored := s.capabilities
	s.mu.Unlock()

	if len(stored) != 2 {
		t.Fatalf("expected 2 capabilities stored, got %d", len(stored))
	}
	if stored[0] != PeerCapability("mesh_relay_v1") || stored[1] != PeerCapability("mesh_routing_v1") {
		t.Fatalf("unexpected stored capabilities: %v", stored)
	}
}

// TestAnnounceStateRegistry_MarkReconnectedStoresCapabilitiesExistingPeer
// verifies the "existing peer" branch of MarkReconnected: when the registry
// already has state for the peer (created earlier by GetOrCreate or a prior
// MarkReconnected call), the new caps slice must overwrite the stored
// capabilities, not append. A reconnect with a different capability set is
// legitimate — e.g. a peer upgraded to a newer protocol version between
// sessions.
func TestAnnounceStateRegistry_MarkReconnectedStoresCapabilitiesExistingPeer(t *testing.T) {
	r := NewAnnounceStateRegistry()

	// Prime state with one capability set.
	r.MarkReconnected(domaintest.ID("peer-A"), []PeerCapability{PeerCapability("mesh_relay_v1")})

	// Reconnect with a different (smaller) set.
	r.MarkReconnected(domaintest.ID("peer-A"), []PeerCapability{PeerCapability("mesh_routing_v1")})

	s := r.Get(domaintest.ID("peer-A"))
	if s == nil {
		t.Fatal("expected state to exist")
	}

	s.mu.Lock()
	stored := s.capabilities
	s.mu.Unlock()

	if len(stored) != 1 {
		t.Fatalf("expected stored caps to be overwritten (len=1), got %d", len(stored))
	}
	if stored[0] != PeerCapability("mesh_routing_v1") {
		t.Fatalf("expected mesh_routing_v1 after overwrite, got %q", stored[0])
	}
}

// TestAnnounceStateRegistry_MarkReconnectedDefensiveCopy verifies that
// mutating the caller's caps slice after MarkReconnected does NOT change the
// stored capabilities. The contract lives inside MarkReconnected so that the
// session-lifecycle hook can pass session-owned slices directly without
// copying at the call site.
func TestAnnounceStateRegistry_MarkReconnectedDefensiveCopy(t *testing.T) {
	r := NewAnnounceStateRegistry()

	caps := []PeerCapability{
		PeerCapability("mesh_relay_v1"),
		PeerCapability("mesh_routing_v1"),
	}
	r.MarkReconnected(domaintest.ID("peer-A"), caps)

	// Mutate the caller's slice after the call.
	caps[0] = PeerCapability("corrupted")
	caps[1] = PeerCapability("also_corrupted")

	s := r.Get(domaintest.ID("peer-A"))
	if s == nil {
		t.Fatal("expected state to exist")
	}

	s.mu.Lock()
	stored := s.capabilities
	s.mu.Unlock()

	if stored[0] != PeerCapability("mesh_relay_v1") || stored[1] != PeerCapability("mesh_routing_v1") {
		t.Fatalf("defensive copy failed — stored slice mutated: %v", stored)
	}
}

// TestAnnounceStateRegistry_MarkReconnectedNilCapabilities verifies the
// nil-means-nil contract documented on copyCapabilities: passing nil caps
// results in a nil stored slice (not an empty non-nil slice). This preserves
// the "capabilities not advertised" signal for callers that will later read
// the field in routing-announce v2.
func TestAnnounceStateRegistry_MarkReconnectedNilCapabilities(t *testing.T) {
	r := NewAnnounceStateRegistry()

	r.MarkReconnected(domaintest.ID("peer-A"), nil)

	s := r.Get(domaintest.ID("peer-A"))
	if s == nil {
		t.Fatal("expected state to exist")
	}

	s.mu.Lock()
	stored := s.capabilities
	s.mu.Unlock()

	if stored != nil {
		t.Fatalf("expected nil stored capabilities for nil input, got %v", stored)
	}
}

func TestAnnounceStateRegistry_EvictStale(t *testing.T) {
	now := time.Now()
	r := NewAnnounceStateRegistry(
		WithRegistryClock(func() time.Time { return now }),
		WithRegistryFlapWindow(60*time.Second),
	)

	s := r.GetOrCreate(domaintest.ID("peer-A"))
	// Simulate disconnect 3 minutes ago (180s > 2*60s evict threshold).
	s.mu.Lock()
	s.disconnectedAt = now.Add(-3 * time.Minute)
	s.mu.Unlock()

	evicted := r.EvictStale()
	if evicted != 1 {
		t.Fatalf("expected 1 evicted, got %d", evicted)
	}

	if r.Get(domaintest.ID("peer-A")) != nil {
		t.Fatal("expected peer-A to be evicted")
	}
}

func TestAnnounceStateRegistry_EvictStaleKeepsConnected(t *testing.T) {
	now := time.Now()
	r := NewAnnounceStateRegistry(
		WithRegistryClock(func() time.Time { return now }),
		WithRegistryFlapWindow(60*time.Second),
	)

	// Connected peer (disconnectedAt = zero) should not be evicted.
	r.GetOrCreate(domaintest.ID("peer-A"))

	evicted := r.EvictStale()
	if evicted != 0 {
		t.Fatalf("expected 0 evicted, got %d", evicted)
	}
	if r.Get(domaintest.ID("peer-A")) == nil {
		t.Fatal("connected peer should not be evicted")
	}
}

func TestAnnounceStateRegistry_EvictStaleKeepsRecent(t *testing.T) {
	now := time.Now()
	r := NewAnnounceStateRegistry(
		WithRegistryClock(func() time.Time { return now }),
		WithRegistryFlapWindow(60*time.Second),
	)

	s := r.GetOrCreate(domaintest.ID("peer-A"))
	// Disconnect 30s ago (30s < 2*60s evict threshold).
	s.mu.Lock()
	s.disconnectedAt = now.Add(-30 * time.Second)
	s.mu.Unlock()

	evicted := r.EvictStale()
	if evicted != 0 {
		t.Fatalf("expected 0 evicted for recent disconnect, got %d", evicted)
	}
}

func TestAnnounceStateRegistry_MarkInvalid(t *testing.T) {
	r := NewAnnounceStateRegistry()

	s := r.GetOrCreate(domaintest.ID("peer-A"))

	// Clear the initial NeedsFullResync via a successful full sync.
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, time.Now())

	view := s.View()
	if view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=false after full sync success")
	}

	r.MarkInvalid(domaintest.ID("peer-A"))

	view = s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true after MarkInvalid")
	}
}

// TestAnnounceStateRegistry_ResyncIsHard_Classification pins the
// Phase 3 digest-suppression contract: the resync triggered by
// MarkInvalid (request_resync / consistency loss) is HARD and must
// never be suppressed by a digest match, while the resync triggered
// by a session boundary (MarkReconnected / MarkDisconnected) is SOFT
// and IS suppressible. RecordFullSyncSuccess clears both flags.
func TestAnnounceStateRegistry_ResyncIsHard_Classification(t *testing.T) {
	r := NewAnnounceStateRegistry()
	s := r.GetOrCreate(domaintest.ID("peer-A"))

	// A fresh peer needs a full resync, but it is soft — the hardness
	// only matters once a baseline exists, and a brand-new peer is
	// non-suppressible via the LastSentSnapshot==nil gate anyway.
	if s.View().ResyncIsHard {
		t.Fatal("fresh peer must not be classified as a hard resync")
	}

	// Establish a baseline so the suppression gate's LastSentSnapshot
	// precondition is met and the classification becomes meaningful.
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, time.Now())
	if v := s.View(); v.NeedsFullResync || v.ResyncIsHard {
		t.Fatalf("RecordFullSyncSuccess must clear both flags, got NeedsFullResync=%v ResyncIsHard=%v", v.NeedsFullResync, v.ResyncIsHard)
	}

	// request_resync / consistency loss → HARD.
	r.MarkInvalid(domaintest.ID("peer-A"))
	if v := s.View(); !v.NeedsFullResync || !v.ResyncIsHard {
		t.Fatalf("MarkInvalid must set a hard resync, got NeedsFullResync=%v ResyncIsHard=%v", v.NeedsFullResync, v.ResyncIsHard)
	}

	// A successful full sync clears the hardness again.
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, time.Now())
	if s.View().ResyncIsHard {
		t.Fatal("RecordFullSyncSuccess did not clear ResyncIsHard")
	}

	// Session boundary → SOFT, even if a hard resync preceded it.
	r.MarkInvalid(domaintest.ID("peer-A"))
	r.MarkReconnected(domaintest.ID("peer-A"), nil)
	if v := s.View(); !v.NeedsFullResync || v.ResyncIsHard {
		t.Fatalf("MarkReconnected must reclassify to a soft resync, got NeedsFullResync=%v ResyncIsHard=%v", v.NeedsFullResync, v.ResyncIsHard)
	}

	// MarkDisconnected is likewise a soft resync.
	r.MarkInvalid(domaintest.ID("peer-A"))
	r.MarkDisconnected(domaintest.ID("peer-A"))
	if v := s.View(); !v.NeedsFullResync || v.ResyncIsHard {
		t.Fatalf("MarkDisconnected must reclassify to a soft resync, got NeedsFullResync=%v ResyncIsHard=%v", v.NeedsFullResync, v.ResyncIsHard)
	}
}
