package routing

import (
	"testing"
	"time"
)

func TestAnnounceStateRegistry_GetOrCreate(t *testing.T) {
	r := NewAnnounceStateRegistry()

	s := r.GetOrCreate("peer-A")
	if s == nil {
		t.Fatal("expected non-nil state")
	}
	if s.PeerIdentity() != "peer-A" {
		t.Fatalf("expected peer-A, got %s", s.PeerIdentity())
	}

	view := s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true for new peer")
	}
	if view.LastSentSnapshot != nil {
		t.Fatal("expected nil LastSentSnapshot for new peer")
	}

	// Verify mode via mutex-protected read.
	s.mu.Lock()
	mode := s.mode
	s.mu.Unlock()
	if mode != AnnounceModeLegacy {
		t.Fatal("expected legacy mode")
	}

	// Second call returns same object.
	s2 := r.GetOrCreate("peer-A")
	if s2 != s {
		t.Fatal("expected same state object")
	}
}

func TestAnnounceStateRegistry_TimestampsInitializedToZero(t *testing.T) {
	r := NewAnnounceStateRegistry()
	s := r.GetOrCreate("peer-A")

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

	s := r.GetOrCreate("peer-A")

	// Set up initial state: resync done, attempt recorded.
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, now.Add(-time.Minute))

	r.MarkDisconnected("peer-A")

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

	s := r.GetOrCreate("peer-A")

	// Set up: mark as synced, then record an attempt.
	now := time.Now()
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, now)
	s.RecordFullSyncAttempt(now)

	r.MarkReconnected("peer-A")

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
	r.MarkReconnected("peer-new")

	s := r.Get("peer-new")
	if s == nil {
		t.Fatal("expected state to be created")
	}

	view := s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true")
	}
}

func TestAnnounceStateRegistry_EvictStale(t *testing.T) {
	now := time.Now()
	r := NewAnnounceStateRegistry(
		WithRegistryClock(func() time.Time { return now }),
		WithRegistryFlapWindow(60*time.Second),
	)

	s := r.GetOrCreate("peer-A")
	// Simulate disconnect 3 minutes ago (180s > 2*60s evict threshold).
	s.mu.Lock()
	s.disconnectedAt = now.Add(-3 * time.Minute)
	s.mu.Unlock()

	evicted := r.EvictStale()
	if evicted != 1 {
		t.Fatalf("expected 1 evicted, got %d", evicted)
	}

	if r.Get("peer-A") != nil {
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
	r.GetOrCreate("peer-A")

	evicted := r.EvictStale()
	if evicted != 0 {
		t.Fatalf("expected 0 evicted, got %d", evicted)
	}
	if r.Get("peer-A") == nil {
		t.Fatal("connected peer should not be evicted")
	}
}

func TestAnnounceStateRegistry_EvictStaleKeepsRecent(t *testing.T) {
	now := time.Now()
	r := NewAnnounceStateRegistry(
		WithRegistryClock(func() time.Time { return now }),
		WithRegistryFlapWindow(60*time.Second),
	)

	s := r.GetOrCreate("peer-A")
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

	s := r.GetOrCreate("peer-A")

	// Clear the initial NeedsFullResync via a successful full sync.
	s.RecordFullSyncSuccess(&AnnounceSnapshot{}, time.Now())

	view := s.View()
	if view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=false after full sync success")
	}

	r.MarkInvalid("peer-A")

	view = s.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true after MarkInvalid")
	}
}
