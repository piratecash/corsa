package node

import (
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testProviderConfig builds a minimal PeerProviderConfig with sensible defaults
// for testing. All callback fields return empty/safe values unless overridden.
func testProviderConfig() PeerProviderConfig {
	return PeerProviderConfig{
		HealthFn:    func(domain.PeerAddress) *PeerHealthView { return nil },
		ConnectedFn: func() map[string]struct{} { return nil },
		QueuedFn:    func() map[string]struct{} { return nil },
		ForbiddenFn: func(ip net.IP) bool { return false },
		NetworksFn: func() map[domain.NetGroup]struct{} {
			return map[domain.NetGroup]struct{}{
				domain.NetGroupIPv4:  {},
				domain.NetGroupIPv6:  {},
				domain.NetGroupLocal: {},
			}
		},
		BannedIPsFn:   func() map[string]domain.BannedIPEntry { return nil },
		ListenAddr:    ":64646",
		DefaultPort:   config.DefaultPeerPort,
		IsSelfAddress: func(domain.PeerAddress) bool { return false },
		NowFn:         func() time.Time { return time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC) },
	}
}

func mustAddr(s string) domain.PeerAddress {
	return domain.PeerAddress(s)
}

// ---------------------------------------------------------------------------
// Add / Restore / Remove / Count
// ---------------------------------------------------------------------------

func TestPeerProvider_AddAndCount(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceAnnounce)

	if got := pp.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2", got)
	}
}

func TestPeerProvider_AddEmptyAddress(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())
	pp.Add(mustAddr(""), domain.PeerSourceManual)
	if got := pp.Count(); got != 0 {
		t.Fatalf("Count() = %d, want 0 for empty address", got)
	}
}

func TestPeerProvider_AddMergeBootstrap(t *testing.T) {
	cfg := testProviderConfig()
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return baseTime }
	pp := NewPeerProvider(cfg)

	// First add as persisted.
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("1.2.3.4:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: baseTime.Add(-24 * time.Hour), // original discovery time
		Network: domain.NetGroupIPv4,
	})

	// Then add same address as bootstrap — Source should update,
	// AddedAt and Network should NOT change.
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	if got := pp.Count(); got != 1 {
		t.Fatalf("Count() = %d, want 1 after merge", got)
	}

	kp := pp.KnownPeerStatic(mustAddr("1.2.3.4:64646"))
	if kp == nil {
		t.Fatal("KnownPeerStatic returned nil")
	}
	if kp.Source != domain.PeerSourceBootstrap {
		t.Errorf("Source = %q, want %q after bootstrap merge", kp.Source, domain.PeerSourceBootstrap)
	}
	if !kp.AddedAt.Equal(baseTime.Add(-24 * time.Hour)) {
		t.Errorf("AddedAt changed after bootstrap merge: got %v", kp.AddedAt)
	}
	if kp.Network != domain.NetGroupIPv4 {
		t.Errorf("Network changed after bootstrap merge: got %v", kp.Network)
	}
}

func TestPeerProvider_AddNonBootstrapDoesNotOverwrite(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourcePeerExchange)

	kp1 := pp.KnownPeerStatic(mustAddr("1.2.3.4:64646"))
	if kp1 == nil {
		t.Fatal("expected known peer")
	}

	// Adding again with different non-bootstrap source should NOT change source.
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceAnnounce)
	kp2 := pp.KnownPeerStatic(mustAddr("1.2.3.4:64646"))
	if kp2.Source != domain.PeerSourcePeerExchange {
		t.Errorf("Source changed to %q, expected peer_exchange to be preserved", kp2.Source)
	}
}

func TestPeerProvider_Restore(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())

	addedAt := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("10.0.0.1:9000"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: addedAt,
		Network: domain.NetGroupLocal,
	})

	kp := pp.KnownPeerStatic(mustAddr("10.0.0.1:9000"))
	if kp == nil {
		t.Fatal("Restore: peer not found")
	}
	if kp.Source != domain.PeerSourcePersisted {
		t.Errorf("Source = %q, want persisted", kp.Source)
	}
	if !kp.AddedAt.Equal(addedAt) {
		t.Errorf("AddedAt = %v, want %v", kp.AddedAt, addedAt)
	}
	if kp.Network != domain.NetGroupLocal {
		t.Errorf("Network = %v, want local", kp.Network)
	}
}

func TestPeerProvider_Remove(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceManual)
	pp.Remove(mustAddr("1.2.3.4:64646"))

	if got := pp.Count(); got != 0 {
		t.Fatalf("Count() = %d after Remove, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Candidates — filtering
// ---------------------------------------------------------------------------

func TestCandidates_SelfAddressFiltered(t *testing.T) {
	cfg := testProviderConfig()
	cfg.IsSelfAddress = func(addr domain.PeerAddress) bool {
		return addr == mustAddr("1.2.3.4:64646")
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (self filtered), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("wrong candidate: %v", candidates[0].Address)
	}
}

func TestCandidates_ForbiddenIPFiltered(t *testing.T) {
	cfg := testProviderConfig()
	cfg.ForbiddenFn = func(ip net.IP) bool {
		return ip != nil && ip.IsLoopback()
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("127.0.0.1:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (loopback filtered), got %d", len(candidates))
	}
}

func TestCandidates_PersistedPrivateIPv4Filtered(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())

	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("127.0.0.1:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Network: domain.NetGroupLocal,
	})
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("10.1.2.3:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 1, 0, 0, time.UTC),
		Network: domain.NetGroupLocal,
	})
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("192.168.1.10:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 2, 0, 0, time.UTC),
		Network: domain.NetGroupLocal,
	})
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("172.16.5.9:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 3, 0, 0, time.UTC),
		Network: domain.NetGroupLocal,
	})
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("8.8.8.8:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 4, 0, 0, time.UTC),
		Network: domain.NetGroupIPv4,
	})

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected only public persisted peer to survive, got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("8.8.8.8:64646") {
		t.Fatalf("unexpected candidate %v", candidates[0].Address)
	}
}

func TestCandidates_RuntimePrivateIPv4NotFilteredByPersistedRule(t *testing.T) {
	cfg := testProviderConfig()
	cfg.ForbiddenFn = func(net.IP) bool { return false }
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("127.0.0.1:64646"), domain.PeerSourceManual)
	pp.Add(mustAddr("10.1.2.3:64646"), domain.PeerSourcePeerExchange)

	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("expected runtime-discovered private peers to be unaffected, got %d", len(candidates))
	}
}

func TestCandidates_BannedIPFiltered(t *testing.T) {
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: now.Add(1 * time.Hour),
				BanOrigin:   mustAddr("1.2.3.4:9999"),
				BanReason:   "incompatible_protocol",
			},
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (banned filtered), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("wrong candidate: %v", candidates[0].Address)
	}
}

func TestCandidates_BanPropagationByIP(t *testing.T) {
	// Peer A (1.2.3.4:8333, score=80) + Peer B (1.2.3.4:9999, banned)
	// → whole IP 1.2.3.4 banned, neither A nor B in candidates.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		switch addr {
		case mustAddr("1.2.3.4:8333"):
			return &PeerHealthView{Score: 80}
		case mustAddr("1.2.3.4:9999"):
			return &PeerHealthView{Score: -50, BannedUntil: now.Add(1 * time.Hour)}
		default:
			return nil
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8333"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (IP banned), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("wrong candidate: %v", candidates[0].Address)
	}
}

func TestCandidates_BanExpiry(t *testing.T) {
	// After BannedUntil expires, IP is unblocked.
	cfg := testProviderConfig()
	banExpiry := time.Date(2026, 4, 11, 11, 0, 0, 0, time.UTC)
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC) // after ban expiry
	cfg.NowFn = func() time.Time { return now }
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9999") {
			return &PeerHealthView{Score: -50, BannedUntil: banExpiry}
		}
		if addr == mustAddr("1.2.3.4:8333") {
			return &PeerHealthView{Score: 80}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8333"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)

	candidates := pp.Candidates()
	// Ban expired → IP unblocked, best score (8333) should be candidate.
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate after ban expiry, got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("1.2.3.4:8333") {
		t.Errorf("expected address 1.2.3.4:8333, got %v", candidates[0].Address)
	}
}

func TestCandidates_ConnectedFiltered(t *testing.T) {
	cfg := testProviderConfig()
	cfg.ConnectedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.4": {}}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (connected filtered), got %d", len(candidates))
	}
}

func TestCandidates_QueuedFiltered(t *testing.T) {
	cfg := testProviderConfig()
	cfg.QueuedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.4": {}}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (queued filtered), got %d", len(candidates))
	}
}

func TestCandidates_CooldownFiltered(t *testing.T) {
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:64646") {
			return &PeerHealthView{
				Score:               0,
				ConsecutiveFailures: 3,                          // > 1 → cooldown active
				LastDisconnectedAt:  now.Add(-10 * time.Second), // recent
			}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (cooldown filtered), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("wrong candidate: %v", candidates[0].Address)
	}
}

func TestCandidates_SingleFailureNoCooldown(t *testing.T) {
	// A single failure does NOT trigger cooldown — immediate retry.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:64646") {
			return &PeerHealthView{
				Score:               0,
				ConsecutiveFailures: 1, // exactly 1 → no cooldown
				LastDisconnectedAt:  now.Add(-1 * time.Second),
			}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (single failure, no cooldown), got %d", len(candidates))
	}
}

func TestCandidates_NetworkReachabilityFiltered(t *testing.T) {
	cfg := testProviderConfig()
	// Only IPv4 reachable, not Tor.
	cfg.NetworksFn = func() map[domain.NetGroup]struct{} {
		return map[domain.NetGroup]struct{}{
			domain.NetGroupIPv4: {},
		}
	}
	pp := NewPeerProvider(cfg)

	// This address is IPv4 — should be in candidates.
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	// Simulate a Tor address by restoring with TorV3 network.
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz1234.onion:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Network: domain.NetGroupTorV3,
	})

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (unreachable filtered), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("1.2.3.4:64646") {
		t.Errorf("wrong candidate: %v", candidates[0].Address)
	}
}

// ---------------------------------------------------------------------------
// Candidates — deduplication
// ---------------------------------------------------------------------------

func TestCandidates_DeduplicationByIP_BestScoreWins(t *testing.T) {
	cfg := testProviderConfig()
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		switch addr {
		case mustAddr("1.2.3.4:8000"):
			return &PeerHealthView{Score: 50}
		case mustAddr("1.2.3.4:9000"):
			return &PeerHealthView{Score: 80}
		default:
			return nil
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9000"), domain.PeerSourcePeerExchange)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (IP dedup), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("1.2.3.4:9000") {
		t.Errorf("expected best score address 1.2.3.4:9000, got %v", candidates[0].Address)
	}
	if candidates[0].Score != 80 {
		t.Errorf("Score = %d, want 80", candidates[0].Score)
	}
}

func TestCandidates_DeduplicationByIP_EqualScore_EarlierAddedAtWins(t *testing.T) {
	cfg := testProviderConfig()
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		return &PeerHealthView{Score: 50}
	}
	pp := NewPeerProvider(cfg)

	early := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("1.2.3.4:8000"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: late,
		Network: domain.NetGroupIPv4,
	})
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("1.2.3.4:9000"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: early,
		Network: domain.NetGroupIPv4,
	})

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (IP dedup), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("1.2.3.4:9000") {
		t.Errorf("expected earlier AddedAt to win: want 1.2.3.4:9000, got %v", candidates[0].Address)
	}
}

// ---------------------------------------------------------------------------
// Candidates — sorting
// ---------------------------------------------------------------------------

func TestCandidates_SortByScoreDesc(t *testing.T) {
	cfg := testProviderConfig()
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		switch addr {
		case mustAddr("1.1.1.1:64646"):
			return &PeerHealthView{Score: 30}
		case mustAddr("2.2.2.2:64646"):
			return &PeerHealthView{Score: 80}
		case mustAddr("3.3.3.3:64646"):
			return &PeerHealthView{Score: 50}
		default:
			return nil
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.1.1.1:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("2.2.2.2:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("3.3.3.3:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(candidates))
	}
	if candidates[0].Score != 80 || candidates[1].Score != 50 || candidates[2].Score != 30 {
		t.Errorf("wrong order: scores = [%d, %d, %d], want [80, 50, 30]",
			candidates[0].Score, candidates[1].Score, candidates[2].Score)
	}
}

func TestCandidates_StableSortEqualScore(t *testing.T) {
	cfg := testProviderConfig()
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		return &PeerHealthView{Score: 50}
	}
	pp := NewPeerProvider(cfg)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	pp.Restore(domain.RestoreEntry{Address: mustAddr("3.3.3.3:64646"), Source: domain.PeerSourcePersisted, AddedAt: t3, Network: domain.NetGroupIPv4})
	pp.Restore(domain.RestoreEntry{Address: mustAddr("1.1.1.1:64646"), Source: domain.PeerSourcePersisted, AddedAt: t1, Network: domain.NetGroupIPv4})
	pp.Restore(domain.RestoreEntry{Address: mustAddr("2.2.2.2:64646"), Source: domain.PeerSourcePersisted, AddedAt: t2, Network: domain.NetGroupIPv4})

	candidates := pp.Candidates()
	if len(candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(candidates))
	}
	// Equal score → sorted by AddedAt ascending.
	if candidates[0].Address != mustAddr("1.1.1.1:64646") ||
		candidates[1].Address != mustAddr("2.2.2.2:64646") ||
		candidates[2].Address != mustAddr("3.3.3.3:64646") {
		t.Errorf("wrong stable sort order: [%v, %v, %v]",
			candidates[0].Address, candidates[1].Address, candidates[2].Address)
	}
}

// ---------------------------------------------------------------------------
// Candidates — fallback port
// ---------------------------------------------------------------------------

func TestCandidates_FallbackPortGenerated(t *testing.T) {
	cfg := testProviderConfig()
	pp := NewPeerProvider(cfg)

	// Non-standard port → should generate fallback with DefaultPeerPort.
	pp.Add(mustAddr("1.2.3.4:9000"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if len(candidates[0].DialAddresses) != 2 {
		t.Fatalf("expected 2 dial addresses (primary + fallback), got %d", len(candidates[0].DialAddresses))
	}
	if candidates[0].DialAddresses[0] != mustAddr("1.2.3.4:9000") {
		t.Errorf("primary = %v, want 1.2.3.4:9000", candidates[0].DialAddresses[0])
	}
	if candidates[0].DialAddresses[1] != mustAddr("1.2.3.4:64646") {
		t.Errorf("fallback = %v, want 1.2.3.4:64646", candidates[0].DialAddresses[1])
	}
}

func TestCandidates_NoFallbackForDefaultPort(t *testing.T) {
	cfg := testProviderConfig()
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if len(candidates[0].DialAddresses) != 1 {
		t.Fatalf("expected 1 dial address (no fallback for default port), got %d", len(candidates[0].DialAddresses))
	}
}

func TestCandidates_NoFallbackForLoopback(t *testing.T) {
	cfg := testProviderConfig()
	// Allow loopback to not be forbidden.
	cfg.ForbiddenFn = func(ip net.IP) bool { return false }
	cfg.NetworksFn = func() map[domain.NetGroup]struct{} {
		return map[domain.NetGroup]struct{}{
			domain.NetGroupLocal: {},
			domain.NetGroupIPv4:  {},
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("127.0.0.1:9000"), domain.PeerSourceManual)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	// Loopback address should NOT get fallback port.
	if len(candidates[0].DialAddresses) != 1 {
		t.Fatalf("expected 1 dial address (no fallback for loopback), got %d", len(candidates[0].DialAddresses))
	}
}

// ---------------------------------------------------------------------------
// Candidates — empty
// ---------------------------------------------------------------------------

func TestCandidates_EmptyWhenNoPeers(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())

	candidates := pp.Candidates()
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidates for empty provider, got %d", len(candidates))
	}
}

// ---------------------------------------------------------------------------
// KnownPeers — ExcludeReasons
// ---------------------------------------------------------------------------

func TestKnownPeers_ExcludeReasons(t *testing.T) {
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }

	// 1.2.3.4 is connected.
	cfg.ConnectedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.4": {}}
	}

	// 5.6.7.8 is in cooldown.
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("5.6.7.8:64646") {
			return &PeerHealthView{
				Score:               0,
				ConsecutiveFailures: 3,
				LastDisconnectedAt:  now.Add(-5 * time.Second),
			}
		}
		return nil
	}

	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("9.9.9.9:64646"), domain.PeerSourceBootstrap)

	peers := pp.KnownPeers()
	if len(peers) != 3 {
		t.Fatalf("expected 3 known peers, got %d", len(peers))
	}

	// Build lookup.
	byAddr := make(map[domain.PeerAddress]domain.KnownPeerInfo)
	for _, p := range peers {
		byAddr[p.Address] = p
	}

	// 1.2.3.4 should have "connected".
	if p, ok := byAddr[mustAddr("1.2.3.4:64646")]; ok {
		if !containsReason(p.ExcludeReasons, domain.ExcludeReasonConnected) {
			t.Errorf("1.2.3.4 should have 'connected' reason, got %v", p.ExcludeReasons)
		}
	} else {
		t.Error("1.2.3.4 not found in KnownPeers")
	}

	// 5.6.7.8 should have "cooldown".
	if p, ok := byAddr[mustAddr("5.6.7.8:64646")]; ok {
		if !containsReason(p.ExcludeReasons, domain.ExcludeReasonCooldown) {
			t.Errorf("5.6.7.8 should have 'cooldown' reason, got %v", p.ExcludeReasons)
		}
	} else {
		t.Error("5.6.7.8 not found in KnownPeers")
	}

	// 9.9.9.9 should have no reasons (valid candidate).
	if p, ok := byAddr[mustAddr("9.9.9.9:64646")]; ok {
		if len(p.ExcludeReasons) != 0 {
			t.Errorf("9.9.9.9 should have no exclude reasons, got %v", p.ExcludeReasons)
		}
	} else {
		t.Error("9.9.9.9 not found in KnownPeers")
	}
}

func TestKnownPeers_MultipleReasons(t *testing.T) {
	// A peer can have multiple exclude reasons simultaneously.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }

	// 1.2.3.4 is both connected AND banned.
	cfg.ConnectedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.4": {}}
	}
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: now.Add(1 * time.Hour),
				BanOrigin:   mustAddr("1.2.3.4:9999"),
				BanReason:   "incompatible_protocol",
			},
		}
	}

	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	peers := pp.KnownPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}

	reasons := peers[0].ExcludeReasons
	if !containsReason(reasons, domain.ExcludeReasonBanned) {
		t.Errorf("expected 'banned' reason, got %v", reasons)
	}
	if !containsReason(reasons, domain.ExcludeReasonConnected) {
		t.Errorf("expected 'connected' reason, got %v", reasons)
	}
}

func TestKnownPeers_PersistedPrivateIPv4MarkedForbidden(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())
	pp.Restore(domain.RestoreEntry{
		Address: mustAddr("192.168.1.10:64646"),
		Source:  domain.PeerSourcePersisted,
		AddedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Network: domain.NetGroupLocal,
	})

	peers := pp.KnownPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 known peer, got %d", len(peers))
	}
	if !containsReason(peers[0].ExcludeReasons, domain.ExcludeReasonForbidden) {
		t.Fatalf("expected persisted private peer to be marked forbidden, got %v", peers[0].ExcludeReasons)
	}
}

func TestKnownPeers_IPWideBanSetsBannedUntilForSiblings(t *testing.T) {
	// Address :8000 is banned via healthFn. Sibling :64646 on the same IP
	// has no direct ban but is banned via IP propagation. KnownPeerInfo
	// for :64646 must show the effective BannedUntil, not zero.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	banExpiry := now.Add(24 * time.Hour)
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:8000") {
			return &PeerHealthView{BannedUntil: banExpiry}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourcePeerExchange)

	peers := pp.KnownPeers()
	byAddr := make(map[domain.PeerAddress]domain.KnownPeerInfo)
	for _, p := range peers {
		byAddr[p.Address] = p
	}

	// :8000 — directly banned, should show ban expiry.
	if p := byAddr[mustAddr("1.2.3.4:8000")]; p.BannedUntil.IsZero() {
		t.Error(":8000 BannedUntil should be non-zero (direct ban)")
	}

	// :64646 — sibling on same IP, no direct ban. Must still show effective
	// ban expiry from IP-wide propagation.
	sibling := byAddr[mustAddr("1.2.3.4:64646")]
	if !containsReason(sibling.ExcludeReasons, domain.ExcludeReasonBanned) {
		t.Errorf(":64646 should have 'banned' reason, got %v", sibling.ExcludeReasons)
	}
	if sibling.BannedUntil.IsZero() {
		t.Fatal(":64646 BannedUntil should be non-zero (IP-wide ban propagation)")
	}
	if !sibling.BannedUntil.Equal(banExpiry) {
		t.Errorf(":64646 BannedUntil = %v, want %v", sibling.BannedUntil, banExpiry)
	}
}

func TestKnownPeers_ConnectedReflectsIPLevel(t *testing.T) {
	// Port :9000 is connected. Sibling :64646 on the same IP is not
	// directly connected but should report Connected=true because another
	// port on the same IP is connected (consistent with ExcludeReasons).
	cfg := testProviderConfig()
	cfg.ConnectedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.4": {}}
	}
	// HealthFn only marks :9000 as connected, not :64646.
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9000") {
			return &PeerHealthView{Connected: true, Score: 50}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:9000"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	peers := pp.KnownPeers()
	byAddr := make(map[domain.PeerAddress]domain.KnownPeerInfo)
	for _, p := range peers {
		byAddr[p.Address] = p
	}

	// :9000 — directly connected.
	if !byAddr[mustAddr("1.2.3.4:9000")].Connected {
		t.Error(":9000 should be Connected=true")
	}

	// :64646 — not directly connected, but IP-level connected.
	sibling := byAddr[mustAddr("1.2.3.4:64646")]
	if !containsReason(sibling.ExcludeReasons, domain.ExcludeReasonConnected) {
		t.Errorf(":64646 should have 'connected' reason, got %v", sibling.ExcludeReasons)
	}
	if !sibling.Connected {
		t.Error(":64646 Connected should be true (IP-level connectivity)")
	}
}

func TestCandidates_DedupStableOnFullTie(t *testing.T) {
	// Two ports on the same IP, same score, same AddedAt.
	// bestByIP must pick the same winner every time (lowest address).
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }

	pp := NewPeerProvider(cfg)
	// Add in non-alphabetical order — if dedup is stable, lowest address wins.
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.3.4:1111"), domain.PeerSourceBootstrap)

	// Run Candidates multiple times to catch flapping.
	for i := 0; i < 20; i++ {
		cands := pp.Candidates()
		if len(cands) != 1 {
			t.Fatalf("iteration %d: expected 1 candidate (deduped by IP), got %d", i, len(cands))
		}
		if cands[0].Address != mustAddr("1.2.3.4:1111") {
			t.Fatalf("iteration %d: expected 1.2.3.4:1111 (lowest addr), got %v", i, cands[0].Address)
		}
	}
}

// ---------------------------------------------------------------------------
// BannedIPs
// ---------------------------------------------------------------------------

func TestBannedIPs_ReturnsIPLevelRecords(t *testing.T) {
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: now.Add(1 * time.Hour),
				BanOrigin:   mustAddr("1.2.3.4:9999"),
				BanReason:   "incompatible_protocol",
			},
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP, got %d", len(banned))
	}
	if banned[0].IP != "1.2.3.4" {
		t.Errorf("IP = %v, want 1.2.3.4", banned[0].IP)
	}
	if banned[0].BanOrigin != mustAddr("1.2.3.4:9999") {
		t.Errorf("BanOrigin = %v, want 1.2.3.4:9999", banned[0].BanOrigin)
	}
	if banned[0].BanReason != "incompatible_protocol" {
		t.Errorf("BanReason = %v, want incompatible_protocol", banned[0].BanReason)
	}
	if len(banned[0].AffectedPeers) != 2 {
		t.Fatalf("expected 2 affected peers, got %d", len(banned[0].AffectedPeers))
	}
}

func TestBannedIPs_DirectBanPropagation(t *testing.T) {
	// Direct ban on one port propagates to whole IP via healthFn.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9999") {
			return &PeerHealthView{BannedUntil: now.Add(1 * time.Hour)}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)

	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP (via direct propagation), got %d", len(banned))
	}
	if banned[0].IP != "1.2.3.4" {
		t.Errorf("IP = %v, want 1.2.3.4", banned[0].IP)
	}
	if banned[0].BanOrigin != mustAddr("1.2.3.4:9999") {
		t.Errorf("BanOrigin = %v, want 1.2.3.4:9999", banned[0].BanOrigin)
	}
}

func TestBannedIPs_DirectBanAggregatesMaxExpiry(t *testing.T) {
	// Two ports on the same IP banned until different times.
	// buildBannedIPsSet must take the MAXIMUM BannedUntil, not first-match.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	shortBan := now.Add(1 * time.Hour)
	longBan := now.Add(24 * time.Hour)
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		switch addr {
		case mustAddr("1.2.3.4:8000"):
			return &PeerHealthView{BannedUntil: shortBan}
		case mustAddr("1.2.3.4:9999"):
			return &PeerHealthView{BannedUntil: longBan}
		default:
			return nil
		}
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)

	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP, got %d", len(banned))
	}
	if banned[0].IP != "1.2.3.4" {
		t.Errorf("IP = %v, want 1.2.3.4", banned[0].IP)
	}
	// Must be the longer ban, regardless of map iteration order.
	if !banned[0].BannedUntil.Equal(longBan) {
		t.Errorf("BannedUntil = %v, want %v (max of two ports)", banned[0].BannedUntil, longBan)
	}
	if banned[0].BanOrigin != mustAddr("1.2.3.4:9999") {
		t.Errorf("BanOrigin = %v, want 1.2.3.4:9999 (port with longest ban)", banned[0].BanOrigin)
	}
}

func TestBannedIPs_DirectBanDoesNotOverrideIPWideBan(t *testing.T) {
	// IP-wide ban (source a) with later expiry must not be overwritten
	// by a per-port ban (source b) with earlier expiry.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	ipWideBan := now.Add(48 * time.Hour)
	portBan := now.Add(1 * time.Hour)
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: ipWideBan,
				BanOrigin:   mustAddr("1.2.3.4:7777"),
				BanReason:   "manual",
			},
		}
	}
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9999") {
			return &PeerHealthView{BannedUntil: portBan}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)

	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP, got %d", len(banned))
	}
	// IP-wide ban has later expiry — must win.
	if !banned[0].BannedUntil.Equal(ipWideBan) {
		t.Errorf("BannedUntil = %v, want %v (IP-wide ban should win)", banned[0].BannedUntil, ipWideBan)
	}
}

func TestBannedIPs_PortBanExtendsIPWideBan(t *testing.T) {
	// Per-port ban (source b) has LATER expiry than IP-wide ban (source a).
	// The aggregation must extend to the per-port maximum.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	ipWideBan := now.Add(1 * time.Hour)
	portBan := now.Add(48 * time.Hour)
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: ipWideBan,
				BanOrigin:   mustAddr("1.2.3.4:7777"),
				BanReason:   "manual",
			},
		}
	}
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9999") {
			return &PeerHealthView{BannedUntil: portBan}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)

	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP, got %d", len(banned))
	}
	// Per-port ban is later — must extend the IP-wide ban.
	if !banned[0].BannedUntil.Equal(portBan) {
		t.Errorf("BannedUntil = %v, want %v (per-port ban extends IP-wide)", banned[0].BannedUntil, portBan)
	}
	if banned[0].BanOrigin != mustAddr("1.2.3.4:9999") {
		t.Errorf("BanOrigin = %v, want 1.2.3.4:9999", banned[0].BanOrigin)
	}
}

func TestBannedIPs_EmptyWhenNoBans(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig())
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	banned := pp.BannedIPs()
	if len(banned) != 0 {
		t.Fatalf("expected 0 banned IPs, got %d", len(banned))
	}
}

// ---------------------------------------------------------------------------
// V2 migration: peers.json without banned_ips section
// ---------------------------------------------------------------------------

func TestCandidates_V2MigrationBansFromHealth(t *testing.T) {
	// Simulates V2 migration: no bannedIPsFn entries, bans reconstructed
	// from per-address BannedUntil in healthFn.
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry { return nil } // empty — v2 has no banned_ips
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9999") {
			return &PeerHealthView{BannedUntil: now.Add(1 * time.Hour)}
		}
		if addr == mustAddr("1.2.3.4:8000") {
			return &PeerHealthView{Score: 80}
		}
		return nil
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	// 1.2.3.4 should be banned (propagated from :9999 via healthFn).
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (v2 ban propagation), got %d", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("wrong candidate: %v", candidates[0].Address)
	}

	// BannedIPs diagnostics should show the ban.
	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP in v2 migration, got %d", len(banned))
	}
	// BanOrigin is best-effort in v2.
	if banned[0].IP != "1.2.3.4" {
		t.Errorf("banned IP = %v, want 1.2.3.4", banned[0].IP)
	}
}

// ---------------------------------------------------------------------------
// V3 round-trip
// ---------------------------------------------------------------------------

func TestV3RoundTrip(t *testing.T) {
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }

	// Set up IP-wide bans from bannedIPSet.
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: now.Add(2 * time.Hour),
				BanOrigin:   mustAddr("1.2.3.4:9999"),
				BanReason:   "incompatible_protocol",
			},
		}
	}
	cfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		if addr == mustAddr("1.2.3.4:9999") {
			return &PeerHealthView{BannedUntil: now.Add(2 * time.Hour)}
		}
		return nil
	}

	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:8000"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	// (1) Candidates() excludes banned IP.
	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}

	// (2) list_banned returns exact BanOrigin and AffectedPeers.
	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 banned IP, got %d", len(banned))
	}
	if banned[0].BanOrigin != mustAddr("1.2.3.4:9999") {
		t.Errorf("BanOrigin = %v, want 1.2.3.4:9999", banned[0].BanOrigin)
	}
	if len(banned[0].AffectedPeers) != 2 {
		t.Errorf("AffectedPeers count = %d, want 2", len(banned[0].AffectedPeers))
	}

	// (3) KnownPeers for addresses on banned IP has 'banned' reason.
	known := pp.KnownPeers()
	for _, kp := range known {
		if kp.Address == mustAddr("1.2.3.4:8000") || kp.Address == mustAddr("1.2.3.4:9999") {
			if !containsReason(kp.ExcludeReasons, domain.ExcludeReasonBanned) {
				t.Errorf("%v should have 'banned' reason, got %v", kp.Address, kp.ExcludeReasons)
			}
		}
	}
}

func TestV3RoundTrip_PartialExpiry(t *testing.T) {
	cfg := testProviderConfig()
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	cfg.NowFn = func() time.Time { return now }

	// Two banned IPs: one expired, one active.
	cfg.BannedIPsFn = func() map[string]domain.BannedIPEntry {
		return map[string]domain.BannedIPEntry{
			"1.2.3.4": {
				IP:          "1.2.3.4",
				BannedUntil: now.Add(-1 * time.Hour), // EXPIRED
				BanOrigin:   mustAddr("1.2.3.4:9999"),
				BanReason:   "incompatible_protocol",
			},
			"5.6.7.8": {
				IP:          "5.6.7.8",
				BannedUntil: now.Add(2 * time.Hour), // ACTIVE
				BanOrigin:   mustAddr("5.6.7.8:64646"),
				BanReason:   "incompatible_protocol",
			},
		}
	}

	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("9.9.9.9:64646"), domain.PeerSourceBootstrap)

	// Candidates: expired IP allowed, active IP blocked.
	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates (expired + clean), got %d", len(candidates))
	}

	// BannedIPs: only active ban.
	banned := pp.BannedIPs()
	if len(banned) != 1 {
		t.Fatalf("expected 1 active banned IP, got %d", len(banned))
	}
	if banned[0].IP != "5.6.7.8" {
		t.Errorf("expected active ban on 5.6.7.8, got %v", banned[0].IP)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func containsReason(reasons []domain.ExcludeReason, target domain.ExcludeReason) bool {
	for _, r := range reasons {
		if r == target {
			return true
		}
	}
	return false
}
