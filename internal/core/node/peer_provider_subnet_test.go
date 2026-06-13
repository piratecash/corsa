package node

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Subnet-diversity filter — one automatic outbound connection per
// subnet group (/24 for public IPv4, /64 for public IPv6). There are
// exactly two deliberate
// exemptions (see docs/protocol/peers.md "subnet diversity"):
//   1. operator add_peer bypasses Candidates() entirely (immediate dial);
//   2. non-routable / private addresses carry no subnet group, so in
//      AllowPrivatePeers mode same-subnet private peers may stack
//      (TestCandidates_PrivateSubnetExempt).
// Startup bootstrap priming is NOT an exemption — it dials through
// Candidates() with the filter active.
// ---------------------------------------------------------------------------

// subnetFilterConfig returns testProviderConfig with the subnet-diversity
// filter ENABLED (production behaviour). The shared helper disables it
// because most provider tests use many fake peers inside one public /24.
func subnetFilterConfig() PeerProviderConfig {
	cfg := testProviderConfig()
	cfg.AllowSameSubnetCandidates = false
	return cfg
}

// scoreHealthFn builds a HealthFn returning a fixed score per address.
func scoreHealthFn(scores map[domain.PeerAddress]int) func(domain.PeerAddress) *PeerHealthView {
	return func(addr domain.PeerAddress) *PeerHealthView {
		score, ok := scores[addr]
		if !ok {
			return nil
		}
		return &PeerHealthView{Score: score}
	}
}

func TestSubnetGroupKey(t *testing.T) {
	cases := map[string]string{
		// Public IPv4 → /24 group.
		"1.2.3.4":        "1.2.3.0/24",
		"1.2.3.255":      "1.2.3.0/24",
		"1.2.4.4":        "1.2.4.0/24",
		"::ffff:1.2.3.4": "1.2.3.0/24", // IPv4-mapped IPv6 collapses to IPv4
		// Public IPv6 → /64 group.
		"2001:db8:1:1::5": "2001:db8:1:1::/64",
		"2001:db8:1:2::5": "2001:db8:1:2::/64",
		// Non-IP hosts carry no subnet group.
		"example.onion": "",
		"node.local":    "",
		"":              "",
		// Non-routable addresses are exempt (LAN/dev clusters).
		"10.0.0.1":    "",
		"192.168.1.7": "",
		"127.0.0.1":   "",
		"fd00::1":     "",
	}
	for host, want := range cases {
		if got := subnetGroupKey(host); got != want {
			t.Errorf("subnetGroupKey(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestCandidates_SameSubnet_BestCandidateWins(t *testing.T) {
	cfg := subnetFilterConfig()
	cfg.HealthFn = scoreHealthFn(map[domain.PeerAddress]int{
		mustAddr("1.2.3.4:64646"): 5,
		mustAddr("1.2.3.5:64646"): 10,
	})
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.3.5:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("Candidates() returned %d entries, want 1 (one per /24)", len(candidates))
	}
	if candidates[0].Address != mustAddr("1.2.3.5:64646") {
		t.Errorf("winner = %s, want 1.2.3.5:64646 (higher score)", candidates[0].Address)
	}
}

func TestCandidates_DistinctSubnets_AllReturned(t *testing.T) {
	pp := NewPeerProvider(subnetFilterConfig())
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.4.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 3 {
		t.Fatalf("Candidates() returned %d entries, want 3 (distinct /24s)", len(candidates))
	}
}

func TestCandidates_SubnetOccupiedByConnection(t *testing.T) {
	cfg := subnetFilterConfig()
	cfg.ConnectedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.9": {}}
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("Candidates() returned %d entries, want 1", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("candidate = %s, want 5.6.7.8:64646 (1.2.3.0/24 occupied by connection)", candidates[0].Address)
	}
}

func TestCandidates_SubnetOccupiedByQueuedSlot(t *testing.T) {
	cfg := subnetFilterConfig()
	cfg.QueuedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.9": {}}
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("Candidates() returned %d entries, want 1", len(candidates))
	}
	if candidates[0].Address != mustAddr("5.6.7.8:64646") {
		t.Errorf("candidate = %s, want 5.6.7.8:64646 (1.2.3.0/24 occupied by queued dial)", candidates[0].Address)
	}
}

func TestCandidates_IPv6SameSlash64_OneSelected(t *testing.T) {
	pp := NewPeerProvider(subnetFilterConfig())
	pp.Add(mustAddr("[2001:db8:1:1::1]:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("[2001:db8:1:1::2]:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("[2001:db8:1:2::1]:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("Candidates() returned %d entries, want 2 (one per /64)", len(candidates))
	}
	groups := make(map[string]int)
	for _, c := range candidates {
		host, _, _ := splitHostPort(string(c.Address))
		groups[subnetGroupKey(host)]++
	}
	for group, count := range groups {
		if count != 1 {
			t.Errorf("group %s has %d candidates, want 1", group, count)
		}
	}
}

func TestCandidates_PrivateSubnetExempt(t *testing.T) {
	// AllowPrivateCandidates is set by the shared helper; the subnet
	// filter must NOT collapse private peers — LAN/dev clusters
	// legitimately co-locate many nodes inside one private subnet.
	pp := NewPeerProvider(subnetFilterConfig())
	pp.Add(mustAddr("10.0.0.1:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("10.0.0.2:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("Candidates() returned %d entries, want 2 (private IPs exempt from subnet filter)", len(candidates))
	}
}

func TestKnownPeers_PrivatePeerNotForbiddenInPrivateMode(t *testing.T) {
	// AllowPrivateCandidates=true (dev/LAN mode). A private peer is a valid
	// automatic candidate here, so KnownPeers() must NOT report it forbidden
	// — list_peers has to agree with Candidates(). The shared helper's
	// ForbiddenFn returns false, isolating the shouldSkipPersistedPrivatePeer
	// gate this test pins.
	cfg := testProviderConfig() // AllowPrivateCandidates: true
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("10.0.0.1:64646"), domain.PeerSourceBootstrap)

	infos := pp.KnownPeers()
	if len(infos) != 1 {
		t.Fatalf("KnownPeers() returned %d entries, want 1", len(infos))
	}
	for _, r := range infos[0].ExcludeReasons {
		if r == domain.ExcludeReasonForbidden {
			t.Fatalf("private peer reported forbidden in AllowPrivateCandidates mode: %v", infos[0].ExcludeReasons)
		}
	}
	// And it must actually be a candidate, confirming the two views agree.
	if got := len(pp.Candidates()); got != 1 {
		t.Fatalf("Candidates() = %d, want 1 (private peer is dialable in dev/LAN mode)", got)
	}
}

func TestKnownPeers_PrivatePeerForbiddenInPublicMode(t *testing.T) {
	// Default (public) mode: a private peer is never an automatic candidate,
	// so KnownPeers() must report it forbidden — matching Candidates().
	cfg := testProviderConfig()
	cfg.AllowPrivateCandidates = false
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("10.0.0.1:64646"), domain.PeerSourceBootstrap)

	infos := pp.KnownPeers()
	if len(infos) != 1 {
		t.Fatalf("KnownPeers() returned %d entries, want 1", len(infos))
	}
	forbidden := false
	for _, r := range infos[0].ExcludeReasons {
		if r == domain.ExcludeReasonForbidden {
			forbidden = true
		}
	}
	if !forbidden {
		t.Fatalf("private peer not reported forbidden in public mode: %v", infos[0].ExcludeReasons)
	}
	if got := len(pp.Candidates()); got != 0 {
		t.Fatalf("Candidates() = %d, want 0 (private peer excluded in public mode)", got)
	}
}

func TestCandidates_AllowSameSubnetFlagDisablesFilter(t *testing.T) {
	pp := NewPeerProvider(testProviderConfig()) // AllowSameSubnetCandidates: true
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.3.5:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("Candidates() returned %d entries, want 2 (filter disabled)", len(candidates))
	}
}

func TestKnownPeers_SameSubnetExcludeReason(t *testing.T) {
	cfg := subnetFilterConfig()
	cfg.HealthFn = scoreHealthFn(map[domain.PeerAddress]int{
		mustAddr("1.2.3.4:64646"): 10,
		mustAddr("1.2.3.5:64646"): 5,
	})
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.3.5:64646"), domain.PeerSourceBootstrap)

	reasons := make(map[domain.PeerAddress][]domain.ExcludeReason)
	for _, info := range pp.KnownPeers() {
		reasons[info.Address] = info.ExcludeReasons
	}

	if got := reasons[mustAddr("1.2.3.4:64646")]; len(got) != 0 {
		t.Errorf("winner 1.2.3.4 has ExcludeReasons %v, want none", got)
	}
	loser := reasons[mustAddr("1.2.3.5:64646")]
	if len(loser) != 1 || loser[0] != domain.ExcludeReasonSameSubnet {
		t.Errorf("loser 1.2.3.5 has ExcludeReasons %v, want [same_subnet]", loser)
	}
}

func TestKnownPeers_SubnetOccupiedByConnectionMarked(t *testing.T) {
	cfg := subnetFilterConfig()
	cfg.ConnectedFn = func() map[string]struct{} {
		return map[string]struct{}{"1.2.3.9": {}}
	}
	pp := NewPeerProvider(cfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	infos := pp.KnownPeers()
	if len(infos) != 1 {
		t.Fatalf("KnownPeers() returned %d entries, want 1", len(infos))
	}
	got := infos[0].ExcludeReasons
	if len(got) != 1 || got[0] != domain.ExcludeReasonSameSubnet {
		t.Errorf("ExcludeReasons = %v, want [same_subnet] (subnet held by connected 1.2.3.9)", got)
	}
}

// TestCM_ManualPeerBypassesSubnetFilter pins the add_peer exception
// end-to-end: fill() takes only one candidate from a shared /24, while
// ManualPeerRequested (the add_peer path) creates a slot in the same /24
// because it bypasses Candidates() entirely.
func TestCM_ManualPeerBypassesSubnetFilter(t *testing.T) {
	t.Parallel()

	b := testCMConfig()
	ppCfg := subnetFilterConfig()
	ppCfg.QueuedFn = func() map[string]struct{} {
		if cm := b.cmPtr.Load(); cm != nil {
			return cm.QueuedIPs()
		}
		return nil
	}
	pp := NewPeerProvider(ppCfg)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("1.2.3.5:64646"), domain.PeerSourceBootstrap)
	b.Cfg.Provider = pp

	cm := b.Build()
	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	waitFor(t, 5*time.Second, "first slot active", func() bool {
		return cm.ActiveCount() == 1
	})
	if got := cm.SlotCount(); got != 1 {
		t.Fatalf("SlotCount() = %d, want 1 (second candidate from 1.2.3.0/24 must be skipped)", got)
	}

	manual := mustAddr("1.2.3.5:64646")
	cm.EmitSlot(ManualPeerRequested{
		Address:       manual,
		DialAddresses: []domain.PeerAddress{manual},
	})

	waitFor(t, 5*time.Second, "manual same-subnet peer active", func() bool {
		return cm.ActiveCount() == 2
	})
}

// TestStartupBootstrapPeersRespectSubnetFilter pins the contract that
// startup bootstrap priming does NOT take the operator-only
// ManualPeerRequested bypass: a default peer list with several nodes in
// one /24 must produce exactly one outbound slot, because the dials flow
// through fill() → PeerProvider.Candidates() with the subnet-diversity
// gate active (production provider config). An operator add_peer for the
// second host afterwards still bypasses the gate.
func TestStartupBootstrapPeersRespectSubnetFilter(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:  "127.0.0.1:64646",
		PeersStatePath: filepath.Join(t.TempDir(), "peers.json"),
		BootstrapPeers: []string{
			"1.2.3.4:64646",
			"1.2.3.5:64646",
			"1.2.3.6:64646",
		},
	}, id, nil)

	// Replace the production CM (never started in this test) with one
	// that shares the Service's production-config PeerProvider but uses
	// a fake DialFn — slot accounting is all this test observes.
	dialFn, _ := fakeDialFn()
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 8 },
		Provider:   svc.peerProvider,
		DialFn:     dialFn,
	})
	svc.connManager = cm
	cancel := runCM(cm)
	defer cancel()

	// Startup priming BEFORE bootstrap-ready — mirrors Run() ordering.
	// A regression to ManualPeerRequested would create three slots here
	// synchronously, regardless of the bootstrap gate.
	svc.primeStartupBootstrapPeers()
	cm.NotifyBootstrapReady()

	waitFor(t, 5*time.Second, "first bootstrap slot", func() bool {
		return cm.SlotCount() >= 1
	})
	// Settle: give the event loop time to process any stray slot events.
	time.Sleep(100 * time.Millisecond)
	if got := cm.SlotCount(); got != 1 {
		t.Fatalf("SlotCount() = %d, want 1 (bootstrap peers share 1.2.3.0/24 and must be gated)", got)
	}

	// Operator add_peer remains the only bypass. Target a fourth host in
	// the same /24 that is NOT in the bootstrap list, so it cannot collide
	// with the (non-deterministic) bootstrap winner's slot via the exact /
	// same-IP dedup in handleManualPeer — the point under test is the
	// subnet bypass, not dedup behaviour.
	resp := svc.addPeerFrame(protocol.Frame{Type: "add_peer", Peers: []string{"1.2.3.99:64646"}})
	if resp.Type == "error" {
		t.Fatalf("addPeerFrame failed: %s", resp.Error)
	}
	waitFor(t, 5*time.Second, "operator same-subnet slot", func() bool {
		return cm.SlotCount() == 2
	})
}

// TestStartupBootstrapPeerKeepsBootstrapSemantics pins that bootstrap
// priming reuses the add-peer bookkeeping WITHOUT the operator-only
// semantics: the peer is stamped Source=Bootstrap (not Manual) and its
// pre-existing automated penalty (a ban) is left intact. A regression to
// the operator path would stamp Manual and clear the ban.
func TestStartupBootstrapPeerKeepsBootstrapSemantics(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate identity failed: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:  "127.0.0.1:64646",
		PeersStatePath: filepath.Join(t.TempDir(), "peers.json"),
		BootstrapPeers: []string{"1.2.3.4:64646"},
	}, id, nil)

	// CM with a fake dialer — priming must not panic on a nil CM and the
	// hint path needs a running loop, but no real dial is required.
	dialFn, _ := fakeDialFn()
	cm := NewConnectionManager(ConnectionManagerConfig{
		MaxSlotsFn: func() int { return 8 },
		Provider:   svc.peerProvider,
		DialFn:     dialFn,
	})
	svc.connManager = cm
	cancel := runCM(cm)
	defer cancel()

	// Seed a pre-existing ban on the bootstrap address. Operator override
	// would clear this; bootstrap priming must preserve it.
	banUntil := time.Now().Add(1 * time.Hour)
	addr := domain.PeerAddress("1.2.3.4:64646")
	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{Address: addr, BannedUntil: banUntil, ConsecutiveFailures: 5}
	svc.peerMu.Unlock()

	svc.primeStartupBootstrapPeers()

	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()

	pm := svc.persistedMeta[addr]
	if pm == nil {
		t.Fatalf("bootstrap peer %s not registered in persistedMeta", addr)
	}
	if pm.Source != domain.PeerSourceBootstrap {
		t.Errorf("Source = %q, want %q (bootstrap must not masquerade as operator add)", pm.Source, domain.PeerSourceBootstrap)
	}
	h := svc.health[addr]
	if h == nil || !h.BannedUntil.Equal(banUntil) {
		t.Errorf("BannedUntil = %v, want %v (bootstrap must not clear automated penalties)", h, banUntil)
	}
}
