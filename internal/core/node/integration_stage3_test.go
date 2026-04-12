package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildTestServiceWithCM creates a minimal Service with a running CM and PP
// for testing buildPeerExchangeResponse.
func buildTestServiceWithCM(t *testing.T, addresses []string, maxSlots int) (*Service, *ConnectionManager, *PeerProvider, context.CancelFunc) {
	t.Helper()

	b := testCMConfig(addresses...)
	b.Cfg.MaxSlotsFn = func() int { return maxSlots }

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()

	svc := &Service{
		connManager:  cm,
		peerProvider: b.Cfg.Provider,
		cfg:          config.Node{MaxOutgoingPeers: maxSlots},
	}

	cancel := runCM(cm)
	cm.NotifyBootstrapReady()

	return svc, cm, b.Cfg.Provider, cancel
}

// collectAddresses returns sorted addresses from buildPeerExchangeResponse.
func collectAddresses(result []domain.PeerAddress) []string {
	out := make([]string, len(result))
	for i, a := range result {
		out[i] = string(a)
	}
	return out
}

// slotActive waits for a specific slot to reach active state.
func slotActive(t *testing.T, cm *ConnectionManager, addr string) {
	t.Helper()
	waitFor(t, 2*time.Second, "slot "+addr+" active", func() bool {
		for _, s := range cm.Slots() {
			if string(s.Address) == addr && s.State == "active" {
				return true
			}
		}
		return false
	})
}

// testServiceConfig returns a minimal config for Service field initialization.
func testServiceConfig() config.Node {
	return config.Node{
		MaxOutgoingPeers: 8,
	}
}

// ---------------------------------------------------------------------------
// Tests: buildPeerExchangeResponse
// ---------------------------------------------------------------------------

func TestBuildPeerExchange_ActivePriorityOverCandidate(t *testing.T) {
	// Active slot on IP 1.2.3.4:9000 + candidate 1.2.3.4:9001 (different port)
	// → result contains :9000 (active wins regardless of score).
	svc, cm, pp, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")

	// Add a candidate on the same IP but different port.
	pp.Add(mustAddr("1.2.3.4:9001"), domain.PeerSourcePeerExchange)

	result := svc.buildPeerExchangeResponse(nil)
	addrs := collectAddresses(result)

	// Should contain only :9000 (active), not :9001.
	found9000 := false
	found9001 := false
	for _, a := range addrs {
		if a == "1.2.3.4:9000" {
			found9000 = true
		}
		if a == "1.2.3.4:9001" {
			found9001 = true
		}
	}
	if !found9000 {
		t.Errorf("expected 1.2.3.4:9000 (active) in result, got %v", addrs)
	}
	if found9001 {
		t.Errorf("expected 1.2.3.4:9001 (candidate) to be suppressed by active, got %v", addrs)
	}
}

func TestBuildPeerExchange_NoDuplicatesSameAddress(t *testing.T) {
	// Active 1.2.3.4:9000 + candidate 1.2.3.4:9000 → one element only.
	svc, cm, _, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")

	result := svc.buildPeerExchangeResponse(nil)
	count := 0
	for _, a := range result {
		if string(a) == "1.2.3.4:9000" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 entry for 1.2.3.4:9000, got %d in %v", count, collectAddresses(result))
	}
}

func TestBuildPeerExchange_NoDuplicatesMultipleIPs(t *testing.T) {
	// Two active slots on different IPs + candidates with same IPs → each IP once.
	svc, cm, pp, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000", "5.6.7.8:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")
	slotActive(t, cm, "5.6.7.8:9000")

	// Candidates with same IPs.
	pp.Add(mustAddr("1.2.3.4:9999"), domain.PeerSourcePeerExchange)
	pp.Add(mustAddr("5.6.7.8:9999"), domain.PeerSourcePeerExchange)

	result := svc.buildPeerExchangeResponse(nil)
	ipSeen := make(map[string]int)
	for _, a := range result {
		ip, _, ok := splitHostPort(string(a))
		if ok {
			ipSeen[ip]++
		}
	}
	for ip, count := range ipSeen {
		if count > 1 {
			t.Errorf("IP %s appears %d times, expected 1", ip, count)
		}
	}
}

func TestBuildPeerExchange_FilterByNetworkGroups(t *testing.T) {
	svc, cm, pp, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")
	pp.Add(mustAddr("5.6.7.8:9000"), domain.PeerSourcePeerExchange)

	// Caller supports IPv4 → both should appear.
	groups := map[domain.NetGroup]struct{}{domain.NetGroupIPv4: {}}
	result := svc.buildPeerExchangeResponse(groups)
	if len(result) != 2 {
		t.Errorf("expected 2 peers for IPv4 caller, got %d: %v", len(result), collectAddresses(result))
	}

	// Caller supports only a non-matching group → none should appear.
	groupsOnion := map[domain.NetGroup]struct{}{domain.NetGroupTorV3: {}}
	result = svc.buildPeerExchangeResponse(groupsOnion)
	if len(result) != 0 {
		t.Errorf("expected 0 peers for Tor-only caller, got %d: %v", len(result), collectAddresses(result))
	}
}

func TestBuildPeerExchange_NilCallerGroupsUnfiltered(t *testing.T) {
	svc, cm, pp, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")
	pp.Add(mustAddr("5.6.7.8:9000"), domain.PeerSourcePeerExchange)

	result := svc.buildPeerExchangeResponse(nil)
	if len(result) < 2 {
		t.Errorf("expected at least 2 peers for nil callerGroups, got %d: %v", len(result), collectAddresses(result))
	}
}

func TestBuildPeerExchange_EmptySlotsEmptyCandidates(t *testing.T) {
	svc, _, _, cancel := buildTestServiceWithCM(t, nil, 8)
	defer cancel()

	result := svc.buildPeerExchangeResponse(nil)
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", collectAddresses(result))
	}
}

func TestBuildPeerExchange_OnlyActiveNoCandidate(t *testing.T) {
	svc, cm, _, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")

	result := svc.buildPeerExchangeResponse(nil)
	addrs := collectAddresses(result)
	if len(addrs) != 1 || addrs[0] != "1.2.3.4:9000" {
		t.Errorf("expected [1.2.3.4:9000], got %v", addrs)
	}
}

func TestBuildPeerExchange_OnlyCandidatesNoActive(t *testing.T) {
	ppCfg := testProviderConfig()
	pp := NewPeerProvider(ppCfg)
	pp.Add(mustAddr("1.2.3.4:9000"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:9000"), domain.PeerSourceBootstrap)

	svc := &Service{
		connManager:  nil,
		peerProvider: pp,
	}

	result := svc.buildPeerExchangeResponse(nil)
	if len(result) != 2 {
		t.Errorf("expected 2 candidates, got %d: %v", len(result), collectAddresses(result))
	}
}

func TestBuildPeerExchange_HidesLoopbackAndRFC1918IPv4(t *testing.T) {
	ppCfg := testProviderConfig()
	pp := NewPeerProvider(ppCfg)
	pp.Add(mustAddr("127.0.0.1:9000"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("10.1.2.3:9000"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("172.16.5.9:9000"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("192.168.1.10:9000"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("8.8.8.8:9000"), domain.PeerSourceBootstrap)

	svc := &Service{
		connManager:  nil,
		peerProvider: pp,
	}

	result := collectAddresses(svc.buildPeerExchangeResponse(nil))
	if len(result) != 1 {
		t.Fatalf("expected only public peer in peer exchange response, got %v", result)
	}
	if result[0] != "8.8.8.8:9000" {
		t.Fatalf("unexpected peer exchange result %v", result)
	}
}

// ---------------------------------------------------------------------------
// Tests: RPC wire schema — ConnectionDiagnosticProvider
// ---------------------------------------------------------------------------

func TestActivePeersJSON_Shape(t *testing.T) {
	svc, cm, _, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")

	data, err := svc.ActivePeersJSON()
	if err != nil {
		t.Fatalf("ActivePeersJSON: %v", err)
	}

	var resp struct {
		Slots    []json.RawMessage `json:"slots"`
		Count    int               `json:"count"`
		MaxSlots int               `json:"max_slots"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Count != 1 {
		t.Errorf("expected count=1, got %d", resp.Count)
	}
	if resp.MaxSlots != 8 {
		t.Errorf("expected max_slots=8, got %d", resp.MaxSlots)
	}
	if len(resp.Slots) != 1 {
		t.Fatalf("expected 1 slot, got %d", len(resp.Slots))
	}

	var slot struct {
		Address          string   `json:"address"`
		State            string   `json:"state"`
		RetryCount       int      `json:"retry_count"`
		Generation       uint64   `json:"generation"`
		Identity         *string  `json:"identity"`
		DialAddresses    []string `json:"dial_addresses"`
		ConnectedAddress *string  `json:"connected_address"`
	}
	if err := json.Unmarshal(resp.Slots[0], &slot); err != nil {
		t.Fatalf("unmarshal slot: %v", err)
	}
	if slot.Address != "1.2.3.4:9000" {
		t.Errorf("expected address 1.2.3.4:9000, got %s", slot.Address)
	}
	if slot.State != "active" {
		t.Errorf("expected state active, got %s", slot.State)
	}
}

func TestListPeersJSON_Shape(t *testing.T) {
	ppCfg := testProviderConfig()
	pp := NewPeerProvider(ppCfg)
	pp.Add(mustAddr("1.2.3.4:9000"), domain.PeerSourceBootstrap)

	svc := &Service{
		peerProvider: pp,
		cfg:          testServiceConfig(),
	}

	data, err := svc.ListPeersJSON()
	if err != nil {
		t.Fatalf("ListPeersJSON: %v", err)
	}

	var resp struct {
		Peers []json.RawMessage `json:"peers"`
		Count int               `json:"count"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Count != 1 {
		t.Errorf("expected count=1, got %d", resp.Count)
	}
	if len(resp.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(resp.Peers))
	}

	var peer struct {
		Address        string   `json:"address"`
		Source         string   `json:"source"`
		AddedAt        string   `json:"added_at"`
		Network        string   `json:"network"`
		Score          int      `json:"score"`
		Failures       int      `json:"failures"`
		BannedUntil    string   `json:"banned_until"`
		Connected      bool     `json:"connected"`
		ExcludeReasons []string `json:"exclude_reasons"`
	}
	if err := json.Unmarshal(resp.Peers[0], &peer); err != nil {
		t.Fatalf("unmarshal peer: %v", err)
	}
	if peer.Address != "1.2.3.4:9000" {
		t.Errorf("expected address 1.2.3.4:9000, got %s", peer.Address)
	}
	if peer.Source != string(domain.PeerSourceBootstrap) {
		t.Errorf("expected source bootstrap, got %s", peer.Source)
	}
}

func TestListBannedJSON_Shape(t *testing.T) {
	bannedIPs := map[string]domain.BannedIPEntry{
		"1.2.3.4": {BannedUntil: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
	}
	ppCfg := testProviderConfig()
	ppCfg.BannedIPsFn = func() map[string]domain.BannedIPEntry { return bannedIPs }
	ppCfg.HealthFn = func(addr domain.PeerAddress) *PeerHealthView {
		return &PeerHealthView{
			BannedUntil: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		}
	}
	pp := NewPeerProvider(ppCfg)
	pp.Add(mustAddr("1.2.3.4:9000"), domain.PeerSourceBootstrap)

	svc := &Service{
		peerProvider: pp,
		cfg:          testServiceConfig(),
	}

	data, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatalf("ListBannedJSON: %v", err)
	}

	var resp struct {
		BannedIPs []json.RawMessage `json:"banned_ips"`
		Count     int               `json:"count"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.Count != len(resp.BannedIPs) {
		t.Errorf("count (%d) != len(banned_ips) (%d)", resp.Count, len(resp.BannedIPs))
	}
}

func TestActivePeersJSON_EmptyState(t *testing.T) {
	svc, _, _, cancel := buildTestServiceWithCM(t, nil, 8)
	defer cancel()

	data, err := svc.ActivePeersJSON()
	if err != nil {
		t.Fatalf("ActivePeersJSON: %v", err)
	}

	var resp struct {
		Slots    []json.RawMessage `json:"slots"`
		Count    int               `json:"count"`
		MaxSlots int               `json:"max_slots"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected count=0, got %d", resp.Count)
	}
	if resp.Slots == nil {
		t.Error("expected empty array, got nil")
	}
}

func TestListPeersJSON_EmptyState(t *testing.T) {
	ppCfg := testProviderConfig()
	pp := NewPeerProvider(ppCfg)
	svc := &Service{peerProvider: pp, cfg: testServiceConfig()}

	data, err := svc.ListPeersJSON()
	if err != nil {
		t.Fatalf("ListPeersJSON: %v", err)
	}

	var resp struct {
		Peers []json.RawMessage `json:"peers"`
		Count int               `json:"count"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected count=0, got %d", resp.Count)
	}
}

func TestListBannedJSON_EmptyState(t *testing.T) {
	ppCfg := testProviderConfig()
	pp := NewPeerProvider(ppCfg)
	svc := &Service{peerProvider: pp, cfg: testServiceConfig()}

	data, err := svc.ListBannedJSON()
	if err != nil {
		t.Fatalf("ListBannedJSON: %v", err)
	}

	var resp struct {
		BannedIPs []json.RawMessage `json:"banned_ips"`
		Count     int               `json:"count"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected count=0, got %d", resp.Count)
	}
}

// ---------------------------------------------------------------------------
// Tests: initializing slots excluded from peer exchange
// ---------------------------------------------------------------------------

// TestBuildPeerExchange_InitializingSlotExcluded verifies that a slot in
// "initializing" state is NOT advertised via buildPeerExchangeResponse.
func TestBuildPeerExchange_InitializingSlotExcluded(t *testing.T) {
	b := testCMConfig("1.2.3.4:9000")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	// Do NOT emit SessionInitReady — slot stays initializing.
	b.Cfg.OnSessionEstablished = func(SessionInfo) {}

	dialFn, _ := fakeDialFn()
	b.Cfg.DialFn = dialFn

	cm := b.Build()

	svc := &Service{
		connManager:  cm,
		peerProvider: b.Cfg.Provider,
		cfg:          config.Node{MaxOutgoingPeers: 1},
	}

	cancel := runCM(cm)
	defer cancel()

	cm.NotifyBootstrapReady()

	// Wait for slot to appear as initializing.
	waitFor(t, 2*time.Second, "initializing", func() bool {
		for _, s := range cm.Slots() {
			if s.State == "initializing" {
				return true
			}
		}
		return false
	})

	// buildPeerExchangeResponse should NOT include the initializing slot.
	result := svc.buildPeerExchangeResponse(nil)
	for _, addr := range result {
		if string(addr) == "1.2.3.4:9000" {
			t.Fatalf("initializing slot 1.2.3.4:9000 should not appear in peer exchange, got %v", collectAddresses(result))
		}
	}
}
