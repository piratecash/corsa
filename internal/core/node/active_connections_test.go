package node

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// activeConnTestService creates a minimal Service suitable for
// ActiveConnectionsJSON unit tests. Health, sessions, peerIDs and the
// conn registry are populated directly — no CM or network I/O required.
//
// Primes the peer_health and cm_slots snapshots at construction so the
// hot-path handler (peerHealthFrames, invoked by ActiveConnectionsJSON)
// observes a non-nil snapshot on first call.  Production code gets the
// same invariant from primeHotReadSnapshots() in Run(); bypassing Run()
// means the test must prime explicitly — there is no synchronous
// fallback rebuild on the RPC path (see peer_health_snapshot.go docstring).
// Tests that mutate state after construction must call
// primeActiveConnSnapshots(svc) again before invoking ActiveConnectionsJSON
// so the snapshots reflect the new state.
func activeConnTestService(t *testing.T) *Service {
	t.Helper()
	svc := &Service{
		health:          make(map[domain.PeerAddress]*peerHealth),
		sessions:        make(map[domain.PeerAddress]*peerSession),
		peerIDs:         make(map[domain.PeerAddress]domain.PeerIdentity),
		peerVersions:    make(map[domain.PeerAddress]string),
		peerBuilds:      make(map[domain.PeerAddress]int),
		conns:           make(map[domain.ConnID]*connEntry),
		connIDByNetConn: make(map[net.Conn]netcore.ConnID),
		pending:         make(map[domain.PeerAddress][]pendingFrame),
		cfg:             config.Node{MaxOutgoingPeers: 8},
	}
	primeActiveConnSnapshots(svc)
	return svc
}

// primeActiveConnSnapshots rebuilds the two snapshots read by peerHealthFrames
// (and therefore ActiveConnectionsJSON): peer_health and cm_slots.  Called
// from activeConnTestService after construction and from the seeding helpers
// after they mutate state.  Tests that do direct map mutations between
// helper calls and the RPC assertion must invoke this explicitly — there is
// no synchronous fallback rebuild on the hot path.
func primeActiveConnSnapshots(svc *Service) {
	svc.rebuildPeerHealthSnapshot()
	svc.rebuildCMSlotsSnapshot()
}

// testInboundCore creates a real netcore.NetCore for an inbound connection
// and returns a cleanup function that closes the underlying pipe.
func testInboundCore(t *testing.T, addr domain.PeerAddress, identity domain.PeerIdentity, connID domain.ConnID) (*netcore.NetCore, func()) {
	t.Helper()
	server, client := net.Pipe()
	core := netcore.New(connID, server, netcore.Inbound, netcore.Options{
		Address:  addr,
		Identity: identity,
	})
	cleanup := func() {
		core.Close()
		_ = client.Close()
	}
	return core, cleanup
}

// addOutboundPeer populates health + session entries for a connected
// outbound peer directly in the Service internal maps, then refreshes the
// hot-read snapshots so peerHealthFrames sees the seeded state.
func addOutboundPeer(svc *Service, addr string, identity string, connID domain.ConnID) {
	pa := domain.PeerAddress(addr)
	now := time.Now().UTC()
	svc.health[pa] = &peerHealth{
		Address:             pa,
		Connected:           true,
		Direction:           peerDirectionOutbound,
		State:               peerStateHealthy,
		LastConnectedAt:     now,
		LastUsefulReceiveAt: now,
	}
	svc.sessions[pa] = &peerSession{
		address:      pa,
		peerIdentity: domain.PeerIdentity(identity),
		connID:       connID,
	}
	svc.peerIDs[pa] = domain.PeerIdentity(identity)
	primeActiveConnSnapshots(svc)
}

// addInboundPeer populates health entry and registers a real NetCore in the
// conn registry so inboundConnIDsLocked can find it, then refreshes the
// hot-read snapshots so peerHealthFrames sees the seeded state.
func addInboundPeer(t *testing.T, svc *Service, addr string, identity string, connID domain.ConnID) func() {
	t.Helper()
	pa := domain.PeerAddress(addr)
	now := time.Now().UTC()
	svc.health[pa] = &peerHealth{
		Address:             pa,
		Connected:           true,
		Direction:           peerDirectionInbound,
		State:               peerStateHealthy,
		LastConnectedAt:     now,
		LastUsefulReceiveAt: now,
	}
	svc.peerIDs[pa] = domain.PeerIdentity(identity)

	core, cleanup := testInboundCore(t, pa, domain.PeerIdentity(identity), connID)
	svc.conns[connID] = &connEntry{core: core}
	primeActiveConnSnapshots(svc)
	return cleanup
}

// addDisconnectedPeer adds a health entry with Connected=false and
// refreshes the hot-read snapshots so peerHealthFrames sees the seeded
// state (or, more precisely, continues to exclude it — disconnected peers
// never appear in ActiveConnectionsJSON, but the rebuild keeps the rest
// of the snapshot current for tests that mix disconnected and connected
// entries).
func addDisconnectedPeer(svc *Service, addr string) {
	pa := domain.PeerAddress(addr)
	svc.health[pa] = &peerHealth{
		Address:   pa,
		Connected: false,
		Direction: peerDirectionOutbound,
		State:     peerStateReconnecting,
	}
	primeActiveConnSnapshots(svc)
}

// ---------------------------------------------------------------------------
// Parsed response types used across tests
// ---------------------------------------------------------------------------

type activeConnResp struct {
	Version     int               `json:"version"`
	Connections []activeConnEntry `json:"connections"`
	Count       int               `json:"count"`
}

type activeConnEntry struct {
	PeerAddress   string `json:"peer_address"`
	RemoteAddress string `json:"remote_address"`
	Identity      string `json:"identity"`
	Direction     string `json:"direction"`
	Network       string `json:"network"`
	State         string `json:"state"`
	ConnID        uint64 `json:"conn_id"`
	SlotState     string `json:"slot_state,omitempty"`
}

func parseActiveConns(t *testing.T, data json.RawMessage) activeConnResp {
	t.Helper()
	var resp activeConnResp
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal ActiveConnectionsJSON: %v", err)
	}
	return resp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestActiveConnectionsJSON_EmptySnapshot(t *testing.T) {
	svc := activeConnTestService(t)
	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}

	// Exact stable shape per design doc §11.1.
	want := `{"version":1,"connections":[],"count":0}`
	if string(data) != want {
		t.Errorf("empty snapshot mismatch:\n got: %s\nwant: %s", string(data), want)
	}
}

func TestActiveConnectionsJSON_InboundOnlyAppears(t *testing.T) {
	svc := activeConnTestService(t)
	cleanup := addInboundPeer(t, svc, "10.0.0.1:9000", "aaa111", domain.ConnID(100))
	defer cleanup()

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Count != 1 {
		t.Fatalf("expected count=1, got %d", resp.Count)
	}
	c := resp.Connections[0]
	if c.Direction != "inbound" {
		t.Errorf("expected direction=inbound, got %s", c.Direction)
	}
	if c.PeerAddress != "10.0.0.1:9000" {
		t.Errorf("expected peer_address=10.0.0.1:9000, got %s", c.PeerAddress)
	}
	if c.Identity != "aaa111" {
		t.Errorf("expected identity=aaa111, got %s", c.Identity)
	}
	if c.ConnID != 100 {
		t.Errorf("expected conn_id=100, got %d", c.ConnID)
	}
}

func TestActiveConnectionsJSON_OutboundWithSlotState(t *testing.T) {
	svc, cm, _, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")

	// buildTestServiceWithCM does not populate health/sessions; the CM-only
	// test helper skips markPeerConnected. Populate manually so that
	// peerHealthFrames() sees the outbound peer.
	svc.mu.Lock()
	if svc.health == nil {
		svc.health = make(map[domain.PeerAddress]*peerHealth)
	}
	if svc.sessions == nil {
		svc.sessions = make(map[domain.PeerAddress]*peerSession)
	}
	if svc.peerIDs == nil {
		svc.peerIDs = make(map[domain.PeerAddress]domain.PeerIdentity)
	}
	if svc.conns == nil {
		svc.conns = make(map[domain.ConnID]*connEntry)
	}
	now := time.Now().UTC()
	pa := domain.PeerAddress("1.2.3.4:9000")
	svc.health[pa] = &peerHealth{
		Address:             pa,
		Connected:           true,
		Direction:           peerDirectionOutbound,
		State:               peerStateHealthy,
		LastConnectedAt:     now,
		LastUsefulReceiveAt: now,
	}
	svc.sessions[pa] = &peerSession{
		address:      pa,
		peerIdentity: "id-1.2.3.4:9000",
		connID:       domain.ConnID(1),
	}
	svc.peerIDs[pa] = "id-1.2.3.4:9000"
	svc.mu.Unlock()

	// Post-mutation prime: the snapshots are loaded atomically by the hot
	// path and never synchronously rebuilt, so direct map writes are only
	// visible once we rebuild explicitly.
	primeActiveConnSnapshots(svc)

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Count != 1 {
		t.Fatalf("expected count=1, got %d", resp.Count)
	}
	c := resp.Connections[0]
	if c.Direction != "outbound" {
		t.Errorf("expected direction=outbound, got %s", c.Direction)
	}
	if c.SlotState != "active" {
		t.Errorf("expected slot_state=active, got %q", c.SlotState)
	}
	if c.PeerAddress != "1.2.3.4:9000" {
		t.Errorf("expected peer_address=1.2.3.4:9000, got %s", c.PeerAddress)
	}
}

func TestActiveConnectionsJSON_DisconnectedExcluded(t *testing.T) {
	svc := activeConnTestService(t)
	addDisconnectedPeer(svc, "10.0.0.2:9000")

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Count != 0 {
		t.Errorf("disconnected peer should not appear, got count=%d", resp.Count)
	}
}

func TestActiveConnectionsJSON_QueuedDialingExcluded(t *testing.T) {
	// Slot in queued/dialing state without established connection should
	// not appear in active connections. Use testCMConfig but block the
	// dial function so the slot stays in dialing state.
	b := testCMConfig("10.0.0.3:9000")
	b.Cfg.MaxSlotsFn = func() int { return 1 }

	dialBlock := make(chan struct{})
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		<-dialBlock
		return DialResult{}, net.ErrClosed
	}
	cm := b.Build()
	svc := &Service{
		connManager:  cm,
		peerProvider: b.Cfg.Provider,
		cfg:          config.Node{MaxOutgoingPeers: 1},
	}

	cancel := runCM(cm)
	defer func() {
		close(dialBlock)
		cancel()
	}()
	cm.NotifyBootstrapReady()

	// Wait a moment for the slot to start dialing.
	time.Sleep(50 * time.Millisecond)

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Count != 0 {
		t.Errorf("queued/dialing slot should not appear, got count=%d", resp.Count)
	}
}

func TestActiveConnectionsJSON_DirectionCorrect(t *testing.T) {
	svc := activeConnTestService(t)
	addOutboundPeer(svc, "1.2.3.4:9000", "out111", domain.ConnID(10))
	cleanup := addInboundPeer(t, svc, "5.6.7.8:9000", "in222", domain.ConnID(20))
	defer cleanup()

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Count != 2 {
		t.Fatalf("expected count=2, got %d", resp.Count)
	}

	// Outbound should come first per sort order.
	if resp.Connections[0].Direction != "outbound" {
		t.Errorf("first entry should be outbound, got %s", resp.Connections[0].Direction)
	}
	if resp.Connections[1].Direction != "inbound" {
		t.Errorf("second entry should be inbound, got %s", resp.Connections[1].Direction)
	}
}

func TestActiveConnectionsJSON_NetworkClassification(t *testing.T) {
	svc := activeConnTestService(t)
	addOutboundPeer(svc, "1.2.3.4:9000", "ipv4peer", domain.ConnID(10))
	cleanup := addInboundPeer(t, svc, "127.0.0.1:9000", "localpeer", domain.ConnID(20))
	defer cleanup()

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	netByAddr := make(map[string]string)
	for _, c := range resp.Connections {
		netByAddr[c.PeerAddress] = c.Network
	}
	if netByAddr["1.2.3.4:9000"] != "ipv4" {
		t.Errorf("expected network=ipv4 for 1.2.3.4, got %s", netByAddr["1.2.3.4:9000"])
	}
	if netByAddr["127.0.0.1:9000"] != "local" {
		t.Errorf("expected network=local for 127.0.0.1, got %s", netByAddr["127.0.0.1:9000"])
	}
}

func TestActiveConnectionsJSON_OverlayClassification(t *testing.T) {
	svc := activeConnTestService(t)
	// Tor v3 address (56-char base32 + .onion)
	torAddr := "abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrst2345.onion:9000"
	cleanup1 := addInboundPeer(t, svc, torAddr, "torpeer", domain.ConnID(30))
	defer cleanup1()

	// I2P address
	i2pAddr := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:9000"
	cleanup2 := addInboundPeer(t, svc, i2pAddr, "i2ppeer", domain.ConnID(31))
	defer cleanup2()

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	netByAddr := make(map[string]string)
	for _, c := range resp.Connections {
		netByAddr[c.PeerAddress] = c.Network
	}
	if netByAddr[torAddr] != "torv3" {
		t.Errorf("expected network=torv3 for tor address, got %s", netByAddr[torAddr])
	}
	if netByAddr[i2pAddr] != "i2p" {
		t.Errorf("expected network=i2p for i2p address, got %s", netByAddr[i2pAddr])
	}
}

func TestActiveConnectionsJSON_NoConnectedField(t *testing.T) {
	svc := activeConnTestService(t)
	addOutboundPeer(svc, "1.2.3.4:9000", "peer1", domain.ConnID(10))

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}

	// Parse as raw map to verify "connected" field is absent.
	var raw struct {
		Connections []map[string]interface{} `json:"connections"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if len(raw.Connections) != 1 {
		t.Fatalf("expected 1 connection, got %d", len(raw.Connections))
	}
	if _, ok := raw.Connections[0]["connected"]; ok {
		t.Error("connected field should not be present in getActiveConnections response")
	}
	// identity must always be present (not omitempty) even when non-empty.
	if _, ok := raw.Connections[0]["identity"]; !ok {
		t.Error("identity field must always be present in getActiveConnections response")
	}
}

// TestActiveConnectionsJSON_IdentityAlwaysPresent verifies that the identity
// field appears in JSON even when the peer identity is an empty string
// (e.g. connection in the window between TCP accept and handshake completion).
func TestActiveConnectionsJSON_IdentityAlwaysPresent(t *testing.T) {
	svc := activeConnTestService(t)
	// Add peer with empty identity to simulate pre-handshake window.
	addOutboundPeer(svc, "1.2.3.4:9000", "", domain.ConnID(10))

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}

	var raw struct {
		Connections []map[string]interface{} `json:"connections"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if len(raw.Connections) != 1 {
		t.Fatalf("expected 1 connection, got %d", len(raw.Connections))
	}
	val, ok := raw.Connections[0]["identity"]
	if !ok {
		t.Fatal("identity field must always be present, even when empty")
	}
	if val != "" {
		t.Errorf("expected empty identity string, got %q", val)
	}
}

func TestActiveConnectionsJSON_VersionField(t *testing.T) {
	svc := activeConnTestService(t)

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Version != 1 {
		t.Errorf("expected version=1, got %d", resp.Version)
	}
}

// TestActiveConnectionsJSON_RegressionActivePeersUnchanged verifies that
// getActivePeers continues to return only CM slots (not mixed with
// inbound connections).
func TestActiveConnectionsJSON_RegressionActivePeersUnchanged(t *testing.T) {
	svc, cm, _, cancel := buildTestServiceWithCM(t, []string{"1.2.3.4:9000"}, 8)
	defer cancel()

	slotActive(t, cm, "1.2.3.4:9000")

	// ActivePeersJSON must still return slot-based data (it reads from CM
	// directly, not from s.health, so no manual population needed).
	peersData, err := svc.ActivePeersJSON()
	if err != nil {
		t.Fatalf("ActivePeersJSON: %v", err)
	}

	var peersResp struct {
		Slots    []json.RawMessage `json:"slots"`
		Count    int               `json:"count"`
		MaxSlots int               `json:"max_slots"`
	}
	if err := json.Unmarshal(peersData, &peersResp); err != nil {
		t.Fatalf("unmarshal ActivePeersJSON: %v", err)
	}
	if peersResp.Count != 1 {
		t.Errorf("ActivePeersJSON count should be 1, got %d", peersResp.Count)
	}
	if peersResp.MaxSlots != 8 {
		t.Errorf("ActivePeersJSON max_slots should be 8, got %d", peersResp.MaxSlots)
	}

	// Verify the slot shape has not changed.
	var slot struct {
		Address string `json:"address"`
		State   string `json:"state"`
	}
	if err := json.Unmarshal(peersResp.Slots[0], &slot); err != nil {
		t.Fatalf("unmarshal slot: %v", err)
	}
	if slot.Address != "1.2.3.4:9000" {
		t.Errorf("slot address should be 1.2.3.4:9000, got %s", slot.Address)
	}
	if slot.State != "active" {
		t.Errorf("slot state should be active, got %s", slot.State)
	}
}

// TestActiveConnectionsJSON_Integration verifies the combined scenario:
// 4 outbound CM slots (active) + 1 inbound live connection.
// getActivePeers.count == 4, getActiveConnections.count == 5.
func TestActiveConnectionsJSON_Integration(t *testing.T) {
	addrs := []string{
		"1.2.3.4:9000",
		"5.6.7.8:9000",
		"9.10.11.12:9000",
		"13.14.15.16:9000",
	}
	svc, cm, _, cancel := buildTestServiceWithCM(t, addrs, 8)
	defer cancel()

	for _, a := range addrs {
		slotActive(t, cm, a)
	}

	// buildTestServiceWithCM does not populate health/sessions; populate
	// manually so peerHealthFrames() can see outbound peers.
	svc.mu.Lock()
	if svc.health == nil {
		svc.health = make(map[domain.PeerAddress]*peerHealth)
	}
	if svc.sessions == nil {
		svc.sessions = make(map[domain.PeerAddress]*peerSession)
	}
	if svc.peerIDs == nil {
		svc.peerIDs = make(map[domain.PeerAddress]domain.PeerIdentity)
	}
	if svc.conns == nil {
		svc.conns = make(map[domain.ConnID]*connEntry)
	}
	now := time.Now().UTC()
	for i, a := range addrs {
		pa := domain.PeerAddress(a)
		svc.health[pa] = &peerHealth{
			Address:             pa,
			Connected:           true,
			Direction:           peerDirectionOutbound,
			State:               peerStateHealthy,
			LastConnectedAt:     now,
			LastUsefulReceiveAt: now,
		}
		id := domain.PeerIdentity("id-" + a)
		svc.sessions[pa] = &peerSession{
			address:      pa,
			peerIdentity: id,
			connID:       domain.ConnID(uint64(i) + 1),
		}
		svc.peerIDs[pa] = id
	}

	// Add an inbound peer directly to health/registry.
	inAddr := domain.PeerAddress("20.20.20.20:9000")
	svc.health[inAddr] = &peerHealth{
		Address:             inAddr,
		Connected:           true,
		Direction:           peerDirectionInbound,
		State:               peerStateHealthy,
		LastConnectedAt:     now,
		LastUsefulReceiveAt: now,
	}
	svc.peerIDs[inAddr] = domain.PeerIdentity("inbound_identity")
	svc.mu.Unlock()

	// Register the inbound core in the conn registry.
	core, coreCleanup := testInboundCore(t, inAddr, domain.PeerIdentity("inbound_identity"), domain.ConnID(999))
	defer coreCleanup()
	func() {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		svc.conns[domain.ConnID(999)] = &connEntry{core: core}
	}()

	// Post-mutation prime so peerHealthFrames observes the seeded state —
	// the hot path never rebuilds synchronously.
	primeActiveConnSnapshots(svc)

	// getActivePeers should return 4 (CM slots only).
	peersData, err := svc.ActivePeersJSON()
	if err != nil {
		t.Fatalf("ActivePeersJSON: %v", err)
	}
	var peersResp struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(peersData, &peersResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if peersResp.Count != 4 {
		t.Errorf("getActivePeers.count should be 4, got %d", peersResp.Count)
	}

	// getActiveConnections should return 5.
	connsData, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, connsData)
	if resp.Count != 5 {
		t.Errorf("getActiveConnections.count should be 5, got %d", resp.Count)
	}

	// Verify inbound entry is present only in getActiveConnections.
	foundInbound := false
	for _, c := range resp.Connections {
		if c.PeerAddress == "20.20.20.20:9000" {
			foundInbound = true
			if c.Direction != "inbound" {
				t.Errorf("inbound peer should have direction=inbound, got %s", c.Direction)
			}
		}
	}
	if !foundInbound {
		t.Error("inbound peer should appear in getActiveConnections")
	}
}

// TestActiveConnectionsJSON_PeerAddressDiffersFromRemote verifies that when
// a CM slot's ConnectedAddress differs from its canonical Address (fallback
// port scenario), peer_address reflects the canonical slot address and
// remote_address reflects the actual connected endpoint.
func TestActiveConnectionsJSON_PeerAddressDiffersFromRemote(t *testing.T) {
	// Build a CM where the DialFn returns a ConnectedAddress on a different
	// port than the slot's canonical address (simulating fallback port dial).
	canonical := "1.2.3.4:9000"
	fallback := "1.2.3.4:9001"

	b := testCMConfig(canonical)
	b.Cfg.MaxSlotsFn = func() int { return 1 }
	b.Cfg.DialFn = func(_ context.Context, addrs []domain.PeerAddress) (DialResult, error) {
		session := fakePeerSession(domain.PeerAddress(fallback), "id-fallback")
		return DialResult{
			Session:          session,
			ConnectedAddress: domain.PeerAddress(fallback),
		}, nil
	}
	cm := b.Build()
	svc := &Service{
		connManager:  cm,
		peerProvider: b.Cfg.Provider,
		cfg:          config.Node{MaxOutgoingPeers: 1},
	}

	cancel := runCM(cm)
	defer cancel()
	cm.NotifyBootstrapReady()

	slotActive(t, cm, canonical)

	// Populate health/sessions manually (buildTestServiceWithCM skips this).
	svc.mu.Lock()
	svc.health = make(map[domain.PeerAddress]*peerHealth)
	svc.sessions = make(map[domain.PeerAddress]*peerSession)
	svc.peerIDs = make(map[domain.PeerAddress]domain.PeerIdentity)
	svc.conns = make(map[domain.ConnID]*connEntry)
	now := time.Now().UTC()
	pa := domain.PeerAddress(canonical)
	svc.health[pa] = &peerHealth{
		Address:             pa,
		Connected:           true,
		Direction:           peerDirectionOutbound,
		State:               peerStateHealthy,
		LastConnectedAt:     now,
		LastUsefulReceiveAt: now,
	}
	svc.sessions[pa] = &peerSession{
		address:      pa,
		peerIdentity: "id-fallback",
		connID:       domain.ConnID(1),
	}
	svc.peerIDs[pa] = "id-fallback"
	svc.mu.Unlock()

	// Post-mutation prime so peerHealthFrames observes the seeded state —
	// the hot path never rebuilds synchronously.
	primeActiveConnSnapshots(svc)

	data, err := svc.ActiveConnectionsJSON()
	if err != nil {
		t.Fatalf("ActiveConnectionsJSON: %v", err)
	}
	resp := parseActiveConns(t, data)

	if resp.Count != 1 {
		t.Fatalf("expected count=1, got %d", resp.Count)
	}
	c := resp.Connections[0]
	if c.PeerAddress != canonical {
		t.Errorf("peer_address should be canonical %s, got %s", canonical, c.PeerAddress)
	}
	if c.RemoteAddress != fallback {
		t.Errorf("remote_address should be fallback %s, got %s", fallback, c.RemoteAddress)
	}
	if c.PeerAddress == c.RemoteAddress {
		t.Error("peer_address and remote_address should differ in fallback scenario")
	}
}
