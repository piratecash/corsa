package service

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// newTestMonitor creates a NodeStatusMonitor with a no-op onChanged callback
// and no event bus (subscriptions tested separately via direct method calls).
func newTestMonitor() *NodeStatusMonitor {
	return NewNodeStatusMonitor(NodeStatusMonitorOpts{
		OnChanged: func() {},
	})
}

// ── applyPeerHealthDelta ──

func TestApplyPeerHealthDeltaCreatesNewEntry(t *testing.T) {
	m := newTestMonitor()

	now := time.Now()
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "1.2.3.4:9999",
		PeerID:          "peer-abc",
		Direction:       "outbound",
		Connected:       true,
		State:           "active",
		Score:           100,
		LastConnectedAt: ebus.TimePtr(now),
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(m.status.PeerHealth))
	}
	ph := m.status.PeerHealth[0]
	if ph.Address != "1.2.3.4:9999" {
		t.Fatalf("address = %q", ph.Address)
	}
	if ph.PeerID != "peer-abc" {
		t.Fatalf("PeerID = %q", ph.PeerID)
	}
	if ph.Direction != "outbound" {
		t.Fatalf("Direction = %q", ph.Direction)
	}
	if !ph.Connected {
		t.Fatal("expected Connected=true")
	}
	if !ph.LastConnectedAt.Valid() || !ph.LastConnectedAt.Time().Equal(now) {
		t.Fatalf("LastConnectedAt = %v, want %v", ph.LastConnectedAt, now)
	}
}

func TestApplyPeerHealthDeltaDisconnectClearsSessionMetadata(t *testing.T) {
	m := newTestMonitor()

	// Seed a connected peer.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "1.2.3.4:9999",
		PeerID:          "peer-abc",
		Direction:       "outbound",
		Connected:       true,
		ClientVersion:   "v0.35",
		ClientBuild:     42,
		ProtocolVersion: 3,
		ConnID:          100,
	})

	// Disconnect: session-scoped fields are overwritten unconditionally.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "1.2.3.4:9999",
		PeerID:          "peer-abc",
		Direction:       "",
		Connected:       false,
		ClientVersion:   "",
		ClientBuild:     0,
		ProtocolVersion: 0,
		ConnID:          0,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	if ph.Connected {
		t.Fatal("expected Connected=false after disconnect")
	}
	if ph.Direction != "" {
		t.Fatalf("Direction should be cleared, got %q", ph.Direction)
	}
	if ph.ClientVersion != "" {
		t.Fatalf("ClientVersion should be cleared, got %q", ph.ClientVersion)
	}
	if ph.ClientBuild != 0 {
		t.Fatalf("ClientBuild should be 0, got %d", ph.ClientBuild)
	}
	if ph.ConnID != 0 {
		t.Fatalf("ConnID should be 0, got %d", ph.ConnID)
	}
}

func TestApplyPeerHealthDeltaConnectBackfillsSessionMetadata(t *testing.T) {
	m := newTestMonitor()

	// Seed with partial data (e.g. from a probe snapshot).
	m.status.PeerHealth = []PeerHealth{{
		Address:       "1.2.3.4:9999",
		PeerID:        "peer-abc",
		Direction:     "outbound",
		ClientVersion: "v0.34",
	}}

	// Connect delta with new version — backfills non-empty fields only.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "1.2.3.4:9999",
		PeerID:          "peer-abc",
		Direction:       "inbound",
		Connected:       true,
		ClientVersion:   "v0.35",
		ClientBuild:     42,
		ProtocolVersion: 3,
		ConnID:          200,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	// Direction was already "outbound" — on connect, non-empty existing
	// Direction is kept (backfill only when empty).
	if ph.Direction != "outbound" {
		t.Fatalf("Direction should keep existing value, got %q", ph.Direction)
	}
	// ClientVersion is updated because the delta carries a non-empty value.
	if ph.ClientVersion != "v0.35" {
		t.Fatalf("ClientVersion = %q, want v0.35", ph.ClientVersion)
	}
	if ph.ClientBuild != 42 {
		t.Fatalf("ClientBuild = %d, want 42", ph.ClientBuild)
	}
	if ph.ConnID != 200 {
		t.Fatalf("ConnID = %d, want 200", ph.ConnID)
	}
}

func TestApplyPeerHealthDeltaOptionalTimestampsNilPreserveExisting(t *testing.T) {
	m := newTestMonitor()

	past := time.Now().Add(-time.Hour)
	m.status.PeerHealth = []PeerHealth{{
		Address:         "1.2.3.4:9999",
		LastConnectedAt: domain.TimeOf(past),
	}}

	// Delta with nil timestamp — should NOT overwrite existing.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "1.2.3.4:9999",
		Connected:       true,
		LastConnectedAt: nil,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	if !ph.LastConnectedAt.Valid() || !ph.LastConnectedAt.Time().Equal(past) {
		t.Fatalf("nil delta should preserve existing timestamp, got %v", ph.LastConnectedAt)
	}
}

func TestApplyPeerHealthDeltaBackfillsPeerID(t *testing.T) {
	m := newTestMonitor()

	// Initial entry without PeerID (placeholder).
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		PeerID:    "",
		Connected: true,
	})

	// Second delta resolves identity.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		PeerID:    "peer-resolved",
		Connected: true,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].PeerID != "peer-resolved" {
		t.Fatalf("PeerID should be backfilled, got %q", m.status.PeerHealth[0].PeerID)
	}
}

// ── applySlotStateDelta ──

func TestApplySlotStateDeltaUpdatesExisting(t *testing.T) {
	m := newTestMonitor()
	m.status.PeerHealth = []PeerHealth{{Address: "1.2.3.4:9999"}}

	m.applySlotStateDelta("1.2.3.4:9999", "dialing")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].SlotState != "dialing" {
		t.Fatalf("SlotState = %q, want dialing", m.status.PeerHealth[0].SlotState)
	}
}

func TestApplySlotStateDeltaCreatesMinimalEntry(t *testing.T) {
	m := newTestMonitor()

	m.applySlotStateDelta("1.2.3.4:9999", "queued")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m.status.PeerHealth))
	}
	if m.status.PeerHealth[0].SlotState != "queued" {
		t.Fatalf("SlotState = %q", m.status.PeerHealth[0].SlotState)
	}
}

func TestApplySlotStateDeltaEmptyStringUnknownPeerNoOp(t *testing.T) {
	m := newTestMonitor()

	m.applySlotStateDelta("unknown:1234", "")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 0 {
		t.Fatal("empty slot state for unknown peer should not create entry")
	}
}

// ── applyPeerPendingDelta ──

func TestApplyPeerPendingDeltaUpdatesPendingCount(t *testing.T) {
	m := newTestMonitor()
	m.status.PeerHealth = []PeerHealth{{Address: "1.2.3.4:9999", PendingCount: 1}}

	m.applyPeerPendingDelta(ebus.PeerPendingDelta{
		Address: "1.2.3.4:9999",
		Count:   5,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].PendingCount != 5 {
		t.Fatalf("PendingCount = %d, want 5", m.status.PeerHealth[0].PendingCount)
	}
}

func TestApplyPeerPendingDeltaUnknownPeerCreatesEntry(t *testing.T) {
	m := newTestMonitor()

	m.applyPeerPendingDelta(ebus.PeerPendingDelta{
		Address: "new-peer:1234",
		Count:   3,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m.status.PeerHealth))
	}
	if m.status.PeerHealth[0].PendingCount != 3 {
		t.Fatalf("PendingCount = %d, want 3", m.status.PeerHealth[0].PendingCount)
	}
}

func TestApplyPeerPendingDeltaZeroCountUnknownPeerIsNoOp(t *testing.T) {
	m := newTestMonitor()

	m.applyPeerPendingDelta(ebus.PeerPendingDelta{
		Address: "unknown:1234",
		Count:   0,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 0 {
		t.Fatal("zero-count delta for unknown peer should not create entry")
	}
}

// ── applyTrafficBatch ──

func TestApplyTrafficBatchUpdatesExistingPeers(t *testing.T) {
	m := newTestMonitor()
	m.status.PeerHealth = []PeerHealth{
		{Address: "peer-a"},
		{Address: "peer-b"},
	}

	m.applyTrafficBatch(ebus.PeerTrafficBatch{
		Peers: []ebus.PeerTrafficSnapshot{
			{Address: "peer-a", BytesSent: 100, BytesReceived: 200},
			{Address: "peer-b", BytesSent: 50, BytesReceived: 75},
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].BytesSent != 100 || m.status.PeerHealth[0].BytesReceived != 200 {
		t.Fatalf("peer-a traffic: sent=%d recv=%d", m.status.PeerHealth[0].BytesSent, m.status.PeerHealth[0].BytesReceived)
	}
	if m.status.PeerHealth[0].TotalTraffic != 300 {
		t.Fatalf("peer-a TotalTraffic = %d, want 300", m.status.PeerHealth[0].TotalTraffic)
	}
	if m.status.PeerHealth[1].BytesSent != 50 {
		t.Fatalf("peer-b BytesSent = %d, want 50", m.status.PeerHealth[1].BytesSent)
	}
}

func TestApplyTrafficBatchIgnoresUnknownPeers(t *testing.T) {
	m := newTestMonitor()
	m.status.PeerHealth = []PeerHealth{{Address: "peer-a"}}

	m.applyTrafficBatch(ebus.PeerTrafficBatch{
		Peers: []ebus.PeerTrafficSnapshot{
			{Address: "unknown:1234", BytesSent: 999, BytesReceived: 999},
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatal("unknown peer should not be added by traffic batch")
	}
	if m.status.PeerHealth[0].BytesSent != 0 {
		t.Fatal("existing peer should be unchanged when not in batch")
	}
}

// ── mergePeerHealth ──

func TestMergePeerHealthEmptyEbusReturnProbe(t *testing.T) {
	probe := []PeerHealth{{Address: "a"}, {Address: "b"}}
	result := mergePeerHealth(nil, probe, nil)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestMergePeerHealthEmptyProbeReturnEbus(t *testing.T) {
	ebusList := []PeerHealth{{Address: "a"}}
	result := mergePeerHealth(ebusList, nil, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
}

func TestMergePeerHealthSeededProtectsStateAndSessionBackfillsPersistent(t *testing.T) {
	// Ebus entry created by a full health delta — Connected=false, Score=0,
	// Direction="" are authoritative (disconnect cleared them). PeerID is
	// empty because identity wasn't resolved yet when the delta fired.
	ebusList := []PeerHealth{{Address: "a", Connected: false, Score: 0, State: ""}}
	now := time.Now()
	bannedUntil := now.Add(time.Hour)
	probe := []PeerHealth{{
		Address:                     "a",
		Connected:                   true,
		Score:                       80,
		State:                       "active",
		PeerID:                      "peer-a",
		Direction:                   "outbound",
		ClientVersion:               "v0.35",
		ClientBuild:                 42,
		ConnID:                      100,
		ProtocolVersion:             3,
		SlotState:                   "active",
		LastConnectedAt:             domain.TimeOf(now),
		BannedUntil:                 domain.TimeOf(bannedUntil),
		LastErrorCode:               "version_mismatch",
		LastDisconnectCode:          "incompatible",
		IncompatibleVersionAttempts: 5,
	}, {Address: "b", Score: 20}}
	seeded := map[string]struct{}{"a": {}}

	result := mergePeerHealth(ebusList, probe, seeded)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}

	// State fields protected — probe cannot overwrite authoritative zeros.
	if result[0].Connected {
		t.Fatal("seeded Connected should stay false")
	}
	if result[0].Score != 0 {
		t.Fatalf("seeded Score should stay 0, got %d", result[0].Score)
	}
	if result[0].State != "" {
		t.Fatalf("seeded State should stay empty, got %q", result[0].State)
	}
	// Session-scoped fields protected — disconnect cleared them to zero/empty.
	if result[0].Direction != "" {
		t.Fatalf("seeded Direction should stay empty (cleared by disconnect), got %q", result[0].Direction)
	}
	if result[0].ClientVersion != "" {
		t.Fatalf("seeded ClientVersion should stay empty (cleared by disconnect), got %q", result[0].ClientVersion)
	}
	if result[0].ClientBuild != 0 {
		t.Fatalf("seeded ClientBuild should stay 0 (cleared by disconnect), got %d", result[0].ClientBuild)
	}
	if result[0].ConnID != 0 {
		t.Fatalf("seeded ConnID should stay 0 (cleared by disconnect), got %d", result[0].ConnID)
	}
	if result[0].ProtocolVersion != 0 {
		t.Fatalf("seeded ProtocolVersion should stay 0 (cleared by disconnect), got %d", result[0].ProtocolVersion)
	}
	// Slot-lifecycle fields protected — empty SlotState is a removal signal.
	if result[0].SlotState != "" {
		t.Fatalf("seeded SlotState should stay empty (slot cleared), got %q", result[0].SlotState)
	}
	// Recovery-clearable fields protected — cleared by resetPeerHealthForRecoveryLocked.
	if result[0].BannedUntil.Valid() {
		t.Fatalf("seeded BannedUntil should stay invalid (cleared by recovery), got %v", result[0].BannedUntil)
	}
	if result[0].LastErrorCode != "" {
		t.Fatalf("seeded LastErrorCode should stay empty (cleared by recovery), got %q", result[0].LastErrorCode)
	}
	if result[0].LastDisconnectCode != "" {
		t.Fatalf("seeded LastDisconnectCode should stay empty (cleared by recovery), got %q", result[0].LastDisconnectCode)
	}
	if result[0].IncompatibleVersionAttempts != 0 {
		t.Fatalf("seeded IncompatibleVersionAttempts should stay 0 (cleared by recovery), got %d", result[0].IncompatibleVersionAttempts)
	}
	// Persistent identity field backfilled from probe.
	if result[0].PeerID != "peer-a" {
		t.Fatalf("PeerID should be backfilled from probe, got %q", result[0].PeerID)
	}
	// Timestamp backfilled (invalid-guarded, persistent).
	if !result[0].LastConnectedAt.Valid() || !result[0].LastConnectedAt.Time().Equal(now) {
		t.Fatalf("LastConnectedAt should be backfilled, got %v", result[0].LastConnectedAt)
	}
	// "b" appended from probe.
	if result[1].Address != "b" || result[1].Score != 20 {
		t.Fatalf("probe entry not appended: %+v", result[1])
	}
}

func TestMergePeerHealthEnrichesPlaceholder(t *testing.T) {
	// Placeholder created by applySlotStateDelta — only Address + SlotState set.
	// NOT in healthSeeded (no applyPeerHealthDelta received).
	ebusList := []PeerHealth{{Address: "1.2.3.4:9999", SlotState: "dialing"}}
	now := time.Now()
	probe := []PeerHealth{{
		Address:         "1.2.3.4:9999",
		PeerID:          "peer-abc",
		Direction:       "outbound",
		ClientVersion:   "v0.35",
		ClientBuild:     42,
		State:           "active",
		Connected:       true,
		Score:           80,
		LastConnectedAt: domain.TimeOf(now),
	}}

	// nil healthSeeded — address was never touched by a health delta.
	result := mergePeerHealth(ebusList, probe, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 merged entry, got %d", len(result))
	}

	ph := result[0]
	// Ebus-set field preserved.
	if ph.SlotState != "dialing" {
		t.Fatalf("SlotState should be preserved, got %q", ph.SlotState)
	}
	// Probe fields enriched into placeholder.
	if ph.PeerID != "peer-abc" {
		t.Fatalf("PeerID should be enriched, got %q", ph.PeerID)
	}
	if ph.Direction != "outbound" {
		t.Fatalf("Direction should be enriched, got %q", ph.Direction)
	}
	if ph.ClientVersion != "v0.35" {
		t.Fatalf("ClientVersion should be enriched, got %q", ph.ClientVersion)
	}
	if ph.Score != 80 {
		t.Fatalf("Score should be enriched, got %d", ph.Score)
	}
	if !ph.Connected {
		t.Fatal("Connected should be enriched from probe")
	}
	if !ph.LastConnectedAt.Valid() || !ph.LastConnectedAt.Time().Equal(now) {
		t.Fatalf("LastConnectedAt should be enriched, got %v", ph.LastConnectedAt)
	}
}

func TestMergePeerHealthNoDuplicates(t *testing.T) {
	ebusList := []PeerHealth{{Address: "x"}, {Address: "y"}}
	probe := []PeerHealth{{Address: "x"}, {Address: "y"}, {Address: "z"}}

	result := mergePeerHealth(ebusList, probe, nil)
	if len(result) != 3 {
		t.Fatalf("expected 3 (x, y from ebus + z from probe), got %d", len(result))
	}
}

func TestMergePeerHealthMixedSeededAndPlaceholder(t *testing.T) {
	// "a" received a health delta (seeded), "b" is a placeholder from slot state.
	ebusList := []PeerHealth{
		{Address: "a", Connected: false, Score: 0},
		{Address: "b", SlotState: "queued"},
	}
	probe := []PeerHealth{
		{Address: "a", Connected: true, Score: 50, PeerID: "peer-a"},
		{Address: "b", Connected: true, Score: 30, PeerID: "peer-b"},
		{Address: "c", Score: 10},
	}
	seeded := map[string]struct{}{"a": {}}

	result := mergePeerHealth(ebusList, probe, seeded)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}

	// "a" is seeded — state fields protected, identity backfilled.
	if result[0].Connected {
		t.Fatal("seeded 'a' Connected should stay false")
	}
	if result[0].Score != 0 {
		t.Fatalf("seeded 'a' Score should stay 0, got %d", result[0].Score)
	}
	if result[0].PeerID != "peer-a" {
		t.Fatalf("seeded 'a' PeerID should be backfilled, got %q", result[0].PeerID)
	}
	// "b" is a placeholder — fully enriched from probe.
	if result[1].PeerID != "peer-b" {
		t.Fatalf("placeholder 'b' PeerID should be enriched, got %q", result[1].PeerID)
	}
	if result[1].SlotState != "queued" {
		t.Fatalf("placeholder 'b' SlotState should be preserved, got %q", result[1].SlotState)
	}
	if result[1].Score != 30 {
		t.Fatalf("placeholder 'b' Score should be enriched, got %d", result[1].Score)
	}
	// "c" is probe-only — appended.
	if result[2].Address != "c" {
		t.Fatalf("probe-only 'c' should be appended")
	}
}

// ── mergeAggregateStatus ──

func TestMergeAggregateStatusNilProbe(t *testing.T) {
	ebusSt := &AggregateStatus{Status: "healthy"}
	result := mergeAggregateStatus(ebusSt, nil, true, false)
	if result != ebusSt {
		t.Fatal("nil probe should return ebus pointer directly")
	}
}

func TestMergeAggregateStatusNilEbus(t *testing.T) {
	probe := &AggregateStatus{Status: "degraded", UsablePeers: 5}
	result := mergeAggregateStatus(nil, probe, false, false)
	if result == probe {
		t.Fatal("should return a clone, not the same pointer")
	}
	if result.Status != "degraded" || result.UsablePeers != 5 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestMergeAggregateStatusCountersSeeded(t *testing.T) {
	ebusSt := &AggregateStatus{
		Status:          "healthy",
		UsablePeers:     10,
		ConnectedPeers:  8,
		TotalPeers:      12,
		PendingMessages: 3,
	}
	probe := &AggregateStatus{
		Status:          "degraded",
		UsablePeers:     5,
		ConnectedPeers:  4,
		TotalPeers:      6,
		PendingMessages: 0,
		UpdateAvailable: true,
		UpdateReason:    "minor",
	}

	result := mergeAggregateStatus(ebusSt, probe, true, false)

	// Counters should come from ebus (seeded).
	if result.Status != "healthy" {
		t.Fatalf("Status = %q, want healthy (ebus)", result.Status)
	}
	if result.UsablePeers != 10 {
		t.Fatalf("UsablePeers = %d, want 10 (ebus)", result.UsablePeers)
	}
	// Version fields should come from probe (not seeded).
	if !result.UpdateAvailable {
		t.Fatal("UpdateAvailable should come from probe")
	}
	if result.UpdateReason != "minor" {
		t.Fatalf("UpdateReason = %q, want minor (probe)", result.UpdateReason)
	}
}

func TestMergeAggregateStatusVersionSeeded(t *testing.T) {
	ebusSt := &AggregateStatus{
		UpdateAvailable:              true,
		UpdateReason:                 "major",
		IncompatibleVersionReporters: 3,
		MaxObservedPeerBuild:         99,
		MaxObservedPeerVersion:       7,
	}
	probe := &AggregateStatus{
		Status:                       "degraded",
		UsablePeers:                  5,
		UpdateAvailable:              false,
		UpdateReason:                 "",
		IncompatibleVersionReporters: 0,
	}

	result := mergeAggregateStatus(ebusSt, probe, false, true)

	// Counters come from probe (not seeded).
	if result.Status != "degraded" {
		t.Fatalf("Status = %q, want degraded (probe)", result.Status)
	}
	if result.UsablePeers != 5 {
		t.Fatalf("UsablePeers = %d, want 5 (probe)", result.UsablePeers)
	}
	// Version fields come from ebus (seeded).
	if !result.UpdateAvailable {
		t.Fatal("UpdateAvailable should come from ebus")
	}
	if result.IncompatibleVersionReporters != 3 {
		t.Fatalf("IncompatibleVersionReporters = %d, want 3", result.IncompatibleVersionReporters)
	}
	if result.MaxObservedPeerBuild != 99 {
		t.Fatalf("MaxObservedPeerBuild = %d, want 99", result.MaxObservedPeerBuild)
	}
}

func TestMergeAggregateStatusBothSeeded(t *testing.T) {
	ebusSt := &AggregateStatus{
		Status:          "healthy",
		UsablePeers:     10,
		UpdateAvailable: true,
		UpdateReason:    "critical",
	}
	probe := &AggregateStatus{
		Status:          "degraded",
		UsablePeers:     3,
		UpdateAvailable: false,
		UpdateReason:    "",
	}

	result := mergeAggregateStatus(ebusSt, probe, true, true)

	// Both seeded — all fields from ebus.
	if result.Status != "healthy" {
		t.Fatalf("Status = %q, want healthy (ebus)", result.Status)
	}
	if !result.UpdateAvailable {
		t.Fatal("UpdateAvailable should come from ebus (seeded)")
	}
}

// ── SeedFromProbe / mergeNodeStatusLocked ──

func TestSeedFromProbeSetsAlwaysWriteFields(t *testing.T) {
	m := newTestMonitor()

	m.SeedFromProbe(NodeStatus{
		Address:   "my-addr",
		Connected: true,
		NodeID:    "node-1",
		NodeType:  "full",
		KnownIDs:  []string{"a", "b"},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.Address != "my-addr" {
		t.Fatalf("Address = %q", m.status.Address)
	}
	if !m.status.Connected {
		t.Fatal("Connected should be true")
	}
	if m.status.NodeID != "node-1" {
		t.Fatalf("NodeID = %q", m.status.NodeID)
	}
}

func TestSeedFromProbeSeededStateProtectedIdentityBackfilled(t *testing.T) {
	m := newTestMonitor()

	// Simulate ebus delivering a peer health delta before probe.
	// This marks the address as seeded. PeerID is empty (not yet resolved).
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		Connected: true,
		Score:     99,
	})

	// Probe arrives with same peer — stale state but has PeerID.
	m.SeedFromProbe(NodeStatus{
		PeerHealth: []PeerHealth{
			{Address: "1.2.3.4:9999", Connected: false, Score: 10, PeerID: "peer-abc", State: "stale-state"},
			{Address: "5.6.7.8:1234", Score: 50},
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(m.status.PeerHealth))
	}
	// State fields preserved from ebus (seeded).
	if !m.status.PeerHealth[0].Connected || m.status.PeerHealth[0].Score != 99 {
		t.Fatalf("ebus state overwritten: Connected=%v Score=%d",
			m.status.PeerHealth[0].Connected, m.status.PeerHealth[0].Score)
	}
	// Identity field backfilled from probe.
	if m.status.PeerHealth[0].PeerID != "peer-abc" {
		t.Fatalf("PeerID should be backfilled from probe, got %q", m.status.PeerHealth[0].PeerID)
	}
	// New probe-only peer appended.
	if m.status.PeerHealth[1].Address != "5.6.7.8:1234" {
		t.Fatalf("probe-only peer not appended")
	}
}

func TestSeedFromProbeDoesNotResurrectDisconnectedPeer(t *testing.T) {
	m := newTestMonitor()

	// Ebus: peer connects then disconnects — state and session fields cleared.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "1.2.3.4:9999",
		PeerID:          "peer-abc",
		Direction:       "outbound",
		Connected:       true,
		Score:           80,
		State:           "active",
		ClientVersion:   "v0.35",
		ClientBuild:     42,
		ConnID:          100,
		ProtocolVersion: 3,
	})
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		PeerID:    "peer-abc",
		Connected: false,
		Score:     0,
		State:     "",
		// Disconnect clears session-scoped fields to zero/empty.
		Direction:       "",
		ClientVersion:   "",
		ClientBuild:     0,
		ConnID:          0,
		ProtocolVersion: 0,
	})

	// Stale probe arrives showing the peer as connected with session info.
	m.SeedFromProbe(NodeStatus{
		PeerHealth: []PeerHealth{{
			Address:         "1.2.3.4:9999",
			PeerID:          "peer-abc",
			Connected:       true,
			Score:           80,
			State:           "active",
			Direction:       "outbound",
			ClientVersion:   "v0.35",
			ClientBuild:     42,
			ConnID:          100,
			ProtocolVersion: 3,
		}},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	// State fields must NOT be overwritten by stale probe.
	if ph.Connected {
		t.Fatal("probe should not resurrect Connected=true for a seeded entry")
	}
	if ph.Score != 0 {
		t.Fatalf("probe should not resurrect Score, got %d", ph.Score)
	}
	if ph.State != "" {
		t.Fatalf("probe should not resurrect State, got %q", ph.State)
	}
	// Session-scoped fields must NOT be resurrected — disconnect cleared them.
	if ph.Direction != "" {
		t.Fatalf("probe should not resurrect Direction, got %q", ph.Direction)
	}
	if ph.ClientVersion != "" {
		t.Fatalf("probe should not resurrect ClientVersion, got %q", ph.ClientVersion)
	}
	if ph.ClientBuild != 0 {
		t.Fatalf("probe should not resurrect ClientBuild, got %d", ph.ClientBuild)
	}
	if ph.ConnID != 0 {
		t.Fatalf("probe should not resurrect ConnID, got %d", ph.ConnID)
	}
	if ph.ProtocolVersion != 0 {
		t.Fatalf("probe should not resurrect ProtocolVersion, got %d", ph.ProtocolVersion)
	}
	// PeerID is persistent — stays from ebus.
	if ph.PeerID != "peer-abc" {
		t.Fatalf("PeerID should remain from ebus, got %q", ph.PeerID)
	}
}

func TestSeedFromProbeDoesNotResurrectClearedSlotState(t *testing.T) {
	m := newTestMonitor()

	// Ebus: health delta seeds the address, then slot state is cleared.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		Connected: true,
	})
	// Slot cleared — empty string is a meaningful removal signal.
	m.applySlotStateDelta("1.2.3.4:9999", "")

	// Stale probe with an active slot state.
	m.SeedFromProbe(NodeStatus{
		PeerHealth: []PeerHealth{{
			Address:           "1.2.3.4:9999",
			SlotState:         "active",
			SlotRetryCount:    3,
			SlotGeneration:    5,
			SlotConnectedAddr: "5.6.7.8:1234",
		}},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	if ph.SlotState != "" {
		t.Fatalf("probe should not resurrect cleared SlotState, got %q", ph.SlotState)
	}
}

func TestSeedFromProbePreservesEbusAggregateCounters(t *testing.T) {
	m := newTestMonitor()

	// Simulate ebus aggregate status arriving first.
	m.mu.Lock()
	m.status.AggregateStatus = &AggregateStatus{
		Status:      "healthy",
		UsablePeers: 10,
	}
	m.ebusAggregateCountersSeeded = true
	m.mu.Unlock()

	// Probe with different counters.
	m.SeedFromProbe(NodeStatus{
		AggregateStatus: &AggregateStatus{
			Status:      "degraded",
			UsablePeers: 3,
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Ebus counters preserved.
	if m.status.AggregateStatus.Status != "healthy" {
		t.Fatalf("AggregateStatus.Status = %q, want healthy (ebus)", m.status.AggregateStatus.Status)
	}
	if m.status.AggregateStatus.UsablePeers != 10 {
		t.Fatalf("UsablePeers = %d, want 10 (ebus)", m.status.AggregateStatus.UsablePeers)
	}
}

func TestSeedFromProbeSeedsContactsWhenEbusEmpty(t *testing.T) {
	m := newTestMonitor()

	m.SeedFromProbe(NodeStatus{
		Contacts: map[string]Contact{
			"alice": {PubKey: "pk-a"},
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.Contacts) != 1 {
		t.Fatalf("expected 1 contact, got %d", len(m.status.Contacts))
	}
}

func TestSeedFromProbeMergesContacts(t *testing.T) {
	m := newTestMonitor()

	// Ebus already has a contact.
	m.mu.Lock()
	m.status.Contacts = map[string]Contact{"bob": {PubKey: "pk-b"}}
	m.mu.Unlock()

	// Probe with different contacts.
	m.SeedFromProbe(NodeStatus{
		Contacts: map[string]Contact{
			"alice": {PubKey: "pk-a"},
			"bob":   {PubKey: "pk-b-stale"},
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Ebus contact preserved (fresher than probe).
	if m.status.Contacts["bob"].PubKey != "pk-b" {
		t.Fatalf("ebus contact bob should keep ebus value, got %q", m.status.Contacts["bob"].PubKey)
	}
	// Probe contact for new key merged in.
	if _, ok := m.status.Contacts["alice"]; !ok {
		t.Fatal("probe contact alice should be merged into result")
	}
	if m.status.Contacts["alice"].PubKey != "pk-a" {
		t.Fatalf("alice PubKey = %q, want pk-a", m.status.Contacts["alice"].PubKey)
	}
}

// ── Reset ──

func TestResetClearsAllState(t *testing.T) {
	m := newTestMonitor()

	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		Connected: true,
	})
	// Seed a capture session so Reset also has to clear the CaptureSessions
	// map from NodeStatus.
	startedAt := time.Now().UTC()
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(4242),
		Address:   domain.PeerAddress("5.6.7.8:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/4242.jsonl",
		StartedAt: &startedAt,
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})
	m.mu.Lock()
	m.ebusAggregateCountersSeeded = true
	m.ebusVersionPolicySeeded = true
	m.mu.Unlock()

	m.Reset()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 0 {
		t.Fatal("PeerHealth should be empty after reset")
	}
	if m.ebusHealthSeeded != nil {
		t.Fatal("ebusHealthSeeded should be nil after reset")
	}
	if len(m.status.CaptureSessions) != 0 {
		t.Fatal("CaptureSessions must be empty after reset")
	}
	if m.ebusAggregateCountersSeeded {
		t.Fatal("ebusAggregateCountersSeeded should be false after reset")
	}
	if m.ebusVersionPolicySeeded {
		t.Fatal("ebusVersionPolicySeeded should be false after reset")
	}
}

// ── IsReachable ──

func TestIsReachable(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.ReachableIDs = map[domain.PeerIdentity]bool{
		"peer-a": true,
	}
	m.mu.Unlock()

	if !m.IsReachable("peer-a") {
		t.Fatal("peer-a should be reachable")
	}
	if m.IsReachable("peer-b") {
		t.Fatal("peer-b should not be reachable")
	}
}

// ── Route-table-based reachability ──

func TestReachabilityProbeSeeded(t *testing.T) {
	m := newTestMonitor()

	// Seed from probe — identity is reachable via routing table snapshot.
	m.SeedFromProbe(NodeStatus{
		ReachableIDs: map[domain.PeerIdentity]bool{
			"peer-probe": true,
		},
	})

	if !m.IsReachable("peer-probe") {
		t.Fatal("peer-probe should be reachable after probe seed")
	}
	if m.IsReachable("peer-unknown") {
		t.Fatal("peer-unknown should not be reachable")
	}
}

// ── NodeStatus deep copy ──

func TestNodeStatusReturnsDeepCopy(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{{Address: "a", Score: 10}}
	m.status.Contacts = map[string]Contact{"alice": {PubKey: "pk"}}
	m.mu.Unlock()

	snap := m.NodeStatus()

	// Mutate the copy.
	snap.PeerHealth[0].Score = 999
	snap.Contacts["alice"] = Contact{PubKey: "mutated"}

	// Original should be unchanged.
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].Score != 10 {
		t.Fatalf("original PeerHealth mutated: Score=%d", m.status.PeerHealth[0].Score)
	}
	if m.status.Contacts["alice"].PubKey != "pk" {
		t.Fatalf("original Contacts mutated: PubKey=%q", m.status.Contacts["alice"].PubKey)
	}
}

// ── Multi-ConnID (per-connection row) tests ──

func TestApplyPeerHealthDeltaWithConnIDTargetsCorrectRow(t *testing.T) {
	m := newTestMonitor()

	// Simulate two inbound rows for the same address (created by a prior
	// probe merge that preserved per-ConnID rows).
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100, Direction: "inbound", ClientVersion: "v1.0"},
		{Address: "10.0.0.1:9999", ConnID: 200, Direction: "inbound", ClientVersion: "v1.1"},
	}
	m.mu.Unlock()

	// Delta for outbound ConnID=200 with InboundConnIDs=[100] — both
	// connections are alive. Session-scoped fields target ConnID=200 only;
	// address-level fields propagate to all rows.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         200,
		InboundConnIDs: []uint64{100},
		Connected:      true,
		State:          "active",
		ClientVersion:  "v2.0",
		Score:          50,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(m.status.PeerHealth))
	}
	// Row 0 (ConnID=100): session-scoped fields preserved (writeSession=false),
	// but address-level fields (Score, State) are propagated.
	if m.status.PeerHealth[0].ClientVersion != "v1.0" {
		t.Fatalf("row[0] ClientVersion mutated to %q, expected v1.0 (session-scoped)", m.status.PeerHealth[0].ClientVersion)
	}
	if m.status.PeerHealth[0].Score != 50 {
		t.Fatalf("row[0] Score = %d, want 50 (address-level propagation)", m.status.PeerHealth[0].Score)
	}
	if m.status.PeerHealth[0].State != "active" {
		t.Fatalf("row[0] State = %q, want active (address-level propagation)", m.status.PeerHealth[0].State)
	}
	// Row 1 (ConnID=200): full session write — all fields updated.
	if m.status.PeerHealth[1].ClientVersion != "v2.0" {
		t.Fatalf("row[1] ClientVersion = %q, want v2.0", m.status.PeerHealth[1].ClientVersion)
	}
	if m.status.PeerHealth[1].Score != 50 {
		t.Fatalf("row[1] Score = %d, want 50", m.status.PeerHealth[1].Score)
	}
}

func TestApplyPeerHealthDeltaConnIDZeroDisconnectPrunesPerConnIDRows(t *testing.T) {
	m := newTestMonitor()

	// Two inbound rows for the same address.
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100, Direction: "inbound"},
		{Address: "10.0.0.1:9999", ConnID: 200, Direction: "inbound"},
	}
	m.mu.Unlock()

	// Address-level disconnect (ConnID=0, Connected=false, no InboundConnIDs)
	// means all connections are gone. Per-ConnID rows are pruned (step 5),
	// leaving a single ConnID=0 address row with disconnected state.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "10.0.0.1:9999",
		ConnID:    0,
		Connected: false,
		State:     "disconnected",
		Score:     -10,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 row after disconnect collapse, got %d", len(m.status.PeerHealth))
	}
	ph := m.status.PeerHealth[0]
	if ph.State != "disconnected" {
		t.Fatalf("State = %q, want disconnected", ph.State)
	}
	if ph.Score != -10 {
		t.Fatalf("Score = %d, want -10", ph.Score)
	}
	if ph.Connected {
		t.Fatal("still Connected after disconnect")
	}
	// Session metadata cleared.
	if ph.ConnID != 0 {
		t.Fatalf("ConnID = %d, want 0 (session cleared)", ph.ConnID)
	}
	if ph.Direction != "" {
		t.Fatalf("Direction = %q, want empty (cleared)", ph.Direction)
	}
}

func TestApplyPeerHealthDeltaConnIDZeroConnectUpdatesAllRows(t *testing.T) {
	m := newTestMonitor()

	// Two inbound rows for the same address (alive).
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100, Direction: "inbound", Score: 0},
		{Address: "10.0.0.1:9999", ConnID: 200, Direction: "inbound", Score: 0},
	}
	m.mu.Unlock()

	// Address-level connect delta (ConnID=0, Connected=true) for an
	// inbound-only peer. InboundConnIDs declares both connections alive.
	// All rows should receive address-level field updates; no ConnID=0
	// row is created because inbound rows already exist.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         0,
		InboundConnIDs: []uint64{100, 200},
		Connected:      true,
		State:          "active",
		Score:          50,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	// No collapse: Connected=true, per-ConnID rows preserved.
	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(m.status.PeerHealth))
	}
	for i, ph := range m.status.PeerHealth {
		if ph.State != "active" {
			t.Fatalf("row[%d] State = %q, want active", i, ph.State)
		}
		if ph.Score != 50 {
			t.Fatalf("row[%d] Score = %d, want 50", i, ph.Score)
		}
	}
	// ConnIDs preserved (connect, not disconnect).
	if m.status.PeerHealth[0].ConnID != 100 {
		t.Fatalf("row[0] ConnID changed to %d", m.status.PeerHealth[0].ConnID)
	}
	if m.status.PeerHealth[1].ConnID != 200 {
		t.Fatalf("row[1] ConnID changed to %d", m.status.PeerHealth[1].ConnID)
	}
}

func TestApplyPeerHealthDeltaPromotesConnIDZeroPlaceholder(t *testing.T) {
	m := newTestMonitor()

	// A placeholder row (ConnID=0) created by applySlotStateDelta.
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 0, SlotState: "dialing"},
	}
	m.mu.Unlock()

	// Delta with specific ConnID should promote the placeholder.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:       "10.0.0.1:9999",
		ConnID:        42,
		Connected:     true,
		Direction:     "outbound",
		ClientVersion: "v3.0",
		State:         "active",
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 row (promoted), got %d", len(m.status.PeerHealth))
	}
	ph := m.status.PeerHealth[0]
	if ph.ConnID != 42 {
		t.Fatalf("ConnID = %d, want 42 (promoted)", ph.ConnID)
	}
	if ph.SlotState != "dialing" {
		t.Fatalf("SlotState lost after promotion: %q", ph.SlotState)
	}
	if ph.ClientVersion != "v3.0" {
		t.Fatalf("ClientVersion = %q, want v3.0", ph.ClientVersion)
	}
}

// TestApplyPeerHealthDeltaInboundDeltaPrunesStalePlaceholder is the P2
// regression guard: a ConnID=0 placeholder (typically created by
// applySlotStateDelta while the outbound dial was still lifecycled) must
// not survive when the peer transitions to inbound-only. delta.ConnID=0
// plus delta.InboundConnIDs=[X] with Connected=true means "no outbound
// session, one live inbound". Previously the step-2 case
// (p.ConnID==0 && delta.ConnID==0) updated the placeholder in-place and
// step-5 pruning skipped ConnID=0 rows, leaving two rows for one peer —
// the stale address-level placeholder alongside the real inbound row.
// The fix prunes ConnID=0 rows whenever per-ConnID rows cover the address.
func TestApplyPeerHealthDeltaInboundDeltaPrunesStalePlaceholder(t *testing.T) {
	m := newTestMonitor()

	// Placeholder from applySlotStateDelta while the outbound dial was in flight.
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 0, SlotState: "dialing"},
	}
	m.mu.Unlock()

	// Outbound dial did not produce a session; an inbound connection landed
	// instead. Delta: no outbound (ConnID=0), one inbound, connected.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		PeerID:         "peer-abc",
		ConnID:         0,
		InboundConnIDs: []uint64{42},
		Connected:      true,
		State:          "active",
		Score:          50,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 row (inbound only, placeholder pruned), got %d", len(m.status.PeerHealth))
	}
	ph := m.status.PeerHealth[0]
	if ph.ConnID != 42 {
		t.Fatalf("surviving row ConnID = %d, want 42 (inbound)", ph.ConnID)
	}
	if ph.Direction != "inbound" {
		t.Fatalf("surviving row Direction = %q, want inbound", ph.Direction)
	}
}

// TestApplyPeerHealthDeltaInboundDeltaMigratesPlaceholderSlotState ensures
// the placeholder's address-level SlotState is not silently lost when the
// placeholder is pruned. SlotState arrives via TopicSlotStateChanged — it
// is NOT carried in the health delta — so the only way for the surviving
// inbound row to show the correct lifecycle state before the next slot
// delta is to migrate it from the placeholder.
func TestApplyPeerHealthDeltaInboundDeltaMigratesPlaceholderSlotState(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 0, SlotState: "reconnecting", PendingCount: 3},
	}
	m.mu.Unlock()

	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         0,
		InboundConnIDs: []uint64{77},
		Connected:      true,
		State:          "active",
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 surviving row, got %d", len(m.status.PeerHealth))
	}
	ph := m.status.PeerHealth[0]
	if ph.SlotState != "reconnecting" {
		t.Fatalf("SlotState must migrate from placeholder, got %q", ph.SlotState)
	}
	if ph.PendingCount != 3 {
		t.Fatalf("PendingCount must migrate from placeholder, got %d", ph.PendingCount)
	}
}

// TestApplyPeerHealthDeltaInboundDeltaMigratesSlotStateToMultipleInbound
// pins the address-scope invariant: SlotState describes the peer's outbound
// slot lifecycle, and applySlotStateDelta writes it to every row for the
// address. When the placeholder is pruned in favor of two inbound rows,
// both must receive the migrated SlotState so the UI renders consistent
// per-peer slot state regardless of which row it reads.
func TestApplyPeerHealthDeltaInboundDeltaMigratesSlotStateToMultipleInbound(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 0, SlotState: "retry_wait"},
	}
	m.mu.Unlock()

	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         0,
		InboundConnIDs: []uint64{11, 22},
		Connected:      true,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("expected 2 inbound rows, got %d", len(m.status.PeerHealth))
	}
	for i, ph := range m.status.PeerHealth {
		if ph.SlotState != "retry_wait" {
			t.Fatalf("row[%d] SlotState = %q, want retry_wait", i, ph.SlotState)
		}
		if ph.ConnID != 11 && ph.ConnID != 22 {
			t.Fatalf("row[%d] unexpected ConnID %d", i, ph.ConnID)
		}
	}
}

// TestApplyPeerHealthDeltaInboundDeltaPreservesExistingRowSlotState guards
// against the migration stomping a non-empty field on an already-existing
// inbound row. If an inbound row was observed earlier and applySlotStateDelta
// has since updated its SlotState, that value is authoritative — the
// placeholder's (possibly older) SlotState must NOT overwrite it.
func TestApplyPeerHealthDeltaInboundDeltaPreservesExistingRowSlotState(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 0, SlotState: "retry_wait"},
		{Address: "10.0.0.1:9999", ConnID: 77, Direction: "inbound", SlotState: "active"},
	}
	m.mu.Unlock()

	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         0,
		InboundConnIDs: []uint64{77},
		Connected:      true,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 row, got %d", len(m.status.PeerHealth))
	}
	if m.status.PeerHealth[0].SlotState != "active" {
		t.Fatalf("existing SlotState must be preserved, got %q", m.status.PeerHealth[0].SlotState)
	}
}

func TestMergePeerHealthPreservesPerConnIDRows(t *testing.T) {
	ebusList := []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100, State: "active", Connected: true},
	}
	probeList := []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100, State: "active", PeerID: "peer-1"},
		{Address: "10.0.0.1:9999", ConnID: 200, State: "active", PeerID: "peer-1", Direction: "inbound"},
	}
	seeded := map[string]struct{}{"10.0.0.1:9999": {}}

	merged := mergePeerHealth(ebusList, probeList, seeded)

	if len(merged) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(merged))
	}

	// Row 0: ebus (ConnID=100) enriched with PeerID from probe.
	if merged[0].ConnID != 100 {
		t.Fatalf("merged[0] ConnID = %d, want 100", merged[0].ConnID)
	}
	if merged[0].PeerID != "peer-1" {
		t.Fatalf("merged[0] PeerID not enriched: %q", merged[0].PeerID)
	}
	// Row 1: new from probe (ConnID=200), appended as-is.
	if merged[1].ConnID != 200 {
		t.Fatalf("merged[1] ConnID = %d, want 200", merged[1].ConnID)
	}
	if merged[1].Direction != "inbound" {
		t.Fatalf("merged[1] Direction = %q, want inbound", merged[1].Direction)
	}
}

func TestMergePeerHealthPromotesPlaceholderByConnID(t *testing.T) {
	// Ebus has a ConnID=0 placeholder (from applySlotStateDelta).
	ebusList := []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 0, SlotState: "dialing"},
	}
	// Probe returns a row with specific ConnID.
	probeList := []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 42, State: "active", PeerID: "peer-1"},
	}
	seeded := map[string]struct{}{}

	merged := mergePeerHealth(ebusList, probeList, seeded)

	if len(merged) != 1 {
		t.Fatalf("expected 1 row (promoted), got %d", len(merged))
	}
	if merged[0].ConnID != 42 {
		t.Fatalf("ConnID = %d, want 42 (promoted from placeholder)", merged[0].ConnID)
	}
	if merged[0].SlotState != "dialing" {
		t.Fatalf("SlotState lost on promotion: %q", merged[0].SlotState)
	}
	if merged[0].PeerID != "peer-1" {
		t.Fatalf("PeerID not enriched: %q", merged[0].PeerID)
	}
}

func TestApplyPeerHealthDeltaCreatesInboundRowsFromDelta(t *testing.T) {
	m := newTestMonitor()

	// No pre-existing rows. Delta declares outbound ConnID=42 and two
	// inbound connections. The monitor should create 3 rows total:
	// one outbound (ConnID=42) and two inbound (ConnID=100, ConnID=200).
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		PeerID:         "peer-abc",
		ConnID:         42,
		InboundConnIDs: []uint64{100, 200},
		Direction:      "outbound",
		Connected:      true,
		State:          "active",
		Score:          80,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 3 {
		t.Fatalf("expected 3 rows (1 outbound + 2 inbound), got %d", len(m.status.PeerHealth))
	}

	// Row 0: outbound (created in step 3).
	if m.status.PeerHealth[0].ConnID != 42 {
		t.Fatalf("row[0] ConnID = %d, want 42 (outbound)", m.status.PeerHealth[0].ConnID)
	}
	if m.status.PeerHealth[0].Direction != "outbound" {
		t.Fatalf("row[0] Direction = %q, want outbound", m.status.PeerHealth[0].Direction)
	}
	// Row 1: inbound ConnID=100 (created in step 4).
	if m.status.PeerHealth[1].ConnID != 100 {
		t.Fatalf("row[1] ConnID = %d, want 100 (inbound)", m.status.PeerHealth[1].ConnID)
	}
	if m.status.PeerHealth[1].Direction != "inbound" {
		t.Fatalf("row[1] Direction = %q, want inbound", m.status.PeerHealth[1].Direction)
	}
	if m.status.PeerHealth[1].PeerID != "peer-abc" {
		t.Fatalf("row[1] PeerID = %q, want peer-abc", m.status.PeerHealth[1].PeerID)
	}
	// Row 2: inbound ConnID=200.
	if m.status.PeerHealth[2].ConnID != 200 {
		t.Fatalf("row[2] ConnID = %d, want 200 (inbound)", m.status.PeerHealth[2].ConnID)
	}
	if m.status.PeerHealth[2].Direction != "inbound" {
		t.Fatalf("row[2] Direction = %q, want inbound", m.status.PeerHealth[2].Direction)
	}
	// All rows share address-level fields.
	for i, ph := range m.status.PeerHealth {
		if ph.Score != 80 {
			t.Fatalf("row[%d] Score = %d, want 80", i, ph.Score)
		}
		if ph.State != "active" {
			t.Fatalf("row[%d] State = %q, want active", i, ph.State)
		}
	}
}

// Regression test: before the fix, new inbound rows created from
// delta.InboundConnIDs had blank ClientVersion/ClientBuild/ProtocolVersion
// because the struct literal omitted those address-scoped fields. Subsequent
// deltas went through applyHealthDeltaToRow with writeSession=false and left
// them blank, so any inbound connection appearing after startup had empty
// version metadata on the peer card until a full probe refresh.
func TestApplyPeerHealthDeltaNewInboundRowsCarryVersionFields(t *testing.T) {
	m := newTestMonitor()

	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "10.0.0.1:9999",
		PeerID:          "peer-abc",
		ConnID:          42,
		InboundConnIDs:  []uint64{100, 200},
		Direction:       "outbound",
		Connected:       true,
		State:           "active",
		ClientVersion:   "v0.35",
		ClientBuild:     42,
		ProtocolVersion: 3,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 3 {
		t.Fatalf("expected 3 rows (1 outbound + 2 inbound), got %d", len(m.status.PeerHealth))
	}

	// Every row — outbound AND inbound — must carry the address-scoped
	// version metadata that peerHealthFrames() duplicates onto inbound rows
	// at probe time. Inbound rows that hit applyHealthDeltaToRow with
	// writeSession=false later cannot re-populate these fields, so missing
	// them here means the peer card will render a blank ClientVersion until
	// the next full probe.
	for i, ph := range m.status.PeerHealth {
		if ph.ClientVersion != "v0.35" {
			t.Fatalf("row[%d] (ConnID=%d) ClientVersion = %q, want v0.35", i, ph.ConnID, ph.ClientVersion)
		}
		if ph.ClientBuild != 42 {
			t.Fatalf("row[%d] (ConnID=%d) ClientBuild = %d, want 42", i, ph.ConnID, ph.ClientBuild)
		}
		if ph.ProtocolVersion != 3 {
			t.Fatalf("row[%d] (ConnID=%d) ProtocolVersion = %d, want 3", i, ph.ConnID, ph.ProtocolVersion)
		}
	}
}

// Regression test: once an inbound row has version metadata populated, a
// follow-up delta that goes through the writeSession=false path (address-level
// update, not a session rewrite) must not clear those fields. Combined with
// the creation-time fix above, this guarantees inbound rows carry version
// metadata for the lifetime of the connection.
func TestApplyPeerHealthDeltaInboundRowsVersionSurvivesFollowupDelta(t *testing.T) {
	m := newTestMonitor()

	// First delta: create outbound + inbound rows with version fields.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:         "10.0.0.1:9999",
		ConnID:          42,
		InboundConnIDs:  []uint64{100},
		Connected:       true,
		State:           "active",
		ClientVersion:   "v0.35",
		ClientBuild:     42,
		ProtocolVersion: 3,
	})

	// Second delta: still connected, same connections, no new version info.
	// Inbound row goes through applyHealthDeltaToRow(..., writeSession=false),
	// which is expected to leave session-scoped fields intact.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         42,
		InboundConnIDs: []uint64{100},
		Connected:      true,
		State:          "active",
		Score:          77,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	for i, ph := range m.status.PeerHealth {
		if ph.ConnID == 100 { // inbound row
			if ph.ClientVersion != "v0.35" {
				t.Fatalf("inbound row[%d] ClientVersion cleared to %q", i, ph.ClientVersion)
			}
			if ph.ClientBuild != 42 {
				t.Fatalf("inbound row[%d] ClientBuild cleared to %d", i, ph.ClientBuild)
			}
			if ph.ProtocolVersion != 3 {
				t.Fatalf("inbound row[%d] ProtocolVersion cleared to %d", i, ph.ProtocolVersion)
			}
		}
	}
}

func TestApplyPeerHealthDeltaPrunesDeadInboundRows(t *testing.T) {
	m := newTestMonitor()

	// Seed: outbound + 2 inbound connections.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         42,
		InboundConnIDs: []uint64{100, 200},
		Connected:      true,
		State:          "active",
	})

	m.mu.RLock()
	if len(m.status.PeerHealth) != 3 {
		t.Fatalf("setup: expected 3 rows, got %d", len(m.status.PeerHealth))
	}
	m.mu.RUnlock()

	// Second delta: ConnID=200 dropped (not in InboundConnIDs anymore).
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		ConnID:         42,
		InboundConnIDs: []uint64{100},
		Connected:      true,
		State:          "active",
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("expected 2 rows after pruning ConnID=200, got %d", len(m.status.PeerHealth))
	}

	connIDs := make(map[uint64]bool)
	for _, ph := range m.status.PeerHealth {
		connIDs[ph.ConnID] = true
	}
	if !connIDs[42] {
		t.Fatal("outbound ConnID=42 should survive")
	}
	if !connIDs[100] {
		t.Fatal("inbound ConnID=100 should survive")
	}
	if connIDs[200] {
		t.Fatal("inbound ConnID=200 should have been pruned")
	}
}

func TestApplyPeerHealthDeltaOutboundDisconnectPreservesInbound(t *testing.T) {
	m := newTestMonitor()

	// Seed: outbound + 1 inbound connection.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		PeerID:         "peer-abc",
		ConnID:         42,
		InboundConnIDs: []uint64{100},
		Connected:      true,
		State:          "active",
		Score:          80,
	})

	m.mu.RLock()
	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("setup: expected 2 rows, got %d", len(m.status.PeerHealth))
	}
	m.mu.RUnlock()

	// Outbound disconnects but inbound survives. Delta carries ConnID=0
	// (no outbound session) and InboundConnIDs=[100] (still alive).
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		PeerID:         "peer-abc",
		ConnID:         0,
		InboundConnIDs: []uint64{100},
		Connected:      true,
		State:          "degraded",
		Score:          30,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	// ConnID=42 outbound row should be pruned (not in expected set).
	// ConnID=100 inbound row survives.
	// No ConnID=0 row created (connected with existing inbound rows).
	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("expected 1 row (surviving inbound), got %d", len(m.status.PeerHealth))
	}
	ph := m.status.PeerHealth[0]
	if ph.ConnID != 100 {
		t.Fatalf("surviving row ConnID = %d, want 100 (inbound)", ph.ConnID)
	}
	if ph.Direction != "inbound" {
		t.Fatalf("surviving row Direction = %q, want inbound", ph.Direction)
	}
	// Address-level fields updated.
	if ph.State != "degraded" {
		t.Fatalf("State = %q, want degraded", ph.State)
	}
	if ph.Score != 30 {
		t.Fatalf("Score = %d, want 30", ph.Score)
	}
}

func TestApplyPeerHealthDeltaMixedOutboundInboundCoexistence(t *testing.T) {
	m := newTestMonitor()

	// First delta: outbound only.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "10.0.0.1:9999",
		PeerID:    "peer-abc",
		ConnID:    42,
		Direction: "outbound",
		Connected: true,
		State:     "active",
	})

	m.mu.RLock()
	if len(m.status.PeerHealth) != 1 {
		t.Fatalf("step 1: expected 1 row, got %d", len(m.status.PeerHealth))
	}
	m.mu.RUnlock()

	// Second delta: new inbound connection appears.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		PeerID:         "peer-abc",
		ConnID:         42,
		InboundConnIDs: []uint64{100},
		Direction:      "outbound",
		Connected:      true,
		State:          "active",
		Score:          90,
	})

	m.mu.RLock()
	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("step 2: expected 2 rows, got %d", len(m.status.PeerHealth))
	}
	m.mu.RUnlock()

	// Third delta: another inbound appears, first stays.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:        "10.0.0.1:9999",
		PeerID:         "peer-abc",
		ConnID:         42,
		InboundConnIDs: []uint64{100, 200},
		Direction:      "outbound",
		Connected:      true,
		State:          "healthy",
		Score:          95,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.PeerHealth) != 3 {
		t.Fatalf("step 3: expected 3 rows, got %d", len(m.status.PeerHealth))
	}

	connIDs := make(map[uint64]string)
	for _, ph := range m.status.PeerHealth {
		connIDs[ph.ConnID] = ph.Direction
		// All rows should have the latest address-level Score.
		if ph.Score != 95 {
			t.Fatalf("ConnID=%d Score = %d, want 95", ph.ConnID, ph.Score)
		}
		if ph.State != "healthy" {
			t.Fatalf("ConnID=%d State = %q, want healthy", ph.ConnID, ph.State)
		}
	}
	if connIDs[42] != "outbound" {
		t.Fatalf("ConnID=42 Direction = %q, want outbound", connIDs[42])
	}
	if connIDs[100] != "inbound" {
		t.Fatalf("ConnID=100 Direction = %q, want inbound", connIDs[100])
	}
	if connIDs[200] != "inbound" {
		t.Fatalf("ConnID=200 Direction = %q, want inbound", connIDs[200])
	}
}

func TestApplySlotStateDeltaUpdatesAllRowsForAddress(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100},
		{Address: "10.0.0.1:9999", ConnID: 200},
		{Address: "10.0.0.2:9999", ConnID: 300},
	}
	m.mu.Unlock()

	m.applySlotStateDelta("10.0.0.1:9999", "reconnecting")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].SlotState != "reconnecting" {
		t.Fatalf("row[0] SlotState = %q", m.status.PeerHealth[0].SlotState)
	}
	if m.status.PeerHealth[1].SlotState != "reconnecting" {
		t.Fatalf("row[1] SlotState = %q", m.status.PeerHealth[1].SlotState)
	}
	// Different address — untouched.
	if m.status.PeerHealth[2].SlotState != "" {
		t.Fatalf("row[2] SlotState = %q, should be empty", m.status.PeerHealth[2].SlotState)
	}
}

func TestApplyPeerPendingDeltaUpdatesAllRowsForAddress(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100},
		{Address: "10.0.0.1:9999", ConnID: 200},
	}
	m.mu.Unlock()

	m.applyPeerPendingDelta(ebus.PeerPendingDelta{
		Address: "10.0.0.1:9999",
		Count:   5,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	for i, ph := range m.status.PeerHealth {
		if ph.PendingCount != 5 {
			t.Fatalf("row[%d] PendingCount = %d, want 5", i, ph.PendingCount)
		}
	}
}

func TestApplyTrafficBatchUpdatesAllRowsForAddress(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{
		{Address: "10.0.0.1:9999", ConnID: 100},
		{Address: "10.0.0.1:9999", ConnID: 200},
	}
	m.mu.Unlock()

	m.applyTrafficBatch(ebus.PeerTrafficBatch{
		Peers: []ebus.PeerTrafficSnapshot{
			{Address: "10.0.0.1:9999", BytesSent: 1000, BytesReceived: 2000},
		},
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	for i, ph := range m.status.PeerHealth {
		if ph.BytesSent != 1000 {
			t.Fatalf("row[%d] BytesSent = %d, want 1000", i, ph.BytesSent)
		}
		if ph.BytesReceived != 2000 {
			t.Fatalf("row[%d] BytesReceived = %d, want 2000", i, ph.BytesReceived)
		}
		if ph.TotalTraffic != 3000 {
			t.Fatalf("row[%d] TotalTraffic = %d, want 3000", i, ph.TotalTraffic)
		}
	}
}

// ── applyCaptureStarted / applyCaptureStopped ──
//
// CaptureSession lives in a dedicated map on NodeStatus, keyed by ConnID.
// PeerHealth rows are never created or mutated by capture events — the UI
// reads recording state from NodeStatus.CaptureSessions so peer-health row
// pruning cannot race capture bookkeeping.

// newCaptureMonitor builds a monitor with an injectable clock and a short
// retention window so tests can verify the TTL sweep without sleeping.
func newCaptureMonitor(clock func() time.Time, retention time.Duration) *NodeStatusMonitor {
	return NewNodeStatusMonitor(NodeStatusMonitorOpts{
		OnChanged:        func() {},
		Clock:            clock,
		CaptureRetention: retention,
	})
}

func TestApplyCaptureStartedCreatesActiveEntry(t *testing.T) {
	m := newTestMonitor()

	startedAt := time.Now().UTC()
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(42),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		PeerID:    domain.PeerIdentity("peer-a"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/42.jsonl",
		StartedAt: &startedAt,
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatPretty,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.CaptureSessions) != 1 {
		t.Fatalf("expected 1 capture session, got %d", len(m.status.CaptureSessions))
	}
	session, ok := m.status.CaptureSessions[domain.ConnID(42)]
	if !ok {
		t.Fatal("expected session for ConnID 42")
	}
	if !session.Active {
		t.Fatal("session must be active after start")
	}
	if session.FilePath != "/tmp/captures/42.jsonl" {
		t.Fatalf("FilePath = %q", session.FilePath)
	}
	if session.Format != domain.CaptureFormatPretty {
		t.Fatalf("Format = %q, want %q", session.Format, domain.CaptureFormatPretty)
	}
	// PeerHealth must remain untouched — the capture path no longer
	// materializes peer rows.
	if len(m.status.PeerHealth) != 0 {
		t.Fatalf("PeerHealth must stay empty, got %d rows", len(m.status.PeerHealth))
	}
}

func TestApplyCaptureStartedDefaultsInvalidFormat(t *testing.T) {
	m := newTestMonitor()

	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(42),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/42.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormat(""),
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	session := m.status.CaptureSessions[domain.ConnID(42)]
	if session.Format != domain.CaptureFormatCompact {
		t.Fatalf("empty Format must default to compact, got %q", session.Format)
	}
}

func TestApplyCaptureStartedOverwritesStoppedEntryOnSameConnID(t *testing.T) {
	clock := &mockClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	m := newCaptureMonitor(clock.Now, time.Minute)

	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(42),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/old.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})
	m.applyCaptureStopped(ebus.CaptureSessionStopped{
		ConnID:        domain.ConnID(42),
		Error:         "disk full",
		DroppedEvents: 7,
	})
	clock.advance(time.Second) // still inside retention window

	// New session on the same ConnID must reset diagnostic counters.
	startedAt := clock.now
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(42),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/new.jsonl",
		StartedAt: &startedAt,
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.CaptureSessions) != 1 {
		t.Fatalf("expected 1 session after restart, got %d", len(m.status.CaptureSessions))
	}
	session := m.status.CaptureSessions[domain.ConnID(42)]
	if !session.Active {
		t.Fatal("session must be Active after restart")
	}
	if session.StoppedAt.Valid() {
		t.Fatal("StoppedAt must be cleared on restart")
	}
	if session.Error != "" {
		t.Fatalf("stale Error should be cleared, got %q", session.Error)
	}
	if session.DroppedEvents != 0 {
		t.Fatalf("stale DroppedEvents should be cleared, got %d", session.DroppedEvents)
	}
	if session.FilePath != "/tmp/captures/new.jsonl" {
		t.Fatalf("FilePath = %q", session.FilePath)
	}
}

func TestApplyCaptureStoppedMarksSessionStoppedWithDiagnostics(t *testing.T) {
	stopTime := time.Date(2026, 1, 1, 12, 0, 5, 0, time.UTC)
	clock := &mockClock{now: stopTime}
	m := newCaptureMonitor(clock.Now, time.Minute)

	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(42),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/42.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})
	m.applyCaptureStopped(ebus.CaptureSessionStopped{
		ConnID:        domain.ConnID(42),
		Error:         "write: disk full",
		DroppedEvents: 17,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	session, ok := m.status.CaptureSessions[domain.ConnID(42)]
	if !ok {
		t.Fatal("stopped session must linger for retention TTL")
	}
	if session.Active {
		t.Fatal("Active must be false after stop")
	}
	if !session.StoppedAt.Valid() || !session.StoppedAt.Time().Equal(stopTime) {
		t.Fatalf("StoppedAt = %v, want %v", session.StoppedAt, stopTime)
	}
	if session.Error != "write: disk full" {
		t.Fatalf("Error = %q", session.Error)
	}
	if session.DroppedEvents != 17 {
		t.Fatalf("DroppedEvents = %d, want 17", session.DroppedEvents)
	}
}

func TestApplyCaptureStoppedUnknownConnIDIsNoOp(t *testing.T) {
	m := newTestMonitor()

	// Seed an unrelated capture session on a different ConnID.
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(1),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/1.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})

	m.applyCaptureStopped(ebus.CaptureSessionStopped{
		ConnID:        domain.ConnID(42), // does not match any active session
		Error:         "detached",
		DroppedEvents: 3,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.status.CaptureSessions) != 1 {
		t.Fatalf("unrelated session must remain, got %d", len(m.status.CaptureSessions))
	}
	session := m.status.CaptureSessions[domain.ConnID(1)]
	if !session.Active {
		t.Fatal("unrelated session must stay Active")
	}
}

// TestEvictExpiredCaptureSessionsDropsStoppedAfterTTL guards the retention
// sweep: stopped sessions older than the retention window are removed on the
// next capture-handler mutation; Active sessions are never evicted.
func TestEvictExpiredCaptureSessionsDropsStoppedAfterTTL(t *testing.T) {
	clock := &mockClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	retention := 30 * time.Second
	m := newCaptureMonitor(clock.Now, retention)

	// Start two sessions: one will be stopped and age past retention, the
	// other stays active and must survive.
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(101),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/101.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(202),
		Address:   domain.PeerAddress("10.0.0.2:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/202.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})

	m.applyCaptureStopped(ebus.CaptureSessionStopped{ConnID: domain.ConnID(101)})
	clock.advance(retention + time.Second) // push 101 past TTL

	// A no-op start on a third ConnID triggers the sweep.
	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(303),
		Address:   domain.PeerAddress("10.0.0.3:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/303.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, evicted := m.status.CaptureSessions[domain.ConnID(101)]; evicted {
		t.Fatal("ConnID 101 must be evicted after retention")
	}
	if _, ok := m.status.CaptureSessions[domain.ConnID(202)]; !ok {
		t.Fatal("active ConnID 202 must survive sweep")
	}
	if _, ok := m.status.CaptureSessions[domain.ConnID(303)]; !ok {
		t.Fatal("freshly started ConnID 303 must be present")
	}
}

// TestMergeCaptureSessionsEbusWinsOnConflict guards the invariant that ebus
// is authoritative for every ConnID it has observed. A probe arriving after
// a stop event must not clobber the terminal diagnostics (Error, StoppedAt).
func TestMergeCaptureSessionsEbusWinsOnConflict(t *testing.T) {
	stopTime := time.Date(2026, 1, 1, 12, 0, 5, 0, time.UTC)
	clock := &mockClock{now: stopTime}
	m := newCaptureMonitor(clock.Now, time.Minute)

	m.applyCaptureStarted(ebus.CaptureSessionStarted{
		ConnID:    domain.ConnID(42),
		Address:   domain.PeerAddress("10.0.0.1:9999"),
		Direction: domain.PeerDirectionOutbound,
		FilePath:  "/tmp/captures/42.jsonl",
		Scope:     domain.CaptureScopeConnID,
		Format:    domain.CaptureFormatCompact,
	})
	m.applyCaptureStopped(ebus.CaptureSessionStopped{
		ConnID: domain.ConnID(42),
		Error:  "network reset",
	})

	// Probe snapshot claims the session is still active — ebus must win.
	m.SeedFromProbe(NodeStatus{
		CaptureSessions: map[domain.ConnID]CaptureSession{
			domain.ConnID(42): {
				ConnID:   domain.ConnID(42),
				Address:  domain.PeerAddress("10.0.0.1:9999"),
				FilePath: "/tmp/captures/42.jsonl",
				Scope:    domain.CaptureScopeConnID,
				Active:   true,
			},
		},
	})

	status := m.NodeStatus()
	session := status.CaptureSessions[domain.ConnID(42)]
	if session.Active {
		t.Fatal("ebus stop must win over probe claim of Active")
	}
	if session.Error != "network reset" {
		t.Fatalf("ebus Error must be preserved, got %q", session.Error)
	}
}

// TestMergeCaptureSessionsProbeFillsGaps guards the late-seed case where a
// recording started before the monitor subscribed — the probe then surfaces
// the pre-existing session via the fetch_peer_health snapshot.
func TestMergeCaptureSessionsProbeFillsGaps(t *testing.T) {
	m := newTestMonitor()

	m.SeedFromProbe(NodeStatus{
		CaptureSessions: map[domain.ConnID]CaptureSession{
			domain.ConnID(77): {
				ConnID:   domain.ConnID(77),
				Address:  domain.PeerAddress("10.0.0.7:9999"),
				FilePath: "/tmp/captures/77.jsonl",
				Scope:    domain.CaptureScopeConnID,
				Format:   domain.CaptureFormatCompact,
				Active:   true,
			},
		},
	})

	status := m.NodeStatus()
	if _, ok := status.CaptureSessions[domain.ConnID(77)]; !ok {
		t.Fatal("probe-only session must be surfaced by merge")
	}
}

// mockClock is an injectable time source used by capture tests. advance
// moves the clock forward deterministically without real sleep.
type mockClock struct {
	now time.Time
}

func (c *mockClock) Now() time.Time { return c.now }

func (c *mockClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// ── PeerHealthDelta diagnostic fields (Bug 4) ──
//
// NodeStatusMonitor now seeds from ProbeNode exactly once and relies on ebus
// for all subsequent updates. The test matrix below guards that every
// diagnostic field carried by the node's peerHealth struct actually reaches
// the subscriber — otherwise ban clears, handshake rejections, and recovery
// events leave the UI stuck on the startup snapshot until a full restart.

func TestApplyPeerHealthDeltaPropagatesBannedUntil(t *testing.T) {
	m := newTestMonitor()

	// Seed a row so this test exercises the update path of applyHealthDeltaToRow,
	// not just the row-creation path.
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{{
		Address: "1.2.3.4:9999",
		ConnID:  42,
	}}
	m.mu.Unlock()

	bannedUntil := time.Now().UTC().Add(10 * time.Minute)
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:     "1.2.3.4:9999",
		ConnID:      42,
		Connected:   false,
		State:       "banned",
		BannedUntil: &bannedUntil,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	if !ph.BannedUntil.Valid() {
		t.Fatal("BannedUntil must be set on the row")
	}
	if !ph.BannedUntil.Time().Equal(bannedUntil) {
		t.Fatalf("BannedUntil = %v, want %v", ph.BannedUntil, bannedUntil)
	}
}

func TestApplyPeerHealthDeltaClearsBannedUntilOnRecovery(t *testing.T) {
	m := newTestMonitor()

	// Seed a row with an active ban — simulating the post-FetchAndSeed state
	// where the probe captured an in-flight ban.
	bannedUntil := time.Now().UTC().Add(-time.Minute) // already expired
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{{
		Address:       "1.2.3.4:9999",
		ConnID:        42,
		BannedUntil:   domain.TimeOf(bannedUntil),
		LastErrorCode: "incompatible-protocol-version",
	}}
	m.mu.Unlock()

	// Node's resetPeerHealthForRecoveryLocked clears both fields on successful
	// reconnect. The delta carries the cleared values (nil pointer, empty
	// string) and applyHealthDeltaToRow must overwrite the stored state —
	// never treat the zero as "unchanged".
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:   "1.2.3.4:9999",
		ConnID:    42,
		Connected: true,
		State:     "active",
		// BannedUntil: nil  ← explicitly cleared
		// LastErrorCode: "" ← explicitly cleared
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	if ph.BannedUntil.Valid() {
		t.Fatalf("BannedUntil must be cleared on recovery, got %v", ph.BannedUntil)
	}
	if ph.LastErrorCode != "" {
		t.Fatalf("LastErrorCode must be cleared on recovery, got %q", ph.LastErrorCode)
	}
}

func TestApplyPeerHealthDeltaPropagatesIncompatibleVersionFields(t *testing.T) {
	m := newTestMonitor()

	lastIncompat := time.Now().UTC()
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:                     "1.2.3.4:9999",
		ConnID:                      42,
		Connected:                   false,
		State:                       "incompatible",
		LastErrorCode:               "incompatible-protocol-version",
		LastDisconnectCode:          "incompatible-protocol-version",
		IncompatibleVersionAttempts: domain.AttemptCount(3),
		LastIncompatibleVersionAt:   &lastIncompat,
		ObservedPeerVersion:         domain.ProtocolVersion(17),
		ObservedPeerMinimumVersion:  domain.ProtocolVersion(15),
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	ph := m.status.PeerHealth[0]
	if ph.LastErrorCode != "incompatible-protocol-version" {
		t.Fatalf("LastErrorCode = %q", ph.LastErrorCode)
	}
	if ph.LastDisconnectCode != "incompatible-protocol-version" {
		t.Fatalf("LastDisconnectCode = %q", ph.LastDisconnectCode)
	}
	if ph.IncompatibleVersionAttempts != 3 {
		t.Fatalf("IncompatibleVersionAttempts = %d, want 3", ph.IncompatibleVersionAttempts)
	}
	if !ph.LastIncompatibleVersionAt.Valid() || !ph.LastIncompatibleVersionAt.Time().Equal(lastIncompat) {
		t.Fatalf("LastIncompatibleVersionAt = %v, want %v", ph.LastIncompatibleVersionAt, lastIncompat)
	}
	if ph.ObservedPeerVersion != 17 {
		t.Fatalf("ObservedPeerVersion = %d, want 17", ph.ObservedPeerVersion)
	}
	if ph.ObservedPeerMinimumVersion != 15 {
		t.Fatalf("ObservedPeerMinimumVersion = %d, want 15", ph.ObservedPeerMinimumVersion)
	}
}

func TestApplyPeerHealthDeltaPropagatesVersionLockoutActive(t *testing.T) {
	m := newTestMonitor()

	// Seed the row so this proves the in-place update path carries the bit,
	// not just the creation path.
	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{{
		Address: "1.2.3.4:9999",
		ConnID:  42,
	}}
	m.mu.Unlock()

	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:              "1.2.3.4:9999",
		ConnID:               42,
		Connected:            false,
		State:                "incompatible",
		VersionLockoutActive: true,
	})

	m.mu.RLock()
	lockoutAfterFirst := m.status.PeerHealth[0].VersionLockoutActive
	m.mu.RUnlock()

	if !lockoutAfterFirst {
		t.Fatal("VersionLockoutActive must reach the row")
	}

	// Now clear: the node's lockout release emits a delta with
	// VersionLockoutActive=false; the row must follow, not retain the stale true.
	// NOTE: applyPeerHealthDelta takes m.mu.Lock(); the RLock above is released
	// before this call to avoid RLock↔Lock re-entry deadlock in the same
	// goroutine.
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:              "1.2.3.4:9999",
		ConnID:               42,
		Connected:            true,
		State:                "active",
		VersionLockoutActive: false,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.status.PeerHealth[0].VersionLockoutActive {
		t.Fatal("VersionLockoutActive must be cleared when delta says false")
	}
}

// TestApplyPeerHealthDeltaInboundRowCarriesDiagnostics guards step 4 of
// applyPeerHealthDelta (new-inbound-row creation). Diagnostic fields are
// address-level — inbound rows share the ban/version state with the
// outbound peer — so a fresh inbound row must be seeded with the same
// diagnostic snapshot. Without this, an incoming connection from a banned
// peer would render with a blank "Banned until —" cell until the next delta.
//
// Setup: pre-seed one inbound row (ConnID=77) so step 3's address-row
// fallback is suppressed (len(existingConnIDs) != 0). The delta adds a
// second inbound ConnID (78) — that fresh row is what this test guards.
func TestApplyPeerHealthDeltaInboundRowCarriesDiagnostics(t *testing.T) {
	m := newTestMonitor()

	m.mu.Lock()
	m.status.PeerHealth = []PeerHealth{{
		Address:   "1.2.3.4:9999",
		ConnID:    77,
		Direction: string(domain.PeerDirectionInbound),
	}}
	m.mu.Unlock()

	bannedUntil := time.Now().UTC().Add(5 * time.Minute)
	lastIncompat := time.Now().UTC()
	m.applyPeerHealthDelta(ebus.PeerHealthDelta{
		Address:                     "1.2.3.4:9999",
		PeerID:                      "peer-abc",
		Direction:                   domain.PeerDirectionInbound,
		ConnID:                      0, // inbound-only peer: no outbound session
		InboundConnIDs:              []uint64{77, 78},
		Connected:                   true,
		State:                       "active",
		BannedUntil:                 &bannedUntil,
		LastErrorCode:               "incompatible-protocol-version",
		LastDisconnectCode:          "incompatible-protocol-version",
		IncompatibleVersionAttempts: domain.AttemptCount(2),
		LastIncompatibleVersionAt:   &lastIncompat,
		ObservedPeerVersion:         domain.ProtocolVersion(19),
		ObservedPeerMinimumVersion:  domain.ProtocolVersion(18),
		VersionLockoutActive:        true,
	})

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Expect exactly 2 rows — the seeded inbound (77) and the newly created
	// inbound (78). Step 3 must NOT add a ConnID=0 address row because the
	// pre-seeded row makes len(existingConnIDs) != 0 in applyPeerHealthDelta.
	if len(m.status.PeerHealth) != 2 {
		t.Fatalf("expected 2 inbound rows, got %d", len(m.status.PeerHealth))
	}
	for _, ph := range m.status.PeerHealth {
		if ph.ConnID != 77 && ph.ConnID != 78 {
			t.Fatalf("unexpected ConnID %d", ph.ConnID)
		}
		if !ph.BannedUntil.Valid() || !ph.BannedUntil.Time().Equal(bannedUntil) {
			t.Fatalf("inbound row ConnID=%d BannedUntil = %v, want %v",
				ph.ConnID, ph.BannedUntil, bannedUntil)
		}
		if ph.LastErrorCode != "incompatible-protocol-version" {
			t.Fatalf("inbound row ConnID=%d LastErrorCode = %q",
				ph.ConnID, ph.LastErrorCode)
		}
		if ph.IncompatibleVersionAttempts != 2 {
			t.Fatalf("inbound row ConnID=%d IncompatibleVersionAttempts = %d, want 2",
				ph.ConnID, ph.IncompatibleVersionAttempts)
		}
		if ph.ObservedPeerVersion != 19 {
			t.Fatalf("inbound row ConnID=%d ObservedPeerVersion = %d, want 19",
				ph.ConnID, ph.ObservedPeerVersion)
		}
		if !ph.VersionLockoutActive {
			t.Fatalf("inbound row ConnID=%d VersionLockoutActive must be true", ph.ConnID)
		}
	}
}
