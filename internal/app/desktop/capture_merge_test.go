package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/service"
)

// TestMergeCapturesIntoPeers_NoCaptures asserts the function is a no-op when
// the capture map is empty — the common case on a node that is not currently
// recording. The caller must receive the exact same slice reference so no
// allocation is paid on the hot path.
func TestMergeCapturesIntoPeers_NoCaptures(t *testing.T) {
	peers := []service.PeerHealth{{Address: "1.2.3.4:5555", ConnID: 42}}
	out := mergeCapturesIntoPeers(peers, nil)
	if len(out) != len(peers) {
		t.Fatalf("expected %d rows, got %d", len(peers), len(out))
	}
	if &out[0] != &peers[0] {
		t.Fatalf("expected caller's slice to be returned unchanged")
	}
}

// TestMergeCapturesIntoPeers_OrphanCaptureSynthesized covers the core bug:
// a capture started before the first health delta must still produce a card.
// The synthetic row carries the capture's identity fields so layoutPeerSection
// can render direction, address, and (via the captures map) the recording dot.
func TestMergeCapturesIntoPeers_OrphanCaptureSynthesized(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		7: {
			ConnID:    7,
			Address:   domain.PeerAddress("10.0.0.1:9999"),
			PeerID:    domain.PeerIdentity("abc123"),
			Direction: domain.PeerDirectionOutbound,
			Active:    true,
		},
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 1 {
		t.Fatalf("expected 1 synthesized row, got %d", len(out))
	}
	got := out[0]
	if got.ConnID != 7 {
		t.Errorf("ConnID: want 7, got %d", got.ConnID)
	}
	if got.Address != "10.0.0.1:9999" {
		t.Errorf("Address: want %q, got %q", "10.0.0.1:9999", got.Address)
	}
	if got.PeerID != "abc123" {
		t.Errorf("PeerID: want %q, got %q", "abc123", got.PeerID)
	}
	if got.Direction != string(domain.PeerDirectionOutbound) {
		t.Errorf("Direction: want %q, got %q", domain.PeerDirectionOutbound, got.Direction)
	}
	if got.SlotState != "active" {
		t.Errorf("SlotState: outbound orphan should land in the Active group, got %q", got.SlotState)
	}
	if !got.Connected {
		t.Errorf("Connected: recording implies an open conn, want true")
	}
}

// TestMergeCapturesIntoPeers_InboundOrphanHasEmptySlotState verifies the
// direction-aware grouping key. Inbound captures must land in the "Inbound"
// bucket (empty SlotState), matching the behavior of real inbound PeerHealth
// rows that never carry a CM slot state.
func TestMergeCapturesIntoPeers_InboundOrphanHasEmptySlotState(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		3: {
			ConnID:    3,
			Address:   domain.PeerAddress("203.0.113.5:40000"),
			Direction: domain.PeerDirectionInbound,
			Active:    true,
		},
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 1 {
		t.Fatalf("expected 1 synthesized row, got %d", len(out))
	}
	if out[0].SlotState != "" {
		t.Errorf("SlotState: inbound orphan should use the Inbound bucket (\"\"), got %q", out[0].SlotState)
	}
}

// TestMergeCapturesIntoPeers_NoDuplicateWhenPeerExists asserts idempotency:
// when a PeerHealth row for the same ConnID is already present, the function
// must not inject a synthetic duplicate. This is the steady-state — any
// orphan-surfacing is strictly the transient start-of-capture window.
func TestMergeCapturesIntoPeers_NoDuplicateWhenPeerExists(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "1.2.3.4:5555", ConnID: 7, State: "healthy"},
	}
	captures := map[domain.ConnID]service.CaptureSession{
		7: {ConnID: 7, Address: "1.2.3.4:5555", Active: true},
	}
	out := mergeCapturesIntoPeers(peers, captures)
	if len(out) != 1 {
		t.Fatalf("expected 1 row (no duplicate), got %d", len(out))
	}
	if out[0].State != "healthy" {
		t.Errorf("expected real PeerHealth row to be preserved, got %+v", out[0])
	}
}

// TestMergeCapturesIntoPeers_StoppedCaptureIgnored guards the TTL-retention
// invariant: stopped sessions linger in the map so the UI can surface the
// terminal error, but they must not produce ghost cards after stop. Only
// Active=true captures get synthesized rows.
func TestMergeCapturesIntoPeers_StoppedCaptureIgnored(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		9: {ConnID: 9, Address: "1.2.3.4:1111", Active: false, Error: "disk full"},
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 0 {
		t.Fatalf("stopped session must not produce a synthesized row, got %d", len(out))
	}
}

// TestMergeCapturesIntoPeers_PreservesPeerOrder guarantees the merge appends
// orphan rows after real ones. Grouping by slot state is later order-agnostic,
// but callers that iterate the raw slice (e.g. debug logs, tests) rely on the
// invariant that merge is non-reordering.
func TestMergeCapturesIntoPeers_PreservesPeerOrder(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "a", ConnID: 1},
		{Address: "b", ConnID: 2},
	}
	captures := map[domain.ConnID]service.CaptureSession{
		99: {ConnID: 99, Address: "c", Active: true, Direction: domain.PeerDirectionInbound},
	}
	out := mergeCapturesIntoPeers(peers, captures)
	if len(out) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(out))
	}
	if out[0].ConnID != 1 || out[1].ConnID != 2 {
		t.Errorf("peer order not preserved, got %+v", out[:2])
	}
	if out[2].ConnID != 99 {
		t.Errorf("orphan row must be appended last, got ConnID=%d", out[2].ConnID)
	}
}

// TestMergeCapturesIntoPeers_MultipleOrphans asserts every active capture
// without a matching PeerHealth row gets its own synthetic entry. Predicting
// the exact append order is not worth the complexity — map iteration in Go
// is non-deterministic — so the test asserts set membership instead.
func TestMergeCapturesIntoPeers_MultipleOrphans(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		10: {ConnID: 10, Address: "x", Active: true, Direction: domain.PeerDirectionOutbound},
		11: {ConnID: 11, Address: "y", Active: true, Direction: domain.PeerDirectionInbound},
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 2 {
		t.Fatalf("expected 2 orphan rows, got %d", len(out))
	}
	ids := map[uint64]bool{out[0].ConnID: true, out[1].ConnID: true}
	if !ids[10] || !ids[11] {
		t.Errorf("expected ConnIDs {10, 11}, got %v", ids)
	}
}

// TestActiveRowsForTab_OrphanCaptureSurvivesEmptyHealth is the regression guard
// for the P1 bug: when status.PeerHealth has no active rows yet but an active
// capture exists, the tab must still produce a row so the empty-state gate in
// layoutPeersTab does not swallow the orphan. An earlier version filtered
// PeerHealth first and gated on that filter's size, hiding capture-only rows.
func TestActiveRowsForTab_OrphanCaptureSurvivesEmptyHealth(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: nil,
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			5: {
				ConnID:    5,
				Address:   domain.PeerAddress("198.51.100.7:8080"),
				Direction: domain.PeerDirectionOutbound,
				Active:    true,
			},
		},
	}
	rows := activeRowsForTab(status)
	if len(rows) != 1 {
		t.Fatalf("orphan capture must produce a row, got %d", len(rows))
	}
	if rows[0].ConnID != 5 {
		t.Errorf("expected ConnID=5, got %d", rows[0].ConnID)
	}
}

// TestActiveRowsForTab_FiltersInactivePeerHealth asserts the helper still runs
// PeerHealth through activePeerHealth (SlotState!="" || Connected). A stale
// disconnected row without a slot must be dropped, so the tab's counter
// reflects only actually-active peers and orphan captures.
func TestActiveRowsForTab_FiltersInactivePeerHealth(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "stale", Connected: false, SlotState: ""},
			{Address: "live", Connected: true, ConnID: 1},
		},
		CaptureSessions: nil,
	}
	rows := activeRowsForTab(status)
	if len(rows) != 1 {
		t.Fatalf("expected 1 active row, got %d", len(rows))
	}
	if rows[0].Address != "live" {
		t.Errorf("expected only the live row, got %+v", rows[0])
	}
}

// TestActiveRowsForTab_EmptyEverything is the steady-state quiet node: no
// peers, no captures → empty result → empty-state label rendered, no
// uptime invalidation scheduled. Critical for CPU behavior on idle nodes.
func TestActiveRowsForTab_EmptyEverything(t *testing.T) {
	rows := activeRowsForTab(service.NodeStatus{})
	if len(rows) != 0 {
		t.Fatalf("empty NodeStatus must produce zero rows, got %d", len(rows))
	}
}

// TestActiveRowsForTab_RealPeerPlusOrphanCapture covers the mixed case: a
// live PeerHealth row for one conn coexists with an orphan CaptureSession
// for a different conn. Both must appear, and neither must duplicate.
func TestActiveRowsForTab_RealPeerPlusOrphanCapture(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "real", ConnID: 1, Connected: true, State: "healthy"},
		},
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Address: "real", Active: true}, // matches existing row — no dup
			2: {ConnID: 2, Address: "orphan", Active: true, Direction: domain.PeerDirectionInbound},
		},
	}
	rows := activeRowsForTab(status)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (1 real + 1 orphan), got %d", len(rows))
	}
	seen := map[uint64]string{}
	for _, r := range rows {
		seen[r.ConnID] = r.Address
	}
	if seen[1] != "real" {
		t.Errorf("real peer lost, got %v", seen)
	}
	if seen[2] != "orphan" {
		t.Errorf("orphan capture not surfaced, got %v", seen)
	}
}

// TestCountConnectedPeers_IncludesOrphanCapture is the info-tab mirror of the
// peers-tab P1 bug: "Connected peers: N" must not drop to zero while a capture
// is active on an otherwise idle conn. An orphan capture is evidence of an
// open connection and must count.
func TestCountConnectedPeers_IncludesOrphanCapture(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: nil,
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {
				ConnID:  1,
				Address: domain.PeerAddress("1.2.3.4:5555"),
				PeerID:  domain.PeerIdentity("peer-x"),
				Active:  true,
			},
		},
	}
	if got := countConnectedPeers(status); got != 1 {
		t.Fatalf("countConnectedPeers = %d, want 1 (orphan capture must count)", got)
	}
}

// TestCountConnectedPeers_NoDoubleCount pins down the dedup invariant: when
// a PeerHealth row and a CaptureSession describe the same peer (same PeerID),
// the counter reports the peer once. Otherwise the user sees inflated numbers
// the moment capture starts, which is worse than the original miss.
func TestCountConnectedPeers_NoDoubleCount(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "1.2.3.4:5555", PeerID: "peer-x", Connected: true},
		},
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			42: {ConnID: 42, PeerID: "peer-x", Address: "1.2.3.4:5555", Active: true},
		},
	}
	if got := countConnectedPeers(status); got != 1 {
		t.Fatalf("countConnectedPeers = %d, want 1 (dedup by PeerID)", got)
	}
}

// TestCountConnectedPeers_IgnoresStoppedCapture guards the retention invariant:
// stopped sessions linger in the map for the UI's terminal-diagnostic window,
// but they must not inflate the connected counter after the writer is gone.
func TestCountConnectedPeers_IgnoresStoppedCapture(t *testing.T) {
	status := service.NodeStatus{
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, PeerID: "peer-x", Address: "x", Active: false, Error: "disk full"},
		},
	}
	if got := countConnectedPeers(status); got != 0 {
		t.Fatalf("countConnectedPeers = %d, want 0 (stopped session must not count)", got)
	}
}

// TestCountConnectedPeers_OrphanCaptureWithoutPeerID covers the pre-handshake
// window: capture may start before the peer's identity is known. Dedup then
// falls back to Address, so the counter still produces a stable number and
// does not panic on empty keys.
func TestCountConnectedPeers_OrphanCaptureWithoutPeerID(t *testing.T) {
	status := service.NodeStatus{
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			7: {ConnID: 7, Address: "10.0.0.1:9000", Active: true}, // no PeerID yet
		},
	}
	if got := countConnectedPeers(status); got != 1 {
		t.Fatalf("countConnectedPeers = %d, want 1 (fallback to Address key)", got)
	}
}

// TestCountUniquePeers_IncludesOrphanCapture mirrors the connected-peers case
// for the "known_peers" metric. A capture-started identity is observed by
// definition — the node has positive evidence of this peer's existence.
func TestCountUniquePeers_IncludesOrphanCapture(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: nil,
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Address: "1.2.3.4:5555", PeerID: "peer-x", Active: true},
		},
	}
	if got := countUniquePeers(status); got != 1 {
		t.Fatalf("countUniquePeers = %d, want 1 (orphan capture identity must count)", got)
	}
}

// TestCountUniquePeers_PreservesKnownButInactive protects the existing
// semantics: known-but-idle peers (queued/retry_wait slots, historical health
// rows) continue to be counted. The orphan-capture augmentation is purely
// additive — it must not filter anything out.
func TestCountUniquePeers_PreservesKnownButInactive(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "idle-1", SlotState: "queued"},
			{Address: "idle-2", SlotState: "retry_wait"},
		},
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Address: "live", PeerID: "peer-x", Active: true},
		},
	}
	if got := countUniquePeers(status); got != 3 {
		t.Fatalf("countUniquePeers = %d, want 3 (2 known-inactive + 1 orphan capture)", got)
	}
}

// TestCountUniquePeers_NoDoubleCount asserts that a peer with both a health
// row and an active capture is reported once. Dedup key is PeerID when set,
// so addresses can legitimately differ across the two sources (e.g., a
// reconnecting peer whose capture keeps the old address).
func TestCountUniquePeers_NoDoubleCount(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "new", PeerID: "peer-x", Connected: true},
		},
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Address: "old", PeerID: "peer-x", Active: true},
		},
	}
	if got := countUniquePeers(status); got != 1 {
		t.Fatalf("countUniquePeers = %d, want 1 (same PeerID across sources)", got)
	}
}

// TestPeerIdentityKey_PrefersPeerID pins down the dedup-key contract that
// both counters rely on: PeerID wins when set; Address is the fallback.
// This is worth testing directly because accidentally inverting the order
// would silently break dedup across address-rotation events.
func TestPeerIdentityKey_PrefersPeerID(t *testing.T) {
	if got := peerIdentityKey("peer-x", "1.2.3.4:5555"); got != "peer-x" {
		t.Errorf("peerIdentityKey with both: got %q, want %q", got, "peer-x")
	}
	if got := peerIdentityKey("", "1.2.3.4:5555"); got != "1.2.3.4:5555" {
		t.Errorf("peerIdentityKey fallback: got %q, want %q", got, "1.2.3.4:5555")
	}
	if got := peerIdentityKey("", ""); got != "" {
		t.Errorf("peerIdentityKey both empty: got %q, want empty", got)
	}
}

// TestSynthesizePeerHealthFromCapture_ZeroHealthFields pins down the zero-value
// invariant: synthesized rows must not fabricate health data. Only identity
// fields cross over from CaptureSession; State/BytesSent/LastPingAt etc. stay
// at their Go zero values so downstream consumers (summaries, sort orders)
// cannot mistake a capture-only row for a connection with observed health.
func TestSynthesizePeerHealthFromCapture_ZeroHealthFields(t *testing.T) {
	row := synthesizePeerHealthFromCapture(service.CaptureSession{
		ConnID:    42,
		Address:   "1.2.3.4:5555",
		Direction: domain.PeerDirectionInbound,
		Active:    true,
	})
	if row.State != "" {
		t.Errorf("State must be empty (no health delta yet), got %q", row.State)
	}
	if row.BytesSent != 0 || row.BytesReceived != 0 || row.TotalTraffic != 0 {
		t.Errorf("traffic counters must be zero, got sent=%d recv=%d total=%d",
			row.BytesSent, row.BytesReceived, row.TotalTraffic)
	}
	if row.Score != 0 {
		t.Errorf("Score must be zero, got %d", row.Score)
	}
	if row.LastPingAt.Valid() || row.LastPongAt.Valid() || row.LastConnectedAt.Valid() {
		t.Errorf("optional timestamps must be unset, got ping=%v pong=%v connected=%v",
			row.LastPingAt, row.LastPongAt, row.LastConnectedAt)
	}
}

// TestMergeCapturesIntoPeers_UnresolvedCaptureIgnored covers the P2 bug: an
// active capture whose publisher could not resolve the connection (both
// Address and PeerID empty) must not produce a synthetic PeerHealth row.
// The contract on ebus.CaptureSessionStarted explicitly permits empty
// identity, so the capture is still recorded server-side, but the desktop
// fallback has no renderable evidence of a peer and would otherwise show a
// blank card. Multiple such captures would render as independent blank
// cards — harmless visually but still misleading — so we skip them all.
func TestMergeCapturesIntoPeers_UnresolvedCaptureIgnored(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		7: {ConnID: 7, Active: true, Direction: domain.PeerDirectionOutbound}, // no Address, no PeerID
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 0 {
		t.Fatalf("unresolved capture must not produce a synthetic row, got %d", len(out))
	}
}

// TestMergeCapturesIntoPeers_MultipleUnresolvedCapturesIgnored asserts that
// several concurrent unresolved captures are all ignored — the fallback must
// not leak even one blank card, and must not depend on map iteration order.
func TestMergeCapturesIntoPeers_MultipleUnresolvedCapturesIgnored(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		1: {ConnID: 1, Active: true, Direction: domain.PeerDirectionOutbound},
		2: {ConnID: 2, Active: true, Direction: domain.PeerDirectionInbound},
		3: {ConnID: 3, Active: true}, // no direction either
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 0 {
		t.Fatalf("all unresolved captures must be skipped, got %d rows", len(out))
	}
}

// TestMergeCapturesIntoPeers_ResolvedCapturesSurviveAlongsideUnresolved mixes
// a resolved capture (Address present) with an unresolved one. Only the
// resolved capture may appear as a synthetic row — the unresolved capture
// is silently dropped from the peer list while still being recorded on
// NodeStatus.CaptureSessions for the "Stop all recordings" path.
func TestMergeCapturesIntoPeers_ResolvedCapturesSurviveAlongsideUnresolved(t *testing.T) {
	captures := map[domain.ConnID]service.CaptureSession{
		10: {ConnID: 10, Address: domain.PeerAddress("1.2.3.4:5555"), Active: true, Direction: domain.PeerDirectionOutbound},
		11: {ConnID: 11, Active: true, Direction: domain.PeerDirectionInbound}, // unresolved
	}
	out := mergeCapturesIntoPeers(nil, captures)
	if len(out) != 1 {
		t.Fatalf("expected 1 row (only the resolved capture), got %d", len(out))
	}
	if out[0].ConnID != 10 {
		t.Errorf("wrong capture surfaced: got ConnID=%d, want 10", out[0].ConnID)
	}
}

// TestCountConnectedPeers_IgnoresUnresolvedCapture guards against the
// phantom-peer inflation: an unresolved capture has no identity, so it
// cannot represent a distinct connected peer. Counting it would always
// land under the empty-string key in the seen map, producing exactly one
// ghost entry regardless of how many unresolved captures are active.
func TestCountConnectedPeers_IgnoresUnresolvedCapture(t *testing.T) {
	status := service.NodeStatus{
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Active: true}, // no Address, no PeerID
			2: {ConnID: 2, Active: true},
		},
	}
	if got := countConnectedPeers(status); got != 0 {
		t.Fatalf("countConnectedPeers = %d, want 0 (unresolved captures must not count)", got)
	}
}

// TestCountConnectedPeers_UnresolvedCaptureDoesNotMaskRealPeer covers the
// mixed case: a real connected peer plus one unresolved capture must report
// exactly 1, not 2 (phantom added) and not 0 (real peer dropped).
func TestCountConnectedPeers_UnresolvedCaptureDoesNotMaskRealPeer(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "1.2.3.4:5555", PeerID: "peer-x", Connected: true},
		},
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			99: {ConnID: 99, Active: true}, // unresolved, must not affect the count
		},
	}
	if got := countConnectedPeers(status); got != 1 {
		t.Fatalf("countConnectedPeers = %d, want 1 (unresolved capture must be ignored)", got)
	}
}

// TestCountUniquePeers_IgnoresUnresolvedCapture mirrors the P2 guard for
// known_peers: unlabeled captures cannot contribute to the distinct-peer
// count because they carry no identity at all. Dropping them prevents the
// "all unresolved captures collapse into one phantom peer" metric bug.
func TestCountUniquePeers_IgnoresUnresolvedCapture(t *testing.T) {
	status := service.NodeStatus{
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Active: true},
			2: {ConnID: 2, Active: true},
		},
	}
	if got := countUniquePeers(status); got != 0 {
		t.Fatalf("countUniquePeers = %d, want 0 (unresolved captures must not count)", got)
	}
}

// TestCountUniquePeers_UnresolvedCaptureDoesNotMaskKnownPeer asserts the
// additive semantics hold in the mixed case: a known-but-inactive peer
// coexists with an unresolved capture — only the known peer is counted.
func TestCountUniquePeers_UnresolvedCaptureDoesNotMaskKnownPeer(t *testing.T) {
	status := service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "known", SlotState: "queued"},
		},
		CaptureSessions: map[domain.ConnID]service.CaptureSession{
			1: {ConnID: 1, Active: true}, // unresolved
		},
	}
	if got := countUniquePeers(status); got != 1 {
		t.Fatalf("countUniquePeers = %d, want 1 (known peer only)", got)
	}
}

// TestMergeCapturesIntoPeers_PromotesSlotPlaceholderForSameAddress covers the
// P2 split-state bug at the UI merge layer: applySlotStateDelta can seed a
// ConnID=0, SlotState="dialing" placeholder for an address before any health
// delta arrives. When an active capture then lands for the same address with
// a real ConnID, the merge must NOT append a second card — it must promote
// the placeholder in-place so a single row carries both the slot lifecycle
// state and the recording dot (which layoutPeerSection looks up by ConnID).
func TestMergeCapturesIntoPeers_PromotesSlotPlaceholderForSameAddress(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "1.2.3.4:5555", SlotState: "dialing"}, // ConnID=0 placeholder
	}
	captures := map[domain.ConnID]service.CaptureSession{
		7: {
			ConnID:    7,
			Address:   domain.PeerAddress("1.2.3.4:5555"),
			PeerID:    domain.PeerIdentity("peer-x"),
			Direction: domain.PeerDirectionOutbound,
			Active:    true,
		},
	}
	out := mergeCapturesIntoPeers(peers, captures)
	if len(out) != 1 {
		t.Fatalf("expected 1 merged row (placeholder promoted), got %d", len(out))
	}
	row := out[0]
	if row.ConnID != 7 {
		t.Errorf("ConnID must come from capture, got %d", row.ConnID)
	}
	if row.Address != "1.2.3.4:5555" {
		t.Errorf("Address preserved from placeholder, got %q", row.Address)
	}
	if row.SlotState != "dialing" {
		t.Errorf("SlotState must be preserved — CM-managed state is authoritative, got %q", row.SlotState)
	}
	if row.PeerID != "peer-x" {
		t.Errorf("PeerID must be filled from capture, got %q", row.PeerID)
	}
	if row.Direction != string(domain.PeerDirectionOutbound) {
		t.Errorf("Direction must come from capture, got %q", row.Direction)
	}
	if !row.Connected {
		t.Errorf("Connected must be true — recording implies an open conn")
	}
}

// TestMergeCapturesIntoPeers_PromotionDoesNotMutateCallerSlice protects the
// function's published contract: the input slice's entries must stay untouched
// even when an in-place-looking promotion happens. The implementation clones
// before mutating so any caller reading the original slice after merge (e.g.,
// diagnostic snapshots) still sees the placeholder unchanged.
func TestMergeCapturesIntoPeers_PromotionDoesNotMutateCallerSlice(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "1.2.3.4:5555", SlotState: "dialing"},
	}
	snapshot := peers[0]
	captures := map[domain.ConnID]service.CaptureSession{
		7: {
			ConnID:    7,
			Address:   "1.2.3.4:5555",
			Direction: domain.PeerDirectionOutbound,
			Active:    true,
		},
	}
	_ = mergeCapturesIntoPeers(peers, captures)
	if peers[0] != snapshot {
		t.Errorf("caller's slice entry was mutated: got %+v, want %+v", peers[0], snapshot)
	}
}

// TestMergeCapturesIntoPeers_PromotionPreservesExistingPeerID guards against
// stomping an already-known identity. A placeholder can legitimately carry a
// non-empty PeerID (enrichment from a probe snapshot) before a health delta
// lands. The capture's PeerID must NOT overwrite it — the placeholder's
// evidence was observed earlier and we have no reason to discard it.
func TestMergeCapturesIntoPeers_PromotionPreservesExistingPeerID(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "1.2.3.4:5555", SlotState: "queued", PeerID: "placeholder-id"},
	}
	captures := map[domain.ConnID]service.CaptureSession{
		7: {
			ConnID:  7,
			Address: "1.2.3.4:5555",
			PeerID:  "different-id",
			Active:  true,
		},
	}
	out := mergeCapturesIntoPeers(peers, captures)
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	if out[0].PeerID != "placeholder-id" {
		t.Errorf("existing PeerID must be preserved, got %q", out[0].PeerID)
	}
}

// TestMergeCapturesIntoPeers_PromotionPlusOrphanSynthesis mixes both paths:
// one capture matches an existing placeholder (promotion), another capture
// has no matching row (synthesis). Result must be exactly two rows — one
// promoted, one synthetic — and the caller's placeholder stays untouched.
func TestMergeCapturesIntoPeers_PromotionPlusOrphanSynthesis(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "dialing-addr", SlotState: "dialing"}, // placeholder
	}
	captures := map[domain.ConnID]service.CaptureSession{
		7:  {ConnID: 7, Address: "dialing-addr", Active: true, Direction: domain.PeerDirectionOutbound},
		99: {ConnID: 99, Address: "orphan-addr", Active: true, Direction: domain.PeerDirectionInbound},
	}
	out := mergeCapturesIntoPeers(peers, captures)
	if len(out) != 2 {
		t.Fatalf("expected 2 rows (1 promoted + 1 orphan), got %d", len(out))
	}
	byAddr := map[string]service.PeerHealth{}
	for _, r := range out {
		byAddr[r.Address] = r
	}
	promoted, ok := byAddr["dialing-addr"]
	if !ok {
		t.Fatalf("promoted row missing for address %q", "dialing-addr")
	}
	if promoted.ConnID != 7 {
		t.Errorf("promoted row ConnID: want 7, got %d", promoted.ConnID)
	}
	if promoted.SlotState != "dialing" {
		t.Errorf("promoted row must keep placeholder SlotState, got %q", promoted.SlotState)
	}
	orphan, ok := byAddr["orphan-addr"]
	if !ok {
		t.Fatalf("orphan row missing for address %q", "orphan-addr")
	}
	if orphan.ConnID != 99 {
		t.Errorf("orphan row ConnID: want 99, got %d", orphan.ConnID)
	}
}

// TestMergeCapturesIntoPeers_NonZeroConnIDRowBlocksPromotion asserts the
// placeholder match is strictly ConnID=0. A pre-existing row with a real
// ConnID for the same address (different connection) must NOT be promoted;
// the capture must produce an independent synthetic row. Otherwise we would
// silently retarget a live conn's row onto another conn's ConnID.
func TestMergeCapturesIntoPeers_NonZeroConnIDRowBlocksPromotion(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "shared-addr", ConnID: 5, Connected: true}, // real row, different conn
	}
	captures := map[domain.ConnID]service.CaptureSession{
		8: {ConnID: 8, Address: "shared-addr", Active: true, Direction: domain.PeerDirectionInbound},
	}
	out := mergeCapturesIntoPeers(peers, captures)
	if len(out) != 2 {
		t.Fatalf("expected 2 rows (real + synthetic), got %d", len(out))
	}
}
