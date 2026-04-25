package routing_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

// v2WireRecorder counts and snapshots calls to both PeerSender wire methods.
// The forced-full / first-sync / divergence-fallback paths are observable by
// looking at which method ran for which payload — there is no other state on
// the AnnounceLoop side that distinguishes the modes after the fact.
type v2WireRecorder struct {
	announceCalls atomic.Int32
	updateCalls   atomic.Int32

	mu       sync.Mutex
	announce []v2SendCall
	update   []v2SendCall
}

type v2SendCall struct {
	Address routing.PeerAddress
	Routes  []routing.AnnounceEntry
}

func (r *v2WireRecorder) recordAnnounce(addr routing.PeerAddress, routes []routing.AnnounceEntry) {
	r.announceCalls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]routing.AnnounceEntry, len(routes))
	copy(cp, routes)
	r.announce = append(r.announce, v2SendCall{Address: addr, Routes: cp})
}

func (r *v2WireRecorder) recordUpdate(addr routing.PeerAddress, routes []routing.AnnounceEntry) {
	r.updateCalls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]routing.AnnounceEntry, len(routes))
	copy(cp, routes)
	r.update = append(r.update, v2SendCall{Address: addr, Routes: cp})
}

func (r *v2WireRecorder) snapshotUpdate() []v2SendCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]v2SendCall, len(r.update))
	copy(cp, r.update)
	return cp
}

// newV2WireSender wires the auto-generated MockPeerSender to a v2WireRecorder.
// Both methods always return true so the per-peer Record* calls fire and the
// next cycle observes the cached snapshot.
func newV2WireSender(t *testing.T) (*routingmocks.MockPeerSender, *v2WireRecorder) {
	t.Helper()
	rec := &v2WireRecorder{}
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, addr routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			rec.recordAnnounce(addr, routes)
			return true
		},
	).Maybe()
	m.EXPECT().SendRoutesUpdate(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, addr routing.PeerAddress, delta []routing.AnnounceEntry) bool {
			rec.recordUpdate(addr, delta)
			return true
		},
	).Maybe()
	return m, rec
}

// runOneCycle runs a single AnnounceLoop cycle by triggering an update,
// waiting for the per-peer goroutines to finish, and cancelling. It is the
// shared fixture for the mode-selection tests below — every test exercises
// the loop's single-cycle behaviour and inspects which wire method ran.
func runOneCycle(t *testing.T, loop *routing.AnnounceLoop) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
}

// caps returns a routing.PeerCapability slice for the given domain values.
// Helper exists so test cases stay readable when constructing capability
// snapshots inline.
func caps(values ...domain.Capability) []routing.PeerCapability {
	out := make([]routing.PeerCapability, len(values))
	copy(out, values)
	return out
}

// TestAnnounceLoop_DeltaModeV2_BothCapsAgree verifies the v2 happy path:
// when both AnnouncePeerState.Capabilities (set via MarkReconnected) and the
// per-cycle AnnounceTarget.Capabilities (returned by peersFn) advertise both
// CapMeshRoutingV1 and CapMeshRoutingV2, the delta sent after the legacy
// baseline is carried by SendRoutesUpdate. This is the only path on which
// the v2 wire frame may fire — every other test in this file asserts the
// negative shape of that path.
func TestAnnounceLoop_DeltaModeV2_BothCapsAgree(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry()

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	v2Caps := caps(domain.CapMeshRoutingV1, domain.CapMeshRoutingV2)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: v2Caps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	// Persistent capability snapshot — populated by node.Service via
	// MarkReconnected on session establishment in production.
	registry.MarkReconnected("peer-C", v2Caps)

	// First cycle: legacy baseline (first-sync invariant — full sync is
	// always SendAnnounceRoutes regardless of v2 capability).
	runOneCycle(t, loop)
	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("first sync must use SendAnnounceRoutes exactly once, got %d", got)
	}
	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("first sync must NOT use SendRoutesUpdate, got %d calls", got)
	}

	// Add a second route so the next cycle has a non-empty delta.
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatalf("AddDirectPeer peer-D: %v", err)
	}

	// Second cycle: delta path — v2 mode must fire SendRoutesUpdate.
	runOneCycle(t, loop)
	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("v2 delta must NOT add another SendAnnounceRoutes call, got total %d", got)
	}
	if got := rec.updateCalls.Load(); got != 1 {
		t.Fatalf("v2 delta must fire SendRoutesUpdate exactly once, got %d", got)
	}

	updates := rec.snapshotUpdate()
	if updates[0].Address != "addr-C" {
		t.Fatalf("expected SendRoutesUpdate addressed to addr-C, got %s", updates[0].Address)
	}
	if len(updates[0].Routes) == 0 {
		t.Fatalf("v2 delta payload must be non-empty")
	}
}

// TestAnnounceLoop_DeltaModeV1_LegacyPeer verifies that a peer advertising
// neither v1 nor v2 in either capability source still receives both the
// first-sync baseline and the subsequent delta on the legacy
// SendAnnounceRoutes wire frame. This is the mixed-version invariant — a
// pre-v2 node never sees a routes_update frame.
func TestAnnounceLoop_DeltaModeV1_LegacyPeer(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry()

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	// Legacy peer: empty capabilities on both sources.
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: nil},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	registry.MarkReconnected("peer-C", nil)

	runOneCycle(t, loop)
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatalf("AddDirectPeer peer-D: %v", err)
	}
	runOneCycle(t, loop)

	if got := rec.announceCalls.Load(); got != 2 {
		t.Fatalf("legacy peer expects 2 SendAnnounceRoutes calls (full + delta), got %d", got)
	}
	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("legacy peer must NEVER receive SendRoutesUpdate, got %d", got)
	}
}

// TestAnnounceLoop_DeltaModeV2WithoutV1_TreatedAsV1 verifies that a peer
// advertising CapMeshRoutingV2 alone (without CapMeshRoutingV1) is treated
// as v1-only on both sources. v2 is opt-in atop v1 — see the type doc on
// CapMeshRoutingV2 — and a peer that "skips" v1 cannot receive the v2
// frame because the first-sync legacy delivery is gated on v1.
func TestAnnounceLoop_DeltaModeV2WithoutV1_TreatedAsV1(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry()

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	// V2 without V1 — illegal per spec but must not crash; classification
	// downgrades to v1, which then reaches the same agreement state as a
	// fully-legacy peer (both sources lack v2 in the "both v1+v2" sense).
	v2OnlyCaps := caps(domain.CapMeshRoutingV2)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: v2OnlyCaps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	registry.MarkReconnected("peer-C", v2OnlyCaps)

	runOneCycle(t, loop)
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatalf("AddDirectPeer peer-D: %v", err)
	}
	runOneCycle(t, loop)

	if got := rec.announceCalls.Load(); got != 2 {
		t.Fatalf("v2-without-v1 peer must take the v1 path: expected 2 SendAnnounceRoutes, got %d", got)
	}
	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("v2-without-v1 peer must NEVER receive SendRoutesUpdate, got %d", got)
	}
}

// TestAnnounceLoop_PersistentVsTargetCapsDisagree_SyncsToTarget pins the
// cycle-time reconciliation that prevents the persistent
// AnnouncePeerState.capabilities snapshot from diverging from the
// AnnounceTarget caps the cycle actually picked. peersFn dedupes
// overlapping sessions for one identity by map iteration order, and a
// partial close (a routing-capable session that previously refreshed the
// snapshot disappears while an older routing-capable session remains)
// can also leave the persistent snapshot describing a session that is no
// longer the chosen target. Either path can produce divergence between
// the two capability sources at cycle entry — the cycle MUST reconcile by
// snapping the persistent snapshot to the per-cycle target before
// classifyDeltaMode runs, so the classification reads two agreeing
// snapshots and never falls into the divergence/forced-full loop.
//
// Setup: persistent state advertises v1+v2 (e.g. left over from a prior
// session), per-cycle target advertises v1 only (the session
// routingCapablePeers ended up picking has no v2). After the cycle, the
// persistent snapshot must equal the target caps and the cycle must take
// the V1 delta path (not divergence, not MarkInvalid).
func TestAnnounceLoop_PersistentVsTargetCapsDisagree_SyncsToTarget(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry()

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	// Persistent state advertises v1+v2 (last MarkReconnected snapshot —
	// imagine this was set by an earlier session that has since been
	// replaced or de-selected by routingCapablePeers).
	stateCaps := caps(domain.CapMeshRoutingV1, domain.CapMeshRoutingV2)
	registry.MarkReconnected("peer-C", stateCaps)

	// Per-cycle target snapshot only advertises v1 — the session
	// routingCapablePeers picked this cycle does not have v2.
	targetCaps := caps(domain.CapMeshRoutingV1)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: targetCaps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	// First cycle: forced full (NeedsFullResync=true after MarkReconnected).
	// Cycle-time sync runs first, snapping state.caps to {v1}.
	runOneCycle(t, loop)
	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("first sync must use legacy wire frame exactly once, got %d", got)
	}

	state := registry.Get("peer-C")
	if state == nil {
		t.Fatalf("peer-C state missing after first cycle")
	}
	post := state.View()
	if len(post.CapabilitiesSnapshot) != 1 || post.CapabilitiesSnapshot[0] != domain.CapMeshRoutingV1 {
		t.Fatalf("cycle-time sync must drop persistent caps to target {v1}, got %v", post.CapabilitiesSnapshot)
	}

	// Add a route so the next cycle takes the delta path.
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatalf("AddDirectPeer peer-D: %v", err)
	}

	// Second cycle: state.caps == target.caps == {v1} after sync, so
	// classifyDeltaMode returns deltaModeV1 — NOT divergence, MarkInvalid
	// is NOT fired. The delta uses legacy SendAnnounceRoutes.
	runOneCycle(t, loop)
	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("V1 delta after sync must NEVER call SendRoutesUpdate, got %d", got)
	}
	if got := rec.announceCalls.Load(); got != 2 {
		t.Fatalf("V1 delta must fire SendAnnounceRoutes once more (total 2), got %d", got)
	}

	post = state.View()
	if post.NeedsFullResync {
		t.Fatalf("cycle-time sync resolves caps to target — MarkInvalid must NOT have fired, but NeedsFullResync==true")
	}
}

// TestAnnounceLoop_FirstSync_V2Capable_StillLegacy is the v2-aware
// counterpart of TestAnnounceLoop_FullSync_UsesLegacyFrame: even when a peer
// advertises full v1+v2 capability on both sources, the first sync (no
// baseline yet) MUST go through SendAnnounceRoutes. This is the protocol-level
// "First-sync wire-frame invariant" documented in docs/routing.md.
func TestAnnounceLoop_FirstSync_V2Capable_StillLegacy(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry()

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	v2Caps := caps(domain.CapMeshRoutingV1, domain.CapMeshRoutingV2)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: v2Caps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	registry.MarkReconnected("peer-C", v2Caps)

	runOneCycle(t, loop)

	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("v2-capable peer first sync must call SendAnnounceRoutes exactly once, got %d", got)
	}
	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("v2-capable peer first sync must NEVER call SendRoutesUpdate, got %d", got)
	}
}

// TestAnnounceLoop_DeltaModeV2_DowngradedWithoutWireBaseline pins the v2
// receive-gate fix: when both capability sources agree on v2 but no legacy
// announce_routes frame has actually gone out to the peer in the current
// session (e.g. the connect-time snapshot was empty and the empty-baseline
// branch recorded a successful local baseline without emitting any wire
// frame), the next non-empty delta MUST fall back to legacy
// SendAnnounceRoutes so the peer's v2 receive gate observes the baseline
// before any routes_update arrives. After the legacy delta lands, the
// wire-baseline flag flips and the cycle after may pick the v2 frame.
//
// Without this gate the v2 routes_update arrives at a peer whose
// HasReceivedBaseline() is false, the peer drops it and emits
// request_resync, and we converge with one wasted round-trip per session
// for every empty-then-non-empty progression.
func TestAnnounceLoop_DeltaModeV2_DowngradedWithoutWireBaseline(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clock))

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	v2Caps := caps(domain.CapMeshRoutingV1, domain.CapMeshRoutingV2)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: v2Caps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	// Persistent caps via MarkReconnected — both sources agree on v1+v2.
	registry.MarkReconnected("peer-C", v2Caps)

	// Simulate the empty-baseline branch: a successful baseline was recorded
	// locally without any wire frame having been emitted. lastSentSnapshot
	// is non-nil so the loop takes the delta path, but the wire-baseline
	// flag stays false so the override below must downgrade v2→v1.
	state := registry.GetOrCreate("peer-C")
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now)
	if state.HasSentWireBaseline() {
		t.Fatalf("precondition: empty-baseline must NOT flip wire-baseline to true")
	}

	// Cycle 1: delta has 1 entry (peer-B). classifyDeltaMode would normally
	// return V2 — but the wire-baseline override must force legacy on this
	// first observable wire frame.
	runOneCycle(t, loop)
	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("first delta after empty baseline must NOT call SendRoutesUpdate, got %d", got)
	}
	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("first delta after empty baseline must call SendAnnounceRoutes exactly once, got %d", got)
	}
	if !state.HasSentWireBaseline() {
		t.Fatalf("after a successful legacy delta, HasSentWireBaseline must be true")
	}

	// Add a new route so the next cycle has a non-empty delta on top of the
	// just-established baseline.
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatalf("AddDirectPeer peer-D: %v", err)
	}

	// Cycle 2: same caps, but the wire-baseline flag is now true → v2 path.
	runOneCycle(t, loop)
	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("second cycle must NOT add another SendAnnounceRoutes call, got total %d", got)
	}
	if got := rec.updateCalls.Load(); got != 1 {
		t.Fatalf("second cycle must call SendRoutesUpdate exactly once, got %d", got)
	}
}

// TestAnnounceLoop_DeltaModeV2_ForcedFullEmptyBaseline_KeepsWireBaselineFalse
// pins the orthogonal forced-full path: if a forced-full cycle has an empty
// snapshot, the empty-baseline branch records a successful baseline locally
// without sending any wire frame and must NOT flip the wire-baseline flag.
// The first non-empty cycle after that empty forced-full is therefore
// downgraded to legacy by the same override as the connect-time path.
func TestAnnounceLoop_DeltaModeV2_ForcedFullEmptyBaseline_KeepsWireBaselineFalse(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	// Empty table — every snapshot built from it is empty.
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, _ := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clock))

	v2Caps := caps(domain.CapMeshRoutingV1, domain.CapMeshRoutingV2)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: v2Caps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	registry.MarkReconnected("peer-C", v2Caps)

	// Forced full sync against an empty table — records baseline, no wire.
	runOneCycle(t, loop)

	state := registry.Get("peer-C")
	if state == nil {
		t.Fatalf("peer-C state must exist after the cycle")
	}
	if state.HasSentWireBaseline() {
		t.Fatalf("forced-full empty-baseline must NOT flip wire-baseline to true")
	}
}

// TestAnnounceLoop_ForcedFull_V2Capable_StillLegacy verifies the periodic
// forced-full path: even after the v2 baseline has been delivered, a peer
// marked NeedsFullResync (e.g. by MarkInvalid or by the periodic full-sync
// scheduler) must receive the full sync via the legacy SendAnnounceRoutes
// wire frame. v2 routes_update is reserved for incremental deltas, never
// for full snapshots — see sendFullAnnounce / sendLegacyFull in announce.go
// and the "First-sync wire-frame invariant" section of docs/routing.md.
func TestAnnounceLoop_ForcedFull_V2Capable_StillLegacy(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newV2WireSender(t)
	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clock))

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatalf("AddDirectPeer peer-B: %v", err)
	}

	v2Caps := caps(domain.CapMeshRoutingV1, domain.CapMeshRoutingV2)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C", Capabilities: v2Caps},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	registry.MarkReconnected("peer-C", v2Caps)

	// Seed a baseline that already exists (RecordFullSyncSuccess) and then
	// flip NeedsFullResync via the test export. The rate-limit clamp does
	// not bite because LastFullSyncAttemptAt is zero on a freshly recorded
	// success.
	state := registry.GetOrCreate("peer-C")
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now.Add(-1*time.Minute))
	state.SetNeedsFullResyncForTest()

	runOneCycle(t, loop)

	if got := rec.updateCalls.Load(); got != 0 {
		t.Fatalf("forced-full on a v2-capable peer must NEVER call SendRoutesUpdate, got %d", got)
	}
	if got := rec.announceCalls.Load(); got != 1 {
		t.Fatalf("forced-full must call SendAnnounceRoutes exactly once, got %d", got)
	}
}
