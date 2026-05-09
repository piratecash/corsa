package routing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

type senderRecorder struct {
	mu    sync.Mutex
	calls []sendCall
}

type sendCall struct {
	PeerAddress routing.PeerAddress
	Routes      []routing.AnnounceEntry
}

func (r *senderRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *senderRecorder) lastCall() sendCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return sendCall{}
	}
	return r.calls[len(r.calls)-1]
}

func newMockPeerSender(t *testing.T) (*routingmocks.MockPeerSender, *senderRecorder) {
	t.Helper()
	rec := &senderRecorder{}
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, addr routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			rec.mu.Lock()
			rec.calls = append(rec.calls, sendCall{PeerAddress: addr, Routes: routes})
			rec.mu.Unlock()
			return true
		},
	).Maybe()
	return m, rec
}

func TestAnnounceLoopPeriodicSend(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Wait for at least one periodic cycle.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if rec.callCount() == 0 {
		t.Fatal("expected at least one periodic announcement, got 0")
	}

	call := rec.lastCall()
	if call.PeerAddress != "addr-C" {
		t.Fatalf("expected peer address addr-C, got %s", call.PeerAddress)
	}
	if len(call.Routes) == 0 {
		t.Fatal("expected at least one route in the announcement")
	}
}

func TestAnnounceLoopTriggeredUpdate(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	// Use a very long interval so only triggered updates fire.
	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Trigger an immediate update.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if rec.callCount() == 0 {
		t.Fatal("expected triggered announcement, got 0")
	}

	cancel()
	<-done
}

func TestAnnounceLoopSplitHorizon(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Announce to peer-B: the direct route for peer-B was learned via
	// peer-B (NextHop == peer-B), so it should be excluded by split horizon.
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-B", Identity: "peer-B"},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

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

	// Split horizon should exclude routes learned from peer-B when
	// announcing to peer-B. The direct route has NextHop == "peer-B",
	// which equals the announce target identity "peer-B", so no routes
	// should be sent.
	if rec.callCount() != 0 {
		t.Fatalf("expected 0 calls due to split horizon, got %d", rec.callCount())
	}
}

func TestAnnounceLoopNoPeersNoSend(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// No peers to announce to.
	peers := func() []routing.AnnounceTarget {
		return nil
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if rec.callCount() != 0 {
		t.Fatalf("expected 0 sends with no peers, got %d", rec.callCount())
	}
}

func TestAnnounceLoopTriggerCoalesces(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Fire multiple triggers rapidly — they should coalesce.
	for i := 0; i < 10; i++ {
		loop.TriggerUpdate()
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Due to coalescing, we expect far fewer than 10 sends.
	// At least 1 (the first trigger), at most a few.
	count := rec.callCount()
	if count == 0 {
		t.Fatal("expected at least 1 triggered send")
	}
	if count > 5 {
		t.Fatalf("expected coalescing to limit sends, got %d", count)
	}
}

func TestAnnounceLoopRunOnlyOnce(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, _ := newMockPeerSender(t)
	peers := func() []routing.AnnounceTarget { return nil }

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done1 := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done1)
	}()
	time.Sleep(10 * time.Millisecond)

	// Second Run should return immediately (already running).
	done2 := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done2)
	}()

	select {
	case <-done2:
		// OK, returned immediately.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second Run call should return immediately when loop is already running")
	}

	cancel()
	<-done1
}

// fixedOverloadGate is a test stub that returns a configurable value
// from IsOverloaded. Used to drive backpressure scenarios without
// touching runtime.NumGoroutine().
type fixedOverloadGate struct {
	overloaded bool
}

func (g *fixedOverloadGate) IsOverloaded() bool { return g.overloaded }

// TestAnnounceLoop_OverloadGate_SuppressesDeltasButForcedFullStillFires
// pins the Phase 0 backpressure contract. When the gate engages at
// cycle start:
//   - peers that owe a forced full sync this round still receive the
//     full announce (freshness invariant for TTL/2 must hold even
//     under overload);
//   - peers on the delta path are skipped — no wire traffic, no
//     CPU on delta computation, but no state staleness past TTL/2
//     because the next forced-full-sync deadline will catch up.
//
// Without the gate (nil OverloadGate or IsOverloaded returning
// false), the loop behaves exactly as before. The contract is checked
// by counting wire-send invocations on the recorder.
func TestAnnounceLoop_OverloadGate_SuppressesDeltasButForcedFullStillFires(t *testing.T) {
	gate := &fixedOverloadGate{overloaded: true}

	table := routing.NewTable(routing.WithLocalOrigin("self-node-identity"))
	if _, err := table.AddDirectPeer("peer-a-identity-direct"); err != nil {
		t.Fatalf("setup AddDirectPeer: %v", err)
	}

	sender, rec := newMockPeerSender(t)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:      "peer-a-addr",
				Identity:     "peer-a-identity",
				Capabilities: []routing.PeerCapability{domain.CapMeshRoutingV1},
			},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(20*time.Millisecond),
		routing.WithOverloadGate(gate),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	go loop.Run(ctx)

	// Step 1 — wait for the initial forced full sync. The peer has no
	// baseline (LastSentSnapshot=nil) at first tick, so per-peer
	// needsFull=true regardless of overload, and the loop fires
	// sendFullAnnounce. This wire send proves the freshness invariant
	// is intact under overload.
	waitForCall := time.Now().Add(500 * time.Millisecond)
	for rec.callCount() == 0 && time.Now().Before(waitForCall) {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.callCount() == 0 {
		t.Fatal("expected forced full sync to fire on first tick even under overload")
	}

	// Step 2 — counter MUST still be 0. Initial sync was a forced full
	// (needsFull=true → forced path; the suppression branch was not
	// taken). The strict "actually shed work" semantic excludes
	// forced-full cycles, so engaging the gate during initial sync
	// does NOT advance the counter.
	if got := loop.OverloadCycleCount(); got != 0 {
		t.Fatalf("strict semantic violation: counter advanced to %d during initial forced full sync; expected 0 (no delta was suppressed)", got)
	}

	// Step 3 — trigger an immediate delta cycle. After initial sync
	// the peer has a baseline, and we are well within the forced-full
	// cap window (forcedCap = 40ms with 20ms AnnounceInterval, so the
	// peer's needsFull will not be re-set for another ~20ms). The
	// triggered cycle therefore takes the delta path, hits the
	// overload-suppression early-return arm, and increments the
	// counter post-wg.
	//
	// Why TriggerUpdate rather than waiting for the natural second
	// tick: with AnnounceInterval=20ms and MinForcedFullSyncInterval=
	// 30s (a fixed routing-package constant, NOT scaled to test
	// intervals), the peer enters the forced-but-rate-limited state
	// quickly after the cap elapses (~40ms after initial sync). Any
	// scheduling delay around the rate-limit window pushes per-peer
	// goroutines into the rate-limit early-return at line 717,
	// bypassing the suppression branch we're trying to validate.
	// TriggerUpdate sidesteps the forced-due path entirely by forcing
	// `lastDeltaCycleAt = time.Time{}` and immediately running a
	// delta-cycle pass — deterministic regardless of scheduling.
	loop.TriggerUpdate()

	// Step 4 — wait for the counter to advance. The triggered cycle
	// runs synchronously inside Run's select, so this should be
	// near-instant; the polling deadline is generous to absorb
	// scheduler jitter.
	waitForCounter := time.Now().Add(200 * time.Millisecond)
	for loop.OverloadCycleCount() == 0 && time.Now().Before(waitForCounter) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()

	if got := loop.OverloadCycleCount(); got == 0 {
		t.Fatal("expected counter > 0 after triggered delta cycle was suppressed by the overload gate")
	}
}

// TestAnnounceLoop_OverloadGate_DisengagedAllowsDeltas pins the
// counterpart: when the gate reports not-overloaded (or is nil),
// the loop runs the normal delta path.
func TestAnnounceLoop_OverloadGate_DisengagedAllowsDeltas(t *testing.T) {
	gate := &fixedOverloadGate{overloaded: false}

	table := routing.NewTable(routing.WithLocalOrigin("self-node-identity"))
	if _, err := table.AddDirectPeer("peer-a-identity-direct"); err != nil {
		t.Fatalf("setup AddDirectPeer: %v", err)
	}

	sender, rec := newMockPeerSender(t)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:      "peer-a-addr",
				Identity:     "peer-a-identity",
				Capabilities: []routing.PeerCapability{domain.CapMeshRoutingV1},
			},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(20*time.Millisecond),
		routing.WithOverloadGate(gate),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go loop.Run(ctx)

	deadline := time.Now().Add(150 * time.Millisecond)
	for rec.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	if rec.callCount() == 0 {
		t.Fatal("expected announce to fire when not overloaded")
	}
	if got := loop.OverloadCycleCount(); got != 0 {
		t.Fatalf("expected OverloadCycleCount == 0 (gate never engaged), got %d", got)
	}
}

// TestAnnounceLoop_TickInterval_DividesBothCadences pins the
// gcd-based tick-interval invariant: the announce-loop ticker must
// divide BOTH the per-input forced-full cap AND the operator-configured
// AnnounceInterval cleanly. Two invariants:
//
//  1. `cap % tick == 0` — forced-full deadlines land exactly on a
//     tick. Without this, with cap=60s and tick=45s the deadline at
//     60s is observed at the 90s tick, actual gap = 90s past
//     cap=60s, single-dropped-sync window collapses past TTL=120s.
//
//  2. `announceInterval % tick == 0` — delta-cycle deadlines also
//     land exactly on a tick. Without this, with interval=45s and
//     tick=30s (a divisor of cap but not of interval) the
//     `now - lastDeltaCycleAt >= interval` check only fires every
//     SECOND tick (60s LCM with 30s tick), and that 60s alignment
//     coincides with the forced-full cycle so delta sends are always
//     pre-empted by forced. Operator's 45s intent collapsed to
//     forced cadence with NO standalone delta sends.
//
// gcd(interval, cap) is the natural fix: largest period that divides
// both cleanly. Sub-cases cover divisor inputs (interval divides
// cap, no scaling), coprime-style inputs (gcd shrinks to a small
// common factor), and above-cap inputs (cap dominates).
func TestAnnounceLoop_TickInterval_DividesBothCadences(t *testing.T) {
	ttlHalf := routing.DefaultTTL / 2 // 60s with default constants

	cases := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		// Divisor inputs — operator value already divides the
		// per-input cap, gcd returns the operator value.
		{"default_30s_divides_60s_cap", routing.DefaultAnnounceInterval, routing.DefaultAnnounceInterval},
		{"divisor_25s_50s_cap", 25 * time.Second, 25 * time.Second},
		{"divisor_20s_40s_cap", 20 * time.Second, 20 * time.Second},
		{"divisor_15s_30s_cap", 15 * time.Second, 15 * time.Second},
		{"divisor_12s_24s_cap", 12 * time.Second, 12 * time.Second},
		{"divisor_10s_20s_cap", 10 * time.Second, 10 * time.Second},
		{"divisor_60s_at_cap", ttlHalf, ttlHalf},
		// Coprime-style inputs — gcd produces a smaller common
		// factor that divides BOTH operator's interval AND the cap
		// (TTL/2=60s for these because 2 × interval > 60s).
		{"non_coprime_45s_60s_gcd_15s", 45 * time.Second, 15 * time.Second},
		{"non_coprime_50s_60s_gcd_10s", 50 * time.Second, 10 * time.Second},
		{"non_coprime_55s_60s_gcd_5s", 55 * time.Second, 5 * time.Second},
		// Above-cap inputs — operator value > TTL/2, cap dominates.
		// Tick = cap, delta cadence is enforced via per-peer
		// lastDeltaCycleAt check (every tick where elapsed ≥
		// announceInterval), so delta fires every announceInterval
		// rounded up to the nearest tick boundary (which equals
		// announceInterval when it's a multiple of cap, else the
		// next multiple).
		{"above_cap_120s_clamped", 120 * time.Second, ttlHalf},
		{"above_cap_300s_clamped", 5 * time.Minute, ttlHalf},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := routing.NewTable(routing.WithLocalOrigin("self-node-identity"))
			sender, _ := newMockPeerSender(t)
			peers := func() []routing.AnnounceTarget { return nil }

			loop := routing.NewAnnounceLoop(table, sender, peers,
				routing.WithAnnounceInterval(tc.interval))

			got := loop.TickInterval()
			if got != tc.want {
				t.Fatalf("TickInterval() = %s for AnnounceInterval=%s, want %s",
					got, tc.interval, tc.want)
			}

			// Invariant 1: tick must DIVIDE the per-input
			// forced-full cap exactly so forced-full deadlines land
			// on a tick boundary.
			perInputCap := routing.EffectiveForcedFullSyncInterval(tc.interval)
			if perInputCap%got != 0 {
				t.Fatalf("invariant 1 violation: TickInterval=%s does not divide cap=%s evenly (cap %% tick = %s) for AnnounceInterval=%s",
					got, perInputCap, perInputCap%got, tc.interval)
			}
			if got > perInputCap {
				t.Fatalf("invariant violation: TickInterval=%s > cap=%s for AnnounceInterval=%s",
					got, perInputCap, tc.interval)
			}
			// Invariant 2: tick must DIVIDE announceInterval too,
			// EXCEPT when the cap dominates (announceInterval ≥
			// cap). In the cap-dominated case delta cadence is
			// enforced via lastDeltaCycleAt and may not align
			// perfectly with the operator interval, but that's
			// inherent — operator asked for slower-than-allowed
			// delta cycles and gets cap-aligned cadence instead.
			if tc.interval < perInputCap && tc.interval%got != 0 {
				t.Fatalf("invariant 2 violation: TickInterval=%s does not divide announceInterval=%s evenly (interval %% tick = %s)",
					got, tc.interval, tc.interval%got)
			}
			// Outer bound: cap itself is bounded by TTL/2.
			if perInputCap > ttlHalf {
				t.Fatalf("setup invariant: cap=%s > TTL/2=%s — EffectiveForcedFullSyncInterval is broken",
					perInputCap, ttlHalf)
			}
		})
	}
}

// TestAnnounceLoop_ForcedFullRateLimit_ClampedToForcedFullCap pins
// the rate-limit clamp invariant: the rate-limit window for
// forced-full retry attempts must be clamped to forcedFullSyncInterval
// so it cannot stretch the effective forced-full cadence beyond what
// EffectiveForcedFullSyncInterval projects.
//
// Without the clamp, an operator setting AnnounceInterval=10s gets
// forcedFullSyncInterval=20s from the helper (and the exported
// helper docs project "next forced-full deadline = LastSuccessful +
// 20s"), but after a successful sync the next forced attempt at 20s
// would be rate-limited by the fixed MinForcedFullSyncInterval=30s,
// silently stretching the actual cadence to 30s. Receivers in
// single-dropped-sync scenarios depend on the helper's projection
// being honest; a stretch from 20s to 30s narrows the dropped-sync
// margin (TTL=120s, two missed = 60s gap; 30s actual leaves only
// 60s margin instead of 80s).
//
// This test exercises the clamp by driving a scenario where the
// fixed MinForcedFullSyncInterval would have rate-limited a
// forced-full retry, but the clamped rate-limit window allows it.
// The presence of a SECOND wire send within the forced cadence
// window is the operator-visible signal that the clamp is active.
func TestAnnounceLoop_ForcedFullRateLimit_ClampedToForcedFullCap(t *testing.T) {
	// AnnounceInterval=10s would be hit by the clamp (forcedCap=20s,
	// rate-limit-window must clamp from 30s down to 20s). For test
	// time-budget we use a sub-second analogue: AnnounceInterval=
	// 100ms gives forcedCap=200ms. MinForcedFullSyncInterval is
	// fixed at 30s — way above forcedCap=200ms — so the clamp MUST
	// kick in. Without the clamp, rate-limit window = 30s and the
	// second forced attempt would be blocked for the entire test.
	//
	// We force needsFull=true on each cycle by NOT seeding
	// LastSentSnapshot through any prior path, then verify TWO
	// forced full sync wire calls happen within the test budget —
	// proving the rate-limit window is at most forcedCap (200ms),
	// not MinForcedFullSyncInterval (30s).
	gate := &fixedOverloadGate{overloaded: false}

	table := routing.NewTable(routing.WithLocalOrigin("self-node-identity"))
	if _, err := table.AddDirectPeer("peer-a-identity-direct"); err != nil {
		t.Fatalf("setup AddDirectPeer: %v", err)
	}

	sender, rec := newMockPeerSender(t)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:      "peer-a-addr",
				Identity:     "peer-a-identity",
				Capabilities: []routing.PeerCapability{domain.CapMeshRoutingV1},
			},
		}
	}

	// AnnounceInterval=100ms → forcedCap=200ms. Without clamp the
	// rate-limit window is 30s, blocking the second forced attempt.
	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(100*time.Millisecond),
		routing.WithOverloadGate(gate),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	go loop.Run(ctx)

	// First forced full sync at first tick (~100ms). Wait for it.
	deadline := time.Now().Add(500 * time.Millisecond)
	for rec.callCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.callCount() < 1 {
		t.Fatalf("expected first forced full sync within 500ms, got %d calls", rec.callCount())
	}

	// State.NeedsFullResync is force-flipped via MarkInvalid so the
	// next tick triggers needsFull=true again, exercising the
	// rate-limit branch. With the clamp, the rate-limit window is
	// 200ms (forcedCap), so the second forced attempt is allowed
	// once 200ms has passed. Without the clamp, the window is 30s
	// and the second attempt would never fire within the test
	// timeout.
	loop.StateRegistry().MarkInvalid("peer-a-identity")

	// Wait for second forced full to fire. Budget: 500ms after first
	// call (well within 30s rate-limit but well past 200ms clamp).
	secondDeadline := time.Now().Add(500 * time.Millisecond)
	for rec.callCount() < 2 && time.Now().Before(secondDeadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	if got := rec.callCount(); got < 2 {
		t.Fatalf("rate-limit clamp regression: expected ≥2 forced full sync calls within forcedCap=200ms window, got %d (without clamp the second attempt would be blocked by MinForcedFullSyncInterval=30s)", got)
	}
}

// TestAnnounceLoop_TickInterval_BusyLoopGuard pins the floor
// behaviour for non-second-aligned inputs. Without the 1-second
// floor on production cadences (≥1s), a misuse like
// `WithAnnounceInterval(59*time.Second + 1*time.Nanosecond)` would
// produce gcd=1ns and turn `time.NewTicker` into a busy loop.
//
// The floor only engages when BOTH inputs are ≥ 1s — sub-second test
// cadences (10ms, 20ms, etc.) bypass it so fast-iteration tests
// keep working. The floor preserves invariant 1 (`cap % tick == 0`)
// because production caps are whole-second multiples of which 1s is
// always a divisor. Invariant 2 (`announceInterval % tick == 0`)
// may exhibit minor aliasing for the malformed input, which is
// acceptable: the operator passed a non-second-aligned value and
// explicitly opted into best-effort handling.
func TestAnnounceLoop_TickInterval_BusyLoopGuard(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			// gcd(59s+1ns, 60s) reduces to a few-nanosecond
			// remainder via Euclidean — well below 1s. Floor must
			// kick in.
			name:     "production_value_with_nanosecond_jitter_floored_to_1s",
			interval: 59*time.Second + 1*time.Nanosecond,
			want:     time.Second,
		},
		{
			// gcd(59500ms, 60000ms) = 500ms (60000 % 59500 = 500,
			// 59500 % 500 = 0). Below 1s → floor.
			name:     "production_value_with_500ms_jitter_floored_to_1s",
			interval: 59*time.Second + 500*time.Millisecond,
			want:     time.Second,
		},
		{
			// gcd(37001ms, 60000ms) = 1ms (Euclidean walks down to
			// 1 because 37001 and 60000 are nearly coprime). Well
			// below floor.
			name:     "production_value_with_1ms_jitter_floored_to_1s",
			interval: 37*time.Second + 1*time.Millisecond,
			want:     time.Second,
		},
		{
			name:     "subsecond_test_cadence_bypasses_floor_10ms",
			interval: 10 * time.Millisecond,
			want:     10 * time.Millisecond, // gcd(10ms, 20ms) = 10ms, no floor
		},
		{
			name:     "subsecond_test_cadence_bypasses_floor_20ms",
			interval: 20 * time.Millisecond,
			want:     20 * time.Millisecond, // gcd(20ms, 40ms) = 20ms, no floor
		},
		{
			name:     "whole_second_input_unaffected_by_floor",
			interval: 45 * time.Second,
			want:     15 * time.Second, // gcd(45s, 60s) = 15s, above floor, returned as-is
		},
		{
			// Sanity: 37s + 500ms gives gcd(37500ms, 60000ms) =
			// 7.5s — that's WELL above the 1s floor, so the floor
			// does NOT kick in. This case is here to pin the math
			// (early reviewers hit the false intuition that
			// "non-second-aligned ⇒ tiny gcd"; not always true).
			name:     "non_second_aligned_with_large_gcd_unfloored",
			interval: 37*time.Second + 500*time.Millisecond,
			want:     7500 * time.Millisecond,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := routing.NewTable(routing.WithLocalOrigin("self-node-identity"))
			sender, _ := newMockPeerSender(t)
			peers := func() []routing.AnnounceTarget { return nil }

			loop := routing.NewAnnounceLoop(table, sender, peers,
				routing.WithAnnounceInterval(tc.interval))

			got := loop.TickInterval()
			if got != tc.want {
				t.Fatalf("TickInterval() = %s for AnnounceInterval=%s, want %s",
					got, tc.interval, tc.want)
			}

			// Cap divisibility (invariant 1) must hold even after
			// the floor — production caps are whole-second
			// multiples of which 1s always divides cleanly.
			perInputCap := routing.EffectiveForcedFullSyncInterval(tc.interval)
			if perInputCap%got != 0 {
				t.Fatalf("invariant 1 violation: TickInterval=%s does not divide cap=%s for AnnounceInterval=%s",
					got, perInputCap, tc.interval)
			}
			// Crucially: tick must NEVER be sub-millisecond
			// regardless of input — anything tighter would qualify
			// as busy-loop behaviour.
			if got > 0 && got < time.Millisecond {
				t.Fatalf("busy-loop risk: TickInterval=%s is sub-millisecond for AnnounceInterval=%s",
					got, tc.interval)
			}
		})
	}
}

// TestAnnounceLoop_TriggerDoesNotDelayForcedFullDeadline pins the
// invariant that the triggerCh handler MUST NOT call
// ticker.Reset(tickInterval). An
// earlier version did so to coalesce "trigger then immediate
// periodic tick" pairs, but with triggers arriving slightly faster
// than the tick the Reset pushed the next natural tick forward on
// every trigger, and the forced-full deadline check (which runs
// every cycle, but the cycle that catches the deadline must land
// AT OR AFTER the deadline) was effectively delayed past
// forcedCap until a trigger happened to land past the deadline.
// Receivers in single-dropped-sync scenarios then lost learned
// routes silently between stretched syncs.
//
// Scenario: AnnounceInterval=200ms → forcedCap=400ms,
// tickInterval=200ms. Triggers fire every 180ms (slightly faster
// than the natural tick), peer has a baseline after the initial
// forced-full so subsequent cycles take the delta path AND check
// the forced-full deadline.
//
//	Without the bug (current code): the natural tick at t≈400ms
//	  fires the forced-full deadline check on schedule. Second
//	  wire send lands at ≤ ~450ms after the first.
//	With the bug back: every trigger at 180ms / 360ms reset the
//	  ticker, so the natural tick at 400ms never fires. The first
//	  cycle that catches the deadline is the trigger at 540ms,
//	  giving a 540ms gap — 35% past the cap.
//
// The assertion `gap ≤ forcedCap + tolerance` (450ms) fails on
// the bug shape and passes on the current code, regardless of
// the exact number of triggers that interleave. The tolerance
// absorbs scheduler jitter without admitting the bug envelope.
func TestAnnounceLoop_TriggerDoesNotDelayForcedFullDeadline(t *testing.T) {
	const (
		announceInterval = 200 * time.Millisecond
		forcedCap        = 400 * time.Millisecond // EffectiveForcedFullSyncInterval(200ms) = 400ms
		triggerCadence   = 180 * time.Millisecond // slightly faster than the natural tick
		gapTolerance     = 50 * time.Millisecond  // absorb scheduler jitter
		// gapBudget is the assertion threshold. forcedCap + tolerance
		// must be < the bug-shape gap (~540ms) so the assertion
		// distinguishes the two shapes cleanly. 450ms passes on the
		// current code (~400ms ideal, plus jitter) and fails on the
		// bug shape (~540ms).
		gapBudget = forcedCap + gapTolerance
	)

	table := routing.NewTable(routing.WithLocalOrigin("self-node-identity"))
	if _, err := table.AddDirectPeer("peer-a-identity-direct"); err != nil {
		t.Fatalf("setup AddDirectPeer: %v", err)
	}

	sender, rec := newMockPeerSender(t)
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{
				Address:      "peer-a-addr",
				Identity:     "peer-a-identity",
				Capabilities: []routing.PeerCapability{domain.CapMeshRoutingV1},
			},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(announceInterval))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go loop.Run(ctx)

	// Trigger the initial forced-full sync immediately; LastSentSnapshot
	// is nil so needsFull=true and the cycle takes the forced-full
	// branch. This is wire send #1 — its timestamp anchors the gap
	// measurement.
	loop.TriggerUpdate()
	deadline := time.Now().Add(500 * time.Millisecond)
	for rec.callCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if rec.callCount() < 1 {
		t.Fatalf("expected first forced-full sync within 500ms, got %d", rec.callCount())
	}
	firstSendAt := time.Now()

	// Drive triggers every triggerCadence (180ms). The trigger
	// goroutine stops when ctx is cancelled by the assertion path
	// below or by t.Cleanup. Bug shape: each trigger ticker.Reset()
	// would push the natural tick at 200ms / 400ms forward by 200ms
	// each time, so the forced-full deadline at t=400ms wouldn't be
	// caught until the trigger past it (t=540ms).
	triggerCtx, stopTriggers := context.WithCancel(ctx)
	defer stopTriggers()
	go func() {
		ticker := time.NewTicker(triggerCadence)
		defer ticker.Stop()
		for {
			select {
			case <-triggerCtx.Done():
				return
			case <-ticker.C:
				loop.TriggerUpdate()
			}
		}
	}()

	// Wait for the second wire send. Budget is generous so the test
	// can still observe the bug shape (gap=540ms) and fail on the
	// gap assertion below — failing fast here would mask the bug
	// signal.
	secondDeadline := time.Now().Add(forcedCap + 500*time.Millisecond)
	for rec.callCount() < 2 && time.Now().Before(secondDeadline) {
		time.Sleep(2 * time.Millisecond)
	}
	stopTriggers()

	if rec.callCount() < 2 {
		t.Fatalf("expected second forced-full sync within %s, got %d sends; ticker.Reset bug may be back blocking the deadline check entirely",
			forcedCap+500*time.Millisecond, rec.callCount())
	}

	gap := time.Since(firstSendAt)
	if gap > gapBudget {
		t.Fatalf("forced-full deadline regression: gap between wire sends %s exceeds budget %s (forcedCap=%s + tolerance=%s); ticker.Reset bug from round 15.9 may be back, triggers stretching the natural tick past the cap",
			gap, gapBudget, forcedCap, gapTolerance)
	}
}

// TestEffectiveForcedFullSyncInterval_CappedAtTTLHalf pins the
// freshness invariant for the forced-full-sync cadence: regardless of
// the announce interval (default 30s, or operator-tuned higher via
// CORSA_ANNOUNCE_INTERVAL_SECONDS for dense meshes), the effective
// cadence MUST NOT exceed DefaultTTL/2. A regression that drops the
// cap reintroduces the silent-expiry window the cap was added to
// close (an operator setting interval=120s would otherwise push
// forced-full to 240s, > DefaultTTL=120s, and learned routes age out
// silently between syncs). See docs/routing.md "Refresh interval
// invariant" for the contract.
func TestEffectiveForcedFullSyncInterval_CappedAtTTLHalf(t *testing.T) {
	maxAllowed := routing.DefaultTTL / 2

	cases := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "default_interval_uses_multiplier",
			interval: routing.DefaultAnnounceInterval, // 30s
			want:     time.Duration(routing.ForcedFullSyncMultiplier) * routing.DefaultAnnounceInterval,
		},
		{
			name:     "short_test_interval_uses_multiplier",
			interval: 10 * time.Second,
			want:     20 * time.Second,
		},
		{
			name:     "interval_exactly_at_cap_uses_cap",
			interval: maxAllowed,
			want:     maxAllowed,
		},
		{
			name:     "interval_above_cap_clamped",
			interval: routing.DefaultTTL, // = 2 * cap
			want:     maxAllowed,
		},
		{
			name:     "operator_dense_60s_clamped_to_60s",
			interval: 60 * time.Second,
			want:     maxAllowed, // 60s = DefaultTTL/2
		},
		{
			name:     "operator_extreme_300s_still_clamped",
			interval: 5 * time.Minute,
			want:     maxAllowed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := routing.EffectiveForcedFullSyncInterval(tc.interval)
			if got != tc.want {
				t.Fatalf("EffectiveForcedFullSyncInterval(%s) = %s, want %s",
					tc.interval, got, tc.want)
			}
			// Invariant: the effective interval must never exceed
			// DefaultTTL/2 regardless of input.
			if got > maxAllowed {
				t.Fatalf("invariant violation: result %s > DefaultTTL/2 = %s", got, maxAllowed)
			}
		})
	}
}
