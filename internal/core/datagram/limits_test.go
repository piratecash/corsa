package datagram

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// limits_test.go pins the §5 numbers to the reasoning that produced them.
// These are not "the constants have the values they have" tests: each one
// fails when a retuning breaks a property the layer depends on elsewhere.

// The crypto budget must be the BINDING constraint on a flood of small signed
// frames (§9). If a retuning ever made the frame budget bite first, the test
// in admission_test.go would still pass — it would just stop testing what it
// claims to. This one states the invariant directly.
func TestLimitsCryptoBudgetBindsBeforeTheOthers(t *testing.T) {
	t.Parallel()

	budget := DefaultLimits().Peer
	if budget.VerifyBurst >= budget.FrameBurst {
		t.Fatalf("verify burst %d must be below frame burst %d, or the frame budget "+
			"refuses a signed flood before the crypto budget does",
			budget.VerifyBurst, budget.FrameBurst)
	}
	if budget.VerifiesPerSecond >= budget.FramesPerSecond {
		t.Fatalf("sustained verify rate %d must be below the frame rate %d",
			budget.VerifiesPerSecond, budget.FramesPerSecond)
	}
	// A burst of minimum-size signed frames must fit in the byte budget with
	// room to spare, otherwise "exhausts the crypto budget WITHOUT exhausting
	// the byte budget" is not achievable at all.
	const smallSignedFrameBytes = 512
	if budget.VerifyBurst*smallSignedFrameBytes >= budget.ByteBurst {
		t.Fatalf("a burst of %d small signed frames (%d B each) does not fit in the %d B byte burst",
			budget.VerifyBurst, smallSignedFrameBytes, budget.ByteBurst)
	}
}

// A budget smaller than one legitimate frame of a class refuses that class
// entirely — a silent, total outage for bulk traffic.
func TestLimitsAdmitOneMaximumFrameOfEveryClass(t *testing.T) {
	t.Parallel()

	budget := DefaultLimits().Peer
	for _, class := range []domain.DatagramClass{domain.DatagramClassControl, domain.DatagramClassBulk} {
		frameBytes := MaxFrameBytes(class)
		if frameBytes > budget.ByteBurst {
			t.Fatalf("%s: a maximum frame is %d B, the byte burst is %d B", class, frameBytes, budget.ByteBurst)
		}
		if frameBytes > protocol.MaxFrameLine {
			t.Fatalf("%s: a maximum frame is %d B, above the %d B line limit", class, frameBytes, protocol.MaxFrameLine)
		}
	}
}

// A queue lane must hold at least one maximum frame of its class, and a
// maximum bulk frame must actually leave the queue while control saturates —
// a quantum far below the frame size would satisfy the arithmetic and still
// stall the transfer.
func TestLimitsQueueLanesDispatchAMaximumFrame(t *testing.T) {
	t.Parallel()

	caps := DefaultLimits().Queue
	if MaxFrameBytes(domain.DatagramClassControl) > caps.ControlBytes {
		t.Fatalf("control lane %d B cannot hold one %d B frame", caps.ControlBytes, MaxFrameBytes(domain.DatagramClassControl))
	}
	if MaxFrameBytes(domain.DatagramClassBulk) > caps.BulkBytes {
		t.Fatalf("bulk lane %d B cannot hold one %d B frame", caps.BulkBytes, MaxFrameBytes(domain.DatagramClassBulk))
	}

	queue := NewWeightedQueue(WeightedQueueConfig{Caps: caps})
	if !queue.Enqueue(queuedFrame(domain.DatagramClassBulk, MaxFrameBytes(domain.DatagramClassBulk), time.Time{})) {
		t.Fatal("a maximum bulk frame did not fit in the bulk lane")
	}
	for i := 0; i < caps.ControlFrames; i++ {
		queue.Enqueue(queuedFrame(domain.DatagramClassControl, MaxFrameBytes(domain.DatagramClassControl), time.Time{}))
	}
	for dispatch := 0; ; dispatch++ {
		item, ok := queue.Dequeue()
		if !ok {
			t.Fatal("the queue emptied without dispatching the bulk frame")
		}
		if item.Frame.Class == domain.DatagramClassBulk {
			break
		}
		if dispatch > caps.ControlFrames {
			t.Fatal("the bulk frame never left while control was saturated")
		}
	}
}

// Bulk's guaranteed share is the §5 starting value of one quarter. The test
// exists so a change of weights is a deliberate change of the promise.
func TestLimitsBulkShareIsOneQuarter(t *testing.T) {
	t.Parallel()

	caps := DefaultLimits().Queue
	total := caps.ControlWeight + caps.BulkWeight
	if caps.BulkWeight*4 != total {
		t.Fatalf("bulk share is %d/%d, want one quarter", caps.BulkWeight, total)
	}
}

// The limits are the single source of the numbers the components already
// carried as their own fallbacks. Drift between the two would make behaviour
// depend on whether the caller wired Limits at all.
func TestLimitsAgreeWithComponentFallbacks(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	global, perUpstream := limits.ReverseStateCaps()
	if global != defaultReverseGlobalCap || perUpstream != defaultReversePerUpstreamCap {
		t.Fatalf("reverse caps %d/%d, want the table's own %d/%d",
			global, perUpstream, defaultReverseGlobalCap, defaultReversePerUpstreamCap)
	}
	if limits.Reverse.ProbeBudget != DefaultReverseProbeBudget {
		t.Fatalf("probe budget %d, want %d", limits.Reverse.ProbeBudget, DefaultReverseProbeBudget)
	}
	if limits.Replay.Capacity != DefaultBaseReplayCapacity {
		t.Fatalf("replay capacity %d, want %d", limits.Replay.Capacity, DefaultBaseReplayCapacity)
	}
}

// Zero is a configuration mistake, not "disable this limit": a zero byte
// budget would admit nothing and a zero cap would store nothing.
func TestLimitsNormalizedFillsEveryZero(t *testing.T) {
	t.Parallel()

	normalized := Limits{}.Normalized()
	if normalized != DefaultLimits() {
		t.Fatalf("Normalized() of the zero value = %+v, want the defaults", normalized)
	}

	custom := Limits{Peer: PeerBudget{VerifiesPerSecond: 7}, Replay: ReplayCaps{Capacity: 11}}.Normalized()
	if custom.Peer.VerifiesPerSecond != 7 || custom.Replay.Capacity != 11 {
		t.Fatalf("Normalized() overwrote explicit values: %+v", custom)
	}
	if custom.Peer.ByteBurst != DefaultLimits().Peer.ByteBurst {
		t.Fatalf("Normalized() left an unset field at zero: %+v", custom.Peer)
	}
}

// The derived configs must carry the numbers through, so a caller cannot wire
// the caps and forget the probe budget or the capacity.
func TestLimitsDerivedConfigsCarryTheNumbers(t *testing.T) {
	t.Parallel()

	limits := Limits{
		Reverse: ReverseCaps{GlobalCap: 9, PerUpstreamCap: 3, ProbeBudget: 2},
		Replay:  ReplayCaps{Capacity: 5},
	}
	clock := func() time.Time { return time.Unix(0, 0) }

	reverse := limits.ReverseTableConfig(clock, nil)
	if reverse.Probes != 2 {
		t.Fatalf("reverse config probes = %d, want 2", reverse.Probes)
	}
	table := NewReverseTable(reverse)
	if table.ProbeBudget() != 2 {
		t.Fatalf("table probe budget = %d, want 2", table.ProbeBudget())
	}

	replay := limits.BaseReplayCacheConfig(clock)
	if replay.Capacity != 5 {
		t.Fatalf("replay config capacity = %d, want 5", replay.Capacity)
	}
}

// ---------------------------------------------------------------------------
// The caps the numbers above configure (§5): reverse state and anti-replay
// ---------------------------------------------------------------------------

// m8ReplayKey is a distinct, reproducible replay key per test index.
func m8ReplayKey(n byte) domain.ReplayKey {
	var key domain.ReplayKey
	for i := range key {
		key[i] = n
	}
	return key
}

// The reverse table is capped globally and per upstream, and when it is full
// the victim is the OLDEST record of the BUSIEST upstream. "Oldest overall"
// would let one noisy neighbour push out everybody else's exchanges, which is
// the opposite of what the cap is for (§5).
func TestLimitsReverseStateEvictsTheBusiestUpstream(t *testing.T) {
	t.Parallel()

	clock := newLimitsClock()
	limits := Limits{Reverse: ReverseCaps{GlobalCap: 4, PerUpstreamCap: 3}}
	metrics := NewMetrics()
	table := NewReverseTable(limits.ReverseTableConfig(clock.Now, metrics))

	loud, quiet := testUpstream(domaintest.ID("loud")), testUpstream(domaintest.ID("quiet"))
	labels := make([]Label, 0, 6)
	reserve := func(upstream Upstream, seed string) ReverseReserveResult {
		label := NewLabel(domaintest.ID("label-" + seed))
		labels = append(labels, label)
		clock.advance(time.Second)
		return table.Reserve(ReverseReserveOpts{
			ReceivedAt: clock.Now(),
			Label:      label,
			Dst:        domaintest.ID("target"),
			DType:      dtypeQuery,
			Upstream:   upstream,
		})
	}

	// The loud neighbour fills its own per-upstream cap.
	for i := 0; i < 3; i++ {
		if got := reserve(loud, "loud-"+string(rune('a'+i))).Outcome(); got != ReverseSlotReserved {
			t.Fatalf("loud reservation %d = %s, want reserved", i, got)
		}
	}
	// Its fourth request is refused by the per-upstream cap, not by evicting
	// somebody else.
	if got := reserve(loud, "loud-d").Outcome(); got != ReverseSlotCapped {
		t.Fatalf("the per-upstream cap did not engage: %s", got)
	}
	// The quiet neighbour still gets in — the table is at three of four.
	if got := reserve(quiet, "quiet-a").Outcome(); got != ReverseSlotReserved {
		t.Fatalf("quiet reservation = %s, want reserved", got)
	}
	if table.Len() != 4 {
		t.Fatalf("table holds %d records, want the global cap of 4", table.Len())
	}

	// Now the table is globally full. The quiet neighbour's SECOND request
	// must still be admitted, at the expense of the busiest upstream.
	if got := reserve(quiet, "quiet-b").Outcome(); got != ReverseSlotReserved {
		t.Fatalf("a quiet neighbour was refused while a noisy one held three slots: %s", got)
	}
	if table.Len() != 4 {
		t.Fatalf("table holds %d records after the eviction, want 4", table.Len())
	}
	if metrics.ReverseCount(ReverseEventEvicted) != 1 {
		t.Fatalf("evictions = %d, want exactly one", metrics.ReverseCount(ReverseEventEvicted))
	}
	// The victim is the loud neighbour's OLDEST record; the quiet
	// neighbour's first one survived.
	if _, alive := table.Lookup(labels[0]); alive {
		t.Fatal("the oldest record of the busiest upstream survived the eviction")
	}
	if _, alive := table.Lookup(labels[4]); !alive {
		t.Fatal("the quiet neighbour's first record was evicted instead")
	}
	if metrics.ReverseCount(ReverseEventCapped) != 1 {
		t.Fatalf("capped events = %d, want the one per-upstream refusal", metrics.ReverseCount(ReverseEventCapped))
	}
}

// At capacity the anti-replay cache refuses the NOISIEST neighbour rather
// than flushing: a global flush would let one peer erase everybody else's
// anti-replay memory, which is the attack the rule exists to stop (§5).
func TestLimitsReplayOverflowRefusesTheNoisiestNeighbour(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newLimitsClock()
	limits := Limits{Replay: ReplayCaps{Capacity: 4}}
	cache := NewBaseReplayCache(limits.BaseReplayCacheConfig(clock.Now))

	noisy := ProvenIngress(testChannel("noisy"), domaintest.ID("noisy"))
	quiet := ProvenIngress(testChannel("quiet"), domaintest.ID("quiet"))
	until := clock.Now().Add(time.Minute)

	// The noisy neighbour fills the cache. Every record is committed, which
	// is what makes it evictable at all — a held reservation is never taken
	// away from the instance that owns it.
	for i := byte(0); i < 4; i++ {
		result := cache.Reserve(ctx, m8ReplayKey(i+1), noisy, until)
		if result.Outcome() != ReserveReserved {
			t.Fatalf("noisy reservation %d = %s", i, result.Outcome())
		}
		token, _ := result.Reservation()
		if applied := cache.Commit(ctx, token); !applied.IsApplied() {
			t.Fatalf("commit %d: %v", i, applied.Err())
		}
	}

	// Its next frame is refused: the cache is full and IT is the noisiest.
	if got := cache.Reserve(ctx, m8ReplayKey(5), noisy, until).Outcome(); got != ReserveRejected {
		t.Fatalf("the noisiest neighbour was not refused at capacity: %s", got)
	}
	if cache.Len() != 4 {
		t.Fatalf("the refusal changed the cache size to %d", cache.Len())
	}

	// A quiet neighbour is still admitted, by evicting one of the noisy
	// one's records — not by flushing the cache.
	if got := cache.Reserve(ctx, m8ReplayKey(6), quiet, until).Outcome(); got != ReserveReserved {
		t.Fatalf("a quiet neighbour was refused at capacity: %s", got)
	}
	if cache.Len() != 4 {
		t.Fatalf("cache holds %d records, want the capacity of 4", cache.Len())
	}

	metrics := cache.Metrics()
	if metrics.RejectedNoisyPeer != 1 {
		t.Fatalf("RejectedNoisyPeer = %d, want 1", metrics.RejectedNoisyPeer)
	}
	if metrics.EvictedNoisyPeer != 1 {
		t.Fatalf("EvictedNoisyPeer = %d, want 1", metrics.EvictedNoisyPeer)
	}
	// The quiet neighbour's own record is intact: a peer that just got in
	// must not be the next victim of its own arrival.
	if cache.Has(ctx, m8ReplayKey(6)).Outcome() != HasHit {
		t.Fatal("the quiet neighbour's record did not survive its own insertion")
	}
}
