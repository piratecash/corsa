package datagram_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

func peerIdentity(seed byte) domain.PeerIdentity {
	var id domain.PeerIdentity
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

// remoteArrival is one neighbour of an ACCEPTED connection, as the receive path
// builds it: the identity is proven, so the budget key the arrival is bucketed
// by is derived from it.
//
// It is the only remote shape these fixtures can use, and that is the point:
// every ingress the layer builds names a budget key (ProvenIngress derives one,
// ClaimedIngress demands one), so a record bucketed by a bare NAME is a record
// no receive path produces.
func remoteArrival(id domain.PeerIdentity) datagram.IngressPeer {
	return datagram.ProvenIngress(replayChannel(id.String()), id)
}

func newBaseCache(t *testing.T, clock *manualClock, capacity int) *datagram.BaseReplayCache {
	t.Helper()
	return datagram.NewBaseReplayCache(datagram.BaseReplayCacheConfig{
		Clock:    clock.Now,
		Capacity: capacity,
	})
}

// Has is the cheap early filter and must take nothing: a frame that has not
// proven itself authentic may not leave a mark that would suppress the
// genuine copy.
func TestBaseCacheHasNeverReserves(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(1)

	for i := 0; i < 3; i++ {
		if got := cache.Has(ctx, key).Outcome(); got != datagram.HasMiss {
			t.Fatalf("probe %d: Has = %s, want miss", i, got)
		}
	}
	if cache.Len() != 0 {
		t.Fatalf("probe inserted %d entries", cache.Len())
	}
	if got := cache.Reserve(ctx, key, remoteArrival(peerIdentity(9)), baseTime.Add(time.Minute)).Outcome(); got != datagram.ReserveReserved {
		t.Fatalf("Reserve after probes = %s, want reserved", got)
	}
}

// Two parallel instances of the same frame: exactly one Reserve wins, the
// other is dropped as a duplicate.
func TestBaseCacheConcurrentReserveHasExactlyOneWinner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(2)
	until := baseTime.Add(time.Minute)

	const instances = 8
	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		mu      sync.Mutex
		results []datagram.ReserveResult
	)
	start.Add(1)
	done.Add(instances)
	for i := 0; i < instances; i++ {
		go func() {
			defer done.Done()
			start.Wait()
			result := cache.Reserve(ctx, key, remoteArrival(peerIdentity(byte(i))), until)
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}()
	}
	start.Done()
	done.Wait()

	reserved, duplicates := 0, 0
	var winner datagram.ReservationToken
	for _, result := range results {
		switch result.Outcome() {
		case datagram.ReserveReserved:
			reserved++
			winner, _ = result.Reservation()
		case datagram.ReserveDuplicate:
			duplicates++
		default:
			t.Fatalf("unexpected outcome %s", result.Outcome())
		}
	}
	if reserved != 1 || duplicates != instances-1 {
		t.Fatalf("reserved=%d duplicates=%d, want 1 and %d", reserved, duplicates, instances-1)
	}
	// The winner's own token addresses the record it holds — read-back by
	// token, not by key.
	if !cache.Commit(ctx, winner).IsApplied() {
		t.Fatal("winner must be able to commit its own reservation")
	}
}

// ABA: a late Release of a finished reservation must not cancel the fresh
// reservation of the same key held by somebody else.
func TestBaseCacheLateReleaseDoesNotCancelFreshReservation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(3)
	until := baseTime.Add(time.Minute)
	peer := remoteArrival(peerIdentity(1))

	first, ok := cache.Reserve(ctx, key, peer, until).Reservation()
	if !ok {
		t.Fatal("first Reserve must win")
	}
	if !cache.Release(ctx, first).IsApplied() {
		t.Fatal("Release must apply")
	}

	second, ok := cache.Reserve(ctx, key, peer, until).Reservation()
	if !ok {
		t.Fatal("second Reserve must win after the release")
	}
	if second == first {
		t.Fatal("a fresh reservation must not reuse the previous token")
	}

	// The late release of the FIRST reservation arrives now.
	if !cache.Release(ctx, first).IsApplied() {
		t.Fatal("a stale Release is a no-op reported as ok")
	}
	if got := cache.Reserve(ctx, key, peer, until).Outcome(); got != datagram.ReserveDuplicate {
		t.Fatalf("after the stale release the key must still be held: got %s", got)
	}
	if !cache.Commit(ctx, second).IsApplied() {
		t.Fatal("the fresh reservation must survive the stale release")
	}
}

// A branch BEFORE the reservation (HandleTransit.drop, an empty candidate
// list) holds no token and therefore cannot cancel anything — including the
// reservation a parallel handler of the same frame is holding right now.
func TestBaseCacheDropBeforeReservationCannotCancelAForeignReservation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(4)
	until := baseTime.Add(time.Minute)

	held, ok := cache.Reserve(ctx, key, remoteArrival(peerIdentity(1)), until).Reservation()
	if !ok {
		t.Fatal("Reserve must win")
	}

	// The parallel instance dropped before reserving: all it could possibly
	// pass to Release is a zero token, and there is no key-addressed
	// Release to reach for.
	if !cache.Release(ctx, datagram.ReservationToken{}).IsApplied() {
		t.Fatal("a zero token release is a no-op reported as ok")
	}
	if got := cache.Has(ctx, key).Outcome(); got != datagram.HasHit {
		t.Fatalf("foreign reservation was cancelled: Has = %s", got)
	}
	if !cache.Commit(ctx, held).IsApplied() {
		t.Fatal("the holder must still own its reservation")
	}
}

// Reserve takes the slot physically, so Commit can never run out of room.
func TestBaseCacheCommitNeverFailsForCapacity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	const capacity = 4
	cache := newBaseCache(t, clock, capacity)
	until := baseTime.Add(time.Minute)

	tokens := make([]datagram.ReservationToken, 0, capacity)
	for i := 0; i < capacity; i++ {
		result := cache.Reserve(ctx, replayKey(byte(10+i)), remoteArrival(peerIdentity(byte(i))), until)
		token, ok := result.Reservation()
		if !ok {
			t.Fatalf("Reserve %d = %s, want reserved", i, result.Outcome())
		}
		tokens = append(tokens, token)
	}
	// The cache is full and refuses new keys — the reservations already
	// taken are unaffected.
	overflow := cache.Reserve(ctx, replayKey(200), remoteArrival(peerIdentity(0)), until)
	if overflow.Outcome() != datagram.ReserveRejected {
		t.Fatalf("overflow = %s, want rejected", overflow.Outcome())
	}
	for i, token := range tokens {
		if result := cache.Commit(ctx, token); !result.IsApplied() {
			t.Fatalf("Commit %d failed: %v", i, result.Err())
		}
	}
}

// Overflow is charged to the noisiest neighbour, not flushed globally.
func TestBaseCacheOverflowRejectsTheNoisiestNeighbour(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	const capacity = 4
	cache := newBaseCache(t, clock, capacity)
	until := baseTime.Add(time.Minute)
	noisy := remoteArrival(peerIdentity(1))

	for i := 0; i < capacity; i++ {
		token, ok := cache.Reserve(ctx, replayKey(byte(20+i)), noisy, until).Reservation()
		if !ok {
			t.Fatalf("Reserve %d must win", i)
		}
		if !cache.Commit(ctx, token).IsApplied() {
			t.Fatalf("Commit %d must apply", i)
		}
	}

	rejected := cache.Reserve(ctx, replayKey(90), noisy, until)
	if rejected.Outcome() != datagram.ReserveRejected {
		t.Fatalf("noisy neighbour = %s, want rejected", rejected.Outcome())
	}
	if !errors.Is(rejected.Err(), datagram.ErrReplayCacheCapacity) {
		t.Fatalf("rejection cause = %v, want ErrReplayCacheCapacity", rejected.Err())
	}
	if got := cache.Metrics().RejectedNoisyPeer; got != 1 {
		t.Fatalf("RejectedNoisyPeer = %d, want 1", got)
	}

	// A quiet neighbour is admitted at the noisy one's expense — the quiet
	// peer is not punished for somebody else's flood, and the cache is not
	// flushed.
	quiet := remoteArrival(peerIdentity(200))
	if got := cache.Reserve(ctx, replayKey(91), quiet, until).Outcome(); got != datagram.ReserveReserved {
		t.Fatalf("quiet neighbour = %s, want reserved", got)
	}
	metrics := cache.Metrics()
	if metrics.EvictedNoisyPeer != 1 {
		t.Fatalf("EvictedNoisyPeer = %d, want 1", metrics.EvictedNoisyPeer)
	}
	if cache.Len() != capacity {
		t.Fatalf("cache holds %d entries, want %d", cache.Len(), capacity)
	}
}

// A held reservation is never an eviction victim: someone owns that token
// right now, and dropping it would hand the same key to a second instance
// of the same frame.
func TestBaseCacheNeverEvictsAHeldReservation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	const capacity = 3
	cache := newBaseCache(t, clock, capacity)
	until := baseTime.Add(time.Minute)
	noisy := remoteArrival(peerIdentity(1))

	for i := 0; i < capacity; i++ {
		if _, ok := cache.Reserve(ctx, replayKey(byte(30+i)), noisy, until).Reservation(); !ok {
			t.Fatalf("Reserve %d must win", i)
		}
	}
	quiet := remoteArrival(peerIdentity(200))
	result := cache.Reserve(ctx, replayKey(95), quiet, until)
	if result.Outcome() != datagram.ReserveRejected {
		t.Fatalf("quiet neighbour = %s, want rejected: nothing is safe to evict", result.Outcome())
	}
	if got := cache.Metrics().RejectedCapacity; got != 1 {
		t.Fatalf("RejectedCapacity = %d, want 1", got)
	}
}

// The key lives through base_until inclusively and dies strictly after it.
func TestBaseCacheKeyLivesThroughTheInclusiveBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	authTime := baseTime
	clock := newClock(authTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(5)
	peer := remoteArrival(peerIdentity(1))

	// The clamp the RECEIVE PATH applies, not one the cache re-derives: the cache
	// is handed an already-clamped replay_until (ComputeDeadlines).
	baseUntil := datagram.BaseReplayDeadline(authTime, authTime.Add(time.Hour))
	if want := authTime.Add(domain.DatagramBaseReplayWindow); !baseUntil.Equal(want) {
		t.Fatalf("base_until = %s, want %s", baseUntil, want)
	}

	token, ok := cache.Reserve(ctx, key, peer, baseUntil).Reservation()
	if !ok {
		t.Fatal("Reserve must win")
	}
	if !cache.Commit(ctx, token).IsApplied() {
		t.Fatal("Commit must apply")
	}

	// now == replay_until == valid_until: the key is still alive, so the
	// repeat is caught by anti-replay rather than slipping through.
	clock.Set(baseUntil)
	if got := cache.Has(ctx, key).Outcome(); got != datagram.HasHit {
		t.Fatalf("at the boundary Has = %s, want hit", got)
	}
	if got := cache.Reserve(ctx, key, peer, baseUntil).Outcome(); got != datagram.ReserveDuplicate {
		t.Fatalf("at the boundary Reserve = %s, want duplicate", got)
	}

	// Strictly after it only the anti-replay meaning dies; a frame with this
	// transcript is already dead by Validity anyway.
	clock.Set(baseUntil.Add(time.Nanosecond))
	if got := cache.Has(ctx, key).Outcome(); got != datagram.HasMiss {
		t.Fatalf("past the boundary Has = %s, want miss", got)
	}
}

// A key that is merely HELD answers the early probe exactly as a committed one
// does. The reservation dimension is the cache's own business — what it still
// owes the key — and it is not something the receive path may act on: a
// duplicate arriving while a concurrent instance of the same frame is mid-flight
// must be dropped, not re-run because "no verdict yet".
func TestBaseCacheHitCoversAHeldReservationAndACommittedOne(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(6)
	peer := remoteArrival(peerIdentity(1))

	token, ok := cache.Reserve(ctx, key, peer, baseTime.Add(time.Minute)).Reservation()
	if !ok {
		t.Fatal("Reserve must win")
	}
	if got := cache.Has(ctx, key).Outcome(); got != datagram.HasHit {
		t.Fatalf("a held key: Has = %s, want hit", got)
	}

	if !cache.Commit(ctx, token).IsApplied() {
		t.Fatal("Commit must apply")
	}
	if got := cache.Has(ctx, key).Outcome(); got != datagram.HasHit {
		t.Fatalf("a committed key: Has = %s, want hit", got)
	}
}

// The cleanup-only phase in its base form: an expired record that owes
// nothing is swept, while an expired record with an unfinished reservation
// stays in quarantine and keeps answering duplicate.
func TestBaseCacheSweepKeepsQuarantinedReservations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	until := baseTime.Add(time.Minute)
	peer := remoteArrival(peerIdentity(1))

	settled, ok := cache.Reserve(ctx, replayKey(7), peer, until).Reservation()
	if !ok {
		t.Fatal("Reserve must win")
	}
	if !cache.Commit(ctx, settled).IsApplied() {
		t.Fatal("Commit must apply")
	}
	quarantined := replayKey(8)
	if _, ok := cache.Reserve(ctx, quarantined, peer, until).Reservation(); !ok {
		t.Fatal("Reserve must win")
	}

	clock.Set(until.Add(time.Second))
	if removed := cache.SweepExpired(ctx); removed != 1 {
		t.Fatalf("Sweep removed %d records, want 1", removed)
	}
	if got := cache.Has(ctx, quarantined).Outcome(); got != datagram.HasHit {
		t.Fatalf("quarantined key = %s, want hit", got)
	}
	if got := cache.Reserve(ctx, quarantined, peer, until).Outcome(); got != datagram.ReserveDuplicate {
		t.Fatalf("quarantined key must keep answering duplicate, got %s", got)
	}
}

// Releasing a committed reservation is a no-op: its fate is already fixed,
// and dropping the key would re-open a frame the node has acted on.
func TestBaseCacheReleaseOfACommittedReservationIsANoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	key := replayKey(9)
	peer := remoteArrival(peerIdentity(1))

	token, _ := cache.Reserve(ctx, key, peer, baseTime.Add(time.Minute)).Reservation()
	if !cache.Commit(ctx, token).IsApplied() {
		t.Fatal("Commit must apply")
	}
	if !cache.Release(ctx, token).IsApplied() {
		t.Fatal("Release of a committed reservation reports ok")
	}
	if got := cache.Has(ctx, key).Outcome(); got != datagram.HasHit {
		t.Fatalf("committed key disappeared: Has = %s", got)
	}
}

// Concurrent traffic over the whole surface must stay race-free: the cache
// sits on the receive path of every datagram and is touched by every session
// goroutine at once.
func TestBaseCacheIsSafeUnderConcurrentTraffic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 64)
	until := baseTime.Add(time.Minute)

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				key := replayKey(byte(worker*10 + i%7))
				peer := remoteArrival(peerIdentity(byte(worker)))
				cache.Has(ctx, key)
				if token, ok := cache.Reserve(ctx, key, peer, until).Reservation(); ok {
					if i%2 == 0 {
						cache.Commit(ctx, token)
					} else {
						cache.Release(ctx, token)
					}
				}
				cache.SweepExpired(ctx)
			}
		}(worker)
	}
	wg.Wait()
}

// A reservation nobody ever finished must not lock a slot forever. The base
// plane has no one to retry it: it keeps no frames, so there is no Requeue
// and no quarantine owner, and a branch lost to a panic in the pipeline would
// otherwise hold its slot until the process restarts. Fill the cache with such
// reservations and the node stops answering `reserved` to EVERY neighbour.
func TestBaseCacheReclaimsAbandonedReservations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 4)
	until := baseTime.Add(time.Minute)

	for i := 0; i < 4; i++ {
		if got := cache.Reserve(ctx, replayKey(byte(30+i)), remoteArrival(peerIdentity(1)), until).Outcome(); got != datagram.ReserveReserved {
			t.Fatalf("Reserve %d = %s, want reserved", i, got)
		}
	}

	// A year later not one of them can be part of a live operation.
	clock.Set(baseTime.Add(365 * 24 * time.Hour))

	if swept := cache.SweepExpired(ctx); swept != 4 {
		t.Fatalf("Sweep = %d, want the four abandoned reservations reclaimed", swept)
	}
	if cache.Len() != 0 {
		t.Fatalf("Len = %d, want an empty cache", cache.Len())
	}
	if got := cache.Metrics().AbandonedReservations; got != 4 {
		t.Fatalf("abandoned reservations metric = %d, want 4", got)
	}
	if got := cache.Reserve(ctx, replayKey(40), remoteArrival(peerIdentity(2)), clock.Now().Add(time.Minute)).Outcome(); got != datagram.ReserveReserved {
		t.Fatalf("Reserve from an innocent neighbour = %s, want reserved", got)
	}
}

// The watchdog only fires past replay_until plus the grace: inside it the
// reservation is still a live operation and the key stays taken.
func TestBaseCacheKeepsAReservationInsideTheGrace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 4)
	key := replayKey(41)
	until := baseTime.Add(time.Minute)

	rsv, ok := cache.Reserve(ctx, key, remoteArrival(peerIdentity(1)), until).Reservation()
	if !ok {
		t.Fatal("Reserve must win")
	}
	clock.Set(until.Add(time.Second))
	if swept := cache.SweepExpired(ctx); swept != 0 {
		t.Fatalf("Sweep = %d, want the quarantine kept inside the grace", swept)
	}
	if got := cache.Has(ctx, key); got.Outcome() != datagram.HasHit {
		t.Fatalf("Has = %s, want the key still held", got.Outcome())
	}
	if got := cache.Release(ctx, rsv); !got.IsApplied() {
		t.Fatalf("Release = %v", got.Err())
	}
	if cache.Len() != 0 {
		t.Fatalf("Len = %d, want the released record gone", cache.Len())
	}
}

// An overflow that finds no victim at all is still a refusal, and a refusal
// nobody counts is a node that silently stops accepting routed datagrams.
func TestBaseCacheCountsARefusalWithNoVictim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 1)
	until := baseTime.Add(time.Minute)

	if got := cache.Reserve(ctx, replayKey(42), datagram.LocalIngress(), until).Outcome(); got != datagram.ReserveReserved {
		t.Fatalf("Reserve = %s, want reserved", got)
	}
	before := cache.Metrics()
	if got := cache.Reserve(ctx, replayKey(43), datagram.LocalIngress(), until).Outcome(); got != datagram.ReserveRejected {
		t.Fatalf("Reserve over capacity = %s, want rejected", got)
	}
	after := cache.Metrics()
	if after.RejectedCapacity+after.RejectedNoisyPeer == before.RejectedCapacity+before.RejectedNoisyPeer {
		t.Fatal("a capacity refusal must be counted")
	}
}
