package datagram_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// replay_base_fairness_test.go pins the two properties the anti-replay cache
// needs at capacity, and both of them are anti-replay properties rather than
// performance ones — a record evicted or refused here is a frame that can be
// replayed at this node afterwards:
//
//   - the fairness bucket is the QUOTA OWNER, so a neighbour cannot look quiet by
//     spreading its records over reconnects and parallel sessions (§5);
//   - "the cache is full" is a statement about records that are actually live, so
//     a bounded pass may not report a full cache while removable records sit in it.
//
// Every fixture below therefore needs a shape the previous rounds' had none of:
// ONE neighbour on SEVERAL channels. With one channel per neighbour, "keyed on
// the channel" and "keyed on the owner" are the same observation — which is
// exactly how the cache shipped a bucket a reconnect renews.

// replayChannel mints a stable, distinct channel per name, so a fixture can say
// "the same neighbour, another socket" at all.
func replayChannel(name string) datagram.ChannelID {
	var hash uint64 = 14695981039346656037
	for i := 0; i < len(name); i++ {
		hash ^= uint64(name[i])
		hash *= 1099511628211
	}
	if hash == 0 {
		hash = 1
	}
	return datagram.NetworkChannel(domain.ConnID(hash))
}

// provenArrival is one neighbour of an ACCEPTED connection as the receive path
// builds it: the identity is proven, so the SAME budget key is charged whatever
// socket the frames arrive on — which is the whole reason a bucket keyed on it
// survives a reconnect.
func provenArrival(id domain.PeerIdentity, session string) datagram.IngressPeer {
	return datagram.ProvenIngress(replayChannel(id.String()+"/"+session), id)
}

// reserveCommitted takes a key and settles it, which is the only state a record
// can be evicted from: a held reservation belongs to an instance of a frame that
// is still being processed.
func reserveCommitted(
	t *testing.T,
	cache *datagram.BaseReplayCache,
	key domain.ReplayKey,
	incoming datagram.IngressPeer,
	until time.Time,
) {
	t.Helper()
	result := cache.Reserve(context.Background(), key, incoming, until)
	token, ok := result.Reservation()
	if !ok {
		t.Fatalf("Reserve(%s) = %s, want reserved", incoming, result.Outcome())
	}
	if applied := cache.Commit(context.Background(), token); !applied.IsApplied() {
		t.Fatalf("Commit(%s): %v", incoming, applied.Err())
	}
}

// ---------------------------------------------------------------------------
// (a) The overflow bucket is the owner, not the arrival
// ---------------------------------------------------------------------------

// TestSpreadingOverChannelsDoesNotHideTheNoisiestNeighbour is the round's first
// P1 on its cheapest observable consequence.
//
// A channel lives until its connection closes; a replay record lives up to
// base_until, which is five minutes. So a bucket keyed on the arrival — which
// carries the channel — is a bucket whose renewal the neighbour controls: fill
// the cache, tear the session down, dial again. Worse than the renewal is what it
// does at capacity: the flooder's records, spread over N sessions, read as N
// lightly loaded neighbours, so the "noisiest" bucket becomes the HONEST peer
// that has only one channel. It is then refused its own frames, or has its
// records evicted — and each evicted record is a frame this node will accept a
// second time.
//
// The fixture holds BOTH peers, because a fixture with only the flooder cannot
// tell a fair eviction from a refusal of everything.
//
// The mutations this kill:
//
//   - keying the tally on the whole IngressPeer again (channel included): the
//     honest neighbour is then the busiest bucket and IS the incoming one, so its
//     third record is Rejected instead of Reserved;
//   - keying it on the presented identity: the positive control below breaks
//     instead — two neighbours could no longer be told apart on a dialled session;
//   - matching the eviction walk on the arrival while the tally is keyed on the
//     owner: no record matches the chosen bucket, the eviction fails, and the
//     honest peer is refused with RejectedCapacity.
func TestSpreadingOverChannelsDoesNotHideTheNoisiestNeighbour(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	const capacity = 8
	cache := newBaseCache(t, clock, capacity)
	until := baseTime.Add(time.Minute)

	flooder := peerIdentity(1)
	honest := peerIdentity(100)

	// The fixture is only a "spread" if the sockets really differ. Without this
	// the whole test would pass against a channel-keyed bucket.
	if provenArrival(flooder, "session-0") == provenArrival(flooder, "session-1") {
		t.Fatal("the fixture reconnected onto the SAME channel: it cannot tell the two keyings apart")
	}

	// Six records of ONE neighbour over six sessions.
	for i := 0; i < 6; i++ {
		reserveCommitted(t, cache, replayKey(byte(10+i)),
			provenArrival(flooder, "session-"+string(rune('a'+i))), until)
	}
	// Two records of an honest neighbour on its single session.
	honestArrival := provenArrival(honest, "only-session")
	for i := 0; i < 2; i++ {
		reserveCommitted(t, cache, replayKey(byte(30+i)), honestArrival, until)
	}

	// One bucket per NEIGHBOUR, whatever the socket count.
	if load := cache.OwnerLoadForTest(provenArrival(flooder, "session-x")); load != 6 {
		t.Fatalf("the flooder reads %d records, want the 6 its owner holds across every session: "+
			"the bucket must follow the neighbour, not the socket", load)
	}
	if load := cache.OwnerLoadForTest(honestArrival); load != 2 {
		t.Fatalf("the honest neighbour holds %d records, want 2", load)
	}

	// THE FINDING: the cache is full (8 of 8) and the honest neighbour asks for
	// one more slot. The victim must be the flooder's oldest record.
	if got := cache.Reserve(ctx, replayKey(200), honestArrival, until).Outcome(); got != datagram.ReserveReserved {
		t.Fatalf("the honest neighbour was refused at capacity (%s): the flooder looked quiet on "+
			"every single one of its six channels", got)
	}
	if got := cache.Metrics().EvictedNoisyPeer; got != 1 {
		t.Fatalf("EvictedNoisyPeer = %d, want the one eviction that made room", got)
	}
	if got := cache.Has(ctx, replayKey(10)).Outcome(); got != datagram.HasMiss {
		t.Fatalf("the flooder's oldest record survived: Has = %s", got)
	}
	for i := 0; i < 2; i++ {
		if got := cache.Has(ctx, replayKey(byte(30+i))).Outcome(); got != datagram.HasHit {
			t.Fatalf("the honest neighbour lost record %d to a flood it did not send", i)
		}
	}
	if load := cache.OwnerLoadForTest(honestArrival); load != 3 {
		t.Fatalf("the honest neighbour holds %d records, want 3", load)
	}
	if load := cache.OwnerLoadForTest(provenArrival(flooder, "session-a")); load != 5 {
		t.Fatalf("the flooder keeps %d records, want 5: exactly one victim per overflow", load)
	}

	// POSITIVE CONTROL. Without it every assertion above is satisfied by a cache
	// that evicts from whichever bucket is not the caller's: the flooder itself,
	// still the noisiest, must be refused rather than served at somebody's
	// expense.
	refused := cache.Reserve(ctx, replayKey(201), provenArrival(flooder, "session-z"), until)
	if refused.Outcome() != datagram.ReserveRejected {
		t.Fatalf("the noisiest neighbour was served on a fresh channel (%s): the refusal is what "+
			"charges the overflow to the peer that caused it", refused.Outcome())
	}
	if got := cache.Metrics().RejectedNoisyPeer; got != 1 {
		t.Fatalf("RejectedNoisyPeer = %d, want 1", got)
	}
}

// TestTheLocalBucketIsSharedWithNobody keeps the remaining bucket rule visible,
// because it is a way to collapse neighbours into one another: our own frames
// are billed to nobody, and that bucket may not be shared with a neighbour's — a
// shared bucket is a shared eviction victim.
//
// It used to carry a third bucket beside these two: an arrival that named no
// budget key at all, bucketed by the name it carried. No receive path could
// produce one — HandleInbound refuses an arrival without an admission key — and
// the constructor that built it is gone, so what is left here is the split that
// exists.
func TestTheLocalBucketIsSharedWithNobody(t *testing.T) {
	t.Parallel()

	clock := newClock(baseTime)
	cache := newBaseCache(t, clock, 8)
	until := baseTime.Add(time.Minute)

	peer := peerIdentity(7)
	billed := provenArrival(peer, "session-1")

	reserveCommitted(t, cache, replayKey(60), datagram.LocalIngress(), until)
	reserveCommitted(t, cache, replayKey(61), billed, until)

	if load := cache.OwnerLoadForTest(datagram.LocalIngress()); load != 1 {
		t.Fatalf("the local bucket holds %d records, want our own single one", load)
	}
	if load := cache.OwnerLoadForTest(billed); load != 1 {
		t.Fatalf("the billed neighbour holds %d records, want 1: this node's own frames must not "+
			"spend the bucket a neighbour is charged from", load)
	}
}

// ---------------------------------------------------------------------------
// (b) A bounded pass may not report a full cache while records are removable
// ---------------------------------------------------------------------------

// TestAFullCacheFindsExpiredRecordsBehindTheScanBudget is the round's second P1.
//
// base_until is measured from the SIGNED auth.time, so deadlines are NOT
// monotonic in insertion order: a frame authored five minutes ago is inserted
// last and expires first. A pass that walks the oldest INSERTED records and gives
// up after a fixed number of INSPECTIONS therefore spends its whole budget on
// live records while expired ones sit behind them — and answers `rejected`. The
// next frame runs the same pass over the same prefix and is refused for the same
// reason, so the routed plane stops remembering ANY new frame until the records
// at the front of that walk expire on their own. That is an anti-replay outage,
// not a slow path.
//
// The fixture puts the expired records where the old pass could not reach them:
// behind a block of HELD reservations larger than the budget. Held records are
// removable by neither the sweep nor the eviction, so nothing but reaching the
// expired ones can free a slot.
//
// The mutations this kills:
//
//   - bounding the pass by records INSPECTED again: the newcomer is Rejected;
//   - ordering the pass by insertion instead of by deadline: same;
//   - dropping the heap's "stop at the first record that is not removable yet"
//     guard: the positive control below starts evicting live records.
func TestAFullCacheFindsExpiredRecordsBehindTheScanBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	// The block of held records is deliberately larger than any bounded scan the
	// implementation may keep, so the expired tail cannot be reached by accident.
	const (
		held     = 200
		expiring = 8
		capacity = held + expiring
	)
	cache := newBaseCache(t, clock, capacity)

	// The head of the cache: reservations nobody finished, alive for a long time
	// and therefore neither sweepable nor evictable.
	blocker := provenArrival(peerIdentity(1), "session-1")
	for i := 0; i < held; i++ {
		if _, ok := cache.Reserve(ctx, longReplayKey(i), blocker, baseTime.Add(time.Hour)).Reservation(); !ok {
			t.Fatalf("held reservation %d must win", i)
		}
	}
	// The tail: settled records of another neighbour, with a SHORT deadline —
	// which is exactly what a frame authored a while ago produces.
	shortLived := provenArrival(peerIdentity(2), "session-1")
	for i := 0; i < expiring; i++ {
		reserveCommitted(t, cache, longReplayKey(held+i), shortLived, baseTime.Add(time.Minute))
	}
	if cache.Len() != capacity {
		t.Fatalf("the fixture holds %d records, want a full cache of %d", cache.Len(), capacity)
	}

	// POSITIVE CONTROL, taken BEFORE anything expires: with nothing removable the
	// refusal has to stand. Without this the test would pass against a cache that
	// simply admits everybody.
	newcomer := provenArrival(peerIdentity(3), "session-1")
	full := cache.Reserve(ctx, longReplayKey(900), newcomer, baseTime.Add(time.Hour))
	if full.Outcome() != datagram.ReserveRejected {
		t.Fatalf("a genuinely full cache admitted a new record (%s): every record in it is either "+
			"held or live, so there is nothing to take", full.Outcome())
	}

	// Now the tail is past its deadline while the head is not.
	clock.Set(baseTime.Add(2 * time.Minute))

	// THE FINDING: eight records are removable, and they are behind the head.
	admitted := cache.Reserve(ctx, longReplayKey(901), newcomer, clock.Now().Add(time.Hour))
	if admitted.Outcome() != datagram.ReserveReserved {
		t.Fatalf("the cache refused a new record (%s) while %d of its %d records were already "+
			"expired: the plane stops remembering frames until the head of the walk expires",
			admitted.Outcome(), expiring, capacity)
	}
	if got := cache.Metrics().ExpiredSwept; got == 0 {
		t.Fatal("the reservation was admitted without a single record being swept: room came from " +
			"somewhere it should not have")
	}
	// The head is untouched: the room came from the expired tail and not from
	// somebody's live reservation.
	if got := cache.OwnerLoadForTest(blocker); got != held {
		t.Fatalf("the blocking neighbour holds %d records, want its %d untouched: a held "+
			"reservation is owned by an instance of a frame that is still running", got, held)
	}
	if got := cache.OwnerLoadForTest(shortLived); got != 0 {
		t.Fatalf("the expired neighbour still holds %d records, want them all reclaimed", got)
	}
}

// TestTheFullSweepReachesEveryExpiredRecord is the same rule for the pass the
// node's background loop runs: it is bounded by nothing, so one call has to
// reclaim every removable record whatever order they were inserted in.
func TestTheFullSweepReachesEveryExpiredRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := newClock(baseTime)
	const records = 300
	cache := newBaseCache(t, clock, records)

	peer := provenArrival(peerIdentity(4), "session-1")
	// Deadlines DESCEND with insertion order, which is the arrangement an
	// insertion-ordered walk reads backwards.
	for i := 0; i < records; i++ {
		reserveCommitted(t, cache, longReplayKey(i), peer,
			baseTime.Add(time.Duration(records-i)*time.Second))
	}

	clock.Set(baseTime.Add(time.Duration(records+1) * time.Second))
	if swept := cache.SweepExpired(ctx); swept != records {
		t.Fatalf("the full sweep reclaimed %d records, want all %d", swept, records)
	}
	if cache.Len() != 0 {
		t.Fatalf("the cache still holds %d records after a full sweep", cache.Len())
	}
	if swept := cache.SweepExpired(ctx); swept != 0 {
		t.Fatalf("a second sweep reclaimed %d records, want none", swept)
	}
}

// longReplayKey builds a distinct key for fixtures that need more of them than a
// single seed byte can produce.
func longReplayKey(seed int) domain.ReplayKey {
	var key domain.ReplayKey
	for i := range key {
		key[i] = byte(seed>>8) ^ byte(i)
	}
	key[0] = byte(seed)
	key[1] = byte(seed >> 8)
	return key
}
