package datagram

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// explore_test.go states the rotation guarantee of §4.3 exactly as the spec
// states it — no stronger. Consecutive explore sends of ONE key walk the
// candidates round-robin under TWO conditions at once: no other send of the
// same key in between, and an unchanged ordered candidate set. Everything
// else legally degrades to decorrelation, and these tests pin the
// degradation instead of denying it.

// exploreFixture builds a destination reachable only through K relays —
// no direct session, because the direct path is not part of the rotation.
func exploreFixture(t *testing.T, opts schedFixtureOpts, relays ...domain.PeerIdentity) (*schedFixture, domain.PeerIdentity) {
	t.Helper()
	fixture := newSchedFixture(t, opts)
	dst := domaintest.ID("dst")
	hints := make([]RouteHint, 0, len(relays))
	for _, relay := range relays {
		fixture.datagramPeer(relay, time.Hour)
		hints = append(hints, fixture.route(relay, 1))
	}
	fixture.routes.set(dst, hints...)
	return fixture, dst
}

// exploreSend performs one explore send and returns the hop it was queued
// to.
func exploreSend(t *testing.T, fixture *schedFixture, dst domain.PeerIdentity, mutators ...func(*frameMutation)) domain.PeerIdentity {
	t.Helper()
	mutation := frameMutation{dtype: schedDType}
	for _, mutate := range mutators {
		mutate(&mutation)
	}
	// The fixture's own switch: "did the caller ask for a TRANSITED frame". It is
	// a neighbour NAME and not an IngressPeer, because the arrival itself is
	// built by fixture.transit out of a real receive path — this is the test's
	// instruction about which path to take, and nothing more.
	if via := mutation.via; !via.IsZero() {
		// A transited frame rotates the SAME counter (§4.3): the public entry
		// for it is HandleInbound, and the hop it reached shows up in the
		// emitter journal rather than in a synchronous outcome.
		fixture.sender.reset()
		result := fixture.transit(t, via, dst, withExplore, withDType(mutation.dtype))
		if result.Outcome() != InboundForwarded {
			t.Fatalf("explore transit outcome = %s (%s)", result.Outcome(), result.Reason())
		}
		tried := fixture.sender.tried()
		if len(tried) == 0 {
			t.Fatal("explore transit published to nobody")
		}
		return tried[len(tried)-1]
	}
	outcome := fixture.send(t, dst, withExplore, withDType(mutation.dtype))
	hop, queued := outcome.NextHop()
	if !queued {
		t.Fatalf("explore send outcome = %s, want queued", outcome)
	}
	return hop
}

type frameMutation struct {
	via   domain.PeerIdentity
	dtype domain.DType
}

func fromTransit(peer domain.PeerIdentity) func(*frameMutation) {
	return func(m *frameMutation) { m.via = peer }
}

func ofDType(dtype domain.DType) func(*frameMutation) {
	return func(m *frameMutation) { m.dtype = dtype }
}

// TestExploreRotatesConsecutiveSendsOfTheSameKey is the guarantee itself:
// K >= 2, no direct path, an unchanged ordered candidate set and no other
// traffic on the key — two consecutive sends deterministically pick
// different first hops. The frames are identical, payload included, which
// is the case a "new hash" scheme would get wrong.
func TestExploreRotatesConsecutiveSendsOfTheSameKey(t *testing.T) {
	t.Parallel()

	relays := []domain.PeerIdentity{
		domaintest.ID("relay-a"), domaintest.ID("relay-b"), domaintest.ID("relay-c"),
	}
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, relays...)

	visited := make(map[domain.PeerIdentity]int, len(relays))
	previous := domain.PeerIdentity{}
	for i := 0; i < len(relays); i++ {
		hop := exploreSend(t, fixture, dst)
		if i > 0 && hop == previous {
			t.Fatalf("send %d repeated the previous first hop %s", i, hop)
		}
		visited[hop]++
		previous = hop
	}
	if len(visited) != len(relays) {
		t.Fatalf("K consecutive sends must visit every candidate once, got %v", visited)
	}
}

// TestExploreWithSingleCandidateDegenerates pins the honest edge: at K = 1
// there is no alternative to rotate to, and that must not be an error.
func TestExploreWithSingleCandidateDegenerates(t *testing.T) {
	t.Parallel()

	only := domaintest.ID("relay-only")
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, only)

	for i := 0; i < 3; i++ {
		if hop := exploreSend(t, fixture, dst); hop != only {
			t.Fatalf("send %d went to %s, want the only candidate", i, hop)
		}
	}
}

// TestExploreChangedCandidateSetMayRepeatTheHop pins the second condition
// of the guarantee: the index is taken modulo K over the SORTED set, so any
// change of composition, health, order or K lawfully returns the same hop.
// The test fixes that this is allowed, not that it is required.
func TestExploreChangedCandidateSetMayRepeatTheHop(t *testing.T) {
	t.Parallel()

	relayA := domaintest.ID("relay-a")
	relayB := domaintest.ID("relay-b")
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, relayA, relayB)

	first := exploreSend(t, fixture, dst)

	// The set shrinks to one candidate: whatever the counter does, the
	// only lawful answer is the surviving hop.
	fixture.peers.stall(relayA)
	fixture.peers.stall(relayB)
	survivor := domaintest.ID("relay-survivor")
	fixture.datagramPeer(survivor, time.Hour)
	fixture.routes.set(dst, fixture.route(survivor, 1))

	if second := exploreSend(t, fixture, dst); second != survivor {
		t.Fatalf("after the set changed, hop = %s, want %s", second, survivor)
	}
	if first != relayA && first != relayB {
		t.Fatalf("the first send left the original set: %s", first)
	}
}

// TestExploreOtherKeysDoNotShiftTheCounter pins why the key is wider than
// dst: frames of another type have their own candidate set and their own K,
// and must not move this key's epoch. Without it two consecutive retries of
// one transfer would land on the same first hop again.
func TestExploreOtherKeysDoNotShiftTheCounter(t *testing.T) {
	t.Parallel()

	relays := []domain.PeerIdentity{
		domaintest.ID("relay-a"), domaintest.ID("relay-b"), domaintest.ID("relay-c"),
	}
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, relays...)

	first := exploreSend(t, fixture, dst)
	// Same destination, different dtype: a different key entirely.
	exploreSend(t, fixture, dst, ofDType("some_other_type"))
	exploreSend(t, fixture, dst, ofDType("some_other_type"))
	second := exploreSend(t, fixture, dst)

	if first == second {
		t.Fatalf("frames of another type shifted this key's counter: both sends chose %s", first)
	}
}

// TestExploreTransitOfTheSameTripleShiftsTheCounter pins the degradation
// the spec admits to: a transit frame with the SAME (dst, dtype) rotates
// through the same counter, so a sender's two retries can land on the same
// hop again. The guarantee degrades to decorrelation — it is not denied.
func TestExploreTransitOfTheSameTripleShiftsTheCounter(t *testing.T) {
	t.Parallel()

	relayA := domaintest.ID("relay-a")
	relayB := domaintest.ID("relay-b")
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, relayA, relayB)
	fixture.routes.setCached(dst, fixture.route(relayA, 1), fixture.route(relayB, 1))

	first := exploreSend(t, fixture, dst)
	exploreSend(t, fixture, dst, fromTransit(domaintest.ID("upstream")))
	second := exploreSend(t, fixture, dst)

	if first != second {
		t.Fatalf("with K=2 an intervening transit send of the same triple must return the same hop: %s then %s", first, second)
	}
}

// TestExploreKeepsTheDirectSessionFirst pins the explicit rule: the direct
// session is NOT part of the rotation — step 1 always tries it first, and
// the rotation acts on routing-table candidates for when the direct path is
// unavailable or exhausted.
func TestExploreKeepsTheDirectSessionFirst(t *testing.T) {
	t.Parallel()

	relayA := domaintest.ID("relay-a")
	relayB := domaintest.ID("relay-b")
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, relayA, relayB)
	fixture.direct.set(dst, fixture.datagramPeer(dst, time.Hour, schedDType.String()))

	for i := 0; i < 3; i++ {
		fixture.sender.reset()
		if hop := exploreSend(t, fixture, dst); hop != dst {
			t.Fatalf("send %d left the direct session for %s", i, hop)
		}
	}

	// With the direct session refusing the enqueue, the rotation is
	// visible on the routing-table candidates behind it.
	fixture.sender.refuseHop(dst)
	fixture.sender.reset()
	firstRelay := exploreSend(t, fixture, dst)
	fixture.sender.reset()
	secondRelay := exploreSend(t, fixture, dst)
	if firstRelay == secondRelay {
		t.Fatalf("the routing candidates behind the direct session must rotate, both chose %s", firstRelay)
	}
}

// TestExploreOffsetDiffersBetweenNodes pins the role of
// HMAC(node_local_secret, dst): two nodes with identical routing tables
// must not hammer the same candidate first.
func TestExploreOffsetDiffersBetweenNodes(t *testing.T) {
	t.Parallel()

	nodeA := newExploreRotator(schedSecret{secret: []byte("node-a-secret")}, DefaultExploreCounters)
	nodeB := newExploreRotator(schedSecret{secret: []byte("node-b-secret")}, DefaultExploreCounters)

	const k = 4
	differences := 0
	for i := 0; i < 16; i++ {
		dst := domaintest.ID(string(rune('a'+i)) + "-destination")
		if nodeA.offset(dst, k) != nodeB.offset(dst, k) {
			differences++
		}
	}
	if differences == 0 {
		t.Fatal("two nodes produced the same starting offset for all 16 destinations")
	}
}

// TestExploreCountersAreBounded pins the LRU: sends to a large set of
// random keys must not grow memory without bound. Eviction is what makes
// the guarantee conditional on "the key is in the LRU".
func TestExploreCountersAreBounded(t *testing.T) {
	t.Parallel()

	const capacity = 8
	rotator := newExploreRotator(schedSecret{secret: []byte("secret")}, capacity)
	for i := 0; i < 500; i++ {
		key := newExploreKey(domaintest.ID(string(rune('a'+i%26))+"-dst"), domain.DType("t"+string(rune('a'+i%17))))
		rotator.next(key)
	}
	rotator.mu.Lock()
	entries, order := len(rotator.entries), rotator.order.Len()
	rotator.mu.Unlock()
	if entries > capacity || order > capacity {
		t.Fatalf("counter LRU grew to %d entries (list %d), capacity is %d", entries, order, capacity)
	}
}

// TestExploreEvictionDegradesToDecorrelation pins the honest half of the
// bounded LRU: once a key is evicted its counter restarts from the
// node-wide one, so the round-robin guarantee is gone and only
// decorrelation remains.
func TestExploreEvictionDegradesToDecorrelation(t *testing.T) {
	t.Parallel()

	rotator := newExploreRotator(schedSecret{secret: []byte("secret")}, 1)
	kept := newExploreKey(domaintest.ID("dst-kept"), schedDType)
	other := newExploreKey(domaintest.ID("dst-other"), schedDType)

	first := rotator.next(kept)
	rotator.next(other) // evicts `kept`
	second := rotator.next(kept)

	if second == first+1 {
		t.Fatal("an evicted key must not continue its own sequence")
	}
	rotator.mu.Lock()
	entries := len(rotator.entries)
	rotator.mu.Unlock()
	if entries != 1 {
		t.Fatalf("LRU holds %d entries, capacity is 1", entries)
	}
}

// TestExploreCounterIsAtomicUnderConcurrency pins that two concurrent sends
// of one key never receive the same epoch. The rotation degrades to
// decorrelation under concurrency, but a shared counter value would be a
// data race, not a degradation.
func TestExploreCounterIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()

	rotator := newExploreRotator(schedSecret{secret: []byte("secret")}, DefaultExploreCounters)
	key := newExploreKey(domaintest.ID("dst"), schedDType)

	const goroutines = 16
	const perGoroutine = 32
	results := make(chan uint64, goroutines*perGoroutine)
	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < perGoroutine; j++ {
				results <- rotator.next(key)
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(results)

	seen := make(map[uint64]struct{}, goroutines*perGoroutine)
	for value := range results {
		if _, duplicate := seen[value]; duplicate {
			t.Fatalf("two concurrent sends received the same counter %d", value)
		}
		seen[value] = struct{}{}
	}
}

// TestExploreKeyIncludesTheDType pins the second half of the key: two frames
// to one destination carrying different types have different candidate sets
// and different K, so they must not share a counter.
func TestExploreKeyIncludesTheDType(t *testing.T) {
	t.Parallel()

	dst := domaintest.ID("dst")
	if newExploreKey(dst, schedDType) == newExploreKey(dst, "some_other_type") {
		t.Fatal("the dtype must be part of the rotation key")
	}
	// And the same dtype for two destinations is two keys.
	if newExploreKey(dst, schedDType) == newExploreKey(domaintest.ID("other-dst"), schedDType) {
		t.Fatal("the destination must be part of the rotation key")
	}
}

// TestAvoidNextHopShrinksTheRotationSet is the exact observable §4.3 names for
// "the exclusion is applied BEFORE direct-first": the rotation index is taken
// modulo K, and K must be the size of the set the send may actually use.
//
// With the exclusion applied as a filter BEHIND the selection, K still counted
// the excluded hop — so consecutive retries of the same key could land on the
// same remaining candidate twice while the rotation believed it had moved on.
// Applying it inside the selection makes the two remaining candidates alternate
// deterministically, which is the whole promise of explore for that key.
func TestAvoidNextHopShrinksTheRotationSet(t *testing.T) {
	t.Parallel()

	relayA := domaintest.ID("relay-a")
	relayB := domaintest.ID("relay-b")
	relayC := domaintest.ID("relay-c")
	fixture, dst := exploreFixture(t, schedFixtureOpts{}, relayA, relayB, relayC)

	hops := make([]domain.PeerIdentity, 0, 4)
	for i := 0; i < 4; i++ {
		fixture.sender.reset()
		outcome := fixture.sendAvoiding(t, dst, AvoidNextHop(relayA), withExplore)
		hop, queued := outcome.NextHop()
		if !queued {
			t.Fatalf("send %d outcome = %s, want queued", i, outcome)
		}
		if hop == relayA {
			t.Fatalf("send %d went to the excluded hop", i)
		}
		hops = append(hops, hop)
	}

	// K is 2, so consecutive sends must alternate between the survivors.
	for i := 1; i < len(hops); i++ {
		if hops[i] == hops[i-1] {
			t.Fatalf("sends %d and %d both chose %s: the rotation set still counts the excluded hop",
				i-1, i, hops[i])
		}
	}
	seen := map[domain.PeerIdentity]bool{}
	for _, hop := range hops {
		seen[hop] = true
	}
	if len(seen) != 2 || !seen[relayB] || !seen[relayC] {
		t.Fatalf("the rotation covered %v, want exactly relay-b and relay-c", seen)
	}
}
