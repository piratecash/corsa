package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_baseline_lifecycle_test.go covers the four decisions that used to
// read a retained snapshot and now read a mark: whether a peer needs its first
// full sync, whether a failed first attempt may retry at once, whether an
// established peer is rate-limited, and whether the periodic full still
// reconciles from the live table.
//
// Every one of them is exercised through AnnounceLoop.Run against a real
// table — never by calling the state object and asserting on its own fields.
// A test that sets a flag and then checks the flag proves the author's
// understanding; what has to be proved here is that the LOOP still decides the
// same way, and the only thing the loop's decision is observable through is
// what it puts on the wire.

// baselineHarness is one loop over one table with one announce target, wired to
// a controllable sender and a frozen clock the loop actually reads
// (announceToAllPeers takes its `now` from the registry clock).
type baselineHarness struct {
	table    *routing.Table
	registry *routing.AnnounceStateRegistry
	loop     *routing.AnnounceLoop
	sent     *controllableSender
	peer     routing.PeerIdentity
	now      time.Time
}

// newBaselineHarness seeds a table with `routes` direct peers, so a full sync
// carries a frame whose entry count is known and a delta could not be mistaken
// for it.
//
// The announce target advertises NO capabilities, which puts the periodic
// freshness deadline on its unconditional-full branch: a peer that cannot
// answer a route_sync digest is full-synced rather than heartbeated. That keeps
// these tests about the baseline decision instead of about the digest gate,
// which has its own file.
func newBaselineHarness(t *testing.T, routes int) *baselineHarness {
	t.Helper()

	h := &baselineHarness{
		peer: domaintest.ID("peer-target"),
		now:  time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
	}
	h.registry = routing.NewAnnounceStateRegistry(
		routing.WithRegistryClock(func() time.Time { return h.now }),
	)
	h.table = routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-self")))
	for i := range routes {
		if _, err := h.table.AddDirectPeer(domaintest.ID(string(rune('a'+i)) + "-neighbour")); err != nil {
			t.Fatalf("AddDirectPeer %d: %v", i, err)
		}
	}

	sender, rec := newControllableMockPeerSender(t)
	h.sent = rec
	h.loop = routing.NewAnnounceLoop(h.table, sender,
		func() []routing.AnnounceTarget {
			return []routing.AnnounceTarget{{Address: "addr-target", Identity: h.peer}}
		},
		routing.WithAnnounceInterval(10*time.Second), // no deadline fires on its own
		routing.WithStateRegistry(h.registry),
	)
	return h
}

// runCycle runs exactly one triggered announce cycle and returns.
func (h *baselineHarness) runCycle(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.loop.Run(ctx)
		close(done)
	}()
	h.loop.TriggerUpdate()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done
}

// seedBaseline establishes a baseline for the target peer AT THE CURRENT
// JOURNAL HEAD, which is what makes the tests below able to tell a full sync
// from a delta.
//
// The cursor matters as much as the mark. Committing a baseline at cursor 0 —
// the obvious thing to write — leaves the whole change journal pending, so the
// cursor-mode delta path projects every route in the table and produces a frame
// indistinguishable by size from a full rebuild. A first version of these tests
// did exactly that and survived a mutation that disabled BOTH periodic
// escalations: what it had been watching all along was the delta path standing
// next to the one under test.
func (h *baselineHarness) seedBaseline(t *testing.T, at time.Time) {
	t.Helper()

	raw, head := h.table.AnnounceToWithChangeHead(h.peer)
	h.table.ReleaseAnnounceEntries(raw)
	h.registry.GetOrCreate(h.peer).RecordFullSyncSuccess(head, at)
}

func (h *baselineHarness) hasBaseline(t *testing.T) bool {
	t.Helper()

	state := h.registry.Get(h.peer)
	if state == nil {
		t.Fatal("announce state missing for the target peer")
	}
	return state.View().HasFullSyncBaseline
}

// TestFirstCycleFullSyncsAPeerWithNoBaseline is the initial-sync case: a peer
// the node has never synced gets a self-contained frame carrying the whole
// table, not a delta.
//
// The entry count is the assertion that separates the two. A delta path that
// happened to project the same journal window would also produce one send, and
// counting sends alone would not tell them apart.
func TestFirstCycleFullSyncsAPeerWithNoBaseline(t *testing.T) {
	const routes = 3
	h := newBaselineHarness(t, routes)

	if h.registry.Get(h.peer) != nil && h.hasBaseline(t) {
		t.Fatal("precondition: a fresh peer must have no baseline")
	}

	h.runCycle(t)

	calls := h.sent.getCalls()
	if len(calls) != 1 {
		t.Fatalf("first cycle sent %d frames, want exactly 1 (the initial full sync)", len(calls))
	}
	if got := len(calls[0].Routes); got != routes {
		t.Fatalf("initial frame carried %d entries, want the whole table (%d): a first sync must be self-contained",
			got, routes)
	}
	if !h.hasBaseline(t) {
		t.Fatal("a successful full sync did not establish the baseline")
	}
}

// TestFailedFirstSyncRetriesImmediately is the failed-send case, and the one
// the rate-limit term exists for: a peer that has never received anything must
// not be made to wait out a rate-limit window earned by an attempt that
// delivered nothing.
//
// The two halves matter together. The retry alone would also pass if the rate
// limiter never worked at all, which is why the sibling test below pins that it
// DOES bite once a baseline exists — same setup, one difference.
func TestFailedFirstSyncRetriesImmediately(t *testing.T) {
	const routes = 3
	h := newBaselineHarness(t, routes)
	h.sent.setFailNext(1)

	h.runCycle(t)

	if calls := h.sent.getCalls(); len(calls) != 1 {
		t.Fatalf("failing cycle sent %d frames, want 1 attempt", len(calls))
	}
	if h.hasBaseline(t) {
		t.Fatal("a FAILED send established the baseline: the commit must be on the success path only")
	}

	// Same instant on the clock: only the missing baseline can let this
	// through, because the attempt timestamp the first cycle wrote is well
	// inside the rate-limit window.
	h.runCycle(t)

	calls := h.sent.getCalls()
	if len(calls) != 2 {
		t.Fatalf("retry after a failed first sync sent %d frames in total, want 2: a peer with nothing must retry without delay",
			len(calls))
	}
	if got := len(calls[1].Routes); got != routes {
		t.Fatalf("retry frame carried %d entries, want the whole table (%d)", got, routes)
	}
	if !h.hasBaseline(t) {
		t.Fatal("the successful retry did not establish the baseline")
	}
}

// TestEstablishedPeerIsRateLimitedOnResync is the other half: with a baseline
// in place, a fresh resync demand inside the rate-limit window is coalesced
// away rather than sent.
//
// Without this the test above would pass under a mutation that removed the
// rate limit entirely.
func TestEstablishedPeerIsRateLimitedOnResync(t *testing.T) {
	h := newBaselineHarness(t, 3)

	h.seedBaseline(t, h.now)                                    // baseline established, journal drained
	h.registry.GetOrCreate(h.peer).RecordFullSyncAttempt(h.now) // ...and an attempt just made
	h.registry.MarkInvalid(h.peer)                              // request_resync: needsFull, hard

	h.runCycle(t)

	if calls := h.sent.getCalls(); len(calls) != 0 {
		t.Fatalf("resync inside the rate-limit window sent %d frames, want 0: an established peer must be coalesced",
			len(calls))
	}
	if !h.hasBaseline(t) {
		t.Fatal("a coalesced cycle cleared the baseline")
	}
}

// TestPeriodicFullReconcilesAgainstTheLiveTable is the self-healing case, and
// the one that answers the question this change had to answer before it could
// be made: with the sent snapshot no longer kept, is the periodic full still a
// reconciliation?
//
// It is, because it never read that snapshot — it rebuilds from the routing
// table. The proof is a route added AFTER the baseline appearing in the frame
// the deadline produces: a rebuild from a stored copy could not contain it.
func TestPeriodicFullReconcilesAgainstTheLiveTable(t *testing.T) {
	const seeded = 3
	h := newBaselineHarness(t, seeded)

	// A baseline whose last success is far enough back that the freshness
	// deadline is due on the next cycle. The cadence here is
	// min(10 × 10s, TTL/2) = 100s; an hour is unambiguously past it.
	h.seedBaseline(t, h.now.Add(-time.Hour))

	// A route the baseline never covered. It is the ONLY thing the cursor
	// window now holds, so a delta would carry exactly one entry and a
	// rebuild from the live table carries all of them — which is what the
	// count below discriminates.
	if _, err := h.table.AddDirectPeer(domaintest.ID("late-neighbour")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	h.runCycle(t)

	calls := h.sent.getCalls()
	if len(calls) == 0 {
		t.Fatal("the periodic deadline produced no frame: the peer would age out of the neighbour's table")
	}
	full := calls[len(calls)-1]
	if got := len(full.Routes); got != seeded+1 {
		t.Fatalf("periodic full carried %d entries, want %d: it must rebuild from the live table, including routes learned after the baseline",
			got, seeded+1)
	}

	var sawLate bool
	for _, entry := range full.Routes {
		if entry.Identity == domaintest.ID("late-neighbour") {
			sawLate = true
		}
	}
	if !sawLate {
		t.Fatal("the route added after the baseline is missing from the periodic full: the rebuild is not reading the live table")
	}
}
