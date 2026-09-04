package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// first_hop_guards_test.go covers the guard policy of docs/protocol/presence.md
// §4.2. Each test names the failure it prevents, because a rule about first hops
// reads as arbitrary caution once the measurement behind it is forgotten — and
// the measurement is the whole argument: rotation does not change the risk per
// choice, it multiplies the number of choices.

// guardTestClock is a hand-driven clock. The policy stores wall-clock dates
// because the set is durable, so a test that used the real clock could not
// reach a back-off expiry without sleeping.
type guardTestClock struct{ at time.Time }

func (c *guardTestClock) now() time.Time          { return c.at }
func (c *guardTestClock) advance(d time.Duration) { c.at = c.at.Add(d) }

// recordingGuardPersister stands in for the durable store.
type recordingGuardPersister struct {
	saves   int
	entries []guardEntry
}

func (p *recordingGuardPersister) Persist(entries []guardEntry) error {
	p.saves++
	p.entries = append([]guardEntry(nil), entries...)
	return nil
}

// newGuardTestSet builds a set with a deterministic clock and a HOSTILE date
// fuzz: every stored date is pushed back by the maximum, so any ordering that
// leans on a timestamp comes out reversed.
//
// An earlier version disabled the fuzz instead, and that hid a real bug for a
// whole round. The set ranked confirmed guards by ConfirmedAt, which production
// fuzzes by up to six days — so a spare confirmed a minute after the primary
// sorted EARLIER about half the time, and the primary never returned to the
// front after its back-off. With fuzz set to zero the tests saw an order that
// no running node would ever have.
//
// The rule this encodes: a test may pin randomness, but pinning it to the value
// that makes the code look right is not a test.
func newGuardTestSet(t *testing.T, seed ...guardEntry) (*firstHopGuards, *guardTestClock, *recordingGuardPersister) {
	t.Helper()
	clock := &guardTestClock{at: time.Unix(1780000000, 0).UTC()}
	persister := &recordingGuardPersister{}
	guards := newFirstHopGuards(clock.now, persister, seed)
	// A GROWING offset, not a constant one: a constant shifts every stored
	// date by the same amount and so preserves their order, which is exactly
	// the property a fuzzed date does not have in production. Each call pushes
	// its date further back than the last, so anything ranked by a timestamp
	// comes out in reverse.
	var call int
	guards.fuzz = func(d time.Duration) time.Duration {
		if d <= 0 {
			return 0
		}
		call++
		offset := time.Duration(call) * (d / 8)
		if offset >= d {
			offset = d - time.Nanosecond
		}
		return offset
	}
	return guards, clock, persister
}

func guardLive(identity domain.PeerIdentity, inbound bool, connectedAt time.Time) guardCandidate {
	return guardCandidate{Identity: identity, Inbound: inbound, ConnectedAt: connectedAt}
}

// TestGuardsStayTheSameAcrossManyProbes is THE property of this subsystem.
//
// Every new first hop is an independent draw against an adversary holding k/N
// of the network: the per-draw risk is fixed, and the cumulative probability of
// at least one bad draw is 1 − (1 − F)^C, which goes to one with C. A probe
// cadence produces dozens of sends an hour, so a set that re-chose per send
// would reach that limit in days.
func TestGuardsStayTheSameAcrossManyProbes(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := []guardCandidate{
		guardLive(domaintest.ID("n1"), false, clock.now().Add(-time.Hour)),
		guardLive(domaintest.ID("n2"), false, clock.now().Add(-2*time.Hour)),
		guardLive(domaintest.ID("n3"), false, clock.now().Add(-3*time.Hour)),
		guardLive(domaintest.ID("n4"), false, clock.now().Add(-4*time.Hour)),
		guardLive(domaintest.ID("n5"), false, clock.now().Add(-5*time.Hour)),
	}

	first := guards.Pick(live)
	if len(first) == 0 {
		t.Fatal("no guard was chosen at all: every send would fall back to the ranking")
	}
	guards.NoteUsed(first[0], len(live))

	for i := 0; i < 50; i++ {
		clock.advance(150 * time.Second)
		again := guards.Pick(live)
		if len(again) == 0 || again[0] != first[0] {
			t.Fatalf("probe %d chose %v, want the same first hop %s — a new choice per "+
				"attempt is the coin flip the guard model exists to stop", i, again, first[0])
		}
		guards.NoteUsed(again[0], len(live))
	}
}

// TestGuardsPreferAnInboundNeighbour: on a session THEY dialled, our identity
// was never proven to them; on one we dialled, it was. So when nothing else
// separates two neighbours, the one that called us is the cheaper confidant.
func TestGuardsPreferAnInboundNeighbour(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	dialled := domaintest.ID("we-dialled-them")
	received := domaintest.ID("they-dialled-us")

	// The outbound one has the OLDER connection, which is the tie-break that
	// would otherwise win: direction has to outrank it, or the preference is
	// decorative.
	chosen := guards.Pick([]guardCandidate{
		guardLive(dialled, false, clock.now().Add(-10*time.Hour)),
		guardLive(received, true, clock.now().Add(-time.Minute)),
	})
	if len(chosen) == 0 || chosen[0] != received {
		t.Fatalf("first hop %v, want the inbound neighbour %s: our identity is "+
			"proven to a neighbour we dialled and not to one that dialled us", chosen, received)
	}
}

// TestABriefFailureDoesNotChangeTheSet is Tor's third fix, imported whole:
// clients dropped a guard on ordinary network noise and thereby rotated exactly
// as much as if they had rotated on purpose. Reconnects are normal life here,
// so a failure arms a wait and never a re-choice.
func TestABriefFailureDoesNotChangeTheSet(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	primary := domaintest.ID("primary")
	spare := domaintest.ID("spare")
	live := []guardCandidate{
		guardLive(primary, true, clock.now().Add(-time.Hour)),
		guardLive(spare, true, clock.now().Add(-time.Minute)),
	}

	chosen := guards.Pick(live)
	if len(chosen) == 0 {
		t.Fatal("no guard chosen")
	}
	guards.NoteUsed(chosen[0], len(live))
	before := guards.Entries()

	guards.NoteUnusable(chosen[0], true)

	after := guards.Entries()
	if len(after) != len(before) {
		t.Fatalf("the set changed size on one failure: %d entries, was %d", len(after), len(before))
	}
	var stillThere bool
	for _, entry := range after {
		if entry.Identity != chosen[0] {
			continue
		}
		stillThere = true
		if entry.ConfirmedAt.IsZero() {
			t.Fatal("the failed guard lost its confirmation: a failure must not undo " +
				"the fact that traffic already crossed it")
		}
	}
	if !stillThere {
		t.Fatal("a single failure removed the guard from the set — that is a rotation, " +
			"which is what this policy exists to prevent")
	}

	// It is skipped while in back-off, and comes back afterwards. Skipping is
	// not removal: the spare carries the traffic meanwhile and does not
	// inherit the slot.
	if chosen := guards.Pick(live); len(chosen) == 0 || chosen[0] == before[0].Identity {
		t.Fatalf("a guard inside its back-off was offered anyway: %v", chosen)
	}
	clock.advance(guardRetryLadder[0] + time.Second)
	if chosen := guards.Pick(live); len(chosen) == 0 || chosen[0] != before[0].Identity {
		t.Fatalf("after the back-off the original guard did not come back: %v", chosen)
	}
}

// TestOurOwnOutageBlamesNoGuard: with our network down every neighbour looks
// dead, and blaming them one by one is how a client rebuilds its whole set out
// of a single local outage. Tor gave this its own interval
// (INTERNET_LIKELY_DOWN_INTERVAL) for exactly that reason.
func TestOurOwnOutageBlamesNoGuard(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := []guardCandidate{guardLive(domaintest.ID("only"), true, clock.now())}
	chosen := guards.Pick(live)
	if len(chosen) == 0 {
		t.Fatal("no guard chosen")
	}
	guards.NoteUsed(chosen[0], len(live))

	guards.NoteUnusable(chosen[0], false)

	for _, entry := range guards.Entries() {
		if entry.Identity != chosen[0] {
			continue
		}
		if entry.Failures != 0 || !entry.RetryAt.IsZero() {
			t.Fatal("a guard was penalised for OUR connectivity being down: the guards " +
				"told us nothing about themselves and the set must not move")
		}
	}
	if again := guards.Pick(live); len(again) == 0 || again[0] != chosen[0] {
		t.Fatalf("the guard was skipped after our own outage: %v", again)
	}
}

// TestAGuardIsConfirmedByUseNotByAiming: "keeps us from committing to a guard
// before we actually use it for sensitive traffic". A candidate walk stops at
// the first hop that accepts, so a preferred guard can be passed over — and a
// guard that never carried anything has told nobody anything about us.
func TestAGuardIsConfirmedByUseNotByAiming(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := []guardCandidate{
		guardLive(domaintest.ID("a"), true, clock.now().Add(-time.Hour)),
		guardLive(domaintest.ID("b"), true, clock.now().Add(-time.Minute)),
	}

	chosen := guards.Pick(live)
	if len(chosen) < 2 {
		t.Fatalf("expected at least two guards, got %v", chosen)
	}
	for _, entry := range guards.Entries() {
		if !entry.ConfirmedAt.IsZero() {
			t.Fatal("a guard was confirmed by being chosen: nothing has crossed it yet")
		}
	}

	// The frame really went through the SECOND one.
	guards.NoteUsed(chosen[1], len(live))
	confirmed := 0
	for _, entry := range guards.Entries() {
		if !entry.ConfirmedAt.IsZero() {
			confirmed++
			if entry.Identity != chosen[1] {
				t.Fatalf("confirmed %s, want the hop the frame actually left through %s",
					entry.Identity, chosen[1])
			}
		}
	}
	if confirmed != 1 {
		t.Fatalf("confirmed %d guards, want exactly the one that carried the frame", confirmed)
	}

	// And confirmation reorders the set: the guard that has already carried
	// traffic goes first from now on, which is what makes the choice stick.
	if again := guards.Pick(live); len(again) == 0 || again[0] != chosen[1] {
		t.Fatalf("after confirmation the hot set starts with %v, want %s", again, chosen[1])
	}
}

// TestTheSetIsCappedByCountAndByShare: the cap is the real bound on how many
// neighbours ever learn anything about us. Without it a busy node would sample
// its way through the whole neighbourhood, which is rotation by another route.
func TestTheSetIsCappedByCountAndByShare(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := make([]guardCandidate, 0, 200)
	for i := 0; i < 200; i++ {
		live = append(live, guardLive(domaintest.ID(string(rune('a'+i%26))+string(rune('a'+i/26))), true, clock.now()))
	}

	// Force repeated top-ups by never confirming anything and stepping past
	// each back-off: this is the shape a node with a hostile neighbourhood
	// would take, and it must not widen the set.
	for i := 0; i < 50; i++ {
		chosen := guards.Pick(live)
		for _, guard := range chosen {
			guards.NoteUnusable(guard, true)
		}
		clock.advance(2 * time.Hour)
	}
	if got := len(guards.Entries()); got > maxSampledGuards {
		t.Fatalf("sampled set grew to %d, cap is %d: the count cap is the bound on how "+
			"many neighbours ever learn we are asking about somebody", got, maxSampledGuards)
	}
}

// TestASmallNeighbourhoodStillGetsGuards: the fractional cap is 20 %, and on a
// node with three neighbours that rounds to zero. A zero cap would mean no
// guards at all — the policy silently switching itself off exactly where the
// neighbourhood is smallest and each hop therefore learns the most.
func TestASmallNeighbourhoodStillGetsGuards(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := []guardCandidate{
		guardLive(domaintest.ID("x"), true, clock.now()),
		guardLive(domaintest.ID("y"), true, clock.now()),
	}
	if chosen := guards.Pick(live); len(chosen) == 0 {
		t.Fatal("a two-neighbour node got no guards: 20% of two is zero, and the " +
			"floor at PRIMARY is what stops the policy from disabling itself")
	}
}

// TestSampledDatesAreRandomised: the moment of a rotation is itself metadata,
// and it correlates across nodes when everybody stamps a round number. Tor
// fuzzes every stored date by RAND(now, LIFETIME/10) for this.
func TestSampledDatesAreRandomised(t *testing.T) {
	clock := &guardTestClock{at: time.Unix(1780000000, 0).UTC()}
	guards := newFirstHopGuards(clock.now, &recordingGuardPersister{}, nil)

	guards.Pick([]guardCandidate{guardLive(domaintest.ID("z"), true, clock.now())})

	entries := guards.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected one sampled entry, got %d", len(entries))
	}
	offset := clock.now().Sub(entries[0].SampledAt)
	if offset < 0 || offset >= guardDateFuzz {
		t.Fatalf("sampled at an offset of %v from now, want a fuzz inside [0, %v)",
			offset, guardDateFuzz)
	}
}

// TestTheSetSurvivesARestart: a set rebuilt on every launch is a rotation once
// per launch. The seed is the whole reason the store exists.
func TestTheSetSurvivesARestart(t *testing.T) {
	guards, clock, persister := newGuardTestSet(t)
	live := []guardCandidate{
		guardLive(domaintest.ID("kept"), true, clock.now().Add(-time.Hour)),
		guardLive(domaintest.ID("other"), true, clock.now()),
	}
	chosen := guards.Pick(live)
	if len(chosen) == 0 {
		t.Fatal("no guard chosen")
	}
	guards.NoteUsed(chosen[0], len(live))
	if persister.saves == 0 {
		t.Fatal("nothing was persisted: the set would be re-sampled on the next start")
	}

	// Restart: a new set seeded from what reached the store.
	restarted, _, _ := newGuardTestSet(t, persister.entries...)
	if again := restarted.Pick(live); len(again) == 0 || again[0] != chosen[0] {
		t.Fatalf("after a restart the first hop is %v, want the confirmed guard %s",
			again, chosen[0])
	}
}

// TestAConfirmedSpareDoesNotStealTheSlot is the regression for the ordering
// bug that the disabled fuzz hid.
//
// The scene is ordinary: the primary hiccups, the spare carries traffic while
// it is in back-off and gets confirmed a minute later, then the primary comes
// back. Ranking by ConfirmedAt made the winner a coin toss — production fuzzes
// that date by up to six days, so the LATER confirmation lands earlier about
// half the time and the original primary never returns to the front. That is a
// rotation produced by the anti-correlation measure itself, which is the exact
// failure the whole file exists to prevent.
//
// The fixture's fuzz is maximal, so an implementation that sorts on the date
// fails here every run rather than one run in two.
func TestAConfirmedSpareDoesNotStealTheSlot(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	primary := domaintest.ID("primary")
	spare := domaintest.ID("spare")
	live := []guardCandidate{
		guardLive(primary, true, clock.now().Add(-time.Hour)),
		guardLive(spare, true, clock.now().Add(-time.Minute)),
	}

	chosen := guards.Pick(live)
	if len(chosen) < 2 {
		t.Fatalf("expected two guards, got %v", chosen)
	}
	leader, reserve := chosen[0], chosen[1]
	guards.NoteUsed(leader, len(live))

	// The leader hiccups and the reserve carries the next probe.
	guards.NoteUnusable(leader, true)
	clock.advance(time.Minute)
	guards.NoteUsed(reserve, len(live))

	// The hiccup is over.
	clock.advance(guardRetryLadder[0] + time.Second)
	again := guards.Pick(live)
	if len(again) == 0 || again[0] != leader {
		t.Fatalf("after the back-off the leading hop is %v, want the original %s: a "+
			"guard confirmed later must not take the slot, or a brief hiccup "+
			"becomes a permanent rotation", again, leader)
	}
}

// TestAFallbackHopIsCounted: the cap claims to bound how many neighbours ever
// learn we are asking about somebody. A neighbour that carried a frame has
// learned exactly that, so leaving it out of the set made the stored set an
// understatement and the cap a number about bookkeeping rather than exposure.
//
// It happens whenever no guard has a route to the destination: the preference
// is not a filter, so the ranking picks somebody else and the frame goes.
func TestAFallbackHopIsCounted(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := []guardCandidate{guardLive(domaintest.ID("sampled"), true, clock.now())}
	guards.Pick(live)

	outsider := domaintest.ID("carried-it-anyway")
	guards.NoteUsed(outsider, len(live)+1)

	var found bool
	for _, entry := range guards.Entries() {
		if entry.Identity == outsider {
			found = true
			if entry.ConfirmedSeq == 0 {
				t.Fatal("the fallback hop was admitted unconfirmed: it carried a frame, " +
					"which is the definition of confirmed here")
			}
		}
	}
	if !found {
		t.Fatal("a neighbour that carried one of our frames is absent from the set: " +
			"the cap counts bookkeeping rather than how many neighbours have seen us")
	}
	if got := guards.Stats().OutsideSetUses; got != 1 {
		t.Fatalf("OutsideSetUses = %d, want 1: the policy's own miss rate has to be "+
			"visible, or a node where every probe leaves through a non-guard looks "+
			"exactly like one where the policy works", got)
	}
}

// TestPrimaryChangesAreCounted: the promise "the first hop stopped changing"
// is statistical and fails silently. Without a counter, a node rotating its
// leading hop every hour is indistinguishable from one that never does.
func TestPrimaryChangesAreCounted(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	first := domaintest.ID("first")
	second := domaintest.ID("second")

	guards.Pick([]guardCandidate{guardLive(first, true, clock.now())})
	if got := guards.Stats().PrimaryChanges; got != 0 {
		t.Fatalf("PrimaryChanges = %d after the FIRST choice, want 0: there was "+
			"nothing to change from", got)
	}
	// The first guard is gone from the neighbourhood entirely.
	guards.Pick([]guardCandidate{guardLive(second, true, clock.now())})
	if got := guards.Stats().PrimaryChanges; got != 1 {
		t.Fatalf("PrimaryChanges = %d, want 1", got)
	}
}

// TestTheSetRidesThePeerStateRows: the set is durable, and its stored form has
// to carry the CONFIRMATION ORDER as well as the dates. Losing the sequence on
// a restart would re-rank the set from fuzzed timestamps — the bug above, one
// process later.
func TestTheSetRidesThePeerStateRows(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	live := []guardCandidate{
		guardLive(domaintest.ID("a"), true, clock.now().Add(-time.Hour)),
		guardLive(domaintest.ID("b"), true, clock.now()),
	}
	chosen := guards.Pick(live)
	if len(chosen) < 2 {
		t.Fatalf("expected two guards, got %v", chosen)
	}
	guards.NoteUsed(chosen[1], len(live))
	guards.NoteUsed(chosen[0], len(live))

	owner := domaintest.ID("this-node")
	restored := firstHopGuardsFromRows(firstHopGuardRows(guards.Entries()), owner.String(), owner)
	if len(restored) != len(guards.Entries()) {
		t.Fatalf("round trip kept %d of %d entries", len(restored), len(guards.Entries()))
	}
	for i, entry := range guards.Entries() {
		if restored[i].Identity != entry.Identity {
			t.Fatalf("entry %d: identity %s, want %s", i, restored[i].Identity, entry.Identity)
		}
		if restored[i].ConfirmedSeq != entry.ConfirmedSeq {
			t.Fatalf("entry %d: confirmation order %d, want %d — a restart would "+
				"re-rank the set from its fuzzed dates",
				i, restored[i].ConfirmedSeq, entry.ConfirmedSeq)
		}
	}

	// And a set seeded from those rows continues the sequence rather than
	// restarting it, which would put the next confirmation ahead of everything
	// already confirmed.
	next, _, _ := newGuardTestSet(t, restored...)
	next.NoteUsed(domaintest.ID("newcomer"), len(live))
	for _, entry := range next.Entries() {
		if entry.Identity != domaintest.ID("newcomer") {
			continue
		}
		if entry.ConfirmedSeq <= 2 {
			t.Fatalf("the newcomer got confirmation order %d, want one past the "+
				"restored maximum: a restart must not renumber the set", entry.ConfirmedSeq)
		}
	}
}

// TestAFallbackHopObeysTheFractionCap: admitting a hop that carried a frame
// must respect BOTH caps, not only the absolute one.
//
// The set promises a bound by count AND by share of the neighbourhood. With ten
// neighbours the ordinary top-up correctly stops at three; if a fallback hop
// were admitted against `maxSampledGuards` alone, successful fallbacks could
// then walk the set up to twelve — more neighbours than the 20 % share the
// public description names, reached through the one door that did not check.
func TestAFallbackHopObeysTheFractionCap(t *testing.T) {
	guards, clock, _ := newGuardTestSet(t)
	const neighbourhood = 10
	live := make([]guardCandidate, 0, neighbourhood)
	for i := 0; i < neighbourhood; i++ {
		live = append(live, guardLive(domaintest.ID(string(rune('a'+i))+"-peer"), true, clock.now()))
	}
	guards.Pick(live)

	admitted := len(guards.Entries())
	for i := 0; i < 20; i++ {
		guards.NoteUsed(domaintest.ID(string(rune('A'+i))+"-outsider"), neighbourhood)
	}

	allowed := int(float64(neighbourhood) * maxSampledFraction)
	if allowed < guardPrimaryCount {
		allowed = guardPrimaryCount
	}
	if got := len(guards.Entries()); got > allowed {
		t.Fatalf("the set grew to %d from %d through fallback hops, but %d neighbours "+
			"allow only %d: the fractional cap is walked around by the one door that "+
			"checked the absolute number alone", got, admitted, neighbourhood, allowed)
	}
	if guards.Stats().OutsideSetUses == 0 {
		t.Fatal("fallback hops that could not be admitted were not counted: the miss " +
			"rate is the only sign that the stated bound is not holding")
	}
}

// TestInspectChangesNothing: a diagnostic that samples guards, moves counters
// and persists is not an observation of the policy — it is a second, unplanned
// caller of it. The first request could report rows it had just created, beside
// an `admitted` count read a moment before creating them, and the observer
// would count its own primary changes.
func TestInspectChangesNothing(t *testing.T) {
	guards, clock, persister := newGuardTestSet(t)
	live := []guardCandidate{
		guardLive(domaintest.ID("a"), true, clock.now()),
		guardLive(domaintest.ID("b"), true, clock.now()),
	}

	before := guards.Entries()
	beforeStats := guards.Stats()
	saves := persister.saves

	for i := 0; i < 3; i++ {
		entries, primary, stats := guards.Inspect(live)
		if len(entries) != len(before) {
			t.Fatalf("Inspect changed the set from %d entries to %d", len(before), len(entries))
		}
		if stats != beforeStats {
			t.Fatalf("Inspect moved the counters: %+v, was %+v", stats, beforeStats)
		}
		if len(primary) != 0 {
			t.Fatalf("Inspect reported %d eligible guards for a set that has none", len(primary))
		}
	}
	if persister.saves != saves {
		t.Fatalf("Inspect persisted %d times: looking at the policy wrote to disk",
			persister.saves-saves)
	}
	if guards.Stats().Admitted != beforeStats.Admitted {
		t.Fatal("Inspect admitted a guard: the diagnostic created the rows it reports")
	}
}

// TestARestoredIdentityDoesNotInheritTheSet: the peer state is scoped by listen
// PORT, and identity_restore swaps the key and restarts on the same one. A set
// restored across that would put the new identity in front of exactly the
// neighbours the old one used, handing them the correlation the guard model is
// supposed to be bounding.
func TestARestoredIdentityDoesNotInheritTheSet(t *testing.T) {
	rows := []firstHopGuardRow{{
		Identity:     domaintest.ID("an-old-guard").String(),
		SampledAt:    time.Unix(1780000000, 0).UTC(),
		ConfirmedSeq: 1,
	}}
	previous := domaintest.ID("the-old-identity")
	restored := domaintest.ID("the-restored-identity")

	if got := firstHopGuardsFromRows(rows, previous.String(), restored); len(got) != 0 {
		t.Fatal("a new identity inherited the previous identity's guard set: those " +
			"neighbours can link the two")
	}
	if got := firstHopGuardsFromRows(rows, previous.String(), previous); len(got) != 1 {
		t.Fatalf("the owning identity did not get its own set back: %d entries", len(got))
	}
	// A file written before the owner field existed cannot be attributed, and
	// unattributable is treated as somebody else's.
	if got := firstHopGuardsFromRows(rows, "", previous); len(got) != 0 {
		t.Fatal("a set with no recorded owner was adopted: whose it is cannot be known, " +
			"and re-sampling costs one round of probes")
	}
}
