package node

// first_hop_guard_target_exclusion_test.go is scenario S24а of
// docs/refactoring/dht/21-anonymity-transport.md §4.3.4″.7 — the guard half of
// the H₁ = B rule, in the form that can be run today.
//
// # The rule being modelled
//
// Owner decision §0″.2: when the target of a request is itself a member of the
// pinned first-hop set, the target is excluded FROM THE CHOICE FOR THAT
// REQUEST, and the pinned set is neither changed nor topped up because of it.
// Another eligible member of the EXISTING set is used; if there is none, the
// anonymous mode refuses. Stepping outside the set is forbidden.
//
// # Why this is a model and not the real thing
//
// Nothing in production knows about a target when it picks a first hop:
// Service.preferredFirstHops calls Pick(firstHopGuardCandidates()) and never
// sees the destination, and Pick itself works from the live neighbourhood. So
// the exclusion does not exist yet, and where it will be wired in is exactly
// what decides whether the rule holds. The adapters below are TEST-ONLY models
// of two wirings — one wrong, one right — around the UNCHANGED Pick.
//
// Production code, network behaviour and Pick are not touched by this file.
//
// ⚠️ What this does NOT establish. That the path an implementation actually
// builds obeys the rule. That is S24б, on actual paths, after 06/08/21b — see
// §4.3.4″.7 stage 2. O5 is open and G2 is not closed.

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// guardEligibility models everything the send path would check about a
// candidate first hop BESIDES its membership in the set: the ¬Q half, transit
// capability, a proven identity, and "not the target of this request".
//
// It is one predicate because S24а is about WHERE the check happens, not about
// what it consists of: any of those reasons leaves the same hole if the
// exclusion is applied to the wrong input.
type guardEligibility func(domain.PeerIdentity) bool

// excludeOnly builds the eligibility of the simplest case: everyone is fine
// except the target of this request.
func excludeOnly(target domain.PeerIdentity) guardEligibility {
	return func(id domain.PeerIdentity) bool { return id != target }
}

// excludeAll builds the eligibility of the interesting case: several members of
// the set are unusable for this request, the target among them.
func excludeAll(unusable ...domain.PeerIdentity) guardEligibility {
	blocked := make(map[domain.PeerIdentity]struct{}, len(unusable))
	for _, id := range unusable {
		blocked[id] = struct{}{}
	}
	return func(id domain.PeerIdentity) bool {
		_, bad := blocked[id]
		return !bad
	}
}

// --- the three wirings ------------------------------------------------------

// selectWithExclusionBeforePick is the WRONG adapter, kept as the negative
// control: it removes the ineligible peers from the live neighbourhood and only
// then calls Pick.
//
// This is the mistake that looks harmless. The live set is what Pick counts to
// decide whether the set is short, and what roomLocked measures the fractional
// cap against — so removing the target from it makes the set top up BECAUSE OF
// THE TARGET, which is precisely what §0″.2 forbids. The role scheme cannot see
// this at all: the halves are still disjoint, the chosen hop is still eligible,
// and the set has simply grown.
func selectWithExclusionBeforePick(
	guards *firstHopGuards,
	live []guardCandidate,
	eligible guardEligibility,
) []domain.PeerIdentity {
	filtered := make([]guardCandidate, 0, len(live))
	for _, candidate := range live {
		if eligible(candidate.Identity) {
			filtered = append(filtered, candidate)
		}
	}
	return guards.Pick(filtered)
}

// selectWithExclusionAfterPick is the CORRECT wiring as far as the set is
// concerned: Pick sees the whole live neighbourhood, and the exclusion is
// applied to what it returned.
//
// It is correct about the set and incomplete about the rule — see
// selectFromPinnedSetAfterPick and the third test.
func selectWithExclusionAfterPick(
	guards *firstHopGuards,
	live []guardCandidate,
	eligible guardEligibility,
) []domain.PeerIdentity {
	preferred := guards.Pick(live)

	out := make([]domain.PeerIdentity, 0, len(preferred))
	for _, id := range preferred {
		if eligible(id) {
			out = append(out, id)
		}
	}
	return out
}

// selectFromPinnedSetAfterPick is the wiring §0″.2 actually describes: take
// another eligible member of the EXISTING set.
//
// Pick still runs on the full live neighbourhood, so the set keeps its own
// lifecycle untouched; the choice, however, is made over the whole pinned set
// rather than over the three entries Pick hands back. primaryLocked truncates
// its answer to guardPrimaryCount BEFORE any of the request's own filters are
// applied, so a set of four with three unusable members would otherwise look
// empty — a refusal while an eligible member is sitting right there.
//
// Order is the set's own: the hot list first, in Pick's order, then the rest in
// stored order. Nothing here reorders the set, and nothing samples.
//
// ⚠️ The request's filter is applied ON TOP of the set's policy, never INSTEAD
// of it. Walking Entries() to reach a member the truncation hid is only allowed
// to recover entries Pick left out FOR THAT REASON — an entry Pick skipped
// because it is in back-off must stay skipped. An earlier revision of this
// adapter checked only liveness and the request's own predicate, and so handed
// back guards the policy had temporarily suspended: a rule about which member
// to use had quietly become a way around rule 3.
func selectFromPinnedSetAfterPick(
	guards *firstHopGuards,
	live []guardCandidate,
	eligible guardEligibility,
) []domain.PeerIdentity {
	now := guards.clock()

	preferred := guards.Pick(live)
	liveByID := make(map[domain.PeerIdentity]struct{}, len(live))
	for _, candidate := range live {
		liveByID[candidate.Identity] = struct{}{}
	}

	out := make([]domain.PeerIdentity, 0, len(preferred))
	seen := make(map[domain.PeerIdentity]struct{}, len(preferred))
	for _, id := range preferred {
		seen[id] = struct{}{}
		if eligible(id) {
			out = append(out, id)
		}
	}
	for _, entry := range guards.Entries() {
		if _, already := seen[entry.Identity]; already {
			continue
		}
		if _, isLive := liveByID[entry.Identity]; !isLive {
			continue
		}
		// The set's own back-off, the same test primaryLocked applies. Without
		// it this loop re-admits exactly what Pick refused.
		if !entry.RetryAt.IsZero() && now.Before(entry.RetryAt) {
			continue
		}
		if eligible(entry.Identity) {
			out = append(out, entry.Identity)
		}
	}
	return out
}

// --- fixture ----------------------------------------------------------------

// pinnedGuard builds a seeded entry: confirmed, in the set, not in back-off.
//
// Confirmed with an explicit sequence number because that is what orders the
// hot list; the stored DATES are fuzzed by the fixture on purpose (see
// newGuardTestSet) and ordering by them is ordering by a random number.
func pinnedGuard(id domain.PeerIdentity, seq uint64, sampledAt time.Time) guardEntry {
	return guardEntry{
		Identity:     id,
		SampledAt:    sampledAt,
		ConfirmedAt:  sampledAt,
		ConfirmedSeq: seq,
	}
}

// guardExclusionFixture is a pinned set plus a live neighbourhood, with every
// input fixed and the clock hand-driven.
type guardExclusionFixture struct {
	guards *firstHopGuards
	clock  *guardTestClock
	pinned []domain.PeerIdentity
	live   []guardCandidate
}

// newGuardExclusionFixture seeds `pinnedCount` guards and offers them plus
// `freshCount` never-sampled neighbours as the live set.
//
// `freshCount` is not decoration: roomLocked measures the fractional cap
// against the length of what Pick is handed, so the neighbourhood has to be
// large enough for a top-up to be POSSIBLE. If it were not, the negative
// control would pass for the wrong reason — no growth because there was no room
// rather than because the wiring was right.
func newGuardExclusionFixture(t *testing.T, pinnedCount, freshCount int) *guardExclusionFixture {
	t.Helper()

	seedClock := &guardTestClock{at: time.Unix(1780000000, 0).UTC()}
	sampledAt := seedClock.now().Add(-time.Hour)

	pinned := make([]domain.PeerIdentity, 0, pinnedCount)
	seed := make([]guardEntry, 0, pinnedCount)
	for i := range pinnedCount {
		id := domaintest.ID(guardName("pinned", i))
		pinned = append(pinned, id)
		seed = append(seed, pinnedGuard(id, uint64(i+1), sampledAt))
	}

	guards, clock, _ := newGuardTestSet(t, seed...)

	live := make([]guardCandidate, 0, pinnedCount+freshCount)
	for i, id := range pinned {
		live = append(live, guardLive(id, false, clock.now().Add(-time.Duration(i+1)*time.Hour)))
	}
	for i := range freshCount {
		live = append(live, guardLive(domaintest.ID(guardName("fresh", i)), false,
			clock.now().Add(-time.Duration(i+1)*time.Minute)))
	}

	return &guardExclusionFixture{guards: guards, clock: clock, pinned: pinned, live: live}
}

func guardName(prefix string, index int) string {
	return prefix + "-" + string(rune('a'+index%26)) + string(rune('a'+index/26))
}

func (f *guardExclusionFixture) entryCount(t *testing.T) int {
	t.Helper()
	return len(f.guards.Entries())
}

// backOff puts a guard into the policy's own back-off, through the production
// path rather than by writing the field: NoteUnusable is what a failed send
// calls, and a hand-set RetryAt would be a fixture agreeing with itself.
func (f *guardExclusionFixture) backOff(t *testing.T, id domain.PeerIdentity) {
	t.Helper()

	f.guards.NoteUnusable(id, true)

	for _, entry := range f.guards.Entries() {
		if entry.Identity != id {
			continue
		}
		if entry.RetryAt.IsZero() || !f.clock.now().Before(entry.RetryAt) {
			t.Fatalf("guard %s is not in back-off after NoteUnusable: RetryAt=%v now=%v",
				id, entry.RetryAt, f.clock.now())
		}
		return
	}
	t.Fatalf("guard %s is not in the set", id)
}

// stats reads the policy's counters WITHOUT changing anything: Inspect is the
// read-only view, unlike Pick which may sample.
func (f *guardExclusionFixture) stats(t *testing.T) guardStats {
	t.Helper()
	_, _, stats := f.guards.Inspect(f.live)
	return stats
}

func (f *guardExclusionFixture) pinnedSet(t *testing.T) map[domain.PeerIdentity]struct{} {
	t.Helper()
	out := make(map[domain.PeerIdentity]struct{}, len(f.pinned))
	for _, id := range f.pinned {
		out[id] = struct{}{}
	}
	return out
}

// --- case 1: the negative control -------------------------------------------

// TestS24aExcludingTheTargetBeforePickGrowsTheSet is the NEGATIVE CONTROL, and
// it PASSES: it demonstrates that the wrong wiring produces the forbidden
// effect and that this fixture notices.
//
// Without it the two tests below would also be satisfied by a fixture that
// could not tell the wirings apart — a test that cannot fail proves nothing
// about the rule it is named after.
//
// The mechanism, spelled out because it is invisible from the rule alone:
// dropping the target from the live set lowers liveEligibleCountLocked below
// guardPrimaryCount, topUpLocked then admits a fresh neighbour, and the pinned
// set has grown for one reason only — who this request was addressed to.
func TestS24aExcludingTheTargetBeforePickGrowsTheSet(t *testing.T) {
	fixture := newGuardExclusionFixture(t, guardPrimaryCount, 18)
	target := fixture.pinned[0]

	before := fixture.entryCount(t)
	if before != guardPrimaryCount {
		t.Fatalf("fixture: seeded %d guards, want %d", before, guardPrimaryCount)
	}

	chosen := selectWithExclusionBeforePick(fixture.guards, fixture.live, excludeOnly(target))

	after := fixture.entryCount(t)
	if after <= before {
		t.Fatalf("negative control is inert: the set did not grow (%d → %d), so this fixture "+
			"cannot distinguish the wrong wiring from the right one", before, after)
	}

	// Name the growth precisely: a neighbour that was NOT pinned is now in the
	// set, and it got there because of the target.
	pinned := fixture.pinnedSet(t)
	admitted := make([]domain.PeerIdentity, 0, after-before)
	for _, entry := range fixture.guards.Entries() {
		if _, wasPinned := pinned[entry.Identity]; !wasPinned {
			admitted = append(admitted, entry.Identity)
		}
	}
	if len(admitted) == 0 {
		t.Fatal("the set grew but no unpinned neighbour appeared: the growth is not the one this " +
			"control is about")
	}

	for _, id := range chosen {
		if id == target {
			t.Errorf("the target %s was still selected as a first hop", id)
		}
	}
}

// --- case 2: the correct order ----------------------------------------------

// TestS24aExcludingTheTargetAfterPickLeavesTheSetAlone is the rule of §0″.2 on
// the same inputs: Pick sees the whole live neighbourhood, the exclusion is
// applied afterwards, and the pinned set is untouched.
//
// Same fixture as the negative control on purpose. The only difference between
// the two tests is WHERE the exclusion is applied, which is the whole claim.
func TestS24aExcludingTheTargetAfterPickLeavesTheSetAlone(t *testing.T) {
	fixture := newGuardExclusionFixture(t, guardPrimaryCount, 18)
	target := fixture.pinned[0]

	before := fixture.guards.Entries()
	beforeStats := fixture.stats(t)

	chosen := selectWithExclusionAfterPick(fixture.guards, fixture.live, excludeOnly(target))

	after := fixture.guards.Entries()
	if len(after) != len(before) {
		t.Fatalf("the set changed size because of the target: %d → %d", len(before), len(after))
	}
	for i := range after {
		if after[i].Identity != before[i].Identity {
			t.Fatalf("entry %d changed identity: %s → %s", i, before[i].Identity, after[i].Identity)
		}
	}
	if stats := fixture.stats(t); stats.Admitted != beforeStats.Admitted {
		t.Errorf("a neighbour was admitted: Admitted %d → %d", beforeStats.Admitted, stats.Admitted)
	}

	if len(chosen) == 0 {
		t.Fatal("no first hop was available although two eligible members of the set are live: " +
			"this is the false refusal the rule forbids")
	}
	pinned := fixture.pinnedSet(t)
	for _, id := range chosen {
		if id == target {
			t.Errorf("the target %s was selected as a first hop", id)
		}
		if _, member := pinned[id]; !member {
			t.Errorf("chosen hop %s is not a member of the pinned set: the choice left the set", id)
		}
	}
}

// --- case 3: the fourth member of the set -----------------------------------

// TestS24aFourthPinnedMemberIsUsedWhenTheHotThreeAreNot is the case the first
// two cannot reach: the hot list is not the set.
//
// primaryLocked truncates to guardPrimaryCount BEFORE the request's own filters
// run, so with four pinned guards and three of them unusable for this request,
// a selector that only ever sees Pick's answer refuses — while a perfectly good
// member of the set is live and eligible. §0″.2 says to take another eligible
// member of the EXISTING set, and that is what this pins.
func TestS24aFourthPinnedMemberIsUsedWhenTheHotThreeAreNot(t *testing.T) {
	const pinnedCount = guardPrimaryCount + 1

	// Fresh neighbours are present on purpose: without something outside the
	// set to reach for, "the choice did not leave the set" is an assertion
	// about an impossibility rather than about the wiring.
	fixture := newGuardExclusionFixture(t, pinnedCount, 18)
	target := fixture.pinned[0]
	// The target plus two more members unusable for this request — role, an
	// unproven identity, transit, it does not matter which: what matters is
	// that the hot three are gone and the fourth is not.
	eligible := excludeAll(fixture.pinned[0], fixture.pinned[1], fixture.pinned[2])
	fourth := fixture.pinned[3]

	before := fixture.guards.Entries()
	beforeStats := fixture.stats(t)

	// The negative control of this case: filtering Pick's answer refuses.
	if hotOnly := selectWithExclusionAfterPick(fixture.guards, fixture.live, eligible); len(hotOnly) != 0 {
		t.Fatalf("this case needs the hot list to be exhausted, got %d eligible: the fixture is "+
			"not exercising the truncation", len(hotOnly))
	}

	chosen := selectFromPinnedSetAfterPick(fixture.guards, fixture.live, eligible)

	if len(chosen) != 1 {
		t.Fatalf("want exactly the fourth member selected, got %d candidates: %v", len(chosen), chosen)
	}
	if chosen[0] != fourth {
		t.Errorf("selected %s, want the fourth pinned member %s", chosen[0], fourth)
	}
	if chosen[0] == target {
		t.Errorf("the target %s was selected", target)
	}

	after := fixture.guards.Entries()
	if len(after) != len(before) {
		t.Fatalf("the set changed size while serving one request: %d → %d", len(before), len(after))
	}
	if stats := fixture.stats(t); stats.Admitted != beforeStats.Admitted {
		t.Errorf("a neighbour was admitted: Admitted %d → %d", beforeStats.Admitted, stats.Admitted)
	}
	pinned := fixture.pinnedSet(t)
	if _, member := pinned[chosen[0]]; !member {
		t.Errorf("chosen hop %s is not a member of the pinned set", chosen[0])
	}
}

// TestS24aNoEligibleMemberIsARefusalNotAnEscape is the other end of the same
// rule: when the whole pinned set is unusable for this request, the answer is
// an empty one.
//
// Empty here means the caller refuses — §7.3 — and the point of asserting it is
// what must NOT happen instead: a hop from outside the set, or a top-up that
// finds one. Both would satisfy "a first hop was found".
func TestS24aNoEligibleMemberIsARefusalNotAnEscape(t *testing.T) {
	const pinnedCount = guardPrimaryCount + 1

	// Eighteen eligible neighbours are live and NOT in the set. The refusal
	// has to hold with an escape route in plain sight, otherwise the test says
	// only that an empty set produces an empty answer.
	fixture := newGuardExclusionFixture(t, pinnedCount, 18)
	eligible := excludeAll(fixture.pinned...)

	before := fixture.guards.Entries()
	beforeStats := fixture.stats(t)

	chosen := selectFromPinnedSetAfterPick(fixture.guards, fixture.live, eligible)

	if len(chosen) != 0 {
		pinned := fixture.pinnedSet(t)
		for _, id := range chosen {
			if _, member := pinned[id]; !member {
				t.Errorf("hop %s comes from OUTSIDE the pinned set: the refusal was escaped, "+
					"which §0″.2 forbids even when the set has nothing to offer", id)
			}
		}
		t.Fatalf("a first hop was produced although no member of the set is eligible: %v", chosen)
	}
	after := fixture.guards.Entries()
	if len(after) != len(before) {
		t.Fatalf("the set changed size on a refusal: %d → %d", len(before), len(after))
	}
	if stats := fixture.stats(t); stats.Admitted != beforeStats.Admitted {
		t.Errorf("a neighbour was admitted on a refusal: Admitted %d → %d",
			beforeStats.Admitted, stats.Admitted)
	}
}

// TestS24aAGuardInBackOffIsNotTheFourthMember is the case that separates "take
// another member of the set" from "take any member of the set".
//
// The hot three are unusable for this request and the fourth is in back-off,
// which the set imposed itself after a failed send. Reaching past the hot list
// is allowed to recover what the TRUNCATION hid — never what the policy
// suspended. So the answer is a refusal, and rule 3 of first_hop_guards.go
// survives contact with the H₁ = B rule.
//
// Without this case the adapter could satisfy every other test while quietly
// offering guards the policy had just stood down: the request would succeed,
// through a neighbour the node had decided not to use.
func TestS24aAGuardInBackOffIsNotTheFourthMember(t *testing.T) {
	const pinnedCount = guardPrimaryCount + 1

	fixture := newGuardExclusionFixture(t, pinnedCount, 18)
	eligible := excludeAll(fixture.pinned[0], fixture.pinned[1], fixture.pinned[2])
	fourth := fixture.pinned[3]

	fixture.backOff(t, fourth)

	before := fixture.guards.Entries()
	beforeStats := fixture.stats(t)

	chosen := selectFromPinnedSetAfterPick(fixture.guards, fixture.live, eligible)

	if len(chosen) != 0 {
		for _, id := range chosen {
			if id == fourth {
				t.Errorf("guard %s is in back-off and was offered anyway: the walk over the set "+
					"re-admitted what Pick refused", id)
			}
		}
		t.Fatalf("want a refusal while the only remaining member is in back-off, got %v", chosen)
	}

	after := fixture.guards.Entries()
	if len(after) != len(before) {
		t.Fatalf("the set changed size: %d → %d", len(before), len(after))
	}
	if stats := fixture.stats(t); stats.Admitted != beforeStats.Admitted {
		t.Errorf("a neighbour was admitted rather than refusing: Admitted %d → %d",
			beforeStats.Admitted, stats.Admitted)
	}
}

// TestS24aTheFourthMemberReturnsWhenItsBackOffElapses is the other half of the
// same boundary: the refusal above is TEMPORARY, and nothing about the H₁ = B
// rule made it permanent.
//
// It also pins that the back-off is what did the refusing. If the fourth member
// were being rejected for some other reason, advancing the clock past the retry
// delay would not bring it back.
func TestS24aTheFourthMemberReturnsWhenItsBackOffElapses(t *testing.T) {
	const pinnedCount = guardPrimaryCount + 1

	fixture := newGuardExclusionFixture(t, pinnedCount, 18)
	eligible := excludeAll(fixture.pinned[0], fixture.pinned[1], fixture.pinned[2])
	fourth := fixture.pinned[3]

	fixture.backOff(t, fourth)
	if chosen := selectFromPinnedSetAfterPick(fixture.guards, fixture.live, eligible); len(chosen) != 0 {
		t.Fatalf("fixture: expected a refusal while in back-off, got %v", chosen)
	}

	// Past the first rung of the retry ladder, and no further: the entry
	// becomes offerable again on its own schedule.
	fixture.clock.advance(guardRetryLadder[0] + time.Second)

	before := len(fixture.guards.Entries())
	chosen := selectFromPinnedSetAfterPick(fixture.guards, fixture.live, eligible)

	if len(chosen) != 1 || chosen[0] != fourth {
		t.Fatalf("after the back-off elapsed want the fourth member %s, got %v", fourth, chosen)
	}
	if after := len(fixture.guards.Entries()); after != before {
		t.Errorf("the set changed size while the back-off elapsed: %d → %d", before, after)
	}
}

// TestS24aExclusionDoesNotDependOnTheClock pins the boundary between this rule
// and the set's own lifecycle: nothing here is allowed to work because a guard
// expired or a back-off elapsed.
//
// The clock is not advanced, so retireExpiredLocked cannot fire (guardLifetime
// is sixty days) and no entry is in back-off. If a future change made the
// exclusion depend on either, this test is where it shows: the two selections
// below are separated by nothing at all and must agree.
func TestS24aExclusionDoesNotDependOnTheClock(t *testing.T) {
	fixture := newGuardExclusionFixture(t, guardPrimaryCount, 18)
	target := fixture.pinned[0]

	first := selectWithExclusionAfterPick(fixture.guards, fixture.live, excludeOnly(target))
	entriesAfterFirst := len(fixture.guards.Entries())

	second := selectWithExclusionAfterPick(fixture.guards, fixture.live, excludeOnly(target))

	if len(fixture.guards.Entries()) != entriesAfterFirst {
		t.Fatalf("the set changed between two identical requests: %d → %d",
			entriesAfterFirst, len(fixture.guards.Entries()))
	}
	if len(first) != len(second) {
		t.Fatalf("two identical requests chose differently: %v then %v", first, second)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("two identical requests chose differently at %d: %s then %s",
				i, first[i], second[i])
		}
	}
}
