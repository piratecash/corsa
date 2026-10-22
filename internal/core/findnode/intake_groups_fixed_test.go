package findnode

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// FROZEN accepted old map source, mechanically renamed to distinct test-only
// types/constants. Its operations do NOT call fixed-storage read/index helpers.
const fixedOldNamedGroups = 256
const fixedOldCells = fixedOldNamedGroups + 1
const fixedOldWindow = Instant(10 * time.Minute)

type fixedOldKey struct {
	cell   int
	target domain.PeerIdentity
}
type fixedOldRecord struct {
	expires Instant
	refs    int
}
type fixedOldPlan struct {
	group   domain.SourceGroup
	cell    int
	name    bool
	key     fixedOldKey
	expires Instant
}

// fixedOldGroups belongs exclusively to intakeCoordinator.mu. Overflow never
// retains a physical group name. References are companions of existing rows.
type fixedOldGroups struct {
	named      map[domain.SourceGroup]int
	records    map[fixedOldKey]fixedOldRecord
	generation uint64
	lastUse    Instant
	used       bool
}

func newFixedOldGroups() fixedOldGroups {
	return fixedOldGroups{named: make(map[domain.SourceGroup]int), records: make(map[fixedOldKey]fixedOldRecord), generation: 1}
}
func (g *fixedOldGroups) prospective(group domain.SourceGroup) int {
	if cell, ok := g.named[group]; ok {
		return cell
	}
	if len(g.named) < fixedOldNamedGroups {
		return len(g.named)
	}
	return fixedOldNamedGroups
}
func (g *fixedOldGroups) orderCell(group domain.SourceGroup) int {
	if cell, ok := g.named[group]; ok {
		return cell
	}
	return fixedOldNamedGroups
}
func (g *fixedOldGroups) plan(group domain.SourceGroup, target domain.PeerIdentity, slot Instant, cap int) (fixedOldPlan, error) {
	if group == "" || target.IsZero() || slot < 0 || cap < 512 || cap > 3840 {
		return fixedOldPlan{}, ErrInvalid
	}
	expiry, err := slot.plus(fixedOldWindow)
	if err != nil {
		return fixedOldPlan{}, err
	}
	cell := g.prospective(group)
	key := fixedOldKey{cell, target}
	active := [fixedOldCells]bool{}
	occupancy := 0
	for record := range g.records {
		active[record.cell] = true
		if record.cell == cell {
			occupancy++
		}
	}
	active[cell] = true
	k := 0
	for _, present := range active {
		if present {
			k++
		}
	}
	old, exists := g.records[key]
	if !exists && (occupancy >= max(16, cap/k) || len(g.records) >= cap+16*fixedOldCells) {
		return fixedOldPlan{}, ErrCapacity
	}
	_, named := g.named[group]
	return fixedOldPlan{group: group, cell: cell, name: !named && cell < fixedOldNamedGroups, key: key, expires: max(expiry, old.expires)}, nil
}
func (g *fixedOldGroups) commit(plan fixedOldPlan, now Instant) {
	if plan.name {
		g.named[plan.group] = plan.cell
	}
	record := g.records[plan.key]
	record.expires = plan.expires
	record.refs++
	g.records[plan.key] = record
	g.lastUse = now
	g.used = true
}
func (g *fixedOldGroups) release(key fixedOldKey) {
	record := g.records[key]
	record.refs--
	g.records[key] = record
}
func (g *fixedOldGroups) finish(key fixedOldKey, at Instant) error {
	expiry, err := at.plus(fixedOldWindow)
	if err != nil {
		return err
	}
	record, ok := g.records[key]
	if !ok {
		return ErrStaleWork
	}
	record.expires = max(record.expires, expiry)
	g.records[key] = record
	g.lastUse = at
	g.used = true
	return nil
}
func (g *fixedOldGroups) expire(now Instant) {
	for key, record := range g.records {
		if record.refs == 0 && now >= record.expires {
			delete(g.records, key)
		}
	}
}
func (g *fixedOldGroups) reset(now Instant, rows int) bool {
	if rows != 0 || len(g.records) != 0 || g.generation == math.MaxUint64 {
		return false
	}
	if g.used && (now < g.lastUse || now-g.lastUse < fixedOldWindow) {
		return false
	}
	clear(g.named)
	g.generation++
	g.used = false
	return true
}

func fixedGroupNames(g *candidateGroups) int   { n, _ := g.counts(); return n }
func fixedGroupRecords(g *candidateGroups) int { _, r := g.counts(); return r }
func fixedMustRecord(t *testing.T, g *candidateGroups, key candidateRecordKey) candidateGroupRecord {
	t.Helper()
	r, ok := g.record(key)
	if !ok {
		t.Fatal("missing seeded semantic record", key)
	}
	return r
}
func fixedSeedName(t *testing.T, g *candidateGroups, name domain.SourceGroup, cell int) {
	t.Helper()
	if cell != g.namedCount || cell < 0 || cell >= 256 || name == "" {
		t.Fatal("invalid dense name fixture")
	}
	for i := 0; i < g.namedCount; i++ {
		if g.named[i] == name {
			t.Fatal("duplicate seeded name")
		}
	}
	g.named[cell] = name
	g.namedCount++
}
func fixedSeedRecord(t *testing.T, g *candidateGroups, key candidateRecordKey, record candidateGroupRecord) {
	t.Helper()
	if g.recordCount >= 7952 || key.cell < 0 || key.cell > 256 || key.target.IsZero() {
		t.Fatal("invalid dense record fixture")
	}
	for i := 0; i < g.recordCount; i++ {
		if g.records[i].key == key {
			t.Fatal("duplicate seeded semantic key")
		}
	}
	g.records[g.recordCount] = candidateGroupSlot{key: key, record: record}
	g.recordCount++
}
func fixedCompare(t *testing.T, g *candidateGroups, old *fixedOldGroups) {
	t.Helper()
	n, r := g.counts()
	if n != len(old.named) || r != len(old.records) || n < 0 || n > 256 || r < 0 || r > 7952 {
		t.Fatal("fixed counts differ from frozen map", n, r, len(old.named), len(old.records))
	}
	names := map[domain.SourceGroup]bool{}
	for cell, name := range g.named {
		if cell < n {
			oldCell, present := old.named[name]
			if name == "" || names[name] || !present || oldCell != cell {
				t.Fatal("dense name prefix differs")
			}
			names[name] = true
		} else if name != "" {
			t.Fatal("inactive name tail retained")
		}
	}
	seen := map[candidateRecordKey]bool{}
	for index, row := range g.records {
		if index < r {
			v, ok := old.records[fixedOldKey{row.key.cell, row.key.target}]
			if row.key.cell < 0 || row.key.cell > 256 || row.key.target.IsZero() || seen[row.key] || !ok || v.expires != row.record.expires || v.refs != row.record.refs {
				t.Fatal("fixed semantic record differs from frozen map", index, row)
			}
			seen[row.key] = true
		} else if row != (candidateGroupSlot{}) {
			t.Fatal("inactive record tail retained")
		}
	}
	if g.generation != old.generation || g.lastUse != old.lastUse || g.used != old.used {
		t.Fatal("fixed lifetime differs from frozen map")
	}
	probes := []domain.SourceGroup{"uncommitted-probe", "g0", "late-overflow"}
	for name := range old.named {
		probes = append(probes, name)
	}
	for _, name := range probes {
		if g.prospective(name) != old.prospective(name) || g.orderCell(name) != old.orderCell(name) {
			t.Fatal("fixed prospective/orderCell differs from frozen map", name)
		}
	}
}
func fixedPlanBoth(t *testing.T, g *candidateGroups, old *fixedOldGroups, name domain.SourceGroup, target domain.PeerIdentity, slot Instant, cap int) (candidateGroupPlan, fixedOldPlan, error) {
	t.Helper()
	a, ae := g.plan(name, target, slot, cap)
	b, be := old.plan(name, target, slot, cap)
	if ae != be {
		t.Fatal("fixed plan error differs from frozen map", ae, be)
	}
	if ae == nil && (a.group != b.group || a.cell != b.cell || a.name != b.name || a.key.cell != b.key.cell || a.key.target != b.key.target || a.expires != b.expires) {
		t.Fatal("fixed plan value differs from frozen map", a, b)
	}
	fixedCompare(t, g, old)
	return a, b, ae
}
func fixedCommitBoth(t *testing.T, g *candidateGroups, old *fixedOldGroups, name domain.SourceGroup, target domain.PeerIdentity, slot, now Instant, cap int) candidateRecordKey {
	t.Helper()
	a, b, e := fixedPlanBoth(t, g, old, name, target, slot, cap)
	if e != nil {
		t.Fatal("valid old-map trace refused", e)
	}
	g.commit(a, now)
	old.commit(b, now)
	fixedCompare(t, g, old)
	return a.key
}
func fixedReleaseBoth(t *testing.T, g *candidateGroups, old *fixedOldGroups, key candidateRecordKey) {
	t.Helper()
	g.release(key)
	old.release(fixedOldKey(key))
	fixedCompare(t, g, old)
}
func fixedExpireBoth(t *testing.T, g *candidateGroups, old *fixedOldGroups, now Instant) {
	t.Helper()
	g.expire(now)
	old.expire(now)
	fixedCompare(t, g, old)
}
func fixedFinishBoth(t *testing.T, g *candidateGroups, old *fixedOldGroups, key candidateRecordKey, at Instant) error {
	t.Helper()
	a := g.finish(key, at)
	b := old.finish(fixedOldKey(key), at)
	if a != b {
		t.Fatal("fixed finish error differs", a, b)
	}
	fixedCompare(t, g, old)
	return a
}
func fixedResetBoth(t *testing.T, g *candidateGroups, old *fixedOldGroups, at Instant, rows int) bool {
	t.Helper()
	a := g.reset(at, rows)
	b := old.reset(at, rows)
	if a != b {
		t.Fatal("fixed reset differs")
	}
	fixedCompare(t, g, old)
	return a
}

func TestCandidateFixedGroupsDifferentialTrace(t *testing.T) {
	for _, name := range []string{"empty", "first-named", "overflow", "active-not-sticky-k", "share-floor", "existing-record", "full-H", "shrink"} {
		t.Run(name, func(t *testing.T) {
			g, old := newCandidateGroups(), newFixedOldGroups()
			fixedCompare(t, &g, &old)
			switch name {
			case "empty":
				if fixedGroupRecords(&g) != 0 || fixedGroupNames(&g) != 0 {
					t.Fatal("nonempty fresh book")
				}
			case "first-named":
				if _, _, err := fixedPlanBoth(t, &g, &old, "first", candidateTarget(1), 0, 512); err != nil {
					t.Fatal("first named plan", err)
				}
				key := fixedCommitBoth(t, &g, &old, "first", candidateTarget(1), 0, 0, 512)
				fixedReleaseBoth(t, &g, &old, key)
			case "overflow", "active-not-sticky-k":
				for i := 0; i < 256; i++ {
					key := fixedCommitBoth(t, &g, &old, domain.SourceGroup(fmt.Sprintf("g%d", i)), candidateTarget(i), 0, 0, 512)
					fixedReleaseBoth(t, &g, &old, key)
				}
				if name == "overflow" {
					for i := 0; i < 16; i++ {
						fixedCommitBoth(t, &g, &old, "late-overflow", candidateTarget(1000+i), 0, 0, 512)
					}
					_, _, e := fixedPlanBoth(t, &g, &old, "another-overflow", candidateTarget(2000), 0, 512)
					if !errors.Is(e, ErrCapacity) {
						t.Fatal("overflow gained separate budget", e)
					}
				} else {
					fixedExpireBoth(t, &g, &old, candidateGroupWindow)
					for i := 0; i < 17; i++ {
						fixedCommitBoth(t, &g, &old, "g0", candidateTarget(3000+i), candidateGroupWindow, candidateGroupWindow, 512)
					}
				}
			case "share-floor":
				for i := 0; i < 33; i++ {
					fixedCommitBoth(t, &g, &old, domain.SourceGroup(fmt.Sprintf("g%d", i)), candidateTarget(i), 0, 0, 512)
				}
				for i := 1; i < 16; i++ {
					fixedCommitBoth(t, &g, &old, "g0", candidateTarget(100+i), 0, 0, 512)
				}
				_, _, e := fixedPlanBoth(t, &g, &old, "g0", candidateTarget(999), 0, 512)
				if !errors.Is(e, ErrCapacity) {
					t.Fatal("share floor exceeded", e)
				}
			case "existing-record":
				key := fixedCommitBoth(t, &g, &old, "g0", candidateTarget(1), 0, 0, 512)
				fixedCommitBoth(t, &g, &old, "g0", candidateTarget(2), 0, 0, 512)
				fixedCommitBoth(t, &g, &old, "g0", key.target, 100, 100, 512)
				fixedReleaseBoth(t, &g, &old, key)
			case "full-H":
				for cell, size := range []int{3840, 1920, 1280, 912} {
					for i := 0; i < size; i++ {
						key := fixedCommitBoth(t, &g, &old, domain.SourceGroup(fmt.Sprintf("g%d", cell)), candidateTarget(i), 0, 0, 3840)
						fixedReleaseBoth(t, &g, &old, key)
					}
				}
				if fixedGroupRecords(&g) != 7952 {
					t.Fatal("hard H witness incomplete")
				}
				_, _, e := fixedPlanBoth(t, &g, &old, "g3", candidateTarget(9999), 0, 3840)
				if !errors.Is(e, ErrCapacity) {
					t.Fatal("full H new key accepted", e)
				}
				fixedCommitBoth(t, &g, &old, "g0", candidateTarget(0), 100, 100, 3840)
			case "shrink":
				for cell := 0; cell <= 50; cell++ {
					n := 16
					if cell == 0 {
						n = 3840
					}
					for i := 0; i < n; i++ {
						fixedCommitBoth(t, &g, &old, domain.SourceGroup(fmt.Sprintf("g%d", cell)), domain.PeerIdentity{byte(cell + 1), byte(i >> 8), byte(i)}, 0, 0, 3840)
					}
				}
				if fixedGroupRecords(&g) != 4640 {
					t.Fatal("shrink witness incomplete")
				}
				_, _, e := fixedPlanBoth(t, &g, &old, "late", candidateTarget(9999), 0, 512)
				if !errors.Is(e, ErrCapacity) {
					t.Fatal("shrink evicted retained H", e)
				}
				fixedCommitBoth(t, &g, &old, "g0", domain.PeerIdentity{1, 0, 0}, 1, 1, 512)
			}
		})
	}
}
func TestCandidateFixedGroupsDenseExpiryAndTailClearing(t *testing.T) {
	for _, name := range []string{"first", "middle", "last", "adjacent-expired", "swap-into-expired", "referenced-swap"} {
		t.Run(name, func(t *testing.T) {
			g, old := newCandidateGroups(), newFixedOldGroups()
			keys := make([]candidateRecordKey, 4)
			for i := range keys {
				keys[i] = fixedCommitBoth(t, &g, &old, "g0", candidateTarget(i), 0, 0, 512)
			}
			released := []int{0}
			switch name {
			case "middle":
				released = []int{1}
			case "last":
				released = []int{3}
			case "adjacent-expired":
				released = []int{0, 1}
			case "swap-into-expired":
				released = []int{0, 3}
			}
			for _, i := range released {
				fixedReleaseBoth(t, &g, &old, keys[i])
			}
			fixedExpireBoth(t, &g, &old, candidateGroupWindow-1)
			fixedExpireBoth(t, &g, &old, candidateGroupWindow)
			fixedExpireBoth(t, &g, &old, candidateGroupWindow+1)
			if fixedGroupRecords(&g) != 4-len(released) {
				t.Fatal("dense expiration skipped swapped row")
			}
		})
	}
}
func TestCandidateFixedGroupsLifetimeAndReset(t *testing.T) {
	for _, name := range []string{"finish-max", "exact-boundary", "idle-window", "refs-retain", "generation-max", "release-last-ref"} {
		t.Run(name, func(t *testing.T) {
			g, old := newCandidateGroups(), newFixedOldGroups()
			if name == "generation-max" {
				g.generation = math.MaxUint64
				old.generation = math.MaxUint64
				if fixedResetBoth(t, &g, &old, 0, 0) {
					t.Fatal("generation wrapped")
				}
				return
			}
			slot := Instant(0)
			if name == "finish-max" {
				slot = Instant(300 * time.Second)
			}
			key := fixedCommitBoth(t, &g, &old, "g0", candidateTarget(1), slot, 0, 512)
			switch name {
			case "finish-max":
				if e := fixedFinishBoth(t, &g, &old, key, Instant(100*time.Second)); e != nil {
					t.Fatal(e)
				}
				if e := fixedFinishBoth(t, &g, &old, key, Instant(500*time.Second)); e != nil {
					t.Fatal(e)
				}
			case "refs-retain":
				fixedExpireBoth(t, &g, &old, candidateGroupWindow)
				if fixedResetBoth(t, &g, &old, candidateGroupWindow, 0) {
					t.Fatal("referenced row reset")
				}
			case "release-last-ref":
				fixedCommitBoth(t, &g, &old, "g0", key.target, 0, 0, 512)
				fixedReleaseBoth(t, &g, &old, key)
				fixedExpireBoth(t, &g, &old, candidateGroupWindow)
				fixedReleaseBoth(t, &g, &old, key)
				fixedExpireBoth(t, &g, &old, candidateGroupWindow)
			default:
				fixedReleaseBoth(t, &g, &old, key)
				fixedExpireBoth(t, &g, &old, candidateGroupWindow-1)
				fixedExpireBoth(t, &g, &old, candidateGroupWindow)
				if fixedResetBoth(t, &g, &old, candidateGroupWindow-1, 0) {
					t.Fatal("idle window shortened")
				}
				if !fixedResetBoth(t, &g, &old, candidateGroupWindow, 0) {
					t.Fatal("exact reset boundary refused")
				}
				fixedCommitBoth(t, &g, &old, "reuse", candidateTarget(2), candidateGroupWindow, candidateGroupWindow, 512)
			}
		})
	}
}
func TestCandidateFixedGroupsNoParallelAuthorityAndAtomicRefusal(t *testing.T) {
	o := capacityOwner(t, candidateCapacityHint{})
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	originalOwner, originalCapacity, originalDomain := o.sourceOwner, o.capacity, o.capacity.domain
	r, out := candidateSourceOfferSeal(t, o, source, target, 1, 0)
	if out.reserved != 1 {
		t.Fatal(out)
	}
	key := r.entry.intake.cell
	calendar := r.entry.intake.calendar
	windows := source.state.sourceWindows.Load()
	projected := calendar.projected
	if ok, e := o.cancel(r, o.profile.round); e != nil || !ok {
		t.Fatal(e)
	}
	before := fixedMustRecord(t, &o.groups, key)
	history := windows.rows
	id := target.state.nextID
	target.state.exhausted = true
	_, out = candidateSourceOfferSeal(t, o, source, target, 2, o.profile.round)
	target.state.exhausted = false
	if out.reserved != 0 || out.refused != 1 || fixedMustRecord(t, &o.groups, key) != before || windows.rows != history || target.state.nextID != id || calendar.projected != projected || o.sourceOwner != originalOwner || o.capacity != originalCapacity || o.capacity.domain != originalDomain || fixedGroupRecords(&o.groups) != 1 {
		t.Fatal("fixed group refusal replaced original authority or refunded debt")
	}
	if ok, _ := o.cancel(r, 2*o.profile.round); ok || fixedMustRecord(t, &o.groups, key).refs != 0 {
		t.Fatal("stale cancel reached group row")
	}
}
func TestCandidateFixedGroupsSemanticKeysSurviveMoves(t *testing.T) {
	o := candidateTestOwner(t)
	groupCommit(t, &o.groups, "expired", candidateTarget(999), 0, 512)
	source, _ := provenFixture(t, 1, "source", 0)
	target, signer := provenFixture(t, 2, "target", 0)
	r, out := candidateSourceOfferSeal(t, o, source, target, 1, 0)
	if out.reserved != 1 {
		t.Fatal(out)
	}
	key := r.entry.intake.cell
	permit, _, e := o.takeForward(r, o.profile.round)
	if e != nil {
		t.Fatal(e)
	}
	if e := o.expire(candidateGroupWindow); e != nil {
		t.Fatal(e)
	}
	if fixedGroupRecords(&o.groups) != 1 || fixedMustRecord(t, &o.groups, key).refs != 1 {
		t.Fatal("referenced semantic key lost during compaction")
	}
	finish := candidateGroupWindow + o.profile.round
	if e := o.finishForward(permit, finish, candidateWriteFinished); e != nil {
		t.Fatal(e)
	}
	if fixedMustRecord(t, &o.groups, key).expires != finish+candidateGroupWindow || fixedMustRecord(t, &o.groups, key).refs != 0 {
		t.Fatal("Finish updated moved wrong semantic row")
	}
	received := finish + o.profile.round
	r2, out := candidateSourceOfferSeal(t, o, source, target, 2, received)
	if out.reserved != 1 {
		t.Fatal(out)
	}
	at := received + o.profile.round
	p, input, e := o.takeForward(r2, at)
	if e != nil {
		t.Fatal(e)
	}
	if e := o.finishForward(p, at, candidateWriteFinished); e != nil {
		t.Fatal(e)
	}
	if _, e := o.answer(target, bodyFor(t, signer).Answer(input.request.ID), at); e != nil || fixedMustRecord(t, &o.groups, key).refs != 0 {
		t.Fatal("Answer lost moved semantic reference", e)
	}
	r3, out := candidateSourceOfferSeal(t, o, source, target, 3, at+o.profile.round)
	if out.reserved != 1 {
		t.Fatal(out)
	}
	before := fixedMustRecord(t, &o.groups, key)
	if ok, _ := o.cancel(r2, at+2*o.profile.round); ok || fixedMustRecord(t, &o.groups, key) != before {
		t.Fatal("stale reservation affected reused semantic row")
	}
	if ok, e := o.cancel(r3, at+2*o.profile.round); !ok || e != nil || fixedMustRecord(t, &o.groups, key).refs != 0 {
		t.Fatal("Cancel lost original semantic row", e)
	}
}
func TestCandidateFixedGroupsBoundaryInputsAndNoPlanEffects(t *testing.T) {
	for _, name := range []string{"empty-group", "zero-target", "negative-slot", "invalid-cap", "expiry-overflow", "missing-record"} {
		t.Run(name, func(t *testing.T) {
			g, old := newCandidateGroups(), newFixedOldGroups()
			group, target, slot, cap := domain.SourceGroup("g0"), candidateTarget(1), Instant(0), 512
			switch name {
			case "empty-group":
				group = ""
			case "zero-target":
				target = domain.PeerIdentity{}
			case "negative-slot":
				slot = -1
			case "invalid-cap":
				cap = 511
			case "expiry-overflow":
				slot = Instant(math.MaxInt64)
			case "missing-record":
				if r, ok := g.record(candidateRecordKey{0, target}); ok || r != (candidateGroupRecord{}) {
					t.Fatal("missing record became authority")
				}
				if e := fixedFinishBoth(t, &g, &old, candidateRecordKey{0, target}, 0); !errors.Is(e, ErrStaleWork) {
					t.Fatal(e)
				}
				return
			}
			_, _, e := fixedPlanBoth(t, &g, &old, group, target, slot, cap)
			if e == nil {
				t.Fatal("invalid pure plan accepted")
			}
			fixedCompare(t, &g, &old)
		})
	}
}
