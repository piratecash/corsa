package findnode

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Actual TLS Session proofs and original fixture roles/Seal. ConnID, opened,
// group, classes and q are labelled logical fixture inputs, not Node provenance.
func ownedScratchMembers(t *testing.T, opened []Instant) []Session {
	t.Helper()
	members := make([]Session, len(opened))
	for i, at := range opened {
		members[i] = ownedOriginalSession(t, domain.ConnID(i+1), at)
	}
	return members
}
func ownedScratchSeal(t *testing.T, members []Session, classes []SessionClass) (DormantIntake, OwnedCut) {
	t.Helper()
	d, clock, _ := ownedOriginalDomain(t)
	clock.Store(int64(600 * time.Second))
	intake, sink, err := NewDormantFixtureIntake(d.Consumer(), ownedBridgeProfile(), max(1, len(members)))
	if err != nil {
		t.Fatal(err)
	}
	facts := make([]ObservedMember, len(members))
	for i, member := range members {
		class := ClassOrdinary
		if classes != nil {
			class = classes[i]
		}
		facts[i] = ObservedMember{Session: member, Class: class}
	}
	stamp, at := ownedBridgePoint(t, d)
	cut, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 600*time.Second), facts, ObservedDegree{})
	if err != nil {
		t.Fatal(err)
	}
	return intake, cut
}
func ownedScratchFixture(t *testing.T, n int) (DormantIntake, OwnedCut, []Session) {
	t.Helper()
	members := ownedScratchMembers(t, make([]Instant, n))
	intake, cut := ownedScratchSeal(t, members, nil)
	return intake, cut, members
}
func ownedScratchBorrow(t *testing.T, intake DormantIntake, cut OwnedCut) ownedAttempt {
	t.Helper()
	attempt, err := intake.tryOwnedAttempt(cut)
	if err != nil || attempt.claim == nil {
		t.Fatal("genuine scratch borrow", err)
	}
	t.Cleanup(func() { _ = attempt.releaseOwnedAttempt() })
	return attempt
}
func ownedScratchZero(t *testing.T, owner *dormantIntakeState, borrowed bool) {
	t.Helper()
	s := &owner.scratch
	if s.targets != ([MaxPendingTotal]ownedProjectionTarget{}) || s.offered != ([MaxPendingTotal]ownedProjectionSource{}) || s.targetCount != 0 || s.offeredCount != 0 || s.maximum != 0 || s.borrowed.Load() != borrowed {
		t.Fatal("scratch cleanup retained Session refs, counts, maximum or wrong borrow")
	}
}
func ownedScratchRelease(t *testing.T, attempt ownedAttempt) {
	t.Helper()
	if err := attempt.releaseOwnedAttempt(); err != nil {
		t.Fatal("release genuine borrow", err)
	}
	if attempt.claim.phase.Load() != ownedAttemptClosed || attempt.claim.projected {
		t.Fatal("released claim not CLOSED/unprojected")
	}
	ownedScratchZero(t, attempt.claim.owner, false)
}

func TestOwnedAttemptBorrowLifecycle(t *testing.T) {
	for _, name := range []string{"zero-intake", "zero-cut", "foreign-cut", "copied-wrapper-busy", "zero-release", "repeated-release", "alias-release", "stale-alias-after-next-borrow"} {
		t.Run(name, func(t *testing.T) {
			intake, cut, members := ownedScratchFixture(t, 2)
			before := intake.dormantOwner.coordinator.snapshot()
			capacity := *intake.dormantOwner.coordinator.capacity
			switch name {
			case "zero-intake":
				got, err := (DormantIntake{}).tryOwnedAttempt(cut)
				if !errors.Is(err, ErrInvalid) || got.claim != nil {
					t.Fatal("zero intake borrowed", err)
				}
			case "zero-cut":
				got, err := intake.tryOwnedAttempt(OwnedCut{})
				if !errors.Is(err, ErrInvalid) || got.claim != nil {
					t.Fatal("zero cut borrowed", err)
				}
			case "foreign-cut":
				_, other, _ := ownedScratchFixture(t, 0)
				got, err := intake.tryOwnedAttempt(other)
				if !errors.Is(err, ErrUnauthenticated) || got.claim != nil {
					t.Fatal("foreign cut borrowed", err)
				}
			case "zero-release":
				if err := (ownedAttempt{}).releaseOwnedAttempt(); !errors.Is(err, ErrInvalid) {
					t.Fatal("zero release", err)
				}
			default:
				attempt := ownedScratchBorrow(t, intake, cut)
				alias := attempt
				if err := attempt.projectOwnedMembership([]domain.PeerIdentity{members[1].Identity()}, members); err != nil {
					t.Fatal(err)
				}
				if name == "copied-wrapper-busy" {
					copied := intake
					got, err := copied.tryOwnedAttempt(cut)
					if !errors.Is(err, ErrNotReady) || got.claim != nil {
						t.Fatal("copied owner acquired a second lane", err)
					}
					// Explicit private phase injection tests refusal mechanics only, not an
					// adjacent-instruction runtime scheduling witness or physical F campaign.
					targets, offered := intake.dormantOwner.scratch.targets, intake.dormantOwner.scratch.offered
					attempt.claim.phase.Store(ownedAttemptBusy)
					releaseErr := alias.releaseOwnedAttempt()
					projectErr := attempt.projectOwnedMembership(nil, nil)
					_, queryErr := attempt.ownedHistoricalPriority(members[0], members[1].Identity())
					intact := intake.dormantOwner.scratch.borrowed.Load() && intake.dormantOwner.scratch.targets == targets && intake.dormantOwner.scratch.offered == offered
					attempt.claim.phase.Store(ownedAttemptLive)
					if !errors.Is(releaseErr, ErrNotReady) || !errors.Is(projectErr, ErrNotReady) || !errors.Is(queryErr, ErrNotReady) || !intact {
						t.Fatal("BUSY operation released or modified original lane")
					}
				}
				if name == "alias-release" {
					ownedScratchRelease(t, alias)
				} else {
					ownedScratchRelease(t, attempt)
				}
				if name == "repeated-release" || name == "alias-release" {
					if err := alias.releaseOwnedAttempt(); !errors.Is(err, ErrStaleWork) {
						t.Fatal("replayed alias released", err)
					}
				}
				if name == "stale-alias-after-next-borrow" {
					next := ownedScratchBorrow(t, intake, cut)
					if next.claim == attempt.claim {
						t.Fatal("next borrow reused old claim")
					}
					if err := next.projectOwnedMembership([]domain.PeerIdentity{members[1].Identity()}, members); err != nil {
						t.Fatal(err)
					}
					targetRows, offerRows := intake.dormantOwner.scratch.targets, intake.dormantOwner.scratch.offered
					if err := alias.releaseOwnedAttempt(); !errors.Is(err, ErrStaleWork) || !intake.dormantOwner.scratch.borrowed.Load() || next.claim.phase.Load() != ownedAttemptLive || intake.dormantOwner.scratch.targets != targetRows || intake.dormantOwner.scratch.offered != offerRows {
						t.Fatal("stale alias cleared next original borrow", err)
					}
					ownedScratchRelease(t, next)
				}
			}
			ownedScratchZero(t, intake.dormantOwner, false)
			if intake.dormantOwner.coordinator.snapshot() != before || *intake.dormantOwner.coordinator.capacity != capacity {
				t.Fatal("borrow lifecycle changed old accounting")
			}
		})
	}
}

func TestOwnedAttemptConcurrentBorrow(t *testing.T) {
	intake, cut, _ := ownedScratchFixture(t, 1)
	type result struct {
		attempt ownedAttempt
		err     error
	}
	results := make(chan result, 8)
	start := make(chan struct{})
	var jobs sync.WaitGroup
	for range 8 {
		jobs.Add(1)
		go func() { defer jobs.Done(); <-start; a, e := intake.tryOwnedAttempt(cut); results <- result{a, e} }()
	}
	close(start)
	observed := make([]result, 0, 8)
	for range 8 {
		observed = append(observed, <-results)
	}
	jobs.Wait()
	winners := 0
	for _, r := range observed {
		if r.err == nil {
			winners++
			t.Cleanup(func() { _ = r.attempt.releaseOwnedAttempt() })
		} else if !errors.Is(r.err, ErrNotReady) || r.attempt.claim != nil {
			t.Fatal("contender returned unexpected partial claim", r.err)
		}
	}
	// No winner was released until ALL eight contenders returned and joined.
	if winners != 1 {
		t.Fatal("held owner lane admitted more than one concurrent winner", winners)
	}
	for _, r := range observed {
		if r.err == nil {
			ownedScratchRelease(t, r.attempt)
		}
	}
}

func TestOwnedProjectionShapeAndCopies(t *testing.T) {
	for _, name := range []string{"empty", "all-offered", "duplicate-targets-and-sources", "missing-offered", "zero-target", "targets-over512", "offered-over512", "caller-array-mutation", "repeated-projection", "failure-clears-partial-output"} {
		t.Run(name, func(t *testing.T) {
			intake, cut, members := ownedScratchFixture(t, 2)
			attempt := ownedScratchBorrow(t, intake, cut)
			s := &intake.dormantOwner.scratch
			targets := []domain.PeerIdentity{members[1].Identity()}
			offered := append([]Session(nil), members...)
			switch name {
			case "empty":
				targets = nil
				offered = nil
			case "duplicate-targets-and-sources":
				targets = append(targets, targets[0], targets[0])
				offered = append(offered, members[0], members[1])
			case "missing-offered", "failure-clears-partial-output":
				offered = append(offered, ownedOriginalSession(t, 9, 0))
			case "zero-target":
				targets = []domain.PeerIdentity{{}}
			case "targets-over512":
				targets = make([]domain.PeerIdentity, MaxPendingTotal+1)
				for i := range targets {
					targets[i] = members[1].Identity()
				}
			case "offered-over512":
				offered = make([]Session, MaxPendingTotal+1)
				for i := range offered {
					offered[i] = members[0]
				}
			}
			err := attempt.projectOwnedMembership(targets, offered)
			invalid := name == "missing-offered" || name == "failure-clears-partial-output" || name == "zero-target" || name == "targets-over512" || name == "offered-over512"
			if invalid {
				want := ErrInvalid
				if name == "missing-offered" || name == "failure-clears-partial-output" {
					want = ErrPrioritySnapshot
				}
				if !errors.Is(err, want) || attempt.claim.projected || attempt.claim.phase.Load() != ownedAttemptLive {
					t.Fatal("projection failure did not retain same LIVE unprojected claim", err)
				}
				ownedScratchZero(t, intake.dormantOwner, true)
				if err := attempt.projectOwnedMembership([]domain.PeerIdentity{members[1].Identity()}, members); err != nil {
					t.Fatal("corrected bounded retry lost original borrow", err)
				}
			} else {
				if err != nil || !attempt.claim.projected || s.maximum != 2 {
					t.Fatal("complete bounded historical projection", err)
				}
				if name == "empty" && (s.targetCount != 0 || s.offeredCount != 0) {
					t.Fatal("empty inputs retained output rows")
				}
				if name == "duplicate-targets-and-sources" && (s.targetCount != 1 || s.offeredCount != 2) {
					t.Fatal("bounded dedup changed map-compatible semantics")
				}
				if name == "all-offered" {
					for i, member := range members {
						if s.offered[i].session != member || s.offered[i].class != ClassOrdinary {
							t.Fatal("offered facts lost exact original class/index")
						}
					}
				}
				if name == "caller-array-mutation" {
					wantedTarget, wantedSource := members[1].Identity(), members[0]
					targets[0] = members[0].Identity()
					for i := range offered {
						offered[i] = Session{}
					}
					got, e := attempt.ownedHistoricalPriority(wantedSource, wantedTarget)
					if e != nil || got != PriorityAged || s.targets[0].identity != wantedTarget || s.offered[0].session != wantedSource {
						t.Fatal("projection borrowed caller arrays", got, e)
					}
				}
				if name == "repeated-projection" {
					oldTargets, oldOffered, oldMaximum := s.targets, s.offered, s.maximum
					if e := attempt.projectOwnedMembership(nil, nil); !errors.Is(e, ErrInvalid) || !attempt.claim.projected || s.targets != oldTargets || s.offered != oldOffered || s.maximum != oldMaximum {
						t.Fatal("repeated projection changed first successful facts", e)
					}
				}
			}
			ownedScratchRelease(t, attempt)
		})
	}
}

func TestOwnedProjectionHistoricalMembership(t *testing.T) {
	for _, name := range []string{"closed-after-Seal-still-historical", "target-link-kept-in-evidence", "ordinary-at600", "young-unselected-original", "protected-excluded-from-oldest", "incoming-pin-not-protected", "equal-opened-ConnID-order", "maximum-from-no-pending-original"} {
		t.Run(name, func(t *testing.T) {
			opened := make([]Instant, 6)
			classes := []SessionClass{ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassOrdinary}
			if name == "young-unselected-original" {
				opened = opened[:2]
				classes = classes[:2]
				opened[0] = 1
			}
			if name == "protected-excluded-from-oldest" {
				classes[0], classes[1] = ClassContact, ClassSelected
			}
			if name == "incoming-pin-not-protected" {
				opened[0] = Instant(600 * time.Second)
				classes[0] = ClassIncomingPinned
				for i := 1; i < len(classes); i++ {
					classes[i] = ClassContact
				}
			}
			if name == "maximum-from-no-pending-original" {
				classes[len(classes)-1] = ClassSelected
			}
			members := ownedScratchMembers(t, opened)
			ordered := append([]Session(nil), members...)
			if name == "equal-opened-ConnID-order" {
				for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
					ordered[i], ordered[j] = ordered[j], ordered[i]
				}
			}
			intake, cut := ownedScratchSeal(t, ordered, classes)
			attempt := ownedScratchBorrow(t, intake, cut)
			target := members[len(members)-1].Identity()
			offered := members
			if name == "closed-after-Seal-still-historical" {
				members[0].End()
			}
			if name == "target-link-kept-in-evidence" {
				target = members[0].Identity()
			}
			if name == "maximum-from-no-pending-original" {
				offered = members[:1]
			}
			if err := attempt.projectOwnedMembership([]domain.PeerIdentity{target}, offered); err != nil {
				t.Fatal("historical projection rejected genuine sealed originals", err)
			}
			s := &intake.dormantOwner.scratch
			if s.maximum != domain.ConnID(len(members)) || len(cut.sealedCut.originals.members) != len(members) {
				t.Fatal("maximum lost full no-pending/target/protected original evidence", s.maximum)
			}
			source := members[0]
			want := PriorityAged
			switch name {
			case "target-link-kept-in-evidence":
				source = members[1]
				for _, row := range s.targets[0].oldest {
					if row == members[0] {
						t.Fatal("target link entered its own oldest four")
					}
				}
			case "young-unselected-original":
				want = PriorityNew
			case "protected-excluded-from-oldest":
				want = PriorityProtected
				for _, row := range s.targets[0].oldest {
					if row == members[0] || row == members[1] {
						t.Fatal("protected member consumed oldest witness")
					}
				}
			case "equal-opened-ConnID-order":
				if s.targets[0].oldest != ([4]Session{members[0], members[1], members[2], members[3]}) {
					t.Fatal("equal-opened witnesses ignored ConnID order")
				}
			}
			got, err := attempt.ownedHistoricalPriority(source, target)
			if err != nil || got != want {
				t.Fatal("historical class/age priority differs", name, got, want, err)
			}
			ownedScratchRelease(t, attempt)
		})
	}
}

func TestOwnedProjectionTargetRelativeOldestFour(t *testing.T) {
	opened := []Instant{0, 0, 0, 0, 0, Instant(599 * time.Second), 0, 0}
	classes := []SessionClass{ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassOrdinary, ClassContact, ClassSelected}
	members := ownedScratchMembers(t, opened)
	ordered := []Session{members[7], members[5], members[4], members[2], members[0], members[6], members[3], members[1]}
	positional := []SessionClass{classes[7], classes[5], classes[4], classes[2], classes[0], classes[6], classes[3], classes[1]}
	intake, cut := ownedScratchSeal(t, ordered, positional)
	attempt := ownedScratchBorrow(t, intake, cut)
	targets := []domain.PeerIdentity{members[0].Identity(), members[1].Identity()}
	if err := attempt.projectOwnedMembership(targets, members); err != nil {
		t.Fatal(err)
	}
	for index, target := range targets {
		// Independent fixture oracle: explicit fixture opened/classes and ordinary
		// comparisons, never candidateOlder or the implementation's projection.
		eligible := []Session{}
		for i, member := range members {
			if member.Identity() != target && classes[i] != ClassContact && classes[i] != ClassSelected && (classes[i] == ClassIncomingPinned || Instant(600*time.Second)-opened[i] >= Instant(600*time.Second)) {
				eligible = append(eligible, member)
			}
		}
		sort.Slice(eligible, func(i, j int) bool {
			if eligible[i].state.opened != eligible[j].state.opened {
				return eligible[i].state.opened < eligible[j].state.opened
			}
			return eligible[i].ConnID() < eligible[j].ConnID()
		})
		var oldest [4]Session
		copy(oldest[:], eligible[:min(4, len(eligible))])
		if intake.dormantOwner.scratch.targets[index].oldest != oldest {
			t.Fatal("target-relative independent oldest-four witnesses differ", index)
		}
		for i, member := range members {
			got, err := attempt.ownedHistoricalPriority(member, target)
			if member.Identity() == target {
				if !errors.Is(err, ErrPrioritySnapshot) {
					t.Fatal("target queried as source", err)
				}
				continue
			}
			want := PriorityNew
			if classes[i] == ClassContact || classes[i] == ClassSelected {
				want = PriorityProtected
			} else {
				for _, older := range oldest {
					if older == member {
						want = PriorityAged
					}
				}
			}
			if err != nil || got != want {
				t.Fatal("independent target-relative priority differs", index, i, got, want, err)
			}
		}
	}
	ownedScratchRelease(t, attempt)
}

func TestOwnedProjectionOwnerAndQueryRefusals(t *testing.T) {
	for _, name := range []string{"zero-attempt", "foreign-source-handle", "source-is-target", "absent-target", "unprojected", "released-attempt"} {
		t.Run(name, func(t *testing.T) {
			intake, cut, members := ownedScratchFixture(t, 2)
			attempt := ownedScratchBorrow(t, intake, cut)
			source, target := members[0], members[1].Identity()
			want := ErrPrioritySnapshot
			if name != "unprojected" {
				if err := attempt.projectOwnedMembership([]domain.PeerIdentity{target}, members); err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "zero-attempt":
				attempt = ownedAttempt{}
				want = ErrInvalid
			case "foreign-source-handle":
				source = ownedOriginalSession(t, 1, 0)
			case "source-is-target":
				source = members[1]
			case "absent-target":
				target = ownedOriginalSession(t, 9, 0).Identity()
			case "unprojected":
				want = ErrNotReady
			case "released-attempt":
				ownedScratchRelease(t, attempt)
				want = ErrStaleWork
			}
			if got, err := attempt.ownedHistoricalPriority(source, target); !errors.Is(err, want) || got != 0 {
				t.Fatal("invalid historical query produced priority", got, err)
			}
		})
	}
}

func TestOwnedProjectionBoundedM(t *testing.T) {
	intake, cut, members := ownedScratchFixture(t, 2)
	owner := intake.dormantOwner
	originalM := owner.maxMembers
	// Explicit negative private shape corruption, never manufactured positives.
	malformed := *cut.sealedCut
	malformed.classes = malformed.classes[:1]
	if a, e := intake.tryOwnedAttempt(OwnedCut{sealedCut: &malformed}); !errors.Is(e, ErrInvalid) || a.claim != nil || owner.scratch.borrowed.Load() {
		t.Fatal("malformed class shape acquired scratch", e)
	}
	extra := ownedOriginalSession(t, 3, 0)
	malformed = *cut.sealedCut
	malformed.originals.members = append(append([]Session(nil), members...), extra)
	malformed.classes = []SessionClass{ClassOrdinary, ClassOrdinary, ClassOrdinary}
	if a, e := intake.tryOwnedAttempt(OwnedCut{sealedCut: &malformed}); !errors.Is(e, ErrInvalid) || a.claim != nil || owner.maxMembers != originalM {
		t.Fatal("M-bound failure altered owner or borrowed", e)
	}
	attempt := ownedScratchBorrow(t, intake, cut)
	targets := make([]domain.PeerIdentity, MaxPendingTotal)
	offered := make([]Session, MaxPendingTotal)
	for i := range targets {
		targets[i] = members[1].Identity()
		offered[i] = members[0]
	}
	if err := attempt.projectOwnedMembership(targets, offered); err != nil || owner.scratch.targetCount != 1 || owner.scratch.offeredCount != 1 || len(owner.scratch.targets) != 512 || len(owner.scratch.offered) != 512 || owner.maxMembers != originalM {
		t.Fatal("fixed scratch/dedup/M bound differs", err)
	}
	ownedScratchRelease(t, attempt)
	empty, emptyCut, _ := ownedScratchFixture(t, 0)
	a := ownedScratchBorrow(t, empty, emptyCut)
	if err := a.projectOwnedMembership(append(targets, targets[0]), nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("N=0 bypassed D before-dedup bound", err)
	}
	if err := a.projectOwnedMembership(nil, append(offered, offered[0])); !errors.Is(err, ErrInvalid) {
		t.Fatal("N=0 bypassed P before-dedup bound", err)
	}
	if err := a.projectOwnedMembership(nil, nil); err != nil || empty.dormantOwner.scratch.maximum != 0 {
		t.Fatal("valid prepared N=D=P=0 refused", err)
	}
	ownedScratchRelease(t, a)
}

// Safe scalar/row copies only: no Mutex, atomic or scratch struct is copied.
func ownedScratchOldAccounting(intake DormantIntake, members []Session) any {
	owner := intake.dormantOwner
	o := owner.coordinator
	names := make(map[domain.SourceGroup]int)
	for cell := 0; cell < o.groups.namedCount; cell++ {
		names[o.groups.named[cell]] = cell
	}
	records := make(map[candidateRecordKey]candidateGroupRecord)
	for index := 0; index < o.groups.recordCount; index++ {
		row := o.groups.records[index]
		records[row.key] = row.record
	}
	calendars := make(map[domain.PeerIdentity]any)
	for _, c := range o.calendars {
		if c == nil {
			continue
		}
		slots := make(map[*forwardSlot]forwardSlot)
		if c.ownedAliases {
			for i := 0; i < c.aliasCount; i++ {
				slot := c.aliases[i]
				slots[slot] = *slot
			}
		} else {
			for slot := range c.slots {
				slots[slot] = *slot
			}
		}
		calendars[c.target] = []any{c, c.target, c.projected, c.actual, c.last, slots}
	}
	bySource := make(map[pendingKey]any)
	for k, e := range pendingTestSourceMapLocked(o.pending) {
		bySource[k] = []any{e, e.source, e.target, e.request, e.forwarded, e.expires, *e.intake}
	}
	byTarget := make(map[pendingKey]*pendingEntry)
	for k, v := range pendingTestTargetMapLocked(o.pending) {
		byTarget[k] = v
	}
	sources := make(map[Session]int)
	for k, v := range pendingTestSourceCountsLocked(o.pending) {
		sources[k] = v
	}
	targets := make(map[domain.PeerIdentity]int)
	for k, v := range pendingTestTargetCountsLocked(o.pending) {
		targets[k] = v
	}
	sessionRows := make([]any, len(members))
	for i, member := range members {
		state := member.state
		windows := state.sourceWindows.Load()
		var history any
		if windows != nil {
			history = []any{windows, windows.owner, windows.rows}
		}
		sessionRows[i] = []any{member, state.nextID, state.exhausted, state.closed.Load(), state.signer, state.signingProjected, state.signingActual, history}
	}
	return []any{owner.consumer, owner.profile, owner.maxMembers, o, o.pending, o.sourceOwner, o.profile, *o.capacity, o.last, o.generation, o.round, o.open, o.cut, o.cursor, o.watermark, o.sweep, o.rrCell, o.rrSession, o.counters, o.snapshot(), names, records, o.groups.generation, o.groups.lastUse, o.groups.used, calendars, bySource, byTarget, sources, targets, o.pending.last, o.pending.counters, o.pending.snapshot(), sessionRows}
}
func TestOwnedAttemptNoAccountingEffects(t *testing.T) {
	intake, cut, members := ownedScratchFixture(t, 3)
	o := intake.dormantOwner.coordinator
	source, target := members[0], members[2]
	sources := members[:2]
	r, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, 0, sources, members, target), 0)
	if err != nil {
		t.Fatal(err)
	}
	at := o.profile.round
	out, err := o.sealRound(candidateView(t, o, at, sources, members, target), at)
	if err != nil || out.reserved != 1 || source.state.sourceWindows.Load() == nil {
		t.Fatal("prior genuine logical accepted/source-debt fixture", out, err)
	}
	if _, err := o.offer(members[1], target, request(2, target.Identity(), 2), candidateView(t, o, at, sources, members, target), at); err != nil {
		t.Fatal("prior logical provisional fixture", err)
	}
	originalSessions := append([]Session(nil), cut.sealedCut.originals.members...)
	originalClasses := append([]SessionClass(nil), cut.sealedCut.classes...)
	before := ownedScratchOldAccounting(intake, members)
	oldPending := *r.entry.intake
	attempt := ownedScratchBorrow(t, intake, cut)
	if err := attempt.projectOwnedMembership([]domain.PeerIdentity{target.Identity()}, members); err != nil {
		t.Fatal(err)
	}
	if _, err := attempt.ownedHistoricalPriority(source, target.Identity()); err != nil {
		t.Fatal(err)
	}
	if err := attempt.projectOwnedMembership(nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("repeated projection accepted", err)
	}
	if _, err := attempt.ownedHistoricalPriority(Session{}, target.Identity()); !errors.Is(err, ErrPrioritySnapshot) {
		t.Fatal("zero source accepted", err)
	}
	ownedScratchRelease(t, attempt)
	if !reflect.DeepEqual(before, ownedScratchOldAccounting(intake, members)) || oldPending != *r.entry.intake || !reflect.DeepEqual(originalSessions, cut.sealedCut.originals.members) || !reflect.DeepEqual(originalClasses, cut.sealedCut.classes) {
		t.Fatal("scratch changed old accounting, source companion, history, ID or original evidence")
	}
}
