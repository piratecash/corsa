package findnode

import (
	"errors"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type ownedRoundFixture struct {
	intake  DormantIntake
	cut     OwnedCut
	attempt ownedAttempt
	members []Session
	handles []candidateReservation
	classes map[domain.ConnID]SessionClass
}

// Actual TLS proofs and the original tagged FixtureDomain are used. ConnIDs,
// classes, receive/q times and scheduling scalars are logical fixture inputs.
func ownedRoundNew(t *testing.T, n int, q Instant) ownedRoundFixture {
	t.Helper()
	d, clock, _ := ownedOriginalDomain(t)
	profile := ownedBridgeProfile()
	profile.Round, profile.Slack = 100*time.Millisecond, 300*time.Millisecond
	ns := max(1, n)
	nt := max(1, (n+MaxPendingPerTarget-1)/MaxPendingPerTarget)
	intake, sink, err := NewDormantFixtureIntake(d.Consumer(), profile, ns+nt)
	if err != nil {
		t.Fatal(err)
	}
	members := make([]Session, 0, ns+nt)
	classes := map[domain.ConnID]SessionClass{}
	for i := 0; i < ns; i++ {
		group := []domain.SourceGroup{"A", "B", "U", "V"}[i%4]
		opened := Instant(599 * time.Second)
		if i < 6 {
			opened = 0
		}
		s, _ := provenFixture(t, domain.ConnID(i+1), group, opened)
		members = append(members, s)
		class := ClassOrdinary
		if i < 3 {
			class = ClassSelected
		}
		classes[s.ConnID()] = class
	}
	for i := 0; i < nt; i++ {
		s, _ := provenFixture(t, domain.ConnID(100000+i), "target", 0)
		members = append(members, s)
		classes[s.ConnID()] = ClassSelected
	}
	o := intake.dormantOwner.coordinator
	groupCommit(t, &o.groups, "A", members[ns].Identity(), 0, 512)
	groupCommit(t, &o.groups, "B", members[ns].Identity(), 0, 512)
	received := Instant(599900 * time.Millisecond)
	targets := make([]domain.PeerIdentity, nt)
	for i := range targets {
		targets[i] = members[ns+i].Identity()
	}
	view, err := captureCandidateMembership(o, targets, members[:ns], members, received, func(info SessionInfo) (SessionClass, error) { return classes[info.ConnID], nil })
	if err != nil {
		t.Fatal(err)
	}
	handles := make([]candidateReservation, 0, n)
	for i := 0; i < ns; i++ {
		target := members[ns+i/MaxPendingPerTarget]
		r, err := o.offer(members[i], target, request(domain.FindNodeRequestID(i+1), target.Identity(), 1), view, received)
		if err != nil {
			t.Fatal("lawful provisional fixture", i, err)
		}
		if n == 0 {
			if ok, e := o.cancel(r, received); !ok || e != nil {
				t.Fatal(e)
			}
		} else {
			handles = append(handles, r)
		}
	}
	// Controlled scheduling values, not a fake Ready or receive-time authority.
	o.cursor, o.watermark, o.rrCell = 2, 2, 1
	o.rrSession[candidateNamedGroups] = 8
	clock.Store(int64(q))
	stamp, at := ownedBridgePoint(t, d)
	facts := make([]ObservedMember, len(members))
	for i, s := range members {
		facts[i] = ObservedMember{s, classes[s.ConnID()]}
	}
	cut, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 600*time.Second), facts, ObservedDegree{})
	if err != nil {
		t.Fatal(err)
	}
	attempt := ownedScratchBorrow(t, intake, cut)
	if err := attempt.projectOwnedMembership(targets, members[:ns]); err != nil {
		t.Fatal(err)
	}
	return ownedRoundFixture{intake, cut, attempt, members, handles, classes}
}
func ownedRoundIDs(plan *ownedRoundPlanState) []domain.ConnID {
	ids := make([]domain.ConnID, plan.count)
	for i := range ids {
		ids[i] = plan.rows[plan.order[i]].connID
	}
	return ids
}

// Frozen old sealRound comparator/order block, with priorities kept in a TEST
// map so it cannot write real pending rows. This is not the new comparator or
// a copy of the new rank algorithm, and does not model old sealRound effects.
func ownedRoundFrozenOldOrder(t *testing.T, f ownedRoundFixture) []candidateReservation {
	t.Helper()
	o := f.intake.dormantOwner.coordinator
	targets := []domain.PeerIdentity{}
	for _, r := range f.handles {
		targets = append(targets, r.entry.request.Target)
	}
	view, err := captureCandidateMembership(o, targets, f.members, f.members, Instant(f.cut.sealedCut.at.Elapsed()), func(info SessionInfo) (SessionClass, error) { return f.classes[info.ConnID], nil })
	if err != nil {
		t.Fatal(err)
	}
	rows := []*pendingEntry{}
	priorities := map[*pendingEntry]Priority{}
	order := map[*pendingEntry]int{}
	for _, e := range o.pending.entries {
		if e != nil && e.intake.phase == candidateProvisional {
			rows = append(rows, e)
			p, err := view.priority(o, e.source, e.request.Target, view.at)
			if err != nil {
				t.Fatal(err)
			}
			priorities[e] = p
			order[e] = o.groups.orderCell(e.source.state.group)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		ap, bp := priorities[a], priorities[b]
		if ap != bp {
			return ap < bp
		}
		if ap == PriorityProtected {
			aw, bw := a.source.ConnID() <= o.cursor, b.source.ConnID() <= o.cursor
			if aw != bw {
				return !aw
			}
			return a.source.ConnID() < b.source.ConnID()
		}
		if ap == PriorityAged {
			if a.source.state.opened != b.source.state.opened {
				return a.source.state.opened < b.source.state.opened
			}
			return a.source.ConnID() < b.source.ConnID()
		}
		ac, bc := order[a], order[b]
		ad, bd := (ac-o.rrCell+candidateGroupCells)%candidateGroupCells, (bc-o.rrCell+candidateGroupCells)%candidateGroupCells
		if ad != bd {
			return ad < bd
		}
		aw, bw := a.source.ConnID() <= o.rrSession[ac], b.source.ConnID() <= o.rrSession[bc]
		if aw != bw {
			return !aw
		}
		return a.source.ConnID() < b.source.ConnID()
	})
	var cells [candidateGroupCells][]*pendingEntry
	ordered := make([]*pendingEntry, 0, len(rows))
	for _, e := range rows {
		if priorities[e] != PriorityNew {
			ordered = append(ordered, e)
		} else {
			cells[order[e]] = append(cells[order[e]], e)
		}
	}
	for len(ordered) < len(rows) {
		for offset := 0; offset < candidateGroupCells; offset++ {
			cell := (o.rrCell + offset) % candidateGroupCells
			if len(cells[cell]) == 0 {
				continue
			}
			ordered = append(ordered, cells[cell][0])
			cells[cell][0] = nil
			cells[cell] = cells[cell][1:]
		}
	}
	out := make([]candidateReservation, len(ordered))
	for i, e := range ordered {
		out[i] = o.handle(e)
	}
	return out
}

func TestOwnedRoundPlanFrozenLegacyOrder(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "insertion", true: "reversed-physical"}[reverse], func(t *testing.T) {
			f := ownedRoundNew(t, 12, Instant(600*time.Second))
			o := f.intake.dormantOwner.coordinator
			if reverse {
				o.mu.Lock()
				o.pending.mu.Lock()
				for i, j := 0, len(f.handles)-1; i < j; i, j = i+1, j-1 {
					o.pending.entries[i], o.pending.entries[j] = o.pending.entries[j], o.pending.entries[i]
				}
				o.pending.mu.Unlock()
				o.mu.Unlock()
			}
			want := ownedRoundFrozenOldOrder(t, f)
			ticket, err := f.attempt.prepareOwnedRoundPlan()
			if err != nil || ticket.claim != f.attempt.claim || ticket.claim.roundPlan == nil {
				t.Fatal("original historical plan", err)
			}
			p := ticket.claim.roundPlan
			got := make([]candidateReservation, p.count)
			for i := range got {
				got[i] = p.rows[p.order[i]].reservation
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("fixed order differs from frozen old reservation pointer/generation order")
			}
			if !reflect.DeepEqual(ownedRoundIDs(p), []domain.ConnID{3, 1, 2, 4, 5, 6, 10, 11, 9, 12, 7, 8}) {
				t.Fatal("explicit L1 L2 cell-turn Session-rotation witness differs")
			}
			if !p.rows[p.order[0]].beyondWatermark {
				t.Fatal("protected newcomer was dropped or relabelled instead of retained as historical")
			}
			if f.attempt.claim.phase.Load() != ownedAttemptLive {
				t.Fatal("planner did not restore LIVE")
			}
		})
	}
}

func TestOwnedRoundPlanOverflowAndFrozenNames(t *testing.T) {
	f := ownedRoundNew(t, 12, Instant(600*time.Second))
	ticket, err := f.attempt.prepareOwnedRoundPlan()
	if err != nil {
		t.Fatal(err)
	}
	p := ticket.claim.roundPlan
	before := *p
	for _, r := range p.rows[:p.count] {
		if (r.source.state.group == "U" || r.source.state.group == "V") && r.cell != candidateNamedGroups {
			t.Fatal("physical overflow groups got separate prospective cells")
		}
	}
	o := f.intake.dormantOwner.coordinator
	groupCommit(t, &o.groups, "U", f.members[len(f.members)-1].Identity(), Instant(600*time.Second), 512)
	if !reflect.DeepEqual(before, *p) {
		t.Fatal("historical cell/order changed after real later name assignment")
	}
}

func TestOwnedRoundPlanNoSharedAccountingEffect(t *testing.T) {
	for _, name := range []string{"success", "pre-cut", "missing-offered", "missing-target", "no-open", "repeated"} {
		t.Run(name, func(t *testing.T) {
			q := Instant(600 * time.Second)
			if name == "pre-cut" {
				q--
			}
			f := ownedRoundNew(t, 2, q)
			o := f.intake.dormantOwner.coordinator
			switch name {
			case "missing-offered":
				f.intake.dormantOwner.scratch.offeredCount = 0
			case "missing-target":
				f.intake.dormantOwner.scratch.targetCount = 0
			case "no-open":
				o.open = false
			}
			before := ownedScratchOldAccounting(f.intake, f.members)
			_, err := f.attempt.prepareOwnedRoundPlan()
			want := error(nil)
			switch name {
			case "pre-cut", "no-open":
				want = ErrNotReady
			case "missing-offered", "missing-target":
				want = ErrPrioritySnapshot
			}
			if !errors.Is(err, want) {
				t.Fatal("planning refusal reached wrong gate", name, err)
			}
			if name == "repeated" {
				if _, err = f.attempt.prepareOwnedRoundPlan(); !errors.Is(err, ErrInvalid) {
					t.Fatal("second plan overwrote same claim", err)
				}
			}
			if !reflect.DeepEqual(before, ownedScratchOldAccounting(f.intake, f.members)) || f.attempt.claim.phase.Load() != ownedAttemptLive {
				t.Fatal("planning changed shared accounting or lost LIVE")
			}
			if want != nil && f.attempt.claim.roundPlan != nil {
				t.Fatal("failed plan exposed partial references")
			}
		})
	}
}

func TestOwnedRoundPlanFullMembershipProjection(t *testing.T) {
	f := ownedRoundNew(t, 12, Instant(600*time.Second))
	ticket, err := f.attempt.prepareOwnedRoundPlan()
	if err != nil {
		t.Fatal(err)
	}
	p := ticket.claim.roundPlan
	if p.maximum != f.members[len(f.members)-1].ConnID() || len(f.cut.sealedCut.originals.members) != len(f.members) {
		t.Fatal("plan maximum lost target/nonoffered original")
	}
	for _, row := range p.rows[:p.count] {
		expected := PriorityNew
		if row.connID <= 3 {
			expected = PriorityProtected
		} else if row.connID <= 6 {
			expected = PriorityAged
		}
		if row.priority != expected || row.source != row.reservation.entry.source || row.target != row.reservation.entry.target || row.request != row.reservation.entry.request {
			t.Fatal("plan lost exact class/oldest-four/request/original Session")
		}
	}
	// The pure projection is independent of later caller slices, not a claim
	// that a ticket remains authoritative after any shared generation change.
	f.members[0] = Session{}
	if p.rows[0].source.state == nil {
		t.Fatal("plan retained caller-owned slice")
	}
}

func TestOwnedRoundPlanOriginalGenerationAndLane(t *testing.T) {
	t.Run("wrong-round", func(t *testing.T) {
		f := ownedRoundNew(t, 2, Instant(600*time.Second))
		f.handles[0].entry.intake.round++
		before := ownedScratchOldAccounting(f.intake, f.members)
		if _, err := f.attempt.prepareOwnedRoundPlan(); !errors.Is(err, ErrStaleWork) || f.attempt.claim.roundPlan != nil || !reflect.DeepEqual(before, ownedScratchOldAccounting(f.intake, f.members)) {
			t.Fatal("stale row round relabelled or changed shared state", err)
		}
	})
	t.Run("foreign-cut", func(t *testing.T) {
		f := ownedRoundNew(t, 1, Instant(600*time.Second))
		other, cut, _ := ownedScratchFixture(t, 1)
		_ = other
		if _, err := f.intake.tryOwnedAttempt(cut); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("foreign original cut borrowed", err)
		}
	})
	t.Run("actual-busy-before-owner-lock", func(t *testing.T) {
		f := ownedRoundNew(t, 2, Instant(600*time.Second))
		o := f.intake.dormantOwner.coordinator
		o.mu.Lock()
		done := make(chan error, 1)
		go func() { _, err := f.attempt.prepareOwnedRoundPlan(); done <- err }()
		deadline := time.After(5 * time.Second)
		timedOut := false
		for f.attempt.claim.phase.Load() != ownedAttemptBusy {
			select {
			case <-deadline:
				timedOut = true
			default:
			}
			if timedOut {
				break
			}
			runtime.Gosched()
		}
		var releaseErr, projectErr error
		if !timedOut {
			releaseErr = f.attempt.releaseOwnedAttempt()
			projectErr = f.attempt.projectOwnedMembership(nil, nil)
		}
		o.mu.Unlock()
		err := <-done
		if timedOut {
			t.Fatal("fixture could not reach pre-owner BUSY barrier")
		}
		if err != nil || !errors.Is(releaseErr, ErrNotReady) || !errors.Is(projectErr, ErrNotReady) || f.attempt.claim.phase.Load() != ownedAttemptLive {
			t.Fatal("copied wrapper released/modified original BUSY lane", err, releaseErr, projectErr)
		}
	})
}

func TestOwnedRoundPlanBoundsAndRelease(t *testing.T) {
	for _, n := range []int{0, 1, MaxPendingTotal} {
		t.Run(string(rune('a'+n%26)), func(t *testing.T) {
			f := ownedRoundNew(t, n, Instant(600*time.Second))
			ticket, err := f.attempt.prepareOwnedRoundPlan()
			if err != nil || ticket.claim.roundPlan == nil || ticket.claim.roundPlan.count != n {
				t.Fatal("complete 0/1/512 plan cardinality differs", n, err)
			}
			old := ticket.claim
			before := ownedScratchOldAccounting(f.intake, f.members)
			if err := f.attempt.releaseOwnedAttempt(); err != nil || old.roundPlan != nil || old.phase.Load() != ownedAttemptClosed {
				t.Fatal("release retained plan references or did not close original claim", err)
			}
			next := ownedScratchBorrow(t, f.intake, f.cut)
			if old == next.claim || ticket.claim.roundPlan != nil || !errors.Is(f.attempt.releaseOwnedAttempt(), ErrStaleWork) || !f.intake.dormantOwner.scratch.borrowed.Load() {
				t.Fatal("old ticket acquired or released new lane")
			}
			if !reflect.DeepEqual(before, ownedScratchOldAccounting(f.intake, f.members)) {
				t.Fatal("plan release refunded or altered existing accounting")
			}
		})
	}
}
