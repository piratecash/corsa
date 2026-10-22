package findnode

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

func ownedBridgePoint(t *testing.T, d findnodebridge.FixtureDomain) (findnodebridge.CaptureStamp, findnodebridge.OwnedInstant) {
	t.Helper()
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	at, err := lease.Sample()
	lease.Release()
	if err != nil {
		t.Fatal(err)
	}
	return stamp, at
}
func ownedBridgeBoundary(t *testing.T, at findnodebridge.OwnedInstant, delta time.Duration) findnodebridge.OwnedBoundary {
	t.Helper()
	bound, err := at.BoundaryAfter(delta)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}
func ownedBridgeRefusal(t *testing.T, got OwnedCut, err error) {
	t.Helper()
	if err == nil || got != (OwnedCut{}) {
		t.Fatal("refused Seal returned partial cut", got, err)
	}
}

// TLS proofs are genuine; opened/ConnID/group here are logical fixture inputs,
// not a claim about Node membership or Session same-domain provenance.
func TestOwnedBridgeCutCopiesAllOriginalsAndClasses(t *testing.T) {
	d, clock, _ := ownedOriginalDomain(t)
	clock.Store(int64(650 * time.Second))
	consumer := d.Consumer()
	intake, sink, err := NewDormantFixtureIntake(consumer, ownedBridgeProfile(), 8)
	if err != nil {
		t.Fatal(err)
	}
	stamp, at := ownedBridgePoint(t, d)
	bound := ownedBridgeBoundary(t, at, 600*time.Second)
	classes := []SessionClass{ClassContact, ClassSelected, ClassIncomingPinned, ClassOrdinary, ClassOrdinary, ClassOrdinary}
	members := make([]ObservedMember, len(classes))
	wantSessions := make([]Session, len(classes))
	wantClasses := append([]SessionClass(nil), classes...)
	for index, class := range classes {
		member := ownedOriginalSession(t, domain.ConnID(index+1), Instant(time.Duration(100+index)*time.Second))
		members[index] = ObservedMember{Session: member, Class: class}
		wantSessions[index] = member
	}
	owner := intake.dormantOwner
	coordinator := owner.coordinator
	capacity := *coordinator.capacity
	sourceOwner := coordinator.sourceOwner
	before := coordinator.snapshot()
	cut, err := sink.Seal(stamp, at, bound, members, ObservedDegree{Outgoing: 2, Incoming: 3, Window: 5})
	if err != nil {
		t.Fatal(err)
	}
	if cut.sealedCut == nil || cut.sealedCut.owner != owner || cut.sealedCut.stamp != stamp || cut.sealedCut.at != at || cut.sealedCut.owner.consumer != consumer || !reflect.DeepEqual(cut.sealedCut.originals.members, wantSessions) || !reflect.DeepEqual(cut.sealedCut.classes, wantClasses) || cut.sealedCut.degree != (ObservedDegree{Outgoing: 2, Incoming: 3, Window: 5}) {
		t.Fatal("cut lost complete original positional evidence")
	}
	copied := cut
	for index := range members {
		members[index] = ObservedMember{}
		classes[index] = ClassContact
	}
	if copied.sealedCut != cut.sealedCut || !reflect.DeepEqual(cut.sealedCut.originals.members, wantSessions) || !reflect.DeepEqual(copied.sealedCut.classes, wantClasses) {
		t.Fatal("cut borrowed caller DTO/classes or dropped tail originals")
	}
	// Every young Ordinary contributes, including the nonoldest tail; class0 is
	// protected and must never replace positional classifications.
	if cut.sealedCut.validBefore.Elapsed() != 703*time.Second {
		t.Fatal("mixed positional class frontier changed", cut.sealedCut.validBefore.Elapsed())
	}
	again, err := sink.Seal(stamp, at, bound, nil, ObservedDegree{})
	if err != nil || again.sealedCut.owner != owner || again.sealedCut.originals.members == nil {
		t.Fatal("valid empty prepared cut lost owner", err)
	}
	bad, err := sink.Seal(stamp, at, bound, []ObservedMember{{}}, ObservedDegree{})
	ownedBridgeRefusal(t, bad, err)
	if owner.coordinator != coordinator || coordinator.sourceOwner != sourceOwner || *coordinator.capacity != capacity || !reflect.DeepEqual(before, coordinator.snapshot()) {
		t.Fatal("pure Seal changed source/capacity/debt or reconstructed owner")
	}
}

func TestOwnedBridgeCutSourceFrontiersExclusive(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		q, opened, history, want time.Duration
		class                    SessionClass
	}{
		{"young-ordinary", 650 * time.Second, 100 * time.Second, 600 * time.Second, 700 * time.Second, ClassOrdinary},
		{"history-protected", 650 * time.Second, 100 * time.Second, 50 * time.Second, 700 * time.Second, ClassContact},
		{"epoch-protected", 650 * time.Second, 100 * time.Second, 600 * time.Second, 1200 * time.Second, ClassSelected},
		{"next-epoch-at-equality", 600 * time.Second, 0, 1000 * time.Second, 1200 * time.Second, ClassIncomingPinned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, clock, _ := ownedOriginalDomain(t)
			clock.Store(int64(tc.q))
			_, sink, err := NewDormantFixtureIntake(d.Consumer(), ownedBridgeProfile(), 2)
			if err != nil {
				t.Fatal(err)
			}
			stamp, at := ownedBridgePoint(t, d)
			member := ownedOriginalSession(t, 1, Instant(tc.opened))
			got, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, tc.history), []ObservedMember{{Session: member, Class: tc.class}}, ObservedDegree{})
			if err != nil {
				t.Fatal("source frontier construction failed", err)
			}
			clock.Store(int64(tc.want - 1))
			_, before := ownedBridgePoint(t, d)
			clock.Store(int64(tc.want))
			_, equal := ownedBridgePoint(t, d)
			if !got.sealedCut.validBefore.Contains(before) || got.sealedCut.validBefore.Contains(equal) {
				t.Fatal("exclusive source transition accepted equality")
			}
			if got.sealedCut.validBefore.Elapsed() != tc.want {
				t.Fatal("source frontier differs", got.sealedCut.validBefore.Elapsed())
			}
		})
	}
}

func TestOwnedBridgeCutRejectsForeignAndInvalidEvidence(t *testing.T) {
	d, clock, _ := ownedOriginalDomain(t)
	clock.Store(int64(time.Second))
	_, sink, err := NewDormantFixtureIntake(d.Consumer(), ownedBridgeProfile(), 2)
	if err != nil {
		t.Fatal(err)
	}
	stamp, at := ownedBridgePoint(t, d)
	bound := ownedBridgeBoundary(t, at, time.Second)
	foreign, fclock, _ := ownedOriginalDomain(t)
	fclock.Store(int64(time.Second))
	fs, fa := ownedBridgePoint(t, foreign)
	fb := ownedBridgeBoundary(t, fa, time.Second)
	good := ownedOriginalSession(t, 1, 0)
	otherSameID := ownedOriginalSession(t, 1, 0)
	future := ownedOriginalSession(t, 2, Instant(2*time.Second))
	closed := ownedOriginalSession(t, 3, 0)
	closed.End()
	for _, tc := range []struct {
		name    string
		sink    ObservationSink
		stamp   findnodebridge.CaptureStamp
		at      findnodebridge.OwnedInstant
		bound   findnodebridge.OwnedBoundary
		members []ObservedMember
		degree  ObservedDegree
	}{
		{"zero-sink", ObservationSink{}, stamp, at, bound, nil, ObservedDegree{}},
		{"foreign-stamp", sink, fs, at, bound, nil, ObservedDegree{}}, {"foreign-point", sink, stamp, fa, bound, nil, ObservedDegree{}}, {"foreign-bound", sink, stamp, at, fb, nil, ObservedDegree{}},
		{"equal-bound", sink, stamp, at, ownedBridgeBoundary(t, at, 0), nil, ObservedDegree{}},
		{"zero-session", sink, stamp, at, bound, []ObservedMember{{Class: ClassOrdinary}}, ObservedDegree{}},
		{"closed-original", sink, stamp, at, bound, []ObservedMember{{Session: closed, Class: ClassOrdinary}}, ObservedDegree{}},
		{"duplicate-original", sink, stamp, at, bound, []ObservedMember{{Session: good, Class: ClassOrdinary}, {Session: otherSameID, Class: ClassContact}}, ObservedDegree{}},
		{"future-opened", sink, stamp, at, bound, []ObservedMember{{Session: future, Class: ClassOrdinary}}, ObservedDegree{}},
		{"unknown-class", sink, stamp, at, bound, []ObservedMember{{Session: good, Class: SessionClass(255)}}, ObservedDegree{}},
		{"M-plus-one", sink, stamp, at, bound, []ObservedMember{{Session: good, Class: ClassOrdinary}, {Session: good, Class: ClassOrdinary}, {Session: good, Class: ClassOrdinary}}, ObservedDegree{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.sink.Seal(tc.stamp, tc.at, tc.bound, tc.members, tc.degree)
			ownedBridgeRefusal(t, got, err)
		})
	}
	// Negative corruption only: never a manufactured positive proof/open instant.
	originalOpened := good.state.opened
	good.state.opened = -1
	got, err := sink.Seal(stamp, at, bound, []ObservedMember{{Session: good, Class: ClassOrdinary}}, ObservedDegree{})
	good.state.opened = originalOpened
	ownedBridgeRefusal(t, got, err)
	for _, degree := range []ObservedDegree{{Outgoing: -1}, {Incoming: -1}, {Window: -1}, {Outgoing: 65, Window: 65}, {Incoming: 65, Window: 65}, {Outgoing: math.MaxInt, Incoming: math.MaxInt, Window: 64}, {Outgoing: 1, Window: 0}, {Window: 65}} {
		got, err := sink.Seal(stamp, at, bound, nil, degree)
		if !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid degree not rejected before addition", degree, err)
		}
		ownedBridgeRefusal(t, got, err)
	}
}

func TestOwnedBridgeCutOverflowAndHistoricalGeneration(t *testing.T) {
	t.Run("checked-finite-frontiers", func(t *testing.T) {
		d, clock, _ := ownedOriginalDomain(t)
		clock.Store(math.MaxInt64 - 1)
		_, sink, err := NewDormantFixtureIntake(d.Consumer(), ownedBridgeProfile(), 1)
		if err != nil {
			t.Fatal(err)
		}
		stamp, at := ownedBridgePoint(t, d)
		maxBoundary := ownedBridgeBoundary(t, at, 1)
		got, err := sink.Seal(stamp, at, maxBoundary, nil, ObservedDegree{})
		ownedBridgeRefusal(t, got, err)
		member := ownedOriginalSession(t, 1, Instant(math.MaxInt64-1))
		got, err = sink.Seal(stamp, at, maxBoundary, []ObservedMember{{Session: member, Class: ClassOrdinary}}, ObservedDegree{})
		ownedBridgeRefusal(t, got, err)
	})
	t.Run("historical-not-current-authority", func(t *testing.T) {
		d, _, _ := ownedOriginalDomain(t)
		_, sink, err := NewDormantFixtureIntake(d.Consumer(), ownedBridgeProfile(), 1)
		if err != nil {
			t.Fatal(err)
		}
		stamp, at := ownedBridgePoint(t, d)
		bound := ownedBridgeBoundary(t, at, time.Second)
		cut, err := sink.Seal(stamp, at, bound, nil, ObservedDegree{})
		if err != nil {
			t.Fatal(err)
		}
		publication := d.Publisher().BeginPublication()
		publication.Complete()
		if lease, err := d.Consumer().TryReadClock(stamp); err == nil {
			lease.Release()
			t.Fatal("writer failed to stale original historical stamp")
		}
		if cut.sealedCut.stamp != stamp || cut.sealedCut.at != at {
			t.Fatal("historical evidence was silently recaptured")
		}
		// Matches preserves identity and old q; Seal is deliberately not a lease.
		again, err := sink.Seal(stamp, at, bound, nil, ObservedDegree{})
		if err != nil || again.sealedCut.stamp != stamp {
			t.Fatal("historical Seal incorrectly promised generation validation", err)
		}
	})
}
