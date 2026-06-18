package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestHealthStore_RoutingRelevantStatesLockedFiltersToDeadAndCooled pins the
// narrowed Snapshot.Health contract: routingRelevantStatesLocked returns only
// Dead pairs and pairs inside an armed black-hole cooldown — the routing-
// relevant subset BestRoute / fetchRouteSummary act on — and omits the healthy
// majority so SnapshotIncremental stops deep-copying the whole set every 2s.
func TestHealthStore_RoutingRelevantStatesLockedFiltersToDeadAndCooled(t *testing.T) {
	s := newHealthStore()
	future := time.Now().Add(time.Minute)
	add := func(id, up string, h RouteHealth, cooldown time.Time) {
		key := healthKey{Identity: domaintest.ID(id), Uplink: domaintest.ID(up)}
		s.states[key] = &RouteHealthState{
			Identity:      domaintest.ID(id),
			Uplink:        domaintest.ID(up),
			Health:        h,
			CooldownUntil: cooldown,
		}
	}
	add("good", "up", HealthGood, time.Time{})
	add("questionable", "up", HealthQuestionable, time.Time{})
	add("bad", "up", HealthBad, time.Time{})
	add("dead", "up", HealthDead, time.Time{})
	add("cooled-good", "up", HealthGood, future) // healthy label but cooled → relevant

	got := s.routingRelevantStatesLocked()
	if len(got) != 2 {
		t.Fatalf("want 2 routing-relevant states (dead + cooled), got %d: %+v", len(got), got)
	}
	for _, st := range got {
		isDead := st.Health == HealthDead
		isCooled := !st.CooldownUntil.IsZero()
		if !isDead && !isCooled {
			t.Fatalf("returned a state that is neither Dead nor cooled: %+v", st)
		}
	}
}

func TestHealthStore_RoutingRelevantStatesLockedEmptyWhenAllHealthy(t *testing.T) {
	s := newHealthStore()
	if got := s.routingRelevantStatesLocked(); got != nil {
		t.Fatalf("empty store must return nil, got %+v", got)
	}
	s.states[healthKey{Identity: domaintest.ID("d"), Uplink: domaintest.ID("u")}] = &RouteHealthState{
		Identity: domaintest.ID("d"),
		Uplink:   domaintest.ID("u"),
		Health:   HealthGood,
	}
	if got := s.routingRelevantStatesLocked(); len(got) != 0 {
		t.Fatalf("only-healthy store → no routing-relevant states, got %d", len(got))
	}
}

// TestSnapshotBestRoute_ExcludesDeadUplinkWithNarrowedHealth proves the data-
// plane consumer is unaffected by the narrowed Snapshot.Health: BestRoute still
// excludes a Dead uplink even though Snapshot.Health now carries ONLY the Dead
// subset (the healthy alternative uplink is absent, exactly as
// routingRelevantStatesLocked produces it). The Dead route has the lower hop
// count, so without the exclusion it would win — the test fails if narrowing
// dropped the signal BestRoute needs.
func TestSnapshotBestRoute_ExcludesDeadUplinkWithNarrowedHealth(t *testing.T) {
	dest := domaintest.ID("dest")
	deadUp := domaintest.ID("up-dead")
	liveUp := domaintest.ID("up-live")

	snap := Snapshot{
		TakenAt: time.Now(),
		Routes: map[PeerIdentity][]RouteEntry{
			dest: {
				{Identity: dest, Origin: domaintest.ID("o1"), NextHop: deadUp, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement},
				{Identity: dest, Origin: domaintest.ID("o2"), NextHop: liveUp, Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement},
			},
		},
		// Narrowed Health: only the Dead pair is present, the healthy live
		// uplink omitted — as routingRelevantStatesLocked would produce.
		Health: []RouteHealthState{
			{Identity: dest, Uplink: deadUp, Health: HealthDead},
		},
	}

	best := snap.BestRoute(dest)
	if best == nil {
		t.Fatal("BestRoute returned nil; expected the live-uplink route")
	}
	if best.NextHop != liveUp {
		t.Fatalf("BestRoute must skip the Dead uplink (lower hops) and pick the live one; got NextHop=%s", best.NextHop)
	}
}

// health_questionable_targets_test.go pins questionableTargetsLocked (and the
// Table.QuestionableHealthTargets wrapper probeTick uses): it must return
// exactly the Questionable (Identity, Uplink) pairs and nothing else, so the
// probe scheduler stops deep-copying the full health set every tick.

func TestHealthStore_QuestionableTargetsLockedFiltersToQuestionable(t *testing.T) {
	s := newHealthStore()
	add := func(id, up string, h RouteHealth) {
		key := healthKey{Identity: domaintest.ID(id), Uplink: domaintest.ID(up)}
		s.states[key] = &RouteHealthState{
			Identity: domaintest.ID(id),
			Uplink:   domaintest.ID(up),
			Health:   h,
		}
	}
	add("dest-good", "up1", HealthGood)
	add("dest-q1", "up1", HealthQuestionable)
	add("dest-q2", "up2", HealthQuestionable)
	add("dest-bad", "up1", HealthBad)
	add("dest-dead", "up1", HealthDead)

	targets := s.questionableTargetsLocked()
	if len(targets) != 2 {
		t.Fatalf("want 2 questionable targets, got %d: %+v", len(targets), targets)
	}
	for _, tgt := range targets {
		st, ok := s.states[healthKey(tgt)]
		if !ok {
			t.Fatalf("returned target %+v not present in store", tgt)
		}
		if st.Health != HealthQuestionable {
			t.Fatalf("returned non-questionable target %+v (health=%s)", tgt, st.Health)
		}
	}
}

func TestHealthStore_QuestionableTargetsLockedEmptyCases(t *testing.T) {
	s := newHealthStore()
	if got := s.questionableTargetsLocked(); got != nil {
		t.Fatalf("empty store must return nil, got %+v", got)
	}

	// A store with only non-Questionable states yields no targets.
	s.states[healthKey{Identity: domaintest.ID("d"), Uplink: domaintest.ID("u")}] = &RouteHealthState{
		Identity: domaintest.ID("d"),
		Uplink:   domaintest.ID("u"),
		Health:   HealthGood,
	}
	if got := s.questionableTargetsLocked(); len(got) != 0 {
		t.Fatalf("no Questionable pairs → empty result, got %d", len(got))
	}
}
