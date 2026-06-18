package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

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
