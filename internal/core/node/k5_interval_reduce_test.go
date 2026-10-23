package node

import (
	"testing"
	"time"
)

// Explicit synthetic certificates for the pure boundary; never actual workload
// authorization, UTC mapping qualification or proof of an observed S6 interval.
func k5TestSpan(a, b time.Duration) k5UTCInterval { return k5UTCInterval{int64(a), int64(b)} }
func k5TestFull(k k5SourceKey, a, b time.Duration) k5MappedFull {
	p := k5TestSpan(a, b)
	return k5MappedFull{key: k, mappingReference: "test-mapping", mappingValid: true, upperValid: true, definite: []k5UTCInterval{p}, possible: []k5UTCInterval{p}}
}
func k5TestLoad(k k5SourceKey, a, b time.Duration) k5ActualLoad {
	p := k5TestSpan(a, b)
	return k5ActualLoad{key: k, mappingReference: "independent-test-mapping", permissionReference: "prior-test-permission", actor: "actor", action: "action", attempt: "attempt", artifact: "artifact", observerReference: "actual-action-evidence", permittedActor: "actor", permittedAction: "action", permittedAttempt: "attempt", permittedArtifact: "artifact", actual: true, permitted: true, mappingValid: true, upperValid: true, coverageComplete: true, definite: []k5UTCInterval{p}, possible: []k5UTCInterval{p}}
}
func k5Assess(full []k5MappedFull, load []k5ActualLoad, covered bool) k5DayAssessment {
	return reduceK5UTCday("test-anchor", [16]byte{3}, 0, full, load, k5ReduceLimits{16, 16, 128}, covered, "test-day-coverage")
}

func TestK5UTCUnionDailyRule(t *testing.T) {
	k := k5TestKey(1)
	t.Run("overlap-nested-duplicates", func(t *testing.T) {
		f := k5TestFull(k, 0, 5000*time.Second)
		nested := k5TestFull(k, 100*time.Second, 700*time.Second)
		a, b := k5TestLoad(k, 0, 2000*time.Second), k5TestLoad(k, 1500*time.Second, 4000*time.Second)
		r := k5Assess([]k5MappedFull{f, f, nested}, []k5ActualLoad{a, b, a}, true)
		if r.verdict != "P_INVESTIGATE" || r.lower != 4000*time.Second {
			t.Fatal("duplicate/overlap counted more than once", r)
		}
	})
	t.Run("daily-sum-not-longest", func(t *testing.T) {
		f := []k5MappedFull{k5TestFull(k, 0, 2000*time.Second), k5TestFull(k, 4000*time.Second, 6000*time.Second)}
		r := k5Assess(f, []k5ActualLoad{k5TestLoad(k, 0, 7000*time.Second)}, true)
		if r.verdict != "P_INVESTIGATE" || r.lower != 4000*time.Second {
			t.Fatal("daily union replaced by longest segment", r)
		}
	})
	t.Run("one-full-two-load-witnesses", func(t *testing.T) {
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, 6000*time.Second)}, []k5ActualLoad{k5TestLoad(k, 0, 2000*time.Second), k5TestLoad(k, 3000*time.Second, 5000*time.Second)}, true)
		if r.lower != 4000*time.Second || r.verdict != "P_INVESTIGATE" {
			t.Fatal("load intersections not unioned", r)
		}
	})
	t.Run("exact3600-not-strictly-greater", func(t *testing.T) {
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, time.Hour)}, []k5ActualLoad{k5TestLoad(k, 0, time.Hour)}, true)
		if r.verdict != "NO_P_TRIGGER_IN_SCOPE" || r.lower != time.Hour || r.upper != time.Hour || !r.upperKnown {
			t.Fatal("strict owner comparison changed", r)
		}
	})
	t.Run("fractional-over3600", func(t *testing.T) {
		end := time.Hour + time.Nanosecond
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, end)}, []k5ActualLoad{k5TestLoad(k, 0, end)}, true)
		if r.verdict != "P_INVESTIGATE" || r.lower != end {
			t.Fatal("rounding lost strict fractional excess", r)
		}
	})
	t.Run("split-UTC-midnight", func(t *testing.T) {
		f := []k5MappedFull{k5TestFull(k, 235*time.Hour/10, 245*time.Hour/10)}
		b := []k5ActualLoad{k5TestLoad(k, 235*time.Hour/10, 245*time.Hour/10)}
		for _, day := range []int64{0, k5DayNanos} {
			r := reduceK5UTCday(k.anchor, k.configuration, day, f, b, k5ReduceLimits{2, 2, 8}, true, "certified-day")
			if r.lower != 30*time.Minute || r.upper != 30*time.Minute || r.verdict != "NO_P_TRIGGER_IN_SCOPE" {
				t.Fatal("UTC day clipping pooled midnight", day, r)
			}
		}
	})
}

func TestK5UTCProofCoverage(t *testing.T) {
	k := k5TestKey(1)
	t.Run("uncertain-bound-straddles", func(t *testing.T) {
		f := k5TestFull(k, 0, 3500*time.Second)
		f.possible = []k5UTCInterval{k5TestSpan(0, 3700*time.Second)}
		r := k5Assess([]k5MappedFull{f}, []k5ActualLoad{k5TestLoad(k, 0, 4000*time.Second)}, true)
		if r.verdict != "UNKNOWN" || r.lower != 3500*time.Second || r.upper != 3700*time.Second || !r.upperKnown {
			t.Fatal("uncertainty became a point verdict", r)
		}
	})
	t.Run("unvalidated-UTC-map", func(t *testing.T) {
		f := k5TestFull(k, 0, 2*time.Hour)
		f.mappingValid = false
		r := k5Assess([]k5MappedFull{f}, []k5ActualLoad{k5TestLoad(k, 0, 2*time.Hour)}, true)
		if r.verdict != "UNKNOWN" || r.lower != 0 || r.upperKnown {
			t.Fatal("duration source relabeled UTC", r)
		}
	})
	t.Run("scheduled-not-actual", func(t *testing.T) {
		b := k5TestLoad(k, 0, 2*time.Hour)
		b.actual = false
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, 2*time.Hour)}, []k5ActualLoad{b}, true)
		if r.verdict != "UNKNOWN" || r.lower != 0 {
			t.Fatal("scheduled intent became actual load", r)
		}
	})
	t.Run("auth-is-not-permission", func(t *testing.T) {
		b := k5TestLoad(k, 0, 2*time.Hour)
		b.permissionReference = ""
		b.permitted = false
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, 2*time.Hour)}, []k5ActualLoad{b}, true)
		if r.verdict != "UNKNOWN" || r.lower != 0 {
			t.Fatal("actor identity became benign permission", r)
		}
	})
	t.Run("wrong-original-attempt", func(t *testing.T) {
		b := k5TestLoad(k, 0, 2*time.Hour)
		b.attempt = "another-attempt"
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, 2*time.Hour)}, []k5ActualLoad{b}, true)
		if r.verdict != "UNKNOWN" || r.lower != 0 {
			t.Fatal("actual action detached from prior permission", r)
		}
	})
	t.Run("actor-gap-not-zero", func(t *testing.T) {
		b := k5TestLoad(k, 0, 30*time.Minute)
		b.coverageComplete = false
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, time.Hour)}, []k5ActualLoad{b}, true)
		if r.verdict != "UNKNOWN" || r.lower != 30*time.Minute || r.upperKnown {
			t.Fatal("missing actor coverage treated as zero", r)
		}
	})
	t.Run("proved-pre-gap-lower-kept", func(t *testing.T) {
		b := k5TestLoad(k, 0, time.Hour+time.Second)
		b.coverageComplete = false
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, 2*time.Hour)}, []k5ActualLoad{b}, false)
		if r.verdict != "P_INVESTIGATE" || r.lower != time.Hour+time.Second {
			t.Fatal("later gap discarded confirmed excess", r)
		}
	})
	t.Run("exact-hour-with-gap-unknown", func(t *testing.T) {
		r := k5Assess([]k5MappedFull{k5TestFull(k, 0, time.Hour)}, []k5ActualLoad{k5TestLoad(k, 0, time.Hour)}, false)
		if r.verdict != "UNKNOWN" || r.lower != time.Hour || r.upperKnown {
			t.Fatal("missing full-day coverage became absence", r)
		}
	})
}

func TestK5UTCScopeAndBounds(t *testing.T) {
	for _, kind := range []string{"wrong-kind", "wrong-anchor", "wrong-config", "wrong-epoch", "capacity", "reversed-endpoint", "different-epochs-past-pieces", "inconsistent-bounds"} {
		t.Run(kind, func(t *testing.T) {
			k := k5TestKey(1)
			f := k5TestFull(k, 0, 2*time.Hour)
			b := k5TestLoad(k, 0, 2*time.Hour)
			switch kind {
			case "wrong-kind":
				f.key.kind = k5HandshakeLoopbackShare
			case "wrong-anchor":
				f.key.anchor = "other-anchor"
			case "wrong-config":
				f.key.configuration = [16]byte{9}
			case "wrong-epoch":
				b.key.epoch = [16]byte{9}
			case "reversed-endpoint":
				f.definite[0] = k5UTCInterval{2, 1}
			case "inconsistent-bounds":
				f.possible = []k5UTCInterval{k5TestSpan(0, time.Second)}
			case "different-epochs-past-pieces":
				f = k5TestFull(k, 0, 2000*time.Second)
				b = k5TestLoad(k, 0, 2000*time.Second)
				other := k5TestKey(2)
				r := k5Assess([]k5MappedFull{f, k5TestFull(other, 3000*time.Second, 5000*time.Second)}, []k5ActualLoad{b, k5TestLoad(other, 3000*time.Second, 5000*time.Second)}, false)
				if r.verdict != "P_INVESTIGATE" || r.lower != 4000*time.Second {
					t.Fatal("separate certified past epochs bridged or discarded", r)
				}
				return
			}
			var r k5DayAssessment
			if kind == "capacity" {
				r = reduceK5UTCday(k.anchor, k.configuration, 0, []k5MappedFull{f, f}, []k5ActualLoad{b}, k5ReduceLimits{1, 1, 4}, true, "coverage")
			} else {
				r = k5Assess([]k5MappedFull{f}, []k5ActualLoad{b}, true)
			}
			if r.verdict != "UNKNOWN" || r.upperKnown {
				t.Fatal("incompatible or unbounded input received a verdict", kind, r)
			}
		})
	}
}
