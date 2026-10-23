package node

import (
	"sort"
	"time"
)

// Pure private boundary only. These are already-validated UTC certificates
// from a future independent mapper/actor, NOT generated from limiter identity,
// IP, auth or scheduled intent. This slice supplies neither certificate issuer.
// int64 endpoints are Unix nanoseconds in one certified UTC calendar/domain.
type k5UTCInterval struct{ start, end int64 }
type k5MappedFull struct {
	key                      k5SourceKey
	mappingReference         string
	mappingValid, upperValid bool
	definite, possible       []k5UTCInterval
}
type k5ActualLoad struct {
	key                                                                                        k5SourceKey
	mappingReference, permissionReference, actor, action, attempt, artifact, observerReference string
	permittedActor, permittedAction, permittedAttempt, permittedArtifact                       string
	actual, permitted, mappingValid, upperValid, coverageComplete                              bool
	definite, possible                                                                         []k5UTCInterval
}
type k5ReduceLimits struct{ full, load, pieces int }
type k5DayAssessment struct {
	verdict, reason        string
	lower, upper           time.Duration
	lowerValid, upperKnown bool
}
type k5Families struct{ fullDef, fullPoss, loadDef, loadPoss []k5UTCInterval }

const k5DayNanos = int64(24 * time.Hour)

func k5Union(in []k5UTCInterval) []k5UTCInterval {
	a := append([]k5UTCInterval(nil), in...)
	sort.Slice(a, func(i, j int) bool {
		if a[i].start == a[j].start {
			return a[i].end < a[j].end
		}
		return a[i].start < a[j].start
	})
	out := a[:0]
	for _, v := range a {
		if v.start == v.end {
			continue
		}
		if len(out) == 0 || v.start > out[len(out)-1].end {
			out = append(out, v)
		} else if v.end > out[len(out)-1].end {
			out[len(out)-1].end = v.end
		}
	}
	return out
}
func k5Intersect(a, b []k5UTCInterval) []k5UTCInterval {
	var out []k5UTCInterval
	for i, j := 0, 0; i < len(a) && j < len(b); {
		start, end := a[i].start, a[i].end
		if b[j].start > start {
			start = b[j].start
		}
		if b[j].end < end {
			end = b[j].end
		}
		if start < end {
			out = append(out, k5UTCInterval{start, end})
		}
		if a[i].end < b[j].end {
			i++
		} else {
			j++
		}
	}
	return out
}
func k5Contained(a, b []k5UTCInterval) bool {
	j := 0
	for _, v := range a {
		for j < len(b) && b[j].end <= v.start {
			j++
		}
		if j == len(b) || b[j].start > v.start || b[j].end < v.end {
			return false
		}
	}
	return true
}
func reduceK5UTCday(anchor string, configuration [16]byte, day int64, full []k5MappedFull, load []k5ActualLoad, limits k5ReduceLimits, wholeDayCovered bool, coverageReference string) k5DayAssessment {
	unknown := func(reason string) k5DayAssessment { return k5DayAssessment{verdict: "UNKNOWN", reason: reason} }
	if anchor == "" || configuration == ([16]byte{}) || day%k5DayNanos != 0 || day > int64(1<<63-1)-k5DayNanos {
		return unknown("invalid-day-or-scope")
	}
	if limits.full <= 0 || limits.load <= 0 || limits.pieces <= 0 || len(full) > limits.full || len(load) > limits.load {
		return unknown("input-capacity")
	}
	end := day + k5DayNanos
	pieces := 0
	count := func(a, b []k5UTCInterval) bool {
		if len(a) > limits.pieces-pieces {
			return false
		}
		pieces += len(a)
		if len(b) > limits.pieces-pieces {
			return false
		}
		pieces += len(b)
		return true
	}
	for _, v := range full {
		if !count(v.definite, v.possible) {
			return unknown("piece-capacity")
		}
	}
	for _, v := range load {
		if !count(v.definite, v.possible) {
			return unknown("piece-capacity")
		}
	}
	validKey := func(k k5SourceKey) bool {
		return k.kind == k5HandshakeNode && k.anchor == anchor && k.configuration == configuration && k.epoch != ([16]byte{}) && k.clockDomain != ([16]byte{})
	}
	clip := func(a []k5UTCInterval) ([]k5UTCInterval, bool) {
		var out []k5UTCInterval
		for _, v := range a {
			if v.end < v.start {
				return nil, false
			}
			if v.start < day {
				v.start = day
			}
			if v.end > end {
				v.end = end
			}
			if v.start < v.end {
				out = append(out, v)
			}
		}
		return k5Union(out), true
	}
	groups := make(map[k5SourceKey]*k5Families)
	group := func(k k5SourceKey) *k5Families {
		g := groups[k]
		if g == nil {
			g = &k5Families{}
			groups[k] = g
		}
		return g
	}
	gap := !wholeDayCovered || coverageReference == ""
	for _, v := range full {
		if !validKey(v.key) {
			return unknown("full-scope-mismatch")
		}
		if !v.mappingValid || v.mappingReference == "" {
			gap = true
			continue
		}
		d, ok := clip(v.definite)
		if !ok {
			return unknown("invalid-full-interval")
		}
		p, ok := clip(v.possible)
		if !ok {
			return unknown("invalid-full-interval")
		}
		if v.upperValid && !k5Contained(d, p) {
			return unknown("full-bounds-inconsistent")
		}
		g := group(v.key)
		g.fullDef = append(g.fullDef, d...)
		if v.upperValid {
			g.fullPoss = append(g.fullPoss, p...)
		} else {
			gap = true
		}
	}
	for _, v := range load {
		if !validKey(v.key) {
			return unknown("load-scope-mismatch")
		}
		if groups[v.key] == nil {
			gap = true
			continue
		} // No cross-epoch/config witness join.
		if !v.actual || !v.permitted || !v.mappingValid || v.mappingReference == "" || v.permissionReference == "" || v.actor == "" || v.action == "" || v.attempt == "" || v.artifact == "" || v.observerReference == "" || v.actor != v.permittedActor || v.action != v.permittedAction || v.attempt != v.permittedAttempt || v.artifact != v.permittedArtifact {
			gap = true
			continue
		}
		d, ok := clip(v.definite)
		if !ok {
			return unknown("invalid-load-interval")
		}
		p, ok := clip(v.possible)
		if !ok {
			return unknown("invalid-load-interval")
		}
		if v.upperValid && !k5Contained(d, p) {
			return unknown("load-bounds-inconsistent")
		}
		g := group(v.key)
		g.loadDef = append(g.loadDef, d...)
		if v.upperValid {
			g.loadPoss = append(g.loadPoss, p...)
		} else {
			gap = true
		}
		if !v.coverageComplete {
			gap = true
		}
	}
	var definite, possible []k5UTCInterval
	for _, g := range groups {
		definite = append(definite, k5Intersect(k5Union(g.fullDef), k5Union(g.loadDef))...)
		possible = append(possible, k5Intersect(k5Union(g.fullPoss), k5Union(g.loadPoss))...)
	}
	measure := func(a []k5UTCInterval) time.Duration {
		var n int64
		for _, v := range k5Union(a) {
			n += v.end - v.start
		} // Disjoint clipped pieces <= one24h day.
		return time.Duration(n)
	}
	lower := measure(definite)
	if lower > time.Hour {
		return k5DayAssessment{verdict: "P_INVESTIGATE", reason: "confirmed-lower-strictly-over3600", lower: lower, lowerValid: true}
	}
	if gap {
		return k5DayAssessment{verdict: "UNKNOWN", reason: "mapping-actor-or-coverage-gap", lower: lower, lowerValid: true}
	}
	upper := measure(possible)
	if upper < lower {
		return unknown("aggregate-bounds-inconsistent")
	}
	if upper > time.Hour {
		return k5DayAssessment{verdict: "UNKNOWN", reason: "threshold-straddled", lower: lower, upper: upper, lowerValid: true, upperKnown: true}
	}
	return k5DayAssessment{verdict: "NO_P_TRIGGER_IN_SCOPE", lower: lower, upper: upper, lowerValid: true, upperKnown: true}
}
