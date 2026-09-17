package overlaysim

// routing_test.go is the greedy-XOR routing measurer of
// docs/refactoring/dht/21-m1-candidate-c1.md §5.3, and nothing more: it walks a
// graph that is already built and never changes it.
//
// Why it exists: the policy comparison could say whether the structural half is
// CONNECTED, and could not say whether it is USABLE FOR ROUTING. Those are
// different questions — a graph can be one component and still leave a greedy
// search stuck — so the answer needs its own instrument.
//
// ⚠️ This is the instrument only. No candidate is measured here, no threshold is
// proposed, and the final C1 runs happen after the owner agrees (index §0.2).

import (
	"fmt"
	"sort"
)

// routingOutcome is one of FOUR results, kept apart on purpose: mixing any two
// of them hides the difference the measurement exists to show.
type routingOutcome int

const (
	// routingSuccess — the search reached the target.
	routingSuccess routingOutcome = iota

	// routingDeadEnd — a local minimum: no neighbour is strictly closer to the
	// target, and this node is not the target. The graph may still be
	// connected; that is the point of measuring it.
	routingDeadEnd

	// routingNoPath — the target is in another component. Established BEFORE
	// the walk, so it can never be confused with a dead end.
	routingNoPath

	// routingBudgetSpent — an OPTIONAL experimental hop budget ran out. ⚠️ A
	// result, not a defect: strict distance decrease forbids cycles but does
	// NOT promise a short path (the chain fixture walks 63 hops legitimately).
	routingBudgetSpent
)

func (o routingOutcome) String() string {
	switch o {
	case routingSuccess:
		return "success"
	case routingDeadEnd:
		return "dead end"
	case routingNoPath:
		return "no path"
	default:
		return "budget spent"
	}
}

// routingResult is one search: what happened and after how many PERFORMED
// transitions.
type routingResult struct {
	Outcome routingOutcome
	// Hops counts transitions actually made. A transition that the budget
	// forbids is not made, so it is not counted either.
	Hops int
	// Stopped is where the walk ended; for routingNoPath it is the source,
	// because no step was taken.
	Stopped int32
}

// noBudget asks for no hop limit at all. The correctness bound of N-1 still
// applies — see greedyRoute.
const noBudget = -1

// greedyRoute walks from source to target, moving only to a neighbour that is
// STRICTLY closer to the target, and among those to the closest one.
//
// `inGraph` selects the graph being routed through: everyone, or only the
// structural half. `component` is the precomputed component label per node in
// THAT graph (-1 for non-members) — reachability is a property of the graph, not
// of the walk, and asking it per pair with a fresh BFS would make the sweep
// unrunnable.
//
// `budget` limits PERFORMED transitions; noBudget means no limit. ⚠️ Reaching
// the target with the last permitted transition is a SUCCESS: the budget is
// spent only when the walk is still unfinished after it.
//
// The error return is for stand defects, not for search outcomes: a revisited
// node or more than N-1 transitions cannot happen while distance strictly
// decreases, so either means the measurer is broken.
func greedyRoute(
	g *graph, inGraph func(int32) bool, component []int, source, target int32, budget int,
) (routingResult, error) {
	if !inGraph(source) || !inGraph(target) {
		return routingResult{}, fmt.Errorf("pair %d→%d is not inside the graph being routed",
			source, target)
	}
	if component[source] != component[target] {
		return routingResult{Outcome: routingNoPath, Stopped: source}, nil
	}

	nodes := len(g.adjacency)
	visited := make(map[int32]struct{}, 16)
	visited[source] = struct{}{}

	current, hops := source, 0
	for {
		if current == target {
			return routingResult{Outcome: routingSuccess, Hops: hops, Stopped: current}, nil
		}

		best, bestDistance := int32(-1), g.ids[current]
		for _, next := range g.adjacency[current] {
			if !inGraph(next) {
				continue
			}
			// Strictly closer than where we stand, then closest among those.
			if !xorLess(g.ids[target], g.ids[next], bestDistance) {
				continue
			}
			best, bestDistance = next, g.ids[next]
		}
		if best == -1 {
			return routingResult{Outcome: routingDeadEnd, Hops: hops, Stopped: current}, nil
		}

		// ⚠️ The budget is checked BEFORE the transition that would exceed it,
		// so that transition is never performed.
		if budget != noBudget && hops == budget {
			return routingResult{Outcome: routingBudgetSpent, Hops: hops, Stopped: current}, nil
		}

		if _, seen := visited[best]; seen {
			return routingResult{}, fmt.Errorf(
				"search %d→%d revisited node %d after %d hops — impossible while the distance "+
					"strictly decreases, so the measurer is broken", source, target, best, hops)
		}
		visited[best] = struct{}{}

		current, hops = best, hops+1
		if hops > nodes-1 {
			return routingResult{}, fmt.Errorf(
				"search %d→%d made %d transitions in a graph of %d nodes — over the N-1 bound",
				source, target, hops, nodes)
		}
	}
}

// --- aggregation ------------------------------------------------------------

// lengthStats summarises path lengths. ⚠️ An empty sample reports NO DATA rather
// than zero: "zero hops" and "nothing was measured" are different statements,
// and a report that prints 0 for both invites the wrong reading.
type lengthStats struct {
	Pairs            int
	Median, P90, Max int
}

func (s lengthStats) String() string {
	if s.Pairs == 0 {
		return "no data"
	}
	return fmt.Sprintf("median %d, p90 %d, max %d (%d pairs)", s.Median, s.P90, s.Max, s.Pairs)
}

func summariseLengths(hops []int) lengthStats {
	if len(hops) == 0 {
		return lengthStats{}
	}
	sorted := append([]int(nil), hops...)
	sort.Ints(sorted)

	at := func(q float64) int {
		index := int(q * float64(len(sorted)-1))
		return sorted[index]
	}
	return lengthStats{
		Pairs:  len(sorted),
		Median: at(0.5),
		P90:    at(0.9),
		Max:    sorted[len(sorted)-1],
	}
}

// routingReport is one graph measured over one set of pairs.
type routingReport struct {
	Pairs    int
	Outcomes map[routingOutcome]int
	Lengths  lengthStats
	// ByPair keeps the FULL result of every pair, in sample order: the outcome
	// and the transitions performed, refusals included.
	//
	// ⚠️ It replaced a map that held the SUCCESSFUL lengths only, and the
	// difference is not bookkeeping. Two things are impossible without the
	// refusals: (a) comparing two graphs pair by pair on anything but success,
	// and (b) answering what the same searches would have done under a hop
	// budget L — a dead end after four hops and a success after forty are the
	// same "absent from the map", and they behave differently at L = 18. The
	// experimental limit of §5.3 is chosen after seeing the lengths, so the
	// record has to survive the run that produced them.
	ByPair []routingResult
	// Budget is the hop budget this report was measured under, so a recomputed
	// view cannot be taken from an already limited run — see underHopLimit.
	Budget int
}

func (r routingReport) share(outcome routingOutcome) string {
	if r.Pairs == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.1f%%", float64(r.Outcomes[outcome])/float64(r.Pairs)*100)
}

// measureRouting runs every pair through one graph.
func measureRouting(
	g *graph, inGraph func(int32) bool, component []int, pairs [][2]int32, budget int,
) (routingReport, error) {
	report := routingReport{
		Pairs:    len(pairs),
		Outcomes: map[routingOutcome]int{},
		ByPair:   make([]routingResult, 0, len(pairs)),
		Budget:   budget,
	}
	hops := make([]int, 0, len(pairs))

	for _, pair := range pairs {
		result, err := greedyRoute(g, inGraph, component, pair[0], pair[1], budget)
		if err != nil {
			return routingReport{}, err
		}
		report.Outcomes[result.Outcome]++
		report.ByPair = append(report.ByPair, result)
		if result.Outcome == routingSuccess {
			hops = append(hops, result.Hops)
		}
	}
	report.Lengths = summariseLengths(hops)
	return report, nil
}

// underHopLimit answers what the SAME searches would have done under a hop
// budget, without walking any of them again.
//
// ⚠️ It is only valid because greedy routing with a budget is the unlimited walk
// truncated: the rule at each step looks at the current node and the target and
// at nothing else, so the first L transitions are the same ones whatever the
// budget is, and the budget is checked BEFORE a transition it would forbid. So a
// pair that finished within L finished identically, and a pair that needed more
// stops with the budget spent after exactly L transitions.
//
// ⚠️ That argument is an argument. It is PROVEN by reference instead: for every
// fixture and every limit, this function's output is compared against actually
// re-running the searches under that limit, dead ends and the exact boundary
// included (routing_reference_test.go).
//
// What it deliberately does NOT reconstruct is WHERE a truncated walk stopped:
// the stopping node is not derivable from an outcome and a hop count, so every
// recomputed result carries Stopped = -1 rather than a plausible guess.
func underHopLimit(report routingReport, limit int) (routingReport, error) {
	if report.Budget != noBudget {
		return routingReport{}, fmt.Errorf(
			"cannot recompute a hop limit from a report already measured under budget %d: its "+
				"searches were cut short, so what they would have done past that point is not in "+
				"the record", report.Budget)
	}
	if limit < 0 {
		return routingReport{}, fmt.Errorf("hop limit %d is negative", limit)
	}

	limited := routingReport{
		Pairs:    report.Pairs,
		Outcomes: map[routingOutcome]int{},
		ByPair:   make([]routingResult, 0, len(report.ByPair)),
		Budget:   limit,
	}
	hops := make([]int, 0, len(report.ByPair))

	for _, result := range report.ByPair {
		if result.Outcome == routingNoPath {
			// Reachability is a property of the graph, not of the walk: the
			// answer is settled before a step is taken, so no limit can turn it
			// into a budget refusal.
			//
			// ⚠️ The guard below is BELT AND BRACES and a mutation proved it:
			// removing "not no-path" from the condition changes nothing, because
			// an unreachable pair carries zero hops and zero is never past a
			// limit. So the property is asserted where it can actually fail —
			// on the hop count itself — rather than defended by a condition that
			// cannot be made red.
			if result.Hops != 0 {
				return routingReport{}, fmt.Errorf(
					"a pair reported as unreachable carries %d transitions — no step is taken "+
						"when the target is in another component, so the measurer is broken",
					result.Hops)
			}
			limited.Outcomes[result.Outcome]++
			limited.ByPair = append(limited.ByPair, result)
			continue
		}

		// ⚠️ A pair the limit did not touch keeps EVERYTHING it had, the stopping
		// node included: the walk is literally the same walk. Only a truncated
		// pair loses it, because where a walk would have been after L
		// transitions is not derivable from an outcome and a hop count — and a
		// plausible guess there would be worse than an admitted gap.
		truncated := result
		if result.Hops > limit {
			truncated = routingResult{Outcome: routingBudgetSpent, Hops: limit, Stopped: -1}
		}

		limited.Outcomes[truncated.Outcome]++
		limited.ByPair = append(limited.ByPair, truncated)
		if truncated.Outcome == routingSuccess {
			hops = append(hops, truncated.Hops)
		}
	}
	limited.Lengths = summariseLengths(hops)
	return limited, nil
}

// comparedLengths is M2-L and M2-G: the two graphs compared ON THE PAIRS THAT
// SUCCEEDED IN BOTH. ⚠️ Comparing the full sets would compare different pair
// populations and call the difference a length difference.
type comparedLengths struct {
	Common int
	Full   lengthStats
	Half   lengthStats
	// Mismatch is set when the two reports did not come from one pair sample.
	// ⚠️ A stand defect rather than a result, and it must surface where the
	// number would have been rather than as a quietly shorter comparison.
	Mismatch string
}

// Ratio is M2-G, the normative "half / whole network" figure. It is a string
// because "no data" is a legitimate answer and must not arrive as 0.0.
func (c comparedLengths) Ratio() string {
	if c.Mismatch != "" {
		return "STAND DEFECT: " + c.Mismatch
	}
	if c.Common == 0 || c.Full.Median == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.2f", float64(c.Half.Median)/float64(c.Full.Median))
}

func compareLengths(full, half routingReport) comparedLengths {
	fullHops := make([]int, 0, len(full.ByPair))
	halfHops := make([]int, 0, len(half.ByPair))

	// ⚠️ Pair INDEX is the join key, and the two reports must therefore come
	// from one sample. That is the §5.3 rule "one pair sample for both graphs",
	// and a difference in length means the two were NOT measured on one sample —
	// a stand defect, not a shorter comparison. Returning what the shorter one
	// happens to cover would compare two graphs on pairs only one of them was
	// asked about.
	if len(full.ByPair) != len(half.ByPair) {
		return comparedLengths{Mismatch: fmt.Sprintf(
			"%d pairs in the full graph against %d in the half — the two were not measured on "+
				"one sample", len(full.ByPair), len(half.ByPair))}
	}

	for index, resultFull := range full.ByPair {
		resultHalf := half.ByPair[index]
		if resultFull.Outcome != routingSuccess || resultHalf.Outcome != routingSuccess {
			continue
		}
		fullHops = append(fullHops, resultFull.Hops)
		halfHops = append(halfHops, resultHalf.Hops)
	}

	return comparedLengths{
		Common: len(fullHops),
		Full:   summariseLengths(fullHops),
		Half:   summariseLengths(halfHops),
	}
}
