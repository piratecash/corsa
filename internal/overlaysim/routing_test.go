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
	// HopsByPair keeps the successful lengths per pair index, so two graphs can
	// later be compared on the pairs that succeeded in BOTH.
	HopsByPair map[int]int
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
		Pairs:      len(pairs),
		Outcomes:   map[routingOutcome]int{},
		HopsByPair: map[int]int{},
	}
	hops := make([]int, 0, len(pairs))

	for index, pair := range pairs {
		result, err := greedyRoute(g, inGraph, component, pair[0], pair[1], budget)
		if err != nil {
			return routingReport{}, err
		}
		report.Outcomes[result.Outcome]++
		if result.Outcome == routingSuccess {
			hops = append(hops, result.Hops)
			report.HopsByPair[index] = result.Hops
		}
	}
	report.Lengths = summariseLengths(hops)
	return report, nil
}

// comparedLengths is M2-L and M2-G: the two graphs compared ON THE PAIRS THAT
// SUCCEEDED IN BOTH. ⚠️ Comparing the full sets would compare different pair
// populations and call the difference a length difference.
type comparedLengths struct {
	Common int
	Full   lengthStats
	Half   lengthStats
}

// Ratio is M2-G, the normative "half / whole network" figure. It is a string
// because "no data" is a legitimate answer and must not arrive as 0.0.
func (c comparedLengths) Ratio() string {
	if c.Common == 0 || c.Full.Median == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.2f", float64(c.Half.Median)/float64(c.Full.Median))
}

func compareLengths(full, half routingReport) comparedLengths {
	fullHops := make([]int, 0, len(full.HopsByPair))
	halfHops := make([]int, 0, len(half.HopsByPair))

	for index, hopsFull := range full.HopsByPair {
		hopsHalf, both := half.HopsByPair[index]
		if !both {
			continue
		}
		fullHops = append(fullHops, hopsFull)
		halfHops = append(halfHops, hopsHalf)
	}

	return comparedLengths{
		Common: len(fullHops),
		Full:   summariseLengths(fullHops),
		Half:   summariseLengths(halfHops),
	}
}
