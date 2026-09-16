package overlaysim

// m5_test.go is the M5 measurer of
// docs/refactoring/dht/21-anonymity-transport.md §4.3.4″.6: the share of TARGETS
// that have no `Q` neighbour at all.
//
// Why the contract is written out before the code: M5 and "isolated node of the
// Q-subgraph" are easy to confuse and are NOT the same measurement.
//
//	isolation (M1)  — a property INSIDE the induced Q-subgraph: a Q node with no
//	                  Q neighbour. It is undefined for a ¬Q node, which is not a
//	                  member of that subgraph at all.
//	M5              — a property of a TARGET's own neighbourhood: any node, of
//	                  either role, that has no neighbour of role Q. Such a target
//	                  is unreachable anonymously by §4.3.4″.4, because the last
//	                  structural hop has nobody to hand the message to, and a
//	                  quiet ¬Q last hop is forbidden.
//
// The two numerators coincide on the Q slice — a Q node without a Q neighbour is
// both — but the POPULATIONS and therefore the shares do not: M5 also asks the
// question of every ¬Q node, and a ¬Q node can be a target like any other.
// Reporting one as the other overstates or understates the reachable share
// depending on which way the mistake runs, so the two are measured apart and
// their relation is asserted by test, not assumed.
//
// ⚠️ Instrument only. No candidate is measured here and no quota is judged: M5
// becomes a verdict on the quota (§4.3.4″.4) only in point B of the queue, on an
// agreed candidate.

import (
	"fmt"
	"sort"
	"strings"
)

// m5Slice is one population: how many targets it holds, how many of them have no
// Q neighbour, and the full distribution behind those two numbers.
type m5Slice struct {
	// Targets is the DENOMINATOR — every node of this population, not only the
	// ones that failed. A share without its denominator named is the mistake
	// this field exists to prevent.
	Targets int
	// Without is the numerator: targets with zero neighbours of role Q.
	Without int
	// ByCount[k] is how many targets have exactly k Q neighbours. The share is
	// a summary of this; the distribution is what shows whether the failures are
	// a tail or the middle of the population.
	ByCount []int
}

// Share is the M5 figure. ⚠️ An empty population reports NO DATA: 0 % would
// claim every target is fine when none was measured.
func (s m5Slice) Share() string {
	if s.Targets == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.1f%%", float64(s.Without)/float64(s.Targets)*100)
}

// Distribution prints the histogram, skipping counts nobody has.
func (s m5Slice) Distribution() string {
	if s.Targets == 0 {
		return "no data"
	}
	parts := make([]string, 0, len(s.ByCount))
	for count, targets := range s.ByCount {
		if targets == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%d:%d", count, targets))
	}
	return strings.Join(parts, " ")
}

func (s m5Slice) String() string {
	if s.Targets == 0 {
		return "no data"
	}
	return fmt.Sprintf("%d of %d without a Q neighbour (%s), distribution %s",
		s.Without, s.Targets, s.Share(), s.Distribution())
}

// m5Report keeps the two roles apart AND the total, because the three answer
// different questions: whether the structural half can be reached, whether an
// ordinary node can be reached, and what the network looks like overall.
type m5Report struct {
	All           m5Slice
	Structural    m5Slice
	NonStructural m5Slice
	// PerTarget is the independently recomputed count per node, kept so a caller
	// can drill into individual targets without trusting the summary.
	PerTarget []int
}

func (r m5Report) String() string {
	return fmt.Sprintf("all: %s\nQ: %s\n¬Q: %s", r.All, r.Structural, r.NonStructural)
}

// verifyEdgeBookkeeping rejects the three shapes of adjacency that would make
// any neighbour count meaningless. Each is a STAND DEFECT, not a result:
//
//	self-loop      — a node would count itself as its own neighbourhood;
//	duplicate      — one edge counted twice;
//	asymmetry      — an edge present at one end only, so the two ends disagree
//	                 about whether it exists.
func verifyEdgeBookkeeping(g *graph) error {
	for i := range g.adjacency {
		u := int32(i)
		seen := make(map[int32]int, len(g.adjacency[u]))
		for _, v := range g.adjacency[u] {
			if v == u {
				return fmt.Errorf("node %d is its own neighbour — a self-loop would count "+
					"the node's own role as its neighbourhood", u)
			}
			seen[v]++
			if seen[v] > 1 {
				return fmt.Errorf("edge %d—%d appears %d times in the adjacency of %d — "+
					"one edge counted more than once", u, v, seen[v], u)
			}
		}
	}
	for i := range g.adjacency {
		u := int32(i)
		for _, v := range g.adjacency[u] {
			back := false
			for _, w := range g.adjacency[v] {
				if w == u {
					back = true
					break
				}
			}
			if !back {
				return fmt.Errorf("edge %d—%d is present at %d only — the two ends disagree "+
					"that it exists", u, v, u)
			}
		}
	}
	return nil
}

// countStructuralNeighboursIndependently recounts, from the EDGES of the built
// graph, how many neighbours of role Q each node has.
//
// ⚠️ It deliberately ignores g.structuralNeighbours instead of reading it: that
// counter is maintained incrementally while the graph is being built, and a
// measurement that reuses it cannot notice when it drifts. The two are compared
// afterwards, and disagreement is a stand defect (§5.5 п.11 of the plan).
//
// ⚠️ A node's OWN role never enters its count. A lone Q node has zero Q
// neighbours and is a target without one — being structural yourself does not
// give the last structural hop anywhere to go.
func countStructuralNeighboursIndependently(g *graph) []int {
	counts := make([]int, len(g.adjacency))
	for i := range g.adjacency {
		u := int32(i)
		for _, v := range g.adjacency[u] {
			if g.roles[v] != roleStructural {
				continue
			}
			counts[u]++
		}
	}
	return counts
}

// measureNeighbourlessTargets is M5 over one built graph.
//
// The population is EVERY node: anyone can be the target of an anonymous search,
// and §4.3.4″.4 puts the requirement on the target's own slot policy regardless
// of which half the target is in. The slices split by the TARGET's role; the
// counting always looks at the NEIGHBOUR's role.
func measureNeighbourlessTargets(g *graph) (m5Report, error) {
	if len(g.roles) != len(g.adjacency) || len(g.structuralNeighbours) != len(g.adjacency) {
		return m5Report{}, fmt.Errorf(
			"graph is malformed: %d adjacency lists, %d roles, %d maintained counters",
			len(g.adjacency), len(g.roles), len(g.structuralNeighbours))
	}
	if err := verifyEdgeBookkeeping(g); err != nil {
		return m5Report{}, err
	}

	counts := countStructuralNeighboursIndependently(g)
	for i, count := range counts {
		if count == g.structuralNeighbours[i] {
			continue
		}
		return m5Report{}, fmt.Errorf(
			"node %d: recount over the edges says %d Q neighbours, the counter maintained "+
				"during construction says %d — one of the two is wrong, so no M5 number is "+
				"reported", i, count, g.structuralNeighbours[i])
	}

	report := m5Report{PerTarget: counts}
	for i, count := range counts {
		slices := []*m5Slice{&report.All}
		if g.roles[i] == roleStructural {
			slices = append(slices, &report.Structural)
		} else {
			slices = append(slices, &report.NonStructural)
		}
		for _, slice := range slices {
			slice.Targets++
			if count == 0 {
				slice.Without++
			}
			for len(slice.ByCount) <= count {
				slice.ByCount = append(slice.ByCount, 0)
			}
			slice.ByCount[count]++
		}
	}
	return report, nil
}

// structuralEdgeMix counts the edges of the built graph by the roles of their
// ends. It exists for ONE assertion — that nothing is counted twice and nothing
// is dropped:
//
//	Σ over all targets of their Q-neighbour count  ==  2·(Q–Q edges) + 1·(Q–¬Q edges)
//
// A Q–Q edge is seen by both ends, a mixed edge only by its ¬Q end, and a ¬Q–¬Q
// edge by neither. Without this identity a role filter applied at the wrong end
// still produces a plausible-looking share.
func structuralEdgeMix(g *graph) (bothQ, mixed, neither int) {
	for i := range g.adjacency {
		u := int32(i)
		for _, v := range g.adjacency[u] {
			if v < u {
				continue // count each undirected edge once
			}
			switch {
			case g.roles[u] == roleStructural && g.roles[v] == roleStructural:
				bothQ++
			case g.roles[u] == roleStructural || g.roles[v] == roleStructural:
				mixed++
			default:
				neither++
			}
		}
	}
	return bothQ, mixed, neither
}

// roleGraph builds a fixture from explicit roles and edges, maintaining
// structuralNeighbours the way the model does at insertion time — so the
// cross-check inside measureNeighbourlessTargets compares two independent
// implementations rather than a counter with itself.
func roleGraph(roles []int, edges [][2]int) *graph {
	g := &graph{
		ids:                  make([]nodeID, len(roles)),
		roles:                append([]int(nil), roles...),
		adjacency:            make([][]int32, len(roles)),
		structuralNeighbours: make([]int, len(roles)),
		initiated:            make([]int, len(roles)),
		shortfall:            make([]quotaShortfall, len(roles)),
	}
	for i := range roles {
		g.ids[i][0] = byte(i)
	}
	for _, edge := range edges {
		a, b := int32(edge[0]), int32(edge[1])
		g.adjacency[a] = append(g.adjacency[a], b)
		g.adjacency[b] = append(g.adjacency[b], a)
		if g.roles[b] == roleStructural {
			g.structuralNeighbours[a]++
		}
		if g.roles[a] == roleStructural {
			g.structuralNeighbours[b]++
		}
	}
	return g
}

// histogramOf renders a distribution as a sorted "count:targets" list, used by
// the fixtures to state expectations as literals.
func histogramOf(pairs map[int]int) string {
	keys := make([]int, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%d:%d", key, pairs[key]))
	}
	return strings.Join(parts, " ")
}
