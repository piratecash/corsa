package overlaysim

// split_diagnosis_test.go answers one question the policy comparison raised and
// could not settle: under the initiated limit at quotas 1..3 the 64k×8 shape has
// NO isolated structural node and still stands in two components of about half
// each (21-m1-policy-comparison.md §2).
//
// ⚠️ Nothing here changes the graph. No edge is added, no budget raised, no
// policy or criterion altered — the diagnosis observes the same construction the
// comparison measured, and there is a guard proving the observer is inert.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// splitShape, splitSeed and splitQuota are the parameters the split was first
// seen on. Named rather than inlined so every part of the diagnosis is
// demonstrably looking at the same run.
var (
	splitShape = shape{name: "64k×8", nodes: 64_000, degree: 8, budget: 16}
	splitSeeds = []uint64{1, 2, 3, 4, 5}
)

const splitQuota = 1

// structuralComponents labels every structural node with its component and
// returns the labels plus the component sizes, largest first.
func structuralComponents(g *graph, nodes int) (labels []int, sizes []int) {
	labels = make([]int, nodes)
	for i := range labels {
		labels[i] = -1
	}

	for i := range nodes {
		if g.roles[i] != roleStructural || labels[i] != -1 {
			continue
		}
		id := len(sizes)
		size := 0
		stack := []int32{int32(i)}
		labels[i] = id

		for len(stack) > 0 {
			u := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			size++
			for _, v := range g.adjacency[u] {
				if g.roles[v] == roleStructural && labels[v] == -1 {
					labels[v] = id
					stack = append(stack, v)
				}
			}
		}
		sizes = append(sizes, size)
	}
	return labels, sizes
}

// TestSplitReproducesOnTheRecordedParameters is step one: the same shape, seeds
// and quota, and the same outcome. A diagnosis of a run that no longer happens
// is a diagnosis of nothing.
func TestSplitReproducesOnTheRecordedParameters(t *testing.T) {
	if testing.Short() {
		t.Skip("the 64k diagnosis is a measurement, not a unit test")
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\n%s, quota %d, %s — structural components per seed\n",
		splitShape.name, splitQuota, policyInitiatedLimit)

	split := 0
	for _, seed := range splitSeeds {
		g := buildGraph(splitShape, seed, splitQuota, policyInitiatedLimit)
		_, sizes := structuralComponents(g, splitShape.nodes)
		sort.Sort(sort.Reverse(sort.IntSlice(sizes)))

		isolated := 0
		for _, size := range sizes {
			if size == 1 {
				isolated++
			}
		}
		shown := sizes
		if len(shown) > 4 {
			shown = shown[:4]
		}

		// ⚠️ Counted SEPARATELY for the structural subgraph and for the whole
		// graph. An edge with a ¬Q end does not join two structural
		// components, so a whole-graph crossing count says nothing about the
		// connectivity under diagnosis — an earlier version of this report
		// mixed them and attributed a whole-graph number to the Q subgraph.
		var edgesAll, edgesQ, edgesMixed, crossAll, crossQ, crossMixed int
		for i := range splitShape.nodes {
			for _, v := range g.adjacency[i] {
				if int32(i) > v {
					continue // count each edge once
				}
				crosses := bitAt(g.ids[i], splitShape.degree) != bitAt(g.ids[v], splitShape.degree)
				bothStructural := g.roles[i] == roleStructural && g.roles[v] == roleStructural

				edgesAll++
				if crosses {
					crossAll++
				}
				if bothStructural {
					edgesQ++
					if crosses {
						crossQ++
					}
				} else {
					edgesMixed++
					if crosses {
						crossMixed++
					}
				}
			}
		}

		// ⚠️ The decomposition is an identity, and stating it is what keeps the
		// two populations apart: an edge is either Q–Q or has a ¬Q end, never
		// both and never neither. Counting Q–Q without the role filter — the
		// mistake this report had — breaks exactly this line and nothing else,
		// so without it the mutation goes through unnoticed.
		if edgesAll != edgesQ+edgesMixed || crossAll != crossQ+crossMixed {
			t.Fatalf("seed %d: %d edges ≠ %d Q–Q + %d mixed, or %d crossings ≠ %d + %d — the two "+
				"populations are not being counted apart",
				seed, edgesAll, edgesQ, edgesMixed, crossAll, crossQ, crossMixed)
		}

		// ⚠️ The invariant is about PURITY, not about the number of components.
		//
		// An earlier version asserted "two components ⇒ no crossing Q–Q edge",
		// which is not a theorem: components need not follow bit d at all, and
		// a perfectly correct run that split some other way would have been
		// rejected. What IS an identity: no Q–Q edge crosses the boundary if
		// and only if every structural component is pure in bit d, because a
		// crossing edge is exactly what would put both values in one component.
		labels, _ := structuralComponents(g, splitShape.nodes)
		mixedComponents := map[int]bool{}
		seenValue := map[int]int{}
		for i := range splitShape.nodes {
			if labels[i] == -1 {
				continue
			}
			value := bitAt(g.ids[i], splitShape.degree)
			if previous, seen := seenValue[labels[i]]; seen && previous != value {
				mixedComponents[labels[i]] = true
			}
			seenValue[labels[i]] = value
		}
		allPure := len(mixedComponents) == 0

		if allPure != (crossQ == 0) {
			t.Errorf("seed %d: %d Q–Q edges cross bit %d while %d component(s) hold both values "+
				"of it — these two cannot disagree", seed, crossQ, splitShape.degree,
				len(mixedComponents))
		}

		// How many components there are, and whether they line up with bit d,
		// is an OBSERVATION about these seeds — recorded, not required.
		alignment := "components are pure in bit d"
		if !allPure {
			alignment = fmt.Sprintf("%d component(s) hold both values of bit d", len(mixedComponents))
		}

		fmt.Fprintf(&out, "  seed %d: %d component(s), largest %v, isolated %d | "+
			"Q–Q edges %d, of them across bit %d: %d | with a ¬Q end: %d, across: %d | "+
			"whole graph: %d edges, across: %d | %s\n",
			seed, len(sizes), shown, isolated,
			edgesQ, splitShape.degree, crossQ, edgesMixed, crossMixed, edgesAll, crossAll,
			alignment)

		if len(sizes) == 2 && isolated == 0 {
			split++
		}
	}

	if split == 0 {
		t.Fatal("no seed produced the two-component split — the parameters this diagnosis " +
			"was written for no longer reproduce it")
	}
	fmt.Fprintf(&out, "  split (exactly two components, none isolated) on %d of %d seeds\n",
		split, len(splitSeeds))
	t.Log(out.String())
}

// TestSplitBoundaryIsAPrefixBitAndWhichOne is step two. ⚠️ That the boundary
// follows a bucket edge is NOT assumed: ALL 160 bits of the identifier are
// tested, not a prefix of them — an earlier version looked at the first 24 and
// the report still said "every bit", which is a claim wider than its check.
// The first guess — the leading bit, the one that divides the keyspace in half
// — is WRONG: both components carry both values of it.
func TestSplitBoundaryIsAPrefixBitAndWhichOne(t *testing.T) {
	if testing.Short() {
		t.Skip("the 64k diagnosis is a measurement, not a unit test")
	}

	g := buildGraph(splitShape, 2, splitQuota, policyInitiatedLimit)
	labels, sizes := structuralComponents(g, splitShape.nodes)
	if len(sizes) != 2 {
		t.Fatalf("seed 2 gave %d components, the diagnosis expects the two-way split", len(sizes))
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\ncomponent sizes: %v\n", sizes)
	fmt.Fprintf(&out, "prefix bit → how the two components fall\n")

	separating := []int{}
	for bit := range nodeIDLen * 8 {
		var table [2][2]int
		for i := range splitShape.nodes {
			if g.roles[i] != roleStructural {
				continue
			}
			table[labels[i]][bitAt(g.ids[i], bit)]++
		}

		pure := (table[0][0] == 0 || table[0][1] == 0) && (table[1][0] == 0 || table[1][1] == 0)
		if pure {
			separating = append(separating, bit)
			fmt.Fprintf(&out, "  bit %2d: comp0 %v comp1 %v  ← SEPARATES\n", bit, table[0], table[1])
			continue
		}
		if bit <= splitShape.degree {
			fmt.Fprintf(&out, "  bit %2d: comp0 %v comp1 %v\n", bit, table[0], table[1])
		}
	}

	if len(separating) != 1 {
		t.Fatalf("bits separating the components: %v — the diagnosis needs exactly one", separating)
	}
	if separating[0] != splitShape.degree {
		t.Fatalf("the boundary is bit %d, the diagnosis expects bit d=%d",
			separating[0], splitShape.degree)
	}

	// The leading bit is the obvious guess and it is wrong. Stating that as an
	// assertion keeps the report from quietly reverting to it.
	var leading [2][2]int
	for i := range splitShape.nodes {
		if g.roles[i] != roleStructural {
			continue
		}
		leading[labels[i]][bitAt(g.ids[i], 0)]++
	}
	if leading[0][0] == 0 || leading[0][1] == 0 {
		t.Fatalf("the leading bit separates the components after all: %v", leading)
	}

	fmt.Fprintf(&out, "\nthe boundary is bit %d = d, the FIRST bit no bucket level discriminates:\n",
		splitShape.degree)
	fmt.Fprintf(&out, "  a node fills levels 0..d-1, and level i means \"differs at bit i\".\n")
	fmt.Fprintf(&out, "  the leading bit does NOT separate them: comp0 %v comp1 %v\n",
		leading[0], leading[1])
	t.Log(out.String())
}

// TestSplitPartitionIsTheSameAtQuotasOneToThree checks what the report claims
// about the quota range instead of extrapolating from one value.
//
// ⚠️ Equal component SIZES would not be enough: two different partitions can
// have the same sizes. The membership itself is compared, canonicalised by the
// smallest index in each component so that component numbering cannot make two
// identical partitions look different.
func TestSplitPartitionIsTheSameAtQuotasOneToThree(t *testing.T) {
	if testing.Short() {
		t.Skip("the 64k diagnosis is a measurement, not a unit test")
	}

	canonical := func(quota int) ([]int, []int) {
		g := buildGraph(splitShape, 2, quota, policyInitiatedLimit)
		labels, sizes := structuralComponents(g, splitShape.nodes)

		smallest := map[int]int{}
		for i := range splitShape.nodes {
			if labels[i] == -1 {
				continue
			}
			if first, seen := smallest[labels[i]]; !seen || i < first {
				smallest[labels[i]] = i
			}
		}
		for i := range splitShape.nodes {
			if labels[i] != -1 {
				labels[i] = smallest[labels[i]]
			}
		}
		return labels, sizes
	}

	base, baseSizes := canonical(1)
	for _, quota := range []int{2, 3} {
		other, sizes := canonical(quota)

		if fmt.Sprint(sizes) != fmt.Sprint(baseSizes) {
			t.Fatalf("quota %d gives component sizes %v, quota 1 gives %v", quota, sizes, baseSizes)
		}
		for i := range splitShape.nodes {
			if base[i] != other[i] {
				t.Fatalf("node %d is in component %d at quota 1 and %d at quota %d — the "+
					"partition is not the same one", i, base[i], other[i], quota)
			}
		}
		t.Logf("quota %d: the same partition as quota 1, sizes %v", quota, sizes)
	}
}

// TestObserverDoesNotChangeTheGraph is the licence for everything below it. A
// diagnosis that perturbs the run is not a diagnosis of that run.
func TestObserverDoesNotChangeTheGraph(t *testing.T) {
	t.Parallel()

	sh := shape{name: "2k×8", nodes: 2_000, degree: 8, budget: 16}

	for _, selection := range allPolicies {
		plain := buildGraph(sh, 3, 4, selection)

		moments := 0
		observed := buildGraphObserved(sh, 3, 4, selection, func(m selectionMoment) {
			moments++
			// Ask the question the diagnosis asks, so the guard covers the
			// probe too and not merely the empty callback.
			m.Available(func(int32) bool { return true })
		})

		if moments == 0 {
			t.Fatalf("%s: the observer was never called — the guard proves nothing", selection)
		}
		for i := range sh.nodes {
			if fmt.Sprint(plain.adjacency[i]) != fmt.Sprint(observed.adjacency[i]) {
				t.Fatalf("%s: node %d has %v without an observer and %v with one",
					selection, i, plain.adjacency[i], observed.adjacency[i])
			}
			if plain.initiated[i] != observed.initiated[i] ||
				plain.structuralNeighbours[i] != observed.structuralNeighbours[i] {
				t.Fatalf("%s: node %d differs in bookkeeping under observation", selection, i)
			}
		}
	}
}

// TestWhyNoEdgeEverCrossesTheBoundary is step three, and the point of the whole
// file: WHY the candidates on the other side never became neighbours.
//
// The distinction that matters is between "there was nobody to take" and "there
// was somebody and the rule never got to them". Only the second is a property
// of the selection order; the first would be a budget story. Both are counted
// here at the moment of the choice, because who has room changes while the
// graph is built and the end state cannot answer it.
func TestWhyNoEdgeEverCrossesTheBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("the 64k diagnosis is a measurement, not a unit test")
	}

	boundary := splitShape.degree

	// Both outcomes, traced the same way: a seed that splits and a seed that
	// does not. Explaining only the failure would leave "and why does it ever
	// work" unanswered — and that half turns out to be the informative one.
	for _, seed := range []uint64{2, 1} {
		ids := make([]nodeID, splitShape.nodes)
		for i := range ids {
			ids[i] = makeNodeID(seed, i)
		}
		side := func(node int32) int { return bitAt(ids[node], boundary) }

		var (
			picks         int
			nobodyAtAll   int
			chosenCrosses int

			// The claim under test: the rule takes the XOR-nearest free
			// candidate, and side plays no part in it.
			otherSideOffered int // a free candidate across the boundary existed
			otherSideFarther int // ...and was farther than the one taken
			ownSideOffered   int
			ownSideFarther   int
		)

		g := buildGraphObserved(splitShape, seed, splitQuota, policyInitiatedLimit,
			func(m selectionMoment) {
				if m.Phase == phaseLeftover {
					return
				}
				picks++
				if m.Chosen == -1 {
					nobodyAtAll++
					return
				}

				mine := side(m.Node)
				if side(m.Chosen) != mine {
					chosenCrosses++
				}

				// The nearest FREE candidate on each side, asked at this very
				// instant with the builder's own filter.
				//
				// ⚠️ The phase's OWN filter has to be included: the quota
				// preference looks for the nearest STRUCTURAL candidate, not
				// the nearest candidate. Comparing against the unfiltered
				// nearest made this test fail on its first run — correctly,
				// because it was then measuring a rule the builder does not
				// follow.
				phaseFilter := func(c int32) bool { return true }
				if m.Phase == phaseQuota {
					phaseFilter = func(c int32) bool { return roleOf(ids[c]) == roleStructural }
				}
				bestOwn := m.Available(func(c int32) bool { return side(c) == mine && phaseFilter(c) })
				bestOther := m.Available(func(c int32) bool { return side(c) != mine && phaseFilter(c) })

				if bestOther != -1 {
					otherSideOffered++
					if xorLess(ids[m.Node], ids[m.Chosen], ids[bestOther]) {
						otherSideFarther++
					} else if bestOther != m.Chosen {
						t.Fatalf("seed %d node %d level %d: took %d while %d across the boundary "+
							"was nearer — then distance is not what decides",
							seed, m.Node, m.Level, m.Chosen, bestOther)
					}
				}
				if bestOwn != -1 {
					ownSideOffered++
					if xorLess(ids[m.Node], ids[m.Chosen], ids[bestOwn]) {
						ownSideFarther++
					} else if bestOwn != m.Chosen {
						t.Fatalf("seed %d node %d level %d: took %d while %d on its own side "+
							"was nearer", seed, m.Node, m.Level, m.Chosen, bestOwn)
					}
				}
			})

		_, sizes := structuralComponents(g, splitShape.nodes)

		t.Logf("\nseed %d — %d structural component(s)\n"+
			"  picks (quota preference + ordinary contact): %d\n"+
			"  the rule found nobody at all:                %d\n"+
			"  picks that crossed the boundary:             %d\n"+
			"\n"+
			"  at the instant of each pick, a FREE candidate existed\n"+
			"    across the boundary: %d — and was FARTHER than the one taken in %d of them\n"+
			"    on its own side:     %d — and was FARTHER than the one taken in %d of them\n"+
			"\n"+
			"  ⚠️ So the candidates across the boundary were not missing and not full.\n"+
			"  They were farther, and the rule takes the nearest — every one of the %d\n"+
			"  picks obeys that, crossings included. Side is not a criterion anywhere;\n"+
			"  it is an OUTCOME of distance.",
			seed, len(sizes), picks, nobodyAtAll, chosenCrosses,
			otherSideOffered, otherSideFarther, ownSideOffered, ownSideFarther, picks)
	}

	t.Log("\n  Why distance produces a boundary at bit d: a level-i bucket holds about\n" +
		"  N/2^(i+1) nodes, and the XOR-nearest of them agrees with the target on roughly\n" +
		"  the next log2 of that many bits. A node fills levels 0..d-1 only, so at 64000\n" +
		"  nodes even the deepest level it visits, 7, offers about 250 candidates — enough\n" +
		"  that one of them almost always matches bit 8 as well, and the nearer one wins.\n" +
		"  Bit 8 is simply the first bit no level is required to differ on.")
}

// scanNearestInBucket answers the same question as idTrie.nearestInBucket by
// brute force: the XOR-nearest node that shares exactly `level` leading bits
// with the target and passes `accept`.
//
// It exists to separate a simulator FAULT from a policy LIMIT. The conclusion
// of this file — that a whole class of candidates is never reached — would be
// worthless if the structure used to reach them were simply wrong, and the trie
// is the one piece of the model with no independent reader.
func scanNearestInBucket(ids []nodeID, target nodeID, level int, accept func(int32) bool) int32 {
	best := int32(-1)
	for i := range ids {
		candidate := int32(i)

		shares := true
		for depth := range level {
			if bitAt(ids[candidate], depth) != bitAt(target, depth) {
				shares = false
				break
			}
		}
		if !shares || bitAt(ids[candidate], level) == bitAt(target, level) {
			continue
		}
		if !accept(candidate) {
			continue
		}
		if best == -1 || xorLess(target, ids[candidate], ids[best]) {
			best = candidate
		}
	}
	return best
}

// TestBucketSearchAgreesWithBruteForce checks the trie under the filters the
// builder actually uses — role, already a neighbour, and no budget left — not
// merely under "any node". A structure that is right in general and wrong under
// a filter would produce exactly the symptom this file is explaining.
func TestBucketSearchAgreesWithBruteForce(t *testing.T) {
	t.Parallel()

	const nodes = 3_000
	ids := make([]nodeID, nodes)
	roles := make([]int, nodes)
	for i := range ids {
		ids[i] = makeNodeID(11, i)
		roles[i] = roleOf(ids[i])
	}
	trie := newIDTrie(ids)

	// Stand-ins for the live state: an arbitrary but fixed set of "full" nodes
	// and of existing neighbours, so the filters really exclude things.
	full := func(c int32) bool { return c%5 == 0 }
	neighbour := func(u, c int32) bool { return (u+c)%7 == 0 }

	filters := []struct {
		name   string
		accept func(u, c int32) bool
	}{
		{"anybody but itself", func(u, c int32) bool { return c != u }},
		{"structural only", func(u, c int32) bool {
			return c != u && roles[c] == roleStructural
		}},
		{"has budget left", func(u, c int32) bool { return c != u && !full(c) }},
		{"not already a neighbour", func(u, c int32) bool { return c != u && !neighbour(u, c) }},
		{"the builder's own combination", func(u, c int32) bool {
			return c != u && !neighbour(u, c) && !full(c) && roles[c] == roleStructural
		}},
		{"nothing passes", func(int32, int32) bool { return false }},
	}

	for _, filter := range filters {
		t.Run(filter.name, func(t *testing.T) {
			t.Parallel()

			compared, answered := 0, 0
			for i := 0; i < nodes; i += 3 {
				u := int32(i)
				accept := func(c int32) bool { return filter.accept(u, c) }

				for level := range 12 {
					compared++

					want := scanNearestInBucket(ids, ids[u], level, accept)
					got := int32(-1)
					if found := trie.nearestInBucket(ids[u], level, 1, accept); len(found) == 1 {
						got = found[0]
					}
					if got != want {
						t.Fatalf("node %d level %d: the trie says %d, a full scan says %d",
							u, level, got, want)
					}
					if want != -1 {
						answered++
					}
				}
			}

			if filter.name != "nothing passes" && answered == 0 {
				t.Fatal("every query came back empty — this filter proves nothing")
			}
			t.Logf("%d bucket queries compared, %d of them non-empty", compared, answered)
		})
	}
}

// TestEveryEdgeAgreesOnTheBoundaryBit is the fact the mechanism has to explain.
// It is stated over EDGES rather than components, because an edge is what a
// selection produces and a component is only the consequence.
func TestEveryEdgeAgreesOnTheBoundaryBit(t *testing.T) {
	if testing.Short() {
		t.Skip("the 64k diagnosis is a measurement, not a unit test")
	}

	g := buildGraph(splitShape, 2, splitQuota, policyInitiatedLimit)

	// ⚠️ Two populations of edges, never merged: the whole graph, and the
	// structural subgraph whose connectivity is the subject. An edge with a ¬Q
	// end preserves the boundary just the same, but it cannot join two
	// structural components, so it belongs to a different sentence.
	var agreeAll, crossAll, agreeQ, crossQ int
	byLevel := map[int]int{}

	for i := range splitShape.nodes {
		for _, v := range g.adjacency[i] {
			if int32(i) > v {
				continue // count each edge once
			}
			crosses := bitAt(g.ids[i], splitShape.degree) != bitAt(g.ids[v], splitShape.degree)
			bothStructural := g.roles[i] == roleStructural && g.roles[v] == roleStructural

			if crosses {
				crossAll++
			} else {
				agreeAll++
			}
			if bothStructural {
				if crosses {
					crossQ++
				} else {
					agreeQ++
				}
			}

			level := nodeIDLen * 8
			for depth := range nodeIDLen * 8 {
				if bitAt(g.ids[i], depth) != bitAt(g.ids[v], depth) {
					level = depth
					break
				}
			}
			byLevel[level]++
		}
	}

	for level := range byLevel {
		if level >= splitShape.degree {
			t.Errorf("an edge joins nodes that first differ at bit %d — no bucket level "+
				"produces that, so the builder did something the model does not describe", level)
		}
	}
	if crossAll != 0 || crossQ != 0 {
		t.Fatalf("seed 2 has %d crossing edges (%d of them Q–Q), so bit %d is not a boundary here",
			crossAll, crossQ, splitShape.degree)
	}

	t.Logf("\nseed 2, bit %d as a boundary\n"+
		"  whole graph:          %d edges, %d crossing\n"+
		"  structural subgraph:  %d edges, %d crossing  ← the connectivity under diagnosis\n"+
		"first differing bit (= the bucket level that produced the edge), whole graph: %v\n"+
		"⚠️ levels stop at d-1=%d by construction; that is what leaves bit %d undiscriminated",
		splitShape.degree, agreeAll+crossAll, crossAll, agreeQ+crossQ, crossQ,
		byLevel, splitShape.degree-1, splitShape.degree)
}
