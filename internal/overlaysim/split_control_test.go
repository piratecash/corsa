package overlaysim

// split_control_test.go is the control experiment of
// docs/refactoring/dht/21-m1-split-diagnosis.md §7: the levels, the budget, the
// quota, the identifiers and the roles all stay exactly as they were, and ONE
// thing changes — which member of the bucket a node takes.
//
// The diagnosis explains the boundary at bit d in two parts: (a) a node only
// ever visits levels 0..d-1, so bit d is unconstrained — true by construction;
// and (b) the NEAREST member of a bucket shares a long prefix with the target
// and therefore matches bit d as well — measured on events, but not yet
// separated from (a). A random choice inside the same bucket keeps (a) and
// removes (b).
//
// ⚠️ The random chooser is a PROBE, not a candidate policy: it destroys the
// routing property the geometry exists for. Nothing may be adopted from a run
// that uses it.
//
// ⚠️ Three things are measured SEPARATELY, because one cannot stand in for
// another: Q–Q edges crossing bit d, survival of the original partition, and
// overall connectivity. A partition that disappears while the graph stays
// disconnected for some other reason is a different result from a graph that
// becomes connected.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// randomTargetInBucket builds a reproducible identifier that lies in the same
// bucket as the node: the first `level` bits are the node's, bit `level` is
// flipped, and everything below is derived from a hash of (seed, node, level).
//
// Choosing "nearest to a random target in the bucket" rather than "a uniformly
// random member" is deliberate: enumerating a level-0 bucket is half the
// network on every pick, which would make the experiment unrunnable. What the
// probe needs is that the choice stops correlating with the node's OWN low
// bits, and a random target inside the bucket does exactly that.
func randomTargetInBucket(seed uint64, id nodeID, node int32, level int) nodeID {
	var buf [8 + 4 + 4]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(node))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(level))
	digest := sha256.Sum256(append([]byte("corsa/overlay/sim/probe/v1"), buf[:]...))

	var target nodeID
	copy(target[:], digest[:nodeIDLen])

	// Keep the bucket: same first `level` bits, opposite bit at `level`.
	for depth := range level {
		setBit(&target, depth, bitAt(id, depth))
	}
	setBit(&target, level, 1-bitAt(id, level))
	return target
}

func setBit(id *nodeID, depth, value int) {
	mask := byte(1) << (7 - depth%8)
	if value == 1 {
		id[depth/8] |= mask
	} else {
		id[depth/8] &^= mask
	}
}

// randomBucketChooser is the probe itself.
func randomBucketChooser(seed uint64, ids []nodeID, trie *idTrie) bucketChooser {
	return func(node int32, level int, accept func(int32) bool) int32 {
		target := randomTargetInBucket(seed, ids[node], node, level)

		inBucket := func(candidate int32) bool {
			for depth := range level {
				if bitAt(ids[candidate], depth) != bitAt(ids[node], depth) {
					return false
				}
			}
			return bitAt(ids[candidate], level) != bitAt(ids[node], level)
		}

		found := trie.nearest(target, 1, func(candidate int32) bool {
			return inBucket(candidate) && accept(candidate)
		})
		if len(found) == 0 {
			return -1
		}
		return found[0]
	}
}

// TestRandomBucketChooserStaysInTheBucket is the guard that makes the control
// meaningful: if the probe silently widened the candidate set, a disappearing
// boundary would prove nothing about the nearest-rule.
func TestRandomBucketChooserStaysInTheBucket(t *testing.T) {
	t.Parallel()

	const nodes = 3_000
	ids := make([]nodeID, nodes)
	for i := range ids {
		ids[i] = makeNodeID(5, i)
	}
	trie := newIDTrie(ids)
	choose := randomBucketChooser(5, ids, trie)

	picked, agreeBelow := 0, 0
	for i := 0; i < nodes; i += 7 {
		u := int32(i)
		for level := range 8 {
			got := choose(u, level, func(c int32) bool { return c != u })
			scan := scanNearestInBucket(ids, ids[u], level, func(c int32) bool { return c != u })

			if got == -1 {
				if scan != -1 {
					t.Fatalf("node %d level %d: the probe found nobody while the bucket holds %d",
						u, level, scan)
				}
				continue
			}
			picked++

			// Membership: the same first `level` bits, a different bit at
			// `level`. This is the bucket, by definition.
			for depth := range level {
				if bitAt(ids[got], depth) != bitAt(ids[u], depth) {
					t.Fatalf("node %d level %d: the probe took %d, which differs at bit %d",
						u, level, got, depth)
				}
			}
			if bitAt(ids[got], level) == bitAt(ids[u], level) {
				t.Fatalf("node %d level %d: the probe took %d, which agrees at bit %d",
					u, level, got, level)
			}

			if bitAt(ids[got], 8) == bitAt(ids[u], 8) {
				agreeBelow++
			}
		}
	}

	if picked == 0 {
		t.Fatal("the probe never picked anybody")
	}

	// The whole point: the choice must stop tracking the node's own bit 8. The
	// nearest-rule agrees on it almost always; a probe that still did would not
	// be removing part (b) at all.
	share := float64(agreeBelow) / float64(picked)
	t.Logf("probe picks: %d, of them agreeing with the node on bit 8: %d (%.1f%%)",
		picked, agreeBelow, share*100)
	if share > 0.75 {
		t.Fatalf("the probe still agrees on bit 8 in %.1f%% of picks — it is not removing the "+
			"correlation the control exists to remove", share*100)
	}
}

// TestSplitControlRandomInBucket runs the experiment.
func TestSplitControlRandomInBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("the 64k control is a measurement, not a unit test")
	}

	boundary := splitShape.degree

	var out strings.Builder
	fmt.Fprintf(&out, "\ncontrol: random choice inside the SAME buckets — %s, quota %d, %s\n",
		splitShape.name, splitQuota, policyInitiatedLimit)
	fmt.Fprintf(&out, "unchanged: levels 0..%d, B=%d, quota, identifiers, roles, order\n",
		boundary-1, splitShape.budget)
	fmt.Fprintf(&out, "changed:   which member of the bucket is taken\n\n")

	for _, seed := range splitSeeds {
		ids := make([]nodeID, splitShape.nodes)
		for i := range ids {
			ids[i] = makeNodeID(seed, i)
		}
		trie := newIDTrie(ids)

		nearest := buildGraph(splitShape, seed, splitQuota, policyInitiatedLimit)
		random := buildGraphProbed(splitShape, seed, splitQuota, policyInitiatedLimit, nil,
			randomBucketChooser(seed, ids, trie))

		// --- measurement 1: Q–Q edges across the boundary -------------------
		crossQ := func(g *graph) (edges, crossing int) {
			for i := range splitShape.nodes {
				for _, v := range g.adjacency[i] {
					if int32(i) > v || g.roles[i] != roleStructural || g.roles[v] != roleStructural {
						continue
					}
					edges++
					if bitAt(g.ids[i], boundary) != bitAt(g.ids[v], boundary) {
						crossing++
					}
				}
			}
			return edges, crossing
		}
		edgesBefore, crossBefore := crossQ(nearest)
		edgesAfter, crossAfter := crossQ(random)

		// --- measurement 2: did the ORIGINAL partition survive --------------
		canonical := func(g *graph) []int {
			labels, _ := structuralComponents(g, splitShape.nodes)
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
			return labels
		}
		before, after := canonical(nearest), canonical(random)
		moved := 0
		for i := range splitShape.nodes {
			if before[i] != after[i] {
				moved++
			}
		}

		// --- measurement 3: connectivity ------------------------------------
		_, sizesBefore := structuralComponents(nearest, splitShape.nodes)
		_, sizesAfter := structuralComponents(random, splitShape.nodes)
		sort.Sort(sort.Reverse(sort.IntSlice(sizesBefore)))
		sort.Sort(sort.Reverse(sort.IntSlice(sizesAfter)))

		isolated := 0
		for _, size := range sizesAfter {
			if size == 1 {
				isolated++
			}
		}
		shown := sizesAfter
		if len(shown) > 4 {
			shown = shown[:4]
		}

		fmt.Fprintf(&out, "seed %d\n", seed)
		fmt.Fprintf(&out, "  1. Q–Q edges across bit %d:  nearest %d of %d  →  random %d of %d\n",
			boundary, crossBefore, edgesBefore, crossAfter, edgesAfter)
		fmt.Fprintf(&out, "  2. original partition:       %d of %d structural nodes changed "+
			"component label\n", moved, countStructural(nearest, splitShape.nodes))
		fmt.Fprintf(&out, "  3. connectivity:             %d component(s) %v  →  %d component(s) "+
			"%v, isolated %d\n\n",
			len(sizesBefore), sizesBefore[:min(2, len(sizesBefore))],
			len(sizesAfter), shown, isolated)

		// ⚠️ The probe must not have bought its result by breaking a constraint.
		// Without this the experiment could be answering "what happens when B
		// is ignored", which is a different question with a known answer.
		if edgesAfter == 0 {
			t.Fatalf("seed %d: the probe built no structural edges at all", seed)
		}
		for i := range splitShape.nodes {
			if degree := len(random.adjacency[i]); degree > splitShape.budget {
				t.Fatalf("seed %d: node %d has degree %d over budget %d under the probe",
					seed, i, degree, splitShape.budget)
			}
			if random.initiated[i] > splitShape.degree {
				t.Fatalf("seed %d: node %d initiated %d links over the desired degree %d",
					seed, i, random.initiated[i], splitShape.degree)
			}
			// Every edge must still come from a bucket level below d: that is
			// part (a), which the probe is NOT allowed to change.
			for _, v := range random.adjacency[i] {
				level := nodeIDLen * 8
				for depth := range nodeIDLen * 8 {
					if bitAt(random.ids[i], depth) != bitAt(random.ids[v], depth) {
						level = depth
						break
					}
				}
				if level >= splitShape.degree {
					t.Fatalf("seed %d: an edge joins nodes first differing at bit %d — the probe "+
						"reached past level %d, so it changed the construction too",
						seed, level, splitShape.degree-1)
				}
			}
		}
	}

	fmt.Fprintf(&out, "⚠️ A probe, not a policy. Whatever these numbers say, nothing is adopted "+
		"from them: the random chooser has no routing property.\n")
	t.Log(out.String())
}

func countStructural(g *graph, nodes int) int {
	count := 0
	for i := range nodes {
		if g.roles[i] == roleStructural {
			count++
		}
	}
	return count
}
