package overlaysim

// c1_test.go is the acceptance of the CANDIDATE, §5.5 of
// docs/refactoring/dht/21-m1-candidate-c1.md. It closes points 1–6 and 8 of that
// list: the hash choice agrees with a full scan, the chosen member lies in its
// own bucket, the correlation with bit d is gone, the ceiling and the initiated
// limit still hold, every BUCKET edge is born at a level below d, a seed
// reproduces a run, and the comparison base has not moved.
//
// ⚠️ Nothing here says C1/v1 is better than anything. It says the rule in the
// stand is the rule in the document. Acceptance numbers come later, on the
// agreed candidate, and O5 stays open.

import (
	"encoding/hex"
	"fmt"
	"testing"
)

// c1Shapes are small on purpose: every check below is a property of the RULE,
// and a property that needs sixty-four thousand nodes to show up is a property
// nobody can debug.
var c1Shapes = []shape{
	{name: "300×4", nodes: 300, degree: 4, budget: 8},
	{name: "1k×8", nodes: 1_000, degree: 8, budget: 16},
}

// sharesExactly is the bucket predicate stated independently of the trie: the
// candidate matches the owner on the first `level` bits and differs at bit
// `level`. The trie answers the same question by construction, which is exactly
// why the reference must not use it.
func sharesExactly(owner, candidate nodeID, level int) bool {
	for bit := range level {
		if bitAt(owner, bit) != bitAt(candidate, bit) {
			return false
		}
	}
	return bitAt(owner, level) != bitAt(candidate, level)
}

// scanRepresentative is §5.5 п.1: the same rule computed the slow, obvious way —
// look at every node in the network, keep the ones in this bucket that the
// filter accepts, take the smallest rank.
func scanRepresentative(ids []nodeID, owner int32, level int, accept func(int32) bool) int32 {
	best := int32(-1)
	var bestRank [32]byte

	for index := range ids {
		candidate := int32(index)
		if candidate == owner || !sharesExactly(ids[owner], ids[candidate], level) {
			continue
		}
		if !accept(candidate) {
			continue
		}
		rank := c1Rank(ids[owner], level, ids[candidate])
		if best == -1 || string(rank[:]) < string(bestRank[:]) {
			best, bestRank = candidate, rank
		}
	}
	return best
}

// TestC1RepresentativeAgreesWithAFullScan is §5.5 п.1.
//
// ⚠️ The check is NOT vacuous, and the filters below are why. The trie walk and
// the scan differ in one thing only — the trie visits a subtree and the scan
// visits the network — so a filter that rejects the first few candidates is what
// makes a wrong subtree visible. With no filter at all a broken walk that
// happened to cover the bucket would pass.
func TestC1RepresentativeAgreesWithAFullScan(t *testing.T) {
	t.Parallel()

	for _, sh := range c1Shapes {
		ids := make([]nodeID, sh.nodes)
		for i := range ids {
			ids[i] = makeNodeID(31, i)
		}
		trie := newIDTrie(ids)

		filters := []struct {
			name   string
			accept func(owner int32) func(int32) bool
		}{
			{"everyone", func(int32) func(int32) bool {
				return func(int32) bool { return true }
			}},
			{"structural only", func(int32) func(int32) bool {
				return func(c int32) bool { return roleOf(ids[c]) == roleStructural }
			}},
			{"every third", func(int32) func(int32) bool {
				return func(c int32) bool { return c%3 == 0 }
			}},
			{"nobody", func(int32) func(int32) bool {
				return func(int32) bool { return false }
			}},
		}

		for _, filter := range filters {
			agreed, nonEmpty := 0, 0
			scratch := make([]int32, 0, sh.nodes)
			for owner := range min(sh.nodes, 120) {
				for level := range sh.degree {
					accept := filter.accept(int32(owner))
					fromTrie := c1Representative(trie, ids, &scratch, int32(owner), level, accept)
					fromScan := scanRepresentative(ids, int32(owner), level, accept)

					if fromTrie != fromScan {
						t.Fatalf("%s/%s: node %d level %d — the bucket walk says %d, the full scan "+
							"says %d", sh.name, filter.name, owner, level, fromTrie, fromScan)
					}
					agreed++
					if fromTrie != -1 {
						nonEmpty++
					}
				}
			}

			// A comparison that only ever compared "nobody to -1" would agree
			// perfectly and check nothing.
			if filter.name != "nobody" && nonEmpty == 0 {
				t.Fatalf("%s/%s: every one of %d questions came back empty, so the agreement is "+
					"vacuous", sh.name, filter.name, agreed)
			}
			if filter.name == "nobody" && nonEmpty != 0 {
				t.Fatalf("%s: a filter that accepts nobody returned %d candidates", sh.name, nonEmpty)
			}
		}
	}
}

// TestC1RankIsPinnedToItsInputs fixes the rule itself. §5.1 requires every run to
// carry a rule label, and a label is worth nothing if the arithmetic behind it
// can change without anybody noticing.
func TestC1RankIsPinnedToItsInputs(t *testing.T) {
	t.Parallel()

	var zero, ones nodeID
	for i := range ones {
		ones[i] = 0xff
	}

	// ⚠️ PINNED, not logged. The first version only logged these and argued that
	// "a literal digest is a number no reviewer can check" — but the properties
	// below check that the three INPUTS reach the hash, and nothing at all
	// checked the hash itself: changing the separator, the field order or the
	// encoding of the level would have left every test green while §5.1 requires
	// every run to carry a rule label. A label is worth nothing if the
	// arithmetic behind it can move without anybody noticing.
	//
	// A reviewer checks these by recomputing them, which is the point of writing
	// the inputs out beside them:
	//
	//	SHA-256( "corsa/overlay/sim/pick/v1" ‖ owner[20] ‖ byte(level) ‖ candidate[20] )
	for _, vector := range []struct {
		name      string
		owner     nodeID
		level     int
		candidate nodeID
		digest    string
	}{
		{"zeros, level 0, ones", zero, 0, ones,
			"777cde83cb9884298993dd78df889c74dfe55af679ca7b43e9a5698acb529704"},
		{"zeros, level 1, ones", zero, 1, ones,
			"3c562e6a207e94bcd703b988d3c6ace169785656a548a65740407336f624b88e"},
		{"ones, level 0, zeros", ones, 0, zero,
			"3d8de34be061aed2be0934df35fbdac5ee50a90b2913886fccc513844fcf6cef"},
	} {
		digest := c1Rank(vector.owner, vector.level, vector.candidate)
		if got := hex.EncodeToString(digest[:]); got != vector.digest {
			t.Errorf("%s: %s, pinned %s — the rule behind the label C1/v1 has changed",
				vector.name, got, vector.digest)
		}
	}

	// The three properties the rule owes, stated as inequalities rather than as
	// literals — literals of a hash say nothing a reader can check by eye:
	//
	//  1. the OWNER is in the hash, so two owners rank one candidate differently
	//     (this is what keeps the bucket from converging on one representative);
	//  2. the LEVEL is in the hash, so one pair ranks differently per level;
	//  3. the CANDIDATE is in the hash, which is the only trivial one.
	byOwnerA := c1Rank(zero, 0, ones)
	byOwnerB := c1Rank(ones, 0, ones)
	if byOwnerA == byOwnerB {
		t.Error("two different owners rank the same candidate identically — the rule would make " +
			"every member of a bucket choose the same representative")
	}
	if c1Rank(zero, 0, ones) == c1Rank(zero, 1, ones) {
		t.Error("the level does not reach the rank: one pair would have one order at every level")
	}
	if c1Rank(zero, 0, ones) == c1Rank(zero, 0, zero) {
		t.Error("the candidate does not reach the rank")
	}

	// And the level is encoded as ONE byte, so a level outside 0…255 must be
	// refused rather than aliased onto another level.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("level 256 was ranked instead of refused — it would alias onto level 0")
			}
		}()
		c1Rank(zero, 256, ones)
	}()
}

// TestC1EdgesAreBornInTheirOwnBucket is §5.5 п.2 and п.5 together: every edge the
// BUCKET phase produced connects two nodes that share exactly `level` leading
// bits, and that level is below d.
//
// ⚠️ п.5 applies to bucket edges ONLY. The leftover fill takes the XOR-nearest
// node of the whole network and is bound by no level at all — checking it with
// the same rule would report a correct run as a defect, which is why the
// provenance is recorded in the first place.
func TestC1EdgesAreBornInTheirOwnBucket(t *testing.T) {
	t.Parallel()

	for _, sh := range c1Shapes {
		for _, quota := range []int{0, 1, sh.degree} {
			g := buildGraph(sh, 5, quota, policyCandidateC1)

			bucket, leftover := 0, 0
			for _, edge := range g.edges {
				switch edge.Origin {
				case edgeFromBucket:
					bucket++
					if edge.Level < 0 || edge.Level >= sh.degree {
						t.Fatalf("%s quota=%d: bucket edge %d→%d carries level %d, outside 0…%d",
							sh.name, quota, edge.Initiator, edge.Peer, edge.Level, sh.degree-1)
					}
					if !sharesExactly(g.ids[edge.Initiator], g.ids[edge.Peer], edge.Level) {
						t.Fatalf("%s quota=%d: bucket edge %d→%d at level %d does not share exactly "+
							"%d leading bits", sh.name, quota, edge.Initiator, edge.Peer,
							edge.Level, edge.Level)
					}
				case edgeFromLeftover:
					leftover++
					if edge.Level != -1 {
						t.Fatalf("%s quota=%d: leftover edge %d→%d carries level %d — the leftover "+
							"fill has no level", sh.name, quota, edge.Initiator, edge.Peer, edge.Level)
					}
				default:
					t.Fatalf("%s quota=%d: C1/v1 produced a repair edge, which only the second "+
						"pass may do", sh.name, quota)
				}
			}

			if bucket == 0 {
				t.Fatalf("%s quota=%d: no bucket edge at all, so the check is vacuous",
					sh.name, quota)
			}
			if got, want := bucket+leftover, len(g.edges); got != want {
				t.Fatalf("%s quota=%d: %d edges split into %d, so the provenance loses edges",
					sh.name, quota, want, got)
			}

			report := measure(sh, 5, quota, policyCandidateC1)
			if report.BucketLinks+report.LeftoverLinks+report.RepairLinks != report.Links {
				t.Fatalf("%s quota=%d: %d bucket + %d leftover + %d repair ≠ %d links",
					sh.name, quota, report.BucketLinks, report.LeftoverLinks,
					report.RepairLinks, report.Links)
			}
			t.Logf("%s quota=%d: %d bucket edges, %d leftover (%s of all)",
				sh.name, quota, report.BucketLinks, report.LeftoverLinks, report.LeftoverShare())
		}
	}
}

// bitDAgreement is the measurement of §5.5 п.3: over the BUCKET edges of a graph,
// the share whose two ends agree on bit `d` — the first bit no visited level
// constrains.
func bitDAgreement(g *graph, degree int) (share float64, edges int) {
	agree := 0
	for _, edge := range g.edges {
		if edge.Origin != edgeFromBucket {
			continue
		}
		edges++
		if bitAt(g.ids[edge.Initiator], degree) == bitAt(g.ids[edge.Peer], degree) {
			agree++
		}
	}
	if edges == 0 {
		return 0, 0
	}
	return float64(agree) / float64(edges), edges
}

// TestC1BreaksTheCorrelationWithBitD is §5.5 п.3, and it is a PAIRED check: the
// base is measured on the same graph inputs, because "about half" means nothing
// without the number it is supposed to be unlike.
func TestC1BreaksTheCorrelationWithBitD(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}
	const quota = 1

	for _, seed := range []uint64{1, 2, 3} {
		baseShare, baseEdges := bitDAgreement(buildGraph(sh, seed, quota, policyInitiatedLimit), sh.degree)
		c1Share, c1Edges := bitDAgreement(buildGraph(sh, seed, quota, policyCandidateC1), sh.degree)

		if baseEdges == 0 || c1Edges == 0 {
			t.Fatalf("seed %d: %d base and %d candidate bucket edges", seed, baseEdges, c1Edges)
		}
		t.Logf("seed %d: bit %d agrees on %.1f%% of initiated-limit bucket edges and %.1f%% of "+
			"C1/v1 ones", seed, sh.degree, baseShare*100, c1Share*100)

		// The base preserves the bit: that is the measured mechanism of the
		// split (21-m1-split-diagnosis.md §3–§4), restated here so the
		// candidate's number has something to be compared with.
		//
		// ⚠️ The threshold is 0.70, not "about 1.0", and the difference is a
		// fact about the FIXTURE rather than a softened claim. Preservation is
		// as strong as the bucket is large: at 64k×8 the deepest visited level
		// offers ≈250 candidates and the nearest one almost always agrees on the
		// next bit, while at 1k×8 it offers ≈4 and often none of them does.
		// Measured here: ≈79 % on every seed. Asserting ≈100 % on a 1k fixture
		// would be asserting a number this network cannot produce.
		if baseShare < 0.70 {
			t.Errorf("seed %d: the base agrees on bit %d only %.1f%% of the time — then the "+
				"correlation this candidate removes is not present in the base either, and the "+
				"test below compares nothing", seed, sh.degree, baseShare*100)
		}
		// ⚠️ A WIDE band on purpose. The claim is "the bit is no longer
		// preserved", not "the bit is exactly fair": a narrow band would turn a
		// property of the rule into a property of this seed.
		if c1Share < 0.40 || c1Share > 0.60 {
			t.Errorf("seed %d: C1/v1 agrees on bit %d in %.1f%% of bucket edges, outside 40…60%% — "+
				"the hash order still correlates with the identifier",
				seed, sh.degree, c1Share*100)
		}
	}
}

// TestC1KeepsEveryOtherRule is §5.5 п.4 restated for this policy alone, plus the
// two rules §2 says are NOT changed.
func TestC1KeepsEveryOtherRule(t *testing.T) {
	t.Parallel()

	for _, sh := range c1Shapes {
		for _, quota := range []int{0, 1, sh.degree} {
			c1 := buildGraph(sh, 9, quota, policyCandidateC1)
			base := buildGraph(sh, 9, quota, policyInitiatedLimit)

			for i := range sh.nodes {
				if degree := len(c1.adjacency[i]); degree > sh.budget {
					t.Fatalf("%s quota=%d: node %d holds %d connections against B=%d",
						sh.name, quota, i, degree, sh.budget)
				}
				if c1.initiated[i] > sh.degree {
					t.Fatalf("%s quota=%d: node %d initiated %d links against d=%d",
						sh.name, quota, i, c1.initiated[i], sh.degree)
				}
				// The candidate must see the same network as its base, or every
				// difference between them is unattributable.
				if c1.ids[i] != base.ids[i] || c1.roles[i] != base.roles[i] {
					t.Fatalf("%s quota=%d: node %d differs in identifier or role between the "+
						"candidate and its base", sh.name, quota, i)
				}
			}

			// And the two rules really do differ somewhere, or "one rule apart"
			// would be describing two identical runs.
			differs := false
			for i := range sh.nodes {
				if fmt.Sprint(c1.adjacency[i]) != fmt.Sprint(base.adjacency[i]) {
					differs = true
					break
				}
			}
			// ⚠️ No exemption for quota 0. The in-bucket rule runs whatever the
			// quota is — the quota only decides whether the bucket is asked for
			// a structural member FIRST — so two rules that produced the same
			// graph at quota 0 would mean the candidate is not reaching the
			// choice at all.
			if !differs {
				t.Fatalf("%s quota=%d: C1/v1 and initiated-limit built the identical graph",
					sh.name, quota)
			}
		}
	}
}

// TestC1IsReproducible is §5.5 п.6.
func TestC1IsReproducible(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1_000, degree: 8, budget: 16}

	first := measure(sh, 4, 2, policyCandidateC1)
	second := measure(sh, 4, 2, policyCandidateC1)
	if fmt.Sprintf("%+v", first) != fmt.Sprintf("%+v", second) {
		t.Fatalf("one seed produced two reports:\n%+v\n%+v", first, second)
	}

	other := measure(sh, 5, 2, policyCandidateC1)
	if fmt.Sprintf("%+v", first) == fmt.Sprintf("%+v", other) {
		t.Fatal("two seeds produced the identical report, so the seed does not reach the rule")
	}
}

// TestComparisonBaseHasNotMoved is §5.5 п.8 for the BASE — the run C1/v1 will be
// compared against.
//
// ⚠️ The base is `initiated-limit`, not the baseline: the candidate differs from
// it by exactly one rule. The baseline keeps its own golden test
// (TestBaselineReproducesThePublishedRun) as a separate control that the stand
// has not drifted; the two answer different questions and neither replaces the
// other.
func TestComparisonBaseHasNotMoved(t *testing.T) {
	if testing.Short() {
		t.Skip("the base pin rebuilds a 10k graph five times")
	}
	t.Parallel()

	// docs/refactoring/dht/21-m1-policy-comparison.md §3.2, row "10k×8, quota 3,
	// initiated-limit": one component, no isolated Q node, 70 873 links as the
	// MEAN over the five published seeds.
	sh := shape{name: "10k×8", nodes: 10_000, degree: 8, budget: 16}
	const quota = 3

	totalLinks, worstComponents, worstIsolated := 0, 0, 0
	for _, seed := range sweepSeeds {
		report := measure(sh, seed, quota, policyInitiatedLimit)
		totalLinks += report.Links
		if report.Structural.Components > worstComponents {
			worstComponents = report.Structural.Components
		}
		if report.Structural.Isolated > worstIsolated {
			worstIsolated = report.Structural.Isolated
		}
	}

	if got := totalLinks / len(sweepSeeds); got != 70_873 {
		t.Errorf("10k×8 quota 3 initiated-limit: %d links on average, the published run says 70873",
			got)
	}
	if worstComponents != 1 {
		t.Errorf("worst structural components %d, the published run says 1", worstComponents)
	}
	if worstIsolated != 0 {
		t.Errorf("worst isolated Q nodes %d, the published run says 0", worstIsolated)
	}
}
