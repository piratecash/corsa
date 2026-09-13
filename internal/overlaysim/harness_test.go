package overlaysim

// harness_test.go is the acceptance of the STAND, not of the result
// (docs/refactoring/dht/21-m1-connectivity-model.md §7): known graphs, seed
// reproducibility, agreement with the published Q contract, and the budget
// never being exceeded.
//
// ⚠️ A green run here says the numbers are trustworthy. It says nothing about
// whether they are acceptable — that threshold belongs to the owner
// (21-anonymity-transport.md §4.3.4″.8 п.1), and O5 stays open either way.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// --- known graphs -----------------------------------------------------------

// ringGraph builds n nodes in a cycle. Every node is a member of the analysed
// set, so this is the simplest connected case.
func ringGraph(n int, roles []int) *graph {
	g := &graph{
		ids:                  make([]nodeID, n),
		roles:                roles,
		adjacency:            make([][]int32, n),
		structuralNeighbours: make([]int, n),
	}
	if n < 2 {
		return g
	}
	for i := range n {
		next := int32((i + 1) % n)
		g.adjacency[i] = append(g.adjacency[i], next)
		g.adjacency[next] = append(g.adjacency[next], int32(i))
	}
	return g
}

func allStructural(n int) []int {
	roles := make([]int, n)
	for i := range roles {
		roles[i] = roleStructural
	}
	return roles
}

func TestComponentsOnKnownGraphs(t *testing.T) {
	t.Parallel()

	everyone := func(int32) bool { return true }

	t.Run("a ring is one component", func(t *testing.T) {
		t.Parallel()
		got := analyseComponents(ringGraph(16, allStructural(16)), everyone)
		if got.Components != 1 || got.Largest != 16 || got.Isolated != 0 {
			t.Fatalf("ring of 16: %+v", got)
		}
		if share := got.LargestShare(); share != 1 {
			t.Fatalf("ring of 16: largest share %v, want 1", share)
		}
	})

	t.Run("two disjoint rings are two components", func(t *testing.T) {
		t.Parallel()
		// Two rings of eight, built by hand so the disconnection is a fact of
		// the fixture rather than an emergent property nobody checked.
		g := &graph{roles: allStructural(16), adjacency: make([][]int32, 16)}
		link := func(a, b int32) {
			g.adjacency[a] = append(g.adjacency[a], b)
			g.adjacency[b] = append(g.adjacency[b], a)
		}
		for offset := 0; offset < 16; offset += 8 {
			for i := range 8 {
				link(int32(offset+i), int32(offset+(i+1)%8))
			}
		}

		got := analyseComponents(g, everyone)
		if got.Components != 2 || got.Largest != 8 || got.Isolated != 0 {
			t.Fatalf("two rings of 8: %+v", got)
		}
		if share := got.LargestShare(); share != 0.5 {
			t.Fatalf("two rings of 8: largest share %v, want 0.5", share)
		}
	})

	t.Run("a node with no edges is isolated", func(t *testing.T) {
		t.Parallel()
		g := ringGraph(8, allStructural(9))
		g.adjacency = append(g.adjacency, nil) // node 8, no edges

		got := analyseComponents(g, everyone)
		if got.Components != 2 || got.Largest != 8 || got.Isolated != 1 {
			t.Fatalf("ring of 8 plus a loner: %+v", got)
		}
	})

	t.Run("an empty set reports nothing rather than dividing by zero", func(t *testing.T) {
		t.Parallel()
		got := analyseComponents(ringGraph(8, allStructural(8)), func(int32) bool { return false })
		if got.Nodes != 0 || got.Components != 0 || got.LargestShare() != 0 {
			t.Fatalf("empty selection: %+v", got)
		}
	})

	t.Run("the induced subgraph does not escape through the other half", func(t *testing.T) {
		t.Parallel()
		// A ring where every second node is ¬Q: the structural nodes are
		// pairwise non-adjacent, so the induced subgraph is all isolated
		// nodes. A walk that followed edges through ¬Q would call it connected.
		roles := make([]int, 16)
		for i := range roles {
			if i%2 == 0 {
				roles[i] = roleStructural
			}
		}
		g := ringGraph(16, roles)

		got := analyseComponents(g, func(i int32) bool { return g.roles[i] == roleStructural })
		if got.Nodes != 8 || got.Components != 8 || got.Isolated != 8 {
			t.Fatalf("alternating ring: %+v, want 8 isolated structural nodes", got)
		}
	})
}

// --- the classifier is the contract, not a variant of it --------------------

// contractVectors is docs/protocol/overlay_role.md §6, transcribed whole. The
// DIGEST column is the point: the harness carries its own copy of the
// classifier (model_test.go roleDigest), so the domain package's contract test
// does not protect it, and a bit-only comparison agrees with a diverged
// implementation with probability about ½ — the contract says so itself.
var contractVectors = []struct {
	name   string
	id     string
	digest string
	role   int
}{
	{"V1", "0000000000000000000000000000000000000000", "301e784ce0a1a025b83514831b0e3a097da1a859e9c3ac91da9b261982928ab6", 0},
	{"V2", "ffffffffffffffffffffffffffffffffffffffff", "2c76d911998fd56f963f2e7f3514e88c42c1d7d969da9348b0ad232cbbfaabef", 1},
	{"V3", "48312d0240502678af2c59455fa48fc04cfd751e", "753afef6fc6bdb450254436bb2dea652eafeaa4867f52c89435be26428ce44b1", 1},
	{"V4", "0d05f2139163b02febd289b3c492baeb4a58c8f2", "b483e93337be551d1b4828dde005d3a37aa342c827b824bf7d6762a3bb117447", 1},
	{"V5", "67f31ef9992f5bd32de9e7d09f262c462778e5ee", "e1cdeca809a3f93a5a77850d0c9ae8a9cb6b31389c72e93c17c1ee404eb326f1", 1},
	{"V6", "2cab5346ac6fa5dda132ecde6febf62e08d69e63", "ed82818e79f88f005627d1ec039c1c109d47f02d4c0445d67d6031a944ac7604", 0},
	{"V7", "ee792b613f849eb71c06179c85ab086efdbdc991", "ba576a6d7d9c3b4e8193590e07e0f22069d72694b0968f62a2c17aa12abb7062", 0},
	{"V8", "dc1e3462e586bbd00bfabbe15f10cc970b690cf7", "6542a4a9f84f85a7d49b8bb6d36d0e4042f43dcb6c98625a0cdea0eeca25c343", 1},
}

// v9HexTextDigest is the NEGATIVE vector of §6: the digest obtained by hashing
// the 40-character hex TEXT of V1's NodeID instead of its 20 raw bytes. It
// fixes what the answer must not be — and note that the wrong input also flips
// the role, so the mistake does not announce itself.
const v9HexTextDigest = "7182b5842b1e866982cd35a4dcfd144c9eedff707f439d09baa7a2b5f73133a9"

func mustVectorID(t *testing.T, name, encoded string) nodeID {
	t.Helper()

	raw, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(raw) != nodeIDLen {
		t.Fatalf("%s: %d bytes, a NodeID is %d", name, len(raw), nodeIDLen)
	}

	var id nodeID
	copy(id[:], raw)
	return id
}

// TestRoleMatchesThePublishedContract pins the harness to
// docs/protocol/overlay_role.md §6. Without it the simulator could measure a
// role function nobody agreed to and the numbers would describe nothing.
func TestRoleMatchesThePublishedContract(t *testing.T) {
	t.Parallel()

	t.Run("every vector reproduces its full digest and its bit", func(t *testing.T) {
		t.Parallel()

		for _, vector := range contractVectors {
			id := mustVectorID(t, vector.name, vector.id)

			digest := roleDigest(id)
			if got := hex.EncodeToString(digest[:]); got != vector.digest {
				t.Errorf("%s: digest %s, contract publishes %s", vector.name, got, vector.digest)
				continue
			}
			if got := roleOf(id); got != vector.role {
				t.Errorf("%s: role %d, contract publishes %d", vector.name, got, vector.role)
			}
		}
	})

	t.Run("both roles occur in the table", func(t *testing.T) {
		t.Parallel()

		// Without this a classifier stuck on one answer would fail only half
		// the rows — and a half-red table is easy to read as a bad vector.
		seen := map[int]int{}
		for _, vector := range contractVectors {
			id := mustVectorID(t, vector.name, vector.id)
			seen[roleOf(id)]++
		}
		if seen[0] == 0 || seen[1] == 0 {
			t.Fatalf("roles observed %v, the table publishes both", seen)
		}
	})

	t.Run("V3-V8 come from the published rule, not from this file", func(t *testing.T) {
		t.Parallel()

		// The vectors are reproducible on purpose: recomputing them here means
		// the table cannot quietly become a list of numbers whose origin nobody
		// can check.
		const vectorSeparator = "corsa/overlay/role/v1/vector/"

		for index, vector := range contractVectors[2:] {
			derived := sha256.Sum256([]byte(vectorSeparator + strconv.Itoa(index+1)))

			var id nodeID
			copy(id[:], derived[:nodeIDLen])

			if got := hex.EncodeToString(id[:]); got != vector.id {
				t.Errorf("%s: rule derives %s, the table says %s", vector.name, got, vector.id)
			}
		}
	})

	t.Run("hashing the hex text is a different answer, not the same one", func(t *testing.T) {
		t.Parallel()

		// The canonical input is 20 RAW bytes. Feeding the hex text is the
		// mistake §4 of the contract exists for, and here the harness proves it
		// is not making it: V1's raw digest and V9's text digest must differ,
		// and the harness must produce the former.
		v1 := contractVectors[0]
		id := mustVectorID(t, v1.name, v1.id)

		rawDigest := roleDigest(id)
		textDigest := sha256.Sum256(append([]byte(roleSeparator), []byte(v1.id)...))

		if hex.EncodeToString(textDigest[:]) != v9HexTextDigest {
			t.Fatalf("V9: text digest %s, contract publishes %s",
				hex.EncodeToString(textDigest[:]), v9HexTextDigest)
		}
		if rawDigest == textDigest {
			t.Fatal("raw bytes and hex text hashed to the same digest, which SHA-256 does not do")
		}
		if got := hex.EncodeToString(rawDigest[:]); got != v1.digest {
			t.Fatalf("the harness hashes the wrong form: %s, want V1 %s", got, v1.digest)
		}

		// And the bit differs too, which is exactly why the wrong input is not
		// self-announcing: 0 for the raw NodeID, 1 for its hex text.
		if roleOf(id) != 0 || int(textDigest[sha256.Size-1]&1) != 1 {
			t.Fatalf("V1 role %d, V9 role %d, want 0 and 1",
				roleOf(id), int(textDigest[sha256.Size-1]&1))
		}
	})
}

// --- the report carries what it measured ------------------------------------

// TestSweepRowReportsEveryMeasuredField is the guard against a report that
// computes a number and drops it on the floor.
//
// The saved tables ARE the result of M1 — nobody re-runs a two-minute sweep to
// answer a question about last week's run — so a field measured but not printed
// is a field that does not exist. Three of them were missing: QuotaMin and
// QuotaMedian (how DEEP the shortfall goes, as opposed to how many nodes have
// one) and Unfilled (how many never reached the desired degree at all).
func TestSweepRowReportsEveryMeasuredField(t *testing.T) {
	t.Parallel()

	// Every value is distinct and none is a prefix of another, so a row that
	// printed the wrong field cannot pass by coincidence.
	report := runReport{
		Shape:           "1k×8",
		Seed:            7,
		Quota:           3,
		Base:            componentReport{Nodes: 1000, Components: 2, Largest: 998},
		Structural:      componentReport{Nodes: 500, Components: 4, Largest: 471, Isolated: 11},
		StructuralNodes: 503,
		QuotaMet:        0.842,
		QuotaMin:        1,
		QuotaMedian:     5,
		MeanDegree:      9.25,
		MaxDegree:       14,
		AtBudget:        0.331,
		Unfilled:        0.207,
	}

	row := formatSweepRow(report)

	// ⚠️ Compared POSITION BY POSITION, not with strings.Contains. The first
	// version of this test searched the row for each value and a mutation
	// walked straight through it: printing QuotaMedian in the "q min" column
	// still left a "1" somewhere in the line, so the assertion was inert. Cells
	// hold no spaces, so splitting the row gives the columns in order.
	want := []string{
		"3",      // quota
		"7",      // seed
		"503",    // Q nodes
		"4",      // Q comps
		"0.9420", // Q share — 471 of 500
		"11",     // Q isolat
		"2",      // base cmp
		"0.842",  // quota✓
		"1",      // q min
		"5",      // q med
		"9.25",   // deg avg
		"14",     // deg max
		"33.1%",  // at B
		"20.7%",  // unfilled
	}

	got := strings.Fields(row)
	if len(got) != len(want) {
		t.Fatalf("row has %d cells, the report measures %d\n%s", len(got), len(want), row)
	}
	for i, value := range want {
		if got[i] != value {
			t.Errorf("column %d (%q): printed %q, measured %q\n%s",
				i, sweepColumns[i], got[i], value, row)
		}
	}

	t.Run("the header names exactly the columns the row prints", func(t *testing.T) {
		t.Parallel()

		// A header maintained apart from the row drifts, and a saved table
		// whose labels have shifted by one misreports every number under them.
		//
		// Column names contain spaces, so counting fields would not work.
		// Instead: fmt marks a mismatch between the format and its arguments in
		// the output itself (%!s(MISSING), %!(EXTRA …)), and equal rune width
		// is what "the columns line up" actually means.
		header := sweepHeader()

		if strings.Contains(header, "%!") {
			t.Fatalf("sweepColumns and the header format disagree:\n%s", header)
		}
		if got, want := utf8.RuneCountInString(header), utf8.RuneCountInString(row); got != want {
			t.Fatalf("header is %d runes wide, the row %d — the columns do not line up\n%s%s",
				got, want, header, row)
		}
	})
}

// TestAttributionRowKeepsTheTwoPopulationsApart pins the second table.
//
// ⚠️ Its whole reason to exist is that the two halves are DIFFERENT
// populations: every node below the quota on the left, the Q nodes with no
// structural neighbour at all on the right. The results once explained the
// second with numbers taken over the first, so a row that printed one half
// twice, or shifted the halves into each other, has to be caught here.
func TestAttributionRowKeepsTheTwoPopulationsApart(t *testing.T) {
	t.Parallel()

	// No value repeats across the two halves: a row that printed the left
	// population's share in a right-hand column would otherwise pass.
	report := runReport{
		Quota: 3,
		Seed:  7,
		ShortOfQuota: shortfallSlice{
			Nodes:                62,
			FullBeforeSearch:     0.451,
			FilledDuringSearch:   0.129,
			CandidateAtBudget:    0.613,
			NoCandidate:          0.774,
			LeftoverIgnoredQuota: 0.226,
		},
		IsolatedStructural: shortfallSlice{
			Nodes:                17,
			FullBeforeSearch:     0.882,
			FilledDuringSearch:   0.059,
			CandidateAtBudget:    0.294,
			NoCandidate:          0.353,
			LeftoverIgnoredQuota: 0.647,
		},
	}

	row := formatAttributionRow(report)
	want := []string{
		"3", "7",
		"62", "45.1%", "12.9%", "61.3%", "77.4%", "22.6%",
		"17", "88.2%", "5.9%", "29.4%", "35.3%", "64.7%",
	}

	got := strings.Fields(row)
	if len(got) != len(want) {
		t.Fatalf("row has %d cells, the attribution measures %d\n%s", len(got), len(want), row)
	}
	for i, value := range want {
		if got[i] != value {
			t.Errorf("column %d (%q): printed %q, measured %q\n%s",
				i, attributionColumns[i], got[i], value, row)
		}
	}

	header := attributionHeader()
	if strings.Contains(header, "%!") {
		t.Fatalf("attributionColumns and the header format disagree:\n%s", header)
	}
	if got, want := utf8.RuneCountInString(header), utf8.RuneCountInString(row); got != want {
		t.Fatalf("header is %d runes wide, the row %d — the columns do not line up\n%s%s",
			got, want, header, row)
	}
}

// --- why the quota went unmet -----------------------------------------------

// TestQuotaShortfallIsAttributedNotAssumed is the answer to a review finding:
// the results blamed the whole shortfall on receiving capacity B, on the
// strength of two aggregates moving in opposite directions (`at B` up,
// `quota✓` down). That is a correlation, and the model contains at least one
// other mechanism entirely — a node fills its DESIRED DEGREE with links others
// made to it and never looks for a structural neighbour at all.
func TestQuotaShortfallIsAttributedNotAssumed(t *testing.T) {
	t.Parallel()

	sh := shape{name: "10k×8", nodes: 10_000, degree: 8, budget: 16}
	const quota = 8

	g := buildGraph(sh, 1, quota, policyBaseline)

	t.Run("every node below the quota has a recorded reason", func(t *testing.T) {
		t.Parallel()

		// Completeness. An unattributed shortfall is exactly what lets a
		// plausible story stand in for a measurement, so it must be impossible
		// to have one rather than merely unlikely.
		for i := range sh.nodes {
			if g.structuralNeighbours[i] >= quota {
				continue
			}
			if !g.shortfall[i].Unmet() {
				t.Fatalf("node %d is %d short of the quota with no reason recorded: %+v",
					i, quota-g.structuralNeighbours[i], g.shortfall[i])
			}
		}
	})

	t.Run("filling up before the search is distinguished from filling up during it", func(t *testing.T) {
		t.Parallel()

		// ⚠️ The two were one flag, set after the loop, and that flag was read
		// as "the node never looked for structural neighbours". It could not
		// carry that: a node that searched, was refused, and only then reached
		// its degree landed in the same bucket. Only the pre-search case
		// supports the claim, so the two must be visibly different populations
		// and never both true of one node.
		before, during := 0, 0
		for i := range sh.nodes {
			reasons := g.shortfall[i]
			if reasons.FullBeforeSearch && reasons.FilledDuringSearch {
				t.Fatalf("node %d is marked as full both before and during its search: %+v",
					i, reasons)
			}
			if reasons.FullBeforeSearch {
				before++
				if g.initiated[i] != 0 {
					t.Fatalf("node %d was full before searching yet initiated %d links",
						i, g.initiated[i])
				}
			}
			if reasons.FilledDuringSearch {
				during++
				if g.initiated[i] == 0 {
					t.Fatalf("node %d filled up during a search it never made", i)
				}
			}
		}

		if before == 0 || during == 0 {
			t.Fatalf("before=%d during=%d — one of the two cases did not occur, so this run "+
				"cannot show the split matters", before, during)
		}
		t.Logf("full before searching: %d; filled while searching: %d", before, during)
	})

	t.Run("nodes filled by incoming links never searched at all", func(t *testing.T) {
		t.Parallel()

		// The reviewer's mechanism, shown to exist rather than argued for: a
		// node that initiated NOTHING, is short of the quota, and is marked as
		// stopped. No budget anywhere else in the network explains it — it
		// never asked anyone.
		never := 0
		for i := range sh.nodes {
			if g.initiated[i] != 0 || g.structuralNeighbours[i] >= quota {
				continue
			}
			never++

			reasons := g.shortfall[i]
			if !reasons.FullBeforeSearch {
				t.Fatalf("node %d initiated nothing and is short, but is not marked as full "+
					"before the search: %+v", i, reasons)
			}
			if reasons.FilledDuringSearch {
				t.Fatalf("node %d never searched, yet is marked as having filled up DURING a "+
					"search: %+v", i, reasons)
			}
			// It asked no one, so no refusal and no leftover slot can be
			// attributed to it. This is what keeps the columns from being four
			// names for one number.
			if reasons.CandidateAtBudget != 0 || reasons.NoCandidate != 0 ||
				reasons.LeftoverIgnoredQuota != 0 {
				t.Fatalf("node %d never initiated a link yet has candidate-side reasons: %+v",
					i, reasons)
			}
			if degree := len(g.adjacency[i]); degree < sh.degree {
				t.Fatalf("node %d stopped at degree %d, below the desired %d", i, degree, sh.degree)
			}
		}

		if never == 0 {
			t.Fatal("no node was filled purely by incoming links — the case the attribution " +
				"exists to separate did not occur, so this run proves nothing about it")
		}
		t.Logf("%d of %d nodes never initiated a link and still missed the quota", never, sh.nodes)
	})

	t.Run("a population too small to fill anyone still attributes the shortfall", func(t *testing.T) {
		t.Parallel()

		// The regime the swept shapes never enter: five nodes each wanting
		// eight neighbours run out of network entirely, so the leftover fill
		// finds nobody and gives up. Completeness has to survive that too —
		// this is where an "everyone is short and nobody knows why" report
		// would come from.
		tiny := shape{name: "5×8", nodes: 5, degree: 8, budget: 16}
		small := buildGraph(tiny, 1, tiny.degree, policyBaseline)

		short := 0
		for i := range tiny.nodes {
			if small.structuralNeighbours[i] >= tiny.degree {
				continue
			}
			short++

			if !small.shortfall[i].Unmet() {
				t.Fatalf("node %d is short in an exhausted population with no reason recorded", i)
			}
			if degree := len(small.adjacency[i]); degree >= tiny.degree {
				t.Fatalf("node %d reached degree %d in a network of %d — the fixture is not "+
					"exercising the exhausted case", i, degree, tiny.nodes)
			}
		}
		if short != tiny.nodes {
			t.Fatalf("%d of %d nodes are short of a quota nothing in this network can fill",
				short, tiny.nodes)
		}
	})

	t.Run("the two attributed populations are the ones they claim to be", func(t *testing.T) {
		t.Parallel()

		// ⚠️ The report explains ISOLATED STRUCTURAL nodes — the ones that
		// actually cost connectivity — and once did so with shares taken over
		// every node below the quota, a set that also holds ¬Q nodes and Q
		// nodes only a neighbour or two short. Re-derive both memberships from
		// the finished graph so the slices cannot quietly become the same set.
		report := measure(sh, 1, quota, policyBaseline)

		short, isolated := 0, 0
		for i := range sh.nodes {
			if g.structuralNeighbours[i] < quota {
				short++
			}
			if g.roles[i] == roleStructural && g.structuralNeighbours[i] == 0 {
				isolated++
			}
		}

		if report.ShortOfQuota.Nodes != short {
			t.Errorf("short population %d, the graph has %d", report.ShortOfQuota.Nodes, short)
		}
		if report.IsolatedStructural.Nodes != isolated {
			t.Errorf("isolated-Q population %d, the graph has %d",
				report.IsolatedStructural.Nodes, isolated)
		}
		if isolated == 0 || isolated == short {
			t.Fatalf("isolated=%d short=%d — the populations are not distinguishable in this "+
				"run, so it cannot show that reporting one for the other is wrong",
				isolated, short)
		}
	})

	t.Run("a refusal is recorded only when a structural candidate was really full", func(t *testing.T) {
		t.Parallel()

		// The budget reason must not be a catch-all. Re-derive it from the
		// finished graph: a node credited with a budget refusal must have at
		// least one structural non-neighbour sitting at the ceiling.
		checked := 0
		for i := range sh.nodes {
			if g.shortfall[i].CandidateAtBudget == 0 {
				continue
			}
			checked++

			neighbours := map[int32]struct{}{}
			for _, v := range g.adjacency[i] {
				neighbours[v] = struct{}{}
			}

			full := false
			for j := range sh.nodes {
				if int32(j) == int32(i) || g.roles[j] != roleStructural {
					continue
				}
				if _, already := neighbours[int32(j)]; already {
					continue
				}
				if len(g.adjacency[j]) >= sh.budget {
					full = true
					break
				}
			}
			if !full {
				t.Fatalf("node %d is credited with a budget refusal, but no structural "+
					"non-neighbour is at the ceiling", i)
			}
			if checked == 32 {
				break // the property is per-node; a sample is enough at O(N) each
			}
		}
	})
}

// --- reproducibility --------------------------------------------------------

func TestOneSeedReproducesOneGraph(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1000, degree: 8, budget: 16}

	first := measure(sh, 42, 2, policyBaseline)
	second := measure(sh, 42, 2, policyBaseline)

	if fmt.Sprintf("%+v", first) != fmt.Sprintf("%+v", second) {
		t.Fatalf("same seed produced different reports:\n%+v\n%+v", first, second)
	}
}

// TestDifferentSeedsProduceDifferentGraphs is the other half: without it the
// test above would also pass a harness that ignored the seed entirely, and
// every "several seeds" claim in the results would be one run repeated.
func TestDifferentSeedsProduceDifferentGraphs(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1000, degree: 8, budget: 16}

	if a, b := makeNodeID(1, 0), makeNodeID(2, 0); a == b {
		t.Fatal("the seed does not reach the identifiers")
	}

	first := measure(sh, 1, 2, policyBaseline)
	second := measure(sh, 2, 2, policyBaseline)
	if fmt.Sprintf("%+v", first) == fmt.Sprintf("%+v", second) {
		t.Fatal("two different seeds produced an identical report")
	}
}

// --- the budget is a ceiling, not a suggestion ------------------------------

func TestBudgetIsNeverExceeded(t *testing.T) {
	t.Parallel()

	shapes := []shape{
		{name: "1k×8", nodes: 1000, degree: 8, budget: 16},
		{name: "1k×8 tight", nodes: 1000, degree: 8, budget: 8},
	}

	for _, sh := range shapes {
		for quota := range sh.degree + 1 {
			g := buildGraph(sh, 7, quota, policyBaseline)
			for i := range sh.nodes {
				if degree := len(g.adjacency[i]); degree > sh.budget {
					t.Fatalf("%s quota=%d: node %d holds %d connections, budget is %d",
						sh.name, quota, i, degree, sh.budget)
				}
			}
		}
	}
}

// TestLinksAreSymmetric guards the model's own claim that a connection occupies
// a slot at both ends: an asymmetric adjacency would let the budget be honoured
// on paper while one side paid nothing.
func TestLinksAreSymmetric(t *testing.T) {
	t.Parallel()

	sh := shape{name: "500×6", nodes: 500, degree: 6, budget: 12}
	g := buildGraph(sh, 3, 2, policyBaseline)

	for i := range sh.nodes {
		for _, peer := range g.adjacency[i] {
			found := false
			for _, back := range g.adjacency[peer] {
				if back == int32(i) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("edge %d→%d has no counterpart", i, peer)
			}
		}
	}
}

// TestQuotaOnlyMovesSlotsItDoesNotAddThem is the guard on the sentence the
// model states twice: the quota is the policy under test, not a repair.
//
// If a larger quota produced a larger mean degree, the sweep would be measuring
// "what happens when nodes make more connections" — a different and much easier
// question than the one M1 asks.
func TestQuotaOnlyMovesSlotsItDoesNotAddThem(t *testing.T) {
	t.Parallel()

	sh := shape{name: "1k×8", nodes: 1000, degree: 8, budget: 16}

	// The claim is about links a node CHOOSES. Total degree may still rise
	// with the quota, because everyone aiming at the structural half means the
	// structural half is aimed at more often — that concentration is a cost the
	// report measures (MeanDegree, MaxDegree, AtBudget), not a defect.
	//
	// An earlier revision asserted the total instead and failed here, which was
	// the test doing its job: the quota loop was ignoring the desired degree
	// and buying extra connections outright.
	for quota := range sh.degree + 1 {
		got := measure(sh, 11, quota, policyBaseline)
		if got.MaxInitiated > sh.degree {
			t.Errorf("quota=%d: a node initiated %d links, desired degree is %d — the quota is "+
				"adding slots rather than redirecting them", quota, got.MaxInitiated, sh.degree)
		}
	}
}

// --- the trie answers the same question a scan would -----------------------

// TestTrieFindsTheSameNearestAsAScan is the correctness check of the shortcut
// that makes the sweep runnable at all: if the trie returned anything but the
// XOR-closest node, every graph in the report would be a different graph.
func TestTrieFindsTheSameNearestAsAScan(t *testing.T) {
	t.Parallel()

	const count = 400
	ids := make([]nodeID, count)
	for i := range ids {
		ids[i] = makeNodeID(99, i)
	}
	trie := newIDTrie(ids)

	for target := range 40 {
		found := trie.nearest(ids[target], 1, func(candidate int32) bool {
			return int(candidate) != target
		})
		if len(found) != 1 {
			t.Fatalf("node %d: trie returned %d candidates", target, len(found))
		}

		best := -1
		for candidate := range count {
			if candidate == target {
				continue
			}
			if best == -1 || xorLess(ids[target], ids[candidate], ids[best]) {
				best = candidate
			}
		}
		if int(found[0]) != best {
			t.Errorf("node %d: trie says %d, scan says %d", target, found[0], best)
		}
	}
}

// --- combining per-shape results --------------------------------------------

// TestSmallestCommonQuotaIntersectsRatherThanTakingMaxOfMinima is the guard on
// the verdict arithmetic, and it uses NON-MONOTONE inputs on purpose.
//
// Success is not monotone in the quota: raising it rebuilds the links and
// redistributes the budget, so a shape can hold together at 3 and come apart at
// 4. On the data this sweep happens to produce every shape either works
// everywhere or nowhere, which is exactly why the old max(minimum) arithmetic
// looked right — and would have printed a quota nobody measured on a different
// network.
func TestSmallestCommonQuotaIntersectsRatherThanTakingMaxOfMinima(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		working []map[int]bool
		limit   int
		want    int
		wantOK  bool
	}{
		{
			name:    "max of minima would answer a quota neither shape satisfies",
			working: []map[int]bool{{1: true, 5: true}, {2: true, 5: true}},
			limit:   8,
			want:    5,
			wantOK:  true,
			// max(min) = max(1, 2) = 2, and 2 works for neither.
		},
		{
			name:    "a gap in the middle is not filled in",
			working: []map[int]bool{{0: true, 3: true}, {3: true, 4: true}},
			limit:   8,
			want:    3,
			wantOK:  true,
		},
		{
			name:    "no common quota is a negative answer, not the largest minimum",
			working: []map[int]bool{{1: true}, {2: true}},
			limit:   8,
			wantOK:  false,
		},
		{
			name:    "quotas beyond the smallest degree are out of scope",
			working: []map[int]bool{{9: true}, {9: true}},
			limit:   8,
			wantOK:  false,
			// A shape with degree 8 cannot be asked about quota 9, so a common
			// answer of 9 would be a claim about a question never put.
		},
		{
			name:    "an empty sweep answers nothing",
			working: nil,
			limit:   8,
			wantOK:  false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, ok := smallestCommonQuota(testCase.working, testCase.limit)
			if ok != testCase.wantOK {
				t.Fatalf("found=%v, want %v (got quota %d)", ok, testCase.wantOK, got)
			}
			if ok && got != testCase.want {
				t.Errorf("quota %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestDescribeQuotaSetReportsTheSetNotAThreshold guards the rendering: printing
// "3+" where the working set is {3, 7} would be a claim about quotas 4, 5 and 6
// that the sweep never made.
func TestDescribeQuotaSetReportsTheSetNotAThreshold(t *testing.T) {
	t.Parallel()

	if got, want := describeQuotaSet(map[int]bool{}, 8), "NONE in 0..8"; got != want {
		t.Errorf("empty set: %q, want %q", got, want)
	}
	if got, want := describeQuotaSet(map[int]bool{3: true, 7: true}, 8), "3,7"; got != want {
		t.Errorf("sparse set: %q, want %q", got, want)
	}

	full := map[int]bool{}
	for quota := range 9 {
		full[quota] = true
	}
	if got, want := describeQuotaSet(full, 8), "all of 0..8"; got != want {
		t.Errorf("full set: %q, want %q", got, want)
	}
}
