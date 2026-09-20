package overlaysim

// m4_sweep_test.go is the DRIVER of the final M4 measurement: how often the
// anonymous mode refuses because the rule of §4.3.4″.3 left no first hop to use.
//
// Why a separate file, and why now. The instrument has existed since
// 2026-09-15 — selectFirstHop, measureFirstHopRefusals, the three workloads, the
// set snapshot — and every one of them is held by m4_reference_test.go. What did
// NOT exist is the loop that builds the candidate's graphs, forms a guard set
// per requester and puts the three agreed loads to them. Without it "M4 is
// ready" meant "the instrument is ready", and the report of 2026-09-19 had to
// say the measurement had never been run.
//
// This file therefore builds, forms, asks and writes down. It decides nothing:
// no threshold of acceptable refusal, no normative population, no acceptance of
// C1/v1. It fails only on a stand defect — a guard set the graph cannot
// describe, two populations that answered different requests — because "the
// candidate refuses often" is a RESULT and results do not fail tests.
//
// ⚠️ THE TWO POPULATIONS ARE THE POINT, and they are kept apart everywhere:
// in the run, in the record and in the report. §4.3.4″.3 leaves open whether an
// unconfirmed member may be taken when the target occupies the confirmed one,
// and says the value is MEASURED. So every configuration produces BOTH readings
// over the SAME requests, plus their difference in percentage points, and the
// driver asserts the two answered the same questions rather than trusting that
// they did. A driver that reported one of them would have answered the owner's
// open question by omission — which is exactly what the index forbids: the
// decision on the normative population blocks the READING of these numbers, not
// their production.
//
// ⚠️ AND THE CEILING ON THE WHOLE THING: alive, transit-capable and
// identity-proven are DECLARED by the guard model, not observed — the M1 model
// builds neighbourhoods and knows none of the three. Every refusal share here is
// therefore a LOWER BOUND and is published as one, in the header of every run.

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"os"
	"strings"
	"testing"
)

// m4SweepShape is the form M4 is measured on.
//
// ⚠️ 10k×8, because the registry (§4.2) puts M4's graph builds in the same
// family as M3-a's and because the base of the comparison, initiated-limit, has
// its published numbers there. It is an input, not a finding, and M4_SHAPES
// overrules it — the number is in the header of every run so no table can be
// read without it.
func m4SweepShape() shape { return sweepShapes[1] }

// m4SweepQuota is the quota the base configuration is measured at, and it is
// the same 1 the M3-a proposal names (§5.5.5). Quota changes the graph, so it
// is a parameter of the run key.
const m4SweepQuota = 1

// m4SweepRequesters is how many nodes have their first-hop rule measured, and
// m4SweepRequesterSeed decides WHICH ones.
//
// ⚠️ Not a prefix of the index order. Index order is construction order, and
// construction order decides who ran out of budget first; a prefix would add a
// bias on top of the measurement (§5.5.5 says this in full for M3-a, and the
// reason does not change because the population is the natural one here).
//
// ⚠️ ARITHMETIC, stated because it differs from one line of the registry.
// Registry §4.2 estimates M4 at "30 × 200 × 2 = 12 000 requests", which counts
// ONE guard set per run. A set in this stand is formed from a node's ¬Q
// neighbours, so a set belongs to a requester, and measuring one arbitrary
// requester would be a hidden choice of exactly the kind §5.5.4 refuses. This
// driver therefore measures 200 requesters per run, and the request count is
// 30 × 200 requesters × 200 requests × 2 populations = 2 400 000 — printed in
// the header, cheap (the rule is a filter over a handful of members), and NOT a
// change to the agreed grid: the registry itself says no line of it is a
// commitment to run exactly that many.
const (
	m4SweepRequesters    = 200
	m4SweepRequesterSeed = 202
)

// m4SweepGuards is the guard model the two ordinary loads are measured under —
// the same proposal M3-a carries (§5.5.5), because two measurements of one rule
// under two different assumed sets cannot be read together.
//
// ⚠️ A PROPOSAL. Item 4 of the registry's open decisions is exactly this model,
// and it is printed with every number it produced.
func m4SweepGuards() guardModel {
	return guardModel{SetSize: 3, ConfirmedPrefix: 1, Requests: 200, TargetSeed: 7}
}

// m4PopularHead is the shape of the second load: a head of `size` contacts takes
// `share` of the requests. ⚠️ Declared, not derived from anything.
const (
	m4PopularHeadSize  = 16
	m4PopularHeadShare = 0.5
)

// m4SweepLoad is one of the three loads §5.5.4 names. They are an enum rather
// than three workload values because the third one CHANGES THE GUARD MODEL —
// k = 1 is part of the load, not a target distribution — and a list of
// workloads could not carry that.
type m4SweepLoad int

const (
	// m4LoadUniform — every node of the network is equally likely to be a
	// target. The optimistic end: the set is a handful of nodes, so the chance
	// that a target IS one of them is small.
	m4LoadUniform m4SweepLoad = iota
	// m4LoadPopular — a small head of popular contacts takes a fixed share.
	m4LoadPopular
	// m4LoadMainContactInSet — the degenerate case of §4.3.4″.3 (S24а): the one
	// contact the user talks to is itself the single member of the set. With
	// k = 1 the refusal is deterministic, and the measurer must report it as
	// 100 % by EXCLUSION rather than as an average softened by other targets.
	m4LoadMainContactInSet
)

func (l m4SweepLoad) String() string {
	switch l {
	case m4LoadUniform:
		return "uniform targets"
	case m4LoadPopular:
		return "popular contacts"
	default:
		return "main contact inside the set, k=1"
	}
}

// guards says which guard model the load is measured under. Only the third one
// moves it, and it moves it because k = 1 IS the load.
func (l m4SweepLoad) guards() guardModel {
	model := m4SweepGuards()
	if l == m4LoadMainContactInSet {
		model.SetSize, model.ConfirmedPrefix = 1, 1
	}
	return model
}

// m4SweepPolicies is the pair M4 compares: the candidate whose acceptance
// numbers these are, and the base it differs from by ONE rule. baseline is not
// here — its role is the golden control of the stand (§2.1 of the registry), not
// a comparand.
var m4SweepPolicies = []policy{policyCandidateC1, policyInitiatedLimit}

// m4SweepConfig is one configuration of the M4 grid.
type m4SweepConfig struct {
	Shape  shape
	Seed   uint64
	Policy policy
	Load   m4SweepLoad
}

// Key is the configuration as the journal sees it.
//
// ⚠️ Every field that changes the result is in here, including the guard model
// and the head of the popular load. A parameter left out is a parameter a
// resumed sweep would not notice had changed.
func (c m4SweepConfig) Key(sources string) runKey {
	model := c.Load.guards()
	params := []runParam{
		{Name: "shape", Value: c.Shape.name},
		{Name: "nodes", Value: fmt.Sprint(c.Shape.nodes)},
		{Name: "degree_d", Value: fmt.Sprint(c.Shape.degree)},
		{Name: "budget_B", Value: fmt.Sprint(c.Shape.budget)},
		{Name: "seed", Value: fmt.Sprint(c.Seed)},
		{Name: "policy", Value: c.Policy.String()},
		{Name: "quota", Value: fmt.Sprint(m4SweepQuota)},
		{Name: "load", Value: c.Load.String()},
		{Name: "guard_set_size_k", Value: fmt.Sprint(model.SetSize)},
		{Name: "guard_confirmed_prefix", Value: fmt.Sprint(model.ConfirmedPrefix)},
		{Name: "requests_per_requester", Value: fmt.Sprint(model.Requests)},
		{Name: "target_seed", Value: fmt.Sprint(model.TargetSeed)},
		{Name: "requesters", Value: fmt.Sprint(m4SweepRequesters)},
		{Name: "requester_seed", Value: fmt.Sprint(m4SweepRequesterSeed)},
	}
	if c.Load == m4LoadPopular {
		params = append(params,
			runParam{Name: "head_size", Value: fmt.Sprint(m4PopularHeadSize)},
			runParam{Name: "head_share", Value: fmt.Sprintf("%.2f", m4PopularHeadShare)})
	}
	return runKey{
		Measurement: "M4",
		Label:       fmt.Sprintf("%s/%s/seed%d/%s", c.Shape.name, c.Policy, c.Seed, c.Load),
		Params:      params,
		Sources:     sources,
	}
}

// m4SweepEnumeration lists every configuration, in the order a range cuts them.
//
// ⚠️ The order is policy → seed → load ON PURPOSE: the three loads of one
// (policy, seed) share a graph, so a contiguous range of this enumeration builds
// the fewest graphs. The order is part of the contract of _RANGE and does not
// change without the identifiers staying put — they are derived from the
// parameters, not from the position, so reordering costs nothing but a re-cut of
// the batch.
func m4SweepEnumeration(shapes []shape) []m4SweepConfig {
	configs := make([]m4SweepConfig, 0, len(shapes)*len(m4SweepPolicies)*len(sweepSeeds)*3)
	for _, sh := range shapes {
		for _, selection := range m4SweepPolicies {
			for _, seed := range sweepSeeds {
				for _, load := range []m4SweepLoad{
					m4LoadUniform, m4LoadPopular, m4LoadMainContactInSet,
				} {
					configs = append(configs, m4SweepConfig{
						Shape: sh, Seed: seed, Policy: selection, Load: load,
					})
				}
			}
		}
	}
	return configs
}

// m4AggregateRun is what one configuration produced.
type m4AggregateRun struct {
	Config m4SweepConfig
	// Confirmed and Sampled are the two readings of §4.3.4″.3 over the SAME
	// requests. ⚠️ Never summed, never merged, never one without the other.
	Confirmed, Sampled m4Slice
	// RequestersMeasured is the denominator in requesters and ShortOfSet how
	// many of them could not fill the set size the model asked for — an
	// undersized set is a measured fact, not an invisible truncation.
	RequestersMeasured, ShortOfSet int
	MembersTotal, ConfirmedTotal   int
	// SetsDigest identifies the COMPOSITION of every set that entered the two
	// aggregates. Two runs that measured the same sets in a different order
	// measured different experiments: the order decides who carries a served
	// request.
	SetsDigest string
}

// GapPP is the price of the open decision, in percentage points, over the same
// requests.
func (r m4AggregateRun) GapPP() string { return refusalGap(r.Confirmed, r.Sampled) }

// Body is the run as the journal stores it: one fact per line, no rendering
// that a later reader would have to un-parse.
func (r m4AggregateRun) Body() []string {
	return []string{
		fmt.Sprintf("requesters_measured=%d short_of_set=%d members_total=%d confirmed_total=%d sets_digest=%s",
			r.RequestersMeasured, r.ShortOfSet, r.MembersTotal, r.ConfirmedTotal, r.SetsDigest),
		fmt.Sprintf("confirmed_only requests=%d served=%d refused=%d nobody_suitable=%d by_target_exclusion=%d",
			r.Confirmed.Requests, r.Confirmed.Served, r.Confirmed.Refused(),
			r.Confirmed.RefusedNobodySuitable, r.Confirmed.RefusedByTargetExclusion),
		fmt.Sprintf("whole_set requests=%d served=%d refused=%d nobody_suitable=%d by_target_exclusion=%d",
			r.Sampled.Requests, r.Sampled.Served, r.Sampled.Refused(),
			r.Sampled.RefusedNobodySuitable, r.Sampled.RefusedByTargetExclusion),
		fmt.Sprintf("gap_pp=%s", r.GapPP()),
		"bound=lower — alive, transit-capable and identity-proven are DECLARED by the guard model, " +
			"not observed in any network",
	}
}

// measureM4Configuration is the measurement itself: form a set per sampled
// requester, put the load to it, accumulate BOTH readings.
//
// ⚠️ It does not modify the graph and does not modify a set. Rule 2 of
// §4.3.4″.3 — the set is not changed and not topped up — is enforced in
// selectFirstHop by having nowhere else to go; here the sets are built once per
// requester and handed over by value.
func measureM4Configuration(g *graph, config m4SweepConfig) (m4AggregateRun, error) {
	model := config.Load.guards()
	run := m4AggregateRun{Config: config}
	composition := newSetsDigest()

	uniform := uniformWorkload(model.TargetSeed, config.Shape.nodes, model.Requests)
	popular := popularContactWorkload(model.TargetSeed, config.Shape.nodes, model.Requests,
		m4PopularHeadSize, m4PopularHeadShare)

	for _, requester := range sampleRequesters(config.Shape.nodes, m4SweepRequesters,
		m4SweepRequesterSeed) {
		set := buildGuardSet(g, requester, model)
		if len(set.Members) < model.SetSize {
			run.ShortOfSet++
		}
		composition.add(snapshotGuardSet(g, set))
		run.MembersTotal += len(set.Members)
		for _, member := range set.Members {
			if member.Confirmed {
				run.ConfirmedTotal++
			}
		}

		load := uniform
		switch config.Load {
		case m4LoadPopular:
			load = popular
		case m4LoadMainContactInSet:
			// Every request aims at the set's own member — with k = 1 that is
			// S24а exactly. confirmedGuardWorkload names the target and the
			// reason in its parameters, including the two degenerate cases (no
			// confirmed member, empty set), so a refusal can never be read
			// without knowing what it was asked.
			load = confirmedGuardWorkload(set, model.Requests)
		case m4LoadUniform:
		}

		report, err := measureFirstHopRefusals(g, set, load)
		if err != nil {
			return m4AggregateRun{}, err
		}
		addRefusals(&run.Confirmed, report.Confirmed)
		addRefusals(&run.Sampled, report.Sampled)
		run.RequestersMeasured++
	}
	run.SetsDigest = composition.digest()

	// ⚠️ A stand defect, not a result: the two readings must have answered the
	// SAME requests, or the gap between them is a difference in what was asked.
	if run.Confirmed.Requests != run.Sampled.Requests {
		return m4AggregateRun{}, fmt.Errorf(
			"the confirmed-only reading answered %d requests and the whole-set reading %d — the two "+
				"populations must see one workload, or their difference means nothing",
			run.Confirmed.Requests, run.Sampled.Requests)
	}
	return run, nil
}

// m4ShapesFromEnvironment selects the forms, because one call of this stand has
// a wall-clock ceiling and a sweep has to be cut somewhere the report can name.
func m4ShapesFromEnvironment(t *testing.T) []shape {
	t.Helper()
	return shapesNamed(t, "M4_SHAPES", []shape{m4SweepShape()})
}

// TestM4FirstHopRefusalsOnTheCandidate is the run.
func TestM4FirstHopRefusalsOnTheCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("the refusal measurement is a measurement, not a unit test")
	}

	selection := runSelectionFrom(t, "M4")
	configs := m4SweepEnumeration(m4ShapesFromEnvironment(t))

	keys := make([]runKey, 0, len(configs))
	for _, config := range configs {
		keys = append(keys, config.Key(selection.Sources))
	}
	if err := requireDistinctKeys(keys); err != nil {
		t.Fatalf("the M4 enumeration is not an enumeration: %v", err)
	}
	if selection.Listing {
		t.Log(enumerationListing("M4 — first-hop refusals", keys))
		return
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\nM4 — first-hop refusal rate of the rule H₁ = B\n")
	fmt.Fprintf(&out, "contract: docs/refactoring/dht/21-m1-candidate-c1.md §5.5.4; "+
		"21-anonymity-transport.md §4.3.4″.3, §4.3.4″.6\n\n")
	fmt.Fprintf(&out, "inputs\n")
	fmt.Fprintf(&out, "  quota %d, seeds %v, rules %v — the candidate first, its base on identical "+
		"inputs\n", m4SweepQuota, sweepSeeds, m4SweepPolicies)
	fmt.Fprintf(&out, "  %d requesters per run, sampled with seed %d independently of construction "+
		"order; %d requests per requester\n",
		m4SweepRequesters, m4SweepRequesterSeed, m4SweepGuards().Requests)
	fmt.Fprintf(&out, "  %s\n", m4SweepGuards().Assumptions())
	fmt.Fprintf(&out, "  %s\n", m4DeclaredNotObserved)
	fmt.Fprintf(&out, "%s\n", selection.Header(len(configs)))
	fmt.Fprintf(&out, "  ⚠️ BOTH populations are reported for every configuration, over the same "+
		"requests, with their difference in percentage points. The normative population is OPEN "+
		"(§4.3.4″.3) and this driver does not take a side; the decision blocks the READING of these "+
		"numbers, not their production.\n\n")

	// One graph serves the three loads of a (shape, policy, seed), and the
	// enumeration is ordered so they are adjacent. A one-entry memo is all that
	// is needed: it keeps a contiguous range from building the same graph three
	// times, and holds no graph a range has moved past.
	var cachedGraph *graph
	cachedFor := ""

	fmt.Fprintf(&out, "%-64s %s\n", "configuration",
		"short sets · refusal (confirmed only) · refusal (whole set) · gap")

	runSweep(t, selection, keys, &out, func(index int, _ runKey) ([]string, string, error) {
		config := configs[index]
		if want := graphCacheKey(config.Shape, config.Seed, config.Policy); cachedFor != want {
			cachedGraph = buildGraph(config.Shape, config.Seed, m4SweepQuota, config.Policy)
			cachedFor = want
		}
		run, err := measureM4Configuration(cachedGraph, config)
		if err != nil {
			return nil, "", err
		}
		headline := fmt.Sprintf("%d/%d short · confirmed %s · whole set %s · gap %s",
			run.ShortOfSet, run.RequestersMeasured,
			run.Confirmed.RefusalShare(), run.Sampled.RefusalShare(), run.GapPP())
		return run.Body(), headline, nil
	})

	fmt.Fprintf(&out, "\n  ⚠️ No threshold is applied and none exists: the acceptable refusal rate "+
		"is the owner's (§4.3.4″.8), and the registry holds none.\n")
	fmt.Fprintf(&out, "  ⚠️ Every share above is a LOWER BOUND — see the declared-not-observed note "+
		"in the inputs.\n")

	t.Log(out.String())
}

// setsDigest folds the COMPOSITION of every guard set that entered a run into
// one token.
//
// ⚠️ It exists because an aggregate over two hundred sets cannot carry two
// hundred listings, and with nothing at all the difference between two runs
// would be unexplainable — the gap between the two readings is a fact about the
// MEMBERS. The digest does not explain the gap; it proves whether two runs used
// the same sets. ⚠️ Order is included: the order decides who carries a served
// request, so two runs that measured the same sets in a different order
// measured different experiments.
type setsDigest struct{ folded hash.Hash }

func newSetsDigest() *setsDigest { return &setsDigest{folded: sha256.New()} }

func (d *setsDigest) add(snapshot guardSetSnapshot) {
	// hash.Hash.Write never returns an error, which is why the rest of the
	// stand folds compositions the same way.
	d.folded.Write(append([]byte(snapshot.Composition()), 0))
}

func (d *setsDigest) digest() string { return fmt.Sprintf("%x", d.folded.Sum(nil)[:4]) }

// --- small shared helpers ------------------------------------------------------

// graphCacheKey names a built graph, so a driver can tell whether the one it
// holds is the one it wants.
func graphCacheKey(sh shape, seed uint64, selection policy) string {
	return fmt.Sprintf("%s/%d/%s", sh.name, seed, selection)
}

// shapesNamed reads a comma-separated list of sweep shapes from an environment
// variable. A name the sweep does not know is FATAL: an operator who asked for
// one form and silently got another cannot tell from the report, which is
// correct about a run nobody ordered.
func shapesNamed(t *testing.T, variable string, fallback []shape) []shape {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(variable))
	if raw == "" {
		return fallback
	}
	byName := map[string]shape{}
	for _, sh := range sweepShapes {
		byName[sh.name] = sh
	}
	selected := make([]shape, 0, len(sweepShapes))
	for _, field := range strings.Split(raw, ",") {
		name := strings.TrimSpace(field)
		sh, known := byName[name]
		if !known {
			t.Fatalf("%s=%q names %q, which is not one of the sweep shapes", variable, raw, name)
		}
		selected = append(selected, sh)
	}
	return selected
}
