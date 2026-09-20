package overlaysim

// m3a_sweep_test.go is the DRIVER of the final M3-a measurement: what the
// mechanism does when the share of Q is not one half.
//
// Why a separate file, and why now. The scenario has existed since 2026-09-15 —
// drawSkewedPopulation, the guard model, runSkewPoint, all held by
// m3a_reference_test.go — and the sweep it suggests was written down as a
// PROPOSAL that "no test runs". The range was agreed for the stand on
// 2026-09-16 (index §0.2), and the report of 2026-09-19 still had to say M3-a
// had never been run, because nothing existed that walked the proposal.
//
// What this file does: walks it, and writes every point into the journal so the
// sweep survives the ≈180 s ceiling on one call of this environment. It decides
// nothing. No acceptable skew is proposed here and none can be — the acceptable
// skew is the RESULT of this experiment, not an input to it (§5.5.5).
//
// ⚠️ WHAT IS AGREED AND WHAT IS NOT, because the two travel together in every
// line of output:
//
//	AGREED for the stand (index §0.2, 2026-09-16): the range and step of the Q
//	    share, the form, the seeds, the number of requesters.
//	NOT AGREED: the GUARD MODEL — set size, what counts as confirmed, the
//	    normative population, whether alive/transit/identity stay declared, how
//	    a set is formed, the workload (registry §6, open decision 4). So every
//	    number carries guardModel.Assumptions(), and the list of open questions
//	    is printed once per sweep. A number produced under an unagreed model is
//	    not wrong — it is unreadable until the model is named, and naming it is
//	    what these lines do.
//
// ⚠️ ONE GRAPH PER POINT, and no cache — so this sweep builds 380 graphs where
// registry §4.1 counts 190. Two loads share a (shape, seed, share, quota,
// policy) graph, so caching would halve the build cost; it would also mean
// reimplementing the body of runSkewPoint here, that is, duplicating an
// instrument that has been read and accepted. The accepted model is not touched
// without a concrete defect (task of 2026-09-19), so the sweep pays the second
// build. The number is printed in the header of every run, because a cost the
// registry states as 190 and the stand pays as 380 must not be discoverable only
// by reading the code.

import (
	"fmt"
	"strings"
	"testing"
)

// m3aAgreedSweep is the sweep as agreed for the stand on 2026-09-16. It is the
// proposal object itself, not a copy of its numbers: two copies of one
// parameter set drift, and the one that drifts is the one nobody looks at.
func m3aAgreedSweep() m3aProposal { return proposedSkewSweep() }

// m3aSweepConfig is one point of it.
type m3aSweepConfig struct {
	Shape  shape
	Seed   uint64
	Share  float64
	Policy policy
	Load   skewWorkload
}

// shareToken renders the share the way the run key and the report both spell
// it. ⚠️ ONE renderer: a key that says 0.15 and a report that says 0.150 are a
// key and a report that cannot be joined.
func shareToken(share float64) string { return fmt.Sprintf("%.2f", share) }

func (c m3aSweepConfig) loadToken() string {
	if c.Load == skewUniformTargets {
		return "uniform"
	}
	return "targets-the-confirmed-guard"
}

// setup is the point as the accepted instrument takes it.
func (c m3aSweepConfig) setup() skewSetup {
	sweep := m3aAgreedSweep()
	return skewSetup{
		Shape:          c.Shape,
		Seed:           c.Seed,
		ShuffleSeed:    sweep.ShuffleSeed,
		RequestedShare: c.Share,
		Quota:          sweep.Quota,
		Policy:         c.Policy,
		Guards:         sweep.Guards,
		Workload:       c.Load,
		Requesters:     sweep.Requesters,
		RequesterSeed:  sweep.RequesterSeed,
	}
}

// Key is the point as the journal sees it. Every input that changes a number is
// here, the guard model included — a run made under another set size is another
// measurement and must not be resumed as this one.
func (c m3aSweepConfig) Key(sources string) runKey {
	setup := c.setup()
	return runKey{
		Measurement: "M3-a",
		Label: fmt.Sprintf("%s/%s/seed%d/Q%s/%s",
			c.Shape.name, c.Policy, c.Seed, shareToken(c.Share), c.loadToken()),
		Params: []runParam{
			{Name: "shape", Value: c.Shape.name},
			{Name: "nodes", Value: fmt.Sprint(c.Shape.nodes)},
			{Name: "degree_d", Value: fmt.Sprint(c.Shape.degree)},
			{Name: "budget_B", Value: fmt.Sprint(c.Shape.budget)},
			{Name: "seed", Value: fmt.Sprint(c.Seed)},
			{Name: "requested_q_share", Value: shareToken(c.Share)},
			{Name: "policy", Value: c.Policy.String()},
			{Name: "quota", Value: fmt.Sprint(setup.Quota)},
			{Name: "load", Value: c.loadToken()},
			{Name: "shuffle_seed", Value: fmt.Sprint(setup.ShuffleSeed)},
			{Name: "requesters", Value: fmt.Sprint(setup.Requesters)},
			{Name: "requester_seed", Value: fmt.Sprint(setup.RequesterSeed)},
			{Name: "guard_set_size_k", Value: fmt.Sprint(setup.Guards.SetSize)},
			{Name: "guard_confirmed_prefix", Value: fmt.Sprint(setup.Guards.ConfirmedPrefix)},
			{Name: "requests_per_requester", Value: fmt.Sprint(setup.Guards.Requests)},
			{Name: "target_seed", Value: fmt.Sprint(setup.Guards.TargetSeed)},
		},
		Sources: sources,
	}
}

// m3aSweepEnumeration lists every point, in the order a range cuts them.
//
// ⚠️ The order is policy → seed → share → load. The two loads of one point are
// then adjacent, which is what a reader of a partial sweep needs (a share
// measured under one load and not the other says nothing about the open
// sub-question), and the policies are the outermost axis because one of them
// costs ≈6 s a graph and the other ≈27 ms — a range that mixes them cannot be
// sized.
func m3aSweepEnumeration(sweep m3aProposal) []m3aSweepConfig {
	points := sweep.Points()
	configs := make([]m3aSweepConfig, 0,
		len(sweep.Shapes)*len(sweep.Policies)*len(sweep.Seeds)*len(points)*len(sweep.Workloads))
	for _, sh := range sweep.Shapes {
		for _, selection := range sweep.Policies {
			for _, seed := range sweep.Seeds {
				for _, share := range points {
					for _, load := range sweep.Workloads {
						configs = append(configs, m3aSweepConfig{
							Shape: sh, Seed: seed, Share: share, Policy: selection, Load: load,
						})
					}
				}
			}
		}
	}
	return configs
}

// m3aRunBody is one measured point as the journal stores it.
//
// ⚠️ The connectivity of an EMPTY Q-subgraph is stored as the words describeConnectivity
// produces, "no data" included: a stored "0 components" would be read later as a
// connected subgraph, and at a Q share where the structural plane cannot exist
// that is the worst possible reading.
func m3aRunBody(run m3aRun) []string {
	return []string{
		fmt.Sprintf("population requested_q_share=%s actual_q_share=%.4f structural=%d nodes=%d drawn=%d",
			shareToken(run.Setup.RequestedShare), run.Population.ActualShare(),
			run.Population.Structural, run.Population.Nodes, run.Population.Drawn),
		fmt.Sprintf("q_subgraph %s", describeConnectivity(run.Connectivity)),
		fmt.Sprintf("m5_all without=%d targets=%d", run.Neighbourless.All.Without,
			run.Neighbourless.All.Targets),
		fmt.Sprintf("m5_structural without=%d targets=%d", run.Neighbourless.Structural.Without,
			run.Neighbourless.Structural.Targets),
		fmt.Sprintf("m5_non_structural without=%d targets=%d",
			run.Neighbourless.NonStructural.Without, run.Neighbourless.NonStructural.Targets),
		fmt.Sprintf("guards requesters=%d short_of_set=%d members=%d confirmed=%d digest=%s",
			run.RequestersMeasured, run.RequestersShortOfGuards, run.MembersTotal,
			run.ConfirmedTotal, run.SetsDigest),
		fmt.Sprintf("confirmed_only requests=%d served=%d refused=%d nobody_suitable=%d by_target_exclusion=%d",
			run.RefusalsConfirmed.Requests, run.RefusalsConfirmed.Served,
			run.RefusalsConfirmed.Refused(), run.RefusalsConfirmed.RefusedNobodySuitable,
			run.RefusalsConfirmed.RefusedByTargetExclusion),
		fmt.Sprintf("whole_set requests=%d served=%d refused=%d nobody_suitable=%d by_target_exclusion=%d",
			run.RefusalsSampled.Requests, run.RefusalsSampled.Served,
			run.RefusalsSampled.Refused(), run.RefusalsSampled.RefusedNobodySuitable,
			run.RefusalsSampled.RefusedByTargetExclusion),
		fmt.Sprintf("gap_pp=%s", run.RefusalGapPP()),
		"bound=lower — alive, transit-capable and identity-proven are DECLARED by the guard " +
			"model, not observed in any network",
	}
}

// TestM3ASkewSweepOnTheCandidate is the run.
func TestM3ASkewSweepOnTheCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("the skew sweep is a measurement, not a unit test")
	}

	selection := runSelectionFrom(t, "M3A")
	sweep := m3aAgreedSweep()
	configs := m3aSweepEnumeration(sweep)

	keys := make([]runKey, 0, len(configs))
	for _, config := range configs {
		keys = append(keys, config.Key(selection.Sources))
	}
	if err := requireDistinctKeys(keys); err != nil {
		t.Fatalf("the M3-a enumeration is not an enumeration: %v", err)
	}
	if selection.Listing {
		t.Log(enumerationListing("M3-a — sensitivity to role skew", keys))
		return
	}
	if !selection.requireACutOrAnExplicitAll(t, len(configs),
		"≈6 s a graph under C1/v1 on 10k×8, two graphs per (share, seed) — ≈20 minutes in all") {
		return
	}

	var out strings.Builder
	fmt.Fprintf(&out, "\nM3-a — sensitivity to role skew\n")
	fmt.Fprintf(&out, "contract: docs/refactoring/dht/21-m1-candidate-c1.md §5.5.5; "+
		"21-anonymity-transport.md §4.3.4″.6; range agreed for the stand 2026-09-16 (index §0.2)\n\n")
	fmt.Fprintf(&out, "inputs\n  %s\n", strings.ReplaceAll(sweep.String(), "\n", "\n  "))
	fmt.Fprintf(&out, "\n%s\n", selection.Header(len(configs)))
	fmt.Fprintf(&out, "  ⚠️ The Q share is an INPUT. M3-a measures the consequences of a skew it is "+
		"given; it says nothing about the skew a live network has — the identifiers are ours, so "+
		"their split is a property of our generator (§4.3.4″.6).\n")
	fmt.Fprintf(&out, "  ⚠️ Both readings of §4.3.4″.3 are kept for every point, over the same "+
		"requests. The normative population is OPEN and this driver takes no side.\n")
	fmt.Fprintf(&out, "  ⚠️ COST: %d graphs are built, not the %d the registry §4.1 counts — the "+
		"two loads of one point share a graph and this sweep does NOT cache it, because caching "+
		"would mean duplicating the accepted runSkewPoint here.\n\n",
		len(configs), len(configs)/len(sweep.Workloads))

	fmt.Fprintf(&out, "%-64s %s\n", "point",
		"Q actual · Q-subgraph · M5 all/Q · refusal (confirmed | whole set) · gap · short sets")

	runSweep(t, selection, keys, &out, func(index int, _ runKey) ([]string, string, error) {
		run, err := runSkewPoint(configs[index].setup())
		if err != nil {
			return nil, "", err
		}
		headline := fmt.Sprintf("Q %.4f · %s · M5 all %s / Q %s · %s | %s · gap %s · %d/%d short",
			run.Population.ActualShare(), describeConnectivity(run.Connectivity),
			run.Neighbourless.All.Share(), run.Neighbourless.Structural.Share(),
			run.RefusalsConfirmed.RefusalShare(), run.RefusalsSampled.RefusalShare(),
			run.RefusalGapPP(), run.RequestersShortOfGuards, run.RequestersMeasured)
		return m3aRunBody(run), headline, nil
	})

	fmt.Fprintf(&out, "\n  ⚠️ No acceptable skew is proposed and none can be: it is the RESULT of "+
		"this experiment (§5.5.5).\n")

	t.Log(out.String())
}
