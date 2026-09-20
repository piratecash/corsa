package overlaysim

// m6_sweep_test.go is the DRIVER of the final M6 measurement: the screened grid
// of §5.9.5, the controls the registry counts beside it, and the δ pairs decision
// 3.4 accepted — 520 scenario runs in all.
//
// Why a separate file, and why now. The model has existed since 2026-09-16 and
// was reviewed to completion on 2026-09-19 (core §6.4, §6.6); the phases, the
// traces, the comparator, the recorded stream and the recovery window all have
// their fixtures. What did NOT exist is the loop that enumerates the 520 and
// runs them. Without it "the stand is ready" meant "the instruments are ready",
// and the report of 2026-09-19 had to leave the recovery axis empty.
//
// ⚠️ THE ARITHMETIC IS THE CONTRACT, and it is checked in code rather than
// trusted (m6_sweep_reference_test.go):
//
//	400 = 20 configurations × 2 populations × 2 shapes × 5 seeds   (registry §5.1)
//	 80 = four controls BEYOND the grid × 2 × 2 × 5                (registry §5.2)
//	 40 = 20 δ pairs × 2 halves                                    (registry §5.2.2)
//	520 total. M2, M3-a, M4 and M5 are NOT in it and are counted on their own
//	lines — that is the mistake the four-column registry was written against.
//
// ⚠️ Three of the controls the registry names are ALREADY INSIDE the 400 and are
// not added a second time: `R` without a ceiling, `C = ∞` and the single
// exchange are axes of the screened grid. They appear here as grid variants,
// with a comment saying so, because the previous count added them twice.
//
// ⚠️ WHAT THIS DRIVER DOES NOT DO. It proposes no threshold, accepts no design,
// does not close O5 or G2, does not adopt policy 19 and does not settle the
// normative population of M4. It also does not touch the model: the accepted
// configuration fields are set, never reinterpreted. A run whose numbers look
// bad is a result; only a stand defect is recorded as a failure.
//
// ⚠️ ONE CALL OF THIS ENVIRONMENT IS CAPPED AT ≈180 s (measured: a 280 s sleep
// was killed at 177.985 s). A run of the contract plan on 1k×8 costs tens of
// seconds and fits; a run on 10k×8 is estimated at 6–7 minutes and DOES NOT. So
// the sweep is built to be cut with M6_RANGE and M6_ONLY and resumed from the
// journal, and the shapes that do not fit are named in the report rather than
// quietly dropped.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// --- the base configuration ----------------------------------------------------

// m6GridBase is the screened grid's base: branch A′, k = 4, R = 4, C = 64,
// shock churn f = 0.20, ret = 0.5, under the §5.9.1 phase plan with the
// W_rec = 1024 window of decision 3.5(ii).
//
// ⚠️ C = 64 and not ∞. Review of 2026-09-16 found the base cadence had been the
// negative control: with C = ∞ branches A and B detect no per-level loss at all,
// the shelf never fills and the third axis — speed of recovery — degenerates
// across the WHOLE base grid, sweeps of R and churn included. C = ∞ stays in the
// grid as the negative control it is.
//
// ⚠️ Every value here is a PROPOSAL of §5 (П-1…П-7) and none is adopted. The
// configuration is printed with every number it produced.
func m6GridBase(sh shape, seed uint64) m6ModelConfig {
	plan := m6ContractPhases()
	return m6ModelConfig{
		Shape:  sh,
		Seed:   seed,
		Policy: policyCandidateC1,
		Quota:  m6GridQuota,

		Branch:   branchAPrime,
		Capacity: 4,
		// NearFrom and NearFromRule are filled in from the built graph — П-5
		// derives the border from the MEASURED pool, not from a formula. The
		// control value d/2 is used only where a configuration asks for it.
		Repair:     4,
		Cadence:    64,
		StaleTicks: 256,
		ShelfFirst: true,

		ExchangeRecords: 4,
		ExchangeEvery:   64,

		AddressedRecords: 2,
		RatePair:         1,
		RateNode:         4,

		Churn:       churnShock,
		ChurnShare:  0.20,
		ReturnShare: 0.5,
		ReturnAfter: 32,
		JoinMaxWait: 64,

		Phases:         &plan,
		RecoveryWindow: m6GridRecoveryWindow,
	}
}

const (
	// m6GridQuota is the quota the graphs are built at — the same 1 the rest of
	// the stage-1 grid uses, so M6's graphs are the graphs M1 and M2 described.
	m6GridQuota = 1
	// m6GridRecoveryWindow is W_rec of decision 3.5(ii): the recovery axis is
	// read on [onset, onset + 1024) for every configuration with churn,
	// whatever its phases decided. A parameter of the experiment, not a
	// derivation from T_cad.
	m6GridRecoveryWindow = 1024
)

// --- populations ----------------------------------------------------------------

// m6Population is one of the two views of a network the grid measures on the
// SAME graph: everybody, and the structural half.
type m6Population int

const (
	m6WholeNetwork m6Population = iota
	m6StructuralHalf
)

func (p m6Population) String() string {
	if p == m6WholeNetwork {
		return "whole network"
	}
	return "Q half"
}

func (p m6Population) member() func(nodeID) bool {
	if p == m6WholeNetwork {
		return everybody
	}
	return func(id nodeID) bool { return roleOf(id) == roleStructural }
}

// --- the variants ----------------------------------------------------------------

// m6VariantKind separates what the registry counts apart: the screened grid, the
// controls beyond it, and the δ pairs.
//
// ⚠️ It exists so the arithmetic can be checked rather than asserted. A control
// folded into the grid would make 400 read as 420 and the grid's own axes
// unreadable; a grid axis counted again as a control is the error the registry
// §5.1 was written to correct.
type m6VariantKind int

const (
	m6Grid m6VariantKind = iota
	m6ControlBeyondGrid
	m6DeltaPair
)

func (k m6VariantKind) String() string {
	switch k {
	case m6Grid:
		return "grid"
	case m6ControlBeyondGrid:
		return "control"
	default:
		return "delta"
	}
}

// runsEach is how many SCENARIO RUNS one configuration of this kind costs. A δ
// pair is two halves stepped in lockstep and cannot be run one half at a time —
// the comparison is what it is for — so it is one configuration and two runs.
func (k m6VariantKind) runsEach() int {
	if k == m6DeltaPair {
		return 2
	}
	return 1
}

// m6Variant is one point of the enumeration: what it changes from the base, and
// what the run key has to say about it.
//
// ⚠️ Params is written by hand rather than diffed out of the configuration. A
// diff would list the fields that happen to differ, which on the day a base
// value changes is a different list — and the identifier of every run would move
// with it, orphaning a whole journal.
type m6Variant struct {
	Kind   m6VariantKind
	Name   string
	Apply  func(*m6ModelConfig)
	Params []runParam
	// PairsAsBase marks the grid's BASE configuration, whose phase boundaries
	// the α control replays; ReplaysTheBase marks the control that does.
	//
	// ⚠️ Two flags rather than a comparison with the name, and rather than an
	// index into the slice. The name is part of runKey.Label, that is, part of
	// the file name of every run: a comparison on it makes renaming a variant
	// silently unpair the control AND orphan the journal at once. An index is
	// worse still — inserting a variant before it re-pairs the control with
	// something else and no test goes red.
	PairsAsBase    bool
	ReplaysTheBase bool
}

func param(name, value string) runParam { return runParam{Name: name, Value: value} }

// m6GridVariants is §5.9.5's screened grid: the base and one axis at a time.
//
// ⚠️ Twenty, and the arithmetic is in the reference test. Screening does NOT see
// interactions between axes — a deliberate price (§5.9.5) — and the one pair the
// owner might want whole, `R × churn form`, is +600 runs and HAS NOT BEEN NAMED;
// it is not in this list and is not added silently.
func m6GridVariants() []m6Variant {
	variants := []m6Variant{
		// The four branches in the base configuration. A′ IS the base.
		{Kind: m6Grid, Name: "branch/A", Params: []runParam{param("branch", "A")},
			Apply: func(c *m6ModelConfig) { c.Branch = branchA }},
		{Kind: m6Grid, Name: "branch/B", Params: []runParam{param("branch", "B")},
			Apply: func(c *m6ModelConfig) { c.Branch = branchB }},
		{Kind: m6Grid, Name: "branch/A-prime (base)", Params: []runParam{param("branch", "A′")},
			Apply: func(*m6ModelConfig) {}, PairsAsBase: true},
		{Kind: m6Grid, Name: "branch/C", Params: []runParam{param("branch", "C")},
			Apply: func(c *m6ModelConfig) { c.Branch = branchC }},

		// Capacity.
		{Kind: m6Grid, Name: "k/1", Params: []runParam{param("capacity_k", "1")},
			Apply: func(c *m6ModelConfig) { c.Capacity = 1 }},

		// Repair ceiling. ⚠️ "R without a ceiling" is the control §5.4 names,
		// and it is HERE, inside the 400 — not added again beside them.
		{Kind: m6Grid, Name: "R/1", Params: []runParam{param("repair_R", "1")},
			Apply: func(c *m6ModelConfig) { c.Repair = 1 }},
		{Kind: m6Grid, Name: "R/no-ceiling (control inside the grid)",
			Params: []runParam{param("repair_R", "no ceiling")},
			Apply:  func(c *m6ModelConfig) { c.Repair = 0 }},

		// Cadence. ⚠️ C = ∞ is the NEGATIVE control, and it too is inside the
		// 400.
		{Kind: m6Grid, Name: "C/256", Params: []runParam{param("cadence_C", "256")},
			Apply: func(c *m6ModelConfig) { c.Cadence = 256 }},
		{Kind: m6Grid, Name: "C/infinite (negative control inside the grid)",
			Params: []runParam{param("cadence_C", "∞")},
			Apply:  func(c *m6ModelConfig) { c.Cadence = 0 }},
	}

	// Churn: two further shock shares, the compensated load and the shrinking
	// network, each at both return shares. ⚠️ Eight, and the shrinking one is
	// named for what it is: it measures degradation, not a steady state.
	for _, form := range []struct {
		name  string
		churn m6ChurnForm
		share float64
	}{
		{"shock-f0.05", churnShock, 0.05},
		{"shock-f0.50", churnShock, 0.50},
		{"compensated", churnCompensated, 0.20},
		{"shrink", churnShrink, 0.20},
	} {
		for _, ret := range []float64{0, 0.5} {
			variants = append(variants, m6Variant{
				Kind: m6Grid,
				Name: fmt.Sprintf("churn/%s-ret%.1f", form.name, ret),
				Params: []runParam{
					param("churn_form", form.churn.String()),
					param("churn_share_f", fmt.Sprintf("%.2f", form.share)),
					param("return_share", fmt.Sprintf("%.1f", ret)),
				},
				Apply: func(c *m6ModelConfig) {
					c.Churn, c.ChurnShare, c.ReturnShare = form.churn, form.share, ret
				},
			})
		}
	}

	// The A′ exchange: m, and the single-exchange control — also INSIDE the 400.
	variants = append(variants,
		m6Variant{Kind: m6Grid, Name: "m/2", Params: []runParam{param("exchange_records_m", "2")},
			Apply: func(c *m6ModelConfig) { c.ExchangeRecords = 2 }},
		m6Variant{Kind: m6Grid, Name: "m/8", Params: []runParam{param("exchange_records_m", "8")},
			Apply: func(c *m6ModelConfig) { c.ExchangeRecords = 8 }},
		m6Variant{Kind: m6Grid, Name: "exchange/once (control inside the grid)",
			Params: []runParam{param("exchange_once", "true")},
			Apply:  func(c *m6ModelConfig) { c.ExchangeOnce = true }})

	return variants
}

// m6ControlVariants are the four the registry §5.2 counts BEYOND the grid, and
// each is beyond it for a reason printed with it.
func m6ControlVariants() []m6Variant {
	return []m6Variant{
		{
			// Not a branch: the source is replaced wholesale (§4.4). It says
			// what the same k, the same B, the same ceiling R and the same graph
			// would give a node that simply knew everybody — and it is NOT a
			// mathematical upper bound.
			Kind: m6ControlBeyondGrid, Name: "control/omniscient-source",
			Params: []runParam{param("source", "omniscient over the whole membership")},
			Apply:  func(c *m6ModelConfig) { c.OmniscientControl = true },
		},
		{
			// α of decision 3.4, under its agreed name. It REPLAYS the phase
			// boundaries of its paired base run — the pairing is compulsory, and
			// the constructor refuses it otherwise.
			Kind: m6ControlBeyondGrid, Name: "control/alpha-recovery-after-cleared-state",
			Params: []runParam{param("start", "state cleared at the churn tick"),
				param("phases", "replayed from the paired base run")},
			Apply:          func(c *m6ModelConfig) { c.StartEmpty = true },
			ReplaysTheBase: true,
		},
		{
			// Decision 3.2(c): a third algorithmic variant, not a cheaper
			// reading of the base. Conclusions do NOT carry to branch C.
			Kind: m6ControlBeyondGrid, Name: "control/local-repeat-filter",
			Params: []runParam{param("local_repeat_filter", "true")},
			Apply:  func(c *m6ModelConfig) { c.LocalRepeatFilter = true },
		},
		{
			// At branch C the border decides behaviour: an addressed request is
			// allowed only for levels ≥ NearFrom. So d/2 is a control there and
			// a recomputation everywhere else (registry §5.2).
			Kind: m6ControlBeyondGrid, Name: "control/near-from-d-half-branch-C",
			Params: []runParam{param("branch", "C"), param("near_from", "d/2 — CONTROL value")},
			Apply: func(c *m6ModelConfig) {
				c.Branch = branchC
				c.NearFrom = c.Shape.degree / 2
				c.NearFromRule = "CONTROL d/2 — not derived; the control of registry §5.2"
			},
		},
	}
}

// m6DeltaVariant is the pair of decision 3.4, form П-6: one candidate stream,
// recorded, and two halves replaying it — one keeping its memory, one cleared.
func m6DeltaVariant() m6Variant {
	return m6Variant{
		Kind: m6DeltaPair, Name: "delta/paired-halves-on-a-recorded-stream",
		Params: []runParam{param("stream", "recorded directly from the paired base A′ run"),
			param("halves", "memory kept | memory cleared")},
		Apply: func(*m6ModelConfig) {},
	}
}

func m6AllVariants() []m6Variant {
	variants := m6GridVariants()
	variants = append(variants, m6ControlVariants()...)
	return append(variants, m6DeltaVariant())
}

// --- the enumeration --------------------------------------------------------------

// m6SweepConfig is one configuration: a variant, a population, a shape, a seed.
type m6SweepConfig struct {
	Variant    m6Variant
	Population m6Population
	Shape      shape
	Seed       uint64
}

// config builds the model configuration, with the near-level border derived from
// the built graph unless the variant fixed it.
//
// ⚠️ The border is derived from the MEASURED pool of branch A over the FULL
// graph (§5.1.1) — the SAME reading for every branch and both populations, so
// the tables are compared at one set of levels. S(u) differs between the
// populations, so a border computed per population would compare them at
// different ones.
//
// ⚠️ It is derived per configuration rather than once per (shape, seed), which
// is a cost and not a difference: referencePool reads a freshly constructed
// network and runs no ticks, and the reading depends only on the graph. Caching
// it would be an optimisation; asserting that the value is the same for every
// configuration is the property, and a fixture does assert it.
func (c m6SweepConfig) config(g *graph) (m6ModelConfig, error) {
	model := m6GridBase(c.Shape, c.Seed)
	model.Membership = c.Population.String()
	c.Variant.Apply(&model)

	if model.NearFromRule == "" {
		border, rule, err := referencePool(g, model, everybody)
		if err != nil {
			return m6ModelConfig{}, fmt.Errorf("deriving the near-level border: %w", err)
		}
		model.NearFrom, model.NearFromRule = border, rule
	}
	return model, nil
}

func (c m6SweepConfig) Key(sources string) runKey {
	params := []runParam{
		{Name: "kind", Value: c.Variant.Kind.String()},
		{Name: "variant", Value: c.Variant.Name},
		{Name: "shape", Value: c.Shape.name},
		{Name: "nodes", Value: fmt.Sprint(c.Shape.nodes)},
		{Name: "degree_d", Value: fmt.Sprint(c.Shape.degree)},
		{Name: "budget_B", Value: fmt.Sprint(c.Shape.budget)},
		{Name: "seed", Value: fmt.Sprint(c.Seed)},
		{Name: "population", Value: c.Population.String()},
		{Name: "policy", Value: policyCandidateC1.String()},
		{Name: "quota", Value: fmt.Sprint(m6GridQuota)},
		{Name: "phases", Value: fmt.Sprintf("T_fill=%d T_idle=%d T_rec=%d T_cad=%d",
			m6ContractPhases().FillTicks, m6ContractPhases().IdleTicks,
			m6ContractPhases().RecoveryTicks, m6ContractPhases().CadenceTicks)},
		{Name: "recovery_window_W_rec", Value: fmt.Sprint(m6GridRecoveryWindow)},
	}
	// The base as the variant found it, so a change to a base value moves every
	// identifier — which is correct: it would be a different experiment.
	base := m6GridBase(c.Shape, c.Seed)
	params = append(params,
		runParam{Name: "base_branch", Value: base.Branch.String()},
		runParam{Name: "base_capacity_k", Value: fmt.Sprint(base.Capacity)},
		runParam{Name: "base_repair_R", Value: fmt.Sprint(base.Repair)},
		runParam{Name: "base_cadence_C", Value: fmt.Sprint(base.Cadence)},
		runParam{Name: "base_churn", Value: fmt.Sprintf("%s f=%.2f ret=%.1f",
			base.Churn, base.ChurnShare, base.ReturnShare)},
		runParam{Name: "base_exchange_m", Value: fmt.Sprint(base.ExchangeRecords)},
		runParam{Name: "base_exchange_every", Value: fmt.Sprint(base.ExchangeEvery)},
		runParam{Name: "base_stale_ticks", Value: fmt.Sprint(base.StaleTicks)},
		runParam{Name: "base_return_after", Value: fmt.Sprint(base.ReturnAfter)},
		runParam{Name: "base_join_max_wait", Value: fmt.Sprint(base.JoinMaxWait)},
		runParam{Name: "base_addressed_n", Value: fmt.Sprint(base.AddressedRecords)},
		runParam{Name: "base_rate_pair", Value: fmt.Sprint(base.RatePair)},
		runParam{Name: "base_rate_node", Value: fmt.Sprint(base.RateNode)},
	)
	params = append(params, c.Variant.Params...)

	return runKey{
		Measurement: "M6",
		Label: fmt.Sprintf("%s/%s/seed%d/%s", c.Shape.name, c.Variant.Name, c.Seed,
			c.Population),
		Params:  params,
		Sources: sources,
	}
}

// m6SweepEnumeration lists all 500 configurations (520 scenario runs), in the
// order a range cuts them.
//
// ⚠️ The order is shape → seed → population → variant. Everything under one
// (shape, seed) shares ONE built graph — the registry counts ten graph builds
// for the whole of M6 — so a contiguous range builds the fewest graphs, and a
// range that stops inside a seed still leaves a readable partial table.
func m6SweepEnumeration(shapes []shape, seeds []uint64) []m6SweepConfig {
	variants := m6AllVariants()
	configs := make([]m6SweepConfig, 0, len(shapes)*len(seeds)*2*len(variants))
	for _, sh := range shapes {
		for _, seed := range seeds {
			for _, population := range []m6Population{m6WholeNetwork, m6StructuralHalf} {
				for _, variant := range variants {
					configs = append(configs, m6SweepConfig{
						Variant: variant, Population: population, Shape: sh, Seed: seed,
					})
				}
			}
		}
	}
	return configs
}

// m6ScenarioRuns counts what the registry counts: scenario runs, not
// configurations. A δ pair is one configuration and two runs.
func m6ScenarioRuns(configs []m6SweepConfig) int {
	total := 0
	for _, config := range configs {
		total += config.Variant.Kind.runsEach()
	}
	return total
}

// m6RunsByKind is the arithmetic the registry prints, per kind, so 400 + 80 + 40
// can be read off the enumeration instead of trusted.
func m6RunsByKind(configs []m6SweepConfig) map[m6VariantKind]int {
	byKind := map[m6VariantKind]int{}
	for _, config := range configs {
		byKind[config.Variant.Kind] += config.Variant.Kind.runsEach()
	}
	return byKind
}

// --- running one configuration ------------------------------------------------------

// m6DeltaGateOnBigShapes is the open point of registry §5.2.2, enforced instead
// of remembered.
//
// ⚠️ The comparator steps both halves and compares their OFFER SEQUENCES, so
// both replaying halves need TraceOffers — an estimated ≈3 GB each on 10k×8. The
// direct recording (decision 3.4) took the trace off the RECORDING run only.
// Until there is either a comparison without a full trace or a memory gate, δ on
// 10k×8 is not run — and it is NAMED here rather than dropped, because a sweep
// that silently ran 500 of 520 would report a complete grid.
const m6DeltaGateOnBigShapes = 2_000

// m6Outcomes is what one configuration produced: one report for an ordinary run,
// two plus a comparison for a δ pair.
type m6Outcomes struct {
	Reports    []*m6ModelReport
	Comparison *m6Comparison
}

// runM6Configuration runs one configuration on a prepared graph.
//
// `boundaries` supplies the phase spans a control must replay; it is nil for
// everything that decides its own.
func runM6Configuration(
	g *graph, config m6SweepConfig, boundaries []m6PhaseBoundary,
) (m6Outcomes, error) {
	model, err := config.config(g)
	if err != nil {
		return m6Outcomes{}, err
	}
	member := config.Population.member()

	switch config.Variant.Kind {
	case m6DeltaPair:
		return runM6DeltaPair(g, model, member)
	case m6ControlBeyondGrid, m6Grid:
		if boundaries != nil {
			model.ReplayPhases = boundaries
		}
		network, prepareErr := newM6Network(g, model, member)
		if prepareErr != nil {
			return m6Outcomes{}, prepareErr
		}
		report, runErr := network.Run()
		if runErr != nil {
			return m6Outcomes{}, runErr
		}
		return m6Outcomes{Reports: []*m6ModelReport{report}}, nil
	}
	return m6Outcomes{}, fmt.Errorf("configuration kind %s is not one this driver runs",
		config.Variant.Kind)
}

// runM6DeltaPair is form П-6 (decision 3.4): record the candidate stream of the
// base A′ run DIRECTLY, then replay it in two halves — memory kept and memory
// cleared — stepped in lockstep and compared.
//
// ⚠️ THE RECORDING RUN IS RE-MADE HERE, and that is a cost, not a count. The
// registry puts the recording on the base A′ grid run ("0 new runs"), which is
// true of the GRID; a stream is a megabytes-large object the journal does not
// carry, so a δ configuration asked for on its own has to produce it again. The
// two runs this configuration books are the PAIR, exactly as §5.2.2 counts them.
//
// ⚠️ Both halves get TraceOffers: the comparison is over the offer sequences and
// compareM6Runs refuses without it. That is the memory bound the gate above is
// about.
func runM6DeltaPair(
	g *graph, model m6ModelConfig, member func(nodeID) bool,
) (m6Outcomes, error) {
	recording := model
	recording.RecordStream = true
	recorder, err := newM6Network(g, recording, member)
	if err != nil {
		return m6Outcomes{}, fmt.Errorf("preparing the recording run: %w", err)
	}
	recorded, err := recorder.Run()
	if err != nil {
		return m6Outcomes{}, fmt.Errorf("the recording run: %w", err)
	}
	if recorded.Recording == nil {
		return m6Outcomes{}, fmt.Errorf("the recording run produced no stream")
	}

	replay := model
	replay.Stream = recorded.Recording
	replay.TraceOffers = true
	replay.ReplayPhases = recorded.PhaseBoundaries()

	kept, err := newM6Network(g, replay, member)
	if err != nil {
		return m6Outcomes{}, fmt.Errorf("preparing the half that keeps its memory: %w", err)
	}
	clearedConfig := replay
	clearedConfig.StartEmpty = true
	cleared, err := newM6Network(g, clearedConfig, member)
	if err != nil {
		return m6Outcomes{}, fmt.Errorf("preparing the half whose memory is cleared: %w", err)
	}

	comparison, err := compareM6Runs(kept, cleared)
	if err != nil {
		return m6Outcomes{}, fmt.Errorf("comparing the halves: %w", err)
	}
	return m6Outcomes{
		Reports:    []*m6ModelReport{kept.report, cleared.report},
		Comparison: comparison,
	}, nil
}

// --- what a run stores ----------------------------------------------------------------

// m6RunBody renders one report as facts, one per line.
//
// ⚠️ Structured, not the report's own prose. The prose is what a reviewer reads
// and it is in the log; the file has to be joinable to other files, and a
// paragraph is not.
func m6RunBody(prefix string, report *m6ModelReport) []string {
	lines := []string{
		fmt.Sprintf("%sphases %s", prefix, report.PhaseLine()),
		fmt.Sprintf("%snear_from level=%d rule=%s", prefix, report.Config.NearFrom,
			report.Config.NearFromRule),
		fmt.Sprintf("%smembers=%d online_end=%d from_population=%d from_reserve=%d reserve=%d",
			prefix, report.Members, onlineAtEnd(report), report.OnlineFromPopulation,
			report.OnlineFromReserve, report.ReserveSize),
		fmt.Sprintf("%scoverage_at_churn claimed=%d actual=%d", prefix,
			report.ClaimedBefore, report.ActualBefore),
	}

	for level, coverage := range report.Levels {
		lines = append(lines, fmt.Sprintf(
			"%scoverage_level_%d claimed=%d actual=%d slots=%d retained_offline=%d population=%d population_joined=%d",
			prefix, level, coverage.Claimed, coverage.Actual, coverage.Slots,
			coverage.RetainedOffline, coverage.Population, coverage.PopulationJoined))
	}

	lines = append(lines,
		fmt.Sprintf("%sprobes measured=%d filled=%d after_churn=%d physical=%d", prefix,
			report.Probes.Probes(), report.Probes.Filled(), report.ProbesAfterChurn.Probes(),
			report.PhysicalProbes.Probes()),
		fmt.Sprintf("%sphysical lost=%d detections=%d strangers_unreachable=%d", prefix,
			report.PhysicalLost, report.PhysicalDetections, report.PhysicalStrangersUnreachable),
		fmt.Sprintf("%sdetection count=%d %s", prefix, len(report.DetectionDelays),
			quantiles(report.DetectionDelays)),
		fmt.Sprintf("%srecovery lost=%d refilled=%d filled_elsewhere=%d shelf_hits=%d shelf_expired=%d",
			prefix, sumOf(report.LostByLevel), sumOf(report.RefilledByLevel),
			report.FilledElsewhere, report.ShelfHits, report.ShelfExpired),
		fmt.Sprintf("%schurn departed=%d decided=%d decided_reserve=%d offered=%d admitted=%d "+
			"gave_up=%d pending=%d not_offered=%d", prefix,
			report.Departed, report.DeparturesDecided, report.DeparturesDecidedInReserve,
			report.ArrivalsOffered, report.ArrivalsAdmitted, report.GaveUpJoining,
			report.PendingAtEnd, report.ArrivalsNotOffered),
		fmt.Sprintf("%sarrivals returns_offered=%d newcomers_offered=%d returns_admitted=%d "+
			"newcomers_admitted=%d returned_with_table=%d returned_empty=%d surplus=%d", prefix,
			report.ReturnsOffered, report.NewcomersOffered, report.ReturnsAdmitted,
			report.NewcomersAdmitted, report.ReturnedWithATable, report.ReturnedEmptyHanded,
			report.OfferedSurplus),
		fmt.Sprintf("%sexchange done=%d refused=%d repeats_filtered=%d", prefix,
			report.ExchangesDone, report.ExchangesRefused, report.RepeatsFiltered),
		fmt.Sprintf("%saddressed answers=%d rate_limited=%d refused=%d", prefix,
			report.AddressedAnswers, report.AddressedRateLimited, report.AddressedRefused),
		fmt.Sprintf("%spool_at_start %s", prefix, medianToken(report.PoolAtStart)),
		fmt.Sprintf("%spool_measured %s", prefix, medianToken(report.PoolByOwner)),
	)

	if window := report.Window; window != nil {
		lines = append(lines, fmt.Sprintf(
			"%swindow from=%d ticks=%d closed=%t lost_events=%d refilled_events=%d "+
				"unique_lost=%d unique_refilled=%d detections=%d probes=%d refreshes=%d "+
				"residual_online=%d residual_offline=%d undetected_dead=%d recovered_at_tau=%d "+
				"known_deficit_cleared_at_tau=%d",
			prefix, window.From, window.Ticks, window.Closed, window.LostEvents,
			window.RefilledEvents, window.UniqueLostRecords, window.UniqueRefilledRecords,
			window.DetectionEvents, window.Probes.Probes(), window.Refreshes,
			window.ResidualDeficit, window.ResidualDeficitOffline, window.UndetectedDeadRecords,
			window.RecoveredAtTau, window.KnownDeficitClearedAtTau))
		// ⚠️ THE MECHANISM'S FRAMES INSIDE THE WINDOW, on a line of their own
		// (P2 of 2026-09-20). The contract's "кадры механизма в окне" row
		// requires them shown APART FROM THE PROBES — a frame and a probe are
		// different units and the contract names no model converting one into
		// the other — and over the SAME 1024 ticks. The whole-run counters a
		// few lines above cannot stand in for them: runs differ in length, so
		// their totals are not comparable across configurations. Dropping these
		// six lost exactly the comparison the owner's P2 of round 29 added.
		lines = append(lines, fmt.Sprintf(
			"%swindow_frames exchanges_served=%d exchanges_refused=%d addressed_answers=%d "+
				"addressed_rate_limited=%d addressed_refused=%d repeats_filtered=%d",
			prefix, window.ExchangesServed, window.ExchangesRefused, window.AddressedAnswers,
			window.AddressedRateLimited, window.AddressedRefused, window.RepeatsFiltered))
	} else {
		lines = append(lines, prefix+"window none — the recovery axis was read off the phases alone")
	}

	if report.Recording != nil {
		lines = append(lines, fmt.Sprintf("%sstream entries=%d fingerprint=%s", prefix,
			report.Recording.Entries(), report.Recording.fingerprint()))
	}
	if report.StreamParticipants > 0 {
		lines = append(lines, fmt.Sprintf("%sreplay exhausted_owners=%d participants=%d consumed=%d",
			prefix, report.StreamExhaustedOwners, report.StreamParticipants, report.StreamConsumed))
	}
	return lines
}

// onlineAtEnd is the population the run finished with.
func onlineAtEnd(report *m6ModelReport) int {
	if len(report.OnlineByTick) == 0 {
		return 0
	}
	return report.OnlineByTick[len(report.OnlineByTick)-1]
}

// quantiles renders a measured distribution, or says there is none. ⚠️ "no data"
// and "0" are different facts and are spelled differently.
func quantiles(values []int) string {
	if len(values) == 0 {
		return "min=no_data median=no_data max=no_data"
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	median, _ := medianOf(sorted)
	return fmt.Sprintf("min=%d median=%d max=%d", sorted[0], median, sorted[len(sorted)-1])
}

// medianToken renders a per-owner distribution the same way.
func medianToken(values []int) string {
	if len(values) == 0 {
		return "owners=0 " + quantiles(nil)
	}
	return fmt.Sprintf("owners=%d %s", len(values), quantiles(values))
}

// m6OutcomeBody is the whole configuration's record: one report, or the two
// halves of a δ pair plus every claim the comparator judged.
func m6OutcomeBody(config m6SweepConfig, outcomes m6Outcomes) []string {
	if config.Variant.Kind != m6DeltaPair {
		return m6RunBody("", outcomes.Reports[0])
	}
	lines := m6RunBody("kept.", outcomes.Reports[0])
	lines = append(lines, m6RunBody("cleared.", outcomes.Reports[1])...)
	lines = append(lines, "pair scenario_runs=2 — the halves are stepped in lockstep and compared; "+
		"neither is a measurement on its own")
	for _, claim := range outcomes.Comparison.Claims {
		lines = append(lines, fmt.Sprintf("claim %s = %s%s", claim.Kind, claim.Status,
			detailSuffix(claim)))
	}
	return lines
}

func detailSuffix(claim m6Claim) string {
	if claim.Detail == "" {
		return ""
	}
	return " — " + claim.Detail
}

// --- the run ------------------------------------------------------------------------

// TestM6BucketFillingAndRecoveryOnTheCandidate is the sweep.
func TestM6BucketFillingAndRecoveryOnTheCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("the M6 grid is a measurement, not a unit test")
	}

	selection := runSelectionFrom(t, "M6")
	shapes := shapesNamed(t, "M6_SHAPES", []shape{sweepShapes[0], sweepShapes[1]})
	configs := m6SweepEnumeration(shapes, sweepSeeds)

	keys := make([]runKey, 0, len(configs))
	for _, config := range configs {
		keys = append(keys, config.Key(selection.Sources))
	}
	if err := requireDistinctKeys(keys); err != nil {
		t.Fatalf("the M6 enumeration is not an enumeration: %v", err)
	}
	if selection.Listing {
		t.Log(enumerationListing("M6 — bucket filling and recovery", keys))
		return
	}
	if !selection.requireACutOrAnExplicitAll(t, len(configs),
		"tens of seconds per run on 1k×8 and an estimated 6–7 minutes on 10k×8") {
		return
	}

	byKind := m6RunsByKind(configs)
	var out strings.Builder
	fmt.Fprintf(&out, "\nM6 — bucket filling, cost and recovery\n")
	fmt.Fprintf(&out, "contract: docs/refactoring/dht/21-m6-bucket-discovery-measurement.md §5.9.5, "+
		"§6; registry §5; decisions of 2026-09-18\n\n")
	fmt.Fprintf(&out, "inputs\n")
	fmt.Fprintf(&out, "  shapes %v, seeds %v, populations: whole network and Q half, on ONE graph "+
		"per (shape, seed)\n", shapeNames(shapes), sweepSeeds)
	fmt.Fprintf(&out, "  rule %s, quota %d; the base is %s\n", policyCandidateC1, m6GridQuota,
		m6GridBase(shapes[0], 1).scheduleLine())
	fmt.Fprintf(&out, "  configurations %d = grid %d + controls %d + δ %d; SCENARIO RUNS %d = "+
		"%d + %d + %d\n",
		len(configs), countOfKind(configs, m6Grid), countOfKind(configs, m6ControlBeyondGrid),
		countOfKind(configs, m6DeltaPair), m6ScenarioRuns(configs),
		byKind[m6Grid], byKind[m6ControlBeyondGrid], byKind[m6DeltaPair])
	fmt.Fprintf(&out, "  ⚠️ `R` without a ceiling, `C = ∞` and the single exchange are INSIDE the "+
		"grid and are not added again as controls (registry §5.1).\n")
	fmt.Fprintf(&out, "  ⚠️ The pair `R × churn form` (+600 runs) has NOT been named by the owner "+
		"and is not in this enumeration.\n")
	fmt.Fprintf(&out, "%s\n", selection.Header(len(configs)))

	var cachedGraph *graph
	cachedFor := ""
	// The phase boundaries of the base A′ run of the current (shape, seed,
	// population), which the α control has to replay. Compulsory pairing: the
	// control has nothing to recover, so its own stop rules would land F4 on
	// other ticks, and every churn event is keyed on the tick.
	baseBoundaries := map[string][]m6PhaseBoundary{}

	runSweep(t, selection, keys, &out, func(index int, _ runKey) ([]string, string, error) {
		config := configs[index]
		if gate := m6GateFor(config); gate != "" {
			// ⚠️ NAMED, not dropped. A gate is recorded like a failure so the
			// ledger cannot report a complete grid, and the reason travels with
			// the record.
			return nil, "", fmt.Errorf("%s", gate)
		}

		if want := graphCacheKey(config.Shape, config.Seed, policyCandidateC1); cachedFor != want {
			cachedGraph = buildGraph(config.Shape, config.Seed, m6GridQuota, policyCandidateC1)
			cachedFor = want
			clear(baseBoundaries)
		}

		boundaries, extra, err := m6BoundariesFor(cachedGraph, config, baseBoundaries)
		if err != nil {
			return nil, "", err
		}

		outcomes, err := runM6Configuration(cachedGraph, config, boundaries)
		if err != nil {
			return nil, "", err
		}
		// The base A′ run of this (shape, seed, population) is what α replays.
		if config.Variant.PairsAsBase {
			baseBoundaries[pairingKey(config)] = outcomes.Reports[0].PhaseBoundaries()
		}
		return m6OutcomeBody(config, outcomes), m6Headline(config, outcomes) + extra, nil
	})

	fmt.Fprintf(&out, "  scenario runs expected %d = grid %d + controls %d + δ %d "+
		"(the whole enumeration, on both shapes and five seeds, is 520 = 400 + 80 + 40); "+
		"M2, M3-a, M4 and M5 are NOT in this number\n",
		m6ScenarioRuns(configs), byKind[m6Grid], byKind[m6ControlBeyondGrid], byKind[m6DeltaPair])
	fmt.Fprintf(&out, "\n  ⚠️ The three axes — coverage, cost, recovery — are stored and read "+
		"APART. None of them carries a threshold: acceptability is the owner's.\n")
	fmt.Fprintf(&out, "  ⚠️ The omniscient source is a CONTROL RESULT under the stated "+
		"constraints, not a mathematical upper bound over all possible mechanisms.\n")

	t.Log(out.String())
}

// m6GateFor names a configuration this environment will not run, and why. ⚠️ An
// empty string means "run it"; anything else is recorded as a refusal with its
// reason, never as a silent skip.
func m6GateFor(config m6SweepConfig) string {
	if config.Variant.Kind == m6DeltaPair && config.Shape.nodes > m6DeltaGateOnBigShapes {
		return fmt.Sprintf("gate: δ on %s needs the offer trace of BOTH replaying halves "+
			"(compareM6Runs compares offer sequences), estimated ≈3 GB each — registry §5.2.2 "+
			"names this open and unresolved: either a comparison without a full trace or a memory "+
			"gate is needed first. Not run, and not counted as complete", config.Shape.name)
	}
	return ""
}

// pairingKey names the (shape, seed, population) a control is paired with.
func pairingKey(config m6SweepConfig) string {
	return fmt.Sprintf("%s/%d/%s", config.Shape.name, config.Seed, config.Population)
}

// m6BoundariesFor supplies the phase spans a configuration must replay, and says
// in its second return value what supplying them COST.
//
// ⚠️ Only the α control needs them, and for it they are COMPULSORY (decision
// 3.3(в)): the whole grid plays its own boundaries, because churn draws are keyed
// on τ = tick − onset, and only the control that clears state has to be pinned to
// its pair.
//
// ⚠️ If the paired base run is not in this call, it is RUN HERE, and the log says
// so — an extra scenario run the registry's 520 does not count. It happens
// whenever a range does not contain the base, and on every resume where the base
// is already recorded (the journal keeps numbers, not phase boundaries). The
// previous version promised this note and did not print it, so the α control
// silently cost two runs instead of one.
func m6BoundariesFor(
	g *graph, config m6SweepConfig, known map[string][]m6PhaseBoundary,
) ([]m6PhaseBoundary, string, error) {
	if !config.Variant.ReplaysTheBase {
		return nil, "", nil
	}
	if boundaries, ok := known[pairingKey(config)]; ok {
		return boundaries, "", nil
	}

	base := config
	base.Variant = m6BaseVariant()
	model, err := base.config(g)
	if err != nil {
		return nil, "", err
	}
	network, err := newM6Network(g, model, config.Population.member())
	if err != nil {
		return nil, "", fmt.Errorf("preparing the paired base run: %w", err)
	}
	report, err := network.Run()
	if err != nil {
		return nil, "", fmt.Errorf("the paired base run: %w", err)
	}
	boundaries := report.PhaseBoundaries()
	known[pairingKey(config)] = boundaries
	return boundaries, " · ⚠️ +1 UNCOUNTED scenario run: the paired base was not in this call, " +
		"so its boundaries were produced by running it here", nil
}

// m6BaseVariant is the grid's base, found by its marker rather than by position.
func m6BaseVariant() m6Variant {
	for _, variant := range m6GridVariants() {
		if variant.PairsAsBase {
			return variant
		}
	}
	// Unreachable while the grid has a base; a fixture asserts it has exactly one.
	return m6Variant{}
}

// m6Headline is the one line the log shows per run: the three axes, apart.
func m6Headline(config m6SweepConfig, outcomes m6Outcomes) string {
	report := outcomes.Reports[0]
	axes := fmt.Sprintf("coverage %d/%d claimed/actual at churn · cost %d probes · ",
		report.ClaimedBefore, report.ActualBefore, report.Probes.Probes())
	axes += m6RecoveryPhrase(report.Window)
	if config.Variant.Kind == m6DeltaPair && outcomes.Comparison != nil {
		axes += fmt.Sprintf(" · δ: %s", m6ClaimSummary(outcomes.Comparison))
	}
	return axes
}

// m6RecoveryPhrase renders the third axis, and it separates THREE outcomes that
// a single "not reached" would fold into one.
//
// ⚠️ The separation is not cosmetic. The recovery criterion is only evaluated
// after at least one loss has been DETECTED inside the window; with none
// detected there was nothing to recover from, and calling that "recovery not
// reached within W_rec" states a failure where the model states an absence. The
// α control is exactly this case — its window closes with nothing lost, nothing
// outstanding and nothing dead — and the first version of this line reported it
// as a failure of recovery, which the report then repeated.
func m6RecoveryPhrase(window *m6RecoveryWindow) string {
	switch {
	case window == nil:
		return "recovery: no window asked for"
	case window.RecoveredAtTau >= 0:
		return fmt.Sprintf("recovery at τ=%d", window.RecoveredAtTau)
	case window.LostEvents == 0:
		return fmt.Sprintf("recovery NOT APPLICABLE: no loss was detected inside W_rec=%d, so "+
			"there was nothing to recover from (residual online %d, offline %d, undetected dead %d)",
			window.Ticks, window.ResidualDeficit, window.ResidualDeficitOffline,
			window.UndetectedDeadRecords)
	default:
		return fmt.Sprintf("recovery NOT REACHED within W_rec=%d after %d detected losses "+
			"(residual online %d, offline %d, undetected dead %d)",
			window.Ticks, window.LostEvents, window.ResidualDeficit,
			window.ResidualDeficitOffline, window.UndetectedDeadRecords)
	}
}

// m6ClaimSummary counts the comparator's verdicts. ⚠️ A claim that does NOT hold
// is a finding of the pair, not a defect of the driver: П-6 asks what memory
// buys when the candidate stream is held fixed, and where the two halves part is
// the answer.
func m6ClaimSummary(comparison *m6Comparison) string {
	holds, broken, notApplicable := 0, 0, 0
	for _, claim := range comparison.Claims {
		switch claim.Status {
		case claimHolds:
			holds++
		case claimNotApplicable:
			notApplicable++
		default:
			broken++
		}
	}
	return fmt.Sprintf("%d claims hold, %d do not, %d not applicable", holds, broken, notApplicable)
}

func countOfKind(configs []m6SweepConfig, kind m6VariantKind) int {
	count := 0
	for _, config := range configs {
		if config.Variant.Kind == kind {
			count++
		}
	}
	return count
}

func shapeNames(shapes []shape) []string {
	names := make([]string, 0, len(shapes))
	for _, sh := range shapes {
		names = append(names, sh.name)
	}
	return names
}
