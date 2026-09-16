package overlaysim

// m4_test.go is the M4 measurer of docs/refactoring/dht/21-anonymity-transport.md
// §4.3.4″.6: how often the anonymous mode REFUSES because the rule of §4.3.4″.3
// left no first hop to use.
//
// The rule it models, word for word from §4.3.4″.3:
//
//  1. the target is excluded from the first-hop choice OF THIS REQUEST;
//  2. the guard set is NOT changed and NOT topped up — not for this request and
//     not after it;
//  3. another SUITABLE MEMBER OF THE EXISTING SET is taken;
//  4. if there is none — an explicit refusal of the anonymous mode;
//  5. stepping outside the set and any revealing fallback are forbidden.
//
// Which is why this file has no rotation, no top-up and no fallback path: their
// absence is the measured object. The set arrives as an input and leaves
// untouched, and a test compares it byte for byte afterwards.
//
// ⚠️ WHAT THIS MEASURES, AND WHAT IT DOES NOT. The refusal rate is a function of
// the guard set and of the workload, both of which are INPUTS here — the M1
// model builds neighbourhoods, not guard sets, and knows nothing about liveness,
// transit capability or proven identity. So a number produced here describes an
// ASSUMED set under an ASSUMED workload. It is not the refusal rate of the
// network, and it is not user load (§5.1 of the plan).
//
// ⚠️ OPEN, AND DELIBERATELY NOT DECIDED HERE — for the owner:
//
//   - CONFIRMED OR THE WHOLE SET. §4.3.4″.3 leaves open whether a still
//     unconfirmed member may be taken when the target occupies the confirmed
//     one. The section says the value "is measured, not assumed", so this
//     measurer reports BOTH populations side by side and never merges them; the
//     gap between the two IS the price of that choice.
//   - `k` IS COUNTED OVER THE WHOLE PINNED SET, not over the hot three. That is
//     what §4.3.4″.3 says ("a suitable member of the set is defined on the WHOLE
//     pinned set"), and today's code disagrees: `primaryLocked` cuts to
//     `guardPrimaryCount = 3` BEFORE any role or target filter. This measurer
//     follows the RULE. The refusal rate of today's code is a different
//     measurement and would need the truncation modelled on purpose.
//   - SUITABILITY IS AN INPUT, not a derivation. Only the ¬Q half of it comes
//     from the graph; alive, transit-capable and proven-identity are declared
//     per member, because the model has no notion of any of the three.
//
// ⚠️ Instrument only: no threshold of acceptability is proposed, and M4 is
// neither passed nor failed here. Acceptance numbers belong to point B of the
// queue, on an agreed candidate.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
)

// guardMember is one member of the PINNED first-hop guard set.
//
// The four flags are kept apart instead of collapsing into one "suitable"
// boolean so a fixture can say WHICH condition failed, and so a mutation that
// drops one of them has something to break.
type guardMember struct {
	// Node indexes the graph: the ¬Q condition is read from its role there.
	Node int32
	// Alive, TransitCapable and IdentityProven are the remaining conditions of
	// §4.3.4″.3. ⚠️ Inputs, not simulation: the model has none of the three.
	Alive          bool
	TransitCapable bool
	IdentityProven bool
	// Confirmed means a frame has actually gone through this member — what
	// `NoteUsed` sets in first_hop_guards.go, and NOT membership of the hot
	// list. An unconfirmed member is still a member.
	Confirmed bool
}

// guardSet is the pinned set. It is passed by value everywhere and never
// written to: rule 2 is the whole point.
type guardSet struct {
	Name    string
	Members []guardMember
}

// suitable is the §4.3.4″.3 filter for the first-hop role, BEFORE the target is
// excluded: alive ∧ ¬Q ∧ transit-capable ∧ proven identity.
func (m guardMember) suitable(g *graph) bool {
	if g.roles[m.Node] == roleStructural {
		return false // Q may carry the structural leg, never the first hop
	}
	return m.Alive && m.TransitCapable && m.IdentityProven
}

// m4Outcome separates the two refusals, because they are different findings:
// one says the set was already useless for this request, the other says the
// RULE turned a usable set into a refusal. Lumping them together would credit
// the exclusion with refusals it did not cause.
type m4Outcome int

const (
	// m4Served — a first hop was found inside the set.
	m4Served m4Outcome = iota
	// m4RefusedNobodySuitable — nothing in the set passed the filter, BEFORE
	// the target was excluded. The target is irrelevant to this refusal.
	m4RefusedNobodySuitable
	// m4RefusedByTargetExclusion — the set had suitable members, and after
	// removing the target none were left. This one belongs to the rule (S24а,
	// the degenerate k = 1 case).
	m4RefusedByTargetExclusion
)

func (o m4Outcome) String() string {
	switch o {
	case m4Served:
		return "served"
	case m4RefusedNobodySuitable:
		return "refused: nobody suitable"
	default:
		return "refused: only the target was suitable"
	}
}

// m4Request is one request: what happened and which member carried it.
type m4Request struct {
	Outcome m4Outcome
	// FirstHop is the chosen member, or -1 on refusal. A refusal that still
	// names a hop would be exactly the forbidden fallback.
	FirstHop int32
	// SuitableBefore is |k| for this request — suitable members BEFORE the
	// target was excluded. Reported so the refusal can be read against the set
	// it happened in.
	SuitableBefore int
}

// selectFirstHop applies §4.3.4″.3 to one request.
//
// ⚠️ It reads the set and returns a member of it or nothing. There is no branch
// that adds, removes, promotes, demotes or reorders anything, and no branch that
// looks outside `set.Members` — rules 2 and 5 are enforced by having nowhere
// else to go, not by a check that could be forgotten.
func selectFirstHop(g *graph, set guardSet, target int32, onlyConfirmed bool) m4Request {
	suitable := make([]int32, 0, len(set.Members))
	for _, member := range set.Members {
		if onlyConfirmed && !member.Confirmed {
			continue
		}
		if !member.suitable(g) {
			continue
		}
		suitable = append(suitable, member.Node)
	}

	if len(suitable) == 0 {
		return m4Request{Outcome: m4RefusedNobodySuitable, FirstHop: -1}
	}

	// Rule 1: the target of THIS request, and only of this request.
	remaining := make([]int32, 0, len(suitable))
	for _, node := range suitable {
		if node == target {
			continue
		}
		remaining = append(remaining, node)
	}
	if len(remaining) == 0 {
		return m4Request{
			Outcome:        m4RefusedByTargetExclusion,
			FirstHop:       -1,
			SuitableBefore: len(suitable),
		}
	}

	// Rule 3: another suitable member of the EXISTING set. The choice among
	// them is deterministic here — which one is taken does not change a refusal
	// rate, and a reproducible pick keeps the fixtures checkable by hand.
	return m4Request{
		Outcome:        m4Served,
		FirstHop:       remaining[0],
		SuitableBefore: len(suitable),
	}
}

// m4Slice is one population of members — confirmed only, or the whole sampled
// set — measured over one workload.
type m4Slice struct {
	// Requests is the DENOMINATOR: every request put to the rule, served and
	// refused alike. A refusal rate whose denominator counts only refusals is
	// the mistake this field is named against.
	Requests int
	Served   int
	// The two refusals, kept apart.
	RefusedNobodySuitable    int
	RefusedByTargetExclusion int
}

func (s m4Slice) Refused() int {
	return s.RefusedNobodySuitable + s.RefusedByTargetExclusion
}

// share renders numerator/denominator. ⚠️ An empty sample is NO DATA: 0 % would
// claim the rule never refuses when it was never asked.
func (s m4Slice) share(numerator int) string {
	if s.Requests == 0 {
		return "no data"
	}
	return fmt.Sprintf("%d/%d = %.1f%%", numerator, s.Requests,
		float64(numerator)/float64(s.Requests)*100)
}

// RefusalShare is M4 itself.
func (s m4Slice) RefusalShare() string { return s.share(s.Refused()) }

// ExclusionShare is the part of it that the rule caused.
func (s m4Slice) ExclusionShare() string { return s.share(s.RefusedByTargetExclusion) }

// NobodySuitableShare is the part that would have happened without any target.
func (s m4Slice) NobodySuitableShare() string { return s.share(s.RefusedNobodySuitable) }

func (s m4Slice) String() string {
	if s.Requests == 0 {
		return "no data"
	}
	return fmt.Sprintf("refused %s (by exclusion %s, nobody suitable %s)",
		s.RefusalShare(), s.ExclusionShare(), s.NobodySuitableShare())
}

// guardSnapshotMember is one member AS IT WAS when the measurement ran,
// including the role the graph gave it. The role belongs in the snapshot
// because suitability depends on it and the graph is not part of the report:
// without it, "member 4 was not suitable" is unexplainable after the fact.
type guardSnapshotMember struct {
	Node                                             int32
	Role                                             int
	Alive, TransitCapable, IdentityProven, Confirmed bool
}

// guardSetSnapshot is the input set frozen into the report.
//
// ⚠️ A NAME IS NOT AN IDENTIFIER. Two sets called "pinned" with different
// members produce different refusal rates, and a published number carrying only
// the name cannot be reproduced or explained — in particular the gap between the
// confirmed and the sampled population is a fact about the MEMBERS, so a report
// that drops them cannot say where the gap came from. The snapshot is a deep
// copy taken at measurement time and carries a digest, so two sets that share a
// name are still told apart by one glance at the report.
type guardSetSnapshot struct {
	Name string
	// Members keeps the ORDER of the input: it decides which member carries a
	// served request, so it is part of what was measured.
	Members []guardSnapshotMember
}

// snapshotGuardSet copies the set out of the caller's reach. The copy is what
// makes the report immutable: later edits to the input — the very edits the rule
// forbids the measurer itself from making — cannot reach a number already
// published.
func snapshotGuardSet(g *graph, set guardSet) guardSetSnapshot {
	snapshot := guardSetSnapshot{
		Name:    set.Name,
		Members: make([]guardSnapshotMember, 0, len(set.Members)),
	}
	for _, member := range set.Members {
		role := -1
		if member.Node >= 0 && int(member.Node) < len(g.roles) {
			role = g.roles[member.Node]
		}
		snapshot.Members = append(snapshot.Members, guardSnapshotMember{
			Node:           member.Node,
			Role:           role,
			Alive:          member.Alive,
			TransitCapable: member.TransitCapable,
			IdentityProven: member.IdentityProven,
			Confirmed:      member.Confirmed,
		})
	}
	return snapshot
}

// Composition renders every member and every flag: this is the part of the
// report from which the experiment can be rebuilt.
func (s guardSetSnapshot) Composition() string {
	if len(s.Members) == 0 {
		return "empty set"
	}
	parts := make([]string, 0, len(s.Members))
	for _, member := range s.Members {
		role := "¬Q"
		switch member.Role {
		case roleStructural:
			role = "Q"
		case -1:
			role = "role unknown"
		}
		parts = append(parts, fmt.Sprintf("%d{%s alive:%t transit:%t identity:%t confirmed:%t}",
			member.Node, role, member.Alive, member.TransitCapable, member.IdentityProven,
			member.Confirmed))
	}
	return strings.Join(parts, " ")
}

// Digest identifies the composition in one short token, so a set can be
// referred to unambiguously even where the full listing does not fit.
func (s guardSetSnapshot) Digest() string {
	sum := sha256.Sum256([]byte("corsa/overlay/sim/m4/guardset/v1\x00" + s.Composition()))
	return fmt.Sprintf("%x", sum[:4])
}

func (s guardSetSnapshot) String() string {
	return fmt.Sprintf("guard set %q [%s]: %s", s.Name, s.Digest(), s.Composition())
}

// m4Report is the pair of populations §4.3.4″.3 leaves open. They are never
// summed: they are two answers to the same question under two different
// readings of the rule.
type m4Report struct {
	Workload workload
	// Set is the frozen input, not a label for it — see guardSetSnapshot.
	Set       guardSetSnapshot
	Confirmed m4Slice
	Sampled   m4Slice
}

func (r m4Report) String() string {
	return fmt.Sprintf("%s\n%s\n  confirmed only: %s\n  whole set:      %s",
		r.Set, r.Workload, r.Confirmed, r.Sampled)
}

// measureFirstHopRefusals runs one workload against one set, twice.
//
// ⚠️ `set` is taken by value and its Members slice is only read. A test compares
// the set before and after, because "the measurement does not change the set" is
// a property of the rule and not a courtesy of the implementation.
func measureFirstHopRefusals(g *graph, set guardSet, load workload) (m4Report, error) {
	for _, member := range set.Members {
		if member.Node < 0 || int(member.Node) >= len(g.roles) {
			return m4Report{}, fmt.Errorf("guard set %q names node %d, which the graph of %d "+
				"nodes does not have", set.Name, member.Node, len(g.roles))
		}
	}
	for _, target := range load.Targets {
		if target < 0 || int(target) >= len(g.roles) {
			return m4Report{}, fmt.Errorf("workload %q names target %d, which the graph of %d "+
				"nodes does not have", load.Name, target, len(g.roles))
		}
	}

	report := m4Report{Workload: load, Set: snapshotGuardSet(g, set)}
	populations := []struct {
		onlyConfirmed bool
		slice         *m4Slice
	}{
		{true, &report.Confirmed},
		{false, &report.Sampled},
	}

	for _, population := range populations {
		for _, target := range load.Targets {
			result := selectFirstHop(g, set, target, population.onlyConfirmed)
			population.slice.Requests++
			switch result.Outcome {
			case m4Served:
				population.slice.Served++
			case m4RefusedNobodySuitable:
				population.slice.RefusedNobodySuitable++
			default:
				population.slice.RefusedByTargetExclusion++
			}
		}
	}
	return report, nil
}

// --- workloads ---------------------------------------------------------------

// workload is a reproducible sequence of request targets.
//
// ⚠️ Parameters is printed with every number ON PURPOSE. These distributions are
// ASSUMPTIONS chosen to bracket the behaviour of the rule; none of them is
// measured user load, and a report that shows the refusal rate without naming
// the distribution behind it invites exactly that misreading.
type workload struct {
	Name       string
	Parameters string
	Targets    []int32
}

func (w workload) String() string {
	return fmt.Sprintf("workload %q (%s) — assumed distribution, NOT measured user load, %d requests",
		w.Name, w.Parameters, len(w.Targets))
}

// m4Random derives a reproducible 64-bit value, in the style the rest of the
// stand uses: a domain separator of its own, so these draws cannot collide with
// identifiers, roles or the split probe.
func m4Random(seed uint64, index int) uint64 {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(index))
	digest := sha256.Sum256(append([]byte("corsa/overlay/sim/m4/v1"), buf[:]...))
	return binary.LittleEndian.Uint64(digest[:8])
}

// uniformWorkload — every node of the network is equally likely to be a target.
// The optimistic end: the guard set is a handful of nodes, so the chance that a
// target IS one of them is small.
func uniformWorkload(seed uint64, nodes, requests int) workload {
	targets := make([]int32, 0, requests)
	for i := range requests {
		targets = append(targets, int32(m4Random(seed, i)%uint64(nodes)))
	}
	return workload{
		Name:       "uniform targets",
		Parameters: fmt.Sprintf("uniform over %d nodes, seed %d", nodes, seed),
		Targets:    targets,
	}
}

// popularContactWorkload — a small head of popular contacts takes a fixed share
// of the requests, the rest spread uniformly.
//
// ⚠️ Both numbers are DECLARED, not derived from anything: headSize contacts
// drawn with probability headShare. They are a shape, not a finding.
func popularContactWorkload(seed uint64, nodes, requests, headSize int, headShare float64) workload {
	targets := make([]int32, 0, requests)
	for i := range requests {
		draw := m4Random(seed, i)
		// Top 16 bits decide head or tail, the rest picks inside the chosen part.
		inHead := float64(draw>>48)/float64(1<<16) < headShare
		if inHead && headSize > 0 {
			targets = append(targets, int32(draw%uint64(headSize)))
			continue
		}
		targets = append(targets, int32(draw%uint64(nodes)))
	}
	return workload{
		Name: "popular contacts",
		Parameters: fmt.Sprintf("head of %d contacts takes %.0f%% of requests, tail uniform over "+
			"%d nodes, seed %d", headSize, headShare*100, nodes, seed),
		Targets: targets,
	}
}

// mainContactInSetWorkload — the degenerate case of §4.3.4″.3: the one contact
// the user talks to is itself a member of the guard set. With k = 1 this is the
// deterministic refusal of S24а, and the measurer must report 100 %, not an
// average softened by other targets.
func mainContactInSetWorkload(mainContact int32, requests int) workload {
	targets := make([]int32, requests)
	for i := range targets {
		targets[i] = mainContact
	}
	return workload{
		Name: "main contact inside the set",
		Parameters: fmt.Sprintf("every request targets node %d, a member of the set",
			mainContact),
		Targets: targets,
	}
}

// --- fixtures ----------------------------------------------------------------

// transitGraph builds a graph of `nodes` nodes with no edges, where roles are
// declared explicitly. M4 does not look at edges at all — it looks at the guard
// set — so a graph with roles and nothing else is the honest fixture.
func transitGraph(roles []int) *graph {
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
	return g
}

// eligibleGuard is a member that passes every condition — the fixtures then say
// what they take away from it.
func eligibleGuard(node int32, confirmed bool) guardMember {
	return guardMember{
		Node:           node,
		Alive:          true,
		TransitCapable: true,
		IdentityProven: true,
		Confirmed:      confirmed,
	}
}

// describeSet renders a set for comparison before and after a measurement. It
// goes through the snapshot on purpose: one renderer, so a report and a
// before/after comparison can never disagree about what the set was.
func describeSet(g *graph, set guardSet) string {
	return snapshotGuardSet(g, set).String()
}
