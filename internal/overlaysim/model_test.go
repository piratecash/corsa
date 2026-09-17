package overlaysim

// model_test.go implements the graph model of
// docs/refactoring/dht/21-m1-connectivity-model.md §2: deterministic node
// identifiers, role classification by the published contract, link formation
// under a hard connection budget, and the component analysis M1 reports on.
//
// Nothing here is production code and nothing may import it — see doc.go.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	// nodeIDLen is the identifier length of the tree: domain.PeerIdentity is
	// 20 bytes. Written out rather than imported so the harness stays free of
	// production packages — a simulator that pulls in the node is a simulator
	// that can drift into being part of it.
	nodeIDLen = 20

	// roleSeparator is the domain separator of the ACCEPTED contract,
	// docs/protocol/overlay_role.md §3. It is duplicated here on purpose and
	// checked against the published vectors by a test: a harness that computed
	// roles its own way would measure its own arithmetic.
	roleSeparator = "corsa/overlay/role/v1"

	// idSeparator derives simulated identifiers. A DIFFERENT domain from the
	// role: these are test subjects, not another use of the role space.
	idSeparator = "corsa/overlay/sim/v1"

	// c1PickSeparator is the domain of the C1/v1 in-bucket order,
	// docs/refactoring/dht/21-m1-candidate-c1.md §2.2. It is a THIRD domain: the
	// rank of a candidate must not coincide with its identifier or its role,
	// or the rule would inherit a correlation it exists to remove.
	c1PickSeparator = "corsa/overlay/sim/pick/v1"

	// roleStructural is Q = 1: the half that may carry the structural leg.
	//
	// The other half, Q = 0 (¬Q), is the one first hops are chosen from. It has
	// no constant here because M1 asks only about the structural subgraph:
	// "not structural" is the whole of its definition, and a named zero nobody
	// compares against is a constant that will drift.
	roleStructural = 1
)

type nodeID [nodeIDLen]byte

// makeNodeID is §2.1 of the model: identifiers are a deterministic function of
// the seed and the index, so one seed reproduces one graph byte for byte.
func makeNodeID(seed uint64, index int) nodeID {
	var buf [8 + 8]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(index))

	digest := sha256.Sum256(append([]byte(idSeparator), buf[:]...))

	var id nodeID
	copy(id[:], digest[:nodeIDLen])
	return id
}

// roleDigest is the whole of the contract's computation. It is separate from
// roleOf so the harness can be pinned to the published DIGESTS: a test that
// compares one bit agrees with a diverged implementation half the time, which
// is barely better than not testing at all.
func roleDigest(id nodeID) [sha256.Size]byte {
	return sha256.Sum256(append([]byte(roleSeparator), id[:]...))
}

// roleOf is the published contract, not a variant of it: the least significant
// bit of the LAST byte of SHA-256 over the separator and the 20 raw bytes.
func roleOf(id nodeID) int {
	digest := roleDigest(id)
	return int(digest[sha256.Size-1] & 1)
}

// xorLess reports whether a is closer to target than b, comparing XOR distances
// as big-endian numbers — the Kademlia metric of ADR 00 §5.
func xorLess(target, a, b nodeID) bool {
	for i := range nodeIDLen {
		da := target[i] ^ a[i]
		db := target[i] ^ b[i]
		if da != db {
			return da < db
		}
	}
	return false
}

// --- trie over identifiers --------------------------------------------------

// idTrie answers "the closest identifiers to this target that satisfy a
// predicate" without scanning every node.
//
// A linear scan is O(N) per query and the sweep asks N times per shape, which
// is 4·10⁹ comparisons at 64k nodes across the quota range — enough to make the
// measurement not happen. The trie walks the preferred branch first, so the
// first leaves it reaches ARE the closest ones.
type idTrie struct {
	// child holds two entries per node: -1 for absent, otherwise the index of
	// the child node. Leaves carry a node index in leaf.
	child [][2]int32
	leaf  []int32
	ids   []nodeID
}

func newIDTrie(ids []nodeID) *idTrie {
	trie := &idTrie{ids: ids}
	trie.newNode()

	for index := range ids {
		trie.insert(int32(index))
	}
	return trie
}

func (t *idTrie) newNode() int32 {
	t.child = append(t.child, [2]int32{-1, -1})
	t.leaf = append(t.leaf, -1)
	return int32(len(t.child) - 1)
}

func bitAt(id nodeID, depth int) int {
	return int(id[depth/8]>>(7-depth%8)) & 1
}

func (t *idTrie) insert(index int32) {
	id := t.ids[index]
	node := int32(0)

	for depth := range nodeIDLen * 8 {
		bit := bitAt(id, depth)
		if t.child[node][bit] == -1 {
			created := t.newNode()
			t.child[node][bit] = created
			t.leaf[created] = index
			return
		}
		node = t.child[node][bit]
		if t.leaf[node] != -1 {
			// Split: push the sitting leaf one level down and continue.
			sitting := t.leaf[node]
			t.leaf[node] = -1
			sittingBit := bitAt(t.ids[sitting], depth+1)
			created := t.newNode()
			t.child[node][sittingBit] = created
			t.leaf[created] = sitting
		}
	}
}

// nearest walks the trie towards target and yields node indices in increasing
// XOR distance, stopping when accept has taken `want` of them or the tree is
// exhausted.
//
// accept is applied at the leaf, so a rejected candidate does not stop the
// walk: the next-closest is simply tried. That is what makes "closest node that
// still has budget" answerable without re-sorting anything.
func (t *idTrie) nearest(target nodeID, want int, accept func(int32) bool) []int32 {
	out := make([]int32, 0, want)
	if want <= 0 {
		return out
	}

	var walk func(node int32, depth int)
	walk = func(node int32, depth int) {
		if len(out) >= want || node == -1 {
			return
		}
		if leaf := t.leaf[node]; leaf != -1 {
			if accept(leaf) {
				out = append(out, leaf)
			}
			return
		}
		if depth >= nodeIDLen*8 {
			return
		}
		preferred := bitAt(target, depth)
		walk(t.child[node][preferred], depth+1)
		walk(t.child[node][1-preferred], depth+1)
	}
	walk(0, 0)
	return out
}

// nearestInBucket answers the Kademlia question rather than the
// nearest-neighbour one: the closest node whose identifier shares EXACTLY
// `level` leading bits with the target.
//
// That set is one bucket. Walking down the target's own bits to depth `level`
// and then taking the SIBLING subtree is exactly the bucket's membership, so no
// scan and no per-node bookkeeping is needed.
func (t *idTrie) nearestInBucket(target nodeID, level, want int, accept func(int32) bool) []int32 {
	node := int32(0)
	for depth := range level {
		node = t.child[node][bitAt(target, depth)]
		if node == -1 || t.leaf[node] != -1 {
			return nil
		}
	}
	sibling := t.child[node][1-bitAt(target, level)]
	if sibling == -1 {
		return nil
	}

	out := make([]int32, 0, want)
	var walk func(node int32, depth int)
	walk = func(node int32, depth int) {
		if len(out) >= want || node == -1 {
			return
		}
		if leaf := t.leaf[node]; leaf != -1 {
			if accept(leaf) {
				out = append(out, leaf)
			}
			return
		}
		if depth >= nodeIDLen*8 {
			return
		}
		preferred := bitAt(target, depth)
		walk(t.child[node][preferred], depth+1)
		walk(t.child[node][1-preferred], depth+1)
	}
	walk(sibling, level+1)
	return out
}

// forEachInBucket visits EVERY member of the bucket, in no particular order.
//
// It exists for C1/v1, whose order over the bucket is a hash of
// (owner, level, candidate) and therefore cannot be walked towards: there is no
// branch to prefer, so the minimum is only known once every member has been
// seen. The walk is still over the bucket's subtree rather than the whole
// population — the saving that makes the rule runnable at all — and a reference
// test compares it against a naive scan (§5.5 п.1).
//
// ⚠️ The early return when the descent meets a leaf is the same one
// nearestInBucket makes, and it is correct for the same reason: the trie holds
// the target itself, so a leaf on the target's own path means the target is the
// only identifier with that prefix and the bucket below it is empty.
func (t *idTrie) forEachInBucket(target nodeID, level int, visit func(int32)) {
	node := int32(0)
	for depth := range level {
		node = t.child[node][bitAt(target, depth)]
		if node == -1 || t.leaf[node] != -1 {
			return
		}
	}
	sibling := t.child[node][1-bitAt(target, level)]
	if sibling == -1 {
		return
	}

	var walk func(node int32)
	walk = func(node int32) {
		if node == -1 {
			return
		}
		if leaf := t.leaf[node]; leaf != -1 {
			visit(leaf)
			return
		}
		walk(t.child[node][0])
		walk(t.child[node][1])
	}
	walk(sibling)
}

// --- C1/v1: the bucket representative by hash -------------------------------

// c1Rank is the published rule of
// docs/refactoring/dht/21-m1-candidate-c1.md §2.2, byte for byte:
//
//	H( "corsa/overlay/sim/pick/v1" ‖ NodeID(u) ‖ uint8(i) ‖ NodeID(c) )
//
// compared as a 32-byte big-endian number.
//
// ⚠️ The owner is INSIDE the hash, and that is the whole point: without it every
// node of a bucket would rank its members identically and they would all pick
// the same representative — a hub by construction. Whether hubs appear anyway is
// still measured (§5.4) rather than argued from this comment.
func c1Rank(owner nodeID, level int, candidate nodeID) [sha256.Size]byte {
	if level < 0 || level > 255 {
		panic(fmt.Sprintf("C1/v1 ranks levels 0…255, got %d — the rule encodes the level as "+
			"one byte and a wider level would silently alias", level))
	}

	var buf [len(c1PickSeparator) + nodeIDLen + 1 + nodeIDLen]byte
	at := copy(buf[:], c1PickSeparator)
	at += copy(buf[at:], owner[:])
	buf[at] = uint8(level)
	at++
	copy(buf[at:], candidate[:])

	return sha256.Sum256(buf[:])
}

// c1Representative is the C1/v1 in-bucket choice: the accepted member of the
// bucket with the smallest rank.
//
// ⚠️ It returns -1 when the bucket holds no acceptable member, exactly as
// nearestInBucket does when its walk finds none — the two rules must be
// indistinguishable in every respect but WHICH member they take, or a difference
// in the results would be unattributable.
//
// ⚠️ THE COST IS THE RULE'S, not the implementation's, and it is not small. A
// hash order gives the walk no branch to prefer, so every member of the bucket
// has to be ranked, and the level-0 bucket is half the network: the candidate is
// quadratic in N where its base is not. Measured on four cores at 56 ns per
// rank: 1k ≈ 70 ms per graph, 10k ≈ 6 s, and 64k extrapolates to ≈4.5 minutes.
// Spreading the ranking over goroutines was tried and gave ≈1.2× end to end —
// the walk and the filter cost as much as the hash — which does not pay for
// concurrency inside a model whose value is that it can be read. The number
// belongs in the run registry so the owner can plan around it.
func c1Representative(
	trie *idTrie, ids []nodeID, scratch *[]int32, owner int32, level int, accept func(int32) bool,
) int32 {
	// The scratch buffer is the caller's and is reused across picks: the
	// builder is sequential, so the level-0 bucket is not re-allocated N times.
	members := (*scratch)[:0]
	trie.forEachInBucket(ids[owner], level, func(candidate int32) {
		if accept(candidate) {
			members = append(members, candidate)
		}
	})
	*scratch = members

	best := int32(-1)
	var bestRank [sha256.Size]byte
	for _, candidate := range members {
		rank := c1Rank(ids[owner], level, ids[candidate])
		if best == -1 || bytes.Compare(rank[:], bestRank[:]) < 0 {
			best, bestRank = candidate, rank
		}
	}
	return best
}

// --- graph ------------------------------------------------------------------

// shape is one network form of the measurement plan: how many nodes, how many
// neighbours each wants, and the hard ceiling it will not exceed.
type shape struct {
	name string
	// nodes is N.
	nodes int
	// degree is d — the neighbours a node WANTS.
	degree int
	// budget is B — the ceiling it will not exceed for any reason
	// (15-overlay-parameters.md §2).
	budget int
}

// graph is one built network: adjacency plus the role of every node.
type graph struct {
	ids       []nodeID
	roles     []int
	adjacency [][]int32
	// structuralNeighbours counts, per node, how many of its neighbours are
	// structural.
	structuralNeighbours []int
	// initiated counts links a node CHOSE, as opposed to accepted. The quota
	// is only allowed to redirect these; a node's total degree may still rise
	// above its desired degree because others chose it, and that rise is a
	// measured cost rather than a modelling error.
	initiated []int
	// shortfall records WHY a node's quota went unmet, per node.
	shortfall []quotaShortfall
	// edges records every edge with the rule that produced it, in creation
	// order. ⚠️ It is the only place the provenance exists: adjacency lists say
	// who is connected to whom and nothing about which rule decided it, and
	// §2.5 of the candidate requires the two rules to be reported apart.
	edges []edgeRecord
}

// quotaShortfall separates the reasons a node ended below its Q quota. They are
// not the same fix, and one of them is not about the budget at all.
//
// ⚠️ This exists because the first version of the results attributed the whole
// shortfall to receiving capacity B, on the strength of two columns moving
// together (`at B` up, `quota✓` down). Correlation between two aggregates is
// not attribution: a node can be filled to its desired degree by INCOMING links
// and never look for a structural neighbour, and no amount of budget at the
// other end changes that.
type quotaShortfall struct {
	// FullBeforeSearch and FilledDuringSearch split what used to be one flag,
	// and the split matters: the first says the node NEVER LOOKED, the second
	// only that it stopped early. Lumped together they were read as "the whole
	// shortfall is nodes that never searched", which the merged flag could not
	// support — it was set after the loop, so a node that searched, was
	// refused, and only then reached the limit landed in the same bucket.
	//
	// "Full" is either limit: the desired degree d, which counts INCOMING
	// links, or the node's own ceiling B. With B > d — true of every swept
	// shape — it is always the former, and that is the point: links the
	// network made TO this node are what stop it.
	//
	// The two are mutually exclusive by construction: the first is decided
	// before the first iteration, the second only if the first is false.
	FullBeforeSearch   bool
	FilledDuringSearch bool

	// CandidateAtBudget counts buckets where a structural candidate existed
	// and was refused because IT had no room. This, and only this, is the
	// receiving-capacity story.
	CandidateAtBudget int

	// NoCandidate counts the times no structural node was available to link
	// to at all — a bucket holding none this node is not already linked to,
	// or, in the leftover fill, nobody left in the whole network. A
	// population and geometry limit, not a budget one.
	NoCandidate int

	// LeftoverIgnoredQuota counts slots spent on a ¬Q node by the leftover
	// fill while the quota was still unmet. That is the POLICY declining to
	// prefer, not the network declining to accept.
	LeftoverIgnoredQuota int

	// SecondPassLinks, SecondPassFoundNobody and SecondPassOutOfBudget exist
	// only under policy 3, and the last two are the two DIFFERENT ways the
	// repair can fail to finish a node: the network had nobody left to give,
	// or this node had no room left to take. Without both, "the second pass
	// did not help" has no answer in the record.
	SecondPassLinks       int
	SecondPassFoundNobody int
	SecondPassOutOfBudget int
}

// Unmet reports whether this node lost the quota for a reason that was recorded
// at all. A node can lose it for several at once, which is why the report gives
// each reason its own share rather than splitting one hundred per cent.
func (s quotaShortfall) Unmet() bool {
	return s.FullBeforeSearch || s.FilledDuringSearch ||
		s.CandidateAtBudget > 0 || s.NoCandidate > 0 || s.LeftoverIgnoredQuota > 0
}

// shortfallSlice is the attribution over one population of nodes.
type shortfallSlice struct {
	// Nodes is the size of the population the shares are taken over.
	Nodes int

	FullBeforeSearch     float64
	FilledDuringSearch   float64
	CandidateAtBudget    float64
	NoCandidate          float64
	LeftoverIgnoredQuota float64
}

// attributeShortfall counts, over the nodes selected by member, how many met
// each obstacle at least once.
func attributeShortfall(g *graph, nodes int, member func(int) bool) shortfallSlice {
	slice := shortfallSlice{}
	counts := map[string]int{}

	for i := range nodes {
		if !member(i) {
			continue
		}
		slice.Nodes++

		reasons := g.shortfall[i]
		if reasons.FullBeforeSearch {
			counts["before"]++
		}
		if reasons.FilledDuringSearch {
			counts["during"]++
		}
		if reasons.CandidateAtBudget > 0 {
			counts["budget"]++
		}
		if reasons.NoCandidate > 0 {
			counts["absent"]++
		}
		if reasons.LeftoverIgnoredQuota > 0 {
			counts["leftover"]++
		}
	}

	if slice.Nodes == 0 {
		return slice
	}
	share := func(key string) float64 { return float64(counts[key]) / float64(slice.Nodes) }
	slice.FullBeforeSearch = share("before")
	slice.FilledDuringSearch = share("during")
	slice.CandidateAtBudget = share("budget")
	slice.NoCandidate = share("absent")
	slice.LeftoverIgnoredQuota = share("leftover")
	return slice
}

// Searched reports whether the node ever got to ask anyone. Only the nodes for
// which this is false support the statement "it did not look for structural
// neighbours at all".
func (s quotaShortfall) Searched() bool { return !s.FullBeforeSearch }

// policy is a neighbour-selection rule, §2.4 of the model. The three exist to
// answer one question: can the isolation of structural nodes be removed by
// changing the SELECTION, without raising B and without softening the
// "one component" criterion.
//
// ⚠️ Their rules were written down before any of them was implemented. A policy
// shaped around a result already seen is a policy nobody can tell from a fitted
// curve afterwards.
type policy int

const (
	// policyBaseline is the algorithm the earlier runs measured, unchanged. It
	// is the control, not a candidate.
	policyBaseline policy = iota

	// policyInitiatedLimit caps a node's OWN set at d counting only the links
	// it initiated, leaving B as the only limit on total connections.
	// Incoming links stop consuming the search.
	policyInitiatedLimit

	// policySecondPass runs the baseline to completion and then gives the nodes
	// that missed their quota one more attempt, spending free B rather than
	// free d.
	policySecondPass

	// policyCandidateC1 is the candidate of
	// docs/refactoring/dht/21-m1-candidate-c1.md §2: the initiated limit of
	// policy 2 plus ONE further change — the member taken from a bucket is the
	// one with the smallest c1Rank instead of the XOR-nearest one.
	//
	// ⚠️ Everything else is deliberately identical to policyInitiatedLimit:
	// levels 0…d-1, the Q quota as a per-bucket preference, the ceiling B, and
	// the leftover fill by XOR-nearest over the whole network (§2.5). That is
	// what makes initiated-limit the comparison BASE — one rule apart, so a
	// difference has one place to come from. The baseline stays in the runs as
	// a control that the stand itself has not moved, not as the base.
	policyCandidateC1
)

func (p policy) String() string {
	switch p {
	case policyInitiatedLimit:
		return "initiated-limit"
	case policySecondPass:
		return "second-pass"
	case policyCandidateC1:
		return "C1/v1"
	default:
		return "baseline"
	}
}

// --- where an edge came from -------------------------------------------------

// edgeOrigin says WHICH rule produced an edge. §2.5 of the candidate keeps the
// leftover fill unchanged on purpose, so the two rules coexist in one run and
// their results must not be read as one: "every bucket edge is born at a level
// below d" is true of the first and false of the second by construction, and a
// check applied to both would call a correct run defective.
type edgeOrigin int

const (
	// edgeFromBucket — the per-level contact, quota preference included.
	edgeFromBucket edgeOrigin = iota
	// edgeFromLeftover — the fill that takes the XOR-nearest node of the WHOLE
	// network for a slot no bucket could fill.
	edgeFromLeftover
	// edgeFromRepair — the second pass of policySecondPass. Zero under every
	// other policy.
	edgeFromRepair
)

func (o edgeOrigin) String() string {
	switch o {
	case edgeFromBucket:
		return "bucket"
	case edgeFromLeftover:
		return "leftover"
	default:
		return "repair"
	}
}

// edgeRecord is one edge with its provenance. The initiator is kept apart from
// the peer because only the initiator chose: an incoming edge is not evidence
// about the rule that runs at the receiving end.
type edgeRecord struct {
	Initiator int32
	Peer      int32
	Origin    edgeOrigin
	// Level is the bucket level the edge was born at, or -1 where the origin
	// has no level (the leftover fill and the repair pass both range over the
	// whole network).
	Level int
}

// selectionPhase says which of the three selection rules produced a moment:
// the quota preference inside a bucket, the ordinary bucket contact, or the
// leftover fill.
type selectionPhase int

const (
	phaseQuota selectionPhase = iota
	phasePlain
	phaseLeftover
)

func (p selectionPhase) String() string {
	switch p {
	case phaseQuota:
		return "quota"
	case phasePlain:
		return "plain"
	default:
		return "leftover"
	}
}

// selectionMoment is one decision, reported to an observer AT THE INSTANT it is
// made. Availability is the thing that cannot be reconstructed afterwards: who
// had room changes as the graph is built, so a post-hoc answer to "was there a
// candidate" describes the end state and not the moment of the choice.
type selectionMoment struct {
	Node   int32
	Level  int
	Phase  selectionPhase
	Chosen int32 // -1 when the rule found nobody

	// Available answers, with the builder's own live filter plus `extra`,
	// which candidate this rule WOULD have taken. It is a question about this
	// instant, asked from the same trie the builder is using.
	Available func(extra func(int32) bool) int32
}

// bucketChooser answers "which member of this bucket does the node take",
// given the filter the builder has already applied. nil means the model's own
// rule: the XOR-nearest one.
//
// ⚠️ This exists for ONE diagnostic question — whether the preservation of bit d
// comes from the nearest-rule or from the bucket structure itself
// (21-m1-split-diagnosis.md §7). It is a PROBE, not a policy: a node that picks
// bucket members at random has no routing property left, and nothing may be
// adopted on the strength of a run that uses it.
type bucketChooser func(node int32, level int, accept func(int32) bool) int32

// selectionObserver watches the construction without touching it. A test that
// attaches one must produce a byte-identical graph — there is a guard for that,
// because a diagnostic that changes what it measures measures nothing.
type selectionObserver func(selectionMoment)

// buildGraph is §2.2 of the model.
//
// Nodes are processed in index order, each filling first its Q quota and then
// the rest of its degree with the XOR-closest peers that still have budget. A
// link consumes a slot at BOTH ends, and a node at its ceiling accepts nothing
// — the one pessimistic element of the model and the reason B exists.
//
// ⚠️ No edge is ever added to make the graph connected. The quota moves slots a
// node was going to spend anyway; it does not add slots.
func buildGraph(sh shape, seed uint64, quota int, selection policy) *graph {
	return buildGraphObserved(sh, seed, quota, selection, nil)
}

// buildGraphObserved is buildGraph with an optional observer. The two share
// this one body on purpose: a diagnostic built on a copy of the algorithm
// diagnoses the copy.
func buildGraphObserved(
	sh shape, seed uint64, quota int, selection policy, observe selectionObserver,
) *graph {
	return buildGraphProbed(sh, seed, quota, selection, observe, nil)
}

// buildGraphProbed is buildGraphObserved with the in-bucket choice replaceable.
// Everything else — levels, budget, quota, order, filters — is the same code.
func buildGraphProbed(
	sh shape, seed uint64, quota int, selection policy,
	observe selectionObserver, chooseInBucket bucketChooser,
) *graph {
	ids := make([]nodeID, sh.nodes)
	for i := range ids {
		ids[i] = makeNodeID(seed, i)
	}
	return buildGraphOnIDs(ids, sh, quota, selection, observe, chooseInBucket)
}

// buildGraphOnIDs is the body of the builder, over identifiers somebody else
// chose. M3-a needs a population with a given share of Q, and it gets it by
// SELECTING identifiers (m3a_test.go) rather than by relabelling nodes — which
// is why this seam exists and why roles are computed here, from the identifier,
// by the one classifier. There is no path through this file that assigns a role
// any other way.
func buildGraphOnIDs(
	ids []nodeID, sh shape, quota int, selection policy,
	observe selectionObserver, chooseInBucket bucketChooser,
) *graph {
	if len(ids) != sh.nodes {
		panic(fmt.Sprintf("shape %s wants %d nodes, got %d identifiers", sh.name, sh.nodes, len(ids)))
	}
	roles := make([]int, sh.nodes)
	for i := range ids {
		roles[i] = roleOf(ids[i])
	}

	trie := newIDTrie(ids)

	// Owned by the builder, reused by every C1/v1 pick: see c1Representative.
	c1Scratch := make([]int32, 0, sh.nodes)

	g := &graph{
		ids:                  ids,
		roles:                roles,
		adjacency:            make([][]int32, sh.nodes),
		structuralNeighbours: make([]int, sh.nodes),
		initiated:            make([]int, sh.nodes),
	}
	linked := make([]map[int32]struct{}, sh.nodes)
	for i := range linked {
		linked[i] = make(map[int32]struct{}, sh.degree)
	}

	g.shortfall = make([]quotaShortfall, sh.nodes)

	degreeOf := func(i int32) int { return len(g.adjacency[i]) }
	hasRoom := func(i int32) bool { return degreeOf(i) < sh.budget }

	// The ONE line that separates policy 2 from the baseline: what counts
	// against the desired degree. The baseline counts every link, so being
	// dialled d times ends the search; the initiated limit counts only the
	// links this node chose, so incoming ones no longer consume it. B still
	// caps the total either way.
	wantsMore := func(i int32) bool {
		if selection == policyInitiatedLimit || selection == policyCandidateC1 {
			return g.initiated[i] < sh.degree
		}
		return degreeOf(i) < sh.degree
	}

	connect := func(u, v int32, origin edgeOrigin, level int) {
		g.edges = append(g.edges, edgeRecord{Initiator: u, Peer: v, Origin: origin, Level: level})
		g.initiated[u]++
		g.adjacency[u] = append(g.adjacency[u], v)
		g.adjacency[v] = append(g.adjacency[v], u)
		linked[u][v] = struct{}{}
		linked[v][u] = struct{}{}
		if roles[v] == roleStructural {
			g.structuralNeighbours[u]++
		}
		if roles[u] == roleStructural {
			g.structuralNeighbours[v]++
		}
	}

	for i := range sh.nodes {
		u := int32(i)

		// Whether this node was already full BEFORE it looked at anyone. It
		// has to be captured here: after the loop the two cases are
		// indistinguishable, and telling them apart is the difference between
		// "never searched" and "searched and ran out".
		fullBeforeSearch := !wantsMore(u) || !hasRoom(u)

		// One contact per BUCKET, from the shallowest level outwards — the
		// routing table of ADR 00 §5, not a nearest-neighbour graph.
		//
		// ⚠️ The first version of this model took the d XOR-closest nodes for
		// every slot, and the report caught it: the BASE graph came apart into
		// seventy-odd components at 1k nodes. Nearest-neighbour graphs are
		// tight local clusters with no long links, so nothing could reach
		// anything — a property of the fixture, not of role separation, and it
		// would have made every Q number below meaningless.
		//
		// Level i is the set sharing exactly i leading bits with u: level 0 is
		// half the network, level 1 a quarter, and so on. One contact from each
		// gives the logarithmic diameter the geometry is chosen for.
		for level := 0; level < sh.degree && wantsMore(u) && hasRoom(u); level++ {
			free := func(candidate int32) bool {
				if candidate == u {
					return false
				}
				if _, already := linked[u][candidate]; already {
					return false
				}
				return hasRoom(candidate)
			}

			// While the quota is unmet, this bucket is asked for a structural
			// contact FIRST. It is a preference per bucket, not an extra slot:
			// when the bucket has no structural member the ordinary contact is
			// taken and the quota simply goes unmet — which is the outcome the
			// report has a column for.
			pick := func(accept func(int32) bool) int32 {
				if chooseInBucket != nil {
					return chooseInBucket(u, level, accept)
				}
				// ⚠️ The candidate differs from the base HERE and nowhere else.
				if selection == policyCandidateC1 {
					return c1Representative(trie, ids, &c1Scratch, u, level, accept)
				}
				if found := trie.nearestInBucket(ids[u], level, 1, accept); len(found) == 1 {
					return found[0]
				}
				return -1
			}

			// ⚠️ Availability is asked through the SAME chooser: with a probe
			// installed, "who would this rule have taken" is a question about
			// the probe, not about the model's nearest-rule.
			available := func(extra func(int32) bool) int32 {
				return pick(func(candidate int32) bool {
					return free(candidate) && extra(candidate)
				})
			}

			if g.structuralNeighbours[u] < quota {
				structural := pick(func(candidate int32) bool {
					return roles[candidate] == roleStructural && free(candidate)
				})
				if structural != -1 {
					if observe != nil {
						observe(selectionMoment{Node: u, Level: level, Phase: phaseQuota,
							Chosen: structural, Available: available})
					}
					connect(u, structural, edgeFromBucket, level)
					continue
				}
				if observe != nil {
					observe(selectionMoment{Node: u, Level: level, Phase: phaseQuota,
						Chosen: -1, Available: available})
				}

				// The quota was wanted here and not filled. WHY is a separate
				// question from THAT, and the report has to be able to answer
				// it: ask the same bucket again ignoring only the candidate's
				// ceiling. Found now ⇒ a structural node was there and refused
				// for lack of budget; still nothing ⇒ the bucket holds no
				// structural node this one is not already linked to.
				reachable := trie.nearestInBucket(ids[u], level, 1, func(candidate int32) bool {
					if candidate == u || roles[candidate] != roleStructural {
						return false
					}
					_, already := linked[u][candidate]
					return !already
				})
				if len(reachable) == 1 {
					g.shortfall[u].CandidateAtBudget++
				} else {
					g.shortfall[u].NoCandidate++
				}
			}

			chosen := pick(free)
			if observe != nil {
				observe(selectionMoment{Node: u, Level: level, Phase: phasePlain,
					Chosen: chosen, Available: available})
			}
			if chosen != -1 {
				connect(u, chosen, edgeFromBucket, level)
			}
		}

		// ⚠️ The loop above stops at the DESIRED DEGREE, and that degree counts
		// INCOMING links. A node the network dialled d times never runs a
		// single iteration: it does not look for structural neighbours at all,
		// whatever budget they have. That is a cause of an unmet quota which
		// has nothing to do with the ceiling B, and until it is counted apart
		// the shortfall cannot be blamed on receiving capacity.
		if g.structuralNeighbours[u] < quota && (!wantsMore(u) || !hasRoom(u)) {
			if fullBeforeSearch {
				g.shortfall[u].FullBeforeSearch = true
			} else {
				g.shortfall[u].FilledDuringSearch = true
			}
		}

		// Buckets that were empty leave slots unused. They are filled from
		// whoever is closest, which is what a node with spare capacity and a
		// sparse table does.
		for wantsMore(u) && hasRoom(u) {
			accept := func(candidate int32) bool {
				if candidate == u {
					return false
				}
				if _, already := linked[u][candidate]; already {
					return false
				}
				return hasRoom(candidate)
			}
			found := trie.nearest(ids[u], 1, accept)
			if len(found) == 0 {
				// Nothing recorded here on purpose. Reaching this point means
				// the bucket loop ran to the end, and every level of it either
				// gained a structural neighbour or wrote down why it did not —
				// so a node still short of the quota already carries a reason.
				// A counter here would be one no run can make fire on its own,
				// which is a counter nobody can trust.
				break
			}

			// ⚠️ A third cause, and one the policy creates rather than the
			// budget: leftover slots ignore the quota entirely. A node still
			// short of structural neighbours spends a slot on whoever is
			// nearest, of either half, while structural nodes with room exist
			// elsewhere in the network.
			if g.structuralNeighbours[u] < quota && roles[found[0]] != roleStructural {
				g.shortfall[u].LeftoverIgnoredQuota++
			}
			connect(u, found[0], edgeFromLeftover, -1)
		}
	}

	if selection == policySecondPass {
		secondPassForUnmetQuota(g, sh, trie, quota, linked, hasRoom, connect)
	}
	return g
}

// secondPassForUnmetQuota is policy 3, §2.4. It runs only after the baseline
// has finished every node, walks the nodes whose OWN quota went unmet, and
// spends free B — not free d — on the closest structural nodes left.
//
// ⚠️ The trigger is the node's own shortfall and nothing else. Adding edges
// because the component analysis asked for them would make the model prove
// what it arranged, so connectivity is never consulted here.
//
// ⚠️ One pass, in index order. Repeating until nothing changes is a different
// policy and would have to be measured as one.
func secondPassForUnmetQuota(
	g *graph,
	sh shape,
	trie *idTrie,
	quota int,
	linked []map[int32]struct{},
	hasRoom func(int32) bool,
	connect func(u, v int32, origin edgeOrigin, level int),
) {
	for i := range sh.nodes {
		u := int32(i)

		if g.structuralNeighbours[u] < quota && !hasRoom(u) {
			// Out of its OWN budget before the repair could start. Recorded
			// apart from "nobody to link to": one is answered by raising B,
			// the other is not, and a report that merges them answers neither.
			g.shortfall[u].SecondPassOutOfBudget++
		}

		for g.structuralNeighbours[u] < quota && hasRoom(u) {
			// Closest structural node anywhere, rather than one per bucket:
			// this is repair of a shortfall, not construction of a routing
			// table, and the bucket structure was already built by the pass
			// before.
			found := trie.nearest(g.ids[u], 1, func(candidate int32) bool {
				if candidate == u || g.roles[candidate] != roleStructural {
					return false
				}
				if _, already := linked[u][candidate]; already {
					return false
				}
				return hasRoom(candidate)
			})
			if len(found) == 0 {
				// Every structural node is either a neighbour already or at
				// its ceiling. The first pass recorded why the quota went
				// unmet; this records that the repair had nothing to work
				// with either.
				g.shortfall[u].SecondPassFoundNobody++
				break
			}
			connect(u, found[0], edgeFromRepair, -1)
			g.shortfall[u].SecondPassLinks++

			if g.structuralNeighbours[u] < quota && !hasRoom(u) {
				g.shortfall[u].SecondPassOutOfBudget++
			}
		}
	}
}

// --- components -------------------------------------------------------------

// componentReport is the connectivity of one graph.
type componentReport struct {
	// Nodes is how many nodes the analysis covered.
	Nodes int
	// Components counts connected components. An isolated node is a component
	// of one.
	Components int
	// Largest is the size of the biggest component.
	Largest int
	// Isolated counts nodes with no edge INSIDE the analysed set. Reported
	// separately because a largest-component share of 0.999 hides sixty-four
	// unreachable nodes at 64k.
	Isolated int
}

// LargestShare is the number M1 turns on.
func (r componentReport) LargestShare() float64 {
	if r.Nodes == 0 {
		return 0
	}
	return float64(r.Largest) / float64(r.Nodes)
}

// analyseComponents runs over the subset of nodes selected by `member`, and
// follows only edges whose BOTH ends are members — which is what makes it the
// induced subgraph of §2.3 rather than a walk that escapes through the other
// half.
func analyseComponents(g *graph, member func(int32) bool) componentReport {
	report := componentReport{}

	seen := make([]bool, len(g.adjacency))
	queue := make([]int32, 0, 64)

	for i := range g.adjacency {
		u := int32(i)
		if !member(u) {
			continue
		}
		report.Nodes++
	}

	for i := range g.adjacency {
		u := int32(i)
		if !member(u) || seen[u] {
			continue
		}

		size := 0
		degreeInside := 0
		seen[u] = true
		queue = append(queue[:0], u)

		for len(queue) > 0 {
			current := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			size++

			inside := 0
			for _, next := range g.adjacency[current] {
				if !member(next) {
					continue
				}
				inside++
				if !seen[next] {
					seen[next] = true
					queue = append(queue, next)
				}
			}
			if current == u {
				degreeInside = inside
			}
		}

		report.Components++
		if size > report.Largest {
			report.Largest = size
		}
		if size == 1 && degreeInside == 0 {
			report.Isolated++
		}
	}
	return report
}

// --- run metrics ------------------------------------------------------------

// runReport is one (shape, seed, quota) point of the sweep.
type runReport struct {
	Shape  string
	Seed   uint64
	Quota  int
	Policy policy

	// Links is the number of EDGES in the graph — every link is initiated by
	// exactly one end, so summing the initiated counts gives it. This is the
	// price of a policy, stated in the unit the budget is stated in.
	//
	// SecondPassLinks is how many of them the repair pass added, and
	// SecondPassStuck how many nodes the repair reached with nothing left to
	// give them. Both are zero for every policy but the third.
	Links           int
	SecondPassLinks int

	// BucketLinks, LeftoverLinks and RepairLinks split Links by the rule that
	// produced the edge (§2.5 of the candidate).
	//
	// ⚠️ The leftover share is a REPORTED NUMBER, not a footnote. C1/v1 changes
	// the bucket rule and leaves the leftover fill alone, so the share of edges
	// the leftover fill produced is the share of the graph the candidate did not
	// touch. If it turns out to be large, the attribution of any difference is
	// weaker than it looks — and that is a question for the owner, not a quiet
	// correction inside the rule.
	BucketLinks   int
	LeftoverLinks int
	RepairLinks   int
	// SecondPassStuck counts nodes the repair could find nobody for;
	// SecondPassOutOfBudget counts nodes that ran out of their own B. Two
	// different failures, and only the second is about capacity.
	SecondPassStuck       int
	SecondPassOutOfBudget int

	Base       componentReport
	Structural componentReport

	// StructuralNodes is how many nodes fell in the Q half. Reported because
	// the 50/50 split is an ASSUMPTION (§0″.4), not a guarantee, and a skewed
	// draw would change what the connectivity number means.
	StructuralNodes int

	// QuotaMet is the share of nodes that actually reached the requested
	// quota. A quota that cannot be filled does not deliver connectivity, and
	// the gap between asked and achieved is a result in its own right.
	QuotaMet    float64
	QuotaMin    int
	QuotaMedian int

	// ShortOfQuota attributes the shortfall over every node below the quota.
	// IsolatedStructural attributes it over the Q nodes with NO structural
	// neighbour at all — the nodes that actually break connectivity.
	//
	// ⚠️ They are kept apart because they are different populations and the
	// second is not a subset worth assuming: the first includes ¬Q nodes and Q
	// nodes that merely fell a neighbour or two short. Explaining isolation
	// with the first set's shares is what this split exists to prevent.
	ShortOfQuota       shortfallSlice
	IsolatedStructural shortfallSlice

	// MeanDegree and MaxDegree are the connection spend; AtBudget is the share
	// of nodes sitting on the ceiling, Unfilled the share that never reached
	// its desired degree.
	MeanDegree float64
	MaxDegree  int
	AtBudget   float64
	Unfilled   float64

	// MaxStructuralDegree is the largest degree of any Q node, and
	// StructuralAtBudget the share of Q nodes sitting on the ceiling — the
	// concentration figures of §5.4, measured on the structural half because a
	// hub would form there first and a network-wide average would hide it.
	//
	// ⚠️ OF THE TWO, ONLY THE SECOND CAN SHOW A HUB. MaxStructuralDegree is
	// bounded by B by construction, so in any run where some node reaches the
	// ceiling it reads exactly B whatever the policy — measured: 16 under all
	// four. It is kept because "the maximum is below B" is a real (if weak)
	// statement about a network that is not saturated; the figure that separates
	// the policies is StructuralAtBudget (measured at 1k×8 quota 1: 61 % under
	// the base against 84 % under the candidate).
	MaxStructuralDegree int
	StructuralAtBudget  float64

	// MaxInitiated is the largest number of links any node CHOSE. It must
	// never exceed the desired degree: everything above that in MeanDegree is
	// connections other nodes made TO it, which is how a quota concentrates
	// load on the structural half.
	MaxInitiated int
}

func measure(sh shape, seed uint64, quota int, selection policy) runReport {
	g := buildGraph(sh, seed, quota, selection)

	report := runReport{Shape: sh.name, Seed: seed, Quota: quota, Policy: selection}
	report.Base = analyseComponents(g, func(int32) bool { return true })
	report.Structural = analyseComponents(g, func(i int32) bool { return g.roles[i] == roleStructural })

	// The provenance split, taken from the edge record rather than counted
	// alongside it: two counters of one thing drift, and this one would drift
	// silently because both summands still add up to Links.
	for _, edge := range g.edges {
		switch edge.Origin {
		case edgeFromBucket:
			report.BucketLinks++
		case edgeFromLeftover:
			report.LeftoverLinks++
		default:
			report.RepairLinks++
		}
	}

	structuralCounts := make([]int, 0, sh.nodes)
	totalDegree := 0
	met, atBudget, unfilled := 0, 0, 0
	structuralAtBudget := 0

	for i := range sh.nodes {
		if g.roles[i] == roleStructural {
			report.StructuralNodes++
			if degree := len(g.adjacency[i]); degree > report.MaxStructuralDegree {
				report.MaxStructuralDegree = degree
			}
			if len(g.adjacency[i]) >= sh.budget {
				structuralAtBudget++
			}
		}
		degree := len(g.adjacency[i])
		totalDegree += degree
		if degree > report.MaxDegree {
			report.MaxDegree = degree
		}
		if degree >= sh.budget {
			atBudget++
		}
		if degree < sh.degree {
			unfilled++
		}
		if g.initiated[i] > report.MaxInitiated {
			report.MaxInitiated = g.initiated[i]
		}
		report.Links += g.initiated[i]
		report.SecondPassLinks += g.shortfall[i].SecondPassLinks
		if g.shortfall[i].SecondPassFoundNobody > 0 {
			report.SecondPassStuck++
		}
		if g.shortfall[i].SecondPassOutOfBudget > 0 {
			report.SecondPassOutOfBudget++
		}
		structuralCounts = append(structuralCounts, g.structuralNeighbours[i])
		if g.structuralNeighbours[i] >= quota {
			met++
		}
	}

	// Attribution, computed over TWO different populations.
	//
	// ⚠️ They answer different questions and were conflated once already: the
	// results explained ISOLATED STRUCTURAL nodes — the ones that actually cost
	// connectivity — with shares taken over EVERY node below the quota, a set
	// that also holds ¬Q nodes and Q nodes with plenty of structural
	// neighbours. A share of the wrong population is not evidence about the
	// right one.
	//
	// Within each population the shares do NOT sum to one: a node can be
	// refused for budget in one bucket and run out of capacity later. Each
	// share answers "how many of these met this obstacle at least once", which
	// is the question that distinguishes the fixes.
	report.ShortOfQuota = attributeShortfall(g, sh.nodes, func(i int) bool {
		return g.structuralNeighbours[i] < quota
	})
	report.IsolatedStructural = attributeShortfall(g, sh.nodes, func(i int) bool {
		return g.roles[i] == roleStructural && g.structuralNeighbours[i] == 0
	})

	sort.Ints(structuralCounts)
	report.QuotaMet = float64(met) / float64(sh.nodes)
	report.QuotaMin = structuralCounts[0]
	report.QuotaMedian = structuralCounts[len(structuralCounts)/2]
	report.MeanDegree = float64(totalDegree) / float64(sh.nodes)
	report.AtBudget = float64(atBudget) / float64(sh.nodes)
	report.Unfilled = float64(unfilled) / float64(sh.nodes)
	if report.StructuralNodes > 0 {
		report.StructuralAtBudget = float64(structuralAtBudget) / float64(report.StructuralNodes)
	}

	return report
}

// LeftoverShare is the share of edges the leftover fill produced, rendered so an
// empty graph says "no data" rather than claiming a clean zero.
func (r runReport) LeftoverShare() string {
	if r.Links == 0 {
		return "no data"
	}
	return fmt.Sprintf("%.1f%%", float64(r.LeftoverLinks)/float64(r.Links)*100)
}
