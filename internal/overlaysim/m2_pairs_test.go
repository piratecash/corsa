package overlaysim

// m2_pairs_test.go is the missing half of the M2 contract
// (docs/refactoring/dht/21-m1-candidate-c1.md §5.3): the SAMPLE OF PAIRS.
//
// The measurer could already route a list of pairs and keep the two graphs on
// one list. What it could not say was where that list comes from — and a length
// distribution is a statement about a sample, so "a fixed sample from the seed"
// is not a contract until the size, the draw, the seed, and what happens to
// self-pairs and repeats are all written down.
//
// The rules, all four of them:
//
//  1. BOTH ENDS ARE Q. M2-H routes through the structural half only, so a pair
//     with a ¬Q end is not a pair that measurement can be put to; and since the
//     two measurements must use ONE sample (§5.3), the whole sample is drawn
//     from the Q population. The ¬Q half is not thereby declared uninteresting —
//     it is measured by M5, which asks a different question.
//  2. SELF-PAIRS ARE REJECTED, not silently kept. Source equal to target
//     succeeds in zero hops by definition, so a sample holding them would pull
//     the median towards zero by an amount nobody chose. They are counted.
//  3. AN EXACT REPEAT IS REJECTED. The same ordered pair twice is one search
//     counted twice, which weights that pair by chance rather than by design.
//     ⚠️ (s, t) and (t, s) are DIFFERENT pairs and both may appear: greedy
//     routing is not symmetric — the star fixture Э3 has 1 → 2 as a dead end and
//     2 → 1 as a dead end for two different reasons, and other graphs succeed
//     one way and not the other.
//  4. THE DRAW DOES NOT LOOK AT THE GRAPH'S EDGES — only at identifiers and
//     roles, which are a function of (shape, seed) alone. That is what makes one
//     sample usable across policies: the candidate and its base see the SAME
//     pairs, so a difference in the result is a difference in the routing and
//     not in what was asked.
//
// ⚠️ And one rule about what the sample must NOT do: it is never filtered by the
// M1 result. Dropping the quotas or the seeds where the structural subgraph came
// apart would measure path length on the graphs that already worked — the
// selection would carry the answer. If the scope of the runs is to change, that
// is a decision, taken in the open, not a filter inside a sampler.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// m2PairSample is a drawn sample together with everything needed to draw it
// again and to read what it is.
type m2PairSample struct {
	// Requested is the size asked for, Pairs what was obtained. They differ only
	// when the eligible population could not supply the rest, and then Short
	// says so — an undersized sample is a finding, not a detail.
	Requested int
	Seed      uint64
	Pairs     [][2]int32

	// Eligible is how many nodes both ends could be drawn from.
	Eligible int
	// Draws is how many candidate pairs were examined, SelfPairs and Repeats how
	// many were rejected by rules 2 and 3. Printed because they are the evidence
	// that the two rules ran at all.
	Draws     int
	SelfPairs int
	Repeats   int
	Short     bool
}

func (s m2PairSample) String() string {
	if len(s.Pairs) == 0 {
		return fmt.Sprintf("M2 pair sample, seed %d: EMPTY — %d eligible nodes (both ends must be "+
			"Q), so no pair could be drawn; no data", s.Seed, s.Eligible)
	}
	line := fmt.Sprintf(
		"M2 pair sample: %d ordered pairs, both ends Q, drawn from %d eligible nodes with seed %d "+
			"(%d draws examined, %d self-pairs and %d repeats rejected; (s,t) and (t,s) are "+
			"different pairs)",
		len(s.Pairs), s.Eligible, s.Seed, s.Draws, s.SelfPairs, s.Repeats)
	if s.Short {
		line += fmt.Sprintf("\n  ⚠️ SHORT: %d pairs requested, %d obtained — the eligible "+
			"population has %d distinct ordered pairs in total",
			s.Requested, len(s.Pairs), s.Eligible*(s.Eligible-1))
	}
	return line
}

// m2Random is the draw. Its own domain separator, like every other reproducible
// source in the stand, so a pair index can never coincide with an identifier, a
// role, a shuffle or an M4 target.
func m2Random(seed uint64, index int) uint64 {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], seed)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(index))
	digest := sha256.Sum256(append([]byte("corsa/overlay/sim/m2/pairs/v1"), buf[:]...))
	return binary.LittleEndian.Uint64(digest[:8])
}

// drawM2Pairs draws the sample of §5.3.
//
// ⚠️ It reads g.roles and nothing else about the graph. Passing it a graph built
// under a different policy must give the identical sample, and there is a test
// for that: a sampler that peeked at the adjacency would quietly make every
// policy answer a different question.
func drawM2Pairs(g *graph, want int, seed uint64) (m2PairSample, error) {
	if want < 0 {
		return m2PairSample{}, fmt.Errorf("a sample of %d pairs is not a sample", want)
	}

	eligible := make([]int32, 0, len(g.roles))
	for i := range g.roles {
		if g.roles[i] == roleStructural {
			eligible = append(eligible, int32(i))
		}
	}

	sample := m2PairSample{Requested: want, Seed: seed, Eligible: len(eligible)}
	if len(eligible) < 2 || want == 0 {
		sample.Short = want > 0
		return sample, nil
	}

	// The draw stops when the sample is full or when it has looked far enough
	// that the population is plainly exhausted. ⚠️ The cap is a TERMINATION
	// guard, not a quality one: without it a request for more pairs than exist
	// would spin forever, and with a silent one it would return fewer pairs and
	// look like a full sample.
	distinct := len(eligible) * (len(eligible) - 1)
	limit := 64*want + 1024
	if distinct < want {
		limit = 64*distinct + 1024
	}

	seen := make(map[[2]int32]struct{}, want)
	for len(sample.Pairs) < want && sample.Draws < limit {
		draw := m2Random(seed, sample.Draws)
		sample.Draws++

		source := eligible[draw%uint64(len(eligible))]
		target := eligible[(draw>>32)%uint64(len(eligible))]

		if source == target {
			sample.SelfPairs++
			continue
		}
		pair := [2]int32{source, target}
		if _, already := seen[pair]; already {
			sample.Repeats++
			continue
		}
		seen[pair] = struct{}{}
		sample.Pairs = append(sample.Pairs, pair)
	}

	sample.Short = len(sample.Pairs) < want
	return sample, nil
}
