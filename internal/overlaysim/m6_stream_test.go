package overlaysim

// m6_stream_test.go is the RECORDED candidate stream: the paired control that
// makes "the same candidate stream in both modes" a property that holds by
// construction rather than a claim to be checked and found wanting.
//
// The adaptive branches cannot deliver it. Their source is either the owner's
// own held edges (A, B), which detection releases at moments that depend on
// what the owner remembers, or another node's table (A′, C), which the ‘from
// scratch’ clearing empties. The exposure is therefore not the same in the two
// modes after the churn onset, and §6.16 of the contract shows the
// counterexample.
//
// The recorded stream cuts that dependency: an ADAPTIVE run of a branch is
// recorded — every candidate its source produced for every node, with the tick
// it became available — and the recording is then REPLAYED as the only source
// in two runs, one keeping memory and one cleared at the onset. Both consume
// the same entries under the same rules, and the difference between them is
// the owner's memory and nothing upstream of it.
//
// ⚠️ THE REPLAYED RUNS ARE A CONTROL OF THEIR OWN, not a measurement of the
// recorded branch. The recording was produced by a node that had memory; a
// cleared node replaying it gets candidates it could not have obtained by the
// branch's mechanism (it does not ask its neighbours, they do not answer from
// emptied tables), and a remembering node replaying it does not perform the
// mechanism either. The pair answers "what does memory buy when the stream is
// held fixed", which is the question П-6 asks; it does not answer "what does
// branch X cost", which the adaptive run answers. The report names the
// replayed run a control and names the branch it was recorded from.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// m6StreamEntry is one candidate the source produced for one owner.
type m6StreamEntry struct {
	// Tick is the first tick the entry is available to the owner. ⚠️ Kept
	// narrow on purpose: a recording of the contract's plan on 10k×8 runs to
	// millions of entries, and every byte per entry is megabytes per run.
	Tick int32
	// Source says which rule the entry follows under replay: a POOL entry
	// (acquaintance, omniscient) is offered whenever the owner does not hold
	// the candidate and is never used up; a HANDED entry (exchange, addressed)
	// is a record another node sent, offered once and consumed on a final
	// outcome — filled, already known, or unusable at any level — exactly like
	// the queue of §5.1.0, a repeat costing its probe.
	Candidate int32
	Source    m6OfferSource
}

// poolSemantics says whether an entry is a standing pool member (re-offered
// whenever it fits) or a handed record (consumed on a final outcome).
func (s m6OfferSource) poolSemantics() bool {
	return s == offerAcquaintance || s == offerOmniscient
}

// streamed says whether an offer of this source is part of the EXTERNAL
// candidate stream. Refresh and shelf are the owner's memory and stay out.
func (s m6OfferSource) streamed() bool {
	switch s {
	case offerAcquaintance, offerOmniscient, offerExchange, offerAddressed:
		return true
	default:
		return false
	}
}

// m6RecordedStream is a recording of one adaptive run's candidate stream.
type m6RecordedStream struct {
	// Branch and Omniscient name the source the stream was recorded from; a
	// replay under any other source is refused.
	Branch     m6Branch
	Omniscient bool
	// IDs pins the meaning of every node index the entries use: the recording
	// run's identifiers, the built population followed by the newcomer
	// reserve, in index order.
	//
	// ⚠️ An index is only a name inside ONE population. The same index in a
	// graph of another seed, another shape or another reserve is another
	// participant, and on a smaller network it is out of range. A replay
	// therefore checks that its own identifiers are exactly these, index by
	// index, before it reads a single entry.
	IDs []nodeID
	// Members is, index by index, whether the recording run measured that
	// identifier. ⚠️ The population under measurement is part of what the
	// stream means: a full-graph recording offers a Q owner ¬Q candidates it
	// could never take in a Q-half run, so a replay under another membership
	// is refused, and the hand-out applies mayTake besides.
	Members []bool
	// ByOwner holds every owner's entries in the order they were produced,
	// ticks non-decreasing. Owners are the whole physical population that was
	// served, newcomers included, keyed by node index — see IDs.
	ByOwner map[int32][]m6StreamEntry
	// Exchanges and Answers count the mechanism events the recording run
	// performed to produce the handed entries. ⚠️ A replay does not perform
	// them and does not count them again: they are the recording's cost, and
	// the replay's report says so.
	Exchanges, Answers int
}

// recordStream derives the stream from a run's offer trace. The run has to
// have been an ADAPTIVE one with the trace on: a replay of a replay would
// record the replay's consumption, not a source.
func recordStream(report *m6ModelReport) (*m6RecordedStream, error) {
	if !report.Config.TraceOffers {
		return nil, fmt.Errorf("the run kept no offer trace, so there is no stream to record")
	}
	if report.Config.Stream != nil {
		return nil, fmt.Errorf("the run replayed a recorded stream; recording it again would record " +
			"consumption, not a source")
	}
	if report.Trace.IDs == nil {
		return nil, fmt.Errorf("the run's trace carries no identifiers, so its indices cannot be pinned")
	}
	stream := &m6RecordedStream{
		Branch:     report.Config.Branch,
		Omniscient: report.Config.OmniscientControl,
		IDs:        append([]nodeID(nil), report.Trace.IDs...),
		Members:    append([]bool(nil), report.Trace.Members...),
		ByOwner:    map[int32][]m6StreamEntry{},
		Exchanges:  report.ExchangesDone,
		Answers:    report.AddressedAnswers,
	}
	for _, offer := range report.Trace.Offers {
		if !offer.Source.streamed() {
			continue
		}
		if offer.Source.poolSemantics() {
			stream.ByOwner[offer.Owner] = append(stream.ByOwner[offer.Owner],
				m6StreamEntry{Tick: int32(offer.Tick), Source: offer.Source, Candidate: offer.Peer})
			continue
		}
		for _, handed := range offer.Handed {
			stream.ByOwner[offer.Owner] = append(stream.ByOwner[offer.Owner],
				m6StreamEntry{Tick: int32(offer.Tick), Source: offer.Source, Candidate: handed})
		}
	}
	return stream, nil
}

// compatibleWith says whether the replaying network's identifiers are exactly
// the ones the recording was made over, index by index — the population AND
// the reserve, because newcomers are indices too.
func (s *m6RecordedStream) compatibleWith(ids []nodeID, member func(nodeID) bool) error {
	if len(ids) != len(s.IDs) || len(s.Members) != len(s.IDs) {
		return fmt.Errorf("the recording names %d identifiers (population and reserve) with %d "+
			"membership marks, this run has %d: the same index would mean a different participant, "+
			"or none", len(s.IDs), len(s.Members), len(ids))
	}
	for index := range ids {
		if ids[index] != s.IDs[index] {
			return fmt.Errorf("index %d is a different identifier in the recording and in this run: "+
				"the recording was made over another population (seed, shape or reserve)", index)
		}
		if member(ids[index]) != s.Members[index] {
			return fmt.Errorf("index %d is measured in one of the recording and this run and not in "+
				"the other: the recording was made under another membership, and its candidates "+
				"are not what this population may take", index)
		}
	}
	return s.validateEntries()
}

// validateEntries is the door for what the entries themselves say. recordStream
// only ever writes the four EXTERNAL sources, but a recording built by hand
// can carry anything: refresh — the zero value, the easiest to leave in by
// accident — shelf, queue, or a number that names no source. The replay used
// to accept such an entry and hand its candidate out as a queue record, so the
// boundary recordStream keeps was not kept by the replay.
//
// The same door checks the shape recordStream guarantees and a hand-built
// recording may not: every owner and every candidate is an index into IDs (an
// index beyond them panicked in the hand-out), no tick is negative, and the
// ticks of one owner never DECREASE — the replay stops at the first entry
// not yet available, so an earlier tick behind a later one was handed out
// late rather than never. Equal ticks are in order.
func (s *m6RecordedStream) validateEntries() error {
	for owner, entries := range s.ByOwner {
		if owner < 0 || int(owner) >= len(s.IDs) {
			return fmt.Errorf("the recording names owner index %d, beyond its %d identifiers", owner,
				len(s.IDs))
		}
		previous := int32(0)
		for index, entry := range entries {
			if !entry.Source.streamed() {
				return fmt.Errorf("entry %d of owner %d names the source %q (%d), which is not part of "+
					"an external candidate stream: a recording carries acquaintance, omniscient, "+
					"exchange and addressed entries only", index, owner, entry.Source, entry.Source)
			}
			if entry.Candidate < 0 || int(entry.Candidate) >= len(s.IDs) {
				return fmt.Errorf("entry %d of owner %d names candidate index %d, beyond the recording's "+
					"%d identifiers", index, owner, entry.Candidate, len(s.IDs))
			}
			if entry.Tick < 0 {
				return fmt.Errorf("entry %d of owner %d has a negative tick %d", index, owner, entry.Tick)
			}
			if entry.Tick < previous {
				return fmt.Errorf("entry %d of owner %d is available at tick %d, after entry %d at tick %d: "+
					"the ticks of one owner must not decrease, or the earlier entry is handed out late",
					index, owner, entry.Tick, index-1, previous)
			}
			previous = entry.Tick
		}
	}
	return nil
}

// Entries is the total number of entries over every owner.
func (s *m6RecordedStream) Entries() int {
	total := 0
	for _, entries := range s.ByOwner {
		total += len(entries)
	}
	return total
}

// fingerprint digests the whole stream so a comparison can assert that neither
// run modified what both were supposed to read — EVERY field a replay or the
// report reads: the source it was recorded from (Branch, Omniscient), the
// cost the recording run paid (Exchanges, Answers), the identifiers, the
// membership marks and every entry. Two recordings that differ in any one of
// these are two recordings, whatever their entries say.
func (s *m6RecordedStream) fingerprint() string {
	owners := make([]int32, 0, len(s.ByOwner))
	for owner := range s.ByOwner {
		owners = append(owners, owner)
	}
	sort.Slice(owners, func(a, b int) bool { return owners[a] < owners[b] })

	digest := sha256.New()
	var buf [16]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(s.Branch))
	buf[4] = 0
	if s.Omniscient {
		buf[4] = 1
	}
	digest.Write(buf[0:5])
	binary.LittleEndian.PutUint64(buf[0:8], uint64(s.Exchanges))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(s.Answers))
	digest.Write(buf[:])
	for index, id := range s.IDs {
		digest.Write(id[:])
		if s.Members[index] {
			digest.Write([]byte{1})
		} else {
			digest.Write([]byte{0})
		}
	}
	for _, owner := range owners {
		binary.LittleEndian.PutUint32(buf[0:4], uint32(owner))
		digest.Write(buf[0:4])
		for _, entry := range s.ByOwner[owner] {
			binary.LittleEndian.PutUint64(buf[0:8], uint64(entry.Tick))
			binary.LittleEndian.PutUint32(buf[8:12], uint32(entry.Source))
			binary.LittleEndian.PutUint32(buf[12:16], uint32(entry.Candidate))
			digest.Write(buf[:])
		}
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

func (s *m6RecordedStream) String() string {
	source := s.Branch.String()
	if s.Omniscient {
		source = "the omniscient control"
	}
	return fmt.Sprintf("RECORDED STREAM of %s: %d entries over %d owners, produced by %d exchanges "+
		"and %d addressed answers of the recording run (not re-performed here)",
		source, s.Entries(), len(s.ByOwner), s.Exchanges, s.Answers)
}

// --- replay ---------------------------------------------------------------------------

// fromRecordedStream is the replay source: the next entry of the owner's
// recording that is available, unconsumed, allowed by the owner's memory, and
// on the level asked for.
//
// ⚠️ THE CONSUMPTION STATE LIVES ON THE NETWORK, NOT ON THE NODE, and the
// clearing does not touch it: a consumed entry was consumed by a probe that
// happened, which is a fact about the world. The memory filters — "do I hold
// this", "did I try it this tick", "can any bucket ever hold it" — are the
// same ones the adaptive branches apply, and the shelf and the refresh run
// natively beside the stream, because they ARE the memory under measurement.
func (n *m6Network) fromRecordedStream(owner int32, state *m6NodeState, level int) (int32, m6OfferSource, bool) {
	entries := n.config.Stream.ByOwner[owner]
	consumed := n.consumed[owner]
	for index, entry := range entries {
		if int(entry.Tick) > n.tick {
			// Ticks are non-decreasing in a recording, so nothing beyond this
			// point is available yet.
			break
		}
		if _, done := consumed[index]; done || n.spent(state, entry.Candidate) {
			continue
		}
		// ⚠️ The measurement filter applies to a recording exactly as to a
		// branch: a measured owner may only take members. The compatibility
		// check refuses a recording made under another membership, and this
		// is the fail-closed half — an entry that names a non-member never
		// reaches a measured table, whatever the recording says.
		if !n.mayTake(owner, entry.Candidate) {
			continue
		}
		if entry.Source.poolSemantics() && state.Table.holds(entry.Candidate) {
			// A pool member already held is not re-offered — that is a refresh,
			// scheduled by the cadence (the same rule as fromAcquaintances).
			continue
		}
		if level >= 0 && levelOf(n.ids[owner], n.ids[entry.Candidate], n.config.Shape.degree) != level {
			continue
		}
		n.streamCursor[owner] = index
		// ⚠️ AN OFFER IS AN OFFER, whoever makes it: the measured S(u) counts
		// what a source actually handed the owner, and a replay is a source.
		// Without this a candidate handed from the recording and refused at
		// its ceiling vanished from the pool — the exchange, the addressed
		// answer and the omniscient control all book their hand-outs here.
		n.noteReachable(state, entry.Candidate)
		// A handed entry is offered AS A QUEUE RECORD: the exchange that
		// produced it happened in the recording run, not here, and the offer
		// trace must not read as if a responder had been asked.
		source := entry.Source
		if !entry.Source.poolSemantics() {
			source = offerQueue
		}
		return entry.Candidate, source, true
	}
	return -1, offerQueue, false
}

// consumeStreamEntry marks a handed entry used up after a FINAL outcome. A
// pool entry is never consumed. The same final outcomes drop a record from the
// §5.1.0 queue: stored, already known, or unusable at any level.
func (n *m6Network) consumeStreamEntry(
	owner int32, state *m6NodeState, candidate int32, outcome m6Outcome,
) {
	index, offered := n.streamCursor[owner]
	if !offered {
		return
	}
	delete(n.streamCursor, owner)
	entry := n.config.Stream.ByOwner[owner][index]
	if entry.Source.poolSemantics() || entry.Candidate != candidate {
		// A pool entry is never used up; a shelf candidate probed instead of
		// the entry (shelf-first order) leaves the entry untouched.
		return
	}
	_, permanent := state.Exhausted[candidate]
	if outcome == m6SlotFilled || outcome == m6AlreadyKnown || permanent {
		if n.consumed[owner] == nil {
			n.consumed[owner] = map[int]struct{}{}
		}
		n.consumed[owner][index] = struct{}{}
	}
}

// streamExhaustion counts, over the measured owners that TOOK PART, how many
// have no unconsumed entry left at all — the recording ran out for them —
// beside how many took part, and how many entries were consumed in total.
// ⚠️ A replay that ran out is reported as such: an owner with nothing left
// to be offered is not an owner that finished filling. And the participants
// are the denominator of the share, not the measured reserve as a whole —
// the reserve that never joined is reported apart.
func (n *m6Network) streamExhaustion() (exhaustedOwners, participants, consumedEntries int) {
	for _, owner := range n.owners {
		if n.states[owner] == nil {
			// Never joined: not a participant, nothing to be exhausted.
			continue
		}
		participants++
		// ⚠️ A MISSING KEY IS AN EMPTY STREAM. The recording run simply never
		// produced a candidate for this owner, which is the same finding as an
		// explicitly empty list; skipping such owners made a recording with no
		// candidates for anybody read as "0 exhausted".
		entries := n.config.Stream.ByOwner[owner]
		state := n.states[owner]
		left := 0
		for index, entry := range entries {
			if _, done := n.consumed[owner][index]; done {
				consumedEntries++
				continue
			}
			// ⚠️ THE SAME PERMANENT EXCLUSIONS THE HAND-OUT APPLIES, and only
			// those. A candidate no bucket can ever hold (Exhausted) or one the
			// measurement never lets this owner take (mayTake) is never offered
			// again, so it is not "left" — counting it kept an owner with nothing
			// offerable from ever reading as exhausted. TriedThisTick is NOT in
			// this list: that refusal is temporary, and the entry is offered again
			// next tick.
			if _, never := state.Exhausted[entry.Candidate]; never || !n.mayTake(owner, entry.Candidate) {
				continue
			}
			if entry.Source.poolSemantics() && state.Table.holds(entry.Candidate) {
				continue
			}
			left++
		}
		if left == 0 {
			exhaustedOwners++
		}
	}
	return exhaustedOwners, participants, consumedEntries
}

// availableFromStream is the replay's exposure: every entry available and
// unconsumed at this tick that the MEASUREMENT lets the owner take, in order.
//
// ⚠️ mayTake is part of the source, not of the memory: the hand-out and the
// exhaustion count apply it, so an entry naming a ¬Q candidate is not
// something the recording can offer a Q owner, and it is not in the exposure
// either. The memory filters — held, tried this tick, exhausted — stay out:
// the exposure is the source BEFORE memory filters it.
func (n *m6Network) availableFromStream(owner int32) []int32 {
	available := make([]int32, 0, 16)
	for index, entry := range n.config.Stream.ByOwner[owner] {
		if int(entry.Tick) > n.tick {
			break
		}
		if _, done := n.consumed[owner][index]; done {
			continue
		}
		if !n.mayTake(owner, entry.Candidate) {
			continue
		}
		available = append(available, entry.Candidate)
	}
	return available
}
