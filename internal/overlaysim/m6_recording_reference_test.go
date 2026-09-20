package overlaysim

// m6_recording_reference_test.go are the references for the DIRECT recording
// of the candidate stream (decision 3.4, p. 6): the stream a run records as
// it goes, without the diagnostic offer trace, is the stream recordStream
// derives from that trace — entry for entry, fingerprint for fingerprint —
// and it costs what a recording costs, not what a trace costs.

import (
	"reflect"
	"runtime"
	"testing"
	"unsafe"
)

// TestM6ADirectRecordingEqualsOneDerivedFromTheTrace runs one configuration
// twice — once with the trace and once recording directly — and compares the
// recordings by fingerprint, which covers every field a replay reads.
//
// ⚠️ Mutation that must break it: the direct path dropping the handed records
// of an exchange (or writing the exchange's responder as a candidate), or
// writing the recording for the measured owners only.
func TestM6ADirectRecordingEqualsOneDerivedFromTheTrace(t *testing.T) {
	t.Parallel()

	base := m6PhasedBase()
	base.Branch = branchAPrime
	base.Phases = &m6PhasePlan{FillTicks: 12, IdleTicks: 4, RecoveryTicks: 8, CadenceTicks: 8}
	g := buildGraph(base.Shape, base.Seed, base.Quota, base.Policy)

	traced := base
	traced.TraceOffers = true
	fromTrace, err := recordStream(runM6ModelOn(t, g, traced, everybody))
	if err != nil {
		t.Fatalf("recording from the trace: %v", err)
	}

	direct := base
	direct.RecordStream = true
	report := runM6ModelOn(t, g, direct, everybody)
	if report.Recording == nil {
		t.Fatal("the run recorded nothing although it was asked to")
	}
	if report.Trace.Offers != nil {
		t.Fatalf("the direct recording kept %d offer-trace entries; it exists so that the trace need "+
			"not be kept", len(report.Trace.Offers))
	}
	if fromTrace.Entries() == 0 || fromTrace.Exchanges == 0 {
		t.Fatalf("the fixture produced %d entries from %d exchanges, so the equivalence is not exercised",
			fromTrace.Entries(), fromTrace.Exchanges)
	}
	if got, want := report.Recording.fingerprint(), fromTrace.fingerprint(); got != want {
		t.Fatalf("the direct recording (%d entries over %d owners) differs from the one derived from "+
			"the trace (%d entries over %d owners)", report.Recording.Entries(), len(report.Recording.ByOwner),
			fromTrace.Entries(), len(fromTrace.ByOwner))
	}
	t.Logf("%d entries over %d owners, %d exchanges: the same recording both ways",
		fromTrace.Entries(), len(fromTrace.ByOwner), fromTrace.Exchanges)

	t.Run("on the Q half the unmeasured owners are recorded too", func(t *testing.T) {
		t.Parallel()
		// ⚠️ The mutation "record the measured owners only" is invisible on the
		// whole-network membership, where everybody is measured; here half the
		// owners are not, and the replay has to serve them from the recording
		// like the full-graph replay does.
		structural := func(id nodeID) bool { return roleOf(id) == roleStructural }
		half := traced
		half.Membership = "Q half"
		fromTrace, err := recordStream(runM6ModelOn(t, g, half, structural))
		if err != nil {
			t.Fatalf("recording from the trace: %v", err)
		}
		half = direct
		half.Membership = "Q half"
		report := runM6ModelOn(t, g, half, structural)
		unmeasured := 0
		for owner := range report.Recording.ByOwner {
			if !structural(report.Recording.IDs[owner]) {
				unmeasured++
			}
		}
		if unmeasured == 0 {
			t.Fatal("no unmeasured owner has entries, so the fixture cannot tell a full recording from a measured-only one")
		}
		if got, want := report.Recording.fingerprint(), fromTrace.fingerprint(); got != want {
			t.Fatalf("on the Q half the direct recording (%d entries) differs from the traced one (%d)",
				report.Recording.Entries(), fromTrace.Entries())
		}
	})

	t.Run("a replay cannot record", func(t *testing.T) {
		t.Parallel()
		replay := base
		replay.Stream = fromTrace
		replay.RecordStream = true
		if _, err := newM6Network(g, replay, everybody); err == nil {
			t.Fatal("a replay was allowed to record its stream")
		}
	})
}

// TestM6TheCostOfARecordingOnTheContractPlan is a MEASUREMENT, not a unit
// test: the contract's plan on 1k×8, branch A′, recorded directly, with the
// heap in use at the end and the size of the recording — the numbers the
// estimate for 10k×8 is scaled from (decision 3.4, p. 6).
func TestM6TheCostOfARecordingOnTheContractPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("a measurement, not a unit test")
	}
	config := m6PhasedBase()
	config.Branch = branchAPrime
	config.Phases = func() *m6PhasePlan { p := m6ContractPhases(); return &p }()
	config.Cadence = 64
	config.ExchangeEvery = 64
	config.StaleTicks = 256
	config.ReturnShare = 0.5
	config.ReturnAfter = 32
	config.JoinMaxWait = 64
	config.RecordStream = true

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	report := runM6Model(t, config)
	runtime.GC()
	runtime.ReadMemStats(&after)

	entries := report.Recording.Entries()
	perEntry := int(unsafe.Sizeof(m6StreamEntry{}))
	t.Logf("contract plan on %s, branch A′: %d ticks played, %d stream entries over %d owners, "+
		"%d B per entry ⇒ %.1f MB of entries; heap in use after the run %.1f MB (before %.1f MB); "+
		"a trace entry is %d B plus its handed slice",
		config.Shape.name, len(report.OnlineByTick), entries, len(report.Recording.ByOwner), perEntry,
		float64(entries*perEntry)/1e6, float64(after.HeapInuse)/1e6, float64(before.HeapInuse)/1e6,
		int(unsafe.Sizeof(m6OfferEntry{})))
}

// TestM6TheStreamProjectionMatchesAnIndependentOne closes И-2.1 (core §5.2):
// the projection of an offer trace into a recorded stream, checked against
// an expectation written OUT BY HAND — entry by entry, tick by tick — and
// not computed by any predicate the projection itself uses. The equivalence
// of the direct recording and the trace-derived one
// (TestM6ADirectRecordingEqualsOneDerivedFromTheTrace) goes through the one
// writer `appendOffer`, so a shared error of both paths was invisible to it;
// this reference pins what that writer must produce, so the chain is
// independent expectation → recordStream ≡ direct recording.
//
// The rules pinned (§6.17.3): refresh, shelf and queue offers are the
// owner's memory and are NOT in the stream; an acquaintance or omniscient
// offer is ONE pool entry naming the candidate; an exchange or an addressed
// answer is one HANDED entry per record handed, in the order handed, and an
// empty answer contributes nothing; entries keep the trace order per owner
// with the tick they were made at; the mechanism cost is the run's counts;
// the diagnostic QuotaSkipped list never enters the stream.
//
// ⚠️ Mutations that must break it: refresh (the zero source) let through;
// only the first handed record written; the exchange's responder written as
// a candidate; a pool offer written per level; the tick taken from the
// previous entry.
func TestM6TheStreamProjectionMatchesAnIndependentOne(t *testing.T) {
	t.Parallel()

	const nodes = 12
	ids := make([]nodeID, nodes)
	members := make([]bool, nodes)
	for index := range ids {
		ids[index] = makeNodeID(7, index)
		members[index] = index%2 == 0 // some measured, some not: the stream carries both
	}
	entry := func(tick int, owner int32, source m6OfferSource, level int, peer int32, handed ...int32) m6OfferEntry {
		return m6OfferEntry{Tick: tick, Owner: owner, Source: source, Level: level, Peer: peer, Handed: handed}
	}
	trace := []m6OfferEntry{
		entry(0, 1, offerAcquaintance, 3, 5),
		entry(0, 1, offerRefresh, 2, 5),             // memory: not in the stream
		entry(0, 2, offerExchange, -1, 4),           // an EMPTY answer: nothing
		entry(1, 1, offerExchange, -1, 6, 7, 8),     // two handed entries, in order
		entry(1, 2, offerAcquaintance, -1, 3),       // branch B aims at no level; still a pool entry
		entry(2, 1, offerShelf, 3, 7),               // memory
		entry(2, 1, offerQueue, 3, 7),               // memory: the queue IS the handed entries already recorded
		entry(3, 1, offerAddressed, 4, 9, 10),       // one handed entry
		entry(3, 1, offerOmniscient, 4, 11),         // pool
		entry(3, 3, offerAcquaintance, 0, 1),        // a third owner, one entry
		entry(5, 2, offerAddressed, 2, 3, 5, 9, 11), // three handed entries at a later tick
		{Tick: 6, Owner: 2, Source: offerAddressed, Level: 2, Peer: 3, Handed: []int32{1}, QuotaSkipped: []int32{4}},
	}
	report := &m6ModelReport{
		Config:           m6ModelConfig{Branch: branchC, TraceOffers: true},
		Trace:            &m6Trace{Offers: trace, IDs: ids, Members: members},
		ExchangesDone:    2,
		AddressedAnswers: 3,
	}
	got, err := recordStream(report)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}

	// The expectation, by hand.
	pool := func(tick int32, source m6OfferSource, candidate int32) m6StreamEntry {
		return m6StreamEntry{Tick: tick, Source: source, Candidate: candidate}
	}
	want := map[int32][]m6StreamEntry{
		1: {
			pool(0, offerAcquaintance, 5),
			{Tick: 1, Source: offerExchange, Candidate: 7},
			{Tick: 1, Source: offerExchange, Candidate: 8},
			{Tick: 3, Source: offerAddressed, Candidate: 10},
			pool(3, offerOmniscient, 11),
		},
		2: {
			pool(1, offerAcquaintance, 3),
			{Tick: 5, Source: offerAddressed, Candidate: 5},
			{Tick: 5, Source: offerAddressed, Candidate: 9},
			{Tick: 5, Source: offerAddressed, Candidate: 11},
			{Tick: 6, Source: offerAddressed, Candidate: 1},
		},
		3: {pool(3, offerAcquaintance, 1)},
	}
	if len(got.ByOwner) != len(want) {
		t.Fatalf("the stream names %d owners, want %d (owner 2's empty answer at tick 0 must not create "+
			"an owner-less entry, and no owner is invented)", len(got.ByOwner), len(want))
	}
	for owner, entries := range want {
		if !reflect.DeepEqual(got.ByOwner[owner], entries) {
			t.Errorf("owner %d:\n got  %v\n want %v", owner, got.ByOwner[owner], entries)
		}
	}
	if got.Exchanges != 2 || got.Answers != 3 || got.Branch != branchC || got.Omniscient {
		t.Errorf("the recording's provenance is %s (%d exchanges, %d answers, omniscient=%v)", got.Branch,
			got.Exchanges, got.Answers, got.Omniscient)
	}
	if !reflect.DeepEqual(got.IDs, ids) || !reflect.DeepEqual(got.Members, members) {
		t.Error("the recording does not pin the run's identifiers and membership marks")
	}
	if err := got.validateEntries(); err != nil {
		t.Errorf("the projection produced a recording its own door refuses: %v", err)
	}
}
