package datagram

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline_response_test.go covers the response fork of §4.1 and the claim
// discipline of §4.2 as the conveyor drives it.

// responseFixture is origin → relay → target, with the relay already holding a
// pending reverse record for `label`.
type responseFixture struct {
	net    *fakeNetwork
	origin *pipelineNode
	relay  *pipelineNode
	target *pipelineNode
	label  Label
}

func newResponseFixture(t *testing.T, label Label) *responseFixture {
	t.Helper()
	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	fixture := &responseFixture{net: net, origin: nodes[0], relay: nodes[1], target: nodes[2], label: label}

	// A request that never gets answered by the target itself: the target has no
	// handler, so nothing comes back on its own and the test drives the answers.
	requireOutcome(t, fixture.relay.deliver(t, fixture.origin.id, requestFrame(t, requestOpts{
		label: label, dst: fixture.target.id,
	})), InboundForwarded)
	return fixture
}

func (f *responseFixture) answer(t *testing.T, dtype domain.DType) InboundResult {
	t.Helper()
	return f.relay.deliver(t, f.target.id, responseFrame(t, responseOpts{
		label: f.label, subject: f.target.id, dtype: dtype,
	}))
}

// TestResponseIsDeliveredWithoutAnyRouteToItsDst is the first §9 row of the
// mode fork: a response travels on stored state alone, so the absence of a
// route to its dst is irrelevant — it is not even a destination.
func TestResponseIsDeliveredWithoutAnyRouteToItsDst(t *testing.T) {
	label := newLabel(t, "no-route")
	fixture := newResponseFixture(t, label)
	// Wipe every route: nothing about the answer may depend on them.
	fixture.relay.routes.set(fixture.target.id)
	fixture.relay.routes.set(domaintest.ID("anything"))

	requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundForwarded)
	journal := fixture.net.journal()
	last := journal[len(journal)-1]
	if last.to != fixture.origin.id {
		t.Fatalf("the answer went to %v, want the stored upstream %v", last.to, fixture.origin.id)
	}
	if fixture.relay.reverse.Len() != 0 {
		t.Fatal("a successfully enqueued answer frees its record")
	}
}

// TestResponseReadOnlyInvariants walks the §4.2 checks that run BEFORE the
// claim. Every one of them leaves the record pending, so the genuine answer
// can still arrive — which the last step of the test proves.
func TestResponseReadOnlyInvariants(t *testing.T) {
	t.Run("from the wrong neighbour", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "wrong-peer"))
		stranger := newPipelineNode(t, fixture.net, nodeOpts{name: "stranger", transit: true})
		link(fixture.relay, stranger, true, true)

		result := fixture.relay.deliver(t, stranger.id, responseFrame(t, responseOpts{
			label: fixture.label, subject: fixture.target.id,
		}))
		requireDrop(t, result, DropReverseWrongDownstream)
		requirePending(t, fixture)
	})

	t.Run("with a mismatched subject", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "wrong-src"))
		result := fixture.relay.deliver(t, fixture.target.id, responseFrame(t, responseOpts{
			label: fixture.label, subject: domaintest.ID("somebody-else"),
		}))
		requireDrop(t, result, DropReverseSubjectMismatch)
		requirePending(t, fixture)
	})

	t.Run("with an unknown label", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "unknown"))
		result := fixture.relay.deliver(t, fixture.target.id, responseFrame(t, responseOpts{
			label: newLabel(t, "never-seen"), subject: fixture.target.id,
		}))
		requireDrop(t, result, DropReverseUnknownLabel)
		requirePending(t, fixture)
	})

	t.Run("after the window expired", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "expired"))
		fixture.relay.advance(ReverseStateWindow() + 1)
		result := fixture.relay.deliver(t, fixture.target.id, responseFrame(t, responseOpts{
			label: fixture.label, subject: fixture.target.id,
		}))
		requireDrop(t, result, DropReverseUnknownLabel)
	})

	t.Run("a second valid answer", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "second"))
		requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundForwarded)
		// The record was freed by the successful enqueue, so the second answer
		// is unaddressed rather than merely late.
		requireDrop(t, fixture.answer(t, dtypeAnswer), DropReverseUnknownLabel)
	})
}

// TestAnswerOnASecondChannelOfTheDownstreamNameIsRefused is the round's P1.
//
// The relay forwarded the request over ONE channel to the target. A DIFFERENT
// channel then presents the target's name and answers. Nothing about that name
// is proven — it is a session this node dialled, where the welcome's address is
// whatever the remote wrote into it — so the only thing separating the impostor
// from the exchange is which socket the question left over.
//
// The fixture deliberately holds TWO channels carrying ONE name: with a single
// channel per name the assertion would pass just as well against the old
// identity comparison, and would prove nothing about which of the two facts the
// record is defended by.
//
// The refusal must also leave the slot PENDING. The record has exactly one
// answer slot, and an impostor that got the frame dropped but the slot claimed
// would kill the exchange just as effectively as one that stole the answer — the
// positive control at the end is what proves it did not.
func TestAnswerOnASecondChannelOfTheDownstreamNameIsRefused(t *testing.T) {
	label := newLabel(t, "borrowed-downstream")
	fixture := newResponseFixture(t, label)

	// The forward itself is PINNED, which is what makes the record's channel a
	// fact about this node's own socket rather than a guess: the emitter may not
	// carry the frame anywhere else, so "the channel the request left over" and
	// "the channel stored in the record" are the same value by construction.
	forward := fixture.net.journal()[0]
	if forward.to != fixture.target.id || forward.channel != testChannel(fixture.target.id.String()) {
		t.Fatalf("the forwarded request was not pinned to the candidate's channel: %+v", forward)
	}

	borrowed := ingressOpts{
		peer:      fixture.target.id,
		channel:   testChannel("second-session-of-" + fixture.target.id.String()),
		authority: AuthorityClaimed,
	}
	requireDrop(t, fixture.relay.deliverOn(t, borrowed, responseFrame(t, responseOpts{
		label: label, subject: fixture.target.id,
	})), DropReverseWrongDownstream)
	requirePending(t, fixture)

	// The channel the request really left over still answers, so the refusal
	// above is about the CHANNEL and not about the answer, the type or the name.
	requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundForwarded)
}

func requirePending(t *testing.T, fixture *responseFixture) {
	t.Helper()
	record, live := fixture.relay.reverse.Lookup(fixture.label)
	if !live {
		t.Fatal("a refusal before the claim must leave the record alive")
	}
	if record.State() != ReverseSlotPending {
		t.Fatalf("record is %s, want pending — the genuine answer can still arrive", record.State())
	}
}

// TestTransitForwardsAnAnswerWhateverItsOwnRegistryPairs is the round's P1: a
// relay does not read the type registry on the forwarding branch AT ALL.
//
// The pairing rule belongs to the node whose exchange the slot is: it is the
// asker that must not have somebody else's protocol take its single answer
// slot. A relay applying it instead judged a foreign exchange by ITS OWN
// registration of the answer type — so a relay carrying an older registration
// of a response dtype dropped the correct answer of a NEW endpoint protocol,
// which is exactly the "upgrade the transit before the protocol can move"
// coupling a stable envelope exists to remove.
//
// The fixture makes the relay maximally opinionated: it knows the answer type
// AND has it paired with an unrelated request. The frame must still travel.
func TestTransitForwardsAnAnswerWhateverItsOwnRegistryPairs(t *testing.T) {
	fixture := newResponseFixture(t, newLabel(t, "pairing"))
	registerType(t, fixture.relay, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))
	registerType(t, fixture.relay, responseType(dtypeCached, dtypeUnrelated, acceptingHandler()))

	requireOutcome(t, fixture.answer(t, dtypeCached), InboundForwarded)
	journal := fixture.net.journal()
	last := journal[len(journal)-1]
	if last.to != fixture.origin.id {
		t.Fatalf("the answer went to %v, want the stored upstream %v", last.to, fixture.origin.id)
	}
}

// TestUnknownAnswerTypeIsForwardedWithoutPairing is the same statement from the
// other side: a relay that knows nothing forwards, exactly as one that knows
// everything does. The two together say the registry is not an input of the
// forwarding decision.
func TestUnknownAnswerTypeIsForwardedWithoutPairing(t *testing.T) {
	fixture := newResponseFixture(t, newLabel(t, "unknown-answer"))
	// The relay registers nothing at all.
	requireOutcome(t, fixture.answer(t, dtypeCached), InboundForwarded)
}

// TestProbeBudgetLimitsExpensiveValidations is §4.2 and §9: the reservation is
// what stands between a forged answer and the expensive part of the fork, and
// it has to hold under the only condition that produces forged answers in the
// first place — several of them at once.
//
// The refusal used to spend a probe is the forwarding ttl, which is the first
// step past the reservation. It used to be an interceptor, and that was a
// transit node reading somebody else's answer to decide whether it travels on.
//
// The refund side of the rule — only REFUSED attempts pay — is not here: it is
// observable only on a record that survives its claim, which is what
// TestSuccessfulForwardDoesNotSpendProbeBudget builds.
func TestProbeBudgetLimitsExpensiveValidations(t *testing.T) {
	// ttl = 1 passes the raw `ttl > 0` gate of the common part and dies on the
	// forwarding decrement, which stands AFTER the probe reservation.
	spendable := func(t *testing.T, fixture *responseFixture) protocol.DatagramFrame {
		t.Helper()
		return responseFrame(t, responseOpts{
			label: fixture.label, subject: fixture.target.id, dtype: dtypeAnswer, ttl: 1,
		})
	}

	t.Run("one at a time", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "probes"))
		budget := fixture.relay.reverse.ProbeBudget()
		for i := 0; i < budget+3; i++ {
			result := fixture.relay.deliver(t, fixture.target.id, spendable(t, fixture))
			if i < budget {
				requireDrop(t, result, DropTTLExhausted)
				continue
			}
			requireDrop(t, result, DropReverseProbeExhausted)
		}
		// Exhaustion does not free the slot: the record stays pending until
		// expires_at, only the expensive work stops being paid for.
		requirePending(t, fixture)
	})

	t.Run("all at once", func(t *testing.T) {
		// The sequential loop above is green against a budget that is merely
		// counted, and a counter is not what §4.2 asks for: without atomicity
		// several forged answers each see a free budget and ALL of them reach
		// the expensive check, so the limit would protect exactly the case that
		// was already safe. Only a race can tell the two apart.
		fixture := newResponseFixture(t, newLabel(t, "probes-parallel"))
		budget := fixture.relay.reverse.ProbeBudget()
		const racers = 64

		frame := spendable(t, fixture)
		arrival := ingressOpts{
			peer:      fixture.target.id,
			channel:   testChannel(fixture.target.id.String()),
			authority: AuthorityProven,
		}
		results := make([]InboundResult, racers)
		var released sync.WaitGroup
		var finished sync.WaitGroup
		released.Add(1)
		for i := 0; i < racers; i++ {
			finished.Add(1)
			go func(slot int) {
				defer finished.Done()
				released.Wait()
				results[slot] = fixture.relay.deliverOn(t, arrival, frame)
			}(i)
		}
		released.Done()
		finished.Wait()

		granted := 0
		for i, result := range results {
			switch result.Reason() {
			case DropTTLExhausted:
				granted++
			case DropReverseProbeExhausted:
			default:
				t.Fatalf("racer %d ended as %s/%s, want either the granted probe's ttl refusal "+
					"or an exhausted budget", i, result.Outcome(), result.Reason())
			}
		}
		if granted != budget {
			t.Fatalf("%d of %d parallel answers got past the reservation, want exactly the budget %d",
				granted, racers, budget)
		}
		requirePending(t, fixture)
	})

	t.Run("a read-only refusal takes no probe", func(t *testing.T) {
		// The reservation stands AFTER the read-only invariants of step 4, and
		// that order is what keeps the budget meaningful: the subject check
		// reads nothing but the arriving frame and the stored record, so an
		// answer that fails it has cost this node nothing and must take
		// nothing. Reserving first would let the cheapest forgery there is
		// drain the budget that bounds the expensive path.
		fixture := newResponseFixture(t, newLabel(t, "probes-read-only"))
		requireDrop(t, fixture.relay.deliver(t, fixture.target.id, responseFrame(t, responseOpts{
			label: fixture.label, subject: domaintest.ID("somebody-else"), ttl: 1,
		})), DropReverseSubjectMismatch)

		record, live := fixture.relay.reverse.Lookup(fixture.label)
		if !live {
			t.Fatal("a refusal before the claim must leave the record alive")
		}
		if left, budget := record.ProbesLeft(), fixture.relay.reverse.ProbeBudget(); left != budget {
			t.Fatalf("probes left = %d after a read-only refusal, want the untouched %d", left, budget)
		}
	})
}

// TestEveryProbeRefusalIsReportedAsItself is the pipeline half of the
// stale-record guard: the reservation of step 5 now has TWO ways to refuse, and
// the fork that reads them must not report both as one.
//
// They are facts about DIFFERENT records. "No budget left" names the record the
// answer belongs to and is the number an operator sizes the probe limit by;
// "the record is stale" names an exchange that has already ended and that spent
// nothing at all. A stale answer counted as an exhausted budget shows a live
// exchange burning probes nobody took from it — the symptom the guard exists to
// remove, reintroduced in the metric.
//
// The completeness half is checked AGAINST the enum rather than maintained
// beside it: a verdict added later is either classified here or a named
// failure.
func TestEveryProbeRefusalIsReportedAsItself(t *testing.T) {
	if reason := probeRefusalReason(ReverseProbeExhausted); reason != DropReverseProbeExhausted {
		t.Fatalf("an exhausted budget is reported as %s", reason)
	}
	stale := probeRefusalReason(ReverseProbeStale)
	if stale == DropReverseProbeExhausted {
		t.Fatal("a record that vanished under the answer is counted as an exhausted budget: " +
			"the number names a record that spent nothing")
	}
	if stale != DropReverseRecordStale {
		t.Fatalf("a stale record is reported as %s", stale)
	}

	for outcome, name := range reverseProbeOutcomeNames {
		reason, classified := probeRefusalReasons[outcome]
		switch outcome {
		case ReverseProbeUnset, ReverseProbeGranted:
			if classified {
				t.Fatalf("%s is not a refusal, yet it carries the drop reason %s", name, reason)
			}
		default:
			if !classified {
				t.Fatalf("the refusal %s has no drop reason: it would be dropped as somebody else's", name)
			}
			// §10 counts "refused answers" as a listed SET, so a reason left
			// out of it is a refusal that disappears from that total.
			if !slices.Contains(refusedAnswerReasons[:], reason) {
				t.Fatalf("%s drops with %s, which is not counted as a refused answer", name, reason)
			}
		}
	}

	// The loop above walks the NAMES, so a verdict added to the enum without a
	// name would slip past both it and this file. The boundary is what catches
	// that: one past the last named outcome must still be unnamed.
	if name := ReverseProbeOutcome(uint8(ReverseProbeStale) + 1).String(); name != "invalid" {
		t.Fatalf("the outcome after the last named one renders as %q: a verdict was added to the "+
			"enum without a name, and the completeness walk above cannot see it", name)
	}
}

// TestTheForkReportsAStaleRecordAsItsOwnReason drives step 5 through the fork's
// own function rather than through the mapping alone.
//
// The mapping tested by itself is inert against the mistake that matters: a
// call site that drops with a constant keeps it green. And the interleaving the
// stale verdict answers to — an exchange ending between the lookup and the
// reservation — cannot be staged from outside the fork, because handleResponse
// makes no foreign call between the two. So the state is built here, in the
// table the pipeline actually reads.
func TestTheForkReportsAStaleRecordAsItsOwnReason(t *testing.T) {
	node := newPipelineNode(t, newFakeNetwork(), nodeOpts{name: "relay", transit: true})
	table := node.pipeline.reverse
	opts := ReverseReserveOpts{
		ReceivedAt: node.clock(),
		Label:      NewLabel(domaintest.ID("recycled-label")),
		Dst:        domaintest.ID("target"),
		DType:      dtypeQuery,
		Upstream:   testUpstream(domaintest.ID("upstream")),
	}
	slot, ok := table.Reserve(opts).Slot()
	if !ok {
		t.Fatal("the first request must take the label")
	}
	stale, live := table.Lookup(opts.Label)
	if !live {
		t.Fatal("the record of the first request must be live")
	}

	// The exchange ends and a FRESH one takes the same label — the label
	// belongs to whoever sent the request, not to the record.
	if !table.Rollback(slot) {
		t.Fatal("Rollback must free the record")
	}
	if _, ok := table.Reserve(opts).Slot(); !ok {
		t.Fatal("the fresh request must take the label")
	}
	fresh, live := table.Lookup(opts.Label)
	if !live {
		t.Fatal("the fresh record must be live")
	}

	if _, refusal := node.pipeline.reserveProbe(stale); refusal != DropReverseRecordStale {
		t.Fatalf("an answer to the ended exchange is dropped as %s, want reverse_record_stale", refusal)
	}
	if left := mustRecord(t, table, opts.Label).ProbesLeft(); left != table.ProbeBudget() {
		t.Fatalf("the fresh exchange has %d probes left, want the untouched %d", left, table.ProbeBudget())
	}

	// POSITIVE CONTROL, twice over: the fresh record's own answer is granted,
	// and once it has spent its own budget the fork reports THAT refusal — so
	// neither assertion above is satisfied by a fork that refuses everything or
	// by one that reports one reason for both.
	ticket, refusal := node.pipeline.reserveProbe(fresh)
	if refusal != DropReasonUnset || ticket.IsZero() {
		t.Fatalf("the fresh exchange's own answer was refused with %s", refusal)
	}
	table.RefundProbe(ticket)
	for attempt := 0; attempt < table.ProbeBudget(); attempt++ {
		if _, refusal := node.pipeline.reserveProbe(fresh); refusal != DropReasonUnset {
			t.Fatalf("probe %d of the fresh exchange was refused with %s", attempt, refusal)
		}
	}
	if _, refusal := node.pipeline.reserveProbe(fresh); refusal != DropReverseProbeExhausted {
		t.Fatalf("the exhausted record is dropped as %s, want reverse_probe_exhausted", refusal)
	}
}

// TestSuccessfulForwardDoesNotSpendProbeBudget is the other half of the rule:
// only REFUSED attempts pay (§4.2, §9 line 1086).
//
// The old version of this test was a tautology — it forwarded one answer, saw
// the record disappear and asserted that a FRESH record starts with a full
// budget — so deleting RefundProbe from both branches left the package green.
// The budget is only observable on a record that SURVIVES its claim, and on
// each branch there is exactly one such record: an enqueue the upstream's queue
// refused, and a resolver that refused the answer. Both claimed first, which is
// what the refund is a fact about, and both are held until expires_at by §4.2.
func TestSuccessfulForwardDoesNotSpendProbeBudget(t *testing.T) {
	t.Run("network upstream", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "refund"))
		// The claim SUCCEEDS — which is what refunds the probe — and only the
		// enqueue afterwards fails, leaving the record readable.
		fixture.net.refuseQueue(fixture.origin.id)

		requireDrop(t, fixture.answer(t, dtypeAnswer), DropAnswerNotDelivered)

		record, live := fixture.relay.reverse.Lookup(fixture.label)
		if !live {
			t.Fatal("the record survives a failed enqueue")
		}
		if record.State() != ReverseSlotClaimed {
			t.Fatalf("record is %s, want claimed", record.State())
		}
		if record.ProbesLeft() != DefaultReverseProbeBudget {
			t.Fatalf("probes left = %d after an accepted answer, want the full budget %d — "+
				"only REFUSED attempts pay", record.ProbesLeft(), DefaultReverseProbeBudget)
		}
	})

	t.Run("local upstream", func(t *testing.T) {
		fixture := newLocalResponseFixture(t, newLabel(t, "refund-local"))
		// The claim SUCCEEDS — which is what refunds the probe — and only the
		// resolver afterwards refuses, leaving the record readable. An
		// ACCEPTED answer would complete its record and leave nothing to read,
		// exactly as a successful forward does.
		registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, refusingHandler()))

		requireDrop(t, fixture.answer(t, dtypeAnswer), DropHandlerRejected)

		record, live := fixture.origin.reverse.Lookup(fixture.label)
		if !live {
			t.Fatal("an answer the resolver refused leaves its record claimed until expires_at")
		}
		if record.State() != ReverseSlotClaimed {
			t.Fatalf("record is %s, want claimed", record.State())
		}
		if record.ProbesLeft() != DefaultReverseProbeBudget {
			t.Fatalf("probes left = %d after a claimed answer, want the full budget %d — "+
				"only REFUSED attempts pay", record.ProbesLeft(), DefaultReverseProbeBudget)
		}
	})

	t.Run("a refused answer does pay", func(t *testing.T) {
		// The contrast that makes the two assertions above mean something: an
		// attempt a gate turns away keeps its unit spent.
		fixture := newLocalResponseFixture(t, newLabel(t, "refund-contrast"))
		requireDrop(t, fixture.answer(t, dtypeCached), DropUnknownDType)

		record, live := fixture.origin.reverse.Lookup(fixture.label)
		if !live {
			t.Fatal("a gate refusal leaves the record pending")
		}
		if record.ProbesLeft() != DefaultReverseProbeBudget-1 {
			t.Fatalf("probes left = %d after a refused answer, want %d",
				record.ProbesLeft(), DefaultReverseProbeBudget-1)
		}
	})
}

// TestFailedEnqueueToUpstreamLeavesTheRecordClaimed is §4.2: the answer is
// lost, the initiator retries with a fresh label, and no second chance is
// granted — otherwise repeats could hammer the upstream for free.
func TestFailedEnqueueToUpstreamLeavesTheRecordClaimed(t *testing.T) {
	fixture := newResponseFixture(t, newLabel(t, "queue-refused"))
	fixture.net.refuseQueue(fixture.origin.id)

	requireDrop(t, fixture.answer(t, dtypeAnswer), DropAnswerNotDelivered)

	record, live := fixture.relay.reverse.Lookup(fixture.label)
	if !live {
		t.Fatal("the record survives a failed enqueue")
	}
	if record.State() != ReverseSlotClaimed {
		t.Fatalf("record is %s, want claimed", record.State())
	}
	// No second chance: another answer finds the slot taken.
	requireDrop(t, fixture.answer(t, dtypeAnswer), DropReverseNotPending)
}

// TestResponseTTLIsCheckedBeforeTheClaim is rule 4 of §4.1.1 met by the §4.2
// invariant "a drop at any step before the CAS leaves the record pending".
//
// An answer that cannot be forwarded — the decrement pays for the hop about to
// be made, so `ttl = 1` leaves nothing to pay with — is a refusal like any
// other, and a refusal must not eat the single answer slot of the record. The
// local branch has no hop to pay for at all, so the same ttl is delivered
// there: the check belongs to the network fork and to nothing else.
func TestResponseTTLIsCheckedBeforeTheClaim(t *testing.T) {
	t.Run("network upstream", func(t *testing.T) {
		fixture := newResponseFixture(t, newLabel(t, "ttl-one"))

		result := fixture.relay.deliver(t, fixture.target.id, responseFrame(t, responseOpts{
			label: fixture.label, subject: fixture.target.id, ttl: 1,
		}))
		requireDrop(t, result, DropTTLExhausted)
		requirePending(t, fixture)

		// The refusal pays its probe, exactly like a gate refusal on the local
		// branch: only the attempt that goes on to claim is refunded.
		record, _ := fixture.relay.reverse.Lookup(fixture.label)
		if record.ProbesLeft() != DefaultReverseProbeBudget-1 {
			t.Fatalf("probes left = %d after a refused answer, want %d",
				record.ProbesLeft(), DefaultReverseProbeBudget-1)
		}

		// The whole point: the genuine answer still gets the slot.
		requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundForwarded)
	})

	t.Run("local upstream", func(t *testing.T) {
		fixture := newLocalResponseFixture(t, newLabel(t, "ttl-one-local"))
		handler := acceptingHandler()
		registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, handler))

		// Rule 4 of §4.1.1: local delivery does not decrement, so there is no
		// budget to run out of and `ttl = 1` is a perfectly good answer.
		requireOutcome(t, fixture.origin.deliver(t, fixture.relay.id, responseFrame(t, responseOpts{
			label: fixture.label, subject: fixture.target.id, ttl: 1,
		})), InboundDelivered)

		delivery, ok := handler.lastContext()
		if !ok {
			t.Fatal("the resolver was not called")
		}
		if got := delivery.Header().TTL(); got != 1 {
			t.Fatalf("the resolver saw ttl %d, want the undecremented 1", got)
		}
	})
}

// TestResponsePlaneNeverTouchesTheRoutedReplayCache is the §4.1 separation of
// the planes on the way back.
func TestResponsePlaneNeverTouchesTheRoutedReplayCache(t *testing.T) {
	fixture := newResponseFixture(t, newLabel(t, "planes"))

	requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundForwarded)
	if fixture.relay.replay.Len() != 0 {
		t.Fatalf("the response plane occupied %d anti-replay records", fixture.relay.replay.Len())
	}
}

// ---------------------------------------------------------------------------
// upstream = local
// ---------------------------------------------------------------------------

// localResponseFixture is an origin that sent its own request and now receives
// the answer.
type localResponseFixture struct {
	net    *fakeNetwork
	origin *pipelineNode
	relay  *pipelineNode
	target *pipelineNode
	label  Label
}

func newLocalResponseFixture(t *testing.T, label Label) *localResponseFixture {
	t.Helper()
	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	fixture := &localResponseFixture{net: net, origin: nodes[0], relay: nodes[1], target: nodes[2], label: label}
	outcome := fixture.origin.pipeline.SendLocal(context.Background(), LocalSendOpts{
		Frame: requestFrame(t, requestOpts{label: label, dst: fixture.target.id}),
	})
	if outcome.Kind() != SendQueued {
		t.Fatalf("SendLocal: %s", outcome)
	}
	return fixture
}

func (f *localResponseFixture) answer(t *testing.T, dtype domain.DType) InboundResult {
	t.Helper()
	return f.origin.deliver(t, f.relay.id, responseFrame(t, responseOpts{
		label: f.label, subject: f.target.id, dtype: dtype,
	}))
}

// TestLocalResponseGatesRunBeforeTheClaim is §4.1 and §9: an answer of an
// unknown or forbidden type is refused by the registry and by authorization
// BEFORE the CAS, so it does not eat the single slot — and the next valid
// answer passes.
func TestLocalResponseGatesRunBeforeTheClaim(t *testing.T) {
	t.Run("unknown type", func(t *testing.T) {
		fixture := newLocalResponseFixture(t, newLabel(t, "local-unknown"))
		requireDrop(t, fixture.answer(t, dtypeCached), DropUnknownDType)
		requireLocalPending(t, fixture)

		registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))
		requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundDelivered)
	})

	t.Run("known type that answers another request", func(t *testing.T) {
		fixture := newLocalResponseFixture(t, newLabel(t, "local-pairing"))
		// A type this node knows, but one that never declared get_identity
		// among the requests it answers: it must not take the single slot of
		// somebody else's exchange (§4.2).
		registerType(t, fixture.origin, responseType(dtypeCached, dtypeUnrelated, acceptingHandler()))
		registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))

		requireDrop(t, fixture.answer(t, dtypeCached), DropReversePairing)
		requireLocalPending(t, fixture)
		requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundDelivered)
	})

	t.Run("authorization reject", func(t *testing.T) {
		fixture := newLocalResponseFixture(t, newLabel(t, "local-auth"))
		refuse := true
		registration := responseType(dtypeAnswer, dtypeQuery, acceptingHandler())
		registration.Authorizer = AuthorizerFunc(
			func(context.Context, DeliveryContext, []byte) AuthorizationDecision {
				if refuse {
					return Reject(errTestRefused)
				}
				return Accept()
			})
		registerType(t, fixture.origin, registration)

		requireDrop(t, fixture.answer(t, dtypeAnswer), DropUnauthorized)
		requireLocalPending(t, fixture)

		refuse = false
		requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundDelivered)
	})
}

func requireLocalPending(t *testing.T, fixture *localResponseFixture) {
	t.Helper()
	record, live := fixture.origin.reverse.Lookup(fixture.label)
	if !live {
		t.Fatal("a gate refusal must leave the record alive")
	}
	if record.State() != ReverseSlotPending {
		t.Fatalf("record is %s, want pending", record.State())
	}
}

// TestLocalResolverOutcomeAfterTheClaimLeavesItClaimed is the §9 row: a
// `rejected` or `failed` from the resolver AFTER the CAS is the local twin of a
// failed enqueue — the answer never reached the thing that asked for it, so the
// record stays claimed until expires_at and no second chance is granted.
func TestLocalResolverOutcomeAfterTheClaimLeavesItClaimed(t *testing.T) {
	refusals := map[string]struct {
		handler *recordingHandler
		reason  DropReason
	}{
		"failed":   {handler: failingHandler(), reason: DropHandlerFailed},
		"rejected": {handler: refusingHandler(), reason: DropHandlerRejected},
	}
	for name, refusal := range refusals {
		t.Run(name, func(t *testing.T) {
			fixture := newLocalResponseFixture(t, newLabel(t, "resolver-"+name))
			registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, refusal.handler))

			requireDrop(t, fixture.answer(t, dtypeAnswer), refusal.reason)

			record, live := fixture.origin.reverse.Lookup(fixture.label)
			if !live {
				t.Fatal("the record survives the resolver's refusal")
			}
			if record.State() != ReverseSlotClaimed {
				t.Fatalf("record is %s, want claimed — no second chance is granted", record.State())
			}
		})
	}
}

// TestLocallyDeliveredAnswerFreesItsRecord is the local half of "the slot is
// freed only after the answer was DELIVERED", and the half the fork was
// missing.
//
// On the network branch the mutating step is the enqueue and its success frees
// the record; on the local branch the mutating step is the resolver, and an
// `accepted` resolver is the same event on this side — the answer reached the
// thing that asked for it and the exchange is over. Holding the record past it
// is not symmetry with a failed enqueue, it is the opposite of it: the network
// branch holds on FAILURE only.
func TestLocallyDeliveredAnswerFreesItsRecord(t *testing.T) {
	fixture := newLocalResponseFixture(t, newLabel(t, "local-complete"))
	registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))

	requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundDelivered)

	if _, live := fixture.origin.reverse.Lookup(fixture.label); live {
		t.Fatal("the answer was delivered and its record is still there: the finished exchange " +
			"keeps its slot charged to the local upstream until expires_at")
	}
	if held := fixture.origin.reverse.Len(); held != 0 {
		t.Fatalf("the table holds %d records after its only exchange finished", held)
	}
	// A late copy of the same answer is UNADDRESSED rather than "already
	// claimed" — exactly what a completed forward leaves behind.
	requireDrop(t, fixture.answer(t, dtypeAnswer), DropReverseUnknownLabel)
}

// TestAnsweredLocalRequestsDoNotFillThePerUpstreamQuota is the consequence that
// makes the missing release a P1 rather than a tidiness matter.
//
// Every request this node originates takes a slot in the LocalUpstream bucket,
// which §5 caps at defaultReversePerUpstreamCap for a whole reverse window of
// 240 s. If a finished exchange never gives its slot back, the node can start
// no more than that many request exchanges per window however promptly they are
// answered — and the refusal surfaces as `rejected`, which tells the caller
// that retrying is pointless when the truth is that the node ran out of its own
// bookkeeping.
func TestAnsweredLocalRequestsDoNotFillThePerUpstreamQuota(t *testing.T) {
	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	origin, relay, target := nodes[0], nodes[1], nodes[2]
	registerType(t, origin, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))

	// One exchange more than the cap, all inside ONE reverse window: each is
	// asked and answered in full before the next one starts.
	for i := 0; i <= defaultReversePerUpstreamCap; i++ {
		label := newLabel(t, "exchange-"+strconv.Itoa(i))
		outcome := origin.pipeline.SendLocal(context.Background(), LocalSendOpts{
			Frame: requestFrame(t, requestOpts{label: label, dst: target.id}),
		})
		if outcome.Kind() != SendQueued {
			t.Fatalf("exchange %d: SendLocal = %s, want queued — %d answered exchanges still hold "+
				"their slots", i, outcome, i)
		}
		requireOutcome(t, origin.deliver(t, relay.id, responseFrame(t, responseOpts{
			label: label, subject: target.id,
		})), InboundDelivered)
	}
}

// TestLocalResponseIsNotDecrementedAndNotEnqueued is rule 4 of §4.1.1 for the
// `upstream = local` branch.
func TestLocalResponseIsNotDecrementedAndNotEnqueued(t *testing.T) {
	fixture := newLocalResponseFixture(t, newLabel(t, "local-ttl"))
	handler := acceptingHandler()
	registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, handler))

	before := len(fixture.net.journal())
	result := fixture.origin.deliver(t, fixture.relay.id, responseFrame(t, responseOpts{
		label: fixture.label, subject: fixture.target.id, ttl: 4,
	}))
	requireOutcome(t, result, InboundDelivered)

	if len(fixture.net.journal()) != before {
		t.Fatal("a locally consumed answer is not enqueued anywhere")
	}
	delivery, ok := handler.lastContext()
	if !ok {
		t.Fatal("the resolver was not called")
	}
	if got := delivery.Header().TTL(); got != 4 {
		t.Fatalf("the resolver saw ttl %d, want the undecremented 4", got)
	}
	if _, ok := delivery.Header().SignedSrc(); ok {
		t.Fatal("a response has no authenticated src, even at the initiator")
	}
}

// TestAuthorizationHookRunsOnAllThreePlanes is the last §9 row of the mode
// fork: the hook is called on local delivery in routed, request and response.
func TestAuthorizationHookRunsOnAllThreePlanes(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	node := newPipelineNode(t, net, nodeOpts{name: "endpoint"})
	link(sender, node, false, false)

	seen := map[domain.DatagramMode]struct{}{}
	hook := AuthorizerFunc(func(_ context.Context, delivery DeliveryContext, _ []byte) AuthorizationDecision {
		seen[delivery.Header().Mode()] = struct{}{}
		return Accept()
	})

	routed := routedType(dtypePush, acceptingHandler())
	routed.Authorizer = hook
	registerType(t, node, routed)
	request := requestType(dtypeQuery, acceptingHandler())
	request.Authorizer = hook
	registerType(t, node, request)
	response := responseType(dtypeAnswer, dtypeQuery, acceptingHandler())
	response.Authorizer = hook
	registerType(t, node, response)

	requireOutcome(t, node.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: node.id, now: node.clock(),
	})), InboundDelivered)
	requireOutcome(t, node.deliver(t, sender.id, requestFrame(t, requestOpts{
		label: newLabel(t, "hook"), dst: node.id,
	})), InboundDelivered)

	// The response needs a reverse record with a local upstream.
	target := newPipelineNode(t, net, nodeOpts{name: "target"})
	link(node, target, false, false)
	route(node, target.id, target.id, 1)
	label := newLabel(t, "hook-response")
	if outcome := node.pipeline.SendLocal(context.Background(), LocalSendOpts{
		Frame: requestFrame(t, requestOpts{label: label, dst: target.id}),
	}); outcome.Kind() != SendQueued {
		t.Fatalf("SendLocal: %s", outcome)
	}
	requireOutcome(t, node.deliver(t, target.id, responseFrame(t, responseOpts{
		label: label, subject: target.id,
	})), InboundDelivered)

	for _, mode := range []domain.DatagramMode{
		domain.DatagramModeRouted, domain.DatagramModeRequest, domain.DatagramModeResponse,
	} {
		if _, called := seen[mode]; !called {
			t.Fatalf("the authorization hook was not called on the %s plane", mode)
		}
	}
}
