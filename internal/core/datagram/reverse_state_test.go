package datagram

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// reverse_state_test.go covers the table itself: the §4.2 window formula, the
// two-phase reservation, the probe budget and the caps. The pipeline-level
// assertions of the same section live in pipeline_request_test.go and
// pipeline_response_test.go.

func newTestReverseTable(now *time.Time) *ReverseTable {
	return NewReverseTable(ReverseTableConfig{Clock: func() time.Time { return *now }})
}

// TestReverseWindowIsDerivedFromTheFormula pins §4.2: 240 seconds is a RESULT,
// not a literal — 2 × 10 hops × (5 s queue + 5 s write grace) + 10 s target
// budget = 210 s, rounded up to whole minutes.
func TestReverseWindowIsDerivedFromTheFormula(t *testing.T) {
	residence, err := domain.QueueResidence(domain.DatagramClassControl)
	if err != nil {
		t.Fatalf("QueueResidence: %v", err)
	}
	grace, err := domain.WriteGrace(domain.DatagramClassControl)
	if err != nil {
		t.Fatalf("WriteGrace: %v", err)
	}
	raw := 2*time.Duration(domain.DatagramDefaultMaxHops)*(residence+grace) + reverseTargetBudget
	if raw != 210*time.Second {
		t.Fatalf("the raw formula gives %s, want 210s", raw)
	}
	if got := ReverseStateWindow(); got != domain.ReverseStateTTL {
		t.Fatalf("derived window %s, want the wire constant %s", got, domain.ReverseStateTTL)
	}
	if domain.ReverseStateTTL != 240*time.Second {
		t.Fatalf("the wire constant moved to %s", domain.ReverseStateTTL)
	}

	// The whole reason the two must agree: a ten-hop round trip where every hop
	// spends its full queue residence AND its full write grace still fits.
	roundTrip := 2 * time.Duration(domain.DatagramDefaultMaxHops) * (residence + grace)
	if roundTrip > domain.ReverseStateTTL {
		t.Fatalf("a full round trip (%s) outlives the record (%s)", roundTrip, domain.ReverseStateTTL)
	}
}

// TestResponseDeadlineEqualsControlQueueResidence pins §9: the send deadline
// of an answer is arrival + queue_residence(control), the SAME 5 seconds the
// reverse window is computed from. Thirty here would make a ten-hop round trip
// take 1200 s against a record that lives 240.
func TestResponseDeadlineEqualsControlQueueResidence(t *testing.T) {
	residence, err := domain.QueueResidence(domain.DatagramClassControl)
	if err != nil {
		t.Fatalf("QueueResidence: %v", err)
	}
	if residence != 5*time.Second {
		t.Fatalf("control queue residence is %s, want 5s", residence)
	}
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	if got := ResponseSendDeadline(now); !got.Equal(now.Add(residence)) {
		t.Fatalf("answer deadline %s, want %s", got, now.Add(residence))
	}
	bulk, err := domain.QueueResidence(domain.DatagramClassBulk)
	if err != nil || bulk != 30*time.Second {
		t.Fatalf("bulk queue residence is %s (%v), want 30s", bulk, err)
	}
}

func TestReverseRecordStoresTheRequestDType(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("label"))
	target := domaintest.ID("target")

	result := table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: target,
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("upstream")),
	})
	if result.Outcome() != ReverseSlotReserved {
		t.Fatalf("Reserve: %s", result.Outcome())
	}
	record, live := table.Lookup(label)
	if !live {
		t.Fatal("the record must be live")
	}
	if record.DType() != dtypeQuery {
		t.Fatalf("record dtype %q, want the REQUEST dtype %q", record.DType(), dtypeQuery)
	}
	if record.Dst() != target {
		t.Fatalf("record dst %q, want %q", record.Dst(), target)
	}
	if record.State() != ReverseSlotPending {
		t.Fatalf("a fresh record is %s, want pending", record.State())
	}
	// expires_at = moment of arrival + the derived window (§4.2).
	if want := now.Add(ReverseStateWindow()); !record.ExpiresAt().Equal(want) {
		t.Fatalf("expires_at %s, want %s", record.ExpiresAt(), want)
	}
	if record.ProbesLeft() != DefaultReverseProbeBudget {
		t.Fatalf("probe budget %d, want %d", record.ProbesLeft(), DefaultReverseProbeBudget)
	}
}

// TestRepeatedLabelDoesNotOverwriteTheRecord is §4.2 phase 2: a busy slot is a
// drop with no overwrite. Re-pointing downstream would lose the return path of
// the first forward, and a repeat can be an honest loop, so no ban either.
func TestRepeatedLabelDoesNotOverwriteTheRecord(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("label"))
	first := domaintest.ID("hop-a")

	slot, _ := table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up")),
	}).Slot()
	table.FixDownstream(slot, testDownstream(first))

	second := table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("other"),
		DType: dtypeUnrelated, Upstream: testUpstream(domaintest.ID("up2")),
	})
	if second.Outcome() != ReverseSlotBusy {
		t.Fatalf("a taken slot must answer busy, got %s", second.Outcome())
	}
	record, _ := table.Lookup(label)
	downstream, set := record.Downstream()
	if !set || downstream.Channel() != testChannel(first.String()) {
		t.Fatalf("downstream was overwritten: %v/%v", downstream, set)
	}
	if record.DType() != dtypeQuery {
		t.Fatalf("the stored dtype was overwritten: %q", record.DType())
	}
}

// TestFreshLabelTakesItsOwnSlot is the other half of §9's "label as the state
// key": the initiator's retry arrives with a NEW label, so the no-overwrite
// rule costs nothing.
func TestFreshLabelTakesItsOwnSlot(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	target := domaintest.ID("target")
	upstream := testUpstream(domaintest.ID("up"))

	for _, name := range []string{"attempt-1", "attempt-2"} {
		result := table.Reserve(ReverseReserveOpts{
			ReceivedAt: now, Label: NewLabel(domaintest.ID(name)), Dst: target,
			DType: dtypeQuery, Upstream: upstream,
		})
		if result.Outcome() != ReverseSlotReserved {
			t.Fatalf("%s: %s", name, result.Outcome())
		}
	}
	if table.Len() != 2 {
		t.Fatalf("two labels must hold two records, got %d", table.Len())
	}
}

// TestRollbackReleasesTheWholeSlot is §4.2 phase 4: when the candidates run
// out the record goes entirely, so a later request with the same label is not
// blocked by a forward that never happened.
func TestRollbackReleasesTheWholeSlot(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("label"))

	slot, _ := table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up")),
	}).Slot()
	if !table.Rollback(slot) {
		t.Fatal("Rollback must succeed for a held slot")
	}
	if _, live := table.Lookup(label); live {
		t.Fatal("a rolled back record must be gone")
	}
	// The stale handle must not evict a record somebody else took meanwhile.
	table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up")),
	})
	if table.Rollback(slot) {
		t.Fatal("a stale slot handle must not release a fresh record (ABA)")
	}
	if _, live := table.Lookup(label); !live {
		t.Fatal("the fresh record must survive the stale rollback")
	}
}

// TestProbeBudgetIsAtomicAndOnlyRefusalsPayIt covers the two probe rules of
// §4.2 at once: the increment-and-test is atomic, so no more than `probes`
// attempts ever reach the expensive check, and a successful forward refunds
// its unit.
func TestProbeBudgetIsAtomicAndOnlyRefusalsPayIt(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("label"))
	table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up")),
	})
	record := mustRecord(t, table, label)

	const attackers = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted int
	)
	for i := 0; i < attackers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, outcome := table.ReserveProbe(record); outcome == ReverseProbeGranted {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != DefaultReverseProbeBudget {
		t.Fatalf("%d parallel attempts entered the expensive check, want exactly %d",
			granted, DefaultReverseProbeBudget)
	}
	// Exhaustion does NOT free the slot: the record stays pending until
	// expires_at, only the cryptographic work stops being paid for.
	exhausted, live := table.Lookup(label)
	if !live || exhausted.State() != ReverseSlotPending {
		t.Fatalf("an exhausted budget must leave the record pending, got %v/%s", live, exhausted.State())
	}

	// A refund puts one unit back, which is what a successful forward does.
	if _, outcome := table.ReserveProbe(record); outcome != ReverseProbeExhausted {
		t.Fatalf("outcome %s, want exhausted: the budget is spent", outcome)
	}
	table.RefundProbe(ProbeTicket{label: label, generation: record.generation, held: true})
	if _, outcome := table.ReserveProbe(record); outcome != ReverseProbeGranted {
		t.Fatalf("a refunded unit must be reusable, got %s", outcome)
	}
}

// TestClaimIsASingleShotCAS covers "a second valid response is dropped" and
// "a drop before the CAS leaves the record pending" (§4.2).
func TestClaimIsASingleShotCAS(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("label"))
	table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up")),
	})

	pending := mustRecord(t, table, label)
	slot, claimed := table.Claim(pending)
	if !claimed {
		t.Fatal("the first claim must win")
	}
	if _, second := table.Claim(pending); second {
		t.Fatal("a second answer must not claim an already claimed record")
	}
	record, _ := table.Lookup(label)
	if record.State() != ReverseSlotClaimed {
		t.Fatalf("state %s, want claimed", record.State())
	}
	// Only a successful enqueue frees the record.
	if !table.Complete(slot) {
		t.Fatal("Complete must free the claimed record")
	}
	if _, live := table.Lookup(label); live {
		t.Fatal("a completed record must be gone")
	}
}

func TestExpiredRecordIsNotLive(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("label"))
	table.Reserve(ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up")),
	})

	now = now.Add(ReverseStateWindow())
	if _, live := table.Lookup(label); !live {
		t.Fatal("the boundary instant is still alive")
	}
	now = now.Add(time.Nanosecond)
	if _, live := table.Lookup(label); live {
		t.Fatal("past expires_at the record is gone")
	}
}

// TestLocalUpstreamIsAMarkerNotAnAddress pins §4.2: `upstream = local` is its
// own bucket, so the transit path and the local one never mix.
func TestLocalUpstreamIsAMarkerNotAnAddress(t *testing.T) {
	if peer, ok := LocalUpstream().Peer(); ok {
		t.Fatalf("the local marker must not be addressable, got %v", peer)
	}
	if !LocalUpstream().IsLocal() {
		t.Fatal("the local marker must report itself as local")
	}
	if sameUpstream(LocalUpstream(), testUpstream(domain.PeerIdentity{})) {
		t.Fatal("the local marker is not the zero identity")
	}
	if LocalUpstream().String() != "local" {
		t.Fatalf("the marker renders as %q", LocalUpstream().String())
	}
	// The response fork of §4.1 step 6 picks its branch by Peer()'s bool alone,
	// so that bool must be EXACTLY the negation of IsLocal(). A third answer
	// here — an upstream that is neither local nor addressable — would put a
	// drop after the CAS again, and such a drop holds the single answer slot
	// until expires_at.
	neighbour := testUpstream(domaintest.ID("upstream"))
	peer, addressable := neighbour.Peer()
	if !addressable || peer != domaintest.ID("upstream") {
		t.Fatalf("a neighbour upstream must be addressable, got %v/%v", peer, addressable)
	}
	if neighbour.IsLocal() {
		t.Fatal("a neighbour upstream must not report itself as local")
	}
}

// stubLimits is the narrow M8 seam the table asks for its two numbers.
type stubLimits struct{ global, perUpstream int }

func (l stubLimits) ReverseStateCaps() (int, int) { return l.global, l.perUpstream }

// TestCapsAndFairEviction covers the §5 bound: a per-upstream cap, a global
// cap, and eviction that takes from the BUSIEST upstream rather than from
// whoever happens to be oldest overall.
func TestCapsAndFairEviction(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := NewReverseTable(ReverseTableConfig{
		Clock:  func() time.Time { return now },
		Limits: stubLimits{global: 3, perUpstream: 2},
	})
	noisy := testUpstream(domaintest.ID("noisy"))
	quiet := testUpstream(domaintest.ID("quiet"))

	reserve := func(name string, upstream Upstream, at time.Time) ReverseReserveOutcome {
		return table.Reserve(ReverseReserveOpts{
			ReceivedAt: at, Label: NewLabel(domaintest.ID(name)), Dst: domaintest.ID("target"),
			DType: dtypeQuery, Upstream: upstream,
		}).Outcome()
	}

	if got := reserve("n1", noisy, now); got != ReverseSlotReserved {
		t.Fatalf("n1: %s", got)
	}
	if got := reserve("n2", noisy, now.Add(time.Second)); got != ReverseSlotReserved {
		t.Fatalf("n2: %s", got)
	}
	// The per-upstream cap refuses a third record of the noisy neighbour, and
	// it refuses it WITHOUT evicting anybody: a cap is not an eviction trigger.
	if got := reserve("n3", noisy, now.Add(2*time.Second)); got != ReverseSlotCapped {
		t.Fatalf("the per-upstream cap must refuse n3, got %s", got)
	}
	if got := reserve("q1", quiet, now.Add(3*time.Second)); got != ReverseSlotReserved {
		t.Fatalf("q1: %s", got)
	}
	// The table is now full (3 of 3). The quiet neighbour's next record evicts
	// from the BUSIEST upstream, which is the noisy one.
	if got := reserve("q2", quiet, now.Add(4*time.Second)); got != ReverseSlotReserved {
		t.Fatalf("q2 must be admitted by evicting the busiest upstream, got %s", got)
	}
	if load := upstreamLoad(table, noisy); load != 1 {
		t.Fatalf("the noisy upstream keeps %d records, want 1 after a fair eviction", load)
	}
	if load := upstreamLoad(table, quiet); load != 2 {
		t.Fatalf("the quiet upstream holds %d records, want 2", load)
	}
}

// reentrantReverseMetrics reads the table back from inside the metric
// callback, which is exactly what a sink publishing "current depth" alongside
// the event counter does — and exactly what M9 is expected to wire.
type reentrantReverseMetrics struct {
	table  *ReverseTable
	events []ReverseEvent
	depths []int
}

func (m *reentrantReverseMetrics) ObserveReverseState(event ReverseEvent) {
	m.events = append(m.events, event)
	m.depths = append(m.depths, upstreamLoad(m.table, LocalUpstream()))
}

// TestReverseMetricsAreNotCalledUnderTheTableMutex pins the CLAUDE.md rule the
// doc comment of the table already claimed to follow ("nothing external is
// called while it is held") but the code broke: observeLocked ran under t.mu
// from Reserve, FixDownstream, Rollback, Lookup, ReserveProbe, Claim, Complete
// and the sweeps.
//
// reverseMetrics is an INJECTED interface, so a sink that reads the table back
// is not a hypothetical: it is a self-deadlock, and a self-deadlock in a
// non-reentrant sync.Mutex is a hang, not a panic. Without the fix this test
// never returns — which is why it runs behind a watchdog rather than simply
// asserting a value.
func TestReverseMetricsAreNotCalledUnderTheTableMutex(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	sink := &reentrantReverseMetrics{}
	table := NewReverseTable(ReverseTableConfig{
		Clock:   func() time.Time { return now },
		Metrics: sink,
	})
	sink.table = table

	label := NewLabel(domaintest.ID("label"))
	done := make(chan struct{})
	go func() {
		defer close(done)
		reservation := table.Reserve(ReverseReserveOpts{
			ReceivedAt: now,
			Label:      label,
			Dst:        domaintest.ID("dst"),
			DType:      dtypeQuery,
			Upstream:   LocalUpstream(),
		})
		slot, ok := reservation.Slot()
		if !ok {
			return
		}
		table.FixDownstream(slot, testDownstream(domaintest.ID("hop")))
		// The record is read ONCE and carried to the calls that need it: both
		// the probe and the claim are addressed by the record the caller holds,
		// and t.Fatalf may not be called from this goroutine anyway.
		record, live := table.Lookup(label)
		if !live {
			t.Error("the record vanished before the probe")
			return
		}
		table.ReserveProbe(record)
		if claimed, taken := table.Claim(record); taken {
			table.Complete(claimed)
		}
		table.Sweep()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reverse table called its metric sink under its own mutex: self-deadlock")
	}
	if len(sink.events) == 0 {
		t.Fatal("no events reached the sink")
	}
}

func mustRecord(t *testing.T, table *ReverseTable, label Label) ReverseRecord {
	t.Helper()
	record, live := table.Lookup(label)
	if !live {
		t.Fatalf("record for %s vanished", label)
	}
	return record
}

// TestClaimRefusesAStaleGeneration pins the ABA guard the second review found
// missing: Rollback and Complete were guarded by the generation, Claim was not.
//
// The label is chosen by whoever sends the request, so an attacker can make a
// record be rolled back and a FRESH one take the same label between the Lookup
// that validated an answer and the Claim that acts on it. Claiming by label
// alone would give the new exchange's single answer slot to an answer whose
// downstream, subject and dtype pairing were checked against the old copy.
func TestClaimRefusesAStaleGeneration(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("recycled-label"))
	first := ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target-one"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up-one")),
	}
	reservation := table.Reserve(first)
	slot, ok := reservation.Slot()
	if !ok {
		t.Fatalf("Reserve: %s", reservation.Outcome())
	}
	stale := mustRecord(t, table, label)

	// The first exchange runs out of candidates and gives the slot back; a new
	// request takes the same label a moment later.
	if !table.Rollback(slot) {
		t.Fatal("Rollback must free the record")
	}
	second := first
	second.Dst = domaintest.ID("target-two")
	second.Upstream = testUpstream(domaintest.ID("up-two"))
	if _, ok := table.Reserve(second).Slot(); !ok {
		t.Fatal("the fresh request must take the label")
	}

	if _, claimed := table.Claim(stale); claimed {
		t.Fatal("a stale record claimed the answer slot of a fresh exchange")
	}
	if record := mustRecord(t, table, label); record.State() != ReverseSlotPending {
		t.Fatalf("the fresh record is %s, want pending — its single slot must still be free", record.State())
	}
	// And the fresh record's own answer still gets through.
	if _, claimed := table.Claim(mustRecord(t, table, label)); !claimed {
		t.Fatal("the fresh exchange must still be claimable")
	}
}

// TestReserveProbeRefusesAStaleGeneration is the same ABA guard one method
// further along: Rollback, Complete and Claim took the handle or the record the
// caller owned, while the probe budget was still charged BY LABEL.
//
// The window is between the Lookup of §4.1 step 4 and the reservation of step 5.
// The label belongs to whoever sent the request, so an exchange can end —
// rolled back, completed or expired — and a FRESH one take the same label
// inside that window. A budget charged by label alone is then charged to the
// fresh record for an answer that was validated against the old one, and a
// handful of such answers leave the live exchange unable to pay for its own
// genuine reply while its slot is still pending and its label still valid.
func TestReserveProbeRefusesAStaleGeneration(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	label := NewLabel(domaintest.ID("recycled-label"))
	first := ReverseReserveOpts{
		ReceivedAt: now, Label: label, Dst: domaintest.ID("target-one"),
		DType: dtypeQuery, Upstream: testUpstream(domaintest.ID("up-one")),
	}
	reservation := table.Reserve(first)
	slot, ok := reservation.Slot()
	if !ok {
		t.Fatalf("Reserve: %s", reservation.Outcome())
	}
	stale := mustRecord(t, table, label)

	// TWO exchanges under ONE label: the first gives its slot back, the second
	// takes the label a moment later. One exchange could not show which of them
	// was charged.
	if !table.Rollback(slot) {
		t.Fatal("Rollback must free the record")
	}
	second := first
	second.Dst = domaintest.ID("target-two")
	second.Upstream = testUpstream(domaintest.ID("up-two"))
	if _, ok := table.Reserve(second).Slot(); !ok {
		t.Fatal("the fresh request must take the label")
	}
	fresh := mustRecord(t, table, label)

	// Enough answers to the ENDED exchange to drain the whole budget, if they
	// were allowed to touch it.
	for attempt := 0; attempt <= DefaultReverseProbeBudget; attempt++ {
		ticket, outcome := table.ReserveProbe(stale)
		if outcome != ReverseProbeStale {
			t.Fatalf("attempt %d against a record that ended: outcome %s, want stale", attempt, outcome)
		}
		if !ticket.IsZero() {
			t.Fatalf("attempt %d holds a probe ticket for a record that no longer exists", attempt)
		}
	}
	if left := mustRecord(t, table, label).ProbesLeft(); left != DefaultReverseProbeBudget {
		t.Fatalf("the fresh exchange has %d probes left, want the untouched %d: "+
			"answers to a finished exchange spent somebody else's budget", left, DefaultReverseProbeBudget)
	}

	// POSITIVE CONTROL. Without it every assertion above is satisfied by a table
	// that refuses every probe: the fresh record's OWN answer must spend and
	// refund exactly as before.
	ticket, outcome := table.ReserveProbe(fresh)
	if outcome != ReverseProbeGranted {
		t.Fatalf("the fresh exchange's own answer got %s, want granted", outcome)
	}
	if left := mustRecord(t, table, label).ProbesLeft(); left != DefaultReverseProbeBudget-1 {
		t.Fatalf("a granted probe left %d, want %d", left, DefaultReverseProbeBudget-1)
	}
	table.RefundProbe(ticket)
	if left := mustRecord(t, table, label).ProbesLeft(); left != DefaultReverseProbeBudget {
		t.Fatalf("a refund left %d, want the full %d", left, DefaultReverseProbeBudget)
	}

	// And the fresh record still runs out on its OWN answers: "exhausted" and
	// "stale" are two verdicts, not one refusal wearing two names.
	for attempt := 0; attempt < DefaultReverseProbeBudget; attempt++ {
		if _, outcome := table.ReserveProbe(fresh); outcome != ReverseProbeGranted {
			t.Fatalf("attempt %d of the fresh exchange: %s, want granted", attempt, outcome)
		}
	}
	if _, outcome := table.ReserveProbe(fresh); outcome != ReverseProbeExhausted {
		t.Fatalf("outcome %s, want exhausted once the record spent its own budget", outcome)
	}
	// Exhaustion still does not free the slot.
	if record := mustRecord(t, table, label); record.State() != ReverseSlotPending {
		t.Fatalf("the exhausted record is %s, want pending", record.State())
	}
}

// TestReserveDoesNotWalkTheWholeTable pins the cost of the request plane. That
// plane is UNSIGNED and cheap for an attacker to generate, and the only thing
// standing between a flood and per-frame work proportional to the table was the
// per-peer frame budget. Reserve used to make three full passes — the sweep,
// the per-upstream load and the busiest-upstream tally, the last of which
// ALLOCATED a map sized for the whole table on every forwarded request.
//
// The table is driven at its global cap here, which is where the fairness
// eviction runs and where that map used to be built. Allocations are the
// observable proxy: with the tally kept incrementally the cost of one Reserve
// does not depend on how many records are live.
func TestReserveDoesNotWalkTheWholeTable(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	const capacity = 512
	table := NewReverseTable(ReverseTableConfig{
		Clock:  func() time.Time { return now },
		Limits: stubLimits{global: capacity, perUpstream: capacity},
	})

	// The opts are built OUTSIDE the measured call. testUpstream renders an
	// identity through PeerIdentity.String(), which allocates a hex string, and
	// a fixture that allocates inside AllocsPerRun measures itself: the bound
	// below is about what Reserve does with a full table and about nothing else.
	optsFor := func(seed string) ReverseReserveOpts {
		return ReverseReserveOpts{
			ReceivedAt: now,
			Label:      NewLabel(domaintest.ID("label-" + seed)),
			Dst:        domaintest.ID("target"),
			DType:      dtypeQuery,
			Upstream:   testUpstream(domaintest.ID("up-" + seed)),
		}
	}
	reserveOne := func(seed string) (ReverseSlot, bool) {
		return table.Reserve(optsFor(seed)).Slot()
	}

	// Fill the table right up to its global cap, each record from its own
	// upstream — the widest tally the eviction has to scan.
	for i := 0; i < capacity; i++ {
		if _, ok := reserveOne(strconv.Itoa(i)); !ok {
			t.Fatalf("filler %d refused", i)
		}
	}

	// Every Reserve from here on evicts, which is the branch that used to
	// build a map the size of the table. The probes are prepared up front, so
	// the measured call is one Reserve and no fixture work at all.
	const probes = 101 // AllocsPerRun calls the body once more than it is told
	prepared := make([]ReverseReserveOpts, 0, probes)
	for i := 0; i < probes; i++ {
		prepared = append(prepared, optsFor("probe-"+strconv.Itoa(i)))
	}
	round := 0
	allocs := testing.AllocsPerRun(100, func() {
		opts := prepared[round]
		round++
		if _, ok := table.Reserve(opts).Slot(); !ok {
			t.Fatalf("probe %d refused at the cap", round)
		}
	})
	// One allocation: the record itself. A tally rebuilt per call adds the
	// map and its buckets on top, which is what this bound refuses.
	if allocs > 2 {
		t.Fatalf("Reserve allocated %.0f objects at a %d-record table: the cost still scales with it",
			allocs, capacity)
	}
}

// TestUpstreamTallyStaysInSyncWithTheTable is the correctness half of the
// incremental accounting: a counter that drifts from the map it summarises
// would refuse a neighbour that holds nothing, or admit one over its cap.
func TestUpstreamTallyStaysInSyncWithTheTable(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	table := newTestReverseTable(&now)
	upstream := testUpstream(domaintest.ID("noisy"))

	take := func(seed string) ReverseSlot {
		reservation := table.Reserve(ReverseReserveOpts{
			ReceivedAt: now, Label: NewLabel(domaintest.ID(seed)),
			Dst: domaintest.ID("target"), DType: dtypeQuery, Upstream: upstream,
		})
		slot, ok := reservation.Slot()
		if !ok {
			t.Fatalf("Reserve(%s) = %s", seed, reservation.Outcome())
		}
		return slot
	}

	rolled := take("one")
	completed := take("two")
	expiring := take("three")
	if load := upstreamLoad(table, upstream); load != 3 {
		t.Fatalf("load = %d after three reservations, want 3", load)
	}

	table.Rollback(rolled)
	if _, claimed := table.Claim(mustRecord(t, table, NewLabel(domaintest.ID("two")))); !claimed {
		t.Fatal("the record must be claimable")
	}
	table.Complete(completed)
	if load := upstreamLoad(table, upstream); load != 1 {
		t.Fatalf("load = %d after a rollback and a completion, want 1", load)
	}

	_ = expiring
	now = now.Add(ReverseStateWindow() + time.Second)
	if swept := table.Sweep(); swept != 1 {
		t.Fatalf("Sweep removed %d records, want 1", swept)
	}
	if load := upstreamLoad(table, upstream); load != 0 {
		t.Fatalf("load = %d after the sweep, want 0", load)
	}
}
