package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// relay_hop_ack_budget_test.go covers Phase 3 PR 12.2 — the relay state
// store's per-message hop-ack budget tracking that drives the
// routing.Table reputation primitive. Tests at this layer exercise the
// tick/cancellation/upsert plumbing directly on relayStateStore, without
// spinning up a full Service. Service-level wiring (the production
// onRelayHopAckTimeout callback that resolves an address to identity
// and calls routing.Table.MarkHopFailure) is exercised by the existing
// routing_integration tests once tests below confirm the tick path
// produces the expected fire-list.

// TestHopAckBudget_TickerFiresOnExactlyOneTickAfterCountdown — a fresh
// forwarded state with HopAckRemainingTicks=N produces ZERO callback
// invocations on the first N-1 ticks and exactly ONE invocation on the
// N-th tick. After the fire the entry's HopAckObserved flag is true so
// subsequent ticks must produce no further invocations.
func TestHopAckBudget_TickerFiresOnExactlyOneTickAfterCountdown(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-1",
		ForwardedTo:          domain.PeerAddress("peer-b"),
		Recipient:            domaintest.ID("id-recipient"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
	})

	for i := 1; i <= 2; i++ {
		fired := rs.tickHopAckBudgets()
		if len(fired) != 0 {
			t.Fatalf("tick %d: fired = %v, want nil (budget not elapsed)", i, fired)
		}
	}

	fired := rs.tickHopAckBudgets()
	if len(fired) != 1 {
		t.Fatalf("tick 3: fired = %v, want 1 entry", fired)
	}
	if fired[0].MessageID != "msg-1" {
		t.Fatalf("fired[0].MessageID = %q, want msg-1", fired[0].MessageID)
	}

	// Subsequent ticks must be no-op (HopAckObserved=true now).
	for i := 4; i <= 6; i++ {
		again := rs.tickHopAckBudgets()
		if len(again) != 0 {
			t.Fatalf("tick %d: re-fired %v after one-shot guard, want nil", i, again)
		}
	}
}

// TestHopAckBudget_MarkObservedCancelsTimer — an ack arriving before
// the budget elapses cancels the timer: markHopAckObserved sets
// HopAckObserved=true, and subsequent ticks skip the entry. This is
// the success-path contract of handleRelayHopAck.
func TestHopAckBudget_MarkObservedCancelsTimer(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-cancel",
		ForwardedTo:          domain.PeerAddress("peer-b"),
		Recipient:            domaintest.ID("id-recipient"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
	})

	// First tick decrements but does not fire.
	if fired := rs.tickHopAckBudgets(); len(fired) != 0 {
		t.Fatalf("tick 1: fired = %v, want nil", fired)
	}
	// Ack arrives — cancels.
	if !rs.markHopAckObserved("msg-cancel") {
		t.Fatal("markHopAckObserved returned false for live state")
	}
	// Subsequent ticks must NOT fire.
	for i := 2; i <= 5; i++ {
		fired := rs.tickHopAckBudgets()
		if len(fired) != 0 {
			t.Fatalf("tick %d: fired %v after markHopAckObserved, want nil", i, fired)
		}
	}
}

// TestHopAckBudget_NotTrackingWhenRemainingTicksZero — entries with
// HopAckRemainingTicks=0 (final-recipient path, stored-locally
// fallback) MUST NOT be tracked. The ticker must never fire a
// callback for them regardless of how many cycles run.
func TestHopAckBudget_NotTrackingWhenRemainingTicksZero(t *testing.T) {
	rs := newRelayStateStore()

	// Simulates the final-recipient branch of handleRelayMessage:
	// ForwardedTo is empty, HopAckRemainingTicks is the default 0.
	rs.store(&relayForwardState{
		MessageID:    "msg-final",
		ForwardedTo:  domain.PeerAddress(""),
		Recipient:    domaintest.ID("id-recipient"),
		RemainingTTL: 60,
		// HopAckRemainingTicks: 0 left explicit for clarity.
		HopAckRemainingTicks: 0,
	})

	for i := 1; i <= 20; i++ {
		if fired := rs.tickHopAckBudgets(); len(fired) != 0 {
			t.Fatalf("tick %d: fired %v on untracked state, want nil", i, fired)
		}
	}
}

// TestHopAckBudget_TracksMultipleStatesIndependently — two forwarded
// messages with different budget windows fire at their respective
// ticks; the per-message countdowns do not interfere.
func TestHopAckBudget_TracksMultipleStatesIndependently(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-short",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-x"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 2,
	})
	rs.store(&relayForwardState{
		MessageID:            "msg-long",
		ForwardedTo:          domain.PeerAddress("peer-b"),
		Recipient:            domaintest.ID("id-y"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 4,
	})

	// Tick 1: nothing.
	if fired := rs.tickHopAckBudgets(); len(fired) != 0 {
		t.Fatalf("tick 1: fired %v, want nil", fired)
	}
	// Tick 2: msg-short fires.
	fired := rs.tickHopAckBudgets()
	if len(fired) != 1 || fired[0].MessageID != "msg-short" {
		t.Fatalf("tick 2: fired = %v, want [msg-short]", fired)
	}
	// Tick 3: nothing (msg-long still has 1 tick left).
	if fired := rs.tickHopAckBudgets(); len(fired) != 0 {
		t.Fatalf("tick 3: fired %v, want nil", fired)
	}
	// Tick 4: msg-long fires.
	fired = rs.tickHopAckBudgets()
	if len(fired) != 1 || fired[0].MessageID != "msg-long" {
		t.Fatalf("tick 4: fired = %v, want [msg-long]", fired)
	}
}

// TestHopAckBudget_StoreUpsertRerouteResetsBudget — when store()
// observes a real reroute (existing entry with non-empty old
// ForwardedTo, new entry carrying a different ForwardedTo) the
// hop-ack budget is reset from the caller's struct. This is the
// PR 12.3 prerequisite (multi-path failover will reroute through a
// second-best uplink and expect a fresh deadline + fresh Observed
// flag). Without the reset a stale HopAckObserved=true from the
// previous attempt would silently swallow the second-best uplink's
// timeout.
func TestHopAckBudget_StoreUpsertRerouteResetsBudget(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-reroute",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
	})

	// First budget elapses — ticker fires, sets HopAckObserved=true.
	rs.tickHopAckBudgets()
	rs.tickHopAckBudgets()
	rs.tickHopAckBudgets()

	// Sanity: verify the fire really happened (Observed flipped).
	rs.mu.Lock()
	if !rs.states["msg-reroute"].HopAckObserved {
		t.Fatal("precondition: expected HopAckObserved=true after budget elapse")
	}
	rs.mu.Unlock()

	// Re-route through a different peer with a fresh budget +
	// Observed=false.
	rs.store(&relayForwardState{
		MessageID:            "msg-reroute",
		ForwardedTo:          domain.PeerAddress("peer-b"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
		HopAckObserved:       false,
	})

	// Verify the reset propagated through store()'s upsert branch.
	rs.mu.Lock()
	st := rs.states["msg-reroute"]
	rs.mu.Unlock()
	if st.HopAckObserved {
		t.Fatal("HopAckObserved still true after reroute store(); want false")
	}
	if st.HopAckRemainingTicks != 3 {
		t.Fatalf("HopAckRemainingTicks = %d after reroute, want 3 (fresh budget)", st.HopAckRemainingTicks)
	}
	// AbandonedForwardedTo should now carry the previous peer.
	if len(st.AbandonedForwardedTo) != 1 || st.AbandonedForwardedTo[0] != "peer-a" {
		t.Fatalf("AbandonedForwardedTo = %v, want [peer-a]", st.AbandonedForwardedTo)
	}

	// New budget elapses → second fire targeting peer-b.
	var lastFired []relayForwardState
	for i := 0; i < 3; i++ {
		lastFired = rs.tickHopAckBudgets()
	}
	if len(lastFired) != 1 {
		t.Fatalf("post-reroute fire: got %v, want 1", lastFired)
	}
	if lastFired[0].ForwardedTo != "peer-b" {
		t.Fatalf("post-reroute fire ForwardedTo = %q, want peer-b", lastFired[0].ForwardedTo)
	}
}

// TestHopAckBudget_StoreUpsertSameForwardedToDoesNotResetBudget — the
// store() upsert that does NOT change ForwardedTo (e.g. duplicate
// retry stamping the same peer) must not reset the budget. Resetting
// on every same-peer upsert would let a retry-loop near the deadline
// indefinitely defer the timeout fire.
func TestHopAckBudget_StoreUpsertSameForwardedToDoesNotResetBudget(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-retry",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 3,
	})

	rs.tickHopAckBudgets() // 3 → 2
	rs.tickHopAckBudgets() // 2 → 1

	// Same ForwardedTo upsert — should NOT reset.
	rs.store(&relayForwardState{
		MessageID:            "msg-retry",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: 99, // attempt to reset — must be ignored
		HopAckObserved:       false,
	})

	rs.mu.Lock()
	st := rs.states["msg-retry"]
	rs.mu.Unlock()
	if st.HopAckRemainingTicks != 1 {
		t.Fatalf("HopAckRemainingTicks = %d after same-peer upsert, want 1 (preserved)", st.HopAckRemainingTicks)
	}

	// Next tick should fire — original deadline preserved.
	fired := rs.tickHopAckBudgets()
	if len(fired) != 1 {
		t.Fatalf("expected fire on preserved deadline, got %v", fired)
	}
}

// TestHopAckBudget_MarkObservedNoopOnUnknownMessage — late ack for a
// message ID we never recorded (TTL eviction beat the ack) must
// return false without creating any phantom state.
func TestHopAckBudget_MarkObservedNoopOnUnknownMessage(t *testing.T) {
	rs := newRelayStateStore()
	if rs.markHopAckObserved("never-existed") {
		t.Fatal("markHopAckObserved returned true for unknown message ID")
	}
	if rs.count() != 0 {
		t.Fatalf("store count = %d after no-op, want 0", rs.count())
	}
}

// TestHopAckBudget_TTLEvictionInvalidatesPendingBudget — if the TTL
// ticker evicts an entry mid-budget (RemainingTTL elapsed first), the
// hop-ack ticker MUST NOT fire a stale callback for the evicted ID.
// Achieved structurally because tickHopAckBudgets iterates the live
// map, but the test pins the contract.
func TestHopAckBudget_TTLEvictionInvalidatesPendingBudget(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-short-ttl",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         2, // expires in 2 ticks
		HopAckRemainingTicks: 5, // would otherwise fire at tick 5
	})

	// Tick once — both counters decrement; no fire yet.
	rs.decrementTTLs()
	if fired := rs.tickHopAckBudgets(); len(fired) != 0 {
		t.Fatalf("tick 1: fired %v, want nil", fired)
	}
	// Tick again — RemainingTTL hits zero, decrementTTLs evicts.
	rs.decrementTTLs()
	if rs.count() != 0 {
		t.Fatalf("expected TTL eviction, count = %d", rs.count())
	}
	// Subsequent hop-ack ticks must NOT fire — the entry is gone.
	for i := 0; i < 10; i++ {
		if fired := rs.tickHopAckBudgets(); len(fired) != 0 {
			t.Fatalf("post-eviction tick fired %v, want nil", fired)
		}
	}
}

// TestHopAckBudget_OriginPathStampsBudget — a sanity check that the
// store-stamped budget IS present on entries that travelled through
// sendRelayMessage's origin path (call shape mirrored here). Pure
// integrity test; the value comes from defaultHopAckBudgetSeconds in
// production, and PR 12.2 keeps that constant unexported.
func TestHopAckBudget_OriginPathStampsBudget(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-origin",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         180,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
	})

	rs.mu.Lock()
	st := rs.states["msg-origin"]
	rs.mu.Unlock()
	if st.HopAckRemainingTicks != defaultHopAckBudgetSeconds {
		t.Fatalf("HopAckRemainingTicks = %d, want %d", st.HopAckRemainingTicks, defaultHopAckBudgetSeconds)
	}
	if st.HopAckObserved {
		t.Fatal("HopAckObserved must be false on initial stamp")
	}
}

// ---------------------------------------------------------------------
// Phase 3 PR 12.3 — recordFailoverRetry / retryAttemptCountFor.
// The helpers are the atomic boundary tryFailoverRelay crosses for
// state mutation; correctness here is what keeps the multi-path
// failover machinery from corrupting the abandoned-uplink list or
// re-arming the budget for the wrong attempt.
// ---------------------------------------------------------------------

// TestFailoverRetry_RecordTransitionsAllFieldsAtomically — the
// recordFailoverRetry helper must move the previous ForwardedTo into
// AbandonedForwardedTo, set the new ForwardedTo, increment
// RetryAttempt, re-arm the hop-ack budget, and clear HopAckObserved
// in a single critical section. A partial update would leave the
// state inconsistent and break the failover-loop invariants
// downstream (stale-ack guards, budget timer, MaxFailoverRetries
// gate).
func TestFailoverRetry_RecordTransitionsAllFieldsAtomically(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-fail",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RouteOrigin:          domaintest.ID("origin-x"),
		RemainingTTL:         60,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		HopAckObserved:       true, // simulate timeout-already-fired
		FrameLine:            "{\"type\":\"relay_message\"}\n",
	})

	if !rs.recordFailoverRetry("msg-fail", domain.PeerAddress("peer-b")) {
		t.Fatal("recordFailoverRetry returned false for live state")
	}

	rs.mu.Lock()
	st := rs.states["msg-fail"]
	rs.mu.Unlock()
	if st.ForwardedTo != "peer-b" {
		t.Fatalf("ForwardedTo = %q, want peer-b", st.ForwardedTo)
	}
	if len(st.AbandonedForwardedTo) != 1 || st.AbandonedForwardedTo[0] != "peer-a" {
		t.Fatalf("AbandonedForwardedTo = %v, want [peer-a]", st.AbandonedForwardedTo)
	}
	if st.RetryAttempt != 1 {
		t.Fatalf("RetryAttempt = %d, want 1", st.RetryAttempt)
	}
	if st.HopAckRemainingTicks != defaultHopAckBudgetSeconds {
		t.Fatalf("HopAckRemainingTicks = %d, want %d (re-armed)", st.HopAckRemainingTicks, defaultHopAckBudgetSeconds)
	}
	if st.HopAckObserved {
		t.Fatal("HopAckObserved must be false after retry (fresh window)")
	}
	if !st.RouteOrigin.IsZero() {
		t.Fatalf("RouteOrigin = %q, want empty (failover does not carry origin)", st.RouteOrigin.String())
	}
	// FrameLine must survive — the same payload is being retried.
	if st.FrameLine == "" {
		t.Fatal("FrameLine cleared by recordFailoverRetry; the retry payload would be lost")
	}
}

// TestFailoverRetry_RecordNoopOnUnknownMessage — a TTL eviction
// landing between the wire send and the bookkeeping call must not
// resurrect a phantom state. Helper returns false so the caller can
// log and move on.
func TestFailoverRetry_RecordNoopOnUnknownMessage(t *testing.T) {
	rs := newRelayStateStore()
	if rs.recordFailoverRetry("never-existed", "peer-b") {
		t.Fatal("recordFailoverRetry returned true on unknown message ID")
	}
	if rs.count() != 0 {
		t.Fatalf("store count = %d after no-op, want 0", rs.count())
	}
}

// TestFailoverRetry_RecordSamePeerDoesNotAppendAbandoned — a defensive
// guard: if the caller passes the SAME ForwardedTo (degenerate retry
// that picked the same uplink, e.g. due to a Lookup quirk), the helper
// must not append a duplicate AbandonedForwardedTo entry. The retry
// counter still bumps and the budget re-arms; the abandoned list stays
// clean so subsequent failover loops cannot wrongly skip that uplink.
func TestFailoverRetry_RecordSamePeerDoesNotAppendAbandoned(t *testing.T) {
	rs := newRelayStateStore()
	rs.store(&relayForwardState{
		MessageID:            "msg-same",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		HopAckObserved:       true,
	})

	if !rs.recordFailoverRetry("msg-same", domain.PeerAddress("peer-a")) {
		t.Fatal("recordFailoverRetry returned false for same-peer retry")
	}

	rs.mu.Lock()
	st := rs.states["msg-same"]
	rs.mu.Unlock()
	if len(st.AbandonedForwardedTo) != 0 {
		t.Fatalf("AbandonedForwardedTo = %v, want empty (same-peer retry must not append)", st.AbandonedForwardedTo)
	}
	if st.RetryAttempt != 1 {
		t.Fatalf("RetryAttempt = %d, want 1", st.RetryAttempt)
	}
	if st.HopAckObserved {
		t.Fatal("HopAckObserved must clear even on same-peer retry")
	}
}

// TestRetryAttemptCountFor_ReturnsCurrentValue — straightforward read
// path. Returns present=false for unknown IDs so the caller can
// distinguish "TTL eviction beat us" from "we have 0 retries so far".
func TestRetryAttemptCountFor_ReturnsCurrentValue(t *testing.T) {
	rs := newRelayStateStore()

	if count, present := rs.retryAttemptCountFor("never-existed"); present || count != 0 {
		t.Fatalf("unknown ID: got (%d, %v), want (0, false)", count, present)
	}

	rs.store(&relayForwardState{
		MessageID:            "msg-counted",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
		RetryAttempt:         3,
	})

	count, present := rs.retryAttemptCountFor("msg-counted")
	if !present {
		t.Fatal("retryAttemptCountFor returned present=false for live state")
	}
	if count != 3 {
		t.Fatalf("retryAttemptCountFor = %d, want 3", count)
	}
}

// TestFailoverRetry_AccumulatesAbandonedAcrossMultipleRetries — a
// future relaxation of MaxFailoverRetries (e.g. =2 or operator-tunable
// per §5 open question #4) must not break the abandoned-list
// accumulation. Each retry pushes the previous ForwardedTo onto the
// stack so handleRelayHopAck's stale-ack guard can reject late acks
// from any abandoned hop, not just the immediately-previous one.
func TestFailoverRetry_AccumulatesAbandonedAcrossMultipleRetries(t *testing.T) {
	rs := newRelayStateStore()

	rs.store(&relayForwardState{
		MessageID:            "msg-chain",
		ForwardedTo:          domain.PeerAddress("peer-a"),
		Recipient:            domaintest.ID("id-target"),
		RemainingTTL:         60,
		HopAckRemainingTicks: defaultHopAckBudgetSeconds,
	})

	if !rs.recordFailoverRetry("msg-chain", "peer-b") {
		t.Fatal("first retry returned false")
	}
	if !rs.recordFailoverRetry("msg-chain", "peer-c") {
		t.Fatal("second retry returned false")
	}

	rs.mu.Lock()
	st := rs.states["msg-chain"]
	rs.mu.Unlock()
	if st.ForwardedTo != "peer-c" {
		t.Fatalf("ForwardedTo = %q, want peer-c", st.ForwardedTo)
	}
	wantAbandoned := []domain.PeerAddress{"peer-a", "peer-b"}
	if len(st.AbandonedForwardedTo) != len(wantAbandoned) {
		t.Fatalf("AbandonedForwardedTo = %v, want %v", st.AbandonedForwardedTo, wantAbandoned)
	}
	for i, want := range wantAbandoned {
		if st.AbandonedForwardedTo[i] != want {
			t.Fatalf("AbandonedForwardedTo[%d] = %q, want %q", i, st.AbandonedForwardedTo[i], want)
		}
	}
	if st.RetryAttempt != 2 {
		t.Fatalf("RetryAttempt = %d, want 2", st.RetryAttempt)
	}
}
