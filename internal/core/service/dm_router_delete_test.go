package service

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// dispatchCounter is a thread-safe test recorder for control-DM
// dispatches. It implements the signature expected by
// DMRouter.dispatchControlDeleteFn so tests can count dispatches and
// inspect their (peer, target) tuples without touching the rpc /
// identity stack.
type dispatchCounter struct {
	mu    sync.Mutex
	calls []dispatchCall
	// failOnce, if non-zero, makes the n-th call (1-indexed) return a
	// non-nil error so tests can exercise the wire-failure branch.
	failOnce int
}

type dispatchCall struct {
	peer   domain.PeerIdentity
	target domain.MessageID
}

func (d *dispatchCounter) record(_ context.Context, peer domain.PeerIdentity, target domain.MessageID) error {
	d.mu.Lock()
	d.calls = append(d.calls, dispatchCall{peer: peer, target: target})
	n := len(d.calls)
	failOnce := d.failOnce
	d.mu.Unlock()
	if failOnce != 0 && n == failOnce {
		// Reuse a generic error type without importing extra packages.
		return errFakeDispatch{}
	}
	return nil
}

func (d *dispatchCounter) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

func (d *dispatchCounter) snapshot() []dispatchCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]dispatchCall, len(d.calls))
	copy(out, d.calls)
	return out
}

type errFakeDispatch struct{}

func (errFakeDispatch) Error() string { return "fake dispatch failure" }

// TestDeleteIntentBackoffDoublesToTheCap pins the schedule the sweep
// spaces retries by: doubling from the initial interval, then flat at
// the cap for as long as the intent lives.
func TestDeleteIntentBackoffDoublesToTheCap(t *testing.T) {
	t.Parallel()

	want := []time.Duration{
		deleteIntentRetryInitial,     // 1st attempt
		deleteIntentRetryInitial * 2, // 2nd
		deleteIntentRetryInitial * 4, // 3rd
		deleteIntentRetryInitial * 8, // 4th
	}
	for i, expected := range want {
		if got := deleteIntentBackoff(i + 1); got != expected {
			t.Errorf("deleteIntentBackoff(%d) = %v, want %v", i+1, got, expected)
		}
	}
	for _, attempts := range []int{12, 50, 500} {
		if got := deleteIntentBackoff(attempts); got != deleteIntentRetryCap {
			t.Errorf("deleteIntentBackoff(%d) = %v, want the cap %v", attempts, got, deleteIntentRetryCap)
		}
	}
	// A zero or negative attempt count is a caller bug, not a reason to
	// dispatch in a tight loop.
	if got := deleteIntentBackoff(0); got != deleteIntentRetryInitial {
		t.Errorf("deleteIntentBackoff(0) = %v, want %v", got, deleteIntentRetryInitial)
	}
}

// TestProcessDeleteRetryDueHoldsForAnUnreachablePeer is the heart of the
// scheduler: an intent whose peer cannot answer is never dispatched and
// never charged, so a deletion survives an arbitrarily long offline
// stretch — and it is PARKED rather than left due, because the sweep
// reads oldest-first under a limit and a pile of intents to one absent
// contact at the head of that queue would starve everyone else.
func TestProcessDeleteRetryDueHoldsForAnUnreachablePeer(t *testing.T) {
	t.Parallel()

	r, c, _, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	const target = domain.MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5")
	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now, NextAttemptAt: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	r.processDeleteRetryDue(context.Background(), now)

	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (the peer cannot answer)", got)
	}
	intent, found, err := store.DeleteIntentByID(context.Background(), target)
	if err != nil || !found {
		t.Fatalf("intent missing after a held sweep: found=%v err=%v", found, err)
	}
	if intent.Attempts != 0 {
		t.Errorf("attempts = %d, want 0 (an offline peer must not burn the schedule)", intent.Attempts)
	}
	if !intent.NextAttemptAt.After(now) {
		t.Errorf("next_attempt_at = %s, want it parked past %s so the sweep queue keeps moving", intent.NextAttemptAt, now)
	}

	// The peer comes back. The connect kick un-parks their intents, and
	// the next sweep sends.
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	r.reviveDeleteIntentsForPeer(peer)
	r.processDeleteRetryDue(context.Background(), time.Now().UTC())

	if got := counter.count(); got != 1 {
		t.Fatalf("dispatch count after the peer returned = %d, want 1", got)
	}
	intent, found, err = store.DeleteIntentByID(context.Background(), target)
	if err != nil || !found {
		t.Fatalf("intent missing after dispatch: found=%v err=%v", found, err)
	}
	if intent.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", intent.Attempts)
	}
}

// TestProcessDeleteRetryDuePacesOnePeer pins the per-peer quota. A bulk
// deletion leaves hundreds of intents due at once; firing them at one
// peer as fast as the sweep can read them is what its control-DM rate
// limiter would answer, turning the burst into rejections and burnt
// backoff. The overflow is parked, not charged.
func TestProcessDeleteRetryDuePacesOnePeer(t *testing.T) {
	t.Parallel()

	r, c, _, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	ids := []domain.MessageID{
		"11111111-2222-4333-8444-555555555551",
		"11111111-2222-4333-8444-555555555552",
		"11111111-2222-4333-8444-555555555553",
		"11111111-2222-4333-8444-555555555554",
		"11111111-2222-4333-8444-555555555555",
		"11111111-2222-4333-8444-555555555556",
	}
	for i, id := range ids {
		if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
			MessageID: id, Peer: peer, CreatedAt: now, NextAttemptAt: now.Add(-time.Duration(len(ids)-i) * time.Second),
		}); err != nil {
			t.Fatalf("NoteDeleteIntent %s: %v", id, err)
		}
	}

	r.processDeleteRetryDue(context.Background(), now)

	if got := counter.count(); got != deleteIntentPerPeerPerSweep {
		t.Fatalf("dispatch count = %d, want the per-peer quota %d", got, deleteIntentPerPeerPerSweep)
	}

	charged := 0
	for _, id := range ids {
		intent, found, err := store.DeleteIntentByID(context.Background(), id)
		if err != nil || !found {
			t.Fatalf("intent %s missing: found=%v err=%v", id, found, err)
		}
		if intent.Attempts > 0 {
			charged++
		}
		if !intent.NextAttemptAt.After(now) {
			t.Errorf("intent %s left due at %s; the sweep would re-read it immediately", id, intent.NextAttemptAt)
		}
	}
	if charged != deleteIntentPerPeerPerSweep {
		t.Errorf("charged attempts = %d, want %d (parked overflow must not be charged)", charged, deleteIntentPerPeerPerSweep)
	}
}

// TestALateRefusalIsNotShownForAChatTheUserHasCleared closes the loop that made
// this feature necessary in the first place.
//
// A per-message request goes out; the user then clears the chat; the peer's
// answer to the OLD request arrives afterwards and says `denied`. Nothing about
// excluding it from future sweeps helps — it was already on the wire. Publishing
// that answer puts "the peer refused the delete request" on a conversation the
// user has been told is gone on both sides.
//
// The wipe asks for that message too and is answered in its own right, so the
// late answer is dropped and the row stays: it is what refuses a re-delivery of
// the id until the wipe settles.
func TestALateRefusalIsNotShownForAChatTheUserHasCleared(t *testing.T) {
	t.Parallel()

	r, c, _, _ := newTestDMRouterForDelete(t)
	bus := ebus.New()
	var (
		outcomesMu sync.Mutex
		outcomes   []ebus.MessageDeleteOutcome
	)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		outcomesMu.Lock()
		outcomes = append(outcomes, o)
		outcomesMu.Unlock()
	}, ebus.WithSync())
	r.eventBus = bus

	ctx := context.Background()
	const target = domain.MessageID("e4f5a6b7-c8d9-4e0f-8a1b-c2d3e4f5a6b7")
	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	// The request the user made before clearing the chat, already dispatched.
	if err := store.NoteDeleteIntent(ctx, chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now, NextAttemptAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	// And the wipe, which from here asks for everything including that message.
	if err := store.NoteConversationDeleteIntent(ctx, chatlog.DeleteIntent{
		Kind: chatlog.DeleteIntentConversation, Peer: peer,
		RequestID: domain.ConversationDeleteRequestID("11111111-2222-4333-8444-555555555555"),
		CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteConversationDeleteIntent: %v", err)
	}

	ack, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   domain.MessageDeleteStatusDenied,
	})
	if err != nil {
		t.Fatalf("marshal the ack: %v", err)
	}
	r.handleInboundMessageDeleteAck(peer, ack)

	outcomesMu.Lock()
	published := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(published) != 0 {
		t.Errorf("a refusal was reported for a cleared chat: %+v", published)
	}
	// The row stays: it is the refusal of that id until the wipe is answered.
	if _, found, err := store.DeleteIntentByID(ctx, target); err != nil || !found {
		t.Errorf("the late answer took the request the wipe carries: found=%v err=%v", found, err)
	}
}

// TestAnAnswerForAnAlreadyRetiredRequestIsDropped pins the third line of the
// rule: an answer for a request we have already retired is dropped.
//
// The row can go while the answer is in flight — the wipe that carried it is
// answered first, or an earlier copy of the same ack settled it. Reporting it
// anyway shows the user a refusal for a deletion nothing is waiting for.
func TestAnAnswerForAnAlreadyRetiredRequestIsDropped(t *testing.T) {
	t.Parallel()

	r, c, _, _ := newTestDMRouterForDelete(t)
	bus := ebus.New()
	var (
		outcomesMu sync.Mutex
		outcomes   []ebus.MessageDeleteOutcome
	)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		outcomesMu.Lock()
		outcomes = append(outcomes, o)
		outcomesMu.Unlock()
	}, ebus.WithSync())
	r.eventBus = bus

	ctx := context.Background()
	const target = domain.MessageID("f5a6b7c8-d9e0-4f1a-8b2c-d3e4f5a6b7c8")
	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(ctx, chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	ack, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   domain.MessageDeleteStatusDenied,
	})
	if err != nil {
		t.Fatalf("marshal the ack: %v", err)
	}
	// The row goes between the handler's read of the intent and its drop —
	// which is what a wipe answered in that window does.
	r.beforeDropDeleteIntentForTest = func() {
		if _, err := store.DropDeleteIntent(ctx, target); err != nil {
			t.Errorf("dropping the row from under the handler: %v", err)
		}
	}
	r.handleInboundMessageDeleteAck(peer, ack)

	outcomesMu.Lock()
	published := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(published) != 0 {
		t.Errorf("an answer for a retired request was reported: %+v", published)
	}
}

// TestTheSweepDoesNotWriteOffARequestAWipeHasTakenOver pins the same rule on
// the other path.
//
// The sweep works from a snapshot. A wipe written after that snapshot was read
// takes over the requests made before it — and the sweep, holding a row with an
// exhausted budget, would otherwise write that row off: delete it (with the
// durable refusal of the id), and tell the user the deletion was abandoned,
// while the wipe that carries it is still going to be delivered.
func TestTheSweepDoesNotWriteOffARequestAWipeHasTakenOver(t *testing.T) {
	t.Parallel()

	r, c, _, _ := newTestDMRouterForDelete(t)
	bus := ebus.New()
	var (
		outcomesMu sync.Mutex
		outcomes   []ebus.MessageDeleteOutcome
	)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		outcomesMu.Lock()
		outcomes = append(outcomes, o)
		outcomesMu.Unlock()
	}, ebus.WithSync())
	r.eventBus = bus

	ctx := context.Background()
	const target = domain.MessageID("a6b7c8d9-e0f1-4a2b-8c3d-e4f5a6b7c8d9")
	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(ctx, chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now.Add(-time.Hour),
		NextAttemptAt: now.Add(-time.Minute), Attempts: deleteIntentGiveUpAttempts,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	// The snapshot the sweep would be holding.
	due, err := store.DueDeleteIntents(ctx, now, 10)
	if err != nil {
		t.Fatalf("DueDeleteIntents: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("the fixture did not produce one due request: %d", len(due))
	}
	// The user clears the chat after that read.
	if err := store.NoteConversationDeleteIntent(ctx, chatlog.DeleteIntent{
		Kind: chatlog.DeleteIntentConversation, Peer: peer,
		RequestID: domain.ConversationDeleteRequestID("22222222-3333-4444-8555-666666666666"),
		CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteConversationDeleteIntent: %v", err)
	}

	if !r.expireDeleteIntent(ctx, store, due[0], now) {
		t.Fatal("the sweep kept working on a request a wipe had taken over")
	}

	if _, found, err := store.DeleteIntentByID(ctx, target); err != nil || !found {
		t.Errorf("the sweep deleted a request the wipe carries: found=%v err=%v", found, err)
	}
	outcomesMu.Lock()
	published := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	for _, outcome := range published {
		if outcome.Abandoned {
			t.Errorf("the sweep reported a deletion as abandoned while a wipe carries it: %+v", outcome)
		}
	}
}

// TestProcessDeleteRetryDueExpiresAnUnansweredIntent pins the only way
// an intent dies without an ack: the peer had the whole TTL and never
// answered. The user hears about it through the Abandoned outcome —
// their copy is gone, the peer's may not be.
func TestProcessDeleteRetryDueExpiresAnUnansweredIntent(t *testing.T) {
	t.Parallel()

	r, c, _, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	bus := ebus.New()
	var (
		outcomesMu sync.Mutex
		outcomes   []ebus.MessageDeleteOutcome
	)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		outcomesMu.Lock()
		outcomes = append(outcomes, o)
		outcomesMu.Unlock()
	}, ebus.WithSync())
	r.eventBus = bus

	const target = domain.MessageID("b2c3d4e5-f6a7-4b8c-9d0e-f1a2b3c4d5e6")
	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
		MessageID:     target,
		Peer:          peer,
		CreatedAt:     now.Add(-30 * 24 * time.Hour),
		NextAttemptAt: now.Add(-time.Minute),
		Attempts:      deleteIntentGiveUpAttempts,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	r.processDeleteRetryDue(context.Background(), now)

	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (an expired intent is dropped, not re-sent)", got)
	}
	if _, found, err := store.DeleteIntentByID(context.Background(), target); err != nil || found {
		t.Fatalf("expired intent survived the sweep: found=%v err=%v", found, err)
	}

	outcomesMu.Lock()
	published := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(published) != 1 {
		t.Fatalf("outcome count = %d, want 1 Abandoned publication", len(published))
	}
	outcome := published[0]
	if !outcome.Abandoned || outcome.Status != "" {
		t.Errorf("outcome = %+v, want Abandoned with an empty status", outcome)
	}
	if outcome.Target != target || outcome.Peer != peer {
		t.Errorf("outcome addressed %s/%s, want %s/%s", outcome.Target, outcome.Peer, target, peer)
	}
	if outcome.Attempts != deleteIntentGiveUpAttempts {
		t.Errorf("outcome.Attempts = %d, want the full budget %d", outcome.Attempts, deleteIntentGiveUpAttempts)
	}
}

// TestGivingUpOnADeletionLeavesNothingInTheLog pins where the failure is
// reported when a deletion is written off.
//
// Failure lines normally stay visible, because a support case must be able to
// see what went wrong. This one does not need to: the user is told on their own
// screen, by the Abandoned outcome. What the line would leave behind instead is
// a permanent note that a deletion was wanted and never delivered — written
// after the request that justified it has been dropped, so it is no longer the
// unfinished work that a durable note is allowed to be.
func TestGivingUpOnADeletionLeavesNothingInTheLog(t *testing.T) {
	var captured bytes.Buffer
	restoreLogger := log.Logger
	log.Logger = zerolog.New(&captured).Level(zerolog.TraceLevel)
	t.Cleanup(func() { log.Logger = restoreLogger })

	r, c, _, _ := newTestDMRouterForDelete(t)
	var abandoned bool
	bus := ebus.New()
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		abandoned = abandoned || o.Abandoned
	}, ebus.WithSync())
	r.eventBus = bus

	const target = domain.MessageID("c3d4e5f6-a7b8-4c9d-8e0f-a1b2c3d4e5f6")
	peer := domaintest.ID("peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()
	asked := now.Add(-30 * 24 * time.Hour)

	if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
		MessageID:     target,
		Peer:          peer,
		CreatedAt:     asked,
		NextAttemptAt: now.Add(-time.Minute),
		Attempts:      deleteIntentGiveUpAttempts,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	captured.Reset()

	r.processDeleteRetryDue(context.Background(), now)

	written := captured.String()
	if strings.Contains(written, "giving up on the peer-side deletion") {
		t.Error("the log states that a deletion was wanted and never delivered")
	}
	if strings.Contains(written, asked.Format(time.RFC3339)) {
		t.Error("the moment the user asked for the deletion is in the log")
	}
	if strings.Contains(written, string(target)) || strings.Contains(written, peer.String()) {
		t.Error("an identifier of the abandoned deletion is in the log")
	}
	// And the user is told, which is why the line is not needed.
	if !abandoned {
		t.Error("nothing told the user their deletion was abandoned")
	}
}

// TestProcessDeleteRetryDueKeepsANeverAskedIntent pins what the give-up
// budget is spent on. A contact who is away for a month is exactly the
// case the durable intent exists for; a calendar deadline would run out
// on them and report "abandoned" about a peer nobody managed to ask.
// Attempts only accrue when the peer was there to be asked.
func TestProcessDeleteRetryDueKeepsANeverAskedIntent(t *testing.T) {
	t.Parallel()

	r, c, _, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	const target = domain.MessageID("c3d4e5f6-a7b8-4c9d-8e0f-a1b2c3d4e5f6")
	peer := domaintest.ID("absent-peer")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
		MessageID:     target,
		Peer:          peer,
		CreatedAt:     now.Add(-365 * 24 * time.Hour),
		NextAttemptAt: now.Add(-time.Minute),
		Attempts:      0,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	r.processDeleteRetryDue(context.Background(), now)

	intent, found, err := store.DeleteIntentByID(context.Background(), target)
	if err != nil {
		t.Fatalf("DeleteIntentByID: %v", err)
	}
	if !found {
		t.Fatal("an intent that was never dispatched was written off")
	}
	if intent.Hold != chatlog.HoldPeerAbsent {
		t.Error("the intent was not parked; an unreachable peer must cost no attempt")
	}
	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (the peer is unreachable)", got)
	}
}

// TestProcessDeleteRetryDueKeepsAnAskedIntentBelowTheBudget pins the
// other side of the same rule: attempts that HAVE gone out do not write
// the request off until the budget is actually spent, however old the
// request is.
func TestProcessDeleteRetryDueKeepsAnAskedIntentBelowTheBudget(t *testing.T) {
	t.Parallel()

	r, c, _, _ := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	const target = domain.MessageID("d4e5f6a7-b8c9-4d0e-8f1a-b2c3d4e5f6a7")
	store := c.chatlog.Store()
	now := time.Now().UTC()

	if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
		MessageID:     target,
		Peer:          domaintest.ID("peer"),
		CreatedAt:     now.Add(-365 * 24 * time.Hour),
		NextAttemptAt: now.Add(-time.Minute),
		Attempts:      deleteIntentGiveUpAttempts - 1,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	r.processDeleteRetryDue(context.Background(), now)

	if _, found, err := store.DeleteIntentByID(context.Background(), target); err != nil || !found {
		t.Fatalf("an intent one attempt short of the budget was written off: found=%v err=%v", found, err)
	}
}

// TestApplyInboundDeleteOnRealChatlog covers the receiver-side
// authorization + DELETE path against a real chatlog.Store. The
// scenarios pin the contract from docs/dm-commands.md §"Authorization":
//
//   - Authorized sender-delete from M.Sender → row removed, ack=deleted.
//   - sender-delete from M.Recipient → row preserved, ack=denied.
//   - immutable flag from anyone → row preserved, ack=immutable.
//   - any-delete from either participant → row removed, ack=deleted.
//   - any-delete from an outsider → row preserved, ack=denied.
//   - target ID absent → ack=not_found, no error log noise.
//
// applyInboundDelete is the pure decision core; replyMessageDeleteAck
// (which would dispatch a control DM via rpc) is invoked only by
// handleInboundMessageDelete higher up. Calling applyInboundDelete
// directly here lets us assert the chatlog-side behaviour without
// standing up the rpc round-trip.
func TestApplyInboundDeleteOnRealChatlog(t *testing.T) {
	t.Parallel()

	c, id := newTestDesktopClientWithNode(t)

	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		// uiEvents is used by notify(); buffered so the test does not
		// need a consumer goroutine. Overflow is silently dropped via
		// notify's `default` branch — fine for assertion purposes.
		uiEvents:    make(chan UIEvent, 32),
		startupDone: make(chan struct{}),
	}

	store := c.chatlog.Store()
	if store == nil {
		t.Fatal("chatlog store is nil; test setup is wrong")
	}

	myAddr := domain.PeerIdentityFromWire(id.Address)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	insert := func(t *testing.T, id, sender, recipient string, flag protocol.MessageFlag) {
		t.Helper()
		entry := chatlog.Entry{
			ID:        id,
			Sender:    sender,
			Recipient: recipient,
			Body:      "ciphertext-stand-in",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Flag:      string(flag),
		}
		// Use the conversation peer (the "other" party) as the owner
		// passed to AppendReportNew. For an outgoing row that's the
		// recipient; for an incoming row it's the sender.
		owner := domain.PeerIdentityFromWire(recipient)
		if domain.PeerIdentityFromWire(sender) != myAddr {
			owner = domain.PeerIdentityFromWire(sender)
		}
		if _, err := c.chatlog.AppendReportNew(context.Background(), "dm", owner, entry); err != nil {
			t.Fatalf("AppendReportNew(%s): %v", id, err)
		}
	}

	cases := []struct {
		name           string
		targetID       string
		insertEntry    bool
		entrySender    domain.PeerIdentity
		entryRecipient domain.PeerIdentity
		entryFlag      protocol.MessageFlag
		envelopeSender domain.PeerIdentity
		wantStatus     domain.MessageDeleteStatus
		wantRowAfter   bool
	}{
		{
			// The peer asks us to delete a message WE sent them, under
			// the default flag. Only the author decides the fate of
			// their own words, so the row stays and the ack says so.
			name:           "sender-delete refuses the recipient",
			targetID:       "11111111-2222-4333-8444-555555555555",
			insertEntry:    true,
			entrySender:    myAddr,
			entryRecipient: peer,
			entryFlag:      protocol.MessageFlagSenderDelete,
			envelopeSender: peer,
			wantStatus:     domain.MessageDeleteStatusDenied,
			wantRowAfter:   true,
		},
		{
			name:           "somebody outside the conversation is refused",
			targetID:       "66666666-7777-4888-8999-aaaaaaaaaaaa",
			insertEntry:    true,
			entrySender:    myAddr,
			entryRecipient: peer,
			entryFlag:      protocol.MessageFlagAnyDelete,
			envelopeSender: domaintest.ID("stranger"),
			wantStatus:     domain.MessageDeleteStatusDenied,
			wantRowAfter:   true,
		},
		{
			name:           "the author may still delete their own message",
			targetID:       "22222222-3333-4444-8555-666666666666",
			insertEntry:    true,
			entrySender:    peer,
			entryRecipient: myAddr,
			entryFlag:      protocol.MessageFlagSenderDelete,
			envelopeSender: peer, // peer (the original sender) asks to delete their own message.
			wantStatus:     domain.MessageDeleteStatusDeleted,
			wantRowAfter:   false,
		},
		{
			name:           "immutable flag refuses everyone",
			targetID:       "33333333-4444-4555-8666-777777777777",
			insertEntry:    true,
			entrySender:    peer,
			entryRecipient: myAddr,
			entryFlag:      protocol.MessageFlagImmutable,
			envelopeSender: peer,
			wantStatus:     domain.MessageDeleteStatusImmutable,
			wantRowAfter:   true,
		},
		{
			name:           "any-delete authorized from recipient",
			targetID:       "44444444-5555-4666-8777-888888888888",
			insertEntry:    true,
			entrySender:    peer,
			entryRecipient: myAddr,
			entryFlag:      protocol.MessageFlagAnyDelete,
			envelopeSender: peer, // any-delete: original sender asks → still allowed.
			wantStatus:     domain.MessageDeleteStatusDeleted,
			wantRowAfter:   false,
		},
		{
			name:           "absent target replies not_found",
			targetID:       "55555555-6666-4777-8888-999999999999",
			insertEntry:    false,
			envelopeSender: peer,
			wantStatus:     domain.MessageDeleteStatusNotFound,
			wantRowAfter:   false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.insertEntry {
				insert(t, tc.targetID, tc.entrySender.String(), tc.entryRecipient.String(), tc.entryFlag)
			}

			status := r.applyInboundDelete(tc.envelopeSender, domain.MessageID(tc.targetID))
			if status != tc.wantStatus {
				t.Errorf("status = %s, want %s", status, tc.wantStatus)
			}

			_, rowFound, err := store.EntryByID(context.Background(), domain.MessageID(tc.targetID))
			if err != nil {
				t.Fatalf("EntryByID after applyInboundDelete: %v", err)
			}
			if rowFound != tc.wantRowAfter {
				t.Errorf("row present after = %v, want %v (status was %s)", rowFound, tc.wantRowAfter, status)
			}
		})
	}
}

// TestHandleInboundMessageDeleteAckSettlesTheIntent covers the
// sender-side reply handler. The local row is already gone by the time
// any ack arrives, so what the ack decides is the fate of the durable
// intent: every terminal status retires it, because none of them gets
// better by asking again. An ack from the wrong peer decides nothing —
// the intent stays scheduled for the peer we actually addressed.
func TestHandleInboundMessageDeleteAckSettlesTheIntent(t *testing.T) {
	t.Parallel()

	c, _ := newTestDesktopClientWithNode(t)

	store := c.chatlog.Store()
	if store == nil {
		t.Fatal("chatlog store is nil; test setup is wrong")
	}

	peer := domain.PeerIdentityFromWire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	cases := []struct {
		name       string
		targetID   string
		ackPeer    domain.PeerIdentity
		ackStatus  domain.MessageDeleteStatus
		wantIntent bool
	}{
		{
			name:      "deleted retires the intent",
			targetID:  "abc11111-2222-4333-8444-555555555555",
			ackPeer:   peer,
			ackStatus: domain.MessageDeleteStatusDeleted,
		},
		{
			name:      "not_found retires the intent",
			targetID:  "abc22222-3333-4444-8555-666666666666",
			ackPeer:   peer,
			ackStatus: domain.MessageDeleteStatusNotFound,
		},
		{
			name:      "denied retires the intent too",
			targetID:  "abc33333-4444-4555-8666-777777777777",
			ackPeer:   peer,
			ackStatus: domain.MessageDeleteStatusDenied,
		},
		{
			name:      "immutable retires the intent too",
			targetID:  "abc44444-5555-4666-8777-888888888888",
			ackPeer:   peer,
			ackStatus: domain.MessageDeleteStatusImmutable,
		},
		{
			name:       "ack from the wrong peer keeps the intent scheduled",
			targetID:   "abc55555-6666-4777-8888-999999999999",
			ackPeer:    domaintest.ID("imposter"),
			ackStatus:  domain.MessageDeleteStatusDeleted,
			wantIntent: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &DMRouter{
				client:         c,
				seenMessageIDs: make(map[string]struct{}),
				peers:          make(map[domain.PeerIdentity]*RouterPeerState),
				peerGen:        make(map[domain.PeerIdentity]uint64),
				cache:          NewConversationCache(),
			}

			now := time.Now().UTC()
			if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
				MessageID: domain.MessageID(tc.targetID), Peer: peer, CreatedAt: now, NextAttemptAt: now,
			}); err != nil {
				t.Fatalf("NoteDeleteIntent: %v", err)
			}

			payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
				TargetID: domain.MessageID(tc.targetID),
				Status:   tc.ackStatus,
			})
			if err != nil {
				t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
			}

			r.handleInboundMessageDeleteAck(tc.ackPeer, payload)

			_, found, err := store.DeleteIntentByID(context.Background(), domain.MessageID(tc.targetID))
			if err != nil {
				t.Fatalf("DeleteIntentByID: %v", err)
			}
			if found != tc.wantIntent {
				t.Errorf("intent present after the ack = %v, want %v (status %s from %s)",
					found, tc.wantIntent, tc.ackStatus, tc.ackPeer)
			}
		})
	}
}

// TestHandleInboundMessageDeleteAckKeepsTheIntentOnATransientFailure
// pins the one status that settles nothing. A peer whose chatlog was
// unavailable has NOT told us the message is gone — retiring the intent
// on that answer strands the message on their side with nobody left to
// ask. The row survives untouched, and the UI is told nothing because
// nothing finished: the dispatch that provoked this answer already
// charged its attempt, and counting the answer as a second one would
// burn the schedule at double rate.
func TestHandleInboundMessageDeleteAckKeepsTheIntentOnATransientFailure(t *testing.T) {
	t.Parallel()

	c, _ := newTestDesktopClientWithNode(t)
	store := c.chatlog.Store()
	peer := domain.PeerIdentityFromWire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	const target = "abc66666-7777-4888-8999-aaaaaaaaaaaa"

	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
	}

	now := time.Now().UTC()
	// The state one dispatch leaves behind: its attempt charged, its
	// backoff set.
	due := now.Add(deleteIntentBackoff(1))
	if err := store.NoteDeleteIntent(context.Background(), chatlog.DeleteIntent{
		MessageID: domain.MessageID(target), Peer: peer, CreatedAt: now, NextAttemptAt: due, Attempts: 1,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: domain.MessageID(target),
		Status:   domain.MessageDeleteStatusError,
	})
	if err != nil {
		t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
	}

	r.handleInboundMessageDeleteAck(peer, payload)

	intent, found, err := store.DeleteIntentByID(context.Background(), domain.MessageID(target))
	if err != nil {
		t.Fatalf("DeleteIntentByID: %v", err)
	}
	if !found {
		t.Fatal("a transient failure retired the intent; the message is now stranded on the peer")
	}
	if intent.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: the ack answers the dispatch, it is not a second attempt", intent.Attempts)
	}
	if !intent.NextAttemptAt.Equal(due) {
		t.Errorf("next attempt = %s, want the dispatch's own %s left untouched", intent.NextAttemptAt, due)
	}
}

// TestApplyInboundDeleteReportsTransientStorageFailure pins the other
// end of the same rule: a receiver that cannot reach its own chatlog
// must not answer "not_found", which reads as "your message is not
// here" and retires the sender's intent for good.
func TestApplyInboundDeleteReportsTransientStorageFailure(t *testing.T) {
	t.Parallel()

	c, _ := newTestDesktopClientWithNode(t)
	c.chatlog.setStoreForTest(nil)

	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
	}

	peer := domain.PeerIdentityFromWire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	status := r.applyInboundDelete(peer, domain.MessageID("abc77777-8888-4999-8aaa-bbbbbbbbbbbb"))
	if status != domain.MessageDeleteStatusError {
		t.Fatalf("status = %s, want %s", status, domain.MessageDeleteStatusError)
	}
}

// newTestDMRouterForDelete assembles a DMRouter bound to a real
// DesktopClient + chatlog (TempDir) with a counted dispatch hook.
// Used by the SendMessageDelete-branch tests below to exercise the
// public entry point without standing up the rpc/identity stack on
// the wire side.
func newTestDMRouterForDelete(t *testing.T) (*DMRouter, *DesktopClient, domain.PeerIdentity, *dispatchCounter) {
	t.Helper()
	c, id := newTestDesktopClientWithNode(t)
	counter := &dispatchCounter{}
	r := &DMRouter{
		client:                  c,
		seenMessageIDs:          make(map[string]struct{}),
		peers:                   make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:                 make(map[domain.PeerIdentity]uint64),
		cache:                   NewConversationCache(),
		uiEvents:                make(chan UIEvent, 32),
		startupDone:             make(chan struct{}),
		dispatchControlDeleteFn: counter.record,
		withdrawals:             newWithdrawalBacklog(),
	}
	r.wipeTombstones = c.wipeTombstones
	return r, c, domain.PeerIdentityFromWire(id.Address), counter
}

// deleteIntentFor reads the durable intent for a target, or reports its
// absence.
func deleteIntentFor(t *testing.T, c *DesktopClient, target string) (chatlog.DeleteIntent, bool) {
	t.Helper()
	intent, found, err := c.chatlog.Store().DeleteIntentByID(context.Background(), domain.MessageID(target))
	if err != nil {
		t.Fatalf("DeleteIntentByID(%s): %v", target, err)
	}
	return intent, found
}

func insertChatlogEntry(t *testing.T, gw *ChatlogGateway, owner domain.PeerIdentity, entry chatlog.Entry) {
	t.Helper()
	if _, err := gw.AppendReportNew(context.Background(), "dm", owner, entry); err != nil {
		t.Fatalf("AppendReportNew(%s): %v", entry.ID, err)
	}
}

// TestSendMessageDeleteOutgoingRemovesRowAndSchedulesPeer pins the
// contract for an outgoing row with the peer reachable: the local copy
// is gone the moment the call returns, one request is on its way, and a
// durable intent addressed to the row's recipient records what the peer
// still owes us.
func TestSendMessageDeleteOutgoingRemovesRowAndSchedulesPeer(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	const target = "10000000-2222-4444-8888-cccccccccccc"
	recipient := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, recipient, chatlog.Entry{
		ID:             target,
		Sender:         myAddr.String(),
		Recipient:      recipient.String(),
		Body:           "ciphertext",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		Flag:           string(protocol.MessageFlagSenderDelete),
		DeliveryStatus: protocol.ReceiptStatusDelivered,
	})

	route, err := r.SendMessageDelete(context.Background(), recipient, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteScheduled {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteScheduled)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil || found {
		t.Fatalf("row still present after the delete (err=%v, found=%v); the local copy must go at once", err, found)
	}

	if got := counter.count(); got != 1 {
		t.Fatalf("dispatch count = %d, want 1 (one immediate request to a reachable peer)", got)
	}

	intent, found := deleteIntentFor(t, c, target)
	if !found {
		t.Fatal("no delete intent recorded; nothing would ever re-ask the peer")
	}
	if intent.Peer != recipient {
		t.Errorf("intent.Peer = %s, want %s (derived from row.Recipient)", intent.Peer, recipient)
	}
	if intent.Attempts != 1 {
		t.Errorf("intent.Attempts = %d, want 1 (the immediate dispatch is charged)", intent.Attempts)
	}
	if !intent.NextAttemptAt.After(time.Now().UTC()) {
		t.Errorf("intent.NextAttemptAt = %s, want it in the future behind the backoff", intent.NextAttemptAt)
	}
}

// TestSendMessageDeleteIncomingIsQueuedWhateverTheFlagSays pins the P0
// the user hit: deleting a message somebody wrote to them must show up as
// a request to that peer, not vanish silently from their own screen.
//
// The row here carries sender-delete — what every message stored before
// the default changed carries, and what any peer on an older build still
// stamps. Reading that flag as "do not even ask" is what made the queue
// and its indicator invisible. The flag is the AUTHOR's answer, and it is
// delivered by their ack; deciding on their behalf that they would refuse
// only hides the request from the user.
func TestSendMessageDeleteIncomingIsQueuedWhateverTheFlagSays(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	const target = "20000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        target,
		Sender:    peer.String(),
		Recipient: myAddr.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagSenderDelete),
	})

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteScheduled {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteScheduled)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil {
		t.Fatalf("EntryByID: %v", err)
	} else if found {
		t.Fatal("row still present after deleting a received message")
	}

	if got := counter.count(); got != 1 {
		t.Errorf("dispatch count = %d, want 1: the author must be asked", got)
	}
	if _, found := deleteIntentFor(t, c, target); !found {
		t.Fatal("no delete intent recorded; the user would see nothing waiting on the peer")
	}
}

// TestSendMessageDeleteIncomingAnyDeleteAsksTheAuthor is the other half
// of the same rule: when the author DID grant it (any-delete), the same
// click also asks them to drop their copy.
func TestSendMessageDeleteIncomingAnyDeleteAsksTheAuthor(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	const target = "21000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        target,
		Sender:    peer.String(),
		Recipient: myAddr.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagAnyDelete),
	})

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteScheduled {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteScheduled)
	}
	if got := counter.count(); got != 1 {
		t.Errorf("dispatch count = %d, want 1", got)
	}
	if _, found := deleteIntentFor(t, c, target); !found {
		t.Error("no delete intent recorded for a row the author allows either side to delete")
	}
}

// TestSendMessageDeleteOverridesWrongCallerPeer pins that for a
// found row the caller-supplied peer is ignored and the actual
// conversation peer is derived from the chatlog entry. A buggy or
// malicious caller passing the wrong peer must not leak the deletion
// to a different conversation.
func TestSendMessageDeleteOverridesWrongCallerPeer(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)

	const target = "30000000-2222-4444-8888-cccccccccccc"
	realPeer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	strangerPeer := domain.PeerIdentityFromWire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	insertChatlogEntry(t, c.chatlog, realPeer, chatlog.Entry{
		ID:        target,
		Sender:    myAddr.String(),
		Recipient: realPeer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagSenderDelete),
	})

	// Caller passes the wrong peer. The router must override.
	if _, err := r.SendMessageDelete(context.Background(), strangerPeer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	calls := counter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("dispatch count = %d, want 1", len(calls))
	}
	if calls[0].peer != realPeer {
		t.Errorf("dispatch addressed to %s, want %s (caller's wrong peer must be overridden)", calls[0].peer, realPeer)
	}

	intent, found := deleteIntentFor(t, c, target)
	if !found {
		t.Fatal("delete intent missing")
	}
	if intent.Peer != realPeer {
		t.Errorf("intent.Peer = %s, want %s (must be the derived peer, not the caller-supplied one)", intent.Peer, realPeer)
	}
}

// TestSendMessageDeleteAbsentTargetSchedulesRecovery pins the !found
// case: with no local row the caller's peer is trusted (no derivation
// possible) and the peer-side deletion is scheduled anyway, so a delete
// re-issued for a row that is already gone on this side still converges
// the other one.
func TestSendMessageDeleteAbsentTargetSchedulesRecovery(t *testing.T) {
	t.Parallel()

	r, c, _, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	const target = "40000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	// No insert — the row is intentionally absent.

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete (recovery !found): %v", err)
	}
	if route != domain.MessageDeleteRouteScheduled {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteScheduled)
	}

	if got := counter.count(); got != 1 {
		t.Fatalf("dispatch count = %d, want 1 (the recovery path must still ask the peer)", got)
	}

	intent, found := deleteIntentFor(t, c, target)
	if !found {
		t.Fatal("delete intent missing on the recovery path")
	}
	if intent.Peer != peer {
		t.Errorf("intent.Peer = %s, want %s (caller-supplied peer is trusted on !found)", intent.Peer, peer)
	}
	if intent.Attempts != 1 {
		t.Errorf("intent.Attempts = %d, want 1", intent.Attempts)
	}
}

// TestSendMessageDeleteImmutableRefuses pins the up-front Immutable
// gate: SendMessageDelete must refuse outright (returning an error
// and producing no dispatch / pending state) if the target row is
// flagged Immutable. The user-visible error reflects intent before
// any wire traffic happens.
func TestSendMessageDeleteImmutableRefuses(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)

	const target = "50000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        target,
		Sender:    myAddr.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagImmutable),
	})

	_, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err == nil {
		t.Fatal("SendMessageDelete on immutable row returned nil error; want refusal")
	}

	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (immutable must not produce any wire send)", got)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil || !found {
		t.Fatalf("immutable row missing after refusal (err=%v, found=%v); refusal must not mutate", err, found)
	}

	if _, found := deleteIntentFor(t, c, target); found {
		t.Error("immutable refusal recorded a delete intent; a refusal must not mutate anything")
	}
}

// insertUnconfirmedOutgoing inserts an outgoing row in the state every
// "still sending" message is in: accepted locally, never acknowledged by
// the recipient.
func insertUnconfirmedOutgoing(t *testing.T, c *DesktopClient, myAddr, peer domain.PeerIdentity, target string) {
	t.Helper()
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:             target,
		Sender:         myAddr.String(),
		Recipient:      peer.String(),
		Body:           "ciphertext",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		Flag:           string(protocol.MessageFlagSenderDelete),
		DeliveryStatus: "sent",
	})
}

// TestSendMessageDeleteWithdrawsUnsentMessageForOfflinePeer is the
// regression test for the reported gap: a message hanging in the
// sending state could not be deleted while the peer was offline. It is
// still in our own delivery queues, so the delete withdraws it — the row
// goes at once, nothing is sent to an unreachable peer, and the intent
// records what that peer still owes us in case a copy escaped before the
// cancellation landed.
func TestSendMessageDeleteWithdrawsUnsentMessageForOfflinePeer(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	const target = "60000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertUnconfirmedOutgoing(t, c, myAddr, peer, target)

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteWithdraw {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteWithdraw)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil {
		t.Fatalf("EntryByID: %v", err)
	} else if found {
		t.Fatal("row still present after withdrawal; the local copy must go at once")
	}

	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (there is nobody to ask yet)", got)
	}

	intent, found := deleteIntentFor(t, c, target)
	if !found {
		t.Fatal("no delete intent recorded; the peer would never be asked once they return")
	}
	if intent.Peer != peer {
		t.Errorf("intent.Peer = %s, want %s", intent.Peer, peer)
	}
	if intent.Attempts != 0 {
		t.Errorf("intent.Attempts = %d, want 0 (nothing was dispatched)", intent.Attempts)
	}
	if intent.NextAttemptAt.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("intent.NextAttemptAt = %s, want it due immediately so the next sweep picks it up", intent.NextAttemptAt)
	}
	if refused, _ := r.wipeTombstones.Refuses(domain.MessageID(target), time.Now().UTC()); !refused {
		t.Error("withdrawn id was not tombstoned; a late relay echo would resurrect the row")
	}
}

// TestSendMessageDeleteSurvivesAFailedCancellation pins the rule that a
// deletion is never held hostage by another subsystem: if the node
// cannot be reached to stop the delivery, the local copy still goes and
// the peer-side request is still scheduled, so a message that does
// escape is recalled. Refusing here would strand the user with an
// undeletable message in exactly the outage the feature exists for.
func TestSendMessageDeleteSurvivesAFailedCancellation(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, errFakeDispatch{}
	}

	const target = "70000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertUnconfirmedOutgoing(t, c, myAddr, peer, target)

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteWithdraw {
		t.Errorf("route = %q, want %q", route, domain.MessageDeleteRouteWithdraw)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil || found {
		t.Fatalf("row survived a failed cancellation (err=%v, found=%v); the user's copy must go regardless", err, found)
	}
	if _, found := deleteIntentFor(t, c, target); !found {
		t.Error("no intent recorded; a message that escapes the failed cancellation would never be recalled")
	}
	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (the peer is unreachable)", got)
	}
}

// TestSendMessageDeleteRecallsAMessageThatNeverLeftTheNode pins the one
// case where nothing is scheduled: the node proved the envelope never
// reached the wire, so the recipient has never seen it. Asking them to
// delete it would announce a message they never received.
func TestSendMessageDeleteRecallsAMessageThatNeverLeftTheNode(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	r.client.freezeMessageDeliveryFn = func(context.Context, domain.MessageID) (bool, error) {
		return true, nil
	}

	bus := ebus.New()
	var (
		outcomesMu sync.Mutex
		outcomes   []ebus.MessageDeleteOutcome
	)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		outcomesMu.Lock()
		outcomes = append(outcomes, o)
		outcomesMu.Unlock()
	}, ebus.WithSync())
	r.eventBus = bus

	const target = "c0000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertUnconfirmedOutgoing(t, c, myAddr, peer, target)

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteRecalled {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteRecalled)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil || found {
		t.Fatalf("row still present (err=%v, found=%v)", err, found)
	}
	if _, found := deleteIntentFor(t, c, target); found {
		t.Error("an intent was scheduled for a message the peer never received")
	}
	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0", got)
	}

	outcomesMu.Lock()
	published := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(published) != 1 || published[0].Status != domain.MessageDeleteStatusDeleted {
		t.Fatalf("outcomes = %+v, want one terminal deleted publication", published)
	}
}

// TestSendMessageDeleteSchedulesConfirmedRowForOfflinePeer is the case
// the user cannot be denied: the peer holds a confirmed copy and is
// offline. The local copy still goes immediately, and the request to
// remove theirs waits in the intent table for them to come back.
func TestSendMessageDeleteSchedulesConfirmedRowForOfflinePeer(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	const target = "80000000-2222-4444-8888-cccccccccccc"
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:             target,
		Sender:         myAddr.String(),
		Recipient:      peer.String(),
		Body:           "ciphertext",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		Flag:           string(protocol.MessageFlagSenderDelete),
		DeliveryStatus: protocol.ReceiptStatusDelivered,
	})

	route, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteScheduled {
		t.Fatalf("route = %q, want %q", route, domain.MessageDeleteRouteScheduled)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(context.Background(), domain.MessageID(target)); err != nil || found {
		t.Fatalf("row still present (err=%v, found=%v); an offline peer must not keep the local copy alive", err, found)
	}
	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (the peer is unreachable)", got)
	}
	if _, found := deleteIntentFor(t, c, target); !found {
		t.Fatal("no delete intent recorded; the peer's copy would stay forever")
	}
}

// TestSendMessageDeletePublishesOnlyWhenTheDeletionIsFinished pins who
// owns the terminal outcome. A deletion that still owes the peer a
// request announces nothing: saying "deleted" up front would make the
// UI overwrite its own "scheduled" caption with a promise nobody has
// kept yet. Only a recall — where the node proved the message never
// went out — is finished when the call returns.
func TestSendMessageDeletePublishesOnlyWhenTheDeletionIsFinished(t *testing.T) {
	t.Parallel()

	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	bus := ebus.New()
	var (
		outcomesMu sync.Mutex
		outcomes   []ebus.MessageDeleteOutcome
	)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(o ebus.MessageDeleteOutcome) {
		outcomesMu.Lock()
		outcomes = append(outcomes, o)
		outcomesMu.Unlock()
	}, ebus.WithSync())
	r.eventBus = bus

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const outgoing = "a0000000-2222-4444-8888-cccccccccccc"
	insertUnconfirmedOutgoing(t, c, myAddr, peer, outgoing)
	if _, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(outgoing)); err != nil {
		t.Fatalf("SendMessageDelete (outgoing): %v", err)
	}

	const incoming = "b0000000-2222-4444-8888-cccccccccccc"
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        incoming,
		Sender:    peer.String(),
		Recipient: myAddr.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagAnyDelete),
	})
	if _, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(incoming)); err != nil {
		t.Fatalf("SendMessageDelete (incoming): %v", err)
	}

	outcomesMu.Lock()
	pending := len(outcomes)
	outcomesMu.Unlock()
	if pending != 0 {
		t.Fatalf("outcome count = %d, want 0 while both deletions still owe the peer a request", pending)
	}

	// A recall owes nobody anything, so it is final on return.
	r.client.freezeMessageDeliveryFn = func(context.Context, domain.MessageID) (bool, error) {
		return true, nil
	}
	const recalled = "b1000000-2222-4444-8888-cccccccccccc"
	insertUnconfirmedOutgoing(t, c, myAddr, peer, recalled)
	if _, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(recalled)); err != nil {
		t.Fatalf("SendMessageDelete (recalled): %v", err)
	}

	outcomesMu.Lock()
	published := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(published) != 1 {
		t.Fatalf("outcome count = %d, want exactly the recall's terminal publication", len(published))
	}
	if published[0].Target != domain.MessageID(recalled) || published[0].Status != domain.MessageDeleteStatusDeleted {
		t.Errorf("outcome = %+v, want deleted for %s", published[0], recalled)
	}
}

// TestRefreshPendingDeleteCountsPublishesTheBadge covers the only lasting
// feedback a scheduled deletion has. The row is gone the moment the user
// clicks, so a request handed to an offline peer is invisible unless this
// count reaches the snapshot the UI renders — and it has to fall back to
// zero when the peer settles, or the badge would outlive the request.
func TestRefreshPendingDeleteCountsPublishesTheBadge(t *testing.T) {
	t.Parallel()

	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	other := domain.PeerIdentityFromWire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	r.mu.Lock()
	r.peers[peer] = &RouterPeerState{}
	r.peers[other] = &RouterPeerState{}
	r.mu.Unlock()

	const first = "d1000000-2222-4444-8888-cccccccccccc"
	const second = "d2000000-2222-4444-8888-cccccccccccc"
	insertUnconfirmedOutgoing(t, c, myAddr, peer, first)
	insertUnconfirmedOutgoing(t, c, myAddr, peer, second)

	for _, target := range []string{first, second} {
		if _, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target)); err != nil {
			t.Fatalf("SendMessageDelete(%s): %v", target, err)
		}
	}

	r.mu.Lock()
	pending := r.peers[peer].PendingDeletes
	untouched := r.peers[other].PendingDeletes
	r.mu.Unlock()
	if pending != 2 {
		t.Fatalf("PendingDeletes = %d, want 2", pending)
	}
	if untouched != 0 {
		t.Errorf("an unrelated peer shows %d pending deletions", untouched)
	}

	// The peer answers one of them; the badge follows.
	store := c.chatlog.Store()
	if _, err := store.DropDeleteIntent(context.Background(), domain.MessageID(first)); err != nil {
		t.Fatalf("DropDeleteIntent: %v", err)
	}
	r.refreshPendingDeleteCounts()

	r.mu.Lock()
	pending = r.peers[peer].PendingDeletes
	r.mu.Unlock()
	if pending != 1 {
		t.Fatalf("PendingDeletes after one settled = %d, want 1", pending)
	}
}

// TestAuthorizedToDelete pins the predicate at the heart of inbound
// message_delete handling. The matrix is small but security-critical: a
// mistake here either lets a peer wipe messages they did not author
// under the default flag, or blocks the deletions the author did allow.
func TestAuthorizedToDelete(t *testing.T) {
	t.Parallel()

	author := domaintest.ID("author")
	recipient := domaintest.ID("recipient")
	stranger := domaintest.ID("stranger")

	tests := []struct {
		name           string
		flag           protocol.MessageFlag
		envelopeSender domain.PeerIdentity
		want           bool
	}{
		{"sender-delete: the author may", protocol.MessageFlagSenderDelete, author, true},
		{"sender-delete: the recipient may not", protocol.MessageFlagSenderDelete, recipient, false},
		{"empty flag falls back to sender-delete", "", recipient, false},
		{"an unknown flag falls back to sender-delete", "invented-by-a-future-version", recipient, false},
		{"auto-delete-ttl behaves as sender-delete", protocol.MessageFlagAutoDeleteTTL, recipient, false},
		{"any-delete: the author may", protocol.MessageFlagAnyDelete, author, true},
		{"any-delete: the recipient may too", protocol.MessageFlagAnyDelete, recipient, true},
		{"any-delete: an outsider may not", protocol.MessageFlagAnyDelete, stranger, false},
		{"immutable refuses the author", protocol.MessageFlagImmutable, author, false},
		{"immutable refuses the recipient", protocol.MessageFlagImmutable, recipient, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := authorizedToDelete(tc.flag, tc.envelopeSender, author, recipient); got != tc.want {
				t.Fatalf("authorizedToDelete(%q, %s) = %v, want %v", tc.flag, tc.envelopeSender, got, tc.want)
			}
		})
	}
}

// TestApplyInboundDeleteRefusesATargetItNeverSaw pins what happens when
// a delete overtakes the message it is about. The DM may still be in a
// relay's buffer; answering not_found and forgetting the id would let
// that copy land minutes later and stay forever, while the sender treats
// not_found as success and retires the request, leaving nobody to ask
// again.
//
// The refusal is in MEMORY, and this test also pins what that costs: nothing
// on disk names the id. A list of "messages this node was asked to delete and
// never had" is a record of the PEER's deletions kept on our side, past the
// moment either of us needed it, and the window it would close — a copy landing
// after this process restarts — is the price of not keeping one.
func TestApplyInboundDeleteRefusesATargetItNeverSaw(t *testing.T) {
	t.Parallel()

	r, c, _, _ := newTestDMRouterForDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("30000000-2222-4444-8888-cccccccccccc")

	if status := r.applyInboundDelete(peer, target); status != domain.MessageDeleteStatusNotFound {
		t.Fatalf("status = %s, want %s", status, domain.MessageDeleteStatusNotFound)
	}

	if refused, _ := r.wipeTombstones.Refuses(target, time.Now().UTC()); !refused {
		t.Fatal("the id was not refused; a late delivery of it would settle in permanently")
	}
	owed, err := c.chatlog.Store().OwedDeleteIntentMessageIDs(context.Background())
	if err != nil {
		t.Fatalf("OwedDeleteIntentMessageIDs: %v", err)
	}
	if len(owed) != 0 {
		t.Errorf("a message we never had was written down as a deletion: %v", owed)
	}
}

// TestStoreRefusesAReDeliveryOfADeletedMessage pins the door the refusal
// has to guard. The node stores a message BEFORE it decides whether the
// message is news, and a replay of an id already in its backlog is not
// news — it returns early and the router never sees an event. Checking
// the refusal only in the router therefore misses the one path that
// actually resurrects the row: the write itself.
func TestStoreRefusesAReDeliveryOfADeletedMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("40000000-2222-4444-8888-cccccccccccc")

	envelope := protocol.Envelope{
		ID:        protocol.MessageID(target),
		Topic:     "dm",
		Sender:    peer.String(),
		Recipient: myAddr.String(),
		Payload:   []byte("ciphertext"),
		CreatedAt: time.Now().UTC(),
	}

	// It stores while nothing refuses it.
	if got := c.store.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want inserted", got)
	}

	// The user deletes it; the refusal is recorded.
	if _, err := r.SendMessageDelete(ctx, peer, target); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	// A relay hands the same envelope over again.
	if got := c.store.StoreMessage(envelope, false); got != node.StoreDuplicate {
		t.Fatalf("StoreMessage after the delete = %v, want duplicate", got)
	}
	if _, found, err := c.chatlog.Store().EntryByID(ctx, target); err != nil || found {
		t.Fatalf("the deleted message was written back: found=%v err=%v", found, err)
	}
}

// TestTheRequestItselfRefusesTheReplayAfterARestart pins the durable half of
// the replay defence — and, in its second act, exactly how far it goes.
//
// Nothing on this disk records a deletion any more. What survives a restart is
// the REQUEST: while the peer has not confirmed erasing their copy, this node
// is openly carrying "delete this id", and a process that reloads that work
// list recognises the id for free. When the peer answers, the row goes, and the
// id stops being refused — by design, because the peer's copy is gone and the
// replay has no source left.
func TestTheRequestItselfRefusesTheReplayAfterARestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("50000000-2222-4444-8888-cccccccccccc")

	envelope := protocol.Envelope{
		ID:        protocol.MessageID(target),
		Topic:     "dm",
		Sender:    peer.String(),
		Recipient: myAddr.String(),
		Payload:   []byte("ciphertext"),
		CreatedAt: time.Now().UTC(),
	}
	if got := c.store.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want inserted", got)
	}
	// The peer is unreachable, so the request stays owed — the state this test
	// is about.
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }
	if _, err := r.SendMessageDelete(ctx, peer, target); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	// A new process over the same database: nothing of the previous run's
	// memory, only what the database holds.
	store := c.chatlog.Store()
	restarted := newWipeTombstoneSet(func() deleteTaskList { return store })
	restarted.Hydrate(ctx, time.Now().UTC())
	afterRestart := NewMessageStoreAdapter(
		NewChatlogGateway(store, myAddr), c.id, restarted, newRemovalGate())

	if got := afterRestart.StoreMessage(envelope, false); got != node.StoreDuplicate {
		t.Fatalf("StoreMessage after the restart = %v, want duplicate: the owed request should still refuse it", got)
	}
	if _, found, err := store.EntryByID(ctx, target); err != nil || found {
		t.Fatalf("a replay resurrected the message across a restart: found=%v err=%v", found, err)
	}

	// The peer confirms. The request goes, and with it the last thing here that
	// knew this id — which is the whole point, and the cost: a replay from a
	// relay that never saw our receipt is accepted from now on.
	if _, err := store.DropDeleteIntent(ctx, target); err != nil {
		t.Fatalf("DropDeleteIntent: %v", err)
	}
	settled := newWipeTombstoneSet(func() deleteTaskList { return store })
	settled.Hydrate(ctx, time.Now().UTC())
	if refused, known := settled.Refuses(target, time.Now().UTC()); refused || !known {
		t.Fatalf("after the peer answered: refused=%v known=%v, want the id forgotten entirely", refused, known)
	}

	// And this is the consequence, written down rather than argued in a
	// comment: a copy delivered from here on IS stored again.
	//
	// It is the price of the contract and not an oversight. The only ways to
	// refuse this envelope are a durable list of the ids this node has deleted
	// — the trace the whole design exists to remove — or telling relays which
	// ids to drop, which hands a third party the fact we refuse to write down
	// about ourselves. The window needs a relay that never managed to deliver
	// its copy (so it never got our ack for it) AND a restart on this side; the
	// user can delete the message again.
	//
	// If this assertion ever starts failing, something began remembering
	// deletions across restarts, and that is the thing to go looking at.
	afterSettlement := NewMessageStoreAdapter(
		NewChatlogGateway(store, myAddr), c.id, settled, newRemovalGate())
	if got := afterSettlement.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want it accepted: the accepted gap is not what this test describes any more", got)
	}
}

// TestDeleteReadsTheRowsProofWhenTheNodeHasForgotten is the case the
// in-memory answer alone cannot cover. A message to a peer who never came
// back spends its whole retry budget failing, and the node then drops the
// entry — after which the cancellation reports "no entry", which is
// indistinguishable from "already emitted". Deleting such a message would
// announce its id to a peer who never received it.
//
// The row knows better, and it is still here at the moment of the delete.
func TestDeleteReadsTheRowsProofWhenTheNodeHasForgotten(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, counter := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	const target = "20000000-2222-4444-8888-cccccccccccc"
	recipient := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, recipient, chatlog.Entry{
		ID:        target,
		Sender:    myAddr.String(),
		Recipient: recipient.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagAnyDelete),
	})
	if err := c.chatlog.Store().MarkNeverEmitted(ctx, []domain.MessageID{domain.MessageID(target)}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	// The node has no entry left, so its answer proves nothing.
	r.client.freezeMessageDeliveryFn = func(context.Context, domain.MessageID) (bool, error) {
		return false, nil
	}

	route, err := r.SendMessageDelete(ctx, recipient, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteRecalled {
		t.Fatalf("route = %q, want %q: the row proves nobody has ever seen this message", route, domain.MessageDeleteRouteRecalled)
	}
	if _, found := deleteIntentFor(t, c, target); found {
		t.Error("the peer is being asked to delete a message that never left this node")
	}
	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0", got)
	}
}

// TestFailedWithdrawalKeepsTheMessageFrozenAndOwed replaces an older
// expectation, and the difference is what the freeze bought.
//
// Before it, a failed withdrawal meant the delivery was still the node's
// to send, so the delete had to schedule a request even for a message the
// row proved had never gone out — the peer was told an id they could not
// resolve. Now the freeze is taken before the row is read and is NOT
// released on a failed withdrawal, so the message cannot reach anyone:
// `recalled` stays true, no request is written, and the withdrawal is
// remembered so the sweep can finish it.
func TestFailedWithdrawalKeepsTheMessageFrozenAndOwed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return false }

	const target = "30000000-2222-4444-8888-cccccccccccc"
	recipient := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	insertChatlogEntry(t, c.chatlog, recipient, chatlog.Entry{
		ID:        target,
		Sender:    myAddr.String(),
		Recipient: recipient.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagAnyDelete),
	})
	if err := c.chatlog.Store().MarkNeverEmitted(ctx, []domain.MessageID{domain.MessageID(target)}); err != nil {
		t.Fatalf("MarkNeverEmitted: %v", err)
	}

	// The freeze holds, so the row's proof is trustworthy; the withdrawal
	// that would tidy up after it does not.
	r.client.freezeMessageDeliveryFn = func(context.Context, domain.MessageID) (bool, error) {
		return false, nil
	}
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{}, errors.New("node unreachable")
	}
	thawed := 0
	r.client.thawConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) error {
		thawed++
		return nil
	}

	route, err := r.SendMessageDelete(ctx, recipient, domain.MessageID(target))
	if err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	if route != domain.MessageDeleteRouteRecalled {
		t.Fatalf("route = %q, want %q: nothing can send a frozen message", route, domain.MessageDeleteRouteRecalled)
	}
	if _, found := deleteIntentFor(t, c, target); found {
		t.Error("the peer is being asked about a message that cannot reach them")
	}
	if thawed != 0 {
		t.Error("the freeze was released although the withdrawal failed; the message could still go out")
	}
	if owed := r.withdrawals.size(); owed != 1 {
		t.Fatalf("owed withdrawals = %d, want 1: nothing else will ever name this id", owed)
	}

	// The node recovers and the sweep finishes what the delete could not.
	r.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		return ConversationCancellation{Cancelled: 1}, nil
	}
	r.retryOwedWithdrawals(ctx)
	if owed := r.withdrawals.size(); owed != 0 {
		t.Errorf("owed withdrawals after the retry = %d, want 0", owed)
	}
}

// TestFreezeIsReleasedOnEveryEarlyExit: the freeze has no TTL, so an exit
// that leaves it standing stops that message being sent for the life of
// the process, with nothing able to release it. Immutable messages and a
// database that blinks are both ordinary — neither may cost a message.
func TestFreezeIsReleasedOnEveryEarlyExit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	var thawed []domain.MessageID
	r.client.thawConversationDeliveryFn = func(_ context.Context, _ domain.PeerIdentity, ids []domain.MessageID) error {
		thawed = append(thawed, ids...)
		return nil
	}
	r.client.freezeMessageDeliveryFn = func(context.Context, domain.MessageID) (bool, error) {
		return false, nil
	}

	const immutable = "40000000-2222-4444-8888-cccccccccccc"
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        immutable,
		Sender:    myAddr.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagImmutable),
	})

	if _, err := r.SendMessageDelete(ctx, peer, domain.MessageID(immutable)); err == nil {
		t.Fatal("an immutable message must be refused")
	}
	if len(thawed) != 1 || string(thawed[0]) != immutable {
		t.Fatalf("thawed = %v, want the refused id: a freeze with no TTL was left standing", thawed)
	}

	// A row that cannot be read at all takes the same exit.
	thawed = nil
	const unreadable = "50000000-2222-4444-8888-cccccccccccc"
	owner := domain.PeerIdentityFromWire(myAddr.String())
	r.client.chatlog = NewChatlogGateway(newClosedChatlogStore(t, owner), owner)
	if _, err := r.SendMessageDelete(ctx, peer, domain.MessageID(unreadable)); err == nil {
		t.Fatal("a delete over a dead database must fail")
	}
	if len(thawed) != 1 || string(thawed[0]) != unreadable {
		t.Fatalf("thawed = %v, want the unreadable id", thawed)
	}
}

// TestFreezeIsReleasedEvenWhenTheCallerCancelled: the compensation runs
// precisely when the caller's context is already dead — a lookup or a
// transaction that ended in Canceled is the likeliest way to reach it.
// Handing it that same context refuses the thaw at the moment it matters,
// and the freeze has no TTL to save the message afterwards.
func TestFreezeIsReleasedEvenWhenTheCallerCancelled(t *testing.T) {
	t.Parallel()

	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	// The error is sampled INSIDE the call: the helper cancels its own
	// context on return, so reading it afterwards would always say
	// "canceled" and prove nothing.
	thawCalled := false
	var thawErr error
	r.client.thawConversationDeliveryFn = func(ctx context.Context, _ domain.PeerIdentity, _ []domain.MessageID) error {
		thawCalled = true
		thawErr = ctx.Err()
		return thawErr
	}
	r.client.freezeMessageDeliveryFn = func(context.Context, domain.MessageID) (bool, error) {
		return false, nil
	}

	const target = "60000000-2222-4444-8888-cccccccccccc"
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        target,
		Sender:    myAddr.String(),
		Recipient: peer.String(),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagImmutable),
	})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := r.SendMessageDelete(cancelled, peer, domain.MessageID(target)); err == nil {
		t.Fatal("an immutable message must be refused")
	}
	if !thawCalled {
		t.Fatal("the freeze was never released")
	}
	if thawErr != nil {
		t.Fatalf("the thaw inherited the caller's dead context: %v", thawErr)
	}
}

// Deleting one message takes its reactions out of the database, and the chips
// are drawn from a per-conversation cache that ONLY this event reloads. Without
// it the facts of a deleted message stay in the window's memory until the user
// leaves the chat — and if the same id is delivered again after its wipe
// tombstone expires, the new bubble is drawn with chips no row backs any more.
func TestDeletingAMessageTellsTheUIToReloadTheChips(t *testing.T) {
	t.Parallel()

	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	r.eventBus = ebus.New()
	t.Cleanup(r.eventBus.Shutdown)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const target = "9b111111-2222-4333-8444-555555555555"
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:             target,
		Sender:         myAddr.String(),
		Recipient:      peer.String(),
		Body:           "ciphertext",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		Flag:           string(protocol.MessageFlagSenderDelete),
		DeliveryStatus: protocol.ReceiptStatusDelivered,
	})

	reloaded := make(chan domain.PeerIdentity, 4)
	r.eventBus.Subscribe(ebus.TopicReactionsChanged, func(p domain.PeerIdentity) {
		reloaded <- p
	})

	if _, err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}
	select {
	case got := <-reloaded:
		if got != peer {
			t.Fatalf("the reload named %s, want %s", got, peer)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deleting a message never told the UI to reload the conversation's chips")
	}
}

// TestSettlingADeletionRetiresItFromTheWriteAheadLog pins the last step of a
// deletion, in the FILE.
//
// The request row is the one place the id legitimately survives its message —
// it is the job "have this deleted at the peer". When the peer answers, that
// row goes, and the id has no business being anywhere on this disk. But a
// deleted row lives on in the write-ahead log until a checkpoint retires its
// page, and nothing on the ack path used to ask for one: the id stayed legible
// in the sidecar until some later, unrelated deletion happened to trigger a
// truncation. The deletion had already been reported as finished.
//
// Nothing here calls CheckpointWAL. The production path has to ask for it, or
// the test fails — which is the whole point, since the earlier version of this
// check called it by hand and could not have noticed.
func TestSettlingADeletionRetiresItFromTheWriteAheadLog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	owner := domain.PeerIdentityFromWire(self.Address)
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := storage.Open(ctx, storage.Config{
		ExplicitPath: path, Owner: owner, Catalog: migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("open state database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store := testChatlogStore(database.Executor(), owner)

	client := &DesktopClient{id: self, appCfg: config.App{Version: "test"}, chatLog: store}
	client.wireSubServices()
	r := &DMRouter{
		client:         client,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		withdrawals:    newWithdrawalBacklog(),
	}
	r.wipeTombstones = client.wipeTombstones
	r.deleteCheckpoint = newDeleteCheckpointer(
		func() *chatlog.Store { return store }, r.opContext)
	// The coalescing window is a second in production; a test that waited it
	// out would be a second slower for nothing.
	r.deleteCheckpoint.delay = time.Millisecond

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("60000000-2222-4444-8888-cccccccccccc")
	insertChatlogEntry(t, client.chatlog, owner, chatlog.Entry{
		ID: string(target), Sender: owner.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	now := time.Now().UTC()
	if err := store.NoteDeleteIntent(ctx, chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}
	if _, err := store.DeleteByID(ctx, target); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}

	payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   domain.MessageDeleteStatusDeleted,
	})
	if err != nil {
		t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
	}
	r.handleInboundMessageDeleteAck(peer, payload)

	// No polling and no window. The checkpoint runs BEFORE the ack path
	// publishes its outcome, so by the time the call returns the pages are out
	// of the log — that is the whole point of the ordering, and a test that
	// waited would pass just as well on the version that only scheduled one.
	if seen := filesNaming(t, path, string(target)); seen != "" {
		t.Fatalf("the ack returned while the id was still in %s: the deletion was reported finished before the pages left the log", seen)
	}
}

// filesNaming reports which of the database files still contain the value, as
// raw bytes. Names the file so a failure says whether the page is in the main
// database or only in the log.
func filesNaming(t *testing.T, path, value string) string {
	t.Helper()
	found := make([]string, 0, 2)
	for _, file := range []string{path, path + "-wal"} {
		raw, err := os.ReadFile(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if bytes.Contains(raw, []byte(value)) {
			found = append(found, filepath.Base(file))
		}
	}
	return strings.Join(found, ", ")
}

// TestAnAckDoesNotReportSuccessWhileTheLocalCopyIsStillHere pins the ordering
// of the ack path against a database that refuses to write.
//
// The peer has confirmed deleting THEIR copy, which is what makes this
// dangerous: the request is the only thing left that would ever ask again, and
// the local row is the only copy the user can still see. Retiring the request
// and publishing "deleted" on a failed local delete leaves the message on this
// disk with nothing scheduled to remove it, and the user told it is gone.
//
// So a failure here changes nothing: the request stays, the UI is not told, and
// the sweep tries again.
func TestAnAckDoesNotReportSuccessWhileTheLocalCopyIsStillHere(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c, id, executor := newTestDesktopClientWithNodeAndDB(t)
	myAddr := domain.PeerIdentityFromWire(id.Address)
	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		withdrawals:    newWithdrawalBacklog(),
	}
	r.wipeTombstones = c.wipeTombstones
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("70000000-2222-4444-8888-cccccccccccc")

	insertChatlogEntry(t, c.chatlog, myAddr, chatlog.Entry{
		ID: string(target), Sender: myAddr.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	now := time.Now().UTC()
	if err := c.chatlog.Store().NoteDeleteIntent(ctx, chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	// The outcome reaches the UI through the bus, so that is where a false
	// "deleted" would be observed.
	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	outcomes := make(chan ebus.MessageDeleteOutcome, 4)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(outcome ebus.MessageDeleteOutcome) {
		outcomes <- outcome
	})
	r.eventBus = bus

	// The message row refuses to go, and ONLY the message row: a trigger that
	// aborts the delete leaves every other write — the request row above all —
	// working normally. That separation is the point. A closed database would
	// fail both halves and the test would pass for the wrong reason, since the
	// request would survive by accident rather than by decision.
	if _, err := executor.ExecContext(ctx, `
		CREATE TRIGGER refuse_message_delete BEFORE DELETE ON messages
		BEGIN SELECT RAISE(ABORT, 'the disk said no'); END`); err != nil {
		t.Fatalf("install the failing delete: %v", err)
	}

	payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   domain.MessageDeleteStatusDeleted,
	})
	if err != nil {
		t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
	}
	r.handleInboundMessageDeleteAck(peer, payload)

	select {
	case outcome := <-outcomes:
		t.Fatalf("the ack reported %+v while the local copy is still here", outcome)
	case <-time.After(200 * time.Millisecond):
	}

	// The message is still here — that is the premise — and so is the request
	// that will remove it.
	if _, found, err := c.chatlog.Store().EntryByID(ctx, target); err != nil || !found {
		t.Fatalf("the fixture did not hold: found=%v err=%v", found, err)
	}
	if _, found := deleteIntentFor(t, c, string(target)); !found {
		t.Fatal("the request was retired while the message it names is still on disk: nothing will ever remove it")
	}
}

// TestAnAckDoesNotReopenTheReplayWindow is the correction of a mistake made in
// review: the refusal of a deleted id was being lifted the moment the peer
// acknowledged the deletion.
//
// An ack says the peer removed the row from THEIR database. It says nothing
// about the copies of the envelope that may still be sitting in a relay's
// buffer or an inbox queue — and those are the only reason the refusal exists.
// Lifting it on the ack re-opened the window inside the same process: no
// restart, no exotic timing, just a late delivery of the copy that was always
// going to arrive.
//
// The entries expire on their own at the sender's reseed horizon, which is the
// moment a replay stops being possible. They are memory, not disk: the contract
// is that nothing is written down.
func TestAnAckDoesNotReopenTheReplayWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("80000000-2222-4444-8888-cccccccccccc")

	envelope := protocol.Envelope{
		ID:        protocol.MessageID(target),
		Topic:     "dm",
		Sender:    peer.String(),
		Recipient: myAddr.String(),
		Payload:   []byte("ciphertext"),
		CreatedAt: time.Now().UTC(),
	}
	if got := c.store.StoreMessage(envelope, false); got != node.StoreInserted {
		t.Fatalf("StoreMessage = %v, want inserted", got)
	}
	if _, err := r.SendMessageDelete(ctx, peer, target); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	// The peer confirms. This is the moment the refusal used to be dropped.
	payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   domain.MessageDeleteStatusDeleted,
	})
	if err != nil {
		t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
	}
	r.handleInboundMessageDeleteAck(peer, payload)

	// A relay hands the old envelope over, in the same process, right after.
	if got := c.store.StoreMessage(envelope, false); got != node.StoreDuplicate {
		t.Fatalf("StoreMessage after the ack = %v, want duplicate: the ack is the peer's database, not the queues", got)
	}
	if _, found, err := c.chatlog.Store().EntryByID(ctx, target); err != nil || found {
		t.Fatalf("a replay right after the ack resurrected the message: found=%v err=%v", found, err)
	}
}

// TestABusyLogIsNotReportedAsAFinishedDeletion is the checkpoint contract under
// the condition that actually breaks it: another reader is holding the
// write-ahead log, so `wal_checkpoint(TRUNCATE)` cannot run.
//
// The row is gone from the database — that part is durable — but the pages that
// held it are still legible in the -wal file. `deleted` is terminal for the
// requester: after it the request is retired and nothing anywhere looks at that
// id again, which would make "still readable in a sidecar" the final state. So
// the answer is `error`, the requester asks once more, and the next attempt
// finds nothing to delete and a log it can retire.
func TestABusyLogIsNotReportedAsAFinishedDeletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	myAddr := domain.PeerIdentityFromWire(self.Address)
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := storage.Open(ctx, storage.Config{
		ExplicitPath: path, Owner: myAddr, Catalog: migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("open state database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	executor := database.Executor()

	c := &DesktopClient{id: self, appCfg: config.App{Version: "test"}, chatLog: testChatlogStore(executor, myAddr)}
	c.wireSubServices()
	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		withdrawals:    newWithdrawalBacklog(),
	}
	r.wipeTombstones = c.wipeTombstones
	r.deleteCheckpoint = newDeleteCheckpointer(
		func() *chatlog.Store { return c.chatlog.Store() }, r.opContext)

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("90000000-2222-4444-8888-cccccccccccc")
	insertChatlogEntry(t, c.chatlog, myAddr, chatlog.Entry{
		ID: string(target), Sender: peer.String(), Recipient: myAddr.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})

	// A reader that holds the log open for the whole of the deletion.
	reader, err := executor.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("open the blocking reader: %v", err)
	}
	var one int
	if err := reader.QueryRowContext(ctx, `SELECT 1 FROM messages LIMIT 1`).Scan(&one); err != nil {
		t.Fatalf("the blocking reader read nothing: %v", err)
	}
	defer func() { _ = reader.Rollback() }()

	status := r.applyInboundDelete(peer, target)
	if status != domain.MessageDeleteStatusError {
		t.Fatalf("status = %s, want error: the log still holds the message and `deleted` is terminal", status)
	}
	// The row itself IS gone — the refusal is about the FILE, not the database.
	if _, found, err := c.chatlog.Store().EntryByID(ctx, target); err != nil || found {
		t.Fatalf("the message survived the delete: found=%v err=%v", found, err)
	}

	// The RETRY, with the reader still holding the log. The row is gone, so
	// this arrives at the "nothing to delete" path — which is terminal for the
	// sender, and must therefore refuse just as firmly. Answering not_found
	// here would close the request over a log that still holds the message,
	// undoing what the first attempt refused to do.
	if status := r.applyInboundDelete(peer, target); status != domain.MessageDeleteStatusError {
		t.Fatalf("the retry answered %s while the log was still busy: not_found is terminal", status)
	}
	if seen := filesNaming(t, path, string(target)); seen == "" {
		t.Fatal("the fixture proves nothing: the log no longer holds the message")
	}

	// The reader lets go, the peer asks again, and now it can be answered —
	// and only now are the bytes actually gone.
	if err := reader.Rollback(); err != nil {
		t.Fatalf("release the blocking reader: %v", err)
	}
	if status := r.applyInboundDelete(peer, target); status != domain.MessageDeleteStatusNotFound {
		t.Fatalf("the retry answered %s, want not_found once the log is free", status)
	}
	if seen := filesNaming(t, path, string(target)); seen != "" {
		t.Fatalf("not_found was answered while the id was still in %s", seen)
	}
}

// TestABusyLogStillReportsTheOutcomeToTheUser is the other side of the
// checkpoint contract, and it is the one a previous round got wrong.
//
// An answer that will be re-asked can be withheld: the ack this node sends a
// peer, because the peer asks again. A report to our OWN user cannot. By the
// time this path runs the request has been retired, so no sweep comes back and
// a repeat of the peer's ack is dropped as an answer to nothing — withholding
// the outcome means the pending indicator disappears and "the messages are
// deleted" is never said. The information is gone for good.
//
// So a log that will not truncate delays the truncation, not the answer.
func TestABusyLogStillReportsTheOutcomeToTheUser(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c, _, executor := newTestDesktopClientWithNodeAndDB(t)
	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		withdrawals:    newWithdrawalBacklog(),
	}
	r.wipeTombstones = c.wipeTombstones
	r.deleteCheckpoint = newDeleteCheckpointer(
		func() *chatlog.Store { return c.chatlog.Store() }, r.opContext)

	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })
	outcomes := make(chan ebus.MessageDeleteOutcome, 4)
	bus.Subscribe(ebus.TopicMessageDeleteCompleted, func(outcome ebus.MessageDeleteOutcome) {
		outcomes <- outcome
	})
	r.eventBus = bus

	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const target = domain.MessageID("a0000000-2222-4444-8888-cccccccccccc")
	now := time.Now().UTC()
	if err := c.chatlog.Store().NoteDeleteIntent(ctx, chatlog.DeleteIntent{
		MessageID: target, Peer: peer, CreatedAt: now, NextAttemptAt: now,
	}); err != nil {
		t.Fatalf("NoteDeleteIntent: %v", err)
	}

	// A reader holds the log for the whole of the ack.
	reader, err := executor.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("open the blocking reader: %v", err)
	}
	var one int
	if err := reader.QueryRowContext(ctx, `SELECT 1 FROM messages LIMIT 1`).Scan(&one); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("the blocking reader read nothing: %v", err)
	}
	defer func() { _ = reader.Rollback() }()

	payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
		TargetID: target,
		Status:   domain.MessageDeleteStatusDeleted,
	})
	if err != nil {
		t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
	}
	r.handleInboundMessageDeleteAck(peer, payload)

	select {
	case outcome := <-outcomes:
		if outcome.Target != target || outcome.Abandoned {
			t.Fatalf("outcome = %+v, want the settled deletion of %s", outcome, target)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the outcome was never published: the request is retired and nothing will report it again")
	}

	// And the request really is gone, which is what makes the lost outcome
	// unrecoverable rather than merely late.
	if _, found := deleteIntentFor(t, c, string(target)); found {
		t.Fatal("the fixture proves nothing: the request is still scheduled")
	}
}
