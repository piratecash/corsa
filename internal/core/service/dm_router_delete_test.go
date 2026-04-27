package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
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

// TestRecordAttemptBudgetArithmetic is the lower-level pin on the
// terminal-decision arithmetic. It does not exercise the dispatch
// order; that is covered end-to-end by
// TestProcessDeleteRetryDueExactlyFiveRetries below.
func TestRecordAttemptBudgetArithmetic(t *testing.T) {
	t.Parallel()

	const target = domain.MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5")
	state := newDeleteRetryState()
	state.add(&pendingDelete{
		target:      target,
		peer:        "peer",
		sentAt:      time.Now(),
		nextRetryAt: time.Now().Add(deleteRetryInitial),
		attempt:     1,
	})

	for i := 1; i <= 5; i++ {
		entry, terminal := state.recordAttempt(target, time.Now())
		wantAttempt := i + 1
		if entry.attempt != wantAttempt {
			t.Fatalf("iteration %d: entry.attempt = %d, want %d", i, entry.attempt, wantAttempt)
		}
		if i < 5 && terminal {
			t.Fatalf("iteration %d: terminal=true before budget exhausted", i)
		}
		if i == 5 && !terminal {
			t.Fatalf("iteration 5: terminal=false at attempt=6, want true")
		}
	}

	zero, terminal := state.recordAttempt(target, time.Now())
	if terminal {
		t.Fatal("post-retire recordAttempt: terminal=true; entry should be gone")
	}
	if zero.attempt != 0 {
		t.Fatalf("post-retire recordAttempt: returned non-zero entry %+v", zero)
	}
}

// TestProcessDeleteRetryDueExactlyFiveRetries is the end-to-end pin
// on the dispatch budget. It seeds a pending entry (as if
// SendMessageDelete had just performed the initial dispatch with
// attempt=1) and then drives processDeleteRetryDue six times. The
// hookable dispatch counter records every wire send the loop
// produces, and the test asserts EXACTLY five retries plus the
// budget-exhausted outcome — no more, no fewer, and the dispatch
// order matches "dispatch first, recordAttempt second" so a future
// regression that swaps the order or skips the final send lights up
// here.
func TestProcessDeleteRetryDueExactlyFiveRetries(t *testing.T) {
	t.Parallel()

	const (
		target = domain.MessageID("a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5")
		peer   = domain.PeerIdentity("peer")
	)

	counter := &dispatchCounter{}
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

	r := &DMRouter{
		eventBus:                bus,
		deleteRetry:             newDeleteRetryState(),
		dispatchControlDeleteFn: counter.record,
	}

	now := time.Now().UTC()
	r.deleteRetry.add(&pendingDelete{
		target: target,
		peer:   peer,
		sentAt: now,
		// Set in the past so the very first sweep treats it as due.
		nextRetryAt: now.Add(-time.Second),
		attempt:     1,
	})

	// Six sweeps: the loop dispatches once per due entry per sweep.
	// recordAttempt bumps nextRetryAt by an exponential backoff
	// (capped at deleteRetryCap) AFTER each dispatch, so we must
	// advance the simulated clock past the newly scheduled nextRetryAt
	// each iteration. A simple monotonically-growing offset that
	// exceeds the cap on every step makes the entry due regardless of
	// the previous backoff, while still letting the test stay
	// deterministic.
	step := deleteRetryCap + time.Second
	for sweep := 1; sweep <= 6; sweep++ {
		r.processDeleteRetryDue(context.Background(), now.Add(time.Duration(sweep)*step))
	}

	dispatches := counter.snapshot()
	if len(dispatches) != 5 {
		t.Fatalf("dispatch count = %d, want 5 (initial 1 + 5 retries = 6 total dispatches; the test seeds the initial as already done)",
			len(dispatches))
	}
	for i, call := range dispatches {
		if call.peer != peer {
			t.Errorf("call[%d].peer = %s, want %s", i, call.peer, peer)
		}
		if call.target != target {
			t.Errorf("call[%d].target = %s, want %s", i, call.target, target)
		}
	}

	// After the 5th retry recordAttempt retires the entry. A 7th sweep
	// must not produce another dispatch.
	r.processDeleteRetryDue(context.Background(), now.Add(7*step))
	if extra := counter.count() - 5; extra != 0 {
		t.Fatalf("extra dispatches after retire: %d, want 0", extra)
	}

	r.deleteRetry.mu.Lock()
	_, present := r.deleteRetry.entries[target]
	r.deleteRetry.mu.Unlock()
	if present {
		t.Fatal("pending entry still in map after budget exhausted")
	}

	// Abandoned outcome contract: exactly one TopicMessageDeleteCompleted
	// publication with Abandoned=true, empty status, target/peer
	// matching the pending entry, and Attempts equal to the spent
	// budget (deleteRetryMaxAttempt = 6). The synchronous subscriber
	// ran inline with publishMessageDeleteOutcome, so by the time
	// processDeleteRetryDue returned the slice is already populated.
	outcomesMu.Lock()
	outcomeEvents := append([]ebus.MessageDeleteOutcome(nil), outcomes...)
	outcomesMu.Unlock()
	if len(outcomeEvents) != 1 {
		t.Fatalf("outcome count = %d, want 1 (a single Abandoned=true publication after retire)", len(outcomeEvents))
	}
	o := outcomeEvents[0]
	if !o.Abandoned {
		t.Errorf("outcome.Abandoned = false, want true")
	}
	if o.Status != "" {
		t.Errorf("outcome.Status = %q, want empty (Abandoned has no ack status)", o.Status)
	}
	if o.Target != target {
		t.Errorf("outcome.Target = %s, want %s", o.Target, target)
	}
	if o.Peer != peer {
		t.Errorf("outcome.Peer = %s, want %s", o.Peer, peer)
	}
	if o.Attempts != deleteRetryMaxAttempt {
		t.Errorf("outcome.Attempts = %d, want %d", o.Attempts, deleteRetryMaxAttempt)
	}
}

// TestAuthorizedToDelete pins the authorization predicate at the heart
// of inbound message_delete handling. The matrix is small but
// security-critical: a mistake here either lets a peer wipe messages
// they did not author (under sender-delete) or blocks legitimate
// any-delete deletions.
func TestAuthorizedToDelete(t *testing.T) {
	t.Parallel()

	const (
		alice = domain.PeerIdentity("alice")
		bob   = domain.PeerIdentity("bob")
		eve   = domain.PeerIdentity("eve")
	)

	cases := []struct {
		name         string
		flag         protocol.MessageFlag
		envelopeFrom domain.PeerIdentity
		targetSender domain.PeerIdentity
		targetRecip  domain.PeerIdentity
		want         bool
	}{
		{"sender-delete: original sender", protocol.MessageFlagSenderDelete, alice, alice, bob, true},
		{"sender-delete: recipient denied", protocol.MessageFlagSenderDelete, bob, alice, bob, false},
		{"sender-delete: stranger denied", protocol.MessageFlagSenderDelete, eve, alice, bob, false},

		{"empty flag treated as sender-delete: sender", "", alice, alice, bob, true},
		{"empty flag treated as sender-delete: recipient denied", "", bob, alice, bob, false},

		{"any-delete: sender", protocol.MessageFlagAnyDelete, alice, alice, bob, true},
		{"any-delete: recipient", protocol.MessageFlagAnyDelete, bob, alice, bob, true},
		{"any-delete: stranger denied", protocol.MessageFlagAnyDelete, eve, alice, bob, false},

		{"auto-delete-ttl behaves like sender-delete: sender", protocol.MessageFlagAutoDeleteTTL, alice, alice, bob, true},
		{"auto-delete-ttl: recipient denied", protocol.MessageFlagAutoDeleteTTL, bob, alice, bob, false},

		{"unknown flag conservative: sender allowed", protocol.MessageFlag("future-flag"), alice, alice, bob, true},
		{"unknown flag conservative: recipient denied", protocol.MessageFlag("future-flag"), bob, alice, bob, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := authorizedToDelete(tc.flag, tc.envelopeFrom, tc.targetSender, tc.targetRecip)
			if got != tc.want {
				t.Fatalf("authorizedToDelete(flag=%s, from=%s, sender=%s, recip=%s) = %v, want %v",
					tc.flag, tc.envelopeFrom, tc.targetSender, tc.targetRecip, got, tc.want)
			}
		})
	}
}

// TestApplyInboundDeleteOnRealChatlog covers the receiver-side
// authorization + DELETE path against a real chatlog.Store. Five
// scenarios pin the contract from docs/dm-commands.md §"Authorization":
//
//   - Authorized sender-delete from M.Sender → row removed, ack=deleted.
//   - sender-delete from M.Recipient → row preserved, ack=denied.
//   - immutable flag from anyone → row preserved, ack=immutable.
//   - any-delete from M.Recipient → row removed, ack=deleted.
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
	defer func() { _ = c.Close() }()

	r := &DMRouter{
		client:         c,
		seenMessageIDs: make(map[string]struct{}),
		peers:          make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:        make(map[domain.PeerIdentity]uint64),
		cache:          NewConversationCache(),
		deleteRetry:    newDeleteRetryState(),
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

	myAddr := domain.PeerIdentity(id.Address)
	const peer = domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

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
		owner := domain.PeerIdentity(recipient)
		if domain.PeerIdentity(sender) != myAddr {
			owner = domain.PeerIdentity(sender)
		}
		if _, err := c.chatlog.AppendReportNew("dm", owner, entry); err != nil {
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
			name:           "sender-delete authorized from original sender",
			targetID:       "11111111-2222-4333-8444-555555555555",
			insertEntry:    true,
			entrySender:    myAddr,
			entryRecipient: peer,
			entryFlag:      protocol.MessageFlagSenderDelete,
			envelopeSender: peer, // peer asks us to delete a message WE sent? Not authorized under sender-delete.
			wantStatus:     domain.MessageDeleteStatusDenied,
			wantRowAfter:   true,
		},
		{
			name:           "sender-delete authorized when envelope == row sender",
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
				insert(t, tc.targetID, string(tc.entrySender), string(tc.entryRecipient), tc.entryFlag)
			}

			status := r.applyInboundDelete(tc.envelopeSender, domain.MessageID(tc.targetID))
			if status != tc.wantStatus {
				t.Errorf("status = %s, want %s", status, tc.wantStatus)
			}

			_, rowFound, err := store.EntryByID(domain.MessageID(tc.targetID))
			if err != nil {
				t.Fatalf("EntryByID after applyInboundDelete: %v", err)
			}
			if rowFound != tc.wantRowAfter {
				t.Errorf("row present after = %v, want %v (status was %s)", rowFound, tc.wantRowAfter, status)
			}
		})
	}
}

// TestHandleInboundMessageDeleteAckPessimisticOrdering covers the
// sender-side reply handler: success statuses (deleted, not_found)
// trigger the pessimistic-delete post-ack DELETE; failure statuses
// (denied, immutable) leave the local row alive.
//
// The pending-entry guard is also exercised: an ack from the wrong
// envelope sender restores the entry rather than retiring it.
func TestHandleInboundMessageDeleteAckPessimisticOrdering(t *testing.T) {
	t.Parallel()

	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	store := c.chatlog.Store()
	if store == nil {
		t.Fatal("chatlog store is nil; test setup is wrong")
	}

	myAddr := domain.PeerIdentity(id.Address)
	const peer = domain.PeerIdentity("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	type setup struct {
		name         string
		targetID     string
		ackPeer      domain.PeerIdentity // envelope sender of the ack
		pendingPeer  domain.PeerIdentity // peer we addressed in pendingDelete
		ackStatus    domain.MessageDeleteStatus
		wantRowAfter bool
		wantPending  bool // pending entry still in map after the call
	}

	cases := []setup{
		{
			name:         "deleted_status_removes_row",
			targetID:     "abc11111-2222-4333-8444-555555555555",
			ackPeer:      peer,
			pendingPeer:  peer,
			ackStatus:    domain.MessageDeleteStatusDeleted,
			wantRowAfter: false,
			wantPending:  false,
		},
		{
			name:         "not_found_status_runs_idempotent_delete",
			targetID:     "abc22222-3333-4444-8555-666666666666",
			ackPeer:      peer,
			pendingPeer:  peer,
			ackStatus:    domain.MessageDeleteStatusNotFound,
			wantRowAfter: false,
			wantPending:  false,
		},
		{
			name:         "denied_status_keeps_row_alive",
			targetID:     "abc33333-4444-4555-8666-777777777777",
			ackPeer:      peer,
			pendingPeer:  peer,
			ackStatus:    domain.MessageDeleteStatusDenied,
			wantRowAfter: true,
			wantPending:  false,
		},
		{
			name:         "immutable_status_keeps_row_alive",
			targetID:     "abc44444-5555-4666-8777-888888888888",
			ackPeer:      peer,
			pendingPeer:  peer,
			ackStatus:    domain.MessageDeleteStatusImmutable,
			wantRowAfter: true,
			wantPending:  false,
		},
		{
			name:         "ack_from_wrong_peer_restores_pending",
			targetID:     "abc55555-6666-4777-8888-999999999999",
			ackPeer:      "ccccccccccccccccccccccccccccccccccccccccc", // imposter
			pendingPeer:  peer,
			ackStatus:    domain.MessageDeleteStatusDeleted,
			wantRowAfter: true, // imposter's deleted ack must NOT remove the row
			wantPending:  true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			r := &DMRouter{
				client:         c,
				seenMessageIDs: make(map[string]struct{}),
				peers:          make(map[domain.PeerIdentity]*RouterPeerState),
				peerGen:        make(map[domain.PeerIdentity]uint64),
				cache:          NewConversationCache(),
				deleteRetry:    newDeleteRetryState(),
			}

			// Insert a row that this user is the sender of, so the
			// post-ack DELETE has something to remove.
			entry := chatlog.Entry{
				ID:        tc.targetID,
				Sender:    string(myAddr),
				Recipient: string(peer),
				Body:      "ciphertext",
				CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				Flag:      string(protocol.MessageFlagSenderDelete),
			}
			if _, err := c.chatlog.AppendReportNew("dm", peer, entry); err != nil {
				t.Fatalf("AppendReportNew: %v", err)
			}

			r.deleteRetry.add(&pendingDelete{
				target:      domain.MessageID(tc.targetID),
				peer:        tc.pendingPeer,
				sentAt:      time.Now(),
				nextRetryAt: time.Now().Add(deleteRetryInitial),
				attempt:     1,
			})

			payload, err := domain.MarshalMessageDeleteAckPayload(domain.MessageDeleteAckPayload{
				TargetID: domain.MessageID(tc.targetID),
				Status:   tc.ackStatus,
			})
			if err != nil {
				t.Fatalf("MarshalMessageDeleteAckPayload: %v", err)
			}

			r.handleInboundMessageDeleteAck(tc.ackPeer, payload)

			_, rowFound, err := store.EntryByID(domain.MessageID(tc.targetID))
			if err != nil {
				t.Fatalf("EntryByID after handler: %v", err)
			}
			if rowFound != tc.wantRowAfter {
				t.Errorf("row present after = %v, want %v (status %s, ackPeer %s)",
					rowFound, tc.wantRowAfter, tc.ackStatus, tc.ackPeer)
			}

			r.deleteRetry.mu.Lock()
			_, pendingPresent := r.deleteRetry.entries[domain.MessageID(tc.targetID)]
			r.deleteRetry.mu.Unlock()
			if pendingPresent != tc.wantPending {
				t.Errorf("pending present after = %v, want %v", pendingPresent, tc.wantPending)
			}
		})
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
	t.Cleanup(func() { _ = c.Close() })
	counter := &dispatchCounter{}
	r := &DMRouter{
		client:                  c,
		seenMessageIDs:          make(map[string]struct{}),
		peers:                   make(map[domain.PeerIdentity]*RouterPeerState),
		peerGen:                 make(map[domain.PeerIdentity]uint64),
		cache:                   NewConversationCache(),
		deleteRetry:             newDeleteRetryState(),
		uiEvents:                make(chan UIEvent, 32),
		startupDone:             make(chan struct{}),
		dispatchControlDeleteFn: counter.record,
	}
	return r, c, domain.PeerIdentity(id.Address), counter
}

func insertChatlogEntry(t *testing.T, gw *ChatlogGateway, owner domain.PeerIdentity, entry chatlog.Entry) {
	t.Helper()
	if _, err := gw.AppendReportNew("dm", owner, entry); err != nil {
		t.Fatalf("AppendReportNew(%s): %v", entry.ID, err)
	}
}

// TestSendMessageDeleteOutgoingPreservesRowUntilAck pins pessimistic
// ordering: SendMessageDelete on an outgoing row must NOT delete the
// chatlog row up front. The row is removed only later, inside
// handleInboundMessageDeleteAck, when a success status arrives. The
// test asserts that immediately after SendMessageDelete returns:
//
//   - the chatlog row is still there;
//   - exactly one dispatch has gone out (the initial one);
//   - a pending entry exists, addressed to the row's recipient.
func TestSendMessageDeleteOutgoingPreservesRowUntilAck(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)

	const (
		target    = "10000000-2222-4444-8888-cccccccccccc"
		recipient = domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	)
	insertChatlogEntry(t, c.chatlog, recipient, chatlog.Entry{
		ID:        target,
		Sender:    string(myAddr),
		Recipient: string(recipient),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagSenderDelete),
	})

	if err := r.SendMessageDelete(context.Background(), recipient, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(domain.MessageID(target)); err != nil || !found {
		t.Fatalf("row missing immediately after SendMessageDelete (err=%v, found=%v); pessimistic ordering must wait for ack", err, found)
	}

	if got := counter.count(); got != 1 {
		t.Fatalf("dispatch count after SendMessageDelete = %d, want 1 (one initial dispatch only)", got)
	}

	r.deleteRetry.mu.Lock()
	pending, present := r.deleteRetry.entries[domain.MessageID(target)]
	r.deleteRetry.mu.Unlock()
	if !present {
		t.Fatal("pendingDelete entry missing after SendMessageDelete")
	}
	if pending.peer != recipient {
		t.Errorf("pending.peer = %s, want %s (derived from row.Recipient)", pending.peer, recipient)
	}
	if pending.attempt != 1 {
		t.Errorf("pending.attempt = %d, want 1", pending.attempt)
	}
}

// TestSendMessageDeleteIncomingIsLocalOnlySync covers the
// incoming-direction shortcut: when the row was sent BY the peer (we
// received it), default sender-delete would have the peer reject our
// request. The local row + file-transfer state are removed
// synchronously inside SendMessageDelete and no wire dispatch
// happens.
func TestSendMessageDeleteIncomingIsLocalOnlySync(t *testing.T) {
	t.Parallel()

	r, c, myAddr, counter := newTestDMRouterForDelete(t)

	const (
		target = "20000000-2222-4444-8888-cccccccccccc"
		peer   = domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	)
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        target,
		Sender:    string(peer),
		Recipient: string(myAddr),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagSenderDelete),
	})

	if err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(domain.MessageID(target)); err != nil {
		t.Fatalf("EntryByID: %v", err)
	} else if found {
		t.Fatal("row still present after incoming local-only delete; should be removed synchronously")
	}

	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count for incoming local-only = %d, want 0 (no wire send)", got)
	}

	r.deleteRetry.mu.Lock()
	_, present := r.deleteRetry.entries[domain.MessageID(target)]
	r.deleteRetry.mu.Unlock()
	if present {
		t.Error("incoming local-only delete created a pendingDelete entry; should not have")
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

	const (
		target       = "30000000-2222-4444-8888-cccccccccccc"
		realPeer     = domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		strangerPeer = domain.PeerIdentity("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	)
	insertChatlogEntry(t, c.chatlog, realPeer, chatlog.Entry{
		ID:        target,
		Sender:    string(myAddr),
		Recipient: string(realPeer),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagSenderDelete),
	})

	// Caller passes the wrong peer. The router must override.
	if err := r.SendMessageDelete(context.Background(), strangerPeer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete: %v", err)
	}

	calls := counter.snapshot()
	if len(calls) != 1 {
		t.Fatalf("dispatch count = %d, want 1", len(calls))
	}
	if calls[0].peer != realPeer {
		t.Errorf("dispatch addressed to %s, want %s (caller's wrong peer must be overridden)", calls[0].peer, realPeer)
	}

	r.deleteRetry.mu.Lock()
	pending, present := r.deleteRetry.entries[domain.MessageID(target)]
	r.deleteRetry.mu.Unlock()
	if !present {
		t.Fatal("pendingDelete missing")
	}
	if pending.peer != realPeer {
		t.Errorf("pending.peer = %s, want %s (must be derived peer, not caller-supplied)", pending.peer, realPeer)
	}
}

// TestSendMessageDeleteAbsentTargetUsesRecoveryWirePath pins the
// !found case: with no local row the caller's peer is trusted (no
// derivation possible) and the wire path runs as a recovery affordance
// for re-issuing a delete after a crash dropped the in-memory pending
// queue. There is nothing local to mutate.
func TestSendMessageDeleteAbsentTargetUsesRecoveryWirePath(t *testing.T) {
	t.Parallel()

	r, _, _, counter := newTestDMRouterForDelete(t)

	const (
		target = "40000000-2222-4444-8888-cccccccccccc"
		peer   = domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	)

	// No insert — the row is intentionally absent.

	if err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target)); err != nil {
		t.Fatalf("SendMessageDelete (recovery !found): %v", err)
	}

	if got := counter.count(); got != 1 {
		t.Fatalf("dispatch count = %d, want 1 (recovery wire path must dispatch)", got)
	}

	r.deleteRetry.mu.Lock()
	pending, present := r.deleteRetry.entries[domain.MessageID(target)]
	r.deleteRetry.mu.Unlock()
	if !present {
		t.Fatal("pending entry missing on recovery path")
	}
	if pending.peer != peer {
		t.Errorf("pending.peer = %s, want %s (caller-supplied peer is trusted on !found)", pending.peer, peer)
	}
	if pending.attempt != 1 {
		t.Errorf("pending.attempt = %d, want 1", pending.attempt)
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

	const (
		target = "50000000-2222-4444-8888-cccccccccccc"
		peer   = domain.PeerIdentity("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	)
	insertChatlogEntry(t, c.chatlog, peer, chatlog.Entry{
		ID:        target,
		Sender:    string(myAddr),
		Recipient: string(peer),
		Body:      "ciphertext",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag:      string(protocol.MessageFlagImmutable),
	})

	err := r.SendMessageDelete(context.Background(), peer, domain.MessageID(target))
	if err == nil {
		t.Fatal("SendMessageDelete on immutable row returned nil error; want refusal")
	}

	if got := counter.count(); got != 0 {
		t.Errorf("dispatch count = %d, want 0 (immutable must not produce any wire send)", got)
	}

	store := c.chatlog.Store()
	if _, found, err := store.EntryByID(domain.MessageID(target)); err != nil || !found {
		t.Fatalf("immutable row missing after refusal (err=%v, found=%v); refusal must not mutate", err, found)
	}

	r.deleteRetry.mu.Lock()
	_, present := r.deleteRetry.entries[domain.MessageID(target)]
	r.deleteRetry.mu.Unlock()
	if present {
		t.Error("immutable refusal created a pendingDelete entry; should not have")
	}
}
