package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestClearingAChatErasesBothSidesMessages is the bug report, written down,
// and driven the way the product drives it: A clears the chat, the request is
// marshalled, handed to B's inbound control-DM handler, and B's answer is read
// back off the wire seam.
//
// A holds TWO messages and B holds THREE — the two sides disagree on purpose,
// because that is the shape of the report. One of B's rows was written by A, and
// under the per-message rule that is the one that used to survive: the peer
// honoured the deletion of what THEY had written and refused what the requester
// had, so the user's screen went empty while their own half of the conversation
// stood on the other side, once per surviving message.
//
// The wipe asks about the CONVERSATION, so all three of B's rows go, whoever
// wrote them and however many A happened to keep.
func TestClearingAChatErasesBothSidesMessages(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// The side doing the clearing.
	sender, senderClient, senderAddr, dispatched := newTestDMRouterForConversationDelete(t)
	sender.peerReachableFn = func(domain.PeerIdentity) bool { return true }

	// The side receiving the request. A router of its own over its own
	// chatlog, so the two databases can disagree — which they do here.
	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)

	const (
		mineBothHave   = "b1000000-2222-4444-8888-cccccccccccc"
		theirsBothHave = "b2000000-2222-4444-8888-cccccccccccc"
		theirsOnlyThey = "b3000000-2222-4444-8888-cccccccccccc"
	)
	written := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	// The third message is NEWER than anything the requester holds, and that is
	// the point of it. It is the message still sitting in a relay's buffer when
	// the user clears the chat: the peer has it, this side never received it,
	// and its stamp is later than every row here. A boundary derived only from
	// what THIS side holds would leave exactly that message standing.
	writtenLater := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)

	// Here: TWO messages.
	insertChatlogEntry(t, senderClient.chatlog, receiverAddr, chatlog.Entry{
		ID: mineBothHave, Sender: senderAddr.String(), Recipient: receiverAddr.String(),
		Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
	})
	insertChatlogEntry(t, senderClient.chatlog, receiverAddr, chatlog.Entry{
		ID: theirsBothHave, Sender: receiverAddr.String(), Recipient: senderAddr.String(),
		Body: "ciphertext", CreatedAt: written, Flag: string(protocol.MessageFlagAnyDelete),
	})

	// There: THREE, including one this side never received — and all stamped
	// with the author-only policy an older build put on every message, which is
	// exactly what the per-message rule tripped over.
	for _, row := range []struct{ id, sender, recipient, createdAt string }{
		{mineBothHave, senderAddr.String(), receiverAddr.String(), written},
		{theirsBothHave, receiverAddr.String(), senderAddr.String(), written},
		{theirsOnlyThey, receiverAddr.String(), senderAddr.String(), writtenLater},
	} {
		insertChatlogEntry(t, receiverClient.chatlog, senderAddr, chatlog.Entry{
			ID: row.id, Sender: row.sender, Recipient: row.recipient,
			Body: "ciphertext", CreatedAt: row.createdAt, Flag: string(protocol.MessageFlagSenderDelete),
		})
	}

	if err := sender.SendConversationDelete(ctx, receiverAddr); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	// Both of A's rows are gone here.
	here, err := senderClient.chatlog.Store().Read(ctx, "dm", receiverAddr)
	if err != nil {
		t.Fatalf("read the cleared thread: %v", err)
	}
	if len(here) != 0 {
		t.Fatalf("%d of 2 rows survived the wipe here: %+v", len(here), here)
	}

	// The sweep dispatches it once. What went out is the request itself —
	// marshalled here rather than assumed, so the payload the peer parses is
	// the payload this node builds.
	sender.processDeleteRetryDue(ctx, time.Now().UTC())
	dispatched.mu.Lock()
	calls := append([]domain.ConversationDeleteRequestID(nil), dispatched.calls...)
	dispatched.mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("dispatched %d requests for a two-message thread, want one", len(calls))
	}
	payload, err := domain.MarshalConversationDeletePayload(domain.ConversationDeletePayload{
		RequestID: calls[0],
	})
	if err != nil {
		t.Fatalf("marshal the dispatched request: %v", err)
	}

	// B receives it as a control DM: same JSON, same handler, same answer path.
	var acks []domain.ConversationDeleteAckPayload
	receiver.dispatchControlConversationDeleteAckFn = func(_ context.Context, to domain.PeerIdentity, ack domain.ConversationDeleteAckPayload) error {
		if to != senderAddr {
			t.Errorf("the answer went to %s, want the requester", to)
		}
		acks = append(acks, ack)
		return nil
	}
	receiver.handleInboundConversationDelete(senderAddr, payload)

	if len(acks) != 1 {
		t.Fatalf("answers = %d, want one", len(acks))
	}
	if acks[0].Status != domain.ConversationDeleteStatusApplied {
		t.Fatalf("status = %q, want applied", acks[0].Status)
	}
	if acks[0].RequestID != calls[0] {
		t.Errorf("the answer echoes %s, want the request %s", acks[0].RequestID, calls[0])
	}

	there, err := receiverClient.chatlog.Store().Read(ctx, "dm", senderAddr)
	if err != nil {
		t.Fatalf("read the peer's thread: %v", err)
	}
	if len(there) != 0 {
		t.Fatalf("%d rows survived on the peer's side: %+v — this is the half-cleared conversation the wipe exists to prevent", len(there), there)
	}

	// And the answer settles the request on A's side, through the same
	// serialisation.
	ackPayload, err := domain.MarshalConversationDeleteAckPayload(acks[0])
	if err != nil {
		t.Fatalf("marshal the answer: %v", err)
	}
	sender.handleInboundConversationDeleteAck(receiverAddr, ackPayload)
	if _, stillPending := conversationIntentFor(t, senderClient, receiverAddr); stillPending {
		t.Error("the request outlived the answer to it")
	}
}

// TestPeerRequestKeepsImmutableRows — authorship is not consulted, but the
// immutable flag still is. It is the one promise no gesture overrides, and it
// holds on the receiving side exactly as it holds locally.
func TestPeerRequestKeepsImmutableRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const (
		ordinary  = "b4000000-2222-4444-8888-cccccccccccc"
		immutable = "b5000000-2222-4444-8888-cccccccccccc"
	)
	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: ordinary, Sender: peer.String(), Recipient: receiverAddr.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: immutable, Sender: peer.String(), Recipient: receiverAddr.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagImmutable),
	})

	if status := receiver.applyInboundConversationDelete(peer, "c0100000-1111-4222-8333-444444444444"); status != domain.ConversationDeleteStatusApplied {
		t.Fatalf("status=%q, want applied", status)
	}
	entries, err := receiverClient.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != immutable {
		t.Fatalf("rows after the request = %+v, want only the immutable one", entries)
	}
}

// TestClearingAnAlreadyEmptyChatStillAsksThePeer is the repair path, and the
// reason the request carries no ids.
//
// A thread an older build cleared here — while the peer refused the user's own
// messages — has nothing left to name. Under a request made of message ids
// there was no way to ask again: the ids went with the rows, and a settled
// request forgets which peer it belonged to. Asking for the CONVERSATION needs
// neither.
func TestClearingAnAlreadyEmptyChatStillAsksThePeer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, _, dispatched := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete over an empty thread: %v", err)
	}

	intent, found := conversationIntentFor(t, c, peer)
	if !found {
		t.Fatal("clearing an empty chat asked the peer for nothing; a split thread can never be repaired")
	}
	if intent.RequestID == "" {
		t.Error("the request carries no id, so no ack can settle it")
	}

	r.processDeleteRetryDue(ctx, time.Now().UTC())
	dispatched.mu.Lock()
	calls := len(dispatched.calls)
	dispatched.mu.Unlock()
	if calls != 1 {
		t.Errorf("dispatched %d requests, want the one that repairs the peer's side", calls)
	}
}

// TestConversationAckSettlesTheRequest walks the answer back: an applied ack
// retires the request, and one echoing a superseded id does not.
func TestConversationAckSettlesTheRequest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "b6000000-2222-4444-8888-cccccccccccc")

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}
	intent, found := conversationIntentFor(t, c, peer)
	if !found {
		t.Fatal("no request to settle")
	}

	// An answer to some other press of the button settles nothing: the
	// current request was made about the conversation as it is now.
	stale, err := domain.MarshalConversationDeleteAckPayload(domain.ConversationDeleteAckPayload{
		RequestID: domain.ConversationDeleteRequestID("00000000-1111-4222-8333-444444444444"),
		Status:    domain.ConversationDeleteStatusApplied,
	})
	if err != nil {
		t.Fatalf("marshal the stale ack: %v", err)
	}
	r.handleInboundConversationDeleteAck(peer, stale)
	if _, stillThere := conversationIntentFor(t, c, peer); !stillThere {
		t.Fatal("an ack for a superseded request retired the current one")
	}

	// A transient failure is not an answer either.
	transient, err := domain.MarshalConversationDeleteAckPayload(domain.ConversationDeleteAckPayload{
		RequestID: intent.RequestID,
		Status:    domain.ConversationDeleteStatusError,
	})
	if err != nil {
		t.Fatalf("marshal the transient ack: %v", err)
	}
	r.handleInboundConversationDeleteAck(peer, transient)
	if _, stillThere := conversationIntentFor(t, c, peer); !stillThere {
		t.Fatal("a peer that could not decide retired the request anyway")
	}

	applied, err := domain.MarshalConversationDeleteAckPayload(domain.ConversationDeleteAckPayload{
		RequestID: intent.RequestID,
		Status:    domain.ConversationDeleteStatusApplied,
	})
	if err != nil {
		t.Fatalf("marshal the ack: %v", err)
	}
	r.handleInboundConversationDeleteAck(peer, applied)
	if _, stillThere := conversationIntentFor(t, c, peer); stillThere {
		t.Error("the request outlived the answer to it; the peer would be asked again forever")
	}
	if pendingWorkFor(t, c, peer).Any() {
		t.Error("the settled wipe is still counted as pending work")
	}
}

// TestConversationRequestIsNeverGivenUpOn: the peer can be silent for any
// number of attempts and the request stands.
//
// Writing it off would leave the one state this gesture may not end in —
// erased here, still there at the peer, and nothing left that will ever ask
// again — and the user would be told so in a status line that reads like a
// completed action with a footnote. The row is tiny, the backoff caps at an
// hour, and it goes when the contact goes.
func TestConversationRequestIsNeverGivenUpOn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, c, myAddr, _ := newTestDMRouterForConversationDelete(t)
	r.peerReachableFn = func(domain.PeerIdentity) bool { return true }
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	seedConversation(t, c, myAddr, peer, "b7000000-2222-4444-8888-cccccccccccc")

	if err := r.SendConversationDelete(ctx, peer); err != nil {
		t.Fatalf("SendConversationDelete: %v", err)
	}

	now := time.Now().UTC()
	for attempt := 0; attempt < deleteIntentGiveUpAttempts+1; attempt++ {
		r.processDeleteRetryDue(ctx, now)
		now = now.Add(time.Hour)
	}

	intent, stillThere := conversationIntentFor(t, c, peer)
	if !stillThere {
		t.Fatal("the request was written off; the chat is erased here and may still stand at the peer, with nobody left to ask")
	}
	if intent.Attempts <= deleteIntentGiveUpAttempts {
		t.Errorf("attempts = %d, want the sweep to have kept asking past the per-message budget", intent.Attempts)
	}
	if !pendingWorkFor(t, c, peer).Conversation {
		t.Error("the pending indicator stopped reporting a wipe that is still outstanding")
	}
}

// TestInboundWipeClosesTheWriteWindowBeforeReading: the barrier goes up before
// the thread is read, so a message that lands while a write is still in flight
// is taken by the wipe rather than silently left behind.
//
// The order matters because the answer is terminal. Read first, and a message
// committed between the read and the barrier misses the list — the requester is
// told `applied`, the request is retired, and that message stays for good.
//
// The sequence below pins it deterministically: the wipe is proven to be
// waiting on the gate BEFORE the row is written, so a run that still deletes it
// can only have read the thread after taking the gate.
func TestInboundWipeClosesTheWriteWindowBeforeReading(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)
	receiver.removals = receiverClient.removals
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	// A writer admitted before the request arrives: the wipe must wait for it.
	releaseWrite, admitted := receiverClient.removals.admitWrite(peer)
	if !admitted {
		t.Fatal("the fixture could not take a write lease")
	}

	applied := make(chan domain.ConversationDeleteStatus, 1)
	go func() {
		status := receiver.applyInboundConversationDelete(peer, "c7000000-1111-4222-8333-444444444444")
		applied <- status
	}()

	// Proven to be inside the gate, and therefore proven not to have read the
	// thread yet if the gate is taken first.
	awaitRemovalInFlight(t, receiverClient.removals, peer)
	select {
	case status := <-applied:
		t.Fatalf("the wipe finished while a write was still in flight (status %q)", status)
	default:
	}

	// The message the in-flight write was carrying.
	const arrivedMidWipe = "c6000000-2222-4444-8888-cccccccccccc"
	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: arrivedMidWipe, Sender: peer.String(), Recipient: receiverAddr.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	releaseWrite()

	select {
	case status := <-applied:
		if status != domain.ConversationDeleteStatusApplied {
			t.Fatalf("status = %q, want applied once the write settled", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wipe never finished after the write released its lease")
	}

	entries, err := receiverClient.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d rows survived: a message that landed while the write was in flight was answered for but not taken", len(entries))
	}
}

// TestInboundWipeRefusesWhenItCannotStopItsOwnDeliveries: erasing the rows and
// answering `applied` while a copy is still queued would retire the request
// with the escape still pending. `error` costs a round-trip; the alternative
// costs the message.
func TestInboundWipeRefusesWhenItCannotStopItsOwnDeliveries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	const queued = "c8000000-2222-4444-8888-cccccccccccc"
	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: queued, Sender: receiverAddr.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})

	receiver.client.freezeConversationDeliveryFn = func(context.Context, domain.PeerIdentity, []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{}, errors.New("node unreachable")
	}

	if status := receiver.applyInboundConversationDelete(peer, "c9000000-1111-4222-8333-444444444444"); status != domain.ConversationDeleteStatusError {
		t.Fatalf("status = %q, want error: the wipe cannot promise what it could not stop", status)
	}
	entries, err := receiverClient.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("the thread was erased anyway (%d rows left); the queued copy would have gone out afterwards", len(entries))
	}
}

// TestInboundWipeKeepsRefusingWhileAWithdrawalIsOwed: same reasoning as the
// freeze, one step later, and it has to survive the REPEAT.
//
// The rows are gone by the time a withdrawal fails, so the request comes back
// with an empty scope and withdraws nothing — and `applied` then would retire
// the requester's only durable record that something is still outstanding here,
// while a message of the erased conversation sits in this node's queue. So the
// answer is decided by what is still owed, not by what this pass attempted.
func TestInboundWipeKeepsRefusingWhileAWithdrawalIsOwed(t *testing.T) {
	t.Parallel()

	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	requestID := domain.ConversationDeleteRequestID("cb000000-1111-4222-8333-444444444444")
	askedAt := time.Now().UTC()

	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: "ca000000-2222-4444-8888-cccccccccccc", Sender: receiverAddr.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: askedAt.Add(-time.Minute).Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})

	receiver.client.freezeConversationDeliveryFn = func(_ context.Context, _ domain.PeerIdentity, scope []domain.MessageID) (ConversationFreeze, error) {
		return ConversationFreeze{Frozen: len(scope)}, nil
	}
	withdrawWorks := false
	receiver.client.cancelConversationDeliveryFn = func(context.Context, domain.PeerIdentity) (ConversationCancellation, error) {
		if withdrawWorks {
			return ConversationCancellation{Cancelled: 1}, nil
		}
		return ConversationCancellation{}, errors.New("node unreachable")
	}

	if status := receiver.applyInboundConversationDelete(peer, requestID); status != domain.ConversationDeleteStatusError {
		t.Fatalf("status = %q, want error: a withdrawal owed only to memory is not a settled request", status)
	}

	// The repeat: the rows are already gone, so its scope is empty and it
	// withdraws nothing on its own. It must still not claim the wipe is done.
	if status := receiver.applyInboundConversationDelete(peer, requestID); status != domain.ConversationDeleteStatusError {
		t.Fatalf("the repeat answered %q while a message of the erased thread is still queued here", status)
	}

	// Once the sweep gets the withdrawal through, the next repeat settles.
	withdrawWorks = true
	receiver.retryOwedWithdrawals(context.Background())
	if status := receiver.applyInboundConversationDelete(peer, requestID); status != domain.ConversationDeleteStatusApplied {
		t.Fatalf("status = %q, want applied once nothing is owed", status)
	}
}

// TestARepeatedWipeTakesWhateverIsThere pins the shape the command was
// deliberately given: it says "erase this conversation", it carries no moment,
// and it is answered every single time it arrives.
//
// Five arrivals, five wipes, five answers. That is what makes the receiving
// side stateless — nothing is written down about having applied a request,
// because nothing needs to be — and it is the reason the earlier boundary was
// removed: it was one machine's clock compared against rows stamped by
// another's, and every way of reconciling the two was wrong in one direction.
//
// The cost is the second half of this test: a message written between two
// arrivals is taken by the second one. That is asserted here on purpose, not
// tolerated: it is forced by the two rules above the command, and the proof —
// with the three boundary designs that were tried — is in docs/dm-commands.md
// §"Why a repeat can take a new message". Changing this assertion means giving
// one of those two rules up; say which one in the same change.
func TestARepeatedWipeTakesWhateverIsThere(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	requestID := domain.ConversationDeleteRequestID("cd000000-1111-4222-8333-444444444444")

	var acked []domain.ConversationDeleteAckPayload
	receiver.dispatchControlConversationDeleteAckFn = func(_ context.Context, _ domain.PeerIdentity, ack domain.ConversationDeleteAckPayload) error {
		acked = append(acked, ack)
		return nil
	}
	payload, err := domain.MarshalConversationDeletePayload(domain.ConversationDeletePayload{RequestID: requestID})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: "ce000000-2222-4444-8888-cccccccccccc", Sender: peer.String(), Recipient: receiverAddr.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})

	// Five arrivals of the SAME request.
	for i := range 5 {
		receiver.handleInboundConversationDelete(peer, payload)
		if len(acked) != i+1 {
			t.Fatalf("arrival %d was not answered: %d answers so far", i, len(acked))
		}
		if acked[i].Status != domain.ConversationDeleteStatusApplied {
			t.Fatalf("arrival %d answered %q, want applied", i, acked[i].Status)
		}
	}
	// The thread went with the first arrival; the rest found nothing to do and
	// said so anyway. How many rows each one took is not on the wire at all —
	// a count of them is a count of the messages the requester never had.
	if left, err := receiverClient.chatlog.Store().Read(ctx, "dm", peer); err != nil || len(left) != 0 {
		t.Fatalf("thread after five arrivals: %d rows (err=%v)", len(left), err)
	}

	// And the cost, written down: a message that arrives between two requests
	// is taken by the next one. The user asked for this conversation to be
	// erased and has not said otherwise.
	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: "cf000000-2222-4444-8888-cccccccccccc", Sender: receiverAddr.String(), Recipient: peer.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	receiver.handleInboundConversationDelete(peer, payload)
	left, err := receiverClient.chatlog.Store().Read(ctx, "dm", peer)
	if err != nil {
		t.Fatalf("read the thread: %v", err)
	}
	if len(left) != 0 {
		t.Fatalf("thread = %+v, want it empty", left)
	}
}

// TestTheReceiverKeepsRefusingTheWipedIdsUntilTheyExpire pins the receiving
// side's half of the same rule.
//
// The receiver never sees an ack — it SENDS one — so there is no moment at
// which it could be told the wipe is finished, and there is no id list left
// anywhere to look one up by. Its refusals therefore live out the sender's
// reseed horizon and then go. That is the intended lifecycle, not an oversight:
// the copies a wipe has to turn away are exactly the ones still in flight when
// it ran, and they are in flight for as long as their sender keeps re-seeding.
func TestTheReceiverKeepsRefusingTheWipedIdsUntilTheyExpire(t *testing.T) {
	t.Parallel()

	receiver, receiverClient, receiverAddr, _ := newTestDMRouterForConversationDelete(t)
	peer := domain.PeerIdentityFromWire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	const wiped = domain.MessageID("d5000000-2222-4444-8888-cccccccccccc")

	insertChatlogEntry(t, receiverClient.chatlog, peer, chatlog.Entry{
		ID: string(wiped), Sender: peer.String(), Recipient: receiverAddr.String(),
		Body: "ciphertext", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Flag: string(protocol.MessageFlagAnyDelete),
	})
	receiver.dispatchControlConversationDeleteAckFn = func(context.Context, domain.PeerIdentity, domain.ConversationDeleteAckPayload) error {
		return nil
	}
	payload, err := domain.MarshalConversationDeletePayload(domain.ConversationDeletePayload{
		RequestID: "d6000000-1111-4222-8333-444444444444",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	receiver.handleInboundConversationDelete(peer, payload)

	now := time.Now().UTC()
	if refused, _ := receiver.wipeTombstones.Refuses(wiped, now); !refused {
		t.Fatal("the wiped id is not refused: a copy still in flight would be stored again")
	}
	// Still refused most of the way through the horizon...
	if refused, _ := receiver.wipeTombstones.Refuses(wiped, now.Add(wipeTombstoneTTL-time.Hour)); !refused {
		t.Error("the refusal expired before the sender could have stopped re-seeding")
	}
	// ...and gone after it, because past that point nothing re-sends the
	// message and an entry that stayed would be a memory of a deletion.
	if refused, _ := receiver.wipeTombstones.Refuses(wiped, now.Add(wipeTombstoneTTL+time.Minute)); refused {
		t.Error("the refusal outlived the window in which a replay was possible")
	}
}
