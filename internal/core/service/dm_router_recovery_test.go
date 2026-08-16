package service

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// newRecoveryRouter builds a router over the real node harness, with the
// recovery manager wired the production way.
func newRecoveryRouter(t *testing.T) (*DMRouter, *DesktopClient, *identity.Identity) {
	t.Helper()
	client, id := newTestDesktopClientWithNode(t)
	t.Cleanup(func() { _ = client.Close() })
	// The production app wiring registers the client as the node's message
	// store (app.go); the recovery flow depends on sends landing in the
	// chatlog, so the harness mirrors it.
	client.localNode.RegisterMessageStore(client)
	router := NewDMRouter(client, nil, ebus.New(), nil)
	return router, client, id
}

// throwawayBoxKey models the §4.10 "old key": an X25519 key nobody holds
// any more.
func throwawayBoxKey(t *testing.T) string {
	t.Helper()
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return identity.BoxPublicKeyBase64(key.PublicKey().Bytes())
}

func testUUID(n byte) string {
	return fmt.Sprintf("0b7d81f2-9c48-4a6e-9d10-0000000000%02x", n)
}

// TestRecoveryReportIdempotent: one unreadable row opens exactly one job
// however many times the UI re-renders it; the missing-key class opens
// none.
func TestRecoveryReportIdempotent(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	peer := domaintest.ID("rot-peer")
	rowID := testUUID(0x01)

	if err := client.chatLog.Append("dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID: rowID, Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	failure := DecryptFailure{MessageID: rowID, Sender: peer.String(), Recipient: id.Address, Class: DecryptFailureSealedUnreadable}
	router.recovery.Report(failure)
	router.recovery.Report(failure)

	jobs, err := client.chatLog.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != 1 || jobs[0].Peer != peer.String() {
		t.Fatalf("jobs = %v err=%v, want exactly one for the peer", jobs, err)
	}

	// missing_sender_key never opens a job — it only kicks the lookup.
	other := domaintest.ID("keyless-peer")
	router.recovery.Report(DecryptFailure{MessageID: testUUID(0x02), Sender: other.String(), Recipient: id.Address, Class: DecryptFailureMissingSenderKey})
	if jobs, _ := client.chatLog.RecoveryJobs(context.Background()); len(jobs) != 1 {
		t.Fatalf("missing-key class opened a job: %v", jobs)
	}
}

// TestOrderRecoveryWorkEstablishedFirst is the §4.10 fair-scheduler test:
// the unified queue serves EVERY established candidate — of either leg —
// before ANY unknown one, so 30 Sybil jobs cannot outrank an established
// re-send and Sybil intents cannot outrank an established job.
func TestOrderRecoveryWorkEstablishedFirst(t *testing.T) {
	t.Parallel()

	due := make([]chatlog.RecoveryJob, 0, 31)
	for i := 0; i < 30; i++ {
		due = append(due, chatlog.RecoveryJob{Peer: fmt.Sprintf("sybil-%02d", i)})
	}
	due = append(due, chatlog.RecoveryJob{Peer: "friend-job"})
	waiting := []chatlog.ResendIntent{
		{Root: "sybil-intent", Peer: "sybil-99"},
		{Root: "friend-intent", Peer: "friend-resend"},
	}
	isEstablished := func(peer string) bool { return peer == "friend-job" || peer == "friend-resend" }

	ordered := orderRecoveryWork(due, waiting, isEstablished)
	if len(ordered) != len(due)+len(waiting) {
		t.Fatalf("ordered = %d items, want %d", len(ordered), len(due)+len(waiting))
	}
	// The two established candidates — one per leg — come first, jobs
	// before intents; every unknown candidate of BOTH legs follows.
	if ordered[0].peer != "friend-job" || ordered[0].job == nil {
		t.Fatalf("ordered[0] = %+v, want the established job", ordered[0])
	}
	if ordered[1].peer != "friend-resend" || ordered[1].intent == nil {
		t.Fatalf("ordered[1] = %+v, want the established re-send ahead of every Sybil job", ordered[1])
	}
	for i, item := range ordered[2:] {
		if isEstablished(item.peer) {
			t.Fatalf("established candidate %+v sorted after unknowns at %d", item, i+2)
		}
	}

	// WITHIN a class both legs interleave by least-recently-served: an
	// OLD waiting intent outranks a job served more recently — a constant
	// stream of due jobs cannot starve the sender leg.
	now := time.Unix(1780000000, 0).UTC()
	served := orderRecoveryWork(
		[]chatlog.RecoveryJob{{Peer: "friend-job", LastNoticeAt: now.Add(time.Hour)}},
		[]chatlog.ResendIntent{{Root: "old-intent", Peer: "friend-resend", CreatedAt: now}},
		isEstablished)
	if len(served) != 2 || served[0].intent == nil {
		t.Fatalf("ordered = %+v, want the older intent ahead of the recently-served job", served)
	}

	// A NEVER-served job ranks by its admission time, not by a zero stamp
	// that would unconditionally outrank every real intent timestamp: a
	// stream of freshly admitted jobs must not starve an older re-send.
	fresh := orderRecoveryWork(
		[]chatlog.RecoveryJob{{Peer: "friend-job", CreatedAt: now.Add(time.Hour)}},
		[]chatlog.ResendIntent{{Root: "old-intent", Peer: "friend-resend", CreatedAt: now}},
		isEstablished)
	if len(fresh) != 2 || fresh[0].intent == nil {
		t.Fatalf("ordered = %+v, want the older intent ahead of the never-served fresher job", fresh)
	}
}

// TestHandleInboundDecryptFailedValidation: a crafted notice must not
// trigger a re-send — the §4.10 anti-exfiltration matrix.
func TestHandleInboundDecryptFailedValidation(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	peerB := domaintest.ID("peer-b")
	peerC := domaintest.ID("peer-c")

	outgoingToB := testUUID(0x10)
	incomingFromB := testUUID(0x11)
	// A REAL envelope: the honest-notice branch must survive the own-copy
	// decrypt and stall only on the unknown recipient keys.
	outgoingCiphertext, err := directmsg.EncryptForParticipants(id, domain.DMRecipient{
		Address:      peerB,
		BoxKeyBase64: throwawayBoxKey(t),
	}, domain.OutgoingDM{Body: "resend me"})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: outgoingToB, Sender: id.Address, Recipient: peerB.String(),
		Body: outgoingCiphertext, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append outgoing: %v", err)
	}
	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: incomingFromB, Sender: peerB.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append incoming: %v", err)
	}

	payloadFor := func(id string) string {
		payload, err := domain.MarshalDecryptFailedPayload(domain.DecryptFailedPayload{MessageID: domain.MessageID(id)})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return payload
	}
	pendingCount := func() int {
		router.recovery.mu.Lock()
		defer router.recovery.mu.Unlock()
		return len(router.recovery.pendingResends)
	}

	// A notice from C about a message addressed to B: the recipient is
	// taken from OUR row, and the row says B — C gets nothing.
	router.handleInboundDecryptFailed(peerC.String(), payloadFor(outgoingToB))
	if pendingCount() != 0 {
		t.Fatal("a foreign peer's notice queued a re-send — plaintext redirection is open")
	}

	// A notice about a message WE RECEIVED (not authored): refused.
	router.handleInboundDecryptFailed(peerB.String(), payloadFor(incomingFromB))
	if pendingCount() != 0 {
		t.Fatal("a notice about a received row queued a re-send")
	}

	// A notice about an unknown id: refused.
	router.handleInboundDecryptFailed(peerB.String(), payloadFor(testUUID(0xEE)))
	if pendingCount() != 0 {
		t.Fatal("a notice about an unknown row queued a re-send")
	}

	// The honest notice admits the durable intent; the scheduler pass —
	// the single activation point — moves it into the active queue.
	router.handleInboundDecryptFailed(peerB.String(), payloadFor(outgoingToB))
	router.recovery.pass(context.Background())
	if pendingCount() != 1 {
		t.Fatal("an honest notice did not queue the re-send")
	}
}

// TestDecryptRecoveryFullCycle composes the §4.10 box-rotation exchange
// end to end over the real node harnesses, without live wire:
//
//	A sends to B's OLD key → B cannot open (confirmed class) → B flags the
//	row and opens a job → B's notice reaches A → A validates against its
//	OWN chatlog and queues the re-send → keys of B arrive (import) → A
//	re-sends with retry_of and supersedes its original → the re-send
//	reaches B → B supersedes the flagged original, collapses unread and
//	closes the job.
func TestDecryptRecoveryFullCycle(t *testing.T) {
	t.Parallel()
	routerA, clientA, idA := newRecoveryRouter(t)
	routerB, clientB, idB := newRecoveryRouter(t)
	selfA := domain.PeerIdentityFromWire(idA.Address)
	selfB := domain.PeerIdentityFromWire(idB.Address)

	// --- A's original send, encrypted to a key B no longer holds. ---
	originalID := testUUID(0x20)
	staleCiphertext, err := directmsg.EncryptForParticipants(idA, domain.DMRecipient{
		Address:      selfB,
		BoxKeyBase64: throwawayBoxKey(t), // B's rotated-away key
	}, domain.OutgoingDM{Body: "important text"})
	if err != nil {
		t.Fatalf("encrypt to old key: %v", err)
	}
	for _, side := range []struct {
		store *chatlog.Store
		self  domain.PeerIdentity
	}{{clientA.chatLog, selfA}, {clientB.chatLog, selfB}} {
		if err := side.store.Append("dm", side.self, chatlog.Entry{
			ID: originalID, Sender: idA.Address, Recipient: idB.Address,
			Body: staleCiphertext, CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append original: %v", err)
		}
	}
	// A must know B's SIGNING key for B's decrypt attempt to reach the
	// sealed parts (B resolves A's pubkey the same way).
	importContact := func(t *testing.T, into *DesktopClient, of *identity.Identity) {
		t.Helper()
		reply, err := into.rpc.LocalRequestFrame(protocol.Frame{
			Type: "import_contacts",
			Contacts: []protocol.ContactFrame{{
				Address: of.Address,
				PubKey:  identity.PublicKeyBase64(of.PublicKey),
				BoxKey:  identity.BoxPublicKeyBase64(of.BoxPublicKey),
				BoxSig:  identity.SignBoxKeyBinding(of),
			}},
		})
		if err != nil || reply.Type != "contacts_imported" {
			t.Fatalf("import contact: %v %v", reply.Type, err)
		}
	}
	importContact(t, clientB, idA)

	// --- Receiver leg at B: the live decrypt reports the confirmed class. ---
	msg := clientB.dm.DecryptIncomingMessage(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm",
		MessageID: originalID, Sender: idA.Address, Recipient: idB.Address,
		Body: staleCiphertext, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if msg != nil {
		t.Fatal("test setup: the stale envelope decrypted")
	}
	marks, _, err := clientB.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || !marks.DecryptFailed {
		t.Fatalf("B did not flag the row: marks=%+v err=%v", marks, err)
	}
	if jobs, _ := clientB.chatLog.RecoveryJobs(context.Background()); len(jobs) != 1 {
		t.Fatalf("B did not open the job: %v", jobs)
	}

	// --- The notice reaches A (payload as B would send it). ---
	payload, err := domain.MarshalDecryptFailedPayload(domain.DecryptFailedPayload{MessageID: domain.MessageID(originalID)})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	routerA.handleInboundDecryptFailed(idB.Address, payload)
	// The notice lands in the durable backlog; the scheduler pass — the
	// single activation point — takes it into the active queue and arms
	// its proof.
	routerA.recovery.pass(context.Background())

	// A cannot re-send yet: B's fresh keys are unknown. The pending
	// re-send waits for the key event instead of being dropped.
	routerA.recovery.mu.Lock()
	pending := len(routerA.recovery.pendingResends)
	routerA.recovery.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pending resends = %d, want 1 (waiting for keys)", pending)
	}

	// --- B's fresh keys arrive at A, and the proof-bearing lookup
	// completes with an AUTHORITATIVE result — the exact event the
	// resolver publishes on a successful merge. Usable alone must not
	// unblock the re-send: it may describe the dead key.
	importContact(t, clientA, idB)
	routerA.recovery.retryPendingResends(idB.Address)
	// The notice armed its proof under the ROOT's own scope (the original
	// id — no prior chain). Raise its watermark so the attempt-generation
	// clauses below are distinguishable.
	rootKey := recoveryProofKey{peer: idB.Address, scope: originalID}
	routerA.recovery.mu.Lock()
	stillPending := len(routerA.recovery.pendingResends)
	request := routerA.recovery.proofs[rootKey]
	request.watermark = 5
	routerA.recovery.proofs[rootKey] = request
	requestedID := request.resolutionID
	routerA.recovery.mu.Unlock()
	if stillPending != 1 {
		t.Fatalf("re-send left without an authoritative proof: pending=%d", stillPending)
	}
	if requestedID == "" {
		t.Fatal("the inbound notice did not arm a proof request with its own lookup")
	}
	// A FOREIGN authoritative completion (some other flow's lookup that
	// finished before this recovery asked its question) must not open the
	// gate — the binding is to the resolution THIS recovery requested.
	routerA.recovery.noteResolution(ebus.IdentityResolutionState{
		ResolutionID:     "another-flows-resolution",
		Target:           domain.PeerIdentityFromWire(idB.Address),
		Lifecycle:        domain.IdentityResolutionSucceeded,
		Authority:        domain.IdentityAuthorityAuthoritative,
		Usable:           true,
		AnswerAttemptGen: 6,
	})
	routerA.recovery.mu.Lock()
	stillPending = len(routerA.recovery.pendingResends)
	routerA.recovery.mu.Unlock()
	if stillPending != 1 {
		t.Fatal("a foreign authoritative completion unblocked the re-send")
	}
	// A matching resolution whose proven answer came from an attempt at or
	// below the arm watermark (the join case: the question predates this
	// recovery) must not open it either.
	routerA.recovery.noteResolution(ebus.IdentityResolutionState{
		ResolutionID:     requestedID,
		Target:           domain.PeerIdentityFromWire(idB.Address),
		Lifecycle:        domain.IdentityResolutionSucceeded,
		Authority:        domain.IdentityAuthorityAuthoritative,
		Usable:           true,
		AnswerAttemptGen: 5,
	})
	routerA.recovery.mu.Lock()
	stillPending = len(routerA.recovery.pendingResends)
	routerA.recovery.mu.Unlock()
	if stillPending != 1 {
		t.Fatal("a proof of a question asked before the failure unblocked the re-send")
	}
	routerA.recovery.noteResolution(ebus.IdentityResolutionState{
		ResolutionID:     requestedID,
		Target:           domain.PeerIdentityFromWire(idB.Address),
		Lifecycle:        domain.IdentityResolutionSucceeded,
		Authority:        domain.IdentityAuthorityAuthoritative,
		Usable:           true,
		AnswerAttemptGen: 6,
	})

	marksA, _, err := clientA.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || marksA.SupersededBy == "" {
		t.Fatalf("A did not supersede its original after the re-send: %+v err=%v", marksA, err)
	}
	resendID := marksA.SupersededBy
	resendEntry, found, err := clientA.chatLog.EntryByID(domain.MessageID(resendID))
	if err != nil || !found {
		t.Fatalf("re-send row missing at A: %v", err)
	}

	// --- The re-send reaches B: the node persists the row, then the live
	// path decrypts it. ---
	if err := clientB.chatLog.Append("dm", selfB, chatlog.Entry{
		ID: resendID, Sender: idA.Address, Recipient: idB.Address,
		Body: resendEntry.Body, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append re-send at B: %v", err)
	}
	resendMsg := clientB.dm.DecryptIncomingMessage(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm",
		MessageID: resendID, Sender: idA.Address, Recipient: idB.Address,
		Body: resendEntry.Body, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if resendMsg == nil || resendMsg.Body != "important text" {
		t.Fatalf("B cannot read the re-send: %+v", resendMsg)
	}
	if resendMsg.RetryOf != domain.MessageID(originalID) {
		t.Fatalf("retry_of lost: %+v", resendMsg)
	}
	routerB.recovery.acceptRetryOf(resendMsg)

	marksB, _, err := clientB.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || marksB.DecryptFailed || marksB.SupersededBy != resendID {
		t.Fatalf("B did not supersede the original: %+v err=%v", marksB, err)
	}
	if jobs, _ := clientB.chatLog.RecoveryJobs(context.Background()); len(jobs) != 0 {
		t.Fatalf("B's job survived the recovery: %v", jobs)
	}
	if unread, _ := clientB.chatLog.UnreadCountFor(selfA); unread != 1 {
		t.Fatalf("unread = %d, want 1 (original collapsed, re-send counts once)", unread)
	}
}

// TestAcceptRetryOfValidation: a re-send link is honoured only for a row
// that is really flagged, from the same author, not yet superseded.
func TestAcceptRetryOfValidation(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	peerB := domaintest.ID("author-b")
	peerC := domaintest.ID("author-c")

	flagged := testUUID(0x30)
	unflagged := testUUID(0x31)
	for _, rowID := range []string{flagged, unflagged} {
		if err := client.chatLog.Append("dm", self, chatlog.Entry{
			ID: rowID, Sender: peerB.String(), Recipient: id.Address,
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := client.chatLog.MarkDecryptFailed(flagged); err != nil {
		t.Fatalf("flag: %v", err)
	}

	supersededOf := func(id string) string {
		marks, _, err := client.chatLog.EntryRecoveryMarks(id)
		if err != nil {
			t.Fatalf("marks: %v", err)
		}
		return marks.SupersededBy
	}

	// A different author cannot replace B's row.
	router.recovery.acceptRetryOf(&DirectMessage{ID: testUUID(0x32), Sender: peerC, Recipient: self, RetryOf: domain.MessageID(flagged)})
	if supersededOf(flagged) != "" {
		t.Fatal("a foreign author replaced the row")
	}

	// An unflagged row cannot be "replaced" — history stays.
	router.recovery.acceptRetryOf(&DirectMessage{ID: testUUID(0x33), Sender: peerB, Recipient: self, RetryOf: domain.MessageID(unflagged)})
	if supersededOf(unflagged) != "" {
		t.Fatal("an unflagged row was superseded")
	}

	// The honest link works once; a second replacement is refused.
	router.recovery.acceptRetryOf(&DirectMessage{ID: testUUID(0x34), Sender: peerB, Recipient: self, RetryOf: domain.MessageID(flagged)})
	if supersededOf(flagged) != testUUID(0x34) {
		t.Fatal("the honest re-send did not supersede")
	}
	router.recovery.acceptRetryOf(&DirectMessage{ID: testUUID(0x35), Sender: peerB, Recipient: self, RetryOf: domain.MessageID(flagged)})
	if supersededOf(flagged) != testUUID(0x34) {
		t.Fatal("a second re-send replaced the row again")
	}
}

// TestRecoveryOrphanReadmission: an evicted (or refused) job's rows keep
// their flags, and the scheduler pass re-admits the peer — without this
// the row-flag idempotency suppressor would orphan them forever.
func TestRecoveryOrphanReadmission(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	peer := domaintest.ID("evicted-peer")
	rowID := testUUID(0x40)

	if err := client.chatLog.Append("dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID: rowID, Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	router.recovery.Report(DecryptFailure{MessageID: rowID, Sender: peer.String(), Recipient: id.Address, Class: DecryptFailureSealedUnreadable})
	if jobs, _ := client.chatLog.RecoveryJobs(context.Background()); len(jobs) != 1 {
		t.Fatalf("setup: job not opened: %v", jobs)
	}

	// The eviction: the job disappears, the row flag stays. A repeat
	// Report is the suppressed no-op (changed=false) — only the pass can
	// bring the job back.
	if err := client.chatLog.DeleteRecoveryJob(peer.String()); err != nil {
		t.Fatalf("evict: %v", err)
	}
	router.recovery.Report(DecryptFailure{MessageID: rowID, Sender: peer.String(), Recipient: id.Address, Class: DecryptFailureSealedUnreadable})
	if jobs, _ := client.chatLog.RecoveryJobs(context.Background()); len(jobs) != 0 {
		t.Fatalf("a suppressed report reopened the job: %v", jobs)
	}

	router.recovery.pass(context.Background())
	jobs, err := client.chatLog.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != 1 || jobs[0].Peer != peer.String() {
		t.Fatalf("the orphaned rows were not re-admitted: %v err=%v", jobs, err)
	}
}

// TestRecoveryProofWaitRotatesQueue: a job selected without a proof grant
// must advance its served stamp — the §4.10 scheduler is fair, and a
// proof-waiting head must not occupy its slot pass after pass while the
// tail never starts.
func TestRecoveryProofWaitRotatesQueue(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	peer := domaintest.ID("proofless-peer")
	rowID := testUUID(0x41)

	if err := client.chatLog.Append("dm", domain.PeerIdentityFromWire(id.Address), chatlog.Entry{
		ID: rowID, Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	router.recovery.Report(DecryptFailure{MessageID: rowID, Sender: peer.String(), Recipient: id.Address, Class: DecryptFailureSealedUnreadable})

	router.recovery.pass(context.Background())

	jobs, err := client.chatLog.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs = %v err=%v", jobs, err)
	}
	if jobs[0].LastNoticeAt.IsZero() {
		t.Fatal("a proof-waiting job kept a zero served stamp — the queue head can never rotate")
	}
	if jobs[0].NoticeAttempts != 0 {
		t.Fatalf("a proof wait burned a notice attempt: %d", jobs[0].NoticeAttempts)
	}
	// The wait armed the job's own lookup under the job scope.
	router.recovery.mu.Lock()
	_, armed := router.recovery.proofs[recoveryProofKey{peer: peer.String(), scope: recoveryProofScopeJob}]
	router.recovery.mu.Unlock()
	if !armed {
		t.Fatal("the proof wait did not arm the job's own lookup")
	}
}

// TestRecoveryPassRetriesStalledResends: the sender leg has no durable
// job — a queued re-send whose grant event was lost must be re-driven by
// the scheduler pass, not sit in pendingResends forever.
func TestRecoveryPassRetriesStalledResends(t *testing.T) {
	t.Parallel()
	router, _, _ := newRecoveryRouter(t)
	peer := domaintest.ID("stalled-peer")
	const root = "stalled-root"

	router.recovery.mu.Lock()
	router.recovery.pendingResends[root] = recoveryResend{originalID: testUUID(0x50), peer: peer.String()}
	router.recovery.proofs[recoveryProofKey{peer: peer.String(), scope: root}] = recoveryProofRequest{resolutionID: "r-1", granted: true}
	router.recovery.mu.Unlock()

	// The pass drives tryResend: with the grant present and the original
	// row missing, the resend resolves (here: dropped) instead of waiting
	// for an event that will never come again.
	router.recovery.pass(context.Background())

	router.recovery.mu.Lock()
	_, stillQueued := router.recovery.pendingResends[root]
	router.recovery.mu.Unlock()
	if stillQueued {
		t.Fatal("the scheduler pass did not re-drive the stalled re-send")
	}
}

// TestResolutionStateFromFrame pins the RPC-poll conversion the lossy-bus
// insurance depends on.
func TestResolutionStateFromFrame(t *testing.T) {
	t.Parallel()
	target := domaintest.ID("poll-target")
	state := resolutionStateFromFrame(protocol.IdentityResolutionFrame{
		ResolutionID:     "r-9",
		Target:           target.String(),
		Lifecycle:        string(domain.IdentityResolutionSucceeded),
		Authority:        string(domain.IdentityAuthorityAuthoritative),
		DMAvailable:      string(domain.DMAvailabilityYes),
		Usable:           true,
		AnswerAttemptGen: 42,
	})
	if state.ResolutionID != "r-9" || state.Target != target ||
		state.Lifecycle != domain.IdentityResolutionSucceeded ||
		state.Authority != domain.IdentityAuthorityAuthoritative ||
		state.AnswerAttemptGen != 42 {
		t.Fatalf("converted state = %+v", state)
	}
	if broken := resolutionStateFromFrame(protocol.IdentityResolutionFrame{Target: "not-an-identity"}); !broken.Target.IsZero() {
		t.Fatalf("a malformed target produced a usable state: %+v", broken)
	}
}

// TestRecoveryProofScopesIsolated: under bilateral rotation the receiver
// job and a sender root arm on the SAME resolution with different
// watermarks — a completion grants each consumer strictly by its own
// anchor, and a grant of one is never consumable by the other.
func TestRecoveryProofScopesIsolated(t *testing.T) {
	t.Parallel()
	router, _, _ := newRecoveryRouter(t)
	peer := domaintest.ID("bilateral-peer")
	const root = "bilateral-root"

	jobKey := recoveryProofKey{peer: peer.String(), scope: recoveryProofScopeJob}
	rootKey := recoveryProofKey{peer: peer.String(), scope: root}
	router.recovery.mu.Lock()
	router.recovery.proofs[jobKey] = recoveryProofRequest{resolutionID: "r-shared", watermark: 5}
	router.recovery.proofs[rootKey] = recoveryProofRequest{resolutionID: "r-shared", watermark: 10}
	router.recovery.mu.Unlock()

	// The answer's attempt (gen 7) postdates the job's question but
	// PREDATES the sender root's — only the job may be granted.
	router.recovery.noteResolution(ebus.IdentityResolutionState{
		ResolutionID:     "r-shared",
		Target:           peer,
		Lifecycle:        domain.IdentityResolutionSucceeded,
		Authority:        domain.IdentityAuthorityAuthoritative,
		AnswerAttemptGen: 7,
	})
	if router.recovery.consumeRecoveryProof(peer.String(), root) {
		t.Fatal("the sender root consumed a proof of a question asked before its failure")
	}
	if !router.recovery.consumeRecoveryProof(peer.String(), recoveryProofScopeJob) {
		t.Fatal("the receiver job's own grant was withheld")
	}
}

// TestResendTerminalRetriedNotDropped: once the replacement left, a failed
// terminal write moves the entry into terminalDebts — retried until it
// commits, never silently dropped, and holding NO active-pool slot (the
// debt is pure DB work; a peer whose terminal cannot commit yet must not
// shrink the shared 20).
func TestResendTerminalRetriedNotDropped(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("terminal-peer")
	originalID := testUUID(0x60)
	replacementID := testUUID(0x61)
	const root = "terminal-root"

	// The replacement row does not exist yet: the terminal write fails
	// WHOLE and the entry must survive as a DEBT for the retry.
	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: originalID, Sender: id.Address, Recipient: peer.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append original: %v", err)
	}
	router.recovery.mu.Lock()
	router.recovery.pendingResends[root] = recoveryResend{
		originalID: originalID, peer: peer.String(), sentReplacementID: replacementID,
	}
	router.recovery.mu.Unlock()

	router.recovery.tryResend(root)
	router.recovery.mu.Lock()
	_, stillActive := router.recovery.pendingResends[root]
	_, indebted := router.recovery.terminalDebts[root]
	router.recovery.mu.Unlock()
	if stillActive {
		t.Fatal("a terminal debt kept its active-pool slot")
	}
	if !indebted {
		t.Fatal("a failed terminal write dropped the resend — the original would race its replacement")
	}
	if marks, _, _ := client.chatLog.EntryRecoveryMarks(originalID); marks.SupersededBy != "" {
		t.Fatal("a failed terminal left a half-written supersede")
	}

	// The replacement lands (the row exists now): the scheduler pass pays
	// the debt and settles it.
	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: replacementID, Sender: id.Address, Recipient: peer.String(),
		Body: "sealed-2", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append replacement: %v", err)
	}
	router.recovery.pass(context.Background())
	router.recovery.mu.Lock()
	_, indebted = router.recovery.terminalDebts[root]
	router.recovery.mu.Unlock()
	if indebted {
		t.Fatal("the retried terminal did not settle the debt")
	}
	marks, _, err := client.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || marks.SupersededBy != replacementID {
		t.Fatalf("terminal marks missing: %+v err=%v", marks, err)
	}
}

// TestCrashedTerminalDebtRestoredNotResent: after a restart, an intent
// whose replacement row already exists is a terminal DEBT — restored into
// the debt ledger and paid through the one common terminal flow, never
// treated as a waiting resend that would re-prove and re-send an id that
// already went out. A debt whose original vanished settles clean.
func TestCrashedTerminalDebtRestoredNotResent(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("debt-peer")

	// Case 1: original and replacement rows exist, no in-memory state —
	// the pass restores the debt and pays it without any proof request.
	originalID := testUUID(0xD0)
	replacementID := testUUID(0xD1)
	for _, row := range []string{originalID, replacementID} {
		if err := client.chatLog.Append("dm", self, chatlog.Entry{
			ID: row, Sender: id.Address, Recipient: peer.String(),
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append %s: %v", row, err)
		}
	}
	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: originalID, OriginalID: originalID, Peer: peer.String(),
		ReplacementID: replacementID, CreatedAt: time.Now().UTC().Add(-time.Minute),
	})
	router.recovery.pass(context.Background())
	marks, _, err := client.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || marks.SupersededBy != replacementID {
		t.Fatalf("restored debt not paid: %+v err=%v", marks, err)
	}
	if intents, _ := client.chatLog.ResendIntents(context.Background(), 10); len(intents) != 0 {
		t.Fatalf("paid debt left its intent: %v", intents)
	}
	router.recovery.mu.Lock()
	_, proofArmed := router.recovery.proofs[recoveryProofKey{peer: peer.String(), scope: originalID}]
	router.recovery.mu.Unlock()
	if proofArmed {
		t.Fatal("a terminal debt armed a proof lookup — it was treated as a waiting resend")
	}

	// Case 2: the replacement exists but the ORIGINAL vanished — nothing
	// left to supersede, the debt settles clean instead of wedging.
	ghostOriginal := testUUID(0xD2)
	ghostReplacement := testUUID(0xD3)
	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: ghostReplacement, Sender: id.Address, Recipient: peer.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append ghost replacement: %v", err)
	}
	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: ghostOriginal, OriginalID: ghostOriginal, Peer: peer.String(),
		ReplacementID: ghostReplacement, CreatedAt: time.Now().UTC().Add(-time.Minute),
	})
	router.recovery.pass(context.Background())
	router.recovery.mu.Lock()
	_, indebted := router.recovery.terminalDebts[ghostOriginal]
	router.recovery.mu.Unlock()
	if indebted {
		t.Fatal("a debt with a vanished original wedged instead of settling")
	}
	if intents, _ := client.chatLog.ResendIntents(context.Background(), 10); len(intents) != 0 {
		t.Fatalf("a moot debt left its intent: %v", intents)
	}
}

// TestOutgoingSendEstablishesFromAnySurface: the established fact fires
// at the SendDirectMessage chokepoint — a send that bypasses the router
// (the file-transfer bridge, direct client calls) still qualifies.
func TestOutgoingSendEstablishesFromAnySurface(t *testing.T) {
	t.Parallel()
	_, client, _ := newRecoveryRouter(t)
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient: %v", err)
	}
	reply, err := client.rpc.LocalRequestFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: recipient.Address,
			PubKey:  identity.PublicKeyBase64(recipient.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(recipient),
		}},
	})
	if err != nil || reply.Type != "contacts_imported" {
		t.Fatalf("import contact: %v %v", reply.Type, err)
	}

	// The DIRECT client path — no DMRouter.SendMessage involved.
	if _, err := client.dm.SendDirectMessage(context.Background(),
		domain.PeerIdentityFromWire(recipient.Address), domain.OutgoingDM{Body: "a file caption"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	established, err := client.chatLog.IsEstablished(recipient.Address)
	if err != nil || !established {
		t.Fatalf("a direct send did not establish the peer: %v %v", established, err)
	}
}

// TestAwaitingTurnBlocksEventRetry: an intent the scheduler selected but
// has not yet reached is not event-runnable — a resolution event landing
// mid-pass must not run a later item ahead of its computed position.
func TestAwaitingTurnBlocksEventRetry(t *testing.T) {
	t.Parallel()
	router, _, _ := newRecoveryRouter(t)
	peer := domaintest.ID("turn-peer")
	root := testUUID(0xE0)

	router.recovery.mu.Lock()
	router.recovery.pendingResends[root] = recoveryResend{
		originalID: root, peer: peer.String(), replacementID: testUUID(0xE1), awaitingTurn: true,
	}
	router.recovery.proofs[recoveryProofKey{peer: peer.String(), scope: root}] = recoveryProofRequest{resolutionID: "r-turn", granted: true}
	router.recovery.mu.Unlock()

	router.recovery.retryPendingResends(peer.String())

	router.recovery.mu.Lock()
	request := router.recovery.proofs[recoveryProofKey{peer: peer.String(), scope: root}]
	router.recovery.mu.Unlock()
	if !request.granted {
		t.Fatal("an event retry drove an item still awaiting its computed turn (proof consumed)")
	}
}

// TestResendClaimDefersRelease: a release (expiry, eviction cleanup)
// arriving while a root's claim is held must not strip the entry or its
// durable intent mid-send — it is recorded and executed when the claim
// drops.
func TestResendClaimDefersRelease(t *testing.T) {
	t.Parallel()
	router, client, _ := newRecoveryRouter(t)
	peer := domaintest.ID("claimed-peer")
	root := testUUID(0xC0)

	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: root, OriginalID: root, Peer: peer.String(),
		ReplacementID: testUUID(0xC1), CreatedAt: time.Now().UTC(),
	})
	router.recovery.mu.Lock()
	router.recovery.pendingResends[root] = recoveryResend{
		originalID: root, peer: peer.String(), replacementID: testUUID(0xC1), busy: true,
	}
	router.recovery.mu.Unlock()

	// The release arrives mid-claim: everything must survive.
	router.recovery.releaseResend(client.chatLog, root)
	router.recovery.mu.Lock()
	entry, stillQueued := router.recovery.pendingResends[root]
	router.recovery.mu.Unlock()
	if !stillQueued || !entry.pendingRelease {
		t.Fatalf("a mid-claim release stripped the entry: queued=%v %+v", stillQueued, entry)
	}
	if _, intact, _ := client.chatLog.ResendIntentByRoot(root); !intact {
		t.Fatal("a mid-claim release deleted the durable intent — the send would leave uninsured")
	}

	// The claim drops: the deferred release executes in full.
	router.recovery.unclaimResend(client.chatLog, root)
	router.recovery.mu.Lock()
	_, stillQueued = router.recovery.pendingResends[root]
	router.recovery.mu.Unlock()
	if stillQueued {
		t.Fatal("the deferred release did not execute at unclaim")
	}
	if _, intact, _ := client.chatLog.ResendIntentByRoot(root); intact {
		t.Fatal("the deferred release left the durable intent behind")
	}
}

// TestResendStaleTaskRetiresOnIDMismatch: a queued task whose replacement
// id no longer matches the durable intent (the root was released and
// re-admitted while the task waited — the ABA case) retires WITHOUT
// sending and WITHOUT touching the new incarnation's intent.
func TestResendStaleTaskRetiresOnIDMismatch(t *testing.T) {
	t.Parallel()
	router, client, _ := newRecoveryRouter(t)
	peer := domaintest.ID("aba-peer")
	root := testUUID(0xC2)

	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: root, OriginalID: root, Peer: peer.String(),
		ReplacementID: testUUID(0xC3), CreatedAt: time.Now().UTC(),
	})
	// The STALE in-memory task still carries the previous incarnation's id
	// and a granted proof, so it reaches the durable re-check.
	router.recovery.mu.Lock()
	router.recovery.pendingResends[root] = recoveryResend{
		originalID: root, peer: peer.String(), replacementID: testUUID(0xC4),
	}
	router.recovery.proofs[recoveryProofKey{peer: peer.String(), scope: root}] = recoveryProofRequest{resolutionID: "r-aba", granted: true}
	router.recovery.mu.Unlock()

	router.recovery.tryResend(root)

	router.recovery.mu.Lock()
	_, stillQueued := router.recovery.pendingResends[root]
	router.recovery.mu.Unlock()
	if stillQueued {
		t.Fatal("the stale task survived the id mismatch")
	}
	intent, intact, err := client.chatLog.ResendIntentByRoot(root)
	if err != nil || !intact || intent.ReplacementID != testUUID(0xC3) {
		t.Fatalf("the new incarnation's intent was damaged: intact=%v %+v err=%v", intact, intent, err)
	}
}

// admitTestResendIntent seeds one durable intent the way a crashed
// predecessor would have left it.
func admitTestResendIntent(t *testing.T, store *chatlog.Store, intent chatlog.ResendIntent) {
	t.Helper()
	if _, admitted, _, err := store.AdmitResendIntent(intent, recoveryMaxResendsPerPeer, recoveryBacklogLimit, chatlog.RecoveryProtectedWork{}); err != nil || !admitted {
		t.Fatalf("admit intent %s: admitted=%v err=%v", intent.Root, admitted, err)
	}
}

// TestReconcileResendIntentsFinishesCrashedTerminal: a durable intent
// with no in-memory queue entry is a crashed predecessor's; the intent
// NAMES its pre-minted replacement, so a present row finishes the
// terminal, an absent row RESTORES the sender task (re-send under the
// same id — the receiver's dedup absorbs a duplicate), and the cycle
// lifetime ages out an intent whose peer never resolves.
func TestReconcileResendIntentsFinishesCrashedTerminal(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("crash-peer")
	originalID := testUUID(0x70)
	replacementID := testUUID(0x71)
	now := time.Now().UTC()

	for _, row := range []string{originalID, replacementID} {
		if err := client.chatLog.Append("dm", self, chatlog.Entry{
			ID: row, Sender: id.Address, Recipient: peer.String(),
			Body: "sealed", CreatedAt: now.Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append %s: %v", row, err)
		}
	}
	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: originalID, OriginalID: originalID, Peer: peer.String(),
		ReplacementID: replacementID, CreatedAt: now.Add(-time.Minute),
	})

	router.recovery.pass(context.Background())

	marks, _, err := client.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || marks.SupersededBy != replacementID {
		t.Fatalf("crashed terminal not recovered: %+v err=%v", marks, err)
	}
	if intents, _ := client.chatLog.ResendIntents(context.Background(), 10); len(intents) != 0 {
		t.Fatalf("intent not settled: %v", intents)
	}

	// A missing replacement row: the activation sweep RESTORES the sender
	// task under the stored id instead of waiting passively or inventing
	// a terminal.
	ghostOriginal := testUUID(0x72)
	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: ghostOriginal, Sender: id.Address, Recipient: peer.String(),
		Body: "sealed", CreatedAt: now.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append ghost original: %v", err)
	}
	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: ghostOriginal, OriginalID: ghostOriginal, Peer: peer.String(),
		ReplacementID: testUUID(0x73), CreatedAt: now,
	})
	router.recovery.pass(context.Background())
	if marks, _, _ := client.chatLog.EntryRecoveryMarks(ghostOriginal); marks.SupersededBy != "" {
		t.Fatal("a never-sent intent invented a terminal")
	}
	router.recovery.mu.Lock()
	restored, active := router.recovery.pendingResends[ghostOriginal]
	router.recovery.mu.Unlock()
	if !active || restored.replacementID != testUUID(0x73) {
		t.Fatalf("the crashed sender task was not restored with its stored id: active=%v %+v", active, restored)
	}
	if intents, _ := client.chatLog.ResendIntents(context.Background(), 10); len(intents) != 1 {
		t.Fatalf("an in-flight intent was dropped: %v", intents)
	}

	// Past the cycle lifetime an UNOWNED intent ages out.
	agedRoot := testUUID(0x74)
	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: agedRoot, OriginalID: agedRoot, Peer: peer.String(),
		ReplacementID: testUUID(0x75), CreatedAt: now.Add(-8 * 24 * time.Hour),
	})
	router.recovery.pass(context.Background())
	intents, err := client.chatLog.ResendIntents(context.Background(), 10)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	for _, intent := range intents {
		if intent.Root == agedRoot {
			t.Fatal("an aged intent survived the sweep")
		}
	}
}

// TestActiveResendAgesOut: the seven-day deadline reaches a resend whose
// root sits in the ACTIVE queue too — an unreachable peer must not hold an
// active slot, a backlog slot and a proof loop forever.
func TestActiveResendAgesOut(t *testing.T) {
	t.Parallel()
	router, client, _ := newRecoveryRouter(t)
	peer := domaintest.ID("forever-peer")
	root := testUUID(0xB0)

	admitTestResendIntent(t, client.chatLog, chatlog.ResendIntent{
		Root: root, OriginalID: root, Peer: peer.String(),
		ReplacementID: testUUID(0xB1), CreatedAt: time.Now().UTC().Add(-8 * 24 * time.Hour),
	})
	router.recovery.mu.Lock()
	router.recovery.pendingResends[root] = recoveryResend{
		originalID: root, peer: peer.String(), replacementID: testUUID(0xB1),
	}
	router.recovery.mu.Unlock()

	router.recovery.pass(context.Background())

	router.recovery.mu.Lock()
	_, stillActive := router.recovery.pendingResends[root]
	router.recovery.mu.Unlock()
	if stillActive {
		t.Fatal("an expired resend kept its active slot")
	}
	if intents, _ := client.chatLog.ResendIntents(context.Background(), 10); len(intents) != 0 {
		t.Fatalf("an expired resend kept its backlog intent: %v", intents)
	}
}

// TestDecryptSuccessHookIndependentOfActiveChat: the established fact and
// the retry_of acceptance fire at the DECRYPT chokepoint — a replacement
// landing in a background conversation (no active chat, no UI branch)
// still supersedes its original and closes the job.
func TestDecryptSuccessHookIndependentOfActiveChat(t *testing.T) {
	t.Parallel()
	_, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	senderID := domain.PeerIdentityFromWire(sender.Address)
	originalID := testUUID(0xA0)

	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: originalID, Sender: sender.Address, Recipient: id.Address,
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append original: %v", err)
	}
	if changed, err := client.chatLog.MarkDecryptFailed(originalID); err != nil || !changed {
		t.Fatalf("flag: %v %v", changed, err)
	}
	// The sender's keys must be known for the replacement to decrypt.
	reply, err := client.rpc.LocalRequestFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: sender.Address,
			PubKey:  identity.PublicKeyBase64(sender.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(sender),
		}},
	})
	if err != nil || reply.Type != "contacts_imported" {
		t.Fatalf("import contact: %v %v", reply.Type, err)
	}

	replacementID := testUUID(0xA1)
	ciphertext, err := directmsg.EncryptForParticipants(sender, domain.DMRecipient{
		Address:      self,
		BoxKeyBase64: identity.BoxPublicKeyBase64(id.BoxPublicKey),
	}, domain.OutgoingDM{Body: "replacement", RetryOf: domain.MessageID(originalID)})
	if err != nil {
		t.Fatalf("encrypt replacement: %v", err)
	}
	// No SelectPeer, no active conversation: the bare decrypt is all that
	// happens — exactly the background-chat / history-load situation.
	msg := client.dm.DecryptIncomingMessage(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: "dm",
		MessageID: replacementID, Sender: sender.Address, Recipient: id.Address,
		Body: ciphertext, CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if msg == nil || msg.RetryOf != domain.MessageID(originalID) {
		t.Fatalf("replacement did not decrypt: %+v", msg)
	}

	marks, _, err := client.chatLog.EntryRecoveryMarks(originalID)
	if err != nil || marks.DecryptFailed || marks.SupersededBy != replacementID {
		t.Fatalf("background replacement did not supersede: %+v err=%v", marks, err)
	}
	established, err := client.chatLog.IsEstablished(senderID.String())
	if err != nil || !established {
		t.Fatalf("decrypted incoming did not establish the peer: %v %v", established, err)
	}
}

// TestJobDeadlineAnchoredToCycle: the deadline derives from the immutable
// cycle anchor — recovering the oldest row (or every row) must not roll
// the clock forward while the cycle is open; only a closed cycle re-arms.
func TestJobDeadlineAnchoredToCycle(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	peer := domaintest.ID("anchor-peer")
	rowID := testUUID(0x80)
	start := time.Now().UTC()

	if err := client.chatLog.Append("dm", self, chatlog.Entry{
		ID: rowID, Sender: peer.String(), Recipient: id.Address,
		Body: "sealed", CreatedAt: start.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if changed, err := client.chatLog.MarkDecryptFailed(rowID); err != nil || !changed {
		t.Fatalf("flag: %v %v", changed, err)
	}

	first := router.recovery.jobDeadline(peer.String(), start)
	// The oldest row recovers; a later re-derivation (the eviction →
	// re-admission path) must keep the ORIGINAL clock.
	if applied, err := client.chatLog.MarkSupersededCollapsing(rowID, testUUID(0x81), rowID); err != nil || !applied {
		t.Fatalf("supersede: %v", err)
	}
	later := start.Add(48 * time.Hour)
	if again := router.recovery.jobDeadline(peer.String(), later); !again.Equal(first) {
		t.Fatalf("deadline moved %v → %v — the cycle clock restarted", first, again)
	}
	// A CLOSED cycle re-arms from scratch (the row recovered above, so
	// the idle close fires).
	router.recovery.closeCycleIfIdle(client.chatLog, peer.String())
	if fresh := router.recovery.jobDeadline(peer.String(), later); fresh.Equal(first) {
		t.Fatal("a closed cycle kept the old anchor")
	}
}

// TestResendQuotasBoundSenderLeg: the sender leg obeys the task's
// active-work quotas — one peer's notice stream caps at 3 queued
// re-sends, the pool at 20 — and a durable intent holds the single-flight
// even with the in-memory queue empty (the crashed-predecessor case).
func TestResendQuotasBoundSenderLeg(t *testing.T) {
	t.Parallel()
	router, client, id := newRecoveryRouter(t)
	self := domain.PeerIdentityFromWire(id.Address)
	flood := domaintest.ID("flood-peer")

	appendOutgoing := func(t *testing.T, n byte) string {
		t.Helper()
		rowID := testUUID(n)
		ciphertext, err := directmsg.EncryptForParticipants(id, domain.DMRecipient{
			Address:      flood,
			BoxKeyBase64: throwawayBoxKey(t),
		}, domain.OutgoingDM{Body: "resend me"})
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		if err := client.chatLog.Append("dm", self, chatlog.Entry{
			ID: rowID, Sender: id.Address, Recipient: flood.String(),
			Body: ciphertext, CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
		return rowID
	}
	notice := func(t *testing.T, rowID string) {
		t.Helper()
		payload, err := domain.MarshalDecryptFailedPayload(domain.DecryptFailedPayload{MessageID: domain.MessageID(rowID)})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		router.handleInboundDecryptFailed(flood.String(), payload)
	}
	pendingCount := func() int {
		router.recovery.mu.Lock()
		defer router.recovery.mu.Unlock()
		return len(router.recovery.pendingResends)
	}

	rows := make([]string, 0, 5)
	for n := byte(0x90); n < 0x95; n++ {
		rows = append(rows, appendOutgoing(t, n))
	}
	for _, rowID := range rows {
		notice(t, rowID)
	}
	// The backlog caps at 3 per peer: the two overflow notices were
	// refused at admission, not queued elsewhere — and nothing activates
	// outside the scheduler pass.
	if got := pendingCount(); got != 0 {
		t.Fatalf("resends activated outside the scheduler pass: %d", got)
	}
	if intents, _ := client.chatLog.ResendIntents(context.Background(), 10); len(intents) != recoveryMaxResendsPerPeer {
		t.Fatalf("backlog intents = %d, want the per-peer cap %d", len(intents), recoveryMaxResendsPerPeer)
	}
	router.recovery.pass(context.Background())
	if got := pendingCount(); got != recoveryMaxResendsPerPeer {
		t.Fatalf("active resends = %d, want the per-peer cap %d", got, recoveryMaxResendsPerPeer)
	}

	// A repeated notice for an admitted root reuses the intent's STORED
	// replacement id even after the in-memory queue lost its entries (the
	// crashed-predecessor case): no divergent second id, no lost task.
	repeatRoot := rows[0]
	var storedID string
	for _, intent := range func() []chatlog.ResendIntent {
		intents, _ := client.chatLog.ResendIntents(context.Background(), 10)
		return intents
	}() {
		if intent.Root == repeatRoot {
			storedID = intent.ReplacementID
		}
	}
	if storedID == "" {
		t.Fatal("test setup: no stored intent for the first root")
	}
	router.recovery.mu.Lock()
	router.recovery.pendingResends = map[string]recoveryResend{}
	router.recovery.mu.Unlock()
	notice(t, repeatRoot)
	router.recovery.pass(context.Background())
	router.recovery.mu.Lock()
	restored, active := router.recovery.pendingResends[repeatRoot]
	router.recovery.mu.Unlock()
	if !active || restored.replacementID != storedID {
		t.Fatalf("repeated notice diverged from the stored id: active=%v got=%q want=%q", active, restored.replacementID, storedID)
	}
}
