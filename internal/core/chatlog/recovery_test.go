package chatlog

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func newRecoveryTestStore(t *testing.T) (*Store, domain.PeerIdentity, domain.PeerIdentity) {
	t.Helper()
	self := domaintest.ID("self")
	peer := domaintest.ID("peer")
	store := newTestStore(t, self)
	return store, self, peer
}

func appendIncomingRow(t *testing.T, store *Store, self, peer domain.PeerIdentity, id, metadata string) {
	t.Helper()
	err := store.Append(context.Background(), "dm", self, Entry{
		ID:        id,
		Sender:    peer.String(),
		Recipient: self.String(),
		Body:      "sealed-envelope-bytes",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Metadata:  metadata,
	})
	if err != nil {
		t.Fatalf("append row %s: %v", id, err)
	}
}

// TestRecoveryMarksLifecycle: flag → state → supersede, with the §4.10
// idempotency suppressor, the unread collapse and foreign metadata keys
// surviving every merge.
func TestRecoveryMarksLifecycle(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	const rowID = "0b7d81f2-9c48-4a6e-9d10-000000000001"
	appendIncomingRow(t, store, self, peer, rowID, `{"foreign_key":"kept"}`)

	changed, err := store.MarkDecryptFailed(context.Background(), rowID)
	if err != nil || !changed {
		t.Fatalf("first flag: changed=%v err=%v", changed, err)
	}
	changed, err = store.MarkDecryptFailed(context.Background(), rowID)
	if err != nil || changed {
		t.Fatalf("second flag must be the idempotent no-op: changed=%v err=%v", changed, err)
	}

	marks, exists, err := store.EntryRecoveryMarks(context.Background(), rowID)
	if err != nil || !exists {
		t.Fatalf("read marks: exists=%v err=%v", exists, err)
	}
	if !marks.DecryptFailed || marks.DecryptState != DecryptStatePendingNotice {
		t.Fatalf("marks = %+v", marks)
	}

	unreadBefore, err := store.UnseenIncomingIDsFor(context.Background(), peer)
	if err != nil || len(unreadBefore) != 1 {
		t.Fatalf("unread before = %v err=%v, want one message", unreadBefore, err)
	}

	const replacementID = "0b7d81f2-9c48-4a6e-9d10-000000000002"
	if applied, err := store.MarkSupersededCollapsing(context.Background(), rowID, replacementID, rowID); err != nil || !applied {
		t.Fatalf("supersede: %v", err)
	}
	marks, _, err = store.EntryRecoveryMarks(context.Background(), rowID)
	if err != nil {
		t.Fatalf("read marks after supersede: %v", err)
	}
	if marks.DecryptFailed || marks.SupersededBy != replacementID || marks.RetryRootID != rowID ||
		marks.DecryptState != DecryptStateRecovered {
		t.Fatalf("marks after supersede = %+v", marks)
	}

	// The unread collapse: the unreadable original no longer counts.
	unreadAfter, err := store.UnseenIncomingIDsFor(context.Background(), peer)
	if err != nil || len(unreadAfter) != 0 {
		t.Fatalf("unread after = %v err=%v, want none (collapse failed)", unreadAfter, err)
	}

	// Foreign metadata survived every merge.
	entry, found, err := store.EntryByID(context.Background(), domain.MessageID(rowID))
	if err != nil || !found {
		t.Fatalf("read row: %v", err)
	}
	if !strings.Contains(entry.Metadata, `"foreign_key":"kept"`) {
		t.Fatalf("foreign metadata lost: %s", entry.Metadata)
	}
}

// TestSenderResendTerminal: the one-transaction sender terminal links the
// original to its replacement, stamps both with the chain root, and stays
// metadata-only — marking one's own outgoing row 'seen' would forge a peer
// confirmation. A terminal naming a missing replacement row must fail
// WHOLE: a half-written terminal re-opens the original for ordinary retry
// while resetting the chain budget.
func TestSenderResendTerminal(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	const rowID = "0b7d81f2-9c48-4a6e-9d10-000000000003"
	const replacementID = "0b7d81f2-9c48-4a6e-9d10-000000000004"
	// OUTGOING rows: self → peer (the original and its re-send).
	for _, id := range []string{rowID, replacementID} {
		if err := store.Append(context.Background(), "dm", self, Entry{
			ID: id, Sender: self.String(), Recipient: peer.String(),
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	if err := store.MarkResendTerminal(context.Background(), rowID, "0b7d81f2-9c48-4a6e-9d10-00000000dead", rowID); err == nil {
		t.Fatal("a terminal naming a missing replacement committed")
	}
	if marks, _, _ := store.EntryRecoveryMarks(context.Background(), rowID); marks.SupersededBy != "" {
		t.Fatal("a failed terminal left a half-written supersede link")
	}

	if err := store.MarkResendTerminal(context.Background(), rowID, replacementID, rowID); err != nil {
		t.Fatalf("terminal: %v", err)
	}
	entry, found, err := store.EntryByID(context.Background(), rowID)
	if err != nil || !found {
		t.Fatalf("read: %v", err)
	}
	if entry.DeliveryStatus != StatusSent {
		t.Fatalf("delivery_status = %s — the sender-side terminal forged a confirmation", entry.DeliveryStatus)
	}
	marks, _, err := store.EntryRecoveryMarks(context.Background(), rowID)
	if err != nil || marks.SupersededBy != replacementID {
		t.Fatalf("metadata link missing: %+v err=%v", marks, err)
	}
	replacementMarks, _, err := store.EntryRecoveryMarks(context.Background(), replacementID)
	if err != nil || replacementMarks.RetryRootID != rowID {
		t.Fatalf("replacement chain stamp missing: %+v err=%v", replacementMarks, err)
	}
	if count, err := store.CountRetryChain(context.Background(), rowID); err != nil || count != 2 {
		t.Fatalf("chain count = %d err=%v, want original + replacement", count, err)
	}
}

// TestAdmitRecoveryJobBacklogBound: the durable backlog is capped with the
// §4.10 reservation — unknown peers own at most half of it and rotate by
// LRU at their share; established peers may fill the rest, and a backlog
// holding only established rows refuses an unknown newcomer outright.
func TestAdmitRecoveryJobBacklogBound(t *testing.T) {
	t.Parallel()
	store, _, _ := newRecoveryTestStore(t)
	now := time.Unix(1780000000, 0).UTC()
	const limit = 6 // unknown share = 3

	for i := 0; i < 3; i++ {
		peer := fmt.Sprintf("sybil-%02d", i)
		admitted, victim, err := store.AdmitRecoveryJob(context.Background(), peer, now.Add(time.Duration(i)*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{})
		if err != nil || !admitted || !victim.None() {
			t.Fatalf("admit %s: %v victim=%+v %v", peer, admitted, victim, err)
		}
	}
	// The unknown share (3) is full: a fourth unknown rotates the oldest
	// unknown out even though the global cap still has room — Sybil rows
	// can never grow past their half.
	admitted, victim, err := store.AdmitRecoveryJob(context.Background(), "sybil-03", now.Add(3*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{})
	if err != nil || !admitted {
		t.Fatalf("admit sybil-03: %v %v", admitted, err)
	}
	if !victim.Job || victim.Key != "sybil-00" {
		t.Fatalf("victim = %+v, want the oldest unknown sybil-00 (reservation rotation)", victim)
	}

	// Established peers fill the rest without touching anyone.
	for i, peer := range []string{"friend-a", "friend-b", "friend-c"} {
		if err := store.MarkEstablished(context.Background(), peer, EstablishedReasonOutgoing, now); err != nil {
			t.Fatalf("mark established: %v", err)
		}
		admitted, victim, err := store.AdmitRecoveryJob(context.Background(), peer, now.Add(time.Duration(10+i)*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{})
		if err != nil || !admitted || !victim.None() {
			t.Fatalf("admit %s: %v victim=%+v %v", peer, admitted, victim, err)
		}
	}

	// The pool is full (3 unknown + 3 established): an established
	// newcomer evicts the oldest unknown; unknowns never displace an
	// established row.
	if err := store.MarkEstablished(context.Background(), "friend-d", EstablishedReasonOutgoing, now); err != nil {
		t.Fatalf("mark established: %v", err)
	}
	admitted, victim, err = store.AdmitRecoveryJob(context.Background(), "friend-d", now.Add(time.Hour), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{})
	if err != nil || !admitted || !victim.Job || victim.Key != "sybil-01" {
		t.Fatalf("established newcomer: admitted=%v victim=%+v err=%v, want eviction of sybil-01", admitted, victim, err)
	}

	// Fill the backlog with established rows only: an unknown newcomer is
	// refused — established rows never leave for it.
	for _, peer := range []string{"sybil-02", "sybil-03"} {
		if err := store.DeleteRecoveryJob(context.Background(), peer); err != nil {
			t.Fatalf("clear %s: %v", peer, err)
		}
	}
	for i, peer := range []string{"friend-e", "friend-f"} {
		if err := store.MarkEstablished(context.Background(), peer, EstablishedReasonOutgoing, now); err != nil {
			t.Fatalf("mark established: %v", err)
		}
		if admitted, _, err := store.AdmitRecoveryJob(context.Background(), peer, now.Add(time.Duration(20+i)*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{}); err != nil || !admitted {
			t.Fatalf("admit %s: %v %v", peer, admitted, err)
		}
	}
	admitted, victim, err = store.AdmitRecoveryJob(context.Background(), "late-sybil", now.Add(2*time.Hour), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if admitted || !victim.None() {
		t.Fatalf("an unknown newcomer displaced an established job (admitted=%v victim=%+v)", admitted, victim)
	}
}

// TestRecoveryBacklogSharedAcrossLegs: receiver jobs and sender intents
// count against ONE global bound, eviction picks the oldest unknown row
// across both tables, and a protected (in-flight) resend root is never
// the victim.
func TestRecoveryBacklogSharedAcrossLegs(t *testing.T) {
	t.Parallel()
	store, _, _ := newRecoveryTestStore(t)
	now := time.Unix(1780000000, 0).UTC()
	const limit = 5 // unknown share = 2

	markEstablished := func(t *testing.T, peer string) {
		t.Helper()
		if err := store.MarkEstablished(context.Background(), peer, EstablishedReasonOutgoing, now); err != nil {
			t.Fatalf("mark established %s: %v", peer, err)
		}
	}

	// Two unknown rows — one per table — then established rows to the cap.
	if _, admitted, _, err := store.AdmitResendIntent(context.Background(), ResendIntent{
		Root: "root-b", OriginalID: "orig-b", Peer: "peer-b",
		ReplacementID: "repl-b", CreatedAt: now,
	}, 3, limit, RecoveryProtectedWork{}); err != nil || !admitted {
		t.Fatalf("admit root-b: %v %v", admitted, err)
	}
	if admitted, victim, err := store.AdmitRecoveryJob(context.Background(), "job-a", now.Add(time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{}); err != nil || !admitted || !victim.None() {
		t.Fatalf("admit job-a: %v victim=%+v err=%v", admitted, victim, err)
	}
	for i, peer := range []string{"job-c", "job-d", "job-e"} {
		markEstablished(t, peer)
		if admitted, victim, err := store.AdmitRecoveryJob(context.Background(), peer, now.Add(time.Duration(2+i)*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{}); err != nil || !admitted || !victim.None() {
			t.Fatalf("admit %s: %v victim=%+v err=%v", peer, admitted, victim, err)
		}
	}

	// The pool is FULL (2 unknown + 3 established). The protected root-b
	// is the OLDEST unknown row, yet the eviction skips it — a possibly
	// in-flight send must keep its crash insurance — and takes the
	// next-oldest unknown across the table boundary: the job.
	markEstablished(t, "job-f")
	admitted, victim, err := store.AdmitRecoveryJob(context.Background(), "job-f", now.Add(10*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{ResendRoots: []string{"root-b"}})
	if err != nil || !admitted {
		t.Fatalf("admit job-f: %v %v", admitted, err)
	}
	if !victim.Job || victim.Key != "job-a" {
		t.Fatalf("victim = %+v, want job-a — the protected root-b must never be evicted", victim)
	}

	// Without protection the intent is the oldest unknown row: a JOB
	// admission evicts a RESEND intent — one backlog, both tables.
	markEstablished(t, "job-g")
	admitted, victim, err = store.AdmitRecoveryJob(context.Background(), "job-g", now.Add(11*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{})
	if err != nil || !admitted {
		t.Fatalf("admit job-g: %v %v", admitted, err)
	}
	if victim.Job || victim.Key != "root-b" || victim.Peer != "peer-b" {
		t.Fatalf("victim = %+v, want the resend intent root-b of peer-b (cross-table eviction with the peer named)", victim)
	}

	// Established rows only now: an unknown newcomer of either kind is
	// refused — the pool never displaces established for unknown.
	_, admitted2, victim, err := store.AdmitResendIntent(context.Background(), ResendIntent{
		Root: "root-h", OriginalID: "orig-h", Peer: "peer-h",
		ReplacementID: "repl-h", CreatedAt: now.Add(12 * time.Second),
	}, 3, limit, RecoveryProtectedWork{})
	if err != nil {
		t.Fatalf("admit root-h: %v", err)
	}
	if admitted2 || !victim.None() {
		t.Fatalf("an unknown intent displaced an established row (admitted=%v victim=%+v)", admitted2, victim)
	}

	// A job whose notice attempt is running is protected the same way an
	// in-flight resend root is: seed one unknown job back, then evict with
	// its peer protected — the eviction must fall through to nothing
	// (job-x is the only unknown row) and refuse.
	if err := store.DeleteRecoveryJob(context.Background(), "job-c"); err != nil {
		t.Fatalf("free a slot: %v", err)
	}
	if admitted, _, err := store.AdmitRecoveryJob(context.Background(), "job-x", now.Add(20*time.Second), now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{}); err != nil || !admitted {
		t.Fatalf("admit job-x: %v %v", admitted, err)
	}
	markEstablished(t, "job-y")
	admitted, victim, err = store.AdmitRecoveryJob(context.Background(), "job-y", now.Add(21*time.Second), now.Add(24*time.Hour), 3, limit,
		RecoveryProtectedWork{JobPeers: []string{"job-x"}})
	if err != nil {
		t.Fatalf("admit job-y: %v", err)
	}
	if admitted || !victim.None() {
		t.Fatalf("a protected in-attempt job was evicted (admitted=%v victim=%+v)", admitted, victim)
	}
}

// TestEstablishedBackfillFromHistory: a database that predates the
// peer_established table seeds the facts from history at open — every
// peer the user already messaged qualifies through the outgoing rule, so
// long-standing real contacts never start as Sybil-evictable unknowns.
func TestEstablishedBackfillFromHistory(t *testing.T) {
	t.Parallel()
	self := domaintest.ID("self")
	oldFriend := domaintest.ID("old-friend")
	strangerSender := domaintest.ID("stranger")

	storePath := filepath.Join(t.TempDir(), "state.db")
	store := newTestStoreAt(t, storePath, self)
	// Pre-feature history: an outgoing message to a friend, an incoming
	// row from a stranger (receipt alone must NOT establish).
	if err := store.Append(context.Background(), "dm", self, Entry{
		ID: "0b7d81f2-9c48-4a6e-9d10-0000000000b0", Sender: self.String(), Recipient: oldFriend.String(),
		Body: "hi", CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("append outgoing: %v", err)
	}
	appendIncomingRow(t, store, self, strangerSender, "0b7d81f2-9c48-4a6e-9d10-0000000000b1", "")
	// Simulate the pre-feature state: the fact table is emptied, as if the
	// rows had been written by a build without it.
	if _, err := store.db.ExecContext(context.Background(), `DELETE FROM peer_established`); err != nil {
		t.Fatalf("clear facts: %v", err)
	}
	// The backfill is an explicit start-up step now: the composition root
	// runs it once the shared database is open, instead of the repository
	// doing I/O in its constructor.
	reopened := newTestStoreAt(t, storePath, self)
	if err := reopened.BackfillEstablishedFromHistory(context.Background(), time.Now().UTC()); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if established, err := reopened.IsEstablished(context.Background(), oldFriend.String()); err != nil || !established {
		t.Fatalf("messaged peer not backfilled: %v %v", established, err)
	}
	if established, _ := reopened.IsEstablished(context.Background(), strangerSender.String()); established {
		t.Fatal("an incoming-only sender was backfilled — receipt alone must not establish")
	}
}

// TestRecoveryJobDurability: the job table survives a store reopen — a
// restart resumes notice retries instead of restarting lookups.
func TestRecoveryJobDurability(t *testing.T) {
	t.Parallel()
	self := domaintest.ID("self")
	peer := domaintest.ID("peer")
	now := time.Unix(1780000000, 0).UTC()

	storePath := filepath.Join(t.TempDir(), "state.db")
	store := newTestStoreAt(t, storePath, self)
	if err := store.UpsertRecoveryJob(context.Background(), peer.String(), now, now.Add(7*24*time.Hour)); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	// A second upsert must not reset the existing job.
	if err := store.UpsertRecoveryJob(context.Background(), peer.String(), now.Add(time.Hour), now.Add(8*24*time.Hour)); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	jobs, err := store.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs = %v err=%v", jobs, err)
	}
	job := jobs[0]
	job.State = DecryptStateWaitingRetry
	job.NoticeAttempts = 3
	job.LastNoticeAt = now.Add(10 * time.Minute)
	job.WaitUntil = now.Add(24 * time.Hour)
	if err := store.UpdateRecoveryJob(context.Background(), job); err != nil {
		t.Fatalf("update: %v", err)
	}
	reopened := newTestStoreAt(t, storePath, self)
	jobs, err = reopened.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs after reopen = %v err=%v", jobs, err)
	}
	got := jobs[0]
	if got.State != DecryptStateWaitingRetry || got.NoticeAttempts != 3 ||
		!got.LastNoticeAt.Equal(now.Add(10*time.Minute)) || !got.WaitUntil.Equal(now.Add(24*time.Hour)) {
		t.Fatalf("job lost state across reopen: %+v", got)
	}
	if err := reopened.DeleteRecoveryJob(context.Background(), peer.String()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if jobs, _ := reopened.RecoveryJobs(context.Background()); len(jobs) != 0 {
		t.Fatalf("job survived deletion: %v", jobs)
	}
}

// TestEstablishedMonotonic: the first qualifying event wins; nothing
// refreshes or revokes it.
func TestEstablishedMonotonic(t *testing.T) {
	t.Parallel()
	store, _, peer := newRecoveryTestStore(t)
	now := time.Unix(1780000000, 0)

	if established, err := store.IsEstablished(context.Background(), peer.String()); err != nil || established {
		t.Fatalf("fresh peer established=%v err=%v", established, err)
	}
	if err := store.MarkEstablished(context.Background(), peer.String(), EstablishedReasonOutgoing, now); err != nil {
		t.Fatalf("mark: %v", err)
	}
	// A later, different reason must NOT overwrite the first fact.
	if err := store.MarkEstablished(context.Background(), peer.String(), EstablishedReasonManual, now.Add(time.Hour)); err != nil {
		t.Fatalf("re-mark: %v", err)
	}
	var reason, at string
	if err := store.db.QueryRowContext(context.Background(), `SELECT established_reason, established_at FROM peer_established WHERE peer = ?`, peer.String()).
		Scan(&reason, &at); err != nil {
		t.Fatalf("read fact: %v", err)
	}
	if reason != EstablishedReasonOutgoing {
		t.Fatalf("reason = %s — the monotonic fact was overwritten", reason)
	}
	if established, err := store.IsEstablished(context.Background(), peer.String()); err != nil || !established {
		t.Fatalf("established=%v err=%v", established, err)
	}
}

// TestDecryptFailedEntriesAndChain: the flagged workset is bounded and
// scoped to one peer; the retry chain counts by root id.
func TestDecryptFailedEntriesAndChain(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	other := domaintest.ID("other")

	rows := []string{
		"0b7d81f2-9c48-4a6e-9d10-00000000000a",
		"0b7d81f2-9c48-4a6e-9d10-00000000000b",
		"0b7d81f2-9c48-4a6e-9d10-00000000000c",
	}
	for _, id := range rows {
		appendIncomingRow(t, store, self, peer, id, "")
	}
	appendIncomingRow(t, store, self, other, "0b7d81f2-9c48-4a6e-9d10-00000000000d", "")

	for _, id := range rows[:2] {
		if _, err := store.MarkDecryptFailed(context.Background(), id); err != nil {
			t.Fatalf("flag %s: %v", id, err)
		}
	}
	flagged, err := store.DecryptFailedEntries(context.Background(), peer.String(), self.String(), 64)
	if err != nil || len(flagged) != 2 {
		t.Fatalf("flagged = %d err=%v, want 2", len(flagged), err)
	}
	if flaggedOther, _ := store.DecryptFailedEntries(context.Background(), other.String(), self.String(), 64); len(flaggedOther) != 0 {
		t.Fatalf("foreign peer leaked into the workset: %v", flaggedOther)
	}

	// A two-hop chain of resend terminals: every stamped row counts once
	// however many fresh ids the hops mint.
	root := rows[0]
	if err := store.MarkResendTerminal(context.Background(), rows[0], rows[1], root); err != nil {
		t.Fatalf("first terminal: %v", err)
	}
	count, err := store.CountRetryChain(context.Background(), root)
	if err != nil || count != 2 {
		t.Fatalf("chain count = %d err=%v, want original + first resend", count, err)
	}
	if err := store.MarkResendTerminal(context.Background(), rows[1], rows[2], root); err != nil {
		t.Fatalf("second terminal: %v", err)
	}
	count, err = store.CountRetryChain(context.Background(), root)
	if err != nil || count != 3 {
		t.Fatalf("chain count = %d err=%v, want the whole chain", count, err)
	}
}

// TestRecoveryOrphanPeers: flagged rows whose job was refused or evicted
// are the reconciliation feed — peers with jobs, recovered rows and
// expired (terminal) rows are all excluded.
func TestRecoveryOrphanPeers(t *testing.T) {
	t.Parallel()
	store, self, orphan := newRecoveryTestStore(t)
	jobbed := domaintest.ID("jobbed-peer")
	expired := domaintest.ID("expired-peer")
	now := time.Unix(1780000000, 0).UTC()

	appendIncomingRow(t, store, self, orphan, "0b7d81f2-9c48-4a6e-9d10-00000000o001", "")
	appendIncomingRow(t, store, self, jobbed, "0b7d81f2-9c48-4a6e-9d10-00000000j001", "")
	appendIncomingRow(t, store, self, expired, "0b7d81f2-9c48-4a6e-9d10-00000000e001", "")
	for _, id := range []string{"0b7d81f2-9c48-4a6e-9d10-00000000o001", "0b7d81f2-9c48-4a6e-9d10-00000000j001", "0b7d81f2-9c48-4a6e-9d10-00000000e001"} {
		if changed, err := store.MarkDecryptFailed(context.Background(), id); err != nil || !changed {
			t.Fatalf("flag %s: %v %v", id, changed, err)
		}
	}
	if err := store.UpsertRecoveryJob(context.Background(), jobbed.String(), now, now.Add(24*time.Hour)); err != nil {
		t.Fatalf("job: %v", err)
	}
	if err := store.SetDecryptState(context.Background(), "0b7d81f2-9c48-4a6e-9d10-00000000e001", DecryptStateExpired); err != nil {
		t.Fatalf("expire: %v", err)
	}

	orphans, err := store.RecoveryOrphanPeers(context.Background(), self.String(), 10)
	if err != nil {
		t.Fatalf("orphans: %v", err)
	}
	if len(orphans) != 1 || orphans[0] != orphan.String() {
		t.Fatalf("orphans = %v, want exactly the jobless live peer", orphans)
	}
}

// TestRecoveryJobsOrder: least-recently-served first, never-served ahead
// of everything — the listing order is the scheduler's fairness policy.
func TestRecoveryJobsOrder(t *testing.T) {
	t.Parallel()
	store, _, _ := newRecoveryTestStore(t)
	now := time.Unix(1780000000, 0).UTC()

	for i, peer := range []string{"served-late", "served-early", "never-served"} {
		if err := store.UpsertRecoveryJob(context.Background(), peer, now.Add(time.Duration(i)*time.Second), now.Add(24*time.Hour)); err != nil {
			t.Fatalf("job %s: %v", peer, err)
		}
	}
	stamp := func(peer string, at time.Time) {
		if err := store.UpdateRecoveryJob(context.Background(), RecoveryJob{Peer: peer, State: DecryptStatePendingNotice, LastNoticeAt: at}); err != nil {
			t.Fatalf("stamp %s: %v", peer, err)
		}
	}
	stamp("served-late", now.Add(2*time.Hour))
	stamp("served-early", now.Add(1*time.Hour))

	jobs, err := store.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != 3 {
		t.Fatalf("jobs = %d err=%v", len(jobs), err)
	}
	want := []string{"never-served", "served-early", "served-late"}
	for i, job := range jobs {
		if job.Peer != want[i] {
			t.Fatalf("order[%d] = %s, want %s (full: %v)", i, job.Peer, want[i], jobs)
		}
	}
}

// TestDecryptFailedEntriesSkipExpired: expired rows are terminal — a later
// job of the same peer must not resurrect them into its workset.
func TestDecryptFailedEntriesSkipExpired(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	appendIncomingRow(t, store, self, peer, "0b7d81f2-9c48-4a6e-9d10-0000000000f1", "")
	if changed, err := store.MarkDecryptFailed(context.Background(), "0b7d81f2-9c48-4a6e-9d10-0000000000f1"); err != nil || !changed {
		t.Fatalf("flag: %v %v", changed, err)
	}
	if err := store.SetDecryptState(context.Background(), "0b7d81f2-9c48-4a6e-9d10-0000000000f1", DecryptStateExpired); err != nil {
		t.Fatalf("expire: %v", err)
	}
	entries, err := store.DecryptFailedEntries(context.Background(), peer.String(), self.String(), 10)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("an expired row re-entered the workset: %v", entries)
	}
}

// TestUndeliveredOutgoingExcludesSuperseded: a recovery-superseded sent
// row must not re-enter the ordinary retry path — its replacement is
// already in flight under a new id.
func TestUndeliveredOutgoingExcludesSuperseded(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	const rowID = "0b7d81f2-9c48-4a6e-9d10-0000000000d1"
	if err := store.Append(context.Background(), "dm", self, Entry{
		ID: rowID, Sender: self.String(), Recipient: peer.String(),
		Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	undelivered, err := store.UndeliveredOutgoing(context.Background(), self, time.Time{}, time.Now().UTC())
	if err != nil || len(undelivered) != 1 {
		t.Fatalf("baseline: %d err=%v, want the sent row", len(undelivered), err)
	}
	const replacementID = "0b7d81f2-9c48-4a6e-9d10-0000000000d2"
	if err := store.Append(context.Background(), "dm", self, Entry{
		ID: replacementID, Sender: self.String(), Recipient: peer.String(),
		Body: "sealed-again", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("append replacement: %v", err)
	}
	if err := store.MarkResendTerminal(context.Background(), rowID, replacementID, rowID); err != nil {
		t.Fatalf("terminal: %v", err)
	}
	undelivered, err = store.UndeliveredOutgoing(context.Background(), self, time.Time{}, time.Now().UTC())
	if err != nil {
		t.Fatalf("undelivered: %v", err)
	}
	// The replacement stays in the ordinary retry path; the superseded
	// original leaves it.
	if len(undelivered) != 1 || undelivered[0].ID != replacementID {
		t.Fatalf("undelivered = %v, want only the replacement", undelivered)
	}
}

// TestDecryptFailedEntriesExpiredDoesNotConsumeLimit: the expired filter
// runs in SQL, BEFORE the LIMIT — a pile of old terminal rows must not
// occupy the whole result and hide a newer live failure.
func TestDecryptFailedEntriesExpiredDoesNotConsumeLimit(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	const expiredID = "0b7d81f2-9c48-4a6e-9d10-0000000000e1"
	const liveID = "0b7d81f2-9c48-4a6e-9d10-0000000000e2"
	appendIncomingRow(t, store, self, peer, expiredID, "")
	appendIncomingRow(t, store, self, peer, liveID, "")
	for _, id := range []string{expiredID, liveID} {
		if changed, err := store.MarkDecryptFailed(context.Background(), id); err != nil || !changed {
			t.Fatalf("flag %s: %v %v", id, changed, err)
		}
	}
	if err := store.SetDecryptState(context.Background(), expiredID, DecryptStateExpired); err != nil {
		t.Fatalf("expire: %v", err)
	}

	entries, err := store.DecryptFailedEntries(context.Background(), peer.String(), self.String(), 1)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != liveID {
		t.Fatalf("limit=1 returned %v — the expired row consumed the slot", entries)
	}

	// The same guarantee for the orphan feed: one old expired row must not
	// hide the live orphan behind the limit.
	orphans, err := store.RecoveryOrphanPeers(context.Background(), self.String(), 1)
	if err != nil || len(orphans) != 1 || orphans[0] != peer.String() {
		t.Fatalf("orphans = %v err=%v, want the live peer", orphans, err)
	}
}

// TestUndeliveredOutgoingNullSupersededKept: the exclusion reads the JSON
// field itself — a null value or the key nested inside another value is
// NOT a superseded row and must stay in the retry path.
func TestUndeliveredOutgoingNullSupersededKept(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	rows := map[string]string{
		"0b7d81f2-9c48-4a6e-9d10-0000000000c1": `{"superseded_by":null}`,
		"0b7d81f2-9c48-4a6e-9d10-0000000000c2": `{"note":"{\"superseded_by\":\"x\"}"}`,
	}
	for id, metadata := range rows {
		if err := store.Append(context.Background(), "dm", self, Entry{
			ID: id, Sender: self.String(), Recipient: peer.String(),
			Body: "sealed", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Metadata: metadata,
		}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	undelivered, err := store.UndeliveredOutgoing(context.Background(), self, time.Time{}, time.Now().UTC())
	if err != nil {
		t.Fatalf("undelivered: %v", err)
	}
	if len(undelivered) != len(rows) {
		t.Fatalf("got %d rows, want %d — a non-superseded row was dropped by the exclusion", len(undelivered), len(rows))
	}
}

// TestAdmitRecoveryJobIfRoomNeverEvicts: the reconciliation admission
// waits for an organic slot — over a full backlog it must refuse, not
// rotate existing jobs out.
func TestAdmitRecoveryJobIfRoomNeverEvicts(t *testing.T) {
	t.Parallel()
	store, _, _ := newRecoveryTestStore(t)
	now := time.Unix(1780000000, 0).UTC()
	const limit = 3

	for i := 0; i < limit; i++ {
		peer := fmt.Sprintf("peer-%02d", i)
		if err := store.MarkEstablished(context.Background(), peer, EstablishedReasonOutgoing, now); err != nil {
			t.Fatalf("mark established: %v", err)
		}
		if _, _, err := store.AdmitRecoveryJob(context.Background(), peer, now, now.Add(24*time.Hour), 3, limit, RecoveryProtectedWork{}); err != nil {
			t.Fatalf("seed admit: %v", err)
		}
	}
	admitted, err := store.AdmitRecoveryJobIfRoom(context.Background(), "orphan-peer", now.Add(time.Hour), now.Add(25*time.Hour), 3, limit)
	if err != nil {
		t.Fatalf("if-room admit: %v", err)
	}
	if admitted {
		t.Fatal("the no-evict admission displaced an existing job")
	}
	jobs, err := store.RecoveryJobs(context.Background())
	if err != nil || len(jobs) != limit {
		t.Fatalf("jobs = %d err=%v, want the untouched cap", len(jobs), err)
	}

	// A freed slot lets the orphan in.
	if err := store.DeleteRecoveryJob(context.Background(), "peer-00"); err != nil {
		t.Fatalf("free a slot: %v", err)
	}
	admitted, err = store.AdmitRecoveryJobIfRoom(context.Background(), "orphan-peer", now.Add(2*time.Hour), now.Add(26*time.Hour), 3, limit)
	if err != nil || !admitted {
		t.Fatalf("free-slot admission refused: %v %v", admitted, err)
	}
}

// TestExpireDecryptFailedReachesEveryRow: the hard deadline is one UPDATE
// over ALL live rows — a workset-sized prefix would leave live rows that
// re-enter through the orphan sweep with a fresh lifetime.
func TestExpireDecryptFailedReachesEveryRow(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	const rowCount = 70 // deliberately past the 64-row workset limit
	for i := 0; i < rowCount; i++ {
		id := fmt.Sprintf("0b7d81f2-9c48-4a6e-9d10-0000000070%02x", i)
		appendIncomingRow(t, store, self, peer, id, "")
		if changed, err := store.MarkDecryptFailed(context.Background(), id); err != nil || !changed {
			t.Fatalf("flag %s: %v %v", id, changed, err)
		}
	}
	if _, found, err := store.OldestDecryptFlaggedAt(context.Background(), peer.String(), self.String()); err != nil || !found {
		t.Fatalf("flagged-at anchor missing: found=%v err=%v", found, err)
	}

	if err := store.ExpireDecryptFailed(context.Background(), peer.String(), self.String()); err != nil {
		t.Fatalf("expire: %v", err)
	}
	live, err := store.DecryptFailedEntries(context.Background(), peer.String(), self.String(), rowCount)
	if err != nil || len(live) != 0 {
		t.Fatalf("live rows after expiry = %d err=%v — the deadline missed the tail", len(live), err)
	}
	orphans, err := store.RecoveryOrphanPeers(context.Background(), self.String(), 10)
	if err != nil || len(orphans) != 0 {
		t.Fatalf("expired rows re-entered the orphan feed: %v err=%v", orphans, err)
	}
	if _, found, err := store.OldestDecryptFlaggedAt(context.Background(), peer.String(), self.String()); err != nil || found {
		t.Fatalf("expired rows still anchor a deadline: found=%v err=%v", found, err)
	}
}

// TestMarkDecryptFailedNeverResurrectsSuperseded: the flag write is one
// conditional UPDATE — a late report racing the recovery must not flip a
// superseded row back into the live workset.
func TestMarkDecryptFailedNeverResurrectsSuperseded(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	const rowID = "0b7d81f2-9c48-4a6e-9d10-0000000000a1"
	appendIncomingRow(t, store, self, peer, rowID, "")
	if changed, err := store.MarkDecryptFailed(context.Background(), rowID); err != nil || !changed {
		t.Fatalf("first flag: %v %v", changed, err)
	}
	if applied, err := store.MarkSupersededCollapsing(context.Background(), rowID, "0b7d81f2-9c48-4a6e-9d10-0000000000a2", rowID); err != nil || !applied {
		t.Fatalf("supersede: %v", err)
	}

	changed, err := store.MarkDecryptFailed(context.Background(), rowID)
	if err != nil {
		t.Fatalf("late flag: %v", err)
	}
	if changed {
		t.Fatal("a late report resurrected a superseded row")
	}
	marks, _, err := store.EntryRecoveryMarks(context.Background(), rowID)
	if err != nil || marks.DecryptFailed || marks.DecryptState != DecryptStateRecovered {
		t.Fatalf("marks after late report = %+v err=%v, want recovered untouched", marks, err)
	}
}

// TestRecoveryCycleAnchorImmutable: the cycle anchor is set once and
// survives later candidates; only an idle close resets it, and the close
// refuses transactionally while a live flagged row exists.
func TestRecoveryCycleAnchorImmutable(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	first := time.Unix(1780000000, 0).UTC()
	later := first.Add(48 * time.Hour)

	anchor, err := store.EnsureRecoveryCycle(context.Background(), peer.String(), first)
	if err != nil || !anchor.Equal(first) {
		t.Fatalf("first anchor = %v err=%v", anchor, err)
	}
	anchor, err = store.EnsureRecoveryCycle(context.Background(), peer.String(), later)
	if err != nil || !anchor.Equal(first) {
		t.Fatalf("anchor moved to %v err=%v — the cycle clock restarted", anchor, err)
	}

	// A live flagged row blocks the close — the check and the delete are
	// one transaction, so a racing fresh failure can never lose the anchor.
	const rowID = "0b7d81f2-9c48-4a6e-9d10-0000000000b1"
	appendIncomingRow(t, store, self, peer, rowID, "")
	if changed, err := store.MarkDecryptFailed(context.Background(), rowID); err != nil || !changed {
		t.Fatalf("flag: %v %v", changed, err)
	}
	closed, err := store.CloseRecoveryCycleIfIdle(context.Background(), peer.String(), self.String())
	if err != nil || closed {
		t.Fatalf("close with live work: closed=%v err=%v", closed, err)
	}
	if anchor, err = store.EnsureRecoveryCycle(context.Background(), peer.String(), later); err != nil || !anchor.Equal(first) {
		t.Fatalf("anchor lost under live work: %v err=%v", anchor, err)
	}

	// Recovered → the idle close fires and a NEW cycle re-arms.
	if applied, err := store.MarkSupersededCollapsing(context.Background(), rowID, "0b7d81f2-9c48-4a6e-9d10-0000000000b2", rowID); err != nil || !applied {
		t.Fatalf("supersede: %v", err)
	}
	closed, err = store.CloseRecoveryCycleIfIdle(context.Background(), peer.String(), self.String())
	if err != nil || !closed {
		t.Fatalf("idle close refused: closed=%v err=%v", closed, err)
	}
	anchor, err = store.EnsureRecoveryCycle(context.Background(), peer.String(), later)
	if err != nil || !anchor.Equal(later) {
		t.Fatalf("post-close anchor = %v err=%v, want a fresh cycle", anchor, err)
	}
}

// TestResendIntentDurability: intents survive a store reopen — the crash
// insurance is worthless if the crash also loses the intent.
func TestResendIntentDurability(t *testing.T) {
	t.Parallel()
	self := domaintest.ID("self")
	now := time.Unix(1780000000, 0).UTC()

	storePath := filepath.Join(t.TempDir(), "state.db")
	store := newTestStoreAt(t, storePath, self)
	if _, admitted, _, err := store.AdmitResendIntent(context.Background(), ResendIntent{
		Root: "root-1", OriginalID: "orig-1", Peer: "peer-1",
		ReplacementID: "repl-1", CreatedAt: now,
	}, 3, 200, RecoveryProtectedWork{}); err != nil || !admitted {
		t.Fatalf("admit: %v %v", admitted, err)
	}
	// A repeat admission returns the CANONICAL intent — the stored
	// replacement id survives, a divergent fresh one is discarded.
	canonical, admitted, _, err := store.AdmitResendIntent(context.Background(), ResendIntent{
		Root: "root-1", OriginalID: "orig-1", Peer: "peer-1",
		ReplacementID: "repl-DIVERGENT", CreatedAt: now.Add(time.Hour),
	}, 3, 200, RecoveryProtectedWork{})
	if err != nil || !admitted || canonical.ReplacementID != "repl-1" {
		t.Fatalf("canonical re-admission = %+v admitted=%v err=%v", canonical, admitted, err)
	}
	reopened := newTestStoreAt(t, storePath, self)
	intents, err := reopened.ResendIntents(context.Background(), 10)
	if err != nil || len(intents) != 1 || intents[0].Root != "root-1" ||
		intents[0].OriginalID != "orig-1" || intents[0].Peer != "peer-1" ||
		intents[0].ReplacementID != "repl-1" {
		t.Fatalf("intents after reopen = %+v err=%v", intents, err)
	}
	if err := reopened.DeleteResendIntent(context.Background(), "root-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if intents, _ := reopened.ResendIntents(context.Background(), 10); len(intents) != 0 {
		t.Fatalf("intent survived deletion: %v", intents)
	}
}

// TestMarkDecryptFailedReplacesNonObjectMetadata: a valid JSON NON-OBJECT
// blob (array/string/number) must be replaced, not silently kept —
// json_set on a non-object returns it unchanged while still counting the
// row as affected, which would report a flag that never landed.
func TestMarkDecryptFailedReplacesNonObjectMetadata(t *testing.T) {
	t.Parallel()
	store, self, peer := newRecoveryTestStore(t)
	blobs := map[string]string{
		"0b7d81f2-9c48-4a6e-9d10-0000000000f1": `[1,2]`,
		"0b7d81f2-9c48-4a6e-9d10-0000000000f2": `"just a string"`,
	}
	for id, metadata := range blobs {
		appendIncomingRow(t, store, self, peer, id, metadata)
		changed, err := store.MarkDecryptFailed(context.Background(), id)
		if err != nil || !changed {
			t.Fatalf("flag %s: %v %v", id, changed, err)
		}
		marks, _, err := store.EntryRecoveryMarks(context.Background(), id)
		if err != nil || !marks.DecryptFailed || marks.DecryptState != DecryptStatePendingNotice || marks.DecryptFlaggedAt == "" {
			t.Fatalf("flag reported written but did not land on %s: %+v err=%v", id, marks, err)
		}
	}
	live, err := store.DecryptFailedEntries(context.Background(), peer.String(), self.String(), 10)
	if err != nil || len(live) != len(blobs) {
		t.Fatalf("live rows = %d err=%v, want %d", len(live), err, len(blobs))
	}
}
