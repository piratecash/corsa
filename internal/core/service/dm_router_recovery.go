package service

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// dm_router_recovery.go is the §4.10 decrypt-recovery subsystem
// (docs/protocol/identity-lookup.md): a rotated box key breaks a contact
// SILENTLY — the sender keeps encrypting to a dead key, the receiver sees
// nothing readable, and `delivered` fires before decryption. The recovery
// is a three-legged exchange:
//
//   - the RECEIVER flags the unreadable row (metadata, chatlog), opens a
//     durable per-peer job, refreshes the sender's keys through the lookup
//     and sends a decrypt_failed notice encrypted to provably-fresh keys;
//   - the SENDER validates the notice against ITS OWN chatlog row (the
//     re-send recipient is never taken from the notice), refreshes the
//     receiver's keys and re-sends the plaintext of its own sealed copy as
//     a NEW message carrying retry_of;
//   - the RECEIVER accepts the retry_of only for a row that really is
//     flagged, from the same author, not yet superseded — then supersedes
//     the original, collapses its unread count and closes the job when no
//     flagged rows remain.

const (
	// recoveryJobLifetime bounds one peer's job (§4.10: 7 days).
	recoveryJobLifetime = 7 * 24 * time.Hour

	// recoveryWaitingRetry is the park interval after the notice ladder is
	// exhausted; parking frees the active slot (§4.10 quotas).
	recoveryWaitingRetry = 24 * time.Hour

	// recoveryMaxActiveGlobal / recoveryEstablishedShare are the §4.10
	// slot quotas: at most 20 peers actively worked at once, at least half
	// the pool reserved for established contacts so Sybil identities can
	// never starve a real one.
	recoveryMaxActiveGlobal  = 20
	recoveryEstablishedShare = 2 // reserve = pool / share

	// recoveryWorksetLimit bounds the flagged-row workset read per job
	// pass; accounting is unbounded via the metadata flags, only the
	// in-memory work is capped.
	recoveryWorksetLimit = 64

	// recoveryRetryChainLimit bounds re-send chains per retry_root_id: a
	// new message id on every hop must not reset the budget.
	recoveryRetryChainLimit = 3

	// recoveryTick is the scheduler cadence; wakes also arrive on reports.
	recoveryTick = 30 * time.Second

	// recoveryBacklogLimit bounds the durable job table (§4.10: global
	// backlog ≤ 200, unknown peers evicted first by LRU, established rows
	// never evicted for an unknown newcomer).
	recoveryBacklogLimit = 200

	// recoveryReconcileLimit bounds the per-pass orphan sweep: flagged rows
	// whose job was refused or evicted re-attempt admission this many peers
	// at a time.
	recoveryReconcileLimit = 20

	// recoveryMaxResendsPerPeer is the §4.10 per-peer bound applied to the
	// SENDER leg on both tiers: at most 3 queued re-sends per peer active
	// at once AND at most 3 durable backlog intents per peer. The global
	// ACTIVE budget is shared with the receiver side (recoveryMaxActiveGlobal
	// per pass, receiver batch first); the global BACKLOG bound is
	// recoveryBacklogLimit with the same established reservation.
	recoveryMaxResendsPerPeer = 3
)

// recoveryNoticeDelays is the notice retry ladder — the delivery-receipt
// schedule of §4.10 (30s → … → 11m, last value is the ceiling).
var recoveryNoticeDelays = []time.Duration{
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	11 * time.Minute,
}

// recoveryManager runs the receiver and sender legs. Its own mutex guards
// only in-memory scheduling state; every durable fact lives in the chatlog
// (jobs, row marks, established facts).
type recoveryManager struct {
	router *DMRouter
	clock  func() time.Time
	wake   chan struct{}

	// admissionMu serializes every backlog admission and every activation
	// decision — the quota arithmetic of BOTH legs. The counters live in
	// two places (the SQLite backlog and the in-memory active queue), so a
	// snapshot-based check would race a concurrent admission: two paths
	// could each observe a free slot and both take it. One mutex, one
	// decision at a time; it is never held across a network send and no
	// domain mutex is anywhere near it.
	admissionMu sync.Mutex

	mu sync.Mutex
	// pendingResends is the sender leg's ACTIVE queue: retry_root → the
	// re-send waiting for keys or mid-send. Entries here hold active-pool
	// slots; a completed send with an unpaid terminal moves to
	// terminalDebts instead.
	pendingResends map[string]recoveryResend
	// terminalDebts holds re-sends whose replacement already left but
	// whose terminal transaction has not committed yet: pure local DB
	// retries, NO network work — they hold no active-pool slot (a peer
	// whose terminal cannot commit must not shrink the global 20 forever),
	// while their durable intents stay protected from eviction.
	terminalDebts map[string]recoveryResend
	// activeNotices marks peers whose receiver-leg notice attempt is
	// running right now: their jobs are eviction-protected for the span of
	// the attempt, or a concurrent admission could delete the job under
	// the attempt's feet.
	activeNotices map[string]struct{}
	// establishedMarked caches peers whose decrypted-incoming established
	// fact already reached the store this process lifetime: the success
	// hook fires for EVERY decrypted incoming message, history loads
	// included, and the fact is monotonic — one durable write per peer is
	// enough. An entry is removed on write failure so the next message
	// retries.
	establishedMarked map[string]struct{}
	// proofs binds each recovery ACTION's gate to the lookup it requested:
	// keyed per consumer (the peer's receiver-leg job, or one sender-leg
	// retry root), created with the resolution id and the resolver's
	// attempt-generation watermark the arm returned, granted when THAT
	// resolution completes authoritative off an attempt NEWER than the
	// watermark, and consumed by the one action that asked. Per-consumer
	// keys matter under bilateral rotation: the receiver job and a later
	// sender notice for the same peer each anchor their own watermark, so
	// neither can ride a proof of the other's older question.
	proofs map[recoveryProofKey]recoveryProofRequest
}

// recoveryProofScopeJob is the receiver-leg scope (one job per peer);
// sender-leg scopes are the retry root ids.
const recoveryProofScopeJob = "job"

// recoveryProofKey identifies one proof consumer.
type recoveryProofKey struct {
	peer  string
	scope string
}

// recoveryProofRequest is one consumer's in-flight proof demand.
type recoveryProofRequest struct {
	resolutionID string
	// watermark is the resolver's attempt generation at arm time: only an
	// answer to an attempt with a STRICTLY greater generation proves a
	// question asked after this consumer's failure. A monotonic counter,
	// not wall clock — clock steps must never re-validate old questions.
	watermark uint64
	granted   bool
}

// recoveryResend is one queued sender-side re-send.
type recoveryResend struct {
	originalID string
	peer       string
	// replacementID is the pre-minted id from the durable intent that
	// admitted this resend: every send retry reuses it, so the id the
	// intent names and the id on the wire can never diverge.
	replacementID string
	// sentReplacementID is set the moment the replacement is accepted for
	// delivery: from then on the entry owes the chatlog its TERMINAL (the
	// supersede + chain transaction), never a second send.
	sentReplacementID string
	// busy is the per-root CLAIM: exactly one tryResend invocation may
	// work a root at a time, and while the claim is held no release —
	// expiry, eviction cleanup — may pull the entry or its durable intent
	// out from under the send; a release arriving mid-claim is deferred
	// (pendingRelease) and executed when the claim drops.
	busy           bool
	pendingRelease bool
	// awaitingTurn marks an entry the scheduler selected but has not yet
	// reached in its ordered execution: event-driven retries skip it, or a
	// resolution event landing mid-pass could run a later (even unknown)
	// item ahead of earlier established ones and break the computed
	// service order. The executor clears it at the entry's position;
	// leftovers from an aborted pass are cleared at the next selection.
	awaitingTurn bool
}

func newRecoveryManager(router *DMRouter) *recoveryManager {
	return &recoveryManager{
		router:            router,
		clock:             func() time.Time { return time.Now().UTC() },
		wake:              make(chan struct{}, 1),
		pendingResends:    map[string]recoveryResend{},
		terminalDebts:     map[string]recoveryResend{},
		activeNotices:     map[string]struct{}{},
		proofs:            map[recoveryProofKey]recoveryProofRequest{},
		establishedMarked: map[string]struct{}{},
	}
}

// claimResend takes the per-root work claim: exactly one tryResend at a
// time, and no release can strip the root while the claim is held.
func (m *recoveryManager) claimResend(root string) (recoveryResend, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resend, ok := m.pendingResends[root]
	if !ok || resend.busy {
		return recoveryResend{}, false
	}
	resend.busy = true
	m.pendingResends[root] = resend
	return resend, true
}

// unclaimResend drops the claim and executes a release that arrived while
// the claim was held.
func (m *recoveryManager) unclaimResend(store *chatlog.Store, root string) {
	m.mu.Lock()
	resend, ok := m.pendingResends[root]
	if ok {
		resend.busy = false
		m.pendingResends[root] = resend
	}
	deferred := ok && resend.pendingRelease
	m.mu.Unlock()
	if deferred {
		m.releaseResend(store, root)
	}
}

// noteDecryptedIncoming is the §4.10 receiver-side qualifying hook, wired
// into EVERY successful decrypt of an incoming DM (live event, history
// load, preview): the established fact and the retry_of acceptance must
// not depend on which chat the UI has open — a replacement landing in a
// background conversation, or read after a restart, closes recovery all
// the same.
func (m *recoveryManager) noteDecryptedIncoming(msg *DirectMessage) {
	if msg == nil {
		return
	}
	m.markEstablishedOnce(msg.Sender.String(), chatlog.EstablishedReasonDecrypted)
	if msg.RetryOf != "" {
		m.acceptRetryOf(msg)
	}
}

// noteOutgoingSent is the sender-side qualifying hook, wired into the
// SendDirectMessage chokepoint: a user-authored outgoing message
// establishes the peer whichever surface it was sent from — the composer,
// the RPC or the file-transfer bridge.
func (m *recoveryManager) noteOutgoingSent(peer string) {
	m.markEstablishedOnce(peer, chatlog.EstablishedReasonOutgoing)
}

// markEstablishedOnce writes the monotonic established fact once per peer
// per process lifetime (the fact itself never changes, so one durable
// write is enough); a failed write drops the cache entry so the next
// qualifying event retries.
func (m *recoveryManager) markEstablishedOnce(peer, reason string) {
	store := m.store()
	if store == nil || peer == "" {
		return
	}
	m.mu.Lock()
	_, alreadyMarked := m.establishedMarked[peer]
	if !alreadyMarked {
		m.establishedMarked[peer] = struct{}{}
	}
	m.mu.Unlock()
	if alreadyMarked {
		return
	}
	if err := store.MarkEstablished(peer, reason, m.clock()); err != nil {
		log.Warn().Err(err).Str("peer", peer).Msg("established_mark_failed")
		m.mu.Lock()
		delete(m.establishedMarked, peer)
		m.mu.Unlock()
	}
}

// noteResolution feeds the resolution events into both legs. A completion
// counts for one consumer only when ALL of: authoritative-successful, the
// very resolution that consumer requested (matched by id), AND its proven
// answer belongs to an attempt with a generation past that consumer's own
// watermark. Every matching consumer is granted — the receiver job and a
// sender root that armed on the same resolution each get their own grant,
// but one that armed LATER (a higher watermark, bilateral rotation) stays
// gated until an attempt newer than ITS question answers.
func (m *recoveryManager) noteResolution(state ebus.IdentityResolutionState) {
	if state.Authority != domain.IdentityAuthorityAuthoritative ||
		state.Lifecycle != domain.IdentityResolutionSucceeded || state.AnswerAttemptGen == 0 {
		return
	}
	peer := state.Target.String()
	grantedAny := false
	m.mu.Lock()
	for key, request := range m.proofs {
		if key.peer != peer || request.resolutionID != state.ResolutionID ||
			state.AnswerAttemptGen <= request.watermark {
			continue
		}
		request.granted = true
		m.proofs[key] = request
		grantedAny = true
	}
	m.mu.Unlock()
	if !grantedAny {
		return
	}
	m.nudge()
	m.retryPendingResends(peer)
}

// requestRecoveryProof opens (or joins) the proof-bearing lookup for the
// consumer and arms its gate with the resolution id and the resolver's
// attempt-generation watermark from the SAME reply: attempts already in
// flight at arm time have generations ≤ the watermark and can never
// satisfy this consumer. An entry already armed on the same resolution is
// kept (its watermark is the older, stricter-history one); a NEW
// resolution id replaces it — the old resolution is terminal history.
func (m *recoveryManager) requestRecoveryProof(peer, scope, messageID string) {
	frame := m.router.resolveIdentityForRecovery(peer, messageID)
	if frame.ResolutionID == "" {
		return
	}
	key := recoveryProofKey{peer: peer, scope: scope}
	m.mu.Lock()
	if current, ok := m.proofs[key]; !ok || current.resolutionID != frame.ResolutionID {
		m.proofs[key] = recoveryProofRequest{
			resolutionID: frame.ResolutionID,
			watermark:    frame.AttemptGenWatermark,
		}
	}
	m.mu.Unlock()
}

// consumeRecoveryProof takes the consumer's granted proof, one action per
// grant: the next attempt re-asks the network instead of reusing a result
// that may predate yet another rotation.
func (m *recoveryManager) consumeRecoveryProof(peer, scope string) bool {
	key := recoveryProofKey{peer: peer, scope: scope}
	m.mu.Lock()
	defer m.mu.Unlock()
	request, ok := m.proofs[key]
	if !ok || !request.granted {
		return false
	}
	delete(m.proofs, key)
	return true
}

// hasGrantedProof reports an unconsumed receiver-leg grant — the scheduler
// treats such a job as due regardless of the notice ladder, so the notice
// leaves the moment the proof lands instead of waiting out a backoff step.
func (m *recoveryManager) hasGrantedProof(peer string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	request, ok := m.proofs[recoveryProofKey{peer: peer, scope: recoveryProofScopeJob}]
	return ok && request.granted
}

func (m *recoveryManager) store() *chatlog.Store {
	if m.router == nil || m.router.client == nil {
		return nil
	}
	return m.router.client.chatLog
}

// Report is the §4.10 single entry point, wired into every DMCrypto
// decrypt path. Idempotent by construction: the row flag suppresses
// repeated job starts however often the UI re-renders the same row.
func (m *recoveryManager) Report(failure DecryptFailure) {
	switch failure.Class {
	case DecryptFailureMissingSenderKey:
		// Keys never seen: a lookup fixes it locally, no notice — the row
		// re-decrypts on the next read once keys arrive.
		m.router.resolveIdentityForRecovery(failure.Sender, failure.MessageID)
	case DecryptFailureSealedUnreadable:
		store := m.store()
		if store == nil {
			return
		}
		changed, err := store.MarkDecryptFailed(failure.MessageID)
		if err != nil {
			log.Warn().Err(err).Str("message_id", failure.MessageID).Msg("decrypt_recovery_flag_failed")
			return
		}
		if !changed {
			return // already flagged — the idempotency suppressor
		}
		now := m.clock()
		m.admissionMu.Lock()
		admitted, victim, err := store.AdmitRecoveryJob(failure.Sender, now, m.jobDeadline(failure.Sender, now),
			recoveryMaxResendsPerPeer, recoveryBacklogLimit, m.protectedWork())
		m.admissionMu.Unlock()
		if err != nil {
			log.Warn().Err(err).Str("peer", failure.Sender).Msg("decrypt_recovery_job_open_failed")
			return
		}
		m.releaseEvictionVictim(store, victim)
		if !admitted {
			// Backlog full of established jobs: the row keeps its flag and
			// the orphan reconciliation in pass() re-attempts admission once
			// slots free up.
			log.Warn().Str("peer", failure.Sender).Msg("decrypt_recovery_backlog_full")
			return
		}
		log.Info().Str("peer", failure.Sender).Str("message_id", failure.MessageID).Msg("decrypt_recovery_job_opened")
		m.nudge()
	}
}

// releaseJobResolution closes the resolver work of a peer whose job left
// the table (closed, expired or evicted): the recovery-typed lookup
// reasons are cancelled and the armed proof dropped, so a jobless peer
// stops consuming background attempts. Both are skipped while the SENDER
// leg still waits on the same peer — its resend owns those resources.
// Row flags are untouched on eviction: the orphan reconciliation re-admits
// them when a slot frees.
func (m *recoveryManager) releaseJobResolution(peer string) {
	m.mu.Lock()
	senderLegBusy := false
	for _, resend := range m.pendingResends {
		if resend.peer == peer {
			senderLegBusy = true
			break
		}
	}
	// The job's own gate always goes with the job; sender-leg scopes stay
	// with their queued resends.
	delete(m.proofs, recoveryProofKey{peer: peer, scope: recoveryProofScopeJob})
	m.mu.Unlock()
	if senderLegBusy {
		return
	}
	m.router.cancelRecoveryResolution(peer)
}

func (m *recoveryManager) nudge() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// run is the scheduler loop: one goroutine under the router's lifecycle.
func (m *recoveryManager) run(ctx context.Context) {
	ticker := time.NewTicker(recoveryTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
		case <-ticker.C:
		}
		m.pass(ctx)
	}
}

// pass runs one scheduling sweep over the durable jobs, applying the slot
// quotas: established jobs are admitted first, unknown peers may take at
// most half the pool. The jobs arrive least-recently-served first and
// every selected job advances its served stamp — proof-waiting included —
// so a stuck head can never freeze the tail of the queue.
func (m *recoveryManager) pass(ctx context.Context) {
	store := m.store()
	if store == nil {
		return
	}
	m.reconcileResendIntents(ctx, store)
	m.reconcileOrphans(ctx, store)
	m.sweepStaleCycles(ctx, store)
	jobs, err := store.RecoveryJobs(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("decrypt_recovery_jobs_read_failed")
		return
	}
	m.sweepProofs(jobs)
	// The event bus is lossy by contract (a full inbox drops the event)
	// and a terminal can land in the window between the arm RPC and the
	// proofs-map write — the status poll is the insurance that turns both
	// losses into a one-tick delay instead of a permanent stall.
	m.pollProofRequests()
	// The sender leg has no durable job to schedule it: without this sweep
	// a queued re-send whose grant event was lost would sit in
	// pendingResends forever.
	m.retryStalledResends()
	now := m.clock()

	due := make([]chatlog.RecoveryJob, 0, len(jobs))
	for _, job := range jobs {
		switch {
		case now.After(job.ExpiresAt):
			m.expireJob(ctx, store, job)
		case job.State == chatlog.DecryptStateWaitingRetry:
			if now.After(job.WaitUntil) {
				// The 24-hour park ended: a fresh proof-bearing lookup and a
				// new notice cycle.
				job.State = chatlog.DecryptStatePendingNotice
				job.NoticeAttempts = 0
				job.WaitUntil = time.Time{}
				if err := store.UpdateRecoveryJob(job); err != nil {
					log.Warn().Err(err).Str("peer", job.Peer).Msg("decrypt_recovery_job_update_failed")
					continue
				}
				due = append(due, job)
			}
		case job.State == chatlog.DecryptStatePendingNotice && m.noticeDue(job, now):
			due = append(due, job)
		}
	}
	// One ACTIVE pool, one arithmetic, one ORDER, one holder: candidates
	// of BOTH legs merge into a single established-first, LRU-interleaved
	// queue, the slot/unknown/per-peer budgets are spent by that queue
	// alone, and the selected actions EXECUTE in the same order they were
	// selected — splitting execution back into per-leg loops would let
	// the actual service order diverge from the computed one. The
	// selection plus the activation map-writes run under admissionMu so
	// no concurrent admission can double-spend a slot; sends happen after
	// the mutex is released, with the picked jobs' peers
	// eviction-protected for the span of their attempts.
	for _, item := range m.selectRecoveryWork(ctx, store, due) {
		if item.job != nil {
			m.attemptNotice(ctx, store, *item.job)
			m.mu.Lock()
			delete(m.activeNotices, item.peer)
			m.mu.Unlock()
			continue
		}
		root := item.intent.Root
		m.mu.Lock()
		resend, owned := m.pendingResends[root]
		if owned {
			// The entry's turn has come: from here on the resolution
			// events may drive it too.
			resend.awaitingTurn = false
			m.pendingResends[root] = resend
		}
		m.mu.Unlock()
		if !owned {
			continue
		}
		m.requestRecoveryProof(resend.peer, root, resend.originalID)
		m.tryResend(root)
	}
}

// selectRecoveryWork picks this pass's work across both legs under
// admissionMu and registers the picked jobs' peers in activeNotices (the
// caller clears each after its attempt). Returns the receiver batch and
// the activated resend roots.
func (m *recoveryManager) selectRecoveryWork(ctx context.Context, store *chatlog.Store, due []chatlog.RecoveryJob) []recoveryWorkItem {
	isEstablished := func(peer string) bool {
		established, err := store.IsEstablished(peer)
		if err != nil {
			log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_established_read_failed")
		}
		return established
	}
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()

	intents, err := store.ResendIntents(ctx, recoveryBacklogLimit)
	if err != nil {
		log.Warn().Err(err).Msg("decrypt_recovery_intents_read_failed")
		intents = nil
	}

	m.mu.Lock()
	perPeer := map[string]int{}
	for root, queued := range m.pendingResends {
		perPeer[queued.peer]++
		// A leftover turn marker from an aborted pass would keep the entry
		// invisible to event-driven retries forever; one pass of deferral
		// is the whole point, so clear stale markers here.
		if queued.awaitingTurn {
			queued.awaitingTurn = false
			m.pendingResends[root] = queued
		}
	}
	senderActive := len(m.pendingResends)
	waiting := make([]chatlog.ResendIntent, 0, len(intents))
	for _, intent := range intents {
		_, active := m.pendingResends[intent.Root]
		_, indebted := m.terminalDebts[intent.Root]
		if !active && !indebted {
			waiting = append(waiting, intent)
		}
	}
	m.mu.Unlock()

	slots := recoveryMaxActiveGlobal - senderActive
	unknownBudget := recoveryMaxActiveGlobal / recoveryEstablishedShare
	for peer, count := range perPeer {
		if !isEstablished(peer) {
			unknownBudget -= count
		}
	}

	var selected []recoveryWorkItem
	m.mu.Lock()
	for _, item := range orderRecoveryWork(due, waiting, isEstablished) {
		if slots <= 0 {
			break
		}
		if perPeer[item.peer] >= recoveryMaxResendsPerPeer {
			continue
		}
		if !isEstablished(item.peer) {
			if unknownBudget <= 0 {
				continue
			}
			unknownBudget--
		}
		slots--
		perPeer[item.peer]++
		if item.intent == nil {
			m.activeNotices[item.peer] = struct{}{}
		} else {
			m.pendingResends[item.intent.Root] = recoveryResend{
				originalID:    item.intent.OriginalID,
				peer:          item.intent.Peer,
				replacementID: item.intent.ReplacementID,
				awaitingTurn:  true,
			}
		}
		selected = append(selected, item)
	}
	m.mu.Unlock()
	return selected
}

// recoveryWorkItem is one candidate of either leg in the unified queue.
type recoveryWorkItem struct {
	peer string
	// stamp orders candidates WITHIN a priority class by
	// least-recently-served: a job's last notice time (zero = never
	// served, first in line), an intent's admission time. One clock for
	// both legs — a constant stream of due jobs on one leg cannot starve
	// older work on the other, because a served job's stamp advances and
	// rotates it behind the waiting intents.
	stamp  time.Time
	job    *chatlog.RecoveryJob
	intent *chatlog.ResendIntent
}

// orderRecoveryWork merges both legs' candidates into the §4.10 service
// order: EVERY established candidate ahead of ANY unknown one, and WITHIN
// each class both legs interleaved least-recently-served first. Unknown
// work of one leg must never outrank established work of the other, and
// neither leg may starve the other inside its class.
func orderRecoveryWork(due []chatlog.RecoveryJob, waiting []chatlog.ResendIntent, isEstablished func(string) bool) []recoveryWorkItem {
	established := make([]recoveryWorkItem, 0, len(due)+len(waiting))
	unknown := make([]recoveryWorkItem, 0, len(due)+len(waiting))
	add := func(item recoveryWorkItem) {
		if isEstablished(item.peer) {
			established = append(established, item)
		} else {
			unknown = append(unknown, item)
		}
	}
	for i := range due {
		// A never-served job carries a ZERO notice time, which would
		// outrank every real intent timestamp unconditionally — a stream
		// of fresh jobs would then starve arbitrarily old re-sends. Its
		// comparable stamp is its admission time, the same clock the
		// intents use.
		stamp := due[i].LastNoticeAt
		if stamp.IsZero() {
			stamp = due[i].CreatedAt
		}
		add(recoveryWorkItem{peer: due[i].Peer, stamp: stamp, job: &due[i]})
	}
	for i := range waiting {
		add(recoveryWorkItem{peer: waiting[i].Peer, stamp: waiting[i].CreatedAt, intent: &waiting[i]})
	}
	sort.SliceStable(established, func(i, j int) bool { return established[i].stamp.Before(established[j].stamp) })
	sort.SliceStable(unknown, func(i, j int) bool { return unknown[i].stamp.Before(unknown[j].stamp) })
	return append(established, unknown...)
}

// reconcileOrphans re-attempts admission for peers whose flagged rows lost
// (or never got) their job — a refused admission or an eviction leaves the
// flags in place, and the row flag suppresses repeat Reports, so this
// sweep is the ONLY path back into the backlog.
func (m *recoveryManager) reconcileOrphans(ctx context.Context, store *chatlog.Store) {
	orphans, err := store.RecoveryOrphanPeers(ctx, m.router.client.id.Address, recoveryReconcileLimit)
	if err != nil {
		log.Warn().Err(err).Msg("decrypt_recovery_orphan_read_failed")
		return
	}
	now := m.clock()
	for _, peer := range orphans {
		// The §4.10 seven-day clock anchors to the FIRST confirmed failure
		// and survives evictions: a re-admitted job inherits the original
		// deadline, and rows already past it expire here instead of buying
		// a fresh lifetime.
		deadline := m.jobDeadline(peer, now)
		if !now.Before(deadline) {
			if err := store.ExpireDecryptFailed(ctx, peer, m.router.client.id.Address); err != nil {
				log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_expire_failed")
				continue // the cycle stays anchored; the next pass retries
			}
			m.closeCycleIfIdle(store, peer)
			log.Info().Str("peer", peer).Msg("decrypt_recovery_orphan_expired")
			continue
		}
		// Free slots only: an evicting sweep over a full backlog would
		// rotate jobs in and out every pass, resetting their lifetime and
		// starving everyone — see AdmitRecoveryJobIfRoom.
		m.admissionMu.Lock()
		admitted, err := store.AdmitRecoveryJobIfRoom(peer, now, deadline, recoveryMaxResendsPerPeer, recoveryBacklogLimit)
		m.admissionMu.Unlock()
		if err != nil {
			log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_job_open_failed")
			continue
		}
		if admitted {
			log.Info().Str("peer", peer).Msg("decrypt_recovery_orphan_readmitted")
		}
	}
}

// jobDeadline derives the peer's hard deadline from the IMMUTABLE cycle
// anchor: first confirmed failure of the cycle + 7 days. The anchor is a
// durable per-peer row created on first use and closed only when the
// cycle truly ends — deriving from the live rows instead would let a
// flood roll the clock forward by recovering the oldest row before each
// eviction. The candidate for a brand-new cycle is the oldest flagged-at
// (≈ now when the row was flagged in this very call).
func (m *recoveryManager) jobDeadline(peer string, now time.Time) time.Time {
	store := m.store()
	if store == nil {
		return now.Add(recoveryJobLifetime)
	}
	candidate := now
	oldest, found, err := store.OldestDecryptFlaggedAt(context.Background(), peer, m.router.client.id.Address)
	if err != nil {
		log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_flagged_at_read_failed")
	}
	if err == nil && found {
		candidate = oldest
	}
	anchor, err := store.EnsureRecoveryCycle(peer, candidate)
	if err != nil {
		log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_cycle_anchor_failed")
		return candidate.Add(recoveryJobLifetime)
	}
	return anchor.Add(recoveryJobLifetime)
}

// closeCycleIfIdle runs the transactional idle-close (job + cycle anchor
// go together, and ONLY when no live flagged row remains — the store
// re-checks inside the same transaction, so a fresh failure racing this
// call keeps the anchor) and, when the close fired, releases the resolver
// work.
func (m *recoveryManager) closeCycleIfIdle(store *chatlog.Store, peer string) {
	closed, err := store.CloseRecoveryCycleIfIdle(peer, m.router.client.id.Address)
	if err != nil {
		log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_cycle_close_failed")
		return
	}
	if closed {
		m.releaseJobResolution(peer)
	}
}

// reconcileResendIntents settles sender terminals a crash interrupted: a
// durable intent whose root has no in-memory queue entry belongs to a
// previous process. The intent NAMES its replacement (the id was minted
// before the send), so the check is one row read, never a search: row
// present → write the terminal and settle. Row absent → the send either
// never left or the store write failed while the message went to the
// network; either way the ACTIVATION sweep restores the resend and sends
// under the SAME pre-minted id (the receiver's dedup makes that harmless
// when the lost send actually left), so nothing waits passively — the
// age-out below is only the terminal backstop for a peer that stays
// unreachable for the whole cycle lifetime.
func (m *recoveryManager) reconcileResendIntents(ctx context.Context, store *chatlog.Store) {
	intents, err := store.ResendIntents(ctx, recoveryBacklogLimit)
	if err != nil {
		log.Warn().Err(err).Msg("decrypt_recovery_intents_read_failed")
		return
	}
	now := m.clock()
	for _, intent := range intents {
		m.mu.Lock()
		entry, inFlight := m.pendingResends[intent.Root]
		_, indebted := m.terminalDebts[intent.Root]
		sentDebt := indebted || (inFlight && entry.sentReplacementID != "")
		m.mu.Unlock()
		if sentDebt {
			// The replacement already left and only its terminal is owed:
			// retryStalledResends retries the write until it commits — a
			// terminal DEBT is never expired, or a temporarily failing
			// write would strand a transmitted replacement.
			continue
		}
		_, found, err := store.EntryByID(domain.MessageID(intent.ReplacementID))
		if err != nil {
			log.Warn().Err(err).Str("retry_root", intent.Root).Msg("decrypt_recovery_intent_reconcile_failed")
			continue // retried next pass
		}
		if found {
			// The row proves the send happened (this run or a crashed one):
			// the intent IS a terminal debt. It is RESTORED into the debt
			// ledger — never treated as a pending resend again — and the
			// one common terminal flow pays it (retrying failures, and
			// settling a debt whose original vanished). Writing the
			// terminal inline here would leave a failing write looking
			// like an ordinary waiting intent: re-activated, re-proved and
			// re-sent under an id that already went out.
			m.mu.Lock()
			if _, active := m.pendingResends[intent.Root]; !active {
				if _, indebted := m.terminalDebts[intent.Root]; !indebted {
					m.terminalDebts[intent.Root] = recoveryResend{
						originalID:        intent.OriginalID,
						peer:              intent.Peer,
						replacementID:     intent.ReplacementID,
						sentReplacementID: intent.ReplacementID,
					}
					log.Info().Str("retry_root", intent.Root).Msg("decrypt_recovery_terminal_debt_restored")
				}
			}
			m.mu.Unlock()
			continue
		}
		// Not sent as far as durable knowledge goes: only these may hit the
		// seven-day age-out — an unreachable peer must not pin active,
		// backlog and proof work forever.
		if now.Sub(intent.CreatedAt) > recoveryJobLifetime {
			log.Info().Str("retry_root", intent.Root).Str("peer", intent.Peer).Msg("decrypt_recovery_resend_expired")
			m.releaseResend(store, intent.Root)
			continue
		}
		// Still live: either this process is driving it (inFlight) or the
		// activation sweep restores it.
	}
}

// sweepStaleCycles closes cycle anchors whose recovery finished while the
// peer had no job to run the close path (rows recovered between eviction
// and re-admission). The idle re-check runs inside the close transaction —
// a cycle with live rows stays anchored, job or not.
func (m *recoveryManager) sweepStaleCycles(ctx context.Context, store *chatlog.Store) {
	cycles, err := store.StaleRecoveryCycles(ctx, recoveryReconcileLimit)
	if err != nil {
		log.Warn().Err(err).Msg("decrypt_recovery_cycles_read_failed")
		return
	}
	for _, peer := range cycles {
		m.closeCycleIfIdle(store, peer)
	}
}

// sweepProofs drops armed proofs whose peer has neither a job nor a queued
// resend — the two owners a proof can serve. Keeps the map bounded by the
// job backlog plus the resend queue.
func (m *recoveryManager) sweepProofs(jobs []chatlog.RecoveryJob) {
	jobPeers := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		jobPeers[job.Peer] = struct{}{}
	}
	m.mu.Lock()
	for key := range m.proofs {
		var owned bool
		if key.scope == recoveryProofScopeJob {
			_, owned = jobPeers[key.peer]
		} else {
			_, owned = m.pendingResends[key.scope]
		}
		if !owned {
			delete(m.proofs, key)
		}
	}
	m.mu.Unlock()
}

// pollProofRequests re-reads every armed-but-ungranted proof through the
// resolve_identity_status RPC and feeds the answer back through the same
// noteResolution gate the live events use. An unknown id (the terminal
// state aged out of the resolver's retention) is left alone — the leg's
// own next attempt re-arms with a fresh resolution.
func (m *recoveryManager) pollProofRequests() {
	m.mu.Lock()
	waits := make(map[string]struct{}, len(m.proofs))
	for _, request := range m.proofs {
		if !request.granted {
			waits[request.resolutionID] = struct{}{}
		}
	}
	m.mu.Unlock()
	for resolutionID := range waits {
		frame := m.router.resolveIdentityStatusForRecovery(resolutionID)
		if frame == nil {
			continue
		}
		m.noteResolution(resolutionStateFromFrame(*frame))
	}
}

// retryStalledResends re-drives every queued sender-side re-send — the
// periodic half of the leg, complementing the event-driven
// retryPendingResends kick — and retries the terminal DEBTS (sent
// replacements whose terminal transaction has not committed): pure DB
// work outside the active pool.
func (m *recoveryManager) retryStalledResends() {
	store := m.store()
	m.mu.Lock()
	roots := make([]string, 0, len(m.pendingResends))
	for root, resend := range m.pendingResends {
		if !resend.awaitingTurn { // see retryPendingResends: order first
			roots = append(roots, root)
		}
	}
	debts := make(map[string]recoveryResend, len(m.terminalDebts))
	for root, debt := range m.terminalDebts {
		debts[root] = debt
	}
	m.mu.Unlock()
	for _, root := range roots {
		m.tryResend(root)
	}
	if store == nil {
		return
	}
	for root, debt := range debts {
		m.finishResendTerminal(store, root, debt)
	}
}

// resolutionStateFromFrame is the RPC-poll inverse of the node's frame
// projection: only the fields the recovery gate reads are recovered.
func resolutionStateFromFrame(frame protocol.IdentityResolutionFrame) ebus.IdentityResolutionState {
	target, err := domain.ParsePeerIdentity(frame.Target)
	if err != nil {
		return ebus.IdentityResolutionState{}
	}
	return ebus.IdentityResolutionState{
		ResolutionID:     frame.ResolutionID,
		Target:           target,
		Lifecycle:        domain.IdentityResolutionLifecycle(frame.Lifecycle),
		Authority:        domain.IdentityRecordAuthority(frame.Authority),
		DMAvailable:      domain.DMAvailability(frame.DMAvailable),
		Usable:           frame.Usable,
		AnswerAttemptGen: frame.AnswerAttemptGen,
	}
}

// noticeDue applies the receipt ladder to the job's attempt counter. An
// unconsumed proof grant makes the job due immediately: the grant event
// arrives between ladder steps, and making it wait a backoff step would
// throw away the freshness the gate exists to guarantee.
func (m *recoveryManager) noticeDue(job chatlog.RecoveryJob, now time.Time) bool {
	if job.LastNoticeAt.IsZero() || m.hasGrantedProof(job.Peer) {
		return true
	}
	step := job.NoticeAttempts
	if step >= len(recoveryNoticeDelays) {
		step = len(recoveryNoticeDelays) - 1
	}
	return now.Sub(job.LastNoticeAt) >= recoveryNoticeDelays[step]
}

// attemptNotice runs one receiver-leg attempt for one peer: refresh the
// sender's keys through the lookup FIRST (a notice encrypted to a stale
// key would be lost silently — control-DM decrypt errors are dropped),
// then send decrypt_failed for the oldest flagged row. The slot is held
// only for this call; a parked or unresolved job frees it immediately.
func (m *recoveryManager) attemptNotice(ctx context.Context, store *chatlog.Store, job chatlog.RecoveryJob) {
	flagged, err := store.DecryptFailedEntries(ctx, job.Peer, m.router.client.id.Address, recoveryWorksetLimit)
	if err != nil {
		log.Warn().Err(err).Str("peer", job.Peer).Msg("decrypt_recovery_workset_read_failed")
		return
	}
	if len(flagged) == 0 {
		// Every row recovered (or removed): the job and its cycle close
		// together, re-checked transactionally against fresh failures.
		m.closeCycleIfIdle(store, job.Peer)
		return
	}

	// The mandatory proof-bearing lookup replaces the removed cache_bypass:
	// a stale-but-valid record is cryptographically indistinguishable from
	// a fresh one, and only the owner can mint the attempt-bound proof. The
	// gate is a consumable grant bound to THIS job's own lookup, never
	// `usable` and never a foreign completion: usable flips on the first
	// provisional source, which may be exactly the dead key this notice is
	// about — encrypting to it would lose the notice silently.
	if !m.consumeRecoveryProof(job.Peer, recoveryProofScopeJob) {
		m.requestRecoveryProof(job.Peer, recoveryProofScopeJob, flagged[0].ID)
		// The resolver keeps digging; the grant makes the job due again
		// through noticeDue. The served stamp still advances — a
		// proof-waiting head must rotate to the back of the queue, not
		// occupy its slot pass after pass while the tail starves.
		job.LastNoticeAt = m.clock()
		if err := store.UpdateRecoveryJob(job); err != nil {
			log.Warn().Err(err).Str("peer", job.Peer).Msg("decrypt_recovery_job_update_failed")
		}
		return
	}

	payload, err := domain.MarshalDecryptFailedPayload(domain.DecryptFailedPayload{
		MessageID: domain.MessageID(flagged[0].ID),
	})
	if err != nil {
		log.Error().Err(err).Str("peer", job.Peer).Msg("decrypt_recovery_notice_marshal_failed")
		return
	}
	_, sendErr := m.router.client.dm.SendControlMessage(ctx, domain.PeerIdentityFromWire(job.Peer), domain.DMCommandDecryptFailed, payload)

	now := m.clock()
	job.LastNoticeAt = now
	if sendErr != nil {
		// A local send error keeps the job in pending_notice with backoff —
		// the attempt counter advances so the ladder spaces retries out.
		log.Warn().Err(sendErr).Str("peer", job.Peer).Msg("decrypt_recovery_notice_send_failed")
	}
	job.NoticeAttempts++
	if job.NoticeAttempts >= len(recoveryNoticeDelays) {
		// Ladder exhausted: park for 24 h, freeing the slot — then a fresh
		// lookup and a new cycle.
		job.State = chatlog.DecryptStateWaitingRetry
		job.WaitUntil = now.Add(recoveryWaitingRetry)
		for _, entry := range flagged {
			if err := store.SetDecryptState(entry.ID, chatlog.DecryptStateWaitingRetry); err != nil {
				log.Warn().Err(err).Str("message_id", entry.ID).Msg("decrypt_recovery_state_update_failed")
			}
		}
	}
	if err := store.UpdateRecoveryJob(job); err != nil {
		log.Warn().Err(err).Str("peer", job.Peer).Msg("decrypt_recovery_job_update_failed")
	}
}

// expireJob marks EVERY remaining live flagged row expired (one UPDATE —
// the deadline must reach all rows, not a workset-sized prefix: a row left
// live would come back through the orphan sweep with a fresh lifetime) and
// drops the job.
func (m *recoveryManager) expireJob(ctx context.Context, store *chatlog.Store, job chatlog.RecoveryJob) {
	if err := store.ExpireDecryptFailed(ctx, job.Peer, m.router.client.id.Address); err != nil {
		log.Warn().Err(err).Str("peer", job.Peer).Msg("decrypt_recovery_expire_failed")
		// The job stays; the next pass retries the expiry — deleting it
		// now would hand the still-live rows a fresh lifetime through the
		// orphan sweep.
		return
	}
	m.closeCycleIfIdle(store, job.Peer)
	log.Info().Str("peer", job.Peer).Msg("decrypt_recovery_job_expired")
}

// ---------------------------------------------------------------------------
// The sender leg
// ---------------------------------------------------------------------------

// handleInboundDecryptFailed serves a decrypt_failed notice from an
// authenticated peer. Every fact is validated against the SENDER'S OWN
// chatlog: the row must exist, be authored by this node and addressed to
// the notice's sender — the re-send recipient comes from the local row,
// never from the notice, or a crafted notice would redirect plaintext.
func (r *DMRouter) handleInboundDecryptFailed(envelopeSender string, payloadJSON string) {
	payload, err := unmarshalDecryptFailedPayload(payloadJSON)
	if err != nil {
		log.Debug().Err(err).Str("peer", envelopeSender).Msg("decrypt_failed_notice_malformed")
		return
	}
	store := r.client.chatLog
	if store == nil || r.recovery == nil {
		return
	}
	entry, found, err := store.EntryByID(payload.MessageID)
	if err != nil || !found {
		log.Debug().Err(err).Str("message_id", string(payload.MessageID)).Msg("decrypt_failed_notice_unknown_row")
		return
	}
	if entry.Sender != r.client.id.Address || entry.Recipient != envelopeSender {
		// Not our message, or not this peer's: a crafted notice.
		log.Warn().Str("peer", envelopeSender).Str("message_id", entry.ID).Msg("decrypt_failed_notice_row_mismatch")
		return
	}
	marks, _, err := store.EntryRecoveryMarks(entry.ID)
	if err != nil {
		log.Warn().Err(err).Str("message_id", entry.ID).Msg("decrypt_failed_notice_marks_read_failed")
		return
	}
	if marks.SupersededBy != "" {
		// Already replaced; a repeat notice references a superseded row.
		return
	}
	root := marks.RetryRootID
	if root == "" {
		root = entry.ID
	}
	chain, err := store.CountRetryChain(root)
	if err != nil {
		log.Warn().Err(err).Str("retry_root", root).Msg("decrypt_failed_chain_count_failed")
		return
	}
	if chain >= recoveryRetryChainLimit {
		log.Warn().Str("retry_root", root).Int("chain", chain).Msg("decrypt_failed_chain_limit_reached")
		return
	}

	// Admission into the bounded durable backlog (§4.10: overflow is
	// QUEUED, never silently dropped — per-peer ≤3, global bounded with
	// the established reservation and unknown-first LRU eviction). The
	// replacement id is minted here, but a root already admitted returns
	// its CANONICAL intent — a retried notice reuses the stored id instead
	// of minting a divergent one.
	replacementID, err := protocol.NewMessageID()
	if err != nil {
		log.Error().Err(err).Str("retry_root", root).Msg("decrypt_recovery_replacement_id_entropy_failed")
		return
	}
	_, admitted, victim, err := r.recovery.admitResendIntent(store, chatlog.ResendIntent{
		Root:          root,
		OriginalID:    entry.ID,
		Peer:          envelopeSender,
		ReplacementID: string(replacementID),
		CreatedAt:     r.recovery.clock(),
	}, recoveryMaxResendsPerPeer, recoveryBacklogLimit)
	if err != nil {
		log.Warn().Err(err).Str("retry_root", root).Msg("decrypt_recovery_intent_admit_failed")
		return
	}
	r.recovery.releaseEvictionVictim(store, victim)
	if !admitted {
		log.Warn().Str("peer", envelopeSender).Str("retry_root", root).Msg("decrypt_recovery_resend_backlog_refused")
		return
	}
	// Activation happens ONLY in the scheduler pass — the single holder
	// of the shared active-pool arithmetic. The nudge fires it now, so the
	// notice-to-resend latency is one wakeup, not a tick.
	r.recovery.nudge()
}

// admitResendIntent runs the backlog admission under admissionMu: the
// protected-roots snapshot and the eviction are one serialized decision
// with every activation, so a victim can never be a root the scheduler is
// concurrently activating.
func (m *recoveryManager) admitResendIntent(store *chatlog.Store, intent chatlog.ResendIntent, perPeerLimit, globalLimit int) (chatlog.ResendIntent, bool, chatlog.RecoveryEvictionVictim, error) {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	return store.AdmitResendIntent(intent, perPeerLimit, globalLimit, m.protectedWork())
}

// tryResend attempts one queued sender-side re-send; keys may not be
// usable yet, in which case the resolution-changed event retries it. The
// whole attempt runs under the per-root CLAIM: a concurrent expiry or
// eviction cleanup cannot strip the entry or its durable intent between
// the intent probe and the send.
func (m *recoveryManager) tryResend(root string) {
	store := m.store()
	if store == nil || m.router.client == nil || m.router.client.dm == nil {
		return
	}
	resend, claimed := m.claimResend(root)
	if !claimed {
		return
	}
	defer m.unclaimResend(store, root)

	if resend.sentReplacementID != "" {
		// The replacement already left; only the durable terminal is owed.
		m.finishResendTerminal(store, root, resend)
		return
	}
	// The re-send encrypts to the target's CURRENT keys, so it waits for
	// its OWN per-root proof grant; the resolution event retries it the
	// moment the grant lands. A missing grant re-arms the request — each
	// root anchors its own watermark and consumes its own grant, so
	// several roots of one peer each re-prove the keys they encrypt to.
	if !m.consumeRecoveryProof(resend.peer, root) {
		m.requestRecoveryProof(resend.peer, root, resend.originalID)
		return
	}
	// The durable re-check, valid for the rest of the attempt because the
	// claim blocks every concurrent release: a missing intent means an
	// eviction won before the claim, and a replacement id that no longer
	// matches means the root was released and re-admitted with a fresh id
	// while this task was queued — sending the STALE id would diverge the
	// wire from the intent (the ABA case). Either way ONLY the stale
	// in-memory task retires: the durable state, if any, belongs to the
	// root's new incarnation and must survive for its own activation.
	intent, intact, err := store.ResendIntentByRoot(root)
	if err != nil {
		log.Warn().Err(err).Str("retry_root", root).Msg("decrypt_recovery_intent_probe_failed")
		return
	}
	if !intact || intent.ReplacementID != resend.replacementID {
		m.mu.Lock()
		delete(m.pendingResends, root)
		delete(m.proofs, recoveryProofKey{peer: resend.peer, scope: root})
		m.mu.Unlock()
		return
	}
	// An unrecoverable plaintext (the original row deleted, or the own
	// sealed copy unreadable — a lost box key) retires the resend WHOLE:
	// leaving the durable intent behind would re-activate it every pass
	// and burn proof lookups forever, while the honest outcome is the
	// manual "send again" fallback.
	entry, found, err := store.EntryByID(domain.MessageID(resend.originalID))
	if err != nil || !found {
		m.releaseResendOwned(store, root)
		return
	}
	self := m.router.client.id
	plain, err := directmsg.DecryptForIdentity(self, entry.Sender, identity.PublicKeyBase64(self.PublicKey), entry.Recipient, entry.Body)
	if err != nil {
		log.Warn().Err(err).Str("message_id", entry.ID).Msg("decrypt_recovery_own_copy_unreadable")
		m.releaseResendOwned(store, root)
		return
	}
	if resend.replacementID == "" {
		log.Error().Str("retry_root", root).Msg("decrypt_recovery_resend_without_replacement_id")
		m.releaseResendOwned(store, root)
		return
	}
	outgoing := domain.OutgoingDM{
		Body:     plain.Body,
		ReplyTo:  domain.MessageID(plain.ReplyTo),
		RetryOf:  domain.MessageID(entry.ID),
		PresetID: domain.MessageID(resend.replacementID),
	}
	echo, err := m.router.client.dm.SendDirectMessage(context.Background(), domain.PeerIdentityFromWire(resend.peer), outgoing)
	if err != nil {
		if errors.Is(err, ErrRecipientKeysUnknown) {
			// Keys still on their way: the resolution event will retry; the
			// intent stays with the queued entry.
			return
		}
		log.Warn().Err(err).Str("message_id", entry.ID).Msg("decrypt_recovery_resend_failed")
		m.releaseResendOwned(store, root)
		return
	}

	// The replacement is accepted for delivery: remember it BEFORE the
	// terminal write, so a failed write is retried instead of shrugged off
	// — a dropped terminal would put the original back into ordinary retry
	// racing its own replacement, and an unstamped replacement would reset
	// the chain budget.
	resend.sentReplacementID = echo.ID
	m.mu.Lock()
	if current, queued := m.pendingResends[root]; queued {
		current.sentReplacementID = echo.ID
		m.pendingResends[root] = current
	}
	m.mu.Unlock()
	m.finishResendTerminal(store, root, resend)
}

// releaseResendOwned is releaseResend for the CLAIM HOLDER: it clears the
// claim first so the release executes now instead of deferring to itself.
func (m *recoveryManager) releaseResendOwned(store *chatlog.Store, root string) {
	m.mu.Lock()
	if resend, ok := m.pendingResends[root]; ok {
		resend.busy = false
		resend.pendingRelease = false
		m.pendingResends[root] = resend
	}
	m.mu.Unlock()
	m.releaseResend(store, root)
}

// finishResendTerminal writes the one-transaction sender terminal
// (supersede the original + stamp the replacement's chain root) and only
// then releases the entry. A failed write moves the entry into
// terminalDebts — pure local DB retries with NO network work, holding no
// active-pool slot (a peer whose terminal cannot commit yet must not
// shrink the shared 20 forever) while the durable intent stays protected.
// An original that no longer exists has nothing left to supersede: the
// replacement stands as an ordinary message and the debt settles.
func (m *recoveryManager) finishResendTerminal(store *chatlog.Store, root string, resend recoveryResend) {
	if err := store.MarkResendTerminal(resend.originalID, resend.sentReplacementID, root); err != nil {
		if _, found, lookupErr := store.EntryByID(domain.MessageID(resend.originalID)); lookupErr == nil && !found {
			log.Info().Str("retry_root", root).Msg("decrypt_recovery_terminal_moot_original_gone")
			m.releaseResendOwned(store, root)
			return
		}
		log.Warn().Err(err).
			Str("original", resend.originalID).
			Str("resend", resend.sentReplacementID).
			Msg("decrypt_recovery_terminal_write_failed")
		m.mu.Lock()
		if current, queued := m.pendingResends[root]; queued {
			delete(m.pendingResends, root)
			current.busy = false
			current.pendingRelease = false
			m.terminalDebts[root] = current
		}
		m.mu.Unlock()
		return
	}
	// The intent outlives the terminal by a hair on purpose: a crash
	// between the two re-runs the (idempotent) terminal from the intent,
	// never the reverse gap. releaseResendOwned also retires the proof
	// scope and, when this was the peer's last recovery work, the durable
	// lookup reasons.
	m.releaseResendOwned(store, root)
	log.Info().
		Str("peer", resend.peer).
		Str("original", resend.originalID).
		Str("resend", resend.sentReplacementID).
		Msg("decrypt_recovery_resent")
}

// protectedWork snapshots the rows live attempts depend on — the eviction
// protection handed to the backlog admissions: every queued or indebted
// resend root (their durable intents are the crash insurance) and every
// peer with a notice attempt running.
func (m *recoveryManager) protectedWork() chatlog.RecoveryProtectedWork {
	m.mu.Lock()
	defer m.mu.Unlock()
	protected := chatlog.RecoveryProtectedWork{
		ResendRoots: make([]string, 0, len(m.pendingResends)+len(m.terminalDebts)),
		JobPeers:    make([]string, 0, len(m.activeNotices)),
	}
	for root := range m.pendingResends {
		protected.ResendRoots = append(protected.ResendRoots, root)
	}
	for root := range m.terminalDebts {
		protected.ResendRoots = append(protected.ResendRoots, root)
	}
	for peer := range m.activeNotices {
		protected.JobPeers = append(protected.JobPeers, peer)
	}
	return protected
}

// releaseResend retires one sender-side re-send WHOLE: the queue (or
// debt) entry, its proof scope, its durable intent, and — when the peer
// has no other resend and no receiver job left — the durable
// recovery-typed lookup reasons. Every terminal outcome of a resend
// funnels through here. A root whose claim is currently held is NOT
// stripped: the release is recorded on the entry and executed when the
// claim drops — expiry and eviction cleanup are mutually exclusive with
// an in-flight send by construction.
func (m *recoveryManager) releaseResend(store *chatlog.Store, root string) {
	m.mu.Lock()
	if resend, ok := m.pendingResends[root]; ok && resend.busy {
		resend.pendingRelease = true
		m.pendingResends[root] = resend
		m.mu.Unlock()
		return
	}
	resend, owned := m.pendingResends[root]
	if !owned {
		resend, owned = m.terminalDebts[root]
	}
	delete(m.pendingResends, root)
	delete(m.terminalDebts, root)
	if owned {
		delete(m.proofs, recoveryProofKey{peer: resend.peer, scope: root})
	}
	othersForPeer := false
	if owned {
		for _, queued := range m.pendingResends {
			if queued.peer == resend.peer {
				othersForPeer = true
				break
			}
		}
		for _, debtor := range m.terminalDebts {
			if debtor.peer == resend.peer {
				othersForPeer = true
				break
			}
		}
	}
	m.mu.Unlock()
	if store != nil {
		if err := store.DeleteResendIntent(root); err != nil {
			log.Warn().Err(err).Str("retry_root", root).Msg("decrypt_recovery_intent_delete_failed")
		}
	}
	if !owned || othersForPeer {
		return
	}
	m.cancelPeerReasonsIfIdle(store, resend.peer)
}

// cancelPeerReasonsIfIdle drops the peer's durable recovery-typed lookup
// reasons when NO recovery work is left for it — no queued resend, no
// terminal debt, no receiver job. Without it a lookup keeps burning
// background attempts for a peer nothing will ever act on.
func (m *recoveryManager) cancelPeerReasonsIfIdle(store *chatlog.Store, peer string) {
	if peer == "" {
		return
	}
	m.mu.Lock()
	busy := false
	for _, queued := range m.pendingResends {
		if queued.peer == peer {
			busy = true
			break
		}
	}
	if !busy {
		for _, debtor := range m.terminalDebts {
			if debtor.peer == peer {
				busy = true
				break
			}
		}
	}
	m.mu.Unlock()
	if busy {
		return
	}
	hasJob := false
	if store != nil {
		var err error
		hasJob, err = store.HasRecoveryJob(peer)
		if err != nil {
			log.Warn().Err(err).Str("peer", peer).Msg("decrypt_recovery_job_probe_failed")
			return // keep the reasons rather than cancel a live job's lookup
		}
	}
	if !hasJob {
		m.router.cancelRecoveryResolution(peer)
	}
}

// releaseEvictionVictim routes a backlog eviction to the right cleanup:
// a receiver job keeps its cycle anchor and row flags (the orphan sweep
// re-admits them), a sender resend retires whole.
func (m *recoveryManager) releaseEvictionVictim(store *chatlog.Store, victim chatlog.RecoveryEvictionVictim) {
	if victim.None() {
		return
	}
	if victim.Job {
		m.releaseJobResolution(victim.Key)
		return
	}
	m.releaseResend(store, victim.Key)
	// releaseResend only reaches the reason-cancel for roots it OWNED in
	// memory; an evicted intent of a crashed predecessor was never
	// restored, so its peer's lookup reasons are dropped here from the
	// victim's own peer field.
	m.cancelPeerReasonsIfIdle(store, victim.Peer)
}

// retryPendingResends is kicked by identity-resolution events: fresh keys
// may unblock queued re-sends of that peer.
func (m *recoveryManager) retryPendingResends(peer string) {
	m.mu.Lock()
	roots := make([]string, 0, len(m.pendingResends))
	for root, resend := range m.pendingResends {
		// An entry still awaiting its computed turn is not event-runnable:
		// running it now could serve a later (even unknown) item ahead of
		// earlier established ones and break the pass's service order.
		if resend.peer == peer && !resend.awaitingTurn {
			roots = append(roots, root)
		}
	}
	m.mu.Unlock()
	for _, root := range roots {
		m.tryResend(root)
	}
}

// ---------------------------------------------------------------------------
// The receiver-side retry_of acceptance
// ---------------------------------------------------------------------------

// acceptRetryOf applies an incoming re-send to its flagged original. The
// §4.10 acceptance contract: same authenticated author, addressed to this
// node, really flagged, not yet superseded — otherwise the link is ignored
// and the message stands on its own (a peer must not be able to "replace"
// arbitrary history).
func (m *recoveryManager) acceptRetryOf(msg *DirectMessage) {
	if msg == nil || msg.RetryOf == "" {
		return
	}
	store := m.store()
	if store == nil {
		return
	}
	original, found, err := store.EntryByID(msg.RetryOf)
	if err != nil || !found {
		return
	}
	if original.Sender != msg.Sender.String() || original.Recipient != m.router.client.id.Address {
		log.Warn().Str("peer", msg.Sender.String()).Str("retry_of", string(msg.RetryOf)).Msg("retry_of_row_mismatch")
		return
	}
	marks, _, err := store.EntryRecoveryMarks(original.ID)
	if err != nil || !marks.DecryptFailed || marks.SupersededBy != "" {
		return
	}
	root := marks.RetryRootID
	if root == "" {
		root = original.ID
	}
	// The store re-checks "still flagged, not superseded" INSIDE the
	// transaction: two paths decrypting replacements concurrently (live
	// event vs history load) both reach here, and only the first may
	// write the link — the loser backs off without touching the rows.
	applied, err := store.MarkSupersededCollapsing(original.ID, msg.ID, root)
	if err != nil {
		log.Warn().Err(err).Str("message_id", original.ID).Msg("retry_of_supersede_failed")
		return
	}
	if !applied {
		return // a concurrent acceptance won the race — its link stands
	}
	log.Info().Str("peer", msg.Sender.String()).Str("original", original.ID).Str("resend", msg.ID).Msg("decrypt_recovery_row_recovered")

	// Close the job when nothing flagged remains.
	m.closeCycleIfIdle(store, original.Sender)
}

// ---------------------------------------------------------------------------
// Router plumbing
// ---------------------------------------------------------------------------

// cancelRecoveryResolution drops every recovery-typed lookup reason of the
// peer (its job closed, expired or was evicted): the refcount semantics
// keep the resolution alive when ui_chat / pending_send reasons remain.
func (r *DMRouter) cancelRecoveryResolution(peer string) {
	if r.client == nil || r.client.localNode == nil {
		return
	}
	target, err := domain.ParsePeerIdentity(peer)
	if err != nil || target.IsZero() {
		return
	}
	r.client.localNode.CancelRecoveryResolutionReasons(target)
}

// resolveIdentityForRecovery starts (or joins) the proof-bearing lookup
// with the recovery reason and the message id as the durable reason id.
func (r *DMRouter) resolveIdentityForRecovery(peer, messageID string) protocol.IdentityResolutionFrame {
	if r.client == nil || r.client.rpc == nil {
		return protocol.IdentityResolutionFrame{}
	}
	reply, err := r.client.rpc.LocalRequestFrame(protocol.Frame{
		Type:             "resolve_identity",
		Address:          peer,
		ID:               messageID,
		ResolutionReason: "recovery",
	})
	if err != nil || reply.Resolution == nil {
		log.Debug().Err(err).Str("peer", peer).Msg("decrypt_recovery_resolve_kick_failed")
		return protocol.IdentityResolutionFrame{}
	}
	return *reply.Resolution
}

// resolveIdentityStatusForRecovery polls one resolution's retained state;
// nil when the id is unknown (terminal aged out) or the RPC is unavailable.
func (r *DMRouter) resolveIdentityStatusForRecovery(resolutionID string) *protocol.IdentityResolutionFrame {
	if r.client == nil || r.client.rpc == nil || resolutionID == "" {
		return nil
	}
	reply, err := r.client.rpc.LocalRequestFrame(protocol.Frame{Type: "resolve_identity_status", ResolutionID: resolutionID})
	if err != nil || reply.Resolution == nil {
		return nil
	}
	return reply.Resolution
}

func unmarshalDecryptFailedPayload(payloadJSON string) (domain.DecryptFailedPayload, error) {
	var payload domain.DecryptFailedPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return domain.DecryptFailedPayload{}, err
	}
	if !payload.Valid() {
		return domain.DecryptFailedPayload{}, errors.New("decrypt_failed payload invalid")
	}
	return payload, nil
}
