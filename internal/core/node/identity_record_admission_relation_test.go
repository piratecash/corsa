package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

type admissionTestClock struct {
	elapsed  atomic.Int64
	wallStep atomic.Int64
}

func (c *admissionTestClock) sample() relationObservedTime {
	elapsed := time.Duration(c.elapsed.Load())
	return relationObservedTime{wall: time.Unix(1780000000, 0).Add(elapsed + time.Duration(c.wallStep.Load())), elapsed: elapsed}
}
func admissionResolverFixture(t *testing.T) (*identityResolver, *admissionTestClock) {
	t.Helper()
	_, svc := newTestResolver(t)
	r := newIdentityResolverWithRecordAdmission(svc, loadIdentityIntentStore(filepath.Join(t.TempDir(), "admission-intents.json")), testRecordStoreNetwork)
	svc.identityResolver = r
	clock := &admissionTestClock{}
	r.recordAdmission.clock = clock.sample
	r.clock = func() time.Time { return clock.sample().wall }
	if !r.beginRecordAdmissionRun() {
		t.Fatal("fresh owner could not begin")
	}
	clock.elapsed.Store(int64(time.Minute))
	return r, clock
}

// The fixture registers exactly as the real send boundary, with the actual
// removal gate held through insertion. TestActualSend below uses sendOwnedAttempt.
func admissionAnswerFixture(t *testing.T, r *identityResolver) (UserIdentityLookup, domain.PeerIdentity, identityAttemptEntry, []byte, []byte) {
	t.Helper()
	action, label, entry, request, answer := lookupProvenAnswer(t, r, true)
	store := r.svc.localContactPinStore()
	store.pinMutationGate.RLock()
	r.mu.Lock()
	entry.recordAdmission = r.reserveRelationAttemptLocked(label, entry, store)
	r.attempts[label] = entry
	r.mu.Unlock()
	store.pinMutationGate.RUnlock()
	if entry.recordAdmission == nil {
		t.Fatal("original attempt owner not captured")
	}
	return action, label, entry, request, answer
}
func admissionQueuedFixture(t *testing.T, r *identityResolver) (*relationAdmissionSlot, UserIdentityLookup) {
	t.Helper()
	action, label, entry, request, answer := admissionAnswerFixture(t, r)
	accepted := lookupRealQueueOutcome(t, r, label, entry, request)
	if !r.HandleAnswer(datagram.NewLabel(label), answer) {
		t.Fatal("verified answer rejected")
	}
	r.mu.Lock()
	_, _, _, _, mutationErr := r.settleQueueLocked(label, entry, accepted)
	if mutationErr != nil {
		r.mu.Unlock()
		t.Fatal(mutationErr)
	}
	slot := r.relationSlotLocked(entry.recordAdmission)
	if slot == nil || slot.phase != relationAdmissionQueued || !slot.protectionHeld {
		r.mu.Unlock()
		t.Fatal("joint actual queue and answer did not transfer")
	}
	r.mu.Unlock()
	return slot, action
}
func TestRecordAdmissionActualSendSignedAnswerAndStrictCompletion(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	target := domain.PeerIdentityFromWire(peer.Address)
	if _, err := r.svc.StartUserIdentityLookup(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	declarations := datagramPeerDeclarations()
	declarations.DeclaredDTypes = domain.ExplicitDTypes([]domain.DType{domain.DTypeGetIdentity})
	installDatagramPeer(t, r.svc, target, datagramPeerConn{version: datagramTestPeerVersion, connectedAt: time.Now(), declared: &declarations})
	r.mu.Lock()
	res := r.resolutions[target]
	r.mu.Unlock()
	r.sendOwnedAttempt(t.Context(), res, r.clock())
	r.mu.Lock()
	var label domain.PeerIdentity
	var entry identityAttemptEntry
	for l, e := range r.attempts {
		if e.owner == res {
			label, entry = l, e
		}
	}
	r.mu.Unlock()
	if entry.recordAdmission == nil || entry.queue != identityAttemptQueueAccepted {
		t.Fatal("real SendLocal registered without original owner/accepted queue")
	}
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: 1, TargetProof: true})
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(request) != entry.qHash {
		t.Fatal("question differs from actual Send")
	}
	record, err := protocol.BuildSignedIdentityRecord(peer, protocol.IdentityRecordSpec{Network: r.network, Seq: 3, IssuedAt: 1, DM: true, MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure)})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := protocol.BuildPostIdentityPayload(protocol.PostIdentityPayload{V: 1, Record: record, TargetProof: protocol.SignTargetProof(peer, r.network, label, request, record)})
	if err != nil {
		t.Fatal(err)
	}
	if !r.HandleAnswer(datagram.NewLabel(label), answer) {
		t.Fatal("actual answer rejected")
	}
	r.mu.Lock()
	slot := r.selectRelationSlotLocked()
	_, running := r.resolutions[target]
	r.mu.Unlock()
	if slot == nil || running {
		t.Fatal("ordinary success delayed or candidate absent")
	}
	if got := r.svc.liveRecordProtectionCounts()[target.String()]; got != 1 {
		t.Fatalf("slot lost reconcile ref: %d", got)
	}
	if err := r.processRelationSlot(t.Context(), slot); err != nil {
		t.Fatal(err)
	}
	stats := r.svc.localContactPinStore().stats()
	if stats.RelationAllocations != 1 || stats.RelationConfirmed != 1 || r.recordAdmission.rate.count != 1 || slot.phase != relationAdmissionFree {
		t.Fatalf("actual stage/strict/barrier/debit incomplete: %+v", stats)
	}
	if got := r.svc.liveRecordProtectionCounts()[target.String()]; got != 0 {
		t.Fatal("terminal ref retained", got)
	}
}
func TestRecordAdmissionFastAnswerQueueWindowCancellationAndReconcile(t *testing.T) {
	for _, kind := range []string{"accepted", "late", "refused", "cancelled", "exact60", "replacement", "finish-failure"} {
		t.Run(kind, func(t *testing.T) {
			r, clock := admissionResolverFixture(t)
			action, label, entry, request, answer := admissionAnswerFixture(t, r)
			accepted := lookupRealQueueOutcome(t, r, label, entry, request)
			if kind == "finish-failure" {
				r.intents.mu.Lock()
				r.intents.snapshotGen = ^uint64(0)
				r.intents.mu.Unlock()
			}
			if !r.HandleAnswer(datagram.NewLabel(label), answer) {
				t.Fatal("normal success lost")
			}
			if kind == "finish-failure" {
				r.mu.Lock()
				slot := r.relationSlotLocked(entry.recordAdmission)
				current := r.attempts[label]
				r.mu.Unlock()
				if slot != nil || current.claim != nil || current.recordAdmission != nil {
					t.Fatal("failed finish leaked/transferred queue authority")
				}
				return
			}
			switch kind {
			case "late":
				clock.elapsed.Add(int64(time.Minute + time.Nanosecond))
			case "exact60":
				clock.elapsed.Add(int64(time.Minute))
			case "refused":
				accepted = datagram.SendOutcome{}
			case "cancelled":
				if err := r.svc.CancelUserIdentityLookup(t.Context(), action); err != nil {
					t.Fatal(err)
				}
			case "replacement":
				if _, err := r.svc.StartUserIdentityLookup(t.Context(), entry.dst); err != nil {
					t.Fatal(err)
				}
			}
			r.mu.Lock()
			_, _, _, _, mutationErr := r.settleQueueLocked(label, entry, accepted)
			if mutationErr != nil {
				r.mu.Unlock()
				t.Fatal(mutationErr)
			}
			slot := r.relationSlotLocked(entry.recordAdmission)
			r.mu.Unlock()
			want := kind == "accepted" || kind == "exact60" || kind == "replacement"
			if (slot != nil && slot.phase == relationAdmissionQueued) != want {
				t.Fatalf("joint qualification %s wrong", kind)
			}
			if want {
				r.svc.reconcileRecordProtection()
				if !r.svc.recordProtection.contains(entry.dst.String()) {
					t.Fatal("reconcile dropped owned seq floor")
				}
				if err := r.svc.CancelUserIdentityLookup(t.Context(), action); err != nil {
					t.Fatal(err)
				}
				r.mu.Lock()
				slot = r.relationSlotLocked(entry.recordAdmission)
				r.mu.Unlock()
				if slot != nil {
					t.Fatal("postSuccess exact cancel lost slot")
				}
			}
			if r.recordAdmission.rate.count != 0 {
				t.Fatal("prestage rejection charged")
			}
		})
	}
}
func TestRecordAdmissionCapacityProcessingAndSlotABA(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	_, label, entry, _, _ := admissionAnswerFixture(t, r)
	r.mu.Lock()
	original := *entry.recordAdmission
	slot := r.relationSlotLocked(&original)
	slot.processing = true
	for i := 1; i < relationAdmissionCapacity; i++ {
		r.recordAdmission.slots[i] = relationAdmissionSlot{generation: 1, phase: relationAdmissionProspective, lifetime: slot.lifetime, registered: slot.registered}
	}
	r.mu.Unlock()
	store := r.svc.localContactPinStore()
	store.pinMutationGate.RLock()
	r.mu.Lock()
	if ref := r.reserveRelationAttemptLocked(label, entry, store); ref != nil {
		r.mu.Unlock()
		store.pinMutationGate.RUnlock()
		t.Fatal("Processing/prospective omitted from shared256")
	}
	r.clearRelationSlotLocked(slot)
	if slot.phase == relationAdmissionFree || !slot.cancelled {
		r.mu.Unlock()
		store.pinMutationGate.RUnlock()
		t.Fatal("cancel reused actual Processing buffer")
	}
	slot.processing = false
	r.clearRelationSlotLocked(slot)
	ref := r.reserveRelationAttemptLocked(label, entry, store)
	stale := r.relationSlotLocked(&original)
	r.mu.Unlock()
	store.pinMutationGate.RUnlock()
	if ref == nil || ref.generation == original.generation || stale != nil {
		t.Fatal("slot ABA/reuse failed")
	}
}
func TestRecordAdmissionPrestageOriginalOwnerKAndCancellation(t *testing.T) {
	// The diagnostic must identify the actual original removal gate, not a
	// different read lock or a stale source location after a prior refactor.
	raw, err := os.ReadFile("identity_record_admission_relation.go")
	if err != nil {
		t.Fatal(err)
	}
	gate := "\tstore.pinMutationGate.RLock()"
	location := strings.Index(string(raw), gate)
	if strings.Count(string(raw), gate) != 1 || location < 0 || strings.Count(string(raw[:location]), "\n")+1 != 137 {
		t.Fatal("original removal-gate diagnostic location needs exact rebind")
	}
	for _, kind := range []string{"cancel-waitgate", "new-owner", "newer-min1", "K", "quota-full", "already-R"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := admissionResolverFixture(t)
			slot, _ := admissionQueuedFixture(t, r)
			store := r.svc.localContactPinStore()
			id := slot.observed.identity
			switch kind {
			case "new-owner":
				store.mu.Lock()
				err := store.recordPinEvidenceLocked(id, store.now())
				store.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case "newer-min1":
				r.svc.trust.pinEvidenceMu.Lock()
				r.svc.trust.mu.Lock()
				key := trustRecordKey{network: r.network.String(), address: id.String()}
				v, ok := r.svc.trust.recordLocked(key)
				if !ok {
					r.svc.trust.mu.Unlock()
					r.svc.trust.pinEvidenceMu.Unlock()
					t.Fatal("fixture record missing")
				}
				v.body.Seq++
				v.body.MinSession = domain.DeclaredMinSession(domain.SessionVersionLegacy)
				r.svc.trust.records[key] = v
				r.svc.trust.mu.Unlock()
				r.svc.trust.pinEvidenceMu.Unlock()
			case "K":
				_, body, present := r.svc.trust.recordFor(r.network, id)
				if !present {
					t.Fatal("current signed record missing")
				}
				result, err := r.svc.AddLocalContact(t.Context(), domain.LocalContactIntent{Network: r.network, Identity: id, PubKey: body.PubKey, BoxKey: body.BoxKey, BoxSignature: body.BoxSig})
				if err != nil || result.Fact != domain.LocalContactFactDurableConfirmed {
					t.Fatal("actual local contact not confirmed", err)
				}
			case "quota-full":
				store.mu.Lock()
				store.relationAllocations = maxRelationPins
				store.mu.Unlock()
			case "already-R":
				store.pinMutationGate.RLock()
				store.mu.Lock()
				stage, err := store.stageRelationRecordLocked(slot.observed, slot.verified)
				slot.observed = pinOwnerObservation{owner: store, identity: id, kind: pinOwnerExisting, incarnation: stage.binding.incarnation}
				store.mu.Unlock()
				store.pinMutationGate.RUnlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			r.mu.Lock()
			slot.processing = true
			r.mu.Unlock()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if kind == "cancel-waitgate" {
				store.mu.Lock()
				beforeEntry, beforePresent := store.identities[id]
				beforeMetadata := beforeEntry.classMetadata()
				beforeChanges, beforeAllocations := store.changes, store.relationAllocations
				store.mu.Unlock()
				r.mu.Lock()
				beforeRate := r.recordAdmission.rate
				r.mu.Unlock()
				done := make(chan error, 1)
				workerDone := make(chan struct{})
				entered := make(chan relationGateWorkerEntry, 1)
				launched, joined, gateHeld := false, false, false
				var result error
				var joinDeadline time.Time
				releaseGate := func() {
					if gateHeld {
						store.pinMutationGate.Unlock()
						gateHeld = false
					}
				}
				joinWorker := func() bool {
					if joined || !launched {
						return true
					}
					if joinDeadline.IsZero() {
						joinDeadline = time.Now().Add(2 * time.Second)
					}
					timer := time.NewTimer(max(time.Duration(0), time.Until(joinDeadline)))
					defer timer.Stop()
					select {
					case <-workerDone:
						result, joined = <-done, true
						return true
					case <-timer.C:
						return false
					}
				}
				noEffects := func() bool {
					store.mu.Lock()
					afterEntry, afterPresent := store.identities[id]
					unchanged := beforePresent == afterPresent && beforeMetadata == afterEntry.classMetadata() && beforeEntry.incarnation == afterEntry.incarnation && beforeChanges == store.changes && beforeAllocations == store.relationAllocations
					store.mu.Unlock()
					r.mu.Lock()
					unchanged = unchanged && beforeRate == r.recordAdmission.rate
					r.mu.Unlock()
					return unchanged
				}
				store.pinMutationGate.Lock()
				gateHeld = true
				// Install before launch: every Fatal cancels, unlocks once, joins.
				t.Cleanup(func() {
					cancel()
					releaseGate()
					if !joinWorker() {
						t.Error("gate-wait cleanup: actual worker did not join")
						return
					}
					if launched {
						canceled, unchanged := errors.Is(result, context.Canceled), noEffects()
						gateReleased := store.pinMutationGate.TryLock()
						if gateReleased {
							store.pinMutationGate.Unlock()
						}
						t.Logf("gate-wait cleanup joined=%t canceled=%t noEffects=%t writerReacquired=%t", joined, canceled, unchanged, gateReleased)
						if !canceled || !unchanged || !gateReleased {
							t.Error("gate-wait cleanup did not retain cancellation/no-effects/released-gate invariants")
						}
					}
				})
				deadline := time.Now().Add(2 * time.Second)
				observation := time.NewTimer(time.Until(deadline))
				defer observation.Stop()
				launched = true
				go func() {
					defer close(workerDone)
					entered <- relationGateWorkerEntry{gid: relationGateCurrentGID(), store: r.svc.localContactPinStore()}
					_, err := r.stageRelationAdmission(ctx, slot)
					done <- err
				}()
				var entry relationGateWorkerEntry
				select {
				case entry = <-entered:
				case <-observation.C:
					t.Fatal("stage entry did not arrive within original gate-wait budget")
				}
				if entry.gid == 0 || entry.store != store {
					t.Fatal("stage target GID/store identity unavailable")
				}
				stacks := relationGateStackCapture{}
				for !stacks.removalWait(entry.gid, deadline) {
					select {
					case <-observation.C:
						t.Fatalf("stage never actually waited on removal gate: target=%d n=%d cap=%d complete=%t offset=%d exhausted=%t", entry.gid, stacks.n, len(stacks.buffer), stacks.complete, stacks.targetOffset, stacks.exhausted)
					case <-workerDone:
						t.Fatal("stage completed before exact removal-gate wait was observed")
					default:
						time.Sleep(time.Millisecond)
					}
				}
				cancel()
				releaseGate()
				if !joinWorker() {
					t.Fatal("gate-wait actual worker did not join after cancel/unlock")
				}
				if !errors.Is(result, context.Canceled) {
					t.Fatal("gate-wait cancel ignored", result)
				}
				if !noEffects() {
					t.Fatal("gate-wait cancellation changed class/owner/debit/store generation")
				}
			} else {
				disposition, err := r.stageRelationAdmission(ctx, slot)
				switch kind {
				case "K":
					if err != nil || disposition != relationAdmissionContactPath {
						t.Fatal("K was charged/staged R", err)
					}
				case "already-R":
					if err != nil || disposition != relationAdmissionStaged {
						t.Fatal("fresh existing R not staged", err)
					}
				default:
					if err == nil {
						t.Fatal("stale/full candidate staged")
					}
				}
			}
			want := 0
			if kind == "already-R" {
				want = 1
			}
			if r.recordAdmission.rate.count != want {
				t.Fatal("refused debit or fresh existingR event missing")
			}
		})
	}
}
func TestRecordAdmissionOwedUnknownReproofCOWAndStrictNinSnapshot(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	slot, _ := admissionQueuedFixture(t, r)
	r.mu.Lock()
	slot.processing = true
	r.mu.Unlock()
	if disposition, err := r.stageRelationAdmission(t.Context(), slot); err != nil || disposition != relationAdmissionStaged {
		t.Fatal(disposition, err)
	}
	store := r.svc.localContactPinStore()
	id := slot.owed.binding.identity
	unknown := NewMockidentityPinDurableFiles(t)
	unknown.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	injected := errors.New("unknown write")
	unknown.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, injected).Once()
	first, err := r.svc.completeOwedRelationRecord(t.Context(), slot.owed, slot.completion, unknown)
	if !errors.Is(err, injected) || first.attemptCovered || first.state != relationRecordStagedOwed || r.recordAdmission.rate.count != 1 {
		t.Fatal("unknown refunded/lost owed state", err)
	}
	store.mu.Lock()
	if err := store.recordPinEvidenceLocked(id, store.now()); err != nil {
		store.mu.Unlock()
		t.Fatal(err)
	}
	store.mu.Unlock()
	next := metadataSnapshot(t, store, id).metadata
	next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: store.now(), SourceGroup: domain.SourceGroupLoopback}
	stageMetadata(t, store, id, next)
	real, err := store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := real.Close(); err != nil {
			t.Error(err)
		}
	}()
	late, cancel := context.WithCancel(t.Context())
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		out, err := real.WriteMain(ctx, raw)
		cancel()
		return out, err
	}).Once()
	covered, err := r.svc.completeOwedRelationRecord(late, slot.owed, first, files)
	if !errors.Is(err, context.Canceled) || !covered.attemptCovered || covered.state != relationRecordCoveredOwed || covered.barrierAttempted {
		t.Fatal("late commit hid historical receipt/owed barrier", err)
	}
	done, err := r.svc.completeOwedRelationRecord(t.Context(), slot.owed, covered, real)
	if err != nil || done.state != relationRecordHistoricalCompleted || !done.barrierAttempted || r.recordAdmission.rate.count != 1 {
		t.Fatal("reproof/COW lost earned barrier or charged retry", err)
	}
	removeContactPinForTest(t, r.svc, id)
	store.mu.Lock()
	if err := store.recordPinEvidenceLocked(id, store.now()); err != nil {
		store.mu.Unlock()
		t.Fatal(err)
	}
	store.mu.Unlock()
	stale, err := r.svc.completeOwedRelationRecord(t.Context(), slot.owed, done, real)
	if !errors.Is(err, errPinMetadataSnapshotStale) || stale.state != relationRecordStaleOwner {
		t.Fatal("old ticket rebound recreated entry", err)
	}
}

func TestRecordAdmissionWorkerActuallyJoinsBlockedIOAndRetainsStoppedOwed(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	slot, _ := admissionQueuedFixture(t, r)
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, []byte) (secretfile.DurableCommit, error) {
		close(entered)
		<-release
		return secretfile.DurableIndeterminate, context.Canceled
	}).Once()
	files.EXPECT().Close().Return(nil).Once()
	r.recordAdmission.openFiles = func() (identityPinDurableFiles, error) { return files, nil }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	r.svc.goRunLoop(func() { defer close(done); r.runRecordAdmission(ctx) })
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		t.Fatal("joined worker never entered actual blocked backend")
	}
	// Slow filesystem work must not serialize ordinary resolver registration.
	if _, err := r.StartResolution(domain.PeerIdentity{212, 1}, identityIntentReason{Type: identityIntentReasonPresence}); err != nil {
		t.Fatal(err)
	}
	cancel()
	r.stop()
	select {
	case <-done:
		t.Fatal("cooperative cancel detached actual IO owner")
	default:
	}
	r.mu.Lock()
	processing, phase := slot.processing, slot.phase
	r.mu.Unlock()
	if !processing || phase != relationAdmissionEarnedOwed {
		t.Fatal("live IO slot erased during shutdown")
	}
	close(release)
	released = true
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("actual backend return did not join worker")
	}
	r.svc.WaitBackground()
	r.mu.Lock()
	defer r.mu.Unlock()
	if slot.processing || slot.phase != relationAdmissionStoppedOwed || slot.resolution != nil || slot.action.origin != identityLookupOriginNone || slot.protectionHeld || r.recordAdmission.rate.count != 1 {
		t.Fatal("stopped obligation hidden/replayable or rate refunded")
	}
	retained := 0
	for i := range r.recordAdmission.slots {
		if r.recordAdmission.slots[i].phase != relationAdmissionFree {
			retained++
		}
	}
	if retained != 1 {
		t.Fatal("stopped row inventory", retained)
	}
}
func TestRecordAdmissionStrictCoveragePreservesFormat3CapturedHighwater(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	store, c, real := ninKnownFixture(t)
	r.svc.secureSessions.store = store
	r.recordAdmission.openFiles = store.openIdentityPinFiles
	slot, _ := admissionQueuedFixture(t, r)
	r.mu.Lock()
	slot.processing = true
	r.mu.Unlock()
	if disposition, err := r.stageRelationAdmission(t.Context(), slot); err != nil || disposition != relationAdmissionStaged {
		t.Fatal(disposition, err)
	}
	store.mu.Lock()
	before := store.ninJournal.stamp()
	store.mu.Unlock()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		result, err := real.WriteMain(ctx, raw)
		c.advance(time.Second)
		store.mu.Lock()
		store.sampleNinQuotaLocked()
		store.mu.Unlock()
		return result, err
	}).Once()
	receipt, err := store.confirmRelationRecordCoverage(t.Context(), slot.owed, files)
	if err != nil || receipt.snapshotDigest == "" {
		t.Fatal(err)
	}
	store.mu.Lock()
	written, confirmed, current := store.ninJournal.written, store.ninJournal.confirmed, store.ninJournal.stamp()
	store.mu.Unlock()
	if written != before || confirmed != before || current.tick <= written.tick {
		t.Fatal("ACK recaptured post-IO current journal highwater")
	}
	restarted := ninOpenFixture(t, filepath.Join(store.dir, store.name), c)
	if !restarted.ninFormat3 || restarted.ninJournal == nil || restarted.ninJournal.stamp().domain != before.domain || restarted.ninJournal.stamp().generation != before.generation || restarted.ninJournal.stamp().tick != before.tick || restarted.relationAllocations != 1 || restarted.relationConfirmed != 0 {
		t.Fatal("strict R writer erased format3 journal or fabricated restart ACK")
	}
}
func TestRecordAdmissionStrictFailuresNeverBecomeSuccessOrRefund(t *testing.T) {
	for _, commit := range []secretfile.DurableCommit{secretfile.DurableNotCommitted, secretfile.DurableIndeterminate, secretfile.DurableCommitted} {
		t.Run(string(rune('a'+int(commit))), func(t *testing.T) {
			r, _ := admissionResolverFixture(t)
			slot, _ := admissionQueuedFixture(t, r)
			r.mu.Lock()
			slot.processing = true
			r.mu.Unlock()
			if _, err := r.stageRelationAdmission(t.Context(), slot); err != nil {
				t.Fatal(err)
			}
			files := NewMockidentityPinDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			injected := errors.New("write path failed")
			files.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(commit, injected).Once()
			result, err := r.svc.completeOwedRelationRecord(t.Context(), slot.owed, slot.completion, files)
			if !errors.Is(err, injected) || result.attemptCovered || result.barrierAttempted || result.receipt.snapshotDigest != "" || r.recordAdmission.rate.count != 1 {
				t.Fatal("ambiguous/failed write confirmed/refunded", err)
			}
		})
	}
}
func TestRecordAdmissionReproofDuringActualDrainKeepsEarnedObligation(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	slot, _ := admissionQueuedFixture(t, r)
	store := r.svc.localContactPinStore()
	id := slot.observed.identity
	r.mu.Lock()
	slot.processing = true
	r.mu.Unlock()
	if _, err := r.stageRelationAdmission(t.Context(), slot); err != nil {
		t.Fatal(err)
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	r.svc.unprovenRouting.enter(id)
	released := false
	defer func() {
		if !released {
			r.svc.unprovenRouting.leave(id)
		}
	}()
	type outcome struct {
		completion relationRecordCompletion
		err        error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := r.svc.completeOwedRelationRecord(t.Context(), slot.owed, slot.completion, files)
		done <- outcome{result, err}
	}()
	waitContactRecordCondition(t, "R completion never reached actual drain", func() bool {
		r.svc.unprovenRouting.mu.Lock()
		defer r.svc.unprovenRouting.mu.Unlock()
		return len(r.svc.unprovenRouting.drained[id]) > 0
	})
	// Actual proof/COW after covering commit must not suppress already-owed purge.
	store.mu.Lock()
	err = store.recordPinEvidenceLocked(id, store.now())
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	next := metadataSnapshot(t, store, id).metadata
	next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: store.now(), SourceGroup: domain.SourceGroupLoopback}
	stageMetadata(t, store, id, next)
	r.svc.unprovenRouting.leave(id)
	released = true
	result := <-done
	if result.err != nil || result.completion.state != relationRecordHistoricalCompleted || !result.completion.barrierAttempted {
		t.Fatal("reproof lost earned actual barrier", result.err)
	}
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	if entry.metadata.confirmedRevision == entry.protectionRevision {
		t.Fatal("historical cover ACKed later COW")
	}
}

func TestRecordAdmissionQualifiedFIFOAndExpiredOriginalNotRebased(t *testing.T) {
	r, c := admissionResolverFixture(t)
	_, labelA, entryA, requestA, answerA := admissionAnswerFixture(t, r)
	_, labelB, entryB, requestB, answerB := admissionAnswerFixture(t, r)
	acceptedB := lookupRealQueueOutcome(t, r, labelB, entryB, requestB)
	if !r.HandleAnswer(datagram.NewLabel(labelB), answerB) {
		t.Fatal("B answer lost")
	}
	r.mu.Lock()
	_, _, _, _, err := r.settleQueueLocked(labelB, entryB, acceptedB)
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	acceptedA := lookupRealQueueOutcome(t, r, labelA, entryA, requestA)
	if !r.HandleAnswer(datagram.NewLabel(labelA), answerA) {
		t.Fatal("A answer lost")
	}
	r.mu.Lock()
	_, _, _, _, err = r.settleQueueLocked(labelA, entryA, acceptedA)
	head := r.selectRelationSlotLocked()
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.label != labelB {
		t.Fatal("registration order replaced qualification FIFO")
	}
	// Correct qualification is not bound to the old60s window during residence.
	c.elapsed.Add(int64(2 * time.Minute))
	if err := r.processRelationSlot(t.Context(), head); err != nil {
		t.Fatal("valid residence beyond60 rejected", err)
	}
	r.mu.Lock()
	head = r.selectRelationSlotLocked()
	r.mu.Unlock()
	if head == nil || head.label != labelA {
		t.Fatal("FIFO tail lost")
	}
	c.elapsed.Add(int64(identityLookupTaskLifetime))
	if _, err := r.stageRelationAdmission(t.Context(), head); !errors.Is(err, errRelationAdmissionStale) {
		t.Fatal("queued residence rebased original7d", err)
	}
	if r.recordAdmission.rate.count != 1 {
		t.Fatal("expired original charged")
	}
}
func TestRecordAdmissionReseedNeverInventsObservedOriginalLifetime(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	target := domain.PeerIdentity{211, 1}
	if _, err := r.svc.StartUserIdentityLookup(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	r.stop()
	next := newIdentityResolverWithRecordAdmission(r.svc, r.intents, r.network)
	next.clock = r.clock // keep persisted wall origin in range; elapsed origin is still absent
	r.svc.identityResolver = next
	next.reseedFromIntents()
	next.mu.Lock()
	res := next.resolutions[target]
	next.mu.Unlock()
	if res == nil || res.recordLifetime != nil {
		t.Fatal("restart reconstructed elapsed creation")
	}
	if _, err := r.svc.StartUserIdentityLookup(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	next.mu.Lock()
	res = next.resolutions[target]
	next.mu.Unlock()
	if res.recordLifetime != nil {
		t.Fatal("genuine join rebased resumed original")
	}
}
func TestRecordAdmissionUnsupportedAndRetryPacingDoNotStage(t *testing.T) {
	r, _ := admissionResolverFixture(t)
	slot, _ := admissionQueuedFixture(t, r)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
	files.EXPECT().Close().Return(nil).Once()
	r.recordAdmission.openFiles = func() (identityPinDurableFiles, error) { return files, nil }
	r.mu.Lock()
	head := r.selectRelationSlotLocked()
	r.mu.Unlock()
	if head != slot {
		t.Fatal("head missing")
	}
	if err := r.processRelationSlot(t.Context(), slot); !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatal(err)
	}
	if slot.phase != relationAdmissionFree || r.recordAdmission.rate.count != 0 || r.svc.localContactPinStore().stats().RelationAllocations != 0 {
		t.Fatal("unsupported backend staged/charged")
	}
}

// GIDs are diagnostic test coordinates, never production identity/authority.
type relationGateWorkerEntry struct {
	gid   uint64
	store *secureSessionStore
}

func relationGateCurrentGID() uint64 {
	var header [512]byte
	n := runtime.Stack(header[:], false)
	fields := strings.Fields(string(header[:n]))
	if len(fields) < 2 || fields[0] != "goroutine" {
		return 0
	}
	gid, _ := strconv.ParseUint(fields[1], 10, 64)
	return gid
}

type relationGateStackCapture struct {
	buffer       []byte
	n            int
	complete     bool
	exhausted    bool
	targetOffset int
}

// Reuses only the latest buffer, grows at most seven sizes, and never treats a
// saturated prefix as a complete observation. Entry and polling share deadline.
func (c *relationGateStackCapture) removalWait(gid uint64, deadline time.Time) bool {
	c.complete, c.targetOffset = false, -1
	if c.exhausted || !time.Now().Before(deadline) {
		return false
	}
	if c.buffer == nil {
		c.buffer = make([]byte, 128<<10)
	}
	for {
		if !time.Now().Before(deadline) {
			return false
		}
		c.n = runtime.Stack(c.buffer, true)
		if c.n < len(c.buffer) {
			c.complete = true
			break
		}
		if len(c.buffer) == 8<<20 {
			c.exhausted = true
			return false
		}
		c.buffer = make([]byte, 2*len(c.buffer))
	}
	if !time.Now().Before(deadline) {
		return false
	}
	dump := string(c.buffer[:c.n])
	prefix := "goroutine " + strconv.FormatUint(gid, 10) + " ["
	for _, block := range strings.Split(dump, "\n\n") {
		if !strings.HasPrefix(block, prefix) {
			continue
		}
		c.targetOffset = strings.Index(dump, prefix)
		if !strings.Contains(block, "sync.(*RWMutex).RLock") {
			return false
		}
		lines := strings.Split(block, "\n")
		for i := 0; i+1 < len(lines); i++ {
			if strings.Contains(lines[i], "(*identityResolver).stageRelationAdmission(") {
				location := strings.Fields(lines[i+1])
				// Exact unchanged R3 production removal gate; evidence/trust
				// read locks at139/143 are intentionally not accepted.
				return len(location) > 0 && strings.HasSuffix(location[0], "/identity_record_admission_relation.go:137")
			}
		}
		return false
	}
	return false
}
