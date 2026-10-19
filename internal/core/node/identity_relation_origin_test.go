package node

import (
	"context"
	"crypto/sha256"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestLookupExpiredAttemptCannotChargeReplacement(t *testing.T) {
	r, _ := newTestResolver(t)
	now := time.Unix(1780000000, 0)
	r.clock = func() time.Time { return now }
	target, label := domaintest.ID("expiry-target"), domaintest.ID("expiry-label")
	registerLookupAttempt(r, target, label, nil, 0)
	replacement := &identityResolution{id: "replacement", dst: target, openAttempts: 2}
	r.mu.Lock()
	r.resolutions[target] = replacement
	r.mu.Unlock()
	r.expireAttemptsLocked(now.Add(identityLookupAttemptWindow + time.Second))
	if replacement.openAttempts != 2 {
		t.Fatalf("expired A decremented replacement B occupancy: %d", replacement.openAttempts)
	}
}

func TestLookupAnswerImportCannotFinishReplacement(t *testing.T) {
	r, svc := newTestResolver(t)
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	target := domain.PeerIdentityFromWire(owner.Address)
	label := domaintest.ID("import-label")
	record, _ := issueTestRecord(t, owner, 3, true)
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: 1, TargetProof: true})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protocol.BuildPostIdentityPayload(protocol.PostIdentityPayload{V: 1, Record: record, TargetProof: protocol.SignTargetProof(owner, testRecordStoreNetwork, label, request, record)})
	if err != nil {
		t.Fatal(err)
	}
	registerLookupAttempt(r, target, label, request, 0)
	svc.trust.mu.Lock()
	svc.trust.contacts[target.String()] = trustedContact{Address: target.String()}
	svc.trust.mu.Unlock()
	svc.trust.saveMu.Lock()
	released := false
	defer func() {
		if !released {
			svc.trust.saveMu.Unlock()
		}
	}()
	done := make(chan bool, 1)
	go func() { done <- r.HandleAnswer(datagram.NewLabel(label), payload) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		svc.trust.mu.RLock()
		_, merged := svc.trust.records[trustRecordKey{network: testRecordStoreNetwork.String(), address: target.String()}]
		svc.trust.mu.RUnlock()
		if merged {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("answer never reached blocked real trust persistence")
		case <-tick.C:
		}
	}
	r.mu.Lock()
	old := r.resolutions[target]
	replacement := &identityResolution{id: "replacement", dst: target, openAttempts: 2, nextSendAt: r.clock().Add(time.Hour)}
	r.resolutions[target] = replacement
	r.mu.Unlock()
	svc.trust.saveMu.Unlock()
	released = true
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("answer did not complete")
	}
	r.mu.Lock()
	current := r.resolutions[target]
	r.mu.Unlock()
	if current != replacement || replacement.answerAttemptGen != 0 || replacement.openAttempts != 2 {
		t.Fatalf("old imported answer finished/mutated replacement B (old=%s current=%p want=%p)", old.id, current, replacement)
	}
}

func TestLookupSendSlotWaitHonorsCancellation(t *testing.T) {
	r, _ := newTestResolver(t)
	target := domaintest.ID("slot-target")
	if _, err := r.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat}); err != nil {
		t.Fatal(err)
	}
	for range identityLookupSendSlots {
		r.sendSlots <- struct{}{}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	go func() { r.sendAttempt(ctx, target, r.clock()); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		for range identityLookupSendSlots {
			<-r.sendSlots
		}
		<-done
		t.Fatal("cancelled lookup remained blocked on global send slot")
	}
	for range identityLookupSendSlots {
		<-r.sendSlots
	}
}

// lookupProvenAnswer registers the same owner/action values as the real send
// boundary and returns a real label-bound signed answer, never pin metadata.
func lookupProvenAnswer(t *testing.T, r *identityResolver, genuine bool) (UserIdentityLookup, domain.PeerIdentity, identityAttemptEntry, []byte, []byte) {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	target := domain.PeerIdentityFromWire(owner.Address)
	r.svc.identityResolver = r
	var action UserIdentityLookup
	if genuine {
		action, err = r.svc.StartUserIdentityLookup(t.Context(), target)
	} else {
		_, err = r.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat})
	}
	if err != nil {
		t.Fatal(err)
	}
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: 1, TargetProof: true})
	if err != nil {
		t.Fatal(err)
	}
	record, err := protocol.BuildSignedIdentityRecord(owner, protocol.IdentityRecordSpec{Network: testRecordStoreNetwork, DM: true, DTypes: domain.ExplicitDTypes([]domain.DType{domain.DTypeGetIdentity}), MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure), IssuedAt: 1780000000, Seq: 3})
	if err != nil {
		t.Fatal(err)
	}
	label := domaintest.ID("proven-" + owner.Address)
	answer, err := protocol.BuildPostIdentityPayload(protocol.PostIdentityPayload{V: 1, Record: record, TargetProof: protocol.SignTargetProof(owner, testRecordStoreNetwork, label, request, record)})
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	res := r.resolutions[target]
	r.attemptGen++
	entry := identityAttemptEntry{owner: res, dst: target, gen: r.attemptGen, qHash: sha256.Sum256(request), proofRequired: true, sentAt: r.clock(), action: r.actionForLocked(target), queue: identityAttemptQueuePending}
	r.attempts[label] = entry
	res.openAttempts++
	r.mu.Unlock()
	return action, label, entry, request, answer
}

func lookupRealQueueOutcome(t *testing.T, r *identityResolver, label domain.PeerIdentity, entry identityAttemptEntry, request []byte) datagram.SendOutcome {
	t.Helper()
	declarations := datagramPeerDeclarations()
	declarations.DeclaredDTypes = domain.ExplicitDTypes([]domain.DType{domain.DTypeGetIdentity})
	installDatagramPeer(t, r.svc, entry.dst, datagramPeerConn{version: datagramTestPeerVersion, connectedAt: time.Now().UTC(), declared: &declarations})
	outcome := r.svc.datagramLayer().pipeline.SendLocal(t.Context(), datagram.LocalSenderIdentityResolver, datagram.LocalSendOpts{Frame: protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRequest, Class: domain.DatagramClassControl, Src: label, Dst: entry.dst, TTL: domain.DatagramDefaultMaxHops, RoutePolicy: domain.RoutePolicyBest, DType: domain.DTypeGetIdentity, Payload: request}})
	if outcome.Kind() != datagram.SendQueued {
		t.Fatalf("real class queue did not admit fixture: %s %v", outcome.Kind(), outcome.Err())
	}
	return outcome
}

func TestLookupFastCompletionKeepsOnlyPendingExactClaim(t *testing.T) {
	for _, kind := range []string{"accepted", "refused", "cancelled", "expired", "shutdown", "duplicate", "replacement"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := newTestResolver(t)
			now := time.Now().UTC()
			r.clock = func() time.Time { return now }
			action, label, entry, request, answer := lookupProvenAnswer(t, r, true)
			accepted := lookupRealQueueOutcome(t, r, label, entry, request)
			if !r.HandleAnswer(datagram.NewLabel(label), answer) {
				t.Fatal("normal fast verified answer not imported")
			}
			r.mu.Lock()
			_, running := r.resolutions[entry.dst]
			claim, exists := r.attempts[label]
			r.mu.Unlock()
			if running || !exists || claim.claim == nil || claim.queue != identityAttemptQueuePending {
				t.Fatal("normal success delayed or pending claim lost")
			}
			if kind == "duplicate" && r.HandleAnswer(datagram.NewLabel(label), answer) {
				t.Fatal("duplicate answer claimed a second completion")
			}
			var replacement *identityResolution
			switch kind {
			case "cancelled":
				if err := r.svc.CancelUserIdentityLookup(t.Context(), action); err != nil {
					t.Fatal(err)
				}
			case "expired":
				now = now.Add(identityLookupAttemptWindow + time.Second)
				r.expireAttemptsLocked(now)
			case "shutdown":
				r.stop()
			case "replacement":
				if _, err := r.svc.StartUserIdentityLookup(t.Context(), entry.dst); err != nil {
					t.Fatal(err)
				}
				r.mu.Lock()
				replacement = r.resolutions[entry.dst]
				replacement.openAttempts = 2
				r.mu.Unlock()
			}
			outcome := accepted
			if kind == "refused" {
				outcome = datagram.SendOutcome{}
			}
			r.mu.Lock()
			completion, _, snapshot, _, _ := r.settleQueueLocked(label, entry, outcome)
			_, still := r.attempts[label]
			r.mu.Unlock()
			if snapshot != nil || still {
				t.Fatal("completed claim resurrected/charged durable replacement intent")
			}
			want := kind == "accepted" || kind == "duplicate" || kind == "replacement"
			if (completion != nil) != want {
				t.Fatalf("soft completion=%v want=%v", completion != nil, want)
			}
			if completion != nil && (completion.action.generation != action.generation || completion.attemptGeneration != entry.gen || completion.target != entry.dst) {
				t.Fatal("wrong completion owner")
			}
			r.mu.Lock()
			again, _, _, _, _ := r.settleQueueLocked(label, entry, accepted)
			r.mu.Unlock()
			if again != nil {
				t.Fatal("completion consumed twice")
			}
			if replacement != nil && (replacement.openAttempts != 2 || replacement.sentAnything || replacement.bgAttempts != 0) {
				t.Fatal("old pending queue claim mutated replacement")
			}
		})
	}
}

func TestLookupGenuineActionIdempotentAndCancelABA(t *testing.T) {
	r, svc := newTestResolver(t)
	svc.identityResolver = r
	target := domaintest.ID("action-aba")
	if _, err := r.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat}); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	generic := r.actionForLocked(target)
	r.mu.Unlock()
	if generic.origin != identityLookupOriginNone {
		t.Fatal("generic UIChat minted Window authority")
	}
	a, err := svc.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := svc.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if a.generation == 0 || repeated.generation != a.generation || repeated.ResolutionID() != a.ResolutionID() {
		t.Fatal("repeat genuine start revoked singleton generation")
	}
	if err := svc.CancelUserIdentityLookup(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	b, err := svc.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if b.generation == a.generation {
		t.Fatal("cancel/add reused action generation")
	}
	if err := svc.CancelUserIdentityLookup(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	current := r.actionForLocked(target)
	res := r.resolutions[target]
	r.mu.Unlock()
	if current.generation != b.generation || res == nil {
		t.Fatal("late A cancellation revoked B")
	}
	reloaded := loadIdentityIntentStore(r.intents.path)
	reloaded.mu.Lock()
	defer reloaded.mu.Unlock()
	for _, row := range reloaded.rows {
		if row.action.origin != identityLookupOriginNone {
			t.Fatal("format1/restart synthesized trusted action")
		}
	}
}

func TestLookupGenuineJoinCannotBlessRegisteredGenericAttempt(t *testing.T) {
	r, svc := newTestResolver(t)
	_, label, entry, request, answer := lookupProvenAnswer(t, r, false)
	if _, err := svc.StartUserIdentityLookup(t.Context(), entry.dst); err != nil {
		t.Fatal(err)
	}
	accepted := lookupRealQueueOutcome(t, r, label, entry, request)
	if !r.HandleAnswer(datagram.NewLabel(label), answer) {
		t.Fatal("valid generic answer lost normal success")
	}
	r.mu.Lock()
	completion, _, _, _, _ := r.settleQueueLocked(label, entry, accepted)
	remaining := len(r.attempts)
	r.mu.Unlock()
	if completion != nil || remaining != 0 {
		t.Fatal("later genuine join retroactively blessed old generic attempt")
	}
}

func TestLookupSlotWaitCannotRegisterReplacedOwner(t *testing.T) {
	r, _ := newTestResolver(t)
	target := domaintest.ID("slot-replacement")
	reason := identityIntentReason{Type: identityIntentReasonRecovery, ID: "A"}
	if _, err := r.StartResolution(target, reason); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	a := r.resolutions[target]
	r.mu.Unlock()
	for range identityLookupSendSlots {
		r.sendSlots <- struct{}{}
	}
	done := make(chan struct{})
	go func() { r.sendOwnedAttempt(t.Context(), a, r.clock()); close(done) }()
	r.CancelReason(target, reason)
	if _, err := r.StartResolution(target, identityIntentReason{Type: identityIntentReasonRecovery, ID: "B"}); err != nil {
		t.Fatal(err)
	}
	<-r.sendSlots
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old slot waiter did not finish")
	}
	for range identityLookupSendSlots - 1 {
		<-r.sendSlots
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.resolutions[target]
	if b == a || b.openAttempts != 0 || b.bgAttempts != 0 || len(r.attempts) != 0 {
		t.Fatal("old registration crossed resolution replacement")
	}
}

func TestLookupPersistenceNeverHoldsResolverOrIntentMutex(t *testing.T) {
	for _, kind := range []string{"start", "background", "finish"} {
		t.Run(kind, func(t *testing.T) {
			r, svc := newTestResolver(t)
			svc.identityResolver = r
			var label domain.PeerIdentity
			var entry identityAttemptEntry
			var request, answer []byte
			if kind != "start" {
				_, label, entry, request, answer = lookupProvenAnswer(t, r, true)
			}
			if kind == "background" {
				// Install only the real queue's peer; sendOwnedAttempt mints its own
				// actual label and reaches ordinary background recordAttempt persistence.
				lookupRealQueueOutcome(t, r, label, entry, request)
				r.mu.Lock()
				delete(r.attempts, label)
				entry.owner.openAttempts = 0
				entry.owner.phase = identityPhaseBackground
				r.mu.Unlock()
			}
			if kind == "finish" {
				r.mu.Lock()
				current := r.attempts[label]
				current.queue = identityAttemptQueueAccepted
				r.attempts[label] = current
				r.mu.Unlock()
			}
			r.intents.saveMu.Lock()
			released := false
			defer func() {
				if !released {
					r.intents.saveMu.Unlock()
				}
			}()
			done := make(chan struct{})
			target := entry.dst
			if kind == "start" {
				target = domaintest.ID("persist-start")
			}
			go func() {
				switch kind {
				case "start":
					_, _ = svc.StartUserIdentityLookup(t.Context(), target)
				case "background":
					r.sendOwnedAttempt(t.Context(), entry.owner, r.clock())
				case "finish":
					r.HandleAnswer(datagram.NewLabel(label), answer)
				}
				close(done)
			}()
			// Poll only the real in-memory intent mutation. The disk call cannot
			// finish while saveMu is held, so both ownership mutexes must be free.
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			ready := false
			for !ready {
				if r.mu.TryLock() {
					if r.intents.mu.TryLock() {
						row, exists := r.intents.rows[identityIntentKey{target: target, reason: identityIntentReason{Type: identityIntentReasonUIChat}}]
						switch kind {
						case "start":
							ready = exists && r.resolutions[target] != nil && row.action.origin == identityLookupOriginWindowUser
						case "background":
							ready = exists && row.Attempts == 1 && entry.owner.bgAttempts == 1
						case "finish":
							ready = !exists && r.resolutions[target] == nil
						}
						r.intents.mu.Unlock()
					}
					r.mu.Unlock()
				}
				if ready {
					break
				}
				select {
				case <-deadline.C:
					t.Fatal("persistence blocked an owner mutex or mutation never reached snapshot boundary")
				case <-tick.C:
				}
			}
			if len(r.sendSlots) != 0 {
				t.Fatal("persistence retained an enqueue-only send slot")
			}
			select {
			case <-done:
				t.Fatal("write bypassed the real blocked persistence primitive")
			default:
			}
			r.intents.saveMu.Unlock()
			released = true
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("persistence did not finish after release")
			}
		})
	}
}

func TestLookupSnapshotsCannotOverwriteReplacementOrPersistOrigin(t *testing.T) {
	r, svc := newTestResolver(t)
	svc.identityResolver = r
	target := domaintest.ID("snapshot-replacement")
	a, err := svc.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	old, err := r.intents.recordAttemptMemory(target)
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CancelUserIdentityLookup(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	b, err := svc.StartUserIdentityLookup(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.intents.persist(old); err != nil {
		t.Fatal(err)
	}
	restored := loadIdentityIntentStore(r.intents.path)
	seeds := restored.seeds()
	if len(seeds) != 1 || seeds[0].Attempts != 0 {
		t.Fatal("out-of-order A snapshot overwrote new B intents")
	}
	r.mu.Lock()
	current := r.actionForLocked(target)
	r.mu.Unlock()
	if current.generation != b.generation {
		t.Fatal("old snapshot changed runtime B action")
	}
	for _, row := range old.rows {
		if row.action.origin != identityLookupOriginNone {
			t.Fatal("immutable durable snapshot retains runtime origin")
		}
	}
}

func TestLookupGenerationExhaustionDoesNotWrapOrMintAction(t *testing.T) {
	for _, kind := range []string{"action", "attempt", "snapshot"} {
		t.Run(kind, func(t *testing.T) {
			r, svc := newTestResolver(t)
			svc.identityResolver = r
			target := domaintest.ID("exhaust-" + kind)
			switch kind {
			case "action":
				r.actionGen = identityLookupActionGeneration(math.MaxUint64)
			case "snapshot":
				r.intents.snapshotGen = math.MaxUint64
			case "attempt":
				if _, err := svc.StartUserIdentityLookup(t.Context(), target); err != nil {
					t.Fatal(err)
				}
				r.attemptGen = math.MaxUint64
				r.sendAttempt(t.Context(), target, r.clock())
				r.mu.Lock()
				defer r.mu.Unlock()
				if r.attemptGen != math.MaxUint64 || len(r.attempts) != 0 || r.resolutions[target].openAttempts != 0 {
					t.Fatal("exhausted attempt generation wrapped or registered")
				}
				return
			}
			if _, err := svc.StartUserIdentityLookup(t.Context(), target); err == nil {
				t.Fatal("exhaustion accepted action")
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if len(r.resolutions) != 0 || len(r.intents.seeds()) != 0 {
				t.Fatal("exhaustion produced visible lookup authority")
			}
		})
	}
}

func TestLookupLateEnqueueCannotChargeReplacement(t *testing.T) {
	r, svc := newTestResolver(t)
	svc.identityResolver = r
	target := domaintest.ID("late-enqueue-target")
	declarations := datagramPeerDeclarations()
	declarations.DeclaredDTypes = domain.ExplicitDTypes([]domain.DType{domain.DTypeGetIdentity})
	installDatagramPeer(t, svc, target, datagramPeerConn{version: datagramTestPeerVersion, connectedAt: time.Now().UTC(), declared: &declarations})
	parts := svc.datagramLayer().datagramPlaneParts
	entered, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	var once sync.Once
	parts.clock = func() time.Time {
		if armed.Load() {
			once.Do(func() { close(entered); <-release })
		}
		return time.Now().UTC()
	}
	layer, err := assembleDatagramLayer(svc, parts)
	if err != nil {
		t.Fatal(err)
	}
	svc.datagramPlane.Store(layer)
	armed.Store(true)
	aReason := identityIntentReason{Type: identityIntentReasonRecovery, ID: "A"}
	if _, err := r.StartResolution(target, aReason); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { r.sendAttempt(t.Context(), target, r.clock()); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("real pipeline did not reach delayed enqueue clock")
	}
	r.CancelReason(target, aReason)
	if _, err := r.StartResolution(target, identityIntentReason{Type: identityIntentReasonRecovery, ID: "B"}); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	replacement := r.resolutions[target]
	replacement.phase = identityPhaseBackground
	expectedAt := replacement.nextSendAt
	r.mu.Unlock()
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("real enqueue did not finish")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.resolutions[target] != replacement || replacement.sentAnything || replacement.bgAttempts != 0 || replacement.openAttempts != 0 || !replacement.nextSendAt.Equal(expectedAt) {
		t.Fatal("late A real class-queue admission mutated B lifecycle/budget/schedule")
	}
}

func TestLookupGenericFramesCannotMintWindowOrigin(t *testing.T) {
	r, svc := newTestResolver(t)
	svc.identityResolver = r
	for _, reason := range []string{"", "ui_chat", "user_lookup", "pending_send", "recovery"} {
		target := domaintest.ID("frame-" + reason)
		reply := svc.HandleLocalFrame(protocol.Frame{Type: "resolve_identity", Address: target.String(), ResolutionReason: reason})
		if reply.Type == "error" || reply.Resolution == nil {
			t.Fatalf("generic compatibility lookup stopped working: %s", reply.Code)
		}
		r.mu.Lock()
		stamp := r.actionForLocked(target)
		r.mu.Unlock()
		if stamp.origin != identityLookupOriginNone {
			t.Fatal("generic Frame minted genuine Window action")
		}
	}
}

func TestLookupFreshExpiryAfterActualRecordImport(t *testing.T) {
	r, svc := newTestResolver(t)
	var clockNanos atomic.Int64
	clockNanos.Store(time.Now().UTC().UnixNano())
	r.clock = func() time.Time { return time.Unix(0, clockNanos.Load()).UTC() }
	_, label, entry, _, answer := lookupProvenAnswer(t, r, true)
	svc.trust.mu.Lock()
	svc.trust.contacts[entry.dst.String()] = trustedContact{Address: entry.dst.String()}
	svc.trust.mu.Unlock()
	svc.trust.saveMu.Lock()
	released := false
	defer func() {
		if !released {
			svc.trust.saveMu.Unlock()
		}
	}()
	done := make(chan bool, 1)
	go func() { done <- r.HandleAnswer(datagram.NewLabel(label), answer) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		svc.trust.mu.RLock()
		_, exists := svc.trust.records[trustRecordKey{network: testRecordStoreNetwork.String(), address: entry.dst.String()}]
		svc.trust.mu.RUnlock()
		if exists {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("import did not reach real persistence")
		case <-tick.C:
		}
	}
	clockNanos.Add(int64(identityLookupAttemptWindow + time.Second))
	svc.trust.saveMu.Unlock()
	released = true
	select {
	case consumed := <-done:
		if !consumed {
			t.Fatal("valid record import lost normal consumption")
		}
	case <-time.After(time.Second):
		t.Fatal("import did not finish")
	}
	r.mu.Lock()
	current := r.resolutions[entry.dst]
	claim := r.attempts[label].claim
	r.mu.Unlock()
	if current != entry.owner || current.answerAttemptGen != 0 || claim != nil {
		t.Fatal("pre-import timestamp authorized stale terminal/completion")
	}
	if _, ok := svc.knownBoxKey(entry.dst.String()); !ok {
		t.Fatal("expired action discarded ordinary verified key availability")
	}
}

func TestLookupExpectedCancelKeepsRecoveryAndRevokesOldAuthority(t *testing.T) {
	r, svc := newTestResolver(t)
	action, label, entry, request, answer := lookupProvenAnswer(t, r, true)
	if _, err := r.StartResolution(entry.dst, identityIntentReason{Type: identityIntentReasonRecovery, ID: "still-live"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.CancelUserIdentityLookup(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	before := r.attempts[label]
	stillRunning := r.resolutions[entry.dst] == entry.owner
	r.mu.Unlock()
	if !stillRunning || !before.revoked || before.action != entry.action {
		t.Fatal("cancel rewrote immutable origin or removed recovery's resolution")
	}
	accepted := lookupRealQueueOutcome(t, r, label, entry, request)
	if !r.HandleAnswer(datagram.NewLabel(label), answer) {
		t.Fatal("revoked Window action lost ordinary valid key resolution")
	}
	r.mu.Lock()
	completion, _, _, _, _ := r.settleQueueLocked(label, entry, accepted)
	r.mu.Unlock()
	if completion != nil {
		t.Fatal("recovery kept canceled Window soft authority alive")
	}
}

func TestLookupLatePublicationCannotReviveFinishedOwner(t *testing.T) {
	r, _ := newTestResolver(t)
	now := time.Now().UTC()
	r.clock = func() time.Time { return now }
	target := domaintest.ID("late-publication")
	pending, err := r.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat})
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	old := r.resolutions[target]
	terminal, finished, snapshot := r.finishLocked(old, domain.IdentityResolutionSucceeded, nil)
	r.mu.Unlock()
	if !finished {
		t.Fatal("fixture could not finish")
	}
	r.persistIntent(snapshot, target)
	r.emit(terminal, old)
	now = now.Add(identityLookupStateRetention + time.Second)
	if _, err := r.StartResolution(target, identityIntentReason{Type: identityIntentReasonRecovery, ID: "B"}); err != nil {
		t.Fatal(err)
	}
	r.emit(pending, old)
	if state, exists := r.StateByID(pending.ResolutionID); exists && !state.Lifecycle.Terminal() {
		t.Fatal("late A publication revived pending state after terminal retention expired")
	}
}
