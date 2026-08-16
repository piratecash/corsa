package node

import (
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func newTestResolver(t *testing.T) (*identityResolver, *Service) {
	t.Helper()
	svc := newDatagramLayerService(t, true)
	store := loadIdentityIntentStore(filepath.Join(t.TempDir(), "intents.json"))
	resolver := newIdentityResolver(svc, store, testRecordStoreNetwork)
	return resolver, svc
}

// TestResolverInteractiveSchedule pins the phase-1 offsets 0/1/4/12/32s and
// the hand-off to the t=77s deadline after the last send.
func TestResolverInteractiveSchedule(t *testing.T) {
	t.Parallel()
	resolver, _ := newTestResolver(t)

	start := time.Unix(1780000000, 0)
	res := &identityResolution{dst: domaintest.ID("b"), createdAt: start, nextSendAt: start}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	for i := 1; i < len(identityLookupInteractiveAt); i++ {
		resolver.scheduleNextSendLocked(res, start.Add(identityLookupInteractiveAt[i-1]))
		want := start.Add(identityLookupInteractiveAt[i])
		if !res.nextSendAt.Equal(want) {
			t.Fatalf("send %d scheduled at %v, want %v", i, res.nextSendAt.Sub(start), identityLookupInteractiveAt[i])
		}
	}
	// After the fifth send the next event is the phase deadline.
	resolver.scheduleNextSendLocked(res, start.Add(identityLookupInteractiveAt[len(identityLookupInteractiveAt)-1]))
	if want := start.Add(identityLookupInteractiveDeadline); !res.nextSendAt.Equal(want) {
		t.Fatalf("post-final schedule = %v, want the t=77s deadline", res.nextSendAt.Sub(start))
	}
}

// TestResolverBackgroundLadder: 30s → 1m → 2m → 5m → 11m with the last
// value as the interval ceiling.
func TestResolverBackgroundLadder(t *testing.T) {
	t.Parallel()
	resolver, _ := newTestResolver(t)

	now := time.Unix(1780000000, 0)
	res := &identityResolution{dst: domaintest.ID("b"), phase: identityPhaseBackground}

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	expected := append(append([]time.Duration(nil), identityLookupBackgroundDelays...),
		identityLookupBackgroundDelays[len(identityLookupBackgroundDelays)-1],
		identityLookupBackgroundDelays[len(identityLookupBackgroundDelays)-1])
	for i, want := range expected {
		res.bgAttempts = i
		resolver.scheduleNextSendLocked(res, now)
		if got := res.nextSendAt.Sub(now); got != want {
			t.Fatalf("bg attempt %d scheduled after %v, want %v", i, got, want)
		}
	}
}

// TestResolverSingleFlightAndCooldown: one resolution per target however
// many reasons arrive, and a terminal arms a cooldown that defers the next
// ATTEMPT — never the operation itself, which the RPC contract needs to
// exist immediately with an id.
func TestResolverSingleFlightAndCooldown(t *testing.T) {
	t.Parallel()
	resolver, _ := newTestResolver(t)
	now := time.Unix(1780000000, 0)
	resolver.clock = func() time.Time { return now }
	target := domaintest.ID("b")

	first, err := resolver.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.ResolutionID == "" || first.Lifecycle != domain.IdentityResolutionPending {
		t.Fatalf("first state = %+v, want pending with an id", first)
	}
	joined, err := resolver.StartResolution(target, identityIntentReason{Type: identityIntentReasonPendingSend, ID: "m1"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if joined.ResolutionID != first.ResolutionID {
		t.Fatal("second reason opened a second operation — single-flight broken")
	}
	resolver.mu.Lock()
	if len(resolver.resolutions) != 1 {
		t.Fatalf("resolutions = %d, want single-flight 1", len(resolver.resolutions))
	}
	state, finished := resolver.finishLocked(target, domain.IdentityResolutionSucceeded)
	resolver.mu.Unlock()
	if !finished || state.Lifecycle != domain.IdentityResolutionSucceeded {
		t.Fatalf("finish state = %+v", state)
	}

	// Inside the cooldown a new reason opens a NEW operation (fresh id)
	// whose first attempt is deferred past the cooldown.
	reopened, err := resolver.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.ResolutionID == first.ResolutionID {
		t.Fatal("terminal resolution id reused")
	}
	resolver.mu.Lock()
	res, running := resolver.resolutions[target]
	deferred := res != nil && !res.nextSendAt.Before(now.Add(identityLookupCooldown))
	resolver.mu.Unlock()
	if !running {
		t.Fatal("resolution not opened during the cooldown")
	}
	if !deferred {
		t.Fatal("cooldown did not defer the first attempt")
	}
}

// TestResolverCancelReasonRefcount: removing one reason keeps the operation
// while others live; removing the last cancels it.
func TestResolverCancelReasonRefcount(t *testing.T) {
	t.Parallel()
	resolver, _ := newTestResolver(t)
	target := domaintest.ID("b")
	chat := identityIntentReason{Type: identityIntentReasonUIChat}
	recovery := identityIntentReason{Type: identityIntentReasonRecovery, ID: "m1"}

	if _, err := resolver.StartResolution(target, chat); err != nil {
		t.Fatalf("start chat reason: %v", err)
	}
	if _, err := resolver.StartResolution(target, recovery); err != nil {
		t.Fatalf("start recovery reason: %v", err)
	}

	resolver.CancelReason(target, chat)
	resolver.mu.Lock()
	_, running := resolver.resolutions[target]
	resolver.mu.Unlock()
	if !running {
		t.Fatal("resolution cancelled while a reason is still alive")
	}

	resolver.CancelReason(target, recovery)
	resolver.mu.Lock()
	_, running = resolver.resolutions[target]
	resolver.mu.Unlock()
	if running {
		t.Fatal("resolution survived the loss of its last reason")
	}
}

// registerLookupAttempt plants one live attempt the way sendAttempt does.
func registerLookupAttempt(resolver *identityResolver, target domain.PeerIdentity, label domain.PeerIdentity, request []byte, minSeq domain.IdentityRecordSeq) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.resolutions[target] = &identityResolution{id: "test-resolution-" + target.String(), dst: target, createdAt: resolver.clock(), openAttempts: 1}
	resolver.attemptGen++
	resolver.attempts[label] = identityAttemptEntry{
		dst:           target,
		gen:           resolver.attemptGen,
		qHash:         sha256.Sum256(request),
		proofRequired: true,
		minSeq:        minSeq,
		sentAt:        resolver.clock(),
	}
}

// TestResolverHandleAnswer covers the initiator-side verdicts of §4.2/§4.3:
// a valid proven answer terminates the resolution and imports the record; a
// missing or foreign proof, a wrong record and an unsatisfied min_seq leave
// the resolution active; an answer without a live attempt is ignored.
func TestResolverHandleAnswer(t *testing.T) {
	t.Parallel()

	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	target := domain.PeerIdentityFromWire(owner.Address)
	record, _ := issueTestRecord(t, owner, 3, true)
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: 1, TargetProof: true})
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	answerPayload := func(t *testing.T, label domain.PeerIdentity, withProof bool) []byte {
		t.Helper()
		answer := protocol.PostIdentityPayload{V: 1, Record: record}
		if withProof {
			answer.TargetProof = protocol.SignTargetProof(owner, testRecordStoreNetwork, label, request, record)
		}
		raw, err := protocol.BuildPostIdentityPayload(answer)
		if err != nil {
			t.Fatalf("build answer: %v", err)
		}
		return raw
	}

	t.Run("valid proven answer succeeds and imports", func(t *testing.T) {
		resolver, svc := newTestResolver(t)
		label := domaintest.ID("label-1")
		registerLookupAttempt(resolver, target, label, request, 0)
		resolver.mu.Lock()
		attemptGen := resolver.attempts[label].gen
		resolutionID := resolver.resolutions[target].id
		resolver.mu.Unlock()

		if !resolver.HandleAnswer(datagram.NewLabel(label), answerPayload(t, label, true)) {
			t.Fatal("valid answer not consumed")
		}
		if _, _, ok := svc.trust.recordFor(testRecordStoreNetwork, target); !ok {
			t.Fatal("record not imported")
		}
		resolver.mu.Lock()
		_, running := resolver.resolutions[target]
		resolver.mu.Unlock()
		if running {
			t.Fatal("resolution not terminated by a successful merge")
		}
		// The terminal state names WHICH attempt's answer proved the record
		// — the recovery gate's question-freshness anchor.
		state, ok := resolver.StateByID(resolutionID)
		if !ok || state.AnswerAttemptGen != attemptGen {
			t.Fatalf("terminal state attempt gen = %d ok=%v, want %d", state.AnswerAttemptGen, ok, attemptGen)
		}
	})

	t.Run("missing proof keeps the resolution active", func(t *testing.T) {
		resolver, svc := newTestResolver(t)
		label := domaintest.ID("label-2")
		registerLookupAttempt(resolver, target, label, request, 0)

		if resolver.HandleAnswer(datagram.NewLabel(label), answerPayload(t, label, false)) {
			t.Fatal("answer without a demanded proof was consumed")
		}
		if _, _, ok := svc.trust.recordFor(testRecordStoreNetwork, target); ok {
			t.Fatal("unproven record imported")
		}
	})

	t.Run("proof bound to a different attempt is refused", func(t *testing.T) {
		resolver, _ := newTestResolver(t)
		label := domaintest.ID("label-3")
		foreign := domaintest.ID("label-4")
		registerLookupAttempt(resolver, target, label, request, 0)

		if resolver.HandleAnswer(datagram.NewLabel(label), answerPayload(t, foreign, true)) {
			t.Fatal("replayed proof accepted — the attempt binding is broken")
		}
	})

	t.Run("record below min_seq is not terminal", func(t *testing.T) {
		resolver, _ := newTestResolver(t)
		label := domaintest.ID("label-5")
		registerLookupAttempt(resolver, target, label, request, 10)

		if resolver.HandleAnswer(datagram.NewLabel(label), answerPayload(t, label, true)) {
			t.Fatal("record below min_seq was consumed as satisfying")
		}
	})

	t.Run("answer without a live attempt is ignored", func(t *testing.T) {
		resolver, _ := newTestResolver(t)
		label := domaintest.ID("label-6")
		if resolver.HandleAnswer(datagram.NewLabel(label), answerPayload(t, label, true)) {
			t.Fatal("answer with no attempt consumed")
		}
	})

	t.Run("record for the wrong identity is refused", func(t *testing.T) {
		resolver, _ := newTestResolver(t)
		other := domaintest.ID("someone-else")
		label := domaintest.ID("label-7")
		registerLookupAttempt(resolver, other, label, request, 0)

		if resolver.HandleAnswer(datagram.NewLabel(label), answerPayload(t, label, true)) {
			t.Fatal("record for a different identity accepted")
		}
	})
}

// TestResolverReseedsFromIntents: durable rows survive the restart into
// background-phase resolutions; rows past the task lifetime are dropped.
func TestResolverReseedsFromIntents(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	path := filepath.Join(t.TempDir(), "intents.json")
	now := time.Unix(1780000000, 0)

	seedStore := loadIdentityIntentStore(path)
	fresh := domaintest.ID("fresh")
	expired := domaintest.ID("expired")
	if _, err := seedStore.add(fresh, identityIntentReason{Type: identityIntentReasonUIChat}, now.Add(-time.Hour)); err != nil {
		t.Fatalf("add fresh: %v", err)
	}
	if err := seedStore.recordAttempt(fresh); err != nil {
		t.Fatalf("record attempt: %v", err)
	}
	if _, err := seedStore.add(expired, identityIntentReason{Type: identityIntentReasonUIChat}, now.Add(-identityLookupTaskLifetime-time.Hour)); err != nil {
		t.Fatalf("add expired: %v", err)
	}

	resolver := newIdentityResolver(svc, loadIdentityIntentStore(path), testRecordStoreNetwork)
	resolver.clock = func() time.Time { return now }
	resolver.reseedFromIntents()

	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	res, ok := resolver.resolutions[fresh]
	if !ok {
		t.Fatal("fresh intent not reseeded")
	}
	if res.phase != identityPhaseBackground || res.bgAttempts != 1 {
		t.Fatalf("reseeded phase/attempts = %v/%d, want background/1", res.phase, res.bgAttempts)
	}
	if _, ok := resolver.resolutions[expired]; ok {
		t.Fatal("an intent past the task lifetime was reseeded")
	}
}

// TestIdentityIntentStoreRefcountAndPersistence pins the durable refcount
// semantics across a reload.
func TestIdentityIntentStoreRefcountAndPersistence(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "intents.json")
	store := loadIdentityIntentStore(path)
	target := domaintest.ID("t")
	now := time.Unix(1780000000, 0)
	chat := identityIntentReason{Type: identityIntentReasonUIChat}
	recovery := identityIntentReason{Type: identityIntentReasonRecovery, ID: "m1"}

	if added, err := store.add(target, chat, now); err != nil || !added {
		t.Fatalf("add chat: added=%v err=%v", added, err)
	}
	if added, err := store.add(target, chat, now); err != nil || added {
		t.Fatalf("re-add chat: added=%v err=%v, want idempotent", added, err)
	}
	if added, err := store.add(target, recovery, now); err != nil || !added {
		t.Fatalf("add recovery: added=%v err=%v", added, err)
	}

	reloaded := loadIdentityIntentStore(path)
	if remaining, err := reloaded.remove(target, chat); err != nil || remaining != 1 {
		t.Fatalf("remove chat after reload: remaining=%d err=%v, want 1", remaining, err)
	}
	if remaining, err := reloaded.remove(target, recovery); err != nil || remaining != 0 {
		t.Fatalf("remove recovery: remaining=%d err=%v, want 0", remaining, err)
	}
	if seeds := loadIdentityIntentStore(path).seeds(); len(seeds) != 0 {
		t.Fatalf("seeds after full removal = %v, want none", seeds)
	}
}

// TestResolveIdentityRPCContract pins the §4.9 local RPC pair: start
// returns the full axis state with a stable id immediately, the status
// poll answers by that id, and malformed input is a typed error frame.
func TestResolveIdentityRPCContract(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	target := domaintest.ID("rpc-target")

	started := svc.HandleLocalFrame(protocol.Frame{Type: "resolve_identity", Address: target.String()})
	if started.Type != "identity_resolution" || started.Resolution == nil {
		t.Fatalf("reply = %+v, want identity_resolution with a body", started)
	}
	if started.Resolution.ResolutionID == "" ||
		started.Resolution.Lifecycle != string(domain.IdentityResolutionPending) ||
		started.Resolution.Authority != string(domain.IdentityAuthorityNone) ||
		started.Resolution.DMAvailable != string(domain.DMAvailabilityUnknown) {
		t.Fatalf("started state = %+v", started.Resolution)
	}

	rejoined := svc.HandleLocalFrame(protocol.Frame{Type: "resolve_identity", Address: target.String()})
	if rejoined.Resolution == nil || rejoined.Resolution.ResolutionID != started.Resolution.ResolutionID {
		t.Fatal("a second resolve_identity opened a second operation")
	}

	status := svc.HandleLocalFrame(protocol.Frame{Type: "resolve_identity_status", ResolutionID: started.Resolution.ResolutionID})
	if status.Type != "identity_resolution" || status.Resolution == nil ||
		status.Resolution.ResolutionID != started.Resolution.ResolutionID {
		t.Fatalf("status reply = %+v", status)
	}

	if bad := svc.HandleLocalFrame(protocol.Frame{Type: "resolve_identity", Address: "not-hex"}); bad.Type != "error" {
		t.Fatalf("malformed address accepted: %+v", bad)
	}
	if unknown := svc.HandleLocalFrame(protocol.Frame{Type: "resolve_identity_status", ResolutionID: "ffffffffffffffff"}); unknown.Type != "error" {
		t.Fatalf("unknown resolution id answered: %+v", unknown)
	}
}

// TestResolverAxisEventsAndProvisionalImport: the state events carry the
// axes, and an external key import flips usable to provisional without
// terminating the operation.
func TestResolverAxisEventsAndProvisionalImport(t *testing.T) {
	t.Parallel()
	svc := newDatagramLayerService(t, true)
	bus := ebus.New()
	svc.eventBus = bus
	resolver := svc.identityResolver
	target := domaintest.ID("axis-target")

	events := make(chan ebus.IdentityResolutionState, 8)
	bus.Subscribe(ebus.TopicIdentityResolutionChanged, func(state ebus.IdentityResolutionState) {
		events <- state
	})

	started, err := resolver.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	select {
	case state := <-events:
		if state.ResolutionID != started.ResolutionID || state.Usable || state.Lifecycle != domain.IdentityResolutionPending {
			t.Fatalf("creation event = %+v", state)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no creation event")
	}

	// External provisional import: keys land in the knowledge maps and the
	// contact event fires.
	svc.addKnownPubKey(target.String(), "pk")
	svc.addKnownBoxKey(target.String(), "bk")
	resolver.noteProvisionalImport(target)

	select {
	case state := <-events:
		if !state.Usable || state.Authority != domain.IdentityAuthorityProvisional {
			t.Fatalf("provisional event = %+v, want usable+provisional", state)
		}
		if state.Lifecycle.Terminal() {
			t.Fatal("provisional import terminated the operation — it must keep digging for the authoritative record")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no provisional event")
	}

	if state, ok := resolver.StateByID(started.ResolutionID); !ok || !state.Usable {
		t.Fatalf("StateByID = %+v ok=%v, want the retained usable state", state, ok)
	}
}

// TestPersistedRecordsReseedKnowledgeOnRestart is the restart half of the
// verify-then-import contract: a record that survived on disk must leave
// the DM path able to encrypt after a restart, and a repeat lookup that
// merges as duplicate must refill the knowledge maps rather than declare
// success over empty ones.
func TestPersistedRecordsReseedKnowledgeOnRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	trustPath := filepath.Join(dir, "trust.json")
	selfID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate self: %v", err)
	}
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	cfg := config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    trustPath,
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}

	first := NewService(cfg, selfID, nil)
	t.Cleanup(first.WaitBackground)
	record, body := issueTestRecord(t, owner, 3, true)
	if _, err := first.importVerifiedIdentityRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatalf("import: %v", err)
	}
	if _, ok := first.knownBoxKey(owner.Address); !ok {
		t.Fatal("test setup: import did not fill the maps")
	}

	// The restart: a fresh Service over the same trust store.
	second := NewService(cfg, selfID, nil)
	t.Cleanup(second.WaitBackground)
	if key, ok := second.knownBoxKey(owner.Address); !ok || key == "" {
		t.Fatal("persisted record did not reseed the box key after restart")
	}
	if key, ok := second.knownPubKey(owner.Address); !ok || key == "" {
		t.Fatal("persisted record did not reseed the signing key after restart")
	}

	// The duplicate-merge path refills the maps when the LRU evicted them.
	second.knowledgeMu.Lock()
	delete(second.boxKeys, owner.Address)
	delete(second.pubKeys, owner.Address)
	delete(second.boxSigs, owner.Address)
	second.knowledgeMu.Unlock()
	outcome, err := second.importVerifiedIdentityRecord(testRecordStoreNetwork, record, body)
	if err != nil || outcome != domain.IdentityRecordMergeDuplicate {
		t.Fatalf("re-import outcome=%v err=%v, want duplicate", outcome, err)
	}
	if _, ok := second.knownBoxKey(owner.Address); !ok {
		t.Fatal("duplicate merge left the knowledge maps empty — usable would lie")
	}
}

// TestImportKeylessRecordRevokesBoxKey: an authoritative dm:false record
// is a revocation — the previously known box key must leave the live maps
// or the direct-send paths keep encrypting to it against the opt-out.
func TestImportKeylessRecordRevokesBoxKey(t *testing.T) {
	t.Parallel()
	selfID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate self: %v", err)
	}
	owner, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}
	cfg := config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(t.TempDir(), "trust.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}
	svc := NewService(cfg, selfID, nil)
	t.Cleanup(svc.WaitBackground)

	withDM, withDMBody := issueTestRecord(t, owner, 3, true)
	if _, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, withDM, withDMBody); err != nil {
		t.Fatalf("import dm record: %v", err)
	}
	if _, ok := svc.knownBoxKey(owner.Address); !ok {
		t.Fatal("test setup: dm record did not fill the box key")
	}

	keyless, keylessBody := issueTestRecord(t, owner, 4, false)
	outcome, err := svc.importVerifiedIdentityRecord(testRecordStoreNetwork, keyless, keylessBody)
	if err != nil || outcome != domain.IdentityRecordMergeReplaced {
		t.Fatalf("keyless import outcome=%v err=%v, want replaced", outcome, err)
	}
	if _, ok := svc.knownBoxKey(owner.Address); ok {
		t.Fatal("the revoked box key survived the authoritative dm:false record")
	}
	if _, ok := svc.knownPubKey(owner.Address); !ok {
		t.Fatal("the signing key must survive a dm opt-out")
	}
}

// TestNotifyIdentityKeysImportedFlipsUsable: the synchronous post-import
// hook must flip a running resolution's usable axis immediately — the ebus
// events are asynchronous and silent for already-known identities.
func TestNotifyIdentityKeysImportedFlipsUsable(t *testing.T) {
	t.Parallel()
	selfID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate self: %v", err)
	}
	cfg := config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(t.TempDir(), "trust.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}
	svc := NewService(cfg, selfID, nil)
	t.Cleanup(svc.WaitBackground)

	target := domaintest.ID("keyless-target")
	state, err := svc.identityResolver.StartResolution(target, identityIntentReason{Type: identityIntentReasonUIChat})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if state.Usable {
		t.Fatal("test setup: the target must start keyless")
	}

	svc.addKnownBoxKey(target.String(), "provisional-box-key")
	svc.notifyIdentityKeysImported(target.String())

	flipped, ok := svc.identityResolver.StateByID(state.ResolutionID)
	if !ok || !flipped.Usable {
		t.Fatalf("usable did not flip synchronously: %+v ok=%v", flipped, ok)
	}
	if flipped.Authority != domain.IdentityAuthorityProvisional {
		t.Fatalf("authority = %s, want provisional", flipped.Authority)
	}
}
