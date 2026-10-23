package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

// Persist the original local fact and signed record, then close that seed owner.
// The candidate must reload an unconfirmed fact; no runtime ACK crosses owners.
func readyContactInputs(t *testing.T) (config.Node, *identity.Identity, *ninFixtureClock, *identity.Identity, protocol.SignedIdentityRecord) {
	t.Helper()
	cfg, id, clock := readyInboundInputs(t)
	seed := NewService(cfg, id, nil)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	addLocalContact(t, seed, peer)
	record := reconciliationRecord(t, seed, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	seed.runCancel()
	seed.closeBackgroundAdmission()
	seed.WaitBackground()
	return cfg, id, clock, peer, record
}

func readyContactBuild(t *testing.T) (*findNodeReadyConstruction, *Service, *ninFixtureClock, *identity.Identity, protocol.SignedIdentityRecord) {
	t.Helper()
	cfg, id, clock, peer, record := readyContactInputs(t)
	r := readyTestPrepare(t, cfg, id)
	s, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, findNodeReadyOptions{loadedInbound: true, contactReconciliation: true}, inboundPinClock{elapsed: clock.read})
	if err != nil || s == nil {
		t.Fatal("original K/N-in/Ready construction refused", err)
	}
	withoutOwnDials(nil)(s)
	s.routeWithdrawalGracePeriodTest = -1
	if !r.bindings(s) || !r.finalized || !r.checked || !r.bridge.statisticInstalled || s.inboundPins == nil || s.contactPins != r.originalContactProducer || s.identityResolver != r.originalContactResolver {
		t.Fatal("composed original owner tuple missing")
	}
	return r, s, clock, peer, record
}

// Gate only the original paced worker's first clock call. Actual Run alone
// opens K readiness; the test never sets readiness or stages a positive event.
func readyContactRun(t *testing.T, s *Service) (*runningTestService, context.CancelFunc) {
	t.Helper()
	allow := make(chan struct{})
	var first, once sync.Once
	release := func() { once.Do(func() { close(allow) }) }
	s.contactPins.clock = func() time.Time { first.Do(func() { <-allow }); return time.Now() }
	ctx, cancel := context.WithCancel(t.Context())
	running := runServiceForTest(ctx, s)
	t.Cleanup(func() { release(); cancel(); contactPushJoin(t, running.exited) })
	waitContactRecordCondition(t, "original Run did not open composed K", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kReady
	})
	release()
	return running, cancel
}

func TestOwnedReadyContactReconciliationLoadedInboundSameRun(t *testing.T) {
	_, s, clock, contact, record := readyContactBuild(t)
	kID := domain.PeerIdentityFromWire(contact.Address)
	fact := s.trust.trustedContacts()[contact.Address].LocalContact
	if fact == nil || fact.confirmed != 0 || fact.proof != nil || s.localContactPinStore().identityPinned(kID) {
		t.Fatal("reload fabricated ACK/proof/K")
	}
	originalFact := fact.fact
	store := s.localContactPinStore()
	store.mu.Lock()
	initialStamp := store.ninJournal.stamp()
	initialRows := store.ninJournal.rows
	initialRow := store.ninJournal.row(0)
	pending := !store.ninJournal.strictConfirmed()
	store.mu.Unlock()
	if !pending {
		t.Fatal("loaded N-in obligation falsely ACKed at construction")
	}
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if store.identityPinned(kID) || s.trust.localContactStatus(fact.stamp(s.trust)) == domain.LocalContactFactDurableConfirmed {
		t.Fatal("pre-Run K authority escaped")
	}
	running, cancel := readyContactRun(t, s)
	waitContactRecordCondition(t, "same original workers did not complete K and loaded N-in", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		e, ok := store.identities[kID]
		return ok && e.classMetadata().Contact.State == domain.IdentityPinAllocationEligible && e.metadata != nil && e.metadata.recordMirror == contactRecordOwnerCompleted && e.metadata.confirmedRevision == e.protectionRevision && store.ninJournal.strictConfirmed()
	})
	store.mu.Lock()
	kEntry := store.identities[kID]
	kAllocation, kOwner := kEntry.classMetadata().Contact, kEntry.metadata.recordOwner
	journalUnchanged := store.ninJournal.stamp().sameContent(initialStamp) && store.ninJournal.rows == initialRows && store.ninJournal.row(0) == initialRow
	store.mu.Unlock()
	if !journalUnchanged || kOwner == nil || s.trust.localContactStatus(fact.stamp(s.trust)) != domain.LocalContactFactDurableConfirmed || s.trust.trustedContacts()[contact.Address].LocalContact.fact != originalFact {
		t.Fatal("K changed original journal/fact or lacked real completion")
	}
	s.trust.mu.RLock()
	current := s.trust.records[trustRecordKey{network: networkName, address: contact.Address}]
	unchangedRecord := bytes.Equal(current.record.Body, record.Body) && bytes.Equal(current.record.Sig, record.Sig)
	s.trust.mu.RUnlock()
	if !unchangedRecord || s.hasContactRecordPushTransportCapability() || s.identityResolver.recordAdmission.admitsR() {
		t.Fatal("K reconciliation changed record or acquired push/R")
	}
	cfg, id := readyTestInputs(t)
	if id.Address == contact.Address {
		t.Fatal("N-in peer must differ from K contact")
	}
	remote, err := newFindNodeReadyCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	remote.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { remote.runCancel(); remote.closeBackgroundAdmission(); remote.WaitBackground() })
	wire, original := ownedRawAccepted(t, s, remote)
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
	if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original || facts.classes[0] != findnode.ClassOrdinary {
		t.Fatal("different peer gained premature N-in/K class", facts, err)
	}
	clock.advance(10 * time.Minute)
	s.inboundPins.notify()
	// Wake coalesces without advancing the original retry deadline. Allow one
	// original two-second cadence plus the existing fixture observation budget.
	waitFor(t, peerSweepInterval+2*time.Second, "original Run N-in worker did not strictly earn distinct peer", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.inboundAllocations == 1 && store.inboundConfirmed == 1 && store.ninJournal.strictConfirmed()
	})
	facts, err = s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
	if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original || facts.classes[0] != findnode.ClassIncomingPinned {
		t.Fatal("actual distinct N-in TLS original not classified", facts, err)
	}
	store.mu.Lock()
	kept := store.identities[kID]
	preserved := store.ninJournal.domain == initialStamp.domain && kept.classMetadata().Contact == kAllocation && kept.metadata.recordOwner == kOwner && kept.metadata.recordMirror == contactRecordOwnerCompleted
	store.mu.Unlock()
	if !preserved {
		t.Fatal("N-in earning replaced K ownership or quota domain")
	}
	if _, err := s.captureOwnedFindNodeCut(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := ownedLocalNodeRequest(t, s)
	if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, wire, request)); err != nil {
		t.Fatal(err)
	}
	answer := ownedLocalNodeReadAnswer(t, wire)
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil || answer.ID != request.ID {
		t.Fatal("same original local answer lost challenge/ID/signature", err)
	}
	waitFor(t, 2*time.Second, "original local writer released", func() bool { return len(s.bridgeCapture.localLoop.slots) == 0 })
	if !wire.fixtureOwner.stopAndJoin(t) {
		t.Fatal("original TLS fixture did not join")
	}
	cancel()
	contactPushJoin(t, running.exited)
}

func TestOwnedReadyContactReconciliationOriginalTuple(t *testing.T) {
	cfg, id, clock, _, _ := readyContactInputs(t)
	r := readyTestPrepare(t, cfg, id)
	r.options = findNodeReadyOptions{loadedInbound: true, contactReconciliation: true}
	r.originalOptions = r.options
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{record: recordAdmissionConstruction{classes: recordAdmissionImmediateK, kSource: contactRecordReconciliationSource}, bridge: r.bridge, ready: r}, inboundPinClock{elapsed: clock.read})
	if r.failure != nil || !r.checked || !r.bindings(s) || r.finalized {
		t.Fatal("original pre-finalize K tuple missing", r.failure)
	}
	resolver, admission, producer := s.identityResolver, s.identityResolver.recordAdmission, s.contactPins
	service, wake := producer.service, producer.wake
	for _, kind := range []string{"resolver", "admission", "source", "classes", "producer", "service", "wake", "request", "unrequested"} {
		switch kind {
		case "resolver":
			s.identityResolver = newIdentityResolverWithContactRecordAdmission(s, resolver.intents, resolver.network, nil, contactRecordReconciliationSource)
		case "admission":
			resolver.recordAdmission = &relationRecordAdmission{classes: recordAdmissionImmediateK, kSource: contactRecordReconciliationSource, service: s, resolver: resolver, network: resolver.network}
		case "source":
			admission.kSource = contactRecordPushSource
		case "classes":
			admission.classes = recordAdmissionChargedR
		case "producer":
			s.contactPins = &contactPinProducer{service: s, wake: wake}
		case "service":
			producer.service = &Service{}
		case "wake":
			producer.wake = make(chan struct{}, 1)
		case "request":
			r.options.contactReconciliation = false
		case "unrequested":
			r.options.contactReconciliation = false
			r.originalOptions.contactReconciliation = false
		}
		if r.bindings(s) || r.finalize(s) == nil || r.finalized {
			t.Fatal("replaced original K tuple became Ready", kind)
		}
		s.identityResolver = resolver
		resolver.recordAdmission = admission
		admission.kSource = contactRecordReconciliationSource
		admission.classes = recordAdmissionImmediateK
		s.contactPins = producer
		producer.service = service
		producer.wake = wake
		r.options.contactReconciliation = true
		r.originalOptions.contactReconciliation = true
	}
	if err := r.finalize(s); err != nil || !r.bindings(s) {
		t.Fatal("restored original tuple refused", err)
	}
}

type readyContactInspectConstructor struct {
	readyInspectConstructor
	replace bool
}

func (c *readyContactInspectConstructor) Build(s *Service, m *datagram.Metrics) (*datagramLayer, error) {
	if s.identityResolver == nil || c.r.originalContactResolver != s.identityResolver || c.r.originalContactAdmission != s.identityResolver.recordAdmission {
		c.t.Fatal("K resolver was not captured BEFORE original callback")
	}
	layer, err := c.readyInspectConstructor.Build(s, m)
	if c.replace {
		original := s.identityResolver
		s.identityResolver = newIdentityResolverWithContactRecordAdmission(s, original.intents, original.network, nil, contactRecordReconciliationSource)
	}
	return layer, err
}
func TestOwnedReadyContactReconciliationCallbackCleanup(t *testing.T) {
	for _, mode := range []string{"cancel", "panic", "replace-resolver"} {
		t.Run(mode, func(t *testing.T) {
			cfg, id, clock := readyInboundInputs(t)
			r := readyTestPrepare(t, cfg, id)
			joined := make(chan struct{})
			callback := &readyContactInspectConstructor{readyInspectConstructor: readyInspectConstructor{t: t, r: r, mode: mode, joined: joined}, replace: mode == "replace-resolver"}
			var escaped *Service
			var err error
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				escaped, err = r.buildWithOptions(cfg, id, nil, callback, findNodeReadyOptions{loadedInbound: true, contactReconciliation: true}, inboundPinClock{elapsed: clock.read})
			}()
			if escaped != nil || r.finalized || r.checked || r.bridge.statisticInstalled || r.originalContext.Err() == nil {
				t.Fatal("failed K callback escaped", err, panicked)
			}
			if mode == "panic" {
				if panicked != "ready constructor callback panic" {
					t.Fatal("wrong original panic", panicked)
				}
			} else if panicked != nil {
				t.Fatal("unexpected original panic", panicked)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("original cancellation lost", err)
			}
			if mode == "replace-resolver" && (!errors.Is(err, findnode.ErrNotReady) || r.failure != err) {
				t.Fatal("coherent pre-Build pair substitution accepted", err)
			}
			select {
			case <-joined:
			default:
				t.Fatal("original callback worker did not join")
			}
			if r.service == nil || r.service.goBackground(func() {}) {
				t.Fatal("failed K callback left admission open")
			}
			if _, err := r.originalCapture.observer.BeginCapture(); err == nil {
				t.Fatal("failed K callback sealed domain")
			}
		})
	}
}

func TestOwnedReadyContactReconciliationOptionsAndPublicDefaults(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing-loaded", true: "R-plus-K"}[own], func(t *testing.T) {
			cfg, id, _ := readyInboundInputs(t)
			r := readyTestPrepare(t, cfg, id)
			o := findNodeReadyOptions{contactReconciliation: true}
			if own {
				o.loadedInbound = true
				o.ownDM = &ownDMRuntimeConstruction{profile: OwnDMResidenceProfile{MaxPrestage: time.Minute}}
			}
			escaped, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, o)
			if escaped != nil || !errors.Is(err, findnode.ErrInvalid) || r.service != nil || r.bridge.used.Load() || r.originalContext.Err() == nil {
				t.Fatal("unsupported K option claimed original Service/bridge", err)
			}
		})
	}
	cfg, id, clock := readyInboundInputs(t)
	reads := clock.reads.Load()
	s, err := newFindNodeReadyLoadedContactReconciliationCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil || s == nil || s.inboundPins == nil {
		t.Fatal("private normal-clock K composition refused", err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if _, err := s.secureSessions.store.ninElapsed(); err != nil || clock.reads.Load() != reads {
		t.Fatal("normal private factory retained fixture clock", err)
	}
	a := s.identityResolver.recordAdmission
	if a == nil || !a.admitsContactReconciliation() || a.admitsR() || a.kFiles != nil || a.openFiles != nil || a.clock != nil || a.wake != nil || s.hasContactRecordPushTransportCapability() {
		t.Fatal("K factory acquired another authority/backend")
	}
	ordinary := NewService(cfg, id, nil)
	t.Cleanup(func() { ordinary.runCancel(); ordinary.closeBackgroundAdmission(); ordinary.WaitBackground() })
	plain := readyTestPrepare(t, cfg, id)
	plainService, err := plain.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil || plainService.identityResolver.recordAdmission != nil || plainService.inboundPins != nil || ordinary.identityResolver.recordAdmission != nil || ordinary.inboundPins != nil {
		t.Fatal("ordinary defaults activated K/N-in", err)
	}
	loaded, err := newFindNodeReadyLoadedInboundCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil || loaded == nil || loaded.identityResolver.recordAdmission != nil {
		t.Fatal("old loaded-only Ready activated K", err)
	}
	t.Cleanup(func() { loaded.runCancel(); loaded.closeBackgroundAdmission(); loaded.WaitBackground() })
	paired, _, cleanup, err := NewInactiveOwnDMRuntimeCandidate(cfg, id, nil, ownDMRuntimeTestProfile())
	if err != nil || paired.identityResolver.recordAdmission.classes != recordAdmissionChargedR || paired.identityResolver.recordAdmission.admitsK() || paired.inboundPins != nil {
		t.Fatal("existing own-DM changed class/defaults", err)
	}
	t.Cleanup(cleanup)
}

func TestOwnedReadyContactReconciliationLoadedRefusal(t *testing.T) {
	for _, kind := range []string{"missing", "format2", "recovery"} {
		t.Run(kind, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			clock := new(ninFixtureClock)
			var before, guard []byte
			var files identityPinDurableFiles
			if kind == "format2" {
				seed := ninOpenFixture(t, cfg.SecureSessionStorePath, clock)
				seed.mu.Lock()
				image := seed.snapshotLocked()
				seed.mu.Unlock()
				image.Format = 2
				var err error
				before, err = json.Marshal(image)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(cfg.SecureSessionStorePath, before, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "recovery" {
				seed := ninOpenFixture(t, cfg.SecureSessionStorePath, clock)
				ninInstallVirginFixture(t, seed, ninFilesFixture(t, seed))
				peer := domain.PeerIdentity{23}
				if err := seed.noteProvenInbound(peer); err != nil {
					t.Fatal(err)
				}
				preview := pinRemovalPreview(t, seed, peer)
				seed.mu.Lock()
				entry := seed.identities[peer]
				seed.mu.Unlock()
				backup, err := domain.NewIdentityPinBackup(peer, entry.pinnedAt, entry.classMetadata())
				if err != nil {
					t.Fatal(err)
				}
				binding := domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentityFromWire(id.Address), Store: "ready-contact-guard"}, RuntimeEpoch: "ready-contact-process", Generation: 1, Identity: peer, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt, Backup: backup}
				seed.mu.Lock()
				err = seed.checkIdentityPinRemovalLocked(binding, preview.Token)
				seed.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				guard, err = json.Marshal(identityPinGuardRecord{Format: identityPinGuardFormat, Permit: domain.IdentityPinRemovalPermit{OperationID: "ready-contact-original-guard", Binding: binding, Generation: 2}})
				if err != nil {
					t.Fatal(err)
				}
				files = ninFilesFixture(t, seed)
				if outcome, err := files.WriteGuard(t.Context(), guard); err != nil || outcome != secretfile.DurableCommitted {
					t.Fatal("actual recovery guard not committed", outcome, err)
				}
				before, err = os.ReadFile(cfg.SecureSessionStorePath)
				if err != nil {
					t.Fatal(err)
				}
			}
			r := readyTestPrepare(t, cfg, id)
			escaped, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, findNodeReadyOptions{loadedInbound: true, contactReconciliation: true}, inboundPinClock{elapsed: clock.read})
			var want error = domain.InboundQuotaNotInstalled
			if kind == "recovery" {
				want = domain.ErrIdentityPinRecoveryRequired
			}
			if escaped != nil || !errors.Is(err, want) || r.failure != err || r.checked || r.finalized || r.bridge.statisticInstalled || r.originalContext.Err() == nil {
				t.Fatal("original loaded refusal escaped/masked", kind, err)
			}
			if r.service == nil || r.service.goBackground(func() {}) {
				t.Fatal("refused K construction left admission open")
			}
			if _, err := r.originalCapture.observer.BeginCapture(); err == nil {
				t.Fatal("loaded refusal sealed domain")
			}
			after, readErr := os.ReadFile(cfg.SecureSessionStorePath)
			if kind == "missing" {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatal("refusal installed a journal", readErr)
				}
			} else if readErr != nil || !bytes.Equal(before, after) {
				t.Fatal("refusal changed journal bytes", readErr)
			}
			if kind == "recovery" {
				after, err := files.ReadGuard(t.Context())
				if err != nil || !bytes.Equal(after, guard) {
					t.Fatal("refusal cleared/recovered guard", err)
				}
			}
		})
	}
}

func TestOwnedReadyContactReconciliationRunJoinsFactClose(t *testing.T) {
	_, s, _, contact, _ := readyContactBuild(t)
	original := s.trust.localFiles
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseClose := func() { once.Do(func() { close(release) }) }
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		return real.Write(ctx, raw)
	}).Once()
	files.EXPECT().Close().RunAndReturn(func() error { close(entered); <-release; return nil }).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	allow := make(chan struct{})
	var first, allowOnce sync.Once
	unblock := func() { allowOnce.Do(func() { close(allow) }) }
	s.contactPins.clock = func() time.Time { first.Do(func() { <-allow }); return time.Now() }
	ctx, cancel := context.WithCancel(t.Context())
	running := runServiceForTest(ctx, s)
	t.Cleanup(func() { unblock(); releaseClose(); cancel(); contactPushJoin(t, running.exited) })
	waitContactRecordCondition(t, "actual Run K readiness missing", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kReady
	})
	unblock()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("original fact Close not reached")
	}
	if contactPushOccupied(s.identityResolver) != 1 {
		t.Fatal("actual Close lost physical owner")
	}
	cancel()
	waitContactRecordCondition(t, "Run did not revoke K", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kClosing
	})
	select {
	case <-running.exited:
		t.Fatal("Run returned before original Close")
	default:
	}
	if contactPushOccupied(s.identityResolver) != 1 {
		t.Fatal("cancel recycled actual synchronous owner")
	}
	releaseClose()
	contactPushJoin(t, running.exited)
	if contactPushOccupied(s.identityResolver) != 0 || s.localContactPinStore().identityPinned(domain.PeerIdentityFromWire(contact.Address)) {
		t.Fatal("close-first leaked stage or authority")
	}
}
