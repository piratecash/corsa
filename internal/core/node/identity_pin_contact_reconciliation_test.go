package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func reconciliationTestService(t *testing.T, files identityPinDurableFiles) (*Service, *identity.Identity) {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	cfg.TrustStorePath = filepath.Join(filepath.Dir(cfg.PeersStatePath), "trust.json")
	s := newServiceWithRecordAdmissionClasses(cfg, owner, nil, nil, productionDatagramConstructor{}, recordAdmissionConstruction{classes: recordAdmissionImmediateK, kSource: contactRecordReconciliationSource, borrowedKFiles: files})
	withoutOwnDials(nil)(s)
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s, peer
}
func reconciliationRecord(t *testing.T, s *Service, peer *identity.Identity, seq domain.IdentityRecordSeq, min domain.IdentityRecordMinSession) protocol.SignedIdentityRecord {
	t.Helper()
	record := buildContactRecord(t, peer, seq, min)
	body, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.trust.rememberRecord(domain.NetworkID(networkName), record, body); err != nil {
		t.Fatal(err)
	}
	return record
}
func reconciliationPrepared(t *testing.T, s *Service) (*contactRecordReconciliationAuthority, verifiedContactMinSessionRecord) {
	t.Helper()
	r := s.identityResolver
	r.beginContactRecordPushRun() // explicitly fixture-only, actual Run tested separately
	g := r.reserveContactRecordReconciliation(t.Context())
	if g == nil {
		t.Fatal("reconciliation reservation refused")
	}
	t.Cleanup(func() { r.finishContactRecordReconciliation(g) })
	candidate, err := s.contactPins.captureReconciliation(g.ctx)
	if err != nil || candidate == nil {
		t.Fatal("original capture failed", err)
	}
	verified, err := verifyContactMinSessionRecord(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.trust.confirmLocalContacts(g.ctx, candidate.fact); err != nil {
		t.Fatal(err)
	}
	if s.trust.localContactStatus(candidate.fact) != domain.LocalContactFactDurableConfirmed {
		t.Fatal("exact captured fact not ACKed")
	}
	g.admission.kStageMu.Lock()
	g.candidate = candidate
	g.admission.kStageMu.Unlock()
	return g, verified
}

func TestContactReconciliationPersistedReloadActualRunAndBarrier(t *testing.T) {
	original, peer := reconciliationTestService(t, nil)
	addLocalContact(t, original, peer)
	record := reconciliationRecord(t, original, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	// Load the actual files through a fresh Service; no copied in-memory fact ACK.
	s := newContactRecordReconciliationCandidateService(original.cfg, original.identity, nil)
	withoutOwnDials(nil)(s)
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	id := domain.PeerIdentityFromWire(peer.Address)
	state := s.trust.trustedContacts()[peer.Address].LocalContact
	if state == nil || state.confirmed != 0 || state.proof != nil || s.localContactPinStore().identityPinned(id) {
		t.Fatal("reload manufactured ACK/proof/pin")
	}
	before := state.fact
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.localContactPinStore().identityPinned(id) || s.trust.localContactStatus(state.stamp(s.trust)) == domain.LocalContactFactDurableConfirmed {
		t.Fatal("pre-Run reconciliation acquired authority")
	}
	// Hold only the worker's initial clock boundary until actual Run opens ready;
	// this avoids measuring/relying on the first periodic 2s pacing race.
	allow, entered := make(chan struct{}), make(chan struct{})
	var allowOnce sync.Once
	release := func() { allowOnce.Do(func() { close(allow) }) }
	var once sync.Once
	s.contactPins.clock = func() time.Time { once.Do(func() { close(entered); <-allow }); return time.Now() }
	ctx, cancel := context.WithCancel(t.Context())
	running := runServiceForTest(ctx, s)
	t.Cleanup(func() { release(); cancel(); contactPushJoin(t, running.exited) })
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual worker did not reach initial clock boundary")
	}
	waitContactRecordCondition(t, "actual Run did not open reconciliation", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kReady
	})
	release()
	waitContactRecordCondition(t, "persisted conjunction did not complete real barrier", func() bool {
		store := s.localContactPinStore()
		store.mu.Lock()
		defer store.mu.Unlock()
		entry, present := store.identities[id]
		return present && entry.classMetadata().Contact.State == domain.IdentityPinAllocationEligible && entry.metadata != nil && entry.metadata.confirmedRevision == entry.protectionRevision && entry.metadata.recordOwner != nil && entry.metadata.recordMirror == contactRecordOwnerCompleted
	})
	store := s.localContactPinStore()
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	if entry.metadata == nil || entry.metadata.recordOwner == nil || entry.metadata.recordMirror != contactRecordOwnerCompleted {
		t.Fatal("no actual owned completion")
	}
	if s.trust.localContactStatus(state.stamp(s.trust)) != domain.LocalContactFactDurableConfirmed || s.trust.trustedContacts()[peer.Address].LocalContact.fact != before {
		t.Fatal("reconciliation rebound/rewrote original fact")
	}
	s.trust.mu.RLock()
	current := s.trust.records[trustRecordKey{network: networkName, address: peer.Address}]
	s.trust.mu.RUnlock()
	if !bytes.Equal(record.Body, current.record.Body) || !bytes.Equal(record.Sig, current.record.Sig) {
		t.Fatal("reconciliation imported/refreshed raw record")
	}
	if s.hasContactRecordPushTransportCapability() || s.identityResolver.reserveContactRecordPush(t.Context()) != nil || s.identityResolver.beginRecordAdmissionRun() {
		t.Fatal("reconciliation enabled push/R")
	}
	cancel()
	contactPushJoin(t, running.exited)
}

func TestContactReconciliationStrictFactOutcomes(t *testing.T) {
	for _, variant := range []string{"unsupported", "not-committed", "indeterminate"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := reconciliationTestService(t, nil)
			addLocalContact(t, s, peer)
			reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			// Actual reload starts unconfirmed, including after its ordinary strict save.
			loaded, err := loadTrustStore(s.trust.path, trustedContact{})
			if err != nil {
				t.Fatal(err)
			}
			s.trust = loaded
			files := NewMocklocalContactDurableFiles(t)
			commit, writeErr := secretfile.DurableNotCommitted, errors.New("not committed")
			if variant == "indeterminate" {
				commit = secretfile.DurableIndeterminate
			}
			if variant == "unsupported" {
				writeErr = secretfile.ErrDurabilityUnsupported
			}
			files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			files.EXPECT().Write(mock.Anything, mock.Anything).Return(commit, writeErr).Once()
			files.EXPECT().Close().Return(nil).Once()
			loaded.localFiles = func() (localContactDurableFiles, error) { return files, nil }
			s.identityResolver.beginContactRecordPushRun()
			if err := s.contactPins.scanReconciliation(t.Context()); err == nil {
				t.Fatal("failed strict fact ACK staged")
			}
			id := domain.PeerIdentityFromWire(peer.Address)
			if s.localContactPinStore().identityPinned(id) || contactPushOccupied(s.identityResolver) != 0 {
				t.Fatal("failed ACK changed pin or retained returned slot")
			}
		})
	}
}

func TestContactReconciliationNilErrorStaleAckDoesNotRebind(t *testing.T) {
	s, peer := reconciliationTestService(t, nil)
	addLocalContact(t, s, peer)
	reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	original := s.trust.localFiles
	deleted := make(chan struct{})
	var once sync.Once
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil)
	files.EXPECT().Close().Return(nil)
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		once.Do(func() {
			go func() { s.deleteTrustedContactFrame(domain.PeerIdentityFromWire(peer.Address)); close(deleted) }()
			waitContactRecordCondition(t, "real delete did not remove original fact before ACK", func() bool {
				s.trust.mu.RLock()
				defer s.trust.mu.RUnlock()
				_, ok := s.trust.contacts[peer.Address]
				return !ok
			})
		})
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		return real.Write(ctx, raw)
	})
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	s.identityResolver.beginContactRecordPushRun()
	if err := s.contactPins.scanReconciliation(t.Context()); !errors.Is(err, domain.ErrLocalContactFactStale) {
		t.Fatal("nil-error stale ACK accepted", err)
	}
	contactPushJoin(t, deleted)
	if s.localContactPinStore().identityPinned(domain.PeerIdentityFromWire(peer.Address)) || contactPushOccupied(s.identityResolver) != 0 {
		t.Fatal("stale ACK created pin/retained completed invocation")
	}
}

func TestContactReconciliationOriginalCapsuleRefusesChanges(t *testing.T) {
	for _, variant := range []string{"delete-readd", "newer-min1", "exact-sig", "C3", "absence-epoch", "store-replaced", "resolver-replaced", "cancelled", "closed", "slot-ABA", "generation-mutated"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := reconciliationTestService(t, nil)
			addLocalContact(t, s, peer)
			record := reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			id := domain.PeerIdentityFromWire(peer.Address)
			if variant == "C3" {
				store := s.localContactPinStore()
				store.mu.Lock()
				err := store.recordPinEvidenceLocked(id, store.now())
				store.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			g, verified := reconciliationPrepared(t, s)
			switch variant {
			case "delete-readd":
				s.deleteTrustedContactFrame(id)
				addLocalContact(t, s, peer)
				reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			case "newer-min1":
				reconciliationRecord(t, s, peer, 3, domain.DeclaredMinSession(domain.SessionVersionLegacy))
			case "exact-sig":
				s.trust.pinEvidenceMu.Lock()
				s.trust.mu.Lock()
				key := trustRecordKey{network: networkName, address: peer.Address}
				current := s.trust.records[key]
				current.record = cloneSignedIdentityRecord(record)
				current.record.Sig[0] ^= 1
				s.trust.records[key] = current
				s.trust.mu.Unlock()
				s.trust.pinEvidenceMu.Unlock()
			case "C3":
				removeContactPinForTest(t, s, id)
			case "absence-epoch":
				store := s.localContactPinStore()
				store.pinMutationGate.Lock()
				store.mu.Lock()
				err := store.advancePinRemovalEpochLocked()
				store.mu.Unlock()
				store.pinMutationGate.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case "store-replaced":
				s.secureSessions.store = loadSecureSessionStore(filepath.Join(t.TempDir(), "replacement.json"), newStoreClock().trusted())
			case "resolver-replaced":
				s.identityResolver = newIdentityResolver(s, nil, domain.NetworkID(networkName))
			case "generation-mutated":
				g.generation++
				defer func() { g.generation-- }()
			case "cancelled":
				g.cancel()
			case "closed":
				g.resolver.closeContactRecordPushes()
			case "slot-ABA":
				g.resolver.finishContactRecordReconciliation(g)
				next := g.resolver.reserveContactRecordReconciliation(t.Context())
				if next == nil {
					t.Fatal("replacement reservation refused")
				}
				defer g.resolver.finishContactRecordReconciliation(next)
				if next.index != g.index || next.generation == g.generation {
					t.Fatal("ABA premise changed")
				}
			}
			if _, err := s.stageContactRecordReconciliation(verified, g); err == nil {
				t.Fatal("stale original reconciliation staged", variant)
			}
			if _, ok := verified.candidate.stagedOutcome(); ok {
				t.Fatal("stale reconciliation retained new obligation")
			}
		})
	}
}

func TestContactReconciliationShared256AndGenerationBound(t *testing.T) {
	s, _ := reconciliationTestService(t, nil)
	r := s.identityResolver
	r.beginContactRecordPushRun()
	var held [relationAdmissionCapacity]*contactRecordReconciliationAuthority
	t.Cleanup(func() {
		for _, g := range held {
			r.finishContactRecordReconciliation(g)
		}
	})
	for i := range held {
		held[i] = r.reserveContactRecordReconciliation(t.Context())
		if held[i] == nil {
			t.Fatal("early shared capacity refusal", i)
		}
	}
	if r.reserveContactRecordReconciliation(t.Context()) != nil || r.reserveContactRecordPush(t.Context()) != nil || contactPushOccupied(r) != 256 {
		t.Fatal("reconciliation escaped shared256/provenance")
	}
	r.closeContactRecordPushes()
	if contactPushOccupied(r) != 256 {
		t.Fatal("cancellation recycled executing ownership")
	}
	for _, g := range held {
		r.finishContactRecordReconciliation(g)
	}
	if contactPushOccupied(r) != 0 {
		t.Fatal("returned owners did not release slots")
	}
}

func TestContactReconciliationRawUnknownFieldsRemainSignedAndExact(t *testing.T) {
	s, peer := reconciliationTestService(t, nil)
	addLocalContact(t, s, peer)
	record := buildContactRecord(t, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	record.Body = append(append([]byte(nil), record.Body[:len(record.Body)-1]...), []byte(",\"future_field\":{\"x\":7}}")...)
	prefix := append([]byte("corsa-identity-record-v1\x00"), binary.BigEndian.AppendUint16(nil, uint16(len(networkName)))...)
	prefix = append(prefix, networkName...)
	record.Sig = ed25519.Sign(peer.PrivateKey, append(prefix, record.Body...))
	body, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.trust.rememberRecord(domain.NetworkID(networkName), record, body); err != nil {
		t.Fatal(err)
	}
	g, verified := reconciliationPrepared(t, s)
	if !bytes.Equal(verified.candidate.record.Body, record.Body) || !bytes.Equal(verified.candidate.record.Sig, record.Sig) {
		t.Fatal("selection reencoded signed unknown fields")
	}
	stage, err := s.stageContactRecordReconciliation(verified, g)
	if err != nil || stage.disposition != contactRecordNewOwnedStage {
		t.Fatal("valid frozen raw record refused", err)
	}
}

func TestContactReconciliationHard20kExistingAndHistorical(t *testing.T) {
	for _, variant := range []string{"full-new", "full-existing", "historical"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := reconciliationTestService(t, nil)
			addLocalContact(t, s, peer)
			reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			id := domain.PeerIdentityFromWire(peer.Address)
			store := s.localContactPinStore()
			if variant != "full-new" {
				store.mu.Lock()
				err := store.recordPinEvidenceLocked(id, store.now())
				store.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			if variant == "historical" {
				metadata := metadataSnapshot(t, store, id).metadata
				metadata.Contact = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: store.now(), Reference: "historical"}
				stageMetadata(t, store, id, metadata)
			} else {
				fillContactRecordPins(t, store, maxPinnedIdentities)
			}
			g, verified := reconciliationPrepared(t, s)
			stage, err := s.stageContactRecordReconciliation(verified, g)
			if variant == "full-new" {
				if !errors.Is(err, errPinStoreFull) || store.identityPinned(id) {
					t.Fatal("reconciliation exceeded20k", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if variant == "historical" && (stage.disposition != contactRecordAlreadyEligibleNoRecordObligation || stage.owed != nil) {
				t.Fatal("historical K fabricated new obligation", stage)
			}
			if variant == "full-existing" && stage.disposition != contactRecordNewOwnedStage {
				t.Fatal("existing pin required new global place", stage)
			}
		})
	}
}

func TestContactReconciliationStageFirstOwedSurvivesDeleteAndClose(t *testing.T) {
	s, peer := reconciliationTestService(t, nil)
	addLocalContact(t, s, peer)
	reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	g, verified := reconciliationPrepared(t, s)
	stage, err := s.stageContactRecordReconciliation(verified, g)
	if err != nil || stage.owed == nil {
		t.Fatal(err)
	}
	g.resolver.closeContactRecordPushes()
	s.deleteTrustedContactFrame(domain.PeerIdentityFromWire(peer.Address))
	g.resolver.finishContactRecordReconciliation(g)
	if contactPushOccupied(g.resolver) != 0 {
		t.Fatal("returned stage retained transient slot")
	}
	if err := s.contactPins.scanOwed(t.Context()); err != nil {
		t.Fatal(err)
	}
	stage.owed.mu.Lock()
	state := stage.owed.completion.state
	stage.owed.mu.Unlock()
	if state != contactRecordHistoricalProtectionCompleted || !s.localContactPinStore().identityPinned(domain.PeerIdentityFromWire(peer.Address)) {
		t.Fatal("close/delete revoked earned protection", state)
	}
}

func TestContactReconciliationNonConjunctionNeverStages(t *testing.T) {
	for _, variant := range []string{"generic-only", "record-only", "TLS-only", "min1", "bad-sig", "wrong-network", "oversized"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := reconciliationTestService(t, nil)
			if variant == "generic-only" {
				intent := contactIntent(peer)
				s.trustContact(peer.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "generic")
			} else if variant != "record-only" {
				addLocalContact(t, s, peer)
			}
			if variant == "TLS-only" {
				publishContactProof(t, s, peer)
			} else {
				min := domain.DeclaredMinSession(domain.SessionVersionSecure)
				if variant == "min1" {
					min = domain.DeclaredMinSession(domain.SessionVersionLegacy)
				}
				record := reconciliationRecord(t, s, peer, 2, min)
				if variant == "bad-sig" || variant == "wrong-network" || variant == "oversized" {
					s.trust.pinEvidenceMu.Lock()
					s.trust.mu.Lock()
					key := trustRecordKey{network: networkName, address: peer.Address}
					current := s.trust.records[key]
					if variant == "bad-sig" {
						current.record = cloneSignedIdentityRecord(record)
						current.record.Sig[0] ^= 1
					}
					if variant == "wrong-network" {
						delete(s.trust.records, key)
						key.network = "other-network"
					}
					if variant == "oversized" {
						current.record.Body = make([]byte, domain.MaxIdentityRecordBodyBytes+1)
					}
					s.trust.records[key] = current
					s.trust.mu.Unlock()
					s.trust.pinEvidenceMu.Unlock()
				}
			}
			s.identityResolver.beginContactRecordPushRun()
			_ = s.contactPins.scanReconciliation(t.Context())
			id := domain.PeerIdentityFromWire(peer.Address)
			store := s.localContactPinStore()
			store.mu.Lock()
			entry := store.identities[id]
			store.mu.Unlock()
			if entry.classMetadata().Contact.State == domain.IdentityPinAllocationEligible || entry.metadata != nil && entry.metadata.recordOwner != nil {
				t.Fatal("non-conjunction created K2", variant)
			}
			if contactPushOccupied(s.identityResolver) != 0 {
				t.Fatal("refusal retained returned slot")
			}
		})
	}
}

func TestContactReconciliationCursorAdvancesOnRefusalAndWraps(t *testing.T) {
	s, first := reconciliationTestService(t, nil)
	second, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range []*identity.Identity{first, second} {
		addLocalContact(t, s, peer)
		reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionLegacy))
	}
	s.identityResolver.beginContactRecordPushRun()
	var positions [3]domain.PeerIdentity
	for i := range positions {
		if err := s.contactPins.scanReconciliation(t.Context()); err == nil {
			t.Fatal("min1 refusal premise lost")
		}
		if s.contactPins.reconcileCursor == nil {
			t.Fatal("refusal did not advance cursor")
		}
		positions[i] = *s.contactPins.reconcileCursor
	}
	if positions[0].String() >= positions[1].String() || positions[0] != positions[2] {
		t.Fatal("one-selection lexicographic wrap/refusal progression changed", positions)
	}
}

func TestContactReconciliationRunJoinsActualFactClose(t *testing.T) {
	s, peer := reconciliationTestService(t, nil)
	addLocalContact(t, s, peer)
	reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
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
	var clockOnce, allowOnce sync.Once
	unblock := func() { allowOnce.Do(func() { close(allow) }) }
	s.contactPins.clock = func() time.Time { clockOnce.Do(func() { <-allow }); return time.Now() }
	ctx, cancel := context.WithCancel(t.Context())
	running := runServiceForTest(ctx, s)
	t.Cleanup(func() { unblock(); releaseClose(); cancel(); contactPushJoin(t, running.exited) })
	waitContactRecordCondition(t, "actual Run readiness missing", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kReady
	})
	unblock()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual worker did not enter strict backend Close")
	}
	if contactPushOccupied(s.identityResolver) != 1 {
		t.Fatal("actual Close lost physical owner")
	}
	cancel()
	waitContactRecordCondition(t, "shutdown did not revoke reconciliation", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kClosing
	})
	select {
	case <-running.exited:
		t.Fatal("Run returned before actual Close")
	default:
	}
	if contactPushOccupied(s.identityResolver) != 1 {
		t.Fatal("cancel released blocked actual Close")
	}
	releaseClose()
	contactPushJoin(t, running.exited)
	if contactPushOccupied(s.identityResolver) != 0 || s.localContactPinStore().identityPinned(domain.PeerIdentityFromWire(peer.Address)) {
		t.Fatal("closed-before-stage leaked authority or completed pin")
	}
}

func TestContactReconciliationOtherConstructorsStayClosed(t *testing.T) {
	s, _ := reconciliationTestService(t, nil)
	normal := NewService(s.cfg, s.identity, nil)
	push := contactPushTestService(t, nil)
	charged := newRelationRecordCandidateService(s.cfg, s.identity, nil)
	for _, other := range []*Service{normal, charged} {
		t.Cleanup(func() { other.runCancel(); other.closeBackgroundAdmission(); other.WaitBackground() })
	}
	for _, other := range []*Service{normal, push, charged} {
		other.identityResolver.beginContactRecordPushRun()
		if other.identityResolver.reserveContactRecordReconciliation(t.Context()) != nil {
			t.Fatal("another constructor borrowed reconciliation authority")
		}
		if err := other.contactPins.scanReconciliation(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if !push.hasContactRecordPushTransportCapability() || s.hasContactRecordPushTransportCapability() || normal.hasContactRecordPushTransportCapability() || charged.hasContactRecordPushTransportCapability() {
		t.Fatal("immutable source changed push boundary")
	}
	if !charged.identityResolver.recordAdmission.admitsR() || s.identityResolver.recordAdmission.admitsR() {
		t.Fatal("reconciliation changed R class authority")
	}
}

func TestContactReconciliationSharesOccupiedRSlotWithoutRateMutation(t *testing.T) {
	s, _ := reconciliationTestService(t, nil)
	r := s.identityResolver
	r.beginContactRecordPushRun()
	a := r.recordAdmission
	// Logical shared-table fixture only; public/private Service constructors do
	// not activate a combined R+K source. Preserve this occupied R slot verbatim.
	original := relationAdmissionSlot{generation: 19, phase: relationAdmissionQueued, fifo: 7, queueAccepted: true}
	r.mu.Lock()
	a.slots[0] = original
	a.fifo = 7
	a.rate.count = 1
	a.rate.events[0] = 42
	r.mu.Unlock()
	var held [relationAdmissionCapacity - 1]*contactRecordReconciliationAuthority
	t.Cleanup(func() {
		for _, g := range held {
			r.finishContactRecordReconciliation(g)
		}
	})
	for i := range held {
		held[i] = r.reserveContactRecordReconciliation(t.Context())
		if held[i] == nil {
			t.Fatal("early mixed-table refusal", i)
		}
	}
	if r.reserveContactRecordReconciliation(t.Context()) != nil {
		t.Fatal("occupied R slot discounted")
	}
	r.mu.Lock()
	unchanged := a.slots[0].phase == original.phase && a.slots[0].generation == 19 && a.slots[0].fifo == 7 && a.slots[0].queueAccepted && a.rate.count == 1 && a.rate.events[0] == 42 && a.fifo == 7
	r.mu.Unlock()
	if !unchanged {
		t.Fatal("K reconciliation changed R quota/slot accounting")
	}
	for _, g := range held {
		r.finishContactRecordReconciliation(g)
	}
}

func TestContactReconciliationRevisionAndGenerationSaturationRefuse(t *testing.T) {
	for _, variant := range []string{"evidence", "protection", "generation"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := reconciliationTestService(t, nil)
			addLocalContact(t, s, peer)
			reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			if variant == "generation" {
				r := s.identityResolver
				r.beginContactRecordPushRun()
				r.mu.Lock()
				for i := range r.recordAdmission.slots {
					r.recordAdmission.slots[i].generation = math.MaxUint64
				}
				r.mu.Unlock()
				if r.reserveContactRecordReconciliation(t.Context()) != nil {
					t.Fatal("generation wrapped")
				}
				return
			}
			g, verified := reconciliationPrepared(t, s)
			store := s.localContactPinStore()
			store.mu.Lock()
			if variant == "evidence" {
				store.evidenceRevision = math.MaxUint64
			} else {
				store.protectionRevision = math.MaxUint64
			}
			before := store.changes
			store.mu.Unlock()
			if _, err := s.stageContactRecordReconciliation(verified, g); err == nil {
				t.Fatal("revision wrapped")
			}
			store.mu.Lock()
			changed := store.changes != before
			store.mu.Unlock()
			if changed || store.identityPinned(domain.PeerIdentityFromWire(peer.Address)) {
				t.Fatal("overflow partially staged")
			}
		})
	}
}

func TestContactReconciliationOriginalPinCapturedBeforeAckWait(t *testing.T) {
	s, peer := reconciliationTestService(t, nil)
	addLocalContact(t, s, peer)
	reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	id := domain.PeerIdentityFromWire(peer.Address)
	store := s.localContactPinStore()
	store.mu.Lock()
	err := store.recordPinEvidenceLocked(id, store.now())
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	original := s.trust.localFiles
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().Close().Return(nil).Once()
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		// Real C3 transaction while exact fact ACK is still pending. A late
		// observeContactRecord would capture the new absence and silently rebind.
		removeContactPinForTest(t, s, id)
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		return real.Write(ctx, raw)
	}).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	s.identityResolver.beginContactRecordPushRun()
	if err := s.contactPins.scanReconciliation(t.Context()); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatal("original pin recaptured after fact ACK", err)
	}
	if store.contactKStatus(id) == domain.LocalContactKStagedUnconfirmed || store.contactKStatus(id) == domain.LocalContactKConfirmed || contactPushOccupied(s.identityResolver) != 0 {
		t.Fatal("C3 during ACK created replacement K or retained returned slot")
	}
}
