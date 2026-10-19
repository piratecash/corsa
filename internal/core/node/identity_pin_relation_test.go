package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

// This compiles on baseline305: actual store diagnostics currently omit R
// allocations even when persisted metadata already contains that allocation.
func TestRelationPinStatsCountRawAllocationsIncludingContact(t *testing.T) {
	store, _, _ := selectedFixture(t)
	allocation := domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: newStoreClock().now.UTC()}
	store.mu.Lock()
	for i := 1; i <= 3; i++ {
		id := domain.PeerIdentity{174, byte(i)}
		if err := store.recordPinEvidenceLocked(id, allocation.At); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
		entry := store.identities[id]
		metadata := domain.IdentityPinMetadata{Relation: allocation}
		if i == 1 {
			metadata.Contact = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: allocation.At}
		}
		if err := entry.setUnpublishedMetadata(metadata); err != nil {
			store.mu.Unlock()
			t.Fatal("unowned raw-allocation fixture metadata refused", err)
		}
		store.putPinEntryLocked(id, entry)
	}
	store.mu.Unlock()
	raw, err := json.Marshal(store.stats())
	if err != nil {
		t.Fatal(err)
	}
	var stats map[string]json.RawMessage
	if err := json.Unmarshal(raw, &stats); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := json.Unmarshal(stats["RelationAllocations"], &count); err != nil {
		t.Fatalf("existing Relation metadata has no allocation diagnostics: %v", err)
	}
	if count != 3 {
		t.Fatalf("raw Relation allocations including K = %d, want3", count)
	}
}

type relationRecordFixture struct {
	question relationPinQuestion
	record   protocol.SignedIdentityRecord
	proof    []byte
	owner    *identity.Identity
	request  []byte
}

func signedRelationFixture(t *testing.T) relationRecordFixture {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	record, err := protocol.BuildSignedIdentityRecord(owner, protocol.IdentityRecordSpec{Network: testRecordStoreNetwork, Seq: 3, IssuedAt: 1, MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure)})
	if err != nil {
		t.Fatal(err)
	}
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: domain.IdentityLookupSchemaVersion, TargetProof: true, MinSeq: 2})
	if err != nil {
		t.Fatal(err)
	}
	question := relationPinQuestion{network: testRecordStoreNetwork, target: domain.PeerIdentityFromWire(owner.Address), label: domain.PeerIdentity{222, 1}, qHash: sha256.Sum256(request), minSeq: 2}
	return relationRecordFixture{question: question, record: record, proof: protocol.SignTargetProof(owner, question.network, question.label, request, record), owner: owner, request: request}
}

func (f relationRecordFixture) verified(t *testing.T) verifiedRelationPinRecord {
	t.Helper()
	witness, err := verifyRelationPinRecord(f.question, f.record, f.proof)
	if err != nil {
		t.Fatal(err)
	}
	if witness.evidence.sequence != 3 {
		t.Fatal("verification lost actual signed sequence")
	}
	return witness
}

func captureRelationOwner(t *testing.T, store *secureSessionStore, id domain.PeerIdentity) pinOwnerObservation {
	t.Helper()
	observation, err := store.capturePinOwnerObservation(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func TestRelationPinVerifierBindsRealSignaturesAndExactQuestion(t *testing.T) {
	f := signedRelationFixture(t)
	f.verified(t)
	cases := []struct {
		name   string
		mutate func(*relationRecordFixture)
	}{
		{"signature", func(f *relationRecordFixture) {
			f.record.Sig = append([]byte(nil), f.record.Sig...)
			f.record.Sig[0] ^= 1
		}},
		{"record signature under valid target proof", func(f *relationRecordFixture) {
			f.record.Sig = append([]byte(nil), f.record.Sig...)
			f.record.Sig[0] ^= 1
			f.proof = protocol.SignTargetProof(f.owner, f.question.network, f.question.label, f.request, f.record)
		}},
		{"target proof", func(f *relationRecordFixture) { f.proof = append([]byte(nil), f.proof...); f.proof[0] ^= 1 }},
		{"missing proof", func(f *relationRecordFixture) { f.proof = nil }},
		{"network", func(f *relationRecordFixture) { f.question.network = "other-network" }},
		{"target", func(f *relationRecordFixture) { f.question.target = domain.PeerIdentity{222, 2} }},
		{"label", func(f *relationRecordFixture) { f.question.label = domain.PeerIdentity{222, 3} }},
		{"question hash", func(f *relationRecordFixture) { f.question.qHash[0] ^= 1 }},
		{"minimum sequence", func(f *relationRecordFixture) { f.question.minSeq = 4 }},
		{"legacy minimum", func(f *relationRecordFixture) {
			f.record = relationRecordWithMinimum(t, *f, domain.DeclaredMinSession(domain.SessionVersionLegacy))
			f.proof = protocol.SignTargetProof(f.owner, f.question.network, f.question.label, f.request, f.record)
		}},
		{"absent minimum", func(f *relationRecordFixture) {
			f.record = relationRecordWithMinimum(t, *f, domain.AbsentMinSession())
			f.proof = protocol.SignTargetProof(f.owner, f.question.network, f.question.label, f.request, f.record)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			copy := f
			tc.mutate(&copy)
			witness, err := verifyRelationPinRecord(copy.question, copy.record, copy.proof)
			if !errors.Is(err, errRelationPinRecord) || witness.evidence != nil {
				t.Fatalf("invalid evidence accepted: %v", err)
			}
		})
	}
}

func relationRecordWithMinimum(t *testing.T, f relationRecordFixture, minimum domain.IdentityRecordMinSession) protocol.SignedIdentityRecord {
	t.Helper()
	record, err := protocol.BuildSignedIdentityRecord(f.owner, protocol.IdentityRecordSpec{Network: f.question.network, Seq: 3, IssuedAt: 1, MinSession: minimum})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestRelationPinRecordOnlyStrictCommitAndHistoricalReload(t *testing.T) {
	store, _, _, files := pinTransactionFixture(t)
	f := signedRelationFixture(t)
	observed := captureRelationOwner(t, store, f.question.target)
	stage, err := store.stageRelationRecord(t.Context(), observed, f.verified(t))
	if err != nil {
		t.Fatal(err)
	}
	if !stage.binding.matchesForTest(store) || stage.mutation.state != pinMetadataStagedUnflushed {
		t.Fatal("record-only stage has no actual owner")
	}
	if bound := store.endpointsBoundTo(f.question.target); len(bound.canonical)+len(bound.nonCanonical) != 0 {
		t.Fatal("signed record fabricated an endpoint/session binding")
	}
	if stats := store.stats(); stats.RelationAllocations != 1 || stats.RelationConfirmed != 0 {
		t.Fatalf("not counted unconfirmed: %+v", stats)
	}
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	if store.stats().RelationConfirmed != 0 {
		t.Fatal("ordinary save acknowledged record class")
	}
	preview := pinRemovalPreview(t, store, f.question.target)
	if _, err := store.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	if store.stats().RelationConfirmed != 1 {
		t.Fatal("strict full snapshot failed to acknowledge")
	}
	if _, err := store.stageRelationRecord(t.Context(), observed, f.verified(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("old absence rebound after installation: %v", err)
	}
	existing := captureRelationOwner(t, store, f.question.target)
	store.mu.Lock()
	before := store.identities[f.question.target]
	store.mu.Unlock()
	fresh := signedRelationFixtureForOwner(t, f)
	stage, err = store.stageRelationRecord(t.Context(), existing, fresh.verified(t))
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	after := store.identities[f.question.target]
	tokenErr := store.validateIdentityPinTokenLocked(f.question.target, preview.Token)
	store.mu.Unlock()
	if !errors.Is(tokenErr, domain.ErrIdentityPinConfirmationStale) || after.evidenceRevision <= before.evidenceRevision || after.incarnation != before.incarnation || after.protectionRevision != before.protectionRevision || !after.pinnedAt.Equal(before.pinnedAt) || !after.relationConfirmed() {
		t.Fatal("fresh record did not invalidate old preview while preserving current confirmed allocation")
	}
	if stage.mutation.state != pinMetadataUnchangedInMemory {
		t.Fatal("fresh existing-class evidence invented metadata change")
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if stats := restarted.stats(); stats.RelationAllocations != 1 || stats.RelationConfirmed != 0 {
		t.Fatalf("restart lost protection or invented runtime ACK: %+v", stats)
	}
	if _, err := restarted.stageRelationRecord(t.Context(), existing, fresh.verified(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("prior process observation accepted after restart: %v", err)
	}
	if err := restarted.noteProvenInbound(f.question.target); err != nil {
		t.Fatal(err)
	}
	if stats := restarted.stats(); stats.RelationAllocations != 1 {
		t.Fatal("later actual session changed record allocation or invented endpoint")
	}
}

func (b identityPinEntryBinding) matchesForTest(s *secureSessionStore) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return b.matchesLocked(s)
}

func signedRelationFixtureForOwner(t *testing.T, f relationRecordFixture) relationRecordFixture {
	t.Helper()
	f.question.label = domain.PeerIdentity{223, 2}
	f.proof = protocol.SignTargetProof(f.owner, f.question.network, f.question.label, f.request, f.record)
	return f
}

func TestRelationPinAbsentABAAndExistingIncarnation(t *testing.T) {
	store, _, _ := selectedFixture(t)
	f := signedRelationFixture(t)
	witness := f.verified(t)
	absent := captureRelationOwner(t, store, f.question.target)
	if err := store.noteProvenInbound(f.question.target); err != nil {
		t.Fatal(err)
	}
	existing := captureRelationOwner(t, store, f.question.target)
	if err := store.noteProvenInbound(f.question.target); err != nil {
		t.Fatal(err)
	}
	store.pinMutationGate.Lock()
	store.mu.Lock()
	if err := store.advancePinRemovalEpochLocked(); err != nil {
		t.Fatal(err)
	}
	store.mu.Unlock()
	store.pinMutationGate.Unlock()
	if _, err := store.stageRelationRecord(t.Context(), existing, witness); err != nil {
		t.Fatalf("unrelated removal/reproof invalidated existing incarnation: %v", err)
	}
	store.pinMutationGate.Lock()
	store.mu.Lock()
	store.deletePinEntryLocked(f.question.target)
	store.mu.Unlock()
	store.pinMutationGate.Unlock()
	for _, old := range []pinOwnerObservation{absent, existing, {}} {
		if _, err := store.stageRelationRecord(t.Context(), old, witness); err == nil {
			t.Fatal("stale/unknown owner installed R after ABA")
		}
	}
	if err := store.noteProvenInbound(f.question.target); err != nil {
		t.Fatal(err)
	}
	if _, err := store.stageRelationRecord(t.Context(), existing, witness); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("old incarnation rebound to replacement: %v", err)
	}
	other := signedRelationFixture(t)
	otherObservation := captureRelationOwner(t, store, other.question.target)
	store.pinMutationGate.Lock()
	store.mu.Lock()
	if err := store.advancePinRemovalEpochLocked(); err != nil {
		t.Fatal(err)
	}
	store.mu.Unlock()
	store.pinMutationGate.Unlock()
	if _, err := store.stageRelationRecord(t.Context(), otherObservation, other.verified(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("absent witness survived destructive epoch: %v", err)
	}
}

func TestRelationPinAtomicRefusalNoPartialMutation(t *testing.T) {
	for _, name := range []string{"evidence overflow", "protection overflow", "unreadable", "fatal recovery", "canceled", "no verified capsule"} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := selectedFixture(t)
			f := signedRelationFixture(t)
			observation := captureRelationOwner(t, store, f.question.target)
			witness := f.verified(t)
			ctx := t.Context()
			store.mu.Lock()
			switch name {
			case "evidence overflow":
				store.evidenceRevision = math.MaxUint64
			case "protection overflow":
				store.protectionRevision = math.MaxUint64
			case "unreadable":
				store.unreadable = true
			case "fatal recovery":
				store.pinRecovery = &identityPinRecoveryState{fatal: true}
			case "no verified capsule":
				witness = verifiedRelationPinRecord{}
			}
			beforeCount, beforeChanges, beforeEvidence, beforeProtection := len(store.identities), store.changes, store.evidenceRevision, store.protectionRevision
			store.mu.Unlock()
			if name == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := store.stageRelationRecord(ctx, observation, witness); err == nil {
				t.Fatal("refusal admitted record")
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.identities) != beforeCount || store.changes != beforeChanges || store.evidenceRevision != beforeEvidence || store.protectionRevision != beforeProtection || store.relationAllocations != 0 {
				t.Fatal("refusal partially mutated owner")
			}
		})
	}
}

func seedRelationAllocations(t *testing.T, store *secureSessionStore, count int) {
	t.Helper()
	store.mu.Lock()
	allocation := domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: store.now()}
	for i := 1; i <= count; i++ {
		id := domain.PeerIdentity{175, byte(i), byte(i >> 8)}
		if err := store.recordPinEvidenceLocked(id, allocation.At); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
		entry := store.identities[id]
		if err := entry.setUnpublishedMetadata(domain.IdentityPinMetadata{Relation: allocation, Contact: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: allocation.At}}); err != nil {
			store.mu.Unlock()
			t.Fatal("unowned relation quota fixture refused metadata", err)
		}
		store.putPinEntryLocked(id, entry)
	}
	store.changes++
	store.mu.Unlock()
}

func TestRelationPinRawQuotaIncludesContactUnconfirmedAndReservation(t *testing.T) {
	store, _, _ := selectedFixture(t)
	seedRelationAllocations(t, store, maxRelationPins)
	f := signedRelationFixture(t)
	observed := captureRelationOwner(t, store, f.question.target)
	if _, err := store.stageRelationRecord(t.Context(), observed, f.verified(t)); !errors.Is(err, errRelationPinQuotaFull) {
		t.Fatalf("K+R exempted raw R quota: %v", err)
	}
	store.mu.Lock()
	reservedID := domain.PeerIdentity{175, 1}
	entry := store.identities[reservedID]
	backup, err := domain.NewIdentityPinBackup(reservedID, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	store.deletePinEntryLocked(reservedID)
	store.pinRecovery = &identityPinRecoveryState{permit: &domain.IdentityPinRemovalPermit{Binding: domain.IdentityPinFenceBinding{Identity: reservedID, PreviousPinnedAt: entry.pinnedAt, Backup: backup}}}
	store.mu.Unlock()
	if stats := store.stats(); stats.RelationAllocations != maxRelationPins || !stats.RelationAccountingAvailable {
		t.Fatalf("absent recovery place uncounted: %+v", stats)
	}
	if _, err := store.stageRelationRecord(t.Context(), observed, f.verified(t)); !errors.Is(err, errRelationPinQuotaFull) {
		t.Fatalf("another identity consumed reservation: %v", err)
	}
	// Use a genuinely signed identity for its own backup below, rather than
	// forging the reservedID's key or treating a signature for f as reservedID.
}

func TestRelationPinOwnRecoveryReservationPreservesCompleteFacts(t *testing.T) {
	store, _, _ := selectedFixture(t)
	f := signedRelationFixture(t)
	if err := store.noteProvenInbound(f.question.target); err != nil {
		t.Fatal(err)
	}
	metadata := pinClassMetadataFixture(t)
	stageMetadata(t, store, f.question.target, metadata)
	store.mu.Lock()
	entry := store.identities[f.question.target]
	backup, err := domain.NewIdentityPinBackup(f.question.target, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	store.deletePinEntryLocked(f.question.target)
	store.pinRecovery = &identityPinRecoveryState{permit: &domain.IdentityPinRemovalPermit{Binding: domain.IdentityPinFenceBinding{Identity: f.question.target, PreviousPinnedAt: entry.pinnedAt, Backup: backup}}}
	store.mu.Unlock()
	observed := captureRelationOwner(t, store, f.question.target)
	if _, err := store.stageRelationRecord(t.Context(), observed, f.verified(t)); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	restored := store.identities[f.question.target]
	store.mu.Unlock()
	actual := restored.classMetadata()
	if actual.Contact != metadata.Contact || actual.Selected != metadata.Selected || actual.Inbound != metadata.Inbound || actual.Relation.State != domain.IdentityPinAllocationEligible || !restored.pinnedAt.Equal(entry.pinnedAt) || store.stats().RelationAllocations != 1 {
		t.Fatal("own reservation replacement lost complete backup or double-counted R")
	}
}

func TestRelationPinStrictUnknownKeepsCountedUnconfirmedProtection(t *testing.T) {
	store, _, _, real := pinTransactionFixture(t)
	f := signedRelationFixture(t)
	stage, err := store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, f.question.target), f.verified(t))
	if err != nil {
		t.Fatal(err)
	}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil)
	injected := errors.New("strict commit outcome unknown")
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		if _, err := real.WriteMain(ctx, raw); err != nil {
			return secretfile.DurableNotCommitted, err
		}
		return secretfile.DurableIndeterminate, injected
	}).Once()
	if _, err := store.confirmPinMetadata(t.Context(), files); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if stats := store.stats(); stats.RelationAllocations != 1 || stats.RelationConfirmed != 0 || !stage.binding.matchesForTest(store) {
		t.Fatal("unknown commit discarded or acknowledged protection")
	}
	if _, err := store.confirmPinMetadata(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	if store.stats().RelationConfirmed != 1 {
		t.Fatal("strict retry did not acknowledge current R")
	}
}

func TestRelationPinCaptureCanceledAndInvalidOwner(t *testing.T) {
	store, _, _ := selectedFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.capturePinOwnerObservation(ctx, domain.PeerIdentity{240}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := store.capturePinOwnerObservation(t.Context(), domain.PeerIdentity{}); !errors.Is(err, domain.ErrIdentityPinIdentityRequired) {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.unreadable = true
	store.mu.Unlock()
	if _, err := store.capturePinOwnerObservation(t.Context(), domain.PeerIdentity{240}); !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
		t.Fatal(err)
	}
}

func TestRelationPinHistoricalOverCapacityIsPreserved(t *testing.T) {
	store, _, _ := selectedFixture(t)
	seedRelationAllocations(t, store, maxRelationPins+1)
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	reloaded := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	stats := reloaded.stats()
	if stats.RelationAllocations != maxRelationPins+1 || !stats.RelationOverCapacity || !stats.RelationAccountingAvailable || stats.RelationConfirmed != 0 {
		t.Fatalf("historical raw R silently evicted/exempted/certified: %+v", stats)
	}
	f := signedRelationFixture(t)
	if _, err := reloaded.stageRelationRecord(t.Context(), captureRelationOwner(t, reloaded, f.question.target), f.verified(t)); !errors.Is(err, errRelationPinQuotaFull) {
		t.Fatal(err)
	}
}

// Time remains owner-local: a correctly signed informational issued_at=1 was
// accepted above, while the new pin uses trusted store time, never record age.
func TestRelationPinNewProtectionUsesTrustedLocalTime(t *testing.T) {
	store, _, _ := selectedFixture(t)
	f := signedRelationFixture(t)
	if _, err := store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, f.question.target), f.verified(t)); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, f.question.target)
	if !preview.PinnedAt.Equal(newStoreClock().now) || preview.PinnedAt.Equal(time.Unix(1, 0)) {
		t.Fatal("remote informational issue time became local pin creation time")
	}
}

func TestRelationPinGlobalBoundDoesNotPartiallyAllocate(t *testing.T) {
	store, _, _ := selectedFixture(t)
	store.mu.Lock()
	for i := 1; len(store.identities) < maxPinnedIdentities; i++ {
		id := domain.PeerIdentity{176, byte(i), byte(i >> 8), byte(i >> 16)}
		if err := store.restoreHistoricalPinLocked(historicalPinEvidence{identity: id, at: store.now()}); err != nil {
			t.Fatal(err)
		}
	}
	beforeEvidence, beforeProtection, beforeChanges := store.evidenceRevision, store.protectionRevision, store.changes
	store.mu.Unlock()
	f := signedRelationFixture(t)
	if _, err := store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, f.question.target), f.verified(t)); !errors.Is(err, errPinStoreFull) {
		t.Fatalf("new record bypassed total20000: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.identities) != maxPinnedIdentities || store.evidenceRevision != beforeEvidence || store.protectionRevision != beforeProtection || store.changes != beforeChanges || store.relationAllocations != 0 {
		t.Fatal("full-store refusal partially staged new R")
	}
}

func TestRelationPinSameIdentityReservationAtRawQuotaBoundary(t *testing.T) {
	for _, hasR := range []bool{false, true} {
		t.Run(map[bool]string{false: "newRrefused", true: "ownRtransfer"}[hasR], func(t *testing.T) {
			store, _, _ := selectedFixture(t)
			f := signedRelationFixture(t)
			seed := maxRelationPins
			if hasR {
				seed--
			}
			seedRelationAllocations(t, store, seed)
			if err := store.noteProvenInbound(f.question.target); err != nil {
				t.Fatal(err)
			}
			metadata := pinClassMetadataFixture(t)
			metadata.Selected = selectedAllocation(t)
			if hasR {
				metadata.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: newStoreClock().now.UTC(), Reference: "old-lookup"}
			}
			stageMetadata(t, store, f.question.target, metadata)
			store.mu.Lock()
			entry := store.identities[f.question.target]
			backup, err := domain.NewIdentityPinBackup(f.question.target, entry.pinnedAt, metadata)
			if err != nil {
				t.Fatal(err)
			}
			store.deletePinEntryLocked(f.question.target)
			store.pinRecovery = &identityPinRecoveryState{permit: &domain.IdentityPinRemovalPermit{Binding: domain.IdentityPinFenceBinding{Identity: f.question.target, PreviousPinnedAt: entry.pinnedAt, Backup: backup}}}
			beforeEvidence, beforeProtection := store.evidenceRevision, store.protectionRevision
			store.mu.Unlock()
			_, err = store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, f.question.target), f.verified(t))
			if !hasR {
				if !errors.Is(err, errRelationPinQuotaFull) {
					t.Fatalf("backup without R bypassed raw quota: %v", err)
				}
				store.mu.Lock()
				_, exists := store.identities[f.question.target]
				changed := store.evidenceRevision != beforeEvidence || store.protectionRevision != beforeProtection
				store.mu.Unlock()
				if exists || changed {
					t.Fatal("refused recovery replacement partially allocated")
				}
				return
			}
			if err != nil {
				t.Fatalf("own reservation charged twice: %v", err)
			}
			store.mu.Lock()
			restored := store.identities[f.question.target]
			store.mu.Unlock()
			if restored.classMetadata() != metadata || !restored.pinnedAt.Equal(entry.pinnedAt) || store.stats().RelationAllocations != maxRelationPins {
				t.Fatal("same-identity replacement lost complete facts or double-counted")
			}
		})
	}
}

func TestRelationPinDamagedReservationAndAccountingFailClosed(t *testing.T) {
	for _, kind := range []string{"negative tally", "damaged backup"} {
		t.Run(kind, func(t *testing.T) {
			store, _, _ := selectedFixture(t)
			f := signedRelationFixture(t)
			observation := captureRelationOwner(t, store, f.question.target)
			store.mu.Lock()
			if kind == "negative tally" {
				store.relationAllocations = -1
			} else {
				store.pinRecovery = &identityPinRecoveryState{permit: &domain.IdentityPinRemovalPermit{Binding: domain.IdentityPinFenceBinding{Identity: domain.PeerIdentity{177}, PreviousPinnedAt: newStoreClock().now}}}
			}
			before := store.evidenceRevision
			store.mu.Unlock()
			if _, err := store.stageRelationRecord(t.Context(), observation, f.verified(t)); err == nil {
				t.Fatal("unavailable accounting interpreted as free capacity")
			}
			if store.stats().RelationAccountingAvailable {
				t.Fatal("damaged accounting certified available")
			}
			store.mu.Lock()
			_, exists := store.identities[f.question.target]
			changed := before != store.evidenceRevision
			store.mu.Unlock()
			if exists || changed {
				t.Fatal("accounting refusal partially staged record")
			}
		})
	}
}
