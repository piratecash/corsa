package node

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func TestInboundN0C3CleanRecoveryPreservesDebitAndFreshIncarnation(t *testing.T) {
	s, c, f, w := ninMatureFixture(t)
	if _, err := s.stageInboundPin(t.Context(), w, f); err != nil {
		t.Fatal(err)
	}
	if _, err := s.confirmPinMetadata(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	id := w.binding.identity
	preview := pinRemovalPreview(t, s, id)
	s.mu.Lock()
	entry := s.identities[id]
	s.mu.Unlock()
	binding := pinBindingBackup(t, domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{22}, Store: "nin-store"}, RuntimeEpoch: "nin-process", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt})
	fullBackup, backupErr := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if backupErr != nil {
		t.Fatal(backupErr)
	}
	binding.Backup = fullBackup
	permit := domain.IdentityPinRemovalPermit{OperationID: "nin-remove", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	c.advance(time.Hour)
	result, err := s.removeIdentityPin(t.Context(), binding, preview.Token, fence, f)
	if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
		t.Fatal(result, err)
	}
	saved := ninReadFileFixture(t, s)
	if saved.Format != 3 || saved.InboundQuota.rows[0].count != 1 || len(saved.Identities) != 0 {
		t.Fatal("C3 dropped journal or kept removed class")
	}
	if s.inboundQuotaStatus(domain.SourceGroupLoopback).Allocations != 1 {
		t.Fatal("absent C3 reservation dropped allocation")
	}
	restarted := openSecureSessionStore(secureSessionStoreOptions{path: filepath.Join(s.dir, s.name), clock: newStoreClock().trusted(), logger: s.logger, audit: secretfile.AuditLocation, ninElapsed: c.read})
	if !restarted.pinMainLoaded || restarted.startupRefusal() == nil {
		t.Fatal("C3 fixture must load complete main but remain unavailable for unsupervised startup")
	}
	recovery := NewMockIdentityPinRemovalFence(t)
	recovery.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	if _, err := restarted.recoverIdentityPinProtection(t.Context(), permit, recovery, f); err != nil {
		t.Fatal(err)
	}
	if restarted.inboundQuotaStatus(domain.SourceGroupLoopback).Events != 1 || restarted.inboundAllocations != 1 {
		t.Fatal("recovery refunded/debited journal")
	}
	if _, err := restarted.stageInboundPin(t.Context(), w, f); !errors.Is(err, errNinWitness) {
		t.Fatal("old witness rebound after recovery", err)
	}
	if got := metadataSnapshot(t, restarted, id).metadata; got.Inbound != entry.classMetadata().Inbound {
		t.Fatal("backup class lost")
	}
}
func TestInboundN0C3RollbackPreservesContentAndProgress(t *testing.T) {
	s, c, f, w := ninMatureFixture(t)
	if _, err := s.stageInboundPin(t.Context(), w, f); err != nil {
		t.Fatal(err)
	}
	id := w.binding.identity
	preview := pinRemovalPreview(t, s, id)
	s.mu.Lock()
	entry := s.identities[id]
	s.mu.Unlock()
	binding := pinBindingBackup(t, domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{22}, Store: "nin-rollback"}, RuntimeEpoch: "nin-process", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt})
	fullBackup, backupErr := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if backupErr != nil {
		t.Fatal(backupErr)
	}
	binding.Backup = fullBackup
	permit := domain.IdentityPinRemovalPermit{OperationID: "nin-rollback", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	calls := 0
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, domain.IdentityPinReceipt) error {
		calls++
		if calls == 1 {
			c.advance(time.Hour)
			s.mu.Lock()
			s.sampleNinQuotaLocked()
			s.mu.Unlock()
			return errors.New("external clean receipt uncertain")
		}
		return nil
	}).Times(2)
	result, err := s.removeIdentityPin(t.Context(), binding, preview.Token, fence, f)
	if err == nil || result.Outcome == domain.IdentityPinRemovalCommitted || !s.identityPinned(id) {
		t.Fatal("failed receipt was clean success", result, err)
	}
	saved := ninReadFileFixture(t, s)
	if saved.InboundQuota.rows[0].count != 1 || saved.InboundQuota.tick < s.ninJournal.tick || saved.IdentityMetadata[id].Inbound != entry.classMetadata().Inbound {
		t.Fatal("rollback lost current journal/class/progress")
	}
}
func TestInboundN0EndpointCommitAndRestorationRetainJournal(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "restore"}[changed], func(t *testing.T) {
			s, c, _ := ninUnknownFixture(t)
			id := domain.PeerIdentity{30}
			address := domain.PeerAddress("192.0.2.30:64646")
			if _, err := s.noteProvenOutbound(dialledAt(address), id, endpointProtectionNone()); err != nil {
				t.Fatal(err)
			}
			actual := s.writeFile
			writes := 0
			s.writeFile = func(raw []byte) error {
				writes++
				if changed && writes == 1 {
					c.advance(time.Hour)
					s.mu.Lock()
					key, _ := canonicalEndpoint(address)
					binding := s.endpoints[key]
					binding.identity = domain.PeerIdentity{31}
					s.replaceBindingLocked(storedBindingKey{address: address, canonical: &key}, binding)
					s.changes++
					s.mu.Unlock()
				}
				return actual(raw)
			}
			c.advance(time.Hour)
			_, err := s.removeGeneralBinding(address, operatorTestOp)
			if changed {
				if !errors.Is(err, domain.ErrEndpointBindingSelectionChanged) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			saved := ninReadFileFixture(t, s)
			if saved.Format != 3 || saved.InboundQuota.tick != uint64(time.Hour)*uint64(writes) {
				t.Fatal("endpoint full writer stripped/stalled history", writes, saved.InboundQuota)
			}
			if s.ninJournal.strictConfirmed() && s.ninJournal.phase == ninKnown {
				t.Fatal("ordinary endpoint write claimed strict ACK")
			}
		})
	}
}
func TestInboundN0K2OwnedCompletionAndMetadataCOWRetainJournal(t *testing.T) {
	service, peer := localContactFixture(t)
	_, stage, files := stagedContactRecord(t, service, peer)
	t.Cleanup(func() { _ = files.Close() })
	s := service.secureSessions.store
	c := new(ninFixtureClock)
	s.ninElapsed = c.read
	ninInstallUnknownFixture(t, s, files)
	c.advance(24 * time.Hour)
	if err := s.flushRenewalsNow(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	entry := s.identities[stage.owed.protection.binding.identity]
	originalOwner, originalMirror := entry.metadata.recordOwner, entry.metadata.recordMirror
	s.mu.Unlock()
	if _, err := s.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	entry = s.identities[stage.owed.protection.binding.identity]
	if entry.metadata.recordOwner != originalOwner || entry.metadata.recordMirror != originalMirror {
		t.Fatal("journal ACK completed/dropped K2 owner")
	}
	s.mu.Unlock()
	// A genuine inbound proof of the SAME earned K2 identity exercises its COW owner.
	w := ninProofForFixture(t, s, c, peer)
	c.advance(10 * time.Minute)
	if _, err := s.stageInboundPin(t.Context(), w, files); err != nil {
		t.Fatal(err)
	}
	if _, err := service.completeOwedContactRecord(t.Context(), stage, files); err != nil {
		t.Fatal(err)
	}
	saved := ninReadFileFixture(t, s)
	if saved.InboundQuota.rows[0].count != 1 || !s.ninJournal.strictConfirmed() {
		t.Fatal("K2 strict main did not preserve/cover counted content")
	}
	s.mu.Lock()
	entry = s.identities[stage.owed.protection.binding.identity]
	if entry.metadata.recordOwner != originalOwner || entry.metadata.recordMirror != contactRecordOwnerCompleted {
		t.Fatal("K2 completion not owned")
	}
	s.mu.Unlock()
}
func TestInboundN0SyntheticLegacyMetadataNotJournalAuthority(t *testing.T) {
	s, binding, _, _ := pinTransactionFixture(t)
	s.mu.Lock()
	entry := s.identities[binding.Identity]
	m := entry.classMetadata()
	m.Inbound = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginInboundSession, At: s.now(), SourceGroup: domain.SourceGroupLoopback}
	if err := entry.setUnpublishedMetadata(m); err != nil {
		t.Fatal(err)
	}
	s.putPinEntryLocked(binding.Identity, entry)
	s.changes++
	s.mu.Unlock()
	if err := s.persist(); err != nil {
		t.Fatal(err)
	}
	restarted := loadSecureSessionStore(filepath.Join(s.dir, s.name), newStoreClock().trusted())
	if restarted.ninJournal != nil || restarted.inboundAllocations != 1 || !reflect.DeepEqual(metadataSnapshot(t, restarted, binding.Identity).metadata, m) {
		t.Fatal("old class invented known history or lost historical metadata")
	}
}
func TestInboundN0ConfirmedSnapshotDoesNotAcknowledgeNewerContent(t *testing.T) {
	s, c, f, w := ninMatureFixture(t) // G1 is strictly confirmed; the in-flight writer may race first G2 admission.
	mockFiles := NewMockidentityPinDurableFiles(t)
	mockFiles.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	mockFiles.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		done := make(chan error, 1)
		go func() { _, err := s.stageInboundPin(ctx, w, f); done <- err }()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		c.advance(time.Minute)
		return f.WriteMain(ctx, raw)
	}).Once()
	if _, err := s.confirmPinMetadata(t.Context(), mockFiles); err != nil {
		t.Fatal(err)
	}
	if s.ninJournal.strictConfirmed() || !s.ninJournal.pending() {
		t.Fatal("old snapshot ACKed newer content")
	}
	if got := ninReadFileFixture(t, s); got.InboundQuota.rows[0].count != 0 || got.IdentityMetadata[w.binding.identity].Inbound.State != domain.IdentityPinAllocationAbsent {
		t.Fatal("old image not internally paired")
	}
	if _, err := s.confirmPinMetadata(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	if got := ninReadFileFixture(t, s); got.InboundQuota.rows[0].count != 1 || got.IdentityMetadata[w.binding.identity].Inbound.State != domain.IdentityPinAllocationEligible {
		t.Fatal("fresh image not internally paired")
	}
}
