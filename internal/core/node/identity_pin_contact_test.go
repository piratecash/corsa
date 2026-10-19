package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
	"github.com/stretchr/testify/mock"
)

func contactIntent(peer *identity.Identity) domain.LocalContactIntent {
	return domain.LocalContactIntent{Network: domain.NetworkID(networkName), Identity: domain.PeerIdentityFromWire(peer.Address), PubKey: domain.PeerPublicKey(identity.PublicKeyBase64(peer.PublicKey)), BoxKey: domain.PeerBoxKey(identity.BoxPublicKeyBase64(peer.BoxPublicKey)), BoxSignature: domain.PeerBoxSignature(identity.SignBoxKeyBinding(peer))}
}
func localContactFixture(t *testing.T) (*Service, *identity.Identity) {
	t.Helper()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	s := NewService(config.Node{ListenAddress: "127.0.0.1:0", TrustStorePath: filepath.Join(dir, "trust.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json"), PeersStatePath: filepath.Join(dir, "peers.json")}, owner, ebus.New())
	t.Cleanup(s.WaitBackground)
	return s, peer
}
func addLocalContact(t *testing.T, s *Service, peer *identity.Identity) domain.LocalContactIntentResult {
	t.Helper()
	result, err := s.AddLocalContact(t.Context(), contactIntent(peer))
	if err != nil || result.Fact != domain.LocalContactFactDurableConfirmed {
		t.Fatalf("local contact result %+v: %v", result, err)
	}
	return result
}
func publishContactProof(t *testing.T, s *Service, peer *identity.Identity) identityPinEntryBinding {
	t.Helper()
	actual := sessionv2test.ProvenPeer(t, peer)
	proof, ok := actual.Proof()
	if !ok {
		t.Fatal("no actual TLS proof")
	}
	binding, err := s.secureSessions.store.noteProvenInboundBinding(contactIntent(peer).Identity)
	if err != nil {
		t.Fatal(err)
	}
	s.noteLocalContactProof(proof, binding)
	return binding
}
func contactNetworkNode(t *testing.T, bootstrap []string, setup func(*Service)) (*Service, string, func()) {
	t.Helper()
	dir := t.TempDir()
	address := freeAddress(t)
	s, stop := startTestNodeWithSetup(t, config.Node{ListenAddress: address, BootstrapPeers: bootstrap, TrustStorePath: filepath.Join(dir, "trust.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json"), PeersStatePath: filepath.Join(dir, "peers.json"), Type: domain.NodeTypeFull}, setup)
	return s, address, stop
}
func TestK1RealOutboundAndInboundProofsUseOnlyExplicitFacts(t *testing.T) {
	listener, address, stop := contactNetworkNode(t, nil, withoutOwnDials(nil))
	t.Cleanup(stop)
	dialer, _, stopDialer := contactNetworkNode(t, []string{address}, func(s *Service) { addLocalContact(t, s, listener.identity); addLocalContact(t, listener, s.identity) })
	t.Cleanup(stopDialer)
	for _, pair := range []struct {
		s  *Service
		id domain.PeerIdentity
	}{{dialer, domain.PeerIdentityFromWire(listener.identity.Address)}, {listener, domain.PeerIdentityFromWire(dialer.identity.Address)}} {
		waitForConditionMsg(t, 10*time.Second, "fresh actual proof did not confirm K", func() bool {
			return pair.s.secureSessions.store.contactKStatus(pair.id) == domain.LocalContactKConfirmed
		})
	}
}
func TestK1OneShotProofCountsButGenericImportNeverDoes(t *testing.T) {
	listener, address, stop := contactNetworkNode(t, nil, withoutOwnDials(nil))
	t.Cleanup(stop)
	s, _, stopDialer := contactNetworkNode(t, nil, withoutOwnDials(nil))
	t.Cleanup(stopDialer)
	id := domain.PeerIdentityFromWire(listener.identity.Address)
	frame := protocol.Frame{Type: "import_contacts", Contacts: []protocol.ContactFrame{{Address: listener.identity.Address, PubKey: identity.PublicKeyBase64(listener.identity.PublicKey), BoxKey: identity.BoxPublicKeyBase64(listener.identity.BoxPublicKey), BoxSig: identity.SignBoxKeyBinding(listener.identity)}}}
	s.HandleLocalFrame(frame)
	if err := dialWithIntent(t, s, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
		t.Fatal(err)
	}
	if s.secureSessions.store.contactKStatus(id) != domain.LocalContactKNotEvaluated || s.trust.trustedContacts()[id.String()].LocalContact != nil {
		t.Fatal("generic import/old proof manufactured K")
	}
	result := addLocalContact(t, s, listener.identity)
	if result.K != domain.LocalContactKAwaitingFreshProof {
		t.Fatalf("old completed proof reused: %+v", result)
	}
	if err := dialWithIntent(t, s, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
		t.Fatal(err)
	}
	waitForConditionMsg(t, 10*time.Second, "one-shot fresh proof did not earn K", func() bool { return s.secureSessions.store.contactKStatus(id) == domain.LocalContactKConfirmed })
}
func TestK1FactSurvivesSourceOverwriteAndRestartWithoutInventingProof(t *testing.T) {
	s, peer := localContactFixture(t)
	id := contactIntent(peer).Identity
	addLocalContact(t, s, peer)
	before := s.trust.trustedContacts()[id.String()].LocalContact.fact
	s.trustContact(peer.Address, identity.PublicKeyBase64(peer.PublicKey), identity.BoxPublicKeyBase64(peer.BoxPublicKey), identity.SignBoxKeyBinding(peer), "import_contacts")
	record, body := issueTestRecord(t, peer, 3, true)
	if _, err := s.importVerifiedIdentityRecord(testRecordStoreNetwork, record, body); err != nil {
		t.Fatal(err)
	}
	after := s.trust.trustedContacts()[id.String()]
	if after.Source != trustContactSourceRecord || after.LocalContact.fact != before {
		t.Fatal("last writer overwrote local intent")
	}
	addLocalContact(t, s, peer)
	if s.trust.trustedContacts()[id.String()].LocalContact.fact != before {
		t.Fatal("explicit retry changed surviving fact")
	}
	loaded, err := loadTrustStore(s.trust.path, trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	state := loaded.trustedContacts()[id.String()].LocalContact
	if state == nil || state.fact != before || state.confirmed != 0 || !loaded.authorityObserved {
		t.Fatal("restart lost fact, invented ACK or forgot authority latch")
	}
	if s.secureSessions.store.contactKStatus(id) != domain.LocalContactKNotEvaluated {
		t.Fatal("contact/record alone created K")
	}
}
func TestK1UnsupportedStorageKeepsOrdinaryLiveContact(t *testing.T) {
	s, peer := localContactFixture(t)
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
	files.EXPECT().Close().Return(nil).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	result, err := s.AddLocalContact(t.Context(), contactIntent(peer))
	if !errors.Is(err, secretfile.ErrDurabilityUnsupported) || result.Contact != domain.ContactAppliedLive || result.Fact != domain.LocalContactFactUnsupported || result.K != domain.LocalContactKNotEvaluated {
		t.Fatalf("dishonest unsupported result %+v %v", result, err)
	}
	if row := s.trust.trustedContacts()[peer.Address]; row.Address == "" || row.LocalContact != nil || s.trust.authorityObserved {
		t.Fatal("unsupported backend blocked ordinary contact or staged authority")
	}
}
func TestK1LoadedUnconfirmedAuthorityNeverFallsBackToWeakWriter(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	loaded, err := loadTrustStore(s.trust.path, trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(loaded.path)
	if err != nil {
		t.Fatal(err)
	}
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Write(mock.Anything, mock.Anything).Return(secretfile.DurableNotCommitted, secretfile.ErrDurabilityUnsupported).Once()
	files.EXPECT().Close().Return(nil).Once()
	loaded.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	row := loaded.trustedContacts()[peer.Address]
	if row.LocalContact.confirmed != 0 {
		t.Fatal("fixture ACK not zero")
	}
	row.Source = "generic_refresh"
	if stored, err := loaded.remember(row); !stored || !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatalf("ordinary refresh silently weak-wrote: %v %v", stored, err)
	}
	after, err := os.ReadFile(loaded.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("loaded unconfirmed authority was overwritten through weak fallback")
	}
}
func TestK1IndeterminateStagingAlsoLatchesStrongWriter(t *testing.T) {
	s, peer := localContactFixture(t)
	original := s.trust.localFiles
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	files.EXPECT().Close().Return(nil).Times(3)
	injected := errors.New("commit ACK lost")
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		if _, err := real.Write(ctx, raw); err != nil {
			return secretfile.DurableNotCommitted, err
		}
		return secretfile.DurableIndeterminate, injected
	}).Once()
	files.EXPECT().Write(mock.Anything, mock.Anything).Return(secretfile.DurableNotCommitted, secretfile.ErrDurabilityUnsupported).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	result, err := s.AddLocalContact(t.Context(), contactIntent(peer))
	if !errors.Is(err, injected) || result.Fact != domain.LocalContactFactUnconfirmed || result.Contact != domain.ContactAppliedLive {
		t.Fatalf("unknown commit was misreported %+v %v", result, err)
	}
	row := s.trust.trustedContacts()[peer.Address]
	if row.LocalContact.confirmed != 0 || !s.trust.authorityObserved {
		t.Fatal("unknown commit falsely ACKed or failed to latch")
	}
	before, err := os.ReadFile(s.trust.path)
	if err != nil {
		t.Fatal(err)
	}
	row.Source = "refresh"
	if _, err := s.trust.remember(row); !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatal("staged authority allowed weaker fallback")
	}
	after, err := os.ReadFile(s.trust.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("weaker writer changed uncertain authority image")
	}
}
func TestK1UnsafeLoadedAuthorityCannotBeBlessedByResave(t *testing.T) {
	for _, mode := range []string{"world-writable-file", "unsafe-parent", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			path := s.trust.path
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "world-writable-file":
				if err := os.Chmod(path, 0o666); err != nil {
					t.Fatal(err)
				}
			case "unsafe-parent":
				if err := os.Chmod(filepath.Dir(path), 0o777); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := path + ".real"
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := loadTrustStore(path, trustedContact{}); err == nil {
				t.Fatal("forged/unsafe loaded authority was blessed")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("unsafe authority was overwritten")
			}
		})
	}
}
func TestK1AuthoritySchemaRejectsUnknownAndLegacyDisguises(t *testing.T) {
	for _, variant := range []string{"old-version", "unknown-fact-field", "unknown-version", "wrong-network", "wrong-identity"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			raw, err := os.ReadFile(s.trust.path)
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			fact := doc["contacts"].(map[string]any)[peer.Address].(map[string]any)["local_contact_intent"].(map[string]any)
			switch variant {
			case "old-version":
				doc["version"] = 3
			case "unknown-fact-field":
				fact["human"] = true
			case "unknown-version":
				fact["version"] = 2
			case "wrong-network":
				fact["network"] = "foreign"
			case "wrong-identity":
				fact["identity"] = s.identity.Address
			}
			raw, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(s.trust.path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadTrustStore(s.trust.path, trustedContact{}); err == nil {
				t.Fatal("unsupported authority was silently accepted/dropped")
			}
		})
	}
}
func TestK1OlderOrdinarySnapshotCannotEraseNewFact(t *testing.T) {
	s, peer := localContactFixture(t)
	s.trust.mu.Lock()
	old := s.trust.snapshotLocked()
	s.trust.mu.Unlock()
	addLocalContact(t, s, peer)
	if err := s.trust.saveSnapshot(old); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadTrustStore(s.trust.path, trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.trustedContacts()[peer.Address].LocalContact == nil {
		t.Fatal("older ordinary snapshot erased new fact")
	}
}
func TestK1FactAndPinCancellationAfterLocksPreventsNewStaging(t *testing.T) {
	s, peer := localContactFixture(t)
	intent := contactIntent(peer)
	s.trustContact(peer.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "generic")
	s.trust.saveMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.trust.stageLocalContact(ctx, intent.Identity); done <- err }()
	cancel()
	s.trust.saveMu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.trust.trustedContacts()[peer.Address].LocalContact != nil || s.trust.authorityObserved {
		t.Fatal("cancelled lock waiter staged authority")
	}
	binding, err := s.secureSessions.store.noteProvenInboundBinding(intent.Identity)
	if err != nil {
		t.Fatal(err)
	}
	s.secureSessions.store.pinMutationGate.Lock()
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		_, err := s.secureSessions.store.stageContactPin(ctx, binding, domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: time.Now()})
		done <- err
	}()
	cancel()
	s.secureSessions.store.pinMutationGate.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.secureSessions.store.contactKStatus(intent.Identity) != domain.LocalContactKNotEvaluated {
		t.Fatal("cancelled pin gate waiter classified K")
	}
}
func removeContactPinForTest(t *testing.T, s *Service, id domain.PeerIdentity) {
	t.Helper()
	store := s.secureSessions.store
	preview := pinRemovalPreview(t, store, id)
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentityFromWire(s.identity.Address), Store: "contact-store"}, RuntimeEpoch: "epoch", Generation: 1, Identity: id, ProofRevision: entry.evidenceRevision, PreviousPinnedAt: entry.pinnedAt, Backup: backup}
	permit := domain.IdentityPinRemovalPermit{OperationID: "remove-contact-pin", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	files, err := store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	result, err := store.removeIdentityPin(t.Context(), binding, preview.Token, fence, files)
	if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
		t.Fatalf("real C3 transaction %+v %v", result, err)
	}
}
func TestK1PendingFactIOCannotAttachOldProofToC3RecreatedEntry(t *testing.T) {
	s, peer := localContactFixture(t)
	original := s.trust.localFiles
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	files.EXPECT().Close().Return(nil).Twice()
	started, release := make(chan struct{}), make(chan struct{})
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		close(started)
		<-release
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		return real.Write(ctx, raw)
	}).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	done := make(chan error, 1)
	go func() { _, err := s.AddLocalContact(t.Context(), contactIntent(peer)); done <- err }()
	<-started
	old := publishContactProof(t, s, peer)
	removeContactPinForTest(t, s, old.identity)
	if _, err := s.secureSessions.store.noteProvenInboundBinding(old.identity); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.trust.localFiles = original
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.secureSessions.store.contactKStatus(old.identity) != domain.LocalContactKNotEvaluated {
		t.Fatal("old proof attached to a C3 recreated entry")
	}
	publishContactProof(t, s, peer)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.secureSessions.store.contactKStatus(old.identity) != domain.LocalContactKConfirmed {
		t.Fatal("genuine later proof failed to repin/classify")
	}
}
func TestK1DeleteReaddInvalidatesPendingFactAndRetainsEarnedK(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	binding := publishContactProof(t, s, peer)
	old := s.trust.trustedContacts()[peer.Address].LocalContact.proof
	s.deleteTrustedContactFrame(binding.identity)
	addLocalContact(t, s, peer)
	if err := s.contactPins.promote(t.Context(), old); !errors.Is(err, domain.ErrLocalContactFactStale) {
		t.Fatal("delete/readd revived old intent")
	}
	publishContactProof(t, s, peer)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, s.secureSessions.store, binding.identity)
	s.deleteTrustedContactFrame(binding.identity)
	if s.secureSessions.store.contactKStatus(binding.identity) != domain.LocalContactKConfirmed || !s.secureSessions.store.identityPinned(binding.identity) {
		t.Fatal("contact deletion stripped historical K/pin")
	}
	if err := s.secureSessions.store.validateIdentityPinTokenLocked(binding.identity, preview.Token); err != nil {
		t.Fatal("contact-only delete unexpectedly changed pin preview")
	}
}

func TestK1WorkerInventoryIsBoundedByPinsNotContactBook(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	publishContactProof(t, s, peer)
	other, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	intent := contactIntent(other)
	s.trustContact(other.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "generic")
	stamp, err := s.trust.stageLocalContact(t.Context(), intent.Identity)
	if err != nil {
		t.Fatal(err)
	}
	s.trust.mu.Lock()
	for n := 0; n < 30000; n++ {
		id := domain.PeerIdentity{0xee}
		binary.BigEndian.PutUint64(id[1:9], uint64(n))
		s.trust.contacts[id.String()] = trustedContact{Address: id.String(), Source: "generic_import"}
	}
	s.trust.mu.Unlock()
	facts, targets := s.contactPins.capture()
	if len(facts) != 1 || len(targets) != 0 {
		t.Fatalf("contact count expanded bounded inventory: %d %d", len(facts), len(targets))
	}
	s.trust.localFiles = func() (localContactDurableFiles, error) {
		t.Error("proofless staged fact triggered periodic full contact write")
		return nil, secretfile.ErrDurabilityUnsupported
	}
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.trust.localContactStatus(stamp) != domain.LocalContactFactUnconfirmed {
		t.Fatal("unrelated proofless fact received periodic ACK")
	}
}
func TestK1AuthorityStartupUnsupportedDoesNotEraseOrBlessFacts(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	before, err := os.ReadFile(s.trust.path)
	if err != nil {
		t.Fatal(err)
	}
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Write(mock.Anything, mock.Anything).Return(secretfile.DurableNotCommitted, secretfile.ErrDurabilityUnsupported).Once()
	files.EXPECT().Close().Return(nil).Once()
	loaded, err := loadTrustStoreWithFiles(s.trust.path, trustedContact{}, func() (localContactDurableFiles, error) { return files, nil })
	if err != nil {
		t.Fatal("valid unsupported authority stopped ordinary startup", err)
	}
	state := loaded.trustedContacts()[peer.Address].LocalContact
	if state == nil || state.confirmed != 0 || !loaded.authorityObserved {
		t.Fatal("unsupported startup erased/blessed authority")
	}
	after, err := os.ReadFile(s.trust.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("unsupported startup weak-wrote file")
	}
}
func TestK1LegacyNoAuthorityDoesNotRequireStrictWriter(t *testing.T) {
	s, peer := localContactFixture(t)
	s.trust.localFiles = func() (localContactDurableFiles, error) {
		t.Error("legacy ordinary writer forced strict backend")
		return nil, secretfile.ErrDurabilityUnsupported
	}
	intent := contactIntent(peer)
	s.trustContact(peer.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "generic")
	if s.trust.trustedContacts()[peer.Address].LocalContact != nil || s.trust.authorityObserved {
		t.Fatal("legacy generic path acquired authority")
	}
	raw, err := os.ReadFile(s.trust.path)
	if err != nil {
		t.Fatal(err)
	}
	var file trustFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if file.Contacts[peer.Address].Address != peer.Address {
		t.Fatal("legacy ordinary persistence was disabled")
	}
}
func TestK1LateCancellationCannotRevokeCommittedFact(t *testing.T) {
	s, peer := localContactFixture(t)
	original := s.trust.localFiles
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	files.EXPECT().Close().Return(nil).Twice()
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		out, err := real.Write(ctx, raw)
		cancel()
		return out, err
	}).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	result, err := s.AddLocalContact(ctx, contactIntent(peer))
	if err != nil || result.Fact != domain.LocalContactFactDurableConfirmed || ctx.Err() == nil {
		t.Fatalf("committed result lost to late cancel: %+v %v", result, err)
	}
}
func TestK1ContactSnapshotsCannotAliasAuthority(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	publishContactProof(t, s, peer)
	before := s.trust.trustedContacts()[peer.Address].LocalContact.fact
	rows := s.trust.trustedContacts()
	rows[peer.Address].LocalContact.fact.At = time.Time{}
	rows[peer.Address].LocalContact.proof.binding = identityPinEntryBinding{}
	s.trust.mu.Lock()
	snapshot := s.trust.snapshotLocked()
	s.trust.mu.Unlock()
	snapshot.contacts[peer.Address].LocalContact.fact.ID = localContactFactID{}
	state := s.trust.trustedContacts()[peer.Address].LocalContact
	if state.fact != before || state.proof.binding.incarnation == 0 {
		t.Fatal("returned/snapshot alias mutated owner authority")
	}
}
func TestK1ClassifiesExistingIdentityAtGlobalCapacityAndPreservesC3Reserve(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	binding := publishContactProof(t, s, peer)
	store := s.secureSessions.store
	store.mu.Lock()
	for n := 0; len(store.identities) < maxPinnedIdentities; n++ {
		id := domain.PeerIdentity{0xdd}
		binary.BigEndian.PutUint64(id[1:9], uint64(n))
		if _, err := store.pinLocked(id, store.now()); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
	}
	store.mu.Unlock()
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if store.contactKStatus(binding.identity) != domain.LocalContactKConfirmed {
		t.Fatal("K used a second global place")
	}
	newID := domain.PeerIdentity{0xaa}
	if _, err := store.noteProvenInboundBinding(newID); !errors.Is(err, errPinStoreFull) {
		t.Fatal("new K-capable identity bypassed global bound")
	}
	removeContactPinForTest(t, s, binding.identity)
	if _, err := store.noteProvenInboundBinding(newID); !errors.Is(err, errPinStoreFull) {
		t.Fatal("C3 reservation became free global capacity")
	}
	if _, err := store.noteProvenInboundBinding(binding.identity); err != nil {
		t.Fatal("same identity could not consume own C3 reservation", err)
	}
}
func TestK1MetadataInvalidatesPreviewWithoutPretendingAnotherProof(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	binding := publishContactProof(t, s, peer)
	store := s.secureSessions.store
	preview := pinRemovalPreview(t, store, binding.identity)
	store.mu.Lock()
	evidenceRevision := store.identities[binding.identity].evidenceRevision
	store.mu.Unlock()
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.identities[binding.identity].evidenceRevision != evidenceRevision {
		t.Fatal("K staging fabricated transport proof")
	}
	if err := store.validateIdentityPinTokenLocked(binding.identity, preview.Token); !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatal("K metadata changed without invalidating old preview")
	}
}

func TestK1LegacyNodeKeepsOrdinaryContactWithoutV2Store(t *testing.T) {
	s, peer := localContactFixture(t)
	asOldNode(s)
	result, err := s.AddLocalContact(t.Context(), contactIntent(peer))
	if err != nil || result.Contact != domain.ContactAppliedLive || result.Fact != domain.LocalContactFactDurableConfirmed || result.K != domain.LocalContactKNotEvaluated {
		t.Fatalf("legacy contact result %+v %v", result, err)
	}
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestK1ProtectedRereadConsumesOnlyExpectedBytesPlusOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trust.json")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	initial := []byte("initial")
	if err := readLocalContactAuthorityContent(file, initial); !errors.Is(err, domain.ErrLocalContactIntent) {
		t.Fatalf("changed authority accepted: %v", err)
	}
	offset, err := file.Seek(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if offset > int64(len(initial)+1) {
		t.Fatalf("protected reread consumed %d bytes; expected at most %d", offset, len(initial)+1)
	}
}
