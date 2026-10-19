package node

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func buildContactRecord(t *testing.T, peer *identity.Identity, seq domain.IdentityRecordSeq, minimum domain.IdentityRecordMinSession) protocol.SignedIdentityRecord {
	t.Helper()
	record, err := protocol.BuildSignedIdentityRecord(peer, protocol.IdentityRecordSpec{Network: domain.NetworkID(networkName), Seq: seq, IssuedAt: 1, DM: true, DTypes: domain.ExplicitDTypes([]domain.DType{"get_identity"}), MinSession: minimum})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func verifiedContactRecord(t *testing.T, s *Service, peer *identity.Identity) (*contactRecordCandidate, verifiedContactMinSessionRecord) {
	t.Helper()
	record := buildContactRecord(t, peer, 1, domain.DeclaredMinSession(domain.SessionVersionSecure))
	candidate, err := s.observeContactRecord(t.Context(), domain.PeerIdentityFromWire(peer.Address), record)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyContactMinSessionRecord(candidate)
	if err != nil {
		t.Fatal(err)
	}
	body, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), candidate.fact.identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.trust.rememberRecord(domain.NetworkID(networkName), record, body); err != nil {
		t.Fatal(err)
	}
	return candidate, verified
}

func stagedContactRecord(t *testing.T, s *Service, peer *identity.Identity) (*contactRecordCandidate, contactRecordStage, identityPinDurableFiles) {
	t.Helper()
	addLocalContact(t, s, peer)
	candidate, verified := verifiedContactRecord(t, s, peer)
	stage, err := s.stageContactRecord(t.Context(), verified)
	if err != nil {
		t.Fatal(err)
	}
	files, err := s.secureSessions.store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	return candidate, stage, files
}

func contactRecordFixture(t *testing.T, s *Service, peer *identity.Identity, seq domain.IdentityRecordSeq, minimum domain.IdentityRecordMinSession) protocol.SignedIdentityRecord {
	t.Helper()
	record, err := protocol.BuildSignedIdentityRecord(peer, protocol.IdentityRecordSpec{Network: domain.NetworkID(networkName), Seq: seq, IssuedAt: 1, DM: true, DTypes: domain.ExplicitDTypes([]domain.DType{"get_identity"}), MinSession: minimum})
	if err != nil {
		t.Fatal(err)
	}
	body, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.trust.rememberRecord(domain.NetworkID(networkName), record, body); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestContactRecordRealVerificationAndOriginalCapture(t *testing.T) {
	for _, variant := range []string{"valid", "signature", "foreign-network", "wrong-owner", "absent-min", "min1", "version", "unverified", "input-mutated-after-capture"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			record := buildContactRecord(t, peer, 1, domain.DeclaredMinSession(domain.SessionVersionSecure))
			switch variant {
			case "signature":
				record.Sig[0] ^= 1
			case "foreign-network":
				var err error
				record, err = protocol.BuildSignedIdentityRecord(peer, protocol.IdentityRecordSpec{Network: "foreign", Seq: 1, MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure)})
				if err != nil {
					t.Fatal(err)
				}
			case "wrong-owner":
				other, err := identity.Generate()
				if err != nil {
					t.Fatal(err)
				}
				record = buildContactRecord(t, other, 1, domain.DeclaredMinSession(domain.SessionVersionSecure))
			case "absent-min":
				record = buildContactRecord(t, peer, 1, domain.AbsentMinSession())
			case "min1":
				record = buildContactRecord(t, peer, 1, domain.DeclaredMinSession(domain.SessionVersionLegacy))
			case "version":
				record.Version++
			}
			candidate, err := s.observeContactRecord(t.Context(), domain.PeerIdentityFromWire(peer.Address), record)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "input-mutated-after-capture" {
				record.Body[0] ^= 1
				record.Sig[0] ^= 1
			}
			if variant == "unverified" {
				if _, err := s.stageContactRecord(t.Context(), verifiedContactMinSessionRecord{candidate: candidate}); !errors.Is(err, errContactRecordEvidence) {
					t.Fatal("raw capsule staged without actual verification", err)
				}
				return
			}
			verified, err := verifyContactMinSessionRecord(candidate)
			positive := variant == "valid" || variant == "input-mutated-after-capture"
			if (err == nil) != positive {
				t.Fatalf("verification=%v, positive=%t", err, positive)
			}
			if !positive {
				return
			}
			body, err := protocol.VerifyIdentityRecord(candidate.record, candidate.network, candidate.fact.identity)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.trust.rememberRecord(candidate.network, candidate.record, body); err != nil {
				t.Fatal(err)
			}
			stage, err := s.stageContactRecord(t.Context(), verified)
			if err != nil || stage.owed == nil {
				t.Fatal("actual signed min2 without TLS/TP did not stage", err)
			}
		})
	}
}

func TestContactRecordAuthorityAndOwnerABARefuseBeforeStage(t *testing.T) {
	for _, variant := range []string{"generic", "self", "zero", "oversize", "short-signature", "newer-min1", "contact-ABA", "owner-ABA", "absence-epoch", "cancel", "foreign-service"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := localContactFixture(t)
			id := domain.PeerIdentityFromWire(peer.Address)
			record := buildContactRecord(t, peer, 1, domain.DeclaredMinSession(domain.SessionVersionSecure))
			if variant != "generic" {
				addLocalContact(t, s, peer)
			}
			switch variant {
			case "self":
				id = domain.PeerIdentityFromWire(s.identity.Address)
			case "zero":
				id = domain.PeerIdentity{}
			case "oversize":
				record.Body = make([]byte, domain.MaxIdentityRecordBodyBytes+1)
			case "short-signature":
				record.Sig = record.Sig[:63]
			}
			if variant == "generic" || variant == "self" || variant == "zero" || variant == "oversize" || variant == "short-signature" {
				if _, err := s.observeContactRecord(t.Context(), id, record); err == nil {
					t.Fatal("unqualified observation accepted")
				}
				return
			}
			if variant == "owner-ABA" {
				if err := s.secureSessions.store.noteProvenInbound(id); err != nil {
					t.Fatal(err)
				}
			}
			_, verified := verifiedContactRecord(t, s, peer)
			ctx := t.Context()
			switch variant {
			case "newer-min1":
				contactRecordFixture(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionLegacy))
			case "contact-ABA":
				s.deleteTrustedContactFrame(id)
				addLocalContact(t, s, peer)
			case "owner-ABA":
				removeContactPinForTest(t, s, id)
				if err := s.secureSessions.store.noteProvenInbound(id); err != nil {
					t.Fatal(err)
				}
			case "absence-epoch":
				store := s.secureSessions.store
				store.pinMutationGate.Lock()
				store.mu.Lock()
				if err := store.advancePinRemovalEpochLocked(); err != nil {
					t.Fatal(err)
				}
				store.mu.Unlock()
				store.pinMutationGate.Unlock()
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "foreign-service":
				other, _ := localContactFixture(t)
				if _, err := other.stageContactRecord(ctx, verified); err == nil {
					t.Fatal("foreign service staged capsule")
				}
				return
			}
			if _, err := s.stageContactRecord(ctx, verified); err == nil {
				t.Fatal("stale original observation staged")
			}
			if s.secureSessions.store.contactKStatus(id) != domain.LocalContactKNotEvaluated {
				t.Fatal("refusal partially added K")
			}
		})
	}
}

func TestContactRecordStageOnceAndHistoricalIdempotence(t *testing.T) {
	s, peer := localContactFixture(t)
	candidate, stage, files := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	before := metadataSnapshot(t, store, id)
	history := stage.owed.protection
	if history.fact != candidate.fact || history.recordDigest != candidate.digest || history.recordSequence != 1 || history.evidenceRevision != before.evidenceRevision || history.protectionRevision != before.protectionRevision {
		t.Fatal("stage outcome lost its immutable original history/revisions")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			result, err := s.stageContactRecord(t.Context(), verifiedContactMinSessionRecord{candidate: candidate})
			if err != nil || result != stage {
				t.Error("copied verified handle re-staged or lost result", err)
			}
		})
	}
	group.Wait()
	if after := metadataSnapshot(t, store, id); after != before {
		t.Fatal("same candidate revised/debited twice")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := s.stageContactRecord(ctx, verifiedContactMinSessionRecord{candidate: candidate}); err != nil || result != stage {
		t.Fatal("late cancellation hid completed stage", err)
	}
	if result, ok := candidate.stagedOutcome(); !ok || result != stage {
		t.Fatal("staged outcome not retained")
	}
	completion, err := s.completeOwedContactRecord(t.Context(), stage, files)
	if err != nil || completion.state != contactRecordHistoricalProtectionCompleted || !completion.attemptCovered || completion.receipt.snapshotDigest == "" {
		t.Fatalf("completion %+v %v", completion, err)
	}
	s.deleteTrustedContactFrame(id)
	removeContactPinForTest(t, s, id)
	// Done is explicitly historical, not current trust or a request to re-pin.
	again, err := s.completeOwedContactRecord(ctx, stage, nil)
	expected := completion.replay()
	if err != nil || again != expected || store.identityPinned(id) {
		t.Fatal("historical completed result lost/re-pinned", err)
	}
}

func TestContactRecordStagePreservesOtherClassesAndOverflowAtomicity(t *testing.T) {
	for _, variant := range []string{"preserve", "evidence-overflow", "protection-overflow"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			store := s.secureSessions.store
			id := domain.PeerIdentityFromWire(peer.Address)
			if err := store.noteProvenInbound(id); err != nil {
				t.Fatal(err)
			}
			next := metadataSnapshot(t, store, id).metadata
			next.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: time.Now()}
			stageMetadata(t, store, id, next)
			_, verified := verifiedContactRecord(t, s, peer)
			before := metadataSnapshot(t, store, id)
			store.mu.Lock()
			pinnedAt := store.identities[id].pinnedAt
			if variant == "evidence-overflow" {
				store.evidenceRevision = math.MaxUint64
			}
			if variant == "protection-overflow" {
				store.protectionRevision = math.MaxUint64
			}
			evidence, protection, changes := store.evidenceRevision, store.protectionRevision, store.changes
			store.mu.Unlock()
			_, err := s.stageContactRecord(t.Context(), verified)
			if variant != "preserve" {
				if !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
					t.Fatal(err)
				}
				store.mu.Lock()
				defer store.mu.Unlock()
				if entry := store.identities[id]; entry.classMetadata() != before.metadata || entry.evidenceRevision != before.evidenceRevision || store.evidenceRevision != evidence || store.protectionRevision != protection || store.changes != changes {
					t.Fatal("overflow partially staged")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			after := metadataSnapshot(t, store, id)
			store.mu.Lock()
			sameTime := store.identities[id].pinnedAt.Equal(pinnedAt)
			rawR, err := store.relationAllocationsLocked()
			store.mu.Unlock()
			if err != nil || rawR != 1 || !sameTime || after.metadata.Relation != before.metadata.Relation || after.metadata.Contact.State != domain.IdentityPinAllocationEligible {
				t.Fatal("K erased another class/rawR/timestamp")
			}
		})
	}
}

func TestContactRecordDTypeOutputsPreserveDeclaredOwnership(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	for index, types := range []domain.DeclaredDTypeSet{domain.AbsentDTypes(), domain.ExplicitDTypes(nil), domain.ExplicitDTypes([]domain.DType{"get_identity"})} {
		record, err := protocol.BuildSignedIdentityRecord(peer, protocol.IdentityRecordSpec{Network: domain.NetworkID(networkName), Seq: domain.IdentityRecordSeq(index + 1), DTypes: types})
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.trust.rememberRecord(domain.NetworkID(networkName), record, parsed); err != nil {
			t.Fatal(err)
		}
		_, got, _ := s.trust.recordFor(domain.NetworkID(networkName), parsed.Address)
		if got.DTypes.Declaration() != types.Declaration() || got.DTypes.Len() != types.Len() {
			t.Fatal("absence/explicit-empty changed")
		}
		if fields := got.DTypes.WireField(); fields != nil && len(*fields) > 0 {
			(*fields)[0] = "changed"
		}
		if labels := got.DTypes.Types(); len(labels) > 0 {
			labels[0] = "changed"
		}
		for _, body := range s.trust.recordBodies(networkName) {
			if body.Address != parsed.Address {
				continue
			}
			if !reflect.DeepEqual(body.DTypes, types) {
				t.Fatal("getter/public DType slice mutated owner")
			}
		}
	}
}

func TestContactRecordUnsupportedPreflightMakesNoStage(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	_, verified := verifiedContactRecord(t, s, peer)
	store := s.secureSessions.store
	store.dir = "" // actual unsupported memory-only backend, no fake success
	if _, err := s.stageContactRecord(t.Context(), verified); !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatal(err)
	}
	if store.identityPinned(domain.PeerIdentityFromWire(peer.Address)) {
		t.Fatal("unsupported backend staged protection")
	}
}

// This compiles and fails on the actual359 baseline: the common merge and
// both persistent/session getters currently expose the owner's signed arrays.
func TestContactRecordInputAndGetterOwnership(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, output := range []bool{false, true} {
			name := "session_input"
			if persistent {
				name = "persistent_input"
			}
			if output {
				name += "_getter"
			}
			t.Run(name, func(t *testing.T) {
				s, peer := localContactFixture(t)
				if persistent {
					addLocalContact(t, s, peer)
				}
				record := contactRecordFixture(t, s, peer, 1, domain.DeclaredMinSession(domain.SessionVersionSecure))
				body, sig := bytes.Clone(record.Body), bytes.Clone(record.Sig)
				if output {
					var ok bool
					record, _, ok = s.trust.recordFor(domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
					if !ok {
						t.Fatal("actual stored record absent")
					}
				}
				record.Body[0] ^= 1
				record.Sig[0] ^= 1
				current, parsed, ok := s.trust.recordFor(domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
				if !ok || !bytes.Equal(current.Body, body) || !bytes.Equal(current.Sig, sig) {
					t.Fatal("external signed-record arrays mutated trust owner")
				}
				_, verifyErr := protocol.VerifyIdentityRecord(current, domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
				if parsed.Seq != 1 || verifyErr != nil {
					t.Fatal("owned record lost real signature/seq")
				}
			})
		}
	}
}
