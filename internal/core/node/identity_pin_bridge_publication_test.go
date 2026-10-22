package node

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
)

// Direct store/leaf composition only: this does not construct a live Node or
// claim that its trust/F1/intake writer family has an installed domain.
func pinPublicationFixture(t *testing.T, seal bool) (*secureSessionStore, findnodebridge.FixtureDomain, *atomic.Int64, chan struct{}) {
	t.Helper()
	clock, done := new(atomic.Int64), make(chan struct{})
	d, err := findnodebridge.NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	s := newSecureSessionStoreWithPublisher(newStoreClock().trusted(), zerolog.Nop(), d.Publisher())
	if seal {
		if err := d.Publisher().SealConstruction(); err != nil {
			t.Fatal(err)
		}
	}
	return s, d, clock, done
}
func pinPublicationCapture(t *testing.T, d findnodebridge.FixtureDomain) findnodebridge.CaptureStamp {
	t.Helper()
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	return stamp
}
func pinPublicationRefused(d findnodebridge.FixtureDomain, stamp findnodebridge.CaptureStamp) bool {
	read, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		return true
	}
	read.Release()
	return false
}
func pinPublicationEntry(t *testing.T) identityPinEntry {
	t.Helper()
	at := newStoreClock().now.UTC()
	value := domain.IdentityPinMetadata{
		Selected: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginSelectedSession, At: at, SourceGroup: domain.SourceGroupLoopback, Reference: "selected"},
		Inbound:  domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginInboundSession, At: at, SourceGroup: domain.SourceGroupLoopback, Reference: "inbound"},
		Relation: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: at, Reference: "relation"},
	}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	// This is a raw memory-leaf fixture, not proof of live class eligibility.
	return identityPinEntry{pinnedAt: at, evidenceRevision: 1, protectionRevision: 1, incarnation: 1, metadata: &identityPinMetadataState{value: value, confirmedRevision: 1}}
}
func pinPublicationTallies(s *secureSessionStore) [6]int {
	return [6]int{s.inboundAllocations, s.inboundConfirmed, s.selectedAllocations, s.selectedConfirmed, s.relationAllocations, s.relationConfirmed}
}
func TestPinBridgeOrdinaryZeroAndConstructingPublication(t *testing.T) {
	plain := newSecureSessionStore(newStoreClock().trusted(), zerolog.Nop())
	if plain.pinPublisher != (findnodebridge.Publisher{}) || plain.beginPinPublication() != nil {
		t.Fatal("ordinary store attached a publisher or allocated a noop lease")
	}
	id, entry := domain.PeerIdentity{201}, pinPublicationEntry(t)
	plain.mu.Lock()
	plain.putPinEntryLocked(id, entry)
	plain.deletePinEntryLocked(id)
	if len(plain.identities) != 0 || pinPublicationTallies(plain) != ([6]int{}) {
		t.Fatal("zero publisher skipped mandatory map or tally mutation")
	}
	plain.mu.Unlock()
	s, d, _, _ := pinPublicationFixture(t, false)
	s.mu.Lock()
	s.putPinEntryLocked(id, entry)
	s.mu.Unlock()
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("constructing store became Ready through publication")
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	stamp := pinPublicationCapture(t, d)
	s.mu.Lock()
	present := s.identities[id] == entry
	s.deletePinEntryLocked(id)
	s.mu.Unlock()
	if !present || !pinPublicationRefused(d, stamp) {
		t.Fatal("construction or deletion failed to publish original map entry")
	}
}
func TestPinBridgeMapReplacementTalliesAndGeneration(t *testing.T) {
	s, d, _, _ := pinPublicationFixture(t, true)
	id, entry := domain.PeerIdentity{202}, pinPublicationEntry(t)
	before := pinPublicationCapture(t, d)
	s.mu.Lock()
	s.putPinEntryLocked(id, entry)
	got, counts := s.identities[id], pinPublicationTallies(s)
	s.mu.Unlock()
	if got != entry || counts != ([6]int{1, 1, 1, 1, 1, 1}) {
		t.Fatal("publication changed original map or six derived tallies")
	}
	if !pinPublicationRefused(d, before) {
		t.Fatal("put preserved a stale capture")
	}
	before = pinPublicationCapture(t, d)
	replacement := entry
	replacement.metadata = nil
	s.mu.Lock()
	s.putPinEntryLocked(id, replacement)
	got, counts = s.identities[id], pinPublicationTallies(s)
	s.mu.Unlock()
	if got != replacement || counts != ([6]int{}) || !pinPublicationRefused(d, before) {
		t.Fatal("replacement lost tally subtraction or generation invalidation")
	}
	before = pinPublicationCapture(t, d)
	s.mu.Lock()
	s.deletePinEntryLocked(id)
	_, present := s.identities[id]
	counts = pinPublicationTallies(s)
	s.mu.Unlock()
	if present || counts != ([6]int{}) || !pinPublicationRefused(d, before) {
		t.Fatal("delete preserved map, tallies or stale capture")
	}
}
func pinPublicationPutWorker(s *secureSessionStore, id domain.PeerIdentity, entry identityPinEntry, entered, finished chan struct{}) {
	s.mu.Lock()
	close(entered)
	s.putPinEntryLocked(id, entry)
	s.mu.Unlock()
	close(finished)
}
func pinPublicationWaitStack(t *testing.T, needle string) {
	t.Helper()
	waitForConditionMsg(t, 2*time.Second, "publication writer did not reach exact awaited lock", func() bool {
		raw := make([]byte, 1<<20)
		n := runtime.Stack(raw, true)
		for _, stack := range strings.Split(string(raw[:n]), "\n\n") {
			if strings.Contains(stack, "pinPublicationPutWorker") && strings.Contains(stack, needle) {
				return true
			}
		}
		return false
	})
}
func TestPinBridgeStoreWaitPrecedesPublicationReservation(t *testing.T) {
	s, d, _, _ := pinPublicationFixture(t, true)
	entry := pinPublicationEntry(t)
	stamp := pinPublicationCapture(t, d)
	entered, finished := make(chan struct{}), make(chan struct{})
	s.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			s.mu.Unlock()
		}
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("store-wait worker cleanup did not join")
		}
	}()
	go pinPublicationPutWorker(s, domain.PeerIdentity{203}, entry, entered, finished)
	// The writer is parked at store.mu, before its nonclock publication W.
	pinPublicationWaitStack(t, "sync.(*Mutex).lockSlow")
	read, err := d.Consumer().TryReadClock(stamp)
	if err == nil {
		read.Release()
	}
	s.mu.Unlock()
	unlocked = true
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("store-wait writer did not join")
	}
	if err != nil {
		t.Fatal("writer held publication W while waiting for store.mu", err)
	}
}
func TestPinBridgeReadReservationBlocksOnlyFinalMemoryPublication(t *testing.T) {
	s, d, _, _ := pinPublicationFixture(t, true)
	entry := pinPublicationEntry(t)
	stamp := pinPublicationCapture(t, d)
	read, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	entered, finished := make(chan struct{}), make(chan struct{})
	defer func() {
		if !released {
			read.Release()
		}
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("publication worker cleanup did not join")
		}
	}()
	go pinPublicationPutWorker(s, domain.PeerIdentity{204}, entry, entered, finished)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer failed to own store before publication")
	}
	pinPublicationWaitStack(t, "findnodebridge.Publisher.BeginPublication")
	select {
	case <-finished:
		t.Fatal("read reservation did not block final memory publication")
	default:
	}
	// No store lock acquisition or map read has occurred while this R is held.
	read.Release()
	released = true
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("publication writer did not join after reservation release")
	}
	s.mu.Lock()
	_, present := s.identities[domain.PeerIdentity{204}]
	s.mu.Unlock()
	if !present || !pinPublicationRefused(d, stamp) {
		t.Fatal("released writer failed publication or generation invalidation")
	}
}
func TestPinBridgeMandatoryMapCleanupAfterPoisonAndClosedDone(t *testing.T) {
	s, d, clock, done := pinPublicationFixture(t, true)
	clock.Store(5)
	observation, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observation.Sample(); err != nil {
		t.Fatal(err)
	}
	observation.Release()
	close(done)
	clock.Store(4)
	observation, err = d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observation.Sample(); err == nil {
		t.Fatal("fixture failed to poison regressing clock")
	}
	observation.Release()
	id, entry := domain.PeerIdentity{205}, pinPublicationEntry(t)
	s.mu.Lock()
	s.putPinEntryLocked(id, entry)
	present := s.identities[id] == entry
	s.deletePinEntryLocked(id)
	count := len(s.identities)
	tallies := pinPublicationTallies(s)
	s.mu.Unlock()
	if !present || count != 0 || tallies != ([6]int{}) {
		t.Fatal("poison or closed Done vetoed mandatory put/delete cleanup")
	}
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("mandatory cleanup revived poisoned observation")
	}
}
func pinPublicationDurable(t *testing.T) (*secureSessionStore, findnodebridge.FixtureDomain, identityPinEntryBinding, identityPinDurableFiles) {
	t.Helper()
	s, d, _, _ := pinPublicationFixture(t, true)
	s.dir, s.name, s.markerWritten = t.TempDir(), "pins.json", true
	s.writeFile = s.writeToDisk
	id := domain.PeerIdentity{206}
	if err := s.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	files, err := s.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	if err := files.Preflight(t.Context()); errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Skip("strict durable backend unavailable")
	} else if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	binding := s.pinEntryBindingLocked(id)
	s.mu.Unlock()
	return s, d, binding, files
}
func TestPinBridgeActualStrictACKCopiesMetadataAndInvalidatesCut(t *testing.T) {
	s, d, binding, files := pinPublicationDurable(t)
	if _, err := s.stageSelectedPin(t.Context(), binding, selectedAllocation(t)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	old := s.identities[binding.identity].metadata
	s.mu.Unlock()
	stamp := pinPublicationCapture(t, d)
	ack, err := s.confirmPinMetadata(t.Context(), files)
	if err != nil || ack.confirmed != 1 {
		t.Fatal("actual strict snapshot did not acknowledge exact revision", ack, err)
	}
	s.mu.Lock()
	current := s.identities[binding.identity].metadata
	s.mu.Unlock()
	if old == current || old.confirmedRevision != 0 || current.confirmedRevision == 0 || old.value != current.value {
		t.Fatal("strict publication mutated previously published metadata")
	}
	if !pinPublicationRefused(d, stamp) {
		t.Fatal("actual strict ACK failed to invalidate captured cut")
	}
	restarted := loadSecureSessionStore(filepath.Join(s.dir, s.name), newStoreClock().trusted())
	if !restarted.identityPinned(binding.identity) || restarted.identities[binding.identity].classMetadata() != old.value {
		t.Fatal("strict publication changed persisted protection or metadata")
	}
}
func TestPinBridgeStrictStaleRevisionDoesNotPublishNewerACK(t *testing.T) {
	s, d, binding, real := pinPublicationDurable(t)
	if _, err := s.stageSelectedPin(t.Context(), binding, selectedAllocation(t)); err != nil {
		t.Fatal(err)
	}
	before := metadataSnapshot(t, s, binding.identity)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	var newer findnodebridge.CaptureStamp
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		next := before.metadata
		next.Selected.Reference = "cm:newer:revision"
		if _, err := s.stagePinMetadata(before, next); err != nil {
			t.Fatal(err)
		}
		newer = pinPublicationCapture(t, d)
		return real.WriteMain(ctx, raw)
	}).Once()
	ack, err := s.confirmPinMetadata(t.Context(), files)
	if err != nil || ack.confirmed != 0 || s.selectedPinStatus(binding).confirmed {
		t.Fatal("stale strict snapshot acknowledged newer revision", ack, err)
	}
	// No blanket W on ACK loops: a skipped stale entry has no publication.
	read, err := d.Consumer().TryReadClock(newer)
	if err != nil {
		t.Fatal("stale ACK invented an entry publication", err)
	}
	read.Release()
	if _, err := s.confirmPinMetadata(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	if !s.selectedPinStatus(binding).confirmed || !pinPublicationRefused(d, newer) {
		t.Fatal("retry failed exact current ACK publication")
	}
}
func TestPinBridgeFatalRollbackPublicationPreservesProtection(t *testing.T) {
	s, d, _, _ := pinPublicationFixture(t, true)
	id := domain.PeerIdentity{207}
	if err := s.noteProvenInbound(id); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	entry := s.identities[id]
	s.mu.Unlock()
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	permit := domain.IdentityPinRemovalPermit{OperationID: "fatal-publication", Generation: 2, Binding: domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{208}, Store: "pin-publisher"}, RuntimeEpoch: "fixture", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt, Backup: backup}}
	files := NewMockidentityPinDurableFiles(t)
	fence := NewMockIdentityPinRemovalFence(t)
	injected := errors.New("strict rollback failed")
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, injected).Once()
	stamp := pinPublicationCapture(t, d)
	result, err := s.rollbackIdentityPinRemoval(t.Context(), permit, fence, files, errors.New("initial commit failed"))
	if !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) || !errors.Is(err, injected) || result.Outcome != domain.IdentityPinRemovalRecoveryRequired {
		t.Fatal("fatal rollback lost its original required refusal", result, err)
	}
	s.mu.Lock()
	kept := s.identities[id] == entry
	refusal := s.pinMetadataMutationRefusalLocked()
	fatal := s.pinRecovery != nil && s.pinRecovery.fatal
	s.mu.Unlock()
	if !kept || !fatal || !errors.Is(refusal, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatal("fatal publication lost original pin or refusal")
	}
	if !pinPublicationRefused(d, stamp) {
		t.Fatal("fatal rollback preserved a stale captured cut")
	}
}
