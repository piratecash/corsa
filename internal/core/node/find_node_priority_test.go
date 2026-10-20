package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

// Small explicit proposal inputs, not deployment defaults or measurements.
func c5Profile() findNodePriorityProfile {
	return findNodePriorityProfile{MaxMembers: 16, MaxRegistryVisits: 32}
}
func c5Service(t *testing.T) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull, TrustStorePath: filepath.Join(dir, "trust.json"), SecureSessionStorePath: filepath.Join(dir, "pins.json")})
	s, err := newFindNodeCandidateService(cfg, id, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Real files are chosen before construction; K and N-sel use normal
	// strict writes. Fixture teardown retains the existing joined path.
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}
func c5Accepted(t *testing.T, s *Service, d *Service) (f1ProofFixture, findnode.Session) {
	t.Helper()
	f := f1PairOn(t, true, nil, s, d)
	s.publishAcceptedProofTransport(f.inbound)
	m, err := s.findNodePriorityMembers(t.Context(), c5Profile())
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range m.sessions {
		if member.ConnID() == f.inbound.id {
			return f, member
		}
	}
	t.Fatal("actual accepted F1 member absent")
	return f, findnode.Session{}
}
func c5ReadPriority(t *testing.T, s *Service, member findnode.Session, want findnode.Priority) {
	t.Helper()
	out, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253, 54}, c5Profile())
	if err != nil || coverage != registeredAcceptedAndCMPostSetup {
		t.Fatal("C5 observation failed", coverage, err)
	}
	got, found := out.Priority(member)
	if !found || got != want {
		t.Fatal("wrong genuine member priority", got, want, found)
	}
}
func c5ReadNotReady(t *testing.T, s *Service) {
	t.Helper()
	_, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253, 54}, c5Profile())
	if !errors.Is(err, findnode.ErrNotReady) || coverage != 0 {
		t.Fatal("unconfirmed class became a completed observation", coverage, err)
	}
}
func c5SelectedCM(t *testing.T) (f1ProofFixture, findnode.Session, *selectedTestClock) {
	t.Helper()
	s, d := c5Service(t), c5Service(t)
	clock := &selectedTestClock{base: time.Now()}
	d.selectedPins.clock = clock.Now // before real proof/CM/background work
	f := f1PairOn(t, true, nil, s, d)
	if f.outbound.selectedProof == nil || f.outbound.selectedGeneration != 0 {
		t.Fatal("genuine original selected proof missing before CM publication")
	}
	f1StartActualCM(t, f, 9)
	waitFor(t, 2*time.Second, "C5 genuine CM member", func() bool { return len(f1Members(t, d).sessions) == 1 })
	m, err := d.findNodePriorityMembers(t.Context(), c5Profile())
	if err != nil || len(m.sessions) != 1 {
		t.Fatal("genuine CM capture", err)
	}
	return f, m.sessions[0], clock
}
func c5InboundService(t *testing.T) (*Service, *ninFixtureClock) {
	t.Helper()
	clock := new(ninFixtureClock)
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull, SecureSessionStorePath: filepath.Join(t.TempDir(), "pins.json")})
	seed := ninOpenFixture(t, cfg.SecureSessionStorePath, clock)
	ninInstallVirginFixture(t, seed, ninFilesFixture(t, seed))
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	s, err := newFindNodeInboundCandidateService(cfg, id, nil, inboundPinClock{elapsed: clock.read})
	if err != nil {
		t.Fatal(err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s, clock
}

func TestFindNodeC5GenuineKAndHistoricalReferenceReadd(t *testing.T) {
	s, d := c5Service(t), c5Service(t)
	addLocalContact(t, s, d.identity)
	_, member := c5Accepted(t, s, d)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, s, member, findnode.PriorityProtected)
	id := member.Identity()
	store := s.localContactPinStore()
	store.mu.Lock()
	original := store.identities[id].classMetadata().Contact
	changes := store.changes
	store.mu.Unlock()
	if removed, err := s.trust.forget(id); err != nil || !removed {
		t.Fatal("actual contact delete", removed, err)
	}
	c5ReadPriority(t, s, member, findnode.PriorityNew)
	intent := contactIntent(d.identity) // generic import cannot mint an explicit local fact
	if stored, err := s.applyTrustedContactContext(t.Context(), intent.Identity.String(), string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "c5_generic_import"); err != nil || !stored {
		t.Fatal("generic contact import", stored, err)
	}
	c5ReadPriority(t, s, member, findnode.PriorityNew)
	addLocalContact(t, s, d.identity)
	s.trust.mu.RLock()
	fact := s.trust.contacts[id.String()].LocalContact
	sameReference := original.Reference == domain.IdentityPinEvidenceReference(fact.fact.ID.String())
	s.trust.mu.RUnlock()
	if sameReference {
		t.Fatal("delete/readd fixture did not create a new explicit fact")
	}
	c5ReadPriority(t, s, member, findnode.PriorityProtected)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.identities[id].classMetadata().Contact != original || store.changes != changes {
		t.Fatal("read rewrote historical K metadata")
	}
}

func TestFindNodeC5CMSelectedRequiresActualMaturityAndStrictACK(t *testing.T) {
	f, member, clock := c5SelectedCM(t)
	c5ReadPriority(t, f.dialer, member, findnode.PriorityNew)
	clock.advance(selectedSessionAge)
	facts := f.dialer.selectedPins.capture(clock.Now())
	if len(facts) != 1 {
		t.Fatal("actual mature CM fact absent")
	}
	unknown := NewMockidentityPinDurableFiles(t)
	unknown.EXPECT().Preflight(mock.Anything).Return(nil).Twice()
	unknown.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, nil).Once()
	if err := f.dialer.selectedPins.promote(t.Context(), facts, unknown); err == nil {
		t.Fatal("Unknown strict write claimed confirmation")
	}
	c5ReadNotReady(t, f.dialer)
	store := f.dialer.localContactPinStore()
	real := ninFilesFixture(t, store)
	if _, err := store.confirmPinMetadata(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityProtected)
	// A later ordinary metadata change makes old current-revision ACK insufficient.
	before := metadataSnapshot(t, store, member.Identity())
	next := before.metadata
	next.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: time.Now(), Reference: "c5-mixed"}
	if _, err := store.stagePinMetadata(before, next); err != nil {
		t.Fatal(err)
	}
	c5ReadNotReady(t, f.dialer)
	if _, err := store.confirmPinMetadata(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityProtected)
}

func TestFindNodeC5IncomingAllMembersAndTargetCannotStealOldestPlace(t *testing.T) {
	s, clock := c5InboundService(t)
	members := make([]findnode.Session, 0, 7)
	for i := 0; i < 7; i++ {
		_, member := c5Accepted(t, s, c5Service(t))
		members = append(members, member)
	}
	clock.advance(10 * time.Minute)
	for range members {
		if err := s.inboundPins.pass(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	out, coverage, err := s.findNodePrioritySnapshot(t.Context(), members[0].Identity(), c5Profile())
	if err != nil || coverage != registeredAcceptedAndCMPostSetup {
		t.Fatal("actual confirmed N-in observation", err)
	}
	if _, found := out.Priority(members[0]); found {
		t.Fatal("target link became requester")
	}
	for i, member := range members[1:] {
		want := findnode.PriorityAged
		if i >= 4 {
			want = findnode.PriorityNew
		}
		if got, found := out.Priority(member); !found || got != want {
			t.Fatal("target link stole requester L2 place", i, got, want, found)
		}
	}
	// There are no TargetCache/pending rows: enumeration comes from real F1 owners.
	s.peerMu.RLock()
	life := s.connEntryByIDLocked(members[1].ConnID()).common
	s.peerMu.RUnlock()
	life.end()
	again, _, err := s.findNodePrioritySnapshot(t.Context(), members[0].Identity(), c5Profile())
	if err != nil {
		t.Fatal(err)
	}
	if got, found := again.Priority(members[5]); !found || got != findnode.PriorityAged {
		t.Fatal("closed oldest did not free next live L2", got, found)
	}
}

func TestFindNodeC5ReadBoundExactMAndOverflowHasNoPartialSuccess(t *testing.T) {
	s := c5Service(t)
	_, a := c5Accepted(t, s, c5Service(t))
	_, b := c5Accepted(t, s, c5Service(t))
	profile := findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 2}
	out, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, profile)
	if err != nil || coverage == 0 {
		t.Fatal("exact M/V full set refused", err)
	}
	if _, ok := out.Priority(a); !ok {
		t.Fatal("complete read lost first member")
	}
	if _, ok := out.Priority(b); !ok {
		t.Fatal("complete read lost second member")
	}
	profile.MaxMembers = 1
	out, coverage, err = s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, profile)
	if !errors.Is(err, findnode.ErrCapacity) || coverage != 0 {
		t.Fatal("M overflow returned truncated success", coverage, err)
	}
	if _, ok := out.Priority(a); ok {
		t.Fatal("M overflow leaked partial snapshot")
	}
	// Target links are counted before filtering, not exempted from the cap.
	_, coverage, err = s.findNodePrioritySnapshot(t.Context(), a.Identity(), profile)
	if !errors.Is(err, findnode.ErrCapacity) || coverage != 0 {
		t.Fatal("target bypassed member workspace cap", err)
	}
}

func TestFindNodeC5ReadVisitBoundIncludesNonF1RegistryKeys(t *testing.T) {
	s := c5Service(t)
	_, member := c5Accepted(t, s, c5Service(t))
	// Explicit adversarial registry-only keys, not fabricated proof/membership.
	s.peerMu.Lock()
	s.conns[domain.ConnID(9001)] = nil
	s.conns[domain.ConnID(9002)] = nil
	s.peerMu.Unlock()
	t.Cleanup(func() {
		s.peerMu.Lock()
		delete(s.conns, domain.ConnID(9001))
		delete(s.conns, domain.ConnID(9002))
		s.peerMu.Unlock()
	})
	out, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 3})
	if err != nil || coverage == 0 {
		t.Fatal("exact visit bound refused", err)
	}
	if _, ok := out.Priority(member); !ok {
		t.Fatal("non-F1 registry displaced actual member")
	}
	out, coverage, err = s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2})
	if !errors.Is(err, findnode.ErrCapacity) || coverage != 0 {
		t.Fatal("registry overflow became subset success", coverage, err)
	}
	if _, ok := out.Priority(member); ok {
		t.Fatal("visit overflow leaked partial snapshot")
	}
}

func TestFindNodeC5InvalidProfileAndOrdinaryDefaultRefuse(t *testing.T) {
	s := c5Service(t)
	for _, p := range []findNodePriorityProfile{{}, {MaxMembers: -1, MaxRegistryVisits: 1}, {MaxMembers: 2, MaxRegistryVisits: 1}, {MaxMembers: 1, MaxRegistryVisits: int(^uint(0) >> 1)}, {MaxMembers: int(^uint(0)>>1) - 1, MaxRegistryVisits: int(^uint(0)>>1) - 1}} {
		if _, _, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, p); !errors.Is(err, findnode.ErrInvalid) {
			t.Fatal("invalid finite profile accepted", p, err)
		}
	}
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ordinary := NewService(configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0"}), id, nil)
	t.Cleanup(func() { ordinary.runCancel(); ordinary.closeBackgroundAdmission(); ordinary.WaitBackground() })
	if ordinary.findNode != nil {
		t.Fatal("normal constructor activated adapter")
	}
	if _, _, err := ordinary.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, c5Profile()); !errors.Is(err, findnode.ErrInvalid) {
		t.Fatal("profile activated ordinary Service", err)
	}
	f := f1Pair(t, false)
	if len(f1Members(t, f.accepted).sessions) != 0 {
		t.Fatal("actual default capability invented Session")
	}
}

func TestFindNodeC5CopiedProfileAndCancellationAfterRegistryWait(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "copied", true: "cancelled"}[cancelled], func(t *testing.T) {
			s := c5Service(t)
			c5Accepted(t, s, c5Service(t))
			c5Accepted(t, s, c5Service(t))
			p := findNodePriorityProfile{MaxMembers: 1, MaxRegistryVisits: 2}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s.peerMu.Lock()
			held := true
			done := make(chan struct{})
			gid := make(chan uint64, 1)
			var actualErr error
			defer func() {
				if held {
					s.peerMu.Unlock()
				}
				f1Await(t, done, "C5 bounded read join")
			}()
			go func(copied findNodePriorityProfile) {
				defer close(done)
				gid <- relationGateCurrentGID()
				_, _, actualErr = s.findNodePrioritySnapshot(ctx, domain.PeerIdentity{253}, copied)
			}(p)
			n1WaitMutexFrame(t, <-gid, "findNodePriorityMembers(")
			p.MaxMembers = 2 // an immutable by-value invocation cannot acquire this change
			if cancelled {
				cancel()
			}
			s.peerMu.Unlock()
			held = false
			f1Await(t, done, "C5 read after held registry lock")
			want := error(findnode.ErrCapacity)
			if cancelled {
				want = context.Canceled
			}
			if !errors.Is(actualErr, want) {
				t.Fatal("copied profile/cancel boundary lost", actualErr, want)
			}
		})
	}
}

func TestFindNodeC5OriginalOwnerForeignSessionAndTerminal(t *testing.T) {
	s := c5Service(t)
	f, member := c5Accepted(t, s, c5Service(t))
	other := c5Service(t)
	_, foreign := c5Accepted(t, other, c5Service(t))
	s.peerMu.RLock()
	_, found, err := s.findNodePriorityMemberLocked(t.Context(), member.ConnID(), foreign)
	s.peerMu.RUnlock()
	if !errors.Is(err, findnode.ErrStaleWork) || found {
		t.Fatal("foreign same-ID Session rebound", found, err)
	}
	f.inbound.life.mu.Lock()
	token := f.inbound.life.token
	f.inbound.life.token = new(proofTransportServiceToken)
	f.inbound.life.mu.Unlock()
	_, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, c5Profile())
	f.inbound.life.mu.Lock()
	f.inbound.life.token = token
	f.inbound.life.mu.Unlock()
	if !errors.Is(err, findnode.ErrUnauthenticated) || coverage != 0 {
		t.Fatal("foreign owner token accepted", err)
	}
	f.inbound.end()
	out, _, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, c5Profile())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out.Priority(member); ok {
		t.Fatal("terminal original retained membership")
	}
}

func TestFindNodeC5RecordMirrorPendingIsNotConfirmedK(t *testing.T) {
	s, d := c5Service(t), c5Service(t)
	_, stage, files := stagedContactRecord(t, s, d.identity)
	_, member := c5Accepted(t, s, d)
	if _, err := s.localContactPinStore().confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	c5ReadNotReady(t, s) // strict ACK alone cannot replace owned completion/barrier
	if _, err := s.completeOwedContactRecord(t.Context(), stage, files); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, s, member, findnode.PriorityProtected)
}

func TestFindNodeC5RuntimeEpochAndPinIncarnationDoNotInheritACK(t *testing.T) {
	f, member, clock := c5SelectedCM(t)
	clock.advance(selectedSessionAge)
	if err := f.dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityProtected)
	oldStore := f.dialer.localContactPinStore()
	path := filepath.Join(oldStore.dir, oldStore.name)
	reloaded := loadSecureSessionStore(path, newStoreClock().trusted())
	reloaded.mu.Lock()
	entry := reloaded.identities[member.Identity()]
	reloaded.mu.Unlock()
	if entry.selectedConfirmed() {
		t.Fatal("reopened store inherited runtime ACK")
	}
	oldStore.mu.Lock()
	binding := oldStore.pinEntryBindingLocked(member.Identity())
	oldStore.mu.Unlock()
	r2aRemoveActualPin(t, f.dialer, member.Identity()) // controlled destructive fixture, not enabled public C3
	if err := oldStore.noteProvenInbound(member.Identity()); err != nil {
		t.Fatal(err)
	}
	oldStore.mu.Lock()
	stale := binding.matchesLocked(oldStore)
	oldStore.mu.Unlock()
	if stale {
		t.Fatal("old incarnation survived delete/readd")
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityNew)
	// New same-id current entry cannot acquire old class merely from this old F1 proof.
}

func TestFindNodeC5MixedRelationPendingAndFull20kRead(t *testing.T) {
	f, member, clock := c5SelectedCM(t)
	store := f.dialer.localContactPinStore()
	before := metadataSnapshot(t, store, member.Identity())
	next := before.metadata
	next.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginOutgoingSend, At: time.Now(), Reference: "read-only-R"}
	next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: time.Now(), SourceGroup: domain.SourceGroupLoopback}
	if _, err := store.stagePinMetadata(before, next); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityNew)
	clock.advance(selectedSessionAge)
	if err := f.dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityProtected)
	store.mu.Lock()
	for k := 1; len(store.identities) < maxPinnedIdentities; k++ {
		id := domain.PeerIdentity{89, byte(k), byte(k >> 8)}
		if err := store.recordPinEvidenceLocked(id, store.now()); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
	}
	changes, evidence, protection := store.changes, store.evidenceRevision, store.protectionRevision
	store.mu.Unlock()
	for range 3 {
		c5ReadPriority(t, f.dialer, member, findnode.PriorityProtected)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.identities) != maxPinnedIdentities || store.changes != changes || store.evidenceRevision != evidence || store.protectionRevision != protection {
		t.Fatal("class read allocated/rewrote at full store")
	}
}

func TestFindNodeC5SameGroupNeverInheritsK(t *testing.T) {
	s, d := c5Service(t), c5Service(t)
	addLocalContact(t, s, d.identity)
	_, contact := c5Accepted(t, s, d)
	if err := s.contactPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, ordinary := c5Accepted(t, s, c5Service(t))
	c5ReadPriority(t, s, contact, findnode.PriorityProtected)
	c5ReadPriority(t, s, ordinary, findnode.PriorityNew)
}

func TestFindNodeC5ConfirmedSelectedCanCoverPendingIndependentK(t *testing.T) {
	f, member, clock := c5SelectedCM(t)
	clock.advance(selectedSessionAge)
	if err := f.dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, stage, files := stagedContactRecord(t, f.dialer, f.accepted.identity)
	c5ReadNotReady(t, f.dialer)
	if _, err := f.dialer.localContactPinStore().confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	// This strict image covers N-sel, while the independently owed K barrier is pending.
	classes, err := f.dialer.captureFindNodePriorityClasses(t.Context(), []findnode.Session{member})
	if err != nil || classes[member.ConnID()].class != findnode.ClassSelected {
		t.Fatal("confirmed N-sel blocked by independently pending K", err)
	}
	c5ReadPriority(t, f.dialer, member, findnode.PriorityProtected)
	if _, err := f.dialer.completeOwedContactRecord(t.Context(), stage, files); err != nil {
		t.Fatal(err)
	}
	classes, err = f.dialer.captureFindNodePriorityClasses(t.Context(), []findnode.Session{member})
	if err != nil || classes[member.ConnID()].class != findnode.ClassContact {
		t.Fatal("completed current K not observed", err)
	}
}

func TestFindNodeC5FreshRuntimeGenuineSessionRequiresNewACK(t *testing.T) {
	f, _, clock := c5SelectedCM(t)
	clock.advance(selectedSessionAge)
	if err := f.dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	// No overlapping instances share the files during this reopen.
	if !f.fixtureOwner.stopAndJoin(t) {
		t.Fatal("original CM owner did not stop")
	}
	s, err := newFindNodeCandidateService(f.accepted.cfg, f.accepted.identity, nil)
	if err != nil {
		t.Fatal(err)
	}
	d, err := newFindNodeCandidateService(f.dialer.cfg, f.dialer.identity, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, svc := range []*Service{s, d} {
		svc.routeWithdrawalGracePeriodTest = -1
		t.Cleanup(func() { svc.runCancel(); svc.closeBackgroundAdmission(); svc.WaitBackground() })
	}
	current := f1PairOn(t, true, nil, s, d)
	f1StartActualCM(t, current, 17)
	waitFor(t, 2*time.Second, "new runtime F1 CM publication", func() bool { return len(f1Members(t, d).sessions) == 1 })
	members, err := d.findNodePriorityMembers(t.Context(), c5Profile())
	if err != nil || len(members.sessions) != 1 {
		t.Fatal("fresh runtime member absent", err)
	}
	c5ReadNotReady(t, d)
	files := ninFilesFixture(t, d.localContactPinStore())
	if _, err := d.localContactPinStore().confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	c5ReadPriority(t, d, members.sessions[0], findnode.PriorityProtected)
}

func TestFindNodeC5UnreadableStoreIsNotOrdinary(t *testing.T) {
	s := c5Service(t)
	c5Accepted(t, s, c5Service(t))
	store := s.localContactPinStore()
	store.mu.Lock()
	store.unreadable = true
	store.mu.Unlock()
	_, coverage, err := s.findNodePrioritySnapshot(t.Context(), domain.PeerIdentity{253}, c5Profile())
	store.mu.Lock()
	store.unreadable = false
	store.mu.Unlock()
	if !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) || coverage != 0 {
		t.Fatal("unreadable owner became ordinary priority", coverage, err)
	}
}
