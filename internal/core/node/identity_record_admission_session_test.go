package node

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func sessionRecordTestService(t *testing.T) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	cfg.TrustStorePath = filepath.Join(filepath.Dir(cfg.PeersStatePath), "trust.json")
	s := newSessionRecordCandidateService(cfg, id, nil)
	s.routeWithdrawalGracePeriodTest = -1
	withoutOwnDials(nil)(s)
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}

// Original sessionv2 TLS/auth/publication on two owned net.Pipe endpoints;
// no fabricated Session, Ready, FindNode capability or transport authority.
func sessionRecordPair(t *testing.T, s *Service) f1ProofFixture {
	t.Helper()
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	d := NewService(cfg, peer, nil)
	d.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { d.runCancel(); d.closeBackgroundAdmission(); d.WaitBackground() })
	owner := newF1FixtureOwner()
	left, right := net.Pipe()
	owner.rawServer, owner.rawClient = left, right
	t.Cleanup(func() { owner.stopAndJoin(t) })
	return f1PairOnRaw(t, false, nil, s, d, owner, left, right)
}

func TestSessionRecordConstructorOriginalResolverStoreAndDefaults(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true})
	constructor := NewMockdatagramConstructor(t)
	var captured *identityResolver
	constructor.EXPECT().Build(mock.Anything, mock.Anything).RunAndReturn(func(s *Service, metrics *datagram.Metrics) (*datagramLayer, error) {
		captured = s.identityResolver
		if captured == nil || captured.recordAdmission == nil || s.secureSessions != nil {
			t.Fatal("combined original resolver was not complete before Build")
		}
		a := captured.recordAdmission
		if a.started != nil || a.kReady || a.clock == nil || a.wake == nil || a.openFiles == nil || a.kFiles != nil {
			t.Fatal("construction invented Run readiness or omitted fixed R dependencies")
		}
		if files, err := a.openFiles(); files != nil || !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
			t.Fatal("unfinished original store did not refuse", err)
		}
		if captured.reserveContactRecordPush(t.Context()) != nil || captured.reserveContactRecordReconciliation(t.Context()) != nil {
			t.Fatal("pre-Run construction minted K invocation authority")
		}
		return newDatagramLayer(s, metrics)
	}).Once()
	s := newServiceWithRecordAdmissionClasses(cfg, id, nil, nil, constructor, recordAdmissionConstruction{classes: recordAdmissionChargedR | recordAdmissionImmediateK, kSource: contactRecordSessionSource})
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	r, a := s.identityResolver, s.identityResolver.recordAdmission
	if r != captured || contactPushRegistered(t, s).kPort != captured || a.service != s || a.resolver != r || a.network != r.network || !a.admitsR() || !a.admitsContactPush() || !a.admitsContactReconciliation() {
		t.Fatal("registration/final resolver/owner changed across construction")
	}
	if s.identity != id || s.findNode != nil || s.degree != nil || s.inboundPins != nil || s.r2OwnDMOwner != nil || s.selectedPins == nil || s.contactPins == nil {
		t.Fatal("session-only constructor activated unrelated authority or lost K1/N-sel")
	}
	files, err := a.openFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	pinned, ok := files.(*pinnedIdentityPinFiles)
	if !ok || pinned.mainName != s.localContactPinStore().name || files.Preflight(t.Context()) != nil {
		t.Fatal("late original-Service opener did not bind the actual strict store")
	}
	normal := NewService(configtest.WithTempDataDirs(t, config.Node{}), id, nil)
	t.Cleanup(func() { normal.runCancel(); normal.closeBackgroundAdmission(); normal.WaitBackground() })
	if normal.identityResolver.recordAdmission != nil || normal.hasContactRecordPushTransportCapability() {
		t.Fatal("ordinary constructor enabled a record class")
	}
	for _, bad := range []struct {
		name     string
		classes  recordAdmissionClasses
		source   contactRecordAdmissionSource
		want     string
		borrowed bool
	}{
		{"both-old-push", recordAdmissionChargedR | recordAdmissionImmediateK, contactRecordPushSource, "unsupported Service record admission classes", false},
		{"both-old-reconcile", recordAdmissionChargedR | recordAdmissionImmediateK, contactRecordReconciliationSource, "unsupported Service K provenance", false},
		{"K-session-source", recordAdmissionImmediateK, contactRecordSessionSource, "unsupported Service K provenance", false},
		{"R-session-source", recordAdmissionChargedR, contactRecordSessionSource, "unsupported Service K provenance", false},
		{"unknown-source", recordAdmissionChargedR | recordAdmissionImmediateK, contactRecordAdmissionSource(255), "unsupported Service K provenance", false},
		{"None-session-source", recordAdmissionNone, contactRecordSessionSource, "unsupported Service K provenance", false},
		{"both-borrowed", recordAdmissionChargedR | recordAdmissionImmediateK, contactRecordSessionSource, "borrowed K dependency without K class", true},
	} {
		t.Run(bad.name, func(t *testing.T) {
			defer func() {
				if recover() != bad.want {
					t.Fatal("unsupported tuple did not refuse before construction")
				}
			}()
			var borrowed identityPinDurableFiles
			if bad.borrowed {
				borrowed = NewMockidentityPinDurableFiles(t)
			}
			_ = newServiceWithRecordAdmissionClasses(config.Node{}, nil, nil, nil, productionDatagramConstructor{}, recordAdmissionConstruction{classes: bad.classes, kSource: bad.source, borrowedKFiles: borrowed})
		})
	}
}

func TestSessionRecordOneServiceGenuineKPushReconciliationAndR(t *testing.T) {
	s := sessionRecordTestService(t)
	f := sessionRecordPair(t, s)
	r := s.identityResolver
	r.beginContactRecordPushRun() // explicit fixture arm; actual Run begins below
	addLocalContact(t, s, f.dialer.identity)
	record := buildContactRecord(t, f.dialer.identity, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ctx, delivery, payload := contactPushDelivery(t, f, record)
	if result := contactPushRegistered(t, s).Handle(ctx, delivery, payload); result.Outcome() != datagram.HandlerAccepted {
		t.Fatal("genuine original push refused", result.Err())
	}
	pushID := domain.PeerIdentityFromWire(f.dialer.identity.Address)
	store := s.localContactPinStore()
	store.mu.Lock()
	pushEntry := store.identities[pushID]
	pushEarned := pushEntry.metadata != nil && pushEntry.metadata.recordOwner != nil
	store.mu.Unlock()
	if !pushEarned {
		t.Fatal("actual registered original push did not earn K")
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	addLocalContact(t, s, peer)
	reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	// Exactly two stored contacts; the original cursor selects each in at most
	// two passes, without manufacturing a transport proof for the stored row.
	for range 2 {
		if err := s.contactPins.scanReconciliation(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	reconcileID := domain.PeerIdentityFromWire(peer.Address)
	store.mu.Lock()
	reconcileEntry := store.identities[reconcileID]
	reconcileEarned := reconcileEntry.metadata != nil && reconcileEntry.metadata.recordOwner != nil
	store.mu.Unlock()
	if !reconcileEarned || contactPushOccupied(r) != 0 || r.recordAdmission.rate.count != 0 || r.recordAdmission.started != nil {
		t.Fatal("K paths lost original ownership/shared capacity or charged R")
	}
	_, clock := relationServiceRun(t, s)
	target, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ref := relationServiceAnswer(t, s, target) // actual SendLocal + signed target proof, not TLS claim
	r.mu.Lock()
	_, allowed := r.recordAdmission.rateTrial(clock.sample())
	beforeRate := r.recordAdmission.rate.count
	r.mu.Unlock()
	if allowed || beforeRate != 0 || store.stats().RelationAllocations != 0 {
		t.Fatal("combined service bypassed observed startup60")
	}
	clock.elapsed.Add(int64(time.Minute))
	relationServiceWait(t, "combined original R strict completion", func() bool {
		stats := store.stats()
		r.mu.Lock()
		defer r.mu.Unlock()
		return stats.RelationConfirmed == 1 && r.recordAdmission.rate.count == 1 && r.relationSlotLocked(ref) == nil
	})
}

func TestSessionRecordCombinedAuthoritiesDoNotBorrowEvidence(t *testing.T) {
	for _, variant := range []string{"foreign-owner", "ended-owner", "delete-readd"} {
		t.Run(variant, func(t *testing.T) {
			s := sessionRecordTestService(t)
			f := sessionRecordPair(t, s)
			r := s.identityResolver
			r.beginContactRecordPushRun()
			addLocalContact(t, s, f.dialer.identity)
			id := domain.PeerIdentityFromWire(f.dialer.identity.Address)
			record := buildContactRecord(t, f.dialer.identity, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			if variant == "foreign-owner" {
				other := sessionRecordTestService(t)
				other.identityResolver.beginContactRecordPushRun()
				addLocalContact(t, other, f.dialer.identity)
				if other.identityResolver.prepareContactRecordPush(t.Context(), f.inbound, r.network, id, record) != nil {
					t.Fatal("foreign-Service transport owner became K authority")
				}
				return
			}
			if variant == "ended-owner" {
				f.inbound.end()
				if r.prepareContactRecordPush(t.Context(), f.inbound, r.network, id, record) != nil {
					t.Fatal("ended original transport became K authority")
				}
				return
			}
			reconciliationRecord(t, s, f.dialer.identity, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			g, verified := reconciliationPrepared(t, s)
			s.deleteTrustedContactFrame(id)
			addLocalContact(t, s, f.dialer.identity)
			if _, err := s.stageContactRecordReconciliation(verified, g); err == nil {
				t.Fatal("recreated current contact substituted for original reconciliation fact")
			}
		})
	}
}

func TestSessionRecordCombinedShared256AndOriginalGeneration(t *testing.T) {
	s := sessionRecordTestService(t)
	r, a := s.identityResolver, s.identityResolver.recordAdmission
	clock := &admissionTestClock{}
	a.clock = clock.sample
	r.clock = func() time.Time { return clock.sample().wall }
	if !r.beginRecordAdmissionRun() {
		t.Fatal("fixture startup refused")
	}
	clock.elapsed.Store(int64(time.Minute))
	r.beginContactRecordPushRun()
	slot, _ := admissionQueuedFixture(t, r) // genuine question/answer/accepted queue, no copied R row
	push := r.reserveContactRecordPush(t.Context())
	if push == nil {
		t.Fatal("original push reservation refused")
	}
	t.Cleanup(func() { r.finishContactRecordPush(push) })
	var held []*contactRecordReconciliationAuthority
	t.Cleanup(func() {
		for _, g := range held {
			r.finishContactRecordReconciliation(g)
		}
	})
	for range relationAdmissionCapacity - 2 {
		g := r.reserveContactRecordReconciliation(t.Context())
		if g == nil {
			t.Fatal("shared table refused early")
		}
		held = append(held, g)
	}
	if contactPushOccupied(r) != 256 || r.reserveContactRecordPush(t.Context()) != nil || r.reserveContactRecordReconciliation(t.Context()) != nil {
		t.Fatal("one class discounted another physical reservation")
	}
	// A second genuine user action/attempt cannot reserve another R physical
	// slot either. The existing helper reassigns only the identical r pointer.
	_, label, entry, _, _ := lookupProvenAnswer(t, r, true)
	store := s.localContactPinStore()
	store.pinMutationGate.RLock()
	r.mu.Lock()
	extraR := r.reserveRelationAttemptLocked(label, entry, store)
	r.mu.Unlock()
	store.pinMutationGate.RUnlock()
	if extraR != nil {
		t.Fatal("R bypassed full shared physical table")
	}
	old := push
	r.finishContactRecordPush(old)
	push = r.reserveContactRecordPush(t.Context())
	if push == nil || push.index != old.index || push.generation <= old.generation {
		t.Fatal("one actual return did not free exactly one fresh generation")
	}
	r.finishContactRecordPush(old)
	r.mu.Lock()
	preserved := a.slots[push.index].kAuthority == push && slot.phase == relationAdmissionQueued && a.rate.count == 0
	r.mu.Unlock()
	if !preserved {
		t.Fatal("stale finish reclaimed replacement/R row or altered R rate")
	}
	r.closeContactRecordPushes()
	if contactPushOccupied(r) != 256 {
		t.Fatal("revocation falsely freed executing physical slots")
	}
	if !errors.Is(push.ctx.Err(), context.Canceled) {
		t.Fatal("original push not cancelled")
	}
	for _, g := range held {
		if !errors.Is(g.ctx.Err(), context.Canceled) {
			t.Fatal("original reconciliation not cancelled")
		}
	}
}

func TestSessionRecordRunJoinsCombinedStrictClose(t *testing.T) {
	s := sessionRecordTestService(t)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	addLocalContact(t, s, peer)
	reconciliationRecord(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	original := s.trust.localFiles
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		real, err := original()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() {
			if err := real.Close(); err != nil {
				t.Error(err)
			}
		}()
		return real.Write(ctx, raw)
	}).Once()
	files.EXPECT().Close().RunAndReturn(func() error { close(entered); <-release; return nil }).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	// Reuse the original reconciliation fixture's admission barrier: prevent
	// an early pre-kReady scan from spending the unchanged two-second cadence.
	allow := make(chan struct{})
	var clockOnce, allowOnce sync.Once
	unblockClock := func() { allowOnce.Do(func() { close(allow) }) }
	s.contactPins.clock = func() time.Time { clockOnce.Do(func() { <-allow }); return time.Now() }
	ctx, cancel := context.WithCancel(t.Context())
	running := runServiceForTest(ctx, s)
	t.Cleanup(func() { unblockClock(); unblock(); cancel(); contactPushJoin(t, running.exited) })
	waitContactRecordCondition(t, "combined actual K readiness missing", func() bool {
		a := s.identityResolver.recordAdmission
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kReady
	})
	unblockClock()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual original fact Close not entered")
	}
	a := s.identityResolver.recordAdmission
	s.identityResolver.mu.Lock()
	started := a.started != nil
	s.identityResolver.mu.Unlock()
	if !started || contactPushOccupied(s.identityResolver) != 1 {
		t.Fatal("combined Run lost R startup or K physical owner")
	}
	cancel()
	waitContactRecordCondition(t, "combined shutdown did not revoke K", func() bool {
		a.kStageMu.Lock()
		defer a.kStageMu.Unlock()
		return a.kClosing
	})
	select {
	case <-running.exited:
		t.Fatal("Run returned before actual original Close")
	default:
	}
	if contactPushOccupied(s.identityResolver) != 1 {
		t.Fatal("R teardown reclaimed executing K slot")
	}
	unblock()
	contactPushJoin(t, running.exited)
	if contactPushOccupied(s.identityResolver) != 0 {
		t.Fatal("actual callback return did not release K capacity")
	}
}
