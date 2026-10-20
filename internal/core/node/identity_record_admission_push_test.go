package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/trustedclock"
	"github.com/stretchr/testify/mock"
)

// Only this named test constructor supplies borrowed files. The dependency
// cannot enable a class, manufacture a transport owner or confirm durability.
func contactPushTestService(t *testing.T, files identityPinDurableFiles) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	// Local-contact confirmation needs real trust files, independently of the pin backend.
	cfg.TrustStorePath = filepath.Join(filepath.Dir(cfg.PeersStatePath), "trust.json")
	s := newServiceWithRecordAdmissionClasses(cfg, id, nil, nil, productionDatagramConstructor{}, recordAdmissionConstruction{classes: recordAdmissionImmediateK, borrowedKFiles: files})
	s.routeWithdrawalGracePeriodTest = -1 // transport ownership, not route-grace timing
	withoutOwnDials(nil)(s)
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}
func contactPushPair(t *testing.T, files identityPinDurableFiles) f1ProofFixture {
	t.Helper()
	s := contactPushTestService(t, files)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	d := NewService(cfg, peer, nil)
	d.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { d.runCancel(); d.closeBackgroundAdmission(); d.WaitBackground() })
	if s.findNode != nil || s.inboundPins != nil || !s.hasContactRecordPushTransportCapability() {
		t.Fatal("K-only accepted fixture changed constructor isolation")
	}
	if d.findNode != nil || d.inboundPins != nil || d.hasContactRecordPushTransportCapability() {
		t.Fatal("ordinary push peer acquired optional transport authority")
	}
	owner := newF1FixtureOwner()
	owner.serviceJobs = [2]*Service{s, d}
	f := f1PairOnOwned(t, false, nil, s, d, owner)
	if f.outbound.common != nil || f.dialerRaw.common.Load() != nil || captureSessionTransportOwner(d, f.outbound) != nil {
		t.Fatal("ordinary push peer minted optional common/raw ownership")
	}
	if f.outbound.proven == nil || f.outbound.netCore == nil || f.outbound.peerIdentity != domain.PeerIdentityFromWire(s.identity.Address) || f.inbound == nil || f.inbound.life == nil {
		t.Fatal("ordinary push peer lost actual proof/auth/accepted owner")
	}
	// Explicit fixture-only arm. The Run join test below arms only through Run.
	s.identityResolver.beginContactRecordPushRun()
	return f
}

// Genuine TLS/auth/registry rows. Nin-only outbound absence is additionally
// covered by the exact source predicate matrix below, without installing a new
// journal or starting its independent producer just for this fixture repair.
func TestContactPushFixtureOutboundOwnerMatchesConstructors(t *testing.T) {
	for _, kind := range []string{"ordinary", "FindNode", "K"} {
		t.Run(kind, func(t *testing.T) {
			s := contactPushTestService(t, nil)
			owner := newF1FixtureOwner()
			owner.serviceJobs[0] = s
			t.Cleanup(func() { owner.stopAndJoin(t) })
			id, err := identity.Generate()
			if err != nil {
				t.Fatal(err)
			}
			cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
			var d *Service
			switch kind {
			case "ordinary":
				d = NewService(cfg, id, nil)
			case "FindNode":
				d, err = newFindNodeCandidateService(cfg, id, nil)
			case "K":
				d = newContactRecordPushCandidateService(cfg, id, nil)
			}
			owner.serviceJobs[1] = d // cleanup owns a returned Service even on constructor error
			if err != nil {
				t.Fatal(err)
			}
			d.routeWithdrawalGracePeriodTest = -1
			if d.inboundPins != nil || (d.findNode != nil) != (kind == "FindNode") || d.hasContactRecordPushTransportCapability() != (kind == "K") {
				t.Fatal("fixture constructor isolation changed", kind)
			}
			f := f1PairOnOwned(t, false, nil, s, d, owner)
			_, proven := f.outbound.provenIdentity()
			if !proven || f.outbound.netCore == nil || f.outbound.peerIdentity != domain.PeerIdentityFromWire(s.identity.Address) || f.inbound == nil {
				t.Fatal("fixture row did not retain genuine proof/auth", kind)
			}
			original := captureSessionTransportOwner(d, f.outbound)
			wantOwner := kind != "ordinary"
			if (f.outbound.common != nil) != wantOwner || (f.dialerRaw.common.Load() != nil) != wantOwner || (original != nil) != wantOwner {
				t.Fatal("fixture outbound optional owner mismatch", kind)
			}
			d.peerMu.RLock()
			entry := d.connEntryByIDLocked(f.outbound.connID)
			registered := entry != nil && entry.core == f.outbound.netCore && f.outbound.authOK && entry.common == f.outbound.common
			matched := !wantOwner
			if l := f.outbound.common; l != nil {
				l.mu.Lock()
				matched = original.matchesTransportLocked(d, f.outbound.connID) && original.life == l && l.entry == entry && l.peer == f.outbound && l.role == sessionv2.RoleDialer && l.outbound && l.nin == nil && f.dialerRaw.common.Load() == l
				l.mu.Unlock()
			}
			d.peerMu.RUnlock()
			if !registered || !matched {
				t.Fatal("fixture original registry/owner tuple mismatch", kind)
			}
		})
	}
}

func contactPushRegistered(t *testing.T, s *Service) *pushIdentityHandler {
	t.Helper()
	layer := s.datagramPlane.Load()
	if layer == nil {
		t.Fatal("missing actual registry")
	}
	registration, ok := layer.types.Lookup(domain.DTypePushIdentity)
	if !ok {
		t.Fatal("push type absent")
	}
	handler, ok := registration.Handler().(*pushIdentityHandler)
	if !ok {
		t.Fatal("wrong push handler")
	}
	if handler.kPort != s.identityResolver {
		t.Fatal("registration did not retain SAME final resolver")
	}
	return handler
}
func contactPushDelivery(t *testing.T, f f1ProofFixture, record protocol.SignedIdentityRecord) (context.Context, datagram.DeliveryContext, []byte) {
	t.Helper()
	id := domain.PeerIdentityFromWire(f.dialer.identity.Address)
	delivery, payload := newPushDelivery(t, f.accepted, f.dialer.identity, id, record)
	actual, err := datagram.NewDeliveryContext(datagram.DeliveryContextOpts{Header: delivery.Header(), IncomingPeer: datagram.ClaimedIngress(datagram.NetworkChannel(f.inbound.id), datagram.AcceptedConnectionKey(f.inbound.id), id), LocalIdentity: domain.PeerIdentityFromWire(f.accepted.identity.Address)})
	if err != nil {
		t.Fatal(err)
	}
	return context.WithValue(t.Context(), proofTransportArrivalKey{}, f.inbound), actual, payload
}
func contactPushOccupied(r *identityResolver) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for i := range r.recordAdmission.slots {
		if r.recordAdmission.slots[i].phase != relationAdmissionFree {
			n++
		}
	}
	return n
}
func contactPushJoin(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actual K push worker not joined")
	}
}

func TestContactPushActualRegisteredHandlerStagesOnlyFreshAuthorizedFact(t *testing.T) {
	for _, variant := range []string{"fresh", "duplicate", "no-fact", "no-owner", "min1", "ended-owner", "stale-record", "ordinary-default"} {
		t.Run(variant, func(t *testing.T) {
			f := contactPushPair(t, nil)
			s := f.accepted
			r := s.identityResolver
			id := domain.PeerIdentityFromWire(f.dialer.identity.Address)
			if variant != "no-fact" {
				addLocalContact(t, s, f.dialer.identity)
			}
			minimum := domain.DeclaredMinSession(domain.SessionVersionSecure)
			if variant == "min1" {
				minimum = domain.DeclaredMinSession(domain.SessionVersionLegacy)
			}
			record := buildContactRecord(t, f.dialer.identity, 2, minimum)
			if variant == "duplicate" {
				body, err := protocol.VerifyIdentityRecord(record, r.network, id)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.importVerifiedIdentityRecord(r.network, record, body); err != nil {
					t.Fatal(err)
				}
			}
			if variant == "stale-record" {
				contactRecordFixture(t, s, f.dialer.identity, 3, minimum)
			}
			ctx, delivery, payload := contactPushDelivery(t, f, record)
			if variant == "no-owner" {
				ctx = t.Context()
			}
			if variant == "ended-owner" {
				f.inbound.end()
			}
			h := contactPushRegistered(t, s)
			if variant == "ordinary-default" {
				h = &pushIdentityHandler{svc: s, network: r.network, clock: time.Now, lastAcceptedAt: map[pushSessionKey]time.Time{}, violationAt: map[pushSessionKey]time.Time{}}
			}
			if result := h.Handle(ctx, delivery, payload); result.Outcome() != datagram.HandlerAccepted {
				t.Fatal("optional K altered ordinary outcome", result.Outcome())
			}
			store := s.localContactPinStore()
			store.mu.Lock()
			entry := store.identities[id]
			store.mu.Unlock()
			earned := entry.metadata != nil && entry.metadata.recordOwner != nil
			if earned != (variant == "fresh" || variant == "duplicate") {
				t.Fatal("wrong original-push K ownership", variant, earned)
			}
			if contactPushOccupied(r) != 0 {
				t.Fatal("returned handler retained physical capacity")
			}
			if r.recordAdmission.started != nil || r.recordAdmission.clock != nil || r.recordAdmission.openFiles != nil || r.recordAdmission.wake != nil || r.recordAdmission.rate.count != 0 {
				t.Fatal("K-only enabled R startup/rate/worker dependencies")
			}
		})
	}
}
func TestContactPushClassIsolationAndSharedPhysicalCapacity(t *testing.T) {
	t.Run("unknown-class", func(t *testing.T) {
		defer func() {
			if recovered := recover(); recovered != "unsupported Service record admission classes" {
				t.Fatal("unknown classes were not specifically refused", recovered)
			}
		}()
		_ = newServiceWithRecordAdmissionClasses(config.Node{}, nil, nil, nil, productionDatagramConstructor{}, recordAdmissionConstruction{classes: recordAdmissionClasses(128)})
	})
	normal, _ := newTestResolver(t)
	if normal.reserveContactRecordPush(t.Context()) != nil || normal.beginRecordAdmissionRun() {
		t.Fatal("ordinary resolver acquired class authority")
	}
	r, _ := admissionResolverFixture(t)
	if r.reserveContactRecordPush(t.Context()) != nil {
		t.Fatal("R-only acquired K")
	}
	// Explicit private combined fixture, assigned once before this owner escapes.
	a := r.recordAdmission
	r.recordAdmission = &relationRecordAdmission{classes: recordAdmissionChargedR | recordAdmissionImmediateK, service: r.svc, resolver: r, network: r.network, clock: a.clock, started: a.started, openFiles: a.openFiles, wake: a.wake}
	r.beginContactRecordPushRun()
	slot, _ := admissionQueuedFixture(t, r)
	var held []*contactRecordStageAuthority
	for i := 0; i < relationAdmissionCapacity-1; i++ {
		g := r.reserveContactRecordPush(t.Context())
		if g == nil {
			t.Fatal("shared capacity lost early", i)
		}
		held = append(held, g)
	}
	if r.reserveContactRecordPush(t.Context()) != nil || contactPushOccupied(r) != 256 {
		t.Fatal("K bypassed occupied R/shared256")
	}
	r.closeContactRecordPushes()
	if contactPushOccupied(r) != 256 || slot.phase != relationAdmissionQueued {
		t.Fatal("cancel falsely reclaimed processing K or altered R")
	}
	for _, g := range held {
		if !errors.Is(g.ctx.Err(), context.Canceled) {
			t.Fatal("close did not cancel original invocation")
		}
		r.finishContactRecordPush(g)
	}
	if contactPushOccupied(r) != 1 {
		t.Fatal("actual returns did not release exactly K slots")
	}
}
func TestContactPushKOnlyLookupNeverSamplesOrReservesR(t *testing.T) {
	s := contactPushTestService(t, nil)
	r := s.identityResolver
	r.beginContactRecordPushRun()
	if r.recordAdmission.clock != nil {
		t.Fatal("K constructor allocated R clock")
	}
	// Existing R clock dependency used only as a test trap, never K authority.
	var samples atomic.Int64
	r.recordAdmission.clock = func() relationObservedTime { samples.Add(1); return relationObservedTime{wall: time.Now()} }
	target, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	id := domain.PeerIdentityFromWire(target.Address)
	if _, err := s.StartUserIdentityLookup(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	resolution := r.resolutions[id]
	if resolution == nil || resolution.recordLifetime != nil {
		r.mu.Unlock()
		t.Fatal("K-only sampled R lookup lifetime")
	}
	if r.selectRelationSlotLocked() != nil {
		r.mu.Unlock()
		t.Fatal("K-only R selector ran")
	}
	r.mu.Unlock()
	if r.beginRecordAdmissionRun() {
		t.Fatal("K-only launched R worker")
	}
	if samples.Load() != 0 {
		t.Fatal("K-only sampled disabled R clock")
	}
	if err := r.processRelationSlot(t.Context(), &relationAdmissionSlot{}); !errors.Is(err, errRelationAdmissionStale) {
		t.Fatal("K-only direct R processing performed work", err)
	}
}
func TestContactPushOriginalCaptureCannotBorrowRecreatedFactOrPin(t *testing.T) {
	for _, variant := range []string{"contact-delete-readd", "pin-C3", "store-replaced", "cancelled", "generation-mutated"} {
		t.Run(variant, func(t *testing.T) {
			f := contactPushPair(t, nil)
			s := f.accepted
			r := s.identityResolver
			id := domain.PeerIdentityFromWire(f.dialer.identity.Address)
			addLocalContact(t, s, f.dialer.identity)
			record := buildContactRecord(t, f.dialer.identity, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
			g := r.prepareContactRecordPush(t.Context(), f.inbound, r.network, id, record)
			if g == nil {
				t.Fatal("original capture refused")
			}
			defer r.finishContactRecordPush(g)
			candidate := g.candidate
			body, err := protocol.VerifyIdentityRecord(record, r.network, id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.importVerifiedIdentityRecord(r.network, record, body); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "contact-delete-readd":
				s.deleteTrustedContactFrame(id)
				addLocalContact(t, s, f.dialer.identity)
			case "pin-C3":
				removeContactPinForTest(t, s, id)
			case "store-replaced":
				s.secureSessions.store = loadSecureSessionStore(filepath.Join(t.TempDir(), "replacement.json"), newStoreClock().trusted())
			case "cancelled":
				g.cancel()
			case "generation-mutated":
				g.generation++
			}
			if _, err := s.stageContactRecordPush(verifiedContactMinSessionRecord{candidate: candidate}, g); err == nil {
				t.Fatal("stale original capsule staged", variant)
			}
			if _, ok := candidate.stagedOutcome(); ok {
				t.Fatal("stale capsule published an owed outcome")
			}
			if variant == "generation-mutated" {
				g.generation--
			}
		})
	}
}

// Real worker waits on the final leaf AFTER store/preparation. Close wins
// without taking an earlier store/trust lock, and cannot release Processing.
func TestContactPushClosingWinsLeafWithoutRecyclingExecutingSlot(t *testing.T) {
	f := contactPushPair(t, nil)
	s := f.accepted
	r := s.identityResolver
	id := domain.PeerIdentityFromWire(f.dialer.identity.Address)
	addLocalContact(t, s, f.dialer.identity)
	record := buildContactRecord(t, f.dialer.identity, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	g := r.prepareContactRecordPush(t.Context(), f.inbound, r.network, id, record)
	if g == nil {
		t.Fatal("capture refused")
	}
	t.Cleanup(func() { r.finishContactRecordPush(g) })
	body, err := protocol.VerifyIdentityRecord(record, r.network, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.importVerifiedIdentityRecord(r.network, record, body); err != nil {
		t.Fatal(err)
	}
	a := r.recordAdmission
	a.kStageMu.Lock()
	locked := true
	done := make(chan struct{})
	var stageErr error
	t.Cleanup(func() {
		if locked {
			a.kStageMu.Unlock()
		}
		contactPushJoin(t, done)
		r.finishContactRecordPush(g)
	})
	go func() {
		defer close(done)
		_, stageErr = s.stageContactRecordPush(verifiedContactMinSessionRecord{candidate: g.candidate}, g)
	}()
	store := s.localContactPinStore()
	waitContactRecordCondition(t, "guarded stage did not retain actual store lock before leaf", func() bool {
		if store.mu.TryLock() {
			store.mu.Unlock()
			return false
		}
		return true
	})
	a.kClosing = true
	a.kReady = false
	g.revoked = true // same final leaf as shutdown; no earlier lock acquired
	a.kStageMu.Unlock()
	locked = false
	contactPushJoin(t, done)
	if stageErr == nil {
		t.Fatal("closing-first stage mutated protection")
	}
	if _, ok := g.candidate.stagedOutcome(); ok {
		t.Fatal("closing-first produced owed ownership")
	}
	if contactPushOccupied(r) != 1 {
		t.Fatal("cancel fabricated callback completion")
	}
	r.finishContactRecordPush(g)
	if contactPushOccupied(r) != 0 {
		t.Fatal("actual return did not release slot")
	}
}

func TestContactPushPreparedClockRestoreAndBoundValueRefusal(t *testing.T) {
	for _, variant := range []string{"absent", "present", "full", "recovery", "corrupt", "foreign-candidate", "permit-mutated"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			candidate, _ := verifiedContactRecord(t, s, peer)
			store := s.localContactPinStore()
			id := domain.PeerIdentityFromWire(peer.Address)
			if variant == "present" || variant == "recovery" || variant == "corrupt" || variant == "permit-mutated" {
				store.mu.Lock()
				err := store.recordPinEvidenceLocked(id, time.Unix(5, 0))
				store.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				candidate, _ = verifiedContactRecord(t, s, peer)
			}
			if variant == "recovery" || variant == "corrupt" || variant == "permit-mutated" {
				removeContactPinForTest(t, s, id)
				candidate, _ = verifiedContactRecord(t, s, peer)
			}
			if variant == "full" {
				fillContactRecordPins(t, store, maxPinnedIdentities)
			}
			var calls atomic.Int64
			at := time.Unix(71, 0)
			store.clock = trustedclock.New(trustedclock.Sources{Wall: func() time.Time { calls.Add(1); return at }})
			store.pinMutationGate.RLock()
			store.mu.Lock()
			held := true
			t.Cleanup(func() {
				if held {
					store.mu.Unlock()
					store.pinMutationGate.RUnlock()
				}
			})
			if variant == "corrupt" {
				store.pinRecovery.permit.Binding.Backup.Payload = "invalid"
			}
			prepared := store.prepareContactRecordStageLocked(candidate)
			beforeChanges, beforeEvidence, beforeProtection := store.changes, store.evidenceRevision, store.protectionRevision
			expected := int64(1)
			if variant == "present" || variant == "full" {
				expected = 0
			}
			if calls.Load() != expected {
				t.Fatal("clock/restore branch order changed", calls.Load(), expected)
			}
			if variant == "recovery" {
				binding := store.pinRecovery.permit.Binding
				restored, err := binding.Backup.Restore(id, binding.PreviousPinnedAt)
				if err != nil {
					t.Fatal(err)
				}
				if prepared.entry.pinnedAt != restored.PinnedAt {
					t.Fatalf("preparation changed canonical restored timestamp: got=%#v want=%#v", prepared.entry.pinnedAt, restored.PinnedAt)
				}
				if !prepared.entry.pinnedAt.Equal(binding.PreviousPinnedAt) {
					t.Fatalf("preparation changed restored instant: got=%#v want=%#v", prepared.entry.pinnedAt, binding.PreviousPinnedAt)
				}
				if prepared.metadata != restored.Metadata {
					t.Fatalf("preparation changed restored metadata: got=%+v want=%+v", prepared.metadata, restored.Metadata)
				}
			}
			if variant == "foreign-candidate" {
				candidate = &contactRecordCandidate{}
			}
			if variant == "permit-mutated" {
				store.pinRecovery.permit.Generation++
			}
			stage, err := store.stageContactRecordLocked(candidate, prepared)
			refusal := variant == "full" || variant == "corrupt" || variant == "foreign-candidate" || variant == "permit-mutated"
			if refusal && (err == nil || stage.owed != nil || store.changes != beforeChanges || store.evidenceRevision != beforeEvidence || store.protectionRevision != beforeProtection) {
				t.Fatal("invalid preparation had effects", variant, err)
			}
			if !refusal && err != nil {
				t.Fatal("genuine preparation refused", variant, err)
			}
			if calls.Load() != expected {
				t.Fatal("final memory body called clock")
			}
			store.mu.Unlock()
			store.pinMutationGate.RUnlock()
			held = false
		})
	}
}

// Real Run listener -> genuine TLS -> registered datagram pipeline -> original
// synchronous Handle -> borrowed strict Preflight. Slot/cancel are not joins.
func TestContactPushRunWaitsForActualTransportCallbackAfterClose(t *testing.T) {
	files := NewMockidentityPinDurableFiles(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(ctx context.Context) error { close(entered); <-release; return ctx.Err() }).Once()
	s := contactPushTestService(t, files)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	addLocalContact(t, s, peer)
	dc := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, Type: domain.NodeTypeFull})
	d := NewService(dc, peer, nil)
	t.Cleanup(func() { d.runCancel(); d.closeBackgroundAdmission(); d.WaitBackground() })
	ctx, cancel := context.WithCancel(t.Context())
	var running *runningTestService
	var done <-chan struct{}
	var raw net.Conn
	var readerDone <-chan struct{}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		cancel()
		if raw != nil {
			_ = raw.Close()
		}
		if done != nil {
			contactPushJoin(t, done)
		}
		if readerDone != nil {
			contactPushJoin(t, readerDone)
		}
	})
	running = runServiceForTest(ctx, s)
	done = running.exited
	readyCtx, readyCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer readyCancel()
	if err := running.AwaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
	raw, err = net.DialTimeout("tcp", s.externalListenAddress(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dialCtx, dialCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer dialCancel()
	proof, err := sessionv2.Dial(dialCtx, raw, d.secureSessions.local, d.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proof.Conn()
	if err != nil {
		t.Fatal(err)
	}
	readJoined := make(chan struct{})
	readerDone = readJoined
	go func() { defer close(readJoined); _, _ = io.Copy(io.Discard, wire) }()
	record := buildContactRecord(t, peer, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	payload, err := protocol.BuildPushIdentityPayload(protocol.PushIdentityPayload{V: domain.IdentityLookupSchemaVersion, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err = rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.SignDatagram(protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeRouted, Class: domain.DatagramClassControl, Src: domain.PeerIdentityFromWire(peer.Address), Dst: domain.PeerIdentityFromWire(s.identity.Address), TTL: 1, RoutePolicy: domain.RoutePolicyBest, DType: domain.DTypePushIdentity, Payload: payload, Auth: &protocol.DatagramAuth{AuthVersion: domain.AuthVersionBase, PubKey: append([]byte(nil), peer.PublicKey...), Salt: salt, MaxTTL: 1, Time: time.Now().Unix()}}, domain.NetworkID(networkName), peer.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	line, err := protocol.MarshalDatagramFrameLine(frame)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(wire, line); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-done:
		t.Fatal("Run returned before actual handler preflight", running.runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("actual TLS ingress did not reach registered K Preflight")
	}
	r := s.identityResolver
	if contactPushOccupied(r) != 1 {
		t.Fatal("actual handler lacks processing slot")
	}
	r.mu.Lock()
	var original *contactRecordStageAuthority
	for i := range r.recordAdmission.slots {
		if g := r.recordAdmission.slots[i].kAuthority; g != nil {
			original = g
		}
	}
	r.mu.Unlock()
	if original == nil {
		t.Fatal("actual registered handler did not publish its invocation")
	}
	original.admission.kStageMu.Lock()
	candidate := original.candidate
	generation := original.generation
	original.admission.kStageMu.Unlock()
	if candidate == nil {
		t.Fatal("actual preflight has no original candidate")
	}
	cancel()
	waitContactRecordCondition(t, "Run did not revoke K before drain", func() bool {
		a := r.recordAdmission
		a.kStageMu.Lock()
		closed := a.kClosing
		a.kStageMu.Unlock()
		return closed
	})
	select {
	case <-done:
		t.Fatal("Run returned while actual handler/preflight had not returned")
	default:
	}
	if contactPushOccupied(r) != 1 {
		t.Fatal("cancel reused still-executing handler capacity")
	}
	r.mu.Lock()
	same := r.recordAdmission.slots[original.index].kAuthority == original && r.recordAdmission.slots[original.index].generation == generation
	r.mu.Unlock()
	if !same {
		t.Fatal("cancel replaced the actual blocked callback owner")
	}
	once.Do(func() { close(release) })
	contactPushJoin(t, done)
	if running.runErr != nil && !errors.Is(running.runErr, context.Canceled) {
		t.Fatal(running.runErr)
	}
	if contactPushOccupied(r) != 0 {
		t.Fatal("actual connWg/runLoops join left executing K callback")
	}
	if _, ok := candidate.stagedOutcome(); ok {
		t.Fatal("closing-first actual handler published a stage")
	}
	// This executes the accepted/connWg root. The outbound root has its own test.
	r.mu.Lock()
	for i := range r.recordAdmission.slots {
		if r.recordAdmission.slots[i].kAuthority != nil {
			r.mu.Unlock()
			t.Fatal("returned Run retained capsule")
		}
	}
	r.mu.Unlock()
}

func TestContactPushConstructorCapturesCompleteResolverBeforeBuild(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{EnableDatagramV1: true, ListenAddress: freeAddress(t)})
	var captured *identityResolver
	constructor := NewMockdatagramConstructor(t)
	constructor.EXPECT().Build(mock.Anything, mock.Anything).RunAndReturn(func(s *Service, metrics *datagram.Metrics) (*datagramLayer, error) {
		captured = s.identityResolver
		if captured == nil || captured.svc != s || captured.intents == nil || captured.resolutions == nil || captured.wake == nil || captured.network != domain.NetworkID(networkName) {
			t.Fatal("Build escaped before complete SAME resolver")
		}
		if captured.recordAdmission.kReady || captured.recordAdmission.kFiles != nil || captured.recordAdmission.openFiles != nil || captured.reserveContactRecordPush(t.Context()) != nil {
			t.Fatal("construction-time K stage became available")
		}
		if s.secureSessions != nil {
			t.Fatal("early constructor requirement accidentally depended on pin store")
		}
		return newDatagramLayer(s, metrics)
	}).Once()
	s := newServiceWithRecordAdmissionClasses(cfg, id, nil, nil, constructor, recordAdmissionConstruction{classes: recordAdmissionImmediateK})
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if s.identityResolver != captured || contactPushRegistered(t, s).kPort != captured {
		t.Fatal("constructor replaced captured resolver")
	}
	if s.secureSessions == nil || s.contactPins == nil || s.selfRecordBody.Seq == 0 {
		t.Fatal("constructor returned before ordinary dependencies")
	}
	normal, peer := localContactFixture(t)
	_ = peer
	if normal.identityResolver.recordAdmission != nil {
		t.Fatal("normal constructor allocated class authority")
	}
}
func TestContactPushStageFirstRetainsOwedAfterClosing(t *testing.T) {
	f := contactPushPair(t, nil)
	s := f.accepted
	r := s.identityResolver
	addLocalContact(t, s, f.dialer.identity)
	record := buildContactRecord(t, f.dialer.identity, 2, domain.DeclaredMinSession(domain.SessionVersionSecure))
	id := domain.PeerIdentityFromWire(f.dialer.identity.Address)
	g := r.prepareContactRecordPush(t.Context(), f.inbound, r.network, id, record)
	if g == nil {
		t.Fatal("capture refused")
	}
	defer r.finishContactRecordPush(g)
	body, err := protocol.VerifyIdentityRecord(record, r.network, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.importVerifiedIdentityRecord(r.network, record, body); err != nil {
		t.Fatal(err)
	}
	stage, err := s.stageContactRecordPush(verifiedContactMinSessionRecord{candidate: g.candidate}, g)
	if err != nil || stage.owed == nil {
		t.Fatal("stage-first did not publish owed ownership", err)
	}
	r.closeContactRecordPushes()
	if !g.staged || !g.revoked || contactPushOccupied(r) != 1 {
		t.Fatal("close erased staged outcome or callback occupancy")
	}
	store := s.localContactPinStore()
	store.mu.Lock()
	current := store.identities[id]
	store.mu.Unlock()
	if current.metadata == nil || current.metadata.recordOwner != stage.owed {
		t.Fatal("closing lost earned obligation")
	}
}

// Separate actual outbound CM/readPeerSession root, not inferred from connWg.
func TestContactPushRunWaitsForActualOutboundCallbackAfterClose(t *testing.T) {
	remote, stop := startTestNodeWithSetup(t, config.Node{ListenAddress: freeAddress(t), ListenerSet: true, ListenerEnabled: true, EnableDatagramV1: true, Type: domain.NodeTypeFull}, withoutOwnDials(nil))
	t.Cleanup(stop)
	files := NewMockidentityPinDurableFiles(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(ctx context.Context) error { close(entered); <-release; return ctx.Err() }).Once()
	s := contactPushTestService(t, files)
	// Only this finite test admits one real manual CM dial, after actual Run ready.
	s.connManager.config.MaxSlotsFn = func() int { return 1 }
	addLocalContact(t, s, remote.identity)
	ctx, cancel := context.WithCancel(t.Context())
	var running *runningTestService
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		cancel()
		if running != nil {
			contactPushJoin(t, running.exited)
		}
	})
	running = runServiceForTest(ctx, s)
	readyCtx, readyCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer readyCancel()
	if err := running.AwaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
	if reply := s.addPeerFrame(protocol.Frame{Type: "add_peer", Peers: []string{remote.externalListenAddress()}}); reply.Type != "ok" {
		t.Fatal("real manual CM dial refused", reply.Type)
	}
	select {
	case <-entered:
	case <-running.exited:
		t.Fatal("Run returned before actual outbound handler", running.runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("actual CM push did not enter borrowed Preflight")
	}
	r := s.identityResolver
	r.mu.Lock()
	var original *contactRecordStageAuthority
	for i := range r.recordAdmission.slots {
		if g := r.recordAdmission.slots[i].kAuthority; g != nil {
			original = g
		}
	}
	r.mu.Unlock()
	if original == nil {
		t.Fatal("outbound callback did not retain original slot")
	}
	original.admission.kStageMu.Lock()
	candidate := original.candidate
	original.admission.kStageMu.Unlock()
	cancel()
	waitContactRecordCondition(t, "outbound shutdown did not revoke K", func() bool {
		a := r.recordAdmission
		a.kStageMu.Lock()
		closing := a.kClosing
		a.kStageMu.Unlock()
		return closing
	})
	select {
	case <-running.exited:
		t.Fatal("Run returned while outbound readPeerSession callback still executing")
	default:
	}
	if contactPushOccupied(r) != 1 {
		t.Fatal("outbound cancel fabricated actual callback return")
	}
	once.Do(func() { close(release) })
	contactPushJoin(t, running.exited)
	if running.runErr != nil && !errors.Is(running.runErr, context.Canceled) {
		t.Fatal(running.runErr)
	}
	if contactPushOccupied(r) != 0 {
		t.Fatal("real outbound runLoops join left K processing")
	}
	if candidate == nil {
		t.Fatal("real outbound preflight lacked original capsule")
	}
	if _, ok := candidate.stagedOutcome(); ok {
		t.Fatal("closing-first outbound callback published stage")
	}
}

// This is a predicate/class fixture, not a proof-owner mint or transport join.
// It evaluates the actual three source predicates; real accepted/CM tests above
// remain the distinct evidence that authenticated delivery really reaches K.
func TestContactPushTransportCapabilityModesAndOriginalPredicates(t *testing.T) {
	fixtureRaw, err := os.ReadFile("find_node_connection_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if failures := contactPushFixtureOwnerContract(fixtureRaw); len(failures) != 0 {
		t.Fatal(failures)
	}
	const check = "if (p.common != nil) != expectsOutboundOwner {"
	if strings.Count(string(fixtureRaw), check) != 1 {
		t.Fatal("fixture optional-owner control premise changed")
	}
	missingCheck := strings.Replace(string(fixtureRaw), check, "if false {", 1)
	failures := contactPushFixtureOwnerContract([]byte(missingCheck))
	if !strings.Contains(strings.Join(failures, ";"), "fixture owner contract statement missing/duplicate: check") {
		t.Fatal("missing fixture owner consistency check escaped", failures)
	}
	raw, err := os.ReadFile("session_secure.go")
	if err != nil {
		t.Fatal(err)
	}
	conditions, failures := contactPushTransportConditions(raw)
	if len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, mode := range []struct {
		name         string
		nin, find, k bool
		classes      recordAdmissionClasses
	}{
		{name: "None"}, {name: "R-only", classes: recordAdmissionChargedR},
		{name: "Nin-only", nin: true}, {name: "FindNode-only", find: true},
		{name: "K-only", k: true, classes: recordAdmissionImmediateK},
		{name: "R-K", k: true, classes: recordAdmissionChargedR | recordAdmissionImmediateK},
		{name: "R-Nin", nin: true, classes: recordAdmissionChargedR},
		{name: "Nin-K", nin: true, k: true, classes: recordAdmissionImmediateK},
		{name: "FindNode-K", find: true, k: true, classes: recordAdmissionImmediateK},
		{name: "Nin-FindNode", nin: true, find: true},
		{name: "Nin-FindNode-K", nin: true, find: true, k: true, classes: recordAdmissionImmediateK},
	} {
		t.Run(mode.name, func(t *testing.T) {
			s := &Service{}
			if mode.nin {
				s.inboundPins = &inboundPinProducer{}
			}
			if mode.find {
				s.findNode = &findNodeAdapter{}
			}
			if mode.classes != recordAdmissionNone {
				r := &identityResolver{svc: s, network: domain.NetworkID(networkName)}
				r.recordAdmission = &relationRecordAdmission{classes: mode.classes, service: s, resolver: r, network: r.network}
				s.identityResolver = r
			}
			if got := s.hasContactRecordPushTransportCapability(); got != mode.k {
				t.Fatal("immutable K capability mode mismatch", got, mode.k)
			}
			for _, proofRaw := range []bool{false, true} {
				accepted := evaluateContactPushTransportCondition(t, conditions["acceptSecureInbound"], s, proofRaw)
				dial := evaluateContactPushTransportCondition(t, conditions["dialV2"], s, proofRaw)
				bound := evaluateContactPushTransportCondition(t, conditions["provenTransport"], s, proofRaw)
				if accepted != (mode.nin || mode.find || mode.k) || dial != (mode.find || mode.k) || bound != ((mode.find || mode.k) && proofRaw) {
					t.Fatal("actual predicate mode boundary changed", mode.name, proofRaw, accepted, dial, bound)
				}
			}
			if a := s.identityResolver; a != nil {
				a.recordAdmission.kReady = true
				a.recordAdmission.kClosing = true
				a.recordAdmission.closing = true
				if s.hasContactRecordPushTransportCapability() != mode.k {
					t.Fatal("runtime lifecycle changed immutable transport capability")
				}
			}
		})
	}
	var missing *Service
	if missing.hasContactRecordPushTransportCapability() {
		t.Fatal("nil Service acquired K transport capability")
	}
	if (&Service{}).hasContactRecordPushTransportCapability() {
		t.Fatal("missing resolver acquired K capability")
	}
	for _, variant := range []string{"missing-admission", "None", "R-only", "unknown-class", "foreign-service", "foreign-resolver", "foreign-resolver-service", "foreign-network", "foreign-equal-network"} {
		t.Run(variant, func(t *testing.T) {
			s := &Service{}
			r := &identityResolver{svc: s, network: domain.NetworkID(networkName)}
			a := &relationRecordAdmission{classes: recordAdmissionImmediateK, service: s, resolver: r, network: r.network}
			r.recordAdmission = a
			s.identityResolver = r
			switch variant {
			case "missing-admission":
				r.recordAdmission = nil
			case "None":
				a.classes = recordAdmissionNone
			case "R-only":
				a.classes = recordAdmissionChargedR
			case "unknown-class":
				a.classes = recordAdmissionImmediateK | recordAdmissionClasses(128)
			case "foreign-service":
				a.service = &Service{}
			case "foreign-resolver":
				a.resolver = &identityResolver{}
			case "foreign-resolver-service":
				r.svc = &Service{}
			case "foreign-network":
				a.network = domain.NetworkID("other")
			case "foreign-equal-network":
				a.network = domain.NetworkID("other")
				r.network = a.network
			}
			if s.hasContactRecordPushTransportCapability() {
				t.Fatal("foreign/disabled immutable K owner accepted", variant)
			}
		})
	}
}

// Exact finite source contract for the one shared test helper, not a production
// caller allowance. Existing mode rows cover ordinary/Nin-only absence and
// FindNode/K presence; the genuine TLS rows above are separate evidence.
func contactPushFixtureOwnerContract(raw []byte) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "find_node_connection_test.go", raw, 0)
	if err != nil {
		return []string{err.Error()}
	}
	var body *ast.BlockStmt
	declarations := 0
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "f1PairOnRaw" {
			declarations++
			if fn.Recv == nil {
				body = fn.Body
			}
		}
	}
	if declarations != 1 || body == nil {
		return []string{"fixture original owner helper declaration changed"}
	}
	wanted := map[string]string{
		"expected": "expectsOutboundOwner := d.findNode != nil || d.hasContactRecordPushTransportCapability()",
		"own":      "fixtureOwner.outbound = p",
		"check":    `if (p.common != nil) != expectsOutboundOwner { t.Fatal("fixture outbound common does not match immutable constructor capability") }`,
		"link":     "if p.common != nil { p.common.mu.Lock() p.common.peer = p p.common.mu.Unlock() }",
	}
	// Compare words after formatting the actual top-level AST statements; no
	// nested decoy, comment or unrelated helper can satisfy this sequence.
	positions := map[string]int{}
	counts := map[string]int{}
	for index, statement := range body.List {
		var printed bytes.Buffer
		if err := format.Node(&printed, token.NewFileSet(), statement); err != nil {
			return []string{err.Error()}
		}
		text := strings.Join(strings.Fields(printed.String()), " ")
		if strings.HasPrefix(text, "p := &peerSession{") {
			counts["peer"]++
			positions["peer"] = index
		}
		for key, expected := range wanted {
			if text == expected {
				counts[key]++
				positions[key] = index
			}
		}
	}
	var failures []string
	for _, key := range []string{"expected", "peer", "own", "check", "link"} {
		if counts[key] != 1 {
			failures = append(failures, "fixture owner contract statement missing/duplicate: "+key)
		}
	}
	if len(failures) == 0 && (positions["expected"]+1 != positions["peer"] || positions["peer"]+1 != positions["own"] || positions["own"]+1 != positions["check"] || positions["check"]+1 != positions["link"]) {
		failures = append(failures, "fixture original owner cleanup/optional-link order changed")
	}
	return failures
}
