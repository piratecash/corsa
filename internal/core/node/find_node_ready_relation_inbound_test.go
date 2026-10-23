package node

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

// All clock/callback seams below are installed before the one original Run.
// No test calls a live pass, stage, ACK callback or strict confirmer.
type readyRelationFixture struct {
	ready    *findNodeReadyConstruction
	own      *ownDMRuntimeConstruction
	dm       ownDMAdmissionFixture
	nin      *ninFixtureClock
	incoming f1ProofFixture
	member   findnode.Session
	z        domain.PeerIdentity
	samples  atomic.Int64
	cancel   context.CancelFunc
	exited   <-chan struct{}
}

type readyRelationStrictHold struct {
	entered  chan struct{}
	written  chan error
	release  chan struct{}
	returned chan struct{}
	once     sync.Once
	calls    atomic.Int32
}

func (h *readyRelationStrictHold) unblock() { h.once.Do(func() { close(h.release) }) }

func readyRelationBuild(t *testing.T, hold *readyRelationStrictHold) *readyRelationFixture {
	t.Helper()
	peer, address, stopPeer := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopPeer)
	cfg, id, nc := readyInboundInputs(t)
	cfg.ListenAddress = freeAddress(t)
	cfg.BootstrapPeers = []string{normalizeAddress(address)}
	cfg = deriveTestAdvertisePort(cfg)
	r := readyTestPrepare(t, cfg, id)
	own := &ownDMRuntimeConstruction{profile: OwnDMResidenceProfile{MaxPrestage: 2 * time.Minute}}
	s, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, findNodeReadyOptions{ownDM: own, loadedInbound: true}, inboundPinClock{elapsed: nc.read})
	if err != nil || s == nil {
		t.Fatal("original Ready/R/N-in construction refused", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	f := &readyRelationFixture{ready: r, own: own, nin: nc}
	clock := &ownDMAdmissionClock{wall: time.Now()}
	s.identityResolver.recordAdmission.clock = func() relationObservedTime { f.samples.Add(1); return clock.sample() }
	store := s.localContactPinStore()
	store.mu.Lock()
	loaded := store.ninFormat3 && store.ninJournal != nil && !store.ninJournal.strictConfirmed()
	store.mu.Unlock()
	if !loaded || !r.bindings(s) || !r.checked || !r.finalized || !r.bridge.statisticInstalled || own.resolver != s.identityResolver || own.admission.admission != s.identityResolver.recordAdmission || s.inboundPins.store != store || store.pinPublisher != s.bridgeTerminal.publisher || s.identityResolver.recordAdmission.started != nil {
		t.Fatal("original composed constructor tuple/strict-first load differs")
	}
	gate := &r2aWireGate{entered: make(chan struct{}), release: make(chan struct{})}
	actualDial := s.peerTCPDial
	s.peerTCPDial = func(ctx context.Context, address string, timeout time.Duration) (net.Conn, error) {
		c, err := actualDial(ctx, address, timeout)
		if err != nil {
			return nil, err
		}
		return &r2aGatedConn{Conn: c, gate: gate, stopped: make(chan struct{})}, nil
	}
	if hold != nil {
		original := s.identityResolver.recordAdmission.openFiles
		s.identityResolver.recordAdmission.openFiles = func() (identityPinDurableFiles, error) {
			files, err := original()
			if err != nil {
				return nil, err
			}
			return &relationServiceReturnHeldFiles{identityPinDurableFiles: files, entered: hold.entered, written: hold.written, allowReturn: hold.release, returned: hold.returned, calls: &hold.calls}, nil
		}
	}
	parent, cancel := context.WithCancel(t.Context())
	running := runServiceForTest(parent, s)
	f.cancel, f.exited = cancel, running.exited
	t.Cleanup(func() {
		cancel()
		contactPushJoin(t, running.exited)
		if running.runErr != nil && !errors.Is(running.runErr, context.Canceled) {
			t.Error(running.runErr)
		}
	})
	t.Cleanup(gate.unblock)
	if hold != nil {
		t.Cleanup(hold.unblock)
	}
	ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
	defer done()
	if err := running.AwaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	var publication r2aHopPublication
	relationServiceWait(t, "composed original Run startup and CM publication", func() bool {
		s.identityResolver.mu.Lock()
		started := s.identityResolver.recordAdmission.started
		zero := started != nil && started.elapsed == 0
		s.identityResolver.mu.Unlock()
		store.mu.Lock()
		confirmed := store.ninJournal.strictConfirmed()
		store.mu.Unlock()
		s.peerMu.RLock()
		defer s.peerMu.RUnlock()
		for _, session := range s.sessions {
			if hop := session.r2OwnDMHop; hop != nil && hop.r2PublishedReady && hop.session == session && session.selectedGeneration != 0 {
				publication = r2aHopPublication{sealed: hop}
			}
		}
		return zero && confirmed && publication.sealed != nil
	})
	s.identityResolver.mu.Lock()
	_, early := s.identityResolver.recordAdmission.rateTrial(clock.sample())
	s.identityResolver.mu.Unlock()
	if early {
		t.Fatal("original R startup60 was bypassed")
	}
	clock.elapsed.Store(int64(time.Minute))
	f.dm = ownDMAdmissionFixture{svc: s, port: own.port, peer: peer, publication: publication, wire: gate, clock: clock}
	dc, di := readyTestInputs(t)
	dialer := NewService(dc, di, nil)
	t.Cleanup(func() { dialer.runCancel(); dialer.closeBackgroundAdmission(); dialer.WaitBackground() })
	f.incoming, f.member = ownedRawAccepted(t, s, dialer)
	f.z = domain.PeerIdentityFromWire(di.Address)
	if f.z == publication.sealed.binding.identity || f.z == domain.PeerIdentityFromWire(s.identity.Address) || f.z == domain.PeerIdentityFromWire(peer.identity.Address) {
		t.Fatal("incoming Z is not distinct from original owner/hop")
	}
	readyRelationClass(t, f, findnode.ClassOrdinary)
	return f
}

func readyRelationClass(t *testing.T, f *readyRelationFixture, want findnode.SessionClass) {
	t.Helper()
	facts, err := f.dm.svc.collectOwnedFindNodeRawFacts(t.Context(), f.dm.svc.bridgeCapture.priority)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i, member := range facts.allOriginal {
		if member == f.member {
			if found || facts.classes[i] != want {
				t.Fatal("same original Z lost expected Ready class")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("original Z absent from Ready capture")
	}
	if _, err := f.dm.svc.captureOwnedFindNodeCut(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func readyRelationPending(t *testing.T, f *readyRelationFixture) (*identity.Identity, OwnDMSubmission) {
	t.Helper()
	x := ownDMRecipient(t)
	xid := domain.PeerIdentityFromWire(x.Address)
	if xid == f.z || xid == f.dm.publication.sealed.binding.identity || xid == domain.PeerIdentityFromWire(f.dm.svc.identity.Address) {
		t.Fatal("original X/Y/Z identities overlap")
	}
	ownDMAdmissionRecord(t, f.dm.svc, x, domain.DeclaredMinSession(domain.SessionVersionSecure))
	ownDMAdmissionRecord(t, f.dm.svc, f.dm.peer.identity, domain.DeclaredMinSession(domain.SessionVersionSecure))
	store := f.dm.svc.localContactPinStore()
	store.mu.Lock()
	nonK := true
	for _, id := range []domain.PeerIdentity{xid, f.z, f.dm.publication.sealed.binding.identity} {
		nonK = nonK && store.identities[id].classMetadata().Contact.State == domain.IdentityPinAllocationAbsent
	}
	store.mu.Unlock()
	if !nonK {
		t.Fatal("X/Y/Z unexpectedly has K metadata")
	}
	_, receipt := ownDMAdmissionSend(t, f.dm, x)
	stamp := receipt.r2Submission.r2Result.stamp
	f.dm.svc.deliveryMu.RLock()
	candidate := stamp.candidate
	offered := candidate != nil && candidate.offered && candidate.stamp == stamp && stamp.r2Claim != nil
	f.dm.svc.deliveryMu.RUnlock()
	if !offered {
		t.Fatal("original submitted action has no exact first offered writer")
	}
	select {
	case <-candidate.ack:
		t.Fatal("held original writer already acknowledged")
	default:
	}
	// Submit has returned. Polling below reads atomics/owned state only, never
	// sampleLocked. With no typed resolutions, only Run's R worker samples now.
	baseline := f.samples.Load()
	relationServiceWait(t, "original R worker attempted pending ACK", func() bool { return f.samples.Load() >= baseline+2 })
	r := f.dm.svc.identityResolver
	r.mu.Lock()
	prospective := true
	for role := range stamp.r2Claim.refs {
		slot := r.relationSlotLocked(&stamp.r2Claim.refs[role])
		prospective = prospective && slot != nil && slot.r2Claim == stamp.r2Claim && slot.phase == relationAdmissionProspective
	}
	noLookups := len(r.resolutions) == 0 && len(r.attempts) == 0
	r.mu.Unlock()
	rows, debits := ownDMAdmissionRows(f.dm.svc)
	stats := f.dm.svc.SecureSessionStoreStats()
	if !noLookups {
		t.Fatal("unexpected lookup interfered with original worker sampling")
	}
	if !prospective || rows != 2 || debits != 0 || stats.RelationAllocations != 0 || stats.RelationConfirmed != 0 {
		t.Fatal("held original writer ACK became R stage or debit")
	}
	return x, receipt
}

func readyRelationMature(t *testing.T, f *readyRelationFixture) *ninJournalImage {
	t.Helper()
	f.nin.advance(10 * time.Minute)
	relationServiceWait(t, "original Run N-in sweep strictly confirmed Z", func() bool {
		store := f.dm.svc.localContactPinStore()
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.inboundAllocations == 1 && store.inboundConfirmed == 1 && store.ninJournal.strictConfirmed()
	})
	readyRelationClass(t, f, findnode.ClassIncomingPinned)
	file := readyRelationDisk(t, f)
	if file.InboundQuota == nil || len(file.InboundQuota.rows) != 1 || file.InboundQuota.rows[0].count != 1 || file.IdentityMetadata[f.z].Inbound.State != domain.IdentityPinAllocationEligible {
		t.Fatal("actual strict image lost original Z/N-in quota")
	}
	return file.InboundQuota
}
func readyRelationDisk(t *testing.T, f *readyRelationFixture) secureSessionFile {
	t.Helper()
	store := f.dm.svc.localContactPinStore()
	raw, err := os.ReadFile(filepath.Join(store.dir, store.name))
	if err != nil {
		t.Fatal(err)
	}
	file, err := decodeSecureSessionFile(raw)
	if err != nil || file.Format != secureSessionJournalFormat || file.InboundQuota == nil {
		t.Fatal("actual strict format3 N-in image was lost", err)
	}
	return file
}
func readyRelationPreserved(t *testing.T, f *readyRelationFixture, before *ninJournalImage) {
	t.Helper()
	file := readyRelationDisk(t, f)
	if !reflect.DeepEqual(before, file.InboundQuota) || file.IdentityMetadata[f.z].Inbound.State != domain.IdentityPinAllocationEligible {
		t.Fatal("R strict snapshot lost actual N-in journal/history/quota")
	}
	readyRelationClass(t, f, findnode.ClassIncomingPinned)
}

func TestOwnedReadyRelationInboundActualACKAndStrictCoexist(t *testing.T) {
	f := readyRelationBuild(t, nil)
	x, receipt := readyRelationPending(t, f)
	before := readyRelationMature(t, f)
	f.dm.wire.unblock()
	ownDMAdmissionSettled(t, f.dm, 2)
	select {
	case <-receipt.r2Submission.r2Result.stamp.candidate.ack:
	default:
		t.Fatal("R completed without original writer ACK")
	}
	file := readyRelationDisk(t, f)
	xid := domain.PeerIdentityFromWire(x.Address)
	yid := f.dm.publication.sealed.binding.identity
	if file.IdentityMetadata[xid].Relation.Origin != domain.IdentityPinOriginOutgoingSend || file.IdentityMetadata[yid].Relation.Origin != domain.IdentityPinOriginProvenNextHop {
		t.Fatal("original X/Y R origins lost in strict image")
	}
	readyRelationPreserved(t, f, before)
}

func TestOwnedReadyRelationInboundCancelledBeforeACKPreservesNin(t *testing.T) {
	f := readyRelationBuild(t, nil)
	x, receipt := readyRelationPending(t, f)
	before := readyRelationMature(t, f)
	stamp := receipt.r2Submission.r2Result.stamp
	if _, err := f.dm.svc.CancelOutgoingDelivery(stamp.prepared.message.ID, domain.PeerIdentityFromWire(x.Address)); err != nil {
		t.Fatal(err)
	}
	f.dm.wire.unblock()
	relationServiceWait(t, "original cancelled R claim retired", func() bool { rows, _ := ownDMAdmissionRows(f.dm.svc); return rows == 0 })
	_, debits := ownDMAdmissionRows(f.dm.svc)
	stats := f.dm.svc.SecureSessionStoreStats()
	if debits != 0 || stats.RelationAllocations != 0 || stats.RelationConfirmed != 0 {
		t.Fatal("cancelled original action earned R stage or debit")
	}
	readyRelationPreserved(t, f, before)
}

func TestOwnedReadyRelationInboundRunJoinsOriginalStrictReturn(t *testing.T) {
	h := &readyRelationStrictHold{entered: make(chan struct{}), written: make(chan error, 1), release: make(chan struct{}), returned: make(chan struct{})}
	f := readyRelationBuild(t, h)
	_, receipt := readyRelationPending(t, f)
	before := readyRelationMature(t, f)
	var worker, run uint64
	t.Cleanup(func() {
		f.cancel()
		h.unblock()
		if worker != 0 {
			relationServiceWait(t, "original R callback worker cleanup", func() bool { _, alive := n1CompleteStacks(t)[worker]; return !alive })
		}
	})
	f.dm.wire.unblock()
	select {
	case <-h.entered:
	case <-time.After(8 * time.Second):
		t.Fatal("original R strict backend not entered")
	}
	select {
	case err := <-h.written:
		if err != nil {
			t.Fatal("original strict WriteMain failed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original strict WriteMain did not complete")
	}
	creator := regexp.MustCompile(`created by [^\n]*\(\*Service\)\.goRunLoop in goroutine ([1-9][0-9]*)\n`)
	relationServiceWait(t, "original R callback creator", func() bool {
		for id, stack := range n1CompleteStacks(t) {
			if !strings.Contains(stack, "(*relationServiceReturnHeldFiles).WriteMain(") || !strings.Contains(stack, "processRelationSlot(") {
				continue
			}
			worker = id
			match := creator.FindStringSubmatch(stack)
			if !strings.Contains(stack, "(*Service).goRunLoop.func1(") || len(match) != 2 {
				t.Fatal("composed R callback detached from original Run owner")
			}
			owner, err := strconv.ParseUint(match[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			run = owner
			return true
		}
		return false
	})
	f.cancel()
	r := f.dm.svc.identityResolver
	relationServiceWait(t, "composed Run closed admission", func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.recordAdmission.closing })
	relationServiceWait(t, "composed original Run waits held callback", func() bool {
		stacks := n1CompleteStacks(t)
		if stacks[worker] == "" {
			t.Fatal("held composed R callback returned early")
		}
		select {
		case <-f.exited:
			t.Fatal("composed Run returned before original R callback")
		default:
		}
		return strings.Contains(stacks[run], "stopRunLifecycle(") && strings.Contains(stacks[run], "sync.")
	})
	r.mu.Lock()
	var ref relationAdmissionRef
	owned := 0
	for _, candidate := range receipt.r2Submission.r2Result.stamp.r2Claim.refs {
		if slot := r.relationSlotLocked(&candidate); slot != nil && slot.processing && slot.phase >= relationAdmissionEarnedOwed {
			ref = candidate
			owned++
		}
	}
	counted := r.recordAdmission.rate.count
	r.mu.Unlock()
	if owned != 1 || counted != 1 || h.calls.Load() != 1 {
		t.Fatal("composed stop released counted Processing obligation")
	}
	h.unblock()
	contactPushJoin(t, f.exited)
	select {
	case <-h.returned:
	default:
		t.Fatal("composed Run joined before callback return")
	}
	r.mu.Lock()
	slot := &r.recordAdmission.slots[ref.index]
	retained := slot.generation == ref.generation && slot.phase == relationAdmissionStoppedOwed && !slot.processing && !slot.protectionHeld && slot.bodyLen == 0 && r.recordAdmission.rate.count == 1
	r.mu.Unlock()
	if !retained {
		t.Fatal("composed shutdown lost counted StoppedOwed history")
	}
	// After cancellation raw collection is lawfully unavailable. Check actual
	// disk/journal directly, without claiming successful pair completion.
	file := readyRelationDisk(t, f)
	if !reflect.DeepEqual(before, file.InboundQuota) {
		t.Fatal("joined R write lost N-in journal/history/quota")
	}
}
