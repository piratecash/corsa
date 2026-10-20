package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func relationServiceFixture(t *testing.T) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, Type: domain.NodeTypeFull})
	s := newRelationRecordCandidateService(cfg, id, nil)
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s
}

func relationServiceWait(t *testing.T, description string, check func() bool) {
	t.Helper()
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("actual Service observation missing: " + description)
		case <-tick.C:
		}
	}
}

func relationServiceRun(t *testing.T, s *Service) (*runningTestService, *admissionTestClock) {
	t.Helper()
	r := s.identityResolver
	clock := &admissionTestClock{}
	clock.elapsed.Store(int64(time.Hour))
	clock.wallStep.Store(int64(time.Since(clock.sample().wall)))
	// Existing paired-clock fixture only. Startup belongs exclusively to Run.
	r.recordAdmission.clock = clock.sample
	r.clock = func() time.Time { return clock.sample().wall }
	r.mu.Lock()
	startedBeforeRun := r.recordAdmission.started
	r.mu.Unlock()
	if startedBeforeRun != nil {
		t.Fatal("construction started the R allowance epoch")
	}
	running := runServiceForTest(t.Context(), s)
	t.Cleanup(func() {
		running.cancel()
		contactPushJoin(t, running.exited)
		if running.runErr != nil && !errors.Is(running.runErr, context.Canceled) {
			t.Error(running.runErr)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := running.AwaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	relationServiceWait(t, "Run-owned startup sample", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.recordAdmission.started != nil
	})
	r.mu.Lock()
	started := *r.recordAdmission.started
	r.mu.Unlock()
	if started.elapsed != time.Hour {
		t.Fatal("startup allowance was not anchored to actual Run", started.elapsed)
	}
	return running, clock
}

// The registry is the actual production registration made before the final
// resolver exists. Invocation is at the handler boundary, not a TLS/network claim.
func relationServiceAnswer(t *testing.T, s *Service, peer *identity.Identity) *relationAdmissionRef {
	t.Helper()
	r := s.identityResolver
	target := domain.PeerIdentityFromWire(peer.Address)
	declarations := datagramPeerDeclarations()
	declarations.DeclaredDTypes = domain.ExplicitDTypes([]domain.DType{domain.DTypeGetIdentity})
	installDatagramPeer(t, s, target, datagramPeerConn{version: datagramTestPeerVersion, connectedAt: time.Now(), declared: &declarations})
	// installDatagramPeer supplies a class-queue peer, not a routing-table
	// entry. Run's resolver gates sends on the published routing snapshot;
	// with the paired clock frozen, a first no-route retry cannot become due.
	if best := s.loadRoutingSnapshot().BestRoute(target); best != nil {
		t.Fatal("fresh relation fixture already has a target route")
	}
	if _, err := s.routingTable.AddDirectPeer(target); err != nil {
		t.Fatal("relation fixture actual direct-route admission failed", err)
	}
	// Wait for Run's real publisher, including its ordinary coalescing floor.
	// Do not write the snapshot, force a rebuild or advance the resolver clock.
	relationServiceWait(t, "Run-owned direct-route snapshot publication", func() bool {
		best := s.loadRoutingSnapshot().BestRoute(target)
		return best != nil && best.Identity == target && best.NextHop == target && best.Hops == 1 && best.Source == routing.RouteSourceDirect
	})
	if _, err := s.StartUserIdentityLookup(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	var label domain.PeerIdentity
	var entry identityAttemptEntry
	relationServiceWait(t, "typed lookup actual SendLocal class-queue acceptance", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		for l, e := range r.attempts {
			if e.dst == target && e.recordAdmission != nil && e.queue == identityAttemptQueueAccepted {
				label, entry = l, e
				return true
			}
		}
		return false
	})
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{V: 1, TargetProof: true})
	if err != nil || sha256.Sum256(request) != entry.qHash {
		t.Fatal("actual SendLocal question differs", err)
	}
	record, err := protocol.BuildSignedIdentityRecord(peer, protocol.IdentityRecordSpec{Network: r.network, Seq: 3, IssuedAt: 1, DM: true, MinSession: domain.DeclaredMinSession(domain.SessionVersionSecure)})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := protocol.BuildPostIdentityPayload(protocol.PostIdentityPayload{V: 1, Record: record, TargetProof: protocol.SignTargetProof(peer, r.network, label, request, record)})
	if err != nil {
		t.Fatal(err)
	}
	header, err := datagram.NewDeliveryHeader(protocol.DatagramFrame{Version: domain.DatagramHeaderVersion, Mode: domain.DatagramModeResponse, Class: domain.DatagramClassControl, Src: target, Dst: label, TTL: 1, DType: domain.DTypePostIdentity, Payload: answer})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := datagram.NewDeliveryContext(datagram.DeliveryContextOpts{Header: header, IncomingPeer: datagram.ClaimedIngress(datagram.NetworkChannel(domain.ConnID(7501)), datagram.AcceptedConnectionKey(domain.ConnID(7501)), target), LocalIdentity: domain.PeerIdentityFromWire(s.identity.Address)})
	if err != nil {
		t.Fatal(err)
	}
	registered, ok := s.datagramLayer().types.Lookup(domain.DTypePostIdentity)
	if !ok {
		t.Fatal("registered post_identity absent")
	}
	handler, ok := registered.Handler().(*postIdentityHandler)
	if !ok || handler.svc != s {
		t.Fatal("post_identity did not capture original Service")
	}
	if result := handler.Handle(t.Context(), delivery, answer); result.Outcome() != datagram.HandlerAccepted {
		t.Fatal("registered signed answer refused", result.Err())
	}
	r.mu.Lock()
	_, stillResolving := r.resolutions[target]
	slot := r.relationSlotLocked(entry.recordAdmission)
	qualified := slot != nil && slot.phase == relationAdmissionQueued && slot.protectionHeld
	r.mu.Unlock()
	if stillResolving || !qualified {
		t.Fatal("normal key success did not precede queued R processing")
	}
	return entry.recordAdmission
}

func TestRelationRecordServiceConstructorFinalStoreAndDefaults(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true})
	constructor := NewMockdatagramConstructor(t)
	var registered *postIdentityHandler
	constructor.EXPECT().Build(mock.Anything, mock.Anything).RunAndReturn(func(s *Service, metrics *datagram.Metrics) (*datagramLayer, error) {
		if s.identityResolver != nil || s.secureSessions != nil {
			t.Fatal("R resolver captured an early store")
		}
		layer, err := newDatagramLayer(s, metrics)
		if err != nil {
			return nil, err
		}
		entry, ok := layer.types.Lookup(domain.DTypePostIdentity)
		if !ok {
			t.Fatal("actual post_identity not registered")
		}
		registered, ok = entry.Handler().(*postIdentityHandler)
		if !ok || registered.svc != s {
			t.Fatal("actual registration lost its Service")
		}
		return layer, nil
	}).Once()
	s := newServiceWithRecordAdmissionClasses(cfg, id, nil, nil, constructor, recordAdmissionConstruction{classes: recordAdmissionChargedR})
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	r := s.identityResolver
	a := r.recordAdmission
	pushRegistration, ok := s.datagramLayer().types.Lookup(domain.DTypePushIdentity)
	if !ok {
		t.Fatal("actual push_identity registration absent")
	}
	push, ok := pushRegistration.Handler().(*pushIdentityHandler)
	if !ok || push.svc != s || push.kPort != nil {
		t.Fatal("R-only registration captured a K port or another Service")
	}
	if registered.svc != s || a == nil || a.classes != recordAdmissionChargedR || a.service != s || a.resolver != r || a.network != r.network || a.started != nil || a.openFiles == nil || a.kFiles != nil || a.kReady || s.hasContactRecordPushTransportCapability() || s.inboundPins != nil || s.findNode != nil || s.selfRecordBody.Seq == 0 {
		t.Fatal("final R-only constructor dependency/default isolation changed")
	}
	files, err := a.openFiles()
	if err != nil {
		t.Fatal("final immutable store receiver unavailable", err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	pinned, ok := files.(*pinnedIdentityPinFiles)
	if !ok || pinned.mainName != s.localContactPinStore().name {
		t.Fatal("R factory did not bind the final actual store")
	}
	if err := files.Preflight(t.Context()); err != nil {
		t.Fatal("real final-store strict preflight refused", err)
	}
	normalCfg := configtest.WithTempDataDirs(t, config.Node{})
	normal := NewService(normalCfg, id, nil)
	t.Cleanup(func() { normal.runCancel(); normal.closeBackgroundAdmission(); normal.WaitBackground() })
	if normal.identityResolver.recordAdmission != nil || normal.hasContactRecordPushTransportCapability() {
		t.Fatal("ordinary construction enabled a record class")
	}
	for _, classes := range []recordAdmissionClasses{recordAdmissionChargedR | recordAdmissionImmediateK, 128} {
		t.Run(map[bool]string{true: "R-plus-K", false: "unknown"}[classes == recordAdmissionChargedR|recordAdmissionImmediateK], func(t *testing.T) {
			defer func() {
				if recover() != "unsupported Service record admission classes" {
					t.Fatal("unsupported Service classes did not refuse before construction")
				}
			}()
			_ = newServiceWithRecordAdmissionClasses(config.Node{}, nil, nil, nil, productionDatagramConstructor{}, recordAdmissionConstruction{classes: classes})
		})
	}
}

func TestRelationRecordServiceRunRegisteredAnswerRealStrictBackend(t *testing.T) {
	s := relationServiceFixture(t)
	_, clock := relationServiceRun(t, s)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ref := relationServiceAnswer(t, s, peer)
	r := s.identityResolver
	r.mu.Lock()
	beforeRate := r.recordAdmission.rate.count
	_, allowed := r.recordAdmission.rateTrial(clock.sample())
	r.mu.Unlock()
	if beforeRate != 0 || allowed || s.localContactPinStore().stats().RelationAllocations != 0 {
		t.Fatal("normal lookup success bypassed observed startup60")
	}
	clock.elapsed.Add(int64(time.Minute))
	relationServiceWait(t, "actual Run worker strict coverage and terminal barrier", func() bool {
		stats := s.localContactPinStore().stats()
		r.mu.Lock()
		defer r.mu.Unlock()
		return stats.RelationConfirmed == 1 && r.recordAdmission.rate.count == 1 && r.relationSlotLocked(ref) == nil
	})
	if s.liveRecordProtectionCounts()[domain.PeerIdentityFromWire(peer.Address).String()] != 0 {
		t.Fatal("actual terminal barrier retained live record protection")
	}
	store := s.localContactPinStore()
	raw, err := os.ReadFile(filepath.Join(store.dir, store.name))
	if err != nil || len(raw) == 0 {
		t.Fatal("real strict store file absent", err)
	}
	file, err := decodeSecureSessionFile(raw)
	if err != nil {
		t.Fatal("real strict store image invalid", err)
	}
	target := domain.PeerIdentityFromWire(peer.Address)
	if _, present := file.Identities[target]; !present || file.IdentityMetadata[target].Relation.State != domain.IdentityPinAllocationEligible || file.IdentityMetadata[target].Relation.Origin != domain.IdentityPinOriginUserLookup {
		t.Fatal("real strict image did not cover the original lookup identity and R provenance")
	}
}

// The real backend performs WriteMain first. Only its callback return is held;
// this qualifies Run's callback join, not a blocked kernel syscall or I/O latency.
type relationServiceReturnHeldFiles struct {
	identityPinDurableFiles
	entered     chan struct{}
	written     chan error
	allowReturn <-chan struct{}
	returned    chan struct{}
	calls       *atomic.Int32
}

func (f *relationServiceReturnHeldFiles) WriteMain(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
	f.calls.Add(1)
	close(f.entered)
	commit, err := f.identityPinDurableFiles.WriteMain(ctx, raw)
	if err == nil && commit != secretfile.DurableCommitted {
		err = errPinMetadataCommitUnconfirmed
	}
	f.written <- err
	<-f.allowReturn
	close(f.returned)
	return commit, err
}

func TestRelationRecordServiceRunJoinsActualStrictCallbackReturn(t *testing.T) {
	s := relationServiceFixture(t)
	r := s.identityResolver
	original := r.recordAdmission.openFiles
	entered, allowReturn, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	written := make(chan error, 1)
	var release sync.Once
	var calls atomic.Int32
	// A fixture-only wrapper around the ORIGINAL bound real backend. The
	// constructor and positive test above use the unmodified original method.
	r.recordAdmission.openFiles = func() (identityPinDurableFiles, error) {
		files, err := original()
		if err != nil {
			return nil, err
		}
		return &relationServiceReturnHeldFiles{identityPinDurableFiles: files, entered: entered, written: written, allowReturn: allowReturn, returned: returned, calls: &calls}, nil
	}
	// Registered before Run's join cleanup, then rearmed below for LIFO release.
	t.Cleanup(func() { release.Do(func() { close(allowReturn) }) })
	running, clock := relationServiceRun(t, s)
	t.Cleanup(func() { release.Do(func() { close(allowReturn) }) })
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ref := relationServiceAnswer(t, s, peer)
	clock.elapsed.Add(int64(time.Minute))
	select {
	case <-entered:
	case <-time.After(8 * time.Second):
		t.Fatal("actual Run worker did not enter strict backend")
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatal("delegated real strict WriteMain failed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("real strict WriteMain did not return to fixture wrapper")
	}
	// Reuse the complete-stack fixture to bind the actual blocked worker and
	// its creator Run GID. Cancellation alone is not evidence of a join.
	var worker, run uint64
	t.Cleanup(func() {
		running.cancel()
		release.Do(func() { close(allowReturn) })
		if worker != 0 {
			relationServiceWait(t, "observed R worker actual return during cleanup", func() bool { _, alive := n1CompleteStacks(t)[worker]; return !alive })
		}
	})
	creator := regexp.MustCompile(`created by [^\n]*\(\*Service\)\.goRunLoop in goroutine ([1-9][0-9]*)\n`)
	relationServiceWait(t, "actual strict callback original Run owner", func() bool {
		for id, stack := range n1CompleteStacks(t) {
			if !strings.Contains(stack, "(*relationServiceReturnHeldFiles).WriteMain(") || !strings.Contains(stack, "processRelationSlot(") {
				continue
			}
			if worker != 0 && worker != id {
				t.Fatal("multiple actual R strict callback workers")
			}
			worker = id // cleanup also reaps a deliberately detached causal mutant
			match := creator.FindStringSubmatch(stack)
			if !strings.Contains(stack, "(*Service).goRunLoop.func1(") || len(match) != 2 {
				t.Fatal("actual R strict callback detached from original Run lifecycle owner")
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
	running.cancel()
	relationServiceWait(t, "Run stop revokes admission while callback remains owned", func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.recordAdmission.closing })
	relationServiceWait(t, "actual Run lifecycle join wait", func() bool {
		stacks := n1CompleteStacks(t)
		if stacks[worker] == "" {
			t.Fatal("held R callback returned before release")
		}
		select {
		case <-running.exited:
			t.Fatal("Run returned before actual held R callback")
		default:
		}
		return strings.Contains(stacks[run], "stopRunLifecycle(") && strings.Contains(stacks[run], "sync.")
	})
	r.mu.Lock()
	slot := r.relationSlotLocked(ref)
	owned := slot != nil && slot.processing && slot.phase >= relationAdmissionEarnedOwed && r.recordAdmission.rate.count == 1
	r.mu.Unlock()
	if !owned || calls.Load() != 1 {
		t.Fatal("stop released counted Processing before actual callback return")
	}
	select {
	case <-running.exited:
		t.Fatal("Run returned before owned callback return")
	default:
	}
	select {
	case <-returned:
		t.Fatal("fixture callback returned before release")
	default:
	}
	release.Do(func() { close(allowReturn) })
	contactPushJoin(t, running.exited)
	select {
	case <-returned:
	default:
		t.Fatal("Run joined without callback return")
	}
	r.mu.Lock()
	stopped := &r.recordAdmission.slots[ref.index]
	retained := stopped.generation == ref.generation && stopped.phase == relationAdmissionStoppedOwed && !stopped.processing && !stopped.protectionHeld && stopped.bodyLen == 0 && r.recordAdmission.rate.count == 1
	r.mu.Unlock()
	if !retained {
		t.Fatal("actual stop did not retain exactly counted StoppedOwed history")
	}
}
