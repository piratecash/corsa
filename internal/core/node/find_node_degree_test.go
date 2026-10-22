package node

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func degreeTestService(t *testing.T, maximum int) (*Service, *atomic.Int64) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	clock := new(atomic.Int64)
	s, err := newDegreeFixtureCandidateService(cfg, id, nil, degreeProfile{MaxActive: maximum}, clock)
	if err != nil {
		t.Fatal(err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	return s, clock
}

type degreeActualResult struct {
	wire net.Conn
	err  error
}

// A completion is published once, after the actual inbound call returns. The
// joined channel is a separate observation: a buffered result alone is no join.
type degreeActualCompletion struct {
	result     degreeActualResult
	returnedAt time.Time
}

type degreeActualWorker struct {
	ctx        context.Context
	cancel     context.CancelFunc
	deadline   time.Time
	server     net.Conn
	client     net.Conn
	outbound   net.Conn // assigned/read only by the test goroutine
	beforeStop func()   // release the fixture writer mutex before cancellation
	stopOnce   sync.Once
	joined     chan struct{}
	completion atomic.Pointer[degreeActualCompletion]
	owned      atomic.Pointer[peekedConn]
	s, d       *Service
}

func degreeStartActualWorker(t *testing.T, s, d *Service, server, client net.Conn, metered *netcore.MeteredConn, beforeStop func(), entered chan<- uint64) *degreeActualWorker {
	t.Helper()
	// One original aggregate deadline; no phase restarts or completion grace.
	deadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	w := &degreeActualWorker{ctx: ctx, cancel: cancel, deadline: deadline, server: server, client: client, beforeStop: beforeStop, joined: make(chan struct{}), s: s, d: d}
	t.Cleanup(func() { w.stop(t) })
	go func() {
		defer close(w.joined)
		if entered != nil {
			entered <- relationGateCurrentGID()
		}
		wire, _, _, err := s.openInboundTransport(metered, metered)
		if owned, ok := wire.(*peekedConn); ok {
			w.owned.Store(owned)
		}
		w.completion.Store(&degreeActualCompletion{result: degreeActualResult{wire, err}, returnedAt: time.Now()})
	}()
	return w
}

func (w *degreeActualWorker) stop(t *testing.T) {
	t.Helper()
	w.stopOnce.Do(func() {
		// Fatal/Goexit must not leave persistence behind the fixture-held mutex.
		if w.beforeStop != nil {
			w.beforeStop()
		}
		w.cancel()
		w.s.runCancel()
		w.d.runCancel()
		_ = w.server.Close()
		_ = w.client.Close()
		if w.outbound != nil {
			_ = w.outbound.Close()
		}
		if wire := w.owned.Load(); wire != nil {
			_ = wire.Close()
		}
		select {
		case <-w.joined:
		case <-time.After(2 * time.Second):
			t.Error("degree actual worker did not join within cleanup watchdog")
			// Keep the failure and fence TempDir/service cleanup. Only the
			// unchanged package timeout may terminate an unjoined worker.
			<-w.joined
		}
		// The worker may have published its original transport during abort.
		if wire := w.owned.Load(); wire != nil {
			_ = wire.Close()
		}
	})
}

func (w *degreeActualWorker) wait(t *testing.T, dialErr error) degreeActualResult {
	t.Helper()
	select {
	case <-w.joined:
	case <-w.ctx.Done():
	}
	// Check after observing join, never turn a late ready result into success.
	// Completion time is diagnostic only; it cannot substitute for joined.
	if w.ctx.Err() != nil || !time.Now().Before(w.deadline) {
		w.stop(t)
		t.Fatalf("degree actual deadline: dial=%v inbound=%+v context=%v", dialErr, w.completion.Load(), w.ctx.Err())
	}
	completed := w.completion.Load()
	if completed == nil {
		w.stop(t)
		t.Fatal("degree actual worker joined without completion", dialErr)
	}
	return completed.result
}

// Actual Node Accept/Dial paths, no registry/auth/CM or negotiated F1. The
// original accepted common has id0. A real TCP pair and both workers are owned
// before any fallible handshake. No synthetic ProvenSession or late capability.
func degreeActualAttempt(t *testing.T, s, d *Service) (degreeActualResult, *dialledTransport, error) {
	t.Helper()
	server, client := tcpPair(t)
	metered, err := netcore.NewMeteredConn(server, &s.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	worker := degreeStartActualWorker(t, s, d, server, client, metered, nil, nil)
	address := domain.PeerAddress(server.LocalAddr().String())
	tr, dialErr := d.dialPeerTransport(worker.ctx, address, domain.DialToAddress(), func(context.Context) (net.Conn, error) { return client, nil })
	if tr != nil {
		worker.outbound = tr.wire
	}
	got := worker.wait(t, dialErr)
	// Preserve both actual outcomes, including the deliberate inbound mandatory
	// pin refusal. Callers decide success only after this confirmed worker join.
	return got, tr, dialErr
}
func degreeActualPair(t *testing.T, s, d *Service) (*peekedConn, *dialledTransport) {
	t.Helper()
	got, tr, err := degreeActualAttempt(t, s, d)
	if got.err != nil || err != nil || tr == nil {
		t.Fatal("degree genuine pair", got.err, err)
	}
	wire, ok := got.wire.(*peekedConn)
	if !ok || wire.common.Load() == nil || tr.common == nil {
		t.Fatal("degree missing original all-proof common")
	}
	return wire, tr
}
func degreeWant(t *testing.T, s *Service, n int) degreeCut {
	t.Helper()
	cut, err := s.degree.degreeSnapshot()
	if err != nil || cut.window != n {
		t.Fatalf("degree complete cut: got%+v err%v want%d", cut, err, n)
	}
	return cut
}
func degreeRefused(t *testing.T, s *Service) {
	t.Helper()
	cut, err := s.degree.degreeSnapshot()
	if !errors.Is(err, errDegreeUnavailable) || cut != (degreeCut{}) {
		t.Fatal("degree incomplete coverage published", cut, err)
	}
}

func TestDegreeGenuineNoF1AcceptedID0AndOutboundBeforeCM(t *testing.T) {
	s, _ := degreeTestService(t, 2)
	d, _ := degreeTestService(t, 2)
	wire, tr := degreeActualPair(t, s, d)
	l := wire.common.Load()
	l.mu.Lock()
	id, entry, w := l.id, l.entry, l.degreeWitness
	l.mu.Unlock()
	if s.findNode != nil || d.findNode != nil || id != 0 || entry != nil || w == nil {
		t.Fatal("degree P1 was restricted to F1 or registered owner")
	}
	if tr.common == l || tr.common.id == 0 || tr.common.degreeWitness == nil {
		t.Fatal("degree outbound original owner absent")
	}
	if degreeWant(t, s, 1).incoming != 1 || degreeWant(t, d, 1).outgoing != 1 {
		t.Fatal("degree genuine immutable role lost")
	}
	s.peerMu.RLock()
	registered := len(s.conns)
	s.peerMu.RUnlock()
	if registered != 0 {
		t.Fatal("degree fixture unexpectedly registered accepted connection")
	}
}
func TestDegreeGenuineTerminalOnceAndExactRecentBoundary(t *testing.T) {
	s, c := degreeTestService(t, 1)
	d, _ := degreeTestService(t, 1)
	wire, _ := degreeActualPair(t, s, d)
	c.Store(int64(700 * time.Second))
	l := wire.common.Load()
	_ = wire.Close()
	l.end()
	l.end()
	c.Store(int64(1299 * time.Second))
	degreeWant(t, s, 1)
	c.Store(int64(1300 * time.Second))
	degreeWant(t, s, 0)
	s.degree.mu.Lock()
	unknown, n := s.degree.unknown, s.degree.active[0].life
	s.degree.mu.Unlock()
	if unknown != 0 || n != nil {
		t.Fatal("degree terminal obligation not released exactly once")
	}
}
func TestDegreeGenuineOverflowRefusesUntilUnknownEnds(t *testing.T) {
	s, c := degreeTestService(t, 1)
	d1, _ := degreeTestService(t, 1)
	d2, _ := degreeTestService(t, 1)
	first, _ := degreeActualPair(t, s, d1)
	second, _ := degreeActualPair(t, s, d2)
	degreeRefused(t, s)
	c.Store(int64(601 * time.Second))
	degreeRefused(t, s) // never silently truncate a still-active unknown
	c.Store(int64(700 * time.Second))
	_ = second.Close()
	degreeWant(t, s, 2)
	c.Store(int64(1300 * time.Second))
	degreeWant(t, s, 1) // tracked original is still live
	_ = first.Close()
	degreeWant(t, s, 1)
}
func TestDegreeGenuineSameIdentityDedupAndIncomingGroupTwo(t *testing.T) {
	s, _ := degreeTestService(t, 4)
	d1, _ := degreeTestService(t, 2)
	d2, _ := degreeTestService(t, 1)
	d3, _ := degreeTestService(t, 1)
	degreeActualPair(t, s, d1)
	degreeActualPair(t, s, d1)
	degreeWant(t, s, 1)
	degreeActualPair(t, s, d2)
	degreeActualPair(t, s, d3)
	cut := degreeWant(t, s, 2)
	if cut.incoming != 2 {
		t.Fatal("degree incoming group quota lost")
	}
}
func TestDegreeGenuineMandatoryPinRefusalNeverPublishesP1(t *testing.T) {
	s, _ := degreeTestService(t, 1)
	d, _ := degreeTestService(t, 1)
	s.secureSessions.store.mu.Lock()
	s.secureSessions.store.evidenceRevision = identityPinEvidenceRevision(^uint64(0))
	s.secureSessions.store.mu.Unlock()
	got, _, _ := degreeActualAttempt(t, s, d)
	if !errors.Is(got.err, domain.ErrIdentityPinRevisionExhausted) {
		t.Fatal("degree actual mandatory pin refusal not reached", got.err)
	}
	degreeWant(t, s, 0)
}
func TestDegreeGenuineClockRegressionRefusesOnlyObservation(t *testing.T) {
	s, c := degreeTestService(t, 2)
	d, _ := degreeTestService(t, 1)
	degreeActualPair(t, s, d)
	c.Store(int64(time.Second))
	degreeWant(t, s, 1)
	c.Store(0)
	degreeRefused(t, s)
	// An ordinary new established transport still succeeds while observation is poisoned.
	d2, _ := degreeTestService(t, 1)
	wire, _ := degreeActualPair(t, s, d2)
	if wire.common.Load().ended {
		t.Fatal("degree clock failure stopped ordinary transport")
	}
	degreeRefused(t, s)
}
func TestDegreeGenuineDuplicateOldEndCannotFreeReusedSlot(t *testing.T) {
	s, _ := degreeTestService(t, 1)
	d1, _ := degreeTestService(t, 1)
	d2, _ := degreeTestService(t, 1)
	old, _ := degreeActualPair(t, s, d1)
	oldLife := old.common.Load()
	_ = old.Close()
	next, _ := degreeActualPair(t, s, d2)
	newLife := next.common.Load()
	oldLife.end()
	oldLife.end()
	s.degree.mu.Lock()
	current := s.degree.active[0].life
	s.degree.mu.Unlock()
	if current != newLife {
		t.Fatal("degree stale terminal freed new slot")
	}
	degreeWant(t, s, 2)
}
func TestDegreeGenuineCMStaleAndShutdownTerminalPaths(t *testing.T) {
	for _, mode := range []string{"cm-stale", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s, c := degreeTestService(t, 1)
			d, dc := degreeTestService(t, 1)
			wire, tr := degreeActualPair(t, s, d)
			c.Store(int64(time.Second))
			dc.Store(int64(time.Second))
			if mode == "cm-stale" {
				p := &peerSession{common: tr.common, connID: tr.connID, conn: tr.wire, address: "127.0.0.1:1", sendCh: make(chan peerSendItem, 1), inboxCh: make(chan protocol.Frame, 1), errCh: make(chan error, 1)}
				d.connManager.handleDialSucceeded(d.runCtx, DialSucceeded{Address: p.address, ConnectedAddress: p.address, SlotGeneration: 99, Session: p})
				d.degree.mu.Lock()
				live := d.degree.active[0].life
				d.degree.mu.Unlock()
				if live != nil {
					t.Fatal("degree CM stale path did not end original")
				}
			} else {
				if err := s.registerInboundConn(wire, nil); err != nil {
					t.Fatal(err)
				}
				s.closeAllInboundConns()
				s.degree.mu.Lock()
				live := s.degree.active[0].life
				s.degree.mu.Unlock()
				if live != nil {
					t.Fatal("degree shutdown did not end original")
				}
			}
		})
	}
}
func TestDegreeGenuineConcurrentCutsAndDuplicateClose(t *testing.T) {
	s, c := degreeTestService(t, 2)
	d, _ := degreeTestService(t, 1)
	wire, _ := degreeActualPair(t, s, d)
	c.Store(int64(time.Second))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				_, _ = s.degree.degreeSnapshot()
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); wire.common.Load().end() }()
	}
	wg.Wait()
	degreeWant(t, s, 1)
}
func TestDegreeGenuineSlotGenerationAndUnknownExhaustionRefuse(t *testing.T) {
	for _, kind := range []string{"generation", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := degreeTestService(t, 1)
			d, _ := degreeTestService(t, 1)
			if kind == "generation" {
				s.degree.mu.Lock()
				s.degree.active[0].generation = ^uint64(0)
				s.degree.mu.Unlock()
			} else {
				degreeActualPair(t, s, d)
				s.degree.mu.Lock()
				s.degree.unknown = ^uint64(0)
				s.degree.mu.Unlock()
				d, _ = degreeTestService(t, 1)
			}
			wire, _ := degreeActualPair(t, s, d)
			if wire.common.Load().ended {
				t.Fatal("degree exhaustion refused ordinary session")
			}
			degreeRefused(t, s)
		})
	}
}
func TestDegreeProfileCopiedAndOrdinaryDefaultsRemainOff(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	for _, maximum := range []int{0, -1, int(^uint(0) >> 1)} {
		if s, e := newDegreeCandidateService(cfg, id, nil, degreeProfile{MaxActive: maximum}); !errors.Is(e, errDegreeProfile) || s != nil {
			t.Fatal("degree invalid profile constructed Service")
		}
	}
	p := degreeProfile{MaxActive: 2}
	clock := new(atomic.Int64)
	s, err := newDegreeFixtureCandidateService(cfg, id, nil, p, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	p.MaxActive = 99
	if s.degree.profile.MaxActive != 2 || len(s.degree.active) != 2 || int(unsafe.Sizeof(degreeActiveRow{})) > degreeAllocationUnit {
		t.Fatal("degree profile not copied or row arithmetic premise false")
	}
	ordinary := NewService(cfg, id, nil)
	t.Cleanup(func() { ordinary.runCancel(); ordinary.closeBackgroundAdmission(); ordinary.WaitBackground() })
	if ordinary.degree != nil {
		t.Fatal("ordinary constructor activated degree")
	}
	if _, err := newDegreeFixtureCandidateService(cfg, id, nil, degreeProfile{MaxActive: 1}, nil); !errors.Is(err, errDegreeProfile) {
		t.Fatal("degree zero clock accepted")
	}
}

// Pure history algebra test, not a ProvenSession/transport authority fixture.
func degreeOracleWindow(events map[degreeKey]time.Duration, at time.Duration) int {
	count := 0
	groups := map[domain.SourceGroup]int{}
	for key, last := range events {
		if at-last >= degreeRecent {
			continue
		}
		if key.outgoing {
			count++
		} else {
			groups[key.group]++
		}
	}
	for _, n := range groups {
		count += min(n, 2)
	}
	return min(count, degreeHistoryLimit)
}
func TestDegreeFixedHistoryMatchesIndependentUnboundedOracle(t *testing.T) {
	for seed := int64(0); seed < 16; seed++ {
		random := rand.New(rand.NewSource(seed))
		o := &degreeOwner{}
		events := map[degreeKey]time.Duration{}
		var at time.Duration
		for step := 0; step < 2048; step++ {
			at += time.Duration(random.Intn(15)) * time.Second
			var id domain.PeerIdentity
			id[0] = byte(1 + random.Intn(200))
			id[1] = byte(random.Intn(2))
			key := degreeKey{outgoing: random.Intn(2) == 0, identity: id}
			if !key.outgoing {
				key.group = domain.SourceGroup(string(rune('a' + random.Intn(16))))
			}
			events[key] = at
			o.degreeHistoryLocked(key, at)
			got := o.historyN
			want := degreeOracleWindow(events, at)
			if got != want || got > 64 {
				t.Fatalf("degree history oracle seed%d step%d got%d want%d", seed, step, got, want)
			}
		}
	}
}
func TestDegreeHistoryCapAndExactHalfOpen600(t *testing.T) {
	o := &degreeOwner{}
	for i := 1; i <= 100; i++ {
		var id domain.PeerIdentity
		id[0] = byte(i)
		o.degreeHistoryLocked(degreeKey{outgoing: true, identity: id}, 0)
	}
	if o.historyN != 64 {
		t.Fatal("degree history cap incorrect")
	}
	o.clock = degreeClock{fixture: new(atomic.Int64)}
	o.clock.fixture.Store(int64(600 * time.Second))
	cut, err := o.degreeSnapshot()
	if err != nil || cut.window != 0 {
		t.Fatal("degree half-open expiry incorrect", cut, err)
	}
}

func TestDegreeActualPinWriterWaitHasNoEarlyP1(t *testing.T) {
	s, c := degreeTestService(t, 1)
	d, _ := degreeTestService(t, 1)
	server, client := tcpPair(t)
	metered, err := netcore.NewMeteredConn(server, &s.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	store := s.secureSessions.store
	store.writeMu.Lock()
	held := true
	releaseWriter := func() {
		if held {
			store.writeMu.Unlock()
			held = false
		}
	}
	gid := make(chan uint64, 1)
	worker := degreeStartActualWorker(t, s, d, server, client, metered, releaseWriter, gid)
	proof, dialErr := sessionv2.Dial(worker.ctx, client, d.secureSessions.local, d.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if dialErr != nil {
		worker.stop(t)
		t.Fatal("degree pin-wait dial", dialErr, worker.completion.Load())
	}
	protected, err := proof.Conn()
	if err != nil {
		worker.stop(t)
		t.Fatal("degree pin-wait protected transport", err, dialErr, worker.completion.Load())
	}
	worker.outbound = protected
	n1WaitMutexFrame(t, <-gid, "persist(")
	if worker.ctx.Err() != nil || !time.Now().Before(worker.deadline) {
		worker.stop(t)
		t.Fatal("degree pin-wait deadline at mandatory writer barrier", worker.completion.Load())
	}
	degreeWant(t, s, 0)
	c.Store(int64(time.Second))
	releaseWriter()
	got := worker.wait(t, dialErr)
	if got.err != nil {
		t.Fatal(got.err)
	}
	if degreeWant(t, s, 1).at != time.Second {
		t.Fatal("degree timestamp was captured before mandatory writer wait")
	}
}
func TestDegreeGenuineRawTerminalBeforeBindingCannotPublishLateP1(t *testing.T) {
	s, _ := degreeTestService(t, 1)
	d, _ := degreeTestService(t, 1)
	server, client := tcpPair(t)
	raw := &peekedConn{Conn: client, reader: client}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type accepted struct {
		proof sessionv2.ProvenSession
		err   error
	}
	results := make(chan accepted, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Error("degree original raw proof worker not joined")
		}
	})
	go func() {
		defer close(joined)
		proof, e := sessionv2.Accept(ctx, server, s.secureSessions.local, s.welcomeFrame("", ""), s.secureSessions.certificates, sessionv2.DefaultTimeouts)
		results <- accepted{proof, e}
	}()
	proof, err := sessionv2.Dial(ctx, raw, d.secureSessions.local, d.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	var got accepted
	select {
	case got = <-results:
	case <-ctx.Done():
		t.Fatal("degree raw proof did not return")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	_ = raw.Close() // actual original terminal before common CAS, not a fake proof
	metered, err := netcore.NewMeteredConn(client, &d.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	address := domain.PeerAddress(server.LocalAddr().String())
	tr := &dialledTransport{raw: client, metered: metered, proofRaw: raw, connID: d.allocateConnID("degree-before-bind", address)}
	tr, err = d.provenTransport(ctx, tr, proof, address, domain.DialToAddress(), d.nodeHelloFrame())
	if err != nil {
		t.Fatal(err)
	}
	if tr.common == nil || !tr.common.ended || tr.common.degreeWitness != nil {
		t.Fatal("degree terminal replay did not win P1")
	}
	degreeWant(t, d, 0)
}

func TestDegreeProductionClockOwnsOriginalMonotonicReading(t *testing.T) {
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", EnableDatagramV1: true, AllowPrivatePeers: true, Type: domain.NodeTypeFull})
	s, err := newDegreeCandidateService(cfg, id, nil, degreeProfile{MaxActive: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	// Round(0) removes only the monotonic reading. This observes owned clock
	// provenance; it does not simulate a wall-clock change or alter the host.
	// Full-value equality is deliberate: it includes the monotonic reading.
	if s.degree.clock.fixture != nil || s.degree.clock.origin.IsZero() || [1]time.Time{s.degree.clock.origin} == [1]time.Time{s.degree.clock.origin.Round(0)} {
		t.Fatal("degree production clock lacks original monotonic reading")
	}
	fixture, atomicClock := degreeTestService(t, 1)
	if fixture.degree.clock.fixture != atomicClock || !fixture.degree.clock.origin.IsZero() {
		t.Fatal("degree fixture and production clock modes mixed")
	}
}

// Both witnesses come from the authentic Accept/Dial P1 mint. A full table
// exercises the real untracked path; equal peer identity does not merge lifetimes.
func TestDegreeWitnessOriginGenuineTrackedAndFullCapacity(t *testing.T) {
	s, _ := degreeTestService(t, 1)
	d, _ := degreeTestService(t, 2)
	first, _ := degreeActualPair(t, s, d)
	original := first.common.Load()
	original.mu.Lock()
	tracked := original.degreeWitness
	trackedOrigin := tracked != nil && tracked.origin == original
	trackedShape := tracked != nil && !tracked.untracked && tracked.slot == 0
	original.mu.Unlock()
	if !trackedOrigin || !trackedShape {
		t.Fatal("genuine tracked witness lost original lifetime")
	}
	second, _ := degreeActualPair(t, s, d)
	next := second.common.Load()
	next.mu.Lock()
	untracked := next.degreeWitness
	untrackedOrigin := untracked != nil && untracked.origin == next
	untrackedShape := untracked != nil && untracked.untracked && untracked.slot == -1
	next.mu.Unlock()
	if !untrackedOrigin || !untrackedShape {
		t.Fatal("genuine full-capacity witness lost original lifetime")
	}
	if next == original || untracked == tracked || untracked.origin == original || tracked.origin == next {
		t.Fatal("distinct genuine lifetimes shared witness origin")
	}
	s.degree.mu.Lock()
	row, unknown := s.degree.active[0], s.degree.unknown
	s.degree.mu.Unlock()
	if row.life != original || row.witness != tracked || unknown != 1 {
		t.Fatal("origin fixture did not reach actual full-capacity accounting")
	}
	degreeRefused(t, s)
}
