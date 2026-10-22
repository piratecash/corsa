package netcore

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Genuine TLS proof but explicit logical core fixture inputs, NOT Node CM/
// same-domain transport provenance. The fault socket below is separate.
func resourceOriginalSession(t *testing.T, id domain.ConnID, opened findnode.Instant) findnode.Session {
	t.Helper()
	a, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	al, err := sessionv2.NewLocal(a, "owned-originals-fixture")
	if err != nil {
		t.Fatal(err)
	}
	bl, err := sessionv2.NewLocal(b, "owned-originals-fixture")
	if err != nil {
		t.Fatal(err)
	}
	certificates, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	// Kernel listen backlog permits Dial before synchronous Accept, so no
	// unowned Accept goroutine exists on an early connection failure.
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	joined := make(chan struct{})
	result := make(chan error, 1)
	// Registered before dispatch; both sockets close before the exact job join.
	t.Cleanup(func() { cancel(); _ = client.Close(); _ = server.Close(); <-joined })
	intro := protocol.Frame{Version: 31, Capabilities: []string{string(domain.CapOverlayFindNodeV1)}}
	go func() {
		defer close(joined)
		_, err := sessionv2.Accept(ctx, server, bl, intro, certificates, sessionv2.DefaultTimeouts)
		result <- err
	}()
	proof, err := sessionv2.Dial(ctx, client, al, intro, sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	<-joined
	member, err := findnode.NewSession(proof, id, "owned-originals", opened, []domain.Capability{domain.CapOverlayFindNodeV1})
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func resourceWork(t *testing.T) (findnode.DormantIntake, findnode.Session, findnode.OwnedForwardWork) {
	t.Helper()
	clock := new(atomic.Int64)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	d, err := findnodebridge.NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	profile := findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, Round: time.Millisecond, Slack: time.Millisecond, InitialTokens: 4}
	intake, sink, err := findnode.NewExecutingFixtureIntake(d.Consumer(), d.Observer(), profile, 2)
	if err != nil {
		t.Fatal(err)
	}
	source := resourceOriginalSession(t, 1, 0)
	target := resourceOriginalSession(t, 2, 0)
	point := func() (findnodebridge.CaptureStamp, findnodebridge.OwnedInstant) {
		stamp, err := d.Observer().BeginCapture()
		if err != nil {
			t.Fatal(err)
		}
		lease, err := d.Consumer().TryReadClock(stamp)
		if err != nil {
			t.Fatal(err)
		}
		at, err := lease.Sample()
		lease.Release()
		if err != nil {
			t.Fatal(err)
		}
		return stamp, at
	}
	cut := func() findnode.OwnedCut {
		stamp, at := point()
		before, err := at.BoundaryAfter(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		result, err := sink.Seal(stamp, at, before, []findnode.ObservedMember{{Session: source, Class: findnode.ClassOrdinary}, {Session: target, Class: findnode.ClassOrdinary}}, findnode.ObservedDegree{Incoming: 2, Window: 2})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request := protocol.FindNodeRequest{ID: 17, Target: target.Identity()}
	request.Challenge[0] = 23
	raw, err := protocol.MarshalFindNodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	stamp, at := point()
	input, err := sink.SealReceived(stamp, at, source, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := intake.OfferReceived(input, target, cut()); err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(time.Millisecond))
	batch, err := intake.CloseOwnedRound(cut())
	if err != nil {
		t.Fatal(err)
	}
	work, ok := batch.Next()
	if !ok {
		t.Fatal("genuine original C3a receipt absent")
	}
	return intake, target, work
}
func resourceWaitZero(t *testing.T, intake findnode.DormantIntake) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := intake.ResourceSnapshot().Writer
		if s.ForwardItems == 0 && s.AnswerItems == 0 && s.CellBytes == 0 && s.PayloadBytes == 0 && s.NamedRoundEstimateBytes == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("last owned item did not release original-owner custody", intake.ResourceSnapshot().Writer)
}
func TestOwnedResourceParkedProducerLateOfferDetachesOnlyOwnWrapper(t *testing.T) {
	intake, target, work := resourceWork(t)
	queued, err := work.QueueForOriginal(target)
	if err != nil {
		t.Fatal(err)
	}
	pc := New(41, newAutoConn(), Inbound, Options{})
	t.Cleanup(pc.Close)
	item := &ownedFindNodeSend{core: pc, queued: queued}
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	unblock := func() {
		if once.CompareAndSwap(false, true) {
			close(release)
		}
	}
	t.Cleanup(unblock)
	pc.enqueueBarrier = func() { close(entered); <-release }
	result := make(chan SendStatus, 1)
	producerDone := make(chan struct{})
	t.Cleanup(func() {
		unblock()
		select {
		case <-producerDone:
		case <-time.After(2 * time.Second):
			t.Error("exact parked producer did not join")
		}
	})
	go func() { defer close(producerDone); result <- pc.queueFrame(sendItem{owned: item}) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("owned producer missed exact preoffer barrier")
	}
	if held := intake.ResourceSnapshot().Writer; held.ForwardItems != 1 || held.CellBytes == 0 || held.PayloadBytes == 0 {
		t.Fatal("preoffer custody absent", held)
	}
	closed := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Error("exact closer did not join")
		}
	})
	go func() { pc.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("original consumer did not exit before lateoffer")
	}
	unblock()
	select {
	case status := <-result:
		if status != SendChanClosed {
			t.Fatal(status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late producer did not finish")
	}
	if item.custodyClaim.Load() != 2 || item.queued != (findnode.OwnedQueuedForward{}) || item.answer != (findnode.OwnedQueuedAnswer{}) || item.resourcesHeld || item.core != pc {
		t.Fatal("terminal producer retained handles or changed immutable header")
	}
	resourceWaitZero(t, intake)
	if len(pc.sendCh) != 1 {
		t.Fatal("producer drained the shared channel or lateoffer was not exercised")
	}
	if _, err := work.QueueForOriginal(target); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("terminal detachment revived exact work", err)
	}
}
func TestOwnedResourceTerminalProducerCannotClearClaimedLiveConsumer(t *testing.T) {
	intake, target, work := resourceWork(t)
	queued, err := work.QueueForOriginal(target)
	if err != nil {
		t.Fatal(err)
	}
	c := newGatedConn()
	pc := New(42, c, Inbound, Options{})
	t.Cleanup(pc.Close)
	item := &ownedFindNodeSend{core: pc, queued: queued}
	if status := pc.queueFrame(sendItem{owned: item}); status != SendOK {
		t.Fatal(status)
	}
	select {
	case <-c.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("original admitted writer did not reach actual Write")
	}
	// Execute the exact postoffer-refusal cleanup action while the real consumer
	// is inside raw Write. This isolates the losing producer branch; no forced
	// claim or fabricated work/permit is used.
	pc.gate.CompareAndSwap(int32(gateOpen), int32(gateSocketFailed))
	if status := pc.settleEnqueuedFrame(sendItem{owned: item}); status != SendWriterDone {
		t.Fatal(status)
	}
	item.retireUnclaimedOwnedItem()
	if item.custodyClaim.Load() != 1 {
		t.Fatal("producer stole original consumer custody")
	}
	// Only the atomic claim may be inspected during concurrent consumer use;
	// mutable wrapper fields are checked after writer completion below.
	if held := intake.ResourceSnapshot().Writer; held.ForwardItems != 1 || held.PayloadBytes == 0 {
		t.Fatal("producer released live original payload", held)
	}
	c.release <- nil
	resourceWaitZero(t, intake)
	pc.Close()
	if item.queued != (findnode.OwnedQueuedForward{}) || item.answer != (findnode.OwnedQueuedAnswer{}) || item.resourcesHeld || item.core != pc {
		t.Fatal("sole consumer final-use cleanup incomplete")
	}
}
