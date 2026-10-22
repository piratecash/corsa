package node

import (
	"errors"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fault endpoint tests the real NetCore writer boundary with an opaque work
// produced by genuine Node/TLS originals and actual Offer/C3b/C3c. It is not
// the actual original registered socket; that queue is exercised by idle-loop
// coverage. Faults cannot manufacture a Session, receipt, timestamp or permit.
type ownedWriterFault struct {
	net.Conn
	mode        string
	clock       *atomic.Int64
	beforeWrite func()
	writes      atomic.Int64
	closed      chan struct{}
	once        sync.Once
}

func (c *ownedWriterFault) SetWriteDeadline(time.Time) error {
	if c.mode == "deadline" {
		return errors.New("fixture deadline refusal")
	}
	return nil
}
func (c *ownedWriterFault) Write(p []byte) (int, error) {
	c.writes.Add(1)
	if c.beforeWrite != nil {
		c.beforeWrite()
	}
	switch c.mode {
	case "partial":
		return len(p) - 1, nil
	case "write-error":
		return 0, io.ErrClosedPipe
	case "finish-poison":
		c.clock.Store(-1)
	}
	return len(p), nil
}
func (c *ownedWriterFault) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type ownedWriterCapture struct{ f func(bool) }

func (s ownedWriterCapture) OnSendAttempt(_ []byte, ok bool) { s.f(ok) }
func ownedWriterActualWork(t *testing.T) (*Service, *atomic.Int64, f1ProofFixture, findnode.Session, findnode.OwnedForwardWork) {
	t.Helper()
	s, clock, _ := ownedExecutingTestServiceWorker(t, false)
	from, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	to, _, _ := ownedCaptureTestService(t, 8, 8, 16)
	source, _ := ownedRawAccepted(t, s, from)
	_, target := ownedRawAccepted(t, s, to)
	clock.Store(int64(3 * time.Second))
	if err := ownedExecutingIngress(t, s, source.inbound, target); err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(3*time.Second + time.Millisecond))
	cut, err := s.captureOwnedFindNodeCut(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := s.bridgeCapture.intake.CloseOwnedRound(cut)
	if err != nil {
		t.Fatal(err)
	}
	work, ok := batch.Next()
	if !ok {
		t.Fatal("actual successful C3a receipt absent")
	}
	if _, ok := batch.Next(); ok {
		t.Fatal("fixture had unexpected extra receipt")
	}
	return s, clock, source, target, work
}
func ownedWriterFaultCore(t *testing.T, mode string, clock *atomic.Int64) (*netcore.NetCore, *ownedWriterFault) {
	t.Helper()
	a, b := net.Pipe()
	c := &ownedWriterFault{Conn: a, mode: mode, clock: clock, closed: make(chan struct{})}
	// This fault endpoint ignores the deadline value (and never writes to its
	// pipe). Only its explicit refusal mode matters, so the ordinary outbound
	// defaults preserve every fault while newTestNetCore owns writer cleanup.
	core := newTestNetCore(t, netcore.ConnID(0), c, netcore.Outbound, netcore.Options{})
	t.Cleanup(func() { core.Close(); _ = b.Close() })
	return core, c
}
func ownedWriterWaitDone(t *testing.T, core *netcore.NetCore, c *ownedWriterFault) {
	t.Helper()
	select {
	case <-core.WriterDone():
	case <-time.After(2 * time.Second):
		t.Fatal("original fault writer did not close gate")
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("writer failure did not close exact raw endpoint")
	}
}
func TestOwnedWriterDeadlineFailureCancelsBeforeTake(t *testing.T) {
	s, clock, source, target, work := ownedWriterActualWork(t)
	core, c := ownedWriterFaultCore(t, "deadline", clock)
	capture := make(chan bool, 1)
	core.SetCaptureSink(ownedWriterCapture{func(ok bool) { capture <- ok }})
	if status := core.SendOwnedFindNode(work, target); status != netcore.SendOK {
		t.Fatal(status)
	}
	ownedWriterWaitDone(t, core, c)
	if c.writes.Load() != 0 {
		t.Fatal("deadline refusal started raw Write")
	}
	select {
	case <-capture:
		t.Fatal("deadline refusal invented write attempt")
	default:
	}
	if err := ownedExecutingIngress(t, s, source.inbound, target); err != nil {
		t.Fatal("pre-T cancellation left original pending row", err)
	}
	if _, err := work.QueueForOriginal(target); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("deadline refusal reused original work", err)
	}
}
func TestOwnedWriterPartialNilAndErrorFinishBeforeCapture(t *testing.T) {
	for _, mode := range []string{"partial", "write-error"} {
		t.Run(mode, func(t *testing.T) {
			s, clock, source, target, work := ownedWriterActualWork(t)
			core, c := ownedWriterFaultCore(t, mode, clock)
			type event struct {
				ok      bool
				reoffer error
			}
			capture := make(chan event, 1)
			wire, _ := ownedReceiveWire(t, target.Identity())
			core.SetCaptureSink(ownedWriterCapture{func(ok bool) {
				receipt, err := s.recordOwnedFindNodeReceive(source.inbound, wire)
				if err == nil {
					err = s.dispatchOwnedFindNodeReceive(receipt)
				}
				capture <- event{ok, err}
			}})
			if status := core.SendOwnedFindNode(work, target); status != netcore.SendOK {
				t.Fatal(status)
			}
			ownedWriterWaitDone(t, core, c)
			select {
			case got := <-capture:
				if got.ok || got.reoffer != nil {
					t.Fatal("partial/error write omitted failed Finish before callback", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("actual write attempt callback absent")
			}
			if c.writes.Load() != 1 {
				t.Fatal("raw fault Write retried", c.writes.Load())
			}
		})
	}
}
func TestOwnedWriterFinishPoisonClosesAfterFullRawWrite(t *testing.T) {
	_, clock, _, target, work := ownedWriterActualWork(t)
	core, c := ownedWriterFaultCore(t, "finish-poison", clock)
	capture := make(chan bool, 1)
	core.SetCaptureSink(ownedWriterCapture{func(ok bool) { capture <- ok }})
	if status := core.SendOwnedFindNode(work, target); status != netcore.SendOK {
		t.Fatal(status)
	}
	ownedWriterWaitDone(t, core, c)
	if c.writes.Load() != 1 {
		t.Fatal("poison fixture did not reach actual Write exactly once")
	}
	select {
	case ok := <-capture:
		if !ok {
			t.Fatal("full raw write was misreported as byte failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("full-write attempt absent")
	}
}
func TestOwnedWriterOriginalEndDuringWriteStillFinishes(t *testing.T) {
	s, clock, source, target, work := ownedWriterActualWork(t)
	core, c := ownedWriterFaultCore(t, "success", clock)
	c.beforeWrite = func() { source.inbound.end(); clock.Add(1) }
	capture := make(chan bool, 1)
	core.SetCaptureSink(ownedWriterCapture{func(ok bool) { capture <- ok }})
	if status := core.SendOwnedFindNode(work, target); status != netcore.SendOK {
		t.Fatal(status)
	}
	select {
	case ok := <-capture:
		if !ok {
			t.Fatal("original End invented write failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("End left writer unable to Finish")
	}
	select {
	case <-core.WriterDone():
		t.Fatal("genuine post-End completion unexpectedly failed")
	default:
	}
	if c.writes.Load() != 1 {
		t.Fatal("End repeated actual Write")
	}
	if _, err := work.QueueForOriginal(target); !errors.Is(err, findnode.ErrStaleWork) {
		t.Fatal("completed work replayed", err)
	}
	if s.bridgeCapture.intake.OwnedRoundPending() {
		t.Fatal("terminal Finish reopened collection")
	}
}
