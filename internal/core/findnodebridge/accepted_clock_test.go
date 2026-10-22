package findnodebridge

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAcceptedClockWaitsOriginalPublicationThenMintsCurrent(t *testing.T) {
	clock := new(atomic.Int64)
	done := make(chan struct{})
	d, err := NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	old, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	publication := d.Publisher().BeginPublication()
	type result struct {
		stamp CaptureStamp
		at    OwnedInstant
		err   error
	}
	ch := make(chan result, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		stamp, lease, err := d.Consumer().ReserveAcceptedClock()
		var at OwnedInstant
		if err == nil {
			at, err = lease.Sample()
			lease.Release()
		}
		ch <- result{stamp, at, err}
	}()
	<-started
	select {
	case got := <-ch:
		publication.Complete()
		t.Fatal("accepted work refused or crossed held original W", got.err)
	case <-time.After(10 * time.Millisecond):
	}
	clock.Store(71)
	publication.Complete()
	select {
	case got := <-ch:
		if got.err != nil || got.at.Elapsed() != 71 || !d.Consumer().Matches(got.stamp, got.at) {
			t.Fatal("accepted current point after wait", got.err, got.at.Elapsed())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted clock waiter did not join")
	}
	if _, err := d.Consumer().TryReadClock(old); err != errStale {
		t.Fatal("historical admission cut unexpectedly remains current", err)
	}
}
func TestAcceptedClockDoneFollowsGenuineRawAndRetiresOnce(t *testing.T) {
	clock := new(atomic.Int64)
	done := make(chan struct{})
	d, err := NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	close(done)
	clock.Store(91)
	stamp, lease, err := d.Consumer().ReserveAcceptedClock()
	if err != nil || !stamp.identityValid() {
		t.Fatal("port shortcut Done before genuine Sample", err)
	}
	at, err := lease.Sample()
	if err != errLifecycle || at != (OwnedInstant{}) || d.fixtureDomain.last != 91 {
		lease.Release()
		t.Fatal("Done did not follow genuine raw/HWM", err)
	}
	lease.Release()
	if _, err := lease.Sample(); err != errLease {
		t.Fatal("accepted lease sampled after release", err)
	}
}
func TestAcceptedClockConstructingPoisonAndInvalidRolesFailClosed(t *testing.T) {
	clock := new(atomic.Int64)
	done := make(chan struct{})
	d, err := NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	if stamp, lease, err := d.Consumer().ReserveAcceptedClock(); err != errUnavailable || stamp != (CaptureStamp{}) || lease != nil {
		t.Fatal("Constructing accepted clock escaped", err)
	}
	if _, _, err := (Consumer{}).ReserveAcceptedClock(); err != errInvalid {
		t.Fatal("invalid Consumer accepted", err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	clock.Store(-1)
	_, lease, err := d.Consumer().ReserveAcceptedClock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Sample(); err != errUnavailable {
		lease.Release()
		t.Fatal("poison minted point", err)
	}
	lease.Release()
	if stamp, lease, err := d.Consumer().ReserveAcceptedClock(); err != errUnavailable || stamp != (CaptureStamp{}) || lease != nil {
		t.Fatal("poison released current authority", err)
	}
}
