package findnode

import (
	"context"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"testing"
	"time"
)

func TestOwnedBootstrapIntakeOriginalRuntimeAndFirstWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := findnodebridge.NewRuntimeDomain(ctx.Done())
	if err != nil {
		t.Fatal(err)
	}
	p := d.Publisher().BeginClockedPublication()
	at, _, err := p.SampleRuntimePair()
	p.Complete()
	if err != nil {
		t.Fatal(err)
	}
	consumer, observer := d.Consumer(), d.Observer()
	intake, sink, err := NewExecutingBootstrapIntake(consumer, observer, ownedBridgeProfile(), 4, at, 16, time.Hour, true)
	if err != nil || intake == (DormantIntake{}) || sink == (ObservationSink{}) {
		t.Fatal("genuine Runtime bootstrap failed", err)
	}
	owner := intake.dormantOwner
	o := owner.coordinator
	epoch := o.capacity
	if owner.consumer != consumer || owner.finishObserver != observer || epoch.owner != o || epoch.domain == nil || epoch.origin != Instant(at.Elapsed()) || o.last != Instant(at.Elapsed()) || epoch.cap != 960 {
		t.Fatal("bootstrap lost exact owner/origin/hint")
	}
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("bootstrap constructor made Runtime Ready")
	}
	// Existing private capacity arithmetic is exercised at the exact original
	// origin. This is not a public fake Runtime clock or admission attempt.
	o.mu.Lock()
	defer o.mu.Unlock()
	end := epoch.origin + candidateCapacityWindow
	cap, err := o.selectCapacityLocked(end-1, nil)
	if err != nil || cap != 960 {
		t.Fatal("bootstrap changed within first window", cap, err)
	}
	cap, err = o.selectCapacityLocked(end, []candidateCapacityCut{{owner: o, domain: epoch.domain, at: end, degree: 0, complete: true}})
	if err != nil || cap != 512 {
		t.Fatal("complete zero did not replace bootstrap at next epoch", cap, err)
	}
	cap, err = o.selectCapacityLocked(end+1, []candidateCapacityCut{{owner: o, domain: epoch.domain, at: end + 1, degree: 64, complete: true}})
	if err != nil || cap != 512 {
		t.Fatal("later same-window hint changed frozen capacity", cap, err)
	}
}
func TestOwnedBootstrapIntakeRejectsForeignRolesAndHintBoundaries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := findnodebridge.NewRuntimeDomain(ctx.Done())
	if err != nil {
		t.Fatal(err)
	}
	b, err := findnodebridge.NewRuntimeDomain(ctx.Done())
	if err != nil {
		t.Fatal(err)
	}
	pub := a.Publisher().BeginClockedPublication()
	at, _, err := pub.SampleRuntimePair()
	pub.Complete()
	if err != nil {
		t.Fatal(err)
	}
	for _, observer := range []findnodebridge.Observer{{}, b.Observer()} {
		intake, sink, err := NewExecutingBootstrapIntake(a.Consumer(), observer, ownedBridgeProfile(), 4, at, 64, 0, true)
		if err == nil || intake != (DormantIntake{}) || sink != (ObservationSink{}) {
			t.Fatal("foreign bootstrap observer acquired owner")
		}
	}
	for _, tc := range []struct {
		name    string
		value   uint8
		age     time.Duration
		present bool
		want    int
	}{
		{"absent", 16, 0, false, 512}, {"stale24h", 16, 24 * time.Hour, true, 512}, {"future", 16, -1, true, 512}, {"zero", 0, 0, true, 512}, {"clipped", 255, 0, true, 3840},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intake, _, err := NewExecutingBootstrapIntake(a.Consumer(), a.Observer(), ownedBridgeProfile(), 4, at, tc.value, tc.age, tc.present)
			if err != nil || intake.dormantOwner.coordinator.capacity.cap != tc.want {
				t.Fatal("bootstrap hint boundary", err)
			}
		})
	}
}
