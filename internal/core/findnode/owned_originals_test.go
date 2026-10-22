package findnode

import (
	"context"
	"errors"
	"math"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Real TLS/session proof, but opened/ConnID/group are explicit logical fixture
// inputs. This is NOT a Node same-domain capture, CM receipt or cut fixture.
func ownedOriginalSession(t *testing.T, id domain.ConnID, opened Instant) Session {
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
	member, err := NewSession(proof, id, "owned-originals", opened, []domain.Capability{domain.CapOverlayFindNodeV1})
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func ownedOriginalDomain(t *testing.T) (findnodebridge.FixtureDomain, *atomic.Int64, func()) {
	t.Helper()
	clock := new(atomic.Int64)
	done := make(chan struct{})
	d, err := findnodebridge.NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			close(done)
			stopped = true
		}
	}
	t.Cleanup(stop)
	return d, clock, stop
}

func ownedOriginalLease(t *testing.T, d findnodebridge.FixtureDomain) (*findnodebridge.CommitReservation, func()) {
	t.Helper()
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			lease.Release()
			released = true
		}
	}
	t.Cleanup(release)
	return lease, release
}

func TestOwnedOriginalSetCopiedBoundedAndExactHandles(t *testing.T) {
	first := ownedOriginalSession(t, 1, 0)
	other := ownedOriginalSession(t, 2, 1)
	sameID := ownedOriginalSession(t, 1, 0)
	input := []Session{first, other}
	set, err := prepareOwnedOriginalSet(input, 2)
	if err != nil || len(set.members) != 2 {
		t.Fatal("genuine bounded originals preparation", err)
	}
	input[0] = sameID
	if set.members[0] != first || set.members[1] != other {
		t.Fatal("prepared originals borrowed or replaced caller handles")
	}
	for _, tc := range []struct {
		name    string
		members []Session
		max     int
		want    error
	}{
		{"M-plus-one", []Session{first, other}, 1, ErrInvalid},
		{"duplicate", []Session{first, first}, 2, ErrInvalid},
		{"foreign-same-id", []Session{first, sameID}, 2, ErrInvalid},
		{"zero", []Session{{}}, 1, ErrUnauthenticated},
		{"zero-bound", nil, 0, ErrInvalid},
		{"overflow-bound", nil, int(^uint(0) >> 1), ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := prepareOwnedOriginalSet(tc.members, tc.max)
			if !errors.Is(err, tc.want) || got.members != nil {
				t.Fatal("invalid originals prepared a set", got, err)
			}
		})
	}
	// Negative private corruption, not a manufactured positive proof.
	invalid := Session{state: &sessionState{connID: 3, identity: first.Identity(), group: "owned-originals"}}
	if _, err := prepareOwnedOriginalSet([]Session{invalid}, 1); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("unproven private state passed preparation", err)
	}
	first.End()
	if _, err := prepareOwnedOriginalSet([]Session{first}, 1); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("closed original prepared", err)
	}
	empty, err := prepareOwnedOriginalSet(nil, 1)
	if err != nil || empty.members == nil || len(empty.members) != 0 {
		t.Fatal("valid empty set confused with zero", empty, err)
	}
	d, _, _ := ownedOriginalDomain(t)
	lease, release := ownedOriginalLease(t, d)
	if _, err := empty.sampleAndCheckOriginals(lease); err != nil {
		t.Fatal("prepared empty set refused genuine sample", err)
	}
	release()
}

func TestOwnedOriginalsSampleChecksEveryOriginal(t *testing.T) {
	for _, kind := range []string{"excluded-target-End", "nonoldest-pending", "nonoldest-target-cache", "nonoldest-signing", "nonoldest-intake"} {
		t.Run(kind, func(t *testing.T) {
			members := make([]Session, 6)
			for i := range members {
				members[i] = ownedOriginalSession(t, domain.ConnID(i+1), Instant(time.Duration(i)*time.Second))
			}
			set, err := prepareOwnedOriginalSet(members, 6)
			if err != nil {
				t.Fatal(err)
			}
			d, clock, _ := ownedOriginalDomain(t)
			stamp, err := d.Observer().BeginCapture()
			if err != nil {
				t.Fatal(err)
			}
			tail := members[len(members)-1]
			// The first source and four older logical Sessions remain open. The
			// tail is a future excluded-target/nonoldest label, not a cut claim.
			switch kind {
			case "excluded-target-End":
				tail.End()
			case "nonoldest-pending":
				NewPendingTable().CloseSession(tail)
			case "nonoldest-target-cache":
				cache, err := NewTargetCache(members[0].Identity())
				if err != nil {
					t.Fatal(err)
				}
				cache.CloseSession(tail)
			case "nonoldest-signing":
				calendar, err := NewSigningCalendar(SigningProfile{NodeSpacing: time.Second, MaxReservations: 8}, 0)
				if err != nil {
					t.Fatal(err)
				}
				calendar.CloseSession(tail)
			case "nonoldest-intake":
				if err := candidateTestOwner(t).closeSession(tail, Instant(10*time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			for _, member := range members[:len(members)-1] {
				if !member.valid() {
					t.Fatal("fixture closed reduced selected subset")
				}
			}
			clock.Store(int64(10 * time.Second))
			lease, err := d.Consumer().TryReadClock(stamp)
			if err != nil {
				t.Fatal("non-W close changed domain generation", err)
			}
			released := false
			release := func() {
				if !released {
					lease.Release()
					released = true
				}
			}
			t.Cleanup(release)
			at, err := set.sampleAndCheckOriginals(lease)
			if !errors.Is(err, ErrUnauthenticated) || at != (findnodebridge.OwnedInstant{}) {
				t.Fatal("closed tail original escaped post-sample pass", at, err)
			}
			release()
		})
	}
}

func TestOwnedOriginalsClosedCheckFollowsActualSample(t *testing.T) {
	member := ownedOriginalSession(t, 1, 0)
	set, err := prepareOwnedOriginalSet([]Session{member}, 1)
	if err != nil {
		t.Fatal(err)
	}
	d, clock, _ := ownedOriginalDomain(t)
	member.End()
	clock.Store(int64(10 * time.Second))
	lease, release := ownedOriginalLease(t, d)
	at, err := set.sampleAndCheckOriginals(lease)
	if !errors.Is(err, ErrUnauthenticated) || at != (findnodebridge.OwnedInstant{}) {
		t.Fatal("closed original did not refuse after sample", at, err)
	}
	release()
	// Observable real leaf HWM, no private clock rewrite or production hook.
	// A precheck-before-Sample mutant leaves HWM0 and wrongly accepts5.
	clock.Store(int64(5 * time.Second))
	observation, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	_, err = observation.Sample()
	observation.Release()
	if err == nil {
		t.Fatal("closed check ran before actual final sample; HWM10 was absent")
	}
}

func TestOwnedOriginalsRefusedSampleDoesNotBecomePoint(t *testing.T) {
	for _, kind := range []string{"Done", "negative", "regression", "maximum", "nil-lease", "released-lease"} {
		t.Run(kind, func(t *testing.T) {
			member := ownedOriginalSession(t, 1, 0)
			set, err := prepareOwnedOriginalSet([]Session{member}, 1)
			if err != nil {
				t.Fatal(err)
			}
			d, clock, stop := ownedOriginalDomain(t)
			if kind == "regression" {
				clock.Store(10)
				o, err := d.Observer().ReserveClock()
				if err != nil {
					t.Fatal(err)
				}
				_, err = o.Sample()
				o.Release()
				if err != nil {
					t.Fatal(err)
				}
			}
			lease, release := ownedOriginalLease(t, d)
			switch kind {
			case "Done":
				stop()
			case "negative":
				clock.Store(-1)
			case "regression":
				clock.Store(5)
			case "maximum":
				clock.Store(math.MaxInt64)
			case "nil-lease":
				release()
				lease = nil
			case "released-lease":
				release()
			}
			at, err := set.sampleAndCheckOriginals(lease)
			if err == nil || at != (findnodebridge.OwnedInstant{}) || !member.valid() {
				t.Fatal("refused actual sample became point or mutated Session", at, err)
			}
			release()
		})
	}
}

func TestOwnedOriginalsLeaveCallerLeaseHeld(t *testing.T) {
	member := ownedOriginalSession(t, 1, 0)
	set, err := prepareOwnedOriginalSet([]Session{member}, 1)
	if err != nil {
		t.Fatal(err)
	}
	d, _, _ := ownedOriginalDomain(t)
	stamp, err := d.Observer().BeginCapture()
	if err != nil {
		t.Fatal(err)
	}
	lease, release := ownedOriginalLease(t, d)
	if _, err := set.sampleAndCheckOriginals(lease); err != nil {
		t.Fatal(err)
	}
	probe, err := d.Consumer().TryReadClock(stamp)
	if err == nil {
		probe.Release()
		t.Fatal("helper released caller's original combined reservation")
	}
	release()
	probe, err = d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal("caller release did not release original ownership", err)
	}
	probe.Release()
	publication := d.Publisher().BeginPublication()
	publication.Complete()
	probe, err = d.Consumer().TryReadClock(stamp)
	if err == nil {
		probe.Release()
		t.Fatal("publication failed to stale original stamp")
	}
}

func TestOwnedOriginalsZeroAndClosedLayout(t *testing.T) {
	var zero ownedOriginalSet
	if at, err := zero.sampleAndCheckOriginals(nil); !errors.Is(err, ErrInvalid) || at != (findnodebridge.OwnedInstant{}) {
		t.Fatal("zero set reached lease", at, err)
	}
	shape := reflect.TypeFor[ownedOriginalSet]()
	if shape.NumField() != 1 || shape.Field(0).Name != "members" || shape.Field(0).Type != reflect.TypeFor[[]Session]() {
		t.Fatal("original set retained extra authority")
	}
	if reflect.TypeFor[Session]().NumField() != 1 || reflect.TypeFor[Session]().Field(0).Name != "state" {
		t.Fatal("original handle representation changed")
	}
}
