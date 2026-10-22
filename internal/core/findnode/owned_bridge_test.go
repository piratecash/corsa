package findnode

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

func ownedBridgeProfile() DormantIntakeProfile {
	return DormantIntakeProfile{Timing: TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, Round: time.Millisecond, Slack: time.Millisecond, InitialTokens: 4}
}

func TestOwnedBridgeConstructorsKindsAndOneOwner(t *testing.T) {
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	runtime, err := findnodebridge.NewRuntimeDomain(done)
	if err != nil {
		t.Fatal(err)
	}
	fixture, _, _ := ownedOriginalDomain(t)
	profile := ownedBridgeProfile()
	consumer := runtime.Consumer()
	intake, sink, err := NewDormantIntake(consumer, profile, 8)
	if err != nil || intake.dormantOwner == nil || intake.dormantOwner != sink.observationOwner {
		t.Fatal("constructors lost exact tuple owner", err)
	}
	owner := intake.dormantOwner
	coordinator := owner.coordinator
	if coordinator == nil || coordinator.capacity == nil || coordinator.capacity.owner != coordinator || coordinator.capacity.domain == nil || coordinator.capacity.origin != 0 || coordinator.capacity.last != 0 || coordinator.capacity.epoch != 0 || coordinator.capacity.cap != 512 || coordinator.sourceOwner == nil {
		t.Fatal("one bootstrap/source owner missing")
	}
	profile.Round++
	if owner.profile != ownedBridgeProfile() || owner.consumer != consumer || owner.maxMembers != 8 {
		t.Fatal("constructor borrowed profile or replaced consumer")
	}
	copiedIntake, copiedSink := intake, sink
	if copiedIntake.dormantOwner != owner || copiedSink.observationOwner != owner {
		t.Fatal("opaque capability copy minted owner")
	}
	for _, tc := range []struct {
		name     string
		fixture  bool
		consumer findnodebridge.Consumer
	}{
		{"runtime-rejects-fixture", false, fixture.Consumer()}, {"fixture-rejects-runtime", true, runtime.Consumer()}, {"runtime-rejects-zero", false, findnodebridge.Consumer{}}, {"fixture-rejects-zero", true, findnodebridge.Consumer{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var i DormantIntake
			var s ObservationSink
			var err error
			if tc.fixture {
				i, s, err = NewDormantFixtureIntake(tc.consumer, ownedBridgeProfile(), 8)
			} else {
				i, s, err = NewDormantIntake(tc.consumer, ownedBridgeProfile(), 8)
			}
			if !errors.Is(err, ErrUnauthenticated) || i != (DormantIntake{}) || s != (ObservationSink{}) {
				t.Fatal("wrong kind returned authority", err)
			}
		})
	}
	fixtureIntake, fixtureSink, err := NewDormantFixtureIntake(fixture.Consumer(), ownedBridgeProfile(), 8)
	if err != nil || fixtureIntake.dormantOwner != fixtureSink.observationOwner || fixtureIntake.dormantOwner == owner {
		t.Fatal("fixture constructor lost distinct original owner", err)
	}
}

func TestOwnedBridgeExplicitProfileBounds(t *testing.T) {
	d, _, _ := ownedOriginalDomain(t)
	for _, tc := range []struct {
		name string
		edit func(*DormantIntakeProfile)
		max  int
	}{
		{"zero-M", func(*DormantIntakeProfile) {}, 0}, {"overflow-M", func(*DormantIntakeProfile) {}, int(^uint(0) >> 1)},
		{"zero-round", func(p *DormantIntakeProfile) { p.Round = 0 }, 1}, {"negative-slack", func(p *DormantIntakeProfile) { p.Slack = -1 }, 1},
		{"tokens", func(p *DormantIntakeProfile) { p.InitialTokens = 5 }, 1}, {"zero-timing", func(p *DormantIntakeProfile) { p.Timing.Target = 0 }, 1},
		{"overflow-sum", func(p *DormantIntakeProfile) {
			p.Round = time.Duration(math.MaxInt64)
			p.Slack = time.Duration(math.MaxInt64)
		}, 1},
		{"runtime-bound", func(p *DormantIntakeProfile) { p.Round = time.Second; p.Slack = 1 }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := ownedBridgeProfile()
			tc.edit(&profile)
			intake, sink, err := NewDormantFixtureIntake(d.Consumer(), profile, tc.max)
			if err == nil || intake != (DormantIntake{}) || sink != (ObservationSink{}) {
				t.Fatal("invalid explicit profile returned authority", err)
			}
		})
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[DormantIntake](), reflect.TypeFor[ObservationSink](), reflect.TypeFor[OwnedCut]()} {
		if typ.NumField() != 1 || typ.Field(0).IsExported() || typ.Field(0).Type.Kind() != reflect.Pointer {
			t.Fatal("opaque representation widened", typ)
		}
	}
	if reflect.TypeFor[DormantIntake]().ConvertibleTo(reflect.TypeFor[ObservationSink]()) || reflect.TypeFor[OwnedCut]().ConvertibleTo(reflect.TypeFor[DormantIntake]()) {
		t.Fatal("distinct capabilities became convertible")
	}
}
