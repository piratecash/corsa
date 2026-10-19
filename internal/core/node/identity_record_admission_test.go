package node

import (
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestRecordAdmissionRateUsesObservedRunAndRollingWindow(t *testing.T) {
	a := &relationRecordAdmission{}
	sample := relationObservedTime{wall: time.Unix(1780000000, 0), elapsed: time.Hour}
	if _, ok := a.rateTrial(sample); ok {
		t.Fatal("constructor delay granted startup allowance")
	}
	started := sample
	a.started = &started
	sample.elapsed += 60*time.Second - time.Nanosecond
	if _, ok := a.rateTrial(sample); ok {
		t.Fatal("startup admitted before 60 observed seconds")
	}
	sample.elapsed++
	for i := 0; i < 30; i++ {
		trial, ok := a.rateTrial(sample)
		if !ok {
			t.Fatalf("event %d refused", i+1)
		}
		a.rate = trial
	}
	if _, ok := a.rateTrial(sample); ok {
		t.Fatal("31st event admitted")
	}
	sample.wall = sample.wall.Add(100 * 24 * time.Hour)
	if _, ok := a.rateTrial(sample); ok {
		t.Fatal("wall jump minted allowance")
	}
	sample.elapsed += 60*time.Second - time.Nanosecond
	if _, ok := a.rateTrial(sample); ok {
		t.Fatal("rolling window expired early")
	}
	sample.elapsed++
	if _, ok := a.rateTrial(sample); !ok {
		t.Fatal("exact oldest+60 did not expire")
	}
}

func TestRecordAdmissionOriginalLifetimeCannotBeRebased(t *testing.T) {
	origin := relationObservedTime{wall: time.Unix(1780000000, 0), elapsed: time.Hour}
	lifetime := relationAdmissionLifetime{origin: origin}
	sample := relationObservedTime{wall: origin.wall.Add(identityLookupTaskLifetime), elapsed: origin.elapsed + identityLookupTaskLifetime}
	if !lifetime.live(sample) {
		t.Fatal("exact seven-day endpoint refused")
	}
	sample.elapsed++
	sample.wall = origin.wall.Add(-24 * time.Hour)
	if lifetime.live(sample) {
		t.Fatal("wall rollback extended original elapsed lifetime")
	}
	sample.elapsed = origin.elapsed
	sample.wall = origin.wall.Add(identityLookupTaskLifetime + time.Nanosecond)
	if lifetime.live(sample) {
		t.Fatal("forward wall expiry ignored")
	}
}

func TestRecordAdmissionFreshOwnerRunOverflowAndReseedBoundary(t *testing.T) {
	r, c := admissionResolverFixture(t)
	if r.beginRecordAdmissionRun() {
		t.Fatal("same owner granted a second startup epoch")
	}
	r.mu.Lock()
	r.recordAdmission.lastObserved = time.Duration(1<<63 - 1)
	_, ok := r.recordAdmission.sampleLocked()
	r.mu.Unlock()
	if ok || !r.recordAdmission.clockFailed {
		t.Fatal("elapsed regression wrapped/granted allowance")
	}
	c.elapsed.Store(1<<63 - 1)
	r.mu.Lock()
	_, ok = r.recordAdmission.sampleLocked()
	r.mu.Unlock()
	if ok {
		t.Fatal("clock failure recovered implicitly")
	}
	normal, _ := newTestResolver(t)
	if normal.recordAdmission != nil || normal.beginRecordAdmissionRun() {
		t.Fatal("normal constructor allocated admission/worker")
	}
}

// time.Now supplies a real monotonic-bearing value. No host-clock mutation or
// unsafe representation is needed to verify the shared wall-only contract.
// Structural equality checks monotonic-field presence here; Time.Equal only
// compares instants and cannot establish that invariant.
func TestRecordAdmissionSharedSamplingStripsMonotonicWall(t *testing.T) {
	bearing := time.Now()
	if reflect.DeepEqual(bearing, bearing.Round(0)) {
		t.Fatal("time.Now fixture lacks its monotonic reading")
	}
	for _, injected := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "injected"}[injected], func(t *testing.T) {
			base, svc := newTestResolver(t)
			r := newIdentityResolverWithRecordAdmission(svc, base.intents, base.network)
			svc.identityResolver = r
			if injected {
				r.recordAdmission.clock = func() relationObservedTime { return relationObservedTime{wall: bearing, elapsed: time.Hour} }
			}
			raw := r.recordAdmission.clock()
			if reflect.DeepEqual(raw.wall, raw.wall.Round(0)) {
				t.Fatal("raw clock fixture lost its monotonic reading")
			}
			if !r.beginRecordAdmissionRun() {
				t.Fatal("begin rejected a fresh sample")
			}
			target := domain.PeerIdentity{210, 9}
			if _, err := svc.StartUserIdentityLookup(t.Context(), target); err != nil {
				t.Fatal(err)
			}
			r.mu.Lock()
			current, ok := r.recordAdmission.sampleLocked()
			res := r.resolutions[target]
			started := *r.recordAdmission.started
			r.mu.Unlock()
			if !ok || res == nil || res.recordLifetime == nil {
				t.Fatal("paired sample/original missing")
			}
			for name, wall := range map[string]time.Time{"started": started.wall, "current": current.wall, "origin": res.recordLifetime.origin.wall, "createdAt": res.createdAt} {
				if wall != wall.Round(0) {
					t.Fatalf("%s retained monotonic authority in the wall upper bound", name)
				}
			}
			if res.createdAt != res.recordLifetime.origin.wall {
				t.Fatal("createdAt/origin used different paired wall samples")
			}
			if injected && (current.wall != bearing.Round(0) || current.elapsed != time.Hour || res.createdAt != bearing.Round(0)) {
				t.Fatal("shared normalization changed wall instant or elapsed accounting")
			}
		})
	}
}
