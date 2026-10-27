package node

import (
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

func appGuardKey() corenode.GuardObservationKey {
	return corenode.GuardObservationKey{Anchor: "app-secure-clock", ArtifactSHA256: strings.Repeat("f", 64), Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "secure-store-since-boot", ClockInstance: [16]byte{101}}
}
func ownUnstartedGuardApp(t *testing.T, a *App, end time.Time) {
	t.Helper()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { defer close(done); a.service.WaitBackground() }()
		timer := time.NewTimer(time.Until(end))
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("unstarted App background work missed absolute cleanup deadline")
		}
	})
}

func TestAppGuardObservationActualConstruction(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	isolatePendingApp(t)
	key := appGuardKey()
	a, err := NewWithGuardObservation(key)
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, end)
	o := a.GuardObservation()
	if o == nil || o.Key() != key || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || a.PendingObservation() != nil || a.TerminalObservation() != nil {
		t.Fatal("actual App did not retain its own guarded-clock handle")
	}
	key.ClockInstance[0] = 0
	copyKey := o.Key()
	copyKey.Boot = "changed"
	if o.Key() != appGuardKey() {
		t.Fatal("App source key aliases caller value")
	}
	v := o.Snapshot()
	if !v.Available || v.SchemaVersion != 1 || v.Instance != appGuardKey().ClockInstance || v.AttributionBound || a.service.StartupRefusal() != nil {
		t.Fatal("actual source acquisition changed startup or attribution", v, a.service.StartupRefusal())
	}
	if _, err := trustedclock.SinceBoot(); err != nil {
		if v.Status != "UNKNOWN" || v.Reason == "" {
			t.Fatal("unsupported source fabricated coverage", v)
		}
		return
	}
	t.Run("actual-clock", func(t *testing.T) {
		v := o.Snapshot()
		if v.Status != "COMPLETE" || !v.Guarded || v.Cut < v.CoverageStart || v.Sequence != 0 {
			t.Fatal("actual secure-store source not acquired at App construction", v)
		}
	})
}

func TestAppGuardObservationCoexistence(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	isolatePendingApp(t)
	pending := corenode.PendingObservationKey{Anchor: "app-pending", ArtifactSHA256: strings.Repeat("a", 64), Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "boot-duration"}
	terminal, guard := appTerminalKey(), appGuardKey()
	a, err := newAppWithObservations(corenode.ObservationOptions{Pending: &pending, Terminal: &terminal, Guard: &guard})
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, end)
	p, tv, g := a.PendingObservation(), a.TerminalObservation(), a.GuardObservation()
	identity := domain.PeerIdentityFromWire(a.service.Address())
	if p == nil || tv == nil || g == nil || p.Identity() != identity || tv.Identity() != identity || g.Identity() != identity {
		t.Fatal("App common builder lost a source or built foreign owners")
	}
	p0, t0, g0 := p.Snapshot(), tv.Snapshot(), g.Snapshot()
	if p0.SourceID == ([16]byte{}) || t0.Instance == ([16]byte{}) || p0.SourceID == t0.Instance || p0.SourceID == g0.Instance || t0.Instance == g0.Instance || g0.Instance != guard.ClockInstance {
		t.Fatal("source instances collapsed")
	}
	if _, err := trustedclock.SinceBoot(); err != nil {
		return
	}
	t.Run("actual-owner", func(t *testing.T) {
		result, err := a.service.CancelOutgoingDelivery(protocol.MessageID("guard-coexist-empty"), domain.PeerIdentityFromWire(strings.Repeat("2", 40)))
		if err != nil || result.Total() != 0 {
			t.Fatal("original App queue cancellation changed", result, err)
		}
		p1, t1, g1 := p.Snapshot(), tv.Snapshot(), g.Snapshot()
		if !p1.CoverageValid || p1.SourceID != p0.SourceID || p1.Sequence != p0.Sequence+1 || t1.Instance != t0.Instance || t1.Version != t0.Version || g1.Instance != g0.Instance || g1.Status != "COMPLETE" || g1.Sequence != g0.Sequence || p1.AttributionBound || t1.AttributionBound || g1.AttributionBound {
			t.Fatal("one App did not preserve independent original sources", p1, t1, g1)
		}
	})
}

func TestAppGuardObservationDefaults(t *testing.T) {
	isolatePendingApp(t)
	a := New()
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	if a.GuardObservation() != nil || a.PendingObservation() != nil || a.TerminalObservation() != nil {
		t.Fatal("ordinary App enabled observation")
	}
	if got, err := NewWithGuardObservation(corenode.GuardObservationKey{}); err == nil || got != nil {
		t.Fatal("invalid App observation key accepted")
	}
	var absent *App
	if absent.GuardObservation() != nil {
		t.Fatal("nil App fabricated source")
	}
}
