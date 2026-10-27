package node

import (
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

func appHandshakeKey() corenode.HandshakeObservationKey {
	return corenode.HandshakeObservationKey{Anchor: "app-handshake", ArtifactSHA256: strings.Repeat("c", 64), Boot: "app-boot", ProcessInstance: "app-process", Epoch: [16]byte{121}, ClockDomain: [16]byte{122}, Configuration: [16]byte{123}}
}
func TestAppHandshakeObservationActualConstruction(t *testing.T) {
	isolatePendingApp(t)
	key := appHandshakeKey()
	a, err := NewWithHandshakeObservation(key, 8)
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	o := a.HandshakeObservation()
	if o == nil || o != a.handshakeObservation || o.Key() != key || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || a.GuardObservation() != nil || a.PendingObservation() != nil || a.TerminalObservation() != nil {
		t.Fatal("App did not retain its own Service construction handle")
	}
	key.Epoch[0] = 0
	key.Anchor = "changed"
	copyKey := o.Key()
	copyKey.Configuration[0] = 0
	if o.Key() != appHandshakeKey() {
		t.Fatal("App key aliases caller data")
	}
	v := o.Snapshot()
	if !v.Available || v.SchemaVersion != 1 || v.AttributionBound || v.Source.Epoch != appHandshakeKey().Epoch || v.Bounds != (corenode.HandshakeObservationBounds{Node: 64, Loopback: 16, PerSource: 4}) || a.service.StartupRefusal() != nil {
		t.Fatal("App acquired a foreign source or changed startup/bounds", v)
	}
	if _, err := trustedclock.SinceBoot(); err != nil {
		if v.Status != "UNKNOWN" || v.Reason == "" {
			t.Fatal("unsupported duration certified coverage", v)
		}
		return
	}
	if v.Status != "COMPLETE" || !v.Cut.Available || v.Cut.Full || v.Cut.Sequence != 0 || v.PastOnly || v.Cut.At < v.CoverageStart {
		t.Fatal("actual prepublication baseline missing", v)
	}
}
func TestAppHandshakeObservationCoexistence(t *testing.T) {
	isolatePendingApp(t)
	key := appHandshakeKey()
	request := corenode.HandshakeObservationRequest{Key: key, Capacity: 8}
	pk := corenode.PendingObservationKey{Anchor: "app-pending", ArtifactSHA256: strings.Repeat("a", 64), Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"}
	tk, gk := appTerminalKey(), appGuardKey()
	a, err := newAppWithObservations(corenode.ObservationOptions{Pending: &pk, Terminal: &tk, Guard: &gk, Handshake: &request})
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	p, tv, g, h := a.PendingObservation(), a.TerminalObservation(), a.GuardObservation(), a.HandshakeObservation()
	if p == nil || tv == nil || g == nil || h == nil || h != a.handshakeObservation {
		t.Fatal("App lost a source handle")
	}
	owner := domain.PeerIdentityFromWire(a.service.Address())
	if p.Identity() != owner || tv.Identity() != owner || g.Identity() != owner || h.Identity() != owner {
		t.Fatal("App constructed multiple/foreign Service owners")
	}
	request.Key.Epoch[0] = 0
	request.Capacity = 0
	if h.Key() != key || h.Snapshot().Source.Epoch != key.Epoch || h.Snapshot().AttributionBound {
		t.Fatal("App failed to freeze request")
	}
}
func TestAppHandshakeObservationUnavailableAndDefaults(t *testing.T) {
	isolatePendingApp(t)
	a := New()
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	if a.HandshakeObservation() != nil || a.GuardObservation() != nil || a.PendingObservation() != nil || a.TerminalObservation() != nil {
		t.Fatal("ordinary App opted in")
	}
	var absent *App
	if absent.HandshakeObservation() != nil {
		t.Fatal("nil App fabricated source")
	}
	if got, err := NewWithHandshakeObservation(corenode.HandshakeObservationKey{}, 8); err == nil || got != nil {
		t.Fatal("invalid references accepted")
	}
	for _, capacity := range []int{0, -1, int(^uint(0) >> 1)} {
		got, err := NewWithHandshakeObservation(appHandshakeKey(), capacity)
		if err != nil {
			t.Fatal("invalid observation capacity became App policy refusal", err)
		}
		ownUnstartedGuardApp(t, got, time.Now().Add(5*time.Second))
		v := got.HandshakeObservation().Snapshot()
		if got.service.StartupRefusal() != nil || !v.Available || v.Status != "UNKNOWN" || v.Reason != "invalid-capacity" || !v.PastOnly || v.AttributionBound {
			t.Fatal("invalid capacity silently activated ordinary source or changed startup", v)
		}
	}
}
