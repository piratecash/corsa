package node

import (
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
)

func appConnectionTotalKey() corenode.ConnectionTotalObservationKey {
	return corenode.ConnectionTotalObservationKey{Anchor: "app-total", ArtifactSHA256: strings.Repeat("a", 64), Boot: "app-boot", ProcessInstance: "app-process", Epoch: [16]byte{141}, ClockDomain: [16]byte{142}, Configuration: [16]byte{143}}
}
func isolateTotalApp(t *testing.T) {
	t.Helper()
	isolatePendingApp(t)
	t.Setenv("CORSA_MAX_TOTAL_CONNECTIONS", "0")
	t.Setenv("CORSA_MAX_AUXILIARY_CONNECTIONS", "")
}
func TestAppConnectionTotalObservationConstruction(t *testing.T) {
	isolateTotalApp(t)
	key := appConnectionTotalKey()
	a, err := NewWithConnectionTotalObservation(key, 8)
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	o := a.ConnectionTotalObservation()
	if o == nil || o != a.connectionTotalObservation || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || o.Key() != key || a.HandshakeObservation() != nil || a.PendingObservation() != nil || a.TerminalObservation() != nil || a.GuardObservation() != nil {
		t.Fatal("App did not retain its actual construction handle")
	}
	key.Epoch[0] = 0
	key.Anchor = "changed"
	copyKey := o.Key()
	copyKey.Configuration[0] = 0
	if o.Key() != appConnectionTotalKey() {
		t.Fatal("App key aliases caller")
	}
	v := o.Snapshot()
	if !v.Available || v.SchemaVersion != 1 || v.AttributionBound || v.Status != "DISABLED" || v.Reason != "total-disabled" || !v.PastOnly || v.Config.Total != 0 || v.Cut.Available || v.Rows != nil || a.service.StartupRefusal() != nil {
		t.Fatal("default-off total falsely proved measured zero", v)
	}
}
func TestAppConnectionTotalObservationCoexistence(t *testing.T) {
	isolateTotalApp(t)
	total := corenode.ConnectionTotalObservationRequest{Key: appConnectionTotalKey(), Capacity: 8}
	handshake := corenode.HandshakeObservationRequest{Key: appHandshakeKey(), Capacity: 8}
	pending := corenode.PendingObservationKey{Anchor: "app-pending", ArtifactSHA256: strings.Repeat("c", 64), Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"}
	terminal, guard := appTerminalKey(), appGuardKey()
	a, err := newAppWithObservations(corenode.ObservationOptions{Pending: &pending, Terminal: &terminal, Guard: &guard, Handshake: &handshake, ConnectionTotal: &total})
	if err != nil {
		t.Fatal(err)
	}
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	p, tv, g, h, c := a.PendingObservation(), a.TerminalObservation(), a.GuardObservation(), a.HandshakeObservation(), a.ConnectionTotalObservation()
	if p == nil || tv == nil || g == nil || h == nil || c == nil {
		t.Fatal("App lost one common source")
	}
	owner := domain.PeerIdentityFromWire(a.service.Address())
	if p.Identity() != owner || tv.Identity() != owner || g.Identity() != owner || h.Identity() != owner || c.Identity() != owner {
		t.Fatal("App sources have foreign identities")
	}
	total.Key.Epoch[0] = 0
	total.Capacity = 0
	if c.Key() != appConnectionTotalKey() || c.Snapshot().Status != "DISABLED" || c.Snapshot().AttributionBound {
		t.Fatal("App did not freeze request")
	}
}
func TestAppConnectionTotalObservationDefaultsAndCapacity(t *testing.T) {
	isolateTotalApp(t)
	a := New()
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	if a.ConnectionTotalObservation() != nil || a.HandshakeObservation() != nil || a.PendingObservation() != nil || a.TerminalObservation() != nil || a.GuardObservation() != nil {
		t.Fatal("ordinary App activated observation")
	}
	var absent *App
	if absent.ConnectionTotalObservation() != nil {
		t.Fatal("nil App fabricated handle")
	}
	if got, e := NewWithConnectionTotalObservation(corenode.ConnectionTotalObservationKey{}, 8); e == nil || got != nil {
		t.Fatal("invalid references accepted")
	}
	for _, capacity := range []int{0, -1, int(^uint(0) >> 1)} {
		got, e := NewWithConnectionTotalObservation(appConnectionTotalKey(), capacity)
		if e != nil {
			t.Fatal("disabled source capacity changed App startup", e)
		}
		ownUnstartedGuardApp(t, got, time.Now().Add(5*time.Second))
		v := got.ConnectionTotalObservation().Snapshot()
		if v.Status != "DISABLED" || v.Cut.Available || v.Rows != nil || v.AttributionBound || got.service.StartupRefusal() != nil {
			t.Fatal("disabled precedes capacity/clock checks", v)
		}
	}
}
