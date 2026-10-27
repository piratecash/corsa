package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func appRegisteredRequest() corenode.RegisteredProvenV2ObservationRequest {
	return corenode.RegisteredProvenV2ObservationRequest{OwnerCapacity: 8, HistoryCapacity: 32, Instance: [16]byte{191}}
}
func appRegisteredConservative(t *testing.T, v corenode.RegisteredProvenV2ObservationSnapshot) {
	t.Helper()
	if v.Population != "RegisteredProvenV2" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("App upgraded private source labels", v)
	}
}
func TestAppRegisteredProvenV2Observation(t *testing.T) {
	t.Run("actual-unstarted-App-owner-key-copy-and-isolation", func(t *testing.T) {
		isolatePendingApp(t)
		request := appRegisteredRequest()
		original := request
		a, err := NewWithRegisteredProvenV2Observation(request)
		if err != nil || a == nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
		o := a.RegisteredProvenV2Observation()
		if o == nil || o != a.registeredProvenV2Observation || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || o.Key().Instance != original.Instance || a.PendingObservation() != nil || a.HandshakeObservation() != nil || a.TerminalObservation() != nil || a.GuardObservation() != nil || a.ConnectionTotalObservation() != nil {
			t.Fatal("App handle not its one selected original Service")
		}
		request.Instance = [16]byte{1}
		request.OwnerCapacity = 0
		request.HistoryCapacity = 0
		key := o.Key()
		key.Instance[0] = 0
		v := o.Snapshot()
		appRegisteredConservative(t, v)
		if o.Key().Instance != original.Instance || v.Instance != original.Instance || v.RegisteredOwners != 0 || v.DistinctIdentities != 0 || v.Sequence != 0 || a.service.StartupRefusal() != nil {
			t.Fatal("App lost copied input/empty prepublication source", v)
		}
		// A separate isolated identity path creates another actual App; an equal
		// caller Instance does not establish service/process attribution.
		isolatePendingApp(t)
		other, err := NewWithRegisteredProvenV2Observation(original)
		if err != nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, other, time.Now().Add(5*time.Second))
		second := other.RegisteredProvenV2Observation()
		if second == nil || second == o || second.Identity() == o.Identity() || second.Key() != o.Key() {
			t.Fatal("actual App owners collapsed into caller label")
		}
		appRegisteredConservative(t, second.Snapshot())
	})
	t.Run("same-Service-composite-file-handles-and-borrowed-directory-cleanup", func(t *testing.T) {
		isolateTotalFileApp(t)
		request := appRegisteredRequest()
		hf := appFileRequest(t)
		tf := appTotalFileRequest(t)
		pending := corenode.PendingObservationKey{Anchor: "registered-composite", ArtifactSHA256: appHandshakeKey().ArtifactSHA256, Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"}
		terminal, guard := appTerminalKey(), appGuardKey()
		a, err := newAppWithObservations(corenode.ObservationOptions{RegisteredProvenV2: &request, Pending: &pending, Terminal: &terminal, Guard: &guard, HandshakeFile: &hf, ConnectionTotalFile: &tf})
		if err != nil || a == nil {
			t.Fatal(err)
		}
		ownFileApp(t, a)
		t.Cleanup(func() {
			if err := a.CloseConnectionTotalFileObservation(); err != nil {
				t.Error(err)
			}
		})
		owner := domain.PeerIdentityFromWire(a.service.Address())
		o := a.RegisteredProvenV2Observation()
		if o == nil || a.PendingObservation() == nil || a.TerminalObservation() == nil || a.GuardObservation() == nil || a.HandshakeFileObservation() == nil || a.HandshakeObservation() == nil || a.ConnectionTotalFileObservation() == nil || a.ConnectionTotalObservation() == nil {
			t.Fatal("composite construction lost an existing handle")
		}
		if o.Identity() != owner || a.PendingObservation().Identity() != owner || a.TerminalObservation().Identity() != owner || a.GuardObservation().Identity() != owner || a.HandshakeFileObservation().Identity() != owner || a.HandshakeObservation().Identity() != owner || a.ConnectionTotalFileObservation().Identity() != owner || a.ConnectionTotalObservation().Identity() != owner {
			t.Fatal("composite constructor created foreign Service owners")
		}
		if lease, err := hf.Directory.TryDirectoryLease(); err == nil {
			_ = lease.Close()
			t.Fatal("composite handshake file source lost original lease")
		}
		if lease, err := tf.Directory.TryDirectoryLease(); err == nil {
			_ = lease.Close()
			t.Fatal("composite total file source lost original lease")
		}
		if err := a.CloseHandshakeFileObservation(); err != nil {
			t.Fatal(err)
		}
		if err := a.CloseConnectionTotalFileObservation(); err != nil {
			t.Fatal(err)
		}
		if err := a.CloseHandshakeFileObservation(); err != nil {
			t.Fatal(err)
		}
		if err := a.CloseConnectionTotalFileObservation(); err != nil {
			t.Fatal(err)
		}
		if !a.HandshakeFileObservation().Status().Closed || !a.ConnectionTotalFileObservation().Status().Closed {
			t.Fatal("unstarted composite file handles did not close")
		}
		for _, dir := range []*secretfile.Dir{hf.Directory, tf.Directory} {
			lease, err := dir.TryDirectoryLease()
			if err != nil {
				t.Fatal("App closed borrowed directory or kept lease", err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		}
		v := o.Snapshot()
		appRegisteredConservative(t, v)
		if v.Instance != request.Instance || v.Sequence != 0 || v.RegisteredOwners != 0 {
			t.Fatal("file cleanup replaced or closed memory-only K3 source", v)
		}
	})
	t.Run("default-nil-and-invalid-observer-do-not-change-startup", func(t *testing.T) {
		isolatePendingApp(t)
		ordinary := New()
		ownUnstartedGuardApp(t, ordinary, time.Now().Add(5*time.Second))
		if ordinary.RegisteredProvenV2Observation() != nil {
			t.Fatal("ordinary App activated K3")
		}
		var absent *App
		if absent.RegisteredProvenV2Observation() != nil {
			t.Fatal("nil App fabricated handle")
		}
		for _, request := range []corenode.RegisteredProvenV2ObservationRequest{{OwnerCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {OwnerCapacity: 1, HistoryCapacity: 0, Instance: [16]byte{1}}, {OwnerCapacity: 1, HistoryCapacity: 1}} {
			a, err := NewWithRegisteredProvenV2Observation(request)
			if err != nil || a == nil {
				t.Fatal("invalid observer denied original App", err)
			}
			ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
			o := a.RegisteredProvenV2Observation()
			if o == nil {
				t.Fatal("requested unavailable handle absent")
			}
			v := o.Snapshot()
			appRegisteredConservative(t, v)
			if v.Status != "UNKNOWN" || v.Reason == "" || v.Sequence != 0 || v.RegisteredOwners != 0 || a.service.StartupRefusal() != nil {
				t.Fatal("observer unavailability changed original startup", v)
			}
		}
	})
}
