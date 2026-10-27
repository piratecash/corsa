package node

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func appRawPinRequest() corenode.RawPinMembershipObservationRequest {
	return corenode.RawPinMembershipObservationRequest{BaselineCapacity: 8, HistoryCapacity: 32, Instance: [16]byte{241}}
}
func appRawPinLabels(t *testing.T, v corenode.RawPinMembershipObservationSnapshot) {
	t.Helper()
	if v.Population != "RawPinMembership" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("App upgraded raw pin attribution/coverage", v)
	}
}
func TestAppRawPinMembershipObservation(t *testing.T) {
	t.Run("unstarted-App-owner-copied-input-key-and-distinct-identity", func(t *testing.T) {
		isolatePendingApp(t)
		request := appRawPinRequest()
		want := request
		a, err := NewWithRawPinMembershipObservation(request)
		if err != nil || a == nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
		o := a.RawPinMembershipObservation()
		if o == nil || o != a.rawPinMembershipObservation || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || o.Key().Instance != want.Instance || a.RegisteredProvenV2Observation() != nil || a.GuardObservation() != nil || a.HandshakeObservation() != nil {
			t.Fatal("App did not retain its one original Service readout")
		}
		request.Instance = [16]byte{}
		request.BaselineCapacity = 0
		request.HistoryCapacity = 0
		key := o.Key()
		key.Instance[0] = 0
		v := o.Snapshot()
		appRawPinLabels(t, v)
		if o.Key().Instance != want.Instance || v.Instance != want.Instance || v.Sequence != 0 || v.CountAtCut != 0 || len(v.BaselineIdentities) != 0 || a.service.StartupRefusal() != nil {
			t.Fatal("App caller alias or baseline/startup mismatch", v)
		}
		isolatePendingApp(t)
		other, err := NewWithRawPinMembershipObservation(want)
		if err != nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, other, time.Now().Add(5*time.Second))
		otherHandle := other.RawPinMembershipObservation()
		if otherHandle == nil || otherHandle == o || otherHandle.Identity() == o.Identity() || otherHandle.Key() != o.Key() {
			t.Fatal("equal unverified labels collapsed actual App identities")
		}
		appRawPinLabels(t, otherHandle.Snapshot())
	})
	t.Run("composite-one-Service-observers-and-original-borrowed-file-cleanup", func(t *testing.T) {
		isolateTotalFileApp(t)
		raw := appRawPinRequest()
		registered := appRegisteredRequest()
		hf := appFileRequest(t)
		tf := appTotalFileRequest(t)
		pending := corenode.PendingObservationKey{Anchor: "raw-pin-composite", ArtifactSHA256: appHandshakeKey().ArtifactSHA256, Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"}
		terminal, guard := appTerminalKey(), appGuardKey()
		a, err := newAppWithObservations(corenode.ObservationOptions{RawPinMembership: &raw, RegisteredProvenV2: &registered, Pending: &pending, Terminal: &terminal, Guard: &guard, HandshakeFile: &hf, ConnectionTotalFile: &tf})
		if err != nil {
			t.Fatal(err)
		}
		ownFileApp(t, a)
		t.Cleanup(func() {
			if err := a.CloseConnectionTotalFileObservation(); err != nil {
				t.Error(err)
			}
		})
		owner := domain.PeerIdentityFromWire(a.service.Address())
		o := a.RawPinMembershipObservation()
		if o == nil || a.RegisteredProvenV2Observation() == nil || a.PendingObservation() == nil || a.TerminalObservation() == nil || a.GuardObservation() == nil || a.HandshakeFileObservation() == nil || a.HandshakeObservation() == nil || a.ConnectionTotalFileObservation() == nil || a.ConnectionTotalObservation() == nil {
			t.Fatal("composite lost an original observer")
		}
		if o.Identity() != owner || a.RegisteredProvenV2Observation().Identity() != owner || a.PendingObservation().Identity() != owner || a.TerminalObservation().Identity() != owner || a.GuardObservation().Identity() != owner || a.HandshakeFileObservation().Identity() != owner || a.HandshakeObservation().Identity() != owner || a.ConnectionTotalFileObservation().Identity() != owner || a.ConnectionTotalObservation().Identity() != owner {
			t.Fatal("composite observers use foreign Service owners")
		}
		raw.Instance = [16]byte{}
		registered.Instance = [16]byte{}
		if o.Key() != (corenode.RawPinMembershipObservationKey{Instance: appRawPinRequest().Instance}) || a.RegisteredProvenV2Observation().Key().Instance != appRegisteredRequest().Instance {
			t.Fatal("composite requests alias callers")
		}
		for _, dir := range []*secretfile.Dir{hf.Directory, tf.Directory} {
			if lease, err := dir.TryDirectoryLease(); err == nil {
				_ = lease.Close()
				t.Fatal("file lease lost during raw-pin composition")
			}
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
		for _, dir := range []*secretfile.Dir{hf.Directory, tf.Directory} {
			lease, err := dir.TryDirectoryLease()
			if err != nil {
				t.Fatal("existing App cleanup closed borrowed directory/kept lease", err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		}
		v := o.Snapshot()
		appRawPinLabels(t, v)
		if v.Instance != appRawPinRequest().Instance || v.CountAtCut != 0 || v.Sequence != 0 {
			t.Fatal("unrelated file cleanup replaced memory-only-lifetime raw readout", v)
		}
	})
	t.Run("nil-default-invalid-observer-and-original-pin-store-refusal", func(t *testing.T) {
		isolatePendingApp(t)
		ordinary := New()
		ownUnstartedGuardApp(t, ordinary, time.Now().Add(5*time.Second))
		if ordinary.RawPinMembershipObservation() != nil {
			t.Fatal("ordinary App selected raw pin history")
		}
		var absent *App
		if absent.RawPinMembershipObservation() != nil {
			t.Fatal("nil App fabricated readout")
		}
		for _, request := range []corenode.RawPinMembershipObservationRequest{{BaselineCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {BaselineCapacity: 1, HistoryCapacity: 0, Instance: [16]byte{1}}, {BaselineCapacity: 1, HistoryCapacity: 1}} {
			a, err := NewWithRawPinMembershipObservation(request)
			if err != nil || a == nil {
				t.Fatal("invalid observation became App constructor denial", err)
			}
			ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
			o := a.RawPinMembershipObservation()
			if o == nil {
				t.Fatal("selected unknown readout absent")
			}
			v := o.Snapshot()
			appRawPinLabels(t, v)
			if v.Status != "UNKNOWN" || v.Reason == "" || a.service.StartupRefusal() != nil {
				t.Fatal("invalid observer changed startup or certified baseline", v)
			}
		}
		// Existing genuine marker-refusal input. No App.Run or listener starts;
		// Source position before config/identity I/O remains CODE evidence.
		isolatePendingApp(t)
		if err := os.WriteFile(os.Getenv("CORSA_IDENTITY_PATH")+".pin-store-marker.json", []byte(`{"format":1,"store":"/elsewhere/secure-sessions.json"}`), 0600); err != nil {
			t.Fatal(err)
		}
		a, err := NewWithRawPinMembershipObservation(appRawPinRequest())
		if err != nil || a == nil {
			t.Fatal("original refusal was converted to new constructor policy", err)
		}
		ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
		v := a.RawPinMembershipObservation().Snapshot()
		appRawPinLabels(t, v)
		if !errors.Is(a.service.StartupRefusal(), domain.ErrPinStoreRefused) || v.Status != "UNKNOWN" || v.Reason == "" || v.PointOnly.Readable || v.PointOnly.InitialStoreRefusal == "" || len(v.BaselineIdentities) != 0 {
			t.Fatal("selected raw readout hid original failclosed refusal", v, a.service.StartupRefusal())
		}
	})
}
