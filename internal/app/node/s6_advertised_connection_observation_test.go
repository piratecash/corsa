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

func appAdvertisedRequest() corenode.AdvertisedConnectionObservationRequest {
	return corenode.AdvertisedConnectionObservationRequest{ConnectionCapacity: 16, SlotCapacity: 8, HistoryCapacity: 128, Instance: [16]byte{212}}
}
func appAdvertisedLabels(t *testing.T, v corenode.AdvertisedConnectionObservationSnapshot) {
	t.Helper()
	if v.Population != "AdvertisedConnections" || v.Qualification != "UNKNOWN" || v.AttributionBound || !v.PastOnly {
		t.Fatal("App upgraded advertised source qualification", v)
	}
}

func TestAppAdvertisedConnectionObservation(t *testing.T) {
	t.Run("actual-unstarted-App-owner-copied-input-key-and-isolation", func(t *testing.T) {
		isolatePendingApp(t)
		request := appAdvertisedRequest()
		want := request
		a, err := NewWithAdvertisedConnectionObservation(request)
		if err != nil || a == nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
		o := a.AdvertisedConnectionObservation()
		if o == nil || o != a.advertisedConnectionObservation || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || o.Key().Instance != want.Instance || a.RegisteredProvenV2Observation() != nil || a.RawPinMembershipObservation() != nil || a.PendingObservation() != nil || a.HandshakeObservation() != nil {
			t.Fatal("App did not retain its one selected original Service readout")
		}
		request = corenode.AdvertisedConnectionObservationRequest{ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: 1, Instance: [16]byte{99}}
		key := o.Key()
		key.Instance[0] = 0
		v := o.Snapshot()
		appAdvertisedLabels(t, v)
		if o.Key().Instance == request.Instance || o.Key().Instance != want.Instance || v.Instance != want.Instance || v.CompletedSequence != 0 || v.CoveredSequence != 0 || v.PeerOwnersAtCut != 0 || v.SlotsAtCut != 0 || v.Pending != 0 || a.service.StartupRefusal() != nil {
			t.Fatal("App caller input/returned key aliased source or baseline", v)
		}
		isolatePendingApp(t)
		other, err := NewWithAdvertisedConnectionObservation(want)
		if err != nil || other == nil {
			t.Fatal(err)
		}
		ownUnstartedGuardApp(t, other, time.Now().Add(10*time.Second))
		second := other.AdvertisedConnectionObservation()
		if second == nil || second == o || second.Identity() == o.Identity() || second.Key() != o.Key() {
			t.Fatal("equal caller Instance collapsed distinct original App owners")
		}
		appAdvertisedLabels(t, second.Snapshot())
	})
	t.Run("same-Service-composite-observers-and-original-borrowed-file-cleanup", func(t *testing.T) {
		isolateTotalFileApp(t)
		request := appAdvertisedRequest()
		raw := appRawPinRequest()
		registered := appRegisteredRequest()
		hf, tf := appFileRequest(t), appTotalFileRequest(t)
		pending := corenode.PendingObservationKey{Anchor: "advertised-composite", ArtifactSHA256: appHandshakeKey().ArtifactSHA256, Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"}
		terminal, guard := appTerminalKey(), appGuardKey()
		a, err := newAppWithObservations(corenode.ObservationOptions{AdvertisedConnections: &request, RawPinMembership: &raw, RegisteredProvenV2: &registered, Pending: &pending, Terminal: &terminal, Guard: &guard, HandshakeFile: &hf, ConnectionTotalFile: &tf})
		if err != nil || a == nil {
			t.Fatal(err)
		}
		ownFileApp(t, a)
		t.Cleanup(func() {
			if err := a.CloseConnectionTotalFileObservation(); err != nil {
				t.Error(err)
			}
		})
		o := a.AdvertisedConnectionObservation()
		owner := domain.PeerIdentityFromWire(a.service.Address())
		if o == nil || a.RawPinMembershipObservation() == nil || a.RegisteredProvenV2Observation() == nil || a.PendingObservation() == nil || a.TerminalObservation() == nil || a.GuardObservation() == nil || a.HandshakeFileObservation() == nil || a.HandshakeObservation() == nil || a.ConnectionTotalFileObservation() == nil || a.ConnectionTotalObservation() == nil {
			t.Fatal("new option displaced an existing observation")
		}
		if o.Identity() != owner || a.RawPinMembershipObservation().Identity() != owner || a.RegisteredProvenV2Observation().Identity() != owner || a.PendingObservation().Identity() != owner || a.TerminalObservation().Identity() != owner || a.GuardObservation().Identity() != owner || a.HandshakeFileObservation().Identity() != owner || a.HandshakeObservation().Identity() != owner || a.ConnectionTotalFileObservation().Identity() != owner || a.ConnectionTotalObservation().Identity() != owner {
			t.Fatal("composite observations use foreign original Services")
		}
		request.Instance = [16]byte{}
		request.ConnectionCapacity = 0
		request.SlotCapacity = 0
		request.HistoryCapacity = 0
		raw.Instance = [16]byte{}
		registered.Instance = [16]byte{}
		if o.Key().Instance != appAdvertisedRequest().Instance || a.RawPinMembershipObservation().Key().Instance != appRawPinRequest().Instance || a.RegisteredProvenV2Observation().Key().Instance != appRegisteredRequest().Instance {
			t.Fatal("composite caller values remained aliased")
		}
		for _, dir := range []*secretfile.Dir{hf.Directory, tf.Directory} {
			if lease, err := dir.TryDirectoryLease(); err == nil {
				_ = lease.Close()
				t.Fatal("original file lease lost during construction composition")
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
				t.Fatal("cleanup kept a lease or closed a borrowed directory", err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		}
		v := o.Snapshot()
		appAdvertisedLabels(t, v)
		if v.Instance != appAdvertisedRequest().Instance || v.CoveredSequence != 0 || v.CompletedSequence != 0 || v.PeerOwnersAtCut != 0 || v.SlotsAtCut != 0 {
			t.Fatal("unrelated file cleanup replaced advertised source", v)
		}
	})
	t.Run("nil-default-invalid-observer-and-original-startup-refusal", func(t *testing.T) {
		isolatePendingApp(t)
		ordinary := New()
		ownUnstartedGuardApp(t, ordinary, time.Now().Add(10*time.Second))
		if ordinary.AdvertisedConnectionObservation() != nil {
			t.Fatal("ordinary App selected advertised observation")
		}
		var absent *App
		if absent.AdvertisedConnectionObservation() != nil {
			t.Fatal("nil App fabricated an observation")
		}
		for _, request := range []corenode.AdvertisedConnectionObservationRequest{{ConnectionCapacity: 0, SlotCapacity: 1, HistoryCapacity: 1, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 0, HistoryCapacity: 1, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: 0, Instance: [16]byte{1}}, {ConnectionCapacity: 1, SlotCapacity: 1, HistoryCapacity: 1}} {
			a, err := NewWithAdvertisedConnectionObservation(request)
			if err != nil || a == nil {
				t.Fatal("invalid observation denied original App construction", err)
			}
			ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
			o := a.AdvertisedConnectionObservation()
			if o == nil {
				t.Fatal("explicit invalid observer handle absent")
			}
			v := o.Snapshot()
			appAdvertisedLabels(t, v)
			if v.Status != "UNKNOWN" || v.Reason == "" || a.service.StartupRefusal() != nil {
				t.Fatal("invalid observer changed original startup or certified facts", v)
			}
		}
		// Original real pin-store marker refusal. No App.Run or listener starts.
		// Advertised facts have no secure-store gate and do not certify startup.
		isolatePendingApp(t)
		if err := os.WriteFile(os.Getenv("CORSA_IDENTITY_PATH")+".pin-store-marker.json", []byte(`{"format":1,"store":"/elsewhere/secure-sessions.json"}`), 0600); err != nil {
			t.Fatal(err)
		}
		a, err := NewWithAdvertisedConnectionObservation(appAdvertisedRequest())
		if err != nil || a == nil {
			t.Fatal("original refusal changed constructor policy", err)
		}
		ownUnstartedGuardApp(t, a, time.Now().Add(10*time.Second))
		o := a.AdvertisedConnectionObservation()
		if o == nil {
			t.Fatal("startup refusal replaced selected handle")
		}
		v := o.Snapshot()
		appAdvertisedLabels(t, v)
		if !errors.Is(a.service.StartupRefusal(), domain.ErrPinStoreRefused) || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || v.Instance != appAdvertisedRequest().Instance || v.CoveredSequence != 0 || v.PeerOwnersAtCut != 0 {
			t.Fatal("readout hid/changed original startup refusal", v, a.service.StartupRefusal())
		}
		// Before-config/identity-I/O copy position is CODE evidence; after-return
		// mutation and real unstarted construction do not invent an I/O barrier.
	})
}
