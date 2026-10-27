package node

import (
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"os"
	"testing"
	"time"
)

func isolateTotalFileApp(t *testing.T) {
	t.Helper()
	isolatePendingApp(t)
	t.Setenv("CORSA_MAX_TOTAL_CONNECTIONS", "3")
	t.Setenv("CORSA_MAX_OUTGOING_PEERS", "1")
	t.Setenv("CORSA_MAX_INCOMING_PEERS", "2")
	t.Setenv("CORSA_MAX_AUXILIARY_CONNECTIONS", "1")
}
func appTotalFileRequest(t *testing.T) corenode.ConnectionTotalFileObservationRequest {
	t.Helper()
	f := appFileRequest(t)
	return corenode.ConnectionTotalFileObservationRequest{Key: appConnectionTotalKey(), Capacity: 4, ChunkCapacity: 1, Session: [16]byte{181}, Directory: f.Directory, Limits: connbudget.TotalFileObservationLimits(f.Limits)}
}
func ownTotalFileApp(t *testing.T, a *App) {
	t.Helper()
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := a.CloseConnectionTotalFileObservation(); err != nil {
			t.Error(err)
		}
	})
}
func TestAppConnectionTotalFileAcquisition(t *testing.T) {
	t.Run("one-Service-frozen-request-legacy-and-unstarted-close", func(t *testing.T) {
		isolateTotalFileApp(t)
		request := appTotalFileRequest(t)
		borrowed := request.Directory
		a, err := NewWithConnectionTotalFileObservation(request)
		if err != nil {
			t.Fatal(err)
		}
		ownTotalFileApp(t, a)
		o := a.ConnectionTotalFileObservation()
		legacy := a.ConnectionTotalObservation()
		if o == nil || o != a.connectionTotalFileObservation || o.Key() != appConnectionTotalKey() || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || legacy == nil || legacy.Identity() != o.Identity() || a.service.StartupRefusal() != nil {
			t.Fatal("App is not one Service construction")
		}
		if v := o.Status(); !v.Available || v.SchemaVersion != 2 || v.AttributionBound || !v.PastOnly || v.Status != "READY" {
			t.Fatal(v)
		}
		if v := legacy.Snapshot(); v.Status != "UNKNOWN" || v.Cut.Available || len(v.Rows) != 0 || !v.PastOnly || v.Config.Total != 3 {
			t.Fatal("legacy copied V2 suffix", v)
		}
		request.Key.Anchor = "changed"
		request.Key.Epoch[0] = 0
		request.Session[0] = 0
		request.Directory = nil
		request.Capacity = 0
		request.Limits.RecordBytes = 0
		copyKey := o.Key()
		copyKey.ClockDomain[0] = 0
		if o.Key() != appConnectionTotalKey() {
			t.Fatal("mutable App source binding")
		}
		if err := a.CloseConnectionTotalFileObservation(); err != nil || !o.Status().Closed {
			t.Fatal(err, o.Status())
		}
		if err := a.CloseConnectionTotalFileObservation(); err != nil {
			t.Fatal(err)
		}
		if err := borrowed.DurablePreflight(); err != nil {
			t.Fatal("App closed caller directory", err)
		}
		// Construction/getter/lifetime only, not App.Run/TLS traffic or full K5.
	})
	t.Run("defaults-validation-backend-and-disabled", func(t *testing.T) {
		isolateTotalFileApp(t)
		ordinary := New()
		ownUnstartedGuardApp(t, ordinary, time.Now().Add(5*time.Second))
		var absent *App
		if ordinary.ConnectionTotalFileObservation() != nil || ordinary.CloseConnectionTotalFileObservation() != nil || absent.ConnectionTotalFileObservation() != nil || absent.CloseConnectionTotalFileObservation() != nil {
			t.Fatal("default/nil file activation")
		}
		request := appTotalFileRequest(t)
		request.Key.Anchor = ""
		if a, err := NewWithConnectionTotalFileObservation(request); err == nil || a != nil {
			t.Fatal("invalid references accepted")
		}
		request = appTotalFileRequest(t)
		request.Directory = nil
		a, err := NewWithConnectionTotalFileObservation(request)
		if err != nil {
			t.Fatal("backend became startup policy", err)
		}
		ownTotalFileApp(t, a)
		if a.service.StartupRefusal() != nil || a.ConnectionTotalFileObservation().Status().Status != "UNKNOWN" || a.ConnectionTotalFileObservation().Err() == nil || a.ConnectionTotalObservation().Snapshot().Status != "UNKNOWN" {
			t.Fatal("App unknown backend fallback")
		}
		t.Setenv("CORSA_MAX_TOTAL_CONNECTIONS", "0")
		request = appTotalFileRequest(t)
		request.Directory = nil
		request.Capacity = -1
		request.ChunkCapacity = -1
		request.Session = [16]byte{}
		request.Limits = connbudget.TotalFileObservationLimits{}
		request.Key.Epoch = [16]byte{}
		request.Key.ClockDomain = [16]byte{}
		request.Key.Configuration = [16]byte{}
		disabled, err := NewWithConnectionTotalFileObservation(request)
		if err != nil {
			t.Fatal(err)
		}
		ownTotalFileApp(t, disabled)
		if v, err := disabled.ConnectionTotalFileObservation().Capture(); err != nil || v.Status != "DISABLED" || v.CommitOutcome != 0 || disabled.ConnectionTotalObservation().Snapshot().Status != "DISABLED" {
			t.Fatal(v, err)
		}
	})
	t.Run("early-App-Run-refusal-closes-actual-lease", func(t *testing.T) {
		isolateTotalFileApp(t)
		// Same original pin-marker refusal format as core's real store tests.
		if err := os.WriteFile(os.Getenv("CORSA_IDENTITY_PATH")+".pin-store-marker.json", []byte(`{"format":1,"store":"/elsewhere/secure-sessions.json"}`), 0600); err != nil {
			t.Fatal(err)
		}
		request := appTotalFileRequest(t)
		a, err := NewWithConnectionTotalFileObservation(request)
		if err != nil {
			t.Fatal(err)
		}
		ownTotalFileApp(t, a)
		if !errors.Is(a.service.StartupRefusal(), domain.ErrPinStoreRefused) || a.ConnectionTotalFileObservation().Status().Status != "READY" {
			t.Fatal("original pin-store refusal", a.service.StartupRefusal())
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := a.Run(ctx); !errors.Is(err, domain.ErrPinStoreRefused) || !a.ConnectionTotalFileObservation().Status().Closed {
			t.Fatal("App early return did not close file source", err)
		}
		lease, err := request.Directory.TryDirectoryLease()
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		// Only the original pre-network refusal was run; no successful App.Run/TLS claim.
	})
}
