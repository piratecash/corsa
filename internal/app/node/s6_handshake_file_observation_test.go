package node

import (
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

func appFileRequest(t *testing.T) corenode.HandshakeFileObservationRequest {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("file observer lease unsupported")
	}
	dir, err := secretfile.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	return corenode.HandshakeFileObservationRequest{Key: appHandshakeKey(), Capacity: 2, ChunkCapacity: 1, Session: [16]byte{151}, Directory: dir, Limits: corenode.HandshakeFileObservationLimits{RecordBytes: 2048, StoreBytes: 131072, OrphanBytes: 65536, ScanBytes: 1048576, StoreRecords: 128, OrphanRecords: 64, ScanBatch: 4, ScanSteps: 4096}}
}
func ownFileApp(t *testing.T, a *App) {
	t.Helper()
	ownUnstartedGuardApp(t, a, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := a.CloseHandshakeFileObservation(); err != nil {
			t.Error(err)
		}
	})
}

func TestAppHandshakeFileObservationConstruction(t *testing.T) {
	isolatePendingApp(t)
	request := appFileRequest(t)
	a, err := NewWithHandshakeFileObservation(request)
	if err != nil {
		t.Fatal(err)
	}
	ownFileApp(t, a)
	o := a.HandshakeFileObservation()
	legacy := a.HandshakeObservation()
	if o == nil || o != a.handshakeFileObservation || o.Key() != request.Key || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || legacy == nil || legacy.Identity() != o.Identity() || a.service.StartupRefusal() != nil {
		t.Fatal("App did not acquire one Service file source")
	}
	if v := o.Status(); v.SchemaVersion != 2 || !v.Available || v.AttributionBound || !v.PastOnly {
		t.Fatal("file handle header", v)
	}
	if v := legacy.Snapshot(); v.Status != "UNKNOWN" || v.Cut.Available || len(v.Rows) != 0 || !v.PastOnly {
		t.Fatal("old App getter certified V2 suffix", v)
	}
	request.Key.Anchor = "changed"
	request.Key.Epoch[0] = 0
	request.Session[0] = 0
	request.Capacity = 0
	request.Directory = nil
	request.Limits.RecordBytes = 0
	key := o.Key()
	key.Configuration[0] = 0
	if o.Key() != appHandshakeKey() {
		t.Fatal("caller mutated App key")
	}
	if err := a.CloseHandshakeFileObservation(); err != nil || !o.Status().Closed {
		t.Fatal("unstarted App close", err, o.Status())
	}
	if err := a.CloseHandshakeFileObservation(); err != nil {
		t.Fatal("idempotent App close", err)
	}
	// Construction/getter/lifetime scope only; no App.Run/TLS path is claimed.
}

func TestAppHandshakeFileObservationCoexistenceDefaults(t *testing.T) {
	isolatePendingApp(t)
	request := appFileRequest(t)
	pk := corenode.PendingObservationKey{Anchor: "file-app-pending", ArtifactSHA256: appHandshakeKey().ArtifactSHA256, Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-duration"}
	tk, gk := appTerminalKey(), appGuardKey()
	total := corenode.ConnectionTotalObservationRequest{Key: appConnectionTotalKey(), Capacity: 4}
	a, err := newAppWithObservations(corenode.ObservationOptions{Pending: &pk, Terminal: &tk, Guard: &gk, HandshakeFile: &request, ConnectionTotal: &total})
	if err != nil {
		t.Fatal(err)
	}
	ownFileApp(t, a)
	owner := domain.PeerIdentityFromWire(a.service.Address())
	if a.PendingObservation() == nil || a.TerminalObservation() == nil || a.GuardObservation() == nil || a.ConnectionTotalObservation() == nil || a.HandshakeFileObservation() == nil || a.HandshakeObservation() == nil || a.PendingObservation().Identity() != owner || a.TerminalObservation().Identity() != owner || a.GuardObservation().Identity() != owner || a.ConnectionTotalObservation().Identity() != owner || a.HandshakeFileObservation().Identity() != owner {
		t.Fatal("App constructed foreign/multiple Services")
	}
	ordinary := New()
	ownUnstartedGuardApp(t, ordinary, time.Now().Add(5*time.Second))
	if ordinary.HandshakeFileObservation() != nil || ordinary.HandshakeObservation() != nil || ordinary.CloseHandshakeFileObservation() != nil {
		t.Fatal("ordinary App activated file source")
	}
	var absent *App
	if absent.HandshakeFileObservation() != nil || absent.CloseHandshakeFileObservation() != nil {
		t.Fatal("nil App fabricated ownership")
	}
}

func TestAppHandshakeFileObservationValidationUnavailable(t *testing.T) {
	isolatePendingApp(t)
	request := appFileRequest(t)
	borrowed := request.Directory
	memory := corenode.HandshakeObservationRequest{Key: request.Key, Capacity: 2}
	if a, err := newAppWithObservations(corenode.ObservationOptions{Handshake: &memory, HandshakeFile: &request}); err == nil || a != nil {
		t.Fatal("App dual mode silently selected")
	}
	bad := request
	bad.Key = corenode.HandshakeObservationKey{}
	if a, err := NewWithHandshakeFileObservation(bad); err == nil || a != nil {
		t.Fatal("invalid App refs accepted")
	}
	lease, err := borrowed.TryDirectoryLease()
	if err != nil {
		t.Fatal("failed App constructor held file lease", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	request.Directory = nil
	a, err := NewWithHandshakeFileObservation(request)
	if err != nil {
		t.Fatal("I/O unavailable became App startup policy", err)
	}
	ownFileApp(t, a)
	if a.service.StartupRefusal() != nil || a.HandshakeFileObservation().Status().Status != "UNKNOWN" || a.HandshakeFileObservation().Err() == nil {
		t.Fatal("unavailable source hidden or node refused")
	}
	if v := a.HandshakeObservation().Snapshot(); v.Status != "UNKNOWN" || v.Cut.Available {
		t.Fatal("file failure fell back to ordinary V1", v)
	}
	if err := a.CloseHandshakeFileObservation(); err != nil {
		t.Fatal(err)
	}
	if err := borrowed.DurablePreflight(); err != nil {
		t.Fatal("App observer closed caller Dir", err)
	}
}
