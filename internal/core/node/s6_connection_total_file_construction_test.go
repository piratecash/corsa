package node

import (
	"bytes"
	"context"
	"errors"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"os"
	"testing"
	"time"
)

func totalFileConstructionRequest(dir *secretfile.Dir) ConnectionTotalFileObservationRequest {
	return ConnectionTotalFileObservationRequest{Key: connectionTotalConstructionKey(), Capacity: 4, ChunkCapacity: 1, Session: [16]byte{171}, Directory: dir, Limits: connbudget.TotalFileObservationLimits{RecordBytes: 2048, StoreBytes: 131072, OrphanBytes: 65536, ScanBytes: 1048576, StoreRecords: 128, OrphanRecords: 64, ScanBatch: 4, ScanSteps: 4096}}
}
func constructTotalFileService(t *testing.T, cfg config.Node, request ConnectionTotalFileObservationRequest, clock func() (time.Duration, error)) (*Service, ObservationReadouts) {
	t.Helper()
	_, id := pendingConstructionInputs(t)
	s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{ConnectionTotalFile: &request}, clock)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		if err := s.CloseConnectionTotalFileObservation(); err != nil {
			t.Error(err)
		}
	})
	if r.ConnectionTotalFile == nil || r.ConnectionTotalFile.service != s || r.ConnectionTotalFile.budget != s.connBudget || s.connManager.config.Budget != s.connBudget || r.ConnectionTotalFile != s.connectionTotalFileObservation || r.ConnectionTotal == nil || r.ConnectionTotal.budget != s.connBudget {
		t.Fatal("file/CM/legacy are not one original Budget")
	}
	return s, r
}
func captureTotalFile(t *testing.T, o *ConnectionTotalFileObservation) ConnectionTotalFileObservationResult {
	t.Helper()
	v, err := o.Capture()
	if err != nil || v.Status != "COMMITTED_PAST" || v.CommitOutcome != secretfile.DurableCommitted || v.AckStatus != "RETIRED" || v.AttributionBound || !v.PastOnly || v.SchemaVersion != 2 {
		t.Fatal(v, err)
	}
	return v
}
func TestS6ConnectionTotalFileAcquisition(t *testing.T) {
	t.Run("actual-CM-budget-file-capture-and-cas", func(t *testing.T) {
		dir := fileConstructionDir(t)
		request := totalFileConstructionRequest(dir)
		clock := &pendingConstructionClock{}
		s, r := constructTotalFileService(t, connectionTotalConfig(t), request, clock.read)
		o := r.ConnectionTotalFile
		if o.Identity() != domain.PeerIdentityFromWire(s.Address()) || o.source.LegacyObservation() == nil || s.StartupRefusal() != nil {
			t.Fatal("actual Service identity/startup")
		}
		out, err := s.connManager.reserveOutbound()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(out.Release)
		in := connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
		clock.set(time.Second, nil)
		aux := connectionTotalTake(t, s.connBudget, connbudget.DirectionAuxiliary)
		v := captureTotalFile(t, o)
		want := connbudget.Config{Total: 3, OutboundReserve: 1, MaxOutbound: 1, MaxInbound: 2, MaxAuxiliary: 1}
		if v.Checkpoint.Config != want || !v.Checkpoint.ThroughFull || v.Checkpoint.ThroughEntry.Count != 3 || v.Checkpoint.Rows != 1 {
			t.Fatal("full shared Budget", v)
		}
		old := fileConstructionRead(t, dir, "q0000000000000001.final")
		if _, err := s.connBudget.Reserve(connbudget.DirectionInbound); !errors.Is(err, connbudget.ErrTotalExhausted) {
			t.Fatal(err)
		}
		if _, err := s.connManager.reserveOutbound(); !errors.Is(err, connbudget.ErrDirectionLimit) {
			t.Fatal("direction precedence", err)
		}
		legacy := r.ConnectionTotal.Snapshot()
		if legacy.Status != "UNKNOWN" || legacy.Cut.Available || len(legacy.Rows) != 0 || !legacy.PastOnly || legacy.Config != want {
			t.Fatal("legacy V2 suffix", legacy)
		}
		clock.set(2*time.Second, nil)
		in.Release()
		in.Release()
		exit := captureTotalFile(t, o)
		if exit.Checkpoint.ThroughFull || exit.Checkpoint.FromEntry != v.Checkpoint.ThroughEntry || exit.Checkpoint.ThroughSequence != 2 || s.connBudget.Snapshot().Used != 2 {
			t.Fatal("actual CAS release", exit)
		}
		for n := 0; n < 3; n++ {
			clock.set(time.Duration(3+2*n)*time.Second, nil)
			fresh := connectionTotalTake(t, s.connBudget, connbudget.DirectionInbound)
			captureTotalFile(t, o)
			clock.set(time.Duration(4+2*n)*time.Second, nil)
			fresh.Release()
			captureTotalFile(t, o)
		}
		if !bytes.Equal(old, fileConstructionRead(t, dir, "q0000000000000001.final")) {
			t.Fatal("ring retirement removed committed file")
		}
		request.Key.Anchor = "changed"
		request.Key.Epoch[0] = 0
		request.Directory = nil
		request.Capacity = 0
		request.Limits.RecordBytes = 0
		if o.Key() != connectionTotalConstructionKey() {
			t.Fatal("request aliases owner")
		}
		out.Release()
		aux.Release()
		// This proves actual Service/CM Reservation owners, not an App.Run or TLS population.
	})
	t.Run("disabled-unavailable-original-errors-and-defaults", func(t *testing.T) {
		clock := &pendingConstructionClock{}
		cfg := connectionTotalConfig(t)
		cfg.MaxTotalConnections = 0
		request := totalFileConstructionRequest(&secretfile.Dir{})
		request.Capacity = -1
		request.ChunkCapacity = -1
		request.Session = [16]byte{}
		request.Limits = connbudget.TotalFileObservationLimits{}
		request.Key.Epoch = [16]byte{}
		request.Key.ClockDomain = [16]byte{}
		request.Key.Configuration = [16]byte{}
		s, r := constructTotalFileService(t, cfg, request, func() (time.Duration, error) { t.Fatal("Total0 clock"); return 0, nil })
		if v, err := r.ConnectionTotalFile.Capture(); err != nil || v.Status != "DISABLED" || v.CommitOutcome != 0 || r.ConnectionTotal.Snapshot().Status != "DISABLED" || s.StartupRefusal() != nil {
			t.Fatal(v, err)
		}
		for _, kind := range []string{"nil-dir", "bad-file-limits", "bad-capacity", "held-lease"} {
			dir := fileConstructionDir(t)
			req := totalFileConstructionRequest(dir)
			switch kind {
			case "nil-dir":
				req.Directory = nil
			case "bad-file-limits":
				req.Limits.RecordBytes = 0
			case "bad-capacity":
				req.Capacity = 0
			case "held-lease":
				lease, err := dir.TryDirectoryLease()
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := lease.Close(); err != nil {
						t.Error(err)
					}
				}()
			}
			s, r := constructTotalFileService(t, connectionTotalConfig(t), req, clock.read)
			if s.StartupRefusal() != nil || r.ConnectionTotalFile.Status().Status != "UNKNOWN" || r.ConnectionTotalFile.Err() == nil {
				t.Fatal("backend failure became policy", kind, r.ConnectionTotalFile.Status())
			}
			token, err := s.connManager.reserveOutbound()
			if err != nil {
				t.Fatal(err)
			}
			token.Release()
			if v, err := r.ConnectionTotalFile.Capture(); err == nil || v.Status != "UNKNOWN" || r.ConnectionTotal.Snapshot().Status != "UNKNOWN" {
				t.Fatal("backend fallback/fake health", kind, v, err)
			}
		}
		cfg = connectionTotalConfig(t)
		cfg.MaxTotalConnections = -1
		s, r = constructTotalFileService(t, cfg, totalFileConstructionRequest(nil), clock.read)
		if !errors.Is(s.connBudgetErr, connbudget.ErrInvalidConfig) || s.connBudget != nil || !errors.Is(s.StartupRefusal(), connbudget.ErrInvalidConfig) || r.ConnectionTotalFile.Err() == nil {
			t.Fatal("original invalid Config lost")
		}
		var absent *ConnectionTotalFileObservation
		var service *Service
		if absent.Key() != (ConnectionTotalObservationKey{}) || !absent.Identity().IsZero() || absent.Status().Status != "INACTIVE" || absent.Close() != nil || service.CloseConnectionTotalFileObservation() != nil {
			t.Fatal("nil file adapter")
		}
		ordinary, _, err := newServiceWithObservations(connectionTotalConfig(t), func() *identity.Identity { _, id := pendingConstructionInputs(t); return id }(), nil, ObservationOptions{}, clock.read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, ordinary, time.Now().Add(5*time.Second))
		if ordinary.connectionTotalFileObservation != nil || ordinary.CloseConnectionTotalFileObservation() != nil {
			t.Fatal("ordinary selected file source")
		}
	})
	t.Run("exclusive-total-mode-and-separate-store-coexistence", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		cfg.MaxTotalConnections = 3
		cfg.MaxOutgoingPeers = 1
		cfg.MaxIncomingPeers = 2
		cfg.MaxAuxiliaryConnections = 1
		dir := fileConstructionDir(t)
		request := totalFileConstructionRequest(dir)
		memory := ConnectionTotalObservationRequest{Key: connectionTotalConstructionKey(), Capacity: 4}
		if s, _, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{ConnectionTotal: &memory, ConnectionTotalFile: &request}, (&pendingConstructionClock{}).read); err == nil || s != nil {
			t.Fatal("two total owners selected")
		}
		lease, err := dir.TryDirectoryLease()
		if err != nil {
			t.Fatal("refused dual-mode acquired lease", err)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		other := fileConstructionDir(t)
		handshake := fileConstructionRequest(other)
		p, g, terminal := pendingConstructionKey(), guardConstructionKey(), terminalConstructionKey()
		options := ObservationOptions{Pending: &p, Guard: &g, Terminal: &terminal, HandshakeFile: &handshake, ConnectionTotalFile: &request}
		s, readouts, err := newServiceWithObservations(cfg, id, nil, options, (&pendingConstructionClock{}).read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		t.Cleanup(func() {
			if err := s.CloseConnectionTotalFileObservation(); err != nil {
				t.Error(err)
			}
			if err := s.CloseHandshakeFileObservation(); err != nil {
				t.Error(err)
			}
		})
		if readouts.Pending == nil || readouts.Guard == nil || readouts.Terminal == nil || readouts.HandshakeFile == nil || readouts.ConnectionTotalFile == nil || readouts.ConnectionTotal == nil {
			t.Fatal("one envelope lost sources")
		}
		if readouts.ConnectionTotalFile.Identity() != domain.PeerIdentityFromWire(s.Address()) || readouts.HandshakeFile.Identity() != readouts.ConnectionTotalFile.Identity() {
			t.Fatal("foreign Service attribution")
		}
		// A shared directory is not auto-partitioned, adopted or stolen.
		shared := request
		shared.Directory = other
		s2, r2, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{ConnectionTotalFile: &shared}, (&pendingConstructionClock{}).read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s2, time.Now().Add(5*time.Second))
		t.Cleanup(func() {
			if err := s2.CloseConnectionTotalFileObservation(); err != nil {
				t.Error(err)
			}
		})
		if r2.ConnectionTotalFile.Status().Status != "UNKNOWN" || s2.StartupRefusal() != nil {
			t.Fatal("shared store silently adopted", r2.ConnectionTotalFile.Status())
		}
	})
	t.Run("Run-early-refusal-releases-actual-file-lease", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		cfg.MaxTotalConnections = 3
		cfg.MaxOutgoingPeers = 1
		cfg.MaxIncomingPeers = 2
		cfg.MaxAuxiliaryConnections = 1
		if err := os.WriteFile(cfg.IdentityPath+pinStoreMarkerSuffix, []byte(`{"format":1,"store":"/elsewhere/secure-sessions.json"}`), 0600); err != nil {
			t.Fatal(err)
		}
		dir := fileConstructionDir(t)
		request := totalFileConstructionRequest(dir)
		s, r, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{ConnectionTotalFile: &request}, (&pendingConstructionClock{}).read)
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, time.Now().Add(5*time.Second))
		t.Cleanup(func() {
			if err := s.CloseConnectionTotalFileObservation(); err != nil {
				t.Error(err)
			}
		})
		if !errors.Is(s.StartupRefusal(), domain.ErrPinStoreRefused) || r.ConnectionTotalFile.Status().Status != "READY" {
			t.Fatal("real pin-store startup refusal", s.StartupRefusal(), r.ConnectionTotalFile.Status())
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		join := k5OwnedCleanup(t, cancel)
		join(done)
		var runErr error
		go func() { defer close(done); runErr = s.Run(ctx) }()
		k5Wait(t, done)
		if !errors.Is(runErr, domain.ErrPinStoreRefused) || !r.ConnectionTotalFile.Status().Closed {
			t.Fatal("Run cleanup/policy", runErr, r.ConnectionTotalFile.Status())
		}
		lease, err := dir.TryDirectoryLease()
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
