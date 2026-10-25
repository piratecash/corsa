package node

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

func terminalConstructionKey() TerminalObservationKey {
	return TerminalObservationKey{Anchor: "terminal-anchor", ArtifactSHA256: strings.Repeat("c", 64), Boot: "terminal-boot", ProcessInstance: "terminal-process"}
}

func ownTerminalConstruction(t *testing.T, s *Service, end time.Time) {
	t.Helper()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			newUnstartedRuntimeCleanup(s, s.runCancel)()
			s.closeAllInboundConns()
			s.WaitBackground()
		}()
		timer := time.NewTimer(time.Until(end))
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("terminal fixture did not join within its original absolute deadline")
		}
	})
}

func terminalConstructionService(t *testing.T, end time.Time) (*Service, *TerminalObservation) {
	t.Helper()
	cfg, id := pendingConstructionInputs(t)
	s, o, err := NewServiceWithTerminalObservation(cfg, id, nil, terminalConstructionKey())
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, end)
	return s, o
}

func terminalCopiedFields(t *testing.T, s *Service, o *TerminalObservation) TerminalObservationSnapshot {
	t.Helper()
	want := s.terminalObservation.snapshot()
	got := o.Snapshot()
	if !got.Available || got.AttributionBound || got.SchemaVersion != 1 || got.Population != want.population || got.Status != want.status || got.CodeStatus != want.codeStatus || got.Reason != want.reason || got.Instance != want.instance ||
		got.Registered != want.registered || got.Active != want.active || got.Removed != want.removed || got.Classified != want.classified || got.Pending != want.pending || got.Missing != want.missing ||
		got.RegistrationSerial != want.registrationSerial || got.TerminalSerial != want.terminalSerial || got.Version != want.version || got.Coarse != want.coarse || got.Buckets != want.buckets {
		t.Fatalf("public readout lost original restricted fields: %+v / %+v", got, want)
	}
	if got.Directions != ([2]string{"inbound", "outbound"}) || got.Codes != k10ProtocolCodes || got.Kinds != ([k10KindCount]string{"missing", "owner-stopped", "owner-error", "owner-EOF", "command-stopped", "loopback-condemned", "local-eviction", "legacy-refusal"}) {
		t.Fatal("fixed direction/kind/code schema labels changed")
	}
	return got
}

func TestS6TerminalConstructionReadout(t *testing.T) {
	for _, order := range []string{"remove-before-owner", "owner-before-remove"} {
		t.Run(order, func(t *testing.T) {
			end := time.Now().Add(5 * time.Second)
			s, o := terminalConstructionService(t, end)
			baseline := terminalCopiedFields(t, s, o)
			if baseline.Registered != 0 || baseline.Active != 0 || baseline.Instance == ([16]byte{}) || o.service != s || o.Identity() != domain.PeerIdentityFromWire(s.Address()) {
				t.Fatal("constructor did not bind before the first actual registration")
			}
			conn, entry := k10Attach(t, s, 1, netcore.Inbound) // original registry owner
			ps := k10Session(s, conn, entry)                   // existing original close/report fixture
			active := terminalCopiedFields(t, s, o)
			if active.Registered != 1 || active.Active != 1 || active.Version <= baseline.Version {
				t.Fatal("original registration not visible", active)
			}
			if order == "owner-before-remove" {
				ps.k10FinishReported(nil)
			} else {
				if err := ps.Close(); err != nil {
					t.Fatal(err)
				}
				v := terminalCopiedFields(t, s, o)
				if v.Status != "IN_FLIGHT" || v.CodeStatus != "UNKNOWN" || v.Pending != 1 || v.Classified != 0 {
					t.Fatal("pending original owner falsely finalized", v)
				}
			}
			if err := ps.Close(); err != nil {
				t.Fatal(err)
			}
			ps.k10FinishReported(nil)
			final := terminalCopiedFields(t, s, o)
			if final.Status != "COMPLETE" || final.CodeStatus != "UNKNOWN" || final.Coarse != 1 || final.Classified != 1 || final.Removed != 1 || final.Active != 0 || final.Pending != 0 {
				t.Fatal("coarse owner completion promoted or lost", final)
			}
			copyKey := o.Key()
			copyKey.Boot = "changed"
			final.Buckets[0][k10OwnerStopped][0] = 99
			final.Codes[1], final.Kinds[1], final.Directions[0] = "changed", "changed", "changed"
			if o.Key() != terminalConstructionKey() || terminalCopiedFields(t, s, o).Buckets[0][k10OwnerStopped][0] != 1 || active.Active != 1 {
				t.Fatal("readout or key aliases mutable source state")
			}
			ps.k10FinishReported(nil)
			if v := o.Snapshot(); v.Classified != 1 || v.TerminalSerial != final.TerminalSerial {
				t.Fatal("repeated original completion counted twice", v)
			}
		})
	}
}

func TestS6TerminalConstructionMissingAndFrozen(t *testing.T) {
	for _, mode := range []string{"missing", "contradictory-owner"} {
		t.Run(mode, func(t *testing.T) {
			end := time.Now().Add(5 * time.Second)
			s, o := terminalConstructionService(t, end)
			conn, entry := k10Attach(t, s, 1, netcore.Outbound)
			ps := k10Session(s, conn, entry)
			if err := ps.Close(); err != nil {
				t.Fatal(err)
			}
			if mode == "missing" {
				ps.k10MissingIfUnfinished()
			} else {
				ps.k10FinishReported(nil)
				ps.k10FinishReported(io.EOF)
			}
			v := terminalCopiedFields(t, s, o)
			if v.Status != "UNKNOWN" || v.CodeStatus != "UNKNOWN" || v.Reason == "" || (mode == "missing" && v.Missing != 1) {
				t.Fatal("source loss/frozen prefix promoted to complete", v)
			}
			if again := o.Snapshot(); again.Version != v.Version || again.Buckets != v.Buckets || again.Missing != v.Missing {
				t.Fatal("readout changed frozen prefix")
			}
		})
	}
}

func TestS6ObservationCombinedConstruction(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	cfg, id := pendingConstructionInputs(t)
	pending, terminal := pendingConstructionKey(), terminalConstructionKey()
	clock := &pendingConstructionClock{}
	s, readouts, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &pending, Terminal: &terminal}, clock.read)
	if err != nil {
		t.Fatal(err)
	}
	ownTerminalConstruction(t, s, end)
	p, o := readouts.Pending, readouts.Terminal
	if p == nil || o == nil || p.service != s || o.service != s || p.Identity() != o.Identity() || o.Identity() != domain.PeerIdentityFromWire(s.Address()) {
		t.Fatal("combined options built two Services or foreign handles")
	}
	pending.Anchor, terminal.Anchor = "changed", "changed"
	before, first := p.Snapshot(), terminalCopiedFields(t, s, o)
	if !before.CoverageValid || before.SourceID == ([16]byte{}) || first.Instance == ([16]byte{}) || before.SourceID == first.Instance || p.Key() != pendingConstructionKey() || o.Key() != terminalConstructionKey() {
		t.Fatal("source IDs or frozen keys collapsed")
	}
	if !s.queuePeerFrame("10.80.0.19:64646", s6PendingFrame(s, "combined")) {
		t.Fatal("actual combined queue admission refused")
	}
	conn, entry := k10Attach(t, s, 1, netcore.Inbound)
	ps := k10Session(s, conn, entry)
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	ps.k10FinishReported(nil)
	after, last := p.Snapshot(), terminalCopiedFields(t, s, o)
	if after.SourceID != before.SourceID || after.Count != 1 || !after.CoverageValid || last.Instance != first.Instance || last.Classified != 1 || after.AttributionBound || last.AttributionBound {
		t.Fatal("one actual Service did not drive both original sources", after, last)
	}
}

func TestS6TerminalConstructionUnavailableAndValidation(t *testing.T) {
	t.Run("invalid-key", func(t *testing.T) {
		cfg, id := pendingConstructionInputs(t)
		key := terminalConstructionKey()
		key.ProcessInstance = ""
		s, o, err := NewServiceWithTerminalObservation(cfg, id, nil, key)
		if err == nil || s != nil || o != nil {
			t.Fatal("invalid caller references accepted")
		}
	})
	t.Run("initial-instance-unknown", func(t *testing.T) {
		end := time.Now().Add(5 * time.Second)
		cfg, id := pendingConstructionInputs(t)
		// Freeze an unavailable source instance at the same constructor edge,
		// never install or replace an observer on an already returned Service.
		c := &terminalObservationConstruction{key: terminalConstructionKey(), identity: domain.PeerIdentityFromWire(id.Address)}
		s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{terminalObservation: c})
		ownTerminalConstruction(t, s, end)
		if v := c.readout.Snapshot(); v.Status != "UNKNOWN" || v.CodeStatus != "UNKNOWN" || v.Reason != "invalid-source-instance" {
			t.Fatal("initial source failure falsely complete", v)
		}
		conn, entry := k10Attach(t, s, 1, netcore.Inbound)
		k10Remove(s, conn, entry)
		if s.StartupRefusal() != nil {
			t.Fatal("observation failure changed original startup/admission")
		}
	})
	t.Run("terminal-no-clock-and-default-off", func(t *testing.T) {
		end := time.Now().Add(5 * time.Second)
		cfg, id := pendingConstructionInputs(t)
		key := terminalConstructionKey()
		calls := 0
		s, o, err := newServiceWithObservations(cfg, id, nil, ObservationOptions{Terminal: &key}, func() (time.Duration, error) { calls++; return 0, errors.New("not a K10 clock") })
		if err != nil {
			t.Fatal(err)
		}
		ownTerminalConstruction(t, s, end)
		if o.Terminal == nil || o.Pending != nil || o.Terminal.Snapshot().Status != "COMPLETE" || calls != 0 {
			t.Fatal("K10 fabricated a duration clock or enabled K12")
		}
		ordinary := NewService(cfg, id, nil)
		ownTerminalConstruction(t, ordinary, end)
		if ordinary.terminalObservation != nil || ordinary.pendingObservation != nil {
			t.Fatal("ordinary construction opted in")
		}
		var absent *TerminalObservation
		if v := absent.Snapshot(); v.Available || v.Status != "INACTIVE" || v.CodeStatus != "UNKNOWN" || v.AttributionBound {
			t.Fatal("nil source fabricated evidence", v)
		}
	})
}
