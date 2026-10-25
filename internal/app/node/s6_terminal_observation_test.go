package node

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	corenode "github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

func appTerminalKey() corenode.TerminalObservationKey {
	return corenode.TerminalObservationKey{Anchor: "app-terminal", ArtifactSHA256: strings.Repeat("d", 64), Boot: "app-boot", ProcessInstance: "app-process"}
}

func waitAppTerminal(t *testing.T, end time.Time, ready func() bool) {
	t.Helper()
	for !ready() {
		if time.Until(end) <= 0 {
			t.Fatal("original terminal owner did not reach its bounded milestone")
		}
		timer := time.NewTimer(min(5*time.Millisecond, time.Until(end)))
		<-timer.C
	}
}

func TestAppTerminalObservationActualRegistration(t *testing.T) {
	// One absolute deadline includes cleanup; work does not consume its last5s.
	end := time.Now().Add(15 * time.Second)
	workEnd := end.Add(-5 * time.Second)
	isolatePendingApp(t)
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CORSA_LISTEN_ADDRESS", address)
	t.Setenv("CORSA_LISTENER", "1")
	// No external seeds or unrelated completed connection. Invalid loopback
	// port0 cannot reach an external peer; it does not change admission limits.
	t.Setenv("CORSA_BOOTSTRAP_PEERS", "127.0.0.1:0")
	t.Setenv("CORSA_BOOTSTRAP_PEER", "")
	t.Setenv("CORSA_CONNECT_ONLY", "")
	t.Setenv("CORSA_PROXY", "")
	a, err := NewWithTerminalObservation(appTerminalKey())
	if err != nil {
		t.Fatal(err)
	}
	o := a.TerminalObservation()
	baseline := o.Snapshot()
	if o == nil || a.PendingObservation() != nil || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) || !baseline.Available || baseline.Status != "COMPLETE" || baseline.Registered != 0 || baseline.Active != 0 || baseline.Instance == ([16]byte{}) || baseline.AttributionBound {
		t.Fatalf("terminal source not attached to actual App before Run: %+v", baseline)
	}
	ctx, cancel := context.WithDeadline(context.Background(), workEnd)
	runDone := make(chan error, 1)
	var client net.Conn
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			if client != nil {
				_ = client.Close()
			}
			cancel()
			timer := time.NewTimer(time.Until(end))
			defer timer.Stop()
			select {
			case err := <-runDone:
				if err != nil {
					t.Errorf("original Service.Run: %v", err)
				}
			case <-timer.C:
				t.Error("original Service.Run did not join by absolute cleanup deadline")
				return
			}
			backgroundDone := make(chan struct{})
			go func() { a.service.WaitBackground(); close(backgroundDone) }()
			select {
			case <-backgroundDone:
			case <-timer.C:
				t.Error("original background work did not join by absolute cleanup deadline")
			}
		})
	}
	t.Cleanup(stop) // Own socket/cancel/joins BEFORE the Run goroutine starts.
	go func() { runDone <- a.service.Run(ctx) }()
	// The first successful readiness dial IS the actual tested connection.
	// A competing bind cannot pass: only this App's source may register it.
	waitAppTerminal(t, workEnd, func() bool {
		var dialErr error
		client, dialErr = (&net.Dialer{Deadline: workEnd}).DialContext(ctx, "tcp", address)
		return dialErr == nil
	})
	if err := client.SetDeadline(workEnd); err != nil {
		t.Fatal(err)
	}
	if n, err := client.Write([]byte("{")); err != nil || n != 1 {
		t.Fatalf("legacy first byte: %d, %v", n, err)
	}
	waitAppTerminal(t, workEnd, func() bool {
		v := o.Snapshot()
		return v.Instance == baseline.Instance && v.Registered == 1 && v.Active == 1
	})
	registered := o.Snapshot()
	if registered.Population != "RegisteredTransportInstance" || registered.Removed != 0 || registered.RegistrationSerial != 1 || registered.Version <= baseline.Version || registered.AttributionBound {
		t.Fatalf("real registration not reflected by the bound handle: %+v", registered)
	}
	if n, err := client.Write([]byte("garbage\n")); err != nil || n != len("garbage\n") {
		t.Fatalf("malformed line completion: %d, %v", n, err)
	}
	reader := bufio.NewReader(io.LimitReader(client, 64*1024+1))
	total := 0
	for {
		line, err := reader.ReadString('\n')
		total += len(line)
		if total > 64*1024 {
			t.Fatal("unbounded original error response")
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("original server did not end its malformed-frame stream: %v", err)
			}
			break
		}
	}
	if total == 0 {
		t.Fatal("stream ended without the original malformed-frame response")
	}
	waitAppTerminal(t, workEnd, func() bool {
		v := o.Snapshot()
		return v.Removed == 1 && v.Active == 0 && v.Classified == 1 && v.Pending == 0
	})
	final := o.Snapshot()
	if final.Instance != baseline.Instance || final.Status != "COMPLETE" || final.CodeStatus != "UNKNOWN" || final.Coarse != 1 || final.Missing != 0 || final.TerminalSerial != 1 || final.AttributionBound {
		t.Fatalf("original coarse terminal classification lost/promoted: %+v", final)
	}
	if registered.Active != 1 || registered.Removed != 0 {
		t.Fatal("later terminal mutated an earlier snapshot")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if again := o.Snapshot(); again.Classified != 1 || again.Version != final.Version {
		t.Fatal("repeated close/read duplicated terminal")
	}
	stop()
}

func TestAppObservationCombinedAndDefaults(t *testing.T) {
	end := time.Now().Add(5 * time.Second)
	isolatePendingApp(t)
	pending := corenode.PendingObservationKey{Anchor: "app-pending", ArtifactSHA256: strings.Repeat("e", 64), Boot: "app-boot", ProcessInstance: "app-process", ClockDomain: "app-boot-duration"}
	terminal := appTerminalKey()
	a, err := newAppWithObservations(corenode.ObservationOptions{Pending: &pending, Terminal: &terminal})
	if err != nil {
		t.Fatal(err)
	}
	boundedBackground := func(a *App) {
		t.Cleanup(func() {
			done := make(chan struct{})
			go func() { a.service.WaitBackground(); close(done) }()
			timer := time.NewTimer(time.Until(end))
			defer timer.Stop()
			select {
			case <-done:
			case <-timer.C:
				t.Error("unstarted App background join exceeded original deadline")
			}
		})
	}
	boundedBackground(a)
	p, o := a.PendingObservation(), a.TerminalObservation()
	if p == nil || o == nil || p.Identity() != o.Identity() || o.Identity() != domain.PeerIdentityFromWire(a.service.Address()) {
		t.Fatal("common App construction did not bind both observations")
	}
	pending.Anchor, terminal.Anchor = "changed", "changed"
	if p.Key().Anchor != "app-pending" || o.Key() != appTerminalKey() {
		t.Fatal("common App constructor retained caller-mutable keys")
	}
	before, registered := p.Snapshot(), o.Snapshot()
	if !registered.Available || registered.AttributionBound || registered.Registered != 0 {
		t.Fatal("terminal baseline not installed", registered)
	}
	if _, err := trustedclock.SinceBoot(); err == nil {
		t.Run("both-bound", func(t *testing.T) {
			if !before.Available || !before.CoverageValid || before.SourceID == registered.Instance || before.AttributionBound {
				t.Fatal("independent source identities or coverage lost", before, registered)
			}
			if _, err := a.service.CancelOutgoingDelivery(protocol.MessageID("combined-app"), domain.PeerIdentityFromWire(strings.Repeat("2", 40))); err != nil {
				t.Fatal(err)
			}
			after := p.Snapshot()
			if after.SourceID != before.SourceID || after.Sequence != before.Sequence+1 || a.service.PendingQueueObservation().SourceID != after.SourceID || o.Snapshot().Instance != registered.Instance {
				t.Fatal("App handle did not read the original Service mutation")
			}
		})
	} else if before.CoverageValid || before.UnknownReason == "" {
		t.Fatal("unsupported K12 clock fabricated coverage")
	}
	ordinary := New()
	boundedBackground(ordinary)
	if ordinary.PendingObservation() != nil || ordinary.TerminalObservation() != nil {
		t.Fatal("ordinary App constructor enabled observation")
	}
	if bad, err := NewWithTerminalObservation(corenode.TerminalObservationKey{}); err == nil || bad != nil {
		t.Fatal("invalid terminal key accepted")
	}
	var absent *App
	if absent.TerminalObservation() != nil {
		t.Fatal("nil App fabricated an observation")
	}
}
