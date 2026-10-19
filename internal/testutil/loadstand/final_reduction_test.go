//go:build (darwin && cgo) || linux

package loadstand

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func reductionFixture(t *testing.T) (*Process, CompletedStartBinding, FinalReport) {
	t.Helper()
	binding, report := finalFixture(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	a, err := newLaunchArtifacts(t.Context(), dir, binding.launch)
	if err != nil {
		t.Fatal(err)
	}
	p := &Process{done: make(chan struct{}), artifacts: a, rpc: terminalRPC(t)}
	t.Cleanup(func() { _ = a.close() })
	observed, err := p.CompletedStart(binding.Receipt())
	if err != nil {
		t.Fatal(err)
	}
	return p, observed, report
}
func finishReductionFixture(t *testing.T, p *Process, report FinalReport) {
	t.Helper()
	raw, err := report.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.artifacts.writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = p.artifacts.writer.Close(); err != nil {
		t.Fatal(err)
	}
	p.artifacts.writer = nil
	p.exit = ProcessExitResult{Kind: WaitNormal}
	close(p.done)
}
func TestFinalReductionBeginsAfterDoneAndSealsCopiedTail(t *testing.T) {
	p, b, r := reductionFixture(t)
	prior := NewTransportHighWater(b.RuntimeBinding())
	prior.Observe(domain.TransportTrafficStats{StartedAt: r.StartedAt, ReadAt: r.ReadAt, BytesSent: 1, BytesReceived: 2}, nil)
	if _, err := p.FinalAccounting(t.Context(), b, prior); !errors.Is(err, ErrFinalAccountingUnavailable) {
		t.Fatal(err)
	}
	finishReductionFixture(t, p, r)
	before, err := p.ExitResult()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.FinalAccounting(t.Context(), b, prior)
	if err != nil || got.Finality != FinalExact || got.Traffic.BytesSent != r.BytesSent || got.Traffic.BytesReceived != r.BytesReceived {
		t.Fatal("exact postjoin tail missing", got, err)
	}
	got.Traffic.BytesSent = 999
	again, err := p.FinalAccounting(t.Context(), b, prior)
	if err != nil || again.Traffic.BytesSent != r.BytesSent {
		t.Fatal("mutable final projection", again, err)
	}
	after, err := p.ExitResult()
	if err != nil || before != after {
		t.Fatal("artifact reduction mutated sealed wait outcome", after, err)
	}
	if p.artifacts.root != nil {
		t.Fatal("reduced capability retained")
	}
	invalid := prior
	invalid.Observe(domain.TransportTrafficStats{}, errors.New("lost sample"))
	if _, err := p.FinalAccounting(t.Context(), b, invalid); err == nil {
		t.Fatal("cached exact result bypassed invalid prior")
	}
}
func TestFinalReductionCancelDoesNotBlockDoneOrDiscardCapability(t *testing.T) {
	p, b, r := reductionFixture(t)
	prior := NewTransportHighWater(b.RuntimeBinding())
	finishReductionFixture(t, p, r)
	if err := p.reduction.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	pending := make(chan error, 1)
	go func() { _, err := p.FinalAccounting(ctx, b, prior); pending <- err }()
	cancel()
	select {
	case err := <-pending:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reduction gate ignored context")
	}
	if err := p.Wait(t.Context()); err != nil {
		t.Fatal("wait owner depended on reducer", err)
	}
	if p.artifacts.root == nil || p.artifacts.pin == nil {
		t.Fatal("canceled reduction discarded root or inode pin")
	}
	if _, err := p.artifacts.pin.Stat(); err != nil {
		t.Fatal("canceled reduction closed original inode", err)
	}
	p.reduction.release()
	got, err := p.FinalAccounting(t.Context(), b, prior)
	if err != nil || got.Finality != FinalExact {
		t.Fatal("retry failed", got, err)
	}
}
func TestFinalReductionRejectsUncleanExitAndMalformedSlot(t *testing.T) {
	for _, kind := range []string{"forced", "unconfirmed", "log-close", "wrong-binding", "missing", "regressing", "credential"} {
		t.Run(kind, func(t *testing.T) {
			p, b, r := reductionFixture(t)
			prior := NewTransportHighWater(b.RuntimeBinding())
			if kind == "wrong-binding" {
				r.Nonce[0]++
			}
			if kind == "regressing" {
				prior.Observe(domain.TransportTrafficStats{StartedAt: r.StartedAt, ReadAt: r.ReadAt, BytesSent: r.BytesSent + 1}, nil)
			}
			finishReductionFixture(t, p, r)
			switch kind {
			case "forced":
				p.exit.Signals.ForceAttempted = true
			case "unconfirmed":
				p.exit.Signals.StopUnconfirmed = true
			case "log-close":
				p.exit.LogCloseFailed = true
			case "missing":
				if err := p.artifacts.root.Remove(p.artifacts.name); err != nil {
					t.Fatal(err)
				}
			case "credential":
				raw := []byte(p.rpc.credentials.passwordText())
				if err := os.WriteFile(p.artifacts.root.Name()+"/"+p.artifacts.name, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, _ := p.FinalAccounting(t.Context(), b, prior)
			if got.Finality == FinalExact {
				t.Fatal("invalid tail accepted", got)
			}
			if p.artifacts.root != nil {
				t.Fatal("terminal validation did not release root")
			}
		})
	}
}
func TestCompletedStartCannotBeInventedAfterExit(t *testing.T) {
	p, b, r := reductionFixture(t)
	finishReductionFixture(t, p, r)
	if _, err := p.CompletedStart(b.Receipt()); err == nil {
		t.Fatal("completed binding invented after Done")
	}
	if err := p.ReleaseArtifacts(t.Context()); err != nil {
		t.Fatal(err)
	}
	final, err := p.FinalAccounting(t.Context(), b, NewTransportHighWater(b.RuntimeBinding()))
	if !errors.Is(err, ErrFinalAccountingUnavailable) || final.Reason != FinalArtifactsReleased {
		t.Fatal("explicit cleanup reported a fictional close failure", final, err)
	}
}

func TestUnconfirmedOwnerRetainsRootAndOriginalInodePin(t *testing.T) {
	p, _, r := reductionFixture(t)
	pin := p.artifacts.pin
	if err := p.ReleaseArtifacts(t.Context()); !errors.Is(err, ErrStopUnconfirmed) {
		t.Fatal(err)
	}
	if p.artifacts.root == nil || p.artifacts.pin != pin {
		t.Fatal("unconfirmed owner discarded authority")
	}
	if _, err := pin.Stat(); err != nil {
		t.Fatal("unconfirmed owner closed inode pin", err)
	}
	finishReductionFixture(t, p, r)
	if err := p.ReleaseArtifacts(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := pin.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("confirmed explicit release leaked pin", err)
	}
}
