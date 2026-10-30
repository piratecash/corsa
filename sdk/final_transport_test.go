package sdk

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestFinalTransportIsUnavailableUntilRealRuntimeJoins(t *testing.T) {
	r, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.FinalTransport(); !errors.Is(err, ErrFinalTransportUnavailable) {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := r.nodeService.TransportTrafficStats()
	if _, err := r.FinalTransport(); !errors.Is(err, ErrFinalTransportUnavailable) {
		t.Fatal("running runtime manufactured finality", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(); err != nil {
		t.Fatal(err)
	}
	final, err := r.FinalTransport()
	if err != nil {
		t.Fatal("joined real runtime has no final transport", err)
	}
	if final.Boundary != TransportQuiescent || !final.StartedAt.Equal(before.StartedAt) || final.ReadAt.Before(before.ReadAt) || final.BytesSent < before.BytesSent || final.BytesReceived < before.BytesReceived {
		t.Fatal("invalid final boundary", final)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := r.FinalTransport()
			if err != nil || got != final {
				t.Error("final latch changed", got, err)
			}
		}()
	}
	wg.Wait()
	final.BytesSent++
	got, err := r.FinalTransport()
	if err != nil || got.BytesSent == final.BytesSent {
		t.Fatal("returned observation aliases latch")
	}
}

func TestFinalTransportDoesNotInventNeverStartedIncarnation(t *testing.T) {
	r, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.FinalTransport(); !errors.Is(err, ErrFinalTransportUnavailable) {
		t.Fatal("never-started close became measured zero", err)
	}
}

func TestFinalTransportRemainsSeparateFromDatabaseCloseFailure(t *testing.T) {
	r, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.database.Close() })
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("controlled database-close failure")
	closer := NewMockruntimeDatabaseCloser(t)
	closer.EXPECT().Close().RunAndReturn(func() error { return errors.Join(r.database.Close(), failure) }).Once()
	// The runtime hasn't begun Close. This is a private instance dependency,
	// replacing only the actual close call; every real lifecycle owner still joins.
	r.lifecycle.Lock()
	r.databaseCloser = closer
	r.lifecycle.Unlock()
	if err := r.Close(); !errors.Is(err, failure) {
		t.Fatal("database failure hidden", err)
	}
	if err := r.Wait(); !errors.Is(err, failure) {
		t.Fatal("Wait rewrote shutdown error", err)
	}
	final, err := r.FinalTransport()
	if err != nil || final.Boundary != TransportQuiescent {
		t.Fatal("joined transport lost because later DB close failed", final, err)
	}
	if err := r.Close(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	again, err := r.FinalTransport()
	if err != nil || again != final {
		t.Fatal("retry changed fact", again, err)
	}
}

func TestFinalTransportIncompleteCloseStaysUnavailableUntilActualRetryJoins(t *testing.T) {
	r, err := New(testRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !r.beginOperation() {
		t.Fatal("lease not admitted")
	}
	held := true
	defer func() {
		if held {
			r.endOperation()
		}
		_ = r.Close()
	}()
	if err := r.Close(); err == nil {
		t.Fatal("held real operation did not prevent clean close")
	}
	if _, err := r.FinalTransport(); !errors.Is(err, ErrFinalTransportUnavailable) {
		t.Fatal("incomplete join produced final zero", err)
	}
	firstWait := r.Wait()
	if firstWait == nil {
		t.Fatal("Wait lost incomplete shutdown")
	}
	r.endOperation()
	held = false
	if err := r.Close(); err != nil {
		t.Fatal("retry failed", err)
	}
	final, err := r.FinalTransport()
	if err != nil || final.Boundary != TransportQuiescent {
		t.Fatal("retry did not latch joined runtime", final, err)
	}
	if again := r.Wait(); again == nil || again.Error() != firstWait.Error() {
		t.Fatal("retry rewrote immutable Wait outcome", again)
	}
}
func TestFinalTransportFailedBeforeNodeStartIsUnavailable(t *testing.T) {
	cfg := testRuntimeConfig(t)
	cfg.RPC.Enabled = true
	cfg.RPC.Username = "operator"
	cfg.RPC.Password = "fixture-rpc-password"
	cfg.RPC.Host = "256.256.256.256"
	cfg.RPC.Port = "1"
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if err := r.Start(t.Context()); err == nil {
		t.Fatal("controlled RPC failure did not fail start")
	}
	if _, err := r.FinalTransport(); !errors.Is(err, ErrFinalTransportUnavailable) {
		t.Fatal("failed pre-node start created a period", err)
	}
}
