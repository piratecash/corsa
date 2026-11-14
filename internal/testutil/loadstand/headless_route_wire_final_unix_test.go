//go:build (darwin && cgo) || linux

package loadstand

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func routeWireFDReport(t *testing.T) (HeadlessRouteWireReport, RouteWireExpectation) {
	t.Helper()
	report, _, expected := routeWireBoundedFixture(t, RouteWireWindowBounds{Begin: 10 * time.Second, End: 20 * time.Second})
	for len(report.Endpoints) < 16 {
		row := report.Endpoints[0]
		row.ConnID = domain.ConnID(len(report.Endpoints) + 1)
		row.SessionID = LaunchFingerprint{byte(row.ConnID)}
		row.Peer = domain.PeerIdentity{byte(row.ConnID) + 7}
		report.Endpoints = append(report.Endpoints, row)
		expected.Endpoints = append(expected.Endpoints, row.RouteWireEndpointKey)
	}
	report.Transport.BytesSent = 20_000
	return report, expected
}

func publishRouteWireFixture(t *testing.T, slot *ChildFinalSlot, report HeadlessRouteWireReport, secrets [][]byte) error {
	t.Helper()
	credentials, err := NewCredentials()
	if err != nil {
		t.Fatal(err)
	}
	transport := report.Transport
	stats := domain.TransportTrafficStats{StartedAt: transport.StartedAt, ReadAt: transport.ReadAt, BytesSent: transport.BytesSent, BytesReceived: transport.BytesReceived}
	return slot.PublishHeadlessRouteWireAndScan(t.Context(), transport.Identity, stats, report.Window, report.Bounds, report.Endpoints, secrets, credentials)
}

func TestHeadlessRouteWireFDWorker(t *testing.T) {
	if os.Getenv("LOADSTAND_ROUTE_WIRE_TEST_CHILD") != "1" {
		return
	}
	slot, err := ReadChildFinalSlot(os.Getenv)
	if err != nil || slot == nil {
		t.Fatal("read inherited writer", err)
	}
	defer func() {
		if err := slot.Close(); err != nil {
			t.Error(err)
		}
	}()
	report, _ := routeWireFDReport(t)
	if err := publishRouteWireFixture(t, slot, report, nil); err != nil {
		t.Fatal(err)
	}
}

func TestHeadlessRouteWireFDParentJoinsAndSeals(t *testing.T) {
	p, binding, _ := reductionFixture(t)
	p.finalRole = MemberFull
	report, expected := routeWireFDReport(t)
	raw, err := report.Encode()
	if err != nil || len(raw) <= int(FinalReportLimit) {
		t.Fatalf("fixture must exceed ordinary report limit: %d, %v", len(raw), err)
	}
	prior := NewTransportHighWater(binding.RuntimeBinding())
	if _, err := p.FinalRouteWireAccounting(t.Context(), binding, prior); !errors.Is(err, ErrFinalAccountingUnavailable) {
		t.Fatalf("live launch was reduced: %v", err)
	}
	if err := p.BindRouteWireWindow(t.Context(), binding, expected); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(expected.Endpoints)
	if err := p.BindRouteWireWindow(t.Context(), binding, expected); err != nil {
		t.Fatal("same census was not idempotent", err)
	}
	expected.Endpoints[0].Peer[0]++
	if err := p.BindRouteWireWindow(t.Context(), binding, expected); !errors.Is(err, ErrFinalAccountingUnavailable) {
		t.Fatalf("rebound a different census: %v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestHeadlessRouteWireFDWorker$", "-test.timeout=8s")
	cmd.Env = append(os.Environ(), "LOADSTAND_ROUTE_WIRE_TEST_CHILD=1", "LOADSTAND_FINAL_FD=3",
		"LOADSTAND_FINAL_NONCE="+binding.launch.nonce.String(), "LOADSTAND_FINAL_PLAN="+binding.launch.fingerprint.String())
	cmd.ExtraFiles = []*os.File{p.artifacts.writer}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child writer failed: %v\n%s", err, out)
	}
	if err := p.artifacts.writer.Close(); err != nil {
		t.Fatal(err)
	}
	p.artifacts.writer = nil
	p.exit = ProcessExitResult{Kind: WaitNormal}
	close(p.done)

	if _, err := p.FinalAccounting(t.Context(), binding, prior); !errors.Is(err, ErrFinalAccountingUnavailable) {
		t.Fatalf("ordinary reducer consumed a route-bound artifact: %v", err)
	}
	if _, err := p.FinalRouteWireAccounting(t.Context(), binding, TransportHighWater{}); !errors.Is(err, ErrFinalReportInvalid) {
		t.Fatalf("invalid prior was accepted: %v", err)
	}
	stale := binding
	stale.receipt.Incarnation.Ordinal++
	if _, err := p.FinalRouteWireAccounting(t.Context(), stale, prior); !errors.Is(err, ErrFinalAccountingUnavailable) {
		t.Fatalf("stale binding consumed the artifact: %v", err)
	}
	if p.artifacts.root == nil {
		t.Fatal("rejected caller closed launch capability")
	}
	got, err := p.FinalRouteWireAccounting(t.Context(), binding, prior)
	if err != nil || got.Finality != FinalExact || !slices.Equal(got.Endpoints, report.Endpoints) {
		t.Fatalf("missing exact endpoints: %+v, %v", got, err)
	}
	census, err := commitRouteWireWindow(RouteWireExpectation{Window: report.Window, Bounds: report.Bounds, Endpoints: routeWireKeys(report.Endpoints)}, binding.RuntimeBinding().Identity)
	if err != nil || !got.Seal.matches(raw, census) {
		t.Fatalf("final seal is not bound to original FD bytes: %v", err)
	}
	got.Endpoints[0].ConnID++
	cached, err := p.FinalRouteWireAccounting(t.Context(), binding, prior)
	if err != nil || !slices.Equal(cached.Endpoints, report.Endpoints) || cached.Seal != got.Seal || p.artifacts.root != nil {
		t.Fatalf("mutable result or retained capability: %+v, %v", cached, err)
	}
	t.Logf("inherited FD report_bytes=%d endpoints=%d", len(raw), len(cached.Endpoints))
}

func routeWireKeys(rows []RouteWireEndpoint) []RouteWireEndpointKey {
	keys := make([]RouteWireEndpointKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.RouteWireEndpointKey)
	}
	return keys
}

func TestHeadlessRouteWireFDRejectsUncleanOrMismatchedArtifact(t *testing.T) {
	for _, mode := range []string{"wrong window", "forced exit", "secret"} {
		t.Run(mode, func(t *testing.T) {
			p, binding, _ := reductionFixture(t)
			p.finalRole = MemberFull
			report, _, expected := routeWireFixture(t)
			if err := p.BindRouteWireWindow(t.Context(), binding, expected); err != nil {
				t.Fatal(err)
			}
			if mode == "wrong window" {
				report.Window[0]++
			}
			scan, err := duplicateFinalFD(p.artifacts.writer)
			if err != nil {
				t.Fatal(err)
			}
			slot := &ChildFinalSlot{writer: p.artifacts.writer, scan: scan, binding: binding.launch}
			p.artifacts.writer = nil
			defer func() {
				if err := slot.Close(); err != nil {
					t.Error(err)
				}
			}()
			var secrets [][]byte
			if mode == "secret" {
				secrets = [][]byte{[]byte(report.Endpoints[0].Sent.FullV3.SHA256)}
			}
			err = publishRouteWireFixture(t, slot, report, secrets)
			if mode == "secret" {
				if !errors.Is(err, ErrSecretLeak) {
					t.Fatalf("same-inode scan accepted the secret: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			p.exit = ProcessExitResult{Kind: WaitNormal}
			if mode == "forced exit" {
				p.exit = ProcessExitResult{Kind: WaitSignaled, Signals: ProcessSignalFacts{ForceAttempted: true}}
			}
			close(p.done)
			got, err := p.FinalRouteWireAccounting(t.Context(), binding, NewTransportHighWater(binding.RuntimeBinding()))
			if got.Finality == FinalExact || len(got.Endpoints) != 0 || (mode == "wrong window" && !errors.Is(err, ErrFinalReportInvalid)) {
				t.Fatalf("invalid terminal result: %+v, %v", got, err)
			}
		})
	}
}
