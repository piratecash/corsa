//go:build linux

package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Uses the original work-owner packet fixture and actual linuxMember.Sample.
// Wire rows exercise composition/custody; they are not Linux OS observations
// and do not grant native-process, namespace or timing acceptance.
type summaryMemberTest struct {
	member    *linuxMember
	mu        sync.Mutex
	trace     []string
	summaries uint64
}

func (f *summaryMemberTest) record(point string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trace = append(f.trace, point)
}
func (f *summaryMemberTest) snapshot() ([]string, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.trace...), f.summaries
}
func summaryMemberFixture(t *testing.T, failure string) *summaryMemberTest {
	t.Helper()
	owner, work, facts := linuxRuntimeTestWire(t)
	owner.mu.Lock()
	owner.helperState = LinuxHelperRunning
	owner.mu.Unlock()
	receipt := &StartReceipt{Public: facts.Public, Incarnation: Incarnation{Ordinal: 1, StartedAt: facts.StartedAt}}
	member := &linuxMember{id: "summary-member", fixture: owner, current: receipt, group: &LinuxMemberGroup{network: &plannedNetwork{ready: true}}}
	member.highWater = NewTransportHighWater(RuntimeBinding{Identity: receipt.Public.Identity, StartedAt: receipt.Incarnation.StartedAt})
	fixture := &summaryMemberTest{member: member}
	source := summaryTestSource(t, func(command string, w http.ResponseWriter) {
		fixture.record(command)
		at := time.Now().UTC()
		var payload any
		switch command {
		case "fetchRouteSummary":
			fixture.mu.Lock()
			fixture.summaries++
			count := fixture.summaries
			fixture.mu.Unlock()
			switch failure {
			case "unauthorized":
				w.WriteHeader(http.StatusUnauthorized)
				return
			case "unavailable":
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			case "malformed-frame":
				_, _ = io.WriteString(w, `{"transport_traffic":`)
				return
			}
			summary := summaryTestPayload(count, receipt.Incarnation.StartedAt, at)
			if failure == "one-family" {
				summary["mode_selection"] = nil
			}
			payload = summary
		case "getResourceBreakdown":
			payload = resourceFixture()
		case "getDatagramRuntimeState":
			identity := receipt.Public.Identity
			if failure == "datagram-binding" {
				identity = domain.PeerIdentity{9}
			}
			payload = domain.DatagramRuntimeState{Version: 1, Identity: identity, StartedAt: receipt.Incarnation.StartedAt, ReadAt: at, ConfiguredEnabled: false, State: domain.DatagramStateConfiguredOff}
		case "listBanned":
			payload = map[string]any{"banned_ips": []any{}, "count": 0, "version": 2, "ready": true, "fresh": true, "started_at": receipt.Incarnation.StartedAt, "observation_started_at": at, "generated_at": at, "local_bans": []any{}, "local_ip_bans": []any{}, "peer_bans": []any{}, "remote_peer_bans": []any{}, "remote_ip_bans": []any{}, "loopback_identities_scored": 0, "loopback_identities_banned": 0, "loopback_identity_penalties_available": true}
		case "getRuntimeMetrics":
			count := uint64(1)
			cpu := 0.0
			started := receipt.Incarnation.StartedAt
			if failure == "runtime-binding" {
				started = started.Add(time.Second)
			}
			payload = domain.RuntimeMetrics{Version: 1, StartedAt: started, ReadAt: at, HeapLiveBytes: &count, HeapObjectsBytes: &count, HeapAllocsBytes: &count, GoMemoryBytes: &count, GCCycles: &count, Goroutines: &count, GOMAXPROCS: &count, CPUTotalSeconds: &cpu, CPUUserSeconds: &cpu, CPUGCSeconds: &cpu, CPUIdleSeconds: &cpu}
		default:
			t.Error("unexpected or duplicate family request", command)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Error(err)
		}
	})
	owner.rpc = source.client
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for index := 0; ; index++ {
			request, err := work.read(time.Now().Add(2 * time.Second))
			if err != nil {
				select {
				case <-stop:
				default:
					t.Error("original sample work request", err)
				}
				return
			}
			if request.kind != linuxSample {
				t.Error("unexpected work operation", request.kind)
				return
			}
			body, err := decodeLinuxBody[linuxSampleRequest](request)
			if err != nil || body.Nonce != owner.correlation.nonce || body.BudgetNanos <= 0 {
				t.Error("changed original sample correlation", err)
				return
			}
			point := "before"
			if index%2 != 0 {
				point = "after"
			}
			fixture.record(point)
			if point == "after" && failure == "final-running" {
				owner.mu.Lock()
				owner.targetKnown = true
				owner.mu.Unlock()
			}
			at := time.Now().UTC()
			sample := LinuxProcessSample{Provenance: facts.Provenance, ReadStartedAt: at, ReadCompletedAt: at}
			packet, err := encodeLinuxPacket(linuxSampleResult, request.id, 1, linuxSampleReply{Nonce: owner.correlation.nonce, Code: linuxResultReady, Sample: sample})
			if err != nil {
				t.Error(err)
				return
			}
			if err = work.write(packet, time.Now().Add(2*time.Second)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		if err := work.close(); err != nil {
			t.Error(err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("test sample reply owner did not join")
		}
	})
	return fixture
}

func TestLinuxMemberSampleUsesOneFreshSummaryPerCallAndOwner(t *testing.T) {
	fixture := summaryMemberFixture(t, "")
	wantTrace := []string{"before", "fetchRouteSummary", "getResourceBreakdown", "getDatagramRuntimeState", "listBanned", "getRuntimeMetrics", "after"}
	for want := uint64(1); want <= 2; want++ {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		sample, err := fixture.member.Sample(ctx)
		cancel()
		if err != nil || !sample.FullSampleAvailable() || sample.Process != fixture.member.RuntimeProcess() || sample.Traffic.Value.BytesSent != want || sample.Sessions.Value.Attempts != want || len(sample.Modes.Value.Decisions) != 1 || sample.Modes.Value.Decisions[0].Count != want || sample.Neighbours.Value.Connections != int(want) || *sample.RouteEntries.Value != int(want) {
			t.Fatal("actual Sample reused an earlier reply or lost a family", err)
		}
		trace, calls := fixture.snapshot()
		if calls != want || len(trace) < len(wantTrace) || !reflect.DeepEqual(trace[len(trace)-len(wantTrace):], wantTrace) || fixture.member.highWater.invalid || !fixture.member.highWater.seen || fixture.member.highWater.sent != want {
			t.Fatal("changed bracket, RPC count/order or original traffic high-water", trace, calls)
		}
	}
	// A different lifecycle owner/identity cannot consume the first owner's data.
	other := summaryMemberFixture(t, "")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	sample, err := other.member.Sample(ctx)
	_, calls := other.snapshot()
	if err != nil || !sample.FullSampleAvailable() || calls != 1 || sample.Identity == fixture.member.current.Public.Identity || sample.Incarnation != other.member.current.Incarnation || sample.Traffic.Value.BytesSent != 1 || other.member.RuntimeProcess() == fixture.member.RuntimeProcess() {
		t.Fatal("summary crossed member/incarnation custody", err, calls)
	}
}

func TestLinuxMemberSummaryFailuresKeepOtherReadsAndAfterGuard(t *testing.T) {
	for _, reason := range []string{"unauthorized", "unavailable", "malformed-frame", "one-family", "runtime-binding", "datagram-binding", "final-running"} {
		t.Run(reason, func(t *testing.T) {
			fixture := summaryMemberFixture(t, reason)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			sample, err := fixture.member.Sample(ctx)
			trace, calls := fixture.snapshot()
			want := []string{"before", "fetchRouteSummary", "getResourceBreakdown", "getDatagramRuntimeState", "listBanned", "getRuntimeMetrics", "after"}
			if err == nil || calls != 1 || !reflect.DeepEqual(trace, want) || sample.LinuxProcess == nil || !sample.LinuxProcess.Available() || !sample.Resources.Available() || !sample.Bans.Available() || sample.ReadAt.IsZero() {
				t.Fatal("failure bypassed original observations/AFTER or retried summary", err, trace)
			}
			switch reason {
			case "unauthorized", "unavailable", "malformed-frame":
				cause := ErrRPCAccess
				if reason == "unavailable" {
					cause = ErrFamilyUnavailable
				}
				if reason == "malformed-frame" {
					cause = ErrInvalidSample
				}
				if !errors.Is(err, cause) || sample.Traffic.Available() || sample.Sessions.Available() || sample.Modes.Available() || sample.Neighbours.Available() || sample.RouteEntries.Available() || !fixture.member.highWater.invalid || !sample.Runtime.Available() {
					t.Fatal("shared original cause lost or failed snapshot fabricated", err)
				}
			case "one-family":
				if sample.Modes.Available() || !sample.Traffic.Available() || !sample.Sessions.Available() || !sample.Neighbours.Available() || !sample.RouteEntries.Available() || !sample.Runtime.Available() || sample.FullSampleAvailable() {
					t.Fatal("one malformed family suppressed valid reads or became full sample")
				}
			case "runtime-binding":
				if sample.Runtime.Available() || !sample.Traffic.Available() || sample.FullSampleAvailable() {
					t.Fatal("runtime binding bypassed")
				}
			case "datagram-binding":
				if sample.Datagram.State != ObservationUnavailable || !sample.Runtime.Available() || sample.FullSampleAvailable() {
					t.Fatal("datagram incarnation binding bypassed")
				}
			case "final-running":
				if !errors.Is(err, ErrMemberState) || !sample.FullSampleAvailable() {
					t.Fatal("final CheckRunning was skipped", err)
				}
			}
		})
	}
}

// Cancellation is delivered only when the actual summary response has been
// read and closed. The transport/server/auth path remains real and per-client.
type summaryCancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b summaryCancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

type summaryCancelTransport struct {
	next   http.RoundTripper
	cancel context.CancelFunc
}

func (tr summaryCancelTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := tr.next.RoundTrip(r)
	if err == nil {
		response.Body = summaryCancelBody{ReadCloser: response.Body, cancel: tr.cancel}
	}
	return response, err
}
func TestLinuxMemberSummaryCancellationCannotReuseFetchedPayload(t *testing.T) {
	fixture := summaryMemberFixture(t, "")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client := fixture.member.fixture.RPC()
	client.http.Transport = summaryCancelTransport{next: client.http.Transport, cancel: cancel}
	sample, err := fixture.member.Sample(ctx)
	trace, calls := fixture.snapshot()
	if !errors.Is(err, context.Canceled) || calls != 1 || !reflect.DeepEqual(trace, []string{"before", "fetchRouteSummary"}) || sample.Traffic.Available() || sample.Sessions.Available() || sample.Modes.Available() || sample.Neighbours.Available() || sample.RouteEntries.Available() || sample.Runtime.Available() || sample.LinuxProcess == nil || sample.LinuxProcess.Available() || sample.ReadAt.IsZero() || !fixture.member.highWater.invalid {
		t.Fatal("cancelled fetched summary admitted, retried, or bypassed AFTER/final cancellation", err, trace)
	}
}
