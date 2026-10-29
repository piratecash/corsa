//go:build linux

package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
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
	workDone  <-chan struct{}
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
	fixture.workDone = done
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
			sample := LinuxProcessSample{Provenance: facts.Provenance, ReadStartedAt: at, ReadCompletedAt: at, ReadElapsedNS: int64(index+1) * 11, ReadElapsedKnown: true,
				ExecutableReadBrackets: ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: int64(index*2 + 1), ElapsedKnown: true, VerifiedBytes: int64(index+1)*100 + 1}, Last: ProcessExecutableReadBracket{ElapsedNS: int64(index*2 + 2), ElapsedKnown: true, VerifiedBytes: int64(index+1)*100 + 2}}}
			if failure == "elapsed-"+point+"-unknown" {
				sample.ReadElapsedKnown = false
			}
			if failure == "elapsed-"+point+"-negative" {
				sample.ReadElapsedNS = -1
			}
			code := linuxResultReady
			if failure == "stream-after-unavailable-second" && index == 3 {
				code = linuxResultUnavailable
				sample = LinuxProcessSample{}
			}
			packet, err := encodeLinuxPacket(linuxSampleResult, request.id, 1, linuxSampleReply{Nonce: owner.correlation.nonce, Code: code, Sample: sample})
			if err != nil {
				t.Error(err)
				return
			}
			if err = work.write(packet, time.Now().Add(2*time.Second)); err != nil {
				t.Error(err)
				return
			}
			if code == linuxResultUnavailable {
				// This scripted reply makes the original work owner poison and
				// close its endpoint. There is no fifth sample request: waiting
				// for it races the expected EOF against test cleanup.
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

// This failure is injected only by the test HTTP response's Close, after the
// original reply is read. It is not a process observation or a native fault.
type summaryHiddenCloseFailure struct{}

func (*summaryHiddenCloseFailure) Error() string { return "hidden-test-close-failure" }

type summaryHiddenCloseBody struct {
	io.ReadCloser
	hidden *summaryHiddenCloseFailure
}

func (body summaryHiddenCloseBody) Close() error {
	return errors.Join(body.ReadCloser.Close(), body.hidden)
}

type summaryHiddenCloseTransport struct {
	next   http.RoundTripper
	hidden *summaryHiddenCloseFailure
}

func (transport summaryHiddenCloseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.next.RoundTrip(request)
	if err == nil {
		response.Body = summaryHiddenCloseBody{ReadCloser: response.Body, hidden: transport.hidden}
	}
	return response, err
}

func TestLinuxMemberSampleFailureLabelsPreserveOriginalCause(t *testing.T) {
	wantTrace := []string{"before", "fetchRouteSummary", "getResourceBreakdown", "getDatagramRuntimeState", "listBanned", "getRuntimeMetrics", "after"}
	t.Run("success", func(t *testing.T) {
		fixture := summaryMemberFixture(t, "")
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		sample, err := fixture.member.Sample(ctx)
		trace, calls := fixture.snapshot()
		if err != nil || !sample.FullSampleAvailable() || calls != 1 || !reflect.DeepEqual(trace, wantTrace) || fixture.member.highWater.invalid || !fixture.member.highWater.seen {
			t.Fatal("labels changed successful sample/custody", err, trace)
		}
	})
	t.Run("initial-running", func(t *testing.T) {
		fixture := summaryMemberFixture(t, "")
		fixture.member.fixture.mu.Lock()
		fixture.member.fixture.targetKnown = true
		fixture.member.fixture.mu.Unlock()
		sample, err := fixture.member.Sample(t.Context())
		failure, ok := err.(*linuxSampleFailure)
		trace, calls := fixture.snapshot()
		if !ok || failure.firstObservedFailedStage != linuxSampleInitialRunning || failure.returnSite != linuxSampleInitialRunning || failure.firstObservedFailureClass != linuxSampleMemberState || failure.Unwrap() != ErrMemberState || !errors.Is(err, ErrMemberState) || sample.ReadAt != (time.Time{}) || len(trace) != 0 || calls != 0 {
			t.Fatal("initial refusal changed or performed reads", err, trace)
		}
	})
	t.Run("final-running", func(t *testing.T) {
		fixture := summaryMemberFixture(t, "final-running")
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		sample, err := fixture.member.Sample(ctx)
		failure, ok := err.(*linuxSampleFailure)
		trace, calls := fixture.snapshot()
		if !ok || failure.firstObservedFailedStage != linuxSampleFinalRunning || failure.returnSite != linuxSampleFinalRunning || failure.firstObservedFailureClass != linuxSampleMemberState || failure.Unwrap() != ErrMemberState || !sample.FullSampleAvailable() || calls != 1 || !reflect.DeepEqual(trace, wantTrace) {
			t.Fatal("final refusal changed original observations", err, trace)
		}
	})
	t.Run("family-full-sample", func(t *testing.T) {
		fixture := summaryMemberFixture(t, "unavailable")
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		sample, err := fixture.member.Sample(ctx)
		failure, ok := err.(*linuxSampleFailure)
		trace, calls := fixture.snapshot()
		if !ok || failure.firstObservedFailedStage != linuxSampleRouteSummary || failure.returnSite != linuxSampleFullSample || failure.firstObservedFailureClass != linuxSampleFamilyUnavailable || !errors.Is(err, ErrFamilyUnavailable) || !errors.Is(err, ErrMemberState) || sample.FullSampleAvailable() || sample.LinuxProcess == nil || !sample.LinuxProcess.Available() || calls != 1 || !reflect.DeepEqual(trace, wantTrace) {
			t.Fatal("original full-sample join or observation order changed", err, trace)
		}
		// Original joined causes retain their order; no earlier cause is added.
		joined, ok := failure.Unwrap().(interface{ Unwrap() []error })
		if !ok {
			t.Fatal("original joined return lost", err)
		}
		causes := joined.Unwrap()
		if len(causes) != 2 || causes[0] != ErrMemberState || causes[1] != ErrFamilyUnavailable {
			t.Fatal("original cause order changed", causes)
		}
	})
	for _, reason := range []string{"cancel-after-summary", "family-before-final-cancel", "malformed-before-final-cancel", "typed-before-final-cancel"} {
		t.Run(reason, func(t *testing.T) {
			fixtureReason := ""
			if reason == "family-before-final-cancel" {
				fixtureReason = "unauthorized"
			}
			if reason == "malformed-before-final-cancel" {
				fixtureReason = "malformed-frame"
			}
			fixture := summaryMemberFixture(t, fixtureReason)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			client := fixture.member.fixture.RPC()
			next := client.http.Transport
			hidden := &summaryHiddenCloseFailure{}
			if reason == "typed-before-final-cancel" {
				next = summaryHiddenCloseTransport{next: next, hidden: hidden}
			}
			client.http.Transport = summaryCancelTransport{next: next, cancel: cancel}
			sample, err := fixture.member.Sample(ctx)
			failure, ok := err.(*linuxSampleFailure)
			trace, calls := fixture.snapshot()
			wantFirst := linuxSampleRouteSummary
			if reason == "cancel-after-summary" {
				wantFirst = linuxSampleTraffic
			}
			wantClass := linuxSampleContextCancelled
			if reason == "family-before-final-cancel" {
				wantClass = linuxSampleRPCAccess
			}
			if reason == "malformed-before-final-cancel" {
				wantClass = linuxSampleInvalidSample
			}
			if reason == "typed-before-final-cancel" {
				wantClass = linuxSampleOtherFailure
			}
			if !ok || failure.firstObservedFailedStage != wantFirst || failure.firstObservedFailureClass != wantClass || failure.returnSite != linuxSampleFinalRunning || failure.Unwrap() != context.Canceled || !errors.Is(err, context.Canceled) || errors.Is(err, ErrRPCAccess) || errors.Is(err, ErrInvalidSample) || errors.Is(err, hidden) || sample.FullSampleAvailable() || sample.ReadAt.IsZero() || calls != 1 || !reflect.DeepEqual(trace, []string{"before", "fetchRouteSummary"}) || !fixture.member.highWater.invalid {
				t.Fatal("hidden earlier failure changed original final cancellation", err, trace)
			}
			var hiddenCause *summaryHiddenCloseFailure
			if errors.As(err, &hiddenCause) || strings.Contains(err.Error(), hidden.Error()) {
				t.Fatal("hidden cause leaked into returned tree or text", err)
			}
			prefix := "linux-member-sample first_observed_failed_stage=" + string(wantFirst) + " first_observed_failure_class=" + string(wantClass) + " return_site=final_running: "
			if !strings.HasPrefix(err.Error(), prefix) || len(prefix) > 256 {
				t.Fatal("unbounded or inaccurate fixed diagnostic prefix", err)
			}
		})
	}
	t.Run("original-typed-cause", func(t *testing.T) {
		original := &summaryHiddenCloseFailure{}
		first := linuxSampleFirstFailure{stage: linuxSampleBeforeOS, class: linuxSampleOtherFailure}
		err := first.at(linuxSampleFullSample, errors.Join(ErrMemberState, original))
		var retained *summaryHiddenCloseFailure
		if classifyLinuxSampleFailure(errors.Join(ErrMemberState, context.Canceled)) != linuxSampleContextCancelled || classifyLinuxSampleFailure(errors.Join(context.Canceled, ErrMemberState)) != linuxSampleContextCancelled {
			t.Fatal("fixed class precedence changed")
		}
		if !errors.Is(err, original) || !errors.As(err, &retained) || retained != original || !errors.Is(err, ErrMemberState) {
			t.Fatal("original typed/sentinel traversal changed", err)
		}
	})
}

func TestLinuxMemberRetainsBothReadBracketScalars(t *testing.T) {
	for _, reason := range []string{"", "elapsed-before-unknown", "elapsed-after-unknown", "elapsed-before-negative", "elapsed-after-negative"} {
		name := reason
		if name == "" {
			name = "both"
		}
		t.Run(name, func(t *testing.T) {
			fixture := summaryMemberFixture(t, reason)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			s, err := fixture.member.Sample(ctx)
			trace, calls := fixture.snapshot()
			want := []string{"before", "fetchRouteSummary", "getResourceBreakdown", "getDatagramRuntimeState", "listBanned", "getRuntimeMetrics", "after"}
			if err != nil || !s.FullSampleAvailable() || calls != 1 || !reflect.DeepEqual(trace, want) || fixture.member.highWater.invalid || !fixture.member.highWater.seen {
				t.Fatal("diagnostic changed original sample", err, trace)
			}
			beforeKnown := reason != "elapsed-before-unknown" && reason != "elapsed-before-negative"
			if s.LinuxBeforeElapsedKnown != beforeKnown || beforeKnown && s.LinuxBeforeElapsedNS != 11 || !beforeKnown && s.LinuxBeforeElapsedNS != 0 {
				t.Fatal("BEFORE scalar lost or fabricated", s)
			}
			expectedBefore := ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: 1, ElapsedKnown: true, VerifiedBytes: 101}, Last: ProcessExecutableReadBracket{ElapsedNS: 2, ElapsedKnown: true, VerifiedBytes: 102}}
			expectedAfter := ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: 3, ElapsedKnown: true, VerifiedBytes: 201}, Last: ProcessExecutableReadBracket{ElapsedNS: 4, ElapsedKnown: true, VerifiedBytes: 202}}
			if s.LinuxBeforeExecutableReadBrackets != expectedBefore || s.LinuxProcess.Value.ExecutableReadBrackets != expectedAfter {
				t.Fatal("fresh before/after stream pair lost", s)
			}
			after := s.LinuxProcess.Value
			if after.ReadElapsedKnown != (reason != "elapsed-after-unknown") || reason == "elapsed-after-negative" && after.ReadElapsedNS != -1 || reason != "elapsed-after-negative" && after.ReadElapsedNS != 22 {
				t.Fatal("AFTER original reply changed", after)
			}
			if reason == "" {
				firstJSON, err := json.Marshal(s)
				if err != nil {
					t.Fatal(err)
				}
				secondCtx, secondCancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer secondCancel()
				second, err := fixture.member.Sample(secondCtx)
				trace, calls = fixture.snapshot()
				if err != nil || !second.FullSampleAvailable() || !second.LinuxBeforeElapsedKnown || second.LinuxBeforeElapsedNS != 33 || second.LinuxProcess == nil || !second.LinuxProcess.Available() || !second.LinuxProcess.Value.ReadElapsedKnown || second.LinuxProcess.Value.ReadElapsedNS != 44 || calls != 2 || !reflect.DeepEqual(trace, append(append([]string(nil), want...), want...)) {
					t.Fatal("second call reused first diagnostic", err, trace, second)
				}
				if second.LinuxBeforeExecutableReadBrackets != (ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: 5, ElapsedKnown: true, VerifiedBytes: 301}, Last: ProcessExecutableReadBracket{ElapsedNS: 6, ElapsedKnown: true, VerifiedBytes: 302}}) || second.LinuxProcess.Value.ExecutableReadBrackets != (ProcessExecutableReadBrackets{First: ProcessExecutableReadBracket{ElapsedNS: 7, ElapsedKnown: true, VerifiedBytes: 401}, Last: ProcessExecutableReadBracket{ElapsedNS: 8, ElapsedKnown: true, VerifiedBytes: 402}}) {
					t.Fatal("stream pairs reused from prior call", second)
				}
				other := summaryMemberFixture(t, "")
				otherCtx, otherCancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer otherCancel()
				fresh, err := other.member.Sample(otherCtx)
				otherTrace, otherCalls := other.snapshot()
				if err != nil || !fresh.FullSampleAvailable() || !fresh.LinuxBeforeElapsedKnown || fresh.LinuxBeforeElapsedNS != 11 || fresh.LinuxProcess == nil || !fresh.LinuxProcess.Available() || !fresh.LinuxProcess.Value.ReadElapsedKnown || fresh.LinuxProcess.Value.ReadElapsedNS != 22 || otherCalls != 1 || !reflect.DeepEqual(otherTrace, want) {
					t.Fatal("diagnostics crossed owner", err, otherTrace, fresh)
				}
				if fresh.LinuxBeforeExecutableReadBrackets != expectedBefore || fresh.LinuxProcess.Value.ExecutableReadBrackets != expectedAfter {
					t.Fatal("stream pairs crossed owners", fresh)
				}
				retainedJSON, err := json.Marshal(s)
				if err != nil || string(retainedJSON) != string(firstJSON) {
					t.Fatal("prior sample snapshot mutated", err)
				}
			}
		})
	}
	t.Run("unavailable-after-success", func(t *testing.T) {
		fixture := summaryMemberFixture(t, "stream-after-unavailable-second")
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		first, err := fixture.member.Sample(ctx)
		if err != nil || !first.FullSampleAvailable() {
			t.Fatal(err)
		}
		frozen, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		nextCtx, nextCancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer nextCancel()
		next, err := fixture.member.Sample(nextCtx)
		select {
		case <-fixture.workDone:
		case <-nextCtx.Done():
			t.Fatal("scripted unavailable reply owner did not join", nextCtx.Err())
		}
		owner := fixture.member.fixture
		owner.mu.Lock()
		stopping, poisoned := owner.stopping, owner.correlation.poisoned
		owner.mu.Unlock()
		if !stopping || !poisoned {
			t.Fatal("unavailable reply did not stop and poison the original owner")
		}
		failure, ok := err.(*linuxSampleFailure)
		trace, calls := fixture.snapshot()
		want := []string{"before", "fetchRouteSummary", "getResourceBreakdown", "getDatagramRuntimeState", "listBanned", "getRuntimeMetrics", "after"}
		if !ok || failure.firstObservedFailedStage != linuxSampleAfterOS || failure.returnSite != linuxSampleFinalRunning || failure.Unwrap() != ErrMemberState || errors.Is(err, ErrLinuxOwnerProtocol) || next.FullSampleAvailable() || next.LinuxProcess == nil || next.LinuxProcess.Available() || next.LinuxProcess.Value != nil || next.LinuxBeforeExecutableReadBrackets.First.ElapsedNS != 5 || next.LinuxBeforeExecutableReadBrackets.Last.ElapsedNS != 6 || !next.Traffic.Available() || calls != 2 || !reflect.DeepEqual(trace, append(append([]string(nil), want...), want...)) {
			t.Fatal("failed AFTER reused pair or altered original cause", next, err, trace)
		}
		retained, err := json.Marshal(first)
		if err != nil || string(retained) != string(frozen) {
			t.Fatal("failed later call mutated first stream pair", err)
		}
	})

}
