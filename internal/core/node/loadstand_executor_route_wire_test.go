package node

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

type loadStandRouteWireLife struct {
	Node        loadstand.MemberID
	Incarnation loadstand.Incarnation
}

type routeWireStopFixture struct {
	calls   []string
	stopErr error
	readErr error
	receipt loadstand.StopReceipt
	traffic domain.TransportTrafficStats
}

func (f *routeWireStopFixture) Stop(context.Context) (loadstand.StopReceipt, error) {
	f.calls = append(f.calls, "stop")
	return f.receipt, f.stopErr
}

func (f *routeWireStopFixture) Owned() bool { return !f.receipt.Terminated }

func (f *routeWireStopFixture) ReadCommittedRouteWireReport(_ context.Context, _ loadstand.MemberID, _ loadstand.IncarnationOrdinal, commitment loadstand.RouteWireCommitment) (loadstand.HeadlessRouteWireAccounting, error) {
	f.calls = append(f.calls, "read")
	return loadstand.HeadlessRouteWireAccounting{Traffic: f.traffic, Bounds: commitment.Bounds}, f.readErr
}

func TestLoadStandRouteWireStopOrder(t *testing.T) {
	life := loadStandRouteWireLife{Node: "full", Incarnation: loadstand.Incarnation{Ordinal: 2, StartedAt: time.Now().UTC()}}
	traffic := domain.TransportTrafficStats{StartedAt: life.Incarnation.StartedAt, ReadAt: life.Incarnation.StartedAt.Add(time.Second), BytesSent: 1}
	failure := errors.New("fixture refusal")
	for _, tc := range []struct {
		name   string
		change func(*routeWireStopFixture)
		calls  []string
		err    error
	}{
		{"original", func(*routeWireStopFixture) {}, []string{"stop", "read"}, nil},
		{"stop failure", func(f *routeWireStopFixture) { f.stopErr = failure }, []string{"stop"}, failure},
		{"foreign incarnation", func(f *routeWireStopFixture) { f.receipt.Ended.Incarnation.Ordinal++ }, []string{"stop"}, loadstand.ErrFinalAccountingUnavailable},
		{"unconfirmed stop", func(f *routeWireStopFixture) { f.receipt.Terminated = false }, []string{"stop"}, loadstand.ErrFinalAccountingUnavailable},
		{"read failure", func(f *routeWireStopFixture) { f.readErr = failure }, []string{"stop", "read"}, failure},
		{"different final traffic", func(f *routeWireStopFixture) { f.traffic.BytesSent++ }, []string{"stop", "read"}, loadstand.ErrFinalReportInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &routeWireStopFixture{traffic: traffic, receipt: loadstand.StopReceipt{Terminated: true, Ended: &loadstand.EndedIncarnation{Incarnation: life.Incarnation, Finality: loadstand.FinalExact, Final: loadstand.Observed(traffic, nil)}}}
			tc.change(fixture)
			commitment := loadstand.RouteWireCommitment{Bounds: loadstand.RouteWireWindowBounds{Begin: 100, End: 200}}
			receipt, report, err := stopLoadStandRouteWire(t.Context(), fixture, fixture, life, commitment)
			if !errors.Is(err, tc.err) || !slices.Equal(fixture.calls, tc.calls) || receipt.Ended != fixture.receipt.Ended {
				t.Fatalf("calls %v, receipt %+v, error %v", fixture.calls, receipt, err)
			}
			if tc.err == nil && (report.loadStandRouteWireLife != life || !loadStandSameTraffic(report.Accounting.Traffic, traffic)) {
				t.Fatalf("unexpected final report %+v", report)
			}
			if tc.err != nil && (report.Node != "" || !report.Accounting.Traffic.ReadAt.IsZero()) {
				t.Fatalf("unexpected evidence after refusal %+v", report)
			}
		})
	}
}

type routeWireCaptureFixture struct {
	routeWireStopFixture
	requests []loadstand.MemberID
	replies  map[loadstand.MemberID]loadstand.RouteWireCommitment
}

func (f *routeWireCaptureFixture) CaptureRouteWireCensus(_ context.Context, id loadstand.MemberID) (loadstand.RouteWireCommitment, error) {
	f.requests = append(f.requests, id)
	if reply, ok := f.replies[id]; ok {
		return reply, nil
	}
	return loadstand.RouteWireCommitment{}, loadstand.ErrFinalAccountingUnavailable
}

func TestLoadStandRouteWireCapturesAllOriginalLivesAfterClose(t *testing.T) {
	const first, second = "full-1", "full-2"
	bounds := loadstand.RouteWireWindowBounds{Begin: 100, End: 200}
	nonce := loadstand.LaunchNonce{1}
	census := loadstand.RouteWireCommitment{Window: nonce, Bounds: bounds, Count: 1, SHA256: loadstand.LaunchFingerprint{1}}
	fixture := &routeWireCaptureFixture{replies: map[loadstand.MemberID]loadstand.RouteWireCommitment{first: census, second: census}}
	var boot time.Duration = 99
	window := loadStandRouteWireWindow{nonce: nonce, bounds: bounds, members: []loadStandRouteWireMember{{first, 1}, {second, 2}}, port: fixture, readBoot: func() (time.Duration, error) { return boot, nil }}
	starts := map[loadStandNodeID]loadstand.StartReceipt{
		first:  {Incarnation: loadstand.Incarnation{Ordinal: 1, StartedAt: time.Unix(1, 0)}},
		second: {Incarnation: loadstand.Incarnation{Ordinal: 2, StartedAt: time.Unix(2, 0)}},
	}
	if err := window.validate([]loadStandExecutionAction{{Kind: loadStandEventStop, Node: "edge"}}); err != nil {
		t.Fatal("unmeasured process churn refused", err)
	}
	if err := window.validate([]loadStandExecutionAction{{Kind: loadStandEventStop, Node: first}}); !errors.Is(err, loadstand.ErrUnsupportedPlan) {
		t.Fatal("measured lifetime lost to a scheduled stop", err)
	}
	if err := window.checkBoundary(false); err != nil {
		t.Fatal("schedule cannot begin before window", err)
	}
	boot = bounds.End
	if got, err := window.capture(t.Context(), starts); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) || got != nil || len(fixture.requests) != 0 {
		t.Fatalf("queried before closing: %v, %v, requests %v", got, err, fixture.requests)
	}
	if err := window.checkBoundary(false); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
		t.Fatal("accepted schedule starting inside or after window", err)
	}
	boot++
	got, err := window.capture(t.Context(), starts)
	if err != nil || len(got) != 2 || got[loadStandRouteWireLife{Node: second, Incarnation: starts[second].Incarnation}] != census ||
		!slices.Equal(fixture.requests, []loadstand.MemberID{first, second}) {
		t.Fatalf("capture %v, requests %v, error %v", got, fixture.requests, err)
	}
	delete(fixture.replies, second)
	if got, err := window.capture(t.Context(), starts); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) || got != nil {
		t.Fatalf("returned partial census: %v, %v", got, err)
	}
	census.Bounds.End++
	fixture.replies[second] = census
	if got, err := window.capture(t.Context(), starts); !errors.Is(err, loadstand.ErrFinalReportInvalid) || got != nil {
		t.Fatalf("accepted another window: %v, %v", got, err)
	}
}

type loadStandRouteWireFinal struct {
	loadStandRouteWireLife
	Accounting loadstand.HeadlessRouteWireAccounting
}

type loadStandRouteWireReportPort interface {
	ReadCommittedRouteWireReport(context.Context, loadstand.MemberID, loadstand.IncarnationOrdinal, loadstand.RouteWireCommitment) (loadstand.HeadlessRouteWireAccounting, error)
}

type loadStandRouteWirePort interface {
	loadStandRouteWireReportPort
	CaptureRouteWireCensus(context.Context, loadstand.MemberID) (loadstand.RouteWireCommitment, error)
}

type loadStandRouteWireMember struct {
	id    loadStandNodeID
	limit int
}

type loadStandRouteWireWindow struct {
	nonce                  loadstand.LaunchNonce
	bounds                 loadstand.RouteWireWindowBounds
	members                []loadStandRouteWireMember
	port                   loadStandRouteWirePort
	readBoot               func() (time.Duration, error)
	origin                 time.Duration
	fullReceiptRouteBudget int
	requiredFullRoutes     int
	requiredFullSource     domain.RouteWireFullSource
	requireReconnect       bool
	disconnectAt           time.Duration
	dialer                 loadstand.MemberID
	seedSender             loadstand.MemberID
	seedSHA256             string
}

func (w *loadStandRouteWireWindow) validate(actions []loadStandExecutionAction) error {
	if w.nonce == (loadstand.LaunchNonce{}) || w.bounds.Begin <= 0 || w.bounds.End <= w.bounds.Begin ||
		len(w.members) < 2 || w.port == nil || w.readBoot == nil {
		return loadstand.ErrUnsupportedPlan
	}
	seen := make(map[loadStandNodeID]bool, len(w.members))
	for _, m := range w.members {
		if m.id == "" || m.limit <= 0 || seen[m.id] {
			return loadstand.ErrUnsupportedPlan
		}
		seen[m.id] = true
	}
	for _, a := range actions {
		if (a.Kind == loadStandEventStop || a.Kind == loadStandEventStart) && seen[a.Node] {
			return loadstand.ErrUnsupportedPlan
		}
	}
	return nil
}

func (w *loadStandRouteWireWindow) checkBoundary(closed bool) error {
	now, err := w.readBoot()
	openBefore := w.bounds.Begin
	if w.origin > 0 {
		openBefore = w.origin
	}
	if err != nil || now < 0 || (closed && now <= w.bounds.End) || (!closed && now >= openBefore) {
		return errors.Join(loadstand.ErrFinalAccountingUnavailable, err)
	}
	return nil
}

func (w *loadStandRouteWireWindow) capture(ctx context.Context, starts map[loadStandNodeID]loadstand.StartReceipt) (map[loadStandRouteWireLife]loadstand.RouteWireCommitment, error) {
	if err := w.checkBoundary(true); err != nil {
		return nil, err
	}
	rows := make(map[loadStandRouteWireLife]loadstand.RouteWireCommitment, len(w.members))
	for _, m := range w.members {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		life := starts[m.id].Incarnation
		if life.Ordinal == 0 || life.StartedAt.IsZero() {
			return nil, loadstand.ErrFinalAccountingUnavailable
		}
		id := loadstand.MemberID(m.id)
		census, err := w.port.CaptureRouteWireCensus(ctx, id)
		if err != nil {
			return nil, err
		}
		if census.Window != w.nonce || census.Bounds != w.bounds || census.Count < 0 || census.Count > m.limit || census.SHA256 == (loadstand.LaunchFingerprint{}) {
			return nil, loadstand.ErrFinalReportInvalid
		}
		rows[loadStandRouteWireLife{Node: id, Incarnation: life}] = census
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (w *loadStandRouteWireWindow) params() []runjournal.Param {
	params := []runjournal.Param{
		loadStandParam("route_wire_pair_policy", "tls_session_queue_intervals_v1"),
		loadStandParam("route_wire_rate_policy", "session_registered_overlap_node_full_window_v1"),
		loadStandParam("route_wire_window", w.nonce.String()),
		loadStandParam("route_wire_begin_ns", strconv.FormatInt(int64(w.bounds.Begin), 10)),
		loadStandParam("route_wire_end_ns", strconv.FormatInt(int64(w.bounds.End), 10)),
	}
	if w.origin > 0 {
		params = append(params,
			loadStandParam("route_wire_schedule_origin_ns", strconv.FormatInt(int64(w.origin), 10)),
			loadStandParam("route_wire_begin_offset_ns", strconv.FormatInt(int64(w.bounds.Begin-w.origin), 10)),
			loadStandParam("route_wire_end_offset_ns", strconv.FormatInt(int64(w.bounds.End-w.origin), 10)),
		)
	}
	if w.fullReceiptRouteBudget > 0 {
		params = append(params,
			loadStandParam("route_wire_full_receipt_route_budget", strconv.Itoa(w.fullReceiptRouteBudget)),
			loadStandParam("route_wire_full_receipt_policy", "complete_sender_passes_with_source_and_reciprocal_receipt_v2"),
		)
	}
	if w.requiredFullRoutes > 0 {
		params = append(params, loadStandParam("route_wire_required_full_routes", strconv.Itoa(w.requiredFullRoutes)))
	}
	if w.requiredFullSource != "" {
		params = append(params, loadStandParam("route_wire_required_full_source", string(w.requiredFullSource)))
	}
	if w.requireReconnect {
		params = append(params, loadStandParam("route_wire_session_policy", "two_sessions_ordered_replacement_v1"),
			loadStandParam("route_wire_disconnect_at_ns", strconv.FormatInt(int64(w.disconnectAt), 10)))
	}
	if w.dialer != "" {
		params = append(params, loadStandParam("route_wire_initiator", string(w.dialer)))
		if !w.requireReconnect {
			params = append(params, loadStandParam("route_wire_session_policy", "single_stable_session_v1"))
		}
	}
	if w.seedSender != "" {
		params = append(params, loadStandParam("route_wire_seed_sender", string(w.seedSender)), loadStandParam("route_wire_seed_identity_sha256", w.seedSHA256))
	}
	for i, member := range w.members {
		params = append(params, loadStandParam(runjournal.ParamName("route_wire_member_"+strconv.Itoa(i)), string(member.id)+"/"+strconv.Itoa(member.limit)))
	}
	return params
}

func stopLoadStandRouteWire(ctx context.Context, member interface {
	Stop(context.Context) (loadstand.StopReceipt, error)
	Owned() bool
}, port loadStandRouteWireReportPort, life loadStandRouteWireLife, commitment loadstand.RouteWireCommitment) (loadstand.StopReceipt, loadStandRouteWireFinal, error) {
	receipt, err := member.Stop(ctx)
	if err != nil {
		return receipt, loadStandRouteWireFinal{}, err
	}
	ended := receipt.Ended
	if !receipt.Terminated || member.Owned() || ended == nil || ended.Incarnation != life.Incarnation ||
		ended.Finality != loadstand.FinalExact || !ended.Final.Available() {
		return receipt, loadStandRouteWireFinal{}, loadstand.ErrFinalAccountingUnavailable
	}
	accounting, err := port.ReadCommittedRouteWireReport(ctx, life.Node, life.Incarnation.Ordinal, commitment)
	if err != nil {
		return receipt, loadStandRouteWireFinal{}, err
	}
	if accounting.Bounds != commitment.Bounds || !loadStandSameTraffic(accounting.Traffic, *ended.Final.Value) {
		return receipt, loadStandRouteWireFinal{}, loadstand.ErrFinalReportInvalid
	}
	return receipt, loadStandRouteWireFinal{loadStandRouteWireLife: life, Accounting: accounting}, nil
}
