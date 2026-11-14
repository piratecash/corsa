package node

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

// All offsets are relative to one controller-selected CLOCK_BOOTTIME epoch.
// The cohort is the complete FullNode topology of this plan.
type loadStandRouteWireLaunch struct {
	Nonce                  loadstand.LaunchNonce
	Origin                 time.Duration
	Begin, End             time.Duration
	MaxEndpoints           int
	FullReceiptRouteBudget int
	RequiredFullRoutes     int
	SeedFullRoutes         bool
	RequiredFullSource     domain.RouteWireFullSource
	RequireReconnect       bool
	DisconnectAt           time.Duration
	SingleInitiator        bool
}

func (l loadStandRouteWireLaunch) selection(plan loadStandPlan) (loadstand.PlannedRouteWireObservation, error) {
	if plan.Duration() <= 0 || plan.Duration() > loadStandMaxPlanDuration ||
		l.Nonce == (loadstand.LaunchNonce{}) || l.Origin <= 0 || l.Begin <= 0 || l.End <= l.Begin ||
		l.End >= plan.Duration() || l.Origin > math.MaxInt64-plan.Duration() ||
		l.MaxEndpoints <= 0 || l.MaxEndpoints > k3RegisteredMaxOwners || l.MaxEndpoints > k3RegisteredMaxHistory/2 ||
		l.FullReceiptRouteBudget < 0 || l.FullReceiptRouteBudget > l.MaxEndpoints*domain.RouteWireFullMaxDestinations ||
		l.RequiredFullRoutes < 0 || l.RequiredFullRoutes > min(l.FullReceiptRouteBudget, domain.RouteWireFullMaxDestinations) ||
		l.SeedFullRoutes && (l.RequiredFullRoutes == 0 || len(plan.nodes) != 2) ||
		l.RequiredFullSource != "" && (l.RequiredFullRoutes == 0 ||
			l.RequiredFullSource != domain.RouteWireFullPeriodic && l.RequiredFullSource != domain.RouteWireFullConnect) ||
		l.SingleInitiator && (len(plan.nodes) != 2 || l.RequiredFullSource == "" || l.RequiredFullSource == domain.RouteWireFullConnect && !l.RequireReconnect) ||
		l.RequireReconnect && (!l.SingleInitiator || l.RequiredFullSource != domain.RouteWireFullConnect || l.MaxEndpoints < 2 ||
			l.DisconnectAt <= l.Begin || l.DisconnectAt >= l.End) || !l.RequireReconnect && l.DisconnectAt != 0 ||
		len(plan.nodes) < 2 {
		return loadstand.PlannedRouteWireObservation{}, loadstand.ErrUnsupportedPlan
	}
	for _, node := range plan.nodes {
		if node.role != loadStandRoleFull {
			return loadstand.PlannedRouteWireObservation{}, loadstand.ErrUnsupportedPlan
		}
	}
	for _, event := range plan.timeline {
		if event.kind == loadStandEventStart || event.kind == loadStandEventStop {
			return loadstand.PlannedRouteWireObservation{}, loadstand.ErrUnsupportedPlan
		}
	}
	return loadstand.PlannedRouteWireObservation{
		Window: l.Nonce, MaxEndpoints: l.MaxEndpoints, FullReceiptRouteBudget: l.FullReceiptRouteBudget,
		Bounds: loadstand.RouteWireWindowBounds{Begin: l.Origin + l.Begin, End: l.Origin + l.End},
	}, nil
}

func TestLoadStandRouteWireLaunchBindsOneClosedTimeline(t *testing.T) {
	plan := mustBuildLoadStandPlan(t, loadStandFullOnlyOptions())
	launch := loadStandRouteWireLaunch{
		Nonce: loadstand.LaunchNonce{1}, Origin: 3 * time.Second,
		Begin: 10 * time.Millisecond, End: 150 * time.Millisecond, MaxEndpoints: 16,
	}
	got, err := launch.selection(plan)
	want := loadstand.PlannedRouteWireObservation{
		Window: launch.Nonce, MaxEndpoints: 16,
		Bounds: loadstand.RouteWireWindowBounds{Begin: 3010 * time.Millisecond, End: 3150 * time.Millisecond},
	}
	if err != nil || got != want {
		t.Fatalf("selection = %+v, %v, want %+v", got, err, want)
	}
	for _, tc := range []struct {
		name   string
		change func(*loadStandRouteWireLaunch, *loadStandPlan)
	}{
		{"no nonce", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.Nonce = loadstand.LaunchNonce{} }},
		{"negative epoch", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.Origin = -1 }},
		{"opens at epoch", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.Begin = 0 }},
		{"empty interval", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.End = l.Begin }},
		{"end at last action", func(l *loadStandRouteWireLaunch, p *loadStandPlan) { l.End = p.Duration() }},
		{"boot time overflow", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.Origin = math.MaxInt64 }},
		{"endpoint cap", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.MaxEndpoints = k3RegisteredMaxOwners + 1 }},
		{"negative route target", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.RequiredFullRoutes = -1 }},
		{"route target without receipt budget", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.RequiredFullRoutes = 10_000 }},
		{"seed without route target", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.SeedFullRoutes = true }},
		{"source without route target", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) {
			l.RequiredFullSource = domain.RouteWireFullPeriodic
		}},
		{"unsupported source", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) {
			l.FullReceiptRouteBudget, l.RequiredFullRoutes, l.RequiredFullSource = 10_000, 10_000, "other"
		}},
		{"reconnect without connect source", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.RequireReconnect = true }},
		{"reconnect cannot retain both endpoints", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) {
			l.FullReceiptRouteBudget, l.RequiredFullRoutes, l.RequiredFullSource = 10_000, 10_000, domain.RouteWireFullConnect
			l.RequireReconnect, l.SingleInitiator, l.MaxEndpoints = true, true, 1
			l.DisconnectAt = l.Begin + time.Millisecond
		}},
		{"disconnect without reconnect profile", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) { l.DisconnectAt = l.Begin + time.Millisecond }},
		{"disconnect at end of window", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) {
			l.FullReceiptRouteBudget, l.RequiredFullRoutes, l.RequiredFullSource = 10_000, 10_000, domain.RouteWireFullConnect
			l.RequireReconnect, l.SingleInitiator, l.DisconnectAt = true, true, l.End
		}},
		{"reconnect without a single initiator", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) {
			l.FullReceiptRouteBudget, l.RequiredFullRoutes, l.RequiredFullSource = 10_000, 10_000, domain.RouteWireFullConnect
			l.RequireReconnect, l.DisconnectAt = true, l.Begin+time.Millisecond
		}},
		{"route target exceeds single projection", func(l *loadStandRouteWireLaunch, _ *loadStandPlan) {
			l.FullReceiptRouteBudget, l.RequiredFullRoutes = 20_000, 10_001
		}},
		{"SDK without reciprocal report", func(_ *loadStandRouteWireLaunch, p *loadStandPlan) { p.nodes[1].role = loadStandRoleEdge }},
		{"single member", func(_ *loadStandRouteWireLaunch, p *loadStandPlan) { p.nodes = p.nodes[:1] }},
		{"member restart", func(_ *loadStandRouteWireLaunch, p *loadStandPlan) {
			p.timeline = []loadStandPlanEvent{{kind: loadStandEventStop, node: p.nodes[0].id}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := launch
			p := mustBuildLoadStandPlan(t, loadStandFullOnlyOptions())
			tc.change(&l, &p)
			if got, err := l.selection(p); !errors.Is(err, loadstand.ErrUnsupportedPlan) || got != (loadstand.PlannedRouteWireObservation{}) {
				t.Fatalf("accepted invalid launch: %+v, %v", got, err)
			}
		})
	}
}

func TestLoadStandRouteWireScheduleUsesBootEpoch(t *testing.T) {
	now := 990 * time.Millisecond
	s := &loadStandRouteWireSchedule{origin: time.Second, readBoot: func() (time.Duration, error) { return now, nil }}
	if got := s.Elapsed(); got != -10*time.Millisecond {
		t.Fatalf("pre-epoch elapsed: %v", got)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.WaitUntil(canceled, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled future wait: %v", err)
	}
	now = 1010 * time.Millisecond
	if err := s.WaitUntil(t.Context(), 10*time.Millisecond); err != nil || s.Elapsed() != 10*time.Millisecond {
		t.Fatalf("reached action: %v", err)
	}
	w := &loadStandRouteWireWindow{origin: time.Second, bounds: loadstand.RouteWireWindowBounds{Begin: 1030 * time.Millisecond, End: 1070 * time.Millisecond}, readBoot: s.readBoot}
	if err := w.checkBoundary(false); !errors.Is(err, loadstand.ErrFinalAccountingUnavailable) {
		t.Fatalf("accepted startup after epoch: %v", err)
	}
	now = 1009 * time.Millisecond
	if err := s.WaitUntil(t.Context(), 0); !errors.Is(err, loadstand.ErrScheduleMissed) {
		t.Fatalf("accepted boot clock regression: %v", err)
	}
	calls := 0
	wake := &loadStandRouteWireSchedule{origin: time.Second, readBoot: func() (time.Duration, error) {
		calls++
		return time.Second + time.Duration(calls), nil
	}}
	if err := wake.WaitUntil(t.Context(), 3); err != nil || calls != 3 {
		t.Fatalf("timer wake without clock confirmation: reads=%d, err=%v", calls, err)
	}
}

// The timeline and frame window use the same boot clock. The timer only
// wakes the executor: the clock is sampled again before dispatch.
type loadStandRouteWireSchedule struct {
	origin, last time.Duration
	readBoot     func() (time.Duration, error)
}

func (s *loadStandRouteWireSchedule) elapsed() (time.Duration, error) {
	now, err := s.readBoot()
	if err != nil || now < s.last {
		return 0, loadstand.ErrScheduleMissed
	}
	s.last = now
	return now - s.origin, nil
}

func (s *loadStandRouteWireSchedule) Elapsed() time.Duration {
	elapsed, err := s.elapsed()
	if err != nil {
		return -1
	}
	return elapsed
}

func (s *loadStandRouteWireSchedule) WaitUntil(ctx context.Context, at time.Duration) error {
	if at < 0 || at > math.MaxInt64-s.origin {
		return loadstand.ErrScheduleMissed
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		elapsed, err := s.elapsed()
		if err != nil || elapsed >= at {
			return err
		}
		timer := time.NewTimer(at - elapsed)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
