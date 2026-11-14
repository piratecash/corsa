package node

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/testutil/loadstand"
)

type loadStandRouteWireNativeProfile struct {
	Plan                             loadStandPlanOpts
	Limits                           loadStandExecutionLimits
	Timeout, StartupLead, Begin, End time.Duration
	MaxEndpoints                     int
	FullReceiptRouteBudget           int
	RequiredFullRoutes               int
	SeedFullRoutes                   bool
	RequiredFullSource               domain.RouteWireFullSource
	RequireReconnect                 bool
	DisconnectAt                     time.Duration
	SingleInitiator                  bool
	JournalDir                       string
}

func decodeLoadStandRouteWireNativeProfile(raw string) (loadStandRouteWireNativeProfile, error) {
	var profile loadStandRouteWireNativeProfile
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return profile, fmt.Errorf("invalid route-wire profile: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return profile, fmt.Errorf("%w: trailing data in route-wire profile", loadstand.ErrInvalidConfiguration)
	}
	return profile, nil
}

func (p loadStandRouteWireNativeProfile) prepare(nonce loadstand.LaunchNonce) (loadStandPlan, loadStandRouteWireLaunch, error) {
	plan, err := buildLoadStandPlan(p.Plan)
	if err != nil {
		return loadStandPlan{}, loadStandRouteWireLaunch{}, err
	}
	if err := p.Limits.validate(); err != nil {
		return loadStandPlan{}, loadStandRouteWireLaunch{}, err
	}
	if p.StartupLead <= 0 || p.Timeout <= plan.Duration() || p.StartupLead >= p.Timeout-plan.Duration() ||
		p.Limits.Cleanup >= p.Timeout-plan.Duration()-p.StartupLead ||
		len(plan.timeline) > p.Limits.MaxActions || len(plan.samples)+2 > p.Limits.MaxSamples ||
		!filepath.IsAbs(p.JournalDir) || filepath.Clean(p.JournalDir) != p.JournalDir {
		return loadStandPlan{}, loadStandRouteWireLaunch{}, fmt.Errorf("%w: route-wire time, sample, action or journal budget", loadstand.ErrInvalidConfiguration)
	}
	launch := loadStandRouteWireLaunch{
		Nonce: nonce, Origin: 1, Begin: p.Begin, End: p.End, MaxEndpoints: p.MaxEndpoints,
		FullReceiptRouteBudget: p.FullReceiptRouteBudget, RequiredFullRoutes: p.RequiredFullRoutes,
		SeedFullRoutes: p.SeedFullRoutes, RequiredFullSource: p.RequiredFullSource,
		RequireReconnect: p.RequireReconnect, DisconnectAt: p.DisconnectAt, SingleInitiator: p.SingleInitiator,
	}
	if _, err := launch.selection(plan); err != nil {
		return loadStandPlan{}, loadStandRouteWireLaunch{}, err
	}
	return plan, launch, nil
}

func TestLoadStandRouteWireNativeProfilesValidateWithoutEffects(t *testing.T) {
	nonce := loadstand.LaunchNonce{1}
	for _, scenario := range []string{"periodic", "reconnect"} {
		t.Run(scenario, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/loadstand-route-wire-" + scenario + ".json")
			if err != nil {
				t.Fatal(err)
			}
			profile, err := decodeLoadStandRouteWireNativeProfile(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			plan, launch, err := profile.prepare(nonce)
			if err != nil || len(plan.nodes) != 2 || len(plan.nodes[0].dialTargets) != 1 || len(plan.nodes[1].dialTargets) != 1 ||
				!launch.SingleInitiator || !launch.SeedFullRoutes || launch.RequiredFullRoutes != 10_000 || launch.FullReceiptRouteBudget != 10_000 {
				t.Fatalf("invalid two-member full route fixture: %+v, %v", launch, err)
			}
			cadence := routing.EffectiveForcedFullSyncInterval(routing.DefaultAnnounceInterval)
			if scenario == "periodic" {
				if launch.RequireReconnect || launch.RequiredFullSource != domain.RouteWireFullPeriodic ||
					launch.Begin >= cadence-profile.StartupLead || launch.End <= cadence+routing.DefaultAnnounceInterval ||
					profile.StartupLead+launch.End >= routing.DefaultTTL {
					t.Fatalf("periodic window does not cover one default full before the seed expires: %+v", launch)
				}
			} else if !launch.RequireReconnect || launch.RequiredFullSource != domain.RouteWireFullConnect ||
				profile.StartupLead+launch.End >= cadence {
				t.Fatalf("reconnect window intersects the earliest default periodic full: %+v", launch)
			}
			for _, change := range []func(*loadStandRouteWireNativeProfile){
				func(p *loadStandRouteWireNativeProfile) {
					p.Timeout = p.StartupLead + p.Plan.Duration + p.Limits.Cleanup
				},
				func(p *loadStandRouteWireNativeProfile) { p.Limits.MaxSamples = 2 },
				func(p *loadStandRouteWireNativeProfile) { p.JournalDir = "relative" },
			} {
				changed := profile
				change(&changed)
				if _, _, err := changed.prepare(nonce); err == nil {
					t.Fatal("accepted incomplete native profile")
				}
			}
			for _, invalid := range []string{string(raw) + "{}", `{"Unknown": true}`} {
				if _, err := decodeLoadStandRouteWireNativeProfile(invalid); err == nil {
					t.Fatal("accepted ambiguous native profile JSON")
				}
			}
		})
	}
}
