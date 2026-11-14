package node

import (
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connbudget"
)

func TestFull512FixtureDescriptorHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name             string
		soft, hard, used int
		want             int
		valid            bool
	}{
		{"one-descriptor-margin", 4096, 4096, 2045, 1, true},
		{"exact-bound", 4096, 4096, 2046, 0, true},
		{"one-descriptor-over", 4096, 4096, 2047, 0, false},
		{"lower-soft-limit", 4095, 4096, 0, 0, false},
		{"different-hard-limit", 4096, 8192, 0, 0, false},
		{"unknown-count", 4096, 4096, -1, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := full512FixtureFDSlack(tc.soft, tc.hard, tc.used)
			if (err == nil) != tc.valid || got != tc.want {
				t.Fatalf("descriptor headroom = %d, %v; want %d, valid=%v", got, err, tc.want, tc.valid)
			}
		})
	}
}

func TestFull512FixtureConnectionBudgetReachesRegistryBoundary(t *testing.T) {
	cfg := config.Node{MaxIncomingPeers: full512FixtureConnections}
	cfg.MaxTotalConnections = full512FixtureConnections + cfg.EffectiveOutboundConnectionReserve()
	base := connbudget.Config{
		Total: cfg.EffectiveMaxTotalConnections(), OutboundReserve: cfg.EffectiveOutboundConnectionReserve(),
		MaxOutbound: cfg.EffectiveMaxOutgoingPeers(), MaxInbound: cfg.EffectiveMaxIncomingPeers(),
		MaxAuxiliary: cfg.EffectiveMaxAuxiliaryConnections(),
	}

	for _, tc := range []struct {
		name       string
		inbound    int
		auxiliary  bool
		wantRefuse error
	}{
		{"exact-registry-overflow-entry", full512FixtureConnections, false, nil},
		{"inbound-cap-refuses-before-registry", full512FixtureRegistryLimit, false, connbudget.ErrDirectionLimit},
		{"auxiliary-uses-non-slot-capacity", full512FixtureConnections, true, connbudget.ErrOutboundReserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile := base
			profile.MaxInbound = tc.inbound
			budget, err := connbudget.New(profile)
			if err != nil {
				t.Fatal(err)
			}
			var held []*connbudget.Reservation
			t.Cleanup(func() {
				for _, reservation := range held {
					reservation.Release()
				}
				if stats := budget.Snapshot(); stats.Used != 0 {
					t.Fatal("fixture did not return all connection reservations", stats)
				}
			})
			if tc.auxiliary {
				auxiliary, err := budget.Reserve(connbudget.DirectionAuxiliary)
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, auxiliary)
			}
			for i := 0; i < full512FixtureRegistryLimit; i++ {
				inbound, err := budget.Reserve(connbudget.DirectionInbound)
				if err != nil {
					t.Fatalf("connection reservation %d: %v", i+1, err)
				}
				held = append(held, inbound)
			}
			extra, err := budget.Reserve(connbudget.DirectionInbound)
			if !errors.Is(err, tc.wantRefuse) || (extra != nil) != (tc.wantRefuse == nil) {
				t.Fatalf("reservation for 1025th registry entry: %v; want %v", err, tc.wantRefuse)
			}
			wantInbound := full512FixtureRegistryLimit
			if extra != nil {
				held = append(held, extra)
				wantInbound++
			}
			if got := budget.Snapshot().Inbound; got != wantInbound {
				t.Fatalf("inbound reservations = %d; want %d", got, wantInbound)
			}
		})
	}
}

func TestFull512FixtureLoopbackStillHasProductionCeiling(t *testing.T) {
	class := newLoopbackInboundClass(productionLoopbackInboundBounds(full512FixtureConnections),
		productionIdentityPenaltyPolicy(), newManualTestClock().now)
	for i := 0; i < 64; i++ {
		release, refusal := class.admit(true)
		if refusal != nil {
			t.Fatalf("loopback reservation %d: %v", i+1, refusal)
		}
		t.Cleanup(release)
	}
	if release, refusal := class.admit(true); release != nil ||
		refusal == nil || refusal.reason != refusalLoopbackConnections {
		t.Fatal("full512 connection budget altered the independent loopback ceiling", refusal)
	}
}
