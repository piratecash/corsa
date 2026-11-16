//go:build linux && d1_full512_native

package node

import (
	"context"
	"net"
	"net/netip"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

const full512F1Members = 512

func TestOwnedP0FullPopulationAndRegistryOverflow(t *testing.T) {
	if os.Getenv("CORSA_D1_FULL512_NATIVE") != "1" {
		t.Skip("requires the separately admitted Full512 guest")
	}
	if runtime.GOARCH != "arm64" {
		t.Fatal("Full512 requires Linux arm64")
	}
	planned := full512GuestHostPlan(t)
	if len(planned) != 1+full512FixtureConnections {
		t.Fatal("guest plan must identify the listener and each client", len(planned))
	}
	for _, prefix := range planned {
		if !prefix.Addr().Is4() {
			t.Fatal("Full512 guest plan requires IPv4 host addresses")
		}
	}
	readFull512GuestPreflight(t, planned)

	priorLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	t.Cleanup(func() { zerolog.SetGlobalLevel(priorLevel) })
	quota := &full512AppQuota{frameLimit: full512FrameLimit, payloadLimit: full512PayloadLimit}
	t.Cleanup(func() {
		frames, payload, exceeded := quota.snapshot()
		t.Logf("D1_P0_FULL512_QUOTA_V1 {\"frames\":%d,\"payload_bytes\":%d,\"exceeded\":%t}", frames, payload, exceeded)
		if exceeded {
			t.Error(errFull512AppQuota)
		}
	})
	s, address := newOwnedFull512GuestRelay(t, planned[0].Addr())
	clientDialer := func(index int) net.Dialer {
		return net.Dialer{LocalAddr: &net.TCPAddr{IP: net.IP(planned[index+1].Addr().AsSlice())}}
	}

	var source, target *ownedBareV2PilotClient
	identities := make(map[domain.PeerIdentity]bool, full512F1Members)
	for i := 0; i < full512F1Members; i++ {
		client := dialOwnedBareV2PilotClientFrom(t, s, address, true, clientDialer(i), quota)
		if identities[client.id] {
			t.Fatal("repeated F1 client identity", i)
		}
		identities[client.id] = true
		switch i {
		case 0:
			source = client
		case 1:
			target = client
		}
	}
	for i := full512F1Members; i < full512FixtureRegistryLimit; i++ {
		dialOwnedPilotUnnegotiated(t, s, address, clientDialer(i), quota)
	}
	var originals []findnode.Session
	waitFor(t, 2*time.Second, "full512 original membership was not published", func() bool {
		facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
		if err != nil || len(facts.allOriginal) != len(identities) {
			return false
		}
		for _, original := range facts.allOriginal {
			if !identities[original.Identity()] {
				return false
			}
		}
		originals = facts.allOriginal
		return true
	})
	runOwnedMixedP0Phases(t, s, source, target, originals, func() net.Conn {
		return dialOwnedPilotUnnegotiated(t, s, address, clientDialer(full512FixtureRegistryLimit), quota)
	})
}

// The full guest fixture owns an inbound-only listener and preserves the
// normal connection, handshake, command and F1 admission paths.
func newOwnedFull512GuestRelay(t *testing.T, host netip.Addr) (*Service, string) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configtest.WithTempDataDirs(t, config.Node{
		ListenAddress: netip.AddrPortFrom(host, 0).String(), ListenerSet: true, ListenerEnabled: true,
		Type: domain.NodeTypeFull, BootstrapPeers: []string{}, AllowPrivatePeers: true,
		MaxIncomingPeers: full512FixtureConnections,
	})
	cfg.MaxTotalConnections = full512FixtureConnections + cfg.EffectiveOutboundConnectionReserve()
	profile := ownedCutProfile()
	profile.Capture = findNodeOwnedCaptureProfile{
		Degree:   degreeProfile{MaxActive: full512FixtureConnections},
		Priority: findNodePriorityProfile{MaxMembers: full512F1Members, MaxRegistryVisits: full512FixtureRegistryLimit},
	}
	profile.Intake = findnode.DormantIntakeProfile{
		Timing: findnode.TimingProfile{Target: 2 * time.Second, Network: 2 * time.Second, Runtime: time.Second},
		Round:  100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4,
	}
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile,
		findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8},
		findnode.OwnedTargetTimingProfile{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil {
		t.Fatal(err)
	}
	if !r.finalized || !r.checked || !r.bindings(s) || !s.bridgeCapture.consumer.IsRuntime() ||
		s.disableRateLimiting || s.loopbackInbound.bounds.maxActive > 64 {
		t.Fatal("full512 fixture changed the original admission bounds")
	}
	s.ownedFindNodeAdvertiseTest = true
	s.connManager.config.MaxSlotsFn = func() int { return 0 }
	running := runServiceForTest(t.Context(), s)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := running.Stop(ctx); err != nil {
			t.Error("full512 service did not stop", err)
		}
		if err := running.DrainBackground(ctx); err != nil {
			t.Error("full512 background work did not join", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := running.AwaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	s.peerMu.RLock()
	address := s.listener.Addr().String()
	s.peerMu.RUnlock()
	endpoint, err := netip.ParseAddrPort(address)
	if err != nil || endpoint.Addr() != host || endpoint.Port() == 0 {
		t.Fatal("full512 listener differs from the verified local host", err)
	}
	return s, address
}
