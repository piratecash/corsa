package node

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

func TestLoadStandRouteWireSingleInitiatorLoopback(t *testing.T) {
	dialAddr, listenAddr := freeAddress(t), freeAddress(t)
	if dialAddr == listenAddr {
		t.Fatal("test members received the same address")
	}
	var at time.Duration
	construct := func(role domain.RouteWireDialRole, address, bootstrap string) (*Service, *RegisteredProvenV2Observation) {
		t.Helper()
		id, err := identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
		cfg := configtest.WithTempDataDirs(t, deriveTestAdvertisePort(config.Node{
			ListenAddress: address, BootstrapPeers: []string{bootstrap}, Type: config.NodeTypeFull,
			ListenerEnabled: true, ListenerSet: true, AllowPrivatePeers: true, DisableDirectMessages: true,
		}))
		service, observation, cleanup, err := NewLoadstandFullNodeWithRouteWireCensus(cfg, id, nil, 4, [32]byte{1}, 0, role)
		if err != nil {
			t.Fatal(err)
		}
		started := false
		defer func() {
			if !started {
				cleanup()
				service.WaitBackground()
			}
		}()
		if role == domain.RouteWireDialer {
			now, err := trustedclock.SinceBoot()
			if err != nil {
				t.Fatal(err)
			}
			begin := now + 3*time.Second
			at = begin + 200*time.Millisecond
			if _, err := service.EnableLoadstandRouteWireWindowWithFullReceipts(4, begin, at+15*time.Second, 1); err != nil {
				t.Fatal(err)
			}
		}
		service.PrimeBootstrapPeers()
		ctx, cancel := context.WithCancel(t.Context())
		started = true
		_, stop := startTestService(t, ctx, cancel, service)
		t.Cleanup(stop)
		return service, observation
	}
	_, listener := construct(domain.RouteWireListener, listenAddr, dialAddr)
	dialer, outbound := construct(domain.RouteWireDialer, dialAddr, listenAddr)
	await := func(transitions int) (k3RegisteredSnapshot, k3RegisteredSnapshot) {
		t.Helper()
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		poll := time.NewTicker(20 * time.Millisecond)
		defer poll.Stop()
		for {
			out, in := outbound.Snapshot(), listener.Snapshot()
			if out.Status != "PAST" || in.Status != "PAST" || len(out.Transitions) > transitions || len(in.Transitions) > transitions {
				t.Fatalf("unexpected connection history at %d transitions: outbound=%+v inbound=%+v", transitions, out, in)
			}
			if len(out.Transitions) == transitions && len(in.Transitions) == transitions {
				return out, in
			}
			select {
			case <-deadline.C:
				t.Fatalf("connection history did not reach %d transitions: outbound=%+v inbound=%+v", transitions, out, in)
			case <-poll.C:
			}
		}
	}
	out, in := await(1)
	checkPair := func(out, in k3RegisteredSnapshot, index int) [32]byte {
		t.Helper()
		a, b := out.Transitions[index].Added, in.Transitions[index].Added
		if out.RegisteredOwners != 1 || in.RegisteredOwners != 1 || a.Direction != "outbound" || b.Direction != "inbound" ||
			a.RouteWireSession == ([32]byte{}) || a.RouteWireSession != b.RouteWireSession {
			t.Fatalf("expected one reciprocal TLS session initiated by the dialer: outbound=%+v inbound=%+v", out, in)
		}
		return a.RouteWireSession
	}
	original := checkPair(out, in, 0)
	now, err := trustedclock.SinceBoot()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(at - now)
	if err := dialer.DisconnectLoadstandRouteWireSession(t.Context(), at); err != nil {
		t.Fatal(err)
	}
	out, in = await(3)
	replacement := checkPair(out, in, 2)
	if replacement == original || out.Transitions[1].Removed.RouteWireSession != original || in.Transitions[1].Removed.RouteWireSession != original ||
		max(out.Transitions[1].Clock.After, in.Transitions[1].Clock.After) >= min(out.Transitions[2].Clock.Before, in.Transitions[2].Clock.Before) {
		t.Fatal("new TLS session did not follow retirement of the original on both ends")
	}
}
