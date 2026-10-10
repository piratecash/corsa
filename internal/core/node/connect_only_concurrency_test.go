package node

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// connect_only_concurrency_test.go pins two rules of the connect_only and
// add_peer commands at the Service boundary:
//
//   - a connect_only command that fails never makes its target the live pin —
//     not even for a moment, and whatever other connect_only commands run
//     beside it;
//   - add_peer under a live pin to another peer says so instead of claiming a
//     dial that will never happen.

const (
	concurrencyGoodPin = domain.PeerAddress("10.0.0.3:64646")
	concurrencyOldPin  = domain.PeerAddress("10.0.0.1:64646")
)

// A failing command and a succeeding one race, over and over, while a sampler
// watches the pin. The failing target must never be observed, and the end
// state must be the succeeding command's pin: the failing one has nothing to
// restore, so it cannot overwrite the winner either.
func TestConnectOnly_AFailedCommandNeverMakesItsTargetLive(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	failing := domain.PeerAddress(retentionUnadmittable)

	var stop atomic.Bool
	var sawFailingTarget atomic.Bool
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		for !stop.Load() {
			if pin, ok := svc.connectOnlyTarget(); ok && pin == failing {
				sawFailingTarget.Store(true)
			}
		}
	}()

	for i := 0; i < 300; i++ {
		old := concurrencyOldPin
		svc.connectOnly.Store(&old)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if reply := svc.enableConnectOnly(context.Background(), string(failing)); reply.Type != "error" {
				t.Errorf("connect_only %s reply = %+v, want an admission error", failing, reply)
			}
		}()
		go func() {
			defer wg.Done()
			if reply := svc.enableConnectOnly(context.Background(), string(concurrencyGoodPin)); reply.Type != "ok" {
				t.Errorf("connect_only %s reply = %+v, want ok", concurrencyGoodPin, reply)
			}
		}()
		wg.Wait()

		if pin, ok := svc.connectOnlyTarget(); !ok || pin != concurrencyGoodPin {
			stop.Store(true)
			<-samplerDone
			t.Fatalf("iteration %d: pin after a failed and a successful connect_only = %q (%v), want %s",
				i, pin, ok, concurrencyGoodPin)
		}
	}
	stop.Store(true)
	<-samplerDone

	if sawFailingTarget.Load() {
		t.Errorf("the target of a failing connect_only (%s) was observed as the live pin", failing)
	}
}

// add_peer of a peer other than the live pin registers it but cannot dial it;
// the reply must say exactly that, with a code a client can act on, rather
// than the usual "peer added" that promises an immediate dial.
func TestAddPeerUnderALivePin_RepliesRegisteredNotDialled(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	pin := concurrencyOldPin
	svc.connectOnly.Store(&pin)
	other := "10.0.0.7:64646"

	reply := svc.addPeerFrame(protocol.Frame{Type: "add_peer", Peers: []string{other}})

	if reply.Type != "ok" {
		t.Fatalf("add_peer under a pin reply = %+v, want ok: the peer is still registered", reply)
	}
	if reply.Code != protocol.CodeAddPeerNotDialledConnectOnly {
		t.Errorf("add_peer under a pin reply code = %q, want %q", reply.Code, protocol.CodeAddPeerNotDialledConnectOnly)
	}
	if !strings.Contains(reply.Status, string(pin)) {
		t.Errorf("add_peer under a pin status = %q, want it to name the pin %s", reply.Status, pin)
	}
	if svc.peerProvider.KnownPeerStatic(domain.PeerAddress(other)) == nil {
		t.Errorf("add_peer under a pin did not register %s", other)
	}
}

// The pin itself, and any add_peer without a pin, keep the ordinary reply.
func TestAddPeerOfThePinOrWithoutAPin_RepliesAsUsual(t *testing.T) {
	cases := []struct {
		name   string
		pin    *domain.PeerAddress
		target string
	}{
		{name: "no pin", target: "10.0.0.7:64646"},
		{name: "the pinned peer", pin: func() *domain.PeerAddress { p := concurrencyOldPin; return &p }(), target: string(concurrencyOldPin)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestService(t, config.NodeTypeFull)
			svc.connectOnly.Store(tc.pin)

			reply := svc.addPeerFrame(protocol.Frame{Type: "add_peer", Peers: []string{tc.target}})

			if reply.Type != "ok" || reply.Code != "" {
				t.Errorf("add_peer reply = %+v, want a plain ok", reply)
			}
		})
	}
}

// The DNS lookup of a connect_only hostname belongs to the request: an
// abandoned request must stop resolving. The resolver's only DNS server never
// answers — its Dial returns only when the lookup's context ends — so the
// lookup can end in time only through the request's own cancellation, never
// through an answer, an NXDOMAIN or the hosts file.
func TestResolveConnectOnlyHost_HonoursTheRequestContext(t *testing.T) {
	dialing := make(chan struct{})
	var dialingOnce sync.Once
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialingOnce.Do(func() { close(dialing) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	svc := newConnectOnlyTestService(t, "127.0.0.1:64646", resolver)
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		host string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		host, err := svc.resolveConnectOnlyHost(ctx, "connect-only-resolver-test.example")
		done <- result{host: host, err: err}
	}()
	awaitClosed(t, dialing, "the lookup to reach the DNS server")
	cancel()

	// Well inside connectOnlyDNSTimeout: only the cancellation can end the
	// lookup this soon.
	select {
	case r := <-done:
		if r.err == nil {
			t.Errorf("resolveConnectOnlyHost after its request was cancelled = %q, want an error", r.host)
		}
	case <-time.After(connectOnlyDNSTimeout / 2):
		t.Fatal("resolveConnectOnlyHost kept resolving after its request was cancelled")
	}
}

// newConnectOnlyTestService is newTestService with the listen address chosen
// by the test and, when resolver is non-nil, the connect_only resolver
// replaced — after NewService and before the Service is used, the discipline
// connectOnlyResolver documents.
func newConnectOnlyTestService(t *testing.T, listenAddress string, resolver *net.Resolver) *Service {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:     listenAddress,
		TrustStorePath:    filepath.Join(t.TempDir(), "trust.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	if resolver != nil {
		svc.connectOnlyResolver = resolver
	}
	return svc
}

// A hostname that resolves to this node is rejected with the same connect_only
// message as the literal self address, from the one self-check that runs on
// the resolved target.
//
// The node listens on whatever "localhost" resolves to here — IPv4 or IPv6 —
// so the premise holds on every host where localhost resolves at all, and the
// test is skipped only where it does not.
func TestConnectOnly_HostnameResolvingToSelfIsRejectedAsSelf(t *testing.T) {
	localhost, err := (&Service{connectOnlyResolver: net.DefaultResolver}).resolveConnectOnlyHost(context.Background(), "localhost")
	if err != nil {
		t.Skipf("localhost not resolvable in this environment: %v", err)
	}
	svc := newConnectOnlyTestService(t, net.JoinHostPort(localhost, "64646"), nil)

	reply := svc.enableConnectOnly(context.Background(), "localhost:64646")

	if reply.Type != "error" || reply.Error != connectOnlySelfRejection {
		t.Errorf("connect_only localhost:64646 on a node listening on %s = error %q (type %s), want error %q",
			svc.cfg.ListenAddress, reply.Error, reply.Type, connectOnlySelfRejection)
	}
	if _, ok := svc.connectOnlyTarget(); ok {
		t.Error("a self target was pinned")
	}
}
