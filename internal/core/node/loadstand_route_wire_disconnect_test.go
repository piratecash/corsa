package node

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestLoadStandRouteWireDisconnectClosesOriginalTLSOnce(t *testing.T) {
	clock := newK3ActualClock()
	request := k3ActualRequest(clock, 4, 8)
	request.routeWireScope = [32]byte{1}
	accepted, dialer := k3ActualService(t, &request), k3ActualService(t, &request)
	const begin, at, end = 4 * time.Second, 5 * time.Second, 8 * time.Second
	window, err := newRouteWireFrameWindow(begin, end, clock.read)
	if err != nil {
		t.Fatal(err)
	}
	dialer.loadstandRouteWire.Store(&LoadstandRouteWireObservation{
		window: window, fullBudget: 1, maximum: 4, connections: make(map[domain.ConnID]*routeWireConnection),
	})
	startPair := func() f1ProofFixture {
		t.Helper()
		owner := newF1FixtureOwner()
		left, right := net.Pipe()
		owner.rawServer, owner.rawClient = left, right
		pair := f1PairOnRaw(t, false, nil, accepted, dialer, owner, left, right)
		k3Publish(t, dialer, pair.outbound)
		return pair
	}
	clock.configure(3*time.Second, time.Nanosecond, nil)
	original := startPair()
	core := original.outbound.netCore
	clock.configure(begin, time.Nanosecond, nil)
	if err := dialer.DisconnectLoadstandRouteWireSession(t.Context(), at); !errors.Is(err, errRouteWireIncomplete) || !core.AcceptingFrames() {
		t.Fatalf("early disconnect affected original session: %v", err)
	}
	clock.configure(at, time.Nanosecond, nil)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := dialer.DisconnectLoadstandRouteWireSession(canceled, at); !errors.Is(err, context.Canceled) || !core.AcceptingFrames() {
		t.Fatalf("canceled disconnect affected original session: %v", err)
	}
	if err := dialer.DisconnectLoadstandRouteWireSession(t.Context(), at); err != nil || core.AcceptingFrames() {
		t.Fatalf("exact original session was not closed: %v", err)
	}
	select {
	case <-core.WriterExited():
	default:
		t.Fatal("original writer was not joined")
	}
	if err := dialer.DisconnectLoadstandRouteWireSession(t.Context(), at); !errors.Is(err, errRouteWireIncomplete) {
		t.Fatalf("second disconnect consumed the same plan: %v", err)
	}
	k3Withdraw(dialer, original.outbound)
	registryUnregister(accepted, original.fixtureOwner.inboundWire)
	if !original.fixtureOwner.stopAndJoin(t) {
		t.Fatal("original fixture did not stop")
	}
	clock.configure(6*time.Second, time.Nanosecond, nil)
	replacement := startPair()
	defer k3Withdraw(dialer, replacement.outbound)
	if err := dialer.DisconnectLoadstandRouteWireSession(t.Context(), at); !errors.Is(err, errRouteWireIncomplete) ||
		!replacement.outbound.netCore.AcceptingFrames() || replacement.outbound.netCore.LoadstandSessionID() == core.LoadstandSessionID() {
		t.Fatalf("replacement session was not preserved: %v", err)
	}
}
