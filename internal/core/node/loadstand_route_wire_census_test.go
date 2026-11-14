package node

import (
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func TestLoadStandRouteWireCensusOriginalRegistry(t *testing.T) {
	clock := newK3ActualClock()
	request := k3ActualRequest(clock, 4, 8)
	request.routeWireScope = [32]byte{1}
	accepted := k3ActualService(t, &request)
	dialer := k3ActualService(t, &request)
	var inbound, outbound []RouteWireCensusEndpoint
	for generation := range 2 {
		clock.configure(time.Duration(3+2*generation)*time.Second, time.Nanosecond, nil)
		owner := newF1FixtureOwner()
		left, right := net.Pipe()
		owner.rawServer, owner.rawClient = left, right
		t.Cleanup(func() { owner.stopAndJoin(t) })
		fixture := f1PairOnRaw(t, false, nil, accepted, dialer, owner, left, right)
		k3Publish(t, dialer, fixture.outbound)
		id := fixture.fixtureOwner.inboundCore.LoadstandSessionID()
		if id == ([32]byte{}) || fixture.fixtureOwner.outboundCore.LoadstandSessionID() != id {
			t.Fatal("original TLS ends did not agree on their window ID")
		}
		other, ok := sessionv2.LoadstandSessionID(fixture.fixtureOwner.inboundWire, [32]byte{2})
		if !ok || other == id {
			t.Fatal("another window reused the session ID")
		}
		if _, ok := sessionv2.LoadstandSessionID(left, request.routeWireScope); ok {
			t.Fatal("raw transport minted a TLS session ID")
		}
		inbound = append(inbound, RouteWireCensusEndpoint{ConnID: fixture.inbound.id, Peer: domain.PeerIdentityFromWire(dialer.Address()), Direction: domain.PeerDirectionInbound, SessionID: id})
		outbound = append(outbound, RouteWireCensusEndpoint{ConnID: fixture.outbound.connID, Peer: domain.PeerIdentityFromWire(accepted.Address()), Direction: domain.PeerDirectionOutbound, SessionID: id})
		clock.configure(time.Duration(4+2*generation)*time.Second, time.Nanosecond, nil)
		k3Withdraw(dialer, fixture.outbound)
		registryUnregister(accepted, fixture.fixtureOwner.inboundWire)
		owner.stopAndJoin(t)
	}
	if inbound[0].SessionID == inbound[1].SessionID {
		t.Fatal("reconnection reused the previous window ID")
	}
	clock.configure(9*time.Second, time.Nanosecond, nil)
	for _, side := range []struct {
		service *Service
		want    []RouteWireCensusEndpoint
	}{{accepted, inbound}, {dialer, outbound}} {
		readout := &RegisteredProvenV2Observation{
			service: side.service, source: side.service.k3Registered, identity: domain.PeerIdentityFromWire(side.service.Address()),
			key: RegisteredProvenV2ObservationKey{Instance: request.Instance},
		}
		got, err := readout.RouteWireCensus(2*time.Second, 8*time.Second)
		for i := range got {
			interval := got[i].Interval
			begin, end := time.Duration(3+2*i)*time.Second, time.Duration(4+2*i)*time.Second
			if interval.BeginBefore < begin || interval.BeginAfter >= begin+time.Microsecond || interval.EndBefore < end || interval.EndAfter >= end+time.Microsecond {
				t.Fatalf("wrong lifetime for generation %d: %+v", i, interval)
			}
			got[i].Interval = domain.RouteWireSessionInterval{}
		}
		if err != nil || !slices.Equal(got, side.want) {
			t.Fatalf("got %+v, want %+v, error %v", got, side.want, err)
		}
	}
}

func TestLoadStandRouteWireCensusWindow(t *testing.T) {
	self, peer := idPeerA, idPeerB
	first := k3RegisteredOwnerRef{ConnID: 11, Generation: 1, Direction: "inbound"}
	second := k3RegisteredOwnerRef{ConnID: 22, Generation: 2, Direction: "outbound", Address: "127.0.0.1:1234"}
	third := k3RegisteredOwnerRef{ConnID: 33, Generation: 3, Direction: "inbound"}
	base := k3RegisteredSnapshot{
		Population: "RegisteredProvenV2", Status: "PAST", PastOnly: true, Instance: [16]byte{1},
		Baseline: k3RegisteredClock{0, 1}, Cut: k3RegisteredClock{14, 15}, Sequence: 4, RegisteredOwners: 2,
		Transitions: []k3RegisteredTransition{
			{Sequence: 1, Clock: k3RegisteredClock{2, 3}, Added: first, AddedIdentity: peer, RegisteredOwners: 1},
			{Sequence: 2, Clock: k3RegisteredClock{6, 7}, Removed: first, RemovedIdentity: peer, RegisteredOwners: 0},
			{Sequence: 3, Clock: k3RegisteredClock{8, 9}, Added: second, AddedIdentity: peer, RegisteredOwners: 1},
			{Sequence: 4, Clock: k3RegisteredClock{12, 13}, Added: third, AddedIdentity: peer, RegisteredOwners: 2},
		},
	}
	want := []RouteWireCensusEndpoint{
		{ConnID: 11, Peer: peer, Direction: domain.PeerDirectionInbound, Interval: domain.RouteWireSessionInterval{BeginBefore: 4, BeginAfter: 4, EndBefore: 6, EndAfter: 7}},
		{ConnID: 22, Peer: peer, Direction: domain.PeerDirectionOutbound, Interval: domain.RouteWireSessionInterval{BeginBefore: 8, BeginAfter: 9, EndBefore: 10, EndAfter: 10}},
	}
	got, err := projectRouteWireCensus(base, self, 4, 10)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("got %+v, want %+v, error %v", got, want, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*k3RegisteredSnapshot)
	}{
		{"source gap", func(s *k3RegisteredSnapshot) { s.Status = "UNKNOWN" }},
		{"uncovered end", func(s *k3RegisteredSnapshot) { s.Cut.Before = 9 }},
		{"uncertain opening", func(s *k3RegisteredSnapshot) { s.Transitions[0].Clock.After = 5 }},
		{"uncertain closing", func(s *k3RegisteredSnapshot) { s.Transitions[3].Clock.Before = 10 }},
		{"missing row", func(s *k3RegisteredSnapshot) { s.Transitions = s.Transitions[1:] }},
		{"wrong removal", func(s *k3RegisteredSnapshot) { s.Transitions[1].Removed.Generation++ }},
		{"reused connection", func(s *k3RegisteredSnapshot) { s.Transitions[2].Added.ConnID = 11 }},
		{"wrong owner total", func(s *k3RegisteredSnapshot) { s.RegisteredOwners++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			changed.Transitions = slices.Clone(base.Transitions)
			tc.mutate(&changed)
			got, err := projectRouteWireCensus(changed, self, 4, 10)
			if !errors.Is(err, errRouteWireIncomplete) || got != nil {
				t.Fatalf("accepted incomplete census %+v, error %v", got, err)
			}
		})
	}
}
