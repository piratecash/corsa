package node

import (
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOwnedP0SmallActiveCapacityAndRecovery(t *testing.T) {
	s, address := newOwnedBareV2PilotRelay(t, 2, 2)
	clients := []*ownedBareV2PilotClient{
		dialOwnedBareV2PilotClient(t, s, address, true),
		dialOwnedBareV2PilotClient(t, s, address, true),
	}
	runOwnedActiveCapacityPhases(t, s, clients, func() *ownedBareV2PilotClient {
		return dialOwnedBareV2PilotClient(t, s, address, false)
	})
}

func runOwnedActiveCapacityPhases(t *testing.T, s *Service, clients []*ownedBareV2PilotClient, dialExcess func() *ownedBareV2PilotClient) {
	t.Helper()
	n := len(clients)
	if s.degree.profile.MaxActive != n || s.bridgeCapture.priority.MaxMembers != n ||
		s.bridgeCapture.priority.MaxRegistryVisits <= n {
		t.Fatal("fixture would conflate the degree limit with another bound")
	}

	expected := make(map[domain.PeerIdentity]bool, n)
	for _, client := range clients {
		if client.id.IsZero() || expected[client.id] {
			t.Fatal("expected distinct original F1 identities", client.id)
		}
		expected[client.id] = true
	}
	activeAndRegistry := func(known, unknown, registry int) bool {
		s.degree.mu.Lock()
		tracked := 0
		for _, row := range s.degree.active {
			if row.life != nil {
				tracked++
			}
		}
		degreeOK := !s.degree.poisoned && tracked == known && s.degree.unknown == uint64(unknown)
		s.degree.mu.Unlock()
		s.peerMu.RLock()
		count := len(s.conns)
		s.peerMu.RUnlock()
		return degreeOK && count == registry
	}
	originalMembership := func() map[domain.PeerIdentity]findnode.Session {
		t.Helper()
		facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
		if err != nil || len(facts.allOriginal) != n {
			t.Fatal("incomplete original F1 population", len(facts.allOriginal), err)
		}
		members := make(map[domain.PeerIdentity]findnode.Session, n)
		for _, member := range facts.allOriginal {
			if !expected[member.Identity()] || member.ConnID() == 0 {
				t.Fatal("unexpected original member", member.Identity(), member.ConnID())
			}
			members[member.Identity()] = member
		}
		if len(members) != n {
			t.Fatal("repeated original membership")
		}
		return members
	}

	source, target := clients[0], clients[1]
	send := func(id domain.FindNodeRequestID) protocol.FindNodeRequest {
		t.Helper()
		request := protocol.FindNodeRequest{ID: id, Target: target.id, Challenge: domain.FindNodeChallenge{byte(id)}}
		wire, err := protocol.MarshalFindNodeRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		if err := source.write(append(wire, '\n')); err != nil {
			t.Fatal(err)
		}
		return request
	}
	answer := func(request protocol.FindNodeRequest) {
		t.Helper()
		select {
		case forwarded := <-target.requests:
			if forwarded.Target != request.Target || forwarded.Challenge != request.Challenge {
				t.Fatal("request changed before forwarding", request.ID)
			}
			target.replyTo(t, forwarded)
		case err := <-target.errors:
			t.Fatal(err)
		case <-time.After(2 * time.Second):
			t.Fatal("expected useful forward", request.ID)
		}
		select {
		case result := <-source.answers:
			if result.ID != request.ID {
				t.Fatal("response has the wrong original ID", result.ID, request.ID)
			}
			if err := protocol.VerifyFindNodeAnswer(result, request, domain.NetworkID(networkName)); err != nil {
				t.Fatal(err)
			}
		case err := <-source.errors:
			t.Fatal(err)
		case <-time.After(2 * time.Second):
			t.Fatal("expected verified signed answer", request.ID)
		}
	}

	waitFor(t, 2*time.Second, "degree capacity not reached", func() bool { return activeAndRegistry(n, 0, n) })
	originals := originalMembership()
	answer(send(18))
	positive := s.bridgeCapture.intake.ResourceSnapshot().Intake
	if positive.Offered != 1 || positive.Committed != 1 || positive.Taken != 1 || positive.Answered != 1 {
		t.Fatal("positive lookup missing from intake", positive)
	}

	extra := dialExcess()
	if extra.id.IsZero() || expected[extra.id] {
		t.Fatal("expected a fresh non-F1 proof identity")
	}
	waitFor(t, 2*time.Second, "excess original active lifetime not published", func() bool { return activeAndRegistry(n, 1, n+1) })
	if !maps.Equal(originals, originalMembership()) {
		t.Fatal("excess non-F1 lifetime changed the original membership")
	}
	if cut, err := s.captureOwnedFindNodeCut(t.Context()); !errors.Is(err, errDegreeUnavailable) || cut != (findnode.OwnedCut{}) {
		t.Fatal("incomplete active coverage returned a cut or the wrong error", err)
	}
	send(19)
	source.barrier(t)
	if got := s.bridgeCapture.intake.ResourceSnapshot().Intake; got != positive {
		t.Fatal("degree overflow admitted work", got, positive)
	}
	select {
	case unexpected := <-target.requests:
		t.Fatal("degree overflow reached the target", unexpected)
	default:
	}

	extra.stop(t)
	waitFor(t, 2*time.Second, "degree did not recover after excess teardown", func() bool { return activeAndRegistry(n, 0, n) })
	if !maps.Equal(originals, originalMembership()) {
		t.Fatal("recovery changed original F1 lifetimes")
	}
	answer(send(20))
	want := positive
	want.Offered++
	want.Committed++
	want.Taken++
	want.Answered++
	if got := s.bridgeCapture.intake.ResourceSnapshot().Intake; got != want {
		t.Fatal("recovery did not account for one new signed answer", got, want)
	}
	t.Logf("active=%d→%d→%d signed_answers=2 refused_before_intake=1", n, n+1, n)
}
