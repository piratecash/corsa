package node

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func localRoleReady(t *testing.T, role findNodeLocalRole, kind domain.NodeType) (*findNodeReadyConstruction, *Service) {
	t.Helper()
	cfg, id := readyTestInputs(t)
	cfg.Type = kind
	profile := ownedCutProfile()
	profile.LocalRole = role
	profile.Intake.Round, profile.Intake.Slack = 100*time.Millisecond, 100*time.Millisecond
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.cleanup)
	s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil || !r.bindings(s) {
		t.Fatal("closed role did not bind before escape", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	return r, s
}

// No capability injection: both original TLS introductions come exclusively
// from the constructor roles. The real proof/auth/registry mint stays original.
func localRolePair(t *testing.T, accepted, dialer *Service) f1ProofFixture {
	t.Helper()
	owner := newF1FixtureOwner()
	left, right := net.Pipe()
	owner.rawServer, owner.rawClient = left, right
	f := f1PairOnRaw(t, false, nil, accepted, dialer, owner, left, right)
	accepted.publishAcceptedProofTransport(f.inbound)
	return f
}

func localRolePeerDeclaration(t *testing.T, proven sessionv2.ProvenSession, wantR bool) {
	t.Helper()
	peer, err := proven.Peer()
	f1, responder := 0, 0
	for _, name := range peer.Intro.Capabilities {
		if name == string(domain.CapOverlayFindNodeV1) {
			f1++
		}
		if name == string(domain.CapOverlayResponderV1) {
			responder++
		}
	}
	if err != nil || f1 != 1 || (responder == 1) != wantR || responder > 1 {
		t.Fatal("original authenticated directional declaration differs", err)
	}
}

func TestFindNodeLocalRoleConstructionAndProjection(t *testing.T) {
	for _, tc := range []struct {
		role  findNodeLocalRole
		kind  domain.NodeType
		valid bool
	}{
		{findNodeRoleOff, domain.NodeTypeFull, true}, {findNodeRoleTarget, domain.NodeTypeClient, true},
		{findNodeRoleTarget, domain.NodeTypeFull, true}, {findNodeRoleResponder, domain.NodeTypeFull, true},
		{findNodeRoleResponder, domain.NodeTypeClient, false}, {findNodeRoleResponder, "unknown", false},
		{findNodeRoleTarget, "unknown", false}, {findNodeLocalRole(99), domain.NodeTypeFull, false},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.kind, tc.role), func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			cfg.Type = tc.kind
			profile := ownedCutProfile()
			profile.LocalRole = tc.role
			r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
			if !tc.valid {
				if r != nil || !errors.Is(err, findnode.ErrInvalid) {
					t.Fatal("invalid role/type accepted", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(r.cleanup)
			s, err := r.build(cfg, id, nil, productionDatagramConstructor{})
			if err != nil || s.localFindNodeRole != tc.role || !r.bindings(s) {
				t.Fatal("role binding failed", err)
			}
			for _, intro := range []protocol.Frame{s.nodeHelloFrame(), s.welcomeFrame("", "")} {
				f1, responder := 0, 0
				for _, capability := range intro.Capabilities {
					if capability == string(domain.CapOverlayFindNodeV1) {
						f1++
					}
					if capability == string(domain.CapOverlayResponderV1) {
						responder++
					}
				}
				if (f1 == 1) != (tc.role != findNodeRoleOff) || f1 > 1 || (responder == 1) != (tc.role == findNodeRoleResponder) || responder > 1 {
					t.Fatal("wrong own role projection", intro.Capabilities)
				}
			}
		})
	}
}

func TestFindNodeF1OnlyTargetAnswersOwnIdentity(t *testing.T) {
	for _, kind := range []domain.NodeType{domain.NodeTypeFull, domain.NodeTypeClient} {
		t.Run(string(kind), func(t *testing.T) {
			_, target := localRoleReady(t, findNodeRoleTarget, kind)
			_, source := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
			pair := localRolePair(t, target, source)
			localRolePeerDeclaration(t, pair.acceptedProof, false)
			localRolePeerDeclaration(t, pair.dialedProof, false)
			request := ownedLocalNodeRequest(t, target)
			if err := target.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, target, pair, request)); err != nil {
				t.Fatal("F1 self target incorrectly requires R", err)
			}
			answer := ownedLocalNodeReadAnswer(t, pair)
			if answer.ID != request.ID || protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)) != nil {
				t.Fatal("original local target lost challenge/signature")
			}
		})
	}
}

func TestFindNodeF1OnlyTargetCannotForwardOrBorrowPeerR(t *testing.T) {
	for _, peerRole := range []findNodeLocalRole{findNodeRoleTarget, findNodeRoleResponder} {
		t.Run(fmt.Sprint(peerRole), func(t *testing.T) {
			_, receiver := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeFull)
			_, source := localRoleReady(t, peerRole, domain.NodeTypeFull)
			_, target := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
			sourcePair := localRolePair(t, receiver, source)
			targetPair := localRolePair(t, receiver, target)
			localRolePeerDeclaration(t, sourcePair.acceptedProof, peerRole == findNodeRoleResponder)
			localRolePeerDeclaration(t, targetPair.acceptedProof, false)
			before := receiver.bridgeCapture.intake.ResourceSnapshot()
			wire, _ := ownedReceiveWire(t, domain.PeerIdentityFromWire(target.identity.Address))
			if sourcePair.outbound.netCore.SendRaw([]byte(wire+"\n")) != netcore.SendOK {
				t.Fatal("original source write failed")
			}
			if err := receiver.dispatchOwnedFindNodeReceive(ownedAnswerReadOriginalInbound(t, receiver, sourcePair)); !errors.Is(err, findnode.ErrUnauthenticated) {
				t.Fatal("F1-only receiver forwarded or inherited peer R", err)
			}
			after := receiver.bridgeCapture.intake.ResourceSnapshot()
			if after.Intake != before.Intake || after.Writer != before.Writer || after.Pending != before.Pending || receiver.bridgeCapture.intake.OwnedRoundPending() {
				t.Fatal("role refusal admitted work")
			}
		})
	}
}

func TestFindNodeResponderForwardsToF1OnlyTargetInbound(t *testing.T) {
	_, relay := localRoleReady(t, findNodeRoleResponder, domain.NodeTypeFull)
	_, source := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
	_, target := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
	from := localRolePair(t, relay, source)
	to := localRolePair(t, relay, target)
	localRolePeerDeclaration(t, from.acceptedProof, false)
	localRolePeerDeclaration(t, to.acceptedProof, false)
	localRolePeerDeclaration(t, from.dialedProof, true)
	wire, request := ownedReceiveWire(t, domain.PeerIdentityFromWire(target.identity.Address))
	if from.outbound.netCore.SendRaw([]byte(wire+"\n")) != netcore.SendOK {
		t.Fatal("source send")
	}
	if err := relay.dispatchOwnedFindNodeReceive(ownedAnswerReadOriginalInbound(t, relay, from)); err != nil {
		t.Fatal("R requires symmetric peer-R", err)
	}
	forward := ownedExecutingReadForward(t, to, request.Target)
	if forward.Target != request.Target || forward.Challenge != request.Challenge {
		t.Fatal("forward changed retained request")
	}
	answer := ownedAnswerSignedBody(t, target, forward)
	raw, err := protocol.MarshalFindNodeAnswer(answer)
	if err != nil {
		t.Fatal(err)
	}
	if to.outbound.netCore.SendRaw(append(raw, '\n')) != netcore.SendOK {
		t.Fatal("target send")
	}
	if err := relay.dispatchOwnedFindNodeReceive(ownedAnswerReadOriginalInbound(t, relay, to)); err != nil {
		t.Fatal(err)
	}
	returned := ownedAnswerReadBody(t, from)
	if returned.ID != request.ID || protocol.VerifyFindNodeAnswer(returned, request, domain.NetworkID(networkName)) != nil {
		t.Fatal("R lost signed return")
	}
}

func TestFindNodeResponderToF1OnlyTargetOriginalOutboundCM(t *testing.T) {
	x := newReadyThreeNodeChain(t, false, findNodeRoleTarget)
	localRolePeerDeclaration(t, x.targetWire.acceptedProof, false)
	localRolePeerDeclaration(t, x.targetWire.dialedProof, true)
	clientPeer, err := x.targetWire.acceptedProof.Peer()
	if err != nil || clientPeer.Intro.NodeType != string(domain.NodeTypeClient) {
		t.Fatal("authenticated client type lost", err)
	}
	if x.relay.localFindNodeRole != findNodeRoleResponder || x.target.localFindNodeRole != findNodeRoleTarget || x.target.cfg.Type != domain.NodeTypeClient || x.targetWire.dialer != x.target {
		t.Fatal("not the actual client's original outbound Target role")
	}
	lookup, _ := d1StartOriginal(t, x.driver, x.targetID)
	var line string
	select {
	case line = <-x.answers:
	case err := <-x.errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("client outbound reader did not produce own signed answer")
	}
	answer, err := protocol.ParseFindNodeAnswer([]byte(line))
	if err != nil || answer.ID != lookup.request.ID || protocol.VerifyFindNodeAnswer(answer, lookup.request, domain.NetworkID(networkName)) != nil {
		t.Fatal("client outbound self-answer lost signed original", err)
	}
	if got := x.driver.Receive(lookup, lookup.peers[0].common, []byte(line)); got.status != d1TestFound || got.err != nil {
		t.Fatal("original CM/outbound role path failed", got)
	}
	x.settled(t, 1)
}

func TestFindNodeNoF1AndUnprovenClaimsCannotAdmit(t *testing.T) {
	_, relay := localRoleReady(t, findNodeRoleResponder, domain.NodeTypeFull)
	_, unnegotiated := localRoleReady(t, findNodeRoleOff, domain.NodeTypeFull)
	pair := localRolePair(t, relay, unnegotiated)
	before := relay.bridgeCapture.intake.ResourceSnapshot()
	wire, _ := ownedReceiveWire(t, domain.PeerIdentityFromWire(relay.identity.Address))
	if pair.outbound.netCore.SendRaw([]byte(wire+"\n")) != netcore.SendOK {
		t.Fatal("source send")
	}
	core := pair.fixtureOwner.inboundCore
	if err := core.Conn().SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, receipt, claimed, receiveErr, readErr := relay.readInboundOwnedFindNodeLine(bufio.NewReader(core.Conn()), pair.inbound.id, pair.inbound)
	if readErr != nil || receiveErr != nil || !claimed {
		t.Fatal("original no-F1 read failed", readErr, receiveErr)
	}
	if relay.admitInboundCommandLine(pair.inbound.id, core.Conn().RemoteAddr().String(), line) {
		if err := relay.dispatchOwnedFindNodeReceive(receipt); !errors.Is(err, findnode.ErrUnauthenticated) {
			t.Fatal("no-F1 proof admitted", err)
		}
	}
	life := &proofTransportLifetime{}
	relay.captureFindNodeSeed(life, sessionv2.ProvenSession{}, protocol.Frame{Capabilities: []string{string(domain.CapOverlayFindNodeV1), string(domain.CapOverlayResponderV1)}})
	if life.findNode != nil || relay.bridgeCapture.intake.ResourceSnapshot().Intake != before.Intake {
		t.Fatal("unproven legacy declarations gained admission")
	}
}

type localRoleTamperConstructor struct {
	ready *findNodeReadyConstruction
	field string
}

func (c localRoleTamperConstructor) Build(s *Service, _ *datagram.Metrics) (*datagramLayer, error) {
	switch c.field {
	case "service":
		s.localFindNodeRole = findNodeRoleResponder
	case "ready":
		c.ready.localRole = findNodeRoleResponder
	case "original":
		c.ready.originalLocalRole = findNodeRoleResponder
	}
	return nil, nil
}
func TestFindNodeLocalRoleBoundToOriginalReadyConstruction(t *testing.T) {
	for _, field := range []string{"service", "ready", "original"} {
		t.Run(field, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			profile := ownedCutProfile()
			profile.LocalRole = findNodeRoleTarget
			r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(r.cleanup)
			if s, err := r.build(cfg, id, nil, localRoleTamperConstructor{r, field}); s != nil || err == nil || r.finalized || r.originalContext.Err() == nil {
				t.Fatal("mutated role escaped Ready", err)
			}
		})
	}
}

// The original observed F1 connection still closes through its genuine owner;
// no copied role or same identity may keep its ended admission authority.
func TestFindNodeRoleDoesNotReviveEndedOriginalF1(t *testing.T) {
	_, relay := localRoleReady(t, findNodeRoleResponder, domain.NodeTypeFull)
	_, source := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
	pair := localRolePair(t, relay, source)
	wire, _ := ownedReceiveWire(t, domain.PeerIdentityFromWire(relay.identity.Address))
	receipt, err := relay.recordOwnedFindNodeReceive(pair.inbound, wire)
	if err != nil {
		t.Fatal(err)
	}
	pair.inbound.end()
	if err := relay.dispatchOwnedFindNodeReceive(receipt); !errors.Is(err, findnode.ErrUnauthenticated) {
		t.Fatal("ended original role revived admission", err)
	}
	if relay.bridgeCapture.intake.ResourceSnapshot().Intake.Offered != 0 {
		t.Fatal("ended source admitted row")
	}
}
