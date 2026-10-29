package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// These are routing-state/handler fixtures, not authenticated wire sessions.
// The captured sender values exercise attribution; they assert no per-retry
// freshness because relay_hop_ack carries only the message ID.
func TestHopAckReroutePreservesReplacementTimerAndPayload(t *testing.T) {
	for _, transition := range []string{"upsert", "failover"} {
		for _, sameAddress := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/same-address-%v", transition, sameAddress), func(t *testing.T) {
				svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
				svc.relayStates = newRelayStateStore()
				a := &peerSession{address: "old", peerIdentity: idPeerA}
				b := &peerSession{address: "new", peerIdentity: idPeerB}
				if sameAddress {
					b.address = a.address
				}
				svc.sessions[a.address] = a
				for _, id := range []domain.PeerIdentity{idPeerA, idPeerB} {
					status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
						Identity: idTargetX, Origin: idTargetX, NextHop: id,
						Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement, ExpiresAt: time.Now().Add(time.Hour),
					})
					if err != nil || status != routing.RouteAccepted {
						t.Fatalf("route fixture: %v %v", status, err)
					}
				}
				svc.relayStates.store(&relayForwardState{
					MessageID: "rerouted", Recipient: idTargetX, ForwardedTo: a.address,
					ForwardedVia: sessionRoutingSender(a), NextHopSelection: relayNextHopRoutingTable,
					HopAckObserved: true, FrameLine: "old-payload",
				})
				svc.sessions[b.address] = b
				if transition == "upsert" {
					svc.relayStates.store(&relayForwardState{
						MessageID: "rerouted", Recipient: idTargetX, ForwardedTo: b.address,
						ForwardedVia: sessionRoutingSender(b), NextHopSelection: relayNextHopRoutingTable,
						HopAckRemainingTicks: defaultHopAckBudgetSeconds, FrameLine: "replacement-payload",
					})
				} else if !svc.relayStates.recordFailoverRetry("rerouted", b.address, sessionRoutingSender(b), "replacement-payload") {
					t.Fatal("failover fixture did not replace the attempt")
				}
				ack := protocol.Frame{Type: "relay_hop_ack", ID: "rerouted", Status: "forwarded"}
				svc.handleRelayHopAck(a.address, sessionRoutingSender(a), ack)
				svc.relayStates.mu.Lock()
				state := *svc.relayStates.states["rerouted"]
				svc.relayStates.mu.Unlock()
				if state.HopAckObserved || state.HopAckRemainingTicks != defaultHopAckBudgetSeconds || state.FrameLine != "replacement-payload" {
					t.Fatalf("late old-identity ack consumed replacement: %+v", state)
				}
				if state.abandonedAckCount != 1 || state.abandonedAckIdentities[0] != idPeerA {
					t.Fatal("reroute did not retain old actual identity")
				}
				svc.handleRelayHopAck(b.address, sessionRoutingSender(b), ack)
				svc.relayStates.mu.Lock()
				state = *svc.relayStates.states["rerouted"]
				svc.relayStates.mu.Unlock()
				if !state.HopAckObserved || state.FrameLine != "" {
					t.Fatal("current identity did not consume live timer payload")
				}
				seenA, seenB := false, false
				for _, route := range svc.routingTable.Lookup(idTargetX) {
					if route.NextHop == idPeerA {
						seenA = route.Source == routing.RouteSourceAnnouncement
					}
					if route.NextHop == idPeerB {
						seenB = route.Source == routing.RouteSourceHopAck
					}
				}
				if !seenA || !seenB {
					t.Fatal("stale/current route credit was lost or misattributed")
				}
			})
		}
	}
}

func TestHopAckCreditsCapturedIdentityAcrossAddressReplacement(t *testing.T) {
	for _, differentSession := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-identity-other-session-%v", differentSession), func(t *testing.T) {
			svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
			svc.relayStates = newRelayStateStore()
			original := &peerSession{address: "shared-address", peerIdentity: idPeerA}
			actual := original
			if differentSession {
				actual = &peerSession{address: "another-session", peerIdentity: idPeerA}
			}
			for _, id := range []domain.PeerIdentity{idPeerA, idPeerB} {
				status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
					Identity: idTargetX, Origin: idTargetX, NextHop: id,
					Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement, ExpiresAt: time.Now().Add(time.Hour),
				})
				if err != nil || status != routing.RouteAccepted {
					t.Fatalf("route fixture: %v %v", status, err)
				}
			}
			svc.relayStates.store(&relayForwardState{
				MessageID: "replacement-address", Recipient: idTargetX, ForwardedTo: original.address,
				ForwardedVia: sessionRoutingSender(original), NextHopSelection: relayNextHopRoutingTable,
				HopAckRemainingTicks: 5, FrameLine: "held",
			})
			// The registry changes after the actual incoming sender was captured.
			svc.sessions[original.address] = &peerSession{address: original.address, peerIdentity: idPeerB}
			svc.handleRelayHopAck(actual.address, sessionRoutingSender(actual), protocol.Frame{
				Type: "relay_hop_ack", ID: "replacement-address", Status: "delivered",
			})
			if !observedHopAck(t, svc, "replacement-address") {
				t.Fatal("current identity was tied to a mutable address or exact session")
			}
			seenA, seenB := false, false
			for _, route := range svc.routingTable.Lookup(idTargetX) {
				if route.NextHop == idPeerA {
					seenA = route.Source == routing.RouteSourceHopAck
				}
				if route.NextHop == idPeerB {
					seenB = route.Source == routing.RouteSourceAnnouncement
				}
			}
			if !seenA || !seenB {
				t.Fatal("ack was attributed to replacement address holder instead of actual captured identity")
			}
		})
	}
}

func TestHopAckUnpinnedHistoryRestrictsOnlyOtherIdentities(t *testing.T) {
	for _, history := range []string{"complete", "unknown", "overflow"} {
		t.Run(history, func(t *testing.T) {
			for _, selection := range []relayNextHopSelection{relayNextHopDirectSession, relayNextHopGossipFanOut, relayNextHopFailover} {
				rs := newRelayStateStore()
				first := routingSender{identity: idPeerA}
				if history == "unknown" {
					first = routingSender{}
				}
				rs.store(&relayForwardState{MessageID: "history", ForwardedTo: "old", ForwardedVia: first, Recipient: idTargetX,
					NextHopSelection: selection, HopAckRemainingTicks: 5, FrameLine: "old"})
				if history == "overflow" {
					for i := 0; i < maxAbandonedHopAckIdentities; i++ {
						id := domain.PeerIdentity{byte(i + 1)}
						if !rs.recordFailoverRetry("history", domain.PeerAddress(fmt.Sprintf("hop-%d", i)), routingSender{identity: id}, "payload") {
							t.Fatal("history fixture lost state")
						}
					}
				}
				rs.store(&relayForwardState{MessageID: "history", ForwardedTo: "current", ForwardedVia: routingSender{identity: idPeerB}, Recipient: idTargetX,
					NextHopSelection: selection, HopAckRemainingTicks: 5, FrameLine: "payload"})
				if _, ok := rs.consumeHopAck("history", idPeerA); ok {
					t.Fatal("captured abandoned identity was accepted")
				}
				_, other := rs.consumeHopAck("history", idPeerC)
				if other != (history == "complete") {
					t.Fatalf("broader unpinned acceptance=%v history=%s", other, history)
				}
				// Use a fresh live timer for the current-sender assertion.
				rs.mu.Lock()
				state := rs.states["history"]
				count, incomplete := state.abandonedAckCount, state.abandonedAckIncomplete
				state.HopAckObserved, state.FrameLine = false, "current-payload"
				rs.mu.Unlock()
				if count > maxAbandonedHopAckIdentities {
					t.Fatal("history exceeded fixed bound")
				}
				if history != "complete" && !incomplete {
					t.Fatal("incomplete history was silently called complete")
				}
				recipient, ok := rs.consumeHopAck("history", idPeerB)
				if !ok || recipient != idTargetX {
					t.Fatal("known current identity was blocked by incomplete history")
				}
			}
		})
	}
	// A current identity may occur in captured history after A→B→A. It is
	// still current; the wire supplies no distinct attempt or session token.
	rs := newRelayStateStore()
	rs.store(&relayForwardState{MessageID: "returned", ForwardedTo: "A", ForwardedVia: routingSender{identity: idPeerA}, Recipient: idTargetX, NextHopSelection: relayNextHopRoutingTable})
	rs.recordFailoverRetry("returned", "B", routingSender{identity: idPeerB}, "B")
	rs.recordFailoverRetry("returned", "A2", routingSender{identity: idPeerA}, "A2")
	rs.recordFailoverRetry("returned", "A3", routingSender{identity: idPeerA}, "A3")
	rs.mu.Lock()
	count := rs.states["returned"].abandonedAckCount
	rs.mu.Unlock()
	if count != 2 {
		t.Fatal("duplicate old identity consumed another history slot")
	}
	if _, ok := rs.consumeHopAck("returned", idPeerA); !ok {
		t.Fatal("known current identity was rejected merely because it was previously abandoned")
	}
	rs.store(&relayForwardState{MessageID: "unknown-pinned", ForwardedTo: "queued", Recipient: idTargetX, NextHopSelection: relayNextHopRoutingTable, FrameLine: "held"})
	if _, ok := rs.consumeHopAck("unknown-pinned", idPeerA); ok {
		t.Fatal("unknown pinned send-time identity accepted a named ack")
	}
	if _, ok := rs.consumeHopAck("missing", idPeerA); ok {
		t.Fatal("unknown message accepted an ack")
	}
	if _, ok := rs.consumeHopAck("unknown-pinned", domain.PeerIdentity{}); ok {
		t.Fatal("zero actual identity accepted an ack")
	}
}

func TestHopAckConsumeBeforeRerouteRearmsNewAttempt(t *testing.T) {
	rs := newRelayStateStore()
	rs.store(&relayForwardState{MessageID: "ordered", ForwardedTo: "A", ForwardedVia: routingSender{identity: idPeerA}, Recipient: idTargetX,
		NextHopSelection: relayNextHopRoutingTable, HopAckRemainingTicks: 5, FrameLine: "A"})
	if _, ok := rs.consumeHopAck("ordered", idPeerA); !ok {
		t.Fatal("current ack was refused")
	}
	if !rs.recordFailoverRetry("ordered", "B", routingSender{identity: idPeerB}, "B") {
		t.Fatal("subsequent reroute lost state")
	}
	if _, ok := rs.consumeHopAck("ordered", idPeerA); ok {
		t.Fatal("old identity consumed an attempt created after its earlier consume")
	}
	rs.mu.Lock()
	state := *rs.states["ordered"]
	rs.mu.Unlock()
	if state.HopAckObserved || state.HopAckRemainingTicks != defaultHopAckBudgetSeconds || state.FrameLine != "B" {
		t.Fatal("consume-before-reroute left replacement timer or payload suppressed")
	}
}
