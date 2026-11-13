package node

import (
	"slices"
	"testing"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
)

func TestK3InboundCoverageRecordsTrafficTrust(t *testing.T) {
	s, id, hello := k3InboundCoverageFixture(t)
	before := advertisedActualSnapshot(t, s)
	fact := advertisedFactFor(t, before, id, "inbound")
	if fact.HealthPresent || fact.InboundMeteredTrusted || k3InboundActiveID(t, s) != 0 {
		t.Fatal("fixture should start with an inactive metered owner", fact)
	}
	s.setConnAuthStateByID(id, &connauth.State{Hello: hello, Verified: true})
	after := advertisedActualSnapshot(t, s)
	fact.InboundMeteredTrusted = true
	if k3InboundActiveID(t, s) != id || after.CoveredSequence != before.CoveredSequence+1 ||
		fact != advertisedFactFor(t, after, id, "inbound") {
		t.Fatal("auth publication did not record the original fallback owner", after)
	}
	s.setConnAuthStateByID(id, &connauth.State{Hello: hello})
	lost := advertisedActualSnapshot(t, s)
	if k3InboundActiveID(t, s) != 0 || lost.CoveredSequence != after.CoveredSequence+1 ||
		advertisedFactFor(t, lost, id, "inbound").InboundMeteredTrusted {
		t.Fatal("withdrawal did not record loss of fallback contribution", lost)
	}
	s.clearConnAuth(id)
	legacy := advertisedActualSnapshot(t, s)
	if k3InboundActiveID(t, s) != id || legacy.CoveredSequence != lost.CoveredSequence+1 ||
		!advertisedFactFor(t, legacy, id, "inbound").InboundMeteredTrusted {
		t.Fatal("legacy nil-auth fallback did not follow the original predicate", legacy)
	}
}

func TestK3InboundAddressFallbackIncludesOtherOwnerAndKeepsAlias(t *testing.T) {
	s, first, hello := k3InboundCoverageFixture(t)
	second, _ := k3AddInboundCoverageOwner(t, s)
	before := advertisedActualSnapshot(t, s)
	firstFact := advertisedFactFor(t, before, first, "inbound")
	secondFact := advertisedFactFor(t, before, second, "inbound")
	if firstFact.Address == "" || firstFact.Address != secondFact.Address ||
		firstFact.InboundRawHealthPresent || secondFact.InboundRawHealthPresent ||
		len(k3InboundActiveIDs(t, s)) != 0 {
		t.Fatal("fixture needs two inactive owners under one raw address", firstFact, secondFact)
	}
	s.setConnAuthStateByID(first, &connauth.State{Hello: hello, Verified: true})
	grouped := advertisedActualSnapshot(t, s)
	if !advertisedFactFor(t, grouped, first, "inbound").InboundMeteredTrusted ||
		advertisedFactFor(t, grouped, second, "inbound").InboundMeteredTrusted ||
		!slices.Equal(k3InboundActiveIDs(t, s), []domain.ConnID{first, second}) {
		t.Fatal("the first contribution should make both ConnIDs visible", grouped)
	}
	// Set the same alias shape the dial path records, without starting a
	// dial. Source and RPC use the original lock and their original readers.
	s.peerMu.Lock()
	ticket := s.beginK3AdvertisedPeerLocked()
	s.dialOrigin[firstFact.Address] = "192.0.2.9:64647"
	s.finishK3AdvertisedPeerLocked(ticket)
	s.peerMu.Unlock()
	aliased := advertisedActualSnapshot(t, s)
	row := advertisedFactFor(t, aliased, first, "inbound")
	if row.HealthAddress == row.Address || !row.InboundMeteredTrusted ||
		row.InboundRawHealthPresent || len(k3InboundActiveIDs(t, s)) != 0 {
		t.Fatal("resolved fallback must not silently re-key the inbound ConnIDs", aliased)
	}
}
