package node

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
)

func TestK3PeerDirectoryMatchesActiveRPCThroughRecordForgetAndEvict(t *testing.T) {
	s, id, hello := k3InboundCoverageFixture(t)
	s.setConnAuthStateByID(id, &connauth.State{Hello: hello, Verified: true})
	var history []k3AdvertisedSnapshot
	check := func(want domain.PeerIdentity) k3AdvertisedPeerFact {
		t.Helper()
		s.rebuildPeerHealthSnapshot()
		raw, err := s.ActiveConnectionsJSON()
		if err != nil {
			t.Fatal(err)
		}
		var active struct {
			Connections []struct {
				ConnID   domain.ConnID `json:"conn_id"`
				Identity string        `json:"identity"`
			} `json:"connections"`
		}
		if err := json.Unmarshal(raw, &active); err != nil {
			t.Fatal(err)
		}
		snapshot := advertisedActualSnapshot(t, s)
		fact := advertisedFactFor(t, snapshot, id, "inbound")
		if len(active.Connections) != 1 || active.Connections[0].ConnID != id ||
			domain.PeerIdentityFromWire(active.Connections[0].Identity) != want ||
			fact.DirectoryIdentity != want || fact.Identity != idPeerB {
			t.Fatal("raw claim and directory identity were not retained separately", active, fact)
		}
		if n := len(history); n > 0 && snapshot.CoveredSequence != history[n-1].CoveredSequence+1 {
			t.Fatal("directory mutation did not publish exactly one fact", snapshot)
		}
		history = append(history, snapshot)
		return fact
	}
	address := check(domain.PeerIdentity{}).Address
	s.addPeerID(address, claimedAddressIdentity(idPeerB))
	check(idPeerB)
	s.addPeerID(address, claimedAddressIdentity(idTargetX))
	check(idTargetX)
	s.forgetClaimedAddressesOf(idTargetX)
	check(domain.PeerIdentity{})
	s.addPeerID(address, claimedAddressIdentity(idPeerB))
	check(idPeerB)

	// Age the original metadata entry while its inbound-only traffic owner
	// still contributes. The actual eviction routine performs the deletion.
	s.peerMu.Lock()
	s.metaOrphanFirstSeen = map[domain.PeerAddress]time.Time{
		address: time.Now().Add(-2 * orphanedHealthEvictWindow),
	}
	s.lastMetaOrphanSweep = time.Time{}
	s.peerMu.Unlock()
	s.evictOrphanedPeerMetadata()
	check(domain.PeerIdentity{})

}
