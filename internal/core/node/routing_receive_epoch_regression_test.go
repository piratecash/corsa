package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestRouteAnnounceHigherEpochRequiresFreshFullForEveryDelta(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)
	svc.handleRouteAnnounceV3(svc.provenRoutingSender(idPeerB), "", protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindFull, Epoch: 1})
	delta := protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindDelta, Epoch: 2, Entries: []protocol.RouteAnnounceV3Entry{{Identity: idTargetX.String(), Hops: 1, SeqNo: 1}}}
	for attempt := 0; attempt < 2; attempt++ {
		svc.handleRouteAnnounceV3(svc.provenRoutingSender(idPeerB), "", delta)
		if len(svc.routingTable.Lookup(idTargetX)) != 0 {
			t.Fatalf("delta%d inherited the prior epoch baseline", attempt+1)
		}
	}
	svc.handleRouteAnnounceV3(svc.provenRoutingSender(idPeerB), "", protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindFull, Epoch: 2})
	svc.handleRouteAnnounceV3(svc.provenRoutingSender(idPeerB), "", delta)
	if len(svc.routingTable.Lookup(idTargetX)) == 0 {
		t.Fatal("fresh epoch2 full did not re-enable epoch2 delta")
	}
}
