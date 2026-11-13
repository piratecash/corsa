package node

import (
	"encoding/json"
	"net"
	"slices"
	"testing"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func k3InboundCoverageFixture(t *testing.T) (*Service, domain.ConnID, protocol.Frame) {
	t.Helper()
	clock := newK3ActualClock()
	request := advertisedActualRequest(clock.read, 32, 8, 256)
	s := advertisedActualService(t, &request)
	id, hello := k3AddInboundCoverageOwner(t, s)
	return s, id, hello
}

func k3AddInboundCoverageOwner(t *testing.T, s *Service) (domain.ConnID, protocol.Frame) {
	t.Helper()
	wire := newNoIOConn(t, &net.TCPAddr{IP: net.IPv4(192, 0, 2, 9), Port: 5001})
	meter, err := netcore.NewMeteredConn(wire, &s.transportTotals)
	if err != nil {
		t.Fatal(err)
	}
	id := s.allocateConnID("k3_inbound_coverage", "192.0.2.9:64646")
	core := newTestNetCore(t, id, meter, netcore.Inbound, netcore.Options{})
	s.peerMu.Lock()
	s.registerInboundConnLocked(meter, core, meter, nil)
	s.peerMu.Unlock()
	t.Cleanup(func() { core.Close(); registryUnregister(s, meter) })
	hello := protocol.Frame{Address: idPeerB.String(), Version: 31}
	s.setConnAuthStateByID(id, &connauth.State{Hello: hello})
	s.rememberConnPeerAddr(id, hello, "192.0.2.9:5001")
	return id, hello
}

func k3InboundActiveIDs(t *testing.T, s *Service) []domain.ConnID {
	t.Helper()
	s.rebuildPeerHealthSnapshot()
	raw, err := s.ActiveConnectionsJSON()
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Connections []struct {
			ConnID domain.ConnID `json:"conn_id"`
		} `json:"connections"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	ids := make([]domain.ConnID, 0, len(result.Connections))
	for _, row := range result.Connections {
		ids = append(ids, row.ConnID)
	}
	slices.Sort(ids)
	return ids
}

func k3InboundActiveID(t *testing.T, s *Service) domain.ConnID {
	t.Helper()
	ids := k3InboundActiveIDs(t, s)
	switch len(ids) {
	case 0:
		return 0
	case 1:
		return ids[0]
	default:
		t.Fatal("fixture has unexpected active connections", ids)
		return 0
	}
}
