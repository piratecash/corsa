package node

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestOwnedFindNodeMixedV2KeepsPeerExchange(t *testing.T) {
	relay, address := newOwnedBareV2PilotRelay(t, 2, 5)
	target := dialOwnedBareV2PilotClient(t, relay, address, true)
	// Seed ordinary discovery readout with a documentation address. This entry
	// is not a dial candidate or an authenticated FIND_NODE member.
	const knownAddress domain.PeerAddress = "198.51.100.7:5050"
	relay.peerMu.Lock()
	relay.health[knownAddress] = &peerHealth{Address: knownAddress, Connected: true, Direction: peerDirectionInbound}
	relay.peerMu.Unlock()
	relay.rebuildPeersExchangeSnapshot()

	for _, advertise := range []bool{false, true} {
		id, conn := dialOwnedBareV2PilotWire(t, relay, address, advertise, net.Dialer{}, nil)
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		writeJSONFrame(t, conn, protocol.Frame{Type: "get_peers"})
		line, err := readFrameLine(reader, protocol.MaxFrameLine)
		if err != nil {
			t.Fatalf("get_peers with FIND_NODE advertised=%t: %v", advertise, err)
		}
		var response struct {
			Type  string   `json:"type"`
			Count *int     `json:"count"`
			Peers []string `json:"peers"`
		}
		if err := json.Unmarshal([]byte(line), &response); err != nil {
			t.Fatal(err)
		}
		if response.Type != "peers" || response.Count == nil || *response.Count != 1 ||
			len(response.Peers) != 1 || response.Peers[0] != string(knownAddress) {
			t.Fatalf("peer exchange changed with FIND_NODE advertised=%t: %s", advertise, line)
		}
		if advertise {
			bareV2PilotOriginalMembers(t, relay, target.id, domain.PeerIdentityFromWire(id.Address))
		} else {
			bareV2PilotOriginalMembers(t, relay, target.id)
		}
	}
}
