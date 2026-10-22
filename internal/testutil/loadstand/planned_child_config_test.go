package loadstand

import (
	"net/netip"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func plannedChildTestConfig() (ChildConfig, PlannedChildConfig) {
	c := ChildConfig{DataDir: "/private-own/member", Listen: "240.70.1.2:20333", RPC: "127.0.0.1:20334", Bootstrap: []domain.PeerAddress{"240.70.2.2:20333"}, Listener: true, Procs: 2}
	p := PlannedChildConfig{ID: "full-a", Role: MemberFull, Prefix: netip.MustParsePrefix("240.70.0.0/16"), Peers: []PlannedChildPeer{{ID: "full-a", Role: MemberFull, Endpoint: "240.70.1.2:20333"}, {ID: "full-b", Role: MemberFull, Endpoint: "240.70.2.2:20333"}, {ID: "sdk-c", Role: MemberSDK, Endpoint: "240.70.3.2:20333"}}}
	return c, p
}
func TestPlannedChildOriginalPeerProjectionAndIndependentCopy(t *testing.T) {
	c, p := plannedChildTestConfig()
	child, plan, err := copyPlannedChild(c, p)
	if err != nil {
		t.Fatal(err)
	}
	c.Bootstrap[0] = "8.8.8.8:20333"
	p.Peers[1].Endpoint = "1.1.1.1:20333"
	if child.Bootstrap[0] != "240.70.2.2:20333" || plan.Peers[1].Endpoint != "240.70.2.2:20333" {
		t.Fatal("mutable caller plan escaped")
	}
	if child.ValidateLoopback() == nil {
		t.Fatal("planned path weakened original loopback validator")
	}
}
func TestPlannedChildRefusesEmptyForeignAndSDKBootstrap(t *testing.T) {
	for _, mode := range []string{"empty", "public", "RFC1918", "sdk", "own", "duplicate", "wrong-role", "wrong-own-endpoint", "duplicate-address", "nonloopback-rpc", "full-no-listener"} {
		t.Run(mode, func(t *testing.T) {
			c, p := plannedChildTestConfig()
			switch mode {
			case "empty":
				c.Bootstrap = nil
			case "public":
				c.Bootstrap = []domain.PeerAddress{"8.8.8.8:20333"}
			case "RFC1918":
				c.Listen = "10.70.1.2:20333"
			case "sdk":
				c.Bootstrap = []domain.PeerAddress{"240.70.3.2:20333"}
			case "own":
				c.Bootstrap = []domain.PeerAddress{"240.70.1.2:20333"}
			case "duplicate":
				c.Bootstrap = append(c.Bootstrap, c.Bootstrap[0])
			case "wrong-role":
				p.Role = MemberSDK
			case "wrong-own-endpoint":
				p.Peers[0].Endpoint = "240.70.1.3:20333"
			case "duplicate-address":
				p.Peers[2].Endpoint = "240.70.2.2:20334"
			case "nonloopback-rpc":
				c.RPC = "240.70.1.2:20334"
			case "full-no-listener":
				c.Listener = false
			}
			if _, _, err := copyPlannedChild(c, p); err == nil {
				t.Fatal("unplanned child accepted", mode)
			}
		})
	}
}

func TestPlannedSDKIsOriginalNoListenerRole(t *testing.T) {
	c, p := plannedChildTestConfig()
	c.Listen = "240.70.3.2:20333"
	c.Listener = false
	p.ID = "sdk-c"
	p.Role = MemberSDK
	if _, _, err := copyPlannedChild(c, p); err != nil {
		t.Fatal("original plannedSDK no-listener refused", err)
	}
	c.Listener = true
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("plannedSDK listener enabled")
	}
}
