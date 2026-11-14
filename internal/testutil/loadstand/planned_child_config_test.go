package loadstand

import (
	"net/netip"
	"testing"
	"time"

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

func TestPlannedRouteWireRequiresExplicitBoundFullNode(t *testing.T) {
	c, p := plannedChildTestConfig()
	p.RouteWire = &PlannedRouteWireObservation{Window: LaunchNonce{1}, MaxEndpoints: 3, Bounds: RouteWireWindowBounds{Begin: 100, End: 200}}
	_, copied, err := copyPlannedChild(c, p)
	if err != nil || copied.RouteWire == nil || !samePlannedChild(copied, p) {
		t.Fatalf("valid FullNode observation refused: %v", err)
	}
	p.RouteWire.Bounds.End = p.RouteWire.Bounds.Begin
	if _, _, err := copyPlannedChild(c, p); err == nil || samePlannedChild(copied, p) {
		t.Fatal("nonpositive duration or changed bounds were accepted")
	}
	p.RouteWire.Bounds = copied.RouteWire.Bounds
	p.RouteWire.MaxEndpoints = 0
	if copied.RouteWire.MaxEndpoints != 3 || samePlannedChild(copied, p) {
		t.Fatal("caller mutation escaped into the selected observation")
	}
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("observation without endpoint budget was accepted")
	}
	p.RouteWire = &PlannedRouteWireObservation{MaxEndpoints: 3}
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("observation without a window nonce was accepted")
	}
	p.RouteWire.Window = LaunchNonce{1}
	c.Listen, c.Listener = "240.70.3.2:20333", false
	p.ID, p.Role = "sdk-c", MemberSDK
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("FullNode observation was accepted for SDK")
	}
}

func TestPlannedRouteWireFullReceiptBudget(t *testing.T) {
	c, p := plannedChildTestConfig()
	p.RouteWire = &PlannedRouteWireObservation{
		Window: LaunchNonce{1}, MaxEndpoints: 2, Bounds: RouteWireWindowBounds{Begin: 100, End: 200},
		FullReceiptRouteBudget: 20_000,
	}
	_, copied, err := copyPlannedChild(c, p)
	if err != nil || !samePlannedChild(copied, p) {
		t.Fatal("bounded receipt plan refused", err)
	}
	p.RouteWire.FullReceiptRouteBudget++
	if _, _, err := copyPlannedChild(c, p); err == nil || samePlannedChild(copied, p) {
		t.Fatal("receipt budget escaped the endpoint bound or immutable copy")
	}
	p.RouteWire.FullReceiptRouteBudget = 10_000
	p.RouteWire.Bounds = RouteWireWindowBounds{}
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("receipt plan admitted without a timed window")
	}
}

func TestPlannedRouteWireSeedRequiresBoundedTwoFullMemberPlan(t *testing.T) {
	c, p := plannedChildTestConfig()
	p.RouteWire = &PlannedRouteWireObservation{
		Window: LaunchNonce{1}, MaxEndpoints: 2, Bounds: RouteWireWindowBounds{Begin: 100, End: 200},
		FullReceiptRouteBudget: 10_000, SeedRoutes: 10_000, DisconnectAt: 150, DialRole: domain.RouteWireDialer,
	}
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("fixture was accepted with a third participant")
	}
	p.Peers = p.Peers[:2]
	_, copied, err := copyPlannedChild(c, p)
	if err != nil || !samePlannedChild(copied, p) {
		t.Fatal("two-member fixture was rejected", err)
	}
	for _, invalid := range []int{-1, 10_001} {
		p.RouteWire.SeedRoutes = invalid
		if _, _, err := copyPlannedChild(c, p); err == nil || samePlannedChild(copied, p) {
			t.Fatalf("accepted invalid size or mutated original plan: %d", invalid)
		}
	}
	p.RouteWire.SeedRoutes = 10_000
	for _, at := range []time.Duration{99, 100, 200} {
		p.RouteWire.DisconnectAt = at
		if _, _, err := copyPlannedChild(c, p); err == nil || samePlannedChild(copied, p) {
			t.Fatalf("accepted disconnect outside window or mutated original plan: %d", at)
		}
	}
	p.RouteWire.DisconnectAt = 150
	p.RouteWire.DialRole = domain.RouteWireListener
	if _, _, err := copyPlannedChild(c, p); err == nil {
		t.Fatal("listener accepted the initiator's disconnect action")
	}
	p.RouteWire.DisconnectAt = 0
	if _, _, err := copyPlannedChild(c, p); err != nil {
		t.Fatal("listener without a disconnect action was refused", err)
	}
}
