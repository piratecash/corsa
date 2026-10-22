//go:build linux

package loadstand

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"net/netip"
	"strconv"
	"testing"
)

func TestPlannedLaunchBindsRoleOrdinalAndOriginalNetworkGraph(t *testing.T) {
	c, p := plannedChildTestConfig()
	networkPlan := plannedTestPlan()
	networkPlan.members = nil
	for i, peer := range p.Peers {
		address, _ := netip.ParseAddrPort(string(peer.Endpoint))
		networkPlan.members = append(networkPlan.members, plannedNetworkMember{id: peer.ID, role: peer.Role, endpoint: peer.Endpoint, hostLink: "h" + strconv.Itoa(i), peerLink: "p" + strconv.Itoa(i), address: address.Addr()})
	}
	member := networkPlan.members[0]
	backend := &linuxPlannedNetworkBackend{plan: networkPlan}
	namespace := newOwnedNetNSState()
	row, err := backend.begin(plannedNetworkOperation{kind: plannedNamespace, plan: networkPlan, member: member})
	if err != nil {
		t.Fatal(err)
	}
	row.namespace = namespace
	network := &plannedNetwork{plan: networkPlan, backend: backend, ready: true}
	launch, err := newPlannedLaunchBinding(c, p, "binary-stamp", 1)
	if err != nil {
		t.Fatal(err)
	}
	selected := &linuxPlannedLaunch{network: network, member: member, plan: p, ordinal: 1, binding: launch}
	spec := LinuxFixtureSpec{Config: c, TargetSHA256: "binary-stamp", planned: selected}
	if !selected.configValid(spec, namespace) {
		t.Fatal("exact private construction graph refused")
	}
	foreign := newOwnedNetNSState()
	if selected.configValid(spec, foreign) {
		t.Fatal("foreign namespace substituted")
	}
	copy := *selected
	copy.ordinal++
	spec.planned = &copy
	if copy.configValid(spec, namespace) {
		t.Fatal("ordinal not bound")
	}
	copy = *selected
	copy.plan.Role = MemberSDK
	spec.planned = &copy
	if copy.configValid(spec, namespace) {
		t.Fatal("role not bound")
	}
	for _, mode := range []string{"missing-peer", "renamed-peer", "wrong-role", "wrong-address"} {
		bad := *selected
		bad.plan = selected.plan
		bad.plan.Peers = append([]PlannedChildPeer(nil), selected.plan.Peers...)
		badSpec := spec
		badSpec.Config = c
		badSpec.Config.Bootstrap = append([]domain.PeerAddress(nil), c.Bootstrap...)
		switch mode {
		case "missing-peer":
			bad.plan.Peers[1].ID = "foreign"
		case "renamed-peer":
			bad.plan.Peers[1].ID = "renamed"
		case "wrong-role":
			bad.plan.Peers[2].Role = MemberFull
		case "wrong-address":
			bad.plan.Peers[1].Endpoint = "240.70.9.9:20333"
			badSpec.Config.Bootstrap[0] = bad.plan.Peers[1].Endpoint
		}
		if _, _, err := copyPlannedChild(badSpec.Config, bad.plan); err != nil {
			t.Fatal("control failed before originalgraph check", mode, err)
		}
		bad.binding, err = newPlannedLaunchBinding(badSpec.Config, bad.plan, spec.TargetSHA256, bad.ordinal)
		if err != nil {
			t.Fatal(err)
		}
		if bad.configValid(badSpec, namespace) {
			t.Fatal("foreign projectedpeer graph accepted", mode)
		}
	}
	network.ready = false
	if selected.configValid(spec, namespace) {
		t.Fatal("unprepared isolation selected")
	}
}
