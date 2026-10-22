package loadstand

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"testing"
)

type plannedNetworkTrace struct {
	kind   plannedNetworkKind
	member MemberID
}
type plannedNetworkFake struct {
	inventory                 plannedNetworkInventory
	applied                   []plannedNetworkKind
	live                      map[plannedNetworkHandle]bool
	removed                   []plannedNetworkHandle
	failAt                    int
	partial                   bool
	foreign                   bool
	wrongMember               bool
	removeError               bool
	verifyError               bool
	removeCalls, failRemoveAt int
	cancelAfterCreate         context.CancelFunc
	cancelAt                  int
	trace                     []plannedNetworkTrace
}

func plannedTestPlan() plannedNetworkPlan {
	var nonce LaunchNonce
	nonce[0] = 1
	return plannedNetworkPlan{nonce: nonce, bridge: "ownbr", prefix: netip.MustParsePrefix("240.70.0.0/16"), members: []plannedNetworkMember{{id: "full-a", hostLink: "hosta", peerLink: "peera", address: netip.MustParseAddr("240.70.1.2")}, {id: "sdk-b", hostLink: "hostb", peerLink: "peerb", address: netip.MustParseAddr("240.70.2.2")}}}
}
func plannedTestBackend() *plannedNetworkFake {
	return &plannedNetworkFake{inventory: plannedNetworkInventory{networkNone: true, noForbiddenCapabilities: true, noBlockDevices: true, links: []plannedNetworkLink{{name: "lo", index: 1}}, routes: []plannedNetworkRoute{{prefix: netip.MustParsePrefix("127.0.0.0/8")}}}, live: map[plannedNetworkHandle]bool{}}
}
func (f *plannedNetworkFake) inspect(context.Context) (plannedNetworkInventory, error) {
	return f.inventory, nil
}
func (f *plannedNetworkFake) apply(_ context.Context, op plannedNetworkOperation) (plannedNetworkHandle, error) {
	f.applied = append(f.applied, op.kind)
	f.trace = append(f.trace, plannedNetworkTrace{op.kind, op.member.id})
	fail := f.failAt == len(f.applied)
	if fail && !f.partial {
		return plannedNetworkHandle{}, errPlannedNetwork
	}
	var handle plannedNetworkHandle
	switch op.kind {
	case plannedBridge, plannedFirewall:
		handle = plannedNetworkHandle{kind: op.kind, name: op.plan.bridge, identity: uint64(len(f.applied)), nonce: op.plan.nonce}
	case plannedNamespace:
		handle = plannedNetworkHandle{kind: op.kind, name: string(op.member.id), identity: uint64(len(f.applied)), nonce: op.plan.nonce}
	case plannedVeth:
		handle = plannedNetworkHandle{kind: op.kind, name: op.member.hostLink, identity: uint64(len(f.applied)), nonce: op.plan.nonce}
	}
	if op.kind == plannedNamespace && f.wrongMember {
		handle.name = string(op.plan.members[1].id)
	}
	if !handle.empty() {
		if f.foreign {
			handle.nonce[0]++
		} else {
			f.live[handle] = true
		}
	}
	if f.cancelAfterCreate != nil && f.cancelAt == len(f.applied) && !handle.empty() {
		f.cancelAfterCreate()
		return handle, context.Canceled
	}
	if fail {
		return handle, errPlannedNetwork
	}
	return handle, nil
}
func (f *plannedNetworkFake) verify(context.Context, plannedNetworkPlan) error {
	if f.verifyError {
		return errPlannedNetwork
	}
	return nil
}
func (f *plannedNetworkFake) remove(_ context.Context, h plannedNetworkHandle) error {
	f.removeCalls++
	if f.removeError || f.removeCalls == f.failRemoveAt || !f.live[h] {
		return errPlannedNetwork
	}
	delete(f.live, h)
	f.removed = append(f.removed, h)
	return nil
}
func TestPlannedNetworkRefusesForeignStateBeforeMutation(t *testing.T) {
	for _, kind := range []string{"default-v4", "default-v6", "loopback-overwide", "loopback-noncanonical", "foreign-link", "wrong-container", "raw-cap", "block-device"} {
		t.Run(kind, func(t *testing.T) {
			f := plannedTestBackend()
			switch kind {
			case "default-v4":
				f.inventory.routes = append(f.inventory.routes, plannedNetworkRoute{netip.MustParsePrefix("0.0.0.0/0")})
			case "default-v6":
				f.inventory.routes = append(f.inventory.routes, plannedNetworkRoute{netip.MustParsePrefix("::/0")})
			case "loopback-overwide":
				f.inventory.routes = append(f.inventory.routes, plannedNetworkRoute{netip.MustParsePrefix("127.0.0.0/1")})
			case "loopback-noncanonical":
				f.inventory.routes = append(f.inventory.routes, plannedNetworkRoute{netip.MustParsePrefix("127.1.2.3/8")})
			case "foreign-link":
				f.inventory.links = append(f.inventory.links, plannedNetworkLink{name: "foreign", index: 2})
			case "wrong-container":
				f.inventory.networkNone = false
			case "raw-cap":
				f.inventory.noForbiddenCapabilities = false
			case "block-device":
				f.inventory.noBlockDevices = false
			}
			if owner, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f); err == nil || owner != nil || len(f.applied) != 0 {
				t.Fatal("foreign state created ownership/mutation", owner, err)
			}
		})
	}
}
func TestPlannedNetworkPartialSetupRetainsOnlyExactOwners(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for fail := 1; fail <= 12; fail++ {
			f := plannedTestBackend()
			f.failAt, f.partial = fail, partial
			n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
			if err != nil {
				t.Fatal(err)
			}
			if err = n.setup(t.Context()); err == nil || n.ready {
				t.Fatal("fault setup accepted", fail)
			}
			kept := append([]plannedNetworkHandle(nil), n.owned...)
			if err = n.close(t.Context()); err != nil {
				t.Fatal("own cleanup", fail, err)
			}
			if len(f.live) != 0 || len(n.owned) != 0 || !n.closed {
				t.Fatal("partial resources leaked", fail)
			}
			for i, h := range kept {
				if f.removed[len(kept)-1-i] != h {
					t.Fatal("cleanup not reverse original ownership")
				}
			}
		}
	}
}
func TestPlannedNetworkCopiedPlanAndChangedPreflight(t *testing.T) {
	p := plannedTestPlan()
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), p, f)
	if err != nil {
		t.Fatal(err)
	}
	p.members[0].hostLink = "rewritten"
	if n.plan.members[0].hostLink != "hosta" {
		t.Fatal("borrowed mutable plan")
	}
	f.inventory.routes = append(f.inventory.routes, plannedNetworkRoute{netip.MustParsePrefix("0.0.0.0/0")})
	if err = n.setup(t.Context()); err == nil || len(f.applied) != 0 {
		t.Fatal("stale constructor preflight authorized mutation")
	}
}
func TestPlannedNetworkForeignHandleNeverDeleted(t *testing.T) {
	f := plannedTestBackend()
	f.foreign = true
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err == nil || len(n.owned) != 0 {
		t.Fatal("foreign resource gained owner")
	}
	if err = n.close(t.Context()); err != nil || len(f.removed) != 0 {
		t.Fatal("foreign resource deleted", err)
	}
}
func TestPlannedNetworkCleanupFailureRetainsOriginalHandle(t *testing.T) {
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := append([]plannedNetworkHandle(nil), n.owned...)
	f.removeError = true
	if err = n.close(t.Context()); err == nil || n.closed || !reflect.DeepEqual(n.owned, before) {
		t.Fatal("unconfirmed cleanup lost ownership")
	}
	f.removeError = false
	if err = n.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	count := len(f.removed)
	if err = n.close(t.Context()); err != nil || len(f.removed) != count {
		t.Fatal("idempotent close deleted again")
	}
}
func TestPlannedNetworkCancelledSetupNeedsOwnCleanupContext(t *testing.T) {
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	f.failAt = 3
	f.partial = true
	if err = n.setup(t.Context()); err == nil {
		t.Fatal("fault not reached")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = n.close(ctx); !errors.Is(err, context.Canceled) || len(n.owned) == 0 {
		t.Fatal("cancelled cleanup erased ownership", err)
	}
	if err = n.close(t.Context()); err != nil || len(f.live) != 0 {
		t.Fatal("fresh own cleanup failed", err)
	}
}

func TestPlannedNetworkVerifyRefusalRetainsResourcesAndCannotRetry(t *testing.T) {
	f := plannedTestBackend()
	f.verifyError = true
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err == nil || n.ready || len(n.owned) == 0 {
		t.Fatal("verify refusal lost owners or admitted network")
	}
	calls := len(f.applied)
	f.verifyError = false
	if err = n.setup(t.Context()); err == nil || len(f.applied) != calls {
		t.Fatal("failed original setup silently retried")
	}
	if err = n.close(t.Context()); err != nil || len(f.live) != 0 {
		t.Fatal("verify failure own cleanup", err)
	}
}

func TestPlannedNetworkInputDoesNotBroadenPrefixOrNamespacePath(t *testing.T) {
	for _, kind := range []string{"overwide-approved-prefix", "namespace-path", "namespace-flag"} {
		p := plannedTestPlan()
		f := plannedTestBackend()
		switch kind {
		case "overwide-approved-prefix":
			p.prefix = netip.MustParsePrefix("224.0.0.0/3")
		case "namespace-path":
			p.members[0].id = "../foreign"
		case "namespace-flag":
			p.members[0].id = "-foreign"
		}
		if n, err := newPlannedNetwork(t.Context(), p, f); err == nil || n != nil || len(f.applied) != 0 {
			t.Fatal("unsafe plan reached backend", kind)
		}
	}
}
func TestPlannedNetworkSameRunOtherMemberHandleRefusesThisOperation(t *testing.T) {
	f := plannedTestBackend()
	f.wrongMember = true
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err == nil || n.ready || len(f.applied) != 2 {
		t.Fatal("same-run wrong-member receipt admitted")
	}
	if len(n.owned) != 1 || n.owned[0].kind != plannedBridge {
		t.Fatal("foreign operation gained cleanup authority")
	}
	if err = n.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(f.removed) != 1 || f.removed[0].kind != plannedBridge {
		t.Fatal("wrong-member resource deleted")
	}
}
func TestPlannedNetworkKernelReplacementModelIsNotDeleted(t *testing.T) {
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	last := n.owned[len(n.owned)-1]
	delete(f.live, last)
	replacement := last
	replacement.identity++
	replacement.nonce[0]++
	f.live[replacement] = true
	if err = n.close(t.Context()); err == nil || n.closed || !f.live[replacement] || len(f.removed) != 0 || n.owned[len(n.owned)-1] != last {
		t.Fatal("same-name replacement deleted or ownership lost")
	}
}

func TestPlannedNetworkExactMemberOperationOrder(t *testing.T) {
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []plannedNetworkTrace{{plannedBridge, ""}, {plannedNamespace, "full-a"}, {plannedVeth, "full-a"}, {plannedAttach, "full-a"}, {plannedMove, "full-a"}, {plannedAddress, "full-a"}, {plannedNamespace, "sdk-b"}, {plannedVeth, "sdk-b"}, {plannedAttach, "sdk-b"}, {plannedMove, "sdk-b"}, {plannedAddress, "sdk-b"}, {plannedFirewall, ""}}
	if !reflect.DeepEqual(f.trace, want) {
		t.Fatal("setup crossed member/order boundary", f.trace)
	}
	if err = n.close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestPlannedNetworkSecondRemovalFailureRetriesOnlyRetainedOwners(t *testing.T) {
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := append([]plannedNetworkHandle(nil), n.owned...)
	f.failRemoveAt = 2
	if err = n.close(t.Context()); err == nil || len(f.removed) != 1 || !reflect.DeepEqual(n.owned, before[:len(before)-1]) {
		t.Fatal("partial cleanup did not retain exact remainder")
	}
	first := f.removed[0]
	if err = n.close(t.Context()); err != nil || len(f.live) != 0 || len(f.removed) != len(before) {
		t.Fatal("remaining own cleanup", err)
	}
	count := 0
	for _, h := range f.removed {
		if h == first {
			count++
		}
	}
	if count != 1 {
		t.Fatal("confirmed deletion replayed")
	}
}
func TestPlannedNetworkSetupCancelAfterCreationRetainsThatHandle(t *testing.T) {
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), plannedTestPlan(), f)
	if err != nil {
		t.Fatal(err)
	}
	setup, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.cancelAfterCreate = cancel
	f.cancelAt = 3
	if err = n.setup(setup); !errors.Is(err, context.Canceled) || n.ready || len(f.applied) != 3 || len(n.owned) != 3 {
		t.Fatal("post-create cancellation dropped handle or continued", err)
	}
	if err = n.close(setup); !errors.Is(err, context.Canceled) || len(n.owned) != 3 {
		t.Fatal("cancelled cleanup erased partial ownership", err)
	}
	if err = n.close(t.Context()); err != nil || len(f.live) != 0 {
		t.Fatal("fresh cleanup did not remove exact partial resources", err)
	}
}

func TestPlannedNetworkAcceptsApproved240RangeAndRejectsPrivateFallback(t *testing.T) {
	p := plannedTestPlan()
	p.prefix = netip.MustParsePrefix("240.0.0.0/4")
	f := plannedTestBackend()
	n, err := newPlannedNetwork(t.Context(), p, f)
	if err != nil {
		t.Fatal("owner-approved240/4 refused", err)
	}
	if err = n.setup(t.Context()); err != nil || !n.ready {
		t.Fatal("approved planned setup refused", err)
	}
	if err = n.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"10.70.1.2", "239.70.1.2", "2001:db8::1", "255.255.255.255"} {
		p := plannedTestPlan()
		p.prefix = netip.MustParsePrefix("240.0.0.0/4")
		p.members[0].address = netip.MustParseAddr(address)
		f := plannedTestBackend()
		if n, err := newPlannedNetwork(t.Context(), p, f); err == nil || n != nil || len(f.applied) != 0 {
			t.Fatal("unapproved member address reached backend", address)
		}
	}
	for _, prefix := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "0.0.0.0/0", "224.0.0.0/3", "8.0.0.0/8"} {
		p := plannedTestPlan()
		p.prefix = netip.MustParsePrefix(prefix)
		f := plannedTestBackend()
		if n, err := newPlannedNetwork(t.Context(), p, f); err == nil || n != nil || len(f.applied) != 0 {
			t.Fatal("unapproved range substituted for240/4", prefix)
		}
	}
}
