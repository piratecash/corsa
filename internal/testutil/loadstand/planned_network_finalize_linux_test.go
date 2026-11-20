//go:build linux

package loadstand

import (
	"context"
	"net/netip"
	"os"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func namespaceFinalizeFixture() (*linuxPlannedNamespaceOperation, linuxNamespaceIdentity, *multicastTestSocket) {
	p := plannedTestPlan()
	identity := linuxNamespaceIdentity{device: 17, inode: 29}
	op := &linuxPlannedNamespaceOperation{identity: identity, plan: p, member: p.members[0], mode: plannedNamespaceSetupFinalize, peerIndex: 3}
	socket := &multicastTestSocket{owners: []plannedKernelLink{{index: 1, name: "lo"}, {index: 3, name: op.member.peerLink, alias: plannedNetworkAlias(p.nonce)}}, rows: nil}
	return op, identity, socket
}

func TestPlannedNamespaceSetupFinalizerLateCarrierRoute(t *testing.T) {
	ctx := multicastTestContext(t)
	op, identity, socket := namespaceFinalizeFixture()
	// Initial member setup is clean; a later host/bridge carrier notification
	// creates precisely the kernel multicast route on the original peer.
	if err := reconcilePlannedMulticast(ctx, socket, socket.owners[1:]); err != nil || socket.deletes != 0 {
		t.Fatal("initial clean setup", err)
	}
	socket.rows = append(socket.rows, multicastTestRoute(3))
	readonly := *op
	readonly.mode = plannedNamespaceReadOnly
	before := []plannedNetworkRoute{{prefix: op.plan.prefix}, {prefix: netip.MustParsePrefix("ff00::/8")}}
	if validatePlannedNamespaceRoutes(&readonly, before, 3) == nil {
		t.Fatal("original readonly oracle did not observe the late route")
	}
	if err := finalizePlannedNamespaceSetup(ctx, socket, op, identity, false); err != nil || socket.deletes != 1 || len(socket.requests) != 1 || len(socket.rows) != 0 {
		t.Fatal("setup finalizer did not delete/readback precisely the original peer route", err, socket.events)
	}
	if multicastTestIndex(socket.requests[0]) != 3 {
		t.Fatal("foreign OIF in deletion")
	}
	if err := validatePlannedNamespaceRoutes(&readonly, []plannedNetworkRoute{{prefix: op.plan.prefix}}, 3); err != nil {
		t.Fatal("original strict readonly oracle did not accept the finalized state", err)
	}
}

func TestPlannedNamespaceSetupFinalizerRejectsForeignBinding(t *testing.T) {
	cases := []string{"claimed", "namespace", "nonce", "name", "index", "member", "readonly", "configure"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			op, identity, socket := namespaceFinalizeFixture()
			socket.rows = []syscall.NetlinkMessage{multicastTestRoute(3)}
			claimed := false
			switch name {
			case "claimed":
				claimed = true
			case "namespace":
				identity.inode++
			case "nonce":
				op.plan.nonce[0]++
			case "name":
				socket.owners[1].name = "foreign"
			case "index":
				op.peerIndex++
			case "member":
				op.member.address = netip.MustParseAddr("240.99.1.1")
			case "readonly":
				op.mode = plannedNamespaceReadOnly
			case "configure":
				op.mode = plannedNamespaceConfigure
			}
			if err := finalizePlannedNamespaceSetup(multicastTestContext(t), socket, op, identity, claimed); err == nil || socket.deletes != 0 {
				t.Fatal("foreign/replaced/claimed namespace authority mutated", err, socket.deletes)
			}
		})
	}
}

func TestPlannedNamespaceSetupFinalizerStrictDumpAndReadback(t *testing.T) {
	t.Run("foreign-oif-refused-before-any-delete", func(t *testing.T) {
		op, id, socket := namespaceFinalizeFixture()
		socket.rows = []syscall.NetlinkMessage{multicastTestRoute(3), multicastTestRoute(99)}
		if err := finalizePlannedNamespaceSetup(multicastTestContext(t), socket, op, id, false); err == nil || socket.deletes != 0 {
			t.Fatal("whole-dump refusal lost", err, socket.deletes)
		}
	})
	t.Run("unexpected-route-attribute", func(t *testing.T) {
		op, id, socket := namespaceFinalizeFixture()
		socket.rows = []syscall.NetlinkMessage{multicastTestAppend(multicastTestRoute(3), unix.RTA_GATEWAY, make([]byte, 16))}
		if err := finalizePlannedNamespaceSetup(multicastTestContext(t), socket, op, id, false); err == nil || socket.deletes != 0 {
			t.Fatal("unknown route authority accepted", err, socket.deletes)
		}
	})
	t.Run("failed-delete-readback", func(t *testing.T) {
		op, id, socket := namespaceFinalizeFixture()
		socket.rows = []syscall.NetlinkMessage{multicastTestRoute(3)}
		socket.keepDeleted = true
		if err := finalizePlannedNamespaceSetup(multicastTestContext(t), socket, op, id, false); err == nil || socket.deletes != 1 {
			t.Fatal("unsuccessful exact readback reported success", err, socket.deletes)
		}
	})
	t.Run("deadline-required", func(t *testing.T) {
		op, id, socket := namespaceFinalizeFixture()
		if err := finalizePlannedNamespaceSetup(context.Background(), socket, op, id, false); err != ErrLinuxOwnerBound || socket.deletes != 0 {
			t.Fatal("unbounded finalization", err)
		}
	})
}

func TestPlannedNamespaceReadonlyRouteOracleStillRejectsMulticast(t *testing.T) {
	op, _, _ := namespaceFinalizeFixture()
	op.mode = plannedNamespaceReadOnly
	for _, prefix := range []string{"ff00::/8", "0.0.0.0/0", "240.99.0.0/16"} {
		if err := validatePlannedNamespaceRoutes(op, []plannedNetworkRoute{{prefix: op.plan.prefix}, {prefix: netip.MustParsePrefix(prefix)}}, 3); err == nil {
			t.Fatal("readonly route oracle weakened", prefix)
		}
	}
	if validatePlannedNamespaceRoutes(op, nil, 3) == nil {
		t.Fatal("missing selected connected route accepted")
	}
}

func TestPlannedNamespaceSetupFinalizeSequenceSource(t *testing.T) {
	body, err := os.ReadFile("planned_network_backend_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	start := strings.Index(source, "func (b *linuxPlannedNetworkBackend) verify(")
	if start < 0 {
		t.Fatal("verify source anchor")
	}
	end := strings.Index(source[start:], "func (b *linuxPlannedNetworkBackend) remove(")
	if start < 0 || end < 0 {
		t.Fatal("verify source anchors")
	}
	source = source[start : start+end]
	up := strings.Index(source, "b.socket.up(ctx, bridge.kernelIndex)")
	finalize := strings.Index(source, "ns.namespace.plannedSetupFinalize(ctx, p, member, ns.namespaceIdentity, ns.peerIndex)")
	readonly := strings.Index(source, "ns.namespace.plannedOperation(ctx, p, member, false)")
	host := strings.Index(source, "reconcilePlannedMulticast(ctx, b.socket, owned)")
	if up < 0 || finalize <= up || readonly <= finalize || host <= readonly {
		t.Fatal("carrier/finalize/readonly/host-reconcile ordering changed")
	}
	if !strings.Contains(string(body), "ns.peerIndex = configured.peerIndex") {
		t.Fatal("original peer index not captured")
	}
}
