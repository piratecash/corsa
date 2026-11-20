//go:build linux

package loadstand

import (
	"context"
	"errors"
	"net/netip"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

type initialSnapshotTestSocket struct {
	*multicastTestSocket
	ipv4          []plannedNetworkRoute
	ipv4Error     error
	lateAfterRead int
	lateRows      []syscall.NetlinkMessage
}

func (s *initialSnapshotTestSocket) routesIPv4(context.Context) ([]plannedNetworkRoute, error) {
	s.events = append(s.events, "ipv4")
	return append([]plannedNetworkRoute(nil), s.ipv4...), s.ipv4Error
}
func (s *initialSnapshotTestSocket) exchange(ctx context.Context, kind, flags uint16, body []byte, dump bool) ([]syscall.NetlinkMessage, error) {
	rows, err := s.multicastTestSocket.exchange(ctx, kind, flags, body, dump)
	// The returned rows are an actual earlier snapshot. A later kernel event
	// changes only what a future independent dump would observe.
	if err == nil && kind == unix.RTM_GETROUTE && s.reads == s.lateAfterRead {
		s.rows = append([]syscall.NetlinkMessage(nil), s.lateRows...)
	}
	return rows, err
}
func initialSnapshotFixture() (*linuxPlannedNamespaceOperation, linuxNamespaceIdentity, *initialSnapshotTestSocket) {
	op, id, base := namespaceFinalizeFixture()
	op.mode = plannedNamespaceConfigure
	return op, id, &initialSnapshotTestSocket{multicastTestSocket: base, ipv4: []plannedNetworkRoute{{prefix: op.plan.prefix}}}
}
func TestPlannedNamespaceInitialRouteSnapshotInterleaving(t *testing.T) {
	for _, deleteOne := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero-candidates", true: "exact-owned-delete"}[deleteOne], func(t *testing.T) {
			op, _, s := initialSnapshotFixture()
			s.rows = []syscall.NetlinkMessage{multicastTestLoopback()}
			wantReads, wantDeletes := 1, 0
			if deleteOne {
				s.rows = append(s.rows, multicastTestRoute(3))
				wantReads, wantDeletes = 2, 1
			}
			s.lateAfterRead = wantReads
			s.lateRows = []syscall.NetlinkMessage{multicastTestLoopback(), multicastTestRoute(3)}
			routes, err := initialPlannedNamespaceRoutes(multicastTestContext(t), s, s.owners[1:])
			if err != nil || validatePlannedNamespaceRoutes(op, routes, 3) != nil || s.reads != wantReads || s.deletes != wantDeletes || len(s.events) == 0 || s.events[0] != "ipv4" {
				t.Fatal("initial evidence did not use exact qualified/readback snapshot", err, s.events)
			}
			if len(routes) != 2 || routes[1].prefix != netip.MustParsePrefix("::1/128") {
				t.Fatal("actual loopback row lost in projection", routes)
			}
			// A later independent read is still strict: snapshot reuse must not turn a
			// subsequently observed multicast route into readonly acceptance.
			later, err := plannedMulticastRead(multicastTestContext(t), s)
			if err != nil {
				t.Fatal(err)
			}
			laterRoutes := append([]plannedNetworkRoute(nil), s.ipv4...)
			for _, row := range later {
				laterRoutes = append(laterRoutes, plannedNetworkRoute{prefix: row.destination})
			}
			op.mode = plannedNamespaceReadOnly
			if validatePlannedNamespaceRoutes(op, laterRoutes, 3) == nil || s.deletes != wantDeletes {
				t.Fatal("fresh readonly check was weakened or mutated")
			}
		})
	}
}
func TestPlannedNamespaceInitialRouteSnapshotStrictRefusals(t *testing.T) {
	for _, name := range []string{"foreign-oif", "wrong-table", "changed-owner", "duplicate", "readback-difference", "foreign-after-delete", "ipv4-default", "missing-connected"} {
		t.Run(name, func(t *testing.T) {
			op, _, s := initialSnapshotFixture()
			s.rows = []syscall.NetlinkMessage{multicastTestRoute(3)}
			wantDeletes := 0
			switch name {
			case "foreign-oif":
				s.rows = append(s.rows, multicastTestRoute(99))
			case "wrong-table":
				s.rows[0].Data[4] = 254
				s.rows[0] = multicastTestAttribute(s.rows[0], unix.RTA_TABLE, multicastTestU32(254))
			case "changed-owner":
				s.owners[1].alias = "foreign"
			case "duplicate":
				s.rows = append(s.rows, multicastTestRoute(3))
			case "readback-difference":
				s.keepDeleted = true
				wantDeletes = 1
			case "foreign-after-delete":
				s.addAfterDelete = []syscall.NetlinkMessage{multicastTestRoute(99)}
				wantDeletes = 1
			case "ipv4-default":
				s.rows = nil
				s.ipv4 = append(s.ipv4, plannedNetworkRoute{prefix: netip.MustParsePrefix("0.0.0.0/0")})
			case "missing-connected":
				s.rows = nil
				s.ipv4 = nil
			}
			// Keep the exact expected ownership separate from any changed actual row.
			owned := []plannedKernelLink{{index: 3, name: op.member.peerLink, alias: plannedNetworkAlias(op.plan.nonce)}}
			routes, err := initialPlannedNamespaceRoutes(multicastTestContext(t), s, owned)
			if err == nil {
				err = validatePlannedNamespaceRoutes(op, routes, 3)
			}
			if err == nil || s.deletes != wantDeletes {
				t.Fatal("foreign/partial evidence was accepted", err, s.events)
			}
		})
	}
}
func TestPlannedNamespaceInitialRouteSnapshotErrorsRemainClosed(t *testing.T) {
	op, _, s := initialSnapshotFixture()
	sentinel := errors.New("private snapshot failure")
	s.ipv4Error = sentinel
	if _, err := initialPlannedNamespaceRoutes(multicastTestContext(t), s, s.owners[1:]); !errors.Is(err, sentinel) || s.reads != 0 || s.deletes != 0 || s.linkReads != 0 {
		t.Fatal("IPv4 failure lost or cleanup ran before its snapshot", err, s.events)
	}
	s.ipv4Error = nil
	if _, err := initialPlannedNamespaceRoutes(context.Background(), s, s.owners[1:]); !errors.Is(err, ErrLinuxOwnerBound) || s.reads != 0 || s.deletes != 0 {
		t.Fatal("deadline requirement lost", err)
	}
	// Late routes are handled by the existing distinct setup finalizer; taking
	// an initial snapshot never substitutes for that mandatory later boundary.
	op.mode = plannedNamespaceSetupFinalize
	s.rows = []syscall.NetlinkMessage{multicastTestRoute(3)}
	if err := finalizePlannedNamespaceSetup(multicastTestContext(t), s, op, op.identity, false); err != nil || s.deletes != 1 {
		t.Fatal("post-carrier finalizer no longer exact", err)
	}
}
