//go:build linux

package loadstand

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/netip"
	"syscall"

	"golang.org/x/sys/unix"
)

// Linux UAPI include/uapi/linux/rtnetlink.h (v6.18.58), enum rtattr_type_t.
// The pinned x/sys version does not expose RTA_NH_ID. Keep its stable ABI
// value local rather than changing the dependency or accepting the attribute.
const plannedRouteNexthopID uint16 = 30

// Only the existing serialized Setup owner supplies this private boundary.
// The real socket and per-call fake implement it; there are no global hooks
// or callbacks in the namespace operation. A privileged external mutator is
// outside the existing fixed-launcher sole-mutator premise.
type plannedMulticastSocket interface {
	links(context.Context) ([]plannedKernelLink, error)
	exchange(context.Context, uint16, uint16, []byte, bool) ([]syscall.NetlinkMessage, error)
}

// Cache counters are intentionally not identity: they may change at readback.
// Every routing-affecting field is retained and compared. Unknown attributes
// refuse rather than becoming opaque bytes in a deletion request.
type plannedMulticastRoute struct {
	destination, source               netip.Prefix
	table, metric, index              uint32
	protocol, scope, kind, preference byte
	flags                             uint32
}

func plannedMulticastRoutes(messages []syscall.NetlinkMessage) ([]plannedMulticastRoute, error) {
	var rows []plannedMulticastRoute
	total := 0
	for _, m := range messages {
		if len(m.Data) > processFileLimit-total || len(m.Data) < unix.SizeofRtMsg || m.Header.Type != unix.RTM_NEWROUTE || m.Header.Len != uint32(unix.NLMSG_HDRLEN+len(m.Data)) || m.Header.Flags&unix.NLM_F_DUMP_INTR != 0 || m.Data[0] != unix.AF_INET6 || m.Data[3] != 0 {
			return nil, errPlannedNetwork
		}
		total += len(m.Data)
		r := plannedMulticastRoute{table: uint32(m.Data[4]), protocol: m.Data[5], scope: m.Data[6], kind: m.Data[7], flags: binary.NativeEndian.Uint32(m.Data[8:12])}
		destination, source := netip.IPv6Unspecified(), netip.IPv6Unspecified()
		seen := make(map[uint16]bool)
		for raw := m.Data[unix.SizeofRtMsg:]; len(raw) != 0; {
			if len(raw) < 4 {
				return nil, errPlannedNetwork
			}
			n := int(binary.NativeEndian.Uint16(raw))
			kind := binary.NativeEndian.Uint16(raw[2:])
			if n < 4 || n > len(raw) || plannedAlign(n) > len(raw) || kind&0xc000 != 0 || seen[kind] {
				return nil, errPlannedNetwork
			}
			seen[kind] = true
			v := raw[4:n]
			switch kind {
			case unix.RTA_DST, unix.RTA_SRC:
				if len(v) != 16 {
					return nil, errPlannedNetwork
				}
				var a [16]byte
				copy(a[:], v)
				if kind == unix.RTA_DST {
					destination = netip.AddrFrom16(a)
				} else {
					source = netip.AddrFrom16(a)
				}
			case unix.RTA_TABLE, unix.RTA_PRIORITY, unix.RTA_OIF:
				if len(v) != 4 {
					return nil, errPlannedNetwork
				}
				x := binary.NativeEndian.Uint32(v)
				switch kind {
				case unix.RTA_TABLE:
					if r.table != x {
						return nil, errPlannedNetwork
					}
					r.table = x
				case unix.RTA_PRIORITY:
					r.metric = x
				case unix.RTA_OIF:
					r.index = x
				}
			case unix.RTA_CACHEINFO:
				// struct rta_cacheinfo: eight native u32 fields. Read only;
				// never replay counters, expiry, error or timestamps.
				if len(v) != 32 {
					return nil, errPlannedNetwork
				}
			case unix.RTA_PREF:
				if len(v) != 1 || (v[0] != 0 && v[0] != 1 && v[0] != 3) {
					return nil, errPlannedNetwork
				}
				r.preference = v[0]
			case plannedRouteNexthopID, unix.RTA_MULTIPATH:
				// Even a different OIF/type may win ip6_route_del before
				// its OIF check. No grouped record can be adopted.
				return nil, errPlannedNetwork
			default:
				// Includes gateway/VIA, encapsulation, preferred source,
				// metrics and arbitrary additional mutation semantics.
				return nil, errPlannedNetwork
			}
			for _, pad := range raw[n:plannedAlign(n)] {
				if pad != 0 {
					return nil, errPlannedNetwork
				}
			}
			raw = raw[plannedAlign(n):]
		}
		r.destination = netip.PrefixFrom(destination, int(m.Data[1]))
		r.source = netip.PrefixFrom(source, int(m.Data[2]))
		if !r.destination.IsValid() || r.destination != r.destination.Masked() || !r.source.IsValid() || r.source != r.source.Masked() || (m.Data[1] != 0 && !seen[unix.RTA_DST]) || (m.Data[2] != 0 && !seen[unix.RTA_SRC]) || !seen[unix.RTA_TABLE] || !seen[unix.RTA_PRIORITY] || !seen[unix.RTA_OIF] || !seen[unix.RTA_PREF] || !seen[unix.RTA_CACHEINFO] || r.index == 0 || r.index > 0x7fffffff {
			return nil, errPlannedNetwork
		}
		for _, old := range rows {
			if old == r {
				return nil, errPlannedNetwork
			}
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func plannedMulticastCandidates(rows []plannedMulticastRoute, owned []plannedKernelLink) ([]plannedMulticastRoute, error) {
	var candidates []plannedMulticastRoute
	for _, r := range rows {
		if plannedLoopbackRoute(r.destination) {
			continue
		}
		// Primary addrconf_add_mroute, not a prefix permission: every
		// identity field must qualify BEFORE any route can be deleted.
		if r.destination != netip.MustParsePrefix("ff00::/8") || r.source != netip.MustParsePrefix("::/0") || r.table != unix.RT_TABLE_LOCAL || r.protocol != unix.RTPROT_KERNEL || r.kind != unix.RTN_MULTICAST || r.scope != unix.RT_SCOPE_UNIVERSE || r.metric != 256 || r.flags != 0 || r.preference != 0 {
			return nil, errPlannedNetwork
		}
		found := false
		for _, link := range owned {
			if uint32(link.index) == r.index {
				found = true
			}
		}
		if !found {
			return nil, errPlannedNetwork
		}
		for _, old := range candidates {
			// Distinct direct owned OIFs are permitted, never multiple
			// entries on one OIF. Type/scope are not kernel DEL filters.
			if old.index == r.index {
				return nil, errPlannedNetwork
			}
		}
		candidates = append(candidates, r)
	}
	return candidates, nil
}

func plannedMulticastDelete(r plannedMulticastRoute) []byte {
	body := make([]byte, unix.SizeofRtMsg)
	body[0], body[1], body[2] = unix.AF_INET6, byte(r.destination.Bits()), byte(r.source.Bits())
	body[4], body[5], body[6], body[7] = byte(r.table), r.protocol, r.scope, r.kind
	raw := r.destination.Addr().As16()
	body = append(body, plannedAttribute(unix.RTA_DST, raw[:])...)
	for _, a := range []struct {
		kind  uint16
		value uint32
	}{{unix.RTA_TABLE, r.table}, {unix.RTA_PRIORITY, r.metric}, {unix.RTA_OIF, r.index}} {
		v := make([]byte, 4)
		binary.NativeEndian.PutUint32(v, a.value)
		body = append(body, plannedAttribute(a.kind, v)...)
	}
	return body
}

func plannedMulticastRead(ctx context.Context, s plannedMulticastSocket) ([]plannedMulticastRoute, error) {
	body := make([]byte, unix.SizeofRtMsg)
	body[0] = unix.AF_INET6
	messages, err := s.exchange(ctx, unix.RTM_GETROUTE, unix.NLM_F_DUMP, body, true)
	if err != nil {
		return nil, err
	}
	return plannedMulticastRoutes(messages)
}

func plannedMulticastOwnership(links, owned []plannedKernelLink) error {
	if len(owned) == 0 {
		return errPlannedNetwork
	}
	for i, want := range owned {
		if want.index <= 0 || want.index > 0x7fffffff || !plannedLinkName(want.name) || want.alias == "" || (i != 0 && want.alias != owned[0].alias) {
			return errPlannedNetwork
		}
		for _, prior := range owned[:i] {
			if prior.index == want.index || prior.name == want.name {
				return errPlannedNetwork
			}
		}
		count := 0
		for _, link := range links {
			if link.index == want.index || link.name == want.name {
				if link != want {
					return errPlannedNetwork
				}
				count++
			}
		}
		if count != 1 {
			return errPlannedNetwork
		}
	}
	return nil
}

func plannedMulticastSame(actual, expected []plannedMulticastRoute) bool {
	if len(actual) != len(expected) {
		return false
	}
	for _, r := range actual {
		found := false
		for _, want := range expected {
			if r == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func reconcilePlannedMulticast(ctx context.Context, s plannedMulticastSocket, owned []plannedKernelLink) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	links, err := s.links(ctx)
	if err != nil {
		return plannedNetworkPhaseError("multicast-owned-links", err)
	}
	if err = plannedMulticastOwnership(links, owned); err != nil {
		return plannedNetworkPhaseError("multicast-ownership", err)
	}
	rows, err := plannedMulticastRead(ctx, s)
	if err != nil {
		return plannedNetworkPhaseError("multicast-complete-dump", err)
	}
	candidates, err := plannedMulticastCandidates(rows, owned)
	if err != nil {
		return plannedNetworkPhaseError("multicast-qualify-entire-dump", err)
	}
	remaining := append([]plannedMulticastRoute(nil), rows...)
	for _, target := range candidates {
		if err = ctx.Err(); err != nil {
			return err
		}
		links, err = s.links(ctx)
		if err != nil {
			return plannedNetworkPhaseError("multicast-predelete-links", err)
		}
		if err = plannedMulticastOwnership(links, owned); err != nil {
			return plannedNetworkPhaseError("multicast-predelete-ownership", err)
		}
		replies, deleteErr := s.exchange(ctx, unix.RTM_DELROUTE, unix.NLM_F_ACK, plannedMulticastDelete(target), false)
		if deleteErr != nil {
			return plannedNetworkPhaseError(fmt.Sprintf("multicast-delete index=%d", target.index), deleteErr)
		}
		if len(replies) != 0 {
			return plannedNetworkPhaseError("multicast-unexpected-delete-data", errPlannedNetwork)
		}
		for i, r := range remaining {
			if r == target {
				remaining = append(remaining[:i], remaining[i+1:]...)
				break
			}
		}
		actual, readErr := plannedMulticastRead(ctx, s)
		if readErr != nil {
			return plannedNetworkPhaseError("multicast-readback", readErr)
		}
		if !plannedMulticastSame(actual, remaining) {
			return plannedNetworkPhaseError("multicast-readback-difference", errPlannedNetwork)
		}
	}
	return nil
}
