//go:build linux

package loadstand

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// One serialized constructor/setup owner uses this socket. This is not an
// ambient network service and does not accept a caller's raw netlink packet.
type plannedRouteSocket struct {
	fd  int
	seq uint32
}
type plannedKernelLink struct {
	index       int
	name, alias string
}

func openPlannedRouteSocket(ctx context.Context) (*plannedRouteSocket, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return nil, err
	}
	if err = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return nil, errors.Join(err, unix.Close(fd))
	}
	return &plannedRouteSocket{fd: fd}, nil
}
func (s *plannedRouteSocket) close() error {
	if s.fd < 0 {
		return nil
	}
	fd := s.fd
	s.fd = -1
	return unix.Close(fd)
}
func plannedAlign(n int) int { return (n + 3) &^ 3 }
func plannedAttribute(kind uint16, value []byte) []byte {
	n := 4 + len(value)
	out := make([]byte, plannedAlign(n))
	binary.NativeEndian.PutUint16(out, uint16(n))
	binary.NativeEndian.PutUint16(out[2:], kind)
	copy(out[4:], value)
	return out
}
func plannedStringAttribute(kind uint16, value string) []byte {
	return plannedAttribute(kind, append([]byte(value), 0))
}
func plannedLinkInfo(index int, flags, change uint32) []byte {
	out := make([]byte, unix.SizeofIfInfomsg)
	binary.NativeEndian.PutUint32(out[4:], uint32(index))
	binary.NativeEndian.PutUint32(out[8:], flags)
	binary.NativeEndian.PutUint32(out[12:], change)
	return out
}
func plannedNetlinkCurrentSequence(received, current uint32) (bool, error) {
	if received == 0 || current == 0 || received > current {
		return false, plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedNetlinkCurrentSequence:guard1", errPlannedNetwork)
	}
	return received == current, nil
}
func (s *plannedRouteSocket) exchange(ctx context.Context, kind, flags uint16, body []byte, dump bool) ([]syscall.NetlinkMessage, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, ErrLinuxOwnerBound
	}
	if s.fd < 0 || s.seq == math.MaxUint32 {
		return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard1", errPlannedNetwork)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.seq++
	packet := make([]byte, unix.NLMSG_HDRLEN+len(body))
	binary.NativeEndian.PutUint32(packet, uint32(len(packet)))
	binary.NativeEndian.PutUint16(packet[4:], kind)
	binary.NativeEndian.PutUint16(packet[6:], flags|unix.NLM_F_REQUEST)
	binary.NativeEndian.PutUint32(packet[8:], s.seq)
	copy(packet[unix.NLMSG_HDRLEN:], body)
	var creation *plannedCreateReplies
	if flags&unix.NLM_F_ECHO != 0 {
		if dump || kind != unix.RTM_NEWLINK || flags != unix.NLM_F_CREATE|unix.NLM_F_EXCL|unix.NLM_F_ACK|unix.NLM_F_ECHO {
			return nil, errPlannedNetwork
		}
		bound, err := unix.Getsockname(s.fd)
		if err != nil {
			return nil, err
		}
		local, ok := bound.(*unix.SockaddrNetlink)
		if !ok || local.Family != unix.AF_NETLINK || local.Pid == 0 {
			return nil, errPlannedNetwork
		}
		creation = &plannedCreateReplies{request: append([]byte(nil), packet[:unix.NLMSG_HDRLEN]...), port: local.Pid}
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, context.DeadlineExceeded
	}
	tv := unix.NsecToTimeval(remaining.Nanoseconds())
	if err := unix.SetsockoptTimeval(s.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
		return nil, err
	}
	if err := unix.SetsockoptTimeval(s.fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, &tv); err != nil {
		return nil, err
	}
	if err := unix.Sendto(s.fd, packet, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return nil, err
	}
	var result []syscall.NetlinkMessage
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		tv = unix.NsecToTimeval(remaining.Nanoseconds())
		if err := unix.SetsockoptTimeval(s.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
			return nil, err
		}
		buffer := make([]byte, processFileLimit)
		n, from, err := unix.Recvfrom(s.fd, buffer, 0)
		if err != nil {
			return nil, err
		}
		peer, ok := from.(*unix.SockaddrNetlink)
		if !ok || peer.Pid != 0 || n == len(buffer) {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard2", errPlannedNetwork)
		}
		messages, err := syscall.ParseNetlinkMessage(buffer[:n])
		if err != nil {
			return nil, err
		}
		for _, m := range messages {
			current, sequenceErr := plannedNetlinkCurrentSequence(m.Header.Seq, s.seq)
			if sequenceErr != nil {
				return nil, sequenceErr
			}
			if !current {
				continue
			}
			if m.Header.Flags&unix.NLM_F_DUMP_INTR != 0 {
				return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard3", errPlannedNetwork)
			}
			if creation != nil {
				if err := creation.accept(m); err != nil {
					return nil, err
				}
				continue
			}
			switch m.Header.Type {
			case unix.NLMSG_ERROR:
				if len(m.Data) < 4 {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard4", errPlannedNetwork)
				}
				code := int32(binary.NativeEndian.Uint32(m.Data))
				if code > 0 {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard5", errPlannedNetwork)
				}
				if code != 0 {
					return nil, syscall.Errno(-code)
				}
				if !dump {
					return result, nil
				}
			case unix.NLMSG_DONE:
				if !dump {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard6", errPlannedNetwork)
				}
				if len(m.Data) != 0 {
					if len(m.Data) < 4 {
						return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard7", errPlannedNetwork)
					}
					code := int32(binary.NativeEndian.Uint32(m.Data))
					if code > 0 {
						return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:exchange:guard8", errPlannedNetwork)
					}
					if code != 0 {
						return nil, syscall.Errno(-code)
					}
				}
				return result, nil
			default:
				result = append(result, m)
			}
		}
		// Never stop halfway through a datagram: a duplicate/conflicting reply
		// after ACK/ECHO must invalidate the same creation attempt.
		if creation != nil && creation.complete() {
			return []syscall.NetlinkMessage{creation.echo}, nil
		}
	}
}
func plannedLinkAttributes(raw []byte) (string, string, error) {
	var name, alias string
	var seenName, seenAlias bool
	for len(raw) != 0 {
		if len(raw) < 4 {
			return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard1", errPlannedNetwork)
		}
		n := int(binary.NativeEndian.Uint16(raw))
		if n < 4 || n > len(raw) {
			return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard2", errPlannedNetwork)
		}
		kind := binary.NativeEndian.Uint16(raw[2:]) & 0x3fff
		if kind == unix.IFLA_IFNAME || kind == unix.IFLA_IFALIAS {
			value := raw[4:n]
			if len(value) == 0 || value[len(value)-1] != 0 {
				return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard3", errPlannedNetwork)
			}
			text := string(value[:len(value)-1])
			if strings.ContainsRune(text, 0) {
				return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard4", errPlannedNetwork)
			}
			if kind == unix.IFLA_IFNAME {
				if seenName {
					return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard5", errPlannedNetwork)
				}
				seenName = true
				name = text
			} else {
				if seenAlias {
					return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard6", errPlannedNetwork)
				}
				seenAlias = true
				alias = text
			}
		}
		step := plannedAlign(n)
		if step > len(raw) {
			return "", "", plannedNetworkPhaseError("planned_network_rtnetlink_linux:plannedLinkAttributes:guard7", errPlannedNetwork)
		}
		raw = raw[step:]
	}
	return name, alias, nil
}
func (s *plannedRouteSocket) links(ctx context.Context) ([]plannedKernelLink, error) {
	messages, err := s.exchange(ctx, unix.RTM_GETLINK, unix.NLM_F_DUMP, plannedLinkInfo(0, 0, 0), true)
	if err != nil {
		return nil, err
	}
	var result []plannedKernelLink
	for _, m := range messages {
		if m.Header.Type != unix.RTM_NEWLINK || len(m.Data) < unix.SizeofIfInfomsg {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:links:guard1", errPlannedNetwork)
		}
		name, alias, err := plannedLinkAttributes(m.Data[unix.SizeofIfInfomsg:])
		if err != nil {
			return nil, err
		}
		index := int(int32(binary.NativeEndian.Uint32(m.Data[4:])))
		if index <= 0 || name == "" {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:links:guard2", errPlannedNetwork)
		}
		result = append(result, plannedKernelLink{index: index, name: name, alias: alias})
	}
	return result, nil
}
func (s *plannedRouteSocket) createBridge(ctx context.Context, name, alias string) error {
	body := plannedLinkInfo(0, 0, 0)
	body = append(body, plannedStringAttribute(unix.IFLA_IFNAME, name)...)
	body = append(body, plannedStringAttribute(unix.IFLA_IFALIAS, alias)...)
	info := plannedStringAttribute(unix.IFLA_INFO_KIND, "bridge")
	body = append(body, plannedAttribute(unix.IFLA_LINKINFO|unix.NLA_F_NESTED, info)...)
	return s.createTaggedLink(ctx, body, name, "", alias)
}
func (s *plannedRouteSocket) createVeth(ctx context.Context, host, peer, alias string) error {
	body := plannedLinkInfo(0, 0, 0)
	body = append(body, plannedStringAttribute(unix.IFLA_IFNAME, host)...)
	body = append(body, plannedStringAttribute(unix.IFLA_IFALIAS, alias)...)
	peerBody := plannedLinkInfo(0, 0, 0)
	peerBody = append(peerBody, plannedStringAttribute(unix.IFLA_IFNAME, peer)...)
	peerBody = append(peerBody, plannedStringAttribute(unix.IFLA_IFALIAS, alias)...)
	// VETH_INFO_PEER is the veth UAPI nested attribute (1), carrying ifinfomsg.
	info := plannedStringAttribute(unix.IFLA_INFO_KIND, "veth")
	info = append(info, plannedAttribute(unix.IFLA_INFO_DATA|unix.NLA_F_NESTED, plannedAttribute(1|unix.NLA_F_NESTED, peerBody))...)
	body = append(body, plannedAttribute(unix.IFLA_LINKINFO|unix.NLA_F_NESTED, info)...)
	return s.createTaggedLink(ctx, body, host, peer, alias)
}
func (s *plannedRouteSocket) setLink(ctx context.Context, index int, kind uint16, value uint32) error {
	body := plannedLinkInfo(index, 0, 0)
	raw := make([]byte, 4)
	binary.NativeEndian.PutUint32(raw, value)
	body = append(body, plannedAttribute(kind, raw)...)
	_, err := s.exchange(ctx, unix.RTM_NEWLINK, unix.NLM_F_ACK, body, false)
	return err
}
func (s *plannedRouteSocket) up(ctx context.Context, index int) error {
	_, err := s.exchange(ctx, unix.RTM_NEWLINK, unix.NLM_F_ACK, plannedLinkInfo(index, unix.IFF_UP, unix.IFF_UP), false)
	return err
}
func (s *plannedRouteSocket) removeLink(ctx context.Context, index int) error {
	_, err := s.exchange(ctx, unix.RTM_DELLINK, unix.NLM_F_ACK, plannedLinkInfo(index, 0, 0), false)
	return err
}

// The kernel adds only the selected connected route. No operation in this
// file can install a gateway or default route.
func (s *plannedRouteSocket) address(ctx context.Context, index int, address netip.Addr, bits int) error {
	if index <= 0 || !plannedApprovedAddress(address) || bits < 4 || bits > 32 {
		return plannedNetworkPhaseError("planned_network_rtnetlink_linux:address:guard1", errPlannedNetwork)
	}
	body := make([]byte, unix.SizeofIfAddrmsg)
	body[0] = unix.AF_INET
	body[1] = byte(bits)
	binary.NativeEndian.PutUint32(body[4:], uint32(index))
	raw := address.As4()
	body = append(body, plannedAttribute(unix.IFA_LOCAL, raw[:])...)
	body = append(body, plannedAttribute(unix.IFA_ADDRESS, raw[:])...)
	_, err := s.exchange(ctx, unix.RTM_NEWADDR, unix.NLM_F_CREATE|unix.NLM_F_EXCL|unix.NLM_F_ACK, body, false)
	return err
}
func (s *plannedRouteSocket) routes(ctx context.Context) ([]plannedNetworkRoute, error) {
	var result []plannedNetworkRoute
	for _, family := range []byte{unix.AF_INET, unix.AF_INET6} {
		body := make([]byte, unix.SizeofRtMsg)
		body[0] = family
		messages, err := s.exchange(ctx, unix.RTM_GETROUTE, unix.NLM_F_DUMP, body, true)
		if err != nil {
			return nil, err
		}
		for _, m := range messages {
			if m.Header.Type != unix.RTM_NEWROUTE || len(m.Data) < unix.SizeofRtMsg || m.Data[0] != family {
				return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard1", errPlannedNetwork)
			}
			bits := int(m.Data[1])
			var address netip.Addr
			if family == unix.AF_INET {
				address = netip.IPv4Unspecified()
			} else {
				address = netip.IPv6Unspecified()
			}
			attrs := m.Data[unix.SizeofRtMsg:]
			seen := false
			for len(attrs) != 0 {
				if len(attrs) < 4 {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard2", errPlannedNetwork)
				}
				size := int(binary.NativeEndian.Uint16(attrs))
				if size < 4 || size > len(attrs) {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard3", errPlannedNetwork)
				}
				kind := binary.NativeEndian.Uint16(attrs[2:]) & 0x3fff
				if kind == unix.RTA_GATEWAY || kind == unix.RTA_MULTIPATH || kind == unix.RTA_VIA {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard4", errPlannedNetwork)
				}
				if kind == unix.RTA_DST {
					if seen {
						return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard5", errPlannedNetwork)
					}
					seen = true
					if family == unix.AF_INET && size == 8 {
						var a [4]byte
						copy(a[:], attrs[4:size])
						address = netip.AddrFrom4(a)
					} else if family == unix.AF_INET6 && size == 20 {
						var a [16]byte
						copy(a[:], attrs[4:size])
						address = netip.AddrFrom16(a)
					} else {
						return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard6", errPlannedNetwork)
					}
				}
				step := plannedAlign(size)
				if step > len(attrs) {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard7", errPlannedNetwork)
				}
				attrs = attrs[step:]
			}
			prefix := netip.PrefixFrom(address, bits)
			if !prefix.IsValid() || prefix != prefix.Masked() {
				return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:routes:guard8", errPlannedNetwork)
			}
			result = append(result, plannedNetworkRoute{prefix: prefix})
		}
	}
	return result, nil
}

type plannedKernelAddress struct {
	index  int
	prefix netip.Prefix
}

func (s *plannedRouteSocket) addresses(ctx context.Context) ([]plannedKernelAddress, error) {
	messages, err := s.exchange(ctx, unix.RTM_GETADDR, unix.NLM_F_DUMP, make([]byte, unix.SizeofIfAddrmsg), true)
	if err != nil {
		return nil, err
	}
	var result []plannedKernelAddress
	for _, message := range messages {
		if message.Header.Type != unix.RTM_NEWADDR || len(message.Data) < unix.SizeofIfAddrmsg {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard1", errPlannedNetwork)
		}
		body := message.Data
		family := body[0]
		bits := int(body[1])
		index := int(binary.NativeEndian.Uint32(body[4:]))
		if index <= 0 || family != unix.AF_INET && family != unix.AF_INET6 {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard2", errPlannedNetwork)
		}
		attrs := body[unix.SizeofIfAddrmsg:]
		var address netip.Addr
		var local netip.Addr
		for len(attrs) != 0 {
			if len(attrs) < 4 {
				return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard3", errPlannedNetwork)
			}
			size := int(binary.NativeEndian.Uint16(attrs))
			if size < 4 || size > len(attrs) {
				return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard4", errPlannedNetwork)
			}
			kind := binary.NativeEndian.Uint16(attrs[2:]) & 0x3fff
			if kind == unix.IFA_ADDRESS || kind == unix.IFA_LOCAL {
				var value netip.Addr
				if family == unix.AF_INET && size == 8 {
					var a [4]byte
					copy(a[:], attrs[4:size])
					value = netip.AddrFrom4(a)
				} else if family == unix.AF_INET6 && size == 20 {
					var a [16]byte
					copy(a[:], attrs[4:size])
					value = netip.AddrFrom16(a)
				} else {
					return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard5", errPlannedNetwork)
				}
				if kind == unix.IFA_ADDRESS {
					if address.IsValid() {
						return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard6", errPlannedNetwork)
					}
					address = value
				} else {
					if local.IsValid() {
						return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard7", errPlannedNetwork)
					}
					local = value
				}
			}
			step := plannedAlign(size)
			if step > len(attrs) {
				return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard8", errPlannedNetwork)
			}
			attrs = attrs[step:]
		}
		if !address.IsValid() {
			address = local
		}
		if local.IsValid() && local != address {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard9", errPlannedNetwork)
		}
		prefix := netip.PrefixFrom(address, bits)
		if !prefix.IsValid() {
			return nil, plannedNetworkPhaseError("planned_network_rtnetlink_linux:addresses:guard10", errPlannedNetwork)
		}
		result = append(result, plannedKernelAddress{index: index, prefix: prefix})
	}
	return result, nil
}
func (s *plannedRouteSocket) disableIPv6(ctx context.Context, index int) error {
	// IN6_ADDR_GEN_MODE_NONE is UAPI value1. This changes only our new link,
	// requires NET_ADMIN, and works with the required read-only /proc/sys mount.
	body := plannedLinkInfo(index, 0, 0)
	ipv6 := plannedAttribute(unix.IFLA_INET6_ADDR_GEN_MODE, []byte{1})
	body = append(body, plannedAttribute(unix.IFLA_AF_SPEC|unix.NLA_F_NESTED, plannedAttribute(unix.AF_INET6|unix.NLA_F_NESTED, ipv6))...)
	if _, err := s.exchange(ctx, unix.RTM_NEWLINK, unix.NLM_F_ACK, body, false); err != nil {
		return plannedNetworkPhaseError(fmt.Sprintf("ipv6-addrgen-RTM_NEWLINK index=%d", index), err)
	}
	addresses, err := s.addresses(ctx)
	if err != nil {
		return plannedNetworkPhaseError(fmt.Sprintf("ipv6-dump-addresses index=%d", index), err)
	}
	for _, address := range addresses {
		if address.index != index || !address.prefix.Addr().Is6() {
			continue
		}
		body := make([]byte, unix.SizeofIfAddrmsg)
		body[0] = unix.AF_INET6
		body[1] = byte(address.prefix.Bits())
		binary.NativeEndian.PutUint32(body[4:], uint32(index))
		raw := address.prefix.Addr().As16()
		body = append(body, plannedAttribute(unix.IFA_ADDRESS, raw[:])...)
		if _, err = s.exchange(ctx, unix.RTM_DELADDR, unix.NLM_F_ACK, body, false); err != nil {
			return plannedNetworkPhaseError(fmt.Sprintf("ipv6-delete-RTM_DELADDR index=%d", index), err)
		}
	}
	return nil
}
