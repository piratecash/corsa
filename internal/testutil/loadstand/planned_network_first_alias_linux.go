//go:build linux

package loadstand

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

// A provisional reply belongs only to the live serialized create exchange.
// It is not a resource handle or authority against a privileged competing
// namespace mutator. The existing fixed launcher retains sole mutation custody
// until first-alias readback (and during later ordinary guarded operations).
type plannedCreateReplies struct {
	request       []byte
	port          uint32
	acked, echoed bool
	echo          syscall.NetlinkMessage
}

func (r *plannedCreateReplies) complete() bool { return r.acked && r.echoed }
func (r *plannedCreateReplies) accept(m syscall.NetlinkMessage) error {
	if len(r.request) != unix.NLMSG_HDRLEN || r.port == 0 ||
		m.Header.Seq == 0 || m.Header.Seq != binary.NativeEndian.Uint32(r.request[8:]) ||
		m.Header.Pid != r.port || m.Header.Flags&unix.NLM_F_DUMP_INTR != 0 ||
		int(m.Header.Len) != unix.NLMSG_HDRLEN+len(m.Data) {
		return errPlannedNetwork
	}
	switch m.Header.Type {
	case unix.NLMSG_ERROR:
		if r.acked || len(m.Data) < 4+unix.NLMSG_HDRLEN || !bytes.Equal(m.Data[4:4+unix.NLMSG_HDRLEN], r.request) {
			return errPlannedNetwork
		}
		code := int32(binary.NativeEndian.Uint32(m.Data))
		if code > 0 {
			return errPlannedNetwork
		}
		if code < 0 {
			return syscall.Errno(-int64(code))
		}
		// No NETLINK_EXT_ACK/CAP_ACK option is enabled on this private socket.
		if len(m.Data) != 4+unix.NLMSG_HDRLEN {
			return errPlannedNetwork
		}
		r.acked = true
	case unix.RTM_NEWLINK:
		if r.echoed {
			return errPlannedNetwork
		}
		if _, err := plannedCreationIdentity(m); err != nil {
			return err
		}
		r.echoed = true
		r.echo = m
	default:
		return errPlannedNetwork
	}
	return nil
}

type plannedCreatedLink struct {
	index, peer       int
	name, alias, kind string
	foreignNS         bool
}

// Read only the finite creation identity. Ordinary link/nonce parsing and all
// post-setup mutation guards remain separate and unchanged.
func plannedCreationIdentity(m syscall.NetlinkMessage) (plannedCreatedLink, error) {
	if m.Header.Type != unix.RTM_NEWLINK || len(m.Data) < unix.SizeofIfInfomsg {
		return plannedCreatedLink{}, errPlannedNetwork
	}
	out := plannedCreatedLink{index: int(int32(binary.NativeEndian.Uint32(m.Data[4:])))}
	raw := m.Data[unix.SizeofIfInfomsg:]
	var err error
	out.name, out.alias, err = plannedLinkAttributes(raw)
	if err != nil || out.index <= 0 || out.name == "" {
		return plannedCreatedLink{}, errors.Join(errPlannedNetwork, err)
	}
	seenLink, seenInfo, seenNS := false, false, false
	for len(raw) != 0 {
		if len(raw) < 4 {
			return plannedCreatedLink{}, errPlannedNetwork
		}
		n := int(binary.NativeEndian.Uint16(raw))
		if n < 4 || plannedAlign(n) > len(raw) {
			return plannedCreatedLink{}, errPlannedNetwork
		}
		value := raw[4:n]
		switch binary.NativeEndian.Uint16(raw[2:]) & 0x3fff {
		case unix.IFLA_LINK:
			if seenLink || len(value) != 4 {
				return plannedCreatedLink{}, errPlannedNetwork
			}
			seenLink = true
			out.peer = int(int32(binary.NativeEndian.Uint32(value)))
			if out.peer <= 0 {
				return plannedCreatedLink{}, errPlannedNetwork
			}
		case unix.IFLA_LINK_NETNSID:
			if seenNS || len(value) != 4 {
				return plannedCreatedLink{}, errPlannedNetwork
			}
			seenNS = true
			out.foreignNS = true
		case unix.IFLA_LINKINFO:
			if seenInfo {
				return plannedCreatedLink{}, errPlannedNetwork
			}
			seenInfo = true
			out.kind, err = plannedCreationKind(value)
			if err != nil {
				return plannedCreatedLink{}, err
			}
		}
		raw = raw[plannedAlign(n):]
	}
	return out, nil
}
func plannedCreationKind(raw []byte) (string, error) {
	kind := ""
	seen := false
	for len(raw) != 0 {
		if len(raw) < 4 {
			return "", errPlannedNetwork
		}
		n := int(binary.NativeEndian.Uint16(raw))
		if n < 4 || plannedAlign(n) > len(raw) {
			return "", errPlannedNetwork
		}
		if binary.NativeEndian.Uint16(raw[2:])&0x3fff == unix.IFLA_INFO_KIND {
			if seen || !bytes.Equal(raw[4:n], []byte("bridge\x00")) && !bytes.Equal(raw[4:n], []byte("veth\x00")) {
				return "", errPlannedNetwork
			}
			seen = true
			kind = string(raw[4 : n-1])
		}
		raw = raw[plannedAlign(n):]
	}
	// Other link kinds in a full namespace dump are not creation authority.
	return kind, nil
}

// These per-call operations are the concrete socket's two methods. There is no
// global replacement hook and no caller-selectable ambient adoption mode.
type plannedFirstAliasOps struct {
	links func(context.Context) ([]plannedCreatedLink, error)
	set   func(context.Context, int, string) error
}

func plannedCreationPair(links []plannedCreatedLink, echo plannedCreatedLink, host, peer, alias string) ([]plannedCreatedLink, error) {
	kind := "bridge"
	if peer != "" {
		kind = "veth"
	}
	if alias == "" || echo.index <= 0 || echo.name != host || echo.kind != kind || echo.foreignNS ||
		echo.alias != "" && echo.alias != alias || peer == "" && echo.peer != 0 ||
		peer != "" && (peer == host || echo.peer <= 0 || echo.peer == echo.index) {
		return nil, errPlannedNetwork
	}
	expected := []plannedCreatedLink{echo}
	if peer != "" {
		expected = append(expected, plannedCreatedLink{index: echo.peer, peer: echo.index, name: peer, kind: "veth"})
	}
	out := make([]plannedCreatedLink, len(expected))
	for i, want := range expected {
		count := 0
		for _, got := range links {
			if got.name != want.name && got.index != want.index {
				continue
			}
			if got.name != want.name || got.index != want.index || got.kind != want.kind || got.peer != want.peer || got.foreignNS ||
				got.alias != "" && got.alias != alias {
				return nil, errPlannedNetwork
			}
			out[i] = got
			count++
		}
		if count != 1 {
			return nil, errPlannedNetwork
		}
	}
	return out, nil
}
func plannedFirstAliases(ctx context.Context, echo plannedCreatedLink, host, peer, alias string, ops plannedFirstAliasOps) error {
	if ops.links == nil || ops.set == nil {
		return errPlannedNetwork
	}
	count := 1
	if peer != "" {
		count = 2
	}
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		links, err := ops.links(ctx)
		if err != nil {
			return err
		}
		pair, err := plannedCreationPair(links, echo, host, peer, alias)
		if err != nil {
			return err
		}
		// Previous endpoints must stay tagged before any following assignment.
		for j := 0; j < i; j++ {
			if pair[j].alias != alias {
				return errPlannedNetwork
			}
		}
		if pair[i].alias == "" {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := ops.set(ctx, pair[i].index, alias); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	links, err := ops.links(ctx)
	if err != nil {
		return err
	}
	pair, err := plannedCreationPair(links, echo, host, peer, alias)
	if err != nil {
		return err
	}
	for _, link := range pair {
		if link.alias != alias {
			return errPlannedNetwork
		}
	}
	return nil
}
func (s *plannedRouteSocket) creationLinks(ctx context.Context) ([]plannedCreatedLink, error) {
	messages, err := s.exchange(ctx, unix.RTM_GETLINK, unix.NLM_F_DUMP, plannedLinkInfo(0, 0, 0), true)
	if err != nil {
		return nil, err
	}
	var links []plannedCreatedLink
	for _, m := range messages {
		link, err := plannedCreationIdentity(m)
		if err != nil {
			return nil, err
		}
		links = append(links, link)
	}
	return links, nil
}
func (s *plannedRouteSocket) firstAlias(ctx context.Context, index int, alias string) error {
	body := append(plannedLinkInfo(index, 0, 0), plannedStringAttribute(unix.IFLA_IFALIAS, alias)...)
	_, err := s.exchange(ctx, unix.RTM_NEWLINK, unix.NLM_F_ACK, body, false)
	return err
}
func (s *plannedRouteSocket) createTaggedLink(ctx context.Context, body []byte, host, peer, alias string) error {
	messages, err := s.exchange(ctx, unix.RTM_NEWLINK, unix.NLM_F_CREATE|unix.NLM_F_EXCL|unix.NLM_F_ACK|unix.NLM_F_ECHO, body, false)
	if err != nil {
		return plannedNetworkPhaseError("create-ack-echo", err)
	}
	if len(messages) != 1 {
		return errPlannedNetwork
	}
	echo, err := plannedCreationIdentity(messages[0])
	if err != nil {
		return err
	}
	return plannedNetworkPhaseError("first-alias-readback", plannedFirstAliases(ctx, echo, host, peer, alias, plannedFirstAliasOps{links: s.creationLinks, set: s.firstAlias}))
}
