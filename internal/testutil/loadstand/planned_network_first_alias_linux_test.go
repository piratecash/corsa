//go:build linux

package loadstand

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func firstAliasTestRequest() []byte {
	p := make([]byte, unix.NLMSG_HDRLEN)
	binary.NativeEndian.PutUint32(p, unix.NLMSG_HDRLEN+unix.SizeofIfInfomsg)
	binary.NativeEndian.PutUint16(p[4:], unix.RTM_NEWLINK)
	binary.NativeEndian.PutUint16(p[6:], unix.NLM_F_REQUEST|unix.NLM_F_CREATE|unix.NLM_F_EXCL|unix.NLM_F_ACK|unix.NLM_F_ECHO)
	binary.NativeEndian.PutUint32(p[8:], 7)
	return p
}
func firstAliasTestACK(request []byte, code int32) syscall.NetlinkMessage {
	b := make([]byte, 4+len(request))
	binary.NativeEndian.PutUint32(b, uint32(code))
	copy(b[4:], request)
	return syscall.NetlinkMessage{Header: syscall.NlMsghdr{Len: uint32(unix.NLMSG_HDRLEN + len(b)), Type: unix.NLMSG_ERROR, Seq: 7, Pid: 19}, Data: b}
}
func firstAliasTestEcho(index, peer int, name, kind, alias string) syscall.NetlinkMessage {
	b := plannedLinkInfo(index, 0, 0)
	b = append(b, plannedStringAttribute(unix.IFLA_IFNAME, name)...)
	if alias != "" {
		b = append(b, plannedStringAttribute(unix.IFLA_IFALIAS, alias)...)
	}
	if kind != "" {
		b = append(b, plannedAttribute(unix.IFLA_LINKINFO|unix.NLA_F_NESTED, plannedStringAttribute(unix.IFLA_INFO_KIND, kind))...)
	}
	if peer != 0 {
		p := make([]byte, 4)
		binary.NativeEndian.PutUint32(p, uint32(peer))
		b = append(b, plannedAttribute(unix.IFLA_LINK, p)...)
	}
	return syscall.NetlinkMessage{Header: syscall.NlMsghdr{Len: uint32(unix.NLMSG_HDRLEN + len(b)), Type: unix.RTM_NEWLINK, Seq: 7, Pid: 19}, Data: b}
}
func TestPlannedFirstAliasACKAndEchoRequireBoth(t *testing.T) {
	for _, order := range []string{"ack-echo", "echo-ack", "same-datagram-ack-echo", "same-datagram-echo-ack"} {
		t.Run(order, func(t *testing.T) {
			r := &plannedCreateReplies{request: firstAliasTestRequest(), port: 19}
			ack, echo := firstAliasTestACK(r.request, 0), firstAliasTestEcho(11, 0, "ownbridge", "bridge", "")
			messages := []syscall.NetlinkMessage{ack, echo}
			if order == "echo-ack" || order == "same-datagram-echo-ack" {
				messages[0], messages[1] = messages[1], messages[0]
			}
			for i, m := range messages {
				if err := r.accept(m); err != nil {
					t.Fatal("own successful create reply refused", err)
				}
				if r.complete() != (i == 1) {
					t.Fatal("one reply authorized creation")
				}
			}
			// This is also an extra reply in the final datagram: exchange must
			// feed every message, never return at the ACK/first completed pair.
			if err := r.accept(echo); err == nil {
				t.Fatal("duplicate echo after completion accepted")
			}
		})
	}
}
func TestPlannedFirstAliasReplyRefusals(t *testing.T) {
	for _, name := range []string{"ack-only", "echo-only", "wrong-port", "wrong-sequence", "wrong-header", "truncated-ack", "positive-error", "eexist", "duplicate-ack", "wrong-type", "invalid-index", "invalid-length", "dump-interrupted"} {
		t.Run(name, func(t *testing.T) {
			r := &plannedCreateReplies{request: firstAliasTestRequest(), port: 19}
			ack, echo := firstAliasTestACK(r.request, 0), firstAliasTestEcho(11, 0, "ownbridge", "bridge", "")
			m := ack
			switch name {
			case "ack-only", "echo-only":
				if name == "echo-only" {
					m = echo
				}
				if err := r.accept(m); err != nil || r.complete() {
					t.Fatal("missing reply gained create authority", err)
				}
				return
			case "wrong-port":
				m.Header.Pid = 0
			case "wrong-sequence":
				m.Header.Seq++
			case "wrong-header":
				m.Data[8]++
			case "truncated-ack":
				m.Data = m.Data[:4]
				m.Header.Len = unix.NLMSG_HDRLEN + 4
			case "positive-error":
				binary.NativeEndian.PutUint32(m.Data, 1)
			case "eexist":
				if err := r.accept(echo); err != nil {
					t.Fatal(err)
				}
				m = firstAliasTestACK(r.request, -int32(unix.EEXIST))
			case "duplicate-ack":
				if err := r.accept(ack); err != nil {
					t.Fatal(err)
				}
			case "wrong-type":
				m.Header.Type = unix.NLMSG_DONE
			case "invalid-index":
				m = firstAliasTestEcho(0, 0, "ownbridge", "bridge", "")
			case "invalid-length":
				m.Header.Len++
			case "dump-interrupted":
				m.Header.Flags |= unix.NLM_F_DUMP_INTR
			}
			err := r.accept(m)
			if err == nil || r.complete() {
				t.Fatal("uncertain/foreign create reply authorized first alias", name, err)
			}
			if name == "eexist" && !errors.Is(err, unix.EEXIST) {
				t.Fatal("kernel errno identity lost", err)
			}
		})
	}
}
func TestPlannedFirstAliasIdentityParsing(t *testing.T) {
	good := firstAliasTestEcho(11, 12, "ownhost", "veth", "nonce")
	link, err := plannedCreationIdentity(good)
	if err != nil || link != (plannedCreatedLink{index: 11, peer: 12, name: "ownhost", kind: "veth", alias: "nonce"}) {
		t.Fatal("exact veth identity lost", link, err)
	}
	for _, name := range []string{"duplicate-peer", "duplicate-kind", "duplicate-info", "truncated-kind", "truncated-peer", "duplicate-name", "negative-peer"} {
		t.Run(name, func(t *testing.T) {
			m := firstAliasTestEcho(11, 12, "ownhost", "veth", "nonce")
			var attr []byte
			switch name {
			case "duplicate-peer":
				attr = plannedAttribute(unix.IFLA_LINK, []byte{12, 0, 0, 0})
			case "duplicate-info":
				attr = plannedAttribute(unix.IFLA_LINKINFO, plannedStringAttribute(unix.IFLA_INFO_KIND, "veth"))
			case "duplicate-kind":
				m = firstAliasTestEcho(11, 12, "ownhost", "", "nonce")
				kind := plannedStringAttribute(unix.IFLA_INFO_KIND, "veth")
				attr = plannedAttribute(unix.IFLA_LINKINFO, append(append([]byte(nil), kind...), kind...))
			case "truncated-kind":
				m = firstAliasTestEcho(11, 12, "ownhost", "", "nonce")
				attr = plannedAttribute(unix.IFLA_LINKINFO, []byte{8, 0, 1, 0, 'v'})
			case "truncated-peer":
				attr = plannedAttribute(unix.IFLA_LINK, []byte{12})
			case "duplicate-name":
				attr = plannedStringAttribute(unix.IFLA_IFNAME, "foreign")
			case "negative-peer":
				m = firstAliasTestEcho(11, 0, "ownhost", "veth", "nonce")
				attr = plannedAttribute(unix.IFLA_LINK, []byte{255, 255, 255, 255})
			}
			m.Data = append(m.Data, attr...)
			if _, err := plannedCreationIdentity(m); err == nil {
				t.Fatal("ambiguous creation identity accepted", name)
			}
		})
	}
}
func firstAliasTestPair() (plannedCreatedLink, []plannedCreatedLink) {
	host := plannedCreatedLink{index: 11, peer: 12, name: "ownhost", kind: "veth"}
	return host, []plannedCreatedLink{host, {index: 12, peer: 11, name: "ownpeer", kind: "veth"}}
}
func TestPlannedFirstAliasFinalizesExactPair(t *testing.T) {
	for _, name := range []string{"bridge", "veth", "already-tagged"} {
		t.Run(name, func(t *testing.T) {
			echo, links := firstAliasTestPair()
			peer := "ownpeer"
			if name == "bridge" {
				echo = plannedCreatedLink{index: 11, name: "ownbridge", kind: "bridge"}
				links = []plannedCreatedLink{echo}
				peer = ""
			}
			if name == "already-tagged" {
				for i := range links {
					links[i].alias = "nonce"
				}
				echo.alias = "nonce"
			}
			var calls []int
			reads := 0
			err := plannedFirstAliases(t.Context(), echo, echo.name, peer, "nonce", plannedFirstAliasOps{
				links: func(context.Context) ([]plannedCreatedLink, error) {
					reads++
					return append([]plannedCreatedLink(nil), links...), nil
				},
				set: func(_ context.Context, index int, alias string) error {
					if reads <= len(calls) {
						t.Fatal("alias write without fresh pair validation")
					}
					calls = append(calls, index)
					for i := range links {
						if links[i].index == index {
							links[i].alias = alias
						}
					}
					return nil
				},
			})
			want := []int{11, 12}
			if name == "bridge" {
				want = []int{11}
			}
			if name == "already-tagged" {
				want = nil
			}
			if err != nil || !reflect.DeepEqual(calls, want) || reads != len(links)+1 {
				t.Fatal("pair not fully finalized before readiness", calls, reads, err)
			}
		})
	}
}
func TestPlannedFirstAliasRefusesWithoutAdoption(t *testing.T) {
	for _, name := range []string{"wrong-echo-name", "wrong-echo-kind", "wrong-echo-alias", "wrong-echo-peer", "missing-peer", "wrong-peer-name", "nonreciprocal", "foreign-namespace", "wrong-alias", "duplicate-index", "reused-index", "read-error", "set-error", "lost-readback", "peer-changed-after-host", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			echo, links := firstAliasTestPair()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes, reads := 0, 0
			switch name {
			case "wrong-echo-name":
				echo.name = "foreign"
			case "wrong-echo-kind":
				echo.kind = "bridge"
			case "wrong-echo-alias":
				echo.alias = "foreign"
			case "wrong-echo-peer":
				echo.peer = 13
			case "missing-peer":
				links = links[:1]
			case "wrong-peer-name":
				links[1].name = "foreign"
			case "nonreciprocal":
				links[1].peer = 13
			case "foreign-namespace":
				links[1].foreignNS = true
			case "wrong-alias":
				links[1].alias = "foreign"
			case "duplicate-index":
				links = append(links, links[0])
			case "reused-index":
				links[0].name = "replacement"
			case "cancelled":
				cancel()
			}
			err := plannedFirstAliases(ctx, echo, "ownhost", "ownpeer", "nonce", plannedFirstAliasOps{
				links: func(context.Context) ([]plannedCreatedLink, error) {
					reads++
					if name == "read-error" {
						return nil, unix.EIO
					}
					if name == "peer-changed-after-host" && reads > 1 {
						links[1].peer++
					}
					return append([]plannedCreatedLink(nil), links...), nil
				},
				set: func(_ context.Context, index int, alias string) error {
					writes++
					if name == "set-error" {
						return unix.EPERM
					}
					if name != "lost-readback" {
						for i := range links {
							if links[i].index == index {
								links[i].alias = alias
							}
						}
					}
					return nil
				},
			})
			if err == nil {
				t.Fatal("uncertain/wrong identity became tagged owner", name)
			}
			maxWrites := 0
			switch name {
			case "set-error", "lost-readback", "peer-changed-after-host":
				maxWrites = 1
			}
			if writes > maxWrites {
				t.Fatal("unvalidated endpoint mutated", name, writes)
			}
			if name == "read-error" && !errors.Is(err, unix.EIO) || name == "set-error" && !errors.Is(err, unix.EPERM) || name == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatal("failure identity lost", err)
			}
		})
	}
}
func TestPlannedFirstAliasUnresolvedCleanupNeverLooksUpName(t *testing.T) {
	for _, kind := range []plannedNetworkKind{plannedBridge, plannedVeth} {
		b := &linuxPlannedNetworkBackend{plan: plannedTestPlan()}
		op := plannedNetworkOperation{kind: kind, plan: b.plan, member: b.plan.members[0]}
		row, err := b.begin(op)
		if err != nil {
			t.Fatal(err)
		}
		// There is deliberately no route socket: an unfinalized create must
		// stop before any lookup/SET/DELLINK, even with a retained attempt row.
		if err := b.remove(t.Context(), row.handle); !errors.Is(err, ErrStopUnconfirmed) || row.removed {
			t.Fatal("unresolved create acquired deletion authority", err)
		}
		if err := b.release(); !errors.Is(err, ErrStopUnconfirmed) {
			t.Fatal("unresolved backend claimed cleanup", err)
		}
	}
}
