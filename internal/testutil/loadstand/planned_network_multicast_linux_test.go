//go:build linux

package loadstand

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func multicastTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}
func multicastTestU32(n uint32) []byte {
	b := make([]byte, 4)
	binary.NativeEndian.PutUint32(b, n)
	return b
}
func multicastTestMessage(body []byte) syscall.NetlinkMessage {
	return syscall.NetlinkMessage{Header: syscall.NlMsghdr{Len: uint32(unix.NLMSG_HDRLEN + len(body)), Type: unix.RTM_NEWROUTE}, Data: body}
}
func multicastTestRoute(index uint32) syscall.NetlinkMessage {
	b := make([]byte, unix.SizeofRtMsg)
	b[0], b[1], b[4], b[5], b[6], b[7] = unix.AF_INET6, 8, 255, 2, 0, 5
	dst := make([]byte, 16)
	dst[0] = 0xff
	for _, a := range []struct {
		kind  uint16
		value []byte
	}{
		{unix.RTA_DST, dst}, {unix.RTA_TABLE, multicastTestU32(255)},
		{unix.RTA_PRIORITY, multicastTestU32(256)}, {unix.RTA_OIF, multicastTestU32(index)},
		{unix.RTA_CACHEINFO, make([]byte, 32)}, {unix.RTA_PREF, []byte{0}},
	} {
		b = append(b, plannedAttribute(a.kind, a.value)...)
	}
	return multicastTestMessage(b)
}
func multicastTestLoopback() syscall.NetlinkMessage {
	m := multicastTestRoute(1)
	m.Data[1], m.Data[7] = 128, unix.RTN_LOCAL
	dst := make([]byte, 16)
	dst[15] = 1
	return multicastTestAttribute(m, unix.RTA_DST, dst)
}
func multicastTestAttribute(m syscall.NetlinkMessage, kind uint16, value []byte) syscall.NetlinkMessage {
	b := append([]byte(nil), m.Data[:unix.SizeofRtMsg]...)
	for raw := m.Data[unix.SizeofRtMsg:]; len(raw) > 0; {
		n := int(binary.NativeEndian.Uint16(raw))
		step := plannedAlign(n)
		if binary.NativeEndian.Uint16(raw[2:]) == kind {
			if value != nil {
				b = append(b, plannedAttribute(kind, value)...)
			}
		} else {
			b = append(b, raw[:step]...)
		}
		raw = raw[step:]
	}
	m.Data = b
	m.Header.Len = uint32(unix.NLMSG_HDRLEN + len(b))
	return m
}
func multicastTestAppend(m syscall.NetlinkMessage, kind uint16, value []byte) syscall.NetlinkMessage {
	m.Data = append(append([]byte(nil), m.Data...), plannedAttribute(kind, value)...)
	m.Header.Len = uint32(unix.NLMSG_HDRLEN + len(m.Data))
	return m
}
func multicastTestIndex(body []byte) uint32 {
	for raw := body[unix.SizeofRtMsg:]; len(raw) > 0; {
		n := int(binary.NativeEndian.Uint16(raw))
		if binary.NativeEndian.Uint16(raw[2:]) == unix.RTA_OIF {
			return binary.NativeEndian.Uint32(raw[4:n])
		}
		raw = raw[plannedAlign(n):]
	}
	return 0
}

type multicastTestSocket struct {
	rows                                                   []syscall.NetlinkMessage
	owners                                                 []plannedKernelLink
	events                                                 []string
	requests                                               [][]byte
	reads, linkReads, deletes                              int
	failReadAt, failLinkAt, failDeleteAt                   int
	failure                                                error
	changeLinksAt                                          int
	changedLinks                                           []plannedKernelLink
	keepDeleted, removeOther, changeOther, extraDeleteData bool
	addAfterDelete                                         []syscall.NetlinkMessage
	cancelAfterDelete                                      context.CancelFunc
}

func multicastTestOwned() []plannedKernelLink {
	return []plannedKernelLink{{index: 3, name: "ownpeer", alias: "corsa.loadstand:ownnonce"}, {index: 4, name: "ownhost", alias: "corsa.loadstand:ownnonce"}}
}
func (s *multicastTestSocket) links(ctx context.Context) ([]plannedKernelLink, error) {
	s.linkReads++
	s.events = append(s.events, "links")
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.linkReads == s.failLinkAt {
		return nil, s.failure
	}
	if s.linkReads == s.changeLinksAt {
		return append([]plannedKernelLink(nil), s.changedLinks...), nil
	}
	return append([]plannedKernelLink(nil), s.owners...), nil
}
func (s *multicastTestSocket) exchange(ctx context.Context, kind, flags uint16, body []byte, dump bool) ([]syscall.NetlinkMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if kind == unix.RTM_GETROUTE {
		s.reads++
		s.events = append(s.events, "dump")
		if flags != unix.NLM_F_DUMP || !dump || len(body) != unix.SizeofRtMsg || body[0] != unix.AF_INET6 {
			return nil, errors.New("not a closed IPv6 dump")
		}
		if s.reads == s.failReadAt {
			return nil, s.failure
		}
		return append([]syscall.NetlinkMessage(nil), s.rows...), nil
	}
	if kind != unix.RTM_DELROUTE || flags != unix.NLM_F_ACK || dump {
		return nil, errors.New("not a closed deletion")
	}
	s.deletes++
	s.events = append(s.events, "delete")
	s.requests = append(s.requests, append([]byte(nil), body...))
	if s.deletes == s.failDeleteAt {
		return nil, s.failure
	}
	if !s.keepDeleted {
		var kept []syscall.NetlinkMessage
		for _, m := range s.rows {
			if multicastTestIndex(m.Data) != multicastTestIndex(body) {
				kept = append(kept, m)
			}
		}
		s.rows = kept
	}
	if s.removeOther {
		s.rows = nil
	}
	if s.changeOther && len(s.rows) > 0 {
		s.rows[0] = multicastTestAttribute(s.rows[0], unix.RTA_PRIORITY, multicastTestU32(257))
	}
	s.rows = append(s.rows, s.addAfterDelete...)
	if s.cancelAfterDelete != nil {
		s.cancelAfterDelete()
	}
	if s.extraDeleteData {
		return []syscall.NetlinkMessage{multicastTestRoute(3)}, nil
	}
	return nil, nil
}

func TestPlannedMulticastExactOwnedBatch(t *testing.T) {
	for _, name := range []string{"absent", "single", "multiple", "late-carrier"} {
		t.Run(name, func(t *testing.T) {
			owned := multicastTestOwned()
			s := &multicastTestSocket{owners: owned}
			want := 0
			switch name {
			case "single":
				s.rows = []syscall.NetlinkMessage{multicastTestRoute(3)}
				want = 1
			case "multiple":
				s.rows = []syscall.NetlinkMessage{multicastTestRoute(3), multicastTestRoute(4), multicastTestLoopback()}
				want = 2
			case "late-carrier":
				// Early host UP had no route. A later peer carrier event
				// creates it; the final setup sweep uses the CURRENT dump.
				if err := reconcilePlannedMulticast(multicastTestContext(t), s, owned); err != nil || s.deletes != 0 {
					t.Fatal("absent early route was not a no-op", err)
				}
				s.rows = []syscall.NetlinkMessage{multicastTestRoute(4)}
				s.reads, s.linkReads = 0, 0
				s.events = nil
				want = 1
			}
			if err := reconcilePlannedMulticast(multicastTestContext(t), s, owned); err != nil || s.deletes != want {
				t.Fatal("exact final owned batch refused", name, s.deletes, err)
			}
			for i, body := range s.requests {
				index := uint32(3)
				if name == "late-carrier" || i == 1 {
					index = 4
				}
				if multicastTestIndex(body) != index {
					t.Fatal("deletion lost the exact owned OIF", name, index)
				}
			}
			if s.reads != want+1 || s.linkReads != want+1 {
				t.Fatal("batch did not use one complete dump plus per-delete ownership/readback", name, s.events)
			}
			if want > 0 && (!reflect.DeepEqual(s.events[:4], []string{"links", "dump", "links", "delete"})) {
				t.Fatal("effect preceded complete snapshot/ownership", s.events)
			}
		})
	}
}

func TestPlannedMulticastWholeDumpRefusesBeforeDelete(t *testing.T) {
	for _, name := range []string{"foreign-oif", "wrong-table", "wrong-protocol", "wrong-metric", "wrong-type", "wrong-scope", "flags", "wrong-prefix", "source-specific", "gateway", "via", "multipath", "nh-id", "encap", "metrics", "unknown", "duplicate-oif", "duplicate-route", "conflicting-table", "truncated-attribute", "bad-padding", "missing-oif", "missing-cacheinfo", "bad-cacheinfo", "bad-pref", "header-length", "wrong-family", "interrupted"} {
		t.Run(name, func(t *testing.T) {
			bad := multicastTestRoute(4)
			switch name {
			case "foreign-oif":
				bad = multicastTestAttribute(bad, unix.RTA_OIF, multicastTestU32(99))
			case "wrong-table":
				bad.Data[4] = 254
				bad = multicastTestAttribute(bad, unix.RTA_TABLE, multicastTestU32(254))
			case "wrong-protocol":
				bad.Data[5] = 3
			case "wrong-metric":
				bad = multicastTestAttribute(bad, unix.RTA_PRIORITY, multicastTestU32(257))
			case "wrong-type":
				bad.Data[7] = unix.RTN_UNICAST
			case "wrong-scope":
				bad.Data[6] = unix.RT_SCOPE_LINK
			case "flags":
				binary.NativeEndian.PutUint32(bad.Data[8:], unix.RTM_F_CLONED)
			case "wrong-prefix":
				bad.Data[1] = 9
			case "source-specific":
				bad.Data[2] = 128
				v := make([]byte, 16)
				v[0], v[1], v[15] = 0x20, 1, 1
				bad = multicastTestAppend(bad, unix.RTA_SRC, v)
			case "gateway":
				bad = multicastTestAppend(bad, unix.RTA_GATEWAY, make([]byte, 16))
			case "via":
				bad = multicastTestAppend(bad, unix.RTA_VIA, make([]byte, 18))
			case "multipath":
				bad = multicastTestAppend(bad, unix.RTA_MULTIPATH, make([]byte, 8))
			case "nh-id":
				bad = multicastTestAppend(bad, plannedRouteNexthopID, multicastTestU32(9))
			case "encap":
				bad = multicastTestAppend(bad, unix.RTA_ENCAP, []byte{0, 0, 0, 0})
			case "metrics":
				bad = multicastTestAppend(bad, unix.RTA_METRICS, []byte{0, 0, 0, 0})
			case "unknown":
				bad = multicastTestAppend(bad, 0x3fff, []byte{0})
			case "duplicate-oif":
				bad = multicastTestAppend(bad, unix.RTA_OIF, multicastTestU32(4))
			case "duplicate-route":
				bad = multicastTestRoute(3)
			case "conflicting-table":
				bad = multicastTestAttribute(bad, unix.RTA_TABLE, multicastTestU32(254))
			case "truncated-attribute":
				bad.Data = append(bad.Data, []byte{12, 0, 1, 0}...)
				bad.Header.Len = uint32(unix.NLMSG_HDRLEN + len(bad.Data))
			case "bad-padding":
				bad.Data[len(bad.Data)-1] = 1
			case "missing-oif":
				bad = multicastTestAttribute(bad, unix.RTA_OIF, nil)
			case "missing-cacheinfo":
				bad = multicastTestAttribute(bad, unix.RTA_CACHEINFO, nil)
			case "bad-cacheinfo":
				bad = multicastTestAttribute(bad, unix.RTA_CACHEINFO, make([]byte, 28))
			case "bad-pref":
				bad = multicastTestAttribute(bad, unix.RTA_PREF, []byte{1})
			case "header-length":
				bad.Header.Len++
			case "wrong-family":
				bad.Data[0] = unix.AF_INET
			case "interrupted":
				bad.Header.Flags |= unix.NLM_F_DUMP_INTR
			}
			// A valid candidate FIRST must not hide the later same-selector
			// NH_ID/type/foreign OIF ambiguity. No effect until full parse.
			s := &multicastTestSocket{owners: multicastTestOwned(), rows: []syscall.NetlinkMessage{multicastTestRoute(3), bad}}
			if err := reconcilePlannedMulticast(multicastTestContext(t), s, s.owners); err == nil || s.deletes != 0 {
				t.Fatal("uncertain complete dump caused a route mutation", name, s.deletes, err)
			}
		})
	}
}

func TestPlannedMulticastOwnershipRevalidated(t *testing.T) {
	for _, name := range []string{"initial-alias", "predelete-index", "second-delete-alias", "duplicate-owner", "missing-owner"} {
		t.Run(name, func(t *testing.T) {
			owned := multicastTestOwned()
			s := &multicastTestSocket{owners: append([]plannedKernelLink(nil), owned...), rows: []syscall.NetlinkMessage{multicastTestRoute(3), multicastTestRoute(4)}}
			wantDeletes := 0
			switch name {
			case "initial-alias":
				s.owners[0].alias = "foreign"
			case "predelete-index":
				s.changeLinksAt = 2
				s.changedLinks = append([]plannedKernelLink(nil), owned...)
				s.changedLinks[0].index = 9
			case "second-delete-alias":
				s.changeLinksAt = 3
				s.changedLinks = append([]plannedKernelLink(nil), owned...)
				s.changedLinks[1].alias = "foreign"
				wantDeletes = 1
			case "duplicate-owner":
				owned = append(owned, owned[0])
			case "missing-owner":
				s.owners = s.owners[:1]
			}
			if err := reconcilePlannedMulticast(multicastTestContext(t), s, owned); err == nil || s.deletes != wantDeletes {
				t.Fatal("changed link incarnation authorized delete", name, s.deletes, err)
			}
		})
	}
}

func TestPlannedMulticastFailuresPreserveErrorAndStop(t *testing.T) {
	for _, name := range []string{"initial-dump", "initial-links", "ack", "disappeared-before-ack", "readback", "cancelled", "no-deadline", "cancel-after-first", "unexpected-delete-data"} {
		t.Run(name, func(t *testing.T) {
			marker := errors.New("owned syscall failure")
			ctx := multicastTestContext(t)
			s := &multicastTestSocket{owners: multicastTestOwned(), rows: []syscall.NetlinkMessage{multicastTestRoute(3), multicastTestRoute(4)}, failure: marker}
			wantDeletes := 0
			wantError := marker
			switch name {
			case "initial-dump":
				s.failReadAt = 1
			case "initial-links":
				s.failLinkAt = 1
			case "ack":
				s.failDeleteAt = 1
				wantDeletes = 1
			case "disappeared-before-ack":
				s.failDeleteAt = 1
				s.failure = unix.ESRCH
				wantError = unix.ESRCH
				wantDeletes = 1
			case "readback":
				s.failReadAt = 2
				wantDeletes = 1
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
				wantError = context.Canceled
			case "no-deadline":
				ctx = context.Background()
				wantError = ErrLinuxOwnerBound
			case "cancel-after-first":
				c, cancel := context.WithCancel(ctx)
				t.Cleanup(cancel)
				ctx = c
				s.cancelAfterDelete = cancel
				wantError = context.Canceled
				wantDeletes = 1
			case "unexpected-delete-data":
				s.extraDeleteData = true
				wantError = errPlannedNetwork
				wantDeletes = 1
			}
			err := reconcilePlannedMulticast(ctx, s, s.owners)
			if !errors.Is(err, wantError) || s.deletes != wantDeletes {
				t.Fatal("syscall identity lost or uncertain deletion retried", name, s.deletes, err)
			}
		})
	}
}

func TestPlannedMulticastReadbackIsExactRemainder(t *testing.T) {
	for _, name := range []string{"survivor", "other-removed", "other-changed", "new-route", "reappeared"} {
		t.Run(name, func(t *testing.T) {
			s := &multicastTestSocket{owners: multicastTestOwned(), rows: []syscall.NetlinkMessage{multicastTestRoute(3), multicastTestRoute(4), multicastTestLoopback()}}
			switch name {
			case "survivor":
				s.keepDeleted = true
			case "other-removed":
				s.removeOther = true
			case "other-changed":
				s.changeOther = true
			case "new-route":
				s.addAfterDelete = []syscall.NetlinkMessage{multicastTestRoute(99)}
			case "reappeared":
				s.addAfterDelete = []syscall.NetlinkMessage{multicastTestRoute(3)}
			}
			if err := reconcilePlannedMulticast(multicastTestContext(t), s, s.owners); err == nil || s.deletes != 1 {
				t.Fatal("readback difference accepted or retried", name, s.deletes, err)
			}
		})
	}
}

func TestPlannedMulticastCacheCountersAreNotAuthority(t *testing.T) {
	before := multicastTestLoopback()
	after := multicastTestAttribute(before, unix.RTA_CACHEINFO, []byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0})
	a, err := plannedMulticastRoutes([]syscall.NetlinkMessage{before})
	if err != nil {
		t.Fatal(err)
	}
	b, err := plannedMulticastRoutes([]syscall.NetlinkMessage{after})
	if err != nil || !plannedMulticastSame(a, b) {
		t.Fatal("read-only cache counters became routing identity", err)
	}
}

func TestPlannedMulticastRequestHasOnlyExactSelector(t *testing.T) {
	rows, err := plannedMulticastRoutes([]syscall.NetlinkMessage{multicastTestRoute(3)})
	if err != nil {
		t.Fatal(err)
	}
	body := plannedMulticastDelete(rows[0])
	if !reflect.DeepEqual(body[:unix.SizeofRtMsg], []byte{unix.AF_INET6, 8, 0, 0, 255, 2, 0, 5, 0, 0, 0, 0}) {
		t.Fatal("deletion header widened or lost route identity", body)
	}
	wantKinds := []uint16{unix.RTA_DST, unix.RTA_TABLE, unix.RTA_PRIORITY, unix.RTA_OIF}
	var gotKinds []uint16
	for raw := body[unix.SizeofRtMsg:]; len(raw) > 0; {
		n := int(binary.NativeEndian.Uint16(raw))
		gotKinds = append(gotKinds, binary.NativeEndian.Uint16(raw[2:]))
		raw = raw[plannedAlign(n):]
	}
	if !reflect.DeepEqual(gotKinds, wantKinds) || multicastTestIndex(body) != 3 {
		t.Fatal("deletion replayed dump metadata or lost exact OIF", gotKinds, body)
	}
}
