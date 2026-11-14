//go:build linux && d1_full512_native

package node

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

const (
	full512FrameLimit   = 131_072
	full512PayloadLimit = 134_217_728
)

func full512GuestLinks() ([]full512GuestLink, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	links := make([]full512GuestLink, 0, len(interfaces))
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		link := full512GuestLink{index: iface.Index, flags: iface.Flags}
		for _, address := range addresses {
			prefix, err := netip.ParsePrefix(address.String())
			if err != nil {
				return nil, err
			}
			link.addresses = append(link.addresses, prefix)
		}
		links = append(links, link)
	}
	return links, nil
}

func full512GuestRoutes() ([]full512GuestRoute, error) {
	var routes []full512GuestRoute
	for _, family := range []int{unix.AF_INET, unix.AF_INET6} {
		raw, err := syscall.NetlinkRIB(unix.RTM_GETROUTE, family)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeFull512GuestRouteDump(raw, family)
		if err != nil {
			return nil, err
		}
		routes = append(routes, decoded...)
	}
	return routes, nil
}

func decodeFull512GuestRouteDump(raw []byte, family int) ([]full512GuestRoute, error) {
	messages, err := syscall.ParseNetlinkMessage(raw)
	if err != nil {
		return nil, err
	}
	var routes []full512GuestRoute
	wireBytes := uint64(0)
	for i, message := range messages {
		wireBytes += (uint64(message.Header.Len) + 3) &^ 3
		if message.Header.Flags&unix.NLM_F_DUMP_INTR != 0 {
			return nil, fmt.Errorf("interrupted guest route dump")
		}
		if message.Header.Type == unix.NLMSG_DONE {
			if i != len(messages)-1 || wireBytes != uint64(len(raw)) || len(message.Data) != 0 &&
				(len(message.Data) < 4 || binary.NativeEndian.Uint32(message.Data[:4]) != 0) {
				return nil, fmt.Errorf("invalid guest route dump terminal status")
			}
			return routes, nil
		}
		route, usable, err := decodeFull512GuestRoute(message, family)
		if err != nil {
			return nil, err
		}
		if usable {
			routes = append(routes, route)
		}
	}
	return nil, fmt.Errorf("missing guest route dump terminal record")
}

func decodeFull512GuestRoute(message syscall.NetlinkMessage, family int) (full512GuestRoute, bool, error) {
	var route full512GuestRoute
	if message.Header.Type != unix.RTM_NEWROUTE || len(message.Data) < unix.SizeofRtMsg ||
		int(message.Data[0]) != family {
		return route, false, fmt.Errorf("unexpected guest route record")
	}
	address := netip.IPv6Unspecified()
	if family == unix.AF_INET {
		address = netip.IPv4Unspecified()
	}
	attrs, err := syscall.ParseNetlinkRouteAttr(&message)
	if err != nil {
		return route, false, err
	}
	table := uint32(message.Data[4])
	seenDestination, seenOutput, seenTable := false, false, false
	for _, attr := range attrs {
		switch attr.Attr.Type {
		case unix.RTA_DST:
			if seenDestination || len(attr.Value) != address.BitLen()/8 {
				return route, false, fmt.Errorf("invalid guest route destination")
			}
			address, _ = netip.AddrFromSlice(attr.Value)
			seenDestination = true
		case unix.RTA_OIF:
			if seenOutput || len(attr.Value) != 4 {
				return route, false, fmt.Errorf("invalid guest route output")
			}
			route.output = int(binary.NativeEndian.Uint32(attr.Value))
			seenOutput = true
		case unix.RTA_TABLE:
			if seenTable || len(attr.Value) != 4 {
				return route, false, fmt.Errorf("invalid guest route table")
			}
			attributeTable := binary.NativeEndian.Uint32(attr.Value)
			if table != unix.RT_TABLE_UNSPEC && table != unix.RT_TABLE_COMPAT && table != attributeTable {
				return route, false, fmt.Errorf("conflicting guest route table")
			}
			table = attributeTable
			seenTable = true
		case unix.RTA_PREFSRC, unix.RTA_PRIORITY, unix.RTA_CACHEINFO, unix.RTA_PREF, unix.RTA_METRICS:
			// Metadata about the observed route.
		default:
			return route, false, fmt.Errorf("unsupported guest route attribute %d", attr.Attr.Type)
		}
	}
	if message.Data[7] == unix.RTN_UNREACHABLE && route.output == 0 {
		return route, false, nil
	}
	if route.output <= 0 {
		return route, false, fmt.Errorf("guest route lacks a link")
	}
	route.destination = netip.PrefixFrom(address, int(message.Data[1]))
	route.local = table == unix.RT_TABLE_LOCAL && message.Data[7] == unix.RTN_LOCAL
	return route, true, nil
}

func TestFull512GuestRouteDumpDecoding(t *testing.T) {
	message := func(kind, flags uint16, data []byte) []byte {
		raw := make([]byte, unix.NLMSG_HDRLEN+len(data))
		binary.NativeEndian.PutUint32(raw, uint32(len(raw)))
		binary.NativeEndian.PutUint16(raw[4:], kind)
		binary.NativeEndian.PutUint16(raw[6:], flags)
		copy(raw[unix.NLMSG_HDRLEN:], data)
		return raw
	}
	route := func(kind, table byte, attributeTable uint32) []byte {
		data := make([]byte, unix.SizeofRtMsg)
		data[0], data[1], data[4], data[7] = unix.AF_INET, 32, table, kind
		var output [4]byte
		binary.NativeEndian.PutUint32(output[:], 1)
		for _, attr := range []struct {
			kind  uint16
			value [4]byte
		}{
			{unix.RTA_DST, [4]byte{10, 77, 0, 1}},
			{unix.RTA_OIF, output},
		} {
			data = binary.NativeEndian.AppendUint16(data, 8)
			data = binary.NativeEndian.AppendUint16(data, attr.kind)
			data = append(data, attr.value[:]...)
		}
		data = binary.NativeEndian.AppendUint16(data, 8)
		data = binary.NativeEndian.AppendUint16(data, unix.RTA_TABLE)
		data = binary.NativeEndian.AppendUint32(data, attributeTable)
		return message(unix.RTM_NEWROUTE, 0, data)
	}
	done := message(unix.NLMSG_DONE, 0, make([]byte, 4))
	local := route(unix.RTN_LOCAL, unix.RT_TABLE_LOCAL, unix.RT_TABLE_LOCAL)
	for _, tc := range []struct {
		name     string
		raw      []byte
		want     bool
		wantFail bool
	}{
		{"local", append(local, done...), true, false},
		{"unicast", append(route(unix.RTN_UNICAST, unix.RT_TABLE_LOCAL, unix.RT_TABLE_LOCAL), done...), false, false},
		{"other-table", append(route(unix.RTN_LOCAL, unix.RT_TABLE_MAIN, unix.RT_TABLE_MAIN), done...), false, false},
		{"conflicting-table", append(route(unix.RTN_LOCAL, unix.RT_TABLE_MAIN, unix.RT_TABLE_LOCAL), done...), false, true},
		{"missing-terminal", local, false, true},
		{"trailing-byte", append(append(local, done...), 1), false, true},
		{"interrupted", append(local, message(unix.NLMSG_DONE, unix.NLM_F_DUMP_INTR, nil)...), false, true},
		{"terminal-error", append(local, message(unix.NLMSG_DONE, 0, []byte{1, 0, 0, 0})...), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes, err := decodeFull512GuestRouteDump(tc.raw, unix.AF_INET)
			if tc.wantFail {
				if err == nil || len(routes) != 0 {
					t.Fatalf("incomplete route dump accepted: %v, %v", routes, err)
				}
				return
			}
			if err != nil || len(routes) != 1 || routes[0].local != tc.want ||
				routes[0].destination != netip.MustParsePrefix("10.77.0.1/32") || routes[0].output != 1 {
				t.Fatalf("route dump = %v, %v", routes, err)
			}
		})
	}
}

// This readback samples the current guest, without creating test connections.
// The host launcher supplies the separately reviewed list of local host IPs.
func TestFull512GuestPreflight(t *testing.T) {
	if os.Getenv("CORSA_D1_FULL512_NATIVE_PREFLIGHT") != "1" {
		t.Skip("requires the admitted native guest preflight")
	}
	readFull512GuestPreflight(t, full512GuestHostPlan(t))
}

func full512GuestHostPlan(t *testing.T) []netip.Prefix {
	t.Helper()
	var planned []netip.Prefix
	raw := os.Getenv("CORSA_D1_FULL512_GUEST_HOSTS")
	if len(raw) == 0 || len(raw) > 64*1024 {
		t.Fatal("missing or oversized guest host plan")
	}
	for _, text := range strings.Split(raw, ",") {
		addr, err := netip.ParseAddr(text)
		if err != nil || addr.Zone() != "" {
			t.Fatal("invalid planned local guest host")
		}
		planned = append(planned, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return planned
}

func readFull512GuestPreflight(t *testing.T, planned []netip.Prefix) {
	t.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var proc unix.Statfs_t
	if err := unix.Statfs("/proc", &proc); err != nil || proc.Type != unix.PROC_SUPER_MAGIC {
		t.Fatal("guest procfs unavailable", err)
	}
	namespace, err := os.Readlink("/proc/thread-self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	if processNS, err := os.Readlink("/proc/self/ns/net"); err != nil || processNS != namespace {
		t.Fatal("guest reader and process network namespaces differ", err)
	}
	links, err := full512GuestLinks()
	if err != nil {
		t.Fatal(err)
	}
	routes, err := full512GuestRoutes()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFull512GuestNetwork(links, routes, planned); err != nil {
		t.Fatal(err)
	}
	swap, err := os.ReadFile("/proc/swaps")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFull512GuestSwap(string(swap)); err != nil {
		t.Fatal(err)
	}
	var limits unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limits); err != nil ||
		limits.Cur != full512FixtureFDLimit || limits.Max != full512FixtureFDLimit {
		t.Fatal("guest descriptor limits do not match Full512", limits, err)
	}
	descriptors, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	slack, err := full512FixtureFDSlack(int(limits.Cur), int(limits.Max), len(descriptors))
	if err != nil {
		t.Fatal(err)
	}
	if after, err := os.Readlink("/proc/thread-self/ns/net"); err != nil || after != namespace {
		t.Fatal("guest network namespace changed during readback", err)
	}
	t.Logf("D1_P0_GUEST_PREFLIGHT_V1 {\"soft_fd\":%d,\"hard_fd\":%d,\"observed_fd\":%d,\"snapshot_slack\":%d}",
		limits.Cur, limits.Max, len(descriptors), slack)
}
