package node

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"
)

const (
	full512FixtureRegistryLimit = 1024
	full512FixtureConnections   = full512FixtureRegistryLimit + 1
	full512FixtureFDLimit       = 4096
)

// full512FixtureFDSlack applies to a process owning both socket ends.
// otherFDs accounts for the listener, logs and all other descriptors.
func full512FixtureFDSlack(soft, hard, otherFDs int) (int, error) {
	if soft != full512FixtureFDLimit || hard != full512FixtureFDLimit || otherFDs < 0 {
		return 0, fmt.Errorf("unexpected full512 fixture descriptor limits or count")
	}
	if otherFDs > soft-2*full512FixtureConnections {
		return 0, fmt.Errorf("insufficient descriptor capacity for both connection ends")
	}
	return soft - 2*full512FixtureConnections - otherFDs, nil
}

type full512GuestLink struct {
	index     int
	flags     net.Flags
	addresses []netip.Prefix
}

type full512GuestRoute struct {
	destination netip.Prefix
	output      int
	local       bool
}

func checkFull512GuestNetwork(links []full512GuestLink, routes []full512GuestRoute, planned []netip.Prefix) error {
	allowed := map[netip.Prefix]bool{
		netip.MustParsePrefix("127.0.0.1/8"): false,
		netip.MustParsePrefix("::1/128"):     false,
	}
	for _, prefix := range planned {
		if !prefix.IsValid() || !prefix.Addr().IsPrivate() || prefix.Addr().IsLoopback() ||
			prefix.Bits() != prefix.Addr().BitLen() {
			return fmt.Errorf("invalid planned guest host address %v", prefix)
		}
		if _, exists := allowed[prefix]; exists {
			return fmt.Errorf("repeated planned guest address %v", prefix)
		}
		allowed[prefix] = false
	}
	loopback := 0
	for _, link := range links {
		if link.flags&net.FlagLoopback == 0 {
			if link.flags&net.FlagUp != 0 || len(link.addresses) != 0 {
				return fmt.Errorf("non-loopback link %d is configured", link.index)
			}
			continue
		}
		if loopback != 0 || link.index <= 0 || link.flags&net.FlagUp == 0 {
			return fmt.Errorf("invalid guest loopback link")
		}
		loopback = link.index
		for _, prefix := range link.addresses {
			seen, expected := allowed[prefix]
			if !expected || seen {
				return fmt.Errorf("unexpected guest address %v", prefix)
			}
			allowed[prefix] = true
		}
	}
	for prefix, seen := range allowed {
		if !seen && prefix != netip.MustParsePrefix("::1/128") {
			return fmt.Errorf("missing guest address %v", prefix)
		}
	}
	localV4 := netip.MustParsePrefix("127.0.0.0/8")
	localV6 := netip.MustParsePrefix("::1/128")
	localRoutes := make(map[netip.Prefix]bool, len(planned))
	for _, route := range routes {
		prefix := route.destination
		if !prefix.IsValid() || prefix != prefix.Masked() || route.output != loopback {
			return fmt.Errorf("guest route does not use the validated loopback link: %v", prefix)
		}
		if prefix.Bits() >= localV4.Bits() && localV4.Contains(prefix.Addr()) || prefix == localV6 {
			continue
		}
		if !allowed[prefix] {
			return fmt.Errorf("unplanned guest route %v", prefix)
		}
		if route.local {
			localRoutes[prefix] = true
		}
	}
	if len(routes) == 0 {
		return fmt.Errorf("guest route inventory is empty")
	}
	for _, prefix := range planned {
		if !localRoutes[prefix] {
			return fmt.Errorf("missing local guest route %v", prefix)
		}
	}
	return nil
}

func checkFull512GuestSwap(raw string) error {
	fields := strings.Fields(raw)
	if strings.Join(fields, " ") != "Filename Type Size Used Priority" {
		return fmt.Errorf("guest swap is active or its inventory is incomplete")
	}
	return nil
}

func TestFull512GuestReadbackPolicy(t *testing.T) {
	loop := netip.MustParsePrefix("127.0.0.1/8")
	local := netip.MustParsePrefix("127.0.0.0/8")
	planned := netip.MustParsePrefix("10.0.0.1/32")
	links := []full512GuestLink{{1, net.FlagUp | net.FlagLoopback, []netip.Prefix{loop, planned}}, {index: 2}}
	routes := []full512GuestRoute{
		{destination: local, output: 1, local: true},
		{destination: planned, output: 1, local: true},
	}
	if err := checkFull512GuestNetwork(links, routes, []netip.Prefix{planned}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		links  []full512GuestLink
		routes []full512GuestRoute
		plan   []netip.Prefix
	}{
		{"missing-planned", []full512GuestLink{{1, net.FlagUp | net.FlagLoopback, []netip.Prefix{loop}}}, routes, []netip.Prefix{planned}},
		{"unexpected-active-link", []full512GuestLink{links[0], {index: 2, flags: net.FlagUp}}, routes, []netip.Prefix{planned}},
		{"unexpected-address-on-down-link", []full512GuestLink{links[0], {index: 2, addresses: []netip.Prefix{planned}}}, routes, []netip.Prefix{planned}},
		{"default-route", links, []full512GuestRoute{{destination: netip.MustParsePrefix("0.0.0.0/0"), output: 1}}, []netip.Prefix{planned}},
		{"route-on-other-link", links, []full512GuestRoute{{destination: local, output: 2}}, []netip.Prefix{planned}},
		{"missing-planned-local-route", links, routes[:1], []netip.Prefix{planned}},
		{"nonlocal-planned-route", links, []full512GuestRoute{routes[0], {destination: planned, output: 1}}, []netip.Prefix{planned}},
		{"public-plan", links, routes, []netip.Prefix{netip.MustParsePrefix("192.0.2.1/32")}},
		{"broad-plan", links, routes, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}},
		{"duplicate-plan", links, routes, []netip.Prefix{planned, planned}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkFull512GuestNetwork(tc.links, tc.routes, tc.plan); err == nil {
				t.Fatal("invalid guest network was accepted")
			}
		})
	}
	for _, raw := range []string{"", "Filename Type Size Used Priority\n/swap file 1 0 -2\n"} {
		if err := checkFull512GuestSwap(raw); err == nil {
			t.Fatal("empty inventory or active swap was accepted")
		}
	}
	if err := checkFull512GuestSwap("Filename\tType\tSize\tUsed\tPriority\n"); err != nil {
		t.Fatal(err)
	}
}

func TestFull512GuestReadbackRequiresAllPlannedLocalRoutes(t *testing.T) {
	const size = 1026
	loopback := full512GuestLink{index: 1, flags: net.FlagUp | net.FlagLoopback}
	loopback.addresses = append(loopback.addresses, netip.MustParsePrefix("127.0.0.1/8"))
	planned := make([]netip.Prefix, 0, size)
	routes := make([]full512GuestRoute, 0, size)
	for i := 0; i < size; i++ {
		host := netip.AddrFrom4([4]byte{10, 77, byte(i / 254), byte(i%254 + 1)})
		prefix := netip.PrefixFrom(host, 32)
		planned = append(planned, prefix)
		loopback.addresses = append(loopback.addresses, prefix)
		routes = append(routes, full512GuestRoute{destination: prefix, output: 1, local: true})
	}
	links := []full512GuestLink{loopback}
	if err := checkFull512GuestNetwork(links, routes, planned); err != nil {
		t.Fatal(err)
	}
	routes[len(routes)-1].local = false
	if err := checkFull512GuestNetwork(links, routes, planned); err == nil ||
		!strings.Contains(err.Error(), "missing local guest route 10.77.4.10/32") {
		t.Fatalf("last host route was not required: %v", err)
	}
}
