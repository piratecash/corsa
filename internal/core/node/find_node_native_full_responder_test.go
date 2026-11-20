//go:build d1_o1a_native_responder

package node

import (
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/trustedclock"
	"testing"
	"time"
)

func nativeResponderFixture(t *testing.T) (*Service, *RegisteredProvenV2Observation, *D1NativeFullResponderReadout) {
	t.Helper()
	cfg, id := readyTestInputs(t)
	cfg.ListenAddress = "240.70.1.2:64646"
	cfg.BootstrapPeers = []string{"240.70.2.2:64646"}
	cfg.DisableDirectMessages = true
	cfg.ListenerSet = true
	cfg.ListenerEnabled = true
	cfg.AllowPrivatePeers = true
	s, c, r, cleanup, err := NewD1NativeFullResponderWithRouteWireCensus(cfg, id, nil, 4, [32]byte{9}, 10000, domain.RouteWireListener)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanup(); s.WaitBackground() })
	return s, c, r
}
func nativeResponderCensus(t *testing.T, s *Service, c *RegisteredProvenV2Observation) ([]RouteWireCensusEndpoint, time.Duration, time.Duration) {
	t.Helper()
	begin := s.k3Registered.baseline.After + time.Nanosecond
	end, err := trustedclock.SinceBoot()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := c.RouteWireCensus(begin, end)
	if err != nil || len(rows) != 1 {
		t.Fatal("genuine original registry census unavailable", err, len(rows))
	}
	return rows, begin, end
}
func TestNativeFullResponderOriginalReadyProfileAndDefaults(t *testing.T) {
	s, c, r := nativeResponderFixture(t)
	if !r.boundTo(s) || c.source != r.source || r.ready.originalLocalRole != findNodeRoleResponder || s.localFindNodeRole != findNodeRoleResponder || s.cfg.Type != config.NodeTypeFull || s.connManager.config.MaxSlotsFn() != 0 {
		t.Fatal("native original Ready/role/census/dial binding unavailable")
	}
	if s.degree.profile.MaxActive != 512 || r.ready.originalCapture.priority.MaxMembers != 512 || r.ready.originalCapture.priority.MaxRegistryVisits != 1024 {
		t.Fatal("wrong diagnostic allocation")
	}
	_, stock := localRoleReady(t, findNodeRoleOff, domain.NodeTypeFull)
	if stock.localFindNodeRole != findNodeRoleOff || stock.k3Registered != nil {
		t.Fatal("ordinary Off gained native role/observer")
	}
}
func TestNativeFullResponderGuardGenuineF1NoPeerRAndRetirement(t *testing.T) {
	s, c, r := nativeResponderFixture(t)
	_, peer := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
	pair := localRolePair(t, s, peer)
	localRolePeerDeclaration(t, pair.acceptedProof, false)
	localRolePeerDeclaration(t, pair.dialedProof, true)

	rows, begin, end := nativeResponderCensus(t, s, c)
	if err := r.GuardCurrentForService(s, rows, begin, end); err != nil {
		t.Fatal("genuine live F1/immutable Ready R rejected", err)
	}
	old := rows[0]
	old.ConnID += 1000
	old.Interval.EndBefore = end - time.Nanosecond
	old.Interval.EndAfter = end - time.Nanosecond
	if err := r.GuardCurrentForService(s, []RouteWireCensusEndpoint{old, rows[0]}, begin, end); err != nil {
		t.Fatal("historical census endpoint incorrectly required current F1", err)
	}
	if r.GuardCurrentForService(s, []RouteWireCensusEndpoint{old}, begin, end) == nil {
		t.Fatal("historical-only guard claimed positive live evidence")
	}
	pair.inbound.end()
	if r.GuardCurrentForService(s, rows, begin, end) == nil {
		t.Fatal("retired original lifetime supplied live evidence")
	}
}
func TestNativeFullResponderGuardRejectsForeignOriginalBindings(t *testing.T) {
	s, c, r := nativeResponderFixture(t)
	_, peer := localRoleReady(t, findNodeRoleTarget, domain.NodeTypeClient)
	pair := localRolePair(t, s, peer)
	rows, begin, end := nativeResponderCensus(t, s, c)
	for _, mode := range []string{"unpublished", "role", "session", "peer", "direction", "ConnID", "foreign-service", "nil-ready"} {
		t.Run(mode, func(t *testing.T) {
			changed := append([]RouteWireCensusEndpoint(nil), rows...)
			life := pair.inbound.life
			life.mu.Lock()
			saved := life.findNode.published
			role := s.localFindNodeRole
			switch mode {
			case "unpublished":
				life.findNode.published = false
			}
			life.mu.Unlock()
			expected := s
			ready := r.ready
			switch mode {
			case "role":
				s.localFindNodeRole = findNodeRoleTarget
			case "session":
				changed[0].SessionID[0] ^= 1
			case "peer":
				changed[0].Peer = domain.PeerIdentityFromWire(s.Address())
			case "direction":
				changed[0].Direction = domain.PeerDirectionOutbound
			case "ConnID":
				changed[0].ConnID++
			case "foreign-service":
				expected = peer
			case "nil-ready":
				r.ready = nil
			}
			err := r.GuardCurrentForService(expected, changed, begin, end)
			r.ready = ready
			life.mu.Lock()
			life.findNode.published = saved
			s.localFindNodeRole = role
			life.mu.Unlock()
			if err == nil {
				t.Fatal("foreign/missing original evidence accepted", mode)
			}
		})
	}
}
func TestNativeFullResponderGuardNoNegotiatedF1IsUnavailable(t *testing.T) {
	s, c, r := nativeResponderFixture(t)
	_, peer := localRoleReady(t, findNodeRoleOff, domain.NodeTypeClient)
	localRolePair(t, s, peer)
	rows, begin, end := nativeResponderCensus(t, s, c)
	if r.GuardCurrentForService(s, rows, begin, end) == nil {
		t.Fatal("ordinary V2 census was renamed into F1/R proof")
	}
}
func TestNativeFullResponderRejectsForeignNativePlanBeforeConstruction(t *testing.T) {
	cfg, id := readyTestInputs(t)
	cfg.ListenAddress = "240.70.1.2:64646"
	cfg.BootstrapPeers = []string{"240.70.2.2:64646"}
	cfg.DisableDirectMessages = true
	cfg.ListenerSet = true
	cfg.ListenerEnabled = true
	for _, mode := range []string{"loopback", "public", "client", "DM", "proxy", "bootstrap", "zero-window", "no-10k"} {
		t.Run(mode, func(t *testing.T) {
			bad := cfg
			window := [32]byte{9}
			seed := 10000
			switch mode {
			case "loopback":
				bad.ListenAddress = "127.0.0.1:64646"
			case "public":
				bad.ListenAddress = "8.8.8.8:64646"
			case "client":
				bad.Type = config.NodeTypeClient
			case "DM":
				bad.DisableDirectMessages = false
			case "proxy":
				bad.ProxyAddress = "127.0.0.1:9050"
			case "bootstrap":
				bad.BootstrapPeers = []string{"127.0.0.1:64646"}
			case "zero-window":
				window = [32]byte{}
			case "no-10k":
				seed = 0
			}
			s, c, r, cleanup, err := NewD1NativeFullResponderWithRouteWireCensus(bad, id, nil, 4, window, seed, domain.RouteWireListener)
			if err == nil || s != nil || c != nil || r != nil || cleanup != nil {
				t.Fatal("foreign native plan constructed", mode, err)
			}
		})
	}
}
