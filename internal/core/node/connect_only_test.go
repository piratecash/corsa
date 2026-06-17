package node

import (
	"net"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestResolveConnectOnlyHost_PassThrough verifies IP literals and overlay
// hosts are returned unchanged (no DNS lookup).
func TestResolveConnectOnlyHost_PassThrough(t *testing.T) {
	cases := []string{
		"1.2.3.4",
		"2001:db8::1",
		"127.0.0.1",
		strings.Repeat("a", 56) + ".onion",
		strings.Repeat("b", 52) + ".b32.i2p",
	}
	for _, host := range cases {
		got, err := resolveConnectOnlyHost(host)
		if err != nil {
			t.Errorf("resolveConnectOnlyHost(%q): unexpected error %v", host, err)
			continue
		}
		if got != host {
			t.Errorf("resolveConnectOnlyHost(%q) = %q, want unchanged", host, got)
		}
	}
}

// TestResolveConnectOnlyHost_DNSName verifies a plain hostname is resolved to
// an IP. Uses "localhost" which resolves via the hosts file without network;
// skips if the environment cannot resolve it.
func TestResolveConnectOnlyHost_DNSName(t *testing.T) {
	got, err := resolveConnectOnlyHost("localhost")
	if err != nil {
		t.Skipf("localhost not resolvable in this environment: %v", err)
	}
	if net.ParseIP(got) == nil {
		t.Errorf("resolveConnectOnlyHost(localhost) = %q, want an IP literal", got)
	}
}

// TestConnectOnlyTargetRoundTrip verifies the atomic pin store/load and that
// connectOnlyTarget reports presence correctly.
func TestConnectOnlyTargetRoundTrip(t *testing.T) {
	s := &Service{}

	if _, ok := s.connectOnlyTarget(); ok {
		t.Fatal("expected no pin on a fresh service")
	}

	want := domain.PeerAddress("5.6.7.8:64646")
	s.connectOnly.Store(&want)

	got, ok := s.connectOnlyTarget()
	if !ok {
		t.Fatal("expected pin to be active after store")
	}
	if got != want {
		t.Errorf("expected pin %v, got %v", want, got)
	}
}

// TestConnectOnlyFrameDisableTokens verifies that an empty argument and each
// disable token clear an active pin via the frame entrypoint. connManager is
// nil here, exercising the nil-guard in disableConnectOnly.
func TestConnectOnlyFrameDisableTokens(t *testing.T) {
	for _, raw := range []string{"", "off", "none", "clear", "   "} {
		s := &Service{}
		pin := domain.PeerAddress("5.6.7.8:64646")
		s.connectOnly.Store(&pin)

		peers := []string{}
		if raw != "" {
			peers = []string{raw}
		}
		reply := s.connectOnlyFrame(protocol.Frame{Type: "connect_only", Peers: peers})

		if reply.Type != "ok" {
			t.Errorf("raw %q: expected ok, got %q (%s)", raw, reply.Type, reply.Error)
		}
		if _, ok := s.connectOnlyTarget(); ok {
			t.Errorf("raw %q: expected pin cleared", raw)
		}
	}
}

// TestConnectOnlyRePinFailurePreservesPrevious verifies that a failed re-pin
// (target rejected by admission) restores the PREVIOUS pin instead of leaving
// egress unrestricted. reachableGroups is nil here, so applyAddPeer rejects the
// new target at canReach before any peer-map mutation.
func TestConnectOnlyRePinFailurePreservesPrevious(t *testing.T) {
	s := &Service{cfg: config.Node{ListenAddress: ":1"}}

	prev := domain.PeerAddress("9.9.9.9:64646")
	s.connectOnly.Store(&prev)

	reply := s.enableConnectOnly("8.8.8.8:64646")
	if reply.Type != "error" {
		t.Fatalf("expected admission error, got %q", reply.Type)
	}

	got, ok := s.connectOnlyTarget()
	if !ok {
		t.Fatal("expected previous pin to be preserved after failed re-pin")
	}
	if got != prev {
		t.Errorf("expected previous pin %v, got %v", prev, got)
	}
}

// TestApplyStartupConnectOnly_FailClosed verifies that an invalid
// CORSA_CONNECT_ONLY seed blocks egress (sets the pin) rather than falling
// back to unrestricted dialing. A self-target is used as the invalid seed.
func TestApplyStartupConnectOnly_FailClosed(t *testing.T) {
	s := &Service{cfg: config.Node{ListenAddress: ":64646", ConnectOnly: ":64646"}}

	s.applyStartupConnectOnly()

	got, ok := s.connectOnlyTarget()
	if !ok {
		t.Fatal("expected egress blocked (pin set) after invalid startup seed")
	}
	if got != connectOnlyBlockedSentinel {
		t.Errorf("expected fail-closed sentinel pin, got %v", got)
	}
}

// TestCandidates_ConnectOnlyBlockedSentinelAdmitsNothing verifies the
// fail-closed sentinel pin yields zero candidates even when a forbidden/private
// peer is already known (e.g. restored from peers.json), so egress is truly
// blocked.
func TestCandidates_ConnectOnlyBlockedSentinelAdmitsNothing(t *testing.T) {
	cfg := testProviderConfig()
	cfg.AllowPrivateCandidates = false
	cfg.ForbiddenFn = func(ip net.IP) bool {
		return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
	}
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) {
		return connectOnlyBlockedSentinel, true
	}
	pp := NewPeerProvider(cfg)

	// A forbidden/private peer plus a public one — both must be excluded.
	pp.Add(mustAddr("10.0.0.2:64646"), domain.PeerSourceManual)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	if candidates := pp.Candidates(); len(candidates) != 0 {
		t.Fatalf("expected zero candidates under fail-closed sentinel, got %v", candidates)
	}
}

// TestApplyStartupConnectOnly_BlankNoop verifies a blank seed leaves dialing
// unrestricted.
func TestApplyStartupConnectOnly_BlankNoop(t *testing.T) {
	s := &Service{cfg: config.Node{ListenAddress: ":64646"}}

	s.applyStartupConnectOnly()

	if _, ok := s.connectOnlyTarget(); ok {
		t.Error("expected no pin for a blank startup seed")
	}
}

// TestConnectOnlyRejectsSelf verifies a pin to our own listen address is
// refused and leaves the pin unset (no zero-egress strand).
func TestConnectOnlyRejectsSelf(t *testing.T) {
	s := &Service{cfg: config.Node{ListenAddress: ":64646"}}

	reply := s.connectOnlyFrame(protocol.Frame{Type: "connect_only", Peers: []string{":64646"}})

	if reply.Type != "error" {
		t.Fatalf("expected error for self pin, got %q", reply.Type)
	}
	if _, ok := s.connectOnlyTarget(); ok {
		t.Error("expected no pin after self rejection")
	}
}
