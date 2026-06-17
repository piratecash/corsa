package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestCandidates_ConnectOnlyPinsSingleTarget verifies that when the
// connectOnly egress pin is active, Candidates() yields only the pinned
// address and drops every other known peer.
func TestCandidates_ConnectOnlyPinsSingleTarget(t *testing.T) {
	target := mustAddr("5.6.7.8:64646")
	cfg := testProviderConfig()
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) { return target, true }
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(target, domain.PeerSourceBootstrap)
	pp.Add(mustAddr("9.9.9.9:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (pinned), got %d: %v", len(candidates), candidates)
	}
	if candidates[0].Address != target {
		t.Errorf("expected pinned target %v, got %v", target, candidates[0].Address)
	}
}

// TestCandidates_ConnectOnlyInactivePassesAll verifies that when the pin
// reports ok=false, dialing is unrestricted and all peers pass.
func TestCandidates_ConnectOnlyInactivePassesAll(t *testing.T) {
	cfg := testProviderConfig()
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) { return "", false }
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates (unrestricted), got %d", len(candidates))
	}
}

// TestCandidates_ConnectOnlyTargetUnknownYieldsNone verifies that pinning to
// an address not present in the known set produces zero candidates — the
// node dials nobody until the pinned target is registered (the runtime
// command registers it via the operator add-peer path).
func TestCandidates_ConnectOnlyTargetUnknownYieldsNone(t *testing.T) {
	cfg := testProviderConfig()
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) {
		return mustAddr("203.0.113.5:64646"), true
	}
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidates (pinned target unknown), got %d", len(candidates))
	}
}

// TestCandidates_ConnectOnlyPinSurvivesPrivateFilter verifies that a pinned
// LAN/private target is NOT dropped by the private/loopback auto-dial guard,
// so it keeps being re-dialed (the "retrying indefinitely" contract) even
// though AllowPrivateCandidates is off.
func TestCandidates_ConnectOnlyPinSurvivesPrivateFilter(t *testing.T) {
	pin := mustAddr("10.0.0.2:64646")
	cfg := testProviderConfig()
	cfg.AllowPrivateCandidates = false // private auto-dial guard ON
	// Mirror production ForbiddenFn (svc.isForbiddenDialIP), which rejects
	// RFC1918/loopback in the default mode — the pin must bypass it too.
	cfg.ForbiddenFn = func(ip net.IP) bool {
		return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
	}
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) { return pin, true }
	pp := NewPeerProvider(cfg)

	pp.Add(pin, domain.PeerSourceManual)
	pp.Add(mustAddr("10.0.0.3:64646"), domain.PeerSourceManual)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected pinned private target to survive, got %d: %v", len(candidates), candidates)
	}
	if candidates[0].Address != pin {
		t.Errorf("expected pinned target %v, got %v", pin, candidates[0].Address)
	}
}

// TestCandidates_PrivateFilterStillAppliesWithoutPin guards the carve-out: a
// private peer is still dropped when it is NOT the pinned target.
func TestCandidates_PrivateFilterStillAppliesWithoutPin(t *testing.T) {
	cfg := testProviderConfig()
	cfg.AllowPrivateCandidates = false
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) { return "", false }
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("10.0.0.2:64646"), domain.PeerSourceManual)

	if candidates := pp.Candidates(); len(candidates) != 0 {
		t.Fatalf("expected private peer filtered without pin, got %v", candidates)
	}
}

// TestBuildDialAddresses_ConnectOnlyExact verifies that the pinned target is
// dialled at exactly its address — the default-port fallback that a
// non-default port would normally add is suppressed for the pin.
func TestBuildDialAddresses_ConnectOnlyExact(t *testing.T) {
	pin := mustAddr("5.6.7.8:7777")

	// Baseline: without a pin, a non-default port adds the default-port
	// fallback.
	cfg := testProviderConfig()
	pp := NewPeerProvider(cfg)
	if got := pp.BuildDialAddresses(pin); len(got) != 2 {
		t.Fatalf("expected 2 dial addresses (with fallback) absent pin, got %v", got)
	}

	// With the pin active, only the exact address is dialled.
	cfg = testProviderConfig()
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) { return pin, true }
	pp = NewPeerProvider(cfg)
	got := pp.BuildDialAddresses(pin)
	if len(got) != 1 || got[0] != pin {
		t.Fatalf("expected exact [%v] for pinned dial, got %v", pin, got)
	}
}

// TestCandidates_ConnectOnlyDialAddressesExact verifies the Candidates()
// re-dial path also yields exact dial addresses for the pinned target.
func TestCandidates_ConnectOnlyDialAddressesExact(t *testing.T) {
	pin := mustAddr("5.6.7.8:7777")
	cfg := testProviderConfig()
	cfg.ConnectOnlyFn = func() (domain.PeerAddress, bool) { return pin, true }
	pp := NewPeerProvider(cfg)

	pp.Add(pin, domain.PeerSourceManual)
	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (pinned), got %d", len(candidates))
	}
	da := candidates[0].DialAddresses
	if len(da) != 1 || da[0] != pin {
		t.Errorf("expected exact dial addresses [%v], got %v", pin, da)
	}
}

// TestCandidates_NilConnectOnlyFnUnrestricted verifies the production-optional
// callback being nil leaves dialing unrestricted (no panic, all peers pass).
func TestCandidates_NilConnectOnlyFnUnrestricted(t *testing.T) {
	cfg := testProviderConfig()
	cfg.ConnectOnlyFn = nil
	pp := NewPeerProvider(cfg)

	pp.Add(mustAddr("1.2.3.4:64646"), domain.PeerSourceBootstrap)
	pp.Add(mustAddr("5.6.7.8:64646"), domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
}
