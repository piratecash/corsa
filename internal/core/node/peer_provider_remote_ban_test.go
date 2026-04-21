package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Gate 6c.2 — honour-remote-ban dial filter.
//
// These tests pin the behaviour of the RemoteBannedFn callback inside
// PeerProvider.Candidates(). The gate exists so we stop dialling peers that
// already told us (via connection_notice{code=peer-banned}) they will
// reject us — the same cascade of cm_session_setup_failed → ebus storm that
// the publisher-side dedup + heartbeat work addressed from the other end.
// If any of these tests fail, the dialler is either ignoring remote bans
// or over-filtering peers that should still be reachable.
// ---------------------------------------------------------------------------

// TestCandidates_RemoteBannedSkipsPeer proves the positive gate: with the
// callback returning true, the peer must not appear in Candidates(). The
// co-located non-banned peer confirms the gate is per-address, not a global
// toggle that accidentally drops every candidate.
func TestCandidates_RemoteBannedSkipsPeer(t *testing.T) {
	t.Parallel()

	banned := mustAddr("1.2.3.4:64646")
	clean := mustAddr("5.6.7.8:64646")

	cfg := testProviderConfig()
	cfg.RemoteBannedFn = func(a domain.PeerAddress) bool {
		return a == banned
	}

	pp := NewPeerProvider(cfg)
	pp.Add(banned, domain.PeerSourceBootstrap)
	pp.Add(clean, domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (remote-banned filtered), got %d", len(candidates))
	}
	if candidates[0].Address != clean {
		t.Fatalf("unexpected candidate %v, want %v", candidates[0].Address, clean)
	}
}

// TestCandidates_RemoteBannedFalseAllowsPeer pins the negative case —
// when the callback reports false (no active ban or ban elapsed), the peer
// must pass through the gate. Otherwise a peer whose ban just expired would
// never be redialled without a full PeerProvider rebuild.
func TestCandidates_RemoteBannedFalseAllowsPeer(t *testing.T) {
	t.Parallel()

	addr := mustAddr("1.2.3.4:64646")

	cfg := testProviderConfig()
	cfg.RemoteBannedFn = func(domain.PeerAddress) bool { return false }

	pp := NewPeerProvider(cfg)
	pp.Add(addr, domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate when gate is open, got %d", len(candidates))
	}
	if candidates[0].Address != addr {
		t.Fatalf("unexpected candidate %v, want %v", candidates[0].Address, addr)
	}
}

// TestCandidates_RemoteBannedNilCallbackAllowsPeer ensures the gate is
// optional: a nil RemoteBannedFn (e.g. a test harness that doesn't wire it)
// must not panic and must not silently filter every peer. Without this the
// feature would be load-bearing even for configurations that never use it.
func TestCandidates_RemoteBannedNilCallbackAllowsPeer(t *testing.T) {
	t.Parallel()

	addr := mustAddr("1.2.3.4:64646")

	cfg := testProviderConfig()
	cfg.RemoteBannedFn = nil

	pp := NewPeerProvider(cfg)
	pp.Add(addr, domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate with nil callback, got %d", len(candidates))
	}
}

// TestCandidates_RemoteBannedDoesNotAffectKnownPeers confirms that the gate
// only influences Candidates() output, not the underlying known set. A
// remote ban is a dialling hint; it must not evict the peer from the
// registry (otherwise the operator would lose visibility via list_peers).
func TestCandidates_RemoteBannedDoesNotAffectKnownPeers(t *testing.T) {
	t.Parallel()

	addr := mustAddr("1.2.3.4:64646")

	cfg := testProviderConfig()
	cfg.RemoteBannedFn = func(domain.PeerAddress) bool { return true }

	pp := NewPeerProvider(cfg)
	pp.Add(addr, domain.PeerSourceBootstrap)

	if got := pp.Count(); got != 1 {
		t.Fatalf("remote ban must not evict peer; Count() = %d, want 1", got)
	}
	if pp.KnownPeerStatic(addr) == nil {
		t.Fatal("remote-banned peer must still be retrievable via KnownPeerStatic")
	}
}
