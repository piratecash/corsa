package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Gate 6c.3 — setup-failure cooldown dial filter.
//
// Mirrors the RemoteBannedFn gate (6c.2) but is driven from the local side:
// after N consecutive cm_session_setup_failed events against the same peer
// address, Service places that address into a cooldown window. PeerProvider
// must honour the SetupFailureBannedFn callback to skip the peer until the
// cooldown elapses, otherwise the CM keeps re-dialling bootstrap nodes that
// can never complete the application-level setup — producing the CPU storm
// observed when peer-side announce_routes flush evicts our handshake reply.
// ---------------------------------------------------------------------------

func TestCandidates_SetupFailureBannedSkipsPeer(t *testing.T) {
	t.Parallel()

	banned := mustAddr("1.2.3.4:64646")
	clean := mustAddr("5.6.7.8:64646")

	cfg := testProviderConfig()
	cfg.SetupFailureBannedFn = func(a domain.PeerAddress) bool {
		return a == banned
	}

	pp := NewPeerProvider(cfg)
	pp.Add(banned, domain.PeerSourceBootstrap)
	pp.Add(clean, domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate (setup-failure filtered), got %d", len(candidates))
	}
	if candidates[0].Address != clean {
		t.Fatalf("unexpected candidate %v, want %v", candidates[0].Address, clean)
	}
}

func TestCandidates_SetupFailureBannedFalseAllowsPeer(t *testing.T) {
	t.Parallel()

	addr := mustAddr("1.2.3.4:64646")

	cfg := testProviderConfig()
	cfg.SetupFailureBannedFn = func(domain.PeerAddress) bool { return false }

	pp := NewPeerProvider(cfg)
	pp.Add(addr, domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate when gate is open, got %d", len(candidates))
	}
}

func TestCandidates_SetupFailureBannedNilCallbackAllowsPeer(t *testing.T) {
	t.Parallel()

	addr := mustAddr("1.2.3.4:64646")

	cfg := testProviderConfig()
	cfg.SetupFailureBannedFn = nil

	pp := NewPeerProvider(cfg)
	pp.Add(addr, domain.PeerSourceBootstrap)

	candidates := pp.Candidates()
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate with nil callback, got %d", len(candidates))
	}
}

func TestCandidates_SetupFailureBannedDoesNotAffectKnownPeers(t *testing.T) {
	t.Parallel()

	addr := mustAddr("1.2.3.4:64646")

	cfg := testProviderConfig()
	cfg.SetupFailureBannedFn = func(domain.PeerAddress) bool { return true }

	pp := NewPeerProvider(cfg)
	pp.Add(addr, domain.PeerSourceBootstrap)

	if got := pp.Count(); got != 1 {
		t.Fatalf("setup-failure ban must not evict peer; Count() = %d, want 1", got)
	}
	if pp.KnownPeerStatic(addr) == nil {
		t.Fatal("setup-failure-banned peer must still be retrievable via KnownPeerStatic")
	}
}
