package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ---------------------------------------------------------------------------
// Setup-failure cooldown — locally-driven dial gate.
//
// Background: when a peer is reachable on TCP and completes the welcome /
// auth handshake but then evicts our session during application-level
// setup (fetch_contacts, get_peers), CM emits
// cm_session_setup_failed. Without a dedicated cooldown the CM
// retry/replace loop puts the same address straight back into the dial
// queue every 2-3s. Combined with a small candidate pool (~7 bootstrap
// nodes), this produces a CPU-burning storm where no slot ever reaches
// Active.
//
// The cooldown sits ALONGSIDE the existing peer-health ConsecutiveFailures
// path. Health tracks ALL dial outcomes (TCP refused, version-incompatible,
// self-identity, EOF) and feeds peerCooldownDuration. setup_failures
// counts only the narrower setup-phase EOFs / explicit setup errors so a
// peer that fails setup specifically (its writer dropped our reply) is
// muted independently of whether the TCP layer thought the attempt
// succeeded.
//
// The contract pinned by setup_failure_test.go:
//
//   - first (setupFailureBanThreshold - 1) failures: count, no cooldown.
//   - reaching the threshold: cooldownUntil = now + setupFailureCooldown.
//   - further failures inside the cooldown: slide cooldownUntil forward
//     by setupFailureCooldown from each new failure timestamp. A peer
//     that keeps failing keeps the gate closed.
//   - clearSetupFailuresLocked wipes both counter and cooldown — called
//     on successful setup so an isolated bad period does not stigmatise
//     a peer permanently.
// ---------------------------------------------------------------------------

const (
	// setupFailureBanThreshold is the number of consecutive setup
	// failures that triggers a cooldown for the peer's address. Tuned
	// so the default CM reconnect cycle (3 retries with 2-4-8s backoff)
	// completes its natural budget before the cooldown engages — the
	// cooldown is a second-tier brake, not a replacement.
	setupFailureBanThreshold = 3

	// setupFailureCooldown is how long an address stays out of the
	// PeerProvider candidate set after hitting the threshold. Short
	// enough that a peer that recovered (its broadcast burst is over)
	// is retried in a reasonable timeframe; long enough that we stop
	// hammering full-routing-table bootstraps for at least a full
	// announce_routes cycle on their side.
	setupFailureCooldown = 30 * time.Second
)

// setupFailureEntry tracks per-address state for the setup-failure gate.
// All fields are mutated only under s.peerMu.
type setupFailureEntry struct {
	// Consecutive is the running count since the last clear. Reset by
	// clearSetupFailuresLocked on successful setup.
	Consecutive int

	// CooldownUntil is the wall-clock instant before which the address
	// is filtered out of Candidates(). Zero means "no active cooldown"
	// (still under threshold, or already cleared). Sliding window: each
	// additional failure inside the cooldown re-anchors this to
	// now + setupFailureCooldown.
	CooldownUntil time.Time

	// LastFailure is the instant of the most recent recordSetupFailureLocked
	// hit. Entries were previously removed ONLY by clearSetupFailuresLocked
	// (successful setup), so an address that never recovered — typical for
	// peers that vanished from the network — pinned its entry in
	// s.setupFailures forever. purgeStaleSetupFailuresLocked (ban_purge.go)
	// uses this to age out entries idle past setupFailureIdleTTL.
	LastFailure time.Time
}

// recordSetupFailureLocked increments the per-address counter and arms
// the cooldown when the threshold is reached. Lazily allocates the
// underlying map so an uninitialised Service (test helpers, zero value)
// does not panic. Caller must hold s.peerMu.
func (s *Service) recordSetupFailureLocked(addr domain.PeerAddress, now time.Time) {
	if s.setupFailures == nil {
		s.setupFailures = make(map[domain.PeerAddress]*setupFailureEntry)
	}
	entry := s.setupFailures[addr]
	if entry == nil {
		entry = &setupFailureEntry{}
		s.setupFailures[addr] = entry
	}
	entry.Consecutive++
	entry.LastFailure = now
	if entry.Consecutive >= setupFailureBanThreshold {
		entry.CooldownUntil = now.Add(setupFailureCooldown)
	}
}

// clearSetupFailuresLocked removes the address from the tracker. Called
// on successful setup (initPeerSession returned nil) so an isolated bad
// period does not pin the peer in cooldown forever. Caller must hold
// s.peerMu.
func (s *Service) clearSetupFailuresLocked(addr domain.PeerAddress) {
	if s.setupFailures == nil {
		return
	}
	delete(s.setupFailures, addr)
}

// IsSetupFailureBanned reports whether the address is currently inside
// an active setup-failure cooldown. Safe for concurrent callers
// (PeerProvider.SetupFailureBannedFn). Takes the peer-domain read lock.
func (s *Service) IsSetupFailureBanned(addr domain.PeerAddress) bool {
	return s.isSetupFailureBannedAt(addr, time.Now())
}

// isSetupFailureBannedAt is the time-injectable form of
// IsSetupFailureBanned. Used by tests to assert the contract at a
// deterministic instant without sleeping.
func (s *Service) isSetupFailureBannedAt(addr domain.PeerAddress, now time.Time) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	entry := s.setupFailures[addr]
	if entry == nil {
		return false
	}
	if entry.CooldownUntil.IsZero() {
		return false
	}
	return now.Before(entry.CooldownUntil)
}

// setupFailureExceedsThresholdLocked reports whether the per-address
// failure counter has crossed setupFailureBanThreshold. Used by
// the routing-quarantine trigger in onCMSessionEstablished's
// setup-failed branch to escalate from B1 (per-address cooldown for
// dial attempts) to also marking the peer's identity as route-
// quarantined. Caller must hold s.peerMu (the existing setup-failed
// cleanup path already holds it).
func (s *Service) setupFailureExceedsThresholdLocked(addr domain.PeerAddress) bool {
	if s.setupFailures == nil {
		return false
	}
	entry := s.setupFailures[addr]
	if entry == nil {
		return false
	}
	return entry.Consecutive >= setupFailureBanThreshold
}
