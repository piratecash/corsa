package node

import (
	"time"

	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// Periodic purge for the ban / blacklist / setup-failure domain.
//
// Background: the cluster-mesh work (v1.0.47..v1.0.49) fixed map hygiene
// for the route-quarantine domain (purgeRouteQuarantineState), but the
// IP-ban domain kept its original lazy-removal model:
//
//   - s.bans            — deleted only in isBlacklistedConn, i.e. only if
//                         the SAME IP connects again after its ban lapsed.
//                         An IP that never returns leaks its entry forever;
//                         sub-threshold score entries had no expiry at all.
//   - s.bannedIPSet     — filtered on read (flushPeerState, BannedIPsFn)
//                         but deleted only on peer recovery or operator
//                         addpeer override.
//   - s.remoteBannedIPs — filtered on read (isRemoteIPBannedLocked) but
//                         deleted only via clearRemoteIPBanLocked on
//                         recovery.
//   - s.setupFailures   — deleted only by clearSetupFailuresLocked on a
//                         SUCCESSFUL setup; addresses that never recover
//                         (gone from the mesh) pin their entry forever.
//
// Individually the entries are small, but on a long-running node every
// transient misbehaving IP and every dead dial target leaves a permanent
// residue, which matches the slow monotonic RSS growth observed after
// v1.0.47. purgeExpiredBanState is the missing counterpart of
// purgeRouteQuarantineState and runs from the same bootstrapLoop tick.
// ---------------------------------------------------------------------------

const (
	// banScoreIdleTTL bounds how long a sub-threshold ban-score entry
	// (Score > 0, not yet blacklisted) survives without a new
	// addBanScore hit. Aligned with banDuration: an IP that has been
	// quiet for a full ban window has earned a fresh score, exactly as
	// if it had served out a real ban.
	banScoreIdleTTL = banDuration

	// setupFailureIdleTTL bounds how long a setup-failure entry survives
	// after its last failure once no cooldown is active. Generous
	// multiple of setupFailureCooldown (30s): real setup-failure storms
	// re-hit the entry every few seconds, so anything quiet for this
	// long is either recovered (entry already cleared) or gone from the
	// mesh. Dropping the entry resets the Consecutive counter, which is
	// the pre-existing semantic of clearSetupFailuresLocked — an
	// isolated bad period must not stigmatise an address permanently.
	setupFailureIdleTTL = 10 * time.Minute
)

// purgeExpiredBansLocked drops s.bans entries whose state can no longer
// influence any decision: expired blacklists (isBlacklistedConn would
// lazily delete them anyway — but only if that IP ever reconnects) and
// sub-threshold score entries idle past banScoreIdleTTL.
//
// Caller MUST hold s.ipStateMu write lock.
func (s *Service) purgeExpiredBansLocked(now time.Time) {
	for ip, entry := range s.bans {
		if !entry.Blacklisted.IsZero() {
			if now.After(entry.Blacklisted) {
				delete(s.bans, ip)
			}
			continue
		}
		// Sub-threshold score-only entry. LastScored is zero only for
		// entries created before this field existed (or hand-built in
		// tests); treat those as stale — they cannot be refreshed
		// without addBanScore stamping LastScored.
		if entry.LastScored.IsZero() || now.Sub(entry.LastScored) > banScoreIdleTTL {
			delete(s.bans, ip)
		}
	}
}

// purgeExpiredBannedIPSetLocked drops expired local IP-wide bans. Uses
// the same liveness predicate as flushPeerState / BannedIPsFn
// (BannedUntil.After(now)), so an entry invisible to every reader is
// guaranteed to be removable. Recovery and operator overrides delete
// their entries directly and are unaffected.
//
// Caller MUST hold s.ipStateMu write lock.
func (s *Service) purgeExpiredBannedIPSetLocked(now time.Time) {
	for ip, entry := range s.bannedIPSet {
		if !entry.BannedUntil.After(now) {
			delete(s.bannedIPSet, ip)
		}
	}
}

// purgeExpiredRemoteIPBansLocked drops expired IP-wide remote bans. Uses
// the same liveness predicate as isRemoteIPBannedLocked
// (now.Before(entry.Until)), whose doc comment has always promised that
// "cleanup happens in flushPeerState / periodic sweep" — this is that
// periodic sweep. flushPeerState only filters what it WRITES; it never
// shrank the in-memory map.
//
// Caller MUST hold s.ipStateMu write lock.
func (s *Service) purgeExpiredRemoteIPBansLocked(now time.Time) {
	for ip, entry := range s.remoteBannedIPs {
		if !now.Before(entry.Until) {
			delete(s.remoteBannedIPs, ip)
		}
	}
}

// purgeStaleSetupFailuresLocked drops setup-failure entries that are not
// inside an active cooldown and have been idle past setupFailureIdleTTL.
// Entries inside an active cooldown are always kept — the sliding-window
// contract (each failure re-anchors CooldownUntil) must not be cut short.
//
// Caller MUST hold s.peerMu write lock.
func (s *Service) purgeStaleSetupFailuresLocked(now time.Time) {
	for addr, entry := range s.setupFailures {
		if entry == nil {
			delete(s.setupFailures, addr)
			continue
		}
		if !entry.CooldownUntil.IsZero() && now.Before(entry.CooldownUntil) {
			continue // active cooldown — keep
		}
		// Idle reference: prefer LastFailure; entries predating that
		// field fall back to CooldownUntil (zero for sub-threshold
		// entries, which then purge immediately — they cannot be
		// refreshed without recordSetupFailureLocked stamping
		// LastFailure).
		ref := entry.LastFailure
		if ref.IsZero() {
			ref = entry.CooldownUntil
		}
		if ref.IsZero() || now.Sub(ref) > setupFailureIdleTTL {
			delete(s.setupFailures, addr)
		}
	}
}

// purgeExpiredBanState is the ban-domain counterpart of
// purgeRouteQuarantineState: one periodic sweep that deletes every
// expired/stale entry across the four lazily-cleaned ban maps. Called
// from bootstrapLoop on the same ticker.
//
// Locks are taken sequentially (peerMu first, then ipStateMu), never
// nested, which trivially satisfies the canonical peerMu → ipStateMu
// order in docs/locking.md.
func (s *Service) purgeExpiredBanState() {
	now := time.Now().UTC()

	s.peerMu.Lock()
	s.purgeStaleSetupFailuresLocked(now)
	s.peerMu.Unlock()

	log.Trace().Str("site", "purgeExpiredBanState").Str("phase", "lock_wait").Msg("ip_state_mu_writer")
	s.ipStateMu.Lock()
	log.Trace().Str("site", "purgeExpiredBanState").Str("phase", "lock_held").Msg("ip_state_mu_writer")
	s.purgeExpiredBansLocked(now)
	s.purgeExpiredBannedIPSetLocked(now)
	s.purgeExpiredRemoteIPBansLocked(now)
	// Volatile pre-escalation offender buckets: already swept on the
	// at-cap path; sweeping here too keeps the steady-state size at the
	// true live working set instead of "whatever accumulated since the
	// last cap hit".
	s.sweepExpiredRemoteIPOffendersLocked(now)
	s.ipStateMu.Unlock()
	log.Trace().Str("site", "purgeExpiredBanState").Str("phase", "lock_released").Msg("ip_state_mu_writer")
}
