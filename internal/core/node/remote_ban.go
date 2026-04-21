package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Remote ban state — "they banned us" side of the peer-banned contract.
//
// When an outbound handshake receives connection_notice{code=peer-banned},
// the dialler records the expiration so the peer_provider dial gate can
// skip the affected candidates until the ban lifts. Separate from
// peerEntry.BannedUntil (which is "we banned them") because the two
// directions have different reconciliation semantics.
//
// Each notice lands in exactly one table — never both — driven by
// details.Reason. The read side (isPeerRemoteBannedLocked) consults
// both tables on every dial, so a record in either one is enough to
// suppress a candidate. Sender-side duplication would add no coverage
// and only create drift between the two tables.
//
//   reason=peer-ban     → per-peer record on peerEntry.RemoteBannedUntil
//                          (applies only to known peers; unknown
//                          addresses are ignored so a hostile peer
//                          cannot grow our state by inventing names).
//   reason=blacklisted  → IP-wide record on Service.remoteBannedIPs,
//                          keyed on the canonical host of peerAddress
//                          (applies to every sibling behind that
//                          egress — NAT gateway, VPN exit, Tor exit,
//                          multi-homed host — including peers
//                          discovered later via peer exchange).
//
// The expiration is trusted as-is from the remote: if a peer refuses us
// for ten years, we record ten years and leave it alone — there is no
// value in fighting a node that has decided it will not speak to us. The
// only normalisations are (a) a zero/missing remote `until` falls back to
// peerBanIncompatible so a minimal notice still produces a bounded record,
// and (b) past timestamps collapse to `now` so downstream `now.Before(until)`
// reads never see a negative window that would misreport the ban as lifted.
// ---------------------------------------------------------------------------

// remoteIPBanEntry is the in-memory record of a blacklisted-reason
// peer-banned notice, keyed on the server-side IP extracted from the
// notice's peerAddress. Separate from peerEntry.RemoteBannedUntil
// (per-peer scope) because a blacklisted notice applies to every
// sibling peer behind the same egress IP, which cannot be expressed on
// a single peerEntry. Reason is kept alongside Until for diagnostics
// only — gating uses Until.
type remoteIPBanEntry struct {
	Until  time.Time
	Reason string
}

// recordRemoteBanLocked stores the remote-reported ban window on the
// persisted metadata for address and returns the effective expiration
// actually recorded (possibly capped). The caller retains responsibility
// for scheduling any follow-up work (queueing a save, emitting metrics,
// cancelling outstanding dials) — this helper only mutates state.
//
// Must be called under s.mu write lock.
func (s *Service) recordRemoteBanLocked(
	address domain.PeerAddress,
	reason protocol.PeerBannedReason,
	remoteUntil time.Time,
	now time.Time,
) time.Time {
	if s.persistedMeta == nil {
		return time.Time{}
	}
	entry, ok := s.persistedMeta[address]
	if !ok {
		// We only honour bans for peers we already know about. Creating an
		// entry on the fly would open an unbounded growth path for a hostile
		// peer that keeps inventing addresses in its notice.
		log.Debug().
			Str("peer", string(address)).
			Msg("remote_ban_ignored_unknown_peer")
		return time.Time{}
	}

	effective := normalizeRemoteBanUntil(remoteUntil, now)
	copyOfEffective := effective
	entry.RemoteBannedUntil = &copyOfEffective
	entry.RemoteBanReason = string(reason)

	log.Info().
		Str("peer", string(address)).
		Str("reason", string(reason)).
		Time("remote_until", remoteUntil).
		Time("effective_until", effective).
		Msg("remote_ban_recorded")

	return effective
}

// clearRemoteBanLocked drops the remote ban for an address, e.g. after
// a successful handshake reconfirms the peer is accepting us again.
// Returns true when the in-memory state was actually mutated, so the
// caller knows whether the change needs to be flushed to disk — without
// this signal the clear would stay in memory until the next periodic
// save and could be lost on restart, leaving a stale RemoteBannedUntil
// to keep excluding a peer that is now willing to talk.
// Must be called under s.mu write lock.
func (s *Service) clearRemoteBanLocked(address domain.PeerAddress) bool {
	if s.persistedMeta == nil {
		return false
	}
	entry, ok := s.persistedMeta[address]
	if !ok {
		return false
	}
	if entry.RemoteBannedUntil == nil && entry.RemoteBanReason == "" {
		return false
	}
	entry.RemoteBannedUntil = nil
	entry.RemoteBanReason = ""
	return true
}

// isPeerRemoteBannedLocked reports whether the peer has an active
// remote ban at `now`. The check consults both the per-peer record
// (persistedMeta[address].RemoteBannedUntil — recorded on reason=peer-ban)
// and the IP-wide record (remoteBannedIPs — recorded on reason=blacklisted),
// so every sibling PeerAddress behind a blacklisted egress IP is
// suppressed even when its own peerEntry has no remote ban set. Returns
// false when neither applies at `now`, when the per-peer entry is
// unknown AND the IP is not blacklisted, or when both windows have
// elapsed.
// Must be called under s.mu read lock.
func (s *Service) isPeerRemoteBannedLocked(address domain.PeerAddress, now time.Time) bool {
	if s.isPeerAddressRemoteBannedLocked(address, now) {
		return true
	}
	if host, _, ok := splitHostPort(string(address)); ok {
		if s.isRemoteIPBannedLocked(host, now) {
			return true
		}
	}
	return false
}

// isPeerAddressRemoteBannedLocked is the per-peer half of
// isPeerRemoteBannedLocked. Kept as a separate helper so tests can
// exercise the per-peer path in isolation from IP-wide resolution.
// Must be called under s.mu read lock.
func (s *Service) isPeerAddressRemoteBannedLocked(address domain.PeerAddress, now time.Time) bool {
	entry, ok := s.persistedMeta[address]
	if !ok {
		return false
	}
	if entry.RemoteBannedUntil == nil {
		return false
	}
	return now.Before(*entry.RemoteBannedUntil)
}

// recordRemoteIPBanLocked stores the IP-wide remote ban communicated
// by a connection_notice{code=peer-banned, reason=blacklisted}. Unlike
// recordRemoteBanLocked, this helper does NOT require the peer to be
// known: the ban applies to the whole server IP and must survive past
// the lifetime of the single peerEntry that carried the notice — a
// fresh sibling discovered via peer exchange tomorrow must still be
// suppressed. Returns the effective expiration actually recorded
// (possibly normalised via normalizeRemoteBanUntil). Must be called
// under s.mu write lock.
func (s *Service) recordRemoteIPBanLocked(
	ip string,
	reason protocol.PeerBannedReason,
	remoteUntil time.Time,
	now time.Time,
) time.Time {
	if ip == "" {
		// No IP to key on — caller should have extracted host from
		// peerAddress before calling. Logged at caller site.
		return time.Time{}
	}
	if s.remoteBannedIPs == nil {
		s.remoteBannedIPs = make(map[string]remoteIPBanEntry)
	}

	effective := normalizeRemoteBanUntil(remoteUntil, now)
	s.remoteBannedIPs[ip] = remoteIPBanEntry{
		Until:  effective,
		Reason: string(reason),
	}

	log.Info().
		Str("ip", ip).
		Str("reason", string(reason)).
		Time("remote_until", remoteUntil).
		Time("effective_until", effective).
		Msg("remote_ip_ban_recorded")

	return effective
}

// isRemoteIPBannedLocked reports whether the server IP has an active
// IP-wide remote ban at `now`. Expired entries are treated as absent
// but are NOT removed here — cleanup happens in flushPeerState /
// periodic sweep so reads stay cheap and lock-free in the common case.
// Must be called under s.mu read lock.
func (s *Service) isRemoteIPBannedLocked(ip string, now time.Time) bool {
	if ip == "" {
		return false
	}
	entry, ok := s.remoteBannedIPs[ip]
	if !ok {
		return false
	}
	return now.Before(entry.Until)
}

// clearRemoteIPBanLocked drops the IP-wide remote ban for ip. Returns
// true when a record was actually removed so the caller can decide
// whether to flush to disk — mirrors the pattern in clearRemoteBanLocked.
// Must be called under s.mu write lock.
func (s *Service) clearRemoteIPBanLocked(ip string) bool {
	if ip == "" {
		return false
	}
	if _, ok := s.remoteBannedIPs[ip]; !ok {
		return false
	}
	delete(s.remoteBannedIPs, ip)
	return true
}

// normalizeRemoteBanUntil trusts a remote-supplied expiration as-is, with
// two narrow normalisations: a zero input (notice carried no `until`)
// falls back to peerBanIncompatible so the minimal notice still yields a
// bounded window, and a past input collapses to `now` so the persisted
// record never starts out already-elapsed. Any positive remote window —
// including decade-long bans — is preserved verbatim: a node that refuses
// to talk to us is authoritative about when it will change its mind, and
// fighting that decision on the dialler side only wastes connect attempts.
func normalizeRemoteBanUntil(remoteUntil, now time.Time) time.Time {
	if remoteUntil.IsZero() {
		return now.Add(peerBanIncompatible)
	}
	if remoteUntil.Before(now) {
		return now
	}
	return remoteUntil
}
