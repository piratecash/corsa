package node

import (
	"sort"
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
// Notice handling is driven by details.Reason (see the per-reason
// breakdown below): peer-ban writes a per-peer row; blacklisted writes a
// per-peer row for the offender AND feeds the remoteIPBanOffenders
// escalation set, promoting to an IP-wide row only after enough distinct
// live offenders accumulate. The read side (isPeerRemoteBannedLocked)
// consults both the per-peer and IP-wide tables on every dial, so a
// record in either one is enough to suppress a candidate. Sender-side
// duplication would add no coverage
// and only create drift between the two tables.
//
//   reason=peer-ban     → per-peer record on peerEntry.RemoteBannedUntil
//                          (applies only to known peers; unknown
//                          addresses are ignored so a hostile peer
//                          cannot grow our state by inventing names).
//   reason=blacklisted  → per-peer record on the offender, PLUS an
//                          escalation counter (noteRemoteIPOffenderLocked).
//                          Only once ipWideRemoteBanMinOffenders DISTINCT
//                          peers behind the same egress have sent
//                          blacklisted does an IP-wide record land on
//                          Service.remoteBannedIPs (keyed on the canonical
//                          host of peerAddress) and suppress every sibling
//                          behind that egress — NAT gateway, VPN exit, Tor
//                          exit, multi-homed host — including peers
//                          discovered later via peer exchange. A lone
//                          blacklisted notice stays per-peer-only.
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

const (
	// ipWideRemoteBanMinOffenders is the number of DISTINCT peer
	// addresses behind one egress IP that must independently deliver a
	// connection_notice{peer-banned, reason=blacklisted} before we
	// escalate from a per-peer remote ban to an IP-WIDE dial
	// suppression.
	//
	// Rationale (blast-radius control): a single blacklisted notice
	// proves only that ONE peer refuses us — but an IP-wide ban also
	// suppresses every innocent sibling sharing that egress (NAT
	// gateway, VPN exit, Tor exit, multi-homed host). Requiring several
	// distinct peers behind the IP to agree is the "the whole egress is
	// hostile" signal that justifies the wider blast radius; until then
	// only the offending peer itself is suppressed (per-peer record).
	ipWideRemoteBanMinOffenders = 3

	// ipWideRemoteBanMaxOffenders caps the per-IP offender set so a peer
	// rotating source addresses behind one egress cannot grow our state
	// without bound. Well above the escalation threshold.
	ipWideRemoteBanMaxOffenders = 16

	// ipWideRemoteBanMaxTrackedIPs bounds the total number of egress IPs
	// for which we keep a pre-escalation offender set. At the cap we
	// stop tracking NEW IPs (they degrade to per-peer-only suppression,
	// the safe direction) so the map cannot grow without bound over a
	// long uptime.
	ipWideRemoteBanMaxTrackedIPs = 4096
)

// noteRemoteIPOffenderLocked records that `offender` (a distinct peer
// address behind egress `ip`) delivered a blacklisted notice whose
// effective window ends at `until`, and returns the IP-WIDE ban horizon
// the caller should record — or the zero time when escalation is not
// (yet) warranted.
//
// Escalation requires at least ipWideRemoteBanMinOffenders offenders
// behind the IP whose windows have NOT elapsed at `now`. When that holds
// the returned horizon is the ipWideRemoteBanMinOffenders-th LARGEST live
// expiry: the IP-wide ban must lapse the moment fewer than the required
// number of distinct offenders remain. Taking the current offender's own
// `until` would let one long-lived notice (e.g. a decade) pin an IP-wide
// ban even after the other offenders that justified the wider blast
// radius have expired — re-introducing exactly the over-suppression the
// per-IP escalation was added to contain.
//
// Expired offenders are pruned BEFORE the per-IP cap is applied, so a
// bucket full of stale entries cannot reject a fresh, valid offender
// (which would delay a legitimate escalation under churn).
//
// Pure IP-domain mutation: caller MUST hold s.ipStateMu write lock.
func (s *Service) noteRemoteIPOffenderLocked(ip string, offender domain.PeerAddress, until, now time.Time) time.Time {
	if ip == "" {
		return time.Time{}
	}
	if s.remoteIPBanOffenders == nil {
		s.remoteIPBanOffenders = make(map[string]map[domain.PeerAddress]time.Time)
	}
	set, tracked := s.remoteIPBanOffenders[ip]
	if !tracked {
		// Bound the number of distinct IPs we track. At the cap, first
		// sweep buckets whose offenders have all elapsed — otherwise the
		// cap becomes sticky forever: stale buckets are normally pruned
		// only when their own IP is touched again, so on a long-running
		// node 4096 long-expired buckets would permanently reject every
		// new IP and disable IP-wide escalation. After the sweep, if we
		// are still at the cap the new IP is left untracked (per-peer-only
		// suppression — the safe default).
		if len(s.remoteIPBanOffenders) >= ipWideRemoteBanMaxTrackedIPs {
			s.sweepExpiredRemoteIPOffendersLocked(now)
			if len(s.remoteIPBanOffenders) >= ipWideRemoteBanMaxTrackedIPs {
				return time.Time{}
			}
		}
		set = make(map[domain.PeerAddress]time.Time)
		s.remoteIPBanOffenders[ip] = set
	}

	// Prune elapsed offenders FIRST, so the cap below reflects only live
	// entries and a stale-full bucket cannot reject a fresh offender.
	for addr, exp := range set {
		if !now.Before(exp) {
			delete(set, addr)
		}
	}

	// Record/refresh this offender's expiry (normalised the same way as
	// the ban records so the windows line up). The cap applies to the
	// live set; refreshing an already-tracked offender is always allowed.
	expiry := normalizeRemoteBanUntil(until, now)
	if offender != "" {
		if _, exists := set[offender]; exists || len(set) < ipWideRemoteBanMaxOffenders {
			set[offender] = expiry
		}
	}

	if len(set) == 0 {
		delete(s.remoteIPBanOffenders, ip)
		return time.Time{}
	}
	if len(set) < ipWideRemoteBanMinOffenders {
		return time.Time{}
	}

	// Horizon = the ipWideRemoteBanMinOffenders-th largest live expiry,
	// i.e. the instant the live-offender count drops below the threshold.
	// The set is bounded by ipWideRemoteBanMaxOffenders so the sort is
	// cheap.
	exps := make([]time.Time, 0, len(set))
	for _, exp := range set {
		exps = append(exps, exp)
	}
	sort.Slice(exps, func(i, j int) bool { return exps[i].After(exps[j]) })
	return exps[ipWideRemoteBanMinOffenders-1]
}

// sweepExpiredRemoteIPOffendersLocked prunes elapsed offenders from
// every tracked IP bucket and deletes buckets left empty. Used to keep
// the ipWideRemoteBanMaxTrackedIPs cap from becoming permanently sticky:
// without it, buckets are pruned only when their own IP is touched
// again, so a long-running node could fill the cap with long-expired
// buckets and reject all new IPs forever. Cost is O(total tracked
// offenders); called only on the rare at-cap path.
//
// Pure IP-domain mutation: caller MUST hold s.ipStateMu write lock.
func (s *Service) sweepExpiredRemoteIPOffendersLocked(now time.Time) {
	for ip, set := range s.remoteIPBanOffenders {
		for addr, exp := range set {
			if !now.Before(exp) {
				delete(set, addr)
			}
		}
		if len(set) == 0 {
			delete(s.remoteIPBanOffenders, ip)
		}
	}
}

// recordRemoteBanLocked stores the remote-reported ban window on the
// persisted metadata for address and returns the effective expiration
// actually recorded (possibly capped). The caller retains responsibility
// for scheduling any follow-up work (queueing a save, emitting metrics,
// cancelling outstanding dials) — this helper only mutates state.
//
// Mutates persistedMeta, which is peer-domain state — caller MUST hold
// s.peerMu write lock.  This helper does NOT touch remoteBannedIPs
// (ipStateMu-owned); it writes only to peerEntry.RemoteBannedUntil on
// persistedMeta, so no ipStateMu acquisition is required here.
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
// Mutates persistedMeta (peer-domain) only — caller MUST hold s.peerMu
// write lock.
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
//
// Cross-domain read: persistedMeta is peer-domain (s.peerMu),
// remoteBannedIPs is IP/advertise-domain (s.ipStateMu).  Caller MUST
// hold s.peerMu (read or write) AND s.ipStateMu (read or write),
// acquired in canonical peerMu → ipStateMu order — see docs/locking.md.
// This helper sits on the gate exercised by PeerProvider.Candidates(),
// so any caller that reverses the order reintroduces a reverse-order
// deadlock risk on the dial path.
func (s *Service) isPeerRemoteBannedLocked(address domain.PeerAddress, now time.Time) bool {
	if s.isPeerAddressRemoteBannedLocked(address, now) {
		return true
	}
	// Derive the IP-wide lookup key the SAME way record (handlePeerBannedNotice)
	// and clear (recordOutboundAuthSuccess) do — via remoteIPBanKey, which
	// canonicalizes IP literals (e.g. ::ffff:203.0.113.5 → 203.0.113.5).
	// Using the raw splitHostPort host here would miss a ban stored under
	// the canonical key for a candidate whose address carries a non-canonical
	// IP form.
	if key, ok := remoteIPBanKey(address); ok {
		if s.isRemoteIPBannedLocked(key, now) {
			return true
		}
	}
	return false
}

// isPeerAddressRemoteBannedLocked is the per-peer half of
// isPeerRemoteBannedLocked. Kept as a separate helper so tests can
// exercise the per-peer path in isolation from IP-wide resolution.
// Reads persistedMeta (peer-domain) only — caller MUST hold s.peerMu
// (read or write).
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
// (possibly normalised via normalizeRemoteBanUntil).
//
// Pure IP-domain mutation: caller MUST hold ipStateMu write lock. The
// *Locked suffix is retained for call-site symmetry with the other
// remote-ban helpers; it now refers to ipStateMu, not s.peerMu.
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
//
// Pure IP-domain read: caller MUST hold ipStateMu (read or write).
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
// true ONLY when a PERSISTED record (remoteBannedIPs) was removed, so
// the caller knows a peers.json flush is actually warranted.
//
// The volatile pre-escalation offender set (remoteIPBanOffenders) is
// always dropped for ip too — a recovered IP should start fresh — but
// because that state is in-memory-only (never persisted), clearing it
// alone does NOT return true: signalling a persisted mutation there
// would trigger a wasteful peers.json flush on every recovery of a peer
// that had only pre-escalation tracking and no active IP-wide ban.
//
// Pure IP-domain mutation: caller MUST hold ipStateMu write lock.
func (s *Service) clearRemoteIPBanLocked(ip string) bool {
	if ip == "" {
		return false
	}
	_, hadPersisted := s.remoteBannedIPs[ip]
	delete(s.remoteBannedIPs, ip)
	delete(s.remoteIPBanOffenders, ip) // volatile; not a persisted mutation
	return hadPersisted
}

// remoteIPBanKey derives the remoteBannedIPs / remoteIPBanOffenders map
// key from a peer address, matching how handlePeerBannedNotice keys an
// inbound peer-banned notice: an IP-literal host collapses to its
// canonical form; a hostname (DNS / manual peer) is kept verbatim. The
// bool is false when the address has no host:port split.
//
// Recovery (recordOutboundAuthSuccess) MUST derive the clear key with
// THIS function, not from the resolved TCP RemoteAddr() IP: a hostname-
// keyed ban (e.g. "peer.example") would otherwise never be cleared by a
// handshake whose RemoteAddr resolved to a numeric IP (203.0.113.x),
// leaving the dial gate suppressing the peer until the window expires.
func remoteIPBanKey(peerAddress domain.PeerAddress) (string, bool) {
	host, _, ok := splitHostPort(string(peerAddress))
	if !ok {
		return "", false
	}
	if canon := canonicalIPFromHost(host); canon != "" {
		return canon, true
	}
	return host, true
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
