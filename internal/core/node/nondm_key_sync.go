package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// nondm_key_sync.go budgets the key sync that a non-DM message of an unknown
// author triggers (refuseUnattributedNonDM).
//
// The keyless-DM recovery (triggerSenderKeySyncAsync) is bounded by
// CONCURRENCY only, which is enough there: a DM names an author whose
// envelope must verify, and an honest DM sender keeps retrying until it does.
// A non-DM author is just a 40-hex string the pushing peer chose, every
// distinct one used to buy a pass of 1+senderKeySyncFanout fresh dials to
// honest neighbours, and one neighbour pushing distinct names could keep this
// node dialling without pause — tripping the neighbours' own per-IP accept
// limits. So this trigger draws from its own pool, never from the DM slots,
// and is bounded in RATE as well:
//
//   - one pass at a time (nonDMKeySyncMaxConcurrent): the refused message is
//     not redelivered either way (no receipts, no retry for non-DM), so a
//     pass only helps the author's NEXT messages — there is nothing urgent
//     to parallelise;
//   - one pass start per nonDMKeySyncMinInterval node-wide: at most
//     6 passes, i.e. ≤ 6·(1+senderKeySyncFanout) = 24 fetch_contacts a
//     minute, whatever the traffic — ≤ 4 dials per 10 s even if every one of
//     them lands on the same neighbour, below the 10-per-10-s per-IP accept
//     limit a neighbour enforces;
//   - one pass start per hop per nonDMKeySyncHopCooldown, so a single noisy
//     neighbour can take at most a third of that budget;
//   - a hop whose non-DM traffic is mostly unattributable (at least
//     nonDMAttributionMinSample unknown authors in a window, and more unknown
//     than known) buys no pass for nonDMHopSuppression. An honest relay
//     forwards mostly authors this node already knows.
//
// Every refusal here is silent and costs the peer nothing — the budget limits
// this node's own work, it is not a verdict on the neighbour.
const (
	nonDMKeySyncMaxConcurrent  = 1
	nonDMKeySyncMinInterval    = 10 * time.Second
	nonDMKeySyncHopCooldown    = 30 * time.Second
	nonDMAttributionWindow     = time.Minute
	nonDMAttributionMinSample  = 20
	nonDMHopSuppression        = 10 * time.Minute
	maxNonDMKeySyncTrackedHops = 1024
)

// nonDMKeySyncVerdict is the limiter's answer for one trigger.
type nonDMKeySyncVerdict int

const (
	nonDMKeySyncAdmitted nonDMKeySyncVerdict = iota
	nonDMKeySyncBusy
	nonDMKeySyncGlobalPacing
	nonDMKeySyncHopCoolingDown
	nonDMKeySyncHopSuppressed
	nonDMKeySyncHopUntracked
)

var nonDMKeySyncVerdictNames = map[nonDMKeySyncVerdict]string{
	nonDMKeySyncAdmitted:       "admitted",
	nonDMKeySyncBusy:           "busy",
	nonDMKeySyncGlobalPacing:   "global_pacing",
	nonDMKeySyncHopCoolingDown: "hop_cooldown",
	nonDMKeySyncHopSuppressed:  "hop_suppressed",
	nonDMKeySyncHopUntracked:   "hop_untracked",
}

func (v nonDMKeySyncVerdict) String() string { return nonDMKeySyncVerdictNames[v] }

// nonDMHopAttribution is one hop's non-DM traffic in the current window.
type nonDMHopAttribution struct {
	windowStart     time.Time
	attributed      int
	unattributed    int
	suppressedUntil time.Time
}

// nonDMKeySyncLimiter is the admission state of non-DM key-sync passes. It is
// owned by Service.senderKeySyncMu — the same tiny domain as the DM recovery
// maps, never held across I/O — and every method with the Locked suffix
// requires that mutex.
type nonDMKeySyncLimiter struct {
	clock      func() time.Time
	inFlight   map[string]struct{}
	hopLastRun map[string]time.Time
	lastStart  time.Time
	hops       map[string]*nonDMHopAttribution
	// nextPruneAt is the earliest moment a full scan can free anything:
	// before it, a full map stays full and is not rescanned.
	nextPruneAt time.Time
}

func newNonDMKeySyncLimiter(clock func() time.Time) *nonDMKeySyncLimiter {
	return &nonDMKeySyncLimiter{
		clock:      clock,
		inFlight:   make(map[string]struct{}),
		hopLastRun: make(map[string]time.Time),
		hops:       make(map[string]*nonDMHopAttribution),
	}
}

// noteLocked records one non-DM arrival from hop and suppresses the hop once
// its window is mostly unattributable. Caller holds senderKeySyncMu.
func (l *nonDMKeySyncLimiter) noteLocked(hop string, attributed bool) {
	now := l.clock()
	entry := l.hops[hop]
	if entry == nil {
		if !l.roomForHopLocked(now) {
			// Not tracked, so never admitted (hop_untracked): the
			// attribution that could suppress this hop has nowhere to live,
			// and making room would mean forgetting a running suppression.
			return
		}
		entry = &nonDMHopAttribution{windowStart: now}
		l.hops[hop] = entry
	}
	if now.Sub(entry.windowStart) >= nonDMAttributionWindow {
		entry.windowStart = now
		entry.attributed = 0
		entry.unattributed = 0
	}
	if attributed {
		entry.attributed++
		return
	}
	entry.unattributed++
	if entry.unattributed >= nonDMAttributionMinSample && entry.unattributed > entry.attributed {
		entry.suppressedUntil = now.Add(nonDMHopSuppression)
	}
}

// admitLocked decides whether a pass for sender, triggered through hop, may
// start now, and reserves it when it may. Caller holds senderKeySyncMu.
func (l *nonDMKeySyncLimiter) admitLocked(hop, sender string) nonDMKeySyncVerdict {
	now := l.clock()
	verdict := l.verdictLocked(hop, now)
	if verdict != nonDMKeySyncAdmitted {
		return verdict
	}
	l.inFlight[sender] = struct{}{}
	l.lastStart = now
	l.hopLastRun[hop] = now
	return nonDMKeySyncAdmitted
}

func (l *nonDMKeySyncLimiter) verdictLocked(hop string, now time.Time) nonDMKeySyncVerdict {
	switch {
	case len(l.inFlight) >= nonDMKeySyncMaxConcurrent:
		return nonDMKeySyncBusy
	case !l.lastStart.IsZero() && now.Sub(l.lastStart) < nonDMKeySyncMinInterval:
		return nonDMKeySyncGlobalPacing
	case l.hops[hop] == nil:
		return nonDMKeySyncHopUntracked
	case l.hopSuppressedLocked(hop, now):
		return nonDMKeySyncHopSuppressed
	case l.hopCoolingLocked(hop, now):
		return nonDMKeySyncHopCoolingDown
	default:
		return nonDMKeySyncAdmitted
	}
}

func (l *nonDMKeySyncLimiter) hopSuppressedLocked(hop string, now time.Time) bool {
	entry := l.hops[hop]
	return entry != nil && now.Before(entry.suppressedUntil)
}

func (l *nonDMKeySyncLimiter) hopCoolingLocked(hop string, now time.Time) bool {
	last, ok := l.hopLastRun[hop]
	return ok && now.Sub(last) < nonDMKeySyncHopCooldown
}

// releaseLocked frees the pass slot of sender. Caller holds senderKeySyncMu.
func (l *nonDMKeySyncLimiter) releaseLocked(sender string) {
	delete(l.inFlight, sender)
}

// roomForHopLocked reports whether one more hop can be tracked. Hops are
// neighbours, but the address fallback of the key is whatever the transport
// reports, so the maps must not grow with connection churn: at the cap a new
// hop is refused, never admitted by evicting a live entry. A full scan runs
// only once something can have lapsed (nextPruneAt), so a stream of new hops
// against a full map does not rescan it under senderKeySyncMu — the mutex the
// keyless-DM recovery shares. Caller holds senderKeySyncMu.
func (l *nonDMKeySyncLimiter) roomForHopLocked(now time.Time) bool {
	if len(l.hops) < maxNonDMKeySyncTrackedHops && len(l.hopLastRun) < maxNonDMKeySyncTrackedHops {
		return true
	}
	if now.Before(l.nextPruneAt) {
		return false
	}
	l.pruneHopsLocked(now)
	return len(l.hops) < maxNonDMKeySyncTrackedHops && len(l.hopLastRun) < maxNonDMKeySyncTrackedHops
}

// pruneHopsLocked drops entries whose window, cooldown and suppression have
// all lapsed — they carry no information — and records when the earliest of
// the survivors will lapse. Caller holds senderKeySyncMu.
func (l *nonDMKeySyncLimiter) pruneHopsLocked(now time.Time) {
	var earliest time.Time
	noteLapse := func(at time.Time) {
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
	}
	for hop, entry := range l.hops {
		lapse := entry.windowStart.Add(nonDMAttributionWindow)
		if entry.suppressedUntil.After(lapse) {
			lapse = entry.suppressedUntil
		}
		if !now.Before(lapse) {
			delete(l.hops, hop)
			continue
		}
		noteLapse(lapse)
	}
	for hop, last := range l.hopLastRun {
		lapse := last.Add(nonDMKeySyncHopCooldown)
		if !now.Before(lapse) {
			delete(l.hopLastRun, hop)
			continue
		}
		noteLapse(lapse)
	}
	l.nextPruneAt = earliest
}

// nonDMHopKey names the neighbour a non-DM push came through by its
// penaltySubject: the identity when a v2 session proved it, so one identity
// holding several connections is one hop; otherwise what this node observed
// of the connection — the address it dialled, the source IP, or the
// connection itself. Never the identity a legacy session merely names: the
// suppression below is a verdict this node keeps for ten minutes, and keyed
// by a welcome's claim it was a verdict any legacy peer could hand to
// somebody else.
func nonDMHopKey(subject penaltySubject) string {
	if subject.IsZero() {
		return ""
	}
	return subject.String()
}

// nonDMKeySyncLocked returns the limiter, creating it for struct-literal test
// Services that bypass NewService. Caller holds senderKeySyncMu.
func (s *Service) nonDMKeySyncLocked() *nonDMKeySyncLimiter {
	if s.nonDMKeySync == nil {
		s.nonDMKeySync = newNonDMKeySyncLimiter(time.Now)
	}
	return s.nonDMKeySync
}

// noteNonDMAttribution records whether a non-DM push from hop named an author
// this node knows.
func (s *Service) noteNonDMAttribution(hop string, attributed bool) {
	s.senderKeySyncMu.Lock()
	s.nonDMKeySyncLocked().noteLocked(hop, attributed)
	s.senderKeySyncMu.Unlock()
}

// triggerNonDMSenderKeySync starts a key-sync pass for the author of a refused
// non-DM message when the non-DM budget allows it. The pass itself is the
// same one the DM recovery runs; only admission differs.
func (s *Service) triggerNonDMSenderKeySync(prevHop domain.PeerAddress, sender string, ownedSession *peerSession, hop string) {
	if !identity.IsValidAddress(sender) {
		// No address-keyed fallback here, unlike the DM recovery: a
		// non-DM pass exists to learn ONE author, and a name that is not
		// an address can never be learned.
		return
	}
	s.senderKeySyncMu.Lock()
	_, dmPassRunning := s.senderKeySyncInFlight[sender]
	verdict := nonDMKeySyncBusy
	if !dmPassRunning {
		verdict = s.nonDMKeySyncLocked().admitLocked(hop, sender)
	}
	s.senderKeySyncMu.Unlock()

	if verdict != nonDMKeySyncAdmitted {
		s.nonDMKeySyncSkipped.Add(1)
		log.Trace().Str("sender", sender).Str("hop_key", hop).Str("verdict", verdict.String()).Msg("non_dm_key_sync_skipped")
		return
	}
	s.nonDMKeySyncPasses.Add(1)
	s.runSenderKeySyncPass(prevHop, sender, ownedSession, func() {
		s.senderKeySyncMu.Lock()
		s.nonDMKeySyncLocked().releaseLocked(sender)
		s.senderKeySyncMu.Unlock()
	})
}
