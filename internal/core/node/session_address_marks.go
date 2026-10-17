package node

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// session_address_marks.go is what the dialler remembers, per address and in
// memory only, about the session kind it may use there
// (docs/protocol/session_v2.md, "Choosing the session kind on dial"). Two
// kinds of entries live here:
//
//   - hints — v2_hint, legacy_observed, v2_strong. The first two are
//     UNPROVEN: a man in the middle can forge the answer behind them, so they
//     only change what may follow a failed TLS attempt, never whether v1 is
//     ruled out.
//     v2_strong is set by a proven outbound v2 session alone and, while it
//     lasts, makes the address require v2.
//   - backoffs — the hold of the fall back to v1 after a refused v1 answer,
//     the v2 retry backoff, and the wait for room in a full pin store. They
//     pace the next attempt and charge nothing to the address.

const (
	// legacyObservedTerm is how long a correct old answer to the probe lets a
	// first TLS alert at the address be followed by the probe at once. The
	// TLS attempt still comes first, and the probe decides afresh: the mark
	// never opens v1 by itself.
	legacyObservedTerm = 24 * time.Hour
	// v2HintTerm keeps the next dial on v2 first. It never forbids v1.
	v2HintTerm = time.Hour
	// v2StrongTerm is how long a proven outbound v2 session makes its
	// address require v2.
	v2StrongTerm = 7 * 24 * time.Hour
	// legacyRefusalHoldBase and legacyRefusalHoldMax bound the hold of the
	// fall back to v1 at an address whose v1 answer was refused because the
	// peer requires v2: 5 minutes after the first refusal, doubled after each
	// further one, at most 6 hours. A man in the middle can provoke such a
	// refusal at will, so it costs the address nothing but this hold — no
	// failure, no score, no cooldown — and the hold never delays a v2
	// attempt.
	legacyRefusalHoldBase = 5 * time.Minute
	legacyRefusalHoldMax  = 6 * time.Hour
	// v2RetryBase and v2RetryMax bound the v2 retry backoff: after a v2
	// attempt that ended without v2 for a reason that says nothing against
	// the address's standing, the next dial to the address waits 30 s,
	// doubled per further such end, at most 30 min; an end met while the
	// wait runs takes no step. The connection manager's
	// own retry backoff still applies; the later of the two wins, because a
	// dial inside this backoff is refused before any socket. It is the
	// pacing of the next v2 attempt, not a penalty: score, failures and setup
	// failures are untouched, and only a proven outbound v2 session to the
	// same address resets it.
	v2RetryBase = 30 * time.Second
	v2RetryMax  = 30 * time.Minute
	// oldVersionRetryBase and oldVersionRetryMax bound the delay of an
	// endpoint whose correct answer — the probe's, a v1 welcome — claimed a
	// version below this node's minimum: 30 s, doubled
	// per further such answer, at most 6 h. It is that endpoint's alone and
	// charges nothing: the claim proves nothing, and a forged one may hold the
	// endpoint off for up to 6 h — the accepted price.
	oldVersionRetryBase = 30 * time.Second
	oldVersionRetryMax  = 6 * time.Hour
	// pinRoomWaitMax bounds the wait after a v2 session to the address was
	// refused because this node's pin store is full: the next dial goes as
	// soon as the store has room, and at the latest after this, never
	// doubled — the store's bound is this node's limit, not the peer's fault.
	pinRoomWaitMax = 30 * time.Minute
	// maxSessionAddressMarks bounds the marks: addresses come from peer
	// exchange, so the map must not grow with what others tell us.
	maxSessionAddressMarks = 16384
)

// errLegacyRefusedBackoff is a fall back to v1 withheld because an earlier v1
// answer from the same address was refused (errLegacyRefusedPinned,
// errLegacyRefusedV2Bound) and the hold has not run out yet.
var errLegacyRefusedBackoff = errors.New("secure session: v1 fall back held after a refused v1 answer from this address")

// errOldVersionAwaited is a dial refused before any socket because the
// endpoint's old-version delay runs.
var errOldVersionAwaited = errors.New("secure session: dial held after the endpoint claimed a version below this node's minimum")

// errV2RetryHeld is a dial refused before any socket because the address's v2
// retry backoff runs.
var errV2RetryHeld = errors.New("secure session: dial held by the v2 retry backoff of this address")

// sessionAddressMark is what the dialler remembers about one address.
type sessionAddressMark struct {
	// legacyObserved: a probe on the address got a correct old-protocol
	// answer; nil when none is on record.
	legacyObserved *expiringMark
	// v2Hint: the address answered like a node that speaks TLS; nil when no
	// such answer is on record.
	v2Hint *v2HintMark
	// v2Strong: an outbound v2 session to the address was proven; nil when
	// none is on record.
	v2Strong *expiringMark
	// legacyHold is the backoff after a refused v1 answer; nil when no such
	// refusal is on record since the last outbound v2 session on the address.
	legacyHold *legacyRefusalHold
	// v2Retry is the backoff of the next v2 attempt; nil when no attempt
	// ended without v2 since the last outbound v2 session on the address.
	v2Retry *v2RetryBackoff
	// pinRoom is the wait for room in the pin store; nil when no v2 session
	// to the address was refused for it.
	pinRoom *expiringMark
	// oldVersion is the delay after a correct answer claimed a version below
	// this node's minimum; nil when none is on record
	// since the last outbound success at the address. Only that success
	// forgets it; a retry by hand ends the wait and keeps the count.
	oldVersion *oldVersionBackoff
	// touched is the last time the mark was written: when the map is full,
	// the least recently written mark goes first.
	touched time.Time
}

// expiringMark is a fact about an address that holds until a moment.
type expiringMark struct {
	until time.Time
}

func newExpiringMark(now time.Time, term time.Duration) *expiringMark {
	return &expiringMark{until: now.Add(term)}
}

func (m *expiringMark) liveAt(now time.Time) bool {
	return m != nil && now.Before(m.until)
}

// v2HintMark is a v2 hint and whether a TLS alert gave it: an alert that
// meets an alert's hint lets the probe run.
type v2HintMark struct {
	expiringMark
	alerted bool
}

func (h *v2HintMark) liveAt(now time.Time) bool {
	return h != nil && h.expiringMark.liveAt(now)
}

// legacyRefusalHold is the backoff of the fall back to v1 at one address.
type legacyRefusalHold struct {
	until time.Time
	// refusals since the last outbound v2 session on the address; the next
	// hold is legacyRefusalHoldBase doubled refusals-1 times.
	refusals int
}

// v2RetryBackoff is the backoff of the next v2 attempt to one address.
type v2RetryBackoff struct {
	until time.Time
	// unanswered v2 attempts since the last outbound v2 session on the
	// address; the next wait is v2RetryBase doubled unanswered-1 times.
	unanswered int
}

// oldVersionBackoff is the old-version delay of one endpoint.
type oldVersionBackoff struct {
	until time.Time
	// steps since the last outbound success at the endpoint; the next wait
	// is oldVersionRetryBase doubled steps-1 times.
	steps int
	// claimed is the version the latest answer announced.
	claimed domain.ProtocolVersion
}

// counted reports whether the mark carries a count that only an outbound
// success may forget: its next backoff doubles on it.
func (m sessionAddressMark) counted() bool {
	return m.legacyHold != nil || m.v2Retry != nil || m.oldVersion != nil
}

// protects reports whether the mark makes the address require v2 at now.
func (m sessionAddressMark) protects(now time.Time) bool {
	return m.v2Strong.liveAt(now)
}

// forgettableAt reports whether the mark says nothing any more at now. A
// hold whose time ran out still carries its count — the next one doubles on
// it — so only an outbound v2 session forgets it.
func (m sessionAddressMark) forgettableAt(now time.Time) bool {
	return !m.legacyObserved.liveAt(now) &&
		!m.v2Hint.liveAt(now) &&
		!m.v2Strong.liveAt(now) &&
		!m.pinRoom.liveAt(now) &&
		!m.counted()
}

// doubledBackoff is base doubled count-1 times, at most ceiling.
func doubledBackoff(base, ceiling time.Duration, count int) time.Duration {
	wait := base
	for i := 1; i < count && wait < ceiling; i++ {
		wait *= 2
	}
	return min(wait, ceiling)
}

// legacyRefusalHoldFor is the hold after the given number of consecutive
// refusals.
func legacyRefusalHoldFor(refusals int) time.Duration {
	return doubledBackoff(legacyRefusalHoldBase, legacyRefusalHoldMax, refusals)
}

// stepV2Retry moves the v2 retry backoff on by one step and reports whether
// it did. An end met while the current wait still runs takes no step: it
// belongs to an attempt that started before the wait — a parallel dial to the
// same address, or a second failure inside one attempt — and one attempt
// counts once.
func stepV2Retry(previous *v2RetryBackoff, now time.Time) (*v2RetryBackoff, bool) {
	if previous != nil && now.Before(previous.until) {
		return previous, false
	}
	unanswered := 1
	if previous != nil {
		unanswered = previous.unanswered + 1
	}
	return &v2RetryBackoff{until: now.Add(doubledBackoff(v2RetryBase, v2RetryMax, unanswered)), unanswered: unanswered}, true
}

// randomWait is a uniformly random wait in [0, upTo), or zero for a bound
// that leaves no room.
func randomWait(upTo time.Duration) time.Duration {
	if upTo <= 0 {
		return 0
	}
	return rand.N(upTo)
}

// sessionAddressMarks remember, per address, what the dialler learnt about
// the session kind it may use there. Its own mutex is a leaf: nothing is
// called under it.
type sessionAddressMarks struct {
	clock func() time.Time
	// jitter returns a random wait in [0, upTo); set once at construction.
	jitter func(upTo time.Duration) time.Duration

	mu    sync.Mutex
	marks map[domain.PeerAddress]sessionAddressMark
	// dials are the dials to an address running now (openDial). They are
	// kept apart from the marks: a running dial is not a fact the address
	// earned, and must neither be evicted with a mark nor keep one alive.
	dials map[domain.PeerAddress]*openDials
}

// openDials is the dials to one address running now and whether one of them
// already moved the address's v2 retry backoff on.
type openDials struct {
	running int
	stepped bool
}

func newSessionAddressMarks(clock func() time.Time) *sessionAddressMarks {
	return &sessionAddressMarks{clock: clock, jitter: randomWait, marks: make(map[domain.PeerAddress]sessionAddressMark)}
}

// update applies change to the mark of address under the mutex and stores
// the result.
func (m *sessionAddressMarks) update(address domain.PeerAddress, change func(mark *sessionAddressMark, now time.Time)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	mark := m.markForUpdateLocked(address, now)
	change(&mark, now)
	m.storeLocked(address, mark, now)
}

// read returns what ask reports about the mark of address now.
func (m *sessionAddressMarks) read(address domain.PeerAddress, ask func(mark sessionAddressMark, now time.Time) bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ask(m.marks[address], m.clock())
}

// noteLegacyObserved records a correct old-protocol answer to the probe. It
// forgets a v2 hint: the later answer wins, and both are unproven.
func (m *sessionAddressMarks) noteLegacyObserved(address domain.PeerAddress) {
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		mark.legacyObserved = newExpiringMark(now, legacyObservedTerm)
		mark.v2Hint = nil
	})
}

func (m *sessionAddressMarks) legacyObserved(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool { return mark.legacyObserved.liveAt(now) })
}

// forgetLegacyObserved drops legacy_observed of address, so its next dial
// starts with v2.
func (m *sessionAddressMarks) forgetLegacyObserved(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || mark.legacyObserved == nil {
		return
	}
	mark.legacyObserved = nil
	m.storeLocked(address, mark, m.clock())
}

// noteV2Hint records an answer that came from a node speaking TLS — a
// handshake record, or a probe answered by a v2 welcome — and forgets
// legacy_observed. Whether an alert gave the hint is kept while it lasts.
func (m *sessionAddressMarks) noteV2Hint(address domain.PeerAddress) {
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		alerted := mark.v2Hint.liveAt(now) && mark.v2Hint.alerted
		mark.v2Hint = &v2HintMark{expiringMark: *newExpiringMark(now, v2HintTerm), alerted: alerted}
		mark.legacyObserved = nil
	})
}

// noteV2Alert records a TLS alert as a v2 hint and reports whether it met a
// live hint an alert gave: that repeated alert lets the probe run.
func (m *sessionAddressMarks) noteV2Alert(address domain.PeerAddress) (repeated bool) {
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		repeated = mark.v2Hint.liveAt(now) && mark.v2Hint.alerted
		mark.v2Hint = &v2HintMark{expiringMark: *newExpiringMark(now, v2HintTerm), alerted: true}
		mark.legacyObserved = nil
	})
	return repeated
}

func (m *sessionAddressMarks) v2Hinted(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool { return mark.v2Hint.liveAt(now) })
}

// noteOutboundV2 records an outbound v2 session to address whose proof
// verified: the address requires v2 for v2StrongTerm, the backoffs and the
// wait for pin room start over, and the unproven hints are forgotten.
func (m *sessionAddressMarks) noteOutboundV2(address domain.PeerAddress) {
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		*mark = sessionAddressMark{v2Strong: newExpiringMark(now, v2StrongTerm)}
	})
}

func (m *sessionAddressMarks) v2Strong(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool { return mark.v2Strong.liveAt(now) })
}

// notePinStoreFull records a v2 session to address refused because the pin
// store is full and returns the longest wait it imposes.
func (m *sessionAddressMarks) notePinStoreFull(address domain.PeerAddress) time.Duration {
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		mark.pinRoom = newExpiringMark(now, pinRoomWaitMax)
	})
	return pinRoomWaitMax
}

// awaitsPinRoom reports whether a v2 session to address was refused for a
// full pin store less than pinRoomWaitMax ago.
func (m *sessionAddressMarks) awaitsPinRoom(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool { return mark.pinRoom.liveAt(now) })
}

// noteLegacyRefused starts the next, longer hold of the fall back to v1 at
// address and returns its length. It also forgets legacy_observed: the v1
// answer behind it may have been forged by whoever forged the refused one,
// and the next dial must try v2 first. It also moves the v2 retry backoff on
// (see below for why).
func (m *sessionAddressMarks) noteLegacyRefused(address domain.PeerAddress) time.Duration {
	var hold time.Duration
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		refusals := 1
		if mark.legacyHold != nil {
			refusals = mark.legacyHold.refusals + 1
		}
		hold = legacyRefusalHoldFor(refusals)
		mark.legacyObserved = nil
		mark.legacyHold = &legacyRefusalHold{until: now.Add(hold), refusals: refusals}
		// The hold above only stops the fall back to v1; it does not delay
		// v2. The refusal is not charged to the address
		// (isUnchargedDialRefusal: whoever forged the v1 answer can repeat it
		// at will), so nothing else slows the connection manager down, and
		// without pacing the refused address would be redialled every few
		// seconds for good. The v2 retry backoff paces it instead; a dial it
		// holds is not charged either.
		m.stepV2RetryLocked(address, mark, now)
	})
	return hold
}

// noteListenerRefused moves the v2 retry backoff of address on after the
// listener there refused a v2 attempt with its alert, with a jitter of up to
// half the wait on top, and returns the wait it now imposes. A refusal met
// while the wait runs takes no step (stepV2Retry) and adds no jitter.
func (m *sessionAddressMarks) noteListenerRefused(address domain.PeerAddress) time.Duration {
	var wait time.Duration
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		if m.stepV2RetryLocked(address, mark, now) {
			mark.v2Retry.until = mark.v2Retry.until.Add(m.jitter(mark.v2Retry.until.Sub(now) / 2))
		}
		wait = mark.v2Retry.until.Sub(now)
	})
	return wait
}

// noteV2Unanswered moves the v2 retry backoff of address on after a v2
// attempt there ended without v2, and returns the wait it now imposes. An end
// met while the wait runs, or in a dial that already took its step, takes no
// step (stepV2RetryLocked).
func (m *sessionAddressMarks) noteV2Unanswered(address domain.PeerAddress) time.Duration {
	var wait time.Duration
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		m.stepV2RetryLocked(address, mark, now)
		wait = mark.v2Retry.until.Sub(now)
	})
	return wait
}

// openDial records a dial to address running from now until the returned
// function is called. One dial moves the address's v2 retry backoff on by
// one step at most, however many of its stages end without v2 and however
// long it runs: a TLS attempt and a probe, or a probe and the TLS attempt a
// v2 welcome called for, are one attempt, and an overlay dial may outlast
// the whole wait its first stage started. Dials to the address that run
// together count as one, as the running wait already makes them.
func (m *sessionAddressMarks) openDial(address domain.PeerAddress) (closeDial func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dials == nil {
		m.dials = make(map[domain.PeerAddress]*openDials)
	}
	open, present := m.dials[address]
	if !present {
		open = &openDials{}
		m.dials[address] = open
	}
	open.running++
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		open.running--
		if open.running == 0 {
			delete(m.dials, address)
		}
	}
}

// stepV2RetryLocked moves the v2 retry backoff of mark on by one step and
// reports whether it did. No step is taken while the wait runs
// (stepV2Retry), nor in a dial to address that already took its own
// (openDial). Caller holds m.mu.
func (m *sessionAddressMarks) stepV2RetryLocked(address domain.PeerAddress, mark *sessionAddressMark, now time.Time) bool {
	open := m.dials[address]
	if open != nil && open.stepped {
		return false
	}
	next, stepped := stepV2Retry(mark.v2Retry, now)
	mark.v2Retry = next
	if open != nil {
		open.stepped = true
	}
	return stepped
}

// legacyHeld reports whether the fall back to v1 at address is held now.
func (m *sessionAddressMarks) legacyHeld(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool {
		return mark.legacyHold != nil && now.Before(mark.legacyHold.until)
	})
}

// v2RetryHeld reports whether the next dial to address must wait for its v2
// retry backoff.
func (m *sessionAddressMarks) v2RetryHeld(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool {
		return mark.v2Retry != nil && now.Before(mark.v2Retry.until)
	})
}

// storeLocked writes mark back, or drops it when it says nothing any more.
// Caller holds m.mu.
func (m *sessionAddressMarks) storeLocked(address domain.PeerAddress, mark sessionAddressMark, now time.Time) {
	if mark.forgettableAt(now) {
		delete(m.marks, address)
		return
	}
	mark.touched = now
	m.marks[address] = mark
}

// markForUpdateLocked returns the mark of address, making room for it first
// when it is new and the map is at its bound. What goes, in this order: a
// mark that says nothing any more; the least recently written mark that
// neither carries a count nor makes its address require v2; the least
// recently written mark of all. The last choice loses a count or a v2_strong,
// so that address's backoffs start over or its next dial may probe again —
// accepted, it takes 16 384 fresher marks, and the store's endpoint binding
// still requires v2 where a proven session left one. One pass, made only when
// the map is full. Caller holds m.mu.
func (m *sessionAddressMarks) markForUpdateLocked(address domain.PeerAddress, now time.Time) sessionAddressMark {
	mark, present := m.marks[address]
	if present || len(m.marks) < maxSessionAddressMarks {
		return mark
	}
	var oldest, oldestPlain markCandidate
	for candidate, other := range m.marks {
		if other.forgettableAt(now) {
			delete(m.marks, candidate)
			return mark
		}
		oldest.consider(candidate, other.touched)
		if !other.counted() && !other.protects(now) {
			oldestPlain.consider(candidate, other.touched)
		}
	}
	if oldestPlain.found {
		delete(m.marks, oldestPlain.address)
		return mark
	}
	delete(m.marks, oldest.address)
	return mark
}

// markCandidate is the least recently written mark seen so far in a scan.
type markCandidate struct {
	found   bool
	address domain.PeerAddress
	touched time.Time
}

func (c *markCandidate) consider(address domain.PeerAddress, touched time.Time) {
	if c.found && !touched.Before(c.touched) {
		return
	}
	c.found, c.address, c.touched = true, address, touched
}

// noteClaimedOldVersion moves the old-version delay of address on after its
// correct answer claimed version, and returns the wait it now imposes. An
// answer met while the wait runs takes no step: one attempt counts once.
func (m *sessionAddressMarks) noteClaimedOldVersion(address domain.PeerAddress, claimed domain.ProtocolVersion) time.Duration {
	var wait time.Duration
	m.update(address, func(mark *sessionAddressMark, now time.Time) {
		previous := mark.oldVersion
		switch {
		case previous != nil && now.Before(previous.until):
			previous.claimed = claimed
		case previous != nil:
			steps := previous.steps + 1
			mark.oldVersion = &oldVersionBackoff{until: now.Add(doubledBackoff(oldVersionRetryBase, oldVersionRetryMax, steps)), steps: steps, claimed: claimed}
		default:
			mark.oldVersion = &oldVersionBackoff{until: now.Add(oldVersionRetryBase), steps: 1, claimed: claimed}
		}
		wait = mark.oldVersion.until.Sub(now)
	})
	return wait
}

// oldVersionHeld reports whether the next dial to address must wait for its
// old-version delay.
func (m *sessionAddressMarks) oldVersionHeld(address domain.PeerAddress) bool {
	return m.read(address, func(mark sessionAddressMark, now time.Time) bool {
		return mark.oldVersion != nil && now.Before(mark.oldVersion.until)
	})
}

// endOldVersionWait ends the current wait of address's old-version delay
// and keeps its count: a retry by hand is one more attempt, and if the peer
// still claims an old version the next delay grows on from where it was. It
// reports whether there was a delay.
func (m *sessionAddressMarks) endOldVersionWait(address domain.PeerAddress) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || mark.oldVersion == nil {
		return false
	}
	ended := *mark.oldVersion
	ended.until = m.clock()
	mark.oldVersion = &ended
	m.storeLocked(address, mark, m.clock())
	return true
}

// forgetOldVersion drops the old-version delay of address and its count, and
// reports whether there was one.
func (m *sessionAddressMarks) forgetOldVersion(address domain.PeerAddress) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || mark.oldVersion == nil {
		return false
	}
	mark.oldVersion = nil
	m.storeLocked(address, mark, m.clock())
	return true
}

// oldVersionDelays is every endpoint with an old-version delay on record.
func (m *sessionAddressMarks) oldVersionDelays() []domain.EndpointDialDelay {
	m.mu.Lock()
	defer m.mu.Unlock()
	var delays []domain.EndpointDialDelay
	for address, mark := range m.marks {
		if mark.oldVersion == nil {
			continue
		}
		delays = append(delays, domain.EndpointDialDelay{
			Endpoint:       address,
			Reason:         domain.EndpointDialDelayClaimedOldVersion,
			ClaimedVersion: mark.oldVersion.claimed,
			Steps:          mark.oldVersion.steps,
			NextAttemptAt:  mark.oldVersion.until,
		})
	}
	return delays
}
