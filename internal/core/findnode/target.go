package findnode

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const MaxTargetEntries = 256
const MaxTargetEntriesPerSession = 8
const MaxChallengeSessions = 3

type TargetResultKind uint8

const (
	TargetNeedsSignature TargetResultKind = iota + 1
	TargetWaiting
	TargetAnswerReady
)

type SignWork struct {
	cache *TargetCache
	entry *targetEntry
}

func (w SignWork) Challenge() domain.FindNodeChallenge {
	if w.entry == nil {
		return domain.FindNodeChallenge{}
	}
	return w.entry.challenge
}
func (w SignWork) Target() domain.PeerIdentity {
	if w.cache == nil {
		return domain.PeerIdentity{}
	}
	return w.cache.self
}

type TargetResult struct {
	Kind   TargetResultKind
	Work   SignWork
	Answer protocol.FindNodeAnswer
}
type targetPhase uint8

const (
	targetReserved targetPhase = iota + 1
	targetReady
	targetCancelled
)

type targetEntry struct {
	challenge         domain.FindNodeChallenge
	ownerGroup        domain.SourceGroup
	history           [MaxChallengeSessions]domain.ConnID
	historyCount      int
	recipients        map[Session]domain.FindNodeRequestID
	received, expires Instant
	phase             targetPhase
	body              AnswerBody
	coordinated       *coordinatedSigning
}
type TargetSnapshot struct {
	Counters
	Entries, Reserved, Ready, CancelledWork, SessionReferences, HistoryReferences int
}

// TargetCache owns only dedup/copy state, not the signing scheduler. The caller
// reserves node/per-session signing budget before dispatching new SignWork.
// Reserved jobs occupy the same 256/8 slots as Ready answers, with an explicit
// deadline; this cannot create a second, uncounted signing queue.
type TargetCache struct {
	mu        sync.Mutex
	self      domain.PeerIdentity
	entries   map[domain.FindNodeChallenge]*targetEntry
	sessions  map[Session]int
	last      Instant
	counters  Counters
	published atomic.Pointer[TargetSnapshot]
}

func NewTargetCache(self domain.PeerIdentity) (*TargetCache, error) {
	if self.IsZero() {
		return nil, ErrInvalid
	}
	return &TargetCache{self: self, entries: make(map[domain.FindNodeChallenge]*targetEntry), sessions: make(map[Session]int)}, nil
}
func (c *TargetCache) Identity() domain.PeerIdentity { return c.self }

// Sessions returns a bounded snapshot for an authoritative PrioritySnapshot.
// The caller adds a new source and must include all live candidates for the
// four-oldest rule, not merely the holders returned here.
func (c *TargetCache) Sessions() []Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Session, 0, len(c.sessions))
	for session := range c.sessions {
		out = append(out, session)
	}
	return out
}
func (c *TargetCache) Admit(source Session, request protocol.FindNodeRequest, now, signBy Instant, priorities PrioritySnapshot) (TargetResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.publish()
	var changes targetChanges
	defer changes.assertEmpty()
	return c.admitLocked(source, request, now, signBy, priorities, &changes)
}

// admitLocked requires c.mu.
func (c *TargetCache) admitLocked(source Session, request protocol.FindNodeRequest, now, signBy Instant, priorities PrioritySnapshot, changes *targetChanges) (TargetResult, error) {
	if err := c.advanceLocked(now, changes); err != nil {
		return TargetResult{}, err
	}
	if !source.valid() {
		c.counters.Refused++
		return TargetResult{}, ErrUnauthenticated
	}
	if request.Target != c.self || signBy <= now || signBy-now > responderTimeout {
		c.counters.Refused++
		return TargetResult{}, ErrInvalid
	}
	if priorities.target != c.self || priorities.at != now || !c.completePriorities(source, priorities) {
		c.counters.Refused++
		return TargetResult{}, ErrPrioritySnapshot
	}
	entry := c.entries[request.Challenge]
	if entry != nil {
		if entry.seen(source.ConnID()) {
			c.counters.Refused++
			return TargetResult{}, ErrDuplicateLink
		}
		if entry.historyCount >= MaxChallengeSessions {
			c.counters.Refused++
			return TargetResult{}, ErrDuplicateExcess
		}
		if entry.phase == targetCancelled {
			c.counters.Refused++
			return TargetResult{}, ErrStaleWork
		}
	}
	if err := c.makeRoomLocked(source, entry, priorities, changes); err != nil {
		c.counters.Refused++
		return TargetResult{}, err
	}
	if entry != nil {
		entry.recipients[source] = request.ID
		entry.remember(source.ConnID())
		c.sessions[source]++
		c.counters.Accepted++
		if entry.phase == targetReady {
			return TargetResult{Kind: TargetAnswerReady, Answer: entry.body.Answer(request.ID)}, nil
		}
		return TargetResult{Kind: TargetWaiting}, nil
	}
	entry = &targetEntry{challenge: request.Challenge, ownerGroup: source.state.group, recipients: map[Session]domain.FindNodeRequestID{source: request.ID}, received: now, expires: signBy, phase: targetReserved}
	entry.remember(source.ConnID())
	c.entries[request.Challenge] = entry
	c.sessions[source]++
	c.counters.Accepted++
	return TargetResult{Kind: TargetNeedsSignature, Work: SignWork{cache: c, entry: entry}}, nil
}
func (c *TargetCache) Complete(work SignWork, body AnswerBody, now Instant) ([]Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.publish()
	var changes targetChanges
	defer changes.assertEmpty()
	return c.completeLocked(work, body, now, &changes)
}

// completeLocked requires c.mu.
func (c *TargetCache) completeLocked(work SignWork, body AnswerBody, now Instant, changes *targetChanges) ([]Delivery, error) {
	if err := c.advanceLocked(now, changes); err != nil {
		return nil, err
	}
	if work.cache != c || work.entry == nil || c.entries[work.entry.challenge] != work.entry || work.entry.phase != targetReserved {
		return nil, ErrStaleWork
	}
	if body.Target != c.self || len(body.PublicKey.Bytes()) != 32 || body.PublicKey.Fingerprint() != c.self.String() {
		return nil, ErrInvalid
	}
	expires, err := now.plus(cacheLifetime)
	if err != nil {
		return nil, err
	}
	entry := work.entry
	changes.retireEntry(entry)
	entry.body = body
	entry.phase = targetReady
	entry.expires = expires
	c.counters.Completed++
	deliveries := make([]Delivery, 0, len(entry.recipients))
	for session, id := range entry.recipients {
		if session.valid() {
			deliveries = append(deliveries, Delivery{Session: session, Answer: body.Answer(id)})
		}
	}
	sort.Slice(deliveries, func(i, j int) bool { return deliveries[i].Session.ConnID() < deliveries[j].Session.ConnID() })
	return deliveries, nil
}
func (c *TargetCache) Cancel(work SignWork) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.publish()
	var changes targetChanges
	defer changes.assertEmpty()
	return c.cancelLocked(work, &changes)
}

// cancelLocked requires c.mu.
func (c *TargetCache) cancelLocked(work SignWork, changes *targetChanges) bool {
	if work.cache != c || work.entry == nil || c.entries[work.entry.challenge] != work.entry || work.entry.phase != targetReserved {
		return false
	}
	// Cancellation invalidates this generation but retains its bounded history
	// until the original signing deadline. It is not a close-driven dedup reset.
	changes.retireEntry(work.entry)
	c.releaseRecipients(work.entry)
	work.entry.phase = targetCancelled
	c.counters.Cancelled++
	return true
}
func (c *TargetCache) CloseSession(session Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.publish()
	var changes targetChanges
	defer changes.assertEmpty()
	c.closeSessionLocked(session, &changes)
}

// closeSessionLocked requires c.mu. Closing a recipient never resets history.
func (c *TargetCache) closeSessionLocked(session Session, changes *targetChanges) {
	session.close()
	for _, entry := range c.entries {
		if state := entry.coordinated; state != nil && state.phase == signingAliasPending && state.alias.slot.source == session {
			changes.retireAlias(state)
			state.phase = signingUnsignable
		}
		if _, ok := entry.recipients[session]; !ok {
			continue
		}
		delete(entry.recipients, session)
		c.sessions[session]--
		c.counters.Closed++

	}
	delete(c.sessions, session)
}
func (c *TargetCache) Expire(now Instant) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.publish()
	var changes targetChanges
	defer changes.assertEmpty()
	return c.advanceLocked(now, &changes)
}

// advanceLocked requires c.mu.
func (c *TargetCache) advanceLocked(now Instant, changes *targetChanges) error {
	if now < 0 || now < c.last {
		return ErrClock
	}
	c.last = now
	for _, entry := range c.entries {
		if now >= entry.expires {
			c.removeLocked(entry, changes)
			c.counters.Expired++
		}
	}
	return nil
}
func (c *TargetCache) completePriorities(source Session, p PrioritySnapshot) bool {
	if _, ok := p.priorities[source]; !ok {
		return false
	}
	for session := range c.sessions {
		if _, ok := p.priorities[session]; !ok {
			return false
		}
	}
	return true
}
func entryPriority(entry *targetEntry, p PrioritySnapshot) (Priority, Instant) {
	best := PriorityNew
	oldest := Instant(1<<63 - 1)
	for session := range entry.recipients {
		priority := p.priorities[session]
		if priority < best {
			best = priority
			oldest = session.state.opened
		} else if priority == best && session.state.opened < oldest {
			oldest = session.state.opened
		}
	}
	return best, oldest
}

// makeRoomLocked requires c.mu.
func (c *TargetCache) makeRoomLocked(source Session, except *targetEntry, p PrioritySnapshot, changes *targetChanges) error {
	sessionFull := c.sessions[source] >= MaxTargetEntriesPerSession
	if !sessionFull && (except != nil || len(c.entries) < MaxTargetEntries) {
		return nil
	}
	weights := make(map[domain.SourceGroup]int)
	for _, entry := range c.entries {
		priority, _ := entryPriority(entry, p)
		if priority == PriorityNew {
			weights[entry.ownerGroup]++
		}
	}
	var victim *targetEntry
	for _, entry := range c.entries {
		if entry == except {
			continue
		}
		if sessionFull {
			if _, ok := entry.recipients[source]; !ok {
				continue
			}
		}
		priority, _ := entryPriority(entry, p)
		if priority == PriorityProtected || priority < p.priorities[source] {
			continue
		}
		if victim == nil || preferEviction(entry, victim, p, weights) {
			victim = entry
		}
	}
	if victim == nil {
		return ErrCapacity
	}
	vp, age := entryPriority(victim, p)
	if vp == PriorityAged && p.priorities[source] == PriorityAged && source.state.opened > age {
		return ErrCapacity
	}
	c.removeLocked(victim, changes)
	c.counters.Evicted++
	return nil
}
func preferEviction(a, b *targetEntry, p PrioritySnapshot, weights map[domain.SourceGroup]int) bool {
	ap, aa := entryPriority(a, p)
	bp, ba := entryPriority(b, p)
	if ap != bp {
		return ap > bp
	}
	if ap == PriorityNew {
		aw, bw := weights[a.ownerGroup], weights[b.ownerGroup]
		if aw != bw {
			return aw > bw
		}
		if a.ownerGroup != b.ownerGroup {
			return a.ownerGroup < b.ownerGroup
		}
	} else if aa != ba {
		return aa > ba
	}
	if a.received != b.received {
		return a.received < b.received
	}
	return string(a.challenge[:]) < string(b.challenge[:])
}

// removeLocked requires c.mu.
func (c *TargetCache) removeLocked(entry *targetEntry, changes *targetChanges) {
	changes.retireEntry(entry)
	delete(c.entries, entry.challenge)
	c.releaseRecipients(entry)
}
func (c *TargetCache) releaseRecipients(entry *targetEntry) {
	for session := range entry.recipients {
		c.sessions[session]--
		if c.sessions[session] == 0 {
			delete(c.sessions, session)
		}
	}
	clear(entry.recipients)
}
func (c *TargetCache) publish() {
	s := c.snapshotLocked()
	c.published.Store(&s)
}

// snapshotLocked requires c.mu.
func (c *TargetCache) snapshotLocked() TargetSnapshot {
	s := TargetSnapshot{Counters: c.counters, Entries: len(c.entries)}
	for _, entry := range c.entries {
		switch entry.phase {
		case targetReady:
			s.Ready++
		case targetReserved:
			s.Reserved++
		case targetCancelled:
			s.CancelledWork++
		}
		s.SessionReferences += len(entry.recipients)
		s.HistoryReferences += entry.historyCount
	}
	return s
}
func (c *TargetCache) Snapshot() TargetSnapshot {
	if s := c.published.Load(); s != nil {
		return *s
	}
	return TargetSnapshot{}
}

func (e *targetEntry) seen(connID domain.ConnID) bool {
	for i := 0; i < e.historyCount; i++ {
		if e.history[i] == connID {
			return true
		}
	}
	return false
}
func (e *targetEntry) remember(connID domain.ConnID) {
	e.history[e.historyCount] = connID
	e.historyCount++
}
