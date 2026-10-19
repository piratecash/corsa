package findnode

import (
	"sync"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const MaxPendingPerSession = 16
const MaxPendingPerTarget = 16
const MaxPendingTotal = 512

type pendingKey struct {
	session Session
	id      domain.FindNodeRequestID
}
type pendingEntry struct {
	source, target Session
	request        protocol.FindNodeRequest
	forwarded      protocol.FindNodeRequest
	expires        Instant
	intake         *candidatePending
}
type Forward struct {
	Request protocol.FindNodeRequest
	entry   *pendingEntry
	table   *PendingTable
}
type PendingSnapshot struct {
	Counters
	Pending int
}

// PendingTable enforces correlation and physical pending caps only. Its caller
// MUST separately pass role/capability, input-rate, pair, distinct-target and
// fair deadline admission gates. Reserve is not a completed PR-4 admission.
type PendingTable struct {
	mu        sync.Mutex
	bySource  map[pendingKey]*pendingEntry
	byTarget  map[pendingKey]*pendingEntry
	sources   map[Session]int
	targets   map[domain.PeerIdentity]int
	last      Instant
	counters  Counters
	published atomic.Pointer[PendingSnapshot]
}

func NewPendingTable() *PendingTable {
	return &PendingTable{bySource: make(map[pendingKey]*pendingEntry), byTarget: make(map[pendingKey]*pendingEntry), sources: make(map[Session]int), targets: make(map[domain.PeerIdentity]int)}
}
func (p *PendingTable) Reserve(source, target Session, request protocol.FindNodeRequest, now Instant) (Forward, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.publish()
	if err := p.advance(now); err != nil {
		return Forward{}, err
	}
	if !source.valid() || !target.valid() {
		p.counters.Refused++
		return Forward{}, ErrUnauthenticated
	}
	if target.Identity() != request.Target || source.Identity() == request.Target || source == target {
		p.counters.Refused++
		return Forward{}, ErrUnmatchedAnswer
	}
	key := pendingKey{source, request.ID}
	if _, ok := p.bySource[key]; ok {
		p.counters.Refused++
		return Forward{}, ErrDuplicateID
	}
	if len(p.bySource) >= MaxPendingTotal || p.sources[source] >= MaxPendingPerSession || p.targets[target.Identity()] >= MaxPendingPerTarget {
		p.counters.Refused++
		return Forward{}, ErrCapacity
	}
	expires, err := now.plus(responderTimeout)
	if err != nil {
		return Forward{}, err
	}
	id, err := target.allocateID()
	if err != nil {
		return Forward{}, err
	}
	forwarded := request
	forwarded.ID = id
	entry := &pendingEntry{source: source, target: target, request: request, forwarded: forwarded, expires: expires}
	p.bySource[key] = entry
	p.byTarget[pendingKey{target, id}] = entry
	p.sources[source]++
	p.targets[target.Identity()]++
	p.counters.Accepted++
	return Forward{Request: forwarded, entry: entry, table: p}, nil
}
func (p *PendingTable) Answer(target Session, answer protocol.FindNodeAnswer, now Instant) (Delivery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.publish()
	if err := p.advance(now); err != nil {
		return Delivery{}, err
	}
	entry := p.byTarget[pendingKey{target, answer.ID}]
	if entry == nil || answer.Target != entry.request.Target || !target.valid() || !entry.source.valid() {
		p.counters.Refused++
		return Delivery{}, ErrUnmatchedAnswer
	}
	p.release(entry)
	p.counters.Completed++
	// R copies the signed body without signature verification. Only its local
	// envelope ID changes; the initiator verifies against its retained challenge.
	answer.ID = entry.request.ID
	return Delivery{Session: entry.source, Answer: answer}, nil
}
func (p *PendingTable) Cancel(forward Forward) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.publish()
	if forward.table != p || forward.entry == nil || p.bySource[pendingKey{forward.entry.source, forward.entry.request.ID}] != forward.entry {
		return false
	}
	p.release(forward.entry)
	p.counters.Cancelled++
	return true
}
func (p *PendingTable) CloseSession(session Session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.publish()
	session.close()
	for _, entry := range p.bySource {
		if entry.source == session || entry.target == session {
			p.release(entry)
			p.counters.Closed++
		}
	}
}
func (p *PendingTable) Expire(now Instant) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.publish()
	return p.advance(now)
}
func (p *PendingTable) advance(now Instant) error {
	if now < 0 || now < p.last {
		return ErrClock
	}
	p.last = now
	for _, entry := range p.bySource {
		if now >= entry.expires {
			p.release(entry)
			p.counters.Expired++
		}
	}
	return nil
}
func (p *PendingTable) release(entry *pendingEntry) {
	delete(p.bySource, pendingKey{entry.source, entry.request.ID})
	if entry.intake == nil || entry.intake.phase != candidateProvisional {
		delete(p.byTarget, pendingKey{entry.target, entry.forwarded.ID})
	}
	p.sources[entry.source]--
	if p.sources[entry.source] == 0 {
		delete(p.sources, entry.source)
	}
	p.targets[entry.request.Target]--
	if p.targets[entry.request.Target] == 0 {
		delete(p.targets, entry.request.Target)
	}
}
func (p *PendingTable) publish() {
	p.published.Store(&PendingSnapshot{Counters: p.counters, Pending: len(p.bySource)})
}
func (p *PendingTable) Snapshot() PendingSnapshot {
	if s := p.published.Load(); s != nil {
		return *s
	}
	return PendingSnapshot{}
}
