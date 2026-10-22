package findnode

import (
	"sync"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
)

// candidatePendingBook is one private physical book, not a public PendingTable.
// Every Locked helper and release require the original book.mu (and owner.mu
// for owner callers). No index/cell pointer escapes a locked call. Old entries
// and intake objects are never zeroed or recycled when their sparse cell clears.
type candidatePendingBook struct {
	mu        sync.Mutex
	entries   [MaxPendingTotal]*pendingEntry
	last      Instant
	counters  Counters
	published atomic.Pointer[PendingSnapshot]
}

func newCandidatePendingBook() *candidatePendingBook { return &candidatePendingBook{} }
func (p *candidatePendingBook) sourceIndexLocked(key pendingKey) int {
	for i, entry := range p.entries {
		if entry != nil && entry.source == key.session && entry.request.ID == key.id {
			return i
		}
	}
	return -1
}
func (p *candidatePendingBook) targetEntryLocked(key pendingKey) *pendingEntry {
	for _, entry := range p.entries {
		if entry == nil || entry.intake == nil || entry.target != key.session || entry.forwarded.ID != key.id {
			continue
		}
		switch entry.intake.phase {
		case candidateReserved, candidateDispatching, candidateAwaitingAnswer:
			return entry
		}
	}
	return nil
}
func (p *candidatePendingBook) countLocked() int {
	n := 0
	for _, entry := range p.entries {
		if entry != nil {
			n++
		}
	}
	return n
}
func (p *candidatePendingBook) sourceCountLocked(source Session) int {
	n := 0
	for _, entry := range p.entries {
		if entry != nil && entry.source == source {
			n++
		}
	}
	return n
}
func (p *candidatePendingBook) targetCountLocked(target domain.PeerIdentity) int {
	n := 0
	for _, entry := range p.entries {
		if entry != nil && entry.request.Target == target {
			n++
		}
	}
	return n
}
func (p *candidatePendingBook) vacancyLocked() int {
	for i, entry := range p.entries {
		if entry == nil {
			return i
		}
	}
	return -1
}
func (p *candidatePendingBook) release(entry *pendingEntry) {
	if entry == nil {
		return
	}
	for i, current := range p.entries {
		if current == entry {
			p.entries[i] = nil
			return
		}
	}
}

// The old owner never published the public subbook diagnostics. Preserve that
// compatibility state; it is never admission/count/correlation authority.
func (p *candidatePendingBook) snapshot() PendingSnapshot {
	if snapshot := p.published.Load(); snapshot != nil {
		return *snapshot
	}
	return PendingSnapshot{}
}
