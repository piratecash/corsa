package node

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// recordProtection is the live set the session-record cache consults before
// it drops an entry: the addresses this node currently holds a session with
// or has an open identity lookup for. A cached record IS the seq floor for
// its address (trust_session_records.go), so dropping one under a live peer
// re-opens the downgrade the floor exists to refuse — the peer's own earlier
// record, the predecessor of a revocation, would merge as `inserted`.
//
// The set is maintained by EVENTS, not by a periodic sample. A pin is taken
// inside the very critical section that makes the session or the lookup
// real — the same s.peerMu section that increments identitySessions, the
// same r.mu section that inserts into resolutions — so the protection and
// the fact that justifies it become visible together. A sampled set could
// not do that: between two samples a session comes up, its record is
// accepted, and an unrelated import evicts it from a full cache before the
// next sample ever names the address.
//
// An eviction does not merely CONSULT this set, it runs INSIDE it:
// dropIfUnprotected performs the check and the removal in one critical
// section, so a pin is ordered strictly before or strictly after the
// decision it would change. Asking first and deleting afterwards — however
// small the gap, and however many times the answer is re-read — leaves the
// interleaving where the pin lands between the two and the record of a
// now-live session is deleted anyway. When the pin loses that race the
// record was already gone when the session began, which is the ordinary
// state of a node that has never met the peer; what must not happen is a
// floor disappearing UNDER a session that already had one.
//
// The lock-free `contains` remains for the diagnostic count, where an
// answer that is one instant stale costs nothing.
//
// refs counts holders rather than recording a flag, because the two sources
// overlap and outlive each other independently: a peer can hold several
// sessions, and a lookup for the same address can still be open when the
// last of them closes.
//
// Size is bounded by what fills it — live sessions and open lookups — so a
// cache in which everything is protected exceeds its budget by that working
// set and no more. reconcile() is the self-heal: it replaces refs with
// counts recomputed from those two sources, so a pin whose release was
// missed (or a release that ran twice) survives at most one maintenance
// pass. It commits only if nothing pinned or unpinned while the caller was
// gathering those counts — an unconditional replacement would erase a pin
// taken during the gather and hand the cache a live session it believes is
// not there.
//
// This mutex is the terminal lock of every path that reaches it: the
// session and lookup paths take it under their own domain mutexes, the
// cache takes it under trustStore.mu, and it acquires nothing itself and
// does no I/O (docs/locking.md).
type recordProtection struct {
	mu   sync.Mutex
	refs map[string]int
	// gen advances on every change to refs. It is what lets a reconcile
	// pass tell "nothing moved while I was looking" from "the world
	// changed under me", without holding this mutex across the peer-domain
	// and resolver reads that produce its input — which would invert the
	// order those paths take when they pin.
	gen       uint64
	published atomic.Pointer[map[string]struct{}]
}

func newRecordProtection() *recordProtection {
	p := &recordProtection{refs: make(map[string]int)}
	p.publishLocked()
	return p
}

// pin adds one holder for the address. Callers hold whichever lock makes
// the underlying fact true, so that the fact and its protection land
// together; this type's own mutex is a leaf and takes nothing further.
func (p *recordProtection) pin(address string) {
	if address == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refs[address]++
	p.gen++
	if p.refs[address] == 1 {
		p.publishLocked()
	}
}

// unpin drops one holder; the address leaves the set when the last one
// goes. An unpin with no matching pin is ignored rather than allowed to go
// negative — the reconcile pass is what repairs drift, and a negative
// count would make it permanent.
func (p *recordProtection) unpin(address string) {
	if address == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	count, ok := p.refs[address]
	if !ok {
		return
	}
	p.gen++
	if count <= 1 {
		delete(p.refs, address)
		p.publishLocked()
		return
	}
	p.refs[address] = count - 1
}

// generation is the value a reconcile pass must quote back to commit.
func (p *recordProtection) generation() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gen
}

// reconcile replaces the holders with counts recomputed from the live
// sources, but ONLY if nothing has pinned or unpinned since `gen` was read
// — that is, since before the caller began gathering them. Pins are taken
// atomically with the facts they describe, so in the ordinary case this
// changes nothing and exists only so that a missed pin or a double release
// cannot outlive one maintenance pass; committing a stale gather would turn
// the self-heal into the very fault it repairs, dropping the pin of a
// session that came up while the counts were being collected.
//
// Returns false when it refused. The caller may retry, and a pass that
// keeps losing simply leaves the set as the events built it — which is the
// authoritative version anyway.
func (p *recordProtection) reconcile(gen uint64, live map[string]int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gen != gen {
		return false
	}
	p.refs = make(map[string]int, len(live))
	for address, count := range live {
		if address == "" || count <= 0 {
			continue
		}
		p.refs[address] = count
	}
	p.gen++
	p.publishLocked()
	return true
}

// dropIfUnprotected runs `drop` and reports true when the address holds no
// protection — with the check and the drop inside one critical section, so
// that a pin taken concurrently either precedes the check (and refuses the
// drop) or follows the drop entirely. This is the whole reason the cache
// reaches for this type at eviction time instead of reading `contains`: the
// answer to "may I delete this" is only meaningful if the deletion happens
// while the answer is still true.
//
// `drop` must touch nothing but state the caller already owns — it runs
// under this mutex, which acquires nothing further.
func (p *recordProtection) dropIfUnprotected(address string, drop func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, protected := p.refs[address]; protected {
		return false
	}
	drop()
	return true
}

// contains reports whether the address is protected. One atomic load and
// one map read: no mutex, so a caller already holding one adds no lock
// ordering by asking.
func (p *recordProtection) contains(address string) bool {
	if p == nil {
		return false
	}
	set := p.published.Load()
	if set == nil {
		return false
	}
	_, ok := (*set)[address]
	return ok
}

// publishLocked swaps in an immutable copy of the current holders. Caller
// holds mu.
func (p *recordProtection) publishLocked() {
	set := make(map[string]struct{}, len(p.refs))
	for address := range p.refs {
		set[address] = struct{}{}
	}
	p.published.Store(&set)
}

// pinRecordProtection / releaseRecordProtection are the Service-side names
// the session and lookup paths call. They exist so those call sites read as
// what they are doing — holding the identity's seq floor for as long as the
// node is talking to it — rather than as reaching into a cache.
func (s *Service) pinRecordProtection(identity domain.PeerIdentity) {
	if identity.IsZero() || s.recordProtection == nil {
		return
	}
	s.recordProtection.pin(identity.String())
}

func (s *Service) releaseRecordProtection(identity domain.PeerIdentity) {
	if identity.IsZero() || s.recordProtection == nil {
		return
	}
	s.recordProtection.unpin(identity.String())
}

// liveRecordProtectionCounts recomputes the truth behind the set: how many
// sessions each identity holds, plus one for an open lookup. Each source is
// read under its own lock and released before the next, and the caller must
// hold none — this is the reconcile input, not a hot path.
func (s *Service) liveRecordProtectionCounts() map[string]int {
	live := make(map[string]int)

	s.peerMu.RLock()
	for identity, sessions := range s.identitySessions {
		if sessions > 0 {
			live[identity.String()] += sessions
		}
	}
	s.peerMu.RUnlock()

	if s.identityResolver != nil {
		for _, target := range s.identityResolver.openResolutionTargets() {
			live[target.String()]++
		}
	}
	return live
}

// reconcileRecordProtectionAttempts bounds the retries of one maintenance
// pass. Each attempt fails only if a session or a lookup started or ended
// while the counts were being gathered; giving up after a few is correct
// rather than merely pragmatic, because what the pass would have installed
// is what the events have already installed — the retry exists for the
// gather, not for the repair.
const reconcileRecordProtectionAttempts = 3

// reconcileRecordProtection runs the self-heal on the maintenance cadence.
func (s *Service) reconcileRecordProtection() {
	if s.recordProtection == nil {
		return
	}
	for range reconcileRecordProtectionAttempts {
		gen := s.recordProtection.generation()
		if s.recordProtection.reconcile(gen, s.liveRecordProtectionCounts()) {
			return
		}
	}
	log.Debug().Msg("record_protection_reconcile_skipped_busy")
}
