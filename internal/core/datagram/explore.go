package datagram

import (
	"container/list"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
)

// explore.go implements the deterministic rotation of route_policy
// `explore` (§4.3).
//
// The candidates are already sorted, so the rotation only chooses where to
// START walking them: the offset HMAC(node_local_secret, dst) mod K
// decorrelates that choice between nodes, and the actual index is
// (offset + counter) mod K.
//
// Rotation, not "a new hash": a new hash does not guarantee a new result
// modulo K, so "the retry goes through a different candidate" has to be
// produced by an algorithm rather than hoped for.
//
// Reference: docs/refactoring/datagram-transport.md §4.3.

// DefaultExploreCounters is the starting size of the bounded counter LRU.
// A map keyed by an arbitrary tuple would grow without bound, so the LRU is
// part of the contract, not a tuning detail: evicted keys fall back to the
// node-wide counter and degrade to decorrelation.
const DefaultExploreCounters = 4096

// exploreKey is the rotation key of §4.3: (dst, hash(dtype)).
//
// The key is wider than dst on purpose. A counter keyed by address alone
// would be shifted by unrelated sends — transit frames to the same
// destination and frames of other types, which have a different candidate
// set — and two consecutive retries of one file would land on the same first
// hop again.
type exploreKey struct {
	digest [sha256.Size]byte
	dst    domain.PeerIdentity
}

// newExploreKey hashes the dtype half of the key. The trailing separator
// keeps the digest injective over the name alphabet, which is what lets a
// second field be folded in later without silently colliding with a dtype
// that spells out the concatenation.
func newExploreKey(dst domain.PeerIdentity, dtype domain.DType) exploreKey {
	hash := sha256.New()
	hash.Write([]byte(dtype))
	hash.Write([]byte{0})
	key := exploreKey{dst: dst}
	hash.Sum(key.digest[:0])
	return key
}

// exploreCounter is one LRU entry.
type exploreCounter struct {
	key   exploreKey
	value uint64
}

// exploreRotator owns the bounded LRU of rotation counters and the node
// secret behind the starting offset.
//
// Concurrency: one mutex guards both the LRU and the global counter, so the
// increment and the lookup that consumes it are one atomic step. Two
// concurrent sends of the same key get two different counters — which is
// all the guarantee §4.3 makes for that case, since it degrades to
// decorrelation under concurrency anyway.
type exploreRotator struct {
	entries  map[exploreKey]*list.Element
	order    *list.List
	secret   NodeSecret
	capacity int
	global   uint64
	mu       sync.Mutex
}

func newExploreRotator(secret NodeSecret, capacity int) *exploreRotator {
	if capacity <= 0 {
		capacity = DefaultExploreCounters
	}
	return &exploreRotator{
		entries:  make(map[exploreKey]*list.Element, capacity),
		order:    list.New(),
		secret:   secret,
		capacity: capacity,
	}
}

// next returns the counter for one explore send and advances it.
//
// A hit increments the entry: consecutive sends of a key that is in the LRU
// walk the candidates round-robin, which is the honest half of the
// guarantee. A miss seeds the entry from the node-wide counter, so the very
// first send of a key and any send after an eviction are decorrelated
// rather than aligned — §4.3 promises exactly that and no more.
func (r *exploreRotator) next(key exploreKey) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if element, ok := r.entries[key]; ok {
		r.order.MoveToFront(element)
		counter, _ := element.Value.(*exploreCounter)
		counter.value++
		return counter.value
	}

	r.global++
	seeded := r.global
	element := r.order.PushFront(&exploreCounter{key: key, value: seeded})
	r.entries[key] = element
	if r.order.Len() > r.capacity {
		r.evictOldestLocked()
	}
	return seeded
}

// evictOldestLocked drops the least recently used entry. The caller holds
// r.mu.
func (r *exploreRotator) evictOldestLocked() {
	oldest := r.order.Back()
	if oldest == nil {
		return
	}
	r.order.Remove(oldest)
	if counter, ok := oldest.Value.(*exploreCounter); ok {
		delete(r.entries, counter.key)
	}
}

// offset is HMAC(node_local_secret, dst) mod k — the per-node starting
// point that keeps two nodes with identical routing tables from hammering
// the same candidate.
func (r *exploreRotator) offset(dst domain.PeerIdentity, k int) uint64 {
	if k <= 0 {
		return 0
	}
	mac := hmac.New(sha256.New, r.nodeSecret())
	identity := dst
	mac.Write(identity[:])
	sum := mac.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8]) % uint64(k)
}

// rotate reorders candidates so the one at (offset + counter) mod K comes
// first, the rest following in comparator order and wrapping around.
//
// It MUTATES the counter and must therefore be called exactly once per
// explore send — never by the read-only plan. The direct session is not
// part of the input: step 1 of the scheduler always tries it first, and the
// rotation acts on routing-table candidates only, for when the direct path
// is unavailable or exhausted.
//
// K = 1 degenerates without an error, and that is honest: there is no
// alternative to rotate to.
func (r *exploreRotator) rotate(candidates []RouteCandidate, key exploreKey) []RouteCandidate {
	k := len(candidates)
	if k < 2 {
		if k == 1 {
			// Still consume a counter tick: the key's epoch must advance
			// with every explore send of it, or a set that shrinks to one
			// candidate and grows back would replay an old index.
			r.next(key)
		}
		return candidates
	}
	index := int((r.offset(key.dst, k) + r.next(key)) % uint64(k))
	rotated := make([]RouteCandidate, 0, k)
	rotated = append(rotated, candidates[index:]...)
	rotated = append(rotated, candidates[:index]...)
	return rotated
}

// nodeSecret is the ONE call into the node's local secret, boundary included.
//
// A panic becomes an EMPTY key, which is the same rotation every node with an
// unset secret already computes: the offset stays deterministic and the walk
// stays correct, only its decorrelation between nodes is lost — a degradation
// of an anti-correlation measure, never of the routing decision itself. The
// alternative is a panic on the session reader for a memory read.
func (r *exploreRotator) nodeSecret() []byte {
	if r.secret == nil {
		return nil
	}
	return guardHook(hookSite{hook: "NodeLocalSecret"}, nil, func() []byte {
		return r.secret.NodeLocalSecret()
	})
}
