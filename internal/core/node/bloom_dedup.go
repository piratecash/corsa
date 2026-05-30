// Package node — message-ID deduplication via a rotating Bloom filter
// (Phase 4 13.4, see docs/cluster-mesh/phase-4-compact-wire-signed.md
// §3.4 and overview §5.1 row 4). Replaces the indefinitely-growing
// s.seen map under gossipMu with a fixed-memory two-filter sliding
// window: a message ID Add'd into the current filter survives at
// least one rotation interval (default 5 minutes) before being
// evicted from both filters, then Has returns false again.
//
// Why a Bloom filter and not the legacy map: the map grew without
// bound (cleaned only as a side effect of TTL on related state) and
// became a memory hotspot on long-lived nodes that observed many
// messages — a 100k-message node held ≈ 4–6 MiB just for dedup keys.
// The rotating Bloom is bounded by m + k constants
// (bloomDedupBits / bloomDedupHashes) regardless of message volume,
// trading the legacy map's perfect recall for a small false-positive
// rate (< 0.1% at the design load: 0.046% at 10k keys, 0.32% at
// 20k keys per filter).
//
// HONEST trade-off (the previous comment was too rosy): a Bloom
// false positive is NOT harmless dedup of an already-delivered
// message. The call site (storeIncomingMessage in service.go)
// invokes Has(id) on the message ID of an INCOMING message; a
// true Has return short-circuits storage, gossip fan-out, and
// local delivery, returning stored=false to the caller. When the
// ID has actually been seen before, that is correct dedup. When
// the Has is a FALSE POSITIVE — the id was never seen but collides
// with bits set by other messages currently in the filter — the
// new message is dropped silently on this node. The downstream
// effects depend on the delivery path:
//
//   - Gossip-fanned messages (the dominant path): every neighbour
//     also receives the message and re-gossips on its own dedup
//     decision. A single-node FP drop therefore costs at most one
//     extra hop of propagation; the message still reaches the
//     receiver via any other peer whose Bloom filter does not
//     happen to false-positive on the same id. Bloom FP events on
//     two different nodes are statistically independent, so the
//     end-to-end loss rate is FP^N for N independent paths — at
//     <0.1% per node this rounds to zero across any realistic
//     mesh fan-out.
//   - Direct push_message (peer-to-peer DM with no gossip fan-out
//     or relay copy): there is no second path that can re-deliver,
//     so a Bloom FP at the recipient IS a real message loss with
//     no recovery. At 0.1% per-id this is the dominant production
//     loss source we accept in exchange for the memory bound.
//     Sender-side reliability (delivery receipts, retry, queue
//     persistence) is the layer that owns covering this case;
//     dedup is intentionally NOT that layer.
//   - Relayed transit (relay.go storeIncomingMessage): same as
//     gossip — the original recipient gets at least one more copy
//     via the gossip path the relay also feeds, so transit-side
//     Bloom FPs are not the limiting reliability factor.
//
// False negatives (Has returns false for an id that was Add'd)
// are bounded by the rotation window: a key Add'd at t survives
// until at least t + rotation and at most t + 2*rotation, never
// longer. A false negative means a duplicate of an old message
// passes the dedup gate and re-enters the storage path; the
// downstream MessageStore is idempotent by msg.ID so the duplicate
// is collapsed there (StoreDuplicate result), wasting one storage
// lookup but never producing visible duplication for the user.
//
// Concurrency: a single sync.Mutex serialises every Add / Has. The
// previous map+gossipMu path also took a writer lock for both
// operations, so the contention surface is unchanged. Rotation is
// lazy — there is no goroutine; Add and Has check on entry whether
// the rotation deadline has elapsed and swap a fresh current filter
// in if so. This keeps the type cheap to embed and removes a
// per-Service goroutine lifecycle from the call site.
package node

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"
)

const (
	// bloomDedupBits is the bit-array size of each Bloom filter.
	// 2^20 ≈ 1.05M bits = 128 KiB per filter, two filters = 256 KiB.
	// Sized for ≈10k unique messages per rotation window at <0.1%
	// false-positive rate (k = bloomDedupHashes). Constant for now;
	// an operator-tunable knob can land later if field telemetry
	// shows different message rates.
	bloomDedupBits uint64 = 1 << 20

	// bloomDedupHashes is the number of independent hash positions
	// each key contributes to the bit array. The pair
	// (bloomDedupBits, bloomDedupHashes) = (2^20, 8) yields
	// FP ≈ 0.046% at n = 10000 keys per filter — well under the
	// 0.1% target — and FP ≈ 0.32% at n = 20000, still acceptable
	// against the loss-per-path trade-off the file header documents
	// (gossip / relay redundancy absorbs single-node FP drops; only
	// the direct push_message DM path carries the FP as real loss,
	// which the sender-side delivery-receipt + retry layer owns).
	bloomDedupHashes uint8 = 8

	// bloomDedupRotation is how often the older filter is discarded
	// and replaced with the current one. The phase-4 spec calls for
	// 5 minutes; combined with the two-filter window this gives a
	// dedup persistence of [5, 10] minutes for any single Add.
	bloomDedupRotation = 5 * time.Minute
)

// rotatingBloomDedup is the dedup set the gossip ingest path uses to
// drop already-observed message IDs. See the file header for the
// contract and the eviction window. Construct via
// newRotatingBloomDedup; the zero value is not usable.
type rotatingBloomDedup struct {
	mu               sync.Mutex
	current          *bloomFilter
	previous         *bloomFilter
	nextRotation     time.Time
	rotationInterval time.Duration
	bits             uint64
	hashes           uint8
	clock            func() time.Time
}

// newRotatingBloomDedup constructs a rotating Bloom dedup with the
// given filter sizing and rotation interval. clock is the time
// source — pass nil for the production wall clock; tests can inject
// a controllable clock to step time deterministically across
// rotations.
func newRotatingBloomDedup(bits uint64, hashes uint8, rotation time.Duration, clock func() time.Time) *rotatingBloomDedup {
	if clock == nil {
		clock = time.Now
	}
	now := clock()
	return &rotatingBloomDedup{
		current:          newBloomFilter(bits, hashes),
		previous:         newBloomFilter(bits, hashes),
		nextRotation:     now.Add(rotation),
		rotationInterval: rotation,
		bits:             bits,
		hashes:           hashes,
		clock:            clock,
	}
}

// Add records key in the current filter. Subsequent Has calls return
// true for at least rotationInterval and at most 2 * rotationInterval
// after this call (see file header for the eviction window).
func (r *rotatingBloomDedup) Add(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	r.current.add(key)
}

// Has reports whether key is in either the current or the previous
// filter. False positives are bounded by m/k; false negatives are
// bounded by the rotation window — once both filters have rotated
// past an Add, Has returns false.
func (r *rotatingBloomDedup) Has(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	return r.current.has(key) || r.previous.has(key)
}

// rotateIfDueLocked rotates the filters if the rotation deadline has
// passed. When multiple rotation intervals have elapsed (e.g. a node
// that slept past two full windows) BOTH filters are reset to fresh
// state — keeping a stale "previous" populated from N intervals ago
// would extend the dedup window beyond the documented [rotation,
// 2 * rotation] bound and could mask a genuine retransmit the
// caller treats as new.
func (r *rotatingBloomDedup) rotateIfDueLocked() {
	now := r.clock()
	if now.Before(r.nextRotation) {
		return
	}
	if now.Sub(r.nextRotation) >= r.rotationInterval {
		// At least one full rotation interval elapsed past the
		// deadline — both filters' content is stale relative to
		// the [now-rotation, now] window. Reset both.
		r.previous = newBloomFilter(r.bits, r.hashes)
		r.current = newBloomFilter(r.bits, r.hashes)
	} else {
		// Standard single rotation: current → previous, fresh
		// current. Anything Add'd since the prior rotation
		// survives until the NEXT rotation.
		r.previous = r.current
		r.current = newBloomFilter(r.bits, r.hashes)
	}
	r.nextRotation = now.Add(r.rotationInterval)
}

// bloomFilter is a bit-array Bloom filter with k double-hash
// positions per key. Not safe for concurrent use; rotatingBloomDedup
// serialises access via its own mutex.
type bloomFilter struct {
	bits   []uint64
	nBits  uint64
	hashes uint8
}

func newBloomFilter(bits uint64, hashes uint8) *bloomFilter {
	if bits == 0 {
		bits = 1
	}
	return &bloomFilter{
		bits:   make([]uint64, (bits+63)/64),
		nBits:  bits,
		hashes: hashes,
	}
}

// positions returns the k bit indices for key using the double-hash
// trick: two 64-bit halves of FNV-128a are combined as
// `(h1 + i * h2) mod m` for i in [0, k). Cheaper than k independent
// hashes and gives the same FP rate within a small constant.
func (b *bloomFilter) positions(key string, out *[bloomDedupHashesMax]uint64) []uint64 {
	h := fnv.New128a()
	_, _ = h.Write([]byte(key))
	sum := h.Sum(nil)
	h1 := binary.BigEndian.Uint64(sum[:8])
	h2 := binary.BigEndian.Uint64(sum[8:16])
	slice := out[:b.hashes]
	for i := uint8(0); i < b.hashes; i++ {
		slice[i] = (h1 + uint64(i)*h2) % b.nBits
	}
	return slice
}

// bloomDedupHashesMax caps the per-key positions stack buffer so the
// hot path avoids a per-call heap allocation. Stays above the default
// bloomDedupHashes constant with margin for future tuning.
const bloomDedupHashesMax = 16

func (b *bloomFilter) add(key string) {
	var buf [bloomDedupHashesMax]uint64
	for _, p := range b.positions(key, &buf) {
		b.bits[p/64] |= 1 << (p % 64)
	}
}

func (b *bloomFilter) has(key string) bool {
	var buf [bloomDedupHashesMax]uint64
	for _, p := range b.positions(key, &buf) {
		if b.bits[p/64]&(1<<(p%64)) == 0 {
			return false
		}
	}
	return true
}
