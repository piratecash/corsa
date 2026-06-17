// Package node — transit delivery-receipt deduplication via a rotating
// hashed key set with a bounded sliding window AND a hard cardinality cap.
//
// Why this exists: the receipt dedup set (Service.seenReceipts) was a
// plain map[string]struct{} that only ever shrank on the single
// gossip-total-failure rollback path (unmarkTransitReceiptSeen). On the
// normal path it grew one entry per unique transit receipt key
// (recipient:messageID:status) and was never evicted, so on a long-lived
// relay node under a steady receipt / re-gossip load it climbed without
// bound — the same unbounded-map memory hotspot that the message-ID dedup
// already retired by moving s.seen onto rotatingBloomDedup (bloom_dedup.go).
//
// Two independent memory bounds, because the time window alone is not enough:
//   - SIZE: keys are stored as a fixed 128-bit hash ([16]byte), the high half
//     of SHA-256, not the raw string. receiptFromFrame does only light
//     validation, so a peer could otherwise feed arbitrarily long
//     ID/Address/Recipient strings straight into the key. Hashing makes
//     per-entry size constant regardless of input.
//   - CARDINALITY: a hard cap bounds the number of retained entries. Within a
//     single window a peer can mint unlimited DISTINCT valid receipt keys
//     (random message IDs); the time window would not evict them until
//     rotation, so without a cap that is a memory-exhaustion vector.
//
// The cap is enforced by EVICTION, not refusal: every Add/MarkIfAbsent always
// records its key. When the current generation reaches its per-generation cap
// it is rotated out early (current → previous, fresh current), so the total is
// bounded at 2 × per-generation cap while insertion never fails. This matters
// for the caller: storeDeliveryReceipt records the receipt locally only when
// the dedup set reports the key as novel, so a key that was silently dropped
// (the earlier refuse-on-full design) would let every later arrival of that
// SAME receipt re-append to s.receipts forever. Always recording guarantees a
// duplicate is caught for as long as its entry survives the eviction window.
//
// Hashing, not exact strings — and honestly NOT a literal exact set: a hash
// collision is the same CLASS of error as a Bloom false positive (a Has can
// suppress a genuine receipt; a Delete removes both colliding keys). The
// difference is magnitude AND adversarial resistance, and the receipt key is
// attacker-controlled (recipient:messageID:status comes off the wire), so the
// hash must resist DELIBERATE collisions, not just random ones:
//   - The Bloom was rejected here for two reasons: at the design load its
//     false-positive rate is material (0.046%–0.32%) AND it cannot Delete a
//     single key (needed by the gossip-failure rollback).
//   - A non-cryptographic hash (FNV, maphash) was rejected too: it is not
//     collision-resistant, so an attacker who controls the key string could
//     CRAFT a second key that collides with a victim's receipt and suppress it
//     — a random-collision probability bound would be a false security claim.
//   - We use the high 128 bits of SHA-256. Random collisions at the capped
//     cardinality are ~2^-129 (negligible), and a DELIBERATE collision against
//     a chosen key requires a second preimage on truncated SHA-256, ~2^128
//     work — infeasible. SHA-256 over a ~90-byte key is microseconds, and
//     receipts are not a high-frequency path, so the cost is irrelevant here.
//
// So the hash is chosen over the raw-string map (unbounded size), the Bloom
// (material FP, no delete), and a fast non-cryptographic hash (forgeable
// collisions); the residual collision risk is cryptographically negligible
// but, to be precise, not exactly zero.
//
// Eviction window: a key Add'd at t answers Has true until at least
// t+rotation and at most t+2*rotation under normal load (time-driven
// rotation); under a flood the per-generation cap rotates sooner, so old
// entries are evicted faster — best-effort recall, never an unbounded set.
//
// Concurrency: a single sync.Mutex serialises every operation, so the type is
// self-contained and unit-testable without a Service. Callers additionally
// hold deliveryMu (the seenReceipts ownership domain, see docs/locking.md) —
// the internal mutex does not change that lock order, it only makes the type
// independently consistent, mirroring rotatingBloomDedup under gossipMu.
package node

import (
	"crypto/sha256"
	"sync"
	"time"
)

// receiptDedupRotation is how often the older generation is discarded and
// replaced by the current one. Combined with the two-generation window this
// gives a dedup persistence of [rotation, 2*rotation] for any single Add under
// normal load. Matched to bloomDedupRotation so transit receipts and the
// messages they acknowledge fall out of dedup on the same horizon.
const receiptDedupRotation = bloomDedupRotation

// maxReceiptDedupEntries caps the retained receipt-dedup cardinality across
// both generations. It is the hard memory ceiling under a flood of distinct
// valid receipt keys: 100k entries × ([16]byte key + map overhead ≈ 64 B) ≈
// 6.5 MiB. Sized far above any legitimate receipt rate within the window
// (rate × 2 × rotation), so honest traffic never reaches it; only an abusive
// peer minting unique keys does, and there the oldest entries are evicted
// rather than memory growing.
const maxReceiptDedupEntries = 100_000

// dedupKey is the fixed-size storage form of a receipt dedup key: the high
// 128 bits of SHA-256 over the raw recipient:messageID:status string. A
// collision-resistant hash is required because the key is attacker-controlled
// (see the file header) — a forgeable hash would let a peer suppress a
// victim's receipt by crafting a colliding key.
type dedupKey [16]byte

func hashDedupKey(key string) dedupKey {
	sum := sha256.Sum256([]byte(key))
	var out dedupKey
	copy(out[:], sum[:16])
	return out
}

// rotatingHashDedup is a hashed key set with a bounded sliding-window eviction
// and a hard cardinality cap enforced by early rotation. See the file header
// for the contract and the collision caveat. Construct via
// newRotatingHashDedup; the zero value is not usable.
type rotatingHashDedup struct {
	mu               sync.Mutex
	current          map[dedupKey]struct{}
	previous         map[dedupKey]struct{}
	nextRotation     time.Time
	rotationInterval time.Duration
	maxPerGen        int // 0 = unbounded; current is rotated out when it hits this
	clock            func() time.Time
}

// newRotatingHashDedup constructs a rotating hashed dedup with the given
// rotation interval and hard cardinality cap (maxEntries <= 0 disables the
// cap). The cap is split across the two generations, so the live set stays at
// or below maxEntries. clock is the time source — pass nil for the production
// wall clock; tests inject a controllable clock to step time deterministically.
func newRotatingHashDedup(rotation time.Duration, maxEntries int, clock func() time.Time) *rotatingHashDedup {
	if clock == nil {
		clock = time.Now
	}
	maxPerGen := 0
	if maxEntries > 0 {
		maxPerGen = maxEntries / 2
		if maxPerGen < 1 {
			maxPerGen = 1
		}
	}
	now := clock()
	return &rotatingHashDedup{
		current:          make(map[dedupKey]struct{}),
		previous:         make(map[dedupKey]struct{}),
		nextRotation:     now.Add(rotation),
		rotationInterval: rotation,
		maxPerGen:        maxPerGen,
		clock:            clock,
	}
}

// Has reports whether key is in the current or previous generation. Exact at
// the hash level; collisions are cryptographically negligible (see header).
// False negatives are bounded by the rotation/eviction window.
func (r *rotatingHashDedup) Has(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	return r.hasLocked(hashDedupKey(key))
}

// Add records key in the current generation. It always records; the
// cardinality bound is maintained by evicting the older generation via early
// rotation, never by refusing the insert.
func (r *rotatingHashDedup) Add(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	r.addLocked(hashDedupKey(key))
}

// MarkIfAbsent records key and reports whether it was ALREADY present (in
// either generation) before this call. A true return means the caller is
// observing a duplicate and should drop it; a false return means this call is
// the first sighting — and the key IS now recorded (even at capacity), so the
// caller can safely act on the novel receipt knowing a later duplicate will be
// caught while the entry survives the eviction window.
func (r *rotatingHashDedup) MarkIfAbsent(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	h := hashDedupKey(key)
	if r.hasLocked(h) {
		return true
	}
	r.addLocked(h)
	return false
}

// Delete removes key from BOTH generations, restoring first-sighting state.
// Used by the gossip-total-failure rollback (unmarkTransitReceiptSeen). On the
// astronomically unlikely event of a hash collision this also clears the
// colliding key — an acceptable risk at the ~2^-129 collision probability.
func (r *rotatingHashDedup) Delete(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	h := hashDedupKey(key)
	delete(r.current, h)
	delete(r.previous, h)
}

// Len reports the number of distinct keys currently retained across both
// generations. Diagnostic only; not on any hot path.
func (r *rotatingHashDedup) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rotateIfDueLocked()
	n := len(r.current)
	for k := range r.previous {
		if _, dup := r.current[k]; !dup {
			n++
		}
	}
	return n
}

func (r *rotatingHashDedup) hasLocked(h dedupKey) bool {
	if _, ok := r.current[h]; ok {
		return true
	}
	_, ok := r.previous[h]
	return ok
}

// addLocked inserts h into the current generation and, when that generation
// reaches the per-generation cap, rotates early so the next insert lands in a
// fresh generation. Insertion always succeeds; the cap is held by eviction of
// the older generation, not by dropping the new key.
func (r *rotatingHashDedup) addLocked(h dedupKey) {
	r.current[h] = struct{}{}
	if r.maxPerGen > 0 && len(r.current) >= r.maxPerGen {
		r.forceRotateLocked()
	}
}

// forceRotateLocked promotes the current generation to previous and starts a
// fresh current, resetting the time deadline. Used both by the time-driven
// rotation and by the cardinality-cap eviction.
func (r *rotatingHashDedup) forceRotateLocked() {
	r.previous = r.current
	r.current = make(map[dedupKey]struct{})
	r.nextRotation = r.clock().Add(r.rotationInterval)
}

// rotateIfDueLocked discards the previous generation and promotes the current
// one when the rotation deadline has passed. A clock jump of at least one full
// interval past the deadline resets BOTH generations — keeping a stale
// previous from N intervals ago would extend dedup persistence beyond the
// documented bound. Mirrors rotatingBloomDedup.rotateIfDueLocked.
func (r *rotatingHashDedup) rotateIfDueLocked() {
	now := r.clock()
	if now.Before(r.nextRotation) {
		return
	}
	if now.Sub(r.nextRotation) >= r.rotationInterval {
		r.previous = make(map[dedupKey]struct{})
		r.current = make(map[dedupKey]struct{})
	} else {
		r.previous = r.current
		r.current = make(map[dedupKey]struct{})
	}
	r.nextRotation = now.Add(r.rotationInterval)
}
