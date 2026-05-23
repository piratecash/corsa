package routing

import "time"

// This file hosts the MaxNextHopsPerOrigin admission policy. After
// Phase 1 P0 Phase A the unit of counting is per-(Identity, Uplink)
// claim rather than per-(Identity, Origin) entry, but the
// operator-facing knob name (maxNextHopsPerOrigin) is preserved so
// the env var CORSA_MAX_NEXT_HOPS_PER_ORIGIN keeps working
// unchanged. The rename + deprecation-warn surface is deferred to
// the Phase 6 cleanup window — both the env var and the
// observability JSON key `cap_admission` are stable knobs until
// then.
//
// Cap admission is fundamentally a storage-shape operation: it
// scans the bucket of a single Identity, picks an evictable
// candidate by trust → hops → expiry order, and decides whether
// the incoming claim is strictly better than the worst candidate.
// Keeping the policy inside route_store_*.go is what allowed Phase
// A to be a local diff: Origin filter dropped, dedup key narrowed
// to (Identity, Uplink), claim slice element type changed —
// without touching Table-level callers.

// AdmitNew decides whether a freshly arrived UplinkClaim — whose
// (Identity, Uplink) pair does not yet exist in the store — can
// be inserted under maxNextHopsPerOrigin. The function performs
// the insertion (or eviction-then-insertion) itself when the
// admission policy permits and returns the resulting decision;
// the caller must NOT touch s.buckets[identity] in either branch.
//
// Admission rules when s.maxNextHopsPerOrigin > 0 (cap active):
//
//  1. Withdrawn claims are EXCLUDED from the cap counter and from
//     the eviction-search entirely. They are metadata markers
//     (the SeqNo resurrection guard for a specific
//     (Identity, Uplink) pair) and live alongside the K-live
//     slots until TickTTL reclaims them; they neither compete
//     with live claims for cap slots nor get displaced to make
//     room for live admissions. See WithdrawTriple idx<0 and the
//     convergence section in docs/routing.md.
//  2. If the Identity bucket has fewer than maxNextHopsPerOrigin
//     LIVE (non-withdrawn) claims, append unconditionally →
//     AdmissionAccepted.
//  3. Otherwise pick the worst evictable existing claim,
//     considering only live (non-withdrawn) claims. The eviction
//     key is (live > dead) → lowest TrustRank → highest Hops →
//     earliest ExpiresAt, evaluated in this order. Liveness as
//     the FIRST tier matters for expired-but-not-withdrawn
//     claims: a claim whose ExpiresAt is in the past is "worse"
//     than any fully-live claim regardless of trust — this
//     prevents a bucket of K stale hop_acks from blocking a
//     fresh announcement. Withdrawn claims are NOT in this
//     comparison (see rule 1). Direct (RouteSourceDirect)
//     entries are NEVER evictable: a direct route represents a
//     live socket and can only retire through the
//     RemoveDirectPeer path. (RouteSourceLocal is symmetric but
//     never persisted in the store — see UplinkClaim.Validate.)
//  4. If every live claim in the bucket is direct (protected),
//     the cap cannot evict anyone → AdmissionRejectedAllProtected,
//     drop the new claim.
//  5. Otherwise compare the incoming claim against the worst
//     evictable candidate using the strict-better FLOOR key
//     (live > dead → TrustRank → Hops). The floor is
//     intentionally narrower than the eviction key in step 3:
//     ExpiresAt participates in step 3 (picking which bucket
//     member to evict, where dropping the row closest to expiry
//     is cheapest) but NOT in step 5 (deciding whether to evict
//     at all). Every re-announce arrives with ExpiresAt =
//     now+defaultTTL, strictly later than any existing claim's,
//     so an ExpiresAt-inclusive floor would let any equal-trust
//     equal-hops re-announce displace a stable incumbent on each
//     announce cycle and thrash the best-K winners. The floor
//     therefore requires a real improvement (live > expired,
//     higher trust, or strictly fewer hops); equal-on-all-three
//     means incumbent wins. If the incoming claim is strictly
//     better, replace the worst entry's slot in place →
//     AdmissionAcceptedReplaced. Otherwise drop →
//     AdmissionRejectedFull. See isStrictlyBetterForCapLocked
//     for the asymmetry rationale.
//
// When the cap is disabled (maxNextHopsPerOrigin <= 0) the function
// always appends and returns AdmissionAccepted, which is what
// makes the cap rollout backward-compatible: a store constructed
// without WithMaxNextHopsPerOrigin behaves exactly as it did
// before the cap was added.
//
// Side effects: on Accepted / AcceptedReplaced the function
// writes s.buckets[identity] (append or in-place overwrite). It
// does NOT touch the dirty flag or the SeqNo counter —
// ApplyUpdate does that itself once the decision returns. The
// function must be called with t.mu held (writer).
//
// `now` is the wall-clock used for liveness comparisons. Withdrawn
// rows are excluded from bucketCount and from the eviction-search
// entirely (rule 1) and never reach isWorseForEvictionLocked; the
// liveness pre-tier inside that helper distinguishes only
// fully-live rows from expired-but-not-withdrawn rows, where the
// latter lose to any live candidate regardless of trust. Pass the
// same `now` value ApplyUpdate uses to fill ExpiresAt, so the
// admission decision and the new claim's TTL agree on the same
// clock reading.
func (s *routeStore) AdmitNew(identity PeerIdentity, claim UplinkClaim, now time.Time) RouteAdmissionDecision {
	bucket := s.buckets[identity]

	if s.maxNextHopsPerOrigin <= 0 {
		// Cap disabled: don't touch the counters. Operators of an
		// uncapped node observe RouteCapStats stuck at zero,
		// which is the correct signal — there is no admission
		// decision to account for.
		s.buckets[identity] = append(bucket, claim)
		return AdmissionAccepted
	}

	// Count NON-TOMBSTONE claims in the Identity bucket. Withdrawn
	// claims are intentionally excluded from the cap counter so
	// tombstones live "outside" the K-counted slots: their job is
	// the SeqNo-based resurrection guard for a specific
	// (Identity, Uplink) pair — a delayed lower-SeqNo announcement
	// for the same pair hits the idx>=0 path in ApplyUpdate and
	// is rejected by the existing tombstone-vs-stale-SeqNo check.
	// If tombstones competed with live claims for the K slots, a
	// bucket already at the cap would either drop the tombstone
	// (losing the SeqNo guard, so a stale announcement could
	// resurrect the lineage through cap admission alone) or evict
	// a live claim to make room (losing real routing capacity for
	// a metadata-only marker). Excluding tombstones from the
	// counter removes the trade-off: the cap keeps "at most K
	// live claims per Identity" intact, and tombstones are bound
	// only by their TTL (TickTTL reclaims them after defaultTTL).
	//
	// Expired-but-not-withdrawn claims ARE counted: they are not
	// metadata markers, they are live-shaped entries that the
	// store simply has not cleaned up yet, and the liveness
	// pre-tier in isWorseForEvictionLocked makes them the first
	// eviction target when a new live claim arrives.
	bucketCount := 0
	for i := range bucket {
		if bucket[i].IsWithdrawn() {
			continue
		}
		bucketCount++
	}
	if bucketCount < s.maxNextHopsPerOrigin {
		s.buckets[identity] = append(bucket, claim)
		s.capStats.accepted.Add(1)
		return AdmissionAccepted
	}

	// Bucket saturated by non-tombstone claims. Find the worst
	// evictable candidate. Tombstones are skipped here too — they
	// are "free" and never displaced to make room for a live
	// admission, mirroring the bucket-count rule above. Evicting
	// a tombstone for a live row would silently drop the SeqNo
	// resurrection guard for that (Identity, Uplink) pair.
	//
	// Liveness pre-tier still applies to non-tombstone claims: an
	// expired-but-not-withdrawn row loses to any fully-live row,
	// regardless of trust. See isWorseForEvictionLocked.
	worstIdx := -1
	for i := range bucket {
		if bucket[i].IsWithdrawn() {
			continue
		}
		if !isEvictable(&bucket[i]) {
			continue
		}
		if worstIdx < 0 || isWorseForEvictionLocked(&bucket[i], &bucket[worstIdx], now) {
			worstIdx = i
		}
	}
	if worstIdx < 0 {
		// Every live claim in the bucket is direct — protected
		// from cap eviction. The incoming claim is dropped without
		// changing the store.
		s.capStats.rejectedAllProtected.Add(1)
		return AdmissionRejectedAllProtected
	}

	// Apply the cap floor: the incoming claim must be strictly
	// better than the worst evictable candidate, or it gets
	// dropped. "Strictly better" means the same key as the
	// eviction order, just inverted: live beats dead first, then
	// higher trust, then strictly fewer hops.
	if !isStrictlyBetterForCapLocked(&claim, &bucket[worstIdx], now) {
		s.capStats.rejectedFull.Add(1)
		return AdmissionRejectedFull
	}

	bucket[worstIdx] = claim
	s.buckets[identity] = bucket
	s.capStats.acceptedReplaced.Add(1)
	return AdmissionAcceptedReplaced
}

// AdmitDirect admits a freshly-arrived direct UplinkClaim into
// the Identity bucket and unlike AdmitNew is allowed to displace
// the worst evictable row to make room — but is NEVER allowed to
// reject. A direct route models a session the OS already holds
// open: refusing to record it because the bucket is saturated
// would silently desync the store from the transport layer, and
// the very next sessionSend(peer, …) would disagree with what
// the routing table claims is reachable.
//
// Algorithm (with cap active):
//
//  1. Withdrawn claims are not counted (rule 1 of AdmitNew
//     applies symmetrically here): tombstones live outside the
//     K-counted slots, so a bucket with K live rows plus any
//     number of tombstones still has room → no eviction.
//  2. If the Identity bucket has fewer than maxNextHopsPerOrigin
//     LIVE claims, append unconditionally; no cap counter is
//     touched (the AdmissionAccepted shape is reserved for
//     AdmitNew, not direct admission, so an operator reading
//     CapStats does not misread direct registers as ordinary
//     admission churn).
//  3. Otherwise pick the worst evictable LIVE claim using the
//     same eviction key as AdmitNew ((live > expired) → trust →
//     hops → expiry) and replace it in-place. Counter
//     `acceptedReplaced` is incremented because operationally
//     this IS a cap-driven displacement: a direct registration
//     on a saturated bucket evicted a row that announce/hop_ack
//     ingestion would also have evicted. The `Accepted` counter
//     is deliberately NOT bumped on the below-K branch above —
//     Accepted is reserved for AdmitNew so the eviction-rate
//     metric `AcceptedReplaced / (Accepted + AcceptedReplaced)`
//     continues to measure cap pressure on announce/hop_ack
//     traffic without direct-registration noise diluting the
//     denominator.
//  4. Defensive fallback: if every live claim in the bucket is
//     direct (the AllProtected shape), the new claim is still
//     appended without bumping any counter, leaving the bucket
//     transiently at K+1 live. This branch is unreachable
//     through the public AddDirectPeer API in production — a
//     direct claim has Uplink == identity, so a second
//     direct-from-the-same-identity claim is impossible by the
//     (Identity, Uplink) dedup key (the idempotent fast path in
//     AdmitDirectPeer intercepts it). The fallback is kept as a
//     guard for synthetic test fixtures and any future caller
//     that constructs a bucket through a non-public mutation
//     path; in production CapStats stays a clean signal.
//
// When the cap is disabled (maxNextHopsPerOrigin <= 0) the
// function degenerates to an unconditional append, which matches
// the pre-cap behaviour of AddDirectPeer.
//
// Caller contract: t.mu held (writer); claim.Uplink == identity
// (direct route invariant); claim.Source = Direct; the
// (Identity, Uplink=identity) pair is NOT already present as a
// live row (the idempotent fast path in AdmitDirectPeer filters
// that case) — though it MAY be present as a tombstone, in
// which case the caller has already detached the tombstone slot
// before calling.
func (s *routeStore) AdmitDirect(identity PeerIdentity, claim UplinkClaim, now time.Time) {
	bucket := s.buckets[identity]

	if s.maxNextHopsPerOrigin <= 0 {
		s.buckets[identity] = append(bucket, claim)
		return
	}

	bucketCount := 0
	for i := range bucket {
		if bucket[i].IsWithdrawn() {
			continue
		}
		bucketCount++
	}
	if bucketCount < s.maxNextHopsPerOrigin {
		s.buckets[identity] = append(bucket, claim)
		return
	}

	worstIdx := -1
	for i := range bucket {
		if bucket[i].IsWithdrawn() {
			continue
		}
		if !isEvictable(&bucket[i]) {
			continue
		}
		if worstIdx < 0 || isWorseForEvictionLocked(&bucket[i], &bucket[worstIdx], now) {
			worstIdx = i
		}
	}
	if worstIdx < 0 {
		// Defensive fallback — see step 4 in the doc-comment for
		// why this is unreachable through AddDirectPeer in
		// production.
		s.buckets[identity] = append(bucket, claim)
		return
	}

	bucket[worstIdx] = claim
	s.buckets[identity] = bucket
	s.capStats.acceptedReplaced.Add(1)
}

// isEvictable reports whether c can be displaced by a cap
// admission decision. Direct routes are protected: their
// lifecycle is managed by RemoveDirectPeer and the cap must
// never silently retire one — that would break a live socket
// representation.
//
// (RouteSourceLocal is symmetric but never reaches storage —
// UplinkClaim.Validate rejects it. The filter is kept for
// defensive symmetry with the pre-Phase-A code.)
func isEvictable(c *UplinkClaim) bool {
	return c.Source != RouteSourceDirect && c.Source != RouteSourceLocal
}

// isLiveForEviction reports whether c counts as "live" for the
// cap admission ordering: not withdrawn AND not wall-clock
// expired against `now`. In practice the cap path (AdmitNew rule
// 0) excludes withdrawn claims from bucketCount and from the
// eviction-search before this comparison ever runs, so the
// predicate effectively distinguishes live from
// expired-but-not-withdrawn claims — and that "expired" arm is
// the FIRST tier of the eviction key, dropping a stale hop_ack
// ahead of a fresh announcement regardless of trust. The
// IsWithdrawn check is kept defensively (AdmitNew rule 0 is the
// only place that relies on this predicate, and a future caller
// that forgets to pre-filter withdrawn rows still gets the
// conservative answer). See isWorseForEvictionLocked.
//
// Held outside an exported helper because the cap admission
// policy is the only consumer; other callers use IsWithdrawn /
// IsExpired directly with their own semantics.
func isLiveForEviction(c *UplinkClaim, now time.Time) bool {
	return !c.IsWithdrawn() && !c.IsExpired(now)
}

// isWorseForEvictionLocked reports whether `a` is a worse
// candidate to keep than `b` under the cap eviction order: live
// beats dead → lowest TrustRank → highest Hops → earliest
// ExpiresAt. The eviction picks whichever claim answers true to
// this against every other evictable candidate, so the key has to
// be total (no ties on the same (Identity, Uplink) — and across
// pairs the lexicographic key is total as well).
//
// "Earliest ExpiresAt wins eviction" feels backward at first
// glance, but it is the right tie-break: a claim that is closer
// to its TTL expiry will be replaced by TickTTL soon anyway, so
// dropping it now to make room for a fresher candidate is
// strictly cheaper than dropping the slot that would have lived
// longer.
//
// Must be called with t.mu held — the comparison itself is pure,
// but the helper is named *Locked because both arguments come
// from inside s.buckets which the caller is reading under the
// same lock.
func isWorseForEvictionLocked(a, b *UplinkClaim, now time.Time) bool {
	// Liveness tier: an expired-but-not-withdrawn row is worse
	// than any fully-live candidate, regardless of trust. This is
	// what keeps a bucket of K stale hop_acks from blocking a
	// fresh announcement: under trust-only ordering the expired
	// hop_acks would dominate (RouteSourceHopAck.TrustRank() = 1
	// > RouteSourceAnnouncement.TrustRank() = 0) and the
	// announcement would be rejected even though Lookup would
	// return zero usable routes for the bucket. Withdrawn rows
	// do not appear here at all — AdmitNew rule 0 excludes them
	// from both bucketCount and the eviction-search, so the cap
	// never trades a tombstone against a live candidate;
	// tombstones live alongside the K cap-counted slots and are
	// reclaimed by TickTTL after defaultTTL.
	aLive := isLiveForEviction(a, now)
	bLive := isLiveForEviction(b, now)
	if aLive != bLive {
		return !aLive
	}
	// Within the same liveness tier the original ranking applies.
	if a.Source.TrustRank() != b.Source.TrustRank() {
		return a.Source.TrustRank() < b.Source.TrustRank()
	}
	if a.Hops != b.Hops {
		return a.Hops > b.Hops
	}
	return a.ExpiresAt.Before(b.ExpiresAt)
}

// isStrictlyBetterForCapLocked is the cap admission "floor":
// returns true when `cand` is strictly better than `worst` and
// therefore should displace it. Sibling of
// isWorseForEvictionLocked, but with an INTENTIONALLY narrower
// key — the asymmetry is the cap stability invariant.
//
// The two helpers answer different questions:
//
//   - isWorseForEvictionLocked picks WHICH bucket member to evict
//     when admission has already decided that a new row will land.
//     There it is rational to break ties by ExpiresAt: among rows
//     of equal trust+hops, the one closest to expiry will be
//     reclaimed by TickTTL soon anyway, so dropping it now is the
//     cheapest choice.
//   - isStrictlyBetterForCapLocked decides WHETHER to evict at
//     all. Here ExpiresAt is the wrong tiebreaker: every announce
//     arrives with `ExpiresAt = now + defaultTTL`, which is by
//     construction strictly later than any existing claim's
//     ExpiresAt. Including ExpiresAt in the floor would let any
//     equal-trust equal-hops re-announce displace a stable
//     incumbent on each cycle, thrashing the best-K winners and
//     producing 90%+ eviction-rate in dense meshes (the "сеть
//     сходит с ума" symptom). The floor therefore requires a
//     real improvement — live > expired, higher trust, or
//     strictly fewer hops — and equal-on-all-tiers means
//     incumbent wins.
//
// The relation is irreflexive (no claim is strictly better than
// itself) and matches the eviction order on its first three
// tiers, so feeding back the same pointer twice returns false —
// the cap "floor" semantics: a worse-or-equal incoming claim is
// dropped, never admitted.
//
// Must be called with t.mu held for the same reason as
// isWorseForEvictionLocked.
func isStrictlyBetterForCapLocked(cand, worst *UplinkClaim, now time.Time) bool {
	// Liveness tier mirrors isWorseForEvictionLocked: a live
	// candidate is strictly better than a dead worst, and a dead
	// candidate is not strictly better than a live worst — so the
	// cap admits a fresh live claim that displaces a dead row,
	// but never admits a new dead claim into a bucket that still
	// has live routes.
	candLive := isLiveForEviction(cand, now)
	worstLive := isLiveForEviction(worst, now)
	if candLive != worstLive {
		return candLive
	}
	if cand.Source.TrustRank() != worst.Source.TrustRank() {
		return cand.Source.TrustRank() > worst.Source.TrustRank()
	}
	if cand.Hops != worst.Hops {
		return cand.Hops < worst.Hops
	}
	// Equal liveness + trust + hops: incumbent wins. ExpiresAt is
	// deliberately NOT a tiebreaker here — see the doc-comment
	// above for why. Real improvements are already covered by the
	// live / trust / hops tiers; everything else is bucket churn.
	return false
}
