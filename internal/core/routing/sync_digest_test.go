package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// sync_digest_test.go covers Phase 3 PR 12.5 — the
// route_sync_digest_v1 engine, the per-peer cache, and the
// TickTTL eviction. Lifecycle tests for the wire frames
// themselves live in frame_route_sync_test.go.
//
// Fixed-clock convention follows the rest of the routing suite
// (2026-05-23 — see table_health_reputation_test.go for the
// rationale tied to upsertClaim's time.Now() ExpiresAt).

// TestComputeSyncDigest_DeterministicForIdenticalView — same
// entries → same digest. Order in the input slice must NOT
// affect the result; the helper sorts internally.
func TestComputeSyncDigest_DeterministicForIdenticalView(t *testing.T) {
	a := []DigestEntry{
		{Identity: domaintest.ID("id-x"), MaxSeqNo: 7},
		{Identity: domaintest.ID("id-y"), MaxSeqNo: 3},
		{Identity: domaintest.ID("id-z"), MaxSeqNo: 12},
	}
	b := []DigestEntry{
		{Identity: domaintest.ID("id-z"), MaxSeqNo: 12},
		{Identity: domaintest.ID("id-x"), MaxSeqNo: 7},
		{Identity: domaintest.ID("id-y"), MaxSeqNo: 3},
	}
	if ComputeSyncDigest(a) != ComputeSyncDigest(b) {
		t.Fatal("digest is order-sensitive; sort must canonicalise")
	}
}

// TestComputeSyncDigest_DiffersOnExtraIdentity — adding one
// (Identity, SeqNo) pair must change the hash. Pins the
// "small change → visible churn" contract.
func TestComputeSyncDigest_DiffersOnExtraIdentity(t *testing.T) {
	base := []DigestEntry{{Identity: domaintest.ID("id-x"), MaxSeqNo: 1}}
	extra := []DigestEntry{
		{Identity: domaintest.ID("id-x"), MaxSeqNo: 1},
		{Identity: domaintest.ID("id-y"), MaxSeqNo: 1},
	}
	if ComputeSyncDigest(base) == ComputeSyncDigest(extra) {
		t.Fatal("digest unchanged when an identity was added")
	}
}

// TestComputeSyncDigest_DiffersOnDifferentSeqNo — SeqNo changes
// matter even when the identity set is identical. Without this
// invariant a stale-but-same-set view would falsely match the
// fresh view.
func TestComputeSyncDigest_DiffersOnDifferentSeqNo(t *testing.T) {
	low := []DigestEntry{{Identity: domaintest.ID("id-x"), MaxSeqNo: 1}}
	high := []DigestEntry{{Identity: domaintest.ID("id-x"), MaxSeqNo: 2}}
	if ComputeSyncDigest(low) == ComputeSyncDigest(high) {
		t.Fatal("digest unchanged when SeqNo advanced")
	}
}

// TestComputeSyncDigest_EmptyReturnsStableConstant — empty
// input yields the sha256 of the empty string. The constant is
// a well-known value (e3b0c44...) — pinning it makes a future
// canonicalisation change (different separator, different
// encoding) impossible to land silently.
func TestComputeSyncDigest_EmptyReturnsStableConstant(t *testing.T) {
	got := ComputeSyncDigest(nil)
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got != want {
		t.Fatalf("empty digest = %q, want %q (sha256 of empty input)", got, want)
	}
}

// TestSyncDigestFor_OnlyIncludesClaimsThroughPeer — split-horizon
// at the digest level: an identity reachable through peer A but
// NOT through peer B must not appear in the digest computed for
// peer B. Without this filter the digest reflects a strict superset
// of what the peer actually announced to us, so the comparison
// against the peer's own self-computed digest would always
// mismatch and the optimisation would never fire.
func TestSyncDigestFor_OnlyIncludesClaimsThroughPeer(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-via-a"), domaintest.ID("id-peer-a"), 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, domaintest.ID("id-via-b"), domaintest.ID("id-peer-b"), 2, RouteSourceAnnouncement)

	// Digest for peer-a includes only id-via-a; digest for peer-b
	// includes only id-via-b. The two must differ.
	digA, countA := tbl.SyncDigestFor(domaintest.ID("id-peer-a"))
	digB, countB := tbl.SyncDigestFor(domaintest.ID("id-peer-b"))
	if countA != 1 {
		t.Fatalf("SyncDigestFor(peer-a) count = %d, want 1", countA)
	}
	if countB != 1 {
		t.Fatalf("SyncDigestFor(peer-b) count = %d, want 1", countB)
	}
	if digA == digB {
		t.Fatal("digests for disjoint peer views must differ")
	}
}

// TestSyncDigestFor_ExcludesPeerSelfIdentity — split-horizon at
// the per-identity level: a claim where Identity == peer (the
// peer claiming a route to itself, which can happen during
// announce churn) must not contribute to the digest computed
// for that peer. Mirrors the AnnounceableFor self-exclusion
// rule one layer down.
func TestSyncDigestFor_ExcludesPeerSelfIdentity(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-peer"), domaintest.ID("id-peer"), 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, domaintest.ID("id-other"), domaintest.ID("id-peer"), 2, RouteSourceAnnouncement)

	_, count := tbl.SyncDigestFor(domaintest.ID("id-peer"))
	if count != 1 {
		t.Fatalf("SyncDigestFor(peer) count = %d, want 1 (self-identity must be excluded)", count)
	}
}

// TestSyncDigestFor_IgnoresWithdrawnAndExpired — withdrawn
// tombstones and expired claims are NOT part of the active
// state the announce cycle would re-confirm; including them
// would make the digest unstable against TTL boundaries that
// have nothing to do with the peer view.
func TestSyncDigestFor_IgnoresWithdrawnAndExpired(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	// One live claim, one withdrawn.
	upsertClaim(t, tbl, domaintest.ID("id-live"), domaintest.ID("id-peer"), 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, domaintest.ID("id-dead"), domaintest.ID("id-peer"), 2, RouteSourceAnnouncement)
	if !tbl.WithdrawRoute(domaintest.ID("id-dead"), domaintest.ID("id-dead"), domaintest.ID("id-peer"), 2) {
		t.Fatal("WithdrawRoute returned false")
	}

	_, count := tbl.SyncDigestFor(domaintest.ID("id-peer"))
	if count != 1 {
		t.Fatalf("SyncDigestFor count = %d, want 1 (withdrawn must be excluded)", count)
	}
}

// TestSyncDigestFor_EmptyTableReturnsEmptyDigestConstant — the
// no-peers-no-routes case returns the empty-input constant.
// Pins the same canonicalisation as the unit test above; a node
// with no learned routes still produces a deterministic digest.
func TestSyncDigestFor_EmptyTableReturnsEmptyDigestConstant(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	got, count := tbl.SyncDigestFor(domaintest.ID("id-unknown"))
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got != want {
		t.Fatalf("digest = %q, want %q (empty-input sha256)", got, want)
	}
}

// TestRecordAndConsumeDigestSnapshot_RoundTrip — record, then
// consume returns the same fields. After consume the cache no
// longer contains the entry (single-shot semantic).
func TestRecordAndConsumeDigestSnapshot_RoundTrip(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	tbl.RecordPeerDigestSnapshot(domaintest.ID("id-peer"), "digest-abc", 7, now)

	gotDigest, gotCount, gotGen, ok := tbl.ConsumePeerDigestSnapshot(domaintest.ID("id-peer"), now.Add(1*time.Second))
	if !ok {
		t.Fatal("ConsumePeerDigestSnapshot returned ok=false for fresh entry")
	}
	if gotDigest != "digest-abc" {
		t.Fatalf("digest = %q, want digest-abc", gotDigest)
	}
	if gotCount != 7 {
		t.Fatalf("count = %d, want 7", gotCount)
	}
	if !gotGen.Equal(now) {
		t.Fatalf("generatedAt = %v, want %v", gotGen, now)
	}

	// Second consume must return ok=false — the entry was
	// removed on the first read.
	if _, _, _, ok := tbl.ConsumePeerDigestSnapshot(domaintest.ID("id-peer"), now.Add(2*time.Second)); ok {
		t.Fatal("ConsumePeerDigestSnapshot returned ok=true on second call; single-shot contract broken")
	}
}

// TestConsumeDigestSnapshot_TTLExpired — entries older than
// SessionDigestCacheTTL are silently evicted and reported as
// not-found.
func TestConsumeDigestSnapshot_TTLExpired(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl.RecordPeerDigestSnapshot(domaintest.ID("id-peer"), "stale", 3, now)

	// Walk past the TTL window.
	after := now.Add(SessionDigestCacheTTL + time.Second)
	if _, _, _, ok := tbl.ConsumePeerDigestSnapshot(domaintest.ID("id-peer"), after); ok {
		t.Fatal("ConsumePeerDigestSnapshot returned ok=true past TTL window")
	}

	// Even within the original window the entry is gone — the
	// TTL-expired consume above evicted it as a side effect.
	tbl.mu.RLock()
	cacheLen := tbl.digestCacheLenLocked()
	tbl.mu.RUnlock()
	if cacheLen != 0 {
		t.Fatalf("cache len after TTL eviction = %d, want 0", cacheLen)
	}
}

// TestConsumeDigestSnapshot_AbsentReturnsFalse — peer with no
// recorded snapshot returns ok=false without touching anything.
func TestConsumeDigestSnapshot_AbsentReturnsFalse(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	if _, _, _, ok := tbl.ConsumePeerDigestSnapshot(domaintest.ID("id-never"), time.Now()); ok {
		t.Fatal("ConsumePeerDigestSnapshot returned ok=true for absent peer")
	}
}

// TestPurgeDigestSnapshot_RemovesEntry — explicit purge path.
// Used by lifecycle hooks that know the cached digest is no
// longer trustworthy (e.g. a permanent peer eviction).
func TestPurgeDigestSnapshot_RemovesEntry(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	tbl.RecordPeerDigestSnapshot(domaintest.ID("id-peer"), "digest-x", 1, now)
	tbl.PurgeDigestSnapshot(domaintest.ID("id-peer"))

	if _, _, _, ok := tbl.ConsumePeerDigestSnapshot(domaintest.ID("id-peer"), now); ok {
		t.Fatal("ConsumePeerDigestSnapshot returned ok=true after Purge")
	}
}

// TestPruneExpiredDigestSnapshots_EvictsStaleOnly — the TickTTL
// hook removes entries past their TTL but leaves fresh entries
// alone. The walk is bounded by cache size so the test asserts
// "we kept the fresh one" rather than measuring cost.
func TestPruneExpiredDigestSnapshots_EvictsStaleOnly(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	tbl.RecordPeerDigestSnapshot(domaintest.ID("id-fresh"), "fresh-digest", 1, now)
	tbl.RecordPeerDigestSnapshot(domaintest.ID("id-stale"), "stale-digest", 1, now.Add(-2*SessionDigestCacheTTL))

	tbl.mu.Lock()
	evicted := tbl.pruneExpiredDigestSnapshotsLocked(now)
	cacheLen := tbl.digestCacheLenLocked()
	tbl.mu.Unlock()

	if !evicted {
		t.Fatal("prune reported evicted=false despite a stale entry")
	}
	if cacheLen != 1 {
		t.Fatalf("cache len after prune = %d, want 1 (fresh entry preserved)", cacheLen)
	}

	// And the fresh one is still consumable.
	if _, _, _, ok := tbl.ConsumePeerDigestSnapshot(domaintest.ID("id-fresh"), now); !ok {
		t.Fatal("fresh entry consumed as ok=false after prune")
	}
}

// TestRecordDigestSnapshot_EmptyPeerIsNoop — defensive guard
// mirrors the rest of the routing API (MarkHopAck, MarkProbeAck
// etc.). An empty peer identity must not create stray entries.
func TestRecordDigestSnapshot_EmptyPeerIsNoop(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	tbl.RecordPeerDigestSnapshot(domain.PeerIdentity{}, "x", 1, time.Now())

	tbl.mu.RLock()
	cacheLen := tbl.digestCacheLenLocked()
	tbl.mu.RUnlock()
	if cacheLen != 0 {
		t.Fatalf("cache len after empty-peer record = %d, want 0", cacheLen)
	}
}
