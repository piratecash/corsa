package routing

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"time"
)

// sync_digest.go is the Phase 3 PR 12.5 incremental-sync digest
// engine: a deterministic hash over the (Identity, SeqNo) pairs a
// peer would expect to receive in the next forced full sync,
// plus an in-memory per-peer cache that survives a short
// disconnect window so the reconnect path can short-circuit the
// full announce on a digest match.
//
// Architectural anchor:
// docs/cluster-mesh/phase-3-multipath-reputation.md §4.5 (engineering
// item) and §2.6 (digest cache placement decision).
//
// The digest is a LOCAL HINT, not authoritative state. A
// mismatched (or absent) reply is harmless because the normal
// announce cycle remains the source of truth for routing state;
// the digest only governs whether the NEXT forced-full-sync to
// the peer can be elided. See the Phase 3 plan §4.5 trust
// invariant for the full contract.

// SessionDigestCacheTTL bounds how long a per-peer digest snapshot
// survives after the session closes. Five minutes covers transient
// reconnects (NAT rebind, brief network blip, mobile churn) while
// keeping the cache from growing unboundedly across a long
// operator-side outage where the peer view has likely drifted
// anyway. Phase 3 §4.9 decision #5 documents the choice; the
// expired entries are reaped lazily on the next consume / prune
// call rather than by a dedicated ticker.
const SessionDigestCacheTTL = 5 * time.Minute

// DigestEntry is one (Identity, SeqNo) pair that participates in a
// route_sync_digest_v1 hash. MaxSeqNo is the SeqNo of the single
// claim (Identity, peer) — the post-A1 storage model keys per
// (Identity, Uplink) so there is at most one SeqNo per pair, but
// the field name preserves the conceptual "high-water" framing
// used in the Phase 3 plan §4.5 / overview §3.1 so future readers
// see the same vocabulary.
type DigestEntry struct {
	Identity PeerIdentity
	MaxSeqNo uint64
}

// ComputeSyncDigest returns the hex-encoded sha256 of a
// canonicalised representation of the entries. The canonical form
// is:
//
//	for each entry, sorted by Identity ascending:
//	    write Identity bytes
//	    write ':'
//	    write SeqNo as decimal digits
//	    write '\n'
//
// Choosing sha256 follows the project-wide hash convention (used
// for message IDs elsewhere). The cost is sub-millisecond for the
// densely-connected 100-node mesh (~100 entries × ~50 bytes), so
// no faster hash (xxhash, blake3) is justified — Phase 3 §4.9
// decision #9 documents the choice.
//
// Empty input produces the sha256 of the empty string, a stable
// constant. The pure-function contract (no clock, no I/O, no
// state mutation) lets callers run it lock-free outside critical
// sections; the caller's lock contract only covers the data
// gathering that feeds entries in.
func ComputeSyncDigest(entries []DigestEntry) string {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Identity < entries[j].Identity
	})
	h := sha256.New()
	for _, e := range entries {
		h.Write([]byte(string(e.Identity)))
		h.Write([]byte{':'})
		h.Write([]byte(strconv.FormatUint(e.MaxSeqNo, 10)))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sessionDigestEntry caches one peer's last-observed digest so the
// reconnect-within-TTL path can emit it without re-walking the
// table. Owned by routing.Table.sessionDigestCache under t.mu.
type sessionDigestEntry struct {
	// Digest is the hex sha256 from ComputeSyncDigest at the time
	// the snapshot was recorded.
	Digest string

	// EntryCount is the number of identities folded into the
	// digest. Carried alongside the digest itself so the
	// route_sync_digest_v1 frame can populate
	// KnownIdentitiesCount as a diagnostic hint without
	// re-walking.
	EntryCount uint32

	// GeneratedAt is the wall-clock time at which the digest was
	// computed — emitted on the wire as RouteSyncDigestFrame.
	// GeneratedAt so the receiver can log freshness without
	// trusting it for the match decision itself.
	GeneratedAt time.Time

	// IdleSince records when the snapshot last became eligible for
	// TTL eviction. Stamped at RecordPeerDigestSnapshot time;
	// consumed by ConsumePeerDigestSnapshot's TTL check. Kept as
	// a distinct field from GeneratedAt so a future refactor that
	// extends the cache lifecycle (e.g. periodic refresh while
	// the session is still up) does not conflate "digest content
	// timestamp" with "TTL anchor".
	IdleSince time.Time
}

// SyncDigestFor walks the routing table and returns the hex
// sha256 over (Identity, SeqNo) pairs of every live claim
// reachable through the given peer (claim.Uplink == peer). The
// peer's own identity is excluded by the standard split-horizon
// rule: it makes no sense to digest the route "reach yourself
// via yourself".
//
// Returns the digest and the count of identities folded in.
// Withdrawn and expired claims are excluded — they are not part
// of the live routing state the next announce cycle would
// transmit, and including them would make the digest unstable
// against TTL boundaries that have nothing to do with the
// peer's view of us.
//
// Lock contract: acquires t.mu in R mode. The data gathering and
// the hash computation both run under the lock because the hash
// is cheap; releasing before hashing would only matter if the
// pair count grew into the thousands, which the bounded
// per-uplink storage forbids. No I/O, no external callbacks —
// safe to call from any goroutine.
func (t *Table) SyncDigestFor(peer PeerIdentity) (string, uint32) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := t.clock()
	var entries []DigestEntry
	for identity, bucket := range t.store.buckets {
		if identity == peer {
			// Split-horizon: a "route to the peer via itself" is
			// degenerate and never travels on the wire either.
			continue
		}
		for i := range bucket {
			claim := &bucket[i]
			if claim.Uplink != peer {
				continue
			}
			if claim.IsWithdrawn() || claim.IsExpired(now) {
				// Skip both — they are not part of the
				// active state the announce cycle would
				// re-confirm. Withdrawn claims are
				// tombstones that exist only for the
				// SeqNo-resurrection guard; expired claims
				// are aging out and the next TickTTL pass
				// will reap them.
				break
			}
			entries = append(entries, DigestEntry{
				Identity: identity,
				MaxSeqNo: claim.SeqNo,
			})
			// Per-(Identity, Uplink) storage guarantees at most
			// one matching claim per identity; bail early once
			// we have it.
			break
		}
	}
	return ComputeSyncDigest(entries), uint32(len(entries))
}

// AnnounceDigestFor returns the hex sha256 over the (Identity, wireSeqNo)
// pairs THIS node has announced to `peer` and would still announce — the
// sender-side digest of the reconnect optimisation. It is the correct
// counterpart to a receiver's SyncDigestFor(thisNode): the receiver
// stores, per (Identity, via=thisNode), the per-peer wire SeqNo we sent,
// and AnnounceDigestFor reproduces exactly those values from
// outboundPeerMax — so when the two nodes are in sync the digests match.
//
// This replaces the pre-fix use of SyncDigestFor(peer) on the sender
// side, which hashed the routes WE learned through the peer (Uplink ==
// peer). That set is disjoint from what the peer stores as its
// via-(this node) view, so the digests almost never matched in a normal
// topology (A learns C via B ⇒ B must offer A the digest of what B
// announces to A, which includes C, not the routes B knows through A).
// See route_store_lookup.go::announceDigestEntriesForLocked.
//
// Returns the digest and the count of identities folded in. Lock
// contract: acquires t.mu in R mode; pure CPU work, no I/O, safe from
// any goroutine.
func (t *Table) AnnounceDigestFor(peer PeerIdentity) (string, uint32) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := t.clock()
	cooledDown := func(identity, uplink PeerIdentity) bool {
		return t.health.isCooledDownLocked(identity, uplink, now)
	}
	entries := t.store.announceDigestEntriesForLocked(peer, now, t.health.isDeadLocked, cooledDown)
	return ComputeSyncDigest(entries), uint32(len(entries))
}

// RecordPeerDigestSnapshot stashes the current peer-keyed digest
// snapshot in the in-memory cache so the next reconnect to the
// same peer can emit it without re-walking the table. Production
// caller: the session-close hook in node.Service (PR 12.5
// session lifecycle wiring) computes SyncDigestFor(peer) right
// before tearing down and hands the result here.
//
// `generatedAt` is the wall-clock time the digest was computed —
// stamped both onto sessionDigestEntry.GeneratedAt (for the
// outbound wire frame) AND onto sessionDigestEntry.IdleSince
// (for the TTL anchor). Treating them as the same instant on
// record-time is intentional: the cache lifetime is bounded by
// SessionDigestCacheTTL since the digest stopped being live, so
// "when did we last touch routing state for this peer" is the
// natural TTL clock.
//
// Lock contract: acquires t.mu in W mode. Side-effect-only call;
// no return value because a duplicate record (e.g. two
// disconnect events firing for the same peer in quick succession)
// is harmless — the second overwrite refreshes the TTL anchor.
func (t *Table) RecordPeerDigestSnapshot(peer PeerIdentity, digest string, entryCount uint32, generatedAt time.Time) {
	if peer == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sessionDigestCache == nil {
		t.sessionDigestCache = make(map[PeerIdentity]sessionDigestEntry)
	}
	t.sessionDigestCache[peer] = sessionDigestEntry{
		Digest:      digest,
		EntryCount:  entryCount,
		GeneratedAt: generatedAt,
		IdleSince:   generatedAt,
	}
}

// ConsumePeerDigestSnapshot returns the cached digest snapshot
// for the peer when one exists AND `now` is within
// SessionDigestCacheTTL of the snapshot's IdleSince. The
// "consume" name is deliberate — a successful read removes the
// entry from the cache so a duplicate emit cannot fire from
// stale data on the same reconnect. Production caller: the
// session-open hook in node.Service uses this to decide whether
// to emit a RouteSyncDigestFrame on the new session.
//
// Returns the digest, the entry count, the original
// GeneratedAt, and ok=true when a fresh entry was consumed.
// Returns ok=false when no entry exists OR the TTL has elapsed
// (the helper also evicts the stale entry on the way out so a
// future call does not waste another comparison on it).
//
// Lock contract: acquires t.mu in W mode (we mutate the cache).
func (t *Table) ConsumePeerDigestSnapshot(peer PeerIdentity, now time.Time) (digest string, entryCount uint32, generatedAt time.Time, ok bool) {
	if peer == "" {
		return "", 0, time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sessionDigestCache == nil {
		return "", 0, time.Time{}, false
	}
	entry, exists := t.sessionDigestCache[peer]
	if !exists {
		return "", 0, time.Time{}, false
	}
	if now.Sub(entry.IdleSince) > SessionDigestCacheTTL {
		delete(t.sessionDigestCache, peer)
		return "", 0, time.Time{}, false
	}
	delete(t.sessionDigestCache, peer)
	return entry.Digest, entry.EntryCount, entry.GeneratedAt, true
}

// PurgeDigestSnapshot drops the cached entry for the peer
// unconditionally. Used by lifecycle hooks that know the cached
// digest is no longer trustworthy (e.g. RemoveDirectPeer in a
// "permanent eviction" path, or a config reload that invalidates
// all peer state). No-op when no entry exists or the cache map
// has not been initialised yet.
//
// Lock contract: acquires t.mu in W mode.
func (t *Table) PurgeDigestSnapshot(peer PeerIdentity) {
	if peer == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sessionDigestCache == nil {
		return
	}
	delete(t.sessionDigestCache, peer)
}

// pruneExpiredDigestSnapshotsLocked reaps every cache entry whose
// IdleSince is older than SessionDigestCacheTTL relative to `now`.
// Called from Table.TickTTL alongside the CompactExpired / health
// reconciliation pass so a peer that never reconnects does not
// keep its snapshot indefinitely.
//
// Caller must hold t.mu in W mode. The walk is O(cache size)
// which is bounded by MaxOutgoingPeers + MaxIncomingPeers in
// practice, so the cost stays trivial against the per-tick
// budget. Returns true when at least one entry was evicted so
// the caller can mark the table dirty if it needs to (the
// digest cache is NOT part of the published Snapshot today, so
// this return is reserved for future observability rather than
// dirty-flag wiring).
func (t *Table) pruneExpiredDigestSnapshotsLocked(now time.Time) bool {
	if len(t.sessionDigestCache) == 0 {
		return false
	}
	evicted := false
	for peer, entry := range t.sessionDigestCache {
		if now.Sub(entry.IdleSince) > SessionDigestCacheTTL {
			delete(t.sessionDigestCache, peer)
			evicted = true
		}
	}
	return evicted
}

// digestCacheLenLocked returns the number of cached entries.
// Test-only observer — production code does not introspect the
// cache size. Keeping it Locked-suffixed forces callers to hold
// t.mu themselves so the assertion observes a consistent state
// rather than a torn read.
func (t *Table) digestCacheLenLocked() int {
	return len(t.sessionDigestCache)
}
