package routing

import (
	"github.com/piratecash/corsa/internal/core/domain"
)

// usage.go reports what this package is holding, and it reports it the only
// way an accounting pass is allowed to: by asking containers for their len.
//
// Nothing here walks a bucket, a claim list or a health map. That is not a
// performance preference, it is the constraint the measurement step names
// outright — a pass that scanned the routing table under t.mu would stall the
// announce loop it shares that mutex with, and an instrument that changes the
// system it measures produces a number about the instrument
// (13-measurements.md §4).
//
// What this costs the caller is therefore one RLock and a handful of map
// lookups, all O(1), on a mutex the hot read paths do not take at all (they
// read the node's published snapshot).
//
// The one number NOT here is the count of claims, which is the number an
// operator most wants: it is a sum over every bucket, and there is no
// maintained counter for it. The node supplies it from the coalesced routing
// snapshot instead, where SnapshotIncremental has already paid for it as a
// by-product — see node.Service.ResourceBreakdown.
//
// Reference: docs/refactoring/dht/13-measurements.md §2, §4.

// Per-entry costs, resolved once at initialisation. Each is the key plus the
// value of one map entry and excludes what those point at: the Extra blob and
// the attestation signature hanging off a claim, the hex string memoised per
// identity, the time slices inside a flap record. The result is a floor, and
// domain.ResourceGauge names it one.
var (
	uplinkClaimBytes = domain.SizeOfAll(UplinkClaim{})
	// A bucket entry is the identity key and the SLICE HEADER of its claim
	// list — not a claim. The claims themselves are counted once, by the
	// route_claims gauge the node assembles from the published snapshot;
	// pricing a bucket at a claim's cost would count the same rows twice and
	// leave the "floor" above the truth it claims to sit under.
	identityBucketBytes  = domain.SizeOfAll(PeerIdentity{}, []UplinkClaim(nil))
	snapshotBucketBytes  = domain.SizeOfAll(PeerIdentity{}, []RouteEntry(nil))
	identityCounterBytes = domain.SizeOfAll(PeerIdentity{}, uint64(0))
	identityHexBytes     = domain.SizeOfAll(PeerIdentity{}, "")
	outboundPeerMaxBytes = domain.SizeOfAll(outboundPeerKey{}, outboundSeqEntry{})
	outboundContentBytes = domain.SizeOfAll(outboundContentKey{}, outboundSeqEntry{})
	routeHealthBytes     = domain.SizeOfAll(healthKey{}, RouteHealthState{})
	peerFlapBytes        = domain.SizeOfAll(PeerIdentity{}, peerFlapState{})
	seqVelocityBytes     = domain.SizeOfAll(PeerIdentity{}, seqVelocity{})
	badHopsBytes         = domain.SizeOfAll(PeerIdentity{}, badHopsState{})
	digestCacheBytes     = domain.SizeOfAll(PeerIdentity{}, sessionDigestEntry{})
	announceEntryBytes   = domain.SizeOfAll(AnnounceEntry{})
	announcePeerBytes    = domain.SizeOfAll(PeerIdentity{}, AnnouncePeerState{})
)

// UplinkClaimBytes is what one stored route claim costs, excluding the Extra
// and signature bytes it points at. Exported because the node reports the
// claim COUNT from its published snapshot rather than from this package, and
// the count and its unit price must not come to disagree about which struct
// they describe.
func UplinkClaimBytes() uint64 { return uplinkClaimBytes }

// Usage reports the route plane's live cardinalities.
//
// The order is significant and is the order of the answer: the two containers
// that grow as identities × peers come first, because they are the ones that
// make a large mesh expensive and the ones a reader must see before deciding
// they have understood the number.
func (t *Table) Usage() domain.SubsystemUsage {
	t.mu.RLock()
	defer t.mu.RUnlock()

	store := t.store
	gauges := []domain.ResourceGauge{
		// Identities × peers. No TTL and no size cap by design — it is a
		// high-water watermark, and forgetting one causes stale-reject loops —
		// so its only relief is a receiver disconnecting or a destination
		// disappearing entirely.
		domain.NewResourceGauge("outbound_peer_seq", len(store.outboundPeerMax), outboundPeerMaxBytes),
		// Identities × distinct wire shapes. Bounded by a TTL and a soft cap,
		// so a rising count here means shapes are churning faster than they
		// age out.
		domain.NewResourceGauge("outbound_content_seq", len(store.outboundContent), outboundContentBytes),
		// Identities × uplinks.
		domain.NewResourceGauge("route_health", len(t.health.states), routeHealthBytes),
		domain.NewResourceGauge("route_identities", len(store.buckets), identityBucketBytes),
		domain.NewResourceGauge("identity_hex_memo", len(store.identityHex), identityHexBytes),
		// Three counters keyed by identity that are never pruned by design:
		// they exist to keep outbound SeqNo monotonic, and forgetting one
		// would let a stale SeqNo be re-emitted. They are here precisely
		// because "grows with every identity ever seen and is never released"
		// is a fact a budget has to be told.
		domain.NewResourceGauge("seq_counters", len(store.seqCounters), identityCounterBytes),
		domain.NewResourceGauge("outbound_max", len(store.outboundMax), identityCounterBytes),
		domain.NewResourceGauge("outbound_broadcast_max", len(store.outboundBroadcastMax), identityCounterBytes),
		// The publisher's projected second copy of the table. Priced at its
		// map entry for the same reason as route_identities: what the slices
		// hold is a copy of rows already counted, and the floor must stay
		// below the truth rather than above it.
		domain.NewResourceGauge("snapshot_raw_cache", len(t.snapRawCache), snapshotBucketBytes),
		domain.NewResourceGauge("session_digest_cache", len(t.sessionDigestCache), digestCacheBytes),
		domain.NewResourceGauge("peer_flap", len(t.flap.state), peerFlapBytes),
		domain.NewResourceGauge("seq_velocity", len(t.flap.seqVelocities), seqVelocityBytes),
		domain.NewResourceGauge("bad_hops", len(t.flap.badHops), badHopsBytes),
	}
	return domain.NewSubsystemUsage(domain.ResourceSubsystemRoutePlane, gauges...)
}

// Usage reports what the announce plane keeps per peer.
//
// Unlike the table's, this pass DOES iterate — over peers, of which there are
// tens, never over identities, of which there are thousands. That is the whole
// distinction: the number worth having is the total size of the snapshots held
// on peers' behalf, and it is a sum of tens of already-known slice lengths.
//
// Two peer mutexes are taken in sequence, never together, and the registry
// mutex is released first, so this adds no edge to the r.mu → s.mu order the
// registry documents.
func (r *AnnounceStateRegistry) Usage() domain.SubsystemUsage {
	r.mu.Lock()
	states := make([]*AnnouncePeerState, 0, len(r.peers))
	for _, state := range r.peers {
		states = append(states, state)
	}
	peerCount := len(r.peers)
	r.mu.Unlock()

	entries := 0
	for _, state := range states {
		entries += state.sentSnapshotLen()
	}
	return domain.NewSubsystemUsage(
		domain.ResourceSubsystemAnnounce,
		// The dominant term of this plane: one announce entry per identity
		// per peer, refreshed only on a forced full sync, so it is held at
		// full size for up to a full sync cadence after the table shrinks.
		domain.NewResourceGauge("last_sent_entries", entries, announceEntryBytes),
		domain.NewResourceGauge("announce_peers", peerCount, announcePeerBytes),
	)
}

// sentSnapshotLen reports how many entries this peer's retained snapshot
// holds, and nothing else about it.
//
// It exists rather than reusing View() because View copies the capability
// slice on every call: a diagnostic that allocated once per peer per sample
// would be paying for a field it does not read.
func (s *AnnouncePeerState) sentSnapshotLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSentSnapshot == nil {
		return 0
	}
	return len(s.lastSentSnapshot.Entries)
}
