package routing

import (
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// present marks an identity as having live storage so the lifecycle
// "identity gone" eviction in pruneOutboundCachesLocked does not fire.
func present(s *routeStore, id PeerIdentity) {
	s.buckets[id] = []UplinkClaim{{}}
}

// TestPruneOutboundCaches_ContentEviction pins the outboundContent leak
// fix: for a PRESENT identity the cache keeps recently-reused shapes and
// drops aged or dead ones; for a fully GONE identity every shape is
// dropped. (Evicting content is always correctness-safe — a miss takes a
// fresh monotonic SeqNo via the never-pruned outboundMax.)
func TestPruneOutboundCaches_ContentEviction(t *testing.T) {
	s := newRouteStore()
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	aged := base.Add(-outboundCacheTTL - time.Minute).UnixNano()

	present(s, domaintest.ID("id-A"))
	present(s, domaintest.ID("id-C"))
	// domaintest.ID("id-GONE") intentionally has no bucket.

	liveKey := outboundContentKey{Identity: domaintest.ID("id-A"), Sig: outboundEmitSig{Uplink: domaintest.ID("up-1")}}
	agedKey := outboundContentKey{Identity: domaintest.ID("id-A"), Sig: outboundEmitSig{Uplink: domaintest.ID("up-2")}}
	deadKey := outboundContentKey{Identity: domaintest.ID("id-C"), Sig: outboundEmitSig{Uplink: domaintest.ID("up-3")}}
	goneKey := outboundContentKey{Identity: domaintest.ID("id-GONE"), Sig: outboundEmitSig{Uplink: domaintest.ID("up-4")}}

	s.outboundContent[liveKey] = outboundSeqEntry{seq: 5, lastUsed: base.UnixNano()}
	s.outboundContent[agedKey] = outboundSeqEntry{seq: 5, lastUsed: aged}
	s.outboundContent[deadKey] = outboundSeqEntry{seq: 3, lastUsed: base.UnixNano()}
	s.outboundBroadcastMax[domaintest.ID("id-C")] = 10 // 3 < 10 → dead
	s.outboundContent[goneKey] = outboundSeqEntry{seq: 5, lastUsed: base.UnixNano()}

	s.pruneOutboundCachesLocked(base)

	if _, ok := s.outboundContent[liveKey]; !ok {
		t.Fatal("recently-used content for a present identity must be retained")
	}
	if _, ok := s.outboundContent[agedKey]; ok {
		t.Fatal("aged content must be evicted by TTL")
	}
	if _, ok := s.outboundContent[deadKey]; ok {
		t.Fatal("dead content (SeqNo below broadcast watermark) must be evicted")
	}
	if _, ok := s.outboundContent[goneKey]; ok {
		t.Fatal("content for a fully-gone identity must be evicted")
	}
}

// TestPruneOutboundCaches_PeerMaxOnlyLifecycle pins the safety fix: a
// receiver high-water watermark for a PRESENT identity is NEVER dropped by
// the prune sweep (regardless of age), because regressing it would cause a
// stale-reject loop; only entries for a fully-gone destination identity
// are dropped.
func TestPruneOutboundCaches_PeerMaxOnlyLifecycle(t *testing.T) {
	s := newRouteStore()
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	aged := base.Add(-outboundCacheTTL - time.Minute).UnixNano()

	present(s, domaintest.ID("id-A"))
	// domaintest.ID("id-GONE") has no bucket.

	liveButIdle := outboundPeerKey{Identity: domaintest.ID("id-A"), Peer: domaintest.ID("peer-Z")}
	goneIdentity := outboundPeerKey{Identity: domaintest.ID("id-GONE"), Peer: domaintest.ID("peer-Z")}

	// Idle (aged) watermark for a PRESENT identity — must survive.
	s.outboundPeerMax[liveButIdle] = outboundSeqEntry{seq: 6, lastUsed: aged}
	s.outboundPeerMax[goneIdentity] = outboundSeqEntry{seq: 6, lastUsed: base.UnixNano()}

	s.pruneOutboundCachesLocked(base)

	if _, ok := s.outboundPeerMax[liveButIdle]; !ok {
		t.Fatal("an idle but live (present-identity) receiver watermark must NOT be TTL-evicted")
	}
	if _, ok := s.outboundPeerMax[goneIdentity]; ok {
		t.Fatal("a watermark for a fully-gone destination identity must be evicted")
	}
}

// TestForgetReceiver_DropsOnlyThatPeer pins the disconnect-driven bound:
// forgetReceiverLocked drops every watermark for the disconnected receiver
// and leaves other receivers' watermarks untouched.
func TestForgetReceiver_DropsOnlyThatPeer(t *testing.T) {
	s := newRouteStore()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	present(s, domaintest.ID("id-A"))
	present(s, domaintest.ID("id-B"))

	gone := outboundPeerKey{Identity: domaintest.ID("id-A"), Peer: domaintest.ID("peer-Z")}
	goneOther := outboundPeerKey{Identity: domaintest.ID("id-B"), Peer: domaintest.ID("peer-Z")}
	keep := outboundPeerKey{Identity: domaintest.ID("id-A"), Peer: domaintest.ID("peer-Y")}
	s.outboundPeerMax[gone] = outboundSeqEntry{seq: 6, lastUsed: now}
	s.outboundPeerMax[goneOther] = outboundSeqEntry{seq: 7, lastUsed: now}
	s.outboundPeerMax[keep] = outboundSeqEntry{seq: 8, lastUsed: now}

	s.forgetReceiverLocked(domaintest.ID("peer-Z"))

	if _, ok := s.outboundPeerMax[gone]; ok {
		t.Fatal("disconnected receiver's watermark must be dropped")
	}
	if _, ok := s.outboundPeerMax[goneOther]; ok {
		t.Fatal("disconnected receiver's watermark must be dropped for every identity")
	}
	if _, ok := s.outboundPeerMax[keep]; !ok {
		t.Fatal("another receiver's watermark must be retained")
	}
}

// TestPruneOutboundCaches_SoftCapBackstop pins the last-resort content
// bound: even with no aged/dead/gone entries the cache cannot exceed the
// soft cap.
func TestPruneOutboundCaches_SoftCapBackstop(t *testing.T) {
	s := newRouteStore()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	nowNanos := now.UnixNano()
	present(s, domaintest.ID("id"))

	for i := 0; i < outboundContentSoftCap+5000; i++ {
		k := outboundContentKey{
			Identity: domaintest.ID("id"),
			Sig:      outboundEmitSig{Uplink: domaintest.ID("up"), AttestedSig: strconv.Itoa(i)},
		}
		s.outboundContent[k] = outboundSeqEntry{seq: uint64(i + 1), lastUsed: nowNanos}
	}

	s.pruneOutboundCachesLocked(now)

	if len(s.outboundContent) > outboundContentSoftCap {
		t.Fatalf("soft cap not enforced: len=%d cap=%d", len(s.outboundContent), outboundContentSoftCap)
	}
}
