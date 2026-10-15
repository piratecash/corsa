package node

import (
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// Session kinds: how many of this node's sessions are secure sessions v2 and
// how many are legacy, per direction, and how many legacy peers announce a
// version from before v2. The legacy path is switched off by raising
// MinimumProtocolVersion to the version that brought v2, and that is decided
// by these numbers together with out-of-band release-adoption data, not by a
// date (docs/protocol/session_v2.md).
//
// The cumulative counts count SESSIONS, not peers, so one peer opening many
// sessions can run them up: they are gameable and are a lower bound on
// activity, never a count of distinct nodes. The reading that resists that is
// the live gauge (SessionKindStats.Live), which counts distinct identities
// among the sessions live right now — bounded by the connection count — and
// LegacyAtClaimedV2, the legacy sessions whose peer claimed v2.
//
// Each direction is counted once per session: inbound in completeInboundAuth
// when the peer's identity is authenticated (v1 auth_session or v2
// session_proof), outbound in publishAndServeCMSession right before the
// ConnectionManager slot is promoted — after the setup succeeded, the session
// was registered and the pin let it through. A dialled session whose setup
// failed, whose slot the ConnectionManager evicted under the setup, or that
// the pin refused at publication never served the peer and is not counted.
// Only ConnectionManager sessions are in the cumulative outbound counts: a
// session of the legacy dial path (openPeerSession / runPeerSession) is not,
// although the live gauge, which walks s.sessions, does include it.
//
// "Proven by v2" is one predicate everywhere: the session's v2 proof of the
// very identity it serves (peerSession.provenIdentity outbound,
// connauth.State.ProvenIdentity inbound). Nothing on the wire or in the
// session's handling depends on the counts.

// sessionKindCounters are the cumulative counts behind SessionKindStats.
// Atomics, outside the domain mutexes: they are written once per session, on
// accept and on outbound publication, and read by fetchRouteSummary, and no decision
// reads them together with domain state. The zero value counts.
type sessionKindCounters struct {
	inboundV2         atomic.Uint64
	inboundLegacy     atomic.Uint64
	outboundV2        atomic.Uint64
	outboundLegacy    atomic.Uint64
	hellosBelow       atomic.Uint64
	welcomesBelow     atomic.Uint64
	legacyAtClaimedV2 atomic.Uint64
}

// sessionKindDirection is one direction's kind counters together with the
// shared cross-direction counter of legacy sessions whose peer claimed v2.
type sessionKindDirection struct {
	v2, legacy, belowSecureVersion, legacyClaimedV2 *atomic.Uint64
}

func (c *sessionKindCounters) inbound() sessionKindDirection {
	return sessionKindDirection{v2: &c.inboundV2, legacy: &c.inboundLegacy, belowSecureVersion: &c.hellosBelow, legacyClaimedV2: &c.legacyAtClaimedV2}
}

func (c *sessionKindCounters) outbound() sessionKindDirection {
	return sessionKindDirection{v2: &c.outboundV2, legacy: &c.outboundLegacy, belowSecureVersion: &c.welcomesBelow, legacyClaimedV2: &c.legacyAtClaimedV2}
}

// note counts one authenticated session. proven is whether v2 proved the
// peer; announced is the version of its hello or welcome. A legacy session is
// split by what the peer claims: below the v2 version it is an old peer; at or
// above it a secure handshake was expected and did not happen.
func (d sessionKindDirection) note(proven bool, announced domain.ProtocolVersion) {
	if proven {
		d.v2.Add(1)
		return
	}
	d.legacy.Add(1)
	if int(announced) < config.ProtocolVersionSecureSession {
		d.belowSecureVersion.Add(1)
	} else {
		d.legacyClaimedV2.Add(1)
	}
}

// liveSessionKindsBuckets accumulates the live gauge during one walk: a
// session count and a distinct-identity set per bucket.
type liveSessionKindsBuckets struct {
	inboundV2      liveKindBucket
	inboundLegacy  liveKindBucket
	outboundV2     liveKindBucket
	outboundLegacy liveKindBucket
}

type liveKindBucket struct {
	sessions   int
	identities map[domain.PeerIdentity]struct{}
}

func (b *liveKindBucket) add(id domain.PeerIdentity) {
	b.sessions++
	if id.IsZero() {
		return
	}
	if b.identities == nil {
		b.identities = make(map[domain.PeerIdentity]struct{})
	}
	b.identities[id] = struct{}{}
}

func (b liveKindBucket) count() domain.LiveSessionKindCount {
	return domain.LiveSessionKindCount{Sessions: b.sessions, Identities: len(b.identities)}
}

// refreshLiveSessionKinds walks the live sessions once under peerMu.RLock and
// publishes the gauge. It rides the announce cadence (announceTargets),
// alongside refreshNeighbourComposition: both are diagnostic walks of the same
// maps, and a hot read must take no domain mutex (docs/locking.md), so the
// gauge is published behind an atomic.Pointer rather than read under the lock.
func (s *Service) refreshLiveSessionKinds() {
	var buckets liveSessionKindsBuckets

	s.peerMu.RLock()
	for _, session := range s.sessions {
		if session == nil {
			continue
		}
		_, proven := session.provenIdentity()
		outboundBucket(&buckets, proven).add(session.peerIdentity)
	}
	for id, entry := range s.conns {
		info, ok := snapshotEntryLocked(id, entry)
		if !ok || info.dir != netcore.Inbound || !info.tracked {
			continue
		}
		// One read: the auth state can be replaced concurrently, and the
		// verified check and the proof must come from the same state.
		auth := entry.core.Auth()
		if auth == nil || !auth.Verified {
			continue
		}
		_, proven := auth.ProvenIdentity()
		inboundBucket(&buckets, proven).add(info.identity)
	}
	s.peerMu.RUnlock()

	s.liveSessionKinds.Store(&domain.LiveSessionKinds{
		Ready:          true,
		UpdatedAt:      time.Now().UTC(),
		InboundV2:      buckets.inboundV2.count(),
		InboundLegacy:  buckets.inboundLegacy.count(),
		OutboundV2:     buckets.outboundV2.count(),
		OutboundLegacy: buckets.outboundLegacy.count(),
	})
}

func inboundBucket(b *liveSessionKindsBuckets, proven bool) *liveKindBucket {
	if proven {
		return &b.inboundV2
	}
	return &b.inboundLegacy
}

func outboundBucket(b *liveSessionKindsBuckets, proven bool) *liveKindBucket {
	if proven {
		return &b.outboundV2
	}
	return &b.outboundLegacy
}

// liveSessionKindsSnapshot is the last published gauge, or a not-ready zero
// before the first walk.
func (s *Service) liveSessionKindsSnapshot() domain.LiveSessionKinds {
	if snap := s.liveSessionKinds.Load(); snap != nil {
		return *snap
	}
	return domain.LiveSessionKinds{}
}

// SessionKindStats returns the cumulative counts of authenticated sessions by
// kind and direction, with the live gauge. Lock-free: the cumulative counters
// are atomics, startedAt is immutable, and the gauge is an immutable value
// behind an atomic.Pointer. Reading resets nothing. Implements
// rpc.RoutingProvider.
func (s *Service) SessionKindStats() domain.SessionKindStats {
	return domain.SessionKindStats{
		StartedAt:                  s.startedAt,
		ReadAt:                     time.Now().UTC(),
		InboundV2:                  s.sessionKinds.inboundV2.Load(),
		InboundLegacy:              s.sessionKinds.inboundLegacy.Load(),
		OutboundV2:                 s.sessionKinds.outboundV2.Load(),
		OutboundLegacy:             s.sessionKinds.outboundLegacy.Load(),
		HellosBelowSecureVersion:   s.sessionKinds.hellosBelow.Load(),
		WelcomesBelowSecureVersion: s.sessionKinds.welcomesBelow.Load(),
		LegacyAtClaimedV2:          s.sessionKinds.legacyAtClaimedV2.Load(),
		Live:                       s.liveSessionKindsSnapshot(),
	}
}
