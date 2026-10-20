package node

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// loopback_inbound.go is the loopback inbound class
// (docs/protocol/network_security.md, "Loopback inbound class"). Every inbound
// onion connection arrives from the local Tor daemon, so on the listener it
// is one source, 127.0.0.1 or ::1. Under the per-IP rules that source shared
// eight connections and ten new ones per ten seconds between every onion
// peer, and one misbehaving onion peer could get 127.0.0.1 banned for a day,
// cutting every other one off.
//
// The class replaces those rules for loopback sources, for v2 and legacy
// alike:
//
//   - budgets of the whole class, never of one connection, so reconnecting
//     opens nothing new: connections held at once (loopbackActiveBound), new
//     connections within a sliding window (loopbackMaxNewPerWindow per
//     loopbackNewWindow) and, already in inbound_handshakes.go, v2 handshakes
//     in flight (16). The node's own bounds — the shared connection budget,
//     the inbound limit, the node's handshake bound — apply on top;
//   - no IP score and no IP ban. A violation before any v2 proof closes the
//     connection it came on, and a legacy connection never gets further: the
//     identity it claims proves nothing and is never charged. A violation on
//     a v2 session charges the identity the session proved
//     (provenIdentityPenalties), and a banned identity's loopback sessions
//     are closed once they prove it.
//
// Loopback is not trust: authentication, pins and the refusal to downgrade
// apply to the class exactly as to an external source.
//
// The node has a single listener, so the class is read from the source
// address alone, and a local process on the same machine falls into it as
// well as the Tor daemon does. A dedicated onion listener would let the class
// be read from the listener instead; the node has none.

const (
	// loopbackMaxActiveCeiling bounds the loopback connections held at once
	// on any node: eight times the per-IP cap the whole onion entry shared
	// before, room for a hub's onion neighbours, while a flood through Tor
	// cannot hold more descriptors and goroutines than this. Not measured
	// yet: to be checked by the Tor measurement before release.
	loopbackMaxActiveCeiling = 64
	// loopbackActiveShareDivisor is the share of a configured inbound
	// capacity the class may hold — the same quarter the class holds of the
	// node's handshake bound (16 of 64) — so loopback cannot take the whole
	// inbound capacity from the clearnet entry.
	loopbackActiveShareDivisor = 4
	// loopbackMinActiveFloor is the least the class holds whatever the
	// configured capacity: the eight connections the whole onion entry had
	// under the per-IP cap before the class, so a small inbound capacity
	// (below 32) never leaves onion peers worse off than they were.
	loopbackMinActiveFloor = 8
	// loopbackMaxNewPerWindow and loopbackNewWindow bound the loopback
	// connections admitted within any ten seconds: ten times the per-IP rate
	// the onion entry shared before, about one reconnect of every active
	// connection per window and a half. Not measured yet either.
	loopbackMaxNewPerWindow = 100
	loopbackNewWindow       = 10 * time.Second
)

// loopbackActiveBound is the class's bound on connections held at once on a
// node whose inbound capacity is inboundCapacity, 0 meaning the node set
// none: a quarter of the capacity, at least the floor, at most the ceiling.
// The node's own inbound bound still applies on top, so on a node whose
// capacity is below the floor that bound is the one that refuses.
func loopbackActiveBound(inboundCapacity int) int {
	if inboundCapacity <= 0 {
		return loopbackMaxActiveCeiling
	}
	return min(loopbackMaxActiveCeiling, max(loopbackMinActiveFloor, inboundCapacity/loopbackActiveShareDivisor))
}

// inboundCapacityOf is the most inbound connections the node's own bounds
// allow: the smaller of the inbound limit and the part of the shared
// connection budget not reserved for outbound, whichever is set; 0 when
// neither is.
func inboundCapacityOf(budget connbudget.Stats) int {
	capacity := budget.MaxInbound
	if budget.Enabled && budget.NonSlotCapacity > 0 && (capacity <= 0 || budget.NonSlotCapacity < capacity) {
		capacity = budget.NonSlotCapacity
	}
	return max(capacity, 0)
}

// loopbackInboundBounds are the class's own budgets on connections.
type loopbackInboundBounds struct {
	maxActive       int
	maxNewPerWindow int
	window          time.Duration
}

// productionLoopbackInboundBounds are the bounds of a node whose inbound
// capacity is inboundCapacity.
func productionLoopbackInboundBounds(inboundCapacity int) loopbackInboundBounds {
	return loopbackInboundBounds{
		maxActive:       loopbackActiveBound(inboundCapacity),
		maxNewPerWindow: loopbackMaxNewPerWindow,
		window:          loopbackNewWindow,
	}
}

// inboundSourceClassOf is the class of an accepted connection's source.
func inboundSourceClassOf(addr net.Addr) domain.InboundSourceClass {
	return domain.ClassifyInboundSource(remoteIP(addr))
}

// inboundSourceClassOfRemote is inboundSourceClassOf for a stringified
// host:port, as *netcore.NetCore.RemoteAddr returns it.
func inboundSourceClassOfRemote(remoteAddr string) domain.InboundSourceClass {
	return domain.ClassifyInboundSource(remoteIPFromString(remoteAddr))
}

// loopbackInboundClass holds the class's connection budgets, its condemned
// connections and the penalties of the identities its sessions proved. Its
// own leaf mutexes, one per kind of state: each is taken with no domain mutex
// held and nothing is called under it. The counters the diagnostics read are atomics, so reading them
// takes no lock.
type loopbackInboundClass struct {
	bounds    loopbackInboundBounds
	clock     func() time.Time
	startedAt time.Time

	mu     sync.Mutex
	active int
	// admittedAt is a ring of the admission times of the last maxNewPerWindow
	// admissions; next is the oldest of them once filled reaches the ring's
	// length. A new connection is admitted only when that oldest one is a
	// whole window old, which makes the bound hold over ANY window rather
	// than per fixed window — a fixed window lets twice the bound through
	// across its boundary.
	admittedAt []time.Time
	next       int
	filled     int

	// condemned holds an entry for every loopback connection whose read loop
	// is running (track to forget), true once the connection is condemned
	// for a violation. The read loop ends a condemned connection after the
	// frame it is dispatching, so the reply to that frame still goes out
	// first. Only a tracked connection can be condemned, so a violation
	// charged after its loop ended leaves nothing behind. A mutex of its
	// own: the read loops reach it on every frame, and admission must not
	// wait for them, nor they for the accept loop.
	condemnedMu sync.Mutex
	condemned   map[domain.ConnID]bool

	activeGauge      atomic.Int64
	admitted         atomic.Uint64
	violationsClosed atomic.Uint64
	identityBanned   atomic.Uint64

	identities *provenIdentityPenalties
}

// newLoopbackInboundClass builds the class. A rate below one is raised to
// one: a ring of no admissions could neither refuse nor record anything.
func newLoopbackInboundClass(bounds loopbackInboundBounds, penalties identityPenaltyPolicy, clock func() time.Time) *loopbackInboundClass {
	if clock == nil {
		clock = time.Now
	}
	bounds.maxNewPerWindow = max(bounds.maxNewPerWindow, 1)
	return &loopbackInboundClass{
		bounds:     bounds,
		clock:      clock,
		startedAt:  clock().UTC(),
		admittedAt: make([]time.Time, bounds.maxNewPerWindow),
		condemned:  make(map[domain.ConnID]bool),
		identities: newProvenIdentityPenalties(penalties, clock),
	}
}

// admit takes a slot of the class for an accepted loopback connection, or
// names the budget that refuses it. With enforced
// false (per-IP limits off) the connection is counted but never refused. The
// returned release gives the slot back; a second call gives nothing back.
func (c *loopbackInboundClass) admit(enforced bool) (func(), *listenerRefusalError) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if enforced {
		if refusal := c.refusalLocked(now); refusal != nil {
			return nil, refusal
		}
		c.recordAdmissionLocked(now)
	}
	c.active++
	c.activeGauge.Store(int64(c.active))
	c.admitted.Add(1)
	var once sync.Once
	return func() { once.Do(c.leave) }, nil
}

// refusalLocked is the budget the next admission would break, nil if none.
// Caller holds c.mu.
func (c *loopbackInboundClass) refusalLocked(now time.Time) *listenerRefusalError {
	switch {
	case c.active >= c.bounds.maxActive:
		return &listenerRefusalError{reason: refusalLoopbackConnections}
	case c.filled == len(c.admittedAt) && now.Sub(c.admittedAt[c.next]) < c.bounds.window:
		return &listenerRefusalError{reason: refusalLoopbackConnectionRate}
	}
	return nil
}

// recordAdmissionLocked puts now in place of the oldest admission. Caller
// holds c.mu.
func (c *loopbackInboundClass) recordAdmissionLocked(now time.Time) {
	c.admittedAt[c.next] = now
	c.next = (c.next + 1) % len(c.admittedAt)
	c.filled = min(c.filled+1, len(c.admittedAt))
}

func (c *loopbackInboundClass) leave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	c.activeGauge.Store(int64(c.active))
}

// track registers a loopback connection whose read loop starts; forget
// ends it.
func (c *loopbackInboundClass) track(id domain.ConnID) {
	c.condemnedMu.Lock()
	defer c.condemnedMu.Unlock()
	c.condemned[id] = false
}

// condemn marks a tracked loopback connection to be closed for a violation
// and reports whether this was the first mark, so the caller counts each
// connection once however many violations it commits before its read loop
// ends it. A connection no read loop tracks any more is not marked.
func (c *loopbackInboundClass) condemn(id domain.ConnID) bool {
	c.condemnedMu.Lock()
	defer c.condemnedMu.Unlock()
	condemned, tracked := c.condemned[id]
	if !tracked || condemned {
		return false
	}
	c.condemned[id] = true
	return true
}

// isCondemned reports whether the connection was condemned.
func (c *loopbackInboundClass) isCondemned(id domain.ConnID) bool {
	c.condemnedMu.Lock()
	defer c.condemnedMu.Unlock()
	return c.condemned[id]
}

// forget drops what the class holds about a connection that ended.
func (c *loopbackInboundClass) forget(id domain.ConnID) {
	c.condemnedMu.Lock()
	defer c.condemnedMu.Unlock()
	delete(c.condemned, id)
}

// noteViolationClosed counts a loopback connection closed for a violation
// before any v2 proof.
func (c *loopbackInboundClass) noteViolationClosed() {
	c.violationsClosed.Add(1)
}

// noteIdentityBanned counts a loopback v2 session closed because the
// identity it proved is banned.
func (c *loopbackInboundClass) noteIdentityBanned() {
	c.identityBanned.Add(1)
}

// stats reads the class's own counters. Lock-free.
func (c *loopbackInboundClass) stats(readAt time.Time, enforced bool, handshakes int) domain.LoopbackInboundStats {
	scored, banned := c.identities.gauges()
	return domain.LoopbackInboundStats{
		StartedAt: c.startedAt,
		ReadAt:    readAt,
		Bounds: domain.LoopbackInboundBounds{
			Enforced:        enforced,
			MaxActive:       c.bounds.maxActive,
			MaxNewPerWindow: c.bounds.maxNewPerWindow,
			Window:          c.bounds.window,
			MaxHandshakes:   handshakes,
		},
		Active:   int(c.activeGauge.Load()),
		Admitted: c.admitted.Load(),
		Refusals: domain.LoopbackInboundRefusals{
			ViolationsClosed: c.violationsClosed.Load(),
			IdentityBanned:   c.identityBanned.Load(),
		},
		IdentitiesScored: scored,
		IdentitiesBanned: banned,
	}
}

// errLoopbackIdentityBanned is a loopback v2 session whose proven identity is
// banned by the loopback class.
var errLoopbackIdentityBanned = errors.New("loopback inbound: the proven identity is banned")

// chargeLoopbackViolation is addBanScore for a loopback connection. Before a
// v2 proof — a legacy connection, whatever identity its hello claimed — the
// connection is condemned and nothing else is charged. On a v2 session the
// proven identity is charged; any session of a banned identity that commits a
// violation — the one that crossed the threshold, or another one open since
// before the ban — receives the ban notice, as a blacklisted IP's session
// does, and is condemned. A charge the full store could not record closes the
// session too: fail closed rather than let the violation go uncounted.
//
// Condemned connections are ended by their own read loop once the frame
// being dispatched is answered (handleConn), so the error reply a violation
// earns still goes out before the close.
func (s *Service) chargeLoopbackViolation(id domain.ConnID, core *netcore.NetCore, delta int) {
	peer, proven := penaltySubjectOfCore(id, core).provenIdentity()
	if !proven {
		if !s.condemnInboundSelected(id) {
			return
		}
		s.loopbackInbound.noteViolationClosed()
		log.Warn().Uint64("conn_id", uint64(id)).Str("addr", core.RemoteAddr()).Int("delta", delta).
			Msg("loopback_violation_connection_closed")
		return
	}
	verdict := s.loopbackInbound.identities.charge(peer, delta)
	firstCondemn := false
	if verdict.unrecorded || verdict.banned {
		firstCondemn = s.condemnInboundSelected(id)
	}
	// Debug, not Warn: a session may commit a violation per frame within its
	// command budget; the ban below is the event an operator looks for.
	log.Debug().Uint64("conn_id", uint64(id)).Str("peer", peer.String()).Int("delta", delta).
		Int("score", verdict.score).Bool("banned", verdict.banned).Msg("loopback_violation_identity_charged")
	switch {
	case verdict.unrecorded:
		if firstCondemn {
			s.loopbackInbound.noteViolationClosed()
			log.Warn().Uint64("conn_id", uint64(id)).Str("peer", peer.String()).
				Msg("loopback_violation_unrecorded_connection_closed")
		}
	case verdict.banned:
		if !firstCondemn {
			return
		}
		s.loopbackInbound.noteIdentityBanned()
		log.Warn().Uint64("conn_id", uint64(id)).Str("peer", peer.String()).Time("until", verdict.until).
			Bool("just_banned", verdict.justBanned).Msg("loopback_identity_banned_session_closed")
		s.emitPeerBannedNoticeByID(id, verdict.until, protocol.PeerBannedReasonPeerBan)
	}
}

// refuseBannedLoopbackIdentity refuses a loopback v2 session whose proven
// identity is banned. The ban applies to the loopback entry only: an external
// source is scored and banned by its IP, as before. Off on a node whose
// per-IP limits are switched off, as the IP blacklist is.
func (s *Service) refuseBannedLoopbackIdentity(addr net.Addr, proof sessionv2.ProvenIdentity) error {
	if s.disableRateLimiting || inboundSourceClassOf(addr) != domain.InboundSourceLoopback {
		return nil
	}
	peer, proven := provenIdentitySubjectOf(proof).provenIdentity()
	if !proven || !s.loopbackInbound.identities.isBanned(peer) {
		return nil
	}
	s.loopbackInbound.noteIdentityBanned()
	return fmt.Errorf("%w: %s", errLoopbackIdentityBanned, peer)
}

// LoopbackInboundStats returns the state of the loopback inbound class.
// Lock-free: atomics only. Implements rpc.RoutingProvider.
func (s *Service) LoopbackInboundStats() domain.LoopbackInboundStats {
	stats := s.loopbackInbound.stats(time.Now().UTC(), !s.disableRateLimiting, s.inboundHandshakes.bounds.loopback)
	stats.HandshakesInFlight = s.inboundHandshakes.loopbackRunning()
	stats.Refusals.ConnectionRate = s.listenerRefusals.loopbackRefused(refusalLoopbackConnectionRate)
	stats.Refusals.ActiveLimit = s.listenerRefusals.loopbackRefused(refusalLoopbackConnections)
	stats.Refusals.ConnectionBudget = s.listenerRefusals.loopbackRefused(refusalConnectionBudget)
	stats.Refusals.InboundLimit = s.listenerRefusals.loopbackRefused(refusalInboundLimit)
	stats.Refusals.HandshakesLoopback = s.listenerRefusals.loopbackRefused(refusalHandshakesLoopback)
	stats.Refusals.HandshakesNode = s.listenerRefusals.loopbackRefused(refusalHandshakesNode)
	return stats
}
