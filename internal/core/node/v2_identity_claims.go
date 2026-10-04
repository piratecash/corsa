package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// v2_identity_claims.go is what a legacy (v1) connection that merely NAMES an
// identity may still do once that identity has proved itself over v2 to this
// node — nothing on the routing plane, and nothing at all from the moment of
// the proof (docs/refactoring/n1-legacy-residual.md §2, owner decision on
// С-9 and L-RT-1…3).
//
//   - An identity REQUIRES v2 here once it is pinned (it proved v2 to this
//     node, persisted across restarts) or while it has a live v2 connection.
//     A v2 session whose pin could not be stored is not established at all
//     (errPinStoreFull), so the second clause is defence in depth, not a
//     substitute for the pin.
//   - A new legacy connection naming such an identity is refused
//     (refuseLegacyIdentity). A legacy connection that predates the proof is
//     CLOSED at the proof: addressing every send, push and selection path
//     that picks a connection by identity reliably is not possible, so the
//     connection that would be picked is removed instead (§7.5 of the v2
//     contract). Its reconnects are then refused, also after the last v2
//     session of the identity is gone.
//   - Between the pin and the close, and for any frame the closing reader
//     still had buffered, the routing-plane handlers drop routing input from
//     a legacy connection naming such an identity, and hop-ack signals of an
//     attempt that went out over one are not charged to the identity.
//     Legacy input admitted BEFORE the pin is waited for by the proof, so it
//     cannot write after the purge (admitRoutingInput).
//   - What a legacy connection wrote BEFORE the proof (claims, withdrawals
//     with an arbitrary SeqNo, cooldowns, flaps, the v3 epoch) is forgotten
//     at the proof, locally and without a wire withdrawal, unless a live v2
//     connection of the identity already vouches for the routes.

// identityRequiresV2 reports whether a legacy connection naming id must be
// refused, and its routing input dropped. Takes the pin store's leaf mutex and
// peerMu.RLock in turn, never together; the caller must hold no domain mutex.
func (s *Service) identityRequiresV2(id domain.PeerIdentity) bool {
	if id.IsZero() || s.sessionMode() == sessionv2.ModeLegacyOnly {
		return false
	}
	if s.secureSessions.store.identityPinned(id) {
		return true
	}
	return s.identityHasProvenConnection(id)
}

// identityHasProvenConnection reports whether id has a registered connection,
// in either direction, on which it proved itself over v2. Takes peerMu.RLock.
func (s *Service) identityHasProvenConnection(id domain.PeerIdentity) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	for _, session := range s.sessions {
		if session.peerIdentity != id {
			continue
		}
		if _, proven := session.provenIdentity(); proven {
			return true
		}
	}
	found := false
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity != id {
			return true
		}
		if s.connProvenLocked(info.id) {
			found = true
			return false
		}
		return true
	})
	return found
}

// connProvenLocked reports whether the accepted connection id carries a v2
// proof. Caller must hold peerMu (read or write).
func (s *Service) connProvenLocked(id domain.ConnID) bool {
	core := s.coreForIDLocked(id)
	if core == nil {
		return false
	}
	_, proven := core.Auth().ProvenIdentity()
	return proven
}

// onIdentityProvenV2 is called once a v2 handshake proved an identity and the
// protection it earned is on disk, BEFORE the v2 connection is registered or
// serves a frame: so the residue it forgets cannot be refreshed by the
// connection it makes room for, and a legacy connection closed here cannot be
// chosen over the v2 one even for an instant after it is registered.
//
// Legacy input admitted before the pin may still be writing, and the purge
// must come after it (admitRoutingInput). If ctx ends first — a dial's
// timeout, not only shutdown — the obligation is not dropped: it passes to
// the last of those writers, whose release purges (unprovenRoutingWriters.
// owePurge). The proof then fails, and the caller must not establish the
// session: a v2 connection registered before that purge would make it stand
// down ("a live v2 connection vouches") and keep the late legacy write.
func (s *Service) onIdentityProvenV2(ctx context.Context, proof sessionv2.ProvenIdentity) error {
	id, ok := proof.Identity()
	if !ok {
		return nil
	}
	s.closeLegacyClaimsOf(ctx, id)
	if err := s.unprovenRouting.waitDrained(ctx, id); err != nil {
		if !s.unprovenRouting.owePurge(id) {
			log.Warn().Err(err).Str("peer", id.String()).Msg("routing_unproven_purge_passed_to_last_legacy_writer")
			return fmt.Errorf("v2 proof of %s: legacy routing input still in flight: %w", id, err)
		}
	}
	s.forgetUnprovenRoutingResidue(id)
	return nil
}

// closeLegacyClaimsOf closes every registered legacy connection, in either
// direction, that names id. The snapshot is taken under peerMu.RLock and the
// sockets are closed after it is released: closing tears the connection down
// through its own reader, which takes peerMu itself.
func (s *Service) closeLegacyClaimsOf(ctx context.Context, id domain.PeerIdentity) {
	var dialled []*peerSession
	var accepted []domain.ConnID
	s.peerMu.RLock()
	for _, session := range s.sessions {
		if session.peerIdentity != id {
			continue
		}
		if _, proven := session.provenIdentity(); !proven {
			dialled = append(dialled, session)
		}
	}
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.identity != id {
			return true
		}
		if !s.connProvenLocked(info.id) {
			accepted = append(accepted, info.id)
		}
		return true
	})
	s.peerMu.RUnlock()

	for _, session := range dialled {
		log.Warn().
			Str("peer", id.String()).
			Str("address", string(session.address)).
			Uint64("conn_id", uint64(session.connID)).
			Msg("legacy_session_closed_identity_proved_v2")
		_ = session.Close()
	}
	network := s.Network()
	for _, connID := range accepted {
		log.Warn().
			Str("peer", id.String()).
			Uint64("conn_id", uint64(connID)).
			Msg("legacy_connection_closed_identity_proved_v2")
		_ = network.Close(ctx, connID)
	}
}

// admitRoutingInput decides whether a routing-plane input from sender may be
// applied: it may, unless it arrived on a connection that proved nothing,
// naming an identity that requires v2. The caller has already charged the
// frame to the sender's own penalty subject (quarantine, announce budget), so
// the connection that sent it still pays for it.
//
// An admitted LEGACY input is registered as in flight for its identity BEFORE
// the check and until the caller calls release, after its last write. That is
// what closes the window between the check and the write: a v2 proof stores
// the pin first and then waits for every in-flight legacy input of the
// identity (onIdentityProvenV2) — an input whose check ran before the pin is
// therefore finished, and its writes on the table, before the proof purges; an
// input whose check runs after the pin is refused. The input also marks its
// identity as written to by an unproven source, which is what the purge goes
// by. A proven sender, or one naming no identity, is admitted with nothing to
// release.
//
// The caller must hold no domain mutex (identityRequiresV2) and must call
// release exactly once, after the input's last write; it must not wait on a
// v2 proof in between.
func (s *Service) admitRoutingInput(sender routingSender, frameType string) (release func(), admitted bool) {
	if _, proven := sender.penalty.provenIdentity(); proven {
		return func() {}, true
	}
	id := sender.identity
	if id.IsZero() {
		// Names nobody: there is no identity whose routes it could write in
		// the name of, and no proof that could have to wait for it.
		return func() {}, true
	}
	s.unprovenRouting.enter(id)
	if s.identityRequiresV2(id) {
		s.unprovenRouting.leave(id)
		log.Debug().
			Str("peer", id.String()).
			Str("penalty_subject", sender.penalty.String()).
			Str("frame_type", frameType).
			Msg("routing_unproven_claim_of_v2_identity_dropped")
		return nil, false
	}
	s.unprovenRouting.note(id, time.Now())
	if s.routingInputAdmittedHook != nil {
		s.routingInputAdmittedHook(sender, frameType)
	}
	return func() { s.releaseRoutingInput(id) }, true
}

// releaseRoutingInput ends one admitted legacy input of id. When it was the
// last one and a cancelled proof left the purge owed to it, it purges here —
// still counted in flight, so a proof waiting meanwhile wakes only after the
// purge. Called with no domain mutex held, like admitRoutingInput.
func (s *Service) releaseRoutingInput(id domain.PeerIdentity) {
	if !s.unprovenRouting.leave(id) {
		return
	}
	log.Info().Str("peer", id.String()).Msg("routing_unproven_owed_purge_run_by_last_legacy_writer")
	s.forgetUnprovenRoutingResidue(id)
	s.unprovenRouting.leave(id)
}

// forgetUnprovenRoutingResidue forgets what legacy connections naming id wrote
// into the routing table and the announce state since the last time it was
// forgotten — unless id already has a live v2 connection, whose routes stand
// (L-RT-2). The table is purged without a wire effect (routing.Table.
// ForgetUplink); the announce state is reset as at a session boundary, which
// also schedules our full sync to id. The identity's own full sync arrives on
// the v2 connection being established — its connect-time full sync — so the
// recovery takes about one exchange; that is the expected time, not a promise.
func (s *Service) forgetUnprovenRoutingResidue(id domain.PeerIdentity) {
	if !s.unprovenRouting.take(id, time.Now()) {
		return
	}
	if s.identityHasProvenConnection(id) {
		log.Info().Str("peer", id.String()).Msg("routing_unproven_residue_kept_v2_session_vouches")
		return
	}
	forgotten := 0
	if s.routingTable != nil {
		forgotten = s.routingTable.ForgetUplink(id)
	}
	if s.announceLoop != nil {
		s.announceLoop.StateRegistry().MarkDisconnected(id)
	}
	// The purge may have taken the direct route a legacy connection had
	// overwritten; while any relay session of id is still counted (the legacy
	// one being closed, or the v2 one) the direct route is re-admitted here,
	// because the v2 session will not be the "first relay session" that would
	// otherwise re-admit it.
	if s.routingTable != nil && s.identityRelaySessionCount(id) > 0 {
		if _, err := s.routingTable.AddDirectPeer(id); err != nil {
			log.Warn().Err(err).Str("peer", id.String()).Msg("routing_unproven_residue_direct_readmit_failed")
		}
	}
	if s.announceLoop != nil {
		s.announceLoop.TriggerUpdate()
	}
	log.Info().
		Str("peer", id.String()).
		Int("claims_forgotten", forgotten).
		Msg("routing_unproven_residue_forgotten_identity_proved_v2")
}

// identityRelaySessionCount is the number of relay-capable sessions counted
// for id. Takes peerMu.RLock.
func (s *Service) identityRelaySessionCount(id domain.PeerIdentity) int {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.identityRelaySessions[id]
}

// unprovenRoutingWriters is what this node knows about legacy connections
// writing routing state in an identity's name: which identities were written
// to and when (the marks a later v2 proof purges by), and which identities
// have legacy routing input admitted and not yet applied (the writes a proof
// must wait for before it purges). One type, one mutex: the two answer the one
// question "what may a legacy connection still have written about X", and a
// proof reads them in one order — drain, then take.
//
// It owns its mutex — a leaf, never held across anything else — instead of
// joining peerMu, because it is touched once per legacy routing frame on the
// receive path, where peerMu is the hottest lock of the node (docs/locking.md,
// "Fields that remain outside this scheme").
type unprovenRoutingWriters struct {
	mu sync.Mutex
	at map[domain.PeerIdentity]time.Time
	// overflowUntil is set when a mark could not be recorded because the
	// marks are full. Until it passes, a missing mark proves nothing: every
	// proof purges (take answers true). A mark is never evicted — the oldest
	// one is not "closest to expiring" when the routes it stands for are
	// refreshed by other writers.
	overflowUntil time.Time
	// inflight counts legacy routing input admitted per identity and not yet
	// released; drained holds the proofs waiting for it to reach zero.
	inflight map[domain.PeerIdentity]int
	drained  map[domain.PeerIdentity][]chan struct{}
	// owed holds the identities whose proof stopped waiting (its context
	// ended) while input was in flight: the purge is owed by the last of
	// those writers, so cancelling a connection attempt never lifts it.
	owed map[domain.PeerIdentity]struct{}
}

// unprovenRoutingMarkTTL is how long a mark matters: the longest any state a
// legacy connection can leave about an identity lives on its own — a claim or
// tombstone (route TTL), a flap hold-down, a black-hole cooldown. Past it
// there is nothing left to forget.
const unprovenRoutingMarkTTL = max(routing.DefaultTTL, routing.MaxHoldDownDuration, routing.BlackHoleCooldown)

// maxUnprovenRoutingMarks bounds the marks. When full, no mark is evicted:
// the store enters overflow instead (overflowUntil).
const maxUnprovenRoutingMarks = 4096

func (m *unprovenRoutingWriters) note(id domain.PeerIdentity, now time.Time) {
	if id.IsZero() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.at == nil {
		m.at = make(map[domain.PeerIdentity]time.Time)
	}
	if _, known := m.at[id]; !known && len(m.at) >= maxUnprovenRoutingMarks {
		if !now.Before(m.overflowUntil) {
			log.Warn().Int("marks", len(m.at)).Msg("routing_unproven_marks_full_every_proof_purges")
		}
		m.overflowUntil = now.Add(unprovenRoutingMarkTTL)
		return
	}
	m.at[id] = now
}

// take consumes id's mark and reports whether legacy routing state about id
// may exist: a live mark, or an overflow during which a mark may be missing.
func (m *unprovenRoutingWriters) take(id domain.PeerIdentity, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	at, marked := m.at[id]
	delete(m.at, id)
	return (marked && now.Sub(at) < unprovenRoutingMarkTTL) || now.Before(m.overflowUntil)
}

// cleanup drops marks past their TTL.
func (m *unprovenRoutingWriters) cleanup(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, at := range m.at {
		if now.Sub(at) >= unprovenRoutingMarkTTL {
			delete(m.at, id)
		}
	}
}

// enter registers legacy routing input for id as admitted and not yet applied.
func (m *unprovenRoutingWriters) enter(id domain.PeerIdentity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight == nil {
		m.inflight = make(map[domain.PeerIdentity]int)
	}
	m.inflight[id]++
}

// leave releases what enter registered, and wakes the proofs waiting for id
// once nothing is in flight. It reports true when the caller was the last
// writer and owes the purge: the purge then stays counted in flight, and the
// caller leaves once more after it.
func (m *unprovenRoutingWriters) leave(id domain.PeerIdentity) (purgeOwed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inflight[id]--
	if m.inflight[id] > 0 {
		return false
	}
	if _, owed := m.owed[id]; owed {
		delete(m.owed, id)
		m.inflight[id] = 1
		return true
	}
	delete(m.inflight, id)
	for _, waiter := range m.drained[id] {
		close(waiter)
	}
	delete(m.drained, id)
	return false
}

// owePurge hands id's purge to its in-flight writers, for a proof that stops
// waiting for them. It reports true when nothing is in flight any more — the
// writers finished meanwhile — and the proof purges itself.
func (m *unprovenRoutingWriters) owePurge(id domain.PeerIdentity) (drained bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight[id] == 0 {
		return true
	}
	if m.owed == nil {
		m.owed = make(map[domain.PeerIdentity]struct{})
	}
	m.owed[id] = struct{}{}
	return false
}

// waitDrained returns once no legacy routing input for id is in flight, or
// with ctx's error. It holds no lock while it waits.
func (m *unprovenRoutingWriters) waitDrained(ctx context.Context, id domain.PeerIdentity) error {
	m.mu.Lock()
	if m.inflight[id] == 0 {
		m.mu.Unlock()
		return nil
	}
	if m.drained == nil {
		m.drained = make(map[domain.PeerIdentity][]chan struct{})
	}
	waiter := make(chan struct{})
	m.drained[id] = append(m.drained[id], waiter)
	m.mu.Unlock()
	select {
	case <-waiter:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
