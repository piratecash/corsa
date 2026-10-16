package node

import (
	"net"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

// acceptInbound admits one connection the listener accepted and starts its
// handler, or refuses it. It runs in the accept loop and never blocks on the
// peer: every bound it checks is a counter, and a refusal is answered by
// listenerRefusals without waiting.
//
// Two bounds are taken, in this order, before any per-connection resource
// exists: the bounds of the connection's source class, then the node's
// shared connection budget.
func (s *Service) acceptInbound(conn net.Conn) {
	class := inboundSourceClassOf(conn.RemoteAddr())
	releaseSource, refusal := s.admitInboundSource(class, conn.RemoteAddr())
	if refusal != nil {
		log.Warn().Str("addr", conn.RemoteAddr().String()).Str("source_class", class.String()).Str("reason", refusal.reason.String()).Msg("reject connection")
		s.listenerRefusals.refuse(conn, refusal.reason)
		return
	}

	// The shared ceiling is taken HERE, in the accept loop, before the
	// handler goroutine exists.
	//
	// ⚠️ Review found reserving inside the handler was too late, and the
	// gap is not theoretical: the handler blocks on peerMu, and while it
	// waits this loop keeps accepting. Every socket in that queue is
	// open, holds a descriptor, and is invisible to Budget.Used — so the
	// node holds more connections than its ceiling while reporting that
	// it does not. The source-class bounds do not close it either: the
	// per-IP ones grow with the number of sources.
	//
	// Refusal is answered right here, which is also the cheapest possible
	// point: no handler goroutine, no metered wrapper, no registry entry.
	inboundReservation, budgetErr := s.connBudget.Reserve(connbudget.DirectionInbound)
	if budgetErr != nil {
		log.Warn().
			Err(budgetErr).
			Str("addr", conn.RemoteAddr().String()).
			Str("source_class", class.String()).
			Str("reason", refusalConnectionBudget.String()).
			Msg("reject connection")
		s.listenerRefusals.refuse(conn, refusalConnectionBudget)
		releaseSource()
		return
	}

	s.connWg.Add(1)
	// lifecycle: joined by connWg, not runLoopsWg. A connection handler is
	// per-CONNECTION rather than per-Run: it ends when its socket closes,
	// and the teardown that owns it is the closeAllInboundConns +
	// connWg.Wait defer in Run, which must run at its own point in the
	// order. Tracking it in the lifecycle group as well would give one
	// goroutine two owners.
	go func(c net.Conn, reservation *connbudget.Reservation) {
		defer s.connWg.Done()
		defer crashlog.DeferRecover()
		defer releaseSource()
		// Release covers every early exit of the handler — blacklist,
		// registration refusal, a socket that says nothing. Once the
		// connection is registered the entry owns the unit and releases
		// it on unregister; Release is idempotent, so the two cannot
		// double-free between them.
		defer reservation.Release()
		s.handleConn(c, reservation)
	}(conn, inboundReservation)
}

// admitInboundSource takes the slot of an accepted connection in the bounds
// of its source class, or names the bound that refuses it. The
// returned release gives the slot back; calling it more than once gives back
// nothing more.
func (s *Service) admitInboundSource(class domain.InboundSourceClass, addr net.Addr) (func(), *listenerRefusalError) {
	switch class {
	case domain.InboundSourceLoopback:
		return s.loopbackInbound.admit(!s.disableRateLimiting)
	default:
		return s.admitExternalSource(remoteIP(addr))
	}
}

// admitExternalSource applies the per-IP bounds of an external source: the
// connection rate, checked before anything is allocated so a flood from one
// IP costs nothing, and the cap on the connections one IP holds at once.
// Both are off on a node whose per-IP limits are switched off.
func (s *Service) admitExternalSource(ip string) (func(), *listenerRefusalError) {
	if s.disableRateLimiting {
		return func() {}, nil
	}
	if !s.connLimiter.allowConnect(ip) {
		return nil, &listenerRefusalError{reason: refusalConnectionRate}
	}
	if !s.tryIncrementIPConn(ip) {
		return nil, &listenerRefusalError{reason: refusalConnectionsPerIP}
	}
	var once sync.Once
	return func() { once.Do(func() { s.decrementIPConn(ip) }) }, nil
}
