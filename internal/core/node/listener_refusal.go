package node

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// listener_refusal.go is how the listener says no (docs/protocol/session_v2.md,
// "Listener refusals"). A connection refused before any session exists gets
// sessionv2.RefusalAlert instead of a silent close: a dialler of this build
// then knows a node that speaks TLS refused it, rather than guessing from a
// bare EOF; an old v1 dialler reads the same EOF as before.

// listenerRefusalReason is the bound that refused a connection.
type listenerRefusalReason int

const (
	refusalConnectionRate listenerRefusalReason = iota
	refusalConnectionsPerIP
	refusalConnectionBudget
	refusalBlacklisted
	refusalInboundLimit
	refusalHandshakesNode
	refusalHandshakesPerIP
	refusalHandshakesLoopback
	refusalLoopbackConnectionRate
	refusalLoopbackConnections
	listenerRefusalReasons
)

var listenerRefusalReasonNames = [listenerRefusalReasons]string{
	refusalConnectionRate:         "conn-rate-limit",
	refusalConnectionsPerIP:       "max-conn-per-ip",
	refusalConnectionBudget:       "connection-budget",
	refusalBlacklisted:            "blacklisted",
	refusalInboundLimit:           "inbound-limit",
	refusalHandshakesNode:         "handshakes-node-limit",
	refusalHandshakesPerIP:        "handshakes-ip-limit",
	refusalHandshakesLoopback:     "handshakes-loopback-limit",
	refusalLoopbackConnectionRate: "loopback-conn-rate-limit",
	refusalLoopbackConnections:    "loopback-max-conn",
}

func (r listenerRefusalReason) String() string {
	if r < 0 || r >= listenerRefusalReasons {
		return fmt.Sprintf("listener-refusal(%d)", int(r))
	}
	return listenerRefusalReasonNames[r]
}

// listenerRefusalError is a connection the listener refused by one of its
// bounds. It travels up from where the bound is checked to the one place
// that answers the connection, listenerRefusals.refuse.
type listenerRefusalError struct {
	reason listenerRefusalReason
}

func (e *listenerRefusalError) Error() string {
	return "listener refused the connection: " + e.reason.String()
}

// alertPoolBounds bound the alert senders in flight, and so the goroutines
// and descriptors refusals hold.
type alertPoolBounds struct {
	// total bounds the senders on the node: a flood of refused connections
	// costs at most this many sockets for at most one delivery each.
	total int
	// perSource bounds the senders one source — an IPv4 address or an IPv6
	// /64 — holds, so one source cannot take the whole pool from the rest.
	perSource int
}

// productionAlertPoolBounds are the bounds a node runs with.
func productionAlertPoolBounds() alertPoolBounds {
	return alertPoolBounds{total: 256, perSource: 8}
}

const (
	// refusalAlertWriteTimeout bounds the write of the seven alert bytes; a
	// peer that does not take them in this time does not get them.
	refusalAlertWriteTimeout = 100 * time.Millisecond
	// refusalAlertDrainTimeout and refusalAlertDrainBytes bound the read of
	// what the peer already sent. Closing a socket with unread input makes
	// the kernel answer with a reset, and a reset may discard the alert at
	// the peer before it reads it; reading that input first is what lets the
	// close be a plain FIN.
	refusalAlertDrainTimeout = 200 * time.Millisecond
	refusalAlertDrainBytes   = 4 << 10
)

// listenerRefusals answers refused connections and counts them. The counters
// are atomics and the senders a channel semaphore; the per-source share is a
// map under a leaf mutex of its own, with nothing called under it. No domain
// mutex is taken, so refuse may be called from the accept loop itself.
type listenerRefusals struct {
	// senders is a semaphore of alert senders: a slot is taken without
	// waiting, and a refusal that finds none closes at once.
	senders chan struct{}
	// inFlight joins the alert senders: each is owned by this struct and
	// waited for by Run's teardown (wait).
	inFlight sync.WaitGroup

	perSource       int
	sourceMu        sync.Mutex
	sendersBySource map[string]int

	startedAt time.Time
	refused   [listenerRefusalReasons]atomic.Uint64
	// refusedLoopback is the part of refused that loopback sources took,
	// read by the loopback inbound class's diagnostics.
	refusedLoopback    [listenerRefusalReasons]atomic.Uint64
	closedWithoutAlert atomic.Uint64
}

// newListenerRefusals builds the refusal path within bounds.
func newListenerRefusals(bounds alertPoolBounds, startedAt time.Time) *listenerRefusals {
	return &listenerRefusals{
		senders:         make(chan struct{}, bounds.total),
		perSource:       bounds.perSource,
		sendersBySource: make(map[string]int),
		startedAt:       startedAt,
	}
}

// refuse answers conn, refused by reason, and takes ownership of it: conn is
// closed in every case. It never blocks — the alert goes out from a sender
// of the bounded pool, and when the pool or the source's share of it is full
// the connection is closed at once without it, which tells the peer no less
// than the silent close it replaces.
func (r *listenerRefusals) refuse(conn net.Conn, reason listenerRefusalReason) {
	source := handshakeSourceOf(conn.RemoteAddr(), true)
	r.count(reason, source)
	if !r.takeSender(source) {
		r.closedWithoutAlert.Add(1)
		// The connection is refused either way; a close error has nobody
		// to tell.
		_ = conn.Close()
		return
	}
	// lifecycle: owned by this struct and joined by wait, which Run's
	// teardown calls after connWg — once no handler can refuse any more. The
	// sender is bounded by the write and drain deadlines, not by a context:
	// shutdown waits at most that long for it.
	r.inFlight.Go(func() {
		defer r.releaseSender(source)
		defer crashlog.DeferRecover()
		if err := sendRefusalAlert(conn); err != nil {
			log.Debug().Err(err).Str("addr", conn.RemoteAddr().String()).Str("reason", reason.String()).Msg("listener_refusal_alert_not_delivered")
		}
	})
}

// takeSender takes a sender for source: its share first, then the pool. A
// loopback source — the onion entry, many clients behind 127.0.0.1 — has no
// share of its own and is bounded by the pool alone.
func (r *listenerRefusals) takeSender(source handshakeSource) bool {
	r.sourceMu.Lock()
	defer r.sourceMu.Unlock()
	if !source.loopback && r.sendersBySource[source.group] >= r.perSource {
		return false
	}
	select {
	case r.senders <- struct{}{}:
	default:
		return false
	}
	if !source.loopback {
		r.sendersBySource[source.group]++
	}
	return true
}

func (r *listenerRefusals) releaseSender(source handshakeSource) {
	r.sourceMu.Lock()
	defer r.sourceMu.Unlock()
	<-r.senders
	if source.loopback {
		return
	}
	if r.sendersBySource[source.group] > 1 {
		r.sendersBySource[source.group]--
		return
	}
	delete(r.sendersBySource, source.group)
}

// closeRefusedSecure closes conn, refused by reason after its v2 handshake
// finished: the TLS session is up, and a plaintext alert record inside it is
// not something the dialler can read as a refusal. Counted like any refusal;
// it never takes an alert sender.
func (r *listenerRefusals) closeRefusedSecure(conn net.Conn, reason listenerRefusalReason) {
	r.count(reason, handshakeSourceOf(conn.RemoteAddr(), true))
	// The connection is refused either way; a close error has nobody to
	// tell.
	_ = conn.Close()
}

// count counts a refusal of source by reason.
func (r *listenerRefusals) count(reason listenerRefusalReason, source handshakeSource) {
	r.refused[reason].Add(1)
	if source.loopback {
		r.refusedLoopback[reason].Add(1)
	}
}

// loopbackRefused is how many loopback connections reason refused. Lock-free.
func (r *listenerRefusals) loopbackRefused(reason listenerRefusalReason) uint64 {
	return r.refusedLoopback[reason].Load()
}

// wait returns once every alert sender has finished. Each finishes within
// the write and drain bounds, so the wait is bounded.
func (r *listenerRefusals) wait() {
	r.inFlight.Wait()
}

// stats reads the counters. Lock-free; reading resets nothing.
func (r *listenerRefusals) stats(readAt time.Time) domain.ListenerRefusalStats {
	return domain.ListenerRefusalStats{
		StartedAt:              r.startedAt,
		ReadAt:                 readAt,
		ConnectionRate:         r.refused[refusalConnectionRate].Load(),
		ConnectionsPerIP:       r.refused[refusalConnectionsPerIP].Load(),
		ConnectionBudget:       r.refused[refusalConnectionBudget].Load(),
		Blacklisted:            r.refused[refusalBlacklisted].Load(),
		InboundLimit:           r.refused[refusalInboundLimit].Load(),
		HandshakesNode:         r.refused[refusalHandshakesNode].Load(),
		HandshakesPerIP:        r.refused[refusalHandshakesPerIP].Load(),
		HandshakesLoopback:     r.refused[refusalHandshakesLoopback].Load(),
		LoopbackConnectionRate: r.refused[refusalLoopbackConnectionRate].Load(),
		LoopbackConnections:    r.refused[refusalLoopbackConnections].Load(),
		ClosedWithoutAlert:     r.closedWithoutAlert.Load(),
	}
}

// sendRefusalAlert writes the alert, half-closes, reads what the peer had
// already sent within the drain bounds and closes conn. conn is closed on
// every path; the error says how far the delivery got.
func sendRefusalAlert(conn net.Conn) (err error) {
	defer func() { err = errors.Join(err, conn.Close()) }()
	if err := conn.SetWriteDeadline(time.Now().Add(refusalAlertWriteTimeout)); err != nil {
		return fmt.Errorf("refusal alert write deadline: %w", err)
	}
	if _, err := conn.Write(sessionv2.RefusalAlert()); err != nil {
		return fmt.Errorf("refusal alert write: %w", err)
	}
	// The FIN goes out right behind the alert, so the peer reads an end
	// instead of waiting for the drain to finish. A socket that cannot
	// half-close is still drained: the drain is what keeps the close from
	// turning into a reset that destroys the alert.
	if halfCloser, ok := conn.(interface{ CloseWrite() error }); ok {
		if err := halfCloser.CloseWrite(); err != nil && !errors.Is(err, netcore.ErrNoHalfClose) {
			return fmt.Errorf("refusal alert half-close: %w", err)
		}
	}
	if err := conn.SetReadDeadline(time.Now().Add(refusalAlertDrainTimeout)); err != nil {
		return fmt.Errorf("refusal alert drain deadline: %w", err)
	}
	// The drain ends at the peer's own end, at the byte bound or at the
	// deadline; any of them is as far as it is meant to go, so how it ended
	// is no failure of the delivery.
	_, _ = io.CopyN(io.Discard, conn, refusalAlertDrainBytes)
	return nil
}

// closeUnregisteredInbound answers a connection registerInboundConn refused
// with err. The inbound limit is a listener refusal: a v1 connection, to
// which nothing has been written yet, gets the alert, and a v2 one is closed
// (closeRefusedSecure). Production reaches this bound only in a race the
// shared connection budget does not leave open — the budget holds the same
// inbound bound from accept on, before the first byte — so it is the defence
// behind that one. Every other refusal (shutdown drain, a Network override)
// is the node's own state, not a bound the peer hit, and is a plain close.
func (s *Service) closeUnregisteredInbound(wire net.Conn, secure bool, err error) {
	switch {
	case !errors.Is(err, errInboundLimitReached):
		// Not a refusal of the peer; a close error has nobody to tell.
		_ = wire.Close()
	case secure:
		s.listenerRefusals.closeRefusedSecure(wire, refusalInboundLimit)
	default:
		s.listenerRefusals.refuse(wire, refusalInboundLimit)
	}
}

// ListenerRefusalStats returns the listener's refusals by bound since the
// process started. Lock-free. Implements rpc.RoutingProvider.
func (s *Service) ListenerRefusalStats() domain.ListenerRefusalStats {
	return s.listenerRefusals.stats(time.Now().UTC())
}
