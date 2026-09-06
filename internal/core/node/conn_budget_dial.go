package node

import (
	"context"
	"net"
	"time"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
)

// conn_budget_dial.go is the admission gate for the outbound sockets that do
// NOT belong to a connection-manager slot.
//
// ⚠️ Review found these paths open real TCP connections while the shared
// ceiling was already exhausted, and they were invisible in the budget
// diagnostic — so "hard shared ceiling" was not true, it was true only of the
// paths that had been wired. A ceiling with side doors is a ceiling nobody can
// reason about.
//
// The paths covered here are short-lived by nature (a key-recovery sync, a
// notice delivered to a peer with no live connection), but "short-lived" is
// not "free": each one is a descriptor and a buffer for as long as it runs,
// and a burst of them is exactly what a limit is for.
//
// ⚠️ These dials draw on DirectionAuxiliary, NOT on the outbound peer limit.
// Review found the first version charged them to the same limit as the
// connection manager's slots, which in the steady state is fully occupied by
// design — so a sender-key sync was refused permanently, and waiting could not
// help. They are still counted in the shared ceiling B; what they no longer do
// is compete for the node's persistent neighbourhood.
//
// ⚠️ NOT covered, deliberately: openPeerSessionForCM. That dial is already
// paid for by the slot reservation the connection manager takes before it
// starts (slot.reservation), and charging it twice would halve the effective
// ceiling.

// budgetedConn ties one unit of the shared ceiling to the lifetime of one
// socket. Release happens in Close, which every caller of these paths already
// runs — the alternative, a separate release call beside each Close, is the
// kind of pairing that survives review and then rots at the third call site.
//
// Reservation.Release is idempotent, so a double Close cannot free capacity
// twice.
type budgetedConn struct {
	net.Conn
	reservation *connbudget.Reservation
}

// Close closes the socket and returns its capacity, in that order: releasing
// first would let another attempt occupy the unit while this socket is still
// open, which is the overshoot the ceiling exists to prevent.
func (c *budgetedConn) Close() error {
	err := c.Conn.Close()
	c.reservation.Release()
	return err
}

// dialPeerWithBudget is dialPeer plus admission: it takes a unit of the shared
// ceiling BEFORE the dial and hands it to the socket, so a refusal means no
// socket is opened at all.
//
// The returned conn releases the unit on Close. Callers that already close the
// connection — directly or through a NetCore wrapper that owns it — need no
// other change.
func (s *Service) dialPeerWithBudget(ctx context.Context, address domain.PeerAddress, timeout time.Duration) (net.Conn, error) {
	reservation, err := s.connBudget.Reserve(connbudget.DirectionAuxiliary)
	if err != nil {
		return nil, err
	}

	conn, err := s.dialPeer(ctx, address, timeout)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	return &budgetedConn{Conn: conn, reservation: reservation}, nil
}

// dialAddressWithBudget is the same gate for the one path that bypasses
// dialPeer entirely and calls the net package directly.
func (s *Service) dialAddressWithBudget(address domain.PeerAddress, timeout time.Duration) (net.Conn, error) {
	reservation, err := s.connBudget.Reserve(connbudget.DirectionAuxiliary)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	return &budgetedConn{Conn: conn, reservation: reservation}, nil
}
