//go:build linux

package loadstand

import (
	"context"
	"net"
	"net/netip"
)

type linuxDialPhase uint8

const (
	linuxDialPending linuxDialPhase = iota
	linuxDialOffered
	linuxDialTransferred
	linuxDialCancelled
)

type linuxDialOffer struct {
	phase  linuxDialPhase
	conn   *net.TCPConn
	notice chan struct{}
	ack    chan struct{}
}

func (n *OwnedNetNS) dialInNamespace(operation linuxNamespaceOperation) error {
	remote := operation.owner.remote
	dialer := net.Dialer{}
	dialer.SetMultipathTCP(false)
	conn, err := dialer.DialTCP(operation.ctx, "tcp4", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0), remote)
	if err != nil {
		return err
	}
	operation.dial.conn = conn // worker remains sole owner through restoration
	return nil
}
func (o *LinuxFixture) offerDial(ctx context.Context, offer *linuxDialOffer) error {
	o.mu.Lock()
	if o.stopping || ctx.Err() != nil {
		o.mu.Unlock()
		return offer.conn.Close()
	}
	offer.phase = linuxDialOffered
	o.mu.Unlock()
	select {
	case offer.notice <- struct{}{}:
		select {
		case <-offer.ack:
		case <-ctx.Done():
		case <-o.stoppingDone:
		}
	case <-ctx.Done():
	case <-o.stoppingDone:
	}
	o.mu.Lock()
	transferred := offer.phase == linuxDialTransferred
	if !transferred {
		offer.phase = linuxDialCancelled
	}
	o.mu.Unlock()
	if !transferred {
		return offer.conn.Close()
	}
	return nil
}
func (o *LinuxFixture) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" || address != o.remote.String() {
		return nil, ErrLinuxOwnerProtocol
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrLinuxOwnerBound
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	available := o.running && !o.stopping && !o.correlation.poisoned
	o.mu.Unlock()
	if !available {
		return nil, ErrLinuxOwnerPoisoned
	}
	offer := &linuxDialOffer{notice: make(chan struct{}), ack: make(chan struct{})}
	operation := linuxNamespaceOperation{ctx: ctx, owner: o, dial: offer, result: make(chan error, 1)}
	if err := o.namespace.submit(ctx, operation); err != nil {
		return nil, err
	}
	select {
	case <-offer.notice:
		o.mu.Lock()
		if !o.stopping && !o.correlation.poisoned && ctx.Err() == nil && offer.phase == linuxDialOffered {
			offer.phase = linuxDialTransferred
			conn := offer.conn
			close(offer.ack)
			o.mu.Unlock()
			return conn, nil // no cancellation check after actual transfer
		}
		offer.phase = linuxDialCancelled
		close(offer.ack)
		o.mu.Unlock()
		return nil, ErrLinuxOwnerPoisoned
	case err := <-operation.result:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.stoppingDone:
		return nil, ErrLinuxOwnerPoisoned
	}
}
