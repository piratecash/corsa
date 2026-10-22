//go:build linux

package loadstand

import (
	"context"
	"errors"
	"net/netip"

	"golang.org/x/sys/unix"
)

// The existing plan supplies the three required negative targets. These are
// connection probes, never routes or permission to select a public fallback.
type LinuxIsolationProbes struct{ ProductionBootstrap, FallbackPublic, PublicIPv6 netip.AddrPort }

func (p LinuxIsolationProbes) valid() bool {
	for _, target := range []netip.AddrPort{p.ProductionBootstrap, p.FallbackPublic} {
		a := target.Addr()
		if !a.Is4() || target.Port() == 0 || !a.IsGlobalUnicast() || a.IsPrivate() || a.IsLoopback() || a.IsLinkLocalUnicast() || plannedApprovedAddress(a) {
			return false
		}
	}
	a := p.PublicIPv6.Addr()
	return a.Is6() && !a.Is4In6() && a.IsGlobalUnicast() && !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() && p.PublicIPv6.Port() != 0
}
func plannedUnreachable(ctx context.Context, probes LinuxIsolationProbes) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrLinuxOwnerBound
	}
	if !probes.valid() {
		return ErrInvalidConfiguration
	}
	for _, target := range []netip.AddrPort{probes.ProductionBootstrap, probes.FallbackPublic, probes.PublicIPv6} {
		if err := ctx.Err(); err != nil {
			return err
		}
		family := unix.AF_INET
		var address unix.Sockaddr
		if target.Addr().Is4() {
			raw := target.Addr().As4()
			address = &unix.SockaddrInet4{Addr: raw, Port: int(target.Port())}
		} else {
			family = unix.AF_INET6
			raw := target.Addr().As16()
			address = &unix.SockaddrInet6{Addr: raw, Port: int(target.Port())}
		}
		fd, err := unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
		if err != nil {
			return err
		}
		connectErr := unix.Connect(fd, address)
		closeErr := unix.Close(fd)
		// A timeout, EINPROGRESS, refusal or successfulconnect is NOT isolation.
		// Only the required immediate routing errors qualify, for every target.
		if connectErr != unix.ENETUNREACH && connectErr != unix.EHOSTUNREACH {
			return errors.Join(errPlannedNetwork, connectErr, closeErr)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return ctx.Err()
}
func (b *linuxPlannedNetworkBackend) originalUnreachable(ctx context.Context, p LinuxIsolationProbes) error {
	fd, current, err := openThreadNetNS()
	if err != nil {
		return err
	}
	closeErr := unix.Close(fd)
	if closeErr != nil || current != b.origin {
		return errors.Join(errPlannedNetwork, closeErr)
	}
	return plannedUnreachable(ctx, p)
}
func (n *OwnedNetNS) plannedProbe(ctx context.Context, probes LinuxIsolationProbes) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrLinuxOwnerBound
	}
	n.mu.Lock()
	identity := n.identity
	n.mu.Unlock()
	request := &linuxPlannedNamespaceOperation{identity: identity, probes: &probes}
	operation := linuxNamespaceOperation{ctx: ctx, planned: request, result: make(chan error, 1)}
	if err := n.submit(ctx, operation); err != nil {
		return err
	}
	select {
	case err := <-operation.result:
		return err
	case <-ctx.Done():
		return errors.Join(ErrStopUnconfirmed, ctx.Err())
	}
}
