//go:build linux

package loadstand

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"syscall"

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

// A per-call, private syscall seam. No constructor accepts it and no mutable
// global hook can replace the production operations.
type plannedProbeSocketOps struct {
	socket  func(int, int, int) (int, error)
	option  func(int, int, int, int) error
	bind    func(int, unix.Sockaddr) error
	name    func(int) (unix.Sockaddr, error)
	connect func(int, unix.Sockaddr) error
	close   func(int) error
}

func plannedUnreachable(ctx context.Context, probes LinuxIsolationProbes) error {
	return plannedUnreachableUsing(ctx, probes, plannedProbeSocketOps{
		socket: unix.Socket, option: unix.SetsockoptInt, bind: unix.Bind,
		name: unix.Getsockname, connect: unix.Connect, close: unix.Close,
	})
}

// Diagnostic metadata only; nil remains nil and every original cause is wrapped.
func plannedProbeFailure(role string, family int, target netip.AddrPort, operation string, err error) error {
	if err == nil {
		return nil
	}
	var number syscall.Errno
	errno := 0
	if errors.As(err, &number) {
		errno = int(number)
	}
	return fmt.Errorf("planned-probe role=%s family=%d target=%s operation=%s errno=%d: %w", role, family, target, operation, errno, err)
}

func plannedUnreachableUsing(ctx context.Context, probes LinuxIsolationProbes, ops plannedProbeSocketOps) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrLinuxOwnerBound
	}
	if !probes.valid() {
		return ErrInvalidConfiguration
	}
	for index, target := range []netip.AddrPort{probes.ProductionBootstrap, probes.FallbackPublic, probes.PublicIPv6} {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := plannedProbeConnect(ctx, target, ops); err != nil {
			family := unix.AF_INET
			if target.Addr().Is6() {
				family = unix.AF_INET6
			}
			return plannedProbeFailure([3]string{"production", "fallback", "ipv6"}[index], family, target, "target-check", err)
		}
	}
	return ctx.Err()
}

// IPv6 uses one explicit nonlocal documentation source, not an address/route
// assignment or fallback. The target and the strict routing-error predicate
// remain unchanged. This conditional candidate still requires native proof.
func plannedProbeConnect(ctx context.Context, target netip.AddrPort, ops plannedProbeSocketOps) (err error) {
	if err = ctx.Err(); err != nil {
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
	fd, err := ops.socket(family, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return plannedProbeFailure("endpoint", family, target, "socket", err)
	}
	// Install before every added setup operation. Even a failed option, bind,
	// observation or cancelled context retains the original close error.
	defer func() {
		if closeErr := ops.close(fd); closeErr != nil {
			failure := plannedProbeFailure("endpoint", family, target, "close", closeErr)
			if err == nil {
				err = failure
			} else {
				err = errors.Join(err, failure)
			}
		}
	}()
	if family == unix.AF_INET6 {
		if err = ops.option(fd, unix.IPPROTO_IPV6, unix.IPV6_FREEBIND, 1); err != nil {
			return plannedProbeFailure("endpoint", family, target, "setsockopt-IPV6_FREEBIND", err)
		}
		source := [16]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
		if err = ops.bind(fd, &unix.SockaddrInet6{Addr: source}); err != nil {
			return plannedProbeFailure("endpoint", family, target, "bind", err)
		}
		actual, nameErr := ops.name(fd)
		if nameErr != nil {
			return plannedProbeFailure("endpoint", family, target, "getsockname", nameErr)
		}
		bound, ok := actual.(*unix.SockaddrInet6)
		if !ok || bound == nil || bound.Addr != source || bound.ZoneId != 0 || bound.Port <= 0 || bound.Port > 65535 {
			return plannedProbeFailure("endpoint", family, target, "getsockname-identity", fmt.Errorf("observed_type=%T: %w", actual, errPlannedNetwork))
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	connectErr := ops.connect(fd, address)
	// A timeout, EADDRNOTAVAIL, EINPROGRESS, refusal or successful connect is
	// NOT isolation. Close always runs before the caller can accept a result.
	if connectErr != unix.ENETUNREACH && connectErr != unix.EHOSTUNREACH {
		return plannedProbeFailure("endpoint", family, target, "connect", fmt.Errorf("unexpected_success=%t: %w", connectErr == nil, errors.Join(errPlannedNetwork, connectErr)))
	}
	return nil
}
func (b *linuxPlannedNetworkBackend) originalUnreachable(ctx context.Context, p LinuxIsolationProbes) error {
	fd, current, err := openThreadNetNS()
	if err != nil {
		return plannedNetworkPhaseError("original-probe-open-namespace", err)
	}
	closeErr := unix.Close(fd)
	if closeErr != nil || current != b.origin {
		return plannedOriginIdentityFailure(current, b.origin, closeErr)
	}
	return plannedUnreachable(ctx, p)
}

// Consumes only the identity snapshots and close result already obtained above.
func plannedOriginIdentityFailure(current, expected linuxNamespaceIdentity, closeErr error) error {
	if closeErr == nil && current == expected {
		return nil
	}
	var number syscall.Errno
	errno := 0
	if errors.As(closeErr, &number) {
		errno = int(number)
	}
	return plannedNetworkPhaseError(fmt.Sprintf("original-probe-identity current_dev=%d current_ino=%d expected_dev=%d expected_ino=%d close_errno=%d", current.device, current.inode, expected.device, expected.inode, errno), errors.Join(errPlannedNetwork, closeErr))
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
