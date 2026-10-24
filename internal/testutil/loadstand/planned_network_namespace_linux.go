//go:build linux

package loadstand

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// A closed typed operation, not a callback on the namespace worker. All fields
// are copied before submission. The worker alone publishes the observation.
type linuxPlannedNamespaceOperation struct {
	probes    *LinuxIsolationProbes
	identity  linuxNamespaceIdentity
	member    plannedNetworkMember
	plan      plannedNetworkPlan
	configure bool
	links     []plannedKernelLink
	routes    []plannedNetworkRoute
}

func (n *OwnedNetNS) plannedOperation(ctx context.Context, p plannedNetworkPlan, m plannedNetworkMember, configure bool) (*linuxPlannedNamespaceOperation, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, ErrLinuxOwnerBound
	}
	copy, err := copyPlannedNetwork(p)
	if err != nil {
		return nil, err
	}
	found := false
	for _, candidate := range copy.members {
		if candidate == m {
			found = true
		}
	}
	if !found {
		return nil, plannedNetworkPhaseError("planned_network_namespace_linux:plannedOperation:guard1", errPlannedNetwork)
	}
	n.mu.Lock()
	identity := n.identity
	claimed := n.claimed
	n.mu.Unlock()
	// Configuration is before the fixture claims its original namespace. A
	// read-only postflight can run later through the same one-operation worker.
	if configure && claimed {
		return nil, plannedNetworkPhaseError("planned_network_namespace_linux:plannedOperation:guard2", errPlannedNetwork)
	}
	request := &linuxPlannedNamespaceOperation{identity: identity, member: m, plan: copy, configure: configure}
	op := linuxNamespaceOperation{ctx: ctx, planned: request, result: make(chan error, 1)}
	if err = n.submit(ctx, op); err != nil {
		return nil, err
	}
	// Once handed off, only the worker can finish using request. On cancellation
	// wait for its bounded syscall result before reading the observation fields.
	select {
	case err = <-op.result:
		return request, err
	case <-ctx.Done():
		return nil, errors.Join(ErrStopUnconfirmed, ctx.Err())
	}
}
func (n *OwnedNetNS) plannedInNamespace(ctx context.Context, op *linuxPlannedNamespaceOperation) error {
	actual, err := namespaceIdentity(n.fd)
	if err != nil || actual != op.identity || actual != n.identity {
		return errors.Join(errPlannedNetwork, err)
	}
	if op.probes != nil {
		return plannedUnreachable(ctx, *op.probes)
	}
	socket, err := openPlannedRouteSocket(ctx)
	if err != nil {
		return plannedNetworkPhaseError("namespace-open-route-socket", err)
	}
	defer func() { _ = socket.close() }()
	links, err := socket.links(ctx)
	if err != nil {
		return plannedNetworkPhaseError("namespace-links", err)
	}
	if len(links) != 2 {
		return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard1", errPlannedNetwork)
	}
	peer := 0
	for _, link := range links {
		if link.name == "lo" {
			continue
		}
		if link.name != op.member.peerLink || link.alias != plannedNetworkAlias(op.plan.nonce) || peer != 0 {
			return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard2", errPlannedNetwork)
		}
		peer = link.index
	}
	if peer == 0 {
		return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard3", errPlannedNetwork)
	}
	if op.configure {
		for _, link := range links {
			if err = socket.disableIPv6(ctx, link.index); err != nil {
				return plannedNetworkPhaseError(fmt.Sprintf("namespace-disable-ipv6 index=%d", link.index), err)
			}
		}
		if err = socket.address(ctx, peer, op.member.address, op.plan.prefix.Bits()); err != nil {
			return plannedNetworkPhaseError(fmt.Sprintf("namespace-address index=%d", peer), err)
		}
		if err = socket.up(ctx, peer); err != nil {
			return plannedNetworkPhaseError(fmt.Sprintf("namespace-up index=%d", peer), err)
		}
		// Only this setup operation reconciles the kernel-created multicast
		// route. configure=false/postflight remain strictly read-only.
		owned := []plannedKernelLink{{index: peer, name: op.member.peerLink, alias: plannedNetworkAlias(op.plan.nonce)}}
		if err = reconcilePlannedMulticast(ctx, socket, owned); err != nil {
			return plannedNetworkPhaseError("namespace-owned-multicast", err)
		}
	}
	routes, err := socket.routes(ctx)
	if err != nil {
		return plannedNetworkPhaseError("namespace-routes", err)
	}
	connected := false
	for _, route := range routes {
		if plannedLoopbackRoute(route.prefix) {
			continue
		}
		// Every selected-prefix subroute stays in240/4; no gateway/default or
		// arbitrary public range can become namespace routing authority.
		if !plannedApprovedPrefix(route.prefix) || route.prefix.Bits() < op.plan.prefix.Bits() || !op.plan.prefix.Contains(route.prefix.Addr()) {
			return plannedNetworkPhaseError(fmt.Sprintf(
				"planned_network_namespace_linux:plannedInNamespace:guard4 rejected_prefix=%s selected_prefix=%s configure=%t peer_ifindex=%d",
				route.prefix, op.plan.prefix, op.configure, peer), errPlannedNetwork)
		}
		if route.prefix == op.plan.prefix {
			connected = true
		}
	}
	if !connected {
		return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard5", errPlannedNetwork)
	}
	addresses, err := socket.addresses(ctx)
	if err != nil {
		return plannedNetworkPhaseError("namespace-addresses", err)
	}
	addressCount := 0
	for _, address := range addresses {
		if address.prefix.Addr().Is6() {
			return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard6", errPlannedNetwork)
		}
		if address.index == peer {
			if address.prefix.Addr() != op.member.address || address.prefix.Bits() != op.plan.prefix.Bits() {
				return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard7", errPlannedNetwork)
			}
			addressCount++
		} else if !address.prefix.Addr().IsLoopback() {
			return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard8", errPlannedNetwork)
		}
	}
	if addressCount != 1 {
		return plannedNetworkPhaseError("planned_network_namespace_linux:plannedInNamespace:guard9", errPlannedNetwork)
	}
	op.links = links
	op.routes = routes
	return nil
}

// Snapshot the FD under its existing owner lock; the caller uses it only while
// setup owns the namespace and before any fixture can claim or close it.
func (n *OwnedNetNS) plannedFD(expected linuxNamespaceIdentity) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.poisoned || n.claimed || n.fd < 0 || n.identity != expected {
		return -1, plannedNetworkPhaseError("planned_network_namespace_linux:plannedFD:guard1", errPlannedNetwork)
	}
	actual, err := namespaceIdentity(n.fd)
	if err != nil || actual != expected {
		return -1, errors.Join(errPlannedNetwork, err)
	}
	fd, err := unix.FcntlInt(uintptr(n.fd), unix.F_DUPFD_CLOEXEC, 0)
	return fd, err
}
