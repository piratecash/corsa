//go:build linux

package loadstand

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Each row is a single finite creation attempt, retained BEFORE its syscall.
// kernelIndex is filled once from the exact nonce-tagged kernel object. An
// uncertain create remains owned but cannot make the network ready.
type linuxPlannedResource struct {
	handle            plannedNetworkHandle
	kernelIndex       int
	namespace         *OwnedNetNS
	namespaceIdentity linuxNamespaceIdentity
	nftHandle         uint64
	nftAttempted      bool
	removed           bool
}
type linuxPlannedNetworkBackend struct {
	plan      plannedNetworkPlan
	socket    *plannedRouteSocket
	originFD  int
	origin    linuxNamespaceIdentity
	resources []*linuxPlannedResource
	nft       linuxPlannedNFT
}

func plannedNetworkAlias(nonce LaunchNonce) string {
	return "corsa.loadstand:" + hex.EncodeToString(nonce[:])
}
func newLinuxPlannedNetwork(ctx context.Context, p plannedNetworkPlan, nftBinary string, nftSHA ExecutableSHA256) (*plannedNetwork, error) {
	p, err := copyPlannedNetwork(p)
	if err != nil {
		return nil, err
	}
	if os.Geteuid() != 0 {
		return nil, ErrLinuxOwnerUnsupported
	}
	if err = auditLinuxExecutable(ctx, nftBinary, nftSHA); err != nil {
		return nil, err
	}
	fd, origin, err := openThreadNetNS()
	if err != nil {
		return nil, err
	}
	socket, err := openPlannedRouteSocket(ctx)
	if err != nil {
		return nil, errors.Join(err, unix.Close(fd))
	}
	backend := &linuxPlannedNetworkBackend{plan: p, socket: socket, originFD: fd, origin: origin, nft: linuxPlannedNFT{binary: nftBinary, sha: nftSHA, blocked: &plannedNFTBlockedEvidence{}}}
	network, err := newPlannedNetwork(ctx, p, backend)
	if err != nil {
		return nil, errors.Join(err, socket.close(), unix.Close(fd))
	}
	return network, nil
}
func plannedContainerRestrictions() bool {
	// Only the existing four setup capabilities, no RAW/NET_BIND/PTRACE/etc.
	// Host launcher independently verifies Docker's actual NetworkMode=none;
	// this observer proves the in-container effectful boundary, not Docker API.
	body, err := os.ReadFile("/proc/self/status")
	if err != nil || len(body) > processFileLimit {
		return false
	}
	allowed := uint64(1<<unix.CAP_SETUID | 1<<unix.CAP_SETGID | 1<<unix.CAP_NET_ADMIN | 1<<unix.CAP_SYS_ADMIN)
	seen := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "CapEff:", "CapPrm:", "CapBnd:":
			if seen[fields[0]] {
				return false
			}
			seen[fields[0]] = true
			value, e := strconv.ParseUint(fields[1], 16, 64)
			if e != nil || value&^allowed != 0 {
				return false
			}
		case "CapAmb:":
			if seen[fields[0]] || fields[1] != "0000000000000000" {
				return false
			}
			seen[fields[0]] = true
		case "NoNewPrivs:":
			if seen[fields[0]] || fields[1] != "1" {
				return false
			}
			seen[fields[0]] = true
		}
	}
	if len(seen) != 5 {
		return false
	}
	var walk func(string) bool
	walk = func(path string) bool {
		entries, e := os.ReadDir(path)
		if e != nil {
			return false
		}
		for _, entry := range entries {
			name := path + "/" + entry.Name()
			info, e := entry.Info()
			if e != nil {
				return false
			}
			if info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice == 0 {
				return false
			}
			if entry.IsDir() && name != "/dev/pts" && name != "/dev/shm" && name != "/dev/mqueue" {
				if !walk(name) {
					return false
				}
			}
		}
		return true
	}
	return walk("/dev")
}
func (b *linuxPlannedNetworkBackend) inspect(ctx context.Context) (plannedNetworkInventory, error) {
	links, err := b.socket.links(ctx)
	if err != nil {
		return plannedNetworkInventory{}, err
	}
	routes, err := b.socket.routes(ctx)
	if err != nil {
		return plannedNetworkInventory{}, err
	}
	restrictions := plannedContainerRestrictions()
	result := plannedNetworkInventory{routes: routes, noForbiddenCapabilities: restrictions, noBlockDevices: restrictions}
	for _, link := range links {
		result.links = append(result.links, plannedNetworkLink{name: link.name, alias: link.alias, index: link.index})
	}
	// This is an observable network-none precondition (onlylo/noexternalroute),
	// not a claim that this process queried the host Docker daemon.
	result.networkNone = len(links) == 1 && links[0].name == "lo"
	return result, nil
}
func (b *linuxPlannedNetworkBackend) resource(kind plannedNetworkKind, name string) *linuxPlannedResource {
	for _, r := range b.resources {
		if r.handle.kind == kind && r.handle.name == name && !r.removed {
			return r
		}
	}
	return nil
}
func (b *linuxPlannedNetworkBackend) begin(op plannedNetworkOperation) (*linuxPlannedResource, error) {
	name := op.plan.bridge
	switch op.kind {
	case plannedNamespace:
		name = string(op.member.id)
	case plannedVeth:
		name = op.member.hostLink
	}
	if b.resource(op.kind, name) != nil {
		return nil, errPlannedNetwork
	}
	row := &linuxPlannedResource{handle: plannedNetworkHandle{kind: op.kind, name: name, identity: uint64(len(b.resources) + 1), nonce: b.plan.nonce}}
	b.resources = append(b.resources, row)
	return row, nil
}
func (b *linuxPlannedNetworkBackend) link(ctx context.Context, name string, index int) (plannedKernelLink, bool, error) {
	links, err := b.socket.links(ctx)
	if err != nil {
		return plannedKernelLink{}, false, err
	}
	var found plannedKernelLink
	count := 0
	for _, link := range links {
		if link.name == name || index != 0 && link.index == index {
			if link.name != name || index != 0 && link.index != index || link.alias != plannedNetworkAlias(b.plan.nonce) {
				return plannedKernelLink{}, false, errPlannedNetwork
			}
			found = link
			count++
		}
	}
	if count > 1 {
		return plannedKernelLink{}, false, errPlannedNetwork
	}
	return found, count == 1, nil
}
func (b *linuxPlannedNetworkBackend) retainLink(ctx context.Context, row *linuxPlannedResource, createErr error) error {
	link, found, err := b.link(ctx, row.handle.name, 0)
	if err == nil && found {
		row.kernelIndex = link.index
	}
	if !found && err == nil && createErr == nil {
		err = errPlannedNetwork
	}
	return errors.Join(createErr, err)
}
func (b *linuxPlannedNetworkBackend) apply(ctx context.Context, op plannedNetworkOperation) (plannedNetworkHandle, error) {
	if op.plan.nonce != b.plan.nonce || op.plan.bridge != b.plan.bridge || op.plan.prefix != b.plan.prefix {
		return plannedNetworkHandle{}, errPlannedNetwork
	}
	var row *linuxPlannedResource
	var err error
	switch op.kind {
	case plannedBridge, plannedNamespace, plannedVeth, plannedFirewall:
		row, err = b.begin(op)
		if err != nil {
			return plannedNetworkHandle{}, err
		}
	}
	if row != nil {
		switch op.kind {
		case plannedBridge:
			err = b.retainLink(ctx, row, b.socket.createBridge(ctx, b.plan.bridge, plannedNetworkAlias(b.plan.nonce)))
		case plannedVeth:
			err = b.retainLink(ctx, row, b.socket.createVeth(ctx, op.member.hostLink, op.member.peerLink, plannedNetworkAlias(b.plan.nonce)))
		case plannedNamespace:
			row.namespace, err = CreateOwnedNetNS(ctx)
			if row.namespace != nil {
				row.namespace.mu.Lock()
				row.namespaceIdentity = row.namespace.identity
				origin := row.namespace.origin
				row.namespace.mu.Unlock()
				if err == nil && (row.namespaceIdentity.inode == 0 || origin != b.origin) {
					err = errPlannedNetwork
				}
			}
		case plannedFirewall:
			row.nftHandle, row.nftAttempted, err = b.nft.create(ctx, b.plan)
		}
		return row.handle, err
	}
	ns := b.resource(plannedNamespace, string(op.member.id))
	veth := b.resource(plannedVeth, op.member.hostLink)
	bridge := b.resource(plannedBridge, b.plan.bridge)
	if ns == nil || ns.namespace == nil || veth == nil || bridge == nil || veth.kernelIndex == 0 || bridge.kernelIndex == 0 {
		return plannedNetworkHandle{}, errPlannedNetwork
	}
	link, found, err := b.link(ctx, op.member.hostLink, veth.kernelIndex)
	if err != nil || !found {
		return plannedNetworkHandle{}, errors.Join(errPlannedNetwork, err)
	}
	switch op.kind {
	case plannedAttach:
		if _, exists, e := b.link(ctx, b.plan.bridge, bridge.kernelIndex); e != nil || !exists {
			return plannedNetworkHandle{}, errors.Join(errPlannedNetwork, e)
		}
		err = b.socket.disableIPv6(ctx, link.index)
		if err == nil {
			err = b.socket.setLink(ctx, link.index, unix.IFLA_MASTER, uint32(bridge.kernelIndex))
		}
		if err == nil {
			err = b.socket.up(ctx, link.index)
		}
	case plannedMove:
		peer, exists, e := b.link(ctx, op.member.peerLink, 0)
		if e != nil || !exists {
			return plannedNetworkHandle{}, errors.Join(errPlannedNetwork, e)
		}
		fd, e := ns.namespace.plannedFD(ns.namespaceIdentity)
		if e != nil {
			return plannedNetworkHandle{}, e
		}
		err = b.socket.setLink(ctx, peer.index, unix.IFLA_NET_NS_FD, uint32(fd))
		err = errors.Join(err, unix.Close(fd))
	case plannedAddress:
		_, err = ns.namespace.plannedOperation(ctx, b.plan, op.member, true)
	default:
		err = errPlannedNetwork
	}
	return plannedNetworkHandle{}, err
}
func (b *linuxPlannedNetworkBackend) verify(ctx context.Context, p plannedNetworkPlan) error {
	bridge := b.resource(plannedBridge, p.bridge)
	if bridge == nil || bridge.kernelIndex == 0 {
		return errPlannedNetwork
	}
	// The launcher retains sole kernel mutation custody for this owned network.
	// Every index effect is preceded by exact name/index/nonce revalidation;
	// an index cache alone is never authority over a replacement.
	if _, exists, e := b.link(ctx, p.bridge, bridge.kernelIndex); e != nil || !exists {
		return errors.Join(errPlannedNetwork, e)
	}
	if err := b.socket.disableIPv6(ctx, bridge.kernelIndex); err != nil {
		return err
	}
	if _, exists, e := b.link(ctx, p.bridge, bridge.kernelIndex); e != nil || !exists {
		return errors.Join(errPlannedNetwork, e)
	}
	if err := b.socket.up(ctx, bridge.kernelIndex); err != nil {
		return err
	}
	links, err := b.socket.links(ctx)
	if err != nil || len(links) != len(p.members)+2 {
		return errors.Join(errPlannedNetwork, err)
	}
	for _, member := range p.members {
		veth := b.resource(plannedVeth, member.hostLink)
		ns := b.resource(plannedNamespace, string(member.id))
		if veth == nil || ns == nil || ns.namespace == nil {
			return errPlannedNetwork
		}
		if _, exists, e := b.link(ctx, member.hostLink, veth.kernelIndex); e != nil || !exists {
			return errors.Join(errPlannedNetwork, e)
		}
		if _, e := ns.namespace.plannedOperation(ctx, p, member, false); e != nil {
			return e
		}
	}
	routes, err := b.socket.routes(ctx)
	if err != nil {
		return err
	}
	for _, route := range routes {
		if !plannedLoopbackRoute(route.prefix) {
			return errPlannedNetwork
		}
	}
	firewall := b.resource(plannedFirewall, p.bridge)
	if firewall == nil {
		return errPlannedNetwork
	}
	return b.nft.verify(ctx, p, firewall.nftHandle)
}
func (b *linuxPlannedNetworkBackend) remove(ctx context.Context, h plannedNetworkHandle) error {
	row := b.resource(h.kind, h.name)
	if row == nil || row.handle != h {
		return errPlannedNetwork
	}
	var err error
	switch h.kind {
	case plannedBridge, plannedVeth:
		link, found, e := b.link(ctx, h.name, row.kernelIndex)
		if e != nil {
			return e
		}
		if found {
			err = b.socket.removeLink(ctx, link.index)
		}
	case plannedNamespace:
		if row.namespace != nil {
			row.namespace.mu.Lock()
			identity := row.namespace.identity
			row.namespace.mu.Unlock()
			if row.namespaceIdentity.inode != 0 && identity != row.namespaceIdentity {
				return errPlannedNetwork
			}
			err = row.namespace.Close(ctx)
		}
	case plannedFirewall:
		if row.nftAttempted {
			err = b.nft.remove(ctx, b.plan, row.nftHandle)
		}
	default:
		err = fmt.Errorf("%w: unowned resource kind", errPlannedNetwork)
	}
	if err == nil {
		row.removed = true
	}
	return err
}

// release is after portable close has confirmed every exact resource removed.
func (b *linuxPlannedNetworkBackend) release() error {
	for _, row := range b.resources {
		if !row.removed {
			return ErrStopUnconfirmed
		}
	}
	err := b.socket.close()
	if b.originFD >= 0 {
		fd := b.originFD
		b.originFD = -1
		err = errors.Join(err, unix.Close(fd))
	}
	return err
}

func (b *linuxPlannedNetworkBackend) memberPostflight(ctx context.Context, m plannedNetworkMember) error {
	ns := b.resource(plannedNamespace, string(m.id))
	if ns == nil || ns.namespace == nil {
		return errPlannedNetwork
	}
	if _, err := ns.namespace.plannedOperation(ctx, b.plan, m, false); err != nil {
		return err
	}
	links, err := b.socket.links(ctx)
	if err != nil {
		return err
	}
	for _, link := range links {
		if link.name == "lo" {
			continue
		}
		kind := plannedVeth
		if link.name == b.plan.bridge {
			kind = plannedBridge
		}
		row := b.resource(kind, link.name)
		if row == nil || row.kernelIndex != link.index || link.alias != plannedNetworkAlias(b.plan.nonce) {
			return errPlannedNetwork
		}
	}
	routes, err := b.socket.routes(ctx)
	if err != nil {
		return err
	}
	for _, route := range routes {
		if !plannedLoopbackRoute(route.prefix) {
			return errPlannedNetwork
		}
	}
	firewall := b.resource(plannedFirewall, b.plan.bridge)
	if firewall == nil {
		return errPlannedNetwork
	}
	return b.nft.verify(ctx, b.plan, firewall.nftHandle)
}
