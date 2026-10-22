package loadstand

import (
	"context"
	"errors"
	"net/netip"
)

var errPlannedNetwork = errors.New("planned loadstand network unavailable")

// Input from the existing immutable plan, not a kernel admission receipt.
// A constructor copies every member before the first backend operation.
type plannedNetworkPlan struct {
	nonce   LaunchNonce
	bridge  string
	prefix  netip.Prefix
	members []plannedNetworkMember
}
type plannedNetworkRoute struct{ prefix netip.Prefix }
type plannedNetworkInventory struct {
	networkNone                             bool
	noForbiddenCapabilities, noBlockDevices bool
	links                                   []plannedNetworkLink
	routes                                  []plannedNetworkRoute
}

type plannedNetworkKind uint8

const (
	plannedBridge plannedNetworkKind = iota + 1
	plannedNamespace
	plannedVeth
	plannedAttach
	plannedMove
	plannedAddress
	plannedFirewall
)

type plannedNetworkOperation struct {
	kind   plannedNetworkKind
	plan   plannedNetworkPlan
	member plannedNetworkMember
}

// Only resource-creating operations return a private creation handle. The
// backend separately retains the kernel identity (or an unresolved attempt).
// A backend must return this handle
// even on a later error, so partial setup never loses an already-created owner.
// Mutable setup operations may address only handles already owned by this run.
type plannedNetworkHandle struct {
	kind     plannedNetworkKind
	name     string
	identity uint64
	nonce    LaunchNonce
}

func (h plannedNetworkHandle) empty() bool { return h == (plannedNetworkHandle{}) }
func (h plannedNetworkHandle) belongs(p plannedNetworkPlan) bool {
	if h.empty() || h.identity == 0 || h.nonce != p.nonce {
		return false
	}
	switch h.kind {
	case plannedBridge, plannedFirewall:
		return h.name == p.bridge
	case plannedNamespace, plannedVeth:
		for _, m := range p.members {
			if (h.kind == plannedNamespace && h.name == string(m.id)) || (h.kind == plannedVeth && h.name == m.hostLink) {
				return true
			}
		}
	}
	return false
}

// This is a private finite syscall boundary for the real Linux implementation
// and fake tests. No exported constructor accepts a caller-supplied backend.
type plannedNetworkBackend interface {
	inspect(context.Context) (plannedNetworkInventory, error)
	apply(context.Context, plannedNetworkOperation) (plannedNetworkHandle, error)
	verify(context.Context, plannedNetworkPlan) error
	remove(context.Context, plannedNetworkHandle) error
}

type plannedNetwork struct {
	plan      plannedNetworkPlan
	backend   plannedNetworkBackend
	owned     []plannedNetworkHandle
	ready     bool
	attempted bool
	closed    bool
}

func plannedLiteralName(s string) bool {
	if s == "" {
		return false
	}
	// Restricted literal identifier subset: never a namespace path or tool flag.
	for i, c := range s {
		valid := c >= 'a' && c <= 'z' || c == '_' || i > 0 && (c >= '0' && c <= '9' || c == '-')
		if !valid {
			return false
		}
	}
	return true
}
func plannedLinkName(s string) bool {
	// IFNAMSIZ includes its terminating NUL; this is a kernel constraint.
	return len(s) < 16 && s != "lo" && plannedLiteralName(s)
}
func plannedApprovedPrefix(p netip.Prefix) bool {
	// T5 decision9г/plan6 explicitly selects 240/4. RFC1918 is not a substitute
	// for that source-address classification experiment or an automatic fallback.
	if !p.IsValid() || p != p.Masked() || !p.Addr().Is4() || p.Bits() < 4 {
		return false
	}
	return p.Addr().As4()[0] >= 240
}
func plannedApprovedAddress(a netip.Addr) bool {
	return a.Is4() && a.As4()[0] >= 240 && a != netip.AddrFrom4([4]byte{255, 255, 255, 255})
}

func plannedLoopbackRoute(p netip.Prefix) bool {
	if !p.IsValid() || p != p.Masked() {
		return false
	}
	if p.Addr().Is4() {
		return p.Addr().As4()[0] == 127 && p.Bits() >= 8
	}
	return p.Addr() == netip.IPv6Loopback() && p.Bits() == 128
}
func (h plannedNetworkHandle) matchesOperation(op plannedNetworkOperation) bool {
	if !h.belongs(op.plan) || h.kind != op.kind {
		return false
	}
	switch op.kind {
	case plannedBridge, plannedFirewall:
		return h.name == op.plan.bridge
	case plannedNamespace:
		return h.name == string(op.member.id)
	case plannedVeth:
		return h.name == op.member.hostLink
	}
	return false
}

func copyPlannedNetwork(p plannedNetworkPlan) (plannedNetworkPlan, error) {
	if p.nonce == (LaunchNonce{}) || !plannedLinkName(p.bridge) || !plannedApprovedPrefix(p.prefix) || len(p.members) == 0 {
		return plannedNetworkPlan{}, errPlannedNetwork
	}
	p.members = append([]plannedNetworkMember(nil), p.members...)
	for i, m := range p.members {
		if !plannedLiteralName(string(m.id)) || !plannedLinkName(m.hostLink) || !plannedLinkName(m.peerLink) || m.hostLink == m.peerLink || m.hostLink == p.bridge || m.peerLink == p.bridge || !plannedApprovedAddress(m.address) || !p.prefix.Contains(m.address) || m.address == p.prefix.Addr() {
			return plannedNetworkPlan{}, errPlannedNetwork
		}
		for _, old := range p.members[:i] {
			if old.id == m.id || old.address == m.address || old.hostLink == m.hostLink || old.peerLink == m.peerLink || old.hostLink == m.peerLink || old.peerLink == m.hostLink {
				return plannedNetworkPlan{}, errPlannedNetwork
			}
		}
	}
	return p, nil
}
func cleanPlannedNetwork(i plannedNetworkInventory) bool {
	if !i.networkNone || !i.noForbiddenCapabilities || !i.noBlockDevices || len(i.links) != 1 || i.links[0].name != "lo" || i.links[0].index <= 0 {
		return false
	}
	for _, r := range i.routes {
		// Invalid/unknown prefixes and every default/non-loopback route refuse,
		// including a default which happens to point at loopback.
		if !plannedLoopbackRoute(r.prefix) {
			return false
		}
	}
	return true
}
func newPlannedNetwork(ctx context.Context, p plannedNetworkPlan, backend plannedNetworkBackend) (*plannedNetwork, error) {
	p, err := copyPlannedNetwork(p)
	if err != nil || backend == nil {
		return nil, errPlannedNetwork
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i, err := backend.inspect(ctx)
	if err != nil || !cleanPlannedNetwork(i) {
		return nil, errors.Join(errPlannedNetwork, err)
	}
	return &plannedNetwork{plan: p, backend: backend}, nil
}
func (n *plannedNetwork) setup(ctx context.Context) error {
	if n == nil || n.backend == nil || n.closed || n.ready || n.attempted || len(n.owned) != 0 {
		return errPlannedNetwork
	}
	n.attempted = true
	// Repeat preflight immediately before the first mutation. No constructor
	// snapshot can authorize a changed host or foreign colliding resources.
	i, err := n.backend.inspect(ctx)
	if err != nil || !cleanPlannedNetwork(i) {
		return errors.Join(errPlannedNetwork, err)
	}
	operations := []plannedNetworkOperation{{kind: plannedBridge, plan: n.plan}}
	for _, m := range n.plan.members {
		for _, kind := range []plannedNetworkKind{plannedNamespace, plannedVeth, plannedAttach, plannedMove, plannedAddress} {
			operations = append(operations, plannedNetworkOperation{kind: kind, plan: n.plan, member: m})
		}
	}
	operations = append(operations, plannedNetworkOperation{kind: plannedFirewall, plan: n.plan})
	for _, op := range operations {
		if err := ctx.Err(); err != nil {
			return err
		}
		handle, applyErr := n.backend.apply(ctx, op)
		creates := op.kind == plannedBridge || op.kind == plannedNamespace || op.kind == plannedVeth || op.kind == plannedFirewall
		if !handle.empty() {
			if !creates || !handle.matchesOperation(op) {
				return errors.Join(errPlannedNetwork, applyErr)
			}
			for _, prior := range n.owned {
				if prior == handle {
					return errors.Join(errPlannedNetwork, applyErr)
				}
			}
			n.owned = append(n.owned, handle)
		} else if creates && applyErr == nil {
			return errPlannedNetwork
		}
		if applyErr != nil {
			return applyErr
		}
	}
	if err := n.backend.verify(ctx, n.plan); err != nil {
		return err
	}
	n.ready = true
	return nil
}

// Called only after every child using this network has joined. On failure the
// caller retains this owner and explicitly closes it with its cleanup context.
// No automatic rollback uses a cancelled setup context or touches other runs.
func (n *plannedNetwork) close(ctx context.Context) error {
	if n == nil || n.backend == nil {
		return errPlannedNetwork
	}
	n.ready = false
	for len(n.owned) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		i := len(n.owned) - 1
		handle := n.owned[i]
		if !handle.belongs(n.plan) {
			return errPlannedNetwork
		}
		// remove must recheck exact kernel identity+nonce/alias. A replacement
		// at the same name is foreign; on refusal retain ownership for diagnosis.
		if err := n.backend.remove(ctx, handle); err != nil {
			return err
		}
		n.owned = n.owned[:i]
	}
	if release, ok := n.backend.(interface{ release() error }); ok {
		if err := release.release(); err != nil {
			return err
		}
	}
	n.closed = true
	return nil
}
