package node

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"

	"github.com/piratecash/corsa/internal/core/domain"
)

var (
	errLoadStandAddressInvalid  = errors.New("invalid load stand primary address plan")
	errLoadStandAddressCapacity = errors.New("load stand primary distinct-source capacity exceeded")
)

// 240/4 contains 2^(16-4) distinct /16 groups, counting FULL and SDK sources.
// This is the range's capacity, not a quota or a runtime-support claim.
const loadStandPrimarySourceCapacity = 1 << (16 - 4)

// An SDK Address denotes its planned source, not a listener. Bootstrap may
// only refer to FULL nodes; no address value establishes actual ownership.
type loadStandPlannedAddress struct {
	Node      loadStandNodeID
	Address   domain.PeerAddress
	Bootstrap []domain.PeerAddress
}

type loadStandAddressRow struct {
	Node        loadStandNodeID
	Role        loadStandRole
	Hub         bool
	Address     domain.PeerAddress
	CanonicalIP domain.PeerIP
	NetGroup    domain.NetGroup
	NonRoutable bool
	Subnet      netip.Prefix
	SourceGroup domain.SourceGroup
	Bootstrap   []domain.PeerAddress
}

type loadStandAddressCheck struct {
	digest loadStandPlanDigest
	rows   []loadStandAddressRow
}

func (c loadStandAddressCheck) Digest() loadStandPlanDigest { return c.digest }

func (c loadStandAddressCheck) Rows() []loadStandAddressRow {
	rows := slices.Clone(c.rows)
	for i := range rows {
		rows[i].Bootstrap = slices.Clone(rows[i].Bootstrap)
	}
	return rows
}

func cloneLoadStandPlannedAddresses(candidates []loadStandPlannedAddress) []loadStandPlannedAddress {
	rows := slices.Clone(candidates)
	for i := range rows {
		rows[i].Bootstrap = slices.Clone(rows[i].Bootstrap)
	}
	return rows
}

// validateLoadStandPrimaryIPv4 is a pure test-package precursor of a future
// launcher. It neither allocates addresses nor changes the plan, and has no
// runtime call root. A failed validation returns no partial result.
func validateLoadStandPrimaryIPv4(plan loadStandPlan, candidates []loadStandPlannedAddress) (loadStandAddressCheck, error) {
	if len(plan.nodes) > loadStandPrimarySourceCapacity || len(candidates) > loadStandPrimarySourceCapacity {
		return loadStandAddressCheck{}, errLoadStandAddressCapacity
	}
	nodes, byID, err := loadStandAddressPlanNodes(plan)
	if err != nil {
		return loadStandAddressCheck{}, err
	}
	if len(candidates) != len(nodes) {
		return loadStandAddressCheck{}, fmt.Errorf("%w: one address row per node is required", errLoadStandAddressInvalid)
	}
	rows, err := loadStandClassifyAddressRows(nodes, byID, cloneLoadStandPlannedAddresses(candidates))
	if err != nil {
		return loadStandAddressCheck{}, err
	}
	if err := loadStandValidateAddressBootstrap(nodes, byID, rows); err != nil {
		return loadStandAddressCheck{}, err
	}
	return loadStandAddressCheck{digest: plan.Digest(), rows: rows}, nil
}

func loadStandAddressPlanNodes(plan loadStandPlan) ([]loadStandPlanNode, map[loadStandNodeID]int, error) {
	if len(plan.nodes) == 0 || plan.Digest() == "" || plan.computeDigest() != plan.Digest() {
		return nil, nil, fmt.Errorf("%w: empty plan or digest mismatch", errLoadStandAddressInvalid)
	}
	versions := 0
	for _, param := range plan.Params() {
		if param.Name != "plan_version" {
			continue
		}
		versions++
		if param.Value != "3" {
			return nil, nil, fmt.Errorf("%w: plan-v3 is required", errLoadStandAddressInvalid)
		}
	}
	if versions != 1 {
		return nil, nil, fmt.Errorf("%w: one plan version is required", errLoadStandAddressInvalid)
	}
	nodes := plan.Nodes()
	byID := make(map[loadStandNodeID]int, len(nodes))
	fulls, edges := 0, 0
	for i, node := range nodes {
		var expected loadStandNodeID
		switch node.Role() {
		case loadStandRoleFull:
			if edges != 0 {
				return nil, nil, fmt.Errorf("%w: FULL follows SDK", errLoadStandAddressInvalid)
			}
			expected = loadStandFullNodeID(fulls)
			fulls++
		case loadStandRoleEdge:
			expected = loadStandEdgeNodeID(edges)
			edges++
		default:
			return nil, nil, fmt.Errorf("%w: unknown node role", errLoadStandAddressInvalid)
		}
		if node.ID() != expected || (node.IsHub() && node.Role() != loadStandRoleFull) {
			return nil, nil, fmt.Errorf("%w: noncanonical node ID or hub role", errLoadStandAddressInvalid)
		}
		if _, exists := byID[node.ID()]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate node ID", errLoadStandAddressInvalid)
		}
		byID[node.ID()] = i
	}
	if fulls == 0 {
		return nil, nil, fmt.Errorf("%w: no FULL node", errLoadStandAddressInvalid)
	}
	return nodes, byID, nil
}

func loadStandClassifyAddressRows(nodes []loadStandPlanNode, byID map[loadStandNodeID]int, candidates []loadStandPlannedAddress) ([]loadStandAddressRow, error) {
	rows := make([]loadStandAddressRow, len(nodes))
	seenNodes := make(map[loadStandNodeID]bool, len(nodes))
	seenIPs := make(map[domain.PeerIP]bool, len(nodes))
	seenEndpoints := make(map[domain.PeerAddress]bool, len(nodes))
	seenSources := make(map[domain.SourceGroup]bool, len(nodes))
	fullSubnets := make(map[netip.Prefix]bool)
	fulls := 0
	for _, candidate := range candidates {
		index, known := byID[candidate.Node]
		if !known || seenNodes[candidate.Node] {
			return nil, fmt.Errorf("%w: unknown or duplicate address node", errLoadStandAddressInvalid)
		}
		row, err := loadStandClassifyPrimaryAddress(candidate.Address)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", candidate.Node, err)
		}
		if seenIPs[row.CanonicalIP] || seenEndpoints[row.Address] || seenSources[row.SourceGroup] {
			return nil, fmt.Errorf("%w: shared IP, endpoint or source group", errLoadStandAddressInvalid)
		}
		seenNodes[candidate.Node] = true
		seenIPs[row.CanonicalIP], seenEndpoints[row.Address], seenSources[row.SourceGroup] = true, true, true
		node := nodes[index]
		row.Node, row.Role, row.Hub = node.ID(), node.Role(), node.IsHub()
		row.Bootstrap = candidate.Bootstrap
		rows[index] = row
		if node.Role() == loadStandRoleFull {
			fulls++
			fullSubnets[row.Subnet] = true
		}
	}
	// /16 uniqueness is stronger today; retain the actual /24 requirement
	// separately so a future classifier change cannot silently erase it.
	if len(fullSubnets) != fulls {
		return nil, fmt.Errorf("%w: FULL /24 diversity mismatch", errLoadStandAddressInvalid)
	}
	return rows, nil
}

func loadStandClassifyPrimaryAddress(address domain.PeerAddress) (loadStandAddressRow, error) {
	host, portText, err := net.SplitHostPort(string(address))
	if err != nil {
		return loadStandAddressRow{}, fmt.Errorf("%w: literal IPv4 host:port is required", errLoadStandAddressInvalid)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.Is4() {
		return loadStandAddressRow{}, fmt.Errorf("%w: a bare IPv4 literal is required", errLoadStandAddressInvalid)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || !domain.PeerPort(port).IsValid() || strconv.FormatUint(port, 10) != portText || net.JoinHostPort(ip.String(), portText) != string(address) {
		return loadStandAddressRow{}, fmt.Errorf("%w: noncanonical host or port", errLoadStandAddressInvalid)
	}
	if !netip.MustParsePrefix("240.0.0.0/4").Contains(ip) {
		return loadStandAddressRow{}, fmt.Errorf("%w: address is outside primary 240/4", errLoadStandAddressInvalid)
	}
	canonical := canonicalIPFromHost(host)
	group := classifyHost(host)
	nonRoutable := isNonRoutableIP(net.ParseIP(host))
	subnet, err := netip.ParsePrefix(subnetGroupKey(host))
	if err != nil || subnet.Bits() != 24 || subnet != subnet.Masked() || !subnet.Contains(ip) {
		return loadStandAddressRow{}, fmt.Errorf("%w: actual IPv4 /24 classification mismatch", errLoadStandAddressInvalid)
	}
	source := sourceGroup(ip)
	sourcePrefix, err := netip.ParsePrefix(string(source))
	if err != nil || sourcePrefix.Bits() != 16 || sourcePrefix != sourcePrefix.Masked() || !sourcePrefix.Contains(ip) {
		return loadStandAddressRow{}, fmt.Errorf("%w: actual IPv4 /16 classification mismatch", errLoadStandAddressInvalid)
	}
	if canonical != host || group != domain.NetGroupIPv4 || nonRoutable {
		return loadStandAddressRow{}, fmt.Errorf("%w: actual canonical/routability classification mismatch", errLoadStandAddressInvalid)
	}
	return loadStandAddressRow{
		Address: address, CanonicalIP: domain.PeerIP(ip), NetGroup: group, NonRoutable: nonRoutable,
		Subnet: subnet, SourceGroup: source,
	}, nil
}

func loadStandValidateAddressBootstrap(nodes []loadStandPlanNode, byID map[loadStandNodeID]int, rows []loadStandAddressRow) error {
	for i, node := range nodes {
		targets := node.DialTargets()
		if len(targets) == 0 || len(rows[i].Bootstrap) != len(targets) {
			return fmt.Errorf("%w: node %q needs exact nonempty bootstrap", errLoadStandAddressInvalid, node.ID())
		}
		seen := make(map[loadStandNodeID]bool, len(targets))
		for j, id := range targets {
			targetIndex, known := byID[id]
			if !known || id == node.ID() || nodes[targetIndex].Role() != loadStandRoleFull || seen[id] {
				return fmt.Errorf("%w: invalid planned bootstrap target", errLoadStandAddressInvalid)
			}
			seen[id] = true
			if rows[i].Bootstrap[j] != rows[targetIndex].Address {
				return fmt.Errorf("%w: bootstrap differs from ordered DialTargets", errLoadStandAddressInvalid)
			}
		}
	}
	return nil
}
