package loadstand

import (
	"bytes"
	"encoding/json"
	"io"
	"net/netip"
	"path/filepath"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Public immutable plan projection; never a namespace/launch receipt. The
// original Linux parent validates and copies it before its private helper init.
// It adds no environment/default switch to the existing loopback constructor.
type PlannedChildPeer struct {
	ID       MemberID
	Role     MemberRole
	Endpoint domain.PeerAddress
}
type PlannedChildConfig struct {
	ID        MemberID
	Role      MemberRole
	Prefix    netip.Prefix
	Peers     []PlannedChildPeer
	RouteWire *PlannedRouteWireObservation `json:",omitempty"`
}

// PlannedRouteWireObservation selects a bounded capture for one FullNode
// launch. The controller independently binds the expected census.
type PlannedRouteWireObservation struct {
	Window                 LaunchNonce
	MaxEndpoints           int
	Bounds                 RouteWireWindowBounds    `json:",omitzero"`
	FullReceiptRouteBudget int                      `json:",omitempty"`
	SeedRoutes             int                      `json:",omitempty"`
	DisconnectAt           time.Duration            `json:",omitempty"`
	DialRole               domain.RouteWireDialRole `json:",omitempty"`
}

func acceptsRouteWireCommitment(selection *PlannedRouteWireObservation, commitment RouteWireCommitment) bool {
	return selection != nil && commitment.valid() && selection.Window == commitment.Window &&
		selection.Bounds == commitment.Bounds && commitment.Count <= selection.MaxEndpoints
}

func samePlannedChild(a, b PlannedChildConfig) bool {
	if a.ID != b.ID || a.Role != b.Role || a.Prefix != b.Prefix || !slices.Equal(a.Peers, b.Peers) {
		return false
	}
	if a.RouteWire == nil || b.RouteWire == nil {
		return a.RouteWire == b.RouteWire
	}
	return *a.RouteWire == *b.RouteWire
}

func copyPlannedChild(c ChildConfig, p PlannedChildConfig) (ChildConfig, PlannedChildConfig, error) {
	if !filepath.IsAbs(c.DataDir) || filepath.Clean(c.DataDir) != c.DataDir || c.Procs < 1 || len(c.Bootstrap) == 0 || !plannedLiteralName(string(p.ID)) || !plannedApprovedPrefix(p.Prefix) || p.Role != MemberFull && p.Role != MemberSDK || p.Role == MemberFull && !c.Listener || p.Role == MemberSDK && c.Listener {
		return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
	}
	if p.RouteWire != nil {
		selection := *p.RouteWire
		if p.Role != MemberFull || selection.Window == (LaunchNonce{}) || selection.MaxEndpoints <= 0 ||
			routeWireReportLimit(selection.MaxEndpoints) == 0 || !selection.Bounds.validOrZero() ||
			selection.FullReceiptRouteBudget < 0 || selection.FullReceiptRouteBudget > 0 &&
			(!selection.Bounds.valid() || (selection.FullReceiptRouteBudget-1)/domain.RouteWireFullMaxDestinations >= selection.MaxEndpoints) ||
			selection.SeedRoutes < 0 || selection.SeedRoutes > min(selection.FullReceiptRouteBudget, domain.RouteWireFullMaxDestinations) ||
			selection.DialRole != "" && (!selection.Bounds.valid() || selection.FullReceiptRouteBudget <= 0 || len(p.Peers) != 2 ||
				len(c.Bootstrap) != 1 || selection.DialRole != domain.RouteWireDialer && selection.DialRole != domain.RouteWireListener) ||
			selection.SeedRoutes > 0 && len(p.Peers) != 2 || selection.DisconnectAt != 0 &&
			(!selection.Bounds.valid() || selection.DisconnectAt <= selection.Bounds.Begin || selection.DisconnectAt >= selection.Bounds.End ||
				selection.FullReceiptRouteBudget <= 0 || selection.MaxEndpoints < 2 || len(p.Peers) != 2 || selection.DialRole != domain.RouteWireDialer) {
			return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
		}
		p.RouteWire = &selection
	}
	listen, err := netip.ParseAddrPort(string(c.Listen))
	if err != nil || listen.Port() == 0 || listen.String() != string(c.Listen) || !plannedApprovedAddress(listen.Addr()) || !p.Prefix.Contains(listen.Addr()) || listen.Addr() == p.Prefix.Addr() {
		return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
	}
	rpc, err := netip.ParseAddrPort(string(c.RPC))
	if err != nil || rpc.Port() == 0 || rpc.Addr() != netip.MustParseAddr("127.0.0.1") || rpc.String() != string(c.RPC) {
		return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
	}
	p.Peers = append([]PlannedChildPeer(nil), p.Peers...)
	c.Bootstrap = append([]domain.PeerAddress(nil), c.Bootstrap...)
	own := 0
	for i, peer := range p.Peers {
		endpoint, err := netip.ParseAddrPort(string(peer.Endpoint))
		if err != nil || endpoint.Port() == 0 || endpoint.String() != string(peer.Endpoint) || !plannedApprovedAddress(endpoint.Addr()) || !p.Prefix.Contains(endpoint.Addr()) || endpoint.Addr() == p.Prefix.Addr() || !plannedLiteralName(string(peer.ID)) || peer.Role != MemberFull && peer.Role != MemberSDK {
			return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
		}
		if peer.ID == p.ID {
			if peer.Role != p.Role || string(peer.Endpoint) != string(c.Listen) {
				return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
			}
			own++
		}
		for _, old := range p.Peers[:i] {
			oldAddress, _ := netip.ParseAddrPort(string(old.Endpoint))
			if old.ID == peer.ID || oldAddress.Addr() == endpoint.Addr() {
				return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
			}
		}
	}
	if own != 1 {
		return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
	}
	for i, seed := range c.Bootstrap {
		found := false
		for _, peer := range p.Peers {
			if seed == peer.Endpoint && peer.Role == MemberFull && peer.ID != p.ID {
				found = true
			}
		}
		if !found {
			return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
		}
		for _, old := range c.Bootstrap[:i] {
			if old == seed {
				return ChildConfig{}, PlannedChildConfig{}, ErrInvalidConfiguration
			}
		}
	}
	return c, p, nil
}

func ReadPlannedChildEnvironment(getenv func(string) string) (ChildConfig, PlannedChildConfig, *Credentials, error) {
	config, credentials, err := readChildEnvironmentUnchecked(getenv)
	if err != nil {
		return ChildConfig{}, PlannedChildConfig{}, nil, err
	}
	raw := getenv("LOADSTAND_MEMBER_PLAN")
	if len(raw) == 0 || len(raw) > linuxPacketLimit {
		return ChildConfig{}, PlannedChildConfig{}, nil, ErrInvalidConfiguration
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var plan PlannedChildConfig
	if err = decoder.Decode(&plan); err != nil {
		return ChildConfig{}, PlannedChildConfig{}, nil, err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return ChildConfig{}, PlannedChildConfig{}, nil, ErrInvalidConfiguration
	}
	config, plan, err = copyPlannedChild(config, plan)
	return config, plan, credentials, err
}
func ReadSDKChildEnvironment(getenv func(string) string) (ChildConfig, *Credentials, error) {
	if getenv("LOADSTAND_MEMBER_PLAN") == "" {
		return ReadChildEnvironment(getenv)
	}
	config, plan, credentials, err := ReadPlannedChildEnvironment(getenv)
	if err != nil || plan.Role != MemberSDK {
		return ChildConfig{}, nil, ErrInvalidConfiguration
	}
	return config, credentials, nil
}
