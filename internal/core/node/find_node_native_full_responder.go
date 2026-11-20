//go:build d1_o1a_native_responder

package node

import (
	"context"
	"crypto/sha256"
	"net/netip"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
)

// Closed native candidate root only. The plan scalars constrain a diagnostic;
// they are neither a namespace receipt nor a source of Responder authority.
// Ordinary factories and the loopback/small2 diagnostic remain unchanged.
func NewD1NativeFullResponderWithRouteWireCensus(cfg config.Node, id *identity.Identity, bus *ebus.Bus, maxEndpoints int, window [32]byte, seedRoutes int, dialRole domain.RouteWireDialRole) (*Service, *RegisteredProvenV2Observation, *D1NativeFullResponderReadout, context.CancelFunc, error) {
	canonical := func(raw string) bool {
		a, err := netip.ParseAddrPort(raw)
		return err == nil && a.String() == raw && a.Port() != 0 && a.Addr().Is4() && netip.MustParsePrefix("240.0.0.0/4").Contains(a.Addr())
	}
	if cfg.Type != config.NodeTypeFull || !cfg.DisableDirectMessages || !cfg.EffectiveListenerEnabled() || !cfg.AllowPrivatePeers || !canonical(cfg.ListenAddress) || cfg.ProxyAddress != "" || cfg.ConnectOnly != "" || len(cfg.BootstrapPeers) != 1 || !canonical(cfg.BootstrapPeers[0]) || maxEndpoints <= 0 || maxEndpoints > k3RegisteredMaxOwners || maxEndpoints > k3RegisteredMaxHistory/2 || window == ([32]byte{}) || seedRoutes != domain.RouteWireFullMaxDestinations || (dialRole != domain.RouteWireDialer && dialRole != domain.RouteWireListener) {
		return nil, nil, nil, nil, findnode.ErrInvalid
	}
	// Same reviewed SG512 allocation, not the SG512 connection-slot overlay.
	profile := findNodeBridgeProfile{LocalRole: findNodeRoleResponder,
		Capture: findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 512}, Priority: findNodePriorityProfile{MaxMembers: 512, MaxRegistryVisits: 1024}},
		Intake:  findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: 2 * time.Second, Network: 2 * time.Second, Runtime: time.Second}, Round: 100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4},
	}
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8}, findnode.OwnedTargetTimingProfile{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	digest := sha256.Sum256(window[:])
	tuple := findNodeReadyRegistered{OwnerCapacity: maxEndpoints, HistoryCapacity: 2 * maxEndpoints, Instance: [16]byte(digest[:16]), Window: window}
	s, err := r.buildWithOptions(cfg, id, bus, productionDatagramConstructor{}, findNodeReadyOptions{registered: &tuple})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cleanup := newUnstartedRuntimeCleanup(s, s.runCancel)
	if !r.finalized || !r.checked || !r.bindings(s) {
		cleanup()
		s.WaitBackground()
		return nil, nil, nil, nil, findnode.ErrNotReady
	}
	if dialRole == domain.RouteWireDialer {
		s.connManager.config.MaxSlotsFn = func() int { return 1 }
	} else {
		s.connManager.config.MaxSlotsFn = func() int { return 0 }
	}
	if err = seedLoadstandRouteWireTable(s.routingTable, domain.PeerIdentityFromWire(id.Address), seedRoutes); err != nil {
		cleanup()
		s.WaitBackground()
		return nil, nil, nil, nil, err
	}
	census := &RegisteredProvenV2Observation{service: s, source: s.k3Registered, identity: domain.PeerIdentityFromWire(id.Address), key: RegisteredProvenV2ObservationKey{Instance: tuple.Instance}}
	readout := &D1NativeFullResponderReadout{ready: r, service: s, census: census, source: s.k3Registered, identity: id, address: id.Address, done: s.runCtx.Done(), maxEndpoints: maxEndpoints}
	return s, census, readout, cleanup, nil
}

// No publisher, clock, seed/session installer or mutation handle escapes.
type D1NativeFullResponderReadout struct {
	ready        *findNodeReadyConstruction
	service      *Service
	census       *RegisteredProvenV2Observation
	source       *k3RegisteredSource
	identity     *identity.Identity
	address      string
	done         <-chan struct{}
	maxEndpoints int
}

func (r *D1NativeFullResponderReadout) boundTo(s *Service) bool {
	return r != nil && s != nil && s == r.service && r.ready != nil && r.ready.finalized && r.ready.checked && r.ready.bindings(s) && s.localFindNodeRole == findNodeRoleResponder && s.identity == r.identity && r.identity != nil && r.identity.Address == r.address && s.runCtx != nil && s.runCtx.Err() == nil && s.runCtx.Done() == r.done && r.done != nil && s.k3Registered == r.source && r.source != nil && r.census != nil && r.census.service == s && r.census.source == r.source && r.census.identity == domain.PeerIdentityFromWire(r.address)
}

// GuardCurrentForService runs ONLY in the existing LIVE census-read callback,
// before stop. It requires nonvacuous evidence for current window-end owners;
// historical retired endpoints retain only their original V2 census meaning.
// This qualifies idle enabled construction, not per-session R wire sampling.
// Actual wire R remains UNKNOWN; no F1 publication time is compared with the
// distinct route-wire clock. Immutable Ready role + original F1 are reused.
func (r *D1NativeFullResponderReadout) GuardCurrentForService(s *Service, rows []RouteWireCensusEndpoint, begin, end time.Duration) error {
	if !r.boundTo(s) || len(rows) == 0 || len(rows) > r.maxEndpoints || begin < 0 || end <= begin {
		return findnode.ErrNotReady
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if r.source.fault != "" {
		return findnode.ErrNotReady
	}
	matched := 0
	for i, row := range rows {
		if row.ConnID == 0 || row.Peer.IsZero() || row.SessionID == ([32]byte{}) || !row.Interval.Within(begin, end) {
			return findnode.ErrUnauthenticated
		}
		for _, prior := range rows[:i] {
			if prior.ConnID == row.ConnID {
				return findnode.ErrUnauthenticated
			}
		}
		if row.Interval.EndBefore != end || row.Interval.EndAfter != end {
			continue
		}
		original, ok, err := s.collectOwnedFindNodeRawMemberLocked(context.Background(), row.ConnID)
		if err != nil || !ok {
			return findnode.ErrUnauthenticated
		}
		e := s.connEntryByIDLocked(row.ConnID)
		if e == nil || e.core == nil || e.common == nil || e.core.Identity() != row.Peer || e.core.LoadstandSessionID() != row.SessionID || coreToPeerDirection(e.core.Dir()) != row.Direction {
			return findnode.ErrUnauthenticated
		}
		l := e.common
		l.mu.Lock()
		b := l.findNode
		valid := !l.ended && l.token == s.proofToken && l.entry == e && l.id == row.ConnID && l.bridgeTerminal == r.ready.originalTerminal && b != nil && b.published && b.session == original && b.session.ConnID() == row.ConnID && b.session.Identity() == row.Peer && b.identity == row.Peer
		l.mu.Unlock()
		if !valid {
			return findnode.ErrUnauthenticated
		}
		matched++
	}
	if matched == 0 || !r.boundTo(s) {
		return findnode.ErrNotReady
	}
	return nil
}
