//go:build d1_resource_probe

package node

import (
	"net/netip"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
)

// D1FindNodeResourceSnapshot contains values from independent owner reads.
// Named byte floors and last publications are not a coherent RAM census.
// Runtime-relative SampleAt has no proved external process/clock attribution.
type D1FindNodeResourceSnapshot struct {
	Available                                                bool
	UnknownReason                                            string
	Identity                                                 string
	MaxActive, MaxMembers, MaxRegistryVisits                 int
	TargetBudget, NetworkBudget, RuntimeBudget               time.Duration
	Round, Slack                                             time.Duration
	InitialTokens                                            int
	TargetServiceBound, TargetRuntimeReserve, SigningSpacing time.Duration
	SigningReservations                                      int
	Owner                                                    findnode.ResourceSnapshot
	RoundPending                                             bool
	IndependentPublications                                  bool
	ExternalClockBound, PhysicalTimeBoundsProven             bool
}

// D1FindNodeResourceReadout exposes no publisher, clock, admission or mutation
// handle. Its private bindings identify the original Ready Service instance.
type D1FindNodeResourceReadout struct {
	service      *Service
	capture      *findNodeBridgeCaptureContext
	captureValue findNodeBridgeCaptureContext
	adapter      *findNodeAdapter
	terminal     *findNodeBridgeTerminal
	publisher    findnodebridge.Publisher
	owner        *degreeOwner
	token        *proofTransportServiceToken
	identity     *identity.Identity
	address      string
	done         <-chan struct{}
}

// NewD1FindNodeResourceDiagnostic is a closed, build-tag-only diagnostic root.
// It selects real RuntimeDomain/Ready/Seal and the fixed small2 P0 hypothesis.
// No ordinary constructor, configuration flag or caller clock selects it.
func NewD1FindNodeResourceDiagnostic(cfg config.Node, id *identity.Identity, bus *ebus.Bus) (*Service, *D1FindNodeResourceReadout, error) {
	loopback := func(raw string) bool {
		address, err := netip.ParseAddrPort(raw)
		return err == nil && address.Addr() == netip.MustParseAddr("127.0.0.1")
	}
	if cfg.Type != domain.NodeTypeFull || !cfg.EffectiveListenerEnabled() || !loopback(cfg.ListenAddress) || cfg.ProxyAddress != "" || cfg.ConnectOnly != "" || len(cfg.BootstrapPeers) > 1 {
		return nil, nil, findnode.ErrInvalid
	}
	for _, peer := range cfg.BootstrapPeers {
		if !loopback(peer) {
			return nil, nil, findnode.ErrInvalid
		}
	}
	profile := findNodeBridgeProfile{
		Capture: findNodeOwnedCaptureProfile{Degree: degreeProfile{MaxActive: 2}, Priority: findNodePriorityProfile{MaxMembers: 2, MaxRegistryVisits: 4}},
		Intake:  findnode.DormantIntakeProfile{Timing: findnode.TimingProfile{Target: 2 * time.Second, Network: 2 * time.Second, Runtime: time.Second}, Round: 100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4},
	}
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile,
		findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 8},
		findnode.OwnedTargetTimingProfile{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond})
	if err != nil {
		return nil, nil, err
	}
	s, err := r.build(cfg, id, bus, productionDatagramConstructor{})
	if err != nil {
		return nil, nil, err
	}
	if !r.finalized || !r.checked || !r.bindings(s) || !s.bridgeCapture.consumer.IsRuntime() {
		r.cleanup()
		return nil, nil, findnode.ErrNotReady
	}
	// Selection precedes escape and any handshake; ordinary construction is off.
	s.ownedFindNodeAdvertiseTest = true
	c := s.bridgeCapture
	readout := &D1FindNodeResourceReadout{service: s, capture: c, captureValue: *c, adapter: s.findNode,
		terminal: c.terminal, publisher: c.terminal.publisher, owner: s.degree, token: s.proofToken,
		identity: s.identity, address: s.identity.Address, done: s.runCtx.Done()}
	return s, readout, nil
}

func (r *D1FindNodeResourceReadout) boundTo(s *Service) bool {
	if r == nil || s == nil || s != r.service || s.runCtx == nil || r.done == nil || s.runCtx.Done() != r.done || s.runCtx.Err() != nil {
		return false
	}
	select {
	case <-r.done:
		return false
	default:
	}
	c, t := r.capture, r.terminal
	return c != nil && t != nil && s.bridgeCapture == c && *c == r.captureValue && c.consumer.IsRuntime() &&
		s.findNode == r.adapter && r.adapter != nil && r.adapter.owned != nil && r.adapter.owned.done == r.done && r.adapter.owned.terminal == t &&
		r.adapter.owned.retirement == c.intake && r.adapter.owned.localRetirement == c.localTarget &&
		s.bridgeTerminal == t && c.terminal == t && t.publisher == r.publisher && t.owner == r.owner && t.token == r.token &&
		s.degree == r.owner && r.owner != nil && r.owner.token == r.token && r.owner.bridgePublisher == r.publisher &&
		s.proofToken == r.token && r.token != nil && c.roundLoop != nil && c.roundLoop.done == r.done && c.localLoop != nil && c.localLoop.done == r.done &&
		s.identity == r.identity && r.identity != nil && r.identity.Address == r.address
}

// SnapshotForService refuses before owner reads on foreign/closed bindings.
// The final binding check detects closure during the independent reads; it does
// not promise an atomic instant across lifecycle and owner publications.
func (r *D1FindNodeResourceReadout) SnapshotForService(expected *Service) D1FindNodeResourceSnapshot {
	unknown := D1FindNodeResourceSnapshot{UnknownReason: "UNKNOWN: service/runtime binding unavailable"}
	if !r.boundTo(expected) {
		return unknown
	}
	owner := r.capture.intake.ResourceSnapshot()
	pending := r.capture.intake.OwnedRoundPending()
	if !r.boundTo(expected) || !owner.Enabled || !owner.LocalEnabled {
		return unknown
	}
	return D1FindNodeResourceSnapshot{Available: true, Identity: r.address,
		MaxActive: 2, MaxMembers: 2, MaxRegistryVisits: 4,
		TargetBudget: 2 * time.Second, NetworkBudget: 2 * time.Second, RuntimeBudget: time.Second,
		Round: 100 * time.Millisecond, Slack: 100 * time.Millisecond, InitialTokens: 4,
		TargetServiceBound: 2 * time.Second, TargetRuntimeReserve: 500 * time.Millisecond, SigningSpacing: 250 * time.Millisecond, SigningReservations: 8,
		Owner: owner, RoundPending: pending, IndependentPublications: true}
}
