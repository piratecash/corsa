package node

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
)

type serviceConstruction struct {
	record              recordAdmissionConstruction
	bridge              *findNodeTerminalConstruction
	ready               *findNodeReadyConstruction
	pendingObservation  *pendingObservationConstruction
	terminalObservation *terminalObservationConstruction
}

type findNodeBridgeProfile struct {
	Capture findNodeOwnedCaptureProfile
	Intake  findnode.DormantIntakeProfile
}

// Distinct unused full-cut root. The original tuple precedes FIRST Service;
// runtime construction remains Constructing and cannot produce online cuts.
func newFindNodeBridgeCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeBridgeProfile) (*Service, error) {
	b, err := prepareFindNodeBridgeConstruction(profile)
	if err != nil {
		return nil, err
	}
	return newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b}), nil
}

func prepareFindNodeBridgeConstruction(profile findNodeBridgeProfile) (*findNodeTerminalConstruction, error) {
	if _, err := profile.Intake.Timing.LatestForward(0); err != nil {
		return nil, err
	}
	if profile.Intake.Round <= 0 || profile.Intake.Slack < 0 || profile.Intake.InitialTokens > 4 || profile.Intake.Slack > profile.Intake.Timing.Runtime || profile.Intake.Round > profile.Intake.Timing.Runtime-profile.Intake.Slack {
		return nil, findnode.ErrInvalid
	}
	b, err := prepareFindNodeOwnedCaptureConstruction(profile.Capture)
	if err != nil {
		return nil, err
	}
	intake, sink, err := findnode.NewDormantIntake(b.capture.consumer, profile.Intake, profile.Capture.Priority.MaxMembers)
	if err != nil {
		b.cancel()
		return nil, err
	}
	b.capture.intake, b.capture.sink = intake, sink
	return b, nil
}

type findNodeTerminalConstruction struct {
	ctx                context.Context
	cancel             context.CancelFunc
	token              *proofTransportServiceToken
	owner              *degreeOwner
	terminal           *findNodeBridgeTerminal
	used               atomic.Bool
	statistic          *ownedDegreeStatisticProducer
	statisticInstalled bool
	findNode           *findNodeAdapter
	capture            *findNodeBridgeCaptureContext
}

// This unused root constructs only terminal/P1 authority. It never seals the
// domain, creates intake, installs a statistic observer or enables a handler.
//
//nolint:unused // Retained inactive terminal-only authority boundary; exact body/caller inventories are negative Source oracles, not a Ready activation path.
func newFindNodeTerminalCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile) (*Service, error) {
	b, err := prepareFindNodeTerminalConstruction(profile)
	if err != nil {
		return nil, err
	}
	return newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b}), nil
}

// Distinct unused Constructing statistics root. The optional binding exists
// before any builder callback; coverage is installed only at finalization.
func newFindNodeTerminalStatisticCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile) (*Service, error) {
	b, err := prepareFindNodeTerminalConstruction(profile)
	if err != nil {
		return nil, err
	}
	b.statistic = &ownedDegreeStatisticProducer{owner: b.owner, publisher: b.terminal.publisher}
	s := newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b})
	if !b.statisticInstalled {
		return s, errDegreeUnavailable
	}
	return s, nil
}

// This unused Constructing root adds only same-domain F1-open publication.
// Registry writers, capture/cut and admission remain incomplete; never Ready.
//
//nolint:unused // Retained inactive F1-only authority boundary; exact body/caller inventories are negative Source oracles, not a Ready activation path.
func newFindNodeOwnedF1CandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile degreeProfile) (*Service, error) {
	b, err := prepareFindNodeTerminalConstruction(profile)
	if err != nil {
		return nil, err
	}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: b.ctx.Done()}}
	return newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b}), nil
}

// Actual Node observation composition, still unused and Constructing. All
// three original roles exist before the FIRST Service literal or callback.
func newFindNodeOwnedCaptureCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeOwnedCaptureProfile) (*Service, error) {
	b, err := prepareFindNodeOwnedCaptureConstruction(profile)
	if err != nil {
		return nil, err
	}
	return newServiceWithConstruction(cfg, id, bus, nil, productionDatagramConstructor{}, serviceConstruction{bridge: b}), nil
}
func prepareFindNodeOwnedCaptureConstruction(profile findNodeOwnedCaptureProfile) (*findNodeTerminalConstruction, error) {
	maxInt := int(^uint(0) >> 1)
	if profile.Degree.MaxActive <= 0 || profile.Degree.MaxActive > maxInt/degreeAllocationUnit || !profile.Priority.valid() || profile.Priority.MaxMembers > maxInt/(ownedFindNodeRawWordsPerMember*(strconv.IntSize/8)) {
		return nil, errDegreeProfile
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := ctx.Done()
	d, err := findnodebridge.NewRuntimeDomain(done)
	if err != nil {
		cancel()
		return nil, err
	}
	b := newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile.Degree)
	b.capture = &findNodeBridgeCaptureContext{terminal: b.terminal, observer: d.Observer(), consumer: d.Consumer(), priority: profile.Priority}
	b.findNode = &findNodeAdapter{owned: &findNodeOwnedAdapter{terminal: b.terminal, done: done}}
	return b, nil
}

func prepareFindNodeTerminalConstruction(profile degreeProfile) (*findNodeTerminalConstruction, error) {
	maxInt := int(^uint(0) >> 1)
	if profile.MaxActive <= 0 || profile.MaxActive > maxInt/degreeAllocationUnit {
		return nil, errDegreeProfile
	}
	ctx, cancel := context.WithCancel(context.Background())
	d, err := findnodebridge.NewRuntimeDomain(ctx.Done())
	if err != nil {
		cancel()
		return nil, err
	}
	return newFindNodeTerminalConstruction(ctx, cancel, d.Publisher(), profile), nil
}

// Closed runtime and tagged fixture callers supply their freshly minted
// same-lifecycle domain before any Service or callback can escape.
func newFindNodeTerminalConstruction(ctx context.Context, cancel context.CancelFunc, publisher findnodebridge.Publisher, profile degreeProfile) *findNodeTerminalConstruction {
	token := &proofTransportServiceToken{}
	owner := &degreeOwner{token: token, profile: profile, active: make([]degreeActiveRow, profile.MaxActive), bridgePublisher: publisher}
	terminal := &findNodeBridgeTerminal{publisher: publisher, owner: owner, token: token}
	return &findNodeTerminalConstruction{ctx: ctx, cancel: cancel, token: token, owner: owner, terminal: terminal}
}
func (b *findNodeTerminalConstruction) claim() {
	if b != nil && b.capture != nil && ((b.capture.intake == (findnode.DormantIntake{})) != (b.capture.sink == (findnode.ObservationSink{}))) {
		panic("invalid owned intake construction pair")
	}
	if b != nil && b.capture != nil && (b.capture.terminal != b.terminal || b.capture.observer == (findnodebridge.Observer{}) || (!b.capture.consumer.IsRuntime() && !b.capture.consumer.IsFixture()) || !b.capture.priority.valid() || b.capture.priority.MaxMembers > int(^uint(0)>>1)/(ownedFindNodeRawWordsPerMember*(strconv.IntSize/8)) || b.findNode == nil) {
		panic("invalid owned capture construction")
	}

	if b != nil && b.findNode != nil && (b.findNode.owned == nil || b.findNode.owned.terminal != b.terminal || b.ctx == nil || b.findNode.owned.done != b.ctx.Done()) {
		panic("invalid owned F1 construction")
	}
	if b == nil || b.ctx == nil || b.cancel == nil || b.token == nil || b.owner == nil || b.terminal == nil || b.owner.token != b.token || b.terminal.owner != b.owner || b.terminal.token != b.token || b.terminal.publisher == (findnodebridge.Publisher{}) || b.owner.bridgePublisher != b.terminal.publisher || !b.used.CompareAndSwap(false, true) {
		panic("invalid or reused terminal construction")
	}
}
func constructionPublisher(b *findNodeTerminalConstruction) findnodebridge.Publisher {
	if b == nil {
		return findnodebridge.Publisher{}
	}
	return b.terminal.publisher
}
func loadTrustStoreForConstruction(path string, self trustedContact, b *findNodeTerminalConstruction) (*trustStore, error) {
	if b == nil {
		return loadTrustStore(path, self)
	}
	return loadTrustStoreWithPublisher(path, self, func() (localContactDurableFiles, error) { return openLocalContactFiles(path) }, b.terminal.publisher)
}
