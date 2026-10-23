package node

import (
	"context"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/rs/zerolog/log"
)

// Private inactive construction unit. No public factory/config/capability path
// selects it. A supplied decoded peer state (including empty) is owned exactly
// once; the builder may order its peers, but never rereads/renews the hint.
// Copied selection is immutable through finalization. The optional clock uses
// only the existing pre-store constructor seam; production leaves it absent.
type findNodeReadyOptions struct {
	ownDM         *ownDMRuntimeConstruction
	loadedInbound bool
	signingGate   *ownedFindNodeSigningTestGate
}

type findNodeReadyConstruction struct {
	bridge                 *findNodeTerminalConstruction
	originalContext        context.Context
	originalCancel         context.CancelFunc
	originalCapture        findNodeBridgeCaptureContext
	originalTerminal       *findNodeBridgeTerminal
	originalPublisher      findnodebridge.Publisher
	originalOwner          *degreeOwner
	originalToken          *proofTransportServiceToken
	originalStatistic      *ownedDegreeStatisticProducer
	peerState              peerStateFile
	service                *Service
	trust                  *trustStore
	store                  *secureSessionStore
	sessions               *secureSessions
	failure                error
	checked                bool
	finalized              bool
	ownDMRuntime           *ownDMRuntimeConstruction
	originalOwnDMRuntime   ownDMRuntimeConstruction
	options                findNodeReadyOptions
	originalOptions        findNodeReadyOptions
	originalInbound        *inboundPinProducer
	originalInboundService *Service
	originalInboundStore   *secureSessionStore
	originalInboundWake    chan struct{}
}

func prepareFindNodeReadyConstruction(cfg config.Node, id *identity.Identity, profile findNodeBridgeProfile, signing findnode.SigningProfile, timing ...findnode.OwnedTargetTimingProfile) (*findNodeReadyConstruction, error) {
	if len(timing) > 1 {
		return nil, findnode.ErrInvalid
	}
	if id == nil {
		return nil, findnode.ErrInvalid
	}
	key, err := identity.ParsePublicKey(id.PublicKey)
	if err != nil || identity.Fingerprint(id.PublicKey) != id.Address {
		return nil, findnode.ErrInvalid
	}
	// Same fallback semantics as ordinary construction; an optional peer-state
	// load error is not a new startup-fatal policy.
	state, loadErr := loadPeerState(cfg.EffectivePeersStatePath())
	if loadErr != nil {
		log.Error().Str("path", cfg.EffectivePeersStatePath()).Err(loadErr).Msg("peer state load failed")
		state = peerStateFile{Version: peerStateVersion, Peers: []peerEntry{}}
	}
	b, err := prepareFindNodeOwnedCaptureConstruction(profile.Capture)
	if err != nil {
		return nil, err
	}
	r := &findNodeReadyConstruction{bridge: b, peerState: state}
	prepared := false
	defer func() {
		if !prepared {
			b.cancel()
		}
	}()
	publication := b.terminal.publisher.BeginClockedPublication()
	start, wall, err := publication.SampleRuntimePair()
	publication.Complete()
	if err != nil {
		return nil, err
	}
	hint := freshOverlayDegreeStatistic(state.OverlayDWin.stat, wall)
	age := wall.Sub(hint.at)
	intake, sink, err := findnode.NewExecutingBootstrapIntake(b.capture.consumer, b.capture.observer, profile.Intake, profile.Capture.Priority.MaxMembers, start, hint.value, age, hint.valid)
	if err != nil {
		return nil, err
	}
	b.capture.intake, b.capture.sink = intake, sink
	b.capture.roundLoop = newOwnedFindNodeRoundLoop(b.ctx.Done(), profile.Intake.Round)
	b.findNode.owned.retirement = intake
	var local findnode.OwnedLocalTarget
	if len(timing) == 0 {
		local, err = intake.NewLocalTarget(key, domain.NetworkID(networkName), signing)
	} else {
		local, err = intake.NewLocalTargetWithTiming(key, domain.NetworkID(networkName), signing, timing[0])
	}
	if err != nil {
		return nil, err
	}
	b.capture.localTarget = local
	b.capture.localLoop = newOwnedFindNodeLocalLoop(b.ctx.Done(), signing.MaxReservations, profile.Intake.Round)
	b.findNode.owned.localRetirement = local
	b.statistic = &ownedDegreeStatisticProducer{owner: b.owner, publisher: b.terminal.publisher}
	r.originalCancel = b.cancel
	r.originalContext, r.originalCapture = b.ctx, *b.capture
	r.originalTerminal, r.originalPublisher = b.terminal, b.terminal.publisher
	r.originalOwner, r.originalToken, r.originalStatistic = b.owner, b.token, b.statistic
	prepared = true
	return r, nil
}

// Sole new coherent private root. Every error/panic cancels the original
// lifecycle and joins any callback-started background work before unwinding.
func newFindNodeReadyCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeBridgeProfile, signing findnode.SigningProfile) (*Service, error) {
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, signing)
	if err != nil {
		return nil, err
	}
	return r.build(cfg, id, bus, productionDatagramConstructor{})
}

// Loaded-only private composition, never an Unknown installer or a public
// configuration/default. Original production elapsed source remains trusted.
func newFindNodeReadyLoadedInboundCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile findNodeBridgeProfile, signing findnode.SigningProfile) (*Service, error) {
	r, err := prepareFindNodeReadyConstruction(cfg, id, profile, signing)
	if err != nil {
		return nil, err
	}
	return r.buildWithOptions(cfg, id, bus, productionDatagramConstructor{}, findNodeReadyOptions{loadedInbound: true})
}
func (r *findNodeReadyConstruction) build(cfg config.Node, id *identity.Identity, bus *ebus.Bus, constructor datagramConstructor) (*Service, error) {
	return r.buildWithOwnDM(cfg, id, bus, constructor, nil)
}
func (r *findNodeReadyConstruction) buildWithOwnDM(cfg config.Node, id *identity.Identity, bus *ebus.Bus, constructor datagramConstructor, own *ownDMRuntimeConstruction) (*Service, error) {
	return r.buildWithOptions(cfg, id, bus, constructor, findNodeReadyOptions{ownDM: own})
}
func (r *findNodeReadyConstruction) buildWithOptions(cfg config.Node, id *identity.Identity, bus *ebus.Bus, constructor datagramConstructor, options findNodeReadyOptions, clocks ...inboundPinClock) (*Service, error) {
	if r == nil || r.bridge == nil || r.originalContext == nil || r.finalized || r.service != nil || r.bridge.used.Load() {
		return nil, findnode.ErrNotReady
	}
	committed := false
	defer func() {
		if !committed {
			r.cleanup()
		}
	}()
	if len(clocks) > 1 || len(clocks) != 0 && !options.loadedInbound {
		return nil, findnode.ErrInvalid
	}
	if r.originalContext.Err() != nil {
		return nil, findnode.ErrNotReady
	}
	if options.signingGate != nil && !options.signingGate.bindings() {
		return nil, findnode.ErrInvalid
	}
	r.options, r.originalOptions = options, options
	r.bridge.capture.localLoop.signingGate = options.signingGate
	r.ownDMRuntime = options.ownDM
	record := recordAdmissionConstruction{}
	if options.ownDM != nil {
		record = recordAdmissionConstruction{classes: recordAdmissionChargedR}
	}
	s := newServiceWithConstruction(cfg, id, bus, nil, constructor, serviceConstruction{record: record, bridge: r.bridge, ready: r}, clocks...)
	if r.failure != nil {
		return nil, r.failure
	}
	if err := r.finalize(s); err != nil {
		return nil, err
	}
	committed = true
	return s, nil
}
func (r *findNodeReadyConstruction) cleanup() {
	if r == nil || r.bridge == nil {
		return
	}
	if r.originalCancel != nil {
		r.originalCancel()
	} else {
		r.bridge.cancel()
	}
	if r.service != nil {
		r.service.closeBackgroundAdmission()
		r.service.WaitBackground()
	}
}

// Comparisons here read immutable constructor bindings, never live tables or
// application locks. The same exact check is repeated after installation.
func (r *findNodeReadyConstruction) bindings(s *Service) bool {
	if r == nil || r.bridge == nil || s == nil {
		return false
	}
	if r.bridge.capture == nil || r.bridge.capture.localLoop == nil || r.options != r.originalOptions || r.ownDMRuntime != r.originalOptions.ownDM || r.bridge.capture.localLoop.signingGate != r.originalOptions.signingGate || (r.originalOptions.signingGate != nil && !r.originalOptions.signingGate.bindings()) {
		return false
	}
	if r.originalOptions.loadedInbound {
		p := r.originalInbound
		if p == nil || s.inboundPins != p || p.service != r.originalInboundService || p.store != r.originalInboundStore || p.wake != r.originalInboundWake || p.service != s || p.store != r.store || p.wake == nil || cap(p.wake) != 1 {
			return false
		}
	} else if s.inboundPins != nil {
		return false
	}
	if r.ownDMRuntime != nil {
		if *r.ownDMRuntime != r.originalOwnDMRuntime || !r.ownDMRuntime.bindings(s) {
			return false
		}
	} else if s.r2OwnDMOwner != nil {
		return false
	}
	b := r.bridge
	c := b.capture
	if c == nil || b.owner == nil || b.terminal == nil || b.token == nil || b.statistic == nil || b.findNode == nil || b.findNode.owned == nil {
		return false
	}
	return b.ctx == r.originalContext && *c == r.originalCapture && b.terminal == r.originalTerminal && b.terminal.publisher == r.originalPublisher && b.owner == r.originalOwner && b.token == r.originalToken && b.statistic == r.originalStatistic && s == r.service && s.runCtx == b.ctx && s.runCtx.Done() == b.ctx.Done() && s.runCancel != nil && s.proofToken == b.token && s.degree == b.owner && s.bridgeTerminal == b.terminal && b.owner.token == b.token && b.owner.bridgePublisher == b.terminal.publisher && b.terminal.owner == b.owner && b.terminal.token == b.token && s.findNode == b.findNode && s.findNode.owned.done == b.ctx.Done() && s.findNode.owned.terminal == b.terminal && s.bridgeCapture == c && c.terminal == b.terminal && c.consumer.IsRuntime() && c.observer != (findnodebridge.Observer{}) && c.intake != (findnode.DormantIntake{}) && c.sink != (findnode.ObservationSink{}) && c.roundLoop != nil && c.roundLoop.done == b.ctx.Done() && c.localLoop != nil && c.localLoop.done == b.ctx.Done() && c.localTarget != (findnode.OwnedLocalTarget{}) && s.findNode.owned.retirement == c.intake && s.findNode.owned.localRetirement == c.localTarget && s.ownedDegreeStatisticProducer == b.statistic && b.statistic.owner == b.owner && b.statistic.publisher == b.terminal.publisher && s.trust != nil && s.trust == r.trust && s.trust.trustPublisher == b.terminal.publisher && s.secureSessions != nil && s.secureSessions == r.sessions && s.secureSessions.store != nil && s.secureSessions.store == r.store && s.secureSessions.store.pinPublisher == b.terminal.publisher
}

// Called after ordinary constructor callbacks and BEFORE statistic install.
// All application locks and startup refusals are handled here; finalization
// below performs no application wait after the coverage-start clock sample.
func (r *findNodeReadyConstruction) checkBeforeInstall(s *Service) error {
	if r.checked || !r.bindings(s) || r.originalContext.Err() != nil {
		return findnode.ErrNotReady
	}
	s.trust.mu.RLock()
	trustErr := s.trust.authorityReadErr
	s.trust.mu.RUnlock()
	if trustErr != nil {
		return trustErr
	}
	if s.secureSessions.refusal != nil {
		return s.secureSessions.refusal
	}
	if err := s.secureSessions.store.startupRefusal(); err != nil {
		return err
	}
	s.backgroundMu.Lock()
	closed := s.backgroundClosed
	s.backgroundMu.Unlock()
	if closed || r.originalContext.Err() != nil {
		return findnode.ErrNotReady
	}
	r.checked = true
	return nil
}

// Exclusive production Seal site. The cancellation check immediately before
// and after Seal defines the construction/return boundary; cancellation after
// the final check is ordinary lifecycle shutdown, never a fresh activation.
func (r *findNodeReadyConstruction) finalize(s *Service) error {
	if r == nil || r.finalized || !r.checked || r.failure != nil || !r.bindings(s) {
		return findnode.ErrNotReady
	}
	b := r.bridge
	if !b.used.Load() || !b.statisticInstalled || !b.statistic.installed || s.trust != r.trust || s.secureSessions.store != r.store || r.originalContext.Err() != nil {
		return findnode.ErrNotReady
	}
	if err := b.terminal.publisher.SealConstruction(); err != nil {
		return err
	}
	if r.originalContext.Err() != nil {
		return findnode.ErrNotReady
	}
	r.finalized = true
	return nil
}

// Original resolver/store are fully bound; copy the exact immutable R2 tuple
// BEFORE checkBeforeInstall/statistic installation/Seal and Service escape.
func (r *findNodeReadyConstruction) installOwnDM(s *Service) {
	if r.ownDMRuntime != nil {
		r.ownDMRuntime.install(s)
		r.originalOwnDMRuntime = *r.ownDMRuntime
	}
}

// All store/files/lifecycle checks finish here BEFORE the final Ready check,
// statistic coverage sample and exclusive Seal. Loaded format3 may retain an
// unconfirmed strict obligation for the existing original Run worker.
func (r *findNodeReadyConstruction) installLoadedInbound(s *Service) error {
	if r.options != r.originalOptions {
		return findnode.ErrNotReady
	}
	if !r.originalOptions.loadedInbound {
		return nil
	}
	if s != r.service || s == nil || s.secureSessions != r.sessions || s.secureSessions == nil || s.secureSessions.store != r.store {
		return findnode.ErrNotReady
	}
	if err := r.originalContext.Err(); err != nil {
		return err
	}
	if err := attachInboundPinCandidate(s, nil); err != nil {
		return err
	}
	p := s.inboundPins
	if p == nil || p.service != s || p.store != r.store || p.wake == nil || cap(p.wake) != 1 {
		return findnode.ErrNotReady
	}
	r.originalInbound, r.originalInboundService, r.originalInboundStore, r.originalInboundWake = p, p.service, p.store, p.wake
	return nil
}
