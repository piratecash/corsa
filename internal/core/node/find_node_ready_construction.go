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
type findNodeReadyConstruction struct {
	bridge               *findNodeTerminalConstruction
	originalContext      context.Context
	originalCancel       context.CancelFunc
	originalCapture      findNodeBridgeCaptureContext
	originalTerminal     *findNodeBridgeTerminal
	originalPublisher    findnodebridge.Publisher
	originalOwner        *degreeOwner
	originalToken        *proofTransportServiceToken
	originalStatistic    *ownedDegreeStatisticProducer
	peerState            peerStateFile
	service              *Service
	trust                *trustStore
	store                *secureSessionStore
	sessions             *secureSessions
	failure              error
	checked              bool
	finalized            bool
	ownDMRuntime         *ownDMRuntimeConstruction
	originalOwnDMRuntime ownDMRuntimeConstruction
}

func prepareFindNodeReadyConstruction(cfg config.Node, id *identity.Identity, profile findNodeBridgeProfile, signing findnode.SigningProfile) (*findNodeReadyConstruction, error) {
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
	local, err := intake.NewLocalTarget(key, domain.NetworkID(networkName), signing)
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
func (r *findNodeReadyConstruction) build(cfg config.Node, id *identity.Identity, bus *ebus.Bus, constructor datagramConstructor) (*Service, error) {
	return r.buildWithOwnDM(cfg, id, bus, constructor, nil)
}
func (r *findNodeReadyConstruction) buildWithOwnDM(cfg config.Node, id *identity.Identity, bus *ebus.Bus, constructor datagramConstructor, own *ownDMRuntimeConstruction) (*Service, error) {
	if r == nil || r.bridge == nil || r.originalContext == nil || r.finalized || r.service != nil || r.bridge.used.Load() {
		return nil, findnode.ErrNotReady
	}
	committed := false
	defer func() {
		if !committed {
			r.cleanup()
		}
	}()
	if r.originalContext.Err() != nil {
		return nil, findnode.ErrNotReady
	}
	r.ownDMRuntime = own
	record := recordAdmissionConstruction{}
	if own != nil {
		record = recordAdmissionConstruction{classes: recordAdmissionChargedR}
	}
	s := newServiceWithConstruction(cfg, id, bus, nil, constructor, serviceConstruction{record: record, bridge: r.bridge, ready: r})
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
