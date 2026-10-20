package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/rs/zerolog/log"
)

// Immutable constructor-only option. The getter is a nonreentrant trusted-clock
// leaf, including in fixtures: no owner lock, logging, blocking callback or I/O.
type inboundPinClock struct{ elapsed func() (time.Duration, error) }

type inboundLifetimePhase uint8

const (
	inboundProvisional inboundLifetimePhase = iota
	inboundPublished
	inboundTerminal
)

// The loaded-only private factory never installs or migrates a journal.
// On refusal the ordinary Service still belongs to the caller.
func newInboundPinCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, clock inboundPinClock) (*Service, error) {
	s := newServiceWithDatagramConstructor(cfg, id, bus, nil, productionDatagramConstructor{}, clock)
	return s, attachInboundPinCandidate(s, nil)
}

// The unused Unknown factory constructs exactly one Service/store. Failures
// leave that same ordinary Service caller-owned, with no producer. A caller
// must not attach later to a returned running Service; the helper checks Run's
// existing irreversible store marker as well as its closed constructor roots.
func newUnknownInboundPinCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, clock inboundPinClock) (*Service, ninInstallResult, error) {
	s := newServiceWithDatagramConstructor(cfg, id, bus, nil, productionDatagramConstructor{}, clock)
	var result ninInstallResult
	store := s.localContactPinStore()
	if store == nil {
		return s, result, domain.InboundQuotaNotInstalled
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		return s, result, err
	}
	result, err = store.installUnknownInboundPinJournal(s.runCtx, files)
	err = errors.Join(err, files.Close())
	if err != nil {
		return s, result, err
	}
	if err := attachInboundPinCandidate(s, &result); err != nil {
		return s, result, err
	}
	return s, result, nil
}

// Exact constructor-only shared tail. nil keeps the old loaded-only behavior:
// its existing Run worker services the loaded journal's strict obligation.
// N2's typed result additionally requires CURRENT strict content confirmation.
func attachInboundPinCandidate(s *Service, installation *ninInstallResult) error {
	store := s.localContactPinStore()
	if store == nil {
		return domain.InboundQuotaNotInstalled
	}
	if err := store.startupRefusal(); err != nil {
		return err
	}
	store.mu.Lock()
	available := store.ninFormat3 && store.ninJournal != nil && !store.memoryOnlyLocked() && store.ninElapsed != nil && !store.pinNetworkStarted
	store.mu.Unlock()
	if !available {
		return domain.InboundQuotaNotInstalled
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		return err
	}
	err = errors.Join(files.Preflight(s.runCtx), files.Close())
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := s.runCtx.Err(); err != nil {
		return err
	}
	if err := store.pinMetadataMutationRefusalLocked(); err != nil {
		return err
	}
	if !store.ninFormat3 || store.ninJournal == nil || store.memoryOnlyLocked() || store.ninElapsed == nil || store.pinNetworkStarted || store.pinOfflineRecovery || store.pinRecovery != nil || (store.pinRemovalScope != nil && !store.pinSupervisedStartup) || s.inboundPins != nil {
		return errNinJournal
	}
	if installation != nil && (installation.owner != store || installation.state != ninInstallReady || !store.ninJournal.strictConfirmed()) {
		return errPinMetadataCommitUnconfirmed
	}
	s.inboundPins = &inboundPinProducer{service: s, store: store, wake: make(chan struct{}, 1)}
	return nil
}

// Arrival capsules carry original transport-only authority through the synchronous
// datagram pipeline. Neither registry lookup nor claimed identity can mint one.
type proofTransportArrivalKey struct{}

func acceptedTransportArrivalContext(ctx context.Context, s *Service, id domain.ConnID, owner *proofTransportOwner) context.Context {
	if owner == nil || owner.life == nil {
		return ctx
	}
	l := owner.life
	l.mu.Lock()
	valid := owner.matchesTransportLocked(s, id) && l.role == sessionv2.RoleListener && !l.outbound
	l.mu.Unlock()
	if valid {
		return context.WithValue(ctx, proofTransportArrivalKey{}, owner)
	}
	return ctx
}

func captureSessionTransportOwner(s *Service, session *peerSession) *proofTransportOwner {
	if session == nil || session.common == nil {
		return nil
	}
	l := session.common
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended || l.token != s.proofToken || l.id != session.connID || l.peer != session || l.role != sessionv2.RoleDialer || !l.outbound {
		return nil
	}
	return l.ownerLocked()
}
func sessionTransportArrivalContext(ctx context.Context, s *Service, session *peerSession) context.Context {
	if owner := captureSessionTransportOwner(s, session); owner != nil {
		return context.WithValue(ctx, proofTransportArrivalKey{}, owner)
	}
	return ctx
}
func transportArrivalOwner(ctx context.Context, s *Service, delivery datagram.DeliveryContext) *proofTransportOwner {
	owner, _ := ctx.Value(proofTransportArrivalKey{}).(*proofTransportOwner)
	channel, ok := delivery.IncomingPeer().Channel()
	if !ok {
		return nil
	}
	id, ok := channel.ConnID()
	if !ok || owner == nil || owner.life == nil {
		return nil
	}
	l := owner.life
	l.mu.Lock()
	defer l.mu.Unlock()
	if !owner.matchesTransportLocked(s, id) || (l.role != sessionv2.RoleListener || l.outbound) && (l.role != sessionv2.RoleDialer || !l.outbound) {
		return nil
	}
	return owner
}

// Immutable service/store/wake. passMu owns cursor and serializes one selection
// and its actual synchronous strict I/O; Run owns the one relative timer.
type inboundPinProducer struct {
	service *Service
	store   *secureSessionStore
	wake    chan struct{}
	passMu  sync.Mutex
	cursor  domain.ConnID
}

func (p *inboundPinProducer) notify() {
	if p == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *inboundPinProducer) run(ctx context.Context) {
	p.runWithFiles(ctx, nil)
}

// One shared scheduler. Normal Run always supplies nil, retaining the original
// per-attempt real-backend ownership. A private fixture may lend the existing
// typed strict-files port; its owner closes it AFTER this synchronous loop joins.
// The port is never retained in the producer and creates no proof/installation.
func (p *inboundPinProducer) runWithFiles(ctx context.Context, borrowed identityPinDurableFiles) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
			// Coalesced observation never advances the retry deadline.
		case <-timer.C:
			if err := p.attempt(ctx, borrowed); err != nil {
				log.Debug().Err(err).Str("node", p.service.identity.Address).Msg("inactive_inbound_pin_attempt_held")
			}
			timer.Reset(peerSweepInterval)
		}
	}
}

func (p *inboundPinProducer) pass(ctx context.Context) error { return p.attempt(ctx, nil) }

// attempt borrows a fixture-owned port only for this call. The live worker
// always opens/closes the real port for actual work, with no new idle I/O.
func (p *inboundPinProducer) attempt(ctx context.Context, files identityPinDurableFiles) (retErr error) {
	p.passMu.Lock()
	defer p.passMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, contactRecordAttemptLimit)
	defer cancel()
	p.store.mu.Lock()
	j := p.store.ninJournal
	owed := j != nil && !j.strictConfirmed()
	if !owed {
		for _, entry := range p.store.identities {
			if entry.classMetadata().Inbound.State == domain.IdentityPinAllocationEligible && !entry.inboundMetadataConfirmed() {
				owed = true
				break
			}
		}
	}
	p.store.mu.Unlock()
	var selected *matureInboundWitness
	if !owed {
		p.service.peerMu.RLock()
		var id domain.ConnID
		selected, id = p.service.selectInboundPinAfterLocked(p.cursor)
		p.service.peerMu.RUnlock()
		if selected == nil {
			return nil
		}
		p.cursor = id
	}
	if files == nil {
		owned, err := p.store.openIdentityPinFiles()
		if err != nil {
			return err
		}
		files = owned
		defer func() { retErr = errors.Join(retErr, owned.Close()) }()
	}
	if owed {
		_, err := p.store.confirmPinMetadata(attemptCtx, files)
		return err
	}
	_, err := p.store.stageInboundPin(attemptCtx, selected, files)
	if err == nil {
		_, err = p.store.confirmPinMetadata(attemptCtx, files)
	}
	return err
}
