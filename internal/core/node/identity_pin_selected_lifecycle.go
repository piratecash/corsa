package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

const selectedSessionAge = 10 * time.Minute
const selectedRetryDelay = time.Minute

type selectedSessionProof struct {
	binding identityPinEntryBinding
	group   domain.SourceGroup
	readyAt time.Time
}
type selectedSessionFact struct {
	proof      *selectedSessionProof
	connID     domain.ConnID
	generation uint64
}
type selectedPinProducerOptions struct {
	service *Service
	clock   func() time.Time
}

// One joined worker owns retry pacing. Its only inventory is a bounded scan
// of the existing CM sessions; wakeups coalesce instead of queuing identities.
type selectedPinProducer struct {
	service *Service
	clock   func() time.Time
	wake    chan struct{}
	passMu  sync.Mutex
	retryAt time.Time
}

func newSelectedPinProducer(opts selectedPinProducerOptions) *selectedPinProducer {
	return &selectedPinProducer{service: opts.service, clock: opts.clock, wake: make(chan struct{}, 1)}
}
func (p *selectedPinProducer) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}
func (p *selectedPinProducer) run(ctx context.Context) {
	ticker := time.NewTicker(peerSweepInterval)
	defer ticker.Stop()
	p.notify()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.wake:
		}
		if err := p.scan(ctx); err != nil && ctx.Err() == nil {
			p.service.secureSessions.store.logger.Debug().Err(err).Msg("selected_pin_confirmation_deferred")
		}
	}
}
func (p *selectedPinProducer) capture(now time.Time) []selectedSessionFact {
	s := p.service
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	facts := make([]selectedSessionFact, 0, len(s.sessions))
	for _, session := range s.sessions {
		if session == nil || session.selectedProof == nil || session.selectedGeneration == 0 || session.connID == 0 || session.eligibilityEnded.Load() || session.closedBy.Load() != nil {
			continue
		}
		proof, ok := session.provenIdentity()
		if !ok {
			continue
		}
		id, ok := proof.Identity()
		if !ok || id != session.selectedProof.binding.identity {
			continue
		}
		if now.Sub(session.selectedProof.readyAt) < selectedSessionAge {
			continue
		}
		facts = append(facts, selectedSessionFact{proof: session.selectedProof, connID: session.connID, generation: session.selectedGeneration})
	}
	return facts
}
func (p *selectedPinProducer) scan(ctx context.Context) error {
	p.passMu.Lock()
	defer p.passMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s := p.service
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return nil
	}
	store := s.secureSessions.store
	now := p.clock()
	if now.Before(p.retryAt) {
		return nil
	}
	captured := p.capture(now)
	facts := make([]selectedSessionFact, 0, len(captured))
	for _, fact := range captured {
		status := store.selectedPinStatus(fact.proof.binding)
		if status.current && !status.confirmed {
			facts = append(facts, fact)
		}
	}
	needed := store.selectedConfirmationNeeded()
	if len(facts) == 0 && !needed {
		return nil
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		p.retryAt = now.Add(selectedRetryDelay)
		return err
	}
	defer func() { _ = files.Close() }()
	if err := p.promote(ctx, facts, files); err != nil {
		p.retryAt = now.Add(selectedRetryDelay)
		return err
	}
	p.retryAt = time.Time{}
	return nil
}

// promote consumes facts already witnessed while the actual session was live.
// Backend preflight must precede any class staging; a later disconnect does not
// erase the captured historical fact. Caller serializes worker passes.
func (p *selectedPinProducer) promote(ctx context.Context, facts []selectedSessionFact, files identityPinDurableFiles) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := files.Preflight(ctx); err != nil {
		return err
	}
	store := p.service.secureSessions.store
	for _, fact := range facts {
		if err := ctx.Err(); err != nil {
			return err
		}
		allocation := domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginSelectedSession, At: store.now(), SourceGroup: fact.proof.group, Reference: domain.IdentityPinEvidenceReference(fmt.Sprintf("cm:%d:%d", fact.connID, fact.generation))}
		if _, err := store.stageSelectedPin(ctx, fact.proof.binding, allocation); err != nil && !errors.Is(err, errSelectedPinQuotaFull) && !errors.Is(err, errPinMetadataSnapshotStale) {
			return err
		}
	}
	if !store.selectedConfirmationNeeded() {
		return nil
	}
	_, err := store.confirmPinMetadata(ctx, files)
	return err
}

// The proof publication binding came from this actual TLS handshake's pin
// mutation. A later lookup of the same identity could bind a recreated pin.
func (s *Service) captureSelectedProof(transport *dialledTransport) *selectedSessionProof {
	if s.selectedPins == nil || transport.proven == nil || transport.pinBinding.incarnation == 0 || transport.pinGroup == "" {
		return nil
	}
	proof, ok := transport.proven.Proof()
	if !ok {
		return nil
	}
	id, ok := proof.Identity()
	if !ok || id != transport.pinBinding.identity {
		return nil
	}
	return &selectedSessionProof{binding: transport.pinBinding, group: transport.pinGroup, readyAt: s.selectedPins.clock()}
}
