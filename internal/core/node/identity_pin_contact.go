package node

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func (s *Service) AddLocalContact(ctx context.Context, intent domain.LocalContactIntent) (domain.LocalContactIntentResult, error) {
	result := domain.LocalContactIntentResult{Identity: intent.Identity}
	store := s.localContactPinStore()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if intent.Identity.IsZero() || intent.Identity.String() == s.identity.Address || intent.Network != domain.NetworkID(networkName) {
		return result, domain.ErrLocalContactIntent
	}
	if err := identity.VerifyBoxKeyBinding(intent.Identity.String(), string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature)); err != nil {
		return result, errors.Join(domain.ErrLocalContactIntent, err)
	}
	applied, appliedRow, applyErr := s.applyTrustedContactOrigin(ctx, intent.Identity.String(), string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "local_contact_intent", contactApplyExplicitLocal)
	if !applied {
		return result, applyErr
	}
	result.Contact = domain.ContactAppliedLive
	if store != nil {
		result.K = store.contactKStatus(intent.Identity)
	}
	if err := s.trust.preflightLocalContact(ctx); err != nil {
		result.Fact = domain.LocalContactFactUnconfirmed
		if errors.Is(err, secretfile.ErrDurabilityUnsupported) {
			result.Fact = domain.LocalContactFactUnsupported
		}
		return result, errors.Join(domain.ErrLocalContactFactUnconfirmed, applyErr, err)
	}
	s.trustMutationMu.Lock()
	stamp, err := s.trust.stageBoundLocalContact(ctx, appliedRow)
	s.trustMutationMu.Unlock()
	if err != nil {
		return result, errors.Join(applyErr, err)
	}
	result.Fact = domain.LocalContactFactUnconfirmed
	if store != nil && result.K == domain.LocalContactKNotEvaluated {
		result.K = domain.LocalContactKAwaitingFactConfirmation
	}
	if err := s.trust.confirmLocalContacts(ctx, stamp); err != nil {
		return result, errors.Join(domain.ErrLocalContactFactUnconfirmed, err)
	}
	result.Fact = s.trust.localContactStatus(stamp)
	if result.Fact != domain.LocalContactFactDurableConfirmed {
		return result, domain.ErrLocalContactFactStale
	}
	if store != nil {
		result.K = store.contactKStatus(intent.Identity)
	}
	if store != nil && (result.K == domain.LocalContactKNotEvaluated || result.K == domain.LocalContactKAwaitingFactConfirmation) {
		result.K = domain.LocalContactKAwaitingFreshProof
		s.trust.mu.RLock()
		row := s.trust.contacts[intent.Identity.String()]
		if stamp.matches(row.LocalContact, s.trust) && row.LocalContact.proof != nil {
			result.K = domain.LocalContactKAwaitingPromotion
		}
		s.trust.mu.RUnlock()
	}
	if s.contactPins != nil {
		s.contactPins.notify()
	}
	return result, nil
}
func (s *secureSessionStore) contactKStatus(id domain.PeerIdentity) domain.LocalContactKState {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.identities[id]
	if !ok || entry.classMetadata().Contact.State != domain.IdentityPinAllocationEligible {
		return domain.LocalContactKNotEvaluated
	}
	if entry.metadata != nil && entry.metadata.recordOwner != nil && entry.metadata.recordMirror != contactRecordOwnerCompleted {
		return domain.LocalContactKStagedUnconfirmed
	}
	if entry.metadata != nil && entry.metadata.confirmedRevision == entry.protectionRevision {
		return domain.LocalContactKConfirmed
	}
	return domain.LocalContactKStagedUnconfirmed
}
func (s *secureSessionStore) stageContactPin(ctx context.Context, binding identityPinEntryBinding, allocation domain.IdentityPinAllocation) (identityPinMetadataMutation, error) {
	if allocation.State != domain.IdentityPinAllocationEligible || allocation.Origin != domain.IdentityPinOriginLocalContact {
		return identityPinMetadataMutation{}, domain.ErrIdentityPinMetadata
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	if !binding.matchesLocked(s) {
		return identityPinMetadataMutation{}, errPinMetadataSnapshotStale
	}
	entry := s.identities[binding.identity]
	metadata := entry.classMetadata()
	if metadata.Contact.State == domain.IdentityPinAllocationEligible {
		return identityPinMetadataMutation{state: pinMetadataUnchangedInMemory, protectionRevision: entry.protectionRevision}, nil
	}
	metadata.Contact = allocation
	if err := metadata.Validate(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	expected := identityPinMetadataSnapshot{owner: s, identity: binding.identity, evidenceRevision: entry.evidenceRevision, protectionRevision: entry.protectionRevision, metadata: entry.classMetadata()}
	return s.stagePinMetadataLocked(expected, metadata)
}
func (s *secureSessionStore) contactConfirmationNeeded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.identities {
		if entry.classMetadata().Contact.State == domain.IdentityPinAllocationEligible && entry.metadata != nil && entry.metadata.confirmedRevision != entry.protectionRevision {
			return true
		}
	}
	return false
}

// K1 facts stay in contact rows; K2 obligations stay in capped pin metadata.
// The joined worker keeps only one selected ticket and coalesces wakeups.
type contactPinProducer struct {
	service         *Service             // immutable after New
	wake            chan struct{}        // coalesced notifications, capacity one
	cursor          *domain.PeerIdentity // worker-owned RR position, never an identity inventory
	reconcileCursor *domain.PeerIdentity // independent one-selection startup cursor
	clock           func() time.Time     // immutable after construction
}

func newContactPinProducer(s *Service) *contactPinProducer {
	return &contactPinProducer{service: s, wake: make(chan struct{}, 1), clock: time.Now}
}
func (p *contactPinProducer) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}
func (s *Service) noteLocalContactProof(proof sessionv2.ProvenIdentity, binding identityPinEntryBinding) {
	if s.contactPins != nil && s.trust.captureLocalContactProof(proof, binding) {
		s.contactPins.notify()
	}
}

const contactRecordAttemptLimit = 2 * time.Second

func (p *contactPinProducer) run(ctx context.Context) {
	ticker := time.NewTicker(peerSweepInterval)
	defer ticker.Stop()
	pacing := time.NewTimer(0)
	defer pacing.Stop()
	next := p.clock()
	p.notify()
	for {
		if wait := next.Sub(p.clock()); wait > 0 {
			pacing.Reset(wait)
			select {
			case <-ctx.Done():
				return
			case <-pacing.C:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.wake:
		}
		next = p.clock().Add(peerSweepInterval)
		if err := p.scan(ctx); err != nil && ctx.Err() == nil {
			p.service.secureSessions.store.logger.Debug().Err(err).Msg("contact_pin_confirmation_deferred")
		}
	}
}

func (p *contactPinProducer) scan(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k1Err := p.scanK1(ctx)
	if err := ctx.Err(); err != nil {
		return errors.Join(k1Err, err)
	}
	reconcileErr := p.scanReconciliation(ctx)
	if err := ctx.Err(); err != nil {
		return errors.Join(k1Err, reconcileErr, err)
	}
	return errors.Join(k1Err, reconcileErr, p.scanOwed(ctx))
}

func (p *contactPinProducer) scanK1(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := p.service
	if s.localContactPinStore() == nil {
		return nil
	}
	facts, targets := p.capture()
	if len(targets) > 0 {
		if err := s.trust.confirmLocalContacts(ctx, targets...); err != nil {
			return err
		}
	}

	store := s.secureSessions.store
	if len(facts) == 0 && !store.contactConfirmationNeeded() {
		return nil
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		return err
	}
	defer func() { _ = files.Close() }()
	if err := files.Preflight(ctx); err != nil {
		return err
	}
	for _, candidate := range facts {
		if err := p.promote(ctx, candidate); err != nil && !errors.Is(err, domain.ErrLocalContactFactStale) && !errors.Is(err, errPinMetadataSnapshotStale) {
			return err
		}
	}
	if !store.contactConfirmationNeeded() {
		return nil
	}
	_, err = store.confirmPinMetadata(ctx, files)
	return err
}
func (p *contactPinProducer) promote(ctx context.Context, candidate *localContactProof) error {
	s := p.service
	s.trustMutationMu.Lock()
	defer s.trustMutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.trust.mu.RLock()
	state := s.trust.contacts[candidate.fact.identity.String()].LocalContact
	valid := candidate.fact.matches(state, s.trust) && state.confirmed == state.revision
	var at time.Time
	if valid {
		at = state.fact.At
	}
	s.trust.mu.RUnlock()
	id, proven := candidate.proof.Identity()
	if !valid || !proven || id != candidate.binding.identity {
		return domain.ErrLocalContactFactStale
	}
	allocation := domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: at, Reference: domain.IdentityPinEvidenceReference(candidate.fact.id.String())}
	_, err := s.secureSessions.store.stageContactPin(ctx, candidate.binding, allocation)
	if err == nil || errors.Is(err, errPinMetadataSnapshotStale) {
		s.trust.mu.Lock()
		row := s.trust.contacts[id.String()]
		if current := row.LocalContact; candidate.fact.matches(current, s.trust) && current.proof == candidate {
			copy := *current
			copy.proof = nil
			row.LocalContact = &copy
			address := id.String()
			publication := s.trust.beginTrustPublication()
			s.trust.contacts[address] = row
			if publication != nil {
				publication.Complete()
			}
		}
		s.trust.mu.Unlock()
	}
	return err
}

// The inventory is a transient snapshot of the capped pin owner, never a scan
// of the uncapped contact book. No two owner mutexes are held together.
func (p *contactPinProducer) capture() ([]*localContactProof, []localContactStamp) {
	s := p.service
	store := s.localContactPinStore()
	if store == nil {
		return nil, nil
	}
	store.mu.Lock()
	ids := make([]domain.PeerIdentity, 0, len(store.identities))
	for id := range store.identities {
		ids = append(ids, id)
	}
	store.mu.Unlock()
	facts := make([]*localContactProof, 0, len(ids))
	var targets []localContactStamp
	s.trust.mu.RLock()
	defer s.trust.mu.RUnlock()
	for _, id := range ids {
		state := s.trust.contacts[id.String()].LocalContact
		if state == nil || state.proof == nil {
			continue
		}
		facts = append(facts, state.proof)
		if state.confirmed != state.revision {
			targets = append(targets, state.stamp(s.trust))
		}
	}
	return facts, targets
}

func (s *Service) localContactPinStore() *secureSessionStore {
	if s.secureSessions == nil {
		return nil
	}
	return s.secureSessions.store
}

type contactRecordSelection struct {
	binding identityPinEntryBinding
	ticket  *contactRecordOwedState
}

// selectContactRecordOwedAfter holds only store.mu for one proportional scan.
// Only one selected reference escapes; no batch survives IO or C3 replacement.
func (s *secureSessionStore) selectContactRecordOwedAfter(cursor *domain.PeerIdentity) *contactRecordSelection {
	s.mu.Lock()
	defer s.mu.Unlock()
	var above, wrap contactRecordSelection
	haveAbove, haveWrap := false, false
	for id, entry := range s.identities {
		if entry.metadata == nil || entry.metadata.recordOwner == nil {
			continue
		}
		switch entry.metadata.recordMirror {
		case contactRecordOwnerPending, contactRecordOwnerCoveredOwed, contactRecordOwnerDeferredInvariant:
		default:
			continue
		}
		selected := contactRecordSelection{binding: s.pinEntryBindingLocked(id), ticket: entry.metadata.recordOwner}
		if !haveWrap || bytes.Compare(id[:], wrap.binding.identity[:]) < 0 {
			wrap, haveWrap = selected, true
		}
		if cursor != nil && bytes.Compare(id[:], cursor[:]) <= 0 {
			continue
		}
		if !haveAbove || bytes.Compare(id[:], above.binding.identity[:]) < 0 {
			above, haveAbove = selected, true
		}
	}
	if haveAbove {
		return &above
	}
	if haveWrap {
		return &wrap
	}
	return nil
}

func (p *contactPinProducer) scanOwed(ctx context.Context) error {
	store := p.service.localContactPinStore()
	if store == nil {
		return nil
	}
	selected := store.selectContactRecordOwedAfter(p.cursor)
	if selected == nil {
		return nil
	}
	cursor := selected.binding.identity
	p.cursor = &cursor
	attemptCtx, cancel := context.WithTimeout(ctx, contactRecordAttemptLimit)
	defer cancel()
	if err := attemptCtx.Err(); err != nil {
		return err
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		return err
	}
	_, attemptErr := p.service.tryCompleteOwedContactRecord(attemptCtx, selected.ticket, files)
	return errors.Join(attemptErr, files.Close())
}
