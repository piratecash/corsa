package node

import (
	"context"
	"math"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Distinct immutable invocation provenance: no transport owner, proof or push
// authority is fabricated from a persisted row. One SAME shared physical slot
// remains occupied until the synchronous attempt, including Close, returns.
type contactRecordReconciliationAuthority struct {
	admission  *relationRecordAdmission
	resolver   *identityResolver
	service    *Service
	network    domain.NetworkID
	index      uint16
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	candidate  *contactRecordCandidate
	revoked    bool
	finished   bool
	staged     bool
}

func (r *identityResolver) reserveContactRecordReconciliation(ctx context.Context) *contactRecordReconciliationAuthority {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.recordAdmission
	if !a.admitsContactReconciliation() || a.closing || r.unavailableLocked() || ctx.Err() != nil {
		return nil
	}
	a.kStageMu.Lock()
	if !a.kReady || a.kClosing || a.resolver != r || a.service != r.svc || a.network != r.network {
		a.kStageMu.Unlock()
		return nil
	}
	for i := range a.slots {
		slot := &a.slots[i]
		if slot.phase != relationAdmissionFree || slot.generation == math.MaxUint64 {
			continue
		}
		generation := slot.generation + 1
		*slot = relationAdmissionSlot{generation: generation, phase: relationAdmissionImmediateKProcessing, processing: true}
		a.kStageMu.Unlock()
		child, cancel := context.WithCancel(ctx)
		g := &contactRecordReconciliationAuthority{admission: a, resolver: r, service: r.svc, network: r.network, index: uint16(i), generation: generation, ctx: child, cancel: cancel}
		a.kStageMu.Lock()
		slot.kReconciliation = g
		a.kStageMu.Unlock()
		return g
	}
	a.kStageMu.Unlock()
	return nil
}

func (r *identityResolver) finishContactRecordReconciliation(g *contactRecordReconciliationAuthority) {
	if g == nil {
		return
	}
	g.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.recordAdmission
	if a != g.admission || g.resolver != r || int(g.index) >= len(a.slots) {
		return
	}
	slot := &a.slots[g.index]
	if slot.kReconciliation != g || slot.generation != g.generation || slot.phase != relationAdmissionImmediateKProcessing {
		return
	}
	a.kStageMu.Lock()
	g.finished = true
	g.candidate = nil
	*slot = relationAdmissionSlot{generation: g.generation}
	a.kStageMu.Unlock()
}

// One lexicographic selection, O(1) retained selection, O(total contacts) scan.
// Capture ORIGINAL fact and original owner/absence before any ACK wait. No
// observeContactRecord after ACK may silently rebind a delete/readd or C3.
func (p *contactPinProducer) captureReconciliation(ctx context.Context) (*contactRecordCandidate, error) {
	s := p.service
	store := s.localContactPinStore()
	if store == nil || s.trust == nil {
		return nil, nil
	}
	s.trustMutationMu.Lock()
	defer s.trustMutationMu.Unlock()
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.trust.pinEvidenceMu.RLock()
	defer s.trust.pinEvidenceMu.RUnlock()
	s.trust.mu.RLock()
	if s.trust.authorityReadErr != nil {
		s.trust.mu.RUnlock()
		return nil, s.trust.authorityReadErr
	}
	network := domain.NetworkID(networkName)
	var selected, wrapped domain.PeerIdentity
	found, wrapFound := false, false
	for address, row := range s.trust.contacts {
		fact := row.LocalContact
		if fact == nil || fact.revision == 0 || fact.fact.Network != network || address == s.identity.Address {
			continue
		}
		id := fact.fact.Identity
		if id.IsZero() || id.String() != address {
			continue
		}
		if _, exists := s.trust.records[trustRecordKey{network: network.String(), address: address}]; !exists {
			continue
		}
		if !wrapFound || id.String() < wrapped.String() {
			wrapped, wrapFound = id, true
		}
		if (p.reconcileCursor == nil || id.String() > p.reconcileCursor.String()) && (!found || id.String() < selected.String()) {
			selected, found = id, true
		}
	}
	if !found {
		selected, found = wrapped, wrapFound
	}
	if !found {
		s.trust.mu.RUnlock()
		return nil, nil
	}
	// Advance on refusal/defer too; one candidate, never an immediate retry loop.
	cursor := selected
	p.reconcileCursor = &cursor
	row := s.trust.contacts[selected.String()]
	current := s.trust.records[trustRecordKey{network: network.String(), address: selected.String()}]
	if len(current.record.Body) == 0 || len(current.record.Body) > domain.MaxIdentityRecordBodyBytes || len(current.record.Sig) != 64 {
		s.trust.mu.RUnlock()
		return nil, errContactRecordEvidence
	}
	candidate := &contactRecordCandidate{service: s, trust: s.trust, store: store, fact: row.LocalContact.stamp(s.trust), factAt: row.LocalContact.fact.At, network: network, record: cloneSignedIdentityRecord(current.record)}
	s.trust.mu.RUnlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	observed, err := store.pinOwnerObservationLocked(selected)
	if err != nil {
		return nil, err
	}
	candidate.observed = observed
	return candidate, nil
}

func (p *contactPinProducer) scanReconciliation(ctx context.Context) error {
	s := p.service
	r := s.identityResolver
	if r == nil || !r.recordAdmission.admitsContactReconciliation() {
		return nil
	}
	attempt, cancel := context.WithTimeout(ctx, contactRecordAttemptLimit)
	defer cancel()
	g := r.reserveContactRecordReconciliation(attempt)
	if g == nil {
		return nil
	}
	// This defer is OUTSIDE staging and all actual backend Close calls.
	defer r.finishContactRecordReconciliation(g)
	candidate, err := p.captureReconciliation(g.ctx)
	if err != nil || candidate == nil {
		return err
	}
	verified, err := verifyContactMinSessionRecord(candidate)
	if err != nil {
		return err
	}
	if err := s.trust.confirmLocalContacts(g.ctx, candidate.fact); err != nil {
		return err
	}
	if s.trust.localContactStatus(candidate.fact) != domain.LocalContactFactDurableConfirmed {
		return domain.ErrLocalContactFactStale
	}
	a := g.admission
	a.kStageMu.Lock()
	if g.revoked || g.finished || a.kClosing || !a.kReady || g.ctx.Err() != nil {
		a.kStageMu.Unlock()
		return domain.ErrLocalContactFactStale
	}
	g.candidate = candidate
	a.kStageMu.Unlock()
	_, err = s.stageContactRecordReconciliation(verified, g)
	return err
}

func (s *Service) stageContactRecordReconciliation(verified verifiedContactMinSessionRecord, guard *contactRecordReconciliationAuthority) (contactRecordStage, error) {
	if guard == nil || guard.admission == nil || guard.ctx == nil {
		return contactRecordStage{}, errContactRecordEvidence
	}
	return s.stageContactRecordWithAuthority(guard.ctx, verified, nil, guard)
}

// Caller holds lifecycle leaf last, after exact evidence and store preparation.
func (guard *contactRecordReconciliationAuthority) validForReconciliationStageLocked(s *Service, candidate *contactRecordCandidate) bool {
	a := guard.admission
	if a == nil || int(guard.index) >= len(a.slots) {
		return false
	}
	slot := &a.slots[guard.index]
	return slot.kReconciliation == guard && slot.kAuthority == nil && slot.generation == guard.generation && slot.processing && slot.phase == relationAdmissionImmediateKProcessing && a.admitsContactReconciliation() && a.kReady && !a.kClosing && !guard.revoked && !guard.finished && !guard.staged && guard.ctx.Err() == nil && guard.service == s && guard.resolver == a.resolver && s.identityResolver == guard.resolver && a.network == guard.resolver.network && a.service == s && a.network == guard.network && candidate != nil && candidate == guard.candidate && candidate.service == s && candidate.network == guard.network && candidate.store == s.localContactPinStore()
}
