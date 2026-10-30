package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Proposal input only. No deployment/default duration is supplied by any root.
type OwnDMResidenceProfile struct{ MaxPrestage time.Duration }

func (p OwnDMResidenceProfile) valid() bool {
	return p.MaxPrestage > 0 && p.MaxPrestage < time.Duration(math.MaxInt64)
}

var errOwnDMResidence = errors.New("own-DM record admission residence unavailable")

type ownDMRecordAdmission struct {
	service   *Service
	resolver  *identityResolver
	admission *relationRecordAdmission
	profile   OwnDMResidenceProfile
}

// Immutable after prepare; Run's original nonzero-sized started pointer is its
// incarnation token. The Done channel retains no context values or waiter.
type ownDMResidenceOrigin struct {
	owner     *ownDMRecordAdmission
	started   *relationObservedTime
	at        relationObservedTime
	deadline  time.Duration
	done      <-chan struct{}
	ttlExpiry time.Time
}
type ownDMEarnedRole struct {
	ref     relationAdmissionRef
	binding identityPinEntryBinding
}

// deliveryMu. Fixed two roles, not another queue/history. Slot refs never rebind.
type ownDMRelationClaim struct {
	stamp     *r2aOwnSendStamp
	refs      [2]relationAdmissionRef
	earned    [2]ownDMEarnedRole
	delegated bool
}
type verifiedOwnDMRelationRecord struct {
	claim    *ownDMRelationClaim
	role     uint8
	target   domain.PeerIdentity
	network  domain.NetworkID
	sequence domain.IdentityRecordSeq
}

// Unused INTERNAL construction root; validates before creating stores/Service.
func NewInactiveOwnDMAdmissionCandidate(cfg config.Node, id *identity.Identity, bus *ebus.Bus, profile OwnDMResidenceProfile) (*Service, OwnDMProvenancePort, error) {
	if !profile.valid() || id == nil {
		return nil, OwnDMProvenancePort{}, errOwnDMResidence
	}
	s := newServiceWithRecordAdmissionClasses(cfg, id, bus, nil, productionDatagramConstructor{}, recordAdmissionConstruction{classes: recordAdmissionChargedR})
	return s, newOwnDMAdmissionPort(s, profile), nil
}

// Sole charged-R own-DM owner mint, before either construction root escapes.
func newOwnDMAdmissionPort(s *Service, profile OwnDMResidenceProfile) OwnDMProvenancePort {
	if s == nil || !profile.valid() || s.identityResolver == nil || !s.identityResolver.recordAdmission.admitsR() || s.r2OwnDMOwner != nil {
		panic("invalid own-DM construction binding")
	}
	owner := &ownDMProvenanceOwner{r2Service: s, r2Identity: s.identity}
	owner.r2Admission = &ownDMRecordAdmission{service: s, resolver: s.identityResolver, admission: s.identityResolver.recordAdmission, profile: profile}
	s.r2OwnDMOwner = owner
	return OwnDMProvenancePort{r2Capability: owner}
}

// Called at NODE Submit entry, before its prepare/verification/store. Earlier
// service encryption/key lookup is outside this original admission boundary.
func (p OwnDMProvenancePort) captureOwnDMResidence(ctx context.Context) *ownDMResidenceOrigin {
	if p.r2Capability == nil || p.r2Capability.r2Admission == nil {
		return nil
	}
	o := p.r2Capability.r2Admission
	r := o.resolver
	if r == nil || o.service != r.svc || o.admission != r.recordAdmission || !o.profile.valid() {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a := o.admission
	if !a.admitsR() || a.started == nil || a.closing || r.unavailableLocked() || ctx.Err() != nil || o.service.runCtx.Err() != nil {
		return nil
	}
	now, ok := a.sampleLocked()
	if !ok || now.elapsed > time.Duration(math.MaxInt64-1)-o.profile.MaxPrestage {
		return nil
	}
	return &ownDMResidenceOrigin{owner: o, started: a.started, at: now, deadline: now.elapsed + o.profile.MaxPrestage, done: ctx.Done()}
}

// Clone the sealed prepared value, never replace a previously stamped origin.
func (s *Service) bindOwnDMResidence(prepared r2aPreparedOwnDM, origin *ownDMResidenceOrigin) r2aPreparedOwnDM {
	if origin == nil || origin.owner.service != s || prepared.sealed == nil || prepared.sealed.owner != s {
		return prepared
	}
	value := *origin
	msg := prepared.sealed.message
	if msg.TTLSeconds > 0 {
		if uint64(msg.TTLSeconds) > uint64(math.MaxInt64/int64(time.Second)) {
			return prepared
		}
		value.ttlExpiry = msg.CreatedAt.Add(time.Duration(msg.TTLSeconds) * time.Second)
		remaining := value.ttlExpiry.Sub(value.at.wall)
		if remaining <= 0 {
			return prepared
		}
		if remaining < value.deadline-value.at.elapsed {
			value.deadline = value.at.elapsed + remaining
		}
	}
	clone := *prepared.sealed
	clone.r2Residence = &value
	return r2aPreparedOwnDM{sealed: &clone}
}

// resolver.mu. No resampling at ACK/queue may replace origin.at/deadline/token.
func (o *ownDMResidenceOrigin) liveLocked(r *identityResolver, now relationObservedTime) bool {
	if o == nil || o.owner == nil || o.owner.resolver != r || o.owner.service != r.svc || o.owner.admission != r.recordAdmission || !o.owner.profile.valid() {
		return false
	}
	a := r.recordAdmission
	if a == nil || a.started != o.started || o.started == nil || a.closing || a.clockFailed || r.unavailableLocked() || r.svc.runCtx.Err() != nil || now.elapsed < o.at.elapsed || now.elapsed >= o.deadline {
		return false
	}
	select {
	case <-o.done:
		return false
	default:
	}
	return o.ttlExpiry.IsZero() || now.wall.Before(o.ttlExpiry)
}

// Immutable identity/observation projection; caller holds deliveryMu when it
// consults the claim's mutable earned/refs/delegated members.
func (c *ownDMRelationClaim) ownDMObservation(role uint8) pinOwnerObservation {
	if role == 0 {
		return c.stamp.prepared.recipient
	}
	b := c.stamp.candidate.hop.binding
	return pinOwnerObservation{owner: b.owner, identity: b.identity, kind: pinOwnerExisting, incarnation: b.incarnation}
}
func (c *ownDMRelationClaim) ownersMatchLocked(store *secureSessionStore) bool {
	for role := uint8(0); role < 2; role++ {
		original := c.ownDMObservation(role)
		if original.matchesLocked(store) {
			continue
		}
		e := c.earned[role]
		if e.ref.owner == nil || e.ref != c.refs[role] || e.binding.identity != original.identity || !e.binding.matchesLocked(store) {
			return false
		}
	}
	return true
}

// Optional Submit-tail registration; ordinary reply/storage is never changed.
func (s *Service) registerOwnDMRecordAdmission(ctx context.Context, result r2aSubmissionResult) {
	stamp := result.stamp
	if stamp == nil || stamp.prepared.r2Residence == nil {
		return
	}
	origin := stamp.prepared.r2Residence
	r := origin.owner.resolver
	store := s.localContactPinStore()
	if store == nil || r == nil {
		return
	}
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if stamp.r2Claim != nil {
		return
	}
	// Record this one original decision even if capacity is full or already stale.
	c := &ownDMRelationClaim{stamp: stamp}
	stamp.r2Claim = c
	now, ok := r.recordAdmission.sampleLocked()
	if !ok || !origin.liveLocked(r, now) || ctx.Err() != nil || !s.r2aCurrentLocked(stamp) || stamp.consumed || stamp.candidate == nil || !stamp.candidate.offered || s.deliveryFrozenLocked(s.ownMessageIdentity(stamp.prepared.message.ID, stamp.prepared.message.Recipient)) || store.pinMetadataMutationRefusalLocked() != nil || !c.ownersMatchLocked(store) {
		return
	}
	for role := uint8(0); role < 2; role++ {
		observed := c.ownDMObservation(role)
		if role == 1 && observed.identity == c.ownDMObservation(0).identity {
			break
		}
		for i := range r.recordAdmission.slots {
			slot := &r.recordAdmission.slots[i]
			if slot.phase != relationAdmissionFree || slot.generation == math.MaxUint64 {
				continue
			}
			generation := slot.generation + 1
			c.refs[role] = relationAdmissionRef{owner: r.recordAdmission, index: uint16(i), generation: generation}
			*slot = relationAdmissionSlot{generation: generation, phase: relationAdmissionProspective, observed: observed, registered: now.elapsed, r2Claim: c, r2Role: role, protectionHeld: true}
			s.pinRecordProtection(observed.identity)
			break
		}
	}
	r.wakeRecordAdmissionLocked()
}

// Combined final order: gateR -> evidenceR -> delivery -> resolver -> trustR
// -> store. Never call this while holding resolver.mu. No crypto/I/O inside.
func (r *identityResolver) lockOwnDMRecord() (*secureSessionStore, func()) {
	store := r.svc.localContactPinStore()
	store.pinMutationGate.RLock()
	r.svc.trust.pinEvidenceMu.RLock()
	r.svc.deliveryMu.Lock()
	r.mu.Lock()
	r.svc.trust.mu.RLock()
	store.mu.Lock()
	return store, func() {
		store.mu.Unlock()
		r.svc.trust.mu.RUnlock()
		r.mu.Unlock()
		r.svc.deliveryMu.Unlock()
		r.svc.trust.pinEvidenceMu.RUnlock()
		store.pinMutationGate.RUnlock()
	}
}
func (r *identityResolver) ownDMClaimCurrentLocked(slot *relationAdmissionSlot, store *secureSessionStore, now relationObservedTime) bool {
	c := slot.r2Claim
	if c == nil || slot.r2Role > 1 || r.relationSlotLocked(&c.refs[slot.r2Role]) != slot || slot.cancelled || !slot.processing || c.stamp.r2Claim != c {
		return false
	}
	s := r.svc
	return c.stamp.prepared.r2Residence.liveLocked(r, now) && s.r2aCurrentLocked(c.stamp) && !s.deliveryFrozenLocked(s.ownMessageIdentity(c.stamp.prepared.message.ID, c.stamp.prepared.message.Recipient)) && c.ownersMatchLocked(store)
}

func verifyOwnDMRelationRecord(c *ownDMRelationClaim, role uint8, network domain.NetworkID, record protocol.SignedIdentityRecord) (verifiedOwnDMRelationRecord, error) {
	if c == nil || role > 1 {
		return verifiedOwnDMRelationRecord{}, errRelationPinRecord
	}
	target := c.ownDMObservation(role).identity
	body, err := protocol.VerifyIdentityRecord(record, network, target)
	if err != nil || !recordRequiresSessionV2(body.MinSession) {
		return verifiedOwnDMRelationRecord{}, errors.Join(errRelationPinRecord, err)
	}
	return verifiedOwnDMRelationRecord{claim: c, role: role, target: target, network: network, sequence: body.Seq}, nil
}

// Data-only bounded ownership copy, not verification or stage authority. The
// caller holds the existing slot/trust locks. Preserve received Version verbatim;
// unsupported versions must reach Verify unchanged, never be normalized to1.
func copyOwnDMRelationRecord(slot *relationAdmissionSlot, current protocol.SignedIdentityRecord) (protocol.SignedIdentityRecord, error) {
	if slot == nil || len(current.Body) > len(slot.body) || len(current.Sig) != len(slot.signature) {
		return protocol.SignedIdentityRecord{}, errRelationPinRecord
	}
	slot.bodyLen = copy(slot.body[:], current.Body)
	copy(slot.signature[:], current.Sig)
	return protocol.SignedIdentityRecord{Version: current.Version, Body: append([]byte(nil), slot.body[:slot.bodyLen]...), Sig: append([]byte(nil), slot.signature[:]...)}, nil
}

// One existing worker pass: nonblocking ACK, then bounded copy, crypto outside
// locks, then exact recheck/FIFO admission. Does not open a backend or stage.
func (r *identityResolver) prepareOwnDMRelation(ctx context.Context, slot *relationAdmissionSlot) (terminal bool, err error) {
	store, unlock := r.lockOwnDMRecord()
	now, ok := r.recordAdmission.sampleLocked()
	if ctx.Err() != nil || !ok || !r.ownDMClaimCurrentLocked(slot, store, now) {
		unlock()
		return true, errRelationAdmissionStale
	}
	c := slot.r2Claim
	if !c.delegated {
		_, _, err := r.svc.settleR2aOwnHopLocked(ctx, c.stamp)
		if err != nil {
			unlock()
			return !errors.Is(err, errR2aNotWritten), err
		}
		c.delegated = true
	}
	current, present := r.svc.trust.recordLocked(trustRecordKey{network: r.network.String(), address: slot.observed.identity.String()})
	if r.svc.trust.authorityReadErr != nil || !present {
		unlock()
		return true, errRelationPinRecord
	}
	// Private bounded arrays, no borrowed trust-store backing survives unlock.
	record, copyErr := copyOwnDMRelationRecord(slot, current.record)
	if copyErr != nil {
		unlock()
		return true, copyErr
	}
	role := slot.r2Role
	unlock()
	verified, err := verifyOwnDMRelationRecord(c, role, r.network, record)
	if err != nil {
		return true, err
	}
	store, unlock = r.lockOwnDMRecord()
	defer unlock()
	now, ok = r.recordAdmission.sampleLocked()
	if ctx.Err() != nil || !ok || !r.ownDMClaimCurrentLocked(slot, store, now) || !c.delegated || r.recordAdmission.fifo == math.MaxUint64 {
		return true, errRelationAdmissionStale
	}
	current, present = r.svc.trust.recordLocked(trustRecordKey{network: r.network.String(), address: slot.observed.identity.String()})
	if r.svc.trust.authorityReadErr != nil || !present || current.record.Version != record.Version || current.body.Seq != verified.sequence || !recordRequiresSessionV2(current.body.MinSession) || !bytes.Equal(current.record.Body, record.Body) || !bytes.Equal(current.record.Sig, record.Sig) {
		return true, errRelationPinRecord
	}
	r.recordAdmission.fifo++
	slot.fifo = r.recordAdmission.fifo
	slot.r2Verified = verified
	slot.phase = relationAdmissionQueued
	r.wakeRecordAdmissionLocked()
	return false, nil
}

func (s *secureSessionStore) stageOwnDMRelationRecordLocked(observed pinOwnerObservation, record verifiedOwnDMRelationRecord) (relationPinStage, error) {
	if record.claim == nil || record.role > 1 || record.target != observed.identity || record.claim.ownDMObservation(record.role) != observed || record.network != record.claim.stamp.prepared.r2Residence.owner.resolver.network {
		return relationPinStage{}, errRelationPinRecord
	}
	origin := domain.IdentityPinOriginOutgoingSend
	if record.role == 1 {
		origin = domain.IdentityPinOriginProvenNextHop
	}
	digest := sha256.Sum256([]byte(string(record.claim.stamp.prepared.message.ID) + ":" + hex.EncodeToString(record.claim.stamp.prepared.digest[:])))
	return s.stageClassifiedRelationRecordLocked(observed, origin, domain.IdentityPinEvidenceReference(hex.EncodeToString(digest[:])))
}

func (r *identityResolver) stageOwnDMRelationAdmission(ctx context.Context, slot *relationAdmissionSlot) (relationAdmissionDisposition, error) {
	store, unlock := r.lockOwnDMRecord()
	defer unlock()
	now, ok := r.recordAdmission.sampleLocked()
	v := slot.r2Verified
	if ctx.Err() != nil {
		return relationAdmissionWait, ctx.Err()
	}
	if !ok || slot.phase != relationAdmissionQueued || !r.ownDMClaimCurrentLocked(slot, store, now) || !slot.r2Claim.delegated || v.claim != slot.r2Claim || v.role != slot.r2Role || v.target != slot.observed.identity || v.network != r.network {
		return relationAdmissionWait, errRelationAdmissionStale
	}
	current, present := r.svc.trust.recordLocked(trustRecordKey{network: r.network.String(), address: slot.observed.identity.String()})
	if r.svc.trust.authorityReadErr != nil || !present || current.record.Version != domain.IdentityRecordVersion || current.body.Seq != v.sequence || !recordRequiresSessionV2(current.body.MinSession) || !bytes.Equal(current.record.Body, slot.body[:slot.bodyLen]) || !bytes.Equal(current.record.Sig, slot.signature[:]) {
		return relationAdmissionWait, errRelationPinRecord
	}
	metadata := store.identities[slot.observed.identity].classMetadata()
	if _, present := store.identities[slot.observed.identity]; !present && store.pinRecovery != nil && store.pinRecovery.permit != nil && store.pinRecovery.permit.Binding.Identity == slot.observed.identity {
		b := store.pinRecovery.permit.Binding
		backup, err := b.Backup.Restore(b.Identity, b.PreviousPinnedAt)
		if err != nil {
			return relationAdmissionWait, err
		}
		metadata = backup.Metadata
	}
	contact := r.svc.trust.contacts[slot.observed.identity.String()].LocalContact
	if metadata.Contact.State == domain.IdentityPinAllocationEligible || (contact != nil && contact.revision != 0 && contact.confirmed == contact.revision && contact.fact.Network == r.network && contact.fact.Identity == slot.observed.identity) {
		return relationAdmissionContactPath, nil
	}
	trial, ready := r.recordAdmission.rateTrial(now)
	if !ready {
		return relationAdmissionWait, nil
	}
	stage, err := store.stageOwnDMRelationRecordLocked(slot.observed, v)
	if err != nil {
		return relationAdmissionWait, err
	}
	// Infallible in-memory stage -> earned ownership -> debit. Not strict ACK.
	slot.owed = owedRelationProtection{service: r.svc, binding: stage.binding, relation: store.identities[slot.observed.identity].classMetadata().Relation}
	slot.completion = relationRecordCompletion{state: relationRecordStagedOwed}
	slot.phase = relationAdmissionEarnedOwed
	slot.r2Claim.earned[slot.r2Role] = ownDMEarnedRole{ref: slot.r2Claim.refs[slot.r2Role], binding: stage.binding}
	r.recordAdmission.rate = trial
	clear(slot.body[:])
	clear(slot.signature[:])
	slot.bodyLen = 0
	slot.r2Verified = verifiedOwnDMRelationRecord{}
	return relationAdmissionStaged, nil
}
