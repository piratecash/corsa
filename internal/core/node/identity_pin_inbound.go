package node

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// N1 binds this leaf only to the ORIGINAL accepted proof and registration.
// N0 fixtures remain explicitly store-level and do not create Service authority.
type matureInboundWitness struct {
	transport *proofTransportLifetime
	phase     inboundLifetimePhase
	producer  *inboundPinProducer
	binding   identityPinEntryBinding
	session   sessionv2.ProvenSession
	group     domain.SourceGroup
	ready     time.Duration
}

type inboundPinStageState uint8

const (
	inboundPinNotEvaluated inboundPinStageState = iota
	inboundPinAlreadyEligible
	inboundPinCountedUnconfirmed
)

type inboundPinStage struct {
	state      inboundPinStageState
	binding    identityPinEntryBinding
	generation uint64
	revision   identityPinProtectionRevision
}

var errNinWitness = errors.New("inbound quota witness stale, incomplete or not mature")

// stageInboundPin is consumed only by the private candidate producer.
// Preflight occurs outside all owner locks.
// Order: removal gate.R → lifetime leaf → store.mu. No I/O under either leaf.
func (s *secureSessionStore) stageInboundPin(ctx context.Context, witness *matureInboundWitness, files identityPinDurableFiles) (inboundPinStage, error) {
	if err := ctx.Err(); err != nil {
		return inboundPinStage{}, err
	}
	s.mu.Lock()
	installed, attached, memory := s.ninFormat3, s.ninJournal != nil, s.memoryOnlyLocked()
	s.mu.Unlock()
	if !installed {
		return inboundPinStage{}, domain.InboundQuotaNotInstalled
	}
	if !attached || memory {
		return inboundPinStage{}, errNinJournal
	}
	if witness == nil || witness.transport == nil || files == nil {
		return inboundPinStage{}, errNinWitness
	}
	if err := files.Preflight(ctx); err != nil {
		return inboundPinStage{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	witness.transport.mu.Lock()
	defer witness.transport.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return inboundPinStage{}, err
	}
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return inboundPinStage{}, err
	}
	if !witness.binding.matchesLocked(s) || witness.transport.ended || witness.phase != inboundPublished || witness.transport.id == 0 {
		return inboundPinStage{}, errNinWitness
	}
	peer, err := witness.session.Peer()
	if err != nil || peer.Identity != witness.binding.identity {
		return inboundPinStage{}, errNinWitness
	}
	role, err := witness.session.Role()
	if err != nil || role != sessionv2.RoleListener {
		return inboundPinStage{}, errNinWitness
	}
	s.sampleNinQuotaLocked()
	j := s.ninJournal
	if j == nil || j.clockReason != domain.InboundQuotaReady || witness.ready < 0 || j.last < witness.ready || j.last-witness.ready < 10*time.Minute {
		return inboundPinStage{}, errNinWitness
	}
	entry := s.identities[witness.binding.identity]
	next := entry.classMetadata()
	if next.Inbound.State == domain.IdentityPinAllocationEligible {
		return inboundPinStage{state: inboundPinAlreadyEligible, binding: witness.binding, generation: j.generation, revision: entry.protectionRevision}, nil
	}
	if reason := j.reason(); reason != domain.InboundQuotaReady {
		return inboundPinStage{}, reason
	}
	count, err := s.inboundAllocationsLocked()
	if err != nil {
		return inboundPinStage{}, err
	}
	if next.Inbound.State == domain.IdentityPinAllocationAbsent && count >= maxInboundPins {
		return inboundPinStage{}, domain.InboundQuotaClassFull
	}
	key, err := ninKeyOf(witness.group)
	if err != nil {
		return inboundPinStage{}, err
	}
	index, found := j.find(key)
	if !found && j.rows == ninGroupCapacity {
		j.prune()
		index, found = j.find(key)
	}
	if !found && j.rows == ninGroupCapacity {
		return inboundPinStage{}, domain.InboundQuotaHistoryFull
	}
	row := ninRow{key: key}
	if found {
		row = pruneNinRow(j.row(index), j.tick)
	}
	if row.count >= ninEventsPerGroup {
		return inboundPinStage{}, domain.InboundQuotaGroupFull
	}
	if s.protectionRevision == math.MaxUint64 || s.changes == math.MaxUint64 || j.generation == math.MaxUint64 {
		return inboundPinStage{}, domain.ErrIdentityPinRevisionExhausted
	}
	next.Inbound = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginInboundSession, At: s.now(), SourceGroup: witness.group}
	if err := entry.setUnpublishedMetadata(next); err != nil {
		return inboundPinStage{}, err
	}
	// All fallible validation precedes the two owner publications.
	if j.arena == nil {
		j.arena = new([ninGroupCapacity]ninRow)
	}
	if !found {
		copy(j.arena[index+1:j.rows+1], j.arena[index:j.rows])
		j.rows++
	}
	row.ticks[row.count] = j.tick
	row.count++
	j.arena[index] = row
	s.protectionRevision++
	entry.protectionRevision = s.protectionRevision
	s.putPinEntryLocked(witness.binding.identity, entry)
	j.generation++
	s.changes++
	return inboundPinStage{state: inboundPinCountedUnconfirmed, binding: witness.binding, generation: j.generation, revision: entry.protectionRevision}, nil
}
