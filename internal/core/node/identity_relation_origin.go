package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

type identityLookupOrigin uint8

const (
	identityLookupOriginNone identityLookupOrigin = iota
	identityLookupOriginWindowUser
)

type identityLookupActionGeneration uint64
type identityLookupActionStamp struct {
	origin     identityLookupOrigin
	generation identityLookupActionGeneration
}
type identityAttemptQueue uint8

const (
	identityAttemptQueuePending identityAttemptQueue = iota
	identityAttemptQueueAccepted
	identityAttemptQueueRefused
)

// UserIdentityLookup is an opaque runtime action handle. No wire/API input can
// construct its private owner/generation. It grants no pin/admission authority.
type UserIdentityLookup struct {
	owner        *identityResolver
	target       domain.PeerIdentity
	generation   identityLookupActionGeneration
	resolutionID string
}

func (a UserIdentityLookup) ResolutionID() string { return a.resolutionID }

// StartUserIdentityLookup is the in-process Window port, never a Frame command.
func (s *Service) StartUserIdentityLookup(ctx context.Context, target domain.PeerIdentity) (UserIdentityLookup, error) {
	if err := ctx.Err(); err != nil {
		return UserIdentityLookup{}, err
	}
	if s.identityResolver == nil {
		return UserIdentityLookup{}, errors.New("identity lookup: resolver unavailable")
	}
	state, action, err := s.identityResolver.startResolution(ctx.Err, target, identityIntentReason{Type: identityIntentReasonUIChat}, true)
	if err != nil {
		return UserIdentityLookup{}, err
	}
	return UserIdentityLookup{owner: s.identityResolver, target: target, generation: action.generation, resolutionID: state.ResolutionID}, nil
}

// CancelUserIdentityLookup is explicit expected-generation cancellation. Window
// navigation/close does not call it; return from Start does not cancel lookup.
func (s *Service) CancelUserIdentityLookup(ctx context.Context, action UserIdentityLookup) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if action.owner == nil || action.owner != s.identityResolver || action.generation == 0 {
		return errors.New("identity lookup: invalid action owner")
	}
	return action.owner.cancelUserAction(action)
}

// identityLookupCompletion is ephemeral soft provenance. R0 logs and discards
// it; R1 cannot replay it as pin authority without a fresh pre-attempt owner fact.
type identityLookupCompletion struct {
	target            domain.PeerIdentity
	resolutionID      string
	label             domain.PeerIdentity
	attemptGeneration uint64
	action            identityLookupActionStamp
	sequence          domain.IdentityRecordSeq
	minSession        domain.IdentityRecordMinSession
	recordDigest      [sha256.Size]byte
}

func observeIdentityLookupCompletion(completion *identityLookupCompletion) {
	if completion == nil {
		return
	}
	log.Debug().Str("target", completion.target.String()).Str("resolution_id", completion.resolutionID).
		Uint64("attempt_generation", completion.attemptGeneration).Uint64("action_generation", uint64(completion.action.generation)).
		Str("origin", "window_user_lookup").Str("queue", "accepted").Msg("identity_lookup_soft_completion")
}

// settleClaimLocked consumes an accepted claimed row once. Pending queue rows
// stay in the EXISTING attempt table only. Caller holds resolver.mu.
func (r *identityResolver) settleClaimLocked(label domain.PeerIdentity) *identityLookupCompletion {
	entry, ok := r.attempts[label]
	if !ok || entry.claim == nil {
		return nil
	}
	if entry.queue == identityAttemptQueuePending && !entry.revoked && !r.unavailableLocked() && entry.liveAt(r.clock()) {
		return nil
	}
	if entry.queue == identityAttemptQueueAccepted && !entry.revoked && !r.unavailableLocked() && entry.liveAt(r.clock()) {
		r.settleRelationClaimLocked(entry)
	} else {
		r.discardRelationAttemptLocked(entry)
	}
	delete(r.attempts, label)
	if entry.queue != identityAttemptQueueAccepted || entry.revoked || r.unavailableLocked() || !entry.liveAt(r.clock()) {
		return nil
	}
	completion := *entry.claim
	return &completion
}

// actionForLocked reads the sole reason owner while resolver.mu is held.
func (r *identityResolver) actionForLocked(target domain.PeerIdentity) identityLookupActionStamp {
	r.intents.mu.Lock()
	defer r.intents.mu.Unlock()
	row, ok := r.intents.rows[identityIntentKey{target: target, reason: identityIntentReason{Type: identityIntentReasonUIChat}}]
	if !ok {
		return identityLookupActionStamp{}
	}
	return row.action
}
func (r *identityResolver) nextActionLocked() (identityLookupActionStamp, error) {
	if r.actionGen == identityLookupActionGeneration(math.MaxUint64) {
		return identityLookupActionStamp{}, errors.New("identity lookup: action generation exhausted")
	}
	r.actionGen++
	return identityLookupActionStamp{origin: identityLookupOriginWindowUser, generation: r.actionGen}, nil
}

func (r *identityResolver) cancelUserAction(action UserIdentityLookup) error {
	r.mu.Lock()
	clearedRuntimeAction := false
	if r.recordAdmission != nil {
		r.cancelRelationActionLocked(action)
		r.revokeActionLocked(action.target, action.generation)
		r.intents.mu.Lock()
		key := identityIntentKey{target: action.target, reason: identityIntentReason{Type: identityIntentReasonUIChat}}
		if row, ok := r.intents.rows[key]; ok && row.action.generation == action.generation {
			row.action = identityLookupActionStamp{}
			r.intents.rows[key] = row
			clearedRuntimeAction = true
		}
		r.intents.mu.Unlock()
	}
	_, snapshot, err := r.intents.removeMemory(action.target, func(key identityIntentKey, row identityIntentRow) bool {
		return key.reason == identityIntentReason{Type: identityIntentReasonUIChat} && ((row.action.generation == action.generation && row.action.origin == identityLookupOriginWindowUser) || clearedRuntimeAction)
	})
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.revokeActionLocked(action.target, action.generation)
	var terminal ebus.IdentityResolutionState
	finished := false
	r.intents.mu.Lock()
	remaining := r.intents.countLocked(action.target)
	r.intents.mu.Unlock()
	if remaining == 0 {
		if res, ok := r.resolutions[action.target]; ok {
			terminal, finished, _ = r.finishLocked(res, domain.IdentityResolutionCancelled, nil)
		}
	}
	r.mu.Unlock()
	if finished {
		r.emit(terminal, nil)
	}
	return r.intents.persist(snapshot)
}

// revokeActionLocked never changes an immutable stamp. Caller holds resolver.mu.
func (r *identityResolver) revokeActionLocked(target domain.PeerIdentity, generation identityLookupActionGeneration) {
	for label, entry := range r.attempts {
		if entry.dst != target || entry.action.generation != generation || entry.action.origin == identityLookupOriginNone {
			continue
		}
		r.discardRelationAttemptLocked(entry)
		entry.revoked = true
		if entry.claim != nil {
			delete(r.attempts, label)
		} else {
			r.attempts[label] = entry
		}
	}
}

// stop revokes runtime authority without deleting resumable durable intents.
func (r *identityResolver) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.stopRecordAdmissionLocked()
	clear(r.attempts)
	r.intents.mu.Lock()
	defer r.intents.mu.Unlock()
	for key, row := range r.intents.rows {
		row.action = identityLookupActionStamp{}
		r.intents.rows[key] = row
	}
}

func recordRequiresSessionV2(minimum domain.IdentityRecordMinSession) bool {
	version, declared := minimum.Version()
	return declared && version >= domain.SessionVersionSecure
}

// unavailableLocked reads only resolver-owned immutable lifecycle context.
// Caller holds resolver.mu; the request context never becomes this context.
func (r *identityResolver) unavailableLocked() bool {
	return r.closed || (r.ownerContext != nil && r.ownerContext.Err() != nil)
}
func (entry identityAttemptEntry) liveAt(now time.Time) bool {
	return !now.Before(entry.sentAt) && now.Sub(entry.sentAt) <= identityLookupAttemptWindow
}
