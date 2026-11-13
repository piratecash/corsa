package node

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// Observation ceilings. Every begin event carries the membership of its exact
// encoded image. These bounds neither change store capacity nor promise coverage.
const pinWriteMaxRetainedIdentities = 2 * maxPinnedIdentities

type pinWriteObservationEvent struct {
	Sequence, Operation, MembershipSequence uint64
	Elapsed                                 time.Duration
	Kind                                    string
	Identities                              []domain.PeerIdentity `json:",omitempty"`
}

type pinWriteObservationSnapshot struct {
	Status, Reason string
	// When true, the parent BaselineIdentities were confirmed on disk by
	// startup and are usable from the parent Baseline.After.
	BaselineConfirmed bool
	Pending           uint64
	Events            []pinWriteObservationEvent
	GuardCoverage     *pinWriteGuardCoverage `json:",omitempty"`
}

// A pin removal or offline recovery attempt can close the covered prefix.
// The gap persists for the original store epoch.
type pinWriteGuardCoverage struct {
	Gap  bool
	From time.Duration
}

// Owned by the original store.mu. The store writer owns writeMu across a
// begin/end pair. Published rows and their image slices are immutable.
type pinWriteObservation struct {
	events            []pinWriteObservationEvent
	capacity          int
	identities        int
	pending           uint64
	fault             string
	baselineConfirmed bool
	guard             pinWriteGuardCoverage
}

type pinWriteObservationTicket struct {
	source    *pinWriteObservation
	operation uint64
}

func newPinWriteObservation(capacity int, startupConfirmed bool) *pinWriteObservation {
	w := &pinWriteObservation{capacity: capacity}
	if runtime.GOOS == "windows" {
		w.fault = "pin-write-durability-unverified"
	} else {
		w.baselineConfirmed = startupConfirmed
	}
	return w
}

// Caller holds store.mu. Sample before the original effect can escape mu.
func (s *secureSessionStore) markK3PinGuardGapLocked() {
	o := s.pinMembership
	if o == nil || o.writes == nil || o.writes.fault != "" || o.writes.guard.Gap {
		return
	}
	if elapsed, valid := o.readClock(); valid {
		o.writes.guard = pinWriteGuardCoverage{Gap: true, From: elapsed}
	}
}

func (s *secureSessionStore) beginPinWriteObservation(ids map[domain.PeerIdentity]time.Time) *pinWriteObservationTicket {
	o := s.pinMembership
	if o == nil || o.writes == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w := o.writes
	if w.fault != "" || o.fault != "" {
		return nil
	}
	if w.pending != 0 {
		w.fault = "overlapping-pin-writes"
		return nil
	}
	if len(w.events)+2 > w.capacity || len(ids) > pinWriteMaxRetainedIdentities-w.identities {
		w.fault = "pin-write-history-capacity"
		return nil
	}
	image := make([]domain.PeerIdentity, 0, len(ids))
	for id := range ids {
		if id.IsZero() {
			w.fault = "invalid-pin-write-image"
			return nil
		}
		image = append(image, id)
	}
	elapsed, valid := o.readClock()
	if !valid {
		return nil
	}
	sequence := uint64(len(w.events) + 1)
	w.events = append(w.events, pinWriteObservationEvent{
		Sequence: sequence, Operation: sequence, MembershipSequence: o.sequence,
		Elapsed: elapsed, Kind: "begin", Identities: image,
	})
	w.identities += len(image)
	w.pending = sequence
	return &pinWriteObservationTicket{source: w, operation: sequence}
}

func (ticket *pinWriteObservationTicket) finish(s *secureSessionStore, commit secretfile.DurableCommit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, o := ticket.source, s.pinMembership
	if w.fault != "" || o.fault != "" {
		return
	}
	if w.pending != ticket.operation {
		w.fault = "pin-write-owner-conflict"
		return
	}
	elapsed, valid := o.readClock()
	if !valid {
		return
	}
	kind := "indeterminate"
	switch commit {
	case secretfile.DurableCommitted:
		kind = "confirmed"
	case secretfile.DurableNotCommitted:
		kind = "not_applied"
	}
	w.events = append(w.events, pinWriteObservationEvent{
		Sequence: uint64(len(w.events) + 1), Operation: ticket.operation,
		MembershipSequence: o.sequence, Elapsed: elapsed, Kind: kind,
	})
	w.pending = 0
}

// A panic leaves the original operation's outcome unchanged and the prefix
// visibly incomplete. No recovery or additional filesystem call is attempted.
func (ticket *pinWriteObservationTicket) abort(s *secureSessionStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := ticket.source
	if w.pending == ticket.operation && w.fault == "" {
		w.fault = "pin-write-incomplete"
	}
}

func (w *pinWriteObservation) capture(membershipFault string) (*pinWriteObservationSnapshot, []pinWriteObservationEvent) {
	status, reason := "PAST", w.fault
	if reason == "" {
		reason = membershipFault
	}
	if reason != "" {
		status = "UNKNOWN"
	}
	snapshot := &pinWriteObservationSnapshot{Status: status, Reason: reason, BaselineConfirmed: w.baselineConfirmed, Pending: w.pending}
	if status == "PAST" {
		guard := w.guard
		snapshot.GuardCoverage = &guard
	}
	return snapshot, w.events[:len(w.events):len(w.events)]
}

func copyPinWriteEvents(prefix []pinWriteObservationEvent) []pinWriteObservationEvent {
	out := slices.Clone(prefix)
	for i := range out {
		out[i].Identities = slices.Clone(out[i].Identities)
	}
	return out
}

func (s *secureSessionStore) writeIdentityPinMain(ctx context.Context, files identityPinDurableFiles, file secureSessionFile, raw []byte) (secretfile.DurableCommit, error) {
	ticket := s.beginPinWriteObservation(file.Identities)
	if ticket == nil {
		return files.WriteMain(ctx, raw)
	}
	defer ticket.abort(s)
	commit, err := files.WriteMain(ctx, raw)
	ticket.finish(s, commit)
	return commit, err
}

func pinSnapshotObservationOutcome(err error) secretfile.DurableCommit {
	if err == nil {
		return secretfile.DurableCommitted
	}
	var failure *pinSnapshotWriteError
	if errors.As(err, &failure) {
		switch failure.outcome {
		case domain.EndpointBindingActionNotApplied:
			return secretfile.DurableNotCommitted
		case domain.EndpointBindingActionAppliedCleanupError:
			return secretfile.DurableCommitted
		}
	}
	return secretfile.DurableIndeterminate
}
