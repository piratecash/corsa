package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// Functional observation ceilings only. Neither is a deployment selection,
// pin admission policy, duration/retention promise or physical memory bound.
const pinMembershipMaxHistory = 4096

// Constructor-only, copied before existing startup callbacks. Instance is an
// unverified caller label. The nil private clock selects production SinceBoot.
// This engine has no public installer, RNG, file, exporter or worker;
// the explicit typed bridge is separate.
type pinMembershipRequest struct {
	Instance                          [16]byte
	BaselineCapacity, HistoryCapacity int
	elapsed                           func() (time.Duration, error)
}

type pinMembershipClock struct{ Before, After time.Duration }

// An actual map membership edge, not metadata evidence renewal or a durable
// pin receipt. Sequence gives owner order; Clock bounds, not dates, the edge.
type pinMembershipTransition struct {
	Sequence        uint64
	Identity        domain.PeerIdentity
	Added           bool
	Clock           pinMembershipClock
	CountAtSequence int
}

// These are CURRENT, point-only observations. They are not continuous flags,
// pin membership, a persistence acknowledgement or an acceptance predicate.
type pinMembershipPointOnly struct {
	MemoryOnly, Readable, LocationUnsafe bool
	InitialStoreRefusal                  domain.PinStoreRefusalReason
	RecoveryPending                      bool
	CapacityReservationCount             int
}

type pinMembershipSnapshot struct {
	Population, Status, Reason string
	Qualification              string
	AttributionBound, PastOnly bool
	Instance                   [16]byte
	Baseline, Cut              pinMembershipClock
	BaselineIdentities         []domain.PeerIdentity
	Sequence                   uint64
	CountAtCut                 int
	Transitions                []pinMembershipTransition
	PointOnly                  pinMembershipPointOnly
}

// Owned by the original store.mu. There is no shadow membership map. All
// baseline/row cells are immutable after publication; Snapshot copies only
// captured value prefixes outside mu. A gap freezes counts/sequence/cut and
// prefix together. This source does not qualify current/durable K3 pins.
type pinMembershipSource struct {
	instance      [16]byte
	clock         func() (time.Duration, error)
	lastClock     time.Duration
	baseline, cut pinMembershipClock
	identities    []domain.PeerIdentity
	history       []pinMembershipTransition
	length        int
	sequence      uint64
	countAtCut    int
	fault         string
}

// Only a bounded mutation-local value: no pin entry, transport or store graph.
// Not retained after its leaf call. Completion sentinels distinguish a normal
// leaf and a returned W.Complete from panic unwinding without recovering it.
type pinMembershipTicket struct {
	source                            *pinMembershipSource
	identity                          domain.PeerIdentity
	added                             bool
	before                            time.Duration
	beforeCount                       int
	leafReturned, publicationReturned bool
}

func (o *pinMembershipSource) fail(reason string) {
	if o.fault == "" {
		o.fault = reason
		o.clock = nil
	}
}

// Caller owns store.mu, NEVER W. Panic is not swallowed or translated to an
// admission decision; its missing bracket makes observation UNKNOWN first.
func (o *pinMembershipSource) readClock() (time.Duration, bool) {
	if o.fault != "" {
		return 0, false
	}
	returned := false
	defer func() {
		if !returned {
			o.fail("duration-clock-incomplete")
		}
	}()
	value, err := o.clock()
	returned = true
	if err != nil || value < 0 {
		o.fail("duration-clock-unavailable")
		return 0, false
	}
	if value < o.lastClock {
		o.fail("duration-clock-regression")
		return 0, false
	}
	o.lastClock = value
	return value, true
}

// Called only after all ORIGINAL marker/load/refusal/guard initialization,
// before openSecureSessionStore exposes this store. Loaded identities are a
// baseline, not new session proofs or coverage dating back to pinnedAt.
func (s *secureSessionStore) initializePinMembership(request pinMembershipRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := &pinMembershipSource{instance: request.Instance, clock: request.elapsed}
	s.pinMembership = o
	if request.Instance == ([16]byte{}) || request.BaselineCapacity < 1 || request.BaselineCapacity > maxPinnedIdentities || request.HistoryCapacity < 1 || request.HistoryCapacity > pinMembershipMaxHistory {
		o.fail("invalid-explicit-request")
		return
	}
	if s.unreadable || s.refusal != nil {
		o.fail("unreadable-initial-baseline")
		return
	}
	if len(s.identities) > request.BaselineCapacity {
		o.fail("initial-baseline-capacity")
		return
	}
	if o.clock == nil {
		o.clock = trustedclock.SinceBoot
	}
	returned := false
	defer func() {
		if !returned {
			o.fail("initial-baseline-incomplete")
		}
	}()
	s.initializePinMembershipBaseline(o, request)
	returned = true
}

func (s *secureSessionStore) initializePinMembershipBaseline(o *pinMembershipSource, request pinMembershipRequest) {
	before, valid := o.readClock()
	if !valid {
		return
	}
	ids := make([]domain.PeerIdentity, len(s.identities))
	index := 0
	for id := range s.identities {
		if id.IsZero() {
			o.fail("invalid-initial-baseline")
			return
		}
		ids[index] = id
		index++
	}
	after, valid := o.readClock()
	if !valid {
		return
	}
	// Publish only the complete baseline; no partial IDs escape on failure.
	o.identities = ids
	o.baseline = pinMembershipClock{Before: before, After: after}
	o.cut = o.baseline
	o.countAtCut = len(ids)
	o.history = make([]pinMembershipTransition, request.HistoryCapacity)
}

// Existing leaf owns store.mu. A nil source returns before any observer clock,
// walk, allocation, RNG or callback. Metadata-only replacements are not edges.
func (s *secureSessionStore) beginPinMembershipLocked(id domain.PeerIdentity, added bool) *pinMembershipTicket {
	o := s.pinMembership
	if o == nil || o.fault != "" {
		return nil
	}
	_, present := s.identities[id]
	if present == added {
		return nil
	}
	if id.IsZero() || len(s.identities) != o.countAtCut {
		o.fail("membership-owner-conflict")
		return nil
	}
	if o.sequence == ^uint64(0) || o.length >= len(o.history) {
		o.fail("membership-history-capacity")
		return nil
	}
	before, valid := o.readClock()
	if !valid {
		return nil
	}
	return &pinMembershipTicket{source: o, identity: id, added: added, before: before, beforeCount: len(s.identities)}
}

// Registered BEFORE the original publication defer: LIFO releases W first.
// Natural panics continue unchanged. A finish/After panic freezes the prefix.
func (ticket *pinMembershipTicket) finish(s *secureSessionStore) {
	o := ticket.source
	if o.fault != "" {
		return
	}
	if !ticket.leafReturned || !ticket.publicationReturned {
		o.fail("membership-mutation-incomplete")
		return
	}
	returned := false
	defer func() {
		if !returned {
			o.fail("membership-finish-incomplete")
		}
	}()
	ticket.finishChecked(s)
	returned = true
}

func (ticket *pinMembershipTicket) finishChecked(s *secureSessionStore) {
	o := ticket.source
	after, valid := o.readClock()
	if !valid {
		return
	}
	_, present := s.identities[ticket.identity]
	wantCount := ticket.beforeCount - 1
	if ticket.added {
		wantCount = ticket.beforeCount + 1
	}
	if s.pinMembership != o || present != ticket.added || len(s.identities) != wantCount || o.countAtCut != ticket.beforeCount || o.sequence == ^uint64(0) || o.length >= len(o.history) {
		o.fail("membership-owner-conflict")
		return
	}
	bracket := pinMembershipClock{Before: ticket.before, After: after}
	o.history[o.length] = pinMembershipTransition{Sequence: o.sequence + 1, Identity: ticket.identity, Added: ticket.added, Clock: bracket, CountAtSequence: wantCount}
	o.length++
	o.sequence++
	o.countAtCut = wantCount
	o.cut = bracket
}

// Nondestructive, bounded by the frozen constructor capacities. CountAtCut is
// the SAME confirmed prefix count even after UNKNOWN, not post-gap map size.
// PointOnly is sampled now and may differ from that prefix. No mutable map is
// exposed/copied after unlock, and no source clock runs under a W lease.
func (s *secureSessionStore) pinMembershipSnapshot() pinMembershipSnapshot {
	out := pinMembershipSnapshot{Population: "RawPinMembership", Status: "UNKNOWN", Qualification: "UNKNOWN", PastOnly: true}
	if s == nil {
		out.Reason = "missing-store"
		return out
	}
	var ids []domain.PeerIdentity
	var prefix []pinMembershipTransition
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		out.PointOnly = pinMembershipPointOnly{MemoryOnly: s.memoryOnlyLocked(), Readable: !s.unreadable, LocationUnsafe: s.locationUnsafe, RecoveryPending: s.pinRecovery != nil, CapacityReservationCount: s.identityPinCapacityUsedLocked() - len(s.identities)}
		if s.refusal != nil {
			out.PointOnly.InitialStoreRefusal = s.refusal.Reason
		}
		o := s.pinMembership
		if o == nil {
			out.Status = "OFF"
			return
		}
		if o.fault == "" {
			if value, valid := o.readClock(); valid {
				o.cut = pinMembershipClock{Before: value, After: value}
			}
		}
		out.Instance, out.Baseline, out.Cut = o.instance, o.baseline, o.cut
		out.Sequence, out.CountAtCut = o.sequence, o.countAtCut
		out.Status = "PAST"
		if o.fault != "" {
			out.Status, out.Reason = "UNKNOWN", o.fault
		} else if o.cut.Before <= o.baseline.After {
			out.Status, out.Reason = "NO_PROGRESS", "no-positive-covered-interval"
		}
		ids = o.identities[:len(o.identities):len(o.identities)]
		prefix = o.history[:o.length:o.length]
	}()
	if out.Status == "OFF" {
		return out
	}
	out.BaselineIdentities = make([]domain.PeerIdentity, len(ids))
	copy(out.BaselineIdentities, ids)
	out.Transitions = make([]pinMembershipTransition, len(prefix))
	copy(out.Transitions, prefix)
	return out
}
