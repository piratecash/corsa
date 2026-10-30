package node

import (
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// These are implementation refusal ceilings for an inactive functional source,
// not selected deployment capacities, admission limits or a resource-fit claim.
const k3RegisteredMaxOwners = 1024
const k3RegisteredMaxHistory = 4096
const k3RegisteredMaxAddressBytes = 1024

// Explicit pre-publication request only. Instance is an unverified caller label.
// elapsed is a private test clock; the production nil choice is SinceBoot.
// There is no installer, App bridge, worker, RNG, file writer or public switch.
type k3RegisteredRequest struct {
	OwnerCapacity, HistoryCapacity int
	Instance                       [16]byte
	elapsed                        func() (time.Duration, error)
}

type k3RegisteredClock struct{ Before, After time.Duration }

type k3RegisteredOwnerRef struct {
	Direction  string
	ConnID     domain.ConnID
	Address    domain.PeerAddress
	Generation uint64
}

// One atomic owner delta. Equal nonzero identities mean a replacement, NOT a
// temporary union gap. Separate removal/re-addition produces separate rows.
// Brackets bound the mutation; they are neither exact instants nor UTC.
type k3RegisteredTransition struct {
	Sequence                             uint64
	Clock                                k3RegisteredClock
	Removed, Added                       k3RegisteredOwnerRef
	RemovedIdentity, AddedIdentity       domain.PeerIdentity
	RegisteredOwners, DistinctIdentities int
}

type k3RegisteredSnapshot struct {
	Population, Status, Reason           string
	Qualification                        string
	Instance                             [16]byte
	AttributionBound                     bool
	PastOnly                             bool
	Baseline, Cut                        k3RegisteredClock
	Sequence                             uint64
	RegisteredOwners, DistinctIdentities int
	Transitions                          []k3RegisteredTransition
}

type k3RegisteredKey struct {
	direction netcore.Direction
	id        domain.ConnID
	address   domain.PeerAddress
}

type k3RegisteredBinding struct {
	entry    *connEntry
	core     *netcore.NetCore
	session  *peerSession
	auth     *connauth.State
	id       domain.ConnID
	identity domain.PeerIdentity
}

func (b k3RegisteredBinding) present() bool { return !b.identity.IsZero() }

type k3RegisteredOwner struct {
	binding    k3RegisteredBinding
	generation uint64
}

// All mutable fields are owned by the SAME Service.peerMu as the actual maps
// and enabled Auth writers. Completed rows are preallocated, append-only and
// immutable forever: Snapshot copies only its captured prefix after unlock.
// A fault freezes counts, sequence, cut and prefix together. No suffix, reset,
// retirement, current-identity map escape or long-term coverage is implied.
type k3RegisteredSource struct {
	instance                       [16]byte
	clock                          func() (time.Duration, error)
	lastClock                      time.Duration
	baseline, cut                  k3RegisteredClock
	fault                          string
	frozenOwners, frozenIdentities int
	ownerCapacity                  int
	owners                         map[k3RegisteredKey]k3RegisteredOwner
	refs                           map[domain.PeerIdentity]int
	history                        []k3RegisteredTransition
	length                         int
	sequence, generation           uint64
}

type k3RegisteredTicket struct {
	active   bool
	before   time.Duration
	key      k3RegisteredKey
	previous k3RegisteredBinding
}

func newK3RegisteredSource(request k3RegisteredRequest) *k3RegisteredSource {
	o := &k3RegisteredSource{instance: request.Instance, clock: request.elapsed}
	if request.OwnerCapacity < 1 || request.OwnerCapacity > k3RegisteredMaxOwners || request.HistoryCapacity < 1 || request.HistoryCapacity > k3RegisteredMaxHistory || request.Instance == ([16]byte{}) {
		o.fail("invalid-explicit-request")
		return o
	}
	if o.clock == nil {
		o.clock = trustedclock.SinceBoot
	}
	before, ok := o.readClock()
	if !ok {
		return o
	}
	after, ok := o.readClock()
	if !ok {
		return o
	}
	o.baseline = k3RegisteredClock{Before: before, After: after}
	o.cut = o.baseline
	o.ownerCapacity = request.OwnerCapacity
	o.owners = make(map[k3RegisteredKey]k3RegisteredOwner, request.OwnerCapacity)
	o.refs = make(map[domain.PeerIdentity]int, request.OwnerCapacity)
	o.history = make([]k3RegisteredTransition, request.HistoryCapacity)
	return o
}

func (o *k3RegisteredSource) fail(reason string) {
	if o.fault == "" {
		o.frozenOwners, o.frozenIdentities = len(o.owners), len(o.refs)
		// History has values only. Release the obsolete transport graphs on
		// the first gap; the coherent frozen prefix needs only these counts.
		o.owners, o.refs, o.clock = nil, nil, nil
		o.fault = reason
	}
}

// Caller holds peerMu, but NEVER a registry W lease or an observer leaf lock.
// The constructor calls this before the empty Service can be published.
func (o *k3RegisteredSource) readClock() (time.Duration, bool) {
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

// Auth.ProvenIdentity intentionally does not distinguish unproven/v1 from a
// hidden proof/Hello mismatch. Its false result is outside this population;
// there is no reflection or wider notion of cryptographic proof here.
func k3RegisteredInbound(id domain.ConnID, entry *connEntry) (k3RegisteredBinding, bool) {
	if entry == nil {
		return k3RegisteredBinding{}, true
	}
	core := entry.core
	if core == nil || id == 0 || core.ConnID() != id {
		return k3RegisteredBinding{}, false
	}
	if core.Dir() == netcore.Outbound {
		return k3RegisteredBinding{}, true
	}
	if core.Dir() != netcore.Inbound {
		return k3RegisteredBinding{}, false
	}
	auth := core.Auth()
	proof, proven := auth.ProvenIdentity()
	if !proven {
		return k3RegisteredBinding{}, true
	}
	identity, valid := proof.Identity()
	if !valid || identity.IsZero() {
		return k3RegisteredBinding{}, false
	}
	return k3RegisteredBinding{entry: entry, core: core, auth: auth, id: id, identity: identity}, true
}

func k3RegisteredOutbound(session *peerSession) (k3RegisteredBinding, bool) {
	if session == nil {
		return k3RegisteredBinding{}, true
	}
	proof, proven := session.provenIdentity()
	if !proven {
		return k3RegisteredBinding{}, true
	}
	identity, valid := proof.Identity()
	if !valid || identity.IsZero() || identity != session.peerIdentity || session.connID == 0 || session.netCore == nil || session.netCore.ConnID() != session.connID || session.netCore.Dir() != netcore.Outbound {
		return k3RegisteredBinding{}, false
	}
	return k3RegisteredBinding{core: session.netCore, session: session, id: session.connID, identity: identity}, true
}

// No physical socket/closed/tracked/advertised metadata predicate is added.
// Outbound membership is the actual sessions[address] binding, not a second
// contribution for its NetCore registry alias or an F1/pin eligibility claim.
func (s *Service) k3BeginRegistryLocked(id domain.ConnID) k3RegisteredTicket {
	o := s.k3Registered
	if o == nil || o.fault != "" {
		return k3RegisteredTicket{}
	}
	before, ok := o.readClock()
	if !ok {
		return k3RegisteredTicket{}
	}
	previous, valid := k3RegisteredInbound(id, s.connEntryByIDLocked(id))
	if !valid {
		o.fail("invalid-registered-owner-binding")
		return k3RegisteredTicket{}
	}
	return k3RegisteredTicket{active: true, before: before, key: k3RegisteredKey{direction: netcore.Inbound, id: id}, previous: previous}
}

func (s *Service) k3FinishRegistryLocked(ticket k3RegisteredTicket) {
	if !ticket.active {
		return
	}
	o := s.k3Registered
	if o == nil || o.fault != "" {
		return
	}
	current, valid := k3RegisteredInbound(ticket.key.id, s.connEntryByIDLocked(ticket.key.id))
	if !valid {
		o.fail("invalid-registered-owner-binding")
		return
	}
	o.complete(ticket, current)
}

func (s *Service) k3BeginSessionLocked(address domain.PeerAddress) k3RegisteredTicket {
	o := s.k3Registered
	if o == nil || o.fault != "" {
		return k3RegisteredTicket{}
	}
	// Observation retention only: do not change the real map/network policy.
	// Check bytes before the clock, ticket, owner map or history can retain it.
	if len(address) > k3RegisteredMaxAddressBytes {
		o.fail("address-reference-too-large")
		return k3RegisteredTicket{}
	}
	before, ok := o.readClock()
	if !ok {
		return k3RegisteredTicket{}
	}
	previous, valid := k3RegisteredOutbound(s.sessions[address])
	if !valid {
		o.fail("invalid-outbound-proof-binding")
		return k3RegisteredTicket{}
	}
	return k3RegisteredTicket{active: true, before: before, key: k3RegisteredKey{direction: netcore.Outbound, address: address}, previous: previous}
}

func (s *Service) k3FinishSessionLocked(ticket k3RegisteredTicket) {
	if !ticket.active {
		return
	}
	o := s.k3Registered
	if o == nil || o.fault != "" {
		return
	}
	current, valid := k3RegisteredOutbound(s.sessions[ticket.key.address])
	if !valid {
		o.fail("invalid-outbound-proof-binding")
		return
	}
	o.complete(ticket, current)
}

func k3RegisteredReference(key k3RegisteredKey, binding k3RegisteredBinding, generation uint64) k3RegisteredOwnerRef {
	if !binding.present() {
		return k3RegisteredOwnerRef{}
	}
	direction := "inbound"
	if key.direction == netcore.Outbound {
		direction = "outbound"
	}
	return k3RegisteredOwnerRef{Direction: direction, ConnID: binding.id, Address: domain.PeerAddress(strings.Clone(string(key.address))), Generation: generation}
}

// All refusal/capacity/arithmetic checks precede the single atomic row and
// current-state commit. A replacement can never expose only its remove half.
func (o *k3RegisteredSource) complete(ticket k3RegisteredTicket, current k3RegisteredBinding) {
	if o.fault != "" {
		return
	}
	after, ok := o.readClock()
	if !ok {
		return
	}
	old, exists := o.owners[ticket.key]
	if exists != ticket.previous.present() || exists && old.binding != ticket.previous {
		o.fail("owner-projection-conflict")
		return
	}
	bracket := k3RegisteredClock{Before: ticket.before, After: after}
	if ticket.previous == current {
		o.cut = bracket
		return
	}
	count := len(o.owners)
	if ticket.previous.present() {
		count--
	}
	if current.present() {
		count++
	}
	if count < 0 || count > o.ownerCapacity {
		o.fail("owner-capacity-exhausted")
		return
	}
	if o.length >= len(o.history) || o.sequence == ^uint64(0) || current.present() && o.generation == ^uint64(0) {
		o.fail("history-or-generation-exhausted")
		return
	}
	oldRefs := o.refs[ticket.previous.identity]
	newRefs := o.refs[current.identity]
	if ticket.previous.present() && oldRefs < 1 || current.present() && ticket.previous.identity != current.identity && newRefs >= o.ownerCapacity {
		o.fail("identity-refcount-conflict")
		return
	}
	if ticket.previous.identity != current.identity {
		if ticket.previous.present() {
			if oldRefs == 1 {
				delete(o.refs, ticket.previous.identity)
			} else {
				o.refs[ticket.previous.identity] = oldRefs - 1
			}
		}
		if current.present() {
			o.refs[current.identity] = newRefs + 1
		}
	}
	generation := uint64(0)
	if current.present() {
		o.generation++
		generation = o.generation
	}
	row := k3RegisteredTransition{Sequence: o.sequence + 1, Clock: bracket,
		Removed: k3RegisteredReference(ticket.key, ticket.previous, old.generation), Added: k3RegisteredReference(ticket.key, current, generation),
		RemovedIdentity: ticket.previous.identity, AddedIdentity: current.identity,
		RegisteredOwners: count, DistinctIdentities: len(o.refs)}
	if current.present() {
		o.owners[ticket.key] = k3RegisteredOwner{binding: current, generation: generation}
	} else {
		delete(o.owners, ticket.key)
	}
	o.history[o.length] = row
	o.length++
	o.sequence++
	o.cut = bracket
}

// Snapshot counts describe Cut/Sequence, including the frozen good prefix
// after UNKNOWN; they never claim current post-gap membership. Instance and
// Qualification remain unverified even for a locally coherent PAST result.
// Nondestructive: an idle cut may progress, but no event is consumed. No
// mutable map escapes or is copied after unlock. Rows below captured length
// can never be overwritten, even when another owner appends or faults later.
func (s *Service) k3RegisteredSnapshot() k3RegisteredSnapshot {
	out := k3RegisteredSnapshot{Population: "RegisteredProvenV2", Status: "OFF", Qualification: "UNKNOWN", PastOnly: true}
	if s.k3Registered == nil {
		return out
	}
	var prefix []k3RegisteredTransition
	func() {
		s.peerMu.Lock()
		defer s.peerMu.Unlock()
		o := s.k3Registered
		if o == nil {
			return
		}
		if o.fault == "" {
			before, ok := o.readClock()
			if ok {
				after, valid := o.readClock()
				if valid {
					o.cut = k3RegisteredClock{Before: before, After: after}
				}
			}
		}
		out.Instance = o.instance
		out.Baseline = o.baseline
		out.Cut = o.cut
		out.Sequence = o.sequence
		out.RegisteredOwners = len(o.owners)
		out.DistinctIdentities = len(o.refs)
		if o.fault != "" {
			out.RegisteredOwners = o.frozenOwners
			out.DistinctIdentities = o.frozenIdentities
		}
		out.Status = "PAST"
		if o.fault != "" {
			out.Status = "UNKNOWN"
			out.Reason = o.fault
		} else if o.cut.Before <= o.baseline.After {
			out.Status = "NO_PROGRESS"
			out.Reason = "no-positive-covered-interval"
		}
		prefix = o.history[:o.length:o.length]
	}()
	out.Transitions = make([]k3RegisteredTransition, len(prefix))
	copy(out.Transitions, prefix)
	return out
}
