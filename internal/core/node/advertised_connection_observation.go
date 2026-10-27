package node

import (
	"strings"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

const k3AdvertisedMaxConnections = 1024
const k3AdvertisedMaxSlots = 1024
const k3AdvertisedMaxHistory = 4096
const k3AdvertisedMaxAddressBytes = 1024

// Explicit private functional request. Instance remains an unverified caller
// label; the runtime source attachment, not this label, binds the two owners.
type k3AdvertisedRequest struct {
	ConnectionCapacity, SlotCapacity, HistoryCapacity int
	Instance                                          [16]byte
	elapsed                                           func() (time.Duration, error)
}

type k3AdvertisedClock struct{ Before, After time.Duration }

type k3AdvertisedPeerFact struct {
	Direction                              string
	Address                                domain.PeerAddress
	ConnID                                 domain.ConnID
	Generation                             uint64
	ClaimSeen                              bool
	Identity                               domain.PeerIdentity
	Version                                int
	HealthAddress                          domain.PeerAddress
	RawAddress                             domain.PeerAddress
	MetadataIdentity                       domain.PeerIdentity
	ClaimConflict                          bool
	HealthPresent, Connected, ContextKnown bool
}

type k3AdvertisedSlotFact struct {
	Address    domain.PeerAddress
	State      domain.SlotState
	Generation uint64
	ConnID     domain.ConnID
}

// One value change in an indivisible original owner transaction. Sequence is
// publication order ONLY; clocks can overlap across peer and CM owners.
// OwnerSequence counts changed atomic batches; no-op attempts consume no row.
type k3AdvertisedFact struct {
	Sequence, Batch, OwnerSequence uint64
	Index, Count                   int
	Owner                          string
	Clock                          k3AdvertisedClock
	Removed                        bool
	Peer                           k3AdvertisedPeerFact
	Slot                           k3AdvertisedSlotFact
}

type k3AdvertisedSnapshot struct {
	Population, Status, Reason, Qualification string
	Instance                                  [16]byte
	AttributionBound, PastOnly                bool
	Baseline, Cut                             k3AdvertisedClock
	CompletedSequence, CoveredSequence        uint64
	CompletedBatches, CoveredBatches          uint64
	PeerOwnersAtCut, SlotsAtCut, Pending      int
	Facts                                     []k3AdvertisedFact
}

type k3AdvertisedPeerKey struct {
	Direction string
	ConnID    domain.ConnID
}

type k3AdvertisedRawClaim struct {
	Seen        bool
	Conflict    bool
	Identity    domain.PeerIdentity
	Version     int
	Address     domain.PeerAddress
	DialAddress domain.PeerAddress
}

// No mutable transport pointers, callbacks to owners, I/O or clock under mu.
// The last covered prefix is separate from completed, unclosed transactions.
type k3AdvertisedSource struct {
	mu                                                 sync.Mutex
	instance                                           [16]byte
	clock                                              func() (time.Duration, error)
	connectionCapacity, slotCapacity                   int
	fault                                              string
	ready                                              bool
	baseline, cut                                      k3AdvertisedClock
	peerFloor, cmFloor                                 time.Duration
	mutationEpoch, cutEpoch, peerSequence, cmSequence  uint64
	pending                                            int
	peer                                               map[k3AdvertisedPeerKey]k3AdvertisedPeerFact
	slots                                              map[domain.PeerAddress]k3AdvertisedSlotFact
	history                                            []k3AdvertisedFact
	length, coveredLength                              int
	sequence, batches, coveredSequence, coveredBatches uint64
	peerOwnersAtCut, slotsAtCut                        int
}

// Mutation-local only; never retained by source/history. The owner lock stays
// with the caller throughout Before -> original mutation -> After.
type k3AdvertisedTicket struct {
	source             *k3AdvertisedSource
	owner              string
	ownerSequence      uint64
	before, lowerBound time.Duration
	done               bool
}

func newK3AdvertisedSource(request k3AdvertisedRequest) *k3AdvertisedSource {
	o := &k3AdvertisedSource{instance: request.Instance, clock: request.elapsed, connectionCapacity: request.ConnectionCapacity, slotCapacity: request.SlotCapacity}
	if request.Instance == ([16]byte{}) || request.ConnectionCapacity < 1 || request.ConnectionCapacity > k3AdvertisedMaxConnections || request.SlotCapacity < 1 || request.SlotCapacity > k3AdvertisedMaxSlots || request.HistoryCapacity < 1 || request.HistoryCapacity > k3AdvertisedMaxHistory {
		o.fault = "invalid-explicit-request"
		o.clock = nil
		return o
	}
	if o.clock == nil {
		o.clock = trustedclock.SinceBoot
	}
	returned := false
	defer func() {
		if !returned {
			o.fail("initial-clock-incomplete")
		}
	}()
	before, err := o.clock()
	if err != nil || before < 0 {
		o.fail("initial-clock-unavailable")
		returned = true
		return o
	}
	after, err := o.clock()
	if err != nil || after < before {
		o.fail("initial-clock-unavailable")
		returned = true
		return o
	}
	o.baseline = k3AdvertisedClock{Before: before, After: after}
	o.cut = o.baseline
	o.peerFloor, o.cmFloor = after, after
	o.peer = make(map[k3AdvertisedPeerKey]k3AdvertisedPeerFact)
	o.slots = make(map[domain.PeerAddress]k3AdvertisedSlotFact)
	o.history = make([]k3AdvertisedFact, request.HistoryCapacity)
	returned = true
	return o
}

func (o *k3AdvertisedSource) failLocked(reason string) {
	if o.fault == "" {
		o.fault = reason
		o.clock = nil
		o.peer = nil
		o.slots = nil
	}
}
func (o *k3AdvertisedSource) fail(reason string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.failLocked(reason)
	o.mu.Unlock()
}
func (o *k3AdvertisedSource) available() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fault == "" && o.ready
}

// Constructor-only: both real owners are still unpublished. Persisted health
// and identity caches are NOT assumed empty and never become live claims.
func (s *Service) bindK3AdvertisedConstruction() {
	o := s.k3Advertised
	if o == nil {
		return
	}
	valid := len(s.conns) == 0 && len(s.sessions) == 0 && len(s.dialOrigin) == 0 && s.connManager != nil && s.connManager.config.advertised == o && len(s.connManager.slots) == 0
	o.mu.Lock()
	defer o.mu.Unlock()
	if !valid {
		o.failLocked("nonempty-or-mismatched-construction-baseline")
		return
	}
	if o.fault == "" {
		o.ready = true
	}
}

func (o *k3AdvertisedSource) begin(owner string) *k3AdvertisedTicket {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	if o.fault != "" {
		o.mu.Unlock()
		return nil
	}
	if !o.ready {
		o.failLocked("publication-before-baseline")
		o.mu.Unlock()
		return nil
	}
	if o.mutationEpoch == ^uint64(0) || o.peerSequence == ^uint64(0) || o.cmSequence == ^uint64(0) {
		o.failLocked("sequence-exhausted")
		o.mu.Unlock()
		return nil
	}
	o.mutationEpoch++
	o.pending++
	seq := uint64(0)
	if owner == "peer" {
		seq = o.peerSequence + 1
	} else {
		seq = o.cmSequence + 1
	}
	lower := o.peerFloor
	if o.cmFloor > lower {
		lower = o.cmFloor
	}
	if o.cut.After > lower {
		lower = o.cut.After
	}
	clock := o.clock
	o.mu.Unlock()
	t := &k3AdvertisedTicket{source: o, owner: owner, ownerSequence: seq, lowerBound: lower}
	returned := false
	defer func() {
		if !returned {
			t.abort("before-clock-incomplete")
		}
	}()
	value, err := clock()
	o.mu.Lock()
	// Only completions/cut known BEFORE this ticket registered constrain it.
	// A later opposite-owner completion cannot invalidate a pending bracket.
	floor := t.lowerBound
	if err != nil || value < 0 {
		o.failLocked("duration-clock-unavailable")
	} else if value < floor {
		o.failLocked("owner-clock-regression")
	}
	t.before = value
	o.mu.Unlock()
	returned = true
	return t
}

func (t *k3AdvertisedTicket) abort(reason string) {
	if t == nil || t.done {
		return
	}
	o := t.source
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failLocked(reason)
	if o.pending > 0 {
		o.pending--
	}
	t.done = true
}
func (t *k3AdvertisedTicket) guard() {
	if t != nil && !t.done {
		t.abort("owner-transaction-incomplete")
	}
}

// The caller still owns peerMu; no Source-to-owner callback or reacquisition.
func (s *Service) guardK3AdvertisedPeerLocked(t *k3AdvertisedTicket) {
	if t == nil {
		return
	}
	t.guard()
	if s.k3AdvertisedActive == t {
		s.k3AdvertisedActive = nil
	}
}

func (s *Service) beginK3AdvertisedPeerLocked() *k3AdvertisedTicket {
	if s.k3Advertised == nil || s.k3AdvertisedActive != nil {
		return nil
	}
	t := s.k3Advertised.begin("peer")
	if t == nil {
		s.k3AdvertisedClaims = nil
		s.k3AdvertisedGenerations = nil
		return nil
	}
	s.k3AdvertisedActive = t
	return t
}
func (s *Service) finishK3AdvertisedPeerLocked(t *k3AdvertisedTicket) {
	if t == nil {
		return
	}
	defer func() {
		if s.k3AdvertisedActive == t {
			s.k3AdvertisedActive = nil
		}
	}()
	defer s.guardK3AdvertisedPeerLocked(t)
	if !t.source.available() {
		t.abort("source-unavailable")
		s.k3AdvertisedClaims = nil
		s.k3AdvertisedGenerations = nil
		return
	}
	peers, ok := s.collectK3AdvertisedPeersLocked(t.source)
	if !ok {
		t.abort("peer-projection-unavailable")
		s.k3AdvertisedActive = nil
		return
	}
	t.complete(peers, nil)
	s.k3AdvertisedActive = nil
}
func (cm *ConnectionManager) beginK3AdvertisedLocked() *k3AdvertisedTicket {
	return cm.config.advertised.begin("cm")
}
func (cm *ConnectionManager) finishK3AdvertisedLocked(t *k3AdvertisedTicket) {
	if t == nil {
		return
	}
	defer t.guard()
	o := t.source
	if !o.available() {
		t.abort("source-unavailable")
		return
	}
	if len(cm.slots) > o.slotCapacity {
		t.abort("slot-capacity")
		return
	}
	values := make(map[domain.PeerAddress]k3AdvertisedSlotFact, len(cm.slots))
	for _, slot := range cm.slots {
		if slot == nil || len(slot.Address) > k3AdvertisedMaxAddressBytes || slot.Generation == 0 {
			t.abort("invalid-slot-binding")
			return
		}
		if _, duplicate := values[slot.Address]; duplicate {
			t.abort("duplicate-slot-address")
			return
		}
		id := domain.ConnID(0)
		if slot.Session != nil {
			id = slot.Session.connID
		}
		addr := domain.PeerAddress(strings.Clone(string(slot.Address)))
		values[addr] = k3AdvertisedSlotFact{Address: addr, State: slot.State, Generation: slot.Generation, ConnID: id}
	}
	t.complete(nil, values)
}

func (t *k3AdvertisedTicket) complete(peers map[k3AdvertisedPeerKey]k3AdvertisedPeerFact, slots map[domain.PeerAddress]k3AdvertisedSlotFact) {
	if t == nil || t.done {
		return
	}
	defer t.guard()
	o := t.source
	o.mu.Lock()
	clock := o.clock
	fault := o.fault != ""
	o.mu.Unlock()
	if fault {
		t.abort("source-unavailable")
		return
	}
	after, err := clock() // owner held; NEVER observer or W
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fault != "" {
		o.pending--
		t.done = true
		return
	}
	floor := o.peerFloor
	if t.owner == "cm" {
		floor = o.cmFloor
	}
	if err != nil || after < 0 {
		o.failLocked("duration-clock-unavailable")
	} else if after < t.before || t.before < t.lowerBound || t.before < floor {
		o.failLocked("owner-clock-regression")
	}
	if o.fault != "" {
		o.pending--
		t.done = true
		return
	}
	n := 0
	if t.owner == "peer" {
		for k, old := range o.peer {
			if next, ok := peers[k]; !ok || next != old {
				n++
			}
		}
		for k := range peers {
			if _, ok := o.peer[k]; !ok {
				n++
			}
		}
	} else {
		for k, old := range o.slots {
			if next, ok := slots[k]; !ok || next != old {
				n++
			}
		}
		for k := range slots {
			if _, ok := o.slots[k]; !ok {
				n++
			}
		}
	}
	if n > len(o.history)-o.length || uint64(n) > ^uint64(0)-o.sequence || (n > 0 && o.batches == ^uint64(0)) {
		o.failLocked("history-capacity-or-sequence")
		o.pending--
		t.done = true
		return
	}
	if n > 0 {
		o.batches++
		if t.owner == "peer" {
			o.peerSequence = t.ownerSequence
		} else {
			o.cmSequence = t.ownerSequence
		}
	}
	index := 0
	put := func(row k3AdvertisedFact) {
		index++
		o.sequence++
		row.Sequence = o.sequence
		row.Batch = o.batches
		row.OwnerSequence = t.ownerSequence
		row.Index = index
		row.Count = n
		row.Owner = t.owner
		row.Clock = k3AdvertisedClock{Before: t.before, After: after}
		o.history[o.length] = row
		o.length++
	}
	if t.owner == "peer" {
		for k, old := range o.peer {
			if next, ok := peers[k]; !ok {
				put(k3AdvertisedFact{Removed: true, Peer: old})
			} else if next != old {
				put(k3AdvertisedFact{Peer: next})
			}
		}
		for k, next := range peers {
			if _, ok := o.peer[k]; !ok {
				put(k3AdvertisedFact{Peer: next})
			}
		}
		o.peer = peers
		o.peerFloor = after
	} else {
		for k, old := range o.slots {
			if next, ok := slots[k]; !ok {
				put(k3AdvertisedFact{Removed: true, Slot: old})
			} else if next != old {
				put(k3AdvertisedFact{Slot: next})
			}
		}
		for k, next := range slots {
			if _, ok := o.slots[k]; !ok {
				put(k3AdvertisedFact{Slot: next})
			}
		}
		o.slots = slots
		o.cmFloor = after
	}
	o.pending--
	t.done = true
}

func (s *Service) k3AdvertisedSnapshot() k3AdvertisedSnapshot {
	out := k3AdvertisedSnapshot{Population: "AdvertisedConnections", Status: "OFF", Qualification: "UNKNOWN", PastOnly: true}
	if s == nil {
		out.Status = "UNKNOWN"
		out.Reason = "missing-service"
		return out
	}
	o := s.k3Advertised
	if o == nil {
		return out
	}
	o.mu.Lock()
	unstable := false
	epoch, cutEpoch := o.mutationEpoch, o.cutEpoch
	sample := o.fault == "" && o.ready && o.pending == 0
	clock := o.clock
	o.mu.Unlock()
	if sample {
		returned := false
		func() {
			defer func() {
				if !returned {
					o.fail("snapshot-clock-incomplete")
				}
			}()
			value, err := clock()
			o.mu.Lock()
			defer o.mu.Unlock()
			returned = true
			if o.fault != "" {
				return
			}
			if err != nil || value < 0 {
				o.failLocked("duration-clock-unavailable")
				return
			}
			if epoch != o.mutationEpoch || cutEpoch != o.cutEpoch || o.pending != 0 {
				unstable = true
				return
			}
			if value < o.peerFloor || value < o.cmFloor || value < o.cut.After {
				o.failLocked("cut-clock-regression")
				return
			}
			if o.cutEpoch == ^uint64(0) {
				o.failLocked("cut-sequence-exhausted")
				return
			}
			o.cutEpoch++
			o.cut = k3AdvertisedClock{Before: value, After: value}
			o.coveredLength = o.length
			o.coveredSequence = o.sequence
			o.coveredBatches = o.batches
			o.peerOwnersAtCut = len(o.peer)
			o.slotsAtCut = len(o.slots)
		}()
	}
	o.mu.Lock()
	out.Instance = o.instance
	out.Baseline = o.baseline
	out.Cut = o.cut
	out.CompletedSequence, out.CoveredSequence = o.sequence, o.coveredSequence
	out.CompletedBatches, out.CoveredBatches = o.batches, o.coveredBatches
	out.PeerOwnersAtCut, out.SlotsAtCut, out.Pending = o.peerOwnersAtCut, o.slotsAtCut, o.pending
	out.Status = "PAST"
	if o.fault != "" {
		out.Status = "UNKNOWN"
		out.Reason = o.fault
	} else if !o.ready {
		out.Status = "UNKNOWN"
		out.Reason = "baseline-unavailable"
	} else if unstable || o.pending != 0 || o.coveredLength != o.length {
		out.Status = "IN_FLIGHT"
		out.Reason = "cut-not-extended"
	} else if o.cut.Before <= o.baseline.After {
		out.Status = "NO_PROGRESS"
		out.Reason = "no-positive-covered-interval"
	}
	prefix := o.history[:o.coveredLength:o.coveredLength]
	o.mu.Unlock()
	out.Facts = make([]k3AdvertisedFact, len(prefix))
	copy(out.Facts, prefix)
	return out
}

func (s *Service) k3AdvertisedAssignGenerationLocked(key k3AdvertisedPeerKey) {
	o := s.k3Advertised
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fault != "" || !o.ready {
		return
	}
	if s.k3AdvertisedNextGeneration == ^uint64(0) {
		o.failLocked("binding-reference-capacity")
		return
	}
	if s.k3AdvertisedGenerations == nil {
		s.k3AdvertisedGenerations = make(map[k3AdvertisedPeerKey]uint64)
	}
	if _, exists := s.k3AdvertisedGenerations[key]; !exists && len(s.k3AdvertisedGenerations) >= o.connectionCapacity {
		o.failLocked("connection-capacity")
		return
	}
	s.k3AdvertisedNextGeneration++
	s.k3AdvertisedGenerations[key] = s.k3AdvertisedNextGeneration
}

func k3AdvertisedDirection(core *netcore.NetCore) string {
	if core.Dir() == netcore.Inbound {
		return "inbound"
	}
	if core.Dir() == netcore.Outbound {
		return "outbound"
	}
	return ""
}
func (s *Service) k3AdvertisedRegistryAddedLocked(id domain.ConnID, direction string) {
	if s.k3Advertised == nil {
		return
	}
	delete(s.k3AdvertisedClaims, id)
	s.k3AdvertisedAssignGenerationLocked(k3AdvertisedPeerKey{Direction: direction, ConnID: id})
}
func (s *Service) k3AdvertisedOtherSessionLocked(id domain.ConnID, core *netcore.NetCore, except domain.PeerAddress, exclude bool) bool {
	if !s.k3Advertised.available() {
		return false
	}
	if len(s.sessions) > s.k3Advertised.connectionCapacity {
		s.k3Advertised.fail("connection-capacity")
		return false
	}
	for address, session := range s.sessions {
		if (!exclude || address != except) && session != nil && session.connID == id && session.netCore == core {
			return true
		}
	}
	return false
}
func (s *Service) k3AdvertisedRegistryRemovedLocked(id domain.ConnID) {
	if !s.k3Advertised.available() {
		return
	}
	if entry := s.conns[id]; entry != nil && entry.core != nil && entry.core.Dir() == netcore.Outbound && s.k3AdvertisedOtherSessionLocked(id, entry.core, "", false) {
		return
	}
	delete(s.k3AdvertisedClaims, id)
	delete(s.k3AdvertisedGenerations, k3AdvertisedPeerKey{Direction: "inbound", ConnID: id})
	delete(s.k3AdvertisedGenerations, k3AdvertisedPeerKey{Direction: "outbound", ConnID: id})
}
func (s *Service) k3AdvertisedSessionChangingLocked(address domain.PeerAddress, next *peerSession) {
	o := s.k3Advertised
	if !o.available() {
		return
	}
	old := s.sessions[address]
	if old == next {
		return
	}
	if old != nil {
		entry := s.conns[old.connID]
		registered := entry != nil && entry.core == old.netCore
		if !registered && !s.k3AdvertisedOtherSessionLocked(old.connID, old.netCore, address, true) {
			delete(s.k3AdvertisedClaims, old.connID)
			delete(s.k3AdvertisedGenerations, k3AdvertisedPeerKey{Direction: "outbound", ConnID: old.connID})
		}
	}
	if next != nil {
		key := k3AdvertisedPeerKey{Direction: "outbound", ConnID: next.connID}
		entry := s.conns[next.connID]
		registered := entry != nil && entry.core == next.netCore && entry.core != nil
		same := registered || s.k3AdvertisedOtherSessionLocked(next.connID, next.netCore, address, true)
		if same {
			if s.k3AdvertisedGenerations[key] == 0 {
				o.fail("missing-connection-generation")
			}
			s.seedK3AdvertisedDialLocked(next.connID, next.address)
			return
		}
		if entry != nil {
			o.fail("connection-binding-conflict")
			return
		}
		s.k3AdvertisedAssignGenerationLocked(key)
		s.seedK3AdvertisedDialLocked(next.connID, next.address)
	}
}

func (s *Service) seedK3AdvertisedDialLocked(id domain.ConnID, address domain.PeerAddress) {
	if !s.k3Advertised.available() {
		return
	}
	if len(address) > k3AdvertisedMaxAddressBytes {
		s.k3Advertised.fail("dial-address-capacity")
		return
	}
	claim := s.k3AdvertisedClaims[id]
	if claim.DialAddress != "" && claim.DialAddress != address {
		s.k3Advertised.fail("dial-context-conflict")
		return
	}
	claim.DialAddress = domain.PeerAddress(strings.Clone(string(address)))
	s.k3AdvertisedRawClaimLocked(id, claim)
}

// The actual welcome writer, under its original peerMu. No retained claim is
// inferred from NetCore's nonzero-skip metadata or from session proof.
func (s *Service) applyK3AdvertisedWelcomeLocked(session *peerSession, welcome protocol.Frame, local []domain.Capability) {
	t := s.beginK3AdvertisedPeerLocked()
	if t != nil {
		defer s.guardK3AdvertisedPeerLocked(t)
	}
	applyWelcomeMetadata(session, welcome, local)
	if entry := s.connEntryByIDLocked(session.connID); entry != nil && entry.core == session.netCore {
		previous := s.k3AdvertisedClaims[session.connID]
		s.k3AdvertisedRawClaimLocked(session.connID, k3AdvertisedRawClaim{Seen: true, Identity: domain.PeerIdentityFromWire(welcome.Address), Version: welcome.Version, Address: session.address, DialAddress: previous.DialAddress})
	} else if session.netCore != nil && s.k3AdvertisedOtherSessionLocked(session.connID, session.netCore, "", false) {
		previous := s.k3AdvertisedClaims[session.connID]
		s.k3AdvertisedRawClaimLocked(session.connID, k3AdvertisedRawClaim{Seen: true, Identity: domain.PeerIdentityFromWire(welcome.Address), Version: welcome.Version, Address: session.address, DialAddress: previous.DialAddress})
	}
	s.finishK3AdvertisedPeerLocked(t)
}

func (s *Service) k3AdvertisedRawClaimLocked(id domain.ConnID, claim k3AdvertisedRawClaim) {
	o := s.k3Advertised
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fault != "" || !o.ready {
		return
	}
	if len(claim.Address) > k3AdvertisedMaxAddressBytes || len(claim.DialAddress) > k3AdvertisedMaxAddressBytes {
		o.failLocked("claim-address-capacity")
		return
	}
	if previous, exists := s.k3AdvertisedClaims[id]; exists && previous.DialAddress != "" {
		claim.DialAddress = previous.DialAddress
	}
	if s.k3AdvertisedClaims == nil {
		s.k3AdvertisedClaims = make(map[domain.ConnID]k3AdvertisedRawClaim)
	}
	if _, exists := s.k3AdvertisedClaims[id]; !exists && len(s.k3AdvertisedClaims) >= o.connectionCapacity {
		o.failLocked("claim-capacity")
		return
	}
	claim.Address = domain.PeerAddress(strings.Clone(string(claim.Address)))
	claim.DialAddress = domain.PeerAddress(strings.Clone(string(claim.DialAddress)))
	s.k3AdvertisedClaims[id] = claim
}

// Called solely by the selected inbound route, with ORIGINAL handle and opts
// prepared outside peerMu. The stale branch never retries onto a replacement.
func (s *Service) applyK3AdvertisedInboundOpts(id domain.ConnID, original *netcore.NetCore, opts netcore.Options, claim k3AdvertisedRawClaim) {
	s.peerMu.Lock()
	entry := s.connEntryByIDLocked(id)
	if entry == nil || entry.core != original {
		s.peerMu.Unlock()
		original.ApplyOpts(opts)
		return
	}
	defer s.peerMu.Unlock()
	t := s.beginK3AdvertisedPeerLocked()
	if t != nil {
		defer s.guardK3AdvertisedPeerLocked(t)
	}
	original.ApplyOpts(opts)
	if claim.Seen {
		s.k3AdvertisedRawClaimLocked(id, claim)
	}
	s.finishK3AdvertisedPeerLocked(t)
}

// Auth mirror is NOT another raw advertisement. Keep prior raw zero/missing
// values and expose contradiction without normalizing it from proof/mirrors.
func (s *Service) mirrorK3AdvertisedInboundIdentity(id domain.ConnID, original *netcore.NetCore, identity domain.PeerIdentity, address domain.PeerAddress) {
	s.peerMu.Lock()
	entry := s.connEntryByIDLocked(id)
	if entry == nil || entry.core != original {
		s.peerMu.Unlock()
		original.SetIdentity(identity)
		original.SetAddress(address)
		return
	}
	defer s.peerMu.Unlock()
	t := s.beginK3AdvertisedPeerLocked()
	if t != nil {
		defer s.guardK3AdvertisedPeerLocked(t)
	}
	original.SetIdentity(identity)
	original.SetAddress(address)
	if claim, seen := s.k3AdvertisedClaims[id]; seen && claim.Seen && claim.Identity != identity {
		claim.Conflict = true
		s.k3AdvertisedRawClaimLocked(id, claim)
	}
	s.finishK3AdvertisedPeerLocked(t)
}

func (s *Service) collectK3AdvertisedPeersLocked(o *k3AdvertisedSource) (map[k3AdvertisedPeerKey]k3AdvertisedPeerFact, bool) {
	if len(s.conns) > o.connectionCapacity || len(s.sessions) > o.connectionCapacity {
		return nil, false
	}
	values := make(map[k3AdvertisedPeerKey]k3AdvertisedPeerFact)
	// Mutation-local validation only, never retained in Source/history.
	cores := make(map[domain.ConnID]*netcore.NetCore)
	add := func(key k3AdvertisedPeerKey, core *netcore.NetCore, address domain.PeerAddress) bool {
		if len(values) >= o.connectionCapacity || key.ConnID == 0 || len(address) > k3AdvertisedMaxAddressBytes {
			return false
		}
		claim := s.k3AdvertisedClaims[key.ConnID]
		if len(claim.Address) > k3AdvertisedMaxAddressBytes || len(claim.DialAddress) > k3AdvertisedMaxAddressBytes {
			return false
		}
		generation := s.k3AdvertisedGenerations[key]
		if generation == 0 {
			return false
		}
		healthAddress := s.resolveHealthAddress(address)
		if len(healthAddress) > k3AdvertisedMaxAddressBytes {
			return false
		}
		h := s.health[healthAddress]
		present := h != nil
		connected := false
		if present {
			connected = h.Connected
		} else if key.Direction == "inbound" {
			connected = true
		}
		metadataIdentity := core.Identity()
		values[key] = k3AdvertisedPeerFact{Direction: key.Direction, Address: domain.PeerAddress(strings.Clone(string(address))), ConnID: key.ConnID, Generation: generation, ClaimSeen: claim.Seen, Identity: claim.Identity, Version: claim.Version, RawAddress: domain.PeerAddress(strings.Clone(string(claim.Address))), MetadataIdentity: metadataIdentity, ClaimConflict: claim.Conflict || (claim.Seen && claim.Identity != metadataIdentity), HealthAddress: domain.PeerAddress(strings.Clone(string(healthAddress))), HealthPresent: present, Connected: connected, ContextKnown: address != ""}
		cores[key.ConnID] = core
		return true
	}
	for id, entry := range s.conns {
		if entry == nil || entry.core == nil || entry.core.ConnID() != id {
			return nil, false
		}
		direction := k3AdvertisedDirection(entry.core)
		if direction == "" {
			return nil, false
		}
		address := entry.core.Address()
		if direction == "outbound" {
			address = s.k3AdvertisedClaims[id].DialAddress
		}
		if !add(k3AdvertisedPeerKey{Direction: direction, ConnID: id}, entry.core, address) {
			return nil, false
		}
	}
	for address, session := range s.sessions {
		if session == nil || session.connID == 0 || session.netCore == nil || session.netCore.ConnID() != session.connID || session.netCore.Dir() != netcore.Outbound {
			return nil, false
		}
		if len(address) > k3AdvertisedMaxAddressBytes || len(session.address) > k3AdvertisedMaxAddressBytes {
			return nil, false
		}
		key := k3AdvertisedPeerKey{Direction: "outbound", ConnID: session.connID}
		if previous, exists := values[key]; exists {
			if cores[session.connID] != session.netCore || !previous.ContextKnown || s.resolveHealthAddress(address) != previous.HealthAddress || s.resolveHealthAddress(session.address) != previous.HealthAddress {
				return nil, false
			}
			continue // SAME actual Core+ConnID; never dedup merely by claim/address.
		}
		if _, conflict := cores[session.connID]; conflict {
			return nil, false
		}
		if s.resolveHealthAddress(address) != s.resolveHealthAddress(session.address) {
			return nil, false
		}
		if !add(key, session.netCore, s.k3AdvertisedClaims[session.connID].DialAddress) {
			return nil, false
		}
	}
	return values, true
}

func (cm *ConnectionManager) finishK3AdvertisedAdmission(t *k3AdvertisedTicket, result manualPeerAdmission) manualPeerAdmission {
	cm.finishK3AdvertisedLocked(t)
	return result
}
