package node

import (
	"context"
	"strconv"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Sole node/findnode import. No handlers, admission, signing or packet writer.
// Ordinary constructors leave this nil. Its origin is independent of N-in.
type findNodeAdapter struct{ origin time.Time }
type findNodeConnectionBinding struct {
	seed      findnode.SessionSeed
	session   findnode.Session
	published bool
	group     domain.SourceGroup
	identity  domain.PeerIdentity
}
type findNodeCMReceipt struct {
	peer       *peerSession
	common     *proofTransportLifetime
	address    domain.PeerAddress
	generation uint64
}

func newFindNodeCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus) (*Service, error) {
	s := newServiceWithDatagramConstructor(cfg, id, bus, nil, productionDatagramConstructor{})
	s.findNode = &findNodeAdapter{origin: time.Now()}
	return s, nil
}
func newFindNodeInboundCandidateService(cfg config.Node, id *identity.Identity, bus *ebus.Bus, clock inboundPinClock) (*Service, error) {
	s, err := newInboundPinCandidateService(cfg, id, bus, clock)
	if err != nil {
		return s, err
	}
	s.findNode = &findNodeAdapter{origin: time.Now()}
	return s, nil
}
func (s *Service) captureFindNodeSeed(l *proofTransportLifetime, proof sessionv2.ProvenSession, intro protocol.Frame) {
	if s.findNode == nil {
		return
	}
	peer, err := proof.Peer()
	if err != nil || s.isSelfIdentity(peer.Identity) {
		return
	}
	local := make([]domain.Capability, 0, len(intro.Capabilities))
	for _, cap := range intro.Capabilities {
		if parsed, ok := domain.ParseCapability(cap); ok {
			local = append(local, parsed)
		}
	}
	seed, err := findnode.PrepareSession(proof, local)
	if err != nil {
		return
	}
	l.findNode = &findNodeConnectionBinding{seed: seed, group: l.evidence.group, identity: peer.Identity}
}
func (b *findNodeConnectionBinding) end() { b.session.End(); b.seed = findnode.SessionSeed{} }

// Called with peerMu then common.mu, AFTER all waits and owner/auth checks.
func (s *Service) publishFindNodeLocked(l *proofTransportLifetime) bool {
	if s.findNode == nil || l.ended || l.findNode == nil {
		return false
	}
	b := l.findNode
	if b.published {
		return true
	}
	elapsed := time.Since(s.findNode.origin)
	if elapsed < 0 || s.runCtx.Err() != nil {
		return false
	}
	session, err := b.seed.Bind(l.id, b.group, findnode.Instant(elapsed))
	if err != nil {
		return false
	}
	b.session = session
	b.published = true
	b.seed = findnode.SessionSeed{}
	return true
}

// Receipt creation is exclusively the generation-accepted CM callback. The
// selectedGeneration mirror alone cannot manufacture this receipt.
func (s *Service) captureFindNodeCMAcceptedLocked(info SessionInfo) {
	p := info.Session
	if p == nil || p.common == nil || info.SlotGeneration == 0 {
		return
	}
	p.findNodeReceipt = &findNodeCMReceipt{peer: p, common: p.common, address: info.Address, generation: info.SlotGeneration}
}
func (s *Service) publishFindNodeCM(p *peerSession, address domain.PeerAddress, generation uint64) bool {
	if p == nil || p.common == nil {
		return false
	}
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	l := p.common
	l.mu.Lock()
	defer l.mu.Unlock()
	r := p.findNodeReceipt
	e := s.connEntryByIDLocked(p.connID)
	if l.token != s.proofToken || l.findNode == nil || p.peerIdentity != l.findNode.identity {
		return false
	}
	if r == nil || r.peer != p || r.common != l || r.address != address || r.generation != generation || p.selectedGeneration != generation || s.sessions[p.address] != p || l.peer != p || l.entry != e || e == nil || e.common != l || e.core != p.netCore || l.id != p.connID || !p.authOK || !l.outbound || l.role != sessionv2.RoleDialer {
		return false
	}
	ok := s.publishFindNodeLocked(l)
	if l.nin == nil {
		l.evidence = nil
	}
	return ok
}

type findNodeMemberCoverage uint8

const registeredAcceptedAndCMPostSetup findNodeMemberCoverage = 1

type findNodeMemberSnapshot struct {
	coverage findNodeMemberCoverage
	sessions []findnode.Session
}

// Observation only. It excludes unregistered one-shots; no complete-overlay or
// class/admission authority is implied. No cached member inventory is retained.
func (s *Service) findNodeMembers(ctx context.Context) (findNodeMemberSnapshot, error) {
	out := findNodeMemberSnapshot{coverage: registeredAcceptedAndCMPostSetup}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	var walkErr error
	s.forEachConnIDLocked(func(id domain.ConnID) bool {
		if err := ctx.Err(); err != nil {
			walkErr = err
			return false
		}
		e := s.connEntryByIDLocked(id)
		if e == nil || e.common == nil {
			return true
		}
		l := e.common
		l.mu.Lock()
		if !l.ended && l.entry == e && l.findNode != nil && l.findNode.published {
			out.sessions = append(out.sessions, l.findNode.session)
		}
		l.mu.Unlock()
		return true
	})
	if walkErr != nil {
		return findNodeMemberSnapshot{}, walkErr
	}
	if err := ctx.Err(); err != nil {
		return findNodeMemberSnapshot{}, err
	}
	return out, nil
}

// Required copied input for this unused observation only. No runtime default,
// configuration, retained member registry or network admission is installed.
type findNodePriorityProfile struct{ MaxMembers, MaxRegistryVisits int }

func (p findNodePriorityProfile) valid() bool {
	maxInt := int(^uint(0) >> 1)
	// Session is a closed one-pointer opaque handle. Check element-byte
	// arithmetic before make; this is not a RAM affordability guarantee.
	return p.MaxMembers > 0 && p.MaxMembers <= maxInt/(strconv.IntSize/8) && p.MaxRegistryVisits >= p.MaxMembers && p.MaxRegistryVisits < maxInt
}

type findNodePriorityContact struct {
	applicable, confirmed bool
	err                   error
}
type findNodePriorityClassFact struct {
	identity domain.PeerIdentity
	class    findnode.SessionClass
}

// peerMu is held. A current lookup may only CHECK the exact captured Session,
// never replace it with a new proof, same identity, address or connection ID.
func (s *Service) findNodePriorityMemberLocked(ctx context.Context, id domain.ConnID, expected findnode.Session) (findnode.Session, bool, error) {
	if err := ctx.Err(); err != nil {
		return findnode.Session{}, false, err
	}
	e := s.connEntryByIDLocked(id)
	if e == nil || e.common == nil {
		return findnode.Session{}, false, nil
	}
	l := e.common
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return findnode.Session{}, false, err
	}
	b := l.findNode
	if l.ended || b == nil || !b.published {
		return findnode.Session{}, false, nil
	}
	if l.token == nil || l.token != s.proofToken || l.id != id || l.entry != e || b.session.ConnID() != id || b.identity != b.session.Identity() {
		return findnode.Session{}, false, findnode.ErrUnauthenticated
	}
	if expected.ConnID() != 0 && b.session != expected {
		return findnode.Session{}, false, findnode.ErrStaleWork
	}
	return b.session, true, nil
}

// Unlike the historical unbounded observer, this path never captures all F1
// members before checking the profile. Success is complete, never first-M.
func (s *Service) findNodePriorityMembers(ctx context.Context, profile findNodePriorityProfile) (findNodeMemberSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return findNodeMemberSnapshot{}, err
	}
	if !profile.valid() {
		return findNodeMemberSnapshot{}, findnode.ErrInvalid
	}
	out := findNodeMemberSnapshot{coverage: registeredAcceptedAndCMPostSetup, sessions: make([]findnode.Session, 0, profile.MaxMembers)}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	visits := 0
	var walkErr error
	s.forEachConnIDLocked(func(id domain.ConnID) bool {
		if walkErr = ctx.Err(); walkErr != nil {
			return false
		}
		if visits == profile.MaxRegistryVisits {
			walkErr = findnode.ErrCapacity
			return false
		}
		visits++
		var member findnode.Session
		var found bool
		member, found, walkErr = s.findNodePriorityMemberLocked(ctx, id, findnode.Session{})
		if walkErr != nil {
			return false
		}
		if !found {
			return true
		}
		if len(out.sessions) == profile.MaxMembers {
			walkErr = findnode.ErrCapacity
			return false
		}
		out.sessions = append(out.sessions, member)
		return true
	})
	if walkErr != nil {
		return findNodeMemberSnapshot{}, walkErr
	}
	if err := ctx.Err(); err != nil {
		return findNodeMemberSnapshot{}, err
	}
	return out, nil
}

// All local relation/pin gates are released before any new F1 owner check or
// callback. Reads are proportional to the bounded member list, not contacts.
func (s *Service) captureFindNodePriorityClasses(ctx context.Context, members []findnode.Session) (map[domain.ConnID]findNodePriorityClassFact, error) {
	store, trust := s.localContactPinStore(), s.trust
	if store == nil || trust == nil {
		return nil, findnode.ErrNotReady
	}
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	trust.pinEvidenceMu.RLock()
	defer trust.pinEvidenceMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	contacts := make(map[domain.PeerIdentity]findNodePriorityContact, len(members))
	trust.mu.RLock()
	for _, member := range members {
		id := member.Identity()
		value := findNodePriorityContact{err: trust.authorityReadErr}
		if fact := trust.contacts[id.String()].LocalContact; fact != nil {
			value.applicable = true
			if fact.revision == 0 || fact.fact.Version != 1 || fact.fact.At.IsZero() || fact.fact.Identity != id || fact.fact.Network != domain.NetworkID(networkName) || fact.fact.ID == (localContactFactID{}) {
				value.err = domain.ErrLocalContactFactStale
			} else {
				value.confirmed = fact.confirmed == fact.revision
			}
		}
		contacts[id] = value
	}
	trust.mu.RUnlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.pinMetadataMutationRefusalLocked(); err != nil {
		return nil, err
	}
	out := make(map[domain.ConnID]findNodePriorityClassFact, len(members))
	for _, member := range members {
		id := member.Identity()
		class := findnode.ClassOrdinary
		if entry, present := store.identities[id]; present {
			if entry.incarnation == 0 || entry.evidenceRevision == 0 || entry.protectionRevision == 0 {
				return nil, domain.ErrIdentityPinStoreUnavailable
			}
			metadata := entry.classMetadata()
			if err := metadata.Validate(); err != nil {
				return nil, err
			}
			contact := contacts[id]
			strict := entry.metadata != nil && entry.metadata.confirmedRevision == entry.protectionRevision
			complete := entry.metadata == nil || entry.metadata.recordOwner == nil || entry.metadata.recordMirror == contactRecordOwnerCompleted
			hasK := metadata.Contact.State == domain.IdentityPinAllocationEligible
			kReady := hasK && contact.applicable && contact.err == nil && contact.confirmed && strict && complete
			kUnknown := hasK && (contact.err != nil || (contact.applicable && !kReady))
			switch {
			case kReady:
				class = findnode.ClassContact
			case entry.selectedConfirmed():
				class = findnode.ClassSelected
			case kUnknown || metadata.Selected.State == domain.IdentityPinAllocationEligible:
				return nil, findnode.ErrNotReady
			case entry.inboundMetadataConfirmed():
				class = findnode.ClassIncomingPinned
			case metadata.Inbound.State == domain.IdentityPinAllocationEligible:
				return nil, findnode.ErrNotReady
			}
		}
		out[member.ConnID()] = findNodePriorityClassFact{identity: id, class: class}
	}
	return out, nil
}

// A private historical observation, NOT R/T admission or a C3-immune permit.
// Mutex waits and runtime map iteration have no hard cancellation latency.
func (s *Service) findNodePrioritySnapshot(ctx context.Context, target domain.PeerIdentity, profile findNodePriorityProfile) (findnode.PrioritySnapshot, findNodeMemberCoverage, error) {
	if err := ctx.Err(); err != nil {
		return findnode.PrioritySnapshot{}, 0, err
	}
	if s.findNode == nil || s.proofToken == nil || s.localContactPinStore() == nil || s.trust == nil || !profile.valid() || target.IsZero() {
		return findnode.PrioritySnapshot{}, 0, findnode.ErrInvalid
	}
	adapter, store, trust := s.findNode, s.localContactPinStore(), s.trust
	members, err := s.findNodePriorityMembers(ctx, profile)
	if err != nil {
		return findnode.PrioritySnapshot{}, 0, err
	}
	// Target links count toward M, but never consume a requester L2 position.
	requesters := members.sessions[:0]
	for _, member := range members.sessions {
		if member.Identity() != target {
			requesters = append(requesters, member)
		}
	}
	clear(members.sessions[len(requesters):])
	members.sessions = requesters
	classes, err := s.captureFindNodePriorityClasses(ctx, members.sessions)
	if err != nil {
		return findnode.PrioritySnapshot{}, 0, err
	}
	s.peerMu.RLock()
	for _, member := range members.sessions {
		_, found, checkErr := s.findNodePriorityMemberLocked(ctx, member.ConnID(), member)
		if checkErr != nil || !found {
			s.peerMu.RUnlock()
			if checkErr != nil {
				return findnode.PrioritySnapshot{}, 0, checkErr
			}
			return findnode.PrioritySnapshot{}, 0, findnode.ErrStaleWork
		}
	}
	s.peerMu.RUnlock()
	if s.findNode != adapter || s.localContactPinStore() != store || s.trust != trust || s.runCtx.Err() != nil {
		return findnode.PrioritySnapshot{}, 0, findnode.ErrStaleWork
	}
	at := time.Since(adapter.origin)
	if at < 0 {
		return findnode.PrioritySnapshot{}, 0, findnode.ErrClock
	}
	snapshot, err := findnode.NewPrioritySnapshot(target, members.sessions, findnode.Instant(at), func(info findnode.SessionInfo) (findnode.SessionClass, error) {
		id, proven := info.Proof.Identity()
		fact, found := classes[info.ConnID]
		if !proven || !found || fact.identity != id {
			return 0, findnode.ErrUnauthenticated
		}
		return fact.class, nil
	})
	if err != nil {
		return findnode.PrioritySnapshot{}, 0, err
	}
	if err := ctx.Err(); err != nil {
		return findnode.PrioritySnapshot{}, 0, err
	}
	return snapshot, members.coverage, nil
}
