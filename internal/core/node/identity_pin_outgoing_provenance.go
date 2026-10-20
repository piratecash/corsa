package node

// Ordinary construction, retries and raw commands carry nil. One unused internal
// candidate root can compose genuine typed own-DM/CM provenance; no R admission.
import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

var errR2aOrigin = errors.New("outgoing provenance unavailable or stale")
var errR2aNotWritten = errors.New("outgoing provenance has no positive first-write witness")

type r2aPreparedOwnDM struct{ sealed *r2aPreparedEnvelope }
type r2aPreparedEnvelope struct {
	owner       *Service
	message     incomingMessage
	digest      [sha256.Size]byte
	recipient   pinOwnerObservation
	r2Residence *ownDMResidenceOrigin
}
type r2aHopPublication struct{ sealed *r2aPublishedHop }
type r2aPublishedHop struct {
	owner            *Service
	session          *peerSession
	connID           domain.ConnID
	proof            sessionv2.ProvenIdentity
	binding          identityPinEntryBinding
	r2PublishedReady bool // peerMu; candidate publication only, after actual CM Ready
}

// r2aOwnSendStamp and its candidate belong to deliveryMu. The immutable
// preparation/hop objects are never exposed as mutable public DTOs.
type r2aOwnSendStamp struct {
	prepared  *r2aPreparedEnvelope
	entry     *deliveryRetryEntry
	candidate *r2aFirstWrite
	consumed  bool
	r2Claim   *ownDMRelationClaim
}
type r2aFirstWrite struct {
	stamp   *r2aOwnSendStamp
	hop     *r2aPublishedHop
	ack     chan struct{}
	offered bool
}
type r2aWireProjection struct {
	createdAt time.Time
	ttl       int
}

type r2aDispatch struct {
	ctx           context.Context
	prepared      *r2aPreparedEnvelope
	expected      r2aHopPublication
	stamp         *r2aOwnSendStamp
	early         bool
	failure       error
	projection    *r2aWireProjection
	r2ResolvedHop *r2aPublishedHop // same synchronous directed dispatch, never caller supplied
}
type r2aSubmissionResult struct {
	r2Dispatched bool // ordinary store call actually occurred
	stored       bool
	count        int
	code         string
	stamp        *r2aOwnSendStamp
}
type r2aRecipientEvidence struct {
	original pinOwnerObservation
	message  messageIdentity
	body     [sha256.Size]byte
}
type r2aNearestHopEvidence struct {
	original  identityPinEntryBinding
	recipient domain.PeerIdentity
	connID    domain.ConnID
	proof     sessionv2.ProvenIdentity
}

func (s *Service) prepareR2aFixtureOwnDM(ctx context.Context, input protocol.Envelope) (r2aPreparedOwnDM, error) {
	return s.prepareR2aOwnDM(ctx, input)
}
func (s *Service) prepareR2aOwnDM(ctx context.Context, input protocol.Envelope) (r2aPreparedOwnDM, error) {
	if err := ctx.Err(); err != nil {
		return r2aPreparedOwnDM{}, err
	}
	if s.identity == nil || s.secureSessions == nil || s.secureSessions.store == nil {
		return r2aPreparedOwnDM{}, errR2aOrigin
	}
	// Clone BEFORE verification; conversion to string owns its bytes as well.
	body := string(append([]byte(nil), input.Payload...))
	target, err := domain.ParsePeerIdentity(input.Recipient)
	if err != nil || target.IsZero() || input.Sender != s.Address() || input.Recipient == s.Address() || input.Topic != "dm" || input.Via != "" || input.ViaIdentity != "" {
		return r2aPreparedOwnDM{}, errR2aOrigin
	}
	msg, err := incomingMessageFromFrame(protocol.Frame{ID: string(input.ID), Type: "send_message", Topic: input.Topic, Address: input.Sender, Recipient: input.Recipient, Flag: string(input.Flag), CreatedAt: input.CreatedAt.Format(time.RFC3339Nano), TTLSeconds: input.TTLSeconds, Body: body})
	if err != nil {
		return r2aPreparedOwnDM{}, errors.Join(errR2aOrigin, err)
	}
	if err := directmsg.VerifyEnvelope(input.Sender, identity.PublicKeyBase64(s.identity.PublicKey), input.Recipient, body); err != nil {
		return r2aPreparedOwnDM{}, errors.Join(errR2aOrigin, err)
	}
	store := s.secureSessions.store
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return r2aPreparedOwnDM{}, err
	}
	observed, err := store.pinOwnerObservationLocked(target)
	if err != nil {
		return r2aPreparedOwnDM{}, err
	}
	return r2aPreparedOwnDM{sealed: &r2aPreparedEnvelope{owner: s, message: msg, digest: sha256.Sum256([]byte(body)), recipient: observed}}, nil
}

func (s *Service) submitR2aFixtureOwnDM(ctx context.Context, prepared r2aPreparedOwnDM, expected r2aHopPublication) (r2aSubmissionResult, error) {
	return s.submitR2aOwnDM(ctx, prepared, expected)
}
func (s *Service) submitR2aOwnDM(ctx context.Context, prepared r2aPreparedOwnDM, expected r2aHopPublication) (r2aSubmissionResult, error) {
	if err := ctx.Err(); err != nil {
		return r2aSubmissionResult{}, err
	}
	if prepared.sealed == nil || prepared.sealed.owner != s {
		return r2aSubmissionResult{}, errR2aOrigin
	}
	dispatch := &r2aDispatch{ctx: ctx, prepared: prepared.sealed, expected: expected}
	stored, count, code := s.storeIncomingMessageWithR2a(prepared.sealed.message, true, dispatch)
	if !stored && code == "" {
		dispatch.stamp = s.existingR2aOrigin(prepared.sealed)
	}
	return r2aSubmissionResult{r2Dispatched: true, stored: stored, count: count, code: code, stamp: dispatch.stamp}, dispatch.failure
}

// registerR2aOrigin runs after real storage admission and before Route/events.
// It takes the removal gate once and uses the existing locked observation API.
func (s *Service) registerR2aOrigin(d *r2aDispatch, envelope protocol.Envelope) {
	store := s.secureSessions.store
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := d.ctx.Err(); err != nil {
		d.failure = err
		return
	}
	if store.pinMetadataMutationRefusalLocked() != nil || !d.prepared.recipient.matchesLocked(store) || !d.prepared.matches(envelope) {
		d.failure = errR2aOrigin
		return
	}
	if existing := s.awaitingDelivered[envelope.ID]; existing != nil {
		// A generic/previous owner is never promoted by a late opt-in call.
		d.failure = errR2aOrigin
		return
	}
	s.registerAwaitingDeliveredLocked(envelope, s.peerNow(), true)
	entry := s.awaitingDelivered[envelope.ID]
	stamp := &r2aOwnSendStamp{prepared: d.prepared, entry: entry}
	entry.outgoingProvenance = stamp
	d.stamp, d.early = stamp, true
}
func (p *r2aPreparedEnvelope) matches(e protocol.Envelope) bool {
	return p != nil && e.ID == p.message.ID && e.Sender == p.message.Sender && e.Recipient == p.message.Recipient && e.Topic == p.message.Topic && e.Flag == p.message.Flag && sha256.Sum256(e.Payload) == p.digest
}
func (s *Service) r2aUpdateInitialHold(d *r2aDispatch, held bool) bool {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	if !s.r2aCurrentLocked(d.stamp) {
		return false
	}
	d.stamp.entry.Hold = holdReasonFor(held)
	return true
}
func (s *Service) r2aCurrentLocked(stamp *r2aOwnSendStamp) bool {
	if stamp == nil || stamp.prepared == nil || stamp.prepared.owner != s || stamp.entry == nil {
		return false
	}
	entry := s.awaitingDelivered[stamp.prepared.message.ID]
	return entry == stamp.entry && entry.outgoingProvenance == stamp && stamp.prepared.matches(entry.Envelope)
}

func (s *Service) openR2aFixtureHop(ctx context.Context, address domain.PeerAddress) (*peerSession, r2aHopPublication, error) {
	transport, err := s.dialPeerTransportForCM(ctx, address)
	if err != nil {
		return nil, r2aHopPublication{}, err
	}
	publication, err := s.captureR2aHopPublication(transport)
	if err != nil {
		_ = transport.wire.Close()
		return nil, r2aHopPublication{}, err
	}
	session, err := s.finishPeerSessionForCM(ctx, address, transport)
	if err != nil {
		return nil, r2aHopPublication{}, err
	}
	publication.sealed.session = session
	return session, publication, nil
}
func (s *Service) captureR2aHopPublication(transport *dialledTransport) (r2aHopPublication, error) {
	if transport == nil || transport.proven == nil || transport.connID == 0 || transport.pinBinding.owner == nil {
		return r2aHopPublication{}, errR2aOrigin
	}
	proof, ok := transport.proven.Proof()
	id, valid := proof.Identity()
	if !ok || !valid || id != transport.pinBinding.identity || s.secureSessions == nil || transport.pinBinding.owner != s.secureSessions.store {
		return r2aHopPublication{}, errR2aOrigin
	}
	// Mandatory publication's existing contract permits memory-only stores.
	// This original binding is NOT a strict metadata durability receipt.
	return r2aHopPublication{sealed: &r2aPublishedHop{owner: s, connID: transport.connID, proof: proof, binding: transport.pinBinding}}, nil
}

type r2aDirectedDispatch struct {
	origin  *r2aDispatch
	nextHop domain.PeerIdentity
	address domain.PeerAddress
	hops    int
}

// snapshotR2aDirectedSession validates the ACTUAL Route result against the
// expected owner; it never selects a session from the expected capsule.
func (s *Service) snapshotR2aDirectedSession(route *r2aDirectedDispatch) *peerSession {
	if route == nil || route.origin == nil || strings.HasPrefix(string(route.address), "inbound:") {
		return nil
	}
	expected := route.origin.expected.sealed
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	session := s.sessions[route.address]
	if expected == nil && s.r2OwnDMOwner != nil && session != nil {
		expected = session.r2OwnDMHop
		if expected == nil || !expected.r2PublishedReady {
			return nil
		}
	}
	if expected == nil || session == nil || session != expected.session || expected.owner != s || session.connID != expected.connID || session.peerIdentity != route.nextHop || expected.binding.identity != route.nextHop || !session.authOK || session.eligibilityEnded.Load() || session.closedBy.Load() != nil {
		return nil
	}
	health := s.health[s.resolveHealthAddress(route.address)]
	if !s.peerHealthAcceptsOutboundFramesLocked(health, s.peerNow()) {
		return nil
	}
	if !sessionHasCap(session.capabilities, domain.CapMeshRelayV1) {
		return nil
	}
	if route.hops > 1 && !sessionHasBothCaps(session.capabilities, domain.CapMeshRelayV1, domain.CapMeshRoutingV1) {
		return nil
	}
	actual, ok := session.provenIdentity()
	id, valid := actual.Identity()
	expectedID, expectedValid := expected.proof.Identity()
	if !ok || !valid || !expectedValid || id != expectedID || id != route.nextHop {
		return nil
	}
	route.origin.r2ResolvedHop = expected
	return session
}

// captureR2aFirstDirectedItem owns its channel and only adds it to the common
// builder's first item. Caller sends after every lock below is released.
func (s *Service) captureR2aFirstDirectedItem(route *r2aDirectedDispatch, session *peerSession, item peerSendItem) *r2aFirstWrite {
	if route == nil || session == nil {
		return nil
	}
	d := route.origin
	store := s.secureSessions.store
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	return s.claimR2aFirstDirectedItemLocked(d, session, item)
}
func (s *Service) claimR2aFirstDirectedItemLocked(d *r2aDispatch, session *peerSession, item peerSendItem) *r2aFirstWrite {
	if d.ctx.Err() != nil || !s.r2aCurrentLocked(d.stamp) || d.stamp.consumed || d.stamp.candidate != nil || d.r2ResolvedHop == nil {
		return nil
	}
	if _, frozen := s.frozenDeliveries[item.delivery.Envelope.ID]; frozen {
		return nil
	}
	hop := d.r2ResolvedHop
	store := s.secureSessions.store
	if store.pinMetadataMutationRefusalLocked() != nil || !d.prepared.recipient.matchesLocked(store) || !hop.binding.matchesLocked(store) || hop.session != session || session.eligibilityEnded.Load() || session.closedBy.Load() != nil || !d.prepared.matches(item.delivery.Envelope) {
		return nil
	}
	if item.ID != string(d.prepared.message.ID) || item.Address != d.prepared.message.Sender || item.Recipient != d.prepared.message.Recipient || item.Topic != d.prepared.message.Topic || item.Flag != string(d.prepared.message.Flag) || sha256.Sum256([]byte(item.Body)) != d.prepared.digest {
		return nil
	}
	if d.projection == nil || item.CreatedAt != d.projection.createdAt.UTC().Format(time.RFC3339) || item.TTLSeconds != d.projection.ttl {
		return nil
	}
	// The projection is built internally; bind its exact values on this item.
	candidate := &r2aFirstWrite{stamp: d.stamp, hop: hop, ack: make(chan struct{})}
	d.stamp.candidate = candidate
	return candidate
}
func (s *Service) r2aRecordOffer(candidate *r2aFirstWrite, offered bool) {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	if s.r2aCurrentLocked(candidate.stamp) && candidate.stamp.candidate == candidate {
		candidate.offered = offered
	}
}
func (s *Service) settleR2aOwnHop(ctx context.Context, result r2aSubmissionResult) (r2aRecipientEvidence, r2aNearestHopEvidence, error) {
	emptyX, emptyY := r2aRecipientEvidence{}, r2aNearestHopEvidence{}
	if err := ctx.Err(); err != nil {
		return emptyX, emptyY, err
	}
	if result.stamp == nil || result.stamp.prepared.owner != s {
		return emptyX, emptyY, errR2aOrigin
	}
	store := s.secureSessions.store
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if result.stamp.r2Claim != nil {
		return emptyX, emptyY, errR2aOrigin
	}
	return s.settleR2aOwnHopLocked(ctx, result.stamp)
}

// Caller holds pinGate.R, deliveryMu and store.mu. Common genuine witness check.
func (s *Service) settleR2aOwnHopLocked(ctx context.Context, stamp *r2aOwnSendStamp) (r2aRecipientEvidence, r2aNearestHopEvidence, error) {
	emptyX, emptyY := r2aRecipientEvidence{}, r2aNearestHopEvidence{}
	store := s.secureSessions.store
	if err := ctx.Err(); err != nil {
		return emptyX, emptyY, err
	}
	if !s.r2aCurrentLocked(stamp) || stamp.consumed || s.runCtx.Err() != nil {
		return emptyX, emptyY, errR2aOrigin
	}
	if _, frozen := s.frozenDeliveries[stamp.prepared.message.ID]; frozen {
		return emptyX, emptyY, errR2aOrigin
	}
	candidate := stamp.candidate
	if candidate == nil || !candidate.offered {
		return emptyX, emptyY, errR2aNotWritten
	}
	select {
	case <-candidate.ack:
	default:
		return emptyX, emptyY, errR2aNotWritten
	}
	if store.pinMetadataMutationRefusalLocked() != nil || !stamp.prepared.recipient.matchesLocked(store) || !candidate.hop.binding.matchesLocked(store) {
		return emptyX, emptyY, errR2aOrigin
	}
	stamp.consumed = true
	return r2aRecipientEvidence{original: stamp.prepared.recipient, message: messageIdentityOf(stamp.entry.Envelope), body: stamp.prepared.digest}, r2aNearestHopEvidence{original: candidate.hop.binding, recipient: stamp.prepared.recipient.identity, connID: candidate.hop.connID, proof: candidate.hop.proof}, nil
}

// An idempotent typed retry can observe the SAME already stamped owner. It
// cannot attach authority to an ordinary duplicate or manufacture a new stamp.
func (s *Service) existingR2aOrigin(prepared *r2aPreparedEnvelope) *r2aOwnSendStamp {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	entry := s.awaitingDelivered[prepared.message.ID]
	if entry == nil || entry.outgoingProvenance == nil || !prepared.matches(entry.Envelope) {
		return nil
	}
	return entry.outgoingProvenance
}

// Called once with the existing internally produced first wire copy, never
// from a caller-supplied projection. No body/backing slice is retained here.
func sealR2aWireProjection(d *r2aDispatch, envelope protocol.Envelope) {
	if d != nil && d.early {
		d.projection = &r2aWireProjection{createdAt: envelope.CreatedAt, ttl: envelope.TTLSeconds}
	}
}
