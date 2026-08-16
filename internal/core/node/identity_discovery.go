package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// identity_discovery.go wires the three discovery dtypes of
// docs/protocol/identity-lookup.md into the datagram type registry and
// implements their endpoint behaviour:
//
//   - get_identity  — answer with the node's OWN record; only the addressee
//     answers, a transit that happens to hold the record must forward;
//   - post_identity — the initiator ingest, delegated to identityResolver;
//   - push_identity — a session peer's own record: authorized by the
//     session-identity rule, merged seq-gated, with the receive-side rate
//     limit and the conflict session-close of the design.
//
// Registration happens at layer construction — before the first handshake,
// because §6.1 fixes the declared dtype set for the lifetime of a session.

// identityLookupPayloadSchemaName names the shared discovery payload family
// in the registry.
const identityLookupPayloadSchemaName = "identity_lookup"

// maxSeenRequesterAttempts bounds the requester-triple dedup cache: the
// freshness window is ±5 minutes, so the cache only ever needs to remember
// minutes' worth of labels — a long window would demand hours.
const maxSeenRequesterAttempts = 4096

// pushIdentityMinInterval is the receive-side floor between accepted pushes
// of one session peer. The sender's contract is one push per minute with
// coalescing; a session breaching it twice inside one window is closed.
const pushIdentityMinInterval = time.Minute

// initialPushNoRouteRetries / initialPushNoRouteRetryDelay bound the wait
// for the handshake side effects (identity↔session binding, hello route)
// the initial push races against.
const (
	initialPushNoRouteRetries    = 20
	initialPushNoRouteRetryDelay = 250 * time.Millisecond
)

// registerIdentityDiscoveryTypes registers the three discovery dtypes. The
// handlers reach mutable Service state (self record, resolver, knowledge)
// through svc at delivery time, so construction order inside NewService
// stays free.
func registerIdentityDiscoveryTypes(types *datagram.TypeRegistry, svc *Service, network domain.NetworkID) error {
	clock := func() time.Time { return time.Now().UTC() }

	getHandler := &getIdentityHandler{
		svc: svc, network: network, clock: clock,
		seenRequesterAttempts: map[domain.PeerIdentity]time.Time{},
	}
	if err := types.Register(datagram.TypeRegistration{
		DType:   domain.DTypeGetIdentity,
		Modes:   []domain.DatagramMode{domain.DatagramModeRequest},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Payload: datagram.PayloadSchema{Name: identityLookupPayloadSchemaName, Version: domain.IdentityLookupSchemaVersion},
		// The request plane authenticates nobody at the transport level;
		// what the answer proves lives INSIDE the payload (record signature,
		// target_proof). Serving every direction is what a client node —
		// whose sessions are almost all dialled — depends on.
		SenderProof: datagram.SenderProvenInPayload,
		Handler:     getHandler,
	}); err != nil {
		return err
	}

	if err := types.Register(datagram.TypeRegistration{
		DType:       domain.DTypePostIdentity,
		Modes:       []domain.DatagramMode{domain.DatagramModeResponse},
		Classes:     []domain.DatagramClass{domain.DatagramClassControl},
		AnswersTo:   []domain.DType{domain.DTypeGetIdentity},
		Payload:     datagram.PayloadSchema{Name: identityLookupPayloadSchemaName, Version: domain.IdentityLookupSchemaVersion},
		SenderProof: datagram.SenderProvenInPayload,
		Handler:     &postIdentityHandler{svc: svc},
	}); err != nil {
		return err
	}

	pushHandler := &pushIdentityHandler{
		svc: svc, network: network, clock: clock,
		lastAcceptedAt: map[pushSessionKey]time.Time{},
		violationAt:    map[pushSessionKey]time.Time{},
	}
	return types.Register(datagram.TypeRegistration{
		DType:   domain.DTypePushIdentity,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Payload: datagram.PayloadSchema{Name: identityLookupPayloadSchemaName, Version: domain.IdentityLookupSchemaVersion},
		// The push carries its sender's proof in BOTH layers of the payload
		// path: the routed frame is signed (SignedSrc) and the record inside
		// is signed by its owner. The session-identity rule is enforced by
		// the authorizer below at the strongest level the direction offers —
		// see its comment for why this is not RequiresProvenPeer.
		SenderProof: datagram.SenderProvenInPayload,
		Authorizer:  &pushIdentityAuthorizer{},
		Handler:     pushHandler,
	})
}

// ---------------------------------------------------------------------------
// get_identity — the owner's answer
// ---------------------------------------------------------------------------

// getIdentityHandler answers lookups addressed to this node. Only the
// addressee ever runs it: transit never opens the payload, and a node
// holding somebody else's record forwards the frame untouched.
type getIdentityHandler struct {
	svc     *Service
	clock   func() time.Time
	network domain.NetworkID

	mu sync.Mutex
	// seenRequesterAttempts dedups the "you were looked up" notification by
	// attempt label within the freshness window.
	seenRequesterAttempts map[domain.PeerIdentity]time.Time
}

func (h *getIdentityHandler) Handle(_ context.Context, delivery datagram.DeliveryContext, payload []byte) datagram.HandlerResult {
	request, err := protocol.ParseGetIdentityPayload(payload)
	if err != nil {
		// Malformed or future-versioned requests are a silent drop: the
		// initiator reads silence as silence and retries; there is no
		// refusal frame in this plane.
		return datagram.RejectDelivery(err)
	}
	if !request.UnderstoodRequirements() {
		// Not understanding a requirement obliges the addressee to stay
		// SILENT: there is no more capable version of itself to forward to,
		// and answering past the requirement would fake compliance.
		return datagram.RejectDelivery(errors.New("identity lookup: requirement not understood"))
	}

	record, _ := h.svc.SelfIdentityRecord()
	if record.Version == 0 {
		return datagram.FailDelivery(errors.New("identity lookup: self record not issued"))
	}

	if !request.Requester.IsZero() {
		if !h.acceptRequesterTriple(delivery, request) {
			return datagram.RejectDelivery(errors.New("identity lookup: requester triple rejected"))
		}
	}

	answer := protocol.PostIdentityPayload{V: domain.IdentityLookupSchemaVersion, Record: record}
	if request.RequiresTargetProof() {
		label, ok := delivery.Header().Label()
		if !ok {
			return datagram.RejectDelivery(errors.New("identity lookup: request without a label"))
		}
		answer.TargetProof = protocol.SignTargetProof(h.svc.identity, h.network, label.Raw(), payload, record)
	}
	raw, err := protocol.BuildPostIdentityPayload(answer)
	if err != nil {
		return datagram.FailDelivery(fmt.Errorf("identity lookup: build answer: %w", err))
	}
	return datagram.AcceptWithAnswer(domain.DTypePostIdentity, raw)
}

// acceptRequesterTriple validates the opt-in "who is asking" triple:
// freshness inside the transport window, a verifiable signature when the
// requester's key is known, and a bounded per-label dedup so a replayed
// request cannot re-raise "you were looked up" for the window's length.
// Returns false when the WHOLE payload must be dropped.
func (h *getIdentityHandler) acceptRequesterTriple(delivery datagram.DeliveryContext, request protocol.GetIdentityPayload) bool {
	now := h.clock()
	issued := time.Unix(int64(request.RequesterIssuedAt), 0)
	if issued.After(now.Add(domain.DatagramFreshnessWindow)) || issued.Before(now.Add(-domain.DatagramFreshnessWindow)) {
		// A signed request without freshness is eternal: outside the window
		// the signature proves nothing about "now", so the payload goes.
		return false
	}

	pubKey, known := h.svc.knownPubKey(request.Requester.String())
	if !known {
		// The triple is addressed to the target's UX; without the key there
		// is nothing to verify and nothing to show. The lookup itself is
		// still answered.
		return true
	}
	label, ok := delivery.Header().Label()
	if !ok {
		return false
	}
	if err := protocol.VerifyLookupRequester(pubKey, h.network, label.Raw(), request, delivery.LocalIdentity()); err != nil {
		// An unverifiable claim of "X asked about you" is slander, not
		// metadata; the payload carries it, the payload goes.
		return false
	}

	h.mu.Lock()
	h.sweepSeenLocked(now)
	_, seen := h.seenRequesterAttempts[label.Raw()]
	if !seen {
		h.seenRequesterAttempts[label.Raw()] = now
	}
	h.mu.Unlock()
	if !seen {
		log.Info().
			Str("requester", request.Requester.String()).
			Msg("identity_lookup_requester_observed")
	}
	return true
}

// sweepSeenLocked evicts dedup entries older than the freshness window and
// bounds the map. Caller holds h.mu.
func (h *getIdentityHandler) sweepSeenLocked(now time.Time) {
	for label, at := range h.seenRequesterAttempts {
		if now.Sub(at) > domain.DatagramFreshnessWindow {
			delete(h.seenRequesterAttempts, label)
		}
	}
	if len(h.seenRequesterAttempts) < maxSeenRequesterAttempts {
		return
	}
	// Over the bound with nothing expired: drop arbitrary entries. The cost
	// of a false miss is one repeated notification line, not a security
	// property.
	for label := range h.seenRequesterAttempts {
		if len(h.seenRequesterAttempts) < maxSeenRequesterAttempts {
			break
		}
		delete(h.seenRequesterAttempts, label)
	}
}

// ---------------------------------------------------------------------------
// post_identity — the initiator ingest
// ---------------------------------------------------------------------------

// postIdentityHandler hands answers to the resolver, which owns attempt
// correlation, verification and the merge decision.
type postIdentityHandler struct {
	svc *Service
}

func (h *postIdentityHandler) Handle(_ context.Context, delivery datagram.DeliveryContext, payload []byte) datagram.HandlerResult {
	label, ok := delivery.Header().Label()
	if !ok {
		return datagram.RejectDelivery(errors.New("identity lookup: answer without a label"))
	}
	resolver := h.svc.identityResolver
	if resolver == nil {
		return datagram.RejectDelivery(errors.New("identity lookup: no resolver"))
	}
	if !resolver.HandleAnswer(label, payload) {
		// A poisoned or late answer costs this attempt only; the retry uses
		// a fresh label. No ban: the neighbour may be an honest transit of
		// somebody else's garbage.
		return datagram.RejectDelivery(errors.New("identity lookup: answer not consumed"))
	}
	return datagram.AcceptDelivery()
}

// ---------------------------------------------------------------------------
// push_identity — a session peer's own record
// ---------------------------------------------------------------------------

// pushIdentityAuthorizer enforces the session-identity rule BEFORE the
// replay key is reserved, so a refused push occupies no slot in the bounded
// cache.
//
// The rule of the design note — "accept only if the authenticated identity
// of the current session equals record.address" — is applied at the
// STRONGEST level the direction offers. Session auth is one-way (the
// initiator proves itself to the responder, docs/protocol/handshake.md), so
// on a session THIS node dialled there is no authenticated identity at all;
// refusing there (RequiresProvenPeer) would refuse the mandatory initial
// push on every dialled session — half of every exchange. Instead:
//
//   - the routed frame's VERIFIED signer (SignedSrc) must equal the address
//     inside the record — only the key owner can produce that pair, and
//     ttl=1 keeps the frame single-hop;
//   - the identity the session PRESENTS must equal it too; where the
//     direction proved the identity this is exactly the spec's check, and
//     where it is a claim the check still pins the push to the session's
//     established name, so an impersonator can neither fill the slot of a
//     third identity nor speak for one it cannot sign for.
type pushIdentityAuthorizer struct{}

func (a *pushIdentityAuthorizer) Authorize(_ context.Context, delivery datagram.DeliveryContext, payload []byte) datagram.AuthorizationDecision {
	signedSrc, ok := delivery.Header().SignedSrc()
	if !ok {
		return datagram.Reject(errors.New("push_identity: no signed source"))
	}
	push, err := protocol.ParsePushIdentityPayload(payload)
	if err != nil {
		return datagram.Reject(err)
	}
	body, err := protocol.ParseIdentityRecordBody(push.Record.Body)
	if err != nil {
		return datagram.Reject(err)
	}
	if body.Address != signedSrc {
		return datagram.Reject(errors.New("push_identity: record is not the frame signer's own"))
	}
	presented, _ := delivery.IncomingPeer().PresentedIdentity()
	if presented.IsZero() || presented != body.Address {
		return datagram.Reject(errors.New("push_identity: record does not match the session identity"))
	}
	return datagram.Accept()
}

// pushIdentityHandler verifies, rate-limits and merges an authorized push.
type pushIdentityHandler struct {
	svc     *Service
	clock   func() time.Time
	network domain.NetworkID

	mu sync.Mutex
	// lastAcceptedAt / violationAt key the receive-side rate limit by
	// (peer, session channel): the contract is per SESSION, so the initial
	// push of a fresh reconnect must never inherit the previous session's
	// budget — inheriting it would drop the mandatory initial push and
	// could close the brand-new connection for the old one's sins. Entries
	// are swept past twice the window, so dead sessions do not accumulate.
	lastAcceptedAt map[pushSessionKey]time.Time
	violationAt    map[pushSessionKey]time.Time
}

// pushSessionKey identifies one peer on one connection.
type pushSessionKey struct {
	peer domain.PeerIdentity
	conn domain.ConnID
}

func (h *pushIdentityHandler) Handle(ctx context.Context, delivery datagram.DeliveryContext, payload []byte) datagram.HandlerResult {
	signedSrc, ok := delivery.Header().SignedSrc()
	if !ok {
		return datagram.RejectDelivery(errors.New("push_identity: no signed source"))
	}
	push, err := protocol.ParsePushIdentityPayload(payload)
	if err != nil {
		return datagram.RejectDelivery(err)
	}
	body, err := protocol.VerifyIdentityRecord(push.Record, h.network, signedSrc)
	if err != nil {
		// A frame that passed the authorizer but fails full verification is
		// a validation error of the session's own making: close it — the
		// fresh handshake re-pushes a coherent record.
		h.closePushSession(ctx, delivery, "push_identity_record_invalid", err)
		return datagram.RejectDelivery(err)
	}

	sessionConn := domain.ConnID(0)
	if channel, ok := delivery.IncomingPeer().Channel(); ok {
		if conn, ok := channel.ConnID(); ok {
			sessionConn = conn
		}
	}
	if verdict := h.admitPushRate(pushSessionKey{peer: signedSrc, conn: sessionConn}); verdict != pushRateAdmit {
		if verdict == pushRateCloseSession {
			h.closePushSession(ctx, delivery, "push_identity_rate_limit", nil)
		}
		return datagram.RejectDelivery(errors.New("push_identity: rate limit"))
	}

	outcome, err := h.svc.importVerifiedIdentityRecord(h.network, push.Record, body)
	if err != nil {
		return datagram.FailDelivery(err)
	}
	switch outcome {
	case domain.IdentityRecordMergeConflict:
		// Same seq, different bytes, from the owner's OWN session: the peer
		// is obliged to bump seq, and the session is not honouring the
		// contract. Closing forces a fresh handshake with a coherent push.
		h.closePushSession(ctx, delivery, "push_identity_seq_conflict", domain.ErrIdentityRecordConflict)
		return datagram.RejectDelivery(domain.ErrIdentityRecordConflict)
	case domain.IdentityRecordMergeStale:
		// A legal reorder after a reconnect: silent no-op.
		return datagram.AcceptDelivery()
	default:
		log.Debug().
			Str("peer", signedSrc.String()).
			Str("merge", outcome.String()).
			Uint64("seq", uint64(body.Seq)).
			Msg("push_identity_merged")
		return datagram.AcceptDelivery()
	}
}

// pushRateVerdict is the three-way outcome of the receive-side limiter.
type pushRateVerdict uint8

const (
	pushRateAdmit pushRateVerdict = iota
	pushRateDrop
	pushRateCloseSession
)

// admitPushRate applies the 1-per-minute floor per (peer, session): first
// breach drops with a log, a second breach inside the window closes the
// session. Stale entries are swept in the same hold, keeping the maps
// bounded by the live-session count.
func (h *pushIdentityHandler) admitPushRate(key pushSessionKey) pushRateVerdict {
	now := h.clock()
	h.mu.Lock()
	defer h.mu.Unlock()

	for stale, at := range h.lastAcceptedAt {
		if now.Sub(at) >= 2*pushIdentityMinInterval {
			delete(h.lastAcceptedAt, stale)
		}
	}
	for stale, at := range h.violationAt {
		if now.Sub(at) >= 2*pushIdentityMinInterval {
			delete(h.violationAt, stale)
		}
	}

	last, seen := h.lastAcceptedAt[key]
	if !seen || now.Sub(last) >= pushIdentityMinInterval {
		h.lastAcceptedAt[key] = now
		delete(h.violationAt, key)
		return pushRateAdmit
	}
	if firstViolation, violated := h.violationAt[key]; violated && now.Sub(firstViolation) < pushIdentityMinInterval {
		delete(h.violationAt, key)
		log.Warn().Str("peer", key.peer.String()).Uint64("conn_id", uint64(key.conn)).Msg("push_identity_rate_limit_repeat")
		return pushRateCloseSession
	}
	h.violationAt[key] = now
	log.Warn().Str("peer", key.peer.String()).Uint64("conn_id", uint64(key.conn)).Msg("push_identity_rate_limit")
	return pushRateDrop
}

// closePushSession tears down the connection the push arrived on.
func (h *pushIdentityHandler) closePushSession(ctx context.Context, delivery datagram.DeliveryContext, reason string, cause error) {
	channel, ok := delivery.IncomingPeer().Channel()
	if !ok {
		return
	}
	connID, ok := channel.ConnID()
	if !ok {
		return
	}
	log.Warn().
		Err(cause).
		Uint64("conn_id", uint64(connID)).
		Str("reason", reason).
		Msg("push_identity_session_closed")
	_ = h.svc.Network().Close(ctx, connID)
}

// ---------------------------------------------------------------------------
// The initial push
// ---------------------------------------------------------------------------

// sendInitialIdentityPush sends this node's own record to a freshly
// authenticated session peer: routed mode, ttl = 1, mandatory auth.
//
// A peer that does not carry the plane or the type is skipped silently —
// v27 peers never see the datagram, their only path to fresh keys is a new
// hello/welcome. For a capable peer the push's reliability IS the session:
// there is no ack and no timer, so a failed enqueue closes the session and
// the reconnect handshake redistributes the record.
func (s *Service) sendInitialIdentityPush(ctx context.Context, peer domain.PeerIdentity, closeSession func()) {
	layer := s.datagramLayer()
	if layer == nil || peer.IsZero() {
		return
	}
	record, _ := s.SelfIdentityRecord()
	if record.Version == 0 {
		return
	}
	payload, err := protocol.BuildPushIdentityPayload(protocol.PushIdentityPayload{
		V:      domain.IdentityLookupSchemaVersion,
		Record: record,
	})
	if err != nil {
		log.Error().Err(err).Msg("push_identity_payload_build_failed")
		return
	}

	frame := protocol.DatagramFrame{
		Version:     domain.DatagramHeaderVersion,
		Mode:        domain.DatagramModeRouted,
		Class:       domain.DatagramClassControl,
		Src:         domain.PeerIdentityFromWire(s.identity.Address),
		Dst:         peer,
		TTL:         1,
		RoutePolicy: domain.RoutePolicyBest,
		DType:       domain.DTypePushIdentity,
	}
	salt := make([]byte, domain.DatagramSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		log.Error().Err(err).Msg("push_identity_salt_entropy_failed")
		return
	}
	frame.Payload = payload
	frame.Auth = &protocol.DatagramAuth{
		AuthVersion: domain.AuthVersionBase,
		PubKey:      append([]byte(nil), s.identity.PublicKey...),
		Salt:        salt,
		MaxTTL:      1,
		Time:        time.Now().UTC().Unix(),
	}
	signed, err := protocol.SignDatagram(frame, layer.network, s.identity.PrivateKey)
	if err != nil {
		log.Error().Err(err).Msg("push_identity_sign_failed")
		return
	}

	// The push races the tail of the handshake side effects: the
	// identity↔session binding and the hello route land moments after
	// auth_ok, and a send before them answers no_route. That state clears
	// on its own, so no_route is retried briefly; every other outcome is
	// final for this session.
	for attempt := 0; attempt < initialPushNoRouteRetries; attempt++ {
		outcome := layer.pipeline.SendLocal(ctx, datagram.LocalSendOpts{Frame: signed, Avoid: datagram.NoAvoidedNextHop()})
		switch outcome.Kind() {
		case datagram.SendQueued:
			log.Debug().Str("peer", peer.String()).Msg("push_identity_sent")
			return
		case datagram.SendNoRoute:
			select {
			case <-ctx.Done():
				return
			case <-time.After(initialPushNoRouteRetryDelay):
			}
		case datagram.SendRejected:
			// The peer is off the plane or off the type — the capability set
			// is fixed for the session's lifetime, so this is the silent
			// skip of a mixed network, not a fault. The fallback for such
			// peers is the legacy contact plane.
			reason := ""
			if rejection, ok := outcome.Rejection(); ok {
				reason = rejection.String()
			}
			log.Debug().
				Str("peer", peer.String()).
				Str("reason", reason).
				Msg("push_identity_skipped")
			return
		default:
			// A local enqueue fault on a capable session: close it, the
			// fresh handshake carries the record.
			log.Warn().Err(outcome.Err()).Str("peer", peer.String()).Msg("push_identity_enqueue_failed_closing_session")
			if closeSession != nil {
				closeSession()
			}
			return
		}
	}
	log.Debug().Str("peer", peer.String()).Str("reason", "no_route").Msg("push_identity_skipped")
}

// knownPubKey reads one identity's learned signing key from the knowledge
// domain.
func (s *Service) knownPubKey(address string) (string, bool) {
	s.knowledgeMu.RLock()
	defer s.knowledgeMu.RUnlock()
	key, ok := s.pubKeys[address]
	return key, ok && key != ""
}

// knownBoxKey reads one identity's learned box key from the knowledge
// domain — the usable-axis source of the identity resolver.
func (s *Service) knownBoxKey(address string) (string, bool) {
	s.knowledgeMu.RLock()
	defer s.knowledgeMu.RUnlock()
	key, ok := s.boxKeys[address]
	return key, ok && key != ""
}

// ---------------------------------------------------------------------------
// Local RPC (§4.9)
// ---------------------------------------------------------------------------

// resolveIdentityFrame serves the local resolve_identity RPC: start (or
// join) the lookup of frame.Address and answer the state immediately —
// the RPC is synchronous and must never block on the network; progress
// arrives via TopicIdentityResolutionChanged and resolve_identity_status.
func (s *Service) resolveIdentityFrame(frame protocol.Frame) protocol.Frame {
	target, err := domain.ParsePeerIdentity(strings.TrimSpace(frame.Address))
	if err != nil || target.IsZero() {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "resolve_identity: address must be 40-hex"}
	}
	if s.identityResolver == nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "resolve_identity: resolver not running"}
	}
	reason := identityIntentReason{Type: identityIntentReasonUIChat}
	if frame.ResolutionReason == string(identityIntentReasonRecovery) {
		// Recovery lookups carry the message id as the durable reason id, so
		// the refcount distinguishes per-message causes.
		reason = identityIntentReason{Type: identityIntentReasonRecovery, ID: strings.TrimSpace(frame.ID)}
	}
	state, err := s.identityResolver.StartResolution(target, reason)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: err.Error()}
	}
	// The watermark is read AFTER StartResolution: every attempt sent
	// before this reply has generation ≤ watermark, so a caller anchoring
	// on it can never mistake a pre-existing question for its own.
	return protocol.Frame{
		Type:       "identity_resolution",
		Resolution: identityResolutionFrame(state, s.identityResolver.CurrentAttemptGen()),
	}
}

// CancelRecoveryResolutionReasons drops every recovery-typed lookup reason
// of the target — called by the DM recovery subsystem when a job closes or
// is evicted, so a jobless peer's background lookup stops consuming
// attempts. Reasons of other types (ui_chat, pending_send) keep the
// resolution alive under the ordinary refcount.
func (s *Service) CancelRecoveryResolutionReasons(target domain.PeerIdentity) {
	if s.identityResolver == nil || target.IsZero() {
		return
	}
	s.identityResolver.CancelReasonType(target, identityIntentReasonRecovery)
}

// resolveIdentityStatusFrame serves resolve_identity_status — the UI's
// insurance against a lost event: the node retains the last state per
// resolution (terminals for 60 s).
func (s *Service) resolveIdentityStatusFrame(frame protocol.Frame) protocol.Frame {
	if s.identityResolver == nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "resolve_identity_status: resolver not running"}
	}
	state, ok := s.identityResolver.StateByID(strings.TrimSpace(frame.ResolutionID))
	if !ok {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "resolve_identity_status: unknown resolution id"}
	}
	return protocol.Frame{
		Type:       "identity_resolution",
		Resolution: identityResolutionFrame(state, s.identityResolver.CurrentAttemptGen()),
	}
}

// identityResolutionFrame projects the event payload onto the RPC frame.
// watermark is the resolver's attempt counter at reply time — the anchor a
// recovery records before relying on a future completion.
func identityResolutionFrame(state ebus.IdentityResolutionState, watermark uint64) *protocol.IdentityResolutionFrame {
	return &protocol.IdentityResolutionFrame{
		ResolutionID:        state.ResolutionID,
		Target:              state.Target.String(),
		Lifecycle:           string(state.Lifecycle),
		Authority:           string(state.Authority),
		DMAvailable:         string(state.DMAvailable),
		Usable:              state.Usable,
		InteractiveTimeout:  state.InteractiveTimeout,
		NoRoute:             state.NoRoute,
		AnswerAttemptGen:    state.AnswerAttemptGen,
		AttemptGenWatermark: watermark,
	}
}
