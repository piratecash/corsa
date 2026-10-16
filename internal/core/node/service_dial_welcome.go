package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// The welcome gate shared by every outbound dial that reads a welcome: the
// peer sessions (openPeerSession, openPeerSessionForCM) and the one-shot
// service dials that never become sessions (the syncPeer recovery dial and the
// push_notice fallback in sendNoticeToPeer).
//
// The one-shot dials send commands — get_peers, fetch_contacts, push_notice —
// that a responder serves only on an authenticated connection, so they need
// the same gate as a session, not a lighter one: every responder at or above
// MinimumProtocolVersion answers a hello carrying identity fields with a
// challenge (connauth.PrepareAuth either issues one or the hello is refused)
// and serves nothing but handshake commands until auth_session verifies. A
// welcome below the floor or without a challenge therefore names a responder
// this node does not serve, and the dial ends before anything but the hello
// has been written.

// serviceDial names a one-shot service dial; its log events are
// "<dial>_<outcome>", so both dials report one outcome under one name shape
// and at one level.
type serviceDial string

const (
	serviceDialSyncPeer serviceDial = "sync_peer"
	serviceDialNotice   serviceDial = "send_notice"
)

func (d serviceDial) event(outcome string) string {
	return string(d) + "_" + outcome
}

// ioFailure is the log level of a socket failure on the dial. The notice dial
// is a best-effort gossip fan-out with no retry behind it, and its transport
// failures (refused, reset, timed out) are routine noise: Debug. The recovery
// dial is scheduled for a specific missing key, and its failure is what an
// operator needs to see: Warn. Refusals by the gate or the responder are Warn
// on both.
func (d serviceDial) ioFailure() *zerolog.Event {
	if d == serviceDialNotice {
		return log.Debug()
	}
	return log.Warn()
}

// errNotAWelcome is a v1 answer to hello that is neither a welcome, an error
// nor a connection_notice.
var errNotAWelcome = errors.New("answer to hello is not a welcome")

// errServiceDialNotice is a responder that answered hello with a
// connection_notice: it is closing, and the notice has been recorded.
var errServiceDialNotice = errors.New("hello answered with a connection_notice")

// admitServiceDialWelcome is the single gate of a one-shot service dial, run
// on the welcome of either session kind (proven: the v2 listener's intro) in
// this order:
//
//  1. a connection_notice is recorded (ban window, self-identity cooldown);
//  2. anything but a welcome is refused — an error frame maps to its sentinel;
//  3. v1 only: a welcome naming an identity that proved v2 is refused, since a
//     man in the middle may have stripped TLS to collect a relayable v1 proof;
//  4. a responder below MinimumProtocolVersion is refused and penalised
//     (refuseIncompatibleWelcome) — the v2 handshake proves who the listener
//     is, not that this node serves its version;
//  5. our own identity is refused with the self-identity cooldown.
//
// The v1 challenge is not checked here: authenticateServiceDial, which a v1
// dial runs right after this gate, starts with authSessionFor — the one check
// the peer sessions share — and refuses a welcome without a challenge
// (protocol.ErrAuthRequired) before anything is signed or written.
//
// Nothing has been signed or sent past the hello when it refuses, and nothing
// of the responder has been recorded as an outbound success. Each refusal is
// logged here, once; the caller only ends the dial.
//
// Takes peerMu and the domains nested under it; the caller holds no domain
// mutex.
func (s *Service) admitServiceDialWelcome(dial serviceDial, address domain.PeerAddress, welcome protocol.Frame, proven bool) error {
	if welcome.Type == protocol.FrameTypeConnectionNotice {
		s.recordServiceDialNotice(dial, address, welcome)
		return errServiceDialNotice
	}
	if err := refuseNonWelcome(welcome); err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Str("code", welcome.Code).Msg(dial.event("welcome_refused"))
		return err
	}
	if !proven {
		if err := s.refuseLegacyWelcome(address, domain.PeerIdentityFromWire(welcome.Address)); err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg(dial.event("v1_refused_identity_pinned_to_v2"))
			return err
		}
	}
	if err := s.refuseIncompatibleWelcome(address, welcome, versionEvidenceOf(proven)); err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Int("version", welcome.Version).Msg(dial.event("protocol_too_old"))
		return err
	}
	if s.isSelfIdentity(domain.PeerIdentityFromWire(welcome.Address)) {
		// The one-shot dials do not run through the connection-manager
		// failure hook, so the cooldown is applied here: without it the next
		// recovery pass or gossip tick would redial the same self-looping
		// alias at once.
		selfErr := s.newSelfIdentityError(address)
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Msg(dial.event("self_identity_rejected"))
		s.applySelfIdentityCooldown(address, selfErr)
		return selfErr
	}
	return nil
}

// recordServiceDialNotice records the connection_notice a responder sent in
// place of a welcome. A peer-banned notice with reason self-identity is our
// own inbound hello handler answering a dial that reached us through an alias
// (NAT hairpin, peer-exchange mirror, alternate port): handleConnectionNotice
// alone would only write the responder's (possibly empty) ban to
// persistedMeta, while the dial gates read health — so the self-identity
// cooldown is applied as on the managed paths.
func (s *Service) recordServiceDialNotice(dial serviceDial, address domain.PeerAddress, notice protocol.Frame) {
	s.handleConnectionNotice(address, notice)
	log.Info().Str("peer", string(address)).Str("code", notice.Code).Msg(dial.event("connection_notice"))
	if errors.Is(protocol.NoticeErrorFromFrame(notice), protocol.ErrSelfIdentity) {
		s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address))
	}
}

// refuseNonWelcome ends a v1 dial whose answer to hello is not a welcome. An
// error frame maps to its protocol sentinel. The connection_notice is handled
// BEFORE this: it carries a ban window or a self-identity verdict that has to
// be recorded, not just refused.
func refuseNonWelcome(answer protocol.Frame) error {
	switch answer.Type {
	case "welcome":
		return nil
	case "error":
		return fmt.Errorf("hello refused: %w", protocol.ErrorFromCode(answer.Code))
	default:
		return fmt.Errorf("%w: %q", errNotAWelcome, answer.Type)
	}
}

// refuseIncompatibleWelcome ends a dial whose responder is below
// MinimumProtocolVersion and reacts exactly as the session path does
// (reactToIncompatibleDial): the evidence is the same welcome, so the health
// record must not depend on which dial happened to read it. evidence says
// what stands behind the welcome — the intro of a proven v2 session is
// penalised, a v1 welcome before auth_ok is a claim and only paces the
// address. Inside a running incompatible-version ban the charge does not
// accumulate (penalizeOldProtocolPeer), so a dial that keeps meeting the peer
// cannot slide the ban.
//
// Callers on the connection manager's dial worker must NOT use it: there the
// reaction runs once in onCMDialFailed from incompatibleProtocolError.
//
// Takes peerMu (and the domains nested under it), so the caller holds no
// domain mutex.
func (s *Service) refuseIncompatibleWelcome(address domain.PeerAddress, welcome protocol.Frame, evidence versionEvidence) error {
	err := validateProtocolHandshake(welcome)
	if err == nil {
		return nil
	}
	refusal := incompatibleVersionOf(welcome, evidence, err)
	s.reactToIncompatibleDial(address, refusal)
	return refusal
}

// versionEvidenceOf is what stands behind a welcome: the intro of a proven v2
// session, or a v1 frame nothing proves.
func versionEvidenceOf(proven bool) versionEvidence {
	if proven {
		return versionProven
	}
	return versionClaimed
}

// welcomeChallenge is the challenge of a v1 welcome. A welcome without one is
// refused with protocol.ErrAuthRequired: a responder that does not challenge
// would serve nothing past the handshake, and continuing would only send it
// commands it must refuse.
func welcomeChallenge(welcome protocol.Frame) (string, error) {
	if strings.TrimSpace(welcome.Challenge) == "" {
		return "", protocol.ErrAuthRequired
	}
	return welcome.Challenge, nil
}

// authSessionFor is this node's auth_session answer to a v1 welcome, refused
// before anything is signed when the welcome carries no challenge.
func (s *Service) authSessionFor(welcome protocol.Frame) (protocol.Frame, error) {
	challenge, err := welcomeChallenge(welcome)
	if err != nil {
		return protocol.Frame{}, err
	}
	return protocol.Frame{
		Type:      "auth_session",
		Address:   s.identity.Address,
		Signature: identity.SignPayload(s.identity, protocol.SessionAuthPayload(challenge, s.identity.Address)),
	}, nil
}

// authenticateServiceDial proves this node to a v1 responder admitted by
// admitServiceDialWelcome: auth_session over the welcome's challenge, written
// by writeLine (each dial owns its socket differently), then the wait for
// auth_ok. A welcome without a challenge is refused first, by authSessionFor,
// with nothing signed or written (welcome_without_challenge).
//
// After auth_session the responder may interleave non-auth frames BEFORE the
// auth_ok reply: handleAuthSession runs trackInboundConnect (which can flush up
// to a full pending ring of fire-and-forget frames — default
// maxPendingFramesPerPeer, operator-raisable) plus route-sync/announce output
// before the caller even enqueues auth_ok. Taking the first line for the
// verdict reads that flood as a refusal, so the wait uses the drain-tolerant
// filtered reader of the recovery replies (readSyncReply: skip budget
// syncReplySkipBudgetFor, wall cap syncReplyDrainCap, ctx-clamped sliding idle
// deadline). A read or parse failure that exhausts it is auth_read_failed, an
// error frame from the responder is auth_rejected.
//
// Any failure ends the dial and is logged once under its own event. Reports
// whether auth_ok arrived.
func (s *Service) authenticateServiceDial(ctx context.Context, dial serviceDial, address domain.PeerAddress, welcome protocol.Frame, writeLine func(string) error, reader *bufio.Reader, refreshDeadline func()) bool {
	authSession, err := s.authSessionFor(welcome)
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg(dial.event("welcome_without_challenge"))
		return false
	}
	line, err := protocol.MarshalFrameLine(authSession)
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg(dial.event("auth_marshal_failed"))
		return false
	}
	if err := writeLine(line); err != nil {
		dial.ioFailure().Err(err).Str("peer", string(address)).Msg(dial.event("auth_write_failed"))
		return false
	}
	reply, err := readSyncReply(ctx, reader, "auth_ok", s.syncReplySkipBudgetFor(), refreshDeadline)
	if err != nil {
		dial.ioFailure().Err(err).Str("peer", string(address)).Msg(dial.event("auth_read_failed"))
		return false
	}
	if reply.Type != "auth_ok" {
		log.Warn().Str("peer", string(address)).Str("type", reply.Type).Str("code", reply.Code).Msg(dial.event("auth_rejected"))
		return false
	}
	return true
}

// serviceDialDeadline is the sliding read deadline of a one-shot dial: one
// idle interval (syncHandshakeTimeout) per read, so a responder still draining
// a flood is not cut off by the dial's initial absolute deadline — but never
// past ctx's own deadline, which stays the cancellation authority.
func serviceDialDeadline(ctx context.Context, conn interface{ SetDeadline(time.Time) error }) func() {
	return func() {
		deadline := time.Now().Add(syncHandshakeTimeout)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		// The error is dropped on purpose: SetDeadline fails only on a
		// connection that is already closed, and the read this deadline
		// guards then fails with that same error and ends the dial — the
		// one place a failure is logged. Reporting it here too would log
		// one failure twice.
		_ = conn.SetDeadline(deadline)
	}
}
