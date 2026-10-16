package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// session_dial_choice.go is how a node that speaks v2 chooses the session
// kind for one dial (docs/protocol/session_v2.md, "Choosing the session kind
// on dial"):
//
//  1. A TLS attempt first, always: skipping it is a separate mode the
//     operator turns on by hand, never a choice a dial makes. A v2 attempt
//     this node cannot start is a local failure.
//  2. What answered a failed TLS attempt decides what may follow. Another
//     identity proven, or a TLS handshake record that then failed (a bad
//     certificate, a proof that does not verify): the attempt ends, a v2 hint
//     is left and the next v2 attempt is paced — no probe. A TLS alert: a v2
//     hint; an alert that meets an alert's hint, or at an address that
//     answered the probe like an old node recently (legacy_observed), lets
//     the probe run. A v1 line,
//     a reset, an EOF, a timeout or other bytes: no evidence, no mark — the
//     probe may run.
//  3. The probe runs on a SEPARATE connection: an identity-less hello
//     (session_probe.go). A correct old-protocol answer names an identity; if
//     it requires v2 the dial is refused there, before this node's identity
//     goes on the wire. Otherwise legacy_observed is set and the transport is
//     handed out on the probe's connection, the full hello to follow on it;
//     its welcome must name what the probe's answer named
//     (refuseFullLegacyWelcome). A v2 answer closes the connection as a v2
//     hint. The plan is read again before the probe and before v1 is
//     admitted, since a parallel dial may have proven v2 meanwhile.
//
// The operator may switch TLS-first off (CORSA_V2_TLS_FIRST) as the way back
// from TLS-first that keeps v2: a build without v2 would be refused v1 by
// every peer that pinned this node. That drops the preliminary ClientHello
// and nothing else: a dial where v1 is not ruled out opens with step 3 — the
// probe. A correct old-protocol answer of a supported old version lets v1 in as
// always; a correct v2 welcome requires v2 from then on — the same dial goes
// on with a TLS attempt on a new connection, and a failure of that attempt
// leads to v1 by no path. An alert ends the dial. Everything that rules v1 out
// is untouched by the switch: a dial where v2 is required, or where the fall
// back to v1 is held, still opens with TLS, and a pinned identity in the
// probe's answer is refused there as always. The mode is not a way around a
// block of TLS to a node of v31 or later.
//
// Where v2 is required — a v2-only node, an endpoint bound to v2 in the store
// or by a proven outbound session (v2_strong), a dial "to X" — no probe runs
// and nothing leads to v1. The answers behind the hints are unproven: a man
// in the middle can forge each of them, so a hint changes where the next dial
// starts, never what it may accept; the residual risk of a downgrade at a
// first contact stays, and the contract names it.

// dialStepTimeouts bound the two steps of a dial before a session exists.
type dialStepTimeouts struct {
	// tls bounds the TLS attempt.
	tls sessionv2.Timeouts
	// probe bounds the identity-less hello and the read of its answer.
	probe legacyProbeOptions
}

// sessionDialTimeouts are the step timeouts for direct addresses and for
// overlay ones (onion, i2p, cjdns), whose first connection crosses a circuit
// that may be cold. The values are placeholders, not decisions: they are to
// be chosen from the first-connection latency measured against real old
// releases and over Tor, since a timeout set too short would cut off the
// old nodes the probe exists for.
type sessionDialTimeouts struct {
	direct  dialStepTimeouts
	overlay dialStepTimeouts
}

func defaultSessionDialTimeouts() sessionDialTimeouts {
	return sessionDialTimeouts{
		direct: dialStepTimeouts{tls: sessionv2.DefaultTimeouts, probe: defaultLegacyProbeOptions()},
		overlay: dialStepTimeouts{
			tls:   sessionv2.Timeouts{TLS: 20 * time.Second, Proof: 10 * time.Second},
			probe: legacyProbeOptions{timeout: 15 * time.Second, systemNow: time.Now},
		},
	}
}

// forAddress is the step timeouts of a dial to address.
func (t sessionDialTimeouts) forAddress(address domain.PeerAddress) dialStepTimeouts {
	if classifyAddress(address).IsOverlay() {
		return t.overlay
	}
	return t.direct
}

var (
	// errLegacyProbeAnsweredV2 is a probe answered by a node that speaks v2:
	// a v2 hint, not a reason to speak v1, and not charged to the address.
	errLegacyProbeAnsweredV2 = errors.New("secure session: the probe was answered by a node that speaks v2")
	// errLegacyProbeWelcomedV2 is the case of errLegacyProbeAnsweredV2 where
	// the answer was a correct welcome of a version that speaks v2, not an
	// alert; it always comes wrapped together with errLegacyProbeAnsweredV2.
	errLegacyProbeWelcomedV2 = errors.New("secure session: the probe was welcomed at a version that speaks v2")
	// errLegacyRefusedAfterV2Welcome is a v1 line answering the TLS attempt
	// that followed a correct v2 welcome to the probe: one of the two answers
	// was forged, so v1 is refused there, counted apart from the refusals of a
	// bound endpoint, held, and not charged.
	errLegacyRefusedAfterV2Welcome = errors.New("secure session: a v1 answer after a v2 welcome to the probe")
	// errLegacyWelcomeUnlikeProbe is a full welcome on the probe's
	// connection that names another identity or version than the probe's
	// answer did: one of the two was substituted, and the dial ends before
	// anything is signed.
	errLegacyWelcomeUnlikeProbe = errors.New("secure session: the full welcome is not the one that answered the probe")
	// errV2DialLocalFailure is a v2 attempt this node could not start — its
	// own identity or arguments unusable — before a byte reached the wire.
	errV2DialLocalFailure = errors.New("secure session: the v2 attempt failed locally")
	// errPinRoomAwaited is a dial refused before any socket because the last
	// v2 session to the address was refused for a full pin store that still
	// has no room.
	errPinRoomAwaited = errors.New("secure session: dial held until the pin store has room")
)

// sessionDialPlan is what the mode, the intent, the store and the marks say
// about the next dial to one address.
type sessionDialPlan struct {
	// v2Only: this node speaks v2 alone.
	v2Only bool
	// required: only v2 may be used — a v2-only node, a dial "to X", an
	// endpoint bound to v2 in the store or by a proven outbound session.
	required bool
	// legacyObserved: a probe there got a correct old-protocol answer
	// recently.
	legacyObserved bool
	// held: a v1 answer from the address was refused and its hold runs.
	held bool
	// tlsFirstDisabled: the operator switched TLS-first off
	// (CORSA_V2_TLS_FIRST).
	tlsFirstDisabled bool
	// v2Welcomed: this attempt follows a correct v2 welcome to the probe of
	// the same dial, so it requires v2: the endpoint has shown it speaks v2,
	// and a v1 answer from it now can only be a downgrade.
	v2Welcomed bool
}

func (s *Service) planSessionDial(mode sessionv2.Mode, address domain.PeerAddress, intent domain.DialIntent) sessionDialPlan {
	_, named := intent.Target()
	marks := s.secureSessions.marks
	v2Only := mode == sessionv2.ModeV2Only
	return sessionDialPlan{
		v2Only:           v2Only,
		required:         v2Only || named || marks.v2Strong(address) || s.secureSessions.store.endpointRequiresV2(address),
		legacyObserved:   marks.legacyObserved(address),
		held:             marks.legacyHeld(address),
		tlsFirstDisabled: s.secureSessions.tlsFirstDisabled,
	}
}

// mayProbe: the probe may run at all — v1 is not ruled out at the address.
func (p sessionDialPlan) mayProbe() bool { return !p.required && !p.held && !p.v2Welcomed }

// opensWithProbe: TLS-first is switched off and v1 is not ruled out, so the
// dial skips the TLS attempt and opens with the probe.
func (p sessionDialPlan) opensWithProbe() bool { return p.tlsFirstDisabled && p.mayProbe() }

// probeAfterAlert: a TLS alert lets the probe follow when it met an alert's
// hint (repeated), or when the address answered the probe like an old node
// recently: a forged alert in front of an old node then costs that node one
// attempt, not two. legacy_observed never skips the TLS attempt itself — that
// is the operator's separate mode, never a dial's own choice.
func (p sessionDialPlan) probeAfterAlert(repeated bool) bool {
	return p.mayProbe() && (repeated || p.legacyObserved)
}

// failedV2Attempt is a TLS attempt that ended without a session.
type failedV2Attempt struct {
	address domain.PeerAddress
	intent  domain.DialIntent
	connID  domain.ConnID
	plan    sessionDialPlan
	err     error
}

// v2AttemptAnswer is what answered a failed TLS attempt, by its first byte
// or by the identity it proved.
type v2AttemptAnswer int

const (
	// v2AnsweredByNothing: silence, a reset, an EOF, a timeout or bytes that
	// are neither TLS nor v1 — no evidence either way.
	v2AnsweredByNothing v2AttemptAnswer = iota + 1
	// v2AnsweredByV1Line: a JSON line, what an old node answers a
	// ClientHello with — and what a man in the middle answers it with.
	v2AnsweredByV1Line
	// v2AnsweredByAlert: a TLS alert, the listener's refusal included.
	v2AnsweredByAlert
	// v2AnsweredByTLS: a TLS handshake record, then a failure.
	v2AnsweredByTLS
	// v2AnsweredByOtherOwner: another identity than the dial's X proved.
	v2AnsweredByOtherOwner
	// v2NotAttempted: this node could not start the attempt — its own
	// identity or arguments unusable — and nothing reached the wire.
	v2NotAttempted
)

func classifyV2Attempt(err error) v2AttemptAnswer {
	switch {
	case errors.Is(err, sessionv2.ErrInvalidArgument):
		return v2NotAttempted
	case errors.Is(err, sessionv2.ErrPeerMismatch):
		return v2AnsweredByOtherOwner
	case errors.Is(err, sessionv2.ErrPeerSpeaksTLS):
		return v2AnsweredByTLS
	case errors.Is(err, sessionv2.ErrPeerAlerted):
		return v2AnsweredByAlert
	case errors.Is(err, sessionv2.ErrPeerSpeaksV1):
		return v2AnsweredByV1Line
	default:
		return v2AnsweredByNothing
	}
}

// afterFailedV2Attempt decides what follows a failed TLS attempt. A dial
// this node aborted itself (ctx) learns nothing about the address, and one it
// could not start says nothing about the peer at all.
func (s *Service) afterFailedV2Attempt(ctx context.Context, attempt failedV2Attempt, dial transportDial) (*dialledTransport, error) {
	answer := classifyV2Attempt(attempt.err)
	switch answer {
	case v2NotAttempted:
		log.Error().Err(attempt.err).Str("peer", string(attempt.address)).Msg("secure_session_v2_attempt_failed_locally")
		return nil, fmt.Errorf("%w: %w", errV2DialLocalFailure, attempt.err)
	case v2AnsweredByOtherOwner:
		return nil, s.noteEndpointOwnerMismatch(attempt.address, attempt.intent, attempt.connID, attempt.err)
	}
	if ctx.Err() != nil {
		return nil, attempt.err
	}
	switch answer {
	case v2AnsweredByTLS:
		return nil, s.noteV2FailedAfterTLS(attempt)
	case v2AnsweredByAlert:
		return s.afterV2Alert(ctx, attempt, dial)
	case v2AnsweredByV1Line:
		return s.afterV1Line(ctx, attempt, dial)
	default:
		return s.afterUnansweredV2(ctx, attempt, dial)
	}
}

// noteV2FailedAfterTLS: the listener speaks TLS, and the handshake with it
// failed — a certificate, an identity, a proof or its binding to the TLS
// session. That ends the attempt without fall back: a v2 hint, and the next
// v2 attempt is paced. The failure itself is the address's, as before.
func (s *Service) noteV2FailedAfterTLS(attempt failedV2Attempt) error {
	marks := s.secureSessions.marks
	marks.noteV2Hint(attempt.address)
	retry := marks.noteV2Unanswered(attempt.address)
	log.Debug().Err(attempt.err).Str("peer", string(attempt.address)).Dur("v2_retry", retry).Msg("secure_session_v2_failed_after_tls")
	return attempt.err
}

// afterV2Alert: a TLS alert is a v2 hint. The listener's own refusal also
// paces the next attempt. The probe follows only as probeAfterAlert allows,
// so that alerts forged in front of an old node do not keep it off v1 for
// good; an honest listener answers the probe with an alert or a v2 welcome,
// which the probe reads as a v2 hint.
func (s *Service) afterV2Alert(ctx context.Context, attempt failedV2Attempt, dial transportDial) (*dialledTransport, error) {
	repeated := s.secureSessions.marks.noteV2Alert(attempt.address)
	err := attempt.err
	if errors.Is(err, sessionv2.ErrRefusedByListener) {
		err = s.noteV2RefusedByListener(attempt.address, err)
	}
	if !attempt.plan.probeAfterAlert(repeated) {
		return nil, err
	}
	return s.probeAfterV2Attempt(ctx, attempt, dial)
}

// afterV1Line: a v1 line is no proof of an old node — a man in the middle
// writes the same line. Where v2 is required it is a downgrade, refused;
// where the fall back is held it is held; otherwise the probe decides.
func (s *Service) afterV1Line(ctx context.Context, attempt failedV2Attempt, dial transportDial) (*dialledTransport, error) {
	switch {
	case attempt.plan.v2Only:
		return nil, fmt.Errorf("%w: %w", errV1RefusedV2Only, attempt.err)
	case attempt.plan.required:
		return nil, s.refuseDowngrade(attempt)
	case attempt.plan.v2Welcomed:
		return nil, s.refuseV1AfterV2Welcome(attempt)
	case attempt.plan.held:
		// No legacy_observed: the refusal that started the hold forgot it,
		// because the v1 answers here may be forged.
		return nil, s.holdLegacyFallback(attempt.address, attempt.err)
	default:
		return s.probeAfterV2Attempt(ctx, attempt, dial)
	}
}

// afterUnansweredV2: nothing answered the attempt that says anything; the
// probe decides where v1 is not ruled out.
func (s *Service) afterUnansweredV2(ctx context.Context, attempt failedV2Attempt, dial transportDial) (*dialledTransport, error) {
	if !attempt.plan.mayProbe() {
		return nil, attempt.err
	}
	return s.probeAfterV2Attempt(ctx, attempt, dial)
}

// refuseDowngrade refuses a v1 line where v2 is required: counted, the fall
// back held, not charged.
func (s *Service) refuseDowngrade(attempt failedV2Attempt) error {
	s.secureSessions.store.noteEndpointRefusal()
	hold := s.secureSessions.marks.noteLegacyRefused(attempt.address)
	log.Warn().Str("peer", string(attempt.address)).Dur("legacy_hold", hold).Msg("secure_session_downgrade_refused")
	return fmt.Errorf("%w: %w", errLegacyRefusedV2Bound, attempt.err)
}

// refuseV1AfterV2Welcome refuses a v1 line on the TLS attempt that followed a
// correct v2 welcome to the probe: one of the two answers was forged. Counted
// on its own, the fall back held — so the next dial opens with TLS — not
// charged.
func (s *Service) refuseV1AfterV2Welcome(attempt failedV2Attempt) error {
	s.secureSessions.v1RefusalsAfterV2Welcome.Add(1)
	hold := s.secureSessions.marks.noteLegacyRefused(attempt.address)
	log.Warn().Str("peer", string(attempt.address)).Uint64("conn_id", uint64(attempt.connID)).Dur("legacy_hold", hold).Msg("secure_session_v1_refused_after_v2_welcome")
	return fmt.Errorf("%w: %w", errLegacyRefusedAfterV2Welcome, attempt.err)
}

func (s *Service) probeAfterV2Attempt(ctx context.Context, attempt failedV2Attempt, dial transportDial) (*dialledTransport, error) {
	log.Debug().Err(attempt.err).Str("peer", string(attempt.address)).Msg("secure_session_probe_after_failed_v2_attempt")
	return s.dialLegacyByProbe(ctx, attempt.address, attempt.intent, dial)
}

// refuseLegacyNowRuledOut reads the plan again and refuses v1 if it has been
// ruled out since the dial began. The TLS attempt and the probe each take
// seconds, and meanwhile a parallel dial may have proven v2 at the address,
// bound it, or a refusal may have started its hold; a plan read at the start
// would let v1 through on stale facts. The refusal says nothing new about
// the address: not counted, not charged.
func (s *Service) refuseLegacyNowRuledOut(address domain.PeerAddress, intent domain.DialIntent) error {
	plan := s.planSessionDial(s.sessionMode(), address, intent)
	switch {
	case plan.required:
		log.Debug().Str("peer", string(address)).Msg("secure_session_v1_ruled_out_during_dial")
		return fmt.Errorf("%w: %s", errLegacyRefusedV2Bound, address)
	case plan.held:
		log.Debug().Str("peer", string(address)).Msg("secure_session_v1_held_during_dial")
		return fmt.Errorf("%w: %s", errLegacyRefusedBackoff, address)
	default:
		return nil
	}
}

// dialLegacyByProbe opens a connection, probes it, and hands it out for v1
// only when the probe allowed v1, v1 is still not ruled out, and the identity
// its answer named passed the pin. Every other outcome closes the connection.
func (s *Service) dialLegacyByProbe(ctx context.Context, address domain.PeerAddress, intent domain.DialIntent, dial transportDial) (*dialledTransport, error) {
	if err := s.refuseLegacyNowRuledOut(address, intent); err != nil {
		return nil, err
	}
	transport, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	answer, err := probeLegacy(ctx, transport.metered, s.secureSessions.dialTimeouts.forAddress(address).probe)
	switch answer.outcome {
	case legacyProbeOutcomeLegacyAllowed:
		return s.admitProbedLegacy(address, intent, transport, answer)
	case legacyProbeOutcomeV2Hint:
		_ = transport.metered.Close()
		return nil, s.noteProbeAnsweredV2(address, answer.reply != nil)
	default:
		_ = transport.metered.Close()
		return nil, s.noteProbeAllowedNothing(address, answer.reply, err)
	}
}

// admitProbedLegacy reads the plan again and checks the identity the probe's
// answer named against the pin BEFORE anything of this node's identity is
// written: a refusal ends the dial on the probe. An admitted one sets
// legacy_observed and leaves the connection, its reader and what the answer
// named to the caller.
func (s *Service) admitProbedLegacy(address domain.PeerAddress, intent domain.DialIntent, transport *dialledTransport, answer legacyProbeAnswer) (*dialledTransport, error) {
	if err := s.refuseLegacyNowRuledOut(address, intent); err != nil {
		_ = transport.metered.Close()
		return nil, err
	}
	claimed := domain.PeerIdentityFromWire(answer.reply.Address)
	if err := s.refuseLegacyWelcome(address, claimed); err != nil {
		_ = transport.metered.Close()
		log.Warn().Err(err).Str("peer", string(address)).Msg("secure_session_probe_refused_identity_pinned_to_v2")
		return nil, err
	}
	s.secureSessions.marks.noteLegacyObserved(address)
	transport.reader = answer.reader
	transport.probed = &legacyProbeWelcome{identity: claimed, version: domain.ProtocolVersion(answer.reply.Version)}
	log.Info().Str("peer", string(address)).Int("version", answer.reply.Version).Msg("secure_session_peer_speaks_v1")
	return transport, nil
}

// noteProbeAnsweredV2: the node at address speaks v2. A v2 hint and nothing
// charged. welcomed is a correct v2 welcome: the next v2 attempt is paced (one
// step per attempt) and the error also matches errLegacyProbeWelcomedV2.
// Otherwise the answer was an alert, which is read as the listener's refusal
// of a TLS attempt is (noteV2RefusedByListener): paced with a jitter.
func (s *Service) noteProbeAnsweredV2(address domain.PeerAddress, welcomed bool) error {
	marks := s.secureSessions.marks
	marks.noteV2Hint(address)
	answered := fmt.Errorf("%w: %s", errLegacyProbeAnsweredV2, address)
	if !welcomed {
		return s.noteV2RefusedByListener(address, answered)
	}
	retry := marks.noteV2Unanswered(address)
	log.Debug().Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_probe_welcomed_by_v2")
	return fmt.Errorf("%w: %w", errLegacyProbeWelcomedV2, answered)
}

// dialOpenedByProbe is a dial with TLS-first switched off: the probe
// first; on a correct v2 welcome to it, a TLS attempt in the same dial that
// requires v2 — its failure ends the dial without a second probe or v1, since
// the endpoint has shown it speaks v2. Any other outcome of the probe is the dial's.
func (s *Service) dialOpenedByProbe(ctx context.Context, address domain.PeerAddress, intent domain.DialIntent, plan sessionDialPlan, dial transportDial) (*dialledTransport, error) {
	s.secureSessions.dialsOpenedByProbe.Add(1)
	transport, err := s.dialLegacyByProbe(ctx, address, intent, dial)
	if !errors.Is(err, errLegacyProbeWelcomedV2) || ctx.Err() != nil {
		return transport, err
	}
	plan.v2Welcomed = true
	return s.dialV2(ctx, address, intent, plan, dial)
}

// noteProbeAllowedNothing: the probe's answer permits nothing. A probe this
// node aborted itself says nothing about the address and changes nothing.
// Otherwise legacy_observed is forgotten; a probe nobody answered in time is
// paced, not charged; an answer below the version floor returns the claimed
// incompatible version, which the dial paths answer with pacing alone
// (reactToIncompatibleDial).
func (s *Service) noteProbeAllowedNothing(address domain.PeerAddress, reply *protocol.Frame, probeErr error) error {
	if errors.Is(probeErr, errLegacyProbeAborted) {
		return probeErr
	}
	s.secureSessions.marks.forgetLegacyObserved(address)
	switch {
	case errors.Is(probeErr, errLegacyProbeTimeout), s.probeClosedWithTLSFirstOff(probeErr):
		retry := s.secureSessions.marks.noteV2Unanswered(address)
		log.Debug().Err(probeErr).Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_probe_unanswered")
		return probeErr
	case reply != nil && errors.Is(probeErr, errLegacyProbeVersionTooOld):
		return incompatibleVersionOf(*reply, versionClaimed, probeErr)
	default:
		return probeErr
	}
}

// probeClosedWithTLSFirstOff: with TLS-first switched off, the far end closed
// the probe's connection without an answer — what a node serving TLS alone
// does to a connection that is not TLS. Nothing follows in that dial
// (session_v2.md: a known limit until S7), so the next dial is paced like one
// after a probe nobody answered in time, not left to the connection manager's
// own rate. With TLS-first on, the TLS attempt before the probe already paced
// what it learned.
func (s *Service) probeClosedWithTLSFirstOff(probeErr error) bool {
	return s.tlsFirstDisabled() && errors.Is(probeErr, errLegacyProbeNoAnswer)
}

// legacyProbeWelcome is what the probe's answer named on one connection.
type legacyProbeWelcome struct {
	identity domain.PeerIdentity
	version  domain.ProtocolVersion
}

// admits reports whether frame may follow the probe's answer on its
// connection: anything that is not a welcome is judged by the gates that
// follow, and a welcome must name the same identity and version. A nil
// receiver — no probe ran — admits everything.
func (w *legacyProbeWelcome) admits(frame protocol.Frame) bool {
	if w == nil || frame.Type != "welcome" {
		return true
	}
	return domain.PeerIdentityFromWire(frame.Address) == w.identity && domain.ProtocolVersion(frame.Version) == w.version
}

// refuseFullLegacyWelcome checks the full welcome on a connection the probe
// opened, before any gate reads it and before anything is signed: v1 must
// still not be ruled out — a parallel dial may have proven v2 at the address
// or bound it since the probe admitted v1 — and the welcome must name what
// the probe's answer on the same connection named; a substituted one also
// forgets legacy_observed. Every v1 dial path calls it on the welcome it
// read; a connection no probe opened is not judged here.
func (s *Service) refuseFullLegacyWelcome(address domain.PeerAddress, transport *dialledTransport, welcome protocol.Frame) error {
	if transport.probed == nil {
		return nil
	}
	if err := s.refuseLegacyNowRuledOut(address, domain.DialToAddress()); err != nil {
		return err
	}
	if transport.probed.admits(welcome) {
		return nil
	}
	s.secureSessions.marks.forgetLegacyObserved(address)
	log.Warn().Str("peer", string(address)).Uint64("conn_id", uint64(transport.connID)).Msg("secure_session_welcome_unlike_probe")
	return fmt.Errorf("%w: %s", errLegacyWelcomeUnlikeProbe, address)
}

// dialLegacyTransport is the dial of a path that speaks v1 only
// (openPeerSession). A node that speaks v2 opens it only on a probe that
// allowed v1 — never while the address waits (refuseHeldDial), never where
// v2 is required, and never while the fall back to v1 at the address is
// held.
func (s *Service) dialLegacyTransport(ctx context.Context, address domain.PeerAddress, dialRaw rawDialer) (*dialledTransport, error) {
	if err := s.refuseHeldDial(address); err != nil {
		return nil, err
	}
	dial := s.transportDialer(address, dialRaw)
	if s.sessionMode() == sessionv2.ModeLegacyOnly {
		return dial(ctx)
	}
	return s.dialLegacyByProbe(ctx, address, domain.DialToAddress(), dial)
}

// reactToIncompatibleDial is the one reaction to a dial that ended on an
// incompatible version, on every dial path. Only a version proven inside a
// v2 session is penalised (penalizeOldProtocolPeer: score, the ban of the
// address and its IP, the lockout of its identity, the update_available
// reporters). A claimed one — the probe's answer, a v1 welcome before
// auth_ok, or no evidence at all — may have been written by a man in the
// middle in front of an honest node, so it is recorded for diagnostics and
// only paces the next attempt to the address.
func (s *Service) reactToIncompatibleDial(address domain.PeerAddress, err error) {
	if !errors.Is(err, errIncompatibleProtocol) {
		return
	}
	var incompatible *incompatibleProtocolError
	if errors.As(err, &incompatible) && incompatible.Evidence == versionProven {
		s.penalizeOldProtocolPeer(address, incompatible.PeerVersion, incompatible.PeerMinimum, domain.ClientVersion(incompatible.ClientVersion))
		return
	}
	claim := incompatibleVersionClaim{}
	if incompatible != nil {
		claim = incompatibleVersionClaim{peerVersion: incompatible.PeerVersion, peerMinimum: incompatible.PeerMinimum, clientVersion: domain.ClientVersion(incompatible.ClientVersion)}
	}
	s.noteClaimedIncompatibleVersion(address, claim)
	if incompatible != nil && incompatible.claimsOldVersion() {
		s.delayEndpointClaimingOldVersion(address, incompatible.PeerVersion)
		return
	}
	s.paceAfterClaimedVersion(address)
}

// paceAfterClaimedVersion makes the next attempt to address wait for its v2
// retry backoff, in every mode: the gate reads it whatever the node speaks.
func (s *Service) paceAfterClaimedVersion(address domain.PeerAddress) {
	marks := s.dialMarks()
	if marks == nil {
		return
	}
	retry := marks.noteV2Unanswered(address)
	log.Debug().Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_claimed_incompatible_version_paced")
}

// delayEndpointClaimingOldVersion: an endpoint whose correct answer claimed
// a version below this node's minimum is delayed on its own, 30 s doubling to
// 6 h, so a node that really is old is not redialled every few seconds. No
// other address, no IP, no identity and no score is touched: the claim is
// unproven and may be forged. It holds in every mode.
func (s *Service) delayEndpointClaimingOldVersion(address domain.PeerAddress, claimed domain.ProtocolVersion) {
	marks := s.dialMarks()
	if marks == nil {
		return
	}
	wait := marks.noteClaimedOldVersion(address, claimed)
	log.Info().Str("peer", string(address)).Int("claimed_version", int(claimed)).Dur("next_attempt_in", wait).Msg("endpoint_dial_delayed_claimed_old_version")
}

// forgetOldVersionDelay ends the old-version delay of address after an
// outbound connection to it succeeded over a protocol it may use, the pin
// respected — the only automatic reset: an inbound session proves nothing
// about what this node reaches at the endpoint.
func (s *Service) forgetOldVersionDelay(address domain.PeerAddress) {
	marks := s.dialMarks()
	if marks == nil || !marks.forgetOldVersion(address) {
		return
	}
	log.Info().Str("peer", string(address)).Msg("endpoint_dial_delay_reset_outbound_success")
}

// ListEndpointDialDelays is every endpoint this node delays its dials to, for
// the operator (loopback-only RPC listEndpointDialDelays). Takes only the
// marks' leaf mutex.
func (s *Service) ListEndpointDialDelays(ctx context.Context) ([]domain.EndpointDialDelay, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	marks := s.dialMarks()
	if marks == nil {
		return []domain.EndpointDialDelay{}, nil
	}
	delays := marks.oldVersionDelays()
	sort.Slice(delays, func(i, j int) bool { return delays[i].Endpoint < delays[j].Endpoint })
	return delays, nil
}

// RetryEndpointDial is the operator's retry by hand of one endpoint after its
// peer was upgraded (loopback-only RPC retryEndpointDial): the current wait
// of that endpoint's old-version delay ends, so its next dial may go at once;
// the count stays, so a peer that still claims an old version is delayed
// longer, not from 30 s again. It dials nothing itself, and nothing else is
// touched — no pin, no endpoint binding, no v2 retry backoff, no connection
// manager cooldown — so a downgrade stays refused.
func (s *Service) RetryEndpointDial(ctx context.Context, endpoint domain.PeerAddress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	marks := s.dialMarks()
	if marks == nil || !marks.endOldVersionWait(endpoint) {
		return fmt.Errorf("%w: %s", domain.ErrEndpointDialDelayNotFound, endpoint)
	}
	log.Info().Str("peer", string(endpoint)).Msg("endpoint_dial_delay_lifted_by_operator")
	return nil
}
