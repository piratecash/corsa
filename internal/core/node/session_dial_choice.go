package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// Outbound sessions use one TLS attempt. A failed attempt can update
// endpoint backoff, then returns without opening a plaintext connection.

// TLS and proof deadlines differ for direct and overlay addresses.
type sessionDialTimeouts struct {
	direct, overlay sessionv2.Timeouts
}

func defaultSessionDialTimeouts() sessionDialTimeouts {
	return sessionDialTimeouts{
		direct:  sessionv2.DefaultTimeouts,
		overlay: sessionv2.Timeouts{TLS: 20 * time.Second, Proof: 10 * time.Second},
	}
}

func (t sessionDialTimeouts) forAddress(address domain.PeerAddress) sessionv2.Timeouts {
	if classifyAddress(address).IsOverlay() {
		return t.overlay
	}
	return t.direct
}

var (
	// errV2DialLocalFailure is a v2 attempt this node could not start — its
	// own identity or arguments unusable — before a byte reached the wire.
	errV2DialLocalFailure = errors.New("secure session: the v2 attempt failed locally")
	// errPinRoomAwaited is a dial refused before any socket because the last
	// v2 session to the address was refused for a full pin store that still
	// has no room.
	errPinRoomAwaited = errors.New("secure session: dial held until the pin store has room")
	// errV2FailedBeforeProof is a TLS attempt over an opened TCP connection
	// that ended before the identity proof verified: a TLS answer that then
	// failed — a certificate, a handshake broken on the way, a proof or its
	// binding that does not verify — an alert that is not the listener's
	// refusal, or, where v1 is ruled out, silence, a reset or bytes that are
	// neither. Nothing proved who answered, so it is no fault of the node
	// there: not charged, only paced. It never leads to v1.
	errV2FailedBeforeProof = errors.New("secure session: the TLS attempt failed before the peer was authenticated")
	// errDialAbortedLocally is a dial this node ended itself — its stop, or
	// the caller cancelling — whatever stage it was in: no failure of the
	// address, and nothing paced.
	errDialAbortedLocally = errors.New("secure session: the dial was ended by this node")
)

// failedV2Attempt is a TLS attempt that ended without a session.
type failedV2Attempt struct {
	address domain.PeerAddress
	intent  domain.DialIntent
	connID  domain.ConnID
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
func (s *Service) afterFailedV2Attempt(ctx context.Context, attempt failedV2Attempt) (*dialledTransport, error) {
	if ctx.Err() == nil && errors.Is(attempt.err, sessionv2.ErrSelfConnection) {
		return nil, s.newSelfIdentityError(attempt.address)
	}
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
	case v2AnsweredByAlert:
		if errors.Is(attempt.err, sessionv2.ErrRefusedByListener) {
			return nil, s.noteV2RefusedByListener(attempt.address, attempt.err)
		}
		return nil, s.noteV2EndedBeforeProof(attempt)
	case v2AnsweredByV1Line:
		return nil, fmt.Errorf("%w: %w", errV1RefusedV2Only, attempt.err)
	default:
		return nil, s.noteV2EndedBeforeProof(attempt)
	}
}

// noteV2EndedBeforeProof ends a TLS attempt that nothing after it may
// follow: the next dial to the address is paced — one step per dial — and the
// address is not charged (errV2FailedBeforeProof).
func (s *Service) noteV2EndedBeforeProof(attempt failedV2Attempt) error {
	retry := s.secureSessions.marks.noteV2Unanswered(attempt.address)
	log.Debug().Err(attempt.err).Str("peer", string(attempt.address)).Dur("v2_retry", retry).Msg("secure_session_v2_ended_before_proof")
	return fmt.Errorf("%w: %w", errV2FailedBeforeProof, attempt.err)
}

// reactToIncompatibleDial is the one reaction to a dial that ended on an
// incompatible version, on every dial path. A proven intro identifies the
// responder, but does not authenticate earlier address bindings or other
// peers sharing the IP. A below-floor refusal only paces this endpoint.
func (s *Service) reactToIncompatibleDial(address domain.PeerAddress, err error) {
	if !errors.Is(err, errIncompatibleProtocol) {
		return
	}
	var incompatible *incompatibleProtocolError
	if errors.As(err, &incompatible) && incompatible.Evidence == versionProven {
		s.paceAfterIncompatibleVersion(address)
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
	s.paceAfterIncompatibleVersion(address)
}

// paceAfterIncompatibleVersion makes the next attempt to address wait for its v2
// retry backoff, in every mode: the gate reads it whatever the node speaks.
func (s *Service) paceAfterIncompatibleVersion(address domain.PeerAddress) {
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
