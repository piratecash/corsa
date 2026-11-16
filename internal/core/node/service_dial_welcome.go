package node

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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

// admitServiceDialWelcome applies the self-dial cooldown before a one-shot
// secure connection sends application frames or records outbound success.
func (s *Service) admitServiceDialWelcome(dial serviceDial, address domain.PeerAddress, welcome protocol.Frame) error {
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
