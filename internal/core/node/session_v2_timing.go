//go:build s6_first_contact_probe

package node

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

func dialSessionV2(ctx context.Context, id domain.ConnID, wire net.Conn, local sessionv2.Local, intro protocol.Frame, expect sessionv2.ExpectedPeer, timeouts sessionv2.Timeouts) (sessionv2.ProvenSession, error) {
	attempt := firstContactFromContext(ctx)
	if attempt.id == 0 {
		return sessionv2.Dial(ctx, wire, local, intro, expect, timeouts)
	}
	type phaseTiming struct {
		phase   string
		elapsed time.Duration
		err     error
	}
	phases := make([]phaseTiming, 0, 2)
	session, err := sessionv2.DialWithTiming(ctx, wire, local, intro, expect, timeouts, func(phase string, elapsed time.Duration, err error) {
		phases = append(phases, phaseTiming{phase, elapsed, err})
	})
	for _, timing := range phases {
		budget := timeouts.TLS
		if timing.phase == "proof" {
			budget = timeouts.Proof
		}
		attempt.event(id).
			Str("phase", timing.phase).
			Int64("budget_ns", budget.Nanoseconds()).
			Int64("elapsed_ns", timing.elapsed.Nanoseconds()).
			Str("outcome", sessionV2TimingOutcome(timing.err, err)).
			Msg("s6_first_contact_v2_phase")
	}
	attempt.event(id).
		Int("phases", len(phases)).
		Str("outcome", sessionV2TimingOutcome(err, err)).
		Msg("s6_first_contact_v2_result")
	return session, err
}

func sessionV2TimingOutcome(phaseErr, dialErr error) string {
	switch {
	case phaseErr == nil:
		return "completed"
	case errors.Is(phaseErr, context.Canceled), errors.Is(phaseErr, context.DeadlineExceeded),
		errors.Is(dialErr, context.Canceled), errors.Is(dialErr, context.DeadlineExceeded):
		return "context_ended"
	case errors.Is(phaseErr, os.ErrDeadlineExceeded):
		return "timeout"
	case errors.Is(dialErr, sessionv2.ErrPeerSpeaksV1):
		return "legacy_answer"
	case errors.Is(phaseErr, io.EOF), errors.Is(phaseErr, io.ErrUnexpectedEOF):
		return "eof"
	case errors.Is(phaseErr, sessionv2.ErrProofInvalid), errors.Is(phaseErr, sessionv2.ErrPeerMismatch),
		errors.Is(phaseErr, sessionv2.ErrSelfConnection):
		return "rejected"
	case errors.Is(phaseErr, sessionv2.ErrIntro):
		return "intro_failed"
	default:
		return "failed"
	}
}
