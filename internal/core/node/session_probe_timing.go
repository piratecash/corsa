//go:build s6_first_contact_probe

package node

import (
	"context"
	"errors"
	"io"
)

func probeLegacyOnTransport(ctx context.Context, transport *dialledTransport, opts legacyProbeOptions) (legacyProbeAnswer, error) {
	attempt := firstContactFromContext(ctx)
	if attempt.id == 0 {
		return probeLegacy(ctx, transport.metered, opts)
	}
	timing := legacyProbeTiming{}
	opts.timing = &timing
	answer, err := probeLegacy(ctx, transport.metered, opts)
	event := attempt.event(transport.connID).
		Int64("budget_ns", opts.timeout.Nanoseconds()).
		Bool("measured", timing.started).
		Str("outcome", legacyProbeTimingOutcome(answer, err))
	if timing.started {
		event.Int64("elapsed_ns", timing.elapsed.Nanoseconds())
	}
	event.Msg("s6_first_contact_legacy_probe")
	return answer, err
}

func legacyProbeTimingOutcome(answer legacyProbeAnswer, err error) string {
	switch {
	case err == nil:
		return answer.outcome.String()
	case errors.Is(err, errLegacyProbeAborted):
		return "cancelled"
	case errors.Is(err, errLegacyProbeTimeout):
		return "timeout"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case isLocalProbeFailure(err), errors.Is(err, errLegacyProbeWrite):
		return "io_or_local_failure"
	case errors.Is(err, errLegacyProbeNoAnswer):
		return "read_failure"
	default:
		return "rejected_answer"
	}
}
