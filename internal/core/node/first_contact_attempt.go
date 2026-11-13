//go:build s6_first_contact_probe

package node

import (
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

type firstContactAttemptKey struct{}

type firstContactAttempt struct {
	id      uint64
	started time.Time
}

var firstContactAttemptSequence atomic.Uint64

func beginFirstContactAttempt(ctx context.Context) context.Context {
	attempt := firstContactAttempt{id: firstContactAttemptSequence.Add(1), started: time.Now()}
	attempt.event(0).Msg("s6_first_contact_attempt")
	return context.WithValue(ctx, firstContactAttemptKey{}, attempt)
}

func firstContactFromContext(ctx context.Context) firstContactAttempt {
	attempt, _ := ctx.Value(firstContactAttemptKey{}).(firstContactAttempt)
	return attempt
}

func (attempt firstContactAttempt) event(connID domain.ConnID) *zerolog.Event {
	return log.Info().Uint64("attempt_id", attempt.id).Uint64("conn_id", uint64(connID))
}

type firstContactSocketTiming struct {
	attempt firstContactAttempt
	elapsed time.Duration
	err     error
}

func openFirstContactSocket(ctx context.Context, dial rawDialer) (net.Conn, firstContactSocketTiming, error) {
	attempt := firstContactFromContext(ctx)
	if attempt.id == 0 {
		conn, err := dial(ctx)
		return conn, firstContactSocketTiming{}, err
	}
	started := time.Now()
	conn, err := dial(ctx)
	return conn, firstContactSocketTiming{attempt, time.Since(started), err}, err
}

func (timing firstContactSocketTiming) record(connID domain.ConnID, outcome string) {
	if timing.attempt.id == 0 {
		return
	}
	switch {
	case errors.Is(timing.err, context.Canceled), errors.Is(timing.err, context.DeadlineExceeded):
		outcome = "context_ended"
	case errors.Is(timing.err, os.ErrDeadlineExceeded):
		outcome = "timeout"
	}
	timing.attempt.event(connID).Int64("elapsed_ns", timing.elapsed.Nanoseconds()).
		Str("outcome", outcome).Msg("s6_first_contact_transport")
}

func recordFirstContactSetup(ctx context.Context, connID domain.ConnID, err error) {
	attempt := firstContactFromContext(ctx)
	if attempt.id == 0 {
		return
	}
	outcome := "ready_for_cm"
	if err != nil {
		outcome = "setup_failed"
	}
	attempt.event(connID).Int64("elapsed_ns", time.Since(attempt.started).Nanoseconds()).
		Str("outcome", outcome).Msg("s6_first_contact_setup")
}

func (attempt firstContactAttempt) recordPublication(connID domain.ConnID, outcome string, version int, proven bool) {
	if attempt.id == 0 {
		return
	}
	attempt.event(connID).Int64("elapsed_ns", time.Since(attempt.started).Nanoseconds()).
		Str("outcome", outcome).Int("protocol_version", version).Bool("v2_proven", proven).
		Msg("s6_first_contact_publication")
}
