package loadstand

import (
	"context"
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type trafficDTO struct {
	StartedAt     time.Time `json:"started_at"`
	ReadAt        time.Time `json:"read_at"`
	BytesSent     *uint64   `json:"bytes_sent"`
	BytesReceived *uint64   `json:"bytes_received"`
}

func (s *ProcessSource) TransportTraffic(ctx context.Context) (domain.TransportTrafficStats, error) {
	var wire trafficDTO
	if err := s.ReadFamily(ctx, "transport_traffic", &wire); err != nil {
		return domain.TransportTrafficStats{}, err
	}
	if wire.BytesSent == nil || wire.BytesReceived == nil {
		return domain.TransportTrafficStats{}, ErrFamilyUnavailable
	}
	if !validPeriod(wire.StartedAt, wire.ReadAt) {
		return domain.TransportTrafficStats{}, ErrInvalidSample
	}
	return domain.TransportTrafficStats{StartedAt: wire.StartedAt, ReadAt: wire.ReadAt, BytesSent: *wire.BytesSent, BytesReceived: *wire.BytesReceived}, nil
}

type sessionsDTO struct {
	StartedAt     time.Time `json:"started_at"`
	ReadAt        time.Time `json:"read_at"`
	Attempts      *uint64   `json:"attempts"`
	Succeeded     *uint64   `json:"succeeded"`
	ErrorsConnect *uint64   `json:"errors_connect"`
	ErrorsCompat  *uint64   `json:"errors_compat"`
	ErrorsOther   *uint64   `json:"errors_other"`
}

func (s *ProcessSource) SessionOutcomes(ctx context.Context) (domain.SessionOutcomeStats, error) {
	var wire sessionsDTO
	if err := s.ReadFamily(ctx, "session_outcomes", &wire); err != nil {
		return domain.SessionOutcomeStats{}, err
	}
	if wire.Attempts == nil || wire.Succeeded == nil || wire.ErrorsConnect == nil || wire.ErrorsCompat == nil || wire.ErrorsOther == nil {
		return domain.SessionOutcomeStats{}, ErrFamilyUnavailable
	}
	if !validPeriod(wire.StartedAt, wire.ReadAt) {
		return domain.SessionOutcomeStats{}, ErrInvalidSample
	}
	var total uint64
	for _, count := range []uint64{*wire.Succeeded, *wire.ErrorsConnect, *wire.ErrorsCompat, *wire.ErrorsOther} {
		if math.MaxUint64-total < count {
			return domain.SessionOutcomeStats{}, ErrInvalidSample
		}
		total += count
	}
	if total != *wire.Attempts {
		return domain.SessionOutcomeStats{}, ErrInvalidSample
	}
	return domain.SessionOutcomeStats{StartedAt: wire.StartedAt, ReadAt: wire.ReadAt, Attempts: *wire.Attempts, Succeeded: *wire.Succeeded, ErrorsConnect: *wire.ErrorsConnect, ErrorsCompat: *wire.ErrorsCompat, ErrorsOther: *wire.ErrorsOther}, nil
}
func (s *ProcessSource) RouteEntries(ctx context.Context) (int, error) {
	summary, err := s.RouteSummary(ctx)
	if err != nil {
		return 0, err
	}
	var value *int
	if err := decodeRequired(summary, "total_entries", &value); err != nil {
		return 0, err
	}
	if value == nil {
		return 0, ErrFamilyUnavailable
	}
	if *value < 0 {
		return 0, ErrInvalidSample
	}
	return *value, nil
}
func validPeriod(started, read time.Time) bool { return !started.IsZero() && !read.Before(started) }
