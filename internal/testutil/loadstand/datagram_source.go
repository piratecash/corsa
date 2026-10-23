package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

var ErrDatagramDisabled = errors.New("process datagram explicitly disabled")

type Reading[T any] struct {
	sample  T
	refusal error
}

func (r Reading[T]) Get() (T, error) { return r.sample, r.refusal }

type DatagramSample struct {
	Metrics datagram.MetricsSnapshot
	Reverse datagram.ReverseDiagnostics
	// Nil preserves older/absent attribution as not measured, never zero.
	LocalSenders *datagram.LocalSendersSnapshot `json:",omitempty"`
}

func (s *ProcessSource) DatagramSummary(ctx context.Context) (Reading[DatagramSample], error) {
	state, err := s.datagramRuntimeState(ctx)
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	if state.State == domain.DatagramStateConfiguredOff {
		return Reading[DatagramSample]{refusal: ErrDatagramDisabled}, nil
	}

	fields, err := s.commandObject(ctx, "fetchDatagramSummary", "enabled")
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	enabled, err := value[bool](fields, "enabled")
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	if !enabled {
		return Reading[DatagramSample]{}, ErrInvalidSample
	}
	metricsRaw, ok := fields["metrics"]
	if !ok {
		return Reading[DatagramSample]{}, ErrFamilyUnavailable
	}
	reverseRaw, ok := fields["reverse"]
	if !ok {
		return Reading[DatagramSample]{}, ErrFamilyUnavailable
	}
	metrics, err := decodeDatagramMetrics(metricsRaw)
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	if !metrics.StartedAt.Equal(s.binding.StartedAt) || metrics.ReadAt.After(s.clock.Now()) {
		return Reading[DatagramSample]{}, ErrInvalidSample
	}
	reverseFields, err := object(reverseRaw, "LocalRefusals", "Held", "LocalSlots")
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	refusals, err := decodeCounterMap(reverseFields["LocalRefusals"])
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	held, err := value[int](reverseFields, "Held")
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	slots, err := value[int](reverseFields, "LocalSlots")
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	if held < 0 || slots < 0 {
		return Reading[DatagramSample]{}, ErrInvalidSample
	}
	local, err := DecodeOptionalLocalSenders(fields, s.binding.StartedAt, s.clock.Now())
	if err != nil {
		return Reading[DatagramSample]{}, err
	}
	return Reading[DatagramSample]{sample: DatagramSample{LocalSenders: local, Metrics: metrics, Reverse: datagram.ReverseDiagnostics{LocalRefusals: refusals, Held: held, LocalSlots: slots}}}, nil
}
func decodeDatagramMetrics(raw json.RawMessage) (datagram.MetricsSnapshot, error) {
	fields, err := object(raw, "Observed", "Accepted", "Delivered", "Forwarded", "Answered", "Dropped", "UnknownDType", "RefusedAnswers", "ByMode", "DropsByReason", "ReverseEvents", "SendRefusals", "StartedAt", "ReadAt")
	if err != nil {
		return datagram.MetricsSnapshot{}, err
	}
	start, err := timestamp(fields, "StartedAt")
	if err != nil {
		return datagram.MetricsSnapshot{}, err
	}
	read, err := timestamp(fields, "ReadAt")
	if err != nil {
		return datagram.MetricsSnapshot{}, err
	}
	if !validPeriod(start, read) {
		return datagram.MetricsSnapshot{}, ErrInvalidSample
	}
	var out datagram.MetricsSnapshot
	if err := decodeDTO(raw, &out); err != nil {
		return out, ErrInvalidSample
	}
	for _, name := range []string{"DropsByReason", "ReverseEvents", "SendRefusals"} {
		if _, err := decodeCounterMap(fields[name]); err != nil {
			return datagram.MetricsSnapshot{}, err
		}
	}
	modes, err := object(fields["ByMode"])
	if err != nil {
		return datagram.MetricsSnapshot{}, err
	}
	var totals datagram.ModeCounts
	for name, raw := range modes {
		if _, err := domain.ParseDatagramMode(name); err != nil {
			return datagram.MetricsSnapshot{}, ErrInvalidSample
		}
		row, err := object(raw, "Observed", "Delivered", "Forwarded", "Answered", "Dropped")
		if err != nil {
			return datagram.MetricsSnapshot{}, err
		}
		targets := []*uint64{&totals.Observed, &totals.Delivered, &totals.Forwarded, &totals.Answered, &totals.Dropped}
		for i, field := range []string{"Observed", "Delivered", "Forwarded", "Answered", "Dropped"} {
			count, err := value[uint64](row, field)
			if err != nil {
				return datagram.MetricsSnapshot{}, err
			}
			if math.MaxUint64-*targets[i] < count {
				return datagram.MetricsSnapshot{}, ErrInvalidSample
			}
			*targets[i] += count
		}
	}
	if totals.Observed != out.Observed || totals.Delivered != out.Delivered || totals.Forwarded != out.Forwarded || totals.Answered != out.Answered || totals.Dropped != out.Dropped {
		return datagram.MetricsSnapshot{}, ErrInvalidSample
	}
	if math.MaxUint64-totals.Delivered < totals.Forwarded || math.MaxUint64-(totals.Delivered+totals.Forwarded) < totals.Answered || totals.Delivered+totals.Forwarded+totals.Answered != out.Accepted {
		return datagram.MetricsSnapshot{}, ErrInvalidSample
	}
	return out, nil
}

func decodeCounterMap(raw json.RawMessage) (map[string]uint64, error) {
	fields, err := object(raw)
	if err != nil {
		return nil, err
	}
	out := make(map[string]uint64, len(fields))
	for name := range fields {
		count, err := value[uint64](fields, name)
		if err != nil {
			return nil, err
		}
		out[name] = count
	}
	return out, nil
}
