package loadstand

import (
	"context"
	"encoding/json"
	"math"

	"github.com/piratecash/corsa/internal/core/domain"
)

var resourceGaugeKinds = []domain.ResourceGaugeKind{domain.ResourceGaugeMemory, domain.ResourceGaugeSaturation}

func (s *ProcessSource) Resources(ctx context.Context) (domain.ResourceBreakdown, error) {
	fields, err := s.commandObject(ctx, "getResourceBreakdown", "sampled_at", "floor_bytes", "subsystems")
	if err != nil {
		return domain.ResourceBreakdown{}, err
	}
	at, err := timestamp(fields, "sampled_at")
	if err != nil {
		return domain.ResourceBreakdown{}, err
	}
	if at.After(s.clock.Now()) {
		return domain.ResourceBreakdown{}, ErrInvalidSample
	}
	floor, err := value[uint64](fields, "floor_bytes")
	if err != nil {
		return domain.ResourceBreakdown{}, err
	}
	rows, err := value[[]json.RawMessage](fields, "subsystems")
	if err != nil {
		return domain.ResourceBreakdown{}, err
	}
	owners := domain.ResourceSubsystems()
	seen := map[domain.ResourceSubsystem]bool{}
	usages := make([]domain.SubsystemUsage, 0, len(rows))
	var total uint64
	for _, raw := range rows {
		usage, err := decodeSubsystem(raw, owners)
		if err != nil {
			return domain.ResourceBreakdown{}, err
		}
		if seen[usage.Subsystem()] {
			return domain.ResourceBreakdown{}, ErrInvalidSample
		}
		seen[usage.Subsystem()] = true
		if math.MaxUint64-total < usage.FloorBytes() {
			return domain.ResourceBreakdown{}, ErrInvalidSample
		}
		total += usage.FloorBytes()
		usages = append(usages, usage)
	}
	if len(seen) != len(owners) {
		return domain.ResourceBreakdown{}, ErrFamilyUnavailable
	}
	if total != floor {
		return domain.ResourceBreakdown{}, ErrInvalidSample
	}
	return domain.NewResourceBreakdown(at, usages...), nil
}
func decodeSubsystem(raw json.RawMessage, owners []domain.ResourceSubsystem) (domain.SubsystemUsage, error) {
	fields, err := object(raw, "subsystem", "floor_bytes", "gauges")
	if err != nil {
		return domain.SubsystemUsage{}, err
	}
	name, err := value[string](fields, "subsystem")
	if err != nil {
		return domain.SubsystemUsage{}, err
	}
	owner, err := parseName(name, owners)
	if err != nil {
		return domain.SubsystemUsage{}, err
	}
	floor, err := value[uint64](fields, "floor_bytes")
	if err != nil {
		return domain.SubsystemUsage{}, err
	}
	rows, err := value[[]json.RawMessage](fields, "gauges")
	if err != nil {
		return domain.SubsystemUsage{}, err
	}
	gauges := make([]domain.ResourceGauge, 0, len(rows))
	seen := map[string]bool{}
	var total uint64
	for _, raw := range rows {
		gauge, err := decodeGauge(raw)
		if err != nil {
			return domain.SubsystemUsage{}, err
		}
		if seen[gauge.Name()] {
			return domain.SubsystemUsage{}, ErrInvalidSample
		}
		seen[gauge.Name()] = true
		if math.MaxUint64-total < gauge.FloorBytes() {
			return domain.SubsystemUsage{}, ErrInvalidSample
		}
		total += gauge.FloorBytes()
		gauges = append(gauges, gauge)
	}
	if floor != total {
		return domain.SubsystemUsage{}, ErrInvalidSample
	}
	return domain.NewSubsystemUsage(owner, gauges...), nil
}
func decodeGauge(raw json.RawMessage) (domain.ResourceGauge, error) {
	fields, err := object(raw, "name", "kind", "count", "entry_bytes", "floor_bytes")
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	name, err := value[string](fields, "name")
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	kind, err := value[string](fields, "kind")
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	count, err := value[uint64](fields, "count")
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	entry, err := value[uint64](fields, "entry_bytes")
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	floor, err := value[uint64](fields, "floor_bytes")
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	if name == "" || count > uint64(math.MaxInt) {
		return domain.ResourceGauge{}, ErrInvalidSample
	}
	typedKind, err := parseName(kind, resourceGaugeKinds)
	if err != nil {
		return domain.ResourceGauge{}, err
	}
	switch typedKind {
	case domain.ResourceGaugeMemory:
		if entry != 0 && count > math.MaxUint64/entry || floor != count*entry {
			return domain.ResourceGauge{}, ErrInvalidSample
		}
		return domain.NewResourceGauge(name, int(count), entry), nil
	case domain.ResourceGaugeSaturation:
		if entry != 0 || floor != 0 {
			return domain.ResourceGauge{}, ErrInvalidSample
		}
		return domain.NewSaturationGauge(name, int(count)), nil
	default:
		return domain.ResourceGauge{}, ErrInvalidSample
	}
}
