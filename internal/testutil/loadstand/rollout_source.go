package loadstand

import (
	"context"
	"encoding/json"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

var modeOperations = []routing.AnnounceOperation{routing.AnnounceOperationDelta, routing.AnnounceOperationFullSync}
var announceModes = []routing.AnnounceMode{routing.AnnounceModeV1, routing.AnnounceModeV2, routing.AnnounceModeV3, routing.AnnounceModeDivergence}
var modeReasons = []routing.ModeReason{routing.ModeReasonNegotiated, routing.ModeReasonMissingRoutingV1, routing.ModeReasonMissingRoutingV2, routing.ModeReasonMissingRoutingV3, routing.ModeReasonMissingRelayV1, routing.ModeReasonNoWireBaselineV2, routing.ModeReasonNoWireBaselineV3, routing.ModeReasonCapabilityDivergence, routing.ModeReasonUnknown, routing.ModeReasonLocalRoutingV1Disabled, routing.ModeReasonLocalRoutingV2Disabled, routing.ModeReasonLocalRoutingV3Disabled, routing.ModeReasonLocalRelayV1Disabled}
var neighbourCapabilities = []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRoutingV3, domain.CapMeshRelayV1, domain.CapMeshDatagramV1, domain.CapMeshDatagramTransitV1}

func parseName[T interface{ String() string }](name string, known []T) (T, error) {
	for _, v := range known {
		if v.String() == name {
			return v, nil
		}
	}
	var zero T
	return zero, ErrInvalidSample
}
func (s *ProcessSource) ModeSelection(ctx context.Context) (routing.ModeSelectionStats, error) {
	summary, err := s.RouteSummary(ctx)
	return s.modeSelectionFromSummary(summary, err)
}

func (s *ProcessSource) modeSelectionFromSummary(summary map[string]json.RawMessage, summaryErr error) (routing.ModeSelectionStats, error) {
	if summaryErr != nil {
		return routing.ModeSelectionStats{}, summaryErr
	}
	fields, err := familyObjectFromSummary(summary, "mode_selection", "started_at", "read_at", "decisions")
	if err != nil {
		return routing.ModeSelectionStats{}, err
	}
	start, err := timestamp(fields, "started_at")
	if err != nil {
		return routing.ModeSelectionStats{}, err
	}
	read, err := timestamp(fields, "read_at")
	if err != nil {
		return routing.ModeSelectionStats{}, err
	}
	if !validPeriod(start, read) || read.After(s.clock.Now()) {
		return routing.ModeSelectionStats{}, ErrInvalidSample
	}
	rows, err := value[[]json.RawMessage](fields, "decisions")
	if err != nil {
		return routing.ModeSelectionStats{}, err
	}
	out := routing.ModeSelectionStats{StartedAt: start, ReadAt: read, Decisions: make([]routing.ModeDecision, 0, len(rows))}
	seen := map[[3]uint8]bool{}
	for _, raw := range rows {
		decision, err := decodeModeDecision(raw)
		if err != nil {
			return routing.ModeSelectionStats{}, err
		}
		key := [3]uint8{uint8(decision.Operation), uint8(decision.Mode), uint8(decision.Reason)}
		if seen[key] {
			return routing.ModeSelectionStats{}, ErrInvalidSample
		}
		seen[key] = true
		out.Decisions = append(out.Decisions, decision)
	}
	return out, nil
}
func decodeModeDecision(raw json.RawMessage) (routing.ModeDecision, error) {
	fields, err := object(raw, "operation", "mode", "reason", "count")
	if err != nil {
		return routing.ModeDecision{}, err
	}
	op, err := value[string](fields, "operation")
	if err != nil {
		return routing.ModeDecision{}, err
	}
	operation, err := parseName(op, modeOperations)
	if err != nil {
		return routing.ModeDecision{}, err
	}
	name, err := value[string](fields, "mode")
	if err != nil {
		return routing.ModeDecision{}, err
	}
	mode, err := parseName(name, announceModes)
	if err != nil {
		return routing.ModeDecision{}, err
	}
	name, err = value[string](fields, "reason")
	if err != nil {
		return routing.ModeDecision{}, err
	}
	reason, err := parseName(name, modeReasons)
	if err != nil {
		return routing.ModeDecision{}, err
	}
	count, err := value[uint64](fields, "count")
	if err != nil {
		return routing.ModeDecision{}, err
	}
	return routing.ModeDecision{Operation: operation, Mode: mode, Reason: reason, Count: count}, nil
}
func (s *ProcessSource) Neighbours(ctx context.Context) (domain.NeighbourComposition, error) {
	summary, err := s.RouteSummary(ctx)
	return s.neighboursFromSummary(summary, err)
}

func (s *ProcessSource) neighboursFromSummary(summary map[string]json.RawMessage, summaryErr error) (domain.NeighbourComposition, error) {
	if summaryErr != nil {
		return domain.NeighbourComposition{}, summaryErr
	}
	fields, err := familyObjectFromSummary(summary, "neighbours", "ready", "updated_at", "connections", "peers", "identity_unproven", "identity_unknown", "capabilities", "routing_v3_triplet")
	if err != nil {
		return domain.NeighbourComposition{}, err
	}
	ready, err := value[bool](fields, "ready")
	if err != nil {
		return domain.NeighbourComposition{}, err
	}
	if !ready {
		return domain.NeighbourComposition{}, ErrFamilyUnavailable
	}
	at, err := timestamp(fields, "updated_at")
	if err != nil {
		return domain.NeighbourComposition{}, err
	}
	if at.After(s.clock.Now()) {
		return domain.NeighbourComposition{}, ErrInvalidSample
	}
	out := domain.NeighbourComposition{Ready: true, UpdatedAt: at}
	counts := []struct {
		name   string
		target *int
	}{{"connections", &out.Connections}, {"peers", &out.Peers}, {"identity_unproven", &out.IdentityUnproven}, {"identity_unknown", &out.IdentityUnknown}}
	for _, field := range counts {
		n, err := value[int](fields, field.name)
		if err != nil {
			return domain.NeighbourComposition{}, err
		}
		if n < 0 {
			return domain.NeighbourComposition{}, ErrInvalidSample
		}
		*field.target = n
	}
	if out.Peers > out.Connections || out.IdentityUnproven > out.Connections || out.IdentityUnknown > out.Connections-out.IdentityUnproven {
		return domain.NeighbourComposition{}, ErrInvalidSample
	}
	rows, err := value[[]json.RawMessage](fields, "capabilities")
	if err != nil {
		return domain.NeighbourComposition{}, err
	}
	seen := map[domain.Capability]bool{}
	for _, raw := range rows {
		usage, err := decodeCapability(raw, out.Connections)
		if err != nil {
			return domain.NeighbourComposition{}, err
		}
		if _, err := parseName(usage.Capability.String(), neighbourCapabilities); err != nil || seen[usage.Capability] {
			return domain.NeighbourComposition{}, ErrInvalidSample
		}
		seen[usage.Capability] = true
		out.Capabilities = append(out.Capabilities, usage)
	}
	if len(seen) != len(neighbourCapabilities) {
		return domain.NeighbourComposition{}, ErrFamilyUnavailable
	}
	out.RoutingV3Triplet, err = decodeCapability(fields["routing_v3_triplet"], out.Connections)
	if err != nil {
		return domain.NeighbourComposition{}, err
	}
	if out.RoutingV3Triplet.Capability != domain.CapMeshRoutingV3 {
		return domain.NeighbourComposition{}, ErrInvalidSample
	}
	return out, nil
}
func decodeCapability(raw json.RawMessage, total int) (domain.CapabilityUsage, error) {
	fields, err := object(raw, "capability", "connections", "peers")
	if err != nil {
		return domain.CapabilityUsage{}, err
	}
	name, err := value[string](fields, "capability")
	if err != nil {
		return domain.CapabilityUsage{}, err
	}
	connections, err := value[int](fields, "connections")
	if err != nil {
		return domain.CapabilityUsage{}, err
	}
	peers, err := value[int](fields, "peers")
	if err != nil {
		return domain.CapabilityUsage{}, err
	}
	if name == "" || connections < 0 || peers < 0 || peers > connections || connections > total {
		return domain.CapabilityUsage{}, ErrInvalidSample
	}
	return domain.CapabilityUsage{Capability: domain.Capability(name), Connections: connections, Peers: peers}, nil
}
